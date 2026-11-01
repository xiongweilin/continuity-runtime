"""Authenticated HTTP facade. Only trusted backend adapters can perform effects."""
from __future__ import annotations

import hmac
import json
import os
from dataclasses import dataclass
from datetime import datetime

from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .contracts import Conflict, Denied, Held, Intent, UnknownOperation
from .db import RuntimeDB
from .runtime import ContinuityRuntime


@dataclass(frozen=True)
class Caller:
    actor: str
    admin: bool = False


class GrantIn(BaseModel):
    id: str
    principal: str
    actors: list[str]
    provider: str
    resource_prefix: str
    action: str
    expires_at: datetime | None = None


class ObservationIn(BaseModel):
    ref: str
    source_revision: str
    evidence_ref: str
    expected_revision: int | None = None


class LinkIn(BaseModel):
    id: str
    source_ref: str
    target_ref: str
    relation: str
    purpose: str
    basis_ref: str
    constraints: dict = Field(default_factory=dict)
    expected_revision: int = 0


class SubscriptionIn(BaseModel):
    id: str


class AckIn(BaseModel):
    expected_revision: int
    next_seq: int


class OperationIn(BaseModel):
    id: str
    principal: str
    provider: str
    resource_ref: str
    action: str
    payload: dict = Field(default_factory=dict)
    authority_id: str
    participants: list[str]
    expected_versions: dict[str, str] = Field(default_factory=dict)
    effect_key: str | None = None


def create_app(runtime: ContinuityRuntime, *, tokens: dict[str, Caller] | None = None) -> FastAPI:
    if tokens is None:
        raw = os.getenv("CONTINUITY_TOKENS_JSON", "")
        values = json.loads(raw) if raw else {}
        tokens = {k: Caller(v["actor"], bool(v.get("admin", False))) for k, v in values.items()}
    app = FastAPI(title="Continuity Runtime", version="0.1.0")

    def principal(authorization: str | None = Header(None)) -> Caller:
        if not authorization or not authorization.startswith("Bearer "):
            raise HTTPException(401, "bearer token required")
        presented = authorization[7:]
        for key, actor in tokens.items():
            if hmac.compare_digest(key, presented):
                return actor
        raise HTTPException(401, "unknown bearer token")

    def admin(actor: Caller = Depends(principal)) -> Caller:
        if not actor.admin:
            raise HTTPException(403, "admin token required")
        return actor

    for exc_type, status in ((Conflict, 409), (Denied, 403), (Held, 409), (UnknownOperation, 404)):
        async def handler(request, exc, http_status=status):
            return JSONResponse({"detail": str(exc)}, status_code=http_status)
        app.add_exception_handler(exc_type, handler)

    @app.get("/health")
    def health():
        return {"status": "ok"}  # liveness only

    @app.post("/v1/grants")
    def issue(value: GrantIn, _: Caller = Depends(admin)):
        return runtime.issue_grant(grant_id=value.id, principal=value.principal,
            actors=value.actors, provider=value.provider,
            resource_prefix=value.resource_prefix, action=value.action,
            expires_at=value.expires_at)

    @app.post("/v1/grants/{grant_id}/revoke")
    def revoke(grant_id: str, _: Caller = Depends(admin)):
        return runtime.revoke_grant(grant_id)

    @app.post("/v1/observations")
    def observation(value: ObservationIn, actor: Caller = Depends(principal)):
        return runtime.observe(actor=actor.actor, ref=value.ref,
            source_revision=value.source_revision, evidence_ref=value.evidence_ref,
            expected_revision=value.expected_revision)

    @app.post("/v1/links")
    def link(value: LinkIn, actor: Caller = Depends(principal)):
        return runtime.link(link_id=value.id, actor=actor.actor,
            source_ref=value.source_ref, target_ref=value.target_ref,
            relation=value.relation, purpose=value.purpose,
            basis_ref=value.basis_ref, constraints=value.constraints,
            expected_revision=value.expected_revision)

    @app.get("/v1/links/{link_id}")
    def read_link(link_id: str, actor: Caller = Depends(principal)):
        value = runtime.get_link(link_id)
        if not actor.admin and value["owner"] != actor.actor:
            raise HTTPException(403, "link owner required")
        return value

    @app.post("/v1/operations")
    def submit(value: OperationIn, actor: Caller = Depends(principal)):
        intent = Intent(operation_id=value.id, principal=value.principal,
            provider=value.provider, resource_ref=value.resource_ref,
            action=value.action, payload=value.payload, authority_id=value.authority_id,
            expected_versions=value.expected_versions, effect_key=value.effect_key)
        return runtime.submit(intent, actor=actor.actor, participants=value.participants)

    @app.get("/v1/operations/{operation_id}")
    def inspect(operation_id: str, actor: Caller = Depends(principal)):
        return runtime.get_operation(operation_id, actor=None if actor.admin else actor.actor)

    @app.post("/v1/operations/{operation_id}/dispatch")
    def dispatch(operation_id: str, actor: Caller = Depends(principal)):
        return runtime.dispatch(operation_id, actor=actor.actor)

    @app.post("/v1/operations/{operation_id}/reconcile")
    def reconcile(operation_id: str, actor: Caller = Depends(principal)):
        return runtime.reconcile(operation_id, actor=actor.actor)

    @app.post("/v1/operations/{operation_id}/verify-outcome")
    def verify(operation_id: str, actor: Caller = Depends(principal)):
        return runtime.verify_outcome(operation_id, actor=actor.actor)

    @app.get("/v1/admin/recovery-queue")
    def recovery_queue(limit: int = 100, _: Caller = Depends(admin)):
        return runtime.recovery_queue(limit=limit)

    @app.get("/v1/events")
    def stream(after: int = 0, limit: int = 100, _: Caller = Depends(admin)):
        return runtime.events(after=after, limit=limit)

    @app.get("/v1/my-events")
    def my_events(after: int = 0, limit: int = 100, actor: Caller = Depends(principal)):
        return runtime.events_for_actor(actor.actor, after=after, limit=limit)

    @app.post("/v1/subscriptions")
    def subscribe(value: SubscriptionIn, actor: Caller = Depends(principal)):
        return runtime.subscribe(value.id, actor=actor.actor)

    @app.get("/v1/subscriptions/{subscription_id}")
    def poll(subscription_id: str, limit: int = 100, actor: Caller = Depends(principal)):
        return runtime.poll_subscription(subscription_id, actor=actor.actor, limit=limit)

    @app.post("/v1/subscriptions/{subscription_id}/ack")
    def ack(subscription_id: str, value: AckIn, actor: Caller = Depends(principal)):
        return runtime.ack_subscription(subscription_id, actor=actor.actor,
            expected_revision=value.expected_revision, next_seq=value.next_seq)

    return app


def default_app() -> FastAPI:
    url = os.environ.get("CONTINUITY_DATABASE_URL")
    if not url:
        return FastAPI(title="Continuity Runtime (unconfigured)")
    return create_app(ContinuityRuntime(RuntimeDB(url)))


app = default_app()
