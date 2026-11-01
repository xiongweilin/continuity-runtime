"""Private, least-privileged GitHub label executor.

Only this service owns the sandbox GitHub token. It pins repository, issue and
label independently of the caller and requires a separate internal service key.
"""
from __future__ import annotations

import hmac
import os
from dataclasses import asdict

from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from .contracts import Intent
from .github_labels import GitHubLabelAdapter, GitHubLabelVerifier
from .remote_executor import read_secret_file


class IntentData(BaseModel):
    model_config = ConfigDict(extra="forbid")
    operation_id: str
    principal: str
    provider: str
    resource_ref: str
    action: str
    payload: dict = Field(default_factory=dict)
    authority_id: str
    expected_versions: dict[str, str] = Field(default_factory=dict)
    effect_key: str | None = None


class Invocation(BaseModel):
    model_config = ConfigDict(extra="forbid")
    intent: IntentData


def create_executor_app(*, adapter: GitHubLabelAdapter, shared_secret: str) -> FastAPI:
    if len(shared_secret) < 32:
        raise ValueError("trusted executor secret must be at least 32 characters")
    if adapter.allowed_issue is None or len(adapter.allowed_labels) != 1:
        raise ValueError("Phase B requires exactly one allowed test issue and label")
    app = FastAPI(title="Isolated GitHub Label Executor")

    def authenticated(
        presented: str | None = Header(None, alias="X-Continuity-Executor-Key"),
    ) -> None:
        if presented is None or not hmac.compare_digest(presented, shared_secret):
            raise HTTPException(401, "trusted coordinator authentication required")

    def checked(value: Invocation) -> Intent:
        intent = Intent(**value.intent.model_dump())
        try:
            adapter._location(intent)
        except (ValueError, TypeError) as exc:
            raise HTTPException(403, "outside pinned GitHub Issue-label scope") from exc
        return intent

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.post("/v1/effects/execute")
    def execute(value: Invocation, _: None = Depends(authenticated)):
        intent = checked(value)
        try:
            return asdict(adapter.execute(intent))
        except Exception as exc:
            raise HTTPException(502, "trusted provider call failed") from exc

    @app.post("/v1/effects/readback")
    def readback(value: Invocation, _: None = Depends(authenticated)):
        intent = checked(value)
        try:
            return asdict(adapter.readback(intent))
        except Exception as exc:
            raise HTTPException(502, "trusted provider readback failed") from exc

    @app.post("/v1/effects/verify")
    def verify(value: Invocation, _: None = Depends(authenticated)):
        intent = checked(value)
        try:
            observed = adapter.readback(intent)  # fresh independent provider GET
        except Exception as exc:
            raise HTTPException(502, "trusted goal readback failed") from exc
        return asdict(GitHubLabelVerifier().verify(intent, observed))

    return app


def default_executor_app() -> FastAPI:
    required = (
        "CONTINUITY_GITHUB_REPOSITORY",
        "CONTINUITY_GITHUB_ISSUE",
        "CONTINUITY_GITHUB_LABEL",
        "CONTINUITY_GITHUB_TOKEN_FILE",
        "CONTINUITY_EXECUTOR_KEY_FILE",
    )
    if not any(os.getenv(name) for name in required):
        return FastAPI(title="GitHub executor (unconfigured)")
    if not all(os.getenv(name) for name in required):
        raise RuntimeError("incomplete trusted GitHub executor configuration")
    adapter = GitHubLabelAdapter(
        repository=os.environ["CONTINUITY_GITHUB_REPOSITORY"],
        token=read_secret_file(os.environ["CONTINUITY_GITHUB_TOKEN_FILE"]),
        allowed_labels={os.environ["CONTINUITY_GITHUB_LABEL"]},
        allowed_issue=int(os.environ["CONTINUITY_GITHUB_ISSUE"]),
    )
    return create_executor_app(
        adapter=adapter,
        shared_secret=read_secret_file(os.environ["CONTINUITY_EXECUTOR_KEY_FILE"]),
    )


app = default_executor_app()
