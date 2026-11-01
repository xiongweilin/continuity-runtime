"""Narrow cross-boundary coordination. Domain truth and workflow history stay outside."""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Mapping

from sqlalchemy import select, update

from .contracts import (
    AdmissionPolicy, BaselinePolicy, Conflict, Denied, DomainVerifier,
    EffectAdapter, Held, Intent, Readback, UnknownOperation,
)
from .db import RuntimeDB, grants, links, observations, operations, utcnow

PURPOSES = frozenset({
    "handoff", "responsibility", "authorization", "conflict", "recovery", "verification"
})


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str)


def _fingerprint(intent: Intent) -> str:
    return hashlib.sha256(_canonical(asdict(intent)).encode("utf-8")).hexdigest()


def _intent(row: Mapping[str, Any]) -> Intent:
    return Intent(
        operation_id=row["id"], principal=row["principal"],
        provider=row["provider"], resource_ref=row["resource_ref"],
        action=row["action"], payload=dict(row["payload"]),
        authority_id=row["authority_id"],
        expected_versions=dict(row["expected_versions"]),
    )


def _scope_allows(prefix: str, resource: str) -> bool:
    # No ambiguous prefix matches: "service/1" never matches "service/10".
    return prefix == "*" or resource == prefix or resource.startswith(prefix.rstrip("/") + "/")


class ContinuityRuntime:
    def __init__(
        self, db: RuntimeDB, *, adapters: dict[str, EffectAdapter] | None = None,
        policy: AdmissionPolicy | None = None,
        verifiers: dict[str, DomainVerifier] | None = None,
    ) -> None:
        self.db = db
        self.adapters = dict(adapters or {})
        self.policy = policy or BaselinePolicy()
        self.verifiers = dict(verifiers or {})

    def issue_grant(
        self, *, grant_id: str, principal: str, actors: list[str], provider: str,
        resource_prefix: str, action: str, expires_at: datetime | None = None,
    ) -> dict:
        if not all([grant_id, principal, actors, provider, resource_prefix, action]):
            raise ValueError("grant fields and actors are required")
        if len(set(actors)) != len(actors) or any(not a for a in actors):
            raise ValueError("grant actors must be distinct and nonempty")
        with self.db.engine.begin() as cx:
            if cx.execute(select(grants.c.id).where(grants.c.id == grant_id)).first():
                raise Conflict("grant id already exists")
            cx.execute(grants.insert().values(
                id=grant_id, principal=principal, actors=actors, provider=provider,
                resource_prefix=resource_prefix, action=action,
                expires_at=expires_at, revoked=False, revision=1, created_at=utcnow(),
            ))
            self.db.emit(cx, "grant:" + grant_id, "grant.issued", {
                "principal": principal, "provider": provider,
                "resource_prefix": resource_prefix, "action": action,
            })
        return self.get_grant(grant_id)

    def get_grant(self, grant_id: str) -> dict:
        with self.db.engine.connect() as cx:
            row = cx.execute(select(grants).where(grants.c.id == grant_id)).mappings().first()
            if row is None:
                raise UnknownOperation("unknown authority grant")
            return dict(row)

    def revoke_grant(self, grant_id: str) -> dict:
        with self.db.engine.begin() as cx:
            row = cx.execute(select(grants).where(grants.c.id == grant_id).with_for_update()).mappings().first()
            if row is None:
                raise UnknownOperation("unknown authority grant")
            if not row["revoked"]:
                cx.execute(update(grants).where(
                    grants.c.id == grant_id, grants.c.revision == row["revision"],
                ).values(revoked=True, revision=row["revision"] + 1))
                self.db.emit(cx, "grant:" + grant_id, "grant.revoked", {
                    "revision": row["revision"] + 1,
                })
        return self.get_grant(grant_id)

    def _assert_authority(self, cx, row: Mapping[str, Any], actor: str) -> None:
        grant = cx.execute(select(grants).where(
            grants.c.id == row["authority_id"],
        )).mappings().first()
        if grant is None or grant["revoked"]:
            raise Denied("missing or revoked authority")
        if grant["expires_at"] is not None:
            expiration = grant["expires_at"]
            if expiration.tzinfo is None:
                expiration = expiration.replace(tzinfo=timezone.utc)
            if expiration <= utcnow():
                raise Denied("authority expired")
        if (grant["principal"] != row["principal"] or actor not in grant["actors"]
            or row["provider"] != grant["provider"] or row["action"] != grant["action"]
            or not _scope_allows(grant["resource_prefix"], row["resource_ref"])):
            raise Denied("authority scope mismatch")

    def _assert_versions(self, cx, expected: Mapping[str, str]) -> None:
        for ref, revision in expected.items():
            current = cx.execute(select(observations.c.source_revision).where(
                observations.c.ref == ref,
            )).scalar_one_or_none()
            if current is None or current != revision:
                raise Held("recorded source version changed or is unknown: " + ref)

    def observe(
        self, *, actor: str, ref: str, source_revision: str, evidence_ref: str,
        expected_revision: int | None = None,
    ) -> dict:
        """An actor observation, NOT an authoritative external effect confirmation."""
        if not all([actor, ref, source_revision, evidence_ref]):
            raise ValueError("observation requires actor, ref, revision and evidence")
        with self.db.engine.begin() as cx:
            old = cx.execute(select(observations).where(
                observations.c.ref == ref,
            ).with_for_update()).mappings().first()
            prior = 0 if old is None else old["revision"]
            if expected_revision is not None and expected_revision != prior:
                raise Conflict("observation revision conflict")
            if old is None:
                cx.execute(observations.insert().values(
                    ref=ref, source_ref="actor:" + actor, source_revision=source_revision,
                    evidence_ref=evidence_ref, observed_at=utcnow(), revision=1,
                ))
            else:
                # Reporting actor cannot usurp ownership of an existing observation.
                if old["source_ref"] != "actor:" + actor:
                    raise Denied("observation owner mismatch")
                changed = cx.execute(update(observations).where(
                    observations.c.ref == ref, observations.c.revision == prior,
                ).values(
                    source_revision=source_revision, evidence_ref=evidence_ref,
                    observed_at=utcnow(), revision=prior + 1,
                ))
                if changed.rowcount != 1:
                    raise Conflict("concurrent observation revision")
            self.db.emit(cx, "observation:" + ref, "observation.recorded", {
                "source": "actor:" + actor, "source_revision": source_revision,
                "evidence_ref": evidence_ref, "revision": prior + 1,
            })
            if old is not None and old["source_revision"] != source_revision:
                # Only actual cross-boundary dependencies, no global graph scan.
                dependencies = cx.execute(select(links).where(links.c.active.is_(True))).mappings()
                for dep in dependencies:
                    if ref in dep["constraints"].get("depends_on", {}):
                        self.db.emit(cx, "link:" + dep["id"], "dependency.invalidated", {
                            "source_ref": ref, "previous_revision": old["source_revision"],
                            "new_revision": source_revision,
                        })
        return self.get_observation(ref)

    def get_observation(self, ref: str) -> dict:
        with self.db.engine.connect() as cx:
            row = cx.execute(select(observations).where(
                observations.c.ref == ref,
            )).mappings().first()
            if row is None:
                raise UnknownOperation("unknown observation")
            return dict(row)

    def link(
        self, *, link_id: str, actor: str, source_ref: str, target_ref: str,
        relation: str, purpose: str, basis_ref: str,
        constraints: dict | None = None, expected_revision: int = 0,
    ) -> dict:
        if purpose not in PURPOSES:
            raise ValueError("purpose must justify cross-boundary persistence")
        if not all([link_id, actor, source_ref, target_ref, relation, basis_ref]):
            raise ValueError("cross-boundary link requires identity, refs and basis")
        constraints = constraints or {}
        if not isinstance(constraints.get("depends_on", {}), dict):
            raise ValueError("depends_on must map reference to version")
        with self.db.engine.begin() as cx:
            self._assert_versions(cx, constraints.get("depends_on", {}))
            old = cx.execute(select(links).where(
                links.c.id == link_id,
            ).with_for_update()).mappings().first()
            old_rev = 0 if old is None else old["revision"]
            if old_rev != expected_revision:
                raise Conflict("link revision mismatch")
            record = dict(
                source_ref=source_ref, target_ref=target_ref, relation=relation,
                purpose=purpose, owner=actor, basis_ref=basis_ref,
                constraints=constraints, active=True,
            )
            if old is None:
                cx.execute(links.insert().values(id=link_id, revision=1, **record))
            else:
                if old["owner"] != actor:
                    raise Denied("link owner mismatch")
                changed = cx.execute(update(links).where(
                    links.c.id == link_id, links.c.revision == old_rev,
                ).values(revision=old_rev + 1, **record))
                if changed.rowcount != 1:
                    raise Conflict("concurrent link update")
            self.db.emit(cx, "link:" + link_id, "link.updated", {
                "revision": old_rev + 1, "purpose": purpose, "basis_ref": basis_ref,
            })
        return self.get_link(link_id)

    def get_link(self, link_id: str) -> dict:
        with self.db.engine.connect() as cx:
            row = cx.execute(select(links).where(links.c.id == link_id)).mappings().first()
            if row is None:
                raise UnknownOperation("unknown link")
            return dict(row)

    def submit(self, intent: Intent, *, actor: str, participants: list[str]) -> dict:
        if not all([intent.operation_id, intent.principal, intent.provider,
                    intent.resource_ref, intent.action, intent.authority_id]):
            raise ValueError("operation identity and authority are required")
        if actor not in participants or not participants or len(participants) != len(set(participants)):
            raise Denied("caller must be an explicit participant")
        fp = _fingerprint(intent)
        with self.db.engine.begin() as cx:
            old = cx.execute(select(operations).where(
                operations.c.id == intent.operation_id,
            ).with_for_update()).mappings().first()
            if old is not None:
                if old["fingerprint"] != fp or old["participants"] != participants:
                    raise Conflict("operation identity rebound")
                if actor not in old["participants"]:
                    raise Denied("not an operation participant")
                return dict(old)
            row = dict(
                id=intent.operation_id, fingerprint=fp, principal=intent.principal,
                provider=intent.provider, resource_ref=intent.resource_ref,
                action=intent.action, payload=intent.payload,
                authority_id=intent.authority_id, expected_versions=intent.expected_versions,
                participants=participants,
            )
            self._assert_authority(cx, row, actor)
            grant = cx.execute(select(grants).where(
                grants.c.id == intent.authority_id,
            )).mappings().one()
            if not set(participants).issubset(set(grant["actors"])):
                raise Denied("participant is not covered by authority")
            cx.execute(operations.insert().values(
                **row, status="registered", dispatch_generation=0,
                effect_disposition="unverified", outcome_disposition="unverified",
                created_at=utcnow(), updated_at=utcnow(),
            ))
            self.db.emit(cx, "operation:" + intent.operation_id, "operation.registered", {
                "actor": actor, "fingerprint": fp, "authority_id": intent.authority_id,
            })
        return self.get_operation(intent.operation_id, actor=actor)

    def get_operation(self, operation_id: str, *, actor: str | None = None) -> dict:
        with self.db.engine.connect() as cx:
            row = cx.execute(select(operations).where(
                operations.c.id == operation_id,
            )).mappings().first()
            if row is None:
                raise UnknownOperation("unknown operation")
            if actor is not None and actor not in row["participants"]:
                raise Denied("not an operation participant")
            return dict(row)

    def dispatch(self, operation_id: str, *, actor: str) -> dict:
        """Evaluate admission outside DB locks; atomically revalidate before dispatch.

        A durable MAY_HAVE_BEEN_SENT reservation precedes provider I/O.
        Lost acknowledgments and crashes must never cause blind redispatch.
        """
        # Preflight: bounded reads without holding a transaction during a
        # potentially expensive external/modeled admission evaluator.
        with self.db.engine.connect() as cx:
            row = cx.execute(select(operations).where(
                operations.c.id == operation_id,
            )).mappings().first()
            if row is None:
                raise UnknownOperation("unknown operation")
            if actor not in row["participants"]:
                raise Denied("not an operation participant")
            if row["status"] != "registered":
                return dict(row)
            self._assert_authority(cx, row, actor)
            self._assert_versions(cx, row["expected_versions"])
            intent = _intent(row)
            seen_versions = {}
            for ref in row["expected_versions"]:
                seen_versions[ref] = cx.execute(select(observations.c.revision).where(
                    observations.c.ref == ref,
                )).scalar_one()
        adapter = self.adapters.get(intent.provider)
        if adapter is None:
            raise Denied("no trusted executor registered for provider")
        decision = self.policy.evaluate(intent, dict(row["expected_versions"]))
        if decision.decision not in ("admit", "hold", "deny"):
            raise Denied("invalid admission decision")
        if decision.decision != "admit":
            with self.db.engine.begin() as cx:
                current = cx.execute(select(operations.c.status).where(
                    operations.c.id == operation_id,
                )).scalar_one_or_none()
                if current == "registered":
                    self.db.emit(cx, "operation:" + operation_id,
                                 "admission." + decision.decision, {
                        "reason": decision.reason, "policy_version": decision.policy_version,
                    })
            if decision.decision == "hold":
                raise Held(decision.reason or "admission held")
            raise Denied(decision.reason or "admission denied")

        with self.db.engine.begin() as cx:
            current = cx.execute(select(operations).where(
                operations.c.id == operation_id,
            ).with_for_update()).mappings().one()
            if actor not in current["participants"]:
                raise Denied("not an operation participant")
            if current["status"] != "registered":
                return dict(current)
            if current["fingerprint"] != row["fingerprint"]:
                raise Conflict("operation was rebound during admission")
            self._assert_authority(cx, current, actor)
            self._assert_versions(cx, current["expected_versions"])
            for ref, revision in seen_versions.items():
                latest = cx.execute(select(observations.c.revision).where(
                    observations.c.ref == ref,
                )).scalar_one()
                if latest != revision:
                    raise Held("admission evidence changed; evaluate again: " + ref)
            updated = cx.execute(update(operations).where(
                operations.c.id == operation_id,
                operations.c.status == "registered",
            ).values(
                status="may_have_been_sent", dispatch_generation=1,
                admission=asdict(decision), updated_at=utcnow(),
            ))
            if updated.rowcount != 1:
                raise Conflict("dispatch race lost")
            self.db.emit(cx, "operation:" + operation_id, "dispatch.reserved", {
                "generation": 1, "actor": actor,
                "policy_version": decision.policy_version,
            })
        # No database locks may be held across an external provider call.
        try:
            receipt = adapter.execute(intent)
            status = "pending_verification" if receipt.accepted else "unknown"
            receipt_ref = receipt.receipt_ref
        except Exception:
            status = "unknown"
            receipt_ref = None
        with self.db.engine.begin() as cx:
            updated = cx.execute(update(operations).where(
                operations.c.id == operation_id,
                operations.c.status == "may_have_been_sent",
                operations.c.dispatch_generation == 1,
            ).values(status=status, receipt_ref=receipt_ref, updated_at=utcnow()))
            if updated.rowcount == 1:
                self.db.emit(cx, "operation:" + operation_id, "dispatch.reported", {
                    "status": status, "generation": 1, "receipt_ref": receipt_ref,
                })
        return self.get_operation(operation_id, actor=actor)

    def reconcile(self, operation_id: str, *, actor: str) -> dict:
        """External readback may confirm effect, but never silently asserts domain outcome."""
        row = self.get_operation(operation_id, actor=actor)
        if row["status"] not in ("may_have_been_sent", "unknown", "pending_verification"):
            return row
        adapter = self.adapters.get(row["provider"])
        if adapter is None:
            raise Denied("historical provider unavailable; manual resolution required")
        try:
            readback = adapter.readback(_intent(row))
        except Exception:
            readback = Readback("unknown")
        if readback.disposition == "present" and not readback.evidence_ref:
            readback = Readback("unknown")
        if readback.disposition == "absent" and (
            not readback.reliable_no_effect or not readback.evidence_ref
        ):
            readback = Readback("unknown")
        with self.db.engine.begin() as cx:
            current = cx.execute(select(operations).where(
                operations.c.id == operation_id,
            ).with_for_update()).mappings().one()
            if current["status"] not in ("may_have_been_sent", "unknown", "pending_verification"):
                return dict(current)
            if readback.disposition == "present":
                next_status, effect = "effect_confirmed", "present"
            elif readback.disposition == "absent":
                next_status, effect = "effect_absent", "absent"
            else:
                next_status, effect = "unknown", "unknown"
            cx.execute(update(operations).where(
                operations.c.id == operation_id,
                operations.c.status == current["status"],
            ).values(
                status=next_status, effect_disposition=effect,
                effect_evidence_ref=readback.evidence_ref, updated_at=utcnow(),
            ))
            self.db.emit(cx, "operation:" + operation_id, "effect.reconciled", {
                "effect": effect, "evidence_ref": readback.evidence_ref,
            })
        return self.get_operation(operation_id, actor=actor)

    def verify_outcome(self, operation_id: str, *, actor: str) -> dict:
        row = self.get_operation(operation_id, actor=actor)
        if row["effect_disposition"] != "present":
            raise Held("external effect is not independently confirmed")
        verifier = self.verifiers.get(row["provider"])
        if verifier is None:
            raise Held("domain verifier is not registered")
        result = verifier.verify(_intent(row), Readback(
            "present", row["effect_evidence_ref"],
        ))
        if result.satisfied is None or not result.evidence_ref:
            raise Held("domain outcome remains unverified")
        with self.db.engine.begin() as cx:
            current = cx.execute(select(operations).where(
                operations.c.id == operation_id,
            ).with_for_update()).mappings().one()
            if current["status"] != "effect_confirmed":
                return dict(current)
            outcome = "satisfied" if result.satisfied else "not_satisfied"
            cx.execute(update(operations).where(
                operations.c.id == operation_id, operations.c.status == "effect_confirmed",
            ).values(
                outcome_disposition=outcome,
                outcome_evidence_ref=result.evidence_ref, updated_at=utcnow(),
            ))
            self.db.emit(cx, "operation:" + operation_id, "outcome.verified", {
                "outcome": outcome, "evidence_ref": result.evidence_ref,
            })
        return self.get_operation(operation_id, actor=actor)

    def events(self, after: int = 0, limit: int = 100) -> list[dict]:
        return self.db.event_stream(after=after, limit=limit)

    def events_for_actor(self, actor: str, after: int = 0, limit: int = 100) -> list[dict]:
        """A bounded event cursor; never expose another actor's event payloads."""
        candidates = self.db.event_stream(after=after, limit=limit)
        authorized = []
        with self.db.engine.connect() as cx:
            for event in candidates:
                entity = event["entity_ref"]
                if entity.startswith("operation:"):
                    row = cx.execute(select(operations.c.participants).where(
                        operations.c.id == entity[len("operation:"):],
                    )).scalar_one_or_none()
                    if row is not None and actor in row:
                        authorized.append(event)
                elif entity.startswith("link:"):
                    owner = cx.execute(select(links.c.owner).where(
                        links.c.id == entity[len("link:"):],
                    )).scalar_one_or_none()
                    if owner == actor:
                        authorized.append(event)
                elif entity.startswith("observation:"):
                    source = cx.execute(select(observations.c.source_ref).where(
                        observations.c.ref == entity[len("observation:"):],
                    )).scalar_one_or_none()
                    if source == "actor:" + actor:
                        authorized.append(event)
        return authorized
