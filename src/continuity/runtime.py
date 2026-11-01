"""Narrow cross-boundary coordination. Domain truth and workflow history stay outside."""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Mapping

from sqlalchemy import delete, select, update
from sqlalchemy.exc import IntegrityError

from .contracts import (
    AdmissionPolicy, BaselinePolicy, Conflict, Denied, DomainVerifier,
    EffectAdapter, Held, Intent, Readback, UnknownOperation, VersionAuthority,
)
from .db import (
    RuntimeDB, consumer_offsets, grants, link_dependencies, links,
    observations, operations, utcnow,
)

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
        effect_key=row["effect_key"],
    )


def _scope_allows(prefix: str, resource: str) -> bool:
    # No ambiguous prefix matches: "service/1" never matches "service/10".
    return prefix == "*" or resource == prefix or resource.startswith(prefix.rstrip("/") + "/")


class ContinuityRuntime:
    def __init__(
        self, db: RuntimeDB, *, adapters: dict[str, EffectAdapter] | None = None,
        policy: AdmissionPolicy | None = None,
        verifiers: dict[str, DomainVerifier] | None = None,
        version_authorities: dict[str, VersionAuthority] | None = None,
    ) -> None:
        self.db = db
        self.adapters = dict(adapters or {})
        self.policy = policy or BaselinePolicy()
        self.verifiers = dict(verifiers or {})
        # Caller-controlled observations can never alone authorize reality-facing writes.
        # An independent trusted source reader must cover every guarded version ref.
        self.version_authorities = dict(version_authorities or {})

    def issue_grant(
        self, *, grant_id: str, principal: str, actors: list[str], provider: str,
        resource_prefix: str, action: str, expires_at: datetime | None = None,
        required_version_refs: list[str] | None = None,
    ) -> dict:
        if not all([grant_id, principal, actors, provider, resource_prefix, action]):
            raise ValueError("grant fields and actors are required")
        if len(set(actors)) != len(actors) or any(not a for a in actors):
            raise ValueError("grant actors must be distinct and nonempty")
        required_version_refs = required_version_refs or []
        if len(set(required_version_refs)) != len(required_version_refs) or any(
            not isinstance(ref, str) or not ref for ref in required_version_refs
        ):
            raise ValueError("required version refs must be distinct and nonempty")
        with self.db.engine.begin() as cx:
            if cx.execute(select(grants.c.id).where(grants.c.id == grant_id)).first():
                raise Conflict("grant id already exists")
            cx.execute(grants.insert().values(
                id=grant_id, principal=principal, actors=actors,
                required_version_refs=required_version_refs, provider=provider,
                resource_prefix=resource_prefix, action=action,
                expires_at=expires_at, revoked=False, revision=1, created_at=utcnow(),
            ))
            self.db.emit(cx, "grant:" + grant_id, "grant.issued", {
                "principal": principal, "provider": provider,
                "resource_prefix": resource_prefix, "action": action,
                "required_version_refs": required_version_refs,
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

    def _assert_authority(
        self, cx, row: Mapping[str, Any], actor: str, *, lock: bool = False,
    ) -> None:
        stmt = select(grants).where(grants.c.id == row["authority_id"])
        if lock:
            stmt = stmt.with_for_update()
        grant = cx.execute(stmt).mappings().first()
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
        required = set(grant["required_version_refs"] or [])
        if not required.issubset(row["expected_versions"].keys()):
            raise Denied("operation omitted authority-required version preconditions")

    def _assert_versions(
        self, cx, expected: Mapping[str, str], *, lock: bool = False,
    ) -> None:
        for ref, revision in sorted(expected.items()):
            stmt = select(observations.c.source_revision).where(observations.c.ref == ref)
            if lock:
                stmt = stmt.with_for_update(read=True)
            current = cx.execute(stmt).scalar_one_or_none()
            if current is None or current != revision:
                raise Held("recorded source version changed or is unknown: " + ref)

    def _assert_authoritative_versions(self, expected: Mapping[str, str]) -> None:
        """Fail closed unless a trusted adapter confirms every guarded source version.

        This external check is not an atomic lock on the remote resource. Where
        needed, the provider must enforce a conditional write on its own side.
        """
        for ref, revision in expected.items():
            authority = self.version_authorities.get(ref)
            if authority is None:
                raise Held("no trusted version authority registered: " + ref)
            try:
                current = authority()
            except Exception as exc:
                raise Held("source version authority unavailable: " + ref) from exc
            if current is None or current != revision:
                raise Held("authoritative version changed or is unknown: " + ref)

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
                # Indexed lookup: only links explicitly dependent on this ref.
                affected = cx.execute(
                    select(links.c.id, links.c.revision)
                    .join(link_dependencies, links.c.id == link_dependencies.c.link_id)
                    .where(
                        link_dependencies.c.dependency_ref == ref,
                        link_dependencies.c.required_version != source_revision,
                        links.c.active.is_(True),
                    )
                ).all()
                for link_id, link_revision in affected:
                    updated = cx.execute(update(links).where(
                        links.c.id == link_id,
                        links.c.revision == link_revision,
                        links.c.active.is_(True),
                    ).values(active=False, revision=link_revision + 1))
                    if updated.rowcount != 1:
                        raise Conflict("concurrent link invalidation; retry observation")
                    self.db.emit(cx, "link:" + link_id, "dependency.invalidated", {
                        "source_ref": ref, "previous_revision": old["source_revision"],
                        "new_revision": source_revision,
                        "revision": link_revision + 1,
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
            cx.execute(delete(link_dependencies).where(link_dependencies.c.link_id == link_id))
            for dep_ref, dep_version in constraints.get("depends_on", {}).items():
                cx.execute(link_dependencies.insert().values(
                    link_id=link_id, dependency_ref=dep_ref, required_version=dep_version,
                ))
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
        if intent.effect_key is not None and not intent.effect_key.strip():
            raise ValueError("effect_key must be nonempty when provided")
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
                id=intent.operation_id, fingerprint=fp,
                effect_key=intent.effect_key, principal=intent.principal,
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
            try:
                cx.execute(operations.insert().values(
                    **row, status="registered", dispatch_generation=0,
                    effect_disposition="unverified", outcome_disposition="unverified",
                    created_at=utcnow(), updated_at=utcnow(),
                ))
            except IntegrityError as exc:
                raise Conflict("operation ID or logical effect key already reserved") from exc
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
        self._assert_authoritative_versions(row["expected_versions"])
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

        # Recheck after an arbitrarily slow admission, *before* taking the row lock.
        self._assert_authoritative_versions(row["expected_versions"])
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
            # Serialize with local revocation and local source revisions until
            # dispatch reservation commits. External API writes are still not atomic.
            self._assert_authority(cx, current, actor, lock=True)
            self._assert_versions(cx, current["expected_versions"], lock=True)
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
            if current["status"] == "may_have_been_sent" and readback.disposition != "present":
                # The original dispatcher may still be executing. Even an
                # authoritative absence at this instant cannot rule out a
                # write about to complete. Do not overwrite its in-flight state.
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

    def recovery_queue(self, *, limit: int = 100) -> list[dict]:
        """Read-only operational worklist; it never authorizes redispatch."""
        bounded = max(1, min(limit, 500))
        with self.db.engine.connect() as cx:
            pending = cx.execute(
                select(
                    operations.c.id, operations.c.provider, operations.c.resource_ref,
                    operations.c.status, operations.c.dispatch_generation,
                    operations.c.effect_disposition, operations.c.outcome_disposition,
                    operations.c.updated_at,
                ).where(
                    (operations.c.status.in_((
                        "may_have_been_sent", "unknown", "pending_verification",
                    ))) | (
                        (operations.c.status == "effect_confirmed") &
                        (operations.c.outcome_disposition == "unverified")
                    )
                ).order_by(operations.c.updated_at, operations.c.id).limit(bounded)
            ).mappings().all()
            return [dict(x) for x in pending]

    def events(self, after: int = 0, limit: int = 100) -> list[dict]:
        return self.db.event_stream(after=after, limit=limit)

    def events_for_actor(self, actor: str, after: int = 0, limit: int = 100) -> dict:
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
        return {"events": authorized, "next_seq": candidates[-1]["seq"] if candidates else after}


    def subscribe(self, subscription_id: str, *, actor: str) -> dict:
        """A durable cursor for a named actor-owned consumer, not a broker lease."""
        if not subscription_id or not actor:
            raise ValueError("subscription ID and actor required")
        with self.db.engine.begin() as cx:
            row = cx.execute(select(consumer_offsets).where(
                consumer_offsets.c.id == subscription_id,
            ).with_for_update()).mappings().first()
            if row is None:
                try:
                    cx.execute(consumer_offsets.insert().values(
                        id=subscription_id, actor=actor, last_acked_seq=0,
                        last_offered_seq=0, revision=1,
                    ))
                except IntegrityError as exc:
                    raise Conflict("subscription already reserved") from exc
            elif row["actor"] != actor:
                raise Denied("subscription owned by a different actor")
        return self.subscription(subscription_id, actor=actor)

    def subscription(self, subscription_id: str, *, actor: str) -> dict:
        with self.db.engine.connect() as cx:
            row = cx.execute(select(consumer_offsets).where(
                consumer_offsets.c.id == subscription_id,
            )).mappings().first()
            if row is None:
                raise UnknownOperation("unknown subscription")
            if row["actor"] != actor:
                raise Denied("subscription owned by a different actor")
            return dict(row)

    def poll_subscription(
        self, subscription_id: str, *, actor: str, limit: int = 100,
    ) -> dict:
        # Duplicate delivery is possible after a crash or across workers.
        # Each consumer must apply idempotent processing and ack only afterwards.
        with self.db.engine.begin() as cx:
            row = cx.execute(select(consumer_offsets).where(
                consumer_offsets.c.id == subscription_id,
            ).with_for_update()).mappings().first()
            if row is None:
                raise UnknownOperation("unknown subscription")
            if row["actor"] != actor:
                raise Denied("subscription owned by a different actor")
            if row["last_offered_seq"] > row["last_acked_seq"]:
                # An outstanding batch is immutable until ack. Polling with a
                # different limit cannot silently expand what the actor can ack.
                batch = self.events_for_actor(
                    actor, after=row["last_acked_seq"], limit=500,
                )
                offered = row["last_offered_seq"]
                visible = [event for event in batch["events"] if event["seq"] <= offered]
            else:
                batch = self.events_for_actor(
                    actor, after=row["last_acked_seq"], limit=limit,
                )
                offered = batch["next_seq"]
                visible = batch["events"]
                cx.execute(update(consumer_offsets).where(
                    consumer_offsets.c.id == subscription_id,
                ).values(last_offered_seq=offered))
            return {
                "subscription_id": subscription_id,
                "revision": row["revision"],
                "last_acked_seq": row["last_acked_seq"],
                "next_seq": offered,
                "events": visible,
            }

    def ack_subscription(
        self, subscription_id: str, *, actor: str, expected_revision: int,
        next_seq: int,
    ) -> dict:
        with self.db.engine.begin() as cx:
            row = cx.execute(select(consumer_offsets).where(
                consumer_offsets.c.id == subscription_id,
            ).with_for_update()).mappings().first()
            if row is None:
                raise UnknownOperation("unknown subscription")
            if row["actor"] != actor:
                raise Denied("subscription owned by a different actor")
            if expected_revision != row["revision"]:
                raise Conflict("consumer checkpoint revision conflict")
            if next_seq != row["last_offered_seq"]:
                raise Conflict("ack must match the exact outstanding batch boundary")
            if next_seq != row["last_acked_seq"]:
                cx.execute(update(consumer_offsets).where(
                    consumer_offsets.c.id == subscription_id,
                    consumer_offsets.c.revision == expected_revision,
                ).values(last_acked_seq=next_seq, revision=expected_revision + 1))
        return self.subscription(subscription_id, actor=actor)
