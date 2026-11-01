"""Independent PostgreSQL coordinator for the Phase C baseline arm.

This module owns its schema and state machine. It does not reuse any
application-coordinator modules or tables. It is for controlled local
experiments only; it is not a provider or a trial runner.
"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    Integer,
    MetaData,
    String,
    Table,
    UniqueConstraint,
    create_engine,
    select,
    update,
)
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _canonical(value: Any) -> str:
    return json.dumps(
        value, sort_keys=True, ensure_ascii=False, separators=(",", ":"),
        allow_nan=False,
    )


def _fingerprint(intent: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical(dict(intent)).encode("utf-8")).hexdigest()


def _scope_allows(prefix: str, resource: str) -> bool:
    """Match exact resources or slash-delimited descendants, not raw prefixes."""
    return prefix == "*" or resource == prefix or resource.startswith(prefix.rstrip("/") + "/")


def _field(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)


def _datetime_value(value: Any) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, datetime):
        raise ValueError("expires_at must be a datetime or None")
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _dict_row(row: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    value = dict(row)
    for key in ("created_at", "updated_at", "expires_at", "observed_at"):
        item = value.get(key)
        if isinstance(item, datetime):
            value[key] = item.isoformat()
    return value


class BaselineError(Exception):
    """Base class for deterministic baseline-arm failures."""


class BaselineConflict(BaselineError):
    """The caller attempted to rebind an identity or reserve a duplicate key."""


class BaselineDenied(BaselineError):
    """The caller or intent is outside the registered authority."""


class BaselineHeld(BaselineError):
    """A required authoritative fact is unavailable or cannot be verified."""


class BaselineUnknownOperation(BaselineError):
    """The requested operation or grant does not exist."""


class BaselineVersionChanged(BaselineHeld):
    """The injected authoritative source no longer has the expected version."""


@dataclass(frozen=True)
class ReadbackEvidence:
    """Small local evidence object passed to the independent domain verifier."""

    disposition: str
    evidence_ref: str | None
    reliable_no_effect: bool
    value: int | None = None


_metadata = MetaData()
_grants = Table(
    "phasec_baseline_grants", _metadata,
    Column("id", String(160), primary_key=True),
    Column("principal", String(160), nullable=False),
    Column("actors", JSON, nullable=False),
    Column("required_version_refs", JSON, nullable=False),
    Column("provider", String(160), nullable=False),
    Column("resource_prefix", String(512), nullable=False),
    Column("action", String(160), nullable=False),
    Column("expires_at", DateTime(timezone=True)),
    Column("revoked", Boolean, nullable=False, default=False),
    Column("revision", Integer, nullable=False, default=1),
    Column("created_at", DateTime(timezone=True), nullable=False),
)
_versions = Table(
    "phasec_baseline_source_versions", _metadata,
    Column("ref", String(512), primary_key=True),
    Column("source_revision", String(200), nullable=False),
    Column("evidence_ref", String(1024), nullable=False),
    Column("observed_at", DateTime(timezone=True), nullable=False),
    Column("revision", Integer, nullable=False, default=1),
)
_operations = Table(
    "phasec_baseline_operations", _metadata,
    Column("id", String(160), primary_key=True),
    Column("effect_key", String(256), nullable=False),
    Column("fingerprint", String(64), nullable=False),
    Column("principal", String(160), nullable=False),
    Column("provider", String(160), nullable=False),
    Column("resource_ref", String(512), nullable=False),
    Column("action", String(160), nullable=False),
    Column("payload", JSON, nullable=False),
    Column("authority_id", String(160), nullable=False),
    Column("expected_versions", JSON, nullable=False),
    Column("participants", JSON, nullable=False),
    Column("status", String(40), nullable=False),
    Column("dispatch_generation", Integer, nullable=False, default=0),
    Column("receipt_ref", String(1024)),
    Column("effect_disposition", String(40), nullable=False, default="unverified"),
    Column("effect_evidence_ref", String(1024)),
    Column("effect_value", Integer),
    Column("outcome_disposition", String(40), nullable=False, default="unverified"),
    Column("outcome_evidence_ref", String(1024)),
    Column("reason", String(512)),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint(
        "principal", "provider", "resource_ref", "effect_key",
        name="uq_phasec_baseline_principal_provider_effect",
    ),
)
_events = Table(
    "phasec_baseline_events", _metadata,
    Column("seq", Integer, primary_key=True, autoincrement=True),
    Column("entity_ref", String(512), nullable=False),
    Column("kind", String(120), nullable=False),
    Column("payload", JSON, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)


class BaselineRuntime:
    """Independent SQLAlchemy coordinator for a DBOS-wrapped baseline.

    Adapters map provider names to objects with execute(intent_dict) and
    readback(intent_dict). Verifiers map provider names to independent objects
    with verify(intent_dict, readback_evidence). Version readers map source
    refs to trusted zero-argument functions that return current revisions.

    after_reservation is a per-dispatch fault barrier. It is called only after
    the reservation transaction commits and before execute is called. If the
    callback raises or the worker is killed there, the durable may-have-been-sent
    state remains; a later dispatch cannot resend.
    """

    _INTENT_FIELDS = frozenset({
        "operation_id", "principal", "provider", "resource_ref", "action",
        "payload", "authority_id", "expected_versions", "effect_key",
    })

    def __init__(
        self,
        database_url: str,
        *,
        adapters: Mapping[str, Any],
        verifiers: Mapping[str, Any],
        version_readers: Mapping[str, Callable[[], str | None]],
        create_schema: bool = True,
    ) -> None:
        if make_url(database_url).get_backend_name() != "postgresql":
            raise ValueError("the baseline requires PostgreSQL; SQLite is unsupported")
        self.engine = create_engine(database_url, pool_pre_ping=True)
        self.adapters = dict(adapters)
        self.verifiers = dict(verifiers)
        self.version_readers = dict(version_readers)
        if create_schema:
            _metadata.create_all(self.engine)

    @staticmethod
    def _emit(cx: Any, entity_ref: str, kind: str, payload: dict[str, Any]) -> None:
        cx.execute(_events.insert().values(
            entity_ref=entity_ref, kind=kind, payload=payload, created_at=_utcnow(),
        ))

    @staticmethod
    def _json_mapping(value: Any, name: str) -> dict[str, Any]:
        if not isinstance(value, Mapping):
            raise ValueError(name + " must be a mapping")
        copied = dict(value)
        try:
            _canonical(copied)
        except (TypeError, ValueError) as exc:
            raise ValueError(name + " must be JSON serializable") from exc
        return copied

    def _read_authoritative_version(self, ref: str) -> str:
        reader = self.version_readers.get(ref)
        if reader is None:
            raise BaselineHeld("no trusted version reader for " + ref)
        try:
            value = reader()
        except Exception as exc:
            raise BaselineHeld("trusted version reader failed for " + ref) from exc
        if not isinstance(value, str) or not value:
            raise BaselineHeld("trusted version reader returned no revision for " + ref)
        return value

    def _assert_authoritative_versions(self, expected: Mapping[str, str]) -> None:
        for ref, revision in sorted(expected.items()):
            if self._read_authoritative_version(ref) != revision:
                raise BaselineVersionChanged("authoritative source version changed: " + ref)

    @staticmethod
    def _assert_local_versions(cx: Any, expected: Mapping[str, str]) -> None:
        for ref, revision in sorted(expected.items()):
            current = cx.execute(
                select(_versions.c.source_revision)
                .where(_versions.c.ref == ref)
                .with_for_update()
            ).scalar_one_or_none()
            if current is None or current != revision:
                raise BaselineVersionChanged("recorded source version changed: " + ref)

    def issue_grant(
        self,
        *,
        grant_id: str,
        principal: str,
        actors: Sequence[str],
        provider: str,
        resource_prefix: str,
        action: str,
        required_version_refs: Sequence[str],
        expires_at: datetime | None = None,
    ) -> dict[str, Any]:
        actors = list(actors)
        required_refs = list(required_version_refs)
        if not all((grant_id, principal, actors, provider, resource_prefix, action)):
            raise ValueError("grant identity, scope, and actors are required")
        if any(not isinstance(actor, str) or not actor for actor in actors):
            raise ValueError("grant actors must be nonempty strings")
        if len(set(actors)) != len(actors):
            raise ValueError("grant actors must be distinct")
        if not required_refs or any(
            not isinstance(ref, str) or not ref for ref in required_refs
        ):
            raise ValueError("at least one nonempty required version ref is needed")
        if len(set(required_refs)) != len(required_refs):
            raise ValueError("required version refs must be distinct")
        if any(ref not in self.version_readers for ref in required_refs):
            raise ValueError("each required version needs a trusted reader")
        expiry = _datetime_value(expires_at)
        try:
            with self.engine.begin() as cx:
                existing = cx.execute(
                    select(_grants.c.id).where(_grants.c.id == grant_id)
                ).scalar_one_or_none()
                if existing is not None:
                    raise BaselineConflict("grant ID already exists")
                cx.execute(_grants.insert().values(
                    id=grant_id,
                    principal=principal,
                    actors=actors,
                    required_version_refs=required_refs,
                    provider=provider,
                    resource_prefix=resource_prefix,
                    action=action,
                    expires_at=expiry,
                    revoked=False,
                    revision=1,
                    created_at=_utcnow(),
                ))
                self._emit(cx, "grant:" + grant_id, "grant.issued", {
                    "principal": principal,
                    "actors": actors,
                    "provider": provider,
                    "resource_prefix": resource_prefix,
                    "action": action,
                    "required_version_refs": required_refs,
                })
        except IntegrityError as exc:
            raise BaselineConflict("grant ID already exists") from exc
        return self._get_grant(grant_id)

    def _get_grant(self, grant_id: str) -> dict[str, Any]:
        with self.engine.connect() as cx:
            row = cx.execute(
                select(_grants).where(_grants.c.id == grant_id)
            ).mappings().first()
        if row is None:
            raise BaselineUnknownOperation("unknown authority grant")
        return _dict_row(row) or {}

    def revoke_grant(self, grant_id: str) -> dict[str, Any]:
        with self.engine.begin() as cx:
            row = cx.execute(
                select(_grants)
                .where(_grants.c.id == grant_id)
                .with_for_update()
            ).mappings().first()
            if row is None:
                raise BaselineUnknownOperation("unknown authority grant")
            if not row["revoked"]:
                new_revision = row["revision"] + 1
                cx.execute(
                    update(_grants)
                    .where(_grants.c.id == grant_id, _grants.c.revision == row["revision"])
                    .values(revoked=True, revision=new_revision)
                )
                self._emit(cx, "grant:" + grant_id, "grant.revoked", {
                    "revision": new_revision,
                })
        return self._get_grant(grant_id)

    def observe_version(
        self,
        ref: str,
        source_revision: str,
        *,
        evidence_ref: str,
        expected_revision: int | None = None,
    ) -> dict[str, Any]:
        if not all((ref, source_revision, evidence_ref)):
            raise ValueError("version observation requires ref, revision, and evidence")
        if self._read_authoritative_version(ref) != source_revision:
            raise BaselineVersionChanged("supplied version is not authoritative: " + ref)
        with self.engine.begin() as cx:
            old = cx.execute(
                select(_versions)
                .where(_versions.c.ref == ref)
                .with_for_update()
            ).mappings().first()
            prior = 0 if old is None else old["revision"]
            if expected_revision is not None and expected_revision != prior:
                raise BaselineConflict("version observation revision conflict")
            next_revision = prior + 1
            values = {
                "source_revision": source_revision,
                "evidence_ref": evidence_ref,
                "observed_at": _utcnow(),
                "revision": next_revision,
            }
            if old is None:
                cx.execute(_versions.insert().values(ref=ref, **values))
            else:
                changed = cx.execute(
                    update(_versions)
                    .where(_versions.c.ref == ref, _versions.c.revision == prior)
                    .values(**values)
                )
                if changed.rowcount != 1:
                    raise BaselineConflict("concurrent version observation")
            self._emit(cx, "version:" + ref, "version.observed", {
                "source_revision": source_revision,
                "evidence_ref": evidence_ref,
                "revision": next_revision,
            })
        return self._get_version(ref)

    def _get_version(self, ref: str) -> dict[str, Any]:
        with self.engine.connect() as cx:
            row = cx.execute(
                select(_versions).where(_versions.c.ref == ref)
            ).mappings().first()
        if row is None:
            raise BaselineHeld("unknown source version: " + ref)
        return _dict_row(row) or {}

    @staticmethod
    def _scope_error(
        grant: Mapping[str, Any] | None,
        intent: Mapping[str, Any],
        actor: str,
        participants: Sequence[str],
    ) -> str | None:
        if grant is None:
            return "missing authority"
        if grant["revoked"]:
            return "authority revoked"
        expires_at = grant["expires_at"]
        if expires_at is not None:
            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=timezone.utc)
            if expires_at <= _utcnow():
                return "authority expired"
        if (
            grant["principal"] != intent["principal"]
            or grant["provider"] != intent["provider"]
            or grant["action"] != intent["action"]
            or not _scope_allows(grant["resource_prefix"], intent["resource_ref"])
        ):
            return "authority scope mismatch"
        allowed = set(grant["actors"])
        if actor not in allowed or not set(participants).issubset(allowed):
            return "caller or participant is outside authority"
        required = set(grant["required_version_refs"])
        if not required or not required.issubset(intent["expected_versions"]):
            return "operation omitted required source-version guard"
        return None

    def submit(
        self,
        intent: Mapping[str, Any],
        *,
        actor: str,
        participants: Sequence[str],
    ) -> dict[str, Any]:
        if not isinstance(intent, Mapping) or set(intent) != self._INTENT_FIELDS:
            raise ValueError("intent must contain exactly the baseline intent fields")
        copied = dict(intent)
        participants = list(participants)
        required_strings = (
            "operation_id", "principal", "provider", "resource_ref",
            "action", "authority_id", "effect_key",
        )
        if any(
            not isinstance(copied[name], str) or not copied[name]
            for name in required_strings
        ):
            raise ValueError("intent identity, scope, authority, and effect key are required")
        if copied["effect_key"].strip() != copied["effect_key"]:
            raise ValueError("effect_key must be a stable nonempty string")
        if (
            not participants
            or actor not in participants
            or len(set(participants)) != len(participants)
        ):
            raise BaselineDenied("caller must be a distinct explicit participant")
        if any(not isinstance(item, str) or not item for item in participants):
            raise ValueError("participants must be nonempty strings")
        if not isinstance(copied["payload"], Mapping):
            raise ValueError("payload must be a JSON mapping")
        copied["payload"] = dict(copied["payload"])
        copied["expected_versions"] = self._json_mapping(
            copied["expected_versions"], "expected_versions",
        )
        if not copied["expected_versions"] or any(
            not isinstance(ref, str) or not ref
            or not isinstance(revision, str) or not revision
            for ref, revision in copied["expected_versions"].items()
        ):
            raise ValueError("a nonempty string-valued source-version guard is required")
        if any(ref not in self.version_readers for ref in copied["expected_versions"]):
            raise BaselineDenied("no authoritative reader configured for expected version")
        _canonical(copied)
        fingerprint = _fingerprint(copied)

        with self.engine.begin() as cx:
            old = cx.execute(
                select(_operations)
                .where(_operations.c.id == copied["operation_id"])
                .with_for_update()
            ).mappings().first()
            if old is not None:
                if old["fingerprint"] != fingerprint or old["participants"] != participants:
                    raise BaselineConflict("operation identity rebound")
                if actor not in old["participants"]:
                    raise BaselineDenied("caller is not an operation participant")
                return _dict_row(old) or {}
            grant = cx.execute(
                select(_grants)
                .where(_grants.c.id == copied["authority_id"])
                .with_for_update()
            ).mappings().first()
            reason = self._scope_error(grant, copied, actor, participants)
            if reason:
                raise BaselineDenied(reason)
            self._assert_local_versions(cx, copied["expected_versions"])
            values = {
                "id": copied["operation_id"],
                "effect_key": copied["effect_key"],
                "fingerprint": fingerprint,
                "principal": copied["principal"],
                "provider": copied["provider"],
                "resource_ref": copied["resource_ref"],
                "action": copied["action"],
                "payload": copied["payload"],
                "authority_id": copied["authority_id"],
                "expected_versions": copied["expected_versions"],
                "participants": participants,
                "status": "registered",
                "dispatch_generation": 0,
                "receipt_ref": None,
                "effect_disposition": "unverified",
                "effect_evidence_ref": None,
                "effect_value": None,
                "outcome_disposition": "unverified",
                "outcome_evidence_ref": None,
                "reason": None,
                "created_at": _utcnow(),
                "updated_at": _utcnow(),
            }
            try:
                cx.execute(_operations.insert().values(**values))
            except IntegrityError as exc:
                raise BaselineConflict(
                    "operation ID or logical effect key already reserved"
                ) from exc
            self._emit(cx, "operation:" + copied["operation_id"], "operation.registered", {
                "actor": actor,
                "fingerprint": fingerprint,
                "authority_id": copied["authority_id"],
                "effect_key": copied["effect_key"],
            })
        return self.get_operation(copied["operation_id"], actor=actor)

    def get_grant(self, grant_id: str) -> dict[str, Any]:
        """Read one persisted grant without changing its revision."""
        return self._get_grant(grant_id)

    def get_version(self, ref: str) -> dict[str, Any]:
        """Read one persisted version observation without changing it."""
        return self._get_version(ref)

    def get_operation(
        self, operation_id: str, *, actor: str | None = None,
    ) -> dict[str, Any]:
        with self.engine.connect() as cx:
            row = cx.execute(
                select(_operations).where(_operations.c.id == operation_id)
            ).mappings().first()
        if row is None:
            raise BaselineUnknownOperation("unknown operation")
        if actor is not None and actor not in row["participants"]:
            raise BaselineDenied("caller is not an operation participant")
        return _dict_row(row) or {}

    def _mark_blocked(self, operation_id: str, actor: str, reason: str) -> dict[str, Any]:
        with self.engine.begin() as cx:
            row = cx.execute(
                select(_operations)
                .where(_operations.c.id == operation_id)
                .with_for_update()
            ).mappings().first()
            if row is None:
                raise BaselineUnknownOperation("unknown operation")
            if actor not in row["participants"]:
                raise BaselineDenied("caller is not an operation participant")
            if row["status"] == "registered":
                cx.execute(
                    update(_operations)
                    .where(
                        _operations.c.id == operation_id,
                        _operations.c.status == "registered",
                    )
                    .values(status="blocked", reason=reason, updated_at=_utcnow())
                )
                self._emit(cx, "operation:" + operation_id, "operation.blocked", {
                    "actor": actor, "reason": reason,
                })
        return self.get_operation(operation_id, actor=actor)

    def _block_in_transaction(
        self, cx: Any, operation_id: str, actor: str, reason: str,
    ) -> None:
        cx.execute(
            update(_operations)
            .where(
                _operations.c.id == operation_id,
                _operations.c.status == "registered",
            )
            .values(status="blocked", reason=reason, updated_at=_utcnow())
        )
        self._emit(cx, "operation:" + operation_id, "operation.blocked", {
            "actor": actor, "reason": reason,
        })

    def dispatch(
        self,
        operation_id: str,
        actor: str,
        *,
        after_reservation: Callable[[str], None] | None = None,
    ) -> dict[str, Any]:
        """Commit one reservation, then invoke the adapter without DB locks.

        Any status other than registered is terminal for dispatch. The
        after_reservation callback runs only after the reservation transaction
        commits and before provider I/O. If it raises or the worker is killed,
        may-have-been-sent persists and a later call cannot resend.
        """
        initial = self.get_operation(operation_id, actor=actor)
        if initial["status"] != "registered":
            return initial
        adapter = self.adapters.get(initial["provider"])
        if adapter is None:
            raise BaselineDenied("no adapter registered for provider")
        expected = dict(initial["expected_versions"])

        try:
            self._assert_authoritative_versions(expected)
        except BaselineVersionChanged as exc:
            return self._mark_blocked(operation_id, actor, str(exc))

        reserved = False
        blocked = False
        with self.engine.begin() as cx:
            current = cx.execute(
                select(_operations)
                .where(_operations.c.id == operation_id)
                .with_for_update()
            ).mappings().first()
            if current is None:
                raise BaselineUnknownOperation("unknown operation")
            if actor not in current["participants"]:
                raise BaselineDenied("caller is not an operation participant")
            if current["status"] != "registered":
                return _dict_row(current) or {}

            grant = cx.execute(
                select(_grants)
                .where(_grants.c.id == current["authority_id"])
                .with_for_update()
            ).mappings().first()
            reason = self._scope_error(grant, current, actor, current["participants"])
            if reason is None:
                try:
                    self._assert_local_versions(cx, current["expected_versions"])
                except BaselineVersionChanged as exc:
                    reason = str(exc)
            if reason is not None:
                self._block_in_transaction(cx, operation_id, actor, reason)
                blocked = True
            else:
                cx.execute(
                    update(_operations)
                    .where(
                        _operations.c.id == operation_id,
                        _operations.c.status == "registered",
                    )
                    .values(
                        status="may_have_been_sent",
                        dispatch_generation=1,
                        reason=None,
                        updated_at=_utcnow(),
                    )
                )
                self._emit(cx, "operation:" + operation_id, "dispatch.reserved", {
                    "actor": actor,
                    "generation": 1,
                })
                reserved = True

        if blocked:
            return self.get_operation(operation_id, actor=actor)
        if not reserved:
            return self.get_operation(operation_id, actor=actor)

        # This is outside engine.begin: a crash here leaves a durable reservation
        # and has not invoked the adapter.
        if after_reservation is not None:
            after_reservation(operation_id)

        intent = {
            "operation_id": initial["id"],
            "principal": initial["principal"],
            "provider": initial["provider"],
            "resource_ref": initial["resource_ref"],
            "action": initial["action"],
            "payload": dict(initial["payload"]),
            "authority_id": initial["authority_id"],
            "expected_versions": dict(initial["expected_versions"]),
            "effect_key": initial["effect_key"],
        }
        try:
            receipt = adapter.execute(intent)
        except Exception as exc:
            return self._finish_dispatch(
                operation_id,
                status="unknown",
                receipt_ref=None,
                reason="adapter exception: " + type(exc).__name__,
            )

        accepted = bool(_field(receipt, "accepted", False))
        receipt_ref = _field(receipt, "receipt_ref")
        if receipt_ref is not None and not isinstance(receipt_ref, str):
            receipt_ref = str(receipt_ref)
        return self._finish_dispatch(
            operation_id,
            status="pending_verification" if accepted else "unknown",
            receipt_ref=receipt_ref if accepted else None,
            reason=None if accepted else "adapter did not acknowledge acceptance",
        )

    def _finish_dispatch(
        self,
        operation_id: str,
        *,
        status: str,
        receipt_ref: str | None,
        reason: str | None,
    ) -> dict[str, Any]:
        with self.engine.begin() as cx:
            changed = cx.execute(
                update(_operations)
                .where(
                    _operations.c.id == operation_id,
                    _operations.c.status == "may_have_been_sent",
                    _operations.c.dispatch_generation == 1,
                )
                .values(
                    status=status,
                    receipt_ref=receipt_ref,
                    reason=reason,
                    updated_at=_utcnow(),
                )
            )
            if changed.rowcount == 1:
                self._emit(cx, "operation:" + operation_id, "dispatch.reported", {
                    "status": status,
                    "generation": 1,
                    "receipt_ref": receipt_ref,
                    "reason": reason,
                })
        return self.get_operation(operation_id)

    def reconcile(self, operation_id: str, actor: str) -> dict[str, Any]:
        row = self.get_operation(operation_id, actor=actor)
        eligible = {"may_have_been_sent", "unknown", "pending_verification"}
        if row["status"] not in eligible:
            return row
        adapter = self.adapters.get(row["provider"])
        if adapter is None:
            raise BaselineDenied("no adapter registered for provider")
        intent = {
            "operation_id": row["id"],
            "principal": row["principal"],
            "provider": row["provider"],
            "resource_ref": row["resource_ref"],
            "action": row["action"],
            "payload": dict(row["payload"]),
            "authority_id": row["authority_id"],
            "expected_versions": dict(row["expected_versions"]),
            "effect_key": row["effect_key"],
        }
        try:
            observed = adapter.readback(intent)
        except Exception:
            observed = None

        disposition = _field(observed, "disposition", "unknown")
        evidence_ref = _field(observed, "evidence_ref")
        reliable_no_effect = bool(_field(observed, "reliable_no_effect", False))
        value = _field(observed, "value")
        if value is not None and type(value) is not int:
            value = None
        if (
            disposition == "present"
            and isinstance(evidence_ref, str)
            and evidence_ref.strip()
        ):
            next_status = "effect_confirmed"
            next_effect = "present"
        else:
            # An absent or unavailable snapshot after reservation is not proof
            # that an in-flight or previously sent request had no effect.
            next_status = "unknown"
            next_effect = "unknown"
            evidence_ref = None
            value = None

        with self.engine.begin() as cx:
            current = cx.execute(
                select(_operations)
                .where(_operations.c.id == operation_id)
                .with_for_update()
            ).mappings().first()
            if current is None:
                raise BaselineUnknownOperation("unknown operation")
            if actor not in current["participants"]:
                raise BaselineDenied("caller is not an operation participant")
            if current["status"] not in eligible:
                return _dict_row(current) or {}
            cx.execute(
                update(_operations)
                .where(
                    _operations.c.id == operation_id,
                    _operations.c.status == current["status"],
                )
                .values(
                    status=next_status,
                    effect_disposition=next_effect,
                    effect_evidence_ref=evidence_ref,
                    effect_value=value,
                    reason=None if next_effect == "present" else "readback unavailable or ambiguous",
                    updated_at=_utcnow(),
                )
            )
            self._emit(cx, "operation:" + operation_id, "effect.reconciled", {
                "effect": next_effect,
                "evidence_ref": evidence_ref,
                "reliable_no_effect": reliable_no_effect,
            })
        return self.get_operation(operation_id, actor=actor)

    def verify_outcome(self, operation_id: str, actor: str) -> dict[str, Any]:
        row = self.get_operation(operation_id, actor=actor)
        if row["outcome_disposition"] != "unverified":
            return row
        if row["effect_disposition"] != "present" or not row["effect_evidence_ref"]:
            raise BaselineHeld("external effect is not independently confirmed")
        verifier = self.verifiers.get(row["provider"])
        if verifier is None:
            raise BaselineHeld("no independent verifier registered for provider")
        intent = {
            "operation_id": row["id"],
            "principal": row["principal"],
            "provider": row["provider"],
            "resource_ref": row["resource_ref"],
            "action": row["action"],
            "payload": dict(row["payload"]),
            "authority_id": row["authority_id"],
            "expected_versions": dict(row["expected_versions"]),
            "effect_key": row["effect_key"],
        }
        evidence = ReadbackEvidence(
            disposition="present",
            evidence_ref=row["effect_evidence_ref"],
            reliable_no_effect=False,
            value=row["effect_value"],
        )
        verify = getattr(verifier, "verify", verifier)
        outcome = verify(intent, evidence)
        satisfied = _field(outcome, "satisfied")
        evidence_ref = _field(outcome, "evidence_ref")
        if (
            type(satisfied) is not bool
            or not isinstance(evidence_ref, str)
            or not evidence_ref.strip()
        ):
            return row

        disposition = "satisfied" if satisfied else "not_satisfied"
        with self.engine.begin() as cx:
            current = cx.execute(
                select(_operations)
                .where(_operations.c.id == operation_id)
                .with_for_update()
            ).mappings().first()
            if current is None:
                raise BaselineUnknownOperation("unknown operation")
            if actor not in current["participants"]:
                raise BaselineDenied("caller is not an operation participant")
            if (
                current["effect_disposition"] != "present"
                or current["status"] != "effect_confirmed"
                or current["outcome_disposition"] != "unverified"
            ):
                return _dict_row(current) or {}
            changed = cx.execute(
                update(_operations)
                .where(
                    _operations.c.id == operation_id,
                    _operations.c.status == "effect_confirmed",
                    _operations.c.effect_disposition == "present",
                    _operations.c.outcome_disposition == "unverified",
                )
                .values(
                    outcome_disposition=disposition,
                    outcome_evidence_ref=evidence_ref,
                    updated_at=_utcnow(),
                )
            )
            if changed.rowcount != 1:
                raise BaselineConflict("concurrent outcome verification")
            self._emit(cx, "operation:" + operation_id, "outcome.verified", {
                "outcome": disposition,
                "evidence_ref": evidence_ref,
            })
        return self.get_operation(operation_id, actor=actor)

    def close(self) -> None:
        self.engine.dispose()
