"""Independent append-only baseline observations and source-dependent links.

This is a PostgreSQL+DBOS control-arm component. It intentionally imports no
Continuity package or table and makes no claim to be a Runtime primitive.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Callable, Mapping

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    delete,
    select,
    update,
)
from sqlalchemy.engine import Engine

_metadata = MetaData()
_observations = Table(
    "phase_d_baseline_observations",
    _metadata,
    Column("ref", String(512), primary_key=True),
    Column("owner", String(160), nullable=False),
    Column("source_revision", String(160), nullable=False),
    Column("evidence_ref", String(1024), nullable=False),
    Column("revision", Integer, nullable=False),
    Column("observed_at", DateTime(timezone=True), nullable=False),
)
_links = Table(
    "phase_d_baseline_links",
    _metadata,
    Column("id", String(256), primary_key=True),
    Column("owner", String(160), nullable=False),
    Column("source_ref", String(512), nullable=False),
    Column("target_ref", String(512), nullable=False),
    Column("relation", String(160), nullable=False),
    Column("purpose", String(160), nullable=False),
    Column("basis_ref", String(1024), nullable=False),
    Column("constraints", JSON, nullable=False),
    Column("active", Boolean, nullable=False),
    Column("revision", Integer, nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)
_dependencies = Table(
    "phase_d_baseline_link_dependencies",
    _metadata,
    Column("link_id", String(256), primary_key=True),
    Column("dependency_ref", String(512), primary_key=True),
    Column("required_version", String(160), nullable=False),
)
_grant_version_pins = Table(
    "phase_d_baseline_grant_version_pins",
    _metadata,
    Column("grant_id", String(256), primary_key=True),
    Column("action", String(160), nullable=False),
    Column("source_ref", String(512), nullable=False),
    Column("pinned_revision", String(160), nullable=False),
    Column("source_evidence_ref", String(1024), nullable=False),
    Column("domain_authority_ref", String(1024), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)
_events = Table(
    "phase_d_baseline_history_events",
    _metadata,
    Column("seq", Integer, primary_key=True, autoincrement=True),
    Column("entity_ref", String(512), nullable=False),
    Column("kind", String(120), nullable=False),
    Column("payload", JSON, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)


class BaselineHistoryError(Exception):
    """A baseline history invariant failed."""


class BaselineHistoryDenied(BaselineHistoryError):
    """The actor does not own a private observation/link."""


class BaselineHistoryConflict(BaselineHistoryError):
    """A revision or identity changed concurrently."""


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _emit(cx: Any, entity_ref: str, kind: str, payload: dict[str, Any]) -> int:
    result = cx.execute(_events.insert().values(
        entity_ref=entity_ref,
        kind=kind,
        payload=payload,
        created_at=_now(),
    ))
    return int(result.inserted_primary_key[0])


class BaselineHistory:
    """Equivalent source-history/link invalidation controls for the baseline."""

    def __init__(
        self,
        database_url: str | Engine,
        *,
        version_reader: Callable[[], str | None],
        create_schema: bool = True,
    ) -> None:
        self.engine = database_url if isinstance(database_url, Engine) else create_engine(
            database_url, pool_pre_ping=True,
        )
        self.version_reader = version_reader
        if create_schema:
            _metadata.create_all(self.engine)

    def observe(
        self,
        *,
        actor: str,
        ref: str,
        source_revision: str,
        evidence_ref: str,
        expected_revision: int | None = None,
    ) -> dict[str, Any]:
        if not all((actor, ref, source_revision, evidence_ref)):
            raise ValueError("observation identity, revision and evidence are required")
        if self.version_reader() != source_revision:
            raise BaselineHistoryError("reported source revision is not independently authoritative")
        with self.engine.begin() as cx:
            old = cx.execute(select(_observations).where(
                _observations.c.ref == ref,
            ).with_for_update()).mappings().first()
            prior_revision = 0 if old is None else int(old["revision"])
            if expected_revision is not None and expected_revision != prior_revision:
                raise BaselineHistoryConflict("observation revision mismatch")
            if old is not None and old["owner"] != actor:
                raise BaselineHistoryDenied("only the original observation owner may revise it")
            next_revision = prior_revision + 1
            values = {
                "owner": actor,
                "source_revision": source_revision,
                "evidence_ref": evidence_ref,
                "revision": next_revision,
                "observed_at": _now(),
            }
            if old is None:
                cx.execute(_observations.insert().values(ref=ref, **values))
            else:
                changed = cx.execute(update(_observations).where(
                    _observations.c.ref == ref,
                    _observations.c.revision == prior_revision,
                ).values(**values))
                if changed.rowcount != 1:
                    raise BaselineHistoryConflict("concurrent observation revision")
            observation_event = _emit(cx, "observation:" + ref, "observation.recorded", {
                "actor": actor,
                "source_revision": source_revision,
                "evidence_ref": evidence_ref,
                "revision": next_revision,
            })
            invalidated: list[dict[str, Any]] = []
            if old is not None and old["source_revision"] != source_revision:
                affected = cx.execute(
                    select(_links.c.id, _links.c.revision)
                    .join(_dependencies, _links.c.id == _dependencies.c.link_id)
                    .where(
                        _dependencies.c.dependency_ref == ref,
                        _dependencies.c.required_version != source_revision,
                        _links.c.active.is_(True),
                    )
                    .order_by(_links.c.id)
                    .with_for_update()
                ).all()
                for link_id, revision in affected:
                    changed = cx.execute(update(_links).where(
                        _links.c.id == link_id,
                        _links.c.revision == revision,
                        _links.c.active.is_(True),
                    ).values(active=False, revision=revision + 1, updated_at=_now()))
                    if changed.rowcount != 1:
                        raise BaselineHistoryConflict("concurrent link invalidation")
                    seq = _emit(cx, "link:" + link_id, "dependency.invalidated", {
                        "actor": actor,
                        "source_ref": ref,
                        "previous_version": old["source_revision"],
                        "new_version": source_revision,
                        "revision": revision + 1,
                    })
                    invalidated.append({"link_id": link_id, "event_seq": seq})
        return {
            "ref": ref,
            "owner": actor,
            "source_revision": source_revision,
            "evidence_ref": evidence_ref,
            "revision": next_revision,
            "event_seq": observation_event,
            "invalidated": invalidated,
        }

    def link(
        self,
        *,
        link_id: str,
        actor: str,
        source_ref: str,
        target_ref: str,
        relation: str,
        purpose: str,
        basis_ref: str,
        constraints: Mapping[str, Any],
        expected_revision: int = 0,
    ) -> dict[str, Any]:
        if not all((link_id, actor, source_ref, target_ref, relation, purpose, basis_ref)):
            raise ValueError("link identity, purpose and evidence are required")
        copied = json.loads(json.dumps(dict(constraints), sort_keys=True))
        required = copied.get("depends_on", {})
        if not isinstance(required, dict) or any(
            not isinstance(ref, str) or not ref
            or not isinstance(version, str) or not version
            for ref, version in required.items()
        ):
            raise ValueError("depends_on must map nonempty refs to versions")
        with self.engine.begin() as cx:
            for ref, version in required.items():
                current = cx.execute(select(_observations.c.source_revision).where(
                    _observations.c.ref == ref,
                )).scalar_one_or_none()
                if current != version:
                    raise BaselineHistoryError("link basis is not present in local observation history")
            old = cx.execute(select(_links).where(
                _links.c.id == link_id,
            ).with_for_update()).mappings().first()
            old_revision = 0 if old is None else int(old["revision"])
            if old_revision != expected_revision:
                raise BaselineHistoryConflict("link revision mismatch")
            if old is not None and old["owner"] != actor:
                raise BaselineHistoryDenied("link owner mismatch")
            new_revision = old_revision + 1
            values = {
                "owner": actor,
                "source_ref": source_ref,
                "target_ref": target_ref,
                "relation": relation,
                "purpose": purpose,
                "basis_ref": basis_ref,
                "constraints": copied,
                "active": True,
                "revision": new_revision,
                "updated_at": _now(),
            }
            if old is None:
                cx.execute(_links.insert().values(id=link_id, **values))
            else:
                cx.execute(update(_links).where(_links.c.id == link_id).values(**values))
            cx.execute(delete(_dependencies).where(_dependencies.c.link_id == link_id))
            for ref, version in required.items():
                cx.execute(_dependencies.insert().values(
                    link_id=link_id,
                    dependency_ref=ref,
                    required_version=version,
                ))
            event_seq = _emit(cx, "link:" + link_id, "link.updated", {
                "actor": actor,
                "revision": new_revision,
                "basis_ref": basis_ref,
                "depends_on": required,
            })
        return self.get_link(link_id, actor=actor) | {"event_seq": event_seq}

    def get_link(self, link_id: str, *, actor: str) -> dict[str, Any]:
        with self.engine.connect() as cx:
            row = cx.execute(select(_links).where(_links.c.id == link_id)).mappings().first()
        if row is None:
            raise KeyError("unknown baseline link")
        value = dict(row)
        if value["owner"] != actor:
            raise BaselineHistoryDenied("link owner mismatch")
        value["constraints"] = dict(value["constraints"])
        return value

    def pin_grant(
        self,
        *,
        grant_id: str,
        action: str,
        source_ref: str,
        pinned_revision: str,
        source_evidence_ref: str,
        domain_authority_ref: str,
    ) -> dict[str, Any]:
        if not all((grant_id, action, source_ref, pinned_revision,
                    source_evidence_ref, domain_authority_ref)):
            raise ValueError("grant version pin requires complete source and owner provenance")
        if self.version_reader() != pinned_revision:
            raise BaselineHistoryError("grant pin revision is not currently trusted")
        with self.engine.begin() as cx:
            if cx.execute(select(_grant_version_pins.c.grant_id).where(
                _grant_version_pins.c.grant_id == grant_id,
            )).scalar_one_or_none() is not None:
                raise BaselineHistoryConflict("grant version pin is immutable")
            cx.execute(_grant_version_pins.insert().values(
                grant_id=grant_id,
                action=action,
                source_ref=source_ref,
                pinned_revision=pinned_revision,
                source_evidence_ref=source_evidence_ref,
                domain_authority_ref=domain_authority_ref,
                created_at=_now(),
            ))
            event_seq = _emit(cx, "grant:" + grant_id, "grant.version_pinned", {
                "action": action,
                "source_ref": source_ref,
                "pinned_revision": pinned_revision,
                "source_evidence_ref": source_evidence_ref,
                "domain_authority_ref": domain_authority_ref,
            })
        return {"grant_id": grant_id, "action": action, "source_ref": source_ref,
                "pinned_revision": pinned_revision, "event_seq": event_seq}

    def assert_grant_version(
        self,
        *,
        grant_id: str,
        action: str,
        expected_versions: Mapping[str, str],
    ) -> None:
        with self.engine.connect() as cx:
            pin = cx.execute(select(_grant_version_pins).where(
                _grant_version_pins.c.grant_id == grant_id,
            )).mappings().first()
        if pin is None or pin["action"] != action:
            raise BaselineHistoryDenied("grant lacks an action-matched immutable source-version pin")
        if expected_versions.get(pin["source_ref"]) != pin["pinned_revision"]:
            raise BaselineHistoryDenied("operation expected version differs from owner-pinned grant revision")

    def events(self) -> list[dict[str, Any]]:
        with self.engine.connect() as cx:
            rows = cx.execute(select(_events).order_by(_events.c.seq)).mappings().all()
        return [dict(row) for row in rows]
