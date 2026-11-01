"""PostgreSQL-first relational state; SQLite is only for local demonstrations/tests.

State records are deliberately narrow. This is NOT a copied world database.
"""
from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import (
    JSON, Boolean, Column, DateTime, ForeignKey, Index, Integer, MetaData,
    String, Table, UniqueConstraint, create_engine, select,
)
from sqlalchemy.engine import Connection, Engine


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


metadata = MetaData()
grants = Table(
    "effect_grants", metadata,
    Column("id", String(160), primary_key=True),
    Column("principal", String(160), nullable=False),
    Column("actors", JSON, nullable=False),
    Column("provider", String(160), nullable=False),
    Column("resource_prefix", String(512), nullable=False),
    Column("action", String(160), nullable=False),
    Column("expires_at", DateTime(timezone=True)),
    Column("revoked", Boolean, nullable=False, default=False),
    Column("revision", Integer, nullable=False, default=1),
    Column("created_at", DateTime(timezone=True), nullable=False),
)
observations = Table(
    "source_observations", metadata,
    Column("ref", String(512), primary_key=True),
    Column("source_ref", String(512), nullable=False),
    Column("source_revision", String(200), nullable=False),
    Column("evidence_ref", String(1024), nullable=False),
    Column("observed_at", DateTime(timezone=True), nullable=False),
    Column("revision", Integer, nullable=False, default=1),
)
links = Table(
    "boundary_links", metadata,
    Column("id", String(160), primary_key=True),
    Column("source_ref", String(512), nullable=False),
    Column("target_ref", String(512), nullable=False),
    Column("relation", String(160), nullable=False),
    Column("purpose", String(40), nullable=False),
    Column("owner", String(160), nullable=False),
    Column("basis_ref", String(1024), nullable=False),
    Column("constraints", JSON, nullable=False),
    Column("revision", Integer, nullable=False, default=1),
    Column("active", Boolean, nullable=False, default=True),
)
link_dependencies = Table(
    "boundary_link_dependencies", metadata,
    Column("link_id", String(160), ForeignKey("boundary_links.id", ondelete="CASCADE"),
           primary_key=True),
    Column("dependency_ref", String(512), primary_key=True),
    Column("required_version", String(200), nullable=False),
)
Index("ix_dependency_ref", link_dependencies.c.dependency_ref)
operations = Table(
    "effect_operations", metadata,
    Column("id", String(160), primary_key=True),
    Column("effect_key", String(256)),
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
    Column("admission", JSON),
    Column("receipt_ref", String(1024)),
    Column("effect_disposition", String(40), nullable=False, default="unverified"),
    Column("effect_evidence_ref", String(1024)),
    Column("outcome_disposition", String(40), nullable=False, default="unverified"),
    Column("outcome_evidence_ref", String(1024)),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    Index("ix_operation_status", "status"),
    UniqueConstraint("principal", "provider", "resource_ref", "effect_key",
                     name="uq_effect_logical_key"),
)
events = Table(
    "boundary_events", metadata,
    Column("seq", Integer, primary_key=True, autoincrement=True),
    Column("entity_ref", String(512), nullable=False),
    Column("kind", String(160), nullable=False),
    Column("payload", JSON, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)
outbox = Table(
    "boundary_outbox", metadata,
    Column("seq", Integer, ForeignKey("boundary_events.seq"), primary_key=True),
    Column("published_at", DateTime(timezone=True)),
)
Index("ix_boundary_events_entity", events.c.entity_ref, events.c.seq)

consumer_offsets = Table(
    "boundary_consumer_offsets", metadata,
    Column("id", String(160), primary_key=True),
    Column("actor", String(160), nullable=False),
    Column("last_acked_seq", Integer, nullable=False, default=0),
    Column("last_offered_seq", Integer, nullable=False, default=0),
    Column("revision", Integer, nullable=False, default=1),
)


class RuntimeDB:
    def __init__(self, url: str, *, create_schema: bool = False) -> None:
        self.engine: Engine = create_engine(url, pool_pre_ping=True)
        if create_schema:
            metadata.create_all(self.engine)

    def emit(self, cx: Connection, entity: str, kind: str, payload: dict) -> int:
        result = cx.execute(events.insert().values(
            entity_ref=entity, kind=kind, payload=payload, created_at=utcnow(),
        ))
        # Works for PostgreSQL and SQLite when sequence is a single integer PK.
        seq = result.inserted_primary_key[0]
        cx.execute(outbox.insert().values(seq=seq))
        return int(seq)

    def event_stream(self, after: int = 0, limit: int = 100) -> list[dict]:
        with self.engine.connect() as cx:
            rows = cx.execute(
                select(events, outbox.c.published_at).join(outbox)
                .where(events.c.seq > after).order_by(events.c.seq).limit(max(1, min(limit, 500)))
            ).mappings()
            return [dict(r) for r in rows]

    def close(self) -> None:
        self.engine.dispose()
