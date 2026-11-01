"""Isolated example of an external durable system, NOT a production effect provider.

The second database represents a separately owned authoritative effect source.
Its unique operation identity supplies provider-side idempotency.
"""
from __future__ import annotations

from sqlalchemy import Column, MetaData, String, Table, create_engine, select
from sqlalchemy.exc import IntegrityError

from .contracts import Intent, Outcome, Readback, Receipt


class SampleExternalSystem:
    def __init__(self, url: str) -> None:
        self.engine = create_engine(url, pool_pre_ping=True)
        self.metadata = MetaData()
        self.effects = Table(
            "sample_external_effects", self.metadata,
            Column("operation_id", String(160), primary_key=True),
            Column("target", String(512), nullable=False),
            Column("action", String(160), nullable=False),
        )
        self.metadata.create_all(self.engine)

    def execute(self, intent: Intent) -> Receipt:
        # This DB is external to the Runtime DB. No distributed atomic commit.
        with self.engine.begin() as cx:
            existing = cx.execute(select(self.effects).where(
                self.effects.c.operation_id == intent.operation_id,
            )).mappings().first()
            if existing is None:
                try:
                    cx.execute(self.effects.insert().values(
                        operation_id=intent.operation_id,
                        target=intent.resource_ref, action=intent.action,
                    ))
                except IntegrityError:
                    # A concurrent insert may have committed. The next readback decides.
                    raise
            elif (existing["target"] != intent.resource_ref
                  or existing["action"] != intent.action):
                raise ValueError("external operation identity rebound")
        return Receipt(True, "sample:receipt:" + intent.operation_id)

    def readback(self, intent: Intent) -> Readback:
        with self.engine.connect() as cx:
            row = cx.execute(select(self.effects).where(
                self.effects.c.operation_id == intent.operation_id,
            )).mappings().first()
        if row is None:
            # Historical non-effect is NOT proven by an empty current snapshot.
            return Readback("unknown")
        if row["target"] != intent.resource_ref or row["action"] != intent.action:
            return Readback("unknown")
        return Readback("present", "sample:authoritative-row:" + intent.operation_id)


class SampleOutcomeVerifier:
    """Only valid for the toy goal 'the sample row for this operation exists'."""
    def verify(self, intent: Intent, effect: Readback) -> Outcome:
        if effect.disposition != "present" or not effect.evidence_ref:
            return Outcome(None)
        return Outcome(True, "sample:goal:" + intent.operation_id)
