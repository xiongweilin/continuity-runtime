"""Frozen v0.1 initial schema.

Revision ID: 0001
Revises:
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table("effect_grants",
        sa.Column("id", sa.String(160), primary_key=True),
        sa.Column("principal", sa.String(160), nullable=False),
        sa.Column("actors", sa.JSON(), nullable=False),
        sa.Column("provider", sa.String(160), nullable=False),
        sa.Column("resource_prefix", sa.String(512), nullable=False),
        sa.Column("action", sa.String(160), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True)),
        sa.Column("revoked", sa.Boolean(), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False))
    op.create_table("source_observations",
        sa.Column("ref", sa.String(512), primary_key=True),
        sa.Column("source_ref", sa.String(512), nullable=False),
        sa.Column("source_revision", sa.String(200), nullable=False),
        sa.Column("evidence_ref", sa.String(1024), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False))
    op.create_table("boundary_links",
        sa.Column("id", sa.String(160), primary_key=True),
        sa.Column("source_ref", sa.String(512), nullable=False),
        sa.Column("target_ref", sa.String(512), nullable=False),
        sa.Column("relation", sa.String(160), nullable=False),
        sa.Column("purpose", sa.String(40), nullable=False),
        sa.Column("owner", sa.String(160), nullable=False),
        sa.Column("basis_ref", sa.String(1024), nullable=False),
        sa.Column("constraints", sa.JSON(), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False))
    op.create_table("effect_operations",
        sa.Column("id", sa.String(160), primary_key=True),
        sa.Column("fingerprint", sa.String(64), nullable=False),
        sa.Column("principal", sa.String(160), nullable=False),
        sa.Column("provider", sa.String(160), nullable=False),
        sa.Column("resource_ref", sa.String(512), nullable=False),
        sa.Column("action", sa.String(160), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("authority_id", sa.String(160), nullable=False),
        sa.Column("expected_versions", sa.JSON(), nullable=False),
        sa.Column("participants", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(40), nullable=False),
        sa.Column("dispatch_generation", sa.Integer(), nullable=False),
        sa.Column("admission", sa.JSON()),
        sa.Column("receipt_ref", sa.String(1024)),
        sa.Column("effect_disposition", sa.String(40), nullable=False),
        sa.Column("effect_evidence_ref", sa.String(1024)),
        sa.Column("outcome_disposition", sa.String(40), nullable=False),
        sa.Column("outcome_evidence_ref", sa.String(1024)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False))
    op.create_index("ix_operation_status", "effect_operations", ["status"])
    op.create_table("boundary_events",
        sa.Column("seq", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("entity_ref", sa.String(512), nullable=False),
        sa.Column("kind", sa.String(160), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False))
    op.create_index("ix_boundary_events_entity", "boundary_events", ["entity_ref", "seq"])
    op.create_table("boundary_outbox",
        sa.Column("seq", sa.Integer(), sa.ForeignKey("boundary_events.seq"), primary_key=True),
        sa.Column("published_at", sa.DateTime(timezone=True)))


def downgrade() -> None:
    op.drop_table("boundary_outbox")
    op.drop_index("ix_boundary_events_entity", table_name="boundary_events")
    op.drop_table("boundary_events")
    op.drop_index("ix_operation_status", table_name="effect_operations")
    op.drop_table("effect_operations")
    op.drop_table("boundary_links")
    op.drop_table("source_observations")
    op.drop_table("effect_grants")
