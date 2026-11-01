"""Durable actor-owned event consumer offsets.

Revision ID: 0004
Revises: 0003
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table("boundary_consumer_offsets",
        sa.Column("id", sa.String(160), primary_key=True),
        sa.Column("actor", sa.String(160), nullable=False),
        sa.Column("last_acked_seq", sa.Integer(), nullable=False),
        sa.Column("last_offered_seq", sa.Integer(), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False))


def downgrade() -> None:
    op.drop_table("boundary_consumer_offsets")
