"""Commit-ordered event cursor; prevent lower-sequence late commits.

Revision ID: 0005
Revises: 0004
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Apply while writers are stopped. This migration does not guarantee
    # online safety and must account for preexisting event rows.
    op.create_table("boundary_event_clock",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("last_seq", sa.Integer(), nullable=False))
    op.execute(sa.text(
        "INSERT INTO boundary_event_clock (id, last_seq) "
        "SELECT 1, COALESCE(MAX(seq), 0) FROM boundary_events"
    ))


def downgrade() -> None:
    op.drop_table("boundary_event_clock")
