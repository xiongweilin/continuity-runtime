"""Optional domain logical-effect identity and uniqueness.

Revision ID: 0003
Revises: 0002
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("effect_operations", sa.Column("effect_key", sa.String(256)))
    op.create_unique_constraint("uq_effect_logical_key", "effect_operations",
        ["principal", "provider", "resource_ref", "effect_key"])


def downgrade() -> None:
    op.drop_constraint("uq_effect_logical_key", "effect_operations", type_="unique")
    op.drop_column("effect_operations", "effect_key")
