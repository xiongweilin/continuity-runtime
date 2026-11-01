"""Enforce authority-owned source-version guards.

Revision ID: 0006
Revises: 0005
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("effect_grants",
        sa.Column("required_version_refs", sa.JSON(), nullable=False,
                  server_default=sa.text("'[]'")))
    op.alter_column("effect_grants", "required_version_refs",
                    server_default=None)


def downgrade() -> None:
    op.drop_column("effect_grants", "required_version_refs")
