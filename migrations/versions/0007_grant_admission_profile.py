"""Authority-owned explicit admission checker binding.

Revision ID: 0007
Revises: 0006
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("effect_grants",
        sa.Column("admission_profile", sa.String(160), nullable=False,
                  server_default="baseline"))
    op.alter_column("effect_grants", "admission_profile", server_default=None)


def downgrade() -> None:
    op.drop_column("effect_grants", "admission_profile")
