"""Add indexed dependency invalidation.

Revision ID: 0002
Revises: 0001
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table("boundary_link_dependencies",
        sa.Column("link_id", sa.String(160),
                  sa.ForeignKey("boundary_links.id", ondelete="CASCADE"),
                  primary_key=True),
        sa.Column("dependency_ref", sa.String(512), primary_key=True),
        sa.Column("required_version", sa.String(200), nullable=False))
    op.create_index("ix_dependency_ref", "boundary_link_dependencies", ["dependency_ref"])


def downgrade() -> None:
    op.drop_index("ix_dependency_ref", table_name="boundary_link_dependencies")
    op.drop_table("boundary_link_dependencies")
