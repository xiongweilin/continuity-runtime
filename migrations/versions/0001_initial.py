"""Initial cross-boundary schema.

Revision ID: 0001
Revises:
"""
from __future__ import annotations

from alembic import op

from continuity.db import metadata

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # The initial baseline is deliberately generated from the same SQLAlchemy
    # metadata as runtime tests. All later upgrades MUST use explicit changes.
    metadata.create_all(bind=op.get_bind())


def downgrade() -> None:
    # Destructive downgrade for local experiments only. Back up production data
    # and write explicit preservation procedures before any real deployment.
    metadata.drop_all(bind=op.get_bind())
