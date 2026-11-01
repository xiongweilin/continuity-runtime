"""Alembic environment for Continuity's explicit relational schema."""
from __future__ import annotations

import os

from alembic import context
from sqlalchemy import create_engine, pool

from continuity.db import metadata

config = context.config
target_metadata = metadata


def run_migrations_offline() -> None:
    url = os.environ.get("CONTINUITY_DATABASE_URL") or config.get_main_option("sqlalchemy.url")
    context.configure(url=url, target_metadata=target_metadata,
                      literal_binds=True, dialect_opts={"paramstyle": "named"})
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    url = os.environ.get("CONTINUITY_DATABASE_URL")
    if not url:
        raise RuntimeError("CONTINUITY_DATABASE_URL is required for migrations")
    engine = create_engine(url, poolclass=pool.NullPool)
    with engine.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata,
                          compare_type=True)
        with context.begin_transaction():
            context.run_migrations()
    engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
