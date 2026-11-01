"""Administrative schema initialization and isolated demonstration."""
from __future__ import annotations

import argparse
import json
import os

from .db import RuntimeDB


def main() -> None:
    parser = argparse.ArgumentParser(prog="continuity")
    parser.add_argument("command", choices=["init-db", "demo"])
    parser.add_argument("--database-url", default=os.environ.get("CONTINUITY_DATABASE_URL"))
    args = parser.parse_args()
    if args.command == "demo":
        from .demo import scenario
        print(json.dumps(scenario(), indent=2))
        return
    if not args.database_url:
        parser.error("database URL is required")
    db = RuntimeDB(args.database_url, create_schema=True)
    db.close()
    print("schema initialized")
