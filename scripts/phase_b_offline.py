"""Phase B offline preflight: fake credentials, no GitHub API calls/writes.

Run from repository root:
    python scripts/phase_b_offline.py

Creates ephemeral secrets OUTSIDE the checkout, launches the optional Compose
overlay, verifies the uncredentialed Agent probe and logical-effect dedup, and
always tears down the isolated containers, networks and volumes.
"""
from __future__ import annotations

import json
import os
import secrets
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
COMPOSE = [
    "docker", "compose", "-p", "continuity-phase-b-offline",
    "-f", "compose.yaml", "-f", "compose.phase-b.yaml",
]


def run(args: list[str], env: dict[str, str]) -> None:
    subprocess.run(args, check=True, cwd=ROOT, env=env)


def main() -> int:
    # No real provider token, App installation secret or owner CLI credential
    # is used at any point. A fake token intentionally cannot write to GitHub.
    env = os.environ.copy()
    for name in (
        "GITHUB_TOKEN", "GH_TOKEN", "GITHUB_PERSONAL_ACCESS_TOKEN",
        "CONTINUITY_PHASE_B_TOKEN_FILE",
        "CONTINUITY_PHASE_B_EXECUTOR_KEY_FILE",
        "CONTINUITY_GITHUB_TOKEN_FILE",
    ):
        env.pop(name, None)
    env["POSTGRES_PASSWORD"] = secrets.token_urlsafe(24)
    env["CONTINUITY_TOKENS_JSON"] = json.dumps({
        "offline-admin": {"actor": "root", "admin": True},
        "offline-agent": {"actor": "phase-b-agent"},
    })
    env["CONTINUITY_PHASE_B_ADMIN_BEARER"] = "offline-admin"
    env["CONTINUITY_PHASE_B_AGENT_BEARER"] = "offline-agent"
    env["CONTINUITY_PHASE_B_API_BASE"] = "http://127.0.0.1:18086"

    with tempfile.TemporaryDirectory(prefix="continuity-phase-b-offline-") as directory:
        base = Path(directory)
        fake_token = base / "synthetic-github-token.txt"
        shared_key = base / "internal-key.txt"
        fake_token.write_text("offline-fake-provider-token-no-github-permission", encoding="utf-8")
        shared_key.write_text(secrets.token_urlsafe(48), encoding="utf-8")
        # Only fake token + test-only service key; Docker's unprivileged
        # container must be able to read their bind-mounted secret files.
        fake_token.chmod(0o644)
        shared_key.chmod(0o644)
        env["CONTINUITY_PHASE_B_TOKEN_FILE"] = str(fake_token.resolve())
        env["CONTINUITY_PHASE_B_EXECUTOR_KEY_FILE"] = str(shared_key.resolve())
        try:
            run(COMPOSE + ["up", "--build", "-d", "--wait"], env)
            run(COMPOSE + ["--profile", "isolation-test", "run", "--rm",
                           "agent-isolation-probe"], env)
            run([sys.executable, "scripts/phase_b_prepare.py"], env)
            print(json.dumps({
                "phase": "offline Phase B preflight",
                "github_provider_credential": "synthetic invalid token",
                "github_writes": 0,
                "result": "local networking, secret and operation checks completed",
                "real_credential_scope": "NOT VERIFIED",
            }))
            return 0
        finally:
            # Even if startup/probe/prepare fails, remove only this isolated
            # Compose project and its temporary test database volume.
            run(COMPOSE + ["down", "-v", "--remove-orphans"], env)


if __name__ == "__main__":
    raise SystemExit(main())
