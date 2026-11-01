"""Prepare Phase B's bounded Runtime operation WITHOUT external provider writes.

Uses two synthetic Continuity API bearer tokens (NOT the GitHub owner CLI token).
Run only after starting the isolated Phase B Compose overlay. It NEVER dispatches.
"""
from __future__ import annotations

import json
import os
import urllib.error
import urllib.request

BASE = os.getenv("CONTINUITY_PHASE_B_API_BASE", "http://127.0.0.1:18086").rstrip("/")
REPO = "xiongweilin/continuity-runtime-phase-b-sandbox"
ISSUE = "1"
LABEL = "continuity-effect-test"
GRANT = "phase-b-sandbox-issue-1-label-grant"
OPERATION = "phase-b-sandbox-issue-1-label-operation"
EFFECT_KEY = "github-issue-1-continuity-effect-test"


def request(method: str, path: str, token: str, payload=None) -> tuple[int, dict]:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {"Authorization": "Bearer " + token, "Accept": "application/json"}
    if data is not None:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(
        BASE + path, data=data, method=method, headers=headers,
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as exc:
        return exc.code, json.load(exc)


def main() -> None:
    admin = os.getenv("CONTINUITY_PHASE_B_ADMIN_BEARER", "")
    agent = os.getenv("CONTINUITY_PHASE_B_AGENT_BEARER", "")
    if not admin or not agent or admin == agent:
        raise SystemExit("two distinct nonempty test-only Runtime bearer tokens required")
    if BASE != "http://127.0.0.1:18086":
        raise SystemExit("Phase B preparation only supports localhost port 18086")

    grant = {
        "id": GRANT, "principal": "phase-b-mission",
        "actors": ["phase-b-agent"], "provider": "github-label",
        "resource_prefix": f"github:issue:{REPO}/{ISSUE}",
        "action": "add_label", "admission_profile": "baseline",
    }
    status, payload = request("POST", "/v1/grants", admin, grant)
    if status not in (200, 409):
        raise RuntimeError(f"grant issuance rejected: {status}: {payload.get('detail')}")

    operation = {
        "id": OPERATION, "principal": "phase-b-mission",
        "provider": "github-label", "resource_ref": f"github:issue:{REPO}/{ISSUE}",
        "action": "add_label", "authority_id": GRANT,
        "payload": {"label": LABEL},
        "effect_key": EFFECT_KEY,
        "participants": ["phase-b-agent"],
    }
    status, body = request("POST", "/v1/operations", agent, operation)
    if status != 200:
        raise RuntimeError(f"operation registration failed: {status}: {body.get('detail')}")
    if body.get("status") != "registered" or body.get("dispatch_generation") != 0:
        raise RuntimeError("operation was already dispatched; stop and inspect")

    duplicate = {**operation, "id": OPERATION + "-duplicate"}
    duplicate_status, _ = request("POST", "/v1/operations", agent, duplicate)
    if duplicate_status != 409:
        raise RuntimeError("different-ID duplicate was not prevented")
    print(json.dumps({
        "phase": "B-preparation-only",
        "provider_writes": 0,
        "runtime_operation": OPERATION,
        "status": body["status"],
        "duplicate_logical_effect": "rejected",
    }))


if __name__ == "__main__":
    main()
