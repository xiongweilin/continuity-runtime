"""HTTP integration smoke against the ephemeral Docker Compose API.

No writable provider is registered. CI test tokens are not production secrets.
"""
from __future__ import annotations

import json
import urllib.error
import urllib.request
import uuid

ROOT = "http://127.0.0.1:18086"


def request(method: str, path: str, token: str | None = None, payload=None):
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(
        ROOT + path, data=data, method=method, headers=headers,
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as exc:
        return exc.code, json.load(exc)


def main():
    assert request("GET", "/health") == (200, {"status": "ok"})
    assert request("GET", "/v1/events")[0] == 401
    assert request("GET", "/v1/events", token="wrong")[0] == 401
    assert request("POST", "/v1/grants", token="ci-agent",
                   payload={"id": "cannot-grant"})[0] == 403

    suffix = uuid.uuid4().hex
    gid = "ci-grant:" + suffix
    op = "ci-effect:" + suffix
    issued = {
        "id": gid, "principal": "ci-mission", "actors": ["ci-agent"],
        "provider": "missing-provider", "resource_prefix": "items",
        "action": "write",
    }
    assert request("POST", "/v1/grants", token="ci-admin", payload=issued)[0] == 200
    intent = {
        "id": op, "principal": "ci-mission", "provider": "missing-provider",
        "resource_ref": "items/one", "action": "write", "payload": {"x": 1},
        "authority_id": gid, "participants": ["ci-agent"],
    }
    assert request("POST", "/v1/operations", token="ci-agent", payload=intent)[0] == 200
    status, denied = request(
        "POST", "/v1/operations/" + op + "/dispatch", token="ci-agent",
    )
    assert status == 403, denied  # No trusted executor configured. Fail closed.
    _, state = request("GET", "/v1/operations/" + op, token="ci-agent")
    assert state["status"] == "registered"
    assert state["dispatch_generation"] == 0
    status, events = request("GET", "/v1/events", token="ci-admin")
    assert status == 200
    assert any(e["entity_ref"] == "operation:" + op for e in events)
    print(json.dumps({"compose_api": "healthy", "auth": "enforced",
                      "no_executor": "fail_closed", "events": "persisted"}))


if __name__ == "__main__":
    main()
