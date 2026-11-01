from __future__ import annotations

from fastapi.testclient import TestClient

from continuity.api import Caller, create_app


def test_authz_and_independent_agent_handoff(setup):
    runtime, adapter = setup
    client = TestClient(create_app(runtime, tokens={
        "owner-token": Caller("root", admin=True),
        "agent-a-token": Caller("agent-a"),
        "agent-b-token": Caller("agent-b"),
        "attacker-token": Caller("untrusted"),
    }))
    def headers(token):
        return {"Authorization": "Bearer " + token}
    payload = {
        "id": "op-api", "principal": "mission", "provider": "counter",
        "resource_ref": "items/one", "action": "increment", "payload": {"count": 1},
        "authority_id": "grant", "participants": ["agent-a", "agent-b"],
    }
    assert client.post("/v1/operations", json=payload).status_code == 401
    assert client.post("/v1/operations", json=payload, headers=headers("attacker-token")).status_code == 403
    assert client.post("/v1/operations", json=payload, headers=headers("agent-a-token")).status_code == 200
    assert client.get("/v1/operations/op-api", headers=headers("attacker-token")).status_code == 403
    assert client.post("/v1/operations/op-api/dispatch", headers=headers("agent-b-token")).status_code == 200
    assert client.post("/v1/operations/op-api/dispatch", headers=headers("agent-a-token")).status_code == 200
    assert adapter.calls == 1
    assert client.post("/v1/operations/op-api/reconcile", headers=headers("agent-a-token")).status_code == 200
    assert client.post("/v1/operations/op-api/verify-outcome",
                       headers=headers("agent-a-token")).status_code == 409
    assert client.get("/v1/events", headers=headers("agent-a-token")).status_code == 403
    assert client.get("/v1/events", headers=headers("owner-token")).status_code == 200
    assert client.post("/v1/grants/grant/revoke", headers=headers("agent-a-token")).status_code == 403


def test_disabled_default_tokens(setup):
    runtime, _ = setup
    client = TestClient(create_app(runtime, tokens={}))
    assert client.get("/health").status_code == 200
    assert client.get("/v1/events", headers={"Authorization": "Bearer anything"}).status_code == 401
