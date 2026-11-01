"""At-least-once event consumption survives a process/runtime restart."""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from continuity.api import Caller, create_app
from continuity.contracts import Conflict, Denied, Intent
from continuity.db import RuntimeDB
from continuity.runtime import ContinuityRuntime


def test_checkpoint_survives_new_runtime_instance(setup):
    runtime, _ = setup
    runtime.subscribe("cursor:1", actor="agent-b")
    runtime.submit(
        Intent("op:feed", "mission", "counter", "items/one",
               "increment", {}, "grant"), actor="agent-a",
        participants=["agent-a", "agent-b"],
    )
    first = runtime.poll_subscription("cursor:1", actor="agent-b", limit=2)
    assert any(e["kind"] == "operation.registered" for e in first["events"])
    with pytest.raises(Conflict):
        runtime.ack_subscription("cursor:1", actor="agent-b",
            expected_revision=first["revision"], next_seq=first["next_seq"] + 1000)
    # Before ack, duplicate delivery is explicit and expected.
    assert runtime.poll_subscription("cursor:1", actor="agent-b")["events"]
    ack = runtime.ack_subscription("cursor:1", actor="agent-b",
        expected_revision=first["revision"], next_seq=first["next_seq"])
    assert ack["last_acked_seq"] == first["next_seq"]
    with pytest.raises(Conflict):
        runtime.ack_subscription("cursor:1", actor="agent-b",
            expected_revision=first["revision"], next_seq=first["next_seq"])
    # New object / new SQLAlchemy engine, same persistent database.
    url = str(runtime.db.engine.url)
    newdb = RuntimeDB(url)
    resumed = ContinuityRuntime(newdb)
    assert resumed.subscription("cursor:1", actor="agent-b")["last_acked_seq"] == first["next_seq"]
    with pytest.raises(Denied):
        resumed.poll_subscription("cursor:1", actor="attacker")
    newdb.close()


def test_api_subscription_auth_and_ack(setup):
    runtime, _ = setup
    client = TestClient(create_app(runtime, tokens={
        "a": Caller("agent-a"), "b": Caller("agent-b"),
    }))
    a = {"Authorization": "Bearer a"}
    b = {"Authorization": "Bearer b"}
    assert client.post("/v1/subscriptions", json={"id": "shared"}, headers=b).status_code == 200
    assert client.get("/v1/subscriptions/shared", headers=a).status_code == 403
    page = client.get("/v1/subscriptions/shared", headers=b).json()
    assert "next_seq" in page
    ack = client.post("/v1/subscriptions/shared/ack",
        json={"expected_revision": page["revision"], "next_seq": page["next_seq"]},
        headers=b)
    assert ack.status_code == 200
    assert client.post("/v1/subscriptions/shared/ack",
        json={"expected_revision": page["revision"], "next_seq": page["next_seq"] + 1000},
        headers=b).status_code == 409



def test_subscription_freezes_offered_batch_until_ack(setup):
    runtime, _ = setup
    runtime.subscribe("cursor:fixed", actor="agent-b")
    for n in range(3):
        runtime.submit(Intent(f"batch:{n}", "mission", "counter", "items/one",
            "increment", {"n": n}, "grant"),
            actor="agent-a", participants=["agent-a", "agent-b"])
    # The first event is an administrator grant, intentionally hidden from
    # this actor. Its empty page still needs a checkpoint acknowledgement.
    invisible = runtime.poll_subscription("cursor:fixed", actor="agent-b", limit=1)
    assert invisible["events"] == []
    runtime.ack_subscription("cursor:fixed", actor="agent-b",
        expected_revision=invisible["revision"], next_seq=invisible["next_seq"])
    first = runtime.poll_subscription("cursor:fixed", actor="agent-b", limit=1)
    assert len(first["events"]) == 1
    larger = runtime.poll_subscription("cursor:fixed", actor="agent-b", limit=100)
    assert larger["next_seq"] == first["next_seq"]
    assert larger["events"] == first["events"]
    with pytest.raises(Conflict, match="exact outstanding"):
        runtime.ack_subscription("cursor:fixed", actor="agent-b",
            expected_revision=first["revision"], next_seq=first["next_seq"] - 1)
    runtime.ack_subscription("cursor:fixed", actor="agent-b",
        expected_revision=first["revision"], next_seq=first["next_seq"])
    next_batch = runtime.poll_subscription("cursor:fixed", actor="agent-b", limit=1)
    assert next_batch["next_seq"] > first["next_seq"]
