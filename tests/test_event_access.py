"""Event discovery must not turn references into cross-principal data leaks."""
from fastapi.testclient import TestClient

from continuity.api import Caller, create_app
from continuity.contracts import Held, Intent


def test_actor_event_cursor_restricts_operations_and_observations(setup):
    runtime, _ = setup
    runtime.submit(
        Intent("secret", "mission", "counter", "items/one",
               "increment", {}, "grant"),
        actor="agent-a", participants=["agent-a", "agent-b"],
    )
    # Even a malicious observation reference cannot impersonate an operation event.
    runtime.observe(actor="attacker", ref="operation:secret",
                    source_revision="1", evidence_ref="attacker:untrusted")
    client = TestClient(create_app(runtime, tokens={
        "a": Caller("agent-a"), "b": Caller("agent-b"),
        "bad": Caller("attacker"),
    }))
    def events(token):
        res = client.get("/v1/my-events", headers={"Authorization": "Bearer " + token})
        assert res.status_code == 200
        payload = res.json()
        assert "next_seq" in payload
        return payload["events"]
    assert any(v["kind"] == "operation.registered" for v in events("a"))
    assert any(v["kind"] == "operation.registered" for v in events("b"))
    assert not any(v["kind"] == "operation.registered" for v in events("bad"))
    assert not any(v["kind"] == "observation.recorded" for v in events("b"))
    assert any(v["kind"] == "observation.recorded" for v in events("bad"))


def test_link_dependency_requires_actual_known_version(setup):
    runtime, _ = setup
    try:
        runtime.link(link_id="a", actor="agent-a", source_ref="mission",
                     target_ref="items/one", relation="depends", purpose="handoff",
                     basis_ref="ticket:1",
                     constraints={"depends_on": {"unobserved:version": "v1"}})
    except Held:
        pass
    else:
        raise AssertionError("unknown version was silently admitted")
