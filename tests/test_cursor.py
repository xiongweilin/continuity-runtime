"""Resumable filtered event cursor and relation dependency lifecycle."""
from continuity.contracts import Intent


def test_filtered_cursor_advances_past_invisible_events(setup):
    runtime, _ = setup
    runtime.submit(
        Intent("shared", "mission", "counter", "items/one",
               "increment", {}, "grant"),
        actor="agent-a", participants=["agent-a", "agent-b"],
    )
    cursor = 0
    seen = []
    for _ in range(12):
        page = runtime.events_for_actor("agent-b", after=cursor, limit=1)
        seen.extend(e["kind"] for e in page["events"])
        if page["next_seq"] == cursor:
            break
        cursor = page["next_seq"]
    assert seen == ["operation.registered"]


def test_revised_link_removes_old_dependency(setup):
    runtime, _ = setup
    runtime.observe(actor="agent-a", ref="policy", source_revision="v1",
                    evidence_ref="e:v1")
    runtime.link(link_id="relation", actor="agent-a", source_ref="mission",
        target_ref="items/one", relation="depends", purpose="handoff",
        basis_ref="case:1", constraints={"depends_on": {"policy": "v1"}})
    runtime.link(link_id="relation", actor="agent-a", source_ref="mission",
        target_ref="items/one", relation="depends", purpose="handoff",
        basis_ref="case:2", constraints={}, expected_revision=1)
    runtime.observe(actor="agent-a", ref="policy", source_revision="v2",
                    evidence_ref="e:v2", expected_revision=1)
    assert not any(e["kind"] == "dependency.invalidated" for e in runtime.events())


def test_dependency_invalidation_is_persisted_and_requires_requalification(setup):
    runtime, _ = setup
    runtime.observe(actor="agent-a", ref="policy", source_revision="v1",
                    evidence_ref="test:e1")
    runtime.link(link_id="gated", actor="agent-a", source_ref="mission",
        target_ref="items/one", relation="requires", purpose="handoff",
        basis_ref="test:case",
        constraints={"depends_on": {"policy": "v1"}})
    runtime.observe(actor="agent-a", ref="policy", source_revision="v2",
                    evidence_ref="test:e2", expected_revision=1)
    stale = runtime.get_link("gated")
    assert stale["active"] is False
    assert stale["revision"] == 2
    assert len([e for e in runtime.events()
                if e["kind"] == "dependency.invalidated"]) == 1
    runtime.link(link_id="gated", actor="agent-a", source_ref="mission",
        target_ref="items/one", relation="requires", purpose="handoff",
        basis_ref="test:requalify",
        constraints={"depends_on": {"policy": "v2"}}, expected_revision=2)
    assert runtime.get_link("gated")["active"] is True
