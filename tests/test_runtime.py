from __future__ import annotations

import pytest

from continuity.contracts import Admission, Conflict, Denied, Held, Intent, Outcome, Readback, Receipt
from continuity.demo import scenario


def intent(name="op1", expected=None):
    return Intent(name, "mission", "counter", "items/one", "increment",
                  {"count": 1}, "grant", expected or {})


def submit(runtime, operation=None):
    op = operation or intent()
    return runtime.submit(op, actor="agent-a", participants=["agent-a", "agent-b"])


def test_demo_cross_agent_handoff():
    result = scenario()
    assert result["status"] == "effect_confirmed"
    assert result["effect"] == "present"
    assert result["outcome"] == "satisfied"
    assert result["provider_calls"] == 1


def test_duplicate_submit_same_identity_and_rebound(setup):
    runtime, adapter = setup
    first = submit(runtime)
    second = submit(runtime)
    assert first["fingerprint"] == second["fingerprint"]
    with pytest.raises(Conflict):
        submit(runtime, Intent("op1", "mission", "counter", "items/one",
                               "increment", {"count": 99}, "grant"))


def test_dispatch_competition_no_duplicate_effect(setup):
    runtime, adapter = setup
    submit(runtime)
    assert runtime.dispatch("op1", actor="agent-b")["status"] == "pending_verification"
    assert runtime.dispatch("op1", actor="agent-a")["status"] == "pending_verification"
    assert adapter.calls == 1


def test_crash_after_effect_before_ack_retains_unknown(setup):
    runtime, adapter = setup

    class LostAck:
        calls = 0
        def execute(self, intent):
            self.calls += 1
            adapter.effects.add(intent.operation_id)
            raise TimeoutError("response lost")
        def readback(self, intent):
            return Readback("present", "provider:authoritative-readback")

    lost = LostAck()
    runtime.adapters["counter"] = lost
    submit(runtime)
    op = runtime.dispatch("op1", actor="agent-b")
    assert op["status"] == "unknown"
    runtime.dispatch("op1", actor="agent-a")
    assert lost.calls == 1
    op = runtime.reconcile("op1", actor="agent-b")
    assert op["status"] == "effect_confirmed"
    assert op["outcome_disposition"] == "unverified"


def test_negative_snapshot_does_not_prove_no_effect(setup):
    runtime, adapter = setup

    class Negative:
        def execute(self, intent):
            return Receipt(False)
        def readback(self, intent):
            return Readback("absent", "snapshot:empty", reliable_no_effect=False)

    runtime.adapters["counter"] = Negative()
    submit(runtime)
    assert runtime.dispatch("op1", actor="agent-a")["status"] == "unknown"
    assert runtime.reconcile("op1", actor="agent-b")["status"] == "unknown"


def test_authority_rechecked_at_dispatch(setup):
    runtime, adapter = setup
    submit(runtime)
    runtime.revoke_grant("grant")
    with pytest.raises(Denied):
        runtime.dispatch("op1", actor="agent-b")
    assert adapter.calls == 0
    assert runtime.get_operation("op1", actor="agent-a")["status"] == "registered"


def test_untrusted_actor_cannot_take_operation(setup):
    runtime, adapter = setup
    submit(runtime)
    with pytest.raises(Denied):
        runtime.dispatch("op1", actor="attacker")
    with pytest.raises(Denied):
        runtime.get_operation("op1", actor="attacker")


def test_resource_scope_not_naive_prefix(setup):
    runtime, _ = setup
    with pytest.raises(Denied):
        runtime.submit(Intent("wrong", "mission", "counter", "items-extra/one",
                              "increment", {}, "grant"),
                       actor="agent-a", participants=["agent-a"])


def test_source_version_changes_block_dispatch(setup):
    runtime, adapter = setup
    runtime.observe(actor="agent-a", ref="policy", source_revision="v1",
                    evidence_ref="evidence:1")
    runtime.version_authorities["policy"] = lambda: "v1"
    submit(runtime, intent(expected={"policy": "v1"}))
    runtime.observe(actor="agent-a", ref="policy", source_revision="v2",
                    evidence_ref="evidence:2", expected_revision=1)
    with pytest.raises(Held):
        runtime.dispatch("op1", actor="agent-b")
    assert adapter.calls == 0


def test_link_revision_invalidation_and_owner(setup):
    runtime, _ = setup
    runtime.observe(actor="agent-a", ref="policy", source_revision="v1",
                    evidence_ref="e:1")
    link = runtime.link(link_id="depends", actor="agent-a", source_ref="mission",
        target_ref="items/one", relation="depends-on", purpose="handoff",
        basis_ref="case:1", constraints={"depends_on": {"policy": "v1"}})
    assert link["revision"] == 1
    with pytest.raises(Conflict):
        runtime.link(link_id="depends", actor="agent-a", source_ref="mission",
            target_ref="items/one", relation="depends-on", purpose="handoff",
            basis_ref="case:1", expected_revision=0)
    runtime.observe(actor="agent-a", ref="policy", source_revision="v2",
                    evidence_ref="e:2", expected_revision=1)
    assert "dependency.invalidated" in [event["kind"] for event in runtime.events()]


def test_baseline_vs_optional_admission_not_confused_with_authority(setup):
    runtime, adapter = setup
    class Policy:
        def evaluate(self, intent, observations):
            return Admission("hold", "insufficient evidence", policy_version="test")
    runtime.policy = Policy()
    submit(runtime)
    with pytest.raises(Held):
        runtime.dispatch("op1", actor="agent-b")
    assert adapter.calls == 0
    runtime.policy = type("DenyPolicy", (), {
        "evaluate": lambda self, i, o: Admission("deny", "not permitted")
    })()
    with pytest.raises(Denied):
        runtime.dispatch("op1", actor="agent-a")
    assert adapter.calls == 0


def test_no_auto_outcome_and_domain_verifier(setup):
    runtime, _ = setup
    submit(runtime)
    runtime.dispatch("op1", actor="agent-a")
    runtime.reconcile("op1", actor="agent-b")
    with pytest.raises(Held):
        runtime.verify_outcome("op1", actor="agent-a")
    class Verifier:
        def verify(self, intent, effect):
            return Outcome(False, "domain:evidence")
    runtime.verifiers["counter"] = Verifier()
    assert runtime.verify_outcome("op1", actor="agent-b")["outcome_disposition"] == "not_satisfied"


def test_admission_rechecks_evidence_after_slow_policy(setup):
    runtime, adapter = setup
    runtime.observe(actor="agent-a", ref="policy", source_revision="v1",
                    evidence_ref="source:v1")
    runtime.version_authorities["policy"] = lambda: "v1"
    submit(runtime, intent(expected={"policy": "v1"}))

    class StalePolicy:
        def evaluate(self, intent, observations):
            # Simulates observation changing while a slow model/policy decides.
            runtime.observe(actor="agent-a", ref="policy", source_revision="v2",
                            evidence_ref="source:v2", expected_revision=1)
            return Admission("admit", policy_version="slow-policy-v1")

    runtime.policy = StalePolicy()
    with pytest.raises(Held):
        runtime.dispatch("op1", actor="agent-b")
    assert adapter.calls == 0
    assert runtime.get_operation("op1", actor="agent-a")["status"] == "registered"


def test_hold_is_auditable_and_never_mints_authority(setup):
    runtime, _ = setup
    submit(runtime)
    runtime.policy = type("HoldPolicy", (), {
        "evaluate": lambda self, i, o: Admission("hold", "needs evidence")
    })()
    with pytest.raises(Held):
        runtime.dispatch("op1", actor="agent-a")
    assert any(e["kind"] == "admission.hold" for e in runtime.events())


def test_untrusted_observation_cannot_authorize_version_guarded_effect(setup):
    runtime, adapter = setup
    runtime.observe(actor="agent-a", ref="version:remote", source_revision="7",
                    evidence_ref="agent:self-assertion")
    submit(runtime, intent(expected={"version:remote": "7"}))
    with pytest.raises(Held, match="no trusted version authority"):
        runtime.dispatch("op1", actor="agent-b")
    assert adapter.calls == 0


def test_trusted_source_disagrees_with_actor_observation(setup):
    runtime, adapter = setup
    runtime.observe(actor="agent-a", ref="version:remote", source_revision="7",
                    evidence_ref="agent:self-assertion")
    runtime.version_authorities["version:remote"] = lambda: "8"
    submit(runtime, intent(expected={"version:remote": "7"}))
    with pytest.raises(Held, match="authoritative version changed"):
        runtime.dispatch("op1", actor="agent-a")
    assert adapter.calls == 0


def test_trusted_source_becomes_unavailable(setup):
    runtime, adapter = setup
    runtime.observe(actor="agent-a", ref="version:remote", source_revision="7",
                    evidence_ref="agent:self-assertion")
    runtime.version_authorities["version:remote"] = lambda: (_ for _ in ()).throw(
        ConnectionError("source offline"))
    submit(runtime, intent(expected={"version:remote": "7"}))
    with pytest.raises(Held, match="authority unavailable"):
        runtime.dispatch("op1", actor="agent-b")
    assert adapter.calls == 0



def test_distinct_operation_ids_cannot_repeat_same_domain_effect_key(setup):
    runtime, adapter = setup
    first = Intent("op:alpha", "mission", "counter", "items/one",
        "increment", {"count": 1}, "grant", effect_key="purchase:123")
    second = Intent("op:beta", "mission", "counter", "items/one",
        "increment", {"count": 1}, "grant", effect_key="purchase:123")
    runtime.submit(first, actor="agent-a", participants=["agent-a", "agent-b"])
    with pytest.raises(Conflict, match="logical effect key"):
        runtime.submit(second, actor="agent-b", participants=["agent-a", "agent-b"])
    runtime.dispatch("op:alpha", actor="agent-b")
    assert adapter.calls == 1


def test_logical_effect_key_scoped_to_target_and_principal(setup):
    runtime, _ = setup
    runtime.submit(Intent("a", "mission", "counter", "items/one",
        "increment", {}, "grant", effect_key="same"),
        actor="agent-a", participants=["agent-a"])
    runtime.submit(Intent("b", "mission", "counter", "items/two",
        "increment", {}, "grant", effect_key="same"),
        actor="agent-b", participants=["agent-b"])


def test_authoritative_remote_version_changed_during_admission(setup):
    runtime, adapter = setup
    remote = {"revision": "v1"}
    runtime.observe(actor="agent-a", ref="policy:remote", source_revision="v1",
                    evidence_ref="agent:reported-v1")
    runtime.version_authorities["policy:remote"] = lambda: remote["revision"]
    submit(runtime, intent(expected={"policy:remote": "v1"}))

    class RacingPolicy:
        def evaluate(self, intent, observations):
            # A remote source changes while admission is being evaluated,
            # without any local observation update.
            remote["revision"] = "v2"
            return Admission("admit", policy_version="racing-v1")

    runtime.policy = RacingPolicy()
    with pytest.raises(Held, match="authoritative version changed"):
        runtime.dispatch("op1", actor="agent-b")
    assert adapter.calls == 0
    assert runtime.get_operation("op1", actor="agent-a")["status"] == "registered"



def test_inflight_readback_does_not_erase_later_dispatch_ack(setup):
    runtime, adapter = setup

    class ReentrantProvider:
        def execute(self, intent):
            # A separate workflow re-reads the effect during provider execution.
            first = runtime.reconcile(intent.operation_id, actor="agent-b")
            assert first["status"] == "may_have_been_sent"
            adapter.effects.add(intent.operation_id)
            return Receipt(True, "provider:ack")

        def readback(self, intent):
            return Readback("unknown")

    runtime.adapters["counter"] = ReentrantProvider()
    submit(runtime)
    op = runtime.dispatch("op1", actor="agent-a")
    assert op["status"] == "pending_verification"
    assert op["receipt_ref"] == "provider:ack"
    assert op["dispatch_generation"] == 1


def test_inflight_absence_cannot_cancel_running_dispatch(setup):
    runtime, adapter = setup

    class EarlyAbsentProvider:
        def execute(self, intent):
            state = runtime.reconcile(intent.operation_id, actor="agent-b")
            assert state["status"] == "may_have_been_sent"
            adapter.effects.add(intent.operation_id)
            return Receipt(True, "provider:ack")

        def readback(self, intent):
            return Readback("absent", "provider:historical-check", reliable_no_effect=True)

    runtime.adapters["counter"] = EarlyAbsentProvider()
    submit(runtime)
    assert runtime.dispatch("op1", actor="agent-a")["status"] == "pending_verification"
