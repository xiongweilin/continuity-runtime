"""BAA-inspired canary guard with trusted evidence, never a general safety claim."""
from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

import pytest

from continuity.canary_admission import (
    CanaryAdmissionPolicy, CanaryGuardrails, CanaryScope, CanarySnapshot, CanaryStage,
)
from continuity.contracts import Denied, Held, Intent


def setup_policy():
    now = datetime(2026, 11, 1, tzinfo=timezone.utc)
    evidence = CanarySnapshot(
        revision="r7", evidence_ref="telemetry:canary:r7", observed_at=now,
        current_weight=10, current_stage_index=0, rollback_ready=True,
        duration_seconds=120, candidate_requests=40, control_requests=360,
        candidate_error_rate=0.01, control_error_rate=0.01,
        candidate_p95_ms=120, control_p95_ms=110,
    )
    scope = CanaryScope(
        provider="traffic", action="promote", resource_ref="deployment:one",
        experiment_id="canary-1", source_ref="deployment:one:revision",
        stages=(CanaryStage(10, 60, 100), CanaryStage(50, 120, 200),
                CanaryStage(100, 180, 300)),
        guardrails=CanaryGuardrails(0.05, 0.02, 250, 1.5),
    )
    box = {"value": evidence}
    policy = CanaryAdmissionPolicy(scope, lambda: box["value"], clock=lambda: now)
    intent = Intent(
        "op-traffic", "mission", "traffic", "deployment:one", "promote",
        {"experiment_id": "canary-1", "stage_index": 1, "weight_percent": 50},
        "traffic-grant", expected_versions={"deployment:one:revision": "r7"},
    )
    return policy, intent, box


def test_canary_approved_under_independent_trusted_telemetry():
    policy, intent, _ = setup_policy()
    result = policy.evaluate(intent, {})
    assert result.decision == "admit"
    assert result.policy_version == "canary-stage-v1"
    assert result.evidence_ref == "telemetry:canary:r7"


@pytest.mark.parametrize("change,expected", [
    ({"revision": "r8"}, "hold"),
    ({"rollback_ready": False}, "hold"),
    ({"effect_unresolved": True}, "hold"),
    ({"candidate_error_rate": 0.25}, "deny"),
    ({"candidate_p95_ms": 400}, "deny"),
    ({"control_requests": 5, "candidate_requests": 5}, "hold"),
    ({"telemetry_complete": False}, "hold"),
    ({"observed_at": datetime(2026, 10, 31, tzinfo=timezone.utc)}, "hold"),
    ({"candidate_error_rate": None}, "hold"),
])
def test_canary_stale_unbounded_or_unsafe_evidence_fails_closed(change, expected):
    policy, intent, box = setup_policy()
    box["value"] = replace(box["value"], **change)
    assert policy.evaluate(intent, {}).decision == expected


def test_canary_ignores_agent_supplied_fake_telemetry():
    policy, intent, box = setup_policy()
    box["value"] = replace(box["value"], candidate_error_rate=0.8)
    malicious = replace(intent, payload={
        **intent.payload, "candidate_error_rate": 0.0, "rollback_ready": True,
    })
    assert policy.evaluate(malicious, {}).decision == "deny"


def test_canary_blocks_skipped_stage_and_mismatched_target():
    policy, intent, _ = setup_policy()
    skipped = replace(intent, payload={**intent.payload, "stage_index": 2,
                                       "weight_percent": 100})
    wrong_target = replace(intent, resource_ref="deployment:two")
    assert policy.evaluate(skipped, {}).decision == "deny"
    assert policy.evaluate(wrong_target, {}).decision == "deny"


def test_canary_integrates_with_separate_authority_and_effect_boundary(setup):
    runtime, provider = setup
    policy, intent, box = setup_policy()
    runtime.admission_profiles["canary-stage-v1"] = policy
    # Use the existing isolated test adapter, but enforce the exact canary
    # provider scope in policy; no real deployment traffic is modified.
    runtime.adapters["traffic"] = provider
    runtime.issue_grant(
        grant_id="traffic-grant", principal="mission",
        actors=["agent-a", "agent-b"], provider="traffic",
        resource_prefix="deployment:one", action="promote",
        required_version_refs=["deployment:one:revision"],
        admission_profile="canary-stage-v1",
    )
    runtime.observe(actor="agent-a", ref="deployment:one:revision",
                    source_revision="r7", evidence_ref="agent:observation")
    runtime.version_authorities["deployment:one:revision"] = lambda: box["value"].revision
    runtime.submit(intent, actor="agent-a", participants=["agent-a", "agent-b"])
    box["value"] = replace(box["value"], candidate_error_rate=0.3)
    with pytest.raises(Denied, match="canary guardrail"):
        runtime.dispatch("op-traffic", actor="agent-b")
    assert provider.calls == 0
    box["value"] = replace(box["value"], candidate_error_rate=0.01)
    assert runtime.dispatch("op-traffic", actor="agent-b")["status"] == "pending_verification"
    assert provider.calls == 1
    assert runtime.get_operation("op-traffic", actor="agent-a")["admission"]["policy_version"] == "canary-stage-v1"


def test_canary_missing_required_version_stops_at_authority(setup):
    runtime, provider = setup
    policy, intent, box = setup_policy()
    runtime.admission_profiles["canary-stage-v1"] = policy
    runtime.adapters["traffic"] = provider
    runtime.issue_grant(
        grant_id="traffic-grant", principal="mission",
        actors=["agent-a"], provider="traffic",
        resource_prefix="deployment:one", action="promote",
        required_version_refs=["deployment:one:revision"],
    )
    with pytest.raises(Denied, match="authority-required"):
        runtime.submit(replace(intent, expected_versions={}),
                       actor="agent-a", participants=["agent-a"])
    assert provider.calls == 0



def test_missing_covered_checker_never_falls_back_to_baseline(setup):
    runtime, provider = setup
    policy, intent, box = setup_policy()
    runtime.adapters["traffic"] = provider
    runtime.issue_grant(
        grant_id="traffic-grant", principal="mission",
        actors=["agent-a", "agent-b"], provider="traffic",
        resource_prefix="deployment:one", action="promote",
        required_version_refs=["deployment:one:revision"],
        admission_profile="canary-stage-v1",
    )
    runtime.observe(actor="agent-a", ref="deployment:one:revision",
                    source_revision="r7", evidence_ref="agent:reported")
    runtime.version_authorities["deployment:one:revision"] = lambda: box["value"].revision
    runtime.submit(intent, actor="agent-a", participants=["agent-a", "agent-b"])
    with pytest.raises(Held, match="profile not installed"):
        runtime.dispatch("op-traffic", actor="agent-b")
    assert provider.calls == 0
    runtime.admission_profiles["canary-stage-v1"] = policy
    assert runtime.dispatch("op-traffic", actor="agent-b")["status"] == "pending_verification"


def test_wrong_checker_identity_does_not_satisfy_covered_grant(setup):
    from continuity.contracts import Admission

    runtime, provider = setup
    _, intent, box = setup_policy()
    runtime.adapters["traffic"] = provider
    runtime.issue_grant(grant_id="traffic-grant", principal="mission",
        actors=["agent-a", "agent-b"], provider="traffic",
        resource_prefix="deployment:one", action="promote",
        required_version_refs=["deployment:one:revision"],
        admission_profile="canary-stage-v1")
    runtime.observe(actor="agent-a", ref="deployment:one:revision",
                    source_revision="r7", evidence_ref="agent:reported")
    runtime.version_authorities["deployment:one:revision"] = lambda: box["value"].revision

    class WrongChecker:
        def evaluate(self, intent, observations):
            return Admission("admit", policy_version="baseline-v1")

    runtime.admission_profiles["canary-stage-v1"] = WrongChecker()
    runtime.submit(intent, actor="agent-a", participants=["agent-a", "agent-b"])
    with pytest.raises(Denied, match="mismatched profile"):
        runtime.dispatch("op-traffic", actor="agent-b")
    assert provider.calls == 0



def test_first_canary_stage_requires_independent_quality_prerequisite():
    policy, intent, box = setup_policy()
    box["value"] = replace(box["value"], current_weight=0, current_stage_index=0)
    first = replace(intent, payload={
        "experiment_id": "canary-1", "stage_index": 0, "weight_percent": 10,
    })
    assert policy.evaluate(first, {}).decision == "hold"
    box["value"] = replace(box["value"], initial_quality_verified=True)
    assert policy.evaluate(first, {}).decision == "admit"


def test_invalid_remote_telemetry_is_held_not_promoted():
    policy, intent, box = setup_policy()
    box["value"] = replace(box["value"], candidate_error_rate="corrupt")
    result = policy.evaluate(intent, {})
    assert result.decision == "hold"
    box["value"] = replace(box["value"], current_stage_index="invalid")
    assert policy.evaluate(intent, {}).decision == "hold"
    box["value"] = replace(box["value"], observed_at="invalid")
    assert policy.evaluate(intent, {}).decision == "hold"



@pytest.mark.parametrize("change", [
    {"rollback_ready": "false"},
    {"initial_quality_verified": "true"},
    {"telemetry_complete": "yes"},
    {"effect_unresolved": None},
])
def test_canary_malformed_assurance_flags_fail_closed(change):
    policy, intent, box = setup_policy()
    box["value"] = replace(box["value"], **change)
    assert policy.evaluate(intent, {}).decision == "hold"


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), 0, -1, "slow"])
def test_canary_guardrail_configuration_rejects_unbounded_latency(bad):
    with pytest.raises(ValueError, match="positive and finite"):
        CanaryGuardrails(0.1, 0.1, bad, 1.5)
