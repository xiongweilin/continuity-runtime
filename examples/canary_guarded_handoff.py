"""Runnable isolated canary admission + authority + effect + outcome demo.

Run: python examples/canary_guarded_handoff.py

No live traffic, network, credentials, or generalized BAA safety guarantee.
"""
from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timezone

from continuity.canary_admission import (
    CanaryAdmissionPolicy, CanaryGuardrails, CanaryScope, CanarySnapshot, CanaryStage,
)
from continuity.contracts import Denied, Intent
from continuity.db import RuntimeDB
from continuity.demo import CounterAdapter, CounterVerifier
from continuity.runtime import ContinuityRuntime


def scenario() -> dict:
    now = datetime.now(timezone.utc)
    snapshot = {"value": CanarySnapshot(
        revision="deploy-r3", evidence_ref="mock-telemetry:r3",
        observed_at=now, current_weight=10, current_stage_index=0,
        rollback_ready=True, duration_seconds=600,
        candidate_requests=100, control_requests=900,
        candidate_error_rate=0.2, control_error_rate=0.01,
        candidate_p95_ms=150, control_p95_ms=120,
    )}
    scope = CanaryScope(
        provider="traffic", action="promote", resource_ref="deploy:demo",
        experiment_id="experiment:demo", source_ref="traffic:current-version",
        stages=(CanaryStage(10, 60, 100), CanaryStage(50, 60, 100),
                CanaryStage(100, 60, 100)),
        guardrails=CanaryGuardrails(0.05, 0.02, 250, 1.5),
    )
    policy = CanaryAdmissionPolicy(scope, lambda: snapshot["value"], clock=lambda: now)
    db = RuntimeDB("sqlite:///:memory:", create_schema=True)
    provider = CounterAdapter()
    runtime = ContinuityRuntime(
        db, adapters={"traffic": provider},
        admission_profiles={"canary-stage-v1": policy},
        version_authorities={"traffic:current-version": lambda: snapshot["value"].revision},
        verifiers={"traffic": CounterVerifier()},
    )
    try:
        runtime.issue_grant(
            grant_id="demo-traffic-grant", principal="mission:demo",
            actors=["agent:one", "workflow:two"],
            provider="traffic", resource_prefix="deploy:demo", action="promote",
            required_version_refs=["traffic:current-version"],
            admission_profile="canary-stage-v1",
        )
        runtime.observe(
            actor="agent:one", ref="traffic:current-version",
            source_revision="deploy-r3", evidence_ref="mock:state-report",
        )
        runtime.submit(Intent(
            operation_id="traffic-op:demo", principal="mission:demo",
            provider="traffic", resource_ref="deploy:demo", action="promote",
            payload={"experiment_id": "experiment:demo",
                     "stage_index": 1, "weight_percent": 50},
            authority_id="demo-traffic-grant",
            expected_versions={"traffic:current-version": "deploy-r3"},
        ), actor="agent:one", participants=["agent:one", "workflow:two"])
        denied = False
        try:
            runtime.dispatch("traffic-op:demo", actor="workflow:two")
        except Denied:
            denied = True
        calls_after_denial = provider.calls
        # The trusted simulated telemetry source now reports a safe window.
        snapshot["value"] = replace(snapshot["value"], candidate_error_rate=0.01)
        dispatched = runtime.dispatch("traffic-op:demo", actor="workflow:two")
        reconciled = runtime.reconcile("traffic-op:demo", actor="agent:one")
        result = runtime.verify_outcome("traffic-op:demo", actor="workflow:two")
        return {
            "denied_unsafe": denied,
            "calls_after_denial": calls_after_denial,
            "calls_total": provider.calls,
            "dispatch_status": dispatched["status"],
            "effect": reconciled["effect_disposition"],
            "outcome": result["outcome_disposition"],
            "admission_profile": result["admission"]["policy_version"],
        }
    finally:
        db.close()


if __name__ == "__main__":
    print(json.dumps(scenario(), indent=2))
