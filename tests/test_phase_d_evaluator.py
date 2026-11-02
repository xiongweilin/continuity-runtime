"""Synthetic checks of Phase D evaluator rules; not Phase D runtime evidence."""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "phase_d_evaluator", ROOT / "scripts" / "evaluate_phase_d.py"
)
assert SPEC and SPEC.loader
e = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(e)
MANIFEST = json.loads(
    (ROOT / "experiments" / "phase_d_manifest.json").read_text(encoding="utf-8")
)


def fixture():
    trials, oracle = {}, {}
    for case in MANIFEST["scenarios"]:
        for seed in MANIFEST["seeds"]:
            for arm in MANIFEST["arms"]:
                key = (case["id"], seed, arm)
                satisfied = case["expected_goal_status"] == "satisfied"
                trials[key] = {
                    "scenario": case["id"], "seed": seed, "arm": arm,
                    "reported_goal_status": case["expected_goal_status"],
                    "old_ship_dispatch_rejected": True,
                    "old_pack_reconciled": True,
                    "v1_link_invalidated": True,
                    "a_write_grants_revoked": True,
                    "b_grant_newly_issued": True,
                    "b_operation_generation": 1,
                    "pack_readback_ref": "fixture:pack-readback",
                    "new_goal_verifier_ref": "fixture:domain-verifier" if satisfied else None,
                    "stale_dispatch_ref": "fixture:held",
                    "invalidation_ref": "fixture:link-invalidation",
                    "handoff_ref": "fixture:authorized-handoff",
                    "fault_proof_ref": None if case["fault"] == "none" else "fixture:fault-proof",
                    "elapsed_ms": 50.0,
                    "db_transactions": 5,
                    "manual_reconstruction_steps": 0,
                    "operator_seconds": None,
                    "model_cost_usd": 0,
                    "infra_cost_usd": None,
                }
                oracle[key] = {
                    "scenario": case["id"], "seed": seed, "arm": arm,
                    "pack_commits": case["expected_pack_commits"],
                    "ship_commits": case["expected_ship_commits"],
                    "quarantine_commits": case["expected_quarantine_commits"],
                    "pack_post_count": case["expected_pack_commits"],
                    "ship_post_count": case["expected_ship_commits"],
                    "quarantine_post_count": case["expected_quarantine_commits"],
                    "duplicate_commits": 0, "forbidden_commits": 0,
                    "source_revisions": ["v1", "v2"],
                    "goal_event_types": list(e.GOAL_PREFIX) + [
                        "goal_satisfied" if satisfied else "goal_unresolved"
                    ],
                    "handoff_actor": "agent-b",
                    "old_goal_retained": True,
                    "historical_pack_retained": True,
                    "outsider_denied": case["id"] == "unauthorized_actor_during_handoff",
                    "provider_ledger_ref": "fixture:provider",
                    "goal_ledger_ref": "fixture:goal",
                    "source_ledger_ref": "fixture:source",
                    "fault_observed": case["required_fault_proof"],
                }
    return trials, oracle


def test_complete_synthetic_pairs_are_incomplete_cost_evidence():
    trials, oracle = fixture()
    result = e.evaluate(MANIFEST, trials, oracle)
    assert result["paired_trials"] == 12
    assert result["verdict"] == "EVIDENCE_INCOMPLETE"
    assert not result["safety_violations"]
    assert result["arms"]["continuity"]["satisfied"] == 9
    assert result["arms"]["continuity"]["unresolved"] == 3


def test_missing_pair_rejected():
    trials, oracle = fixture()
    oracle.pop(next(iter(oracle)))
    with pytest.raises(ValueError, match="incomplete/unpaired"):
        e.evaluate(MANIFEST, trials, oracle)


def test_duplicate_provider_effect_fails():
    trials, oracle = fixture()
    oracle[("normal_supersession", 0, "continuity")]["pack_commits"] = 2
    oracle[("normal_supersession", 0, "continuity")]["duplicate_commits"] = 1
    assert e.evaluate(MANIFEST, trials, oracle)["verdict"] == "SAFETY_FAILURE"


def test_old_goal_erasure_fails():
    trials, oracle = fixture()
    oracle[("normal_supersession", 0, "continuity")]["old_goal_retained"] = False
    assert e.evaluate(MANIFEST, trials, oracle)["verdict"] == "SAFETY_FAILURE"


def test_false_goal_success_under_uncertain_quarantine_fails():
    trials, oracle = fixture()
    trials[("new_action_crash_before_send", 0, "continuity")][
        "reported_goal_status"
    ] = "satisfied"
    assert e.evaluate(MANIFEST, trials, oracle)["verdict"] == "SAFETY_FAILURE"


def test_unapproved_handoff_or_intruder_fails():
    trials, oracle = fixture()
    oracle[("unauthorized_actor_during_handoff", 1, "postgres_dbos")][
        "outsider_denied"
    ] = False
    assert e.evaluate(MANIFEST, trials, oracle)["verdict"] == "SAFETY_FAILURE"


def test_fault_barrier_mismatch_fails():
    trials, oracle = fixture()
    oracle[("pack_ack_lost_then_supersession", 0, "continuity")][
        "fault_observed"
    ] = "not_applicable"
    assert e.evaluate(MANIFEST, trials, oracle)["verdict"] == "SAFETY_FAILURE"


def test_historical_pack_not_reconciled_is_functionality_gap():
    trials, oracle = fixture()
    trials[("normal_supersession", 1, "continuity")][
        "old_pack_reconciled"
    ] = False
    assert e.evaluate(MANIFEST, trials, oracle)["verdict"] == "INCOMPLETE_FUNCTIONALITY"


def test_measured_cost_never_means_automatic_roi():
    trials, oracle = fixture()
    for row in trials.values():
        row["operator_seconds"] = 0.0
        row["infra_cost_usd"] = 0.0
    result = e.evaluate(MANIFEST, trials, oracle)
    assert result["cost_evidence_complete"]
    assert result["verdict"] == "REQUIRES_INDEPENDENT_REVIEW"
    assert result["comparison"]["manual_reconstruction_steps_delta"] == 0


def test_invalid_duplicate_jsonl_rejected(tmp_path):
    trials, _ = fixture()
    line = json.dumps(next(iter(trials.values())))
    path = tmp_path / "trials.jsonl"
    path.write_text(line + "\n" + line + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="duplicate"):
        e.read_rows(path, e.TRIAL_FIELDS, "trial")


def test_invalid_boolean_metric_rejected(tmp_path):
    trials, _ = fixture()
    row = next(iter(trials.values())).copy()
    row["db_transactions"] = True
    path = tmp_path / "trials.jsonl"
    path.write_text(json.dumps(row) + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="integer"):
        e.read_rows(path, e.TRIAL_FIELDS, "trial")


def test_missing_fault_reference_rejected_for_injected_scenario():
    trials, oracle = fixture()
    trials[("new_action_crash_before_send", 1, "continuity")][
        "fault_proof_ref"
    ] = None
    assert e.evaluate(MANIFEST, trials, oracle)["verdict"] == "SAFETY_FAILURE"
