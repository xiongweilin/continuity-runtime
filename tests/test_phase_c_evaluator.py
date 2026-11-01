"""Synthetic evaluator fixtures test evidence checks, not real benchmark outcomes."""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "evaluate_phase_c.py"
SPEC = importlib.util.spec_from_file_location("phase_c_evaluator", SCRIPT)
assert SPEC and SPEC.loader
mod = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(mod)
MANIFEST = json.loads((ROOT / "experiments" / "phase_c_manifest.json").read_text())


def fixture():
    reports, oracle = {}, {}
    for case in MANIFEST["scenarios"]:
        for seed in range(MANIFEST["seed_count"]):
            for arm in mod.ARMS:
                key = (case["id"], seed, arm)
                effect = case["expected_effect"]
                reports[key] = {
                    "scenario": case["id"], "seed": seed, "arm": arm,
                    "reported_status": case["expected_reported_status"],
                    "outcome_evidence": case["expected_reported_status"] == "satisfied",
                    "elapsed_ms": 20.0, "provider_calls": int(effect),
                    "db_transactions": 3, "recovery_actions": 0,
                    "manual_interventions": 0,
                    "operator_seconds": None, "model_cost_usd": None,
                    "infra_cost_usd": None,
                }
                oracle[key] = {
                    "scenario": case["id"], "seed": seed, "arm": arm,
                    "commits": int(effect), "duplicate_commits": 0,
                    "forbidden_commits": 0, "effect_present": effect,
                    "provider_post_count": int(effect),
                    "oracle_log_ref": f"fixture:{case['id']}:{seed}:{arm}",
                }
    return reports, oracle


def test_complete_pairs_safe_but_costs_unknown():
    reports, oracle = fixture()
    result = mod.evaluate(MANIFEST, reports, oracle)
    assert result["paired_trials_per_arm"] == 80
    assert result["safety_violations"] == []
    assert result["verdict"] == "EVIDENCE_INCOMPLETE"
    assert not result["cost_claim_possible"]


def test_missing_pair_rejected():
    reports, oracle = fixture()
    reports.pop(next(iter(reports)))
    with pytest.raises(ValueError, match="incomplete/unpaired"):
        mod.evaluate(MANIFEST, reports, oracle)


def test_duplicate_provider_commit_is_safety_failure():
    reports, oracle = fixture()
    key = ("clean", 0, "continuity")
    oracle[key].update(commits=2, duplicate_commits=1, provider_post_count=2)
    reports[key]["provider_calls"] = 2
    outcome = mod.evaluate(MANIFEST, reports, oracle)
    assert outcome["verdict"] == "SAFETY_FAILURE"
    assert outcome["safety_violations"]


def test_false_goal_satisfaction_rejected():
    reports, oracle = fixture()
    key = ("timeout_before_send", 3, "postgres_dbos")
    reports[key].update(reported_status="satisfied", outcome_evidence=True)
    assert mod.evaluate(MANIFEST, reports, oracle)["verdict"] == "SAFETY_FAILURE"


def test_blocked_claim_under_ambiguity_rejected():
    reports, oracle = fixture()
    reports[("crash_after_reservation_before_send", 4, "continuity")][
        "reported_status"] = "blocked"
    assert mod.evaluate(MANIFEST, reports, oracle)["verdict"] == "SAFETY_FAILURE"


def test_recovery_gap_not_silent_success():
    reports, oracle = fixture()
    reports[("ack_lost_after_commit", 1, "postgres_dbos")][
        "reported_status"] = "unknown"
    assert mod.evaluate(MANIFEST, reports, oracle)["verdict"] == "INCOMPLETE_RECOVERY"


def test_jsonl_duplicate_and_malformed_rejected(tmp_path):
    reports, _ = fixture()
    row = next(iter(reports.values()))
    path = tmp_path / "evidence.jsonl"
    path.write_text(json.dumps(row) + "\n" + json.dumps(row))
    with pytest.raises(ValueError, match="duplicate trial"):
        mod._load_jsonl(path, mod.RUN_FIELDS, "results")
    path.write_text("{invalid\n")
    with pytest.raises(ValueError, match="invalid JSON"):
        mod._load_jsonl(path, mod.RUN_FIELDS, "results")


def test_no_cost_claim_until_all_components_measured():
    reports, oracle = fixture()
    for row in reports.values():
        row.update(operator_seconds=0.0, model_cost_usd=0.0, infra_cost_usd=0.0)
    result = mod.evaluate(MANIFEST, reports, oracle)
    assert result["cost_claim_possible"]
    assert result["verdict"] == "REQUIRES_INDEPENDENT_REVIEW"


def test_invalid_negative_metric_rejected(tmp_path):
    reports, _ = fixture()
    row = next(iter(reports.values())).copy()
    row["manual_interventions"] = -1
    path = tmp_path / "trials.jsonl"
    path.write_text(json.dumps(row) + "\n")
    with pytest.raises(ValueError, match="nonnegative integer"):
        mod._load_jsonl(path, mod.RUN_FIELDS, "results")
