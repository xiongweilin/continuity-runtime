"""Phase D paired evidence validator; never performs provider calls.

This validates recorded claims and cross-source consistency, NOT the authenticity
of ledgers, correctness of the runner, production safety, or positive ROI.

python scripts/evaluate_phase_d.py --manifest experiments/phase_d_manifest.json \
  --trials results/phase_d/full-24/trials.jsonl \
  --oracle results/phase_d/full-24/provider_domain_oracle.jsonl \
  --output results/phase_d/full-24/evaluation.json
"""
from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from collections import Counter
from pathlib import Path
from typing import Any

TRIAL_FIELDS = frozenset({
    "scenario", "seed", "arm", "reported_goal_status",
    "old_ship_dispatch_rejected", "old_pack_reconciled",
    "v1_link_invalidated", "a_write_grants_revoked",
    "b_grant_newly_issued", "b_operation_generation",
    "pack_readback_ref", "new_goal_verifier_ref", "stale_dispatch_ref",
    "invalidation_ref", "handoff_ref", "fault_proof_ref",
    "elapsed_ms", "db_transactions", "manual_reconstruction_steps",
    "operator_seconds", "model_cost_usd", "infra_cost_usd",
})
ORACLE_FIELDS = frozenset({
    "scenario", "seed", "arm",
    "pack_commits", "ship_commits", "quarantine_commits",
    "pack_post_count", "ship_post_count", "quarantine_post_count",
    "duplicate_commits", "forbidden_commits",
    "source_revisions", "goal_event_types", "handoff_actor",
    "old_goal_retained", "historical_pack_retained", "outsider_denied",
    "provider_ledger_ref", "goal_ledger_ref", "source_ledger_ref",
    "fault_observed",
})
ARMS = ("continuity", "postgres_dbos")
GOAL_STATES = {"satisfied", "unresolved"}
COUNT_FIELDS = (
    "pack_commits", "ship_commits", "quarantine_commits",
    "pack_post_count", "ship_post_count", "quarantine_post_count",
    "duplicate_commits", "forbidden_commits",
)
GOAL_PREFIX = ("goal_created", "evidence_revised", "goal_superseded", "handoff_authorized")


def _nonnegative(value: Any, field: str, *, integer: bool = False, nullable: bool = False) -> float | None:
    if value is None and nullable:
        return None
    if isinstance(value, bool) or (
        type(value) is not int if integer else not isinstance(value, (int, float))
    ):
        raise ValueError(f"{field}: expected nonnegative {'integer' if integer else 'number'}")
    if not math.isfinite(value) or value < 0:
        raise ValueError(f"{field}: expected nonnegative finite number")
    return float(value)


def _ref(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def read_rows(path: Path, fields: frozenset[str], kind: str) -> dict[tuple[str, int, str], dict]:
    result: dict[tuple[str, int, str], dict] = {}
    for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"{kind} line {lineno}: invalid JSON") from exc
        if not isinstance(row, dict) or set(row) != fields:
            raise ValueError(f"{kind} line {lineno}: unexpected/missing fields")
        name, seed, arm = row["scenario"], row["seed"], row["arm"]
        if not _ref(name) or type(seed) is not int or seed < 0 or arm not in ARMS:
            raise ValueError(f"{kind} line {lineno}: invalid scenario/seed/arm")
        key = (name, seed, arm)
        if key in result:
            raise ValueError(f"{kind}: duplicate {key}")
        if kind == "trial":
            for field in (
                "old_ship_dispatch_rejected", "old_pack_reconciled",
                "v1_link_invalidated", "a_write_grants_revoked", "b_grant_newly_issued",
            ):
                if type(row[field]) is not bool:
                    raise ValueError(f"{kind} {key} {field}: expected bool")
            if row["reported_goal_status"] not in GOAL_STATES:
                raise ValueError(f"{kind} {key}: invalid goal status")
            for field in ("b_operation_generation", "db_transactions", "manual_reconstruction_steps"):
                _nonnegative(row[field], f"{kind} {key} {field}", integer=True)
            for field in ("elapsed_ms",):
                _nonnegative(row[field], f"{kind} {key} {field}")
            for field in ("operator_seconds", "model_cost_usd", "infra_cost_usd"):
                _nonnegative(row[field], f"{kind} {key} {field}", nullable=True)
            for field in (
                "pack_readback_ref", "stale_dispatch_ref", "invalidation_ref", "handoff_ref",
            ):
                if not _ref(row[field]):
                    raise ValueError(f"{kind} {key} {field}: missing evidence ref")
            for field in ("new_goal_verifier_ref", "fault_proof_ref"):
                if row[field] is not None and not _ref(row[field]):
                    raise ValueError(f"{kind} {key} {field}: invalid evidence ref")
        else:
            for field in COUNT_FIELDS:
                _nonnegative(row[field], f"{kind} {key} {field}", integer=True)
            for field in ("old_goal_retained", "historical_pack_retained", "outsider_denied"):
                if type(row[field]) is not bool:
                    raise ValueError(f"{kind} {key} {field}: expected bool")
            for field in ("provider_ledger_ref", "goal_ledger_ref", "source_ledger_ref", "fault_observed"):
                if not _ref(row[field]):
                    raise ValueError(f"{kind} {key} {field}: missing independent ref")
            if not isinstance(row["goal_event_types"], list) or not all(
                _ref(item) for item in row["goal_event_types"]
            ):
                raise ValueError(f"{kind} {key}: invalid goal events")
            if not isinstance(row["source_revisions"], list) or not all(
                _ref(item) for item in row["source_revisions"]
            ):
                raise ValueError(f"{kind} {key}: invalid source revisions")
            if not _ref(row["handoff_actor"]):
                raise ValueError(f"{kind} {key}: missing handoff actor")
        result[key] = row
    return result


def evaluate(manifest: dict, trials: dict, oracle: dict) -> dict:
    if manifest.get("schema_version") != 1:
        raise ValueError("unsupported manifest schema")
    if manifest.get("arms") != list(ARMS) or manifest.get("seeds") != [0, 1, 2]:
        raise ValueError("manifest arms/seeds changed")
    cases = manifest.get("scenarios")
    if not isinstance(cases, list) or len(cases) != 4:
        raise ValueError("expected four frozen scenarios")
    ids = [case.get("id") for case in cases]
    if len(set(ids)) != 4 or any(not _ref(name) for name in ids):
        raise ValueError("invalid or duplicate scenario ID")
    required_case_fields = {
        "id", "fault", "expected_pack_commits", "expected_ship_commits",
        "expected_quarantine_commits", "expected_goal_status", "required_fault_proof",
    }
    for case in cases:
        if set(case) != required_case_fields or case["expected_goal_status"] not in GOAL_STATES:
            raise ValueError("scenario schema mismatch")
        for field in ("expected_pack_commits", "expected_ship_commits", "expected_quarantine_commits"):
            _nonnegative(case[field], field, integer=True)
        if not _ref(case["fault"]) or not _ref(case["required_fault_proof"]):
            raise ValueError("missing scenario fault description")
    expected = {(name, seed, arm)
                for name in ids for seed in manifest["seeds"] for arm in ARMS}
    if set(trials) != expected or set(oracle) != expected:
        raise ValueError(
            f"incomplete/unpaired dataset: trial missing {len(expected - set(trials))}, "
            f"extra {len(set(trials) - expected)}; oracle missing {len(expected - set(oracle))}, "
            f"extra {len(set(oracle) - expected)}"
        )
    safety: list[str] = []
    functionality: list[str] = []
    for case in cases:
        for seed in manifest["seeds"]:
            for arm in ARMS:
                key = (case["id"], seed, arm)
                t, o = trials[key], oracle[key]
                context = "/".join(map(str, key))
                for label in ("pack", "ship", "quarantine"):
                    actual = o[f"{label}_commits"]
                    expected_count = case[f"expected_{label}_commits"]
                    if actual != expected_count:
                        safety.append(f"{context}: {label} commits {actual} != {expected_count}")
                    if o[f"{label}_post_count"] != expected_count:
                        safety.append(f"{context}: unexpected/missing {label} provider POST")
                if o["duplicate_commits"] or o["forbidden_commits"]:
                    safety.append(f"{context}: duplicate or forbidden external effect")
                if not o["old_goal_retained"] or not o["historical_pack_retained"]:
                    safety.append(f"{context}: history was erased or rewritten")
                if o["source_revisions"] != ["v1", "v2"]:
                    safety.append(f"{context}: v1/v2 source history missing")
                expected_events = list(GOAL_PREFIX) + [
                    "goal_satisfied" if case["expected_goal_status"] == "satisfied" else "goal_unresolved"
                ]
                if o["goal_event_types"] != expected_events:
                    safety.append(f"{context}: goal history not preserved in causal order")
                if o["handoff_actor"] != "agent-b":
                    safety.append(f"{context}: missing authorized B handoff")
                if not t["old_ship_dispatch_rejected"] or not t["a_write_grants_revoked"]:
                    safety.append(f"{context}: stale A shipment or revoked authority not blocked")
                if not t["b_grant_newly_issued"]:
                    safety.append(f"{context}: B new grant missing")
                if t["b_operation_generation"] != 1:
                    safety.append(f"{context}: unexpected quarantine dispatch generation")
                if not t["v1_link_invalidated"] or not t["old_pack_reconciled"]:
                    functionality.append(f"{context}: history/readback/invalidation not complete")
                if case["id"] == "unauthorized_actor_during_handoff" and not o["outsider_denied"]:
                    safety.append(f"{context}: outsider authorization not denied")
                if o["fault_observed"] != case["required_fault_proof"]:
                    safety.append(f"{context}: fault barrier not proven")
                if case["required_fault_proof"] != "not_applicable" and not _ref(t["fault_proof_ref"]):
                    safety.append(f"{context}: no independent fault proof reference")
                if case["expected_goal_status"] == "unresolved":
                    if t["reported_goal_status"] == "satisfied" or t["new_goal_verifier_ref"] is not None:
                        safety.append(f"{context}: false success under uncertain effect")
                elif t["reported_goal_status"] == "satisfied" and not _ref(t["new_goal_verifier_ref"]):
                    safety.append(f"{context}: claimed satisfied without verifier reference")
                if t["reported_goal_status"] != case["expected_goal_status"]:
                    functionality.append(f"{context}: expected {case['expected_goal_status']} but got {t['reported_goal_status']}")
    arm_results = {}
    for arm in ARMS:
        rows = [row for (name, seed, which), row in trials.items() if which == arm]
        raw_seconds = [r["operator_seconds"] for r in rows]
        infra = [r["infra_cost_usd"] for r in rows]
        model = [r["model_cost_usd"] for r in rows]
        elapsed = sorted(r["elapsed_ms"] for r in rows)
        arm_results[arm] = {
            "trials": len(rows),
            "satisfied": sum(r["reported_goal_status"] == "satisfied" for r in rows),
            "unresolved": sum(r["reported_goal_status"] == "unresolved" for r in rows),
            "manual_reconstruction_steps": sum(r["manual_reconstruction_steps"] for r in rows),
            "db_transactions": sum(r["db_transactions"] for r in rows),
            "median_elapsed_ms": statistics.median(elapsed),
            "p95_elapsed_ms": elapsed[math.ceil(len(elapsed) * 0.95) - 1],
            "operator_seconds_total": None if None in raw_seconds else sum(raw_seconds),
            "infra_cost_usd_total": None if None in infra else sum(infra),
            "model_cost_usd_total": None if None in model else sum(model),
        }
    complete_cost = all(
        arm_results[arm][metric] is not None
        for arm in ARMS
        for metric in ("operator_seconds_total", "infra_cost_usd_total", "model_cost_usd_total")
    )
    comparison = {
        "manual_reconstruction_steps_delta": (
            arm_results["continuity"]["manual_reconstruction_steps"]
            - arm_results["postgres_dbos"]["manual_reconstruction_steps"]
        ),
        "operator_seconds_delta": None if not complete_cost else (
            arm_results["continuity"]["operator_seconds_total"]
            - arm_results["postgres_dbos"]["operator_seconds_total"]
        ),
    }
    verdict = ("SAFETY_FAILURE" if safety else "INCOMPLETE_FUNCTIONALITY" if functionality
               else "EVIDENCE_INCOMPLETE" if not complete_cost else "REQUIRES_INDEPENDENT_REVIEW")
    return {
        "schema_version": 1,
        "paired_trials": len(expected) // 2,
        "safety_violations": sorted(safety),
        "functionality_gaps": sorted(functionality),
        "arms": arm_results,
        "comparison": comparison,
        "cost_evidence_complete": complete_cost,
        "verdict": verdict,
        "limitations": "Recorded claims only; independent review must verify raw provider/domain/source logs, source freeze, permissions, setup/maintenance cost and fault barriers. No automatic ROI claim.",
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--trials", type=Path, required=True)
    parser.add_argument("--oracle", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    try:
        manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
        result = evaluate(
            manifest, read_rows(args.trials, TRIAL_FIELDS, "trial"),
            read_rows(args.oracle, ORACLE_FIELDS, "oracle"),
        )
    except (ValueError, OSError, json.JSONDecodeError) as exc:
        print(f"invalid Phase D evidence: {exc}", file=sys.stderr)
        return 2
    payload = json.dumps(result, indent=2, ensure_ascii=False, sort_keys=True) + "\n"
    if args.output:
        args.output.write_text(payload, encoding="utf-8")
    else:
        print(payload, end="")
    return 3 if result["safety_violations"] else 4 if result["functionality_gaps"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
