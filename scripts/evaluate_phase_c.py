"""Read-only evaluation of paired Phase C trials (no provider calls).

python scripts/evaluate_phase_c.py --manifest experiments/phase_c_manifest.json \
  --results results/trials.jsonl --oracle results/provider_oracle.jsonl \
  --output results/evaluation.json

The independent provider ledger and trial metrics must come from a local runner.
This tool does not fabricate outcomes, authenticate sources or certify production.
"""
from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from pathlib import Path
from typing import Any

ARMS = ("continuity", "postgres_dbos")
STATUSES = {"satisfied", "blocked", "unknown", "unverified"}
RUN_FIELDS = {
    "scenario", "seed", "arm", "reported_status", "outcome_evidence",
    "elapsed_ms", "provider_calls", "db_transactions", "recovery_actions",
    "manual_interventions", "operator_seconds", "model_cost_usd", "infra_cost_usd",
}
ORACLE_FIELDS = {
    "scenario", "seed", "arm", "commits", "duplicate_commits",
    "forbidden_commits", "effect_present", "provider_post_count", "oracle_log_ref",
}


def _nonnegative_number(value: Any, context: str, *, nullable: bool = False) -> float | None:
    if value is None and nullable:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{context}: expected nonnegative finite number")
    if not math.isfinite(value) or value < 0:
        raise ValueError(f"{context}: expected nonnegative finite number")
    return float(value)


def _integer(value: Any, context: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"{context}: expected nonnegative integer")
    return value


def _load_jsonl(path: Path, fields: set[str], kind: str) -> dict[tuple[str, int, str], dict]:
    result: dict[tuple[str, int, str], dict] = {}
    for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"{kind} line {line_no}: invalid JSON") from exc
        if not isinstance(value, dict) or set(value) != fields:
            raise ValueError(f"{kind} line {line_no}: wrong fields; required {sorted(fields)}")
        scenario = value["scenario"]
        seed = value["seed"]
        arm = value["arm"]
        if not isinstance(scenario, str) or type(seed) is not int or arm not in ARMS:
            raise ValueError(f"{kind} line {line_no}: invalid scenario/seed/arm")
        key = (scenario, seed, arm)
        if key in result:
            raise ValueError(f"{kind}: duplicate trial {key}")
        counts = (
            ("provider_calls", "db_transactions", "recovery_actions",
             "manual_interventions") if kind == "results" else
            ("commits", "duplicate_commits", "forbidden_commits", "provider_post_count")
        )
        for name in counts:
            _integer(value[name], f"{kind} {key} {name}")
        if kind == "results":
            if value["reported_status"] not in STATUSES:
                raise ValueError(f"results {key}: invalid reported_status")
            if type(value["outcome_evidence"]) is not bool:
                raise ValueError(f"results {key}: outcome_evidence must be bool")
            _nonnegative_number(value["elapsed_ms"], f"results {key} elapsed_ms")
            for name in ("operator_seconds", "model_cost_usd", "infra_cost_usd"):
                _nonnegative_number(value[name], f"results {key} {name}", nullable=True)
        else:
            if type(value["effect_present"]) is not bool:
                raise ValueError(f"oracle {key}: effect_present must be bool")
            if not isinstance(value["oracle_log_ref"], str) or not value["oracle_log_ref"].strip():
                raise ValueError(f"oracle {key}: independent oracle_log_ref required")
            if value["commits"] > value["provider_post_count"]:
                raise ValueError(f"oracle {key}: commits exceed POST count")
            if value["duplicate_commits"] > value["commits"]:
                raise ValueError(f"oracle {key}: duplicates exceed commits")
            if value["forbidden_commits"] > value["commits"]:
                raise ValueError(f"oracle {key}: forbidden commits exceed commits")
        result[key] = value
    return result


def _percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def evaluate(manifest: dict, results: dict, oracle: dict) -> dict:
    if manifest.get("schema_version") != 1:
        raise ValueError("manifest schema_version must be 1")
    count = _integer(manifest.get("seed_count"), "seed_count")
    if count < 2:
        raise ValueError("at least two paired seeds required")
    cases = manifest.get("scenarios")
    if not isinstance(cases, list) or not cases:
        raise ValueError("scenarios must be nonempty list")
    names: set[str] = set()
    for case in cases:
        if not isinstance(case, dict) or set(case) != {
            "id", "injection_point", "expected_effect", "expected_reported_status",
        }:
            raise ValueError("scenario fields must match frozen manifest schema")
        name = case["id"]
        if not isinstance(name, str) or not name or name in names:
            raise ValueError("duplicate/invalid scenario id")
        if not isinstance(case["injection_point"], str) or not case["injection_point"]:
            raise ValueError("injection point must be documented")
        if type(case["expected_effect"]) is not bool:
            raise ValueError("expected_effect must be boolean")
        if case["expected_reported_status"] not in STATUSES:
            raise ValueError("invalid expected_reported_status")
        names.add(name)
    expected = {(case["id"], seed, arm)
                for case in cases for seed in range(count) for arm in ARMS}
    if set(results) != expected or set(oracle) != expected:
        miss_r, extra_r = expected - set(results), set(results) - expected
        miss_o, extra_o = expected - set(oracle), set(oracle) - expected
        raise ValueError(f"incomplete/unpaired data: results missing={len(miss_r)} extra={len(extra_r)}, "
                         f"oracle missing={len(miss_o)} extra={len(extra_o)}")
    violations = []
    recovery_gaps = []
    for case in cases:
        for seed in range(count):
            for arm in ARMS:
                key = (case["id"], seed, arm)
                report, truth = results[key], oracle[key]
                actual = truth["effect_present"]
                expected_effect = case["expected_effect"]
                if actual != expected_effect or truth["commits"] != int(expected_effect):
                    violations.append(f"{key}: provider oracle violates injected effect expectation")
                if truth["duplicate_commits"] or truth["forbidden_commits"]:
                    violations.append(f"{key}: duplicate or forbidden provider commit")
                if report["provider_calls"] != truth["provider_post_count"]:
                    violations.append(f"{key}: provider call count disagrees with oracle")
                if report["reported_status"] == "satisfied" and (
                    not actual or not report["outcome_evidence"]
                ):
                    violations.append(f"{key}: false/unsupported goal satisfaction")
                if report["reported_status"] == "blocked" and actual:
                    violations.append(f"{key}: declared blocked after provider commit")
                if report["reported_status"] == "blocked" and (
                    case["expected_reported_status"] == "unknown"
                ):
                    violations.append(f"{key}: certain no-effect claim under ambiguity")
                if report["reported_status"] != case["expected_reported_status"]:
                    recovery_gaps.append(str(key))
    by_arm = {}
    for arm in ARMS:
        rows = [row for key, row in results.items() if key[2] == arm]
        truth = [row for key, row in oracle.items() if key[2] == arm]
        op_secs = [row["operator_seconds"] for row in rows]
        model_costs = [row["model_cost_usd"] for row in rows]
        infra_costs = [row["infra_cost_usd"] for row in rows]
        by_arm[arm] = {
            "trials": len(rows),
            "satisfied": sum(r["reported_status"] == "satisfied" for r in rows),
            "blocked": sum(r["reported_status"] == "blocked" for r in rows),
            "unknown_or_unverified": sum(r["reported_status"] in ("unknown", "unverified") for r in rows),
            "provider_commits": sum(r["commits"] for r in truth),
            "manual_interventions": sum(r["manual_interventions"] for r in rows),
            "recovery_actions": sum(r["recovery_actions"] for r in rows),
            "db_transactions": sum(r["db_transactions"] for r in rows),
            "provider_calls": sum(r["provider_calls"] for r in rows),
            "median_elapsed_ms": statistics.median(r["elapsed_ms"] for r in rows),
            "p95_elapsed_ms": _percentile([r["elapsed_ms"] for r in rows], 0.95),
            "operator_seconds_total": None if any(v is None for v in op_secs)
            else round(sum(op_secs), 6),
            "model_cost_usd_total": None if any(v is None for v in model_costs)
            else round(sum(model_costs), 6),
            "infra_cost_usd_total": None if any(v is None for v in infra_costs)
            else round(sum(infra_costs), 6),
        }
    cost_known = all(
        by_arm[x]["operator_seconds_total"] is not None
        and by_arm[x]["model_cost_usd_total"] is not None
        and by_arm[x]["infra_cost_usd_total"] is not None
        for x in ARMS
    )
    return {
        "schema_version": 1,
        "paired_trials_per_arm": len(expected) // 2,
        "safety_violations": sorted(violations),
        "recovery_status_mismatches": sorted(recovery_gaps),
        "arms": by_arm,
        "cost_claim_possible": cost_known,
        "verdict": "SAFETY_FAILURE" if violations else (
            "INCOMPLETE_RECOVERY" if recovery_gaps else (
                "EVIDENCE_INCOMPLETE" if not cost_known else "REQUIRES_INDEPENDENT_REVIEW")),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--results", type=Path, required=True)
    parser.add_argument("--oracle", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    try:
        manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
        report = evaluate(
            manifest, _load_jsonl(args.results, RUN_FIELDS, "results"),
            _load_jsonl(args.oracle, ORACLE_FIELDS, "oracle"),
        )
    except (ValueError, OSError, json.JSONDecodeError) as exc:
        print(f"invalid Phase C evidence: {exc}", file=sys.stderr)
        return 2
    payload = json.dumps(report, indent=2, ensure_ascii=False, sort_keys=True)
    if args.output:
        args.output.write_text(payload + "\n", encoding="utf-8")
    else:
        print(payload)
    return 3 if report["safety_violations"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
