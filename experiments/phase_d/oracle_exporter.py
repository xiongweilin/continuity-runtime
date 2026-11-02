"""Derive Phase D oracle rows from independent append-only ledgers only."""
from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any

from experiments.phase_c.provider_service import read_verified_ledger
from experiments.phase_d.fault_proxy import read_proxy_ledger

from .domain_owner import GOALS
from .provider_service import ACTIONS

ARMS = ("continuity", "postgres_dbos")
GOAL_PREFIX = (
    "goal_created", "evidence_revised", "goal_superseded", "handoff_authorized",
)
FAULT_BY_SCENARIO = {
    "normal_supersession": "not_applicable",
    "pack_ack_lost_then_supersession": "ack_dropped_after_pack_commit",
    "unauthorized_actor_during_handoff": "outsider_denied",
    "new_action_crash_before_send": "reservation_and_sigkill_before_quarantine_post",
}


def _ref(path_label: str, line_or_seq: int, digest: str) -> str:
    return f"{path_label}:seq={line_or_seq}:sha256={digest}"


def _source_revisions(events: list[dict[str, Any]]) -> list[str]:
    return [
        row["event"].get("revision")
        for row in events
        if row["event"].get("type") == "source_revision_published"
    ]


def _goal_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        row["event"] for row in events
        if row["event"].get("type") in set(GOAL_PREFIX) | {"goal_satisfied", "goal_unresolved"}
    ]


def _proxy_events(path: Path) -> tuple[list[dict[str, Any]], str]:
    rows, head, _raw = read_proxy_ledger(str(path))
    return [row["event"] for row in rows], head


def derive_oracle(
    *,
    scenario: str,
    seed: int,
    arm: str,
    relative_base: str,
    provider_path: Path,
    proxy_path: Path,
    source_path: Path,
    goal_path: Path,
    authority_path: Path,
    outsider_result: dict[str, Any] | None,
    crash_proof: dict[str, Any] | None,
) -> dict[str, Any]:
    """Create the exact precommitted oracle schema from ledger records.

    No arm-reported outcome or trial-row field is an input to this function.
    """
    if scenario not in FAULT_BY_SCENARIO or arm not in ARMS or type(seed) is not int:
        raise ValueError("invalid oracle identity")
    provider_rows, provider_head, _ = read_verified_ledger(provider_path)
    proxy_events, proxy_head = _proxy_events(proxy_path)
    source_rows, source_head, _ = read_verified_ledger(source_path)
    goal_rows, goal_head, _ = read_verified_ledger(goal_path)
    authority_rows, authority_head, _ = read_verified_ledger(authority_path)
    provider_events = [row["event"] for row in provider_rows]
    authority_events = [row["event"] for row in authority_rows]
    goal_events = _goal_events(goal_rows)
    source_revisions = _source_revisions(source_rows)

    commits = [event for event in provider_events if event.get("type") == "effect_committed"]
    received_rows = [
        row for row in provider_rows
        if row["event"].get("type") == "effect_post_received"
    ]
    proxy_posts = [
        event for event in proxy_events
        if event.get("type") == "upstream_attempt_started"
        and event.get("method") == "POST" and event.get("forwarded") is True
    ]
    commit_by_post = {event.get("post_seq"): event for event in commits}
    if len(commit_by_post) != len(commits):
        raise ValueError("provider ledger contains duplicate request-to-commit mapping")
    if {row.get("seq") for row in received_rows} != set(commit_by_post):
        raise ValueError("provider POST records and committed effects are not one-to-one")
    if len(proxy_posts) != len(received_rows):
        raise ValueError("fault-proxy forwarded POST count differs from provider ledger")

    action_counts = Counter(event.get("action") for event in commits)
    post_counts: Counter[str] = Counter()
    for event in commits:
        action = event.get("action")
        if action not in ACTIONS:
            raise ValueError("provider ledger contains an unknown action")
        post_counts[action] += 1
    effects_by_key: Counter[tuple[str, str]] = Counter()
    forbidden = 0
    for event in commits:
        action = event.get("action")
        effects_by_key[(action, event.get("effect_key"))] += 1
        if (
            action == "ship"
            or event.get("actor") not in {"agent-a", "agent-b"}
            or event.get("provider") != "phase-d-mock-shipment"
            or event.get("source_revision") != ("v1" if action in {"pack", "ship"} else "v2")
        ):
            forbidden += 1
    duplicates = sum(max(count - 1, 0) for count in effects_by_key.values())
    counts = {action: int(action_counts.get(action, 0)) for action in ACTIONS}
    post_counts_row = {action: int(post_counts.get(action, 0)) for action in ACTIONS}

    expected_fault = FAULT_BY_SCENARIO[scenario]
    fault_observed = "not_applicable"
    if scenario == "pack_ack_lost_then_supersession":
        drops = [event for event in proxy_events if event.get("type") == "fault_triggered"]
        pack_commit_rows = [
            row for row in provider_rows
            if row["event"].get("type") == "effect_committed"
            and row["event"].get("action") == "pack"
        ]
        pack_commits = [row["event"] for row in pack_commit_rows]
        expected_commit_ref = None
        if len(pack_commit_rows) == 1:
            wrapper = pack_commit_rows[0]
            expected_commit_ref = (
                f"mock-provider-commit:{wrapper['event']['trial_id']}:{wrapper['seq']}:"
                f"sha256={wrapper['hash']}"
            )
        if (
            len(drops) == 1
            and drops[0].get("fault") == expected_fault
            and drops[0].get("action") == "pack"
            and len(pack_commits) == 1
            and drops[0].get("provider_commit_ref") == expected_commit_ref
        ):
            fault_observed = expected_fault
    elif scenario == "unauthorized_actor_during_handoff":
        owner_denials = [
            event for event in authority_events
            if event.get("type") == "domain_write_denied"
            and event.get("path") == "/v1/owner/grant-agent-b"
        ]
        runtime_denied = bool(outsider_result) and outsider_result.get("denied") is True
        if owner_denials and runtime_denied and not any(event.get("actor") == "outsider" for event in commits):
            fault_observed = expected_fault
    elif scenario == "new_action_crash_before_send":
        proxy_quarantine_posts = [
            event for event in proxy_posts if event.get("action") == "quarantine"
        ]
        valid_crash = (
            bool(crash_proof)
            and crash_proof.get("barrier_reached") is True
            and crash_proof.get("reservation_generation") == 1
            and crash_proof.get("worker_exit_code") == -9
            and crash_proof.get("release_marker_present") is False
            and crash_proof.get("worker_pid") == (crash_proof.get("barrier_event") or {}).get("pid")
            and not proxy_quarantine_posts
            and post_counts_row["quarantine"] == 0
        )
        if valid_crash:
            fault_observed = expected_fault

    handoffs = [event for event in goal_events if event.get("type") == "handoff_authorized"]
    handoff_actor = handoffs[-1].get("actor") if handoffs else ""
    old_goal_retained = any(
        event.get("type") == "goal_created" and event.get("goal_id") == GOALS["old"]
        for event in goal_events
    ) and any(
        event.get("type") == "goal_superseded" and event.get("old_goal") == GOALS["old"]
        for event in goal_events
    )
    pack_readback_verified = any(
        event.get("type") == "pack_readback_verified"
        and event.get("counts") == {"pack": 1, "ship": 0, "quarantine": 0}
        for event in authority_events
    )
    historical_pack_retained = counts["pack"] >= 1 and pack_readback_verified

    expected_status = "unresolved" if scenario == "new_action_crash_before_send" else "satisfied"
    expected_goal_events = list(GOAL_PREFIX) + [f"goal_{expected_status}"]
    if [event.get("type") for event in goal_events] != expected_goal_events:
        raise ValueError("domain goal history is not the exact frozen event sequence")
    if source_revisions != ["v1", "v2"]:
        raise ValueError("source ledger is not exactly the frozen v1/v2 history")
    if fault_observed != expected_fault:
        raise ValueError("required Phase D fault proof is absent or inconsistent")
    unexpected_faults = [
        event for event in proxy_events if event.get("type") == "fault_triggered"
        and not (scenario == "pack_ack_lost_then_supersession"
                 and event.get("fault") == "ack_dropped_after_pack_commit")
    ]
    if unexpected_faults:
        raise ValueError("fault proxy ledger contains an unplanned injected fault")

    # Human-readable locators point into the copied raw hash-chained ledgers.
    provider_last = provider_rows[-1]
    goal_last = goal_rows[-1]
    source_last = next(
        row for row in reversed(source_rows)
        if row["event"].get("type") == "source_revision_published"
        and row["event"].get("revision") == "v2"
    )
    return {
        "scenario": scenario,
        "seed": seed,
        "arm": arm,
        "pack_commits": counts["pack"],
        "ship_commits": counts["ship"],
        "quarantine_commits": counts["quarantine"],
        "pack_post_count": post_counts_row["pack"],
        "ship_post_count": post_counts_row["ship"],
        "quarantine_post_count": post_counts_row["quarantine"],
        "duplicate_commits": duplicates,
        "forbidden_commits": forbidden,
        "source_revisions": source_revisions,
        "goal_event_types": [event["type"] for event in goal_events],
        "handoff_actor": handoff_actor,
        "old_goal_retained": bool(old_goal_retained),
        "historical_pack_retained": bool(historical_pack_retained),
        "outsider_denied": bool(scenario == "unauthorized_actor_during_handoff"
            and outsider_result and outsider_result.get("denied") is True and owner_denials),
        "provider_ledger_ref": (
            f"{relative_base}/provider_ledger.jsonl#seq={provider_last['seq']}:sha256={provider_head}"
        ),
        "goal_ledger_ref": (
            f"{relative_base}/goal_ledger.jsonl#seq={goal_last['seq']}:sha256={goal_head}"
        ),
        "source_ledger_ref": (
            f"{relative_base}/source_ledger.jsonl#seq={source_last['seq']}:sha256={source_head}"
        ),
        "fault_observed": fault_observed,
    }
