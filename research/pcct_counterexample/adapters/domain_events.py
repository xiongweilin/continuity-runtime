from __future__ import annotations

import json
from pathlib import Path

from ..model import Effect, Grant, Obligation, SourceCut


def read_domain_event_cut(path: Path) -> SourceCut:
    raw = path.read_bytes()
    events = [json.loads(line) for line in raw.splitlines() if line.strip()]
    if not events or [row["seq"] for row in events] != list(range(1, len(events) + 1)):
        raise ValueError("domain event sequence is empty or non-contiguous")
    header = events[0]["data"]
    obligations: list[Obligation] = []
    effects: list[Effect] = []
    grants: list[Grant] = []
    goal_from = ""
    goal_to = ""
    goal_decision_ref = ""
    for row in events:
        data = row["data"]
        match row["event"]:
            case "scope.cut":
                continue
            case "goal.superseded":
                goal_from, goal_to = data["old_goal"], data["new_goal"]
                goal_decision_ref = data["decision_ref"]
            case "responsibility.opened":
                obligations.append(Obligation(data["id"], data["owner"], data["action"]))
            case "provider.effect":
                effects.append(Effect(data["effect_key"], data["status"], data.get("readback_ref")))
            case "authorization.snapshot":
                for grant in data["grants"]:
                    grants.append(
                        Grant(
                            grant["id"],
                            grant["scope"],
                            grant["actor"],
                            grant["action"],
                            grant["revision"],
                            grant["status"],
                            grant["issuer_id"],
                        )
                    )
            case _:
                raise ValueError(f"unsupported domain event: {row['event']}")
    scope = events[0]["scope"]
    if any(row["scope"] != scope for row in events):
        raise ValueError("domain event stream crosses scope boundary")
    return SourceCut(
        scope=scope,
        source_id=header["source_id"],
        source_format="append-only-domain-events/v1",
        cut_id=header["cut_id"],
        query_id=header["query_id"],
        revision=header["revision"],
        current_revision=header["current_revision"],
        sequence_start=events[0]["seq"],
        sequence_end=events[-1]["seq"],
        raw_bytes=raw,
        obligations=obligations,
        effects=effects,
        grants=grants,
        goal_from=goal_from,
        goal_to=goal_to,
        goal_decision_ref=goal_decision_ref,
    )
