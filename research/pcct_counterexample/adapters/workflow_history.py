from __future__ import annotations

import json
from pathlib import Path

from ..model import Effect, Grant, Obligation, SourceCut


def read_workflow_history_cut(path: Path) -> SourceCut:
    raw = path.read_bytes()
    document = json.loads(raw)
    steps = document["workflow_history"]
    if not steps or [row["event_index"] for row in steps] != list(range(1, len(steps) + 1)):
        raise ValueError("workflow history is empty or non-contiguous")
    header = document["snapshot"]
    obligations: list[Obligation] = []
    effects: list[Effect] = []
    grants: list[Grant] = []
    goal_from = ""
    goal_to = ""
    goal_decision_ref = ""
    for step in steps:
        payload = step["result"]
        match step["event_type"]:
            case "SnapshotPinned":
                continue
            case "DecisionApplied":
                goal_from = payload["superseded_definition"]
                goal_to = payload["active_definition"]
                goal_decision_ref = payload["owner_decision"]
            case "TaskRegistered":
                obligations.append(
                    Obligation(payload["task_key"], payload["assignee"], payload["required_action"])
                )
            case "ExternalOperationReadBack":
                effects.append(
                    Effect(payload["idempotency_key"], payload["result"], payload.get("readback"))
                )
            case "AuthorizationProjection":
                grants.extend(
                    Grant(
                        row["authorization_id"],
                        row["case"],
                        row["principal"],
                        row["capability"],
                        row["evidence_revision"],
                        row["state"],
                        row["issuer"],
                    )
                    for row in payload["authorizations"]
                )
            case _:
                raise ValueError(f"unsupported workflow event: {step['event_type']}")
    return SourceCut(
        scope=header["case"],
        source_id=header["owner_system"],
        source_format="workflow-history/v1",
        cut_id=header["history_cut"],
        query_id=header["projection_query"],
        revision=header["evidence_revision"],
        current_revision=header["current_revision"],
        sequence_start=steps[0]["event_index"],
        sequence_end=steps[-1]["event_index"],
        raw_bytes=raw,
        obligations=obligations,
        effects=effects,
        grants=grants,
        goal_from=goal_from,
        goal_to=goal_to,
        goal_decision_ref=goal_decision_ref,
    )
