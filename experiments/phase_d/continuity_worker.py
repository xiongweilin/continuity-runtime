"""DBOS workflows that exercise the existing Continuity API for Phase D."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any

from dbos import DBOS, DBOSConfig, SetWorkflowID
from sqlalchemy import create_engine

from continuity.contracts import Denied, Held, Intent
from continuity.db import RuntimeDB
from continuity.runtime import ContinuityRuntime
from experiments.phase_c.shared import PhaseCBarrier

from .shared import (
    CASE_REF,
    NEW_GOAL,
    OLD_GOAL,
    PROVIDER_NAME,
    SOURCE_REF,
    PhaseDOperationVerifier,
    PhaseDProviderAdapter,
    PhaseDVersionPinPolicy,
    TrustedSourceReader,
)

_RUNTIME: ContinuityRuntime | None = None
_ADAPTER: PhaseDProviderAdapter | None = None
_BARRIER: PhaseCBarrier | None = None
_HISTORY: list[dict[str, Any]] = []


def _runtime() -> ContinuityRuntime:
    if _RUNTIME is None:
        raise RuntimeError("Continuity runtime was not initialized")
    return _RUNTIME


def _adapter() -> PhaseDProviderAdapter:
    if _ADAPTER is None:
        raise RuntimeError("Phase D provider adapter was not initialized")
    return _ADAPTER


def _barrier() -> PhaseCBarrier:
    if _BARRIER is None:
        raise RuntimeError("Phase D barriers were not initialized")
    return _BARRIER


def _emit(event_type: str, **details: Any) -> None:
    row = {"type": event_type, "at_monotonic_ns": time.monotonic_ns(), **details}
    _HISTORY.append(row)
    print("PHASED_EVENT=" + json.dumps(row, sort_keys=True, separators=(",", ":")), flush=True)


def _intent(operation_id: str, action: str, grant_id: str, revision: str,
            payload_extra: dict[str, Any] | None = None) -> Intent:
    payload = {"case_ref": CASE_REF, "quantity": 1}
    if payload_extra:
        payload.update(payload_extra)
    return Intent(
        operation_id=operation_id,
        principal="shipment-owner",
        provider=PROVIDER_NAME,
        # One case resource plus action-scoped grants lets the provider oracle
        # report the full pack/ship/quarantine state for the case.
        resource_ref=CASE_REF,
        action=action,
        payload=payload,
        authority_id=grant_id,
        expected_versions={SOURCE_REF: revision},
        effect_key=f"phase-d-effect:{CASE_REF}:{action}",
    )


def _public_operation(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    names = (
        "id", "status", "dispatch_generation", "effect_disposition",
        "effect_evidence_ref", "outcome_disposition", "outcome_evidence_ref",
        "authority_id", "expected_versions", "participants", "action",
    )
    return {name: row.get(name) for name in names}


def _register_and_dispatch(
    operation_id: str, action: str, grant_id: str, revision: str,
    payload_extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    actor = os.environ["PHASED_ACTOR"]
    intent = _intent(operation_id, action, grant_id, revision, payload_extra)
    runtime = _runtime()
    registered = runtime.submit(intent, actor=actor, participants=[actor])
    if registered.get("status") != "registered":
        raise RuntimeError(f"{action} registration not durable: {registered.get('status')}")
    try:
        dispatched = runtime.dispatch(operation_id, actor=actor)
        _emit("dispatch_returned", operation_id=operation_id, action=action,
              operation=_public_operation(dispatched))
    except (Held, Denied) as exc:
        blocked = runtime.get_operation(operation_id, actor=actor)
        _emit("dispatch_rejected", operation_id=operation_id, action=action,
              reason=type(exc).__name__, message=str(exc), operation=_public_operation(blocked))
        return {"operation": _public_operation(blocked), "rejected": True,
                "reason": type(exc).__name__}

    current = runtime.get_operation(operation_id, actor=actor)
    if current.get("status") in {"may_have_been_sent", "pending_verification", "unknown"}:
        current = runtime.reconcile(operation_id, actor=actor)
        _emit("operation_reconciled", operation_id=operation_id, action=action,
              operation=_public_operation(current))
    if current.get("effect_disposition") == "present":
        current = runtime.verify_outcome(operation_id, actor=actor)
        _emit("operation_verified", operation_id=operation_id, action=action,
              operation=_public_operation(current))
    return {"operation": _public_operation(current), "rejected": False}


@DBOS.step()
def _record_v1_observation_and_link(v1_evidence_ref: str) -> dict[str, Any]:
    actor = os.environ["PHASED_ACTOR"]
    observation = _runtime().observe(
        actor=actor,
        ref=SOURCE_REF,
        source_revision="v1",
        evidence_ref=v1_evidence_ref,
        expected_revision=0,
    )
    link = _runtime().link(
        link_id=f"phase-d-link:{CASE_REF}:old-goal",
        actor=actor,
        source_ref=SOURCE_REF,
        target_ref=OLD_GOAL,
        relation="basis_for_goal",
        purpose="authorization",
        basis_ref=v1_evidence_ref,
        constraints={"depends_on": {SOURCE_REF: "v1"}},
        expected_revision=0,
    )
    _emit("v1_observed_and_linked", observation_revision=observation["revision"],
          link_revision=link["revision"], link_active=link["active"])
    return {"observation": observation, "link": link}


@DBOS.step()
def _run_pack_and_register_ship(pack_operation_id: str, ship_operation_id: str) -> dict[str, Any]:
    grants = json.loads(os.environ["PHASED_AGENT_GRANTS_JSON"])
    pack = _register_and_dispatch(
        pack_operation_id, "pack", grants["pack"], "v1",
    )
    if pack["rejected"] or pack["operation"].get("effect_disposition") != "present":
        raise RuntimeError("pack was not independently reconciled")
    ship_intent = _intent(ship_operation_id, "ship", grants["ship"], "v1")
    ship = _runtime().submit(ship_intent, actor="agent-a", participants=["agent-a"])
    if ship.get("status") != "registered" or ship.get("dispatch_generation") != 0:
        raise RuntimeError("old ship operation was not left registered before v2")
    _emit("pack_verified_ship_registered", pack_operation=_public_operation(pack["operation"]),
          ship_operation=_public_operation(ship))
    return {"pack": pack["operation"], "ship": _public_operation(ship)}


@DBOS.workflow()
def _agent_a_workflow(pack_operation_id: str, ship_operation_id: str, v1_evidence_ref: str,
                      pair_id: str) -> dict[str, Any]:
    source = TrustedSourceReader(
        os.environ["PHASED_SOURCE_URL"], SOURCE_REF,
        os.environ["PHASED_TRIAL_ID"], os.environ["PHASED_ARM"],
    )
    current = source()
    if current != "v1":
        raise RuntimeError("Agent A did not observe authoritative v1")
    _record_v1_observation_and_link(v1_evidence_ref)
    phase1 = _run_pack_and_register_ship(pack_operation_id, ship_operation_id)
    _barrier().announce_and_wait("a_ready_for_v2", pair_id)

    source_v2 = source.current()
    if source_v2 is None or source_v2.get("revision") != "v2":
        raise RuntimeError("Agent A did not observe authoritative v2 after owner release")
    v2 = _runtime().observe(
        actor="agent-a",
        ref=SOURCE_REF,
        source_revision="v2",
        evidence_ref=source_v2["source_event_ref"],
        expected_revision=1,
    )
    link = _runtime().get_link(f"phase-d-link:{CASE_REF}:old-goal")
    events = _runtime().events_for_actor(actor="agent-a", after=0, limit=1000)["events"]
    invalidations = [
        event for event in events
        if event.get("kind") == "dependency.invalidated"
        and event.get("payload", {}).get("source_ref") == SOURCE_REF
    ]
    if link.get("active") or not invalidations:
        raise RuntimeError("v1-dependent link did not become inactive")
    invalidation = invalidations[-1]
    _emit("v2_observed_link_invalidated", observation_revision=v2["revision"],
          link_active=link["active"], invalidation_event_seq=invalidation.get("seq"))

    stale_result = _register_and_dispatch(
        ship_operation_id, "ship", json.loads(os.environ["PHASED_AGENT_GRANTS_JSON"])["ship"], "v1",
    )
    if not stale_result["rejected"]:
        raise RuntimeError("stale ship dispatch was not rejected")
    stale_snapshot = stale_result["operation"]
    if stale_snapshot.get("dispatch_generation") != 0:
        raise RuntimeError("stale ship operation unexpectedly reserved dispatch")
    _barrier().announce("stale_ship_attempt_done", ship_operation_id)
    _emit("stale_ship_rejected_before_revocation", operation=_public_operation(stale_snapshot),
          rejection=stale_result.get("reason"))
    return {
        "pack_operation": _public_operation(phase1["pack"]),
        "ship_operation": _public_operation(stale_snapshot),
        "stale_rejection": stale_result.get("reason"),
        "link_active": False,
        "invalidation_event_seq": invalidation.get("seq"),
        "events": _HISTORY,
    }


@DBOS.step()
def _run_quarantine_operation(operation_id: str, grant_id: str, projection_path: str) -> dict[str, Any]:
    projection = json.loads(Path(projection_path).read_text(encoding="utf-8"))
    required = {
        "actor", "goal", "basis", "verified_prior_work", "handoff_ref", "authority_ref",
    }
    if not isinstance(projection, dict) or set(projection) != required:
        raise RuntimeError("Agent B received an invalid domain projection")
    if (
        not isinstance(projection["goal"], dict)
        or set(projection["goal"]) != {"id", "status"}
        or not isinstance(projection["basis"], dict)
        or set(projection["basis"]) != {"revision", "fact", "evidence_ref", "source_event_ref"}
        or not isinstance(projection["verified_prior_work"], list)
        or len(projection["verified_prior_work"]) != 1
        or not isinstance(projection["verified_prior_work"][0], dict)
        or set(projection["verified_prior_work"][0]) != {"action", "count", "provider_readback_ref"}
    ):
        raise RuntimeError("Agent B projection contains fields outside the domain-owned schema")
    if (
        projection["actor"] != "agent-b"
        or projection["goal"].get("id") != NEW_GOAL
        or projection["goal"].get("status") != "active"
        or projection["basis"].get("revision") != "v2"
        or projection["basis"].get("fact") != "fraud_hold"
        or not projection["basis"].get("evidence_ref")
        or not projection["basis"].get("source_event_ref")
        or len(projection["verified_prior_work"]) != 1
        or projection["verified_prior_work"][0].get("action") != "pack"
        or projection["verified_prior_work"][0].get("count") != 1
        or not projection["verified_prior_work"][0].get("provider_readback_ref")
        or not projection["handoff_ref"]
        or not projection["authority_ref"]
    ):
        raise RuntimeError("Agent B projection does not authorize this action")
    operation = _register_and_dispatch(
        operation_id, "quarantine", grant_id, "v2",
        payload_extra={
            "domain_authority_ref": projection["authority_ref"],
            "handoff_ref": projection["handoff_ref"],
            "prior_pack_readback_ref": projection["verified_prior_work"][0]["provider_readback_ref"],
        },
    )
    return {
        "quarantine_operation": operation["operation"],
        "projection_sha256": hashlib.sha256(
            json.dumps(projection, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest(),
    }


@DBOS.workflow()
def _agent_b_workflow(operation_id: str, grant_id: str, projection_path: str) -> dict[str, Any]:
    if os.environ.get("PHASED_ACTOR") != "agent-b":
        raise RuntimeError("B workflow must run under Agent B identity")
    source = TrustedSourceReader(
        os.environ["PHASED_SOURCE_URL"], SOURCE_REF,
        os.environ["PHASED_TRIAL_ID"], os.environ["PHASED_ARM"],
    )
    if source() != "v2":
        raise RuntimeError("Agent B action is not guarded by trusted v2")
    return _run_quarantine_operation(operation_id, grant_id, projection_path)


def _initialize(arm: str) -> tuple[RuntimeDB, Any, PhaseDProviderAdapter, PhaseCBarrier]:
    global _RUNTIME, _ADAPTER, _BARRIER
    db_url = os.environ["PHASED_DATABASE_URL"]
    trial_id = os.environ["PHASED_TRIAL_ID"]
    actor = os.environ["PHASED_ACTOR"]
    _BARRIER = PhaseCBarrier(
        trial_id=trial_id,
        event_dir=os.environ["PHASED_BARRIER_EVENT_DIR"],
        release_dir=os.environ["PHASED_BARRIER_RELEASE_DIR"],
        timeout_seconds=90.0,
    )
    _ADAPTER = PhaseDProviderAdapter(
        base_url=os.environ["PHASED_PROXY_URL"],
        trial_id=trial_id,
        arm=arm,
        actor=actor,
        proxy_client_token=os.environ["PHASED_PROXY_CLIENT_TOKEN"],
        source_ref=SOURCE_REF,
        barrier=_BARRIER,
    )
    db = RuntimeDB(db_url, create_schema=True)
    engine = create_engine(db_url, pool_pre_ping=True)
    _RUNTIME = ContinuityRuntime(
        db,
        adapters={PROVIDER_NAME: _ADAPTER},
        verifiers={PROVIDER_NAME: PhaseDOperationVerifier()},
        version_authorities={SOURCE_REF: TrustedSourceReader(
            os.environ["PHASED_SOURCE_URL"], SOURCE_REF, trial_id, arm,
        )},
        admission_profiles={
            "phase-d-v1": PhaseDVersionPinPolicy("phase-d-v1", "v1"),
            "phase-d-v2": PhaseDVersionPinPolicy("phase-d-v2", "v2"),
        },
    )
    return db, engine, _ADAPTER, _BARRIER


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", choices=("agent-a", "agent-b"), required=True)
    parser.add_argument("--operation-id", action="append", required=True)
    parser.add_argument("--grant-id", required=True)
    parser.add_argument("--projection")
    args = parser.parse_args(argv)
    if os.name != "posix" or os.getuid() == 0:
        raise RuntimeError("Phase D arm worker must run as an unprivileged Linux UID")
    arm = os.environ["PHASED_ARM"]
    if arm != "continuity":
        raise ValueError("Continuity worker may only run the continuity arm")
    db, engine, _adapter_obj, _barrier_obj = _initialize(arm)
    DBOS(config=DBOSConfig(
        name="phase-d-continuity",
        application_version=os.environ["PHASED_APPLICATION_VERSION"],
        system_database_url=os.environ["PHASED_DATABASE_URL"],
    ))
    DBOS.launch()
    started_ns = time.perf_counter_ns()
    try:
        if args.stage == "agent-a":
            if os.environ.get("PHASED_ACTOR") != "agent-a" or len(args.operation_id) != 2:
                raise ValueError("Agent A requires pack and ship operation IDs")
            with SetWorkflowID(f"phase-d:{arm}:{os.environ['PHASED_TRIAL_ID']}:agent-a"):
                payload = _agent_a_workflow(
                    args.operation_id[0], args.operation_id[1],
                    os.environ["PHASED_V1_EVIDENCE_REF"], os.environ["PHASED_PAIR_ID"],
                )
        else:
            if os.environ.get("PHASED_ACTOR") != "agent-b" or len(args.operation_id) != 1:
                raise ValueError("Agent B requires one new quarantine operation ID")
            if not args.projection:
                raise ValueError("Agent B requires the domain-owned projection file")
            with SetWorkflowID(f"phase-d:{arm}:{os.environ['PHASED_TRIAL_ID']}:agent-b"):
                payload = _agent_b_workflow(args.operation_id[0], args.grant_id, args.projection)
    finally:
        DBOS.destroy()
        db.close()
        engine.dispose()
    payload = {
        **payload,
        "attempted_provider_posts": _ADAPTER.attempted_posts if _ADAPTER else 0,
        "attempted_provider_gets": _ADAPTER.attempted_gets if _ADAPTER else 0,
        "worker_elapsed_ms": round((time.perf_counter_ns() - started_ns) / 1e6, 3),
    }
    print("PHASED_RESULT=" + json.dumps(payload, sort_keys=True, separators=(",", ":")), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
