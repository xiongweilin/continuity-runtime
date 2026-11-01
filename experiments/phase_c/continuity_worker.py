"""DBOS worker for Arm A, using the real Continuity runtime.

This module is run as a restricted child process inside the local Phase C
runner. It only sees a PostgreSQL URL scoped to the current trial and the
fault-proxy URL; it does not receive the provider key, fault schedule, or
provider ledger path.
"""
from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from typing import Any

from dbos import DBOS, DBOSConfig, SetWorkflowID
from sqlalchemy import Column, MetaData, String, Table, create_engine, select

from continuity.contracts import (
    Conflict,
    Denied,
    Held,
    Intent,
    UnknownOperation,
)
from continuity.db import RuntimeDB
from continuity.runtime import ContinuityRuntime

from .shared import PhaseCBarrier, PhaseCGoalVerifier, PhaseCProviderAdapter

_RUNTIME: ContinuityRuntime | None = None
_BARRIER: PhaseCBarrier | None = None
_ADAPTER: PhaseCProviderAdapter | None = None
_CONFIG: dict[str, Any] = {}
_ACTIONS: dict[str, int] = {"reconcile": 0, "verify": 0}

source_metadata = MetaData()
source_versions = Table(
    "phasec_authoritative_versions",
    source_metadata,
    Column("ref", String(256), primary_key=True),
    Column("revision", String(128), nullable=False),
)


def _runtime() -> ContinuityRuntime:
    if _RUNTIME is None:
        raise RuntimeError("Continuity runtime is not initialized")
    return _RUNTIME


def _barrier() -> PhaseCBarrier:
    if _BARRIER is None:
        raise RuntimeError("Phase C barriers are not initialized")
    return _BARRIER


@DBOS.step()
def _register_operation(operation_id: str, actor: str, *, pre_submit_barrier: bool) -> dict:
    runtime = _runtime()
    cfg = _CONFIG
    if pre_submit_barrier:
        _barrier().announce_and_wait("pre_submit", operation_id)
    intent = Intent(
        operation_id=operation_id,
        principal=cfg["principal"],
        provider=cfg["provider"],
        resource_ref=cfg["resource_ref"],
        action=cfg["action"],
        payload={"delta": 1},
        authority_id=cfg["grant_id"],
        expected_versions={cfg["version_ref"]: "v1"},
        effect_key=cfg["effect_key"],
    )
    try:
        try:
            row = runtime.get_operation(operation_id, actor=actor)
        except UnknownOperation:
            if pre_submit_barrier:
                _barrier().announce("submit_ready", operation_id)
                _barrier().announce_and_wait("submit_go", cfg["pair_id"])
                _barrier().announce("submit_started", operation_id)
                # A fixed, symmetric rendezvous hold makes concurrent submit
                # attempts observable even if a thread is briefly descheduled.
                time.sleep(0.1)
            try:
                row = runtime.submit(intent, actor=actor, participants=cfg["participants"])
            finally:
                if pre_submit_barrier:
                    _barrier().announce("submit_finished", operation_id)
    except Conflict as exc:
        return {"kind": "blocked", "reason": "operation_or_effect_conflict", "detail": str(exc)}
    except Denied as exc:
        return {"kind": "blocked", "reason": "authority_denied", "detail": str(exc)}
    # A fresh submit is durably committed before this barrier. The controller
    # independently checks status=registered and generation=0 before release.
    _barrier().announce_and_wait("registered", operation_id)
    return {"kind": "registered", "operation_id": operation_id, "status": row["status"]}


@DBOS.step()
def _dispatch_operation(operation_id: str, actor: str) -> dict:
    runtime = _runtime()
    try:
        row = runtime.dispatch(operation_id, actor=actor)
        return {"kind": "dispatched", "row": _public_row(row)}
    except (Denied, Held, Conflict) as exc:
        try:
            row = runtime.get_operation(operation_id, actor=actor)
        except Exception:
            row = {}
        return {
            "kind": "blocked",
            "reason": "dispatch_precondition",
            "detail": type(exc).__name__,
            "row": _public_row(row),
        }


@DBOS.step()
def _reconcile_operation(operation_id: str, actor: str) -> dict:
    _ACTIONS["reconcile"] += 1
    row = _runtime().reconcile(operation_id, actor=actor)
    return _public_row(row)


@DBOS.step()
def _verify_operation(operation_id: str, actor: str) -> dict:
    _ACTIONS["verify"] += 1
    try:
        return _public_row(_runtime().verify_outcome(operation_id, actor=actor))
    except Held as exc:
        return {"status": "unverified", "reason": type(exc).__name__}


@DBOS.workflow()
def _effect_workflow(operation_id: str, actor: str, *, pre_submit_barrier: bool) -> dict:
    registration = _register_operation(
        operation_id, actor, pre_submit_barrier=pre_submit_barrier,
    )
    if registration["kind"] == "blocked":
        return {"operation_id": operation_id, "reported_status": "blocked", "registration": registration}
    dispatched = _dispatch_operation(operation_id, actor)
    if dispatched["kind"] == "blocked":
        return {
            "operation_id": operation_id,
            "reported_status": "blocked",
            "registration": registration,
            "dispatch": dispatched,
            "operation": dispatched.get("row", {}),
        }
    row = dispatched["row"]
    if row.get("status") in {"may_have_been_sent", "pending_verification", "unknown"}:
        row = _reconcile_operation(operation_id, actor)
    if row.get("effect_disposition") == "present":
        row = _verify_operation(operation_id, actor)
        # The verifier step returns a raw operation row on success; on a held
        # outcome it returns a small unverified summary.
        if not row.get("outcome_disposition") and row.get("status") == "unverified":
            return {
                "operation_id": operation_id,
                "reported_status": "unverified",
                "registration": registration,
                "dispatch": dispatched,
                "operation": row,
            }
    reported = _reported_status(row)
    return {
        "operation_id": operation_id,
        "reported_status": reported,
        "registration": registration,
        "dispatch": dispatched,
        "operation": row,
    }


def _reported_status(row: dict) -> str:
    if row.get("outcome_disposition") == "satisfied" and row.get("outcome_evidence_ref"):
        return "satisfied"
    if row.get("outcome_disposition") == "not_satisfied":
        return "unverified"
    # Protocol's reported_status is the domain-goal state, not the internal
    # transport/operation label. An accepted receipt plus blocked readback is
    # therefore unverified; it is not evidence that the effect is present.
    if row.get("receipt_ref") and row.get("outcome_disposition") == "unverified":
        return "unverified"
    return "unknown"


def _public_row(row: dict) -> dict:
    if not row:
        return {}
    keys = (
        "id", "status", "dispatch_generation", "receipt_ref",
        "effect_disposition", "effect_evidence_ref", "outcome_disposition",
        "outcome_evidence_ref",
    )
    return {key: row.get(key) for key in keys}


def _ensure_setup(runtime: ContinuityRuntime, engine, cfg: dict) -> None:
    source_metadata.create_all(engine)
    with engine.begin() as cx:
        row = cx.execute(select(source_versions.c.revision).where(
            source_versions.c.ref == cfg["version_ref"],
        )).scalar_one_or_none()
        if row is None:
            cx.execute(source_versions.insert().values(
                ref=cfg["version_ref"], revision="v1",
            ))
    try:
        runtime.get_grant(cfg["grant_id"])
    except UnknownOperation:
        runtime.issue_grant(
            grant_id=cfg["grant_id"],
            principal=cfg["principal"],
            actors=cfg["participants"],
            provider=cfg["provider"],
            resource_prefix=cfg["resource_ref"],
            action=cfg["action"],
            required_version_refs=[cfg["version_ref"]],
        )
    try:
        runtime.get_observation(cfg["version_ref"])
    except UnknownOperation:
        runtime.observe(
            actor=cfg["source_owner"],
            ref=cfg["version_ref"],
            source_revision="v1",
            evidence_ref="phasec-authority:" + cfg["version_ref"] + ":v1",
        )


def _read_authoritative_version(engine, ref: str) -> str | None:
    with engine.connect() as cx:
        return cx.execute(select(source_versions.c.revision).where(
            source_versions.c.ref == ref,
        )).scalar_one_or_none()


def run(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--operation-id", action="append", required=True)
    parser.add_argument("--actor", action="append", required=True)
    parser.add_argument("--concurrent", action="store_true")
    parser.add_argument("--pre-submit-barrier", action="store_true")
    args = parser.parse_args(argv)
    if len(args.operation_id) != len(args.actor) or len(args.operation_id) not in {1, 2}:
        parser.error("supply one or two matching --operation-id/--actor pairs")
    if len(args.operation_id) == 2 and not args.concurrent:
        parser.error("two actors require --concurrent")

    global _RUNTIME, _BARRIER, _ADAPTER, _CONFIG
    global _ACTIONS
    db_url = os.environ["PHASEC_DATABASE_URL"]
    trial_id = os.environ["PHASEC_TRIAL_ID"]
    arm = os.environ["PHASEC_ARM"]
    if arm != "continuity":
        raise RuntimeError("continuity_worker must only run Arm A")
    _CONFIG = {
        "trial_id": trial_id,
        "pair_id": os.environ["PHASEC_PAIR_ID"],
        "arm": arm,
        "principal": "phasec-principal",
        "provider": "phasec-mock",
        "resource_ref": "counter:" + os.environ["PHASEC_PAIR_ID"],
        "action": "increment",
        "grant_id": "grant:" + os.environ["PHASEC_PAIR_ID"],
        "effect_key": "logical-effect:" + os.environ["PHASEC_PAIR_ID"],
        "version_ref": "source:" + os.environ["PHASEC_PAIR_ID"],
        "source_owner": "phasec-source-owner",
        "participants": ["phasec-actor-a", "phasec-actor-b"],
    }
    engine = create_engine(db_url, pool_pre_ping=True)
    _ADAPTER = PhaseCProviderAdapter(
        base_url=os.environ["PHASEC_PROVIDER_URL"],
        trial_id=trial_id,
        arm=arm,
        barrier=PhaseCBarrier(
            trial_id=trial_id,
            event_dir=os.environ["PHASEC_BARRIER_EVENT_DIR"],
            release_dir=os.environ["PHASEC_BARRIER_RELEASE_DIR"],
        ),
    )
    verifier = PhaseCGoalVerifier()
    db = RuntimeDB(db_url, create_schema=True)
    _RUNTIME = ContinuityRuntime(
        db,
        adapters={"phasec-mock": _ADAPTER},
        verifiers={"phasec-mock": verifier},
        version_authorities={
            _CONFIG["version_ref"]: lambda: _read_authoritative_version(
                engine, _CONFIG["version_ref"],
            ),
        },
    )
    _BARRIER = PhaseCBarrier(
        trial_id=trial_id,
        event_dir=os.environ["PHASEC_BARRIER_EVENT_DIR"],
        release_dir=os.environ["PHASEC_BARRIER_RELEASE_DIR"],
    )
    _ACTIONS = {"reconcile": 0, "verify": 0}
    _ensure_setup(_RUNTIME, engine, _CONFIG)

    DBOS(config=DBOSConfig(
        name="phase-c-continuity",
        application_version=os.environ["PHASEC_APPLICATION_VERSION"],
        system_database_url=os.environ["DBOS_SYSTEM_DATABASE_URL"],
    ))
    DBOS.launch()
    started = time.perf_counter()
    try:
        from concurrent.futures import ThreadPoolExecutor

        def execute_one(index: int) -> dict:
            op_id = args.operation_id[index]
            actor = args.actor[index]
            with SetWorkflowID("phasec-continuity:" + op_id):
                return _effect_workflow(
                    op_id, actor, pre_submit_barrier=args.pre_submit_barrier,
                )

        if len(args.operation_id) == 1:
            rows = [execute_one(0)]
        else:
            with ThreadPoolExecutor(max_workers=2) as pool:
                futures = [pool.submit(execute_one, i) for i in range(2)]
                rows = [future.result() for future in futures]
    finally:
        DBOS.destroy()
        db.close()
        engine.dispose()

    finished = time.perf_counter()
    statuses = [row.get("reported_status", "unknown") for row in rows]
    if "satisfied" in statuses:
        reported = "satisfied"
    elif statuses and all(value == "blocked" for value in statuses):
        reported = "blocked"
    elif "unverified" in statuses:
        reported = "unverified"
    else:
        reported = "unknown"
    outcome_refs = [
        row.get("operation", {}).get("outcome_evidence_ref")
        for row in rows
        if row.get("operation", {}).get("outcome_evidence_ref")
    ]
    payload = {
        "arm": arm,
        "trial_id": trial_id,
        "reported_status": reported,
        "outcome_evidence": bool(outcome_refs),
        "outcome_evidence_refs": outcome_refs,
        "operations": rows,
        "attempted_provider_posts": _ADAPTER.attempted_posts if _ADAPTER else 0,
        "attempted_provider_gets": _ADAPTER.attempted_gets if _ADAPTER else 0,
        "recovery_actions": _ACTIONS["reconcile"] + _ACTIONS["verify"],
        "manual_interventions": 0,
        "operator_seconds": 0.0,
        "model_cost_usd": 0.0,
        "worker_elapsed_ms": round((finished - started) * 1000, 3),
        "worker_completed_at": datetime.now(timezone.utc).isoformat(),
    }
    print("PHASEC_RESULT=" + json.dumps(payload, sort_keys=True, separators=(",", ":")), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
