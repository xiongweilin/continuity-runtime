"""DBOS worker for Arm B: independent PostgreSQL + DBOS + scripts baseline.

This module intentionally imports no code from ``continuity``. Its state
machine/tables are implemented in ``baseline_runtime.py`` and share only the
provider client and barrier instrumentation with Arm A.
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

from .baseline_runtime import BaselineRuntime
from .shared import PhaseCBarrier, PhaseCGoalVerifier, PhaseCProviderAdapter

_RUNTIME: BaselineRuntime | None = None
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


def _runtime() -> BaselineRuntime:
    if _RUNTIME is None:
        raise RuntimeError("baseline runtime is not initialized")
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
    intent = {
        "operation_id": operation_id,
        "principal": cfg["principal"],
        "provider": cfg["provider"],
        "resource_ref": cfg["resource_ref"],
        "action": cfg["action"],
        "payload": {"delta": 1},
        "authority_id": cfg["grant_id"],
        "expected_versions": {cfg["version_ref"]: "v1"},
        "effect_key": cfg["effect_key"],
    }
    try:
        try:
            row = runtime.get_operation(operation_id, actor=actor)
        except Exception as exc:
            if type(exc).__name__ not in {
                "UnknownOperation", "UnknownGrant", "BaselineUnknownOperation", "KeyError",
            }:
                raise
            if pre_submit_barrier:
                _barrier().announce("submit_ready", operation_id)
                _barrier().announce_and_wait("submit_go", cfg["pair_id"])
                _barrier().announce("submit_started", operation_id)
                # Symmetric test-only hold after the synchronized release.
                time.sleep(0.1)
            try:
                row = runtime.submit(intent, actor=actor, participants=cfg["participants"])
            finally:
                if pre_submit_barrier:
                    _barrier().announce("submit_finished", operation_id)
    except Exception as exc:
        if type(exc).__name__ in {"BaselineConflict", "Conflict", "IntegrityError"}:
            return {"kind": "blocked", "reason": "operation_or_effect_conflict", "detail": type(exc).__name__}
        if type(exc).__name__ in {"BaselineDenied", "Denied", "PermissionError"}:
            return {"kind": "blocked", "reason": "authority_denied", "detail": type(exc).__name__}
        raise
    _barrier().announce_and_wait("registered", operation_id)
    return {"kind": "registered", "operation_id": operation_id, "status": row.get("status")}


@DBOS.step()
def _dispatch_operation(operation_id: str, actor: str) -> dict:
    runtime = _runtime()
    try:
        row = runtime.dispatch(
            operation_id,
            actor=actor,
            after_reservation=lambda reserved_id: _barrier().announce_and_wait(
                "reserved_before_send", reserved_id,
            ),
        )
        public = _public_row(row)
        if public.get("status") == "blocked":
            return {
                "kind": "blocked",
                "reason": "dispatch_precondition",
                "detail": row.get("reason"),
                "row": public,
            }
        return {"kind": "dispatched", "row": public}
    except Exception as exc:
        try:
            row = runtime.get_operation(operation_id, actor=actor)
        except Exception:
            row = {}
        if row.get("dispatch_generation", 0) == 0:
            return {
                "kind": "blocked",
                "reason": "dispatch_precondition",
                "detail": type(exc).__name__,
                "row": _public_row(row),
            }
        raise


@DBOS.step()
def _reconcile_operation(operation_id: str, actor: str) -> dict:
    _ACTIONS["reconcile"] += 1
    return _public_row(_runtime().reconcile(operation_id, actor=actor))


@DBOS.step()
def _verify_operation(operation_id: str, actor: str) -> dict:
    _ACTIONS["verify"] += 1
    try:
        return _public_row(_runtime().verify_outcome(operation_id, actor=actor))
    except Exception as exc:
        if type(exc).__name__ in {"BaselineHeld", "Held", "UnverifiedOutcome"}:
            return {"status": "unverified", "reason": type(exc).__name__}
        raise


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
        if not row.get("outcome_disposition") and row.get("status") == "unverified":
            return {
                "operation_id": operation_id,
                "reported_status": "unverified",
                "registration": registration,
                "dispatch": dispatched,
                "operation": row,
            }
    return {
        "operation_id": operation_id,
        "reported_status": _reported_status(row),
        "registration": registration,
        "dispatch": dispatched,
        "operation": row,
    }


def _reported_status(row: dict) -> str:
    if row.get("status") == "blocked":
        return "blocked"
    if row.get("outcome_disposition") == "satisfied" and row.get("outcome_evidence_ref"):
        return "satisfied"
    if row.get("outcome_disposition") == "not_satisfied":
        return "unverified"
    if row.get("receipt_ref") and row.get("outcome_disposition") == "unverified":
        return "unverified"
    return "unknown"


def _public_row(row: dict | None) -> dict:
    if not row:
        return {}
    aliases = {
        "id": ("id", "operation_id"),
        "status": ("status", "operation_status"),
        "dispatch_generation": ("dispatch_generation", "generation"),
        "receipt_ref": ("receipt_ref",),
        "effect_disposition": ("effect_disposition",),
        "effect_evidence_ref": ("effect_evidence_ref", "evidence_ref"),
        "outcome_disposition": ("outcome_disposition",),
        "outcome_evidence_ref": ("outcome_evidence_ref",),
    }
    result = {}
    for output, names in aliases.items():
        for name in names:
            if name in row:
                result[output] = row[name]
                break
    return result


def _ensure_setup(runtime: BaselineRuntime, engine, cfg: dict) -> None:
    source_metadata.create_all(engine)
    with engine.begin() as cx:
        row = cx.execute(select(source_versions.c.revision).where(
            source_versions.c.ref == cfg["version_ref"],
        )).scalar_one_or_none()
        if row is None:
            cx.execute(source_versions.insert().values(
                ref=cfg["version_ref"], revision="v1",
            ))
    grant_exists = True
    try:
        runtime.get_grant(cfg["grant_id"])
    except Exception as exc:
        if type(exc).__name__ not in {
            "UnknownGrant", "UnknownOperation", "BaselineUnknownOperation", "KeyError",
        }:
            raise
        grant_exists = False
    if not grant_exists:
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
        runtime.get_version(cfg["version_ref"])
    except Exception as exc:
        if type(exc).__name__ not in {
            "UnknownVersion", "BaselineHeld", "BaselineUnknownOperation", "KeyError",
        }:
            raise
        runtime.observe_version(
            cfg["version_ref"], "v1",
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

    global _RUNTIME, _BARRIER, _ADAPTER, _CONFIG, _ACTIONS
    db_url = os.environ["PHASEC_DATABASE_URL"]
    trial_id = os.environ["PHASEC_TRIAL_ID"]
    arm = os.environ["PHASEC_ARM"]
    if arm != "postgres_dbos":
        raise RuntimeError("baseline_worker must only run Arm B")
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
        "participants": ["phasec-actor-a", "phasec-actor-b"],
    }
    engine = create_engine(db_url, pool_pre_ping=True)
    _ADAPTER = PhaseCProviderAdapter(
        base_url=os.environ["PHASEC_PROVIDER_URL"],
        trial_id=trial_id,
        arm=arm,
        # BaselineRuntime's after_reservation hook below is the same external
        # stop point used by Continuity's trusted adapter hook; avoid a second
        # duplicate barrier inside the shared HTTP adapter.
        barrier=None,
    )
    verifier = PhaseCGoalVerifier()
    _RUNTIME = BaselineRuntime(
        db_url,
        adapters={"phasec-mock": _ADAPTER},
        verifiers={"phasec-mock": verifier},
        version_readers={
            _CONFIG["version_ref"]: lambda: _read_authoritative_version(
                engine, _CONFIG["version_ref"],
            ),
        },
        create_schema=True,
    )
    _BARRIER = PhaseCBarrier(
        trial_id=trial_id,
        event_dir=os.environ["PHASEC_BARRIER_EVENT_DIR"],
        release_dir=os.environ["PHASEC_BARRIER_RELEASE_DIR"],
    )
    _ACTIONS = {"reconcile": 0, "verify": 0}
    _ensure_setup(_RUNTIME, engine, _CONFIG)

    DBOS(config=DBOSConfig(
        name="phase-c-postgres-dbos",
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
            with SetWorkflowID("phasec-postgres-dbos:" + op_id):
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
        _RUNTIME.close()
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
