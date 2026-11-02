"""Isolated Phase D paired runner; all effects terminate at the local mock provider."""
from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import os
import platform
import secrets
import shutil
import signal
import subprocess
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
import psycopg
from psycopg import sql
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import URL, make_url

from continuity.db import RuntimeDB
from continuity.runtime import ContinuityRuntime
from experiments.phase_c.baseline_runtime import BaselineRuntime
from experiments.phase_c.provider_service import read_verified_ledger
from experiments.phase_c.runner import (
    FORBIDDEN_CREDENTIAL_ENV,
    ManagedProcess,
    TrialInvalid,
    _make_ledger,
    _sha256_file,
    _utcnow,
    _wait_event,
)
from experiments.phase_c.shared import PhaseCBarrier, safe_component
from experiments.phase_d.baseline_history import BaselineHistory
from experiments.phase_d.domain_owner import CASE_REF, EVIDENCE_REF, FACTS
from experiments.phase_d.fault_proxy import read_proxy_ledger
from experiments.phase_d.oracle_exporter import derive_oracle
from experiments.phase_d.provider_service import ACTIONS, MOCK_PROVIDER
from experiments.phase_d.shared import SOURCE_REF, PhaseDVersionPinPolicy, TrustedSourceReader

ROOT = Path(__file__).resolve().parents[2]
UID_PROVIDER, GID_PROVIDER = 10001, 10001
UID_PROXY, GID_PROXY = 10002, 10002
UID_AGENT_A, GID_AGENT = 10003, 10003
UID_SOURCE, GID_SOURCE = 10004, 10004
UID_DOMAIN, GID_DOMAIN = 10005, 10005
UID_AGENT_B, GID_AGENT_B = 10006, 10006
UID_OUTSIDER, GID_OUTSIDER = 10007, 10007
UID_READER, GID_READER = 10008, 10008
ARMS = ("continuity", "postgres_dbos")
SCENARIO_IDS = (
    "normal_supersession",
    "pack_ack_lost_then_supersession",
    "unauthorized_actor_during_handoff",
    "new_action_crash_before_send",
)
FORBIDDEN_ENV_NAMES = frozenset(set(FORBIDDEN_CREDENTIAL_ENV) | {
    "GITHUB_TOKEN", "GH_TOKEN", "GH_ENTERPRISE_TOKEN", "GITHUB_PAT",
    "OPENAI_API_KEY", "ANTHROPIC_API_KEY", "PHASED_LIVE_PROVIDER_KEY",
    "GITHUB_PERSONAL_ACCESS_TOKEN", "GITHUB_API_TOKEN", "GH_API_TOKEN",
})
DESIGN_COMMIT = "26e53f279bf5049b69c7508e6abc5921cf132152"
FROZEN_HASHES = {
    "docs/phase-d-protocol.md": "6fd40abab74a7a4e8de01d08701ae0491bf50fe391e87bbf15729bac34a04bd1",
    "docs/phase-d-codex-handoff.md": "4d98ad5998d1b75d01cc07f693fd4f22d08c5cf59ff44df8f2f307ae08bac93c",
    "experiments/phase_d_manifest.json": "a301a6458b086a969d4c6cb935a655afeaf61a0c3b228a7d9a3128650f4c1e19",
    "scripts/evaluate_phase_d.py": "efd5a104088960bc82e12d4c78f690b2d352a4b4c0788a0bb3e72c3b85afe86d",
    "tests/test_phase_d_evaluator.py": "0944ce04ab9d4cd02f76f9cec93d6d22efe6e73981b211924fd35fa942e6bfc1",
}


def _sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return value.as_posix()
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, bytes):
        return {"sha256": _sha256_bytes(value), "bytes": len(value)}
    return value


def _json_bytes(value: Any) -> bytes:
    return (json.dumps(_jsonable(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                        allow_nan=False) + "\n").encode("utf-8")


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as handle:
        handle.write(_json_bytes(value))
        handle.flush()
        os.fsync(handle.fileno())
    os.chmod(path, 0o600)


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as handle:
        for row in rows:
            handle.write(_json_bytes(row))
        handle.flush()
        os.fsync(handle.fileno())
    os.chmod(path, 0o600)


def _check_environment(mode: str) -> None:
    if os.name != "posix" or platform.system().lower() != "linux":
        raise RuntimeError("refusing Phase D run outside the isolated Linux Docker runner")
    if os.getuid() != 0:
        raise RuntimeError("Phase D controller must start as container root to set per-role UIDs")
    forbidden = sorted(name for name in FORBIDDEN_ENV_NAMES if os.environ.get(name))
    if forbidden:
        # Print names only. Never reveal values or forward them to any child.
        raise RuntimeError("refusing Phase D run: forbidden credential environment names are present")
    if os.environ.get("PHASED_NETWORK_POLICY") != "isolated_internal_no_egress":
        raise RuntimeError("refusing Phase D run without the internal/no-egress Docker network")
    if not os.environ.get("PHASED_ADMIN_DATABASE_URL"):
        raise RuntimeError("test-only PostgreSQL admin URL is required")
    if not os.environ.get("PHASED_OUTPUT_DIR"):
        raise RuntimeError("PHASED_OUTPUT_DIR is required")
    if not os.environ.get("PHASED_CODE_SHA"):
        raise RuntimeError("implementation SHA must be supplied by the host wrapper")
    clean = os.environ.get("PHASED_WORKTREE_CLEAN", "false").lower() == "true"
    if mode == "full" and not clean:
        raise RuntimeError("full Phase D run requires a frozen clean implementation worktree")


def _load_frozen_manifest() -> tuple[dict[str, Any], dict[str, str]]:
    hashes: dict[str, str] = {}
    for relative, expected in FROZEN_HASHES.items():
        path = ROOT / relative
        actual = _sha256_file(path)
        hashes[relative] = actual
        if actual != expected:
            raise RuntimeError(f"frozen Phase D input changed: {relative}")
    manifest = json.loads((ROOT / "experiments/phase_d_manifest.json").read_text(encoding="utf-8"))
    if manifest.get("schema_version") != 1 or manifest.get("seeds") != [0, 1, 2]:
        raise RuntimeError("Phase D frozen manifest schema/seeds changed")
    if manifest.get("arms") != list(ARMS):
        raise RuntimeError("Phase D frozen arm order changed")
    if [case.get("id") for case in manifest.get("scenarios", [])] != list(SCENARIO_IDS):
        raise RuntimeError("Phase D frozen scenario order changed")
    return manifest, hashes


def _admin_conn() -> psycopg.Connection:
    configured = make_url(os.environ["PHASED_ADMIN_DATABASE_URL"])
    if configured.get_backend_name() != "postgresql":
        raise RuntimeError("Phase D admin URL must target PostgreSQL")
    conninfo = configured.set(drivername="postgresql").render_as_string(hide_password=False)
    return psycopg.connect(conninfo, autocommit=True)


def _create_trial_database(trial_id: str) -> tuple[str, str, str, str]:
    sha = os.environ["PHASED_CODE_SHA"]
    slug = hashlib.sha256((sha + trial_id).encode("utf-8")).hexdigest()[:12]
    db_name = "pd_" + slug
    role = "pdu_" + slug
    password = secrets.token_urlsafe(24)
    with _admin_conn() as cx:
        cx.execute(sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
            sql.Identifier(role), sql.Literal(password),
        ))
        cx.execute(sql.SQL("CREATE DATABASE {} OWNER {}").format(
            sql.Identifier(db_name), sql.Identifier(role),
        ))
    admin = make_url(os.environ["PHASED_ADMIN_DATABASE_URL"])
    db_url = URL.create(
        "postgresql+psycopg",
        username=role,
        password=password,
        host=admin.host,
        port=admin.port or 5432,
        database=db_name,
    ).render_as_string(hide_password=False)
    return db_name, role, password, db_url


def _drop_trial_database(db_name: str, role: str) -> None:
    with _admin_conn() as cx:
        cx.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = %s AND pid <> pg_backend_pid()",
            (db_name,),
        )
        cx.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(db_name)))
        cx.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(role)))


def _pg_stats(db_name: str) -> dict[str, int]:
    with _admin_conn() as cx:
        row = cx.execute(
            "SELECT xact_commit, xact_rollback FROM pg_stat_database WHERE datname = %s",
            (db_name,),
        ).fetchone()
    if not row:
        return {"commit": 0, "rollback": 0, "total": 0}
    commit, rollback = int(row[0]), int(row[1])
    return {"commit": commit, "rollback": rollback, "total": commit + rollback}


def _pg_version() -> str:
    with _admin_conn() as cx:
        row = cx.execute("SELECT version()").fetchone()
    return str(row[0]) if row else "unknown"


def _service_env(uid: int, gid: int, run_id: str, role: str,
                 extra: dict[str, str]) -> dict[str, str]:
    home = Path("/tmp/phase-d-home") / safe_component(run_id) / safe_component(role)
    home.mkdir(parents=True, exist_ok=True)
    os.chown(home, uid, gid)
    os.chmod(home, 0o700)
    env = {
        "PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
        "PYTHONPATH": str(ROOT),
        "PYTHONUNBUFFERED": "1",
        "PYTHONDONTWRITEBYTECODE": "1",
        "LANG": "C.UTF-8",
        "HOME": str(home),
    }
    env.update(extra)
    for name in FORBIDDEN_ENV_NAMES:
        env.pop(name, None)
    return env


def _private_barrier_dirs(event_dir: Path, release_dir: Path, uid: int, gid: int) -> None:
    for path in (event_dir, release_dir):
        path.mkdir(parents=True, exist_ok=True)
        os.chown(path, uid, gid)
        os.chmod(path, 0o700)


def _release(barrier: PhaseCBarrier, stage: str, operation_id: str) -> dict[str, Any]:
    path = barrier.release_path(stage, operation_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "trial_id": barrier.trial_id,
        "stage": stage,
        "operation_id": operation_id,
        "released_by": "phase-d-controller",
        "released_monotonic_ns": time.monotonic_ns(),
        "released_at_utc": _utcnow(),
    }
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o444)
    try:
        os.write(fd, _json_bytes(payload))
        os.fsync(fd)
    finally:
        os.close(fd)
    os.chmod(path, 0o444)
    return payload


def _http_json(method: str, url: str, *, body: dict[str, Any] | None = None,
               headers: dict[str, str] | None = None, timeout: float = 2.0) -> tuple[int, dict[str, Any]]:
    with httpx.Client(trust_env=False, timeout=timeout) as client:
        response = client.request(method, url, json=body, headers=headers or {})
        try:
            value = response.json()
        except ValueError as exc:
            raise TrialInvalid("Phase D service returned non-JSON response") from exc
    if not isinstance(value, dict):
        raise TrialInvalid("Phase D service returned a non-object response")
    return response.status_code, value


def _source_publish(source_url: str, source_key: str, revision: str) -> dict[str, Any]:
    facts = {"v1": FACTS["v1"], "v2": FACTS["v2"]}
    status, result = _http_json(
        "POST", source_url.rstrip("/") + "/v1/source/publish",
        body={"revision": revision, "fact": facts[revision], "evidence_ref": EVIDENCE_REF},
        headers={"X-PhaseD-Source-Key": source_key},
    )
    if status != 201 or result.get("revision") != revision:
        raise TrialInvalid(f"trusted source rejected frozen {revision} publication")
    return result


def _owner_command(owner_url: str, owner_key: str, path: str,
                   body: dict[str, Any] | None = None) -> dict[str, Any]:
    status, result = _http_json(
        "POST", owner_url.rstrip("/") + path,
        body=body or {},
        headers={"X-PhaseD-Owner-Key": owner_key},
    )
    if not 200 <= status < 300:
        raise TrialInvalid(f"domain owner rejected required command {path} with HTTP {status}")
    return result


def _owner_projection(owner_url: str) -> dict[str, Any]:
    status, result = _http_json(
        "GET", owner_url.rstrip("/") + "/v1/owner/projection?actor=agent-b",
        headers={"Accept": "application/json"},
    )
    if status != 200:
        raise TrialInvalid(f"domain owner refused authorized Agent B projection (HTTP {status})")
    return result


def _provider_counts(provider_url: str, oracle_key: str, trial_id: str, arm: str) -> tuple[dict[str, int], str]:
    status, result = _http_json(
        "GET", provider_url.rstrip("/") + "/v1/effects?resource_ref=" + CASE_REF,
        headers={
            "X-PhaseD-Provider-Oracle-Key": oracle_key,
            "X-PhaseD-Trial-ID": trial_id,
            "X-PhaseD-Arm": arm,
        },
    )
    counts = result.get("action_counts")
    ref = result.get("evidence_ref") or result.get("ledger_ref")
    if status != 200 or not isinstance(counts, dict) or set(counts) != set(ACTIONS):
        raise TrialInvalid("independent provider readback response is invalid")
    if any(type(counts[action]) is not int or counts[action] < 0 for action in ACTIONS):
        raise TrialInvalid("independent provider readback counts are invalid")
    if not isinstance(ref, str) or not ref:
        raise TrialInvalid("independent provider readback omitted its ledger ref")
    return {action: counts[action] for action in ACTIONS}, ref


def _runtime_handles(arm: str, db_url: str, source_url: str,
                     trial_id: str) -> tuple[Any, Any]:
    reader = TrustedSourceReader(source_url, SOURCE_REF, trial_id, arm)
    if arm == "continuity":
        db = RuntimeDB(db_url, create_schema=True)
        runtime = ContinuityRuntime(
            db,
            version_authorities={SOURCE_REF: reader},
            admission_profiles={
                "phase-d-v1": PhaseDVersionPinPolicy("phase-d-v1", "v1"),
                "phase-d-v2": PhaseDVersionPinPolicy("phase-d-v2", "v2"),
            },
        )
        return runtime, db
    runtime = BaselineRuntime(
        db_url,
        adapters={},
        verifiers={},
        version_readers={SOURCE_REF: reader},
        create_schema=True,
    )
    return runtime, runtime


def _issue_grant(runtime: Any, arm: str, grant_id: str, action: str,
                 actor: str) -> dict[str, Any]:
    if arm == "continuity":
        pinned_revision = "v1" if actor == "agent-a" else "v2"
        return runtime.issue_grant(
            grant_id=grant_id,
            principal="shipment-owner",
            actors=[actor],
            provider=MOCK_PROVIDER,
            resource_prefix=CASE_REF,
            action=action,
            required_version_refs=[SOURCE_REF],
            admission_profile=f"phase-d-{pinned_revision}",
        )
    return runtime.issue_grant(
        grant_id=grant_id,
        principal="shipment-owner",
        actors=[actor],
        provider=MOCK_PROVIDER,
        resource_prefix=CASE_REF,
        action=action,
        required_version_refs=[SOURCE_REF],
    )


def _pin_baseline_grant(
    *,
    db_url: str,
    source_url: str,
    trial_id: str,
    arm: str,
    grant_id: str,
    action: str,
    revision: str,
    source_event_ref: str,
    domain_authority_ref: str,
) -> dict[str, Any]:
    reader = TrustedSourceReader(source_url, SOURCE_REF, trial_id, arm)
    history = BaselineHistory(db_url, version_reader=reader, create_schema=True)
    try:
        return history.pin_grant(
            grant_id=grant_id,
            action=action,
            source_ref=SOURCE_REF,
            pinned_revision=revision,
            source_evidence_ref=source_event_ref,
            domain_authority_ref=domain_authority_ref,
        )
    finally:
        history.engine.dispose()


def _grant_snapshot(runtime: Any, arm: str, grant_id: str) -> dict[str, Any]:
    return runtime.get_grant(grant_id)


def _operation_snapshot(runtime: Any, arm: str, operation_id: str, actor: str) -> dict[str, Any]:
    return runtime.get_operation(operation_id, actor=actor)


def _grant_state(runtime: Any, grant_ids: dict[str, str], arm: str) -> dict[str, Any]:
    grants = {name: _grant_snapshot(runtime, arm, grant_id) for name, grant_id in grant_ids.items()}
    return grants


def _runtime_events(runtime: Any, arm: str) -> list[dict[str, Any]]:
    if arm == "continuity":
        return runtime.events(after=0, limit=1000)
    return []


def _start_service(
    *,
    run_id: str,
    trial_id: str,
    role: str,
    module: str,
    uid: int,
    gid: int,
    args: list[str],
    extra_env: dict[str, str],
    ready_role: str,
) -> tuple[ManagedProcess, dict[str, Any]]:
    env = _service_env(uid, gid, run_id, role, extra_env)
    process = ManagedProcess(
        [sys.executable, "-m", module, *args],
        env=env,
        uid=uid,
        gid=gid,
        cwd=ROOT,
        name=f"{role}-{trial_id}",
    )
    process.start()
    try:
        ready = process.wait_ready(role=ready_role, timeout_seconds=20.0)
    except Exception:
        try:
            process.interrupt(timeout_seconds=5.0)
        except Exception:
            try:
                process.kill(signal.SIGKILL)
            except Exception:
                pass
        raise
    return process, ready


def _parse_result(process: ManagedProcess, prefix: str) -> dict[str, Any]:
    for line in reversed(process.output()):
        if line.startswith(prefix):
            try:
                value = json.loads(line.split("=", 1)[1])
            except json.JSONDecodeError as exc:
                raise TrialInvalid(f"{process.name} emitted invalid result JSON") from exc
            if isinstance(value, dict):
                return value
    raise TrialInvalid(f"{process.name} did not emit {prefix} result")


def _phase_events(process: ManagedProcess) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for line in process.output():
        if not line.startswith("PHASED_EVENT="):
            continue
        try:
            event = json.loads(line.split("=", 1)[1])
        except json.JSONDecodeError as exc:
            raise TrialInvalid(f"{process.name} emitted an invalid event row") from exc
        if not isinstance(event, dict):
            raise TrialInvalid(f"{process.name} event row is not an object")
        events.append(event)
    return events


def _create_ledger(path: Path, uid: int, gid: int) -> None:
    _make_ledger(path, uid, gid)


def _copy_ledger(source: Path, dest: Path) -> dict[str, Any]:
    rows, head, raw = read_verified_ledger(source)
    dest.parent.mkdir(parents=True, exist_ok=True)
    with dest.open("wb") as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())
    os.chmod(dest, 0o600)
    copied_rows, copied_head, copied_raw = read_verified_ledger(dest)
    if head != copied_head or raw != copied_raw or len(rows) != len(copied_rows):
        raise TrialInvalid(f"copied ledger verification failed: {dest.name}")
    return {
        "relative_path": dest.name,
        "sha256": _sha256_bytes(raw),
        "bytes": len(raw),
        "rows": len(rows),
        "head_sha256": head,
    }


def _copy_proxy_ledger(source: Path, dest: Path) -> dict[str, Any]:
    rows, head, raw = read_proxy_ledger(str(source))
    dest.parent.mkdir(parents=True, exist_ok=True)
    with dest.open("wb") as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())
    os.chmod(dest, 0o600)
    copied_rows, copied_head, copied_raw = read_proxy_ledger(str(dest))
    if head != copied_head or raw != copied_raw or len(rows) != len(copied_rows):
        raise TrialInvalid("copied fault-proxy ledger verification failed")
    return {
        "relative_path": dest.name,
        "sha256": _sha256_bytes(raw),
        "bytes": len(raw),
        "rows": len(rows),
        "head_sha256": head,
    }


def _write_event_rows(path: Path, events: list[dict[str, Any]]) -> dict[str, Any]:
    _write_jsonl(path, events)
    raw = path.read_bytes()
    return {"relative_path": path.name, "sha256": _sha256_bytes(raw),
            "bytes": len(raw), "rows": len(events)}


def _write_process_log(path: Path, process: ManagedProcess,
                       secret_values: list[str], db_url: str | None) -> dict[str, Any]:
    lines = process.output()
    safe_lines = []
    for original in lines:
        line = original
        for secret in secret_values:
            if secret:
                line = line.replace(secret, "[test secret redacted]")
        if db_url:
            line = line.replace(db_url, "[trial database URL redacted]")
            try:
                hidden = make_url(db_url).render_as_string(hide_password=True)
                line = line.replace(hidden, "[trial database URL redacted]")
            except Exception:
                pass
        safe_lines.append(line)
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = ("\n".join(safe_lines) + ("\n" if safe_lines else "")).encode("utf-8")
    with path.open("wb") as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())
    os.chmod(path, 0o600)
    return {"relative_path": path.name, "sha256": _sha256_bytes(raw),
            "bytes": len(raw), "lines": len(safe_lines)}


def _event_locator(label: str, event: dict[str, Any], line_index: int) -> str:
    return f"{label}:line={line_index}:sha256={_sha256_bytes(_json_bytes(event))}"


def _dump_database_snapshot(db_url: str, arm: str) -> dict[str, Any]:
    """Copy relevant Runtime/DBOS tables without credentials or secret columns."""
    allow_prefixes = (
        "effect_", "source_", "boundary_", "phasec_baseline_",
        "phase_d_baseline_", "dbos_", "workflow_",
    )
    forbidden_column_parts = ("password", "secret", "token", "credential", "database_url")
    engine = create_engine(db_url, pool_pre_ping=True)
    try:
        inspector = inspect(engine)
        table_names = sorted(name for name in inspector.get_table_names()
                             if name.startswith(allow_prefixes))
        snapshot: dict[str, Any] = {"arm": arm, "tables": {}}
        with engine.connect() as cx:
            for name in table_names:
                columns = [column["name"] for column in inspector.get_columns(name)
                           if not any(part in column["name"].lower()
                                      for part in forbidden_column_parts)]
                if not columns:
                    continue
                quoted = ", ".join('"' + column.replace('"', '""') + '"' for column in columns)
                table = '"' + name.replace('"', '""') + '"'
                rows = cx.execute(text(f"SELECT {quoted} FROM {table}")).mappings().all()
                serialized = []
                for row in rows:
                    item = {}
                    for key, value in row.items():
                        if isinstance(value, datetime):
                            item[key] = value.isoformat()
                        elif isinstance(value, bytes):
                            item[key] = {"sha256": _sha256_bytes(value), "bytes": len(value)}
                        else:
                            item[key] = value
                    serialized.append(item)
                snapshot["tables"][name] = serialized
        return snapshot
    finally:
        engine.dispose()


def _safe_error(exc: BaseException) -> str:
    value = f"{type(exc).__name__}: {exc}"
    for name in ("PHASED_ADMIN_DATABASE_URL", "PHASEC_ADMIN_DATABASE_URL"):
        secret = os.environ.get(name)
        if secret:
            value = value.replace(secret, "[admin database URL redacted]")
            try:
                value = value.replace(make_url(secret).render_as_string(hide_password=False),
                                      "[admin database URL redacted]")
            except Exception:
                pass
    return value[:1000]


def _domain_owner_write_ref(owner_result: dict[str, Any], field: str = "goal_event_ref") -> str:
    ref = owner_result.get(field)
    if not isinstance(ref, str) or not ref.strip():
        raise TrialInvalid("domain owner omitted a required append-only event reference")
    return ref


def _reconstruction_audit(trial_output: Path) -> list[str]:
    """Perform the predeclared five-source operator reconstruction checklist."""
    source_rows, _, _ = read_verified_ledger(trial_output / "source_ledger.jsonl")
    if not source_rows:
        raise TrialInvalid("reconstruction audit found no trusted-source history")
    steps = ["trusted-source revision history"]
    goal_rows, _, _ = read_verified_ledger(trial_output / "goal_ledger.jsonl")
    authority_rows, _, _ = read_verified_ledger(trial_output / "authority_ledger.jsonl")
    if not goal_rows or not authority_rows:
        raise TrialInvalid("reconstruction audit found incomplete domain-owner history")
    steps.append("domain goal and authority history")
    provider_rows, _, _ = read_verified_ledger(trial_output / "provider_ledger.jsonl")
    if not provider_rows:
        raise TrialInvalid("reconstruction audit found no provider ledger")
    steps.append("mock-provider effects and readbacks")
    proxy_rows, _, _ = read_proxy_ledger(str(trial_output / "fault_proxy_ledger.jsonl"))
    barrier_files = list((trial_output / "barriers").rglob("*.json"))
    for path in barrier_files:
        json.loads(path.read_text(encoding="utf-8"))
    if not proxy_rows:
        raise TrialInvalid("reconstruction audit found no fault-proxy history")
    steps.append("fault-proxy and reservation-barrier evidence")
    snapshot = json.loads((trial_output / "database_snapshot.json").read_text(encoding="utf-8"))
    if not isinstance(snapshot.get("tables"), dict):
        raise TrialInvalid("reconstruction audit found no Runtime/DBOS database snapshot")
    steps.append("Runtime/DBOS operation, grant and event state")
    return steps


def _run_outsider_probe(
    *,
    run_id: str,
    trial_id: str,
    arm: str,
    db_url: str,
    source_url: str,
    owner_url: str,
    grant_ids: dict[str, str],
    pack_operation_id: str,
) -> tuple[ManagedProcess, dict[str, Any]]:
    module = (
        "experiments.phase_d.continuity_outsider_probe"
        if arm == "continuity"
        else "experiments.phase_d.baseline_outsider_probe"
    )
    env = _service_env(UID_OUTSIDER, GID_OUTSIDER, run_id, f"outsider-{trial_id}", {
        "PHASED_DATABASE_URL": db_url,
        "PHASED_TRIAL_ID": trial_id,
        "PHASED_ARM": arm,
        "PHASED_SOURCE_URL": source_url,
        "PHASED_OWNER_URL": owner_url,
        "PHASED_AGENT_GRANTS_JSON": json.dumps(grant_ids, sort_keys=True),
        "PHASED_A_PACK_OPERATION_ID": pack_operation_id,
    })
    process = ManagedProcess(
        [sys.executable, "-m", module],
        env=env,
        uid=UID_OUTSIDER,
        gid=GID_OUTSIDER,
        cwd=ROOT,
        name=f"outsider-{arm}-{trial_id}",
    )
    process.start()
    try:
        code = process.wait(30.0)
    except Exception:
        try:
            process.kill(signal.SIGKILL)
        except Exception:
            pass
        raise
    if code != 0:
        raise TrialInvalid(f"unauthorized actor probe exited with {code}")
    result = _parse_result(process, "PHASED_OUTSIDER=")
    if result.get("denied") is not True:
        raise TrialInvalid("unauthorized actor was not denied from old state and new authority")
    return process, result


def _root_reconcile_only(
    runtime: Any,
    arm: str,
    operation_id: str,
    trial_id: str,
    proxy_url: str,
    proxy_client_token: str,
) -> dict[str, Any]:
    from experiments.phase_d.shared import PhaseDProviderAdapter

    adapter = PhaseDProviderAdapter(
        base_url=proxy_url,
        trial_id=trial_id,
        arm=arm,
        actor="agent-b",
        proxy_client_token=proxy_client_token,
        source_ref=SOURCE_REF,
        barrier=None,
    )
    runtime.adapters[MOCK_PROVIDER] = adapter
    if arm == "continuity":
        row = runtime.reconcile(operation_id, actor="agent-b")
    else:
        row = runtime.reconcile(operation_id, "agent-b")
    return {
        "mode": "reconcile_only_no_dispatch",
        "provider_posts_attempted": adapter.attempted_posts,
        "provider_readbacks_attempted": adapter.attempted_gets,
        "operation": row,
    }


def _copy_barriers(event_dir: Path, release_dir: Path, dest_dir: Path) -> dict[str, Any]:
    copied: list[dict[str, Any]] = []
    for label, source_dir in (("events", event_dir), ("releases", release_dir)):
        target_dir = dest_dir / label
        target_dir.mkdir(parents=True, exist_ok=True)
        if not source_dir.exists():
            continue
        for source in sorted(source_dir.iterdir()):
            if not source.is_file():
                continue
            target = target_dir / source.name
            shutil.copyfile(source, target)
            os.chmod(target, 0o600)
            raw = target.read_bytes()
            copied.append({
                "relative_path": f"{label}/{target.name}",
                "sha256": _sha256_bytes(raw),
                "bytes": len(raw),
            })
    return {"files": copied, "count": len(copied)}


def _extract_invalidation(arm: str, runtime: Any, db_url: str,
                          trial_id: str, source_url: str,
                          source_ref: str, event_seq: int,
                          relative_base: str) -> tuple[str, list[dict[str, Any]]]:
    if arm == "continuity":
        events = runtime.events(after=0, limit=1000)
        candidates = [event for event in events
                      if event.get("kind") == "dependency.invalidated"
                      and event.get("payload", {}).get("source_ref") == source_ref
                      and event.get("seq") == event_seq]
        if len(candidates) != 1:
            raise TrialInvalid("Continuity runtime invalidation event is not uniquely reviewable")
        event = candidates[0]
        event_line_index = events.index(event) + 1
    else:
        reader = TrustedSourceReader(
            source_url, SOURCE_REF, trial_id, arm,
        )
        history = BaselineHistory(db_url, version_reader=reader, create_schema=False)
        try:
            events = history.events()
        finally:
            history.engine.dispose()
        candidates = [event for event in events
                      if event.get("kind") == "dependency.invalidated"
                      and int(event.get("seq", -1)) == int(event_seq)]
        if len(candidates) != 1:
            raise TrialInvalid("baseline link invalidation event is not uniquely reviewable")
        event = candidates[0]
        event_line_index = events.index(event) + 1
    event_file = "runtime_events.jsonl" if arm == "continuity" else "baseline_history_events.jsonl"
    locator = _event_locator(
        f"{relative_base}/{event_file}", event, event_line_index,
    )
    return locator, events


def run_trial(
    *,
    run_id: str,
    mode: str,
    scenario_spec: dict[str, Any],
    seed: int,
    arm: str,
    arm_order: list[str],
    output_root: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    scenario = scenario_spec["id"]
    if scenario not in SCENARIO_IDS or arm not in ARMS:
        raise TrialInvalid("trial key is outside the frozen Phase D design")
    token = uuid.uuid4().hex[:12]
    trial_id = "pd-" + token
    pair_id = "pair-" + hashlib.sha256(f"{scenario}:{seed}".encode()).hexdigest()[:16]
    relative_base = f"raw/{scenario}/seed-{seed}/{arm}"
    trial_output = output_root / relative_base
    trial_output.mkdir(parents=True, exist_ok=True)

    data_root = Path(os.environ["PHASED_PROVIDER_DATA_DIR"])
    proxy_root = Path(os.environ["PHASED_PROXY_LOG_DIR"])
    source_root = Path(os.environ["PHASED_SOURCE_DATA_DIR"])
    domain_root = Path(os.environ["PHASED_DOMAIN_DATA_DIR"])
    barrier_root = Path(os.environ["PHASED_BARRIER_ROOT"])
    provider_dir = data_root / run_id / trial_id
    proxy_dir = proxy_root / run_id / trial_id
    source_dir = source_root / run_id / trial_id
    domain_dir = domain_root / run_id / trial_id
    event_dir_a = barrier_root / run_id / trial_id / "agent-a" / "events"
    release_dir_a = barrier_root / run_id / trial_id / "agent-a" / "releases"
    event_dir_b = barrier_root / run_id / trial_id / "agent-b" / "events"
    release_dir_b = barrier_root / run_id / trial_id / "agent-b" / "releases"
    _private_barrier_dirs(event_dir_a, release_dir_a, UID_AGENT_A, GID_AGENT)
    _private_barrier_dirs(event_dir_b, release_dir_b, UID_AGENT_B, GID_AGENT_B)

    provider_ledger = provider_dir / "provider.jsonl"
    proxy_ledger = proxy_dir / "fault_proxy.jsonl"
    source_ledger = source_dir / "source.jsonl"
    goal_ledger = domain_dir / "goal.jsonl"
    authority_ledger = domain_dir / "authority.jsonl"
    _create_ledger(provider_ledger, UID_PROVIDER, GID_PROVIDER)
    _create_ledger(proxy_ledger, UID_PROXY, GID_PROXY)
    _create_ledger(source_ledger, UID_SOURCE, GID_SOURCE)
    _create_ledger(goal_ledger, UID_DOMAIN, GID_DOMAIN)
    _create_ledger(authority_ledger, UID_DOMAIN, GID_DOMAIN)

    secrets_by_role = {
        "provider_proxy": secrets.token_urlsafe(40),
        "provider_oracle": secrets.token_urlsafe(40),
        "source_control": secrets.token_urlsafe(40),
        "owner_control": secrets.token_urlsafe(40),
        "proxy_client": secrets.token_urlsafe(40),
    }
    if len(set(secrets_by_role.values())) != len(secrets_by_role):
        raise TrialInvalid("test-only role credentials were not unique")

    processes: dict[str, ManagedProcess] = {}
    ready: dict[str, dict[str, Any]] = {}
    db_name = role_name = database_url = db_password = None
    database_created = False
    root_runtime = root_aux = None
    cleanup: dict[str, Any] = {"services": {}, "database_dropped": False}
    error_value: str | None = None
    result_bundle: dict[str, Any] = {}
    completed = False
    start_ns = end_ns = 0
    db_stats_before: dict[str, int] = {"total": 0}
    db_stats_after: dict[str, int] = {"total": 0}
    expected_status = scenario_spec["expected_goal_status"]
    grant_ids = {
        "pack": f"grant-a-pack-{token}",
        "ship": f"grant-a-ship-{token}",
        "quarantine": f"grant-b-quarantine-{token}",
    }
    operation_ids = {
        "pack": f"op-a-pack-{token}",
        "ship": f"op-a-ship-{token}",
        "quarantine": f"op-b-quarantine-{token}",
    }
    source_url = owner_url = provider_url = proxy_url = None
    source_v1 = source_v2 = None
    projection: dict[str, Any] | None = None
    projection_path: Path | None = None
    a_process: ManagedProcess | None = None
    b_process: ManagedProcess | None = None
    outsider_process: ManagedProcess | None = None
    a_payload: dict[str, Any] | None = None
    b_payload: dict[str, Any] | None = None
    outsider_result: dict[str, Any] | None = None
    crash_proof: dict[str, Any] | None = None
    a_events: list[dict[str, Any]] = []
    b_events: list[dict[str, Any]] = []
    stale_ref = invalidation_ref = handoff_ref = pack_readback_ref = ""
    new_goal_verifier_ref: str | None = None
    fault_ref: str | None = None
    reported_goal_status: str | None = None
    final_provider_counts: dict[str, int] | None = None
    invalidation_events: list[dict[str, Any]] = []
    projection_digest: str | None = None
    operation_snapshots: dict[str, Any] = {}
    grant_snapshots: dict[str, Any] = {}
    manual_reconstruction_steps = 0
    elapsed_ms = 0.0
    db_transactions = 0

    try:
        db_name, role_name, db_password, database_url = _create_trial_database(trial_id)
        database_created = True
        provider_process, provider_ready = _start_service(
            run_id=run_id,
            trial_id=trial_id,
            role="provider",
            module="experiments.phase_d.provider_service",
            uid=UID_PROVIDER,
            gid=GID_PROVIDER,
            args=["--trial-id", trial_id, "--arm", arm, "--seed", str(seed),
                  "--ledger-path", str(provider_ledger), "--port", "0"],
            extra_env={
                "PHASED_PROVIDER_PROXY_KEY": secrets_by_role["provider_proxy"],
                "PHASED_PROVIDER_ORACLE_KEY": secrets_by_role["provider_oracle"],
            },
            ready_role="phase_d_provider",
        )
        processes["provider"] = provider_process
        ready["provider"] = provider_ready
        provider_url = f"http://127.0.0.1:{provider_ready['port']}"

        proxy_process, proxy_ready = _start_service(
            run_id=run_id,
            trial_id=trial_id,
            role="fault-proxy",
            module="experiments.phase_d.fault_proxy",
            uid=UID_PROXY,
            gid=GID_PROXY,
            args=["--trial-id", trial_id, "--arm", arm, "--seed", str(seed),
                  "--scenario", scenario, "--ledger-path", str(proxy_ledger),
                  "--provider-url", provider_url, "--port", "0"],
            extra_env={
                "PHASED_PROVIDER_PROXY_KEY": secrets_by_role["provider_proxy"],
                "PHASED_PROXY_CLIENT_TOKEN": secrets_by_role["proxy_client"],
            },
            ready_role="phase_d_fault_proxy",
        )
        processes["fault_proxy"] = proxy_process
        ready["fault_proxy"] = proxy_ready
        proxy_url = f"http://127.0.0.1:{proxy_ready['port']}"

        source_process, source_ready = _start_service(
            run_id=run_id,
            trial_id=trial_id,
            role="trusted-source",
            module="experiments.phase_d.source_authority",
            uid=UID_SOURCE,
            gid=GID_SOURCE,
            args=["--trial-id", trial_id, "--arm", arm, "--ledger-path", str(source_ledger), "--port", "0"],
            extra_env={"PHASED_SOURCE_CONTROL_KEY": secrets_by_role["source_control"]},
            ready_role="phase_d_source",
        )
        processes["source"] = source_process
        ready["source"] = source_ready
        source_url = f"http://127.0.0.1:{source_ready['port']}"

        owner_process, owner_ready = _start_service(
            run_id=run_id,
            trial_id=trial_id,
            role="domain-owner",
            module="experiments.phase_d.domain_owner",
            uid=UID_DOMAIN,
            gid=GID_DOMAIN,
            args=["--trial-id", trial_id, "--arm", arm,
                  "--goal-ledger-path", str(goal_ledger),
                  "--authority-ledger-path", str(authority_ledger),
                  "--source-url", source_url, "--provider-url", provider_url, "--port", "0"],
            extra_env={
                "PHASED_OWNER_CONTROL_KEY": secrets_by_role["owner_control"],
                "PHASED_PROVIDER_ORACLE_KEY": secrets_by_role["provider_oracle"],
            },
            ready_role="phase_d_domain_owner",
        )
        processes["domain_owner"] = owner_process
        ready["domain_owner"] = owner_ready
        owner_url = f"http://127.0.0.1:{owner_ready['port']}"

        source_v1 = _source_publish(source_url, secrets_by_role["source_control"], "v1")
        goal_created = _owner_command(owner_url, secrets_by_role["owner_control"], "/v1/owner/goal-created")
        a_authority = _owner_command(owner_url, secrets_by_role["owner_control"], "/v1/owner/grant-agent-a")
        if not a_authority.get("authority_ref"):
            raise TrialInvalid("domain owner did not authorize A's distinct pack/ship grants")

        root_runtime, root_aux = _runtime_handles(arm, database_url, source_url, trial_id)
        _issue_grant(root_runtime, arm, grant_ids["pack"], "pack", "agent-a")
        _issue_grant(root_runtime, arm, grant_ids["ship"], "ship", "agent-a")
        if arm == "postgres_dbos":
            _pin_baseline_grant(
                db_url=database_url, source_url=source_url, trial_id=trial_id, arm=arm,
                grant_id=grant_ids["pack"], action="pack", revision="v1",
                source_event_ref=source_v1["source_event_ref"],
                domain_authority_ref=a_authority["authority_ref"],
            )
            _pin_baseline_grant(
                db_url=database_url, source_url=source_url, trial_id=trial_id, arm=arm,
                grant_id=grant_ids["ship"], action="ship", revision="v1",
                source_event_ref=source_v1["source_event_ref"],
                domain_authority_ref=a_authority["authority_ref"],
            )
        a_grant_ids = {"pack": grant_ids["pack"], "ship": grant_ids["ship"]}
        grant_snapshots = _grant_state(root_runtime, a_grant_ids, arm)
        if grant_snapshots["pack"]["action"] != "pack" or grant_snapshots["ship"]["action"] != "ship":
            raise TrialInvalid("Agent A did not receive distinct action-scoped grants")
        if grant_snapshots["pack"]["actors"] != ["agent-a"] or grant_snapshots["ship"]["actors"] != ["agent-a"]:
            raise TrialInvalid("Agent A grants are not scoped to Agent A only")

        barrier_a = PhaseCBarrier(
            trial_id=trial_id,
            event_dir=event_dir_a,
            release_dir=release_dir_a,
            timeout_seconds=90.0,
        )
        barrier_b = PhaseCBarrier(
            trial_id=trial_id,
            event_dir=event_dir_b,
            release_dir=release_dir_b,
            timeout_seconds=90.0,
        )
        start_ns = time.perf_counter_ns()
        db_stats_before = _pg_stats(db_name)
        a_env = _service_env(UID_AGENT_A, GID_AGENT, run_id, f"agent-a-{trial_id}", {
            "PHASED_DATABASE_URL": database_url,
            "PHASED_TRIAL_ID": trial_id,
            "PHASED_ARM": arm,
            "PHASED_ACTOR": "agent-a",
            "PHASED_PAIR_ID": pair_id,
            "PHASED_PROXY_URL": proxy_url,
            "PHASED_PROXY_CLIENT_TOKEN": secrets_by_role["proxy_client"],
            "PHASED_SOURCE_URL": source_url,
            "PHASED_V1_EVIDENCE_REF": source_v1["source_event_ref"],
            "PHASED_AGENT_GRANTS_JSON": json.dumps({
                "pack": grant_ids["pack"], "ship": grant_ids["ship"],
            }, sort_keys=True),
            "PHASED_BARRIER_EVENT_DIR": str(event_dir_a),
            "PHASED_BARRIER_RELEASE_DIR": str(release_dir_a),
            "PHASED_APPLICATION_VERSION": os.environ["PHASED_CODE_SHA"],
        })
        worker_module = (
            "experiments.phase_d.continuity_worker"
            if arm == "continuity" else "experiments.phase_d.baseline_worker"
        )
        a_process = ManagedProcess(
            [sys.executable, "-m", worker_module, "--stage", "agent-a",
             "--operation-id", operation_ids["pack"],
             "--operation-id", operation_ids["ship"],
             "--grant-id", grant_ids["pack"]],
            env=a_env,
            uid=UID_AGENT_A,
            gid=GID_AGENT,
            cwd=ROOT,
            name=f"agent-a-{arm}-{trial_id}",
        )
        processes["agent_a"] = a_process
        a_process.start()
        ready_event = _wait_event(
            barrier_a, "a_ready_for_v2", pair_id, a_process, timeout_seconds=120.0,
        )
        pack_verification = _owner_command(
            owner_url, secrets_by_role["owner_control"], "/v1/owner/verify-pack",
        )
        if (
            pack_verification.get("verified") is not True
            or pack_verification.get("counts") != {"pack": 1, "ship": 0, "quarantine": 0}
        ):
            raise TrialInvalid("independent owner readback did not verify exactly one historical pack")
        pack_readback_ref = str(pack_verification.get("provider_readback_ref", ""))
        if not pack_readback_ref:
            raise TrialInvalid("pack verification omitted its independent provider readback reference")

        source_v2 = _source_publish(source_url, secrets_by_role["source_control"], "v2")
        _release(barrier_a, "a_ready_for_v2", pair_id)
        stale_event = _wait_event(
            barrier_a, "stale_ship_attempt_done", operation_ids["ship"],
            a_process, timeout_seconds=90.0,
        )
        a_exit = a_process.wait(60.0)
        if a_exit != 0:
            raise TrialInvalid(f"Agent A worker exited {a_exit} before handoff")
        a_payload = _parse_result(a_process, "PHASED_RESULT=")
        a_events = _phase_events(a_process)
        stale_events = [event for event in a_events if event.get("type") == "stale_ship_rejected_before_revocation"]
        if len(stale_events) != 1 or not a_payload.get("stale_rejection"):
            raise TrialInvalid("A did not record one rejected stale ship dispatch")
        stale_index = a_events.index(stale_events[0]) + 1
        stale_ref = _event_locator(
            f"{relative_base}/agent_a_events.jsonl", stale_events[0], stale_index,
        )
        if a_payload.get("link_active") is not False:
            raise TrialInvalid("A's v1-dependent link remained active after trusted v2")
        invalidation_seq = a_payload.get("invalidation_event_seq")
        if type(invalidation_seq) is not int or invalidation_seq < 1:
            raise TrialInvalid("v1 link invalidation lacks a persisted event sequence")
        invalidation_ref, invalidation_events = _extract_invalidation(
            arm, root_runtime, database_url, trial_id, source_url, SOURCE_REF,
            invalidation_seq, relative_base,
        )

        evidence_revised = _owner_command(
            owner_url, secrets_by_role["owner_control"], "/v1/owner/evidence-revised",
        )
        superseded = _owner_command(
            owner_url, secrets_by_role["owner_control"], "/v1/owner/supersede",
        )
        if not superseded.get("goal_event_ref"):
            raise TrialInvalid("business owner did not append old-goal supersession")
        handoff_ref = ""
        if scenario == "unauthorized_actor_during_handoff":
            outsider_process, outsider_result = _run_outsider_probe(
                run_id=run_id,
                trial_id=trial_id,
                arm=arm,
                db_url=database_url,
                source_url=source_url,
                owner_url=owner_url,
                grant_ids=grant_ids,
                pack_operation_id=operation_ids["pack"],
            )
            processes["outsider"] = outsider_process

        root_runtime.revoke_grant(grant_ids["pack"])
        root_runtime.revoke_grant(grant_ids["ship"])
        grants_after_revoke = _grant_state(root_runtime, a_grant_ids, arm)
        if not grants_after_revoke["pack"]["revoked"] or not grants_after_revoke["ship"]["revoked"]:
            raise TrialInvalid("A's original pack/ship grants were not both revoked")
        a_revoked = _owner_command(
            owner_url, secrets_by_role["owner_control"], "/v1/owner/revoke-agent-a",
            {"stale_dispatch_ref": stale_ref},
        )
        handoff = _owner_command(
            owner_url, secrets_by_role["owner_control"], "/v1/owner/handoff",
        )
        handoff_ref = _domain_owner_write_ref(handoff)
        b_authority = _owner_command(
            owner_url, secrets_by_role["owner_control"], "/v1/owner/grant-agent-b",
        )
        if not b_authority.get("authority_ref"):
            raise TrialInvalid("business owner did not issue B's new quarantine authority")
        _issue_grant(root_runtime, arm, grant_ids["quarantine"], "quarantine", "agent-b")
        if arm == "postgres_dbos":
            _pin_baseline_grant(
                db_url=database_url, source_url=source_url, trial_id=trial_id, arm=arm,
                grant_id=grant_ids["quarantine"], action="quarantine", revision="v2",
                source_event_ref=source_v2["source_event_ref"],
                domain_authority_ref=b_authority["authority_ref"],
            )
        grant_snapshots = _grant_state(root_runtime, grant_ids, arm)
        b_grant = grant_snapshots["quarantine"]
        if (
            b_grant.get("revoked") is not False
            or b_grant.get("actors") != ["agent-b"]
            or b_grant.get("action") != "quarantine"
            or b_grant.get("required_version_refs") != [SOURCE_REF]
        ):
            raise TrialInvalid("B's new grant is not actor/action/version scoped")

        projection = _owner_projection(owner_url)
        if projection.get("authority_ref") != b_authority.get("authority_ref"):
            raise TrialInvalid("Agent B projection authority does not match owner grant decision")
        projection_dir = Path("/tmp/phase-d-projections") / safe_component(run_id) / safe_component(trial_id)
        projection_dir.mkdir(parents=True, exist_ok=True)
        os.chown(projection_dir, UID_AGENT_B, GID_AGENT_B)
        os.chmod(projection_dir, 0o700)
        projection_path = projection_dir / "domain-authorized-projection.json"
        with projection_path.open("wb") as handle:
            handle.write(_json_bytes(projection))
            handle.flush()
            os.fsync(handle.fileno())
        os.chown(projection_path, UID_AGENT_B, GID_AGENT_B)
        os.chmod(projection_path, 0o400)
        projection_digest = _sha256_file(projection_path)

        b_env = _service_env(UID_AGENT_B, GID_AGENT_B, run_id, f"agent-b-{trial_id}", {
            "PHASED_DATABASE_URL": database_url,
            "PHASED_TRIAL_ID": trial_id,
            "PHASED_ARM": arm,
            "PHASED_ACTOR": "agent-b",
            "PHASED_PAIR_ID": pair_id,
            "PHASED_PROXY_URL": proxy_url,
            "PHASED_PROXY_CLIENT_TOKEN": secrets_by_role["proxy_client"],
            "PHASED_SOURCE_URL": source_url,
            "PHASED_BARRIER_EVENT_DIR": str(event_dir_b),
            "PHASED_BARRIER_RELEASE_DIR": str(release_dir_b),
            "PHASED_APPLICATION_VERSION": os.environ["PHASED_CODE_SHA"],
        })
        b_process = ManagedProcess(
            [sys.executable, "-m", worker_module, "--stage", "agent-b",
             "--operation-id", operation_ids["quarantine"],
             "--grant-id", grant_ids["quarantine"],
             "--projection", str(projection_path)],
            env=b_env,
            uid=UID_AGENT_B,
            gid=GID_AGENT_B,
            cwd=ROOT,
            name=f"agent-b-{arm}-{trial_id}",
        )
        processes["agent_b"] = b_process
        b_process.start()
        quarantine_reserved = _wait_event(
            barrier_b, "reserved_before_send", operation_ids["quarantine"],
            b_process, timeout_seconds=90.0,
        )
        b_reserved_row = _operation_snapshot(root_runtime, arm, operation_ids["quarantine"], "agent-b")
        if b_reserved_row.get("dispatch_generation") != 1:
            raise TrialInvalid("B quarantine barrier was not after durable generation-1 reservation")
        before_quarantine_counts, _ = _provider_counts(
            provider_url, secrets_by_role["provider_oracle"], trial_id, arm,
        )
        if before_quarantine_counts != {"pack": 1, "ship": 0, "quarantine": 0}:
            raise TrialInvalid("provider state changed before B's quarantined action release")

        if scenario == "new_action_crash_before_send":
            release_marker_present = barrier_b.release_path(
                "reserved_before_send", operation_ids["quarantine"],
            ).exists()
            if release_marker_present:
                raise TrialInvalid("crash scenario unexpectedly released B's pre-send barrier")
            b_exit = b_process.kill(signal.SIGKILL)
            if b_exit != -9:
                raise TrialInvalid("B crash injection did not produce the expected SIGKILL exit")
            after_kill_counts, _ = _provider_counts(
                provider_url, secrets_by_role["provider_oracle"], trial_id, arm,
            )
            if after_kill_counts.get("quarantine") != 0:
                raise TrialInvalid("quarantine provider POST occurred before/after the crash barrier")
            crash_proof = {
                "barrier_reached": True,
                "barrier_event": quarantine_reserved,
                "reservation_generation": b_reserved_row.get("dispatch_generation"),
                "worker_pid": b_process.proc.pid if b_process.proc else None,
                "worker_exit_code": b_exit,
                "provider_counts_after_kill": after_kill_counts,
                "release_marker_present": release_marker_present,
            }
            crash_proof_path = trial_output / "crash_barrier_proof.json"
            _write_json(crash_proof_path, crash_proof)
            root_recovery = _root_reconcile_only(
                root_runtime, arm, operation_ids["quarantine"], trial_id,
                proxy_url, secrets_by_role["proxy_client"],
            )
            result_bundle["root_recovery"] = root_recovery
        else:
            _release(barrier_b, "reserved_before_send", operation_ids["quarantine"])
            b_exit = b_process.wait(90.0)
            if b_exit != 0:
                raise TrialInvalid(f"Agent B worker exited {b_exit} after allowed handoff")
            b_payload = _parse_result(b_process, "PHASED_RESULT=")
            b_events = _phase_events(b_process)
            if not b_payload.get("quarantine_operation"):
                raise TrialInvalid("Agent B did not retain its new quarantine operation")

        goal_terminal_body = (
            {"crash_barrier_ref": f"crash-barrier:{trial_id}:sha256={_sha256_file(trial_output / 'crash_barrier_proof.json')}"}
            if crash_proof is not None else {}
        )
        finalized = _owner_command(
            owner_url, secrets_by_role["owner_control"], "/v1/owner/finalize", goal_terminal_body,
        )
        reported_goal_status = finalized.get("goal_status")
        final_provider_counts = finalized.get("provider_counts")
        if reported_goal_status not in {"satisfied", "unresolved"}:
            raise TrialInvalid("domain verifier did not report a frozen goal status")
        if reported_goal_status != expected_status:
            # Preserve observed status. The precommitted evaluator will classify
            # a completed 24-row dataset; never rewrite it to expected success.
            pass
        if reported_goal_status == "satisfied":
            new_goal_verifier_ref = finalized.get("verifier_ref")
            if not isinstance(new_goal_verifier_ref, str) or not new_goal_verifier_ref:
                raise TrialInvalid("satisfied goal lacks independent verifier reference")
        else:
            new_goal_verifier_ref = None
        end_ns = time.perf_counter_ns()
        elapsed_ms = round((end_ns - start_ns) / 1e6, 3)
        db_stats_after = _pg_stats(db_name)
        db_transactions = db_stats_after["total"] - db_stats_before["total"]
        if db_transactions < 0:
            raise TrialInvalid("PostgreSQL transaction counter regressed")
        operation_snapshots = {
            "pack": _operation_snapshot(root_runtime, arm, operation_ids["pack"], "agent-a"),
            "ship": _operation_snapshot(root_runtime, arm, operation_ids["ship"], "agent-a"),
            "quarantine": _operation_snapshot(root_runtime, arm, operation_ids["quarantine"], "agent-b"),
        }
        if operation_snapshots["quarantine"].get("dispatch_generation") != 1:
            raise TrialInvalid("B operation generation is not exactly one")
        if operation_snapshots["ship"].get("dispatch_generation") != 0:
            raise TrialInvalid("stale A ship operation reserved a provider dispatch")
        if not all(grants_after_revoke[key].get("revoked") is True for key in ("pack", "ship")):
            raise TrialInvalid("A's original grants are not both revoked")
        if final_provider_counts is None:
            raise TrialInvalid("domain finalizer omitted independent provider counts")
        root_runtime_events = _runtime_events(root_runtime, arm)
        if arm == "continuity":
            invalidation_events = [event for event in root_runtime_events
                                   if event.get("kind") == "dependency.invalidated"]
        else:
            reader = TrustedSourceReader(source_url, SOURCE_REF, trial_id, arm)
            history = BaselineHistory(database_url, version_reader=reader, create_schema=False)
            try:
                invalidation_events = history.events()
            finally:
                history.engine.dispose()
        handoff_ref = _domain_owner_write_ref(handoff)
        completed = True
        result_bundle.update({
            "trial_id": trial_id,
            "pair_id": pair_id,
            "scenario": scenario,
            "seed": seed,
            "arm": arm,
            "arm_order": arm_order,
            "run_mode": mode,
            "source_v1_event_ref": source_v1.get("source_event_ref"),
            "source_v2_event_ref": source_v2.get("source_event_ref"),
            "goal_created_ref": goal_created.get("goal_event_ref"),
            "goal_evidence_revised_ref": evidence_revised.get("goal_event_ref"),
            "goal_superseded_ref": superseded.get("goal_event_ref"),
            "a_authority_ref": a_authority.get("authority_ref"),
            "a_revocation_ref": a_revoked.get("authority_ref"),
            "handoff_ref": handoff_ref,
            "b_authority_ref": b_authority.get("authority_ref"),
            "projection_sha256": projection_digest,
            "projection": projection,
            "pack_readback_ref": pack_readback_ref,
            "stale_dispatch_ref": stale_ref,
            "invalidation_ref": invalidation_ref,
            "a_barrier_event": ready_event,
            "stale_barrier_event": stale_event,
            "b_reservation_event": quarantine_reserved,
            "outsider_result": outsider_result,
            "crash_proof": crash_proof,
            "goal_finalizer": finalized,
            "reported_goal_status": reported_goal_status,
            "expected_goal_status": expected_status,
            "provider_counts": final_provider_counts,
            "elapsed_ms": elapsed_ms,
            "db_transactions": db_transactions,
            "db_stats_before": db_stats_before,
            "db_stats_after": db_stats_after,
            "manual_reconstruction_steps": 5,
            "operator_seconds": None,
            "model_cost_usd": 0.0,
            "infra_cost_usd": None,
            "operation_snapshots": operation_snapshots,
            "grant_snapshots": grant_snapshots,
            "root_runtime_events": root_runtime_events,
            "baseline_history_events": invalidation_events if arm == "postgres_dbos" else [],
            "a_worker_exit_code": a_exit,
            "b_worker_exit_code": b_exit,
            "outsider_worker_exit_code": outsider_process.poll() if outsider_process else None,
            "service_ready": ready,
            "process_pids": {
                name: (process.proc.pid if process.proc else None)
                for name, process in processes.items()
            },
        })
    except BaseException as exc:
        error_value = _safe_error(exc)
        result_bundle["error"] = error_value
        raise
    finally:
        # Stop workers first, then independent services. The B SIGKILL in the
        # crash case is recorded as an expected injection, not hidden cleanup.
        for name in ("outsider", "agent_b", "agent_a", "domain_owner", "source", "fault_proxy", "provider"):
            process = processes.get(name)
            if process is None or process.poll() is not None:
                if process is not None:
                    cleanup["services"][name] = process.poll()
                continue
            try:
                cleanup["services"][name] = process.interrupt(timeout_seconds=10.0)
            except Exception:
                try:
                    cleanup["services"][name] = process.kill(signal.SIGKILL)
                except Exception:
                    cleanup["services"][name] = "unreaped"
        cleanup["processes"] = {
            name: {
                "pid": process.proc.pid if process.proc else None,
                "uid": process.uid,
                "gid": process.gid,
                "exit_code": process.poll(),
            }
            for name, process in processes.items()
        }
        if root_runtime is not None:
            try:
                if arm == "continuity" and root_aux is not None:
                    root_aux.close()
                elif arm == "postgres_dbos":
                    root_runtime.close()
            except Exception as close_exc:
                cleanup["runtime_close_error"] = _safe_error(close_exc)
        if database_url and database_created:
            try:
                snapshot_path = trial_output / "database_snapshot.json"
                if not snapshot_path.exists():
                    _write_json(snapshot_path, _dump_database_snapshot(database_url, arm))
                result_bundle["database_snapshot_sha256"] = _sha256_file(snapshot_path)
            except Exception as db_exc:
                result_bundle["database_snapshot_error"] = _safe_error(db_exc)
        if database_created and db_name and role_name:
            try:
                _drop_trial_database(db_name, role_name)
                cleanup["database_dropped"] = True
            except Exception as drop_exc:
                cleanup["database_drop_error"] = _safe_error(drop_exc)
        ledger_files: dict[str, Any] = {}
        for key, source_path in (
            ("provider", provider_ledger),
            ("source", source_ledger),
            ("goal", goal_ledger),
            ("authority", authority_ledger),
        ):
            try:
                if source_path.is_file():
                    ledger_files[key] = _copy_ledger(source_path, trial_output / f"{key}_ledger.jsonl")
            except Exception as copy_exc:
                ledger_files[key] = {"error": _safe_error(copy_exc)}
        try:
            if proxy_ledger.is_file():
                ledger_files["fault_proxy"] = _copy_proxy_ledger(proxy_ledger, trial_output / "fault_proxy_ledger.jsonl")
        except Exception as copy_exc:
            ledger_files["fault_proxy"] = {"error": _safe_error(copy_exc)}
        try:
            barriers_copy = {
                "agent_a": _copy_barriers(event_dir_a, release_dir_a, trial_output / "barriers" / "agent_a"),
                "agent_b": _copy_barriers(event_dir_b, release_dir_b, trial_output / "barriers" / "agent_b"),
            }
        except Exception as copy_exc:
            barriers_copy = {"error": _safe_error(copy_exc)}
        if a_process is not None:
            a_events = _phase_events(a_process)
        if b_process is not None:
            b_events = _phase_events(b_process)
        process_log_metadata: dict[str, Any] = {}
        secret_values = list(secrets_by_role.values())
        if db_password:
            secret_values.append(db_password)
        for process_name, process in processes.items():
            try:
                process_log_metadata[process_name] = _write_process_log(
                    trial_output / "process_logs" / f"{process_name}.log",
                    process, secret_values, database_url,
                )
            except Exception as log_exc:
                process_log_metadata[process_name] = {"error": _safe_error(log_exc)}
        result_bundle["process_log_artifacts"] = process_log_metadata
        combined_events = [
            {"process": "agent-a", **event} for event in a_events
        ] + [
            {"process": "agent-b", **event} for event in b_events
        ]
        if combined_events:
            _write_event_rows(trial_output / "arm_events.jsonl", combined_events)
        if a_events:
            _write_event_rows(trial_output / "agent_a_events.jsonl", a_events)
        if b_events:
            _write_event_rows(trial_output / "agent_b_events.jsonl", b_events)
        if projection is not None:
            _write_json(trial_output / "domain_authorized_projection.json", projection)
        if result_bundle.get("root_recovery"):
            _write_json(trial_output / "recovery_readback.json", result_bundle["root_recovery"])
        if result_bundle.get("crash_proof"):
            _write_json(trial_output / "crash_barrier_proof.json", result_bundle["crash_proof"])
        if result_bundle.get("root_runtime_events"):
            _write_jsonl(trial_output / "runtime_events.jsonl", result_bundle["root_runtime_events"])
        if result_bundle.get("baseline_history_events"):
            _write_jsonl(trial_output / "baseline_history_events.jsonl", result_bundle["baseline_history_events"])
        if result_bundle.get("operation_snapshots"):
            _write_json(trial_output / "operation_snapshots.json", result_bundle["operation_snapshots"])
        if result_bundle.get("grant_snapshots"):
            _write_json(trial_output / "grant_snapshots.json", result_bundle["grant_snapshots"])
        if event_dir_a.exists() or release_dir_a.exists() or event_dir_b.exists() or release_dir_b.exists():
            result_bundle["barriers"] = barriers_copy
        result_bundle["ledger_files"] = ledger_files
        result_bundle["cleanup"] = cleanup
        result_bundle["status"] = "complete" if completed else "aborted"
        if error_value:
            result_bundle["error"] = error_value
        _write_json(trial_output / "trial_diagnostics.json", {
            key: value for key, value in result_bundle.items()
        })

    if not completed:
        raise TrialInvalid("Phase D trial aborted without a complete evidence row")
    expected_exits = {
        "provider": 0,
        "fault_proxy": 0,
        "source": 0,
        "domain_owner": 0,
        "agent_a": 0,
        "agent_b": -9 if scenario == "new_action_crash_before_send" else 0,
    }
    if scenario == "unauthorized_actor_during_handoff":
        expected_exits["outsider"] = 0
    for name, expected in expected_exits.items():
        actual = cleanup["services"].get(name)
        if actual != expected:
            raise TrialInvalid(f"{name} process cleanup exit {actual!r} did not match {expected}")
    if cleanup.get("database_dropped") is not True:
        raise TrialInvalid("per-trial PostgreSQL database/role cleanup was not verified")
    if not isinstance(result_bundle.get("ledger_files"), dict) or any(
        not isinstance(result_bundle["ledger_files"].get(name), dict)
        or result_bundle["ledger_files"][name].get("sha256") is None
        for name in ("provider", "fault_proxy", "source", "goal", "authority")
    ):
        raise TrialInvalid("one or more independent ledger artifacts were not copied/verified")
    if not isinstance(result_bundle.get("barriers"), dict) or result_bundle["barriers"].get("error"):
        raise TrialInvalid("reservation barrier evidence was not copied")

    # The human review path uses a fixed five-source checklist; the number is
    # computed from real ledger/database reads. Human elapsed time is unknown.
    reconstruction_steps = _reconstruction_audit(trial_output)
    manual_reconstruction_steps = len(reconstruction_steps)
    _write_json(trial_output / "reconstruction_checklist.json", {
        "steps": reconstruction_steps,
        "steps_completed": manual_reconstruction_steps,
        "operator_seconds": None,
        "note": "scripted evidence lookup count; human operator time was not observed",
    })

    a_pack = operation_snapshots["pack"]
    a_ship = operation_snapshots["ship"]
    b_quarantine = operation_snapshots["quarantine"]
    old_pack_reconciled = bool(
        a_pack.get("effect_disposition") == "present"
        and a_pack.get("outcome_disposition") == "satisfied"
    )
    old_ship_rejected = bool(
        a_payload and a_payload.get("stale_rejection")
        and int(a_ship.get("dispatch_generation", 0)) == 0
    )
    v1_invalidated = bool(a_payload and a_payload.get("link_active") is False)
    a_grants_revoked = bool(
        grant_snapshots["pack"].get("revoked") is True
        and grant_snapshots["ship"].get("revoked") is True
    )
    b_grant_issued = bool(
        grant_snapshots["quarantine"].get("revoked") is False
        and grant_snapshots["quarantine"].get("actors") == ["agent-b"]
    )
    invalidation_seq = a_payload.get("invalidation_event_seq") if a_payload else None
    if not all((old_pack_reconciled, old_ship_rejected, v1_invalidated, a_grants_revoked, b_grant_issued)):
        raise TrialInvalid("required arm-side functionality evidence is incomplete")

    evidence_rows, evidence_head, _ = read_verified_ledger(source_ledger)
    domain_rows, domain_head, _ = read_verified_ledger(goal_ledger)
    authority_rows, authority_head, _ = read_verified_ledger(authority_ledger)
    provider_rows, provider_head, _ = read_verified_ledger(provider_ledger)
    proxy_rows, proxy_head, _ = read_proxy_ledger(str(proxy_ledger))
    (trial_output / "trial_diagnostics.json").exists()
    oracle_row = derive_oracle(
        scenario=scenario,
        seed=seed,
        arm=arm,
        relative_base=relative_base,
        provider_path=trial_output / "provider_ledger.jsonl",
        proxy_path=trial_output / "fault_proxy_ledger.jsonl",
        source_path=trial_output / "source_ledger.jsonl",
        goal_path=trial_output / "goal_ledger.jsonl",
        authority_path=trial_output / "authority_ledger.jsonl",
        outsider_result=outsider_result,
        crash_proof=crash_proof,
    )
    if scenario == "pack_ack_lost_then_supersession":
        fault_rows = [row for row in proxy_rows if row["event"].get("type") == "fault_triggered"]
        if len(fault_rows) != 1:
            raise TrialInvalid("ack-loss fault proof is not exactly one proxy event")
        fault_ref = f"phase-d-fault-ledger:{fault_rows[0]['seq']}:sha256={fault_rows[0]['hash']}"
    elif scenario == "unauthorized_actor_during_handoff":
        denials = [row for row in authority_rows
                   if row["event"].get("type") == "domain_write_denied"
                   and row["event"].get("path") == "/v1/owner/grant-agent-b"]
        if not denials:
            raise TrialInvalid("unauthorized owner-control write denial is not in raw authority ledger")
        fault_ref = f"authority-event:{trial_id}:{denials[-1]['seq']}:sha256={denials[-1]['hash']}"
    elif scenario == "new_action_crash_before_send":
        fault_ref = f"crash-barrier:{trial_id}:sha256={_sha256_file(trial_output / 'crash_barrier_proof.json')}"

    trial_row = {
        "scenario": scenario,
        "seed": seed,
        "arm": arm,
        "reported_goal_status": reported_goal_status,
        "old_ship_dispatch_rejected": old_ship_rejected,
        "old_pack_reconciled": old_pack_reconciled,
        "v1_link_invalidated": v1_invalidated,
        "a_write_grants_revoked": a_grants_revoked,
        "b_grant_newly_issued": b_grant_issued,
        "b_operation_generation": int(b_quarantine.get("dispatch_generation", 0)),
        "pack_readback_ref": pack_readback_ref,
        "new_goal_verifier_ref": new_goal_verifier_ref,
        "stale_dispatch_ref": stale_ref,
        "invalidation_ref": invalidation_ref,
        "handoff_ref": handoff_ref,
        "fault_proof_ref": fault_ref,
        "elapsed_ms": float(elapsed_ms),
        "db_transactions": int(db_transactions),
        "manual_reconstruction_steps": int(manual_reconstruction_steps),
        "operator_seconds": None,
        "model_cost_usd": 0.0,
        "infra_cost_usd": None,
    }
    evidence_row = {
        "scenario": scenario,
        "seed": seed,
        "arm": arm,
        "trial_id": trial_id,
        "trial_relative_path": relative_base,
        "expected_goal_status": expected_status,
        "reported_goal_status": reported_goal_status,
        "provider_counts": final_provider_counts,
        "provider_ledger_sha256": provider_head,
        "fault_proxy_ledger_sha256": proxy_head,
        "source_ledger_sha256": evidence_head,
        "goal_ledger_sha256": domain_head,
        "authority_ledger_sha256": authority_head,
        "stale_dispatch_ref": stale_ref,
        "invalidation_ref": invalidation_ref,
        "handoff_ref": handoff_ref,
        "pack_readback_ref": pack_readback_ref,
        "new_goal_verifier_ref": new_goal_verifier_ref,
        "fault_proof_ref": fault_ref,
        "cleanup": cleanup,
    }
    return trial_row, oracle_row, evidence_row


def _run_preflight_tests(run_id: str) -> dict[str, Any]:
    test_db_name = test_role = None
    test_db_created = False
    output: dict[str, Any] = {"trials_counted": False, "exit_code": None}
    try:
        test_db_name, test_role, _test_password, test_url = _create_trial_database("preflight-" + run_id)
        test_db_created = True
        env = _service_env(0, 0, run_id, "preflight-tests", {
            "PHASEC_TEST_POSTGRES_URL": test_url,
            "CONTINUITY_TEST_POSTGRES_URL": test_url,
        })
        commands = [
            ([sys.executable, "-m", "pytest", "-q"], 300),
            ([sys.executable, "-m", "ruff", "check", "experiments/phase_d",
              "tests/test_phase_d_evaluator.py", "tests/test_phase_d_baseline_history.py",
              "tests/test_phase_d_services.py"], 120),
        ]
        outputs = []
        exit_codes = []
        for command, timeout in commands:
            proc = subprocess.run(
                command,
                cwd=ROOT,
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=timeout,
                check=False,
            )
            safe = proc.stdout.replace(test_url, "[preflight database URL redacted]")
            try:
                safe = safe.replace(make_url(test_url).render_as_string(hide_password=False),
                                    "[preflight database URL redacted]")
            except Exception:
                pass
            outputs.append({"command": command[2:4], "exit_code": proc.returncode,
                            "output_tail": safe[-6000:]})
            exit_codes.append(proc.returncode)
        output.update({
            "tests": outputs,
            "exit_code": 0 if all(code == 0 for code in exit_codes) else max(exit_codes),
            "database_scope": "separate disposable PostgreSQL maintenance database; not a trial DBOS database",
            "database_dropped": False,
        })
    finally:
        if test_db_created and test_db_name and test_role:
            _drop_trial_database(test_db_name, test_role)
            output["database_dropped"] = True
    return output


def _run_isolation_preflight(run_id: str, output_root: Path) -> dict[str, Any]:
    trial_id = "iso-" + secrets.token_hex(8)
    provider_dir = Path(os.environ["PHASED_PROVIDER_DATA_DIR"]) / run_id / trial_id
    proxy_dir = Path(os.environ["PHASED_PROXY_LOG_DIR"]) / run_id / trial_id
    provider_ledger = provider_dir / "provider.jsonl"
    proxy_ledger = proxy_dir / "fault_proxy.jsonl"
    _create_ledger(provider_ledger, UID_PROVIDER, GID_PROVIDER)
    _create_ledger(proxy_ledger, UID_PROXY, GID_PROXY)
    provider_key = secrets.token_urlsafe(40)
    oracle_key = secrets.token_urlsafe(40)
    client_token = secrets.token_urlsafe(40)
    preflight_dir = output_root / "preflight" / "isolation"
    preflight_dir.mkdir(parents=True, exist_ok=True)
    processes: dict[str, ManagedProcess] = {}
    statuses: dict[str, Any] = {}
    result: dict[str, Any] = {"trials_counted": False, "trial_id": trial_id}
    try:
        provider_proc, provider_ready = _start_service(
            run_id=run_id,
            trial_id=trial_id,
            role="isolation-provider",
            module="experiments.phase_d.provider_service",
            uid=UID_PROVIDER,
            gid=GID_PROVIDER,
            args=["--trial-id", trial_id, "--arm", "continuity", "--seed", "0",
                  "--ledger-path", str(provider_ledger), "--port", "0"],
            extra_env={"PHASED_PROVIDER_PROXY_KEY": provider_key,
                       "PHASED_PROVIDER_ORACLE_KEY": oracle_key},
            ready_role="phase_d_provider",
        )
        processes["provider"] = provider_proc
        provider_url = f"http://127.0.0.1:{provider_ready['port']}"
        proxy_proc, proxy_ready = _start_service(
            run_id=run_id,
            trial_id=trial_id,
            role="isolation-proxy",
            module="experiments.phase_d.fault_proxy",
            uid=UID_PROXY,
            gid=GID_PROXY,
            args=["--trial-id", trial_id, "--arm", "continuity", "--seed", "0",
                  "--scenario", "normal_supersession", "--ledger-path", str(proxy_ledger),
                  "--provider-url", provider_url, "--port", "0"],
            extra_env={"PHASED_PROVIDER_PROXY_KEY": provider_key,
                       "PHASED_PROXY_CLIENT_TOKEN": client_token},
            ready_role="phase_d_fault_proxy",
        )
        processes["fault_proxy"] = proxy_proc
        proxy_url = f"http://127.0.0.1:{proxy_ready['port']}"
        probe_env = _service_env(UID_AGENT_A, GID_AGENT, run_id, "isolation-probe", {
            "PHASED_PROBE_TRIAL_ID": trial_id,
            "PHASED_PROVIDER_URL": provider_url,
            "PHASED_PROXY_URL": proxy_url,
            "PHASED_PROVIDER_LEDGER_PATH": str(provider_ledger),
            "PHASED_PROXY_LEDGER_PATH": str(proxy_ledger),
            "PHASED_PROVIDER_PID": str(provider_proc.proc.pid),
            "PHASED_PROXY_PID": str(proxy_proc.proc.pid),
        })
        probe = ManagedProcess(
            [sys.executable, "-m", "experiments.phase_d.isolation_probe"],
            env=probe_env,
            uid=UID_AGENT_A,
            gid=GID_AGENT,
            cwd=ROOT,
            name=f"isolation-probe-{trial_id}",
        )
        processes["probe"] = probe
        probe.start()
        code = probe.wait(20.0)
        result["probe_exit_code"] = code
        probe_payload = _parse_result(probe, "PHASED_ISOLATION=")
        result["probe"] = probe_payload
        if code != 0 or probe_payload.get("denied") is not True:
            raise TrialInvalid("provider/proxy credential and ledger isolation preflight failed")
        result["service_ready"] = {"provider": provider_ready, "fault_proxy": proxy_ready}
    finally:
        for name in ("probe", "fault_proxy", "provider"):
            process = processes.get(name)
            if process is None:
                continue
            if process.poll() is None:
                try:
                    statuses[name] = process.interrupt(timeout_seconds=10.0)
                except Exception:
                    statuses[name] = process.kill(signal.SIGKILL)
            else:
                statuses[name] = process.poll()
        result["process_exit_codes"] = statuses
        try:
            result["provider_ledger"] = _copy_ledger(
                provider_ledger, preflight_dir / "provider_ledger.jsonl",
            )
            result["proxy_ledger"] = _copy_proxy_ledger(
                proxy_ledger, preflight_dir / "fault_proxy_ledger.jsonl",
            )
            provider_rows, provider_head, _ = read_verified_ledger(provider_ledger)
            proxy_rows, proxy_head, _ = read_proxy_ledger(str(proxy_ledger))
            provider_events = [row["event"] for row in provider_rows]
            proxy_events = [row["event"] for row in proxy_rows]
            result["provider_commits"] = sum(
                event.get("type") == "effect_committed" for event in provider_events
            )
            result["proxy_upstream_requests"] = sum(
                event.get("type") == "upstream_attempt_started" for event in proxy_events
            )
            result["ledger_heads"] = {"provider": provider_head, "fault_proxy": proxy_head}
        except Exception as copy_exc:
            result["ledger_copy_error"] = _safe_error(copy_exc)
        _write_json(preflight_dir / "isolation_preflight.json", result)
    if result.get("provider_commits") != 0 or result.get("proxy_upstream_requests") != 0:
        raise TrialInvalid("unauthenticated provider/proxy probe reached an external-effect boundary")
    if any(value != 0 for name, value in statuses.items() if name in {"provider", "fault_proxy"}):
        raise TrialInvalid("isolation preflight services did not stop cleanly")
    return result


def _source_hashes() -> dict[str, str]:
    relatives = [
        "experiments/phase_d/__init__.py",
        "experiments/phase_d/shared.py",
        "experiments/phase_d/baseline_history.py",
        "experiments/phase_d/baseline_worker.py",
        "experiments/phase_d/continuity_worker.py",
        "experiments/phase_d/provider_service.py",
        "experiments/phase_d/fault_proxy.py",
        "experiments/phase_d/source_authority.py",
        "experiments/phase_d/domain_owner.py",
        "experiments/phase_d/oracle_exporter.py",
        "experiments/phase_d/isolation_probe.py",
        "experiments/phase_d/runner.py",
        "experiments/phase_d_manifest.json",
        "scripts/evaluate_phase_d.py",
        "tests/test_phase_d_evaluator.py",
        "tests/test_phase_d_baseline_history.py",
        "tests/test_phase_d_services.py",
        "docs/phase-d-protocol.md",
        "docs/phase-d-codex-handoff.md",
        "Dockerfile.phase-d",
        "compose.phase-d.yaml",
        "scripts/run_phase_d.py",
        "scripts/run_phase_d_docker.ps1",
        "experiments/phase_c/baseline_runtime.py",
        "experiments/phase_c/provider_service.py",
        "experiments/phase_c/shared.py",
        "experiments/phase_c/requirements.lock",
    ]
    return {relative: _sha256_file(ROOT / relative) for relative in relatives if (ROOT / relative).is_file()}


def _write_artifact_manifest(output_root: Path) -> dict[str, Any]:
    rows = []
    for path in sorted(output_root.rglob("*")):
        if not path.is_file() or path.name in {"artifact_manifest.jsonl", "run_metadata.json"}:
            continue
        raw = path.read_bytes()
        rows.append({
            "path": path.relative_to(output_root).as_posix(),
            "sha256": _sha256_bytes(raw),
            "bytes": len(raw),
        })
    _write_jsonl(output_root / "artifact_manifest.jsonl", rows)
    raw = (output_root / "artifact_manifest.jsonl").read_bytes()
    return {"file": "artifact_manifest.jsonl", "sha256": _sha256_bytes(raw), "entries": len(rows)}


def _metadata_base(
    *,
    run_id: str,
    mode: str,
    manifest: dict[str, Any],
    frozen_hashes: dict[str, str],
    output_root: Path,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "run_id": run_id,
        "mode": mode,
        "status": "running",
        "started_at_utc": _utcnow(),
        "finished_at_utc": None,
        "code_sha": os.environ["PHASED_CODE_SHA"],
        "worktree_clean_at_launch": os.environ.get("PHASED_WORKTREE_CLEAN", "false").lower() == "true",
        "design_pr": {"number": 11, "head_commit": DESIGN_COMMIT},
        "frozen_input_sha256": frozen_hashes,
        "source_sha256": _source_hashes(),
        "manifest_scenario_order": [case["id"] for case in manifest["scenarios"]],
        "seeds": [0] if mode == "development" else manifest["seeds"],
        "arms": list(ARMS),
        "acceptance_dataset": mode == "full",
        "container": {
            "runner_image_id": os.environ.get("PHASED_RUNNER_IMAGE_ID"),
            "postgres_image_id": os.environ.get("PHASED_POSTGRES_IMAGE_ID"),
            "postgres_image": os.environ.get("PHASED_POSTGRES_IMAGE"),
            "network_policy": os.environ.get("PHASED_NETWORK_POLICY"),
            "no_published_ports": True,
        },
        "agent_boundary": {
            "agent_a_uid": UID_AGENT_A,
            "agent_b_uid": UID_AGENT_B,
            "outsider_uid": UID_OUTSIDER,
            "provider_and_oracle_ledgers": "separate service-owned files; arm-UID read denial preflight",
            "agent_b_input": "domain-owner projection only; no Agent A operation IDs or private events",
            "barrier_filesystem_scope": "separate Agent A and Agent B directories",
            "baseline_version_scope": "immutable Phase D grant-version pin table checked by baseline worker wrapper",
            "continuity_version_scope": "grant-owned admission profiles pin A to v1 and B to v2",
            "database_scope_limitation": (
                "A and B share one per-trial application DB credential; Runtime actor scoping is API-level, "
                "not an arbitrary-SQL/RLS isolation boundary"
            ),
        },
        "runtime_versions": {
            "python": sys.version.split()[0],
            "platform": platform.platform(),
            "postgres_server": _pg_version(),
            "dbos": importlib.metadata.version("dbos"),
            "sqlalchemy": importlib.metadata.version("sqlalchemy"),
            "psycopg": importlib.metadata.version("psycopg"),
        },
        "resource_limits": {
            "runner_cpus": 2,
            "runner_memory_mb": 4096,
            "postgres_cpus": 2,
            "postgres_memory_mb": 2048,
        },
        "setup_maintenance_effort_by_arm": {
            "continuity": {"operator_seconds": None, "note": "not prospectively time-tracked"},
            "postgres_dbos": {"operator_seconds": None, "note": "not prospectively time-tracked"},
        },
        "cost_accounting": {
            "model_cost_usd": 0.0,
            "infra_cost_usd": None,
            "operator_seconds": None,
            "note": "no model/provider/cloud billing calls; local hardware cost was not attributed",
        },
        "output_relative_path": output_root.name,
        "trials_expected": (4 * 2 if mode == "development" else 4 * 3 * 2),
        "trials_completed": 0,
        "evaluator_ran": False,
        "evaluator_exit_code": None,
        "evaluator_verdict": None,
        "cleanup": {"trial_databases_dropped": 0, "trial_databases_expected": 0},
    }


def _run_evaluator(output_root: Path) -> dict[str, Any]:
    evaluator_output = output_root / "evaluation.json"
    proc = subprocess.run(
        [
            sys.executable,
            "scripts/evaluate_phase_d.py",
            "--manifest", "experiments/phase_d_manifest.json",
            "--trials", str(output_root / "trials.jsonl"),
            "--oracle", str(output_root / "provider_domain_oracle.jsonl"),
            "--output", str(evaluator_output),
        ],
        cwd=ROOT,
        env=_service_env(0, 0, output_root.name, "evaluator", {}),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=120,
        check=False,
    )
    if proc.stdout:
        (output_root / "evaluator_stdout.txt").write_text(proc.stdout[-6000:], encoding="utf-8")
        os.chmod(output_root / "evaluator_stdout.txt", 0o600)
    result = json.loads(evaluator_output.read_text(encoding="utf-8")) if evaluator_output.exists() else {}
    return {"exit_code": proc.returncode, "output": result}


def run_phase_d(mode: str, output_dir: str | Path) -> int:
    if mode not in {"development", "full"}:
        raise ValueError("mode must be development or full")
    _check_environment(mode)
    manifest, frozen_hashes = _load_frozen_manifest()
    output_root = Path(output_dir).resolve()
    results_root = Path(os.environ["PHASED_OUTPUT_DIR"]).resolve()
    if not output_root.is_relative_to(results_root):
        raise RuntimeError("output directory must stay under the bound /results volume")
    if output_root.exists() and any(output_root.iterdir()):
        raise RuntimeError("refusing to overwrite a nonempty Phase D output directory")
    output_root.mkdir(parents=True, exist_ok=True)
    os.chmod(output_root, 0o700)
    run_id = "phased-" + uuid.uuid4().hex[:16]
    metadata = _metadata_base(
        run_id=run_id,
        mode=mode,
        manifest=manifest,
        frozen_hashes=frozen_hashes,
        output_root=output_root,
    )
    trial_rows: list[dict[str, Any]] = []
    oracle_rows: list[dict[str, Any]] = []
    evidence_rows: list[dict[str, Any]] = []
    run_order: list[dict[str, Any]] = []
    try:
        preflight = _run_preflight_tests(run_id)
        metadata["preflight_tests"] = preflight
        if preflight.get("exit_code") != 0 or preflight.get("database_dropped") is not True:
            raise TrialInvalid("Phase D implementation test preflight failed")
        isolation = _run_isolation_preflight(run_id, output_root)
        metadata["isolation_preflight"] = isolation
        if isolation.get("probe", {}).get("denied") is not True:
            raise TrialInvalid("Phase D provider/proxy isolation preflight failed")

        seeds = [0] if mode == "development" else list(manifest["seeds"])
        for scenario_index, scenario_spec in enumerate(manifest["scenarios"]):
            scenario = scenario_spec["id"]
            for seed in seeds:
                pair_index = scenario_index * len(manifest["seeds"]) + seed
                arm_order = list(ARMS) if pair_index % 2 == 0 else list(reversed(ARMS))
                for arm in arm_order:
                    run_order.append({"scenario": scenario, "seed": seed, "arm": arm,
                                      "arm_order": arm_order})
                    trial, oracle, evidence = run_trial(
                        run_id=run_id,
                        mode=mode,
                        scenario_spec=scenario_spec,
                        seed=seed,
                        arm=arm,
                        arm_order=arm_order,
                        output_root=output_root,
                    )
                    trial_rows.append(trial)
                    oracle_rows.append(oracle)
                    evidence_rows.append(evidence)
                    _write_jsonl(output_root / "partial_trials.jsonl", trial_rows)
                    _write_jsonl(output_root / "partial_provider_domain_oracle.jsonl", oracle_rows)
                    _write_jsonl(output_root / "trial_evidence.jsonl", evidence_rows)

        expected = len(manifest["scenarios"]) * len(seeds) * len(ARMS)
        if len(trial_rows) != expected or len(oracle_rows) != expected:
            raise TrialInvalid("completed Phase D rows do not match the frozen full key set")
        keys = {(row["scenario"], row["seed"], row["arm"]) for row in trial_rows}
        expected_keys = {
            (scenario["id"], seed, arm)
            for scenario in manifest["scenarios"] for seed in seeds for arm in ARMS
        }
        if keys != expected_keys:
            raise TrialInvalid("completed Phase D rows are not the exact scenario×seed×arm product")

        _write_jsonl(output_root / "trials.jsonl", trial_rows)
        _write_jsonl(output_root / "provider_domain_oracle.jsonl", oracle_rows)
        _write_jsonl(output_root / "trial_evidence.jsonl", evidence_rows)
        (output_root / "partial_trials.jsonl").unlink(missing_ok=True)
        (output_root / "partial_provider_domain_oracle.jsonl").unlink(missing_ok=True)
        if mode == "full":
            evaluation = _run_evaluator(output_root)
            metadata["evaluator_ran"] = True
            metadata["evaluator_exit_code"] = evaluation["exit_code"]
            metadata["evaluator_verdict"] = evaluation["output"].get("verdict") if evaluation["output"] else None
            metadata["evaluation_output"] = evaluation["output"]
        else:
            metadata["evaluator_ran"] = False
            metadata["evaluator_exit_code"] = None
            metadata["evaluator_verdict"] = "development_not_scored"
        metadata.update({
            "status": "complete",
            "finished_at_utc": _utcnow(),
            "trials_completed": len(trial_rows),
            "trial_order": run_order,
            "cleanup": {
                "trial_databases_dropped": len(evidence_rows),
                "trial_databases_expected": len(trial_rows),
                "all_trial_databases_dropped": len(evidence_rows) == len(trial_rows),
                "per_trial_cleanup_refs": [row["trial_relative_path"] for row in evidence_rows],
            },
        })
        _write_json(output_root / "run_metadata.json", metadata)
        artifact_manifest = _write_artifact_manifest(output_root)
        metadata["artifact_manifest"] = artifact_manifest
        _write_json(output_root / "run_metadata.json", metadata)
        if mode == "full":
            return int(metadata.get("evaluator_exit_code") or 0)
        return 0
    except BaseException as exc:
        metadata.update({
            "status": "aborted",
            "finished_at_utc": _utcnow(),
            "abort_reason": _safe_error(exc),
            "trials_completed": len(trial_rows),
            "trial_order": run_order,
            "evaluator_ran": False,
            "evaluator_exit_code": None,
            "partial_rows_are_not_acceptance_data": True,
        })
        if trial_rows:
            _write_jsonl(output_root / "partial_trials.jsonl", trial_rows)
            _write_jsonl(output_root / "partial_provider_domain_oracle.jsonl", oracle_rows)
            _write_jsonl(output_root / "trial_evidence.jsonl", evidence_rows)
        _write_json(output_root / "run_metadata.json", metadata)
        _write_artifact_manifest(output_root)
        return 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("development", "full"), required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args(argv)
    try:
        return run_phase_d(args.mode, args.output_dir)
    except Exception as exc:
        print(f"phase-d runner error: {_safe_error(exc)}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
