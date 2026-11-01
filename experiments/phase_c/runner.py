"""Local-only Phase C experiment harness.

The harness runs each arm as an unprivileged child process, with a fresh
PostgreSQL database/DBOS history and a separate mock-provider process/ledger
for every arm trial. Only the harness can read provider/fault ledgers. It
refuses to run if GitHub/provider credentials are present in its environment.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import os
import platform
import queue
import secrets
import shutil
import signal
import subprocess
import sys
import threading
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql
from sqlalchemy import (
    Column,
    MetaData,
    String,
    Table,
    create_engine,
    select,
    text,
    update,
)
from sqlalchemy.engine import URL, make_url

from .fault_proxy import read_proxy_ledger
from .provider_service import export_oracle, read_verified_ledger
from .shared import PhaseCBarrier, safe_component

ROOT = Path(__file__).resolve().parents[2]
UID_PROVIDER = 10001
GID_PROVIDER = 10001
UID_PROXY = 10002
GID_PROXY = 10002
UID_ARM = 10003
GID_ARM = 10003
ARM_TIMEOUT_SECONDS = 180.0
BARRIER_TIMEOUT_SECONDS = 45.0
PROVIDER_CLIENT_TIMEOUT_SECONDS = 0.7

FROZEN_HASHES = {
    "manifest_sha256": "c5b71b025ca4ced53e8b76c9aa137f75829f2cdc7ee959539ad66d7cae1ca448",
    "protocol_sha256": "0afb7bc54c9062d8b15dde62ba9a35b09f289267db0011355566f069cd5d9ff8",
    "evaluator_sha256": "8fa4613f1589d48a9b6cb2b0b526d5b707110ea47573af94382b291ed50d77d1",
}
FORBIDDEN_CREDENTIAL_ENV = (
    "GITHUB_TOKEN", "GH_TOKEN", "GH_ENTERPRISE_TOKEN",
    "GITHUB_PERSONAL_ACCESS_TOKEN", "CONTINUITY_GITHUB_TOKEN",
    "CONTINUITY_GITHUB_TOKEN_FILE",
)
FORBIDDEN_ARM_ENV = set(FORBIDDEN_CREDENTIAL_ENV) | {
    "PHASEC_PROVIDER_PROXY_KEY", "PHASEC_PROVIDER_LEDGER_PATH",
    "PHASEC_FAULT_SCHEDULE", "PHASEC_ADMIN_DATABASE_URL",
    "PHASEC_PG_ADMIN_PASSWORD",
}
RUN_FIELDS = {
    "scenario", "seed", "arm", "reported_status", "outcome_evidence",
    "elapsed_ms", "provider_calls", "db_transactions", "recovery_actions",
    "manual_interventions", "operator_seconds", "model_cost_usd", "infra_cost_usd",
}
ORACLE_FIELDS = {
    "scenario", "seed", "arm", "commits", "duplicate_commits",
    "forbidden_commits", "effect_present", "provider_post_count", "oracle_log_ref",
}

source_meta = MetaData()
authoritative_versions = Table(
    "phasec_authoritative_versions",
    source_meta,
    Column("ref", String(256), primary_key=True),
    Column("revision", String(128), nullable=False),
)


class TrialInvalid(RuntimeError):
    """A barrier, source, or cleanup invariant failed; do not fill missing rows."""


class ManagedProcess:
    """Drain a child process's output continuously and retain its real exit code."""

    def __init__(
        self,
        argv: list[str],
        *,
        env: dict[str, str],
        uid: int,
        gid: int,
        cwd: Path = ROOT,
        name: str,
    ) -> None:
        if os.name != "posix":
            raise RuntimeError("Phase C process isolation requires the Linux runner container")
        self.argv = argv
        self.env = env
        self.uid = uid
        self.gid = gid
        self.cwd = cwd
        self.name = name
        self.proc: subprocess.Popen[str] | None = None
        self.lines: list[str] = []
        self.line_queue: queue.Queue[str | None] = queue.Queue()
        self._reader: threading.Thread | None = None
        self._lock = threading.Lock()

    def start(self) -> None:
        self.proc = subprocess.Popen(
            self.argv,
            cwd=self.cwd,
            env=self.env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            user=self.uid,
            group=self.gid,
            extra_groups=[],
            start_new_session=True,
        )
        assert self.proc.stdout is not None

        def read_lines() -> None:
            try:
                for line in self.proc.stdout:  # type: ignore[union-attr]
                    clean = line.rstrip("\r\n")
                    with self._lock:
                        self.lines.append(clean)
                    self.line_queue.put(clean)
            finally:
                self.line_queue.put(None)

        self._reader = threading.Thread(target=read_lines, name=f"read-{self.name}", daemon=True)
        self._reader.start()

    def wait_ready(self, *, role: str, timeout_seconds: float = 15.0) -> dict[str, Any]:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            if self.proc is not None and self.proc.poll() is not None:
                raise TrialInvalid(
                    f"{self.name} exited before readiness with {self.proc.returncode}: {self.output()}"
                )
            try:
                line = self.line_queue.get(timeout=0.1)
            except queue.Empty:
                continue
            if line is None:
                break
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if value.get("ready") is True and value.get("role") == role:
                return value
        raise TrialInvalid(f"{self.name} readiness timeout; output={self.output()}")

    def poll(self) -> int | None:
        return None if self.proc is None else self.proc.poll()

    def wait(self, timeout_seconds: float) -> int:
        if self.proc is None:
            raise RuntimeError("process has not started")
        code = self.proc.wait(timeout=timeout_seconds)
        if self._reader is not None:
            self._reader.join(timeout=2.0)
        return code

    def interrupt(self, timeout_seconds: float = 8.0) -> int | None:
        if self.proc is None:
            return None
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGINT)
        try:
            return self.wait(timeout_seconds)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            return self.wait(5.0)

    def kill(self, sig: int = signal.SIGKILL) -> int | None:
        if self.proc is None:
            return None
        if self.proc.poll() is None:
            self.proc.send_signal(sig)
        return self.wait(10.0)

    def output(self) -> list[str]:
        with self._lock:
            return list(self.lines)

    def result_payload(self) -> dict[str, Any] | None:
        for line in reversed(self.output()):
            if line.startswith("PHASEC_RESULT="):
                return json.loads(line.split("=", 1)[1])
        return None


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _json_dump(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")
    os.chmod(path, 0o600)


def _jsonl_write(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=False))
            handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.chmod(path, 0o600)


def _check_environment() -> None:
    if os.name != "posix" or platform.system().lower() != "linux":
        raise RuntimeError("refusing Phase C run outside the isolated Linux Docker runner")
    present = [name for name in FORBIDDEN_CREDENTIAL_ENV if os.environ.get(name)]
    if present:
        # Never print values or forward them to child processes.
        raise RuntimeError("refusing run because forbidden GitHub credential variables are present")
    if os.environ.get("PHASEC_NETWORK_POLICY") != "isolated_internal_no_egress":
        raise RuntimeError("refusing run without the internal/no-egress Phase C Docker network")
    if not os.environ.get("PHASEC_ADMIN_DATABASE_URL"):
        raise RuntimeError("test-only PostgreSQL admin URL is required")


def _load_frozen_inputs() -> tuple[dict[str, Any], dict[str, str]]:
    manifest_path = ROOT / "experiments" / "phase_c_manifest.json"
    protocol_path = ROOT / "docs" / "phase-c-experiment-protocol.md"
    evaluator_path = ROOT / "scripts" / "evaluate_phase_c.py"
    hashes = {
        "manifest_sha256": _sha256_file(manifest_path),
        "protocol_sha256": _sha256_file(protocol_path),
        "evaluator_sha256": _sha256_file(evaluator_path),
    }
    if hashes != FROZEN_HASHES:
        raise RuntimeError("frozen protocol/manifest/evaluator hash changed; refusing to alter rules")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("schema_version") != 1 or manifest.get("seed_count") != 10:
        raise RuntimeError("manifest does not match the frozen Phase C schema")
    scenarios = manifest.get("scenarios")
    if not isinstance(scenarios, list) or len(scenarios) != 8:
        raise RuntimeError("manifest must contain the frozen eight scenarios")
    return manifest, hashes


def _admin_conn() -> psycopg.Connection:
    url = make_url(os.environ["PHASEC_ADMIN_DATABASE_URL"])
    return psycopg.connect(
        host=url.host,
        port=url.port or 5432,
        dbname=url.database or "postgres",
        user=url.username,
        password=url.password,
        connect_timeout=10,
        autocommit=True,
    )


def _create_database(trial_id: str) -> tuple[str, str, str, str]:
    slug = hashlib.sha256((os.environ.get("PHASEC_CODE_SHA", "") + trial_id).encode()).hexdigest()[:12]
    db_name = "pc_" + slug
    role = "pcu_" + slug
    password = secrets.token_hex(24)
    with _admin_conn() as cx:
        cx.execute(
            sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                sql.Identifier(role), sql.Literal(password),
            )
        )
        cx.execute(
            sql.SQL("CREATE DATABASE {} OWNER {}").format(
                sql.Identifier(db_name), sql.Identifier(role),
            )
        )
    admin = make_url(os.environ["PHASEC_ADMIN_DATABASE_URL"])
    db_url = URL.create(
        "postgresql+psycopg",
        username=role,
        password=password,
        host=admin.host,
        port=admin.port or 5432,
        database=db_name,
    )
    # SQLAlchemy URL.__str__ redacts passwords; this URL is used only as an
    # in-memory/process-environment credential and is never logged.
    return db_name, role, password, db_url.render_as_string(hide_password=False)


def _drop_database(db_name: str, role: str) -> None:
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


def _seed_authoritative_version(db_url: str, ref: str) -> None:
    engine = create_engine(db_url, pool_pre_ping=True)
    try:
        source_meta.create_all(engine)
        with engine.begin() as cx:
            current = cx.execute(select(authoritative_versions.c.revision).where(
                authoritative_versions.c.ref == ref,
            )).scalar_one_or_none()
            if current is None:
                cx.execute(authoritative_versions.insert().values(ref=ref, revision="v1"))
            elif current != "v1":
                raise TrialInvalid("fresh trial database has a non-v1 source revision")
    finally:
        engine.dispose()


def _read_authoritative_version(db_url: str, ref: str) -> str | None:
    engine = create_engine(db_url, pool_pre_ping=True)
    try:
        with engine.connect() as cx:
            return cx.execute(select(authoritative_versions.c.revision).where(
                authoritative_versions.c.ref == ref,
            )).scalar_one_or_none()
    finally:
        engine.dispose()


def _set_authoritative_version(db_url: str, ref: str, revision: str) -> int:
    engine = create_engine(db_url, pool_pre_ping=True)
    try:
        with engine.begin() as cx:
            result = cx.execute(
                update(authoritative_versions)
                .where(authoritative_versions.c.ref == ref)
                .values(revision=revision)
            )
            return int(result.rowcount or 0)
    finally:
        engine.dispose()


def _operation_snapshot(db_url: str, arm: str, operation_id: str) -> dict[str, Any] | None:
    table = "effect_operations" if arm == "continuity" else "phasec_baseline_operations"
    engine = create_engine(db_url, pool_pre_ping=True)
    try:
        with engine.connect() as cx:
            row = cx.execute(text(
                f"SELECT status, dispatch_generation FROM {table} WHERE id = :id"
            ), {"id": operation_id}).mappings().first()
            return None if row is None else {
                "status": str(row["status"]),
                "dispatch_generation": int(row["dispatch_generation"]),
            }
    finally:
        engine.dispose()


def _release(barrier: PhaseCBarrier, stage: str, operation_id: str) -> dict[str, Any]:
    path = barrier.release_path(stage, operation_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "trial_id": barrier.trial_id,
        "stage": stage,
        "operation_id": operation_id,
        "released_by": "phase-c-runner",
        "released_monotonic_ns": time.monotonic_ns(),
        "released_utc": _utcnow(),
    }
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o444)
    try:
        os.write(fd, json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8") + b"\n")
        os.fsync(fd)
    finally:
        os.close(fd)
    os.chmod(path, 0o444)
    return payload


def _wait_event(
    barrier: PhaseCBarrier,
    stage: str,
    operation_id: str,
    process: ManagedProcess,
    *,
    timeout_seconds: float = BARRIER_TIMEOUT_SECONDS,
    optional: bool = False,
) -> dict[str, Any] | None:
    path = barrier.event_path(stage, operation_id)
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if path.is_file():
            try:
                value = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise TrialInvalid(f"corrupt barrier event {stage}: {exc}") from exc
            if value.get("trial_id") != barrier.trial_id or value.get("stage") != stage:
                raise TrialInvalid("barrier event identity mismatch")
            if process.proc is not None and value.get("pid") != process.proc.pid:
                raise TrialInvalid("barrier event PID does not match worker process")
            return value
        if process.poll() is not None:
            if optional:
                return None
            raise TrialInvalid(
                f"arm exited before {stage} barrier (exit {process.poll()}): {process.output()}"
            )
        time.sleep(0.01)
    if optional:
        return None
    raise TrialInvalid(f"missed required barrier {stage} for {operation_id}")


def _make_dirs(root: Path, uid: int, gid: int) -> None:
    root.mkdir(parents=True, exist_ok=True)
    os.chown(root, uid, gid)
    os.chmod(root, 0o700)


def _make_ledger(path: Path, uid: int, gid: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    os.chown(path.parent, uid, gid)
    os.chmod(path.parent, 0o700)
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    os.chown(path, uid, gid)
    os.chmod(path, 0o600)


def _child_home(uid: int, run_id: str, role: str) -> Path:
    path = Path("/tmp/phasec-home") / safe_component(run_id) / role
    path.mkdir(parents=True, exist_ok=True)
    os.chown(path, uid, uid)
    os.chmod(path, 0o700)
    return path


def _arm_workdir(run_id: str, trial_id: str) -> Path:
    path = Path("/tmp/phasec-work") / safe_component(run_id) / safe_component(trial_id)
    path.mkdir(parents=True, exist_ok=True)
    os.chown(path, UID_ARM, GID_ARM)
    os.chmod(path, 0o700)
    return path


def _service_env(uid: int, run_id: str, role: str, extra: dict[str, str]) -> dict[str, str]:
    env = {
        "PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
        "PYTHONPATH": str(ROOT),
        "PYTHONUNBUFFERED": "1",
        "PYTHONDONTWRITEBYTECODE": "1",
        "LANG": "C.UTF-8",
        "HOME": str(_child_home(uid, run_id, role)),
    }
    env.update(extra)
    for name in FORBIDDEN_CREDENTIAL_ENV:
        env.pop(name, None)
    return env


def _copy_evidence(src: Path, dst: Path) -> tuple[str, int]:
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(src, dst)
    os.chmod(dst, 0o600)
    return _sha256_file(dst), dst.stat().st_size


def _read_proxy_summary(path: Path) -> dict[str, Any]:
    rows, head, raw = read_proxy_ledger(path)
    events = [row["event"] for row in rows]
    forwarded_posts = sum(
        event.get("type") == "upstream_request_started"
        and event.get("method") == "POST"
        and event.get("forwarded") is True
        for event in events
    )
    forwarded_gets = sum(
        event.get("type") == "upstream_request_started"
        and event.get("method") == "GET"
        and event.get("forwarded") is True
        for event in events
    )
    upstream_responses = sum(event.get("type") == "upstream_response_received" for event in events)
    faults: dict[str, int] = {}
    for event in events:
        if event.get("type") == "fault_triggered":
            fault = event.get("fault")
            faults[fault] = faults.get(fault, 0) + 1
    return {
        "forwarded_post_count": forwarded_posts,
        "forwarded_get_count": forwarded_gets,
        "upstream_response_count": upstream_responses,
        "fault_trigger_counts": faults,
        "ledger_event_count": len(events),
        "ledger_head_sha256": head,
        "ledger_sha256": hashlib.sha256(raw).hexdigest(),
    }


def _read_provider_evidence_refs(path: Path) -> dict[str, dict[str, Any]]:
    rows, _, _ = read_verified_ledger(path)
    refs: dict[str, dict[str, Any]] = {}
    for row in rows:
        event = row["event"]
        if event.get("type") == "readback":
            trial_id = event.get("trial_id")
            seq = row["seq"]
            value = event.get("value")
            digest = row.get("hash")
            reference = f"mock-readback:{trial_id}:{seq}:value={value}:sha256={digest}"
            refs[reference] = {
                "trial_id": trial_id,
                "resource_ref": event.get("resource_ref"),
                "value": value,
                "effect_present": event.get("effect_present"),
                "seq": seq,
                "event_sha256": digest,
            }
    return refs


def _provider_export(
    *,
    trial_id: str,
    scenario: str,
    expected_effect: bool,
    ledger_path: Path,
    run_id: str,
) -> dict[str, Any]:
    env = _service_env(UID_PROVIDER, run_id, "provider-export", {})
    proc = subprocess.run(
        [
            sys.executable, "-m", "experiments.phase_c.provider_service", "export",
            "--ledger-path", str(ledger_path),
            "--expected-effect", "true" if expected_effect else "false",
            "--scenario", scenario,
        ],
        cwd=ROOT,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=20,
        check=False,
        user=UID_PROVIDER,
        group=GID_PROVIDER,
        extra_groups=[],
    )
    if proc.returncode != 0:
        raise TrialInvalid(f"provider exporter exit {proc.returncode}: {proc.stdout[-1000:]}")
    try:
        value = json.loads(proc.stdout.strip().splitlines()[-1])
    except (IndexError, json.JSONDecodeError) as exc:
        raise TrialInvalid("provider exporter did not return a JSON oracle row") from exc
    if set(value) != ORACLE_FIELDS:
        raise TrialInvalid("provider exporter returned a noncanonical oracle schema")
    return value


def _make_arm_env(
    *,
    db_url: str,
    trial_id: str,
    pair_id: str,
    arm: str,
    proxy_port: int,
    event_dir: Path,
    release_dir: Path,
    run_id: str,
) -> dict[str, str]:
    return _service_env(
        UID_ARM,
        run_id,
        "arm-" + arm,
        {
            "PHASEC_DATABASE_URL": db_url,
            "DBOS_SYSTEM_DATABASE_URL": db_url,
            "PHASEC_PROVIDER_URL": f"http://127.0.0.1:{proxy_port}",
            "PHASEC_TRIAL_ID": trial_id,
            "PHASEC_PAIR_ID": pair_id,
            "PHASEC_ARM": arm,
            "PHASEC_BARRIER_EVENT_DIR": str(event_dir),
            "PHASEC_BARRIER_RELEASE_DIR": str(release_dir),
            "PHASEC_APPLICATION_VERSION": os.environ.get("PHASEC_CODE_SHA", "phase-c-dev")[:64],
        },
    )


def _worker_argv(arm: str, operation_ids: list[str], actors: list[str], *, concurrent: bool) -> list[str]:
    module = "experiments.phase_c.continuity_worker" if arm == "continuity" else "experiments.phase_c.baseline_worker"
    argv = [sys.executable, "-m", module]
    for operation_id, actor in zip(operation_ids, actors, strict=True):
        argv.extend(["--operation-id", operation_id, "--actor", actor])
    if concurrent:
        argv.extend(["--concurrent", "--pre-submit-barrier"])
    return argv


def _control_revoke(arm: str, db_url: str, grant_id: str, version_ref: str) -> dict[str, Any]:
    if arm == "continuity":
        from continuity.db import RuntimeDB
        from continuity.runtime import ContinuityRuntime

        db = RuntimeDB(db_url, create_schema=False)
        try:
            row = ContinuityRuntime(db).revoke_grant(grant_id)
            return {"revoked": bool(row["revoked"]), "revision": int(row["revision"])}
        finally:
            db.close()
    from .baseline_runtime import BaselineRuntime

    reader_engine = create_engine(db_url, pool_pre_ping=True)

    def reader() -> str | None:
        with reader_engine.connect() as cx:
            return cx.execute(select(authoritative_versions.c.revision).where(
                authoritative_versions.c.ref == version_ref,
            )).scalar_one_or_none()

    runtime = BaselineRuntime(
        db_url, adapters={}, verifiers={},
        version_readers={version_ref: reader}, create_schema=False,
    )
    try:
        row = runtime.revoke_grant(grant_id)
        return {"revoked": bool(row["revoked"]), "revision": int(row["revision"])}
    finally:
        runtime.close()
        reader_engine.dispose()


def _write_event(path: Path, event: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(event, sort_keys=True, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.chmod(path, 0o600)


def _isolation_probe(run_id: str, provider_root: Path, proxy_root: Path) -> dict[str, Any]:
    """Prove an arm UID cannot read either ledger or bypass provider auth.

    The disposable probe uses its own non-trial ledger and is removed after
    inspection; it never contributes rows to either evaluator input.
    """
    trial_id = "isolation-probe-" + secrets.token_hex(4)
    provider_dir = provider_root / run_id / trial_id
    proxy_dir = proxy_root / run_id / trial_id
    _make_dirs(provider_dir, UID_PROVIDER, GID_PROVIDER)
    _make_dirs(proxy_dir, UID_PROXY, GID_PROXY)
    provider_ledger = provider_dir / "provider.jsonl"
    proxy_ledger = proxy_dir / "proxy.jsonl"
    _make_ledger(provider_ledger, UID_PROVIDER, GID_PROVIDER)
    _make_ledger(proxy_ledger, UID_PROXY, GID_PROXY)
    key = secrets.token_hex(32)
    provider_env = _service_env(
        UID_PROVIDER, run_id, "isolation-provider",
        {"PHASEC_PROVIDER_PROXY_KEY": key},
    )
    provider = ManagedProcess(
        [
            sys.executable, "-m", "experiments.phase_c.provider_service", "serve",
            "--trial-id", trial_id, "--arm", "continuity", "--seed", "0",
            "--scenario", "clean", "--ledger-path", str(provider_ledger), "--port", "0",
        ], env=provider_env, uid=UID_PROVIDER, gid=GID_PROVIDER, name="isolation-provider",
    )
    proxy = None
    cleanup_codes: dict[str, int] = {}
    try:
        provider.start()
        ready = provider.wait_ready(role="mock_provider")
        provider_port = int(ready["port"])
        proxy_env = _service_env(
            UID_PROXY, run_id, "isolation-proxy",
            {"PHASEC_PROVIDER_PROXY_KEY": key},
        )
        proxy = ManagedProcess(
            [
                sys.executable, "-m", "experiments.phase_c.fault_proxy", "serve",
                "--trial-id", trial_id, "--arm", "continuity", "--seed", "0",
                "--scenario", "clean", "--proxy-log-path", str(proxy_ledger),
                "--provider-url", f"http://127.0.0.1:{provider_port}", "--port", "0",
                "--client-timeout-seconds", str(PROVIDER_CLIENT_TIMEOUT_SECONDS),
            ], env=proxy_env, uid=UID_PROXY, gid=GID_PROXY, name="isolation-proxy",
        )
        proxy.start()
        proxy_ready = proxy.wait_ready(role="fault_proxy")
        # The actual arm child gets neither ledger path nor proxy key. Probe OS
        # permissions from the arm UID directly; expected result is EACCES.
        probe_env = _service_env(UID_ARM, run_id, "isolation-arm", {})
        read_probe = subprocess.run(
            [sys.executable, "-c", "import pathlib,sys; pathlib.Path(sys.argv[1]).read_bytes()", str(provider_ledger)],
            cwd=ROOT, env=probe_env, stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            timeout=10, check=False, user=UID_ARM, group=GID_ARM, extra_groups=[],
        )
        proxy_read_probe = subprocess.run(
            [sys.executable, "-c", "import pathlib,sys; pathlib.Path(sys.argv[1]).read_bytes()", str(proxy_ledger)],
            cwd=ROOT, env=probe_env, stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            timeout=10, check=False, user=UID_ARM, group=GID_ARM, extra_groups=[],
        )
        provider_env_probe = subprocess.run(
            [sys.executable, "-c", "import pathlib,sys; pathlib.Path('/proc/'+sys.argv[1]+'/environ').read_bytes()", str(provider.proc.pid)],
            cwd=ROOT, env=probe_env, stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            timeout=10, check=False, user=UID_ARM, group=GID_ARM, extra_groups=[],
        )
        proxy_env_probe = subprocess.run(
            [sys.executable, "-c", "import pathlib,sys; pathlib.Path('/proc/'+sys.argv[1]+'/environ').read_bytes()", str(proxy.proc.pid)],
            cwd=ROOT, env=probe_env, stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            timeout=10, check=False, user=UID_ARM, group=GID_ARM, extra_groups=[],
        )
        if (
            read_probe.returncode == 0
            or proxy_read_probe.returncode == 0
            or provider_env_probe.returncode == 0
            or proxy_env_probe.returncode == 0
        ):
            raise TrialInvalid("arm UID can read an independent ledger")
        # Test direct provider access without its internal key; both requests
        # must be rejected and no effect can be committed. This is an isolated
        # sacrificial ledger, not one of the 160 experiment trials.
        direct_probe_code = (
            "import json,urllib.request,urllib.error,sys; "
            "base=sys.argv[1]; "
            "req=urllib.request.Request(base+'/v1/effects?resource_ref=probe',headers={'X-PhaseC-Trial-ID':sys.argv[2],'X-PhaseC-Arm':'continuity','X-PhaseC-Scenario':'clean'}); "
            "out=[]; "
            "\ntry: urllib.request.urlopen(req,timeout=2); out.append(200) "
            "\nexcept urllib.error.HTTPError as e: out.append(e.code) "
            "\nexcept Exception: out.append(-1) "
            "\nbody=json.dumps({'trial_id':sys.argv[2],'operation_id':'probe-op','principal':'p','provider':'phasec-mock','resource_ref':'probe','action':'increment','payload':{'delta':1},'effect_key':'probe-key'}).encode() "
            "\nreq=urllib.request.Request(base+'/v1/effects',data=body,method='POST',headers={'Content-Type':'application/json','X-PhaseC-Trial-ID':sys.argv[2],'X-PhaseC-Arm':'continuity','X-PhaseC-Scenario':'clean'}) "
            "\ntry: urllib.request.urlopen(req,timeout=2); out.append(200) "
            "\nexcept urllib.error.HTTPError as e: out.append(e.code) "
            "\nexcept Exception: out.append(-1) "
            "\nprint(json.dumps(out))"
        )
        probe = subprocess.run(
            [sys.executable, "-c", direct_probe_code, f"http://127.0.0.1:{provider_port}", trial_id],
            cwd=ROOT, env=probe_env, stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            timeout=10, check=False, user=UID_ARM, group=GID_ARM, extra_groups=[],
        )
        if probe.returncode != 0:
            raise TrialInvalid("direct provider probe failed unexpectedly")
        statuses = json.loads(probe.stdout.strip().splitlines()[-1])
        if statuses != [401, 401]:
            raise TrialInvalid("arm UID bypassed provider proxy authentication")
        # Let proxy/Provider logs flush, then verify there were no commits.
        proxy.interrupt()
        provider.interrupt()
        cleanup_codes["proxy"] = int(proxy.proc.returncode if proxy.proc else -999)
        cleanup_codes["provider"] = int(provider.proc.returncode if provider.proc else -999)
        oracle = export_oracle(provider_ledger, expected_effect=False, scenario="clean")
        if oracle["commits"] != 0 or oracle["effect_present"] is not False:
            raise TrialInvalid("isolation probe caused a mock-provider effect")
        return {
            "arm_uid": UID_ARM,
            "provider_uid": UID_PROVIDER,
            "proxy_uid": UID_PROXY,
            "provider_ledger_read_denied": read_probe.returncode != 0,
            "proxy_ledger_read_denied": proxy_read_probe.returncode != 0,
            "provider_process_environment_read_denied": provider_env_probe.returncode != 0,
            "proxy_process_environment_read_denied": proxy_env_probe.returncode != 0,
            "direct_provider_get_status": statuses[0],
            "direct_provider_post_status": statuses[1],
            "provider_commits_from_probe": oracle["commits"],
            "provider_oracle_hash": oracle["oracle_log_ref"],
            "provider_ready_port_internal_only": provider_port,
            "proxy_ready_port_loopback": int(proxy_ready["port"]),
            "child_environment_contains_github_credentials": False,
            "cleanup_exit_codes": cleanup_codes,
            "probe_artifacts_removed": True,
        }
    finally:
        for process in (proxy, provider):
            if process is not None and process.poll() is None:
                try:
                    process.interrupt()
                except Exception:
                    process.kill()
        shutil.rmtree(provider_dir, ignore_errors=True)
        shutil.rmtree(proxy_dir, ignore_errors=True)


def _control_mutation(
    *, arm: str, scenario: str, db_url: str, trial_id: str, grant_id: str, version_ref: str,
) -> dict[str, Any] | None:
    if scenario == "revoke_before_dispatch":
        return {
            "kind": "grant_revoked",
            "result": _control_revoke(arm, db_url, grant_id, version_ref),
            "at_utc": _utcnow(),
        }
    if scenario == "stale_version_before_dispatch":
        changed = _set_authoritative_version(db_url, version_ref, "v2")
        if changed != 1:
            raise TrialInvalid("authoritative version mutation did not update exactly one row")
        return {"kind": "authoritative_version_changed", "from": "v1", "to": "v2", "at_utc": _utcnow()}
    return None


def _count_provider_evidence_ref(reference: str, readback_refs: dict[str, dict[str, Any]]) -> bool:
    if not reference.startswith("goal-verifier:"):
        return False
    return reference.removeprefix("goal-verifier:") in readback_refs


def _run_trial(
    *,
    manifest_case: dict[str, Any],
    seed: int,
    arm: str,
    run_id: str,
    output_root: Path,
    provider_root: Path,
    proxy_root: Path,
    barrier_root: Path,
    process_exit_records: list[dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    scenario = manifest_case["id"]
    expected_effect = manifest_case["expected_effect"]
    pair_id = "pair-" + hashlib.sha256(f"{scenario}|{seed}".encode()).hexdigest()[:16]
    trial_id = "trial-" + hashlib.sha256(f"{scenario}|{seed}|{arm}".encode()).hexdigest()[:16]
    op_ids = ["op:" + pair_id + ":actor-a"]
    actors = ["phasec-actor-a"]
    concurrent = scenario == "concurrent_duplicate_intent"
    if concurrent:
        op_ids.append("op:" + pair_id + ":actor-b")
        actors.append("phasec-actor-b")
    grant_id = "grant:" + pair_id
    version_ref = "source:" + pair_id

    db_name, db_role, db_password, db_url = _create_database(trial_id)
    provider_dir = provider_root / run_id / trial_id
    proxy_dir = proxy_root / run_id / trial_id
    _make_dirs(provider_dir, UID_PROVIDER, GID_PROVIDER)
    _make_dirs(proxy_dir, UID_PROXY, GID_PROXY)
    provider_ledger = provider_dir / "provider.jsonl"
    proxy_ledger = proxy_dir / "proxy.jsonl"
    _make_ledger(provider_ledger, UID_PROVIDER, GID_PROVIDER)
    _make_ledger(proxy_ledger, UID_PROXY, GID_PROXY)
    barrier_run_dir = barrier_root / run_id
    barrier_trial_dir = barrier_run_dir / trial_id
    barrier_run_dir.mkdir(parents=True, exist_ok=True)
    barrier_trial_dir.mkdir(parents=True, exist_ok=True)
    os.chown(barrier_run_dir, 0, 0)
    os.chmod(barrier_run_dir, 0o711)
    os.chown(barrier_trial_dir, 0, 0)
    os.chmod(barrier_trial_dir, 0o711)
    event_dir = barrier_trial_dir / "events"
    release_dir = barrier_trial_dir / "releases"
    event_dir.mkdir(mode=0o733, exist_ok=True)
    release_dir.mkdir(mode=0o755, exist_ok=True)
    os.chown(event_dir, UID_ARM, GID_ARM)
    os.chmod(event_dir, 0o733)
    os.chmod(release_dir, 0o755)
    barrier = PhaseCBarrier(
        trial_id=trial_id,
        event_dir=event_dir,
        release_dir=release_dir,
        timeout_seconds=BARRIER_TIMEOUT_SECONDS,
    )

    provider_key = secrets.token_hex(32)
    provider_env = _service_env(
        UID_PROVIDER, run_id, "provider-" + trial_id,
        {"PHASEC_PROVIDER_PROXY_KEY": provider_key},
    )
    provider_process = ManagedProcess(
        [
            sys.executable, "-m", "experiments.phase_c.provider_service", "serve",
            "--trial-id", trial_id, "--arm", arm, "--seed", str(seed),
            "--scenario", scenario, "--ledger-path", str(provider_ledger), "--port", "0",
        ], env=provider_env, uid=UID_PROVIDER, gid=GID_PROVIDER,
        name="provider-" + trial_id,
    )
    proxy_process: ManagedProcess | None = None
    worker_process: ManagedProcess | None = None
    worker_first_exit: int | None = None
    worker_recovery_exit: int | None = None
    worker_is_recovery = False
    worker_payload: dict[str, Any] | None = None
    recovery_payload: dict[str, Any] | None = None
    start_ns: int | None = None
    finish_ns: int | None = None
    barrier_events: dict[str, Any] = {}
    releases: list[dict[str, Any]] = []
    control_event: dict[str, Any] | None = None
    db_stats_before: dict[str, int] = {}
    db_stats_after: dict[str, int] = {}
    cleanup: dict[str, Any] = {"database_dropped": False}
    arm_workdir: Path | None = None
    proxy_summary: dict[str, Any] = {}
    provider_oracle: dict[str, Any] | None = None
    provider_copy = output_root / "provider_ledgers" / f"{trial_id}.jsonl"
    proxy_copy = output_root / "fault_proxy_logs" / f"{trial_id}.jsonl"
    trial_evidence: dict[str, Any] = {
        "scenario": scenario,
        "seed": seed,
        "arm": arm,
        "trial_id": trial_id,
        "paired_input_id": pair_id,
        "operation_ids": op_ids,
        "database_name": db_name,
        "database_role_scope": "one fresh owner role for this single-trial database; credentials omitted",
        "provider_ledger_relpath": f"provider_ledgers/{trial_id}.jsonl",
        "fault_proxy_relpath": f"fault_proxy_logs/{trial_id}.jsonl",
        "barriers": barrier_events,
        "releases": releases,
        "control_event": None,
        "process_exit_codes": {},
        "cleanup": cleanup,
    }
    try:
        arm_workdir = _arm_workdir(run_id, trial_id)
        _seed_authoritative_version(db_url, version_ref)
        db_stats_before = _pg_stats(db_name)
        provider_process.start()
        provider_ready = provider_process.wait_ready(role="mock_provider")
        provider_port = int(provider_ready["port"])
        proxy_env = _service_env(
            UID_PROXY, run_id, "proxy-" + trial_id,
            {"PHASEC_PROVIDER_PROXY_KEY": provider_key},
        )
        proxy_process = ManagedProcess(
            [
                sys.executable, "-m", "experiments.phase_c.fault_proxy", "serve",
                "--trial-id", trial_id, "--arm", arm, "--seed", str(seed),
                "--scenario", scenario, "--proxy-log-path", str(proxy_ledger),
                "--provider-url", f"http://127.0.0.1:{provider_port}", "--port", "0",
                "--client-timeout-seconds", str(PROVIDER_CLIENT_TIMEOUT_SECONDS),
            ], env=proxy_env, uid=UID_PROXY, gid=GID_PROXY,
            name="proxy-" + trial_id,
        )
        proxy_process.start()
        proxy_ready = proxy_process.wait_ready(role="fault_proxy")
        proxy_port = int(proxy_ready["port"])

        arm_env = _make_arm_env(
            db_url=db_url,
            trial_id=trial_id,
            pair_id=pair_id,
            arm=arm,
            proxy_port=proxy_port,
            event_dir=event_dir,
            release_dir=release_dir,
            run_id=run_id,
        )
        arm_env_names = set(arm_env)
        if arm_env_names & FORBIDDEN_ARM_ENV:
            raise TrialInvalid("arm process would receive a forbidden credential/control variable")

        worker_process = ManagedProcess(
            _worker_argv(arm, op_ids, actors, concurrent=concurrent),
            env=arm_env,
            uid=UID_ARM,
            gid=GID_ARM,
            cwd=arm_workdir,
            name="worker-" + trial_id,
        )
        start_ns = time.perf_counter_ns()
        worker_process.start()

        if concurrent:
            pre_events = []
            for op_id in op_ids:
                event = _wait_event(barrier, "pre_submit", op_id, worker_process)
                assert event is not None
                pre_events.append(event)
            barrier_events["pre_submit"] = pre_events
            for op_id in op_ids:
                releases.append(_release(barrier, "pre_submit", op_id))
            # Each actor records readiness and then waits on one shared gate.
            # The controller releases a single marker only after both actors
            # are waiting; a symmetric 100ms test-only hold after the gate
            # makes the overlapping submit-call intervals robust to scheduling.
            barrier_events["submit_ready"] = [
                _wait_event(barrier, "submit_ready", op_id, worker_process)
                for op_id in op_ids
            ]
            shared_gate = _wait_event(barrier, "submit_go", pair_id, worker_process)
            barrier_events["submit_go"] = shared_gate
            if shared_gate is None:
                raise TrialInvalid("shared concurrent-submit gate was not observed")
            gate_release = _release(barrier, "submit_go", pair_id)
            releases.append(gate_release)
            barrier_events["submit_started"] = [
                _wait_event(barrier, "submit_started", op_id, worker_process)
                for op_id in op_ids
            ]
            barrier_events["submit_finished"] = [
                _wait_event(barrier, "submit_finished", op_id, worker_process)
                for op_id in op_ids
            ]
            started = {event["operation_id"]: event["announced_monotonic_ns"]
                       for event in barrier_events["submit_started"] if event}
            finished = {event["operation_id"]: event["announced_monotonic_ns"]
                        for event in barrier_events["submit_finished"] if event}
            if len(started) != 2 or len(finished) != 2:
                raise TrialInvalid("concurrent submit instrumentation incomplete")
            a, b = op_ids
            overlap = started[a] < finished[b] and started[b] < finished[a]
            barrier_events["concurrent_submit_overlap"] = {
                "overlap": overlap,
                "shared_gate_release_monotonic_ns": gate_release["released_monotonic_ns"],
                "symmetric_test_hold_ms": 100,
                "intervals_monotonic_ns": {
                    op_id: {"start": started[op_id], "finish": finished[op_id]}
                    for op_id in op_ids
                },
            }
            if not overlap:
                raise TrialInvalid("submitter intervals did not overlap")
            # Exactly one logical intent should survive the independent unique
            # constraint; the loser is a genuine concurrent conflict.
            snapshots = {op_id: _operation_snapshot(db_url, arm, op_id) for op_id in op_ids}
            registered = [op_id for op_id, snapshot in snapshots.items() if snapshot is not None]
            if len(registered) != 1:
                raise TrialInvalid(f"concurrent unique-effect reservation count is {len(registered)}")
            winner = registered[0]
            winner_snapshot = snapshots[winner]
            if winner_snapshot != {"status": "registered", "dispatch_generation": 0}:
                raise TrialInvalid("concurrent winner was not registered before dispatch")
            barrier_events["registered_snapshot"] = {"winner": winner, "rows": snapshots}
            releases.append(_release(barrier, "registered", winner))
            barrier_events["registered"] = _wait_event(barrier, "registered", winner, worker_process)
            if barrier_events["registered"] is None:
                raise TrialInvalid("winner registration barrier was not observed")
        else:
            op_id = op_ids[0]
            registered_event = _wait_event(barrier, "registered", op_id, worker_process)
            barrier_events["registered"] = registered_event
            snapshot = _operation_snapshot(db_url, arm, op_id)
            if snapshot != {"status": "registered", "dispatch_generation": 0}:
                raise TrialInvalid(f"registration barrier did not follow durable registration: {snapshot}")
            barrier_events["registered_snapshot"] = snapshot
            control_event = _control_mutation(
                arm=arm,
                scenario=scenario,
                db_url=db_url,
                trial_id=trial_id,
                grant_id=grant_id,
                version_ref=version_ref,
            )
            trial_evidence["control_event"] = control_event
            releases.append(_release(barrier, "registered", op_id))

        blocked_case = scenario in {"revoke_before_dispatch", "stale_version_before_dispatch"}
        if blocked_case:
            # The worker must return a block before a reservation or POST.
            try:
                worker_first_exit = worker_process.wait(ARM_TIMEOUT_SECONDS)
            except subprocess.TimeoutExpired as exc:
                worker_process.kill()
                raise TrialInvalid("blocked precondition trial did not terminate") from exc
            worker_payload = worker_process.result_payload()
            if worker_first_exit != 0 or worker_payload is None:
                raise TrialInvalid(f"blocked arm exit/result invalid: {worker_first_exit}")
            reserved = barrier.event_path("reserved_before_send", op_ids[0])
            if reserved.exists():
                raise TrialInvalid("blocked scenario reached provider reservation")
            final_snapshot = _operation_snapshot(db_url, arm, op_ids[0])
            expected_generation = 0
            if not final_snapshot or final_snapshot.get("dispatch_generation") != expected_generation:
                raise TrialInvalid("blocked scenario unexpectedly created a dispatch reservation")
            barrier_events["final_blocked_snapshot"] = final_snapshot
        else:
            send_op_id = op_ids[0]
            if concurrent:
                send_op_id = str(barrier_events["registered_snapshot"]["winner"])
            reservation_event = _wait_event(
                barrier, "reserved_before_send", send_op_id, worker_process,
            )
            barrier_events["reserved_before_send"] = reservation_event
            reservation_snapshot = _operation_snapshot(db_url, arm, send_op_id)
            barrier_events["reservation_snapshot"] = reservation_snapshot
            if reservation_snapshot != {"status": "may_have_been_sent", "dispatch_generation": 1}:
                raise TrialInvalid(f"reservation barrier was not durable before send: {reservation_snapshot}")
            if scenario == "crash_after_reservation_before_send":
                # The adapter is paused at a harness barrier after the PG
                # reservation commit and before the first proxy request.
                worker_first_exit = worker_process.kill(signal.SIGKILL)
                barrier_events["crash_injection"] = {
                    "point": "after_committed_may_have_been_sent_before_provider_POST",
                    "pid": worker_process.proc.pid if worker_process.proc else None,
                    "exit_code": worker_first_exit,
                    "at_utc": _utcnow(),
                }
                recovery_events = barrier_trial_dir / "recovery-events"
                recovery_events.mkdir(mode=0o733, exist_ok=True)
                os.chown(recovery_events, UID_ARM, GID_ARM)
                os.chmod(recovery_events, 0o733)
                recovery_env = _make_arm_env(
                    db_url=db_url,
                    trial_id=trial_id,
                    pair_id=pair_id,
                    arm=arm,
                    proxy_port=proxy_port,
                    event_dir=recovery_events,
                    release_dir=release_dir,
                    run_id=run_id,
                )
                worker_process = ManagedProcess(
                    _worker_argv(arm, [op_ids[0]], [actors[0]], concurrent=False),
                    env=recovery_env,
                    uid=UID_ARM,
                    gid=GID_ARM,
                    cwd=arm_workdir,
                    name="recovery-" + trial_id,
                )
                worker_is_recovery = True
                recovery_started = time.perf_counter_ns()
                worker_process.start()
                recovery_send_event = _wait_event(
                    PhaseCBarrier(trial_id=trial_id, event_dir=recovery_events,
                                  release_dir=release_dir, timeout_seconds=2.0),
                    "reserved_before_send", op_ids[0], worker_process,
                    timeout_seconds=3.0, optional=True,
                )
                if recovery_send_event is not None:
                    worker_process.kill()
                    raise TrialInvalid("recovery attempted a second provider dispatch")
                try:
                    worker_recovery_exit = worker_process.wait(ARM_TIMEOUT_SECONDS)
                except subprocess.TimeoutExpired as exc:
                    worker_process.kill()
                    raise TrialInvalid("DBOS recovery did not finish after crash injection") from exc
                recovery_payload = worker_process.result_payload()
                finish_ns = time.perf_counter_ns()
                if worker_recovery_exit != 0 or recovery_payload is None:
                    raise TrialInvalid("recovery worker failed to return a result")
                worker_payload = recovery_payload
                trial_evidence["recovery"] = {
                    "workflow_reinvoked": True,
                    "recovery_worker_exit_code": worker_recovery_exit,
                    "recovery_worker_elapsed_ms": round((finish_ns - recovery_started) / 1e6, 3),
                    "no_second_reservation_barrier": True,
                    "final_snapshot": _operation_snapshot(db_url, arm, op_ids[0]),
                }
            else:
                releases.append(_release(barrier, "reserved_before_send", send_op_id))
                try:
                    worker_first_exit = worker_process.wait(ARM_TIMEOUT_SECONDS)
                except subprocess.TimeoutExpired as exc:
                    worker_process.kill()
                    raise TrialInvalid("arm worker timed out after provider dispatch") from exc
                worker_payload = worker_process.result_payload()
                finish_ns = time.perf_counter_ns()
                if worker_first_exit != 0 or worker_payload is None:
                    raise TrialInvalid(f"arm worker exit/result invalid: {worker_first_exit}")

        if finish_ns is None:
            finish_ns = time.perf_counter_ns()
        if start_ns is None:
            raise RuntimeError("trial timer not started")
        elapsed_ms = round((finish_ns - start_ns) / 1e6, 3)

        # Capture the arm's reported outcome separately from the provider
        # ledger. Only the independent provider exporter supplies oracle truth.
        reported = (worker_payload or {}).get("reported_status")
        if reported not in {"satisfied", "blocked", "unknown", "unverified"}:
            raise TrialInvalid("arm did not return an allowed reported_status")
        evidence_refs = (worker_payload or {}).get("outcome_evidence_refs", [])
        if not isinstance(evidence_refs, list) or any(not isinstance(x, str) for x in evidence_refs):
            raise TrialInvalid("arm outcome evidence refs have invalid shape")
        claimed_outcome_evidence = (worker_payload or {}).get("outcome_evidence")
        if type(claimed_outcome_evidence) is not bool or claimed_outcome_evidence != bool(evidence_refs):
            raise TrialInvalid("arm outcome_evidence boolean does not match its verifier references")

        # Stop local services cleanly so the independent provider ledger is
        # flushed before export. Real exit codes are captured.
        if proxy_process is None:
            raise TrialInvalid("fault proxy was not created")
        proxy_exit = proxy_process.interrupt()
        provider_exit = provider_process.interrupt()
        trial_evidence["process_exit_codes"].update({
            "arm_first": worker_first_exit,
            "arm_recovery": worker_recovery_exit,
            "fault_proxy": proxy_exit,
            "mock_provider": provider_exit,
        })
        process_exit_records.extend([
            {"trial_id": trial_id, "component": "arm_first", "exit_code": worker_first_exit},
            {"trial_id": trial_id, "component": "arm_recovery", "exit_code": worker_recovery_exit},
            {"trial_id": trial_id, "component": "fault_proxy", "exit_code": proxy_exit},
            {"trial_id": trial_id, "component": "mock_provider", "exit_code": provider_exit},
        ])
        if proxy_exit != 0 or provider_exit != 0:
            raise TrialInvalid("provider/proxy did not exit cleanly after SIGINT")

        provider_oracle = _provider_export(
            trial_id=trial_id,
            scenario=scenario,
            expected_effect=expected_effect,
            ledger_path=provider_ledger,
            run_id=run_id,
        )
        proxy_summary = _read_proxy_summary(proxy_ledger)
        if proxy_summary["forwarded_post_count"] != provider_oracle["provider_post_count"]:
            raise TrialInvalid("fault-proxy forwarded POST count disagrees with provider-owned ledger")
        if provider_oracle["provider_post_count"] != (worker_payload or {}).get("attempted_provider_posts", -1):
            # A timeout-before-send is an intentional proxy suppression; arm
            # attempted count is separately retained and may exceed upstream
            # provider calls by exactly the suppressed count.
            suppressed = sum(
                event.get("type") == "fault_triggered"
                and event.get("fault") == "timeout_before_send"
                for event in [row["event"] for row in read_proxy_ledger(proxy_ledger)[0]]
            )
            attempted = (worker_payload or {}).get("attempted_provider_posts", -1)
            if attempted != provider_oracle["provider_post_count"] + suppressed:
                raise TrialInvalid("arm POST attempt telemetry and independent proxy/provider counts disagree")

        # Expected fault must actually be observed at its external barrier.
        faults = proxy_summary["fault_trigger_counts"]
        if scenario == "ack_lost_after_commit" and faults.get("ack_lost_after_commit") != 1:
            raise TrialInvalid("ACK-loss barrier was not observed exactly once")
        if scenario == "timeout_before_send" and faults.get("timeout_before_send") != 1:
            raise TrialInvalid("pre-send timeout injector was not observed exactly once")
        if scenario == "readback_unavailable_after_commit" and faults.get("readback_unavailable_after_commit", 0) < 1:
            raise TrialInvalid("readback outage barrier was not observed")
        if scenario in {"clean", "concurrent_duplicate_intent", "revoke_before_dispatch", "stale_version_before_dispatch"} and faults:
            raise TrialInvalid("unexpected fault-proxy injection occurred")
        if scenario == "crash_after_reservation_before_send" and provider_oracle["provider_post_count"] != 0:
            raise TrialInvalid("crash-before-send trial unexpectedly reached provider")

        readback_refs = _read_provider_evidence_refs(provider_ledger)
        if reported == "satisfied":
            matched_refs = [
                ref.removeprefix("goal-verifier:")
                for ref in evidence_refs
                if _count_provider_evidence_ref(ref, readback_refs)
            ]
            if not matched_refs:
                raise TrialInvalid("satisfied outcome lacks an independently reviewable provider verifier reference")
            if not any(
                readback_refs[ref].get("trial_id") == trial_id
                and readback_refs[ref].get("resource_ref") == "counter:" + pair_id
                and readback_refs[ref].get("value") == 1
                and readback_refs[ref].get("effect_present") is True
                for ref in matched_refs
            ):
                raise TrialInvalid("goal verifier reference does not prove the paired resource's value=1")
        valid_outcome_evidence = any(
            _count_provider_evidence_ref(ref, readback_refs)
            for ref in evidence_refs
        )
        if claimed_outcome_evidence and not valid_outcome_evidence:
            raise TrialInvalid("arm claimed outcome evidence without a provider-owned readback event")

        db_stats_after = _pg_stats(db_name)
        db_transactions = max(0, db_stats_after["total"] - db_stats_before["total"])
        trial_evidence["db_transactions"] = {
            "measurement": "pg_stat_database.xact_commit + xact_rollback delta in the isolated trial DB",
            "before": db_stats_before,
            "after": db_stats_after,
            "controller_barrier_reads_included": True,
            "value": db_transactions,
        }
        trial_evidence["timing"] = {
            "elapsed_ms": elapsed_ms,
            "measurement": "runner monotonic_ns from first arm worker start through terminal/recovery result",
        }
        trial_evidence["arm_report"] = worker_payload
        trial_evidence["provider_oracle"] = provider_oracle
        trial_evidence["fault_proxy_summary"] = proxy_summary
        trial_evidence["outcome_evidence_refs"] = evidence_refs
        trial_evidence["provider_readback_evidence"] = list(readback_refs.values())

        provider_digest, provider_bytes = _copy_evidence(provider_ledger, provider_copy)
        proxy_digest, proxy_bytes = _copy_evidence(proxy_ledger, proxy_copy)
        if provider_digest not in provider_oracle["oracle_log_ref"]:
            raise TrialInvalid("provider exporter oracle ref is not bound to copied ledger digest")
        trial_evidence["provider_ledger"] = {
            "relpath": f"provider_ledgers/{trial_id}.jsonl",
            "sha256": provider_digest,
            "bytes": provider_bytes,
            "events": len(read_verified_ledger(provider_copy)[0]),
        }
        trial_evidence["fault_proxy_ledger"] = {
            "relpath": f"fault_proxy_logs/{trial_id}.jsonl",
            "sha256": proxy_digest,
            "bytes": proxy_bytes,
            "events": proxy_summary["ledger_event_count"],
        }

        report_row = {
            "scenario": scenario,
            "seed": seed,
            "arm": arm,
            "reported_status": reported,
            "outcome_evidence": valid_outcome_evidence,
            "elapsed_ms": elapsed_ms,
            # This is independently measured upstream-forwarded provider POST
            # count from the fault proxy and cross-checked against Provider.
            "provider_calls": proxy_summary["forwarded_post_count"],
            "db_transactions": db_transactions,
            "recovery_actions": int((worker_payload or {}).get("recovery_actions", 0))
            + (1 if scenario == "crash_after_reservation_before_send" else 0),
            "manual_interventions": int((worker_payload or {}).get("manual_interventions", 0)),
            "operator_seconds": (worker_payload or {}).get("operator_seconds"),
            "model_cost_usd": (worker_payload or {}).get("model_cost_usd"),
            "infra_cost_usd": None,
        }
        if set(report_row) != RUN_FIELDS:
            raise TrialInvalid("runner generated a noncanonical trials row")
        if set(provider_oracle) != ORACLE_FIELDS:
            raise TrialInvalid("provider exporter generated a noncanonical oracle row")
        if report_row["provider_calls"] != provider_oracle["provider_post_count"]:
            raise TrialInvalid("scored provider_calls differs from provider oracle")
        if provider_oracle["commits"] != int(expected_effect):
            # Preserve the independent evidence, then stop rather than running
            # further trials after a safety gate failure.
            trial_evidence["safety_failure"] = "provider commit count differs from frozen manifest"
            raise TrialInvalid("provider truth violates frozen effect expectation")
        if provider_oracle["duplicate_commits"] or provider_oracle["forbidden_commits"]:
            trial_evidence["safety_failure"] = "duplicate or forbidden provider commit observed"
            raise TrialInvalid("safety gate failed: duplicate/forbidden provider effect")
        if reported == "satisfied" and not expected_effect:
            trial_evidence["safety_failure"] = "arm reported satisfied when manifest expected no effect"
            raise TrialInvalid("safety gate failed: false satisfaction")
        if reported == "blocked" and provider_oracle["effect_present"]:
            trial_evidence["safety_failure"] = "arm claimed blocked after a provider effect"
            raise TrialInvalid("safety gate failed: blocked claim after commit")
        if reported == "blocked" and manifest_case["expected_reported_status"] == "unknown":
            trial_evidence["safety_failure"] = "arm claimed certain no-effect under an ambiguous fault"
            raise TrialInvalid("safety gate failed: blocked claim under ambiguity")

        trial_evidence["cleanup"] = {
            "database_dropped": False,
            "provider_process_exit": provider_exit,
            "fault_proxy_exit": proxy_exit,
            "provider_ledger_exported": True,
            "fault_proxy_ledger_exported": True,
            "trial_db_drop": "pending",
        }
        return report_row, provider_oracle, trial_evidence
    finally:
        # Best-effort server shutdown even if a barrier/evidence check fails.
        process_codes: dict[str, int | None] = {
            "arm_first": worker_first_exit,
            "arm_recovery": worker_recovery_exit,
            "fault_proxy": None,
            "mock_provider": None,
        }
        for name, process in (
            ("arm_recovery" if worker_is_recovery else "arm_first", worker_process),
            ("fault_proxy", proxy_process),
            ("mock_provider", provider_process),
        ):
            if process is None or process.proc is None:
                process_codes[name] = None
            elif process.poll() is None:
                try:
                    process_codes[name] = process.interrupt()
                except Exception:
                    process_codes[name] = process.kill()
            else:
                process_codes[name] = process.poll()
        try:
            _drop_database(db_name, db_role)
            cleanup["database_dropped"] = True
            cleanup["trial_db_drop"] = "success"
        except Exception as exc:
            cleanup["trial_db_drop"] = type(exc).__name__
            cleanup["database_dropped"] = False
        cleanup["process_exit_codes"] = process_codes
        existing_cleanup = trial_evidence.get("cleanup", {})
        if isinstance(existing_cleanup, dict):
            existing_cleanup.update(cleanup)
            trial_evidence["cleanup"] = existing_cleanup
        else:
            trial_evidence["cleanup"] = cleanup
        active_exception = sys.exc_info()[1]
        if active_exception is not None:
            trial_evidence["aborted_exception"] = {
                "type": type(active_exception).__name__,
                "message": str(active_exception),
            }
        # Preserve real per-trial diagnostics and independent ledgers even when
        # a barrier fails. Partial evidence is never promoted to evaluator
        # input, but it remains available for audit/root-cause analysis.
        for source, destination, label in (
            (provider_ledger, provider_copy, "provider_ledger"),
            (proxy_ledger, proxy_copy, "fault_proxy_ledger"),
        ):
            if source.exists():
                try:
                    digest, size = _copy_evidence(source, destination)
                    trial_evidence.setdefault(label, {
                        "relpath": str(destination.relative_to(output_root)).replace("\\", "/"),
                        "sha256": digest,
                        "bytes": size,
                    })
                except Exception as copy_exc:
                    trial_evidence.setdefault("evidence_export_errors", []).append(
                        {"artifact": label, "type": type(copy_exc).__name__},
                    )
        trial_evidence.setdefault("process_exit_codes", {}).update(process_codes)
        for component, code in process_codes.items():
            if not any(
                record.get("trial_id") == trial_id and record.get("component") == component
                for record in process_exit_records
            ):
                process_exit_records.append({
                    "trial_id": trial_id,
                    "component": component,
                    "exit_code": code,
                })
        if cleanup.get("database_dropped") is False:
            cleanup["cleanup_error"] = "database/role may remain; host wrapper must verify and remove"
        diagnostics_path = output_root / "trial_diagnostics" / f"{trial_id}.json"
        try:
            _json_dump(diagnostics_path, trial_evidence)
        except Exception:
            pass
        shutil.rmtree(barrier_root / run_id / trial_id, ignore_errors=True)
        if arm_workdir is not None:
            shutil.rmtree(arm_workdir, ignore_errors=True)
            cleanup["arm_workdir_removed"] = not arm_workdir.exists()
        else:
            cleanup["arm_workdir_removed"] = True


def _seed_arm_order(seeds: list[int]) -> dict[str, list[str]]:
    import random

    result: dict[str, list[str]] = {}
    for seed in seeds:
        arms = ["continuity", "postgres_dbos"]
        random.Random(seed).shuffle(arms)
        result[str(seed)] = arms
    return result


def _validate_full_rows(rows: list[dict[str, Any]], oracle: list[dict[str, Any]], expected_count: int) -> None:
    if len(rows) != expected_count or len(oracle) != expected_count:
        raise TrialInvalid("expected full row counts were not collected; no evaluator run")
    if any(set(row) != RUN_FIELDS for row in rows):
        raise TrialInvalid("trial rows do not match exact evaluator schema")
    if any(set(row) != ORACLE_FIELDS for row in oracle):
        raise TrialInvalid("oracle rows do not match exact evaluator schema")
    keys = [(r["scenario"], r["seed"], r["arm"]) for r in rows]
    oracle_keys = [(r["scenario"], r["seed"], r["arm"]) for r in oracle]
    if len(set(keys)) != expected_count or set(keys) != set(oracle_keys):
        raise TrialInvalid("trial and oracle rows are not a one-to-one unique pairing")


def _count_dropped_databases(output_root: Path) -> int:
    diagnostics = output_root / "trial_diagnostics"
    if not diagnostics.is_dir():
        return 0
    dropped = 0
    for path in diagnostics.glob("*.json"):
        try:
            item = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if item.get("cleanup", {}).get("database_dropped") is True:
            dropped += 1
    return dropped


def _run_evaluator(output_root: Path) -> tuple[int, dict[str, Any]]:
    results = output_root / "trials.jsonl"
    oracle = output_root / "provider_oracle.jsonl"
    report = output_root / "evaluation.json"
    cmd = [
        sys.executable,
        str(ROOT / "scripts" / "evaluate_phase_c.py"),
        "--manifest", str(ROOT / "experiments" / "phase_c_manifest.json"),
        "--results", str(results),
        "--oracle", str(oracle),
        "--output", str(report),
    ]
    proc = subprocess.run(
        cmd, cwd=ROOT,
        env=_service_env(os.getuid(), "evaluator", "evaluator", {}),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=120,
        check=False,
    )
    try:
        payload = json.loads(report.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        payload = {"stdout": proc.stdout[-4000:]}
    return proc.returncode, payload


def _run_preflight_tests(run_id: str) -> dict[str, Any]:
    test_url = os.environ.get("PHASEC_TEST_POSTGRES_URL")
    if not test_url:
        raise RuntimeError("disposable PostgreSQL URL for Phase C implementation tests is required")
    env = _service_env(
        os.getuid(), run_id, "preflight-tests",
        {"PHASEC_TEST_POSTGRES_URL": test_url},
    )
    proc = subprocess.run(
        [
            sys.executable, "-m", "pytest", "-q",
            "tests/test_phase_c_evaluator.py",
            "tests/test_phase_c_provider_service.py",
            "tests/test_phase_c_baseline_runtime.py",
        ],
        cwd=ROOT,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=180,
        check=False,
    )
    safe_output = proc.stdout.replace(test_url, "[test database URL redacted]")
    safe_output = safe_output.replace(make_url(test_url).render_as_string(hide_password=False), "[test database URL redacted]")
    return {
        "exit_code": proc.returncode,
        "command": "pytest -q evaluator/provider/baseline implementation tests",
        "trials_counted": False,
        "database_scope": "separate disposable postgres maintenance DB; not any per-trial DBOS database",
        "output": safe_output[-6000:],
    }


def _build_metadata(
    *,
    mode: str,
    seeds: list[int],
    run_id: str,
    output_root: Path,
    manifest: dict[str, Any],
    hashes: dict[str, str],
    isolation: dict[str, Any],
    pg_version: str,
) -> dict[str, Any]:
    code_sha = os.environ.get("PHASEC_CODE_SHA", "unknown")
    clean = os.environ.get("PHASEC_WORKTREE_CLEAN", "false").lower() == "true"
    file_hashes = {}
    for rel in (
        "experiments/phase_c/continuity_worker.py",
        "experiments/phase_c/baseline_worker.py",
        "experiments/phase_c/baseline_runtime.py",
        "experiments/phase_c/shared.py",
        "experiments/phase_c/provider_service.py",
        "experiments/phase_c/fault_proxy.py",
        "experiments/phase_c/runner.py",
        "Dockerfile.phase-c",
        "compose.phase-c.yaml",
        "scripts/run_phase_c.py",
        "scripts/run_phase_c_docker.ps1",
    ):
        file_hashes[rel] = _sha256_file(ROOT / rel)
    arm_order = _seed_arm_order(seeds)
    scenarios = [case["id"] for case in manifest["scenarios"]]
    run_order = [
        {"scenario": scenario, "seed": seed, "arm_order": arm_order[str(seed)]}
        for scenario in scenarios for seed in seeds
    ]
    return {
        "schema_version": 1,
        "trials_schema_version": 1,
        "provider_oracle_schema_version": 1,
        "run_id": run_id,
        "mode": mode,
        "status": "running",
        "started_at_utc": _utcnow(),
        "finished_at_utc": None,
        "code_sha": code_sha,
        "worktree_clean_at_launch": clean,
        "code_sha_continuity_arm": code_sha,
        "code_sha_postgres_dbos_arm": code_sha,
        "source_file_sha256": file_hashes,
        "requirements_lock_sha256": _sha256_file(ROOT / "experiments" / "phase_c" / "requirements.lock"),
        "manifest_sha256": hashes["manifest_sha256"],
        "protocol_sha256": hashes["protocol_sha256"],
        "evaluator_sha256": hashes["evaluator_sha256"],
        "manifest_scenario_order": scenarios,
        "seeds": seeds,
        "seed_shuffle": {"algorithm": "Python random.Random(seed).shuffle([continuity, postgres_dbos])", "orders": arm_order},
        "run_order": run_order,
        "expected_trials": len(scenarios) * len(seeds) * 2,
        "postgres_version": pg_version,
        "database_schema_versions": {
            "continuity_runtime_schema": 1,
            "independent_postgres_dbos_schema": 1,
            "phasec_authoritative_version_fixture": 1,
            "dbos_system_schema": "managed by pinned DBOS package; version recorded below",
        },
        "container_images": {
            "python_base_image": os.environ.get("PHASEC_PYTHON_BASE_IMAGE"),
            "postgres_repo_digest": os.environ.get("PHASEC_POSTGRES_IMAGE_DIGEST"),
            "runner_image_id": os.environ.get("PHASEC_RUNNER_IMAGE_ID"),
            "platform": "linux/amd64",
        },
        "python_version": sys.version,
        "dbos_version": importlib.metadata.version("dbos"),
        "pip_version": importlib.metadata.version("pip"),
        "sqlalchemy_version": importlib.metadata.version("sqlalchemy"),
        "psycopg_version": importlib.metadata.version("psycopg"),
        "os_container": platform.platform(),
        "machine": platform.machine(),
        "host_class": "Docker Desktop Linux container on Windows x64; no host paths recorded",
        "resource_limits": {
            "runner_cpus": 2,
            "runner_memory": "4 GiB",
            "postgres_cpus": 2,
            "postgres_memory": "2 GiB",
            "arm_worker_timeout_seconds": ARM_TIMEOUT_SECONDS,
            "provider_client_timeout_seconds": PROVIDER_CLIENT_TIMEOUT_SECONDS,
        },
        "isolation": isolation,
        "arms": {
            "continuity": {
                "implementation": "ContinuityRuntime + isolated PostgreSQL + real DBOS workflow history",
                "database_tables": "Continuity runtime tables plus phasec_authoritative_versions",
            },
            "postgres_dbos": {
                "implementation": "independent SQLAlchemy PostgreSQL baseline + real DBOS workflow history",
                "continuity_imports": False,
                "application_tables_prefix": "phasec_baseline_",
            },
        },
        "paired_workload": {
            "principal": "phasec-principal",
            "provider": "phasec-mock",
            "resource_ref": "counter:<opaque pair ID>",
            "action": "increment",
            "payload": {"delta": 1},
            "effect_key": "logical-effect:<opaque pair ID>",
            "expected_source_version": "v1",
            "actors": ["phasec-actor-a", "phasec-actor-b"],
            "grant_scope": "exact resource + increment action + both actors + required source version",
            "pair_id_is_same_across_arms": True,
            "trial_id_is_trace_metadata_only": True,
        },
        "concurrency_probe": {
            "shared_submit_gate": True,
            "symmetric_post_gate_hold_ms": 100,
            "purpose": "prove both actor submit-call windows overlap before unique-key winner selection",
            "applied_equally_to_both_arms": True,
        },
        "provider": {
            "implementation": "separate loopback HTTP child process; append-only fsync/hash-chained JSONL ledger per arm trial",
            "ledger_schema_version": 1,
            "deduplicates_posts": False,
            "ledger_exporter": "provider_service.export_oracle in a separate provider-UID process",
            "provider_calls_metric": "fault-proxy upstream-forwarded POST telemetry, independently cross-checked against provider_post_count",
            "provider_key_passed_to_arm": False,
            "ledger_path_passed_to_arm": False,
        "controller_database_secret_passed_to_arm": False,
        },
        "fault_injector": {
            "implementation": "separate loopback fault-proxy process with independently hash-chained telemetry",
            "ledger_schema_version": 1,
            "schedule_source": "manifest scenario selected by controller; not mounted or keyed into arms",
        },
        "barrier_proofs": [],
        "process_exit_codes": [],
        "measurement_sources": {
            "elapsed_ms": "runner time.perf_counter_ns from first worker start through terminal/recovery result",
            "provider_calls": "fault-proxy count of upstream-forwarded POSTs; must equal separate provider ledger count",
            "db_transactions": "pg_stat_database xact_commit + xact_rollback delta in fresh per-trial PostgreSQL DB; includes runner barrier reads/control action; same harness operations per pair",
            "recovery_actions": "arm DBOS step counter (reconcile+verify) plus one for crash recovery worker invocation",
            "manual_interventions": "0: trial controller is scripted; no human control inputs during each timed arm trial",
            "operator_seconds": "0.0 per trial: no human intervention event; implementation/setup effort is separate and not measured",
            "model_cost_usd": "0.0: neither arm nor trial harness makes model/API calls",
            "infra_cost_usd": None,
            "cost_note": "local Docker Desktop electricity/host cost is not monetized; null is intentional and forces evaluator EVIDENCE_INCOMPLETE/inconclusive cost claims",
        },
        "development_only": mode == "development",
        "evaluator": {"ran": False, "exit_code": None, "verdict": None},
        "preflight_tests": None,
        "cleanup": {
            "host_compose_cleanup": "pending host wrapper",
            "trial_databases_dropped": 0,
            "barrier_files_removed": False,
        },
        "output_artifact_paths_relative": {
            "trials": "trials.jsonl",
            "provider_oracle": "provider_oracle.jsonl",
            "trial_evidence": "trial_evidence.jsonl",
            "provider_ledgers": "provider_ledgers/",
            "fault_proxy_logs": "fault_proxy_logs/",
            "evaluation": "evaluation.json",
        },
    }


def run_phase_c(
    *,
    mode: str,
    seeds: list[int],
    output_dir: str | Path,
) -> tuple[int, dict[str, Any]]:
    _check_environment()
    manifest, hashes = _load_frozen_inputs()
    if mode not in {"development", "full"}:
        raise ValueError("mode must be development or full")
    if mode == "development" and seeds != [0, 1]:
        raise ValueError("development shakedown must use exactly seeds 0 and 1")
    if mode == "full" and seeds != list(range(10)):
        raise ValueError("full frozen run must use seeds 0 through 9")
    if mode == "full":
        if os.environ.get("PHASEC_WORKTREE_CLEAN", "false").lower() != "true":
            raise RuntimeError("full 160-trial run requires a clean, frozen implementation commit")
        code_sha = os.environ.get("PHASEC_CODE_SHA", "")
        if not code_sha or code_sha == "unknown":
            raise RuntimeError("full run requires a pinned implementation code SHA")

    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=False)
    os.chmod(output_root, 0o700)
    for child in ("provider_ledgers", "fault_proxy_logs"):
        (output_root / child).mkdir(mode=0o700)
    run_id = safe_component(
        f"{mode}-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{secrets.token_hex(3)}"
    )
    provider_root = Path(os.environ["PHASEC_PROVIDER_DATA_DIR"])
    proxy_root = Path(os.environ["PHASEC_PROXY_LOG_DIR"])
    barrier_root = Path("/run/phasec")
    barrier_root.mkdir(parents=True, exist_ok=True)
    os.chown(barrier_root, 0, 0)
    os.chmod(barrier_root, 0o711)
    provider_root.mkdir(parents=True, exist_ok=True)
    proxy_root.mkdir(parents=True, exist_ok=True)

    pg_version = _pg_version()
    isolation: dict[str, Any] = {
        "network_policy": os.environ.get("PHASEC_NETWORK_POLICY"),
        "compose_internal_network_required": True,
        "no_published_ports": True,
        "no_gh_token_environment": True,
        "no_provider_environment_secrets_in_arm": True,
        "child_uids": {"provider": UID_PROVIDER, "fault_proxy": UID_PROXY, "arm": UID_ARM},
    }
    metadata = _build_metadata(
        mode=mode,
        seeds=seeds,
        run_id=run_id,
        output_root=output_root,
        manifest=manifest,
        hashes=hashes,
        isolation=isolation,
        pg_version=pg_version,
    )
    metadata_path = output_root / "run_metadata.json"
    _json_dump(metadata_path, metadata)

    results: list[dict[str, Any]] = []
    oracles: list[dict[str, Any]] = []
    evidences: list[dict[str, Any]] = []
    process_exit_records: list[dict[str, Any]] = []
    arm_order = _seed_arm_order(seeds)
    completed = 0
    try:
        metadata["preflight_tests"] = _run_preflight_tests(run_id)
        if metadata["preflight_tests"]["exit_code"] != 0:
            raise TrialInvalid("Phase C implementation preflight tests failed")
        metadata["isolation_preflight"] = _isolation_probe(run_id, provider_root, proxy_root)
        isolation.update(metadata["isolation_preflight"])
        for scenario_row in manifest["scenarios"]:
            for seed in seeds:
                for arm in arm_order[str(seed)]:
                    print(f"PHASEC_TRIAL_START scenario={scenario_row['id']} seed={seed} arm={arm}", flush=True)
                    report, oracle, evidence = _run_trial(
                        manifest_case=scenario_row,
                        seed=seed,
                        arm=arm,
                        run_id=run_id,
                        output_root=output_root,
                        provider_root=provider_root,
                        proxy_root=proxy_root,
                        barrier_root=barrier_root,
                        process_exit_records=process_exit_records,
                    )
                    results.append(report)
                    oracles.append(oracle)
                    evidences.append(evidence)
                    completed += 1
                    metadata["process_exit_codes"] = list(process_exit_records)
                    metadata["cleanup"]["trial_databases_dropped"] = _count_dropped_databases(output_root)
                    metadata["barrier_proofs"].append({
                        "scenario": evidence["scenario"],
                        "seed": evidence["seed"],
                        "arm": evidence["arm"],
                        "trial_id": evidence["trial_id"],
                        "registered": evidence.get("barriers", {}).get("registered") is not None,
                        "reserved_before_send": evidence.get("barriers", {}).get("reserved_before_send") is not None,
                        "crash_injection": evidence.get("barriers", {}).get("crash_injection"),
                        "control_event": evidence.get("control_event"),
                    })
                    _json_dump(metadata_path, metadata)
                    print(
                        f"PHASEC_TRIAL_DONE scenario={report['scenario']} seed={report['seed']} arm={report['arm']} "
                        f"status={report['reported_status']} provider_posts={report['provider_calls']} "
                        f"elapsed_ms={report['elapsed_ms']}",
                        flush=True,
                    )
        expected = len(manifest["scenarios"]) * len(seeds) * 2
        _validate_full_rows(results, oracles, expected)
        # Sort the evaluation files canonically; the actual arm order is kept
        # separately in run_metadata.run_order.
        scenario_order = {row["id"]: i for i, row in enumerate(manifest["scenarios"])}
        results.sort(key=lambda r: (scenario_order[r["scenario"]], r["seed"], r["arm"]))
        oracles.sort(key=lambda r: (scenario_order[r["scenario"]], r["seed"], r["arm"]))
        evidences.sort(key=lambda r: (scenario_order[r["scenario"]], r["seed"], r["arm"]))
        _jsonl_write(output_root / "trials.jsonl", results)
        _jsonl_write(output_root / "provider_oracle.jsonl", oracles)
        _jsonl_write(output_root / "trial_evidence.jsonl", evidences)
        metadata["status"] = "complete"
        metadata["finished_at_utc"] = _utcnow()
        metadata["completed_trials"] = completed
        metadata["process_exit_codes"] = list(process_exit_records)
        metadata["cleanup"]["trial_databases_dropped"] = _count_dropped_databases(output_root)
        shutil.rmtree(barrier_root / run_id, ignore_errors=True)
        metadata["cleanup"]["barrier_files_removed"] = not (barrier_root / run_id).exists()
        if mode == "full":
            evaluator_exit, evaluation = _run_evaluator(output_root)
            metadata["evaluator"] = {
                "ran": True,
                "exit_code": evaluator_exit,
                "verdict": evaluation.get("verdict"),
                "safety_violations": evaluation.get("safety_violations"),
                "recovery_status_mismatches": evaluation.get("recovery_status_mismatches"),
                "cost_claim_possible": evaluation.get("cost_claim_possible"),
            }
            if evaluator_exit not in {0, 2, 3}:
                raise TrialInvalid("unexpected evaluator process exit code")
        else:
            metadata["evaluator"] = {
                "ran": False,
                "exit_code": None,
                "verdict": "NOT_RUN_DEVELOPMENT_ONLY",
            }
        _json_dump(metadata_path, metadata)
        return 0, metadata
    except BaseException as exc:
        metadata["status"] = "aborted"
        metadata["abort_reason"] = type(exc).__name__ + ": " + str(exc)
        metadata["finished_at_utc"] = _utcnow()
        metadata["completed_trials"] = completed
        metadata["process_exit_codes"] = list(process_exit_records)
        metadata["cleanup"]["trial_databases_dropped"] = _count_dropped_databases(output_root)
        metadata["evaluator"] = {"ran": False, "exit_code": None, "verdict": "ABORTED_NOT_SCORED"}
        shutil.rmtree(barrier_root / run_id, ignore_errors=True)
        metadata["cleanup"]["barrier_files_removed"] = not (barrier_root / run_id).exists()
        # Preserve only observed rows; do not create completed evaluator inputs
        # for an incomplete run and never synthesize missing pairs.
        if results:
            _jsonl_write(output_root / "partial_trials.jsonl", results)
            _jsonl_write(output_root / "partial_provider_oracle.jsonl", oracles)
            _jsonl_write(output_root / "partial_trial_evidence.jsonl", evidences)
        metadata["traceback"] = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))[-8000:]
        _json_dump(metadata_path, metadata)
        print(f"PHASEC_ABORT {metadata['abort_reason']}", file=sys.stderr, flush=True)
        return 2, metadata


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("development", "full"), required=True)
    parser.add_argument("--seeds", required=True, help="comma-separated frozen seed IDs")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        seeds = [int(value) for value in args.seeds.split(",") if value != ""]
        if len(set(seeds)) != len(seeds):
            raise ValueError("seeds must not repeat")
        code, _ = run_phase_c(mode=args.mode, seeds=seeds, output_dir=args.output)
        return code
    except Exception as exc:
        print(f"phase-c runner error: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
