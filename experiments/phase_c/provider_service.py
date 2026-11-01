"""Independent append-only mock provider for Phase C fault experiments.

The provider owns its ledger and accepts traffic only from the loopback fault
proxy using X-PhaseC-Proxy-Key. Arms should receive only the proxy URL plus
X-PhaseC-Trial-ID/X-PhaseC-Arm headers; never pass them this ledger path or key.
The orchestrator must create and permission the ledger file before starting the
provider. This module never chmods it and exposes no HTTP health/export/control
endpoint. All effects are simulated counter increments; there is no outbound
network or production-provider integration.

CLI examples (run from the repository root):

  PHASEC_PROVIDER_PROXY_KEY="$PHASE_C_PROXY_KEY" python -m experiments.phase_c.provider_service serve \
    --trial-id clean-0-continuity --arm continuity --seed 0 \
    --scenario clean --ledger-path results/provider.jsonl \
    --port 8211

  python -m experiments.phase_c.provider_service export \
    --ledger-path results/provider.jsonl --expected-effect true --scenario clean

The exporter prints one JSON object matching the evaluator's oracle row schema.
"""
from __future__ import annotations

import argparse
import hashlib
import hmac
import http.server
import json
import os
import stat
import sys
import threading
import time
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

ARMS = ("continuity", "postgres_dbos")
SCENARIOS = (
    "clean",
    "ack_lost_after_commit",
    "crash_after_reservation_before_send",
    "timeout_before_send",
    "concurrent_duplicate_intent",
    "revoke_before_dispatch",
    "stale_version_before_dispatch",
    "readback_unavailable_after_commit",
)
POST_FIELDS = {
    "trial_id", "operation_id", "principal", "provider", "resource_ref",
    "action", "payload", "effect_key",
}
ZERO_HASH = "0" * 64
MAX_BODY_BYTES = 1024 * 1024


class LedgerIntegrityError(ValueError):
    """Raised when an append-only JSONL ledger is missing or invalid."""


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON constant is not allowed: {value}")


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _regular_existing_file(path: Path) -> os.stat_result:
    try:
        info = path.lstat()
    except FileNotFoundError as exc:
        raise FileNotFoundError(
            f"orchestrator must pre-create and secure ledger file: {path.name}"
        ) from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise LedgerIntegrityError("ledger path must be a regular, non-symlink file")
    return info


def read_verified_ledger(path: str | Path) -> tuple[list[dict], str, bytes]:
    """Read and verify every hash-chain link; never repairs or rewrites a ledger."""
    ledger_path = Path(path)
    _regular_existing_file(ledger_path)
    raw = ledger_path.read_bytes()
    if raw and not raw.endswith(b"\n"):
        raise LedgerIntegrityError("ledger has a partial final line")
    events: list[dict] = []
    previous = ZERO_HASH
    for line_no, line in enumerate(raw.splitlines(), 1):
        if not line:
            raise LedgerIntegrityError(f"ledger line {line_no} is blank")
        try:
            row = json.loads(line.decode("utf-8"), parse_constant=_reject_json_constant)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            raise LedgerIntegrityError(f"ledger line {line_no} is invalid JSON") from exc
        if not isinstance(row, dict) or set(row) != {"seq", "prev_hash", "event", "hash"}:
            raise LedgerIntegrityError(f"ledger line {line_no} has an invalid envelope")
        seq = row["seq"]
        if type(seq) is not int or seq != line_no:
            raise LedgerIntegrityError(f"ledger line {line_no} has a noncontiguous sequence")
        if row["prev_hash"] != previous or not isinstance(row["event"], dict):
            raise LedgerIntegrityError(f"ledger line {line_no} has a broken chain link")
        base = {"seq": seq, "prev_hash": row["prev_hash"], "event": row["event"]}
        expected = _sha256(_canonical_bytes(base))
        if not isinstance(row["hash"], str) or not hmac.compare_digest(row["hash"], expected):
            raise LedgerIntegrityError(f"ledger line {line_no} hash mismatch")
        if not isinstance(row["event"].get("type"), str):
            raise LedgerIntegrityError(f"ledger line {line_no} has no event type")
        events.append(row)
        previous = expected
    return events, previous, raw


class AppendOnlyLedger:
    """Thread-safe append+fsync JSONL writer; file ownership/ACL is external."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        before = _regular_existing_file(self.path)
        rows, previous, _ = read_verified_ledger(self.path)
        flags = os.O_WRONLY | os.O_APPEND
        if hasattr(os, "O_BINARY"):
            flags |= os.O_BINARY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        self.fd = os.open(self.path, flags)
        opened = os.fstat(self.fd)
        if (before.st_dev, before.st_ino) != (opened.st_dev, opened.st_ino):
            os.close(self.fd)
            raise LedgerIntegrityError("ledger identity changed during open")
        self.lock = threading.Lock()
        self.next_seq = len(rows) + 1
        self.previous_hash = previous
        self.closed = False

    def append(self, event: dict) -> dict:
        if not isinstance(event, dict) or not isinstance(event.get("type"), str):
            raise ValueError("ledger event must be a typed object")
        with self.lock:
            if self.closed:
                raise OSError("ledger is closed")
            base = {
                "seq": self.next_seq,
                "prev_hash": self.previous_hash,
                "event": event,
            }
            digest = _sha256(_canonical_bytes(base))
            row = {**base, "hash": digest}
            encoded = _canonical_bytes(row) + b"\n"
            view = memoryview(encoded)
            while view:
                written = os.write(self.fd, view)
                if written <= 0:
                    raise OSError("short write while appending provider evidence")
                view = view[written:]
            os.fsync(self.fd)
            self.next_seq += 1
            self.previous_hash = digest
            return row

    def close(self) -> None:
        with self.lock:
            if not self.closed:
                os.fsync(self.fd)
                os.close(self.fd)
                self.closed = True


def _nonempty_string(value: Any, name: str, *, maximum: int = 512) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise ValueError(f"{name} must be a nonempty string of at most {maximum} chars")
    return value


def _valid_seed(seed: Any) -> int:
    if type(seed) is not int or seed < 0:
        raise ValueError("seed must be a nonnegative integer")
    return seed


class ProviderState:
    """One trial's simulated external state, sourced solely from its ledger."""

    def __init__(
        self,
        *,
        trial_id: str,
        arm: str,
        seed: int,
        proxy_key: str,
        ledger: AppendOnlyLedger,
        scenario: str | None = None,
    ):
        self.trial_id = _nonempty_string(trial_id, "trial_id")
        if arm not in ARMS:
            raise ValueError("arm must be continuity or postgres_dbos")
        self.arm = arm
        self.seed = _valid_seed(seed)
        if len(proxy_key) < 32:
            raise ValueError("proxy key must be at least 32 characters")
        self._proxy_key = proxy_key
        self.ledger = ledger
        self.configured_scenario = scenario
        if scenario is not None and scenario not in SCENARIOS:
            raise ValueError("scenario is not in the frozen Phase C manifest")
        self.lock = threading.RLock()
        self.values: dict[str, int] = {}
        rows, _, _ = read_verified_ledger(self.ledger.path)
        starts = [r["event"] for r in rows if r["event"].get("type") == "provider_started"]
        if len(starts) > 1:
            raise LedgerIntegrityError("ledger has multiple provider_started records")
        if starts:
            start = starts[0]
            expected = (self.trial_id, self.arm, self.seed)
            actual = (start.get("trial_id"), start.get("arm"), start.get("seed"))
            if actual != expected:
                raise LedgerIntegrityError("ledger metadata does not match provider process")
            prior_scenario = start.get("scenario")
            if scenario is not None and prior_scenario not in (None, scenario):
                raise LedgerIntegrityError("ledger scenario does not match provider process")
            if scenario is None:
                self.configured_scenario = prior_scenario
        for row in rows:
            event = row["event"]
            if event.get("type") == "commit":
                ref = event.get("resource_ref")
                value = event.get("value")
                if not isinstance(ref, str) or type(value) is not int:
                    raise LedgerIntegrityError("invalid commit event in provider ledger")
                expected_value = self.values.get(ref, 0) + 1
                if value != expected_value:
                    raise LedgerIntegrityError("provider counter is not monotone")
                self.values[ref] = value
        if not starts:
            self.ledger.append({
                "type": "provider_started",
                "component": "mock_provider",
                "schema_version": 1,
                "trial_id": self.trial_id,
                "arm": self.arm,
                "seed": self.seed,
                "scenario": self.configured_scenario,
                "created_at_ns": time.time_ns(),
            })
        else:
            self.ledger.append({
                "type": "provider_process_restarted",
                "trial_id": self.trial_id,
                "arm": self.arm,
                "seed": self.seed,
                "scenario": self.configured_scenario,
                "created_at_ns": time.time_ns(),
            })

    def _context(self, headers: Any) -> tuple[bool, str | None]:
        supplied_key = headers.get("X-PhaseC-Proxy-Key", "")
        auth_ok = bool(supplied_key) and hmac.compare_digest(supplied_key, self._proxy_key)
        request_trial = headers.get("X-PhaseC-Trial-ID")
        request_arm = headers.get("X-PhaseC-Arm")
        request_scenario = headers.get("X-PhaseC-Scenario")
        valid = request_trial == self.trial_id and request_arm == self.arm
        if self.configured_scenario is not None:
            valid = valid and request_scenario == self.configured_scenario
        if request_scenario is not None and request_scenario not in SCENARIOS:
            valid = False
        return auth_ok, (None if valid else "trial_context_mismatch")

    def post_effect(self, headers: Any, raw_body: bytes | None) -> tuple[int, dict]:
        auth_ok, context_error = self._context(headers)
        digest = _sha256(raw_body) if raw_body is not None else None
        parse_error: str | None = None
        value: Any = None
        if raw_body is not None:
            try:
                value = json.loads(raw_body.decode("utf-8"), parse_constant=_reject_json_constant)
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
                parse_error = "invalid_json"
        header_scenario = headers.get("X-PhaseC-Scenario")
        received = self.ledger.append({
            "type": "post_received",
            "trial_id": self.trial_id,
            "scenario": header_scenario or self.configured_scenario,
            "seed": self.seed,
            "arm": self.arm,
            "request_trial_id": headers.get("X-PhaseC-Trial-ID"),
            "request_arm": headers.get("X-PhaseC-Arm"),
            "authenticated": auth_ok,
            "body_size": len(raw_body) if raw_body is not None else None,
            "body_sha256": digest,
            "received_at_ns": time.time_ns(),
        })
        if not auth_ok:
            self.ledger.append({
                "type": "request_rejected", "trial_id": self.trial_id,
                "request_seq": received["seq"], "reason": "proxy_authentication_failed",
            })
            return 401, {"error": "trusted proxy authentication required"}
        if context_error:
            self.ledger.append({
                "type": "request_rejected", "trial_id": self.trial_id,
                "request_seq": received["seq"], "reason": context_error,
            })
            return 403, {"error": "request outside configured trial"}
        if raw_body is None or parse_error:
            self.ledger.append({
                "type": "request_rejected", "trial_id": self.trial_id,
                "request_seq": received["seq"], "reason": parse_error or "missing_body",
            })
            return 400, {"error": "invalid JSON request"}
        if not isinstance(value, dict) or set(value) != POST_FIELDS:
            self.ledger.append({
                "type": "request_rejected", "trial_id": self.trial_id,
                "request_seq": received["seq"], "reason": "wrong_body_fields",
            })
            return 400, {"error": "request fields do not match mock provider contract"}
        try:
            body_trial = _nonempty_string(value["trial_id"], "trial_id")
            operation_id = _nonempty_string(value["operation_id"], "operation_id")
            principal = _nonempty_string(value["principal"], "principal")
            provider = _nonempty_string(value["provider"], "provider")
            resource_ref = _nonempty_string(value["resource_ref"], "resource_ref")
            action = _nonempty_string(value["action"], "action")
            effect_key = _nonempty_string(value["effect_key"], "effect_key")
            if not isinstance(value["payload"], dict):
                raise ValueError("payload must be an object")
            payload_hash = _sha256(_canonical_bytes(value["payload"]))
        except (TypeError, ValueError):
            self.ledger.append({
                "type": "request_rejected", "trial_id": self.trial_id,
                "request_seq": received["seq"], "reason": "invalid_body_schema",
            })
            return 400, {"error": "invalid request schema"}
        if body_trial != self.trial_id:
            self.ledger.append({
                "type": "request_rejected", "trial_id": self.trial_id,
                "request_seq": received["seq"], "reason": "body_trial_mismatch",
            })
            return 403, {"error": "request outside configured trial"}
        if provider != "phasec-mock" or action != "increment" or value["payload"] != {"delta": 1}:
            self.ledger.append({
                "type": "request_rejected", "trial_id": self.trial_id,
                "request_seq": received["seq"], "reason": "unsupported_effect_contract",
            })
            return 400, {"error": "mock provider only supports increment delta=1"}

        # No idempotency or dedup check is intentionally performed here.
        # Every valid authenticated POST commits one additional counter value.
        with self.lock:
            new_value = self.values.get(resource_ref, 0) + 1
            commit = self.ledger.append({
                "type": "commit",
                "trial_id": self.trial_id,
                "scenario": header_scenario or self.configured_scenario,
                "seed": self.seed,
                "arm": self.arm,
                "post_seq": received["seq"],
                "operation_id": operation_id,
                "principal": principal,
                "provider": provider,
                "resource_ref": resource_ref,
                "action": action,
                "effect_key": effect_key,
                "payload_sha256": payload_hash,
                "value": new_value,
                "committed_at_ns": time.time_ns(),
            })
            self.values[resource_ref] = new_value
        receipt_ref = (
            f"mock-commit:{self.trial_id}:{commit['seq']}:sha256={commit['hash']}"
        )
        return 200, {"accepted": True, "receipt_ref": receipt_ref}

    def read_effect(self, headers: Any, resource_ref: str | None) -> tuple[int, dict]:
        auth_ok, context_error = self._context(headers)
        header_scenario = headers.get("X-PhaseC-Scenario")
        event = self.ledger.append({
            "type": "get_received",
            "trial_id": self.trial_id,
            "scenario": header_scenario or self.configured_scenario,
            "seed": self.seed,
            "arm": self.arm,
            "request_trial_id": headers.get("X-PhaseC-Trial-ID"),
            "request_arm": headers.get("X-PhaseC-Arm"),
            "authenticated": auth_ok,
            "resource_ref": resource_ref,
            "received_at_ns": time.time_ns(),
        })
        if not auth_ok:
            self.ledger.append({
                "type": "request_rejected", "trial_id": self.trial_id,
                "request_seq": event["seq"], "reason": "proxy_authentication_failed",
            })
            return 401, {"error": "trusted proxy authentication required"}
        if context_error:
            self.ledger.append({
                "type": "request_rejected", "trial_id": self.trial_id,
                "request_seq": event["seq"], "reason": context_error,
            })
            return 403, {"error": "request outside configured trial"}
        try:
            ref = _nonempty_string(resource_ref, "resource_ref")
        except ValueError:
            self.ledger.append({
                "type": "request_rejected", "trial_id": self.trial_id,
                "request_seq": event["seq"], "reason": "invalid_resource_ref",
            })
            return 400, {"error": "resource_ref query parameter is required"}
        with self.lock:
            value = self.values.get(ref, 0)
            readback = self.ledger.append({
                "type": "readback",
                "trial_id": self.trial_id,
                "scenario": header_scenario or self.configured_scenario,
                "seed": self.seed,
                "arm": self.arm,
                "get_seq": event["seq"],
                "resource_ref": ref,
                "effect_present": value > 0,
                "value": value,
                "read_at_ns": time.time_ns(),
            })
        evidence_ref = (
            f"mock-readback:{self.trial_id}:{readback['seq']}:"
            f"value={value}:sha256={readback['hash']}"
        )
        return 200, {
            "effect_present": value > 0,
            "value": value,
            "evidence_ref": evidence_ref,
        }


class ProviderHandler(http.server.BaseHTTPRequestHandler):
    server_version = "PhaseCMockProvider/1"
    sys_version = ""
    protocol_version = "HTTP/1.1"

    @property
    def state(self) -> ProviderState:
        return self.server.provider_state  # type: ignore[attr-defined]

    def log_message(self, _format: str, *_args: Any) -> None:
        return

    def _send_json(self, status: int, value: dict) -> None:
        body = _canonical_bytes(value) + b"\n"
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            # Provider effect/readback is already durably in the independent log.
            pass

    def _read_body(self) -> bytes | None:
        raw_len = self.headers.get("Content-Length")
        if raw_len is None or not raw_len.isdecimal():
            return None
        length = int(raw_len)
        if length < 0 or length > MAX_BODY_BYTES:
            return None
        return self.rfile.read(length)

    def do_POST(self) -> None:  # noqa: N802
        if urlsplit(self.path).path != "/v1/effects":
            self._send_json(404, {"error": "not found"})
            return
        raw = self._read_body()
        try:
            status, response = self.state.post_effect(self.headers, raw)
        except Exception:
            self._send_json(500, {"error": "mock provider internal error"})
            return
        self._send_json(status, response)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlsplit(self.path)
        if parsed.path != "/v1/effects":
            self._send_json(404, {"error": "not found"})
            return
        query = parse_qs(parsed.query, keep_blank_values=True, strict_parsing=False)
        refs = query.get("resource_ref", [])
        ref = refs[0] if len(refs) == 1 and len(query) == 1 else None
        try:
            status, response = self.state.read_effect(self.headers, ref)
        except Exception:
            self._send_json(500, {"error": "mock provider internal error"})
            return
        self._send_json(status, response)

    def do_PUT(self) -> None:  # noqa: N802
        self._send_json(405, {"error": "method not allowed"})

    def do_PATCH(self) -> None:  # noqa: N802
        self._send_json(405, {"error": "method not allowed"})

    def do_DELETE(self) -> None:  # noqa: N802
        self._send_json(405, {"error": "method not allowed"})

    def do_OPTIONS(self) -> None:  # noqa: N802
        self._send_json(405, {"error": "method not allowed"})

    def do_HEAD(self) -> None:  # noqa: N802
        self._send_json(405, {"error": "method not allowed"})


class LoopbackHTTPServer(http.server.ThreadingHTTPServer):
    # Ensure provider writes finish before the ledger is fsynced and closed.
    daemon_threads = False
    block_on_close = True
    allow_reuse_address = True


def serve_provider(args: argparse.Namespace) -> int:
    if not 0 <= args.port <= 65535:
        raise ValueError("port must be between 0 and 65535")
    ledger = AppendOnlyLedger(args.ledger_path)
    try:
        state = ProviderState(
            trial_id=args.trial_id,
            arm=args.arm,
            seed=args.seed,
            scenario=args.scenario,
            proxy_key=os.environ.get(args.proxy_key_env, ""),
            ledger=ledger,
        )
        server = LoopbackHTTPServer(("127.0.0.1", args.port), ProviderHandler)
        server.provider_state = state  # type: ignore[attr-defined]
        actual_port = int(server.server_address[1])
        print(json.dumps({
            "ready": True, "role": "mock_provider", "trial_id": args.trial_id,
            "arm": args.arm, "port": actual_port,
        }, sort_keys=True), flush=True)
        try:
            server.serve_forever(poll_interval=0.1)
        except KeyboardInterrupt:
            pass
        finally:
            server.server_close()
            ledger.append({
                "type": "provider_process_stopped",
                "schema_version": 1,
                "trial_id": args.trial_id,
                "arm": args.arm,
                "seed": args.seed,
                "scenario": args.scenario,
                "stopped_at_ns": time.time_ns(),
            })
    finally:
        ledger.close()
    return 0


def _scenario_from_ledger(events: list[dict], override: str | None) -> str:
    values = {
        row["event"].get("scenario")
        for row in events
        if isinstance(row["event"].get("scenario"), str)
    }
    if override is not None:
        if override not in SCENARIOS:
            raise ValueError("scenario override is not in the frozen manifest")
        if values and values != {override}:
            raise LedgerIntegrityError("scenario override does not match provider ledger")
        return override
    if len(values) != 1:
        raise ValueError("scenario missing/ambiguous; pass --scenario for zero-call trials")
    scenario = next(iter(values))
    if scenario not in SCENARIOS:
        raise LedgerIntegrityError("ledger scenario is not in the frozen manifest")
    return scenario


def export_oracle(
    ledger_path: str | Path,
    *,
    expected_effect: bool,
    scenario: str | None = None,
) -> dict:
    """Verify a provider ledger and derive one evaluator oracle row."""
    if type(expected_effect) is not bool:
        raise ValueError("expected_effect must be boolean")
    rows, _, raw = read_verified_ledger(ledger_path)
    starts = [row["event"] for row in rows if row["event"].get("type") == "provider_started"]
    if len(starts) != 1:
        raise LedgerIntegrityError("provider ledger must contain exactly one start record")
    start = starts[0]
    trial_id = _nonempty_string(start.get("trial_id"), "ledger trial_id")
    arm = start.get("arm")
    seed = start.get("seed")
    if arm not in ARMS or type(seed) is not int or seed < 0:
        raise LedgerIntegrityError("provider start record has invalid arm/seed")
    selected_scenario = _scenario_from_ledger(rows, scenario or start.get("scenario"))
    for row in rows:
        event = row["event"]
        if event.get("trial_id") != trial_id:
            raise LedgerIntegrityError("ledger mixes trial IDs")
        if event.get("arm", arm) != arm or event.get("seed", seed) != seed:
            raise LedgerIntegrityError("ledger mixes arms or seeds")
        observed_scenario = event.get("scenario")
        if observed_scenario is not None and observed_scenario != selected_scenario:
            raise LedgerIntegrityError("ledger mixes scenarios")
    post_count = sum(row["event"].get("type") == "post_received" for row in rows)
    commits = [row["event"] for row in rows if row["event"].get("type") == "commit"]
    counts: dict[str, int] = {}
    for event in commits:
        effect_key = event.get("effect_key")
        if not isinstance(effect_key, str) or not effect_key:
            raise LedgerIntegrityError("commit is missing effect_key")
        counts[effect_key] = counts.get(effect_key, 0) + 1
    duplicates = sum(max(0, count - 1) for count in counts.values())
    forbidden = len(commits) if not expected_effect else 0
    file_digest = _sha256(raw)
    return {
        "scenario": selected_scenario,
        "seed": seed,
        "arm": arm,
        "commits": len(commits),
        "duplicate_commits": duplicates,
        "forbidden_commits": forbidden,
        "effect_present": bool(commits),
        "provider_post_count": post_count,
        "oracle_log_ref": f"mock-provider-ledger:sha256:{file_digest}",
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    serve = sub.add_parser("serve", help="start one loopback mock provider")
    serve.add_argument("--trial-id", required=True)
    serve.add_argument("--arm", choices=ARMS, required=True)
    serve.add_argument("--seed", type=int, required=True)
    serve.add_argument("--scenario", choices=SCENARIOS)
    serve.add_argument("--ledger-path", required=True)
    # Never put the internal provider key in argv: process command lines may be
    # observable by other users. The runner passes it only in the provider
    # process environment, which is unreadable to the arm UID.
    serve.add_argument("--proxy-key-env", default="PHASEC_PROVIDER_PROXY_KEY")
    serve.add_argument("--port", type=int, required=True)
    export = sub.add_parser("export", help="verify and export one provider oracle row")
    export.add_argument("--ledger-path", required=True)
    export.add_argument("--expected-effect", choices=("true", "false"), required=True)
    export.add_argument("--scenario", choices=SCENARIOS)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        if args.command == "serve":
            return serve_provider(args)
        result = export_oracle(
            args.ledger_path,
            expected_effect=args.expected_effect == "true",
            scenario=args.scenario,
        )
    except (OSError, ValueError, LedgerIntegrityError) as exc:
        print(f"phase-c provider error: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
