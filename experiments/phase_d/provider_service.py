"""Independent local-only mock provider for Phase D shipment experiments.

This service is the sole writer of its append-only effect ledger. It binds only
127.0.0.1 and never calls an external provider. The test arm reaches it through
the separate fault proxy using X-PhaseD-Provider-Key; the domain owner may use
a distinct read-only X-PhaseD-Provider-Oracle-Key for independent readback.
"""

from __future__ import annotations

import argparse
import hmac
import http.server
import json
import os
import signal
import sys
import threading
import time
from typing import Any
from urllib.parse import parse_qs, urlsplit

from experiments.phase_c.provider_service import (
    AppendOnlyLedger,
    LedgerIntegrityError,
    _canonical_bytes,
    read_verified_ledger,
)

ACTIONS = ("pack", "ship", "quarantine")
ARMS = ("continuity", "postgres_dbos")
SCENARIOS = (
    "normal_supersession",
    "pack_ack_lost_then_supersession",
    "unauthorized_actor_during_handoff",
    "new_action_crash_before_send",
)
MOCK_PROVIDER = "phase-d-mock-shipment"
MAX_BODY_BYTES = 1024 * 1024
PROXY_HEADER = "X-PhaseD-Provider-Key"
ORACLE_HEADER = "X-PhaseD-Provider-Oracle-Key"


def _sha256(raw: bytes) -> str:
    import hashlib

    return hashlib.sha256(raw).hexdigest()


def _reject_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON constant is not allowed: {value}")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON object key")
        result[key] = value
    return result


def _decode_json(raw: bytes) -> Any:
    return json.loads(
        raw.decode("utf-8"),
        parse_constant=_reject_constant,
        object_pairs_hook=_unique_object,
    )


def _bounded_text(value: Any, field: str, maximum: int = 512) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise ValueError(f"{field} must be a nonempty string of at most {maximum} chars")
    return value


def _valid_seed(seed: Any) -> int:
    if type(seed) is not int or seed < 0:
        raise ValueError("seed must be a nonnegative integer")
    return seed


def _graceful_shutdown(_signum: int, _frame: Any) -> None:
    raise KeyboardInterrupt


def _ref(kind: str, trial_id: str, row: dict[str, Any]) -> str:
    return f"{kind}:{trial_id}:{row['seq']}:sha256={row['hash']}"


class ProviderState:
    """Thread-safe simulated effect store backed only by the provider ledger."""

    def __init__(
        self,
        *,
        trial_id: str,
        arm: str,
        seed: int,
        proxy_key: str,
        oracle_key: str,
        ledger: AppendOnlyLedger,
    ) -> None:
        self.trial_id = _bounded_text(trial_id, "trial_id", 160)
        if arm not in ARMS:
            raise ValueError("arm must be continuity or postgres_dbos")
        self.arm = arm
        self.seed = _valid_seed(seed)
        if len(proxy_key) < 32 or len(oracle_key) < 32:
            raise ValueError("provider credentials must each contain at least 32 characters")
        if hmac.compare_digest(proxy_key, oracle_key):
            raise ValueError("provider proxy and oracle credentials must be distinct")
        self._proxy_key = proxy_key
        self._oracle_key = oracle_key
        self.ledger = ledger
        self.lock = threading.RLock()
        self.action_counts: dict[str, dict[str, int]] = {}

        rows, _, _ = read_verified_ledger(ledger.path)
        starts = [row["event"] for row in rows if row["event"].get("type") == "provider_started"]
        if len(starts) > 1:
            raise LedgerIntegrityError("provider ledger contains multiple start events")
        for start in starts:
            if (start.get("trial_id"), start.get("arm"), start.get("seed")) != (
                self.trial_id,
                self.arm,
                self.seed,
            ):
                raise LedgerIntegrityError("provider ledger metadata does not match process")

        for row in rows:
            event = row["event"]
            if event.get("type") != "effect_committed":
                continue
            if event.get("trial_id") != self.trial_id or event.get("arm") != self.arm:
                raise LedgerIntegrityError("provider ledger mixes trial or arm identities")
            resource_ref = event.get("resource_ref")
            action = event.get("action")
            count = event.get("action_count")
            if not isinstance(resource_ref, str) or action not in ACTIONS or type(count) is not int:
                raise LedgerIntegrityError("provider ledger has a malformed effect record")
            counts = self.action_counts.setdefault(resource_ref, {name: 0 for name in ACTIONS})
            if count != counts[action] + 1:
                raise LedgerIntegrityError("provider action counter is not monotone")
            counts[action] = count

        if starts:
            self.ledger.append(
                {
                    "type": "provider_process_restarted",
                    "schema_version": 1,
                    "component": "phase_d_mock_provider",
                    "trial_id": self.trial_id,
                    "arm": self.arm,
                    "seed": self.seed,
                    "restarted_at_ns": time.time_ns(),
                }
            )
        else:
            self.ledger.append(
                {
                    "type": "provider_started",
                    "schema_version": 1,
                    "component": "phase_d_mock_provider",
                    "trial_id": self.trial_id,
                    "arm": self.arm,
                    "seed": self.seed,
                    "created_at_ns": time.time_ns(),
                }
            )

    def _auth_role(self, headers: Any, method: str) -> str | None:
        supplied_proxy = headers.get(PROXY_HEADER, "")
        supplied_oracle = headers.get(ORACLE_HEADER, "")
        proxy_ok = bool(supplied_proxy) and hmac.compare_digest(supplied_proxy, self._proxy_key)
        oracle_ok = bool(supplied_oracle) and hmac.compare_digest(supplied_oracle, self._oracle_key)
        if method == "GET" and oracle_ok:
            return "oracle"
        if proxy_ok:
            return "proxy"
        return None

    def _context_error(self, headers: Any, body_trial: str | None = None) -> str | None:
        request_trial = headers.get("X-PhaseD-Trial-ID")
        request_arm = headers.get("X-PhaseD-Arm")
        if request_trial is not None and request_trial != self.trial_id:
            return "trial_header_mismatch"
        if request_arm is not None and request_arm != self.arm:
            return "arm_header_mismatch"
        if body_trial is not None and body_trial != self.trial_id:
            return "body_trial_mismatch"
        scenario = headers.get("X-PhaseD-Scenario")
        if scenario is not None and scenario not in SCENARIOS:
            return "scenario_header_invalid"
        return None

    def post_effect(self, headers: Any, raw_body: bytes | None) -> tuple[int, dict[str, Any]]:
        role = self._auth_role(headers, "POST")
        digest = _sha256(raw_body) if raw_body is not None else None
        received = self.ledger.append(
            {
                "type": "effect_post_received",
                "trial_id": self.trial_id,
                "arm": self.arm,
                "seed": self.seed,
                "scenario": headers.get("X-PhaseD-Scenario"),
                "request_trial_id": headers.get("X-PhaseD-Trial-ID"),
                "request_arm": headers.get("X-PhaseD-Arm"),
                "authenticated_as": role,
                "body_size": len(raw_body) if raw_body is not None else None,
                "body_sha256": digest,
                "received_at_ns": time.time_ns(),
            }
        )
        if role != "proxy":
            self.ledger.append(
                {
                    "type": "request_rejected",
                    "trial_id": self.trial_id,
                    "request_seq": received["seq"],
                    "reason": "proxy_authentication_required_for_write",
                }
            )
            return 401, {"error": "trusted provider proxy authentication required"}
        if raw_body is None:
            self.ledger.append(
                {
                    "type": "request_rejected",
                    "trial_id": self.trial_id,
                    "request_seq": received["seq"],
                    "reason": "missing_or_invalid_content_length",
                }
            )
            return 400, {"error": "JSON request body required"}
        try:
            value = _decode_json(raw_body)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
            value = None
        if not isinstance(value, dict):
            self.ledger.append(
                {
                    "type": "request_rejected",
                    "trial_id": self.trial_id,
                    "request_seq": received["seq"],
                    "reason": "invalid_json_object",
                }
            )
            return 400, {"error": "valid JSON object required"}
        required = {
            "trial_id",
            "operation_id",
            "actor",
            "provider",
            "resource_ref",
            "action",
            "payload",
            "authority_id",
            "source_revision",
            "effect_key",
        }
        if set(value) != required:
            self.ledger.append(
                {
                    "type": "request_rejected",
                    "trial_id": self.trial_id,
                    "request_seq": received["seq"],
                    "reason": "wrong_body_fields",
                }
            )
            return 400, {"error": "request fields do not match Phase D provider contract"}
        try:
            body_trial = _bounded_text(value["trial_id"], "trial_id", 160)
            operation_id = _bounded_text(value["operation_id"], "operation_id")
            actor = _bounded_text(value["actor"], "actor")
            provider = _bounded_text(value["provider"], "provider")
            resource_ref = _bounded_text(value["resource_ref"], "resource_ref")
            action = _bounded_text(value["action"], "action")
            authority_id = _bounded_text(value["authority_id"], "authority_id")
            source_revision = _bounded_text(value["source_revision"], "source_revision")
            effect_key = _bounded_text(value["effect_key"], "effect_key")
            if not isinstance(value["payload"], dict):
                raise ValueError("payload must be a JSON object")
            payload_hash = _sha256(_canonical_bytes(value["payload"]))
        except (TypeError, ValueError):
            self.ledger.append(
                {
                    "type": "request_rejected",
                    "trial_id": self.trial_id,
                    "request_seq": received["seq"],
                    "reason": "invalid_body_schema",
                }
            )
            return 400, {"error": "invalid request schema"}

        if provider != MOCK_PROVIDER:
            self.ledger.append(
                {
                    "type": "request_rejected",
                    "trial_id": self.trial_id,
                    "request_seq": received["seq"],
                    "reason": "provider_not_mock_provider",
                }
            )
            return 400, {"error": "only the Phase D local mock provider is supported"}
        if action not in ACTIONS:
            self.ledger.append(
                {
                    "type": "request_rejected",
                    "trial_id": self.trial_id,
                    "request_seq": received["seq"],
                    "reason": "unsupported_action",
                }
            )
            return 400, {"error": "action must be pack, ship, or quarantine"}
        context_error = self._context_error(headers, body_trial)
        if context_error:
            self.ledger.append(
                {
                    "type": "request_rejected",
                    "trial_id": self.trial_id,
                    "request_seq": received["seq"],
                    "reason": context_error,
                }
            )
            return 403, {"error": "request outside configured trial"}

        # Deliberately do not deduplicate effect_key or operation_id. Each valid
        # POST is an independently visible simulated external effect.
        with self.lock:
            counts = self.action_counts.setdefault(resource_ref, {name: 0 for name in ACTIONS})
            next_count = counts[action] + 1
            committed = self.ledger.append(
                {
                    "type": "effect_committed",
                    "schema_version": 1,
                    "trial_id": self.trial_id,
                    "arm": self.arm,
                    "seed": self.seed,
                    "scenario": headers.get("X-PhaseD-Scenario"),
                    "post_seq": received["seq"],
                    "operation_id": operation_id,
                    "actor": actor,
                    "provider": provider,
                    "resource_ref": resource_ref,
                    "action": action,
                    "payload_sha256": payload_hash,
                    "authority_id": authority_id,
                    "source_revision": source_revision,
                    "effect_key": effect_key,
                    "action_count": next_count,
                    "committed_at_ns": time.time_ns(),
                }
            )
            counts[action] = next_count
        commit_ref = _ref("mock-provider-commit", self.trial_id, committed)
        return 200, {
            "accepted": True,
            "trial_id": self.trial_id,
            "arm": self.arm,
            "action": action,
            "action_count": next_count,
            "commit_ref": commit_ref,
            "receipt_ref": commit_ref,
        }

    def read_effect(
        self,
        headers: Any,
        resource_ref: str | None,
        action: str | None = None,
    ) -> tuple[int, dict[str, Any]]:
        role = self._auth_role(headers, "GET")
        request = self.ledger.append(
            {
                "type": "effect_readback_requested",
                "trial_id": self.trial_id,
                "arm": self.arm,
                "seed": self.seed,
                "scenario": headers.get("X-PhaseD-Scenario"),
                "request_trial_id": headers.get("X-PhaseD-Trial-ID"),
                "request_arm": headers.get("X-PhaseD-Arm"),
                "authenticated_as": role,
                "resource_ref": resource_ref,
                "action": action,
                "received_at_ns": time.time_ns(),
            }
        )
        if role is None:
            self.ledger.append(
                {
                    "type": "request_rejected",
                    "trial_id": self.trial_id,
                    "request_seq": request["seq"],
                    "reason": "provider_readback_authentication_required",
                }
            )
            return 401, {"error": "provider readback authentication required"}
        try:
            ref = _bounded_text(resource_ref, "resource_ref")
        except ValueError:
            self.ledger.append(
                {
                    "type": "request_rejected",
                    "trial_id": self.trial_id,
                    "request_seq": request["seq"],
                    "reason": "invalid_resource_ref",
                }
            )
            return 400, {"error": "resource_ref query parameter is required"}
        if action is not None and action not in ACTIONS:
            self.ledger.append(
                {
                    "type": "request_rejected",
                    "trial_id": self.trial_id,
                    "request_seq": request["seq"],
                    "reason": "unsupported_readback_action",
                }
            )
            return 400, {"error": "action must be pack, ship, or quarantine"}
        context_error = self._context_error(headers)
        if context_error:
            self.ledger.append(
                {
                    "type": "request_rejected",
                    "trial_id": self.trial_id,
                    "request_seq": request["seq"],
                    "reason": context_error,
                }
            )
            return 403, {"error": "request outside configured trial"}
        with self.lock:
            counts = dict(self.action_counts.get(ref, {name: 0 for name in ACTIONS}))
            readback = self.ledger.append(
                {
                    "type": "effect_readback",
                    "schema_version": 1,
                    "trial_id": self.trial_id,
                    "arm": self.arm,
                    "seed": self.seed,
                    "scenario": headers.get("X-PhaseD-Scenario"),
                    "readback_request_seq": request["seq"],
                    "resource_ref": ref,
                    "action_counts": counts,
                    "effect_present": any(counts.values()),
                    "action": action,
                    "action_count": counts[action] if action is not None else None,
                    "read_at_ns": time.time_ns(),
                }
            )
        evidence_ref = _ref("mock-provider-readback", self.trial_id, readback)
        result: dict[str, Any] = {
            "trial_id": self.trial_id,
            "arm": self.arm,
            "resource_ref": ref,
            "action_counts": counts,
            "effect_present": any(counts.values()),
            "evidence_ref": evidence_ref,
            "ledger_ref": evidence_ref,
        }
        if action is not None:
            result["action"] = action
            result["action_count"] = counts[action]
        return 200, result


class ProviderHandler(http.server.BaseHTTPRequestHandler):
    server_version = "PhaseDMockProvider/1"
    sys_version = ""
    protocol_version = "HTTP/1.1"

    @property
    def state(self) -> ProviderState:
        return self.server.provider_state  # type: ignore[attr-defined]

    def log_message(self, _format: str, *_args: Any) -> None:
        return

    def _send_json(self, status: int, value: dict[str, Any]) -> None:
        body = _canonical_bytes(value) + b"\n"
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "close")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.close_connection = True
        try:
            self.wfile.write(body)
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _read_body(self) -> bytes | None:
        raw_length = self.headers.get("Content-Length")
        if raw_length is None or not raw_length.isdecimal():
            return None
        length = int(raw_length)
        if length > MAX_BODY_BYTES:
            return None
        return self.rfile.read(length)

    def _handle_get(self) -> None:
        if self.state._auth_role(self.headers, "GET") is None:
            status, result = self.state.read_effect(self.headers, None)
            self._send_json(status, result)
            return
        parsed = urlsplit(self.path)
        query = parse_qs(parsed.query, keep_blank_values=True, strict_parsing=False)
        if set(query) - {"resource_ref", "action"}:
            self._send_json(400, {"error": "unsupported query parameter"})
            return
        refs = query.get("resource_ref", [])
        actions = query.get("action", [])
        resource_ref = refs[0] if len(refs) == 1 else None
        action = actions[0] if len(actions) == 1 else None
        if len(actions) > 1:
            self._send_json(400, {"error": "action must appear at most once"})
            return
        try:
            status, result = self.state.read_effect(self.headers, resource_ref, action)
        except Exception:
            self._send_json(500, {"error": "mock provider internal error"})
            return
        self._send_json(status, result)

    def do_GET(self) -> None:  # noqa: N802
        if urlsplit(self.path).path == "/health":
            self._send_json(200, {"ok": True, "role": "phase_d_provider"})
            return
        if urlsplit(self.path).path != "/v1/effects":
            self._send_json(404, {"error": "not found"})
            return
        self._handle_get()

    def do_POST(self) -> None:  # noqa: N802
        if urlsplit(self.path).path != "/v1/effects":
            self._send_json(404, {"error": "not found"})
            return
        if self.state._auth_role(self.headers, "POST") is None:
            status, result = self.state.post_effect(self.headers, None)
            self._send_json(status, result)
            return
        if urlsplit(self.path).query:
            self._send_json(400, {"error": "POST query parameters are not supported"})
            return
        raw_body = self._read_body()
        try:
            status, result = self.state.post_effect(self.headers, raw_body)
        except Exception:
            self._send_json(500, {"error": "mock provider internal error"})
            return
        self._send_json(status, result)

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


class LoopbackProviderServer(http.server.ThreadingHTTPServer):
    daemon_threads = False
    block_on_close = True
    allow_reuse_address = True


def serve_provider(args: argparse.Namespace) -> int:
    if not 0 <= args.port <= 65535:
        raise ValueError("port must be between 0 and 65535")
    ledger = AppendOnlyLedger(args.ledger_path)
    server: LoopbackProviderServer | None = None
    started = False
    previous_signals: dict[int, Any] = {}
    try:
        server = LoopbackProviderServer(("127.0.0.1", args.port), ProviderHandler)
        state = ProviderState(
            trial_id=args.trial_id,
            arm=args.arm,
            seed=args.seed,
            proxy_key=os.environ.get("PHASED_PROVIDER_PROXY_KEY", ""),
            oracle_key=os.environ.get("PHASED_PROVIDER_ORACLE_KEY", ""),
            ledger=ledger,
        )
        started = True
        server.provider_state = state  # type: ignore[attr-defined]
        if threading.current_thread() is threading.main_thread():
            for signum in (signal.SIGTERM, getattr(signal, "SIGBREAK", None)):
                if signum is not None:
                    previous_signals[signum] = signal.signal(signum, _graceful_shutdown)
        actual_port = int(server.server_address[1])
        print(
            json.dumps(
                {
                    "ready": True,
                    "role": "phase_d_provider",
                    "trial_id": args.trial_id,
                    "arm": args.arm,
                    "seed": args.seed,
                    "port": actual_port,
                },
                sort_keys=True,
            ),
            flush=True,
        )
        try:
            server.serve_forever(poll_interval=0.1)
        except KeyboardInterrupt:
            pass
    finally:
        try:
            if server is not None:
                server.server_close()
            if started:
                ledger.append(
                    {
                        "type": "provider_stopped",
                        "schema_version": 1,
                        "trial_id": args.trial_id,
                        "arm": args.arm,
                        "seed": args.seed,
                        "stopped_at_ns": time.time_ns(),
                    }
                )
        finally:
            for signum, previous_handler in previous_signals.items():
                signal.signal(signum, previous_handler)
            ledger.close()
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trial-id", required=True)
    parser.add_argument("--arm", choices=ARMS, required=True)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--ledger-path", required=True)
    parser.add_argument("--port", type=int, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        return serve_provider(args)
    except (OSError, ValueError, LedgerIntegrityError) as exc:
        print(f"phase-d provider error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
