"""Local-only Phase D fault proxy for the independent mock provider.

The proxy is a separate process with its own append-only telemetry ledger. It
accepts requests only from the configured arm-specific client token, forwards
them only to 127.0.0.1, and never reads goal/source/provider ledgers. In the
pack-ACK scenario it drops one acknowledgement only after the mock provider has
returned a durable commit reference for a pack effect.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import http.client
import http.server
import json
import os
import signal
import socket
import sys
import threading
import time
from typing import Any
from urllib.parse import parse_qs, urlencode, urlsplit

from experiments.phase_c.provider_service import (
    AppendOnlyLedger,
    LedgerIntegrityError,
    _canonical_bytes,
    read_verified_ledger,
)
from experiments.phase_d.provider_service import (
    ACTIONS,
    ARMS,
    MOCK_PROVIDER,
    SCENARIOS,
    _bounded_text,
    _decode_json,
)

MAX_BODY_BYTES = 1024 * 1024
PROVIDER_KEY_HEADER = "X-PhaseD-Provider-Key"
CLIENT_TOKEN_HEADER = "X-PhaseD-Proxy-Client-Token"


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _graceful_shutdown(_signum: int, _frame: Any) -> None:
    raise KeyboardInterrupt


def _event_ref(row: dict[str, Any]) -> str:
    return f"phase-d-fault-ledger:{row['seq']}:sha256={row['hash']}"


class ProxyState:
    """Per-trial proxy state; holds no source or domain ledger handles."""

    def __init__(
        self,
        *,
        trial_id: str,
        arm: str,
        seed: int,
        scenario: str,
        provider_url: str,
        provider_key: str,
        client_token: str,
        ledger: AppendOnlyLedger,
        upstream_timeout_seconds: float = 5.0,
    ) -> None:
        self.trial_id = _bounded_text(trial_id, "trial_id", 160)
        if arm not in ARMS:
            raise ValueError("arm must be continuity or postgres_dbos")
        self.arm = arm
        if type(seed) is not int or seed < 0:
            raise ValueError("seed must be a nonnegative integer")
        self.seed = seed
        if scenario not in SCENARIOS:
            raise ValueError("scenario is not in the frozen Phase D manifest")
        self.scenario = scenario
        parsed = urlsplit(provider_url)
        if (
            parsed.scheme != "http"
            or parsed.hostname != "127.0.0.1"
            or parsed.port is None
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path not in ("", "/")
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("provider URL must be a plain 127.0.0.1 HTTP origin")
        if len(provider_key) < 32 or len(client_token) < 32:
            raise ValueError("proxy credentials must each contain at least 32 characters")
        if hmac.compare_digest(provider_key, client_token):
            raise ValueError("provider key and arm client token must be distinct")
        if not 0.1 <= upstream_timeout_seconds <= 30:
            raise ValueError("upstream timeout outside supported bounds")
        self.provider_host = parsed.hostname
        self.provider_port = parsed.port
        self._provider_key = provider_key
        self._client_token = client_token
        self.ledger = ledger
        self.upstream_timeout_seconds = upstream_timeout_seconds
        self.lock = threading.RLock()
        self.ack_drop_used = False
        self.upstream_attempt_count = 0
        self.ledger.append(
            {
                "type": "fault_proxy_started",
                "schema_version": 1,
                "component": "phase_d_fault_proxy",
                "trial_id": self.trial_id,
                "arm": self.arm,
                "seed": self.seed,
                "scenario": self.scenario,
                "provider_port": self.provider_port,
                "upstream_timeout_seconds": self.upstream_timeout_seconds,
                "started_at_ns": time.time_ns(),
            }
        )

    def record(self, event: dict[str, Any]) -> dict[str, Any]:
        return self.ledger.append(
            {
                "trial_id": self.trial_id,
                "arm": self.arm,
                "seed": self.seed,
                "scenario": self.scenario,
                **event,
            }
        )

    def _valid_client(self, headers: Any) -> tuple[bool, str | None]:
        supplied = headers.get(CLIENT_TOKEN_HEADER, "")
        token_ok = bool(supplied) and hmac.compare_digest(supplied, self._client_token)
        if not token_ok:
            return False, "client_token_invalid"
        if headers.get("X-PhaseD-Trial-ID") != self.trial_id:
            return False, "trial_header_mismatch"
        if headers.get("X-PhaseD-Arm") != self.arm:
            return False, "arm_header_mismatch"
        requested_scenario = headers.get("X-PhaseD-Scenario")
        if requested_scenario is not None and requested_scenario != self.scenario:
            return False, "scenario_header_mismatch"
        return True, None

    def _next_attempt_id(self) -> int:
        with self.lock:
            self.upstream_attempt_count += 1
            return self.upstream_attempt_count

    def _forward(
        self, method: str, path: str, body: bytes | None
    ) -> tuple[int, dict[str, str], bytes]:
        connection = http.client.HTTPConnection(
            self.provider_host,
            self.provider_port,
            timeout=self.upstream_timeout_seconds,
        )
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            PROVIDER_KEY_HEADER: self._provider_key,
            "X-PhaseD-Trial-ID": self.trial_id,
            "X-PhaseD-Arm": self.arm,
            "X-PhaseD-Scenario": self.scenario,
            "Connection": "close",
        }
        try:
            connection.request(method, path, body=body, headers=headers)
            response = connection.getresponse()
            raw = response.read(MAX_BODY_BYTES + 1)
            if len(raw) > MAX_BODY_BYTES:
                return (
                    502,
                    {"content-type": "application/json"},
                    b'{"error":"provider response too large"}\n',
                )
            return (
                response.status,
                {key.lower(): value for key, value in response.getheaders()},
                raw,
            )
        finally:
            connection.close()

    def _parse_post(self, raw: bytes | None) -> tuple[dict[str, Any] | None, str | None]:
        if raw is None:
            return None, "missing_or_invalid_content_length"
        try:
            value = _decode_json(raw)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
            return None, "invalid_json"
        if not isinstance(value, dict):
            return None, "body_not_object"
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
            return None, "wrong_body_fields"
        try:
            trial_id = _bounded_text(value["trial_id"], "trial_id", 160)
            for field in (
                "operation_id",
                "actor",
                "provider",
                "resource_ref",
                "action",
                "authority_id",
                "source_revision",
                "effect_key",
            ):
                _bounded_text(value[field], field)
        except (TypeError, ValueError):
            return None, "invalid_body_schema"
        if trial_id != self.trial_id:
            return None, "body_trial_mismatch"
        if value["provider"] != MOCK_PROVIDER:
            return None, "provider_not_mock_provider"
        if value["action"] not in ACTIONS:
            return None, "unsupported_action"
        if not isinstance(value["payload"], dict):
            return None, "payload_not_object"
        return value, None

    def _read_query(self, query: str) -> tuple[str | None, str | None, str | None]:
        parameters = parse_qs(query, keep_blank_values=True, strict_parsing=False)
        if set(parameters) - {"resource_ref", "action"}:
            return None, None, "unsupported_query_parameter"
        refs = parameters.get("resource_ref", [])
        actions = parameters.get("action", [])
        if len(refs) != 1:
            return None, None, "resource_ref_must_appear_once"
        try:
            resource_ref = _bounded_text(refs[0], "resource_ref")
        except ValueError:
            return None, None, "invalid_resource_ref"
        if len(actions) > 1:
            return None, None, "action_must_appear_at_most_once"
        action = actions[0] if actions else None
        if action is not None and action not in ACTIONS:
            return None, None, "unsupported_action"
        return resource_ref, action, None

    def _drop_pack_ack(
        self, action: str | None, status: int, response: dict[str, Any], raw: bytes
    ) -> bool:
        """Consume the one scheduled drop only for an acknowledged committed pack."""
        if self.scenario != "pack_ack_lost_then_supersession" or action != "pack" or status != 200:
            return False
        if response.get("accepted") is not True:
            return False
        commit_ref = response.get("commit_ref")
        if not isinstance(commit_ref, str) or not commit_ref:
            return False
        with self.lock:
            if self.ack_drop_used:
                return False
            self.ack_drop_used = True
        fault = self.record(
            {
                "type": "fault_triggered",
                "fault": "ack_dropped_after_pack_commit",
                "method": "POST",
                "action": "pack",
                "forwarded": True,
                "provider_status": status,
                "provider_commit_ref": commit_ref,
                "provider_receipt_ref": response.get("receipt_ref"),
                "provider_response_sha256": _sha256(raw),
                "client_ack_delivered": False,
                "event_at_ns": time.time_ns(),
            }
        )
        self.record(
            {
                "type": "client_response_dropped",
                "method": "POST",
                "action": "pack",
                "fault_event_ref": _event_ref(fault),
                "reason": "provider committed pack; acknowledgement discarded before arm receipt",
                "event_at_ns": time.time_ns(),
            }
        )
        return True


class ProxyHandler(http.server.BaseHTTPRequestHandler):
    server_version = "PhaseDFaultProxy/1"
    sys_version = ""
    protocol_version = "HTTP/1.1"

    @property
    def state(self) -> ProxyState:
        return self.server.proxy_state  # type: ignore[attr-defined]

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

    def _drop_client_response(self) -> None:
        self.close_connection = True
        try:
            self.connection.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self.connection.close()
        except OSError:
            pass

    def _read_body(self) -> bytes | None:
        raw_length = self.headers.get("Content-Length")
        if raw_length is None or not raw_length.isdecimal():
            return None
        length = int(raw_length)
        if length > MAX_BODY_BYTES:
            return None
        return self.rfile.read(length)

    def _handle(self, method: str) -> None:
        parsed = urlsplit(self.path)
        if parsed.path == "/health" and method == "GET":
            self._send_json(200, {"ok": True, "role": "phase_d_fault_proxy"})
            return
        if parsed.path != "/v1/effects" or method not in {"GET", "POST"}:
            self._send_json(404, {"error": "not found"})
            return

        raw_body = self._read_body() if method == "POST" else None
        body: dict[str, Any] | None = None
        action: str | None = None
        resource_ref: str | None = None
        if method == "POST":
            body, body_error = self.state._parse_post(raw_body)
            action = body.get("action") if body is not None else None
        else:
            resource_ref, action, query_error = self.state._read_query(parsed.query)
            body_error = query_error

        body_hash = _sha256(raw_body) if raw_body is not None else None
        received = self.state.record(
            {
                "type": "proxy_request_received",
                "method": method,
                "path": parsed.path,
                "body_sha256": body_hash,
                "resource_ref": resource_ref,
                "action": action,
                "operation_id": body.get("operation_id") if body is not None else None,
                "effect_key": body.get("effect_key") if body is not None else None,
                "request_at_ns": time.time_ns(),
            }
        )

        authorized, auth_error = self.state._valid_client(self.headers)
        if not authorized:
            self.state.record(
                {
                    "type": "request_rejected",
                    "method": method,
                    "request_seq": received["seq"],
                    "reason": auth_error,
                    "request_at_ns": time.time_ns(),
                }
            )
            self._send_json(
                401 if auth_error == "client_token_invalid" else 403,
                {
                    "error": "arm-scoped proxy authorization required",
                },
            )
            return
        if body_error:
            self.state.record(
                {
                    "type": "request_rejected",
                    "method": method,
                    "request_seq": received["seq"],
                    "reason": body_error,
                    "request_at_ns": time.time_ns(),
                }
            )
            self._send_json(400, {"error": "invalid Phase D effects request"})
            return

        if method == "GET":
            upstream_path = "/v1/effects?" + urlencode(
                [("resource_ref", resource_ref)]
                + ([("action", action)] if action is not None else [])
            )
            upstream_body = None
        else:
            upstream_path = "/v1/effects"
            upstream_body = raw_body

        attempt_id = self.state._next_attempt_id()
        self.state.record(
            {
                "type": "upstream_attempt_started",
                "attempt_id": attempt_id,
                "method": method,
                "path": upstream_path,
                "action": action,
                "resource_ref": resource_ref or (body.get("resource_ref") if body else None),
                "body_sha256": body_hash,
                "forwarded": True,
                "event_at_ns": time.time_ns(),
            }
        )
        try:
            status, upstream_headers, upstream_body = self.state._forward(
                method,
                upstream_path,
                upstream_body,
            )
        except Exception as exc:
            self.state.record(
                {
                    "type": "upstream_error",
                    "attempt_id": attempt_id,
                    "method": method,
                    "exception_type": type(exc).__name__,
                    "forwarded": True,
                    "event_at_ns": time.time_ns(),
                }
            )
            self._send_json(502, {"error": "mock provider upstream unavailable"})
            return

        parsed_body: dict[str, Any] = {}
        try:
            decoded = _decode_json(upstream_body)
            if isinstance(decoded, dict):
                parsed_body = decoded
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
            pass
        accepted = parsed_body.get("accepted") is True
        commit_ref = parsed_body.get("commit_ref")
        upstream_row = self.state.record(
            {
                "type": "upstream_response_received",
                "attempt_id": attempt_id,
                "method": method,
                "status": status,
                "action": action,
                "resource_ref": resource_ref or (body.get("resource_ref") if body else None),
                "accepted": accepted,
                "provider_commit_ref": commit_ref if isinstance(commit_ref, str) else None,
                "response_sha256": _sha256(upstream_body),
                "response_size": len(upstream_body),
                "forwarded": True,
                "event_at_ns": time.time_ns(),
            }
        )
        if self.state._drop_pack_ack(action, status, parsed_body, upstream_body):
            self._drop_client_response()
            return

        self.close_connection = True
        self.send_response(status)
        self.send_header("Content-Type", upstream_headers.get("content-type", "application/json"))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "close")
        self.send_header("Content-Length", str(len(upstream_body)))
        self.end_headers()
        try:
            self.wfile.write(upstream_body)
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            self.state.record(
                {
                    "type": "client_disconnected",
                    "method": method,
                    "attempt_id": attempt_id,
                    "upstream_response_seq": upstream_row["seq"],
                    "event_at_ns": time.time_ns(),
                }
            )

    def do_GET(self) -> None:  # noqa: N802
        self._handle("GET")

    def do_POST(self) -> None:  # noqa: N802
        self._handle("POST")

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


class LoopbackProxyServer(http.server.ThreadingHTTPServer):
    daemon_threads = False
    block_on_close = True
    allow_reuse_address = True


def serve_proxy(args: argparse.Namespace) -> int:
    if not 0 <= args.port <= 65535:
        raise ValueError("port must be between 0 and 65535")
    ledger = AppendOnlyLedger(args.ledger_path)
    server: LoopbackProxyServer | None = None
    started = False
    previous_signals: dict[int, Any] = {}
    try:
        state = ProxyState(
            trial_id=args.trial_id,
            arm=args.arm,
            seed=args.seed,
            scenario=args.scenario,
            provider_url=args.provider_url,
            provider_key=os.environ.get("PHASED_PROVIDER_PROXY_KEY", ""),
            client_token=os.environ.get("PHASED_PROXY_CLIENT_TOKEN", ""),
            ledger=ledger,
        )
        started = True
        server = LoopbackProxyServer(("127.0.0.1", args.port), ProxyHandler)
        server.proxy_state = state  # type: ignore[attr-defined]
        if threading.current_thread() is threading.main_thread():
            for signum in (signal.SIGTERM, getattr(signal, "SIGBREAK", None)):
                if signum is not None:
                    previous_signals[signum] = signal.signal(signum, _graceful_shutdown)
        actual_port = int(server.server_address[1])
        print(
            json.dumps(
                {
                    "ready": True,
                    "role": "phase_d_fault_proxy",
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
                        "type": "fault_proxy_stopped",
                        "schema_version": 1,
                        "trial_id": args.trial_id,
                        "arm": args.arm,
                        "seed": args.seed,
                        "scenario": args.scenario,
                        "stopped_at_ns": time.time_ns(),
                    }
                )
        finally:
            for signum, previous_handler in previous_signals.items():
                signal.signal(signum, previous_handler)
            ledger.close()
    return 0


def read_proxy_ledger(path: str) -> tuple[list[dict[str, Any]], str, bytes]:
    """Verify proxy telemetry from the append-only hash-chained fault ledger."""
    return read_verified_ledger(path)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trial-id", required=True)
    parser.add_argument("--arm", choices=ARMS, required=True)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--scenario", choices=SCENARIOS, required=True)
    parser.add_argument("--ledger-path", required=True)
    parser.add_argument("--provider-url", required=True)
    parser.add_argument("--port", type=int, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        return serve_proxy(args)
    except (OSError, ValueError, LedgerIntegrityError) as exc:
        print(f"phase-d fault proxy error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
