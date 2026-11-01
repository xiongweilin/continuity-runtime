"""Independent local fault injector/proxy for the Phase C mock provider.

The proxy is a separate process and owns its own append-only fault log. Arms
can reach only this loopback listener; the provider's internal key, service
port, schedule, and ledger path are not passed to arm processes.
"""
from __future__ import annotations

import argparse
import hashlib
import http.client
import json
import os
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from .provider_service import (
    ARMS,
    SCENARIOS,
    AppendOnlyLedger,
    _canonical_bytes,
    read_verified_ledger,
)

MAX_BODY_BYTES = 1024 * 1024


class ProxyState:
    def __init__(
        self,
        *,
        trial_id: str,
        arm: str,
        seed: int,
        scenario: str,
        provider_url: str,
        proxy_key: str,
        ledger: AppendOnlyLedger,
        client_timeout_seconds: float,
    ) -> None:
        if not trial_id or len(trial_id) > 160:
            raise ValueError("invalid trial_id")
        if arm not in ARMS:
            raise ValueError("invalid arm")
        if type(seed) is not int or seed < 0:
            raise ValueError("invalid seed")
        if scenario not in SCENARIOS:
            raise ValueError("invalid scenario")
        parsed = urlsplit(provider_url)
        if parsed.scheme != "http" or parsed.hostname != "127.0.0.1" or not parsed.port:
            raise ValueError("provider must be a loopback HTTP service")
        if len(proxy_key) < 32:
            raise ValueError("internal proxy key is too short")
        if not 0.1 <= client_timeout_seconds <= 5:
            raise ValueError("client timeout outside deterministic test bounds")
        self.trial_id = trial_id
        self.arm = arm
        self.seed = seed
        self.scenario = scenario
        self.provider_host = parsed.hostname
        self.provider_port = parsed.port
        self.proxy_key = proxy_key
        self.ledger = ledger
        self.client_timeout_seconds = client_timeout_seconds
        self.lock = threading.RLock()
        self.committed = False
        self.forwarded_post_count = 0
        self.forwarded_get_count = 0
        self.suppressed_post_count = 0
        self.blocked_get_count = 0
        self.fault_trigger_counts: dict[str, int] = {}
        self.ledger.append({
            "type": "fault_proxy_started",
            "schema_version": 1,
            "trial_id": trial_id,
            "arm": arm,
            "seed": seed,
            "scenario": scenario,
            "provider_port": parsed.port,
            "client_timeout_seconds": client_timeout_seconds,
            "started_at_ns": time.time_ns(),
        })

    def record(self, event: dict[str, Any]) -> None:
        self.ledger.append({
            "trial_id": self.trial_id,
            "arm": self.arm,
            "seed": self.seed,
            "scenario": self.scenario,
            **event,
        })

    def count_fault(self, fault: str) -> None:
        with self.lock:
            self.fault_trigger_counts[fault] = self.fault_trigger_counts.get(fault, 0) + 1


class ProxyHandler(BaseHTTPRequestHandler):
    server_version = "PhaseCFaultProxy/1"
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
        value = self.headers.get("Content-Length")
        if value is None or not value.isdecimal():
            return None
        length = int(value)
        if length < 0 or length > MAX_BODY_BYTES:
            return None
        return self.rfile.read(length)

    def _validate_context(self, body: dict[str, Any] | None) -> str | None:
        if self.headers.get("X-PhaseC-Trial-ID") != self.state.trial_id:
            return "trial_header_mismatch"
        if self.headers.get("X-PhaseC-Arm") != self.state.arm:
            return "arm_header_mismatch"
        if body is not None:
            if body.get("trial_id") != self.state.trial_id:
                return "body_trial_mismatch"
        return None

    def _forward(self, method: str, path: str, body: bytes | None) -> tuple[int, dict[str, str], bytes]:
        conn = http.client.HTTPConnection(
            self.state.provider_host,
            self.state.provider_port,
            timeout=3.0,
        )
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-PhaseC-Proxy-Key": self.state.proxy_key,
            "X-PhaseC-Trial-ID": self.state.trial_id,
            "X-PhaseC-Arm": self.state.arm,
            "X-PhaseC-Scenario": self.state.scenario,
            "Connection": "close",
        }
        try:
            conn.request(method, path, body=body, headers=headers)
            response = conn.getresponse()
            data = response.read(MAX_BODY_BYTES + 1)
            if len(data) > MAX_BODY_BYTES:
                return 502, {}, b'{"error":"upstream response too large"}\n'
            return response.status, {k.lower(): v for k, v in response.getheaders()}, data
        finally:
            conn.close()

    def _handle(self, method: str) -> None:
        parsed = urlsplit(self.path)
        if parsed.path == "/health" and method == "GET":
            self._send_json(200, {"ok": True, "role": "phasec-fault-proxy"})
            return
        if parsed.path != "/v1/effects" or method not in {"GET", "POST"}:
            self._send_json(404, {"error": "not found"})
            return

        raw_body = self._read_body() if method == "POST" else None
        body: dict[str, Any] | None = None
        if method == "POST":
            try:
                row = json.loads(raw_body.decode("utf-8")) if raw_body is not None else None
                body = row if isinstance(row, dict) else None
            except (UnicodeDecodeError, json.JSONDecodeError):
                body = None
        context_error = self._validate_context(body)
        if context_error:
            self.state.record({
                "type": "request_rejected",
                "method": method,
                "reason": context_error,
                "path": parsed.path,
                "request_at_ns": time.time_ns(),
            })
            self._send_json(403, {"error": "request outside configured trial"})
            return

        body_hash = hashlib.sha256(raw_body).hexdigest() if raw_body is not None else None
        self.state.record({
            "type": "proxy_request_received",
            "method": method,
            "path": parsed.path,
            "body_sha256": body_hash,
            "request_at_ns": time.time_ns(),
        })

        if method == "POST" and self.state.scenario == "timeout_before_send":
            with self.state.lock:
                self.state.suppressed_post_count += 1
            self.state.count_fault("timeout_before_send")
            self.state.record({
                "type": "fault_triggered",
                "fault": "timeout_before_send",
                "method": method,
                "forwarded": False,
                "event_at_ns": time.time_ns(),
            })
            # Keep the client waiting beyond its pinned timeout. The request is
            # never forwarded, and the Provider ledger therefore has no POST.
            time.sleep(self.state.client_timeout_seconds + 0.4)
            self._send_json(504, {"error": "injected pre-send timeout"})
            return

        if (
            method == "GET"
            and self.state.scenario == "readback_unavailable_after_commit"
            and self.state.committed
        ):
            with self.state.lock:
                self.state.blocked_get_count += 1
            self.state.count_fault("readback_unavailable_after_commit")
            self.state.record({
                "type": "fault_triggered",
                "fault": "readback_unavailable_after_commit",
                "method": method,
                "forwarded": False,
                "event_at_ns": time.time_ns(),
            })
            self._send_json(503, {"error": "injected authoritative readback outage"})
            return

        self.state.record({
            "type": "upstream_request_started",
            "method": method,
            "path": self.path,
            "forwarded": True,
            "event_at_ns": time.time_ns(),
        })
        try:
            status, upstream_headers, upstream_body = self._forward(method, self.path, raw_body)
        except Exception as exc:
            self.state.record({
                "type": "upstream_error",
                "method": method,
                "exception_type": type(exc).__name__,
                "event_at_ns": time.time_ns(),
            })
            self._send_json(502, {"error": "mock-provider upstream unavailable"})
            return

        parsed_body: dict[str, Any] = {}
        try:
            loaded = json.loads(upstream_body.decode("utf-8"))
            if isinstance(loaded, dict):
                parsed_body = loaded
        except (UnicodeDecodeError, json.JSONDecodeError):
            pass
        accepted = parsed_body.get("accepted") is True
        if method == "POST":
            with self.state.lock:
                self.state.forwarded_post_count += 1
                if status == 200 and accepted:
                    self.state.committed = True
        else:
            with self.state.lock:
                self.state.forwarded_get_count += 1
        self.state.record({
            "type": "upstream_response_received",
            "method": method,
            "status": status,
            "forwarded": True,
            "accepted": accepted,
            "response_sha256": hashlib.sha256(upstream_body).hexdigest(),
            "event_at_ns": time.time_ns(),
        })

        if method == "POST" and self.state.scenario == "ack_lost_after_commit" and status == 200 and accepted:
            self.state.count_fault("ack_lost_after_commit")
            self.state.record({
                "type": "fault_triggered",
                "fault": "ack_lost_after_commit",
                "method": method,
                "forwarded": True,
                "provider_ack_status": status,
                "provider_receipt_ref": parsed_body.get("receipt_ref"),
                "event_at_ns": time.time_ns(),
            })
            self.state.record({
                "type": "client_response_dropped",
                "method": method,
                "reason": "provider committed; ACK discarded before arm receipt",
                "event_at_ns": time.time_ns(),
            })
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
            self.state.record({
                "type": "client_disconnected",
                "method": method,
                "event_at_ns": time.time_ns(),
            })

    def do_POST(self) -> None:  # noqa: N802
        self._handle("POST")

    def do_GET(self) -> None:  # noqa: N802
        self._handle("GET")

    def do_PUT(self) -> None:  # noqa: N802
        self._send_json(405, {"error": "method not allowed"})

    def do_PATCH(self) -> None:  # noqa: N802
        self._send_json(405, {"error": "method not allowed"})

    def do_DELETE(self) -> None:  # noqa: N802
        self._send_json(405, {"error": "method not allowed"})

    def do_OPTIONS(self) -> None:  # noqa: N802
        self._send_json(405, {"error": "method not allowed"})

class LoopbackProxyServer(ThreadingHTTPServer):
    # Wait for delayed timeout/drop handlers to finish before closing the
    # append-only fault log.
    daemon_threads = False
    block_on_close = True
    allow_reuse_address = True


def serve_proxy(args: argparse.Namespace) -> int:
    provider_key = os.environ.get(args.proxy_key_env, "")
    if len(provider_key) < 32:
        raise ValueError("proxy internal key missing from restricted process environment")
    ledger = AppendOnlyLedger(args.proxy_log_path)
    try:
        state = ProxyState(
            trial_id=args.trial_id,
            arm=args.arm,
            seed=args.seed,
            scenario=args.scenario,
            provider_url=args.provider_url,
            proxy_key=provider_key,
            ledger=ledger,
            client_timeout_seconds=args.client_timeout_seconds,
        )
        server = LoopbackProxyServer(("127.0.0.1", args.port), ProxyHandler)
        server.proxy_state = state  # type: ignore[attr-defined]
        actual_port = int(server.server_address[1])
        print(json.dumps({
            "ready": True,
            "role": "fault_proxy",
            "trial_id": args.trial_id,
            "arm": args.arm,
            "port": actual_port,
        }, sort_keys=True), flush=True)
        try:
            server.serve_forever(poll_interval=0.1)
        except KeyboardInterrupt:
            pass
        finally:
            server.server_close()
            ledger.append({
                "type": "fault_proxy_stopped",
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


def read_proxy_ledger(path: str | Path) -> tuple[list[dict], str, bytes]:
    """Verify proxy telemetry before deriving upstream-call/barrier counts."""
    return read_verified_ledger(path)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    serve = sub.add_parser("serve", help="start one fault proxy for a trial")
    serve.add_argument("--trial-id", required=True)
    serve.add_argument("--arm", choices=ARMS, required=True)
    serve.add_argument("--seed", type=int, required=True)
    serve.add_argument("--scenario", choices=SCENARIOS, required=True)
    serve.add_argument("--proxy-log-path", required=True)
    serve.add_argument("--provider-url", required=True)
    serve.add_argument("--proxy-key-env", default="PHASEC_PROVIDER_PROXY_KEY")
    serve.add_argument("--port", type=int, required=True)
    serve.add_argument("--client-timeout-seconds", type=float, default=0.7)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        return serve_proxy(args)
    except (OSError, ValueError) as exc:
        print(f"phase-c fault proxy error: {exc}", file=__import__("sys").stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
