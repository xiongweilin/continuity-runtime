"""Independent Phase D trusted risk-decision source.

The process owns an append-only source history and exposes only a read-only
current-value endpoint to experiment arms. Publishing v1/v2 requires a
separate control key that must never be passed to an arm. Bind to loopback;
the orchestrator is responsible for pre-creating and restricting the ledger.
"""
from __future__ import annotations

import argparse
import hmac
import http.server
import json
import os
import re
import sys
import threading
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from experiments.phase_c.provider_service import (
    AppendOnlyLedger,
    LedgerIntegrityError,
    read_verified_ledger,
)

ARMS = ("continuity", "postgres_dbos")
FACTS = {"v1": "approved_for_pack", "v2": "fraud_hold"}
EVIDENCE_REF = "case-risk-v1-to-v2"
MAX_BODY_BYTES = 64 * 1024
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,119}$")


def _event_ref(role: str, trial_id: str, row: dict[str, Any]) -> str:
    return f"{role}:{trial_id}:{row['seq']}:sha256={row['hash']}"


def _json_bytes(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8") + b"\n"


def _read_json_body(handler: http.server.BaseHTTPRequestHandler) -> dict[str, Any] | None:
    length = handler.headers.get("Content-Length")
    if length is None or not length.isdecimal():
        return None
    size = int(length)
    if size < 0 or size > MAX_BODY_BYTES:
        return None
    raw = handler.rfile.read(size)
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


class SourceState:
    def __init__(
        self,
        *,
        trial_id: str,
        arm: str,
        control_key: str,
        ledger_path: str | Path,
        ledger: AppendOnlyLedger,
    ) -> None:
        if not _IDENTIFIER.fullmatch(trial_id):
            raise ValueError("trial-id must be a safe identifier")
        if arm not in ARMS:
            raise ValueError("invalid arm")
        if len(control_key) < 32:
            raise ValueError("PHASED_SOURCE_CONTROL_KEY must contain at least 32 characters")
        self.trial_id = trial_id
        self.arm = arm
        self.control_key = control_key
        self.ledger_path = Path(ledger_path)
        self.ledger = ledger
        self.lock = threading.RLock()
        self.events, _, _ = read_verified_ledger(self.ledger_path)
        self.revisions: list[tuple[str, str, str, str]] = []
        for row in self.events:
            event = row["event"]
            if event.get("trial_id", trial_id) != trial_id or event.get("arm", arm) != arm:
                raise LedgerIntegrityError("source ledger mixes trials or arms")
            if event.get("type") == "source_revision_published":
                revision = event.get("revision")
                expected_revision = "v1" if not self.revisions else "v2"
                if revision != expected_revision or len(self.revisions) >= 2:
                    raise LedgerIntegrityError("source history is not exactly monotone v1 then v2")
                if event.get("fact") != FACTS[expected_revision]:
                    raise LedgerIntegrityError("source fact does not match the frozen decision")
                if event.get("evidence_ref") != EVIDENCE_REF:
                    raise LedgerIntegrityError("source evidence reference does not match the manifest")
                if event.get("source_owner") != "trusted_decision_source":
                    raise LedgerIntegrityError("source revision lacks the trusted source owner identity")
                ref = _event_ref("source-event", trial_id, row)
                self.revisions.append((revision, event["fact"], event["evidence_ref"], ref))
        self.ledger.append({
            "type": "source_service_started",
            "schema_version": 1,
            "trial_id": trial_id,
            "arm": arm,
            "started_at_ns": time.time_ns(),
        })

    def authenticate(self, supplied: str) -> bool:
        return bool(supplied) and hmac.compare_digest(supplied, self.control_key)

    def log_denied_write(self, path: str, reason: str) -> None:
        with self.lock:
            self.ledger.append({
                "type": "source_write_denied",
                "trial_id": self.trial_id,
                "arm": self.arm,
                "path": path,
                "reason": reason,
                "occurred_at_ns": time.time_ns(),
            })

    def current(self) -> dict[str, Any] | None:
        with self.lock:
            if not self.revisions:
                return None
            revision, fact, evidence_ref, source_ref = self.revisions[-1]
            return {
                "trial_id": self.trial_id,
                "arm": self.arm,
                "revision": revision,
                "fact": fact,
                "evidence_ref": evidence_ref,
                "source_event_ref": source_ref,
            }

    def publish(self, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        if set(body) != {"revision", "fact", "evidence_ref"}:
            return self._reject("wrong_body_fields")
        revision = body.get("revision")
        fact = body.get("fact")
        evidence_ref = body.get("evidence_ref")
        with self.lock:
            next_revision = "v1" if not self.revisions else "v2"
            if len(self.revisions) >= 2 or revision != next_revision:
                return self._reject_locked("revision_not_monotone")
            if fact != FACTS[next_revision] or evidence_ref != EVIDENCE_REF:
                return self._reject_locked("decision_does_not_match_frozen_manifest")
            event = {
                "type": "source_revision_published",
                "schema_version": 1,
                "trial_id": self.trial_id,
                "arm": self.arm,
                "source_owner": "trusted_decision_source",
                "revision": next_revision,
                "fact": fact,
                "evidence_ref": evidence_ref,
                "published_at_ns": time.time_ns(),
            }
            row = self.ledger.append(event)
            source_ref = _event_ref("source-event", self.trial_id, row)
            self.revisions.append((next_revision, fact, evidence_ref, source_ref))
            return 201, {
                "accepted": True,
                "revision": next_revision,
                "fact": fact,
                "evidence_ref": evidence_ref,
                "source_event_ref": source_ref,
            }

    def _reject(self, reason: str) -> tuple[int, dict[str, Any]]:
        with self.lock:
            return self._reject_locked(reason)

    def _reject_locked(self, reason: str) -> tuple[int, dict[str, Any]]:
        self.ledger.append({
            "type": "source_publish_rejected",
            "trial_id": self.trial_id,
            "arm": self.arm,
            "reason": reason,
            "occurred_at_ns": time.time_ns(),
        })
        return 409, {"error": reason}


class SourceHandler(http.server.BaseHTTPRequestHandler):
    server_version = "PhaseDSource/1"
    sys_version = ""
    protocol_version = "HTTP/1.1"

    @property
    def state(self) -> SourceState:
        return self.server.source_state  # type: ignore[attr-defined]

    def log_message(self, _format: str, *_args: Any) -> None:
        return

    def _send_json(self, status: int, value: dict[str, Any]) -> None:
        payload = _json_bytes(value)
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        try:
            self.wfile.write(payload)
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_GET(self) -> None:  # noqa: N802
        path = urlsplit(self.path).path
        if path == "/healthz":
            self._send_json(200, {"ready": True, "role": "phase_d_source"})
            return
        if path != "/v1/source/current":
            self._send_json(404, {"error": "not found"})
            return
        current = self.state.current()
        if current is None:
            self._send_json(404, {"error": "source not yet published"})
        else:
            self._send_json(200, current)

    def do_POST(self) -> None:  # noqa: N802
        path = urlsplit(self.path).path
        if path != "/v1/source/publish":
            self.state.log_denied_write(path, "unsupported_control_path")
            self.close_connection = True
            self._send_json(404, {"error": "not found"})
            return
        if not self.state.authenticate(self.headers.get("X-PhaseD-Source-Key", "")):
            self.state.log_denied_write(path, "control_key_rejected")
            self.close_connection = True
            self._send_json(403, {"error": "source publication is not authorized"})
            return
        body = _read_json_body(self)
        if body is None:
            self.state.log_denied_write(path, "invalid_json_or_body_size")
            self.close_connection = True
            self._send_json(400, {"error": "expected a bounded JSON object"})
            return
        status, response = self.state.publish(body)
        self._send_json(status, response)

    def _method_denied(self) -> None:
        path = urlsplit(self.path).path
        self.state.log_denied_write(path, "method_not_allowed")
        self.close_connection = True
        self._send_json(405, {"error": "method not allowed"})

    do_PUT = _method_denied  # noqa: N815
    do_PATCH = _method_denied  # noqa: N815
    do_DELETE = _method_denied  # noqa: N815
    do_OPTIONS = _method_denied  # noqa: N815


class LoopbackHTTPServer(http.server.ThreadingHTTPServer):
    daemon_threads = False
    block_on_close = True
    allow_reuse_address = True


def serve(args: argparse.Namespace) -> int:
    if not 0 <= args.port <= 65535:
        raise ValueError("port must be between 0 and 65535")
    ledger = AppendOnlyLedger(args.ledger_path)
    try:
        state = SourceState(
            trial_id=args.trial_id,
            arm=args.arm,
            control_key=os.environ.get("PHASED_SOURCE_CONTROL_KEY", ""),
            ledger_path=args.ledger_path,
            ledger=ledger,
        )
        server = LoopbackHTTPServer(("127.0.0.1", args.port), SourceHandler)
        server.source_state = state  # type: ignore[attr-defined]
        actual_port = int(server.server_address[1])
        print(json.dumps({
            "ready": True,
            "role": "phase_d_source",
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
                "type": "source_service_stopped",
                "trial_id": args.trial_id,
                "arm": args.arm,
                "stopped_at_ns": time.time_ns(),
            })
    finally:
        ledger.close()
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trial-id", required=True)
    parser.add_argument("--arm", choices=ARMS, required=True)
    parser.add_argument("--ledger-path", required=True)
    parser.add_argument("--port", required=True, type=int)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        return serve(args)
    except (OSError, ValueError, LedgerIntegrityError) as exc:
        print(f"phase-d source error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
