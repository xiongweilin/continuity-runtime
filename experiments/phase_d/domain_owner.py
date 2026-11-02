"""Independent Phase D business-goal and authority owner.

The service owns the append-only business-goal and authority histories. It
reads the trusted source through its read-only API and verifies provider truth
directly with an oracle-only key. Neither key nor either ledger path belongs
in an experiment arm. The owner binds to loopback and has no arm database
connection.
"""
from __future__ import annotations

import argparse
import hmac
import http.client
import http.server
import ipaddress
import json
import os
import sys
import threading
import time
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urlsplit

from experiments.phase_c.provider_service import (
    AppendOnlyLedger,
    LedgerIntegrityError,
    read_verified_ledger,
)

ARMS = ("continuity", "postgres_dbos")
CASE_REF = "case/phase-d-one"
EVIDENCE_REF = "case-risk-v1-to-v2"
FACTS = {"v1": "approved_for_pack", "v2": "fraud_hold"}
GOALS = {"old": "prepare_shipment", "new": "quarantine_and_review"}
GOAL_PREFIX = (
    "goal_created", "evidence_revised", "goal_superseded", "handoff_authorized",
)
TERMINAL_GOALS = {"goal_satisfied", "goal_unresolved"}
MAX_BODY_BYTES = 64 * 1024
MAX_RESPONSE_BYTES = 1024 * 1024


class CommandError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


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


def _event_ref(role: str, trial_id: str, row: dict[str, Any]) -> str:
    return f"{role}:{trial_id}:{row['seq']}:sha256={row['hash']}"


def _check_loopback_url(value: str, field: str) -> None:
    parsed = urlsplit(value)
    if parsed.scheme != "http" or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError(f"{field} must be a plain HTTP loopback URL")
    if parsed.query or parsed.fragment:
        raise ValueError(f"{field} must not contain a query or fragment")
    if parsed.path not in ("", "/"):
        raise ValueError(f"{field} must be a URL origin without an application path")
    host = parsed.hostname.lower()
    if host == "localhost":
        return
    try:
        address = ipaddress.ip_address(host)
    except ValueError as exc:
        raise ValueError(f"{field} must use localhost or a literal loopback IP") from exc
    if not address.is_loopback:
        raise ValueError(f"{field} must resolve only to loopback addresses")


def _http_get_json(url: str, headers: dict[str, str]) -> dict[str, Any]:
    parsed = urlsplit(url)
    if parsed.scheme != "http" or not parsed.hostname:
        raise ValueError("only HTTP loopback service URLs are supported")
    connection = http.client.HTTPConnection(parsed.hostname, parsed.port or 80, timeout=3.0)
    try:
        target = parsed.path or "/"
        if parsed.query:
            target += "?" + parsed.query
        connection.request("GET", target, headers=headers)
        response = connection.getresponse()
        raw = response.read(MAX_RESPONSE_BYTES + 1)
        if len(raw) > MAX_RESPONSE_BYTES:
            raise ValueError("service response exceeded size limit")
        if response.status != 200:
            raise ValueError(f"independent readback returned HTTP {response.status}")
        try:
            result = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("independent readback returned invalid JSON") from exc
        if not isinstance(result, dict):
            raise ValueError("independent readback must be a JSON object")
        return result
    finally:
        connection.close()


def _empty_body(body: dict[str, Any]) -> None:
    if body:
        raise CommandError(400, "command expects an empty JSON object")


class DomainOwnerState:
    def __init__(
        self,
        *,
        trial_id: str,
        arm: str,
        goal_ledger_path: str | Path,
        authority_ledger_path: str | Path,
        source_url: str,
        provider_url: str,
        owner_key: str,
        provider_oracle_key: str,
        goal_ledger: AppendOnlyLedger,
        authority_ledger: AppendOnlyLedger,
    ) -> None:
        if not trial_id or len(trial_id) > 120:
            raise ValueError("trial-id must be a nonempty identifier of at most 120 characters")
        if arm not in ARMS:
            raise ValueError("invalid arm")
        if len(owner_key) < 32:
            raise ValueError("PHASED_OWNER_CONTROL_KEY must contain at least 32 characters")
        if len(provider_oracle_key) < 32:
            raise ValueError("PHASED_PROVIDER_ORACLE_KEY must contain at least 32 characters")
        if hmac.compare_digest(owner_key, provider_oracle_key):
            raise ValueError("domain owner and provider oracle credentials must be distinct")
        _check_loopback_url(source_url, "source-url")
        _check_loopback_url(provider_url, "provider-url")
        self.trial_id = trial_id
        self.arm = arm
        self.goal_ledger_path = Path(goal_ledger_path)
        self.authority_ledger_path = Path(authority_ledger_path)
        self.source_url = source_url.rstrip("/")
        self.provider_url = provider_url.rstrip("/")
        self.owner_key = owner_key
        self.provider_oracle_key = provider_oracle_key
        self.goal_ledger = goal_ledger
        self.authority_ledger = authority_ledger
        self.lock = threading.RLock()

        self.goal_rows, _, _ = read_verified_ledger(self.goal_ledger_path)
        self.authority_rows, _, _ = read_verified_ledger(self.authority_ledger_path)
        self.goal_refs: dict[str, str] = {}
        self.goal_events = [row["event"] for row in self.goal_rows]
        self._validate_goal_history()
        self.a_grants_ref: str | None = None
        self.a_revoked_ref: str | None = None
        self.pack_readback: dict[str, Any] | None = None
        self.handoff_decision_ref: str | None = None
        self.handoff_ref: str | None = None
        self.b_grant_ref: str | None = None
        self._rebuild_authority_state()
        self.authority_ledger.append({
            "type": "domain_owner_service_started",
            "schema_version": 1,
            "trial_id": trial_id,
            "arm": arm,
            "started_at_ns": time.time_ns(),
        })

    def _validate_goal_history(self) -> None:
        event_types: list[str] = []
        for row in self.goal_rows:
            event = row["event"]
            if event.get("trial_id") != self.trial_id or event.get("arm") != self.arm:
                raise LedgerIntegrityError("goal ledger mixes trials or arms")
            kind = event.get("type")
            if kind not in (*GOAL_PREFIX, *TERMINAL_GOALS):
                raise LedgerIntegrityError("goal ledger contains a non-lifecycle event")
            event_types.append(kind)
            self.goal_refs[kind] = _event_ref("goal-event", self.trial_id, row)
        if len(event_types) <= len(GOAL_PREFIX):
            if event_types != list(GOAL_PREFIX[:len(event_types)]):
                raise LedgerIntegrityError("goal history is not in the frozen causal order")
        elif (
            len(event_types) != len(GOAL_PREFIX) + 1
            or event_types[:len(GOAL_PREFIX)] != list(GOAL_PREFIX)
            or event_types[-1] not in TERMINAL_GOALS
        ):
            raise LedgerIntegrityError("goal history has an invalid terminal transition")
        for index, event in enumerate(self.goal_events):
            if index == 0 and (
                event.get("goal_id") != GOALS["old"]
                or event.get("decision_maker") != "business-owner"
                or event.get("basis_revision") != "v1"
                or event.get("basis_fact") != FACTS["v1"]
                or event.get("evidence_ref") != EVIDENCE_REF
                or not event.get("source_event_ref")
            ):
                raise LedgerIntegrityError("goal_created does not preserve the v1 basis")
            if index == 1 and (
                event.get("goal_id") != GOALS["old"]
                or event.get("decision_source") != "trusted_decision_source"
                or event.get("prior_revision") != "v1"
                or event.get("revision") != "v2"
                or event.get("fact") != FACTS["v2"]
                or event.get("evidence_ref") != EVIDENCE_REF
                or not event.get("source_event_ref")
            ):
                raise LedgerIntegrityError("evidence_revised does not preserve the v2 source fact")
            if index == 2 and (
                event.get("decision_maker") != "business-owner"
                or event.get("old_goal") != GOALS["old"]
                or event.get("new_goal") != GOALS["new"]
                or event.get("basis_revision") != "v2"
                or event.get("basis_fact") != FACTS["v2"]
                or not event.get("source_event_ref")
            ):
                raise LedgerIntegrityError("goal_superseded does not preserve the replacement decision")
            if index == 3 and (
                event.get("goal_id") != GOALS["new"]
                or event.get("from_actor") != "agent-a"
                or event.get("actor") != "agent-b"
                or event.get("basis_revision") != "v2"
                or not event.get("authority_ref")
            ):
                raise LedgerIntegrityError("handoff record does not authorize Agent B for the v2 goal")
            if index == 4:
                if event.get("goal_id") != GOALS["new"] or event.get("provider_counts") != {
                    "pack": 1, "ship": 0,
                    "quarantine": 1 if event.get("type") == "goal_satisfied" else 0,
                }:
                    raise LedgerIntegrityError("terminal goal record does not match verified provider truth")
                if event.get("type") == "goal_satisfied" and event.get("goal_status") != "satisfied":
                    raise LedgerIntegrityError("goal_satisfied has an invalid status")
                if event.get("decision_maker") != "business-owner":
                    raise LedgerIntegrityError("terminal outcome lacks a domain owner decision")
                if event.get("type") == "goal_unresolved" and (
                    event.get("goal_status") != "unresolved"
                    or not isinstance(event.get("crash_barrier_ref"), str)
                    or not event["crash_barrier_ref"].strip()
                ):
                    raise LedgerIntegrityError("goal_unresolved lacks its crash barrier evidence")
                if not event.get("provider_readback_ref"):
                    raise LedgerIntegrityError("terminal goal record lacks provider readback evidence")

    def _rebuild_authority_state(self) -> None:
        for row in self.authority_rows:
            event = row["event"]
            if event.get("trial_id") != self.trial_id or event.get("arm") != self.arm:
                raise LedgerIntegrityError("authority ledger mixes trials or arms")
            kind = event.get("type")
            ref = _event_ref("authority-event", self.trial_id, row)
            if kind == "agent_a_grants_issued":
                if (
                    event.get("principal") != "agent-a"
                    or event.get("actions") != ["pack", "ship"]
                    or event.get("required_source_revision") != "v1"
                ):
                    raise LedgerIntegrityError("Agent A grant record exceeds its frozen v1 scope")
                self.a_grants_ref = ref
            elif kind == "agent_a_write_grants_revoked":
                if (
                    event.get("principal") != "agent-a"
                    or event.get("actions") != ["pack", "ship"]
                    or not isinstance(event.get("stale_dispatch_ref"), str)
                    or not event["stale_dispatch_ref"].strip()
                ):
                    raise LedgerIntegrityError("Agent A revocation lacks its stale-dispatch evidence")
                self.a_revoked_ref = ref
            elif kind == "pack_readback_verified":
                if (
                    event.get("counts") != {"pack": 1, "ship": 0, "quarantine": 0}
                    or not isinstance(event.get("provider_readback_ref"), str)
                    or not event["provider_readback_ref"].strip()
                ):
                    raise LedgerIntegrityError("stored pack verification does not match provider truth")
                self.pack_readback = {
                    "provider_readback_ref": event.get("provider_readback_ref"),
                    "counts": event.get("counts"),
                    "authority_ref": ref,
                }
            elif kind == "handoff_decision_approved":
                if (
                    event.get("from_actor") != "agent-a"
                    or event.get("to_actor") != "agent-b"
                    or event.get("goal_id") != GOALS["new"]
                    or event.get("basis_revision") != "v2"
                ):
                    raise LedgerIntegrityError("handoff authority record exceeds the frozen actor/goal scope")
                self.handoff_decision_ref = ref
            elif kind == "agent_b_quarantine_grant_issued":
                if (
                    event.get("principal") != "agent-b"
                    or event.get("actions") != ["quarantine"]
                    or event.get("required_source_revision") != "v2"
                    or event.get("goal_id") != GOALS["new"]
                ):
                    raise LedgerIntegrityError("Agent B grant exceeds its frozen v2 quarantine scope")
                self.b_grant_ref = ref
        if len(self.goal_rows) >= 4:
            self.handoff_ref = self.goal_refs.get("handoff_authorized")

    def authenticate(self, supplied: str) -> bool:
        return bool(supplied) and hmac.compare_digest(supplied, self.owner_key)

    def _append_authority(self, event: dict[str, Any]) -> tuple[dict[str, Any], str]:
        payload = {
            "schema_version": 1,
            "trial_id": self.trial_id,
            "arm": self.arm,
            "occurred_at_ns": time.time_ns(),
            **event,
        }
        row = self.authority_ledger.append(payload)
        self.authority_rows.append(row)
        return row, _event_ref("authority-event", self.trial_id, row)

    def log_denied_write(self, path: str, reason: str) -> None:
        with self.lock:
            self._append_authority({
                "type": "domain_write_denied",
                "path": path,
                "reason": reason,
            })

    def log_verification_failure(self, path: str) -> None:
        with self.lock:
            self._append_authority({
                "type": "independent_verification_unavailable",
                "path": path,
            })

    def _reject(self, path: str, reason: str, status: int = 409) -> None:
        self._append_authority({
            "type": "domain_command_rejected",
            "path": path,
            "reason": reason,
        })
        raise CommandError(status, reason)

    def _append_goal(self, event: dict[str, Any]) -> str:
        row = self.goal_ledger.append({
            "schema_version": 1,
            "trial_id": self.trial_id,
            "arm": self.arm,
            "occurred_at_ns": time.time_ns(),
            **event,
        })
        self.goal_rows.append(row)
        self.goal_events.append(row["event"])
        ref = _event_ref("goal-event", self.trial_id, row)
        self.goal_refs[event["type"]] = ref
        return ref

    def _source_current(self) -> dict[str, Any]:
        result = _http_get_json(
            self.source_url + "/v1/source/current",
            {"Accept": "application/json", "Cache-Control": "no-cache"},
        )
        expected_fields = {
            "trial_id", "arm", "revision", "fact", "evidence_ref", "source_event_ref",
        }
        if set(result) != expected_fields:
            raise ValueError("source current response fields do not match the domain contract")
        if result["trial_id"] != self.trial_id or result["arm"] != self.arm:
            raise ValueError("source current response belongs to another trial or arm")
        revision = result["revision"]
        if revision not in FACTS or result["fact"] != FACTS[revision]:
            raise ValueError("source current response does not match the frozen decision")
        if result["evidence_ref"] != EVIDENCE_REF or not result["source_event_ref"]:
            raise ValueError("source current response has invalid evidence provenance")
        return result

    def _provider_counts(self) -> tuple[dict[str, int], str]:
        target = self.provider_url + "/v1/effects?" + urlencode({"resource_ref": CASE_REF})
        result = _http_get_json(target, {
            "Accept": "application/json",
            "X-PhaseD-Provider-Oracle-Key": self.provider_oracle_key,
            "X-PhaseD-Trial-ID": self.trial_id,
            "X-PhaseD-Arm": self.arm,
        })
        counts = result.get("action_counts")
        ref = result.get("evidence_ref") or result.get("ledger_ref")
        if not isinstance(counts, dict) or set(counts) != {"pack", "ship", "quarantine"}:
            raise ValueError("provider readback must return all three action counts")
        if any(type(counts[action]) is not int or counts[action] < 0 for action in counts):
            raise ValueError("provider readback action counts must be nonnegative integers")
        if not isinstance(ref, str) or not ref.strip():
            raise ValueError("provider readback lacks an independent ledger reference")
        if result.get("trial_id", self.trial_id) != self.trial_id:
            raise ValueError("provider readback belongs to another trial")
        if result.get("arm", self.arm) != self.arm:
            raise ValueError("provider readback belongs to another arm")
        return {key: counts[key] for key in ("pack", "ship", "quarantine")}, ref

    def _goal_prefix_has(self, count: int) -> bool:
        return [event.get("type") for event in self.goal_events] == list(GOAL_PREFIX[:count])

    def command(self, path: str, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        with self.lock:
            if path == "/v1/owner/grant-agent-a":
                _empty_body(body)
                if not self._goal_prefix_has(1) or self.a_grants_ref is not None:
                    self._reject(path, "agent-a grant requires a newly created v1 goal")
                source = self._source_current()
                if source["revision"] != "v1":
                    self._reject(path, "agent-a grants are limited to source v1")
                _, authority_ref = self._append_authority({
                    "type": "agent_a_grants_issued",
                    "actor": "business-owner",
                    "principal": "agent-a",
                    "actions": ["pack", "ship"],
                    "required_source_revision": "v1",
                    "source_event_ref": source["source_event_ref"],
                })
                self.a_grants_ref = authority_ref
                return 201, {"accepted": True, "authority_ref": authority_ref}

            if path == "/v1/owner/verify-pack":
                _empty_body(body)
                source = self._source_current()
                if not self._goal_prefix_has(1) or self.a_grants_ref is None:
                    self._reject(path, "pack verification requires the initial Agent A grant")
                if source["revision"] != "v1":
                    self._reject(path, "pack must be verified against source v1")
                counts, provider_ref = self._provider_counts()
                if counts != {"pack": 1, "ship": 0, "quarantine": 0}:
                    self._reject(path, "provider does not independently show exactly one pack and no later action")
                _, authority_ref = self._append_authority({
                    "type": "pack_readback_verified",
                    "actor": "business-owner",
                    "provider_readback_ref": provider_ref,
                    "counts": counts,
                    "source_event_ref": source["source_event_ref"],
                })
                self.pack_readback = {
                    "provider_readback_ref": provider_ref,
                    "counts": counts,
                    "authority_ref": authority_ref,
                }
                return 200, {
                    "verified": True,
                    "counts": counts,
                    "provider_readback_ref": provider_ref,
                    "authority_ref": authority_ref,
                }

            if path == "/v1/owner/goal-created":
                _empty_body(body)
                if not self._goal_prefix_has(0):
                    self._reject(path, "initial goal already exists")
                source = self._source_current()
                if source["revision"] != "v1":
                    self._reject(path, "prepare_shipment must be created against source v1")
                goal_ref = self._append_goal({
                    "type": "goal_created",
                "decision_maker": "business-owner",
                    "goal_id": GOALS["old"],
                    "goal_status": "active",
                    "basis_revision": "v1",
                    "basis_fact": FACTS["v1"],
                    "evidence_ref": source["evidence_ref"],
                    "source_event_ref": source["source_event_ref"],
                })
                return 201, {"accepted": True, "goal_event_ref": goal_ref}

            if path == "/v1/owner/evidence-revised":
                _empty_body(body)
                if not self._goal_prefix_has(1) or self.pack_readback is None:
                    self._reject(path, "evidence revision requires the original goal and verified pack")
                source = self._source_current()
                if source["revision"] != "v2":
                    self._reject(path, "trusted source has not published v2")
                goal_ref = self._append_goal({
                    "type": "evidence_revised",
                "decision_source": "trusted_decision_source",
                    "goal_id": GOALS["old"],
                    "prior_revision": "v1",
                    "revision": "v2",
                    "fact": FACTS["v2"],
                    "evidence_ref": source["evidence_ref"],
                    "source_event_ref": source["source_event_ref"],
                })
                return 201, {"accepted": True, "goal_event_ref": goal_ref}

            if path == "/v1/owner/supersede":
                _empty_body(body)
                if not self._goal_prefix_has(2):
                    self._reject(path, "goal supersession requires the v2 evidence revision")
                source = self._source_current()
                if source["revision"] != "v2":
                    self._reject(path, "goal supersession basis is not current source v2")
                goal_ref = self._append_goal({
                    "type": "goal_superseded",
                "decision_maker": "business-owner",
                    "old_goal": GOALS["old"],
                    "new_goal": GOALS["new"],
                    "basis_revision": "v2",
                    "basis_fact": FACTS["v2"],
                    "evidence_ref": source["evidence_ref"],
                    "source_event_ref": source["source_event_ref"],
                    "prior_goal_event_ref": self.goal_refs["goal_created"],
                })
                return 201, {"accepted": True, "goal_event_ref": goal_ref}

            if path == "/v1/owner/revoke-agent-a":
                if set(body) != {"stale_dispatch_ref"} or not isinstance(body.get("stale_dispatch_ref"), str) or not body["stale_dispatch_ref"].strip():
                    raise CommandError(400, "revoke-agent-a requires a nonempty stale_dispatch_ref")
                if not self._goal_prefix_has(3) or self.a_grants_ref is None or self.a_revoked_ref is not None:
                    self._reject(path, "Agent A revocation requires a superseded goal and active original grants")
                source = self._source_current()
                if source["revision"] != "v2":
                    self._reject(path, "Agent A write authority must be revoked after source v2")
                _, authority_ref = self._append_authority({
                    "type": "agent_a_write_grants_revoked",
                    "actor": "business-owner",
                    "principal": "agent-a",
                    "actions": ["pack", "ship"],
                    "reason": "goal_superseded_after_v2",
                    "stale_dispatch_ref": body["stale_dispatch_ref"],
                })
                self.a_revoked_ref = authority_ref
                return 200, {"accepted": True, "authority_ref": authority_ref}

            if path == "/v1/owner/handoff":
                _empty_body(body)
                if not self._goal_prefix_has(3) or self.a_revoked_ref is None or self.handoff_ref is not None:
                    self._reject(path, "handoff requires supersession and Agent A grant revocation")
                source = self._source_current()
                if source["revision"] != "v2":
                    self._reject(path, "handoff basis is not current source v2")
                _, authority_ref = self._append_authority({
                    "type": "handoff_decision_approved",
                    "actor": "business-owner",
                    "from_actor": "agent-a",
                    "to_actor": "agent-b",
                    "goal_id": GOALS["new"],
                    "basis_revision": "v2",
                    "basis_source_event_ref": source["source_event_ref"],
                    "revocation_ref": self.a_revoked_ref,
                })
                self.handoff_decision_ref = authority_ref
                goal_ref = self._append_goal({
                    "type": "handoff_authorized",
                    "goal_id": GOALS["new"],
                    "from_actor": "agent-a",
                    "actor": "agent-b",
                    "basis_revision": "v2",
                    "authority_ref": authority_ref,
                })
                self.handoff_ref = goal_ref
                return 201, {
                    "accepted": True,
                    "goal_event_ref": goal_ref,
                    "authority_ref": authority_ref,
                }

            if path == "/v1/owner/grant-agent-b":
                _empty_body(body)
                if self.handoff_ref is None or self.b_grant_ref is not None:
                    self._reject(path, "Agent B grant requires an authorized handoff")
                source = self._source_current()
                if source["revision"] != "v2":
                    self._reject(path, "Agent B grant is limited to source v2")
                _, authority_ref = self._append_authority({
                    "type": "agent_b_quarantine_grant_issued",
                    "actor": "business-owner",
                    "principal": "agent-b",
                    "actions": ["quarantine"],
                    "required_source_revision": "v2",
                    "goal_id": GOALS["new"],
                    "handoff_ref": self.handoff_ref,
                    "source_event_ref": source["source_event_ref"],
                })
                self.b_grant_ref = authority_ref
                return 201, {"accepted": True, "authority_ref": authority_ref}

            if path == "/v1/owner/finalize":
                allowed = set(body) <= {"crash_barrier_ref"}
                if not allowed:
                    raise CommandError(400, "finalize accepts only crash_barrier_ref")
                if self.handoff_ref is None or self.b_grant_ref is None or self.pack_readback is None:
                    self._reject(path, "new goal cannot be finalized before Agent B authorization")
                if len(self.goal_events) != len(GOAL_PREFIX):
                    self._reject(path, "goal already has a terminal result or is not ready")
                counts, provider_ref = self._provider_counts()
                if counts == {"pack": 1, "ship": 0, "quarantine": 1}:
                    goal_status = "goal_satisfied"
                    terminal_event = {
                        "type": goal_status,
                        "decision_maker": "business-owner",
                        "goal_id": GOALS["new"],
                        "goal_status": "satisfied",
                        "provider_counts": counts,
                        "provider_readback_ref": provider_ref,
                        "verifier": "independent_provider_readback",
                    }
                elif counts == {"pack": 1, "ship": 0, "quarantine": 0}:
                    barrier_ref = body.get("crash_barrier_ref")
                    if not isinstance(barrier_ref, str) or not barrier_ref.strip():
                        self._reject(path, "zero quarantine requires a nonempty crash_barrier_ref", 422)
                    goal_status = "goal_unresolved"
                    terminal_event = {
                        "type": goal_status,
                        "decision_maker": "business-owner",
                        "goal_id": GOALS["new"],
                        "goal_status": "unresolved",
                        "provider_counts": counts,
                        "provider_readback_ref": provider_ref,
                        "crash_barrier_ref": barrier_ref,
                        "verifier": "independent_provider_readback_and_crash_barrier",
                    }
                else:
                    self._reject(path, "provider counts violate the frozen goal outcome conditions", 422)
                goal_ref = self._append_goal(terminal_event)
                _, audit_ref = self._append_authority({
                    "type": "domain_goal_outcome_verified",
                    "goal_status": goal_status.removeprefix("goal_"),
                    "provider_counts": counts,
                    "provider_readback_ref": provider_ref,
                    "goal_event_ref": goal_ref,
                })
                return 200, {
                    "goal_status": goal_status.removeprefix("goal_"),
                    "goal_event_ref": goal_ref,
                    "provider_counts": counts,
                    "provider_readback_ref": provider_ref,
                    "verifier_ref": audit_ref,
                }

            self._reject(path, "unknown owner command", 404)

    def projection(self, actor: str) -> tuple[int, dict[str, Any]]:
        with self.lock:
            if actor != "agent-b":
                self.log_denied_write("/v1/owner/projection", "projection_actor_not_authorized")
                return 403, {"error": "projection is available only to agent-b"}
            if self.handoff_ref is None or self.b_grant_ref is None or self.pack_readback is None:
                return 409, {"error": "Agent B projection is not ready"}
            source = self._source_current()
            if source["revision"] != "v2":
                return 409, {"error": "Agent B projection is not based on current v2 evidence"}
            return 200, {
                "actor": "agent-b",
                "goal": {"id": GOALS["new"], "status": "active"},
                "basis": {
                    "revision": "v2",
                    "fact": FACTS["v2"],
                    "evidence_ref": source["evidence_ref"],
                    "source_event_ref": source["source_event_ref"],
                },
                "verified_prior_work": [{
                    "action": "pack",
                    "count": 1,
                    "provider_readback_ref": self.pack_readback["provider_readback_ref"],
                }],
                "handoff_ref": self.handoff_ref,
                "authority_ref": self.b_grant_ref,
            }


class DomainOwnerHandler(http.server.BaseHTTPRequestHandler):
    server_version = "PhaseDDomainOwner/1"
    sys_version = ""
    protocol_version = "HTTP/1.1"

    @property
    def state(self) -> DomainOwnerState:
        return self.server.domain_owner_state  # type: ignore[attr-defined]

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
        parsed = urlsplit(self.path)
        if parsed.path == "/healthz":
            self._send_json(200, {"ready": True, "role": "phase_d_domain_owner"})
            return
        if parsed.path != "/v1/owner/projection":
            self._send_json(404, {"error": "not found"})
            return
        query = parse_qs(parsed.query, keep_blank_values=True)
        if set(query) != {"actor"} or len(query["actor"]) != 1:
            self._send_json(400, {"error": "exactly one actor query is required"})
            return
        status, value = self.state.projection(query["actor"][0])
        self._send_json(status, value)

    def do_POST(self) -> None:  # noqa: N802
        path = urlsplit(self.path).path
        if not path.startswith("/v1/owner/"):
            self.close_connection = True
            self._send_json(404, {"error": "not found"})
            return
        if not self.state.authenticate(self.headers.get("X-PhaseD-Owner-Key", "")):
            self.state.log_denied_write(path, "control_key_rejected")
            self.close_connection = True
            self._send_json(403, {"error": "domain owner command is not authorized"})
            return
        body = _read_json_body(self)
        if body is None:
            self.state.log_denied_write(path, "invalid_json_or_body_size")
            self.close_connection = True
            self._send_json(400, {"error": "expected a bounded JSON object"})
            return
        try:
            status, value = self.state.command(path, body)
        except CommandError as exc:
            self._send_json(exc.status, {"error": exc.message})
            return
        except (OSError, ValueError, http.client.HTTPException, TimeoutError):
            self.state.log_verification_failure(path)
            self._send_json(503, {"error": "independent source/provider verification unavailable"})
            return
        self._send_json(status, value)

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
    if args.goal_ledger_path == args.authority_ledger_path:
        raise ValueError("goal and authority ledgers must be separate files")
    if os.path.samefile(args.goal_ledger_path, args.authority_ledger_path):
        raise ValueError("goal and authority ledgers must be distinct files")
    goal_ledger = AppendOnlyLedger(args.goal_ledger_path)
    try:
        authority_ledger = AppendOnlyLedger(args.authority_ledger_path)
    except Exception:
        goal_ledger.close()
        raise
    try:
        state = DomainOwnerState(
            trial_id=args.trial_id,
            arm=args.arm,
            goal_ledger_path=args.goal_ledger_path,
            authority_ledger_path=args.authority_ledger_path,
            source_url=args.source_url,
            provider_url=args.provider_url,
            owner_key=os.environ.get("PHASED_OWNER_CONTROL_KEY", ""),
            provider_oracle_key=os.environ.get("PHASED_PROVIDER_ORACLE_KEY", ""),
            goal_ledger=goal_ledger,
            authority_ledger=authority_ledger,
        )
        server = LoopbackHTTPServer(("127.0.0.1", args.port), DomainOwnerHandler)
        server.domain_owner_state = state  # type: ignore[attr-defined]
        actual_port = int(server.server_address[1])
        print(json.dumps({
            "ready": True,
            "role": "phase_d_domain_owner",
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
            authority_ledger.append({
                "type": "domain_owner_service_stopped",
                "trial_id": args.trial_id,
                "arm": args.arm,
                "stopped_at_ns": time.time_ns(),
            })
    finally:
        authority_ledger.close()
        goal_ledger.close()
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trial-id", required=True)
    parser.add_argument("--arm", choices=ARMS, required=True)
    parser.add_argument("--goal-ledger-path", required=True)
    parser.add_argument("--authority-ledger-path", required=True)
    parser.add_argument("--source-url", required=True)
    parser.add_argument("--provider-url", required=True)
    parser.add_argument("--port", required=True, type=int)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        return serve(args)
    except (OSError, ValueError, LedgerIntegrityError) as exc:
        print(f"phase-d domain owner error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
