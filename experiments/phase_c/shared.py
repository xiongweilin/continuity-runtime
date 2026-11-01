"""Shared, provider-agnostic types and instrumentation for Phase C arms.

The baseline arm may import this module, but it must not import ``continuity``
or use Continuity tables. The actual provider ledger and fault schedule live in
separate processes and are never mounted into an arm's readable filesystem.
"""
from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import httpx

ReadbackDisposition = Literal["present", "absent", "unknown"]


@dataclass(frozen=True)
class PhaseCReceipt:
    accepted: bool
    receipt_ref: str | None = None


@dataclass(frozen=True)
class PhaseCReadback:
    disposition: ReadbackDisposition
    evidence_ref: str | None = None
    reliable_no_effect: bool = False
    value: int | None = None


@dataclass(frozen=True)
class PhaseCOutcome:
    satisfied: bool | None
    evidence_ref: str | None = None


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def safe_component(value: str) -> str:
    """Make an identifier safe for a filename without retaining arbitrary text."""
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("._-")
    if not cleaned or len(cleaned) > 120:
        raise ValueError("invalid Phase C identifier")
    return cleaned


class PhaseCBarrier:
    """One-way arm-to-harness barriers.

    Arms can append event files under ``event_dir``. The harness is the only
    process with write permission to ``release_dir``. The caller must verify
    the corresponding PostgreSQL row independently before releasing a
    reservation barrier or killing the worker.
    """

    def __init__(
        self,
        *,
        trial_id: str,
        event_dir: str | Path,
        release_dir: str | Path,
        timeout_seconds: float = 30.0,
        poll_seconds: float = 0.01,
    ) -> None:
        self.trial_id = safe_component(trial_id)
        self.event_dir = Path(event_dir)
        self.release_dir = Path(release_dir)
        self.timeout_seconds = timeout_seconds
        self.poll_seconds = poll_seconds

    def event_path(self, stage: str, operation_id: str) -> Path:
        return self.event_dir / f"{self.trial_id}__{safe_component(stage)}__{safe_component(operation_id)}.json"

    def release_path(self, stage: str, operation_id: str) -> Path:
        return self.release_dir / f"{self.trial_id}__{safe_component(stage)}__{safe_component(operation_id)}.release"

    def announce(self, stage: str, operation_id: str) -> dict[str, Any]:
        event_path = self.event_path(stage, operation_id)
        event_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "trial_id": self.trial_id,
            "stage": stage,
            "operation_id": operation_id,
            "pid": os.getpid(),
            "announced_monotonic_ns": time.monotonic_ns(),
        }
        # DBOS may replay an unfinished step after a worker crash. Preserve
        # the original event rather than replacing its timestamp on replay.
        try:
            fd = os.open(event_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            prior = json.loads(event_path.read_text(encoding="utf-8"))
            if any(prior.get(k) != payload.get(k) for k in ("trial_id", "stage", "operation_id")):
                raise RuntimeError("Phase C barrier event identity mismatch")
        else:
            try:
                os.write(fd, (canonical_json(payload) + "\n").encode("utf-8"))
                os.fsync(fd)
            finally:
                os.close(fd)
        return payload

    def announce_and_wait(self, stage: str, operation_id: str) -> None:
        self.announce(stage, operation_id)
        release = self.release_path(stage, operation_id)
        deadline = time.monotonic() + self.timeout_seconds
        while time.monotonic() < deadline:
            if release.is_file():
                return
            time.sleep(self.poll_seconds)
        raise TimeoutError(f"Phase C harness did not release barrier {stage}")


class PhaseCProviderAdapter:
    """Client for the fault-proxy only; it has no provider key or ledger path."""

    def __init__(
        self,
        *,
        base_url: str,
        trial_id: str,
        arm: str,
        barrier: PhaseCBarrier | None = None,
        timeout_seconds: float = 0.7,
    ) -> None:
        if arm not in {"continuity", "postgres_dbos"}:
            raise ValueError("invalid Phase C arm")
        self.base_url = base_url.rstrip("/")
        self.trial_id = trial_id
        self.arm = arm
        self.barrier = barrier
        self.timeout_seconds = timeout_seconds
        self.attempted_posts = 0
        self.attempted_gets = 0

    def execute(self, intent: Any) -> PhaseCReceipt:
        operation_id = _intent_value(intent, "operation_id")
        if self.barrier is not None:
            # Continuity calls adapter.execute only after its reservation
            # transaction commits. Baseline receives the same callback point.
            self.barrier.announce_and_wait("reserved_before_send", operation_id)
        self.attempted_posts += 1
        payload = {
            "trial_id": self.trial_id,
            "operation_id": operation_id,
            "principal": _intent_value(intent, "principal"),
            "provider": _intent_value(intent, "provider"),
            "resource_ref": _intent_value(intent, "resource_ref"),
            "action": _intent_value(intent, "action"),
            "payload": dict(_intent_value(intent, "payload")),
            "effect_key": _intent_value(intent, "effect_key"),
        }
        headers = self._headers()
        with httpx.Client(trust_env=False, timeout=self.timeout_seconds) as client:
            response = client.post(self.base_url + "/v1/effects", json=payload, headers=headers)
            response.raise_for_status()
            row = response.json()
        return PhaseCReceipt(bool(row.get("accepted")), row.get("receipt_ref"))

    def readback(self, intent: Any) -> PhaseCReadback:
        self.attempted_gets += 1
        resource = str(_intent_value(intent, "resource_ref"))
        headers = self._headers()
        try:
            with httpx.Client(trust_env=False, timeout=self.timeout_seconds) as client:
                response = client.get(
                    self.base_url + "/v1/effects",
                    params={"resource_ref": resource},
                    headers=headers,
                )
                response.raise_for_status()
                row = response.json()
        except httpx.HTTPError:
            # An unavailable read is not evidence of absence and never permits
            # a retry of an ambiguous effect.
            return PhaseCReadback("unknown")
        present = row.get("effect_present") is True
        evidence = row.get("evidence_ref")
        value = row.get("value")
        if type(value) is not int or not isinstance(evidence, str) or not evidence:
            return PhaseCReadback("unknown")
        return PhaseCReadback("present" if present else "absent", evidence, False, value)

    def _headers(self) -> dict[str, str]:
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-PhaseC-Trial-ID": self.trial_id,
            "X-PhaseC-Arm": self.arm,
        }


class PhaseCGoalVerifier:
    """Independent domain rule: the simulated counter must equal exactly 1."""

    def verify(self, intent: Any, effect: Any) -> PhaseCOutcome:
        if getattr(effect, "disposition", None) != "present":
            return PhaseCOutcome(None)
        reference = getattr(effect, "evidence_ref", None)
        value = getattr(effect, "value", None)
        if type(value) is not int:
            # Continuity's DomainVerifier contract intentionally receives only
            # a readback evidence reference, not adapter-owned data. The mock
            # Provider embeds the observed counter value in its independently
            # reviewable evidence reference for the verifier to qualify.
            match = re.search(r"(?:^|[:;])value=(\d+)(?:[:;]|$)", reference)
            value = int(match.group(1)) if match else None
        if not isinstance(reference, str) or not reference:
            return PhaseCOutcome(None)
        if type(value) is not int:
            return PhaseCOutcome(None)
        return PhaseCOutcome(value == 1, "goal-verifier:" + reference)


def _intent_value(intent: Any, name: str) -> Any:
    if isinstance(intent, dict):
        return intent[name]
    return getattr(intent, name)
