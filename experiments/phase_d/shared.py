"""Small shared contracts for the Phase D workers and trusted adapters."""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

import httpx

from experiments.phase_c.shared import PhaseCBarrier

PROVIDER_NAME = "phase-d-mock-shipment"
CASE_REF = "case/phase-d-one"
SOURCE_REF = "source/phase-d-one/risk-decision"


@dataclass(frozen=True)
class PhaseDReceipt:
    accepted: bool
    receipt_ref: str | None = None
OLD_GOAL = "prepare_shipment"
NEW_GOAL = "quarantine_and_review"


@dataclass(frozen=True)
class PhaseDReadback:
    disposition: str
    evidence_ref: str | None
    reliable_no_effect: bool
    value: int | None


class PhaseDProviderAdapter:
    """Arm-side client for the fault proxy; never carries provider credentials."""

    def __init__(
        self,
        *,
        base_url: str,
        trial_id: str,
        arm: str,
        actor: str,
        proxy_client_token: str,
        source_ref: str,
        barrier: PhaseCBarrier | None = None,
        timeout_seconds: float = 0.8,
    ) -> None:
        parsed = urlsplit(base_url)
        if parsed.scheme != "http" or parsed.hostname != "127.0.0.1" or not parsed.port:
            raise ValueError("Phase D adapter requires a loopback-only fault proxy")
        if arm not in {"continuity", "postgres_dbos"}:
            raise ValueError("invalid Phase D arm")
        if actor not in {"agent-a", "agent-b"}:
            raise ValueError("invalid Phase D actor")
        if len(proxy_client_token) < 32:
            raise ValueError("test-only proxy client token is too short")
        self.base_url = base_url.rstrip("/")
        self.trial_id = trial_id
        self.arm = arm
        self.actor = actor
        self.proxy_client_token = proxy_client_token
        self.source_ref = source_ref
        self.barrier = barrier
        self.timeout_seconds = timeout_seconds
        self.attempted_posts = 0
        self.attempted_gets = 0

    def _headers(self) -> dict[str, str]:
        return {
            "X-PhaseD-Proxy-Client-Token": self.proxy_client_token,
            "X-PhaseD-Trial-ID": self.trial_id,
            "X-PhaseD-Arm": self.arm,
            "X-PhaseD-Actor": self.actor,
        }

    def execute(self, intent: Any) -> PhaseDReceipt:
        action = str(_intent_value(intent, "action"))
        operation_id = str(_intent_value(intent, "operation_id"))
        if action == "quarantine" and self.barrier is not None:
            # Runtime's reservation is durable before adapter.execute(); baseline
            # invokes this same adapter only after its own reservation commits.
            self.barrier.announce_and_wait("reserved_before_send", operation_id)
        body = {
            "trial_id": self.trial_id,
            "operation_id": operation_id,
            "actor": self.actor,
            "provider": _intent_value(intent, "provider"),
            "resource_ref": _intent_value(intent, "resource_ref"),
            "action": action,
            "payload": dict(_intent_value(intent, "payload")),
            "authority_id": _intent_value(intent, "authority_id"),
            "source_revision": dict(_intent_value(intent, "expected_versions"))[self.source_ref],
            "effect_key": _intent_value(intent, "effect_key"),
        }
        self.attempted_posts += 1
        with httpx.Client(trust_env=False, timeout=self.timeout_seconds) as client:
            response = client.post(self.base_url + "/v1/effects", json=body, headers=self._headers())
            response.raise_for_status()
            result = response.json()
        if not isinstance(result, dict):
            raise ValueError("provider proxy returned a non-object receipt")
        if (
            result.get("trial_id") != self.trial_id
            or result.get("arm") != self.arm
            or result.get("action") != action
            or result.get("accepted") is not True
        ):
            raise ValueError("provider receipt identity/action does not match this operation")
        receipt_ref = result.get("receipt_ref")
        if not isinstance(receipt_ref, str) or not receipt_ref:
            raise ValueError("provider receipt is missing its independent ledger reference")
        return PhaseDReceipt(True, receipt_ref)

    def readback(self, intent: Any) -> PhaseDReadback:
        self.attempted_gets += 1
        resource_ref = str(_intent_value(intent, "resource_ref"))
        action = str(_intent_value(intent, "action"))
        with httpx.Client(trust_env=False, timeout=self.timeout_seconds) as client:
            response = client.get(
                self.base_url + "/v1/effects",
                params={"resource_ref": resource_ref, "action": action},
                headers=self._headers(),
            )
            response.raise_for_status()
            result = response.json()
        if not isinstance(result, dict):
            raise ValueError("provider proxy returned a non-object readback")
        count = result.get("action_count")
        if type(count) is not int or count < 0:
            raise ValueError("provider readback has an invalid action count")
        if (
            result.get("trial_id") != self.trial_id
            or result.get("arm") != self.arm
            or result.get("resource_ref") != resource_ref
            or result.get("action") != action
            or result.get("action_counts", {}).get(action) != count
        ):
            raise ValueError("provider readback identity/action does not match this operation")
        evidence_ref = result.get("evidence_ref") or result.get("ledger_ref")
        if not isinstance(evidence_ref, str) or not evidence_ref:
            raise ValueError("provider readback is missing its ledger evidence reference")
        return PhaseDReadback(
            disposition="present" if count else "absent",
            evidence_ref=evidence_ref,
            reliable_no_effect=False,
            value=count,
        )


class PhaseDOperationVerifier:
    """Verifies a single effect operation, never the business goal itself."""

    def verify(self, intent: Any, effect: Any) -> PhaseDOutcome:
        action = str(_intent_value(intent, "action"))
        disposition = str(_intent_value(effect, "disposition"))
        evidence_ref = _intent_value(effect, "evidence_ref")
        if action in {"pack", "quarantine"} and disposition == "present" and evidence_ref:
            return PhaseDOutcome(True, evidence_ref=str(evidence_ref))
        # An absent snapshot is not proof of historical non-occurrence and never
        # earns a new-goal success claim.
        return PhaseDOutcome(None, evidence_ref=None)


@dataclass(frozen=True)
class PhaseDOutcome:
    satisfied: bool | None
    evidence_ref: str | None = None


@dataclass(frozen=True)
class PhaseDAdmission:
    decision: str
    reason: str
    policy_version: str


class PhaseDVersionPinPolicy:
    """Continuity host admission profile: bind each action grant to one revision."""

    def __init__(self, profile_name: str, pinned_revision: str) -> None:
        if profile_name not in {"phase-d-v1", "phase-d-v2"}:
            raise ValueError("unknown Phase D source-version profile")
        if pinned_revision not in {"v1", "v2"}:
            raise ValueError("unknown Phase D source revision")
        self.profile_name = profile_name
        self.pinned_revision = pinned_revision

    def evaluate(self, intent: Any, _observations: dict[str, str]) -> PhaseDAdmission:
        expected = dict(_intent_value(intent, "expected_versions"))
        if expected.get(SOURCE_REF) == self.pinned_revision:
            return PhaseDAdmission("admit", "grant_version_pin_match", self.profile_name)
        return PhaseDAdmission("deny", "grant_version_pin_mismatch", self.profile_name)


class TrustedSourceReader:
    """Read-only version adapter for both arms; writes stay with the source owner."""

    def __init__(
        self,
        base_url: str,
        source_ref: str,
        trial_id: str,
        arm: str,
        timeout_seconds: float = 0.5,
    ) -> None:
        parsed = urlsplit(base_url)
        if parsed.scheme != "http" or parsed.hostname != "127.0.0.1" or not parsed.port:
            raise ValueError("trusted source reader must be loopback-only")
        self.base_url = base_url.rstrip("/")
        self.source_ref = source_ref
        self.trial_id = trial_id
        self.arm = arm
        self.timeout_seconds = timeout_seconds

    def current(self) -> dict[str, Any] | None:
        try:
            with httpx.Client(trust_env=False, timeout=self.timeout_seconds) as client:
                response = client.get(self.base_url + "/v1/source/current")
                response.raise_for_status()
                row = response.json()
            if not isinstance(row, dict):
                return None
            if row.get("trial_id") != self.trial_id or row.get("arm") != self.arm:
                return None
            if not row.get("evidence_ref") or not row.get("source_event_ref"):
                return None
            revision = row.get("revision")
            if revision not in {"v1", "v2"} or not isinstance(row.get("fact"), str):
                return None
            return row if isinstance(row, dict) else None
        except (httpx.HTTPError, ValueError):
            return None

    def __call__(self) -> str | None:
        row = self.current()
        return None if row is None else str(row["revision"])


def _intent_value(value: Any, name: str) -> Any:
    if isinstance(value, dict):
        return value[name]
    return getattr(value, name)


def child_environment_names() -> list[str]:
    """Return names only, for sanitized isolation diagnostics."""
    return sorted(os.environ)
