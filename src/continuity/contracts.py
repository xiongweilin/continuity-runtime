"""Small protocol surface; domain payloads remain with their owners."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal, Protocol

DecisionKind = Literal["admit", "hold", "deny"]
ReadbackKind = Literal["present", "absent", "unknown"]


@dataclass(frozen=True)
class Admission:
    decision: DecisionKind
    reason: str = ""
    evidence_ref: str | None = None
    # Evaluators may inspect immutable intent and current versions; this is NOT authority.
    policy_version: str = "baseline-v1"


@dataclass(frozen=True)
class Receipt:
    """A provider acknowledgement, NOT proof of the external effect."""
    accepted: bool
    receipt_ref: str | None = None


@dataclass(frozen=True)
class Readback:
    disposition: ReadbackKind
    evidence_ref: str | None = None
    # A negative snapshot generally does NOT prove the historical effect never happened.
    reliable_no_effect: bool = False


@dataclass(frozen=True)
class Outcome:
    satisfied: bool | None
    evidence_ref: str | None = None


@dataclass(frozen=True)
class Intent:
    operation_id: str
    principal: str
    provider: str
    resource_ref: str
    action: str
    payload: dict[str, Any]
    authority_id: str
    expected_versions: dict[str, str] = field(default_factory=dict)


class EffectAdapter(Protocol):
    """Trusted executor with credentials unavailable to the proposing agents."""
    def execute(self, intent: Intent) -> Receipt: ...
    def readback(self, intent: Intent) -> Readback: ...


class AdmissionPolicy(Protocol):
    def evaluate(self, intent: Intent, observations: dict[str, str]) -> Admission: ...


class DomainVerifier(Protocol):
    """Only domain-owned verification can declare goal satisfaction."""
    def verify(self, intent: Intent, effect: Readback) -> Outcome: ...


class BaselinePolicy:
    """No BAA guarantee. Authorization and effect fencing are enforced separately."""
    def evaluate(self, intent: Intent, observations: dict[str, str]) -> Admission:
        return Admission("admit", policy_version="baseline-v1")


class NoAutoOutcome:
    def verify(self, intent: Intent, effect: Readback) -> Outcome:
        return Outcome(None)


class ContinuityError(Exception):
    pass


class Conflict(ContinuityError):
    pass


class Denied(ContinuityError):
    pass


class Held(ContinuityError):
    pass


class UnknownOperation(ContinuityError):
    pass
