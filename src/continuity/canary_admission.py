"""Narrow, fail-closed canary promotion admission using independent telemetry.

Scope inspired by BAA-Protocol's bounded canary case, NOT a formal guarantee,
universal risk model, or production deployment authorization. The caller cannot
supply the telemetry, target policy or rollback status.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

from .contracts import Admission, Intent


@dataclass(frozen=True)
class CanaryStage:
    weight: int
    min_duration_seconds: int
    min_requests: int

    def __post_init__(self) -> None:
        if not 1 <= self.weight <= 100:
            raise ValueError("stage weight must be 1..100")
        if self.min_duration_seconds < 1 or self.min_requests < 1:
            raise ValueError("stage evidence minimums must be positive")


@dataclass(frozen=True)
class CanaryGuardrails:
    max_candidate_error_rate: float
    max_error_rate_delta: float
    max_candidate_p95_ms: float
    max_p95_ratio: float

    def __post_init__(self) -> None:
        if not (0 <= self.max_candidate_error_rate <= 1):
            raise ValueError("invalid candidate error threshold")
        if not (0 <= self.max_error_rate_delta <= 1):
            raise ValueError("invalid error delta threshold")
        if self.max_candidate_p95_ms <= 0 or self.max_p95_ratio <= 0:
            raise ValueError("latency bounds must be positive")


@dataclass(frozen=True)
class CanarySnapshot:
    revision: str
    evidence_ref: str
    observed_at: datetime
    current_weight: int
    current_stage_index: int
    rollback_ready: bool
    # Explicit evidence of prerequisite tests for the initial 0% -> first stage.
    initial_quality_verified: bool = False
    effect_unresolved: bool = False
    telemetry_complete: bool = True
    duration_seconds: int = 0
    candidate_requests: int = 0
    control_requests: int = 0
    candidate_error_rate: float | None = None
    control_error_rate: float | None = None
    candidate_p95_ms: float | None = None
    control_p95_ms: float | None = None


@dataclass(frozen=True)
class CanaryScope:
    provider: str
    resource_ref: str
    action: str
    experiment_id: str
    source_ref: str
    stages: tuple[CanaryStage, ...]
    guardrails: CanaryGuardrails
    max_evidence_age_seconds: int = 300

    def __post_init__(self) -> None:
        weights = tuple(stage.weight for stage in self.stages)
        if (not weights or weights != tuple(sorted(set(weights)))
                or weights[-1] != 100):
            raise ValueError("canary stages must be unique, increasing, end at 100")
        if self.max_evidence_age_seconds < 1:
            raise ValueError("evidence age must be positive")
        if not all((self.provider, self.resource_ref, self.action,
                    self.experiment_id, self.source_ref)):
            raise ValueError("canary scope identifiers required")


class CanaryAdmissionPolicy:
    """Independent bounded checker. No side effects, grants or provider credentials."""

    def __init__(
        self, scope: CanaryScope, read_snapshot: Callable[[], CanarySnapshot],
        *, clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.scope = scope
        self.read_snapshot = read_snapshot
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    def evaluate(self, intent: Intent, observations: dict[str, str]) -> Admission:
        policy = "canary-stage-v1"

        def decision(kind: str, reason: str, evidence: str | None = None) -> Admission:
            return Admission(kind, reason, evidence_ref=evidence, policy_version=policy)

        if ((intent.provider, intent.resource_ref, intent.action)
            != (self.scope.provider, self.scope.resource_ref, self.scope.action)):
            return decision("deny", "outside the protected canary operation scope")
        if intent.payload.get("experiment_id") != self.scope.experiment_id:
            return decision("deny", "experiment binding mismatch")
        index = intent.payload.get("stage_index")
        weight = intent.payload.get("weight_percent")
        if type(index) is not int or type(weight) is not int:
            return decision("deny", "stage index and weight must be exact integers")
        if not 0 <= index < len(self.scope.stages):
            return decision("deny", "stage outside approved sequence")
        if weight != self.scope.stages[index].weight:
            return decision("deny", "weight outside approved stage")
        # Mandatory source identity remains enforced by the grant and trusted
        # VersionAuthority. The canary admission itself also fails closed.
        expected_version = intent.expected_versions.get(self.scope.source_ref)
        if not expected_version:
            return decision("hold", "trusted canary state dependency missing")
        try:
            evidence = self.read_snapshot()
            now = self.clock()
        except Exception:
            return decision("hold", "independent canary telemetry unavailable")
        if not isinstance(evidence, CanarySnapshot):
            return decision("hold", "unexpected canary evidence structure")
        if not isinstance(evidence.observed_at, datetime):
            return decision("hold", "telemetry timestamp is invalid")
        if now.tzinfo is None or evidence.observed_at.tzinfo is None:
            return decision("hold", "telemetry has no trusted time zone")
        age = (now - evidence.observed_at).total_seconds()
        if age < 0 or age > self.scope.max_evidence_age_seconds:
            return decision("hold", "canary evidence stale or in future", evidence.evidence_ref)
        if not evidence.evidence_ref or not evidence.revision:
            return decision("hold", "canary evidence lacks provenance")
        if evidence.revision != expected_version:
            return decision("hold", "canary state revision changed", evidence.evidence_ref)
        if evidence.effect_unresolved:
            return decision("hold", "previous traffic effect unresolved", evidence.evidence_ref)
        if not evidence.rollback_ready:
            return decision("hold", "rollback unavailable", evidence.evidence_ref)
        numeric_fields = (
            evidence.current_weight, evidence.current_stage_index, evidence.duration_seconds,
            evidence.candidate_requests, evidence.control_requests,
        )
        if any(type(v) is not int for v in numeric_fields):
            return decision("hold", "invalid observed integer telemetry", evidence.evidence_ref)
        if not 0 <= evidence.current_weight <= 100:
            return decision("hold", "invalid observed traffic weight", evidence.evidence_ref)
        expected_index = (
            0 if evidence.current_weight == 0 else evidence.current_stage_index + 1
        )
        if index != expected_index:
            return decision("deny", "stage skipped or stale", evidence.evidence_ref)
        if evidence.current_weight == 0 and not evidence.initial_quality_verified:
            return decision("hold", "initial quality prerequisites unverified",
                            evidence.evidence_ref)
        if evidence.current_weight:
            if not (0 <= evidence.current_stage_index < len(self.scope.stages)):
                return decision("hold", "observed stage invalid", evidence.evidence_ref)
            current = self.scope.stages[evidence.current_stage_index]
            if evidence.current_weight != current.weight:
                return decision("hold", "observed traffic does not match stage", evidence.evidence_ref)
            if not evidence.telemetry_complete or evidence.duration_seconds < current.min_duration_seconds:
                return decision("hold", "insufficient observation duration", evidence.evidence_ref)
            n = evidence.candidate_requests + evidence.control_requests
            if n < current.min_requests:
                return decision("hold", "insufficient total requests", evidence.evidence_ref)
            min_candidate = max(1, math.ceil(current.min_requests * current.weight / 100))
            min_control = (
                0 if current.weight == 100
                else max(1, math.ceil(current.min_requests * (100 - current.weight) / 100))
            )
            if (evidence.candidate_requests < min_candidate
                    or evidence.control_requests < min_control):
                return decision("hold", "insufficient per-arm requests", evidence.evidence_ref)
            measurements = (
                evidence.candidate_error_rate, evidence.candidate_p95_ms,
                evidence.control_error_rate, evidence.control_p95_ms,
            )
            if any(type(v) not in (float, int) or not math.isfinite(v) or v < 0
                   for v in measurements):
                return decision("hold", "incomplete or invalid comparison telemetry",
                                evidence.evidence_ref)
            err, latency, control_err, control_latency = measurements
            if err > 1 or control_err > 1 or control_latency <= 0:
                return decision("hold", "invalid comparison metrics", evidence.evidence_ref)
            limits = self.scope.guardrails
            if (err > limits.max_candidate_error_rate
                    or err - control_err > limits.max_error_rate_delta
                    or latency > limits.max_candidate_p95_ms
                    or latency / control_latency > limits.max_p95_ratio):
                return decision("deny", "canary guardrail exceeded", evidence.evidence_ref)
        return decision("admit", "canary stage and telemetry checks passed",
                        evidence.evidence_ref)
