from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Obligation:
    obligation_id: str
    owner: str
    action: str

    def wire(self) -> dict[str, str]:
        return {"id": self.obligation_id, "owner": self.owner, "action": self.action}


@dataclass(frozen=True)
class Effect:
    effect_key: str
    status: str
    readback_ref: str | None

    def wire(self) -> dict[str, str | None]:
        return {
            "effect_key": self.effect_key,
            "status": self.status,
            "readback_ref": self.readback_ref,
        }


@dataclass(frozen=True)
class Grant:
    grant_id: str
    scope: str
    actor: str
    action: str
    revision: str
    status: str
    issuer_id: str

    def wire(self) -> dict[str, str]:
        return {
            "grant_id": self.grant_id,
            "scope": self.scope,
            "actor": self.actor,
            "action": self.action,
            "revision": self.revision,
            "status": self.status,
            "issuer_id": self.issuer_id,
        }


@dataclass(frozen=True)
class SignedStatement:
    issuer_id: str
    kind: str
    payload: dict[str, Any]
    signature: bytes


@dataclass
class SourceCut:
    scope: str
    source_id: str
    source_format: str
    cut_id: str
    query_id: str
    revision: str
    current_revision: str
    sequence_start: int
    sequence_end: int
    raw_bytes: bytes
    obligations: list[Obligation]
    effects: list[Effect]
    grants: list[Grant]
    goal_from: str
    goal_to: str
    goal_decision_ref: str
    attestations: list[SignedStatement] = field(default_factory=list)

    @property
    def raw_sha256(self) -> str:
        from hashlib import sha256

        return sha256(self.raw_bytes).hexdigest()


@dataclass(frozen=True)
class Disposition:
    obligation_id: str
    kind: str
    from_owner: str
    to_owner: str | None = None
    release_ref: str | None = None
    acceptance_ref: str | None = None
    authority_ref: str | None = None
    decision_ref: str | None = None
    residual_custodian: str | None = None
    outcome_ref: str | None = None

    def wire(self) -> dict[str, str | None]:
        return {
            "obligation_id": self.obligation_id,
            "kind": self.kind,
            "from_owner": self.from_owner,
            "to_owner": self.to_owner,
            "release_ref": self.release_ref,
            "acceptance_ref": self.acceptance_ref,
            "authority_ref": self.authority_ref,
            "decision_ref": self.decision_ref,
            "residual_custodian": self.residual_custodian,
            "outcome_ref": self.outcome_ref,
        }


@dataclass(frozen=True)
class FutureAction:
    action_id: str
    actor: str
    action: str
    grant_id: str
    dependency_revision: str | None
    provider_id: str | None = None


@dataclass(frozen=True)
class Proposal:
    transition_id: str
    old_goal: str
    new_goal: str
    goal_decision_ref: str
    historical_effect_keys: tuple[str, ...]
    dispositions: tuple[Disposition, ...]
    future_actions: tuple[FutureAction, ...]


@dataclass(frozen=True)
class Decision:
    verdict: str
    reasons: tuple[str, ...]
    eligible_actions: tuple[str, ...] = ()

    def wire(self) -> dict[str, Any]:
        return {
            "verdict": self.verdict,
            "reasons": list(self.reasons),
            "eligible_actions": list(self.eligible_actions),
        }
