from __future__ import annotations

import json
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from .adapters.domain_events import read_domain_event_cut
from .adapters.workflow_history import read_workflow_history_cut
from .crypto import TrustedIssuer, TrustStore, digest, signed_statement, utc_text
from .model import (
    Disposition,
    FutureAction,
    Proposal,
    SignedStatement,
    SourceCut,
)
from .policy_digest import POLICY_SHA256

FIXTURE_DIR = Path(__file__).with_name("fixtures")
SOURCE_POLICY_SHA256 = POLICY_SHA256
SOURCE_FACTS = {
    "scope": "purchase-7",
    "goal_from": "purchase-v1",
    "goal_to": "investigate-v2",
    "goal_decision_ref": "goal-owner:decision-92",
    "obligations": [
        {"id": "notify-supplier", "owner": "agent-a", "action": "notify"},
        {"id": "verify-po-07", "owner": "agent-a", "action": "investigate"},
    ],
    "effects": [
        {"effect_key": "erp-draft-po-07", "status": "confirmed", "readback_ref": "erp:readback-217"}
    ],
    "grants": [
        {
            "grant_id": "grant-a-v1",
            "scope": "purchase-7",
            "actor": "agent-a",
            "action": "investigate",
            "revision": "v1",
            "status": "revoked",
            "issuer_id": "authority-owner",
        },
        {
            "grant_id": "grant-b-v2",
            "scope": "purchase-7",
            "actor": "agent-b",
            "action": "investigate",
            "revision": "v2",
            "status": "pending",
            "issuer_id": "authority-owner",
        },
        {
            "grant_id": "grant-c-v1",
            "scope": "purchase-7",
            "actor": "agent-c",
            "action": "notify",
            "revision": "v1",
            "status": "active",
            "issuer_id": "authority-owner",
        },
    ],
}
SOURCE_IDENTITIES = {
    "domain-events": {
        "issuer": "domain-owner",
        "source_id": "domain-ledger-procurement",
        "source_format": "append-only-domain-events/v1",
        "cut_id": "purchase-7-cut-44",
        "query_id": "open-obligations-effects-v1",
    },
    "workflow-history": {
        "issuer": "workflow-owner",
        "source_id": "dbos-procurement-history",
        "source_format": "workflow-history/v1",
        "cut_id": "purchase-7-history-901",
        "query_id": "task-effect-authorization-projection-v3",
    },
}


@dataclass(frozen=True)
class Signers:
    trust_store: TrustStore
    private_keys: dict[str, Ed25519PrivateKey]


@dataclass(frozen=True)
class CaseEvidence:
    cut: SourceCut
    signers: Signers


def new_signers(source_issuer: str) -> Signers:
    roles = {
        source_issuer: {"source-owner"},
        "goal-owner": {"goal-owner"},
        "authority-owner": {"authority"},
        "effect-ledger": {"effect-owner"},
        "synthetic-provider": {"provider"},
        "agent-a": {"agent"},
        "agent-b": {"agent"},
    }
    private = {issuer: Ed25519PrivateKey.generate() for issuer in roles}
    trusted = {
        issuer: TrustedIssuer(private[issuer].public_key(), frozenset(role_set))
        for issuer, role_set in roles.items()
    }
    source_id = (
        "domain-ledger-procurement"
        if source_issuer == "domain-owner"
        else "dbos-procurement-history"
    )
    return Signers(TrustStore(trusted, {source_id: source_issuer}), private)


def mutate_fixture_bytes(source_format: str, mutate: Callable[[dict], None] | None) -> bytes:
    name = "domain_events.jsonl" if source_format == "domain-events" else "workflow_history.json"
    raw = (FIXTURE_DIR / name).read_bytes()
    if mutate is None:
        return raw
    if source_format == "domain-events":
        document = [json.loads(line) for line in raw.splitlines() if line.strip()]
        mutate(document)
        return b"".join(
            json.dumps(row, separators=(",", ":"), ensure_ascii=False).encode() + b"\n"
            for row in document
        )
    document = json.loads(raw)
    mutate(document)
    return json.dumps(document, separators=(",", ":"), ensure_ascii=False).encode()


def load_cut(source_format: str, mutate: Callable[[dict], None] | None = None) -> SourceCut:
    raw = mutate_fixture_bytes(source_format, mutate)
    filename = "events.jsonl" if source_format == "domain-events" else "history.json"
    with tempfile.TemporaryDirectory(prefix="pcct-fixture-") as scratch:
        path = Path(scratch) / filename
        path.write_bytes(raw)
        if source_format == "domain-events":
            return read_domain_event_cut(path)
        if source_format == "workflow-history":
            return read_workflow_history_cut(path)
    raise ValueError(f"unknown fixture format: {source_format}")


def _claim(
    signers: Signers,
    issuer: str,
    kind: str,
    payload: dict,
) -> SignedStatement:
    return signed_statement(issuer, kind, payload, signers.private_keys[issuer])


def attest_cut(
    cut: SourceCut,
    signers: Signers,
    now: datetime,
    *,
    completeness: dict[str, bool] | None = None,
    expires_at: datetime | None = None,
    tamper_kind: str | None = None,
    unknown_source_issuer: bool = False,
    self_issued_grant: bool = False,
    equivocal: bool = False,
    effects_override: list[dict] | None = None,
    head_revision: str | None = None,
    include_effect_index: bool = True,
    include_provider: bool = True,
    provider_supported: bool = True,
) -> None:
    source_issuer = signers.trust_store.source_owners[
        SOURCE_IDENTITIES["domain-events"]["source_id"]
        if cut.source_format == "append-only-domain-events/v1"
        else SOURCE_IDENTITIES["workflow-history"]["source_id"]
    ]
    identity = (
        SOURCE_IDENTITIES["domain-events"]
        if cut.source_format == "append-only-domain-events/v1"
        else SOURCE_IDENTITIES["workflow-history"]
    )
    expiration = expires_at or now + timedelta(hours=1)
    obligation_rows = SOURCE_FACTS["obligations"]
    effect_rows = effects_override or SOURCE_FACTS["effects"]
    grant_rows = SOURCE_FACTS["grants"]
    claims = {
        "scope": SOURCE_FACTS["scope"],
        "source_id": identity["source_id"],
        "source_format": identity["source_format"],
        "cut_id": identity["cut_id"],
        "query_id": identity["query_id"],
        "revision": "v2",
        "sequence_start": 1,
        "sequence_end": 6,
        "raw_sha256": cut.raw_sha256,
        "policy_sha256": SOURCE_POLICY_SHA256,
        "obligation_count": len(obligation_rows),
        "obligation_sha256": digest(sorted(obligation_rows, key=lambda row: row["id"])),
        "goal_from": SOURCE_FACTS["goal_from"],
        "goal_to": SOURCE_FACTS["goal_to"],
        "goal_decision_ref": SOURCE_FACTS["goal_decision_ref"],
        "completeness": completeness or {"source": True, "obligations": True},
        "issued_at": utc_text(now),
        "expires_at": utc_text(expiration),
    }
    issuer_id = "unknown-source" if unknown_source_issuer else source_issuer
    source_signer = source_issuer
    cut_statement = _claim(signers, source_signer, "source-cut", claims)
    if unknown_source_issuer:
        cut_statement = SignedStatement(
            issuer_id, cut_statement.kind, claims, cut_statement.signature
        )
    if equivocal:
        other_claim = {**claims, "obligation_sha256": "0" * 64}
        cut.attestations.append(_claim(signers, source_signer, "source-cut", other_claim))
    if tamper_kind == "source-cut":
        cut_statement = SignedStatement(
            cut_statement.issuer_id, cut_statement.kind, cut_statement.payload, b"\x00" * 64
        )
    cut.attestations.append(cut_statement)
    cut.attestations.append(
        _claim(
            signers,
            source_signer,
            "source-head",
            {
                "scope": SOURCE_FACTS["scope"],
                "source_id": identity["source_id"],
                "current_revision": head_revision or "v2",
                "observed_at": utc_text(now),
                "expires_at": utc_text(expiration),
            },
        )
    )
    if include_effect_index:
        cut.attestations.append(
            _claim(
                signers,
                "effect-ledger",
                "effect-index",
                {
                    "scope": SOURCE_FACTS["scope"],
                    "cut_id": identity["cut_id"],
                    "complete": True,
                    "count": len(effect_rows),
                    "sha256": digest(sorted(effect_rows, key=lambda row: row["effect_key"])),
                    "effects": sorted(effect_rows, key=lambda row: row["effect_key"]),
                    "expires_at": utc_text(expiration),
                },
            )
        )
    if include_provider:
        cut.attestations.append(
            _claim(
                signers,
                "synthetic-provider",
                "provider-capability",
                {
                    "scope": SOURCE_FACTS["scope"],
                    "cut_id": identity["cut_id"],
                    "provider_id": "synthetic-erp",
                    "action": "investigate",
                    "revision": "v2",
                    "conditional_write": provider_supported,
                    "expires_at": utc_text(expiration),
                },
            )
        )
    cut.attestations.append(
        _claim(
            signers,
            "goal-owner",
            "goal-decision",
            {
                "scope": SOURCE_FACTS["scope"],
                "cut_id": identity["cut_id"],
                "old_goal": SOURCE_FACTS["goal_from"],
                "new_goal": SOURCE_FACTS["goal_to"],
                "decision_ref": SOURCE_FACTS["goal_decision_ref"],
                "superseded_obligations": ["notify-supplier"],
                "expires_at": utc_text(expiration),
            },
        )
    )
    grant_issuer = "agent-a" if self_issued_grant else "authority-owner"
    cut.attestations.append(
        _claim(
            signers,
            grant_issuer,
            "grant-register",
            {
                "scope": SOURCE_FACTS["scope"],
                "cut_id": identity["cut_id"],
                "revision": "v2",
                "complete": True,
                "count": len(grant_rows),
                "sha256": digest(sorted(grant_rows, key=lambda row: row["grant_id"])),
                "grants": sorted(grant_rows, key=lambda row: row["grant_id"]),
                "expires_at": utc_text(expiration),
            },
        )
    )
    release_signature = _claim(
        signers,
        "agent-a",
        "obligation-release",
        {
            "scope": SOURCE_FACTS["scope"],
            "cut_id": identity["cut_id"],
            "obligation_id": "verify-po-07",
            "from_owner": "agent-a",
            "to_owner": "agent-b",
            "action": "investigate",
            "release_ref": "release:agent-a:19",
            "expires_at": utc_text(expiration),
        },
    )
    acceptance_signature = _claim(
        signers,
        "agent-b",
        "obligation-acceptance",
        {
            "scope": SOURCE_FACTS["scope"],
            "cut_id": identity["cut_id"],
            "obligation_id": "verify-po-07",
            "from_owner": "agent-a",
            "to_owner": "agent-b",
            "action": "investigate",
            "revision": "v2",
            "acceptance_ref": "accept:agent-b:21",
            "expires_at": utc_text(expiration),
        },
    )
    if tamper_kind == "acceptance":
        acceptance_signature = SignedStatement(
            acceptance_signature.issuer_id,
            acceptance_signature.kind,
            acceptance_signature.payload,
            b"\x00" * 64,
        )
    if tamper_kind == "release":
        release_signature = SignedStatement(
            release_signature.issuer_id,
            release_signature.kind,
            release_signature.payload,
            b"\x00" * 64,
        )
    cut.attestations.extend((release_signature, acceptance_signature))


def build_case_evidence(
    source_format: str,
    now: datetime | None = None,
    *,
    mutate: Callable[[dict], None] | None = None,
    completeness: dict[str, bool] | None = None,
    expires_at: datetime | None = None,
    tamper_kind: str | None = None,
    unknown_source_issuer: bool = False,
    self_issued_grant: bool = False,
    equivocal: bool = False,
    effects_override: list[dict] | None = None,
    head_revision: str | None = None,
    include_effect_index: bool = True,
    include_provider: bool = True,
    provider_supported: bool = True,
) -> CaseEvidence:
    clock = now or datetime(2026, 11, 2, 12, 0, tzinfo=timezone.utc)
    cut = load_cut(source_format, mutate)
    source_issuer = SOURCE_IDENTITIES[source_format]["issuer"]
    signers = new_signers(source_issuer)
    attest_cut(
        cut,
        signers,
        clock,
        completeness=completeness,
        expires_at=expires_at,
        tamper_kind=tamper_kind,
        unknown_source_issuer=unknown_source_issuer,
        self_issued_grant=self_issued_grant,
        equivocal=equivocal,
        effects_override=effects_override,
        head_revision=head_revision,
        include_effect_index=include_effect_index,
        include_provider=include_provider,
        provider_supported=provider_supported,
    )
    return CaseEvidence(cut, signers)


def clean_proposal(cut: SourceCut, transition_id: str = "transition-v1-v2") -> Proposal:
    dispositions = (
        Disposition(
            obligation_id="verify-po-07",
            kind="transfer",
            from_owner="agent-a",
            to_owner="agent-b",
            release_ref="release:agent-a:19",
            acceptance_ref="accept:agent-b:21",
            authority_ref="grant-b-v2",
        ),
        Disposition(
            obligation_id="notify-supplier",
            kind="supersede",
            from_owner="agent-a",
            decision_ref=cut.goal_decision_ref,
            residual_custodian="procurement-desk",
        ),
    )
    actions = (
        FutureAction(
            action_id="new-check",
            actor="agent-b",
            action="investigate",
            grant_id="grant-b-v2",
            dependency_revision=cut.revision,
            provider_id="synthetic-erp",
        ),
        FutureAction(
            action_id="unaffected-notice",
            actor="agent-c",
            action="notify",
            grant_id="grant-c-v1",
            dependency_revision=None,
            provider_id=None,
        ),
    )
    return Proposal(
        transition_id=transition_id,
        old_goal=cut.goal_from,
        new_goal=cut.goal_to,
        goal_decision_ref=cut.goal_decision_ref,
        historical_effect_keys=("erp-draft-po-07",),
        dispositions=dispositions,
        future_actions=actions,
    )


def fixed_clock() -> datetime:
    return datetime(2026, 11, 2, 12, 0, tzinfo=timezone.utc)
