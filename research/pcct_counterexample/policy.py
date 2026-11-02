from __future__ import annotations

import json
from datetime import datetime
from hashlib import sha256

import psycopg
from psycopg import Connection
from psycopg.errors import IntegrityError, SerializationFailure

from .crypto import TrustStore, canonical_bytes, digest
from .model import Decision, Proposal, SignedStatement, SourceCut
from .policy_digest import POLICY_SHA256
from .repository import execute_many, persist_cut

ROLE_BY_KIND = {
    "source-cut": "source-owner",
    "source-head": "source-owner",
    "goal-decision": "goal-owner",
    "grant-register": "authority",
    "effect-index": "effect-owner",
    "provider-capability": "provider",
    "obligation-outcome": "source-owner",
    "obligation-release": "agent",
    "obligation-acceptance": "agent",
}


class _Rejected(Exception):
    def __init__(self, decision: Decision) -> None:
        super().__init__(decision.verdict)
        self.decision = decision


def _decision(
    invalid: list[str], hold: list[str], incomplete: list[str], eligible: list[str]
) -> Decision:
    if invalid:
        return Decision("INVALID", tuple(sorted(set(invalid))))
    if incomplete:
        return Decision("INCOMPLETE_SCOPE", tuple(sorted(set(incomplete))))
    if hold:
        return Decision("HOLD", tuple(sorted(set(hold))), tuple(sorted(set(eligible))))
    return Decision("VALID", (), tuple(sorted(set(eligible))))


def _statements(conn: Connection, cut: SourceCut) -> list[SignedStatement]:
    rows = conn.execute(
        """SELECT kind, issuer_id, payload_bytes, payload_sha256, signature
           FROM attestations WHERE scope=%s AND cut_id=%s ORDER BY statement_id""",
        (cut.scope, cut.cut_id),
    ).fetchall()
    result: list[SignedStatement] = []
    for kind, issuer_id, payload_bytes, payload_hash, signature in rows:
        raw = bytes(payload_bytes)
        if sha256(raw).hexdigest() != payload_hash:
            raise ValueError("stored_attestation_digest_mismatch")
        decoded = json.loads(raw)
        if (
            not isinstance(decoded, dict)
            or not isinstance(decoded.get("payload"), dict)
            or decoded.get("issuer_id") != issuer_id
            or decoded.get("kind") != kind
            or canonical_bytes(decoded) != raw
        ):
            raise ValueError("stored_attestation_envelope_mismatch")
        result.append(SignedStatement(issuer_id, kind, decoded["payload"], bytes(signature)))
    return result


def _verify_all(
    statements: list[SignedStatement],
    trust: TrustStore,
    now: datetime,
    invalid: list[str],
    incomplete: list[str],
    hold: list[str],
) -> dict[str, list[SignedStatement]]:
    by_kind: dict[str, list[SignedStatement]] = {}
    for statement in statements:
        by_kind.setdefault(statement.kind, []).append(statement)
        expected_role = ROLE_BY_KIND.get(statement.kind)
        if expected_role is None:
            invalid.append(f"unsupported_attestation_kind:{statement.kind}")
            continue
        reason = trust.verify(statement, expected_role, now)
        if reason == "unknown_issuer":
            incomplete.append(f"unknown_issuer:{statement.issuer_id}")
        elif reason == "expired_statement":
            if statement.kind in {"source-cut", "source-head", "grant-register", "effect-index"}:
                incomplete.append(f"expired_{statement.kind}")
            else:
                hold.append(f"expired_{statement.kind}")
        elif reason:
            invalid.append(f"{reason}:{statement.kind}:{statement.issuer_id}")
    return by_kind


def _one(
    by_kind: dict[str, list[SignedStatement]],
    kind: str,
    invalid: list[str],
    incomplete: list[str],
    hold: list[str],
) -> SignedStatement | None:
    matches = by_kind.get(kind, [])
    if not matches:
        target = incomplete if kind in {"source-cut", "source-head", "grant-register"} else hold
        target.append(f"missing_{kind}")
        return None
    if len(matches) != 1:
        payload_hashes = {digest(item.payload) for item in matches}
        if len(payload_hashes) > 1:
            invalid.append(f"equivocal_{kind}")
        else:
            invalid.append(f"duplicate_{kind}")
    statement = matches[0]
    return statement


def _rows_digest(conn: Connection, cut: SourceCut) -> tuple[int, str, int, str, int, str]:
    obligations = [
        {"id": row[0], "owner": row[1], "action": row[2]}
        for row in conn.execute(
            """SELECT obligation_id, owner_id, required_action FROM open_obligations
               WHERE scope=%s AND cut_id=%s ORDER BY obligation_id""",
            (cut.scope, cut.cut_id),
        ).fetchall()
    ]
    effects = [
        {"effect_key": row[0], "status": row[1], "readback_ref": row[2]}
        for row in conn.execute(
            """SELECT effect_key, disposition, readback_ref FROM external_effects
               WHERE scope=%s AND cut_id=%s ORDER BY effect_key""",
            (cut.scope, cut.cut_id),
        ).fetchall()
    ]
    grants = [
        {
            "grant_id": row[0],
            "scope": cut.scope,
            "actor": row[1],
            "action": row[2],
            "revision": row[3],
            "status": row[4],
            "issuer_id": row[5],
        }
        for row in conn.execute(
            """SELECT grant_id, actor_id, action, evidence_revision, status, issuer_id
               FROM grants WHERE scope=%s AND cut_id=%s ORDER BY grant_id""",
            (cut.scope, cut.cut_id),
        ).fetchall()
    ]
    return (
        len(obligations),
        digest(obligations),
        len(effects),
        digest(effects),
        len(grants),
        digest(grants),
    )


def _validate(
    conn: Connection,
    cut: SourceCut,
    proposal: Proposal,
    trust: TrustStore,
    now: datetime,
) -> Decision:
    invalid: list[str] = []
    incomplete: list[str] = []
    hold: list[str] = []
    eligible: list[str] = []
    source_row = conn.execute(
        """SELECT source_id, source_format, query_id, cut_revision, current_revision,
                  sequence_start, sequence_end, raw_sha256, raw_evidence, policy_sha256
           FROM source_revisions WHERE scope=%s AND cut_id=%s FOR UPDATE""",
        (cut.scope, cut.cut_id),
    ).fetchone()
    if source_row is None:
        return Decision("INCOMPLETE_SCOPE", ("missing_source_revision",))
    counts = _rows_digest(conn, cut)
    statements = _statements(conn, cut)
    by_kind = _verify_all(statements, trust, now, invalid, incomplete, hold)
    cut_statement = _one(by_kind, "source-cut", invalid, incomplete, hold)
    head_statement = _one(by_kind, "source-head", invalid, incomplete, hold)
    goal_statement = _one(by_kind, "goal-decision", invalid, incomplete, hold)
    grant_statement = _one(by_kind, "grant-register", invalid, incomplete, hold)
    effect_statement = _one(by_kind, "effect-index", invalid, incomplete, hold)

    expected_completeness = {"source", "obligations"}
    if cut_statement:
        claim = cut_statement.payload
        if claim.get("scope") != cut.scope or claim.get("cut_id") != cut.cut_id:
            invalid.append("source_cut_scope_or_identity_mismatch")
        completeness = claim.get("completeness")
        if not isinstance(completeness, dict):
            incomplete.append("missing_completeness_claim")
        elif completeness.keys() < expected_completeness:
            incomplete.append("missing_completeness_claim")
        elif not all(completeness.get(key) is True for key in expected_completeness):
            incomplete.append("source_owner_did_not_attest_complete_cut")
        source_fields = (
            claim.get("source_id"),
            claim.get("source_format"),
            claim.get("query_id"),
            claim.get("revision"),
            claim.get("sequence_start"),
            claim.get("sequence_end"),
            claim.get("raw_sha256"),
            claim.get("policy_sha256"),
        )
        persisted_fields = source_row[:4] + source_row[5:8] + source_row[9:]
        if source_fields != persisted_fields:
            invalid.append("source_cut_query_or_digest_mismatch")
        if sha256(bytes(source_row[8])).hexdigest() != source_row[7]:
            invalid.append("stored_raw_evidence_digest_mismatch")
        expected_source_issuer = trust.source_owners.get(cut.source_id)
        if expected_source_issuer is None:
            incomplete.append("source_owner_not_configured")
        elif (
            cut_statement.issuer_id in trust.issuers
            and expected_source_issuer != cut_statement.issuer_id
        ):
            invalid.append("source_owner_identity_not_authorized")
        if (
            head_statement
            and cut_statement.issuer_id in trust.issuers
            and head_statement.issuer_id != cut_statement.issuer_id
        ):
            invalid.append("source_owner_identity_not_authorized")
        if claim.get("policy_sha256") != POLICY_SHA256:
            invalid.append("unrecognized_policy_revision")
        if (
            claim.get("goal_from") != cut.goal_from
            or claim.get("goal_to") != cut.goal_to
            or claim.get("goal_decision_ref") != cut.goal_decision_ref
        ):
            invalid.append("goal_cut_differs_from_signed_owner_view")
        if counts[:2] != (claim.get("obligation_count"), claim.get("obligation_sha256")):
            invalid.append("database_obligations_do_not_match_attested_cut")
    else:
        incomplete.append("source_cut_not_usable")

    if head_statement:
        head = head_statement.payload
        if head.get("scope") != cut.scope or head.get("source_id") != cut.source_id:
            invalid.append("source_head_identity_mismatch")
        if head.get("current_revision") != source_row[4]:
            invalid.append("source_head_database_mismatch")
        if head.get("current_revision") != cut.revision:
            hold.append("source_changed_after_attested_cut")
    else:
        incomplete.append("source_head_not_usable")

    if effect_statement:
        effect_claim = effect_statement.payload
        signed_effects = effect_claim.get("effects", [])
        if (
            effect_claim.get("scope") != cut.scope
            or effect_claim.get("cut_id") != cut.cut_id
            or effect_claim.get("complete") is not True
        ):
            incomplete.append("effect_owner_completeness_missing")
        if (
            not isinstance(signed_effects, list)
            or not all(isinstance(row, dict) for row in signed_effects)
            or len(signed_effects) != effect_claim.get("count")
            or digest(sorted(signed_effects, key=lambda row: row.get("effect_key", "")))
            != effect_claim.get("sha256")
        ):
            invalid.append("effect_index_attestation_digest_mismatch")
        if effect_claim.get("count") != counts[2] or effect_claim.get("sha256") != counts[3]:
            invalid.append("database_effects_do_not_match_signed_provider_index")
    else:
        incomplete.append("effect_index_not_usable")

    if goal_statement:
        goal = goal_statement.payload
        if (
            goal.get("scope") != cut.scope
            or goal.get("cut_id") != cut.cut_id
            or goal.get("old_goal") != proposal.old_goal
            or goal.get("new_goal") != proposal.new_goal
            or goal.get("decision_ref") != proposal.goal_decision_ref
        ):
            invalid.append("goal_termination_not_authorized_for_transition")
        authorized_supersessions = goal.get("superseded_obligations")
        if (
            not isinstance(authorized_supersessions, list)
            or not all(isinstance(item, str) and item for item in authorized_supersessions)
            or len(authorized_supersessions) != len(set(authorized_supersessions))
        ):
            invalid.append("goal_owner_supersession_scope_mismatch")
        if proposal.goal_decision_ref != cut.goal_decision_ref:
            invalid.append("goal_decision_reference_mismatch")
    else:
        hold.append("goal_termination_lacks_owner_decision")

    obligations = {
        row[0]: {"owner": row[1], "action": row[2]}
        for row in conn.execute(
            """SELECT obligation_id, owner_id, required_action FROM open_obligations
               WHERE scope=%s AND cut_id=%s""",
            (cut.scope, cut.cut_id),
        ).fetchall()
    }
    if goal_statement:
        superseded = goal_statement.payload.get("superseded_obligations", [])
        if (
            isinstance(superseded, list)
            and all(isinstance(item, str) and item for item in superseded)
            and not set(superseded).issubset(obligations)
        ):
            invalid.append("goal_decision_names_unknown_obligation")
    effects = {
        row[0]: (row[1], row[2])
        for row in conn.execute(
            """SELECT effect_key, disposition, readback_ref FROM external_effects
               WHERE scope=%s AND cut_id=%s""",
            (cut.scope, cut.cut_id),
        ).fetchall()
    }
    if len(proposal.historical_effect_keys) != len(set(proposal.historical_effect_keys)):
        invalid.append("duplicate_historical_effect_reference")
    if set(proposal.historical_effect_keys) != set(effects):
        invalid.append("historical_effect_omission_or_invention")
    for effect_key, (status, readback_ref) in effects.items():
        if status == "unknown":
            hold.append(f"unknown_prior_effect:{effect_key}")
        elif status != "confirmed" or not readback_ref:
            hold.append(f"effect_lacks_confirming_readback:{effect_key}")

    if len(proposal.dispositions) != len({item.obligation_id for item in proposal.dispositions}):
        invalid.append("duplicate_successor_disposition")
    dispositions = {item.obligation_id: item for item in proposal.dispositions}
    if set(dispositions) != set(obligations):
        invalid.append("open_obligation_omitted_or_invented")

    if grant_statement:
        grants_claim = grant_statement.payload
        if grants_claim.get("scope") != cut.scope or grants_claim.get("cut_id") != cut.cut_id:
            invalid.append("grant_register_scope_mismatch")
        expected_grants = sorted(
            (row.wire() for row in cut.grants), key=lambda row: row["grant_id"]
        )
        if grants_claim.get("complete") is not True:
            incomplete.append("authority_owner_completeness_missing")
        if (
            grants_claim.get("grants") != expected_grants
            or grants_claim.get("count") != counts[4]
            or grants_claim.get("sha256") != counts[5]
        ):
            invalid.append("grant_register_attestation_mismatch")
        if grants_claim.get("revision") != cut.revision:
            invalid.append("grant_register_revision_mismatch")
        if any(row.issuer_id != grant_statement.issuer_id for row in cut.grants):
            invalid.append("grant_issuer_identity_mismatch")
    else:
        incomplete.append("authority_register_not_usable")

    grant_rows = {
        row[0]: {
            "actor": row[1],
            "action": row[2],
            "revision": row[3],
            "status": row[4],
            "issuer": row[5],
        }
        for row in conn.execute(
            """SELECT grant_id, actor_id, action, evidence_revision, status, issuer_id
               FROM grants WHERE scope=%s AND cut_id=%s""",
            (cut.scope, cut.cut_id),
        ).fetchall()
    }
    statement_by_id = {}
    for statement in statements:
        if statement.kind not in {
            "obligation-release",
            "obligation-acceptance",
            "obligation-outcome",
        }:
            continue
        reference = (
            statement.payload.get("release_ref")
            or statement.payload.get("acceptance_ref")
            or statement.payload.get("outcome_ref")
        )
        key = (statement.kind, reference)
        if not isinstance(reference, str) or not reference:
            invalid.append(f"missing_signed_disposition_reference:{statement.kind}")
        elif key in statement_by_id:
            prior = statement_by_id[key]
            if digest(prior.payload) != digest(statement.payload):
                invalid.append(f"equivocal_signed_disposition:{statement.kind}:{reference}")
            else:
                invalid.append(f"duplicate_signed_disposition:{statement.kind}:{reference}")
        else:
            statement_by_id[key] = statement
    provider_by_capability = {}
    for statement in statements:
        if statement.kind != "provider-capability":
            continue
        provider_claim = statement.payload
        capability = (
            provider_claim.get("provider_id"),
            provider_claim.get("action"),
            provider_claim.get("revision"),
        )
        if not all(isinstance(value, str) and value for value in capability):
            invalid.append("malformed_provider_capability")
            continue
        if capability in provider_by_capability:
            invalid.append("duplicate_provider_capability")
        provider_by_capability[capability] = statement
    transfer_grants: set[str] = set()
    for obligation_id, disposition in dispositions.items():
        obligation = obligations.get(obligation_id)
        if not obligation:
            continue
        if disposition.from_owner != obligation["owner"]:
            invalid.append(f"old_owner_mismatch:{obligation_id}")
        if disposition.kind == "retain":
            if disposition.to_owner not in {None, obligation["owner"]}:
                invalid.append(f"illegal_retain_owner_change:{obligation_id}")
        elif disposition.kind == "fulfill":
            outcome = statement_by_id.get(("obligation-outcome", disposition.outcome_ref))
            if not outcome:
                hold.append(f"completion_witness_missing:{obligation_id}")
            elif (
                outcome.issuer_id != trust.source_owners.get(cut.source_id)
                or outcome.payload.get("scope") != cut.scope
                or outcome.payload.get("cut_id") != cut.cut_id
                or outcome.payload.get("obligation_id") != obligation_id
                or outcome.payload.get("outcome_ref") != disposition.outcome_ref
                or outcome.payload.get("revision") != cut.revision
                or outcome.payload.get("status") != "fulfilled"
            ):
                invalid.append(f"completion_witness_mismatch:{obligation_id}")
        elif disposition.kind == "supersede":
            if disposition.decision_ref != proposal.goal_decision_ref:
                invalid.append(f"supersession_basis_mismatch:{obligation_id}")
            if goal_statement and obligation_id not in goal_statement.payload.get(
                "superseded_obligations", []
            ):
                invalid.append(f"goal_owner_did_not_authorize_supersession:{obligation_id}")
            if not disposition.residual_custodian:
                invalid.append(f"residual_custodian_missing:{obligation_id}")
        elif disposition.kind == "transfer":
            if not disposition.release_ref or not disposition.acceptance_ref:
                hold.append(f"transfer_acceptance_missing:{obligation_id}")
                continue
            if not grant_statement:
                continue
            release = statement_by_id.get(("obligation-release", disposition.release_ref))
            acceptance = statement_by_id.get(("obligation-acceptance", disposition.acceptance_ref))
            grant = grant_rows.get(disposition.authority_ref or "")
            if not release or not acceptance:
                hold.append(f"transfer_signatures_missing:{obligation_id}")
                continue
            if (
                release.issuer_id != disposition.from_owner
                or release.payload.get("scope") != cut.scope
                or release.payload.get("obligation_id") != obligation_id
                or release.payload.get("to_owner") != disposition.to_owner
                or release.payload.get("action") != obligation["action"]
            ):
                invalid.append(f"release_claim_mismatch:{obligation_id}")
            if (
                acceptance.issuer_id != disposition.to_owner
                or acceptance.payload.get("scope") != cut.scope
                or acceptance.payload.get("obligation_id") != obligation_id
                or acceptance.payload.get("from_owner") != disposition.from_owner
                or acceptance.payload.get("action") != obligation["action"]
                or acceptance.payload.get("revision") != cut.revision
            ):
                invalid.append(f"acceptance_claim_mismatch:{obligation_id}")
            if (
                not grant
                or grant["actor"] != disposition.to_owner
                or grant["action"] != obligation["action"]
                or grant["revision"] != cut.revision
                or grant["status"] != "pending"
                or not grant_statement
                or grant["issuer"] != grant_statement.issuer_id
            ):
                invalid.append(f"replacement_authority_invalid:{obligation_id}")
            else:
                transfer_grants.add(disposition.authority_ref)
        elif disposition.kind == "hold":
            if not disposition.residual_custodian:
                hold.append(f"hold_custodian_missing:{obligation_id}")
        else:
            invalid.append(f"unknown_disposition:{obligation_id}")

    if len(proposal.future_actions) != len({item.action_id for item in proposal.future_actions}):
        invalid.append("duplicate_future_action_identity")
    for action in proposal.future_actions:
        grant = grant_rows.get(action.grant_id)
        if not grant or grant["actor"] != action.actor or grant["action"] != action.action:
            invalid.append(f"action_authority_mismatch:{action.action_id}")
            continue
        if grant["status"] == "revoked":
            invalid.append(f"revoked_grant_used:{action.action_id}")
            continue
        if action.grant_id in transfer_grants:
            if action.dependency_revision != cut.revision:
                hold.append(f"dependent_action_stale:{action.action_id}")
                continue
            provider_statement = provider_by_capability.get(
                (action.provider_id, action.action, cut.revision)
            )
            if (
                action.provider_id is None
                or not provider_statement
                or (
                    provider_statement.payload.get("scope") != cut.scope
                    or provider_statement.payload.get("cut_id") != cut.cut_id
                    or provider_statement.payload.get("conditional_write") is not True
                )
            ):
                hold.append(f"provider_version_fence_unproven:{action.action_id}")
            else:
                eligible.append(action.action_id)
        elif action.dependency_revision is None and grant["status"] == "active":
            eligible.append(action.action_id)
        elif grant["status"] != "active":
            hold.append(f"grant_not_active:{action.action_id}")
        else:
            hold.append(f"unrecognized_action_dependency:{action.action_id}")

    decision = _decision(invalid, hold, incomplete, eligible)
    if decision.verdict != "VALID":
        return decision
    return decision


def _apply_transition(
    conn: Connection,
    cut: SourceCut,
    proposal: Proposal,
) -> None:
    conn.execute(
        """INSERT INTO transitions
           (scope, cut_id, transition_id, old_goal, new_goal, decision_ref, outcome)
           VALUES (%s, %s, %s, %s, %s, %s, 'accepted')""",
        (
            cut.scope,
            cut.cut_id,
            proposal.transition_id,
            proposal.old_goal,
            proposal.new_goal,
            proposal.goal_decision_ref,
        ),
    )
    execute_many(
        conn,
        """INSERT INTO dispositions
           (scope, cut_id, transition_id, obligation_id, kind, from_owner, to_owner,
            release_ref, acceptance_ref, authority_ref, decision_ref,
            residual_custodian, outcome_ref)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        [
            (
                cut.scope,
                cut.cut_id,
                proposal.transition_id,
                item.obligation_id,
                item.kind,
                item.from_owner,
                item.to_owner,
                item.release_ref,
                item.acceptance_ref,
                item.authority_ref,
                item.decision_ref,
                item.residual_custodian,
                item.outcome_ref,
            )
            for item in proposal.dispositions
        ],
    )
    for item in proposal.dispositions:
        if item.kind == "transfer" and item.authority_ref:
            changed = conn.execute(
                """UPDATE grants SET status='active'
                   WHERE scope=%s AND cut_id=%s AND grant_id=%s AND status='pending'""",
                (cut.scope, cut.cut_id, item.authority_ref),
            ).rowcount
            if changed != 1:
                raise psycopg.errors.SerializationFailure("pending grant changed during transition")


def evaluate_transition(
    conn: Connection,
    cut: SourceCut,
    proposal: Proposal,
    trust: TrustStore,
    now: datetime,
) -> Decision:
    try:
        with conn.transaction():
            conn.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            persist_cut(conn, cut)
            decision = _validate(conn, cut, proposal, trust, now)
            if decision.verdict != "VALID":
                raise _Rejected(decision)
            _apply_transition(conn, cut, proposal)
            return decision
    except _Rejected as rejected:
        return rejected.decision
    except SerializationFailure:
        return Decision("HOLD", ("source_or_authority_changed_during_serializable_transition",))
    except IntegrityError as exc:
        constraint = getattr(getattr(exc, "diag", None), "constraint_name", None)
        return Decision("INVALID", (f"database_constraint:{constraint or 'integrity'}",))
    except ValueError as exc:
        return Decision("INVALID", (str(exc),))
