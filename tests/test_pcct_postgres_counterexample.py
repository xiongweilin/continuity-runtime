from __future__ import annotations

import os
import uuid
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import psycopg
import pytest
from psycopg import sql

from research.pcct_counterexample.fixtures import (
    build_case_evidence,
    clean_proposal,
    fixed_clock,
    load_cut,
)
from research.pcct_counterexample.model import FutureAction
from research.pcct_counterexample.policy import evaluate_transition

FORMATS = ("domain-events", "workflow-history")
SCHEMA = Path(__file__).parents[1] / "research/pcct_counterexample/schema.sql"


@pytest.fixture
def database():
    dsn = os.environ.get("CONTINUITY_PCCT_TEST_DSN")
    if not dsn:
        pytest.skip("CONTINUITY_PCCT_TEST_DSN is not configured")
    conn = psycopg.connect(dsn, autocommit=True)
    schema_name = f"pcct_{uuid.uuid4().hex[:16]}"
    conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name)))
    conn.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema_name)))
    conn.execute(SCHEMA.read_text(encoding="utf-8"), prepare=False)
    try:
        yield conn
    finally:
        conn.execute("SET search_path TO public")
        conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema_name)))
        conn.close()


def evaluate(database, source_format, **evidence_options):
    now = fixed_clock()
    evidence = build_case_evidence(source_format, now, **evidence_options)
    proposal = clean_proposal(evidence.cut)
    decision = evaluate_transition(
        database, evidence.cut, proposal, evidence.signers.trust_store, now
    )
    return decision, evidence, proposal


def test_independent_formats_normalize_the_same_source_owned_facts():
    event_cut = load_cut("domain-events")
    history_cut = load_cut("workflow-history")
    assert event_cut.scope == history_cut.scope
    assert [item.wire() for item in event_cut.obligations] == [
        item.wire() for item in history_cut.obligations
    ]
    assert [item.wire() for item in event_cut.effects] == [
        item.wire() for item in history_cut.effects
    ]
    assert [item.wire() for item in event_cut.grants] == [
        item.wire() for item in history_cut.grants
    ]
    assert (event_cut.goal_from, event_cut.goal_to, event_cut.goal_decision_ref) == (
        history_cut.goal_from,
        history_cut.goal_to,
        history_cut.goal_decision_ref,
    )


@pytest.mark.parametrize("source_format", FORMATS)
def test_complete_cut_terminates_old_goal_and_atomically_transfers_authority(
    database, source_format
):
    decision, evidence, proposal = evaluate(database, source_format)
    assert decision.wire() == {
        "verdict": "VALID",
        "reasons": [],
        "eligible_actions": ["new-check", "unaffected-notice"],
    }
    assert database.execute("SELECT old_goal, new_goal, outcome FROM transitions").fetchone() == (
        "purchase-v1",
        "investigate-v2",
        "accepted",
    )
    assert (
        database.execute(
            "SELECT COUNT(*) FROM dispositions WHERE transition_id=%s",
            (proposal.transition_id,),
        ).fetchone()[0]
        == 2
    )
    assert (
        database.execute("SELECT status FROM grants WHERE grant_id='grant-a-v1'").fetchone()[0]
        == "revoked"
    )
    assert (
        database.execute("SELECT status FROM grants WHERE grant_id='grant-b-v2'").fetchone()[0]
        == "active"
    )
    assert evidence.cut.source_format in {"append-only-domain-events/v1", "workflow-history/v1"}


@pytest.mark.parametrize("source_format", FORMATS)
def test_second_open_obligation_cannot_be_orphaned(database, source_format):
    evidence = build_case_evidence(source_format)
    proposal = clean_proposal(evidence.cut)
    proposal = replace(proposal, dispositions=(proposal.dispositions[0],))
    decision = evaluate_transition(
        database, evidence.cut, proposal, evidence.signers.trust_store, fixed_clock()
    )
    assert decision.verdict == "INVALID"
    assert "open_obligation_omitted_or_invented" in decision.reasons
    assert database.execute("SELECT COUNT(*) FROM source_revisions").fetchone()[0] == 0


@pytest.mark.parametrize("source_format", FORMATS)
def test_confirmed_prior_effect_cannot_be_omitted(database, source_format):
    evidence = build_case_evidence(source_format)
    proposal = replace(clean_proposal(evidence.cut), historical_effect_keys=())
    decision = evaluate_transition(
        database, evidence.cut, proposal, evidence.signers.trust_store, fixed_clock()
    )
    assert decision.verdict == "INVALID"
    assert "historical_effect_omission_or_invention" in decision.reasons


@pytest.mark.parametrize("source_format", FORMATS)
def test_duplicate_successor_disposition_is_rejected(database, source_format):
    evidence = build_case_evidence(source_format)
    proposal = clean_proposal(evidence.cut)
    proposal = replace(proposal, dispositions=proposal.dispositions + (proposal.dispositions[0],))
    decision = evaluate_transition(
        database, evidence.cut, proposal, evidence.signers.trust_store, fixed_clock()
    )
    assert decision.verdict == "INVALID"
    assert "duplicate_successor_disposition" in decision.reasons


@pytest.mark.parametrize("source_format", FORMATS)
def test_transfer_without_recipient_acceptance_stays_on_hold(database, source_format):
    evidence = build_case_evidence(source_format)
    proposal = clean_proposal(evidence.cut)
    transfer = replace(proposal.dispositions[0], acceptance_ref=None)
    proposal = replace(proposal, dispositions=(transfer, proposal.dispositions[1]))
    decision = evaluate_transition(
        database, evidence.cut, proposal, evidence.signers.trust_store, fixed_clock()
    )
    assert decision.verdict == "HOLD"
    assert "transfer_acceptance_missing:verify-po-07" in decision.reasons


@pytest.mark.parametrize("source_format", FORMATS)
@pytest.mark.parametrize("tamper_kind", ["release", "acceptance"])
def test_altered_transfer_signature_is_rejected(database, source_format, tamper_kind):
    decision, _, _ = evaluate(database, source_format, tamper_kind=tamper_kind)
    assert decision.verdict == "INVALID"
    assert "bad_signature" in " ".join(decision.reasons)


@pytest.mark.parametrize("source_format", FORMATS)
def test_stale_source_cut_holds_transition(database, source_format):
    def advance_source(document):
        if source_format == "domain-events":
            document[0]["data"]["current_revision"] = "v3"
        else:
            document["snapshot"]["current_revision"] = "v3"

    decision, _, _ = evaluate(database, source_format, mutate=advance_source, head_revision="v3")
    assert decision.verdict == "HOLD"
    assert "source_changed_after_attested_cut" in decision.reasons


@pytest.mark.parametrize("source_format", FORMATS)
def test_unknown_prior_effect_is_held_not_erased(database, source_format):
    def make_effect_unknown(document):
        if source_format == "domain-events":
            effect = next(row for row in document if row["event"] == "provider.effect")
            effect["data"]["status"] = "unknown"
            effect["data"].pop("readback_ref", None)
        else:
            effect = next(
                row
                for row in document["workflow_history"]
                if row["event_type"] == "ExternalOperationReadBack"
            )
            effect["result"]["result"] = "unknown"
            effect["result"].pop("readback", None)

    decision, _, _ = evaluate(
        database,
        source_format,
        mutate=make_effect_unknown,
        effects_override=[
            {"effect_key": "erp-draft-po-07", "status": "unknown", "readback_ref": None}
        ],
    )
    assert decision.verdict == "HOLD"
    assert "unknown_prior_effect:erp-draft-po-07" in decision.reasons


@pytest.mark.parametrize("source_format", FORMATS)
def test_missing_source_completeness_is_incomplete_scope(database, source_format):
    decision, _, _ = evaluate(
        database,
        source_format,
        completeness={"source": True, "obligations": False},
    )
    assert decision.verdict == "INCOMPLETE_SCOPE"
    assert "source_owner_did_not_attest_complete_cut" in decision.reasons
    assert database.execute("SELECT COUNT(*) FROM transitions").fetchone()[0] == 0


@pytest.mark.parametrize("source_format", FORMATS)
def test_unknown_source_issuer_is_incomplete_scope(database, source_format):
    decision, _, _ = evaluate(database, source_format, unknown_source_issuer=True)
    assert decision.verdict == "INCOMPLETE_SCOPE"
    assert any(reason.startswith("unknown_issuer") for reason in decision.reasons)


@pytest.mark.parametrize("source_format", FORMATS)
def test_expired_source_cut_is_incomplete_scope(database, source_format):
    decision, _, _ = evaluate(
        database, source_format, expires_at=fixed_clock() - timedelta(seconds=1)
    )
    assert decision.verdict == "INCOMPLETE_SCOPE"
    assert "expired_source-cut" in decision.reasons


@pytest.mark.parametrize("source_format", FORMATS)
def test_changed_signed_digest_is_invalid(database, source_format):
    decision, _, _ = evaluate(database, source_format, tamper_kind="source-cut")
    assert decision.verdict == "INVALID"
    assert "bad_signature" in " ".join(decision.reasons)


@pytest.mark.parametrize("source_format", FORMATS)
def test_equivocal_signed_cuts_are_rejected(database, source_format):
    decision, _, _ = evaluate(database, source_format, equivocal=True)
    assert decision.verdict == "INVALID"
    assert "equivocal_source-cut" in decision.reasons


@pytest.mark.parametrize("source_format", FORMATS)
def test_former_agent_cannot_self_issue_replacement_authority(database, source_format):
    decision, _, _ = evaluate(database, source_format, self_issued_grant=True)
    assert decision.verdict == "INVALID"
    assert "issuer_role_not_authorized" in " ".join(decision.reasons)


@pytest.mark.parametrize("source_format", FORMATS)
def test_revoked_old_grant_cannot_be_reused(database, source_format):
    evidence = build_case_evidence(source_format)
    proposal = clean_proposal(evidence.cut)
    stale_action = FutureAction("stale-old-agent", "agent-a", "investigate", "grant-a-v1", None)
    proposal = replace(proposal, future_actions=proposal.future_actions + (stale_action,))
    decision = evaluate_transition(
        database, evidence.cut, proposal, evidence.signers.trust_store, fixed_clock()
    )
    assert decision.verdict == "INVALID"
    assert "revoked_grant_used:stale-old-agent" in decision.reasons


@pytest.mark.parametrize("source_format", FORMATS)
def test_stale_dependent_action_does_not_block_unrelated_action(database, source_format):
    evidence = build_case_evidence(source_format)
    proposal = clean_proposal(evidence.cut)
    stale_dependent = replace(proposal.future_actions[0], dependency_revision="v1")
    proposal = replace(
        proposal,
        future_actions=(stale_dependent, proposal.future_actions[1]),
    )
    decision = evaluate_transition(
        database, evidence.cut, proposal, evidence.signers.trust_store, fixed_clock()
    )
    assert decision.verdict == "HOLD"
    assert decision.eligible_actions == ("unaffected-notice",)


@pytest.mark.parametrize("source_format", FORMATS)
def test_goal_termination_requires_matching_domain_owner_decision(database, source_format):
    evidence = build_case_evidence(source_format)
    proposal = replace(clean_proposal(evidence.cut), goal_decision_ref="other-decision")
    decision = evaluate_transition(
        database, evidence.cut, proposal, evidence.signers.trust_store, fixed_clock()
    )
    assert decision.verdict == "INVALID"
    assert "goal_termination_not_authorized_for_transition" in decision.reasons


@pytest.mark.parametrize("source_format", FORMATS)
def test_successor_must_have_exact_scoped_pending_authority(database, source_format):
    evidence = build_case_evidence(source_format)
    evidence.cut.grants = [
        replace(grant, actor="agent-c") if grant.grant_id == "grant-b-v2" else grant
        for grant in evidence.cut.grants
    ]
    # The authority owner attestation remains bound to the original grant list.
    proposal = clean_proposal(evidence.cut)
    decision = evaluate_transition(
        database, evidence.cut, proposal, evidence.signers.trust_store, fixed_clock()
    )
    assert decision.verdict == "INVALID"
    assert "grant_register_attestation_mismatch" in decision.reasons


@pytest.mark.parametrize("source_format", FORMATS)
def test_missing_effect_owner_completeness_is_incomplete_scope(database, source_format):
    decision, _, _ = evaluate(database, source_format, include_effect_index=False)
    assert decision.verdict == "INCOMPLETE_SCOPE"
    assert "effect_index_not_usable" in decision.reasons


@pytest.mark.parametrize("source_format", FORMATS)
def test_dependent_action_needs_signed_provider_version_fence(database, source_format):
    decision, _, _ = evaluate(database, source_format, include_provider=False)
    assert decision.verdict == "HOLD"
    assert "provider_version_fence_unproven:new-check" in decision.reasons


@pytest.mark.parametrize("source_format", FORMATS)
def test_provider_cannot_claim_an_unsupported_conditional_write(database, source_format):
    decision, _, _ = evaluate(database, source_format, provider_supported=False)
    assert decision.verdict == "HOLD"
    assert "provider_version_fence_unproven:new-check" in decision.reasons


@pytest.mark.parametrize("source_format", FORMATS)
def test_goal_owner_must_name_obligation_before_supersession(database, source_format):
    evidence = build_case_evidence(source_format)
    proposal = clean_proposal(evidence.cut)
    transfer = replace(
        proposal.dispositions[0],
        kind="supersede",
        to_owner=None,
        release_ref=None,
        acceptance_ref=None,
        authority_ref=None,
        decision_ref=proposal.goal_decision_ref,
        residual_custodian="procurement-desk",
    )
    proposal = replace(
        proposal,
        dispositions=(transfer, proposal.dispositions[1]),
        future_actions=(proposal.future_actions[1],),
    )
    decision = evaluate_transition(
        database, evidence.cut, proposal, evidence.signers.trust_store, fixed_clock()
    )
    assert decision.verdict == "INVALID"
    assert "goal_owner_did_not_authorize_supersession:verify-po-07" in decision.reasons


@pytest.mark.parametrize("source_format", FORMATS)
def test_fulfillment_reference_without_signed_outcome_stays_on_hold(database, source_format):
    evidence = build_case_evidence(source_format)
    proposal = clean_proposal(evidence.cut)
    fulfilled = replace(
        proposal.dispositions[1], kind="fulfill", outcome_ref="outcome:notice-fulfilled"
    )
    proposal = replace(proposal, dispositions=(proposal.dispositions[0], fulfilled))
    decision = evaluate_transition(
        database, evidence.cut, proposal, evidence.signers.trust_store, fixed_clock()
    )
    assert decision.verdict == "HOLD"
    assert "completion_witness_missing:notify-supplier" in decision.reasons


@pytest.mark.parametrize("source_format", FORMATS)
def test_postgres_rejects_two_active_grants_for_one_action(database, source_format):
    def add_conflicting_grant(document):
        duplicate = {
            "id": "grant-d-notify",
            "scope": "purchase-7",
            "actor": "agent-d",
            "action": "notify",
            "revision": "v2",
            "status": "active",
            "issuer_id": "authority-owner",
        }
        if source_format == "domain-events":
            event = next(row for row in document if row["event"] == "authorization.snapshot")
            event["data"]["grants"].append(duplicate)
        else:
            event = next(
                row
                for row in document["workflow_history"]
                if row["event_type"] == "AuthorizationProjection"
            )
            event["result"]["authorizations"].append(
                {
                    "authorization_id": duplicate["id"],
                    "case": duplicate["scope"],
                    "principal": duplicate["actor"],
                    "capability": duplicate["action"],
                    "evidence_revision": duplicate["revision"],
                    "state": duplicate["status"],
                    "issuer": duplicate["issuer_id"],
                }
            )

    decision, _, _ = evaluate(database, source_format, mutate=add_conflicting_grant)
    assert decision.verdict == "INVALID"
    assert any(
        reason.startswith("database_constraint:one_active_grant_per_action")
        for reason in decision.reasons
    )
    assert database.execute("SELECT COUNT(*) FROM grants").fetchone()[0] == 0
