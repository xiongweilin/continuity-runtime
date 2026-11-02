"""A *pure semantic oracle* for PCCT research traces, NOT a secure verifier.

Caller must resolve and authenticate all source-owner cuts before invoking.
No signatures, Merkle proofs, non-equivocation, live freshness or provider writes
are implemented here. These rules are prior-art-compatible by design.
"""
from __future__ import annotations

from typing import Any


def _unique_index(items: list[dict], key: str) -> dict[str, dict] | None:
    index: dict[str, dict] = {}
    for row in items:
        value = row.get(key)
        if not isinstance(value, str) or not value or value in index:
            return None
        index[value] = row
    return index


def check_transition(resolved: dict[str, Any], proposed: dict[str, Any]) -> dict[str, Any]:
    """Check a bounded and separately resolved evidence cut.

    This is a model-checking oracle with explicit trust assumptions, not a
    verifier of the authenticity/completeness of an untrusted submitter's data.
    """
    invalid: list[str] = []
    hold: list[str] = []
    incomplete: list[str] = []
    eligible: list[str] = []

    if resolved.get("scope") != proposed.get("scope") or not resolved.get("scope"):
        invalid.append("scope_mismatch")
    cuts = resolved.get("cuts", {})
    for owner in ("obligations", "effects", "source", "authority"):
        if cuts.get(owner) is not True:
            incomplete.append(f"unverified_{owner}_cut")
    if incomplete:
        return {"verdict": "INCOMPLETE_SCOPE", "reasons": sorted(set(incomplete)),
                "eligible_actions": []}

    if resolved.get("cut_token") != proposed.get("cut_token"):
        hold.append("stale_or_mismatched_cut_token")
    if not proposed.get("goal_decision_ref"):
        hold.append("missing_authoritative_goal_decision")
    if resolved.get("source_revision") != proposed.get("source_revision"):
        hold.append("source_revision_changed")
    if resolved.get("source_current_revision") != resolved.get("source_revision"):
        hold.append("source_changed_after_attested_cut")

    obligations = _unique_index(resolved.get("open_obligations", []), "id")
    dispositions = _unique_index(proposed.get("dispositions", []), "id")
    effects = _unique_index(resolved.get("effect_records", []), "effect_key")
    covered = proposed.get("historical_effect_keys", [])
    if obligations is None or dispositions is None or effects is None:
        invalid.append("duplicate_or_unidentified_source_or_disposition")
        obligations = obligations or {}
        dispositions = dispositions or {}
        effects = effects or {}
    if len(covered) != len(set(covered)):
        invalid.append("duplicate_historical_effect")
    if set(obligations) != set(dispositions):
        invalid.append("orphaned_or_invented_obligation")
    if set(effects) != set(covered):
        invalid.append("historical_effect_omission_or_invention")
    for key, item in effects.items():
        if item.get("disposition") == "unknown":
            hold.append(f"unresolved_effect:{key}")
        elif item.get("disposition") != "confirmed" or not item.get("readback_ref"):
            hold.append(f"unconfirmed_effect:{key}")
    grants = _unique_index(resolved.get("authority_grants", []), "id")
    if grants is None:
        invalid.append("duplicate_authority_grant")
        grants = {}

    for oid, d in dispositions.items():
        if oid not in obligations:
            continue
        existing = obligations[oid]
        disposition = d.get("kind")
        if d.get("from_owner") != existing.get("owner"):
            invalid.append(f"old_owner_mismatch:{oid}")
        if disposition == "retain":
            if d.get("to_owner") != existing.get("owner"):
                invalid.append(f"illegal_retain_owner_change:{oid}")
        elif disposition == "hold":
            if not d.get("custodian"):
                hold.append(f"missing_hold_custodian:{oid}")
        elif disposition == "fulfill":
            if not d.get("outcome_ref"):
                hold.append(f"missing_completion_witness:{oid}")
        elif disposition == "supersede":
            if not d.get("decision_ref") or not d.get("residual_custodian"):
                invalid.append(f"unauthorized_or_unaccountable_supersession:{oid}")
        elif disposition == "transfer":
            if not d.get("release_ref") or not d.get("accept_ref"):
                hold.append(f"unaccepted_transfer:{oid}")
            grant = grants.get(d.get("authority_ref"))
            if (not grant or grant.get("actor") != d.get("to_owner")
                    or grant.get("scope") != proposed.get("scope")
                    or grant.get("revision") != resolved.get("source_revision")
                    or not grant.get("active")):
                invalid.append(f"unauthorized_transfer:{oid}")
        else:
            invalid.append(f"invalid_disposition:{oid}")

    ids: set[str] = set()
    for action in proposed.get("future_actions", []):
        aid = action.get("id")
        if not isinstance(aid, str) or not aid or aid in ids:
            invalid.append("duplicate_or_missing_action_identity")
            continue
        ids.add(aid)
        grant = grants.get(action.get("grant_ref"))
        if (not grant or not grant.get("active")
                or grant.get("actor") != action.get("actor")
                or grant.get("scope") != proposed.get("scope")
                or grant.get("action") != action.get("action")):
            invalid.append(f"unauthorized_future_action:{aid}")
            continue
        dependency = action.get("dependency")
        if dependency is None:
            eligible.append(aid)  # Independent of changed source evidence.
        elif (dependency != resolved.get("source_revision")
              or grant.get("revision") != dependency):
            hold.append(f"stale_future_action:{aid}")
        elif action.get("provider_conditional_write") is not True:
            hold.append(f"no_remote_version_fence:{aid}")
        else:
            eligible.append(aid)
    if invalid:
        return {"verdict": "INVALID", "reasons": sorted(set(invalid)),
                "eligible_actions": []}
    if hold:
        return {"verdict": "HOLD", "reasons": sorted(set(hold)),
                "eligible_actions": sorted(eligible)}
    return {"verdict": "VALID", "reasons": [], "eligible_actions": sorted(eligible)}
