"""Adversarial PCCT research traces, not empirical product or security tests."""
from __future__ import annotations

import copy
import importlib.util
from pathlib import Path

import pytest

PATH = Path(__file__).resolve().parents[1] / "research/pcct/semantic_oracle.py"
SPEC = importlib.util.spec_from_file_location("pcct_semantic_oracle", PATH)
assert SPEC and SPEC.loader
pcct = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pcct)


def state():
    resolved = {
        "scope": "purchase-7",
        "cut_token": "owner-cut-44",
        "cuts": {"obligations": True, "effects": True, "source": True, "authority": True},
        "source_revision": "v2", "source_current_revision": "v2",
        "open_obligations": [
            {"id": "verify-po", "owner": "agent-a", "action": "investigate"},
            {"id": "notify-supplier", "owner": "agent-a", "action": "notify"},
        ],
        "effect_records": [
            {"effect_key": "odoo-draft-07", "disposition": "confirmed",
             "readback_ref": "odoo:lookup-47"},
        ],
        "authority_grants": [
            {"id": "grant-b", "actor": "agent-b", "scope": "purchase-7",
             "action": "investigate", "revision": "v2", "active": True},
            {"id": "grant-c", "actor": "agent-c", "scope": "purchase-7",
             "action": "notify", "revision": "v1", "active": True},
        ],
    }
    proposed = {
        "scope": "purchase-7", "cut_token": "owner-cut-44",
        "goal_decision_ref": "owner:supersede-92", "source_revision": "v2",
        "historical_effect_keys": ["odoo-draft-07"],
        "dispositions": [
            {"id": "verify-po", "kind": "transfer", "from_owner": "agent-a",
             "to_owner": "agent-b", "release_ref": "release:19",
             "accept_ref": "accept:21", "authority_ref": "grant-b"},
            {"id": "notify-supplier", "kind": "supersede", "from_owner": "agent-a",
             "decision_ref": "owner:cancel-20", "residual_custodian": "procurement"},
        ],
        "future_actions": [
            {"id": "new-check", "actor": "agent-b", "action": "investigate",
             "grant_ref": "grant-b", "dependency": "v2",
             "provider_conditional_write": True},
            {"id": "unaffected", "actor": "agent-c", "action": "notify",
             "grant_ref": "grant-c", "dependency": None,
             "provider_conditional_write": False},
        ],
    }
    return copy.deepcopy(resolved), copy.deepcopy(proposed)


def verdict(resolved, proposed):
    return pcct.check_transition(resolved, proposed)


def test_clean_complete_scope():
    r, p = state()
    result = verdict(r, p)
    assert result == {"verdict": "VALID", "reasons": [],
                      "eligible_actions": ["new-check", "unaffected"]}


@pytest.mark.parametrize("remove", ["verify-po", "notify-supplier"])
def test_orphaned_obligation_rejected(remove):
    r, p = state()
    p["dispositions"] = [d for d in p["dispositions"] if d["id"] != remove]
    assert verdict(r, p)["verdict"] == "INVALID"


def test_unaccounted_historical_effect_rejected():
    r, p = state()
    p["historical_effect_keys"] = []
    assert "historical_effect_omission_or_invention" in verdict(r, p)["reasons"]


def test_effect_unknown_cannot_be_declared_disposed():
    r, p = state()
    r["effect_records"][0]["disposition"] = "unknown"
    assert verdict(r, p)["verdict"] == "HOLD"


def test_missing_owner_authorized_scope_is_not_a_negative_proof():
    r, p = state()
    r["cuts"]["obligations"] = False
    assert verdict(r, p)["verdict"] == "INCOMPLETE_SCOPE"


def test_effect_cut_missing_is_not_a_negative_proof():
    r, p = state()
    r["cuts"]["effects"] = False
    assert verdict(r, p)["verdict"] == "INCOMPLETE_SCOPE"


def test_transfer_without_recipient_acceptance_is_held():
    r, p = state()
    p["dispositions"][0]["accept_ref"] = None
    assert "unaccepted_transfer:verify-po" in verdict(r, p)["reasons"]


def test_transfer_with_unscoped_authority_rejected():
    r, p = state()
    r["authority_grants"][0]["scope"] = "other-case"
    assert verdict(r, p)["verdict"] == "INVALID"


def test_improper_old_owner_revision_rejected():
    r, p = state()
    p["dispositions"][0]["from_owner"] = "attacker"
    assert verdict(r, p)["verdict"] == "INVALID"


def test_stale_cut_holds_no_false_validity():
    r, p = state()
    r["source_current_revision"] = "v3"
    result = verdict(r, p)
    assert result["verdict"] == "HOLD"
    assert "source_changed_after_attested_cut" in result["reasons"]


def test_stale_action_holds_but_unrelated_action_eligible():
    r, p = state()
    p["future_actions"][0]["dependency"] = "v1"
    result = verdict(r, p)
    assert result["verdict"] == "HOLD"
    assert result["eligible_actions"] == ["unaffected"]


def test_no_provider_side_condition_holds():
    r, p = state()
    p["future_actions"][0]["provider_conditional_write"] = False
    assert "no_remote_version_fence:new-check" in verdict(r, p)["reasons"]


def test_duplicate_effect_or_obligation_record_rejected():
    r, p = state()
    p["historical_effect_keys"].append("odoo-draft-07")
    assert verdict(r, p)["verdict"] == "INVALID"
    r, p = state()
    p["dispositions"].append(p["dispositions"][0])
    assert verdict(r, p)["verdict"] == "INVALID"


def test_wrong_goal_owner_decision_cannot_return_valid():
    r, p = state()
    p["goal_decision_ref"] = None
    assert verdict(r, p)["verdict"] == "HOLD"


def test_supersession_without_residual_custodian_rejected():
    r, p = state()
    p["dispositions"][1]["residual_custodian"] = None
    assert verdict(r, p)["verdict"] == "INVALID"


def test_inactive_replacement_authority_rejected():
    r, p = state()
    r["authority_grants"][0]["active"] = False
    assert verdict(r, p)["verdict"] == "INVALID"


def test_unauthorized_future_action_rejected():
    r, p = state()
    p["future_actions"][0]["actor"] = "outsider"
    assert verdict(r, p)["verdict"] == "INVALID"


def test_deterministic_semantics_no_mutation():
    r, p = state()
    before = copy.deepcopy((r, p))
    assert verdict(r, p) == verdict(r, p)
    assert (r, p) == before
