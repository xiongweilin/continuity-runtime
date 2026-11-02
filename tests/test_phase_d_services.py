"""Focused local tests for Phase D service state machines and ledgers."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from experiments.phase_c.provider_service import AppendOnlyLedger, read_verified_ledger
from experiments.phase_d import domain_owner
from experiments.phase_d.domain_owner import CASE_REF, EVIDENCE_REF, FACTS, DomainOwnerState
from experiments.phase_d.fault_proxy import ProxyState
from experiments.phase_d.provider_service import MOCK_PROVIDER, ProviderState
from experiments.phase_d.source_authority import SourceState

TRIAL = "phase-d-unit-test"
OWNER_KEY = "owner-control-key-0123456789-abcdefghijkl"
ORACLE_KEY = "provider-oracle-key-0123456789-abcdefgh"
SOURCE_KEY = "source-control-key-0123456789-abcdefg"
PROXY_KEY = "provider-proxy-key-0123456789-abcdefghij"
CLIENT_TOKEN = "proxy-client-token-0123456789-abcdefgh"


def _ledger(path: Path) -> AppendOnlyLedger:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()
    return AppendOnlyLedger(path)


def test_source_v1_v2_is_monotone_and_credential_protected(tmp_path: Path) -> None:
    path = tmp_path / "source.jsonl"
    ledger = _ledger(path)
    try:
        state = SourceState(
            trial_id=TRIAL, arm="continuity", control_key=SOURCE_KEY,
            ledger_path=path, ledger=ledger,
        )
        assert state.authenticate(SOURCE_KEY)
        assert not state.authenticate(OWNER_KEY)
        assert state.publish({
            "revision": "v1", "fact": FACTS["v1"], "evidence_ref": EVIDENCE_REF,
        })[0] == 201
        assert state.current()["revision"] == "v1"
        assert state.publish({
            "revision": "v2", "fact": FACTS["v2"], "evidence_ref": EVIDENCE_REF,
        })[0] == 201
        assert state.current()["revision"] == "v2"
        assert state.publish({
            "revision": "v2", "fact": FACTS["v2"], "evidence_ref": EVIDENCE_REF,
        })[0] == 409
        rows, _head, _raw = read_verified_ledger(path)
        assert [row["event"].get("revision") for row in rows
                if row["event"].get("type") == "source_revision_published"] == ["v1", "v2"]
    finally:
        ledger.close()


def test_provider_requires_distinct_write_and_oracle_credentials(tmp_path: Path) -> None:
    path = tmp_path / "provider.jsonl"
    ledger = _ledger(path)
    try:
        state = ProviderState(
            trial_id=TRIAL, arm="continuity", seed=0,
            proxy_key=PROXY_KEY, oracle_key=ORACLE_KEY, ledger=ledger,
        )
        headers = {
            "X-PhaseD-Provider-Key": PROXY_KEY,
            "X-PhaseD-Trial-ID": TRIAL,
            "X-PhaseD-Arm": "continuity",
            "X-PhaseD-Scenario": "normal_supersession",
        }
        body = {
            "trial_id": TRIAL,
            "operation_id": "op-pack-one",
            "actor": "agent-a",
            "provider": MOCK_PROVIDER,
            "resource_ref": CASE_REF,
            "action": "pack",
            "payload": {"case_ref": CASE_REF, "quantity": 1},
            "authority_id": "grant-pack-one",
            "source_revision": "v1",
            "effect_key": "phase-d-pack-one",
        }
        raw = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
        assert state.post_effect({}, raw)[0] == 401
        assert state.post_effect(headers, raw)[0] == 200
        body["operation_id"] = "op-pack-retry"
        raw_retry = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
        assert state.post_effect(headers, raw_retry)[1]["action_count"] == 2
        oracle_headers = {
            "X-PhaseD-Provider-Oracle-Key": ORACLE_KEY,
            "X-PhaseD-Trial-ID": TRIAL,
            "X-PhaseD-Arm": "continuity",
        }
        status, readback = state.read_effect(oracle_headers, CASE_REF, "pack")
        assert status == 200
        assert readback["action_count"] == 2
        assert readback["action_counts"] == {"pack": 2, "ship": 0, "quarantine": 0}
        assert readback["evidence_ref"]
        assert state.read_effect({}, CASE_REF)[0] == 401
    finally:
        ledger.close()


def test_ack_drop_is_single_use_and_pack_only(tmp_path: Path) -> None:
    path = tmp_path / "proxy.jsonl"
    ledger = _ledger(path)
    try:
        state = ProxyState(
            trial_id=TRIAL, arm="continuity", seed=0,
            scenario="pack_ack_lost_then_supersession",
            provider_url="http://127.0.0.1:25431",
            provider_key=PROXY_KEY, client_token=CLIENT_TOKEN, ledger=ledger,
        )
        response = {"accepted": True, "commit_ref": "mock-provider-commit:unit:1:sha256=abc"}
        assert state._drop_pack_ack("pack", 200, response, b'{"accepted":true}')
        assert not state._drop_pack_ack("pack", 200, response, b'{"accepted":true}')
        assert not state._drop_pack_ack("quarantine", 200, response, b'{"accepted":true}')
        rows, _head, _raw = read_verified_ledger(path)
        events = [row["event"] for row in rows]
        assert sum(event.get("type") == "fault_triggered" for event in events) == 1
        assert sum(event.get("type") == "client_response_dropped" for event in events) == 1
    finally:
        ledger.close()


def test_domain_owner_history_and_projection_are_domain_owned(
    tmp_path: Path, monkeypatch: Any,
) -> None:
    goal_path = tmp_path / "goal.jsonl"
    authority_path = tmp_path / "authority.jsonl"
    goal_ledger = _ledger(goal_path)
    authority_ledger = _ledger(authority_path)
    current = {"revision": "v1"}
    counts = {"pack": 1, "ship": 0, "quarantine": 0}

    def fake_get_json(url: str, _headers: dict[str, str]) -> dict[str, Any]:
        if url.endswith("/v1/source/current"):
            revision = current["revision"]
            return {
                "trial_id": TRIAL,
                "arm": "continuity",
                "revision": revision,
                "fact": FACTS[revision],
                "evidence_ref": EVIDENCE_REF,
                "source_event_ref": f"source-event:{revision}",
            }
        return {
            "action_counts": dict(counts),
            "evidence_ref": "provider-readback:authoritative",
            "trial_id": TRIAL,
            "arm": "continuity",
        }

    monkeypatch.setattr(domain_owner, "_http_get_json", fake_get_json)
    try:
        state = DomainOwnerState(
            trial_id=TRIAL,
            arm="continuity",
            goal_ledger_path=goal_path,
            authority_ledger_path=authority_path,
            source_url="http://127.0.0.1:12345",
            provider_url="http://127.0.0.1:23456",
            owner_key=OWNER_KEY,
            provider_oracle_key=ORACLE_KEY,
            goal_ledger=goal_ledger,
            authority_ledger=authority_ledger,
        )
        assert state.command("/v1/owner/goal-created", {})[0] == 201
        a_grant = state.command("/v1/owner/grant-agent-a", {})[1]
        assert a_grant["authority_ref"]
        assert state.command("/v1/owner/verify-pack", {})[1]["verified"] is True
        current["revision"] = "v2"
        assert state.command("/v1/owner/evidence-revised", {})[0] == 201
        assert state.command("/v1/owner/supersede", {})[0] == 201
        assert state.command("/v1/owner/revoke-agent-a", {"stale_dispatch_ref": "stale-ref"})[0] == 200
        assert state.command("/v1/owner/handoff", {})[0] == 201
        b_grant = state.command("/v1/owner/grant-agent-b", {})[1]
        status, projection = state.projection("agent-b")
        assert status == 200
        assert projection["authority_ref"] == b_grant["authority_ref"]
        assert set(projection) == {"actor", "goal", "basis", "verified_prior_work", "handoff_ref", "authority_ref"}
        assert projection["verified_prior_work"] == [{
            "action": "pack", "count": 1, "provider_readback_ref": "provider-readback:authoritative",
        }]
        counts["quarantine"] = 1
        outcome = state.command("/v1/owner/finalize", {})[1]
        assert outcome["goal_status"] == "satisfied"
        assert [event["type"] for event in state.goal_events] == [
            "goal_created", "evidence_revised", "goal_superseded",
            "handoff_authorized", "goal_satisfied",
        ]
        goal_rows, _head, _raw = read_verified_ledger(goal_path)
        assert len(goal_rows) == 5
    finally:
        authority_ledger.close()
        goal_ledger.close()
