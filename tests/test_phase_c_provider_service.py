"""Unit tests for the independent mock-provider ledger (not experiment trials)."""
from __future__ import annotations

import json

import pytest

from experiments.phase_c.provider_service import (
    AppendOnlyLedger,
    LedgerIntegrityError,
    ProviderState,
    export_oracle,
    read_verified_ledger,
)


def _provider(tmp_path, *, scenario="clean"):
    path = tmp_path / "provider.jsonl"
    path.write_bytes(b"")
    path.chmod(0o600)
    ledger = AppendOnlyLedger(path)
    key = "test-only-" + "k" * 48
    state = ProviderState(
        trial_id="test-trial",
        arm="continuity",
        seed=0,
        scenario=scenario,
        proxy_key=key,
        ledger=ledger,
    )
    headers = {
        "X-PhaseC-Proxy-Key": key,
        "X-PhaseC-Trial-ID": "test-trial",
        "X-PhaseC-Arm": "continuity",
        "X-PhaseC-Scenario": scenario,
    }
    body = {
        "trial_id": "test-trial",
        "operation_id": "op:test",
        "principal": "test-principal",
        "provider": "phasec-mock",
        "resource_ref": "counter:test",
        "action": "increment",
        "payload": {"delta": 1},
        "effect_key": "effect:test",
    }
    return path, ledger, state, headers, body


def test_mock_provider_applies_every_duplicate_post_and_exports_hash_chained_oracle(tmp_path):
    path, ledger, state, headers, body = _provider(tmp_path)
    try:
        for _ in range(2):
            status, response = state.post_effect(headers, json.dumps(body).encode())
            assert status == 200
            assert response["accepted"] is True
            assert response["receipt_ref"]
        status, readback = state.read_effect(headers, "counter:test")
        assert status == 200
        assert readback["effect_present"] is True
        assert readback["value"] == 2
        assert "value=2" in readback["evidence_ref"]
    finally:
        ledger.close()

    oracle = export_oracle(path, expected_effect=True, scenario="clean")
    assert oracle["provider_post_count"] == 2
    assert oracle["commits"] == 2
    assert oracle["duplicate_commits"] == 1
    assert oracle["forbidden_commits"] == 0
    assert oracle["effect_present"] is True
    assert oracle["oracle_log_ref"].startswith("mock-provider-ledger:sha256:")
    rows, _, _ = read_verified_ledger(path)
    assert sum(row["event"]["type"] == "commit" for row in rows) == 2


def test_provider_authentication_rejects_direct_arm_post_without_commit(tmp_path):
    path, ledger, state, headers, body = _provider(tmp_path)
    try:
        headers.pop("X-PhaseC-Proxy-Key")
        status, _ = state.post_effect(headers, json.dumps(body).encode())
        assert status == 401
    finally:
        ledger.close()

    oracle = export_oracle(path, expected_effect=False, scenario="clean")
    assert oracle["provider_post_count"] == 1
    assert oracle["commits"] == 0
    assert oracle["duplicate_commits"] == 0
    assert oracle["forbidden_commits"] == 0
    assert oracle["effect_present"] is False


def test_provider_ledger_verifier_detects_post_export_tampering(tmp_path):
    path, ledger, state, headers, body = _provider(tmp_path)
    try:
        status, _ = state.post_effect(headers, json.dumps(body).encode())
        assert status == 200
    finally:
        ledger.close()
    original = path.read_bytes()
    assert original
    path.write_bytes(original.replace(b"counter:test", b"counter:evil", 1))
    with pytest.raises(LedgerIntegrityError):
        read_verified_ledger(path)
