"""Focused tests for the independent PostgreSQL baseline; not Phase C trials."""
from __future__ import annotations

import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest

from experiments.phase_c.baseline_runtime import (
    BaselineConflict,
    BaselineDenied,
    BaselineRuntime,
)
from experiments.phase_c.shared import PhaseCGoalVerifier, PhaseCReadback, PhaseCReceipt

DATABASE_URL = os.getenv("PHASEC_TEST_POSTGRES_URL")
pytestmark = pytest.mark.skipif(
    not DATABASE_URL,
    reason="set PHASEC_TEST_POSTGRES_URL to a disposable PostgreSQL database",
)


class FakeAdapter:
    def __init__(self) -> None:
        self.calls = 0
        self.value = 0
        self.lock = threading.Lock()

    def execute(self, intent):
        with self.lock:
            self.calls += 1
            self.value += 1
            count = self.value
        return PhaseCReceipt(True, f"fake-receipt:{intent['operation_id']}:{count}")

    def readback(self, intent):
        with self.lock:
            count = self.value
        evidence = f"fake-readback:{intent['resource_ref']}:value={count}"
        return PhaseCReadback(
            "present" if count > 0 else "absent",
            evidence,
            reliable_no_effect=False,
            value=count,
        )


def _runtime():
    suffix = uuid.uuid4().hex
    revision = {"value": "v1"}
    ref = "test-source:" + suffix
    adapter = FakeAdapter()
    runtime = BaselineRuntime(
        DATABASE_URL,
        adapters={"phasec-mock": adapter},
        verifiers={"phasec-mock": PhaseCGoalVerifier()},
        version_readers={ref: lambda: revision["value"]},
        create_schema=True,
    )
    grant_id = "test-grant:" + suffix
    resource = "test-counter:" + suffix
    runtime.issue_grant(
        grant_id=grant_id,
        principal="test-principal",
        actors=["actor-a", "actor-b"],
        provider="phasec-mock",
        resource_prefix=resource,
        action="increment",
        required_version_refs=[ref],
    )
    runtime.observe_version(ref, "v1", evidence_ref="test-source-evidence:v1")
    def intent(operation_id: str, *, effect_key: str = "same"):
        return {
            "operation_id": operation_id,
            "principal": "test-principal",
            "provider": "phasec-mock",
            "resource_ref": resource,
            "action": "increment",
            "payload": {"delta": 1},
            "authority_id": grant_id,
            "expected_versions": {ref: "v1"},
            "effect_key": effect_key,
        }
    return runtime, adapter, revision, ref, grant_id, intent


def test_baseline_reserves_then_reconciles_and_verifies_goal():
    runtime, adapter, _revision, _ref, _grant_id, intent = _runtime()
    try:
        submitted = runtime.submit(
            intent("op:" + uuid.uuid4().hex),
            actor="actor-a",
            participants=["actor-a"],
        )
        reservation = []
        row = runtime.dispatch(
            submitted["id"],
            "actor-a",
            after_reservation=lambda operation_id: reservation.append(operation_id),
        )
        assert reservation == [submitted["id"]]
        assert row["status"] == "pending_verification"
        assert row["dispatch_generation"] == 1
        assert adapter.calls == 1
        row = runtime.reconcile(submitted["id"], "actor-a")
        assert row["status"] == "effect_confirmed"
        row = runtime.verify_outcome(submitted["id"], "actor-a")
        assert row["outcome_disposition"] == "satisfied"
        assert row["outcome_evidence_ref"].startswith("goal-verifier:")
    finally:
        runtime.close()


def test_baseline_crash_after_reservation_never_blindly_resends():
    runtime, adapter, _revision, _ref, _grant_id, intent = _runtime()
    try:
        submitted = runtime.submit(
            intent("op:" + uuid.uuid4().hex),
            actor="actor-a",
            participants=["actor-a"],
        )
        with pytest.raises(RuntimeError, match="injected crash point"):
            runtime.dispatch(
                submitted["id"],
                "actor-a",
                after_reservation=lambda _operation_id: (_ for _ in ()).throw(
                    RuntimeError("injected crash point")
                ),
            )
        reserved = runtime.get_operation(submitted["id"], actor="actor-a")
        assert reserved["status"] == "may_have_been_sent"
        assert reserved["dispatch_generation"] == 1
        resumed = runtime.dispatch(submitted["id"], "actor-a")
        assert resumed["status"] == "may_have_been_sent"
        assert adapter.calls == 0
        reconciled = runtime.reconcile(submitted["id"], "actor-a")
        assert reconciled["status"] == "unknown"
        assert reconciled["effect_disposition"] == "unknown"
        assert adapter.calls == 0
    finally:
        runtime.close()


def test_baseline_concurrent_same_effect_key_has_one_winner():
    runtime, _adapter, _revision, _ref, _grant_id, intent = _runtime()
    gate = threading.Barrier(2)
    try:
        def submit(actor, operation_id):
            gate.wait(timeout=10)
            return runtime.submit(
                intent(operation_id),
                actor=actor,
                participants=["actor-a", "actor-b"],
            )

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [
                pool.submit(submit, "actor-a", "op:" + uuid.uuid4().hex),
                pool.submit(submit, "actor-b", "op:" + uuid.uuid4().hex),
            ]
            successes = 0
            conflicts = 0
            for future in futures:
                try:
                    future.result(timeout=30)
                    successes += 1
                except BaselineConflict:
                    conflicts += 1
        assert (successes, conflicts) == (1, 1)
    finally:
        runtime.close()


def test_baseline_revocation_and_stale_source_fail_closed():
    runtime, adapter, revision, ref, grant_id, intent = _runtime()
    try:
        runtime.revoke_grant(grant_id)
        with pytest.raises(BaselineDenied):
            runtime.submit(
                intent("op:revoked:" + uuid.uuid4().hex),
                actor="actor-a",
                participants=["actor-a"],
            )
        assert adapter.calls == 0
    finally:
        runtime.close()

    runtime, adapter, revision, ref, _grant_id, intent = _runtime()
    try:
        submitted = runtime.submit(
            intent("op:stale:" + uuid.uuid4().hex),
            actor="actor-a",
            participants=["actor-a"],
        )
        revision["value"] = "v2"
        row = runtime.dispatch(submitted["id"], "actor-a")
        assert row["status"] == "blocked"
        assert row["dispatch_generation"] == 0
        assert adapter.calls == 0
    finally:
        runtime.close()
