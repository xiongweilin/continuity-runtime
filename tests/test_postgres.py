"""A real PostgreSQL concurrency test runs in GitHub Actions; never simulates PG with SQLite."""
from __future__ import annotations

import os
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest

from continuity.contracts import Intent
from continuity.db import RuntimeDB
from continuity.demo import CounterAdapter
from continuity.runtime import ContinuityRuntime


@pytest.mark.skipif(not os.getenv("CONTINUITY_TEST_POSTGRES_URL"), reason="PostgreSQL not configured")
def test_concurrent_dispatch_across_independent_runtime_instances():
    url = os.environ["CONTINUITY_TEST_POSTGRES_URL"]
    db1 = RuntimeDB(url, create_schema=True)
    db2 = RuntimeDB(url)
    adapter = CounterAdapter()
    a = ContinuityRuntime(db1, adapters={"counter": adapter})
    b = ContinuityRuntime(db2, adapters={"counter": adapter})
    unique = uuid.uuid4().hex
    grant = "grant:" + unique
    operation = "op:" + unique
    a.issue_grant(grant_id=grant, principal="mission", actors=["a", "b"],
                  provider="counter", resource_prefix="items", action="increment")
    a.submit(Intent(operation, "mission", "counter", "items/one", "increment",
                    {"count": 1}, grant), actor="a", participants=["a", "b"])
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(runtime.dispatch, operation, actor=actor)
                   for runtime, actor in ((a, "a"), (b, "b"))]
        results = [future.result() for future in futures]
    assert all(result["dispatch_generation"] == 1 for result in results)
    assert adapter.calls == 1
    assert a.get_operation(operation, actor="a")["dispatch_generation"] == 1
    a.reconcile(operation, actor="a")
    assert b.get_operation(operation, actor="b")["effect_disposition"] == "present"
    db1.close()
    db2.close()


@pytest.mark.skipif(not os.getenv("CONTINUITY_TEST_POSTGRES_URL"),
                    reason="PostgreSQL not configured")
def test_concurrent_logical_effect_key_reservation_across_process_facades():
    """Two Agent IDs cannot reserve the same provider-scoped logical effect."""
    from continuity.contracts import Conflict

    url = os.environ["CONTINUITY_TEST_POSTGRES_URL"]
    a_db = RuntimeDB(url, create_schema=True)
    b_db = RuntimeDB(url)
    a = ContinuityRuntime(a_db)
    b = ContinuityRuntime(b_db)
    prefix = uuid.uuid4().hex
    grant = "grant:" + prefix
    a.issue_grant(grant_id=grant, principal="mission", actors=["a", "b"],
        provider="counter", resource_prefix="items", action="increment")

    def reserve(runtime, actor, ident):
        return runtime.submit(Intent(ident, "mission", "counter", "items/one",
            "increment", {"count": 1}, grant, effect_key="invoice:" + prefix),
            actor=actor, participants=["a", "b"])

    with ThreadPoolExecutor(max_workers=2) as pool:
        jobs = [pool.submit(reserve, a, "a", "op-a:" + prefix),
                pool.submit(reserve, b, "b", "op-b:" + prefix)]
        outcomes = []
        for job in jobs:
            try:
                outcomes.append(job.result())
            except Conflict:
                outcomes.append("conflict")
    assert len([x for x in outcomes if isinstance(x, dict)]) == 1
    assert outcomes.count("conflict") == 1
    a_db.close()
    b_db.close()



@pytest.mark.skipif(not os.getenv("CONTINUITY_TEST_POSTGRES_URL"),
                    reason="PostgreSQL required for concurrent committed event sequence test")
def test_event_sequence_cannot_overtake_uncommitted_transaction():
    from threading import Event

    url = os.environ["CONTINUITY_TEST_POSTGRES_URL"]
    db = RuntimeDB(url, create_schema=True)
    old = db.event_stream(limit=500)
    prior = old[-1]["seq"] if old else 0
    first_emitted = Event()
    release_first = Event()
    second_started = Event()

    def first_transaction():
        with db.engine.begin() as cx:
            number = db.emit(cx, "test:first", "test.order", {"id": "first"})
            first_emitted.set()
            if not release_first.wait(timeout=15):
                raise TimeoutError("test failed to release first transaction")
        return number

    def second_transaction():
        if not first_emitted.wait(timeout=10):
            raise TimeoutError("first event not ready")
        second_started.set()
        with db.engine.begin() as cx:
            return db.emit(cx, "test:second", "test.order", {"id": "second"})

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            f1 = pool.submit(first_transaction)
            assert first_emitted.wait(timeout=10)
            f2 = pool.submit(second_transaction)
            assert second_started.wait(timeout=10)
            # Second event cannot become visible while first has not committed.
            assert not f2.done()
            assert db.event_stream(after=prior) == []
            release_first.set()
            assert f1.result(timeout=15) == prior + 1
            assert f2.result(timeout=15) == prior + 2
        actual = db.event_stream(after=prior)
        assert [r["payload"]["id"] for r in actual] == ["first", "second"]
    finally:
        release_first.set()
        db.close()
