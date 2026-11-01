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
