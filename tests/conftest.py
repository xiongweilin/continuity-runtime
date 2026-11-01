from __future__ import annotations

import pytest

from continuity.db import RuntimeDB
from continuity.demo import CounterAdapter
from continuity.runtime import ContinuityRuntime


@pytest.fixture()
def setup(tmp_path):
    db = RuntimeDB("sqlite:///" + str(tmp_path / "continuity.db"), create_schema=True)
    adapter = CounterAdapter()
    runtime = ContinuityRuntime(db, adapters={"counter": adapter})
    runtime.issue_grant(
        grant_id="grant", principal="mission", actors=["agent-a", "agent-b"],
        provider="counter", resource_prefix="items", action="increment",
    )
    yield runtime, adapter
    db.close()
