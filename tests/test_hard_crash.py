"""Hard process death after durable dispatch reservation, before any provider reply."""
from __future__ import annotations

import os
import subprocess
import sys

from continuity.contracts import Intent
from continuity.db import RuntimeDB
from continuity.demo import CounterAdapter
from continuity.runtime import ContinuityRuntime


def test_process_kill_after_reservation_never_blindly_dispatches_again(tmp_path):
    url = "sqlite:///" + str(tmp_path / "runtime.db")
    db = RuntimeDB(url, create_schema=True)
    runtime = ContinuityRuntime(db)
    runtime.issue_grant(grant_id="crash-grant", principal="mission",
        actors=["agent-a", "agent-b"], provider="counter",
        resource_prefix="items", action="increment")
    runtime.submit(Intent("crash-op", "mission", "counter", "items/one",
        "increment", {}, "crash-grant"),
        actor="agent-a", participants=["agent-a", "agent-b"])

    child = r"""
import os
from continuity.db import RuntimeDB
from continuity.runtime import ContinuityRuntime

class ProcessExit:
    def execute(self, intent):
        os._exit(97)
    def readback(self, intent):
        raise AssertionError("not reached")

runtime = ContinuityRuntime(RuntimeDB(os.environ["CRASH_RUNTIME_URL"]),
                            adapters={"counter": ProcessExit()})
runtime.dispatch("crash-op", actor="agent-b")
raise AssertionError("process was expected to die")
"""
    env = os.environ.copy()
    env["CRASH_RUNTIME_URL"] = url
    result = subprocess.run([sys.executable, "-c", child], env=env,
                            text=True, capture_output=True)
    assert result.returncode == 97, result.stderr
    # Recover using a different runtime object, without the original Agent.
    adapter = CounterAdapter()
    recovered = ContinuityRuntime(RuntimeDB(url), adapters={"counter": adapter})
    op = recovered.get_operation("crash-op", actor="agent-b")
    assert op["status"] == "may_have_been_sent"
    assert op["dispatch_generation"] == 1
    recovered.dispatch("crash-op", actor="agent-a")
    assert adapter.calls == 0
    assert recovered.reconcile("crash-op", actor="agent-b")["status"] == "unknown"
    recovered.db.close()
    db.close()
