"""Deterministic cross-agent handoff; NOT a real provider integration."""
from __future__ import annotations

from .contracts import Intent, Outcome, Readback, Receipt
from .db import RuntimeDB
from .runtime import ContinuityRuntime


class CounterAdapter:
    def __init__(self):
        self.effects = set()
        self.calls = 0

    def execute(self, intent: Intent) -> Receipt:
        self.calls += 1
        self.effects.add(intent.operation_id)
        return Receipt(True, "ack:" + intent.operation_id)

    def readback(self, intent: Intent) -> Readback:
        if intent.operation_id in self.effects:
            return Readback("present", "readback:" + intent.operation_id)
        return Readback("unknown")


class CounterVerifier:
    def verify(self, intent: Intent, effect: Readback) -> Outcome:
        return Outcome(True, "domain-verification:" + intent.operation_id)


def scenario(url: str = "sqlite:///:memory:") -> dict:
    db = RuntimeDB(url, create_schema=True)
    adapter = CounterAdapter()
    runtime = ContinuityRuntime(db, adapters={"counter": adapter},
                                verifiers={"counter": CounterVerifier()},
                                version_authorities={"policy:demo": lambda: "1"})
    runtime.issue_grant(grant_id="demo-grant", principal="demo-mission",
        actors=["agent-a", "agent-b"], provider="counter",
        resource_prefix="items", action="increment")
    runtime.observe(actor="agent-a", ref="policy:demo", source_revision="1",
                    evidence_ref="test:policy-v1")
    runtime.link(link_id="demo-handoff", actor="agent-a", source_ref="demo-mission",
        target_ref="items/example", relation="requires", purpose="handoff",
        basis_ref="demo-mission")
    intent = Intent("demo-op", "demo-mission", "counter", "items/example",
                    "increment", {"amount": 1}, "demo-grant", {"policy:demo": "1"})
    runtime.submit(intent, actor="agent-a", participants=["agent-a", "agent-b"])
    # The second Agent resumes; no shared session or workflow history.
    runtime.dispatch("demo-op", actor="agent-b")
    runtime.dispatch("demo-op", actor="agent-a")  # never dispatches twice
    runtime.reconcile("demo-op", actor="agent-a")
    op = runtime.verify_outcome("demo-op", actor="agent-b")
    report = {"status": op["status"], "effect": op["effect_disposition"],
        "outcome": op["outcome_disposition"], "provider_calls": adapter.calls,
        "events": len(runtime.events())}
    db.close()
    return report
