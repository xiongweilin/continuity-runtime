"""Two CLI invocations in separate processes share one mission and effect.

Example:
  CONTINUITY_DATABASE_URL=sqlite:///runtime.db \\
  CONTINUITY_SAMPLE_PROVIDER_URL=sqlite:///external.db python -m continuity.handoff start
  CONTINUITY_DATABASE_URL=sqlite:///runtime.db \\
  CONTINUITY_SAMPLE_PROVIDER_URL=sqlite:///external.db python -m continuity.handoff finish
"""
from __future__ import annotations

import json
import os
import sys

from .contracts import Intent
from .db import RuntimeDB
from .runtime import ContinuityRuntime
from .sample_connector import SampleExternalSystem, SampleOutcomeVerifier


def main() -> None:
    phase = sys.argv[1] if len(sys.argv) == 2 else ""
    if phase not in ("start", "finish"):
        raise SystemExit("usage: python -m continuity.handoff start|finish")
    url = os.environ["CONTINUITY_DATABASE_URL"]
    external = os.environ["CONTINUITY_SAMPLE_PROVIDER_URL"]
    db = RuntimeDB(url, create_schema=(phase == "start"))
    adapter = SampleExternalSystem(external)
    runtime = ContinuityRuntime(db, adapters={"sample": adapter},
                                verifiers={"sample": SampleOutcomeVerifier()},
                                version_authorities={"policy:handoff": lambda: "1"})
    if phase == "start":
        runtime.issue_grant(
            grant_id="handoff-grant", principal="handoff-mission",
            actors=["agent-a", "agent-b"], provider="sample",
            resource_prefix="sample/items", action="record",
        )
        runtime.observe(actor="agent-a", ref="policy:handoff", source_revision="1",
                        evidence_ref="sample:policy-v1")
        runtime.link(
            link_id="handoff-relation", actor="agent-a",
            source_ref="handoff-mission", target_ref="sample/items/1",
            relation="requires", purpose="handoff",
            basis_ref="mission:handoff",
            constraints={"depends_on": {"policy:handoff": "1"}},
        )
        runtime.submit(Intent(
            "handoff-op", "handoff-mission", "sample", "sample/items/1",
            "record", {"value": "done"}, "handoff-grant", {"policy:handoff": "1"},
        ), actor="agent-a", participants=["agent-a", "agent-b"])
        print(json.dumps({"phase": "agent-a", "status": "registered"}))
    else:
        runtime.dispatch("handoff-op", actor="agent-b")
        runtime.reconcile("handoff-op", actor="agent-b")
        result = runtime.verify_outcome("handoff-op", actor="agent-b")
        print(json.dumps({
            "phase": "agent-b", "status": result["status"],
            "effect": result["effect_disposition"],
            "outcome": result["outcome_disposition"],
        }))
    db.close()


if __name__ == "__main__":
    main()
