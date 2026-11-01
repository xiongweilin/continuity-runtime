"""Real DBOS workflow/step history, separate from Continuity runtime state.

HTTP calls are routed through FastAPI TestClient to avoid production network I/O.
"""
from __future__ import annotations

import os
import uuid

import httpx
import pytest
from fastapi.testclient import TestClient

from continuity.api import Caller, create_app
from continuity.contracts import Intent


@pytest.mark.skipif(not os.getenv("CONTINUITY_TEST_POSTGRES_URL"),
                    reason="PostgreSQL required for real DBOS history")
def test_dbos_workflow_uses_same_effect_coordinator_across_engines(setup, monkeypatch):
    from dbos import DBOS, SetWorkflowID
    from examples import dbos_bridge as bridge

    runtime, adapter = setup
    runtime.submit(Intent("dbos-op", "mission", "counter", "items/one",
                          "increment", {"count": 1}, "grant"),
                   actor="agent-a", participants=["agent-a", "agent-b"])
    client = TestClient(create_app(runtime, tokens={"token-b": Caller("agent-b")}))

    def mock_route(request: httpx.Request) -> httpx.Response:
        result = client.request(request.method, request.url.path,
                                headers=dict(request.headers), content=request.content)
        return httpx.Response(result.status_code, content=result.content,
                              headers=dict(result.headers))

    def fake_client(*, base_url: str, headers: dict[str, str]) -> httpx.Client:
        return httpx.Client(base_url=base_url, headers=headers,
                            transport=httpx.MockTransport(mock_route))

    monkeypatch.setattr(bridge, "_api_client", fake_client)
    monkeypatch.setenv("CONTINUITY_API_BASE", "http://internal.test")
    monkeypatch.setenv("CONTINUITY_AGENT_TOKEN", "token-b")
    DBOS(config={"name": "continuity-" + uuid.uuid4().hex[:12],
                 "system_database_url": os.environ["CONTINUITY_TEST_POSTGRES_URL"],
                 "application_version": "0.1"})
    DBOS.launch()
    try:
        with SetWorkflowID("continuity:" + uuid.uuid4().hex):
            result = bridge.resume_handoff("dbos-op")
        assert result["effect_disposition"] == "present"
        # An independent direct Agent request after the DBOS workflow must not
        # produce a second provider call.
        assert runtime.dispatch("dbos-op", actor="agent-a")["dispatch_generation"] == 1
        assert adapter.calls == 1
    finally:
        DBOS.destroy()
