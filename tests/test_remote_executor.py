"""Prove the service boundary using mock HTTP: NEVER mutate the real sandbox."""
from __future__ import annotations

from dataclasses import asdict

import httpx
from fastapi.testclient import TestClient

from continuity.api import Caller, create_app
from continuity.contracts import Intent
from continuity.github_executor import create_executor_app
from continuity.github_labels import GitHubLabelAdapter
from continuity.remote_executor import RemoteGitHubExecutor

REPO = "xiongweilin/continuity-runtime-phase-b-sandbox"
SECRET = "local-isolated-test-secret-at-least-32-characters"


def make_remote(setup):
    runtime, _ = setup
    state = {"labeled": False, "writes": 0, "reads": 0}

    def github_route(request: httpx.Request) -> httpx.Response:
        assert request.url.host == "api.github.com"
        assert request.url.path == f"/repos/{REPO}/issues/1/labels"
        if request.method == "POST":
            state["writes"] += 1
            state["labeled"] = True
        else:
            state["reads"] += 1
        return httpx.Response(
            200, json=[{"name": "continuity-effect-test"}] if
            state["labeled"] else [],
        )

    client = httpx.Client(
        base_url="https://api.github.com", transport=httpx.MockTransport(github_route),
    )
    adapter = GitHubLabelAdapter(
        repository=REPO, token="mock-credential-only-in-executor",
        allowed_labels={"continuity-effect-test"}, allowed_issue=1, client=client,
    )
    service = TestClient(create_executor_app(adapter=adapter, shared_secret=SECRET))

    def remote_route(request: httpx.Request) -> httpx.Response:
        response = service.request(
            request.method, request.url.path,
            headers=dict(request.headers), content=request.content,
        )
        return httpx.Response(
            response.status_code, content=response.content,
            headers=dict(response.headers),
        )

    proxy = RemoteGitHubExecutor(
        base_url="http://trusted-executor:9090", shared_secret=SECRET,
        client=httpx.Client(
            base_url="http://trusted-executor:9090",
            transport=httpx.MockTransport(remote_route),
        ),
    )
    runtime.adapters["github-label"] = proxy
    runtime.verifiers["github-label"] = proxy
    runtime.issue_grant(
        grant_id="phaseb-grant", principal="phaseb", actors=["agent-a", "agent-b"],
        provider="github-label", resource_prefix=f"github:issue:{REPO}/1",
        action="add_label",
    )
    intent = Intent(
        "phaseb-op", "phaseb", "github-label", f"github:issue:{REPO}/1",
        "add_label", {"label": "continuity-effect-test"}, "phaseb-grant",
        effect_key="issue-1-label-continuity-effect-test",
    )
    return runtime, service, state, intent


def test_mocked_remote_executor_separates_provider_write_and_goal_readback(setup):
    runtime, service, state, intent = make_remote(setup)
    body = {"intent": asdict(intent)}
    assert service.post("/v1/effects/execute", json=body).status_code == 401
    assert service.post("/v1/effects/execute",
        headers={"X-Continuity-Executor-Key": "bad"}, json=body).status_code == 401
    assert state["writes"] == 0

    runtime.submit(intent, actor="agent-a", participants=["agent-a", "agent-b"])
    result = runtime.dispatch(intent.operation_id, actor="agent-b")
    assert result["status"] == "pending_verification"
    assert state["writes"] == 1
    assert result["outcome_disposition"] == "unverified"

    runtime.reconcile(intent.operation_id, actor="agent-a")
    op = runtime.verify_outcome(intent.operation_id, actor="agent-b")
    assert op["effect_disposition"] == "present"
    assert op["outcome_disposition"] == "satisfied"
    assert state["reads"] == 2  # reconciliation and second, independent GET
    runtime.dispatch(intent.operation_id, actor="agent-a")
    assert state["writes"] == 1


def test_executor_rejects_wrong_issue_and_label_without_provider_call(setup):
    _, service, state, intent = make_remote(setup)
    base = asdict(intent)
    wrong_issue = {**base, "resource_ref": f"github:issue:{REPO}/2"}
    wrong_label = {**base, "payload": {"label": "unapproved"}}
    for wrong in (wrong_issue, wrong_label):
        result = service.post("/v1/effects/execute",
            headers={"X-Continuity-Executor-Key": SECRET}, json={"intent": wrong})
        assert result.status_code == 403
    assert state["writes"] == 0


def test_agent_authentication_does_not_grant_executor_access(setup):
    runtime, service, state, intent = make_remote(setup)
    api = TestClient(create_app(runtime, tokens={"agent-token": Caller("agent-a")}))
    assert service.post("/v1/effects/execute",
        headers={"X-Continuity-Executor-Key": "agent-token"},
        json={"intent": asdict(intent)}).status_code == 401
    assert api.post("/v1/grants", json={},
        headers={"Authorization": "Bearer agent-token"}).status_code == 403
    assert state["writes"] == 0
