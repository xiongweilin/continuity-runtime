from __future__ import annotations

import httpx
import pytest

from continuity.contracts import Intent
from continuity.github_labels import GitHubLabelAdapter, GitHubLabelVerifier


def test_allowlisted_github_label_with_independent_readback(setup):
    runtime, _ = setup
    requests = []
    def route(request):
        requests.append((request.method, str(request.url)))
        if request.method == "POST":
            return httpx.Response(200, json=[{"name": "triaged"}])
        return httpx.Response(200, json=[{"name": "triaged"}])
    client = httpx.Client(
        base_url="https://api.github.com", transport=httpx.MockTransport(route),
    )
    adapter = GitHubLabelAdapter(repository="example/project", token="dummy-test",
        allowed_labels={"triaged"}, client=client)
    runtime.adapters["github-label"] = adapter
    runtime.verifiers["github-label"] = GitHubLabelVerifier()
    runtime.issue_grant(grant_id="github-grant", principal="mission",
        actors=["agent-a", "agent-b"], provider="github-label",
        resource_prefix="github:issue:example/project/17", action="add_label")
    intent = Intent("label-17", "mission", "github-label",
        "github:issue:example/project/17", "add_label",
        {"label": "triaged"}, "github-grant")
    runtime.submit(intent, actor="agent-a", participants=["agent-a", "agent-b"])
    runtime.dispatch("label-17", actor="agent-b")
    runtime.reconcile("label-17", actor="agent-a")
    assert runtime.verify_outcome("label-17", actor="agent-b")["outcome_disposition"] == "satisfied"
    runtime.dispatch("label-17", actor="agent-a")
    assert len([x for x in requests if x[0] == "POST"]) == 1
    assert len([x for x in requests if x[0] == "GET"]) == 1


def test_github_adapter_rejects_unauthorized_resource_and_label():
    adapter = GitHubLabelAdapter(
        repository="example/project", token="dummy-test", allowed_labels={"triaged"},
        client=httpx.Client(base_url="https://api.github.com",
                            transport=httpx.MockTransport(lambda x: httpx.Response(500))),
    )
    bad_label = Intent("op", "mission", "github-label",
        "github:issue:example/project/17", "add_label", {"label": "admin"}, "g")
    bad_repo = Intent("op", "mission", "github-label",
        "github:issue:other/project/17", "add_label", {"label": "triaged"}, "g")
    with pytest.raises(ValueError):
        adapter.execute(bad_label)
    with pytest.raises(ValueError):
        adapter.execute(bad_repo)
