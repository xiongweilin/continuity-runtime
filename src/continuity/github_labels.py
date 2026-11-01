"""Optional real GitHub Issues label adapter with scoped repo and label allowlists.

Only suitable for a low-risk, idempotent 'label is present' effect. NOT a general
GitHub mutation adapter or an exactly-once-effect guarantee.
"""
from __future__ import annotations

import re

import httpx

from .contracts import Intent, Outcome, Readback, Receipt


class GitHubLabelAdapter:
    def __init__(self, *, repository: str, token: str, allowed_labels: set[str],
                 client: httpx.Client | None = None, allowed_issue: int | None = None) -> None:
        if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
            raise ValueError("an exact owner/repository is required")
        if not token or not allowed_labels:
            raise ValueError("a server-side token and explicit label allowlist are required")
        self.repository = repository
        if allowed_issue is not None and (type(allowed_issue) is not int or allowed_issue < 1):
            raise ValueError("an exact positive test issue number is required")
        self.allowed_issue = allowed_issue
        self.allowed_labels = frozenset(allowed_labels)
        self.client = client or httpx.Client(base_url="https://api.github.com", timeout=15)
        self.headers = {
            "Authorization": "Bearer " + token,
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

    def _location(self, intent: Intent) -> tuple[str, str]:
        if intent.provider != "github-label" or intent.action != "add_label":
            raise ValueError("unsupported provider operation")
        prefix = "github:issue:" + self.repository + "/"
        if not intent.resource_ref.startswith(prefix):
            raise ValueError("target outside configured repository")
        issue = intent.resource_ref[len(prefix):]
        if not issue.isdigit() or int(issue) < 1:
            raise ValueError("invalid issue number")
        if self.allowed_issue is not None and int(issue) != self.allowed_issue:
            raise ValueError("issue outside configured allowlist")
        label = intent.payload.get("label")
        if not isinstance(label, str) or label not in self.allowed_labels:
            raise ValueError("label outside allowlist")
        return issue, label

    def execute(self, intent: Intent) -> Receipt:
        number, label = self._location(intent)
        response = self.client.post(
            f"/repos/{self.repository}/issues/{number}/labels",
            headers=self.headers, json={"labels": [label]},
        )
        response.raise_for_status()
        return Receipt(True, f"github:label-response:{self.repository}/{number}/{label}")

    def readback(self, intent: Intent) -> Readback:
        number, label = self._location(intent)
        response = self.client.get(
            f"/repos/{self.repository}/issues/{number}/labels",
            headers=self.headers,
        )
        response.raise_for_status()
        values = response.json()
        if not isinstance(values, list):
            return Readback("unknown")
        labels = {v.get("name") for v in values if isinstance(v, dict)}
        if label in labels:
            return Readback("present", f"github:label-readback:{self.repository}/{number}/{label}")
        return Readback("unknown")  # absence now is not proof of historical no-effect


class GitHubLabelVerifier:
    """A domain goal that requires the intended label to be present."""
    def verify(self, intent: Intent, effect: Readback) -> Outcome:
        if effect.disposition != "present" or not effect.evidence_ref:
            return Outcome(None)
        return Outcome(True, "github:label-goal:" + intent.operation_id)
