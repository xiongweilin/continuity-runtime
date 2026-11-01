"""Internal-only bridge to a separately credentialed, narrowly scoped executor.

The runtime has a service authentication key, but never a GitHub provider token.
Network isolation is an additional deployment requirement, not a Python guarantee.
"""
from __future__ import annotations

from dataclasses import asdict
from pathlib import Path

import httpx

from .contracts import Intent, Outcome, Readback, Receipt


def read_secret_file(path: str, *, minimum_length: int = 32) -> str:
    if not path:
        raise ValueError("secret file path is required")
    secret = Path(path).read_text(encoding="utf-8").strip()
    if len(secret) < minimum_length:
        raise ValueError("executor credential is missing or too short")
    return secret


class RemoteGitHubExecutor:
    """EffectAdapter + DomainVerifier via an internal service."""

    def __init__(
        self, *, base_url: str, shared_secret: str,
        client: httpx.Client | None = None,
    ) -> None:
        if not base_url.startswith(("http://", "https://")):
            raise ValueError("explicit trusted executor URL is required")
        if len(shared_secret) < 32:
            raise ValueError("executor authentication secret must be at least 32 characters")
        self.client = client or httpx.Client(
            base_url=base_url, timeout=15, follow_redirects=False,
        )
        self.secret = shared_secret

    def _request(self, endpoint: str, intent: Intent) -> dict:
        response = self.client.post(
            "/v1/effects/" + endpoint,
            headers={"X-Continuity-Executor-Key": self.secret},
            json={"intent": asdict(intent)},
        )
        response.raise_for_status()
        body = response.json()
        if not isinstance(body, dict):
            raise ValueError("unexpected trusted executor response")
        return body

    def execute(self, intent: Intent) -> Receipt:
        return Receipt(**self._request("execute", intent))

    def readback(self, intent: Intent) -> Readback:
        return Readback(**self._request("readback", intent))

    def verify(self, intent: Intent, effect: Readback) -> Outcome:
        # A fresh provider GET, not the original effect receipt or stored readback.
        if effect.disposition != "present":
            return Outcome(None)
        return Outcome(**self._request("verify", intent))
