"""Run in an uncredentialed Agent container; no provider write is attempted."""
from __future__ import annotations

import os
import socket

FORBIDDEN = (
    "GITHUB_TOKEN", "GH_TOKEN", "CONTINUITY_GITHUB_TOKEN",
    "CONTINUITY_GITHUB_TOKEN_FILE", "CONTINUITY_EXECUTOR_KEY_FILE",
    "CONTINUITY_EXECUTOR_URL", "CONTINUITY_PHASE_B_TOKEN_FILE",
    "CONTINUITY_PHASE_B_EXECUTOR_KEY_FILE",
)
for name in FORBIDDEN:
    assert name not in os.environ, "Agent received a privileged setting: " + name
for path in (
    "/run/secrets/phase_b_github_token", "/run/secrets/continuity_executor_key",
):
    assert not os.path.exists(path), "Agent can read a trusted credential mount"
try:
    socket.getaddrinfo("trusted-executor", 9090)
except socket.gaierror:
    pass  # Not on the trusted effects network.
else:
    raise AssertionError("Agent can resolve trusted executor on an unauthorized network")
assert socket.getaddrinfo("api", 8080), "Agent cannot resolve the public runtime API"
print("isolation probe: no agent secrets, no trusted-executor DNS, Runtime API reachable")
