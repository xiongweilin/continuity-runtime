"""Prove the shipped tutorial covers one complete bounded workflow."""
from __future__ import annotations

from examples.canary_guarded_handoff import scenario


def test_canary_walkthrough_blocks_then_admits_without_bypass():
    result = scenario()
    assert result == {
        "denied_unsafe": True,
        "calls_after_denial": 0,
        "calls_total": 1,
        "dispatch_status": "pending_verification",
        "effect": "present",
        "outcome": "satisfied",
        "admission_profile": "canary-stage-v1",
    }
