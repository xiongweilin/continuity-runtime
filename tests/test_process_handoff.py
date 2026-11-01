"""Separate operating-system processes and separate databases; no shared Agent session."""
from __future__ import annotations

import json
import os
import subprocess
import sys


def test_two_processes_resume_with_independent_external_effect(tmp_path):
    env = os.environ.copy()
    env["CONTINUITY_DATABASE_URL"] = "sqlite:///" + str(tmp_path / "runtime.db")
    env["CONTINUITY_SAMPLE_PROVIDER_URL"] = "sqlite:///" + str(tmp_path / "external.db")
    def run(phase):
        result = subprocess.run(
            [sys.executable, "-m", "continuity.handoff", phase], env=env,
            capture_output=True, text=True, check=True,
        )
        return json.loads(result.stdout)
    assert run("start")["status"] == "registered"
    finished = run("finish")
    assert finished["effect"] == "present"
    assert finished["outcome"] == "satisfied"
    # Second run must not dispatch another external write.
    assert run("finish")["outcome"] == "satisfied"
