"""Focused tests for the independent Phase D baseline history layer."""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

from experiments.phase_d.baseline_history import (
    BaselineHistory,
    BaselineHistoryConflict,
    BaselineHistoryDenied,
)

SOURCE_REF = "source/phase-d-one/risk-decision"


def test_baseline_link_invalidation_is_append_only_and_owner_scoped() -> None:
    current = {"revision": "v1"}
    history = BaselineHistory(
        "sqlite:///:memory:", version_reader=lambda: current["revision"],
    )
    try:
        observed = history.observe(
            actor="agent-a", ref=SOURCE_REF, source_revision="v1",
            evidence_ref="source-event:v1", expected_revision=0,
        )
        assert observed["revision"] == 1
        link = history.link(
            link_id="phase-d-test-link", actor="agent-a",
            source_ref=SOURCE_REF, target_ref="prepare_shipment",
            relation="basis_for_goal", purpose="authorization",
            basis_ref="source-event:v1",
            constraints={"depends_on": {SOURCE_REF: "v1"}},
        )
        assert link["active"] is True
        with pytest.raises(BaselineHistoryDenied):
            history.get_link("phase-d-test-link", actor="agent-b")

        current["revision"] = "v2"
        changed = history.observe(
            actor="agent-a", ref=SOURCE_REF, source_revision="v2",
            evidence_ref="source-event:v2", expected_revision=1,
        )
        assert changed["revision"] == 2
        assert changed["invalidated"] == [{"link_id": "phase-d-test-link", "event_seq": 4}]
        assert history.get_link("phase-d-test-link", actor="agent-a")["active"] is False
        kinds = [event["kind"] for event in history.events()]
        assert kinds == [
            "observation.recorded", "link.updated", "observation.recorded",
            "dependency.invalidated",
        ]
    finally:
        history.engine.dispose()


def test_baseline_grant_version_pin_is_immutable_and_enforced() -> None:
    current = {"revision": "v1"}
    history = BaselineHistory(
        "sqlite:///:memory:", version_reader=lambda: current["revision"],
    )
    try:
        pin = history.pin_grant(
            grant_id="phase-d-pack-v1", action="pack", source_ref=SOURCE_REF,
            pinned_revision="v1", source_evidence_ref="source-event:v1",
            domain_authority_ref="owner-grant:pack-v1",
        )
        assert pin["pinned_revision"] == "v1"
        history.assert_grant_version(
            grant_id="phase-d-pack-v1", action="pack",
            expected_versions={SOURCE_REF: "v1"},
        )
        with pytest.raises(BaselineHistoryDenied):
            history.assert_grant_version(
                grant_id="phase-d-pack-v1", action="pack",
                expected_versions={SOURCE_REF: "v2"},
            )
        with pytest.raises(BaselineHistoryDenied):
            history.assert_grant_version(
                grant_id="phase-d-pack-v1", action="ship",
                expected_versions={SOURCE_REF: "v1"},
            )
        with pytest.raises(BaselineHistoryConflict):
            history.pin_grant(
                grant_id="phase-d-pack-v1", action="pack", source_ref=SOURCE_REF,
                pinned_revision="v1", source_evidence_ref="source-event:v1",
                domain_authority_ref="owner-grant:pack-v1",
            )
    finally:
        history.engine.dispose()


def test_control_arm_does_not_import_continuity_runtime() -> None:
    paths = (
        "experiments/phase_d/baseline_worker.py",
        "experiments/phase_d/baseline_history.py",
        "experiments/phase_d/baseline_outsider_probe.py",
        "experiments/phase_d/shared.py",
        "experiments/phase_c/baseline_runtime.py",
        "experiments/phase_c/shared.py",
    )
    for relative in paths:
        tree = ast.parse(Path(relative).read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                assert all(alias.name.split(".")[0] != "continuity" for alias in node.names), relative
            elif isinstance(node, ast.ImportFrom):
                assert (node.module or "").split(".")[0] != "continuity", relative
