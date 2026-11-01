"""Cursor safety: transactional event ordering, including rolls back."""
from __future__ import annotations

import pytest

from continuity.db import RuntimeDB


def test_rollback_reuses_committed_next_sequence(tmp_path):
    db = RuntimeDB("sqlite:///" + str(tmp_path / "events.db"), create_schema=True)
    before = db.event_stream()
    with pytest.raises(RuntimeError):
        with db.engine.begin() as cx:
            db.emit(cx, "work:uncommitted", "work.test", {"x": 1})
            raise RuntimeError("transaction rolled back")
    assert db.event_stream() == before
    with db.engine.begin() as cx:
        assigned = db.emit(cx, "work:committed", "work.test", {"x": 2})
    assert assigned == 1
    assert [r["seq"] for r in db.event_stream()] == [1]
    db.close()
