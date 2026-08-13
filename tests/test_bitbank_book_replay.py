"""bitbank whole/diff 线序重放的最小反例集。"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import duckdb

from guvolu.data.bitbank_book_replay import (
    bitbank_replay_actions,
    wire_session_sql,
)


@dataclass(frozen=True)
class F:
    key: str
    kind: str
    seq: int
    wire: int
    published: datetime


def _actions(frames: list[F]) -> list[tuple[str, bool, bool, bool]]:
    return [
        (a.frame.key, a.apply_levels, a.reset_book, a.attribute_changes)
        for a in bitbank_replay_actions(
            sorted(frames, key=lambda frame: frame.wire),
            message_kind=lambda frame: frame.kind,
            sequence_id=lambda frame: frame.seq,
            session_id=lambda _frame: "conn-1",
            available_time=lambda frame: frame.published,
        )
    ]


def test_delayed_whole_discards_equal_and_older_diff_replays_only_newer() -> None:
    now = datetime(2026, 8, 1, tzinfo=UTC)
    actions = _actions([
        F("d5", "delta", 5, 1, now + timedelta(seconds=9)),
        F("d6", "delta", 6, 2, now + timedelta(seconds=8)),
        F("d8", "delta", 8, 3, now + timedelta(seconds=7)),
        F("w5", "snapshot", 5, 4, now),
    ])
    assert actions == [
        ("d5", False, False, False),
        ("d6", False, False, False),
        ("d8", False, False, False),
        ("w5", True, True, False),
        ("d6", True, False, False),
        ("d8", True, False, False),
    ]


def test_equal_delta_after_whole_is_not_applied() -> None:
    now = datetime(2026, 8, 1, tzinfo=UTC)
    assert _actions([
        F("w5", "snapshot", 5, 1, now),
        F("d5", "delta", 5, 2, now + timedelta(seconds=1)),
    ]) == [
        ("w5", True, True, False),
        ("d5", False, False, False),
    ]


def test_publish_timestamp_inversion_does_not_change_wire_semantics() -> None:
    now = datetime(2026, 8, 1, tzinfo=UTC)
    actions = _actions([
        F("d6", "delta", 6, 1, now + timedelta(seconds=30)),
        F("w5", "snapshot", 5, 2, now),
    ])
    assert [row[0] for row in actions] == ["d6", "w5", "d6"]


def test_later_whole_replays_still_newer_applied_diff() -> None:
    now = datetime(2026, 8, 1, tzinfo=UTC)
    actions = _actions([
        F("w5", "snapshot", 5, 1, now),
        F("d6", "delta", 6, 2, now + timedelta(seconds=1)),
        F("d8", "delta", 8, 3, now + timedelta(seconds=2)),
        F("w7", "snapshot", 7, 4, now + timedelta(seconds=3)),
    ])
    assert [row[0] for row in actions] == ["w5", "d6", "d8", "w7", "d8"]
    assert actions[-1] == ("d8", True, False, False)


def test_same_room_duplicate_and_regression_are_not_applied() -> None:
    now = datetime(2026, 8, 1, tzinfo=UTC)
    actions = _actions([
        F("w5", "snapshot", 5, 1, now),
        F("d6", "delta", 6, 2, now + timedelta(seconds=1)),
        F("d6-duplicate", "delta", 6, 3, now + timedelta(seconds=2)),
        F("d4-regression", "delta", 4, 4, now + timedelta(seconds=3)),
        F("d7", "delta", 7, 5, now + timedelta(seconds=4)),
    ])
    assert actions == [
        ("w5", True, True, False),
        ("d6", True, False, True),
        ("d6-duplicate", False, False, False),
        ("d4-regression", False, False, False),
        ("d7", True, False, True),
    ]


def test_wire_session_spans_segments_only_with_recorded_connection() -> None:
    db = duckdb.connect(":memory:")
    try:
        rows = db.execute(
            f"""
            SELECT segment_sequence,
                   {wire_session_sql(
                       {"connection_id", "run_id", "segment_sequence"},
                       "segment_sequence",
                   )} AS wire_session
            FROM (VALUES
              (NULL, 'run-old', 1),
              (NULL, 'run-old', 2),
              ('run-new-c000001', 'run-new', 1),
              ('run-new-c000001', 'run-new', 2)
            ) t(connection_id,run_id,segment_sequence)
            ORDER BY run_id,segment_sequence
            """
        ).fetchall()
    finally:
        db.close()
    assert rows == [
        (1, "run-new-c000001"),
        (2, "run-new-c000001"),
        (1, "run-old/segment-1"),
        (2, "run-old/segment-2"),
    ]
