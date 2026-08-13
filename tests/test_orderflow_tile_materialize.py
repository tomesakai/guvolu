"""OFL tile 相邻帧连续性状态机的精准契约测试。"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

from guvolu.data.orderflow_tile_materialize import (
    Frame,
    Level,
    TileBuild,
    _load_frames,
    build_tiles,
    main,
)
from guvolu.data.l2_source_precedence import okx_live_event_coverage
from guvolu.ui.query_catalog import ActiveOutput, ActiveOutputSnapshot


START = datetime(2026, 8, 1, tzinfo=UTC)
MARKET = {
    "market_id": "mkt__test__btc_jpy__r0",
    "venue_id": "test",
    "tick_size": "1",
}


def _frame(
    offset: float,
    kind: str,
    source_run: str,
    levels: tuple[Level, ...],
    *,
    available_offset: float | None = None,
) -> Frame:
    return Frame(
        f"frame-{offset}-{kind}", kind,
        START + timedelta(seconds=offset),
        START + timedelta(seconds=(offset if available_offset is None else available_offset)),
        "snapshot_no_sequence", source_run, levels,
    )


def _column(build: TileBuild, offset: int) -> tuple[object, ...]:
    epoch = int(START.timestamp()) + offset
    return next(row for row in build.columns if row[6] == epoch)


def test_gap_delta_and_snapshot_in_same_bucket_reanchor_next_clean_bucket() -> None:
    frames = (
        _frame(0, "snapshot", "run-a", (Level("ask", Decimal("100"), Decimal("1"), "set"),)),
        _frame(40.1, "delta", "run-a", (Level("ask", Decimal("100"), Decimal("0.5"), "set"),)),
        _frame(40.2, "snapshot", "run-a", (Level("ask", Decimal("101"), Decimal("2"), "set"),)),
    )
    trade_epoch = int(START.timestamp()) + 40
    build = build_tiles(
        MARKET, frames, {(trade_epoch, "buy", 100): (Decimal("0.25"), 1)},
        START, START + timedelta(seconds=50), "5s", "sha256-source",
    )

    interrupted = _column(build, 40)
    assert interrupted[9] == "gap"
    assert interrupted[10] is False  # 缺口桶不能作为锚点
    assert interrupted[11] is True
    assert interrupted[13] is True
    assert interrupted[15] == 1
    gap_trade_cells = [row for row in build.cells if row[6] == trade_epoch]
    assert len(gap_trade_cells) == 1
    assert gap_trade_cells[0][10] is None
    assert gap_trade_cells[0][13:15] == ("0.25", "0")

    recovered = _column(build, 45)
    assert recovered[9] == "carried"
    assert recovered[10] is True
    assert recovered[13] is False
    recovered_cells = [row for row in build.cells if row[6] == trade_epoch + 5]
    assert [(row[8], row[10], row[15]) for row in recovered_cells] == [
        (50, "2", "anchor"),
    ]


@pytest.mark.parametrize(
    ("available_offset", "source_run"),
    ((31.1, "run-a"), (1.0, "run-b")),
    ids=("available-time-gap", "source-session-change"),
)
def test_available_gap_or_source_change_invalidates_delta(
    available_offset: float, source_run: str,
) -> None:
    frames = (
        _frame(0, "snapshot", "run-a", (Level("bid", Decimal("99"), Decimal("1"), "set"),)),
        _frame(
            1, "delta", source_run,
            (Level("bid", Decimal("99"), Decimal("2"), "set"),),
            available_offset=available_offset,
        ),
    )
    build = build_tiles(
        MARKET, frames, {}, START, START + timedelta(seconds=5), "5s",
        "sha256-source",
    )

    interrupted = _column(build, 0)
    assert interrupted[9] == "gap"
    assert interrupted[13] is True
    assert all(row[10] is None for row in build.cells)


def test_load_frames_carries_source_run_from_frame_fact(tmp_path: Path) -> None:
    frame_path = tmp_path / "frame.parquet"
    level_path = tmp_path / "level.parquet"
    db = duckdb.connect(":memory:")
    try:
        db.execute(
            f"""COPY (SELECT 'f1' frame_id,'mkt__test__btc_jpy__r0' market_id,
            'snapshot' message_kind,TIMESTAMPTZ '2026-08-01 00:00:00+00' event_time,
            TIMESTAMPTZ '2026-08-01 00:00:00+00' available_time,
            TIMESTAMPTZ '2026-08-01 00:00:00+00' ingest_time,
            'snapshot_no_sequence' integrity_mode,'run-from-fact' run_id,
            1 segment_sequence,0 source_row_index)
            TO '{frame_path.as_posix()}' (FORMAT PARQUET)"""
        )
        db.execute(
            f"""COPY (SELECT 'f1' frame_id,'ask' side,'100' price,'1' size,
            'set' AS "action",0 source_level_index)
            TO '{level_path.as_posix()}' (FORMAT PARQUET)"""
        )
    finally:
        db.close()
    outputs = (
        ActiveOutput("book_l2", "p", "v", "a", "book_l2_frame", "af", frame_path, 1, START, START),
        ActiveOutput("book_l2", "p", "v", "a", "book_l2_level", "al", level_path, 1, START, START),
    )
    snapshot = ActiveOutputSnapshot(MARKET, outputs, "sha256-source")

    frames = _load_frames(snapshot, START, START + timedelta(seconds=5))

    assert len(frames) == 1
    assert frames[0].source_run == "run-from-fact/segment-1"


def test_load_frames_prefers_connection_boundary_over_run(tmp_path: Path) -> None:
    frame_path = tmp_path / "frame-v3.parquet"
    level_path = tmp_path / "level-v3.parquet"
    db = duckdb.connect(":memory:")
    try:
        db.execute(
            f"""COPY (SELECT
            frame_id,'mkt__test__btc_jpy__r0' market_id,message_kind,
            event_time,event_time available_time,event_time ingest_time,
            'snapshot_no_sequence' integrity_mode,'run-one' run_id,
            connection_id,segment_sequence,source_row_index
            FROM (VALUES
            ('f1','snapshot',TIMESTAMPTZ '2026-08-01 00:00:00+00','conn-1',1,0),
            ('f2','delta',TIMESTAMPTZ '2026-08-01 00:00:01+00','conn-2',2,1))
            AS t(frame_id,message_kind,event_time,connection_id,segment_sequence,
                 source_row_index))
            TO '{frame_path.as_posix()}' (FORMAT PARQUET)"""
        )
        db.execute(
            f"""COPY (SELECT * FROM (VALUES
            ('f1','ask','100','1','set',0),
            ('f2','ask','100','2','set',0))
            AS t(frame_id,side,price,size,"action",source_level_index))
            TO '{level_path.as_posix()}' (FORMAT PARQUET)"""
        )
    finally:
        db.close()
    outputs = (
        ActiveOutput("book_l2", "p", "v", "a", "book_l2_frame", "af", frame_path, 2, START, START),
        ActiveOutput("book_l2", "p", "v", "a", "book_l2_level", "al", level_path, 2, START, START),
    )
    snapshot = ActiveOutputSnapshot(MARKET, outputs, "sha256-source")

    frames = _load_frames(snapshot, START, START + timedelta(seconds=5))

    assert [frame.source_run for frame in frames] == ["conn-1", "conn-2"]
    build = build_tiles(
        MARKET, tuple(frames), {}, START, START + timedelta(seconds=5),
        "5s", "sha256-source",
    )
    assert _column(build, 0)[9] == "gap"


def test_load_frames_uses_event_window_for_late_available_archive(
    tmp_path: Path,
) -> None:
    frame_path = tmp_path / "okx-frame.parquet"
    level_path = tmp_path / "okx-level.parquet"
    db = duckdb.connect(":memory:")
    try:
        db.execute(
            f"""COPY (SELECT
            frame_id,'mkt__okx__btc_usdt__r0' market_id,message_kind,
            event_time,TIMESTAMPTZ '2026-08-02 00:00:00+00' available_time,
            TIMESTAMPTZ '2026-08-02 00:00:00+00' ingest_time,
            'strict_timestamp' integrity_mode,'archive-day' source_session_id,
            source_row_index
            FROM (VALUES
            ('snapshot','snapshot',TIMESTAMPTZ '2026-07-31 23:59:59+00',0),
            ('delta','delta',TIMESTAMPTZ '2026-08-01 00:00:01+00',1))
            AS t(frame_id,message_kind,event_time,source_row_index))
            TO '{frame_path.as_posix()}' (FORMAT PARQUET)"""
        )
        db.execute(
            f"""COPY (SELECT * FROM (VALUES
            ('snapshot','ask','100','1','set',0),
            ('delta','ask','100','2','set',0))
            AS t(frame_id,side,price,size,"action",source_level_index))
            TO '{level_path.as_posix()}' (FORMAT PARQUET)"""
        )
    finally:
        db.close()
    market = {
        "market_id": "mkt__okx__btc_usdt__r0",
        "venue_id": "okx",
        "tick_size": "0.1",
    }
    outputs = (
        ActiveOutput(
            "book_l2", "p", "v", "a", "book_l2_frame", "af",
            frame_path, 2, START, START,
        ),
        ActiveOutput(
            "book_l2", "p", "v", "a", "book_l2_level", "al",
            level_path, 2, START, START,
        ),
    )
    snapshot = ActiveOutputSnapshot(market, outputs, "sha256-source")

    frames = _load_frames(snapshot, START, START + timedelta(seconds=5))

    assert [frame.frame_id for frame in frames] == ["snapshot", "delta"]


def test_load_frames_prefers_okx_live_v5_over_archive_v2_overlap(
    tmp_path: Path,
) -> None:
    archive_frame = tmp_path / "archive-frame.parquet"
    archive_level = tmp_path / "archive-level.parquet"
    live_frame = tmp_path / "live-frame.parquet"
    live_level = tmp_path / "live-level.parquet"
    db = duckdb.connect(":memory:")
    try:
        for path, prefix, normalization, connection in (
            (
                archive_frame, "archive", "book-l2-normalization-v2",
                "NULL::VARCHAR",
            ),
            (
                live_frame, "live", "book-l2-normalization-v5",
                "'live-connection'",
            ),
        ):
            values = (
                "('snapshot',0),('delta-1',1),('delta-2',2)"
                if prefix == "archive" else "('snapshot',1),('delta-2',2)"
            )
            db.execute(
                f"""COPY (SELECT CONCAT('{prefix}-',kind) frame_id,
                'mkt__okx__btc_usdt__r0' market_id,kind message_kind,
                TIMESTAMPTZ '2026-08-01 00:00:00+00' + sec * INTERVAL 1 SECOND
                  event_time,
                TIMESTAMPTZ '2026-08-01 00:00:00+00' + sec * INTERVAL 1 SECOND
                  available_time,
                TIMESTAMPTZ '2026-08-01 00:00:00+00' + sec * INTERVAL 1 SECOND
                  ingest_time,
                'native_prev_seq' integrity_mode,'{prefix}-run' source_session_id,
                {connection} connection_id,1 segment_sequence,sec source_row_index,
                '{normalization}' normalization_version
                FROM (VALUES {values})
                AS t(kind,sec)) TO '{path.as_posix()}' (FORMAT PARQUET)"""
            )
        for path, prefix in (
            (archive_level, "archive"), (live_level, "live"),
        ):
            values = (
                "('snapshot',0),('delta-1',1),('delta-2',2)"
                if prefix == "archive" else "('snapshot',1),('delta-2',2)"
            )
            db.execute(
                f"""COPY (SELECT CONCAT('{prefix}-',kind) frame_id,'ask' side,
                '100' price,CAST(sec + 1 AS VARCHAR) size,'set' AS "action",
                0 source_level_index
                FROM (VALUES {values})
                AS t(kind,sec)) TO '{path.as_posix()}' (FORMAT PARQUET)"""
            )
    finally:
        db.close()
    market = {
        "market_id": "mkt__okx__btc_usdt__r0", "venue_id": "okx",
        "tick_size": "0.1",
    }
    live_start = START + timedelta(seconds=1)
    live_end = START + timedelta(seconds=2)
    outputs = (
        ActiveOutput(
            "book_l2", "2026-08-01", "book-l2-normalization-v2", "archive",
            "book_l2_frame", "archive-frame", archive_frame, 3, START, live_end,
        ),
        ActiveOutput(
            "book_l2", "2026-08-01", "book-l2-normalization-v2", "archive",
            "book_l2_level", "archive-level", archive_level, 3, START, live_end,
        ),
        ActiveOutput(
            "book_l2", "live/run/segment-1", "book-l2-normalization-v5", "live",
            "book_l2_frame", "live-frame", live_frame, 2, live_start, live_end,
        ),
        ActiveOutput(
            "book_l2", "live/run/segment-1", "book-l2-normalization-v5", "live",
            "book_l2_level", "live-level", live_level, 2, live_start, live_end,
        ),
    )

    frames = _load_frames(
        ActiveOutputSnapshot(market, outputs, "sha256-overlap"),
        START, START + timedelta(seconds=3),
    )

    assert {frame.frame_id for frame in frames} == {
        "archive-snapshot", "live-snapshot", "live-delta-2",
    }
    assert not any(
        frame.frame_id in {"archive-delta-1", "archive-delta-2"}
        for frame in frames
    )


def test_okx_live_precedence_does_not_switch_to_archive_between_segments(
    tmp_path: Path,
) -> None:
    outputs = (
        ActiveOutput(
            "book_l2", "live/run/segment-1", "book-l2-normalization-v5", "a1",
            "book_l2_frame", "f1", tmp_path / "f1.parquet", 1,
            START + timedelta(seconds=1), START + timedelta(seconds=2),
        ),
        ActiveOutput(
            "book_l2", "live/run/segment-2", "book-l2-normalization-v5", "a2",
            "book_l2_frame", "f2", tmp_path / "f2.parquet", 1,
            START + timedelta(seconds=4), START + timedelta(seconds=5),
        ),
    )

    coverage = okx_live_event_coverage("okx", outputs)

    assert coverage == (
        type(coverage[0])(
            START + timedelta(seconds=1), START + timedelta(seconds=5),
        ),
    )


def test_watch_retries_after_writer_lock_timeout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    class FakeConnection:
        closed = False

        def close(self) -> None:
            self.closed = True

    connection = FakeConnection()
    calls = 0

    def lock_then_stop(_root: Path) -> object:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise TimeoutError("writer busy")
        raise KeyboardInterrupt

    monkeypatch.setattr(
        "guvolu.data.orderflow_tile_materialize.connect", lambda _root: connection,
    )
    monkeypatch.setattr(
        "guvolu.data.orderflow_tile_materialize.sqlite_writer_lock", lock_then_stop,
    )
    sleeps: list[float] = []
    monkeypatch.setattr(
        "guvolu.data.orderflow_tile_materialize.time.sleep", sleeps.append,
    )

    result = main([
        "--data-root", str(tmp_path), "watch", "--poll-seconds", "30",
    ])

    output = capsys.readouterr().out
    assert result == 0
    assert calls == 2
    assert sleeps == [30.0]
    assert '"event": "orderflow_tile_cycle_error"' in output
    assert '"error": "TimeoutError: writer busy"' in output
    assert '"elapsed_seconds":' in output
    assert "OFL tile watcher 已停止" in output
    assert connection.closed is True


def test_bitbank_delayed_whole_recovers_state_without_false_change_attribution() -> None:
    market = {**MARKET, "venue_id": "bitbank"}
    frames = (
        Frame(
            "d6", "delta", START + timedelta(seconds=30),
            START + timedelta(seconds=1), "monotonic", "conn-1",
            (Level("ask", Decimal("102"), Decimal("3"), "set"),),
            6, 1, 0,
        ),
        Frame(
            "d5", "delta", START + timedelta(seconds=29),
            START + timedelta(seconds=2), "monotonic", "conn-1",
            (Level("ask", Decimal("101"), Decimal("9"), "set"),),
            5, 1, 1,
        ),
        Frame(
            "w5", "snapshot", START, START + timedelta(seconds=3),
            "monotonic", "conn-1",
            (
                Level("ask", Decimal("101"), Decimal("1"), "set"),
                Level("bid", Decimal("99"), Decimal("2"), "set"),
            ),
            5, 1, 2,
        ),
    )
    build = build_tiles(
        market, frames, {}, START, START + timedelta(seconds=10), "5s",
        "sha256-source",
    )
    first = _column(build, 0)
    assert first[9] == "reset" and first[11] is True
    cells = [row for row in build.cells if row[6] == int(START.timestamp())]
    sizes = {(row[7], row[8]): row[10] for row in cells}
    assert sizes == {("ask", 50): "1", ("ask", 51): "3", ("bid", 49): "2"}
    assert all(row[11:13] == ("0", "0") for row in cells)


def test_bitbank_whole_does_not_erase_prior_pit_delta_in_same_bucket() -> None:
    market = {**MARKET, "venue_id": "bitbank"}
    frames = (
        Frame(
            "w5", "snapshot", START, START, "monotonic", "conn-1",
            (Level("ask", Decimal("100"), Decimal("1"), "set"),), 5, 1, 0,
        ),
        Frame(
            "d8", "delta", START + timedelta(seconds=1),
            START + timedelta(seconds=1), "monotonic", "conn-1",
            (Level("ask", Decimal("100"), Decimal("2"), "set"),), 8, 1, 1,
        ),
        Frame(
            "w7", "snapshot", START + timedelta(seconds=3),
            START + timedelta(seconds=3), "monotonic", "conn-1",
            (Level("ask", Decimal("100"), Decimal("1"), "set"),), 7, 1, 2,
        ),
    )

    build = build_tiles(
        market, frames, {}, START, START + timedelta(seconds=5), "5s",
        "sha256-source",
    )

    cells = [row for row in build.cells if row[6] == int(START.timestamp())]
    assert [(row[10], row[11], row[12]) for row in cells] == [("2", "1", "0")]


def test_bitbank_whole_keeps_prior_delta_across_bucket_boundary() -> None:
    market = {**MARKET, "venue_id": "bitbank"}
    frames = (
        Frame(
            "w5", "snapshot", START, START, "monotonic", "conn-1",
            (Level("ask", Decimal("100"), Decimal("1"), "set"),), 5, 1, 0,
        ),
        Frame(
            "d6", "delta", START + timedelta(seconds=1),
            START + timedelta(seconds=1), "monotonic", "conn-1",
            (Level("ask", Decimal("100"), Decimal("2"), "set"),), 6, 1, 1,
        ),
        Frame(
            "w7", "snapshot", START + timedelta(seconds=6),
            START + timedelta(seconds=6), "monotonic", "conn-1",
            (Level("ask", Decimal("100"), Decimal("1.5"), "set"),), 7, 1, 2,
        ),
    )

    build = build_tiles(
        market, frames, {}, START, START + timedelta(seconds=10), "5s",
        "sha256-source",
    )

    first_cells = [row for row in build.cells if row[6] == int(START.timestamp())]
    second_cells = [
        row for row in build.cells
        if row[6] == int(START.timestamp()) + 5
    ]
    assert [(row[10], row[11], row[12]) for row in first_cells] == [
        ("2", "1", "0"),
    ]
    assert [(row[10], row[11], row[12]) for row in second_cells] == [
        ("1.5", "0", "0"),
    ]


def test_bitbank_new_connection_delta_invalidates_old_book_until_whole() -> None:
    market = {**MARKET, "venue_id": "bitbank"}
    frames = (
        Frame(
            "w5", "snapshot", START, START, "monotonic", "conn-a",
            (Level("ask", Decimal("101"), Decimal("1"), "set"),), 5, 1, 0,
        ),
        Frame(
            "d6", "delta", START + timedelta(seconds=6),
            START + timedelta(seconds=6), "monotonic", "conn-b",
            (Level("ask", Decimal("102"), Decimal("2"), "set"),), 6, 1, 0,
        ),
    )
    build = build_tiles(
        market, frames, {}, START, START + timedelta(seconds=10), "5s",
        "sha256-source",
    )
    assert _column(build, 5)[9] == "gap"
    assert _column(build, 5)[13] is True
    assert all(row[10] is None for row in build.cells if row[6] == int(START.timestamp()) + 5)
