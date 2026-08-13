"""L2 五分钟质量窗口的高信号契约测试。"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pytest

from guvolu.data import store
from guvolu.data.store import DB_SCHEMA_VERSION
from guvolu.data.l2_quality import (
    QUALITY_VERSION,
    compute_quality_windows,
    upsert_quality_windows,
)
from guvolu.data.materialize import ensure_markets
from guvolu.venues import registry

BASE = datetime(2026, 8, 12, 12, tzinfo=UTC)


def _write_frames(path: Path, rows: list[tuple[object, ...]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    db = duckdb.connect(":memory:")
    try:
        db.execute("SET TimeZone='UTC'")
        db.execute("""
          CREATE TABLE frames (
            frame_id VARCHAR,market_id VARCHAR,venue_id VARCHAR,
            event_time TIMESTAMPTZ,source_publish_time TIMESTAMPTZ,
            available_time TIMESTAMPTZ,ingest_time TIMESTAMPTZ,
            recv_ts_mono_ns UBIGINT,message_kind VARCHAR,sequence_id VARCHAR,
            changed_bid_levels BIGINT,changed_ask_levels BIGINT,
            checksum VARCHAR,endpoint VARCHAR,integrity_mode VARCHAR,
            source_session_id VARCHAR,connection_id VARCHAR,channel_id VARCHAR,
            data_quality VARCHAR,segment_sequence BIGINT,source_row_index BIGINT,
            normalization_version VARCHAR
          )
        """)
        db.executemany(
            "INSERT INTO frames VALUES (" + ",".join("?" for _ in range(22)) + ")",
            rows,
        )
        db.execute(
            f"COPY frames TO '{path.as_posix()}' (FORMAT PARQUET)"
        )
    finally:
        db.close()


def _row(
    frame_id: str,
    market_id: str,
    venue_id: str,
    offset: int,
    kind: str,
    *,
    sequence: str | None = None,
    endpoint: str,
    connection: str | None = "run-c1",
    channel: str | None = "room",
    source_offset: int | None = 1,
    mono_seconds: int | None = None,
    quality: list[str] | None = None,
    segment: int = 1,
    source_row: int = 1,
    event_shift: int = 0,
    checksum: str | None = None,
    integrity: str = "test-integrity",
    changed_bids: int | None = None,
    changed_asks: int | None = None,
) -> tuple[object, ...]:
    event = BASE + timedelta(seconds=offset + event_shift)
    ingest = BASE + timedelta(seconds=offset + 1)
    source_publish = (
        None if source_offset is None
        else ingest - timedelta(seconds=source_offset)
    )
    return (
        frame_id, market_id, venue_id, event, source_publish, ingest, ingest,
        None if mono_seconds is None else mono_seconds * 1_000_000_000,
        kind, sequence, changed_bids, changed_asks, checksum, endpoint,
        integrity, "run", connection,
        channel, None if quality is None else json.dumps(quality), segment,
        source_row, "book-l2-normalization-v4",
    )


def _register(
    root: Path,
    conn: sqlite3.Connection,
    market_id: str,
    attempt: str,
    partition: str,
    path: Path,
    low: datetime,
    high: datetime,
    normalization: str = "book-l2-normalization-v4",
) -> None:
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    artifact = "sha256-" + digest
    relative = path.relative_to(root).as_posix()
    stamp = high.isoformat()
    conn.execute(
        "INSERT INTO partition_attempt "
        "(attempt_id,market_id,domain,partition_key,normalization_version,"
        "input_set_hash,status,source_rows,normalized_rows,ignored_rows,"
        "rejected_rows,started_at,finished_at,code_version,config_hash) "
        "VALUES (?,?, 'book_l2',?, ?,?,'complete',"
        "1,1,0,0,?,?, 'test','test')",
        (attempt, market_id, partition, normalization, digest, stamp, stamp),
    )
    conn.execute(
        "INSERT INTO artifact VALUES "
        "(?,'materialized_parquet',?,?,?, ?,?,'sha256-file-v1',3)",
        (artifact, relative, digest, path.stat().st_size, stamp, stamp),
    )
    conn.execute(
        "INSERT INTO materialization_output VALUES "
        "(?,?,'book_l2_frame',1,?,?,?)",
        (attempt, artifact, low.isoformat(), high.isoformat(), stamp),
    )
    conn.execute(
        "INSERT INTO materialization_partition_head VALUES "
        "(?, 'book_l2',?,?,?,?)",
        (market_id, partition, normalization, attempt, stamp),
    )


def _connection(root: Path) -> sqlite3.Connection:
    conn = store.connect(root)
    registry.register_all(conn)
    ensure_markets(conn)
    conn.commit()
    return conn


def test_quality_schema_and_market_foreign_key(tmp_path: Path) -> None:
    conn = _connection(tmp_path)
    try:
        assert (
            conn.execute("PRAGMA user_version").fetchone()[0]
            == DB_SCHEMA_VERSION
        )
        columns = {
            str(row[1]) for row in conn.execute(
                "PRAGMA table_info(l2_quality_window)"
            )
        }
        assert {
            "source_head_generation", "source_attempt_ids",
            "observed_silence_gt_30s", "window_clock_basis",
            "materialized_freshness_status", "latency_status", "reasons",
        } <= columns
        foreign_keys = conn.execute(
            "PRAGMA foreign_key_list(l2_quality_window)"
        ).fetchall()
        assert any(row[2] == "market" and row[3] == "market_id" for row in foreign_keys)
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        conn.close()


def test_cross_segment_sequence_predecessor_is_compared(tmp_path: Path) -> None:
    market = "mkt__bitbank__btc_jpy__r0"
    first = tmp_path / "materialized/first.parquet"
    second = tmp_path / "materialized/second.parquet"
    _write_frames(first, [
        _row("f1", market, "bitbank", 10, "snapshot", sequence="100",
             endpoint="depth_whole/depth_diff", channel="depth_diff_btc_jpy",
             quality=[], segment=1),
    ])
    _write_frames(second, [
        _row("f2", market, "bitbank", 20, "delta", sequence="100",
             endpoint="depth_whole/depth_diff", channel="depth_diff_btc_jpy",
             quality=[], segment=2, source_row=1),
        _row("f3", market, "bitbank", 21, "delta", sequence="99",
             endpoint="depth_whole/depth_diff", channel="depth_diff_btc_jpy",
             quality=[], segment=2, source_row=2),
    ])
    conn = _connection(tmp_path)
    try:
        _register(tmp_path, conn, market, "a1", "p1", first,
                  BASE + timedelta(seconds=10), BASE + timedelta(seconds=10))
        _register(tmp_path, conn, market, "a2", "p2", second,
                  BASE + timedelta(seconds=20), BASE + timedelta(seconds=21))
        conn.commit()
        result = compute_quality_windows(
            tmp_path, conn, BASE, BASE + timedelta(minutes=5),
            market_ids=(market,), computed_at=BASE + timedelta(minutes=6),
        )
    finally:
        conn.close()
    assert len(result) == 1
    row = result[0]
    assert row.sequence_duplicates == 1
    assert row.sequence_regressions == 1
    assert row.predecessor_unknown_frames == 0
    assert json.loads(row.source_attempt_ids) == ["a1", "a2"]
    assert row.source_attempt_count == 2
    assert row.source_head_generation.startswith("sha256-")
    assert row.status == "failed"


def test_bitflyer_delta_before_snapshot_and_unsupported_checksum(
    tmp_path: Path,
) -> None:
    market = "mkt__bitflyer__btc_jpy__r0"
    path = tmp_path / "materialized/bitflyer.parquet"
    _write_frames(path, [
        _row("d1", market, "bitflyer", 1, "delta",
             endpoint="board_snapshot/board", channel="board", quality=[],
             source_offset=None, source_row=1),
        _row("s1", market, "bitflyer", 2, "snapshot",
             endpoint="board_snapshot/board", channel="board_snapshot", quality=[],
             source_offset=None, source_row=2),
    ])
    conn = _connection(tmp_path)
    try:
        _register(tmp_path, conn, market, "bf", "p", path,
                  BASE + timedelta(seconds=1), BASE + timedelta(seconds=2))
        conn.commit()
        row = compute_quality_windows(
            tmp_path, conn, BASE, BASE + timedelta(minutes=5),
            market_ids=(market,), computed_at=BASE + timedelta(minutes=6),
        )[0]
    finally:
        conn.close()
    assert row.unanchored_before_snapshot_frames == 1
    assert row.anchor_unknown_frames == 0
    assert row.sequence_duplicates is None
    assert row.sequence_regressions is None
    assert row.checksum_status == "unsupported"
    assert row.checksum_observed_frames == 0
    assert row.checksum_checked_frames is None
    assert row.checksum_failures is None
    assert row.latency_status == "unmeasurable"
    assert row.status == "degraded"


def test_ingest_clock_owns_window_when_venue_event_clock_is_shifted(
    tmp_path: Path,
) -> None:
    market = "mkt__gmo__btc__r0"
    path = tmp_path / "materialized/clock-basis.parquet"
    _write_frames(path, [
        _row("g1", market, "gmo", 10, "snapshot", endpoint="orderbooks/ws",
             event_shift=-3600, quality=[]),
    ])
    conn = _connection(tmp_path)
    try:
        _register(tmp_path, conn, market, "clock", "p", path,
                  BASE - timedelta(hours=1), BASE - timedelta(hours=1))
        conn.commit()
        row = compute_quality_windows(
            tmp_path, conn, BASE, BASE + timedelta(minutes=5),
            market_ids=(market,), computed_at=BASE + timedelta(minutes=6),
        )[0]
    finally:
        conn.close()
    assert row.window_start == BASE.isoformat()
    assert row.window_clock_basis == "ingest"
    assert row.first_event_time == (
        BASE - timedelta(hours=1) + timedelta(seconds=10)
    ).isoformat()
    assert row.first_observation_time == (
        BASE + timedelta(seconds=11)
    ).isoformat()


def test_segment_local_untrusted_flag_is_overridden_by_complete_anchor(
    tmp_path: Path,
) -> None:
    market = "mkt__bitbank__btc_jpy__r0"
    first = tmp_path / "materialized/anchor.parquet"
    second = tmp_path / "materialized/flagged.parquet"
    _write_frames(first, [
        _row("s", market, "bitbank", 1, "snapshot",
             endpoint="depth_whole/depth_diff", quality=[], segment=1),
    ])
    _write_frames(second, [
        _row("d", market, "bitbank", 2, "delta",
             endpoint="depth_whole/depth_diff",
             quality=["replay_untrusted_until_snapshot"], segment=2),
    ])
    conn = _connection(tmp_path)
    try:
        _register(tmp_path, conn, market, "anchor", "p1", first,
                  BASE + timedelta(seconds=1), BASE + timedelta(seconds=1))
        _register(tmp_path, conn, market, "flag", "p2", second,
                  BASE + timedelta(seconds=2), BASE + timedelta(seconds=2))
        conn.commit()
        row = compute_quality_windows(
            tmp_path, conn, BASE, BASE + timedelta(minutes=5),
            market_ids=(market,), computed_at=BASE + timedelta(minutes=6),
        )[0]
    finally:
        conn.close()
    assert row.unanchored_before_snapshot_frames == 0
    assert row.untrusted_frames == 0
    assert row.fact_untrusted_flag_conflicts == 1
    assert "source_data_quality_untrusted" not in json.loads(row.reasons)
    assert row.status == "ok"


def test_nonempty_checksum_without_verified_evidence_stays_unknown(
    tmp_path: Path,
) -> None:
    market = "mkt__gmo__btc__r0"
    path = tmp_path / "materialized/checksum.parquet"
    _write_frames(path, [
        _row("c", market, "gmo", 1, "snapshot", endpoint="future-book",
             checksum="1234", integrity="checksum_present", quality=[]),
    ])
    conn = _connection(tmp_path)
    try:
        _register(tmp_path, conn, market, "checksum", "p", path,
                  BASE + timedelta(seconds=1), BASE + timedelta(seconds=1))
        conn.commit()
        row = compute_quality_windows(
            tmp_path, conn, BASE, BASE + timedelta(minutes=5),
            market_ids=(market,), computed_at=BASE + timedelta(minutes=6),
        )[0]
    finally:
        conn.close()
    assert row.checksum_status == "unknown"
    assert row.checksum_observed_frames == 1
    assert row.checksum_checked_frames is None
    assert row.checksum_failures is None
    assert "checksum_observed_but_not_verified" in json.loads(row.reasons)


def test_okx_books_empty_update_is_heartbeat_not_sequence_duplicate(
    tmp_path: Path,
) -> None:
    market = "mkt__okx__btc_usdt__r0"
    path = tmp_path / "materialized/okx-live.parquet"
    _write_frames(path, [
        _row(
            "snapshot", market, "okx", 1, "snapshot", sequence="41",
            endpoint="books", quality=[], changed_bids=1, changed_asks=1,
            source_row=1,
        ),
        _row(
            "heartbeat", market, "okx", 2, "delta", sequence="41",
            endpoint="books", quality=["empty_update_heartbeat"],
            changed_bids=0, changed_asks=0, source_row=2,
        ),
    ])
    conn = _connection(tmp_path)
    try:
        _register(
            tmp_path, conn, market, "okx-live", "live/run/segment-1", path,
            BASE + timedelta(seconds=1), BASE + timedelta(seconds=2),
            normalization="book-l2-normalization-v5",
        )
        conn.commit()
        row = compute_quality_windows(
            tmp_path, conn, BASE, BASE + timedelta(minutes=5),
            market_ids=(market,), computed_at=BASE + timedelta(minutes=6),
        )[0]
    finally:
        conn.close()

    assert row.checksum_status == "unsupported"
    assert row.sequence_duplicates == 0
    assert "sequence_duplicate_same_connection_channel" not in json.loads(
        row.reasons
    )


def test_okx_first_snapshot_establishes_sequence_predecessor(
    tmp_path: Path,
) -> None:
    market = "mkt__okx__btc_usdt__r0"
    path = tmp_path / "materialized/okx-first-snapshot.parquet"
    _write_frames(path, [
        _row(
            "snapshot", market, "okx", 1, "snapshot", sequence="77",
            endpoint="books", quality=[], changed_bids=1, changed_asks=1,
        ),
    ])
    conn = _connection(tmp_path)
    try:
        _register(
            tmp_path, conn, market, "okx-snapshot", "live/run/segment-1",
            path, BASE + timedelta(seconds=1), BASE + timedelta(seconds=1),
            normalization="book-l2-normalization-v5",
        )
        conn.commit()
        row = compute_quality_windows(
            tmp_path, conn, BASE, BASE + timedelta(minutes=5),
            market_ids=(market,), computed_at=BASE + timedelta(minutes=6),
        )[0]
    finally:
        conn.close()

    assert row.predecessor_unknown_frames == 0
    assert "sequence_predecessor_unknown" not in json.loads(row.reasons)
    assert row.status == "ok"


def test_negative_signed_recv_source_offset_is_clock_skewed(
    tmp_path: Path,
) -> None:
    market = "mkt__gmo__btc__r0"
    path = tmp_path / "materialized/gmo.parquet"
    # 来源钟比接收钟晚两秒。
    _write_frames(path, [
        _row("g1", market, "gmo", 1, "snapshot", endpoint="orderbooks/ws",
             source_offset=-2, quality=[]),
    ])
    conn = _connection(tmp_path)
    try:
        _register(tmp_path, conn, market, "gmo", "p", path,
                  BASE + timedelta(seconds=1), BASE + timedelta(seconds=1))
        conn.commit()
        row = compute_quality_windows(
            tmp_path, conn, BASE, BASE + timedelta(minutes=5),
            market_ids=(market,), computed_at=BASE + timedelta(minutes=6),
        )[0]
    finally:
        conn.close()
    assert row.recv_source_offset_samples == 1
    assert row.recv_source_offset_p50_ms == pytest.approx(-2000)
    assert row.recv_source_offset_p95_ms == pytest.approx(-2000)
    assert row.latency_status == "clock_skewed"
    assert "negative_recv_source_offset_clock_skew" in json.loads(row.reasons)


def test_legacy_missing_connection_is_unknown_not_fake_zero(
    tmp_path: Path,
) -> None:
    market = "mkt__bitbank__btc_jpy__r0"
    path = tmp_path / "materialized/legacy.parquet"
    _write_frames(path, [
        _row("old", market, "bitbank", 1, "delta", sequence="7",
             endpoint="depth_whole/depth_diff", connection=None, channel=None,
             quality=None, source_offset=1),
    ])
    conn = _connection(tmp_path)
    try:
        _register(tmp_path, conn, market, "old", "p", path,
                  BASE + timedelta(seconds=1), BASE + timedelta(seconds=1))
        conn.commit()
        row = compute_quality_windows(
            tmp_path, conn, BASE, BASE + timedelta(minutes=5),
            market_ids=(market,), computed_at=BASE + timedelta(minutes=6),
        )[0]
    finally:
        conn.close()
    assert row.connection_count is None
    assert row.channel_count is None
    assert row.identity_unknown_frames == 1
    assert row.observed_silence_gt_30s is None
    assert row.max_observed_interarrival_ms is None
    assert row.sequence_duplicates is None
    assert row.sequence_regressions is None
    assert row.predecessor_unknown_frames == 1
    assert row.unanchored_before_snapshot_frames is None
    assert row.anchor_unknown_frames == 1
    assert row.untrusted_frames is None
    assert row.latency_status == "unmeasurable"
    assert row.status == "degraded"


def test_observed_silence_is_same_channel_monotonic_telemetry(
    tmp_path: Path,
) -> None:
    market = "mkt__gmo__btc__r0"
    path = tmp_path / "materialized/silence.parquet"
    _write_frames(path, [
        _row("g1", market, "gmo", 1, "snapshot", endpoint="orderbooks/ws",
             quality=[], mono_seconds=1, source_row=1),
        _row("g2", market, "gmo", 40, "snapshot", endpoint="orderbooks/ws",
             quality=[], mono_seconds=40, source_row=2),
    ])
    conn = _connection(tmp_path)
    try:
        _register(tmp_path, conn, market, "silence", "p", path,
                  BASE + timedelta(seconds=1), BASE + timedelta(seconds=40))
        conn.commit()
        row = compute_quality_windows(
            tmp_path, conn, BASE, BASE + timedelta(minutes=5),
            market_ids=(market,), computed_at=BASE + timedelta(minutes=6),
        )[0]
    finally:
        conn.close()
    assert row.max_observed_interarrival_ms == pytest.approx(39_000)
    assert row.observed_silence_gt_30s == 1
    assert json.loads(row.reasons) == ["observed_receive_silence_gt_30s"]


def test_current_empty_window_persists_stale_heartbeat(tmp_path: Path) -> None:
    market = "mkt__gmo__btc__r0"
    path = tmp_path / "materialized/stopped.parquet"
    _write_frames(path, [
        _row("old", market, "gmo", 1, "snapshot", endpoint="orderbooks/ws",
             quality=[]),
    ])
    conn = _connection(tmp_path)
    try:
        _register(tmp_path, conn, market, "stopped", "p", path,
                  BASE + timedelta(seconds=1), BASE + timedelta(seconds=1))
        conn.commit()
        fresh_rows = compute_quality_windows(
            tmp_path, conn, BASE, BASE + timedelta(minutes=7),
            market_ids=(market,), computed_at=BASE + timedelta(minutes=6),
        )
        rows = compute_quality_windows(
            tmp_path, conn, BASE, BASE + timedelta(minutes=21),
            market_ids=(market,), computed_at=BASE + timedelta(minutes=20),
        )
    finally:
        conn.close()
    fresh_heartbeat = next(
        row for row in fresh_rows if row.window_start.endswith("12:05:00+00:00")
    )
    assert fresh_heartbeat.frames == 0
    assert fresh_heartbeat.materialized_freshness_status == "fresh"
    assert fresh_heartbeat.status == "ok"
    heartbeat = next(row for row in rows if row.window_start.endswith("12:20:00+00:00"))
    assert heartbeat.frames == 0
    assert heartbeat.window_clock_basis == "none"
    assert heartbeat.first_event_time is None
    assert heartbeat.latest_materialized_observation_time == (
        BASE + timedelta(seconds=2)
    ).isoformat()
    assert heartbeat.materialized_freshness_seconds == pytest.approx(1198)
    assert heartbeat.materialized_freshness_status == "stale"
    assert heartbeat.status == "degraded"
    assert {
        "no_frames_current_unsealed_window", "materialized_observation_stale",
    } <= set(json.loads(heartbeat.reasons))


def test_historical_okx_heartbeat_does_not_claim_live_staleness(
    tmp_path: Path,
) -> None:
    market = "mkt__okx__btc_usdt__r0"
    path = tmp_path / "materialized/okx-history.parquet"
    _write_frames(path, [
        _row("old", market, "okx", 1, "snapshot",
             endpoint="historical-data/order-book", quality=[],
             event_shift=-2 * 24 * 60 * 60),
    ])
    conn = _connection(tmp_path)
    try:
        old = BASE - timedelta(days=2)
        _register(
            tmp_path, conn, market, "okx", "2026-08-10", path, old, old,
            normalization="book-l2-normalization-v2",
        )
        conn.commit()
        heartbeat = compute_quality_windows(
            tmp_path, conn, BASE, BASE + timedelta(minutes=7),
            market_ids=(market,), computed_at=BASE + timedelta(minutes=6),
        )[0]
    finally:
        conn.close()
    assert heartbeat.frames == 0
    assert heartbeat.materialized_freshness_seconds is None
    assert heartbeat.materialized_freshness_status == "not_applicable"
    assert heartbeat.status == "ok"
    assert json.loads(heartbeat.reasons) == [
        "historical_archive_current_freshness_not_applicable"
    ]


def test_quality_upsert_is_idempotent_and_keeps_fk_clean(tmp_path: Path) -> None:
    market = "mkt__gmo__btc__r0"
    path = tmp_path / "materialized/upsert.parquet"
    _write_frames(path, [
        _row("g1", market, "gmo", 1, "snapshot", endpoint="orderbooks/ws",
             quality=[]),
    ])
    conn = _connection(tmp_path)
    try:
        _register(tmp_path, conn, market, "upsert", "p", path,
                  BASE + timedelta(seconds=1), BASE + timedelta(seconds=1))
        conn.commit()
        windows = compute_quality_windows(
            tmp_path, conn, BASE, BASE + timedelta(minutes=5),
            market_ids=(market,), computed_at=BASE + timedelta(minutes=6),
        )
        assert upsert_quality_windows(conn, windows) == 1
        assert upsert_quality_windows(conn, windows) == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM l2_quality_window WHERE quality_version=?",
            (QUALITY_VERSION,),
        ).fetchone()[0] == 1
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        conn.close()


def test_watch_quality_failure_is_observable_and_nonblocking(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import guvolu.data.l2_quality as quality_module
    from guvolu.data.l2_materialize import _refresh_quality_nonblocking

    def fail_refresh(*_args: object, **_kwargs: object) -> dict[str, object]:
        raise RuntimeError("quality probe failed")

    monkeypatch.setattr(quality_module, "refresh_recent", fail_refresh)
    conn = _connection(tmp_path)
    try:
        summary, error = _refresh_quality_nonblocking(tmp_path, conn)
        # 事实连接仍可使用。
        assert conn.execute("SELECT 1").fetchone() == (1,)
    finally:
        conn.close()
    assert summary is None
    assert isinstance(error, RuntimeError)
    assert str(error) == "quality probe failed"
