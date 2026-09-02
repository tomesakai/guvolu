"""逐笔实时活动 head 按日合并的离线测试（TBD-40）。"""
from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest

from guvolu.data import store, trade_capture
from guvolu.data.materialize import audit_materializations
from guvolu.data.segmented_raw import SegmentedRawWriter
from guvolu.data.trade_realtime_compact import (
    CompactionError,
    _active_heads,
    compact_market,
    plan_days,
)
from guvolu.data.trade_realtime_materialize import (
    audit_realtime_trades,
    materialize_all,
)

MARKET = "mkt__gmo__btc__r0"
AFTER = datetime(2026, 8, 13, 0, 0, tzinfo=UTC)


def _write_segment(root: Path, run_id: str, payloads: list[str]) -> None:
    endpoint_id, revision = trade_capture.ENDPOINT_BINDINGS["gmo"]
    writer = SegmentedRawWriter(
        root, "gmo", "BTC", domain="trade_realtime", run_id=run_id,
        endpoint_id=endpoint_id, endpoint_revision=revision,
        segment_seconds=3600, segment_max_bytes=1024 * 1024,
    )
    for payload in payloads:
        writer.write_frame(
            payload, "trades/ws",
            connection_id=f"{run_id}-c000001", channel_id="trades",
        )
    writer.finish()


def _trade(price: str, stamp: str) -> str:
    return json.dumps({
        "channel": "trades", "price": price, "size": "0.01",
        "side": "BUY", "timestamp": stamp,
    })


def _heads(conn: object) -> dict[str, int]:
    return {
        head.partition_key: head.row_count
        for head in _active_heads(conn, MARKET)  # type: ignore[arg-type]
    }


def _parquet_rows(path: Path) -> int:
    db = duckdb.connect(":memory:")
    try:
        return int(db.execute(
            "SELECT COUNT(*) FROM read_parquet(?)", [str(path)]
        ).fetchone()[0])
    finally:
        db.close()


def test_compaction_merges_day_and_retires_segment_heads(
    tmp_path: Path,
) -> None:
    """同日两段合并为一件，段头撤销，行数守恒，审计通过。"""
    _write_segment(tmp_path, "run-a", [
        _trade("17000000", "2026-08-11T00:00:00.000Z"),
        _trade("17000001", "2026-08-11T00:00:01.000Z"),
    ])
    _write_segment(tmp_path, "run-b", [
        _trade("17000002", "2026-08-11T12:00:00.000Z"),
    ])
    conn = store.connect(tmp_path)
    try:
        materialize_all(tmp_path, conn, report_reused=False)
        assert len(_heads(conn)) == 2
        # 宽限期内不合并
        assert plan_days(
            _active_heads(conn, MARKET),
            now=datetime(2026, 8, 11, 23, 30, tzinfo=UTC),
        ) == []
        results = compact_market(tmp_path, conn, MARKET, now=AFTER)
        assert [result.partition_key for result in results] == [
            "day/2026-08-11"
        ]
        assert results[0].merged_heads == 2
        assert results[0].row_count == 3
        assert results[0].reused is False
        assert _heads(conn) == {"day/2026-08-11": 3}
        output = tmp_path / results[0].output_path
        assert _parquet_rows(output) == 3
        # 原段 attempt 与输出保留
        assert conn.execute(
            "SELECT COUNT(*) FROM partition_attempt WHERE domain='trade_realtime'"
            " AND status='complete'"
        ).fetchone()[0] == 3
        dependencies = conn.execute(
            "SELECT COUNT(*) FROM materialization_dependency WHERE attempt_id=?",
            (results[0].attempt_id,),
        ).fetchone()[0]
        assert dependencies == 2
        assert audit_realtime_trades(tmp_path, conn)["ok"] is True
        audit = audit_materializations(tmp_path, conn)
        assert not audit.errors, audit.errors
        # 幂等：再跑无事可做
        assert compact_market(tmp_path, conn, MARKET, now=AFTER) == []
    finally:
        conn.close()


def test_compaction_absorbs_late_segment_into_existing_day(
    tmp_path: Path,
) -> None:
    """日分区已存在时，迟到段与之再合并，旧日 attempt 退出活动头。"""
    _write_segment(tmp_path, "run-a", [
        _trade("17000000", "2026-08-11T00:00:00.000Z"),
    ])
    conn = store.connect(tmp_path)
    try:
        materialize_all(tmp_path, conn, report_reused=False)
        first = compact_market(tmp_path, conn, MARKET, now=AFTER)
        assert first[0].merged_heads == 1
        _write_segment(tmp_path, "run-b", [
            _trade("17000003", "2026-08-11T18:00:00.000Z"),
        ])
        materialize_all(tmp_path, conn, report_reused=False)
        assert set(_heads(conn)) == {"day/2026-08-11", "run-b/segment-000001"}
        second = compact_market(tmp_path, conn, MARKET, now=AFTER)
        assert second[0].merged_heads == 2
        assert second[0].attempt_id != first[0].attempt_id
        assert _heads(conn) == {"day/2026-08-11": 2}
        assert audit_realtime_trades(tmp_path, conn)["ok"] is True
        assert not audit_materializations(tmp_path, conn).errors
    finally:
        conn.close()


def test_merge_refuses_duplicate_observations(tmp_path: Path) -> None:
    """跨文件重复观察拒绝拼接。"""
    from guvolu.data.trade_realtime_compact import _write_merged_parquet

    db = duckdb.connect(":memory:")
    try:
        db.execute("SET TimeZone='UTC'")
        db.execute(
            "CREATE TABLE t AS SELECT 'gmo|m|1|r0' AS observation_id,"
            "TIMESTAMPTZ '2026-08-11 00:00:00+00' AS event_time,"
            "'1' AS venue_trade_id, 0::BIGINT AS source_row_index,"
            "-1 AS source_item_index, 'sha256-a' AS source_artifact_id"
        )
        for name in ("a", "b"):
            db.execute(
                f"COPY t TO '{(tmp_path / name).as_posix()}.parquet' "
                "(FORMAT PARQUET)"
            )
    finally:
        db.close()
    with pytest.raises(CompactionError, match="重复观察"):
        _write_merged_parquet(
            [tmp_path / "a.parquet", tmp_path / "b.parquet"],
            tmp_path / "merged.parquet",
        )
    assert not (tmp_path / "merged.parquet").exists()
