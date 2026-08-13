"""数据层单测：行格式、计划求缺、时间语义、重建。全程离线（C-13）。"""
import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest

from guvolu.data.kline_plan import (
    available_time,
    daily_dates,
    missing_requests,
    plan_requests,
    trading_day,
    yearly_dates,
)
from guvolu.data.raw_writer import RawWriter, reconcile_unfinished_runs
from guvolu.data.raw_records import ws_channel, ws_payload
from guvolu.data.rebuild import rebuild_klines
from guvolu.data.store import (
    DB_SCHEMA_VERSION,
    connect,
    fetched_periods,
    insert_alert_event,
    insert_book_feature,
    upsert_klines,
)
from guvolu.domain.enums import KlineInterval


def test_trading_day_boundary() -> None:
    """交易日按 JST 06:00 归属（D-08）。"""
    before = datetime(2026, 8, 5, 20, 59, tzinfo=UTC)
    after = datetime(2026, 8, 5, 21, 0, tzinfo=UTC)
    assert trading_day(before) == "20260805"
    assert trading_day(after) == "20260806"


def test_available_time_month_rollover() -> None:
    """月线收束跨月，十二月跨年（D-04）。"""
    december = datetime(2025, 12, 1, tzinfo=UTC)
    result = available_time(KlineInterval.MONTH_1, december)
    assert result == datetime(2026, 1, 1, tzinfo=UTC)


def test_available_time_hourly() -> None:
    """小时线收束为开盘加一小时。"""
    opened = datetime(2026, 8, 5, 10, 0, tzinfo=UTC)
    result = available_time(KlineInterval.HOUR_1, opened)
    assert result == datetime(2026, 8, 5, 11, 0, tzinfo=UTC)


def test_date_enumeration() -> None:
    """年序列与日序列闭区间。"""
    years = yearly_dates(datetime(2026, 8, 6, tzinfo=UTC))
    assert years[0] == "2018" and years[-1] == "2026"
    days = daily_dates("20260130", "20260202")
    assert days == ["20260130", "20260131", "20260201", "20260202"]


def test_missing_requests_skips_complete_periods() -> None:
    """历史期已取则跳过，当期一律重取。"""
    plan = plan_requests(["BTC"], [KlineInterval.DAY_1], ["2025", "2026"])
    fetched = {("BTC", "1day", "2025"), ("BTC", "1day", "2026")}
    todo = missing_requests(plan, fetched, "2026")
    assert [item[2] for item in todo] == ["2026"]


def test_raw_writer_line_and_manifest(tmp_path: Path) -> None:
    """行字段齐全且清单计数正确。"""
    writer = RawWriter(tmp_path, run_id="runtest")
    writer.rest(
        "klines",
        "rest_public",
        "GET",
        "/v1/klines",
        {"symbol": "BTC"},
        200,
        {"status": 0, "data": []},
        12.3,
    )
    writer.ws("ws_public", "trades", "BTC", {"price": "1"})
    manifest_path = writer.finish()
    day_dir = manifest_path.parent
    line = json.loads(
        (day_dir / "klines.jsonl").read_text(encoding="utf-8").splitlines()[0]
    )
    for key in ("schema_version", "run_id", "source", "path", "ingest_time", "payload"):
        assert key in line
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["record_counts"] == {"klines": 1, "ws_public": 1}


def test_raw_writer_preserves_wire_frame_before_parse(tmp_path: Path) -> None:
    """wire 帧逐字节可逆，读取侧兼容新格式且畸形帧仍会落盘。"""
    writer = RawWriter(tmp_path, run_id="wiretest")
    raw = '{"channel":"trades","symbol":"BTC","price":"1"}'
    writer.ws_frame("ws_public", raw)
    malformed = '{"channel":"trades"'
    writer.ws_frame("ws_public", malformed)
    manifest_path = writer.finish()
    records = [
        json.loads(line)
        for line in (manifest_path.parent / "ws_public.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
    ]
    assert records[0]["payload_raw"] == raw
    payload = ws_payload(records[0])
    assert payload is not None
    assert ws_channel(records[0], payload) == "trades"
    assert ws_payload(records[1]) is None
    assert json.loads(manifest_path.read_text(encoding="utf-8"))["record_counts"] == {
        "ws_public": 2
    }


def test_reconcile_unfinished_raw_run_is_append_only(tmp_path: Path) -> None:
    """静默旧 run 以恢复清单封口，旧 checkpoint 不覆盖。"""
    day = tmp_path / "raw" / "2026-08-01"
    day.mkdir(parents=True)
    record = {
        "schema_version": 1,
        "run_id": "runold",
        "source": "ws_public",
        "payload_raw": "{}",
        "ingest_time": "2026-08-01T00:00:00+00:00",
    }
    (day / "ws_public.jsonl").write_text(
        json.dumps(record) + "\n", encoding="utf-8"
    )
    open_path = day / "manifest-runold.json"
    open_path.write_text(
        json.dumps({
            "status": "open", "heartbeat": True, "run_id": "runold",
            "record_counts": {"ws_public": 1},
        }),
        encoding="utf-8",
    )

    paths = reconcile_unfinished_runs(tmp_path, older_minutes=1)
    assert len(paths) == 1
    assert paths[0].name == "manifest-runold-reconciled.json"
    manifest = json.loads(paths[0].read_text(encoding="utf-8"))
    assert manifest["status"] == "recovered_incomplete"
    assert manifest["completion_claim"] is False
    assert manifest["record_counts"] == {"ws_public": 1}
    assert open_path.exists()
    assert reconcile_unfinished_runs(tmp_path, older_minutes=1) == ()


def test_store_upsert_idempotent(tmp_path: Path) -> None:
    """主键幂等，重复写入零新增。"""
    conn = connect(tmp_path)
    row = (
        "BTC", "1day", "2026-08-05T00:00:00+00:00", "2026-08-06T00:00:00+00:00",
        "2026-08-06T01:00:00+00:00", "20260805",
        "1", "2", "0.5", "1.5", "10", 0, "d/klines.jsonl:1",
    )
    assert upsert_klines(conn, [row]) == 1
    assert upsert_klines(conn, [row]) == 0
    assert ("BTC", "1day", "20260805") in fetched_periods(conn)
    assert ("BTC", "1day", "2026") in fetched_periods(conn)
    conn.close()


def test_book_feature_replay_reuses_row(tmp_path: Path) -> None:
    """同请求同配置重复落库复用既有行，报警不重复。"""
    conn = connect(tmp_path)
    row = (
        "liquidity_vacuum", "gmo", "BTC", "100", "100",
        "2026-01-03T00:01:35+00:00", "2026-01-03T00:01:40+00:00",
        '{"min_depth": "0"}', "hashcfg", "2026-08-10T00:00:00+00:00",
        "reqhash1",
    )
    first = insert_book_feature(conn, row)
    again = insert_book_feature(conn, row)
    assert first == again
    count = conn.execute("SELECT COUNT(*) FROM book_feature").fetchone()[0]
    assert count == 1
    alert_first = insert_alert_event(conn, first, "vacuum-btc", "t1")
    alert_again = insert_alert_event(conn, again, "vacuum-btc", "t2")
    assert alert_first == alert_again
    assert conn.execute("SELECT COUNT(*) FROM alert_event").fetchone()[0] == 1
    conn.close()


def test_store_migrates_v3_book_feature(tmp_path: Path) -> None:
    """存量 v3 库补 request_hash 列并可用。"""
    import sqlite3

    path = tmp_path / "guvolu.sqlite3"
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE book_feature (
          feature_id INTEGER PRIMARY KEY AUTOINCREMENT,
          kind TEXT NOT NULL, venue_id TEXT NOT NULL, symbol TEXT NOT NULL,
          price_low TEXT NOT NULL, price_high TEXT NOT NULL,
          from_ts TEXT NOT NULL, to_ts TEXT NOT NULL, metrics TEXT NOT NULL,
          config_hash TEXT NOT NULL, created_at TEXT NOT NULL
        );
        CREATE TABLE alert_event (
          alert_id INTEGER PRIMARY KEY AUTOINCREMENT,
          feature_id INTEGER NOT NULL, rule_id TEXT NOT NULL,
          triggered_at TEXT NOT NULL, acked_at TEXT
        );
        PRAGMA user_version=3;
        """
    )
    conn.close()
    conn = connect(tmp_path)
    cols = {row[1] for row in conn.execute("PRAGMA table_info(book_feature)")}
    assert "request_hash" in cols
    assert conn.execute("PRAGMA user_version").fetchone()[0] >= DB_SCHEMA_VERSION
    row = (
        "absorption", "gmo", "BTC", "100", "101",
        "2026-01-03T00:00:00+00:00", "2026-01-03T00:01:00+00:00",
        '{"executed_total": "1"}', "hashcfg", "2026-08-10T00:00:00+00:00",
        "reqhash2",
    )
    assert insert_book_feature(conn, row) >= 1
    conn.close()


def test_store_migrates_v5_analysis_run(tmp_path: Path) -> None:
    """存量 v5 台账补基准与窗列数两列并可插读。"""
    import sqlite3

    from guvolu.data.store import insert_analysis_run, list_analysis_runs

    path = tmp_path / "guvolu.sqlite3"
    raw_conn = sqlite3.connect(path)
    raw_conn.executescript(
        """
        CREATE TABLE analysis_run (
          run_id TEXT PRIMARY KEY, request_hash TEXT NOT NULL UNIQUE,
          venue_id TEXT NOT NULL, symbol TEXT NOT NULL,
          price_low TEXT NOT NULL, price_high TEXT NOT NULL,
          from_ts TEXT NOT NULL, to_ts TEXT NOT NULL, bucket TEXT NOT NULL,
          judgments TEXT NOT NULL, baseline TEXT NOT NULL,
          baseline_hash TEXT NOT NULL, source_hash TEXT NOT NULL,
          config_hash TEXT NOT NULL, code_version TEXT NOT NULL,
          confidence_version TEXT NOT NULL, created_at TEXT NOT NULL,
          status TEXT NOT NULL, failure_detail TEXT
        );
        PRAGMA user_version=5;
        """
    )
    raw_conn.close()
    conn = connect(tmp_path)
    cols = {row[1] for row in conn.execute("PRAGMA table_info(analysis_run)")}
    assert {"basis", "window_columns"} <= cols
    run_id = insert_analysis_run(
        conn,
        (
            "runa", "reqa", "gmo", "BTC", "100", "101",
            "2026-01-03T00:00:00+00:00", "2026-01-03T00:01:00+00:00",
            "1s", "[]", "{}", "bh", "sh", "ch", "code", "conf",
            "2026-08-10T00:00:00+00:00", "complete", None,
            "quantity", 60,
        ),
    )
    assert run_id == "runa"
    rows = list_analysis_runs(conn, "BTC", 10)
    assert rows[0][7] == "quantity"
    assert rows[0][8] == 60
    conn.close()


def test_store_refuses_newer_schema_without_mutation(tmp_path: Path) -> None:
    path = tmp_path / "guvolu.sqlite3"
    raw = sqlite3.connect(path)
    raw.execute(f"PRAGMA user_version={DB_SCHEMA_VERSION + 1}")
    raw.execute("CREATE TABLE future_only (identity TEXT PRIMARY KEY)")
    raw.execute("INSERT INTO future_only VALUES ('preserved')")
    raw.commit()
    raw.close()

    with pytest.raises(RuntimeError, match="newer than supported"):
        connect(tmp_path)

    check = sqlite3.connect(path)
    assert check.execute("PRAGMA user_version").fetchone()[0] == DB_SCHEMA_VERSION + 1
    assert check.execute("SELECT identity FROM future_only").fetchone() == (
        "preserved",
    )
    assert check.execute(
        "SELECT name FROM sqlite_master WHERE name='venue'"
    ).fetchone() is None
    check.close()


def test_rebuild_from_raw_fixture(tmp_path: Path) -> None:
    """重建吃成功行、跳过错误行，raw_source 带行号。"""
    day_dir = tmp_path / "raw" / "2026-08-06"
    day_dir.mkdir(parents=True)
    good = {
        "schema_version": 1,
        "run_id": "runtest",
        "source": "rest_public",
        "method": "GET",
        "path": "/v1/klines",
        "params": {"symbol": "BTC", "interval": "1day", "date": "2026"},
        "ingest_time": "2026-08-06T01:00:00+00:00",
        "latency_ms": 10.0,
        "http_status": 200,
        "payload": {
            "status": 0,
            "data": [
                {
                    "openTime": "1754352000000",
                    "open": "1", "high": "2", "low": "0.5",
                    "close": "1.5", "volume": "10",
                }
            ],
        },
        "network_error": None,
    }
    bad = dict(good)
    bad["payload"] = {"status": 2, "messages": [{"message_code": "ERR-5207"}]}
    lines = [json.dumps(bad), json.dumps(good)]
    (day_dir / "klines.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8")
    conn = connect(tmp_path)
    stats = rebuild_klines(tmp_path, conn)
    assert stats == {"scanned_lines": 2, "inserted_rows": 1}
    source = conn.execute("SELECT raw_source FROM kline").fetchone()[0]
    assert source == "2026-08-06/klines.jsonl:2"
    assert rebuild_klines(tmp_path, conn)["inserted_rows"] == 0
