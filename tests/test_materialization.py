"""分析物化的键链、PIT 与幂等验证。"""
from __future__ import annotations

import hashlib
import gzip
import json
import os
import sqlite3
from pathlib import Path

import pytest

from guvolu.data.materialize import (
    _register_content_artifact,
    audit_materializations,
    artifact_id,
    market_id,
    materialize_archive_trade_month,
    materialize_trade_month,
    open_analytics,
    plan_archive_backfill,
    sha256_file,
)
from guvolu.data.store import (
    DB_SCHEMA_VERSION,
    connect,
    insert_trade_ticks,
    upsert_coverage,
    upsert_normalized_partition,
)


def _seed_partition(
    root: Path, available_time: str
) -> tuple[Path, sqlite3.Connection]:
    """建立两行已核对 bitbank 输入。"""
    archive = (
        root / "archive" / "bitbank" / "trades" / "btc_jpy"
        / "2026" / "20260807_btc_jpy.json.gz"
    )
    archive.parent.mkdir(parents=True)
    archive.write_bytes(b"sealed-source-bytes")
    source_sha = hashlib.sha256(archive.read_bytes()).hexdigest()
    conn = connect(root)
    raw_source = "data/archive/bitbank/trades/btc_jpy/2026/20260807_btc_jpy.json.gz"
    upsert_normalized_partition(conn, (
        "bitbank", "btc_jpy", "trade", "20260807", raw_source,
        source_sha, 2, 2, 0, "2026-08-11T00:00:00+00:00",
        "2026-08-11T00:01:00+00:00", "trade-normalization-v1", 1,
        "complete", None,
    ))
    event_one = "2026-08-07T00:00:01+00:00"
    event_two = "2026-08-07T00:00:02+00:00"
    insert_trade_ticks(conn, [
        (
            "bitbank", "SPOT:BTC/JPY", "101", event_one,
            available_time, "2026-08-07T00:00:03+00:00", "buy",
            "taker", "100", "0.01", "match", "native", None,
            None, None, "exchange", "trade-normalization-v1", 1, 0, 1,
            "archive/bitbank/trades/btc_jpy/2026/20260807_btc_jpy.json.gz:1",
        ),
        (
            "bitbank", "SPOT:BTC/JPY", "102", event_two,
            "2026-08-07T00:00:02+00:00",
            "2026-08-07T00:00:03+00:00", "sell", "taker", "101",
            "0.02", "match", "native", None, None, None, "exchange",
            "trade-normalization-v1", 1, 0, 2,
            "archive/bitbank/trades/btc_jpy/2026/20260807_btc_jpy.json.gz:2",
        ),
    ])
    return archive, conn


def test_trade_materialization_preserves_lineage_and_reuses(tmp_path: Path) -> None:
    """Parquet 保留三键血缘，完全相同输入重放复用输出。"""
    root = tmp_path / "data"
    archive, conn = _seed_partition(
        root, "2026-08-07T00:00:01+00:00"
    )
    first = materialize_trade_month(
        root, conn, "bitbank", "btc_jpy", "2026-08"
    )
    assert first.row_count == 2
    assert not first.reused
    assert first.market_id == "mkt__bitbank__btc_jpy__r0"
    assert first.output_artifact_id.startswith("sha256-")
    output = root / first.output_path
    assert output.is_file()
    query = open_analytics()
    rows = query.execute(
        "SELECT market_id, source_artifact_id, normalization_version, "
        "price, typeof(price) FROM read_parquet(?) ORDER BY event_time",
        [str(output)],
    ).fetchall()
    query.close()
    source_identity = artifact_id(hashlib.sha256(archive.read_bytes()).hexdigest())
    assert rows == [
        (
            market_id("bitbank", "btc_jpy", 0), source_identity,
            "trade-normalization-v1", "100", "VARCHAR",
        ),
        (
            market_id("bitbank", "btc_jpy", 0), source_identity,
            "trade-normalization-v1", "101", "VARCHAR",
        ),
    ]
    again = materialize_trade_month(
        root, conn, "bitbank", "btc_jpy", "2026-08"
    )
    assert again.reused
    assert again.attempt_id == first.attempt_id
    assert conn.execute("PRAGMA user_version").fetchone()[0] == DB_SCHEMA_VERSION
    assert conn.execute("SELECT COUNT(*) FROM partition_attempt").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM partition_input").fetchone()[0] == 1
    binding = conn.execute(
        "SELECT venue_id, domain, endpoint, revision_id, binding_basis "
        "FROM partition_capability_binding"
    ).fetchone()
    assert binding == (
        "bitbank", "trade", "transactions/{day}", 0, "recorded",
    )
    manifest_path = next(output.parent.glob("manifest-*.json"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["capability_bindings"] == [{
        "venue_id": "bitbank",
        "domain": "trade",
        "endpoint": "transactions/{day}",
        "revision_id": 0,
        "binding_basis": "recorded",
    }]
    audit = audit_materializations(root, conn)
    assert not audit.errors
    assert audit.outputs_checked == 1
    assert audit.rows_checked == 2
    conn.close()


def test_generic_audit_compares_content_and_location_input_ledgers(
    tmp_path: Path,
) -> None:
    """输入内容总账必须与位置分账的四项计数一致。"""
    root = tmp_path / "data"
    _, conn = _seed_partition(
        root, "2026-08-07T00:00:01+00:00"
    )
    result = materialize_trade_month(
        root, conn, "bitbank", "btc_jpy", "2026-08"
    )
    conn.execute(
        "UPDATE partition_input_binding SET normalized_rows=normalized_rows+1 "
        "WHERE attempt_id=?",
        (result.attempt_id,),
    )
    conn.commit()

    audit = audit_materializations(root, conn)

    assert any(
        error.startswith(
            f"输入位置台账不符: {result.attempt_id} sha256-"
        )
        for error in audit.errors
    )
    conn.execute(
        "UPDATE partition_input SET normalized_rows=normalized_rows+1 "
        "WHERE attempt_id=?",
        (result.attempt_id,),
    )
    conn.commit()

    reconciled_locations = audit_materializations(root, conn)

    assert f"输入计数台账不符: {result.attempt_id}" in (
        reconciled_locations.errors
    )
    assert not any(
        error.startswith(
            f"输入位置台账不符: {result.attempt_id} "
        )
        for error in reconciled_locations.errors
    )
    conn.close()


def test_generic_audit_accepts_registered_historical_trade_schema(
    tmp_path: Path,
) -> None:
    """逐笔历史版本按规范化闭集核对，不强制升级为当前结构。"""
    root = tmp_path / "data"
    _, conn = _seed_partition(root, "2026-08-07T00:00:01+00:00")
    result = materialize_trade_month(
        root, conn, "bitbank", "btc_jpy", "2026-08"
    )
    output = root / result.output_path
    old = conn.execute(
        "SELECT o.artifact_id,o.row_count,o.min_event_time,o.max_event_time,"
        "o.created_at FROM materialization_output o WHERE o.attempt_id=?",
        (result.attempt_id,),
    ).fetchone()
    assert old is not None
    staged = output.with_name("schema-v2.parquet")
    query = open_analytics()
    source_sql = str(output).replace("'", "''")
    staged_sql = str(staged).replace("'", "''")
    query.execute(
        "COPY (SELECT * REPLACE ("
        "'trade-realtime-normalization-v2' AS normalization_version,"
        f"2 AS schema_version) FROM read_parquet('{source_sql}')) "
        f"TO '{staged_sql}' (FORMAT PARQUET)",
    )
    query.close()
    new_relative = result.output_path.replace(
        "schema_version=1", "schema_version=2"
    ).replace(
        "normalization_version=trade-normalization-v1",
        "normalization_version=trade-realtime-normalization-v2",
    )
    new_output = root / new_relative
    new_output.parent.mkdir(parents=True, exist_ok=True)
    output.unlink()
    os.replace(staged, new_output)
    sha = sha256_file(new_output)
    identity = artifact_id(sha)
    conn.execute(
        "DELETE FROM materialization_output WHERE attempt_id=?",
        (result.attempt_id,),
    )
    conn.execute(
        "DELETE FROM artifact_location WHERE artifact_id=?", (str(old[0]),)
    )
    conn.execute("DELETE FROM artifact WHERE artifact_id=?", (str(old[0]),))
    _register_content_artifact(
        conn, identity, "materialized_parquet", new_relative, sha,
        new_output.stat().st_size, str(old[4]), 2,
    )
    conn.execute(
        "INSERT INTO materialization_output VALUES (?,?,?,?,?,?,?)",
        (
            result.attempt_id, identity, "trade_observation", int(old[1]),
            old[2], old[3], old[4],
        ),
    )
    conn.execute(
        "UPDATE partition_attempt SET normalization_version=? "
        "WHERE attempt_id=?",
        ("trade-realtime-normalization-v2", result.attempt_id),
    )
    conn.execute(
        "UPDATE materialization_partition_head SET normalization_version=? "
        "WHERE attempt_id=?",
        ("trade-realtime-normalization-v2", result.attempt_id),
    )
    conn.commit()

    report = audit_materializations(root, conn)

    assert not report.errors
    assert report.rows_checked == 2
    conn.close()


def test_direct_archive_materialization_records_rejection_and_head(
    tmp_path: Path,
) -> None:
    """完整归档直写保留拒绝行，并原子推进活动分区。"""
    root = tmp_path / "data"
    archive = (
        root / "archive" / "bitflyer" / "executions" / "BTC_JPY"
        / "2026" / "20260807_BTC_JPY.jsonl.gz"
    )
    archive.parent.mkdir(parents=True)
    rows = [
        {
            "id": 1, "side": "BUY", "price": 100, "size": "0.01",
            "exec_date": "2026-08-07T00:00:01.000",
        },
        {
            "id": 2, "side": "", "price": 101, "size": "0.02",
            "exec_date": "2026-08-07T00:00:02.000",
        },
    ]
    body = "".join(json.dumps(row) + "\n" for row in rows)
    archive.write_bytes(gzip.compress(body.encode("utf-8")))
    conn = connect(root)
    upsert_coverage(conn, [
        (
            "bitflyer", "BTC_JPY", "trade", "20260807", 2,
            "2026-08-07T00:00:01+00:00",
            "2026-08-07T00:00:02+00:00", "ok",
            "2026-08-11T00:00:00+00:00",
        )
    ])
    result = materialize_archive_trade_month(
        root, conn, "bitflyer", "BTC_JPY", "2026-08"
    )
    assert result.row_count == 1
    assert result.rejected_rows == 1
    assert result.status == "complete_with_rejections"
    rejection = conn.execute(
        "SELECT source_row_index, raw_source, reason "
        "FROM materialization_rejection"
    ).fetchone()
    assert rejection[0] == 1
    assert str(rejection[1]).endswith(":2")
    assert "side" in str(rejection[2])
    head = conn.execute(
        "SELECT attempt_id FROM materialization_partition_head"
    ).fetchone()[0]
    assert head == result.attempt_id
    assert conn.execute(
        "SELECT COUNT(*) FROM partition_input_binding"
    ).fetchone()[0] == 1
    plan = plan_archive_backfill(
        root, conn, (("bitflyer", "BTC_JPY"),)
    )
    assert [(task.event_month, task.status) for task in plan.tasks] == [
        ("2026-08", "complete")
    ]
    conn.close()


def test_direct_archive_materialization_refuses_known_gap(
    tmp_path: Path,
) -> None:
    """覆盖台账已知缺失时不得把月份伪装成完整分区。"""
    root = tmp_path / "data"
    conn = connect(root)
    upsert_coverage(conn, [
        (
            "bitbank", "btc_jpy", "trade", "20260807", None,
            None, None, "missing", "2026-08-11T00:00:00+00:00",
        )
    ])
    with pytest.raises(ValueError, match="归档覆盖存在缺口: 20260807"):
        materialize_archive_trade_month(
            root, conn, "bitbank", "btc_jpy", "2026-08"
        )
    assert conn.execute(
        "SELECT COUNT(*) FROM partition_attempt"
    ).fetchone()[0] == 0
    plan = plan_archive_backfill(
        root, conn, (("bitbank", "btc_jpy"),)
    )
    assert plan.tasks[0].status == "blocked_missing"
    assert plan.tasks[0].reason == "missing_days=20260807"
    conn.close()


def test_direct_archive_materialization_isolates_identical_duplicate(
    tmp_path: Path,
) -> None:
    """完全相同的重复来源成交只写一条事实并保留拒绝证据。"""
    root = tmp_path / "data"
    archive = (
        root / "archive" / "bitflyer" / "executions" / "BTC_JPY"
        / "2026" / "20260807_BTC_JPY.jsonl.gz"
    )
    archive.parent.mkdir(parents=True)
    row = {
        "id": 1, "side": "BUY", "price": 100, "size": "0.01",
        "exec_date": "2026-08-07T00:00:01.000",
    }
    body = json.dumps(row) + "\n" + json.dumps(row) + "\n"
    archive.write_bytes(gzip.compress(body.encode("utf-8")))
    conn = connect(root)
    upsert_coverage(conn, [
        (
            "bitflyer", "BTC_JPY", "trade", "20260807", 2,
            "2026-08-07T00:00:01+00:00",
            "2026-08-07T00:00:01+00:00", "ok",
            "2026-08-11T00:00:00+00:00",
        )
    ])
    result = materialize_archive_trade_month(
        root, conn, "bitflyer", "BTC_JPY", "2026-08"
    )
    assert result.status == "complete_with_rejections"
    assert result.row_count == 1
    assert result.rejected_rows == 1
    assert conn.execute(
        "SELECT COUNT(*) FROM materialization_partition_head"
    ).fetchone()[0] == 1
    reason = conn.execute(
        "SELECT reason FROM materialization_rejection"
    ).fetchone()[0]
    assert "相同语义来源成交重复" in reason
    conn.close()


def test_direct_archive_materialization_rejects_conflicting_duplicate(
    tmp_path: Path,
) -> None:
    """同一来源成交身份若内容冲突，整月保持失败。"""
    root = tmp_path / "data"
    archive = (
        root / "archive" / "bitflyer" / "executions" / "BTC_JPY"
        / "2026" / "20260807_BTC_JPY.jsonl.gz"
    )
    archive.parent.mkdir(parents=True)
    rows = [
        {
            "id": 1, "side": "BUY", "price": 100, "size": "0.01",
            "exec_date": "2026-08-07T00:00:01.000",
        },
        {
            "id": 1, "side": "BUY", "price": 101, "size": "0.01",
            "exec_date": "2026-08-07T00:00:01.000",
        },
    ]
    body = "".join(json.dumps(row) + "\n" for row in rows)
    archive.write_bytes(gzip.compress(body.encode("utf-8")))
    conn = connect(root)
    upsert_coverage(conn, [
        (
            "bitflyer", "BTC_JPY", "trade", "20260807", 2,
            "2026-08-07T00:00:01+00:00",
            "2026-08-07T00:00:01+00:00", "ok",
            "2026-08-11T00:00:00+00:00",
        )
    ])
    with pytest.raises(ValueError, match="来源成交身份重复但语义冲突"):
        materialize_archive_trade_month(
            root, conn, "bitflyer", "BTC_JPY", "2026-08"
        )
    assert conn.execute(
        "SELECT status FROM partition_attempt"
    ).fetchone()[0] == "failed"
    assert conn.execute(
        "SELECT COUNT(*) FROM materialization_partition_head"
    ).fetchone()[0] == 0
    conn.close()


def test_gmo_archive_month_uses_0600_jst_session_boundary(
    tmp_path: Path,
) -> None:
    """GMO 月分区接纳首日 JST 06:00 对应的前一 UTC 日成交。"""
    root = tmp_path / "data"
    archive = (
        root / "archive" / "trades" / "ETH" / "2024" / "04"
        / "20240401_ETH.csv.gz"
    )
    archive.parent.mkdir(parents=True)
    body = (
        "symbol,side,size,price,timestamp\n"
        "ETH,SELL,0.0700,550345.000,2024-03-31 21:01:45.379\n"
    )
    archive.write_bytes(gzip.compress(body.encode("utf-8")))
    empty_archive = archive.with_name("20240402_ETH.csv.gz")
    empty_archive.write_bytes(gzip.compress(b""))
    conn = connect(root)
    upsert_coverage(conn, [
        (
            "gmo", "ETH", "trade", "20240401", 1,
            "2024-03-31T21:01:45.379000+00:00",
            "2024-03-31T21:01:45.379000+00:00", "ok",
            "2026-08-11T00:00:00+00:00",
        ),
        (
            "gmo", "ETH", "trade", "20240402", 0,
            None, None, "empty", "2026-08-11T00:00:00+00:00",
        ),
    ])
    result = materialize_archive_trade_month(
        root, conn, "gmo", "ETH", "2024-04"
    )
    assert result.status == "complete"
    assert conn.execute(
        "SELECT COUNT(*) FROM partition_input_binding"
    ).fetchone()[0] == 2
    query = open_analytics()
    event_time = query.execute(
        "SELECT event_time FROM read_parquet(?)",
        [str(root / result.output_path)],
    ).fetchone()[0]
    query.close()
    assert event_time.isoformat() == "2024-03-31T21:01:45.379000+00:00"
    conn.close()


def test_fully_empty_archive_month_commits_zero_row_partition(
    tmp_path: Path,
) -> None:
    """官方确认的全空月仍应形成可审计断点，而不是永久失败。"""
    root = tmp_path / "data"
    archive = (
        root / "archive" / "trades" / "ETH" / "2024" / "05"
        / "20240501_ETH.csv.gz"
    )
    archive.parent.mkdir(parents=True)
    archive.write_bytes(gzip.compress(b""))
    conn = connect(root)
    upsert_coverage(conn, [
        (
            "gmo", "ETH", "trade", "20240501", 0, None, None, "empty",
            "2026-08-11T00:00:00+00:00",
        )
    ])

    first = materialize_archive_trade_month(
        root, conn, "gmo", "ETH", "2024-05"
    )
    assert first.status == "complete"
    assert first.row_count == 0
    output = root / first.output_path
    query = open_analytics()
    assert query.execute(
        "SELECT COUNT(*) FROM read_parquet(?)", [str(output)]
    ).fetchone()[0] == 0
    query.close()
    assert conn.execute(
        "SELECT source_rows, normalized_rows, rejected_rows "
        "FROM partition_input_binding"
    ).fetchone() == (0, 0, 0)
    assert conn.execute(
        "SELECT min_event_time, max_event_time FROM materialization_output"
    ).fetchone() == (None, None)
    assert not audit_materializations(root, conn).errors

    second_archive = (
        root / "archive" / "trades" / "ETH" / "2024" / "06"
        / "20240601_ETH.csv.gz"
    )
    second_archive.parent.mkdir(parents=True)
    second_archive.write_bytes(gzip.compress(b""))
    upsert_coverage(conn, [
        (
            "gmo", "ETH", "trade", "20240601", 0, None, None, "empty",
            "2026-08-11T00:00:00+00:00",
        )
    ])
    second = materialize_archive_trade_month(
        root, conn, "gmo", "ETH", "2024-06"
    )
    assert second.output_artifact_id == first.output_artifact_id
    assert conn.execute(
        "SELECT COUNT(*) FROM artifact WHERE artifact_kind='materialized_parquet'"
    ).fetchone()[0] == 1
    assert conn.execute(
        "SELECT COUNT(*) FROM artifact_location WHERE artifact_id=?",
        (first.output_artifact_id,),
    ).fetchone()[0] == 2
    assert conn.execute(
        "SELECT SUM(is_canonical) FROM artifact_location WHERE artifact_id=?",
        (first.output_artifact_id,),
    ).fetchone()[0] == 1
    assert not audit_materializations(root, conn).errors

    replay = materialize_archive_trade_month(
        root, conn, "gmo", "ETH", "2024-05"
    )
    assert replay.reused
    assert replay.row_count == 0
    conn.close()


def test_trade_materialization_rejects_pit_violation(tmp_path: Path) -> None:
    """available_time 早于 event_time 时不产生完成分区。"""
    root = tmp_path / "data"
    _, conn = _seed_partition(
        root, "2026-08-06T23:59:59+00:00"
    )
    with pytest.raises(ValueError, match="PIT 违规"):
        materialize_trade_month(
            root, conn, "bitbank", "btc_jpy", "2026-08"
        )
    status = conn.execute(
        "SELECT status FROM partition_attempt"
    ).fetchone()[0]
    assert status == "failed"
    assert conn.execute(
        "SELECT COUNT(*) FROM materialization_output"
    ).fetchone()[0] == 0
    conn.close()
