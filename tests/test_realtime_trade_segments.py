from __future__ import annotations

import hashlib
import json
from pathlib import Path

import duckdb
import pytest

from guvolu.data import store, trade_capture
from guvolu.data.segmented_raw import SegmentedRawWriter
from guvolu.data.trade_realtime_materialize import (
    TRADE_REALTIME_NORMALIZATION_VERSION,
    _ENDPOINT_BINDINGS,
    _sealed_inputs,
    audit_realtime_trades,
    materialize_all,
)


def _write_segment(
    root: Path, venue: str, symbol: str, payloads: list[str], run_id: str,
    *, endpoint_revision: int | None = None,
) -> Path:
    endpoint_id, current_revision = trade_capture.ENDPOINT_BINDINGS[venue]
    selected_revision = (
        current_revision if endpoint_revision is None else endpoint_revision
    )
    writer = SegmentedRawWriter(
        root, venue, symbol, domain="trade_realtime", run_id=run_id,
        endpoint_id=endpoint_id, endpoint_revision=selected_revision,
        segment_seconds=3600, segment_max_bytes=1024 * 1024,
    )
    source_endpoints = {
        "gmo": "trades/ws",
        "bitbank": "transactions",
        "bitflyer": "lightning_executions",
    }
    channels = {
        "gmo": "trades",
        "bitbank": f"transactions_{symbol}",
        "bitflyer": f"lightning_executions_{symbol}",
    }
    for payload in payloads:
        channel = (
            "protocol_control"
            if venue == "bitbank" and not payload.startswith("42")
            else channels[venue]
        )
        writer.write_frame(
            payload, source_endpoints[venue],
            connection_id=f"{run_id}-c000001", channel_id=channel,
        )
    writer.finish()
    return next(writer.directory.glob("segment-*.manifest.json"))


def _write_legacy_segment(
    root: Path, *, raw_schema: int, venue: str, symbol: str,
    run_id: str, payload: str, ingest_time: str,
) -> Path:
    """构造冻结旧契约；测试不得借现行 writer 伪装旧格式。"""
    directory = (
        root / "raw" / "realtime" / "trade_realtime"
        / f"venue_id={venue}" / f"venue_symbol={symbol}"
        / f"run_id={run_id}"
    )
    directory.mkdir(parents=True)
    source_endpoint = {
        "gmo": "trades/ws",
        "bitbank": "transactions",
        "bitflyer": "lightning_executions",
    }[venue]
    record: dict[str, object] = {
        "schema_version": raw_schema,
        "durability_version": "fsync-per-record-v1",
        "run_id": run_id,
        "segment_sequence": 1,
        "record_sequence": 1,
        "venue_id": venue,
        "venue_symbol": symbol,
        "domain": "trade_realtime",
        "source": "websocket",
        "source_endpoint": source_endpoint,
        "payload_raw": payload,
        "ingest_time": ingest_time,
    }
    endpoint_id: str | None = None
    if raw_schema == 2:
        endpoint_id = trade_capture.ENDPOINT_BINDINGS[venue][0]
        record.update({
            "endpoint_id": endpoint_id,
            "connection_id": f"{run_id}-c000001",
            "channel_id": (
                f"transactions_{symbol}" if venue == "bitbank" else "trades"
            ),
            "raw_payload_sha256": hashlib.sha256(
                payload.encode("utf-8")
            ).hexdigest(),
            "recv_ts_utc": ingest_time,
            "recv_ts_mono_ns": 123456789,
        })
    segment = directory / "segment-000001.jsonl"
    segment.write_text(
        json.dumps(record, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    sha = hashlib.sha256(segment.read_bytes()).hexdigest()
    storage = segment.relative_to(root).as_posix()
    manifest: dict[str, object] = {
        "schema_version": raw_schema,
        "status": "sealed",
        "completion_claim": True,
        "artifact_id": f"sha256-{sha}",
        "sha256": sha,
        "byte_count": segment.stat().st_size,
        "record_count": 1,
        "run_id": run_id,
        "segment_sequence": 1,
        "venue_id": venue,
        "venue_symbol": symbol,
        "domain": "trade_realtime",
        "endpoint_id": endpoint_id,
        "storage_path": storage,
    }
    path = directory / "segment-000001.manifest.json"
    path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return path


def test_three_venue_realtime_trade_fact_contract(tmp_path: Path) -> None:
    """三所逐笔帧保留 row/item 血缘并满足键与 PIT 契约。"""
    assert _ENDPOINT_BINDINGS == trade_capture.ENDPOINT_BINDINGS
    _write_segment(
        tmp_path, "gmo", "BTC", [json.dumps({
            "channel": "trades", "price": "17000000", "size": "0.01",
            "side": "BUY", "timestamp": "2026-08-11T00:00:00.000Z",
        })], "run-trade-gmo",
    )
    bitbank_packet = ["message", {
        "room_name": "transactions_btc_jpy",
        "message": {"data": {"transactions": [{
            "transaction_id": 101, "price": "17000001",
            "amount": "0.02", "side": "sell",
            "executed_at": 1786406400100,
        }, {
            "transaction_id": 102, "price": "17000002",
            "amount": "0.03", "side": "buy",
            "executed_at": 1786406400200,
        }]}},
    }]
    _write_segment(
        tmp_path, "bitbank", "btc_jpy",
        ["0{}", "42" + json.dumps(bitbank_packet)], "run-trade-bitbank",
    )
    _write_segment(
        tmp_path, "bitflyer", "BTC_JPY", [json.dumps({
            "jsonrpc": "2.0", "method": "channelMessage",
            "params": {
                "channel": "lightning_executions_BTC_JPY",
                "message": [{
                    "id": 201, "price": 17000003, "size": 0.04,
                    "side": "BUY", "exec_date": "2026-08-11T00:00:00.300Z",
                }],
            },
        })], "run-trade-bitflyer",
    )

    conn = store.connect(tmp_path)
    try:
        results = materialize_all(tmp_path, conn, report_reused=False)
        assert len(results) == 3
        assert sum(result.trade_rows for result in results) == 4
        assert sum(result.ignored_rows for result in results) == 1
        assert audit_realtime_trades(tmp_path, conn)["ok"] is True
        controls = conn.execute(
            "SELECT COUNT(*),COUNT(DISTINCT endpoint_id),"
            "MIN(connection_ordinal),MAX(connection_ordinal),"
            "COUNT(DISTINCT opened_at_basis) FROM collection_connection"
        ).fetchone()
        channels = conn.execute(
            "SELECT COUNT(*),COUNT(DISTINCT market_id),"
            "COUNT(DISTINCT subscribed_at_basis),"
            "COUNT(DISTINCT capability_domain) FROM collection_channel"
        ).fetchone()
        paths = [tmp_path / result.output_path for result in results]
    finally:
        conn.close()

    db = duckdb.connect(":memory:")
    try:
        rows = db.execute(
            "SELECT venue_id,source_row_index,source_item_index,"
            "capability_revision,normalization_version,endpoint_id,"
            "endpoint_revision,connection_id,channel_id,recv_ts_mono_ns,"
            "raw_payload_sha256,data_quality,raw_schema_version "
            "FROM read_parquet(?, union_by_name=true) "
            "ORDER BY venue_id,venue_trade_id",
            [[str(path) for path in paths]],
        ).fetchall()
        side_basis = db.execute(
            "SELECT venue_id,source_side_basis FROM "
            "read_parquet(?, union_by_name=true) ORDER BY venue_id",
            [[str(path) for path in paths]],
        ).fetchall()
    finally:
        db.close()
    assert len(rows) == 4
    assert {(row[0], row[1], row[2]) for row in rows} == {
        ("gmo", 1, 0),
        ("bitbank", 2, 0),
        ("bitbank", 2, 1),
        ("bitflyer", 1, 0),
    }
    assert all(row[3] == 0 for row in rows)
    assert all(row[4] == TRADE_REALTIME_NORMALIZATION_VERSION for row in rows)
    assert {(row[0], row[5], row[6]) for row in rows} == {
        ("gmo", "EP-0007", 1),
        ("bitbank", "EP-0075", 0),
        ("bitflyer", "EP-0002", 0),
    }
    assert all(str(row[7]).endswith("-c000001") for row in rows)
    assert all(row[8] not in {None, "protocol_control"} for row in rows)
    assert all(isinstance(row[9], int) and row[9] >= 0 for row in rows)
    assert all(len(str(row[10])) == 64 for row in rows)
    assert all("endpoint_binding_verified" in str(row[11]) for row in rows)
    assert all(row[12] == 3 for row in rows)
    assert controls == (3, 3, 1, 1, 1)
    assert channels == (3, 3, 1, 1)
    assert ("gmo", "taker") in side_basis


def test_legacy_gmo_raw_v3_preserves_unfiltered_participant_side(
    tmp_path: Path,
) -> None:
    """旧 GMO r0 不得把公开双方成交腿继续声明为 taker。"""
    _write_segment(
        tmp_path,
        "gmo",
        "BTC",
        [json.dumps({
            "channel": "trades", "price": "17000000", "size": "0.01",
            "side": "BUY", "timestamp": "2026-08-11T00:00:00.000Z",
        })],
        "run-trade-gmo-r0",
        endpoint_revision=0,
    )
    conn = store.connect(tmp_path)
    try:
        result = materialize_all(tmp_path, conn, report_reused=False)[0]
        assert audit_realtime_trades(tmp_path, conn)["ok"] is True
    finally:
        conn.close()
    db = duckdb.connect(":memory:")
    try:
        row = db.execute(
            "SELECT endpoint_revision,source_side_basis FROM read_parquet(?)",
            [str(tmp_path / result.output_path)],
        ).fetchone()
    finally:
        db.close()
    assert row == (0, "participant_side_unfiltered")


def test_bitflyer_bad_item_rejects_whole_frame_without_blocking_gmo(
    tmp_path: Path,
) -> None:
    """一个 execution 非法时整帧 reject，且后续市场仍能物化。"""
    malformed = json.dumps({
        "jsonrpc": "2.0", "method": "channelMessage",
        "params": {
            "channel": "lightning_executions_BTC_JPY",
            "message": [
                {
                    "id": 301, "price": 17000000, "size": 0.01,
                    "side": "", "exec_date": "2026-08-11T00:00:00.100Z",
                },
                {
                    "id": 302, "price": 17000001, "size": 0.02,
                    "side": "BUY", "exec_date": "2026-08-11T00:00:00.200Z",
                },
            ],
        },
    })
    valid = json.dumps({
        "jsonrpc": "2.0", "method": "channelMessage",
        "params": {
            "channel": "lightning_executions_BTC_JPY",
            "message": [{
                "id": 303, "price": 17000002, "size": 0.03,
                "side": "SELL", "exec_date": "2026-08-11T00:00:00.300Z",
            }],
        },
    })
    _write_segment(
        tmp_path, "bitflyer", "BTC_JPY", [malformed, valid],
        "run-trade-bitflyer-atomic",
    )
    _write_segment(
        tmp_path, "gmo", "BTC", [json.dumps({
            "channel": "trades", "price": "17000003", "size": "0.04",
            "side": "BUY", "timestamp": "2026-08-11T00:00:00.400Z",
        })], "run-trade-gmo-after-bitflyer",
    )

    conn = store.connect(tmp_path)
    try:
        results = materialize_all(tmp_path, conn, report_reused=False)
        by_market = {result.market_id: result for result in results}
        bitflyer = by_market["mkt__bitflyer__btc_jpy__r0"]
        gmo = by_market["mkt__gmo__btc__r0"]
        assert bitflyer.status == "complete_with_rejections"
        assert bitflyer.source_rows == 2
        assert bitflyer.data_frames == 1
        assert bitflyer.trade_rows == 1
        assert bitflyer.rejected_rows == 1
        assert gmo.status == "complete"
        assert gmo.trade_rows == 1
        rejection = conn.execute(
            "SELECT source_row_index,reason FROM materialization_rejection "
            "WHERE attempt_id=?",
            (bitflyer.attempt_id,),
        ).fetchone()
        assert rejection is not None
        assert rejection[0] == 1
        assert "side" in rejection[1]
        assert audit_realtime_trades(tmp_path, conn)["ok"] is True
    finally:
        conn.close()


def test_legacy_raw_v1_v2_preserve_only_recorded_identity(tmp_path: Path) -> None:
    """v1 明确留空身份；v2 保留端点但不补造修订。"""
    gmo_payload = json.dumps({
        "channel": "trades", "price": "17000000", "size": "0.01",
        "side": "BUY", "timestamp": "2026-08-11T00:00:00.000Z",
    })
    bitbank_payload = "42" + json.dumps(["message", {
        "room_name": "transactions_btc_jpy",
        "message": {"data": {"transactions": [{
            "transaction_id": 501, "price": "17000001",
            "amount": "0.02", "side": "sell",
            "executed_at": 1786406400100,
        }]}},
    }])
    _write_legacy_segment(
        tmp_path, raw_schema=1, venue="gmo", symbol="BTC",
        run_id="run-legacy-trade-v1", payload=gmo_payload,
        ingest_time="2026-08-11T00:00:01+00:00",
    )
    _write_legacy_segment(
        tmp_path, raw_schema=2, venue="bitbank", symbol="btc_jpy",
        run_id="run-legacy-trade-v2", payload=bitbank_payload,
        ingest_time="2026-08-11T00:00:01+00:00",
    )

    conn = store.connect(tmp_path)
    try:
        results = materialize_all(tmp_path, conn, report_reused=False)
        assert len(results) == 2
        assert audit_realtime_trades(tmp_path, conn)["ok"] is True
        assert conn.execute(
            "SELECT (SELECT COUNT(*) FROM collection_connection),"
            "(SELECT COUNT(*) FROM collection_channel)"
        ).fetchone() == (0, 0)
        paths = [str(tmp_path / result.output_path) for result in results]
    finally:
        conn.close()

    db = duckdb.connect(":memory:")
    try:
        rows = db.execute(
            "SELECT raw_schema_version,endpoint_id,endpoint_revision,"
            "connection_id,channel_id,recv_ts_mono_ns,raw_payload_sha256,"
            "data_quality FROM read_parquet(?, union_by_name=true) "
            "ORDER BY raw_schema_version",
            [paths],
        ).fetchall()
        legacy_output = tmp_path / "legacy-trade-realtime-v2.parquet"
        escaped = legacy_output.as_posix().replace("'", "''")
        db.execute(
            "COPY (SELECT * EXCLUDE (endpoint_id,endpoint_revision,"
            "connection_id,channel_id,recv_ts_mono_ns,raw_payload_sha256,"
            "data_quality,raw_schema_version,normalization_version,"
            "schema_version),'trade-realtime-normalization-v2' AS "
            "normalization_version,2 AS schema_version FROM read_parquet(?) "
            f"LIMIT 1) TO '{escaped}' (FORMAT PARQUET)",
            [paths[0]],
        )
        union_count = db.execute(
            "SELECT COUNT(*) FROM read_parquet(?, union_by_name=true)",
            [[str(legacy_output), *paths]],
        ).fetchone()
    finally:
        db.close()
    assert union_count == (3,)
    assert rows[0][:6] == (1, None, None, None, None, None)
    assert rows[0][6] == hashlib.sha256(gmo_payload.encode()).hexdigest()
    assert "raw_payload_hash_derived" in json.loads(rows[0][7])
    assert rows[1][:6] == (
        2, "EP-0075", None, "run-legacy-trade-v2-c000001",
        "transactions_btc_jpy", 123456789,
    )
    assert rows[1][6] == hashlib.sha256(bitbank_payload.encode()).hexdigest()
    assert "endpoint_revision_unrecorded" in json.loads(rows[1][7])


def test_raw_v3_bad_payload_hash_never_becomes_trade_fact(
    tmp_path: Path,
) -> None:
    """即使 segment manifest 已重算，错误逐帧 payload hash 仍被拒绝。"""
    manifest_path = _write_segment(
        tmp_path, "gmo", "BTC", [json.dumps({
            "channel": "trades", "price": "17000000", "size": "0.01",
            "side": "BUY", "timestamp": "2026-08-11T00:00:00.000Z",
        })], "run-trade-bad-payload-hash",
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    segment = tmp_path / manifest["storage_path"]
    record = json.loads(segment.read_text(encoding="utf-8"))
    record["raw_payload_sha256"] = "0" * 64
    segment.write_text(json.dumps(record) + "\n", encoding="utf-8")
    sha = hashlib.sha256(segment.read_bytes()).hexdigest()
    manifest.update({
        "artifact_id": f"sha256-{sha}",
        "sha256": sha,
        "byte_count": segment.stat().st_size,
    })
    manifest_path.write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )

    conn = store.connect(tmp_path)
    try:
        results = materialize_all(tmp_path, conn, report_reused=False)
        assert len(results) == 1
        result = results[0]
        assert result.status == "complete_with_rejections"
        assert result.trade_rows == 0
        assert result.rejected_rows == 1
        assert conn.execute(
            "SELECT (SELECT COUNT(*) FROM collection_connection),"
            "(SELECT COUNT(*) FROM collection_channel)"
        ).fetchone() == (0, 0)
        reason = conn.execute(
            "SELECT reason FROM materialization_rejection WHERE attempt_id=?",
            (result.attempt_id,),
        ).fetchone()
        assert reason is not None
        assert "SHA-256" in reason[0]
    finally:
        conn.close()


def test_raw_v3_manifest_revision_must_be_json_integer(tmp_path: Path) -> None:
    manifest_path = _write_segment(
        tmp_path, "gmo", "BTC", [json.dumps({
            "channel": "trades", "price": "17000000", "size": "0.01",
            "side": "BUY", "timestamp": "2026-08-11T00:00:00.000Z",
        })], "run-trade-string-revision",
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["endpoint_revision"] = "0"
    manifest_path.write_text(json.dumps(manifest) + "\n", encoding="utf-8")

    with pytest.raises(ValueError, match="endpoint_revision 非整数"):
        _sealed_inputs(tmp_path)


def test_raw_v3_record_sequence_string_is_rejected_without_control_row(
    tmp_path: Path,
) -> None:
    manifest_path = _write_segment(
        tmp_path, "gmo", "BTC", [json.dumps({
            "channel": "trades", "price": "17000000", "size": "0.01",
            "side": "BUY", "timestamp": "2026-08-11T00:00:00.000Z",
        })], "run-trade-string-sequence",
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    segment = tmp_path / manifest["storage_path"]
    record = json.loads(segment.read_text(encoding="utf-8"))
    record["record_sequence"] = "1"
    segment.write_text(json.dumps(record) + "\n", encoding="utf-8")
    sha = hashlib.sha256(segment.read_bytes()).hexdigest()
    manifest.update({
        "artifact_id": f"sha256-{sha}",
        "sha256": sha,
        "byte_count": segment.stat().st_size,
    })
    manifest_path.write_text(json.dumps(manifest) + "\n", encoding="utf-8")

    conn = store.connect(tmp_path)
    try:
        result = materialize_all(tmp_path, conn, report_reused=False)[0]
        assert (result.trade_rows, result.rejected_rows) == (0, 1)
        assert conn.execute(
            "SELECT (SELECT COUNT(*) FROM collection_connection),"
            "(SELECT COUNT(*) FROM collection_channel)"
        ).fetchone() == (0, 0)
    finally:
        conn.close()
