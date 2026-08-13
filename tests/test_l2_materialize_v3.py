from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pytest

from guvolu.data import store
from guvolu.data.book_l2_contract import BOOK_L2_V3_NORMALIZATION_VERSION
from guvolu.data.l2_materialize import (
    L2_NORMALIZATION_VERSION,
    L2_SCHEMA_VERSION,
    L2Result,
    _DESCRIPTORS,
    _raw_metadata,
    _sealed_inputs,
    audit_l2,
    materialize_segment,
)


BASE = datetime(2026, 8, 12, tzinfo=UTC)


def _bitbank_payload(kind: str, sequence: int, offset_ms: int) -> str:
    millis = int(BASE.timestamp() * 1000) + offset_ms
    if kind == "snapshot":
        room = "depth_whole_btc_jpy"
        data = {
            "bids": [["100", "1"]], "asks": [["101", "1"]],
            "sequenceId": str(sequence), "timestamp": str(millis),
        }
    else:
        room = "depth_diff_btc_jpy"
        data = {
            "b": [["100", "2"]], "a": [["102", "1"]],
            "s": str(sequence), "t": str(millis),
        }
    return "42" + json.dumps([
        "message", {"room_name": room, "message": {"data": data}},
    ], separators=(",", ":"))


def _gmo_payload() -> str:
    return json.dumps({
        "channel": "orderbooks", "symbol": "BTC",
        "timestamp": BASE.isoformat(),
        "bids": [{"price": "100", "size": "1"}],
        "asks": [{"price": "101", "size": "2"}],
    }, separators=(",", ":"))


def _bitflyer_payload() -> str:
    return json.dumps({
        "jsonrpc": "2.0", "method": "channelMessage",
        "params": {
            "channel": "lightning_board_snapshot_BTC_JPY",
            "message": {
                "mid_price": 100,
                "bids": [
                    {"price": 99, "size": 0},
                    {"price": 98, "size": 1},
                ],
                "asks": [{"price": 101, "size": 2}],
            },
        },
    }, separators=(",", ":"))


def _write_segment(
    root: Path,
    venue: str,
    symbol: str,
    run_id: str,
    payloads: Sequence[tuple[str, str | None, str | None]],
    *,
    schema_version: int,
) -> None:
    descriptor = _DESCRIPTORS[venue]
    directory = (
        root / "raw/realtime/book_l2" / f"venue_id={venue}"
        / f"venue_symbol={symbol}" / f"run_id={run_id}"
    )
    directory.mkdir(parents=True)
    path = directory / "segment-000001.jsonl"
    rows: list[dict[str, object]] = []
    for index, (payload, connection_id, channel_id) in enumerate(
        payloads, start=1
    ):
        received = (BASE + timedelta(seconds=10, milliseconds=index)).isoformat()
        row: dict[str, object] = {
            "schema_version": schema_version,
            "run_id": run_id, "segment_sequence": 1,
            "record_sequence": index, "venue_id": venue,
            "venue_symbol": symbol, "domain": "book_l2",
            "source": "websocket",
            "source_endpoint": descriptor.endpoint,
            "payload_raw": payload, "ingest_time": received,
        }
        if schema_version in {2, 3}:
            row.update({
                "endpoint_id": descriptor.endpoint_id,
                "connection_id": connection_id, "channel_id": channel_id,
                "recv_ts_utc": received,
                "recv_ts_mono_ns": 10_000_000 + index,
                "raw_payload_sha256": hashlib.sha256(
                    payload.encode("utf-8")
                ).hexdigest(),
            })
        if schema_version == 3:
            row["endpoint_revision"] = descriptor.endpoint_revision
        rows.append(row)
    path.write_text(
        "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8"
    )
    sha = hashlib.sha256(path.read_bytes()).hexdigest()
    manifest = {
        "schema_version": schema_version, "status": "sealed",
        "completion_claim": True, "artifact_id": f"sha256-{sha}",
        "sha256": sha, "byte_count": path.stat().st_size,
        "record_count": len(rows), "run_id": run_id,
        "segment_sequence": 1, "venue_id": venue,
        "venue_symbol": symbol, "domain": "book_l2",
        "endpoint_id": (
            descriptor.endpoint_id if schema_version in {2, 3} else None
        ),
        "endpoint_revision": (
            descriptor.endpoint_revision if schema_version == 3 else None
        ),
        "storage_path": path.relative_to(root).as_posix(),
    }
    path.with_name("segment-000001.manifest.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )


def _materialize(root: Path) -> L2Result:
    conn = store.connect(root)
    try:
        result = materialize_segment(root, conn, _sealed_inputs(root)[0])
    finally:
        conn.close()
    return result


def test_legacy_raw_v1_becomes_explicit_nullable_v3_fact(tmp_path: Path) -> None:
    _write_segment(
        tmp_path, "gmo", "BTC", "run-legacy-v1",
        [(_gmo_payload(), None, None)], schema_version=1,
    )

    result = _materialize(tmp_path)

    assert result.status == "complete"
    db = duckdb.connect(":memory:")
    try:
        row = db.execute(
            "SELECT schema_version,normalization_version,"
            "capability_revision,endpoint,endpoint_id,endpoint_revision,"
            "connection_id,channel_id,"
            "recv_ts_mono_ns,raw_payload_sha256,data_quality,source_level,"
            "source_publish_time,available_time>=event_time,"
            "source_session_id=run_id FROM read_parquet(?)",
            [str(tmp_path / result.frame_path)],
        ).fetchone()
    finally:
        db.close()
    assert row is not None
    assert row[:9] == (
        L2_SCHEMA_VERSION, L2_NORMALIZATION_VERSION, 0, "orderbooks/ws",
        None, None, None, None, None,
    )
    assert row[9] == hashlib.sha256(_gmo_payload().encode()).hexdigest()
    quality = set(json.loads(row[10]))
    assert {
        "connection_boundary_unknown", "raw_payload_hash_derived",
        "recv_ts_mono_missing", "endpoint_revision_unrecorded",
    } <= quality
    assert row[11:] == ("L2", BASE, True, True)
    manifest = json.loads(next(
        (tmp_path / result.frame_path).parent.glob("manifest-*.json")
    ).read_text(encoding="utf-8"))
    assert manifest["schema_version"] == 3
    assert manifest["normalization_version"] == "book-l2-normalization-v5"
    assert L2_NORMALIZATION_VERSION != BOOK_L2_V3_NORMALIZATION_VERSION
    assert manifest["input_schema_version"] == 1
    assert manifest["input_endpoint_binding"] == {
        "endpoint_id": None, "endpoint_revision": None,
    }
    conn = store.connect(tmp_path)
    try:
        audit = audit_l2(tmp_path, conn)
        control_counts = conn.execute(
            "SELECT (SELECT COUNT(*) FROM collection_connection),"
            "(SELECT COUNT(*) FROM collection_channel)"
        ).fetchone()
    finally:
        conn.close()
    assert audit["ok"] is True
    assert audit["frames"] == 1
    assert control_counts == (0, 0)


def test_bitbank_source_sequence_is_preserved_without_local_fact_inference(
    tmp_path: Path,
) -> None:
    run_id = "run-bitbank-v2"
    c1, c2 = f"{run_id}-c000001", f"{run_id}-c000002"
    payloads = [
        (_bitbank_payload("snapshot", 100, 0), c1, "depth_whole_btc_jpy"),
        (_bitbank_payload("delta", 100, 1), c1, "depth_diff_btc_jpy"),
        (_bitbank_payload("snapshot", 100, 2), c1, "depth_whole_btc_jpy"),
        (_bitbank_payload("delta", 101, 3), c1, "depth_diff_btc_jpy"),
        (_bitbank_payload("delta", 101, 4), c1, "depth_diff_btc_jpy"),
        (_bitbank_payload("delta", 102, 5), c1, "depth_diff_btc_jpy"),
        (_bitbank_payload("snapshot", 102, 6), c1, "depth_whole_btc_jpy"),
        (_bitbank_payload("snapshot", 102, 7), c1, "depth_whole_btc_jpy"),
        (_bitbank_payload("delta", 103, 8), c1, "depth_diff_btc_jpy"),
        (_bitbank_payload("delta", 200, 9), c2, "depth_diff_btc_jpy"),
        (_bitbank_payload("snapshot", 200, 10), c2, "depth_whole_btc_jpy"),
        (_bitbank_payload("delta", 201, 11), c2, "depth_diff_btc_jpy"),
        (_bitbank_payload("delta", 199, 12), c2, "depth_diff_btc_jpy"),
    ]
    _write_segment(
        tmp_path, "bitbank", "btc_jpy", run_id, payloads,
        schema_version=2,
    )

    result = _materialize(tmp_path)

    assert result.status == "complete"
    assert (result.source_rows, result.frame_rows, result.rejected_rows) == (
        13, 13, 0,
    )
    db = duckdb.connect(":memory:")
    try:
        rows = db.execute(
            "SELECT connection_id,sequence_id,prev_sequence_id,data_quality,"
            "endpoint_id,endpoint_revision,channel_id,recv_ts_mono_ns "
            "FROM read_parquet(?) ORDER BY source_row_index",
            [str(tmp_path / result.frame_path)],
        ).fetchall()
    finally:
        db.close()
    assert [row[1] for row in rows] == [
        "100", "100", "100", "101", "101", "102", "102",
        "102", "103", "200", "200", "201", "199",
    ]
    assert all(row[2] is None for row in rows)
    local_flags = {
        "cross_room_same_sequence_observed",
        "replay_untrusted_until_snapshot",
        "sequence_predecessor_untrusted",
    }
    assert all(not (set(json.loads(row[3])) & local_flags) for row in rows)
    assert all(row[4] == "EP-0005" for row in rows)
    assert all(row[5] is None for row in rows)
    assert all("endpoint_revision_unrecorded" in json.loads(row[3]) for row in rows)
    assert all(row[6] and row[7] is not None for row in rows)
def test_bitbank_delayed_whole_sequence_is_independent_authority(
    tmp_path: Path,
) -> None:
    run_id = "run-bitbank-delayed-whole-v3"
    connection = f"{run_id}-c000001"
    payloads = [
        (_bitbank_payload("delta", 6, 1), connection, "depth_diff_btc_jpy"),
        (_bitbank_payload("delta", 8, 2), connection, "depth_diff_btc_jpy"),
        (_bitbank_payload("snapshot", 5, 3), connection, "depth_whole_btc_jpy"),
        (_bitbank_payload("delta", 9, 4), connection, "depth_diff_btc_jpy"),
    ]
    _write_segment(
        tmp_path, "bitbank", "btc_jpy", run_id, payloads,
        schema_version=3,
    )

    result = _materialize(tmp_path)

    assert result.status == "complete"
    assert (result.source_rows, result.frame_rows, result.rejected_rows) == (
        4, 4, 0,
    )
    db = duckdb.connect(":memory:")
    try:
        rows = db.execute(
            "SELECT message_kind,sequence_id,prev_sequence_id,data_quality "
            "FROM read_parquet(?) ORDER BY source_row_index",
            [str(tmp_path / result.frame_path)],
        ).fetchall()
    finally:
        db.close()
    assert [(row[0], row[1], row[2]) for row in rows] == [
        ("delta", "6", None),
        ("delta", "8", None),
        ("snapshot", "5", None),
        ("delta", "9", None),
    ]
    local_flags = {
        "cross_room_same_sequence_observed",
        "replay_untrusted_until_snapshot",
        "sequence_predecessor_untrusted",
    }
    assert all(not (set(json.loads(row[3])) & local_flags) for row in rows)


def test_bitflyer_snapshot_zero_levels_are_ignored_not_set(
    tmp_path: Path,
) -> None:
    run_id = "run-bitflyer-v2"
    _write_segment(
        tmp_path, "bitflyer", "BTC_JPY", run_id,
        [(
            _bitflyer_payload(), f"{run_id}-c000001",
            "lightning_board_snapshot_BTC_JPY",
        )],
        schema_version=2,
    )

    result = _materialize(tmp_path)

    assert (result.source_rows, result.frame_rows, result.level_rows) == (1, 1, 2)
    db = duckdb.connect(":memory:")
    try:
        levels = db.execute(
            "SELECT size,action,order_count FROM read_parquet(?) "
            "ORDER BY side,source_level_index",
            [str(tmp_path / result.level_path)],
        ).fetchall()
        quality = db.execute(
            "SELECT data_quality,source_publish_time FROM read_parquet(?)",
            [str(tmp_path / result.frame_path)],
        ).fetchone()
    finally:
        db.close()
    assert levels == [("2", "set", None), ("1", "set", None)]
    assert quality is not None and quality[1] is None
    assert {
        "snapshot_zero_levels_ignored", "source_publish_time_missing",
        "raw_payload_hash_verified", "endpoint_revision_unrecorded",
    } <= set(json.loads(quality[0]))


def test_raw_v3_binds_recorded_endpoint_revision_without_latest_lookup(
    tmp_path: Path,
) -> None:
    """v3 尚未投产，可在保持 schema=3 时补齐端点修订必填列。"""
    run_id = "run-gmo-v3-endpoint-r0"
    _write_segment(
        tmp_path, "gmo", "BTC", run_id,
        [(_gmo_payload(), f"{run_id}-c000001", "orderbooks")],
        schema_version=3,
    )

    result = _materialize(tmp_path)

    db = duckdb.connect(":memory:")
    try:
        row = db.execute(
            "SELECT endpoint_id,endpoint_revision,data_quality "
            "FROM read_parquet(?)", [str(tmp_path / result.frame_path)],
        ).fetchone()
    finally:
        db.close()
    assert row is not None
    assert row[:2] == ("EP-0007", 0)
    assert "endpoint_revision_unrecorded" not in json.loads(row[2])
    manifest = json.loads(next(
        (tmp_path / result.frame_path).parent.glob("manifest-*.json")
    ).read_text(encoding="utf-8"))
    assert manifest["input_endpoint_binding"] == {
        "endpoint_id": "EP-0007", "endpoint_revision": 0,
    }
    conn = store.connect(tmp_path)
    try:
        connection = conn.execute(
            "SELECT endpoint_id,endpoint_revision,collection_run_id,"
            "connection_ordinal,opened_at_basis FROM collection_connection"
        ).fetchone()
        channel = conn.execute(
            "SELECT channel_id,market_id,subscribed_at_basis,"
            "capability_domain,capability_endpoint,capability_revision "
            "FROM collection_channel"
        ).fetchone()
    finally:
        conn.close()
    assert connection == (
        "EP-0007", 0, run_id, 1,
        "first_successfully_materialized_raw_v3_frame",
    )
    assert channel == (
        "orderbooks", "mkt__gmo__btc__r0",
        "first_successfully_materialized_raw_v3_frame",
        "book_realtime", "orderbooks/ws", 0,
    )


def test_raw_v2_payload_hash_is_verified() -> None:
    descriptor = _DESCRIPTORS["gmo"]
    envelope = {
        "schema_version": 2, "run_id": "run-hash-v2",
        "segment_sequence": 1, "venue_id": "gmo", "venue_symbol": "BTC",
        "domain": "book_l2", "source_endpoint": descriptor.endpoint,
        "endpoint_id": descriptor.endpoint_id,
        "connection_id": "run-hash-v2-c000001", "channel_id": "orderbooks",
        "payload_raw": _gmo_payload(), "raw_payload_sha256": "0" * 64,
        "recv_ts_utc": BASE.isoformat(), "ingest_time": BASE.isoformat(),
        "recv_ts_mono_ns": 1,
    }
    item = type("Item", (), {
        "raw_schema_version": 2,
        "endpoint_id": descriptor.endpoint_id,
        "run_id": "run-hash-v2",
    })()

    with pytest.raises(ValueError, match="SHA-256"):
        _raw_metadata(envelope, item, descriptor)


def test_raw_v3_rejects_endpoint_revision_not_recorded_by_manifest() -> None:
    descriptor = _DESCRIPTORS["gmo"]
    envelope = {
        "schema_version": 3, "run_id": "run-endpoint-r0",
        "segment_sequence": 1, "venue_id": "gmo", "venue_symbol": "BTC",
        "domain": "book_l2", "source_endpoint": descriptor.endpoint,
        "endpoint_id": descriptor.endpoint_id, "endpoint_revision": 1,
        "connection_id": "run-endpoint-r0-c000001", "channel_id": "orderbooks",
        "payload_raw": _gmo_payload(),
        "raw_payload_sha256": hashlib.sha256(
            _gmo_payload().encode("utf-8")
        ).hexdigest(),
        "recv_ts_utc": BASE.isoformat(), "ingest_time": BASE.isoformat(),
        "recv_ts_mono_ns": 1,
    }
    item = type("Item", (), {
        "raw_schema_version": 3,
        "endpoint_id": descriptor.endpoint_id,
        "endpoint_revision": 0,
        "run_id": "run-endpoint-r0",
    })()

    with pytest.raises(ValueError, match="endpoint_revision"):
        _raw_metadata(envelope, item, descriptor)
