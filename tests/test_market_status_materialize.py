from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pytest

from guvolu.data import store
from guvolu.data.l2_materialize import (
    _refresh_market_status_nonblocking,
    _sealed_inputs,
    materialize_segment as materialize_l2_segment,
)
from guvolu.data.market_status_materialize import (
    audit_market_status,
    materialize_all,
    sealed_status_inputs,
)


BASE = datetime(2026, 8, 12, 15, tzinfo=UTC)


def _socket_message(room: str, data: dict[str, object]) -> str:
    return "42" + json.dumps([
        "message", {"room_name": room, "message": {"data": data}},
    ], separators=(",", ":"))


def _write_r1_segment(root: Path) -> tuple[Path, int]:
    run_id = "run-bitbank-status-r1"
    symbol = "btc_jpy"
    directory = (
        root / "raw/realtime/book_l2/venue_id=bitbank"
        / f"venue_symbol={symbol}" / f"run_id={run_id}"
    )
    directory.mkdir(parents=True)
    millis = int(BASE.timestamp() * 1000)
    payloads = [
        ("0{\"sid\":\"x\"}", "protocol_control"),
        (_socket_message("depth_whole_btc_jpy", {
            "bids": [["100", "1"]], "asks": [["101", "2"]],
            "sequenceId": "10", "timestamp": millis,
        }), "depth_whole_btc_jpy"),
        (_socket_message("circuit_break_info_btc_jpy", {
            "mode": "CIRCUIT_BREAK",
            "estimated_itayose_price": "100.5",
            "estimated_itayose_amount": "3.25",
            "itayose_upper_price": "110",
            "itayose_lower_price": "90",
            "upper_trigger_price": None,
            "lower_trigger_price": None,
            "fee_type": "SELL_MAKER",
            "reopen_timestamp": millis + 60_000,
            "timestamp": millis + 1_000,
        }), "circuit_break_info_btc_jpy"),
    ]
    path = directory / "segment-000001.jsonl"
    rows: list[dict[str, object]] = []
    for index, (payload, channel) in enumerate(payloads, start=1):
        received = (BASE + timedelta(seconds=index + 2)).isoformat()
        rows.append({
            "schema_version": 3,
            "durability_version": "raw-segment-durability-v2",
            "run_id": run_id,
            "segment_sequence": 1,
            "record_sequence": index,
            "venue_id": "bitbank",
            "venue_symbol": symbol,
            "domain": "book_l2",
            "endpoint_id": "EP-0005",
            "endpoint_revision": 1,
            "connection_id": f"{run_id}-c000001",
            "channel_id": channel,
            "source": "websocket",
            "source_endpoint": (
                "depth_whole/depth_diff/circuit_break_info"
            ),
            "payload_raw": payload,
            "raw_payload_sha256": hashlib.sha256(
                payload.encode("utf-8")
            ).hexdigest(),
            "recv_ts_utc": received,
            "recv_ts_mono_ns": 20_000_000 + index,
            "ingest_time": received,
        })
    path.write_text(
        "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8"
    )
    sha = hashlib.sha256(path.read_bytes()).hexdigest()
    manifest = {
        "schema_version": 3,
        "status": "sealed",
        "completion_claim": True,
        "artifact_id": f"sha256-{sha}",
        "sha256": sha,
        "byte_count": path.stat().st_size,
        "record_count": len(rows),
        "run_id": run_id,
        "segment_sequence": 1,
        "venue_id": "bitbank",
        "venue_symbol": symbol,
        "domain": "book_l2",
        "endpoint_id": "EP-0005",
        "endpoint_revision": 1,
        "storage_path": path.relative_to(root).as_posix(),
    }
    path.with_name("segment-000001.manifest.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )
    return path, len(rows)


def test_bitbank_r1_status_is_independent_and_lineage_complete(
    tmp_path: Path,
) -> None:
    _, source_rows = _write_r1_segment(tmp_path)
    conn = store.connect(tmp_path)
    try:
        inputs = _sealed_inputs(tmp_path)
        assert [(item.endpoint_id, item.endpoint_revision) for item in inputs] == [
            ("EP-0005", 1)
        ]
        l2 = materialize_l2_segment(tmp_path, conn, inputs[0])
        assert (l2.frame_rows, l2.ignored_rows, l2.rejected_rows) == (1, 2, 0)
        ignore_reasons = {
            str(row[0]) for row in conn.execute(
                "SELECT reason FROM materialization_ignore "
                "WHERE attempt_id=?", (l2.attempt_id,)
            )
        }
        assert ignore_reasons == {"protocol_control_frame", "market_status_frame"}

        selected = sealed_status_inputs(tmp_path, conn)
        assert len(selected) == 1
        selected_again = sealed_status_inputs(tmp_path, conn)
        assert len(selected_again) == 1
        assert conn.execute(
            "SELECT COUNT(*),source_rows,candidate_rows "
            "FROM market_status_input_scan"
        ).fetchone() == (1, source_rows, 1)

        [status] = materialize_all(tmp_path, conn)
        assert (
            status.source_rows, status.observation_rows,
            status.ignored_rows, status.rejected_rows,
        ) == (source_rows, 1, 2, 0)
        assert status.source_rows == (
            status.observation_rows + status.ignored_rows + status.rejected_rows
        )
        assert materialize_all(tmp_path, conn)[0].reused is True
        assert audit_market_status(tmp_path, conn)["ok"] is True
        control = conn.execute(
            "SELECT cc.endpoint_id,cc.endpoint_revision,ch.channel_id,"
            "ch.capability_domain,ch.capability_endpoint "
            "FROM collection_connection cc JOIN collection_channel ch "
            "ON ch.connection_id=cc.connection_id "
            "WHERE ch.channel_id='circuit_break_info_btc_jpy'"
        ).fetchone()
        assert control == (
            "EP-0005", 1, "circuit_break_info_btc_jpy",
            "market_status", "circuit_break_info",
        )
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        conn.close()

    db = duckdb.connect(":memory:")
    try:
        fact = db.execute(
            "SELECT mode,fee_type,estimated_auction_price,"
            "estimated_auction_amount,auction_upper_price,"
            "auction_lower_price,reopen_time IS NOT NULL,"
            "endpoint_revision,channel_id,available_time>=event_time "
            "FROM read_parquet(?)",
            [str(tmp_path / status.output_path)],
        ).fetchone()
    finally:
        db.close()
    assert fact == (
        "CIRCUIT_BREAK", "SELL_MAKER", "100.5", "3.25", "110", "90",
        True, 1, "circuit_break_info_btc_jpy", True,
    )


def test_market_status_watcher_failure_is_nonblocking(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import guvolu.data.market_status_materialize as status_module

    def fail(*_args: object, **_kwargs: object) -> list[object]:
        raise RuntimeError("status probe failed")

    monkeypatch.setattr(status_module, "materialize_all", fail)
    conn = store.connect(tmp_path)
    try:
        summary, error = _refresh_market_status_nonblocking(tmp_path, conn)
        assert conn.execute("SELECT 1").fetchone() == (1,)
    finally:
        conn.close()
    assert summary is None
    assert isinstance(error, RuntimeError)
    assert str(error) == "status probe failed"


def test_r0_registry_remains_immutable_and_has_no_status_scan(
    tmp_path: Path,
) -> None:
    conn = store.connect(tmp_path)
    try:
        from guvolu.venues.registry import register_all

        register_all(conn)
        revisions = conn.execute(
            "SELECT revision_id,scope,source_schema_revision,effective_from "
            "FROM endpoint_revision WHERE endpoint_id='EP-0005' "
            "ORDER BY revision_id"
        ).fetchall()
        assert revisions == [
            (
                0, "depth_whole/depth_diff",
                "socket.io-eio4-depth-schema@2026-08-12",
                "2026-08-12T00:00:00+00:00",
            ),
            (
                1, "depth_whole/depth_diff/circuit_break_info",
                "local_registry_extension:circuit_break_info@2026-08-12",
                "2026-08-12T14:49:04+00:00",
            ),
        ]
        assert conn.execute(
            "SELECT COUNT(*) FROM market_status_input_scan"
        ).fetchone() == (0,)
    finally:
        conn.close()
