from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Sequence
from datetime import timedelta
from pathlib import Path

import duckdb
import pytest

from guvolu.data import store
from guvolu.data.okx_l2_live_capture import (
    CaptureStats,
    OKX_ENDPOINT_ID,
    OKX_RAW_DOMAIN,
    _consume_connection,
    _opened_connection,
    record_books,
)
from guvolu.data.okx_l2_live_materialize import (
    L2_NORMALIZATION_VERSION,
    _checkpoint_contract_artifact,
    audit_live_l2,
    materialize_all,
    materialize_segment,
    sealed_inputs,
)
from guvolu.data.okx_l2_terminal_checkpoint import (
    TERMINAL_CHECKPOINT_DATASET,
    TerminalCheckpointError,
    load_latest_terminal_checkpoint,
    load_terminal_checkpoint_for_attempt,
)
from guvolu.data.segmented_raw import SegmentedRawWriter
from guvolu.venues import registry


def test_terminal_contract_identity_binds_mapping_and_capability(
    tmp_path: Path,
) -> None:
    conn = store.connect(tmp_path)
    try:
        first = _checkpoint_contract_artifact(
            tmp_path, conn, mapping_revision=0, capability_revision=1,
        )
        second = _checkpoint_contract_artifact(
            tmp_path, conn, mapping_revision=1, capability_revision=1,
        )
        third = _checkpoint_contract_artifact(
            tmp_path, conn, mapping_revision=0, capability_revision=2,
        )
    finally:
        conn.close()
    assert len({first.artifact_id, second.artifact_id, third.artifact_id}) == 3
    assert len({first.storage_path, second.storage_path, third.storage_path}) == 3


def _payload(
    action: str,
    sequence: int,
    previous: int,
    timestamp: int,
    *,
    asks: Sequence[Sequence[str]],
    bids: Sequence[Sequence[str]],
) -> str:
    return json.dumps({
        "arg": {"channel": "books", "instId": "BTC-USDT"},
        "action": action,
        "data": [{
            "asks": list(asks),
            "bids": list(bids),
            "ts": str(timestamp),
            "checksum": 0,
            "seqId": sequence,
            "prevSeqId": previous,
        }],
    }, separators=(",", ":"))


def _writer(root: Path, run_id: str) -> SegmentedRawWriter:
    return SegmentedRawWriter(
        root,
        "okx",
        "BTC-USDT",
        domain=OKX_RAW_DOMAIN,
        run_id=run_id,
        endpoint_id=OKX_ENDPOINT_ID,
        endpoint_revision=0,
        segment_seconds=3600,
        segment_max_bytes=1024 * 1024,
    )


def _write(
    writer: SegmentedRawWriter,
    payload: str,
    connection_id: str,
    channel_id: str = "books:BTC-USDT",
) -> None:
    writer.write_frame(
        payload,
        "books",
        connection_id=connection_id,
        channel_id=channel_id,
    )


def test_okx_live_two_segment_materialization_and_audit(tmp_path: Path) -> None:
    writer = _writer(tmp_path, "run-okx-live")
    connection = "run-okx-live-c000001"
    _write(
        writer,
        '{"event":"subscribe","arg":{"channel":"books",'
        '"instId":"BTC-USDT"}}',
        connection,
    )
    _write(
        writer,
        _payload(
            "snapshot", 100, -1, 1_786_400_000_000,
            asks=[["101", "2", "0", "2"]],
            bids=[["100", "3", "0", "3"]],
        ),
        connection,
    )
    writer.seal_segment()
    _write(
        writer,
        _payload(
            "update", 101, 100, 1_786_400_000_100,
            asks=[["102", "1", "0", "1"]],
            bids=[["100", "0", "0", "0"]],
        ),
        connection,
    )
    _write(
        writer,
        _payload(
            "update", 101, 101, 1_786_400_000_200,
            asks=[], bids=[],
        ),
        connection,
    )
    writer.finish()

    conn = store.connect(tmp_path)
    try:
        results = materialize_all(tmp_path, conn)
        audit = audit_live_l2(tmp_path, conn)
        endpoints = conn.execute(
            "SELECT legal_entity,venue_brand,product,environment,region,"
            "transport,protocol,auth_mode,host,port,base_path_or_channel,"
            "data_level,scope,documentation_sha256 FROM endpoint_revision "
            "WHERE endpoint_id='EP-0032' AND revision_id=0"
        ).fetchone()
        capabilities = conn.execute(
            "SELECT evidence_level,implementation_status FROM "
            "venue_capability_revision WHERE venue_id='okx' "
            "AND domain='book_realtime' AND endpoint='books' "
            "ORDER BY revision_id"
        ).fetchall()
        second_inputs = conn.execute(
            "SELECT COUNT(*),SUM(source_rows),SUM(normalized_rows),"
            "SUM(ignored_rows),SUM(rejected_rows) FROM partition_input "
            "WHERE attempt_id=?",
            (results[1].attempt_id,),
        ).fetchone()
        dependencies = conn.execute(
            "SELECT COUNT(*) FROM materialization_dependency "
            "WHERE attempt_id=? AND binding_basis='explicit-replay'",
            (results[1].attempt_id,),
        ).fetchone()
    finally:
        conn.close()

    assert len(results) == 2
    assert [(row.source_rows, row.frame_rows, row.ignored_rows) for row in results] == [
        (2, 1, 1), (2, 2, 0),
    ]
    assert all(row.partition_key.startswith("live/run-okx-live/") for row in results)
    assert audit == {
        "ok": True, "attempts": 2, "frames": 3, "levels": 4, "errors": [],
    }
    assert endpoints == (
        "OKX", "OKX", "All", "prod", "global", "WSS", "v5 public",
        "P0/P3", "ws.okx.com", 8443, "/ws/v5/public", "L2",
        "market data", None,
    )
    assert capabilities == [("documented", "planned"), ("measured", "implemented")]
    # 原始、契约与前段终态。
    assert second_inputs == (3, 2, 2, 0, 0)
    assert dependencies == (1,)

    db = duckdb.connect(":memory:")
    try:
        rows = db.execute(
            "SELECT sequence_id,prev_sequence_id,checksum,integrity_mode,"
            "data_quality FROM read_parquet(?, union_by_name=true) "
            "ORDER BY segment_sequence,source_row_index",
            [[str(tmp_path / result.frame_path) for result in results]],
        ).fetchall()
        levels = db.execute(
            "SELECT price,size,order_count,action FROM read_parquet(?, union_by_name=true) "
            "ORDER BY frame_id,side,source_level_index",
            [[str(tmp_path / result.level_path) for result in results]],
        ).fetchall()
    finally:
        db.close()
    assert [row[:3] for row in rows] == [
        ("100", "-1", None), ("101", "100", None), ("101", "101", None),
    ]
    assert all("checksum_unsupported" in row[3] for row in rows)
    assert "empty_update_heartbeat" in json.loads(rows[-1][4])
    assert ("100", "0", 0, "delete") in levels


def test_pre_snapshot_delta_is_preserved_and_flagged(tmp_path: Path) -> None:
    writer = _writer(tmp_path, "run-okx-pre-snapshot")
    connection = "run-okx-pre-snapshot-c000001"
    _write(
        writer,
        _payload(
            "update", 201, 200, 1_786_400_100_000,
            asks=[["102", "1", "0", "1"]], bids=[],
        ),
        connection,
    )
    _write(
        writer,
        _payload(
            "snapshot", 300, -1, 1_786_400_100_100,
            asks=[["101", "1", "0", "1"]],
            bids=[["100", "1", "0", "1"]],
        ),
        connection,
    )
    writer.finish()
    conn = store.connect(tmp_path)
    try:
        result = materialize_all(tmp_path, conn)[0]
        audit = audit_live_l2(tmp_path, conn)
    finally:
        conn.close()
    assert (result.frame_rows, result.rejected_rows) == (2, 0)
    assert audit["ok"] is True
    db = duckdb.connect(":memory:")
    try:
        quality = db.execute(
            "SELECT data_quality FROM read_parquet(?) "
            "ORDER BY source_row_index LIMIT 1",
            [str(tmp_path / result.frame_path)],
        ).fetchone()
    finally:
        db.close()
    assert quality is not None
    assert {
        "connection_sequence_anchor_missing",
        "delta_before_connection_snapshot",
    } <= set(json.loads(quality[0]))


def test_terminal_book_checkpoint_cross_segment_and_idempotent(
    tmp_path: Path,
) -> None:
    writer = _writer(tmp_path, "run-okx-terminal")
    connection = "run-okx-terminal-c000001"
    _write(writer, _payload(
        "snapshot", 10, -1, 1_786_400_150_000,
        asks=[["101", "1", "0", "1"], ["102", "2", "0", "2"]],
        bids=[["100", "3", "0", "3"], ["99", "4", "0", "4"]],
    ), connection)
    writer.seal_segment()
    _write(writer, _payload(
        "update", 11, 10, 1_786_400_150_100,
        asks=[["101", "5", "0", "5"], ["103", "1", "0", "1"]],
        bids=[["100", "0", "0", "0"]],
    ), connection)
    writer.finish()
    inputs = sealed_inputs(tmp_path)
    conn = store.connect(tmp_path)
    try:
        first = materialize_all(tmp_path, conn)
        again = materialize_all(tmp_path, conn)
        one = load_terminal_checkpoint_for_attempt(
            tmp_path, conn, first[0].attempt_id, require_trusted=True,
        )
        two = load_terminal_checkpoint_for_attempt(
            tmp_path, conn, first[1].attempt_id, require_trusted=True,
        )
        output_count = conn.execute(
            "SELECT COUNT(*) FROM materialization_output WHERE dataset=?",
            (TERMINAL_CHECKPOINT_DATASET,),
        ).fetchone()
    finally:
        conn.close()

    assert [row.attempt_id for row in again] == [row.attempt_id for row in first]
    assert all(row.reused for row in again)
    assert output_count == (2,)
    assert one.checkpoint.bids[0].price == "100"
    assert [(row.price, row.size, row.order_count) for row in two.checkpoint.asks] == [
        ("101", "5", 5), ("102", "2", 2), ("103", "1", 1),
    ]
    assert [(row.price, row.size, row.order_count) for row in two.checkpoint.bids] == [
        ("99", "4", 4),
    ]
    assert two.checkpoint.connection_id == connection
    assert two.checkpoint.sequence_id == 11
    assert two.checkpoint.as_of_frame_id is not None
    assert two.checkpoint.as_of_event_time is not None
    assert two.checkpoint.as_of_available_time is not None
    assert two.checkpoint.state_sha256.startswith("sha256-")
    assert two.checkpoint.source_attempt_id == first[1].attempt_id
    assert two.checkpoint.source_artifact_id == inputs[1].artifact.artifact_id
    assert two.checkpoint.upstream_attempt_id == first[0].attempt_id
    assert two.checkpoint.upstream_checkpoint_artifact_id == one.artifact_id


def test_new_connection_delta_does_not_inherit_old_terminal_book(
    tmp_path: Path,
) -> None:
    writer = _writer(tmp_path, "run-okx-reconnect")
    first_connection = "run-okx-reconnect-c000001"
    second_connection = "run-okx-reconnect-c000002"
    _write(writer, _payload(
        "snapshot", 20, -1, 1_786_400_160_000,
        asks=[["101", "1", "0", "1"]],
        bids=[["100", "1", "0", "1"]],
    ), first_connection)
    writer.seal_segment()
    _write(writer, _payload(
        "update", 31, 30, 1_786_400_160_100,
        asks=[["102", "1", "0", "1"]], bids=[],
    ), second_connection)
    writer.finish()
    conn = store.connect(tmp_path)
    try:
        results = materialize_all(tmp_path, conn)
        prior = load_terminal_checkpoint_for_attempt(
            tmp_path, conn, results[0].attempt_id, require_trusted=True,
        )
        current = load_terminal_checkpoint_for_attempt(
            tmp_path, conn, results[1].attempt_id,
        )
        with pytest.raises(TerminalCheckpointError, match="untrusted"):
            load_terminal_checkpoint_for_attempt(
                tmp_path, conn, results[1].attempt_id, require_trusted=True,
            )
    finally:
        conn.close()

    assert prior.checkpoint.trusted is True
    assert current.checkpoint.connection_id == second_connection
    assert current.checkpoint.trusted is False
    assert current.checkpoint.trust_reason == "connection_without_snapshot"
    assert current.checkpoint.asks == () and current.checkpoint.bids == ()


def test_terminal_checkpoint_tamper_blocks_next_segment(tmp_path: Path) -> None:
    writer = _writer(tmp_path, "run-okx-tamper")
    connection = "run-okx-tamper-c000001"
    _write(writer, _payload(
        "snapshot", 40, -1, 1_786_400_170_000,
        asks=[["101", "1", "0", "1"]],
        bids=[["100", "1", "0", "1"]],
    ), connection)
    writer.seal_segment()
    _write(writer, _payload(
        "update", 41, 40, 1_786_400_170_100,
        asks=[["101", "2", "0", "2"]], bids=[],
    ), connection)
    writer.finish()
    inputs = sealed_inputs(tmp_path)
    conn = store.connect(tmp_path)
    try:
        first = materialize_segment(tmp_path, conn, inputs[0])
        loaded = load_terminal_checkpoint_for_attempt(
            tmp_path, conn, first.attempt_id,
        )
        path = tmp_path / loaded.storage_path
        path.write_bytes(path.read_bytes() + b"tamper")
        with pytest.raises(ValueError, match="checkpoint.*(?:SHA|byte count)"):
            materialize_segment(tmp_path, conn, inputs[1])
    finally:
        conn.close()


def test_terminal_checkpoint_enforces_pit_visibility(tmp_path: Path) -> None:
    writer = _writer(tmp_path, "run-okx-pit")
    connection = "run-okx-pit-c000001"
    _write(writer, _payload(
        "snapshot", 50, -1, 1_786_400_180_000,
        asks=[["101", "1", "0", "1"]],
        bids=[["100", "1", "0", "1"]],
    ), connection)
    writer.finish()
    conn = store.connect(tmp_path)
    try:
        result = materialize_all(tmp_path, conn)[0]
        loaded = load_terminal_checkpoint_for_attempt(
            tmp_path, conn, result.attempt_id,
        )
        with pytest.raises(TerminalCheckpointError, match="PIT-visible"):
            load_terminal_checkpoint_for_attempt(
                tmp_path, conn, result.attempt_id,
                decision_time=(
                    loaded.checkpoint.checkpoint_available_time
                    - timedelta(microseconds=1)
                ),
            )
        visible = load_terminal_checkpoint_for_attempt(
            tmp_path, conn, result.attempt_id,
            decision_time=loaded.checkpoint.checkpoint_available_time,
            require_trusted=True,
        )
        latest = load_latest_terminal_checkpoint(
            tmp_path, conn, result.market_id,
            decision_time=loaded.checkpoint.checkpoint_available_time,
            require_trusted=True,
        )
    finally:
        conn.close()
    assert visible.artifact_id == loaded.artifact_id
    assert latest is not None
    assert latest.artifact_id == loaded.artifact_id
    assert latest.checkpoint.as_book_state()["state_source"] == (
        TERMINAL_CHECKPOINT_DATASET
    )


def test_sequence_mismatch_is_rejected_and_audit_fails(tmp_path: Path) -> None:
    writer = _writer(tmp_path, "run-okx-gap")
    connection = "run-okx-gap-c000001"
    _write(
        writer,
        _payload(
            "snapshot", 100, -1, 1_786_400_200_000,
            asks=[["101", "1", "0", "1"]],
            bids=[["100", "1", "0", "1"]],
        ),
        connection,
    )
    _write(
        writer,
        _payload(
            "update", 102, 99, 1_786_400_200_100,
            asks=[["102", "1", "0", "1"]], bids=[],
        ),
        connection,
    )
    writer.finish()
    conn = store.connect(tmp_path)
    try:
        result = materialize_all(tmp_path, conn)[0]
        audit = audit_live_l2(tmp_path, conn)
    finally:
        conn.close()
    assert (
        result.source_rows,
        result.frame_rows,
        result.ignored_rows,
        result.rejected_rows,
    ) == (2, 1, 0, 1)
    assert result.status == "complete_with_rejections"
    assert audit["ok"] is False
    assert any("rejected raw rows" in error for error in audit["errors"])


class _FakeConnection:
    def __init__(self, responses: list[str]) -> None:
        self.responses = responses
        self.sent: list[str] = []

    async def send(self, message: str) -> None:
        self.sent.append(message)

    async def recv(self) -> str | bytes:
        if self.responses:
            return self.responses.pop(0)
        await asyncio.sleep(1)
        return "pong"


def test_capture_session_records_ack_and_data_without_network(tmp_path: Path) -> None:
    writer = _writer(tmp_path, "run-okx-capture")
    stats = CaptureStats("BTC-USDT")
    connection_id = _opened_connection(writer, stats)
    fake = _FakeConnection([
        '{"event":"subscribe","arg":{"channel":"books",'
        '"instId":"BTC-USDT"}}',
        _payload(
            "snapshot", 10, -1, 1_786_400_300_000,
            asks=[["101", "1", "0", "1"]],
            bids=[["100", "1", "0", "1"]],
        ),
    ])
    asyncio.run(_consume_connection(
        fake, writer, stats, connection_id, time.monotonic() + 0.02
    ))
    manifest = writer.finish()
    run = json.loads(manifest.read_text(encoding="utf-8"))
    assert json.loads(fake.sent[0]) == {
        "op": "subscribe",
        "args": [{"channel": "books", "instId": "BTC-USDT"}],
    }
    assert (stats.wire_frames, stats.data_frames, stats.control_frames) == (2, 1, 1)
    assert run["record_count"] == 2
    assert len(sealed_inputs(tmp_path)) == 1


@pytest.mark.parametrize("minutes", (0.0, 15.1))
def test_capture_rejects_resident_or_long_run(
    tmp_path: Path, minutes: float,
) -> None:
    with pytest.raises(ValueError, match="尚未开放常驻采集"):
        asyncio.run(record_books(tmp_path, minutes=minutes))


def test_registry_can_be_replayed_idempotently(tmp_path: Path) -> None:
    conn = store.connect(tmp_path)
    try:
        registry.register_all(conn)
        registry.register_all(conn)
        assert conn.execute(
            "SELECT COUNT(*) FROM endpoint_revision "
            "WHERE endpoint_id='EP-0032' AND revision_id=0"
        ).fetchone() == (1,)
    finally:
        conn.close()

    assert L2_NORMALIZATION_VERSION == "book-l2-normalization-v5"
