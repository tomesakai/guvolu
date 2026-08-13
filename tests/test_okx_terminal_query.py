"""OKX 终态基座接入查询与 book_state 的精准回归。"""
from __future__ import annotations

import json
import time
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest

from guvolu.data import store
from guvolu.data.book_state_materialize import (
    audit as audit_book_state,
    materialize_all as materialize_book_state,
)
from guvolu.data.okx_l2_live_capture import OKX_ENDPOINT_ID, OKX_RAW_DOMAIN
from guvolu.data.okx_l2_live_materialize import materialize_all
from guvolu.data.okx_l2_terminal_checkpoint import (
    TERMINAL_CHECKPOINT_DATASET,
    load_terminal_checkpoint_for_attempt,
)
from guvolu.data.segmented_raw import SegmentedRawWriter
from guvolu.ui import materialized_query as materialized_query_module
from guvolu.ui.materialized_query import MaterializedQuery, MaterializedQueryError


def _payload(
    action: str,
    sequence: int,
    previous: int,
    *,
    asks: Sequence[Sequence[str]],
    bids: Sequence[Sequence[str]],
) -> str:
    return json.dumps({
        "arg": {"channel": "books", "instId": "BTC-USDT"},
        "action": action,
        "data": [{
            "asks": list(asks), "bids": list(bids),
            "ts": "1786400000000", "checksum": 0,
            "seqId": sequence, "prevSeqId": previous,
        }],
    }, separators=(",", ":"))


def _writer(root: Path, run_id: str) -> SegmentedRawWriter:
    return SegmentedRawWriter(
        root, "okx", "BTC-USDT", domain=OKX_RAW_DOMAIN,
        run_id=run_id, endpoint_id=OKX_ENDPOINT_ID, endpoint_revision=0,
        segment_seconds=3600, segment_max_bytes=1024 * 1024,
    )


def _write(
    writer: SegmentedRawWriter,
    payload: str,
    connection_id: str,
) -> None:
    writer.write_frame(
        payload, "books", connection_id=connection_id,
        channel_id="books:BTC-USDT",
    )


def test_terminal_base_survives_more_than_twelve_segments_and_binds_book_state(
    tmp_path: Path,
) -> None:
    writer = _writer(tmp_path, "run-query-many")
    connection = "run-query-many-c000001"
    _write(writer, _payload(
        "snapshot", 1, -1,
        asks=[["101", "1", "0", "1"]],
        bids=[["100", "1", "0", "1"]],
    ), connection)
    writer.seal_segment()
    for sequence in range(2, 15):
        _write(writer, _payload(
            "update", sequence, sequence - 1,
            asks=[["101", str(sequence), "0", str(sequence)]], bids=[],
        ), connection)
        if sequence < 14:
            writer.seal_segment()
    writer.finish()

    conn = store.connect(tmp_path)
    try:
        results = materialize_all(tmp_path, conn)
        terminal_row = conn.execute(
            "SELECT artifact_id FROM materialization_output "
            "WHERE attempt_id=? AND dataset=?",
            (results[-1].attempt_id, TERMINAL_CHECKPOINT_DATASET),
        ).fetchone()
        assert terminal_row is not None
        terminal_artifact = str(terminal_row[0])
        checkpoint = materialize_book_state(tmp_path, conn)[0]
        dependencies = {
            str(row[0]) for row in conn.execute(
                "SELECT upstream_attempt_id FROM materialization_dependency "
                "WHERE attempt_id=?", (checkpoint.attempt_id,),
            )
        }
        inputs = {
            str(row[0]) for row in conn.execute(
                "SELECT artifact_id FROM partition_input WHERE attempt_id=?",
                (checkpoint.attempt_id,),
            )
        }
        manifest_registered = conn.execute(
            "SELECT COUNT(*) FROM artifact_location l JOIN artifact a "
            "ON a.artifact_id=l.artifact_id WHERE a.artifact_kind="
            "'materialization_manifest' AND l.storage_path LIKE ?",
            (f"%/manifest-{checkpoint.attempt_id}.json",),
        ).fetchone()[0]
        audit = audit_book_state(tmp_path, conn)
    finally:
        conn.close()
    book, _ = MaterializedQuery(tmp_path).latest_l2(
        results[-1].market_id, 5, decision_time=datetime.now(UTC),
    )

    assert len(results) == 14
    assert book["asks"][0]["size"] == "14"
    assert book["meta"]["state_source"] == TERMINAL_CHECKPOINT_DATASET
    assert book["meta"]["source_attempt_ids"] == [results[-1].attempt_id]
    assert book["meta"]["source_artifact_ids"] == [terminal_artifact]
    assert dependencies == {results[-1].attempt_id}
    assert inputs == {terminal_artifact}
    assert manifest_registered == 1
    assert audit["ok"] is True


def test_terminal_base_excludes_dozen_prior_frames_in_same_segment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    writer = _writer(tmp_path, "run-query-dense")
    connection = "run-query-dense-c000001"
    _write(writer, _payload(
        "snapshot", 1, -1,
        asks=[["101", "1", "0", "1"]],
        bids=[["100", "1", "0", "1"]],
    ), connection)
    for sequence in range(2, 65):
        _write(writer, _payload(
            "update", sequence, sequence - 1,
            asks=[["101", str(sequence), "0", str(sequence)]], bids=[],
        ), connection)
    writer.finish()

    conn = store.connect(tmp_path)
    try:
        result = materialize_all(tmp_path, conn)[0]
        loaded = load_terminal_checkpoint_for_attempt(
            tmp_path, conn, result.attempt_id, require_trusted=True,
        )
    finally:
        conn.close()
    monkeypatch.setattr(
        materialized_query_module, "MAX_L2_REPLAY_FRAMES", 12,
    )

    book, _ = MaterializedQuery(tmp_path).latest_l2(
        result.market_id, 5, decision_time=datetime.now(UTC),
    )
    assert book["asks"][0]["size"] == "64"
    assert book["meta"]["replay_frames"] == 0
    assert book["meta"]["state_source"] == TERMINAL_CHECKPOINT_DATASET
    assert book["meta"]["source_attempt_ids"] == [result.attempt_id]
    assert book["meta"]["source_artifact_ids"] == [loaded.artifact_id]


def test_pit_future_terminal_uses_prior_base_and_exact_tail(tmp_path: Path) -> None:
    writer = _writer(tmp_path, "run-query-pit")
    connection = "run-query-pit-c000001"
    _write(writer, _payload(
        "snapshot", 10, -1,
        asks=[["101", "1", "0", "1"]],
        bids=[["100", "1", "0", "1"]],
    ), connection)
    writer.seal_segment()
    _write(writer, _payload(
        "update", 11, 10,
        asks=[["101", "2", "0", "2"]], bids=[],
    ), connection)
    time.sleep(0.002)
    _write(
        writer,
        '{"event":"subscribe","arg":{"channel":"books",'
        '"instId":"BTC-USDT"}}',
        connection,
    )
    writer.finish()

    conn = store.connect(tmp_path)
    try:
        results = materialize_all(tmp_path, conn)
        first = load_terminal_checkpoint_for_attempt(
            tmp_path, conn, results[0].attempt_id,
        )
        second = load_terminal_checkpoint_for_attempt(
            tmp_path, conn, results[1].attempt_id,
        )
        tail_artifacts = {
            str(row[0]) for row in conn.execute(
                "SELECT artifact_id FROM materialization_output "
                "WHERE attempt_id=? AND dataset IN "
                "('book_l2_frame','book_l2_level')",
                (results[1].attempt_id,),
            )
        }
        frame_row = conn.execute(
            "SELECT a.storage_path FROM materialization_output o "
            "JOIN artifact a ON a.artifact_id=o.artifact_id "
            "WHERE o.attempt_id=? AND o.dataset='book_l2_frame'",
            (results[1].attempt_id,),
        ).fetchone()
        assert frame_row is not None
        frame_path = str(frame_row[0])
    finally:
        conn.close()
    db = duckdb.connect(":memory:")
    try:
        decision_row = db.execute(
            "SELECT available_time FROM read_parquet(?) LIMIT 1",
            [str(tmp_path / str(frame_path))],
        ).fetchone()
        assert decision_row is not None
        decision = decision_row[0]
    finally:
        db.close()
    assert first.checkpoint.checkpoint_available_time <= decision
    assert decision < second.checkpoint.checkpoint_available_time

    book, _ = MaterializedQuery(tmp_path).latest_l2(
        results[-1].market_id, 5, decision_time=decision,
    )
    assert book["asks"][0]["size"] == "2"
    assert book["meta"]["state_attempt_id"] == results[0].attempt_id
    assert book["meta"]["state_source"] == (
        TERMINAL_CHECKPOINT_DATASET + "_plus_tail"
    )
    assert set(book["meta"]["source_attempt_ids"]) == {
        results[0].attempt_id, results[1].attempt_id,
    }
    assert set(book["meta"]["source_artifact_ids"]) == {
        first.artifact_id, *tail_artifacts,
    }


def test_connection_switch_without_snapshot_never_inherits_old_book(
    tmp_path: Path,
) -> None:
    writer = _writer(tmp_path, "run-query-switch")
    _write(writer, _payload(
        "snapshot", 20, -1,
        asks=[["101", "1", "0", "1"]],
        bids=[["100", "1", "0", "1"]],
    ), "run-query-switch-c000001")
    writer.seal_segment()
    _write(writer, _payload(
        "update", 31, 30,
        asks=[["102", "1", "0", "1"]], bids=[],
    ), "run-query-switch-c000002")
    writer.finish()
    conn = store.connect(tmp_path)
    try:
        market_id = materialize_all(tmp_path, conn)[-1].market_id
    finally:
        conn.close()

    with pytest.raises(MaterializedQueryError):
        MaterializedQuery(tmp_path).latest_l2(
            market_id, 5, decision_time=datetime.now(UTC),
        )


def test_tampered_terminal_falls_back_to_complete_fact_replay(
    tmp_path: Path,
) -> None:
    writer = _writer(tmp_path, "run-query-tamper")
    connection = "run-query-tamper-c000001"
    _write(writer, _payload(
        "snapshot", 40, -1,
        asks=[["101", "1", "0", "1"]],
        bids=[["100", "1", "0", "1"]],
    ), connection)
    writer.seal_segment()
    _write(writer, _payload(
        "update", 41, 40,
        asks=[["101", "3", "0", "3"]], bids=[],
    ), connection)
    writer.finish()
    conn = store.connect(tmp_path)
    try:
        results = materialize_all(tmp_path, conn)
        loaded = load_terminal_checkpoint_for_attempt(
            tmp_path, conn, results[-1].attempt_id,
        )
    finally:
        conn.close()
    path = tmp_path / loaded.storage_path
    path.write_bytes(path.read_bytes() + b"tamper")

    book, _ = MaterializedQuery(tmp_path).latest_l2(
        results[-1].market_id, 5, decision_time=datetime.now(UTC),
    )
    assert book["asks"][0]["size"] == "3"
    assert book["meta"]["state_source"] == (
        "l2_wire_order_snapshot_delta_replay"
    )
    assert book["meta"]["state_artifact_id"] is None
