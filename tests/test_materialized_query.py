"""活动 head 成品查询的最小跨域契约测试。"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import duckdb
import pytest
from fastapi.testclient import TestClient

from guvolu.data import store
from guvolu.data.materialize import audit_materializations, ensure_markets
from guvolu.data.book_state_materialize import (
    audit as audit_checkpoints,
    main as checkpoint_main,
    materialize_all as materialize_checkpoints,
)
from guvolu.data.orderflow_tile_materialize import (
    DATASET_CELL,
    DATASET_COLUMN,
    METHOD_VERSION as TILE_METHOD_VERSION,
    TileBuild,
    _write_outputs,
    audit as audit_tiles,
    materialize_hour,
    recent_l2_markets,
)
from guvolu.domain.config import load_config
from guvolu.ui.materialized_query import MaterializedQuery, replay_l2_snapshot
from guvolu.ui.query_catalog import ActiveOutput, ActiveOutputSnapshot
from guvolu.ui.query_service import create_app
from guvolu.venues import registry


def _parquet(path: Path, sql: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    db = duckdb.connect(":memory:")
    try:
        db.execute("SET TimeZone='UTC'")
        db.execute(
            f"COPY ({sql}) TO '{path.as_posix()}' (FORMAT PARQUET)"
        )
    finally:
        db.close()


def _register_output(
    conn: object, root: Path, attempt: str, market_id: str, domain: str,
    dataset_paths: list[tuple[str, Path, int]], low: str, high: str,
    *, partition_key: str = "p1",
) -> None:
    connection = conn
    connection.execute(
        "INSERT INTO partition_attempt "
        "(attempt_id,market_id,domain,partition_key,normalization_version,"
        "input_set_hash,status,source_rows,normalized_rows,ignored_rows,"
        "rejected_rows,started_at,finished_at,code_version,config_hash) "
        "VALUES (?,?,?,?,?,?,'complete',1,1,0,0,?,?, 'test','cfg')",
        (attempt, market_id, domain, partition_key, f"{domain}-v1",
         f"input-{attempt}", low, high),
    )
    for index, (dataset, path, rows) in enumerate(dataset_paths):
        digest = hashlib.sha256(f"{attempt}:{dataset}:{index}".encode()).hexdigest()
        artifact_id = "sha256-" + digest
        relative = path.relative_to(root).as_posix()
        connection.execute(
            "INSERT INTO artifact VALUES "
            "(?, 'materialized_parquet', ?, ?, 1, ?, ?, 'sha256-file-v1', ?)",
            (artifact_id, relative, digest, high, high, path.stat().st_size),
        )
        connection.execute(
            "INSERT OR IGNORE INTO artifact_location VALUES (?,?,?,1)",
            (artifact_id, relative, high),
        )
        connection.execute(
            "INSERT INTO materialization_output VALUES (?,?,?,?,?,?,?)",
            (attempt, artifact_id, dataset, rows, low, high, high),
        )
    connection.execute(
        "INSERT INTO materialization_partition_head VALUES (?,?,?,?,?,?)",
        (market_id, domain, partition_key, f"{domain}-v1", attempt, high),
    )


def _fixture(root: Path) -> str:
    market_id = "mkt__gmo__btc__r0"
    kline = root / "materialized/kline.parquet"
    trade = root / "materialized/trade.parquet"
    frame = root / "materialized/frame.parquet"
    level = root / "materialized/level.parquet"
    _parquet(kline, """
      SELECT 'k1' kline_id,'mkt__gmo__btc__r0' market_id,'BTC' venue_symbol,
        '1hour' AS "interval",TIMESTAMPTZ '2026-08-01 00:00:00+00' open_time,
        TIMESTAMPTZ '2026-08-01 01:00:00+00' close_time,'venue' origin,
        'v1' value_revision,'100' AS "open",'110' AS "high",'90' AS "low",
        '105' AS "close",'3' AS "volume",
        TIMESTAMPTZ '2026-08-01 01:00:01+00' available_time,
        TIMESTAMPTZ '2026-08-01 01:00:01+00' first_seen_at,
        TIMESTAMPTZ '2026-08-01 01:00:01+00' last_seen_at,
        TIMESTAMPTZ '2026-08-01 01:00:01+00' closed_available_time,
        1 evidence_count,true is_closed,1 revision_ordinal,'native_sparse' gap_policy,
        'kline-v1' normalization_version,1 schema_version
    """)
    _parquet(trade, """
      SELECT * FROM (VALUES
        ('o1','gmo','BTC','mkt__gmo__btc__r0',0,'SPOT:BTC/JPY','t1',0,
         TIMESTAMPTZ '2026-08-01 00:01:00+00',TIMESTAMPTZ '2026-08-01 00:01:01+00',
         TIMESTAMPTZ '2026-08-01 00:01:01+00','buy','taker','100','1','match','native',
         NULL,NULL,NULL,'venue','a',0,'trade-v1',1),
        ('o2','gmo','BTC','mkt__gmo__btc__r0',0,'SPOT:BTC/JPY','t2',0,
         TIMESTAMPTZ '2026-08-01 00:02:00+00',TIMESTAMPTZ '2026-08-01 00:02:01+00',
         TIMESTAMPTZ '2026-08-01 00:02:01+00','sell','taker','110','2','match','native',
         NULL,NULL,NULL,'venue','a',1,'trade-v1',1)
      ) t(observation_id,venue_id,venue_symbol,market_id,mapping_revision,
        instrument_id,venue_trade_id,revision_id,event_time,available_time,
        ingest_time,side,source_side_basis,price,size,match_granularity,id_origin,
        sequence_id,first_trade_id,last_trade_id,time_origin,source_artifact_id,
        source_row_index,normalization_version,schema_version)
    """)
    _parquet(frame, """
      SELECT * FROM (VALUES
        ('f1','gmo','BTC','mkt__gmo__btc__r0',0,'SPOT:BTC/JPY','snapshot',
         TIMESTAMPTZ '2026-08-01 00:00:00+00',TIMESTAMPTZ '2026-08-01 00:00:01+00',
         TIMESTAMPTZ '2026-08-01 00:00:01+00','venue',NULL,NULL,'snapshot_no_sequence',
         1,1,2,'100',NULL,NULL,NULL,NULL,'run',1,'a',0,'l2-v1',1),
        ('f2','gmo','BTC','mkt__gmo__btc__r0',0,'SPOT:BTC/JPY','delta',
         TIMESTAMPTZ '2026-08-01 00:00:02+00',TIMESTAMPTZ '2026-08-01 00:00:03+00',
         TIMESTAMPTZ '2026-08-01 00:00:03+00','venue',NULL,NULL,'snapshot_no_sequence',
         1,1,2,'100',NULL,NULL,NULL,NULL,'run',1,'a',1,'l2-v1',1)
      ) f(frame_id,venue_id,venue_symbol,market_id,mapping_revision,instrument_id,
        message_kind,event_time,available_time,ingest_time,time_origin,sequence_id,
        checksum,integrity_mode,bid_levels,ask_levels,source_depth_levels,mid_price,
        ask_market_size,bid_market_size,asks_over,bids_under,run_id,segment_sequence,
        source_artifact_id,source_row_index,normalization_version,schema_version)
    """)
    _parquet(level, """
      SELECT * FROM (VALUES
        ('f1','mkt__gmo__btc__r0','ask',0,'101','2','set','limit','a',0,'l2-v1',1),
        ('f1','mkt__gmo__btc__r0','bid',0,'99','3','set','limit','a',0,'l2-v1',1),
        ('f2','mkt__gmo__btc__r0','ask',0,'101','0','delete','limit','a',1,'l2-v1',1),
        ('f2','mkt__gmo__btc__r0','ask',1,'102','4','set','limit','a',1,'l2-v1',1)
      ) l(frame_id,market_id,side,source_level_index,price,size,action,level_kind,
        source_artifact_id,source_row_index,normalization_version,schema_version)
    """)
    conn = store.connect(root)
    try:
        registry.register_all(conn)
        ensure_markets(conn)
        _register_output(conn, root, "k-attempt", market_id, "kline",
                         [("market_kline", kline, 1)],
                         "2026-08-01T00:00:00+00:00", "2026-08-01T01:00:00+00:00")
        _register_output(conn, root, "t-attempt", market_id, "trade",
                         [("trade_observation", trade, 2)],
                         "2026-08-01T00:01:00+00:00", "2026-08-01T00:02:00+00:00")
        _register_output(conn, root, "l-attempt", market_id, "book_l2",
                         [("book_l2_frame", frame, 2), ("book_l2_level", level, 4)],
                         "2026-08-01T00:00:00+00:00", "2026-08-01T00:00:02+00:00")
        conn.commit()
    finally:
        conn.close()
    return market_id


class _FailingCommitConnection:
    """在指定提交前注入一次 SQLite 故障。"""

    def __init__(self, conn: sqlite3.Connection, fail_on: int) -> None:
        self._conn = conn
        self._fail_on = fail_on
        self._commits = 0

    def commit(self) -> None:
        self._commits += 1
        if self._commits == self._fail_on:
            raise sqlite3.OperationalError("injected commit failure")
        self._conn.commit()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)


def _failed_publication(
    root: Path, conn: sqlite3.Connection, domain: str,
) -> tuple[str, dict[str, object]]:
    row = conn.execute(
        "SELECT attempt_id FROM partition_attempt WHERE domain=? "
        "AND status='failed' ORDER BY started_at DESC LIMIT 1",
        (domain,),
    ).fetchone()
    assert row is not None
    attempt_id = str(row[0])
    manifests = list((root / "materialized").rglob(
        f"manifest-{attempt_id}.json",
    ))
    assert len(manifests) == 1
    manifest = manifests[0]
    body = json.loads(manifest.read_text(encoding="utf-8"))
    assert body["status"] == "failed"
    assert body["failure_detail"] == "injected commit failure"
    manifest_kind = conn.execute(
        "SELECT a.artifact_kind FROM artifact_location l JOIN artifact a "
        "ON a.artifact_id=l.artifact_id WHERE l.storage_path=?",
        (manifest.relative_to(root).as_posix(),),
    ).fetchone()
    assert manifest_kind == ("failed_materialization_manifest",)
    outputs = body["non_promoted_outputs"]
    assert isinstance(outputs, list) and outputs
    for output in outputs:
        assert isinstance(output, dict)
        storage = str(output["output"])
        path = root / storage
        assert path.is_file()
        registered = conn.execute(
            "SELECT a.artifact_kind FROM artifact_location l JOIN artifact a "
            "ON a.artifact_id=l.artifact_id WHERE l.storage_path=?",
            (storage,),
        ).fetchone()
        assert registered == ("materialized_parquet",)
    return attempt_id, body


def test_active_head_queries_close_across_domains(tmp_path: Path) -> None:
    market_id = _fixture(tmp_path)
    query = MaterializedQuery(tmp_path)
    start = datetime(2026, 8, 1, tzinfo=UTC)
    end = start + timedelta(hours=2)

    klines, kline_tag = query.klines(market_id, "1hour", start, end)
    trades, trade_tag = query.trades(market_id, start, end)
    footprint, _ = query.footprint(market_id, "1hour", "100", start, end)
    book, book_tag = query.latest_l2(market_id, 5)

    assert len(klines["items"]) == 1
    assert [row["side"] for row in trades["items"]] == ["buy", "sell"]
    assert footprint["bars"][0]["total"] == "3.000000000000"
    assert footprint["bars"][0]["delta"] == "-1.000000000000"
    assert footprint["bars"][0]["trade_count"] == 2
    assert book["asks"][0]["price"] == "102"
    assert book["bids"][0]["price"] == "99"
    assert book["meta"]["replay_frames"] == 2
    assert len({kline_tag, trade_tag, book_tag}) == 3
    assert all(tag.startswith('"sha256-') for tag in (kline_tag, trade_tag, book_tag))


def test_latest_l2_bands_use_full_state_and_blank_incomplete_widths(
    tmp_path: Path,
) -> None:
    market_id = _fixture(tmp_path)
    query = MaterializedQuery(tmp_path)
    snapshot = query.catalog.active_outputs(
        market_id, domains=("book_l2",),
        datasets=("book_l2_frame", "book_l2_level"),
    )
    query._l2_cache[(market_id, snapshot.head_generation)] = {
        "asks": [
            {"price": "100.01", "size": "1"},
            {"price": "100.04", "size": "2"},
            {"price": "100.06", "size": "4"},
        ],
        "bids": [
            {"price": "99.99", "size": "1"},
            {"price": "99.96", "size": "3"},
            {"price": "99.94", "size": "5"},
        ],
        "as_of_event_time": "2026-08-01T00:00:02+00:00",
        "as_of_available_time": "2026-08-01T00:00:03+00:00",
        "snapshot_event_time": "2026-08-01T00:00:00+00:00",
        "replay_frames": 2,
        "integrity_mode": "snapshot_no_sequence",
        "source_depth_levels": 3,
        "state_source": "test_full_state",
    }

    book, _ = query.latest_l2(market_id, 1)

    assert len(book["asks"]) == len(book["bids"]) == 1
    assert book["coverage"] == {"ask_bp": "1.0000", "bid_bp": "1.0000"}
    assert book["source_coverage"] == {
        "ask_bp": "6.0000", "bid_bp": "6.0000",
    }
    band_5 = book["bands"][0]
    assert band_5["complete"] is True
    assert band_5["ask_size"] == "3"
    assert band_5["bid_size"] == "4"
    assert band_5["imbalance_size"] == "0.1428571428571428571428571429"
    band_10 = book["bands"][1]
    assert band_10["complete"] is False
    assert band_10["ask_size"] is None
    assert band_10["bid_size"] is None
    assert band_10["imbalance_size"] is None
    assert book["meta"]["returned_depth_clipped"] is True
    assert book["meta"]["band_basis"] == "full_replayed_state"


def test_latest_l2_explicit_decision_time_enforces_pit_and_exposes_lineage(
    tmp_path: Path,
) -> None:
    market_id = _fixture(tmp_path)
    decision = datetime(2026, 8, 1, 0, 0, 2, tzinfo=UTC)

    book, _ = MaterializedQuery(tmp_path).latest_l2(
        market_id, 5, decision_time=decision,
    )

    # PIT 排除尚不可用的 f2。
    assert book["best_ask"] == "101"
    assert book["meta"]["decision_time"] == decision.isoformat()
    assert book["meta"]["as_of_frame_id"] == "f1"
    assert book["meta"]["source_attempt_id"] == "l-attempt"
    assert book["meta"]["source_artifact_id"].startswith("sha256-")
    assert book["meta"]["source_attempt_ids"] == ["l-attempt"]


def test_v2_endpoint_revalidates_by_head_generation(tmp_path: Path) -> None:
    market_id = _fixture(tmp_path)
    app = create_app(
        load_config(env_file=tmp_path / "absent.env"),
        object(),  # v2 成品端点不调用来源客户端
        object(),
        "token",
        data_root=tmp_path,
    )
    client = TestClient(
        app, base_url="http://127.0.0.1",
        headers={"X-Guvolu-Token": "token"},
    )
    url = (
        f"/api/v2/markets/{market_id}/klines?interval=1hour"
        "&from=2026-08-01T00:00:00Z&to=2026-08-01T02:00:00Z"
    )
    first = client.get(url)
    assert first.status_code == 200
    assert first.headers["etag"].startswith('"sha256-')
    assert first.headers["x-guvolu-head-generation"].startswith("sha256-")
    second = client.get(url, headers={"If-None-Match": first.headers["etag"]})
    assert second.status_code == 304
    assert second.content == b""


def test_book_state_checkpoint_binds_upstream_attempt(tmp_path: Path) -> None:
    market_id = _fixture(tmp_path)
    conn = store.connect(tmp_path)
    try:
        first = materialize_checkpoints(tmp_path, conn)
        second = materialize_checkpoints(tmp_path, conn)
        report = audit_checkpoints(tmp_path, conn)
        dependency = conn.execute(
            "SELECT upstream_attempt_id FROM materialization_dependency "
            "WHERE attempt_id=?", (first[0].attempt_id,),
        ).fetchone()
        head = conn.execute(
            "SELECT attempt_id FROM materialization_partition_head "
            "WHERE market_id=? AND domain='book_state'",
            (market_id,),
        ).fetchone()
    finally:
        conn.close()
    checkpoint_book, _ = MaterializedQuery(tmp_path).latest_l2(market_id, 5)
    assert first[0].rows == 2 and first[0].replay_frames == 2
    assert second[0].reused is True
    assert dependency == ("l-attempt",)
    assert head == (first[0].attempt_id,)
    assert report == {"checkpoints": 1, "errors": [], "ok": True}
    assert checkpoint_book["meta"]["state_source"] == "book_state_checkpoint"


def test_book_state_final_commit_failure_is_registered_and_retryable(
    tmp_path: Path,
) -> None:
    market_id = _fixture(tmp_path)
    conn = store.connect(tmp_path)
    failing = cast(
        sqlite3.Connection,
        _FailingCommitConnection(conn, fail_on=3),
    )
    try:
        with pytest.raises(
            sqlite3.OperationalError,
            match="injected commit failure",
        ):
            materialize_checkpoints(tmp_path, failing)
        failed_attempt, body = _failed_publication(
            tmp_path, conn, "book_state",
        )
        assert conn.execute(
            "SELECT COUNT(*) FROM materialization_output WHERE attempt_id=?",
            (failed_attempt,),
        ).fetchone() == (0,)
        assert conn.execute(
            "SELECT COUNT(*) FROM materialization_partition_head "
            "WHERE market_id=? AND domain='book_state'",
            (market_id,),
        ).fetchone() == (0,)

        retry = materialize_checkpoints(tmp_path, conn)[0]
        retry_paths = {
            str(row[0]) for row in conn.execute(
                "SELECT a.storage_path FROM materialization_output o "
                "JOIN artifact a ON a.artifact_id=o.artifact_id "
                "WHERE o.attempt_id=?",
                (retry.attempt_id,),
            )
        }
    finally:
        conn.close()

    failed_paths = {
        str(row["output"])
        for row in cast(list[dict[str, object]], body["non_promoted_outputs"])
    }
    assert retry_paths == failed_paths
    assert retry.attempt_id != failed_attempt


def test_book_state_initial_commit_failure_rolls_back_cleanly(
    tmp_path: Path,
) -> None:
    market_id = _fixture(tmp_path)
    conn = store.connect(tmp_path)
    failing = cast(
        sqlite3.Connection,
        _FailingCommitConnection(conn, fail_on=2),
    )
    try:
        with pytest.raises(
            sqlite3.OperationalError,
            match="injected commit failure",
        ):
            materialize_checkpoints(tmp_path, failing)
        assert conn.in_transaction is False
        assert conn.execute(
            "SELECT COUNT(*) FROM partition_attempt WHERE domain='book_state'",
        ).fetchone() == (0,)
        assert not (
            tmp_path / "materialized" / "book_state_checkpoint"
        ).exists()

        retry = materialize_checkpoints(tmp_path, conn)[0]
        assert retry.market_id == market_id
    finally:
        conn.close()


def test_checkpoint_rejects_same_time_historical_head_correction(
    tmp_path: Path,
) -> None:
    market_id = _fixture(tmp_path)
    recent_frame = tmp_path / "materialized/recent-frame.parquet"
    recent_level = tmp_path / "materialized/recent-level.parquet"
    _parquet(recent_frame, """
      SELECT 'f3' frame_id,'gmo' venue_id,'BTC' venue_symbol,
        'mkt__gmo__btc__r0' market_id,0 mapping_revision,
        'SPOT:BTC/JPY' instrument_id,'delta' message_kind,
        TIMESTAMPTZ '2026-08-01 00:00:04+00' event_time,
        TIMESTAMPTZ '2026-08-01 00:00:05+00' available_time,
        TIMESTAMPTZ '2026-08-01 00:00:05+00' ingest_time,'venue' time_origin,
        NULL::VARCHAR sequence_id,NULL::VARCHAR checksum,
        'snapshot_no_sequence' integrity_mode,1 bid_levels,1 ask_levels,
        2 source_depth_levels,'101' mid_price,NULL::VARCHAR ask_market_size,
        NULL::VARCHAR bid_market_size,NULL::VARCHAR asks_over,
        NULL::VARCHAR bids_under,'run' run_id,1 segment_sequence,
        'recent-source' source_artifact_id,2 source_row_index,'l2-v1' normalization_version,
        1 schema_version
    """)
    _parquet(recent_level, """
      SELECT 'f3' frame_id,'mkt__gmo__btc__r0' market_id,'ask' side,
        0 source_level_index,'104' price,'5' size,'set' "action",'limit' level_kind,
        'recent-source' source_artifact_id,2 source_row_index,'l2-v1' normalization_version,
        1 schema_version
    """)
    conn = store.connect(tmp_path)
    try:
        _register_output(
            conn, tmp_path, "l-recent", market_id, "book_l2",
            [("book_l2_frame", recent_frame, 1),
             ("book_l2_level", recent_level, 1)],
            "2026-08-01T00:00:04+00:00", "2026-08-01T00:00:04+00:00",
            partition_key="p2",
        )
        conn.commit()
        checkpoint = materialize_checkpoints(tmp_path, conn)[0]
    finally:
        conn.close()

    before, _ = MaterializedQuery(tmp_path).latest_l2(market_id, 5)
    assert before["best_ask"] == "102"
    assert before["meta"]["state_source"] == "book_state_checkpoint"

    corrected_frame = tmp_path / "materialized/corrected-frame.parquet"
    corrected_level = tmp_path / "materialized/corrected-level.parquet"
    _parquet(corrected_frame, """
      SELECT * FROM (VALUES
        ('c1','gmo','BTC','mkt__gmo__btc__r0',0,'SPOT:BTC/JPY','snapshot',
         TIMESTAMPTZ '2026-08-01 00:00:00+00',TIMESTAMPTZ '2026-08-01 00:00:01+00',
         TIMESTAMPTZ '2026-08-01 00:00:01+00','venue',NULL,NULL,'snapshot_no_sequence',
         1,1,2,'101',NULL,NULL,NULL,NULL,'run',1,'corrected-source',0,'l2-v1',1),
        ('c2','gmo','BTC','mkt__gmo__btc__r0',0,'SPOT:BTC/JPY','delta',
         TIMESTAMPTZ '2026-08-01 00:00:02+00',TIMESTAMPTZ '2026-08-01 00:00:03+00',
         TIMESTAMPTZ '2026-08-01 00:00:03+00','venue',NULL,NULL,'snapshot_no_sequence',
         1,1,2,'101',NULL,NULL,NULL,NULL,'run',1,'corrected-source',1,'l2-v1',1)
      ) f(frame_id,venue_id,venue_symbol,market_id,mapping_revision,instrument_id,
        message_kind,event_time,available_time,ingest_time,time_origin,sequence_id,
        checksum,integrity_mode,bid_levels,ask_levels,source_depth_levels,mid_price,
        ask_market_size,bid_market_size,asks_over,bids_under,run_id,segment_sequence,
        source_artifact_id,source_row_index,normalization_version,schema_version)
    """)
    _parquet(corrected_level, """
      SELECT * FROM (VALUES
        ('c1','mkt__gmo__btc__r0','ask',0,'104','2','set','limit','corrected-source',0,'l2-v1',1),
        ('c1','mkt__gmo__btc__r0','bid',0,'98','3','set','limit','corrected-source',0,'l2-v1',1),
        ('c2','mkt__gmo__btc__r0','bid',0,'98','4','set','limit','corrected-source',1,'l2-v1',1)
      ) l(frame_id,market_id,side,source_level_index,price,size,"action",level_kind,
        source_artifact_id,source_row_index,normalization_version,schema_version)
    """)
    conn = store.connect(tmp_path)
    try:
        conn.execute(
            "DELETE FROM materialization_partition_head "
            "WHERE market_id=? AND domain='book_l2' AND partition_key='p1'",
            (market_id,),
        )
        _register_output(
            conn, tmp_path, "l-corrected", market_id, "book_l2",
            [("book_l2_frame", corrected_frame, 2),
             ("book_l2_level", corrected_level, 3)],
            "2026-08-01T00:00:00+00:00", "2026-08-01T00:00:02+00:00",
            partition_key="p1",
        )
        conn.commit()
        report = audit_checkpoints(tmp_path, conn)
    finally:
        conn.close()

    after, _ = MaterializedQuery(tmp_path).latest_l2(market_id, 5)
    assert checkpoint.attempt_id
    assert report["ok"] is False
    assert after["best_ask"] == "104"
    assert after["meta"]["state_source"] == "l2_wire_order_snapshot_delta_replay"


def test_checkpoint_without_complete_lineage_safely_replays(tmp_path: Path) -> None:
    market_id = _fixture(tmp_path)
    conn = store.connect(tmp_path)
    try:
        checkpoint = materialize_checkpoints(tmp_path, conn)[0]
        conn.execute(
            "DELETE FROM partition_input WHERE attempt_id=?",
            (checkpoint.attempt_id,),
        )
        conn.commit()
    finally:
        conn.close()

    book, _ = MaterializedQuery(tmp_path).latest_l2(market_id, 5)
    assert book["best_ask"] == "102"
    assert book["meta"]["state_source"] == "l2_wire_order_snapshot_delta_replay"


def test_checkpoint_cli_enumerates_under_writer_lock(tmp_path: Path) -> None:
    _fixture(tmp_path)
    assert checkpoint_main(["--data-root", str(tmp_path), "all"]) == 0


def test_sparse_orderflow_tile_binds_l2_and_trade_attempts(tmp_path: Path) -> None:
    market_id = _fixture(tmp_path)
    conn = store.connect(tmp_path)
    try:
        first = materialize_hour(
            tmp_path, conn, market_id,
            datetime(2026, 8, 1, tzinfo=UTC), "5s",
        )
        again = materialize_hour(
            tmp_path, conn, market_id,
            datetime(2026, 8, 1, tzinfo=UTC), "5s",
        )
        outputs = dict(conn.execute(
            "SELECT dataset,row_count FROM materialization_output "
            "WHERE attempt_id=?", (first.attempt_id,),
        ).fetchall())
        dependencies = {
            str(row[0]) for row in conn.execute(
                "SELECT upstream_attempt_id FROM materialization_dependency "
                "WHERE attempt_id=?", (first.attempt_id,),
            )
        }
        paths = dict(conn.execute(
            "SELECT o.dataset,a.storage_path FROM materialization_output o "
            "JOIN artifact a ON a.artifact_id=o.artifact_id "
            "WHERE o.attempt_id=?", (first.attempt_id,),
        ).fetchall())
        report = audit_tiles(tmp_path, conn)
        generic_report = audit_materializations(tmp_path, conn)
        manifest_registered = conn.execute(
            "SELECT COUNT(*) FROM artifact_location l JOIN artifact a "
            "ON a.artifact_id=l.artifact_id WHERE a.artifact_kind="
            "'materialization_manifest' AND l.storage_path LIKE ?",
            (f"%/manifest-{first.attempt_id}.json",),
        ).fetchone()[0]
        recent = recent_l2_markets(
            conn, datetime(2026, 8, 1, 1, tzinfo=UTC),
        )
    finally:
        conn.close()
    db = duckdb.connect(":memory:")
    try:
        columns = db.execute(
            "SELECT COUNT(*),SUM(is_anchor),SUM(is_gap) FROM read_parquet(?)",
            [str(tmp_path / paths[DATASET_COLUMN])],
        ).fetchone()
        cells = db.execute(
            "SELECT COUNT(*),SUM(try_cast(taker_buy_size AS DECIMAL(38,12))),"
            "SUM(try_cast(taker_sell_size AS DECIMAL(38,12))),"
            "COUNT(*) FILTER (WHERE net_decrease_unknown<>'0') "
            "FROM read_parquet(?)", [str(tmp_path / paths[DATASET_CELL])],
        ).fetchone()
    finally:
        db.close()
    assert first.column_rows == 720
    assert outputs[DATASET_COLUMN] == 720
    assert outputs[DATASET_CELL] == first.cell_rows
    assert again.reused is True and again.attempt_id == first.attempt_id
    assert dependencies == {"l-attempt", "t-attempt"}
    assert columns[0] == 720 and columns[1] >= 1 and columns[2] > 0
    assert cells[0] == first.cell_rows
    assert str(cells[1]) == "1.000000000000"
    assert str(cells[2]) == "2.000000000000"
    assert report["ok"] is True
    assert manifest_registered == 1
    assert not any(
        error.startswith("输入位置台账不符:")
        for error in generic_report.errors
    )
    assert market_id in recent

    query_payload, tag = MaterializedQuery(tmp_path).orderflow_tiles(
        market_id, "5s", datetime(2026, 8, 1, tzinfo=UTC),
        datetime(2026, 8, 1, 1, tzinfo=UTC),
    )
    assert len(query_payload["columns"]) == 720
    assert query_payload["meta"]["attribution"] == (
        "l2_change_and_trade_kept_separate"
    )
    assert tag.startswith('"sha256-')
    app = create_app(
        load_config(env_file=tmp_path / "absent.env"), object(), object(),
        "token", data_root=tmp_path,
    )
    response = TestClient(
        app, base_url="http://127.0.0.1",
        headers={"X-Guvolu-Token": "token"},
    ).get(
        f"/api/v2/markets/{market_id}/orderflow/tiles",
        params={
            "bucket": "5s", "from": "2026-08-01T00:00:00Z",
            "to": "2026-08-01T01:00:00Z",
        },
    )
    assert response.status_code == 200
    assert response.headers["x-guvolu-head-generation"].startswith("sha256-")
    assert len(response.json()["columns"]) == 720

    middle, _ = MaterializedQuery(tmp_path).orderflow_tiles(
        market_id, "5s", datetime(2026, 8, 1, 0, 10, tzinfo=UTC),
        datetime(2026, 8, 1, 0, 20, tzinfo=UTC),
    )
    assert middle["columns"][0]["is_anchor"] is True
    assert middle["columns"][0]["context_only"] is True
    assert middle["meta"]["anchor_context_columns"] > 0


def test_orderflow_final_commit_failure_is_registered_and_retryable(
    tmp_path: Path,
) -> None:
    market_id = _fixture(tmp_path)
    start = datetime(2026, 8, 1, tzinfo=UTC)
    conn = store.connect(tmp_path)
    failing = cast(
        sqlite3.Connection,
        _FailingCommitConnection(conn, fail_on=2),
    )
    try:
        with pytest.raises(
            sqlite3.OperationalError,
            match="injected commit failure",
        ):
            materialize_hour(tmp_path, failing, market_id, start, "5s")
        failed_attempt, body = _failed_publication(
            tmp_path, conn, "orderflow_tile",
        )
        assert conn.execute(
            "SELECT COUNT(*) FROM materialization_output WHERE attempt_id=?",
            (failed_attempt,),
        ).fetchone() == (0,)
        assert conn.execute(
            "SELECT COUNT(*) FROM materialization_partition_head "
            "WHERE market_id=? AND domain='orderflow_tile'",
            (market_id,),
        ).fetchone() == (0,)

        retry = materialize_hour(tmp_path, conn, market_id, start, "5s")
        retry_paths = {
            str(row[0]) for row in conn.execute(
                "SELECT a.storage_path FROM materialization_output o "
                "JOIN artifact a ON a.artifact_id=o.artifact_id "
                "WHERE o.attempt_id=?",
                (retry.attempt_id,),
            )
        }
    finally:
        conn.close()

    failed_paths = {
        str(row["output"])
        for row in cast(list[dict[str, object]], body["non_promoted_outputs"])
    }
    assert len(failed_paths) == 2
    assert retry_paths == failed_paths
    assert retry.attempt_id != failed_attempt


def test_orderflow_initial_commit_failure_rolls_back_cleanly(
    tmp_path: Path,
) -> None:
    market_id = _fixture(tmp_path)
    start = datetime(2026, 8, 1, tzinfo=UTC)
    conn = store.connect(tmp_path)
    failing = cast(
        sqlite3.Connection,
        _FailingCommitConnection(conn, fail_on=1),
    )
    try:
        with pytest.raises(
            sqlite3.OperationalError,
            match="injected commit failure",
        ):
            materialize_hour(tmp_path, failing, market_id, start, "5s")
        assert conn.in_transaction is False
        assert conn.execute(
            "SELECT COUNT(*) FROM partition_attempt "
            "WHERE domain='orderflow_tile'",
        ).fetchone() == (0,)
        assert not (
            tmp_path / "materialized" / "orderflow_tile"
        ).exists()

        retry = materialize_hour(tmp_path, conn, market_id, start, "5s")
        assert retry.market_id == market_id
    finally:
        conn.close()


def test_orderflow_tile_writes_schemaful_empty_cell_parquet(tmp_path: Path) -> None:
    start = datetime(2026, 8, 1, tzinfo=UTC)
    end = start + timedelta(seconds=5)
    snapshot = ActiveOutputSnapshot(
        market={"market_id": "mkt__test__btc_jpy__r0", "venue_id": "test"},
        outputs=(), head_generation="sha256-test",
    )
    column = (
        "column-1", "mkt__test__btc_jpy__r0", "test", "5s", "1",
        "instrument_map_tick_size", int(start.timestamp()), start, end, "ok",
        True, False, False, False, 1, 0, start, start, "test",
        "sha256-test", TILE_METHOD_VERSION, 2,
    )
    outputs = _write_outputs(
        tmp_path, snapshot,
        TileBuild((column,), (), start, end), start, "5s", "attempt-empty",
    )
    by_dataset = {dataset: (path, rows) for dataset, path, _sha, rows in outputs}
    cell_path, cell_rows = by_dataset[DATASET_CELL]
    db = duckdb.connect(":memory:")
    try:
        count = db.execute(
            "SELECT COUNT(*) FROM read_parquet(?)", [str(cell_path)],
        ).fetchone()[0]
    finally:
        db.close()
    assert cell_rows == 0
    assert count == 0


def test_large_unsequenced_l2_replays_only_latest_snapshot_tail(
    tmp_path: Path,
) -> None:
    frame_path = tmp_path / "okx-frame.parquet"
    level_path = tmp_path / "okx-level.parquet"
    _parquet(frame_path, """
      SELECT printf('f%d', i) frame_id,'mkt__okx__btc_usdt__r0' market_id,
        CASE WHEN i IN (0,99990) THEN 'snapshot' ELSE 'delta' END message_kind,
        to_timestamp(1722470400 + i / 1000.0) event_time,
        to_timestamp(1722470400 + i / 1000.0) available_time,
        to_timestamp(1722470400 + i / 1000.0) ingest_time,
        NULL::VARCHAR sequence_id,'okx-day' source_session_id,
        1 segment_sequence,i source_row_index,'snapshot_plus_absolute_delta' integrity_mode,
        400 source_depth_levels FROM range(100005) t(i)
    """)
    _parquet(level_path, """
      SELECT * FROM (VALUES
        ('f99990','ask','101','2','set',0),
        ('f99990','bid','99','3','set',0),
        ('f100004','ask','101','4','set',0)
      ) t(frame_id,side,price,size,"action",source_level_index)
    """)
    start = datetime.fromtimestamp(1722470400, UTC)
    end = start + timedelta(milliseconds=100004)
    outputs = (
        ActiveOutput(
            "book_l2", "day", "v2", "a", "book_l2_frame", "af",
            frame_path, 100005, start, end,
        ),
        ActiveOutput(
            "book_l2", "day", "v2", "a", "book_l2_level", "al",
            level_path, 3, start + timedelta(milliseconds=99990), end,
        ),
    )
    state = replay_l2_snapshot(ActiveOutputSnapshot(
        {
            "market_id": "mkt__okx__btc_usdt__r0",
            "venue_id": "okx",
        },
        outputs,
        "sha256-okx",
    ))
    assert state["replay_frames"] == 15
    assert state["asks"] == [{"price": "101", "size": "4"}]
    assert state["bids"] == [{"price": "99", "size": "3"}]


def test_bitbank_delayed_whole_replays_buffer_across_segments(
    tmp_path: Path,
) -> None:
    frame_one = tmp_path / "bitbank-frame-1.parquet"
    frame_two = tmp_path / "bitbank-frame-2.parquet"
    level_one = tmp_path / "bitbank-level-1.parquet"
    level_two = tmp_path / "bitbank-level-2.parquet"
    _parquet(frame_one, """
      SELECT 'd6' frame_id,'mkt__bitbank__btc_jpy__r0' market_id,
        'delta' message_kind,TIMESTAMPTZ '2026-08-01 00:00:01+00' event_time,
        TIMESTAMPTZ '2026-08-01 00:00:01+00' available_time,
        TIMESTAMPTZ '2026-08-01 00:00:01+00' ingest_time,'6' sequence_id,
        'conn-1' connection_id,'run-1' run_id,1 segment_sequence,
        1 source_row_index,'monotonic' integrity_mode,200 source_depth_levels
    """)
    _parquet(frame_two, """
      SELECT 'w5' frame_id,'mkt__bitbank__btc_jpy__r0' market_id,
        'snapshot' message_kind,TIMESTAMPTZ '2026-08-01 00:00:00+00' event_time,
        TIMESTAMPTZ '2026-08-01 00:00:02+00' available_time,
        TIMESTAMPTZ '2026-08-01 00:00:02+00' ingest_time,'5' sequence_id,
        'conn-1' connection_id,'run-1' run_id,2 segment_sequence,
        1 source_row_index,'monotonic' integrity_mode,200 source_depth_levels
    """)
    _parquet(level_one, """
      SELECT 'd6' frame_id,'ask' side,'102' price,'3' size,'set' "action",0 source_level_index
    """)
    _parquet(level_two, """
      SELECT * FROM (VALUES
        ('w5','ask','101','1','set',0),
        ('w5','bid','99','2','set',0)
      ) t(frame_id,side,price,size,"action",source_level_index)
    """)
    start = datetime(2026, 8, 1, tzinfo=UTC)
    outputs = (
        ActiveOutput(
            "book_l2", "s1", "v4", "a1", "book_l2_frame", "af1",
            frame_one, 1, start + timedelta(seconds=1),
            start + timedelta(seconds=1),
        ),
        ActiveOutput(
            "book_l2", "s1", "v4", "a1", "book_l2_level", "al1",
            level_one, 1, start + timedelta(seconds=1),
            start + timedelta(seconds=1),
        ),
        ActiveOutput(
            "book_l2", "s2", "v4", "a2", "book_l2_frame", "af2",
            frame_two, 1, start, start + timedelta(seconds=2),
        ),
        ActiveOutput(
            "book_l2", "s2", "v4", "a2", "book_l2_level", "al2",
            level_two, 2, start, start + timedelta(seconds=2),
        ),
    )
    state = replay_l2_snapshot(ActiveOutputSnapshot(
        {
            "market_id": "mkt__bitbank__btc_jpy__r0",
            "venue_id": "bitbank",
        },
        outputs,
        "sha256-bitbank",
    ))
    assert state["asks"] == [
        {"price": "101", "size": "1"},
        {"price": "102", "size": "3"},
    ]
    assert state["bids"] == [{"price": "99", "size": "2"}]
    assert state["source_attempt_id"] == "a1"
    assert state["source_partition_key"] == "s1"
