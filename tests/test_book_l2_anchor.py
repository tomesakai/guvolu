from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import threading
import time
from pathlib import Path
from typing import Any

import duckdb
import requests

from guvolu.data import book_l2_anchor, l2_capture, store
from guvolu.data.book_l2_anchor import (
    ANCHOR_QUEUE_SIZE,
    AnchorMaterializationResult,
    RestAnchorWorker,
    WsCheckpoint,
    _persist_raw,
    compare_anchor,
    parse_anchor,
    persist_anchor_fetch,
    recover_raw_anchor,
)
from guvolu.data.materialize import (
    _market_row,
    _register_content_artifact,
    _relative_storage_path,
    artifact_id,
    ensure_markets,
    sha256_file,
)
from guvolu.venues import registry
from guvolu.venues.l2_anchor import (
    AnchorFetch,
    BITBANK_ENDPOINT,
    BITFLYER_ENDPOINT,
    GMO_ENDPOINT,
    PublicRestAnchorAdapter,
    _request_url,
)


def _fetch(
    endpoint: object,
    symbol: str,
    body: bytes | None,
    *,
    status: int | None = 200,
    error_kind: str | None = None,
) -> AnchorFetch:
    resolved = {
        "gmo": GMO_ENDPOINT,
        "bitbank": BITBANK_ENDPOINT,
        "bitflyer": BITFLYER_ENDPOINT,
    }[str(endpoint)]
    url = f"https://example.invalid/{resolved.venue_id}/{symbol}"
    return AnchorFetch(
        resolved, symbol, url,
        hashlib.sha256(f"GET\n{url}\n".encode()).hexdigest(),
        "2026-08-13T00:00:00+00:00",
        "2026-08-13T00:00:00.200000+00:00",
        status, body, error_kind,
        None if error_kind is None else "fixture unavailable",
    )


def test_three_venue_parsers_preserve_native_time_and_sequence() -> None:
    gmo = _fetch("gmo", "BTC", json.dumps({
        "status": 0,
        "data": {
            "symbol": "BTC",
            "asks": [{"price": "101.00", "size": "2.500"}],
            "bids": [{"price": "100", "size": "3"}],
        },
        "responsetime": "2026-08-13T00:00:00.100Z",
    }).encode())
    bitbank = _fetch("bitbank", "btc_jpy", (
        b'{"success":1,"data":{"asks":[["101","2"]],'
        b'"bids":[["100","3"]],"timestamp":1786579200100,'
        b'"sequenceId":"9007199254740993"}}'
    ))
    bitflyer = _fetch("bitflyer", "BTC_JPY", (
        b'{"mid_price":100.5,"bids":[{"price":100.0,"size":3.25}],'
        b'"asks":[{"price":101.0,"size":2.5}]}'
    ))

    parsed_gmo = parse_anchor(gmo, "2026-08-13T00:00:00.300+00:00")
    parsed_bitbank = parse_anchor(
        bitbank, "2026-08-13T00:00:00.300+00:00"
    )
    parsed_bitflyer = parse_anchor(
        bitflyer, "2026-08-13T00:00:00.300+00:00"
    )

    assert parsed_gmo.time_origin == "venue_response"
    assert parsed_gmo.available_time == "2026-08-13T00:00:00.300000+00:00"
    assert parsed_gmo.receive_source_offset_ms == 100.0
    assert parsed_gmo.availability_basis == "ingest_time"
    assert parsed_gmo.best_ask == "101"
    assert parsed_gmo.ask_depth == "2.5"
    assert parsed_bitbank.time_origin == "venue"
    assert parsed_bitbank.sequence_id == "9007199254740993"
    assert parsed_bitflyer.time_origin == "receive_proxy"
    assert parsed_bitflyer.event_time < parsed_bitflyer.available_time
    assert parsed_bitflyer.best_bid == "100"
    assert parsed_bitflyer.bid_depth == "3.25"

    stale_same_sequence = WsCheckpoint(
        attempt_id="checkpoint", artifact_id="sha256-" + "a" * 64,
        as_of_frame_id="frame", event_time="2026-08-12T23:59:00+00:00",
        available_time="2026-08-12T23:59:00+00:00",
        sequence_id=parsed_bitbank.sequence_id,
        best_bid="99", best_ask="102", bid_levels=1, ask_levels=1,
        bid_depth="1", ask_depth="1", book_hash="b" * 64,
    )
    comparison = compare_anchor(
        "bitbank", parsed_bitbank, stale_same_sequence, "unused"
    )
    assert comparison.status == "mismatch"
    assert comparison.basis == "bitbank_equal_sequence"
    assert comparison.full_book_comparable is True

    wrong_scope = WsCheckpoint(
        attempt_id="checkpoint-2", artifact_id="sha256-" + "c" * 64,
        as_of_frame_id="frame-2", event_time=stale_same_sequence.event_time,
        available_time=stale_same_sequence.available_time,
        sequence_id=parsed_bitbank.sequence_id,
        best_bid="99", best_ask="102", bid_levels=2, ask_levels=1,
        bid_depth="1", ask_depth="1", book_hash="d" * 64,
    )
    different_depth = compare_anchor(
        "bitbank", parsed_bitbank, wrong_scope, "unused"
    )
    assert different_depth.status == "unknown"
    assert different_depth.full_book_comparable is False


def test_gmo_source_clock_ahead_uses_event_as_pit_visibility() -> None:
    fetch = _fetch("gmo", "BTC", json.dumps({
        "status": 0,
        "data": {
            "symbol": "BTC",
            "asks": [{"price": "101", "size": "2"}],
            "bids": [{"price": "100", "size": "3"}],
        },
        "responsetime": "2026-08-13T00:00:01.000Z",
    }).encode())
    parsed = parse_anchor(fetch, "2026-08-13T00:00:00.300+00:00")
    assert parsed.event_time == "2026-08-13T00:00:01+00:00"
    assert parsed.available_time == parsed.event_time
    assert parsed.receive_source_offset_ms == -800.0
    assert parsed.availability_basis == "event_time"


def test_raw_anchor_and_normalized_levels_are_independent_from_ws(
    tmp_path: Path,
) -> None:
    body = json.dumps({
        "status": 0,
        "data": {
            "symbol": "BTC",
            "asks": [{"price": "101", "size": "2"}],
            "bids": [{"price": "100", "size": "3"}],
        },
        "responsetime": "2026-08-13T00:00:00.100Z",
    }, separators=(",", ":")).encode()
    result = persist_anchor_fetch(
        tmp_path, _fetch("gmo", "BTC", body),
        trigger_reason="connection_open",
        connection_id="run-anchor-c000001",
        request_id="fixture-one",
        ingest_time="2026-08-13T00:00:00.300000+00:00",
    )

    conn = store.connect(tmp_path)
    try:
        summary = conn.execute(
            "SELECT status,comparison_status,source_artifact_id,"
            "observation_artifact_id,reconciliation_artifact_id "
            "FROM l2_anchor_status WHERE market_id=?",
            (result.market_id,),
        ).fetchone()
        assert summary == (
            "unknown", "unknown", result.raw_artifact_id,
            result.observation_artifact_id,
            result.reconciliation_artifact_id,
        )
        assert conn.execute(
            "SELECT COUNT(*) FROM materialization_partition_head "
            "WHERE domain='book_l2'"
        ).fetchone()[0] == 0
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
        raw_storage = conn.execute(
            "SELECT storage_path FROM artifact WHERE artifact_id=?",
            (result.raw_artifact_id,),
        ).fetchone()[0]
        observation_storage = conn.execute(
            "SELECT storage_path FROM artifact WHERE artifact_id=?",
            (result.observation_artifact_id,),
        ).fetchone()[0]
        level_storage = conn.execute(
            "SELECT a.storage_path FROM materialization_output o "
            "JOIN artifact a ON a.artifact_id=o.artifact_id "
            "WHERE o.attempt_id=? AND o.dataset='book_l2_anchor_level'",
            (result.anchor_attempt_id,),
        ).fetchone()[0]
        reconciliation_storage = conn.execute(
            "SELECT a.storage_path FROM materialization_output o "
            "JOIN artifact a ON a.artifact_id=o.artifact_id "
            "WHERE o.attempt_id=? AND "
            "o.dataset='book_l2_anchor_reconciliation'",
            (result.reconciliation_attempt_id,),
        ).fetchone()[0]
        bindings = conn.execute(
            "SELECT attempt_id,venue_id,domain,endpoint,revision_id,"
            "binding_basis FROM partition_capability_binding "
            "WHERE attempt_id IN (?,?) ORDER BY attempt_id",
            (result.anchor_attempt_id, result.reconciliation_attempt_id),
        ).fetchall()
    finally:
        conn.close()
    raw = json.loads((tmp_path / raw_storage).read_text(encoding="utf-8"))
    assert base64.b64decode(raw["response_body_base64"]) == body
    assert raw["response_sha256"] == hashlib.sha256(body).hexdigest()
    assert raw["endpoint_id"] == "EP-0006"
    assert raw["endpoint_revision"] == 0
    db: Any = duckdb.connect(":memory:")
    try:
        observation = db.execute(
            "SELECT anchor_availability,best_bid,best_ask,book_hash,"
            "source_artifact_id,endpoint_id,endpoint_revision,event_time,"
            "available_time FROM read_parquet(?)",
            [str(tmp_path / observation_storage)],
        ).fetchone()
        levels = db.execute(
            "SELECT side,price,size FROM read_parquet(?) ORDER BY side",
            [str(tmp_path / level_storage)],
        ).fetchall()
        reconciliation = db.execute(
            "SELECT endpoint_id,endpoint_revision,comparison_status,"
            "full_book_comparable FROM read_parquet(?)",
            [str(tmp_path / reconciliation_storage)],
        ).fetchone()
    finally:
        db.close()
    assert observation[0:3] == ("available", "100", "101")
    assert len(observation[3]) == 64
    assert observation[4] == result.raw_artifact_id
    assert observation[5:7] == ("EP-0006", 0)
    assert observation[8] >= observation[7]
    assert levels == [("ask", "101", "2"), ("bid", "100", "3")]
    assert reconciliation == ("EP-0006", 0, "unknown", False)
    assert bindings == [
        (
            result.anchor_attempt_id, "gmo", "book_l2_anchor",
            "/v1/orderbooks", 0, "recorded",
        ),
        (
            result.reconciliation_attempt_id, "gmo", "book_l2_anchor",
            "/v1/orderbooks", 0, "recorded",
        ),
    ]


def _install_checkpoint(
    root: Path,
    *,
    available_time: str,
    bid: tuple[str, str],
    ask: tuple[str, str],
) -> tuple[str, str]:
    conn = store.connect(root)
    registry.register_all(conn)
    ensure_markets(conn)
    market_id, _, _ = _market_row(conn, "gmo", "BTC", None)
    attempt_id = "checkpoint-fixture"
    path = root / "materialized/checkpoint-fixture.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    db: Any = duckdb.connect(":memory:")
    try:
        db.execute("""
            CREATE TABLE checkpoint (
              source_attempt_id VARCHAR,as_of_frame_id VARCHAR,
              event_time TIMESTAMPTZ,available_time TIMESTAMPTZ,
              snapshot_frame_id VARCHAR,integrity_mode VARCHAR,
              side VARCHAR,price VARCHAR,size VARCHAR
            )
        """)
        db.executemany(
            "INSERT INTO checkpoint VALUES (?,?,?,?,?,?,?,?,?)",
            [
                (
                    "source-l2-fixture", "frame-1",
                    "2026-08-13T00:00:00.100+00:00", available_time,
                    "snapshot-1", "snapshot_no_sequence", "bid", *bid,
                ),
                (
                    "source-l2-fixture", "frame-1",
                    "2026-08-13T00:00:00.100+00:00", available_time,
                    "snapshot-1", "snapshot_no_sequence", "ask", *ask,
                ),
            ],
        )
        db.execute(
            "COPY checkpoint TO ? (FORMAT PARQUET,COMPRESSION ZSTD)",
            [str(path)],
        )
    finally:
        db.close()
    sha = sha256_file(path)
    identity = artifact_id(sha)
    _register_content_artifact(
        conn, identity, "materialized_parquet",
        _relative_storage_path(root, path), sha, path.stat().st_size,
        "2026-08-13T00:00:00.160+00:00", 1,
    )
    conn.execute(
        "INSERT INTO partition_attempt "
        "(attempt_id,market_id,domain,partition_key,normalization_version,"
        "input_set_hash,status,source_rows,normalized_rows,ignored_rows,"
        "rejected_rows,started_at,finished_at,code_version,config_hash) "
        "VALUES (?,?,'book_state','latest','book-state-checkpoint-v2',"
        "?,'complete',2,2,0,0,?,?,?,?)",
        (
            attempt_id, market_id, "f" * 64,
            "2026-08-13T00:00:00.150+00:00",
            "2026-08-13T00:00:00.160+00:00", "fixture", "c" * 64,
        ),
    )
    conn.execute(
        "INSERT INTO materialization_output VALUES (?,?,?,?,?,?,?)",
        (
            attempt_id, identity, "book_state_checkpoint", 2,
            "2026-08-13T00:00:00.100+00:00",
            "2026-08-13T00:00:00.100+00:00",
            "2026-08-13T00:00:00.160+00:00",
        ),
    )
    conn.execute(
        "INSERT INTO materialization_partition_head VALUES (?,?,?,?,?,?)",
        (
            market_id, "book_state", "latest", "book-state-checkpoint-v2",
            attempt_id, "2026-08-13T00:00:00.160+00:00",
        ),
    )
    conn.commit()
    conn.close()
    return market_id, attempt_id


def test_gmo_recent_ws_checkpoint_is_approximate_unknown_and_lineage_bound(
    tmp_path: Path,
) -> None:
    market_id, checkpoint_attempt = _install_checkpoint(
        tmp_path,
        available_time="2026-08-13T00:00:00.150+00:00",
        bid=("100", "3"), ask=("101", "2"),
    )
    body = json.dumps({
        "status": 0,
        "data": {
            "symbol": "BTC",
            "asks": [{"price": "101", "size": "2"}],
            "bids": [{"price": "100", "size": "3"}],
        },
        "responsetime": "2026-08-13T00:00:00.100Z",
    }).encode()
    result = persist_anchor_fetch(
        tmp_path, _fetch("gmo", "BTC", body),
        trigger_reason="reconnect",
        connection_id="run-anchor-c000002",
        request_id="fixture-two",
        ingest_time="2026-08-13T00:00:00.300+00:00",
    )

    assert result.status == "unknown"
    assert result.comparison_status == "unknown"
    conn = store.connect(tmp_path)
    try:
        summary = conn.execute(
            "SELECT status,comparison_status,comparison_lag_ms,"
            "ws_checkpoint_attempt_id FROM l2_anchor_status "
            "WHERE market_id=?",
            (market_id,),
        ).fetchone()
        assert summary == ("unknown", "unknown", 150.0, checkpoint_attempt)
        dependencies = {
            row[0] for row in conn.execute(
                "SELECT upstream_attempt_id FROM materialization_dependency "
                "WHERE attempt_id=?",
                (result.reconciliation_attempt_id,),
            )
        }
        assert dependencies == {result.anchor_attempt_id, checkpoint_attempt}
    finally:
        conn.close()


def test_unavailable_anchor_is_persisted_without_fake_levels(
    tmp_path: Path,
) -> None:
    fetch = _fetch(
        "bitflyer", "BTC_JPY", None,
        status=None, error_kind="timeout",
    )
    result = persist_anchor_fetch(
        tmp_path, fetch, trigger_reason="periodic", connection_id=None,
        request_id="fixture-timeout",
        ingest_time="2026-08-13T00:00:00.300+00:00",
    )

    assert result.status == "unavailable"
    assert result.level_rows == 0
    conn = store.connect(tmp_path)
    try:
        assert conn.execute(
            "SELECT status,comparison_status FROM l2_anchor_status "
            "WHERE market_id=?",
            (result.market_id,),
        ).fetchone() == ("unavailable", "unknown")
    finally:
        conn.close()


def _orphan_raw(
    root: Path, fetch: AnchorFetch, *, request_id: str,
    ingest_time: str,
) -> Path:
    request_url = _request_url(fetch.endpoint, fetch.venue_symbol)
    canonical_fetch = AnchorFetch(
        fetch.endpoint, fetch.venue_symbol, request_url,
        hashlib.sha256(f"GET\n{request_url}\n".encode()).hexdigest(),
        fetch.requested_at, fetch.response_received_at, fetch.http_status,
        fetch.response_body, fetch.error_kind, fetch.error_detail,
    )
    path, _, _ = _persist_raw(
        root, canonical_fetch, "periodic", None, ingest_time, request_id,
    )
    return path


def test_recover_raw_anchor_rejects_response_hash_mismatch(
    tmp_path: Path,
) -> None:
    body = (
        b'{"success":1,"data":{"asks":[["101","2"]],'
        b'"bids":[["100","3"]],"timestamp":1786579200100,'
        b'"sequenceId":"1"}}'
    )
    path = _orphan_raw(
        tmp_path, _fetch("bitbank", "btc_jpy", body),
        request_id="bad-response-hash",
        ingest_time="2026-08-13T00:00:00.300+00:00",
    )
    record = json.loads(path.read_text(encoding="utf-8"))
    record["response_sha256"] = "0" * 64
    encoded = (
        json.dumps(
            record, ensure_ascii=False, separators=(",", ":"), sort_keys=True,
        ) + "\n"
    ).encode()
    bad_sha = hashlib.sha256(encoded).hexdigest()
    bad_path = path.with_name(f"sha256-{bad_sha}.json")
    bad_path.write_bytes(encoded)

    try:
        recover_raw_anchor(tmp_path, bad_path)
    except ValueError as exc:
        assert str(exc) == "REST 锚点 raw 响应散列不匹配"
    else:
        raise AssertionError("坏响应散列必须拒绝恢复")
    assert not (tmp_path / "guvolu.sqlite3").exists()


def test_recover_raw_anchor_rejects_filename_hash_mismatch(
    tmp_path: Path,
) -> None:
    body = (
        b'{"success":1,"data":{"asks":[["101","2"]],'
        b'"bids":[["100","3"]],"timestamp":1786579200100,'
        b'"sequenceId":"1"}}'
    )
    path = _orphan_raw(
        tmp_path, _fetch("bitbank", "btc_jpy", body),
        request_id="bad-filename-hash",
        ingest_time="2026-08-13T00:00:00.300+00:00",
    )
    bad_path = path.with_name(f"sha256-{'0' * 64}.json")
    bad_path.write_bytes(path.read_bytes())

    try:
        recover_raw_anchor(tmp_path, bad_path)
    except ValueError as exc:
        assert str(exc) == "REST 锚点 raw 文件名散列不匹配"
    else:
        raise AssertionError("坏文件名散列必须拒绝恢复")
    assert not (tmp_path / "guvolu.sqlite3").exists()


def test_recover_raw_anchor_is_complete_and_idempotent(
    tmp_path: Path,
) -> None:
    body = json.dumps({
        "status": 0,
        "data": {
            "symbol": "BTC",
            "asks": [{"price": "101", "size": "2"}],
            "bids": [{"price": "100", "size": "3"}],
        },
        "responsetime": "2026-08-13T00:00:00.100Z",
    }, separators=(",", ":")).encode()
    path = _orphan_raw(
        tmp_path, _fetch("gmo", "BTC", body), request_id="recover-once",
        ingest_time="2026-08-13T00:00:00.300+00:00",
    )
    before = path.read_bytes()

    first = recover_raw_anchor(tmp_path, path)
    second = recover_raw_anchor(tmp_path, path)

    assert first.outcome == "recovered"
    assert first.materialization is not None
    assert first.materialization.level_rows == 2
    assert second.outcome == "already_recovered"
    assert second.anchor_attempt_id == first.anchor_attempt_id
    assert second.reconciliation_attempt_id == first.reconciliation_attempt_id
    assert path.read_bytes() == before
    conn = store.connect(tmp_path)
    try:
        assert conn.execute(
            "SELECT COUNT(*) FROM partition_attempt WHERE attempt_id IN (?,?)",
            (first.anchor_attempt_id, first.reconciliation_attempt_id),
        ).fetchone()[0] == 2
        comparison = conn.execute(
            "SELECT comparison_status,reason FROM l2_anchor_status"
        ).fetchone()
        assert comparison == (
            "unknown", "恢复原件没有可证明的 PIT WS checkpoint",
        )
    finally:
        conn.close()


def test_recover_old_raw_does_not_use_future_ws_or_replace_heads(
    tmp_path: Path,
) -> None:
    market_id, ws_attempt = _install_checkpoint(
        tmp_path,
        available_time="2026-08-13T00:10:00.150+00:00",
        bid=("100", "3"), ask=("101", "2"),
    )
    body = json.dumps({
        "status": 0,
        "data": {
            "symbol": "BTC",
            "asks": [{"price": "101", "size": "2"}],
            "bids": [{"price": "100", "size": "3"}],
        },
        "responsetime": "2026-08-13T00:10:00.100Z",
    }, separators=(",", ":")).encode()
    current = persist_anchor_fetch(
        tmp_path, _fetch("gmo", "BTC", body), trigger_reason="periodic",
        connection_id=None, request_id="current-anchor",
        ingest_time="2026-08-13T00:10:00.300+00:00",
    )
    old_body = body.replace(b"00:10:00.100Z", b"00:00:00.100Z")
    old_path = _orphan_raw(
        tmp_path, _fetch("gmo", "BTC", old_body),
        request_id="old-orphan",
        ingest_time="2026-08-13T00:00:00.300+00:00",
    )

    recovered = recover_raw_anchor(tmp_path, old_path)

    conn = store.connect(tmp_path)
    try:
        heads = dict(conn.execute(
            "SELECT domain,attempt_id FROM materialization_partition_head "
            "WHERE market_id=? AND domain IN "
            "('book_state','book_l2_anchor','book_l2_anchor_reconciliation')",
            (market_id,),
        ).fetchall())
        summary = conn.execute(
            "SELECT anchor_attempt_id,reconciliation_attempt_id,"
            "ws_checkpoint_attempt_id FROM l2_anchor_status WHERE market_id=?",
            (market_id,),
        ).fetchone()
        recovered_comparison = conn.execute(
            "SELECT a.storage_path FROM materialization_output o "
            "JOIN artifact a ON a.artifact_id=o.artifact_id "
            "WHERE o.attempt_id=? AND o.dataset=?",
            (recovered.reconciliation_attempt_id,
             "book_l2_anchor_reconciliation"),
        ).fetchone()
    finally:
        conn.close()
    db: Any = duckdb.connect(":memory:")
    try:
        comparison = db.execute(
            "SELECT comparison_basis,ws_checkpoint_attempt_id "
            "FROM read_parquet(?)",
            [str(tmp_path / recovered_comparison[0])],
        ).fetchone()
    finally:
        db.close()
    assert heads == {
        "book_state": ws_attempt,
        "book_l2_anchor": current.anchor_attempt_id,
        "book_l2_anchor_reconciliation": current.reconciliation_attempt_id,
    }
    assert summary == (
        current.anchor_attempt_id, current.reconciliation_attempt_id,
        ws_attempt,
    )
    assert comparison == ("recovery_no_pit_ws_checkpoint", None)


def test_recover_equal_time_raw_preserves_existing_heads_and_status(
    tmp_path: Path,
) -> None:
    market_id, ws_attempt = _install_checkpoint(
        tmp_path,
        available_time="2026-08-13T00:10:00.300+00:00",
        bid=("100", "3"), ask=("101", "2"),
    )
    body = json.dumps({
        "status": 0,
        "data": {
            "symbol": "BTC",
            "asks": [{"price": "101", "size": "2"}],
            "bids": [{"price": "100", "size": "3"}],
        },
        "responsetime": "2026-08-13T00:10:00.100Z",
    }, separators=(",", ":")).encode()
    current = persist_anchor_fetch(
        tmp_path, _fetch("gmo", "BTC", body), trigger_reason="periodic",
        connection_id=None, request_id="equal-current",
        ingest_time="2026-08-13T00:10:00.300+00:00",
    )
    recovery_body = body.replace(
        b'"price":"101"', b'"price":"103"'
    ).replace(b'"price":"100"', b'"price":"98"')
    orphan = _orphan_raw(
        tmp_path, _fetch("gmo", "BTC", recovery_body),
        request_id="equal-recovery",
        ingest_time="2026-08-13T00:10:00.300+00:00",
    )

    recovered = recover_raw_anchor(tmp_path, orphan)
    repeated = recover_raw_anchor(tmp_path, orphan)

    conn = store.connect(tmp_path)
    try:
        heads = dict(conn.execute(
            "SELECT domain,attempt_id FROM materialization_partition_head "
            "WHERE market_id=? AND domain IN "
            "('book_state','book_l2_anchor','book_l2_anchor_reconciliation')",
            (market_id,),
        ).fetchall())
        summary = conn.execute(
            "SELECT anchor_attempt_id,reconciliation_attempt_id,"
            "ws_checkpoint_attempt_id FROM l2_anchor_status WHERE market_id=?",
            (market_id,),
        ).fetchone()
        recovered_statuses = conn.execute(
            "SELECT status FROM partition_attempt WHERE attempt_id IN (?,?) "
            "ORDER BY attempt_id",
            (recovered.anchor_attempt_id,
             recovered.reconciliation_attempt_id),
        ).fetchall()
    finally:
        conn.close()
    assert recovered.anchor_attempt_id != current.anchor_attempt_id
    assert repeated.outcome == "already_recovered"
    assert heads == {
        "book_state": ws_attempt,
        "book_l2_anchor": current.anchor_attempt_id,
        "book_l2_anchor_reconciliation": current.reconciliation_attempt_id,
    }
    assert summary == (
        current.anchor_attempt_id, current.reconciliation_attempt_id,
        ws_attempt,
    )
    assert recovered_statuses == [("complete",), ("complete",)]


def test_normal_older_anchor_fact_does_not_replace_current_state(
    tmp_path: Path,
) -> None:
    current_body = json.dumps({
        "status": 0,
        "data": {
            "symbol": "BTC",
            "asks": [{"price": "101", "size": "2"}],
            "bids": [{"price": "100", "size": "3"}],
        },
        "responsetime": "2026-08-13T00:10:00.100Z",
    }, separators=(",", ":")).encode()
    current = persist_anchor_fetch(
        tmp_path, _fetch("gmo", "BTC", current_body),
        trigger_reason="periodic", connection_id=None,
        request_id="ordered-current",
        ingest_time="2026-08-13T00:10:00.300+00:00",
    )
    older_body = current_body.replace(
        b"00:10:00.100Z", b"00:00:00.100Z"
    ).replace(b'"price":"101"', b'"price":"104"')
    older = persist_anchor_fetch(
        tmp_path, _fetch("gmo", "BTC", older_body),
        trigger_reason="periodic", connection_id=None,
        request_id="ordered-older",
        ingest_time="2026-08-13T00:00:00.300+00:00",
    )

    conn = store.connect(tmp_path)
    try:
        heads = dict(conn.execute(
            "SELECT domain,attempt_id FROM materialization_partition_head "
            "WHERE market_id=? AND domain IN "
            "('book_l2_anchor','book_l2_anchor_reconciliation')",
            (current.market_id,),
        ).fetchall())
        summary = conn.execute(
            "SELECT anchor_attempt_id,reconciliation_attempt_id "
            "FROM l2_anchor_status WHERE market_id=?",
            (current.market_id,),
        ).fetchone()
        older_statuses = conn.execute(
            "SELECT status FROM partition_attempt WHERE attempt_id IN (?,?) "
            "ORDER BY attempt_id",
            (older.anchor_attempt_id, older.reconciliation_attempt_id),
        ).fetchall()
    finally:
        conn.close()
    assert heads == {
        "book_l2_anchor": current.anchor_attempt_id,
        "book_l2_anchor_reconciliation": current.reconciliation_attempt_id,
    }
    assert summary == (
        current.anchor_attempt_id, current.reconciliation_attempt_id,
    )
    assert older_statuses == [("complete",), ("complete",)]


class _Response:
    status_code = 200
    content = b"{}"


class _Session:
    def __init__(self) -> None:
        self.urls: list[str] = []

    def get(self, url: str, *, timeout: float) -> _Response:
        assert timeout == 10.0
        self.urls.append(url)
        return _Response()


def test_anchor_retries_same_publication_after_writer_lock_timeout(
    tmp_path: Path, monkeypatch: Any,
) -> None:
    fetch = _fetch("bitflyer", "BTC_JPY", (
        b'{"mid_price":100.5,"bids":[{"price":100.0,"size":3.25}],'
        b'"asks":[{"price":101.0,"size":2.5}]}'
    ))
    original = book_l2_anchor._register_attempts
    publications: list[tuple[Path, Path, Path, Path]] = []

    def flaky_register(*args: object, **kwargs: Any) -> AnchorMaterializationResult:
        publications.append((
            kwargs["observation_path"], kwargs["level_path"],
            kwargs["reconciliation_path"], kwargs["manifest_path"],
        ))
        if len(publications) == 1:
            raise TimeoutError("fixture writer lock")
        return original(*args, **kwargs)

    monkeypatch.setattr(book_l2_anchor, "_register_attempts", flaky_register)
    result = persist_anchor_fetch(
        tmp_path, fetch, trigger_reason="periodic", connection_id=None,
        ingest_time="2026-08-13T00:00:00.300000+00:00",
    )

    assert len(publications) == 2
    assert publications[0] == publications[1]
    assert result.anchor_attempt_id.startswith("book-l2-anchor-")
    conn = store.connect_readonly(tmp_path)
    assert conn is not None
    try:
        assert conn.execute(
            "SELECT COUNT(*) FROM partition_attempt WHERE attempt_id IN (?,?)",
            (result.anchor_attempt_id, result.reconciliation_attempt_id),
        ).fetchone()[0] == 2
    finally:
        conn.close()


def test_anchor_retries_connect_without_refetching_or_rewriting_raw(
    tmp_path: Path, monkeypatch: Any,
) -> None:
    fetch = _fetch("gmo", "BTC", (
        b'{"data":{"bids":[{"price":"100","size":"1"}],'
        b'"asks":[{"price":"101","size":"2"}]}}'
    ))
    original_connect = store.connect
    connect_calls = 0
    raw_calls = 0
    original_persist_raw = book_l2_anchor._persist_raw

    def connect_after_timeout(root: Path) -> Any:
        nonlocal connect_calls
        connect_calls += 1
        if connect_calls == 1:
            raise TimeoutError("fixture connect lock")
        return original_connect(root)

    def observed_persist_raw(*args: object, **kwargs: object) -> Any:
        nonlocal raw_calls
        raw_calls += 1
        return original_persist_raw(*args, **kwargs)

    monkeypatch.setattr(store, "connect", connect_after_timeout)
    monkeypatch.setattr(book_l2_anchor, "_persist_raw", observed_persist_raw)

    result = persist_anchor_fetch(
        tmp_path, fetch, trigger_reason="periodic", connection_id=None,
        ingest_time="2026-08-13T00:00:00.300000+00:00",
    )

    assert connect_calls == 2
    assert raw_calls == 1
    assert result.anchor_attempt_id.startswith("book-l2-anchor-")


class _TimeoutSession:
    def get(self, url: str, *, timeout: float) -> _Response:
        raise requests.Timeout("fixture")


def test_public_adapter_and_trigger_queue_are_bounded() -> None:
    session = _Session()
    adapter = PublicRestAnchorAdapter(
        BITBANK_ENDPOINT, session=session, sleeper=lambda _: None,
    )
    fetched = adapter.fetch("btc_jpy")
    assert fetched.request_url == "https://public.bitbank.cc/btc_jpy/depth"
    assert fetched.http_status == 200
    assert fetched.response_sha256 == hashlib.sha256(b"{}").hexdigest()

    timeout = PublicRestAnchorAdapter(
        BITFLYER_ENDPOINT, session=_TimeoutSession(), sleeper=lambda _: None,
    ).fetch("BTC_JPY")
    assert timeout.http_status is None
    assert timeout.error_kind == "timeout"
    assert timeout.response_sha256 is None

    worker = RestAnchorWorker(Path("."), "gmo", "BTC")
    assert all(
        worker.submit(f"c{index}", "reconnect")
        for index in range(ANCHOR_QUEUE_SIZE)
    )
    assert worker.submit("overflow", "reconnect") is False
    assert worker.enqueued == ANCHOR_QUEUE_SIZE
    assert worker.dropped == 1
    assert worker.completed == 0
    assert worker.failed == 0

    stats = l2_capture.CaptureStats("gmo", "BTC")
    reasons: list[str] = []

    def submit(_: str | None, reason: str) -> bool:
        reasons.append(reason)
        return True

    stats.successful_sessions = 1
    l2_capture._trigger_anchor(submit, stats, "c1")
    stats.successful_sessions = 2
    l2_capture._trigger_anchor(submit, stats, "c2")
    assert reasons == ["connection_open", "reconnect"]
    assert stats.anchor_triggers_enqueued == 2


def test_worker_settlement_callback_runs_on_event_loop(
    tmp_path: Path, monkeypatch: Any,
) -> None:
    event_loop_thread = threading.get_ident()
    worker_threads: list[int] = []
    settlements: list[tuple[int, int, int]] = []

    def capture(*args: object, **kwargs: object) -> AnchorMaterializationResult:
        worker_threads.append(threading.get_ident())
        if kwargs["connection_id"] == "fail":
            raise ValueError("fixture failure")
        return AnchorMaterializationResult(
            observation_id="observation", market_id="market",
            status="unavailable", comparison_status="unknown",
            raw_artifact_id="raw", observation_artifact_id="observation-artifact",
            level_artifact_id="level", reconciliation_artifact_id="reconciliation",
            anchor_attempt_id="anchor",
            reconciliation_attempt_id="reconciliation-attempt",
            level_rows=0,
        )

    def settled(completed: int, failed: int) -> None:
        settlements.append((completed, failed, threading.get_ident()))

    monkeypatch.setattr(
        "guvolu.data.book_l2_anchor.capture_and_persist_anchor", capture,
    )

    async def exercise() -> RestAnchorWorker:
        worker = RestAnchorWorker(
            tmp_path, "gmo", "BTC", on_settled=settled,
        )
        worker.start()
        assert worker.submit("complete", "connection_open")
        assert worker.submit("fail", "reconnect")
        await worker.close()
        return worker

    worker = asyncio.run(exercise())
    assert worker_threads
    assert all(thread_id != event_loop_thread for thread_id in worker_threads)
    assert settlements == [
        (1, 0, event_loop_thread),
        (1, 1, event_loop_thread),
    ]
    assert worker.completed == 1
    assert worker.failed == 1


def test_anchor_settlement_immediately_refreshes_checkpoint(
    tmp_path: Path,
) -> None:
    writer = l2_capture.SegmentedRawWriter(
        tmp_path, "gmo", "BTC", run_id="run-anchor-stats",
        endpoint_id="EP-0007", endpoint_revision=0,
    )
    stats = l2_capture.CaptureStats("gmo", "BTC")
    stats.anchor_triggers_enqueued = 3
    stats.anchor_triggers_dropped = 1

    l2_capture._checkpoint_anchor_settlement(writer, stats, 1, 1)

    body = json.loads((writer.directory / "checkpoint.json").read_text())
    assert body["anchor_completed"] == 1
    assert body["anchor_failed"] == 1
    assert body["anchor_triggers_enqueued"] == 3
    assert body["anchor_triggers_dropped"] == 1
    writer.finish()


def test_periodic_anchor_loop_enqueues_without_connection_identity() -> None:
    stats = l2_capture.CaptureStats("gmo", "BTC")
    observed: list[tuple[str | None, str]] = []

    async def exercise() -> None:
        deadline = time.monotonic() + 0.035

        def submit(connection_id: str | None, reason: str) -> bool:
            observed.append((connection_id, reason))
            return True

        await l2_capture._periodic_anchor_loop(
            submit, stats, deadline, interval_seconds=0.01,
        )

    asyncio.run(exercise())
    assert observed
    assert set(observed) == {(None, "periodic")}
    assert stats.anchor_triggers_enqueued == len(observed)


def test_v20_schema_is_pure_append_from_v19_shape(tmp_path: Path) -> None:
    conn = store.connect(tmp_path)
    conn.execute("DROP TABLE l2_anchor_status")
    conn.execute("PRAGMA user_version=19")
    before = {
        table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        for table in ("venue", "instrument", "market", "artifact")
    }
    conn.commit()
    conn.close()

    migrated = store.connect(tmp_path)
    try:
        after = {
            table: migrated.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0]
            for table in before
        }
        assert after == before
        assert migrated.execute("PRAGMA user_version").fetchone()[0] == 20
        assert migrated.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name='l2_anchor_status'"
        ).fetchone() == ("l2_anchor_status",)
        assert migrated.execute("PRAGMA foreign_key_check").fetchall() == []
        assert migrated.execute("PRAGMA quick_check").fetchone() == ("ok",)
    finally:
        migrated.close()
