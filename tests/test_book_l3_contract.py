"""L3/MBO 列契约与逻辑事件身份测试。"""
from __future__ import annotations

from datetime import UTC, datetime

import duckdb
import pytest

from guvolu.data.book_l3_contract import (
    BOOK_L3_NORMALIZATION_VERSION,
    BOOK_L3_ORDER_EVENT_DATASET,
    BOOK_L3_SCHEMA_VERSION,
    EVENT_EVIDENCE_COLUMNS,
    EVENT_EVIDENCE_DATASET,
    EVENT_EVIDENCE_PRIMARY_KEY,
    MATCH_LINK_COLUMNS,
    MATCH_LINK_DATASET,
    MATCH_LINK_PRIMARY_KEY,
    ORDER_EVENT_COLUMNS,
    ORDER_EVENT_PRIMARY_KEY,
    STATE_CHECKPOINT_COLUMNS,
    STATE_CHECKPOINT_DATASET,
    STATE_CHECKPOINT_PRIMARY_KEY,
    create_book_l3_tables,
    make_checkpoint_key,
    make_event_evidence_key,
    make_match_link_key,
    make_source_event_key,
    validate_event_evidence,
    validate_match_link,
    validate_order_event,
    validate_state_checkpoint,
)


def test_source_event_key_is_deterministic_and_scope_explicit() -> None:
    assert ORDER_EVENT_PRIMARY_KEY == (
        "market_id", "source_event_key", "normalization_version"
    )
    first = make_source_event_key(
        endpoint_id="EP-0042", endpoint_revision=3,
        market_id="mkt__kraken__xbt_usd__r0",
        identity_scope="connection/channel",
        identity_basis="native_sequence",
        identity_parts=("book-session-9", 81234, 2),
    )
    assert first == make_source_event_key(
        endpoint_id="EP-0042", endpoint_revision=3,
        market_id="mkt__kraken__xbt_usd__r0",
        identity_scope="connection/channel",
        identity_basis="native_sequence",
        identity_parts=("book-session-9", 81234, 2),
    )
    assert first.startswith("l3evt-sha256-") and len(first) == 77
    assert first != make_source_event_key(
        endpoint_id="EP-0042", endpoint_revision=3,
        market_id="mkt__kraken__xbt_usd__r0",
        identity_scope="connection/channel",
        identity_basis="native_sequence",
        identity_parts=("book-session-9", 81234, 3),
    )
    with pytest.raises(ValueError, match="basis"):
        make_source_event_key(
            endpoint_id="EP-0042", endpoint_revision=3,
            market_id="mkt__kraken__xbt_usd__r0",
            identity_scope="connection/channel",
            identity_basis="synthetic_l2",
            identity_parts=("book-session-9", 81234, 2),
        )


def test_duckdb_temp_tables_match_declared_column_order() -> None:
    db = duckdb.connect(":memory:")
    create_book_l3_tables(db)
    assert tuple(
        row[1] for row in db.execute(
            f"PRAGMA table_info('{BOOK_L3_ORDER_EVENT_DATASET}')"
        ).fetchall()
    ) == ORDER_EVENT_COLUMNS
    primary_key = db.execute(
        "SELECT constraint_column_names FROM duckdb_constraints() "
        "WHERE table_name=? AND constraint_type='PRIMARY KEY'",
        [BOOK_L3_ORDER_EVENT_DATASET],
    ).fetchone()
    assert primary_key is not None
    assert tuple(primary_key[0]) == ORDER_EVENT_PRIMARY_KEY
    assert tuple(
        row[1] for row in db.execute(
            f"PRAGMA table_info('{EVENT_EVIDENCE_DATASET}')"
        ).fetchall()
    ) == EVENT_EVIDENCE_COLUMNS
    assert tuple(
        row[1] for row in db.execute(
            f"PRAGMA table_info('{MATCH_LINK_DATASET}')"
        ).fetchall()
    ) == MATCH_LINK_COLUMNS
    assert tuple(
        row[1] for row in db.execute(
            f"PRAGMA table_info('{STATE_CHECKPOINT_DATASET}')"
        ).fetchall()
    ) == STATE_CHECKPOINT_COLUMNS
    assert MATCH_LINK_DATASET == "book_l3_match_link"
    assert STATE_CHECKPOINT_DATASET == "book_l3_state_checkpoint"
    keys = {
        str(row[0]): tuple(row[1])
        for row in db.execute(
            "SELECT table_name,constraint_column_names "
            "FROM duckdb_constraints() WHERE constraint_type='PRIMARY KEY'"
        ).fetchall()
    }
    assert keys[EVENT_EVIDENCE_DATASET] == EVENT_EVIDENCE_PRIMARY_KEY
    assert keys[MATCH_LINK_DATASET] == MATCH_LINK_PRIMARY_KEY
    assert keys[STATE_CHECKPOINT_DATASET] == STATE_CHECKPOINT_PRIMARY_KEY
    db.close()


def test_order_event_validator_rejects_l2_level_updates() -> None:
    received = datetime(2026, 8, 13, 0, 0, tzinfo=UTC)
    available = datetime(2026, 8, 13, 0, 0, 1, tzinfo=UTC)
    source_artifact_id = "sha256-" + "a" * 64
    evidence_key = make_event_evidence_key(
        source_artifact_id=source_artifact_id,
        source_row_index=1,
        source_item_index=0,
    )
    row: dict[str, object] = {name: None for name in ORDER_EVENT_COLUMNS}
    row.update({
        "source_event_key": make_source_event_key(
                endpoint_id="EP-0042", endpoint_revision=3,
                market_id="mkt__kraken__xbt_usd__r0",
                identity_scope="connection/channel",
                identity_basis="native_event_id", identity_parts=("event-1",),
        ),
        "source_event_key_basis": "native_event_id",
        "selected_evidence_key": evidence_key,
        "market_id": "mkt__kraken__xbt_usd__r0",
        "venue_id": "kraken", "native_symbol": "BTC/USD",
        "mapping_revision": 0, "instrument_id": "SPOT:BTC/USD",
        "source_level": "A", "endpoint_id": "EP-0042",
        "endpoint_revision": 3, "capability_revision": 1,
        "connection_id": "run-1-c000001", "channel_id": "book",
        "sequence_domain": "connection/channel",
        "source_schema_revision": "kraken-ws-v2-level3@2026-08-13",
        "native_order_id": "order-1", "order_id_scope": "market",
        "event_type": "ADD",
        "native_event_index": 0, "side": "buy", "price_unit": "USD/BTC",
        "native_qty": "0.5", "native_qty_unit": "BTC", "qty": "0.5",
        "qty_unit": "BTC", "quantity_basis": "base_asset",
        "qty_semantics": "absolute_resting",
        "priority_origin": "native", "priority_effect": "retained",
        "priority_policy_revision": "kraken-level3-priority-v1",
        "is_snapshot": False,
        "checksum": "123", "checksum_algorithm": "crc32",
        "checksum_scope": "top10", "checksum_status": "passed",
        "visibility_flags": "[]", "quality_flags": "[]",
        "data_quality": "verified",
        "event_time": received, "recv_time_utc": received,
        "available_time": available, "ingest_time": received,
        "normalization_version": BOOK_L3_NORMALIZATION_VERSION,
        "schema_version": BOOK_L3_SCHEMA_VERSION,
    })
    validate_order_event(row)
    row["event_type"] = "SET_LEVEL"
    with pytest.raises(ValueError, match="lifecycle"):
        validate_order_event(row)
    row["event_type"] = "SNAPSHOT_ORDER"
    row["is_snapshot"] = True
    validate_order_event(row)
    row["event_type"] = "DELETE"
    row["is_snapshot"] = False
    row["qty_semantics"] = "removed_unknown"
    validate_order_event(row)
    row["event_type"] = "OUT_OF_SCOPE"
    validate_order_event(row)


def test_event_evidence_keeps_alternate_raw_observations() -> None:
    event_key = make_source_event_key(
        endpoint_id="EP-0025", endpoint_revision=0,
        market_id="mkt__coinbase__btc_usd__r0",
        identity_scope="coinbase-exchange-global",
        identity_basis="native_sequence", identity_parts=("full", 100, 0),
    )
    when = datetime(2026, 8, 13, tzinfo=UTC)
    rows: list[dict[str, object]] = []
    for digest, row_index in (("a", 7), ("b", 9)):
        artifact_id = "sha256-" + digest * 64
        evidence_key = make_event_evidence_key(
            source_artifact_id=artifact_id,
            source_row_index=row_index,
            source_item_index=0,
        )
        row: dict[str, object] = {
            name: None for name in EVENT_EVIDENCE_COLUMNS
        }
        row.update({
            "evidence_key": evidence_key,
            "market_id": "mkt__coinbase__btc_usd__r0",
            "source_event_key": event_key, "endpoint_id": "EP-0025",
            "endpoint_revision": 0, "capability_revision": 1,
            "connection_id": f"run-{digest}-c000001",
            "channel_id": "full:BTC-USD",
            "sequence_domain": "coinbase-exchange-global",
            "source_schema_revision": "coinbase-full@2026-08-13",
            "source_artifact_id": artifact_id,
            "source_row_index": row_index, "source_item_index": 0,
            "raw_payload_sha256": digest * 64,
            "recv_time_utc": when, "available_time": when,
            "ingest_time": when, "data_quality": "verified",
            "quality_flags": "[]",
            "normalization_version": BOOK_L3_NORMALIZATION_VERSION,
            "schema_version": BOOK_L3_SCHEMA_VERSION,
        })
        validate_event_evidence(row)
        rows.append(row)
    assert rows[0]["source_event_key"] == rows[1]["source_event_key"]
    assert rows[0]["evidence_key"] != rows[1]["evidence_key"]


def test_match_and_checkpoint_keys_freeze_identity_scope() -> None:
    match = make_match_link_key(
        market_id="mkt__coinbase__btc_usd__r0",
        endpoint_id="EP-0025", endpoint_revision=0,
        identity_scope="coinbase-exchange-global",
        identity_parts=("match", 101, "maker-1", "taker-1"),
    )
    assert match.startswith("l3match-sha256-")
    event_key = "l3evt-sha256-" + "c" * 64
    evidence_key = "l3evi-sha256-" + "d" * 64
    when = datetime(2026, 8, 13, tzinfo=UTC)
    match_row: dict[str, object] = {name: None for name in MATCH_LINK_COLUMNS}
    match_row.update({
        "match_link_key": match,
        "market_id": "mkt__coinbase__btc_usd__r0", "venue_id": "coinbase",
        "native_symbol": "BTC-USD", "mapping_revision": 0,
        "instrument_id": "SPOT:BTC/USD", "source_level": "A",
        "endpoint_id": "EP-0025", "endpoint_revision": 0,
        "capability_revision": 1, "connection_id": "run-1-c000001",
        "channel_id": "full:BTC-USD",
        "sequence_domain": "coinbase-exchange-global",
        "source_schema_revision": "coinbase-full@2026-08-13",
        "source_event_key": event_key, "selected_evidence_key": evidence_key,
        "native_match_id": "match-101", "native_sequence": "101",
        "maker_order_id": "maker-1", "maker_order_id_scope": "market",
        "taker_order_id": "taker-1", "taker_order_id_scope": "market",
        "resting_order_id": "maker-1", "resting_order_id_scope": "market",
        "aggressor_side": "buy", "price": "60000", "price_unit": "USD/BTC",
        "qty": "0.1", "qty_unit": "BTC", "quantity_basis": "base_asset",
        "qty_semantics": "executed", "event_time": when,
        "data_quality": "verified", "quality_flags": "[]",
        "normalization_version": BOOK_L3_NORMALIZATION_VERSION,
        "schema_version": BOOK_L3_SCHEMA_VERSION,
    })
    validate_match_link(match_row)
    checkpoint = make_checkpoint_key(
        market_id="mkt__coinbase__btc_usd__r0",
        connection_id="run-1-c000001",
        sequence_domain="coinbase-exchange-global",
        through_source_event_key=event_key,
        derivation_method_version="book-l3-checkpoint-v1",
    )
    assert checkpoint.startswith("l3ckpt-sha256-")
    checkpoint_row: dict[str, object] = {
        name: None for name in STATE_CHECKPOINT_COLUMNS
    }
    checkpoint_row.update({
        "checkpoint_key": checkpoint,
        "market_id": "mkt__coinbase__btc_usd__r0", "venue_id": "coinbase",
        "native_symbol": "BTC-USD", "mapping_revision": 0,
        "instrument_id": "SPOT:BTC/USD", "source_level": "A",
        "endpoint_id": "EP-0025", "endpoint_revision": 0,
        "capability_revision": 1, "connection_id": "run-1-c000001",
        "channel_id": "full:BTC-USD",
        "sequence_domain": "coinbase-exchange-global",
        "source_schema_revision": "coinbase-full@2026-08-13",
        "through_source_event_key": event_key, "native_sequence": "101",
        "checkpoint_time": when, "available_time": when,
        "order_count": 3, "bid_order_count": 2, "ask_order_count": 1,
        "depth_limit": None, "completeness": "complete_public_book",
        "state_sha256": "e" * 64, "checksum_status": "not_available",
        "visibility_flags": "[]", "data_quality": "verified",
        "quality_flags": "[]", "source_input_set_hash": "f" * 64,
        "derivation_method_version": "book-l3-checkpoint-v1",
        "priority_policy_revision": "coinbase-full-priority-v1",
        "normalization_version": BOOK_L3_NORMALIZATION_VERSION,
        "schema_version": BOOK_L3_SCHEMA_VERSION,
    })
    validate_state_checkpoint(checkpoint_row)
    checkpoint_row["order_count"] = 4
    with pytest.raises(ValueError, match="counts"):
        validate_state_checkpoint(checkpoint_row)
