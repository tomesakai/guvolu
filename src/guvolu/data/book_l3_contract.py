"""可回放 L3/MBO 订单生命周期的列契约。

高频事实写 Parquet；SQLite 只登记端点、连接、频道与制品血缘。逻辑事件、
原始观察证据、成交关联和状态检查点分别建表。一个逻辑事件可以有多份原件
证据；选中证据被冻结在该 normalization_version 内，不能因后到观察而原地
覆盖。L2 档位与逐笔交易不能合成此契约。
"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

from guvolu.data.source_contract import validate_endpoint_id

BOOK_L3_SCHEMA_VERSION = 1
BOOK_L3_NORMALIZATION_VERSION = "book-l3-normalization-v1"

BOOK_L3_ORDER_EVENT_DATASET = "book_l3_order_event"
EVENT_EVIDENCE_DATASET = "book_l3_event_evidence"
MATCH_LINK_DATASET = "book_l3_match_link"
STATE_CHECKPOINT_DATASET = "book_l3_state_checkpoint"

L3_EVENT_TYPES = frozenset({
    "SNAPSHOT_ORDER", "ADD", "AMEND", "CANCEL", "EXECUTE", "DELETE",
    "OUT_OF_SCOPE", "RESET",
})
SOURCE_EVENT_KEY_BASES = frozenset({
    "native_event_id", "native_sequence", "raw_observation",
})
QTY_SEMANTICS = frozenset({
    "absolute_resting", "delta_resting", "remaining",
    "cancelled", "executed", "removed_unknown", "not_applicable",
})
EVENT_QTY_SEMANTICS: dict[str, frozenset[str]] = {
    "SNAPSHOT_ORDER": frozenset({"absolute_resting"}),
    "ADD": frozenset({"absolute_resting", "delta_resting"}),
    "AMEND": frozenset({"absolute_resting", "delta_resting", "remaining"}),
    "CANCEL": frozenset({"cancelled"}),
    "EXECUTE": frozenset({"executed"}),
    "DELETE": frozenset({"removed_unknown"}),
    "OUT_OF_SCOPE": frozenset({"removed_unknown", "not_applicable"}),
    "RESET": frozenset({"not_applicable"}),
}
PRIORITY_ORIGINS = frozenset({"native", "reconstructed", "unknown"})
PRIORITY_EFFECTS = frozenset({"retained", "lost", "unknown", "not_applicable"})
DATA_QUALITY_VALUES = frozenset({"verified", "valid_unchecked", "degraded"})
SOURCE_LEVELS = frozenset({"A", "B"})
CHECKSUM_STATUSES = frozenset({
    "passed", "failed", "not_available", "not_checked",
})

ORDER_EVENT_PRIMARY_KEY = (
    "market_id", "source_event_key", "normalization_version",
)
EVENT_EVIDENCE_PRIMARY_KEY = (
    "market_id", "source_event_key", "normalization_version", "evidence_key",
)
MATCH_LINK_PRIMARY_KEY = (
    "market_id", "match_link_key", "normalization_version",
)
STATE_CHECKPOINT_PRIMARY_KEY = (
    "market_id", "checkpoint_key", "normalization_version",
)

ORDER_EVENT_COLUMNS: tuple[str, ...] = (
    "source_event_key", "source_event_key_basis", "selected_evidence_key",
    "market_id", "venue_id", "native_symbol", "mapping_revision",
    "instrument_id", "source_level", "endpoint_id", "endpoint_revision",
    "capability_revision", "connection_id", "channel_id", "sequence_domain",
    "source_schema_revision", "event_type", "native_order_id",
    "order_id_scope", "native_sequence", "previous_native_sequence",
    "native_event_index", "side", "price", "price_unit", "native_qty",
    "native_qty_unit", "qty", "qty_unit", "quantity_basis", "qty_semantics",
    "previous_qty", "remaining_qty", "priority_key", "priority_time",
    "priority_origin", "priority_effect", "priority_policy_revision",
    "is_snapshot", "checkpoint_key", "event_time", "source_publish_time",
    "recv_time_utc", "recv_time_mono_ns", "available_time", "ingest_time",
    "checksum", "checksum_algorithm", "checksum_scope", "checksum_status",
    "visibility_flags", "data_quality", "quality_flags",
    "normalization_version", "schema_version",
)

EVENT_EVIDENCE_COLUMNS: tuple[str, ...] = (
    "evidence_key", "market_id", "source_event_key", "endpoint_id",
    "endpoint_revision", "capability_revision", "connection_id", "channel_id",
    "sequence_domain", "source_schema_revision", "source_artifact_id",
    "source_row_index", "source_item_index", "raw_payload_sha256", "recv_time_utc",
    "recv_time_mono_ns", "available_time", "ingest_time", "data_quality",
    "quality_flags", "normalization_version", "schema_version",
)

MATCH_LINK_COLUMNS: tuple[str, ...] = (
    "match_link_key", "market_id", "venue_id", "native_symbol",
    "mapping_revision", "instrument_id", "source_level", "endpoint_id",
    "endpoint_revision", "capability_revision", "connection_id", "channel_id",
    "sequence_domain", "source_schema_revision", "source_event_key",
    "selected_evidence_key", "native_match_id", "trade_observation_id",
    "native_sequence", "maker_order_id", "maker_order_id_scope",
    "taker_order_id", "taker_order_id_scope", "resting_order_id",
    "resting_order_id_scope", "aggressor_side", "price", "price_unit", "qty",
    "qty_unit", "quantity_basis", "qty_semantics", "event_time",
    "data_quality", "quality_flags", "normalization_version", "schema_version",
)

STATE_CHECKPOINT_COLUMNS: tuple[str, ...] = (
    "checkpoint_key", "market_id", "venue_id", "native_symbol",
    "mapping_revision", "instrument_id", "source_level", "endpoint_id",
    "endpoint_revision", "capability_revision", "connection_id", "channel_id",
    "sequence_domain", "source_schema_revision", "through_source_event_key",
    "native_sequence", "checkpoint_time", "available_time", "order_count",
    "bid_order_count", "ask_order_count", "depth_limit", "completeness",
    "state_sha256", "checksum", "checksum_algorithm", "checksum_scope",
    "checksum_status", "visibility_flags", "data_quality", "quality_flags",
    "source_input_set_hash", "derivation_method_version",
    "priority_policy_revision", "normalization_version", "schema_version",
)


def _stable_key(prefix: str, body: Mapping[str, object]) -> str:
    encoded = json.dumps(
        dict(body), ensure_ascii=False, separators=(",", ":"), sort_keys=True,
    ).encode("utf-8")
    return prefix + hashlib.sha256(encoded).hexdigest()


def make_source_event_key(
    *,
    endpoint_id: str,
    endpoint_revision: int,
    market_id: str,
    identity_scope: str,
    identity_basis: str,
    identity_parts: Sequence[str | int],
) -> str:
    """从调用方选定的最强来源身份生成可重复逻辑事件键。"""

    stable_endpoint_id = validate_endpoint_id(endpoint_id)
    if endpoint_revision < 0:
        raise ValueError("endpoint_revision must be non-negative")
    if not market_id.strip() or not identity_scope.strip():
        raise ValueError("market_id and identity_scope must not be empty")
    if identity_basis not in SOURCE_EVENT_KEY_BASES:
        raise ValueError("unknown source_event_key basis")
    if not identity_parts or any(str(part).strip() == "" for part in identity_parts):
        raise ValueError("identity_parts must contain non-empty source identity")
    return _stable_key("l3evt-sha256-", {
        "endpoint_id": stable_endpoint_id,
        "endpoint_revision": endpoint_revision,
        "identity_basis": identity_basis,
        "identity_parts": list(identity_parts),
        "identity_scope": identity_scope.strip(),
        "market_id": market_id.strip(),
    })


def make_event_evidence_key(
    *, source_artifact_id: str, source_row_index: int, source_item_index: int,
) -> str:
    """从不可变原件位置生成一份观察证据键。"""

    _sha_artifact(source_artifact_id)
    _non_negative(source_row_index, "source_row_index")
    _non_negative(source_item_index, "source_item_index")
    return _stable_key("l3evi-sha256-", {
        "source_artifact_id": source_artifact_id,
        "source_item_index": source_item_index,
        "source_row_index": source_row_index,
    })


def make_match_link_key(
    *, market_id: str, endpoint_id: str, endpoint_revision: int,
    identity_scope: str, identity_parts: Sequence[str | int],
) -> str:
    """生成场所作用域内稳定的成交关联键。"""

    stable_endpoint_id = validate_endpoint_id(endpoint_id)
    if endpoint_revision < 0:
        raise ValueError("endpoint_revision must be non-negative")
    if not market_id.strip() or not identity_scope.strip() or not identity_parts:
        raise ValueError("match identity must not be empty")
    return _stable_key("l3match-sha256-", {
        "endpoint_id": stable_endpoint_id,
        "endpoint_revision": endpoint_revision,
        "identity_parts": list(identity_parts),
        "identity_scope": identity_scope.strip(),
        "market_id": market_id.strip(),
    })


def make_checkpoint_key(
    *, market_id: str, connection_id: str, sequence_domain: str,
    through_source_event_key: str, derivation_method_version: str,
) -> str:
    """生成绑定连接、序列域和派生方法的状态检查点键。"""

    values = (
        market_id, connection_id, sequence_domain, through_source_event_key,
        derivation_method_version,
    )
    if any(not value.strip() for value in values):
        raise ValueError("checkpoint identity must not be empty")
    return _stable_key("l3ckpt-sha256-", {
        "connection_id": connection_id,
        "derivation_method_version": derivation_method_version,
        "market_id": market_id,
        "sequence_domain": sequence_domain,
        "through_source_event_key": through_source_event_key,
    })


def _non_negative(value: object, field: str) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")


def _sha_artifact(value: object) -> None:
    if (
        not isinstance(value, str) or not value.startswith("sha256-")
        or len(value) != 71
        or any(char not in "0123456789abcdef" for char in value[7:])
    ):
        raise ValueError("source_artifact_id must be a canonical SHA-256 ID")


def _sha_digest(value: object, field: str) -> None:
    if (
        not isinstance(value, str) or len(value) != 64
        or any(char not in "0123456789abcdef" for char in value)
    ):
        raise ValueError(f"{field} must be lowercase SHA-256")


def _canonical_json(value: object, field: str) -> None:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be canonical JSON text")
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{field} must be canonical JSON text") from exc
    canonical = json.dumps(
        decoded, ensure_ascii=False, separators=(",", ":"), sort_keys=True,
    )
    if canonical != value:
        raise ValueError(f"{field} must be canonical JSON text")


def validate_order_event(row: Mapping[str, object]) -> None:
    """执行不可妥协的 L3 逻辑事件检查。"""

    missing = [name for name in ORDER_EVENT_COLUMNS if name not in row]
    if missing:
        raise ValueError(f"missing L3 order-event columns: {', '.join(missing)}")
    required_text = (
        "source_event_key", "source_event_key_basis", "selected_evidence_key",
        "market_id", "venue_id", "native_symbol", "instrument_id",
        "endpoint_id", "connection_id", "channel_id", "sequence_domain",
        "source_schema_revision", "price_unit", "qty_unit", "quantity_basis",
        "priority_policy_revision", "normalization_version",
    )
    for name in required_text:
        value = row[name]
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{name} must be non-empty text")
    if not str(row["source_event_key"]).startswith("l3evt-sha256-"):
        raise ValueError("source_event_key must be canonical")
    if not str(row["selected_evidence_key"]).startswith("l3evi-sha256-"):
        raise ValueError("selected_evidence_key must be canonical")
    validate_endpoint_id(str(row["endpoint_id"]))
    if row["source_level"] not in SOURCE_LEVELS:
        raise ValueError("source_level must be native L3 grade A or B")
    if row["event_type"] not in L3_EVENT_TYPES:
        raise ValueError("event_type is not an L3 order lifecycle event")
    if row["source_event_key_basis"] not in SOURCE_EVENT_KEY_BASES:
        raise ValueError("invalid source_event_key_basis")
    event_type = str(row["event_type"])
    if row["qty_semantics"] not in EVENT_QTY_SEMANTICS[event_type]:
        raise ValueError("qty_semantics is incompatible with event_type")
    if row["priority_origin"] not in PRIORITY_ORIGINS:
        raise ValueError("invalid priority_origin")
    if row["priority_effect"] not in PRIORITY_EFFECTS:
        raise ValueError("invalid priority_effect")
    if row["data_quality"] not in DATA_QUALITY_VALUES:
        raise ValueError("invalid data_quality")
    if row["checksum_status"] not in CHECKSUM_STATUSES:
        raise ValueError("invalid checksum_status")
    _canonical_json(row["visibility_flags"], "visibility_flags")
    _canonical_json(row["quality_flags"], "quality_flags")
    if row["normalization_version"] != BOOK_L3_NORMALIZATION_VERSION:
        raise ValueError("invalid L3 normalization_version")
    if row["schema_version"] != BOOK_L3_SCHEMA_VERSION:
        raise ValueError("invalid L3 schema_version")
    for name in (
        "mapping_revision", "endpoint_revision", "capability_revision",
        "native_event_index",
    ):
        if row[name] is not None:
            _non_negative(row[name], name)
    if not isinstance(row["is_snapshot"], bool):
        raise ValueError("is_snapshot must be boolean")
    if event_type == "SNAPSHOT_ORDER" and row["is_snapshot"] is not True:
        raise ValueError("SNAPSHOT_ORDER must set is_snapshot=true")
    if event_type != "RESET":
        for name in ("native_order_id", "order_id_scope", "qty"):
            value = row[name]
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"non-RESET L3 events require {name}")
        if row["side"] not in {"buy", "sell"}:
            raise ValueError("non-RESET L3 events require buy/sell side")
    recv_time = row["recv_time_utc"]
    available_time = row["available_time"]
    ingest_time = row["ingest_time"]
    for name, value in (
        ("recv_time_utc", recv_time), ("available_time", available_time),
        ("ingest_time", ingest_time),
    ):
        if (
            not isinstance(value, datetime) or value.tzinfo is None
            or value.utcoffset() is None
        ):
            raise ValueError(f"{name} must be offset-aware datetime")
    assert isinstance(recv_time, datetime)
    assert isinstance(available_time, datetime)
    assert isinstance(ingest_time, datetime)
    event_time = row["event_time"]
    if event_time is not None and not isinstance(event_time, datetime):
        raise ValueError("event_time must be datetime or null")
    if event_time is not None and available_time < event_time:
        raise ValueError("available_time precedes event_time")
    if available_time < recv_time:
        raise ValueError("available_time precedes recv_time_utc")
    if available_time < ingest_time:
        raise ValueError("available_time precedes ingest_time")
    if (row["native_qty"] is None) != (row["native_qty_unit"] is None):
        raise ValueError("native_qty and native_qty_unit must be paired")
    if row["checksum_status"] in {"passed", "failed"} and (
        row["checksum"] is None or row["checksum_algorithm"] is None
        or row["checksum_scope"] is None
    ):
        raise ValueError("checked checksum requires value, algorithm, and scope")


def validate_event_evidence(row: Mapping[str, object]) -> None:
    """验证一份可追加的原始观察证据。"""

    missing = [name for name in EVENT_EVIDENCE_COLUMNS if name not in row]
    if missing:
        raise ValueError(f"missing L3 evidence columns: {', '.join(missing)}")
    source_row = row["source_row_index"]
    source_item = row["source_item_index"]
    _non_negative(source_row, "source_row_index")
    _non_negative(source_item, "source_item_index")
    assert isinstance(source_row, int) and not isinstance(source_row, bool)
    assert isinstance(source_item, int) and not isinstance(source_item, bool)
    for name in (
        "market_id", "source_event_key", "connection_id", "channel_id",
        "sequence_domain", "source_schema_revision",
    ):
        value = row[name]
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{name} must be non-empty evidence text")
    if not str(row["source_event_key"]).startswith("l3evt-sha256-"):
        raise ValueError("evidence source_event_key must be canonical")
    expected = make_event_evidence_key(
        source_artifact_id=str(row["source_artifact_id"]),
        source_row_index=source_row,
        source_item_index=source_item,
    )
    if row["evidence_key"] != expected:
        raise ValueError("evidence_key does not match immutable raw location")
    _sha_digest(row["raw_payload_sha256"], "raw_payload_sha256")
    validate_endpoint_id(str(row["endpoint_id"]))
    for name in (
        "endpoint_revision", "capability_revision", "source_row_index",
        "source_item_index",
    ):
        _non_negative(row[name], name)
    for name in ("recv_time_mono_ns",):
        if row[name] is not None:
            _non_negative(row[name], name)
    _canonical_json(row["quality_flags"], "quality_flags")
    if row["data_quality"] not in DATA_QUALITY_VALUES:
        raise ValueError("invalid evidence data_quality")
    if row["normalization_version"] != BOOK_L3_NORMALIZATION_VERSION:
        raise ValueError("invalid evidence normalization_version")
    if row["schema_version"] != BOOK_L3_SCHEMA_VERSION:
        raise ValueError("invalid evidence schema_version")
    recv_time = row["recv_time_utc"]
    available_time = row["available_time"]
    ingest_time = row["ingest_time"]
    for name, value in (
        ("recv_time_utc", recv_time), ("available_time", available_time),
        ("ingest_time", ingest_time),
    ):
        if (
            not isinstance(value, datetime) or value.tzinfo is None
            or value.utcoffset() is None
        ):
            raise ValueError(f"{name} must be offset-aware datetime")
    assert isinstance(recv_time, datetime)
    assert isinstance(available_time, datetime)
    assert isinstance(ingest_time, datetime)
    if available_time < recv_time or available_time < ingest_time:
        raise ValueError("evidence available_time violates PIT ordering")


def _validate_common_fact(
    row: Mapping[str, object], columns: Sequence[str], *, fact: str,
) -> None:
    missing = [name for name in columns if name not in row]
    if missing:
        raise ValueError(f"missing L3 {fact} columns: {', '.join(missing)}")
    for name in (
        "market_id", "venue_id", "native_symbol", "instrument_id",
        "endpoint_id", "connection_id", "channel_id", "sequence_domain",
        "source_schema_revision", "normalization_version",
    ):
        value = row[name]
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{fact} {name} must be non-empty text")
    validate_endpoint_id(str(row["endpoint_id"]))
    for name in ("mapping_revision", "endpoint_revision", "capability_revision"):
        _non_negative(row[name], name)
    if row["source_level"] not in SOURCE_LEVELS:
        raise ValueError(f"invalid {fact} source_level")
    if row["data_quality"] not in DATA_QUALITY_VALUES:
        raise ValueError(f"invalid {fact} data_quality")
    _canonical_json(row["quality_flags"], "quality_flags")
    if row["normalization_version"] != BOOK_L3_NORMALIZATION_VERSION:
        raise ValueError(f"invalid {fact} normalization_version")
    if row["schema_version"] != BOOK_L3_SCHEMA_VERSION:
        raise ValueError(f"invalid {fact} schema_version")


def validate_match_link(row: Mapping[str, object]) -> None:
    """验证订单事件与成交观察之间的可证明关联。"""

    _validate_common_fact(row, MATCH_LINK_COLUMNS, fact="match-link")
    if not str(row["match_link_key"]).startswith("l3match-sha256-"):
        raise ValueError("match_link_key must be canonical")
    if not str(row["source_event_key"]).startswith("l3evt-sha256-"):
        raise ValueError("match source_event_key must be canonical")
    if not str(row["selected_evidence_key"]).startswith("l3evi-sha256-"):
        raise ValueError("match selected_evidence_key must be canonical")
    if row["qty_semantics"] != "executed":
        raise ValueError("match qty_semantics must be executed")
    for name in ("price", "price_unit", "qty", "qty_unit", "quantity_basis"):
        value = row[name]
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"match {name} must be non-empty text")
    for value_name, scope_name in (
        ("maker_order_id", "maker_order_id_scope"),
        ("taker_order_id", "taker_order_id_scope"),
        ("resting_order_id", "resting_order_id_scope"),
    ):
        if (row[value_name] is None) != (row[scope_name] is None):
            raise ValueError(f"{value_name} and {scope_name} must be paired")
    if row["native_match_id"] is None and row["trade_observation_id"] is None:
        raise ValueError("match needs native_match_id or trade_observation_id")
    event_time = row["event_time"]
    if event_time is not None and (
        not isinstance(event_time, datetime) or event_time.tzinfo is None
        or event_time.utcoffset() is None
    ):
        raise ValueError("match event_time must be offset-aware")


def validate_state_checkpoint(row: Mapping[str, object]) -> None:
    """验证 L3 公开订单态检查点的身份、守恒与 PIT。"""

    _validate_common_fact(row, STATE_CHECKPOINT_COLUMNS, fact="checkpoint")
    if not str(row["checkpoint_key"]).startswith("l3ckpt-sha256-"):
        raise ValueError("checkpoint_key must be canonical")
    if not str(row["through_source_event_key"]).startswith("l3evt-sha256-"):
        raise ValueError("checkpoint through event key must be canonical")
    for name in ("order_count", "bid_order_count", "ask_order_count"):
        _non_negative(row[name], name)
    order_count = row["order_count"]
    bid_count = row["bid_order_count"]
    ask_count = row["ask_order_count"]
    assert isinstance(order_count, int) and not isinstance(order_count, bool)
    assert isinstance(bid_count, int) and not isinstance(bid_count, bool)
    assert isinstance(ask_count, int) and not isinstance(ask_count, bool)
    if order_count != bid_count + ask_count:
        raise ValueError("checkpoint order counts do not close")
    if row["depth_limit"] is not None:
        _non_negative(row["depth_limit"], "depth_limit")
        if row["depth_limit"] == 0:
            raise ValueError("checkpoint depth_limit must be positive")
    _sha_digest(row["state_sha256"], "state_sha256")
    _sha_digest(row["source_input_set_hash"], "source_input_set_hash")
    _canonical_json(row["visibility_flags"], "visibility_flags")
    if row["checksum_status"] not in CHECKSUM_STATUSES:
        raise ValueError("invalid checkpoint checksum_status")
    if row["checksum_status"] in {"passed", "failed"} and (
        row["checksum"] is None or row["checksum_algorithm"] is None
        or row["checksum_scope"] is None
    ):
        raise ValueError("checked checkpoint checksum is incomplete")
    for name in (
        "completeness", "derivation_method_version", "priority_policy_revision",
    ):
        value = row[name]
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"checkpoint {name} must be non-empty text")
    checkpoint_time = row["checkpoint_time"]
    available_time = row["available_time"]
    for name, value in (
        ("checkpoint_time", checkpoint_time), ("available_time", available_time),
    ):
        if (
            not isinstance(value, datetime) or value.tzinfo is None
            or value.utcoffset() is None
        ):
            raise ValueError(f"{name} must be offset-aware datetime")
    assert isinstance(checkpoint_time, datetime)
    assert isinstance(available_time, datetime)
    if available_time < checkpoint_time:
        raise ValueError("checkpoint available_time violates PIT ordering")


def create_book_l3_tables(db: Any) -> None:
    """建立四个会话级 DuckDB 临时表，不污染控制面。"""

    db.execute(f"""
        CREATE TEMP TABLE {BOOK_L3_ORDER_EVENT_DATASET} (
          source_event_key VARCHAR NOT NULL,
          source_event_key_basis VARCHAR NOT NULL,
          selected_evidence_key VARCHAR NOT NULL,
          market_id VARCHAR NOT NULL, venue_id VARCHAR NOT NULL,
          native_symbol VARCHAR NOT NULL, mapping_revision INTEGER NOT NULL,
          instrument_id VARCHAR NOT NULL, source_level VARCHAR NOT NULL,
          endpoint_id VARCHAR NOT NULL, endpoint_revision INTEGER NOT NULL,
          capability_revision INTEGER NOT NULL, connection_id VARCHAR NOT NULL,
          channel_id VARCHAR NOT NULL, sequence_domain VARCHAR NOT NULL,
          source_schema_revision VARCHAR NOT NULL, event_type VARCHAR NOT NULL,
          native_order_id VARCHAR, order_id_scope VARCHAR,
          native_sequence VARCHAR, previous_native_sequence VARCHAR,
          native_event_index BIGINT, side VARCHAR, price VARCHAR,
          price_unit VARCHAR NOT NULL, native_qty VARCHAR,
          native_qty_unit VARCHAR, qty VARCHAR, qty_unit VARCHAR NOT NULL,
          quantity_basis VARCHAR NOT NULL, qty_semantics VARCHAR NOT NULL,
          previous_qty VARCHAR, remaining_qty VARCHAR, priority_key VARCHAR,
          priority_time TIMESTAMPTZ, priority_origin VARCHAR NOT NULL,
          priority_effect VARCHAR NOT NULL,
          priority_policy_revision VARCHAR NOT NULL, is_snapshot BOOLEAN NOT NULL,
          checkpoint_key VARCHAR, event_time TIMESTAMPTZ,
          source_publish_time TIMESTAMPTZ, recv_time_utc TIMESTAMPTZ NOT NULL,
          recv_time_mono_ns UBIGINT, available_time TIMESTAMPTZ NOT NULL,
          ingest_time TIMESTAMPTZ NOT NULL, checksum VARCHAR,
          checksum_algorithm VARCHAR, checksum_scope VARCHAR,
          checksum_status VARCHAR NOT NULL, visibility_flags VARCHAR NOT NULL,
          data_quality VARCHAR NOT NULL, quality_flags VARCHAR NOT NULL,
          normalization_version VARCHAR NOT NULL, schema_version INTEGER NOT NULL,
          PRIMARY KEY (market_id, source_event_key, normalization_version)
        )
    """)
    db.execute(f"""
        CREATE TEMP TABLE {EVENT_EVIDENCE_DATASET} (
          evidence_key VARCHAR NOT NULL, market_id VARCHAR NOT NULL,
          source_event_key VARCHAR NOT NULL, endpoint_id VARCHAR NOT NULL,
          endpoint_revision INTEGER NOT NULL, capability_revision INTEGER NOT NULL,
          connection_id VARCHAR NOT NULL,
          channel_id VARCHAR NOT NULL, sequence_domain VARCHAR NOT NULL,
          source_schema_revision VARCHAR NOT NULL,
          source_artifact_id VARCHAR NOT NULL, source_row_index BIGINT NOT NULL,
          source_item_index BIGINT NOT NULL, raw_payload_sha256 VARCHAR NOT NULL,
          recv_time_utc TIMESTAMPTZ NOT NULL, recv_time_mono_ns UBIGINT,
          available_time TIMESTAMPTZ NOT NULL, ingest_time TIMESTAMPTZ NOT NULL,
          data_quality VARCHAR NOT NULL, quality_flags VARCHAR NOT NULL,
          normalization_version VARCHAR NOT NULL, schema_version INTEGER NOT NULL,
          PRIMARY KEY (
            market_id, source_event_key, normalization_version, evidence_key
          ), UNIQUE (evidence_key)
        )
    """)
    db.execute(f"""
        CREATE TEMP TABLE {MATCH_LINK_DATASET} (
          match_link_key VARCHAR NOT NULL, market_id VARCHAR NOT NULL,
          venue_id VARCHAR NOT NULL, native_symbol VARCHAR NOT NULL,
          mapping_revision INTEGER NOT NULL, instrument_id VARCHAR NOT NULL,
          source_level VARCHAR NOT NULL, endpoint_id VARCHAR NOT NULL,
          endpoint_revision INTEGER NOT NULL, capability_revision INTEGER NOT NULL,
          connection_id VARCHAR NOT NULL, channel_id VARCHAR NOT NULL,
          sequence_domain VARCHAR NOT NULL, source_schema_revision VARCHAR NOT NULL,
          source_event_key VARCHAR NOT NULL, selected_evidence_key VARCHAR NOT NULL,
          native_match_id VARCHAR, trade_observation_id VARCHAR,
          native_sequence VARCHAR, maker_order_id VARCHAR,
          maker_order_id_scope VARCHAR, taker_order_id VARCHAR,
          taker_order_id_scope VARCHAR, resting_order_id VARCHAR,
          resting_order_id_scope VARCHAR, aggressor_side VARCHAR, price VARCHAR,
          price_unit VARCHAR NOT NULL, qty VARCHAR NOT NULL,
          qty_unit VARCHAR NOT NULL, quantity_basis VARCHAR NOT NULL,
          qty_semantics VARCHAR NOT NULL, event_time TIMESTAMPTZ,
          data_quality VARCHAR NOT NULL, quality_flags VARCHAR NOT NULL,
          normalization_version VARCHAR NOT NULL, schema_version INTEGER NOT NULL,
          PRIMARY KEY (market_id, match_link_key, normalization_version)
        )
    """)
    db.execute(f"""
        CREATE TEMP TABLE {STATE_CHECKPOINT_DATASET} (
          checkpoint_key VARCHAR NOT NULL, market_id VARCHAR NOT NULL,
          venue_id VARCHAR NOT NULL, native_symbol VARCHAR NOT NULL,
          mapping_revision INTEGER NOT NULL, instrument_id VARCHAR NOT NULL,
          source_level VARCHAR NOT NULL, endpoint_id VARCHAR NOT NULL,
          endpoint_revision INTEGER NOT NULL, capability_revision INTEGER NOT NULL,
          connection_id VARCHAR NOT NULL, channel_id VARCHAR NOT NULL,
          sequence_domain VARCHAR NOT NULL, source_schema_revision VARCHAR NOT NULL,
          through_source_event_key VARCHAR NOT NULL, native_sequence VARCHAR,
          checkpoint_time TIMESTAMPTZ NOT NULL, available_time TIMESTAMPTZ NOT NULL,
          order_count BIGINT NOT NULL, bid_order_count BIGINT NOT NULL,
          ask_order_count BIGINT NOT NULL, depth_limit INTEGER,
          completeness VARCHAR NOT NULL, state_sha256 VARCHAR NOT NULL,
          checksum VARCHAR, checksum_algorithm VARCHAR, checksum_scope VARCHAR,
          checksum_status VARCHAR NOT NULL, visibility_flags VARCHAR NOT NULL,
          data_quality VARCHAR NOT NULL, quality_flags VARCHAR NOT NULL,
          source_input_set_hash VARCHAR NOT NULL,
          derivation_method_version VARCHAR NOT NULL,
          priority_policy_revision VARCHAR NOT NULL,
          normalization_version VARCHAR NOT NULL, schema_version INTEGER NOT NULL,
          PRIMARY KEY (market_id, checkpoint_key, normalization_version)
        )
    """)
