"""盘口 L2 事实列契约。

第二版仍是 OKX 历史成品的不可变契约。第三版 schema 是它的按列名兼容超集，
供实时来源记录连接边界、接收时钟和逐帧原文身份。第四、五版只修订实时
归一语义，不改变第三版列布局。读取跨版本成品时必须使用
``union_by_name=true``，不得要求重写既有 v2/v3/v4 Parquet。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

BOOK_L2_SCHEMA_VERSION = 2
BOOK_L2_NORMALIZATION_VERSION = "book-l2-normalization-v2"
BOOK_L2_FRAME_DATASET = "book_l2_frame"
BOOK_L2_LEVEL_DATASET = "book_l2_level"

FRAME_COLUMNS: tuple[str, ...] = (
    "frame_id",
    "venue_id",
    "venue_symbol",
    "market_id",
    "mapping_revision",
    "capability_revision",
    "instrument_id",
    "endpoint",
    "payload_schema_version",
    "message_kind",
    "book_mode",
    "replay_fidelity",
    "event_time",
    "source_publish_time",
    "available_time",
    "ingest_time",
    "time_origin",
    "sequence_id",
    "prev_sequence_id",
    "checksum",
    "integrity_mode",
    "changed_bid_levels",
    "changed_ask_levels",
    "book_bid_levels",
    "book_ask_levels",
    "depth_limit",
    "mid_price",
    "source_session_id",
    "source_member",
    "source_artifact_id",
    "source_row_index",
    "normalization_version",
    "schema_version",
)

LEVEL_COLUMNS: tuple[str, ...] = (
    "frame_id",
    "market_id",
    "side",
    "source_level_index",
    "price",
    "size",
    "order_count",
    "action",
    "level_kind",
    "source_artifact_id",
    "source_row_index",
    "normalization_version",
    "schema_version",
)

# 保留历史第二版名称。
# 实时三所改用第三版。
# 避免误重建历史数据。
BOOK_L2_V3_SCHEMA_VERSION = 3
BOOK_L2_V3_NORMALIZATION_VERSION = "book-l2-normalization-v3"

# v3 已有不可变产物。
# 同序规则必须升语义版本。
# v4 沿用 schema v3 列布局。
BOOK_L2_V4_SCHEMA_VERSION = BOOK_L2_V3_SCHEMA_VERSION
BOOK_L2_V4_NORMALIZATION_VERSION = "book-l2-normalization-v4"

# v5 不再把分段局部状态写成来源事实。
# 只保留原生前驱。
BOOK_L2_V5_SCHEMA_VERSION = BOOK_L2_V3_SCHEMA_VERSION
BOOK_L2_V5_NORMALIZATION_VERSION = "book-l2-normalization-v5"

V3_FRAME_COLUMNS: tuple[str, ...] = (
    "frame_id",
    "venue_id",
    "venue_symbol",
    "market_id",
    "mapping_revision",
    "capability_revision",
    "instrument_id",
    "endpoint",
    "endpoint_id",
    "endpoint_revision",
    "payload_schema_version",
    "message_kind",
    "book_mode",
    "replay_fidelity",
    "event_time",
    "source_publish_time",
    "available_time",
    "ingest_time",
    "recv_ts_mono_ns",
    "time_origin",
    "sequence_id",
    "prev_sequence_id",
    "checksum",
    "integrity_mode",
    "changed_bid_levels",
    "changed_ask_levels",
    "book_bid_levels",
    "book_ask_levels",
    "depth_limit",
    "source_depth_levels",
    "mid_price",
    "source_session_id",
    "connection_id",
    "channel_id",
    "source_member",
    "raw_payload_sha256",
    "data_quality",
    "source_level",
    # 保留三所来源汇总。
    "ask_market_size",
    "bid_market_size",
    "asks_over",
    "bids_under",
    "asks_under",
    "bids_over",
    "run_id",
    "segment_sequence",
    "source_artifact_id",
    "source_row_index",
    "normalization_version",
    "schema_version",
)

# 第三版档位沿用旧列。
# 订单数缺失时留空。
# 下游按列名合并。
V3_LEVEL_COLUMNS: tuple[str, ...] = LEVEL_COLUMNS

# v4 没有物理列变化。
# 别名区分列与语义版本。
V4_FRAME_COLUMNS: tuple[str, ...] = V3_FRAME_COLUMNS
V4_LEVEL_COLUMNS: tuple[str, ...] = V3_LEVEL_COLUMNS

# v5 没有物理列变化。
V5_FRAME_COLUMNS: tuple[str, ...] = V3_FRAME_COLUMNS
V5_LEVEL_COLUMNS: tuple[str, ...] = V3_LEVEL_COLUMNS


@dataclass(frozen=True, slots=True)
class BookSourceDescriptor:
    """端点级盘口语义；同一来源的不同端点不得混用。"""

    venue_id: str
    domain: str
    endpoint: str
    transport: str
    payload_schema_version: str
    timestamp_unit: str
    book_mode: str
    replay_fidelity: str
    sequence_policy: str
    checksum_policy: str
    availability_policy: str
    depth_limit: int | None
    endpoint_id: str | None = None
    endpoint_revision: int | None = None
    source_level: str = "L2"


def create_book_l2_tables(db: Any) -> None:
    """建立第二版盘口帧与档位临时表。"""
    db.execute(f"""
        CREATE TABLE {BOOK_L2_FRAME_DATASET} (
          frame_id VARCHAR, venue_id VARCHAR, venue_symbol VARCHAR,
          market_id VARCHAR, mapping_revision INTEGER,
          capability_revision INTEGER, instrument_id VARCHAR,
          endpoint VARCHAR, payload_schema_version VARCHAR,
          message_kind VARCHAR, book_mode VARCHAR, replay_fidelity VARCHAR,
          event_time TIMESTAMPTZ, source_publish_time TIMESTAMPTZ,
          available_time TIMESTAMPTZ, ingest_time TIMESTAMPTZ,
          time_origin VARCHAR, sequence_id VARCHAR, prev_sequence_id VARCHAR,
          checksum VARCHAR, integrity_mode VARCHAR,
          changed_bid_levels INTEGER, changed_ask_levels INTEGER,
          book_bid_levels INTEGER, book_ask_levels INTEGER,
          depth_limit INTEGER, mid_price VARCHAR,
          source_session_id VARCHAR, source_member VARCHAR,
          source_artifact_id VARCHAR, source_row_index BIGINT,
          normalization_version VARCHAR, schema_version INTEGER
        )
    """)
    db.execute(f"""
        CREATE TABLE {BOOK_L2_LEVEL_DATASET} (
          frame_id VARCHAR, market_id VARCHAR, side VARCHAR,
          source_level_index INTEGER, price VARCHAR, size VARCHAR,
          order_count INTEGER, action VARCHAR, level_kind VARCHAR,
          source_artifact_id VARCHAR, source_row_index BIGINT,
          normalization_version VARCHAR, schema_version INTEGER
        )
    """)


def create_book_l2_v3_tables(db: Any) -> None:
    """建立 v3 实时盘口帧与档位临时表。"""
    db.execute(f"""
        CREATE TABLE {BOOK_L2_FRAME_DATASET} (
          frame_id VARCHAR, venue_id VARCHAR, venue_symbol VARCHAR,
          market_id VARCHAR, mapping_revision INTEGER,
          capability_revision INTEGER, instrument_id VARCHAR,
          endpoint VARCHAR, endpoint_id VARCHAR, endpoint_revision INTEGER,
          payload_schema_version VARCHAR,
          message_kind VARCHAR, book_mode VARCHAR, replay_fidelity VARCHAR,
          event_time TIMESTAMPTZ, source_publish_time TIMESTAMPTZ,
          available_time TIMESTAMPTZ, ingest_time TIMESTAMPTZ,
          recv_ts_mono_ns UBIGINT, time_origin VARCHAR,
          sequence_id VARCHAR, prev_sequence_id VARCHAR,
          checksum VARCHAR, integrity_mode VARCHAR,
          changed_bid_levels INTEGER, changed_ask_levels INTEGER,
          book_bid_levels INTEGER, book_ask_levels INTEGER,
          depth_limit INTEGER, source_depth_levels INTEGER,
          mid_price VARCHAR, source_session_id VARCHAR,
          connection_id VARCHAR, channel_id VARCHAR, source_member VARCHAR,
          raw_payload_sha256 VARCHAR, data_quality VARCHAR,
          source_level VARCHAR,
          ask_market_size VARCHAR, bid_market_size VARCHAR,
          asks_over VARCHAR, bids_under VARCHAR, asks_under VARCHAR,
          bids_over VARCHAR, run_id VARCHAR, segment_sequence INTEGER,
          source_artifact_id VARCHAR, source_row_index BIGINT,
          normalization_version VARCHAR, schema_version INTEGER
        )
    """)
    db.execute(f"""
        CREATE TABLE {BOOK_L2_LEVEL_DATASET} (
          frame_id VARCHAR, market_id VARCHAR, side VARCHAR,
          source_level_index INTEGER, price VARCHAR, size VARCHAR,
          order_count INTEGER, action VARCHAR, level_kind VARCHAR,
          source_artifact_id VARCHAR, source_row_index BIGINT,
          normalization_version VARCHAR, schema_version INTEGER
        )
    """)


def create_book_l2_v4_tables(db: Any) -> None:
    """建立 v4 语义使用的 schema v3 实时盘口临时表。"""

    create_book_l2_v3_tables(db)


def create_book_l2_v5_tables(db: Any) -> None:
    """建立 v5 语义使用的 schema v3 实时盘口临时表。"""

    create_book_l2_v3_tables(db)
