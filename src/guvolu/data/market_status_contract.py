"""交易场所市场状态观察事实契约。

该事实描述交易模式与集合竞价/熔断控制面，不是盘口帧。即使来源频道与
``book_l2`` 共用一个 WebSocket 连接，也不得把它的事件当作盘口 sequence，
更不得用它修改 L2 本地簿。
"""
from __future__ import annotations

from typing import Any


MARKET_STATUS_DATASET = "market_status_observation"
MARKET_STATUS_SCHEMA_VERSION = 1
MARKET_STATUS_NORMALIZATION_VERSION = "market-status-normalization-v1"

MARKET_STATUS_COLUMNS: tuple[str, ...] = (
    "observation_id",
    "venue_id",
    "venue_symbol",
    "market_id",
    "mapping_revision",
    "capability_revision",
    "instrument_id",
    "endpoint_id",
    "endpoint_revision",
    "payload_schema_version",
    "event_time",
    "available_time",
    "ingest_time",
    "recv_ts_mono_ns",
    "time_origin",
    "mode",
    "fee_type",
    "estimated_auction_price",
    "estimated_auction_amount",
    "auction_upper_price",
    "auction_lower_price",
    "upper_trigger_price",
    "lower_trigger_price",
    "reopen_time",
    "collection_run_id",
    "connection_id",
    "channel_id",
    "source_member",
    "raw_payload_sha256",
    "data_quality",
    "source_artifact_id",
    "source_row_index",
    "normalization_version",
    "schema_version",
)


def create_market_status_table(db: Any) -> None:
    """建立市场状态 Parquet 的 DuckDB 暂存表。"""
    db.execute(f"""
        CREATE TABLE {MARKET_STATUS_DATASET} (
          observation_id VARCHAR,
          venue_id VARCHAR,
          venue_symbol VARCHAR,
          market_id VARCHAR,
          mapping_revision INTEGER,
          capability_revision INTEGER,
          instrument_id VARCHAR,
          endpoint_id VARCHAR,
          endpoint_revision INTEGER,
          payload_schema_version VARCHAR,
          event_time TIMESTAMPTZ,
          available_time TIMESTAMPTZ,
          ingest_time TIMESTAMPTZ,
          recv_ts_mono_ns UBIGINT,
          time_origin VARCHAR,
          mode VARCHAR,
          fee_type VARCHAR,
          estimated_auction_price VARCHAR,
          estimated_auction_amount VARCHAR,
          auction_upper_price VARCHAR,
          auction_lower_price VARCHAR,
          upper_trigger_price VARCHAR,
          lower_trigger_price VARCHAR,
          reopen_time TIMESTAMPTZ,
          collection_run_id VARCHAR,
          connection_id VARCHAR,
          channel_id VARCHAR,
          source_member VARCHAR,
          raw_payload_sha256 VARCHAR,
          data_quality VARCHAR,
          source_artifact_id VARCHAR,
          source_row_index BIGINT,
          normalization_version VARCHAR,
          schema_version INTEGER
        )
    """)
