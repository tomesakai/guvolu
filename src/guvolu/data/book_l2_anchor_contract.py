"""独立 REST L2 锚点事实契约。

REST 观察与 WS ``book_l2`` 事实严格分域。观察表保存请求、响应、时间、
摘要和对照裁决；档位表保存完整价格级。二者都不得回写或修补 WS 事实。
"""
from __future__ import annotations

from typing import Any

ANCHOR_OBSERVATION_DATASET = "book_l2_anchor_observation"
ANCHOR_LEVEL_DATASET = "book_l2_anchor_level"
ANCHOR_RECONCILIATION_DATASET = "book_l2_anchor_reconciliation"
ANCHOR_SCHEMA_VERSION = 2
ANCHOR_NORMALIZATION_VERSION = "book-l2-anchor-normalization-v2"
ANCHOR_RECONCILIATION_VERSION = "book-l2-anchor-reconciliation-v2"

ANCHOR_OBSERVATION_COLUMNS: tuple[str, ...] = (
    "observation_id", "venue_id", "venue_symbol", "market_id",
    "mapping_revision", "instrument_id", "endpoint_id", "endpoint_key",
    "endpoint_revision", "request_method", "request_url",
    "request_sha256", "response_sha256", "http_status",
    "trigger_reason", "connection_id", "event_time", "available_time",
    "ingest_time", "time_origin", "receive_source_offset_ms",
    "availability_basis", "sequence_id", "best_bid", "best_ask",
    "bid_levels", "ask_levels", "bid_depth", "ask_depth", "book_hash",
    "anchor_availability", "failure_reason", "source_artifact_id",
    "source_storage_path", "normalization_version", "schema_version",
)

ANCHOR_LEVEL_COLUMNS: tuple[str, ...] = (
    "observation_id", "market_id", "side", "source_level_index", "price",
    "size", "source_artifact_id", "normalization_version", "schema_version",
)

ANCHOR_RECONCILIATION_COLUMNS: tuple[str, ...] = (
    "reconciliation_id", "observation_id", "market_id", "venue_id",
    "endpoint_id", "endpoint_revision", "anchor_available_time",
    "anchor_sequence_id", "comparison_status",
    "comparison_basis", "comparison_reason", "ws_checkpoint_attempt_id",
    "ws_checkpoint_artifact_id", "ws_as_of_frame_id", "ws_as_of_event_time",
    "ws_as_of_available_time", "ws_sequence_id", "comparison_lag_ms",
    "anchor_best_bid", "anchor_best_ask", "anchor_bid_levels",
    "anchor_ask_levels", "anchor_bid_depth", "anchor_ask_depth",
    "anchor_book_hash", "ws_best_bid", "ws_best_ask", "ws_bid_levels",
    "ws_ask_levels", "ws_bid_depth", "ws_ask_depth", "ws_book_hash",
    "best_bid_match", "best_ask_match", "depth_match", "book_hash_match",
    "full_book_comparable", "source_anchor_attempt_id",
    "source_anchor_artifact_id",
    "normalization_version", "schema_version",
)


def create_anchor_tables(db: Any) -> None:
    """建立单次锚点物化所需的 DuckDB 暂存表。"""
    db.execute(f"""
        CREATE TABLE {ANCHOR_OBSERVATION_DATASET} (
          observation_id VARCHAR,
          venue_id VARCHAR,
          venue_symbol VARCHAR,
          market_id VARCHAR,
          mapping_revision INTEGER,
          instrument_id VARCHAR,
          endpoint_id VARCHAR,
          endpoint_key VARCHAR,
          endpoint_revision INTEGER,
          request_method VARCHAR,
          request_url VARCHAR,
          request_sha256 VARCHAR,
          response_sha256 VARCHAR,
          http_status INTEGER,
          trigger_reason VARCHAR,
          connection_id VARCHAR,
          event_time TIMESTAMPTZ,
          available_time TIMESTAMPTZ,
          ingest_time TIMESTAMPTZ,
          time_origin VARCHAR,
          receive_source_offset_ms DOUBLE,
          availability_basis VARCHAR,
          sequence_id VARCHAR,
          best_bid VARCHAR,
          best_ask VARCHAR,
          bid_levels INTEGER,
          ask_levels INTEGER,
          bid_depth VARCHAR,
          ask_depth VARCHAR,
          book_hash VARCHAR,
          anchor_availability VARCHAR,
          failure_reason VARCHAR,
          source_artifact_id VARCHAR,
          source_storage_path VARCHAR,
          normalization_version VARCHAR,
          schema_version INTEGER
        )
    """)
    db.execute(f"""
        CREATE TABLE {ANCHOR_RECONCILIATION_DATASET} (
          reconciliation_id VARCHAR,
          observation_id VARCHAR,
          market_id VARCHAR,
          venue_id VARCHAR,
          endpoint_id VARCHAR,
          endpoint_revision INTEGER,
          anchor_available_time TIMESTAMPTZ,
          anchor_sequence_id VARCHAR,
          comparison_status VARCHAR,
          comparison_basis VARCHAR,
          comparison_reason VARCHAR,
          ws_checkpoint_attempt_id VARCHAR,
          ws_checkpoint_artifact_id VARCHAR,
          ws_as_of_frame_id VARCHAR,
          ws_as_of_event_time TIMESTAMPTZ,
          ws_as_of_available_time TIMESTAMPTZ,
          ws_sequence_id VARCHAR,
          comparison_lag_ms DOUBLE,
          anchor_best_bid VARCHAR,
          anchor_best_ask VARCHAR,
          anchor_bid_levels INTEGER,
          anchor_ask_levels INTEGER,
          anchor_bid_depth VARCHAR,
          anchor_ask_depth VARCHAR,
          anchor_book_hash VARCHAR,
          ws_best_bid VARCHAR,
          ws_best_ask VARCHAR,
          ws_bid_levels INTEGER,
          ws_ask_levels INTEGER,
          ws_bid_depth VARCHAR,
          ws_ask_depth VARCHAR,
          ws_book_hash VARCHAR,
          best_bid_match BOOLEAN,
          best_ask_match BOOLEAN,
          depth_match BOOLEAN,
          book_hash_match BOOLEAN,
          full_book_comparable BOOLEAN,
          source_anchor_attempt_id VARCHAR,
          source_anchor_artifact_id VARCHAR,
          normalization_version VARCHAR,
          schema_version INTEGER
        )
    """)
    db.execute(f"""
        CREATE TABLE {ANCHOR_LEVEL_DATASET} (
          observation_id VARCHAR,
          market_id VARCHAR,
          side VARCHAR,
          source_level_index INTEGER,
          price VARCHAR,
          size VARCHAR,
          source_artifact_id VARCHAR,
          normalization_version VARCHAR,
          schema_version INTEGER
        )
    """)
