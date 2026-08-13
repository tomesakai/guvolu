"""SQLite 库：结构初始化与 kline 存取（storage-design 第 5 节）。

金额一律 TEXT、时间一律 UTC ISO 文本（D-07、D-08）。
版本 2 增补多源维度表与归档覆盖表
（multi-source-data-design 第 4 节，TBD-23 序 1）。
版本 3 增补派生事件两表（footprint-design 6.4、6.8 节，TBD-29）：
book_feature 追加式登记判读事件（指标值 JSON、配置散列），
alert_event 追加式登记报警触发，确认仅回填 acked_at，无交易语义。
版本 4（2026-08-10）增补区域分析幂等键：book_feature 加
request_hash 唯一列，alert_event 加 (feature_id, rule_id) 唯一索引，
同请求同配置重放复用既有行，不重复落库与报警。
版本 5 增补端点能力版本、规范化逐笔与顶档、回补任务、
分析全量台账；book_feature 补 run_id 与版本双钥。
版本 6（2026-08-10）analysis_run 补区域参数两列：
basis（数值基准）与 window_columns（窗列数），存量库补列迁移。
版本 7 增补归一化分区台账。台账以归档内容散列为键，
使大规模逐笔投影可中断续跑，并保留拒绝行数与失败原因。
版本 8 增补已发生元数据/PIT 修正台账，任何历史修正可追溯。
版本 9 增补市场、内容制品与分析物化台账，事实通过
market_id、artifact_id 与 normalization_version 串联。
版本 10 增补活动分区指针，重物化保留旧制品但查询只读新头。
版本 11 增补物化拒绝台账，以原件与行号定位未入事实的记录。
版本 12 分离制品内容身份与存放位置，保留重复内容的分区次数。
版本 13 增补物化尝试到端点能力修订的批级绑定；新任务现场记录，
存量任务以 ``migration-inferred`` 明示迁移推断，不伪装原生证据。
版本 15 区分被接受的来源观察、协议控制行和真正拒绝行；实时流
握手/订阅确认不再被误报为坏数据，并保留逐项 ignore 证据。
版本 16 增补派生物化依赖；checkpoint/tile 尝试以外键绑定确切上游
活动 attempt，不能只靠路径或约定名称推断血缘。
版本 17 增补版本化端点身份、实际采集连接与订阅频道控制面；能力行保持
原样，只有未来采集频道可选择显式绑定能力修订，不推断存量端点 ID。
版本 18 将端点自然键严格收敛到工作簿的十二个身份维度；品牌名与内部
venue 外键分列，scope/source schema 只作为修订属性。v17 原行先冻结到
``endpoint_revision_v17_archive``，有连接引用的旧行以保留修订迁入新表。
连接与频道时刻另存 observation basis，首帧观察不伪装握手确认时刻。
版本 19 增补活动 L2 的五分钟质量窗口，以及 bitbank 市场状态 raw 输入的
断点扫描台账。二者都只保存小型控制遥测；来源没有的序号、checksum 与接收
时钟指标保持 NULL，不把观察到的接收静默伪装为确定的数据缺口。
版本 20 增补每市场一行的 REST L2 锚点最新摘要。原始响应、规范化档位与
对照事实仍保存为不可变内容制品；SQLite 只保存低基数活动指针与状态。
"""
from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path

from guvolu.data.source_contract import EndpointNaturalIdentity, EndpointRevisionRow

DB_SCHEMA_VERSION = 20
DB_FILE_NAME = "guvolu.sqlite3"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta_schema_history (
  schema_version INTEGER PRIMARY KEY,
  applied_at     TEXT NOT NULL,
  code_version   TEXT
);
CREATE TABLE IF NOT EXISTS kline (
  symbol         TEXT NOT NULL,
  interval       TEXT NOT NULL,
  open_time      TEXT NOT NULL,
  available_time TEXT NOT NULL,
  ingest_time    TEXT NOT NULL,
  trading_day    TEXT NOT NULL,
  open   TEXT NOT NULL,
  high   TEXT NOT NULL,
  low    TEXT NOT NULL,
  close  TEXT NOT NULL,
  volume TEXT NOT NULL,
  revision_id    INTEGER NOT NULL DEFAULT 0,
  raw_source     TEXT NOT NULL,
  PRIMARY KEY (symbol, interval, open_time, revision_id)
);
CREATE INDEX IF NOT EXISTS idx_kline_day
  ON kline (symbol, interval, trading_day);
CREATE TABLE IF NOT EXISTS venue (
  venue_id          TEXT PRIMARY KEY,
  kind              TEXT NOT NULL,   -- exchange 或 vendor
  role              TEXT NOT NULL,   -- execution 或 market 或 reference
  timestamp_unit    TEXT NOT NULL,   -- 秒 毫秒 微秒 纳秒
  trading_day_rule  TEXT NOT NULL,   -- 如 JST06 或 UTC00
  write_allowed     INTEGER NOT NULL -- 仅 gmo 为 1
);
CREATE TABLE IF NOT EXISTS endpoint_revision (
  endpoint_id          TEXT NOT NULL,
  revision_id          INTEGER NOT NULL CHECK (revision_id >= 0),
  natural_key          TEXT NOT NULL,
  natural_key_sha256   TEXT NOT NULL CHECK (length(natural_key_sha256) = 64),
  legal_entity         TEXT NOT NULL,
  venue_brand          TEXT NOT NULL,
  venue_id             TEXT NOT NULL REFERENCES venue(venue_id),
  product              TEXT NOT NULL,
  environment          TEXT NOT NULL,
  region               TEXT NOT NULL,
  transport            TEXT NOT NULL,
  protocol             TEXT NOT NULL,
  auth_mode            TEXT NOT NULL,
  host                 TEXT NOT NULL,
  port                 INTEGER CHECK (port BETWEEN 1 AND 65535),
  base_path_or_channel TEXT NOT NULL,
  data_level           TEXT NOT NULL,
  scope                TEXT NOT NULL,
  source_schema_revision TEXT NOT NULL,
  documentation_uri    TEXT NOT NULL,
  documentation_sha256 TEXT CHECK (
    documentation_sha256 IS NULL OR length(documentation_sha256) = 64
  ),
  effective_from       TEXT NOT NULL,
  valid_until          TEXT NOT NULL,
  registered_at        TEXT NOT NULL,
  PRIMARY KEY (endpoint_id, revision_id),
  UNIQUE (natural_key_sha256, revision_id)
);
CREATE INDEX IF NOT EXISTS idx_endpoint_revision_lookup
  ON endpoint_revision (
    venue_id, product, environment, effective_from, valid_until
  );
CREATE TRIGGER IF NOT EXISTS trg_endpoint_natural_identity_owner
BEFORE INSERT ON endpoint_revision
WHEN EXISTS (
  SELECT 1 FROM endpoint_revision existing
  WHERE existing.natural_key_sha256=NEW.natural_key_sha256
    AND existing.natural_key=NEW.natural_key
    AND existing.endpoint_id<>NEW.endpoint_id
)
BEGIN
  SELECT RAISE(ABORT, 'endpoint natural identity belongs to another endpoint_id');
END;
CREATE TABLE IF NOT EXISTS collection_connection (
  connection_id      TEXT PRIMARY KEY,
  endpoint_id        TEXT NOT NULL,
  endpoint_revision  INTEGER NOT NULL,
  collection_run_id  TEXT NOT NULL,
  connection_ordinal INTEGER NOT NULL,
  opened_at          TEXT NOT NULL,
  opened_at_basis    TEXT NOT NULL,
  closed_at          TEXT,
  close_reason       TEXT,
  UNIQUE (collection_run_id, connection_ordinal),
  CHECK (
    connection_ordinal > 0 OR (
      connection_ordinal = 0 AND
      opened_at_basis = 'legacy_unqualified_recorded_time'
    )
  ),
  FOREIGN KEY (endpoint_id, endpoint_revision)
    REFERENCES endpoint_revision(endpoint_id, revision_id)
);
CREATE TABLE IF NOT EXISTS instrument (
  instrument_id TEXT PRIMARY KEY,
  base          TEXT NOT NULL,
  quote         TEXT NOT NULL,
  kind          TEXT NOT NULL        -- spot 或 leverage 或 perpetual
);
CREATE TABLE IF NOT EXISTS instrument_map (
  venue_id      TEXT NOT NULL REFERENCES venue(venue_id),
  venue_symbol  TEXT NOT NULL,
  instrument_id TEXT NOT NULL REFERENCES instrument(instrument_id),
  tick_size     TEXT,
  size_step     TEXT,
  min_size      TEXT,
  revision_id   INTEGER NOT NULL DEFAULT 0,
  observed_at   TEXT NOT NULL,
  raw_source    TEXT NOT NULL,
  PRIMARY KEY (venue_id, venue_symbol, revision_id)
);
CREATE TABLE IF NOT EXISTS archive_coverage (
  venue_id     TEXT NOT NULL,
  venue_symbol TEXT NOT NULL,
  domain       TEXT NOT NULL,        -- trade 等，同 venue_capability
  day          TEXT NOT NULL,        -- 来源分区日 YYYYMMDD
  rows         INTEGER,
  first_ts     TEXT,
  last_ts      TEXT,
  status       TEXT NOT NULL,        -- ok 或 missing 或 empty
  ingest_time  TEXT NOT NULL,
  PRIMARY KEY (venue_id, venue_symbol, domain, day)
);
CREATE TABLE IF NOT EXISTS venue_capability_revision (
  venue_id       TEXT NOT NULL REFERENCES venue(venue_id),
  domain         TEXT NOT NULL,
  endpoint       TEXT NOT NULL,
  revision_id    INTEGER NOT NULL,
  available      INTEGER NOT NULL,
  access_mode    TEXT NOT NULL,
  backfill_mode  TEXT NOT NULL,
  replay_fidelity TEXT NOT NULL,
  integrity      TEXT NOT NULL,
  rate_model     TEXT NOT NULL,
  timestamp_unit TEXT NOT NULL,
  evidence_level TEXT NOT NULL,
  implementation_status TEXT NOT NULL,
  evidence_uri   TEXT NOT NULL,
  surveyed_at    TEXT NOT NULL,
  valid_until    TEXT NOT NULL,
  schema_version INTEGER NOT NULL,
  PRIMARY KEY (venue_id, domain, endpoint, revision_id)
);
CREATE TABLE IF NOT EXISTS trade_tick (
  venue_id       TEXT NOT NULL,
  instrument_id  TEXT NOT NULL,
  venue_trade_id TEXT NOT NULL,
  event_time     TEXT NOT NULL,
  available_time TEXT NOT NULL,
  ingest_time    TEXT NOT NULL,
  side           TEXT NOT NULL CHECK(side IN ('buy', 'sell')),
  source_side_basis TEXT NOT NULL,
  price          TEXT NOT NULL,
  size           TEXT NOT NULL,
  match_granularity TEXT NOT NULL,
  id_origin      TEXT NOT NULL,
  sequence_id    TEXT,
  first_trade_id TEXT,
  last_trade_id  TEXT,
  time_origin    TEXT NOT NULL,
  normalization_version TEXT NOT NULL,
  schema_version INTEGER NOT NULL,
  revision_id    INTEGER NOT NULL DEFAULT 0,
  raw_item_index INTEGER NOT NULL,
  raw_source     TEXT NOT NULL,
  PRIMARY KEY (venue_id, instrument_id, venue_trade_id, revision_id)
);
CREATE INDEX IF NOT EXISTS idx_trade_tick_event
  ON trade_tick (venue_id, instrument_id, event_time);
CREATE TABLE IF NOT EXISTS book_top (
  venue_id       TEXT NOT NULL,
  instrument_id  TEXT NOT NULL,
  frame_id       TEXT NOT NULL,
  event_time     TEXT NOT NULL,
  available_time TEXT NOT NULL,
  ingest_time    TEXT NOT NULL,
  bid            TEXT NOT NULL,
  bid_size       TEXT NOT NULL,
  ask            TEXT NOT NULL,
  ask_size       TEXT NOT NULL,
  depth_levels   INTEGER NOT NULL,
  source_depth_levels INTEGER NOT NULL,
  time_origin    TEXT NOT NULL,
  sequence_id    TEXT,
  normalization_version TEXT NOT NULL,
  schema_version INTEGER NOT NULL,
  raw_item_index INTEGER NOT NULL,
  raw_source     TEXT NOT NULL,
  PRIMARY KEY (venue_id, instrument_id, frame_id)
);
CREATE INDEX IF NOT EXISTS idx_book_top_event
  ON book_top (venue_id, instrument_id, event_time);
CREATE TABLE IF NOT EXISTS stream_health (
  venue_id       TEXT NOT NULL,
  channel        TEXT NOT NULL,
  instrument_id  TEXT NOT NULL,
  window_start   TEXT NOT NULL,
  last_event_time TEXT,
  frames         INTEGER NOT NULL,
  sequence_gaps  INTEGER NOT NULL,
  sequence_regressions INTEGER NOT NULL,
  checksum_failures INTEGER NOT NULL,
  snapshot_mismatches INTEGER NOT NULL,
  reconnects     INTEGER NOT NULL,
  status         TEXT NOT NULL,
  PRIMARY KEY (venue_id, channel, instrument_id, window_start)
);
CREATE TABLE IF NOT EXISTS backfill_run (
  run_id         TEXT PRIMARY KEY,
  venue_id       TEXT NOT NULL,
  venue_symbol   TEXT NOT NULL,
  domain         TEXT NOT NULL,
  from_day       TEXT NOT NULL,
  to_day         TEXT NOT NULL,
  planned_parts  INTEGER NOT NULL,
  ok_parts       INTEGER NOT NULL,
  missing_parts  INTEGER NOT NULL,
  empty_parts    INTEGER NOT NULL,
  rows           INTEGER NOT NULL,
  checksum_failures INTEGER NOT NULL,
  status         TEXT NOT NULL,
  failure_detail TEXT,
  started_at     TEXT NOT NULL,
  finished_at    TEXT,
  config_hash    TEXT NOT NULL,
  code_version   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS normalized_partition (
  venue_id       TEXT NOT NULL,
  venue_symbol   TEXT NOT NULL,
  domain         TEXT NOT NULL,
  day            TEXT NOT NULL,
  raw_source     TEXT NOT NULL,
  source_sha256  TEXT NOT NULL,
  raw_rows       INTEGER NOT NULL,
  normalized_rows INTEGER NOT NULL,
  rejected_rows  INTEGER NOT NULL,
  started_at     TEXT NOT NULL,
  finished_at    TEXT NOT NULL,
  normalization_version TEXT NOT NULL,
  schema_version INTEGER NOT NULL,
  status         TEXT NOT NULL,
  failure_detail TEXT,
  PRIMARY KEY (venue_id, venue_symbol, domain, day, raw_source)
);
CREATE INDEX IF NOT EXISTS idx_normalized_partition_status
  ON normalized_partition (venue_id, venue_symbol, domain, status, day);
CREATE TABLE IF NOT EXISTS data_correction (
  correction_id  TEXT PRIMARY KEY,
  subject_table  TEXT NOT NULL,
  predicate_sql  TEXT NOT NULL,
  affected_rows  INTEGER NOT NULL,
  before_sha256  TEXT NOT NULL,
  after_sha256   TEXT NOT NULL,
  reason         TEXT NOT NULL,
  applied_at     TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_instrument_map_market_fk
  ON instrument_map (venue_id, venue_symbol, instrument_id, revision_id);
CREATE TABLE IF NOT EXISTS market (
  market_id        TEXT PRIMARY KEY,
  venue_id         TEXT NOT NULL,
  venue_symbol     TEXT NOT NULL,
  instrument_id    TEXT NOT NULL,
  mapping_revision INTEGER NOT NULL,
  market_kind      TEXT NOT NULL,
  created_at       TEXT NOT NULL,
  UNIQUE (venue_id, venue_symbol, mapping_revision),
  FOREIGN KEY (venue_id, venue_symbol, instrument_id, mapping_revision)
    REFERENCES instrument_map (
      venue_id, venue_symbol, instrument_id, revision_id
    )
);
CREATE TABLE IF NOT EXISTS collection_channel (
  connection_id       TEXT NOT NULL
    REFERENCES collection_connection(connection_id),
  channel_id           TEXT NOT NULL,
  native_channel       TEXT NOT NULL,
  market_id            TEXT REFERENCES market(market_id),
  subscription_key     TEXT NOT NULL,
  subscription_sha256  TEXT NOT NULL CHECK (length(subscription_sha256) = 64),
  subscribed_at        TEXT NOT NULL,
  subscribed_at_basis  TEXT NOT NULL,
  unsubscribed_at      TEXT,
  capability_venue_id  TEXT,
  capability_domain    TEXT,
  capability_endpoint  TEXT,
  capability_revision  INTEGER,
  PRIMARY KEY (connection_id, channel_id),
  UNIQUE (connection_id, subscription_sha256, subscribed_at),
  CHECK (
    (capability_venue_id IS NULL AND capability_domain IS NULL AND
     capability_endpoint IS NULL AND capability_revision IS NULL) OR
    (capability_venue_id IS NOT NULL AND capability_domain IS NOT NULL AND
     capability_endpoint IS NOT NULL AND capability_revision IS NOT NULL)
  ),
  FOREIGN KEY (
    capability_venue_id, capability_domain,
    capability_endpoint, capability_revision
  ) REFERENCES venue_capability_revision (
    venue_id, domain, endpoint, revision_id
  )
);
CREATE TABLE IF NOT EXISTS artifact (
  artifact_id        TEXT PRIMARY KEY,
  artifact_kind      TEXT NOT NULL,
  storage_path       TEXT NOT NULL UNIQUE,
  sha256             TEXT NOT NULL,
  byte_count         INTEGER NOT NULL CHECK (byte_count >= 0),
  sealed_at          TEXT NOT NULL,
  registered_at      TEXT NOT NULL,
  verification_method TEXT NOT NULL,
  schema_version     INTEGER NOT NULL,
  CHECK (length(sha256) = 64),
  CHECK (artifact_id = 'sha256-' || sha256)
);
CREATE TABLE IF NOT EXISTS artifact_location (
  artifact_id    TEXT NOT NULL REFERENCES artifact(artifact_id),
  storage_path   TEXT NOT NULL UNIQUE,
  observed_at    TEXT NOT NULL,
  is_canonical   INTEGER NOT NULL CHECK (is_canonical IN (0, 1)),
  PRIMARY KEY (artifact_id, storage_path)
);
CREATE TABLE IF NOT EXISTS market_status_input_scan (
  artifact_id          TEXT NOT NULL REFERENCES artifact(artifact_id),
  normalization_version TEXT NOT NULL,
  endpoint_id          TEXT NOT NULL,
  endpoint_revision    INTEGER NOT NULL,
  source_rows          INTEGER NOT NULL CHECK (source_rows >= 0),
  candidate_rows       INTEGER NOT NULL CHECK (
    candidate_rows >= 0 AND candidate_rows <= source_rows
  ),
  scanned_at           TEXT NOT NULL,
  PRIMARY KEY (artifact_id, normalization_version),
  FOREIGN KEY (endpoint_id, endpoint_revision)
    REFERENCES endpoint_revision(endpoint_id, revision_id)
);
CREATE TABLE IF NOT EXISTS partition_attempt (
  attempt_id           TEXT PRIMARY KEY,
  market_id            TEXT NOT NULL REFERENCES market(market_id),
  domain               TEXT NOT NULL,
  partition_key        TEXT NOT NULL,
  normalization_version TEXT NOT NULL,
  input_set_hash       TEXT NOT NULL,
  status               TEXT NOT NULL CHECK (
    status IN (
      'planned', 'running', 'complete',
      'complete_with_rejections', 'failed'
    )
  ),
  source_rows          INTEGER NOT NULL DEFAULT 0 CHECK (source_rows >= 0),
  normalized_rows      INTEGER NOT NULL DEFAULT 0 CHECK (normalized_rows >= 0),
  ignored_rows         INTEGER NOT NULL DEFAULT 0 CHECK (ignored_rows >= 0),
  rejected_rows        INTEGER NOT NULL DEFAULT 0 CHECK (rejected_rows >= 0),
  started_at           TEXT NOT NULL,
  finished_at          TEXT,
  code_version         TEXT NOT NULL,
  config_hash          TEXT NOT NULL,
  failure_detail       TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_partition_attempt_complete
  ON partition_attempt (
    market_id, domain, partition_key,
    normalization_version, input_set_hash
  )
  WHERE status IN ('complete', 'complete_with_rejections');
CREATE TABLE IF NOT EXISTS partition_input (
  attempt_id      TEXT NOT NULL REFERENCES partition_attempt(attempt_id),
  artifact_id     TEXT NOT NULL REFERENCES artifact(artifact_id),
  source_rows     INTEGER NOT NULL CHECK (source_rows >= 0),
  normalized_rows INTEGER NOT NULL CHECK (normalized_rows >= 0),
  ignored_rows    INTEGER NOT NULL DEFAULT 0 CHECK (ignored_rows >= 0),
  rejected_rows  INTEGER NOT NULL CHECK (rejected_rows >= 0),
  PRIMARY KEY (attempt_id, artifact_id)
);
CREATE TABLE IF NOT EXISTS partition_input_binding (
  attempt_id      TEXT NOT NULL REFERENCES partition_attempt(attempt_id),
  artifact_id     TEXT NOT NULL REFERENCES artifact(artifact_id),
  storage_path    TEXT NOT NULL,
  source_rows     INTEGER NOT NULL CHECK (source_rows >= 0),
  normalized_rows INTEGER NOT NULL CHECK (normalized_rows >= 0),
  ignored_rows    INTEGER NOT NULL DEFAULT 0 CHECK (ignored_rows >= 0),
  rejected_rows  INTEGER NOT NULL CHECK (rejected_rows >= 0),
  PRIMARY KEY (attempt_id, artifact_id, storage_path),
  FOREIGN KEY (artifact_id, storage_path)
    REFERENCES artifact_location(artifact_id, storage_path)
);
CREATE TABLE IF NOT EXISTS materialization_output (
  attempt_id       TEXT NOT NULL REFERENCES partition_attempt(attempt_id),
  artifact_id      TEXT NOT NULL REFERENCES artifact(artifact_id),
  dataset          TEXT NOT NULL,
  row_count        INTEGER NOT NULL CHECK (row_count >= 0),
  min_event_time   TEXT,
  max_event_time   TEXT,
  created_at       TEXT NOT NULL,
  PRIMARY KEY (attempt_id, artifact_id)
);
CREATE TABLE IF NOT EXISTS materialization_partition_head (
  market_id            TEXT NOT NULL REFERENCES market(market_id),
  domain               TEXT NOT NULL,
  partition_key        TEXT NOT NULL,
  normalization_version TEXT NOT NULL,
  attempt_id           TEXT NOT NULL UNIQUE
    REFERENCES partition_attempt(attempt_id),
  activated_at         TEXT NOT NULL,
  PRIMARY KEY (market_id, domain, partition_key)
);
CREATE TABLE IF NOT EXISTS l2_quality_window (
  market_id          TEXT NOT NULL REFERENCES market(market_id),
  window_start       TEXT NOT NULL,
  window_end         TEXT NOT NULL,
  quality_version    TEXT NOT NULL,
  source_head_generation TEXT NOT NULL CHECK (
    length(source_head_generation) = 71 AND
    substr(source_head_generation, 1, 7) = 'sha256-'
  ),
  source_attempt_ids TEXT NOT NULL,
  source_attempt_count INTEGER NOT NULL CHECK (source_attempt_count > 0),
  source_normalization_versions TEXT NOT NULL,
  window_clock_basis TEXT NOT NULL CHECK (
    window_clock_basis IN ('ingest', 'available', 'event', 'mixed', 'none')
  ),
  frames             INTEGER NOT NULL CHECK (frames >= 0),
  snapshot_frames    INTEGER NOT NULL CHECK (snapshot_frames >= 0),
  delta_frames       INTEGER NOT NULL CHECK (delta_frames >= 0),
  connection_count   INTEGER CHECK (connection_count > 0),
  channel_count      INTEGER CHECK (channel_count > 0),
  identity_unknown_frames INTEGER NOT NULL CHECK (
    identity_unknown_frames >= 0
  ),
  first_observation_time TEXT,
  last_observation_time TEXT,
  first_event_time   TEXT,
  last_event_time    TEXT,
  first_available_time TEXT,
  last_available_time  TEXT,
  first_ingest_time  TEXT,
  last_ingest_time   TEXT,
  max_observed_interarrival_ms REAL CHECK (
    max_observed_interarrival_ms IS NULL OR
    max_observed_interarrival_ms >= 0
  ),
  observed_silence_gt_30s INTEGER CHECK (
    observed_silence_gt_30s IS NULL OR observed_silence_gt_30s >= 0
  ),
  sequence_duplicates INTEGER CHECK (
    sequence_duplicates IS NULL OR sequence_duplicates >= 0
  ),
  sequence_regressions INTEGER CHECK (
    sequence_regressions IS NULL OR sequence_regressions >= 0
  ),
  predecessor_unknown_frames INTEGER CHECK (
    predecessor_unknown_frames IS NULL OR predecessor_unknown_frames >= 0
  ),
  unanchored_before_snapshot_frames INTEGER CHECK (
    unanchored_before_snapshot_frames IS NULL OR
    unanchored_before_snapshot_frames >= 0
  ),
  anchor_unknown_frames INTEGER NOT NULL CHECK (anchor_unknown_frames >= 0),
  untrusted_frames INTEGER CHECK (
    untrusted_frames IS NULL OR untrusted_frames >= 0
  ),
  fact_untrusted_flag_conflicts INTEGER NOT NULL CHECK (
    fact_untrusted_flag_conflicts >= 0
  ),
  checksum_status TEXT NOT NULL CHECK (
    checksum_status IN ('passed', 'failed', 'unsupported', 'unknown')
  ),
  checksum_observed_frames INTEGER NOT NULL CHECK (
    checksum_observed_frames >= 0
  ),
  checksum_checked_frames INTEGER CHECK (
    checksum_checked_frames IS NULL OR checksum_checked_frames >= 0
  ),
  checksum_failures INTEGER CHECK (
    checksum_failures IS NULL OR checksum_failures >= 0
  ),
  recv_source_offset_samples INTEGER NOT NULL CHECK (
    recv_source_offset_samples >= 0
  ),
  recv_source_offset_p50_ms REAL,
  recv_source_offset_p95_ms REAL,
  latency_status TEXT NOT NULL CHECK (
    latency_status IN ('measurable', 'clock_skewed', 'unmeasurable')
  ),
  latest_materialized_observation_time TEXT,
  materialized_freshness_seconds REAL,
  materialized_freshness_status TEXT NOT NULL CHECK (
    materialized_freshness_status IN (
      'fresh', 'stale', 'clock_skewed', 'unknown', 'not_applicable'
    )
  ),
  window_complete INTEGER NOT NULL CHECK (window_complete IN (0, 1)),
  status TEXT NOT NULL CHECK (status IN ('ok', 'degraded', 'failed')),
  reasons TEXT NOT NULL,
  computed_at TEXT NOT NULL,
  PRIMARY KEY (market_id, window_start, quality_version),
  CHECK (window_end > window_start),
  CHECK (snapshot_frames + delta_frames <= frames),
  CHECK (
    (frames = 0 AND first_observation_time IS NULL AND
     last_observation_time IS NULL AND first_event_time IS NULL AND
     last_event_time IS NULL) OR
    (frames > 0 AND first_observation_time IS NOT NULL AND
     last_observation_time IS NOT NULL AND first_event_time IS NOT NULL AND
     last_event_time IS NOT NULL)
  ),
  CHECK (
    (recv_source_offset_samples = 0 AND
     recv_source_offset_p50_ms IS NULL AND
     recv_source_offset_p95_ms IS NULL AND
     latency_status = 'unmeasurable') OR
    (recv_source_offset_samples > 0 AND
     recv_source_offset_p50_ms IS NOT NULL AND
     recv_source_offset_p95_ms IS NOT NULL AND
     latency_status IN ('measurable', 'clock_skewed'))
  ),
  CHECK (
    (checksum_status IN ('unsupported', 'unknown') AND
     checksum_checked_frames IS NULL AND checksum_failures IS NULL) OR
    (checksum_status IN ('passed', 'failed') AND
     checksum_checked_frames IS NOT NULL AND checksum_failures IS NOT NULL)
  )
);
CREATE INDEX IF NOT EXISTS idx_l2_quality_window_latest
  ON l2_quality_window (market_id, window_start DESC, quality_version);
CREATE TABLE IF NOT EXISTS l2_anchor_status (
  market_id               TEXT PRIMARY KEY REFERENCES market(market_id),
  observation_id          TEXT NOT NULL,
  status                  TEXT NOT NULL CHECK (
    status IN ('fresh', 'mismatch', 'unknown', 'unavailable')
  ),
  comparison_status       TEXT NOT NULL CHECK (
    comparison_status IN ('match', 'mismatch', 'unknown')
  ),
  trigger_reason          TEXT NOT NULL CHECK (
    trigger_reason IN ('connection_open', 'reconnect', 'periodic')
  ),
  connection_id           TEXT,
  endpoint_key            TEXT NOT NULL,
  endpoint_revision       INTEGER NOT NULL CHECK (endpoint_revision >= 0),
  event_time              TEXT NOT NULL,
  available_time          TEXT NOT NULL,
  source_artifact_id      TEXT NOT NULL REFERENCES artifact(artifact_id),
  observation_artifact_id TEXT NOT NULL REFERENCES artifact(artifact_id),
  reconciliation_artifact_id TEXT NOT NULL REFERENCES artifact(artifact_id),
  anchor_attempt_id       TEXT NOT NULL REFERENCES partition_attempt(attempt_id),
  reconciliation_attempt_id TEXT NOT NULL
    REFERENCES partition_attempt(attempt_id),
  ws_checkpoint_attempt_id TEXT REFERENCES partition_attempt(attempt_id),
  comparison_lag_ms       REAL,
  reason                  TEXT NOT NULL,
  updated_at              TEXT NOT NULL,
  CHECK (length(observation_id) = 71),
  CHECK (substr(observation_id, 1, 7) = 'sha256-')
);
CREATE INDEX IF NOT EXISTS idx_l2_anchor_status_state
  ON l2_anchor_status (status, available_time DESC);
CREATE TABLE IF NOT EXISTS materialization_rejection (
  attempt_id       TEXT NOT NULL REFERENCES partition_attempt(attempt_id),
  artifact_id      TEXT NOT NULL REFERENCES artifact(artifact_id),
  source_row_index INTEGER NOT NULL,
  raw_source       TEXT NOT NULL,
  reason           TEXT NOT NULL,
  created_at       TEXT NOT NULL,
  PRIMARY KEY (attempt_id, artifact_id, source_row_index)
);
CREATE TABLE IF NOT EXISTS materialization_ignore (
  attempt_id       TEXT NOT NULL REFERENCES partition_attempt(attempt_id),
  artifact_id      TEXT NOT NULL REFERENCES artifact(artifact_id),
  source_row_index INTEGER NOT NULL,
  source_item_index INTEGER NOT NULL DEFAULT -1,
  raw_source       TEXT NOT NULL,
  reason           TEXT NOT NULL,
  created_at       TEXT NOT NULL,
  PRIMARY KEY (attempt_id, artifact_id, source_row_index, source_item_index)
);
CREATE TABLE IF NOT EXISTS partition_capability_binding (
  attempt_id      TEXT NOT NULL REFERENCES partition_attempt(attempt_id),
  venue_id        TEXT NOT NULL,
  domain          TEXT NOT NULL,
  endpoint        TEXT NOT NULL,
  revision_id     INTEGER NOT NULL,
  binding_basis   TEXT NOT NULL CHECK (
    binding_basis IN ('recorded', 'migration-inferred')
  ),
  bound_at        TEXT NOT NULL,
  PRIMARY KEY (attempt_id, venue_id, domain, endpoint),
  FOREIGN KEY (venue_id, domain, endpoint, revision_id)
    REFERENCES venue_capability_revision (
      venue_id, domain, endpoint, revision_id
    )
);
CREATE TABLE IF NOT EXISTS materialization_dependency (
  attempt_id          TEXT NOT NULL REFERENCES partition_attempt(attempt_id),
  upstream_attempt_id TEXT NOT NULL REFERENCES partition_attempt(attempt_id),
  binding_basis       TEXT NOT NULL CHECK (
    binding_basis IN ('active-head', 'explicit-replay')
  ),
  bound_at            TEXT NOT NULL,
  PRIMARY KEY (attempt_id, upstream_attempt_id)
);
CREATE TABLE IF NOT EXISTS analysis_run (
  run_id         TEXT PRIMARY KEY,
  request_hash   TEXT NOT NULL UNIQUE,
  venue_id       TEXT NOT NULL,
  symbol         TEXT NOT NULL,
  price_low      TEXT NOT NULL,
  price_high     TEXT NOT NULL,
  from_ts        TEXT NOT NULL,
  to_ts          TEXT NOT NULL,
  bucket         TEXT NOT NULL,
  judgments      TEXT NOT NULL,
  baseline       TEXT NOT NULL,
  baseline_hash  TEXT NOT NULL,
  source_hash    TEXT NOT NULL,
  config_hash    TEXT NOT NULL,
  code_version   TEXT NOT NULL,
  confidence_version TEXT NOT NULL,
  created_at     TEXT NOT NULL,
  status         TEXT NOT NULL,
  failure_detail TEXT,
  basis          TEXT NOT NULL DEFAULT 'quantity',
  window_columns INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS book_feature (
  feature_id  INTEGER PRIMARY KEY AUTOINCREMENT,
  kind        TEXT NOT NULL,
  venue_id    TEXT NOT NULL,
  symbol      TEXT NOT NULL,
  price_low   TEXT NOT NULL,
  price_high  TEXT NOT NULL,
  from_ts     TEXT NOT NULL,
  to_ts       TEXT NOT NULL,
  metrics     TEXT NOT NULL,         -- 指标值 JSON，数值全为字符串
  config_hash TEXT NOT NULL,
  created_at  TEXT NOT NULL,
  request_hash TEXT,
  run_id       TEXT,
  code_version TEXT,
  confidence_version TEXT
);
CREATE TABLE IF NOT EXISTS alert_event (
  alert_id     INTEGER PRIMARY KEY AUTOINCREMENT,
  feature_id   INTEGER NOT NULL REFERENCES book_feature(feature_id),
  rule_id      TEXT NOT NULL,
  triggered_at TEXT NOT NULL,
  acked_at     TEXT
);
"""


_WORKBOOK_ENDPOINT_IDS = frozenset({"EP-0002", "EP-0005", "EP-0007"})
_LEGACY_REVISION_OFFSET = 1_000_000_000


def _endpoint_table_columns(conn: sqlite3.Connection) -> set[str]:
    return {
        str(row[1])
        for row in conn.execute("PRAGMA table_info(endpoint_revision)")
    }


def _create_v18_endpoint_control_tables(conn: sqlite3.Connection) -> None:
    """在 v17 表改名后建立 v18 端点、连接与频道表。"""
    conn.execute(
        """
        CREATE TABLE endpoint_revision (
          endpoint_id          TEXT NOT NULL,
          revision_id          INTEGER NOT NULL CHECK (revision_id >= 0),
          natural_key          TEXT NOT NULL,
          natural_key_sha256   TEXT NOT NULL
            CHECK (length(natural_key_sha256) = 64),
          legal_entity         TEXT NOT NULL,
          venue_brand          TEXT NOT NULL,
          venue_id             TEXT NOT NULL REFERENCES venue(venue_id),
          product              TEXT NOT NULL,
          environment          TEXT NOT NULL,
          region               TEXT NOT NULL,
          transport            TEXT NOT NULL,
          protocol             TEXT NOT NULL,
          auth_mode            TEXT NOT NULL,
          host                 TEXT NOT NULL,
          port                 INTEGER CHECK (port BETWEEN 1 AND 65535),
          base_path_or_channel TEXT NOT NULL,
          data_level           TEXT NOT NULL,
          scope                TEXT NOT NULL,
          source_schema_revision TEXT NOT NULL,
          documentation_uri    TEXT NOT NULL,
          documentation_sha256 TEXT CHECK (
            documentation_sha256 IS NULL OR
            length(documentation_sha256) = 64
          ),
          effective_from       TEXT NOT NULL,
          valid_until          TEXT NOT NULL,
          registered_at        TEXT NOT NULL,
          PRIMARY KEY (endpoint_id, revision_id),
          UNIQUE (natural_key_sha256, revision_id)
        )
        """
    )
    conn.execute(
        "CREATE INDEX idx_endpoint_revision_lookup ON endpoint_revision "
        "(venue_id, product, environment, effective_from, valid_until)"
    )
    conn.execute(
        """
        CREATE TRIGGER trg_endpoint_natural_identity_owner
        BEFORE INSERT ON endpoint_revision
        WHEN EXISTS (
          SELECT 1 FROM endpoint_revision existing
          WHERE existing.natural_key_sha256=NEW.natural_key_sha256
            AND existing.natural_key=NEW.natural_key
            AND existing.endpoint_id<>NEW.endpoint_id
        )
        BEGIN
          SELECT RAISE(
            ABORT, 'endpoint natural identity belongs to another endpoint_id'
          );
        END
        """
    )
    conn.execute(
        """
        CREATE TABLE collection_connection (
          connection_id      TEXT PRIMARY KEY,
          endpoint_id        TEXT NOT NULL,
          endpoint_revision  INTEGER NOT NULL,
          collection_run_id  TEXT NOT NULL,
          connection_ordinal INTEGER NOT NULL,
          opened_at          TEXT NOT NULL,
          opened_at_basis    TEXT NOT NULL,
          closed_at          TEXT,
          close_reason       TEXT,
          UNIQUE (collection_run_id, connection_ordinal),
          CHECK (
            connection_ordinal > 0 OR (
              connection_ordinal = 0 AND
              opened_at_basis = 'legacy_unqualified_recorded_time'
            )
          ),
          FOREIGN KEY (endpoint_id, endpoint_revision)
            REFERENCES endpoint_revision(endpoint_id, revision_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE collection_channel (
          connection_id       TEXT NOT NULL
            REFERENCES collection_connection(connection_id),
          channel_id           TEXT NOT NULL,
          native_channel       TEXT NOT NULL,
          market_id            TEXT REFERENCES market(market_id),
          subscription_key     TEXT NOT NULL,
          subscription_sha256  TEXT NOT NULL
            CHECK (length(subscription_sha256) = 64),
          subscribed_at        TEXT NOT NULL,
          subscribed_at_basis  TEXT NOT NULL,
          unsubscribed_at      TEXT,
          capability_venue_id  TEXT,
          capability_domain    TEXT,
          capability_endpoint  TEXT,
          capability_revision  INTEGER,
          PRIMARY KEY (connection_id, channel_id),
          UNIQUE (connection_id, subscription_sha256, subscribed_at),
          CHECK (
            (capability_venue_id IS NULL AND capability_domain IS NULL AND
             capability_endpoint IS NULL AND capability_revision IS NULL) OR
            (capability_venue_id IS NOT NULL AND capability_domain IS NOT NULL
             AND capability_endpoint IS NOT NULL AND
             capability_revision IS NOT NULL)
          ),
          FOREIGN KEY (
            capability_venue_id, capability_domain,
            capability_endpoint, capability_revision
          ) REFERENCES venue_capability_revision (
            venue_id, domain, endpoint, revision_id
          )
        )
        """
    )


def _legacy_base_path(path: str, channel: str) -> str:
    """无损合并 v17 的 path/channel；明确标记无法等同工作簿原字段。"""
    clean_path = path.strip() or "/"
    clean_channel = channel.strip()
    if not clean_channel:
        return clean_path
    return f"{clean_path} | legacy-channel={clean_channel}"


def _migrate_endpoint_identity_v18(conn: sqlite3.Connection) -> None:
    """非破坏迁移 v17 端点身份，并保留全部被引用连接。

    旧表永久保留为 ``endpoint_revision_v17_archive``。工作簿三行若没有连接
    引用，只留档、不把错误自然键带入现行表；若有引用，则以十亿偏移的保留
    修订迁入并同步连接外键，从而给正确的 revision 0 留出位置。
    """
    columns = _endpoint_table_columns(conn)
    if "base_path_or_channel" in columns:
        return
    expected_legacy = {"path", "channel", "source_schema_revision"}
    if not expected_legacy.issubset(columns):
        raise RuntimeError("endpoint_revision has an unknown pre-v18 schema")
    archive_exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' "
        "AND name='endpoint_revision_v17_archive'"
    ).fetchone()
    if archive_exists is not None:
        raise RuntimeError("v17 endpoint archive already exists before migration")

    conn.commit()
    conn.execute("PRAGMA foreign_keys=OFF")
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "ALTER TABLE collection_channel "
            "RENAME TO collection_channel_v17_migration"
        )
        conn.execute(
            "ALTER TABLE collection_connection "
            "RENAME TO collection_connection_v17_migration"
        )
        conn.execute(
            "ALTER TABLE endpoint_revision "
            "RENAME TO endpoint_revision_v17_archive"
        )
        conn.execute("DROP INDEX IF EXISTS idx_endpoint_revision_lookup")
        conn.execute("DROP TRIGGER IF EXISTS trg_endpoint_natural_identity_owner")
        _create_v18_endpoint_control_tables(conn)

        referenced = {
            (str(row[0]), int(row[1]))
            for row in conn.execute(
                "SELECT DISTINCT endpoint_id, endpoint_revision "
                "FROM collection_connection_v17_migration"
            )
        }
        revision_map: dict[tuple[str, int], int] = {}
        old_rows = conn.execute(
            "SELECT endpoint_id, revision_id, natural_key, "
            "natural_key_sha256, legal_entity, venue_id, product, "
            "environment, region, transport, protocol, auth_mode, host, "
            "port, path, channel, source_schema_revision, documentation_uri, "
            "documentation_sha256, effective_from, valid_until, registered_at "
            "FROM endpoint_revision_v17_archive"
        ).fetchall()
        insert_endpoint = (
            "INSERT INTO endpoint_revision VALUES ("
            + ",".join("?" for _ in range(24))
            + ")"
        )
        brands = {
            "bitflyer": "bitFlyer",
            "bitbank": "bitbank",
            "gmo": "GMO Coin",
        }
        for old in old_rows:
            endpoint_id = str(old[0])
            old_revision = int(old[1])
            old_key = (endpoint_id, old_revision)
            is_referenced = old_key in referenced
            if endpoint_id in _WORKBOOK_ENDPOINT_IDS and not is_referenced:
                continue
            new_revision = old_revision
            if endpoint_id in _WORKBOOK_ENDPOINT_IDS:
                new_revision += _LEGACY_REVISION_OFFSET
            revision_map[old_key] = new_revision
            venue_id = str(old[5])
            identity = EndpointNaturalIdentity(
                legal_entity=str(old[4]),
                venue_brand=brands.get(venue_id, venue_id),
                product=str(old[6]),
                environment=str(old[7]),
                region=str(old[8]),
                transport=str(old[9]),
                protocol=str(old[10]),
                auth_mode=str(old[11]),
                host=str(old[12]),
                port=None if old[13] is None else int(old[13]),
                base_path_or_channel=_legacy_base_path(
                    str(old[14]), str(old[15])
                ),
                data_level="legacy-v17-unclassified",
            )
            migrated = EndpointRevisionRow(
                endpoint_id=endpoint_id,
                revision_id=new_revision,
                venue_id=venue_id,
                identity=identity,
                scope=f"legacy-v17 channel={str(old[15]).strip() or '<empty>'}",
                source_schema_revision=str(old[16]),
                documentation_uri=str(old[17]),
                documentation_sha256=(
                    None if old[18] is None else str(old[18])
                ),
                effective_from=str(old[19]),
                valid_until=str(old[20]),
                registered_at=str(old[21]),
            )
            conn.execute(insert_endpoint, migrated.as_db_row())

        for connection in conn.execute(
            "SELECT connection_id, endpoint_id, endpoint_revision, "
            "collection_run_id, reconnect_ordinal, opened_at, closed_at, "
            "close_reason FROM collection_connection_v17_migration"
        ).fetchall():
            old_key = (str(connection[1]), int(connection[2]))
            conn.execute(
                "INSERT INTO collection_connection "
                "(connection_id,endpoint_id,endpoint_revision,"
                "collection_run_id,connection_ordinal,opened_at,"
                "opened_at_basis,closed_at,close_reason) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    connection[0],
                    connection[1],
                    revision_map[old_key],
                    *connection[3:6],
                    "legacy_unqualified_recorded_time",
                    *connection[6:],
                ),
            )
        conn.execute(
            "INSERT INTO collection_channel "
            "(connection_id,channel_id,native_channel,market_id,"
            "subscription_key,subscription_sha256,subscribed_at,"
            "subscribed_at_basis,unsubscribed_at,capability_venue_id,"
            "capability_domain,capability_endpoint,capability_revision) "
            "SELECT connection_id,channel_id,native_channel,market_id,"
            "subscription_key,subscription_sha256,subscribed_at,"
            "'legacy_unqualified_recorded_time',unsubscribed_at,"
            "capability_venue_id,capability_domain,capability_endpoint,"
            "capability_revision FROM collection_channel_v17_migration"
        )
        conn.execute("DROP TABLE collection_channel_v17_migration")
        conn.execute("DROP TABLE collection_connection_v17_migration")
        violations = [
            *conn.execute("PRAGMA foreign_key_check(collection_connection)"),
            *conn.execute("PRAGMA foreign_key_check(collection_channel)"),
        ]
        if violations:
            raise RuntimeError(f"v18 endpoint migration broke foreign keys: {violations}")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.execute("PRAGMA foreign_keys=ON")


def _ensure_v18_observation_basis_columns(conn: sqlite3.Connection) -> None:
    """修复短暂存在过的无观察基准 v18 测试库。"""
    connection_columns = {
        str(row[1])
        for row in conn.execute("PRAGMA table_info(collection_connection)")
    }
    if (
        "connection_ordinal" not in connection_columns
        and "reconnect_ordinal" in connection_columns
    ):
        conn.execute(
            "ALTER TABLE collection_connection RENAME COLUMN "
            "reconnect_ordinal TO connection_ordinal"
        )
    additions = (
        (
            "collection_connection", "opened_at_basis",
            "legacy_unqualified_recorded_time",
        ),
        (
            "collection_channel", "subscribed_at_basis",
            "legacy_unqualified_recorded_time",
        ),
    )
    for table, column, default in additions:
        columns = {
            str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")
        }
        if column not in columns:
            conn.execute(
                f"ALTER TABLE {table} ADD COLUMN {column} TEXT NOT NULL "
                f"DEFAULT '{default}'"
            )


def _connect_unlocked(data_root: Path) -> sqlite3.Connection:
    """在调用方持有写锁时连接库文件并保证结构就绪。"""
    data_root.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(data_root / DB_FILE_NAME)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    # 覆盖表跨进程写，等锁三十秒
    conn.execute("PRAGMA busy_timeout=30000")
    version = int(conn.execute("PRAGMA user_version").fetchone()[0])
    if version > DB_SCHEMA_VERSION:
        conn.close()
        raise RuntimeError(
            f"SQLite schema v{version} is newer than supported "
            f"v{DB_SCHEMA_VERSION}"
        )
    conn.executescript(_SCHEMA)
    _migrate_endpoint_identity_v18(conn)
    _ensure_v18_observation_basis_columns(conn)
    # 版本 15：协议控制行与真正拒绝行分账。
    for table in (
        "partition_attempt", "partition_input", "partition_input_binding"
    ):
        table_columns = {
            row[1] for row in conn.execute(f"PRAGMA table_info({table})")
        }
        if "ignored_rows" not in table_columns:
            conn.execute(
                f"ALTER TABLE {table} ADD COLUMN ignored_rows "
                "INTEGER NOT NULL DEFAULT 0 CHECK (ignored_rows >= 0)"
            )
    # 存量库补分析列
    cols = {row[1] for row in conn.execute("PRAGMA table_info(book_feature)")}
    additions = {
        "request_hash": "TEXT",
        "run_id": "TEXT",
        "code_version": "TEXT",
        "confidence_version": "TEXT",
    }
    for name, data_type in additions.items():
        if name not in cols:
            conn.execute(
                f"ALTER TABLE book_feature ADD COLUMN {name} {data_type}"
            )
    # 存量库补台账区域参数列（版本 6）
    run_cols = {
        row[1] for row in conn.execute("PRAGMA table_info(analysis_run)")
    }
    run_additions = {
        "basis": "TEXT NOT NULL DEFAULT 'quantity'",
        "window_columns": "INTEGER NOT NULL DEFAULT 0",
    }
    for name, data_type in run_additions.items():
        if name not in run_cols:
            conn.execute(
                f"ALTER TABLE analysis_run ADD COLUMN {name} {data_type}"
            )
    # 新旧库统一建唯一索引
    # SQLite 唯一索引允许多个 NULL
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_book_feature_request_hash "
        "ON book_feature(request_hash)"
    )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_alert_event_feature_rule "
        "ON alert_event(feature_id, rule_id)"
    )
    if version < 10:
        conn.execute(
            "INSERT OR IGNORE INTO materialization_partition_head "
            "(market_id, domain, partition_key, normalization_version, "
            "attempt_id, activated_at) "
            "SELECT a.market_id, a.domain, a.partition_key, "
            "a.normalization_version, a.attempt_id, a.finished_at "
            "FROM partition_attempt a WHERE a.status IN "
            "('complete', 'complete_with_rejections') AND NOT EXISTS ("
            "SELECT 1 FROM partition_attempt newer WHERE "
            "newer.market_id=a.market_id AND newer.domain=a.domain AND "
            "newer.partition_key=a.partition_key AND "
            "newer.status IN ('complete', 'complete_with_rejections') AND ("
            "newer.finished_at>a.finished_at OR ("
            "newer.finished_at=a.finished_at AND newer.attempt_id>a.attempt_id)))"
        )
    conn.execute(
        "INSERT OR IGNORE INTO artifact_location "
        "SELECT artifact_id, storage_path, registered_at, 1 FROM artifact"
    )
    # v13：存量物化来自锁定端点。
    # 有能力证据才补批级外键。
    # 迁移行明确标记为推断绑定。
    conn.execute(
        "INSERT OR IGNORE INTO partition_capability_binding "
        "(attempt_id, venue_id, domain, endpoint, revision_id, "
        "binding_basis, bound_at) "
        "SELECT p.attempt_id, m.venue_id, p.domain, e.endpoint, "
        "MAX(c.revision_id), 'migration-inferred', "
        "COALESCE(p.finished_at, p.started_at) "
        "FROM partition_attempt p JOIN market m ON m.market_id=p.market_id "
        "JOIN (SELECT 'gmo' venue_id, 'trades/archive' endpoint "
        "UNION ALL SELECT 'bitbank', 'transactions/{day}' "
        "UNION ALL SELECT 'bitflyer', '/v1/executions' "
        "UNION ALL SELECT 'binance', 'data.binance.vision/aggTrades') e "
        "ON e.venue_id=m.venue_id "
        "JOIN venue_capability_revision c ON c.venue_id=m.venue_id "
        "AND c.domain=p.domain AND c.endpoint=e.endpoint "
        "AND c.available=1 AND c.implementation_status='implemented' "
        "WHERE p.domain='trade' "
        "GROUP BY p.attempt_id, m.venue_id, p.domain, e.endpoint"
    )
    # v14：空制品可以服务多个分区。
    # 每个制品只许一个主位置。
    # 依主表路径修复旧标记。
    if version < 14:
        conn.execute(
            "UPDATE artifact_location SET is_canonical=CASE WHEN "
            "storage_path=(SELECT a.storage_path FROM artifact a WHERE "
            "a.artifact_id=artifact_location.artifact_id) THEN 1 ELSE 0 END"
        )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_artifact_one_canonical "
        "ON artifact_location(artifact_id) WHERE is_canonical=1"
    )
    if version < DB_SCHEMA_VERSION:
        conn.execute(f"PRAGMA user_version={DB_SCHEMA_VERSION}")
        conn.execute(
            "INSERT OR IGNORE INTO meta_schema_history VALUES (?, ?, ?)",
            (DB_SCHEMA_VERSION, datetime.now(UTC).isoformat(), None),
        )
    # 初始化也可能开启写事务。
    # 交还前释放锁，允许并发读。
    conn.commit()
    return conn


def connect(data_root: Path) -> sqlite3.Connection:
    """连接共享 SQLite；结构检查与迁移按数据根目录串行。"""
    from guvolu.data.sqlite_writer_lock import sqlite_writer_lock

    with sqlite_writer_lock(data_root):
        return _connect_unlocked(data_root)


def register_endpoint_revisions(
    conn: sqlite3.Connection,
    rows: Iterable[EndpointRevisionRow],
) -> int:
    """追加端点修订；完全相同行可重放，冲突身份拒绝覆盖。

    稳定 ID 由工作簿/登记流程提供，自然键只做唯一性与复算校验。此函数不
    扫描或猜测 ``venue_capability_revision.endpoint`` 的文本值。
    """
    prepared = [row.as_db_row() for row in rows]
    before = conn.total_changes
    statement = (
        "INSERT OR IGNORE INTO endpoint_revision ("
        "endpoint_id, revision_id, natural_key, natural_key_sha256, "
        "legal_entity, venue_brand, venue_id, product, environment, region, "
        "transport, protocol, auth_mode, host, port, base_path_or_channel, "
        "data_level, scope, source_schema_revision, documentation_uri, "
        "documentation_sha256, effective_from, valid_until, registered_at"
        ") VALUES ("
        + ",".join("?" for _ in range(24))
        + ")"
    )
    for row in prepared:
        owner = conn.execute(
            "SELECT DISTINCT endpoint_id FROM endpoint_revision "
            "WHERE natural_key_sha256=? AND natural_key=?",
            (row[3], row[2]),
        ).fetchall()
        if owner and owner != [(row[0],)]:
            conn.rollback()
            raise ValueError(
                f"endpoint natural identity conflicts with stable ID: {row[0]}"
            )
        try:
            conn.execute(statement, row)
        except sqlite3.IntegrityError as exc:
            conn.rollback()
            raise ValueError(
                f"endpoint revision conflicts with registry: {row[0]} r{row[1]}"
            ) from exc
        existing = conn.execute(
            "SELECT endpoint_id, revision_id, natural_key, natural_key_sha256, "
            "legal_entity, venue_brand, venue_id, product, environment, region, "
            "transport, protocol, auth_mode, host, port, base_path_or_channel, "
            "data_level, scope, source_schema_revision, documentation_uri, "
            "documentation_sha256, effective_from, valid_until, registered_at "
            "FROM endpoint_revision WHERE endpoint_id=? AND revision_id=?",
            (row[0], row[1]),
        ).fetchone()
        if existing != row:
            conn.rollback()
            raise ValueError(
                f"endpoint revision conflicts with immutable row: {row[0]} r{row[1]}"
            )
    conn.commit()
    return conn.total_changes - before


KlineRow = tuple[
    str, str, str, str, str, str, str, str, str, str, str, int, str
]


def upsert_klines(conn: sqlite3.Connection, rows: list[KlineRow]) -> int:
    """幂等写入，同键较新摄取时刻胜出，返回变更行数。

    会话中抓取的行是临时值，完结后重取的行摄取时刻更新，
    据此覆盖；重放与乱序不影响终态（与摄取次序无关）。
    """
    before = conn.total_changes
    conn.executemany(
        "INSERT INTO kline VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) "
        "ON CONFLICT(symbol, interval, open_time, revision_id) DO UPDATE SET "
        "available_time=excluded.available_time, "
        "ingest_time=excluded.ingest_time, "
        "trading_day=excluded.trading_day, "
        "open=excluded.open, high=excluded.high, low=excluded.low, "
        "close=excluded.close, volume=excluded.volume, "
        "raw_source=excluded.raw_source "
        "WHERE excluded.ingest_time > kline.ingest_time",
        rows,
    )
    conn.commit()
    return conn.total_changes - before


def fetched_periods(conn: sqlite3.Connection) -> set[tuple[str, str, str]]:
    """已取期集合：交易日与年份两种粒度并存。

    含临时行（摄取早于可得时刻）的期不算已取，
    使求缺在期完结后自动重取（D-04 可得时刻纪律）。
    """
    out: set[tuple[str, str, str]] = set()
    year_ok: dict[tuple[str, str, str], bool] = {}
    query = (
        "SELECT symbol, interval, trading_day, "
        "MIN(ingest_time >= available_time) FROM kline "
        "GROUP BY symbol, interval, trading_day"
    )
    for symbol, interval, day, final in conn.execute(query):
        year_key = (symbol, interval, day[:4])
        year_ok[year_key] = year_ok.get(year_key, True) and bool(final)
        if final:
            out.add((symbol, interval, day))
    out.update(key for key, ok in year_ok.items() if ok)
    return out


def connect_readonly(data_root: Path) -> sqlite3.Connection | None:
    """只读连接，库不存在时返回 None。"""
    path = data_root / DB_FILE_NAME
    if not path.exists():
        return None
    uri = f"file:{path.as_posix()}?mode=ro"
    return sqlite3.connect(uri, uri=True)


def query_klines(
    conn: sqlite3.Connection,
    symbol: str,
    interval: str,
    from_day: str,
    to_day: str,
) -> list[tuple[str, str, str, str, str, str]]:
    """按交易日区间取 K 线，返回时间与五值文本。"""
    query = (
        "SELECT open_time, open, high, low, close, volume FROM kline "
        "WHERE symbol=? AND interval=? AND trading_day>=? AND trading_day<=? "
        "AND revision_id=0 ORDER BY open_time"
    )
    return [
        tuple(row)
        for row in conn.execute(query, (symbol, interval, from_day, to_day))
    ]


def kline_counts(conn: sqlite3.Connection) -> list[tuple[str, str, int, str, str]]:
    """按品种与周期统计根数与时段，供汇报。"""
    query = (
        "SELECT symbol, interval, COUNT(*), MIN(open_time), MAX(open_time) "
        "FROM kline GROUP BY symbol, interval ORDER BY symbol, interval"
    )
    return [tuple(row) for row in conn.execute(query)]


VenueRow = tuple[str, str, str, str, str, int]
InstrumentRow = tuple[str, str, str, str]
InstrumentMapRow = tuple[
    str, str, str, str | None, str | None, str | None, int, str, str
]
CoverageRow = tuple[
    str, str, str, str, int | None, str | None, str | None, str, str
]
CapabilityRow = tuple[
    str, str, str, int, int, str, str, str, str, str, str, str, str,
    str, str, str, int,
]
TradeTickRow = tuple[
    str, str, str, str, str, str, str, str, str, str, str, str,
    str | None, str | None, str | None, str, str, int, int, int, str,
]
BookTopRow = tuple[
    str, str, str, str, str, str, str, str, str, str, int, int, str,
    str | None, str, int, int, str,
]
MarketRow = tuple[str, str, str, str, int, str, str]
ArtifactRow = tuple[str, str, str, str, int, str, str, str, int]


def register_dimensions(
    conn: sqlite3.Connection,
    venues: list[VenueRow],
    instruments: list[InstrumentRow],
    mappings: list[InstrumentMapRow],
) -> int:
    """登记维度行，只增不改（D-05、D-06）。"""
    before = conn.total_changes
    conn.executemany(
        "INSERT OR IGNORE INTO venue VALUES (?,?,?,?,?,?)", venues
    )
    conn.executemany(
        "INSERT OR IGNORE INTO instrument VALUES (?,?,?,?)", instruments
    )
    conn.executemany(
        "INSERT OR IGNORE INTO instrument_map VALUES (?,?,?,?,?,?,?,?,?)",
        mappings,
    )
    conn.commit()
    return conn.total_changes - before


def register_markets(
    conn: sqlite3.Connection, rows: list[MarketRow]
) -> int:
    """登记来源市场及其确定映射修订。"""
    before = conn.total_changes
    conn.executemany(
        "INSERT OR IGNORE INTO market VALUES (?,?,?,?,?,?,?)", rows
    )
    conn.commit()
    return conn.total_changes - before


def register_artifact(conn: sqlite3.Connection, row: ArtifactRow) -> int:
    """登记内容制品，不允许同身份改写元数据。"""
    before = conn.total_changes
    conn.execute(
        "INSERT OR IGNORE INTO artifact VALUES (?,?,?,?,?,?,?,?,?)", row
    )
    found = conn.execute(
        "SELECT artifact_kind, storage_path, sha256, byte_count, sealed_at, "
        "verification_method, schema_version FROM artifact "
        "WHERE artifact_id=?",
        (row[0],),
    ).fetchone()
    expected = (row[1], row[3], row[4], row[7], row[8])
    actual = None if found is None else (
        found[0], found[2], found[3], found[5], found[6]
    )
    if actual != expected:
        conn.rollback()
        raise ValueError(f"制品身份冲突: {row[0]}")
    canonical = 1 if str(found[1]) == row[2] else 0
    conn.execute(
        "INSERT OR IGNORE INTO artifact_location VALUES (?,?,?,?)",
        (row[0], row[2], row[6], canonical),
    )
    location = conn.execute(
        "SELECT artifact_id FROM artifact_location WHERE storage_path=?",
        (row[2],),
    ).fetchone()
    if location is None or str(location[0]) != row[0]:
        conn.rollback()
        raise ValueError(f"制品路径冲突: {row[2]}")
    conn.commit()
    return conn.total_changes - before


def upsert_coverage(conn: sqlite3.Connection, rows: list[CoverageRow]) -> int:
    """覆盖登记幂等写入，同键以新摄取时刻覆盖。"""
    before = conn.total_changes
    conn.executemany(
        "INSERT INTO archive_coverage VALUES (?,?,?,?,?,?,?,?,?) "
        "ON CONFLICT(venue_id, venue_symbol, domain, day) DO UPDATE SET "
        "rows=excluded.rows, first_ts=excluded.first_ts, "
        "last_ts=excluded.last_ts, status=excluded.status, "
        "ingest_time=excluded.ingest_time",
        rows,
    )
    conn.commit()
    return conn.total_changes - before


def coverage_days(
    conn: sqlite3.Connection,
    venue_id: str,
    venue_symbol: str,
    domain: str,
) -> dict[str, str]:
    """已登记日到状态的映射，供求缺与续传。"""
    query = (
        "SELECT day, status FROM archive_coverage "
        "WHERE venue_id=? AND venue_symbol=? AND domain=?"
    )
    return {
        str(day): str(status)
        for day, status in conn.execute(query, (venue_id, venue_symbol, domain))
    }


def coverage_summary(
    conn: sqlite3.Connection,
) -> list[tuple[str, str, str, int, int, int, int, str | None, str | None, int]]:
    """按来源与品种汇总覆盖，供汇报与界面。"""
    query = (
        "SELECT venue_id, venue_symbol, domain, COUNT(*), "
        "SUM(status='ok'), SUM(status='missing'), SUM(status='empty'), "
        "MIN(day), MAX(day), COALESCE(SUM(rows), 0) "
        "FROM archive_coverage "
        "GROUP BY venue_id, venue_symbol, domain "
        "ORDER BY venue_id, venue_symbol, domain"
    )
    return [tuple(row) for row in conn.execute(query)]


def register_capabilities(
    conn: sqlite3.Connection, rows: list[CapabilityRow]
) -> int:
    """追加能力证据版本，不覆盖旧结论。"""
    before = conn.total_changes
    conn.executemany(
        "INSERT OR IGNORE INTO venue_capability_revision "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        rows,
    )
    conn.commit()
    return conn.total_changes - before


def insert_trade_ticks(
    conn: sqlite3.Connection, rows: list[TradeTickRow]
) -> int:
    """追加规范化逐笔，同版本重放幂等。"""
    before = conn.total_changes
    conn.executemany(
        "INSERT OR IGNORE INTO trade_tick VALUES "
        "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        rows,
    )
    conn.commit()
    return conn.total_changes - before


def insert_book_tops(
    conn: sqlite3.Connection, rows: list[BookTopRow]
) -> int:
    """追加顶档帧，帧标识消除同刻碰撞。"""
    before = conn.total_changes
    conn.executemany(
        "INSERT OR IGNORE INTO book_top VALUES "
        "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        rows,
    )
    conn.commit()
    return conn.total_changes - before


BackfillRunRow = tuple[
    str, str, str, str, str, str, int, int, int, int, int, int, str,
    str | None, str, str | None, str, str,
]


NormalizedPartitionRow = tuple[
    str, str, str, str, str, str, int, int, int, str, str, str, int, str,
    str | None,
]


def normalized_partition(
    conn: sqlite3.Connection,
    venue_id: str,
    venue_symbol: str,
    domain: str,
    day: str,
    raw_source: str,
) -> tuple[str, str, int, int, int] | None:
    """读取既有分区的散列、状态与行数。"""
    row = conn.execute(
        "SELECT source_sha256, status, raw_rows, normalized_rows, rejected_rows "
        "FROM normalized_partition WHERE venue_id=? AND venue_symbol=? "
        "AND domain=? AND day=? AND raw_source=?",
        (venue_id, venue_symbol, domain, day, raw_source),
    ).fetchone()
    if row is None:
        return None
    return (str(row[0]), str(row[1]), int(row[2]), int(row[3]), int(row[4]))


def upsert_normalized_partition(
    conn: sqlite3.Connection, row: NormalizedPartitionRow
) -> int:
    """登记归一化分区终态，内容散列变化时覆盖台账。"""
    before = conn.total_changes
    conn.execute(
        "INSERT INTO normalized_partition VALUES "
        "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
        "ON CONFLICT(venue_id, venue_symbol, domain, day, raw_source) "
        "DO UPDATE SET source_sha256=excluded.source_sha256, "
        "raw_rows=excluded.raw_rows, normalized_rows=excluded.normalized_rows, "
        "rejected_rows=excluded.rejected_rows, started_at=excluded.started_at, "
        "finished_at=excluded.finished_at, "
        "normalization_version=excluded.normalization_version, "
        "schema_version=excluded.schema_version, status=excluded.status, "
        "failure_detail=excluded.failure_detail",
        row,
    )
    conn.commit()
    return conn.total_changes - before


StreamHealthRow = tuple[
    str, str, str, str, str | None, int, int, int, int, int, int, str,
]


def upsert_stream_health(
    conn: sqlite3.Connection, rows: list[StreamHealthRow]
) -> int:
    """按来源窗口登记流健康终态。"""
    before = conn.total_changes
    conn.executemany(
        "INSERT INTO stream_health VALUES (?,?,?,?,?,?,?,?,?,?,?,?) "
        "ON CONFLICT(venue_id, channel, instrument_id, window_start) "
        "DO UPDATE SET last_event_time=excluded.last_event_time, "
        "frames=excluded.frames, sequence_gaps=excluded.sequence_gaps, "
        "sequence_regressions=excluded.sequence_regressions, "
        "checksum_failures=excluded.checksum_failures, "
        "snapshot_mismatches=excluded.snapshot_mismatches, "
        "reconnects=excluded.reconnects, status=excluded.status",
        rows,
    )
    conn.commit()
    return conn.total_changes - before


def insert_backfill_run(
    conn: sqlite3.Connection, row: BackfillRunRow
) -> int:
    """追加回补任务终态，同 run 重放幂等。"""
    before = conn.total_changes
    conn.execute(
        "INSERT OR IGNORE INTO backfill_run ("
        "run_id, venue_id, venue_symbol, domain, from_day, to_day, "
        "planned_parts, ok_parts, missing_parts, empty_parts, rows, "
        "checksum_failures, status, failure_detail, started_at, finished_at, "
        "config_hash, code_version) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        row,
    )
    conn.commit()
    return conn.total_changes - before


AnalysisRunRow = tuple[
    str, str, str, str, str, str, str, str, str, str, str, str, str,
    str, str, str, str, str, str | None, str, int,
]

_ANALYSIS_RUN_COLUMNS = (
    "run_id, request_hash, venue_id, symbol, price_low, price_high, "
    "from_ts, to_ts, bucket, judgments, baseline, baseline_hash, "
    "source_hash, config_hash, code_version, confidence_version, "
    "created_at, status, failure_detail, basis, window_columns"
)


def insert_analysis_run(
    conn: sqlite3.Connection, row: AnalysisRunRow
) -> str:
    """保存四判定全量台账，同请求复用 run。

    列名显式列举：迁移库列序与新建库不同，
    位置式插入在两形态间不可靠。
    """
    conn.execute(
        f"INSERT INTO analysis_run ({_ANALYSIS_RUN_COLUMNS}) VALUES "
        "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
        "ON CONFLICT(request_hash) DO NOTHING",
        row,
    )
    conn.commit()
    found = conn.execute(
        "SELECT run_id FROM analysis_run WHERE request_hash=?", (row[1],)
    ).fetchone()
    assert found is not None
    return str(found[0])


AnalysisRunOut = tuple[
    str, str, str, str, str, str, str, str, int, str, str, str, str,
    str, str, str,
]


def list_analysis_runs(
    conn: sqlite3.Connection, symbol: str | None, limit: int
) -> list[AnalysisRunOut]:
    """台账时序清单：按创建时刻倒序（6.4 节检索）。"""
    query = (
        "SELECT run_id, symbol, price_low, price_high, from_ts, to_ts, "
        "bucket, basis, window_columns, judgments, baseline_hash, "
        "config_hash, code_version, confidence_version, created_at, status "
        "FROM analysis_run "
    )
    args: tuple[object, ...]
    if symbol is None:
        args = (limit,)
    else:
        query += "WHERE symbol=? "
        args = (symbol, limit)
    query += "ORDER BY created_at DESC, run_id DESC LIMIT ?"
    return [tuple(row) for row in conn.execute(query, args)]


BookFeatureRow = tuple[
    str, str, str, str, str, str, str, str, str, str, str
]


def insert_book_feature(
    conn: sqlite3.Connection,
    row: BookFeatureRow,
    *,
    run_id: str | None = None,
    code_version: str | None = None,
    confidence_version: str | None = None,
) -> int:
    """幂等追加判读事件行，同请求复用既有 feature_id。

    request_hash 为同请求同配置的确定性散列，冲突不改写既有行；
    只增不删语义保持，重放返回原行标识。
    """
    conn.execute(
        "INSERT INTO book_feature "
        "(kind, venue_id, symbol, price_low, price_high, from_ts, to_ts, "
        "metrics, config_hash, created_at, request_hash, run_id, "
        "code_version, confidence_version) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
        "ON CONFLICT(request_hash) DO NOTHING",
        (*row, run_id, code_version, confidence_version),
    )
    conn.commit()
    found = conn.execute(
        "SELECT feature_id FROM book_feature WHERE request_hash=?",
        (row[10],),
    ).fetchone()
    assert found is not None
    return int(found[0])


def insert_alert_event(
    conn: sqlite3.Connection, feature_id: int, rule_id: str, triggered_at: str
) -> int:
    """幂等追加报警触发行，同事件同规则复用既有 alert_id。"""
    conn.execute(
        "INSERT OR IGNORE INTO alert_event "
        "(feature_id, rule_id, triggered_at, acked_at) VALUES (?,?,?,NULL)",
        (feature_id, rule_id, triggered_at),
    )
    conn.commit()
    found = conn.execute(
        "SELECT alert_id FROM alert_event "
        "WHERE feature_id=? AND rule_id=?",
        (feature_id, rule_id),
    ).fetchone()
    assert found is not None
    return int(found[0])


AlertRowOut = tuple[
    int, int, str, str, str | None, str, str, str, str, str, str, str
]

_ALERT_SELECT = (
    "SELECT a.alert_id, a.feature_id, a.rule_id, a.triggered_at, a.acked_at, "
    "f.kind, f.symbol, f.price_low, f.price_high, f.from_ts, f.to_ts, "
    "f.metrics FROM alert_event a "
    "JOIN book_feature f ON f.feature_id = a.feature_id "
)


def list_alert_events(
    conn: sqlite3.Connection, limit: int
) -> list[AlertRowOut]:
    """报警清单：未确认优先，再按触发时刻倒序。"""
    query = (
        _ALERT_SELECT
        + "ORDER BY (a.acked_at IS NULL) DESC, a.triggered_at DESC, "
        "a.alert_id DESC LIMIT ?"
    )
    return [tuple(row) for row in conn.execute(query, (limit,))]


def ack_alert_event(
    conn: sqlite3.Connection, alert_id: int, acked_at: str
) -> AlertRowOut | None:
    """确认报警：仅回填 acked_at，已确认者保持原值。

    确认是呈现状态的唯一写动作，无任何交易语义（6.8 节）。
    """
    conn.execute(
        "UPDATE alert_event SET acked_at=? "
        "WHERE alert_id=? AND acked_at IS NULL",
        (acked_at, alert_id),
    )
    conn.commit()
    row = conn.execute(
        _ALERT_SELECT + "WHERE a.alert_id=?", (alert_id,)
    ).fetchone()
    return None if row is None else tuple(row)
