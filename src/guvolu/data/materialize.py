"""P2 分析物化：内容制品、稳定市场键与 Parquet 输出。"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import sqlite3
import sys
import time
import uuid
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Any

import duckdb

from guvolu.data.durable_io import atomic_write_text
from guvolu.data.normalization import (
    NormalizationError,
    NormalizedTrade,
    normalize_trade,
    trade_normalization_version,
)
from guvolu.data.paths import data_root as configured_data_root
from guvolu.data.sqlite_writer_lock import sqlite_writer_lock
from guvolu.data import store
from guvolu.data.storage_paths import (
    relative_storage_path,
    resolve_storage_path,
)
from guvolu.data.projection import (
    ArchivePartition,
    archive_partitions,
    rows_from_partition,
)
from guvolu.venues import archive, registry

DATASET_TRADE = "trade_observation"
TRADE_SCHEMA_VERSION = 1
DEFAULT_NORMALIZATION_VERSION = "trade-normalization-v1"
TRADE_SCHEMA_BY_NORMALIZATION = {
    "trade-normalization-v1": 1,
    "binance-aggtrade-normalization-v2": 1,
    "trade-realtime-normalization-v1": 2,
    "trade-realtime-normalization-v2": 2,
    "trade-realtime-normalization-v3": 3,
    "trade-realtime-normalization-v4": 3,
}
DEFAULT_BACKFILL_MARKETS: tuple[tuple[str, str], ...] = (
    ("bitbank", "btc_jpy"),
    ("bitbank", "eth_jpy"),
    ("bitbank", "xrp_jpy"),
    ("bitflyer", "BTC_JPY"),
    ("bitflyer", "FX_BTC_JPY"),
    *[("gmo", symbol) for symbol in registry.GMO_ARCHIVE_SPOT_SYMBOLS],
    ("binance", "BTCUSDT"),
)
_TRADE_ENDPOINT_BY_VENUE = {
    "gmo": "trades/archive",
    "bitbank": "transactions/{day}",
    "bitflyer": "/v1/executions",
    "binance": "data.binance.vision/aggTrades",
}
HASH_CHUNK_BYTES = 4 * 1024 * 1024
STAGING_BATCH_ROWS = 10_000
_SAFE_VALUE = re.compile(r"^[a-z0-9][a-z0-9._-]*$")
_MONTH = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
_RAW_ANCHOR_NAME = re.compile(r"^sha256-([0-9a-f]{64})\.json$")
_CARDINALITY_CHANGING_DOMAINS = ("book_state", "orderflow_tile")
_DERIVED_MANIFEST_SCHEMAS = {
    "book_state": frozenset({1}),
    "orderflow_tile": frozenset({1, 2}),
}
_DERIVED_OUTPUT_DATASETS = {
    "book_state": frozenset({"book_state_checkpoint"}),
    "orderflow_tile": frozenset({
        "orderflow_tile_column", "orderflow_tile_cell",
    }),
}


@dataclass(frozen=True)
class SourceArtifact:
    """已验证的规范化输入原件。"""

    artifact_id: str
    storage_path: str
    absolute_path: Path
    source_rows: int
    normalized_rows: int
    rejected_rows: int


@dataclass(frozen=True)
class ArchiveInput:
    """月物化使用的一个日归档与内容身份。"""

    partition: ArchivePartition
    artifact: SourceArtifact
    ingest_time: str


@dataclass(frozen=True)
class ArchiveStage:
    """归档流式规范化后的精确计数。"""

    row_count: int
    source_counts: dict[str, int]
    normalized_counts: dict[str, int]
    rejected_counts: dict[str, int]
    rejections: tuple[tuple[str, int, str, str], ...]
    min_event_time: str
    max_event_time: str


@dataclass(frozen=True)
class ArchiveBackfillTask:
    """一个可恢复的来源市场月任务。"""

    venue_id: str
    venue_symbol: str
    market_id: str
    event_month: str
    status: str
    source_rows: int
    normalized_rows: int
    reason: str | None


@dataclass(frozen=True)
class ArchiveBackfillPlan:
    """由覆盖台账、文件位置和活动指针推导的回补计划。"""

    tasks: tuple[ArchiveBackfillTask, ...]

    @property
    def complete_tasks(self) -> tuple[ArchiveBackfillTask, ...]:
        return tuple(task for task in self.tasks if task.status == "complete")

    @property
    def pending_tasks(self) -> tuple[ArchiveBackfillTask, ...]:
        return tuple(task for task in self.tasks if task.status == "pending")

    @property
    def blocked_tasks(self) -> tuple[ArchiveBackfillTask, ...]:
        return tuple(
            task for task in self.tasks if task.status.startswith("blocked_")
        )


@dataclass
class _HeadBinding:
    """活动月的输入位置集合与完成计数。"""

    version: str
    normalized_rows: int
    paths: set[str]
    source_rows: int = 0


@dataclass(frozen=True)
class MaterializationResult:
    """一次物化的可审计结果。"""

    attempt_id: str
    market_id: str
    dataset: str
    partition_key: str
    normalization_version: str
    status: str
    row_count: int
    rejected_rows: int
    output_path: str
    output_artifact_id: str
    reused: bool


@dataclass(frozen=True)
class MaterializationAudit:
    """物化文件与控制台账的一致性结果。"""

    artifacts_checked: int
    outputs_checked: int
    rows_checked: int
    warnings: tuple[str, ...]
    errors: tuple[str, ...]


@dataclass(frozen=True)
class _LegacyInputBindingRepair:
    """一条已核验的旧输入位置修复。"""

    attempt_id: str
    artifact_id: str
    storage_path: str
    source_rows: int
    normalized_rows: int
    ignored_rows: int
    rejected_rows: int
    had_binding: bool


@dataclass(frozen=True)
class _ManifestRepair:
    """一份已由控制台账与输出字节共同核验的旧清单。"""

    path: Path
    schema_version: int
    attempt_id: str
    status: str
    sealed_at: str
    artifact_kind: str
    sha256: str
    byte_count: int


def utc_now() -> str:
    """返回 UTC ISO 时刻。"""
    return datetime.now(UTC).isoformat()


def _bind_trade_capability(
    conn: sqlite3.Connection, attempt_id: str, venue_id: str
) -> dict[str, object]:
    """把物化尝试绑定到当时已实现的逐笔端点能力修订。"""
    endpoint = _TRADE_ENDPOINT_BY_VENUE.get(venue_id)
    if endpoint is None:
        raise ValueError(f"来源没有已核证逐笔端点: {venue_id}")
    found = conn.execute(
        "SELECT revision_id, evidence_level, implementation_status "
        "FROM venue_capability_revision WHERE venue_id=? AND domain='trade' "
        "AND endpoint=? AND available=1 ORDER BY revision_id DESC LIMIT 1",
        (venue_id, endpoint),
    ).fetchone()
    if found is None or str(found[2]) != "implemented":
        raise ValueError(f"逐笔端点尚未实现或无能力证据: {venue_id}/{endpoint}")
    revision_id = int(found[0])
    bound_at = utc_now()
    conn.execute(
        "INSERT INTO partition_capability_binding "
        "(attempt_id, venue_id, domain, endpoint, revision_id, "
        "binding_basis, bound_at) VALUES (?,?, 'trade', ?, ?, 'recorded', ?)",
        (attempt_id, venue_id, endpoint, revision_id, bound_at),
    )
    return {
        "venue_id": venue_id,
        "domain": "trade",
        "endpoint": endpoint,
        "revision_id": revision_id,
        "evidence_level": str(found[1]),
        "binding_basis": "recorded",
    }


def open_analytics() -> Any:
    """打开 UTC 固定的内存 DuckDB 查询连接。"""
    connection: Any = duckdb.connect(":memory:")
    connection.execute("SET TimeZone='UTC'")
    return connection


def sha256_file(path: Path) -> str:
    """流式计算文件 SHA-256。"""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(HASH_CHUNK_BYTES):
            digest.update(chunk)
    return digest.hexdigest()


def artifact_id(sha256: str) -> str:
    """由六十四位散列生成内容制品标识。"""
    normalized = sha256.lower()
    if len(normalized) != 64 or any(
        char not in "0123456789abcdef" for char in normalized
    ):
        raise ValueError("SHA-256 必须为六十四位十六进制")
    return f"sha256-{normalized}"


def _slug(value: str) -> str:
    """生成跨 Windows 与 POSIX 安全的目录值。"""
    slug = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    if not slug:
        raise ValueError(f"无法生成安全目录值: {value!r}")
    return slug


def market_id(
    venue_id: str, venue_symbol: str, mapping_revision: int
) -> str:
    """生成来源市场与映射修订的稳定标识。"""
    if mapping_revision < 0:
        raise ValueError("映射修订号不得为负")
    return (
        f"mkt__{_slug(venue_id)}__{_slug(venue_symbol)}"
        f"__r{mapping_revision}"
    )


def _validate_safe_value(value: str, field: str) -> None:
    """拒绝可能逃逸 Hive 目录的值。"""
    if not _SAFE_VALUE.fullmatch(value):
        raise ValueError(f"{field} 不是安全目录值: {value!r}")


def _resolve_recorded_path(root: Path, recorded: str) -> Path:
    """把逻辑台账路径解析到已验证存储根。"""
    normalized = recorded.replace("\\", "/")
    parts = PurePosixPath(normalized).parts
    if parts and parts[0].lower() == root.name.lower():
        normalized = PurePosixPath(*parts[1:]).as_posix()
    return resolve_storage_path(root, normalized)


def _relative_storage_path(root: Path, path: Path) -> str:
    """生成跨热冷根稳定的逻辑路径。"""
    return relative_storage_path(root, path)


def _recorded_path_from_reference(reference: str) -> str:
    """从路径加行号血缘中取原件路径。"""
    path_text, separator, item = reference.rpartition(":")
    return path_text if separator and item.isdigit() else reference


def ensure_markets(conn: sqlite3.Connection) -> int:
    """为全部已登记映射建立稳定市场维度。"""
    now = utc_now()
    rows: list[store.MarketRow] = []
    query = (
        "SELECT m.venue_id, m.venue_symbol, m.instrument_id, m.revision_id, "
        "i.kind FROM instrument_map m JOIN instrument i "
        "ON i.instrument_id=m.instrument_id "
        "ORDER BY m.venue_id, m.venue_symbol, m.revision_id"
    )
    for venue, symbol, instrument, revision, kind in conn.execute(query):
        rows.append((
            market_id(str(venue), str(symbol), int(revision)),
            str(venue), str(symbol), str(instrument), int(revision),
            str(kind), now,
        ))
    changed = store.register_markets(conn, rows)
    for row in rows:
        found = conn.execute(
            "SELECT venue_id, venue_symbol, instrument_id, "
            "mapping_revision, market_kind FROM market WHERE market_id=?",
            (row[0],),
        ).fetchone()
        if found is None or tuple(found) != row[1:6]:
            raise ValueError(f"市场标识冲突: {row[0]}")
    return changed


def _market_row(
    conn: sqlite3.Connection,
    venue_id: str,
    venue_symbol: str,
    mapping_revision: int | None,
) -> tuple[str, str, int]:
    """读取指定或最新映射修订的市场。"""
    if mapping_revision is None:
        row = conn.execute(
            "SELECT market_id, instrument_id, mapping_revision FROM market "
            "WHERE venue_id=? AND venue_symbol=? "
            "ORDER BY mapping_revision DESC LIMIT 1",
            (venue_id, venue_symbol),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT market_id, instrument_id, mapping_revision FROM market "
            "WHERE venue_id=? AND venue_symbol=? AND mapping_revision=?",
            (venue_id, venue_symbol, mapping_revision),
        ).fetchone()
    if row is None:
        raise ValueError(f"未登记市场: {venue_id}/{venue_symbol}")
    return str(row[0]), str(row[1]), int(row[2])


def _register_source_artifacts(
    root: Path,
    conn: sqlite3.Connection,
    venue_id: str,
    venue_symbol: str,
    event_month: str,
    normalization_version: str,
) -> list[SourceArtifact]:
    """核验月内输入分区并登记逐文件散列。"""
    day_prefix = event_month.replace("-", "")
    query = (
        "SELECT raw_source, source_sha256, raw_rows, normalized_rows, "
        "rejected_rows FROM normalized_partition "
        "WHERE venue_id=? AND venue_symbol=? AND domain='trade' "
        "AND day LIKE ? AND normalization_version=? "
        "AND status IN ('complete', 'complete_with_rejections') "
        "ORDER BY day, raw_source"
    )
    source_rows = conn.execute(
        query,
        (venue_id, venue_symbol, f"{day_prefix}%", normalization_version),
    ).fetchall()
    if not source_rows:
        raise ValueError("没有符合月份与版本的完成输入分区")
    artifacts: list[SourceArtifact] = []
    registered_at = utc_now()
    for recorded, expected_sha, raw_rows, normalized_rows, rejected_rows in source_rows:
        path = _resolve_recorded_path(root, str(recorded))
        if not path.is_file():
            raise FileNotFoundError(path)
        actual_sha = sha256_file(path)
        if actual_sha != str(expected_sha).lower():
            raise ValueError(f"输入散列不符: {path}")
        storage_path = _relative_storage_path(root, path)
        sealed_at = datetime.fromtimestamp(
            path.stat().st_mtime, UTC
        ).isoformat()
        identity = artifact_id(actual_sha)
        store.register_artifact(conn, (
            identity, "source_archive", storage_path, actual_sha,
            path.stat().st_size, sealed_at, registered_at,
            "sha256-file-v1", 1,
        ))
        artifacts.append(SourceArtifact(
            identity, storage_path, path, int(raw_rows),
            int(normalized_rows), int(rejected_rows),
        ))
    return artifacts


def _input_set_hash(artifacts: list[SourceArtifact]) -> str:
    """散列排序后的内容与位置绑定集合。"""
    body = "\n".join(sorted(
        f"{item.artifact_id}|{item.storage_path}" for item in artifacts
    ))
    return hashlib.sha256(body.encode("ascii")).hexdigest()


def _completed_result(
    conn: sqlite3.Connection,
    selected_market_id: str,
    event_month: str,
    normalization_version: str,
    input_hash: str,
) -> MaterializationResult | None:
    """读取完全相同输入与版本的既有完成结果。"""
    row = conn.execute(
        "SELECT a.attempt_id, a.status, a.normalized_rows, a.rejected_rows, "
        "r.storage_path, r.artifact_id FROM partition_attempt a "
        "JOIN materialization_output o ON o.attempt_id=a.attempt_id "
        "JOIN artifact r ON r.artifact_id=o.artifact_id "
        "WHERE a.market_id=? AND a.domain='trade' AND a.partition_key=? "
        "AND a.normalization_version=? AND a.input_set_hash=? "
        "AND a.status IN ('complete', 'complete_with_rejections') "
        "ORDER BY a.finished_at DESC LIMIT 1",
        (
            selected_market_id, event_month,
            normalization_version, input_hash,
        ),
    ).fetchone()
    if row is None:
        return None
    return MaterializationResult(
        attempt_id=str(row[0]), market_id=selected_market_id,
        dataset=DATASET_TRADE, partition_key=event_month,
        normalization_version=normalization_version, status=str(row[1]),
        row_count=int(row[2]), rejected_rows=int(row[3]),
        output_path=str(row[4]), output_artifact_id=str(row[5]),
        reused=True,
    )


def _month_bounds(event_month: str) -> tuple[str, str]:
    """返回事件月的 UTC 半开区间。"""
    if not _MONTH.fullmatch(event_month):
        raise ValueError("事件月须为 YYYY-MM")
    start = datetime.strptime(event_month, "%Y-%m").replace(tzinfo=UTC)
    if start.month == 12:
        end = start.replace(year=start.year + 1, month=1)
    else:
        end = start.replace(month=start.month + 1)
    return start.isoformat(), end.isoformat()


def _archive_partition_bounds(
    venue_id: str, partition_month: str,
) -> tuple[str, str]:
    """返回来源归档月在 UTC 上的半开区间。"""
    start_text, end_text = _month_bounds(partition_month)
    start = datetime.fromisoformat(start_text)
    end = datetime.fromisoformat(end_text)
    if venue_id == "gmo":
        # GMO 会话日界。
        offset = timedelta(hours=3)
        start -= offset
        end -= offset
    return start.isoformat(), end.isoformat()


def _create_duckdb_table(db: Any) -> None:
    """建立首版逐笔观察临时表。"""
    db.execute(
        """
        CREATE TABLE trade_observation (
          observation_id VARCHAR NOT NULL,
          venue_id VARCHAR NOT NULL,
          venue_symbol VARCHAR NOT NULL,
          market_id VARCHAR NOT NULL,
          mapping_revision INTEGER NOT NULL,
          instrument_id VARCHAR NOT NULL,
          venue_trade_id VARCHAR NOT NULL,
          revision_id INTEGER NOT NULL,
          event_time TIMESTAMPTZ NOT NULL,
          available_time TIMESTAMPTZ NOT NULL,
          ingest_time TIMESTAMPTZ NOT NULL,
          side VARCHAR NOT NULL,
          source_side_basis VARCHAR NOT NULL,
          price VARCHAR NOT NULL,
          size VARCHAR NOT NULL,
          match_granularity VARCHAR NOT NULL,
          id_origin VARCHAR NOT NULL,
          sequence_id VARCHAR,
          first_trade_id VARCHAR,
          last_trade_id VARCHAR,
          time_origin VARCHAR NOT NULL,
          source_artifact_id VARCHAR NOT NULL,
          source_row_index BIGINT NOT NULL,
          normalization_version VARCHAR NOT NULL,
          schema_version INTEGER NOT NULL
        )
        """
    )


def _stage_trade_rows(
    root: Path,
    conn: sqlite3.Connection,
    staging_path: Path,
    venue_id: str,
    venue_symbol: str,
    selected_market_id: str,
    instrument_id: str,
    mapping_revision: int,
    event_month: str,
    normalization_version: str,
    artifacts: list[SourceArtifact],
) -> tuple[int, dict[str, int], str, str]:
    """把已核对旧事实流式写入临时批量文件。"""
    start, end = _month_bounds(event_month)
    artifact_by_path = {
        item.storage_path: item.artifact_id for item in artifacts
    }
    source_path_cache: dict[str, str] = {}
    selected_counts: dict[str, int] = defaultdict(int)
    cursor = conn.execute(
        "SELECT venue_trade_id, revision_id, event_time, available_time, "
        "ingest_time, side, source_side_basis, price, size, "
        "match_granularity, id_origin, sequence_id, first_trade_id, "
        "last_trade_id, time_origin, raw_item_index, raw_source "
        "FROM trade_tick WHERE venue_id=? AND instrument_id=? "
        "AND event_time>=? AND event_time<? AND normalization_version=? "
        "ORDER BY event_time, venue_trade_id, revision_id",
        (venue_id, instrument_id, start, end, normalization_version),
    )
    total = 0
    min_event = ""
    max_event = ""
    with staging_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        while source_batch := cursor.fetchmany(STAGING_BATCH_ROWS):
            output_batch: list[tuple[object, ...]] = []
            for row in source_batch:
                recorded_path = _recorded_path_from_reference(str(row[16]))
                storage_path = source_path_cache.get(recorded_path)
                if storage_path is None:
                    storage_path = _relative_storage_path(
                        root, _resolve_recorded_path(root, recorded_path)
                    )
                    source_path_cache[recorded_path] = storage_path
                source_identity = artifact_by_path.get(storage_path)
                if source_identity is None:
                    continue
                event_time = str(row[2])
                available_time = str(row[3])
                if available_time < event_time:
                    raise ValueError(
                        f"PIT 违规: {venue_id}/{row[0]} {available_time}"
                    )
                observation = (
                    f"{venue_id}|{selected_market_id}|{row[0]}|r{row[1]}"
                )
                output_batch.append((
                    observation, venue_id, venue_symbol, selected_market_id,
                    mapping_revision, instrument_id, str(row[0]), int(row[1]),
                    event_time, available_time, str(row[4]), str(row[5]),
                    str(row[6]), str(row[7]), str(row[8]), str(row[9]),
                    str(row[10]), row[11], row[12], row[13], str(row[14]),
                    source_identity, int(row[15]), normalization_version,
                    TRADE_SCHEMA_VERSION,
                ))
                selected_counts[storage_path] += 1
                min_event = (
                    event_time if not min_event else min(min_event, event_time)
                )
                max_event = (
                    event_time if not max_event else max(max_event, event_time)
                )
            if output_batch:
                writer.writerows(
                    tuple("\\N" if value is None else value for value in row)
                    for row in output_batch
                )
                total += len(output_batch)
        handle.flush()
        os.fsync(handle.fileno())
    return total, dict(selected_counts), min_event, max_event


def _load_staged_rows(db: Any, staging_path: Path) -> None:
    """让 DuckDB 原生批量读取临时 CSV。"""
    escaped = staging_path.as_posix().replace("'", "''")
    db.execute(
        f"COPY trade_observation FROM '{escaped}' "
        "(FORMAT CSV, HEADER false, NULL '\\N')"
    )


def _output_directory(
    root: Path,
    venue_id: str,
    selected_market_id: str,
    event_month: str,
    normalization_version: str,
) -> Path:
    """生成固定 Hive 风格物化目录。"""
    _validate_safe_value(venue_id, "venue_id")
    _validate_safe_value(selected_market_id, "market_id")
    _validate_safe_value(normalization_version, "normalization_version")
    year, month = event_month.split("-", 1)
    logical = PurePosixPath(
        "materialized", DATASET_TRADE,
        f"schema_version={TRADE_SCHEMA_VERSION}",
        f"normalization_version={normalization_version}",
        f"venue_id={venue_id}", f"market_id={selected_market_id}",
        f"event_year={year}", f"event_month={month}",
    )
    return _resolve_recorded_path(root, logical.as_posix())


def _copy_parquet(db: Any, temp_path: Path) -> None:
    """按稳定次序写入 ZSTD Parquet。"""
    escaped = temp_path.as_posix().replace("'", "''")
    db.execute(
        "COPY (SELECT * FROM trade_observation "
        "ORDER BY event_time, venue_trade_id, revision_id) "
        f"TO '{escaped}' (FORMAT PARQUET, COMPRESSION ZSTD, "
        "ROW_GROUP_SIZE 122880)"
    )
    with temp_path.open("rb+") as handle:
        handle.flush()
        os.fsync(handle.fileno())


def _artifact_row_totals(
    artifacts: Sequence[SourceArtifact], counts: dict[str, int]
) -> dict[str, int]:
    """把逐位置行数汇总为逐内容行数。"""
    totals: dict[str, int] = defaultdict(int)
    for item in artifacts:
        totals[item.artifact_id] += counts.get(item.storage_path, 0)
    return dict(totals)


def _validate_staged_contract(
    db: Any,
    selected_market_id: str,
    normalization_version: str,
    artifacts: Sequence[SourceArtifact],
    normalized_counts: dict[str, int],
    expected_rows: int,
) -> None:
    """在输出提交前验证事实身份、版本、粒度与原件绑定。"""
    result = db.execute(
        "SELECT COUNT(*), COUNT(*) - COUNT(DISTINCT observation_id), "
        "SUM(CASE WHEN available_time < event_time THEN 1 ELSE 0 END), "
        "COUNT(DISTINCT market_id), MIN(market_id), "
        "COUNT(DISTINCT normalization_version), "
        "MIN(normalization_version), "
        "COUNT(DISTINCT schema_version), MIN(schema_version), "
        "SUM(CASE WHEN match_granularity NOT IN "
        "('match', 'aggregate') THEN 1 ELSE 0 END) "
        "FROM trade_observation"
    ).fetchone()
    if result is None or int(result[0]) != expected_rows:
        raise ValueError("暂存事实行数与物化计数不符")
    if int(result[1] or 0):
        raise ValueError("暂存事实存在重复 observation_id")
    if int(result[2] or 0):
        raise ValueError("暂存事实存在 PIT 违规")
    if expected_rows == 0:
        if any(_artifact_row_totals(artifacts, normalized_counts).values()):
            raise ValueError("零行物化仍有非零原件规范化计数")
        return
    if int(result[3]) != 1 or str(result[4]) != selected_market_id:
        raise ValueError("暂存事实 market_id 契约不符")
    if int(result[5]) != 1 or str(result[6]) != normalization_version:
        raise ValueError("暂存事实 normalization_version 契约不符")
    if int(result[7]) != 1 or int(result[8]) != TRADE_SCHEMA_VERSION:
        raise ValueError("暂存事实 schema_version 契约不符")
    if int(result[9] or 0):
        raise ValueError("暂存事实存在未知 match_granularity")
    actual_sources = {
        str(row[0]): int(row[1])
        for row in db.execute(
            "SELECT source_artifact_id, COUNT(*) FROM trade_observation "
            "GROUP BY source_artifact_id"
        ).fetchall()
    }
    expected_sources = {
        identity: count
        for identity, count in _artifact_row_totals(
            artifacts, normalized_counts
        ).items()
        if count > 0
    }
    if actual_sources != expected_sources:
        raise ValueError("暂存事实 source_artifact_id 契约不符")


def _finalize_file(temp_path: Path, output_sha: str) -> Path:
    """以内容散列命名并避免覆盖既有制品。"""
    final_path = temp_path.with_name(f"part-{output_sha[:12]}.parquet")
    if final_path.exists():
        if sha256_file(final_path) != output_sha:
            raise ValueError(f"输出文件名散列冲突: {final_path}")
        temp_path.unlink()
    else:
        os.replace(temp_path, final_path)
    return final_path


def _register_content_artifact(
    conn: sqlite3.Connection,
    identity: str,
    kind: str,
    storage_path: str,
    sha256: str,
    byte_count: int,
    created_at: str,
    schema_version: int,
    registered_at: str | None = None,
) -> None:
    """登记内容制品；相同字节可有多个物理位置。"""
    observed_at = registered_at or created_at
    row = (
        identity, kind, storage_path, sha256, byte_count, created_at,
        observed_at, "sha256-file-v1", schema_version,
    )
    conn.execute("INSERT OR IGNORE INTO artifact VALUES (?,?,?,?,?,?,?,?,?)", row)
    recorded = conn.execute(
        "SELECT artifact_kind, storage_path, sha256, byte_count, "
        "verification_method, schema_version FROM artifact WHERE artifact_id=?",
        (identity,),
    ).fetchone()
    expected = (kind, sha256, byte_count, "sha256-file-v1", schema_version)
    actual = None if recorded is None else (
        recorded[0], recorded[2], recorded[3], recorded[4], recorded[5]
    )
    if actual != expected:
        raise ValueError(f"制品身份复用但元数据冲突: {identity}")
    canonical = 1 if str(recorded[1]) == storage_path else 0
    conn.execute(
        "INSERT OR IGNORE INTO artifact_location VALUES (?,?,?,?)",
        (identity, storage_path, observed_at, canonical),
    )
    location = conn.execute(
        "SELECT artifact_id FROM artifact_location WHERE storage_path=?",
        (storage_path,),
    ).fetchone()
    if location is None or str(location[0]) != identity:
        raise ValueError(f"制品路径复用但内容身份冲突: {storage_path}")


def register_materialization_manifest(
    root: Path,
    conn: sqlite3.Connection,
    path: Path,
    schema_version: int,
    sealed_at: str,
    *,
    registered_at: str | None = None,
    artifact_kind: str = "materialization_manifest",
) -> str:
    """把物化清单登记为内容制品并校验尝试身份。"""
    root = root.resolve()
    resolved = path.resolve()
    try:
        resolved.relative_to((root / "materialized").resolve())
    except ValueError as exc:
        raise ValueError("物化清单路径超出物化根") from exc
    if not (
        resolved.is_file()
        and resolved.name.startswith("manifest-")
        and resolved.suffix == ".json"
    ):
        raise ValueError(f"物化清单路径非法: {path}")
    if schema_version <= 0:
        raise ValueError("物化清单结构版本必须为正数")
    if artifact_kind not in {
        "materialization_manifest", "failed_materialization_manifest",
    }:
        raise ValueError("物化清单制品类型非法")
    try:
        body = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"物化清单 JSON 非法: {path}") from exc
    if not isinstance(body, Mapping):
        raise ValueError(f"物化清单必须为对象: {path}")
    attempt_id = body.get("attempt_id")
    if not isinstance(attempt_id, str) or not attempt_id:
        raise ValueError(f"物化清单缺少 attempt_id: {path}")
    if resolved.name != f"manifest-{attempt_id}.json":
        raise ValueError(f"物化清单文件名与 attempt_id 不一致: {path}")
    if conn.execute(
        "SELECT 1 FROM partition_attempt WHERE attempt_id=?", (attempt_id,)
    ).fetchone() is None:
        raise ValueError(f"物化清单尝试未登记: {attempt_id}")
    sha = sha256_file(resolved)
    identity = artifact_id(sha)
    _register_content_artifact(
        conn, identity, artifact_kind,
        _relative_storage_path(root, resolved), sha,
        resolved.stat().st_size, sealed_at, schema_version, registered_at,
    )
    return identity


def _manifest_schema_version(root: Path, path: Path) -> int:
    """从物化目录的结构版本分区读取清单版本。"""
    parts = PurePosixPath(_relative_storage_path(root, path)).parts
    versions = [
        part.removeprefix("schema_version=") for part in parts
        if part.startswith("schema_version=")
    ]
    if len(versions) != 1 or not versions[0].isdigit():
        raise ValueError(f"物化清单结构版本目录非法: {path}")
    version = int(versions[0])
    if version <= 0:
        raise ValueError(f"物化清单结构版本非法: {path}")
    return version


def _manifest_string_set(
    body: Mapping[str, object], key: str, path: Path,
) -> set[str]:
    """读取不允许空值的字符串数组并按事实身份去重。"""
    value = body.get(key)
    if not isinstance(value, list) or any(
        not isinstance(item, str) or not item for item in value
    ):
        raise ValueError(f"物化清单 {key} 非法: {path}")
    return set(value)


def _manifest_upstream_set(
    body: Mapping[str, object], path: Path,
) -> set[str]:
    """兼容早期单数依赖字段，未知形态仍拒绝。"""
    if "upstream_attempt_ids" in body:
        return _manifest_string_set(body, "upstream_attempt_ids", path)
    legacy = body.get("upstream_attempt_id")
    if isinstance(legacy, str) and legacy:
        return {legacy}
    raise ValueError(f"物化清单 upstream_attempt_ids 非法: {path}")


def _verify_parquet_output(
    path: Path, expected_rows: int, expected_schema: int, attempt_id: str,
) -> None:
    """同时核验派生输出的物理行数与行内结构版本。"""
    db = open_analytics()
    try:
        result = db.execute(
            "SELECT COUNT(*),COUNT(DISTINCT schema_version),"
            "MIN(schema_version) FROM read_parquet(?)",
            [str(path)],
        ).fetchone()
    except duckdb.Error as exc:
        raise ValueError(f"物化清单输出不可读: {attempt_id}") from exc
    finally:
        db.close()
    if result is None or int(result[0]) != expected_rows:
        raise ValueError(f"物化清单物理输出行数不符: {attempt_id}")
    if expected_rows and (
        int(result[1]) != 1 or int(result[2]) != expected_schema
    ):
        raise ValueError(f"物化清单物理结构版本不符: {attempt_id}")


def _output_path_in_manifest_directory(
    root: Path, conn: sqlite3.Connection, identity: str,
    manifest: Path, attempt_id: str,
) -> Path:
    """选择与清单同目录的已登记输出位置，拒绝搬移或多义位置。"""
    matches: list[Path] = []
    for row in conn.execute(
        "SELECT storage_path FROM artifact_location WHERE artifact_id=?",
        (identity,),
    ):
        candidate = _resolve_recorded_path(root, str(row[0]))
        if candidate.parent == manifest.parent:
            matches.append(candidate)
    if len(matches) != 1:
        raise ValueError(f"物化清单输出目录绑定不唯一: {attempt_id}")
    return matches[0]


def _verify_failed_output_rows(
    root: Path, conn: sqlite3.Connection, manifest: Path,
    attempt_id: str, domain: str, schema_version: int,
    value: object,
) -> None:
    """核验新失败清单中未晋升输出的完整内容身份。"""
    if not isinstance(value, list) or len(value) != len(
        _DERIVED_OUTPUT_DATASETS[domain]
    ):
        raise ValueError(f"失败物化清单未晋升输出非法: {attempt_id}")
    seen: set[str] = set()
    seen_identities: set[str] = set()
    for item in value:
        if not isinstance(item, Mapping):
            raise ValueError(f"失败物化清单未晋升输出非法: {attempt_id}")
        dataset = item.get("dataset")
        identity = item.get("artifact_id")
        storage_path = item.get("output")
        sha = item.get("sha256")
        rows = item.get("row_count")
        row_schema = item.get("schema_version")
        if (
            not isinstance(dataset, str)
            or dataset not in _DERIVED_OUTPUT_DATASETS[domain]
            or dataset in seen
            or not isinstance(identity, str)
            or identity in seen_identities
            or not isinstance(storage_path, str)
            or not isinstance(sha, str)
            or type(rows) is not int
            or int(rows) < 0
            or type(row_schema) is not int
            or int(row_schema) != schema_version
        ):
            raise ValueError(f"失败物化清单未晋升输出非法: {attempt_id}")
        seen.add(dataset)
        seen_identities.add(identity)
        artifact = conn.execute(
            "SELECT artifact_kind,sha256,byte_count,schema_version "
            "FROM artifact WHERE artifact_id=?",
            (identity,),
        ).fetchone()
        location = conn.execute(
            "SELECT 1 FROM artifact_location WHERE artifact_id=? "
            "AND storage_path=?",
            (identity, storage_path),
        ).fetchone()
        output_path = _resolve_recorded_path(root, storage_path)
        if (
            artifact is None
            or location is None
            or str(artifact[0]) != "materialized_parquet"
            or str(artifact[1]) != sha
            or int(artifact[3]) != schema_version
            or output_path.parent != manifest.parent
            or not output_path.is_file()
            or output_path.stat().st_size != int(artifact[2])
            or sha256_file(output_path) != sha
            or artifact_id(sha) != identity
        ):
            raise ValueError(f"失败物化清单输出证据不符: {attempt_id}")
        _verify_parquet_output(output_path, int(rows), schema_version, attempt_id)
    if seen != _DERIVED_OUTPUT_DATASETS[domain]:
        raise ValueError(f"失败物化清单输出数据集不符: {attempt_id}")


def _verify_legacy_failed_outputs(
    root: Path, conn: sqlite3.Connection, manifest: Path,
    attempt_id: str, body: Mapping[str, object], schema_version: int,
) -> None:
    """只为已知早期 OFL 提交失败形态核验同目录物理输出。"""
    column_rows = body.get("column_rows")
    cell_rows = body.get("cell_rows")
    if (
        not isinstance(column_rows, int)
        or isinstance(column_rows, bool)
        or not isinstance(cell_rows, int)
        or isinstance(cell_rows, bool)
    ):
        raise ValueError(f"旧失败物化清单输出摘要非法: {attempt_id}")
    prefix = _relative_storage_path(root, manifest.parent).rstrip("/") + "/"
    found: dict[str, Path] = {}
    for identity, sha, byte_count, row_schema, storage_path in conn.execute(
        "SELECT a.artifact_id,a.sha256,a.byte_count,a.schema_version,"
        "l.storage_path FROM artifact a JOIN artifact_location l "
        "ON l.artifact_id=a.artifact_id WHERE a.artifact_kind="
        "'materialized_parquet' AND l.storage_path LIKE ?",
        (prefix + "%",),
    ):
        name = PurePosixPath(str(storage_path)).name
        dataset = next((
            candidate for candidate in _DERIVED_OUTPUT_DATASETS["orderflow_tile"]
            if name.startswith(candidate + "-")
        ), None)
        if dataset is None or dataset in found or int(row_schema) != schema_version:
            continue
        output_path = _resolve_recorded_path(root, str(storage_path))
        if (
            output_path.parent != manifest.parent
            or not output_path.is_file()
            or output_path.stat().st_size != int(byte_count)
            or sha256_file(output_path) != str(sha)
            or artifact_id(str(sha)) != str(identity)
        ):
            raise ValueError(f"旧失败物化清单输出证据不符: {attempt_id}")
        found[dataset] = output_path
    if set(found) != _DERIVED_OUTPUT_DATASETS["orderflow_tile"]:
        raise ValueError(f"旧失败物化清单输出数据集不符: {attempt_id}")
    _verify_parquet_output(
        found["orderflow_tile_column"], column_rows,
        schema_version, attempt_id,
    )
    _verify_parquet_output(
        found["orderflow_tile_cell"], cell_rows,
        schema_version, attempt_id,
    )


def _verified_manifest_repair(
    root: Path, conn: sqlite3.Connection, path: Path,
) -> _ManifestRepair:
    """闭集核验一份可安全补登记的物化清单。"""
    root = root.resolve()
    resolved = path.resolve()
    try:
        resolved.relative_to((root / "materialized").resolve())
    except ValueError as exc:
        raise ValueError(f"物化清单路径超出物化根: {path}") from exc
    schema_version = _manifest_schema_version(root, resolved)
    try:
        body = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"物化清单 JSON 非法: {path}") from exc
    if not isinstance(body, Mapping):
        raise ValueError(f"物化清单必须为对象: {path}")
    attempt_id = body.get("attempt_id")
    if not isinstance(attempt_id, str) or not attempt_id:
        raise ValueError(f"物化清单缺少 attempt_id: {path}")
    if resolved.name != f"manifest-{attempt_id}.json":
        raise ValueError(f"物化清单文件名与 attempt_id 不一致: {path}")
    attempt = conn.execute(
        "SELECT market_id,domain,partition_key,normalization_version,status,"
        "normalized_rows,finished_at,failure_detail FROM partition_attempt "
        "WHERE attempt_id=?",
        (attempt_id,),
    ).fetchone()
    if attempt is None:
        raise ValueError(f"物化清单尝试未登记: {attempt_id}")
    market_id, domain, partition_key, normalization_version, status = (
        str(attempt[index]) for index in range(5)
    )
    normalized_rows = int(attempt[5])
    finished_at = attempt[6]
    failure_detail = attempt[7]
    if domain not in {"book_state", "orderflow_tile"}:
        raise ValueError(f"物化清单修复域不受支持: {attempt_id} {domain}")
    expected_directory = (
        "book_state_checkpoint" if domain == "book_state" else "orderflow_tile"
    )
    relative_parts = resolved.relative_to(root / "materialized").parts
    if not relative_parts or relative_parts[0] != expected_directory:
        raise ValueError(f"物化清单目录与域不符: {attempt_id}")
    if schema_version not in _DERIVED_MANIFEST_SCHEMAS[domain]:
        raise ValueError(f"物化清单结构版本不受支持: {attempt_id}")
    if not isinstance(finished_at, str) or not finished_at:
        raise ValueError(f"物化清单尝试没有完成时刻: {attempt_id}")
    if body.get("market_id") != market_id:
        raise ValueError(f"物化清单市场与尝试不符: {attempt_id}")
    if body.get("partition_key") != partition_key:
        raise ValueError(f"物化清单分区与尝试不符: {attempt_id}")
    version_key = (
        "normalization_version" if domain == "book_state" else "method_version"
    )
    if body.get(version_key) != normalization_version:
        raise ValueError(f"物化清单规范化版本不符: {attempt_id}")
    upstream = _manifest_upstream_set(body, resolved)
    recorded_upstream = {
        str(row[0]) for row in conn.execute(
            "SELECT upstream_attempt_id FROM materialization_dependency "
            "WHERE attempt_id=?", (attempt_id,),
        )
    }
    if upstream != recorded_upstream:
        raise ValueError(f"物化清单依赖集合与台账不符: {attempt_id}")
    inputs = _manifest_string_set(body, "input_artifact_ids", resolved)
    recorded_inputs = {
        str(row[0]) for row in conn.execute(
            "SELECT artifact_id FROM partition_input WHERE attempt_id=?",
            (attempt_id,),
        )
    }
    outputs = conn.execute(
        "SELECT o.dataset,o.row_count,o.min_event_time,o.max_event_time,"
        "o.artifact_id,a.sha256,a.byte_count,a.schema_version,a.artifact_kind "
        "FROM materialization_output o JOIN artifact a "
        "ON a.artifact_id=o.artifact_id WHERE o.attempt_id=? "
        "ORDER BY o.dataset,o.artifact_id",
        (attempt_id,),
    ).fetchall()
    head_exists = conn.execute(
        "SELECT 1 FROM materialization_partition_head WHERE attempt_id=?",
        (attempt_id,),
    ).fetchone() is not None
    if status == "failed":
        if outputs or head_exists:
            raise ValueError(f"失败物化清单仍有输出或活动指针: {attempt_id}")
        if recorded_inputs and inputs != recorded_inputs:
            raise ValueError(f"失败物化清单输入集合与台账不符: {attempt_id}")
        missing_inputs = [
            identity for identity in inputs if conn.execute(
                "SELECT 1 FROM artifact WHERE artifact_id=?", (identity,),
            ).fetchone() is None
        ]
        if missing_inputs:
            raise ValueError(f"失败物化清单输入制品未登记: {attempt_id}")
        body_status = body.get("status")
        if body_status == "failed":
            if (
                not isinstance(body.get("failed_at"), str)
                or not isinstance(body.get("failure_detail"), str)
            ):
                raise ValueError(f"失败物化清单错误信息非法: {attempt_id}")
            _verify_failed_output_rows(
                root, conn, resolved, attempt_id, domain, schema_version,
                body.get("non_promoted_outputs"),
            )
        elif body_status == "complete":
            if not (
                domain == "orderflow_tile"
                and schema_version == 1
                and normalization_version == "orderflow-tile-sparse-v1"
                and isinstance(failure_detail, str)
                and failure_detail.startswith(
                    "UNIQUE constraint failed: partition_input."
                )
            ):
                raise ValueError(f"旧失败物化清单不在兼容闭集: {attempt_id}")
            if any(
                type(body.get(key)) is not int or int(body[key]) < 0
                for key in ("column_rows", "cell_rows")
            ) or any(
                not isinstance(body.get(key), str)
                for key in ("coverage_from", "coverage_to")
            ):
                raise ValueError(f"旧失败物化清单输出摘要非法: {attempt_id}")
            _verify_legacy_failed_outputs(
                root, conn, resolved, attempt_id, body, schema_version,
            )
        else:
            raise ValueError(f"失败物化清单状态非法: {attempt_id}")
        kind = "failed_materialization_manifest"
    elif status in {"complete", "complete_with_rejections"}:
        if body.get("status") != status:
            raise ValueError(f"物化清单状态与尝试不符: {attempt_id}")
        if inputs != recorded_inputs:
            raise ValueError(f"物化清单输入集合与台账不符: {attempt_id}")
        expected_datasets = _DERIVED_OUTPUT_DATASETS[domain]
        if len(outputs) != len(expected_datasets):
            raise ValueError(f"物化清单输出数量不符: {attempt_id}")
        verified_outputs: list[tuple[sqlite3.Row, Path]] = []
        for output in outputs:
            if (
                int(output[7]) != schema_version
                or str(output[8]) != "materialized_parquet"
            ):
                raise ValueError(f"物化清单与输出结构版本不符: {attempt_id}")
            output_path = _output_path_in_manifest_directory(
                root, conn, str(output[4]), resolved, attempt_id,
            )
            if (
                not output_path.is_file()
                or output_path.stat().st_size != int(output[6])
            ):
                raise ValueError(f"物化清单输出缺失或字节数不符: {attempt_id}")
            actual_sha = sha256_file(output_path)
            if actual_sha != str(output[5]) or artifact_id(actual_sha) != str(output[4]):
                raise ValueError(f"物化清单输出散列不符: {attempt_id}")
            _verify_parquet_output(
                output_path, int(output[1]), schema_version, attempt_id,
            )
            verified_outputs.append((output, output_path))
        by_dataset = {
            str(row[0][0]): row for row in verified_outputs
        }
        if domain == "book_state":
            if set(by_dataset) != {"book_state_checkpoint"}:
                raise ValueError(f"物化清单输出数据集不符: {attempt_id}")
            output, output_path = by_dataset["book_state_checkpoint"]
            if (
                body.get("rows") != normalized_rows
                or int(output[1]) != normalized_rows
                or body.get("output") != _relative_storage_path(root, output_path)
            ):
                raise ValueError(f"物化清单输出计数或位置不符: {attempt_id}")
        else:
            expected = {"orderflow_tile_column", "orderflow_tile_cell"}
            if set(by_dataset) != expected:
                raise ValueError(f"物化清单输出数据集不符: {attempt_id}")
            column = by_dataset["orderflow_tile_column"][0]
            cell = by_dataset["orderflow_tile_cell"][0]
            if (
                body.get("column_rows") != int(column[1])
                or body.get("cell_rows") != int(cell[1])
                or int(column[1]) + int(cell[1]) != normalized_rows
                or body.get("coverage_from") != column[2]
                or body.get("coverage_from") != cell[2]
                or body.get("coverage_to") != column[3]
                or body.get("coverage_to") != cell[3]
            ):
                raise ValueError(f"物化清单输出计数或覆盖不符: {attempt_id}")
        kind = "materialization_manifest"
    else:
        raise ValueError(f"物化清单尝试状态不受支持: {attempt_id} {status}")
    sha = sha256_file(resolved)
    return _ManifestRepair(
        path=resolved, schema_version=schema_version, attempt_id=attempt_id,
        status=status, sealed_at=finished_at, artifact_kind=kind,
        sha256=sha, byte_count=resolved.stat().st_size,
    )


def _verified_legacy_input_bindings(
    root: Path, conn: sqlite3.Connection,
) -> list[_LegacyInputBindingRepair]:
    """验证可由唯一主位置恢复的旧输入位置台账。"""
    rows = conn.execute(
        "SELECT i.attempt_id,i.artifact_id,i.source_rows,"
        "i.normalized_rows,i.ignored_rows,i.rejected_rows,a.sha256,"
        "a.byte_count FROM partition_input i JOIN partition_attempt p "
        "ON p.attempt_id=i.attempt_id JOIN artifact a "
        "ON a.artifact_id=i.artifact_id LEFT JOIN "
        "partition_input_binding b ON b.attempt_id=i.attempt_id "
        "AND b.artifact_id=i.artifact_id WHERE p.status IN "
        "('complete','complete_with_rejections') GROUP BY "
        "i.attempt_id,i.artifact_id,i.source_rows,i.normalized_rows,"
        "i.ignored_rows,i.rejected_rows,a.sha256,a.byte_count HAVING "
        "COUNT(b.storage_path)=0 OR (COUNT(b.storage_path)=1 AND ("
        "SUM(b.source_rows)!=i.source_rows OR "
        "SUM(b.normalized_rows)!=i.normalized_rows OR "
        "SUM(b.ignored_rows)!=i.ignored_rows OR "
        "SUM(b.rejected_rows)!=i.rejected_rows)) "
        "ORDER BY i.attempt_id,i.artifact_id"
    ).fetchall()
    attempt_ids = sorted({str(row[0]) for row in rows})
    for attempt_id in attempt_ids:
        counts = conn.execute(
            "SELECT p.source_rows,p.normalized_rows,p.ignored_rows,"
            "p.rejected_rows,SUM(i.source_rows),SUM(i.normalized_rows),"
            "SUM(i.ignored_rows),SUM(i.rejected_rows),p.domain "
            "FROM partition_attempt p JOIN partition_input i "
            "ON i.attempt_id=p.attempt_id WHERE p.attempt_id=? "
            "GROUP BY p.attempt_id,p.source_rows,p.normalized_rows,"
            "p.ignored_rows,p.rejected_rows,p.domain",
            (attempt_id,),
        ).fetchone()
        if counts is None:
            raise ValueError(f"旧输入内容台账缺失: {attempt_id}")
        direct_normalized_mismatch = (
            str(counts[8]) not in _CARDINALITY_CHANGING_DOMAINS
            and int(counts[1]) != int(counts[5])
        )
        if (
            int(counts[0]) != int(counts[4])
            or direct_normalized_mismatch
            or int(counts[2]) != int(counts[6])
            or int(counts[3]) != int(counts[7])
        ):
            raise ValueError(
                f"旧输入内容台账与尝试计数不符: {attempt_id}"
            )
    verified: list[_LegacyInputBindingRepair] = []
    for (
        attempt_id, identity, source_rows, normalized_rows,
        ignored_rows, rejected_rows, expected_sha, expected_bytes,
    ) in rows:
        bindings = conn.execute(
            "SELECT storage_path FROM partition_input_binding "
            "WHERE attempt_id=? AND artifact_id=? ORDER BY storage_path",
            (str(attempt_id), str(identity)),
        ).fetchall()
        if len(bindings) == 1:
            storage_path = str(bindings[0][0])
            location = conn.execute(
                "SELECT 1 FROM artifact_location WHERE artifact_id=? "
                "AND storage_path=?",
                (str(identity), storage_path),
            ).fetchone()
            if location is None:
                raise ValueError(
                    "旧输入位置没有制品登记: "
                    f"{attempt_id} {identity} {storage_path}"
                )
            had_binding = True
        elif not bindings:
            locations = conn.execute(
                "SELECT storage_path FROM artifact_location "
                "WHERE artifact_id=? AND is_canonical=1", (str(identity),)
            ).fetchall()
            if len(locations) != 1:
                raise ValueError(
                    f"旧输入制品没有唯一主位置: {attempt_id} {identity}"
                )
            storage_path = str(locations[0][0])
            had_binding = False
        else:
            raise ValueError(
                f"旧输入位置台账不唯一: {attempt_id} {identity}"
            )
        path = _resolve_recorded_path(root, storage_path)
        if not path.is_file() or path.stat().st_size != int(expected_bytes):
            raise ValueError(
                f"旧输入制品缺失或字节数不符: {attempt_id} {identity}"
            )
        actual_sha = sha256_file(path)
        if (
            actual_sha != str(expected_sha)
            or artifact_id(actual_sha) != str(identity)
        ):
            raise ValueError(
                f"旧输入制品散列不符: {attempt_id} {identity}"
            )
        verified.append(_LegacyInputBindingRepair(
            attempt_id=str(attempt_id), artifact_id=str(identity),
            storage_path=storage_path, source_rows=int(source_rows),
            normalized_rows=int(normalized_rows),
            ignored_rows=int(ignored_rows), rejected_rows=int(rejected_rows),
            had_binding=had_binding,
        ))
    return verified


def repair_materialization_controls(
    root: Path, conn: sqlite3.Connection, *, apply: bool = False,
) -> dict[str, object]:
    """规划或只增修复清单制品与旧输入位置台账。"""
    root = root.resolve()
    known_paths = {
        str(row[0]).replace("\\", "/") for row in conn.execute(
            "SELECT storage_path FROM artifact_location"
        )
    }
    manifests: list[_ManifestRepair] = []
    materialized_root = root / "materialized"
    repair_roots = (
        materialized_root / "book_state_checkpoint",
        materialized_root / "orderflow_tile",
    )
    for repair_root in repair_roots:
        if repair_root.is_dir():
            candidates = sorted(repair_root.rglob("manifest-*.json"))
        else:
            candidates = []
        for path in candidates:
            storage_path = _relative_storage_path(root, path)
            if storage_path in known_paths:
                continue
            manifests.append(_verified_manifest_repair(root, conn, path))
    legacy_bindings = _verified_legacy_input_bindings(root, conn)
    result: dict[str, object] = {
        "applied": apply,
        "unregistered_manifests_found": len(manifests),
        "complete_attempt_manifests_found": sum(
            row.status in {"complete", "complete_with_rejections"}
            for row in manifests
        ),
        "failed_attempt_manifests_found": sum(
            row.status == "failed" for row in manifests
        ),
        "legacy_input_bindings_found": len(legacy_bindings),
        "manifest_artifacts_repaired": 0,
        "legacy_input_bindings_repaired": 0,
    }
    if not apply:
        return result
    repaired_manifests = repaired_bindings = 0
    registered_at = utc_now()
    conn.execute("BEGIN IMMEDIATE")
    try:
        for planned in manifests:
            storage_path = _relative_storage_path(root, planned.path)
            if conn.execute(
                "SELECT 1 FROM artifact_location WHERE storage_path=?",
                (storage_path,),
            ).fetchone() is not None:
                continue
            current = _verified_manifest_repair(root, conn, planned.path)
            if current != planned:
                raise ValueError(
                    f"物化清单在修复前已变化: {planned.attempt_id}"
                )
            registered_identity = register_materialization_manifest(
                root, conn, planned.path, planned.schema_version,
                planned.sealed_at,
                registered_at=registered_at,
                artifact_kind=planned.artifact_kind,
            )
            if (
                registered_identity != artifact_id(planned.sha256)
                or planned.path.stat().st_size != planned.byte_count
                or sha256_file(planned.path) != planned.sha256
            ):
                raise ValueError(
                    f"物化清单在登记期间已变化: {planned.attempt_id}"
                )
            repaired_manifests += 1
        for row in legacy_bindings:
            existing_paths = tuple(
                str(item[0]) for item in conn.execute(
                    "SELECT storage_path FROM partition_input_binding "
                    "WHERE attempt_id=? AND artifact_id=? "
                    "ORDER BY storage_path",
                    (row.attempt_id, row.artifact_id),
                )
            )
            expected_paths = (row.storage_path,) if row.had_binding else ()
            if existing_paths != expected_paths:
                raise ValueError(
                    "旧输入位置台账在修复前已变化: "
                    f"{row.attempt_id} {row.artifact_id}"
                )
            values = (
                row.source_rows, row.normalized_rows, row.ignored_rows,
                row.rejected_rows, row.attempt_id, row.artifact_id,
                row.storage_path,
            )
            if row.had_binding:
                cursor = conn.execute(
                    "UPDATE partition_input_binding SET source_rows=?,"
                    "normalized_rows=?,ignored_rows=?,rejected_rows=? "
                    "WHERE attempt_id=? AND artifact_id=? AND storage_path=?",
                    values,
                )
            else:
                cursor = conn.execute(
                    "INSERT INTO partition_input_binding "
                    "(source_rows,normalized_rows,ignored_rows,rejected_rows,"
                    "attempt_id,artifact_id,storage_path) VALUES (?,?,?,?,?,?,?)",
                    values,
                )
            if cursor.rowcount != 1:
                raise ValueError(
                    "旧输入位置台账修复数不符: "
                    f"{row.attempt_id} {row.artifact_id}"
                )
            repaired_bindings += 1
        conn.commit()
    except (OSError, sqlite3.Error, TypeError, ValueError):
        conn.rollback()
        raise
    result["manifest_artifacts_repaired"] = repaired_manifests
    result["legacy_input_bindings_repaired"] = repaired_bindings
    return result


def _finish_staged_attempt(
    root: Path,
    conn: sqlite3.Connection,
    attempt_id: str,
    venue_id: str,
    selected_market_id: str,
    event_month: str,
    normalization_version: str,
    artifacts: list[SourceArtifact],
    source_counts: dict[str, int],
    normalized_counts: dict[str, int],
    rejected_counts: dict[str, int],
    rejections: Sequence[tuple[str, int, str, str]],
    row_count: int,
    min_event: str,
    max_event: str,
    staging_path: Path,
    temp_path: Path,
    output_dir: Path,
) -> MaterializationResult:
    """完成共同的 Parquet、清单与活动指针事务。"""
    rejected_total = sum(rejected_counts.values())
    source_total = sum(source_counts.values())
    normalized_total = sum(normalized_counts.values())
    if normalized_total != row_count:
        raise ValueError("逐原件规范化计数与物化行数不符")
    if source_total != row_count + rejected_total:
        raise ValueError("来源行数不等于事实行数加拒绝行数")
    db = open_analytics()
    try:
        _create_duckdb_table(db)
        if row_count:
            _load_staged_rows(db, staging_path)
        _validate_staged_contract(
            db, selected_market_id, normalization_version, artifacts,
            normalized_counts, row_count,
        )
        staging_path.unlink()
        _copy_parquet(db, temp_path)
    finally:
        db.close()
    output_sha = sha256_file(temp_path)
    final_path = _finalize_file(temp_path, output_sha)
    output_storage = _relative_storage_path(root, final_path)
    output_identity = artifact_id(output_sha)
    finished_at = utc_now()
    status = "complete_with_rejections" if rejected_total else "complete"
    capability_bindings = [
        {
            "venue_id": str(row[0]),
            "domain": str(row[1]),
            "endpoint": str(row[2]),
            "revision_id": int(row[3]),
            "binding_basis": str(row[4]),
        }
        for row in conn.execute(
            "SELECT venue_id, domain, endpoint, revision_id, binding_basis "
            "FROM partition_capability_binding WHERE attempt_id=? "
            "ORDER BY venue_id, domain, endpoint",
            (attempt_id,),
        )
    ]
    if not capability_bindings:
        raise ValueError("物化尝试缺少能力修订绑定")
    manifest = {
        "attempt_id": attempt_id,
        "capability_bindings": capability_bindings,
        "dataset": DATASET_TRADE,
        "input_artifact_ids": sorted(item.artifact_id for item in artifacts),
        "input_bindings": sorted(
            [{
                "artifact_id": item.artifact_id,
                "storage_path": item.storage_path,
            } for item in artifacts],
            key=lambda item: (item["storage_path"], item["artifact_id"]),
        ),
        "input_set_hash": _input_set_hash(artifacts),
        "market_id": selected_market_id,
        "normalization_version": normalization_version,
        "output_artifact_id": output_identity,
        "output_path": output_storage,
        "rejected_rows": rejected_total,
        "row_count": row_count,
        "schema_version": TRADE_SCHEMA_VERSION,
        "status": status,
    }
    manifest_path = output_dir / f"manifest-{attempt_id}.json"
    atomic_write_text(
        manifest_path,
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
    )
    manifest_sha = sha256_file(manifest_path)
    manifest_storage = _relative_storage_path(root, manifest_path)

    conn.execute("BEGIN IMMEDIATE")
    _register_content_artifact(
        conn, output_identity, "materialized_parquet", output_storage,
        output_sha, final_path.stat().st_size, finished_at,
        TRADE_SCHEMA_VERSION,
    )
    _register_content_artifact(
        conn, artifact_id(manifest_sha), "materialization_manifest",
        manifest_storage, manifest_sha, manifest_path.stat().st_size,
        finished_at, 1,
    )
    aggregate_inputs: dict[str, list[int]] = {}
    for item in artifacts:
        values = aggregate_inputs.setdefault(item.artifact_id, [0, 0, 0])
        values[0] += source_counts.get(item.storage_path, 0)
        values[1] += normalized_counts.get(item.storage_path, 0)
        values[2] += rejected_counts.get(item.storage_path, 0)
    conn.executemany(
        "INSERT INTO partition_input "
        "(attempt_id, artifact_id, source_rows, normalized_rows, rejected_rows) "
        "VALUES (?,?,?,?,?)",
        [
            (
                attempt_id, source_identity, values[0], values[1], values[2],
            )
            for source_identity, values in aggregate_inputs.items()
        ],
    )
    conn.executemany(
        "INSERT INTO partition_input_binding "
        "(attempt_id, artifact_id, storage_path, source_rows, "
        "normalized_rows, rejected_rows) VALUES (?,?,?,?,?,?)",
        [
            (
                attempt_id, item.artifact_id, item.storage_path,
                source_counts.get(item.storage_path, 0),
                normalized_counts.get(item.storage_path, 0),
                rejected_counts.get(item.storage_path, 0),
            )
            for item in artifacts
        ],
    )
    if rejections:
        conn.executemany(
            "INSERT INTO materialization_rejection VALUES (?,?,?,?,?,?)",
            [
                (
                    attempt_id, source_identity, row_index,
                    raw_source, reason, finished_at,
                )
                for source_identity, row_index, raw_source, reason in rejections
            ],
        )
    conn.execute(
        "INSERT INTO materialization_output VALUES (?,?,?,?,?,?,?)",
        (
            attempt_id, output_identity, DATASET_TRADE, row_count,
            min_event or None, max_event or None, finished_at,
        ),
    )
    conn.execute(
        "UPDATE partition_attempt SET status=?, source_rows=?, "
        "normalized_rows=?, rejected_rows=?, finished_at=? "
        "WHERE attempt_id=? AND status='running'",
        (
            status, sum(source_counts.values()), row_count,
            rejected_total, finished_at, attempt_id,
        ),
    )
    conn.execute(
        "INSERT INTO materialization_partition_head VALUES (?,?,?,?,?,?) "
        "ON CONFLICT(market_id, domain, partition_key) DO UPDATE SET "
        "normalization_version=excluded.normalization_version, "
        "attempt_id=excluded.attempt_id, "
        "activated_at=excluded.activated_at",
        (
            selected_market_id, "trade", event_month,
            normalization_version, attempt_id, finished_at,
        ),
    )
    conn.commit()
    return MaterializationResult(
        attempt_id=attempt_id, market_id=selected_market_id,
        dataset=DATASET_TRADE, partition_key=event_month,
        normalization_version=normalization_version, status=status,
        row_count=row_count, rejected_rows=rejected_total,
        output_path=output_storage, output_artifact_id=output_identity,
        reused=False,
    )


def _fail_attempt(
    conn: sqlite3.Connection, attempt_id: str, detail: str
) -> None:
    """把运行中尝试收束为失败。"""
    conn.execute(
        "UPDATE partition_attempt SET status='failed', finished_at=?, "
        "failure_detail=? WHERE attempt_id=? AND status='running'",
        (utc_now(), detail[:2000], attempt_id),
    )
    conn.commit()


def _remove_unregistered_attempt_files(
    root: Path,
    conn: sqlite3.Connection,
    output_dir: Path,
    attempt_id: str,
) -> None:
    """只删除失败尝试在专属分区目录留下的未登记终态文件。"""
    candidates = [output_dir / f"manifest-{attempt_id}.json"]
    candidates.extend(output_dir.glob("part-*.parquet"))
    for path in candidates:
        if not path.is_file():
            continue
        storage_path = _relative_storage_path(root, path)
        registered = conn.execute(
            "SELECT 1 FROM artifact_location WHERE storage_path=?",
            (storage_path,),
        ).fetchone()
        if registered is None:
            path.unlink()


def recover_stale_attempts(
    root: Path,
    conn: sqlite3.Connection,
    older_minutes: int,
) -> tuple[int, int]:
    """收束超时运行态并删除其专属临时文件。"""
    if older_minutes <= 0:
        raise ValueError("超时分钟数必须为正")
    cutoff = datetime.now(UTC) - timedelta(minutes=older_minutes)
    rows = conn.execute(
        "SELECT attempt_id, started_at FROM partition_attempt "
        "WHERE status='running'"
    ).fetchall()
    stale = [
        str(attempt)
        for attempt, started in rows
        if datetime.fromisoformat(str(started)) <= cutoff
    ]
    removed = 0
    materialized_root = root / "materialized"
    for attempt in stale:
        for suffix in ("stage.csv", "tmp.parquet"):
            if not materialized_root.exists():
                break
            for path in materialized_root.rglob(f".{attempt}.{suffix}"):
                path.resolve().relative_to(materialized_root.resolve())
                path.unlink()
                removed += 1
        _fail_attempt(
            conn, attempt,
            f"stale materialization recovered after {older_minutes} minutes",
        )
    return len(stale), removed


def audit_materializations(
    root: Path, conn: sqlite3.Connection
) -> MaterializationAudit:
    """复算逐文件散列并核对完成输出语义。"""
    errors: list[str] = []
    warnings: list[str] = []
    opened_ns = time.time_ns()
    raw_anchor_files: dict[str, tuple[int, int]] = {}
    raw_anchor_root = root / "raw" / "rest" / "book_l2_anchor"
    if raw_anchor_root.is_dir():
        for path in raw_anchor_root.rglob("*.json"):
            if not path.is_file():
                continue
            try:
                file_stat = path.stat()
                if file_stat.st_mtime_ns > opened_ns:
                    continue
                recorded = _relative_storage_path(root, path)
                raw_anchor_files[recorded] = (
                    file_stat.st_size, file_stat.st_mtime_ns,
                )
            except (OSError, ValueError) as exc:
                errors.append(f"REST 锚点 raw 快照失败: {path}: {exc}")
    artifacts = conn.execute(
        "SELECT a.artifact_id, l.storage_path, a.sha256, a.byte_count "
        "FROM artifact a JOIN artifact_location l "
        "ON l.artifact_id=a.artifact_id "
        "ORDER BY a.artifact_id, l.storage_path"
    ).fetchall()
    registered_paths = {str(row[1]) for row in artifacts}
    terminal_paths: list[str] = []
    materialized_root = root / "materialized"
    if materialized_root.is_dir():
        for path in materialized_root.rglob("*"):
            if not path.is_file():
                continue
            if not (
                (path.name.startswith("part-") and path.suffix == ".parquet")
                or (
                    path.name.startswith("manifest-")
                    and path.suffix == ".json"
                )
            ):
                continue
            terminal_paths.append(_relative_storage_path(root, path))
    for identity, recorded, expected_sha, expected_bytes in artifacts:
        try:
            path = _resolve_recorded_path(root, str(recorded))
            if not path.is_file():
                errors.append(f"制品缺失: {recorded}")
                continue
            if path.stat().st_size != int(expected_bytes):
                errors.append(f"制品字节数不符: {recorded}")
                continue
            actual_sha = sha256_file(path)
            if actual_sha != str(expected_sha):
                errors.append(f"制品散列不符: {recorded}")
            if artifact_id(actual_sha) != str(identity):
                errors.append(f"制品身份不符: {recorded}")
        except (OSError, ValueError) as exc:
            errors.append(f"制品检查失败: {recorded}: {exc}")

    closing_paths = {
        str(row[0]) for row in conn.execute(
            "SELECT storage_path FROM artifact_location"
        )
    }
    newly_registered = closing_paths - registered_paths
    if newly_registered:
        warnings.append(
            f"审计期间新增制品登记: {len(newly_registered)}"
        )
    registered_paths.update(closing_paths)
    for recorded in terminal_paths:
        if recorded not in registered_paths:
            errors.append(f"未登记物化终态文件: {recorded}")
    for recorded, opening_stat in raw_anchor_files.items():
        try:
            path = _resolve_recorded_path(root, recorded)
            closing_stat = path.stat()
            current_stat = (closing_stat.st_size, closing_stat.st_mtime_ns)
            if current_stat != opening_stat:
                errors.append(f"REST 锚点 raw 审计期间变化: {recorded}")
                continue
            actual_sha = sha256_file(path)
            name_match = _RAW_ANCHOR_NAME.fullmatch(path.name)
            if name_match is None or name_match.group(1) != actual_sha:
                errors.append(f"REST 锚点 raw 文件名散列不符: {recorded}")
            expected_identity = artifact_id(actual_sha)
            registration = conn.execute(
                "SELECT l.artifact_id,a.artifact_kind,a.sha256,a.byte_count "
                "FROM artifact_location l JOIN artifact a "
                "ON a.artifact_id=l.artifact_id WHERE l.storage_path=?",
                (recorded,),
            ).fetchone()
            if registration is None:
                errors.append(f"未登记 REST 锚点 raw: {recorded}")
                continue
            if str(registration[0]) != expected_identity:
                errors.append(f"REST 锚点 raw 制品身份不符: {recorded}")
            if str(registration[1]) != "raw_rest_l2_anchor":
                errors.append(f"REST 锚点 raw 制品类型不符: {recorded}")
            if str(registration[2]) != actual_sha:
                errors.append(f"REST 锚点 raw 制品散列不符: {recorded}")
            if int(registration[3]) != closing_stat.st_size:
                errors.append(f"REST 锚点 raw 制品字节数不符: {recorded}")
        except (OSError, ValueError) as exc:
            errors.append(f"REST 锚点 raw 检查失败: {recorded}: {exc}")

    running = conn.execute(
        "SELECT attempt_id FROM partition_attempt WHERE status='running'"
    ).fetchall()
    warnings.extend(f"尝试仍在运行: {row[0]}" for row in running)
    missing_root_bindings = conn.execute(
        "WITH RECURSIVE relevant(attempt_id) AS ("
        "SELECT attempt_id FROM partition_attempt WHERE status IN "
        "('complete','complete_with_rejections') UNION "
        "SELECT d.upstream_attempt_id FROM relevant r JOIN "
        "materialization_dependency d ON d.attempt_id=r.attempt_id), "
        "roots(attempt_id) AS (SELECT r.attempt_id FROM relevant r WHERE "
        "NOT EXISTS (SELECT 1 FROM materialization_dependency d "
        "WHERE d.attempt_id=r.attempt_id)) "
        "SELECT roots.attempt_id FROM roots WHERE NOT EXISTS ("
        "SELECT 1 FROM partition_capability_binding b "
        "WHERE b.attempt_id=roots.attempt_id) ORDER BY roots.attempt_id"
    ).fetchall()
    errors.extend(
        f"完成血缘根缺少能力修订绑定: {row[0]}"
        for row in missing_root_bindings
    )
    incomplete_dependencies = conn.execute(
        "SELECT d.attempt_id,d.upstream_attempt_id,u.status FROM "
        "materialization_dependency d JOIN partition_attempt a "
        "ON a.attempt_id=d.attempt_id JOIN partition_attempt u "
        "ON u.attempt_id=d.upstream_attempt_id WHERE a.status IN "
        "('complete','complete_with_rejections') AND u.status NOT IN "
        "('complete','complete_with_rejections') ORDER BY d.attempt_id,"
        "d.upstream_attempt_id"
    ).fetchall()
    errors.extend(
        "完成尝试依赖非完成上游: "
        f"{row[0]} {row[1]} status={row[2]}"
        for row in incomplete_dependencies
    )
    dependency_cycles = conn.execute(
        "WITH RECURSIVE reach(start,node) AS ("
        "SELECT attempt_id,upstream_attempt_id FROM "
        "materialization_dependency UNION SELECT r.start,d.upstream_attempt_id "
        "FROM reach r JOIN materialization_dependency d "
        "ON d.attempt_id=r.node) SELECT DISTINCT start FROM reach "
        "WHERE start=node ORDER BY start"
    ).fetchall()
    errors.extend(
        f"物化依赖存在循环: {row[0]}" for row in dependency_cycles
    )
    invalid_producer_counts = conn.execute(
        "WITH produced AS MATERIALIZED (SELECT DISTINCT artifact_id FROM "
        "materialization_output), eligible AS MATERIALIZED ("
        "SELECT i.attempt_id,i.artifact_id,i.source_rows FROM partition_input i "
        "JOIN partition_attempt a ON a.attempt_id=i.attempt_id "
        "JOIN produced p ON p.artifact_id=i.artifact_id WHERE a.status IN "
        "('complete','complete_with_rejections')), producer_count AS ("
        "SELECT e.attempt_id,e.artifact_id,e.source_rows,"
        "COUNT(DISTINCT o.attempt_id) AS producers FROM eligible e LEFT JOIN "
        "materialization_dependency d ON d.attempt_id=e.attempt_id "
        "LEFT JOIN materialization_output o ON o.attempt_id="
        "d.upstream_attempt_id AND o.artifact_id=e.artifact_id GROUP BY "
        "e.attempt_id,e.artifact_id,e.source_rows) SELECT attempt_id,"
        "artifact_id,source_rows,producers FROM producer_count WHERE "
        "(source_rows>0 AND producers!=1) OR "
        "(source_rows=0 AND producers<1) "
        "ORDER BY attempt_id,artifact_id"
    ).fetchall()
    errors.extend(
        "物化输入生产依赖数量不符: "
        f"{row[0]} {row[1]} source_rows={row[2]} producers={row[3]}"
        for row in invalid_producer_counts
    )
    invalid_bindings = conn.execute(
        "WITH RECURSIVE relevant(attempt_id) AS ("
        "SELECT attempt_id FROM partition_attempt WHERE status IN "
        "('complete','complete_with_rejections') UNION "
        "SELECT d.upstream_attempt_id FROM relevant r JOIN "
        "materialization_dependency d ON d.attempt_id=r.attempt_id) "
        "SELECT DISTINCT b.attempt_id,b.venue_id,b.endpoint,b.revision_id "
        "FROM relevant r JOIN partition_capability_binding b "
        "ON b.attempt_id=r.attempt_id LEFT JOIN venue_capability_revision c "
        "ON c.venue_id=b.venue_id AND c.domain=b.domain "
        "AND c.endpoint=b.endpoint AND c.revision_id=b.revision_id "
        "WHERE c.venue_id IS NULL OR c.available!=1 "
        "OR c.implementation_status!='implemented' "
        "ORDER BY b.attempt_id,b.venue_id,b.endpoint,b.revision_id"
    ).fetchall()
    errors.extend(
        "能力修订绑定不可用于物化: "
        f"{row[0]} {row[1]}/{row[2]} r{row[3]}"
        for row in invalid_bindings
    )
    outputs = conn.execute(
        "SELECT a.attempt_id, o.row_count, o.min_event_time, "
        "o.max_event_time, r.storage_path, a.market_id, "
        "a.normalization_version,r.schema_version FROM partition_attempt a "
        "JOIN materialization_output o ON o.attempt_id=a.attempt_id "
        "JOIN artifact r ON r.artifact_id=o.artifact_id "
        "WHERE a.status IN ('complete', 'complete_with_rejections') "
        "AND o.dataset=? ORDER BY a.attempt_id",
        (DATASET_TRADE,),
    ).fetchall()
    rows_checked = 0
    db = open_analytics()
    try:
        for (
            attempt, expected_rows, expected_min, expected_max, recorded,
            expected_market, expected_normalization, expected_schema,
        ) in outputs:
            try:
                path = _resolve_recorded_path(root, str(recorded))
                contract_schema = TRADE_SCHEMA_BY_NORMALIZATION.get(
                    str(expected_normalization)
                )
                if contract_schema is None:
                    errors.append(
                        "输出使用未知逐笔规范化版本: "
                        f"{recorded} {expected_normalization}"
                    )
                    continue
                path_schema = _manifest_schema_version(root, path)
                if (
                    int(expected_schema) != contract_schema
                    or path_schema != contract_schema
                ):
                    errors.append(f"输出登记结构契约不符: {recorded}")
                result = db.execute(
                    "SELECT COUNT(*), "
                    "SUM(CASE WHEN available_time < event_time THEN 1 ELSE 0 END), "
                    "COUNT(*) - COUNT(DISTINCT observation_id), "
                    "MIN(event_time), MAX(event_time), "
                    "COUNT(DISTINCT market_id), MIN(market_id), "
                    "COUNT(DISTINCT normalization_version), "
                    "MIN(normalization_version), "
                    "COUNT(DISTINCT schema_version), MIN(schema_version), "
                    "SUM(CASE WHEN match_granularity NOT IN "
                    "('match', 'aggregate') THEN 1 ELSE 0 END) "
                    "FROM read_parquet(?)",
                    [str(path)],
                ).fetchone()
                if result is None:
                    errors.append(f"输出不可读: {recorded}")
                    continue
                actual_rows = int(result[0])
                rows_checked += actual_rows
                if actual_rows != int(expected_rows):
                    errors.append(f"输出行数不符: {recorded}")
                if int(result[1] or 0):
                    errors.append(f"输出存在 PIT 违规: {recorded}")
                if int(result[2] or 0):
                    errors.append(f"输出存在重复观察: {recorded}")
                actual_min = result[3].isoformat() if result[3] else None
                actual_max = result[4].isoformat() if result[4] else None
                if actual_min != expected_min or actual_max != expected_max:
                    errors.append(f"输出时间边界不符: {recorded}")
                if actual_rows:
                    if (
                        int(result[5]) != 1
                        or str(result[6]) != expected_market
                    ):
                        errors.append(f"输出市场契约不符: {recorded}")
                    if (
                        int(result[7]) != 1
                        or str(result[8]) != expected_normalization
                    ):
                        errors.append(f"输出规范化契约不符: {recorded}")
                    if (
                        int(result[9]) != 1
                        or int(result[10]) != contract_schema
                    ):
                        errors.append(f"输出结构契约不符: {recorded}")
                elif any(result[index] not in (0, None) for index in (5, 7, 9)):
                    errors.append(f"零行输出含有事实契约值: {recorded}")
                if int(result[11] or 0):
                    errors.append(f"输出存在未知成交粒度: {recorded}")
                actual_sources = {
                    str(row[0]): int(row[1])
                    for row in db.execute(
                        "SELECT source_artifact_id, COUNT(*) "
                        "FROM read_parquet(?) GROUP BY source_artifact_id",
                        [str(path)],
                    ).fetchall()
                }
                expected_sources = {
                    str(row[0]): int(row[1])
                    for row in conn.execute(
                        "SELECT artifact_id, normalized_rows "
                        "FROM partition_input WHERE attempt_id=? "
                        "AND normalized_rows>0",
                        (str(attempt),),
                    )
                }
                if actual_sources != expected_sources:
                    errors.append(f"输出原件绑定不符: {recorded}")
            except (OSError, ValueError, duckdb.Error) as exc:
                errors.append(f"输出检查失败: {attempt}: {exc}")
    finally:
        db.close()

    input_mismatches = conn.execute(
        "SELECT a.attempt_id FROM partition_attempt a "
        "LEFT JOIN partition_input i ON i.attempt_id=a.attempt_id "
        "WHERE a.status IN ('complete', 'complete_with_rejections') "
        "GROUP BY a.attempt_id, a.domain, a.source_rows, "
        "a.normalized_rows, a.ignored_rows, a.rejected_rows HAVING "
        "COALESCE(SUM(i.source_rows), -1) != a.source_rows OR "
        "(a.domain NOT IN (?,?) AND "
        "COALESCE(SUM(i.normalized_rows), -1) != a.normalized_rows) OR "
        "COALESCE(SUM(i.ignored_rows), -1) != a.ignored_rows OR "
        "COALESCE(SUM(i.rejected_rows), -1) != a.rejected_rows",
        _CARDINALITY_CHANGING_DOMAINS,
    ).fetchall()
    errors.extend(f"输入计数台账不符: {row[0]}" for row in input_mismatches)
    binding_mismatches = conn.execute(
        "WITH binding_total AS ("
        "SELECT attempt_id, artifact_id, SUM(source_rows) source_rows, "
        "SUM(normalized_rows) normalized_rows, "
        "SUM(ignored_rows) ignored_rows, "
        "SUM(rejected_rows) rejected_rows FROM partition_input_binding "
        "GROUP BY attempt_id, artifact_id), ledger_key AS ("
        "SELECT attempt_id, artifact_id FROM partition_input UNION "
        "SELECT attempt_id, artifact_id FROM binding_total) "
        "SELECT k.attempt_id, k.artifact_id FROM ledger_key k "
        "JOIN partition_attempt a ON a.attempt_id=k.attempt_id "
        "LEFT JOIN partition_input i ON i.attempt_id=k.attempt_id "
        "AND i.artifact_id=k.artifact_id "
        "LEFT JOIN binding_total b ON b.attempt_id=k.attempt_id "
        "AND b.artifact_id=k.artifact_id "
        "WHERE a.status IN ('complete', 'complete_with_rejections') AND ("
        "i.attempt_id IS NULL OR b.attempt_id IS NULL OR "
        "i.source_rows != b.source_rows OR "
        "i.normalized_rows != b.normalized_rows OR "
        "i.ignored_rows != b.ignored_rows OR "
        "i.rejected_rows != b.rejected_rows) "
        "ORDER BY k.attempt_id, k.artifact_id"
    ).fetchall()
    errors.extend(
        f"输入位置台账不符: {row[0]} {row[1]}"
        for row in binding_mismatches
    )
    return MaterializationAudit(
        artifacts_checked=len(artifacts), outputs_checked=len(outputs),
        rows_checked=rows_checked, warnings=tuple(warnings),
        errors=tuple(errors),
    )


def active_output_paths(
    root: Path, conn: sqlite3.Connection, dataset: str = DATASET_TRADE
) -> list[Path]:
    """返回每个逻辑分区当前活动的 Parquet 路径。"""
    rows = conn.execute(
        "SELECT r.storage_path FROM materialization_partition_head h "
        "JOIN materialization_output o ON o.attempt_id=h.attempt_id "
        "JOIN artifact r ON r.artifact_id=o.artifact_id "
        "WHERE o.dataset=? ORDER BY h.market_id, h.partition_key",
        (dataset,),
    )
    return [_resolve_recorded_path(root, str(row[0])) for row in rows]


def plan_archive_backfill(
    root: Path,
    conn: sqlite3.Connection,
    markets: Sequence[tuple[str, str]] = DEFAULT_BACKFILL_MARKETS,
    from_month: str | None = None,
    to_month: str | None = None,
) -> ArchiveBackfillPlan:
    """按覆盖、文件和活动输入绑定生成全局可恢复计划。"""
    if from_month is not None:
        _month_bounds(from_month)
    if to_month is not None:
        _month_bounds(to_month)
    registry.register_all(conn)
    ensure_markets(conn)
    selected = tuple(dict.fromkeys(markets))
    selected_set = set(selected)
    venue_ids = tuple(dict.fromkeys(venue for venue, _ in selected))
    file_paths: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    for part in archive_partitions(root, venue_ids, None, None):
        pair = (part.venue_id, part.venue_symbol)
        if pair not in selected_set:
            continue
        month = f"{part.day[:4]}-{part.day[4:6]}"
        if (from_month is not None and month < from_month) or (
            to_month is not None and month > to_month
        ):
            continue
        file_paths[(part.venue_id, part.venue_symbol, month)].add(
            _relative_storage_path(root, part.path)
        )

    tasks: list[ArchiveBackfillTask] = []
    for venue_id, venue_symbol in selected:
        selected_market_id, _, _ = _market_row(
            conn, venue_id, venue_symbol, None
        )
        coverage_rows = conn.execute(
            "SELECT day, rows, status FROM archive_coverage "
            "WHERE venue_id=? AND venue_symbol=? AND domain='trade' "
            "ORDER BY day",
            (venue_id, venue_symbol),
        ).fetchall()
        coverage_by_month: dict[str, list[tuple[str, int, str]]] = (
            defaultdict(list)
        )
        for day, rows, status in coverage_rows:
            month = f"{str(day)[:4]}-{str(day)[4:6]}"
            if (from_month is not None and month < from_month) or (
                to_month is not None and month > to_month
            ):
                continue
            coverage_by_month[month].append(
                (str(day), int(rows or 0), str(status))
            )
        file_months = {
            key[2] for key in file_paths
            if key[0] == venue_id and key[1] == venue_symbol
        }
        months = sorted(set(coverage_by_month) | file_months)
        head_rows = conn.execute(
            "SELECT h.partition_key, h.normalization_version, "
            "a.normalized_rows, b.storage_path, b.source_rows "
            "FROM materialization_partition_head h "
            "JOIN partition_attempt a ON a.attempt_id=h.attempt_id "
            "LEFT JOIN partition_input_binding b "
            "ON b.attempt_id=h.attempt_id "
            "WHERE h.market_id=? AND h.domain='trade'",
            (selected_market_id,),
        ).fetchall()
        heads: dict[str, _HeadBinding] = {}
        for month, version, normalized, storage_path, source_rows in head_rows:
            head = heads.setdefault(
                str(month),
                _HeadBinding(str(version), int(normalized), set()),
            )
            if storage_path is not None:
                head.paths.add(str(storage_path))
                head.source_rows += int(source_rows or 0)
        expected_version = trade_normalization_version(venue_id)
        for month in months:
            coverage = coverage_by_month.get(month, [])
            expected_paths = file_paths.get(
                (venue_id, venue_symbol, month), set()
            )
            source_rows = sum(row[1] for row in coverage)
            coverage_days = {row[0] for row in coverage}
            missing_days = [
                row[0] for row in coverage
                if row[2] not in {"ok", "empty"}
            ]
            actual_days = {
                PurePosixPath(path).name[:8] for path in expected_paths
            }
            status = "pending"
            normalized_rows = 0
            reason: str | None = None
            if not coverage:
                status = "blocked_coverage"
                reason = "归档文件没有覆盖台账"
            elif missing_days:
                status = "blocked_missing"
                reason = "missing_days=" + ",".join(missing_days)
            elif coverage_days != actual_days:
                status = "blocked_files"
                reason = (
                    f"missing_files={sorted(coverage_days - actual_days)} "
                    f"unexpected_files={sorted(actual_days - coverage_days)}"
                )
            else:
                current_head = heads.get(month)
                if current_head is not None:
                    if (
                        current_head.version == expected_version
                        and current_head.paths == expected_paths
                        and current_head.source_rows == source_rows
                    ):
                        status = "complete"
                        normalized_rows = current_head.normalized_rows
            tasks.append(ArchiveBackfillTask(
                venue_id, venue_symbol, selected_market_id, month,
                status, source_rows, normalized_rows, reason,
            ))
    return ArchiveBackfillPlan(tuple(tasks))


def archive_backfill_plan_summary(plan: ArchiveBackfillPlan) -> dict[str, object]:
    """生成适合终端和 JSON 的全体进度摘要。"""
    complete = plan.complete_tasks
    pending = plan.pending_tasks
    blocked = plan.blocked_tasks
    return {
        "total_tasks": len(plan.tasks),
        "complete_tasks": len(complete),
        "pending_tasks": len(pending),
        "blocked_tasks": len(blocked),
        "complete_source_rows": sum(task.source_rows for task in complete),
        "complete_normalized_rows": sum(
            task.normalized_rows for task in complete
        ),
        "pending_source_rows": sum(task.source_rows for task in pending),
        "blocked_source_rows": sum(task.source_rows for task in blocked),
        "blocked": [asdict(task) for task in blocked],
    }


def run_archive_backfill(
    root: Path,
    conn: sqlite3.Connection,
    markets: Sequence[tuple[str, str]] = DEFAULT_BACKFILL_MARKETS,
    from_month: str | None = None,
    to_month: str | None = None,
    code_version: str = "working-tree",
    progress: Callable[[str], None] | None = None,
) -> dict[str, object]:
    """跳过已知缺口并持续执行所有其余月任务。"""
    running = conn.execute(
        "SELECT attempt_id FROM partition_attempt WHERE status='running'"
    ).fetchall()
    if running:
        raise ValueError(
            "已有物化尝试正在运行: "
            + ",".join(str(row[0]) for row in running)
        )
    plan = plan_archive_backfill(
        root, conn, markets, from_month, to_month
    )
    pending = plan.pending_tasks
    blocked = plan.blocked_tasks
    total_rows = sum(task.source_rows for task in pending)
    processed_rows = 0
    completed = 0
    reused = 0
    normalized_rows = 0
    failures: list[dict[str, str]] = []
    if progress is not None:
        progress(
            "PLAN "
            f"total={len(plan.tasks)} complete={len(plan.complete_tasks)} "
            f"pending={len(pending)} blocked={len(blocked)} "
            f"pending_source_rows={total_rows:,}"
        )
        for task in blocked:
            progress(
                f"BLOCKED {task.venue_id}/{task.venue_symbol} "
                f"{task.event_month} reason={task.reason}"
            )
    started_all = time.perf_counter()
    for index, task in enumerate(pending, start=1):
        if progress is not None:
            progress(
                f"OVERALL [{index}/{len(pending)}] START "
                f"{task.venue_id}/{task.venue_symbol} {task.event_month} "
                f"source_rows={task.source_rows:,} "
                f"completed_source_rows={processed_rows:,}/{total_rows:,}"
            )
        started = time.perf_counter()
        month_progress: Callable[[str], None] | None = None
        if progress is not None:
            progress_callback = progress

            def report_month(
                message: str, task_index: int = index
            ) -> None:
                progress_callback(
                    f"OVERALL [{task_index}/{len(pending)}] {message}"
                )

            month_progress = report_month
        try:
            result = materialize_archive_trade_month(
                root, conn, task.venue_id, task.venue_symbol,
                task.event_month, None, code_version,
                month_progress,
            )
        except (OSError, sqlite3.Error, ValueError, duckdb.Error) as exc:
            failures.append({
                "venue_id": task.venue_id,
                "venue_symbol": task.venue_symbol,
                "event_month": task.event_month,
                "reason": str(exc),
            })
            if progress is not None:
                progress(
                    f"OVERALL [{index}/{len(pending)}] FAILED "
                    f"{task.venue_id}/{task.venue_symbol} "
                    f"{task.event_month} reason={exc}"
                )
            continue
        processed_rows += task.source_rows
        normalized_rows += result.row_count
        if result.reused:
            reused += 1
        else:
            completed += 1
        if progress is not None:
            percent = (
                processed_rows * 100.0 / total_rows if total_rows else 100.0
            )
            progress(
                f"OVERALL [{index}/{len(pending)}] DONE "
                f"{task.venue_id}/{task.venue_symbol} "
                f"{task.event_month} rows={result.row_count:,} "
                f"rejected={result.rejected_rows:,} "
                f"elapsed={time.perf_counter() - started:.1f}s "
                f"progress={percent:.2f}%"
            )
    return {
        "plan": archive_backfill_plan_summary(plan),
        "completed_now": completed,
        "reused_now": reused,
        "failed_now": len(failures),
        "normalized_rows_now": normalized_rows,
        "processed_source_rows": processed_rows,
        "elapsed_seconds": round(time.perf_counter() - started_all, 3),
        "failures": failures,
    }


def _archive_month_inputs(
    root: Path,
    conn: sqlite3.Connection,
    venue_id: str,
    venue_symbol: str,
    event_month: str,
) -> list[ArchiveInput]:
    """核验一个市场月的全部日归档与覆盖台账。"""
    start_text, end_text = _month_bounds(event_month)
    start = datetime.fromisoformat(start_text)
    end = datetime.fromisoformat(end_text) - timedelta(days=1)
    month_prefix = event_month.replace("-", "")
    coverage_rows = conn.execute(
        "SELECT day, rows, status FROM archive_coverage WHERE venue_id=? "
        "AND venue_symbol=? AND domain='trade' AND day LIKE ? "
        "ORDER BY day",
        (venue_id, venue_symbol, f"{month_prefix}%"),
    ).fetchall()
    if not coverage_rows:
        raise ValueError("该市场月没有归档覆盖台账")
    incomplete_days = [
        str(row[0]) for row in coverage_rows
        if str(row[2]) not in {"ok", "empty"}
    ]
    if incomplete_days:
        raise ValueError(
            "归档覆盖存在缺口: " + ",".join(incomplete_days)
        )
    expected_days = {str(row[0]) for row in coverage_rows}
    parts = [
        part for part in archive_partitions(
            root, (venue_id,), start.strftime("%Y%m%d"),
            end.strftime("%Y%m%d"),
        )
        if part.venue_symbol == venue_symbol
    ]
    if not parts:
        raise ValueError("该市场月没有归档原件")
    actual_days = {part.day for part in parts}
    if actual_days != expected_days:
        missing_files = sorted(expected_days - actual_days)
        unexpected_files = sorted(actual_days - expected_days)
        raise ValueError(
            "归档文件与覆盖台账不一致: "
            f"missing_files={missing_files} "
            f"unexpected_files={unexpected_files}"
        )
    registered_at = utc_now()
    inputs: list[ArchiveInput] = []
    for part in parts:
        coverage = conn.execute(
            "SELECT rows, status FROM archive_coverage WHERE venue_id=? "
            "AND venue_symbol=? AND domain='trade' AND day=?",
            (venue_id, venue_symbol, part.day),
        ).fetchone()
        if coverage is None or str(coverage[1]) not in {"ok", "empty"}:
            raise ValueError(
                f"归档覆盖未完成: {venue_id}/{venue_symbol}/{part.day}"
            )
        expected_rows = int(coverage[0] or 0)
        actual_sha = sha256_file(part.path)
        if venue_id == "binance":
            checksum = archive.binance_checksum_path(
                root, venue_symbol, part.day
            )
            if not checksum.is_file():
                raise ValueError(f"Binance 缺少 CHECKSUM: {checksum}")
            expected_sha = checksum.read_text(
                encoding="utf-8"
            ).split()[0].lower()
            if actual_sha != expected_sha:
                raise ValueError(f"Binance CHECKSUM 不匹配: {part.path}")
        storage_path = _relative_storage_path(root, part.path)
        sealed_at = datetime.fromtimestamp(
            part.path.stat().st_mtime, UTC
        ).isoformat()
        identity = artifact_id(actual_sha)
        store.register_artifact(conn, (
            identity, "source_archive", storage_path, actual_sha,
            part.path.stat().st_size, sealed_at, registered_at,
            "sha256-file-v1", 1,
        ))
        source = SourceArtifact(
            identity, storage_path, part.path, expected_rows, 0, 0
        )
        inputs.append(ArchiveInput(part, source, sealed_at))
    return inputs


def _trade_output_row(
    trade: NormalizedTrade,
    venue_symbol: str,
    selected_market_id: str,
    mapping_revision: int,
    source_identity: str,
) -> tuple[object, ...]:
    """绑定规范化成交与市场、原件、版本三键。"""
    observation = _trade_observation_id(trade, selected_market_id)
    return (
        observation, trade.venue_id, venue_symbol, selected_market_id,
        mapping_revision, trade.instrument_id, trade.venue_trade_id,
        trade.revision_id, trade.event_time, trade.available_time,
        trade.ingest_time, trade.side, trade.source_side_basis,
        trade.price, trade.size, trade.match_granularity,
        trade.id_origin, trade.sequence_id, trade.first_trade_id,
        trade.last_trade_id, trade.time_origin, source_identity,
        trade.raw_item_index, trade.normalization_version,
        trade.schema_version,
    )


def _trade_observation_id(
    trade: NormalizedTrade, selected_market_id: str
) -> str:
    """生成不含来源文件位置的稳定事实身份。"""
    return (
        f"{trade.venue_id}|{selected_market_id}|"
        f"{trade.venue_trade_id}|r{trade.revision_id}"
    )


def _trade_semantic_fingerprint(trade: NormalizedTrade) -> tuple[object, ...]:
    """比较重复来源成交是否表达同一经济事实。"""
    return (
        trade.venue_id, trade.instrument_id, trade.venue_trade_id,
        trade.revision_id, trade.event_time, trade.side,
        trade.source_side_basis, trade.price, trade.size,
        trade.match_granularity, trade.id_origin, trade.sequence_id,
        trade.first_trade_id, trade.last_trade_id, trade.time_origin,
        trade.normalization_version, trade.schema_version,
    )


def _stage_archive_trade_rows(
    root: Path,
    staging_path: Path,
    venue_id: str,
    venue_symbol: str,
    selected_market_id: str,
    instrument_id: str,
    mapping_revision: int,
    event_month: str,
    normalization_version: str,
    inputs: list[ArchiveInput],
    progress: Callable[[str], None] | None = None,
) -> ArchiveStage:
    """从压缩归档直接流式规范化，不经过 SQLite 事实表。"""
    start, end = _archive_partition_bounds(venue_id, event_month)
    source_counts: dict[str, int] = {}
    normalized_counts: dict[str, int] = {}
    rejected_counts: dict[str, int] = {}
    rejections: list[tuple[str, int, str, str]] = []
    row_count = 0
    min_event = ""
    max_event = ""
    seen_observations: dict[str, tuple[object, ...]] = {}
    with staging_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        output_batch: list[tuple[object, ...]] = []
        for item in inputs:
            identity = item.artifact.artifact_id
            binding = item.artifact.storage_path
            raw_rows = 0
            normalized_rows = 0
            rejected_rows = 0
            if item.artifact.source_rows == 0:
                source_counts[binding] = 0
                normalized_counts[binding] = 0
                rejected_counts[binding] = 0
                if progress is not None:
                    progress(
                        f"{binding} source=0 normalized=0 rejected=0 "
                        f"month_total={row_count}"
                    )
                continue
            for payload, context in rows_from_partition(
                root, item.partition, instrument_id, item.ingest_time
            ):
                raw_rows += 1
                try:
                    trade = normalize_trade(payload, context)
                except NormalizationError as exc:
                    rejected_rows += 1
                    rejections.append((
                        identity, context.raw_item_index,
                        context.raw_source, str(exc),
                    ))
                    continue
                if trade.normalization_version != normalization_version:
                    raise ValueError("规范化器版本与物化分区不一致")
                if not start <= trade.event_time < end:
                    raise ValueError(
                        f"事件时刻超出来源会话月份: {context.raw_source}"
                    )
                observation = _trade_observation_id(
                    trade, selected_market_id
                )
                semantic = _trade_semantic_fingerprint(trade)
                previous = seen_observations.get(observation)
                if previous is not None:
                    if previous != semantic:
                        raise ValueError(
                            "来源成交身份重复但语义冲突: "
                            f"{observation} {context.raw_source}"
                        )
                    rejected_rows += 1
                    rejections.append((
                        identity, context.raw_item_index,
                        context.raw_source,
                        f"相同语义来源成交重复: {observation}",
                    ))
                    continue
                seen_observations[observation] = semantic
                output_batch.append(_trade_output_row(
                    trade, venue_symbol, selected_market_id,
                    mapping_revision, identity,
                ))
                normalized_rows += 1
                row_count += 1
                min_event = (
                    trade.event_time if not min_event
                    else min(min_event, trade.event_time)
                )
                max_event = (
                    trade.event_time if not max_event
                    else max(max_event, trade.event_time)
                )
                if len(output_batch) >= STAGING_BATCH_ROWS:
                    writer.writerows(
                        tuple(
                            "\\N" if value is None else value
                            for value in output_row
                        )
                        for output_row in output_batch
                    )
                    output_batch = []
            if raw_rows != item.artifact.source_rows:
                raise ValueError(
                    f"归档行数不符: {item.partition.path} "
                    f"expected={item.artifact.source_rows} actual={raw_rows}"
                )
            source_counts[binding] = raw_rows
            normalized_counts[binding] = normalized_rows
            rejected_counts[binding] = rejected_rows
            if progress is not None:
                progress(
                    f"  DAY {item.partition.day} source_rows={raw_rows:,} "
                    f"normalized={normalized_rows:,} "
                    f"rejected={rejected_rows:,} "
                    f"cumulative_rows={row_count:,}"
                )
        if output_batch:
            writer.writerows(
                tuple(
                    "\\N" if value is None else value
                    for value in output_row
                )
                for output_row in output_batch
            )
        handle.flush()
        os.fsync(handle.fileno())
    return ArchiveStage(
        row_count, source_counts, normalized_counts, rejected_counts,
        tuple(rejections), min_event, max_event,
    )


@dataclass(frozen=True)
class ArchiveMonthKey:
    """重算一个归档月 Parquet 所需的市场与版本键。"""

    venue_id: str
    venue_symbol: str
    market_id: str
    instrument_id: str
    mapping_revision: int
    event_month: str
    normalization_version: str


def rebuild_archive_trade_month_parquet(
    root: Path,
    inputs: Sequence[ArchiveInput],
    key: ArchiveMonthKey,
    temp_dir: Path,
) -> Path:
    """只在临时目录重算归档月 Parquet 字节，不触碰控制面。"""
    _month_bounds(key.event_month)
    if not inputs:
        raise ValueError("重算归档月没有输入原件")
    ordered = sorted(
        inputs, key=lambda item: (item.partition.day, item.artifact.storage_path),
    )
    artifacts = [item.artifact for item in ordered]
    temp_dir.mkdir(parents=True, exist_ok=True)
    staging_path = temp_dir / "stage.csv"
    temp_path = temp_dir / "rebuild.parquet"
    for stale in (staging_path, temp_path):
        stale.unlink(missing_ok=True)
    stage = _stage_archive_trade_rows(
        root, staging_path, key.venue_id, key.venue_symbol, key.market_id,
        key.instrument_id, key.mapping_revision, key.event_month,
        key.normalization_version, list(ordered),
    )
    source_total = sum(stage.source_counts.values())
    rejected_total = sum(stage.rejected_counts.values())
    if source_total != stage.row_count + rejected_total:
        raise ValueError("来源行数不等于事实行数加拒绝行数")
    db = open_analytics()
    try:
        _create_duckdb_table(db)
        if stage.row_count:
            _load_staged_rows(db, staging_path)
        _validate_staged_contract(
            db, key.market_id, key.normalization_version, artifacts,
            stage.normalized_counts, stage.row_count,
        )
        staging_path.unlink()
        _copy_parquet(db, temp_path)
    finally:
        db.close()
    return temp_path


def materialize_archive_trade_month(
    root: Path,
    conn: sqlite3.Connection,
    venue_id: str,
    venue_symbol: str,
    event_month: str,
    mapping_revision: int | None = None,
    code_version: str = "working-tree",
    progress: Callable[[str], None] | None = None,
) -> MaterializationResult:
    """从完整日归档直接物化一个来源市场月。"""
    _month_bounds(event_month)
    registry.register_all(conn)
    ensure_markets(conn)
    selected_market_id, instrument_id, revision = _market_row(
        conn, venue_id, venue_symbol, mapping_revision
    )
    normalization_version = trade_normalization_version(venue_id)
    inputs = _archive_month_inputs(
        root, conn, venue_id, venue_symbol, event_month
    )
    artifacts = [item.artifact for item in inputs]
    input_hash = _input_set_hash(artifacts)
    completed = _completed_result(
        conn, selected_market_id, event_month,
        normalization_version, input_hash,
    )
    if completed is not None:
        output = root / completed.output_path
        if not output.is_file():
            raise FileNotFoundError(output)
        if artifact_id(sha256_file(output)) != completed.output_artifact_id:
            raise ValueError(f"既有输出散列不符: {output}")
        return completed

    config_body = json.dumps({
        "dataset": DATASET_TRADE,
        "event_month": event_month,
        "market_id": selected_market_id,
        "normalization_version": normalization_version,
        "schema_version": TRADE_SCHEMA_VERSION,
        "source_mode": "archive-direct-v1",
    }, sort_keys=True, separators=(",", ":"))
    config_hash = hashlib.sha256(config_body.encode("utf-8")).hexdigest()
    attempt_id = f"mat-{uuid.uuid4().hex}"
    started_at = utc_now()
    conn.execute(
        "INSERT INTO partition_attempt (attempt_id, market_id, domain, "
        "partition_key, normalization_version, input_set_hash, status, "
        "source_rows, normalized_rows, rejected_rows, started_at, "
        "code_version, config_hash) VALUES (?,?,?,?,?,?,'running',?,?,?,?,?,?)",
        (
            attempt_id, selected_market_id, "trade", event_month,
            normalization_version, input_hash,
            sum(item.source_rows for item in artifacts), 0, 0,
            started_at, code_version, config_hash,
        ),
    )
    _bind_trade_capability(conn, attempt_id, venue_id)
    conn.commit()
    output_dir = _output_directory(
        root, venue_id, selected_market_id, event_month,
        normalization_version,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    temp_path = output_dir / f".{attempt_id}.tmp.parquet"
    staging_path = output_dir / f".{attempt_id}.stage.csv"
    try:
        stage = _stage_archive_trade_rows(
            root, staging_path, venue_id, venue_symbol, selected_market_id,
            instrument_id, revision, event_month, normalization_version,
            inputs, progress,
        )
        return _finish_staged_attempt(
            root, conn, attempt_id, venue_id, selected_market_id,
            event_month, normalization_version, artifacts,
            stage.source_counts, stage.normalized_counts,
            stage.rejected_counts, stage.rejections, stage.row_count,
            stage.min_event_time, stage.max_event_time,
            staging_path, temp_path, output_dir,
        )
    except (OSError, sqlite3.Error, ValueError, duckdb.Error) as exc:
        conn.rollback()
        if temp_path.exists():
            temp_path.unlink()
        if staging_path.exists():
            staging_path.unlink()
        _remove_unregistered_attempt_files(
            root, conn, output_dir, attempt_id
        )
        _fail_attempt(conn, attempt_id, str(exc))
        raise


def materialize_archive_market(
    root: Path,
    conn: sqlite3.Connection,
    venue_id: str,
    venue_symbol: str,
    from_month: str | None = None,
    to_month: str | None = None,
    mapping_revision: int | None = None,
    code_version: str = "working-tree",
    progress: Callable[[str], None] | None = None,
) -> list[MaterializationResult]:
    """按月扩展一个来源市场的全部现有归档。"""
    parts = [
        part for part in archive_partitions(root, (venue_id,), None, None)
        if part.venue_symbol == venue_symbol
    ]
    months = sorted({f"{part.day[:4]}-{part.day[4:6]}" for part in parts})
    selected = [
        month for month in months
        if (from_month is None or month >= from_month)
        and (to_month is None or month <= to_month)
    ]
    if not selected:
        raise ValueError("范围内没有可物化月份")
    results: list[MaterializationResult] = []
    total_rows = 0
    total_started = time.perf_counter()
    for index, month in enumerate(selected, start=1):
        expected = conn.execute(
            "SELECT COALESCE(SUM(rows), 0) FROM archive_coverage "
            "WHERE venue_id=? AND venue_symbol=? AND domain='trade' "
            "AND day LIKE ? AND status IN ('ok', 'empty')",
            (venue_id, venue_symbol, f"{month.replace('-', '')}%"),
        ).fetchone()
        expected_rows = int(expected[0]) if expected is not None else 0
        if progress is not None:
            progress(
                f"[{index}/{len(selected)}] START {venue_id}/{venue_symbol} "
                f"{month} source_rows={expected_rows:,}"
            )
        started = time.perf_counter()
        try:
            result = materialize_archive_trade_month(
                root, conn, venue_id, venue_symbol, month,
                mapping_revision, code_version, progress,
            )
        except (OSError, sqlite3.Error, ValueError, duckdb.Error) as exc:
            if progress is not None:
                progress(
                    f"[{index}/{len(selected)}] FAILED {month} "
                    f"elapsed={time.perf_counter() - started:.1f}s "
                    f"reason={exc}"
                )
            raise
        results.append(result)
        total_rows += result.row_count
        if progress is not None:
            action = "REUSED" if result.reused else "DONE"
            progress(
                f"[{index}/{len(selected)}] {action} {month} "
                f"rows={result.row_count:,} rejected={result.rejected_rows:,} "
                f"elapsed={time.perf_counter() - started:.1f}s "
                f"cumulative_rows={total_rows:,}"
            )
    if progress is not None:
        progress(
            f"COMPLETE months={len(results)} rows={total_rows:,} "
            f"elapsed={time.perf_counter() - total_started:.1f}s"
        )
    return results


def materialize_trade_month(
    root: Path,
    conn: sqlite3.Connection,
    venue_id: str,
    venue_symbol: str,
    event_month: str,
    mapping_revision: int | None = None,
    normalization_version: str = DEFAULT_NORMALIZATION_VERSION,
    code_version: str = "working-tree",
) -> MaterializationResult:
    """将一个来源市场月物化为可重放 Parquet。"""
    _month_bounds(event_month)
    registry.register_all(conn)
    ensure_markets(conn)
    selected_market_id, instrument_id, revision = _market_row(
        conn, venue_id, venue_symbol, mapping_revision
    )
    artifacts = _register_source_artifacts(
        root, conn, venue_id, venue_symbol, event_month,
        normalization_version,
    )
    input_hash = _input_set_hash(artifacts)
    completed = _completed_result(
        conn, selected_market_id, event_month,
        normalization_version, input_hash,
    )
    if completed is not None:
        output = root / completed.output_path
        if not output.is_file():
            raise FileNotFoundError(output)
        if artifact_id(sha256_file(output)) != completed.output_artifact_id:
            raise ValueError(f"既有输出散列不符: {output}")
        return completed

    config_body = json.dumps({
        "dataset": DATASET_TRADE,
        "event_month": event_month,
        "market_id": selected_market_id,
        "normalization_version": normalization_version,
        "schema_version": TRADE_SCHEMA_VERSION,
    }, sort_keys=True, separators=(",", ":"))
    config_hash = hashlib.sha256(config_body.encode("utf-8")).hexdigest()
    attempt_id = f"mat-{uuid.uuid4().hex}"
    started_at = utc_now()
    source_total = sum(item.source_rows for item in artifacts)
    rejected_total = sum(item.rejected_rows for item in artifacts)
    conn.execute(
        "INSERT INTO partition_attempt (attempt_id, market_id, domain, "
        "partition_key, normalization_version, input_set_hash, status, "
        "source_rows, normalized_rows, rejected_rows, started_at, "
        "code_version, config_hash) VALUES (?,?,?,?,?,?,'running',?,?,?,?,?,?)",
        (
            attempt_id, selected_market_id, "trade", event_month,
            normalization_version, input_hash, source_total, 0,
            rejected_total, started_at, code_version, config_hash,
        ),
    )
    _bind_trade_capability(conn, attempt_id, venue_id)
    conn.commit()

    output_dir = _output_directory(
        root, venue_id, selected_market_id, event_month,
        normalization_version,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    temp_path = output_dir / f".{attempt_id}.tmp.parquet"
    staging_path = output_dir / f".{attempt_id}.stage.csv"
    try:
        row_count, selected_counts, min_event, max_event = _stage_trade_rows(
            root, conn, staging_path, venue_id, venue_symbol, selected_market_id,
            instrument_id, revision, event_month, normalization_version,
            artifacts,
        )
        expected_rows = sum(item.normalized_rows for item in artifacts)
        if row_count != expected_rows:
            raise ValueError(
                f"物化行数不符: expected={expected_rows} actual={row_count}"
            )
        source_counts = {
            item.storage_path: item.source_rows for item in artifacts
        }
        rejected_counts = {
            item.storage_path: item.rejected_rows for item in artifacts
        }
        return _finish_staged_attempt(
            root, conn, attempt_id, venue_id, selected_market_id,
            event_month, normalization_version, artifacts, source_counts,
            selected_counts, rejected_counts, (), row_count, min_event,
            max_event, staging_path, temp_path, output_dir,
        )
    except (OSError, sqlite3.Error, ValueError, duckdb.Error) as exc:
        conn.rollback()
        if temp_path.exists():
            temp_path.unlink()
        if staging_path.exists():
            staging_path.unlink()
        _remove_unregistered_attempt_files(
            root, conn, output_dir, attempt_id
        )
        _fail_attempt(conn, attempt_id, str(exc))
        raise


def _parse_market_args(values: Sequence[str] | None) -> tuple[tuple[str, str], ...]:
    """解析重复的 venue:symbol 参数；缺省为全部本地完整归档市场。"""
    if not values:
        return DEFAULT_BACKFILL_MARKETS
    parsed: list[tuple[str, str]] = []
    for value in values:
        venue_id, separator, venue_symbol = value.partition(":")
        if not separator or not venue_id or not venue_symbol:
            raise ValueError(f"市场参数须为 venue:symbol: {value}")
        parsed.append((venue_id, venue_symbol))
    return tuple(dict.fromkeys(parsed))


def _add_backfill_arguments(parser: argparse.ArgumentParser) -> None:
    """登记全局计划与执行命令的共同参数。"""
    parser.add_argument(
        "--market", action="append", default=None,
        help="重复指定 venue:symbol；缺省为全部本地完整归档市场",
    )
    parser.add_argument("--from-month", default=None)
    parser.add_argument("--to-month", default=None)


def main(argv: Sequence[str] | None = None) -> int:
    """命令行入口。"""
    parser = argparse.ArgumentParser(description="guvolu 分析物化")
    parser.add_argument("--data-root", type=Path, default=None)
    sub = parser.add_subparsers(dest="command", required=True)
    trades = sub.add_parser("trades", help="物化一个来源市场月")
    trades.add_argument("--venue", required=True)
    trades.add_argument("--symbol", required=True)
    trades.add_argument("--event-month", required=True, help="YYYY-MM")
    trades.add_argument("--mapping-revision", type=int, default=None)
    trades.add_argument(
        "--normalization-version",
        default=DEFAULT_NORMALIZATION_VERSION,
    )
    trades.add_argument("--code-version", default="working-tree")
    archive_month = sub.add_parser(
        "archive-trades", help="从完整日归档直接物化一个市场月"
    )
    archive_month.add_argument("--venue", required=True)
    archive_month.add_argument("--symbol", required=True)
    archive_month.add_argument("--event-month", required=True, help="YYYY-MM")
    archive_month.add_argument("--mapping-revision", type=int, default=None)
    archive_month.add_argument("--code-version", default="working-tree")
    archive_market = sub.add_parser(
        "archive-market", help="从完整日归档扩展一个市场的全部月份"
    )
    archive_market.add_argument("--venue", required=True)
    archive_market.add_argument("--symbol", required=True)
    archive_market.add_argument("--from-month", default=None)
    archive_market.add_argument("--to-month", default=None)
    archive_market.add_argument("--mapping-revision", type=int, default=None)
    archive_market.add_argument("--code-version", default="working-tree")
    archive_plan = sub.add_parser(
        "archive-plan", help="显示全部本地归档市场物化进度与阻断月"
    )
    _add_backfill_arguments(archive_plan)
    archive_backfill = sub.add_parser(
        "archive-backfill", help="断点续跑全部可完成归档市场月份"
    )
    _add_backfill_arguments(archive_backfill)
    archive_backfill.add_argument("--code-version", default="working-tree")
    sub.add_parser("audit", help="复核制品散列与完成输出")
    repair = sub.add_parser(
        "repair-control-ledger", help="规划或只增修复物化控制台账"
    )
    repair.add_argument(
        "--apply", action="store_true", help="显式执行已验证的只增修复"
    )
    recover = sub.add_parser("recover-stale", help="收束超时物化尝试")
    recover.add_argument("--older-minutes", type=int, default=60)
    args = parser.parse_args(argv)
    root = (args.data_root or configured_data_root()).resolve()
    if args.command == "repair-control-ledger" and not bool(args.apply):
        readonly = store.connect_readonly(root)
        if readonly is None:
            raise FileNotFoundError(f"SQLite 控制库不存在: {root}")
        conn = readonly
    else:
        conn = store.connect(root)
    try:
        if args.command == "trades":
            result: object = materialize_trade_month(
                root, conn, str(args.venue), str(args.symbol),
                str(args.event_month), args.mapping_revision,
                str(args.normalization_version), str(args.code_version),
            )
            exit_code = 0
        elif args.command == "archive-trades":
            result = materialize_archive_trade_month(
                root, conn, str(args.venue), str(args.symbol),
                str(args.event_month), args.mapping_revision,
                str(args.code_version),
                lambda message: print(message, file=sys.stderr, flush=True),
            )
            exit_code = 0
        elif args.command == "archive-market":
            result = materialize_archive_market(
                root, conn, str(args.venue), str(args.symbol),
                args.from_month, args.to_month, args.mapping_revision,
                str(args.code_version),
                lambda message: print(message, file=sys.stderr, flush=True),
            )
            exit_code = 0
        elif args.command == "archive-plan":
            plan = plan_archive_backfill(
                root, conn, _parse_market_args(args.market),
                args.from_month, args.to_month,
            )
            result = archive_backfill_plan_summary(plan)
            exit_code = 0
        elif args.command == "archive-backfill":
            result = run_archive_backfill(
                root, conn, _parse_market_args(args.market),
                args.from_month, args.to_month, str(args.code_version),
                lambda message: print(message, file=sys.stderr, flush=True),
            )
            failed_now = result["failed_now"]
            assert isinstance(failed_now, int)
            exit_code = 1 if failed_now else 0
        elif args.command == "audit":
            audit = audit_materializations(root, conn)
            result = {
                **asdict(audit),
                "ok": not audit.errors,
            }
            exit_code = 1 if audit.errors else 0
        elif args.command == "repair-control-ledger":
            if bool(args.apply):
                with sqlite_writer_lock(root):
                    result = repair_materialization_controls(
                        root, conn, apply=True,
                    )
            else:
                result = repair_materialization_controls(root, conn)
            exit_code = 0
        elif args.command == "recover-stale":
            stale, removed = recover_stale_attempts(
                root, conn, int(args.older_minutes)
            )
            result = {
                "stale_attempts_closed": stale,
                "temporary_files_removed": removed,
            }
            exit_code = 0
        else:
            raise ValueError(f"未知命令: {args.command}")
    finally:
        conn.close()
    body: object
    if isinstance(result, MaterializationResult):
        body = asdict(result)
    elif isinstance(result, list):
        body = [asdict(item) for item in result]
    else:
        body = result
    print(json.dumps(body, ensure_ascii=False, indent=2))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
