"""封口 fx_rate segment 到 ``fx_rate`` Parquet（TBD-32 实时旁路）。

登记链与实时逐笔物化一致：attempt、能力绑定、输入与位置绑定、输出制品、
清单、活动 head 与 raw v3 连接/频道观察在同一事务内提交。每个封口段
一个 attempt；只接受 raw v3 与 EP-0076 r0；未登记市场的 symbol 失败关闭。
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sqlite3
import time
import uuid
from collections.abc import Collection, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path, PurePosixPath
from typing import Any

import duckdb

from guvolu.data import store
from guvolu.data.durable_io import atomic_write_text
from guvolu.data.fx_capture import (
    ENDPOINT_BINDING,
    FX_RATE_DOMAIN,
    FX_SOURCE_ENDPOINT,
    FX_VENUE_ID,
    fx_channel_id,
    parse_ticker_frame,
)
from guvolu.data.materialize import (
    SourceArtifact,
    _input_set_hash,
    _market_row,
    _register_content_artifact,
    _relative_storage_path,
    _resolve_recorded_path,
    artifact_id,
    ensure_markets,
    sha256_file,
    utc_now,
)
from guvolu.data.paths import data_root as configured_data_root
from guvolu.data.realtime_control import (
    RealtimeChannelObservation,
    register_materialized_raw_v3_observations,
)
from guvolu.data.sqlite_writer_lock import sqlite_writer_lock
from guvolu.data.watch_connection import connect_with_retry
from guvolu.venues import registry

DATASET_FX_RATE = "fx_rate"
FX_RATE_SCHEMA_VERSION = 1
FX_RATE_NORMALIZATION_VERSION = "fx-rate-normalization-v1"
SUPPORTED_RAW_SCHEMA_VERSIONS = frozenset({3})
ALLOWED_ENDPOINT_REVISIONS = frozenset({0})
FAILED_RETRY_SECONDS = 3600
MIN_WATCH_INTERVAL_SECONDS = 10.0
# 常驻循环每隔多少轮全量复核
FULL_SCAN_EVERY_CYCLES = 288
_MID_DIVISOR = Decimal(2)
_RAW_V3_QUALITY_FLAGS = (
    "connection_channel_identity_verified",
    "endpoint_binding_verified",
    "raw_payload_hash_verified",
    "receive_clock_verified",
)
_TIMESTAMP_COLUMNS = ("event_time", "available_time", "ingest_time")


@dataclass(frozen=True)
class FxSegmentInput:
    """经散列验证的 fx_rate segment。"""

    manifest_path: Path
    venue_symbol: str
    run_id: str
    segment_sequence: int
    raw_schema_version: int
    endpoint_id: str
    endpoint_revision: int
    artifact: SourceArtifact

    @property
    def partition_key(self) -> str:
        return f"{self.run_id}/segment-{self.segment_sequence:06d}"


@dataclass(frozen=True)
class FxScanStats:
    """一轮输入选择的扫描成本。"""

    scanned_manifests: int
    hash_recomputed: int
    hash_reused: int
    elapsed_scan_seconds: float
    skipped_completed: int = 0


@dataclass(frozen=True)
class FxRateResult:
    """一个 fx_rate segment 的物化结果。"""

    attempt_id: str
    market_id: str
    partition_key: str
    status: str
    source_rows: int
    data_frames: int
    rate_rows: int
    ignored_rows: int
    rejected_rows: int
    output_path: str
    reused: bool


class MaterializationRetryDeferred(ValueError):
    """同一输入与配置刚失败过；本轮不重复追加失败 attempt。"""


@dataclass(frozen=True)
class _RawFrameMetadata:
    """不得从报价 payload 猜测的 wire envelope 身份。"""

    ingest_time: datetime
    endpoint_id: str
    endpoint_revision: int
    connection_id: str
    channel_id: str
    recv_ts_mono_ns: int
    raw_payload_sha256: str
    record_sequence: int


def _iso(value: object, label: str) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{label} 缺少时区")
    return parsed.astimezone(UTC)


def _path_part(path: Path, prefix: str) -> str:
    return next(
        (
            part.split("=", 1)[1]
            for part in path.parts
            if part.startswith(prefix)
        ),
        "",
    )


def _registered_input_hashes(
    conn: sqlite3.Connection,
) -> dict[str, tuple[str, int]]:
    """取已完成 fx_rate attempt 输入制品的登记散列与字节数。"""
    rows = conn.execute(
        "SELECT DISTINCT r.storage_path,r.sha256,r.byte_count FROM artifact r "
        "JOIN partition_input i ON i.artifact_id=r.artifact_id "
        "JOIN partition_attempt a ON a.attempt_id=i.attempt_id "
        "WHERE a.domain=? AND a.status LIKE 'complete%' "
        "AND r.artifact_kind='raw_realtime_segment'",
        (FX_RATE_DOMAIN,),
    ).fetchall()
    return {str(row[0]): (str(row[1]), int(row[2])) for row in rows}


def _completed_input_paths(conn: sqlite3.Connection) -> frozenset[str]:
    """取现行规范化版本下已完成物化的原件登记路径。"""
    rows = conn.execute(
        "SELECT DISTINCT r.storage_path FROM artifact r "
        "JOIN partition_input i ON i.artifact_id=r.artifact_id "
        "JOIN partition_attempt a ON a.attempt_id=i.attempt_id "
        "WHERE a.domain=? AND a.normalization_version=? "
        "AND a.status IN ('complete','complete_with_rejections') "
        "AND r.artifact_kind='raw_realtime_segment'",
        (FX_RATE_DOMAIN, FX_RATE_NORMALIZATION_VERSION),
    ).fetchall()
    return frozenset(str(row[0]) for row in rows)


def _sealed_inputs(
    root: Path, *,
    registered_hashes: Mapping[str, tuple[str, int]] | None = None,
) -> list[FxSegmentInput]:
    return _scan_sealed_inputs(root, registered_hashes=registered_hashes)[0]


def _scan_sealed_inputs(
    root: Path, *,
    registered_hashes: Mapping[str, tuple[str, int]] | None = None,
    skip_paths: Collection[str] | None = None,
) -> tuple[list[FxSegmentInput], FxScanStats]:
    """选择封口 fx_rate segment；登记散列命中时复用，不一致即失败。

    `skip_paths` 内的原件不读 manifest 直接跳过，供常驻循环把单轮
    成本限制在新封口段；全量复核不传该参数。
    """
    started = time.monotonic()
    inputs: list[FxSegmentInput] = []
    scanned = recomputed = reused = skipped = 0
    base = root / "raw" / "realtime" / FX_RATE_DOMAIN
    if not base.is_dir():
        return [], FxScanStats(0, 0, 0, round(time.monotonic() - started, 3))
    endpoint_id, _ = ENDPOINT_BINDING
    for manifest_path in sorted(base.rglob("segment-*.manifest.json")):
        if skip_paths is not None:
            candidate = manifest_path.with_name(
                manifest_path.name.removesuffix(".manifest.json") + ".jsonl"
            ).relative_to(root).as_posix()
            if candidate in skip_paths:
                skipped += 1
                continue
        scanned += 1
        body = json.loads(manifest_path.read_text(encoding="utf-8"))
        if not isinstance(body, Mapping):
            continue
        if (
            body.get("status") != "sealed"
            or body.get("completion_claim") is not True
        ):
            continue
        recorded = str(body["storage_path"])
        path = (root / recorded).resolve()
        try:
            path.relative_to(root.resolve())
        except ValueError as exc:
            raise ValueError(f"fx_rate segment 路径越界: {recorded}") from exc
        expected_path = manifest_path.with_name(
            manifest_path.name.removesuffix(".manifest.json") + ".jsonl"
        ).resolve()
        if path != expected_path:
            raise ValueError(f"fx_rate manifest 与 segment 不同目录: {recorded}")
        recorded_sha = str(body["sha256"])
        recorded_bytes = int(str(body["byte_count"]))
        if path.stat().st_size != recorded_bytes:
            raise ValueError(f"fx_rate segment 字节数不符: {recorded}")
        registered = (
            None if registered_hashes is None
            else registered_hashes.get(recorded)
        )
        if registered is None:
            sha = sha256_file(path)
            recomputed += 1
            if sha != recorded_sha:
                raise ValueError(f"fx_rate segment 散列不符: {recorded}")
        else:
            if registered != (recorded_sha, recorded_bytes):
                raise ValueError(f"fx_rate segment 登记散列不符: {recorded}")
            sha = recorded_sha
            reused += 1
        if body.get("artifact_id") != artifact_id(sha):
            raise ValueError(f"fx_rate segment artifact_id 不符: {recorded}")
        raw_schema_version = int(str(body.get("schema_version", 1)))
        if raw_schema_version not in SUPPORTED_RAW_SCHEMA_VERSIONS:
            raise ValueError(
                f"fx_rate segment schema_version 尚不支持: "
                f"{recorded}: {raw_schema_version}"
            )
        if _path_part(path, "venue_id=") != FX_VENUE_ID:
            raise ValueError(f"fx_rate segment 场所不受支持: {recorded}")
        venue_symbol = _path_part(path, "venue_symbol=")
        if not venue_symbol or body.get("venue_symbol") != venue_symbol:
            raise ValueError(f"fx_rate manifest symbol 与路径不一致: {recorded}")
        revision_raw = body.get("endpoint_revision")
        if isinstance(revision_raw, bool) or not isinstance(revision_raw, int):
            raise ValueError(
                f"raw v3 manifest endpoint_revision 非整数: {recorded}"
            )
        if (
            body.get("endpoint_id") != endpoint_id
            or revision_raw not in ALLOWED_ENDPOINT_REVISIONS
        ):
            raise ValueError(f"raw v3 manifest 端点绑定非法: {recorded}")
        inputs.append(FxSegmentInput(
            manifest_path=manifest_path,
            venue_symbol=venue_symbol,
            run_id=str(body["run_id"]),
            segment_sequence=int(str(body["segment_sequence"])),
            raw_schema_version=raw_schema_version,
            endpoint_id=endpoint_id,
            endpoint_revision=revision_raw,
            artifact=SourceArtifact(
                artifact_id=artifact_id(sha), storage_path=recorded,
                absolute_path=path, source_rows=int(str(body["record_count"])),
                normalized_rows=0, rejected_rows=0,
            ),
        ))
    return inputs, FxScanStats(
        scanned, recomputed, reused, round(time.monotonic() - started, 3),
        skipped,
    )


def _bind_capability(conn: sqlite3.Connection, attempt_id: str) -> int:
    row = conn.execute(
        "SELECT revision_id FROM venue_capability_revision "
        "WHERE venue_id=? AND domain=? AND endpoint=? "
        "AND available=1 AND implementation_status='implemented' "
        "ORDER BY revision_id DESC LIMIT 1",
        (FX_VENUE_ID, FX_RATE_DOMAIN, FX_SOURCE_ENDPOINT),
    ).fetchone()
    if row is None:
        raise ValueError(
            f"fx_rate 能力未登记: {FX_VENUE_ID}/{FX_SOURCE_ENDPOINT}"
        )
    revision = int(row[0])
    conn.execute(
        "INSERT INTO partition_capability_binding "
        "(attempt_id,venue_id,domain,endpoint,revision_id,binding_basis,bound_at) "
        "VALUES (?,?,?,?,?,'recorded',?)",
        (
            attempt_id, FX_VENUE_ID, FX_RATE_DOMAIN, FX_SOURCE_ENDPOINT,
            revision, utc_now(),
        ),
    )
    return revision


def _register_source(conn: sqlite3.Connection, item: FxSegmentInput) -> None:
    created = datetime.fromtimestamp(
        item.artifact.absolute_path.stat().st_mtime, UTC
    ).isoformat()
    _register_content_artifact(
        conn, item.artifact.artifact_id, "raw_realtime_segment",
        item.artifact.storage_path,
        item.artifact.artifact_id.removeprefix("sha256-"),
        item.artifact.absolute_path.stat().st_size, created,
        item.raw_schema_version,
    )


def _raw_metadata(
    envelope: Mapping[str, object], item: FxSegmentInput,
) -> tuple[str, _RawFrameMetadata]:
    """验证 raw manifest、行与 payload 三层来源身份（仅 raw v3）。"""
    payload = envelope.get("payload_raw")
    if not isinstance(payload, str):
        raise ValueError("segment 行 payload_raw 不是字符串")
    if int(str(envelope.get("schema_version", 1))) != item.raw_schema_version:
        raise ValueError("segment 行 schema_version 与 manifest 不一致")
    if envelope.get("source_endpoint") != FX_SOURCE_ENDPOINT:
        raise ValueError("segment 行 source_endpoint 与端点契约不一致")
    if envelope.get("endpoint_id") != item.endpoint_id:
        raise ValueError("raw endpoint_id 与 manifest 不一致")
    revision = envelope.get("endpoint_revision")
    if (
        isinstance(revision, bool)
        or not isinstance(revision, int)
        or revision != item.endpoint_revision
    ):
        raise ValueError("raw v3 endpoint_revision 与端点契约不一致")
    connection_id = envelope.get("connection_id")
    if (
        not isinstance(connection_id, str)
        or not connection_id.startswith(f"{item.run_id}-c")
    ):
        raise ValueError("raw connection_id 不属于当前 source_session")
    channel_id = envelope.get("channel_id")
    if not isinstance(channel_id, str) or channel_id != fx_channel_id(payload):
        raise ValueError("raw channel_id 与 wire 帧不一致")
    ingest = _iso(envelope["ingest_time"], "ingest_time")
    if _iso(envelope["recv_ts_utc"], "recv_ts_utc") != ingest:
        raise ValueError("raw recv_ts_utc 与 ingest_time 不一致")
    mono = envelope.get("recv_ts_mono_ns")
    if (
        isinstance(mono, bool)
        or not isinstance(mono, int)
        or mono < 0
        or mono > 2**64 - 1
    ):
        raise ValueError("raw recv_ts_mono_ns 非法")
    computed_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    if envelope.get("raw_payload_sha256") != computed_hash:
        raise ValueError("raw payload SHA-256 校验失败")
    record_sequence = envelope.get("record_sequence")
    if (
        isinstance(record_sequence, bool)
        or not isinstance(record_sequence, int)
        or record_sequence <= 0
    ):
        raise ValueError("raw v3 record_sequence 非法")
    return payload, _RawFrameMetadata(
        ingest_time=ingest,
        endpoint_id=str(item.endpoint_id),
        endpoint_revision=revision,
        connection_id=connection_id,
        channel_id=channel_id,
        recv_ts_mono_ns=mono,
        raw_payload_sha256=computed_hash,
        record_sequence=record_sequence,
    )


def _stage(
    item: FxSegmentInput, market_id: str, mapping_revision: int,
    capability_revision: int, instrument_id: str, staging: Path,
) -> tuple[
    int, int, list[tuple[int, str]], list[tuple[int, str]], str, str,
    tuple[RealtimeChannelObservation, ...],
]:
    data_frames = rate_rows = 0
    ignored: list[tuple[int, str]] = []
    rejected: list[tuple[int, str]] = []
    min_event = ""
    max_event = ""
    observations: list[RealtimeChannelObservation] = []
    previous_record_sequence: int | None = None
    quality = json.dumps(list(_RAW_V3_QUALITY_FLAGS), separators=(",", ":"))
    with (
        item.artifact.absolute_path.open(encoding="utf-8") as source,
        staging.open("w", encoding="utf-8", newline="") as target,
    ):
        writer = csv.writer(target, lineterminator="\n")
        for source_row, line in enumerate(source, start=1):
            try:
                envelope = json.loads(line)
                if not isinstance(envelope, Mapping):
                    raise ValueError("segment 行不是对象")
                if (
                    envelope.get("venue_id") != FX_VENUE_ID
                    or envelope.get("venue_symbol") != item.venue_symbol
                    or envelope.get("run_id") != item.run_id
                    or int(str(envelope.get("segment_sequence")))
                    != item.segment_sequence
                    or envelope.get("domain") != FX_RATE_DOMAIN
                ):
                    raise ValueError("fx_rate segment 身份不符")
                payload_raw, raw = _raw_metadata(envelope, item)
                if (
                    previous_record_sequence is not None
                    and raw.record_sequence <= previous_record_sequence
                ):
                    raise ValueError("raw v3 record_sequence 未严格递增")
                previous_record_sequence = raw.record_sequence
                frame = parse_ticker_frame(payload_raw)
                if frame is None:
                    ignored.append((source_row, "protocol_control_frame"))
                    continue
                quote = frame.quotes.get(item.venue_symbol)
                if quote is None:
                    raise ValueError(f"ticker 缺少 {item.venue_symbol}")
                event = quote.event_time.isoformat()
                # 可见时刻取事件与落盘较晚者
                available = max(quote.event_time, raw.ingest_time).isoformat()
                mid = str((quote.bid + quote.ask) / _MID_DIVISOR)
                observation = "|".join((
                    FX_VENUE_ID, item.venue_symbol, event,
                    item.artifact.artifact_id, str(source_row),
                ))
                writer.writerow([
                    "\\N" if value is None else value
                    for value in (
                        observation, FX_VENUE_ID, item.venue_symbol,
                        market_id, mapping_revision, capability_revision,
                        instrument_id, event, available,
                        raw.ingest_time.isoformat(),
                        (
                            None if frame.response_time is None
                            else frame.response_time.isoformat()
                        ),
                        str(quote.bid), str(quote.ask), mid, quote.status,
                        FX_SOURCE_ENDPOINT, raw.endpoint_id,
                        raw.endpoint_revision, raw.connection_id,
                        raw.channel_id, raw.recv_ts_mono_ns,
                        raw.raw_payload_sha256, quality,
                        item.raw_schema_version, item.run_id,
                        item.segment_sequence, item.artifact.artifact_id,
                        source_row, FX_RATE_NORMALIZATION_VERSION,
                        FX_RATE_SCHEMA_VERSION,
                    )
                ])
                observations.append(RealtimeChannelObservation(
                    raw.connection_id, raw.channel_id,
                    raw.ingest_time.isoformat(),
                ))
                data_frames += 1
                rate_rows += 1
                min_event = event if not min_event else min(min_event, event)
                max_event = event if not max_event else max(max_event, event)
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                rejected.append((source_row, f"{type(exc).__name__}: {exc}"))
        target.flush()
        os.fsync(target.fileno())
    if item.artifact.source_rows != data_frames + len(ignored) + len(rejected):
        raise ValueError("fx_rate 来源帧分类不守恒")
    return (
        data_frames, rate_rows, ignored, rejected, min_event, max_event,
        tuple(observations),
    )


def _create_table(db: Any) -> None:
    db.execute("""
        CREATE TABLE fx_rate (
          observation_id VARCHAR, venue_id VARCHAR, symbol VARCHAR,
          market_id VARCHAR, mapping_revision INTEGER,
          capability_revision INTEGER, instrument_id VARCHAR,
          event_time TIMESTAMPTZ, available_time TIMESTAMPTZ,
          ingest_time TIMESTAMPTZ, response_time TIMESTAMPTZ,
          bid VARCHAR, ask VARCHAR, mid VARCHAR, status VARCHAR,
          source_endpoint VARCHAR, endpoint_id VARCHAR,
          endpoint_revision INTEGER, connection_id VARCHAR,
          channel_id VARCHAR, recv_ts_mono_ns UBIGINT,
          raw_payload_sha256 VARCHAR, data_quality VARCHAR,
          raw_schema_version INTEGER, run_id VARCHAR,
          segment_sequence INTEGER, source_artifact_id VARCHAR,
          source_row_index BIGINT, normalization_version VARCHAR,
          schema_version INTEGER
        )
    """)


def _copy_csv(db: Any, path: Path) -> None:
    escaped = path.as_posix().replace("'", "''")
    db.execute(
        f"COPY fx_rate FROM '{escaped}' (FORMAT CSV, HEADER false, NULL '\\N')"
    )


def _write_parquet(db: Any, path: Path) -> None:
    escaped = path.as_posix().replace("'", "''")
    db.execute(
        "COPY (SELECT * FROM fx_rate ORDER BY event_time,source_row_index) "
        f"TO '{escaped}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    with path.open("rb+") as handle:
        handle.flush()
        os.fsync(handle.fileno())


def _finalize(temp: Path) -> tuple[Path, str]:
    sha = sha256_file(temp)
    final = temp.with_name(f"part-{sha[:12]}.parquet")
    if final.exists():
        if sha256_file(final) != sha:
            raise ValueError(f"fx_rate 输出散列冲突: {final}")
        temp.unlink()
    else:
        os.replace(temp, final)
    return final, sha


def _verify_staged(
    db: Any, item: FxSegmentInput, market_id: str, rate_rows: int,
) -> None:
    """提交前核对计数、键、PIT、市场、原件、版本与端点绑定。"""
    check = db.execute(
        "SELECT COUNT(*),COUNT(*)-COUNT(DISTINCT observation_id),"
        "SUM(available_time<event_time),COUNT(DISTINCT market_id),"
        "MIN(market_id),COUNT(DISTINCT source_artifact_id),"
        "MIN(source_artifact_id),COUNT(DISTINCT normalization_version),"
        "MIN(normalization_version),COUNT(DISTINCT schema_version),"
        "MIN(schema_version),COUNT(DISTINCT endpoint_id),MIN(endpoint_id),"
        "COUNT(DISTINCT endpoint_revision),MIN(endpoint_revision),"
        "SUM(CASE WHEN connection_id IS NULL OR channel_id IS NULL "
        "OR recv_ts_mono_ns IS NULL OR length(raw_payload_sha256)!=64 "
        "OR bid IS NULL OR ask IS NULL OR mid IS NULL THEN 1 ELSE 0 END) "
        "FROM fx_rate"
    ).fetchone()
    if check is None or int(check[0]) != rate_rows:
        raise ValueError("fx_rate 输出计数不符")
    if int(check[1] or 0) or int(check[2] or 0):
        raise ValueError("fx_rate 键或 PIT 契约不符")
    if rate_rows and (
        int(check[3]) != 1 or str(check[4]) != market_id
        or int(check[5]) != 1
        or str(check[6]) != item.artifact.artifact_id
        or int(check[7]) != 1
        or str(check[8]) != FX_RATE_NORMALIZATION_VERSION
        or int(check[9]) != 1
        or int(check[10]) != FX_RATE_SCHEMA_VERSION
    ):
        raise ValueError("fx_rate 来源或版本契约不符")
    if rate_rows and (
        int(check[11]) != 1 or str(check[12]) != item.endpoint_id
        or int(check[13]) != 1 or int(check[14]) != item.endpoint_revision
        or int(check[15] or 0)
    ):
        raise ValueError("fx_rate raw v3 来源保真契约不符")


def materialize_segment(
    root: Path, conn: sqlite3.Connection, item: FxSegmentInput,
) -> FxRateResult:
    registry.register_all(conn)
    ensure_markets(conn)
    market_id, instrument_id, mapping_revision = _market_row(
        conn, FX_VENUE_ID, item.venue_symbol, None
    )
    _register_source(conn, item)
    conn.commit()
    input_hash = _input_set_hash([item.artifact])
    config_hash = hashlib.sha256(json.dumps({
        "dataset": DATASET_FX_RATE,
        "normalization_version": FX_RATE_NORMALIZATION_VERSION,
        "schema_version": FX_RATE_SCHEMA_VERSION,
        "frame_atomicity": "reject-whole-source-row-v1",
        "supported_raw_schema_versions": sorted(SUPPORTED_RAW_SCHEMA_VERSIONS),
    }, sort_keys=True).encode()).hexdigest()
    existing = conn.execute(
        "SELECT a.attempt_id,a.status,a.source_rows,a.normalized_rows,"
        "a.ignored_rows,a.rejected_rows,r.storage_path "
        "FROM partition_attempt a JOIN materialization_output o "
        "ON o.attempt_id=a.attempt_id JOIN artifact r "
        "ON r.artifact_id=o.artifact_id "
        "WHERE a.market_id=? AND a.domain=? AND a.partition_key=? "
        "AND a.normalization_version=? AND a.input_set_hash=? "
        "AND a.status IN ('complete','complete_with_rejections') LIMIT 1",
        (
            market_id, FX_RATE_DOMAIN, item.partition_key,
            FX_RATE_NORMALIZATION_VERSION, input_hash,
        ),
    ).fetchone()
    if existing is not None:
        return FxRateResult(
            str(existing[0]), market_id, item.partition_key, str(existing[1]),
            int(existing[2]), 0, int(existing[3]), int(existing[4]),
            int(existing[5]), str(existing[6]), True,
        )
    recent_failure = conn.execute(
        "SELECT attempt_id,finished_at,failure_detail FROM partition_attempt "
        "WHERE market_id=? AND domain=? AND partition_key=? "
        "AND normalization_version=? AND input_set_hash=? AND config_hash=? "
        "AND status='failed' ORDER BY finished_at DESC LIMIT 1",
        (
            market_id, FX_RATE_DOMAIN, item.partition_key,
            FX_RATE_NORMALIZATION_VERSION, input_hash, config_hash,
        ),
    ).fetchone()
    if recent_failure is not None and recent_failure[1]:
        failed_at = datetime.fromisoformat(str(recent_failure[1]))
        if failed_at.tzinfo is None:
            failed_at = failed_at.replace(tzinfo=UTC)
        age = (datetime.now(UTC) - failed_at.astimezone(UTC)).total_seconds()
        if age < FAILED_RETRY_SECONDS:
            raise MaterializationRetryDeferred(
                f"{item.partition_key} 延迟重试；最近 attempt="
                f"{recent_failure[0]}: {recent_failure[2]}"
            )
    attempt_id = f"fx-rate-{uuid.uuid4().hex}"
    conn.execute(
        "INSERT INTO partition_attempt "
        "(attempt_id,market_id,domain,partition_key,normalization_version,"
        "input_set_hash,status,source_rows,normalized_rows,ignored_rows,"
        "rejected_rows,started_at,code_version,config_hash) "
        "VALUES (?,?,?,?,?,?,'running',?,0,0,0,?,'working-tree',?)",
        (
            attempt_id, market_id, FX_RATE_DOMAIN, item.partition_key,
            FX_RATE_NORMALIZATION_VERSION, input_hash,
            item.artifact.source_rows, utc_now(), config_hash,
        ),
    )
    capability_revision = _bind_capability(conn, attempt_id)
    conn.commit()
    output_dir = _resolve_recorded_path(
        root,
        PurePosixPath(
            "materialized", DATASET_FX_RATE,
            f"schema_version={FX_RATE_SCHEMA_VERSION}",
            f"normalization_version={FX_RATE_NORMALIZATION_VERSION}",
            f"venue_id={FX_VENUE_ID}", f"market_id={market_id}",
            f"run_id={item.run_id}",
            f"segment={item.segment_sequence:06d}",
        ).as_posix(),
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    staging = output_dir / f".{attempt_id}.csv"
    temporary = output_dir / f".{attempt_id}.parquet"
    try:
        (
            data_frames, rate_rows, ignored, rejected, min_event, max_event,
            observations,
        ) = _stage(
            item, market_id, mapping_revision, capability_revision,
            instrument_id, staging,
        )
        db: Any = duckdb.connect(":memory:")
        db.execute("SET TimeZone='UTC'")
        try:
            _create_table(db)
            if rate_rows:
                _copy_csv(db, staging)
            _verify_staged(db, item, market_id, rate_rows)
            _write_parquet(db, temporary)
        finally:
            db.close()
        staging.unlink()
        output_path, output_sha = _finalize(temporary)
        finished = utc_now()
        status = "complete_with_rejections" if rejected else "complete"
        storage = _relative_storage_path(root, output_path)
        manifest = {
            "attempt_id": attempt_id, "status": status,
            "market_id": market_id, "partition_key": item.partition_key,
            "normalization_version": FX_RATE_NORMALIZATION_VERSION,
            "schema_version": FX_RATE_SCHEMA_VERSION,
            "input_schema_version": item.raw_schema_version,
            "endpoint_id": item.endpoint_id,
            "endpoint_revision": item.endpoint_revision,
            "input_artifact_id": item.artifact.artifact_id,
            "source_rows": item.artifact.source_rows,
            "data_frames": data_frames, "rate_rows": rate_rows,
            "ignored_rows": len(ignored), "rejected_rows": len(rejected),
            "output": storage,
        }
        manifest_path = output_dir / f"manifest-{attempt_id}.json"
        atomic_write_text(
            manifest_path,
            json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        )
        conn.execute("BEGIN IMMEDIATE")
        output_id = artifact_id(output_sha)
        _register_content_artifact(
            conn, output_id, "materialized_parquet", storage, output_sha,
            output_path.stat().st_size, finished, FX_RATE_SCHEMA_VERSION,
        )
        conn.execute(
            "INSERT INTO materialization_output VALUES (?,?,?,?,?,?,?)",
            (
                attempt_id, output_id, DATASET_FX_RATE, rate_rows,
                min_event or None, max_event or None, finished,
            ),
        )
        manifest_sha = sha256_file(manifest_path)
        _register_content_artifact(
            conn, artifact_id(manifest_sha), "materialization_manifest",
            _relative_storage_path(root, manifest_path), manifest_sha,
            manifest_path.stat().st_size, finished, 1,
        )
        conn.execute(
            "INSERT INTO partition_input "
            "(attempt_id,artifact_id,source_rows,normalized_rows,ignored_rows,"
            "rejected_rows) VALUES (?,?,?,?,?,?)",
            (
                attempt_id, item.artifact.artifact_id,
                item.artifact.source_rows, rate_rows, len(ignored),
                len(rejected),
            ),
        )
        conn.execute(
            "INSERT INTO partition_input_binding "
            "(attempt_id,artifact_id,storage_path,source_rows,normalized_rows,"
            "ignored_rows,rejected_rows) VALUES (?,?,?,?,?,?,?)",
            (
                attempt_id, item.artifact.artifact_id,
                item.artifact.storage_path, item.artifact.source_rows,
                rate_rows, len(ignored), len(rejected),
            ),
        )
        conn.executemany(
            "INSERT INTO materialization_ignore VALUES (?,?,?,?,?,?,?)",
            [
                (
                    attempt_id, item.artifact.artifact_id, row, -1,
                    f"{item.artifact.storage_path}:{row}", reason, finished,
                )
                for row, reason in ignored
            ],
        )
        conn.executemany(
            "INSERT INTO materialization_rejection VALUES (?,?,?,?,?,?)",
            [
                (
                    attempt_id, item.artifact.artifact_id, row,
                    f"{item.artifact.storage_path}:{row}", reason, finished,
                )
                for row, reason in rejected
            ],
        )
        conn.execute(
            "UPDATE partition_attempt SET status=?,normalized_rows=?,"
            "ignored_rows=?,rejected_rows=?,finished_at=? WHERE attempt_id=?",
            (
                status, rate_rows, len(ignored), len(rejected), finished,
                attempt_id,
            ),
        )
        conn.execute(
            "INSERT INTO materialization_partition_head VALUES (?,?,?,?,?,?) "
            "ON CONFLICT(market_id,domain,partition_key) DO UPDATE SET "
            "normalization_version=excluded.normalization_version,"
            "attempt_id=excluded.attempt_id,activated_at=excluded.activated_at",
            (
                market_id, FX_RATE_DOMAIN, item.partition_key,
                FX_RATE_NORMALIZATION_VERSION, attempt_id, finished,
            ),
        )
        register_materialized_raw_v3_observations(
            conn,
            endpoint_id=item.endpoint_id,
            endpoint_revision=item.endpoint_revision,
            run_id=item.run_id,
            market_id=market_id,
            capability_venue_id=FX_VENUE_ID,
            capability_domain=FX_RATE_DOMAIN,
            capability_endpoint=FX_SOURCE_ENDPOINT,
            capability_revision=capability_revision,
            observations=observations,
        )
        conn.commit()
        return FxRateResult(
            attempt_id, market_id, item.partition_key, status,
            item.artifact.source_rows, data_frames, rate_rows,
            len(ignored), len(rejected), storage, False,
        )
    except Exception as exc:
        conn.rollback()
        for path in (staging, temporary):
            if path.exists():
                path.unlink()
        conn.execute(
            "UPDATE partition_attempt SET status='failed',finished_at=?,"
            "failure_detail=? WHERE attempt_id=? AND status='running'",
            (utc_now(), str(exc)[:2000], attempt_id),
        )
        conn.commit()
        raise


def materialize_all(
    root: Path, conn: sqlite3.Connection, *, report_reused: bool = True,
    verify_all_hashes: bool = False,
) -> list[FxRateResult]:
    """断点复用地物化全部封口 fx_rate segment。"""
    return _materialize_cycle(
        root, conn, report_reused=report_reused,
        verify_all_hashes=verify_all_hashes,
    )[0]


def _materialize_cycle(
    root: Path, conn: sqlite3.Connection, *, report_reused: bool = True,
    verify_all_hashes: bool = False,
) -> tuple[list[FxRateResult], FxScanStats]:
    """物化一轮并返回扫描成本。"""
    inputs, stats = _scan_sealed_inputs(
        root,
        registered_hashes=(
            None if verify_all_hashes else _registered_input_hashes(conn)
        ),
    )
    return _materialize_inputs(
        root, conn, inputs, report_reused=report_reused,
    ), stats


def _materialize_inputs(
    root: Path, conn: sqlite3.Connection, inputs: Sequence[FxSegmentInput],
    *, report_reused: bool,
) -> list[FxRateResult]:
    """逐个物化已选输入；单段失败只报告，不中断其余。"""
    results: list[FxRateResult] = []
    for index, item in enumerate(inputs, start=1):
        try:
            result = materialize_segment(root, conn, item)
        except MaterializationRetryDeferred as exc:
            if report_reused:
                print(
                    f"[{index}/{len(inputs)}] DEFERRED {item.partition_key} "
                    f"reason={exc}",
                    flush=True,
                )
            continue
        except (OSError, sqlite3.Error, ValueError, duckdb.Error) as exc:
            print(
                f"[{index}/{len(inputs)}] FAILED {item.partition_key} "
                f"reason={type(exc).__name__}: {exc}",
                flush=True,
            )
            continue
        results.append(result)
        if report_reused or not result.reused:
            print(
                f"[{index}/{len(inputs)}] "
                f"{'REUSED' if result.reused else 'DONE'} "
                f"{result.market_id} {result.partition_key} "
                f"frames={result.data_frames:,} rates={result.rate_rows:,} "
                f"ignored={result.ignored_rows} rejected={result.rejected_rows}",
                flush=True,
            )
    return results


def audit_fx_rates(root: Path, conn: sqlite3.Connection) -> dict[str, object]:
    """复核活动 fx_rate 输出的键、PIT、时区列与来源绑定。"""
    errors: list[str] = []
    rows = conn.execute(
        "SELECT a.attempt_id,a.market_id,a.normalization_version,"
        "a.normalized_rows,r.storage_path FROM materialization_partition_head h "
        "JOIN partition_attempt a ON a.attempt_id=h.attempt_id "
        "JOIN materialization_output o ON o.attempt_id=a.attempt_id "
        "JOIN artifact r ON r.artifact_id=o.artifact_id "
        "WHERE h.domain=? AND o.dataset=?",
        (FX_RATE_DOMAIN, DATASET_FX_RATE),
    ).fetchall()
    total = 0
    db: Any = duckdb.connect(":memory:")
    db.execute("SET TimeZone='UTC'")
    try:
        for attempt, market, version, expected, path_text in rows:
            parquet_path = str(root / str(path_text))
            column_types = {
                str(row[0]): str(row[1])
                for row in db.execute(
                    "DESCRIBE SELECT * FROM read_parquet(?)", [parquet_path],
                ).fetchall()
            }
            if any(
                column_types.get(column) != "TIMESTAMP WITH TIME ZONE"
                for column in _TIMESTAMP_COLUMNS
            ):
                errors.append(f"fx_rate 时刻列非带时区类型: {attempt}")
                continue
            result = db.execute(
                "SELECT COUNT(*),COUNT(*)-COUNT(DISTINCT observation_id),"
                "SUM(available_time<event_time),COUNT(DISTINCT market_id),"
                "MIN(market_id),COUNT(DISTINCT source_artifact_id),"
                "COUNT(DISTINCT normalization_version),"
                "MIN(normalization_version),COUNT(DISTINCT endpoint_id),"
                "MIN(endpoint_id),COUNT(DISTINCT endpoint_revision),"
                "MIN(endpoint_revision),SUM(CASE WHEN status NOT IN "
                "('OPEN','CLOSE') OR TRY_CAST(bid AS DECIMAL(38,18)) IS NULL "
                "OR TRY_CAST(ask AS DECIMAL(38,18)) IS NULL "
                "OR TRY_CAST(mid AS DECIMAL(38,18)) IS NULL THEN 1 ELSE 0 END) "
                "FROM read_parquet(?)",
                [parquet_path],
            ).fetchone()
            if result is None:
                errors.append(f"fx_rate 输出不可读: {attempt}")
                continue
            count = int(result[0])
            total += count
            if count != int(expected) or int(result[1] or 0) or int(result[2] or 0):
                errors.append(f"fx_rate 计数、键或 PIT 失败: {attempt}")
            if count and (
                int(result[3]) != 1 or str(result[4]) != str(market)
                or int(result[5]) != 1
                or int(result[6]) != 1
                or str(result[7]) != str(version)
            ):
                errors.append(f"fx_rate 市场、原件或版本失败: {attempt}")
            if count and (
                int(result[8]) != 1 or str(result[9]) != ENDPOINT_BINDING[0]
                or int(result[10]) != 1
                or int(result[11]) not in ALLOWED_ENDPOINT_REVISIONS
                or int(result[12] or 0)
            ):
                errors.append(f"fx_rate 端点绑定或报价列失败: {attempt}")
    finally:
        db.close()
    return {
        "attempts": len(rows), "rate_rows": total,
        "errors": errors, "ok": not errors,
    }


def _watch(root: Path, interval: float, *, verify_all_hashes: bool) -> int:
    """持续追赶封口 fx_rate segment；启动锁竞争只延后本轮。"""
    def report_connect_error(exc: Exception, elapsed: float) -> None:
        print(json.dumps({
            "event": "fx_rate_materialization_startup_error",
            "error": f"{type(exc).__name__}: {exc}",
            "elapsed_seconds": round(elapsed, 3),
            "retry_seconds": interval,
        }, ensure_ascii=False), flush=True)

    conn: sqlite3.Connection | None = None
    try:
        conn = connect_with_retry(
            root,
            retry_seconds=interval,
            connector=store.connect,
            report_error=report_connect_error,
        )
        cycle_index = 0
        while True:
            started = time.monotonic()
            try:
                # 定期全量复核，其余只看新段
                full_scan = (
                    verify_all_hashes
                    or cycle_index % FULL_SCAN_EVERY_CYCLES == 0
                )
                cycle_index += 1
                completed = _completed_input_paths(conn)
                inputs, stats = _scan_sealed_inputs(
                    root,
                    registered_hashes=(
                        None if verify_all_hashes
                        else _registered_input_hashes(conn)
                    ),
                    skip_paths=None if full_scan else completed,
                )
                pending = [
                    item for item in inputs
                    if item.artifact.storage_path not in completed
                ]
                cycle: list[FxRateResult] = []
                if pending:
                    # 扫描不持写锁，只锁写入
                    with sqlite_writer_lock(root):
                        cycle = _materialize_inputs(
                            root, conn, pending, report_reused=False,
                        )
                created = [item for item in cycle if not item.reused]
                print(json.dumps({
                    "event": "fx_rate_materialization_cycle",
                    "verify_all_hashes": verify_all_hashes,
                    "full_scan": full_scan,
                    "sealed_segments": len(inputs),
                    "skipped_completed": stats.skipped_completed,
                    "materialized_now": len(created),
                    "rate_rows_now": sum(item.rate_rows for item in created),
                    "scanned_manifests": stats.scanned_manifests,
                    "hash_recomputed": stats.hash_recomputed,
                    "hash_reused": stats.hash_reused,
                    "elapsed_scan_seconds": stats.elapsed_scan_seconds,
                    "elapsed_seconds": round(time.monotonic() - started, 3),
                }, ensure_ascii=False), flush=True)
            except (OSError, sqlite3.Error, ValueError, duckdb.Error) as exc:
                print(json.dumps({
                    "event": "fx_rate_materialization_error",
                    "error": f"{type(exc).__name__}: {exc}",
                }, ensure_ascii=False), flush=True)
            time.sleep(interval)
    except KeyboardInterrupt:
        print("fx_rate 物化已停止", flush=True)
        return 0
    finally:
        if conn is not None:
            conn.close()


def main(argv: Sequence[str] | None = None) -> int:
    """命令行入口。"""
    parser = argparse.ArgumentParser(
        description="GMO 外国為替FX fx_rate segment 物化",
        allow_abbrev=False,
    )
    parser.add_argument("--data-root", type=Path, default=None)
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("all", "watch"):
        command = sub.add_parser(name, allow_abbrev=False)
        command.add_argument(
            "--verify-all-hashes", action="store_true",
            help="关闭登记散列复用预筛，逐个重算 SHA-256 供审计",
        )
        if name == "watch":
            command.add_argument(
                "--interval-seconds", type=float, default=300.0,
            )
    sub.add_parser("audit", allow_abbrev=False)
    args = parser.parse_args(argv)
    root = (args.data_root or configured_data_root()).resolve()
    if args.command == "watch":
        interval = float(args.interval_seconds)
        if interval < MIN_WATCH_INTERVAL_SECONDS:
            raise ValueError(
                f"interval-seconds 不得小于 {MIN_WATCH_INTERVAL_SECONDS:g}"
            )
        return _watch(
            root, interval, verify_all_hashes=bool(args.verify_all_hashes),
        )

    conn = store.connect(root)
    try:
        if args.command == "all":
            with sqlite_writer_lock(root):
                completed = materialize_all(
                    root, conn,
                    verify_all_hashes=bool(args.verify_all_hashes),
                )
            result: object = [asdict(item) for item in completed]
            code = 0
        elif args.command == "audit":
            result = audit_fx_rates(root, conn)
            code = 0 if bool(result["ok"]) else 1
        else:
            raise AssertionError(f"未知命令: {args.command}")
    finally:
        conn.close()
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
