"""封口实时逐笔 segment 到 ``trade_observation`` Parquet。"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sqlite3
import time
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb

from guvolu.data import store
from guvolu.data.durable_io import atomic_write_text
from guvolu.data.materialize import (
    DATASET_TRADE,
    SourceArtifact,
    _input_set_hash,
    _market_row,
    _register_content_artifact,
    _relative_storage_path,
    artifact_id,
    ensure_markets,
    sha256_file,
    utc_now,
)
from guvolu.data.normalization import (
    NormalizationContext,
    NormalizationError,
    NormalizedTrade,
    normalize_trade,
)
from guvolu.data.paths import data_root as configured_data_root
from guvolu.data.realtime_control import (
    RealtimeChannelObservation,
    register_materialized_raw_v3_observations,
)
from guvolu.data.sqlite_writer_lock import sqlite_writer_lock
from guvolu.data.watch_connection import connect_with_retry
from guvolu.venues import registry

TRADE_REALTIME_SCHEMA_VERSION = 3
TRADE_REALTIME_NORMALIZATION_VERSION = "trade-realtime-normalization-v3"
LEGACY_TRADE_REALTIME_NORMALIZATION_VERSION = "trade-realtime-normalization-v2"
SUPPORTED_RAW_SCHEMA_VERSIONS = frozenset({1, 2, 3})
FAILED_RETRY_SECONDS = 3600
_ENDPOINT = {
    "gmo": "trades/ws",
    "bitbank": "transactions",
    "bitflyer": "lightning_executions",
}
_ENDPOINT_BINDINGS = {
    "gmo": ("EP-0007", 0),
    "bitbank": ("EP-0075", 0),
    "bitflyer": ("EP-0002", 0),
}
_TIMESTAMP_UNIT = {
    "gmo": "iso8601",
    "bitbank": "milliseconds",
    "bitflyer": "iso8601",
}


@dataclass(frozen=True)
class SegmentInput:
    """经散列验证的逐笔 segment。"""

    manifest_path: Path
    run_id: str
    segment_sequence: int
    raw_schema_version: int
    endpoint_id: str | None
    endpoint_revision: int | None
    artifact: SourceArtifact

    @property
    def partition_key(self) -> str:
        return f"{self.run_id}/segment-{self.segment_sequence:06d}"


@dataclass(frozen=True)
class RealtimeTradeResult:
    """一个逐笔 segment 的物化结果。"""

    attempt_id: str
    market_id: str
    partition_key: str
    status: str
    source_rows: int
    data_frames: int
    trade_rows: int
    ignored_rows: int
    rejected_rows: int
    output_path: str
    reused: bool


class MaterializationRetryDeferred(ValueError):
    """同一输入与配置刚失败过；本轮不重复追加失败 attempt。"""


@dataclass(frozen=True)
class _RawFrameMetadata:
    """不得从成交 payload 猜测的 wire envelope 身份。"""

    raw_schema_version: int
    ingest_time: str
    endpoint_id: str | None
    endpoint_revision: int | None
    connection_id: str | None
    channel_id: str | None
    recv_ts_mono_ns: int | None
    raw_payload_sha256: str
    quality_flags: tuple[str, ...]
    record_sequence: int | None


def _iso(value: object) -> str:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("逐笔时刻缺少时区")
    return parsed.astimezone(UTC).isoformat()


def _event_time(venue_id: str, payload: Mapping[str, object]) -> str:
    if venue_id == "gmo":
        return _iso(payload["timestamp"])
    if venue_id == "bitbank":
        return datetime.fromtimestamp(
            int(str(payload["executed_at"])) / 1000, UTC
        ).isoformat()
    return _iso(payload["exec_date"])


def _parse_gmo(payload_raw: str) -> list[Mapping[str, object]] | None:
    payload = json.loads(payload_raw, parse_float=Decimal)
    if not isinstance(payload, Mapping) or payload.get("channel") != "trades":
        return None
    return [payload]


def _parse_bitbank(payload_raw: str) -> list[Mapping[str, object]] | None:
    if not payload_raw.startswith("42"):
        return None
    packet = json.loads(payload_raw[2:], parse_float=Decimal)
    if not isinstance(packet, list) or len(packet) < 2:
        raise ValueError("bitbank 逐笔 Socket.IO 帧非法")
    envelope = packet[1]
    if not isinstance(envelope, Mapping):
        raise ValueError("bitbank 逐笔 envelope 非对象")
    if not str(envelope.get("room_name", "")).startswith("transactions_"):
        return None
    message = envelope.get("message")
    data = message.get("data") if isinstance(message, Mapping) else None
    transactions = data.get("transactions") if isinstance(data, Mapping) else None
    if not isinstance(transactions, list):
        raise ValueError("bitbank transactions 缺失")
    if not all(isinstance(item, Mapping) for item in transactions):
        raise ValueError("bitbank transaction 非对象")
    return transactions


def _parse_bitflyer(payload_raw: str) -> list[Mapping[str, object]] | None:
    payload = json.loads(payload_raw, parse_float=Decimal)
    if not isinstance(payload, Mapping) or payload.get("method") != "channelMessage":
        return None
    params = payload.get("params")
    if not isinstance(params, Mapping):
        raise ValueError("bitFlyer channelMessage.params 缺失")
    if not str(params.get("channel", "")).startswith("lightning_executions_"):
        return None
    message = params.get("message")
    if not isinstance(message, list):
        raise ValueError("bitFlyer executions 非数组")
    if not all(isinstance(item, Mapping) for item in message):
        raise ValueError("bitFlyer execution 非对象")
    return message


_PARSERS = {
    "gmo": _parse_gmo,
    "bitbank": _parse_bitbank,
    "bitflyer": _parse_bitflyer,
}


def _sealed_inputs(root: Path) -> list[SegmentInput]:
    inputs: list[SegmentInput] = []
    base = root / "raw" / "realtime" / "trade_realtime"
    if not base.is_dir():
        return []
    for manifest_path in sorted(base.rglob("segment-*.manifest.json")):
        body = json.loads(manifest_path.read_text(encoding="utf-8"))
        if not isinstance(body, Mapping):
            continue
        if body.get("status") != "sealed" or body.get("completion_claim") is not True:
            continue
        recorded = str(body["storage_path"])
        path = (root / recorded).resolve()
        try:
            path.relative_to(root.resolve())
        except ValueError as exc:
            raise ValueError(f"逐笔 segment 路径越界: {recorded}") from exc
        expected_path = manifest_path.with_name(
            manifest_path.name.removesuffix(".manifest.json") + ".jsonl"
        ).resolve()
        if path != expected_path:
            raise ValueError(f"逐笔 manifest 与 segment 不同目录: {recorded}")
        sha = sha256_file(path)
        if sha != str(body["sha256"]):
            raise ValueError(f"逐笔 segment 散列不符: {recorded}")
        if path.stat().st_size != int(str(body["byte_count"])):
            raise ValueError(f"逐笔 segment 字节数不符: {recorded}")
        if body.get("artifact_id") not in {None, artifact_id(sha)}:
            raise ValueError(f"逐笔 segment artifact_id 不符: {recorded}")
        raw_schema_version = int(str(body.get("schema_version", 1)))
        if raw_schema_version not in SUPPORTED_RAW_SCHEMA_VERSIONS:
            raise ValueError(
                f"逐笔 segment schema_version 尚不支持: "
                f"{recorded}: {raw_schema_version}"
            )
        venue_id = next(
            (
                part.split("=", 1)[1]
                for part in path.parts
                if part.startswith("venue_id=")
            ),
            "",
        )
        binding = _ENDPOINT_BINDINGS.get(venue_id)
        if binding is None:
            raise ValueError(f"逐笔 segment 场所不受支持: {recorded}")
        endpoint_id = (
            str(body["endpoint_id"])
            if body.get("endpoint_id") is not None else None
        )
        endpoint_revision_raw = body.get("endpoint_revision")
        if raw_schema_version == 3 and (
            isinstance(endpoint_revision_raw, bool)
            or not isinstance(endpoint_revision_raw, int)
        ):
            raise ValueError(f"raw v3 manifest endpoint_revision 非整数: {recorded}")
        endpoint_revision = (
            endpoint_revision_raw
            if isinstance(endpoint_revision_raw, int)
            and not isinstance(endpoint_revision_raw, bool)
            else None
        )
        if raw_schema_version == 1:
            if endpoint_id is not None or endpoint_revision is not None:
                raise ValueError(f"raw v1 不得声称端点身份: {recorded}")
        elif raw_schema_version == 2:
            if endpoint_id != binding[0] or endpoint_revision is not None:
                raise ValueError(f"raw v2 端点身份或修订非法: {recorded}")
        elif (
            endpoint_id != binding[0]
            or endpoint_revision != binding[1]
            or body.get("artifact_id") != artifact_id(sha)
        ):
            raise ValueError(f"raw v3 manifest 端点绑定非法: {recorded}")
        inputs.append(SegmentInput(
            manifest_path=manifest_path,
            run_id=str(body["run_id"]),
            segment_sequence=int(str(body["segment_sequence"])),
            raw_schema_version=raw_schema_version,
            endpoint_id=endpoint_id,
            endpoint_revision=endpoint_revision,
            artifact=SourceArtifact(
                artifact_id=artifact_id(sha), storage_path=recorded,
                absolute_path=path, source_rows=int(str(body["record_count"])),
                normalized_rows=0, rejected_rows=0,
            ),
        ))
    return inputs


def _bind_capability(
    conn: sqlite3.Connection, attempt_id: str, venue_id: str,
) -> int:
    endpoint = _ENDPOINT[venue_id]
    row = conn.execute(
        "SELECT revision_id FROM venue_capability_revision "
        "WHERE venue_id=? AND domain='trade_realtime' AND endpoint=? "
        "AND available=1 AND implementation_status='implemented' "
        "ORDER BY revision_id DESC LIMIT 1",
        (venue_id, endpoint),
    ).fetchone()
    if row is None:
        raise ValueError(f"实时逐笔能力未登记: {venue_id}/{endpoint}")
    revision = int(row[0])
    conn.execute(
        "INSERT INTO partition_capability_binding "
        "(attempt_id,venue_id,domain,endpoint,revision_id,binding_basis,bound_at) "
        "VALUES (?,?,'trade_realtime',?,?,'recorded',?)",
        (attempt_id, venue_id, endpoint, revision, utc_now()),
    )
    return revision


def _register_source(conn: sqlite3.Connection, item: SegmentInput) -> None:
    created = datetime.fromtimestamp(
        item.artifact.absolute_path.stat().st_mtime, UTC
    ).isoformat()
    # 旧投影将 raw v1 登记为 v2。
    # 内容身份不可改写。
    _register_content_artifact(
        conn, item.artifact.artifact_id, "raw_realtime_segment",
        item.artifact.storage_path,
        item.artifact.artifact_id.removeprefix("sha256-"),
        item.artifact.absolute_path.stat().st_size, created,
        max(2, item.raw_schema_version),
    )


def _expected_channel(venue_id: str, payload_raw: str) -> str:
    """按采集端同一规则复算 wire 帧的原生频道。"""
    if venue_id == "bitbank":
        if not payload_raw.startswith("42"):
            return "protocol_control"
        try:
            packet = json.loads(payload_raw[2:])
        except json.JSONDecodeError:
            return "protocol_control"
        if not isinstance(packet, list) or len(packet) < 2:
            return "protocol_control"
        envelope = packet[1]
        if not isinstance(envelope, Mapping):
            return "protocol_control"
        room = envelope.get("room_name")
        return room if isinstance(room, str) and room else "protocol_control"
    try:
        payload = json.loads(payload_raw)
    except json.JSONDecodeError:
        return "trades" if venue_id == "gmo" else "protocol_control"
    if not isinstance(payload, Mapping):
        return "trades" if venue_id == "gmo" else "protocol_control"
    if venue_id == "gmo":
        channel = payload.get("channel")
        return channel if isinstance(channel, str) and channel else "trades"
    params = payload.get("params")
    if not isinstance(params, Mapping):
        return "protocol_control"
    channel = params.get("channel")
    return channel if isinstance(channel, str) and channel else "protocol_control"


def _raw_metadata(
    envelope: Mapping[str, object], item: SegmentInput, venue_id: str,
) -> tuple[str, _RawFrameMetadata]:
    """验证 raw manifest、行与 payload 三层来源身份。"""
    payload = envelope.get("payload_raw")
    if not isinstance(payload, str):
        raise ValueError("segment 行 payload_raw 不是字符串")
    raw_schema = int(str(envelope.get("schema_version", 1)))
    if raw_schema != item.raw_schema_version:
        raise ValueError("segment 行 schema_version 与 manifest 不一致")
    endpoint, endpoint_revision = _ENDPOINT_BINDINGS[venue_id]
    if envelope.get("source_endpoint") != _ENDPOINT[venue_id]:
        raise ValueError("segment 行 source_endpoint 与端点契约不一致")
    ingest = _iso(envelope["ingest_time"])
    computed_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    if raw_schema == 1:
        if (
            envelope.get("endpoint_id") is not None
            or envelope.get("endpoint_revision") is not None
        ):
            raise ValueError("raw v1 不得声称端点身份")
        return payload, _RawFrameMetadata(
            raw_schema_version=raw_schema,
            ingest_time=ingest,
            endpoint_id=None,
            endpoint_revision=None,
            connection_id=None,
            channel_id=None,
            recv_ts_mono_ns=None,
            raw_payload_sha256=computed_hash,
            quality_flags=(
                "channel_identity_unrecorded",
                "connection_boundary_unknown",
                "endpoint_identity_unrecorded",
                "endpoint_revision_unrecorded",
                "raw_payload_hash_derived",
                "recv_ts_mono_missing",
            ),
            record_sequence=None,
        )
    if raw_schema not in {2, 3}:
        raise ValueError(f"raw schema_version 尚不支持: {raw_schema}")
    row_endpoint = envelope.get("endpoint_id")
    if row_endpoint != endpoint or row_endpoint != item.endpoint_id:
        raise ValueError("raw endpoint_id 与行/manifest/端点契约不一致")
    row_revision_raw = envelope.get("endpoint_revision")
    if raw_schema == 2:
        if row_revision_raw is not None or item.endpoint_revision is not None:
            raise ValueError("raw v2 不得补造 endpoint_revision")
        row_revision = None
    else:
        if (
            isinstance(row_revision_raw, bool)
            or not isinstance(row_revision_raw, int)
        ):
            raise ValueError("raw v3 endpoint_revision 非法")
        row_revision = row_revision_raw
        if (
            row_revision != endpoint_revision
            or row_revision != item.endpoint_revision
        ):
            raise ValueError("raw v3 endpoint_revision 与端点契约不一致")
    connection_id = envelope.get("connection_id")
    if (
        not isinstance(connection_id, str)
        or not connection_id.startswith(f"{item.run_id}-c")
    ):
        raise ValueError("raw connection_id 不属于当前 source_session")
    channel_id = envelope.get("channel_id")
    if (
        not isinstance(channel_id, str)
        or channel_id != _expected_channel(venue_id, payload)
    ):
        raise ValueError("raw channel_id 与 wire 帧不一致")
    recv_utc = _iso(envelope["recv_ts_utc"])
    if recv_utc != ingest:
        raise ValueError("raw recv_ts_utc 与 ingest_time 不一致")
    mono = envelope.get("recv_ts_mono_ns")
    if (
        isinstance(mono, bool)
        or not isinstance(mono, int)
        or mono < 0
        or mono > 2**64 - 1
    ):
        raise ValueError("raw recv_ts_mono_ns 非法")
    if envelope.get("raw_payload_sha256") != computed_hash:
        raise ValueError("raw payload SHA-256 校验失败")
    record_sequence_raw = envelope.get("record_sequence")
    record_sequence: int | None = None
    if raw_schema == 3:
        if (
            isinstance(record_sequence_raw, bool)
            or not isinstance(record_sequence_raw, int)
        ):
            raise ValueError("raw v3 record_sequence 非法")
        record_sequence = record_sequence_raw
        if record_sequence <= 0:
            raise ValueError("raw v3 record_sequence 必须为正数")
    flags = ["raw_payload_hash_verified", "receive_clock_verified"]
    if raw_schema == 2:
        flags.append("endpoint_revision_unrecorded")
    else:
        flags.extend((
            "connection_channel_identity_verified",
            "endpoint_binding_verified",
        ))
    return payload, _RawFrameMetadata(
        raw_schema_version=raw_schema,
        ingest_time=recv_utc,
        endpoint_id=str(row_endpoint),
        endpoint_revision=row_revision,
        connection_id=connection_id,
        channel_id=channel_id,
        recv_ts_mono_ns=mono,
        raw_payload_sha256=computed_hash,
        quality_flags=tuple(sorted(flags)),
        record_sequence=record_sequence,
    )


def _source_scoped_gmo_id(
    trade: NormalizedTrade, artifact: str, source_row: int, item_index: int,
) -> NormalizedTrade:
    body = "|".join((
        trade.venue_id, trade.instrument_id, trade.event_time,
        trade.price, trade.size, trade.side, artifact,
        str(source_row), str(item_index),
    ))
    return replace(
        trade,
        venue_trade_id="sha256-" + hashlib.sha256(body.encode()).hexdigest(),
        id_origin="synthetic_source_scoped",
    )


def _stage(
    item: SegmentInput, venue_id: str, venue_symbol: str,
    market_id: str, mapping_revision: int, capability_revision: int,
    instrument_id: str, staging: Path,
) -> tuple[
    int, int, list[tuple[int, str]], list[tuple[int, str]], str, str,
    tuple[RealtimeChannelObservation, ...],
]:
    parser = _PARSERS[venue_id]
    data_frames = trade_rows = 0
    ignored: list[tuple[int, str]] = []
    rejected: list[tuple[int, str]] = []
    min_event = ""
    max_event = ""
    observations: list[RealtimeChannelObservation] = []
    previous_record_sequence: int | None = None
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
                    envelope.get("venue_id") != venue_id
                    or envelope.get("venue_symbol") != venue_symbol
                    or envelope.get("run_id") != item.run_id
                    or int(str(envelope.get("segment_sequence")))
                    != item.segment_sequence
                    or envelope.get("domain") != "trade_realtime"
                ):
                    raise ValueError("逐笔 segment 身份不符")
                payload_raw, raw = _raw_metadata(envelope, item, venue_id)
                if raw.record_sequence is not None:
                    if (
                        previous_record_sequence is not None
                        and raw.record_sequence <= previous_record_sequence
                    ):
                        raise ValueError("raw v3 record_sequence 未严格递增")
                    previous_record_sequence = raw.record_sequence
                payloads = parser(payload_raw)
                if payloads is None:
                    ignored.append((source_row, "protocol_control_frame"))
                    continue
                staged: list[list[object]] = []
                staged_events: list[str] = []
                for item_index, payload in enumerate(payloads):
                    event = _event_time(venue_id, payload)
                    context = NormalizationContext(
                        venue_id=venue_id, instrument_id=instrument_id,
                        endpoint=_ENDPOINT[venue_id],
                        ingest_time=raw.ingest_time,
                        raw_source=f"{item.artifact.storage_path}:{source_row}",
                        raw_item_index=source_row * 1_000_000 + item_index,
                        timestamp_unit=_TIMESTAMP_UNIT[venue_id],
                        available_time=max(event, raw.ingest_time),
                    )
                    trade = normalize_trade(payload, context)
                    if venue_id == "gmo":
                        trade = _source_scoped_gmo_id(
                            trade, item.artifact.artifact_id,
                            source_row, item_index,
                        )
                    trade = replace(
                        trade,
                        normalization_version=TRADE_REALTIME_NORMALIZATION_VERSION,
                        schema_version=TRADE_REALTIME_SCHEMA_VERSION,
                    )
                    observation = (
                        f"{venue_id}|{market_id}|{trade.venue_trade_id}"
                        f"|r{trade.revision_id}"
                    )
                    staged.append([
                        observation, venue_id, venue_symbol, market_id,
                        mapping_revision, capability_revision, instrument_id,
                        trade.venue_trade_id, trade.revision_id,
                        trade.event_time, trade.available_time, trade.ingest_time,
                        trade.side, trade.source_side_basis, trade.price,
                        trade.size, trade.match_granularity, trade.id_origin,
                        trade.sequence_id, trade.first_trade_id,
                        trade.last_trade_id, trade.time_origin,
                        _ENDPOINT[venue_id], raw.endpoint_id,
                        raw.endpoint_revision, raw.connection_id,
                        raw.channel_id, raw.recv_ts_mono_ns,
                        raw.raw_payload_sha256,
                        json.dumps(
                            sorted(set(raw.quality_flags)), separators=(",", ":")
                        ),
                        raw.raw_schema_version, item.run_id,
                        item.segment_sequence, item.artifact.artifact_id,
                        source_row, item_index,
                        TRADE_REALTIME_NORMALIZATION_VERSION,
                        TRADE_REALTIME_SCHEMA_VERSION,
                    ])
                    staged_events.append(event)
                writer.writerows(
                    ["\\N" if value is None else value for value in row]
                    for row in staged
                )
                if item.raw_schema_version == 3 and staged:
                    assert raw.connection_id is not None
                    assert raw.channel_id is not None
                    observations.append(RealtimeChannelObservation(
                        raw.connection_id, raw.channel_id, raw.ingest_time,
                    ))
                data_frames += 1
                trade_rows += len(staged)
                for event in staged_events:
                    min_event = event if not min_event else min(min_event, event)
                    max_event = event if not max_event else max(max_event, event)
            except (
                KeyError, TypeError, ValueError, NormalizationError,
                json.JSONDecodeError,
            ) as exc:
                rejected.append((source_row, f"{type(exc).__name__}: {exc}"))
        target.flush()
        os.fsync(target.fileno())
    if item.artifact.source_rows != data_frames + len(ignored) + len(rejected):
        raise ValueError("逐笔来源帧分类不守恒")
    return (
        data_frames, trade_rows, ignored, rejected, min_event, max_event,
        tuple(observations),
    )


def _create_table(db: Any) -> None:
    db.execute("""
        CREATE TABLE trade_observation (
          observation_id VARCHAR, venue_id VARCHAR, venue_symbol VARCHAR,
          market_id VARCHAR, mapping_revision INTEGER,
          capability_revision INTEGER, instrument_id VARCHAR,
          venue_trade_id VARCHAR, revision_id INTEGER,
          event_time TIMESTAMPTZ, available_time TIMESTAMPTZ,
          ingest_time TIMESTAMPTZ, side VARCHAR, source_side_basis VARCHAR,
          price VARCHAR, size VARCHAR, match_granularity VARCHAR,
          id_origin VARCHAR, sequence_id VARCHAR, first_trade_id VARCHAR,
          last_trade_id VARCHAR, time_origin VARCHAR, source_endpoint VARCHAR,
          endpoint_id VARCHAR, endpoint_revision INTEGER,
          connection_id VARCHAR, channel_id VARCHAR,
          recv_ts_mono_ns UBIGINT, raw_payload_sha256 VARCHAR,
          data_quality VARCHAR, raw_schema_version INTEGER,
          run_id VARCHAR, segment_sequence INTEGER,
          source_artifact_id VARCHAR, source_row_index BIGINT,
          source_item_index INTEGER, normalization_version VARCHAR,
          schema_version INTEGER
        )
    """)


def _copy_csv(db: Any, path: Path) -> None:
    escaped = path.as_posix().replace("'", "''")
    db.execute(
        f"COPY trade_observation FROM '{escaped}' "
        "(FORMAT CSV, HEADER false, NULL '\\N')"
    )


def _write_parquet(db: Any, path: Path) -> None:
    escaped = path.as_posix().replace("'", "''")
    db.execute(
        "COPY (SELECT * FROM trade_observation "
        "ORDER BY event_time,venue_trade_id,source_row_index,source_item_index) "
        f"TO '{escaped}' (FORMAT PARQUET, COMPRESSION ZSTD, "
        "ROW_GROUP_SIZE 122880)"
    )
    with path.open("rb+") as handle:
        handle.flush()
        os.fsync(handle.fileno())


def _finalize(temp: Path) -> tuple[Path, str]:
    sha = sha256_file(temp)
    final = temp.with_name(f"part-{sha[:12]}.parquet")
    if final.exists():
        if sha256_file(final) != sha:
            raise ValueError(f"逐笔输出散列冲突: {final}")
        temp.unlink()
    else:
        os.replace(temp, final)
    return final, sha


def materialize_segment(
    root: Path, conn: sqlite3.Connection, item: SegmentInput,
) -> RealtimeTradeResult:
    parts = item.artifact.absolute_path.parts
    venue_id = next(
        part.split("=", 1)[1] for part in parts if part.startswith("venue_id=")
    )
    venue_symbol = next(
        part.split("=", 1)[1]
        for part in parts if part.startswith("venue_symbol=")
    )
    registry.register_all(conn)
    ensure_markets(conn)
    market_id, instrument_id, mapping_revision = _market_row(
        conn, venue_id, venue_symbol, None
    )
    _register_source(conn, item)
    conn.commit()
    input_hash = _input_set_hash([item.artifact])
    config_hash = hashlib.sha256(json.dumps({
        "dataset": DATASET_TRADE,
        "normalization_version": TRADE_REALTIME_NORMALIZATION_VERSION,
        "schema_version": TRADE_REALTIME_SCHEMA_VERSION,
        "frame_atomicity": "reject-whole-source-row-v2",
        "supported_raw_schema_versions": sorted(SUPPORTED_RAW_SCHEMA_VERSIONS),
    }, sort_keys=True).encode()).hexdigest()
    existing = conn.execute(
        "SELECT a.attempt_id,a.status,a.source_rows,a.normalized_rows,"
        "a.ignored_rows,a.rejected_rows,r.storage_path "
        "FROM partition_attempt a JOIN materialization_output o "
        "ON o.attempt_id=a.attempt_id JOIN artifact r "
        "ON r.artifact_id=o.artifact_id "
        "WHERE a.market_id=? AND a.domain='trade_realtime' "
        "AND a.partition_key=? AND a.normalization_version=? "
        "AND a.input_set_hash=? AND a.status IN "
        "('complete','complete_with_rejections') LIMIT 1",
        (
            market_id, item.partition_key,
            TRADE_REALTIME_NORMALIZATION_VERSION, input_hash,
        ),
    ).fetchone()
    if existing is not None:
        return RealtimeTradeResult(
            str(existing[0]), market_id, item.partition_key, str(existing[1]),
            int(existing[2]), 0, int(existing[3]), int(existing[4]),
            int(existing[5]), str(existing[6]), True,
        )
    recent_failure = conn.execute(
        "SELECT attempt_id,finished_at,failure_detail FROM partition_attempt "
        "WHERE market_id=? AND domain='trade_realtime' AND partition_key=? "
        "AND normalization_version=? AND input_set_hash=? AND config_hash=? "
        "AND status='failed' ORDER BY finished_at DESC LIMIT 1",
        (
            market_id, item.partition_key,
            TRADE_REALTIME_NORMALIZATION_VERSION, input_hash, config_hash,
        ),
    ).fetchone()
    if recent_failure is not None and recent_failure[1]:
        failed_at = datetime.fromisoformat(str(recent_failure[1]))
        if failed_at.tzinfo is None:
            failed_at = failed_at.replace(tzinfo=UTC)
        age = (datetime.now(UTC) - failed_at.astimezone(UTC)).total_seconds()
        if age < FAILED_RETRY_SECONDS:
            raise MaterializationRetryDeferred(
                f"{item.partition_key} 延迟重试；最近 attempt={recent_failure[0]}: "
                f"{recent_failure[2]}"
            )
    attempt_id = f"trade-rt-{uuid.uuid4().hex}"
    conn.execute(
        "INSERT INTO partition_attempt "
        "(attempt_id,market_id,domain,partition_key,normalization_version,"
        "input_set_hash,status,source_rows,normalized_rows,ignored_rows,"
        "rejected_rows,started_at,code_version,config_hash) "
        "VALUES (?,?,?,?,?,?,'running',?,0,0,0,?,'working-tree',?)",
        (
            attempt_id, market_id, "trade_realtime", item.partition_key,
            TRADE_REALTIME_NORMALIZATION_VERSION, input_hash,
            item.artifact.source_rows, utc_now(), config_hash,
        ),
    )
    capability_revision = _bind_capability(conn, attempt_id, venue_id)
    conn.commit()
    output_dir = (
        root / "materialized" / DATASET_TRADE
        / f"schema_version={TRADE_REALTIME_SCHEMA_VERSION}"
        / f"normalization_version={TRADE_REALTIME_NORMALIZATION_VERSION}"
        / f"venue_id={venue_id}" / f"market_id={market_id}"
        / f"run_id={item.run_id}"
        / f"segment={item.segment_sequence:06d}"
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    staging = output_dir / f".{attempt_id}.csv"
    temporary = output_dir / f".{attempt_id}.parquet"
    try:
        (
            data_frames, trades, ignored, rejected, min_event, max_event,
            observations,
        ) = _stage(
            item, venue_id, venue_symbol, market_id, mapping_revision,
            capability_revision, instrument_id, staging,
        )
        db: Any = duckdb.connect(":memory:")
        db.execute("SET TimeZone='UTC'")
        try:
            _create_table(db)
            if trades:
                _copy_csv(db, staging)
            check = db.execute(
                "SELECT COUNT(*),COUNT(*)-COUNT(DISTINCT observation_id),"
                "SUM(available_time<event_time),"
                "COUNT(DISTINCT market_id),MIN(market_id),"
                "COUNT(DISTINCT source_artifact_id),MIN(source_artifact_id),"
                "COUNT(DISTINCT normalization_version),"
                "MIN(normalization_version) FROM trade_observation"
            ).fetchone()
            if check is None or int(check[0]) != trades:
                raise ValueError("实时逐笔输出计数不符")
            if int(check[1] or 0) or int(check[2] or 0):
                raise ValueError("实时逐笔键或 PIT 契约不符")
            if trades and (
                int(check[3]) != 1 or str(check[4]) != market_id
                or int(check[5]) != 1
                or str(check[6]) != item.artifact.artifact_id
                or int(check[7]) != 1
                or str(check[8]) != TRADE_REALTIME_NORMALIZATION_VERSION
            ):
                raise ValueError("实时逐笔来源或版本契约不符")
            source_check = db.execute(
                "SELECT COUNT(DISTINCT raw_schema_version),"
                "MIN(raw_schema_version),"
                "SUM(CASE WHEN raw_payload_sha256 IS NULL OR "
                "length(raw_payload_sha256)!=64 OR data_quality IS NULL "
                "THEN 1 ELSE 0 END),"
                "SUM(CASE WHEN raw_schema_version=1 AND "
                "(endpoint_id IS NOT NULL OR endpoint_revision IS NOT NULL "
                "OR connection_id IS NOT NULL OR channel_id IS NOT NULL "
                "OR recv_ts_mono_ns IS NOT NULL OR "
                "NOT contains(data_quality,'raw_payload_hash_derived')) "
                "THEN 1 ELSE 0 END),"
                "SUM(CASE WHEN raw_schema_version=2 AND "
                "(endpoint_id IS NULL OR endpoint_revision IS NOT NULL "
                "OR connection_id IS NULL OR channel_id IS NULL "
                "OR recv_ts_mono_ns IS NULL OR "
                "NOT contains(data_quality,'endpoint_revision_unrecorded')) "
                "THEN 1 ELSE 0 END),"
                "SUM(CASE WHEN raw_schema_version=3 AND "
                "(endpoint_id IS NULL OR endpoint_revision IS NULL "
                "OR connection_id IS NULL OR channel_id IS NULL "
                "OR recv_ts_mono_ns IS NULL OR "
                "NOT contains(data_quality,'endpoint_binding_verified')) "
                "THEN 1 ELSE 0 END),"
                "COUNT(DISTINCT endpoint_id),MIN(endpoint_id),"
                "COUNT(DISTINCT endpoint_revision),MIN(endpoint_revision),"
                "COUNT(DISTINCT source_endpoint),MIN(source_endpoint) "
                "FROM trade_observation"
            ).fetchone()
            if trades and (
                source_check is None
                or int(source_check[0]) != 1
                or int(source_check[1]) != item.raw_schema_version
                or any(int(source_check[index] or 0) for index in (2, 3, 4, 5))
                or int(source_check[10]) != 1
                or str(source_check[11]) != _ENDPOINT[venue_id]
            ):
                raise ValueError("实时逐笔 raw 来源保真契约不符")
            if trades and item.raw_schema_version == 1 and (
                int(source_check[6]) != 0 or int(source_check[8]) != 0
            ):
                raise ValueError("raw v1 事实补造了端点身份")
            if trades and item.raw_schema_version == 2 and (
                int(source_check[6]) != 1
                or str(source_check[7]) != item.endpoint_id
                or int(source_check[8]) != 0
            ):
                raise ValueError("raw v2 事实端点身份不符")
            if trades and item.raw_schema_version == 3 and (
                int(source_check[6]) != 1
                or str(source_check[7]) != item.endpoint_id
                or int(source_check[8]) != 1
                or int(source_check[9]) != item.endpoint_revision
            ):
                raise ValueError("raw v3 事实端点绑定不符")
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
            "normalization_version": TRADE_REALTIME_NORMALIZATION_VERSION,
            "schema_version": TRADE_REALTIME_SCHEMA_VERSION,
            "input_schema_version": item.raw_schema_version,
            "endpoint_id": item.endpoint_id,
            "endpoint_revision": item.endpoint_revision,
            "input_artifact_id": item.artifact.artifact_id,
            "source_rows": item.artifact.source_rows,
            "data_frames": data_frames, "trade_rows": trades,
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
            output_path.stat().st_size, finished,
            TRADE_REALTIME_SCHEMA_VERSION,
        )
        conn.execute(
            "INSERT INTO materialization_output VALUES (?,?,?,?,?,?,?)",
            (
                attempt_id, output_id, DATASET_TRADE, trades,
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
                item.artifact.source_rows, trades, len(ignored), len(rejected),
            ),
        )
        conn.execute(
            "INSERT INTO partition_input_binding "
            "(attempt_id,artifact_id,storage_path,source_rows,normalized_rows,"
            "ignored_rows,rejected_rows) VALUES (?,?,?,?,?,?,?)",
            (
                attempt_id, item.artifact.artifact_id,
                item.artifact.storage_path, item.artifact.source_rows,
                trades, len(ignored), len(rejected),
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
            (status, trades, len(ignored), len(rejected), finished, attempt_id),
        )
        conn.execute(
            "INSERT INTO materialization_partition_head VALUES (?,?,?,?,?,?) "
            "ON CONFLICT(market_id,domain,partition_key) DO UPDATE SET "
            "normalization_version=excluded.normalization_version,"
            "attempt_id=excluded.attempt_id,activated_at=excluded.activated_at",
            (
                market_id, "trade_realtime", item.partition_key,
                TRADE_REALTIME_NORMALIZATION_VERSION, attempt_id, finished,
            ),
        )
        if item.raw_schema_version == 3:
            assert item.endpoint_id is not None
            assert item.endpoint_revision is not None
            register_materialized_raw_v3_observations(
                conn,
                endpoint_id=item.endpoint_id,
                endpoint_revision=item.endpoint_revision,
                run_id=item.run_id,
                market_id=market_id,
                capability_venue_id=venue_id,
                capability_domain="trade_realtime",
                capability_endpoint=_ENDPOINT[venue_id],
                capability_revision=capability_revision,
                observations=observations,
            )
        conn.commit()
        return RealtimeTradeResult(
            attempt_id, market_id, item.partition_key, status,
            item.artifact.source_rows, data_frames, trades,
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
) -> list[RealtimeTradeResult]:
    """断点复用地物化全部封口逐笔 segment。"""
    inputs = _sealed_inputs(root)
    results: list[RealtimeTradeResult] = []
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
                f"frames={result.data_frames:,} trades={result.trade_rows:,} "
                f"ignored={result.ignored_rows} rejected={result.rejected_rows}",
                flush=True,
            )
    return results


def audit_realtime_trades(
    root: Path, conn: sqlite3.Connection,
) -> dict[str, object]:
    """复核活动实时逐笔输出的键、PIT 与来源绑定。"""
    errors: list[str] = []
    rows = conn.execute(
        "SELECT a.attempt_id,a.market_id,a.normalization_version,"
        "a.normalized_rows,r.storage_path FROM materialization_partition_head h "
        "JOIN partition_attempt a ON a.attempt_id=h.attempt_id "
        "JOIN materialization_output o ON o.attempt_id=a.attempt_id "
        "JOIN artifact r ON r.artifact_id=o.artifact_id "
        "WHERE h.domain='trade_realtime' AND o.dataset=?",
        (DATASET_TRADE,),
    ).fetchall()
    total = 0
    db: Any = duckdb.connect(":memory:")
    db.execute("SET TimeZone='UTC'")
    try:
        for attempt, market, version, expected, path_text in rows:
            parquet_path = str(root / str(path_text))
            result = db.execute(
                "SELECT COUNT(*),COUNT(*)-COUNT(DISTINCT observation_id),"
                "SUM(available_time<event_time),COUNT(DISTINCT market_id),"
                "MIN(market_id),COUNT(DISTINCT source_artifact_id),"
                "COUNT(DISTINCT normalization_version),MIN(normalization_version) "
                "FROM read_parquet(?)",
                [parquet_path],
            ).fetchone()
            if result is None:
                errors.append(f"逐笔输出不可读: {attempt}")
                continue
            count = int(result[0])
            total += count
            if count != int(expected) or int(result[1] or 0) or int(result[2] or 0):
                errors.append(f"逐笔计数、键或 PIT 失败: {attempt}")
            if count and (
                int(result[3]) != 1 or str(result[4]) != str(market)
                or int(result[5]) != 1 or int(result[6]) != 1
                or str(result[7]) != str(version)
            ):
                errors.append(f"逐笔市场、原件或版本失败: {attempt}")
            if count and str(version) == TRADE_REALTIME_NORMALIZATION_VERSION:
                columns = {
                    str(row[0])
                    for row in db.execute(
                        "DESCRIBE SELECT * FROM read_parquet(?)",
                        [parquet_path],
                    ).fetchall()
                }
                required = {
                    "endpoint_id", "endpoint_revision", "connection_id",
                    "channel_id", "recv_ts_mono_ns", "raw_payload_sha256",
                    "data_quality", "raw_schema_version",
                }
                if not required.issubset(columns):
                    errors.append(f"逐笔 v3 来源列缺失: {attempt}")
                    continue
                source = db.execute(
                    "SELECT COUNT(DISTINCT schema_version),MIN(schema_version),"
                    "COUNT(DISTINCT raw_schema_version),"
                    "MIN(raw_schema_version),MIN(venue_id),"
                    "SUM(CASE WHEN raw_payload_sha256 IS NULL OR "
                    "length(raw_payload_sha256)!=64 OR data_quality IS NULL "
                    "THEN 1 ELSE 0 END),"
                    "SUM(CASE WHEN raw_schema_version=1 AND "
                    "(endpoint_id IS NOT NULL OR endpoint_revision IS NOT NULL "
                    "OR connection_id IS NOT NULL OR channel_id IS NOT NULL "
                    "OR recv_ts_mono_ns IS NOT NULL OR "
                    "NOT contains(data_quality,'raw_payload_hash_derived')) "
                    "THEN 1 ELSE 0 END),"
                    "SUM(CASE WHEN raw_schema_version=2 AND "
                    "(endpoint_id IS NULL OR endpoint_revision IS NOT NULL "
                    "OR connection_id IS NULL OR channel_id IS NULL "
                    "OR recv_ts_mono_ns IS NULL OR NOT contains(data_quality,"
                    "'endpoint_revision_unrecorded')) THEN 1 ELSE 0 END),"
                    "SUM(CASE WHEN raw_schema_version=3 AND "
                    "(endpoint_id IS NULL OR endpoint_revision IS NULL "
                    "OR connection_id IS NULL OR channel_id IS NULL "
                    "OR recv_ts_mono_ns IS NULL OR NOT contains(data_quality,"
                    "'endpoint_binding_verified')) THEN 1 ELSE 0 END),"
                    "COUNT(DISTINCT endpoint_id),MIN(endpoint_id),"
                    "COUNT(DISTINCT endpoint_revision),MIN(endpoint_revision) "
                    "FROM read_parquet(?)",
                    [parquet_path],
                ).fetchone()
                if source is None or (
                    int(source[0]) != 1
                    or int(source[1]) != TRADE_REALTIME_SCHEMA_VERSION
                    or int(source[2]) != 1
                    or int(source[5] or 0)
                    or any(int(source[index] or 0) for index in (6, 7, 8))
                ):
                    errors.append(f"逐笔 v3 来源保真失败: {attempt}")
                    continue
                raw_schema = int(source[3])
                venue_id = str(source[4])
                binding = _ENDPOINT_BINDINGS.get(venue_id)
                if raw_schema not in SUPPORTED_RAW_SCHEMA_VERSIONS:
                    errors.append(f"逐笔 v3 未知 raw schema: {attempt}")
                elif binding is None:
                    errors.append(f"逐笔 v3 未知端点: {attempt}")
                elif raw_schema == 1 and (
                    int(source[9]) != 0 or int(source[11]) != 0
                ):
                    errors.append(f"逐笔 v3 补造旧端点: {attempt}")
                elif raw_schema == 2 and (
                    int(source[9]) != 1 or str(source[10]) != binding[0]
                    or int(source[11]) != 0
                ):
                    errors.append(f"逐笔 v3 的 raw v2 绑定失败: {attempt}")
                elif raw_schema == 3 and (
                    int(source[9]) != 1 or str(source[10]) != binding[0]
                    or int(source[11]) != 1 or int(source[12]) != binding[1]
                ):
                    errors.append(f"逐笔 v3 的 raw v3 绑定失败: {attempt}")
    finally:
        db.close()
    return {
        "attempts": len(rows), "trade_rows": total,
        "errors": errors, "ok": not errors,
    }


def _watch(root: Path, interval: float) -> int:
    """持续追赶封口逐笔；启动锁竞争只延后本轮。"""
    def report_connect_error(exc: Exception, elapsed: float) -> None:
        print(json.dumps({
            "event": "trade_realtime_materialization_startup_error",
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
        while True:
            started = time.monotonic()
            try:
                with sqlite_writer_lock(root):
                    cycle = materialize_all(root, conn, report_reused=False)
                created = [item for item in cycle if not item.reused]
                print(json.dumps({
                    "event": "trade_realtime_materialization_cycle",
                    "sealed_segments": len(cycle),
                    "materialized_now": len(created),
                    "trade_rows_now": sum(
                        item.trade_rows for item in created
                    ),
                    "elapsed_seconds": round(
                        time.monotonic() - started, 3
                    ),
                }, ensure_ascii=False), flush=True)
            except (OSError, sqlite3.Error, ValueError, duckdb.Error) as exc:
                print(json.dumps({
                    "event": "trade_realtime_materialization_error",
                    "error": f"{type(exc).__name__}: {exc}",
                }, ensure_ascii=False), flush=True)
            time.sleep(interval)
    except KeyboardInterrupt:
        print("实时逐笔物化已停止", flush=True)
        return 0
    finally:
        if conn is not None:
            conn.close()


def main(argv: Sequence[str] | None = None) -> int:
    """命令行入口。"""
    parser = argparse.ArgumentParser(description="三所实时逐笔 segment 物化")
    parser.add_argument("--data-root", type=Path, default=None)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("all")
    sub.add_parser("audit")
    watch = sub.add_parser("watch")
    watch.add_argument("--interval-seconds", type=float, default=300.0)
    args = parser.parse_args(argv)
    root = (args.data_root or configured_data_root()).resolve()
    if args.command == "watch":
        interval = float(args.interval_seconds)
        if interval < 10:
            raise ValueError("interval-seconds 不得小于 10")
        return _watch(root, interval)

    conn = store.connect(root)
    try:
        if args.command == "all":
            with sqlite_writer_lock(root):
                completed = materialize_all(root, conn)
            result: object = [asdict(item) for item in completed]
            code = 0
        elif args.command == "audit":
            result = audit_realtime_trades(root, conn)
            code = 0 if bool(result["ok"]) else 1
        else:
            raise AssertionError(f"未知命令: {args.command}")
    finally:
        conn.close()
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
