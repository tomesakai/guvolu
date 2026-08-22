"""封口实时 segment 到 ``book_l2_frame``/``book_l2_level`` Parquet。"""
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
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path, PurePosixPath
from typing import Any

import duckdb

from guvolu.data import store
from guvolu.data.book_l2_contract import (
    BOOK_L2_FRAME_DATASET,
    BOOK_L2_LEVEL_DATASET,
    BOOK_L2_V3_NORMALIZATION_VERSION,
    BOOK_L2_V4_NORMALIZATION_VERSION,
    BOOK_L2_V5_NORMALIZATION_VERSION,
    BOOK_L2_V5_SCHEMA_VERSION,
    BookSourceDescriptor,
    create_book_l2_v5_tables,
)
from guvolu.data.durable_io import atomic_write_text
from guvolu.data.materialize import (
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
from guvolu.data.materialize import SourceArtifact
from guvolu.data.paths import data_root as configured_data_root
from guvolu.data.realtime_control import (
    RealtimeChannelObservation,
    register_materialized_raw_v3_observations,
)
from guvolu.data.sqlite_writer_lock import sqlite_writer_lock
from guvolu.data.watch_connection import connect_with_retry
from guvolu.venues import registry

FRAME_DATASET = BOOK_L2_FRAME_DATASET
LEVEL_DATASET = BOOK_L2_LEVEL_DATASET
L2_SCHEMA_VERSION = BOOK_L2_V5_SCHEMA_VERSION
L2_NORMALIZATION_VERSION = BOOK_L2_V5_NORMALIZATION_VERSION
LEGACY_L2_NORMALIZATION_VERSION = "book-l2-normalization-v1"
SUPERSEDED_L2_NORMALIZATION_VERSIONS = (
    BOOK_L2_V3_NORMALIZATION_VERSION,
    BOOK_L2_V4_NORMALIZATION_VERSION,
)
_DESCRIPTORS = {
    "gmo": BookSourceDescriptor(
        venue_id="gmo", domain="book_realtime", endpoint="orderbooks/ws",
        endpoint_id="EP-0007", endpoint_revision=0, transport="websocket",
        payload_schema_version="gmo-orderbooks-ws-v1",
        timestamp_unit="iso8601", book_mode="full_snapshot",
        replay_fidelity="snapshot_only", sequence_policy="none",
        checksum_policy="none", availability_policy="recv_ts_utc",
        depth_limit=None,
    ),
    "bitbank": BookSourceDescriptor(
        venue_id="bitbank", domain="book_realtime",
        endpoint="depth_whole/depth_diff", endpoint_id="EP-0005",
        endpoint_revision=0,
        transport="websocket",
        payload_schema_version="bitbank-depth-stream-v1",
        timestamp_unit="milliseconds",
        book_mode="absolute_level_update",
        replay_fidelity="snapshot_monotonic_delta",
        sequence_policy="monotonic_per_connection_and_room",
        checksum_policy="none", availability_policy="recv_ts_utc",
        depth_limit=None,
    ),
    "bitflyer": BookSourceDescriptor(
        venue_id="bitflyer", domain="book_realtime",
        endpoint="board_snapshot/board", endpoint_id="EP-0002",
        endpoint_revision=0,
        transport="websocket",
        payload_schema_version="bitflyer-lightning-board-v1",
        timestamp_unit="receive_clock",
        book_mode="absolute_level_update",
        replay_fidelity="snapshot_unsequenced_delta",
        sequence_policy="none", checksum_policy="none",
        availability_policy="recv_ts_utc", depth_limit=None,
    ),
}

# r0 是历史深度范围。
# r1 增加市场状态频道。
# L2 只按原件修订解析。
_BITBANK_RAW_ENDPOINT_SCOPES = {
    0: "depth_whole/depth_diff",
    1: "depth_whole/depth_diff/circuit_break_info",
}


def _allowed_endpoint_revisions(venue_id: str) -> frozenset[int]:
    if venue_id == "bitbank":
        return frozenset(_BITBANK_RAW_ENDPOINT_SCOPES)
    revision = _DESCRIPTORS[venue_id].endpoint_revision
    return frozenset() if revision is None else frozenset({revision})


def _raw_endpoint_scope(
    descriptor: BookSourceDescriptor, item: "SegmentInput",
) -> str:
    if descriptor.venue_id != "bitbank" or item.raw_schema_version < 3:
        return descriptor.endpoint
    revision = item.endpoint_revision
    if revision is None:
        raise ValueError("bitbank raw endpoint revision 未登记")
    try:
        return _BITBANK_RAW_ENDPOINT_SCOPES[revision]
    except KeyError as exc:
        raise ValueError("bitbank raw endpoint revision 未登记") from exc


@dataclass(frozen=True)
class SegmentInput:
    """经 segment manifest 验证的单一市场原件。"""

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
class L2Result:
    """一个 L2 segment 的物化结果。"""

    attempt_id: str
    market_id: str
    partition_key: str
    status: str
    source_rows: int
    frame_rows: int
    level_rows: int
    ignored_rows: int
    rejected_rows: int
    frame_path: str
    level_path: str
    reused: bool


def _iso_millis(value: object) -> str:
    moment = datetime.fromtimestamp(int(str(value)) / 1000, UTC)
    return moment.isoformat()


def _time(value: object, label: str) -> datetime:
    text = str(value)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{label} 不是 ISO-8601 时间: {text!r}") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{label} 缺少时区: {text!r}")
    return parsed.astimezone(UTC)


def _available(event_time: str, ingest_time: str) -> str:
    return max(
        _time(event_time, "event_time"),
        _time(ingest_time, "ingest_time"),
    ).isoformat()


def _frame_id(
    venue_id: str, market_id: str, source_artifact_id: str, source_row: int
) -> str:
    body = f"{venue_id}|{market_id}|{source_artifact_id}|{source_row}"
    return "sha256-" + hashlib.sha256(body.encode("utf-8")).hexdigest()


def _decimal_text(value: object) -> str:
    text = str(value)
    number = Decimal(text)
    if not number.is_finite() or number < 0:
        raise ValueError(f"盘口数值非法: {text}")
    return text


def _parse_levels(
    rows: object, side: str, message_kind: str, *,
    ignore_snapshot_zero: bool = False,
) -> tuple[list[tuple[int, str, str, str, str]], int]:
    if not isinstance(rows, list):
        raise ValueError(f"{side} 价位不是数组")
    out: list[tuple[int, str, str, str, str]] = []
    ignored_zero = 0
    for index, row in enumerate(rows):
        if isinstance(row, Mapping):
            raw_price, raw_size = row.get("price"), row.get("size")
        elif isinstance(row, list) and len(row) >= 2:
            raw_price, raw_size = row[0], row[1]
        else:
            raise ValueError(f"{side} 价位结构非法")
        price = _decimal_text(raw_price)
        size = _decimal_text(raw_size)
        if message_kind == "snapshot" and Decimal(size) == 0:
            if ignore_snapshot_zero:
                ignored_zero += 1
                continue
            raise ValueError(f"{side} snapshot 含零数量档")
        action = (
            "delete" if message_kind == "delta" and Decimal(size) == 0
            else "set"
        )
        level_kind = "market" if Decimal(price) == 0 else "limit"
        out.append((index, side, price, size, action + ":" + level_kind))
    return out, ignored_zero


def _parse_gmo(payload_raw: str, ingest: str) -> dict[str, object] | None:
    payload = json.loads(payload_raw)
    if not isinstance(payload, Mapping) or payload.get("channel") != "orderbooks":
        return None
    event = str(payload.get("timestamp") or ingest)
    return {
        "kind": "snapshot", "event": event, "time_origin": "venue",
        "sequence": None, "integrity": "snapshot_no_sequence",
        "channel": "orderbooks", "source_publish": event,
        "bids": payload.get("bids"), "asks": payload.get("asks"),
        "mid": None, "ask_market": None, "bid_market": None,
        "asks_over": None, "bids_under": None,
        "asks_under": None, "bids_over": None,
    }


def _parse_bitbank(payload_raw: str, ingest: str) -> dict[str, object] | None:
    if not payload_raw.startswith("42"):
        return None
    packet = json.loads(payload_raw[2:])
    if not isinstance(packet, list) or len(packet) < 2 or not isinstance(packet[1], Mapping):
        return None
    envelope = packet[1]
    room = str(envelope.get("room_name", ""))
    message = envelope.get("message")
    data = message.get("data") if isinstance(message, Mapping) else None
    if not isinstance(data, Mapping):
        raise ValueError("bitbank message.data 缺失")
    if room.startswith("depth_whole_"):
        kind = "snapshot"
        bids = data.get("bids")
        asks = data.get("asks")
        sequence = data.get("sequenceId")
        raw_time = data.get("timestamp")
        ask_market = data.get("ask_market")
        bid_market = data.get("bid_market")
        asks_over = data.get("asks_over")
        bids_under = data.get("bids_under")
        asks_under = data.get("asks_under")
        bids_over = data.get("bids_over")
    elif room.startswith("depth_diff_"):
        kind = "delta"
        bids = data.get("b")
        asks = data.get("a")
        sequence = data.get("s")
        raw_time = data.get("t")
        ask_market = data.get("am")
        bid_market = data.get("bm")
        asks_over = data.get("ao")
        bids_under = data.get("bu")
        asks_under = data.get("au")
        bids_over = data.get("bo")
    else:
        return None
    if raw_time is None or sequence is None:
        raise ValueError("bitbank 盘口缺 timestamp/sequence")
    return {
        "kind": kind, "event": _iso_millis(raw_time),
        "time_origin": "venue", "sequence": str(sequence),
        "integrity": "snapshot_plus_monotonic_delta",
        "channel": room, "source_publish": _iso_millis(raw_time),
        "bids": bids, "asks": asks, "mid": None,
        "ask_market": ask_market, "bid_market": bid_market,
        "asks_over": asks_over, "bids_under": bids_under,
        "asks_under": asks_under, "bids_over": bids_over,
    }


def _parse_bitflyer(payload_raw: str, ingest: str) -> dict[str, object] | None:
    payload = json.loads(payload_raw)
    if not isinstance(payload, Mapping) or payload.get("method") != "channelMessage":
        return None
    params = payload.get("params")
    if not isinstance(params, Mapping):
        raise ValueError("bitFlyer channelMessage.params 缺失")
    channel = str(params.get("channel", ""))
    message = params.get("message")
    if not isinstance(message, Mapping):
        raise ValueError("bitFlyer channelMessage.message 缺失")
    if "_snapshot_" in channel:
        kind = "snapshot"
    elif channel.startswith("lightning_board_"):
        kind = "delta"
    else:
        return None
    return {
        "kind": kind, "event": ingest, "time_origin": "ingest_proxy",
        "sequence": None, "integrity": "snapshot_plus_unsequenced_delta",
        "channel": channel, "source_publish": None,
        "bids": message.get("bids"), "asks": message.get("asks"),
        "mid": message.get("mid_price"), "ask_market": None,
        "bid_market": None, "asks_over": None, "bids_under": None,
        "asks_under": None, "bids_over": None,
    }


_PARSERS = {
    "gmo": _parse_gmo,
    "bitbank": _parse_bitbank,
    "bitflyer": _parse_bitflyer,
}


def _sealed_inputs(
    root: Path, *, latest_run_only: bool = False,
) -> list[SegmentInput]:
    inputs: list[SegmentInput] = []
    base = _resolve_recorded_path(root, "raw/realtime/book_l2")
    if not base.is_dir():
        return []
    if latest_run_only:
        latest_directories: dict[str, tuple[int, int, str, Path]] = {}
        for run_directory in base.rglob("run_id=*"):
            if not run_directory.is_dir():
                continue
            venue_id = next(
                (
                    part.split("=", 1)[1] for part in run_directory.parts
                    if part.startswith("venue_id=")
                ),
                "",
            )
            run_id = run_directory.name.split("=", 1)[1]
            open_run = int(
                (run_directory / "checkpoint.json").is_file()
                and next(run_directory.glob("segment-*.open"), None)
                is not None
            )
            candidate = (
                open_run, run_directory.stat().st_mtime_ns,
                run_id, run_directory,
            )
            if candidate[:3] > latest_directories.get(
                venue_id, (-1, -1, "", run_directory),
            )[:3]:
                latest_directories[venue_id] = candidate
        manifest_paths = sorted(
            path
            for _, _, _, directory in latest_directories.values()
            for path in directory.glob("segment-*.manifest.json")
        )
    else:
        manifest_paths = sorted(base.rglob("segment-*.manifest.json"))
    manifests: list[tuple[Path, Mapping[str, object]]] = []
    for manifest_path in manifest_paths:
        body = json.loads(manifest_path.read_text(encoding="utf-8"))
        if not isinstance(body, Mapping):
            continue
        if body.get("status") != "sealed" or body.get("completion_claim") is not True:
            continue
        manifests.append((manifest_path, body))
    for manifest_path, body in manifests:
        recorded = str(body["storage_path"])
        path = _resolve_recorded_path(root, recorded)
        expected_path = manifest_path.with_name(
            manifest_path.name.removesuffix(".manifest.json") + ".jsonl"
        ).resolve()
        if path != expected_path:
            raise ValueError(f"manifest 与 segment 不同目录: {recorded}")
        sha = sha256_file(path)
        if sha != str(body["sha256"]):
            raise ValueError(f"segment 散列不符: {recorded}")
        if path.stat().st_size != int(str(body["byte_count"])):
            raise ValueError(f"segment 字节数不符: {recorded}")
        if body.get("artifact_id") not in {None, artifact_id(sha)}:
            raise ValueError(f"segment artifact_id 不符: {recorded}")
        source_rows = int(str(body["record_count"]))
        raw_schema_version = int(str(body.get("schema_version", 1)))
        if raw_schema_version not in {1, 2, 3}:
            raise ValueError(
                f"segment schema_version 尚不支持: {recorded}: "
                f"{raw_schema_version}"
            )
        if (
            raw_schema_version == 3
            and body.get("artifact_id") != artifact_id(sha)
        ):
            raise ValueError(f"raw v3 manifest artifact_id 缺失: {recorded}")
        endpoint_id = (
            str(body["endpoint_id"])
            if body.get("endpoint_id") is not None else None
        )
        endpoint_revision: int | None = None
        if raw_schema_version == 3:
            recorded_revision = body.get("endpoint_revision")
            try:
                venue_id = next(
                    part.split("=", 1)[1] for part in path.parts
                    if part.startswith("venue_id=")
                )
                descriptor = _DESCRIPTORS[venue_id]
            except (KeyError, StopIteration) as exc:
                raise ValueError(
                    f"raw v3 manifest venue 端点契约未知: {recorded}"
                ) from exc
            if not isinstance(endpoint_id, str) or not endpoint_id:
                raise ValueError(f"raw v3 manifest endpoint_id 缺失: {recorded}")
            if (
                isinstance(recorded_revision, bool)
                or not isinstance(recorded_revision, int)
                or recorded_revision < 0
            ):
                raise ValueError(
                    f"raw v3 manifest endpoint_revision 非法: {recorded}"
                )
            if (
                endpoint_id != descriptor.endpoint_id
                or recorded_revision not in _allowed_endpoint_revisions(
                    venue_id
                )
            ):
                raise ValueError(
                    f"raw v3 manifest 与端点契约不一致: {recorded}"
                )
            endpoint_revision = recorded_revision
        inputs.append(SegmentInput(
            manifest_path=manifest_path,
            run_id=str(body["run_id"]),
            segment_sequence=int(str(body["segment_sequence"])),
            raw_schema_version=raw_schema_version,
            endpoint_id=endpoint_id,
            endpoint_revision=endpoint_revision,
            artifact=SourceArtifact(
                artifact_id=artifact_id(sha), storage_path=recorded,
                absolute_path=path, source_rows=source_rows,
                normalized_rows=0, rejected_rows=0,
            ),
        ))
    return inputs


def _latest_run_inputs(inputs: Sequence[SegmentInput]) -> list[SegmentInput]:
    """每所只保留最近写入的封口运行。"""
    latest: dict[str, tuple[int, str]] = {}
    for item in inputs:
        venue = next(
            part.split("=", 1)[1] for part in item.manifest_path.parts
            if part.startswith("venue_id=")
        )
        candidate = (item.manifest_path.stat().st_mtime_ns, item.run_id)
        if candidate > latest.get(venue, (-1, "")):
            latest[venue] = candidate
    selected_runs = {venue: value[1] for venue, value in latest.items()}
    return [
        item for item in inputs
        if item.run_id == selected_runs[next(
            part.split("=", 1)[1] for part in item.manifest_path.parts
            if part.startswith("venue_id=")
        )]
    ]


def _capability_revision(
    conn: sqlite3.Connection, venue_id: str,
) -> int:
    descriptor = _DESCRIPTORS[venue_id]
    row = conn.execute(
        "SELECT revision_id FROM venue_capability_revision "
        "WHERE venue_id=? AND domain='book_realtime' AND endpoint=? "
        "AND available=1 AND implementation_status='implemented' "
        "ORDER BY revision_id DESC LIMIT 1",
        (venue_id, descriptor.endpoint),
    ).fetchone()
    if row is None:
        raise ValueError(
            "L2 能力尚未登记为 implemented: "
            f"{venue_id}/{descriptor.endpoint}"
        )
    return int(row[0])


def _bind_capability(
    conn: sqlite3.Connection, attempt_id: str, venue_id: str,
    capability_revision: int,
) -> None:
    descriptor = _DESCRIPTORS[venue_id]
    conn.execute(
        "INSERT INTO partition_capability_binding "
        "(attempt_id,venue_id,domain,endpoint,revision_id,binding_basis,bound_at) "
        "VALUES (?,?,'book_realtime',?,?,'recorded',?)",
        (attempt_id, venue_id, descriptor.endpoint, capability_revision,
         utc_now()),
    )


def _register_source(conn: sqlite3.Connection, item: SegmentInput) -> None:
    created = datetime.fromtimestamp(item.artifact.absolute_path.stat().st_mtime, UTC).isoformat()
    _register_content_artifact(
        conn, item.artifact.artifact_id, "raw_realtime_segment",
        item.artifact.storage_path,
        item.artifact.artifact_id.removeprefix("sha256-"),
        item.artifact.absolute_path.stat().st_size, created,
        item.raw_schema_version,
    )


def _field(value: object) -> object:
    return "\\N" if value is None else value


@dataclass(frozen=True)
class _RawFrameMetadata:
    """从 wire envelope 验证出的、不得由业务 payload 猜测的字段。"""

    ingest_time: str
    endpoint_id: str | None
    endpoint_revision: int | None
    connection_id: str | None
    channel_id: str | None
    recv_ts_mono_ns: int | None
    raw_payload_sha256: str
    quality_flags: tuple[str, ...]
    record_sequence: int | None


def _raw_metadata(
    envelope: Mapping[str, object], item: SegmentInput,
    descriptor: BookSourceDescriptor,
) -> tuple[str, _RawFrameMetadata]:
    payload = envelope.get("payload_raw")
    if not isinstance(payload, str):
        raise ValueError("segment 行 payload_raw 不是字符串")
    raw_schema = int(str(envelope.get("schema_version", 1)))
    if raw_schema != item.raw_schema_version:
        raise ValueError("segment 行 schema_version 与 manifest 不一致")
    if envelope.get("source_endpoint") not in {
        None, _raw_endpoint_scope(descriptor, item)
    }:
        raise ValueError("segment 行 source_endpoint 与端点契约不一致")
    if envelope.get("domain") not in {None, "book_l2"}:
        raise ValueError("segment 行 domain 与端点契约不一致")
    computed_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    ingest = str(envelope["ingest_time"])
    _time(ingest, "ingest_time")
    if raw_schema == 1:
        return payload, _RawFrameMetadata(
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
    endpoint_id = envelope.get("endpoint_id")
    if (
        not isinstance(endpoint_id, str)
        or endpoint_id != descriptor.endpoint_id
        or endpoint_id != item.endpoint_id
    ):
        raise ValueError(
            f"raw v{raw_schema} endpoint_id 与行/manifest/端点契约不一致"
        )
    connection_id = envelope.get("connection_id")
    if (
        not isinstance(connection_id, str)
        or not connection_id.startswith(f"{item.run_id}-c")
    ):
        raise ValueError(
            f"raw v{raw_schema} connection_id 不属于当前 source_session"
        )
    channel_id = envelope.get("channel_id")
    if not isinstance(channel_id, str) or not channel_id:
        raise ValueError(f"raw v{raw_schema} channel_id 缺失")
    recv_utc = str(envelope.get("recv_ts_utc", ""))
    if _time(recv_utc, "recv_ts_utc") != _time(ingest, "ingest_time"):
        raise ValueError(
            f"raw v{raw_schema} recv_ts_utc 与 ingest_time 不一致"
        )
    mono = envelope.get("recv_ts_mono_ns")
    if (
        isinstance(mono, bool) or not isinstance(mono, int)
        or mono < 0 or mono > 2**64 - 1
    ):
        raise ValueError(f"raw v{raw_schema} recv_ts_mono_ns 非法")
    recorded_hash = envelope.get("raw_payload_sha256")
    if recorded_hash != computed_hash:
        raise ValueError(f"raw v{raw_schema} payload SHA-256 校验失败")
    endpoint_revision: int | None = None
    quality_flags = ["raw_payload_hash_verified"]
    if raw_schema == 2:
        quality_flags.append("endpoint_revision_unrecorded")
    else:
        recorded_revision = envelope.get("endpoint_revision")
        if (
            isinstance(recorded_revision, bool)
            or not isinstance(recorded_revision, int)
            or recorded_revision < 0
            or recorded_revision != item.endpoint_revision
            or recorded_revision not in _allowed_endpoint_revisions(
                descriptor.venue_id
            )
        ):
            raise ValueError(
                "raw v3 endpoint_revision 与行/manifest/端点契约不一致"
            )
        endpoint_revision = recorded_revision
    record_sequence: int | None = None
    if raw_schema == 3:
        recorded_sequence = envelope.get("record_sequence")
        if (
            isinstance(recorded_sequence, bool)
            or not isinstance(recorded_sequence, int)
            or recorded_sequence <= 0
        ):
            raise ValueError("raw v3 record_sequence 非法")
        record_sequence = recorded_sequence
    return payload, _RawFrameMetadata(
        ingest_time=recv_utc,
        endpoint_id=endpoint_id,
        endpoint_revision=endpoint_revision,
        connection_id=connection_id,
        channel_id=channel_id,
        recv_ts_mono_ns=mono,
        raw_payload_sha256=computed_hash,
        quality_flags=tuple(quality_flags),
        record_sequence=record_sequence,
    )


def _optional_decimal(value: object) -> str | None:
    return None if value is None else _decimal_text(value)


def _ignored_frame_reason(venue_id: str, payload_raw: str) -> str:
    """区分协议控制、旁路业务域与 bitbank 市场状态。"""
    if venue_id != "bitbank" or not payload_raw.startswith("42"):
        return "protocol_control_frame"
    try:
        packet = json.loads(payload_raw[2:])
        envelope = packet[1] if isinstance(packet, list) and len(packet) > 1 else None
        room = (
            envelope.get("room_name")
            if isinstance(envelope, Mapping) else None
        )
    except json.JSONDecodeError:
        return "protocol_control_frame"
    if isinstance(room, str) and room.startswith("circuit_break_info_"):
        return "market_status_frame"
    return "unsupported_domain_frame"


def _snapshot_mid(
    bids: list[tuple[int, str, str, str, str]],
    asks: list[tuple[int, str, str, str, str]],
) -> str | None:
    bid_prices = [Decimal(row[2]) for row in bids if Decimal(row[2]) > 0]
    ask_prices = [Decimal(row[2]) for row in asks if Decimal(row[2]) > 0]
    if not bid_prices or not ask_prices:
        return None
    return str((max(bid_prices) + min(ask_prices)) / 2)


def _stage(
    item: SegmentInput, venue_id: str, venue_symbol: str,
    market_id: str, mapping_revision: int, capability_revision: int,
    instrument_id: str,
    frame_csv: Path, level_csv: Path,
) -> tuple[
    int, int, list[tuple[int, str]], list[tuple[int, str]], str, str,
    tuple[RealtimeChannelObservation, ...],
]:
    parser = _PARSERS[venue_id]
    descriptor = _DESCRIPTORS[venue_id]
    frames = levels = 0
    ignored: list[tuple[int, str]] = []
    rejected: list[tuple[int, str]] = []
    min_event = ""
    max_event = ""
    observations: list[RealtimeChannelObservation] = []
    previous_record_sequence: int | None = None
    with (
        item.artifact.absolute_path.open(encoding="utf-8") as source,
        frame_csv.open("w", encoding="utf-8", newline="") as frame_handle,
        level_csv.open("w", encoding="utf-8", newline="") as level_handle,
    ):
        frame_writer = csv.writer(frame_handle, lineterminator="\n")
        level_writer = csv.writer(level_handle, lineterminator="\n")
        for source_row, line in enumerate(source, start=1):
            try:
                envelope = json.loads(line)
                if not isinstance(envelope, Mapping):
                    raise ValueError("segment 行不是对象")
                if (
                    envelope.get("venue_id") != venue_id
                    or envelope.get("venue_symbol") != venue_symbol
                    or envelope.get("run_id") != item.run_id
                    or int(str(envelope.get("segment_sequence"))) != item.segment_sequence
                ):
                    raise ValueError("segment 行身份与路径/manifest 不符")
                payload_raw, raw = _raw_metadata(envelope, item, descriptor)
                if raw.record_sequence is not None:
                    if (
                        previous_record_sequence is not None
                        and raw.record_sequence <= previous_record_sequence
                    ):
                        raise ValueError("raw v3 record_sequence 未严格递增")
                    previous_record_sequence = raw.record_sequence
                parsed = parser(payload_raw, raw.ingest_time)
                if parsed is None:
                    ignored.append((
                        source_row,
                        _ignored_frame_reason(venue_id, payload_raw),
                    ))
                    continue
                if (
                    raw.channel_id is not None
                    and raw.channel_id != str(parsed["channel"])
                ):
                    raise ValueError(
                        "raw v2/v3 channel_id 与 payload channel 不一致"
                    )
                kind = str(parsed["kind"])
                ignore_snapshot_zero = venue_id == "bitflyer"
                bids, ignored_bid_zero = _parse_levels(
                    parsed["bids"], "bid", kind,
                    ignore_snapshot_zero=ignore_snapshot_zero,
                )
                asks, ignored_ask_zero = _parse_levels(
                    parsed["asks"], "ask", kind,
                    ignore_snapshot_zero=ignore_snapshot_zero,
                )
                event = _time(parsed["event"], "event_time").isoformat()
                source_publish = (
                    None if parsed["source_publish"] is None
                    else _time(
                        parsed["source_publish"], "source_publish_time"
                    ).isoformat()
                )
                available = _available(event, raw.ingest_time)
                identity = _frame_id(
                    venue_id, market_id, item.artifact.artifact_id, source_row
                )
                sequence = parsed["sequence"]
                prev_sequence: str | None = None
                quality = list(raw.quality_flags)
                if venue_id == "bitbank":
                    if sequence is None:
                        raise ValueError("bitbank sequence 缺失")
                    int(str(sequence))
                zero_levels = ignored_bid_zero + ignored_ask_zero
                if zero_levels:
                    quality.append("snapshot_zero_levels_ignored")
                if source_publish is None:
                    quality.append("source_publish_time_missing")
                native_mid = _optional_decimal(parsed["mid"])
                mid = (
                    native_mid if native_mid is not None
                    else _snapshot_mid(bids, asks) if kind == "snapshot"
                    else None
                )
                book_bid_levels = len(bids) if kind == "snapshot" else None
                book_ask_levels = len(asks) if kind == "snapshot" else None
                frame_writer.writerow([_field(value) for value in (
                    identity, venue_id, venue_symbol, market_id,
                    mapping_revision, capability_revision, instrument_id,
                    descriptor.endpoint, raw.endpoint_id,
                    raw.endpoint_revision,
                    descriptor.payload_schema_version,
                    kind, descriptor.book_mode, descriptor.replay_fidelity,
                    event, source_publish, available,
                    raw.ingest_time, raw.recv_ts_mono_ns,
                    parsed["time_origin"], sequence, prev_sequence, None,
                    parsed["integrity"], len(bids), len(asks),
                    book_bid_levels, book_ask_levels, descriptor.depth_limit,
                    len(bids) + len(asks), mid, item.run_id,
                    raw.connection_id, raw.channel_id,
                    item.artifact.absolute_path.name,
                    raw.raw_payload_sha256,
                    json.dumps(sorted(set(quality)), separators=(",", ":")),
                    descriptor.source_level,
                    _optional_decimal(parsed["ask_market"]),
                    _optional_decimal(parsed["bid_market"]),
                    _optional_decimal(parsed["asks_over"]),
                    _optional_decimal(parsed["bids_under"]),
                    _optional_decimal(parsed["asks_under"]),
                    _optional_decimal(parsed["bids_over"]),
                    item.run_id, item.segment_sequence,
                    item.artifact.artifact_id, source_row,
                    L2_NORMALIZATION_VERSION, L2_SCHEMA_VERSION,
                )])
                for index, side, price, size, action_kind in bids + asks:
                    action, level_kind = action_kind.split(":", 1)
                    level_writer.writerow((
                        identity, market_id, side, index, price, size,
                        "\\N", action, level_kind,
                        item.artifact.artifact_id, source_row,
                        L2_NORMALIZATION_VERSION, L2_SCHEMA_VERSION,
                    ))
                    levels += 1
                if item.raw_schema_version == 3:
                    assert raw.connection_id is not None
                    assert raw.channel_id is not None
                    observations.append(RealtimeChannelObservation(
                        raw.connection_id, raw.channel_id, raw.ingest_time,
                    ))
                frames += 1
                min_event = event if not min_event else min(min_event, event)
                max_event = event if not max_event else max(max_event, event)
            except (
                KeyError, TypeError, ValueError, InvalidOperation,
                json.JSONDecodeError,
            ) as exc:
                rejected.append((source_row, f"{type(exc).__name__}: {exc}"))
        for handle in (frame_handle, level_handle):
            handle.flush()
            os.fsync(handle.fileno())
    if item.artifact.source_rows != frames + len(ignored) + len(rejected):
        raise ValueError("L2 来源行分类不守恒")
    return (
        frames, levels, ignored, rejected, min_event, max_event,
        tuple(observations),
    )


def _create_tables(db: Any) -> None:
    create_book_l2_v5_tables(db)


def _copy_csv(db: Any, table: str, path: Path) -> None:
    escaped = path.as_posix().replace("'", "''")
    db.execute(
        f"COPY {table} FROM '{escaped}' "
        "(FORMAT CSV, HEADER false, NULL '\\N')"
    )


def _write_parquet(db: Any, query: str, path: Path) -> None:
    escaped = path.as_posix().replace("'", "''")
    db.execute(
        f"COPY ({query}) TO '{escaped}' "
        "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880)"
    )
    with path.open("rb+") as handle:
        handle.flush()
        os.fsync(handle.fileno())


def _finalize(temp: Path) -> tuple[Path, str]:
    sha = sha256_file(temp)
    final = temp.with_name(f"part-{sha[:12]}.parquet")
    if final.exists():
        if sha256_file(final) != sha:
            raise ValueError(f"输出散列命名冲突: {final}")
        temp.unlink()
    else:
        os.replace(temp, final)
    return final, sha


def materialize_segment(
    root: Path, conn: sqlite3.Connection, item: SegmentInput,
) -> L2Result:
    parts = item.artifact.absolute_path.parts
    venue_id = next(part.split("=", 1)[1] for part in parts if part.startswith("venue_id="))
    venue_symbol = next(part.split("=", 1)[1] for part in parts if part.startswith("venue_symbol="))
    descriptor = _DESCRIPTORS[venue_id]
    if not _allowed_endpoint_revisions(venue_id):
        raise ValueError(f"L2 端点修订未登记: {venue_id}")
    registry.register_all(conn)
    ensure_markets(conn)
    market_id, instrument_id, mapping_revision = _market_row(
        conn, venue_id, venue_symbol, None
    )
    capability_revision = _capability_revision(conn, venue_id)
    _register_source(conn, item)
    conn.commit()
    input_hash = _input_set_hash([item.artifact])
    existing = conn.execute(
        "SELECT a.attempt_id,a.status,a.source_rows,a.normalized_rows,"
        "a.ignored_rows,a.rejected_rows,"
        "MAX(CASE WHEN o.dataset=? THEN r.storage_path END),"
        "MAX(CASE WHEN o.dataset=? THEN r.storage_path END) "
        "FROM partition_attempt a JOIN materialization_output o "
        "ON o.attempt_id=a.attempt_id JOIN artifact r ON r.artifact_id=o.artifact_id "
        "WHERE a.market_id=? AND a.domain='book_l2' AND a.partition_key=? "
        "AND a.normalization_version=? AND a.input_set_hash=? "
        "AND a.status IN ('complete','complete_with_rejections') "
        "GROUP BY a.attempt_id,a.status,a.source_rows,a.normalized_rows,"
        "a.ignored_rows,a.rejected_rows LIMIT 1",
        (FRAME_DATASET, LEVEL_DATASET, market_id, item.partition_key,
         L2_NORMALIZATION_VERSION, input_hash),
    ).fetchone()
    if existing is not None and existing[6] and existing[7]:
        return L2Result(
            str(existing[0]), market_id, item.partition_key, str(existing[1]),
            int(existing[2]), int(existing[3]),
            int(conn.execute(
                "SELECT row_count FROM materialization_output "
                "WHERE attempt_id=? AND dataset=?", (str(existing[0]), LEVEL_DATASET)
            ).fetchone()[0]),
            int(existing[4]), int(existing[5]), str(existing[6]), str(existing[7]), True,
        )
    attempt_id = f"l2-{uuid.uuid4().hex}"
    config_hash = hashlib.sha256(json.dumps({
        "dataset": [FRAME_DATASET, LEVEL_DATASET],
        "normalization_version": L2_NORMALIZATION_VERSION,
        "schema_version": L2_SCHEMA_VERSION,
        "input_endpoint_binding": {
            "endpoint_id": item.endpoint_id,
            "endpoint_revision": item.endpoint_revision,
        },
    }, sort_keys=True).encode()).hexdigest()
    conn.execute(
        "INSERT INTO partition_attempt "
        "(attempt_id,market_id,domain,partition_key,normalization_version,"
        "input_set_hash,status,source_rows,normalized_rows,ignored_rows,"
        "rejected_rows,started_at,code_version,config_hash) "
        "VALUES (?,?,?,?,?,?,'running',?,0,0,0,?,'working-tree',?)",
        (attempt_id, market_id, "book_l2", item.partition_key,
         L2_NORMALIZATION_VERSION, input_hash, item.artifact.source_rows,
         utc_now(), config_hash),
    )
    _bind_capability(
        conn, attempt_id, venue_id, capability_revision
    )
    conn.commit()
    output_dir = _resolve_recorded_path(
        root,
        PurePosixPath(
            "materialized", "book_l2",
            f"schema_version={L2_SCHEMA_VERSION}",
            f"normalization_version={L2_NORMALIZATION_VERSION}",
            f"venue_id={venue_id}", f"market_id={market_id}",
            f"run_id={item.run_id}",
            f"segment={item.segment_sequence:06d}",
        ).as_posix(),
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    frame_csv = output_dir / f".{attempt_id}.frames.csv"
    level_csv = output_dir / f".{attempt_id}.levels.csv"
    frame_tmp = output_dir / f".{attempt_id}.frames.parquet"
    level_tmp = output_dir / f".{attempt_id}.levels.parquet"
    try:
        (
            frames, levels, ignored, rejected, min_event, max_event,
            observations,
        ) = _stage(
            item, venue_id, venue_symbol, market_id, mapping_revision,
            capability_revision, instrument_id, frame_csv, level_csv,
        )
        db: Any = duckdb.connect(":memory:")
        db.execute("SET TimeZone='UTC'")
        try:
            _create_tables(db)
            if frames:
                _copy_csv(db, FRAME_DATASET, frame_csv)
            if levels:
                _copy_csv(db, LEVEL_DATASET, level_csv)
            valid_endpoint_revisions = ",".join(
                str(revision) for revision in sorted(
                    _allowed_endpoint_revisions(venue_id)
                )
            )
            check = db.execute(
                "SELECT COUNT(*),COUNT(*)-COUNT(DISTINCT frame_id),"
                "SUM(available_time<event_time),SUM(source_depth_levels),"
                "COUNT(DISTINCT schema_version),MIN(schema_version),"
                "COUNT(DISTINCT normalization_version),"
                "MIN(normalization_version),"
                "COUNT(DISTINCT capability_revision),"
                "MIN(capability_revision),"
                "SUM(CASE WHEN source_level!='L2' THEN 1 ELSE 0 END),"
                "SUM(CASE WHEN raw_payload_sha256 IS NULL OR "
                "length(raw_payload_sha256)!=64 THEN 1 ELSE 0 END),"
                "SUM(CASE WHEN endpoint_revision IS NULL AND "
                "strpos(data_quality,'endpoint_revision_unrecorded')=0 "
                "THEN 1 WHEN endpoint_revision IS NOT NULL AND "
                f"endpoint_revision NOT IN ({valid_endpoint_revisions}) "
                "THEN 1 ELSE 0 END) "
                "FROM book_l2_frame"
            ).fetchone()
            if (
                check is None or int(check[0]) != frames
                or int(check[1] or 0) or int(check[2] or 0)
                or (frames and (
                    int(check[4]) != 1 or int(check[5]) != L2_SCHEMA_VERSION
                    or int(check[6]) != 1
                    or str(check[7]) != L2_NORMALIZATION_VERSION
                    or int(check[8]) != 1
                    or int(check[9]) != capability_revision
                    or int(check[10] or 0) or int(check[11] or 0)
                    or int(check[12] or 0)
                ))
            ):
                raise ValueError("L2 frame 键或 PIT 契约不符")
            if int(check[3] or 0) != levels:
                raise ValueError("L2 frame/level 行数不守恒")
            orphan = db.execute(
                "SELECT COUNT(*) FROM book_l2_level l LEFT JOIN book_l2_frame f "
                "ON f.frame_id=l.frame_id WHERE f.frame_id IS NULL"
            ).fetchone()
            if orphan is None or int(orphan[0]):
                raise ValueError("L2 level 存在 orphan")
            _write_parquet(db, "SELECT * FROM book_l2_frame ORDER BY event_time,frame_id", frame_tmp)
            _write_parquet(db, "SELECT * FROM book_l2_level ORDER BY frame_id,side,source_level_index", level_tmp)
        finally:
            db.close()
        frame_csv.unlink()
        level_csv.unlink()
        frame_path, frame_sha = _finalize(frame_tmp)
        level_path, level_sha = _finalize(level_tmp)
        finished = utc_now()
        status = "complete_with_rejections" if rejected else "complete"
        frame_storage = _relative_storage_path(root, frame_path)
        level_storage = _relative_storage_path(root, level_path)
        manifest = {
            "attempt_id": attempt_id, "status": status,
            "market_id": market_id, "partition_key": item.partition_key,
            "normalization_version": L2_NORMALIZATION_VERSION,
            "schema_version": L2_SCHEMA_VERSION,
            "capability_revision": capability_revision,
            "input_schema_version": item.raw_schema_version,
            "input_endpoint_binding": {
                "endpoint_id": item.endpoint_id,
                "endpoint_revision": item.endpoint_revision,
            },
            "input_artifact_id": item.artifact.artifact_id,
            "source_rows": item.artifact.source_rows,
            "frame_rows": frames, "level_rows": levels,
            "ignored_rows": len(ignored), "rejected_rows": len(rejected),
            "outputs": {FRAME_DATASET: frame_storage, LEVEL_DATASET: level_storage},
        }
        manifest_path = output_dir / f"manifest-{attempt_id}.json"
        atomic_write_text(manifest_path, json.dumps(manifest, ensure_ascii=False, indent=2) + "\n")
        conn.execute("BEGIN IMMEDIATE")
        for identity, dataset, path, sha, rows in (
            (artifact_id(frame_sha), FRAME_DATASET, frame_path, frame_sha, frames),
            (artifact_id(level_sha), LEVEL_DATASET, level_path, level_sha, levels),
        ):
            storage_path = _relative_storage_path(root, path)
            _register_content_artifact(
                conn, identity, "materialized_parquet", storage_path, sha,
                path.stat().st_size, finished, L2_SCHEMA_VERSION,
            )
            conn.execute(
                "INSERT INTO materialization_output VALUES (?,?,?,?,?,?,?)",
                (attempt_id, identity, dataset, rows,
                 min_event or None, max_event or None, finished),
            )
        manifest_sha = sha256_file(manifest_path)
        _register_content_artifact(
            conn, artifact_id(manifest_sha), "materialization_manifest",
            _relative_storage_path(root, manifest_path), manifest_sha,
            manifest_path.stat().st_size, finished, L2_SCHEMA_VERSION,
        )
        conn.execute(
            "INSERT INTO partition_input "
            "(attempt_id,artifact_id,source_rows,normalized_rows,ignored_rows,rejected_rows) "
            "VALUES (?,?,?,?,?,?)",
            (attempt_id, item.artifact.artifact_id, item.artifact.source_rows,
             frames, len(ignored), len(rejected)),
        )
        conn.execute(
            "INSERT INTO partition_input_binding "
            "(attempt_id,artifact_id,storage_path,source_rows,normalized_rows,"
            "ignored_rows,rejected_rows) VALUES (?,?,?,?,?,?,?)",
            (attempt_id, item.artifact.artifact_id, item.artifact.storage_path,
             item.artifact.source_rows, frames, len(ignored), len(rejected)),
        )
        conn.executemany(
            "INSERT INTO materialization_ignore VALUES (?,?,?,?,?,?,?)",
            [(attempt_id, item.artifact.artifact_id, row, -1,
              f"{item.artifact.storage_path}:{row}", reason, finished)
             for row, reason in ignored],
        )
        conn.executemany(
            "INSERT INTO materialization_rejection VALUES (?,?,?,?,?,?)",
            [(attempt_id, item.artifact.artifact_id, row,
              f"{item.artifact.storage_path}:{row}", reason, finished)
             for row, reason in rejected],
        )
        conn.execute(
            "UPDATE partition_attempt SET status=?,normalized_rows=?,"
            "ignored_rows=?,rejected_rows=?,finished_at=? WHERE attempt_id=?",
            (status, frames, len(ignored), len(rejected), finished, attempt_id),
        )
        conn.execute(
            "INSERT INTO materialization_partition_head VALUES (?,?,?,?,?,?) "
            "ON CONFLICT(market_id,domain,partition_key) DO UPDATE SET "
            "normalization_version=excluded.normalization_version,"
            "attempt_id=excluded.attempt_id,activated_at=excluded.activated_at",
            (market_id, "book_l2", item.partition_key,
             L2_NORMALIZATION_VERSION, attempt_id, finished),
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
                capability_domain="book_realtime",
                capability_endpoint=descriptor.endpoint,
                capability_revision=capability_revision,
                observations=observations,
            )
        conn.commit()
        return L2Result(
            attempt_id, market_id, item.partition_key, status,
            item.artifact.source_rows, frames, levels, len(ignored), len(rejected),
            frame_storage, level_storage, False,
        )
    except Exception as exc:
        conn.rollback()
        for path in (frame_csv, level_csv, frame_tmp, level_tmp):
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
    root: Path,
    conn: sqlite3.Connection,
    *,
    report_reused: bool = True,
    latest_run_only: bool = False,
) -> list[L2Result]:
    """断点复用地物化全部已封口 L2 segment。"""
    inputs = _sealed_inputs(root, latest_run_only=latest_run_only)
    results: list[L2Result] = []
    for index, item in enumerate(inputs, start=1):
        result = materialize_segment(root, conn, item)
        results.append(result)
        if report_reused or not result.reused:
            print(
                f"[{index}/{len(inputs)}] "
                f"{'REUSED' if result.reused else 'DONE'} "
                f"{result.market_id} {result.partition_key} "
                f"frames={result.frame_rows:,} levels={result.level_rows:,} "
                f"ignored={result.ignored_rows} rejected={result.rejected_rows}",
                flush=True,
            )
    return results


def audit_l2(root: Path, conn: sqlite3.Connection) -> dict[str, object]:
    """复核活动 L2 输出的键、PIT、父子行数和来源分类。"""
    errors: list[str] = []
    rows = conn.execute(
        "SELECT a.attempt_id,a.market_id,a.normalization_version,"
        "a.source_rows,a.normalized_rows,a.ignored_rows,a.rejected_rows,"
        "MAX(CASE WHEN o.dataset=? THEN r.storage_path END),"
        "MAX(CASE WHEN o.dataset=? THEN r.storage_path END) "
        "FROM materialization_partition_head h JOIN partition_attempt a "
        "ON a.attempt_id=h.attempt_id JOIN market m ON m.market_id=a.market_id "
        "JOIN materialization_output o "
        "ON o.attempt_id=a.attempt_id JOIN artifact r ON r.artifact_id=o.artifact_id "
        "WHERE h.domain='book_l2' AND a.normalization_version IN (?,?,?,?) "
        "AND m.venue_id IN ('gmo','bitbank','bitflyer') "
        "GROUP BY a.attempt_id,a.market_id,"
        "a.normalization_version,a.source_rows,a.normalized_rows,"
        "a.ignored_rows,a.rejected_rows ORDER BY a.attempt_id",
        (
            FRAME_DATASET, LEVEL_DATASET, LEGACY_L2_NORMALIZATION_VERSION,
            *SUPERSEDED_L2_NORMALIZATION_VERSIONS, L2_NORMALIZATION_VERSION,
        ),
    ).fetchall()
    total_frames = total_levels = 0
    db: Any = duckdb.connect(":memory:")
    db.execute("SET TimeZone='UTC'")
    try:
        for row in rows:
            attempt, market, version = str(row[0]), str(row[1]), str(row[2])
            source, normalized, ignored, rejected = map(int, row[3:7])
            if source != normalized + ignored + rejected:
                errors.append(f"来源分类不守恒: {attempt}")
            if not row[7] or not row[8]:
                errors.append(f"L2 双输出缺失: {attempt}")
                continue
            frame_path = _resolve_recorded_path(root, str(row[7]))
            level_path = _resolve_recorded_path(root, str(row[8]))
            result = db.execute(
                "SELECT COUNT(*),COUNT(*)-COUNT(DISTINCT frame_id),"
                "SUM(available_time<event_time),SUM(source_depth_levels),"
                "COUNT(DISTINCT market_id),MIN(market_id),"
                "COUNT(DISTINCT normalization_version),MIN(normalization_version) "
                "FROM read_parquet(?)", [str(frame_path)],
            ).fetchone()
            level_result = db.execute(
                "SELECT COUNT(*),COUNT(*)-COUNT(DISTINCT "
                "(frame_id,side,source_level_index)),"
                "SUM(CASE WHEN side NOT IN ('bid','ask') OR action NOT IN "
                "('set','delete') THEN 1 ELSE 0 END) FROM read_parquet(?)",
                [str(level_path)],
            ).fetchone()
            if result is None or level_result is None:
                errors.append(f"输出不可读: {attempt}")
                continue
            frame_count, level_count = int(result[0]), int(level_result[0])
            total_frames += frame_count
            total_levels += level_count
            if frame_count != normalized or int(result[1] or 0) or int(result[2] or 0):
                errors.append(f"frame 键/PIT/计数失败: {attempt}")
            if int(result[3] or 0) != level_count or int(level_result[1] or 0) or int(level_result[2] or 0):
                errors.append(f"level 父子/键/枚举失败: {attempt}")
            if frame_count and (int(result[4]) != 1 or str(result[5]) != market):
                errors.append(f"market 契约失败: {attempt}")
            if frame_count and (int(result[6]) != 1 or str(result[7]) != version):
                errors.append(f"normalization 契约失败: {attempt}")
            if frame_count and version in {
                *SUPERSEDED_L2_NORMALIZATION_VERSIONS,
                L2_NORMALIZATION_VERSION,
            }:
                realtime = db.execute(
                    "SELECT COUNT(DISTINCT schema_version),"
                    "MIN(schema_version),"
                    "SUM(CASE WHEN capability_revision IS NULL OR "
                    "endpoint IS NULL OR payload_schema_version IS NULL OR "
                    "source_session_id IS NULL OR source_level!='L2' OR "
                    "data_quality IS NULL OR raw_payload_sha256 IS NULL OR "
                    "length(raw_payload_sha256)!=64 THEN 1 ELSE 0 END),"
                    "SUM(CASE WHEN connection_id IS NOT NULL AND "
                    "(endpoint_id IS NULL OR channel_id IS NULL OR "
                    "recv_ts_mono_ns IS NULL) THEN 1 ELSE 0 END),"
                    "SUM(CASE WHEN source_session_id!=run_id THEN 1 ELSE 0 END) "
                    ",SUM(CASE WHEN endpoint_revision IS NULL AND "
                    "strpos(data_quality,'endpoint_revision_unrecorded')=0 "
                    "THEN 1 WHEN endpoint_revision IS NOT NULL AND "
                    "endpoint_revision<0 THEN 1 ELSE 0 END) "
                    "FROM read_parquet(?)", [str(frame_path)],
                ).fetchone()
                if (
                    realtime is None or int(realtime[0]) != 1
                    or int(realtime[1]) != L2_SCHEMA_VERSION
                    or int(realtime[2] or 0) or int(realtime[3] or 0)
                    or int(realtime[4] or 0) or int(realtime[5] or 0)
                ):
                    errors.append(f"实时 L2 来源身份/质量契约失败: {attempt}")
    finally:
        db.close()
    return {
        "attempts": len(rows), "frames": total_frames,
        "levels": total_levels, "errors": errors, "ok": not errors,
    }


def _refresh_quality_nonblocking(
    root: Path, conn: sqlite3.Connection,
) -> tuple[dict[str, object] | None, Exception | None]:
    """刷新控制遥测；错误作为值交给 watch 记录，不撤销事实物化。"""
    try:
        from guvolu.data.l2_quality import refresh_recent

        return refresh_recent(root, conn), None
    except Exception as exc:
        return None, exc


def _refresh_market_status_nonblocking(
    root: Path, conn: sqlite3.Connection,
) -> tuple[dict[str, object] | None, Exception | None]:
    """追赶 bitbank 状态事实；失败可观察但不阻断 L2/质量下一轮。"""
    try:
        from guvolu.data.market_status_materialize import (
            materialize_all as materialize_market_status,
        )

        results = materialize_market_status(
            root, conn, report_reused=False
        )
        created = [result for result in results if not result.reused]
        return {
            "candidate_segments": len(results),
            "materialized_now": len(created),
            "observations_now": sum(
                result.observation_rows for result in created
            ),
            "ignored_now": sum(result.ignored_rows for result in created),
            "rejected_now": sum(result.rejected_rows for result in created),
        }, None
    except Exception as exc:
        return None, exc


def _watch(root: Path, interval: float, *, latest_run_only: bool) -> int:
    """持续追赶封口盘口；启动锁竞争只延后本轮。"""
    def report_connect_error(exc: Exception, elapsed: float) -> None:
        print(json.dumps({
            "event": "l2_materialization_startup_error",
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
            cycle_started = time.monotonic()
            try:
                with sqlite_writer_lock(root):
                    cycle = materialize_all(
                        root, conn, report_reused=False,
                        latest_run_only=latest_run_only,
                    )
                    market_status_summary: dict[str, object] | None
                    market_status_error: Exception | None
                    if latest_run_only:
                        market_status_summary = {
                            "candidate_segments": 0,
                            "materialized_now": 0,
                            "deferred": True,
                        }
                        market_status_error = None
                    else:
                        market_status_summary, market_status_error = (
                            _refresh_market_status_nonblocking(root, conn)
                        )
                    # 质量遥测共用写锁。
                    # 已完成事实不阻断追赶。
                    quality_summary, quality_error = (
                        _refresh_quality_nonblocking(root, conn)
                    )
                created = [result for result in cycle if not result.reused]
                if quality_error is not None:
                    print(json.dumps({
                        "event": "l2_quality_window_error",
                        "error": (
                            f"{type(quality_error).__name__}: "
                            f"{quality_error}"
                        ),
                    }, ensure_ascii=False), flush=True)
                if market_status_error is not None:
                    print(json.dumps({
                        "event": "market_status_materialization_error",
                        "error": (
                            f"{type(market_status_error).__name__}: "
                            f"{market_status_error}"
                        ),
                    }, ensure_ascii=False), flush=True)
                print(json.dumps({
                    "event": "l2_materialization_cycle",
                    "sealed_segments": len(cycle),
                    "materialized_now": len(created),
                    "frames_now": sum(
                        result.frame_rows for result in created
                    ),
                    "levels_now": sum(
                        result.level_rows for result in created
                    ),
                    "elapsed_seconds": round(
                        time.monotonic() - cycle_started, 3
                    ),
                    "market_status": market_status_summary,
                    "quality": quality_summary,
                }, ensure_ascii=False), flush=True)
            except (
                OSError, sqlite3.Error, ValueError, duckdb.Error
            ) as exc:
                print(json.dumps({
                    "event": "l2_materialization_error",
                    "error": f"{type(exc).__name__}: {exc}",
                }, ensure_ascii=False), flush=True)
            time.sleep(interval)
    except KeyboardInterrupt:
        print("L2 增量物化已停止", flush=True)
        return 0
    finally:
        if conn is not None:
            conn.close()


def main(argv: Sequence[str] | None = None) -> int:
    """命令行入口。"""
    parser = argparse.ArgumentParser(description="三所 L2 segment 物化")
    parser.add_argument("--data-root", type=Path, default=None)
    sub = parser.add_subparsers(dest="command", required=True)
    all_parser = sub.add_parser("all", help="物化全部封口 segment")
    all_parser.add_argument("--latest-run-only", action="store_true")
    sub.add_parser("audit", help="审计活动 L2 输出")
    watch = sub.add_parser("watch", help="周期追赶新封口 segment")
    watch.add_argument("--interval-seconds", type=float, default=300.0)
    watch.add_argument("--latest-run-only", action="store_true")
    args = parser.parse_args(argv)
    root = (args.data_root or configured_data_root()).resolve()
    if args.command == "watch":
        interval = float(args.interval_seconds)
        if interval < 10:
            raise ValueError("interval-seconds 不得小于 10")
        return _watch(
            root, interval, latest_run_only=bool(args.latest_run_only),
        )

    conn = store.connect(root)
    try:
        if args.command == "all":
            with sqlite_writer_lock(root):
                completed = materialize_all(
                    root, conn,
                    latest_run_only=bool(args.latest_run_only),
                )
            results: object = [asdict(result) for result in completed]
            code = 0
        elif args.command == "audit":
            results = audit_l2(root, conn)
            code = 0 if bool(results["ok"]) else 1
        else:
            raise AssertionError(f"未知命令: {args.command}")
    finally:
        conn.close()
    print(json.dumps(results, ensure_ascii=False, indent=2))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
