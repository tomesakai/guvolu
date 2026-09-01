"""封口实时 segment 到 ``book_l2_frame``/``book_l2_level`` Parquet。"""
from __future__ import annotations

import argparse
import csv
import hashlib
import importlib
import json
import os
import sqlite3
import sys
import time
import uuid
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO, cast

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
class _BoundedRunContract:
    """用于 bounded 选择的稳定 run 身份快照。"""

    directory: Path
    state_path: Path
    state_bytes: bytes
    body: Mapping[str, object]
    started_at: datetime
    open_run: bool


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


@dataclass(frozen=True)
class ScanStats:
    """一轮输入选择的扫描成本，用于发现语料线性退化。"""

    scanned_manifests: int
    hash_recomputed: int
    hash_reused: int
    elapsed_scan_seconds: float


def _registered_input_hashes(
    conn: sqlite3.Connection,
) -> dict[str, tuple[str, int]]:
    """取已完成 L2 attempt 输入制品的登记散列与字节数。"""
    rows = conn.execute(
        "SELECT DISTINCT r.storage_path,r.sha256,r.byte_count FROM artifact r "
        "JOIN partition_input i ON i.artifact_id=r.artifact_id "
        "JOIN partition_attempt a ON a.attempt_id=i.attempt_id "
        "WHERE a.domain='book_l2' AND a.status LIKE 'complete%' "
        "AND r.artifact_kind='raw_realtime_segment'"
    ).fetchall()
    return {
        str(row[0]): (str(row[1]), int(row[2])) for row in rows
    }


def _sealed_inputs(
    root: Path, *, latest_run_only: bool = False,
    latest_sealed_segments_per_stream: int | None = None,
    registered_hashes: Mapping[str, tuple[str, int]] | None = None,
) -> list[SegmentInput]:
    return _scan_sealed_inputs(
        root,
        latest_run_only=latest_run_only,
        latest_sealed_segments_per_stream=(
            latest_sealed_segments_per_stream
        ),
        registered_hashes=registered_hashes,
    )[0]


def _scan_sealed_inputs(
    root: Path, *, latest_run_only: bool = False,
    latest_sealed_segments_per_stream: int | None = None,
    registered_hashes: Mapping[str, tuple[str, int]] | None = None,
) -> tuple[list[SegmentInput], ScanStats]:
    """选择封口 L2 segment，并报告本轮扫描成本。

    ``registered_hashes`` 为控制面预筛：键是输入制品的登记 storage_path，
    值是该制品已随某个完成态 book_l2 attempt 登记的散列与字节数。命中且
    磁盘字节数、manifest 散列与字节数三者一致时复用登记散列，不再重算
    SHA-256；任一不一致即抛错，不退化为静默重算。传 ``None`` 表示回到
    逐个重算，用于审计。
    """
    started = time.monotonic()
    scanned = 0
    recomputed = 0
    reused = 0
    _validate_input_selection(
        latest_run_only=latest_run_only,
        latest_sealed_segments_per_stream=(
            latest_sealed_segments_per_stream
        ),
    )
    inputs: list[SegmentInput] = []
    base = _resolve_recorded_path(root, "raw/realtime/book_l2")
    if not base.is_dir():
        return [], ScanStats(0, 0, 0, round(time.monotonic() - started, 3))
    manifests: list[tuple[Path, Mapping[str, object]]]
    if latest_sealed_segments_per_stream is not None:
        manifests = _latest_sealed_manifest_entries(
            base, latest_sealed_segments_per_stream,
        )
        scanned = len(manifests)
    else:
        manifest_paths: list[Path]
        if latest_run_only:
            manifest_paths = sorted(
                path
                for directory in _latest_run_directories(base).values()
                for path in directory.glob("segment-*.manifest.json")
            )
        else:
            manifest_paths = sorted(base.rglob("segment-*.manifest.json"))
        manifests = []
        for manifest_path in manifest_paths:
            scanned += 1
            body = json.loads(manifest_path.read_text(encoding="utf-8"))
            if not isinstance(body, Mapping):
                continue
            if (
                body.get("status") != "sealed"
                or body.get("completion_claim") is not True
            ):
                continue
            manifests.append((manifest_path, body))
    for manifest_path, body in manifests:
        recorded = str(body["storage_path"])
        relative = PurePosixPath(recorded)
        if (
            relative.is_absolute()
            or relative.as_posix() != recorded
            or relative.parts[:3] != ("raw", "realtime", "book_l2")
            or ".." in relative.parts
        ):
            raise ValueError(f"segment 逻辑路径非法: {recorded}")
        expected_logical_path = manifest_path.with_name(
            manifest_path.name.removesuffix(".manifest.json") + ".jsonl"
        )
        expected_logical = PurePosixPath(
            "raw/realtime/book_l2",
            expected_logical_path.relative_to(base).as_posix(),
        )
        if relative != expected_logical:
            raise ValueError(f"manifest 与 segment 不同目录: {recorded}")
        expected_path = expected_logical_path.resolve()
        try:
            expected_path.relative_to(base)
        except ValueError as exc:
            raise ValueError(f"segment 路径越界: {recorded}") from exc
        path = _resolve_recorded_path(root, recorded)
        if path != expected_path:
            raise ValueError(f"manifest 与 segment 不同目录: {recorded}")
        recorded_sha = str(body["sha256"])
        recorded_bytes = int(str(body["byte_count"]))
        if path.stat().st_size != recorded_bytes:
            raise ValueError(f"segment 字节数不符: {recorded}")
        registered = (
            None if registered_hashes is None
            else registered_hashes.get(recorded)
        )
        if registered is None:
            sha = sha256_file(path)
            recomputed += 1
            if sha != recorded_sha:
                raise ValueError(f"segment 散列不符: {recorded}")
        else:
            if registered != (recorded_sha, recorded_bytes):
                raise ValueError(f"segment 登记散列不符: {recorded}")
            sha = recorded_sha
            reused += 1
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
    return inputs, ScanStats(
        scanned, recomputed, reused,
        round(time.monotonic() - started, 3),
    )


def _validate_input_selection(
    *,
    latest_run_only: bool,
    latest_sealed_segments_per_stream: int | None,
) -> None:
    """拒绝含糊或无界的增量选择参数。"""
    limit = latest_sealed_segments_per_stream
    if limit is not None and (
        isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0
    ):
        raise ValueError("latest-sealed-segments-per-stream 必须为正整数")
    if latest_run_only and limit is not None:
        raise ValueError(
            "latest-run-only 与 latest-sealed-segments-per-stream 互斥"
        )


def _json_object_snapshot(
    path: Path, label: str,
) -> tuple[bytes, Mapping[str, object]]:
    """单次读取 JSON 对象，供随后逐字节稳定性复核。"""
    try:
        raw = path.read_bytes()
        loaded: object = json.loads(raw)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"{label} 无法读取: {path}") from exc
    if not isinstance(loaded, Mapping):
        raise ValueError(f"{label} 结构非法: {path}")
    if any(not isinstance(key, str) for key in loaded):
        raise ValueError(f"{label} 键类型非法: {path}")
    return raw, cast(Mapping[str, object], loaded)


def _bounded_integer(
    value: object, label: str, *, minimum: int = 0,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{label} 必须为整数")
    if value < minimum:
        raise ValueError(f"{label} 不得小于 {minimum}")
    return value


def _bounded_run_contract(
    run_directory: Path,
    *,
    venue_id: str,
    venue_symbol: str,
) -> _BoundedRunContract:
    """读取 checkpoint 或 terminal run manifest 的唯一身份合同。

    checkpoint 是可变文件，run manifest 也尚无自身内容散列；因此这里不把
    两者提升为不可伪造事实，只把逐字节稳定快照与严格身份/时序作为 bounded
    模式的最低失败关闭边界。生产格式升级前仍不能据此声称 run liveness。
    """
    run_id = run_directory.name.split("=", 1)[1]
    if not run_id:
        raise ValueError(f"bounded freshness run 目录非法: {run_directory}")
    checkpoint = run_directory / "checkpoint.json"
    terminal = run_directory / "run.manifest.json"
    states = [path for path in (checkpoint, terminal) if path.is_file()]
    if len(states) != 1:
        raise ValueError(
            "bounded freshness run 状态合同不唯一: "
            f"{run_directory}"
        )
    state_path = states[0]
    state_bytes, body = _json_object_snapshot(
        state_path, "bounded freshness run 状态合同",
    )
    if (
        body.get("run_id") != run_id
        or body.get("venue_id") != venue_id
        or body.get("venue_symbol") != venue_symbol
        or body.get("domain") != "book_l2"
    ):
        raise ValueError(
            "bounded freshness run 状态身份与路径不一致: "
            f"{state_path}"
        )
    run_schema_version = _bounded_integer(
        body.get("schema_version"), "bounded run schema_version", minimum=1,
    )
    if run_schema_version != 3:
        raise ValueError(
            f"bounded freshness run schema_version 非法: {state_path}"
        )
    run_endpoint_revision = body.get("endpoint_revision")
    if run_endpoint_revision is not None:
        _bounded_integer(
            run_endpoint_revision,
            "bounded run endpoint_revision",
        )
    started_at = _time(
        body.get("started_at"), "bounded run started_at",
    )
    if state_path == checkpoint:
        if body.get("status") != "open":
            raise ValueError(
                f"bounded freshness checkpoint 状态非法: {state_path}"
            )
        checkpoint_at = _time(
            body.get("checkpoint_at"), "bounded checkpoint_at",
        )
        if checkpoint_at < started_at:
            raise ValueError(
                f"bounded freshness checkpoint 时序倒置: {state_path}"
            )
        _bounded_integer(
            body.get("sealed_segments"),
            "bounded checkpoint sealed_segments",
        )
        _bounded_integer(body.get("records"), "bounded checkpoint records")
        return _BoundedRunContract(
            run_directory, state_path, state_bytes, body, started_at, True,
        )
    status = body.get("status")
    if status not in {"complete", "interrupted", "failed"}:
        raise ValueError(
            f"bounded freshness run manifest 状态非法: {state_path}"
        )
    if body.get("completion_claim") is not (status == "complete"):
        raise ValueError(
            f"bounded freshness run completion_claim 非法: {state_path}"
        )
    finished_at = _time(body.get("finished_at"), "bounded run finished_at")
    if finished_at < started_at:
        raise ValueError(
            f"bounded freshness run manifest 时序倒置: {state_path}"
        )
    _bounded_integer(body.get("segment_count"), "bounded run segment_count")
    _bounded_integer(body.get("record_count"), "bounded run record_count")
    if not isinstance(body.get("segments"), list):
        raise ValueError(
            f"bounded freshness run receipts 结构非法: {state_path}"
        )
    return _BoundedRunContract(
        run_directory, state_path, state_bytes, body, started_at, False,
    )


def _run_rank(run_directory: Path) -> tuple[int, int, str]:
    run_id = run_directory.name.split("=", 1)[1]
    if not run_id:
        raise ValueError(f"L2 run 目录非法: {run_directory}")
    open_run = int(
        (run_directory / "checkpoint.json").is_file()
        and next(run_directory.glob("segment-*.open"), None) is not None
    )
    return open_run, run_directory.stat().st_mtime_ns, run_id


def _latest_stream_run_directories(
    base: Path,
) -> dict[tuple[str, str], Path]:
    """按规范目录深度定位每个 (venue,symbol) 流的最新运行。"""
    latest: dict[tuple[str, str], tuple[int, int, str, Path]] = {}
    for venue_directory in sorted(base.glob("venue_id=*")):
        if not venue_directory.is_dir():
            continue
        venue_id = venue_directory.name.split("=", 1)[1]
        if not venue_id:
            raise ValueError(f"L2 venue 目录非法: {venue_directory}")
        for symbol_directory in sorted(
            venue_directory.glob("venue_symbol=*")
        ):
            if not symbol_directory.is_dir():
                continue
            venue_symbol = symbol_directory.name.split("=", 1)[1]
            if not venue_symbol:
                raise ValueError(
                    f"L2 symbol 目录非法: {symbol_directory}"
                )
            stream = (venue_id, venue_symbol)
            for run_directory in sorted(symbol_directory.glob("run_id=*")):
                if not run_directory.is_dir():
                    continue
                candidate = (*_run_rank(run_directory), run_directory)
                if candidate[:3] > latest.get(
                    stream, (-1, -1, "", run_directory),
                )[:3]:
                    latest[stream] = candidate
    return {stream: row[3] for stream, row in latest.items()}


def _bounded_latest_stream_run_contracts(
    base: Path,
) -> dict[tuple[str, str], _BoundedRunContract]:
    """按合同 started_at，而非目录 mtime，选择每个流的最新 run。"""
    latest: dict[
        tuple[str, str], tuple[datetime, str, _BoundedRunContract]
    ] = {}
    for venue_directory in sorted(base.glob("venue_id=*")):
        if not venue_directory.is_dir():
            continue
        venue_id = venue_directory.name.split("=", 1)[1]
        if not venue_id:
            raise ValueError(f"L2 venue 目录非法: {venue_directory}")
        for symbol_directory in sorted(
            venue_directory.glob("venue_symbol=*")
        ):
            if not symbol_directory.is_dir():
                continue
            venue_symbol = symbol_directory.name.split("=", 1)[1]
            if not venue_symbol:
                raise ValueError(
                    f"L2 symbol 目录非法: {symbol_directory}"
                )
            stream = (venue_id, venue_symbol)
            seen_run_directory = False
            for run_directory in sorted(symbol_directory.glob("run_id=*")):
                if not run_directory.is_dir():
                    continue
                seen_run_directory = True
                if _is_legacy_run_directory(run_directory):
                    # 旧版 run 不参选
                    continue
                contract = _bounded_run_contract(
                    run_directory,
                    venue_id=venue_id,
                    venue_symbol=venue_symbol,
                )
                run_id = run_directory.name.split("=", 1)[1]
                candidate = (contract.started_at, run_id, contract)
                prior = latest.get(stream)
                if prior is None or candidate[:2] > prior[:2]:
                    latest[stream] = candidate
            if seen_run_directory and stream not in latest:
                raise ValueError(
                    f"流没有任何 v3 run 候选: {venue_id}/{venue_symbol}"
                )
    return {stream: row[2] for stream, row in latest.items()}


def _is_legacy_run_directory(run_directory: Path) -> bool:
    """判定 run 状态合同是否显式声明为 v3 之前的旧版。

    仅当恰有一个状态文件、可解析为 JSON 对象且 schema_version 为
    小于 3 的整数时判为旧版；其余情形一律交由严格合同失败关闭，
    不得借本判定掩盖真实损坏。某流全部 run 均为旧版时选择器无
    候选，仍会响亮失败。
    """
    checkpoint = run_directory / "checkpoint.json"
    terminal = run_directory / "run.manifest.json"
    states = [path for path in (checkpoint, terminal) if path.is_file()]
    if len(states) != 1:
        return False
    try:
        body = json.loads(states[0].read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    if not isinstance(body, Mapping):
        return False
    version = body.get("schema_version")
    return (
        isinstance(version, int)
        and not isinstance(version, bool)
        and version < 3
    )


def _latest_run_directories(base: Path) -> dict[str, Path]:
    """保留 legacy latest-run-only 的每所单运行语义。"""
    latest: dict[str, tuple[int, int, str, Path]] = {}
    for (venue_id, _), run_directory in (
        _latest_stream_run_directories(base).items()
    ):
        candidate = (*_run_rank(run_directory), run_directory)
        if candidate[:3] > latest.get(
            venue_id, (-1, -1, "", run_directory),
        )[:3]:
            latest[venue_id] = candidate
    return {venue_id: row[3] for venue_id, row in latest.items()}


def _manifest_segment_sequence(manifest_path: Path) -> int:
    name = manifest_path.name
    prefix = "segment-"
    suffix = ".manifest.json"
    raw_sequence = name.removeprefix(prefix).removesuffix(suffix)
    if (
        not name.startswith(prefix)
        or not name.endswith(suffix)
        or not raw_sequence.isdigit()
        or int(raw_sequence) <= 0
    ):
        raise ValueError(f"segment manifest 文件名非法: {manifest_path}")
    return int(raw_sequence)


def _terminal_run_receipts(
    contract: _BoundedRunContract,
    base: Path,
) -> dict[int, Mapping[str, object]]:
    raw_receipts = contract.body.get("segments")
    if not isinstance(raw_receipts, list):
        raise ValueError(
            "bounded freshness terminal run receipts 结构非法: "
            f"{contract.state_path}"
        )
    expected_count = _bounded_integer(
        contract.body.get("segment_count"),
        "bounded terminal segment_count",
    )
    if expected_count != len(raw_receipts):
        raise ValueError(
            "bounded freshness terminal run receipt 数量不闭合: "
            f"{contract.state_path}"
        )
    run_relative = contract.directory.relative_to(base).as_posix()
    receipts: dict[int, Mapping[str, object]] = {}
    total_records = 0
    for expected_sequence, loaded in enumerate(raw_receipts, start=1):
        if not isinstance(loaded, Mapping) or any(
            not isinstance(key, str) for key in loaded
        ):
            raise ValueError(
                "bounded freshness terminal receipt 结构非法: "
                f"{contract.state_path}"
            )
        receipt = cast(Mapping[str, object], loaded)
        sequence = _bounded_integer(
            receipt.get("segment_sequence"),
            "bounded terminal receipt segment_sequence",
            minimum=1,
        )
        if sequence != expected_sequence or sequence in receipts:
            raise ValueError(
                "bounded freshness terminal receipt 序列不连续: "
                f"{contract.state_path}"
            )
        storage_path = (
            "raw/realtime/book_l2/"
            f"{run_relative}/segment-{sequence:06d}.jsonl"
        )
        manifest_path = (
            "raw/realtime/book_l2/"
            f"{run_relative}/segment-{sequence:06d}.manifest.json"
        )
        if (
            receipt.get("storage_path") != storage_path
            or receipt.get("manifest_path") != manifest_path
        ):
            raise ValueError(
                "bounded freshness terminal receipt 路径非法: "
                f"{contract.state_path}"
            )
        _bounded_integer(
            receipt.get("byte_count"),
            "bounded terminal receipt byte_count",
            minimum=1,
        )
        records = _bounded_integer(
            receipt.get("record_count"),
            "bounded terminal receipt record_count",
            minimum=1,
        )
        total_records += records
        sha = receipt.get("sha256")
        if (
            not isinstance(sha, str)
            or len(sha) != 64
            or receipt.get("artifact_id") != f"sha256-{sha}"
        ):
            raise ValueError(
                "bounded freshness terminal receipt 散列身份非法: "
                f"{contract.state_path}"
            )
        try:
            int(sha, 16)
        except ValueError as exc:
            raise ValueError(
                "bounded freshness terminal receipt SHA-256 非法: "
                f"{contract.state_path}"
            ) from exc
        for key in ("first_ingest_time", "last_ingest_time"):
            if not isinstance(receipt.get(key), str):
                raise ValueError(
                    f"bounded freshness terminal receipt 缺 {key}: "
                    f"{contract.state_path}"
                )
        receipts[sequence] = receipt
    if total_records != _bounded_integer(
        contract.body.get("record_count"),
        "bounded terminal record_count",
    ):
        raise ValueError(
            "bounded freshness terminal receipt 行数不闭合: "
            f"{contract.state_path}"
        )
    return receipts


def _latest_sealed_manifest_entries(
    base: Path, limit: int,
) -> list[tuple[Path, Mapping[str, object]]]:
    """按 run 合同与单调 segment receipt 取每流最新 N 片。

    当前 run/checkpoint 格式没有状态文件自身的内容寻址身份，也没有把
    ``sealed_at`` 写入 terminal run receipt。因此本模式只能验证稳定字节快照、
    数据散列绑定和可复核时序，不能把 checkpoint 新鲜度等同于 collector 存活。
    任何状态身份、计数、时序或扫描期间稳定性含糊都会失败关闭。
    """
    selected: list[
        tuple[str, str, int, str, Path, Mapping[str, object]]
    ] = []
    for (venue_id, venue_symbol), contract in sorted(
        _bounded_latest_stream_run_contracts(base).items()
    ):
        run_directory = contract.directory
        receipts = (
            {} if contract.open_run
            else _terminal_run_receipts(contract, base)
        )
        candidates: list[tuple[int, str, Path, Mapping[str, object]]] = []
        manifest_snapshots: dict[Path, bytes] = {}
        for manifest_path in sorted(
            run_directory.glob("segment-*.manifest.json")
        ):
            raw, body = _json_object_snapshot(
                manifest_path, "bounded freshness manifest",
            )
            manifest_snapshots[manifest_path] = raw
            if (
                body.get("status") != "sealed"
                or body.get("completion_claim") is not True
            ):
                raise ValueError(
                    "bounded freshness latest run 含非完整 segment: "
                    f"{manifest_path}"
                )
            segment_sequence = _manifest_segment_sequence(manifest_path)
            if body.get("sealed_at") is None:
                raise ValueError(
                    "bounded freshness manifest 缺 sealed_at: "
                    f"{manifest_path}"
                )
            sealed_at = _time(body["sealed_at"], "sealed_at")
            segment_started_at = _time(
                body.get("started_at"), "segment started_at",
            )
            first_ingest = _time(
                body.get("first_ingest_time"), "first_ingest_time",
            )
            last_ingest = _time(
                body.get("last_ingest_time"), "last_ingest_time",
            )
            recorded_sequence = _bounded_integer(
                body.get("segment_sequence"),
                "bounded manifest segment_sequence",
                minimum=1,
            )
            _bounded_integer(
                body.get("byte_count"),
                "bounded manifest byte_count",
                minimum=1,
            )
            _bounded_integer(
                body.get("record_count"),
                "bounded manifest record_count",
                minimum=1,
            )
            manifest_schema_version = _bounded_integer(
                body.get("schema_version", 1),
                "bounded manifest schema_version",
                minimum=1,
            )
            if manifest_schema_version not in {1, 2, 3}:
                raise ValueError(
                    "bounded freshness manifest schema_version 非法: "
                    f"{manifest_path}"
                )
            manifest_endpoint_revision = body.get("endpoint_revision")
            if manifest_endpoint_revision is not None:
                _bounded_integer(
                    manifest_endpoint_revision,
                    "bounded manifest endpoint_revision",
                )
            if (
                body.get("venue_id") != venue_id
                or body.get("venue_symbol") != venue_symbol
                or str(body.get("run_id"))
                != run_directory.name.split("=", 1)[1]
                or recorded_sequence != segment_sequence
            ):
                raise ValueError(
                    "bounded freshness manifest 身份与路径不一致: "
                    f"{manifest_path}"
                )
            if body.get("domain") != "book_l2":
                raise ValueError(
                    "bounded freshness manifest domain 非法: "
                    f"{manifest_path}"
                )
            if any(
                body.get(key) != contract.body.get(key)
                for key in ("endpoint_id", "endpoint_revision")
            ):
                raise ValueError(
                    "bounded freshness manifest 与 run 端点身份不一致: "
                    f"{manifest_path}"
                )
            if (
                first_ingest > last_ingest
                or segment_started_at < contract.started_at
                or first_ingest < contract.started_at
                or sealed_at < segment_started_at
                or sealed_at < first_ingest
                or sealed_at < last_ingest
            ):
                raise ValueError(
                    "bounded freshness manifest 时序倒置: "
                    f"{manifest_path}"
                )
            receipt = receipts.get(segment_sequence)
            if not contract.open_run:
                if receipt is None:
                    raise ValueError(
                        "bounded freshness manifest 缺 terminal receipt: "
                        f"{manifest_path}"
                    )
                for key in (
                    "artifact_id", "sha256", "byte_count", "record_count",
                    "storage_path", "first_ingest_time", "last_ingest_time",
                ):
                    if body.get(key) != receipt.get(key):
                        raise ValueError(
                            "bounded freshness manifest 与 terminal receipt "
                            f"不一致: {manifest_path}: {key}"
                        )
            stable_path = manifest_path.relative_to(base).as_posix()
            candidates.append(
                (segment_sequence, stable_path, manifest_path, body)
            )

        candidates.sort(key=lambda row: (row[0], row[1]))
        sequences = [row[0] for row in candidates]
        if sequences != list(range(1, len(candidates) + 1)):
            raise ValueError(
                "bounded freshness latest run segment 序列不连续: "
                f"{run_directory}"
            )
        previous_sealed: datetime | None = None
        previous_started: datetime | None = None
        total_records = 0
        for _, _, manifest_path, body in candidates:
            sealed_at = _time(body.get("sealed_at"), "sealed_at")
            segment_started_at = _time(
                body.get("started_at"), "segment started_at",
            )
            if (
                previous_sealed is not None
                and sealed_at < previous_sealed
            ) or (
                previous_started is not None
                and segment_started_at < previous_started
            ):
                raise ValueError(
                    "bounded freshness latest run segment 时间非单调: "
                    f"{manifest_path}"
                )
            previous_sealed = sealed_at
            previous_started = segment_started_at
            total_records += _bounded_integer(
                body.get("record_count"),
                "bounded manifest record_count",
                minimum=1,
            )
        if contract.open_run:
            checkpoint_segments = _bounded_integer(
                contract.body.get("sealed_segments"),
                "bounded checkpoint sealed_segments",
            )
            checkpoint_records = _bounded_integer(
                contract.body.get("records"),
                "bounded checkpoint records",
            )
            if (
                checkpoint_segments != len(candidates)
                or checkpoint_records < total_records
            ):
                raise ValueError(
                    "bounded freshness checkpoint 与 segment 计数不闭合: "
                    f"{contract.state_path}"
                )
            checkpoint_at = _time(
                contract.body.get("checkpoint_at"),
                "bounded checkpoint_at",
            )
            if previous_sealed is not None and checkpoint_at < previous_sealed:
                raise ValueError(
                    "bounded freshness checkpoint 早于 segment seal: "
                    f"{contract.state_path}"
                )
        else:
            if len(receipts) != len(candidates):
                raise ValueError(
                    "bounded freshness terminal receipt 未全部落盘: "
                    f"{contract.state_path}"
                )
            finished_at = _time(
                contract.body.get("finished_at"), "bounded run finished_at",
            )
            if previous_sealed is not None and finished_at < previous_sealed:
                raise ValueError(
                    "bounded freshness run finish 早于 segment seal: "
                    f"{contract.state_path}"
                )
        for path, raw in manifest_snapshots.items():
            if path.read_bytes() != raw:
                raise ValueError(
                    "bounded freshness manifest 在选择期间变化: "
                    f"{path}"
                )
        if contract.state_path.read_bytes() != contract.state_bytes:
            raise ValueError(
                "bounded freshness run 状态在选择期间变化: "
                f"{contract.state_path}"
            )
        newest = candidates[-limit:]
        selected.extend(
            (
                venue_id, venue_symbol, segment_sequence,
                stable_path, path, body,
            )
            for segment_sequence, stable_path, path, body in newest
        )
    selected.sort(key=lambda row: row[:4])
    return [(path, body) for _, _, _, _, path, body in selected]


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
    latest_sealed_segments_per_stream: int | None = None,
    verify_all_hashes: bool = False,
) -> list[L2Result]:
    """断点复用地物化全部已封口 L2 segment。"""
    return _materialize_cycle(
        root, conn, report_reused=report_reused,
        latest_run_only=latest_run_only,
        latest_sealed_segments_per_stream=(
            latest_sealed_segments_per_stream
        ),
        verify_all_hashes=verify_all_hashes,
    )[0]


def _materialize_cycle(
    root: Path,
    conn: sqlite3.Connection,
    *,
    report_reused: bool = True,
    latest_run_only: bool = False,
    latest_sealed_segments_per_stream: int | None = None,
    verify_all_hashes: bool = False,
) -> tuple[list[L2Result], ScanStats]:
    """物化一轮并返回扫描成本。"""
    inputs, stats = _scan_sealed_inputs(
        root,
        latest_run_only=latest_run_only,
        latest_sealed_segments_per_stream=(
            latest_sealed_segments_per_stream
        ),
        registered_hashes=(
            None if verify_all_hashes else _registered_input_hashes(conn)
        ),
    )
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
    return results, stats


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
    """刷新控制遥测；错误作为值交给 watch 记录，不撤销事实物化。

    质量计算耗时且只读事实，在写锁外执行；仅 upsert 遥测行时
    短暂取写锁，避免长时间占锁饿死其他写者（R-04 保守取向）。
    """
    try:
        from collections import defaultdict

        from guvolu.data.l2_quality import (
            MATERIALIZED_FRESH_SECONDS,
            QUALITY_VERSION,
            _floor_window,
            compute_quality_windows,
            upsert_quality_windows,
        )

        minutes = 20
        finished = datetime.now(UTC)
        windows = compute_quality_windows(
            root, conn, finished - timedelta(minutes=minutes), finished,
            computed_at=finished,
        )
        with sqlite_writer_lock(root):
            upserted = upsert_quality_windows(conn, windows)
        counts: dict[str, int] = defaultdict(int)
        for row in windows:
            counts[row.status] += 1
        return {
            "quality_version": QUALITY_VERSION,
            "materialized_freshness_threshold_seconds": (
                MATERIALIZED_FRESH_SECONDS
            ),
            "from": _floor_window(
                finished - timedelta(minutes=minutes)
            ).isoformat(),
            "to": finished.isoformat(),
            "windows": len(windows),
            "upserted": upserted,
            "status_counts": dict(sorted(counts.items())),
        }, None
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


def _watch_selection(
    *,
    latest_run_only: bool,
    latest_sealed_segments_per_stream: int | None,
) -> str:
    _validate_input_selection(
        latest_run_only=latest_run_only,
        latest_sealed_segments_per_stream=(
            latest_sealed_segments_per_stream
        ),
    )
    if latest_run_only:
        return "latest_run"
    if latest_sealed_segments_per_stream is not None:
        return (
            "latest_sealed_per_stream:"
            f"{latest_sealed_segments_per_stream}"
        )
    return "all"


def _try_l2_owner_lock(stream: BinaryIO) -> bool:
    stream.seek(0)
    if os.name == "nt":
        import msvcrt

        try:
            msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError:
            return False
        return True
    fcntl: Any = importlib.import_module("fcntl")
    try:
        fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return False
    return True


def _release_l2_owner_lock(stream: BinaryIO) -> None:
    stream.seek(0)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
        return
    fcntl: Any = importlib.import_module("fcntl")
    fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def _l2_owner_paths(root: Path) -> tuple[Path, Path]:
    directory = root / ".locks"
    return (
        directory / "l2-materializer-owner.lock",
        directory / "l2-materializer-owner.json",
    )


@contextmanager
def _l2_watch_owner(
    root: Path, *, selection: str,
) -> Iterator[Mapping[str, object]]:
    """Nonblocking singleton ownership with an atomic, fixed truth record."""
    lock_path, owner_path = _l2_owner_paths(root)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+b") as stream:
        stream.seek(0, os.SEEK_END)
        if stream.tell() == 0:
            stream.write(b"\0")
            stream.flush()
            os.fsync(stream.fileno())
        if not _try_l2_owner_lock(stream):
            raise RuntimeError(
                "L2 watch singleton is already owned for data-root: "
                f"{root.resolve()}"
            )
        try:
            owner_path.unlink()
        except FileNotFoundError:
            pass
        nonce = uuid.uuid4().hex
        owner: Mapping[str, object] = {
            "schema_version": 1,
            "pid": os.getpid(),
            "selection": selection,
            "data_root": str(root.resolve(strict=True)),
            "executable_path": str(
                Path(
                    getattr(sys, "_base_executable", sys.executable)
                ).resolve(strict=True)
            ),
            "started_at": datetime.now(UTC).isoformat(),
            "nonce": nonce,
        }
        try:
            atomic_write_text(
                owner_path,
                json.dumps(
                    owner,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ) + "\n",
            )
            yield owner
        finally:
            try:
                loaded: object = json.loads(
                    owner_path.read_text(encoding="utf-8")
                )
                if (
                    isinstance(loaded, Mapping)
                    and loaded.get("pid") == os.getpid()
                    and loaded.get("nonce") == nonce
                ):
                    owner_path.unlink()
            except (OSError, UnicodeError, json.JSONDecodeError):
                pass
            _release_l2_owner_lock(stream)


def _watch(
    root: Path,
    interval: float,
    *,
    latest_run_only: bool,
    latest_sealed_segments_per_stream: int | None,
    verify_all_hashes: bool = False,
) -> int:
    selection = _watch_selection(
        latest_run_only=latest_run_only,
        latest_sealed_segments_per_stream=(
            latest_sealed_segments_per_stream
        ),
    )
    with _l2_watch_owner(root, selection=selection):
        return _watch_as_owner(
            root,
            interval,
            latest_run_only=latest_run_only,
            latest_sealed_segments_per_stream=(
                latest_sealed_segments_per_stream
            ),
            verify_all_hashes=verify_all_hashes,
        )


def _watch_as_owner(
    root: Path,
    interval: float,
    *,
    latest_run_only: bool,
    latest_sealed_segments_per_stream: int | None,
    verify_all_hashes: bool = False,
) -> int:
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
                    cycle, stats = _materialize_cycle(
                        root, conn, report_reused=False,
                        latest_run_only=latest_run_only,
                        latest_sealed_segments_per_stream=(
                            latest_sealed_segments_per_stream
                        ),
                        verify_all_hashes=verify_all_hashes,
                    )
                    market_status_summary: dict[str, object] | None
                    market_status_error: Exception | None
                    if (
                        latest_run_only
                        or latest_sealed_segments_per_stream is not None
                    ):
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
                # 质量锁外计算，upsert 短暂取锁
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
                    "input_selection": (
                        "latest_run"
                        if latest_run_only
                        else "latest_sealed_per_stream"
                        if latest_sealed_segments_per_stream is not None
                        else "all"
                    ),
                    "latest_sealed_segments_per_stream": (
                        latest_sealed_segments_per_stream
                    ),
                    "verify_all_hashes": verify_all_hashes,
                    "sealed_segments": len(cycle),
                    "materialized_now": len(created),
                    "frames_now": sum(
                        result.frame_rows for result in created
                    ),
                    "levels_now": sum(
                        result.level_rows for result in created
                    ),
                    "scanned_manifests": stats.scanned_manifests,
                    "hash_recomputed": stats.hash_recomputed,
                    "hash_reused": stats.hash_reused,
                    "elapsed_scan_seconds": stats.elapsed_scan_seconds,
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


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("必须为正整数") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("必须为正整数")
    return parsed


def main(argv: Sequence[str] | None = None) -> int:
    """命令行入口。"""
    parser = argparse.ArgumentParser(
        description="三所 L2 segment 物化",
        allow_abbrev=False,
    )
    parser.add_argument("--data-root", type=Path, default=None)
    sub = parser.add_subparsers(dest="command", required=True)
    all_parser = sub.add_parser(
        "all", help="物化全部封口 segment", allow_abbrev=False,
    )
    all_selection = all_parser.add_mutually_exclusive_group()
    all_selection.add_argument("--latest-run-only", action="store_true")
    all_selection.add_argument(
        "--latest-sealed-segments-per-stream",
        type=_positive_int,
        help=(
            "每个 (venue,symbol) 最新 run 仅物化按 sealed_at 最新的 N 片"
        ),
    )
    all_parser.add_argument(
        "--verify-all-hashes", action="store_true",
        help="关闭控制面散列预筛，逐个重算输入散列",
    )
    sub.add_parser("audit", help="审计活动 L2 输出", allow_abbrev=False)
    watch = sub.add_parser(
        "watch", help="周期追赶新封口 segment", allow_abbrev=False,
    )
    watch.add_argument("--interval-seconds", type=float, default=300.0)
    watch_selection = watch.add_mutually_exclusive_group()
    watch_selection.add_argument("--latest-run-only", action="store_true")
    watch_selection.add_argument(
        "--latest-sealed-segments-per-stream",
        type=_positive_int,
        help=(
            "每个 (venue,symbol) 最新 run 仅追赶按 sealed_at 最新的 N 片"
        ),
    )
    watch.add_argument(
        "--verify-all-hashes", action="store_true",
        help="关闭控制面散列预筛，逐个重算输入散列",
    )
    args = parser.parse_args(argv)
    root = (args.data_root or configured_data_root()).resolve()
    if args.command == "watch":
        interval = float(args.interval_seconds)
        if interval < 10:
            raise ValueError("interval-seconds 不得小于 10")
        return _watch(
            root,
            interval,
            latest_run_only=bool(args.latest_run_only),
            latest_sealed_segments_per_stream=(
                args.latest_sealed_segments_per_stream
            ),
            verify_all_hashes=bool(args.verify_all_hashes),
        )

    conn = store.connect(root)
    try:
        if args.command == "all":
            with sqlite_writer_lock(root):
                completed = materialize_all(
                    root, conn,
                    latest_run_only=bool(args.latest_run_only),
                    latest_sealed_segments_per_stream=(
                        args.latest_sealed_segments_per_stream
                    ),
                    verify_all_hashes=bool(args.verify_all_hashes),
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
