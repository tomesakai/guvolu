"""活动 L2 成品的五分钟生产质量窗口。

SQLite 只保存低基数控制遥测。事实仍在活动 ``book_l2_frame`` Parquet；本模块
按登记的 head 路径读取，不扫描目录。实时事实优先按 ``ingest_time``（raw v3
接收 UTC）归窗；历史归档按 event clock。``observed_silence_gt_30s`` 只表示
同一 connection/channel 的单调接收时钟间隔，不能解释成已证明的数据缺口。
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import sqlite3
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb

from guvolu.data import store
from guvolu.data.paths import data_root as configured_data_root
from guvolu.data.sqlite_writer_lock import sqlite_writer_lock

QUALITY_VERSION = "l2-quality-v1"
WINDOW_SECONDS = 300
OBSERVED_SILENCE_NS = 30_000_000_000
DEFAULT_RECENT_MINUTES = 20
# 封口与 watch 各五分钟。
# 另留两分钟处理余量。
# 此阈值仅衡量物化新鲜度。
MATERIALIZED_FRESH_SECONDS = 12 * 60

_CHECKSUM_UNSUPPORTED_ENDPOINTS = frozenset({
    "books",
    "orderbooks/ws",
    "depth_whole/depth_diff",
    "board_snapshot/board",
    "historical-data/order-book",
})
_UNTRUSTED_FLAGS = frozenset({
    "connection_boundary_unknown",
    "sequence_predecessor_untrusted",
})


@dataclass(frozen=True, slots=True)
class _FrameSource:
    """一个活动 frame 输出及其不可变 head 身份。"""

    market_id: str
    venue_id: str
    partition_key: str
    normalization_version: str
    attempt_id: str
    artifact_id: str
    path: Path
    min_event_time: datetime | None
    max_event_time: datetime | None


@dataclass(frozen=True, slots=True)
class _Frame:
    """质量计算所需的最小 frame 投影。"""

    market_id: str
    venue_id: str
    observation_time: datetime
    clock_basis: str
    event_time: datetime
    source_publish_time: datetime | None
    available_time: datetime | None
    ingest_time: datetime | None
    recv_ts_mono_ns: int | None
    message_kind: str
    sequence_id: str | None
    changed_bid_levels: int | None
    changed_ask_levels: int | None
    checksum: str | None
    endpoint: str | None
    integrity_mode: str | None
    source_session_id: str | None
    connection_id: str | None
    channel_id: str | None
    data_quality: frozenset[str] | None
    segment_sequence: int | None
    source_row_index: int | None
    attempt_id: str
    normalization_version: str
    artifact_id: str


@dataclass(frozen=True, slots=True)
class L2QualityWindow:
    """一条可直接 upsert 到 SQLite v19 的质量窗口。"""

    market_id: str
    window_start: str
    window_end: str
    quality_version: str
    source_head_generation: str
    source_attempt_ids: str
    source_attempt_count: int
    source_normalization_versions: str
    window_clock_basis: str
    frames: int
    snapshot_frames: int
    delta_frames: int
    connection_count: int | None
    channel_count: int | None
    identity_unknown_frames: int
    first_observation_time: str | None
    last_observation_time: str | None
    first_event_time: str | None
    last_event_time: str | None
    first_available_time: str | None
    last_available_time: str | None
    first_ingest_time: str | None
    last_ingest_time: str | None
    max_observed_interarrival_ms: float | None
    observed_silence_gt_30s: int | None
    sequence_duplicates: int | None
    sequence_regressions: int | None
    predecessor_unknown_frames: int | None
    unanchored_before_snapshot_frames: int | None
    anchor_unknown_frames: int
    untrusted_frames: int | None
    fact_untrusted_flag_conflicts: int
    checksum_status: str
    checksum_observed_frames: int
    checksum_checked_frames: int | None
    checksum_failures: int | None
    recv_source_offset_samples: int
    recv_source_offset_p50_ms: float | None
    recv_source_offset_p95_ms: float | None
    latency_status: str
    latest_materialized_observation_time: str | None
    materialized_freshness_seconds: float | None
    materialized_freshness_status: str
    window_complete: int
    status: str
    reasons: str
    computed_at: str


def _utc(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _iso(value: datetime | None) -> str | None:
    return None if value is None else value.astimezone(UTC).isoformat()


def _floor_window(value: datetime) -> datetime:
    value = value.astimezone(UTC)
    seconds = int(value.timestamp())
    return datetime.fromtimestamp(
        seconds - seconds % WINDOW_SECONDS, UTC
    )


def _quality_flags(value: object) -> frozenset[str] | None:
    if value is None:
        return None
    try:
        body = json.loads(str(value))
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(body, list) or any(not isinstance(item, str) for item in body):
        return None
    return frozenset(body)


def _integer(value: object) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(str(value))
    except ValueError:
        return None


def _percentile(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * quantile
    low = math.floor(position)
    high = math.ceil(position)
    if low == high:
        return ordered[low]
    weight = position - low
    return ordered[low] * (1.0 - weight) + ordered[high] * weight


def _head_generation(sources: Iterable[_FrameSource]) -> str:
    body = json.dumps(sorted(
        (
            source.partition_key,
            source.attempt_id,
            source.normalization_version,
            source.artifact_id,
        )
        for source in sources
    ), separators=(",", ":"), ensure_ascii=False)
    return "sha256-" + hashlib.sha256(body.encode("utf-8")).hexdigest()


def _active_frame_sources(
    root: Path,
    conn: sqlite3.Connection,
    market_ids: Sequence[str] | None,
) -> dict[str, list[_FrameSource]]:
    """冻结当前活动 frame head；空的当前窗口也必须保留市场身份。"""
    marks = ""
    params: list[object] = []
    selected = tuple(sorted(set(market_ids or ())))
    if selected:
        marks = " AND h.market_id IN (" + ",".join("?" for _ in selected) + ")"
        params.extend(selected)
    rows = conn.execute(
        "SELECT h.market_id,m.venue_id,h.partition_key,"
        "h.normalization_version,h.attempt_id,o.artifact_id,a.storage_path,"
        "o.min_event_time,o.max_event_time "
        "FROM materialization_partition_head h "
        "JOIN market m ON m.market_id=h.market_id "
        "JOIN materialization_output o ON o.attempt_id=h.attempt_id "
        "JOIN artifact a ON a.artifact_id=o.artifact_id "
        "WHERE h.domain='book_l2' AND o.dataset='book_l2_frame'" + marks +
        " ORDER BY h.market_id,h.partition_key,h.attempt_id",
        params,
    ).fetchall()
    resolved_root = root.resolve()
    grouped: dict[str, list[_FrameSource]] = defaultdict(list)
    for row in rows:
        low, high = _utc(row[7]), _utc(row[8])
        path = (resolved_root / str(row[6])).resolve()
        if not path.is_relative_to(resolved_root) or path.suffix.lower() != ".parquet":
            raise ValueError(f"活动 L2 frame 路径越界或类型非法: {row[6]}")
        if not path.is_file():
            raise FileNotFoundError(f"活动 L2 frame 缺失: {row[6]}")
        grouped[str(row[0])].append(_FrameSource(
            market_id=str(row[0]), venue_id=str(row[1]),
            partition_key=str(row[2]), normalization_version=str(row[3]),
            attempt_id=str(row[4]), artifact_id=str(row[5]), path=path,
            min_event_time=low, max_event_time=high,
        ))
    return dict(grouped)


_COLUMN_TYPES: dict[str, str] = {
    "frame_id": "VARCHAR",
    "market_id": "VARCHAR",
    "venue_id": "VARCHAR",
    "event_time": "TIMESTAMPTZ",
    "source_publish_time": "TIMESTAMPTZ",
    "available_time": "TIMESTAMPTZ",
    "ingest_time": "TIMESTAMPTZ",
    "recv_ts_mono_ns": "UBIGINT",
    "message_kind": "VARCHAR",
    "sequence_id": "VARCHAR",
    "changed_bid_levels": "BIGINT",
    "changed_ask_levels": "BIGINT",
    "checksum": "VARCHAR",
    "endpoint": "VARCHAR",
    "integrity_mode": "VARCHAR",
    "source_session_id": "VARCHAR",
    "connection_id": "VARCHAR",
    "channel_id": "VARCHAR",
    "data_quality": "VARCHAR",
    "segment_sequence": "BIGINT",
    "source_row_index": "BIGINT",
    "normalization_version": "VARCHAR",
}


def _read_frames(
    sources: list[_FrameSource], start: datetime, end: datetime
) -> list[_Frame]:
    """读取区间帧及完整 connection 前驱，旧列缺失时显式投影 NULL。"""
    paths = [str(source.path) for source in sources]
    source_by_path = {
        str(source.path.resolve()).casefold(): source for source in sources
    }
    db: Any = duckdb.connect(":memory:")
    db.execute("SET TimeZone='UTC'")
    db.execute("SET threads=2")
    try:
        columns = {
            str(row[0]) for row in db.execute(
                "DESCRIBE SELECT * FROM read_parquet(?, union_by_name=true)",
                [paths],
            ).fetchall()
        }
        projection = [
            name if name in columns else f"NULL::{kind} AS {name}"
            for name, kind in _COLUMN_TYPES.items()
        ]
        names = list(_COLUMN_TYPES)
        base = (
            "WITH base AS (SELECT " + ",".join(projection) +
            ",filename AS _filename FROM read_parquet(?, "
            "union_by_name=true, filename=true)), decorated AS (SELECT *,"
            "CASE WHEN endpoint='historical-data/order-book' THEN event_time "
            "ELSE COALESCE(ingest_time,available_time,event_time) END "
            "AS _observation_time,CASE "
            "WHEN endpoint='historical-data/order-book' THEN 'event' "
            "WHEN ingest_time IS NOT NULL THEN 'ingest' "
            "WHEN available_time IS NOT NULL THEN 'available' ELSE 'event' END "
            "AS _clock_basis FROM base) "
        )
        output = ",".join(names) + ",_filename,_observation_time,_clock_basis "
        current = db.execute(
            base + "SELECT " + output + "FROM decorated "
            "WHERE _observation_time>=? AND _observation_time<?",
            [paths, start, end],
        ).fetchall()
        # 取各连接最近快照。
        # 取各频道最近前驱帧。
        prior_channel = db.execute(
            base + "SELECT " + output + "FROM decorated WHERE "
            "_observation_time<? AND endpoint!='historical-data/order-book' "
            "AND connection_id IS NOT NULL AND channel_id IS NOT NULL "
            "QUALIFY row_number() OVER (PARTITION BY connection_id,channel_id "
            "ORDER BY _observation_time DESC,segment_sequence DESC,"
            "source_row_index DESC)=1",
            [paths, start],
        ).fetchall()
        prior_snapshot = db.execute(
            base + "SELECT " + output + "FROM decorated WHERE "
            "_observation_time<? AND endpoint!='historical-data/order-book' "
            "AND connection_id IS NOT NULL AND message_kind='snapshot' "
            "QUALIFY row_number() OVER (PARTITION BY connection_id "
            "ORDER BY _observation_time DESC,segment_sequence DESC,"
            "source_row_index DESC)=1",
            [paths, start],
        ).fetchall()
        rows = [*current, *prior_channel, *prior_snapshot]
    finally:
        db.close()
    frames: list[_Frame] = []
    seen: set[tuple[str, str]] = set()
    names = [*_COLUMN_TYPES, "_filename", "_observation_time", "_clock_basis"]
    for row in rows:
        values = dict(zip(names, row, strict=True))
        event = _utc(values["event_time"])
        observation = _utc(values["_observation_time"])
        if event is None or observation is None:
            continue
        filename = str(Path(str(values["_filename"])).resolve()).casefold()
        source = source_by_path.get(filename)
        if source is None:
            raise ValueError(
                f"DuckDB 返回未冻结的 frame 路径: {values['_filename']}"
            )
        frame_identity = str(values["frame_id"] or values["source_row_index"])
        if (filename, frame_identity) in seen:
            continue
        seen.add((filename, frame_identity))
        frames.append(_Frame(
            market_id=str(values["market_id"]),
            venue_id=str(values["venue_id"]), observation_time=observation,
            clock_basis=str(values["_clock_basis"]), event_time=event,
            source_publish_time=_utc(values["source_publish_time"]),
            available_time=_utc(values["available_time"]),
            ingest_time=_utc(values["ingest_time"]),
            recv_ts_mono_ns=_integer(values["recv_ts_mono_ns"]),
            message_kind=str(values["message_kind"]),
            sequence_id=(
                None if values["sequence_id"] is None
                else str(values["sequence_id"])
            ),
            changed_bid_levels=_integer(values["changed_bid_levels"]),
            changed_ask_levels=_integer(values["changed_ask_levels"]),
            checksum=(
                None if values["checksum"] is None else str(values["checksum"])
            ),
            endpoint=(
                None if values["endpoint"] is None else str(values["endpoint"])
            ),
            integrity_mode=(
                None if values["integrity_mode"] is None
                else str(values["integrity_mode"])
            ),
            source_session_id=(
                None if values["source_session_id"] is None
                else str(values["source_session_id"])
            ),
            connection_id=(
                None if values["connection_id"] is None
                else str(values["connection_id"])
            ),
            channel_id=(
                None if values["channel_id"] is None
                else str(values["channel_id"])
            ),
            data_quality=_quality_flags(values["data_quality"]),
            segment_sequence=_integer(values["segment_sequence"]),
            source_row_index=_integer(values["source_row_index"]),
            normalization_version=(
                source.normalization_version
                if values["normalization_version"] is None
                else str(values["normalization_version"])
            ),
            attempt_id=source.attempt_id, artifact_id=source.artifact_id,
        ))
    return frames


def _wire_key(frame: _Frame) -> tuple[object, ...]:
    """同一采集 run 内 segment/row 是 wire 顺序，接收时钟只作回退。"""
    if frame.segment_sequence is not None and frame.source_row_index is not None:
        return (
            0, frame.source_session_id or frame.connection_id or "",
            frame.segment_sequence, frame.source_row_index,
        )
    if frame.recv_ts_mono_ns is not None:
        return (1, frame.recv_ts_mono_ns, frame.source_row_index or -1)
    return (
        2, frame.ingest_time or frame.event_time,
        frame.source_row_index or -1,
    )


def _checksum_profile(
    frames: list[_Frame], reasons: set[str]
) -> tuple[str, int, int | None, int | None]:
    observed = [frame for frame in frames if frame.checksum not in (None, "")]
    if observed:
        failed = [
            frame for frame in observed
            if frame.data_quality is not None
            and any(
                "checksum" in flag and "fail" in flag
                for flag in frame.data_quality
            )
        ]
        verified = [
            frame for frame in observed
            if (
                frame.integrity_mode is not None
                and "checksum" in frame.integrity_mode.casefold()
                and any(
                    token in frame.integrity_mode.casefold()
                    for token in ("verified", "passed")
                )
            ) or (
                frame.data_quality is not None
                and any(
                    flag in {"checksum_verified", "checksum_passed"}
                    for flag in frame.data_quality
                )
            )
        ]
        failures = len(failed)
        if failures:
            reasons.add("checksum_failed")
            explicitly_checked = len(set(verified) | set(failed))
            return "failed", len(observed), explicitly_checked, failures
        if len(verified) == len(observed):
            return "passed", len(observed), len(verified), 0
        reasons.add("checksum_observed_but_not_verified")
        return "unknown", len(observed), None, None
    if all(
        frame.endpoint in _CHECKSUM_UNSUPPORTED_ENDPOINTS for frame in frames
    ):
        return "unsupported", 0, None, None
    reasons.add("checksum_capability_unknown")
    return "unknown", 0, None, None


def _build_window(
    frames: list[_Frame], context: list[_Frame], sources: list[_FrameSource],
    window_start: datetime, computed_at: datetime,
) -> L2QualityWindow:
    window_end = window_start + timedelta(seconds=WINDOW_SECONDS)
    frames = sorted(frames, key=_wire_key)
    all_wire = sorted(context, key=_wire_key)
    reasons: set[str] = set()

    connections = {frame.connection_id for frame in frames if frame.connection_id}
    channels = {frame.channel_id for frame in frames if frame.channel_id}
    identity_unknown = sum(
        frame.connection_id is None or frame.channel_id is None for frame in frames
    )
    if identity_unknown:
        reasons.add("connection_or_channel_identity_unknown")

    previous_mono: dict[tuple[str, str], int] = {}
    interarrivals: list[float] = []
    silence = 0
    for frame in all_wire:
        if (
            frame.connection_id is None or frame.channel_id is None
            or frame.recv_ts_mono_ns is None
        ):
            continue
        key = (frame.connection_id, frame.channel_id)
        previous = previous_mono.get(key)
        if previous is not None and frame.observation_time >= window_start:
            delta_ns = frame.recv_ts_mono_ns - previous
            if delta_ns >= 0:
                interarrivals.append(delta_ns / 1_000_000)
                if delta_ns > OBSERVED_SILENCE_NS:
                    silence += 1
        previous_mono[key] = frame.recv_ts_mono_ns
    if silence:
        reasons.add("observed_receive_silence_gt_30s")
    observed_silence: int | None = silence if interarrivals else None

    sequence_frames = [frame for frame in frames if frame.sequence_id is not None]
    prior_sequence: dict[tuple[str, str], int] = {}
    duplicate = regression = predecessor_unknown = comparisons = 0
    for frame in all_wire:
        sequence = _integer(frame.sequence_id)
        if sequence is None:
            continue
        in_window = window_start <= frame.observation_time < window_end
        if frame.connection_id is None or frame.channel_id is None:
            if in_window:
                predecessor_unknown += 1
            continue
        key = (frame.connection_id, frame.channel_id)
        previous = prior_sequence.get(key)
        if in_window:
            # 快照建立前驱。
            if frame.message_kind == "snapshot":
                pass
            elif previous is None:
                predecessor_unknown += 1
            else:
                comparisons += 1
                if sequence == previous:
                    is_okx_heartbeat = (
                        frame.endpoint == "books"
                        and frame.message_kind == "delta"
                        and frame.changed_bid_levels == 0
                        and frame.changed_ask_levels == 0
                        and frame.data_quality is not None
                        and "empty_update_heartbeat" in frame.data_quality
                    )
                    if not is_okx_heartbeat:
                        duplicate += 1
                elif sequence < previous:
                    regression += 1
        # 只比较紧邻 wire 前驱。
        prior_sequence[key] = sequence
    sequence_duplicates: int | None = duplicate if comparisons else None
    sequence_regressions: int | None = regression if comparisons else None
    predecessor_unknown_frames: int | None = (
        predecessor_unknown if sequence_frames else None
    )
    if predecessor_unknown:
        reasons.add("sequence_predecessor_unknown")
    if duplicate:
        reasons.add("sequence_duplicate_same_connection_channel")
    if regression:
        reasons.add("sequence_regression_same_connection_channel")

    anchored: set[str] = set()
    unanchored = anchor_unknown = fact_flag_conflicts = 0
    for frame in all_wire:
        in_window = window_start <= frame.observation_time < window_end
        if frame.message_kind == "snapshot" and frame.connection_id is not None:
            anchored.add(frame.connection_id)
        elif frame.message_kind == "delta" and in_window:
            if frame.connection_id is None:
                anchor_unknown += 1
            elif frame.connection_id not in anchored:
                unanchored += 1
            elif (
                frame.data_quality is not None
                and "replay_untrusted_until_snapshot" in frame.data_quality
            ):
                # segment 会重置局部锚点。
                # 全局锚定后只记录冲突。
                fact_flag_conflicts += 1
    delta_frames = sum(frame.message_kind == "delta" for frame in frames)
    unanchored_value: int | None = None if anchor_unknown else unanchored
    if unanchored:
        reasons.add("delta_before_connection_snapshot")
    if anchor_unknown:
        reasons.add("snapshot_anchor_identity_unknown")

    quality_known = all(frame.data_quality is not None for frame in frames)
    untrusted: int | None
    if quality_known:
        untrusted = sum(
            bool((frame.data_quality or frozenset()) & _UNTRUSTED_FLAGS)
            for frame in frames
        )
        if untrusted:
            reasons.add("source_data_quality_untrusted")
    else:
        untrusted = None
        reasons.add("source_data_quality_unavailable")

    (
        checksum_status, checksum_observed,
        checksum_checked, checksum_failures,
    ) = _checksum_profile(frames, reasons)

    offsets = [
        (frame.ingest_time - frame.source_publish_time).total_seconds() * 1000
        for frame in frames
        if frame.connection_id is not None
        and frame.ingest_time is not None
        and frame.source_publish_time is not None
    ]
    p50: float | None
    p95: float | None
    if offsets:
        p50, p95 = _percentile(offsets, 0.50), _percentile(offsets, 0.95)
        latency_status = "clock_skewed" if any(value < 0 for value in offsets) else "measurable"
        if latency_status == "clock_skewed":
            reasons.add("negative_recv_source_offset_clock_skew")
    else:
        p50 = p95 = None
        latency_status = "unmeasurable"

    latest = max(frame.observation_time for frame in context)
    current_window = _floor_window(computed_at) == window_start
    materialized_freshness_seconds: float | None
    if current_window:
        materialized_freshness_seconds = (computed_at - latest).total_seconds()
        if materialized_freshness_seconds < -1:
            materialized_freshness_status = "clock_skewed"
            reasons.add("latest_materialized_observation_clock_skew")
        elif materialized_freshness_seconds > MATERIALIZED_FRESH_SECONDS:
            materialized_freshness_status = "stale"
            reasons.add("materialized_observation_stale")
        else:
            materialized_freshness_status = "fresh"
    else:
        materialized_freshness_seconds = None
        materialized_freshness_status = "not_applicable"

    failed = duplicate > 0 or regression > 0 or checksum_status == "failed"
    status = "failed" if failed else "degraded" if reasons else "ok"
    used_attempts = sorted({source.attempt_id for source in sources})
    normalizations = sorted({source.normalization_version for source in sources})
    available = [frame.available_time for frame in frames if frame.available_time]
    ingested = [frame.ingest_time for frame in frames if frame.ingest_time]
    events = [frame.event_time for frame in frames]
    observations = [frame.observation_time for frame in frames]
    clock_bases = {frame.clock_basis for frame in frames}
    clock_basis = next(iter(clock_bases)) if len(clock_bases) == 1 else "mixed"
    return L2QualityWindow(
        market_id=frames[0].market_id,
        window_start=window_start.isoformat(), window_end=window_end.isoformat(),
        quality_version=QUALITY_VERSION,
        source_head_generation=_head_generation(sources),
        source_attempt_ids=json.dumps(used_attempts, separators=(",", ":")),
        source_attempt_count=len(used_attempts),
        source_normalization_versions=json.dumps(
            normalizations, separators=(",", ":")
        ),
        window_clock_basis=clock_basis,
        frames=len(frames),
        snapshot_frames=sum(frame.message_kind == "snapshot" for frame in frames),
        delta_frames=delta_frames,
        connection_count=len(connections) or None,
        channel_count=len(channels) or None,
        identity_unknown_frames=identity_unknown,
        first_observation_time=min(observations).isoformat(),
        last_observation_time=max(observations).isoformat(),
        first_event_time=min(events).isoformat(),
        last_event_time=max(events).isoformat(),
        first_available_time=_iso(min(available)) if available else None,
        last_available_time=_iso(max(available)) if available else None,
        first_ingest_time=_iso(min(ingested)) if ingested else None,
        last_ingest_time=_iso(max(ingested)) if ingested else None,
        max_observed_interarrival_ms=(max(interarrivals) if interarrivals else None),
        observed_silence_gt_30s=observed_silence,
        sequence_duplicates=sequence_duplicates,
        sequence_regressions=sequence_regressions,
        predecessor_unknown_frames=predecessor_unknown_frames,
        unanchored_before_snapshot_frames=unanchored_value,
        anchor_unknown_frames=anchor_unknown,
        untrusted_frames=untrusted,
        fact_untrusted_flag_conflicts=fact_flag_conflicts,
        checksum_status=checksum_status,
        checksum_observed_frames=checksum_observed,
        checksum_checked_frames=checksum_checked,
        checksum_failures=checksum_failures,
        recv_source_offset_samples=len(offsets),
        recv_source_offset_p50_ms=p50,
        recv_source_offset_p95_ms=p95,
        latency_status=latency_status,
        latest_materialized_observation_time=latest.isoformat(),
        materialized_freshness_seconds=materialized_freshness_seconds,
        materialized_freshness_status=materialized_freshness_status,
        window_complete=int(window_end <= computed_at),
        status=status,
        reasons=json.dumps(sorted(reasons), separators=(",", ":")),
        computed_at=computed_at.isoformat(),
    )


def _empty_current_window(
    sources: list[_FrameSource], context: list[_Frame],
    window_start: datetime, computed_at: datetime,
) -> L2QualityWindow:
    """无帧也落 heartbeat；最新时刻优先实读，才退回 head event 摘要。"""
    historical_archive = (
        sources[0].venue_id == "okx"
        and all(
            source.normalization_version == "book-l2-normalization-v2"
            for source in sources
        )
    )
    reasons = {
        "historical_archive_current_freshness_not_applicable"
        if historical_archive else "no_frames_current_unsealed_window"
    }
    latest: datetime | None = None
    if context:
        latest = max(frame.observation_time for frame in context)
    else:
        high = [source.max_event_time for source in sources if source.max_event_time]
        if high:
            latest = max(high)
            if not historical_archive:
                reasons.add("materialized_freshness_event_time_fallback")
    materialized_freshness_seconds: float | None = None
    if historical_archive:
        materialized_freshness_status = "not_applicable"
    elif latest is None:
        materialized_freshness_status = "unknown"
        reasons.add("latest_materialized_observation_unknown")
    else:
        materialized_freshness_seconds = (computed_at - latest).total_seconds()
        if materialized_freshness_seconds < -1:
            materialized_freshness_status = "clock_skewed"
            reasons.add("latest_materialized_observation_clock_skew")
        elif materialized_freshness_seconds > MATERIALIZED_FRESH_SECONDS:
            materialized_freshness_status = "stale"
            reasons.add("materialized_observation_stale")
        else:
            materialized_freshness_status = "fresh"
    attempt_ids = sorted({source.attempt_id for source in sources})
    normalizations = sorted({source.normalization_version for source in sources})
    unsupported = sources[0].venue_id in {"gmo", "bitbank", "bitflyer", "okx"}
    return L2QualityWindow(
        market_id=sources[0].market_id,
        window_start=window_start.isoformat(),
        window_end=(window_start + timedelta(seconds=WINDOW_SECONDS)).isoformat(),
        quality_version=QUALITY_VERSION,
        source_head_generation=_head_generation(sources),
        source_attempt_ids=json.dumps(attempt_ids, separators=(",", ":")),
        source_attempt_count=len(attempt_ids),
        source_normalization_versions=json.dumps(
            normalizations, separators=(",", ":")
        ),
        window_clock_basis="none", frames=0, snapshot_frames=0,
        delta_frames=0, connection_count=None, channel_count=None,
        identity_unknown_frames=0, first_observation_time=None,
        last_observation_time=None, first_event_time=None,
        last_event_time=None, first_available_time=None,
        last_available_time=None, first_ingest_time=None,
        last_ingest_time=None, max_observed_interarrival_ms=None,
        observed_silence_gt_30s=None, sequence_duplicates=None,
        sequence_regressions=None, predecessor_unknown_frames=None,
        unanchored_before_snapshot_frames=None, anchor_unknown_frames=0,
        untrusted_frames=None, fact_untrusted_flag_conflicts=0,
        checksum_status="unsupported" if unsupported else "unknown",
        checksum_observed_frames=0, checksum_checked_frames=None,
        checksum_failures=None, recv_source_offset_samples=0,
        recv_source_offset_p50_ms=None, recv_source_offset_p95_ms=None,
        latency_status="unmeasurable",
        latest_materialized_observation_time=_iso(latest),
        materialized_freshness_seconds=materialized_freshness_seconds,
        materialized_freshness_status=materialized_freshness_status,
        window_complete=0,
        status=(
            "ok" if materialized_freshness_status in {"fresh", "not_applicable"}
            else "degraded"
        ),
        reasons=json.dumps(sorted(reasons), separators=(",", ":")),
        computed_at=computed_at.isoformat(),
    )


def compute_quality_windows(
    root: Path,
    conn: sqlite3.Connection,
    start: datetime,
    end: datetime,
    *,
    market_ids: Sequence[str] | None = None,
    computed_at: datetime | None = None,
) -> list[L2QualityWindow]:
    """按观测时钟计算窗口，并为当前活动市场保留空窗 heartbeat。"""
    start = _floor_window(start)
    end = end.astimezone(UTC)
    if end <= start:
        raise ValueError("质量窗口结束时刻必须晚于开始时刻")
    now = (computed_at or datetime.now(UTC)).astimezone(UTC)
    grouped = _active_frame_sources(root, conn, market_ids)
    output: list[L2QualityWindow] = []
    for sources in grouped.values():
        context = _read_frames(sources, start, end)
        windows: dict[datetime, list[_Frame]] = defaultdict(list)
        for frame in context:
            if start <= frame.observation_time < end:
                windows[_floor_window(frame.observation_time)].append(frame)
        for window_start, frames in sorted(windows.items()):
            prior = [
                frame for frame in context
                if frame.observation_time < (
                    window_start + timedelta(seconds=WINDOW_SECONDS)
                )
            ]
            output.append(_build_window(
                frames, prior, sources, window_start, now
            ))
        heartbeat_start = _floor_window(now)
        if (
            start <= heartbeat_start < end
            and heartbeat_start not in windows
        ):
            output.append(_empty_current_window(
                sources, context, heartbeat_start, now
            ))
    return sorted(output, key=lambda row: (row.market_id, row.window_start))


_UPSERT_COLUMNS = tuple(L2QualityWindow.__dataclass_fields__)


def upsert_quality_windows(
    conn: sqlite3.Connection, windows: Iterable[L2QualityWindow]
) -> int:
    """幂等刷新质量窗口；算法版本不变时只更新同一活动事实视图。"""
    rows = list(windows)
    if not rows:
        return 0
    marks = ",".join("?" for _ in _UPSERT_COLUMNS)
    updates = ",".join(
        f"{name}=excluded.{name}"
        for name in _UPSERT_COLUMNS
        if name not in {"market_id", "window_start", "quality_version"}
    )
    sql = (
        "INSERT INTO l2_quality_window (" + ",".join(_UPSERT_COLUMNS) + ") "
        f"VALUES ({marks}) ON CONFLICT(market_id,window_start,quality_version) "
        f"DO UPDATE SET {updates}"
    )
    try:
        conn.executemany(
            sql,
            [tuple(getattr(row, name) for name in _UPSERT_COLUMNS) for row in rows],
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return len(rows)


def refresh_recent(
    root: Path,
    conn: sqlite3.Connection,
    *,
    minutes: int = DEFAULT_RECENT_MINUTES,
    market_ids: Sequence[str] | None = None,
    now: datetime | None = None,
) -> dict[str, object]:
    """计算并 upsert 最近窗口；供 CLI 与 L2 watch 同步调用。"""
    if minutes < 5 or minutes > 24 * 60:
        raise ValueError("recent minutes 必须位于 5..1440")
    finished = (now or datetime.now(UTC)).astimezone(UTC)
    windows = compute_quality_windows(
        root, conn, finished - timedelta(minutes=minutes), finished,
        market_ids=market_ids, computed_at=finished,
    )
    upserted = upsert_quality_windows(conn, windows)
    counts: dict[str, int] = defaultdict(int)
    for row in windows:
        counts[row.status] += 1
    return {
        "quality_version": QUALITY_VERSION,
        "materialized_freshness_threshold_seconds": (
            MATERIALIZED_FRESH_SECONDS
        ),
        "from": _floor_window(finished - timedelta(minutes=minutes)).isoformat(),
        "to": finished.isoformat(),
        "windows": len(windows), "upserted": upserted,
        "status_counts": dict(sorted(counts.items())),
    }


def audit_range(
    root: Path,
    conn: sqlite3.Connection,
    start: datetime,
    end: datetime,
    *,
    market_ids: Sequence[str] | None = None,
) -> dict[str, object]:
    """只读计算指定区间，返回失败/降级摘要，不改 SQLite。"""
    windows = compute_quality_windows(
        root, conn, start, end, market_ids=market_ids
    )
    counts: dict[str, int] = defaultdict(int)
    for row in windows:
        counts[row.status] += 1
    failures = [asdict(row) for row in windows if row.status == "failed"]
    return {
        "ok": not failures, "quality_version": QUALITY_VERSION,
        "materialized_freshness_threshold_seconds": (
            MATERIALIZED_FRESH_SECONDS
        ),
        "windows": len(windows), "status_counts": dict(sorted(counts.items())),
        "failed_windows": failures,
    }


def _parse_time(value: str) -> datetime:
    parsed = _utc(value)
    if parsed is None:
        raise argparse.ArgumentTypeError(f"不是 ISO-8601 时间: {value}")
    return parsed


def main(argv: Sequence[str] | None = None) -> int:
    """命令行入口：recent 写控制表，audit 对指定区间只读复算。"""
    parser = argparse.ArgumentParser(description="L2 五分钟质量窗口")
    parser.add_argument("--data-root", type=Path, default=None)
    sub = parser.add_subparsers(dest="command", required=True)
    recent = sub.add_parser("recent", help="刷新最近活动 L2 窗口")
    recent.add_argument("--minutes", type=int, default=DEFAULT_RECENT_MINUTES)
    recent.add_argument("--market-id", action="append", default=[])
    audit = sub.add_parser("audit", help="只读复算指定 event 区间")
    audit.add_argument("--from-time", type=_parse_time, required=True)
    audit.add_argument("--to-time", type=_parse_time, required=True)
    audit.add_argument("--market-id", action="append", default=[])
    args = parser.parse_args(argv)
    root = (args.data_root or configured_data_root()).resolve()
    conn = store.connect(root)
    try:
        if args.command == "recent":
            with sqlite_writer_lock(root):
                result = refresh_recent(
                    root, conn, minutes=int(args.minutes),
                    market_ids=tuple(args.market_id) or None,
                )
            code = 0
        else:
            result = audit_range(
                root, conn, args.from_time, args.to_time,
                market_ids=tuple(args.market_id) or None,
            )
            code = 0 if bool(result["ok"]) else 1
    finally:
        conn.close()
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
