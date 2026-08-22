"""OKX 日级历史 L2 原件的流式重放与第二版事实物化。"""
from __future__ import annotations

import argparse
import csv
import hashlib
import heapq
import json
import os
import sqlite3
import tarfile
import time
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from email.utils import parsedate_to_datetime
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO, cast

import duckdb

from guvolu.data import store
from guvolu.data.book_l2_contract import (
    BOOK_L2_FRAME_DATASET,
    BOOK_L2_LEVEL_DATASET,
    BOOK_L2_NORMALIZATION_VERSION,
    BOOK_L2_SCHEMA_VERSION,
    BookSourceDescriptor,
    create_book_l2_tables,
)
from guvolu.data.durable_io import atomic_write_text
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
from guvolu.data.okx_l2_archive import OKX_BOOK_HISTORY_ENDPOINT
from guvolu.data.paths import data_root as configured_data_root
from guvolu.data.sqlite_writer_lock import sqlite_writer_lock
from guvolu.venues import registry

OKX_PAYLOAD_SCHEMA_VERSION = "okx-history-l2-jsonl-v1"
OKX_BOOK_MODE = "absolute_level_update"
OKX_REPLAY_FIDELITY = "periodic_snapshot_absolute_delta"
OKX_INTEGRITY_MODE = "archive_sha256+strict_ts+periodic_snapshot"
PROGRESS_FRAMES = 250_000
MAX_SNAPSHOT_GAP_MS = 901_000


def source_descriptor(depth_limit: int) -> BookSourceDescriptor:
    """返回 OKX 历史文件的端点级语义。"""
    return BookSourceDescriptor(
        venue_id="okx",
        domain="book_history",
        endpoint=OKX_BOOK_HISTORY_ENDPOINT,
        transport="official_archive",
        payload_schema_version=OKX_PAYLOAD_SCHEMA_VERSION,
        timestamp_unit="milliseconds",
        book_mode=OKX_BOOK_MODE,
        replay_fidelity=OKX_REPLAY_FIDELITY,
        sequence_policy="none",
        checksum_policy="archive_sha256",
        availability_policy="source_last_modified",
        depth_limit=depth_limit,
    )


@dataclass(frozen=True, slots=True)
class OkxArchiveInput:
    """一份散列和 manifest 已核对的 OKX 日档。"""

    manifest_path: Path
    venue_symbol: str
    day: str
    depth_limit: int
    source_last_modified: str
    ingest_time: str
    artifact: SourceArtifact

    @property
    def partition_key(self) -> str:
        return self.day


@dataclass(frozen=True, slots=True)
class ReplayProfile:
    """重放期间同步得到的完整性证据。"""

    frames: int
    levels: int
    snapshots: int
    updates: int
    sets: int
    deletes: int
    timestamp_gaps_gt_10ms: int
    timestamp_gaps_gt_100ms: int
    max_timestamp_gap_ms: int
    max_snapshot_gap_ms: int
    crossed_frames: int
    min_event_time: str
    max_event_time: str
    source_member: str
    uncompressed_bytes: int


@dataclass(frozen=True, slots=True)
class OkxL2Result:
    """一份日档的物化结果。"""

    attempt_id: str
    market_id: str
    partition_key: str
    status: str
    frame_rows: int
    level_rows: int
    frame_path: str
    level_path: str
    reused: bool


@dataclass(frozen=True, slots=True)
class _PreparedAttempt:
    """短写锁内建立的物化尝试。"""

    attempt_id: str
    market_id: str
    instrument_id: str
    mapping_revision: int
    capability_revision: int
    reused_result: OkxL2Result | None


class _ReplayBook:
    """按绝对数量更新维护盘口状态。"""

    def __init__(self) -> None:
        self.books: dict[str, dict[Decimal, tuple[str, int]]] = {
            "ask": {}, "bid": {},
        }
        self.heaps: dict[str, list[Decimal]] = {"ask": [], "bid": []}

    def reset(self) -> None:
        for side in ("ask", "bid"):
            self.books[side].clear()
            self.heaps[side].clear()

    def apply(
        self, side: str, price: Decimal, size: str, order_count: int
    ) -> str:
        if Decimal(size) == 0:
            self.books[side].pop(price, None)
            return "delete"
        self.books[side][price] = (size, order_count)
        heapq.heappush(self.heaps[side], price if side == "ask" else -price)
        return "set"

    def best(self, side: str) -> Decimal | None:
        heap = self.heaps[side]
        book = self.books[side]
        while heap:
            price = heap[0] if side == "ask" else -heap[0]
            if price in book:
                return price
            heapq.heappop(heap)
        return None

    def count(self, side: str) -> int:
        return len(self.books[side])


def _iso_millis(value: object) -> tuple[str, int]:
    if isinstance(value, bool):
        raise ValueError("OKX ts 类型非法")
    millis = int(str(value))
    return datetime.fromtimestamp(millis / 1000, UTC).isoformat(), millis


def _available_time(event_time: str, published_at: str) -> str:
    event = datetime.fromisoformat(event_time)
    published = datetime.fromisoformat(published_at)
    return max(event, published).isoformat()


def _decimal_text(value: object, field: str, *, allow_zero: bool) -> str:
    if isinstance(value, (bool, float)):
        raise ValueError(f"{field} 不得为布尔或浮点")
    text = str(value)
    number = Decimal(text)
    if not number.is_finite() or number < 0 or (not allow_zero and number == 0):
        raise ValueError(f"{field} 数值非法: {text}")
    return text


def _frame_id(market_id: str, source_artifact_id: str, source_row: int) -> str:
    body = f"okx|{market_id}|{source_artifact_id}|{source_row}"
    return "sha256-" + hashlib.sha256(body.encode("ascii")).hexdigest()


def _field(value: object) -> object:
    return "\\N" if value is None else value


def _parse_source_last_modified(value: object) -> str:
    text = str(value or "")
    parsed = parsedate_to_datetime(text)
    if parsed.tzinfo is None:
        raise ValueError("OKX Last-Modified 缺少时区")
    return parsed.astimezone(UTC).isoformat()


def sealed_input(root: Path, manifest_path: Path) -> OkxArchiveInput | None:
    """核对一份 manifest；非终态或非本端点时返回空。"""
    body = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(body, Mapping):
        raise ValueError(f"OKX manifest 顶层不是对象: {manifest_path}")
    if body.get("status") != "sealed" or body.get("completion_claim") is not True:
        return None
    if body.get("endpoint") != OKX_BOOK_HISTORY_ENDPOINT:
        return None
    recorded = str(body["storage_path"])
    path = root / recorded
    sha = sha256_file(path)
    if sha != str(body["sha256"]):
        raise ValueError(f"OKX 日档散列不符: {recorded}")
    if path.stat().st_size != int(str(body["byte_count"])):
        raise ValueError(f"OKX 日档字节数不符: {recorded}")
    return OkxArchiveInput(
        manifest_path=manifest_path,
        venue_symbol=str(body["venue_symbol"]),
        day=str(body["day"]),
        depth_limit=int(str(body["depth_levels"])),
        source_last_modified=_parse_source_last_modified(
            body.get("source_last_modified")
        ),
        ingest_time=str(body["sealed_at"]),
        artifact=SourceArtifact(
            artifact_id=artifact_id(sha),
            storage_path=recorded,
            absolute_path=path,
            source_rows=0,
            normalized_rows=0,
            rejected_rows=0,
        ),
    )


def sealed_inputs(root: Path) -> list[OkxArchiveInput]:
    """发现并核对全部已封口 OKX L2 日档。"""
    base = root / "raw" / "archive" / "okx" / "book_l2"
    if not base.is_dir():
        return []
    inputs: list[OkxArchiveInput] = []
    for manifest_path in sorted(base.rglob("*.tar.gz.manifest.json")):
        item = sealed_input(root, manifest_path)
        if item is not None:
            inputs.append(item)
    return inputs


def _capability_revision(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT revision_id FROM venue_capability_revision "
        "WHERE venue_id='okx' AND domain='book_history' AND endpoint=? "
        "AND available=1 AND implementation_status='implemented' "
        "ORDER BY revision_id DESC LIMIT 1",
        (OKX_BOOK_HISTORY_ENDPOINT,),
    ).fetchone()
    if row is None:
        raise ValueError("OKX 历史 L2 能力尚未登记为 implemented")
    return int(row[0])


def _bind_capability(
    conn: sqlite3.Connection, attempt_id: str, revision: int
) -> None:
    conn.execute(
        "INSERT INTO partition_capability_binding "
        "(attempt_id,venue_id,domain,endpoint,revision_id,binding_basis,bound_at) "
        "VALUES (?,'okx','book_history',?,?,'recorded',?)",
        (attempt_id, OKX_BOOK_HISTORY_ENDPOINT, revision, utc_now()),
    )


def _register_source(
    root: Path, conn: sqlite3.Connection, item: OkxArchiveInput
) -> None:
    _register_content_artifact(
        conn,
        item.artifact.artifact_id,
        "source_archive",
        item.artifact.storage_path,
        item.artifact.artifact_id.removeprefix("sha256-"),
        item.artifact.absolute_path.stat().st_size,
        item.ingest_time,
        BOOK_L2_SCHEMA_VERSION,
    )
    manifest_sha = sha256_file(item.manifest_path)
    _register_content_artifact(
        conn,
        artifact_id(manifest_sha),
        "source_manifest",
        _relative_storage_path(root, item.manifest_path),
        manifest_sha,
        item.manifest_path.stat().st_size,
        item.ingest_time,
        1,
    )


def _member(tf: tarfile.TarFile) -> tarfile.TarInfo:
    member = tf.next()
    if member is None or not member.isfile():
        raise ValueError("OKX 日档首成员不是数据文件")
    if not member.name.endswith(".data"):
        raise ValueError(f"OKX 日档成员格式未知: {member.name}")
    return member


def _parse_levels(
    values: object,
    *,
    side: str,
    kind: str,
    replay: _ReplayBook,
) -> list[tuple[int, str, str, int, str]]:
    if not isinstance(values, list):
        raise ValueError(f"OKX {side} 不是数组")
    parsed: list[tuple[int, str, str, int, str]] = []
    for index, level in enumerate(values):
        if not isinstance(level, list) or len(level) != 3:
            raise ValueError(f"OKX {side} 档位结构非法")
        price_text = _decimal_text(level[0], "price", allow_zero=False)
        size_text = _decimal_text(level[1], "size", allow_zero=True)
        if isinstance(level[2], (bool, float)):
            raise ValueError("OKX order_count 类型非法")
        order_count = int(str(level[2]))
        size = Decimal(size_text)
        if order_count < 0 or (size == 0) != (order_count == 0):
            raise ValueError("OKX size/order_count 删除语义矛盾")
        if kind == "snapshot" and size == 0:
            raise ValueError("OKX snapshot 不得含删除档")
        action = replay.apply(
            side, Decimal(price_text), size_text, order_count
        )
        parsed.append((index, price_text, size_text, order_count, action))
    return parsed


def _stage(
    item: OkxArchiveInput,
    *,
    market_id: str,
    mapping_revision: int,
    capability_revision: int,
    instrument_id: str,
    frame_csv: Path,
    level_csv: Path,
    require_full_day: bool,
) -> ReplayProfile:
    descriptor = source_descriptor(item.depth_limit)
    replay = _ReplayBook()
    frames = levels = snapshots = updates = sets = deletes = 0
    gap_10 = gap_100 = max_gap = max_snapshot_gap = crossed = 0
    previous_ts: int | None = None
    previous_snapshot_ts: int | None = None
    min_event = max_event = ""
    source_member = ""
    uncompressed_bytes = 0
    session_id = item.artifact.artifact_id
    started = time.monotonic()
    with (
        tarfile.open(item.artifact.absolute_path, "r|gz") as tf,
        frame_csv.open("w", encoding="utf-8", newline="") as frame_handle,
        level_csv.open("w", encoding="utf-8", newline="") as level_handle,
    ):
        member = _member(tf)
        source_member = member.name
        uncompressed_bytes = member.size
        extracted = tf.extractfile(member)
        if extracted is None:
            raise ValueError("OKX 日档成员不可读取")
        source = cast(BinaryIO, extracted)
        frame_writer = csv.writer(frame_handle, lineterminator="\n")
        level_writer = csv.writer(level_handle, lineterminator="\n")
        for source_row, raw_line in enumerate(source, start=1):
            try:
                payload = json.loads(raw_line)
                if not isinstance(payload, Mapping):
                    raise ValueError("OKX 数据行顶层不是对象")
                if payload.get("instId") != item.venue_symbol:
                    raise ValueError("OKX 数据行品种与市场不一致")
                raw_kind = str(payload.get("action", ""))
                if raw_kind == "snapshot":
                    kind = "snapshot"
                    replay.reset()
                    snapshots += 1
                elif raw_kind == "update":
                    kind = "delta"
                    updates += 1
                    if frames == 0:
                        raise ValueError("OKX 日档首行不是 snapshot")
                else:
                    raise ValueError(f"OKX action 未核证: {raw_kind!r}")
                event_time, event_ms = _iso_millis(payload.get("ts"))
                if previous_ts is not None:
                    delta = event_ms - previous_ts
                    if delta <= 0:
                        raise ValueError("OKX 历史 L2 时间未严格递增")
                    gap_10 += int(delta > 10)
                    gap_100 += int(delta > 100)
                    max_gap = max(max_gap, delta)
                previous_ts = event_ms
                if kind == "snapshot":
                    if previous_snapshot_ts is not None:
                        max_snapshot_gap = max(
                            max_snapshot_gap, event_ms - previous_snapshot_ts
                        )
                    previous_snapshot_ts = event_ms
                asks = _parse_levels(
                    payload.get("asks"), side="ask", kind=kind, replay=replay
                )
                bids = _parse_levels(
                    payload.get("bids"), side="bid", kind=kind, replay=replay
                )
                if kind == "snapshot" and (
                    len(asks) != item.depth_limit or len(bids) != item.depth_limit
                ):
                    raise ValueError("OKX snapshot 深度与归档声明不一致")
                best_ask = replay.best("ask")
                best_bid = replay.best("bid")
                if best_ask is None or best_bid is None:
                    raise ValueError("OKX 重放后盘口单侧为空")
                if best_bid >= best_ask:
                    crossed += 1
                    raise ValueError("OKX 重放后盘口交叉")
                mid = str((best_ask + best_bid) / 2)
                identity = _frame_id(
                    market_id, item.artifact.artifact_id, source_row
                )
                available = _available_time(
                    event_time, item.source_last_modified
                )
                frame_writer.writerow([_field(value) for value in (
                    identity, "okx", item.venue_symbol, market_id,
                    mapping_revision, capability_revision, instrument_id,
                    descriptor.endpoint, descriptor.payload_schema_version,
                    kind, descriptor.book_mode, descriptor.replay_fidelity,
                    event_time, event_time, available, item.ingest_time,
                    "venue", None, None, None, OKX_INTEGRITY_MODE,
                    len(bids), len(asks), replay.count("bid"),
                    replay.count("ask"), item.depth_limit, mid,
                    session_id, source_member, item.artifact.artifact_id,
                    source_row, BOOK_L2_NORMALIZATION_VERSION,
                    BOOK_L2_SCHEMA_VERSION,
                )])
                for side, rows in (("ask", asks), ("bid", bids)):
                    for index, price, size, order_count, action in rows:
                        level_writer.writerow((
                            identity, market_id, side, index, price, size,
                            order_count, action, "limit",
                            item.artifact.artifact_id, source_row,
                            BOOK_L2_NORMALIZATION_VERSION,
                            BOOK_L2_SCHEMA_VERSION,
                        ))
                        levels += 1
                        if action == "set":
                            sets += 1
                        else:
                            deletes += 1
                frames += 1
                min_event = event_time if not min_event else min(min_event, event_time)
                max_event = event_time if not max_event else max(max_event, event_time)
                if frames % PROGRESS_FRAMES == 0:
                    print(json.dumps({
                        "event": "okx_l2_stage_progress",
                        "day": item.day,
                        "frames": frames,
                        "levels": levels,
                        "event_time": event_time,
                        "elapsed_seconds": round(time.monotonic() - started, 3),
                    }, ensure_ascii=False), flush=True)
            except (
                json.JSONDecodeError, UnicodeError, KeyError, TypeError,
                ValueError, InvalidOperation,
            ) as exc:
                raise ValueError(
                    f"OKX L2 不可安全重放: {source_member}:{source_row}: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc
        for handle in (frame_handle, level_handle):
            handle.flush()
            os.fsync(handle.fileno())
        extra = tf.next()
        if extra is not None:
            raise ValueError("OKX 日档含未核证的额外成员")
    if frames == 0 or snapshots == 0 or updates == 0:
        raise ValueError("OKX 日档缺少快照或增量")
    if max_snapshot_gap > MAX_SNAPSHOT_GAP_MS:
        raise ValueError("OKX 周期快照间隔超过十五分钟容差")
    if previous_snapshot_ts is None or previous_ts is None:
        raise ValueError("OKX 日档缺少时间证据")
    if previous_ts - previous_snapshot_ts > MAX_SNAPSHOT_GAP_MS:
        raise ValueError("OKX 日档尾部缺少周期快照")
    if require_full_day:
        start = datetime.strptime(item.day, "%Y-%m-%d").replace(tzinfo=UTC)
        end = start + timedelta(days=1)
        first = datetime.fromisoformat(min_event)
        last = datetime.fromisoformat(max_event)
        if first - start > timedelta(seconds=1):
            raise ValueError("OKX 日档未覆盖 UTC 日初")
        if end - last > timedelta(seconds=1):
            raise ValueError("OKX 日档未覆盖 UTC 日末")
    return ReplayProfile(
        frames=frames, levels=levels, snapshots=snapshots, updates=updates,
        sets=sets, deletes=deletes, timestamp_gaps_gt_10ms=gap_10,
        timestamp_gaps_gt_100ms=gap_100, max_timestamp_gap_ms=max_gap,
        max_snapshot_gap_ms=max_snapshot_gap, crossed_frames=crossed,
        min_event_time=min_event, max_event_time=max_event,
        source_member=source_member, uncompressed_bytes=uncompressed_bytes,
    )


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
            raise ValueError(f"OKX L2 输出散列命名冲突: {final}")
        temp.unlink()
    else:
        os.replace(temp, final)
    return final, sha


def _validate_tables(
    db: Any,
    *,
    profile: ReplayProfile,
    market_id: str,
    artifact_identity: str,
) -> None:
    frame = db.execute(
        f"SELECT COUNT(*),COUNT(*)-COUNT(DISTINCT frame_id),"
        "SUM(available_time<event_time),SUM(changed_bid_levels+changed_ask_levels),"
        "COUNT(DISTINCT market_id),MIN(market_id),"
        "COUNT(DISTINCT source_artifact_id),MIN(source_artifact_id),"
        "SUM(CASE WHEN message_kind NOT IN ('snapshot','delta') THEN 1 ELSE 0 END) "
        f"FROM {BOOK_L2_FRAME_DATASET}"
    ).fetchone()
    level = db.execute(
        f"SELECT COUNT(*),COUNT(*)-COUNT(DISTINCT "
        "(frame_id,side,source_level_index)),"
        "SUM(CASE WHEN side NOT IN ('bid','ask') OR action NOT IN "
        "('set','delete') OR order_count<0 OR "
        "((size='0')<>(order_count=0)) THEN 1 ELSE 0 END) "
        f"FROM {BOOK_L2_LEVEL_DATASET}"
    ).fetchone()
    if frame is None or level is None:
        raise ValueError("OKX L2 暂存表不可读")
    if (
        int(frame[0]) != profile.frames
        or int(frame[1] or 0)
        or int(frame[2] or 0)
        or int(frame[3] or 0) != profile.levels
        or int(frame[4]) != 1
        or str(frame[5]) != market_id
        or int(frame[6]) != 1
        or str(frame[7]) != artifact_identity
        or int(frame[8] or 0)
    ):
        raise ValueError("OKX L2 frame 身份、PIT 或计数契约失败")
    if (
        int(level[0]) != profile.levels
        or int(level[1] or 0)
        or int(level[2] or 0)
    ):
        raise ValueError("OKX L2 level 键或删除语义契约失败")
    orphan = db.execute(
        f"SELECT COUNT(*) FROM {BOOK_L2_LEVEL_DATASET} l LEFT JOIN "
        f"{BOOK_L2_FRAME_DATASET} f ON f.frame_id=l.frame_id "
        "WHERE f.frame_id IS NULL"
    ).fetchone()
    if orphan is None or int(orphan[0]):
        raise ValueError("OKX L2 level 存在 orphan")


def _prepare_attempt(
    root: Path,
    conn: sqlite3.Connection,
    item: OkxArchiveInput,
    *,
    require_full_day: bool,
) -> _PreparedAttempt:
    """短时持锁登记输入、能力绑定和 running 尝试。"""
    with sqlite_writer_lock(root):
        registry.register_all(conn)
        ensure_markets(conn)
        market_id, instrument_id, mapping_revision = _market_row(
            conn, "okx", item.venue_symbol, None
        )
        capability_revision = _capability_revision(conn)
        _register_source(root, conn, item)
        conn.commit()
        input_hash = _input_set_hash([item.artifact])
        existing = conn.execute(
            "SELECT a.attempt_id,a.status,a.normalized_rows,"
            "MAX(CASE WHEN o.dataset=? THEN r.storage_path END),"
            "MAX(CASE WHEN o.dataset=? THEN r.storage_path END),"
            "MAX(CASE WHEN o.dataset=? THEN o.row_count END) "
            "FROM partition_attempt a JOIN materialization_output o "
            "ON o.attempt_id=a.attempt_id JOIN artifact r "
            "ON r.artifact_id=o.artifact_id "
            "WHERE a.market_id=? AND a.domain='book_l2' "
            "AND a.partition_key=? AND a.normalization_version=? "
            "AND a.input_set_hash=? AND a.status='complete' "
            "GROUP BY a.attempt_id,a.status,a.normalized_rows LIMIT 1",
            (
                BOOK_L2_FRAME_DATASET, BOOK_L2_LEVEL_DATASET,
                BOOK_L2_LEVEL_DATASET, market_id, item.partition_key,
                BOOK_L2_NORMALIZATION_VERSION, input_hash,
            ),
        ).fetchone()
        if existing is not None and existing[3] and existing[4]:
            reused = OkxL2Result(
                attempt_id=str(existing[0]), market_id=market_id,
                partition_key=item.partition_key, status=str(existing[1]),
                frame_rows=int(existing[2]), level_rows=int(existing[5]),
                frame_path=str(existing[3]), level_path=str(existing[4]),
                reused=True,
            )
            return _PreparedAttempt(
                str(existing[0]), market_id, instrument_id,
                mapping_revision, capability_revision, reused,
            )
        attempt_id = f"okx-l2-{uuid.uuid4().hex}"
        config_hash = hashlib.sha256(json.dumps({
            "dataset": [BOOK_L2_FRAME_DATASET, BOOK_L2_LEVEL_DATASET],
            "normalization_version": BOOK_L2_NORMALIZATION_VERSION,
            "schema_version": BOOK_L2_SCHEMA_VERSION,
            "payload_schema_version": OKX_PAYLOAD_SCHEMA_VERSION,
            "depth_limit": item.depth_limit,
            "require_full_day": require_full_day,
        }, sort_keys=True).encode()).hexdigest()
        conn.execute(
            "INSERT INTO partition_attempt "
            "(attempt_id,market_id,domain,partition_key,normalization_version,"
            "input_set_hash,status,source_rows,normalized_rows,ignored_rows,"
            "rejected_rows,started_at,code_version,config_hash) "
            "VALUES (?,?,?,?,?,?,'running',0,0,0,0,?,'working-tree',?)",
            (
                attempt_id, market_id, "book_l2", item.partition_key,
                BOOK_L2_NORMALIZATION_VERSION, input_hash, utc_now(),
                config_hash,
            ),
        )
        _bind_capability(conn, attempt_id, capability_revision)
        conn.commit()
        return _PreparedAttempt(
            attempt_id, market_id, instrument_id, mapping_revision,
            capability_revision, None,
        )


def _commit_outputs(
    root: Path,
    conn: sqlite3.Connection,
    item: OkxArchiveInput,
    prepared: _PreparedAttempt,
    profile: ReplayProfile,
    frame_path: Path,
    frame_sha: str,
    level_path: Path,
    level_sha: str,
    manifest_path: Path,
    finished: str,
) -> None:
    """短时持锁提交双输出、输入计数、覆盖与活动头。"""
    with sqlite_writer_lock(root):
        conn.execute("BEGIN IMMEDIATE")
        for identity, dataset, path, sha, rows in (
            (
                artifact_id(frame_sha), BOOK_L2_FRAME_DATASET,
                frame_path, frame_sha, profile.frames,
            ),
            (
                artifact_id(level_sha), BOOK_L2_LEVEL_DATASET,
                level_path, level_sha, profile.levels,
            ),
        ):
            _register_content_artifact(
                conn, identity, "materialized_parquet",
                _relative_storage_path(root, path), sha, path.stat().st_size,
                finished, BOOK_L2_SCHEMA_VERSION,
            )
            conn.execute(
                "INSERT INTO materialization_output VALUES (?,?,?,?,?,?,?)",
                (
                    prepared.attempt_id, identity, dataset, rows,
                    profile.min_event_time, profile.max_event_time, finished,
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
            "(attempt_id,artifact_id,source_rows,normalized_rows,"
            "ignored_rows,rejected_rows) VALUES (?,?,?,?,0,0)",
            (
                prepared.attempt_id, item.artifact.artifact_id,
                profile.frames, profile.frames,
            ),
        )
        conn.execute(
            "INSERT INTO partition_input_binding "
            "(attempt_id,artifact_id,storage_path,source_rows,normalized_rows,"
            "ignored_rows,rejected_rows) VALUES (?,?,?,?,?,0,0)",
            (
                prepared.attempt_id, item.artifact.artifact_id,
                item.artifact.storage_path, profile.frames, profile.frames,
            ),
        )
        conn.execute(
            "UPDATE partition_attempt SET status='complete',source_rows=?,"
            "normalized_rows=?,finished_at=? WHERE attempt_id=?",
            (
                profile.frames, profile.frames, finished,
                prepared.attempt_id,
            ),
        )
        conn.execute(
            "INSERT INTO materialization_partition_head VALUES (?,?,?,?,?,?) "
            "ON CONFLICT(market_id,domain,partition_key) DO UPDATE SET "
            "normalization_version=excluded.normalization_version,"
            "attempt_id=excluded.attempt_id,"
            "activated_at=excluded.activated_at",
            (
                prepared.market_id, "book_l2", item.partition_key,
                BOOK_L2_NORMALIZATION_VERSION, prepared.attempt_id, finished,
            ),
        )
        conn.execute(
            "INSERT INTO archive_coverage VALUES (?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(venue_id,venue_symbol,domain,day) DO UPDATE SET "
            "rows=excluded.rows,first_ts=excluded.first_ts,"
            "last_ts=excluded.last_ts,status=excluded.status,"
            "ingest_time=excluded.ingest_time",
            (
                "okx", item.venue_symbol, "book_l2",
                item.day.replace("-", ""), profile.frames,
                profile.min_event_time, profile.max_event_time,
                "ok", finished,
            ),
        )
        conn.commit()


def _mark_failed(
    root: Path,
    conn: sqlite3.Connection,
    attempt_id: str,
    exc: Exception,
) -> None:
    """短时持锁把未完成尝试标记为失败。"""
    conn.rollback()
    with sqlite_writer_lock(root):
        conn.execute(
            "UPDATE partition_attempt SET status='failed',finished_at=?,"
            "failure_detail=? WHERE attempt_id=? AND status='running'",
            (utc_now(), str(exc)[:2000], attempt_id),
        )
        conn.commit()


def materialize_archive(
    root: Path,
    conn: sqlite3.Connection,
    item: OkxArchiveInput,
    *,
    require_full_day: bool = True,
) -> OkxL2Result:
    """原子物化一份已封口 OKX 日档。"""
    prepared = _prepare_attempt(
        root, conn, item, require_full_day=require_full_day
    )
    if prepared.reused_result is not None:
        return prepared.reused_result
    attempt_id = prepared.attempt_id
    market_id = prepared.market_id
    output_dir = _resolve_recorded_path(
        root,
        PurePosixPath(
            "materialized", "book_l2",
            f"schema_version={BOOK_L2_SCHEMA_VERSION}",
            f"normalization_version={BOOK_L2_NORMALIZATION_VERSION}",
            "venue_id=okx", f"market_id={market_id}",
            f"event_day={item.day}",
        ).as_posix(),
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    frame_csv = output_dir / f".{attempt_id}.frames.csv"
    level_csv = output_dir / f".{attempt_id}.levels.csv"
    frame_tmp = output_dir / f".{attempt_id}.frames.parquet"
    level_tmp = output_dir / f".{attempt_id}.levels.parquet"
    try:
        profile = _stage(
            item,
            market_id=market_id,
            mapping_revision=prepared.mapping_revision,
            capability_revision=prepared.capability_revision,
            instrument_id=prepared.instrument_id,
            frame_csv=frame_csv,
            level_csv=level_csv,
            require_full_day=require_full_day,
        )
        db: Any = duckdb.connect(":memory:")
        db.execute("SET TimeZone='UTC'")
        try:
            create_book_l2_tables(db)
            _copy_csv(db, BOOK_L2_FRAME_DATASET, frame_csv)
            _copy_csv(db, BOOK_L2_LEVEL_DATASET, level_csv)
            _validate_tables(
                db,
                profile=profile,
                market_id=market_id,
                artifact_identity=item.artifact.artifact_id,
            )
            _write_parquet(
                db,
                f"SELECT * FROM {BOOK_L2_FRAME_DATASET} "
                "ORDER BY event_time,frame_id",
                frame_tmp,
            )
            _write_parquet(
                db,
                f"SELECT * FROM {BOOK_L2_LEVEL_DATASET} "
                "ORDER BY frame_id,side,source_level_index",
                level_tmp,
            )
        finally:
            db.close()
        frame_csv.unlink()
        level_csv.unlink()
        frame_path, frame_sha = _finalize(frame_tmp)
        level_path, level_sha = _finalize(level_tmp)
        finished = utc_now()
        frame_storage = _relative_storage_path(root, frame_path)
        level_storage = _relative_storage_path(root, level_path)
        manifest_body = {
            "attempt_id": attempt_id,
            "status": "complete",
            "market_id": market_id,
            "partition_key": item.partition_key,
            "normalization_version": BOOK_L2_NORMALIZATION_VERSION,
            "schema_version": BOOK_L2_SCHEMA_VERSION,
            "capability_revision": prepared.capability_revision,
            "input_artifact_id": item.artifact.artifact_id,
            "profile": asdict(profile),
            "outputs": {
                BOOK_L2_FRAME_DATASET: frame_storage,
                BOOK_L2_LEVEL_DATASET: level_storage,
            },
        }
        manifest_path = output_dir / f"manifest-{attempt_id}.json"
        atomic_write_text(
            manifest_path,
            json.dumps(manifest_body, ensure_ascii=False, indent=2) + "\n",
        )
        _commit_outputs(
            root, conn, item, prepared, profile,
            frame_path, frame_sha, level_path, level_sha,
            manifest_path, finished,
        )
        return OkxL2Result(
            attempt_id=attempt_id, market_id=market_id,
            partition_key=item.partition_key, status="complete",
            frame_rows=profile.frames, level_rows=profile.levels,
            frame_path=frame_storage, level_path=level_storage, reused=False,
        )
    except Exception as exc:
        for path in (frame_csv, level_csv, frame_tmp, level_tmp):
            try:
                path.unlink()
            except FileNotFoundError:
                pass
        _mark_failed(root, conn, attempt_id, exc)
        raise


def materialize_all(
    root: Path, conn: sqlite3.Connection
) -> list[OkxL2Result]:
    """逐日断点复用地物化全部 OKX 封口原件。"""
    inputs = sealed_inputs(root)
    results: list[OkxL2Result] = []
    for index, item in enumerate(inputs, start=1):
        result = materialize_archive(root, conn, item)
        results.append(result)
        print(
            f"[{index}/{len(inputs)}] "
            f"{'REUSED' if result.reused else 'DONE'} "
            f"{result.market_id} {result.partition_key} "
            f"frames={result.frame_rows:,} levels={result.level_rows:,}",
            flush=True,
        )
    return results


def audit_okx_l2(
    root: Path,
    conn: sqlite3.Connection,
    *,
    from_day: str | None = None,
    to_day: str | None = None,
) -> dict[str, object]:
    """逐活动日复核 OKX L2 的散列、PIT、键与能力绑定。"""
    errors: list[str] = []
    filters = ""
    parameters: list[object] = [
        BOOK_L2_FRAME_DATASET,
        BOOK_L2_FRAME_DATASET,
        BOOK_L2_LEVEL_DATASET,
        BOOK_L2_LEVEL_DATASET,
        BOOK_L2_LEVEL_DATASET,
    ]
    if from_day is not None:
        filters += " AND a.partition_key>=?"
        parameters.append(from_day)
    if to_day is not None:
        filters += " AND a.partition_key<=?"
        parameters.append(to_day)
    rows = conn.execute(
        "SELECT a.attempt_id,a.market_id,a.partition_key,a.source_rows,"
        "a.normalized_rows,a.ignored_rows,a.rejected_rows,"
        "MAX(CASE WHEN o.dataset=? THEN r.storage_path END),"
        "MAX(CASE WHEN o.dataset=? THEN r.sha256 END),"
        "MAX(CASE WHEN o.dataset=? THEN r.storage_path END),"
        "MAX(CASE WHEN o.dataset=? THEN r.sha256 END),"
        "MAX(CASE WHEN o.dataset=? THEN o.row_count END) "
        "FROM materialization_partition_head h JOIN partition_attempt a "
        "ON a.attempt_id=h.attempt_id JOIN market m ON m.market_id=a.market_id "
        "JOIN materialization_output o ON o.attempt_id=a.attempt_id "
        "JOIN artifact r ON r.artifact_id=o.artifact_id "
        "WHERE h.domain='book_l2' AND m.venue_id='okx' "
        f"{filters} "
        "GROUP BY a.attempt_id,a.market_id,a.partition_key,a.source_rows,"
        "a.normalized_rows,a.ignored_rows,a.rejected_rows "
        "ORDER BY a.market_id,a.partition_key",
        parameters,
    ).fetchall()
    if not rows:
        return {"ok": False, "errors": ["没有活动 OKX L2 分区"]}
    total_frames = 0
    total_levels = 0
    partition_results: list[dict[str, object]] = []
    db: Any = duckdb.connect(":memory:")
    db.execute("SET TimeZone='UTC'")
    try:
        for row in rows:
            attempt = str(row[0])
            day = str(row[2])
            prefix = f"{row[1]}/{day}"
            source, normalized, ignored, rejected = map(int, row[3:7])
            partition_errors: list[str] = []
            if source != normalized + ignored + rejected or ignored or rejected:
                partition_errors.append("来源分类不守恒或存在未接受行")
            if not row[7] or not row[9]:
                partition_errors.append("双输出缺失")
                errors.extend(f"{prefix}: {item}" for item in partition_errors)
                continue
            frame_path = root / str(row[7])
            level_path = root / str(row[9])
            if not frame_path.is_file() or not level_path.is_file():
                partition_errors.append("输出文件缺失")
                errors.extend(f"{prefix}: {item}" for item in partition_errors)
                continue
            if sha256_file(frame_path) != str(row[8]):
                partition_errors.append("frame 散列不符")
            if sha256_file(level_path) != str(row[10]):
                partition_errors.append("level 散列不符")
            frame = db.execute(
                "SELECT COUNT(*),COUNT(*)-COUNT(DISTINCT frame_id),"
                "SUM(available_time<event_time),"
                "SUM(changed_bid_levels+changed_ask_levels),"
                "COUNT(DISTINCT source_artifact_id),"
                "COUNT(DISTINCT capability_revision),"
                "MIN(capability_revision),"
                "SUM(CASE WHEN endpoint<>? OR book_mode<>? "
                "OR replay_fidelity<>? OR normalization_version<>? "
                "OR schema_version<>? OR sequence_id IS NOT NULL "
                "OR prev_sequence_id IS NOT NULL OR checksum IS NOT NULL "
                "THEN 1 ELSE 0 END) FROM read_parquet(?)",
                [
                    OKX_BOOK_HISTORY_ENDPOINT,
                    OKX_BOOK_MODE,
                    OKX_REPLAY_FIDELITY,
                    BOOK_L2_NORMALIZATION_VERSION,
                    BOOK_L2_SCHEMA_VERSION,
                    str(frame_path),
                ],
            ).fetchone()
            level = db.execute(
                "SELECT COUNT(*),COUNT(*)-COUNT(DISTINCT "
                "(frame_id,side,source_level_index)),"
                "SUM(CASE WHEN side NOT IN ('bid','ask') OR action NOT IN "
                "('set','delete') OR order_count<0 OR "
                "normalization_version<>? OR schema_version<>? OR "
                "((size='0')<>(order_count=0)) THEN 1 ELSE 0 END) "
                "FROM read_parquet(?)",
                [
                    BOOK_L2_NORMALIZATION_VERSION,
                    BOOK_L2_SCHEMA_VERSION,
                    str(level_path),
                ],
            ).fetchone()
            if frame is None or level is None:
                partition_errors.append("Parquet 不可读")
                level_rows = 0
            else:
                level_rows = int(level[0])
                if (
                    int(frame[0]) != normalized
                    or int(frame[1] or 0)
                    or int(frame[2] or 0)
                    or int(frame[3] or 0) != level_rows
                    or int(frame[4]) != 1
                    or int(frame[5]) != 1
                    or int(frame[7] or 0)
                ):
                    partition_errors.append("frame 身份、PIT、端点或计数失败")
                if (
                    level_rows != int(row[11])
                    or int(level[1] or 0)
                    or int(level[2] or 0)
                ):
                    partition_errors.append("level 键或删除语义失败")
                binding = conn.execute(
                    "SELECT revision_id,binding_basis FROM "
                    "partition_capability_binding WHERE attempt_id=? "
                    "AND venue_id='okx' AND domain='book_history' "
                    "AND endpoint=?",
                    (attempt, OKX_BOOK_HISTORY_ENDPOINT),
                ).fetchall()
                if (
                    len(binding) != 1
                    or int(binding[0][0]) != int(frame[6])
                    or str(binding[0][1]) != "recorded"
                ):
                    partition_errors.append("能力修订绑定失败")
            total_frames += normalized
            total_levels += level_rows
            errors.extend(f"{prefix}: {item}" for item in partition_errors)
            partition_results.append({
                "attempt_id": attempt,
                "market_id": str(row[1]),
                "partition_key": day,
                "frames": normalized,
                "levels": level_rows,
                "ok": not partition_errors,
            })
    finally:
        db.close()
    foreign_key_errors = conn.execute("PRAGMA foreign_key_check").fetchall()
    if foreign_key_errors:
        errors.append(f"SQLite 外键错误 {len(foreign_key_errors)} 条")
    result: dict[str, object] = {
        "ok": not errors,
        "partition_count": len(rows),
        "frames": total_frames,
        "levels": total_levels,
        "partitions": partition_results,
        "errors": errors,
    }
    if len(partition_results) == 1:
        result.update({
            "attempt_id": partition_results[0]["attempt_id"],
            "market_id": partition_results[0]["market_id"],
            "partition_key": partition_results[0]["partition_key"],
        })
    return result


def main(argv: Sequence[str] | None = None) -> int:
    """命令行入口。"""
    parser = argparse.ArgumentParser(description="OKX 历史 L2 物化")
    parser.add_argument("--data-root", type=Path, default=None)
    parser.add_argument("command", choices=("all", "audit"))
    parser.add_argument("--from-day")
    parser.add_argument("--to-day")
    args = parser.parse_args(argv)
    root = (args.data_root or configured_data_root()).resolve()
    conn = store.connect(root)
    try:
        if args.command == "all":
            result: object = [
                asdict(item) for item in materialize_all(root, conn)
            ]
            code = 0
        else:
            result = audit_okx_l2(
                root, conn, from_day=args.from_day, to_day=args.to_day
            )
            code = 0 if bool(result["ok"]) else 1
    finally:
        conn.close()
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
