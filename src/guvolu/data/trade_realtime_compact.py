"""逐笔实时活动 head 按日合并（TBD-40）。

实时逐笔物化按封口段落 Parquet，活动 head 随段数线性增长；冻结研究树按
物理文件逐个拼资格谓词，每小时预测耗时随之线性上升。本模块把已过当日的
实时段按 UTC 日拼接为单一 Parquet，登记为新 attempt 与输出，切换活动
head 到日分区 ``day/YYYY-MM-DD``，并撤销被合并段的活动指针。

原段制品、attempt 与输出一律保留（D-02），旧输入收据仍可重放；只有活动
head 表变化，与 book-state 撤销旧分段头的做法一致。合并件继承
``normalization_version`` 与 ``schema_version``，行数为各段之和，来源
行按 ``source_artifact_id`` 归入 raw 段制品的输入登记，血缘经
``materialization_dependency`` 指向各段 attempt。
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Any

import duckdb

from guvolu.data import store
from guvolu.data.materialize import (
    DATASET_TRADE,
    _register_content_artifact,
    _relative_storage_path,
    _resolve_recorded_path,
    artifact_id,
    sha256_file,
    utc_now,
)
from guvolu.data.paths import data_root as configured_data_root
from guvolu.data.sqlite_writer_lock import sqlite_writer_lock
from guvolu.data.trade_realtime_materialize import (
    TRADE_REALTIME_NORMALIZATION_VERSION,
    TRADE_REALTIME_SCHEMA_VERSION,
)

DOMAIN = "trade_realtime"
# 日分区键前缀
PARTITION_PREFIX = "day/"
# 当日结束后再等这么久才合并
DEFAULT_GRACE_SECONDS = 3600
COMPACTION_METHOD_VERSION = "trade-realtime-day-compaction-v1"


class CompactionError(RuntimeError):
    """合并前置条件或一致性校验失败。"""


@dataclass(frozen=True)
class HeadOutput:
    """一个活动 head 及其成交输出。"""

    partition_key: str
    attempt_id: str
    normalization_version: str
    artifact_id: str
    storage_path: str
    row_count: int
    min_event_time: str | None
    max_event_time: str | None

    @property
    def is_day(self) -> bool:
        return self.partition_key.startswith(PARTITION_PREFIX)


@dataclass(frozen=True)
class DayPlan:
    """一个 UTC 日待合并的活动 head 集合。"""

    day: date
    heads: tuple[HeadOutput, ...]

    @property
    def partition_key(self) -> str:
        return f"{PARTITION_PREFIX}{self.day.isoformat()}"

    @property
    def row_count(self) -> int:
        return sum(head.row_count for head in self.heads)


@dataclass(frozen=True)
class CompactionResult:
    """一次日合并的结果。"""

    market_id: str
    partition_key: str
    attempt_id: str
    status: str
    merged_heads: int
    row_count: int
    output_path: str
    reused: bool


def _active_heads(
    conn: sqlite3.Connection, market_id: str,
) -> list[HeadOutput]:
    rows = conn.execute(
        "SELECT h.partition_key,h.attempt_id,h.normalization_version,"
        "o.artifact_id,a.storage_path,o.row_count,o.min_event_time,"
        "o.max_event_time FROM materialization_partition_head h "
        "JOIN materialization_output o ON o.attempt_id=h.attempt_id "
        "JOIN artifact a ON a.artifact_id=o.artifact_id "
        "WHERE h.market_id=? AND h.domain=? AND o.dataset=? "
        "ORDER BY h.partition_key",
        (market_id, DOMAIN, DATASET_TRADE),
    ).fetchall()
    return [
        HeadOutput(
            str(row[0]), str(row[1]), str(row[2]), str(row[3]), str(row[4]),
            int(row[5]),
            None if row[6] is None else str(row[6]),
            None if row[7] is None else str(row[7]),
        )
        for row in rows
    ]


def plan_days(
    heads: Sequence[HeadOutput],
    *,
    now: datetime,
    grace_seconds: int = DEFAULT_GRACE_SECONDS,
) -> list[DayPlan]:
    """按输出最早事件时间的 UTC 日分组，只保留已过宽限期的日。"""
    if now.tzinfo is None:
        raise CompactionError("当前时刻必须带时区")
    grouped: dict[date, list[HeadOutput]] = {}
    for head in heads:
        # 零行或无事件时间的段不合并
        if head.min_event_time is None or head.row_count <= 0:
            continue
        if head.normalization_version != TRADE_REALTIME_NORMALIZATION_VERSION:
            continue
        day = datetime.fromisoformat(head.min_event_time).astimezone(UTC).date()
        grouped.setdefault(day, []).append(head)
    plans: list[DayPlan] = []
    for day in sorted(grouped):
        day_end = datetime.combine(day, datetime.min.time(), UTC) + timedelta(
            days=1
        )
        if now < day_end + timedelta(seconds=grace_seconds):
            continue
        members = tuple(sorted(grouped[day], key=lambda item: item.partition_key))
        # 只剩日分区本身即已合并
        if len(members) == 1 and members[0].is_day:
            continue
        plans.append(DayPlan(day, members))
    return plans


def _input_set_hash(heads: Sequence[HeadOutput]) -> str:
    body = "\n".join(sorted(
        f"{head.artifact_id}|{head.storage_path}" for head in heads
    ))
    return hashlib.sha256(body.encode("ascii")).hexdigest()


def _quote(text: str) -> str:
    return text.replace("'", "''")


def _write_merged_parquet(
    sources: Sequence[Path], destination: Path,
) -> tuple[int, str | None, str | None, dict[str, int]]:
    """拼接来源 Parquet，返回行数、时间边界与来源制品行数。"""
    listing = "[" + ",".join(
        "'" + _quote(path.as_posix()) + "'" for path in sources
    ) + "]"
    db: Any = duckdb.connect(":memory:")
    try:
        db.execute("SET TimeZone='UTC'")
        db.execute(
            f"CREATE TABLE merged AS SELECT * FROM read_parquet({listing},"
            "union_by_name=true)"
        )
        total, distinct, low, high = db.execute(
            "SELECT COUNT(*),COUNT(DISTINCT observation_id),"
            "MIN(event_time),MAX(event_time) FROM merged"
        ).fetchone()
        if int(total) != int(distinct):
            raise CompactionError("跨段存在重复观察，拒绝合并")
        db.execute(
            "COPY (SELECT * FROM merged ORDER BY event_time,venue_trade_id,"
            "source_row_index,source_item_index) "
            f"TO '{_quote(destination.as_posix())}' "
            "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880)"
        )
        per_source = {
            str(row[0]): int(row[1])
            for row in db.execute(
                "SELECT source_artifact_id,COUNT(*) FROM merged GROUP BY 1"
            ).fetchall()
        }
    finally:
        db.close()
    with destination.open("rb+") as handle:
        handle.flush()
        os.fsync(handle.fileno())
    return (
        int(total),
        None if low is None else low.isoformat(),
        None if high is None else high.isoformat(),
        per_source,
    )


def _finalize(temporary: Path) -> tuple[Path, str]:
    sha = sha256_file(temporary)
    final = temporary.with_name(f"part-{sha[:12]}.parquet")
    if final.exists():
        if sha256_file(final) != sha:
            raise CompactionError(f"合并输出散列冲突: {final}")
        temporary.unlink()
    else:
        os.replace(temporary, final)
    return final, sha


def _source_bindings(
    conn: sqlite3.Connection, attempt_ids: Sequence[str],
) -> dict[str, str]:
    """来源 attempt 的 raw 输入制品到位置绑定。"""
    bindings: dict[str, str] = {}
    for attempt_id in attempt_ids:
        for row in conn.execute(
            "SELECT artifact_id,storage_path FROM partition_input_binding "
            "WHERE attempt_id=?",
            (attempt_id,),
        ).fetchall():
            bindings.setdefault(str(row[0]), str(row[1]))
    return bindings


def _capability_bindings(
    conn: sqlite3.Connection, attempt_ids: Sequence[str],
) -> list[tuple[str, str, str, int]]:
    """来源 attempt 能力绑定按端点取最高修订。"""
    latest: dict[tuple[str, str, str], int] = {}
    for attempt_id in attempt_ids:
        for row in conn.execute(
            "SELECT venue_id,domain,endpoint,revision_id "
            "FROM partition_capability_binding WHERE attempt_id=?",
            (attempt_id,),
        ).fetchall():
            key = (str(row[0]), str(row[1]), str(row[2]))
            latest[key] = max(latest.get(key, -1), int(row[3]))
    return [(*key, revision) for key, revision in sorted(latest.items())]


def _venue_id(storage_path: str) -> str:
    for part in PurePosixPath(storage_path).parts:
        if part.startswith("venue_id="):
            return part.split("=", 1)[1]
    raise CompactionError(f"输出路径缺少 venue_id: {storage_path}")


def _switch_heads(
    conn: sqlite3.Connection,
    market_id: str,
    plan: DayPlan,
    attempt_id: str,
    activated_at: str,
) -> None:
    """撤销被合并段的活动指针并指向合并 attempt。"""
    conn.executemany(
        "DELETE FROM materialization_partition_head "
        "WHERE market_id=? AND domain=? AND partition_key=?",
        [(market_id, DOMAIN, head.partition_key) for head in plan.heads],
    )
    conn.execute(
        "INSERT INTO materialization_partition_head VALUES (?,?,?,?,?,?) "
        "ON CONFLICT(market_id,domain,partition_key) DO UPDATE SET "
        "normalization_version=excluded.normalization_version,"
        "attempt_id=excluded.attempt_id,activated_at=excluded.activated_at",
        (
            market_id, DOMAIN, plan.partition_key,
            TRADE_REALTIME_NORMALIZATION_VERSION, attempt_id, activated_at,
        ),
    )


def compact_day(
    root: Path, conn: sqlite3.Connection, market_id: str, plan: DayPlan,
) -> CompactionResult:
    """把一日的活动段拼接为单一输出并切换活动 head。"""
    if not plan.heads:
        raise CompactionError("空合并计划")
    input_hash = _input_set_hash(plan.heads)
    source_attempts = [head.attempt_id for head in plan.heads]
    existing = conn.execute(
        "SELECT a.attempt_id,r.storage_path,o.row_count "
        "FROM partition_attempt a JOIN materialization_output o "
        "ON o.attempt_id=a.attempt_id JOIN artifact r "
        "ON r.artifact_id=o.artifact_id "
        "WHERE a.market_id=? AND a.domain=? AND a.partition_key=? "
        "AND a.normalization_version=? AND a.input_set_hash=? "
        "AND a.status='complete' LIMIT 1",
        (
            market_id, DOMAIN, plan.partition_key,
            TRADE_REALTIME_NORMALIZATION_VERSION, input_hash,
        ),
    ).fetchone()
    if existing is not None:
        # 同一输入集已合并过，只需重新指向
        conn.execute("BEGIN IMMEDIATE")
        _switch_heads(conn, market_id, plan, str(existing[0]), utc_now())
        conn.commit()
        return CompactionResult(
            market_id, plan.partition_key, str(existing[0]), "complete",
            len(plan.heads), int(existing[2]), str(existing[1]), True,
        )
    venue_id = _venue_id(plan.heads[0].storage_path)
    config_hash = hashlib.sha256(json.dumps({
        "method_version": COMPACTION_METHOD_VERSION,
        "dataset": DATASET_TRADE,
        "normalization_version": TRADE_REALTIME_NORMALIZATION_VERSION,
        "schema_version": TRADE_REALTIME_SCHEMA_VERSION,
    }, sort_keys=True).encode()).hexdigest()
    attempt_id = f"trade-rtc-{uuid.uuid4().hex}"
    started = utc_now()
    conn.execute(
        "INSERT INTO partition_attempt "
        "(attempt_id,market_id,domain,partition_key,normalization_version,"
        "input_set_hash,status,source_rows,normalized_rows,ignored_rows,"
        "rejected_rows,started_at,code_version,config_hash) "
        "VALUES (?,?,?,?,?,?,'running',?,0,0,0,?,'working-tree',?)",
        (
            attempt_id, market_id, DOMAIN, plan.partition_key,
            TRADE_REALTIME_NORMALIZATION_VERSION, input_hash,
            plan.row_count, started, config_hash,
        ),
    )
    conn.executemany(
        "INSERT INTO partition_capability_binding VALUES (?,?,?,?,?,?,?)",
        [
            (attempt_id, *binding, "recorded", started)
            for binding in _capability_bindings(conn, source_attempts)
        ],
    )
    conn.commit()
    output_dir = _resolve_recorded_path(
        root,
        PurePosixPath(
            "materialized", DATASET_TRADE,
            f"schema_version={TRADE_REALTIME_SCHEMA_VERSION}",
            f"normalization_version={TRADE_REALTIME_NORMALIZATION_VERSION}",
            f"venue_id={venue_id}", f"market_id={market_id}",
            f"day={plan.day.isoformat()}",
        ).as_posix(),
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    temporary = output_dir / f".{attempt_id}.parquet"
    try:
        sources = [
            _resolve_recorded_path(root, head.storage_path)
            for head in plan.heads
        ]
        for head, path in zip(plan.heads, sources, strict=True):
            if not path.is_file():
                raise CompactionError(f"活动段制品缺失: {head.storage_path}")
        rows, low, high, per_source = _write_merged_parquet(
            sources, temporary
        )
        if rows != plan.row_count:
            raise CompactionError("合并行数与控制面各段行数之和不符")
        raw_bindings = _source_bindings(conn, source_attempts)
        missing = sorted(set(per_source).difference(raw_bindings))
        if missing:
            raise CompactionError(f"来源 raw 制品无位置绑定: {missing[:3]}")
        output_path, output_sha = _finalize(temporary)
        storage = _relative_storage_path(root, output_path)
        finished = utc_now()
        conn.execute("BEGIN IMMEDIATE")
        output_id = artifact_id(output_sha)
        _register_content_artifact(
            conn, output_id, "materialized_parquet", storage, output_sha,
            output_path.stat().st_size, finished,
            TRADE_REALTIME_SCHEMA_VERSION,
        )
        conn.execute(
            "INSERT INTO materialization_output VALUES (?,?,?,?,?,?,?)",
            (attempt_id, output_id, DATASET_TRADE, rows, low, high, finished),
        )
        for raw_id, count in sorted(per_source.items()):
            conn.execute(
                "INSERT INTO partition_input "
                "(attempt_id,artifact_id,source_rows,normalized_rows,"
                "ignored_rows,rejected_rows) VALUES (?,?,?,?,0,0)",
                (attempt_id, raw_id, count, count),
            )
            conn.execute(
                "INSERT INTO partition_input_binding "
                "(attempt_id,artifact_id,storage_path,source_rows,"
                "normalized_rows,ignored_rows,rejected_rows) "
                "VALUES (?,?,?,?,?,0,0)",
                (attempt_id, raw_id, raw_bindings[raw_id], count, count),
            )
        conn.executemany(
            "INSERT INTO materialization_dependency VALUES (?,?,?,?)",
            [
                (attempt_id, upstream, "active-head", finished)
                for upstream in source_attempts
            ],
        )
        conn.execute(
            "UPDATE partition_attempt SET status='complete',"
            "normalized_rows=?,finished_at=? WHERE attempt_id=?",
            (rows, finished, attempt_id),
        )
        _switch_heads(conn, market_id, plan, attempt_id, finished)
        conn.commit()
        return CompactionResult(
            market_id, plan.partition_key, attempt_id, "complete",
            len(plan.heads), rows, storage, False,
        )
    except Exception as exc:
        temporary.unlink(missing_ok=True)
        conn.execute(
            "UPDATE partition_attempt SET status='failed',finished_at=?,"
            "failure_detail=? WHERE attempt_id=? AND status='running'",
            (utc_now(), str(exc)[:2000], attempt_id),
        )
        conn.commit()
        raise


def compact_market(
    root: Path,
    conn: sqlite3.Connection,
    market_id: str,
    *,
    now: datetime | None = None,
    grace_seconds: int = DEFAULT_GRACE_SECONDS,
) -> list[CompactionResult]:
    """在写锁内合并一个市场所有已过宽限期的日。"""
    moment = now if now is not None else datetime.now(UTC)
    with sqlite_writer_lock(root):
        plans = plan_days(
            _active_heads(conn, market_id), now=moment,
            grace_seconds=grace_seconds,
        )
        return [compact_day(root, conn, market_id, plan) for plan in plans]


def _result_payload(result: CompactionResult) -> dict[str, object]:
    return {
        "market_id": result.market_id,
        "partition_key": result.partition_key,
        "attempt_id": result.attempt_id,
        "status": result.status,
        "merged_heads": result.merged_heads,
        "row_count": result.row_count,
        "output_path": result.output_path,
        "reused": result.reused,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="逐笔实时活动 head 按日合并")
    parser.add_argument("--data-root", type=Path, default=None)
    parser.add_argument("--market-id", required=True)
    parser.add_argument(
        "--grace-seconds", type=int, default=DEFAULT_GRACE_SECONDS,
    )
    parser.add_argument(
        "--plan-only", action="store_true", help="只列出待合并日，不写入",
    )
    args = parser.parse_args(argv)
    root = (
        args.data_root.resolve() if args.data_root is not None
        else configured_data_root()
    )
    now = datetime.now(UTC)
    if args.plan_only:
        conn = store.connect_readonly(root)
        if conn is None:
            raise SystemExit("控制库不可读")
        try:
            plans = plan_days(
                _active_heads(conn, str(args.market_id)), now=now,
                grace_seconds=int(args.grace_seconds),
            )
        finally:
            conn.close()
        print(json.dumps([
            {
                "day": plan.day.isoformat(),
                "heads": len(plan.heads),
                "row_count": plan.row_count,
            }
            for plan in plans
        ], ensure_ascii=False))
        return 0
    conn = store.connect(root)
    try:
        results = compact_market(
            root, conn, str(args.market_id), now=now,
            grace_seconds=int(args.grace_seconds),
        )
    finally:
        conn.close()
    print(json.dumps(
        [_result_payload(result) for result in results], ensure_ascii=False,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
