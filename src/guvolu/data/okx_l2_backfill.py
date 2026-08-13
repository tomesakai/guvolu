"""OKX 400 档历史 L2 的日级求缺、磁盘门禁与回补编排。"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sqlite3
import uuid
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_CEILING
from pathlib import Path

import duckdb
import requests

from guvolu.data import store
from guvolu.data.book_l2_contract import BOOK_L2_NORMALIZATION_VERSION
from guvolu.data.materialize import ensure_markets, utc_now
from guvolu.data.okx_l2_archive import (
    download_archive,
    request_download_plan,
    request_instrument,
)
from guvolu.data.okx_l2_materialize import (
    materialize_archive,
    sealed_input,
    sealed_inputs,
)
from guvolu.data.paths import data_root as configured_data_root
from guvolu.data.sqlite_writer_lock import sqlite_writer_lock
from guvolu.venues import registry

OKX_L2_HISTORY_START = date(2023, 3, 1)
VERIFIED_DEPTH_LEVELS = 400
DEFAULT_RESERVE_GIB = 20
MAX_CONSECUTIVE_FAILURES = 3
GIB = 1024 ** 3

# 单日实测校准值。
CALIBRATED_RAW_BYTES = 111_062_744
CALIBRATED_OUTPUT_BYTES = 467_897_746
CALIBRATED_TEMP_BYTES = 7_213_750_978
MIN_WORKING_FREE_BYTES = 10 * GIB


@dataclass(frozen=True, slots=True)
class OkxL2BackfillTask:
    """一个 UTC 日的本地求缺结果。"""

    venue_symbol: str
    day: str
    depth_levels: int
    status: str
    frame_rows: int
    manifest_path: str | None
    reason: str | None


@dataclass(frozen=True, slots=True)
class OkxL2BackfillPlan:
    """一个市场日区间的可重算回补计划。"""

    market_id: str
    tasks: tuple[OkxL2BackfillTask, ...]

    @property
    def active_tasks(self) -> tuple[OkxL2BackfillTask, ...]:
        return tuple(task for task in self.tasks if task.status == "active")

    @property
    def sealed_tasks(self) -> tuple[OkxL2BackfillTask, ...]:
        return tuple(task for task in self.tasks if task.status == "sealed")

    @property
    def pending_tasks(self) -> tuple[OkxL2BackfillTask, ...]:
        return tuple(task for task in self.tasks if task.status == "pending")

    @property
    def blocked_tasks(self) -> tuple[OkxL2BackfillTask, ...]:
        return tuple(task for task in self.tasks if task.status.startswith("blocked"))


def _day_range(from_day: date, to_day: date) -> tuple[date, ...]:
    if from_day < OKX_L2_HISTORY_START:
        raise ValueError("OKX 历史 L2 起点不得早于 2023-03-01")
    if to_day < from_day:
        raise ValueError("to_day 不得早于 from_day")
    if to_day >= datetime.now(UTC).date():
        raise ValueError("OKX 当日历史档尚未封口")
    count = (to_day - from_day).days + 1
    return tuple(from_day + timedelta(days=index) for index in range(count))


def _market_id(
    conn: sqlite3.Connection,
    venue_symbol: str,
    *,
    initialize_dimensions: bool,
) -> str:
    if initialize_dimensions:
        registry.register_all(conn)
        ensure_markets(conn)
    row = conn.execute(
        "SELECT market_id FROM market WHERE venue_id='okx' "
        "AND venue_symbol=? ORDER BY mapping_revision DESC LIMIT 1",
        (venue_symbol,),
    ).fetchone()
    if row is None:
        raise ValueError(f"OKX 品种尚无已核证映射: {venue_symbol}")
    if initialize_dimensions:
        conn.commit()
    return str(row[0])


def plan_okx_l2_backfill(
    root: Path,
    conn: sqlite3.Connection,
    *,
    venue_symbol: str,
    from_day: date,
    to_day: date,
    depth_levels: int = VERIFIED_DEPTH_LEVELS,
    initialize_dimensions: bool = True,
) -> OkxL2BackfillPlan:
    """从封口原件和活动头现场推导日级计划。"""
    if depth_levels != VERIFIED_DEPTH_LEVELS:
        raise ValueError("5000 档尚未核证，不得进入全量回补")
    days = _day_range(from_day, to_day)
    market_id = _market_id(
        conn,
        venue_symbol,
        initialize_dimensions=initialize_dimensions,
    )
    sealed = {
        (item.venue_symbol, item.day, item.depth_limit): item
        for item in sealed_inputs(root)
    }
    active = {
        str(row[0]): (str(row[1]), int(row[2]))
        for row in conn.execute(
            "SELECT h.partition_key,h.normalization_version,a.normalized_rows "
            "FROM materialization_partition_head h "
            "JOIN partition_attempt a ON a.attempt_id=h.attempt_id "
            "WHERE h.market_id=? AND h.domain='book_l2'",
            (market_id,),
        )
    }
    tasks: list[OkxL2BackfillTask] = []
    for value in days:
        day = value.isoformat()
        item = sealed.get((venue_symbol, day, depth_levels))
        head = active.get(day)
        manifest_path = (
            item.manifest_path.relative_to(root).as_posix()
            if item is not None else None
        )
        if head is not None and item is None:
            status = "blocked_raw_missing"
            reason = "活动头存在但封口原件缺失"
        elif (
            head is not None
            and head[0] == BOOK_L2_NORMALIZATION_VERSION
            and item is not None
        ):
            status = "active"
            reason = None
        elif item is not None:
            status = "sealed"
            reason = None
        else:
            status = "pending"
            reason = None
        tasks.append(OkxL2BackfillTask(
            venue_symbol=venue_symbol,
            day=day,
            depth_levels=depth_levels,
            status=status,
            frame_rows=0 if head is None else head[1],
            manifest_path=manifest_path,
            reason=reason,
        ))
    return OkxL2BackfillPlan(market_id, tuple(tasks))


def plan_summary(plan: OkxL2BackfillPlan) -> dict[str, object]:
    """输出终端和调度器共用的简要计划。"""
    additional = (
        len(plan.pending_tasks) * (CALIBRATED_RAW_BYTES + CALIBRATED_OUTPUT_BYTES)
        + len(plan.sealed_tasks) * CALIBRATED_OUTPUT_BYTES
    )
    return {
        "market_id": plan.market_id,
        "total_days": len(plan.tasks),
        "active_days": len(plan.active_tasks),
        "sealed_days": len(plan.sealed_tasks),
        "pending_days": len(plan.pending_tasks),
        "blocked_days": len(plan.blocked_tasks),
        "active_frames": sum(task.frame_rows for task in plan.active_tasks),
        "estimated_additional_bytes": additional,
        "estimated_additional_gib": round(additional / GIB, 3),
        "calibration": "BTC-USDT/2026-08-07/400lv",
        "blocked": [asdict(task) for task in plan.blocked_tasks],
    }


def _advertised_bytes(size_mb: str) -> int:
    try:
        value = Decimal(size_mb)
    except InvalidOperation as exc:
        raise ValueError(f"OKX sizeMB 非数值: {size_mb!r}") from exc
    if not value.is_finite() or value <= 0:
        raise ValueError(f"OKX sizeMB 非正数: {size_mb!r}")
    return int((value * (1024 ** 2)).to_integral_value(rounding=ROUND_CEILING))


def required_free_bytes(advertised_raw_bytes: int, reserve_gib: int) -> int:
    """按实测放大率估计单日开工前的最小可用空间。"""
    if advertised_raw_bytes <= 0 or reserve_gib < 0:
        raise ValueError("磁盘门禁参数非法")
    calibrated_ratio = Decimal(CALIBRATED_TEMP_BYTES + CALIBRATED_OUTPUT_BYTES)
    calibrated_ratio /= Decimal(CALIBRATED_RAW_BYTES)
    working = int(
        (Decimal(advertised_raw_bytes) * calibrated_ratio).to_integral_value(
            rounding=ROUND_CEILING
        )
    )
    return reserve_gib * GIB + max(MIN_WORKING_FREE_BYTES, working)


def _record_run(
    root: Path,
    conn: sqlite3.Connection,
    *,
    run_id: str,
    venue_symbol: str,
    from_day: date,
    to_day: date,
    planned: int,
    ok: int,
    rows: int,
    status: str,
    failures: Sequence[str],
    started_at: str,
    config_hash: str,
) -> None:
    detail = None if not failures else "; ".join(failures)[-2_000:]
    row: store.BackfillRunRow = (
        run_id,
        "okx",
        venue_symbol,
        "book_l2",
        from_day.isoformat(),
        to_day.isoformat(),
        planned,
        ok,
        0,
        0,
        rows,
        0,
        status,
        detail,
        started_at,
        utc_now(),
        config_hash,
        "working-tree",
    )
    with sqlite_writer_lock(root):
        store.insert_backfill_run(conn, row)


def run_okx_l2_backfill(
    root: Path,
    conn: sqlite3.Connection,
    *,
    venue_symbol: str,
    from_day: date,
    to_day: date,
    reserve_gib: int = DEFAULT_RESERVE_GIB,
    max_days: int | None = None,
    progress: Callable[[str], None] | None = None,
) -> dict[str, object]:
    """串行执行求缺日；已活动日和已封口日均复用。"""
    plan = plan_okx_l2_backfill(
        root,
        conn,
        venue_symbol=venue_symbol,
        from_day=from_day,
        to_day=to_day,
    )
    runnable = [
        task for task in plan.tasks if task.status in {"sealed", "pending"}
    ]
    truncated = max_days is not None and len(runnable) > max_days
    if max_days is not None:
        if max_days <= 0:
            raise ValueError("max_days 必须大于零")
        runnable = runnable[:max_days]
    run_id = f"okx-l2-backfill-{uuid.uuid4().hex}"
    started_at = utc_now()
    config_hash = hashlib.sha256(json.dumps({
        "venue_symbol": venue_symbol,
        "from_day": from_day.isoformat(),
        "to_day": to_day.isoformat(),
        "depth_levels": VERIFIED_DEPTH_LEVELS,
        "reserve_gib": reserve_gib,
        "max_days": max_days,
        "normalization_version": BOOK_L2_NORMALIZATION_VERSION,
    }, sort_keys=True).encode()).hexdigest()
    ok = len(plan.active_tasks)
    rows = sum(task.frame_rows for task in plan.active_tasks)
    completed_now = 0
    reused_now = 0
    failures = [
        f"{task.day}:{task.reason}" for task in plan.blocked_tasks
    ]
    stopped_disk = False
    consecutive_failures = 0
    if progress is not None:
        summary = plan_summary(plan)
        progress(
            "PLAN "
            f"total={summary['total_days']} active={summary['active_days']} "
            f"sealed={summary['sealed_days']} pending={summary['pending_days']} "
            f"blocked={summary['blocked_days']}"
        )
    session = requests.Session()
    session.headers.update({"User-Agent": "guvolu-okx-l2-backfill/1"})
    instrument_body: bytes | None = None
    try:
        for index, task in enumerate(runnable, start=1):
            if progress is not None:
                progress(
                    f"OVERALL [{index}/{len(runnable)}] START "
                    f"{venue_symbol} {task.day} state={task.status}"
                )
            try:
                if task.status == "pending":
                    day_value = date.fromisoformat(task.day)
                    remote_plan = request_download_plan(
                        session,
                        venue_symbol=venue_symbol,
                        day=day_value,
                        depth_levels=VERIFIED_DEPTH_LEVELS,
                    )
                    source_bytes = _advertised_bytes(
                        remote_plan.advertised_size_mb
                    )
                    free = shutil.disk_usage(root).free
                    required = required_free_bytes(source_bytes, reserve_gib)
                    if free < required:
                        stopped_disk = True
                        failures.append(
                            f"{task.day}:磁盘门禁 "
                            f"free={free} required={required}"
                        )
                        if progress is not None:
                            progress(
                                f"OVERALL [{index}/{len(runnable)}] STOP_DISK "
                                f"free={free} required={required}"
                            )
                        break
                    if instrument_body is None:
                        instrument_body = request_instrument(
                            session, venue_symbol=venue_symbol
                        )
                    download = download_archive(
                        session,
                        root,
                        remote_plan,
                        instrument_body=instrument_body,
                    )
                    manifest_path = root / download.manifest_path
                else:
                    if task.manifest_path is None:
                        raise ValueError("已封口任务缺少 manifest")
                    manifest_path = root / task.manifest_path
                item = sealed_input(root, manifest_path)
                if item is None:
                    raise ValueError("下载结果未形成封口原件")
                result = materialize_archive(root, conn, item)
            except (
                duckdb.Error,
                OSError,
                requests.RequestException,
                sqlite3.Error,
                ValueError,
            ) as exc:
                consecutive_failures += 1
                failures.append(f"{task.day}:{exc}")
                if progress is not None:
                    progress(
                        f"OVERALL [{index}/{len(runnable)}] FAILED "
                        f"{task.day} reason={exc}"
                    )
                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                    break
                continue
            consecutive_failures = 0
            ok += 1
            rows += result.frame_rows
            if result.reused:
                reused_now += 1
            else:
                completed_now += 1
            if progress is not None:
                progress(
                    f"OVERALL [{index}/{len(runnable)}] DONE {task.day} "
                    f"frames={result.frame_rows:,} levels={result.level_rows:,} "
                    f"reused={result.reused}"
                )
    finally:
        session.close()
    if not failures and not truncated and ok == len(plan.tasks):
        status = "complete"
    elif stopped_disk or truncated:
        status = "partial"
    else:
        status = "failed"
    _record_run(
        root,
        conn,
        run_id=run_id,
        venue_symbol=venue_symbol,
        from_day=from_day,
        to_day=to_day,
        planned=len(plan.tasks),
        ok=ok,
        rows=rows,
        status=status,
        failures=failures,
        started_at=started_at,
        config_hash=config_hash,
    )
    return {
        "run_id": run_id,
        "status": status,
        "plan": plan_summary(plan),
        "completed_now": completed_now,
        "reused_now": reused_now,
        "ok_days": ok,
        "frame_rows": rows,
        "failed_days": len(failures),
        "stopped_disk": stopped_disk,
        "failures": failures,
    }


def main(argv: Sequence[str] | None = None) -> int:
    """命令行入口。"""
    parser = argparse.ArgumentParser(description="OKX 400 档 L2 回补编排")
    parser.add_argument("--data-root", type=Path, default=None)
    parser.add_argument("command", choices=("plan", "status", "run"))
    parser.add_argument("--symbol", default="BTC-USDT")
    parser.add_argument("--from-day", type=date.fromisoformat, required=True)
    parser.add_argument("--to-day", type=date.fromisoformat, required=True)
    parser.add_argument("--reserve-gib", type=int, default=DEFAULT_RESERVE_GIB)
    parser.add_argument("--max-days", type=int)
    args = parser.parse_args(argv)
    root = (args.data_root or configured_data_root()).resolve()
    conn = (
        store.connect_readonly(root)
        if args.command == "status"
        else store.connect(root)
    )
    if conn is None:
        raise ValueError(f"SQLite control plane does not exist: {root}")
    try:
        if args.command in {"plan", "status"}:
            plan = plan_okx_l2_backfill(
                root,
                conn,
                venue_symbol=args.symbol,
                from_day=args.from_day,
                to_day=args.to_day,
                initialize_dimensions=args.command == "plan",
            )
            result: object = plan_summary(plan)
            code = 0 if not plan.blocked_tasks else 1
        else:
            result = run_okx_l2_backfill(
                root,
                conn,
                venue_symbol=args.symbol,
                from_day=args.from_day,
                to_day=args.to_day,
                reserve_gib=args.reserve_gib,
                max_days=args.max_days,
                progress=lambda message: print(message, flush=True),
            )
            code = 0 if result["status"] == "complete" else 1
    finally:
        conn.close()
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
