"""为决策级研究构建静态数据快照（runtime-ops 第 8.2 节）。

从生产数据根单步备份控制库到目标目录，复制研究市场的成交活动输出、
其余小体积域的全部活动输出，以及影子市场最近若干段 L2 输出；随后在
快照库里撤销未复制的 book_l2 头与其它市场的成交头，使快照自洽。快照
只读、一次性，研究登记与制品仍写项目目录。生产数据根只被读取。
"""
from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import sys
import time
from collections.abc import Sequence
from pathlib import Path

from guvolu.data.materialize import _resolve_recorded_path

TRADE_DOMAINS = ("trade", "trade_realtime")
SMALL_DOMAINS_EXCLUDED = ("trade", "trade_realtime", "book_l2")


def _backup(source: Path, destination: Path) -> None:
    """单步无日志在线备份。"""
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.unlink(missing_ok=True)
    for suffix in ("-shm", "-wal", "-journal"):
        destination.with_name(destination.name + suffix).unlink(missing_ok=True)
    source_conn = sqlite3.connect(f"file:{source}?mode=ro", uri=True)
    destination_conn = sqlite3.connect(destination)
    try:
        destination_conn.execute("PRAGMA journal_mode=OFF")
        destination_conn.execute("PRAGMA synchronous=OFF")
        source_conn.backup(destination_conn, pages=-1)
    finally:
        destination_conn.close()
        source_conn.close()


def _copy(source_root: Path, snapshot_root: Path, storage_path: str) -> bool:
    destination = snapshot_root / storage_path
    if destination.exists():
        return False
    source = _resolve_recorded_path(source_root, storage_path)
    if not source.is_file():
        raise FileNotFoundError(f"活动输出缺失: {storage_path}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, destination)
    return True


def build_snapshot(
    source_root: Path,
    snapshot_root: Path,
    market_ids: Sequence[str],
    shadow_market_ids: Sequence[str],
    *,
    l2_segments: int = 3,
) -> dict[str, object]:
    """构建快照并返回复制统计。"""
    started = time.perf_counter()
    source_root = source_root.resolve()
    snapshot_root = snapshot_root.resolve()
    if snapshot_root == source_root or source_root in snapshot_root.parents:
        raise ValueError("快照目录不得位于生产数据根内")
    database = snapshot_root / "guvolu.sqlite3"
    _backup(source_root / "guvolu.sqlite3", database)
    conn = sqlite3.connect(database)
    copied = 0
    bytes_copied = 0
    try:
        market_marks = ",".join("?" * len(market_ids))
        rows = conn.execute(
            "SELECT h.market_id,h.domain,h.partition_key,a.storage_path,"
            "a.byte_count FROM materialization_partition_head h "
            "JOIN materialization_output o ON o.attempt_id=h.attempt_id "
            "JOIN artifact a ON a.artifact_id=o.artifact_id "
            f"WHERE h.domain IN ('trade','trade_realtime') "
            f"AND h.market_id IN ({market_marks})",
            tuple(market_ids),
        ).fetchall()
        for _market, _domain, _key, storage_path, byte_count in rows:
            if _copy(source_root, snapshot_root, str(storage_path)):
                copied += 1
                bytes_copied += int(byte_count or 0)
        # 其它市场的成交头撤销，快照只服务研究市场
        conn.execute(
            "DELETE FROM materialization_partition_head WHERE domain IN "
            f"('trade','trade_realtime') AND market_id NOT IN ({market_marks})",
            tuple(market_ids),
        )
        rows = conn.execute(
            "SELECT a.storage_path,a.byte_count FROM materialization_partition_head h "
            "JOIN materialization_output o ON o.attempt_id=h.attempt_id "
            "JOIN artifact a ON a.artifact_id=o.artifact_id "
            "WHERE h.domain NOT IN ('trade','trade_realtime','book_l2')"
        ).fetchall()
        for storage_path, byte_count in rows:
            if _copy(source_root, snapshot_root, str(storage_path)):
                copied += 1
                bytes_copied += int(byte_count or 0)
        kept: list[tuple[str, str]] = []
        for market_id in shadow_market_ids:
            latest = conn.execute(
                "SELECT h.partition_key,h.attempt_id FROM materialization_partition_head h "
                "JOIN materialization_output o ON o.attempt_id=h.attempt_id "
                "WHERE h.market_id=? AND h.domain='book_l2' "
                "AND o.dataset='book_l2_frame' AND o.max_event_time IS NOT NULL "
                "ORDER BY o.max_event_time DESC LIMIT ?",
                (market_id, l2_segments),
            ).fetchall()
            for partition_key, attempt_id in latest:
                kept.append((market_id, str(partition_key)))
                for (storage_path, byte_count) in conn.execute(
                    "SELECT a.storage_path,a.byte_count FROM materialization_output o "
                    "JOIN artifact a ON a.artifact_id=o.artifact_id "
                    "WHERE o.attempt_id=?",
                    (attempt_id,),
                ).fetchall():
                    if _copy(source_root, snapshot_root, str(storage_path)):
                        copied += 1
                        bytes_copied += int(byte_count or 0)
        conn.execute("DELETE FROM materialization_partition_head WHERE domain='book_l2'")
        conn.commit()
    finally:
        conn.close()
    # 重新写回保留的 L2 头（备份库为来源）
    source_conn = sqlite3.connect(f"file:{source_root / 'guvolu.sqlite3'}?mode=ro", uri=True)
    conn = sqlite3.connect(database)
    try:
        for market_id, partition_key in kept:
            row = source_conn.execute(
                "SELECT market_id,domain,partition_key,normalization_version,"
                "attempt_id,activated_at FROM materialization_partition_head "
                "WHERE market_id=? AND domain='book_l2' AND partition_key=?",
                (market_id, partition_key),
            ).fetchone()
            if row is not None:
                conn.execute(
                    "INSERT OR REPLACE INTO materialization_partition_head "
                    "VALUES (?,?,?,?,?,?)",
                    row,
                )
        conn.commit()
        heads = conn.execute(
            "SELECT domain,COUNT(*) FROM materialization_partition_head GROUP BY 1"
        ).fetchall()
    finally:
        conn.close()
        source_conn.close()
    return {
        "snapshot_root": str(snapshot_root),
        "market_ids": list(market_ids),
        "shadow_market_ids": list(shadow_market_ids),
        "files_copied": copied,
        "bytes_copied": bytes_copied,
        "heads": {str(domain): int(count) for domain, count in heads},
        "elapsed_seconds": round(time.perf_counter() - started, 1),
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-data-root", type=Path, required=True)
    parser.add_argument("--snapshot-root", type=Path, required=True)
    parser.add_argument("--market-id", action="append", dest="market_ids", required=True)
    parser.add_argument(
        "--shadow-market-id", action="append", dest="shadow_market_ids",
        default=None, help="跨所影子所需 L2 市场，缺省三所 BTC",
    )
    parser.add_argument("--l2-segments", type=int, default=3)
    arguments = parser.parse_args(argv)
    shadow = arguments.shadow_market_ids or [
        "mkt__gmo__btc__r0", "mkt__bitbank__btc_jpy__r0", "mkt__bitflyer__btc_jpy__r0",
    ]
    summary = build_snapshot(
        arguments.source_data_root, arguments.snapshot_root,
        arguments.market_ids, shadow, l2_segments=int(arguments.l2_segments),
    )
    sys.stdout.write(json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
