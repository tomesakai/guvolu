"""独立 L2 质量刷新循环：锁外计算、短暂取锁 upsert。

从 L2 物化热循环分离质量遥测，避免长质量计算撑长物化 cycle
并与冻结刷新争用写锁（runtime-ops 第 8.1 节）。质量为旁路
遥测，不是冻结链输入，独立低频进程刷新即可。
"""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Sequence

from guvolu.data import store
from guvolu.data.l2_materialize import _refresh_quality_nonblocking
from guvolu.data.paths import data_root as configured_data_root
from guvolu.data.watch_connection import connect_with_retry


def _watch(root: Path, interval: float) -> int:
    """周期刷新最近质量窗口；连接失败按间隔重试。"""
    def report_connect_error(exc: Exception, elapsed: float) -> None:
        print(json.dumps({
            "event": "quality_watcher_startup_error",
            "error": f"{type(exc).__name__}: {exc}",
            "retry_seconds": interval,
        }, ensure_ascii=False), flush=True)

    conn = None
    try:
        conn = connect_with_retry(
            root, retry_seconds=interval,
            connector=store.connect, report_error=report_connect_error,
        )
        while True:
            started = time.monotonic()
            summary, error = _refresh_quality_nonblocking(root, conn)
            if error is not None:
                print(json.dumps({
                    "event": "quality_watcher_error",
                    "error": f"{type(error).__name__}: {error}",
                }, ensure_ascii=False), flush=True)
            else:
                print(json.dumps({
                    "event": "quality_watcher_cycle",
                    "elapsed_seconds": round(time.monotonic() - started, 3),
                    "quality": summary,
                }, ensure_ascii=False), flush=True)
            time.sleep(interval)
    except KeyboardInterrupt:
        print("质量刷新已停止", flush=True)
        return 0
    finally:
        if conn is not None:
            conn.close()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="独立 L2 质量刷新循环")
    parser.add_argument("--data-root", type=Path, default=None)
    sub = parser.add_subparsers(dest="command", required=True)
    watch = sub.add_parser("watch", help="周期刷新最近质量窗口")
    watch.add_argument("--interval-seconds", type=float, default=300.0)
    args = parser.parse_args(argv)
    root = (args.data_root or configured_data_root()).resolve()
    interval = float(args.interval_seconds)
    if interval < 10:
        raise ValueError("interval-seconds 不得小于 10")
    return _watch(root, interval)


if __name__ == "__main__":
    raise SystemExit(main())
