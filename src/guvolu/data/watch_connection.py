"""常驻物化器的 SQLite 启动连接重试。"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from pathlib import Path
from time import monotonic, sleep

ConnectionFactory = Callable[[Path], sqlite3.Connection]
ErrorReporter = Callable[[Exception, float], None]


def connect_with_retry(
    root: Path,
    *,
    retry_seconds: float,
    connector: ConnectionFactory,
    report_error: ErrorReporter,
) -> sqlite3.Connection:
    """写锁超时后报告并等待；其他错误和中断原样上抛。"""
    if retry_seconds <= 0:
        raise ValueError("retry_seconds 必须大于 0")
    while True:
        started = monotonic()
        try:
            return connector(root)
        except TimeoutError as exc:
            report_error(exc, monotonic() - started)
            sleep(retry_seconds)
