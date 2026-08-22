"""研究治理使用的进程壁钟。"""
from __future__ import annotations

from datetime import UTC, datetime


def utc_now() -> datetime:
    """返回不可由治理 API 调用方覆盖的当前 UTC 时间。"""
    return datetime.now(UTC)
