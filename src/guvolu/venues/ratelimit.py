"""来源限速与退避（multi-source-data-design 第 8 节）。

本批仅需固定速率一种；权重记账与计数器衰减
随对应来源接入时补齐。模式沿用 api.transport 限速器。
"""
from __future__ import annotations

import threading
import time
from collections.abc import Callable

# 频率超限退避秒
BACKOFF_SECONDS: tuple[float, ...] = (2.0, 4.0, 8.0, 16.0)


class FixedRateLimiter:
    """固定速率限速器，线程安全。"""

    def __init__(
        self,
        rate_per_second: float,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        if rate_per_second <= 0:
            raise ValueError("限速速率必须为正")
        self._interval = 1.0 / rate_per_second
        self._sleeper = sleeper
        self._lock = threading.Lock()
        self._next_at = 0.0

    def acquire(self) -> None:
        """必要时阻塞等待到下一时隙。"""
        with self._lock:
            now = time.monotonic()
            wait = self._next_at - now
            self._next_at = max(now, self._next_at) + self._interval
        if wait > 0:
            self._sleeper(wait)
