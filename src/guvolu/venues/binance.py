"""Binance 公开逐笔归档下载与 SHA-256 校验。"""
from __future__ import annotations

import hashlib
import re
import time
from collections.abc import Callable
from dataclasses import dataclass

from guvolu.venues.bitbank import (
    FetchResult,
    HttpSessionLike,
    _fetch_with_backoff,
)
from guvolu.venues.ratelimit import FixedRateLimiter

VENUE_ID = "binance"
ARCHIVE_BASE_URL = "https://data.binance.vision/data/spot/daily/aggTrades"
ARCHIVE_RATE_PER_SECOND = 1.0
_SHA256 = re.compile(r"^[0-9a-fA-F]{64}$")


@dataclass(frozen=True, slots=True)
class ArchiveDay:
    """归档 ZIP 与校验文件的原始响应。"""

    symbol: str
    day: str
    archive: FetchResult
    checksum: FetchResult

    def expected_sha256(self) -> str:
        """解析官方 CHECKSUM 的首个 SHA-256 字段。"""
        fields = self.checksum.text().strip().split()
        if not fields or not _SHA256.fullmatch(fields[0]):
            raise ValueError("Binance CHECKSUM 格式非法")
        return fields[0].lower()

    def verify(self) -> bool:
        """校验 ZIP 字节散列，绝不解压后替代原件校验。"""
        return hashlib.sha256(self.archive.body).hexdigest() == self.expected_sha256()


class BinanceArchiveSource:
    """只读 Binance 日度聚合成交归档来源。"""

    venue_id = VENUE_ID

    def __init__(
        self,
        limiter: FixedRateLimiter | None = None,
        session: HttpSessionLike | None = None,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        self._limiter = (
            limiter if limiter is not None
            else FixedRateLimiter(ARCHIVE_RATE_PER_SECOND)
        )
        if session is None:
            import requests

            self._session: HttpSessionLike = requests.Session()
        else:
            self._session = session
        self._sleeper = sleeper

    @staticmethod
    def archive_url(symbol: str, day: str) -> str:
        """生成官方日归档地址。"""
        return (
            f"{ARCHIVE_BASE_URL}/{symbol}/"
            f"{symbol}-aggTrades-{day[:4]}-{day[4:6]}-{day[6:8]}.zip"
        )

    def fetch_day(self, symbol: str, day: str) -> ArchiveDay:
        """读取同目录 ZIP 与 CHECKSUM，调用方决定缺失处理。"""
        url = self.archive_url(symbol, day)
        archive = _fetch_with_backoff(
            self.venue_id, self._session, self._limiter, url, self._sleeper
        )
        checksum = _fetch_with_backoff(
            self.venue_id, self._session, self._limiter, f"{url}.CHECKSUM",
            self._sleeper,
        )
        return ArchiveDay(symbol, day, archive, checksum)
