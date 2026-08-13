"""bitbank 公开行情适配器（venue-api-reference 第 4 节）。

仅公开 REST；实时流为 socket.io 依赖，留待后批。
只做地址、分页、限速、退避重试；语义决策不入内。
无密钥、无写方法（不变量二）。
"""
from __future__ import annotations

import json
import time
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from typing import Protocol

import requests

from guvolu.venues.base import (
    Channel,
    RawEnvelope,
    RawFrame,
    VenueRequestError,
    Window,
    window_days,
)
from guvolu.venues.ratelimit import BACKOFF_SECONDS, FixedRateLimiter

VENUE_ID = "bitbank"
PUBLIC_BASE_URL = "https://public.bitbank.cc"
REST_BASE_URL = "https://api.bitbank.cc/v1"
# 公开限速自约束，文档上限五次的六成
PUBLIC_RATE_PER_SECOND = 3.0
_TIMEOUT_SECONDS = 60.0


class HttpResponseLike(Protocol):
    """响应依赖面，测试可仿制。"""

    @property
    def status_code(self) -> int: ...

    @property
    def content(self) -> bytes: ...


class HttpSessionLike(Protocol):
    """会话依赖面，测试可注入。"""

    def get(self, url: str, *, timeout: float) -> HttpResponseLike: ...


@dataclass(frozen=True)
class FetchResult:
    """一次拉取的原样结果，body 为响应原文字节。"""

    url: str
    http_status: int
    body: bytes
    latency_ms: float

    def text(self) -> str:
        """按 UTF-8 解码原文。"""
        return self.body.decode("utf-8")


def _fetch_with_backoff(
    venue_id: str,
    session: HttpSessionLike,
    limiter: FixedRateLimiter,
    url: str,
    sleeper: Callable[[float], None],
) -> FetchResult:
    """GET 一次；429 与 5xx 与网络错按表退避（C-08）。"""
    last_detail = ""
    for attempt in range(len(BACKOFF_SECONDS) + 1):
        limiter.acquire()
        started = time.monotonic()
        try:
            response = session.get(url, timeout=_TIMEOUT_SECONDS)
        except requests.RequestException as exc:
            last_detail = f"{type(exc).__name__}: {exc}"
        else:
            latency = (time.monotonic() - started) * 1000
            status = response.status_code
            if status != 429 and status < 500:
                return FetchResult(url, status, response.content, latency)
            last_detail = f"HTTP {status}"
        if attempt < len(BACKOFF_SECONDS):
            sleeper(BACKOFF_SECONDS[attempt])
    raise VenueRequestError(venue_id, url, last_detail)


class BitbankPublicSource:
    """bitbank 公开来源，实现 ReadOnlySource 读取面。"""

    venue_id = VENUE_ID

    def __init__(
        self,
        limiter: FixedRateLimiter | None = None,
        session: HttpSessionLike | None = None,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        self._limiter = (
            limiter
            if limiter is not None
            else FixedRateLimiter(PUBLIC_RATE_PER_SECOND)
        )
        self._session: HttpSessionLike = (
            session if session is not None else requests.Session()
        )
        self._sleeper = sleeper

    def fetch(self, url: str) -> FetchResult:
        """限速拉取任意公开地址。"""
        return _fetch_with_backoff(
            self.venue_id, self._session, self._limiter, url, self._sleeper
        )

    def fetch_pairs(self) -> FetchResult:
        """取全部品种规则原文。"""
        return self.fetch(f"{REST_BASE_URL}/spot/pairs")

    def fetch_day(self, pair: str, day: str) -> FetchResult:
        """取单日全量逐笔原文；404 表示该日无文件。"""
        return self.fetch(f"{PUBLIC_BASE_URL}/{pair}/transactions/{day}")

    def _envelope(self, result: FetchResult, path: str) -> RawEnvelope:
        payload: object | None
        try:
            payload = json.loads(result.body)
        except ValueError:
            payload = None
        return {
            "source": "rest_public",
            "method": "GET",
            "path": path,
            "params": None,
            "http_status": result.http_status,
            "latency_ms": round(result.latency_ms, 1),
            "payload": payload,
            "network_error": None,
        }

    def instruments(self) -> Sequence[RawEnvelope]:
        """品种规则包络，单元素序列。"""
        result = self.fetch_pairs()
        return [self._envelope(result, "/spot/pairs")]

    def klines(
        self, symbol: str, interval: str, window: Window
    ) -> Iterator[RawEnvelope]:
        """K 线采集留待后批。"""
        raise NotImplementedError("bitbank K 线为后批范围")

    def trades(self, symbol: str, window: Window) -> Iterator[RawEnvelope]:
        """窗内逐日全量逐笔包络。"""
        for day in window_days(window):
            result = self.fetch_day(symbol, day)
            envelope = self._envelope(
                result, f"/{symbol}/transactions/{day}"
            )
            envelope["params"] = {"pair": symbol, "day": day}
            yield envelope

    def stream(self, channels: Sequence[Channel]) -> Iterator[RawFrame]:
        """实时流为 socket.io 依赖，留待后批。"""
        raise NotImplementedError("bitbank 实时流为后批范围")
