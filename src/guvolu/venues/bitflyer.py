"""bitFlyer 公开行情适配器（venue-api-reference 第 3 节）。

仅公开 REST 逐笔与品种端点；盘口流留待后批。
只做地址、游标分页、限速、退避重试；语义决策不入内。
无密钥、无写方法（不变量二）。
"""
from __future__ import annotations

import json
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from urllib.parse import urlencode

import requests

from guvolu.venues.archive import split_json_array_items
from guvolu.venues.base import (
    Channel,
    RawEnvelope,
    RawFrame,
    Window,
)
from guvolu.venues.bitbank import (
    FetchResult,
    HttpSessionLike,
    _fetch_with_backoff,
)
from guvolu.venues.ratelimit import FixedRateLimiter

VENUE_ID = "bitflyer"
PUBLIC_BASE_URL = "https://api.bitflyer.com"
# 公开限速自约束，官方窗口的九成
PUBLIC_RATE_PER_SECOND = 1.5
# 单页上限，实测静默截断值
EXECUTIONS_PAGE_LIMIT = 500
# 31 天边界错误码（实测快照第 2 节）
HISTORY_BOUNDARY_STATUS = -156


@dataclass(frozen=True)
class ExecutionsPage:
    """一页逐笔的原样结果。"""

    result: FetchResult

    def rows_text(self) -> list[str]:
        """按原文切分数组元素，保数字原样（T-08）。"""
        return split_json_array_items(self.result.text())

    def is_boundary(self) -> bool:
        """是否触达 31 天历史边界。"""
        if self.result.http_status != 400:
            return False
        try:
            payload = json.loads(self.result.body)
        except ValueError:
            return False
        return (
            isinstance(payload, Mapping)
            and payload.get("status") == HISTORY_BOUNDARY_STATUS
        )


class BitflyerPublicSource:
    """bitFlyer 公开来源，实现 ReadOnlySource 读取面。"""

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

    def fetch_markets(self) -> FetchResult:
        """取品种一览原文。"""
        return self.fetch(f"{PUBLIC_BASE_URL}/v1/markets")

    def fetch_executions_page(
        self, product: str, before: int | None
    ) -> ExecutionsPage:
        """取一页逐笔原文，id 域游标向旧回扫。"""
        params: dict[str, str] = {
            "product_code": product,
            "count": str(EXECUTIONS_PAGE_LIMIT),
        }
        if before is not None:
            params["before"] = str(before)
        url = f"{PUBLIC_BASE_URL}/v1/executions?{urlencode(params)}"
        return ExecutionsPage(self.fetch(url))

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
        """品种一览包络，单元素序列。"""
        result = self.fetch_markets()
        return [self._envelope(result, "/v1/markets")]

    def klines(
        self, symbol: str, interval: str, window: Window
    ) -> Iterator[RawEnvelope]:
        """无 K 线端点（实测快照第 2 节）。"""
        raise NotImplementedError("bitFlyer 无 K 线端点")

    def trades(self, symbol: str, window: Window) -> Iterator[RawEnvelope]:
        """游标回扫窗内逐笔，至边界或窗前沿。"""
        before: int | None = None
        floor_day = window.start
        while True:
            page = self.fetch_executions_page(symbol, before)
            if page.is_boundary():
                return
            envelope = self._envelope(page.result, "/v1/executions")
            envelope["params"] = {
                "product_code": symbol,
                "before": before,
            }
            yield envelope
            payload = envelope["payload"]
            if not isinstance(payload, list) or not payload:
                return
            oldest = payload[-1]
            if not isinstance(oldest, Mapping):
                return
            before = int(str(oldest.get("id")))
            exec_date = str(oldest.get("exec_date", ""))
            if exec_date[:10].replace("-", "") < floor_day:
                return

    def stream(self, channels: Sequence[Channel]) -> Iterator[RawFrame]:
        """盘口与逐笔流留待后批。"""
        raise NotImplementedError("bitFlyer 实时流为后批范围")
