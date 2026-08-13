"""三所公开 REST 盘口锚点适配器。

适配器只负责限频、超时、GET 退避与原文字节返回。业务解析、持久化和
WS 对照由数据层完成，因此 HTTP 错误与畸形 JSON 也能先作为原件保存。
"""
from __future__ import annotations

import hashlib
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol
from urllib.parse import quote, urlencode

import requests

from guvolu.venues.ratelimit import FixedRateLimiter

TIMEOUT_SECONDS = 10.0
GET_RETRY_MAX = 2
GET_BACKOFF_SECONDS = 0.5


class AnchorHttpResponse(Protocol):
    """测试可替代的最小 HTTP 响应面。"""

    @property
    def status_code(self) -> int: ...

    @property
    def content(self) -> bytes: ...


class AnchorHttpSession(Protocol):
    """测试可注入的最小 GET 会话面。"""

    def get(self, url: str, *, timeout: float) -> AnchorHttpResponse: ...


@dataclass(frozen=True, slots=True)
class AnchorEndpoint:
    """一版独立 REST 盘口端点身份。"""

    venue_id: str
    endpoint_id: str
    endpoint_key: str
    endpoint_revision: int
    base_url: str
    path_template: str
    symbol_parameter: str | None
    rate_per_second: float
    documentation_uri: str


@dataclass(frozen=True, slots=True)
class AnchorFetch:
    """一次公开 GET 的完整结果；失败也返回而非丢弃请求证据。"""

    endpoint: AnchorEndpoint
    venue_symbol: str
    request_url: str
    request_sha256: str
    requested_at: str
    response_received_at: str
    http_status: int | None
    response_body: bytes | None
    error_kind: str | None
    error_detail: str | None

    @property
    def response_sha256(self) -> str | None:
        """返回原始响应字节散列；无响应时明确为空。"""
        if self.response_body is None:
            return None
        return hashlib.sha256(self.response_body).hexdigest()


GMO_ENDPOINT = AnchorEndpoint(
    venue_id="gmo",
    endpoint_id="EP-0006",
    endpoint_key="gmo-public-rest-orderbooks",
    endpoint_revision=0,
    base_url="https://api.coin.z.com/public",
    path_template="/v1/orderbooks",
    symbol_parameter="symbol",
    rate_per_second=2.0,
    documentation_uri="https://api.coin.z.com/docs/en/",
)
BITBANK_ENDPOINT = AnchorEndpoint(
    venue_id="bitbank",
    endpoint_id="EP-0003",
    endpoint_key="bitbank-public-rest-depth",
    endpoint_revision=0,
    base_url="https://public.bitbank.cc",
    path_template="/{symbol}/depth",
    symbol_parameter=None,
    rate_per_second=2.0,
    documentation_uri=(
        "https://github.com/bitbankinc/bitbank-api-docs/"
        "blob/master/public-api.md"
    ),
)
BITFLYER_ENDPOINT = AnchorEndpoint(
    venue_id="bitflyer",
    endpoint_id="EP-0001",
    endpoint_key="bitflyer-public-rest-getboard",
    endpoint_revision=0,
    base_url="https://api.bitflyer.com",
    path_template="/v1/getboard",
    symbol_parameter="product_code",
    rate_per_second=1.0,
    documentation_uri="https://lightning.bitflyer.com/docs?lang=en",
)

ANCHOR_ENDPOINTS: dict[str, AnchorEndpoint] = {
    endpoint.venue_id: endpoint
    for endpoint in (GMO_ENDPOINT, BITBANK_ENDPOINT, BITFLYER_ENDPOINT)
}


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _request_url(endpoint: AnchorEndpoint, venue_symbol: str) -> str:
    if not venue_symbol or any(char.isspace() for char in venue_symbol):
        raise ValueError("venue_symbol 不能为空或含空白")
    path = endpoint.path_template.format(
        symbol=quote(venue_symbol, safe="_-", encoding="utf-8")
    )
    if endpoint.symbol_parameter is None:
        return endpoint.base_url + path
    query = urlencode({endpoint.symbol_parameter: venue_symbol})
    return f"{endpoint.base_url}{path}?{query}"


class PublicRestAnchorAdapter:
    """公开 REST 盘口锚点读取器；不含密钥与写方法。"""

    def __init__(
        self,
        endpoint: AnchorEndpoint,
        *,
        session: AnchorHttpSession | None = None,
        limiter: FixedRateLimiter | None = None,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        self.endpoint = endpoint
        self._session: AnchorHttpSession = (
            session if session is not None else requests.Session()
        )
        self._sleeper = sleeper
        self._limiter = limiter if limiter is not None else FixedRateLimiter(
            endpoint.rate_per_second, sleeper=sleeper
        )

    def fetch(self, venue_symbol: str) -> AnchorFetch:
        """读取一次快照；仅 GET 可有界重试（C-08）。"""
        url = _request_url(self.endpoint, venue_symbol)
        request_wire = f"GET\n{url}\n".encode("utf-8")
        request_sha256 = hashlib.sha256(request_wire).hexdigest()
        requested_at = _now()
        last_kind: str | None = None
        last_detail: str | None = None
        last_status: int | None = None
        last_body: bytes | None = None
        for attempt in range(GET_RETRY_MAX):
            self._limiter.acquire()
            try:
                response = self._session.get(url, timeout=TIMEOUT_SECONDS)
            except requests.Timeout as exc:
                last_kind = "timeout"
                last_detail = f"{type(exc).__name__}: {exc}"
            except requests.RequestException as exc:
                last_kind = "network_error"
                last_detail = f"{type(exc).__name__}: {exc}"
            else:
                last_status = int(response.status_code)
                last_body = bytes(response.content)
                if last_status == 200:
                    return AnchorFetch(
                        self.endpoint, venue_symbol, url, request_sha256,
                        requested_at, _now(), last_status, last_body,
                        None, None,
                    )
                last_kind = "http_error"
                last_detail = f"HTTP {last_status}"
                if last_status != 429 and last_status < 500:
                    break
            if attempt + 1 < GET_RETRY_MAX:
                self._sleeper(GET_BACKOFF_SECONDS * (2**attempt))
        return AnchorFetch(
            self.endpoint, venue_symbol, url, request_sha256,
            requested_at, _now(), last_status, last_body,
            last_kind or "unavailable", last_detail or "无可用响应",
        )


def anchor_adapter(
    venue_id: str,
    *,
    session: AnchorHttpSession | None = None,
    limiter: FixedRateLimiter | None = None,
    sleeper: Callable[[float], None] = time.sleep,
) -> PublicRestAnchorAdapter:
    """构造指定三所的公开 REST 锚点适配器。"""
    try:
        endpoint = ANCHOR_ENDPOINTS[venue_id]
    except KeyError as exc:
        raise ValueError(f"暂不支持 REST L2 锚点: {venue_id}") from exc
    return PublicRestAnchorAdapter(
        endpoint, session=session, limiter=limiter, sleeper=sleeper
    )
