"""公开 API 客户端：行情与服务状态，无需密钥。

端点路径与字段以 2026-08-05 官方文档核实为准（A-04）。
HTTP 200 与 status 的判定已在传输层完成（T-10），本层只做模型解析。
"""
from __future__ import annotations

from datetime import UTC, datetime

from guvolu.api.envelope import one, rows
from guvolu.api.transport import PublicTransport, RateLimiter
from guvolu.domain.config import Config
from guvolu.domain.enums import KlineInterval, ServiceStatus
from guvolu.domain.errors import ApiSchemaError, ClockDriftError
from guvolu.domain.models import (
    Kline,
    Orderbook,
    PublicTrade,
    SymbolRule,
    Ticker,
    parse_service_status,
)

# 时钟偏移容许上限（R-05）
DEFAULT_MAX_DRIFT_SECONDS = 5.0

Query = dict[str, str | int]


class PublicClient:
    """公开端点客户端。不持任何密钥，永不进入写路径（T-02）。"""

    def __init__(self, transport: PublicTransport) -> None:
        self._transport = transport

    @classmethod
    def from_config(
        cls, config: Config, limiter: RateLimiter | None = None
    ) -> "PublicClient":
        """按配置构造，限速取公开档；可注入共享限速器（R-04）。"""
        shared = limiter if limiter is not None else RateLimiter(config.public_rps)
        return cls(PublicTransport(shared))

    def status(self) -> ServiceStatus:
        """取服务稼働状态，写请求前的门禁依据（R-03）。"""
        data = self._transport.get("/v1/status")
        return parse_service_status(one(data, "/v1/status"))

    def ticker(self, symbol: str | None = None) -> tuple[Ticker, ...]:
        """取最新レート。省略品种时返回全部品种。"""
        params: Query = {}
        if symbol is not None:
            params["symbol"] = symbol
        data = self._transport.get("/v1/ticker", params or None)
        return tuple(Ticker.from_api(row) for row in rows(data))

    def orderbooks(self, symbol: str) -> Orderbook:
        """取板情報快照。"""
        data = self._transport.get("/v1/orderbooks", {"symbol": symbol})
        return Orderbook.from_api(one(data, "/v1/orderbooks"))

    def trades(
        self, symbol: str, page: int | None = None, count: int | None = None
    ) -> tuple[PublicTrade, ...]:
        """取逐笔成交。载荷中的分页信息不使用。"""
        params: Query = {"symbol": symbol}
        if page is not None:
            params["page"] = page
        if count is not None:
            params["count"] = count
        data = self._transport.get("/v1/trades", params)
        return tuple(PublicTrade.from_api(row) for row in rows(data))

    def klines(
        self, symbol: str, interval: KlineInterval, date: str
    ) -> tuple[Kline, ...]:
        """取 KLine。date 为 YYYYMMDD 或 YYYY，按 JST 交易日（C-11）。"""
        params: Query = {
            "symbol": symbol,
            "interval": interval.value,
            "date": date,
        }
        data = self._transport.get("/v1/klines", params)
        return tuple(Kline.from_api(row) for row in rows(data))

    def symbols(self) -> tuple[SymbolRule, ...]:
        """取全部品种的取引ルール。"""
        data = self._transport.get("/v1/symbols")
        return tuple(SymbolRule.from_api(row) for row in rows(data))

    def check_clock(
        self, max_drift_seconds: float = DEFAULT_MAX_DRIFT_SECONDS
    ) -> float:
        """校验本机时钟偏移，超限拒绝启动（R-05）。

        以 status 端点载荷的 responsetime 为服务器时刻，
        返回本机时刻减服务器时刻的秒数，正值表示本机偏快。
        """
        payload = self._transport.get_payload("/v1/status")
        raw = payload.get("responsetime")
        if raw is None:
            raise ApiSchemaError("响应缺少 responsetime")
        server_time = datetime.fromisoformat(str(raw))
        if server_time.tzinfo is None:
            # 未标注时区时按 UTC 处理
            server_time = server_time.replace(tzinfo=UTC)
        drift = (datetime.now(UTC) - server_time).total_seconds()
        if abs(drift) > max_drift_seconds:
            raise ClockDriftError(
                f"时钟偏移 {drift:.3f} 秒，超过上限 {max_drift_seconds} 秒"
            )
        return drift
