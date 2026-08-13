"""公开客户端单测。全程使用传输替身，禁止访问网络（C-13）。"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from guvolu.api.public_client import PublicClient
from guvolu.api.transport import Params, PublicTransport, RateLimiter
from guvolu.domain.enums import KlineInterval, ServiceStatus
from guvolu.domain.errors import ClockDriftError

TICKER_ROW: dict[str, object] = {
    "ask": "10140050",
    "bid": "10138320",
    "high": "10174419",
    "last": "10141182",
    "low": "10068720",
    "symbol": "BTC",
    "timestamp": "2026-08-05T12:24:46.560Z",
    "volume": "352.72",
}
ORDERBOOKS_DATA: dict[str, object] = {
    "symbol": "BTC",
    "asks": [{"price": "10140050", "size": "0.03"}],
    "bids": [{"price": "10138320", "size": "0.019"}],
}
TRADES_DATA: dict[str, object] = {
    "list": [
        {
            "price": "10141182",
            "side": "SELL",
            "size": "0.01",
            "timestamp": "2026-08-05T12:24:46.560Z",
        }
    ],
    "pagination": {"currentPage": 1, "count": 100},
}
KLINE_ROW: dict[str, object] = {
    "openTime": "1785790800000",
    "open": "10021773",
    "high": "10023519",
    "low": "9982862",
    "close": "9990000",
    "volume": "12.3",
}
SYMBOL_ROW: dict[str, object] = {
    "symbol": "BTC",
    "minOrderSize": "0.00001",
    "maxOrderSize": "5",
    "sizeStep": "0.00001",
    "tickSize": "1",
    "takerFee": "0.0005",
    "makerFee": "-0.0001",
}


def iso_z(moment: datetime) -> str:
    """转 GMO 载荷的时间写法。"""
    return moment.astimezone(UTC).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def payload(data: object, responsetime: str | None = None) -> dict[str, object]:
    """构造成功载荷。status 判定已在传输层完成（T-10）。"""
    return {
        "status": 0,
        "data": data,
        "responsetime": responsetime or iso_z(datetime.now(UTC)),
    }


class FakePublicTransport(PublicTransport):
    """公开传输替身，按队列返回预置载荷，绝不发出真实请求。"""

    def __init__(self, payloads: Sequence[Mapping[str, object]]) -> None:
        super().__init__(RateLimiter(1000.0))
        self.calls: list[tuple[str, dict[str, str | int]]] = []
        self._payloads: list[Mapping[str, object]] = list(payloads)

    def get_payload(
        self, path: str, params: Params | None = None
    ) -> Mapping[str, object]:
        self.calls.append((path, dict(params) if params is not None else {}))
        return self._payloads.pop(0)

    def get(self, path: str, params: Params | None = None) -> object:
        return self.get_payload(path, params).get("data")


def build(*payloads: Mapping[str, object]) -> tuple[PublicClient, FakePublicTransport]:
    """构造客户端与其传输替身。"""
    transport = FakePublicTransport(payloads)
    return PublicClient(transport), transport


def test_status_open() -> None:
    """服务状态解析（R-03）。"""
    client, transport = build(payload({"status": "OPEN"}))
    assert client.status() is ServiceStatus.OPEN
    assert transport.calls[0][0] == "/v1/status"


def test_ticker_parses_decimal_and_symbol_param() -> None:
    """最新レート解析为 Decimal（T-08），并传品种参数。"""
    client, transport = build(payload([TICKER_ROW]))
    tickers = client.ticker("BTC")
    assert len(tickers) == 1
    assert tickers[0].ask == Decimal("10140050")
    assert isinstance(tickers[0].volume, Decimal)
    assert tickers[0].timestamp.tzinfo is not None
    assert transport.calls[0] == ("/v1/ticker", {"symbol": "BTC"})


def test_ticker_without_symbol_sends_no_params() -> None:
    """省略品种时不传参数。"""
    client, transport = build(payload([TICKER_ROW]))
    assert len(client.ticker()) == 1
    assert transport.calls[0] == ("/v1/ticker", {})


def test_orderbooks_parse() -> None:
    """盘口解析。"""
    client, transport = build(payload(ORDERBOOKS_DATA))
    book = client.orderbooks("BTC")
    assert book.asks[0].price == Decimal("10140050")
    assert book.bids[0].size == Decimal("0.019")
    assert transport.calls[0] == ("/v1/orderbooks", {"symbol": "BTC"})


def test_trades_list_envelope_and_paging() -> None:
    """逐笔成交取 list 包络，分页信息忽略。"""
    client, transport = build(payload(TRADES_DATA))
    trades = client.trades("BTC", page=1, count=100)
    assert trades[0].price == Decimal("10141182")
    assert transport.calls[0] == (
        "/v1/trades",
        {"symbol": "BTC", "page": 1, "count": 100},
    )


def test_klines_parse() -> None:
    """KLine 解析与参数。"""
    client, transport = build(payload([KLINE_ROW]))
    klines = client.klines("BTC", KlineInterval.MIN_1, "20260805")
    assert klines[0].close == Decimal("9990000")
    assert klines[0].open_time.tzinfo is not None
    assert transport.calls[0] == (
        "/v1/klines",
        {"symbol": "BTC", "interval": "1min", "date": "20260805"},
    )


def test_symbols_parse() -> None:
    """取引ルール解析。"""
    client, _ = build(payload([SYMBOL_ROW]))
    rules = client.symbols()
    assert rules[0].min_order_size == Decimal("0.00001")
    assert rules[0].maker_fee == Decimal("-0.0001")


def test_empty_data_returns_empty_tuple() -> None:
    """data 为空对象或缺省时返回空元组。"""
    client, _ = build(payload({}), payload(None))
    assert client.trades("BTC") == ()
    assert client.ticker() == ()


def test_check_clock_returns_drift() -> None:
    """时钟偏移在限内时返回浮点秒数（R-05）。"""
    client, transport = build(payload({"status": "OPEN"}))
    drift = client.check_clock()
    assert isinstance(drift, float)
    assert abs(drift) < 5.0
    assert transport.calls[0][0] == "/v1/status"


def test_check_clock_drift_exceeded_raises() -> None:
    """时钟偏移超限拒绝启动（R-05）。"""
    stale = iso_z(datetime.now(UTC) - timedelta(seconds=60))
    client, _ = build(payload({"status": "OPEN"}, responsetime=stale))
    with pytest.raises(ClockDriftError):
        client.check_clock()


def test_check_clock_accepts_wider_limit() -> None:
    """放宽上限后同一偏移可通过。"""
    stale = iso_z(datetime.now(UTC) - timedelta(seconds=30))
    client, _ = build(payload({"status": "OPEN"}, responsetime=stale))
    assert client.check_clock(max_drift_seconds=120.0) > 0.0
