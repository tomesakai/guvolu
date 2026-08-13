"""只读客户端单测。全程使用传输替身，禁止访问网络（C-13）。"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from guvolu.api.read_client import ReadClient, format_history_timestamp
from guvolu.api.transport import HttpMethod, Params, PrivateTransport, RateLimiter
from guvolu.domain.config import load_config
from guvolu.domain.enums import OrderStatus, Side
from guvolu.domain.errors import ConfigError
from guvolu.domain.symbols import LeverageSymbol

ASSETS_DATA: list[object] = [
    {"amount": "3009", "available": "3009", "conversionRate": "1", "symbol": "JPY"},
    {
        "amount": "0",
        "available": "0",
        "conversionRate": "10141182",
        "symbol": "BTC",
    },
]
MARGIN_DATA: dict[str, object] = {
    "actualProfitLoss": "3009",
    "availableAmount": "3009",
    "availableAmountForSpot": "3009",
    "margin": "0",
    "profitLoss": "0",
    "transferableAmount": "3009",
}
TRADING_VOLUME_DATA: dict[str, object] = {
    "jpyVolume": "0",
    "tierLevel": 1,
    "limit": [
        {
            "symbol": "BTC",
            "todayLimitBuySize": "2000000",
            "todayLimitSellSize": "2000000",
            "takerFee": "0.0005",
            "makerFee": "-0.0001",
        }
    ],
}
ORDER_ROW: dict[str, object] = {
    "rootOrderId": 123456789,
    "orderId": 123456789,
    "symbol": "BTC",
    "side": "BUY",
    "orderType": "NORMAL",
    "executionType": "LIMIT",
    "settleType": "OPEN",
    "size": "0.02",
    "executedSize": "0",
    "price": "9000000",
    "losscutPrice": "0",
    "status": "ORDERED",
    "timeInForce": "FAS",
    "timestamp": "2019-03-19T01:07:24.217Z",
}
EXECUTION_ROW: dict[str, object] = {
    "executionId": 72123911,
    "orderId": 123456789,
    "positionId": 1234567,
    "symbol": "BTC",
    "side": "BUY",
    "settleType": "OPEN",
    "size": "0.7361",
    "price": "877404",
    "lossGain": "0",
    "fee": "323",
    "timestamp": "2019-03-19T02:15:06.081Z",
}
POSITION_ROW: dict[str, object] = {
    "positionId": 1234567,
    "symbol": "BTC_JPY",
    "side": "BUY",
    "size": "0.22",
    "orderdSize": "0",
    "price": "876045",
    "lossGain": "-6084",
    "leverage": "4",
    "losscutPrice": "766540",
    "timestamp": "2019-03-19T02:15:06.094Z",
}
POSITION_SUMMARY_DATA: dict[str, object] = {
    "list": [
        {
            "averagePositionRate": "715656",
            "positionLossGain": "250675",
            "side": "BUY",
            "sumOrderQuantity": "2",
            "sumPositionQuantity": "11.6",
            "symbol": "BTC_JPY",
        }
    ]
}
FIAT_ROW: dict[str, object] = {
    "amount": "5000",
    "status": "DONE",
    "symbol": "JPY",
    "timestamp": "2019-03-19T02:15:06.081Z",
}
CRYPTO_ROW: dict[str, object] = {
    "address": "3LP4C4pYYBrCTLZ9nzWKPfHDCwYVFvXo3Y",
    "amount": "0.1",
    "fee": "0",
    "status": "DONE",
    "symbol": "BTC",
    "timestamp": "2019-03-19T02:15:06.081Z",
    "txHash": "d330ff1f3b8b6da1e1eff8fc84b1c8b8a4e3b3d0",
}
FROM_TS = datetime(2026, 8, 5, 12, 0, 0, tzinfo=UTC)


@dataclass(frozen=True, slots=True)
class Call:
    """一次请求的记录。"""

    method: str
    path: str
    params: dict[str, str | int] = field(default_factory=dict)
    body: dict[str, object] = field(default_factory=dict)


class FakePrivateTransport(PrivateTransport):
    """私有传输替身，按队列返回预置 data，绝不发出真实请求（C-14）。"""

    def __init__(self, tmp_path: Path, responses: Sequence[object] = ()) -> None:
        super().__init__("k", "s", RateLimiter(1000.0), tmp_path)
        self.calls: list[Call] = []
        self._responses: list[object] = list(responses)

    def request(
        self,
        method: HttpMethod,
        path: str,
        *,
        params: Params | None = None,
        body: Mapping[str, object] | None = None,
    ) -> object:
        self.calls.append(
            Call(
                method=method,
                path=path,
                params=dict(params) if params is not None else {},
                body=dict(body) if body is not None else {},
            )
        )
        return self._responses.pop(0) if self._responses else None


def build(
    tmp_path: Path, *responses: object
) -> tuple[ReadClient, FakePrivateTransport]:
    """构造只读客户端与其传输替身。"""
    transport = FakePrivateTransport(tmp_path, responses)
    return ReadClient(transport), transport


def test_assets_parse(tmp_path: Path) -> None:
    """資産残高解析，区分总额与可用（U-03）。"""
    client, transport = build(tmp_path, ASSETS_DATA)
    assets = client.assets()
    assert len(assets) == 2
    assert assets[0].amount == Decimal("3009")
    assert isinstance(assets[1].conversion_rate, Decimal)
    assert transport.calls[0].method == "GET"
    assert transport.calls[0].path == "/v1/account/assets"


def test_margin_parse(tmp_path: Path) -> None:
    """余力情報解析。"""
    client, transport = build(tmp_path, MARGIN_DATA)
    margin = client.margin()
    assert margin.available_amount_for_spot == Decimal("3009")
    assert margin.margin_ratio is None
    assert transport.calls[0].path == "/v1/account/margin"


def test_trading_volume_parse(tmp_path: Path) -> None:
    """取引高情報解析。"""
    client, transport = build(tmp_path, TRADING_VOLUME_DATA)
    volume = client.trading_volume()
    assert volume.tier_level == 1
    assert volume.limits[0].today_limit_buy_size == Decimal("2000000")
    assert transport.calls[0].path == "/v1/account/tradingVolume"


def test_history_timestamp_format() -> None:
    """履历时间为毫秒三位并以 Z 结尾（C-12）。"""
    moment = datetime(2026, 8, 5, 12, 34, 56, 789012, tzinfo=UTC)
    assert format_history_timestamp(moment) == "2026-08-05T12:34:56.789Z"


def test_history_timestamp_converts_to_utc() -> None:
    """非 UTC 时区先转 UTC（C-11）。"""
    jst = timezone(timedelta(hours=9))
    moment = datetime(2026, 8, 5, 21, 0, 0, tzinfo=jst)
    assert format_history_timestamp(moment) == "2026-08-05T12:00:00.000Z"


def test_history_timestamp_rejects_naive() -> None:
    """无时区时间立即报错。"""
    with pytest.raises(ValueError):
        format_history_timestamp(datetime(2026, 8, 5, 12, 0, 0))


def test_fiat_deposit_history_params(tmp_path: Path) -> None:
    """日本円入金履历必传 fromTimestamp（C-12）。"""
    client, transport = build(tmp_path, [FIAT_ROW])
    items = client.fiat_deposit_history(FROM_TS)
    assert items[0].amount == Decimal("5000")
    assert transport.calls[0].path == "/v1/account/fiatDeposit/history"
    assert transport.calls[0].params == {"fromTimestamp": "2026-08-05T12:00:00.000Z"}


def test_fiat_withdrawal_history_with_to_timestamp(tmp_path: Path) -> None:
    """出金履历可带 toTimestamp。"""
    client, transport = build(tmp_path, [FIAT_ROW])
    client.fiat_withdrawal_history(FROM_TS, FROM_TS + timedelta(minutes=30))
    assert transport.calls[0].path == "/v1/account/fiatWithdrawal/history"
    assert transport.calls[0].params == {
        "fromTimestamp": "2026-08-05T12:00:00.000Z",
        "toTimestamp": "2026-08-05T12:30:00.000Z",
    }


def test_history_window_exceeded_raises(tmp_path: Path) -> None:
    """履历窗口超过三十分钟即快速失败。"""
    client, transport = build(tmp_path)
    with pytest.raises(ValueError):
        client.fiat_deposit_history(FROM_TS, FROM_TS + timedelta(minutes=31))
    assert transport.calls == []


def test_deposit_history_params(tmp_path: Path) -> None:
    """暗号資産预入履历带品种参数。"""
    client, transport = build(tmp_path, [CRYPTO_ROW])
    items = client.deposit_history("BTC", FROM_TS)
    assert items[0].tx_hash is not None
    assert transport.calls[0].path == "/v1/account/deposit/history"
    assert transport.calls[0].params["symbol"] == "BTC"


def test_withdrawal_history_path(tmp_path: Path) -> None:
    """暗号資産送付履历路径。"""
    client, transport = build(tmp_path, [])
    assert client.withdrawal_history("BTC", FROM_TS) == ()
    assert transport.calls[0].path == "/v1/account/withdrawal/history"


def test_orders_join_ids(tmp_path: Path) -> None:
    """委托号以逗号连接（U-01）。"""
    client, transport = build(tmp_path, {"list": [ORDER_ROW]})
    orders = client.orders([123456789, 987654321])
    assert orders[0].status is OrderStatus.ORDERED
    assert orders[0].price == Decimal("9000000")
    assert transport.calls[0].path == "/v1/orders"
    assert transport.calls[0].params == {"orderId": "123456789,987654321"}


def test_orders_id_count_bounds(tmp_path: Path) -> None:
    """委托号数量越界即报错。"""
    client, transport = build(tmp_path)
    with pytest.raises(ValueError):
        client.orders([])
    with pytest.raises(ValueError):
        client.orders(list(range(1, 12)))
    assert transport.calls == []


def test_orders_empty_data_returns_empty(tmp_path: Path) -> None:
    """空结果时 data 可能为空对象。"""
    client, _ = build(tmp_path, {}, None)
    assert client.orders([1]) == ()
    assert client.orders([1]) == ()


def test_active_orders_params(tmp_path: Path) -> None:
    """挂单查询带品种与分页。"""
    client, transport = build(tmp_path, {"list": [ORDER_ROW]})
    assert len(client.active_orders("BTC", page=2, count=50)) == 1
    assert transport.calls[0].path == "/v1/activeOrders"
    assert transport.calls[0].params == {"symbol": "BTC", "page": 2, "count": 50}


def test_executions_by_order_id(tmp_path: Path) -> None:
    """按委托号查成交（U-01）。"""
    client, transport = build(tmp_path, {"list": [EXECUTION_ROW]})
    executions = client.executions(order_id=123456789)
    assert executions[0].side is Side.BUY
    assert executions[0].fee == Decimal("323")
    assert transport.calls[0].params == {"orderId": 123456789}


def test_executions_by_execution_ids(tmp_path: Path) -> None:
    """按成交号查成交，逗号连接。"""
    client, transport = build(tmp_path, {"list": [EXECUTION_ROW]})
    client.executions(execution_ids=[72123911, 72123912])
    assert transport.calls[0].params == {"executionId": "72123911,72123912"}


def test_executions_arguments_mutually_exclusive(tmp_path: Path) -> None:
    """两个查询参数恰须提供其一。"""
    client, transport = build(tmp_path)
    with pytest.raises(ValueError):
        client.executions()
    with pytest.raises(ValueError):
        client.executions(order_id=1, execution_ids=[2])
    with pytest.raises(ValueError):
        client.executions(execution_ids=list(range(1, 12)))
    assert transport.calls == []


def test_latest_executions_params(tmp_path: Path) -> None:
    """最新成交一览参数。"""
    client, transport = build(tmp_path, {"list": [EXECUTION_ROW]})
    assert len(client.latest_executions("BTC", count=10)) == 1
    assert transport.calls[0].path == "/v1/latestExecutions"
    assert transport.calls[0].params == {"symbol": "BTC", "count": 10}


def test_open_positions_leverage_symbol(tmp_path: Path) -> None:
    """建玉一覧只接受杠杆形态品种（U-02）。"""
    client, transport = build(tmp_path, {"list": [POSITION_ROW]})
    positions = client.open_positions(LeverageSymbol("BTC_JPY"))
    assert positions[0].leverage == Decimal("4")
    assert transport.calls[0].path == "/v1/openPositions"
    assert transport.calls[0].params == {"symbol": "BTC_JPY"}


def test_position_summary_optional_symbol(tmp_path: Path) -> None:
    """持仓汇总可省略品种。"""
    client, transport = build(tmp_path, POSITION_SUMMARY_DATA, POSITION_SUMMARY_DATA)
    assert client.position_summary()[0].symbol == "BTC_JPY"
    assert transport.calls[0].params == {}
    client.position_summary(LeverageSymbol("BTC_JPY"))
    assert transport.calls[1].params == {"symbol": "BTC_JPY"}


def test_read_client_requires_read_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """只读客户端只索取 READ_ONLY 密钥（T-02）。"""
    for name in (
        "GMO_COIN_READ_ONLY_API_KEY",
        "GMO_COIN_READ_ONLY_API_SECRET",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("GMO_COIN_TRADE_API_KEY", "dummy-key")
    monkeypatch.setenv("GMO_COIN_TRADE_API_SECRET", "dummy-secret")
    config = load_config(env_file=tmp_path / "absent.env")
    with pytest.raises(ConfigError):
        ReadClient.from_config(config)
