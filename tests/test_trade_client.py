"""写路径客户端单测。绝不打真实写端点（C-13、C-14）。"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path

import pytest

from guvolu.api.trade_client import CancelFailure, TradeClient
from guvolu.api.transport import HttpMethod, Params, PrivateTransport, RateLimiter
from guvolu.domain.config import load_config
from guvolu.domain.enums import ExecutionType, RunMode, SettleType, Side, TimeInForce
from guvolu.domain.errors import ConfigError, DryRunBlocked, SymbolError
from guvolu.domain.symbols import LeverageSymbol, SpotSymbol, Symbol

WHITELIST = frozenset({SpotSymbol("BTC")})


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
    tmp_path: Path, mode: RunMode = RunMode.DRY_RUN, *responses: object
) -> tuple[TradeClient, FakePrivateTransport]:
    """构造写路径客户端与其传输替身。"""
    transport = FakePrivateTransport(tmp_path, responses)
    return TradeClient(transport, mode, WHITELIST), transport


def test_order_blocked_in_dry_run(tmp_path: Path) -> None:
    """缺省模拟运行拒绝下单且不发请求（T-04）。"""
    client, transport = build(tmp_path)
    with pytest.raises(DryRunBlocked):
        client.order(
            SpotSymbol("BTC"), Side.BUY, ExecutionType.MARKET, Decimal("0.0001")
        )
    assert transport.calls == []


def test_change_order_blocked_in_dry_run(tmp_path: Path) -> None:
    """模拟运行同样拒绝改单（T-04）。"""
    client, transport = build(tmp_path)
    with pytest.raises(DryRunBlocked):
        client.change_order(123456789, Decimal("9000000"))
    assert transport.calls == []


def test_order_rejects_symbol_outside_whitelist(tmp_path: Path) -> None:
    """白名单外品种一律拒绝（T-09）。"""
    client, transport = build(tmp_path, RunMode.LIVE)
    with pytest.raises(SymbolError):
        client.order(
            SpotSymbol("ETH"), Side.BUY, ExecutionType.MARKET, Decimal("0.01")
        )
    assert transport.calls == []


def test_mode_guard_precedes_whitelist_guard(tmp_path: Path) -> None:
    """模拟运行守卫先于白名单守卫。"""
    client, _ = build(tmp_path)
    with pytest.raises(DryRunBlocked):
        client.order(
            SpotSymbol("ETH"), Side.BUY, ExecutionType.MARKET, Decimal("0.01")
        )


def test_market_order_rejects_price(tmp_path: Path) -> None:
    """市价委托带价格即参数错误。"""
    client, transport = build(tmp_path, RunMode.LIVE)
    with pytest.raises(ValueError):
        client.order(
            SpotSymbol("BTC"),
            Side.BUY,
            ExecutionType.MARKET,
            Decimal("0.0001"),
            price=Decimal("9000000"),
        )
    assert transport.calls == []


def test_limit_and_stop_order_require_price(tmp_path: Path) -> None:
    """限价与止损委托缺价格即参数错误。"""
    client, transport = build(tmp_path, RunMode.LIVE)
    for execution_type in (ExecutionType.LIMIT, ExecutionType.STOP):
        with pytest.raises(ValueError):
            client.order(
                SpotSymbol("BTC"), Side.BUY, execution_type, Decimal("0.0001")
            )
    assert transport.calls == []


def test_live_market_order_body_and_return(tmp_path: Path) -> None:
    """实盘市价委托的请求体与返回值。"""
    client, transport = build(tmp_path, RunMode.LIVE, "637000")
    order_id = client.order(
        SpotSymbol("BTC"), Side.BUY, ExecutionType.MARKET, Decimal("0.00001")
    )
    assert order_id == 637000
    call = transport.calls[0]
    assert (call.method, call.path) == ("POST", "/v1/order")
    assert call.body == {
        "symbol": "BTC",
        "side": "BUY",
        "executionType": "MARKET",
        "size": "0.00001",
    }


def test_live_limit_order_body(tmp_path: Path) -> None:
    """实盘限价委托带价格与执行数量条件。"""
    client, transport = build(tmp_path, RunMode.LIVE, "637001")
    client.order(
        SpotSymbol("BTC"),
        Side.SELL,
        ExecutionType.LIMIT,
        Decimal("0.0002"),
        price=Decimal("10000000"),
        time_in_force=TimeInForce.FAS,
        cancel_before=True,
    )
    assert transport.calls[0].body == {
        "symbol": "BTC",
        "side": "SELL",
        "executionType": "LIMIT",
        "size": "0.0002",
        "price": "10000000",
        "timeInForce": "FAS",
        "cancelBefore": True,
    }


def test_size_serialization_never_uses_float(tmp_path: Path) -> None:
    """数量与价格按 Decimal 原样序列化（T-08）。"""
    client, transport = build(tmp_path, RunMode.LIVE, "637002")
    client.order(
        SpotSymbol("BTC"),
        Side.BUY,
        ExecutionType.LIMIT,
        Decimal("1E-5"),
        price=Decimal("0.10"),
    )
    body = transport.calls[0].body
    assert body["size"] == "0.00001"
    assert body["price"] == "0.10"


def test_cancel_before_omitted_when_false(tmp_path: Path) -> None:
    """cancel_before 为假时不进请求体。"""
    client, transport = build(tmp_path, RunMode.LIVE, "637003")
    client.order(
        SpotSymbol("BTC"), Side.BUY, ExecutionType.MARKET, Decimal("0.0001")
    )
    assert "cancelBefore" not in transport.calls[0].body


def test_change_order_body_in_live(tmp_path: Path) -> None:
    """实盘改单的请求体。"""
    client, transport = build(tmp_path, RunMode.LIVE, None)
    client.change_order(123456789, Decimal("9000000"), Decimal("8000000"))
    call = transport.calls[0]
    assert (call.method, call.path) == ("POST", "/v1/changeOrder")
    assert call.body == {
        "orderId": 123456789,
        "price": "9000000",
        "losscutPrice": "8000000",
    }


def test_cancel_order_allowed_in_dry_run(tmp_path: Path) -> None:
    """撤单在模拟运行下同样可用，紧急路径必须可达（T-07）。"""
    client, transport = build(tmp_path, RunMode.DRY_RUN, None)
    client.cancel_order(123456789)
    call = transport.calls[0]
    assert (call.method, call.path) == ("POST", "/v1/cancelOrder")
    assert call.body == {"orderId": 123456789}


def test_cancel_orders_parses_success_and_failed(tmp_path: Path) -> None:
    """批量撤单区分受理与被拒。"""
    data: dict[str, object] = {
        "success": [637000, 637001],
        "failed": [
            {
                "message_code": "ERR-151",
                "message_string": "Service Unavailable",
                "orderId": 637002,
            }
        ],
    }
    client, transport = build(tmp_path, RunMode.DRY_RUN, data)
    result = client.cancel_orders([637000, 637001, 637002])
    assert result.success == (637000, 637001)
    assert result.failed == (
        CancelFailure(
            order_id=637002,
            code="ERR-151",
            message="Service Unavailable",
        ),
    )
    assert transport.calls[0].body == {"orderIds": [637000, 637001, 637002]}


def test_cancel_orders_empty_data(tmp_path: Path) -> None:
    """载荷缺省时返回空结果。"""
    client, _ = build(tmp_path, RunMode.DRY_RUN, None)
    result = client.cancel_orders([637000])
    assert result.success == ()
    assert result.failed == ()


def test_cancel_bulk_order_body_and_result(tmp_path: Path) -> None:
    """全撤请求体与受理的委托号（T-07）。"""
    symbols: list[Symbol] = [SpotSymbol("BTC"), LeverageSymbol("BTC_JPY")]
    client, transport = build(tmp_path, RunMode.DRY_RUN, [637000, 637001])
    order_ids = client.cancel_bulk_order(
        symbols,
        side=Side.BUY,
        settle_type=SettleType.OPEN,
        newer_first=True,
    )
    assert order_ids == (637000, 637001)
    call = transport.calls[0]
    assert (call.method, call.path) == ("POST", "/v1/cancelBulkOrder")
    assert call.body == {
        "symbols": ["BTC", "BTC_JPY"],
        "side": "BUY",
        "settleType": "OPEN",
        "desc": True,
    }


def test_cancel_bulk_order_minimal_body(tmp_path: Path) -> None:
    """可选参数缺省时不进请求体。"""
    client, transport = build(tmp_path, RunMode.DRY_RUN, None)
    assert client.cancel_bulk_order([SpotSymbol("BTC")]) == ()
    assert transport.calls[0].body == {"symbols": ["BTC"]}


def test_trade_client_requires_trade_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """写路径客户端只索取 TRADE 密钥（T-02）。"""
    for name in ("GMO_COIN_TRADE_API_KEY", "GMO_COIN_TRADE_API_SECRET"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("GMO_COIN_READ_ONLY_API_KEY", "dummy-key")
    monkeypatch.setenv("GMO_COIN_READ_ONLY_API_SECRET", "dummy-secret")
    config = load_config(env_file=tmp_path / "absent.env")
    with pytest.raises(ConfigError):
        TradeClient.from_config(config)


def test_from_config_keeps_mode_and_whitelist(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """构造后仍保持缺省模拟运行（T-04）。"""
    monkeypatch.setenv("GMO_COIN_TRADE_API_KEY", "dummy-key")
    monkeypatch.setenv("GMO_COIN_TRADE_API_SECRET", "dummy-secret")
    monkeypatch.delenv("GUVOLU_MODE", raising=False)
    monkeypatch.setenv("GUVOLU_LOG_DIR", str(tmp_path))
    config = load_config(env_file=tmp_path / "absent.env")
    client = TradeClient.from_config(config)
    with pytest.raises(DryRunBlocked):
        client.order(
            SpotSymbol("BTC"), Side.BUY, ExecutionType.MARKET, Decimal("0.0001")
        )
