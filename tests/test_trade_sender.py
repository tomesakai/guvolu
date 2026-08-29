"""发送适配器单测：三分类翻译，绝不触发真实端点（C-13、C-14）。"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from guvolu.api.trade_client import TradeClient
from guvolu.api.transport import (
    HttpMethod,
    Params,
    PrivateTransport,
    RateLimiter,
)
from guvolu.domain.enums import ExecutionType, RunMode, Side, TimeInForce
from guvolu.domain.errors import (
    ApiHttpError,
    ApiNetworkError,
    ApiTimeout,
    DryRunBlocked,
    GmoApiError,
)
from guvolu.domain.intent import OrderIntent
from guvolu.domain.symbols import SpotSymbol
from guvolu.execution.trade_sender import TradeClientSender

WHITELIST = frozenset({SpotSymbol("BTC")})
CREATED_AT = datetime(2026, 8, 16, 0, 0, tzinfo=UTC)


@dataclass(frozen=True, slots=True)
class Call:
    """一次请求的记录。"""

    method: str
    path: str
    params: dict[str, str | int] = field(default_factory=dict)
    body: dict[str, object] = field(default_factory=dict)


class FakePrivateTransport(PrivateTransport):
    """私有传输替身，按预置响应或异常应答，绝不发真实请求（C-14）。"""

    def __init__(
        self,
        tmp_path: Path,
        response: object = None,
        error: Exception | None = None,
    ) -> None:
        super().__init__("k", "s", RateLimiter(1000.0), tmp_path)
        self.calls: list[Call] = []
        self._response = response
        self._error = error

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
        if self._error is not None:
            raise self._error
        return self._response


def build(
    tmp_path: Path,
    mode: RunMode = RunMode.LIVE,
    response: object = None,
    error: Exception | None = None,
) -> tuple[TradeClientSender, FakePrivateTransport]:
    """构造适配器与其传输替身。"""
    transport = FakePrivateTransport(tmp_path, response, error)
    client = TradeClient(transport, mode, WHITELIST)
    return TradeClientSender(client), transport


def make_intent(
    execution_type: ExecutionType = ExecutionType.LIMIT,
    price: Decimal | None = Decimal("1000000"),
    time_in_force: TimeInForce | None = None,
) -> OrderIntent:
    """构造合法意图。"""
    return OrderIntent(
        intent_id="it0001",
        correlation_id="co0001",
        symbol=SpotSymbol("BTC"),
        side=Side.BUY,
        execution_type=execution_type,
        size=Decimal("0.0001"),
        price=price,
        time_in_force=time_in_force,
        created_at=CREATED_AT,
    )


def test_send_success_returns_order_id(tmp_path: Path) -> None:
    """明确成功：返回交易所委托号，请求体来自意图字段。"""
    sender, transport = build(tmp_path, response="637000")
    order_id = sender.send(
        make_intent(time_in_force=TimeInForce.FAS)
    )
    assert order_id == 637000
    call = transport.calls[0]
    assert (call.method, call.path) == ("POST", "/v1/order")
    assert call.body == {
        "symbol": "BTC",
        "side": "BUY",
        "executionType": "LIMIT",
        "size": "0.0001",
        "price": "1000000",
        "timeInForce": "FAS",
    }


def test_send_market_intent_without_price(tmp_path: Path) -> None:
    """市价意图不携带价格字段。"""
    sender, transport = build(tmp_path, response="637001")
    sender.send(make_intent(ExecutionType.MARKET, None))
    assert "price" not in transport.calls[0].body


def test_dry_run_guard_blocks_before_transport(tmp_path: Path) -> None:
    """模拟运行守卫在传输之前拦截下单（T-04）。"""
    sender, transport = build(tmp_path, mode=RunMode.DRY_RUN)
    with pytest.raises(DryRunBlocked):
        sender.send(make_intent())
    assert transport.calls == []


def test_api_error_passes_through(tmp_path: Path) -> None:
    """明确失败：业务错误原样上抛。"""
    error = GmoApiError(
        codes=("ERR-5106",),
        messages=("Invalid request parameter.",),
        path="/v1/order",
        http_status=200,
    )
    sender, _ = build(tmp_path, error=error)
    with pytest.raises(GmoApiError) as caught:
        sender.send(make_intent())
    assert caught.value is error


def test_timeout_passes_through(tmp_path: Path) -> None:
    """超时原样上抛，保持超时分类（T-06）。"""
    sender, _ = build(tmp_path, error=ApiTimeout("/v1/order", "超时"))
    with pytest.raises(ApiNetworkError):
        sender.send(make_intent())


def test_http_error_translates_to_network(tmp_path: Path) -> None:
    """HTTP 层异常写结果未知，折算为网络错（T-06）。"""
    error = ApiHttpError(502, "/v1/order", "非 JSON 响应")
    sender, _ = build(tmp_path, error=error)
    with pytest.raises(ApiNetworkError) as caught:
        sender.send(make_intent())
    assert not isinstance(caught.value, ApiHttpError)
    assert caught.value.__cause__ is error


def test_unparsable_order_id_translates_to_network(tmp_path: Path) -> None:
    """委托号不可解析时结果未知，折算为网络错（T-06）。"""
    sender, transport = build(tmp_path, response=None)
    with pytest.raises(ApiNetworkError):
        sender.send(make_intent())
    assert len(transport.calls) == 1


def test_cancel_not_blocked_in_dry_run(tmp_path: Path) -> None:
    """撤单透传不受模拟运行限制（T-07）。"""
    sender, transport = build(tmp_path, mode=RunMode.DRY_RUN)
    sender.cancel(637002)
    call = transport.calls[0]
    assert (call.method, call.path) == ("POST", "/v1/cancelOrder")
    assert call.body == {"orderId": 637002}
