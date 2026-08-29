"""熔断全撤接线单测：注入替身，绝不触发真实端点（C-13、C-14）。"""
from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal
from pathlib import Path

from guvolu.api.public_client import PublicClient
from guvolu.api.trade_client import TradeClient
from guvolu.api.transport import (
    HttpMethod,
    Params,
    PrivateTransport,
    PublicTransport,
    RateLimiter,
)
from guvolu.domain.enums import RunMode
from guvolu.domain.errors import ApiTimeout, GmoApiError
from guvolu.domain.symbols import SpotSymbol
from guvolu.execution.emergency_stop import (
    EMERGENCY_WRITE_ENDPOINT,
    EmergencyStopAction,
    arm_emergency_stop,
)
from guvolu.risk.circuit_breaker import (
    BreakerState,
    BreakerThresholds,
    CircuitBreaker,
)

THRESHOLDS = BreakerThresholds(
    schema_version=1,
    consecutive_failure_limit=2,
    stream_gap_seconds=90,
    asset_deviation_ratio=Decimal("0.01"),
    asset_deviation_floor_jpy=Decimal("30"),
)
SYMBOL_ROWS: list[object] = [
    {
        "symbol": "BTC",
        "minOrderSize": "0.00001",
        "maxOrderSize": "5",
        "sizeStep": "0.00001",
        "tickSize": "1",
        "takerFee": "0.0005",
        "makerFee": "-0.0001",
    },
    {
        "symbol": "BTC_JPY",
        "minOrderSize": "0.01",
        "maxOrderSize": "5",
        "sizeStep": "0.01",
        "tickSize": "1",
        "takerFee": "0",
        "makerFee": "0",
    },
]


class FakePublicTransport(PublicTransport):
    """公开传输替身，只返回品种一览。"""

    def __init__(self) -> None:
        super().__init__(RateLimiter(1000.0))

    def get_payload(
        self, path: str, params: Params | None = None
    ) -> Mapping[str, object]:
        return {"status": 0, "data": SYMBOL_ROWS}

    def get(self, path: str, params: Params | None = None) -> object:
        return self.get_payload(path, params).get("data")


class FakePrivateTransport(PrivateTransport):
    """私有传输替身，记录写调用并可注入异常。"""

    def __init__(self, tmp_path: Path, error: Exception | None = None) -> None:
        super().__init__("k", "s", RateLimiter(1000.0), tmp_path)
        self.calls: list[tuple[str, str, dict[str, object]]] = []
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
            (method, path, dict(body) if body is not None else {})
        )
        if self._error is not None:
            raise self._error
        return [637000]


def build(
    tmp_path: Path, error: Exception | None = None
) -> tuple[CircuitBreaker, FakePrivateTransport, EmergencyStopAction]:
    """构造已接线全撤动作的熔断器与写传输替身。"""
    breaker = CircuitBreaker(THRESHOLDS)
    public = PublicClient(FakePublicTransport())
    private = FakePrivateTransport(tmp_path, error)
    trade = TradeClient(
        private, RunMode.DRY_RUN, frozenset({SpotSymbol("BTC")})
    )
    action = arm_emergency_stop(breaker, public, trade)
    assert action.records == ()
    return breaker, private, action


def test_trip_invokes_cancel_bulk_once(tmp_path: Path) -> None:
    """连续异常触发熔断即全量撤单，重复触发不重复撤（T-07）。"""
    breaker, private, _action = build(tmp_path)
    breaker.record_write_failure()
    assert private.calls == []
    breaker.record_write_failure()
    assert breaker.state is BreakerState.TRIPPED
    assert len(private.calls) == 1
    method, path, body = private.calls[0]
    assert (method, path) == ("POST", "/v1/cancelBulkOrder")
    assert body == {"symbols": ["BTC", "BTC_JPY"]}
    assert EMERGENCY_WRITE_ENDPOINT == "POST /v1/cancelBulkOrder"
    breaker.trip("再次触发")
    breaker.record_write_failure()
    assert len(private.calls) == 1
    assert breaker.trip_reason is not None
    assert "连续写路径异常" in breaker.trip_reason


def test_cancel_reaches_endpoint_in_dry_run(tmp_path: Path) -> None:
    """撤单不受模拟运行守卫限制，紧急路径必达（T-07）。

    留痕记录零退出码，供会话报告列明触碰端点（A-03）。
    """
    breaker, private, action = build(tmp_path)
    breaker.trip("人工触发")
    assert len(private.calls) == 1
    assert action.records[0].exit_code == 0
    assert action.records[0].reason == "人工触发"
    assert action.records[0].error is None


def test_cancel_api_error_recorded_breaker_stays_tripped(
    tmp_path: Path,
) -> None:
    """交易所拒绝全撤时留痕非零退出码，熔断状态不回滚。"""
    error = GmoApiError(
        codes=("ERR-5201",),
        messages=("MAINTENANCE",),
        path="/v1/cancelBulkOrder",
        http_status=200,
    )
    breaker, _private, action = build(tmp_path, error)
    breaker.trip("人工触发")
    assert breaker.state is BreakerState.TRIPPED
    assert action.records[0].exit_code == 1
    assert action.records[0].error is None


def test_cancel_network_error_recorded_not_raised(tmp_path: Path) -> None:
    """网络错不外抛、不破坏熔断状态，留痕待人工（T-06 口径）。"""
    breaker, _private, action = build(
        tmp_path, ApiTimeout("/v1/cancelBulkOrder", "超时")
    )
    breaker.trip("人工触发")
    assert breaker.state is BreakerState.TRIPPED
    assert action.records[0].exit_code is None
    assert action.records[0].error is not None
    assert "超时" in action.records[0].error


def test_reset_then_new_trip_fires_again(tmp_path: Path) -> None:
    """人工复位后再次触发重新执行全撤动作。"""
    breaker, private, _action = build(tmp_path)
    breaker.trip("第一次")
    breaker.reset()
    assert breaker.state is BreakerState.NORMAL
    breaker.trip("第二次")
    assert len(private.calls) == 2
