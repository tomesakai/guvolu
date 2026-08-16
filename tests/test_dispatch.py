"""发送编排单测：注入替身，绝不触发真实端点（C-13、C-14）。"""
from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from guvolu.data.intent_ledger import IntentLedger
from guvolu.domain.config import Limits
from guvolu.domain.enums import ExecutionType, ServiceStatus, Side
from guvolu.domain.errors import ApiTimeout, GmoApiError
from guvolu.domain.intent import IntentState, OrderIntent
from guvolu.domain.symbols import SpotSymbol
from guvolu.execution.dispatch import dispatch_order_intent
from guvolu.risk.circuit_breaker import (
    BreakerState,
    BreakerThresholds,
    CircuitBreaker,
)
from guvolu.risk.limits import LimitGate

MOMENT = datetime(2026, 8, 16, 0, 0, tzinfo=UTC)
WHITELIST = frozenset({SpotSymbol("BTC")})
THRESHOLDS = BreakerThresholds(
    schema_version=1,
    consecutive_failure_limit=3,
    stream_gap_seconds=90,
    asset_deviation_ratio=Decimal("0.01"),
    asset_deviation_floor_jpy=Decimal("30"),
)


class FakeSender:
    """发送替身，按预设行为响应。"""

    def __init__(
        self, order_id: int | None = None, error: Exception | None = None
    ) -> None:
        self._order_id = order_id
        self._error = error
        self.sent: list[OrderIntent] = []

    def send(self, intent: OrderIntent) -> int:
        self.sent.append(intent)
        if self._error is not None:
            raise self._error
        assert self._order_id is not None
        return self._order_id


def make_intent(
    intent_id: str,
    symbol: str = "BTC",
    price: Decimal | None = Decimal("1000000"),
    execution_type: ExecutionType = ExecutionType.LIMIT,
) -> OrderIntent:
    """构造名义 100 JPY 的意图。"""
    return OrderIntent(
        intent_id=intent_id,
        correlation_id="co0001",
        symbol=SpotSymbol(symbol),
        side=Side.BUY,
        execution_type=execution_type,
        size=Decimal("0.0001"),
        price=price,
        time_in_force=None,
        created_at=MOMENT,
    )


def build(tmp_path: Path) -> tuple[IntentLedger, LimitGate, CircuitBreaker]:
    """构造账本、限额闸门与熔断器。"""
    ledger = IntentLedger(tmp_path / "intent_ledger.jsonl")
    gate = LimitGate(
        Limits(
            order_jpy_max=Decimal("500"),
            day_jpy_max=Decimal("2000"),
            day_count_max=50,
        )
    )
    return ledger, gate, CircuitBreaker(THRESHOLDS)


def dispatch(
    tmp_path: Path,
    intent: OrderIntent,
    sender: FakeSender,
    *,
    parts: tuple[IntentLedger, LimitGate, CircuitBreaker] | None = None,
    status: ServiceStatus = ServiceStatus.OPEN,
    reference_price: Decimal | None = None,
) -> tuple[
    IntentLedger, LimitGate, CircuitBreaker, object
]:
    """执行一次编排并返回各部件与结果。"""
    ledger, gate, breaker = parts if parts is not None else build(tmp_path)
    result = dispatch_order_intent(
        intent,
        ledger=ledger,
        limit_gate=gate,
        breaker=breaker,
        service_status=status,
        whitelist=WHITELIST,
        sender=sender,
        reference_price=reference_price,
        moment=MOMENT,
    )
    return ledger, gate, breaker, result


def test_success_path(tmp_path: Path) -> None:
    """成功路径：受理、映射、计数、限额记账。"""
    sender = FakeSender(order_id=637001)
    ledger, gate, breaker, result = dispatch(
        tmp_path, make_intent("it01"), sender
    )
    assert getattr(result, "state") is IntentState.ACCEPTED
    assert getattr(result, "order_id") == 637001
    assert ledger.state("it01") is IntentState.ACCEPTED
    assert ledger.intent_id_for_order(637001) == "it01"
    assert len(sender.sent) == 1
    assert gate.usage().total_jpy == Decimal("100")
    assert breaker.state is BreakerState.NORMAL


def test_whitelist_rejects_before_send(tmp_path: Path) -> None:
    """白名单外品种在闸门拒绝（T-09）。"""
    sender = FakeSender(order_id=637001)
    ledger, gate, _, result = dispatch(
        tmp_path, make_intent("it01", symbol="ETH"), sender
    )
    assert getattr(result, "state") is IntentState.GATE_REJECTED
    assert ledger.state("it01") is IntentState.GATE_REJECTED
    assert sender.sent == []
    assert gate.usage().order_count == 0


def test_service_not_open_rejected(tmp_path: Path) -> None:
    """非 OPEN 拒绝新意图（R-03）。"""
    for index, status in enumerate(
        (ServiceStatus.MAINTENANCE, ServiceStatus.PREOPEN)
    ):
        sender = FakeSender(order_id=637001)
        ledger, _, _, result = dispatch(
            tmp_path / str(index),
            make_intent("it01"),
            sender,
            status=status,
        )
        assert getattr(result, "state") is IntentState.GATE_REJECTED
        reason = getattr(result, "reason")
        assert status.value in str(reason)
        assert sender.sent == []
        assert ledger.state("it01") is IntentState.GATE_REJECTED


def test_breaker_tripped_rejected(tmp_path: Path) -> None:
    """熔断触发后拒绝新意图（R-02）。"""
    parts = build(tmp_path)
    parts[2].trip("人工触发")
    sender = FakeSender(order_id=637001)
    ledger, _, _, result = dispatch(
        tmp_path, make_intent("it01"), sender, parts=parts
    )
    assert getattr(result, "state") is IntentState.GATE_REJECTED
    assert "熔断" in str(getattr(result, "reason"))
    assert sender.sent == []
    assert ledger.state("it01") is IntentState.GATE_REJECTED


def test_limit_exceeded_trips_breaker(tmp_path: Path) -> None:
    """限额超限拒绝并触发熔断（T-11）。"""
    sender = FakeSender(order_id=637001)
    over = make_intent("it01", price=Decimal("6000000"))
    ledger, gate, breaker, result = dispatch(tmp_path, over, sender)
    assert getattr(result, "state") is IntentState.GATE_REJECTED
    assert breaker.state is BreakerState.TRIPPED
    assert "限额超限" in str(breaker.trip_reason)
    assert sender.sent == []
    assert gate.usage().order_count == 0
    assert ledger.state("it01") is IntentState.GATE_REJECTED


def test_timeout_marks_send_timeout(tmp_path: Path) -> None:
    """超时转入超时态并占用在途（T-06）。"""
    sender = FakeSender(error=ApiTimeout("/v1/order", "超时"))
    parts = build(tmp_path)
    ledger, gate, breaker, result = dispatch(
        tmp_path, make_intent("it01"), sender, parts=parts
    )
    assert getattr(result, "state") is IntentState.SEND_TIMEOUT
    assert ledger.state("it01") is IntentState.SEND_TIMEOUT
    assert breaker.consecutive_failures == 1
    assert gate.usage().total_jpy == Decimal("100")
    follow = FakeSender(order_id=637002)
    _, _, _, second = dispatch(
        tmp_path, make_intent("it02"), follow, parts=parts
    )
    assert getattr(second, "state") is IntentState.GATE_REJECTED
    assert "在途" in str(getattr(second, "reason"))
    assert follow.sent == []


def test_api_error_rejects(tmp_path: Path) -> None:
    """业务错误转入明确失败并计入熔断计数。"""
    error = GmoApiError(
        codes=("ERR-5106",),
        messages=("Invalid request parameter.",),
        path="/v1/order",
        http_status=200,
    )
    sender = FakeSender(error=error)
    ledger, _, breaker, result = dispatch(
        tmp_path, make_intent("it01"), sender
    )
    assert getattr(result, "state") is IntentState.REJECTED
    assert "ERR-5106" in str(getattr(result, "reason"))
    assert ledger.state("it01") is IntentState.REJECTED
    assert breaker.consecutive_failures == 1


def test_market_without_reference_rejected(tmp_path: Path) -> None:
    """市价意图缺参考价在闸门拒绝。"""
    sender = FakeSender(order_id=637001)
    market = make_intent(
        "it01", price=None, execution_type=ExecutionType.MARKET
    )
    ledger, gate, _, result = dispatch(tmp_path, market, sender)
    assert getattr(result, "state") is IntentState.GATE_REJECTED
    assert "参考价" in str(getattr(result, "reason"))
    assert sender.sent == []
    assert gate.usage().order_count == 0


def test_market_with_reference_passes(tmp_path: Path) -> None:
    """市价意图按参考价计名义金额。"""
    sender = FakeSender(order_id=637001)
    market = make_intent(
        "it01", price=None, execution_type=ExecutionType.MARKET
    )
    _, gate, _, result = dispatch(
        tmp_path, market, sender, reference_price=Decimal("2000000")
    )
    assert getattr(result, "state") is IntentState.ACCEPTED
    assert gate.usage().total_jpy == Decimal("200")
