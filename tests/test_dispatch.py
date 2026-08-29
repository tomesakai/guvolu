"""发送编排单测：注入替身，绝不触发真实端点（C-13、C-14）。"""
from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from guvolu.data.intent_ledger import IntentLedger
from guvolu.domain.config import Limits
from guvolu.domain.enums import ExecutionType, ServiceStatus, Side
from guvolu.domain.errors import (
    ApiTimeout,
    DryRunBlocked,
    GmoApiError,
    PaperSettled,
)
from guvolu.domain.intent import IntentState, OrderIntent
from guvolu.domain.symbols import SpotSymbol
from guvolu.execution.dispatch import dispatch_order_intent
from guvolu.execution.inflight_lock import acquire_symbol_inflight_lock
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
        self,
        order_id: int | None = None,
        error: Exception | None = None,
        consumes_write_budget: bool = True,
    ) -> None:
        self._order_id = order_id
        self._error = error
        self.consumes_write_budget = consumes_write_budget
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
        inflight_dir=tmp_path / "inflight",
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


def test_dry_run_block_is_local_terminal(tmp_path: Path) -> None:
    """模拟拦截记为本地终态，不计熔断异常（T-04）。"""
    sender = FakeSender(error=DryRunBlocked("模拟运行模式拒绝写请求"))
    ledger, gate, breaker, result = dispatch(
        tmp_path, make_intent("it01"), sender
    )
    assert getattr(result, "state") is IntentState.DRY_RUN_BLOCKED
    assert "模拟运行" in str(getattr(result, "reason"))
    assert ledger.state("it01") is IntentState.DRY_RUN_BLOCKED
    assert ledger.in_flight() == ()
    assert breaker.consecutive_failures == 0
    # 保守计数不回退
    assert gate.usage().total_jpy == Decimal("100")
    reloaded = IntentLedger(tmp_path / "intent_ledger.jsonl")
    assert reloaded.state("it01") is IntentState.DRY_RUN_BLOCKED


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


def test_zero_write_senders_never_consume_daily_budget(tmp_path: Path) -> None:
    """paper 与 dry-run 连续多笔不累计用量、不熔断（T-11）。"""
    parts = build(tmp_path)
    ledger, gate, breaker = parts
    for index in range(30):
        if index % 2 == 0:
            sender = FakeSender(
                error=PaperSettled("paper 结算", {"fill_basis": "test"}),
                consumes_write_budget=False,
            )
            expected = IntentState.PAPER_FILLED
        else:
            sender = FakeSender(
                error=DryRunBlocked("模拟运行模式拒绝写请求"),
                consumes_write_budget=False,
            )
            expected = IntentState.DRY_RUN_BLOCKED
        _, _, _, result = dispatch(
            tmp_path, make_intent(f"it{index:02d}"), sender, parts=parts
        )
        assert getattr(result, "state") is expected
        assert getattr(result, "consumed_write_budget") is False
    # 累计远超单日上限仍不熔断
    assert gate.usage().total_jpy == Decimal("0")
    assert gate.usage().order_count == 0
    assert breaker.state is BreakerState.NORMAL
    assert ledger.in_flight() == ()


def test_zero_write_sender_still_rehearses_limit_gate(tmp_path: Path) -> None:
    """零写路径仍彩排三限额：超单笔上限照旧拒绝并熔断（T-11）。"""
    sender = FakeSender(
        error=PaperSettled("paper 结算", {"fill_basis": "test"}),
        consumes_write_budget=False,
    )
    over = make_intent("it01", price=Decimal("6000000"))
    ledger, gate, breaker, result = dispatch(tmp_path, over, sender)
    assert getattr(result, "state") is IntentState.GATE_REJECTED
    assert breaker.state is BreakerState.TRIPPED
    assert sender.sent == []
    assert gate.usage().order_count == 0
    assert ledger.state("it01") is IntentState.GATE_REJECTED


def test_consuming_sender_accumulates_and_trips_on_day_limit(
    tmp_path: Path,
) -> None:
    """真实写替身照常累计并在超当日预算时熔断（T-11）。"""
    ledger = IntentLedger(tmp_path / "intent_ledger.jsonl")
    gate = LimitGate(
        Limits(
            order_jpy_max=Decimal("500"),
            day_jpy_max=Decimal("250"),
            day_count_max=50,
        )
    )
    parts = (ledger, gate, CircuitBreaker(THRESHOLDS))
    for index in range(2):
        _, _, _, result = dispatch(
            tmp_path,
            make_intent(f"it{index:02d}"),
            FakeSender(order_id=637001 + index),
            parts=parts,
        )
        assert getattr(result, "state") is IntentState.ACCEPTED
        assert getattr(result, "consumed_write_budget") is True
    assert gate.usage().total_jpy == Decimal("200")
    _, _, breaker, third = dispatch(
        tmp_path,
        make_intent("it02"),
        FakeSender(order_id=637999),
        parts=parts,
    )
    assert getattr(third, "state") is IntentState.GATE_REJECTED
    assert breaker.state is BreakerState.TRIPPED
    assert "当日累计" in str(getattr(third, "reason"))


def test_ledger_persists_write_budget_marker(tmp_path: Path) -> None:
    """账本迁移行留写预算标记，可审计重放（T-11、R-07）。"""
    parts = build(tmp_path)
    dispatch(
        tmp_path,
        make_intent("it-real"),
        FakeSender(order_id=637001),
        parts=parts,
    )
    dispatch(
        tmp_path,
        make_intent("it-paper"),
        FakeSender(
            error=PaperSettled("paper 结算", {"fill_basis": "test"}),
            consumes_write_budget=False,
        ),
        parts=parts,
    )
    reloaded = IntentLedger(tmp_path / "intent_ledger.jsonl")
    assert reloaded.consumed_write_budget("it-real") is True
    assert reloaded.consumed_write_budget("it-paper") is False


def make_second_gate(
    tmp_path: Path,
) -> tuple[IntentLedger, LimitGate, CircuitBreaker]:
    """构造第二本账本与闸门，模拟另一进程。"""
    ledger2 = IntentLedger(tmp_path / "ledger2.jsonl")
    gate2 = LimitGate(
        Limits(
            order_jpy_max=Decimal("500"),
            day_jpy_max=Decimal("2000"),
            day_count_max=50,
        )
    )
    return ledger2, gate2, CircuitBreaker(THRESHOLDS)


class NestedDispatchSender:
    """发送期间以第二本账本模拟另一进程并发送单。"""

    consumes_write_budget = True

    def __init__(self, tmp_path: Path, order_id: int) -> None:
        self._tmp_path = tmp_path
        self._order_id = order_id
        self.inner_result: object | None = None

    def send(self, intent: OrderIntent) -> int:
        ledger2, gate2, breaker2 = make_second_gate(self._tmp_path)
        self.inner_result = dispatch_order_intent(
            make_intent("it-second"),
            ledger=ledger2,
            limit_gate=gate2,
            breaker=breaker2,
            service_status=ServiceStatus.OPEN,
            whitelist=WHITELIST,
            sender=FakeSender(order_id=637999),
            moment=MOMENT,
            inflight_dir=self._tmp_path / "inflight",
        )
        return self._order_id


def test_cross_process_inflight_lock_rejects_concurrent_send(
    tmp_path: Path,
) -> None:
    """两本账本并发同品种：锁内第二笔被拒，释放后可再发（T-05）。"""
    outer = NestedDispatchSender(tmp_path, order_id=637001)
    _, _, _, result = dispatch(tmp_path, make_intent("it-first"), outer)
    assert getattr(result, "state") is IntentState.ACCEPTED
    inner = outer.inner_result
    assert inner is not None
    assert getattr(inner, "state") is IntentState.GATE_REJECTED
    assert "跨进程在途" in str(getattr(inner, "reason"))
    ledger2 = IntentLedger(tmp_path / "ledger2.jsonl")
    assert ledger2.state("it-second") is IntentState.GATE_REJECTED
    # 首笔终态落账后锁已释放，可再发
    ledger2b, gate2b, breaker2b = make_second_gate(tmp_path)
    retry = dispatch_order_intent(
        make_intent("it-third"),
        ledger=ledger2b,
        limit_gate=gate2b,
        breaker=breaker2b,
        service_status=ServiceStatus.OPEN,
        whitelist=WHITELIST,
        sender=FakeSender(order_id=638000),
        moment=MOMENT,
        inflight_dir=tmp_path / "inflight",
    )
    assert retry.state is IntentState.ACCEPTED


def test_zero_write_send_path_takes_no_inflight_lock(tmp_path: Path) -> None:
    """零写发送路径不取锁：锁被他方持有时 paper 照常结算。"""
    held = acquire_symbol_inflight_lock(
        SpotSymbol("BTC"), directory=tmp_path / "inflight"
    )
    assert held is not None
    try:
        sender = FakeSender(
            error=PaperSettled("paper 结算", {"fill_basis": "test"}),
            consumes_write_budget=False,
        )
        _, _, _, result = dispatch(tmp_path, make_intent("it-paper"), sender)
        assert getattr(result, "state") is IntentState.PAPER_FILLED
        blocked_parts = build(tmp_path / "other")
        _, _, _, blocked = dispatch(
            tmp_path,
            make_intent("it-real"),
            FakeSender(order_id=637001),
            parts=blocked_parts,
        )
        assert getattr(blocked, "state") is IntentState.GATE_REJECTED
        assert "跨进程在途" in str(getattr(blocked, "reason"))
    finally:
        held.release()
    released_parts = build(tmp_path / "released")
    _, _, _, after = dispatch(
        tmp_path,
        make_intent("it-after"),
        FakeSender(order_id=637002),
        parts=released_parts,
    )
    assert getattr(after, "state") is IntentState.ACCEPTED
