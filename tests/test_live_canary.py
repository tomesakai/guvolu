"""最小实盘 canary 的离线单测（C-13、C-14：不触任何真实端点）。"""
from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from guvolu.domain.config import Config, Limits, load_config
from guvolu.domain.enums import (
    ExecutionType,
    OrderStatus,
    OrderType,
    RunMode,
    ServiceStatus,
    SettleType,
    Side,
    TimeInForce,
)
from guvolu.domain.intent import IntentState, OrderIntent
from guvolu.domain.models import Execution, Order
from guvolu.domain.symbols import SpotSymbol
from guvolu.execution.conversion import MarketRule
from guvolu.execution.live_canary import (
    CANARY_MAX_NOTIONAL_JPY,
    CanaryError,
    build_plan,
    confirm_plan,
    main,
    poll_until_terminal,
    render_banner,
    run_canary,
)
from guvolu.data.intent_ledger import IntentLedger
from guvolu.risk.circuit_breaker import (
    CircuitBreaker,
    load_breaker_thresholds,
)
from guvolu.risk.limits import LimitGate

REPO = Path(__file__).resolve().parents[1]
BTC = SpotSymbol("BTC")
NOW = datetime(2026, 8, 29, 12, 0, tzinfo=UTC)

RULE = MarketRule(
    symbol=BTC,
    tick_size=Decimal("1"),
    size_step=Decimal("0.00001"),
    min_order_size=Decimal("0.00001"),
    max_order_size=Decimal("5"),
)


def _order(order_id: int, status: OrderStatus) -> Order:
    return Order(
        root_order_id=order_id,
        order_id=order_id,
        symbol="BTC",
        side=Side.BUY,
        order_type=OrderType.NORMAL,
        execution_type=ExecutionType.LIMIT,
        settle_type=SettleType.OPEN,
        size=Decimal("0.00002"),
        executed_size=(
            Decimal("0.00002")
            if status is OrderStatus.EXECUTED else Decimal("0")
        ),
        price=Decimal("12000000"),
        losscut_price=Decimal("0"),
        status=status,
        cancel_type=None,
        time_in_force=TimeInForce.FAS,
        timestamp=NOW,
    )


class _Reader:
    """READ_ONLY 替身：按脚本给出委托快照。"""

    def __init__(self, script: list[OrderStatus]) -> None:
        self.script = script
        self.calls = 0

    def orders(self, order_ids: Sequence[int]) -> tuple[Order, ...]:
        status = self.script[min(self.calls, len(self.script) - 1)]
        self.calls += 1
        return (_order(order_ids[0], status),)

    def active_orders(
        self, symbol: str, page: int | None = None, count: int | None = None,
    ) -> tuple[Order, ...]:
        return ()

    def latest_executions(
        self, symbol: str, page: int | None = None, count: int | None = None,
    ) -> tuple[Execution, ...]:
        return ()


class _Sender:
    """发送替身：受理并记录撤单调用。"""

    consumes_write_budget = True

    def __init__(self) -> None:
        self.cancelled: list[int] = []

    def send(self, intent: OrderIntent) -> int:
        return 4242

    def cancel(self, order_id: int) -> None:
        self.cancelled.append(order_id)
        # 撤单后快照转终态
        self.on_cancel()

    def on_cancel(self) -> None:
        pass


class _Clock:
    """假单调时钟，sleep 即推进。"""

    def __init__(self) -> None:
        self.value = 0.0

    def clock(self) -> float:
        return self.value

    def sleep(self, seconds: float) -> None:
        self.value += seconds


def _config(mode: RunMode = RunMode.LIVE) -> Config:
    return Config(
        mode=mode,
        read_api_key=None,
        read_api_secret=None,
        trade_api_key=None,
        trade_api_secret=None,
        bitflyer_read_api_key=None,
        bitflyer_read_api_secret=None,
        bitflyer_private_rps=1.0,
        spot_whitelist=frozenset({BTC}),
        limits=Limits(
            order_jpy_max=Decimal("500"),
            day_jpy_max=Decimal("2000"),
            day_count_max=50,
        ),
        log_dir=Path("logs"),
        private_rps=1.0,
        public_rps=1.0,
        recent_trades_max_seconds=60,
        tile_refresh_seconds=60,
    )


def _gates(tmp_path: Path) -> tuple[IntentLedger, LimitGate, CircuitBreaker]:
    ledger = IntentLedger(tmp_path / "canary_intent_ledger.jsonl")
    gate = LimitGate(_config().limits)
    breaker = CircuitBreaker(
        load_breaker_thresholds(REPO / "config" / "circuit_breaker.json")
    )
    return ledger, gate, breaker


def test_build_plan_floors_price_to_tick_and_caps_notional() -> None:
    plan = build_plan(
        rule=MarketRule(
            symbol=BTC,
            tick_size=Decimal("100"),
            size_step=Decimal("0.00001"),
            min_order_size=Decimal("0.00001"),
            max_order_size=Decimal("5"),
        ),
        best_bid=Decimal("12345678"),
        size=Decimal("0.00002"),
        order_jpy_max=Decimal("500"),
        max_wait_seconds=60,
    )
    assert plan.price == Decimal("12345600")
    assert plan.notional_jpy == plan.size * plan.price
    assert plan.notional_jpy <= CANARY_MAX_NOTIONAL_JPY


def test_build_plan_rejects_notional_over_canary_ceiling() -> None:
    with pytest.raises(CanaryError, match="canary 上限"):
        build_plan(
            rule=RULE,
            best_bid=Decimal("12000000"),
            size=Decimal("0.00005"),
            order_jpy_max=Decimal("1000"),
            max_wait_seconds=60,
        )


def test_build_plan_ceiling_takes_current_limit_when_lower() -> None:
    with pytest.raises(CanaryError, match="canary 上限 100"):
        build_plan(
            rule=RULE,
            best_bid=Decimal("12000000"),
            size=Decimal("0.00002"),
            order_jpy_max=Decimal("100"),
            max_wait_seconds=60,
        )


def test_build_plan_rejects_price_above_best_bid() -> None:
    with pytest.raises(CanaryError, match="最优买价"):
        build_plan(
            rule=RULE,
            best_bid=Decimal("12000000"),
            size=Decimal("0.00002"),
            order_jpy_max=Decimal("500"),
            max_wait_seconds=60,
            price=Decimal("12000001"),
        )


def test_build_plan_rejects_off_step_size() -> None:
    with pytest.raises(CanaryError, match="步长"):
        build_plan(
            rule=RULE,
            best_bid=Decimal("12000000"),
            size=Decimal("0.000015"),
            order_jpy_max=Decimal("500"),
            max_wait_seconds=60,
        )


def test_confirm_refuses_non_interactive() -> None:
    plan = build_plan(
        rule=RULE, best_bid=Decimal("12000000"),
        size=Decimal("0.00002"), order_jpy_max=Decimal("500"),
        max_wait_seconds=60,
    )
    with pytest.raises(CanaryError, match="交互式终端"):
        confirm_plan(plan, input_fn=lambda _: "x", interactive=False)


def test_confirm_requires_phrase_and_notional() -> None:
    plan = build_plan(
        rule=RULE, best_bid=Decimal("12000000"),
        size=Decimal("0.00002"), order_jpy_max=Decimal("500"),
        max_wait_seconds=60,
    )
    answers = iter(["实盘 canary 确认", str(plan.notional_jpy)])
    assert confirm_plan(
        plan, input_fn=lambda _: next(answers), interactive=True,
    )
    wrong = iter(["实盘 canary 确认", "999"])
    assert not confirm_plan(
        plan, input_fn=lambda _: next(wrong), interactive=True,
    )
    bad_phrase = iter(["yes", "240.00000"])
    assert not confirm_plan(
        plan, input_fn=lambda _: next(bad_phrase), interactive=True,
    )


def test_banner_marks_live_and_lists_endpoints() -> None:
    plan = build_plan(
        rule=RULE, best_bid=Decimal("12000000"),
        size=Decimal("0.00002"), order_jpy_max=Decimal("500"),
        max_wait_seconds=60,
    )
    banner = render_banner(plan, _config())
    assert "实盘（live）" in banner
    assert "POST /v1/order" in banner
    assert "POST /v1/cancelOrder" in banner
    assert "撤单" in banner
    assert "kill_switch" in banner


def test_run_canary_filled_within_window(tmp_path: Path) -> None:
    ledger, gate, breaker = _gates(tmp_path)
    reader = _Reader([OrderStatus.ORDERED, OrderStatus.EXECUTED])
    sender = _Sender()
    fake = _Clock()
    plan = build_plan(
        rule=RULE, best_bid=Decimal("12000000"),
        size=Decimal("0.00002"), order_jpy_max=Decimal("500"),
        max_wait_seconds=60,
    )
    outcome = run_canary(
        plan, config=_config(), ledger=ledger, reader=reader,
        sender=sender, service_status=ServiceStatus.OPEN,
        limit_gate=gate, breaker=breaker, moment=NOW,
        clock=fake.clock, sleep=fake.sleep,
        inflight_dir=tmp_path / "inflight",
    )
    assert outcome.dispatch.state is IntentState.ACCEPTED
    assert outcome.dispatch.order_id == 4242
    assert outcome.final_order is not None
    assert outcome.final_order.status is OrderStatus.EXECUTED
    assert not outcome.cancel_requested
    assert sender.cancelled == []
    assert ledger.state(outcome.dispatch.intent_id) is IntentState.ACCEPTED


def test_run_canary_cancels_after_window(tmp_path: Path) -> None:
    ledger, gate, breaker = _gates(tmp_path)
    reader = _Reader([OrderStatus.ORDERED])
    sender = _Sender()

    def flip() -> None:
        reader.script = [OrderStatus.CANCELED]

    sender.on_cancel = flip  # type: ignore[method-assign]
    fake = _Clock()
    plan = build_plan(
        rule=RULE, best_bid=Decimal("12000000"),
        size=Decimal("0.00002"), order_jpy_max=Decimal("500"),
        max_wait_seconds=10,
    )
    outcome = run_canary(
        plan, config=_config(), ledger=ledger, reader=reader,
        sender=sender, service_status=ServiceStatus.OPEN,
        limit_gate=gate, breaker=breaker, moment=NOW,
        clock=fake.clock, sleep=fake.sleep,
        inflight_dir=tmp_path / "inflight",
    )
    assert outcome.cancel_requested
    assert sender.cancelled == [4242]
    assert outcome.final_order is not None
    assert outcome.final_order.status is OrderStatus.CANCELED


def test_main_refuses_non_live_mode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.delenv("GUVOLU_MODE", raising=False)
    env = tmp_path / "canary.env"
    env.write_text("GUVOLU_MODE=dry-run\n", encoding="utf-8")
    assert load_config(env).mode is RunMode.DRY_RUN
    assert main(["--env-file", str(env)]) == 2
    assert "GUVOLU_MODE=live" in capsys.readouterr().out
