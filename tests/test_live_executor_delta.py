"""live 执行链 2026-09-04 修复批次：差分计划、减仓豁免、撤单容错等。"""
from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from test_live_executor import (
    BTC,
    EXIT_OK,
    EXIT_REFUSED,
    NOW,
    PRICE,
    RULE,
    _Reader,
    _Sender,
    _artifact,
    _assets,
    _book,
    _cleared_state,
    _order,
    _runtime,
    _sending_intent,
    _write_envelope,
)

from guvolu.data.intent_ledger import IntentLedger
from guvolu.domain.enums import OrderStatus, Side
from guvolu.domain.errors import ApiNetworkError
from guvolu.domain.intent import IntentState
from guvolu.execution.authorization_envelope import (
    VERDICT_ALLOW,
    VERDICT_HALT,
    VERDICT_SKIP,
    EnvelopeUsage,
    GateInputs,
    evaluate_envelope_gates,
    load_envelope,
)
from guvolu.execution.dry_run_executor import build_delta_plan, build_plan
from guvolu.execution.live_executor import (
    apply_jpy_headroom,
    main,
    reconcile_usage_with_ledger,
    run_live_cycle,
)
from guvolu.risk.circuit_breaker import (
    BreakerState,
    BreakerThresholds,
    CircuitBreaker,
)


def test_delta_plan_sells_when_target_below_position() -> None:
    """差分计划：目标低于持仓卖出，高于买入，带内跳过。"""
    sell = build_delta_plan(
        _artifact(target=0.0), rule=RULE, reference_price=PRICE,
        budget_jpy=Decimal("5000"), position_size=Decimal("0.00020"),
        no_trade_band=Decimal("0.01"),
    )
    assert sell.proposal is not None and sell.proposal.side is Side.SELL
    assert sell.proposal.size == Decimal("0.00020")
    assert sell.position_size == Decimal("0.00020")
    buy = build_delta_plan(
        _artifact(target=0.5), rule=RULE, reference_price=PRICE,
        budget_jpy=Decimal("5000"), position_size=Decimal("0.00002"),
        no_trade_band=Decimal("0.01"),
    )
    assert buy.proposal is not None and buy.proposal.side is Side.BUY
    # 目标减持仓后向下取整
    assert buy.proposal.size == Decimal("0.00018")
    flat = build_delta_plan(
        _artifact(target=0.5), rule=RULE, reference_price=PRICE,
        budget_jpy=Decimal("5000"), position_size=Decimal("0.00020833"),
        no_trade_band=Decimal("0.01"),
    )
    assert flat.proposal is None and "不交易带" in str(flat.skip_reason)


def test_jpy_headroom_turns_buy_into_skip() -> None:
    """可用 JPY 不含手续费余量时买入改为零写跳过（R-06）。"""
    plan = build_delta_plan(
        _artifact(target=0.5), rule=RULE, reference_price=PRICE,
        budget_jpy=Decimal("5000"), position_size=Decimal("0"),
        no_trade_band=Decimal("0.01"),
    )
    assert plan.proposal is not None
    short = apply_jpy_headroom(plan, _assets(jpy="2000"))
    assert short.proposal is None and "可用 JPY" in str(short.skip_reason)
    assert apply_jpy_headroom(plan, _assets(jpy="2403")).proposal is not None


def test_sell_exempt_from_budget_gates_but_not_spread(tmp_path: Path) -> None:
    """减仓单豁免额度类门，点差与深度门仍生效。"""
    envelope = load_envelope(_write_envelope(tmp_path), whitelist=frozenset({BTC}))
    state = _cleared_state()
    base = dict(
        now=NOW, price_observed_at=NOW,
        used_total_jpy=Decimal("100000"), day_used_jpy=Decimal("100000"),
        day_order_count=48, current_value_jpy=Decimal("100000"),
        position_notional_jpy=Decimal("3000"), order_side=Side.SELL,
        order_notional_jpy=Decimal("50000"), spread_bp=Decimal("10"),
        opposite_depth_jpy=Decimal("1000000"), decision_time=NOW,
    )
    decision, _ = evaluate_envelope_gates(envelope, state, GateInputs(**base))
    assert decision.verdict == VERDICT_ALLOW
    exempt = {row.name for row in decision.gates if row.detail == "减仓豁免"}
    assert exempt == {
        "envelope_total", "day_budget", "order_max", "first_order_canary",
    }
    decision, _ = evaluate_envelope_gates(
        envelope, state, GateInputs(**dict(base, spread_bp=Decimal("999"))),
    )
    assert decision.verdict == VERDICT_SKIP
    decision, _ = evaluate_envelope_gates(
        envelope, state, GateInputs(**dict(base, order_side=Side.BUY)),
    )
    assert decision.verdict == VERDICT_HALT


class _FailingCancelSender(_Sender):
    """撤单请求抛网络错的替身。"""

    def cancel(self, order_id: int) -> None:
        self.events.append("cancel-failed")
        raise ApiNetworkError("/v1/cancelOrder", "模拟撤单超时")


def test_cancel_failure_is_recorded_not_raised(tmp_path: Path) -> None:
    """撤单异常不崩溃：留痕、计一次写异常，委托到终态仍算闭环。"""
    reader = _Reader([OrderStatus.ORDERED, OrderStatus.CANCELED])
    sender = _FailingCancelSender()
    runtime = _runtime(tmp_path, reader=reader, sender=sender)
    plan = build_plan(
        _artifact(), rule=RULE, reference_price=PRICE,
        budget_jpy=Decimal("500"),
    )
    exit_code, fragment = run_live_cycle(
        runtime, plan, assets=reader.assets(), price_observed_at=NOW,
        book=_book(), cancel_all=lambda: 0, now=NOW,
    )
    assert "撤单请求异常" in str(fragment["resolution"])
    assert fragment["final_order_status"] == "CANCELED"
    assert runtime.breaker.consecutive_failures == 1
    assert exit_code == EXIT_OK


class _LateReader(_Reader):
    """活动委托第二次查询才出现的替身。"""

    def __init__(self) -> None:
        super().__init__([OrderStatus.EXECUTED])
        self.active_calls = 0

    def active_orders(self, symbol, page=None, count=None):  # type: ignore[no-untyped-def]
        self.active_calls += 1
        if self.active_calls < 2:
            return ()
        return (_order(7100, OrderStatus.ORDERED),)


def test_send_timeout_waits_for_settlement_then_accepts(tmp_path: Path) -> None:
    """发送超时先等落账再查询：候选迟到一轮仍能判定为受理。"""
    reader = _LateReader()
    sender = _Sender(fail_network=True)
    runtime = _runtime(tmp_path, reader=reader, sender=sender)
    slept: list[float] = []
    runtime.sleep = slept.append
    plan = build_plan(
        _artifact(), rule=RULE, reference_price=PRICE,
        budget_jpy=Decimal("500"),
    )
    _exit_code, fragment = run_live_cycle(
        runtime, plan, assets=reader.assets(), price_observed_at=NOW,
        book=_book(), cancel_all=lambda: 0, now=NOW,
    )
    intent = fragment["intent"]
    assert intent["order_id"] == 7100
    assert runtime.ledger.state(intent["intent_id"]) is IntentState.ACCEPTED
    assert slept and slept[0] == 5.0


def test_usage_reconciled_from_ledger_and_breaker_seeded(tmp_path: Path) -> None:
    """用量文件缺行时从账本补记；熔断计数跨周期注入。"""
    ledger = IntentLedger(tmp_path / "ledger.jsonl")
    intent = _sending_intent(ledger, at=NOW - timedelta(minutes=1))
    ledger.accept(intent.intent_id, 9001, at=NOW)
    envelope = load_envelope(_write_envelope(tmp_path), whitelist=frozenset({BTC}))
    usage = EnvelopeUsage.for_envelope(envelope, directory=tmp_path)
    assert reconcile_usage_with_ledger(usage, ledger, now=NOW) == (intent.intent_id,)
    assert usage.total_jpy() == Decimal("0.00003") * PRICE
    assert reconcile_usage_with_ledger(usage, ledger, now=NOW) == ()
    breaker = CircuitBreaker(BreakerThresholds(
        schema_version=1, consecutive_failure_limit=3, stream_gap_seconds=90,
        asset_deviation_ratio=Decimal("0.01"),
        asset_deviation_floor_jpy=Decimal("100"),
    ))
    breaker.seed_failures(2)
    assert breaker.consecutive_failures == 2
    breaker.seed_failures(3)
    assert breaker.state is BreakerState.TRIPPED


def test_main_refuses_stale_observer_and_debug_overrides(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """观察进程心跳缺失即拒绝；live 下拒绝行情覆盖参数。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GUVOLU_MODE", "live")
    monkeypatch.setenv("GUVOLU_DATA_ROOT", str(tmp_path / "data"))
    envelope_path = _write_envelope(tmp_path)
    common = [
        "--target", str(tmp_path / "missing-target.json"),
        "--source-prediction", str(tmp_path / "missing-prediction.json"),
        "--source-prediction-sha256", "0" * 64,
        "--envelope", str(envelope_path),
    ]
    assert main([*common, "--reference-price", "1"], moment=NOW) == EXIT_REFUSED
    assert "debug_override_in_live" in capsys.readouterr().out
    assert main(common, moment=NOW) == EXIT_REFUSED
    assert "observer_stale" in capsys.readouterr().out
