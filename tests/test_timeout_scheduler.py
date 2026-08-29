"""超时自动查询调度单测：注入替身，绝不触发真实端点（C-13、C-14）。"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from guvolu.data.intent_ledger import IntentLedger
from guvolu.domain.enums import ExecutionType, Side
from guvolu.domain.errors import ApiTimeout
from guvolu.domain.intent import IntentState, OrderIntent
from guvolu.domain.models import Execution, Order
from guvolu.domain.symbols import SpotSymbol
from guvolu.execution.timeout_scheduler import (
    BackoffError,
    BackoffPolicy,
    TimeoutQueryScheduler,
)
from test_order_state import rest_order

MOMENT = datetime(2026, 8, 16, 0, 0, tzinfo=UTC)
POLICY = BackoffPolicy(initial_seconds=5.0, max_seconds=20.0)


class ScriptedReader:
    """只读替身：按脚本依次返回挂单集或抛出异常。"""

    def __init__(self, script: list[object]) -> None:
        self._script = script
        self.queries = 0

    def active_orders(
        self, symbol: str, page: int | None = None, count: int | None = None
    ) -> tuple[Order, ...]:
        self.queries += 1
        step = self._script.pop(0)
        if isinstance(step, Exception):
            raise step
        assert isinstance(step, tuple)
        return step

    def latest_executions(
        self, symbol: str, page: int | None = None, count: int | None = None
    ) -> tuple[Execution, ...]:
        return ()


def timed_out_ledger(tmp_path: Path, intent_id: str = "it01") -> IntentLedger:
    """构造一笔已处超时态的意图账本。"""
    ledger = IntentLedger(tmp_path / "intent_ledger.jsonl")
    intent = OrderIntent(
        intent_id=intent_id,
        correlation_id="co0001",
        symbol=SpotSymbol("BTC"),
        side=Side.BUY,
        execution_type=ExecutionType.LIMIT,
        size=Decimal("0.0002"),
        price=Decimal("1000000"),
        time_in_force=None,
        created_at=MOMENT,
    )
    ledger.record_intent(intent, at=MOMENT)
    ledger.begin_send(intent_id, at=MOMENT)
    ledger.mark_send_timeout(intent_id, reason="发送超时", at=MOMENT)
    return ledger


def test_backoff_policy_exponential_with_cap() -> None:
    """退避按倍率指数增长并封顶。"""
    assert POLICY.delay_seconds(1) == 5.0
    assert POLICY.delay_seconds(2) == 10.0
    assert POLICY.delay_seconds(3) == 20.0
    assert POLICY.delay_seconds(4) == 20.0
    with pytest.raises(BackoffError):
        POLICY.delay_seconds(0)


def test_backoff_policy_rejects_bad_values() -> None:
    """非法退避参数被拒。"""
    with pytest.raises(BackoffError):
        BackoffPolicy(initial_seconds=0, max_seconds=10)
    with pytest.raises(BackoffError):
        BackoffPolicy(initial_seconds=5, max_seconds=4)
    with pytest.raises(BackoffError):
        BackoffPolicy(initial_seconds=5, max_seconds=10, multiplier=0.5)


def test_entering_timeout_is_due_immediately(tmp_path: Path) -> None:
    """进入超时态即到期，首查在当轮执行（T-06）。"""
    ledger = timed_out_ledger(tmp_path)
    scheduler = TimeoutQueryScheduler(POLICY)
    entered = scheduler.sync(ledger, MOMENT)
    assert entered == ("it01",)
    assert scheduler.due_intents(MOMENT) == ("it01",)
    outcomes = scheduler.run_due(
        ledger=ledger, reader=ScriptedReader([()]), now=MOMENT
    )
    assert outcomes[0].disposition == "resolved"
    assert outcomes[0].state is IntentState.FAILED
    assert scheduler.pending() == ()


def test_ambiguity_backs_off_until_resolution(tmp_path: Path) -> None:
    """歧义按指数退避重查，直至恰一笔候选受理。"""
    ledger = timed_out_ledger(tmp_path)
    scheduler = TimeoutQueryScheduler(POLICY)
    ambiguous: tuple[Order, ...] = (
        rest_order(637001), rest_order(637002)
    )
    reader = ScriptedReader([ambiguous, ambiguous, (rest_order(637001),)])
    first = scheduler.run_due(ledger=ledger, reader=reader, now=MOMENT)
    assert first[0].disposition == "ambiguous"
    assert first[0].attempt == 1
    assert first[0].next_attempt_at == MOMENT + timedelta(seconds=5)
    # 未到期不重查
    early = scheduler.run_due(
        ledger=ledger, reader=reader, now=MOMENT + timedelta(seconds=4)
    )
    assert early == ()
    second = scheduler.run_due(
        ledger=ledger, reader=reader, now=MOMENT + timedelta(seconds=5)
    )
    assert second[0].attempt == 2
    assert second[0].next_attempt_at == MOMENT + timedelta(seconds=15)
    third = scheduler.run_due(
        ledger=ledger, reader=reader, now=MOMENT + timedelta(seconds=15)
    )
    assert third[0].disposition == "resolved"
    assert third[0].state is IntentState.ACCEPTED
    assert third[0].order_id == 637001
    assert ledger.state("it01") is IntentState.ACCEPTED
    assert scheduler.pending() == ()


def test_query_error_backs_off_and_keeps_timeout(tmp_path: Path) -> None:
    """查询自身失败同样退避，意图保持超时态占用在途。"""
    ledger = timed_out_ledger(tmp_path)
    scheduler = TimeoutQueryScheduler(POLICY)
    reader = ScriptedReader([ApiTimeout("/v1/activeOrders", "超时")])
    outcomes = scheduler.run_due(ledger=ledger, reader=reader, now=MOMENT)
    assert outcomes[0].disposition == "query_error"
    assert outcomes[0].next_attempt_at == MOMENT + timedelta(seconds=5)
    assert ledger.state("it01") is IntentState.SEND_TIMEOUT
    assert scheduler.pending() == ("it01",)


def test_externally_resolved_intent_dequeued(tmp_path: Path) -> None:
    """经他途（如 WS 通道）离开超时态的意图自动出队。"""
    ledger = timed_out_ledger(tmp_path)
    scheduler = TimeoutQueryScheduler(POLICY)
    scheduler.sync(ledger, MOMENT)
    ledger.accept(
        "it01", 637009, evidence={"source": "READ_ONLY"}, at=MOMENT
    )
    outcomes = scheduler.run_due(
        ledger=ledger, reader=ScriptedReader([]), now=MOMENT
    )
    assert outcomes == ()
    assert scheduler.pending() == ()
