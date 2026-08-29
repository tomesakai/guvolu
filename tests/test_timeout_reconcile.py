"""超时对账单测：只读替身注入，绝不触发真实端点（C-13、C-14）。"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from guvolu.data.intent_ledger import IntentLedger, LedgerError
from guvolu.domain.enums import (
    ExecutionType,
    OrderStatus,
    OrderType,
    SettleType,
    Side,
    TimeInForce,
)
from guvolu.domain.intent import IntentState, OrderIntent
from guvolu.domain.models import Execution, Order
from guvolu.domain.symbols import SpotSymbol
from guvolu.execution.reconcile import (
    ReconcileAmbiguity,
    resolve_send_timeout,
    send_timeout_intents,
)

MOMENT = datetime(2026, 8, 16, 0, 0, tzinfo=UTC)
LATER = MOMENT + timedelta(seconds=5)


class FakeReader:
    """只读查询替身，返回预置挂单与成交。"""

    def __init__(
        self,
        orders: tuple[Order, ...] = (),
        executions: tuple[Execution, ...] = (),
    ) -> None:
        self._orders = orders
        self._executions = executions
        self.queried_symbols: list[str] = []

    def active_orders(
        self, symbol: str, page: int | None = None, count: int | None = None
    ) -> tuple[Order, ...]:
        self.queried_symbols.append(symbol)
        return self._orders

    def latest_executions(
        self, symbol: str, page: int | None = None, count: int | None = None
    ) -> tuple[Execution, ...]:
        self.queried_symbols.append(symbol)
        return self._executions


def make_order(
    order_id: int,
    *,
    side: Side = Side.BUY,
    size: str = "0.0001",
    price: str | None = "1000000",
    timestamp: datetime = LATER,
) -> Order:
    """构造挂单一览返回的委托。"""
    return Order(
        root_order_id=order_id,
        order_id=order_id,
        symbol="BTC",
        side=side,
        order_type=OrderType.NORMAL,
        execution_type=ExecutionType.LIMIT,
        settle_type=SettleType.OPEN,
        size=Decimal(size),
        executed_size=Decimal("0"),
        price=None if price is None else Decimal(price),
        losscut_price=Decimal("0"),
        status=OrderStatus.ORDERED,
        cancel_type=None,
        time_in_force=TimeInForce.FAS,
        timestamp=timestamp,
    )


def make_execution(
    order_id: int,
    *,
    side: Side = Side.BUY,
    size: str = "0.0001",
    timestamp: datetime = LATER,
) -> Execution:
    """构造最新成交一览返回的成交。"""
    return Execution(
        execution_id=order_id * 10,
        order_id=order_id,
        position_id=None,
        symbol="BTC",
        side=side,
        settle_type=SettleType.OPEN,
        size=Decimal(size),
        price=Decimal("1000000"),
        loss_gain=Decimal("0"),
        fee=Decimal("0"),
        timestamp=timestamp,
    )


def timed_out_ledger(tmp_path: Path, intent_id: str = "it01") -> IntentLedger:
    """构造一笔已处超时态的意图账本。"""
    ledger = IntentLedger(tmp_path / "intent_ledger.jsonl")
    intent = OrderIntent(
        intent_id=intent_id,
        correlation_id="co0001",
        symbol=SpotSymbol("BTC"),
        side=Side.BUY,
        execution_type=ExecutionType.LIMIT,
        size=Decimal("0.0001"),
        price=Decimal("1000000"),
        time_in_force=None,
        created_at=MOMENT,
    )
    ledger.record_intent(intent, at=MOMENT)
    ledger.begin_send(intent_id, at=MOMENT)
    ledger.mark_send_timeout(intent_id, reason="发送超时", at=MOMENT)
    return ledger


def test_active_order_match_accepts(tmp_path: Path) -> None:
    """挂单恰一笔匹配即受理并映射委托号（T-06）。"""
    ledger = timed_out_ledger(tmp_path)
    reader = FakeReader(orders=(make_order(637000),))
    resolution = resolve_send_timeout(
        "it01", ledger=ledger, reader=reader, moment=LATER
    )
    assert resolution.state is IntentState.ACCEPTED
    assert resolution.order_id == 637000
    assert resolution.evidence["source"] == "READ_ONLY"
    assert resolution.evidence["matched_source"] == "activeOrders"
    assert "GET /v1/activeOrders" in resolution.evidence["endpoints"]
    assert ledger.state("it01") is IntentState.ACCEPTED
    assert ledger.intent_id_for_order(637000) == "it01"
    reloaded = IntentLedger(tmp_path / "intent_ledger.jsonl")
    assert reloaded.state("it01") is IntentState.ACCEPTED
    assert reader.queried_symbols == ["BTC", "BTC"]


def test_no_match_resolves_failed(tmp_path: Path) -> None:
    """零笔候选即判定未受理，证据入账（T-06）。"""
    ledger = timed_out_ledger(tmp_path)
    reader = FakeReader()
    resolution = resolve_send_timeout(
        "it01", ledger=ledger, reader=reader, moment=LATER
    )
    assert resolution.state is IntentState.FAILED
    assert resolution.order_id is None
    assert resolution.evidence["matched_order_id"] == ""
    assert ledger.state("it01") is IntentState.FAILED
    assert ledger.in_flight() == ()
    reloaded = IntentLedger(tmp_path / "intent_ledger.jsonl")
    assert reloaded.state("it01") is IntentState.FAILED


def test_execution_match_accepts(tmp_path: Path) -> None:
    """挂单缺席但成交匹配同样受理（U-01 区分委托与成交）。"""
    ledger = timed_out_ledger(tmp_path)
    reader = FakeReader(executions=(make_execution(637001),))
    resolution = resolve_send_timeout(
        "it01", ledger=ledger, reader=reader, moment=LATER
    )
    assert resolution.state is IntentState.ACCEPTED
    assert resolution.order_id == 637001
    assert resolution.evidence["matched_source"] == "latestExecutions"


def test_mismatched_candidates_ignored(tmp_path: Path) -> None:
    """方向、数量或价格不符的候选一律排除。"""
    ledger = timed_out_ledger(tmp_path)
    reader = FakeReader(
        orders=(
            make_order(637002, side=Side.SELL),
            make_order(637003, size="0.0002"),
            make_order(637004, price="999999"),
            make_order(
                637005, timestamp=MOMENT - timedelta(minutes=10)
            ),
        ),
        executions=(
            make_execution(637006, side=Side.SELL),
            make_execution(637007, size="0.0002"),
        ),
    )
    resolution = resolve_send_timeout(
        "it01", ledger=ledger, reader=reader, moment=LATER
    )
    assert resolution.state is IntentState.FAILED


def test_mapped_order_excluded(tmp_path: Path) -> None:
    """已映射到其他意图的委托号不参与匹配（T-05）。"""
    ledger = timed_out_ledger(tmp_path)
    other = OrderIntent(
        intent_id="it00",
        correlation_id="co0000",
        symbol=SpotSymbol("ETH"),
        side=Side.BUY,
        execution_type=ExecutionType.LIMIT,
        size=Decimal("0.001"),
        price=Decimal("500000"),
        time_in_force=None,
        created_at=MOMENT,
    )
    ledger.record_intent(other, at=MOMENT)
    ledger.begin_send("it00", at=MOMENT)
    ledger.accept("it00", 637008, at=MOMENT)
    reader = FakeReader(orders=(make_order(637008),))
    resolution = resolve_send_timeout(
        "it01", ledger=ledger, reader=reader, moment=LATER
    )
    assert resolution.state is IntentState.FAILED


def test_ambiguity_refuses_and_keeps_timeout(tmp_path: Path) -> None:
    """候选多于一笔即拒绝判定，超时态保持占用在途。"""
    ledger = timed_out_ledger(tmp_path)
    reader = FakeReader(
        orders=(make_order(637009), make_order(637010))
    )
    with pytest.raises(ReconcileAmbiguity):
        resolve_send_timeout(
            "it01", ledger=ledger, reader=reader, moment=LATER
        )
    assert ledger.state("it01") is IntentState.SEND_TIMEOUT
    assert ledger.in_flight(SpotSymbol("BTC")) == ("it01",)


def test_non_timeout_state_rejected(tmp_path: Path) -> None:
    """非超时态意图拒绝对账。"""
    ledger = IntentLedger(tmp_path / "intent_ledger.jsonl")
    intent = OrderIntent(
        intent_id="it02",
        correlation_id="co0002",
        symbol=SpotSymbol("BTC"),
        side=Side.BUY,
        execution_type=ExecutionType.LIMIT,
        size=Decimal("0.0001"),
        price=Decimal("1000000"),
        time_in_force=None,
        created_at=MOMENT,
    )
    ledger.record_intent(intent, at=MOMENT)
    with pytest.raises(LedgerError, match="超时态"):
        resolve_send_timeout(
            "it02", ledger=ledger, reader=FakeReader(), moment=LATER
        )


def test_send_timeout_intents_listing(tmp_path: Path) -> None:
    """超时意图清单只含超时态。"""
    ledger = timed_out_ledger(tmp_path)
    assert send_timeout_intents(ledger) == ("it01",)
    reader = FakeReader()
    resolve_send_timeout("it01", ledger=ledger, reader=reader, moment=LATER)
    assert send_timeout_intents(ledger) == ()
