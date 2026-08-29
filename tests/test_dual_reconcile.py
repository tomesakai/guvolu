"""双通道对账单测：注入事件与快照，绝不触发真实端点（C-13、C-14）。"""
from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from guvolu.data.intent_ledger import IntentLedger
from guvolu.domain.enums import ExecutionType, OrderStatus, Side
from guvolu.domain.intent import IntentState, OrderIntent
from guvolu.domain.models import Asset, Execution, Order, WsPositionSummaryEvent
from guvolu.domain.symbols import SpotSymbol
from guvolu.execution.dual_reconcile import (
    AssetReconcileError,
    AssetReconciliation,
    RestSnapshot,
    SnapshotMode,
    apply_private_event,
    reconcile_snapshot,
    take_snapshot,
)
from guvolu.execution.order_state import OrderStateStore, fact_from_execution
from guvolu.risk.circuit_breaker import (
    BreakerState,
    BreakerThresholds,
    CircuitBreaker,
)
from test_order_state import (
    execution_event,
    order_event,
    rest_execution,
    rest_order,
)

MOMENT = datetime(2026, 8, 16, 0, 0, tzinfo=UTC)
LATER = MOMENT + timedelta(seconds=5)
THRESHOLDS = BreakerThresholds(
    schema_version=1,
    consecutive_failure_limit=3,
    stream_gap_seconds=90,
    asset_deviation_ratio=Decimal("0.01"),
    asset_deviation_floor_jpy=Decimal("30"),
)


class FakeSnapshotReader:
    """只读快照替身，返回预置数据并记录调用。"""

    def __init__(
        self,
        orders_active: tuple[Order, ...] = (),
        executions_latest: tuple[Execution, ...] = (),
        assets_rows: tuple[Asset, ...] = (),
        lookup: tuple[Order, ...] = (),
    ) -> None:
        self._orders_active = orders_active
        self._executions_latest = executions_latest
        self._assets_rows = assets_rows
        self._lookup = lookup
        self.calls: list[str] = []
        self.lookup_ids: list[tuple[int, ...]] = []

    def active_orders(
        self, symbol: str, page: int | None = None, count: int | None = None
    ) -> tuple[Order, ...]:
        self.calls.append("activeOrders")
        return self._orders_active

    def latest_executions(
        self, symbol: str, page: int | None = None, count: int | None = None
    ) -> tuple[Execution, ...]:
        self.calls.append("latestExecutions")
        return self._executions_latest

    def orders(self, order_ids: Sequence[int]) -> tuple[Order, ...]:
        self.calls.append("orders")
        self.lookup_ids.append(tuple(order_ids))
        return tuple(
            order for order in self._lookup if order.order_id in order_ids
        )

    def executions(
        self,
        order_id: int | None = None,
        execution_ids: Sequence[int] | None = None,
    ) -> tuple[Execution, ...]:
        self.calls.append("executions")
        return ()

    def assets(self) -> tuple[Asset, ...]:
        self.calls.append("assets")
        return self._assets_rows


def make_asset(symbol: str, amount: str, rate: str = "1") -> Asset:
    """构造資産残高一条。"""
    return Asset(
        amount=Decimal(amount),
        available=Decimal(amount),
        conversion_rate=Decimal(rate),
        symbol=symbol,
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
        size=Decimal("0.0002"),
        price=Decimal("1000000"),
        time_in_force=None,
        created_at=MOMENT,
    )
    ledger.record_intent(intent, at=MOMENT)
    ledger.begin_send(intent_id, at=MOMENT)
    ledger.mark_send_timeout(intent_id, reason="发送超时", at=MOMENT)
    return ledger


def snapshot(
    orders_active: tuple[Order, ...] = (),
    executions_latest: tuple[Execution, ...] = (),
    assets_rows: tuple[Asset, ...] = (),
) -> RestSnapshot:
    """构造 REST 快照。"""
    return RestSnapshot(
        symbol="BTC",
        taken_at=LATER,
        active_orders=orders_active,
        latest_executions=executions_latest,
        assets=assets_rows,
    )


def test_ws_order_event_accepts_timeout_intent(tmp_path: Path) -> None:
    """匹配的委托事件即 READ_ONLY 证据，受理超时意图（T-06）。"""
    ledger = timed_out_ledger(tmp_path)
    store = OrderStateStore()
    outcome = apply_private_event(
        order_event(637001), store=store, ledger=ledger, moment=LATER
    )
    assert outcome.kind == "order"
    assert outcome.accepted_intent_id == "it01"
    assert ledger.state("it01") is IntentState.ACCEPTED
    assert ledger.intent_id_for_order(637001) == "it01"
    reloaded = IntentLedger(tmp_path / "intent_ledger.jsonl")
    assert reloaded.state("it01") is IntentState.ACCEPTED


def test_ws_execution_event_accepts_partial_fill(tmp_path: Path) -> None:
    """部分成交事件同样构成受理证据，累计量不超过意图。"""
    ledger = timed_out_ledger(tmp_path)
    store = OrderStateStore()
    outcome = apply_private_event(
        execution_event(637001, 900001), store=store, ledger=ledger,
        moment=LATER,
    )
    assert outcome.accepted_intent_id == "it01"
    assert ledger.state("it01") is IntentState.ACCEPTED
    assert store.execution_ids() == frozenset({900001})


def test_ws_mismatched_event_does_not_accept(tmp_path: Path) -> None:
    """方向、数量或类型不符的事件不驱动受理。"""
    ledger = timed_out_ledger(tmp_path)
    store = OrderStateStore()
    for event in (
        order_event(637002, side=Side.SELL),
        order_event(637003, size="0.0009"),
        order_event(637004, price="999999"),
    ):
        outcome = apply_private_event(
            event, store=store, ledger=ledger, moment=LATER
        )
        assert outcome.accepted_intent_id is None
    assert ledger.state("it01") is IntentState.SEND_TIMEOUT


def test_ws_mapped_order_only_updates_store(tmp_path: Path) -> None:
    """已映射委托的事件只推进对账域，不回写意图账本。"""
    ledger = timed_out_ledger(tmp_path)
    store = OrderStateStore()
    apply_private_event(
        order_event(637001), store=store, ledger=ledger, moment=LATER
    )
    follow = apply_private_event(
        order_event(637001, status=OrderStatus.CANCELED, msg_type="COR"),
        store=store,
        ledger=ledger,
        moment=LATER,
    )
    assert follow.accepted_intent_id is None
    assert ledger.state("it01") is IntentState.ACCEPTED
    view = store.order(637001)
    assert view is not None
    assert view.status is OrderStatus.CANCELED


def test_position_summary_event_ignored(tmp_path: Path) -> None:
    """持仓汇总事件属杠杆域，现物执行链忽略（T-09）。"""
    ledger = timed_out_ledger(tmp_path)
    store = OrderStateStore()
    event = WsPositionSummaryEvent(
        symbol="BTC_JPY",
        side=Side.BUY,
        average_position_rate=Decimal("1"),
        position_loss_gain=Decimal("0"),
        sum_order_quantity=Decimal("0"),
        sum_position_quantity=Decimal("1"),
        timestamp=LATER,
        msg_type="INIT",
    )
    outcome = apply_private_event(
        event, store=store, ledger=ledger, moment=LATER
    )
    assert outcome.kind == "ignored"
    assert store.orders() == ()


def test_audit_counts_mismatch_and_rest_wins(tmp_path: Path) -> None:
    """稳态快照不一致计入熔断计数且 REST 为准（R-08、T-03）。"""
    ledger = IntentLedger(tmp_path / "intent_ledger.jsonl")
    breaker = CircuitBreaker(THRESHOLDS)
    store = OrderStateStore()
    store.apply_order_event(order_event(637001, executed="0"))
    reader = FakeSnapshotReader()
    result = reconcile_snapshot(
        snapshot(
            orders_active=(
                rest_order(637001, executed="0.0001"),
                rest_order(637002),
            )
        ),
        store=store,
        ledger=ledger,
        breaker=breaker,
        reader=reader,
        mode=SnapshotMode.AUDIT,
    )
    kinds = sorted(item.kind for item in result.mismatches)
    assert kinds == ["executed_size_mismatch", "ws_missing_order"]
    assert result.counted_into_breaker == 2
    assert breaker.consecutive_failures == 2
    assert breaker.state is BreakerState.NORMAL
    view = store.order(637001)
    assert view is not None
    assert view.executed_size == Decimal("0.0001")
    assert store.order(637002) is not None


def test_audit_stale_active_resolved_via_order_lookup(
    tmp_path: Path,
) -> None:
    """WS 在场而 REST 未列时按委托号查询终态覆写（T-03）。"""
    ledger = IntentLedger(tmp_path / "intent_ledger.jsonl")
    breaker = CircuitBreaker(THRESHOLDS)
    store = OrderStateStore()
    store.apply_order_event(order_event(637001))
    reader = FakeSnapshotReader(
        lookup=(rest_order(637001, status=OrderStatus.CANCELED),)
    )
    result = reconcile_snapshot(
        snapshot(),
        store=store,
        ledger=ledger,
        breaker=breaker,
        reader=reader,
        mode=SnapshotMode.AUDIT,
    )
    assert [item.kind for item in result.mismatches] == [
        "stale_active_order"
    ]
    assert result.order_lookup_used
    assert reader.lookup_ids == [(637001,)]
    assert breaker.consecutive_failures == 1
    view = store.order(637001)
    assert view is not None
    assert view.status is OrderStatus.CANCELED


def test_lookup_without_result_marks_expired(tmp_path: Path) -> None:
    """委托号查询无果时按失效登记，不留悬置场内视图。"""
    ledger = IntentLedger(tmp_path / "intent_ledger.jsonl")
    breaker = CircuitBreaker(THRESHOLDS)
    store = OrderStateStore()
    store.apply_order_event(order_event(637001))
    result = reconcile_snapshot(
        snapshot(),
        store=store,
        ledger=ledger,
        breaker=breaker,
        reader=FakeSnapshotReader(),
        mode=SnapshotMode.AUDIT,
    )
    assert result.order_lookup_used
    view = store.order(637001)
    assert view is not None
    assert view.status is OrderStatus.EXPIRED
    assert not view.is_active


def test_baseline_and_realign_do_not_count(tmp_path: Path) -> None:
    """基线与重连补齐只对齐不计数（C-10）。"""
    for mode in (SnapshotMode.BASELINE, SnapshotMode.REALIGN):
        ledger = IntentLedger(tmp_path / f"{mode.value}.jsonl")
        breaker = CircuitBreaker(THRESHOLDS)
        store = OrderStateStore()
        store.apply_order_event(order_event(637001, executed="0"))
        result = reconcile_snapshot(
            snapshot(
                orders_active=(rest_order(637001, executed="0.0001"),)
            ),
            store=store,
            ledger=ledger,
            breaker=breaker,
            reader=FakeSnapshotReader(),
            mode=mode,
        )
        assert result.mismatches != ()
        assert result.counted_into_breaker == 0
        assert breaker.consecutive_failures == 0
        view = store.order(637001)
        assert view is not None
        assert view.executed_size == Decimal("0.0001")


def test_snapshot_merges_new_executions(tmp_path: Path) -> None:
    """快照并入新成交事实并去重。"""
    ledger = IntentLedger(tmp_path / "intent_ledger.jsonl")
    store = OrderStateStore()
    store.apply_execution_event(execution_event(637001, 900001))
    result = reconcile_snapshot(
        snapshot(
            executions_latest=(
                rest_execution(637001, 900001),
                rest_execution(637001, 900002),
            )
        ),
        store=store,
        ledger=ledger,
        breaker=CircuitBreaker(THRESHOLDS),
        reader=FakeSnapshotReader(),
        mode=SnapshotMode.BASELINE,
    )
    assert result.new_execution_count == 1
    assert store.execution_ids() == frozenset({900001, 900002})


def test_take_snapshot_touches_three_endpoints() -> None:
    """快照拉取覆盖挂单、最新成交与资产三端点。"""
    reader = FakeSnapshotReader(assets_rows=(make_asset("JPY", "3009"),))
    result = take_snapshot(reader, "BTC", LATER)
    assert result.symbol == "BTC"
    assert reader.calls == ["activeOrders", "latestExecutions", "assets"]
    assert result.assets[0].amount == Decimal("3009")


def test_asset_reconciliation_explains_owned_executions() -> None:
    """账本内成交与手续费解释资产变动，差额为零。"""
    check = AssetReconciliation()
    check.initialize(
        (make_asset("JPY", "3009"), make_asset("BTC", "0")), ()
    )
    fact = fact_from_execution(rest_execution(637001, 900001))
    assert check.explain(fact) is True
    assert check.explain(fact) is False
    unexplained, total = check.evaluate(
        (
            make_asset("JPY", "2909"),
            make_asset("BTC", "0.0001", rate="1000000"),
        )
    )
    assert unexplained == Decimal("0")
    assert total == Decimal("3009")


def test_asset_reconciliation_flags_unexplained_gap() -> None:
    """无法解释的差额按快照汇率折 JPY 报告（TBD-10 口径）。"""
    check = AssetReconciliation()
    check.initialize((make_asset("JPY", "3009"),), ())
    unexplained, total = check.evaluate((make_asset("JPY", "2959"),))
    assert unexplained == Decimal("50")
    assert total == Decimal("2959")


def test_asset_reconciliation_guards_order_of_use() -> None:
    """基线未建立不得评估，重复建立被拒。"""
    check = AssetReconciliation()
    with pytest.raises(AssetReconcileError):
        check.evaluate(())
    check.initialize((), ())
    with pytest.raises(AssetReconcileError):
        check.initialize((), ())
