"""对账域状态存储单测：纯内存，无任何网络（C-13、C-14）。"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from guvolu.domain.enums import (
    ExecutionType,
    OrderStatus,
    OrderType,
    SettleType,
    Side,
    TimeInForce,
)
from guvolu.domain.models import Execution, Order, WsExecutionEvent, WsOrderEvent
from guvolu.execution.order_state import (
    REST_SOURCE,
    WS_SOURCE,
    OrderStateStore,
)

MOMENT = datetime(2026, 8, 16, 0, 0, tzinfo=UTC)
LATER = MOMENT + timedelta(seconds=5)


def order_event(
    order_id: int,
    *,
    status: OrderStatus = OrderStatus.ORDERED,
    size: str = "0.0002",
    executed: str = "0",
    price: str | None = "1000000",
    side: Side = Side.BUY,
    msg_type: str = "NOR",
    timestamp: datetime = MOMENT,
) -> WsOrderEvent:
    """构造委托事件。"""
    return WsOrderEvent(
        order_id=order_id,
        symbol="BTC",
        settle_type=SettleType.OPEN,
        execution_type=ExecutionType.LIMIT,
        side=side,
        order_status=status,
        cancel_type=None,
        order_timestamp=timestamp,
        order_price=None if price is None else Decimal(price),
        order_size=Decimal(size),
        order_executed_size=Decimal(executed),
        losscut_price=Decimal("0"),
        time_in_force=TimeInForce.FAS,
        msg_type=msg_type,
    )


def execution_event(
    order_id: int,
    execution_id: int,
    *,
    size: str = "0.0001",
    order_size: str = "0.0002",
    order_executed: str = "0.0001",
    side: Side = Side.BUY,
    price: str = "1000000",
    fee: str = "0",
    timestamp: datetime = LATER,
) -> WsExecutionEvent:
    """构造成交事件。"""
    return WsExecutionEvent(
        order_id=order_id,
        execution_id=execution_id,
        symbol="BTC",
        settle_type=SettleType.OPEN,
        execution_type=ExecutionType.LIMIT,
        side=side,
        execution_price=Decimal(price),
        execution_size=Decimal(size),
        position_id=None,
        order_timestamp=MOMENT,
        execution_timestamp=timestamp,
        loss_gain=Decimal("0"),
        fee=Decimal(fee),
        order_price=Decimal("1000000"),
        order_size=Decimal(order_size),
        order_executed_size=Decimal(order_executed),
        time_in_force=TimeInForce.FAS,
        msg_type="ER",
    )


def rest_order(
    order_id: int,
    *,
    status: OrderStatus = OrderStatus.ORDERED,
    size: str = "0.0002",
    executed: str = "0",
    side: Side = Side.BUY,
) -> Order:
    """构造 REST 委托。"""
    return Order(
        root_order_id=order_id,
        order_id=order_id,
        symbol="BTC",
        side=side,
        order_type=OrderType.NORMAL,
        execution_type=ExecutionType.LIMIT,
        settle_type=SettleType.OPEN,
        size=Decimal(size),
        executed_size=Decimal(executed),
        price=Decimal("1000000"),
        losscut_price=Decimal("0"),
        status=status,
        cancel_type=None,
        time_in_force=TimeInForce.FAS,
        timestamp=MOMENT,
    )


def rest_execution(
    order_id: int,
    execution_id: int,
    *,
    size: str = "0.0001",
    side: Side = Side.BUY,
) -> Execution:
    """构造 REST 成交。"""
    return Execution(
        execution_id=execution_id,
        order_id=order_id,
        position_id=None,
        symbol="BTC",
        side=side,
        settle_type=SettleType.OPEN,
        size=Decimal(size),
        price=Decimal("1000000"),
        loss_gain=Decimal("0"),
        fee=Decimal("0"),
        timestamp=LATER,
    )


def test_order_event_creates_active_view() -> None:
    """委托事件建立场内视图，来源标注 WS 通道。"""
    store = OrderStateStore()
    view = store.apply_order_event(order_event(637001))
    assert view.is_active
    assert view.source == WS_SOURCE
    assert store.order(637001) is view
    assert store.active_orders("BTC") == (view,)
    assert store.active_orders("ETH") == ()


def test_execution_event_records_fact_and_advances_view() -> None:
    """成交事件登记事实并推进已成量（U-01 两概念分离）。"""
    store = OrderStateStore()
    store.apply_order_event(order_event(637001))
    store.apply_execution_event(execution_event(637001, 900001))
    view = store.order(637001)
    assert view is not None
    assert view.executed_size == Decimal("0.0001")
    assert view.status is OrderStatus.ORDERED
    assert store.execution_ids() == frozenset({900001})


def test_full_fill_marks_executed() -> None:
    """累计成交达委托量即转 EXECUTED。"""
    store = OrderStateStore()
    store.apply_order_event(order_event(637001))
    store.apply_execution_event(
        execution_event(
            637001, 900002, size="0.0002", order_executed="0.0002"
        )
    )
    view = store.order(637001)
    assert view is not None
    assert view.status is OrderStatus.EXECUTED
    assert not view.is_active


def test_out_of_order_frames_do_not_regress() -> None:
    """乱序帧不回退已成量，终态不被复活。"""
    store = OrderStateStore()
    store.apply_order_event(
        order_event(637001, status=OrderStatus.CANCELED, executed="0.0001")
    )
    view = store.apply_order_event(order_event(637001, executed="0"))
    assert view.executed_size == Decimal("0.0001")
    assert view.status is OrderStatus.CANCELED


def test_duplicate_execution_fact_ignored() -> None:
    """成交号去重（D-05）。"""
    store = OrderStateStore()
    store.apply_execution_event(execution_event(637001, 900001))
    store.apply_execution_event(execution_event(637001, 900001))
    assert len(store.executions()) == 1
    assert store.merge_rest_execution(rest_execution(637001, 900001)) is False
    assert store.merge_rest_execution(rest_execution(637001, 900003)) is True


def test_rest_merge_overwrites_view() -> None:
    """REST 合并无条件覆写（R-08 REST 为准）。"""
    store = OrderStateStore()
    store.apply_order_event(order_event(637001, executed="0.0002"))
    view = store.merge_rest_order(
        rest_order(637001, status=OrderStatus.CANCELED, executed="0.0001")
    )
    assert view.status is OrderStatus.CANCELED
    assert view.executed_size == Decimal("0.0001")
    assert view.source == REST_SOURCE


def test_position_only_from_owned_executions() -> None:
    """持仓推算只计入账本映射委托的成交，买加卖减（T-03）。"""
    store = OrderStateStore()
    store.apply_execution_event(
        execution_event(637001, 900001, size="0.0003")
    )
    store.apply_execution_event(
        execution_event(637002, 900002, size="0.0001", side=Side.SELL)
    )
    store.apply_execution_event(
        execution_event(637999, 900003, size="0.005")
    )
    owned = {637001, 637002}
    position = store.position_size("BTC", lambda oid: oid in owned)
    assert position == Decimal("0.0002")
    assert store.position_size("ETH", lambda oid: True) == Decimal("0")
