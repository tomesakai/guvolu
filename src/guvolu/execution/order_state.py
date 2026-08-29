"""对账域委托状态：双通道 READ_ONLY 事实的累积视图（T-03、R-08）。

意图账本以 ACCEPTED 为账本视角终态，其后的委托生命周期（成交、
撤销、失效）属对账域，由本模块承载，不回写意图账本。状态来源
仅限 READ_ONLY 的两条通道：WS 私有事件与 REST 快照。本模块是
纯内存结构，不含 IO（C-02）；事实合并与通道仲裁由
execution.dual_reconcile 编排。持仓推算只依据成交事实（U-01），
绝不使用受理回执（T-03）。
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal

from guvolu.domain.enums import ExecutionType, OrderStatus, Side
from guvolu.domain.models import (
    Execution,
    Order,
    WsExecutionEvent,
    WsOrderEvent,
)

# 事实来源标识
WS_SOURCE = "READ_ONLY_WS"
REST_SOURCE = "READ_ONLY_REST"

# 仍在场内的委托状态集合
ACTIVE_ORDER_STATUSES: frozenset[OrderStatus] = frozenset(
    {
        OrderStatus.WAITING,
        OrderStatus.ORDERED,
        OrderStatus.MODIFYING,
        OrderStatus.CANCELLING,
    }
)


@dataclass(frozen=True, slots=True)
class OrderView:
    """单委托的对账域视图（U-01 委托，不混同成交）。"""

    order_id: int
    symbol: str
    side: Side
    execution_type: ExecutionType
    status: OrderStatus
    size: Decimal
    executed_size: Decimal
    price: Decimal | None
    timestamp: datetime
    source: str

    @property
    def is_active(self) -> bool:
        """委托是否仍在场内。"""
        return self.status in ACTIVE_ORDER_STATUSES


@dataclass(frozen=True, slots=True)
class ExecutionFact:
    """单笔成交事实（U-01 成交），持仓推算的唯一依据（T-03）。"""

    execution_id: int
    order_id: int
    symbol: str
    side: Side
    size: Decimal
    price: Decimal
    fee: Decimal
    timestamp: datetime
    source: str

    def signed_size(self) -> Decimal:
        """买为正、卖为负的数量。"""
        return self.size if self.side is Side.BUY else -self.size


def view_from_order(order: Order, source: str = REST_SOURCE) -> OrderView:
    """由 REST 委托模型构造视图。"""
    return OrderView(
        order_id=order.order_id,
        symbol=order.symbol,
        side=order.side,
        execution_type=order.execution_type,
        status=order.status,
        size=order.size,
        executed_size=order.executed_size,
        price=order.price,
        timestamp=order.timestamp,
        source=source,
    )


def view_from_order_event(event: WsOrderEvent) -> OrderView:
    """由 WS 委托事件构造视图。"""
    return OrderView(
        order_id=event.order_id,
        symbol=event.symbol,
        side=event.side,
        execution_type=event.execution_type,
        status=event.order_status,
        size=event.order_size,
        executed_size=event.order_executed_size,
        price=event.order_price,
        timestamp=event.order_timestamp,
        source=WS_SOURCE,
    )


def fact_from_execution(
    execution: Execution, source: str = REST_SOURCE
) -> ExecutionFact:
    """由 REST 成交模型构造事实。"""
    return ExecutionFact(
        execution_id=execution.execution_id,
        order_id=execution.order_id,
        symbol=execution.symbol,
        side=execution.side,
        size=execution.size,
        price=execution.price,
        fee=execution.fee,
        timestamp=execution.timestamp,
        source=source,
    )


def fact_from_execution_event(event: WsExecutionEvent) -> ExecutionFact:
    """由 WS 成交事件构造事实。"""
    return ExecutionFact(
        execution_id=event.execution_id,
        order_id=event.order_id,
        symbol=event.symbol,
        side=event.side,
        size=event.execution_size,
        price=event.execution_price,
        fee=event.fee,
        timestamp=event.execution_timestamp,
        source=WS_SOURCE,
    )


class OrderStateStore:
    """对账域状态存储，只接受 READ_ONLY 事实（T-03）。

    WS 事件按到达顺序应用并做保守单调守卫；REST 合并无条件
    覆写，体现不一致以 REST 为准（R-08）。成交事实按
    execution_id 去重（D-05），永不删除。
    """

    def __init__(self) -> None:
        self._orders: dict[int, OrderView] = {}
        self._executions: dict[int, ExecutionFact] = {}

    def order(self, order_id: int) -> OrderView | None:
        """取单委托视图，未知返回 None。"""
        return self._orders.get(order_id)

    def orders(self) -> tuple[OrderView, ...]:
        """按登记顺序列出全部委托视图。"""
        return tuple(self._orders.values())

    def active_orders(self, symbol: str | None = None) -> tuple[OrderView, ...]:
        """列出场内委托，可按品种过滤。"""
        return tuple(
            view
            for view in self._orders.values()
            if view.is_active and (symbol is None or view.symbol == symbol)
        )

    def executions(self) -> tuple[ExecutionFact, ...]:
        """按登记顺序列出全部成交事实。"""
        return tuple(self._executions.values())

    def execution_ids(self) -> frozenset[int]:
        """全部已登记成交号。"""
        return frozenset(self._executions)

    def put(self, view: OrderView) -> None:
        """无条件写入视图，供 REST 裁决覆写（T-03）。"""
        self._orders[view.order_id] = view

    def apply_order_event(self, event: WsOrderEvent) -> OrderView:
        """应用 WS 委托事件，带单调守卫防乱序回退。"""
        view = view_from_order_event(event)
        existing = self._orders.get(view.order_id)
        if existing is not None:
            if existing.executed_size > view.executed_size:
                # 乱序帧不回退已成量
                view = replace(view, executed_size=existing.executed_size)
            if not existing.is_active and view.is_active:
                # 终态不被乱序帧复活
                view = replace(view, status=existing.status)
        self._orders[view.order_id] = view
        return view

    def apply_execution_event(self, event: WsExecutionEvent) -> ExecutionFact:
        """应用 WS 成交事件：登记事实并推进委托视图。"""
        fact = fact_from_execution_event(event)
        self._executions.setdefault(fact.execution_id, fact)
        existing = self._orders.get(event.order_id)
        executed = event.order_executed_size
        if existing is not None and existing.executed_size > executed:
            executed = existing.executed_size
        if executed >= event.order_size:
            status = OrderStatus.EXECUTED
        elif existing is not None and not existing.is_active:
            status = existing.status
        else:
            status = OrderStatus.ORDERED
        self._orders[event.order_id] = OrderView(
            order_id=event.order_id,
            symbol=event.symbol,
            side=event.side,
            execution_type=event.execution_type,
            status=status,
            size=event.order_size,
            executed_size=executed,
            price=event.order_price,
            timestamp=event.execution_timestamp,
            source=WS_SOURCE,
        )
        return fact

    def merge_rest_order(self, order: Order) -> OrderView:
        """合并 REST 委托，无条件覆写（REST 为准，R-08）。"""
        view = view_from_order(order)
        self._orders[view.order_id] = view
        return view

    def merge_rest_execution(self, execution: Execution) -> bool:
        """合并 REST 成交事实，新事实返回真。"""
        if execution.execution_id in self._executions:
            return False
        self._executions[execution.execution_id] = fact_from_execution(
            execution
        )
        return True

    def position_size(
        self, symbol: str, belongs: Callable[[int], bool]
    ) -> Decimal:
        """由成交事实推算持仓，买加卖减（T-03）。

        belongs 判定委托是否属本系统（意图账本已映射），
        受理回执不参与推算。
        """
        total = Decimal("0")
        for fact in self._executions.values():
            if fact.symbol == symbol and belongs(fact.order_id):
                total += fact.signed_size()
        return total
