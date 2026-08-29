"""双通道对账：WS 事实应用与 REST 快照裁决（R-08、C-10、T-03）。

WS 通道消费 api.ws_private 解析出的私有事件，累积对账域状态并
驱动超时意图受理；REST 通道定时拉取全量快照（挂单、最新成交、
资产），与 WS 累积状态比对。两通道不一致时以 REST 为准（T-03）
并逐项计入熔断计数（R-08）。三种快照模式：基线（首轮建立比对
起点）、重连补齐（C-10 修复已知增量缺口）只对齐不计数；稳态
周期快照按 R-08 计数。资产异动核对口径见 TBD-10：无法由账本内
委托、成交与手续费解释的資産残高 amount 合计差额。
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Protocol

from guvolu.data.intent_ledger import IntentLedger
from guvolu.domain.enums import ExecutionType, OrderStatus, Side
from guvolu.domain.errors import GuvoluError
from guvolu.domain.intent import IntentState, OrderIntent
from guvolu.domain.models import (
    Asset,
    Execution,
    Order,
    WsExecutionEvent,
    WsOrderEvent,
    WsPositionEvent,
    WsPositionSummaryEvent,
)
from guvolu.execution.order_state import (
    REST_SOURCE,
    ExecutionFact,
    OrderStateStore,
    OrderView,
)
from guvolu.execution.reconcile import CLOCK_TOLERANCE
from guvolu.risk.circuit_breaker import CircuitBreaker

# 快照必触的只读端点（A-03）
SNAPSHOT_READ_ENDPOINTS = (
    "GET /v1/activeOrders",
    "GET /v1/latestExecutions",
    "GET /v1/account/assets",
)
# 歧义裁决另触的只读端点
ORDER_LOOKUP_ENDPOINT = "GET /v1/orders"
# 单次委托查询的批量上限
_ORDER_LOOKUP_BATCH = 10
# 日本円资产符号
_JPY = "JPY"

PrivateEvent = (
    WsOrderEvent | WsExecutionEvent | WsPositionEvent | WsPositionSummaryEvent
)


class ReadOnlySnapshotReader(Protocol):
    """快照对账所需的只读抽象，ReadClient 满足本形态（T-02）。"""

    def active_orders(
        self, symbol: str, page: int | None = None, count: int | None = None
    ) -> tuple[Order, ...]: ...

    def latest_executions(
        self, symbol: str, page: int | None = None, count: int | None = None
    ) -> tuple[Execution, ...]: ...

    def orders(self, order_ids: Sequence[int]) -> tuple[Order, ...]: ...

    def executions(
        self,
        order_id: int | None = None,
        execution_ids: Sequence[int] | None = None,
    ) -> tuple[Execution, ...]: ...

    def assets(self) -> tuple[Asset, ...]: ...


class SnapshotMode(StrEnum):
    """快照对账模式。"""

    BASELINE = "baseline"
    REALIGN = "realign"
    AUDIT = "audit"


@dataclass(frozen=True, slots=True)
class RestSnapshot:
    """一次 REST 全量快照。"""

    symbol: str
    taken_at: datetime
    active_orders: tuple[Order, ...]
    latest_executions: tuple[Execution, ...]
    assets: tuple[Asset, ...]


def take_snapshot(
    reader: ReadOnlySnapshotReader,
    symbol: str,
    moment: datetime | None = None,
) -> RestSnapshot:
    """经 READ_ONLY 拉取一次全量快照（T-03）。"""
    now = moment if moment is not None else datetime.now(UTC)
    return RestSnapshot(
        symbol=symbol,
        taken_at=now,
        active_orders=reader.active_orders(symbol),
        latest_executions=reader.latest_executions(symbol),
        assets=reader.assets(),
    )


@dataclass(frozen=True, slots=True)
class WsApplyOutcome:
    """一次 WS 事件应用的结果。"""

    kind: str
    order_id: int | None
    accepted_intent_id: str | None


def _timeout_intent_matching(
    ledger: IntentLedger,
    *,
    symbol: str,
    side: Side,
    execution_type: ExecutionType,
    size: Decimal,
    price: Decimal | None,
    observed_at: datetime,
    partial: bool,
) -> str | None:
    """找与事件相符的超时意图；同品种单在途保证至多一笔（T-05）。

    partial 为真表示 size 是累计成交量，只需不超过意图数量。
    """
    for intent_id in ledger.intent_ids():
        if ledger.state(intent_id) is not IntentState.SEND_TIMEOUT:
            continue
        intent: OrderIntent = ledger.intent(intent_id)
        if str(intent.symbol) != symbol or intent.side is not side:
            continue
        if intent.execution_type is not execution_type:
            continue
        if partial:
            if size > intent.size:
                continue
        else:
            if size != intent.size:
                continue
            if intent.price is not None and price != intent.price:
                continue
        if observed_at < intent.created_at - CLOCK_TOLERANCE:
            continue
        return intent_id
    return None


def apply_private_event(
    event: PrivateEvent,
    *,
    store: OrderStateStore,
    ledger: IntentLedger,
    moment: datetime | None = None,
) -> WsApplyOutcome:
    """应用一条私有事件：更新对账域并驱动意图受理（T-06）。

    未映射委托的事件与恰一笔同品种超时意图相符时，事件本身即
    READ_ONLY 证据，受理并登记映射；已映射委托只推进对账域
    视图，不回写意图账本（账本以 ACCEPTED 为终态）。持仓类
    事件属杠杆域，现物执行链忽略（T-09）。
    """
    now = moment if moment is not None else datetime.now(UTC)
    if isinstance(event, WsOrderEvent):
        view = store.apply_order_event(event)
        accepted = _accept_from_ws(
            ledger,
            order_id=event.order_id,
            symbol=event.symbol,
            side=event.side,
            execution_type=event.execution_type,
            size=event.order_size,
            price=event.order_price,
            observed_at=event.order_timestamp,
            partial=False,
            channel="orderEvents",
            msg_type=event.msg_type,
            now=now,
        )
        return WsApplyOutcome("order", view.order_id, accepted)
    if isinstance(event, WsExecutionEvent):
        fact = store.apply_execution_event(event)
        accepted = _accept_from_ws(
            ledger,
            order_id=event.order_id,
            symbol=event.symbol,
            side=event.side,
            execution_type=event.execution_type,
            size=event.order_executed_size,
            price=event.order_price,
            observed_at=event.execution_timestamp,
            partial=True,
            channel="executionEvents",
            msg_type=event.msg_type,
            now=now,
        )
        return WsApplyOutcome("execution", fact.order_id, accepted)
    return WsApplyOutcome("ignored", None, None)


def _accept_from_ws(
    ledger: IntentLedger,
    *,
    order_id: int,
    symbol: str,
    side: Side,
    execution_type: ExecutionType,
    size: Decimal,
    price: Decimal | None,
    observed_at: datetime,
    partial: bool,
    channel: str,
    msg_type: str,
    now: datetime,
) -> str | None:
    """未映射委托匹配超时意图即受理，证据入账（T-06）。"""
    if ledger.intent_id_for_order(order_id) is not None:
        return None
    intent_id = _timeout_intent_matching(
        ledger,
        symbol=symbol,
        side=side,
        execution_type=execution_type,
        size=size,
        price=price,
        observed_at=observed_at,
        partial=partial,
    )
    if intent_id is None:
        return None
    evidence: Mapping[str, str] = {
        "source": "READ_ONLY",
        "channel": channel,
        "msg_type": msg_type,
        "matched_order_id": str(order_id),
        "observed_at": observed_at.isoformat(),
        "queried_at": now.isoformat(),
    }
    ledger.accept(intent_id, order_id, evidence=evidence, at=now)
    return intent_id


@dataclass(frozen=True, slots=True)
class Mismatch:
    """一处双通道不一致（R-08）。"""

    kind: str
    order_id: int
    detail: str


@dataclass(frozen=True, slots=True)
class SnapshotReconcileResult:
    """一次快照对账的结果。"""

    mode: SnapshotMode
    mismatches: tuple[Mismatch, ...]
    counted_into_breaker: int
    new_execution_count: int
    order_lookup_used: bool


def _detect_mismatches(
    store: OrderStateStore, snapshot: RestSnapshot
) -> tuple[Mismatch, ...]:
    """比对 WS 累积状态与 REST 快照，列出不一致。"""
    found: list[Mismatch] = []
    rest_active_ids = {order.order_id for order in snapshot.active_orders}
    for order in snapshot.active_orders:
        view = store.order(order.order_id)
        if view is None:
            found.append(
                Mismatch(
                    "ws_missing_order",
                    order.order_id,
                    "REST 场内委托未出现在 WS 累积状态",
                )
            )
            continue
        if view.status is not order.status:
            found.append(
                Mismatch(
                    "status_mismatch",
                    order.order_id,
                    f"WS {view.status.value} 对 REST {order.status.value}",
                )
            )
        if view.executed_size != order.executed_size:
            found.append(
                Mismatch(
                    "executed_size_mismatch",
                    order.order_id,
                    f"WS {view.executed_size} 对 REST {order.executed_size}",
                )
            )
    for view in store.active_orders(snapshot.symbol):
        if view.order_id not in rest_active_ids:
            found.append(
                Mismatch(
                    "stale_active_order",
                    view.order_id,
                    "WS 视图在场而 REST 快照未列",
                )
            )
    return tuple(found)


def _resolve_stale_orders(
    store: OrderStateStore,
    reader: ReadOnlySnapshotReader,
    order_ids: Sequence[int],
) -> bool:
    """按委托号查询终态并覆写视图（REST 为准，T-03）。"""
    if not order_ids:
        return False
    remaining = set(order_ids)
    for start in range(0, len(order_ids), _ORDER_LOOKUP_BATCH):
        batch = order_ids[start : start + _ORDER_LOOKUP_BATCH]
        for order in reader.orders(batch):
            store.merge_rest_order(order)
            remaining.discard(order.order_id)
    for order_id in sorted(remaining):
        view = store.order(order_id)
        if view is None:
            continue
        # 查询无果按失效登记，明细在不一致表
        store.put(
            OrderView(
                order_id=view.order_id,
                symbol=view.symbol,
                side=view.side,
                execution_type=view.execution_type,
                status=OrderStatus.EXPIRED,
                size=view.size,
                executed_size=view.executed_size,
                price=view.price,
                timestamp=view.timestamp,
                source=REST_SOURCE,
            )
        )
    return True


def reconcile_snapshot(
    snapshot: RestSnapshot,
    *,
    store: OrderStateStore,
    ledger: IntentLedger,
    breaker: CircuitBreaker,
    reader: ReadOnlySnapshotReader,
    mode: SnapshotMode,
) -> SnapshotReconcileResult:
    """以 REST 快照裁决对账域状态（T-03、R-08、C-10）。

    先比对后裁决：稳态模式把每处不一致计入熔断计数（R-08）；
    基线与重连补齐模式只对齐不计数，因为首轮无比对起点、断线
    期间的增量缺口是 C-10 预期修复对象而非双通道矛盾。裁决后
    对账域与 REST 快照一致。
    """
    mismatches = _detect_mismatches(store, snapshot)
    stale_ids = tuple(
        item.order_id
        for item in mismatches
        if item.kind == "stale_active_order"
    )
    for order in snapshot.active_orders:
        store.merge_rest_order(order)
    lookup_used = _resolve_stale_orders(store, reader, stale_ids)
    new_facts = sum(
        1
        for execution in snapshot.latest_executions
        if store.merge_rest_execution(execution)
    )
    counted = 0
    if mode is SnapshotMode.AUDIT:
        for _item in mismatches:
            breaker.record_reconciliation_mismatch()
            counted += 1
    return SnapshotReconcileResult(
        mode=mode,
        mismatches=mismatches,
        counted_into_breaker=counted,
        new_execution_count=new_facts,
        order_lookup_used=lookup_used,
    )


class AssetReconcileError(GuvoluError):
    """资产核对的输入或次序非法。"""


class AssetReconciliation:
    """资产异动核对（R-02，口径见 TBD-10）。

    基线取会话首轮快照的資産残高 amount（U-03 语义限定）；此后
    仅账本内委托的成交与手续费推进期望值。评估返回无法解释的
    合计差额与总额，均按快照汇率折 JPY，由调用方交给熔断器
    判定阈值。
    """

    def __init__(self) -> None:
        self._expected: dict[str, Decimal] | None = None
        self._explained_ids: set[int] = set()

    @property
    def initialized(self) -> bool:
        """基线是否已建立。"""
        return self._expected is not None

    def initialize(
        self,
        assets: Iterable[Asset],
        seen_execution_ids: Iterable[int],
    ) -> None:
        """以首轮快照建立基线；既有成交视为已解释。"""
        if self._expected is not None:
            raise AssetReconcileError("资产基线不得重复建立")
        self._expected = {asset.symbol: asset.amount for asset in assets}
        self._explained_ids = set(seen_execution_ids)

    def explain(self, fact: ExecutionFact) -> bool:
        """把一笔账本内成交计入期望资产，重复事实忽略。"""
        if self._expected is None:
            raise AssetReconcileError("资产基线未建立")
        if fact.execution_id in self._explained_ids:
            return False
        self._explained_ids.add(fact.execution_id)
        notional = fact.size * fact.price
        jpy = self._expected.get(_JPY, Decimal("0"))
        held = self._expected.get(fact.symbol, Decimal("0"))
        if fact.side is Side.BUY:
            self._expected[_JPY] = jpy - notional - fact.fee
            self._expected[fact.symbol] = held + fact.size
        else:
            self._expected[_JPY] = jpy + notional - fact.fee
            self._expected[fact.symbol] = held - fact.size
        return True

    def evaluate(
        self, assets: Iterable[Asset]
    ) -> tuple[Decimal, Decimal]:
        """返回（未解释差额，资产总额），均按 JPY 计。"""
        if self._expected is None:
            raise AssetReconcileError("资产基线未建立")
        actual = {asset.symbol: asset for asset in assets}
        unexplained = Decimal("0")
        total = Decimal("0")
        for symbol in sorted(set(self._expected) | set(actual)):
            entry = actual.get(symbol)
            amount = entry.amount if entry is not None else Decimal("0")
            rate = (
                entry.conversion_rate if entry is not None else Decimal("1")
            )
            expected = self._expected.get(symbol, Decimal("0"))
            unexplained += abs(amount - expected) * rate
            total += amount * rate
        return unexplained, total
