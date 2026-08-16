"""超时意图对账：READ_ONLY 查询证据驱动的终态判定（T-06 后半）。

对账查询只走注入的只读抽象，ReadClient 是其生产实现（T-02、
T-03）。GMO 无客户端自定义委托号，匹配依据是同品种单在途约束
（T-05）：超时窗口内至多存在一笔本系统发出而未映射的委托。
候选取自挂单一览与最新成交一览两个只读端点；恰一笔匹配即受理
并登记映射，零笔匹配即判定未受理，多笔匹配是歧义，拒绝自动
判定，留待人工处置。查询证据随迁移写入账本（T-06）。
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Protocol

from guvolu.data.intent_ledger import IntentLedger, LedgerError
from guvolu.domain.intent import IntentState, OrderIntent
from guvolu.domain.models import Execution, Order

# 本机与交易所时戳容差
_CLOCK_TOLERANCE = timedelta(seconds=60)
# 对账触碰的只读端点
READ_ENDPOINTS = ("GET /v1/activeOrders", "GET /v1/latestExecutions")


class ReadOnlyOrderReader(Protocol):
    """对账所需的只读查询抽象，ReadClient 满足本形态（T-02）。"""

    def active_orders(
        self, symbol: str, page: int | None = None, count: int | None = None
    ) -> tuple[Order, ...]: ...

    def latest_executions(
        self, symbol: str, page: int | None = None, count: int | None = None
    ) -> tuple[Execution, ...]: ...


class ReconcileAmbiguity(LedgerError):
    """候选多于一笔，拒绝自动判定，须人工处置。"""


@dataclass(frozen=True, slots=True)
class TimeoutResolution:
    """一次超时对账的判定结果。"""

    intent_id: str
    state: IntentState
    order_id: int | None
    evidence: Mapping[str, str]


def _order_matches(intent: OrderIntent, order: Order, since: datetime) -> bool:
    """挂单候选判据：品种、方向、类型、数量、价格与时窗。"""
    if order.symbol != str(intent.symbol):
        return False
    if order.side is not intent.side:
        return False
    if order.execution_type is not intent.execution_type:
        return False
    if order.size != intent.size:
        return False
    if intent.price is not None and order.price != intent.price:
        return False
    return order.timestamp >= since


def _execution_group_matches(
    intent: OrderIntent,
    rows: tuple[Execution, ...],
    since: datetime,
) -> bool:
    """成交候选判据：方向、时窗，累计数量不超过意图数量。"""
    total = sum((row.size for row in rows), Decimal("0"))
    if total > intent.size:
        return False
    return all(
        row.symbol == str(intent.symbol)
        and row.side is intent.side
        and row.timestamp >= since
        for row in rows
    )


def resolve_send_timeout(
    intent_id: str,
    *,
    ledger: IntentLedger,
    reader: ReadOnlyOrderReader,
    moment: datetime | None = None,
) -> TimeoutResolution:
    """查询 READ_ONLY 判定一笔超时意图并把证据写回账本。

    判定结果只有两种：恰一笔候选即 ACCEPTED 并映射委托号，
    零笔候选即 FAILED；多笔候选抛出歧义异常，账本保持超时态
    继续占用在途额度（T-05），等待人工处置。
    """
    state = ledger.state(intent_id)
    if state is not IntentState.SEND_TIMEOUT:
        raise LedgerError(f"意图 {intent_id} 不在超时态: {state.value}")
    intent = ledger.intent(intent_id)
    since = intent.created_at - _CLOCK_TOLERANCE
    now = moment if moment is not None else datetime.now(UTC)
    symbol = str(intent.symbol)
    orders = reader.active_orders(symbol)
    executions = reader.latest_executions(symbol)
    # 候选表：委托号到来源端点
    candidates: dict[int, str] = {}
    for order in orders:
        if ledger.intent_id_for_order(order.order_id) is not None:
            continue
        if _order_matches(intent, order, since):
            candidates[order.order_id] = "activeOrders"
    grouped: dict[int, tuple[Execution, ...]] = {}
    for row in executions:
        grouped[row.order_id] = grouped.get(row.order_id, ()) + (row,)
    for order_id, rows in grouped.items():
        if order_id in candidates:
            continue
        if ledger.intent_id_for_order(order_id) is not None:
            continue
        if _execution_group_matches(intent, rows, since):
            candidates[order_id] = "latestExecutions"
    evidence: dict[str, str] = {
        "source": "READ_ONLY",
        "endpoints": ",".join(READ_ENDPOINTS),
        "queried_at": now.isoformat(),
        "active_order_count": str(len(orders)),
        "latest_execution_count": str(len(executions)),
    }
    if len(candidates) > 1:
        listed = ",".join(str(order_id) for order_id in sorted(candidates))
        raise ReconcileAmbiguity(
            f"意图 {intent_id} 候选委托多于一笔: {listed}"
        )
    if candidates:
        order_id, matched_source = next(iter(candidates.items()))
        evidence["matched_order_id"] = str(order_id)
        evidence["matched_source"] = matched_source
        ledger.accept(intent_id, order_id, evidence=evidence, at=moment)
        return TimeoutResolution(
            intent_id, IntentState.ACCEPTED, order_id, evidence
        )
    evidence["matched_order_id"] = ""
    ledger.resolve_timeout_failed(intent_id, evidence=evidence, at=moment)
    return TimeoutResolution(intent_id, IntentState.FAILED, None, evidence)


def send_timeout_intents(ledger: IntentLedger) -> tuple[str, ...]:
    """按落盘顺序列出全部处于超时态的意图。"""
    return tuple(
        intent_id
        for intent_id in ledger.intent_ids()
        if ledger.state(intent_id) is IntentState.SEND_TIMEOUT
    )
