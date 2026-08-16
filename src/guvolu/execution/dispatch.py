"""执行链发送编排：闸门次序与发送边界抽象（阶段一）。

真实 TradeClient 接线留待阶段二；本模块只依赖注入的发送接口，
使闸门与状态机可在无网络下单测（C-13、C-14）。闸门次序与拒绝
动作见执行链设计第 5 节。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Protocol

from guvolu.data.intent_ledger import IntentLedger
from guvolu.domain.enums import ServiceStatus
from guvolu.domain.errors import ApiNetworkError, GmoApiError
from guvolu.domain.intent import IntentError, IntentState, OrderIntent
from guvolu.domain.symbols import SpotSymbol
from guvolu.risk.circuit_breaker import CircuitBreaker
from guvolu.risk.errors import CircuitTripped, LimitExceeded
from guvolu.risk.limits import LimitGate
from guvolu.risk.service_gate import allows_new_intent


class OrderSender(Protocol):
    """发送边界抽象，返回交易所委托号。阶段二由 TradeClient 适配。"""

    def send(self, intent: OrderIntent) -> int: ...


@dataclass(frozen=True, slots=True)
class DispatchResult:
    """一次编排的本地视角结果。真实状态以 READ_ONLY 为准（T-03）。"""

    intent_id: str
    state: IntentState
    order_id: int | None
    reason: str | None


def dispatch_order_intent(
    intent: OrderIntent,
    *,
    ledger: IntentLedger,
    limit_gate: LimitGate,
    breaker: CircuitBreaker,
    service_status: ServiceStatus,
    whitelist: frozenset[SpotSymbol],
    sender: OrderSender,
    reference_price: Decimal | None = None,
    moment: datetime | None = None,
) -> DispatchResult:
    """落盘意图、依次过闸、经注入接口发送并登记结果。

    闸门次序：白名单（T-09）、熔断（R-02）、服务状态（R-03）、
    在途约束（T-05）、三限额（T-11）。限额在通过时记入当日累计，
    此后发送即使超时或被拒也不回退，保守计数。限额超限按 T-11
    触发熔断而非仅拒绝。超时与网络错转入超时态等待查询（T-06）。
    """
    now = moment if moment is not None else datetime.now(UTC)
    # 落盘先于发送
    ledger.record_intent(intent, at=now)

    def rejected(reason: str) -> DispatchResult:
        ledger.gate_reject(intent.intent_id, reason=reason, at=now)
        return DispatchResult(
            intent.intent_id, IntentState.GATE_REJECTED, None, reason
        )

    if intent.symbol not in whitelist:
        return rejected(f"品种不在白名单: {intent.symbol}")
    try:
        breaker.ensure_can_send()
    except CircuitTripped as exc:
        return rejected(str(exc))
    if not allows_new_intent(service_status):
        return rejected(f"服务状态 {service_status.value} 拒绝新意图")
    if ledger.in_flight(intent.symbol):
        return rejected(f"品种 {intent.symbol} 已有在途写请求")
    try:
        notional_jpy = intent.notional_jpy(reference_price)
    except IntentError as exc:
        return rejected(str(exc))
    try:
        limit_gate.commit(notional_jpy, now)
    except LimitExceeded as exc:
        # T-11
        breaker.trip(f"限额超限: {exc}")
        return rejected(str(exc))
    ledger.begin_send(intent.intent_id, at=now)
    try:
        order_id = sender.send(intent)
    except ApiNetworkError as exc:
        # 含超时，绝不盲目重发（T-06）
        breaker.record_write_failure()
        ledger.mark_send_timeout(
            intent.intent_id, reason=str(exc), at=now
        )
        return DispatchResult(
            intent.intent_id, IntentState.SEND_TIMEOUT, None, str(exc)
        )
    except GmoApiError as exc:
        breaker.record_write_failure()
        ledger.reject(intent.intent_id, reason=str(exc), at=now)
        return DispatchResult(
            intent.intent_id, IntentState.REJECTED, None, str(exc)
        )
    breaker.record_write_success()
    ledger.accept(intent.intent_id, order_id, at=now)
    return DispatchResult(
        intent.intent_id, IntentState.ACCEPTED, order_id, None
    )
