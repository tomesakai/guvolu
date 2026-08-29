"""执行链发送编排：闸门次序与发送边界抽象。

本模块只依赖注入的发送接口，使闸门与状态机可在无网络下
单测（C-13、C-14）；生产接线由 execution.trade_sender 适配
TradeClient 完成（T-02）。闸门次序与拒绝动作见执行链设计
第 5 节。模拟运行模式下发送边界抛出拦截异常，编排把它记为
本地终态，作为 dry-run 彩排的预期终点（T-04）。paper 模式的
发送边界以成交模型结算或拒绝，同样记为本地终态，零写请求。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Protocol

from guvolu.data.intent_ledger import IntentLedger
from guvolu.domain.enums import ServiceStatus
from guvolu.domain.errors import (
    ApiNetworkError,
    DryRunBlocked,
    GmoApiError,
    PaperRejected,
    PaperSettled,
)
from guvolu.domain.intent import IntentError, IntentState, OrderIntent
from guvolu.domain.symbols import SpotSymbol
from guvolu.execution.inflight_lock import (
    SymbolInFlightLock,
    acquire_symbol_inflight_lock,
)
from guvolu.risk.circuit_breaker import CircuitBreaker
from guvolu.risk.errors import CircuitTripped, LimitExceeded
from guvolu.risk.limits import LimitGate
from guvolu.risk.service_gate import allows_new_intent


class OrderSender(Protocol):
    """发送边界抽象，返回交易所委托号。阶段二由 TradeClient 适配。

    consumes_write_budget 标记发送路径是否触碰真实写端点：真实
    写路径消耗 T-11 单日预算并取跨进程在途锁；零写发送边界
    （dry-run 拦截、paper 成交模型）两者皆免。
    """

    consumes_write_budget: bool

    def send(self, intent: OrderIntent) -> int: ...


@dataclass(frozen=True, slots=True)
class DispatchResult:
    """一次编排的本地视角结果。真实状态以 READ_ONLY 为准（T-03）。

    consumed_write_budget 记录该笔是否已计入当日预算（T-11）。
    """

    intent_id: str
    state: IntentState
    order_id: int | None
    reason: str | None
    consumed_write_budget: bool = False


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
    inflight_dir: Path | None = None,
) -> DispatchResult:
    """落盘意图、依次过闸、经注入接口发送并登记结果。

    闸门次序：白名单（T-09）、熔断（R-02）、服务状态（R-03）、
    在途约束（T-05）、三限额（T-11）。三限额校验对全部模式照常
    执行，限额超限按 T-11 触发熔断而非仅拒绝；用量累计仅当发送
    边界消耗写预算时记入，零写终态不占单日预算（T-11 的语义边界
    是真实写请求），记入后即使超时或被拒也不回退，保守计数。
    消耗写预算的发送期间另持品种级跨进程独占锁扩展在途约束
    （T-05）：取不到锁即按闸门拒绝，不阻塞等待，终态落账后释放；
    锁目录缺省在数据根下解析（C-04）。超时与网络错转入超时态
    等待查询（T-06）。
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
    consumes = sender.consumes_write_budget
    inflight_lock: SymbolInFlightLock | None = None
    if consumes:
        # 跨进程在途扩展，仅真实写路径（T-05）
        inflight_lock = acquire_symbol_inflight_lock(
            intent.symbol, directory=inflight_dir
        )
        if inflight_lock is None:
            return rejected(f"品种 {intent.symbol} 同品种跨进程在途")
    try:
        try:
            if consumes:
                limit_gate.commit(notional_jpy, now)
            else:
                # 零写路径只校验不累计（T-11）
                limit_gate.check(notional_jpy, now)
        except LimitExceeded as exc:
            # T-11
            breaker.trip(f"限额超限: {exc}")
            return rejected(str(exc))
        ledger.begin_send(
            intent.intent_id, consumes_write_budget=consumes, at=now
        )
        try:
            order_id = sender.send(intent)
        except DryRunBlocked as exc:
            # 模拟拦截是预期终点，不计异常（T-04）
            ledger.block_dry_run(intent.intent_id, reason=str(exc), at=now)
            return DispatchResult(
                intent.intent_id,
                IntentState.DRY_RUN_BLOCKED,
                None,
                str(exc),
                consumed_write_budget=consumes,
            )
        except PaperSettled as exc:
            # 成交模型结算，零写请求（T-04）
            ledger.paper_fill(
                intent.intent_id,
                reason=str(exc),
                evidence=exc.evidence,
                at=now,
            )
            return DispatchResult(
                intent.intent_id,
                IntentState.PAPER_FILLED,
                None,
                str(exc),
                consumed_write_budget=consumes,
            )
        except PaperRejected as exc:
            ledger.paper_reject(intent.intent_id, reason=str(exc), at=now)
            return DispatchResult(
                intent.intent_id,
                IntentState.PAPER_REJECTED,
                None,
                str(exc),
                consumed_write_budget=consumes,
            )
        except ApiNetworkError as exc:
            # 含超时，绝不盲目重发（T-06）
            breaker.record_write_failure()
            ledger.mark_send_timeout(
                intent.intent_id, reason=str(exc), at=now
            )
            return DispatchResult(
                intent.intent_id,
                IntentState.SEND_TIMEOUT,
                None,
                str(exc),
                consumed_write_budget=consumes,
            )
        except GmoApiError as exc:
            breaker.record_write_failure()
            ledger.reject(intent.intent_id, reason=str(exc), at=now)
            return DispatchResult(
                intent.intent_id,
                IntentState.REJECTED,
                None,
                str(exc),
                consumed_write_budget=consumes,
            )
        breaker.record_write_success()
        ledger.accept(intent.intent_id, order_id, at=now)
        return DispatchResult(
            intent.intent_id,
            IntentState.ACCEPTED,
            order_id,
            None,
            consumed_write_budget=consumes,
        )
    finally:
        if inflight_lock is not None:
            # 终态落账后释放（T-05）
            inflight_lock.release()
