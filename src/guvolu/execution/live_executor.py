"""live 执行器：授权信封约束下的实盘自动执行链（阶段六）。

消费 frozen_target_adapter 的 live 模式执行目标，经 G-05 转换
闸门折算为单笔限价意图，先过授权信封门禁（第 14 节），再经
统一发送编排进入真实发送适配（T-02、T-05、T-11）。受理后以
READ_ONLY 轮询至终态或有界等待后撤单确认（T-03、R-01）；发送
超时经查询后决策（T-06）。信封用量与状态持久追踪，重启不重置；
触界按信封登记动作熔断停机，cancel_and_flatten 先全撤后市价
清仓（T-07）。进程配置非 live 即拒绝启动（T-04）；本模块永不
打印任何密钥内容（T-01、A-06）。

命令行入口见 scripts/run_live_executor.py，形态与 dry-run
执行器对齐，另加 --envelope。熔断阈值取版本化配置与信封
ops_breaker 的逐项更严者（G-06、R-02）。
"""
from __future__ import annotations

import argparse
import hashlib
import json
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Protocol

from guvolu.api.public_client import PublicClient
from guvolu.api.read_client import ReadClient
from guvolu.api.trade_client import TradeClient
from guvolu.data.intent_ledger import IntentLedger
from guvolu.data.paths import data_root
from guvolu.domain.config import Config, load_config
from guvolu.domain.enums import (
    ExecutionType,
    OrderStatus,
    RunMode,
    ServiceStatus,
    Side,
)
from guvolu.domain.errors import (
    ApiNetworkError,
    DryRunBlocked,
    GmoApiError,
    GuvoluError,
)
from guvolu.domain.ids import new_correlation_id, new_intent_id
from guvolu.domain.intent import (
    LOCAL_TERMINAL_STATES,
    IntentState,
    OrderIntent,
)
from guvolu.domain.models import Asset, Execution, Order
from guvolu.domain.symbols import SpotSymbol
from guvolu.execution.authorization_envelope import (
    DEFAULT_ENVELOPE_PATH,
    VERDICT_ALLOW,
    VERDICT_HALT,
    VERDICT_TRIP,
    AuthorizationEnvelope,
    EnvelopeDecision,
    EnvelopeState,
    EnvelopeStateStore,
    EnvelopeUsage,
    GateInputs,
    OnTrip,
    ValuationBaseline,
    evaluate_envelope_gates,
    gate_records_payload,
    load_envelope,
    observe_price,
)
from guvolu.execution.conversion import MarketRule
from guvolu.execution.dispatch import DispatchResult, dispatch_order_intent
from guvolu.execution.dry_run_executor import (
    ORDER_ENDPOINT,
    DryRunPlan,
    TargetArtifact,
    build_plan,
    fetch_market_rule,
    load_market_rule,
    load_target_artifact,
    verify_v2_source_prediction,
)
from guvolu.execution.inflight_lock import (
    INFLIGHT_LOCK_RELATIVE_DIR,
    acquire_symbol_inflight_lock,
)
from guvolu.execution.limit_replay import replay_limit_usage
from guvolu.execution.paper_config import (
    DEFAULT_PAPER_CONFIG_PATH,
    PaperExecutorConfig,
    load_paper_config,
)
from guvolu.execution.paper_fill_model import (
    PUBLIC_ORDERBOOK_BASIS,
    BookSnapshot,
    FillModelError,
    load_book_snapshot_file,
)
from guvolu.execution.reconcile import (
    ReconcileAmbiguity,
    resolve_send_timeout,
)
from guvolu.execution.trade_sender import TradeClientSender
from guvolu.ops import kill_switch
from guvolu.risk.circuit_breaker import (
    DEFAULT_THRESHOLDS_PATH,
    BreakerState,
    BreakerThresholds,
    CircuitBreaker,
    load_breaker_thresholds,
)
from guvolu.risk.limits import LimitGate, trading_day
from guvolu.risk.service_gate import allows_new_intent

# 报告与账本在数据根下的目录
LIVE_RELATIVE_DIR = Path("execution") / "live"
LIVE_LEDGER_NAME = "intent_ledger.jsonl"
# 受理后有界等待的缺省秒数
DEFAULT_MAX_WAIT_SECONDS = 240
# 委托状态轮询间隔秒数
POLL_INTERVAL_SECONDS = 5.0
# 撤单后确认终态的时限秒数
CANCEL_VERIFY_TIMEOUT_SECONDS = 90.0
# 触碰端点名（A-03）
CANCEL_ENDPOINT = "POST /v1/cancelOrder"
CANCEL_BULK_ENDPOINT = "POST /v1/cancelBulkOrder"
SYMBOLS_ENDPOINT = "GET /v1/symbols"
TICKER_ENDPOINT = "GET /v1/ticker"
STATUS_ENDPOINT = "GET /v1/status"
ORDERBOOKS_ENDPOINT = "GET /v1/orderbooks"
ASSETS_ENDPOINT = "GET /v1/account/assets"
ORDERS_ENDPOINT = "GET /v1/orders"
ACTIVE_ORDERS_ENDPOINT = "GET /v1/activeOrders"
LATEST_EXECUTIONS_ENDPOINT = "GET /v1/latestExecutions"
# 委托终态集合（对账域口径）
TERMINAL_ORDER_STATUSES = frozenset(
    {OrderStatus.EXECUTED, OrderStatus.CANCELED, OrderStatus.EXPIRED}
)
# JPY 资产键名
_JPY = "JPY"
# 退出码语义
EXIT_OK = 0
EXIT_ANOMALY = 1
EXIT_REFUSED = 2


class LiveExecutorError(GuvoluError):
    """live 执行器输入非法或前置条件不满足。"""


class LiveReader(Protocol):
    """live 执行所需 READ_ONLY 能力子集（T-02、T-03）。"""

    def orders(self, order_ids: Sequence[int]) -> tuple[Order, ...]: ...

    def active_orders(
        self, symbol: str, page: int | None = None, count: int | None = None
    ) -> tuple[Order, ...]: ...

    def latest_executions(
        self, symbol: str, page: int | None = None, count: int | None = None
    ) -> tuple[Execution, ...]: ...

    def assets(self) -> tuple[Asset, ...]: ...


class LiveSender(Protocol):
    """发送与撤单边界：生产实现为 TradeClientSender（T-02）。"""

    consumes_write_budget: bool

    def send(self, intent: OrderIntent) -> int: ...

    def cancel(self, order_id: int) -> None: ...


@dataclass(slots=True)
class LiveRuntime:
    """一次 live 周期的可注入依赖集合（C-13 替身边界）。"""

    config: Config
    envelope: AuthorizationEnvelope
    usage: EnvelopeUsage
    state_store: EnvelopeStateStore
    state: EnvelopeState
    ledger: IntentLedger
    limit_gate: LimitGate
    breaker: CircuitBreaker
    reader: LiveReader
    sender: LiveSender
    service_status: ServiceStatus
    rule: MarketRule
    inflight_dir: Path
    max_wait_seconds: int = DEFAULT_MAX_WAIT_SECONDS
    clock: Callable[[], float] = time.monotonic
    sleep: Callable[[float], None] = time.sleep
    read_touched: list[str] = field(default_factory=list)
    write_touched: list[str] = field(default_factory=list)

    def save_state(self) -> None:
        """状态原子覆写落盘。"""
        self.state_store.save(self.state)


def merged_breaker_thresholds(
    base: BreakerThresholds, envelope: AuthorizationEnvelope
) -> BreakerThresholds:
    """取版本化配置与信封的逐项更严阈值（R-02、G-06）。"""
    env = envelope.breaker_thresholds()
    return BreakerThresholds(
        schema_version=base.schema_version,
        consecutive_failure_limit=min(
            base.consecutive_failure_limit, env.consecutive_failure_limit
        ),
        stream_gap_seconds=min(
            base.stream_gap_seconds, env.stream_gap_seconds
        ),
        asset_deviation_ratio=min(
            base.asset_deviation_ratio, env.asset_deviation_ratio
        ),
        asset_deviation_floor_jpy=min(
            base.asset_deviation_floor_jpy, env.asset_deviation_floor_jpy
        ),
    )


def validate_live_target(
    artifact: TargetArtifact,
    *,
    runtime_mode: RunMode,
    target_config: PaperExecutorConfig,
    envelope: AuthorizationEnvelope,
    now: datetime,
) -> None:
    """live 目标消费前义务：模式、市场、时效与预算绑定。"""
    if now.tzinfo is None or now.utcoffset() is None:
        raise LiveExecutorError("执行时刻必须带时区")
    if runtime_mode is not RunMode.LIVE:
        raise LiveExecutorError("live 执行器拒绝非 live 运行模式")
    if artifact.mode != RunMode.LIVE.value:
        raise LiveExecutorError("执行目标 mode 不是 live")
    if (
        artifact.symbol is None
        or artifact.risk_budget_jpy is None
        or artifact.valid_from is None
        or artifact.valid_until is None
    ):
        raise LiveExecutorError("live 只接受 adapter v2 执行目标")
    if (
        artifact.market_id != target_config.market_id
        or artifact.symbol != target_config.symbol
        or artifact.payload.get("bar_interval") != target_config.bar_interval
    ):
        raise LiveExecutorError(
            "live 目标 market/symbol/bar_interval 与执行配置不一致"
        )
    if artifact.risk_budget_jpy != target_config.risk_budget_jpy:
        raise LiveExecutorError("live 目标 risk_budget_jpy 与执行配置不一致")
    if artifact.symbol not in envelope.symbols:
        raise LiveExecutorError(
            f"live 目标品种 {artifact.symbol} 不在信封白名单"
        )
    if not artifact.valid_from <= now < artifact.valid_until:
        raise LiveExecutorError("live 目标尚未生效或已经过期")


def _assets_view(assets: Sequence[Asset]) -> dict[str, dict[str, str]]:
    """资产快照视图，amount 与 available 分列（U-03）。"""
    return {
        asset.symbol: {
            "amount": format(asset.amount, "f"),
            "available": format(asset.available, "f"),
        }
        for asset in assets
    }


def _asset_amount(assets: Sequence[Asset], symbol: str) -> Decimal:
    for asset in assets:
        if asset.symbol == symbol:
            return asset.amount
    return Decimal("0")


def _asset_available(assets: Sequence[Asset], symbol: str) -> Decimal:
    for asset in assets:
        if asset.symbol == symbol:
            return asset.available
    return Decimal("0")


def refresh_baselines(
    runtime: LiveRuntime,
    *,
    assets: Sequence[Asset],
    reference_price: Decimal,
    now: datetime,
) -> None:
    """建立或滚动估值基线并记录参考价观测。

    loss_baseline 只在首个 live 周期建立；day_baseline 按 JST
    06:00 交易日边界滚动（C-11、D-08）。
    """
    snapshot = ValuationBaseline(
        at=now,
        jpy_amount=_asset_amount(assets, _JPY),
        btc_amount=_asset_amount(assets, str(runtime.rule.symbol)),
        reference_price=reference_price,
    )
    state = runtime.state
    if state.loss_baseline is None:
        state = replace(state, loss_baseline=snapshot)
    day = trading_day(now)
    if state.day_baseline_day != day:
        state = replace(state, day_baseline=snapshot, day_baseline_day=day)
    runtime.state = observe_price(state, price=reference_price, at=now)


def evaluate_gates_for_plan(
    runtime: LiveRuntime,
    plan: DryRunPlan,
    *,
    assets: Sequence[Asset],
    price_observed_at: datetime,
    book: BookSnapshot | None,
    decision_time: datetime,
    now: datetime,
) -> EnvelopeDecision:
    """组装门禁输入并评估，新状态写回运行时。"""
    reference_price = plan.reference_price
    btc_amount = _asset_amount(assets, str(runtime.rule.symbol))
    current_value = (
        _asset_amount(assets, _JPY) + btc_amount * reference_price
    )
    order_side: Side | None = None
    order_notional: Decimal | None = None
    spread_bp: Decimal | None = None
    opposite_depth: Decimal | None = None
    if plan.proposal is not None:
        order_side = plan.proposal.side
        order_notional = plan.proposal.notional_jpy
        if book is not None:
            spread_bp = book.spread_bps()
            if order_side is Side.BUY:
                opposite_depth = book.asks[0].price * book.asks[0].size
            else:
                opposite_depth = book.bids[0].price * book.bids[0].size
    day = trading_day(now)
    inputs = GateInputs(
        now=now,
        price_observed_at=price_observed_at,
        used_total_jpy=runtime.usage.total_jpy(),
        day_used_jpy=runtime.usage.day_jpy(day),
        day_order_count=runtime.usage.day_count(day),
        current_value_jpy=current_value,
        position_notional_jpy=btc_amount * reference_price,
        order_side=order_side,
        order_notional_jpy=order_notional,
        spread_bp=spread_bp,
        opposite_depth_jpy=opposite_depth,
        decision_time=decision_time,
    )
    decision, new_state = evaluate_envelope_gates(
        runtime.envelope, runtime.state, inputs
    )
    runtime.state = new_state
    return decision


def poll_order(
    runtime: LiveRuntime,
    order_id: int,
    *,
    wait_seconds: float,
) -> Order | None:
    """轮询 READ_ONLY 委托快照至终态或届满（T-03）。"""
    deadline = runtime.clock() + wait_seconds
    runtime.read_touched.append(ORDERS_ENDPOINT)
    last: Order | None = None
    while True:
        orders = runtime.reader.orders([order_id])
        if orders:
            last = orders[0]
            if last.status in TERMINAL_ORDER_STATUSES:
                return last
        if runtime.clock() >= deadline:
            return last
        runtime.sleep(POLL_INTERVAL_SECONDS)


@dataclass(frozen=True, slots=True)
class LiveOrderOutcome:
    """一次 live 委托的终局与全过程证据（R-07）。"""

    intent: OrderIntent
    dispatch: DispatchResult
    final_order: Order | None
    cancel_requested: bool
    resolution: str
    terminal: bool


def execute_live_order(
    runtime: LiveRuntime, plan: DryRunPlan, *, now: datetime
) -> LiveOrderOutcome:
    """把计划折算为限价意图并走完发送、轮询与撤单闭环。

    发送经统一编排：意图先落盘、五道闸门、跨进程在途锁、写预算
    累计（T-05、T-11）；消耗写预算即追加信封用量行，保守计数。
    受理后轮询至终态或有界等待届满撤单确认（R-01、TBD-11 先撤
    方向）；超时经 READ_ONLY 查询后决策（T-06）。
    """
    if plan.proposal is None:
        raise LiveExecutorError("计划无委托，不应进入发送")
    proposal = plan.proposal
    intent = OrderIntent(
        intent_id=new_intent_id(),
        correlation_id=plan.artifact.correlation_id,
        symbol=proposal.symbol,
        side=proposal.side,
        execution_type=ExecutionType.LIMIT,
        size=proposal.size,
        price=proposal.price,
        time_in_force=None,
        created_at=now,
        prediction_id=plan.artifact.run_id,
        decision_time=plan.artifact.decision_time,
    )
    result = dispatch_order_intent(
        intent,
        ledger=runtime.ledger,
        limit_gate=runtime.limit_gate,
        breaker=runtime.breaker,
        service_status=runtime.service_status,
        whitelist=runtime.config.spot_whitelist,
        sender=runtime.sender,
        moment=now,
        inflight_dir=runtime.inflight_dir,
    )
    if result.consumed_write_budget:
        # 信封用量保守计数（T-11 口径）
        runtime.usage.append(
            intent_id=intent.intent_id,
            notional_jpy=proposal.notional_jpy,
            at=now,
        )
    if result.state not in LOCAL_TERMINAL_STATES:
        runtime.write_touched.append(ORDER_ENDPOINT)
    if result.state is IntentState.SEND_TIMEOUT:
        runtime.read_touched.extend(
            (ACTIVE_ORDERS_ENDPOINT, LATEST_EXECUTIONS_ENDPOINT)
        )
        try:
            resolution = resolve_send_timeout(
                intent.intent_id, ledger=runtime.ledger, reader=runtime.reader
            )
        except ReconcileAmbiguity as exc:
            return LiveOrderOutcome(
                intent, result, None, False,
                f"超时对账歧义，保持在途等待人工处置: {exc}", False,
            )
        if resolution.order_id is None:
            return LiveOrderOutcome(
                intent, result, None, False,
                "超时对账判定为未受理（FAILED）", False,
            )
        result = DispatchResult(
            result.intent_id,
            IntentState.ACCEPTED,
            resolution.order_id,
            "超时对账判定为已受理",
            consumed_write_budget=result.consumed_write_budget,
        )
    if result.state is not IntentState.ACCEPTED or result.order_id is None:
        return LiveOrderOutcome(
            intent, result, None, False,
            f"发送未受理: {result.reason}", False,
        )
    snapshot = poll_order(
        runtime, result.order_id,
        wait_seconds=float(runtime.max_wait_seconds),
    )
    if snapshot is not None and snapshot.status in TERMINAL_ORDER_STATUSES:
        return LiveOrderOutcome(
            intent, result, snapshot, False, "窗口内到达终态", True,
        )
    # R-01：窗口届满执行退出条件
    runtime.sender.cancel(result.order_id)
    runtime.write_touched.append(CANCEL_ENDPOINT)
    snapshot = poll_order(
        runtime, result.order_id,
        wait_seconds=CANCEL_VERIFY_TIMEOUT_SECONDS,
    )
    if snapshot is not None and snapshot.status in TERMINAL_ORDER_STATUSES:
        return LiveOrderOutcome(
            intent, result, snapshot, True, "届满撤单并确认终态", True,
        )
    return LiveOrderOutcome(
        intent, result, snapshot, True,
        "撤单后未确认终态，请立即人工复核或使用 kill-switch（T-07）",
        False,
    )


def verify_first_order(
    runtime: LiveRuntime, order: Order
) -> tuple[bool, dict[str, object]]:
    """首单快照对账：委托快照与成交一览两视图相互校验。

    以 GET /v1/orders 的终态快照与 GET /v1/latestExecutions 的
    成交合计相互校验（R-08 口径的快照两视图；本执行器无 WS
    通道，实时通道对账由浸泡进程承担）。一致方可解除首单
    canary 压额（T-12）。
    """
    executions = runtime.reader.latest_executions(order.symbol)
    runtime.read_touched.append(LATEST_EXECUTIONS_ENDPOINT)
    matched = tuple(
        row for row in executions if row.order_id == order.order_id
    )
    executed_total = sum((row.size for row in matched), Decimal("0"))
    consistent = (
        order.status in TERMINAL_ORDER_STATUSES
        and executed_total == order.executed_size
    )
    evidence: dict[str, object] = {
        "order_id": order.order_id,
        "order_status": order.status.value,
        "order_executed_size": format(order.executed_size, "f"),
        "execution_total_size": format(executed_total, "f"),
        "execution_count": len(matched),
        "consistent": consistent,
        "endpoints": [ORDERS_ENDPOINT, LATEST_EXECUTIONS_ENDPOINT],
    }
    return consistent, evidence


def flatten_position(
    runtime: LiveRuntime,
    *,
    reference_price: Decimal,
    now: datetime,
) -> dict[str, object]:
    """市价卖出全部现物持仓（cancel_and_flatten 的清仓步）。

    清仓豁免信封额度门但仍记录用量与证据；意图同样先落盘
    （T-05），发送异常按 T-06 分类落账，绝不重发。数量取撤单后
    可用量向下取整到 sizeStep；服务状态非 OPEN 时不发送市价单
    （R-03 清仓非撤单，无 T-07 豁免），留待人工处置。
    """
    assets = runtime.reader.assets()
    runtime.read_touched.append(ASSETS_ENDPOINT)
    available = _asset_available(assets, str(runtime.rule.symbol))
    size = (available // runtime.rule.size_step) * runtime.rule.size_step
    payload: dict[str, object] = {
        "available": format(available, "f"),
        "size": format(size, "f"),
    }
    if size < runtime.rule.min_order_size:
        payload["status"] = "skipped"
        payload["reason"] = "持仓低于最小委托量，无需清仓"
        return payload
    if not allows_new_intent(runtime.service_status):
        payload["status"] = "deferred"
        payload["reason"] = (
            f"服务状态 {runtime.service_status.value} 不发市价清仓，"
            "留待人工处置（R-03）"
        )
        return payload
    lock = acquire_symbol_inflight_lock(
        runtime.rule.symbol, directory=runtime.inflight_dir
    )
    if lock is None:
        payload["status"] = "lock_unavailable"
        payload["reason"] = "同品种跨进程在途，清仓留待人工处置"
        return payload
    intent = OrderIntent(
        intent_id=new_intent_id(),
        correlation_id=new_correlation_id(),
        symbol=runtime.rule.symbol,
        side=Side.SELL,
        execution_type=ExecutionType.MARKET,
        size=size,
        price=None,
        time_in_force=None,
        created_at=now,
    )
    payload["intent_id"] = intent.intent_id
    try:
        # 清仓意图先落盘（T-05）
        runtime.ledger.record_intent(intent, at=now)
        runtime.ledger.begin_send(
            intent.intent_id, consumes_write_budget=True, at=now
        )
        # 清仓豁免额度门仍记录用量
        runtime.usage.append(
            intent_id=intent.intent_id,
            notional_jpy=size * reference_price,
            at=now,
        )
        try:
            order_id = runtime.sender.send(intent)
        except DryRunBlocked as exc:
            runtime.ledger.block_dry_run(
                intent.intent_id, reason=str(exc), at=now
            )
            payload["status"] = "blocked"
            payload["reason"] = str(exc)
            return payload
        except ApiNetworkError as exc:
            # 结果未知，绝不重发（T-06）
            runtime.ledger.mark_send_timeout(
                intent.intent_id, reason=str(exc), at=now
            )
            runtime.write_touched.append(ORDER_ENDPOINT)
            payload["status"] = "send_timeout"
            payload["reason"] = str(exc)
            return payload
        except GmoApiError as exc:
            runtime.ledger.reject(intent.intent_id, reason=str(exc), at=now)
            runtime.write_touched.append(ORDER_ENDPOINT)
            payload["status"] = "rejected"
            payload["reason"] = str(exc)
            return payload
        runtime.ledger.accept(intent.intent_id, order_id, at=now)
        runtime.write_touched.append(ORDER_ENDPOINT)
        payload["status"] = "accepted"
        payload["order_id"] = order_id
        return payload
    finally:
        lock.release()


def execute_on_trip(
    runtime: LiveRuntime,
    *,
    reason: str,
    reference_price: Decimal,
    cancel_all: Callable[[], int],
    now: datetime,
) -> dict[str, object]:
    """执行信封登记的熔断动作：先全撤，后按需清仓（T-07）。

    全撤经 kill_switch 口径真实触碰端点；动作完成后信封状态置
    熔断锁定并落盘，后续周期停机待人工复核（重启不重置）。
    """
    runtime.breaker.trip(reason)
    payload: dict[str, object] = {
        "reason": reason,
        "on_trip": runtime.envelope.on_trip.value,
    }
    try:
        cancel_exit = cancel_all()
        payload["cancel_all_exit_code"] = cancel_exit
    except GuvoluError as exc:
        # 全撤失败留痕待人工
        payload["cancel_all_error"] = str(exc)
    runtime.read_touched.append(SYMBOLS_ENDPOINT)
    runtime.write_touched.append(CANCEL_BULK_ENDPOINT)
    if runtime.envelope.on_trip is OnTrip.CANCEL_AND_FLATTEN:
        payload["flatten"] = flatten_position(
            runtime, reference_price=reference_price, now=now
        )
    runtime.state = replace(
        runtime.state, tripped_at=now, trip_reason=reason
    )
    runtime.save_state()
    return payload


def write_live_report(
    body: Mapping[str, object], directory: Path
) -> Path:
    """内容寻址落盘 live 执行报告（R-07、A-03）。"""
    payload = json.dumps(
        dict(body), ensure_ascii=False, sort_keys=True, indent=2
    ) + "\n"
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"live-report-sha256-{digest}.json"
    path.write_text(payload, encoding="utf-8")
    return path


def _emit_report(report: Mapping[str, object], destination: str) -> None:
    text = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
    if destination == "-":
        print(text)
    else:
        path = Path(destination)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text + "\n", encoding="utf-8")


def _decimal_argument(raw: str, name: str) -> Decimal:
    """命令行金额参数直接进 Decimal，绝不经 float（T-08）。"""
    try:
        value = Decimal(raw)
    except InvalidOperation as exc:
        raise LiveExecutorError(f"参数 {name} 不是合法数值: {raw!r}") from exc
    if not value.is_finite() or value <= 0:
        raise LiveExecutorError(f"参数 {name} 必须为正: {raw!r}")
    return value


def build_parser() -> argparse.ArgumentParser:
    """构造命令行参数定义，形态与 dry-run 执行器对齐。"""
    parser = argparse.ArgumentParser(
        description="live 执行器：授权信封约束下的实盘自动执行链"
    )
    parser.add_argument("--target", type=Path, required=True)
    parser.add_argument(
        "--source-prediction", type=Path, required=True,
        help="由编排侧绑定的来源冻结预测路径",
    )
    parser.add_argument(
        "--source-prediction-sha256", required=True,
        help="编排侧在适配前固定的来源预测 SHA-256",
    )
    parser.add_argument(
        "--target-config", type=Path, default=DEFAULT_PAPER_CONFIG_PATH,
        help="执行目标绑定的版本化执行配置（G-06）",
    )
    parser.add_argument(
        "--rules", type=Path, default=None,
        help="取引ルール快照 JSON；缺省经公开端点拉取",
    )
    parser.add_argument(
        "--reference-price", default=None,
        help="参考价；缺省经公开端点取最新成交价",
    )
    parser.add_argument(
        "--book", type=Path, default=None,
        help="盘口快照 JSON；缺省经公开端点拉取",
    )
    parser.add_argument(
        "--service-status", default=None,
        choices=[status.value for status in ServiceStatus],
        help="服务状态；缺省经公开端点拉取",
    )
    parser.add_argument(
        "--ledger", type=Path, default=None,
        help="意图账本路径；缺省数据根 execution/live/intent_ledger.jsonl",
    )
    parser.add_argument(
        "--breaker-config", type=Path, default=DEFAULT_THRESHOLDS_PATH,
        help="熔断阈值配置路径，与信封逐项取更严（G-06）",
    )
    parser.add_argument(
        "--envelope", type=Path, default=DEFAULT_ENVELOPE_PATH,
        help="授权信封路径（执行链设计第 14 节）",
    )
    parser.add_argument(
        "--max-wait-seconds", type=int, default=DEFAULT_MAX_WAIT_SECONDS,
        help="受理后有界等待秒数，届满撤单（R-01）",
    )
    parser.add_argument("--env-file", type=Path, default=None)
    parser.add_argument(
        "--report", default="-",
        help="报告输出路径，- 表示标准输出（A-03）",
    )
    return parser


def _refusal(status: str, detail: str) -> int:
    """打印拒绝启动的明确状态（不含任何密钥）。"""
    print(json.dumps(
        {"status": status, "detail": detail},
        ensure_ascii=False, sort_keys=True,
    ))
    return EXIT_REFUSED


def run_live_cycle(
    runtime: LiveRuntime,
    plan: DryRunPlan,
    *,
    assets: Sequence[Asset],
    price_observed_at: datetime,
    book: BookSnapshot | None,
    cancel_all: Callable[[], int],
    now: datetime,
) -> tuple[int, dict[str, object]]:
    """执行一轮门禁判定与发送闭环，返回退出码与报告片段。

    裁决语义：skip 与 pause 为零写成功轮（退出 0）；halt 停机
    （退出 2）；trip 执行 on_trip（退出 1）；reject 记录后按零写
    轮结束（退出 0）。
    """
    refresh_baselines(
        runtime, assets=assets,
        reference_price=plan.reference_price, now=now,
    )
    decision = evaluate_gates_for_plan(
        runtime, plan,
        assets=assets,
        price_observed_at=price_observed_at,
        book=book,
        decision_time=plan.artifact.decision_time,
        now=now,
    )
    fragment: dict[str, object] = {
        "gates": gate_records_payload(decision),
        "gate_verdict": decision.verdict,
        "gate_reason": decision.reason,
        "skip_reason": plan.skip_reason,
    }
    if decision.verdict != VERDICT_ALLOW:
        if decision.verdict == VERDICT_TRIP:
            fragment["trip"] = execute_on_trip(
                runtime,
                reason=decision.reason or "信封门禁熔断",
                reference_price=plan.reference_price,
                cancel_all=cancel_all,
                now=now,
            )
            runtime.save_state()
            return EXIT_ANOMALY, fragment
        runtime.save_state()
        if decision.verdict == VERDICT_HALT:
            return EXIT_REFUSED, fragment
        # 跳过、暂停与拒单均为零写轮
        return EXIT_OK, fragment
    runtime.save_state()
    if plan.proposal is None:
        return EXIT_OK, fragment
    outcome = execute_live_order(runtime, plan, now=now)
    fragment["intent"] = {
        "intent_id": outcome.intent.intent_id,
        "correlation_id": outcome.intent.correlation_id,
        "state": outcome.dispatch.state.value,
        "order_id": outcome.dispatch.order_id,
        "reason": outcome.dispatch.reason,
        "consumed_write_budget": outcome.dispatch.consumed_write_budget,
    }
    fragment["resolution"] = outcome.resolution
    fragment["cancel_requested"] = outcome.cancel_requested
    final = outcome.final_order
    fragment["final_order_status"] = (
        None if final is None else final.status.value
    )
    fragment["executed_size"] = (
        None if final is None else format(final.executed_size, "f")
    )
    if runtime.breaker.state is BreakerState.TRIPPED:
        # T-11 超限等编排内触发
        fragment["trip"] = execute_on_trip(
            runtime,
            reason=runtime.breaker.trip_reason or "熔断已触发",
            reference_price=plan.reference_price,
            cancel_all=cancel_all,
            now=now,
        )
        runtime.save_state()
        return EXIT_ANOMALY, fragment
    if (
        outcome.terminal
        and final is not None
        and not runtime.state.first_order_cleared
    ):
        cleared, evidence = verify_first_order(runtime, final)
        fragment["first_order_verification"] = evidence
        if cleared:
            # 首单终态对账通过即解除压额（T-12）
            runtime.state = replace(
                runtime.state, first_order_cleared=True
            )
    runtime.save_state()
    return (EXIT_OK if outcome.terminal else EXIT_ANOMALY), fragment


def main(
    argv: Sequence[str] | None = None,
    *,
    moment: datetime | None = None,
) -> int:
    """命令行入口。非 live 配置直接退出码 2（T-04、A-01）。"""
    args = build_parser().parse_args(argv)
    env_file: Path | None = args.env_file
    config = load_config(env_file)
    if config.mode is not RunMode.LIVE:
        # 不打印任何密钥（T-01、A-06）
        return _refusal(
            "not_live",
            "当前不是 live 模式。切换实盘需人工设置 GUVOLU_MODE=live"
            "（T-04、A-01），本入口不会代为切换。",
        )
    now = moment if moment is not None else datetime.now(UTC)
    try:
        envelope = load_envelope(
            Path(args.envelope), whitelist=config.spot_whitelist
        )
    except GuvoluError as exc:
        return _refusal("envelope_invalid", str(exc))
    root = data_root()
    usage = EnvelopeUsage.for_envelope(envelope)
    state_store = EnvelopeStateStore.for_envelope(envelope)
    state = state_store.load()
    if state.tripped_at is not None:
        return _refusal(
            "envelope_tripped",
            f"信封已于 {state.tripped_at.isoformat()} 熔断锁定:"
            f" {state.trip_reason}，停机待人工复核",
        )
    if not envelope.valid_from <= now < envelope.valid_until:
        return _refusal("envelope_expired", "信封不在有效期内，拒绝进入 live")
    if usage.total_jpy() >= envelope.envelope_jpy_total:
        return _refusal(
            "envelope_exhausted",
            f"信封总额已耗尽: 已用 {usage.total_jpy()} JPY /"
            f" {envelope.envelope_jpy_total} JPY，停机复核",
        )
    artifact = load_target_artifact(Path(args.target))
    verify_v2_source_prediction(
        artifact,
        source_prediction_path=Path(args.source_prediction),
        expected_source_sha256=str(args.source_prediction_sha256),
    )
    target_config = load_paper_config(Path(args.target_config))
    validate_live_target(
        artifact,
        runtime_mode=config.mode,
        target_config=target_config,
        envelope=envelope,
        now=now,
    )
    symbol = target_config.symbol
    read_touched: list[str] = []
    public: PublicClient | None = None

    def get_public() -> PublicClient:
        nonlocal public
        if public is None:
            public = PublicClient.from_config(config)
        return public

    rules_arg: Path | None = args.rules
    if rules_arg is not None:
        rule = load_market_rule(rules_arg, symbol)
    else:
        rule = fetch_market_rule(get_public(), symbol)
        read_touched.append(SYMBOLS_ENDPOINT)
    price_arg: str | None = args.reference_price
    if price_arg is not None:
        reference_price = _decimal_argument(price_arg, "--reference-price")
        price_observed_at = now
    else:
        tickers = get_public().ticker(str(symbol))
        read_touched.append(TICKER_ENDPOINT)
        if not tickers:
            raise LiveExecutorError(f"公开端点无品种 {symbol} 的最新レート")
        reference_price = tickers[0].last
        price_observed_at = tickers[0].timestamp
    book: BookSnapshot | None = None
    book_arg: Path | None = args.book
    try:
        if book_arg is not None:
            book = load_book_snapshot_file(
                book_arg, basis=PUBLIC_ORDERBOOK_BASIS
            )
        else:
            book = BookSnapshot.from_orderbook(
                get_public().orderbooks(str(symbol)),
                observed_at=datetime.now(UTC),
                basis=PUBLIC_ORDERBOOK_BASIS,
            )
            read_touched.append(ORDERBOOKS_ENDPOINT)
    except FillModelError:
        # 盘口不可得由门禁按跳过处置
        book = None
    status_arg: str | None = args.service_status
    if status_arg is not None:
        service_status = ServiceStatus(status_arg)
    else:
        service_status = get_public().status()
        read_touched.append(STATUS_ENDPOINT)
    if not allows_new_intent(service_status):
        return _refusal(
            "service_not_open",
            f"服务状态 {service_status.value}，不发写请求（R-03）",
        )
    ledger_arg: Path | None = args.ledger
    ledger_path = (
        ledger_arg if ledger_arg is not None
        else root / LIVE_RELATIVE_DIR / LIVE_LEDGER_NAME
    )
    ledger = IntentLedger(ledger_path)
    thresholds = merged_breaker_thresholds(
        load_breaker_thresholds(Path(args.breaker_config)), envelope
    )
    breaker = CircuitBreaker(thresholds)
    limit_gate = LimitGate(config.limits)
    # 重放当日用量（T-11）
    replay_limit_usage(limit_gate, ledger, moment=now)
    reader = ReadClient.from_config(config)
    trade = TradeClient.from_config(config)
    sender = TradeClientSender(trade)

    def cancel_all() -> int:
        """全量撤单动作（T-07）。"""
        return kill_switch.cancel_all(get_public(), trade)

    runtime = LiveRuntime(
        config=config,
        envelope=envelope,
        usage=usage,
        state_store=state_store,
        state=state,
        ledger=ledger,
        limit_gate=limit_gate,
        breaker=breaker,
        reader=reader,
        sender=sender,
        service_status=service_status,
        rule=rule,
        inflight_dir=root / INFLIGHT_LOCK_RELATIVE_DIR,
        max_wait_seconds=int(args.max_wait_seconds),
        read_touched=read_touched,
    )
    assets_before = reader.assets()
    runtime.read_touched.append(ASSETS_ENDPOINT)
    budget = artifact.risk_budget_jpy
    if budget is None:
        raise LiveExecutorError("live 目标缺少 risk_budget_jpy")
    plan = build_plan(
        artifact,
        rule=rule,
        reference_price=reference_price,
        budget_jpy=budget,
    )
    exit_code, fragment = run_live_cycle(
        runtime, plan,
        assets=assets_before,
        price_observed_at=price_observed_at,
        book=book,
        cancel_all=cancel_all,
        now=now,
    )
    assets_after = reader.assets()
    runtime.read_touched.append(ASSETS_ENDPOINT)
    after_value = (
        _asset_amount(assets_after, _JPY)
        + _asset_amount(assets_after, str(symbol)) * reference_price
    )
    report: dict[str, object] = {
        "schema_version": 1,
        "kind": "live_execution_report",
        "mode": config.mode.value,
        "generated_at": datetime.now(UTC).isoformat(),
        "service_status": service_status.value,
        "envelope": {
            "path": str(envelope.path),
            "sha256": envelope.sha256,
            "on_trip": envelope.on_trip.value,
            "used_total_jpy": format(usage.total_jpy(), "f"),
            "envelope_jpy_total": format(envelope.envelope_jpy_total, "f"),
            "first_order_cleared": runtime.state.first_order_cleared,
        },
        "artifact": {
            "path": str(artifact.path),
            "sha256": artifact.sha256,
            "run_id": artifact.run_id,
            "decision_time": artifact.decision_time.isoformat(),
            "market_id": artifact.market_id,
            "aggregate_target": artifact.aggregate_target,
        },
        "budget_jpy": format(budget, "f"),
        "reference_price": format(reference_price, "f"),
        "assets_before": _assets_view(assets_before),
        "assets_after": _assets_view(assets_after),
        "valuation_after_jpy": format(after_value, "f"),
        "endpoints": {
            "read_touched": list(runtime.read_touched),
            "write_planned": (
                [] if plan.proposal is None
                else [ORDER_ENDPOINT, CANCEL_ENDPOINT]
            ),
            "write_touched": list(runtime.write_touched),
        },
        "ledger_path": str(ledger_path),
        "usage_path": str(usage.path),
        "state_path": str(state_store.path),
        "exit_code": exit_code,
    }
    report.update(fragment)
    report_path = write_live_report(report, root / LIVE_RELATIVE_DIR)
    report["report_path"] = str(report_path)
    _emit_report(report, str(args.report))
    return exit_code
