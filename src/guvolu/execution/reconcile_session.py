"""对账会话：快照对账、超时处理与差分决策的单轮编排。

单命令跑一轮 dry-run 会话（T-04）：先应用注入的 WS 事实序列，
再以 REST 全量快照裁决（R-08、C-10、T-03），自动处置超时意图
（T-06），随后按目标与账本推算持仓做差分决策（持仓只来自
READ_ONLY 成交事实，T-03），经第 5 节全量闸门进入发送边界。
熔断动作已接入紧急停止全撤（T-07）。报告列明触碰端点（A-03）。
命令行入口见 scripts/run_reconcile_session.py。周期与退避参数
为 TBD-07 提案值，载于版本化配置（G-06）。
"""
from __future__ import annotations

import argparse
import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from guvolu.api.public_client import PublicClient
from guvolu.api.read_client import ReadClient
from guvolu.api.trade_client import TradeClient
from guvolu.api.ws_private import parse_private_message
from guvolu.data.intent_ledger import LEDGER_RELATIVE_PATH, IntentLedger
from guvolu.data.paths import data_root
from guvolu.domain.config import Config, load_config
from guvolu.domain.enums import ExecutionType, RunMode, ServiceStatus
from guvolu.domain.errors import ConfigError
from guvolu.domain.ids import new_correlation_id, new_intent_id
from guvolu.domain.intent import (
    LOCAL_TERMINAL_STATES,
    IntentState,
    OrderIntent,
)
from guvolu.domain.symbols import SpotSymbol
from guvolu.execution.conversion import (
    DeltaDecision,
    MarketRule,
    convert_target_to_delta_order,
)
from guvolu.execution.dispatch import DispatchResult, dispatch_order_intent
from guvolu.execution.dry_run_executor import (
    ORDER_ENDPOINT,
    ExecutorError,
)
from guvolu.execution.dual_reconcile import (
    ORDER_LOOKUP_ENDPOINT,
    SNAPSHOT_READ_ENDPOINTS,
    AssetReconciliation,
    PrivateEvent,
    ReadOnlySnapshotReader,
    SnapshotMode,
    SnapshotReconcileResult,
    WsApplyOutcome,
    apply_private_event,
    reconcile_snapshot,
    take_snapshot,
)
from guvolu.execution.emergency_stop import (
    EMERGENCY_READ_ENDPOINT,
    EMERGENCY_WRITE_ENDPOINT,
    EmergencyStopAction,
    arm_emergency_stop,
)
from guvolu.execution.order_state import OrderStateStore
from guvolu.execution.timeout_scheduler import (
    BackoffPolicy,
    TimeoutQueryOutcome,
    TimeoutQueryScheduler,
)
from guvolu.execution.trade_sender import TradeClientSender
from guvolu.risk.circuit_breaker import (
    DEFAULT_THRESHOLDS_PATH,
    BreakerState,
    CircuitBreaker,
    load_breaker_thresholds,
)
from guvolu.risk.limits import LimitGate

# 缺省会话配置相对路径（G-06）
SESSION_CONFIG_PATH = Path("config") / "reconcile_session.json"
# 成交回填触碰的只读端点（A-03）
EXECUTION_BACKFILL_ENDPOINT = "GET /v1/executions"


@dataclass(frozen=True, slots=True)
class SessionSettings:
    """会话参数，数值为 TBD-07 提案（G-06）。"""

    schema_version: int
    snapshot_interval_seconds: int
    timeout_query_initial_seconds: int
    timeout_query_max_seconds: int
    no_trade_band: Decimal


def load_session_settings(path: Path) -> SessionSettings:
    """从版本化配置装载会话参数，缺失或非法即配置错误。"""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ConfigError(f"会话配置不存在: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ConfigError(f"会话配置不是合法 JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise ConfigError("会话配置根必须是对象")

    def positive_int(key: str) -> int:
        value = payload.get(key)
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise ConfigError(f"会话配置 {key} 必须为正整数")
        return value

    band_raw = payload.get("no_trade_band")
    # 比例以字符串承载（D-07）
    if not isinstance(band_raw, str):
        raise ConfigError("会话配置 no_trade_band 必须为字符串数值")
    try:
        band = Decimal(band_raw)
    except InvalidOperation as exc:
        raise ConfigError("会话配置 no_trade_band 不是合法数值") from exc
    if band < 0 or band >= 1:
        raise ConfigError("会话配置 no_trade_band 必须在 [0, 1) 内")
    settings = SessionSettings(
        schema_version=positive_int("schema_version"),
        snapshot_interval_seconds=positive_int("snapshot_interval_seconds"),
        timeout_query_initial_seconds=positive_int(
            "timeout_query_initial_seconds"
        ),
        timeout_query_max_seconds=positive_int("timeout_query_max_seconds"),
        no_trade_band=band,
    )
    if settings.timeout_query_max_seconds < settings.timeout_query_initial_seconds:
        raise ConfigError("会话配置超时退避上限低于初始值")
    return settings


@dataclass(frozen=True, slots=True)
class SnapshotOutcome:
    """一轮快照对账连同资产核对的结果。"""

    reconcile: SnapshotReconcileResult
    asset_unexplained_jpy: Decimal | None
    asset_total_jpy: Decimal | None
    backfilled_orders: int


class ReconcileSession:
    """双通道对账会话。REST 是唯一裁决通道（T-03）。"""

    def __init__(
        self,
        *,
        ledger: IntentLedger,
        reader: ReadOnlySnapshotReader,
        breaker: CircuitBreaker,
        symbol: SpotSymbol,
        policy: BackoffPolicy,
    ) -> None:
        self._ledger = ledger
        self._reader = reader
        self._breaker = breaker
        self._symbol = symbol
        self._store = OrderStateStore()
        self._scheduler = TimeoutQueryScheduler(policy)
        self._assets = AssetReconciliation()
        self._snapshot_taken = False
        self._read_endpoints: list[str] = []

    @property
    def store(self) -> OrderStateStore:
        """对账域状态存储。"""
        return self._store

    @property
    def scheduler(self) -> TimeoutQueryScheduler:
        """超时查询调度器。"""
        return self._scheduler

    def read_endpoints(self) -> tuple[str, ...]:
        """本会话触碰过的只读端点（A-03）。"""
        return tuple(self._read_endpoints)

    def _touch(self, endpoint: str) -> None:
        """登记触碰端点，保持首次触碰顺序。"""
        if endpoint not in self._read_endpoints:
            self._read_endpoints.append(endpoint)

    def _is_ours(self, order_id: int) -> bool:
        """委托是否属本系统（账本已映射，T-05）。"""
        return self._ledger.intent_id_for_order(order_id) is not None

    def ingest_ws_events(
        self, events: Iterable[PrivateEvent], now: datetime | None = None
    ) -> tuple[WsApplyOutcome, ...]:
        """应用 WS 事件序列，驱动意图与委托状态更新。"""
        moment = now if now is not None else datetime.now(UTC)
        return tuple(
            apply_private_event(
                event, store=self._store, ledger=self._ledger, moment=moment
            )
            for event in events
        )

    def snapshot_round(
        self,
        now: datetime | None = None,
        mode: SnapshotMode | None = None,
    ) -> SnapshotOutcome:
        """拉取快照并裁决；缺省首轮基线，其后稳态计数。"""
        moment = now if now is not None else datetime.now(UTC)
        if mode is None:
            mode = (
                SnapshotMode.AUDIT
                if self._snapshot_taken
                else SnapshotMode.BASELINE
            )
        snapshot = take_snapshot(self._reader, str(self._symbol), moment)
        for endpoint in SNAPSHOT_READ_ENDPOINTS:
            self._touch(endpoint)
        result = reconcile_snapshot(
            snapshot,
            store=self._store,
            ledger=self._ledger,
            breaker=self._breaker,
            reader=self._reader,
            mode=mode,
        )
        if result.order_lookup_used:
            self._touch(ORDER_LOOKUP_ENDPOINT)
        self._snapshot_taken = True
        backfilled = 0
        unexplained: Decimal | None = None
        total: Decimal | None = None
        if not self._assets.initialized:
            backfilled = self._backfill_mapped_executions()
            self._assets.initialize(
                snapshot.assets, self._store.execution_ids()
            )
        else:
            for fact in self._store.executions():
                if self._is_ours(fact.order_id):
                    self._assets.explain(fact)
            unexplained, total = self._assets.evaluate(snapshot.assets)
            self._breaker.record_asset_deviation(unexplained, total)
        return SnapshotOutcome(
            reconcile=result,
            asset_unexplained_jpy=unexplained,
            asset_total_jpy=total,
            backfilled_orders=backfilled,
        )

    def on_ws_reconnect(
        self, now: datetime | None = None
    ) -> SnapshotOutcome:
        """重连后强制全量快照对账，修复增量缺口（C-10）。"""
        return self.snapshot_round(now, mode=SnapshotMode.REALIGN)

    def _backfill_mapped_executions(self) -> int:
        """基线时按账本映射委托回填成交事实（T-03）。"""
        mapped = [
            order_id
            for intent_id in self._ledger.intent_ids()
            if (order_id := self._ledger.order_id_of(intent_id)) is not None
        ]
        if not mapped:
            return 0
        self._touch(EXECUTION_BACKFILL_ENDPOINT)
        count = 0
        for order_id in mapped:
            for execution in self._reader.executions(order_id=order_id):
                self._store.merge_rest_execution(execution)
            count += 1
        return count

    def resolve_timeouts(
        self, now: datetime | None = None
    ) -> tuple[TimeoutQueryOutcome, ...]:
        """自动处置超时意图，退避至终态（T-06）。"""
        outcomes = self._scheduler.run_due(
            ledger=self._ledger, reader=self._reader, now=now
        )
        if outcomes:
            self._touch("GET /v1/activeOrders")
            self._touch("GET /v1/latestExecutions")
        return outcomes

    def position_size(self) -> Decimal:
        """账本推算持仓：只依据 READ_ONLY 成交事实（T-03）。"""
        return self._store.position_size(str(self._symbol), self._is_ours)

    def decide_delta(
        self,
        target: float,
        *,
        rule: MarketRule,
        reference_price: Decimal,
        budget_jpy: Decimal,
        no_trade_band: Decimal,
    ) -> DeltaDecision:
        """目标与推算持仓的差分折算（G-05 闸门）。"""
        return convert_target_to_delta_order(
            target,
            position_size=self.position_size(),
            budget_jpy=budget_jpy,
            reference_price=reference_price,
            rule=rule,
            no_trade_band=no_trade_band,
        )


def execute_delta(
    decision: DeltaDecision,
    *,
    ledger: IntentLedger,
    limit_gate: LimitGate,
    breaker: CircuitBreaker,
    service_status: ServiceStatus,
    whitelist: frozenset[SpotSymbol],
    sender: TradeClientSender,
    moment: datetime | None = None,
) -> tuple[OrderIntent, DispatchResult] | None:
    """把差分决策落为限价意图并经发送编排执行。"""
    if decision.proposal is None:
        return None
    now = moment if moment is not None else datetime.now(UTC)
    intent = OrderIntent(
        intent_id=new_intent_id(),
        correlation_id=new_correlation_id(),
        symbol=decision.proposal.symbol,
        side=decision.proposal.side,
        execution_type=ExecutionType.LIMIT,
        size=decision.proposal.size,
        price=decision.proposal.price,
        time_in_force=None,
        created_at=now,
    )
    result = dispatch_order_intent(
        intent,
        ledger=ledger,
        limit_gate=limit_gate,
        breaker=breaker,
        service_status=service_status,
        whitelist=whitelist,
        sender=sender,
        moment=now,
    )
    return intent, result


def timeout_outcome_payload(
    outcome: TimeoutQueryOutcome,
) -> dict[str, object]:
    """超时查询结果的报告形态。"""
    return {
        "intent_id": outcome.intent_id,
        "disposition": outcome.disposition,
        "state": None if outcome.state is None else outcome.state.value,
        "order_id": outcome.order_id,
        "attempt": outcome.attempt,
        "next_attempt_at": (
            None
            if outcome.next_attempt_at is None
            else outcome.next_attempt_at.isoformat()
        ),
        "detail": outcome.detail,
    }


def snapshot_payload(outcome: SnapshotOutcome) -> dict[str, object]:
    """快照对账结果的报告形态，金额落字符串（D-07）。"""
    reconcile = outcome.reconcile
    return {
        "mode": reconcile.mode.value,
        "mismatches": [
            {
                "kind": item.kind,
                "order_id": item.order_id,
                "detail": item.detail,
            }
            for item in reconcile.mismatches
        ],
        "counted_into_breaker": reconcile.counted_into_breaker,
        "new_execution_count": reconcile.new_execution_count,
        "order_lookup_used": reconcile.order_lookup_used,
        "backfilled_orders": outcome.backfilled_orders,
        "asset_unexplained_jpy": (
            None
            if outcome.asset_unexplained_jpy is None
            else format(outcome.asset_unexplained_jpy, "f")
        ),
        "asset_total_jpy": (
            None
            if outcome.asset_total_jpy is None
            else format(outcome.asset_total_jpy, "f")
        ),
    }


def delta_payload(
    delta: DeltaDecision,
    *,
    target: float | None,
    no_trade_band: Decimal,
) -> dict[str, object]:
    """差分决策的报告形态，金额落字符串（D-07）。"""
    proposal_payload: dict[str, str] | None = None
    if delta.proposal is not None:
        proposal_payload = {
            "symbol": str(delta.proposal.symbol),
            "side": delta.proposal.side.value,
            "size": format(delta.proposal.size, "f"),
            "price": format(delta.proposal.price, "f"),
            "notional_jpy": format(delta.proposal.notional_jpy, "f"),
        }
    return {
        "target": target,
        "desired_size": format(delta.desired_size, "f"),
        "position_size": format(delta.position_size, "f"),
        "delta_size": format(delta.delta_size, "f"),
        "no_trade_band": format(no_trade_band, "f"),
        "skip_reason": delta.skip_reason,
        "proposal": proposal_payload,
    }


def intent_payload(
    outcome: tuple[OrderIntent, DispatchResult],
) -> dict[str, object]:
    """一次编排结果的报告形态。"""
    intent, result = outcome
    return {
        "intent_id": intent.intent_id,
        "correlation_id": intent.correlation_id,
        "state": result.state.value,
        "order_id": result.order_id,
        "reason": result.reason,
    }


def render_session_report(
    *,
    mode: RunMode,
    service_status: ServiceStatus,
    symbol: SpotSymbol,
    settings: SessionSettings,
    interrupted: Sequence[str],
    ws_outcomes: Sequence[WsApplyOutcome],
    snapshot: SnapshotOutcome,
    timeouts: Sequence[TimeoutQueryOutcome],
    position_size: Decimal,
    delta: DeltaDecision | None,
    target: float | None,
    no_trade_band: Decimal,
    outcome: tuple[OrderIntent, DispatchResult] | None,
    breaker: CircuitBreaker,
    emergency: EmergencyStopAction,
    read_endpoints: Sequence[str],
    ledger_path: Path,
) -> dict[str, object]:
    """生成会话端点报告（A-03），金额落字符串（D-07）。"""
    reads = list(read_endpoints)
    write_planned: list[str] = []
    write_touched: list[str] = []
    intent_body: dict[str, object] | None = None
    if delta is not None and delta.proposal is not None:
        write_planned.append(ORDER_ENDPOINT)
    if outcome is not None:
        intent_body = intent_payload(outcome)
        if outcome[1].state not in LOCAL_TERMINAL_STATES:
            write_touched.append(ORDER_ENDPOINT)
    if emergency.records:
        # 全撤动作已真实触碰端点（T-07）
        if EMERGENCY_READ_ENDPOINT not in reads:
            reads.append(EMERGENCY_READ_ENDPOINT)
        write_touched.append(EMERGENCY_WRITE_ENDPOINT)
    delta_body: dict[str, object] | None = None
    if delta is not None:
        delta_body = delta_payload(
            delta, target=target, no_trade_band=no_trade_band
        )
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "mode": mode.value,
        "service_status": service_status.value,
        "symbol": str(symbol),
        "recovery": {"interrupted_marked": list(interrupted)},
        "ws_channel": {
            "events_applied": len(ws_outcomes),
            "accepted_intents": [
                item.accepted_intent_id
                for item in ws_outcomes
                if item.accepted_intent_id is not None
            ],
            "ignored": sum(
                1 for item in ws_outcomes if item.kind == "ignored"
            ),
        },
        "snapshot": snapshot_payload(snapshot),
        "timeouts": [timeout_outcome_payload(item) for item in timeouts],
        "position": {
            "size": format(position_size, "f"),
            "basis": "READ_ONLY 成交事实",
        },
        "delta": delta_body,
        "intent": intent_body,
        "breaker": {
            "state": breaker.state.value,
            "consecutive_failures": breaker.consecutive_failures,
            "trip_reason": breaker.trip_reason,
            "emergency_stop": [
                {
                    "at": record.at.isoformat(),
                    "reason": record.reason,
                    "exit_code": record.exit_code,
                    "error": record.error,
                }
                for record in emergency.records
            ],
        },
        "endpoints": {
            "read_touched": reads,
            "write_planned": write_planned,
            "write_touched": write_touched,
        },
        "settings": {
            "schema_version": settings.schema_version,
            "snapshot_interval_seconds": settings.snapshot_interval_seconds,
            "timeout_query_initial_seconds": (
                settings.timeout_query_initial_seconds
            ),
            "timeout_query_max_seconds": settings.timeout_query_max_seconds,
            "no_trade_band": format(settings.no_trade_band, "f"),
        },
        "ledger_path": str(ledger_path),
    }


def load_ws_events(path: Path) -> tuple[PrivateEvent, ...]:
    """从 JSONL 文件装载注入的私有事件帧序列。"""
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise ExecutorError(f"WS 事件文件不存在: {path}") from exc
    return tuple(
        parse_private_message(line)
        for line in text.splitlines()
        if line.strip()
    )


def decimal_argument(raw: str, name: str) -> Decimal:
    """命令行数值参数直接进 Decimal，绝不经 float（T-08）。"""
    try:
        value = Decimal(raw)
    except InvalidOperation as exc:
        raise ExecutorError(f"参数 {name} 不是合法数值: {raw!r}") from exc
    return value


def build_parser() -> argparse.ArgumentParser:
    """构造命令行参数定义。"""
    parser = argparse.ArgumentParser(
        description="对账会话：快照对账、超时处理与差分决策的单轮 dry-run"
    )
    parser.add_argument("--symbol", default="BTC", help="现物品种，缺省 BTC")
    parser.add_argument(
        "--target", type=Path, default=None,
        help="target-position 制品路径；缺省跳过差分决策",
    )
    parser.add_argument(
        "--budget-jpy", default="500", help="名义预算 JPY，缺省 500"
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
        "--service-status", default=None,
        choices=[status.value for status in ServiceStatus],
        help="服务状态；缺省经公开端点拉取",
    )
    parser.add_argument(
        "--no-trade-band", default=None,
        help="不交易带比例；缺省取会话配置",
    )
    parser.add_argument(
        "--ws-events", type=Path, default=None,
        help="注入的私有事件帧 JSONL，离线驱动 WS 通道",
    )
    parser.add_argument(
        "--snapshot-mode", default=None,
        choices=[mode.value for mode in SnapshotMode],
        help="快照模式；缺省注入事件时稳态计数，否则基线",
    )
    parser.add_argument(
        "--ledger", type=Path, default=None,
        help="意图账本路径；缺省数据根下 execution/intent_ledger.jsonl",
    )
    parser.add_argument(
        "--breaker-config", type=Path, default=DEFAULT_THRESHOLDS_PATH,
        help="熔断阈值配置路径（G-06）",
    )
    parser.add_argument(
        "--session-config", type=Path, default=SESSION_CONFIG_PATH,
        help="会话参数配置路径（G-06、TBD-07）",
    )
    parser.add_argument(
        "--env-file", type=Path, default=None, help="配置文件路径，缺省 .env"
    )
    parser.add_argument(
        "--report", default="-",
        help="报告输出路径，- 表示标准输出（A-03）",
    )
    return parser


def _emit(report: dict[str, object], destination: str) -> None:
    """把报告 JSON 写到标准输出或指定文件。"""
    text = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
    if destination == "-":
        print(text)
    else:
        Path(destination).write_text(text + "\n", encoding="utf-8")


def _resolve_service_status(
    args: argparse.Namespace,
    config: Config,
    session: ReconcileSession,
) -> ServiceStatus:
    """取服务状态；无显式注入时只触碰公开端点。"""
    public: PublicClient | None = None

    def get_public() -> PublicClient:
        nonlocal public
        if public is None:
            public = PublicClient.from_config(config)
        return public

    status_arg: str | None = args.service_status
    if status_arg is not None:
        return ServiceStatus(status_arg)
    service_status = get_public().status()
    session._touch("GET /v1/status")
    return service_status


def main(argv: Sequence[str] | None = None) -> int:
    """命令行入口；当前只允许无目标的 dry-run 对账会话。"""
    args = build_parser().parse_args(argv)
    if args.target is not None:
        raise ExecutorError(
            "对账会话的目标驱动发送尚未获准；请使用独立 dry-run/paper 入口"
        )
    env_file: Path | None = args.env_file
    config = load_config(env_file)
    if config.mode is not RunMode.DRY_RUN:
        raise ExecutorError("对账会话当前只允许 dry-run 模式")
    settings = load_session_settings(args.session_config)
    ledger_arg: Path | None = args.ledger
    ledger_path = (
        ledger_arg if ledger_arg is not None
        else data_root() / LEDGER_RELATIVE_PATH
    )
    ledger = IntentLedger(ledger_path)
    # 恢复：中断的发送转入超时态（T-06）
    interrupted = ledger.mark_interrupted_sends()
    breaker = CircuitBreaker(load_breaker_thresholds(args.breaker_config))
    public = PublicClient.from_config(config)
    trade = TradeClient.from_config(config)
    emergency = arm_emergency_stop(breaker, public, trade)
    reader = ReadClient.from_config(config)
    symbol = SpotSymbol(str(args.symbol))
    policy = BackoffPolicy(
        initial_seconds=float(settings.timeout_query_initial_seconds),
        max_seconds=float(settings.timeout_query_max_seconds),
    )
    session = ReconcileSession(
        ledger=ledger,
        reader=reader,
        breaker=breaker,
        symbol=symbol,
        policy=policy,
    )
    service_status = _resolve_service_status(args, config, session)
    now = datetime.now(UTC)
    ws_events_arg: Path | None = args.ws_events
    ws_outcomes: tuple[WsApplyOutcome, ...] = ()
    if ws_events_arg is not None:
        ws_outcomes = session.ingest_ws_events(
            load_ws_events(ws_events_arg), now
        )
    mode_arg: str | None = args.snapshot_mode
    if mode_arg is not None:
        mode: SnapshotMode | None = SnapshotMode(mode_arg)
    elif ws_outcomes:
        mode = SnapshotMode.AUDIT
    else:
        mode = None
    snapshot = session.snapshot_round(now, mode=mode)
    timeouts = session.resolve_timeouts(now)
    band_arg: str | None = args.no_trade_band
    if band_arg is not None:
        no_trade_band = decimal_argument(band_arg, "--no-trade-band")
    else:
        no_trade_band = settings.no_trade_band
    delta: DeltaDecision | None = None
    target_value: float | None = None
    outcome: tuple[OrderIntent, DispatchResult] | None = None
    report = render_session_report(
        mode=config.mode,
        service_status=service_status,
        symbol=symbol,
        settings=settings,
        interrupted=interrupted,
        ws_outcomes=ws_outcomes,
        snapshot=snapshot,
        timeouts=timeouts,
        position_size=session.position_size(),
        delta=delta,
        target=target_value,
        no_trade_band=no_trade_band,
        outcome=outcome,
        breaker=breaker,
        emergency=emergency,
        read_endpoints=session.read_endpoints(),
        ledger_path=ledger_path,
    )
    _emit(report, str(args.report))
    pending = any(
        item.disposition in ("ambiguous", "query_error") for item in timeouts
    )
    tripped = breaker.state is BreakerState.TRIPPED
    return 1 if pending or tripped else 0
