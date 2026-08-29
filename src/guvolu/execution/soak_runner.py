"""常驻浸泡进程：WS 常连与周期对账循环的驻留编排（阶段四）。

复用 execution.reconcile_session 的单轮逻辑，把「WS 事实消费、
快照对账、超时处理、差分决策」编排为长期驻留循环，全程模拟
运行（T-04），收到 live 配置直接拒绝启动（切换实盘属 A-01，
本阶段不提供入口）。私有 WS 经 api.ws_private 的令牌生命周期
常连消费 orderEvents 与 executionEvents，断线由客户端按退避
重连，重连后的下一轮强制全量快照对账（C-10、R-08）。目标制品
每轮重读，更新即生效，缺省目标为零。停止有控制台中断与停止
标记文件双通道，停止前完成当轮并写终态 checkpoint；每轮报告
追加 JSONL 持续落盘并累计触碰端点（A-03）；心跳文件供外部
判活。稳态轮记录双通道时延竞态观测（设计文档第 12 节），不
改变裁决与熔断计数逻辑。命令行入口见
scripts/run_execution_soak.py。
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sys
from collections.abc import (
    AsyncIterator,
    Awaitable,
    Callable,
    Iterable,
    Mapping,
    Sequence,
)
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from types import FrameType
from typing import Protocol

from guvolu.api.public_client import PublicClient
from guvolu.api.read_client import ReadClient
from guvolu.api.trade_client import TradeClient
from guvolu.api.transport import PrivateTransport, RateLimiter
from guvolu.api.ws_private import (
    PrivateWsClient,
    create_ws_token,
    keepalive,
    revoke_ws_token,
)
from guvolu.data.durable_io import atomic_write_text, durable_append_bytes
from guvolu.data.intent_ledger import LEDGER_RELATIVE_PATH, IntentLedger
from guvolu.data.paths import data_root
from guvolu.domain.config import load_config
from guvolu.domain.enums import RunMode, ServiceStatus, WsChannel
from guvolu.domain.errors import GuvoluError
from guvolu.domain.intent import (
    LOCAL_TERMINAL_STATES,
    IntentState,
    OrderIntent,
)
from guvolu.domain.models import Asset, Execution, Order
from guvolu.domain.symbols import SpotSymbol
from guvolu.execution.conversion import DeltaDecision, MarketRule
from guvolu.execution.dispatch import DispatchResult
from guvolu.execution.dry_run_executor import (
    ORDER_ENDPOINT,
    fetch_market_rule,
    load_market_rule,
)
from guvolu.execution.dual_reconcile import (
    PrivateEvent,
    ReadOnlySnapshotReader,
    SnapshotMode,
    WsApplyOutcome,
)
from guvolu.execution.emergency_stop import (
    EMERGENCY_READ_ENDPOINT,
    EMERGENCY_WRITE_ENDPOINT,
    EmergencyStopAction,
    arm_emergency_stop,
)
from guvolu.execution.order_state import OrderView
from guvolu.execution.reconcile_session import (
    SESSION_CONFIG_PATH,
    ReconcileSession,
    decimal_argument,
    delta_payload,
    execute_delta,
    intent_payload,
    load_session_settings,
    snapshot_payload,
    timeout_outcome_payload,
)
from guvolu.execution.timeout_scheduler import BackoffPolicy
from guvolu.execution.trade_sender import TradeClientSender
from guvolu.risk.circuit_breaker import (
    DEFAULT_THRESHOLDS_PATH,
    BreakerState,
    CircuitBreaker,
    load_breaker_thresholds,
)
from guvolu.risk.limits import LimitGate

# 浸泡落盘的 schema 版本
SOAK_SCHEMA_VERSION = 1
# 数据根下的缺省落盘位置
REPORT_RELATIVE_PATH = Path("execution") / "soak_report.jsonl"
CHECKPOINT_RELATIVE_PATH = Path("execution") / "soak_checkpoint.json"
HEARTBEAT_RELATIVE_PATH = Path("execution") / "soak_heartbeat.json"
STOP_FILE_RELATIVE_PATH = Path("execution") / "soak.stop"
# 令牌六十分钟有效，提前延长
TOKEN_KEEPALIVE_SECONDS = 1500.0
# 停止轮询与心跳节流间隔
STOP_POLL_SECONDS = 1.0
HEARTBEAT_MIN_INTERVAL_SECONDS = 10.0
# 连续轮次错误上限
ROUND_ERROR_LIMIT = 3
# 令牌生命周期端点（A-03）
WS_AUTH_CREATE_ENDPOINT = "POST /v1/ws-auth"
WS_AUTH_EXTEND_ENDPOINT = "PUT /v1/ws-auth"
WS_AUTH_REVOKE_ENDPOINT = "DELETE /v1/ws-auth"
# 竞态观测的三个分类键
RACE_WS_ONLY = "ws_seen_rest_missing"
RACE_REST_ONLY = "rest_seen_ws_missing"
RACE_REST_FIRST_EXECUTION = "rest_first_execution"
_RACE_CATEGORIES = (RACE_WS_ONLY, RACE_REST_ONLY, RACE_REST_FIRST_EXECUTION)


class SoakError(GuvoluError):
    """浸泡进程输入非法或启动条件不满足。"""


def ensure_dry_run(mode: RunMode) -> None:
    """断言模拟运行；live 配置拒绝启动（T-04、A-01）。"""
    if mode is not RunMode.DRY_RUN:
        raise SoakError(
            "浸泡进程仅限模拟运行，live 配置拒绝启动（T-04）"
        )


class RecordingSnapshotReader:
    """只读来源包装：透传查询并留存最近一次快照行。

    留存的挂单与最新成交行供竞态观测使用（设计文档第 12 节），
    不改变任何查询语义。
    """

    def __init__(self, inner: ReadOnlySnapshotReader) -> None:
        self._inner = inner
        self.last_active_orders: tuple[Order, ...] = ()
        self.last_latest_executions: tuple[Execution, ...] = ()

    def active_orders(
        self, symbol: str, page: int | None = None, count: int | None = None
    ) -> tuple[Order, ...]:
        """透传挂单一览并留存结果。"""
        result = self._inner.active_orders(symbol, page, count)
        self.last_active_orders = result
        return result

    def latest_executions(
        self, symbol: str, page: int | None = None, count: int | None = None
    ) -> tuple[Execution, ...]:
        """透传最新成交一览并留存结果。"""
        result = self._inner.latest_executions(symbol, page, count)
        self.last_latest_executions = result
        return result

    def orders(self, order_ids: Sequence[int]) -> tuple[Order, ...]:
        """透传委托查询。"""
        return self._inner.orders(order_ids)

    def executions(
        self,
        order_id: int | None = None,
        execution_ids: Sequence[int] | None = None,
    ) -> tuple[Execution, ...]:
        """透传成交查询。"""
        return self._inner.executions(order_id, execution_ids)

    def assets(self) -> tuple[Asset, ...]:
        """透传資産残高查询。"""
        return self._inner.assets()


class _DelayAggregate:
    """时延样本聚合，单位为秒。"""

    __slots__ = ("count", "total", "minimum", "maximum")

    def __init__(self) -> None:
        self.count = 0
        self.total = 0.0
        self.minimum: float | None = None
        self.maximum: float | None = None

    def add(self, seconds: float) -> None:
        """记入一个样本。"""
        self.count += 1
        self.total += seconds
        if self.minimum is None or seconds < self.minimum:
            self.minimum = seconds
        if self.maximum is None or seconds > self.maximum:
            self.maximum = seconds

    def payload(self) -> dict[str, object]:
        """聚合结果的报告形态。"""
        if self.count == 0:
            return {
                "count": 0,
                "min_seconds": None,
                "mean_seconds": None,
                "max_seconds": None,
            }
        return {
            "count": self.count,
            "min_seconds": self.minimum,
            "mean_seconds": self.total / self.count,
            "max_seconds": self.maximum,
        }


class ChannelRaceObserver:
    """双通道时延竞态观测：分类计数与时间差分布摘要。

    只观测不裁决（设计文档第 12 节）：稳态快照轮把「WS 已见但
    REST 未含」「REST 已含但 WS 未见」的委托与「REST 先见」的
    成交分类计数，时间差取观测时点与事实时戳之差。裁决与熔断
    计数逻辑不经本类。
    """

    def __init__(self) -> None:
        self._cumulative = {
            key: _DelayAggregate() for key in _RACE_CATEGORIES
        }
        self._audit_rounds = 0

    def observe(
        self,
        *,
        pre_orders: Mapping[int, OrderView],
        pre_execution_ids: frozenset[int],
        symbol: str,
        rest_active: Sequence[Order],
        rest_executions: Sequence[Execution],
        at: datetime,
    ) -> dict[str, object]:
        """按裁决前视图与 REST 快照行分类计数，返回本轮摘要。"""
        current = {key: _DelayAggregate() for key in _RACE_CATEGORIES}

        def record(key: str, observed: datetime) -> None:
            seconds = (at - observed).total_seconds()
            current[key].add(seconds)
            self._cumulative[key].add(seconds)

        rest_ids = {order.order_id for order in rest_active}
        for view in pre_orders.values():
            if (
                view.symbol == symbol
                and view.is_active
                and view.order_id not in rest_ids
            ):
                record(RACE_WS_ONLY, view.timestamp)
        for order in rest_active:
            if order.order_id not in pre_orders:
                record(RACE_REST_ONLY, order.timestamp)
        for execution in rest_executions:
            if execution.execution_id not in pre_execution_ids:
                record(RACE_REST_FIRST_EXECUTION, execution.timestamp)
        self._audit_rounds += 1
        return {
            key: current[key].payload() for key in _RACE_CATEGORIES
        }

    def cumulative_payload(self) -> dict[str, object]:
        """累计摘要，含已观测的稳态轮数。"""
        body: dict[str, object] = {"audit_rounds": self._audit_rounds}
        for key in _RACE_CATEGORIES:
            body[key] = self._cumulative[key].payload()
        return body


@dataclass(frozen=True, slots=True)
class MarketInputs:
    """一轮差分决策所需的市场输入。"""

    rule: MarketRule
    reference_price: Decimal
    service_status: ServiceStatus


class MarketInputSource(Protocol):
    """按轮提供市场输入的抽象，便于离线注入（C-13）。"""

    def current(self) -> MarketInputs: ...

    def touched(self) -> tuple[str, ...]: ...


class PublicMarketInputSource:
    """经公开只读端点取市场输入；静态给定项不再拉取。

    取引ルール取一次后缓存；参考价与服务状态每轮刷新，除非
    静态给定。触碰端点按首次触碰顺序登记（A-03）。
    """

    def __init__(
        self,
        public: PublicClient,
        symbol: SpotSymbol,
        *,
        rule: MarketRule | None = None,
        reference_price: Decimal | None = None,
        service_status: ServiceStatus | None = None,
    ) -> None:
        self._public = public
        self._symbol = symbol
        self._rule = rule
        self._static_price = reference_price
        self._static_status = service_status
        self._touched: list[str] = []

    def _touch(self, endpoint: str) -> None:
        if endpoint not in self._touched:
            self._touched.append(endpoint)

    def touched(self) -> tuple[str, ...]:
        """已触碰的公开端点（A-03）。"""
        return tuple(self._touched)

    def current(self) -> MarketInputs:
        """取当前市场输入，必要时经公开端点拉取。"""
        if self._rule is None:
            self._rule = fetch_market_rule(self._public, self._symbol)
            self._touch("GET /v1/symbols")
        if self._static_price is not None:
            price = self._static_price
        else:
            tickers = self._public.ticker(str(self._symbol))
            self._touch("GET /v1/ticker")
            if not tickers:
                raise SoakError(
                    f"公开端点无品种 {self._symbol} 的最新レート"
                )
            price = tickers[0].last
        if self._static_status is not None:
            status = self._static_status
        else:
            status = self._public.status()
            self._touch("GET /v1/status")
        return MarketInputs(
            rule=self._rule,
            reference_price=price,
            service_status=status,
        )


@dataclass(frozen=True, slots=True)
class SoakPaths:
    """浸泡进程的落盘路径集合。"""

    report: Path
    checkpoint: Path
    heartbeat: Path
    stop_file: Path


class SoakRunner:
    """驻留浸泡循环的核心状态机，方法同步可测（C-13）。

    异步编排见 run_soak：本类不做 IO 等待，只执行单轮逻辑、
    落盘与停止判定。构造时断言模拟运行（T-04）。
    """

    def __init__(
        self,
        *,
        mode: RunMode,
        session: ReconcileSession,
        reader: RecordingSnapshotReader,
        ledger: IntentLedger,
        breaker: CircuitBreaker,
        emergency: EmergencyStopAction,
        symbol: SpotSymbol,
        market_source: MarketInputSource,
        limit_gate: LimitGate,
        whitelist: frozenset[SpotSymbol],
        sender: TradeClientSender,
        paths: SoakPaths,
        target_path: Path | None,
        budget_jpy: Decimal,
        no_trade_band: Decimal,
        started_at: datetime | None = None,
    ) -> None:
        ensure_dry_run(mode)
        if target_path is not None:
            raise SoakError(
                "浸泡证据暂不接受动态目标；须先实现版本化目标头与逐轮血缘校验"
            )
        self._mode = mode
        self._session = session
        self._reader = reader
        self._ledger = ledger
        self._breaker = breaker
        self._emergency = emergency
        self._symbol = symbol
        self._market_source = market_source
        self._limit_gate = limit_gate
        self._whitelist = whitelist
        self._sender = sender
        self._paths = paths
        self._target_path = target_path
        self._budget_jpy = budget_jpy
        self._no_trade_band = no_trade_band
        self._started_at = (
            started_at if started_at is not None else datetime.now(UTC)
        )
        self._observer = ChannelRaceObserver()
        self._realign_pending = False
        self._reconnects = 0
        self._rounds = 0
        self._round_errors = 0
        self._ws_events_total = 0
        self._ws_outcomes_pending: list[WsApplyOutcome] = []
        self._stop_reason: str | None = None
        self._last_heartbeat_at: datetime | None = None
        self._auth_lifecycle: tuple[str, ...] = ()
        self._write_planned: list[str] = []
        self._write_touched: list[str] = []
        # 恢复：中断的发送转入超时态（T-06）
        self._interrupted = ledger.mark_interrupted_sends()

    @property
    def rounds_completed(self) -> int:
        """已完成的轮次数。"""
        return self._rounds

    @property
    def reconnects(self) -> int:
        """已发生的 WS 重连次数。"""
        return self._reconnects

    @property
    def interrupted_marked(self) -> tuple[str, ...]:
        """启动恢复时转入超时态的意图。"""
        return self._interrupted

    def set_auth_lifecycle(self, endpoints: Sequence[str]) -> None:
        """登记令牌生命周期端点集合（A-03）。"""
        self._auth_lifecycle = tuple(endpoints)

    def note_reconnect(self) -> None:
        """WS 重连回调：下一轮强制全量快照（C-10）。"""
        self._realign_pending = True
        self._reconnects += 1

    def realign_pending(self) -> bool:
        """下一轮是否将强制全量快照。"""
        return self._realign_pending

    def apply_events(
        self, events: Iterable[PrivateEvent], now: datetime | None = None
    ) -> tuple[WsApplyOutcome, ...]:
        """消费 WS 事件并累积到下一轮报告。"""
        outcomes = self._session.ingest_ws_events(events, now)
        self._ws_outcomes_pending.extend(outcomes)
        self._ws_events_total += len(outcomes)
        return outcomes

    def request_stop(self, reason: str) -> None:
        """登记停止请求，保留首个事由。"""
        if self._stop_reason is None:
            self._stop_reason = reason

    def check_stop(self) -> str | None:
        """停止判定：先看显式请求，再看停止标记文件。"""
        if self._stop_reason is None and self._paths.stop_file.exists():
            self._stop_reason = "stop-file"
        return self._stop_reason

    def heartbeat(
        self, now: datetime | None = None, *, force: bool = False
    ) -> None:
        """写心跳文件，缺省按最小间隔节流。"""
        moment = now if now is not None else datetime.now(UTC)
        if not force and self._last_heartbeat_at is not None:
            elapsed = (moment - self._last_heartbeat_at).total_seconds()
            if elapsed < HEARTBEAT_MIN_INTERVAL_SECONDS:
                return
        body = {
            "schema_version": SOAK_SCHEMA_VERSION,
            "at": moment.isoformat(),
            "pid": os.getpid(),
            "rounds_completed": self._rounds,
            "stop_reason": self._stop_reason,
        }
        atomic_write_text(
            self._paths.heartbeat,
            json.dumps(body, ensure_ascii=False) + "\n",
        )
        self._last_heartbeat_at = moment

    def run_round(self, now: datetime | None = None) -> dict[str, object]:
        """执行一轮：快照对账、超时处理、差分决策并落盘。"""
        moment = now if now is not None else datetime.now(UTC)
        realign = self._realign_pending
        self._realign_pending = False
        ws_outcomes = tuple(self._ws_outcomes_pending)
        self._ws_outcomes_pending.clear()
        pre_orders = {
            view.order_id: view for view in self._session.store.orders()
        }
        pre_execution_ids = self._session.store.execution_ids()
        if realign:
            snapshot = self._session.on_ws_reconnect(moment)
        else:
            snapshot = self._session.snapshot_round(moment)
        race_round: dict[str, object] | None = None
        if snapshot.reconcile.mode is SnapshotMode.AUDIT:
            race_round = self._observer.observe(
                pre_orders=pre_orders,
                pre_execution_ids=pre_execution_ids,
                symbol=str(self._symbol),
                rest_active=self._reader.last_active_orders,
                rest_executions=self._reader.last_latest_executions,
                at=moment,
            )
        timeouts = self._session.resolve_timeouts(moment)
        target_value, target_error = self._read_target()
        delta: DeltaDecision | None = None
        outcome: tuple[OrderIntent, DispatchResult] | None = None
        if target_error is None and target_value is not None:
            inputs = self._market_source.current()
            delta = self._session.decide_delta(
                target_value,
                rule=inputs.rule,
                reference_price=inputs.reference_price,
                budget_jpy=self._budget_jpy,
                no_trade_band=self._no_trade_band,
            )
            if delta.proposal is not None:
                self._plan_write(ORDER_ENDPOINT)
            outcome = execute_delta(
                delta,
                ledger=self._ledger,
                limit_gate=self._limit_gate,
                breaker=self._breaker,
                service_status=inputs.service_status,
                whitelist=self._whitelist,
                sender=self._sender,
                moment=moment,
            )
            if (
                outcome is not None
                and outcome[1].state not in LOCAL_TERMINAL_STATES
            ):
                self._touch_write(ORDER_ENDPOINT)
        self._rounds += 1
        self._round_errors = 0
        payload: dict[str, object] = {
            "schema_version": SOAK_SCHEMA_VERSION,
            "record": "round",
            "round": self._rounds,
            "at": moment.isoformat(),
            "mode": self._mode.value,
            "realign": realign,
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
            "timeouts": [
                timeout_outcome_payload(item) for item in timeouts
            ],
            "race": {
                "round": race_round,
                "cumulative": self._observer.cumulative_payload(),
            },
            "position": {
                "size": format(self._session.position_size(), "f"),
                "basis": "READ_ONLY 成交事实",
            },
            "target": {
                "path": (
                    None
                    if self._target_path is None
                    else str(self._target_path)
                ),
                "value": target_value,
                "error": target_error,
            },
            "delta": (
                None
                if delta is None
                else delta_payload(
                    delta,
                    target=target_value,
                    no_trade_band=self._no_trade_band,
                )
            ),
            "intent": None if outcome is None else intent_payload(outcome),
            "breaker": self._breaker_payload(),
            "endpoints": self._endpoints_payload(),
            "ledger_path": str(self._ledger.path),
        }
        self._append_report(payload)
        self._write_checkpoint(moment, "running")
        self.heartbeat(moment, force=True)
        return payload

    def record_round_error(
        self, error: str, now: datetime | None = None
    ) -> int:
        """登记一次轮内错误并落盘，返回连续错误次数。"""
        moment = now if now is not None else datetime.now(UTC)
        self._round_errors += 1
        payload: dict[str, object] = {
            "schema_version": SOAK_SCHEMA_VERSION,
            "record": "round_error",
            "at": moment.isoformat(),
            "error": error,
            "consecutive_errors": self._round_errors,
        }
        self._append_report(payload)
        self._write_checkpoint(moment, "running")
        self.heartbeat(moment, force=True)
        return self._round_errors

    def finalize(self, reason: str, now: datetime | None = None) -> None:
        """停止收尾：写终态 checkpoint 与心跳。"""
        moment = now if now is not None else datetime.now(UTC)
        self.request_stop(reason)
        self._write_checkpoint(moment, "stopped")
        self.heartbeat(moment, force=True)

    def _read_target(self) -> tuple[float | None, str | None]:
        """当前浸泡只运行零目标基础设施路径。"""
        return 0.0, None

    def _plan_write(self, endpoint: str) -> None:
        if endpoint not in self._write_planned:
            self._write_planned.append(endpoint)

    def _touch_write(self, endpoint: str) -> None:
        if endpoint not in self._write_touched:
            self._write_touched.append(endpoint)

    def _breaker_payload(self) -> dict[str, object]:
        """熔断与紧急停止留痕的报告形态。"""
        return {
            "state": self._breaker.state.value,
            "consecutive_failures": self._breaker.consecutive_failures,
            "trip_reason": self._breaker.trip_reason,
            "emergency_stop": [
                {
                    "at": record.at.isoformat(),
                    "reason": record.reason,
                    "exit_code": record.exit_code,
                    "error": record.error,
                }
                for record in self._emergency.records
            ],
        }

    def _endpoints_payload(self) -> dict[str, object]:
        """累计触碰端点的报告形态（A-03）。"""
        reads: list[str] = []
        for endpoint in (
            *self._session.read_endpoints(),
            *self._market_source.touched(),
        ):
            if endpoint not in reads:
                reads.append(endpoint)
        write_touched = list(self._write_touched)
        if self._emergency.records:
            # 全撤动作已真实触碰端点（T-07）
            if EMERGENCY_READ_ENDPOINT not in reads:
                reads.append(EMERGENCY_READ_ENDPOINT)
            if EMERGENCY_WRITE_ENDPOINT not in write_touched:
                write_touched.append(EMERGENCY_WRITE_ENDPOINT)
        return {
            "read_touched": reads,
            "auth_lifecycle": list(self._auth_lifecycle),
            "write_planned": list(self._write_planned),
            "write_touched": write_touched,
        }

    def _append_report(self, payload: Mapping[str, object]) -> None:
        """报告行追加落盘并 fsync。"""
        line = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        durable_append_bytes(
            self._paths.report, (line + "\n").encode("utf-8")
        )

    def _write_checkpoint(self, moment: datetime, status: str) -> None:
        """原子写运行状态 checkpoint。"""
        body = {
            "schema_version": SOAK_SCHEMA_VERSION,
            "status": status,
            "started_at": self._started_at.isoformat(),
            "checkpoint_at": moment.isoformat(),
            "rounds_completed": self._rounds,
            "ws_events_applied": self._ws_events_total,
            "ws_reconnects": self._reconnects,
            "interrupted_marked": list(self._interrupted),
            "breaker_state": self._breaker.state.value,
            "trip_reason": self._breaker.trip_reason,
            "stop_reason": self._stop_reason,
            "endpoints": self._endpoints_payload(),
            "report_path": str(self._paths.report),
            "ledger_path": str(self._ledger.path),
        }
        atomic_write_text(
            self._paths.checkpoint,
            json.dumps(body, ensure_ascii=False, indent=2) + "\n",
        )


class EventStreamClient(Protocol):
    """浸泡进程所需的私有事件流形态，PrivateWsClient 满足。"""

    async def subscribe(
        self, channel: WsChannel, option: str | None = None
    ) -> None: ...

    def events(self) -> AsyncIterator[PrivateEvent]: ...

    async def run(
        self,
        on_reconnect: Callable[[], Awaitable[None]] | None = None,
    ) -> None: ...


async def _wait_interval(
    runner: SoakRunner,
    wake: asyncio.Event,
    interval_seconds: float,
    poll_seconds: float,
) -> None:
    """等到下一轮：到期、被重连唤醒或收到停止即返回。"""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + interval_seconds
    while True:
        if runner.check_stop() is not None:
            return
        if wake.is_set():
            wake.clear()
            return
        remaining = deadline - loop.time()
        if remaining <= 0:
            return
        try:
            await asyncio.wait_for(
                wake.wait(), timeout=min(poll_seconds, remaining)
            )
        except TimeoutError:
            pass
        runner.heartbeat()


async def run_soak(
    runner: SoakRunner,
    client: EventStreamClient,
    *,
    interval_seconds: float,
    max_rounds: int | None = None,
    poll_seconds: float = STOP_POLL_SECONDS,
) -> str:
    """驻留主循环：常连消费、周期轮次与双通道停止（T-04）。

    订阅 orderEvents 与 executionEvents 后并发运行连接循环与
    事件消费；重连回调把下一轮标记为强制全量快照并立即唤醒
    （C-10）。轮内错误落盘留痕，连续达上限即停止。返回停止
    事由；退出前完成当轮并写终态 checkpoint。
    """
    if interval_seconds <= 0:
        raise SoakError("轮次间隔必须为正")
    if poll_seconds <= 0:
        raise SoakError("停止轮询间隔必须为正")
    wake = asyncio.Event()

    async def _on_reconnect() -> None:
        runner.note_reconnect()
        wake.set()

    async def _consume() -> None:
        async for event in client.events():
            runner.apply_events((event,))

    await client.subscribe(WsChannel.ORDER_EVENTS)
    await client.subscribe(WsChannel.EXECUTION_EVENTS)
    run_task = asyncio.create_task(client.run(_on_reconnect))
    consume_task = asyncio.create_task(_consume())
    runner.heartbeat(force=True)
    reason: str = "stopped"
    try:
        while runner.check_stop() is None:
            try:
                runner.run_round()
            except GuvoluError as exc:
                if runner.record_round_error(str(exc)) >= ROUND_ERROR_LIMIT:
                    runner.request_stop("round-errors")
                    continue
            else:
                if (
                    max_rounds is not None
                    and runner.rounds_completed >= max_rounds
                ):
                    runner.request_stop("max-rounds")
                    continue
            await _wait_interval(
                runner, wake, interval_seconds, poll_seconds
            )
    finally:
        run_task.cancel()
        consume_task.cancel()
        await asyncio.gather(
            run_task, consume_task, return_exceptions=True
        )
        checked = runner.check_stop()
        reason = checked if checked is not None else "stopped"
        runner.finalize(reason)
    return reason


def build_parser() -> argparse.ArgumentParser:
    """构造命令行参数定义。"""
    parser = argparse.ArgumentParser(
        description="常驻对账浸泡进程：WS 常连与周期对账循环的 dry-run"
    )
    parser.add_argument("--symbol", default="BTC", help="现物品种，缺省 BTC")
    parser.add_argument(
        "--target", type=Path, default=None,
        help="保留参数；动态目标浸泡尚未获准，传入即失败关闭",
    )
    parser.add_argument(
        "--budget-jpy", default="500", help="名义预算 JPY，缺省 500"
    )
    parser.add_argument(
        "--rules", type=Path, default=None,
        help="取引ルール快照 JSON；缺省经公开端点拉取一次",
    )
    parser.add_argument(
        "--reference-price", default=None,
        help="静态参考价；缺省每轮经公开端点取最新成交价",
    )
    parser.add_argument(
        "--service-status", default=None,
        choices=[status.value for status in ServiceStatus],
        help="静态服务状态；缺省每轮经公开端点拉取",
    )
    parser.add_argument(
        "--no-trade-band", default=None,
        help="不交易带比例；缺省取会话配置",
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
        "--report", type=Path, default=None,
        help="每轮报告 JSONL 路径；缺省数据根下 execution/soak_report.jsonl",
    )
    parser.add_argument(
        "--checkpoint", type=Path, default=None,
        help="checkpoint 路径；缺省数据根下 execution/soak_checkpoint.json",
    )
    parser.add_argument(
        "--heartbeat", type=Path, default=None,
        help="心跳文件路径；缺省数据根下 execution/soak_heartbeat.json",
    )
    parser.add_argument(
        "--stop-file", type=Path, default=None,
        help="停止标记文件路径；缺省数据根下 execution/soak.stop",
    )
    parser.add_argument(
        "--interval-seconds", type=float, default=None,
        help="轮次间隔秒；缺省取会话配置快照周期",
    )
    parser.add_argument(
        "--max-rounds", type=int, default=None,
        help="轮次上限，达到即停止；缺省不设上限",
    )
    return parser


def _resolve_paths(args: argparse.Namespace) -> SoakPaths:
    """按参数与数据根解析落盘路径（C-04）。"""
    root = data_root()
    report_arg: Path | None = args.report
    checkpoint_arg: Path | None = args.checkpoint
    heartbeat_arg: Path | None = args.heartbeat
    stop_arg: Path | None = args.stop_file
    return SoakPaths(
        report=(
            report_arg if report_arg is not None
            else root / REPORT_RELATIVE_PATH
        ),
        checkpoint=(
            checkpoint_arg if checkpoint_arg is not None
            else root / CHECKPOINT_RELATIVE_PATH
        ),
        heartbeat=(
            heartbeat_arg if heartbeat_arg is not None
            else root / HEARTBEAT_RELATIVE_PATH
        ),
        stop_file=(
            stop_arg if stop_arg is not None
            else root / STOP_FILE_RELATIVE_PATH
        ),
    )


def main(argv: Sequence[str] | None = None) -> int:
    """命令行入口。仅限模拟运行，live 配置拒绝启动（T-04）。"""
    args = build_parser().parse_args(argv)
    if args.target is not None:
        print(
            "浸泡证据暂不接受动态目标；须先实现版本化目标头与逐轮血缘校验",
            file=sys.stderr,
        )
        return 2
    env_file: Path | None = args.env_file
    config = load_config(env_file)
    if config.mode is not RunMode.DRY_RUN:
        print(
            "浸泡进程仅限模拟运行，live 配置拒绝启动（T-04、A-01）",
            file=sys.stderr,
        )
        return 2
    settings = load_session_settings(args.session_config)
    ledger_arg: Path | None = args.ledger
    ledger_path = (
        ledger_arg if ledger_arg is not None
        else data_root() / LEDGER_RELATIVE_PATH
    )
    ledger = IntentLedger(ledger_path)
    breaker = CircuitBreaker(load_breaker_thresholds(args.breaker_config))
    public = PublicClient.from_config(config)
    trade = TradeClient.from_config(config)
    emergency = arm_emergency_stop(breaker, public, trade)
    limiter = RateLimiter(config.private_rps)
    read_key, read_secret = config.require_read_credentials()
    transport = PrivateTransport(
        read_key, read_secret, limiter, config.log_dir
    )
    reader = RecordingSnapshotReader(ReadClient(transport))
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
    rules_arg: Path | None = args.rules
    rule = (
        load_market_rule(rules_arg, symbol)
        if rules_arg is not None
        else None
    )
    price_arg: str | None = args.reference_price
    static_price: Decimal | None = None
    if price_arg is not None:
        static_price = decimal_argument(price_arg, "--reference-price")
        if static_price <= 0:
            raise SoakError("参考价必须为正")
    status_arg: str | None = args.service_status
    static_status = (
        ServiceStatus(status_arg) if status_arg is not None else None
    )
    market_source = PublicMarketInputSource(
        public,
        symbol,
        rule=rule,
        reference_price=static_price,
        service_status=static_status,
    )
    band_arg: str | None = args.no_trade_band
    if band_arg is not None:
        no_trade_band = decimal_argument(band_arg, "--no-trade-band")
        if no_trade_band < 0 or no_trade_band >= 1:
            raise SoakError("不交易带必须在 [0, 1) 内")
    else:
        no_trade_band = settings.no_trade_band
    budget_jpy = decimal_argument(str(args.budget_jpy), "--budget-jpy")
    if budget_jpy <= 0:
        raise SoakError("预算必须为正")
    runner = SoakRunner(
        mode=config.mode,
        session=session,
        reader=reader,
        ledger=ledger,
        breaker=breaker,
        emergency=emergency,
        symbol=symbol,
        market_source=market_source,
        limit_gate=LimitGate(config.limits),
        whitelist=config.spot_whitelist,
        sender=TradeClientSender(trade),
        paths=_resolve_paths(args),
        target_path=args.target,
        budget_jpy=budget_jpy,
        no_trade_band=no_trade_band,
    )
    token = create_ws_token(transport)
    runner.set_auth_lifecycle(
        (
            WS_AUTH_CREATE_ENDPOINT,
            WS_AUTH_EXTEND_ENDPOINT,
            WS_AUTH_REVOKE_ENDPOINT,
        )
    )
    client: EventStreamClient = PrivateWsClient(token)
    interval_arg: float | None = args.interval_seconds
    interval = (
        interval_arg
        if interval_arg is not None
        else float(settings.snapshot_interval_seconds)
    )
    max_rounds: int | None = args.max_rounds

    def _on_sigint(signum: int, frame: FrameType | None) -> None:
        runner.request_stop("SIGINT")

    async def _run() -> str:
        alive = asyncio.create_task(
            keepalive(transport, token, TOKEN_KEEPALIVE_SECONDS)
        )
        try:
            return await run_soak(
                runner,
                client,
                interval_seconds=interval,
                max_rounds=max_rounds,
            )
        finally:
            alive.cancel()
            await asyncio.gather(alive, return_exceptions=True)

    try:
        previous = signal.signal(signal.SIGINT, _on_sigint)
    except ValueError:
        # 非主线程时不接管信号
        previous = None
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        runner.finalize("SIGINT")
    finally:
        if previous is not None:
            signal.signal(signal.SIGINT, previous)
        try:
            revoke_ws_token(transport, token)
        except GuvoluError:
            # 撤销失败不阻碍停机
            pass
    return 1 if breaker.state is BreakerState.TRIPPED else 0
