"""最小实盘 canary：单笔限价、双重确认、闭环复核（T-12 第三级）。

按策略研究管线第 6 节 canary 合同实现：固定 GMO 现物 BTC、单笔
限价、名义不超过五百日元；完成成交或撤单及账户对账后立即停机
复核，不自动扩大范围。切换实盘由人工在交互终端二次确认（A-01、
X-02），启动横幅醒目标示 live 模式并明示将触碰的端点（T-04）。

入场前先确定退出条件（R-01）：等待窗口届满即撤单，撤单经
`POST /v1/cancelOrder`；紧急处置另有独立 kill-switch（T-07）。
真实状态一律以 READ_ONLY 为准（T-03），超时按查询后决策（T-06）。
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Protocol

from guvolu.api.public_client import PublicClient
from guvolu.api.read_client import ReadClient
from guvolu.api.trade_client import TradeClient
from guvolu.data.intent_ledger import LEDGER_RELATIVE_PATH, IntentLedger
from guvolu.data.paths import data_root
from guvolu.domain.config import Config, load_config
from guvolu.domain.enums import (
    ExecutionType,
    OrderStatus,
    RunMode,
    ServiceStatus,
    Side,
)
from guvolu.domain.errors import GuvoluError
from guvolu.domain.ids import new_correlation_id, new_intent_id
from guvolu.domain.intent import IntentState, OrderIntent
from guvolu.domain.models import Order
from guvolu.domain.symbols import SpotSymbol
from guvolu.execution.conversion import MarketRule
from guvolu.execution.dispatch import DispatchResult, dispatch_order_intent
from guvolu.execution.dry_run_executor import fetch_market_rule
from guvolu.execution.limit_replay import replay_limit_usage
from guvolu.execution.reconcile import (
    ReadOnlyOrderReader,
    ReconcileAmbiguity,
    resolve_send_timeout,
)
from guvolu.execution.trade_sender import TradeClientSender
from guvolu.risk.circuit_breaker import (
    CircuitBreaker,
    load_breaker_thresholds,
)
from guvolu.risk.limits import LimitGate


class CanaryError(GuvoluError):
    """canary 前置条件或运行约束不满足。"""


class CanarySender(Protocol):
    """发送与撤单边界：生产实现为 TradeClientSender（T-02）。"""

    consumes_write_budget: bool

    def send(self, intent: OrderIntent) -> int: ...

    def cancel(self, order_id: int) -> None: ...


class CanaryReader(ReadOnlyOrderReader, Protocol):
    """canary 所需 READ_ONLY 能力子集（T-03）。"""

    def orders(self, order_ids: Sequence[int]) -> tuple[Order, ...]: ...


# canary 合同名义上限
CANARY_MAX_NOTIONAL_JPY = Decimal("500")
# 缺省委托数量
DEFAULT_SIZE_TEXT = "0.00002"
# 缺省等待窗口秒数
DEFAULT_MAX_WAIT_SECONDS = 300
# 委托状态轮询间隔秒数
POLL_INTERVAL_SECONDS = 5.0
# 撤单后确认终态的时限秒数
CANCEL_VERIFY_TIMEOUT_SECONDS = 90.0
# 可用余力的手续费缓冲系数
FEE_BUFFER_RATIO = Decimal("1.001")
# 第一重确认口令
CONFIRM_PHRASE = "实盘 canary 确认"

# 触碰端点清单（X-02、A-03）
READ_ENDPOINTS = (
    "GET /v1/status",
    "GET /v1/symbols",
    "GET /v1/orderbooks",
    "GET /v1/account/assets",
    "GET /v1/orders",
    "GET /v1/activeOrders",
    "GET /v1/latestExecutions",
)
WRITE_ENDPOINTS = (
    "POST /v1/order",
    "POST /v1/cancelOrder",
)

# 委托终态集合
_TERMINAL_ORDER_STATUSES = frozenset(
    {OrderStatus.EXECUTED, OrderStatus.CANCELED, OrderStatus.EXPIRED}
)


@dataclass(frozen=True, slots=True)
class CanaryPlan:
    """一次 canary 的完整计划，确认前全部定死（R-01）。"""

    symbol: SpotSymbol
    side: Side
    size: Decimal
    price: Decimal
    notional_jpy: Decimal
    max_wait_seconds: int

    def as_dict(self) -> dict[str, object]:
        return {
            "symbol": str(self.symbol),
            "side": self.side.value,
            "execution_type": ExecutionType.LIMIT.value,
            "size": str(self.size),
            "price": str(self.price),
            "notional_jpy": str(self.notional_jpy),
            "max_wait_seconds": self.max_wait_seconds,
        }


def _floor_to_step(value: Decimal, step: Decimal) -> Decimal:
    """向下取整到步长。"""
    return (value // step) * step


def build_plan(
    *,
    rule: MarketRule,
    best_bid: Decimal,
    size: Decimal,
    order_jpy_max: Decimal,
    max_wait_seconds: int,
    price: Decimal | None = None,
) -> CanaryPlan:
    """构造并校验计划：买向下取整到 tick，名义受双重上限约束。

    上限取 canary 合同五百日元与当前 T-11 单笔限额的较小者；
    数量必须落在取引ルール步长与上下限内。
    """
    if best_bid <= 0:
        raise CanaryError("最优买价必须为正")
    if max_wait_seconds <= 0:
        raise CanaryError("等待窗口必须为正")
    limit_price = price if price is not None else best_bid
    limit_price = _floor_to_step(limit_price, rule.tick_size)
    if limit_price <= 0:
        raise CanaryError("限价取整后必须为正")
    if limit_price > best_bid:
        raise CanaryError("canary 限价不得越过最优买价")
    if size < rule.min_order_size:
        raise CanaryError(
            f"数量 {size} 低于最小委托量 {rule.min_order_size}"
        )
    if size > rule.max_order_size:
        raise CanaryError(f"数量 {size} 超过最大委托量")
    if size != _floor_to_step(size, rule.size_step):
        raise CanaryError(f"数量 {size} 不在步长 {rule.size_step} 上")
    notional = size * limit_price
    ceiling = min(CANARY_MAX_NOTIONAL_JPY, order_jpy_max)
    if notional > ceiling:
        raise CanaryError(
            f"名义 {notional} JPY 超过 canary 上限 {ceiling} JPY"
        )
    return CanaryPlan(
        symbol=rule.symbol,
        side=Side.BUY,
        size=size,
        price=limit_price,
        notional_jpy=notional,
        max_wait_seconds=max_wait_seconds,
    )


def render_banner(plan: CanaryPlan, config: Config) -> str:
    """live 醒目横幅：模式、端点、计划与退出条件（T-04、X-02）。"""
    lines = [
        "=" * 62,
        "警告：实盘（live）模式——将发送真实写请求",
        "=" * 62,
        f"运行模式        : {config.mode.value}",
        f"品种            : {plan.symbol}（现物，白名单 T-09）",
        f"委托            : {plan.side.value} LIMIT "
        f"{plan.size} @ {plan.price}",
        f"名义金额        : {plan.notional_jpy} JPY"
        f"（canary 上限 {CANARY_MAX_NOTIONAL_JPY} JPY）",
        f"单笔/单日限额   : {config.limits.order_jpy_max} / "
        f"{config.limits.day_jpy_max} JPY（T-11）",
        f"退出条件        : {plan.max_wait_seconds} 秒未成交即撤单（R-01）",
        "将触碰的读取端点:",
        *(f"  {endpoint}" for endpoint in READ_ENDPOINTS),
        "将触碰的写入端点:",
        *(f"  {endpoint}" for endpoint in WRITE_ENDPOINTS),
        "紧急停止        : python -m guvolu.ops.kill_switch（T-07）",
        "=" * 62,
    ]
    return "\n".join(lines)


def confirm_plan(
    plan: CanaryPlan,
    *,
    input_fn: Callable[[str], str],
    interactive: bool,
) -> bool:
    """二次确认（X-02）：口令加名义金额复述，非交互直接拒绝。"""
    if not interactive:
        raise CanaryError(
            "实盘 canary 需要交互式终端完成二次确认（X-02、A-01）"
        )
    first = input_fn(f"第一重确认，请原样输入「{CONFIRM_PHRASE}」: ")
    if first.strip() != CONFIRM_PHRASE:
        return False
    second = input_fn(
        f"第二重确认，请原样输入名义金额「{plan.notional_jpy}」: "
    )
    return second.strip() == str(plan.notional_jpy)


@dataclass(frozen=True, slots=True)
class CanaryOutcome:
    """canary 终局：委托快照与全过程证据（R-07）。"""

    dispatch: DispatchResult
    final_order: Order | None
    cancel_requested: bool
    resolution: str


def poll_until_terminal(
    reader: CanaryReader,
    order_id: int,
    *,
    deadline_monotonic: float,
    clock: Callable[[], float],
    sleep: Callable[[float], None],
) -> Order | None:
    """轮询委托直到终态或窗口届满，返回最后快照（T-03）。"""
    last: Order | None = None
    while True:
        orders = reader.orders([order_id])
        if orders:
            last = orders[0]
            if last.status in _TERMINAL_ORDER_STATUSES:
                return last
        if clock() >= deadline_monotonic:
            return last
        sleep(POLL_INTERVAL_SECONDS)


def run_canary(
    plan: CanaryPlan,
    *,
    config: Config,
    ledger: IntentLedger,
    reader: CanaryReader,
    sender: CanarySender,
    service_status: ServiceStatus,
    limit_gate: LimitGate,
    breaker: CircuitBreaker,
    moment: datetime | None = None,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    inflight_dir: Path | None = None,
) -> CanaryOutcome:
    """执行一次 canary：落盘、过闸、发送、等待、撤单、判终局。

    发送经统一编排（T-05、T-11、跨进程在途锁），真实状态以
    READ_ONLY 为准（T-03）；超时先查询再决策（T-06）。
    """
    now = moment if moment is not None else datetime.now(UTC)
    intent = OrderIntent(
        intent_id=new_intent_id(),
        correlation_id=new_correlation_id(),
        symbol=plan.symbol,
        side=plan.side,
        execution_type=ExecutionType.LIMIT,
        size=plan.size,
        price=plan.price,
        time_in_force=None,
        created_at=now,
    )
    result = dispatch_order_intent(
        intent,
        ledger=ledger,
        limit_gate=limit_gate,
        breaker=breaker,
        service_status=service_status,
        whitelist=config.spot_whitelist,
        sender=sender,
        inflight_dir=inflight_dir,
    )
    if result.state is IntentState.SEND_TIMEOUT:
        # T-06：查询后决策
        try:
            resolution = resolve_send_timeout(
                intent.intent_id, ledger=ledger, reader=reader
            )
        except ReconcileAmbiguity as exc:
            return CanaryOutcome(
                result, None, False,
                f"超时对账歧义，保持在途等待人工处置: {exc}",
            )
        if resolution.order_id is None:
            return CanaryOutcome(
                result, None, False, "超时对账判定为未受理（FAILED）"
            )
        result = DispatchResult(
            result.intent_id,
            IntentState.ACCEPTED,
            resolution.order_id,
            "超时对账判定为已受理",
            consumed_write_budget=result.consumed_write_budget,
        )
    if result.state is not IntentState.ACCEPTED or result.order_id is None:
        return CanaryOutcome(
            result, None, False, f"发送未受理: {result.reason}"
        )
    order_id = result.order_id
    deadline = clock() + float(plan.max_wait_seconds)
    snapshot = poll_until_terminal(
        reader, order_id,
        deadline_monotonic=deadline, clock=clock, sleep=sleep,
    )
    if snapshot is not None and snapshot.status in _TERMINAL_ORDER_STATUSES:
        return CanaryOutcome(result, snapshot, False, "窗口内到达终态")
    # R-01：窗口届满执行退出条件
    sender.cancel(order_id)
    verify_deadline = clock() + CANCEL_VERIFY_TIMEOUT_SECONDS
    snapshot = poll_until_terminal(
        reader, order_id,
        deadline_monotonic=verify_deadline, clock=clock, sleep=sleep,
    )
    if snapshot is not None and snapshot.status in _TERMINAL_ORDER_STATUSES:
        return CanaryOutcome(result, snapshot, True, "届满撤单并确认终态")
    return CanaryOutcome(
        result, snapshot, True,
        "撤单后未确认终态，请立即人工复核或使用 kill-switch（T-07）",
    )


def _assets_view(reader: ReadClient) -> dict[str, dict[str, str]]:
    """资产快照，amount 与 available 分列（U-03）。"""
    view: dict[str, dict[str, str]] = {}
    for asset in reader.assets():
        view[asset.symbol] = {
            "amount": str(asset.amount),
            "available": str(asset.available),
        }
    return view


def write_report(body: dict[str, object], directory: Path) -> Path:
    """内容寻址落盘 canary 报告（R-07）。"""
    payload = json.dumps(
        body, ensure_ascii=False, sort_keys=True, indent=2
    ) + "\n"
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"canary-report-sha256-{digest}.json"
    path.write_text(payload, encoding="utf-8")
    return path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="最小实盘 canary（单笔限价，双重确认）",
        allow_abbrev=False,
    )
    parser.add_argument("--env-file", type=Path, default=None)
    parser.add_argument("--symbol", default="BTC")
    parser.add_argument("--size", default=DEFAULT_SIZE_TEXT)
    parser.add_argument(
        "--price", default=None,
        help="限价；缺省取最优买价向下取整到 tick",
    )
    parser.add_argument(
        "--max-wait-seconds", type=int, default=DEFAULT_MAX_WAIT_SECONDS,
    )
    parser.add_argument(
        "--breaker-config", type=Path,
        default=Path("config/circuit_breaker.json"),
    )
    parser.add_argument("--ledger", type=Path, default=None)
    parser.add_argument(
        "--report-directory", type=Path, default=None,
        help="缺省为数据根 execution/canary/",
    )
    return parser


def _decimal_argument(text: str, name: str) -> Decimal:
    try:
        value = Decimal(text)
    except InvalidOperation as exc:
        raise CanaryError(f"{name} 不是合法数字: {text}") from exc
    if value <= 0:
        raise CanaryError(f"{name} 必须为正")
    return value


def main(argv: Sequence[str] | None = None) -> int:
    """命令行入口。仅交互终端、仅 live 模式、双重确认后执行。"""
    args = build_parser().parse_args(argv)
    env_file: Path | None = args.env_file
    config = load_config(env_file)
    if config.mode is not RunMode.LIVE:
        print(
            "当前不是 live 模式。切换实盘需人工设置 GUVOLU_MODE=live"
            "（T-04、A-01），本入口不会代为切换。",
        )
        return 2
    root = data_root()
    ledger_arg: Path | None = args.ledger
    ledger_path = (
        ledger_arg if ledger_arg is not None
        else root / LEDGER_RELATIVE_PATH
    )
    report_arg: Path | None = args.report_directory
    report_directory = (
        report_arg if report_arg is not None
        else root / "execution" / "canary"
    )
    public = PublicClient.from_config(config)
    reader = ReadClient.from_config(config)
    service_status = public.status()
    if service_status is not ServiceStatus.OPEN:
        print(f"服务状态 {service_status.value}，不发写请求（R-03）")
        return 2
    symbol = SpotSymbol(str(args.symbol))
    rule = fetch_market_rule(public, symbol)
    book = public.orderbooks(str(symbol))
    if not book.bids:
        print("盘口无买档，放弃")
        return 2
    best_bid = book.bids[0].price
    price_arg: str | None = args.price
    plan = build_plan(
        rule=rule,
        best_bid=best_bid,
        size=_decimal_argument(str(args.size), "--size"),
        order_jpy_max=config.limits.order_jpy_max,
        max_wait_seconds=int(args.max_wait_seconds),
        price=(
            _decimal_argument(price_arg, "--price")
            if price_arg is not None else None
        ),
    )
    assets_before = _assets_view(reader)
    jpy = assets_before.get("JPY")
    if jpy is None or Decimal(jpy["available"]) < (
        plan.notional_jpy * FEE_BUFFER_RATIO
    ):
        print(
            f"可用 JPY 不足：需约 {plan.notional_jpy * FEE_BUFFER_RATIO}"
            f"，现有 {jpy['available'] if jpy else '0'}（R-06）"
        )
        return 2
    print(render_banner(plan, config))
    if not confirm_plan(
        plan, input_fn=input, interactive=sys.stdin.isatty()
    ):
        print("确认失败，未发送任何写请求。")
        return 2
    ledger = IntentLedger(ledger_path)
    breaker = CircuitBreaker(load_breaker_thresholds(args.breaker_config))
    limit_gate = LimitGate(config.limits)
    # 重放当日用量（T-11）
    replay_limit_usage(limit_gate, ledger, moment=datetime.now(UTC))
    trade = TradeClient.from_config(config)
    sender = TradeClientSender(trade)
    started_at = datetime.now(UTC)
    outcome = run_canary(
        plan,
        config=config,
        ledger=ledger,
        reader=reader,
        sender=sender,
        service_status=service_status,
        limit_gate=limit_gate,
        breaker=breaker,
    )
    assets_after = _assets_view(reader)
    final = outcome.final_order
    body: dict[str, object] = {
        "schema_version": 1,
        "kind": "live_canary_report",
        "mode": config.mode.value,
        "started_at": started_at.isoformat(),
        "completed_at": datetime.now(UTC).isoformat(),
        "plan": plan.as_dict(),
        "intent_id": outcome.dispatch.intent_id,
        "dispatch_state": outcome.dispatch.state.value,
        "order_id": outcome.dispatch.order_id,
        "consumed_write_budget": outcome.dispatch.consumed_write_budget,
        "cancel_requested": outcome.cancel_requested,
        "resolution": outcome.resolution,
        "final_order_status": (
            final.status.value if final is not None else None
        ),
        "executed_size": (
            str(final.executed_size) if final is not None else None
        ),
        "assets_before": assets_before,
        "assets_after": assets_after,
        "read_touched": list(READ_ENDPOINTS),
        "write_touched": list(WRITE_ENDPOINTS),
        "ledger_path": str(ledger_path),
    }
    report_path = write_report(body, report_directory)
    print(f"报告: {report_path}")
    print(f"终局: {outcome.resolution}")
    print("canary 完成。按合同立即停机复核，不自动扩大范围。")
    terminal = final is not None and final.status in _TERMINAL_ORDER_STATUSES
    return 0 if terminal else 1
