"""dry-run 执行器：目标位置制品到发送边界的全链彩排（T-04）。

读取研究域 target-position 制品的运行快照目标，经 G-05 转换
闸门生成意图，过全量风控闸门后进入发送适配；模拟运行模式下
被 TradeClient 守卫在发送边界拦截即为预期终点，账本留痕全程
（T-05、R-07）。报告列明品种、方向、数量、金额与触碰端点
（A-03）。对账模式经 READ_ONLY 处置超时意图（T-06）。

命令行入口见 scripts/run_dry_run_executor.py。品种取引ルール、
参考价与服务状态可由参数离线给定；缺省经公开只读端点拉取，
不涉及任何写请求。
"""
from __future__ import annotations

import argparse
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from guvolu.api.public_client import PublicClient
from guvolu.api.read_client import ReadClient
from guvolu.api.trade_client import TradeClient
from guvolu.data.intent_ledger import LEDGER_RELATIVE_PATH, IntentLedger
from guvolu.data.paths import data_root
from guvolu.domain.config import Config, load_config
from guvolu.domain.enums import ExecutionType, RunMode, ServiceStatus
from guvolu.domain.errors import DryRunBlocked, GuvoluError
from guvolu.domain.ids import new_correlation_id, new_intent_id
from guvolu.domain.intent import IntentState, OrderIntent
from guvolu.domain.models import SymbolRule
from guvolu.domain.symbols import SpotSymbol
from guvolu.execution.conversion import (
    MarketRule,
    OrderProposal,
    convert_target_to_order,
)
from guvolu.execution.dispatch import (
    DispatchResult,
    OrderSender,
    dispatch_order_intent,
)
from guvolu.execution.reconcile import (
    ReconcileAmbiguity,
    resolve_send_timeout,
    send_timeout_intents,
)
from guvolu.execution.trade_sender import TradeClientSender
from guvolu.risk.circuit_breaker import (
    DEFAULT_THRESHOLDS_PATH,
    CircuitBreaker,
    load_breaker_thresholds,
)
from guvolu.risk.limits import LimitGate

# 目标契约的口径标识
TARGET_UNIT = "risk_weighted_directional_target"
# 下单触碰的写端点（A-03）
ORDER_ENDPOINT = "POST /v1/order"
# 预期终点状态，进程返回零
EXPECTED_END_STATES = frozenset(
    {IntentState.DRY_RUN_BLOCKED, IntentState.ACCEPTED}
)


class ExecutorError(GuvoluError):
    """执行器输入非法或制品契约不符。"""


class _DryRunSender:
    """无私钥的模拟发送边界；绝不构造私有客户端。"""

    def send(self, intent: OrderIntent) -> int:
        del intent
        raise DryRunBlocked("dry-run 模式禁止私有写请求")


@dataclass(frozen=True, slots=True)
class TargetArtifact:
    """target-position 制品中执行器消费的字段。"""

    path: Path
    run_id: str
    decision_time: str
    market_id: str
    unit: str
    aggregate_target: float


@dataclass(frozen=True, slots=True)
class DryRunPlan:
    """一次执行计划，报告义务的数据载体（A-03）。"""

    artifact: TargetArtifact
    budget_jpy: Decimal
    reference_price: Decimal
    proposal: OrderProposal | None
    skip_reason: str | None


def _required_str(payload: Mapping[str, object], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise ExecutorError(f"制品字段 {key} 缺失或非文本")
    return value


def load_target_artifact(path: Path) -> TargetArtifact:
    """装载 target-position 制品并校验执行器消费的契约。

    只消费运行快照目标 operational_target_contract；研究回放
    目标属研究域，不进入执行链（G-01 边界）。
    """
    try:
        payload: object = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ExecutorError(f"目标位置制品不存在: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ExecutorError(f"目标位置制品不是合法 JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise ExecutorError("目标位置制品根必须是对象")
    contract = payload.get("operational_target_contract")
    if not isinstance(contract, dict):
        raise ExecutorError("制品缺少 operational_target_contract")
    unit = _required_str(contract, "unit")
    if unit != TARGET_UNIT:
        raise ExecutorError(f"目标口径不符: {unit!r} 应为 {TARGET_UNIT!r}")
    raw_target = contract.get("aggregate_target")
    if isinstance(raw_target, bool) or not isinstance(
        raw_target, (int, float)
    ):
        raise ExecutorError("aggregate_target 缺失或非数值")
    return TargetArtifact(
        path=path,
        run_id=_required_str(payload, "run_id"),
        decision_time=_required_str(payload, "decision_time"),
        market_id=_required_str(payload, "market_id"),
        unit=unit,
        aggregate_target=float(raw_target),
    )


def load_market_rule(path: Path, symbol: SpotSymbol) -> MarketRule:
    """从取引ルール快照文件取指定品种规则。

    快照为公开端点响应的 data 列表，或含 data 键的完整载荷。
    """
    try:
        payload: object = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ExecutorError(f"取引ルール快照不存在: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ExecutorError(f"取引ルール快照不是合法 JSON: {path}") from exc
    if isinstance(payload, dict):
        payload = payload.get("data")
    if not isinstance(payload, list):
        raise ExecutorError("取引ルール快照必须是列表")
    for row in payload:
        if isinstance(row, dict) and row.get("symbol") == str(symbol):
            return MarketRule.from_symbol_rule(SymbolRule.from_api(row))
    raise ExecutorError(f"取引ルール快照缺少品种 {symbol}")


def fetch_market_rule(public: PublicClient, symbol: SpotSymbol) -> MarketRule:
    """经公开端点取指定品种取引ルール。"""
    for rule in public.symbols():
        if rule.symbol == str(symbol):
            return MarketRule.from_symbol_rule(rule)
    raise ExecutorError(f"公开端点无品种 {symbol} 的取引ルール")


def build_plan(
    artifact: TargetArtifact,
    *,
    rule: MarketRule,
    reference_price: Decimal,
    budget_jpy: Decimal,
) -> DryRunPlan:
    """经 G-05 转换闸门把制品目标折算为执行计划。"""
    if artifact.aggregate_target == 0:
        return DryRunPlan(
            artifact=artifact,
            budget_jpy=budget_jpy,
            reference_price=reference_price,
            proposal=None,
            skip_reason="目标为零，无需委托",
        )
    proposal = convert_target_to_order(
        artifact.aggregate_target,
        budget_jpy=budget_jpy,
        reference_price=reference_price,
        rule=rule,
    )
    if proposal is None:
        return DryRunPlan(
            artifact=artifact,
            budget_jpy=budget_jpy,
            reference_price=reference_price,
            proposal=None,
            skip_reason="折算数量低于最小委托量，无需委托",
        )
    return DryRunPlan(
        artifact=artifact,
        budget_jpy=budget_jpy,
        reference_price=reference_price,
        proposal=proposal,
        skip_reason=None,
    )


def execute_plan(
    plan: DryRunPlan,
    *,
    ledger: IntentLedger,
    limit_gate: LimitGate,
    breaker: CircuitBreaker,
    service_status: ServiceStatus,
    whitelist: frozenset[SpotSymbol],
    sender: OrderSender,
    moment: datetime | None = None,
) -> tuple[OrderIntent, DispatchResult] | None:
    """把计划落为意图并经发送编排执行，账本留痕全程。

    执行器只生成限价意图，价格来自 G-05 取整后的转换产物。
    """
    if plan.proposal is None:
        return None
    now = moment if moment is not None else datetime.now(UTC)
    intent = OrderIntent(
        intent_id=new_intent_id(),
        correlation_id=new_correlation_id(),
        symbol=plan.proposal.symbol,
        side=plan.proposal.side,
        execution_type=ExecutionType.LIMIT,
        size=plan.proposal.size,
        price=plan.proposal.price,
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


def render_report(
    plan: DryRunPlan,
    outcome: tuple[OrderIntent, DispatchResult] | None,
    *,
    mode: RunMode,
    service_status: ServiceStatus,
    ledger_path: Path,
    read_endpoints: Sequence[str],
) -> dict[str, object]:
    """生成执行报告：品种、方向、数量、金额与触碰端点（A-03）。

    金额与数量以字符串表达（D-07）；目标为研究域 float 原文。
    write_planned 是实盘将触碰的写端点，write_touched 是本次
    实际触碰的写端点，模拟拦截时为空。
    """
    proposal_payload: dict[str, str] | None = None
    if plan.proposal is not None:
        proposal_payload = {
            "symbol": str(plan.proposal.symbol),
            "side": plan.proposal.side.value,
            "size": format(plan.proposal.size, "f"),
            "price": format(plan.proposal.price, "f"),
            "notional_jpy": format(plan.proposal.notional_jpy, "f"),
        }
    intent_payload: dict[str, object] | None = None
    write_touched: list[str] = []
    if outcome is not None:
        intent, result = outcome
        intent_payload = {
            "intent_id": intent.intent_id,
            "correlation_id": intent.correlation_id,
            "state": result.state.value,
            "order_id": result.order_id,
            "reason": result.reason,
        }
        if result.state not in (
            IntentState.GATE_REJECTED, IntentState.DRY_RUN_BLOCKED
        ):
            write_touched.append(ORDER_ENDPOINT)
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "mode": mode.value,
        "service_status": service_status.value,
        "artifact": {
            "path": str(plan.artifact.path),
            "run_id": plan.artifact.run_id,
            "decision_time": plan.artifact.decision_time,
            "market_id": plan.artifact.market_id,
            "unit": plan.artifact.unit,
            "aggregate_target": plan.artifact.aggregate_target,
        },
        "budget_jpy": format(plan.budget_jpy, "f"),
        "reference_price": format(plan.reference_price, "f"),
        "proposal": proposal_payload,
        "skip_reason": plan.skip_reason,
        "intent": intent_payload,
        "endpoints": {
            "read_touched": list(read_endpoints),
            "write_planned": [] if plan.proposal is None else [ORDER_ENDPOINT],
            "write_touched": write_touched,
        },
        "ledger_path": str(ledger_path),
    }


def _emit_report(report: Mapping[str, object], destination: str) -> None:
    """把报告 JSON 写到标准输出或指定文件。"""
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
        raise ExecutorError(f"参数 {name} 不是合法数值: {raw!r}") from exc
    if value <= 0:
        raise ExecutorError(f"参数 {name} 必须为正: {raw!r}")
    return value


def _resolve_timeouts(config: Config, ledger_path: Path) -> int:
    """对账模式：经 READ_ONLY 处置全部超时意图（T-06）。"""
    ledger = IntentLedger(ledger_path)
    reader = ReadClient.from_config(config)
    resolved: list[dict[str, object]] = []
    ambiguous: list[dict[str, object]] = []
    for intent_id in send_timeout_intents(ledger):
        try:
            resolution = resolve_send_timeout(
                intent_id, ledger=ledger, reader=reader
            )
        except ReconcileAmbiguity as exc:
            ambiguous.append({"intent_id": intent_id, "error": str(exc)})
            continue
        resolved.append(
            {
                "intent_id": resolution.intent_id,
                "state": resolution.state.value,
                "order_id": resolution.order_id,
                "evidence": dict(resolution.evidence),
            }
        )
    print(
        json.dumps(
            {"resolved": resolved, "ambiguous": ambiguous},
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    return 1 if ambiguous else 0


def build_parser() -> argparse.ArgumentParser:
    """构造命令行参数定义。"""
    parser = argparse.ArgumentParser(
        description="dry-run 执行器：目标位置制品到发送边界的彩排"
    )
    parser.add_argument("--target", type=Path, help="target-position 制品路径")
    parser.add_argument("--symbol", default="BTC", help="现物品种，缺省 BTC")
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
        "--ledger", type=Path, default=None,
        help="意图账本路径；缺省数据根下 execution/intent_ledger.jsonl",
    )
    parser.add_argument(
        "--breaker-config", type=Path, default=DEFAULT_THRESHOLDS_PATH,
        help="熔断阈值配置路径（G-06）",
    )
    parser.add_argument(
        "--env-file", type=Path, default=None, help="配置文件路径，缺省 .env"
    )
    parser.add_argument(
        "--dry-run-report", default="-",
        help="报告输出路径，- 表示标准输出（A-03）",
    )
    parser.add_argument(
        "--resolve-timeouts", action="store_true",
        help="对账模式：经 READ_ONLY 处置超时意图（T-06）",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """命令行入口。缺省模拟运行，实盘须显式配置（T-04、A-01）。"""
    args = build_parser().parse_args(argv)
    env_file: Path | None = args.env_file
    config = load_config(env_file)
    ledger_arg: Path | None = args.ledger
    ledger_path = (
        ledger_arg if ledger_arg is not None
        else data_root() / LEDGER_RELATIVE_PATH
    )
    if args.resolve_timeouts:
        return _resolve_timeouts(config, ledger_path)
    target_arg: Path | None = args.target
    if target_arg is None:
        raise ExecutorError("缺少 --target 制品路径")
    symbol = SpotSymbol(str(args.symbol))
    budget_jpy = _decimal_argument(str(args.budget_jpy), "--budget-jpy")
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
        read_touched.append("GET /v1/symbols")
    price_arg: str | None = args.reference_price
    if price_arg is not None:
        reference_price = _decimal_argument(price_arg, "--reference-price")
    else:
        tickers = get_public().ticker(str(symbol))
        read_touched.append("GET /v1/ticker")
        if not tickers:
            raise ExecutorError(f"公开端点无品种 {symbol} 的最新レート")
        reference_price = tickers[0].last
    status_arg: str | None = args.service_status
    if status_arg is not None:
        service_status = ServiceStatus(status_arg)
    else:
        service_status = get_public().status()
        read_touched.append("GET /v1/status")
    artifact = load_target_artifact(target_arg)
    plan = build_plan(
        artifact,
        rule=rule,
        reference_price=reference_price,
        budget_jpy=budget_jpy,
    )
    ledger = IntentLedger(ledger_path)
    breaker_config: Path = args.breaker_config
    breaker = CircuitBreaker(load_breaker_thresholds(breaker_config))
    limit_gate = LimitGate(config.limits)
    sender: OrderSender
    if config.mode is RunMode.DRY_RUN:
        sender = _DryRunSender()
    else:
        sender = TradeClientSender(TradeClient.from_config(config))
    outcome = execute_plan(
        plan,
        ledger=ledger,
        limit_gate=limit_gate,
        breaker=breaker,
        service_status=service_status,
        whitelist=config.spot_whitelist,
        sender=sender,
    )
    report = render_report(
        plan,
        outcome,
        mode=config.mode,
        service_status=service_status,
        ledger_path=ledger_path,
        read_endpoints=read_touched,
    )
    _emit_report(report, str(args.dry_run_report))
    if outcome is None:
        return 0
    return 0 if outcome[1].state in EXPECTED_END_STATES else 1
