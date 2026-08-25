"""dry-run 执行器：目标位置制品到发送边界的全链彩排（T-04）。

读取研究域 target-position 制品的运行快照目标，经 G-05 转换
闸门生成意图，过全量风控闸门后进入本地发送适配；该入口只允许
dry-run，并由无私钥 sender 在发送边界拦截，账本留痕全程
（T-05、R-07）。报告列明品种、方向、数量、金额与触碰端点
（A-03）。对账模式经 READ_ONLY 处置超时意图（T-06）。

命令行入口见 scripts/run_dry_run_executor.py。品种取引ルール、
参考价与服务状态可由参数离线给定；缺省经公开只读端点拉取，
不涉及任何写请求。
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import cast

from guvolu.api.public_client import PublicClient
from guvolu.api.read_client import ReadClient
from guvolu.data.intent_ledger import LEDGER_RELATIVE_PATH, IntentLedger
from guvolu.data.paths import data_root
from guvolu.domain.config import MAX_ORDER_JPY_CEILING, Config, load_config
from guvolu.domain.enums import ExecutionType, RunMode, ServiceStatus
from guvolu.domain.errors import ConfigError, DryRunBlocked, GuvoluError
from guvolu.domain.ids import new_intent_id
from guvolu.domain.intent import (
    LOCAL_TERMINAL_STATES,
    IntentState,
    OrderIntent,
)
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
from guvolu.execution.frozen_target_adapter import (
    FrozenTargetError,
    validate_frozen_prediction_bytes,
)
from guvolu.execution.paper_config import (
    DEFAULT_PAPER_CONFIG_PATH,
    TARGET_MODES,
    PaperExecutorConfig,
    bar_interval_duration,
    load_paper_config,
)
from guvolu.execution.reconcile import (
    ReconcileAmbiguity,
    resolve_send_timeout,
    send_timeout_intents,
)
from guvolu.execution.target_contract import TARGET_UNIT
from guvolu.risk.circuit_breaker import (
    DEFAULT_THRESHOLDS_PATH,
    CircuitBreaker,
    load_breaker_thresholds,
)
from guvolu.risk.limits import LimitGate

# 冻结目标的执行侧合同
# 显式非法字段不得降级
OPERATIONAL_TARGET_SCHEMA_VERSION = 2
OPERATIONAL_TARGET_KIND = "operational_target_snapshot"
OPERATIONAL_TARGET_METHOD = "frozen-forward-operational-target-v2"
DRY_RUN_TARGET_MODE = "dry-run"
TARGET_SEMANTICS: Mapping[str, object] = {
    "domain": "long_only_spot",
    "range": [0, 1],
    "reference": "fraction_of_risk_budget",
    "short_allowed": False,
}
_CORRELATION_ID = re.compile(r"^co[0-9a-f]{16}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_QUALITY_FLAGS = (
    "clock", "coverage", "eligible", "freshness",
    "integrity", "lineage", "pit",
)
_LEGACY_TARGET_SCHEMA_VERSIONS = frozenset({11, 12})
_V2_TARGET_FIELDS = frozenset({
    "artifact_kind",
    "bar_interval",
    "correlation_id",
    "correlation_id_source",
    "decision_time",
    "exposure_target",
    "lineage",
    "market_id",
    "method_version",
    "mode",
    "operational_target_contract",
    "quality",
    "risk_budget_jpy",
    "run_id",
    "schema_version",
    "symbol",
    "target_semantics",
    "valid_from",
    "valid_until",
    "valid_until_source",
})
_V2_EXCLUSIVE_FIELDS = frozenset({
    "artifact_kind",
    "bar_interval",
    "correlation_id",
    "correlation_id_source",
    "exposure_target",
    "lineage",
    "method_version",
    "mode",
    "quality",
    "risk_budget_jpy",
    "symbol",
    "target_semantics",
    "valid_from",
    "valid_until",
    "valid_until_source",
})
# 下单触碰的写端点（A-03）
ORDER_ENDPOINT = "POST /v1/order"
# 预期终点状态，进程返回零
EXPECTED_END_STATES = frozenset(
    {IntentState.DRY_RUN_BLOCKED}
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
    sha256: str
    payload: Mapping[str, object]
    run_id: str
    decision_time: datetime
    correlation_id: str
    market_id: str
    unit: str
    aggregate_target: float
    symbol: SpotSymbol | None = None
    risk_budget_jpy: Decimal | None = None
    mode: str | None = None
    valid_from: datetime | None = None
    valid_until: datetime | None = None


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


def _decision_time(payload: Mapping[str, object]) -> datetime:
    """读取带时区的决策时刻，供意图账本保留 PIT 血缘。"""
    raw = _required_str(payload, "decision_time")
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ExecutorError("制品字段 decision_time 不是合法时刻") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ExecutorError("制品字段 decision_time 缺少时区")
    return parsed


def _derived_correlation_id(run_id: str) -> str:
    digest = hashlib.sha256(
        f"guvolu-prediction:{run_id}".encode("utf-8")
    ).hexdigest()
    return f"co{digest[:16]}"


def _target_correlation_id(
    payload: Mapping[str, object], run_id: str, *, legacy: bool,
) -> str:
    """继承目标因果链；旧制品按运行身份确定性派生。"""
    if "correlation_id" in payload:
        raw = payload["correlation_id"]
        if (
            not isinstance(raw, str)
            or not raw.strip()
            or _CORRELATION_ID.fullmatch(raw) is None
        ):
            raise ExecutorError("制品字段 correlation_id 非法")
        if (
            not legacy
            and payload.get("correlation_id_source") == "adapter"
            and raw != _derived_correlation_id(run_id)
        ):
            raise ExecutorError("v2 adapter correlation_id 与 run_id 不一致")
        return raw
    if not legacy:
        raise ExecutorError("v2 执行目标缺少 correlation_id")
    return _derived_correlation_id(run_id)


def _aware_field(payload: Mapping[str, object], key: str) -> datetime:
    raw = _required_str(payload, key)
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ExecutorError(f"制品字段 {key} 不是合法时刻") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ExecutorError(f"制品字段 {key} 缺少时区")
    return parsed


def _v2_fields(
    payload: Mapping[str, object],
    *,
    path: Path,
    raw_bytes: bytes,
    run_id: str,
    decision_time: datetime,
) -> tuple[SpotSymbol, Decimal, str, datetime, datetime]:
    """复核 adapter v2 身份、PIT 时间与执行参数。"""
    if (
        payload.get("schema_version") != OPERATIONAL_TARGET_SCHEMA_VERSION
        or payload.get("artifact_kind") != OPERATIONAL_TARGET_KIND
        or payload.get("method_version") != OPERATIONAL_TARGET_METHOD
        or payload.get("mode") not in TARGET_MODES
        or set(payload) != _V2_TARGET_FIELDS
    ):
        raise ExecutorError("v2 执行目标结构、方法或模式不受支持")
    digest = hashlib.sha256(raw_bytes).hexdigest()
    if path.name != f"target-{digest}.json":
        raise ExecutorError("v2 执行目标不是内容寻址文件")
    valid_from = _aware_field(payload, "valid_from")
    valid_until = _aware_field(payload, "valid_until")
    if valid_from != decision_time or valid_until <= valid_from:
        raise ExecutorError("v2 执行目标有效期与决策时点不一致")
    semantics = payload.get("target_semantics")
    if not isinstance(semantics, Mapping) or dict(semantics) != dict(
        TARGET_SEMANTICS
    ):
        raise ExecutorError("v2 执行目标 target_semantics 不一致")
    lineage = payload.get("lineage")
    if not isinstance(lineage, Mapping) or lineage.get("prediction_id") != run_id:
        raise ExecutorError("v2 执行目标 lineage.prediction_id 不一致")
    for key in ("plan_id", "input_head_generation", "source_prediction_path"):
        value = lineage.get(key)
        if not isinstance(value, str) or not value.strip():
            raise ExecutorError(f"v2 执行目标 lineage.{key} 缺失")
    source_sha = lineage.get("source_prediction_sha256")
    if not isinstance(source_sha, str) or _SHA256.fullmatch(source_sha) is None:
        raise ExecutorError("v2 执行目标来源预测 SHA-256 非法")
    quality = payload.get("quality")
    if (
        not isinstance(quality, Mapping)
        or any(quality.get(key) is not True for key in _QUALITY_FLAGS)
        or quality.get("reasons") not in ([], None)
    ):
        raise ExecutorError("v2 执行目标质量未通过")
    exposure = payload.get("exposure_target")
    contract = payload.get("operational_target_contract")
    if (
        isinstance(exposure, bool)
        or not isinstance(exposure, (int, float))
    ):
        raise ExecutorError("v2 执行目标 exposure_target 非数值")
    exposure_value = float(exposure)
    if not math.isfinite(exposure_value) or not 0.0 <= exposure_value <= 1.0:
        raise ExecutorError("v2 执行目标 exposure_target 越界")
    if (
        not isinstance(contract, Mapping)
        or contract.get("aggregate_target") != exposure
    ):
        raise ExecutorError("v2 执行目标 exposure 与运行合同不一致")
    symbol = SpotSymbol(_required_str(payload, "symbol"))
    try:
        budget = Decimal(_required_str(payload, "risk_budget_jpy"))
    except InvalidOperation as exc:
        raise ExecutorError("v2 执行目标 risk_budget_jpy 非法") from exc
    if not budget.is_finite() or budget <= 0 or budget > MAX_ORDER_JPY_CEILING:
        raise ExecutorError("v2 执行目标 risk_budget_jpy 越界")
    source = payload.get("correlation_id_source")
    if source not in ("adapter", "prediction"):
        raise ExecutorError("v2 执行目标 correlation_id_source 非法")
    return symbol, budget, _required_str(payload, "mode"), valid_from, valid_until


def load_target_artifact(path: Path) -> TargetArtifact:
    """装载 target-position 制品并校验执行器消费的契约。

    只消费运行快照目标 operational_target_contract；研究回放
    目标属研究域，不进入执行链（G-01 边界）。
    """
    try:
        raw_bytes = path.read_bytes()
        payload: object = json.loads(raw_bytes.decode("utf-8"))
    except FileNotFoundError as exc:
        raise ExecutorError(f"目标位置制品不存在: {path}") from exc
    except OSError as exc:
        raise ExecutorError(f"目标位置制品不可读取: {path}") from exc
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
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
    target_value = float(raw_target)
    if not math.isfinite(target_value) or not 0.0 <= target_value <= 1.0:
        raise ExecutorError("aggregate_target 必须是 [0, 1] 内有限数值")
    run_id = _required_str(payload, "run_id")
    decision_time = _decision_time(payload)
    v2_marker = (
        payload.get("schema_version") == OPERATIONAL_TARGET_SCHEMA_VERSION
        or any(key in payload for key in _V2_EXCLUSIVE_FIELDS)
    )
    correlation_id = _target_correlation_id(
        payload, run_id, legacy=not v2_marker,
    )
    symbol: SpotSymbol | None = None
    risk_budget_jpy: Decimal | None = None
    mode: str | None = None
    valid_from: datetime | None = None
    valid_until: datetime | None = None
    if v2_marker:
        symbol, risk_budget_jpy, mode, valid_from, valid_until = _v2_fields(
            payload,
            path=path,
            raw_bytes=raw_bytes,
            run_id=run_id,
            decision_time=decision_time,
        )
    elif payload.get("schema_version") not in _LEGACY_TARGET_SCHEMA_VERSIONS:
        raise ExecutorError("legacy 目标 schema_version 不受支持")
    return TargetArtifact(
        path=path,
        sha256=hashlib.sha256(raw_bytes).hexdigest(),
        payload={str(key): value for key, value in payload.items()},
        run_id=run_id,
        decision_time=decision_time,
        correlation_id=correlation_id,
        market_id=_required_str(payload, "market_id"),
        unit=unit,
        aggregate_target=target_value,
        symbol=symbol,
        risk_budget_jpy=risk_budget_jpy,
        mode=mode,
        valid_from=valid_from,
        valid_until=valid_until,
    )


def _required_mapping(
    payload: Mapping[str, object], key: str,
) -> Mapping[str, object]:
    value = payload.get(key)
    if not isinstance(value, Mapping):
        raise ExecutorError(f"制品字段 {key} 缺失或非对象")
    return value


def _stable_file_bytes(path: Path, *, label: str) -> bytes:
    """从同一打开句柄取稳定字节，并拒绝读取期间的路径替换。"""
    try:
        with path.open("rb") as stream:
            before = os.fstat(stream.fileno())
            body = stream.read()
            after = os.fstat(stream.fileno())
        final = path.stat()
    except OSError as exc:
        raise ExecutorError(f"{label}不可稳定读取: {path}") from exc
    before_id = (
        before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns,
    )
    after_id = (
        after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns,
    )
    final_id = (
        final.st_dev, final.st_ino, final.st_size, final.st_mtime_ns,
    )
    if before_id != after_id or after_id != final_id or len(body) != after.st_size:
        raise ExecutorError(f"{label}在读取期间发生变化")
    return body


def verify_v2_source_prediction(
    artifact: TargetArtifact,
    *,
    source_prediction_path: Path,
    expected_source_sha256: str,
) -> None:
    """由编排侧可信路径/SHA 重建 adapter v2 的来源语义。"""
    if artifact.mode is None:
        raise ExecutorError("legacy 目标没有可验证的 v2 来源血缘")
    if _SHA256.fullmatch(expected_source_sha256) is None:
        raise ExecutorError("来源预测预期 SHA-256 非法")
    payload = artifact.payload
    lineage = _required_mapping(payload, "lineage")
    declared_path = _required_str(lineage, "source_prediction_path")
    try:
        expected_path = source_prediction_path.resolve(strict=True)
        lineage_path = Path(declared_path).resolve(strict=True)
    except OSError as exc:
        raise ExecutorError("来源预测路径不可达") from exc
    if source_prediction_path.is_symlink() or expected_path != lineage_path:
        raise ExecutorError("来源预测路径与编排绑定不一致")
    source_bytes = _stable_file_bytes(expected_path, label="来源预测")
    try:
        source, actual_sha = validate_frozen_prediction_bytes(source_bytes)
    except FrozenTargetError as exc:
        raise ExecutorError(str(exc)) from exc
    if (
        actual_sha != expected_source_sha256
        or lineage.get("source_prediction_sha256") != actual_sha
    ):
        raise ExecutorError("来源预测 SHA-256 与编排/lineage 不一致")
    source_prediction_id = _required_str(source, "prediction_id")
    source_plan_id = _required_str(source, "plan_id")
    source_head = _required_str(source, "input_head_generation")
    source_decision = _aware_field(source, "decision_time")
    if (
        artifact.run_id != source_prediction_id
        or lineage.get("prediction_id") != source_prediction_id
        or lineage.get("plan_id") != source_plan_id
        or lineage.get("input_head_generation") != source_head
        or artifact.decision_time != source_decision
    ):
        raise ExecutorError("v2 目标身份/时点与来源预测不一致")

    source_target = cast(int | float, source["aggregate_target"])
    target_value = float(source_target)
    source_families = source["families"]
    contract = _required_mapping(payload, "operational_target_contract")
    if (
        set(contract) != {"aggregate_target", "families", "reserve", "unit"}
        or contract.get("aggregate_target") != source_target
        or contract.get("families") != source_families
        or contract.get("reserve") != source.get("reserve")
        or contract.get("unit") != TARGET_UNIT
        or artifact.aggregate_target != target_value
    ):
        raise ExecutorError("v2 目标运行合同与来源预测不一致")
    source_quality = source.get("quality")
    if (
        not isinstance(source_quality, Mapping)
        or payload.get("quality") != source_quality
    ):
        raise ExecutorError("v2 目标质量合同与来源预测不一致")

    inherited_correlation = source.get("correlation_id")
    target_correlation_source = payload.get("correlation_id_source")
    if inherited_correlation is None:
        expected_correlation = _derived_correlation_id(source_prediction_id)
        expected_correlation_source = "adapter"
    else:
        if (
            not isinstance(inherited_correlation, str)
            or _CORRELATION_ID.fullmatch(inherited_correlation) is None
        ):
            raise ExecutorError("来源预测 correlation_id 非法")
        expected_correlation = inherited_correlation
        expected_correlation_source = "prediction"
    if (
        artifact.correlation_id != expected_correlation
        or target_correlation_source != expected_correlation_source
    ):
        raise ExecutorError("v2 目标 correlation 血缘与来源预测不一致")

    decision_input = source.get("decision_input_sha256")
    expected_lineage_keys = {
        "input_head_generation", "plan_id", "prediction_id",
        "source_prediction_path", "source_prediction_sha256",
    }
    if decision_input is not None:
        if not isinstance(decision_input, str) or not decision_input:
            raise ExecutorError("来源预测 decision_input_sha256 非法")
        expected_lineage_keys.add("decision_input_sha256")
    if (
        set(lineage) != expected_lineage_keys
        or lineage.get("decision_input_sha256") != decision_input
    ):
        raise ExecutorError("v2 目标 lineage 与来源预测不一致")

    interval = _required_str(payload, "bar_interval")
    source_interval = source.get("bar_interval")
    if source_interval is not None and source_interval != interval:
        raise ExecutorError("v2 目标 bar_interval 与来源预测不一致")
    source_valid_until = source.get("valid_until")
    if source_valid_until is None:
        try:
            expected_valid_until = source_decision + bar_interval_duration(interval)
        except ConfigError as exc:
            raise ExecutorError(str(exc)) from exc
        valid_until_source = "derived"
    else:
        expected_valid_until = _aware_field(source, "valid_until")
        valid_until_source = "prediction"
    if (
        artifact.valid_from != source_decision
        or artifact.valid_until != expected_valid_until
        or payload.get("valid_until_source") != valid_until_source
    ):
        raise ExecutorError("v2 目标有效期与来源预测不一致")


def validate_target_runtime(
    artifact: TargetArtifact,
    *,
    runtime_mode: RunMode,
    symbol: SpotSymbol,
    budget_jpy: Decimal,
    target_config: PaperExecutorConfig | None,
    now: datetime,
) -> None:
    """在任何账本或发送器构造前绑定模式、市场、时效和预算。"""
    if now.tzinfo is None or now.utcoffset() is None:
        raise ExecutorError("执行时刻必须带时区")
    if runtime_mode is not RunMode.DRY_RUN:
        raise ExecutorError("dry-run 执行器拒绝非 dry-run 运行模式")
    if artifact.mode is None:
        if target_config is not None:
            raise ExecutorError("legacy 目标不得绑定 v2 执行配置")
        return
    if artifact.mode != runtime_mode.value:
        raise ExecutorError("执行目标 mode 与运行模式不一致")
    if target_config is None:
        raise ExecutorError("v2 执行目标缺少版本化执行配置")
    if (
        artifact.market_id != target_config.market_id
        or artifact.symbol != target_config.symbol
        or artifact.symbol != symbol
        or artifact.payload.get("bar_interval") != target_config.bar_interval
    ):
        raise ExecutorError(
            "v2 执行目标 market/symbol/bar_interval 与执行配置不一致"
        )
    if (
        artifact.risk_budget_jpy != target_config.risk_budget_jpy
        or artifact.risk_budget_jpy != budget_jpy
    ):
        raise ExecutorError("v2 执行目标 risk_budget_jpy 与执行配置不一致")
    if artifact.valid_from is None or artifact.valid_until is None:
        raise ExecutorError("v2 执行目标缺少有效期")
    if not artifact.valid_from <= now < artifact.valid_until:
        raise ExecutorError("v2 执行目标尚未生效或已经过期")


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
        correlation_id=plan.artifact.correlation_id,
        symbol=plan.proposal.symbol,
        side=plan.proposal.side,
        execution_type=ExecutionType.LIMIT,
        size=plan.proposal.size,
        price=plan.proposal.price,
        time_in_force=None,
        created_at=now,
        prediction_id=plan.artifact.run_id,
        decision_time=plan.artifact.decision_time,
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
        if result.state not in LOCAL_TERMINAL_STATES:
            write_touched.append(ORDER_ENDPOINT)
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "mode": mode.value,
        "service_status": service_status.value,
        "artifact": {
            "path": str(plan.artifact.path),
            "sha256": plan.artifact.sha256,
            "run_id": plan.artifact.run_id,
            "decision_time": plan.artifact.decision_time.isoformat(),
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
    if not value.is_finite() or value <= 0:
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
        "--target-config",
        type=Path,
        default=DEFAULT_PAPER_CONFIG_PATH,
        help="adapter v2 目标绑定的版本化执行配置",
    )
    parser.add_argument(
        "--source-prediction",
        type=Path,
        default=None,
        help="由编排侧绑定的来源冻结预测路径；v2 必填",
    )
    parser.add_argument(
        "--source-prediction-sha256",
        default=None,
        help="编排侧在适配前固定的来源预测 SHA-256；v2 必填",
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


def main(
    argv: Sequence[str] | None = None,
    *,
    moment: datetime | None = None,
) -> int:
    """命令行入口；该模块永远不进入真实发送器（T-04、A-01）。"""
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
    if config.mode is not RunMode.DRY_RUN:
        raise ExecutorError("dry-run 执行器拒绝非 dry-run 运行模式")
    target_arg: Path | None = args.target
    if target_arg is None:
        raise ExecutorError("缺少 --target 制品路径")
    symbol = SpotSymbol(str(args.symbol))
    budget_jpy = _decimal_argument(str(args.budget_jpy), "--budget-jpy")
    artifact = load_target_artifact(target_arg)
    if artifact.mode is None:
        raise ExecutorError(
            "公共 dry-run 只接受可重建来源血缘的 adapter v2 目标"
        )
    source_prediction: Path | None = args.source_prediction
    source_sha: str | None = args.source_prediction_sha256
    if source_prediction is None or source_sha is None:
        raise ExecutorError("v2 目标缺少编排绑定的来源预测路径或 SHA-256")
    verify_v2_source_prediction(
        artifact,
        source_prediction_path=source_prediction,
        expected_source_sha256=source_sha,
    )
    target_config = load_paper_config(Path(args.target_config))
    execution_moment = moment if moment is not None else datetime.now(UTC)
    validate_target_runtime(
        artifact,
        runtime_mode=config.mode,
        symbol=symbol,
        budget_jpy=budget_jpy,
        target_config=target_config,
        now=execution_moment,
    )
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
    plan = build_plan(
        artifact,
        rule=rule,
        reference_price=reference_price,
        budget_jpy=budget_jpy,
    )
    if plan.proposal is None:
        report = render_report(
            plan,
            None,
            mode=config.mode,
            service_status=service_status,
            ledger_path=ledger_path,
            read_endpoints=read_touched,
        )
        _emit_report(report, str(args.dry_run_report))
        return 0
    ledger = IntentLedger(ledger_path)
    breaker_config: Path = args.breaker_config
    breaker = CircuitBreaker(load_breaker_thresholds(breaker_config))
    limit_gate = LimitGate(config.limits)
    sender: OrderSender = _DryRunSender()
    outcome = execute_plan(
        plan,
        ledger=ledger,
        limit_gate=limit_gate,
        breaker=breaker,
        service_status=service_status,
        whitelist=config.spot_whitelist,
        sender=sender,
        moment=execution_moment,
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
