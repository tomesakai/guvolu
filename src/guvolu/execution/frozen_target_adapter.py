"""把冻结前向预测封装为执行目标快照。

第 2 版只增字段不改语义（D-06）：在第 1 版的 operational_target_contract
之外新增 exposure_target、target_semantics、valid_from、valid_until、
correlation_id、risk_budget_jpy、mode 与 symbol，对应决策 I/O 契约 v2 的
ExecutionTarget。目标域唯一为纯多头现货 [0, 1]，负值与越界在此拒绝，
不在下游解释。第 1 版预测缺少的有效期与因果链标识在此本地派生并
标注来源。
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Mapping, Sequence

from guvolu.domain.config import MAX_ORDER_JPY_CEILING
from guvolu.domain.errors import ConfigError
from guvolu.domain.symbols import SpotSymbol
from guvolu.execution.paper_config import (
    TARGET_MODES,
    bar_interval_duration,
    load_paper_config,
)
from guvolu.execution.target_contract import TARGET_UNIT


ADAPTER_SCHEMA_VERSION = 2
ADAPTER_METHOD_VERSION = "frozen-forward-operational-target-v2"
# 缺省决策柱间隔，预测与参数均未给出时使用
DEFAULT_BAR_INTERVAL = "1hour"
# 全链路唯一目标域声明
TARGET_SEMANTICS: Mapping[str, object] = {
    "domain": "long_only_spot",
    "range": [0, 1],
    "reference": "fraction_of_risk_budget",
    "short_allowed": False,
}
SUPPORTED_PREDICTION_SCHEMA_VERSIONS = frozenset({1, 2})
REQUIRED_QUALITY_FLAGS = frozenset({
    "clock", "coverage", "eligible", "freshness",
    "integrity", "lineage", "pit",
})


class FrozenTargetError(ValueError):
    """表示冻结预测不能进入执行目标层。"""


def _canonical_bytes(value: object) -> bytes:
    """生成稳定 JSON 字节。"""
    return (
        json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        + "\n"
    ).encode("utf-8")


def _required_text(payload: Mapping[str, object], key: str) -> str:
    """读取非空文本字段。"""
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise FrozenTargetError(f"冻结预测字段缺失: {key}")
    return value


def _optional_text(payload: Mapping[str, object], key: str) -> str | None:
    """读取可缺省的文本字段，存在即须为非空文本。"""
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise FrozenTargetError(f"冻结预测字段非法: {key}")
    return value


def _aware_time(value: str, field: str = "decision_time") -> datetime:
    """确认时间带有明确时区并返回解析结果。"""
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise FrozenTargetError(f"冻结预测 {field} 非法") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise FrozenTargetError(f"冻结预测 {field} 缺少时区")
    return parsed


def _validate_target_value(raw: object) -> float:
    """校验目标为有限、非负且不超过 1 的数值。"""
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise FrozenTargetError("冻结预测 aggregate_target 非数值")
    value = float(raw)
    if not math.isfinite(value) or value > 1:
        raise FrozenTargetError("冻结预测 aggregate_target 越界")
    if value < 0:
        raise FrozenTargetError(
            "冻结预测 aggregate_target 为负，目标域仅接受纯多头 [0, 1]"
        )
    return value


def _validate_semantics(prediction: Mapping[str, object]) -> None:
    """第 2 版预测自带目标域声明时必须与全链路唯一目标域一致。"""
    semantics = prediction.get("target_semantics")
    if semantics is None:
        return
    if not isinstance(semantics, Mapping) or dict(semantics) != dict(
        TARGET_SEMANTICS
    ):
        raise FrozenTargetError("冻结预测 target_semantics 与目标域不一致")


def validate_frozen_prediction_bytes(
    source_bytes: bytes,
) -> tuple[dict[str, object], str]:
    """从一次捕获的原始字节验证冻结预测及其质量合同。"""
    try:
        raw: object = json.loads(source_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FrozenTargetError("冻结预测不是合法 JSON") from exc
    if not isinstance(raw, dict):
        raise FrozenTargetError("冻结预测根必须为对象")
    schema_version = raw.get("schema_version")
    if (
        isinstance(schema_version, bool)
        or not isinstance(schema_version, int)
        or schema_version not in SUPPORTED_PREDICTION_SCHEMA_VERSIONS
        or raw.get("scope") != "FROZEN_FORWARD"
    ):
        raise FrozenTargetError("冻结预测结构或范围不受支持")
    if raw.get("unit") != TARGET_UNIT:
        raise FrozenTargetError("冻结预测目标口径不受支持")
    decision_time = _required_text(raw, "decision_time")
    _aware_time(decision_time)
    _required_text(raw, "prediction_id")
    _required_text(raw, "plan_id")
    _required_text(raw, "input_head_generation")
    _validate_target_value(raw.get("aggregate_target"))
    _validate_semantics(raw)
    exposure = raw.get("exposure_target")
    if exposure is not None and _validate_target_value(exposure) != float(
        _validate_target_value(raw.get("aggregate_target"))
    ):
        raise FrozenTargetError("冻结预测 exposure_target 与 aggregate_target 不符")
    quality = raw.get("quality")
    if not isinstance(quality, dict):
        raise FrozenTargetError("冻结预测缺少质量合同")
    missing = sorted(REQUIRED_QUALITY_FLAGS - set(quality))
    failed = sorted(
        key for key in REQUIRED_QUALITY_FLAGS if quality.get(key) is not True
    )
    reasons = quality.get("reasons")
    if missing or failed or reasons not in ([], None):
        raise FrozenTargetError(
            f"冻结预测质量未通过: missing={missing} failed={failed}"
        )
    families = raw.get("families")
    if not isinstance(families, list):
        raise FrozenTargetError("冻结预测缺少 family 贡献")
    return raw, hashlib.sha256(source_bytes).hexdigest()


def load_frozen_prediction(path: Path) -> tuple[dict[str, object], str]:
    """稳定读取并验证冻结预测与质量合同。"""
    try:
        source_bytes = path.read_bytes()
    except FileNotFoundError as exc:
        raise FrozenTargetError(f"冻结预测不存在: {path}") from exc
    except OSError as exc:
        raise FrozenTargetError(f"冻结预测不可读取: {path}") from exc
    return validate_frozen_prediction_bytes(source_bytes)


def derive_correlation_id(prediction_id: str) -> str:
    """预测未携带因果链标识时，由 prediction_id 确定性派生。

    同一预测重跑得到同一标识，保持目标快照内容寻址与幂等；
    形态与 ids.new_correlation_id 一致（D-05）。
    """
    digest = hashlib.sha256(
        f"guvolu-prediction:{prediction_id}".encode("utf-8")
    ).hexdigest()
    return f"co{digest[:16]}"


def _resolve_validity(
    prediction: Mapping[str, object],
    decision_time: datetime,
    bar_interval: str,
) -> tuple[datetime, str]:
    """取有效期终点：预测自带优先，否则按决策柱间隔派生。"""
    given = _optional_text(prediction, "valid_until")
    if given is not None:
        parsed = _aware_time(given, "valid_until")
        if parsed <= decision_time:
            raise FrozenTargetError("冻结预测 valid_until 不晚于 decision_time")
        return parsed, "prediction"
    try:
        span = bar_interval_duration(bar_interval)
    except ConfigError as exc:
        raise FrozenTargetError(str(exc)) from exc
    return decision_time + span, "derived"


def build_operational_target(
    prediction_path: Path,
    *,
    market_id: str,
    symbol: SpotSymbol,
    risk_budget_jpy: Decimal,
    mode: str,
    bar_interval: str | None = None,
) -> dict[str, object]:
    """构造只供执行边界消费的目标快照（ExecutionTarget）。

    risk_budget_jpy 由调用方自版本化配置给出，不得超过 T-11 硬顶。
    """
    if not market_id:
        raise FrozenTargetError("market_id 不能为空")
    if mode not in TARGET_MODES:
        raise FrozenTargetError(f"mode 不受支持: {mode!r}")
    if (
        not risk_budget_jpy.is_finite()
        or risk_budget_jpy <= 0
        or risk_budget_jpy > MAX_ORDER_JPY_CEILING
    ):
        raise FrozenTargetError(
            f"risk_budget_jpy 必须在 (0, {MAX_ORDER_JPY_CEILING}] 内"
        )
    prediction, source_sha = load_frozen_prediction(prediction_path)
    prediction_id = _required_text(prediction, "prediction_id")
    decision_text = _required_text(prediction, "decision_time")
    decision_time = _aware_time(decision_text)
    interval = (
        _optional_text(prediction, "bar_interval")
        or bar_interval
        or DEFAULT_BAR_INTERVAL
    )
    valid_until, valid_until_source = _resolve_validity(
        prediction, decision_time, interval,
    )
    inherited = _optional_text(prediction, "correlation_id")
    correlation_id = (
        inherited if inherited is not None
        else derive_correlation_id(prediction_id)
    )
    target_value = _validate_target_value(prediction["aggregate_target"])
    # 夹到 [0, 1]，越界已在校验处拒绝
    exposure_target = min(max(target_value, 0.0), 1.0)
    lineage: dict[str, object] = {
        "input_head_generation": _required_text(
            prediction, "input_head_generation",
        ),
        "plan_id": _required_text(prediction, "plan_id"),
        "prediction_id": prediction_id,
        "source_prediction_path": str(prediction_path),
        "source_prediction_sha256": source_sha,
    }
    decision_input_sha = _optional_text(prediction, "decision_input_sha256")
    if decision_input_sha is not None:
        lineage["decision_input_sha256"] = decision_input_sha
    return {
        "artifact_kind": "operational_target_snapshot",
        "bar_interval": interval,
        "correlation_id": correlation_id,
        "correlation_id_source": (
            "prediction" if inherited is not None else "adapter"
        ),
        "decision_time": decision_text,
        "exposure_target": exposure_target,
        "lineage": lineage,
        "market_id": market_id,
        "method_version": ADAPTER_METHOD_VERSION,
        "mode": mode,
        "operational_target_contract": {
            "aggregate_target": prediction["aggregate_target"],
            "families": prediction["families"],
            "reserve": prediction.get("reserve"),
            "unit": TARGET_UNIT,
        },
        "quality": prediction["quality"],
        "risk_budget_jpy": format(risk_budget_jpy, "f"),
        "run_id": prediction_id,
        "schema_version": ADAPTER_SCHEMA_VERSION,
        "symbol": str(symbol),
        "target_semantics": dict(TARGET_SEMANTICS),
        "valid_from": decision_text,
        "valid_until": valid_until.isoformat(),
        "valid_until_source": valid_until_source,
    }


def persist_operational_target(
    prediction_path: Path,
    output_directory: Path,
    *,
    market_id: str,
    symbol: SpotSymbol,
    risk_budget_jpy: Decimal,
    mode: str,
    bar_interval: str | None = None,
) -> tuple[Path, str]:
    """以内容散列文件名原子保存执行目标。"""
    payload = _canonical_bytes(build_operational_target(
        prediction_path,
        market_id=market_id,
        symbol=symbol,
        risk_budget_jpy=risk_budget_jpy,
        mode=mode,
        bar_interval=bar_interval,
    ))
    digest = hashlib.sha256(payload).hexdigest()
    output_directory.mkdir(parents=True, exist_ok=True)
    target = output_directory / f"target-{digest}.json"
    if target.is_file():
        if target.read_bytes() != payload:
            raise FrozenTargetError("执行目标文件名发生散列冲突")
        return target, digest
    temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    with temporary.open("wb", buffering=0) as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, target)
    return target, digest


def _decimal_argument(raw: str, name: str) -> Decimal:
    """命令行金额直接进 Decimal，绝不经 float（T-08）。"""
    try:
        value = Decimal(raw)
    except InvalidOperation as exc:
        raise FrozenTargetError(f"参数 {name} 不是合法数值: {raw!r}") from exc
    if not value.is_finite():
        raise FrozenTargetError(f"参数 {name} 必须是有限数值: {raw!r}")
    return value


def main(argv: Sequence[str] | None = None) -> int:
    """命令行生成执行目标快照。

    预算、品种、市场与决策柱间隔来自 --config 指向的版本化配置，
    显式参数可覆盖；预算没有命令行缺省值。
    """
    parser = argparse.ArgumentParser(description="冻结预测转执行目标")
    parser.add_argument("--prediction", type=Path, required=True)
    parser.add_argument("--output-directory", type=Path, required=True)
    parser.add_argument(
        "--config", type=Path, default=None,
        help="paper 执行器配置，提供预算、品种、市场与柱间隔",
    )
    parser.add_argument("--market-id", default=None)
    parser.add_argument("--symbol", default=None)
    parser.add_argument("--bar-interval", default=None)
    parser.add_argument(
        "--risk-budget-jpy", default=None,
        help="名义预算 JPY，显式给出时覆盖配置，无缺省值",
    )
    parser.add_argument(
        "--mode", default="dry-run", choices=sorted(TARGET_MODES),
        help="目标消费模式，缺省 dry-run（T-04）",
    )
    args = parser.parse_args(argv)
    config_path: Path | None = args.config
    config = load_paper_config(config_path) if config_path is not None else None
    market_id: str | None = args.market_id or (
        config.market_id if config is not None else None
    )
    symbol_text: str | None = args.symbol or (
        str(config.symbol) if config is not None else None
    )
    budget_text: str | None = args.risk_budget_jpy
    budget = (
        _decimal_argument(budget_text, "--risk-budget-jpy")
        if budget_text is not None
        else (config.risk_budget_jpy if config is not None else None)
    )
    bar_interval: str | None = args.bar_interval or (
        config.bar_interval if config is not None else None
    )
    if market_id is None or symbol_text is None or budget is None:
        raise FrozenTargetError(
            "缺少 --config 或显式的 --market-id、--symbol、--risk-budget-jpy"
        )
    path, digest = persist_operational_target(
        args.prediction,
        args.output_directory,
        market_id=market_id,
        symbol=SpotSymbol(symbol_text),
        risk_budget_jpy=budget,
        mode=str(args.mode),
        bar_interval=bar_interval,
    )
    print(json.dumps({
        "path": str(path),
        "sha256": digest,
        "status": f"ready_for_{str(args.mode).replace('-', '_')}",
    }, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
