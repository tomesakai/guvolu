"""趋势、量价确认趋势、突破、均值回归与网格基线。"""
from __future__ import annotations

import math
from collections.abc import Mapping, Sequence

from guvolu.strategy.contracts import CandidateSpec, FeatureRow, ResearchBar
from guvolu.strategy.expression import (
    candidate_identity,
    evaluate_expression,
    expression_id,
    strategy_expression,
)

STRATEGY_METHOD_VERSION = "typed-signal-rules-v3"
SUPPORTED_FAMILIES = (
    "breakout",
    "flow_trend",
    "grid_shadow",
    "mean_reversion",
    "trend",
)


def _mapping(value: object, name: str) -> Mapping[str, object]:
    """验证配置对象。"""
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} 必须为对象")
    return {str(key): item for key, item in value.items()}


def _numbers(value: object, name: str) -> tuple[int | float, ...]:
    """验证数值配置数组。"""
    if not isinstance(value, list) or not value:
        raise ValueError(f"{name} 必须为非空数组")
    result: list[int | float] = []
    for item in value:
        if not isinstance(item, (int, float)) or isinstance(item, bool):
            raise ValueError(f"{name} 只能包含数值")
        result.append(item)
    return tuple(result)


def _number(value: object, name: str) -> float:
    """验证单个数值。"""
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValueError(f"{name} 必须为数值")
    return float(value)


def _candidate(
    family: str,
    mode: str,
    parameters: Mapping[str, int | float],
) -> CandidateSpec:
    """生成确定性候选身份。"""
    template = strategy_expression(family)
    if template.mode != mode:
        raise ValueError(f"策略表达式模式不一致: {family}")
    return CandidateSpec(
        candidate_id=candidate_identity(template, parameters),
        family=family,
        mode=mode,
        parameters=dict(parameters),
        complexity=len(parameters),
        expression_id=expression_id(template),
    )


def build_candidates(
    config: Mapping[str, object],
    family_scope: Sequence[str] | None = None,
) -> tuple[CandidateSpec, ...]:
    """从版本化配置展开全部候选。"""
    strategies = _mapping(config.get("strategies"), "strategies")
    requested = (
        SUPPORTED_FAMILIES
        if family_scope is None
        else tuple(sorted(set(family_scope)))
    )
    if not requested:
        raise ValueError("策略家族范围不得为空")
    unknown = sorted(set(requested) - set(SUPPORTED_FAMILIES))
    if unknown:
        raise ValueError(f"未知策略家族: {','.join(unknown)}")
    result: list[CandidateSpec] = []
    if "trend" in requested:
        trend = _mapping(strategies.get("trend"), "strategies.trend")
        for lookback in _numbers(trend.get("lookbacks"), "trend.lookbacks"):
            for entry in _numbers(trend.get("entry_scores"), "trend.entry_scores"):
                result.append(_candidate("trend", "paper", {
                "lookback": int(lookback),
                "entry_score": float(entry),
                "exit_score": _number(trend.get("exit_score"), "trend.exit_score"),
                "annual_volatility_target": _number(
                    trend.get("annual_volatility_target"),
                    "trend.annual_volatility_target",
                ),
                "maximum_target": _number(
                    trend.get("maximum_target"), "trend.maximum_target",
                ),
                }))
    if "flow_trend" in requested:
        flow_trend = _mapping(
            strategies.get("flow_trend"), "strategies.flow_trend",
        )
        for lookback in _numbers(
            flow_trend.get("lookbacks"), "flow_trend.lookbacks",
        ):
            for entry in _numbers(
                flow_trend.get("entry_scores"), "flow_trend.entry_scores",
            ):
                for confirmation in _numbers(
                    flow_trend.get("flow_confirmations"),
                    "flow_trend.flow_confirmations",
                ):
                    result.append(_candidate("flow_trend", "paper", {
                    "lookback": int(lookback),
                    "entry_score": float(entry),
                    "flow_confirmation": float(confirmation),
                    "minimum_volume_score": _number(
                        flow_trend.get("minimum_volume_score"),
                        "flow_trend.minimum_volume_score",
                    ),
                    "exit_score": _number(
                        flow_trend.get("exit_score"),
                        "flow_trend.exit_score",
                    ),
                    "annual_volatility_target": _number(
                        flow_trend.get("annual_volatility_target"),
                        "flow_trend.annual_volatility_target",
                    ),
                    "maximum_target": _number(
                        flow_trend.get("maximum_target"),
                        "flow_trend.maximum_target",
                    ),
                    }))
    if "breakout" in requested:
        breakout = _mapping(strategies.get("breakout"), "strategies.breakout")
        for lookback in _numbers(breakout.get("lookbacks"), "breakout.lookbacks"):
            for confirmation in _numbers(
                breakout.get("flow_confirmations"), "breakout.flow_confirmations",
            ):
                result.append(_candidate("breakout", "paper", {
                "lookback": int(lookback),
                "flow_confirmation": float(confirmation),
                "annual_volatility_target": _number(
                    breakout.get("annual_volatility_target"),
                    "breakout.annual_volatility_target",
                ),
                "maximum_target": _number(
                    breakout.get("maximum_target"), "breakout.maximum_target",
                ),
                }))
    if "mean_reversion" in requested:
        reversion = _mapping(
            strategies.get("mean_reversion"), "strategies.mean_reversion",
        )
        for lookback in _numbers(reversion.get("lookbacks"), "mean_reversion.lookbacks"):
            for entry in _numbers(
                reversion.get("entry_scores"), "mean_reversion.entry_scores",
            ):
                result.append(_candidate("mean_reversion", "paper", {
                "lookback": int(lookback),
                "entry_score": float(entry),
                "exit_score": _number(
                    reversion.get("exit_score"), "mean_reversion.exit_score",
                ),
                "trend_limit": _number(
                    reversion.get("trend_limit"), "mean_reversion.trend_limit",
                ),
                "annual_volatility_target": _number(
                    reversion.get("annual_volatility_target"),
                    "mean_reversion.annual_volatility_target",
                ),
                "maximum_target": _number(
                    reversion.get("maximum_target"), "mean_reversion.maximum_target",
                ),
                }))
    if "grid_shadow" in requested:
        grid = _mapping(strategies.get("grid_shadow"), "strategies.grid_shadow")
        for lookback in _numbers(grid.get("lookbacks"), "grid_shadow.lookbacks"):
            for entry in _numbers(grid.get("entry_scores"), "grid_shadow.entry_scores"):
                result.append(_candidate("grid_shadow", "shadow", {
                "lookback": int(lookback),
                "entry_score": float(entry),
                "maximum_target": _number(
                    grid.get("maximum_target"), "grid_shadow.maximum_target",
                ),
                }))
    return tuple(result)


def _parameter(candidate: CandidateSpec, name: str) -> float:
    """读取候选参数。"""
    value = candidate.parameters.get(name)
    if value is None:
        raise ValueError(f"候选缺少参数: {name}")
    return float(value)


def _scaled_target(
    feature: FeatureRow,
    lookback: int,
    annual_target: float,
    maximum: float,
    periods_per_year: float,
) -> float:
    """按实现波动率缩放目标。"""
    hourly = feature.volatility.get(lookback)
    if hourly is None or hourly <= 0:
        return 0.0
    annual = hourly * math.sqrt(periods_per_year)
    return min(maximum, annual_target / annual)


def generate_targets(
    candidate: CandidateSpec,
    bars: Sequence[ResearchBar],
    features: Sequence[FeatureRow],
    periods_per_year: float,
) -> tuple[float, ...]:
    """以同一纯函数生成回测与当前目标。"""
    if len(bars) != len(features):
        raise ValueError("行情柱与特征数量不一致")
    if periods_per_year <= 0:
        raise ValueError("年化周期必须为正")
    template = strategy_expression(candidate.family)
    expected_expression_id = expression_id(template)
    if candidate.expression_id is not None:
        if candidate.expression_id != expected_expression_id:
            raise ValueError("候选表达式身份与流派模板不一致")
        if candidate.candidate_id != candidate_identity(template, candidate.parameters):
            raise ValueError("候选身份与表达式及参数不一致")
    lookback = int(_parameter(candidate, "lookback"))
    position = 0.0
    targets: list[float] = []
    for bar, feature in zip(bars, features, strict=True):
        if feature.as_of > feature.decision_time or not feature.contiguous:
            position = 0.0
            targets.append(position)
            continue
        required_valid = all(
            evaluate_expression(node, candidate.parameters, bar, feature) is not None
            for node in template.required
        )
        if not required_valid:
            position = 0.0
            targets.append(position)
            continue
        if template.sizing == "expression_target":
            if template.target is None:
                raise ValueError("表达式目标策略缺少 target AST")
            target = evaluate_expression(
                template.target,
                candidate.parameters,
                bar,
                feature,
            )
            position = (
                float(target)
                if isinstance(target, (int, float)) and not isinstance(target, bool)
                else 0.0
            )
            targets.append(position)
            continue
        if template.entry is None or template.exit is None:
            raise ValueError("状态策略缺少 entry 或 exit AST")
        entry = evaluate_expression(
            template.entry,
            candidate.parameters,
            bar,
            feature,
        )
        exit_signal = evaluate_expression(
            template.exit,
            candidate.parameters,
            bar,
            feature,
        )
        if position <= 0 and entry is True:
            position = _scaled_target(
                feature,
                lookback,
                _parameter(candidate, "annual_volatility_target"),
                _parameter(candidate, "maximum_target"),
                periods_per_year,
            )
        elif position > 0 and (exit_signal is True or exit_signal is None):
            position = 0.0
        targets.append(position)
    return tuple(targets)
