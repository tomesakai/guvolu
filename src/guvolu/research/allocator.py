"""市场状态约束下的策略软分配器。"""
from __future__ import annotations

import math
import statistics
from collections.abc import Mapping, Sequence

from guvolu.research.contracts import (
    AllocationResult,
    FamilyEvaluation,
    QualityVector,
)
from guvolu.research.features import MarketState


def _number(value: object, name: str) -> float:
    """验证数值配置。"""
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValueError(f"{name} 必须为数值")
    return float(value)


def _integer(value: object, name: str) -> int:
    """验证正整数配置。"""
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{name} 必须为正整数")
    return value


def _strings(value: object, name: str) -> tuple[str, ...]:
    """验证非空字符串数组。"""
    if not isinstance(value, list) or not value:
        raise ValueError(f"{name} 必须为非空数组")
    if not all(isinstance(item, str) and item for item in value):
        raise ValueError(f"{name} 只能包含非空字符串")
    return tuple(value)


def _covariance(left: Sequence[float], right: Sequence[float]) -> float:
    """计算同长收益序列协方差。"""
    if len(left) != len(right) or len(left) < 2:
        return 0.0
    left_mean = statistics.fmean(left)
    right_mean = statistics.fmean(right)
    return statistics.fmean(
        (x - left_mean) * (y - right_mean)
        for x, y in zip(left, right, strict=True)
    )


def _project(
    values: dict[str, float],
    gross_cap: float,
    directional_cap: float,
    reversion_cap: float,
    directional_families: Sequence[str],
) -> None:
    """投影到家族和总风险上限。"""
    for key in values:
        values[key] = max(values[key], 0.0)
    directional = set(directional_families)
    directional_keys = [key for key in values if key in directional]
    directional_total = sum(values[key] for key in directional_keys)
    if directional_total > directional_cap and directional_total > 0:
        scale = directional_cap / directional_total
        for key in directional_keys:
            values[key] *= scale
    if "mean_reversion" in values:
        values["mean_reversion"] = min(values["mean_reversion"], reversion_cap)
    gross = sum(values.values())
    if gross > gross_cap and gross > 0:
        scale = gross_cap / gross
        for key in values:
            values[key] *= scale


def _regime_caps(
    state: MarketState,
    configured_trend_cap: float,
    configured_reversion_cap: float,
) -> tuple[float, float]:
    """按市场状态收紧家族上限。"""
    if state.regime == "unconditional":
        return configured_trend_cap, configured_reversion_cap
    if state.regime in {"jump_risk", "negative_trend"}:
        return 0.0, 0.0
    if state.regime == "positive_trend":
        return configured_trend_cap, min(configured_reversion_cap, 0.1)
    if state.regime == "range":
        return min(configured_trend_cap, 0.15), configured_reversion_cap
    return min(configured_trend_cap, 0.4), min(configured_reversion_cap, 0.15)


def flat_allocation(regime: str, families: Sequence[str]) -> AllocationResult:
    """生成质量失败时的空仓结果。"""
    return AllocationResult(
        weights={family: 0.0 for family in families},
        reserve=1.0,
        objective=0.0,
        regime=regime,
        iterations=0,
    )


def allocate(
    evaluations: Sequence[FamilyEvaluation],
    state: MarketState,
    quality: QualityVector,
    config: Mapping[str, object],
    l2_overlay: float = 0.0,
    previous_weights: Mapping[str, float] | None = None,
) -> AllocationResult:
    """求解带风险、换手和不确定性惩罚的目标。"""
    family_names = tuple(sorted(item.family for item in evaluations))
    if not quality.eligible:
        return flat_allocation(state.regime, family_names)
    previous = dict(previous_weights or {})
    directional_families = _strings(
        config.get("directional_families"), "directional_families",
    )
    maximum_gross = _number(config.get("maximum_gross_weight"), "maximum_gross_weight")
    configured_trend = _number(config.get("trend_breakout_cap"), "trend_breakout_cap")
    configured_reversion = _number(config.get("mean_reversion_cap"), "mean_reversion_cap")
    minimum_reserve = _number(config.get("minimum_risk_reserve"), "minimum_risk_reserve")
    maximum_gross = min(maximum_gross, 1.0 - minimum_reserve)
    overlay_limit = _number(config.get("l2_overlay_limit"), "l2_overlay_limit")
    risk_aversion = _number(config.get("risk_aversion"), "risk_aversion")
    turnover_penalty = _number(config.get("turnover_penalty"), "turnover_penalty")
    uncertainty_penalty = _number(
        config.get("uncertainty_penalty"), "uncertainty_penalty",
    )
    no_trade_band = _number(config.get("no_trade_band"), "no_trade_band")
    iterations = _integer(config.get("solver_iterations"), "solver_iterations")
    step = _number(config.get("solver_step"), "solver_step")
    trend_cap, reversion_cap = _regime_caps(
        state,
        configured_trend,
        configured_reversion,
    )
    eligible = {
        item.family: item for item in evaluations
        if item.eligible and item.mode == "paper" and item.latest_target > 0
    }
    if not eligible or (trend_cap == 0 and reversion_cap == 0):
        return flat_allocation(state.regime, family_names)
    keys = tuple(sorted(eligible))
    expected = {
        key: max(eligible[key].metrics.annual_return, 0.0)
        * eligible[key].metrics.capacity_score
        for key in keys
    }
    annualization = statistics.fmean(
        eligible[key].periods_per_year for key in keys
    )
    covariance = {
        (left, right): _covariance(
            eligible[left].oos_returns,
            eligible[right].oos_returns,
        ) * annualization
        for left in keys for right in keys
    }
    uncertainty = {
        key: eligible[key].metrics.annual_volatility
        / math.sqrt(max(eligible[key].metrics.bars, 1))
        for key in keys
    }
    weights = {key: max(previous.get(key, 0.0), 0.0) for key in keys}
    _project(
        weights,
        maximum_gross,
        trend_cap,
        reversion_cap,
        directional_families,
    )
    for _ordinal in range(iterations):
        updated: dict[str, float] = {}
        for key in keys:
            risk_gradient = 2.0 * risk_aversion * sum(
                covariance[(key, other)] * weights[other] for other in keys
            )
            change = weights[key] - previous.get(key, 0.0)
            turnover_gradient = turnover_penalty * (
                1.0 if change > 0 else -1.0 if change < 0 else 0.0
            )
            gradient = (
                expected[key]
                - risk_gradient
                - turnover_gradient
                - uncertainty_penalty * uncertainty[key]
            )
            updated[key] = weights[key] + step * gradient
        weights = updated
        _project(
            weights,
            maximum_gross,
            trend_cap,
            reversion_cap,
            directional_families,
        )
    overlay = max(min(l2_overlay, 1.0), -1.0) * overlay_limit
    for key in weights:
        weights[key] *= 1.0 + overlay
        if abs(weights[key] - previous.get(key, 0.0)) < no_trade_band:
            weights[key] = previous.get(key, 0.0)
    _project(
        weights,
        maximum_gross,
        trend_cap,
        reversion_cap,
        directional_families,
    )
    complete = {family: weights.get(family, 0.0) for family in family_names}
    gross = sum(complete.values())
    objective = sum(expected.get(key, 0.0) * complete[key] for key in keys)
    objective -= risk_aversion * sum(
        complete[left] * covariance[(left, right)] * complete[right]
        for left in keys for right in keys
    )
    objective -= turnover_penalty * sum(
        abs(complete[key] - previous.get(key, 0.0)) for key in keys
    )
    objective -= uncertainty_penalty * sum(
        uncertainty[key] * complete[key] for key in keys
    )
    return AllocationResult(
        weights=complete,
        reserve=max(1.0 - gross, 0.0),
        objective=objective,
        regime=state.regime,
        iterations=iterations,
    )


def allocation_payload(result: AllocationResult) -> Mapping[str, object]:
    """把分配结果转换为 JSON 载荷。"""
    return {
        "weights": dict(result.weights),
        "reserve": result.reserve,
        "objective": result.objective,
        "regime": result.regime,
        "iterations": result.iterations,
    }
