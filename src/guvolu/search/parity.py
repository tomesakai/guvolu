"""数值对照：CPU f64 有序精确复算与容差比较。

容差为版本化配置（G-06），阈值不由 GPU 侧决定（禁区第 3 条）。
"""
from __future__ import annotations

import math
import statistics
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from guvolu.strategy.baselines import generate_targets
from guvolu.strategy.contracts import CandidateSpec, FeatureRow, ResearchBar

PARITY_TOLERANCE_VERSION = "searchfast-parity-tolerance-v1"
REFERENCE_METHOD_VERSION = "searchfast-cpu-exact-reference-v1"


@dataclass(frozen=True)
class ParityTolerance:
    """各指标的绝对差容差初值。"""

    target_abs: float = 1e-5
    sharpe_abs: float = 1e-3
    turnover_abs: float = 1e-6

    def payload(self) -> Mapping[str, object]:
        """生成可登记的容差配置。"""
        return {
            "tolerance_version": PARITY_TOLERANCE_VERSION,
            "target_abs": self.target_abs,
            "sharpe_abs": self.sharpe_abs,
            "turnover_abs": self.turnover_abs,
        }


def tolerance_from_config(config: Mapping[str, object] | None) -> ParityTolerance:
    """由配置读取容差，缺省使用初值。"""
    if config is None:
        return ParityTolerance()
    result = ParityTolerance()
    values: dict[str, float] = {}
    for name in ("target_abs", "sharpe_abs", "turnover_abs"):
        raw = config.get(name, getattr(result, name))
        if (
            not isinstance(raw, (int, float))
            or isinstance(raw, bool)
            or not math.isfinite(float(raw))
            or float(raw) < 0
        ):
            raise ValueError(f"容差必须为非负有限数值: {name}")
        values[name] = float(raw)
    return ParityTolerance(**values)


@dataclass(frozen=True)
class ReferenceMetrics:
    """CPU 精确复算的区段指标。"""

    bars: int
    net_return: float
    annual_return: float
    annual_volatility: float
    sharpe: float
    maximum_drawdown: float
    turnover: float
    annual_turnover: float
    hit_rate: float
    exposure: float
    cost: float

    def payload(self) -> Mapping[str, float | int]:
        """导出为数值行。"""
        return {
            "bars": self.bars,
            "net_return": self.net_return,
            "annual_return": self.annual_return,
            "annual_volatility": self.annual_volatility,
            "sharpe": self.sharpe,
            "maximum_drawdown": self.maximum_drawdown,
            "turnover": self.turnover,
            "annual_turnover": self.annual_turnover,
            "hit_rate": self.hit_rate,
            "exposure": self.exposure,
            "cost": self.cost,
        }


def reference_returns(
    bars: Sequence[ResearchBar],
    targets: Sequence[float],
    one_way_cost_rate: float,
    maximum_gap_seconds: float | None,
) -> tuple[float, ...]:
    """以前一决策目标计算下一期成本后收益，与 validation 同口径。"""
    if len(bars) != len(targets):
        raise ValueError("行情柱与目标数量不一致")
    if one_way_cost_rate < 0:
        raise ValueError("成本率不得为负")
    result = [0.0]
    for index in range(1, len(bars)):
        held = targets[index - 1]
        previous = targets[index - 2] if index >= 2 else 0.0
        turnover = abs(held - previous)
        gap_seconds = (
            bars[index].open_time - bars[index - 1].open_time
        ).total_seconds()
        if maximum_gap_seconds is not None and gap_seconds > maximum_gap_seconds:
            result.append(-(turnover + abs(held)) * one_way_cost_rate)
            continue
        market_return = math.log(bars[index].close / bars[index - 1].close)
        result.append(held * market_return - turnover * one_way_cost_rate)
    return tuple(result)


def reference_metrics(
    bars: Sequence[ResearchBar],
    targets: Sequence[float],
    one_way_cost_rate: float,
    maximum_gap_seconds: float | None,
    periods_per_year: float,
) -> ReferenceMetrics:
    """以 f64 有序累加计算 [1, B) 区段指标。"""
    if len(bars) < 2:
        raise ValueError("柱数至少为二")
    if periods_per_year <= 0:
        raise ValueError("年化周期必须为正")
    returns = reference_returns(bars, targets, one_way_cost_rate, maximum_gap_seconds)
    segment = list(returns[1:])
    count = len(segment)
    mean = statistics.fmean(segment)
    standard = statistics.pstdev(segment) if count > 1 else 0.0
    sharpe = mean / standard * math.sqrt(periods_per_year) if standard > 0 else 0.0
    cumulative = 0.0
    peak = 0.0
    drawdown = 0.0
    for value in segment:
        cumulative += value
        peak = max(peak, cumulative)
        drawdown = max(drawdown, 1.0 - math.exp(cumulative - peak))
    turnovers: list[float] = []
    for index in range(1, len(bars)):
        held = targets[index - 1]
        previous = targets[index - 2] if index >= 2 else 0.0
        turnover = abs(held - previous)
        gap_seconds = (
            bars[index].open_time - bars[index - 1].open_time
        ).total_seconds()
        if maximum_gap_seconds is not None and gap_seconds > maximum_gap_seconds:
            turnover += abs(held)
        turnovers.append(turnover)
    turnover_total = math.fsum(turnovers)
    return ReferenceMetrics(
        bars=count,
        net_return=math.fsum(segment),
        annual_return=mean * periods_per_year,
        annual_volatility=standard * math.sqrt(periods_per_year),
        sharpe=sharpe,
        maximum_drawdown=drawdown,
        turnover=turnover_total,
        annual_turnover=turnover_total / count * periods_per_year,
        hit_rate=sum(value > 0 for value in segment) / count,
        exposure=statistics.fmean(abs(targets[index - 1]) for index in range(1, len(bars))),
        cost=turnover_total * one_way_cost_rate,
    )


def exact_reference(
    candidate: CandidateSpec,
    bars: Sequence[ResearchBar],
    features: Sequence[FeatureRow],
    cost_model: Mapping[str, object],
) -> tuple[tuple[float, ...], ReferenceMetrics]:
    """对一个候选执行 CPU 精确复算：目标序列与指标。"""
    rate = cost_model.get("one_way_cost_rate")
    gap = cost_model.get("maximum_gap_seconds")
    periods = cost_model.get("periods_per_year")
    if not isinstance(rate, (int, float)) or not isinstance(periods, (int, float)):
        raise ValueError("成本模型缺少成本率或年化周期")
    if gap is not None and not isinstance(gap, (int, float)):
        raise ValueError("maximum_gap_seconds 必须为数值或空")
    targets = generate_targets(candidate, bars, features, float(periods))
    metrics = reference_metrics(
        bars,
        targets,
        float(rate),
        None if gap is None else float(gap),
        float(periods),
    )
    return targets, metrics


@dataclass(frozen=True)
class ParityResult:
    """一个候选的 GPU 对 CPU 数值对照。"""

    target_max_abs_diff: float
    sharpe_abs_diff: float
    turnover_abs_diff: float
    tolerance: ParityTolerance
    passed: bool

    def payload(self) -> Mapping[str, object]:
        """导出为台账字段。"""
        return {
            "max_abs_diff": {
                "target": self.target_max_abs_diff,
                "sharpe": self.sharpe_abs_diff,
                "turnover": self.turnover_abs_diff,
            },
            "tolerance": self.tolerance.payload(),
            "passed": self.passed,
        }


def compare_parity(
    fast_targets: Sequence[float],
    fast_metrics: Mapping[str, float | int],
    reference_targets: Sequence[float],
    reference: ReferenceMetrics,
    tolerance: ParityTolerance,
) -> ParityResult:
    """按容差比较 GPU 粗筛结果与 CPU 精确复算。"""
    if len(fast_targets) != len(reference_targets):
        raise ValueError("目标序列长度不一致")
    target_diff = max(
        (abs(float(left) - float(right))
         for left, right in zip(fast_targets, reference_targets, strict=True)),
        default=0.0,
    )
    sharpe_diff = abs(float(fast_metrics["sharpe"]) - reference.sharpe)
    turnover_diff = abs(float(fast_metrics["turnover"]) - reference.turnover)
    finite = all(math.isfinite(value) for value in (target_diff, sharpe_diff, turnover_diff))
    passed = finite and (
        target_diff <= tolerance.target_abs
        and sharpe_diff <= tolerance.sharpe_abs
        and turnover_diff <= tolerance.turnover_abs
    )
    return ParityResult(
        target_max_abs_diff=target_diff,
        sharpe_abs_diff=sharpe_diff,
        turnover_abs_diff=turnover_diff,
        tolerance=tolerance,
        passed=passed,
    )
