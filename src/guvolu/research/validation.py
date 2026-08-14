"""成本后 walk-forward 与多重检验。"""
from __future__ import annotations

import hashlib
import itertools
import math
import random
import statistics
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

from guvolu.research.contracts import (
    FamilyEvaluation,
    PerformanceMetrics,
    TrialRecord,
)
from guvolu.research.provenance import stable_identifier
from guvolu.strategy.baselines import generate_targets
from guvolu.strategy.contracts import CandidateSpec, FeatureRow, ResearchBar

SECONDS_PER_YEAR = 365.0 * 24.0 * 60.0 * 60.0
P_VALUE_METHOD_VERSION = "probabilistic-sharpe-nonnormal-v1"
PBO_METHOD_VERSION = "cscv-balanced-fold-block-v2"
BLOCK_BOOTSTRAP_METHOD_VERSION = "circular-block-bootstrap-sharpe-v1"
DEFLATED_SHARPE_METHOD_VERSION = "deflated-sharpe-family-effective-gate-v3"
EFFECTIVE_TRIAL_METHOD_VERSION = "fold-score-correlation-participation-v1"
PARAMETER_STABILITY_METHOD_VERSION = "one-axis-nearest-neighbor-v1"
_INTERVAL_SECONDS = {
    "5min": 300.0,
    "15min": 900.0,
    "1hour": 3600.0,
    "4hour": 14_400.0,
}


@dataclass(frozen=True)
class WalkForwardFold:
    """扩展训练窗与隔离测试窗。"""

    fold_id: str
    train_start: int
    train_end: int
    test_start: int
    test_end: int


@dataclass(frozen=True)
class ValidationResult:
    """全家族验证及候选目标。"""

    families: tuple[FamilyEvaluation, ...]
    trials: tuple[TrialRecord, ...]
    candidate_targets: Mapping[str, tuple[float, ...]]
    folds: tuple[WalkForwardFold, ...]
    family_validation_targets: Mapping[str, tuple[float, ...]] = field(
        default_factory=dict,
    )


@dataclass(frozen=True)
class _PendingFamily:
    """FDR 计算前暂存的一组家族验证事实。"""

    family: str
    mode: str
    deployment_candidate: CandidateSpec
    fold_selected_candidate_ids: tuple[str, ...]
    oos_returns: tuple[float, ...]
    metrics: PerformanceMetrics
    adjusted_sharpe: float
    positive_fold_ratio: float
    most_selected_share: float
    median_selected_sharpe: float
    pbo: float
    median_cscv_rank: float
    cscv_split_count: int
    bootstrap_lower: float
    bootstrap_p: float
    bootstrap_count: int


def _mapping(value: object, name: str) -> Mapping[str, object]:
    """验证配置对象。"""
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} 必须为对象")
    return {str(key): item for key, item in value.items()}


def _integer(value: object, name: str) -> int:
    """验证正整数配置。"""
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{name} 必须为正整数")
    return value


def _number(value: object, name: str) -> float:
    """验证数值配置。"""
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValueError(f"{name} 必须为数值")
    return float(value)


def _probabilistic_sharpe_probability(
    values: Sequence[float],
    benchmark: float = 0.0,
) -> float:
    """返回非正态修正后 Sharpe 超过指定基准的概率。"""
    count = len(values)
    if count < 3:
        return 0.0
    mean = statistics.fmean(values)
    standard = statistics.pstdev(values)
    if standard <= 0:
        if mean > benchmark:
            return 1.0
        if mean < benchmark:
            return 0.0
        return 0.5
    centered = [(value - mean) / standard for value in values]
    skewness = statistics.fmean(value ** 3 for value in centered)
    kurtosis = statistics.fmean(value ** 4 for value in centered)
    period_sharpe = mean / standard
    variance_term = (
        1.0
        - skewness * period_sharpe
        + ((kurtosis - 1.0) / 4.0) * period_sharpe * period_sharpe
    )
    if variance_term <= 0:
        if period_sharpe > benchmark:
            return 1.0
        if period_sharpe < benchmark:
            return 0.0
        return 0.5
    statistic = (
        (period_sharpe - benchmark)
        * math.sqrt(count - 1)
        / math.sqrt(variance_term)
    )
    return min(max(0.5 * math.erfc(-statistic / math.sqrt(2.0)), 0.0), 1.0)


def _probabilistic_sharpe_p_value(values: Sequence[float]) -> float:
    """以偏度和峰度修正 Sharpe 大于零的单侧 p 值。"""
    return 1.0 - _probabilistic_sharpe_probability(values)


def _deflated_sharpe_probability(
    values: Sequence[float],
    trial_period_sharpes: Sequence[float],
    trial_count: float,
) -> tuple[float, float]:
    """计算 Bailey--López de Prado DSR 与每期 Sharpe 基准。"""
    if trial_count < 1.0:
        raise ValueError("DSR 试验数不得小于一")
    if not trial_period_sharpes:
        raise ValueError("DSR 缺少试验 Sharpe")
    dispersion = (
        statistics.pstdev(trial_period_sharpes)
        if len(trial_period_sharpes) > 1
        else 0.0
    )
    benchmark = 0.0
    if trial_count > 1.0 and dispersion > 0.0:
        normal = statistics.NormalDist()
        euler_mascheroni = 0.5772156649015329
        expected_maximum = (
            (1.0 - euler_mascheroni)
            * normal.inv_cdf(1.0 - 1.0 / trial_count)
            + euler_mascheroni
            * normal.inv_cdf(1.0 - 1.0 / (trial_count * math.e))
        )
        benchmark = dispersion * max(expected_maximum, 0.0)
    return _probabilistic_sharpe_probability(values, benchmark), benchmark


def _effective_trial_count(
    fold_scores: Mapping[str, Sequence[float]],
) -> float:
    """以折级得分相关矩阵参与率估计有效试验数。"""
    identifiers = tuple(sorted(fold_scores))
    count = len(identifiers)
    if count <= 1:
        return float(count)
    lengths = {len(fold_scores[identifier]) for identifier in identifiers}
    if len(lengths) != 1 or not lengths or next(iter(lengths)) < 2:
        raise ValueError("有效试验数需要同长的至少两个折级得分")

    def correlation(left: Sequence[float], right: Sequence[float]) -> float:
        left_mean = statistics.fmean(left)
        right_mean = statistics.fmean(right)
        left_centered = [value - left_mean for value in left]
        right_centered = [value - right_mean for value in right]
        left_square = sum(value * value for value in left_centered)
        right_square = sum(value * value for value in right_centered)
        if left_square <= 0.0 or right_square <= 0.0:
            return 1.0 if tuple(left) == tuple(right) else 0.0
        value = sum(
            left_value * right_value
            for left_value, right_value in zip(
                left_centered,
                right_centered,
                strict=True,
            )
        ) / math.sqrt(left_square * right_square)
        return min(max(value, -1.0), 1.0)

    squared_sum = 0.0
    for left in identifiers:
        for right in identifiers:
            value = 1.0 if left == right else correlation(
                fold_scores[left],
                fold_scores[right],
            )
            squared_sum += value * value
    return min(max(count * count / squared_sum, 1.0), float(count))


def _parameter_neighbors(
    selected: CandidateSpec,
    candidates: Sequence[CandidateSpec],
) -> tuple[CandidateSpec, ...]:
    """返回其他参数不变时，每个数值轴最近的上下邻居。"""
    selected_keys = set(selected.parameters)
    neighbors: dict[str, CandidateSpec] = {}
    for parameter in sorted(selected.parameters):
        selected_value = float(selected.parameters[parameter])
        axis_candidates = [
            candidate
            for candidate in candidates
            if candidate.candidate_id != selected.candidate_id
            and set(candidate.parameters) == selected_keys
            and all(
                candidate.parameters[name] == selected.parameters[name]
                for name in selected_keys
                if name != parameter
            )
        ]
        lower = [
            candidate for candidate in axis_candidates
            if float(candidate.parameters[parameter]) < selected_value
        ]
        upper = [
            candidate for candidate in axis_candidates
            if float(candidate.parameters[parameter]) > selected_value
        ]
        if lower:
            candidate = max(
                lower,
                key=lambda item: (
                    float(item.parameters[parameter]),
                    item.candidate_id,
                ),
            )
            neighbors[candidate.candidate_id] = candidate
        if upper:
            candidate = min(
                upper,
                key=lambda item: (
                    float(item.parameters[parameter]),
                    item.candidate_id,
                ),
            )
            neighbors[candidate.candidate_id] = candidate
    return tuple(neighbors[key] for key in sorted(neighbors))


def _probability_backtest_overfitting(
    fold_scores: Mapping[str, Sequence[float]],
    split_budget: int,
    seed: int,
) -> tuple[float, float, int]:
    """在 walk-forward 折块上执行确定性 CSCV/PBO 诊断。"""
    identifiers = tuple(sorted(fold_scores))
    if len(identifiers) < 2:
        return 0.0, 1.0, 0
    fold_count = len(fold_scores[identifiers[0]])
    if fold_count < 4 or any(
        len(fold_scores[identifier]) != fold_count for identifier in identifiers
    ):
        raise ValueError("PBO 需要至少四个同长测试折")
    half = fold_count // 2
    paired_halves = fold_count % 2 == 0
    total_unique = math.comb(fold_count, half) // (2 if paired_halves else 1)
    target = min(split_budget, total_unique)
    subsets: set[tuple[int, ...]] = set()
    if total_unique <= split_budget:
        for raw_subset in itertools.combinations(range(fold_count), half):
            complement = tuple(
                index for index in range(fold_count) if index not in raw_subset
            )
            subsets.add(min(raw_subset, complement) if paired_halves else raw_subset)
    else:
        generator = random.Random(seed)
        while len(subsets) < target:
            raw_subset = tuple(sorted(generator.sample(range(fold_count), half)))
            complement = tuple(
                index for index in range(fold_count) if index not in raw_subset
            )
            subsets.add(min(raw_subset, complement) if paired_halves else raw_subset)
    below_median = 0
    ranks: list[float] = []
    for subset in sorted(subsets):
        complement = tuple(
            index for index in range(fold_count) if index not in subset
        )
        in_sample = {
            identifier: statistics.fmean(
                fold_scores[identifier][index] for index in subset
            )
            for identifier in identifiers
        }
        out_sample = {
            identifier: statistics.fmean(
                fold_scores[identifier][index] for index in complement
            )
            for identifier in identifiers
        }
        winner = min(
            identifiers,
            key=lambda identifier: (-in_sample[identifier], identifier),
        )
        winner_score = out_sample[winner]
        lower_count = sum(score < winner_score for score in out_sample.values())
        tie_count = sum(score == winner_score for score in out_sample.values())
        relative_rank = (lower_count + tie_count / 2.0) / len(identifiers)
        ranks.append(relative_rank)
        below_median += int(relative_rank <= 0.5)
    return (
        below_median / len(subsets),
        statistics.median(ranks),
        len(subsets),
    )


def _quantile(values: Sequence[float], probability: float) -> float:
    """以线性插值计算确定性经验分位数。"""
    if not values or probability < 0.0 or probability > 1.0:
        raise ValueError("经验分位数输入非法")
    ordered = sorted(values)
    position = probability * (len(ordered) - 1)
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def _circular_block_bootstrap_sharpe(
    values: Sequence[float],
    block_bars: int,
    samples: int,
    one_sided_alpha: float,
    seed: int,
    periods_per_year: float = 365.0 * 24.0,
) -> tuple[float, float, int]:
    """以循环折块保留短程依赖，估计 Sharpe 下界和小于零概率。"""
    count = len(values)
    if count < 2:
        raise ValueError("block bootstrap 需要至少两个收益观测")
    if block_bars <= 0 or block_bars > count:
        raise ValueError("block bootstrap 折块长度超出收益序列")
    if samples <= 0:
        raise ValueError("block bootstrap 样本数必须为正")
    if one_sided_alpha <= 0.0 or one_sided_alpha >= 0.5:
        raise ValueError("block bootstrap 单侧 alpha 必须位于 (0, 0.5)")
    doubled = tuple(values) + tuple(values)
    prefix = [0.0]
    prefix_square = [0.0]
    for value in doubled:
        prefix.append(prefix[-1] + value)
        prefix_square.append(prefix_square[-1] + value * value)
    full_blocks, remainder = divmod(count, block_bars)
    generator = random.Random(seed)
    sharpes: list[float] = []
    non_positive = 0
    for _sample in range(samples):
        total = 0.0
        total_square = 0.0
        for _block in range(full_blocks):
            start = generator.randrange(count)
            end = start + block_bars
            total += prefix[end] - prefix[start]
            total_square += prefix_square[end] - prefix_square[start]
        if remainder:
            start = generator.randrange(count)
            end = start + remainder
            total += prefix[end] - prefix[start]
            total_square += prefix_square[end] - prefix_square[start]
        mean = total / count
        variance = max(total_square / count - mean * mean, 0.0)
        if variance <= 0.0:
            sharpe = 0.0
        else:
            sharpe = mean / math.sqrt(variance) * math.sqrt(periods_per_year)
        sharpes.append(sharpe)
        non_positive += int(sharpe <= 0.0)
    return (
        _quantile(sharpes, one_sided_alpha),
        (non_positive + 1) / (samples + 1),
        samples,
    )


def make_folds(bar_count: int, config: Mapping[str, object]) -> tuple[WalkForwardFold, ...]:
    """生成带 embargo 的扩展 walk-forward。"""
    minimum_train = _integer(config.get("minimum_train_bars"), "minimum_train_bars")
    test_bars = _integer(config.get("test_bars"), "test_bars")
    step_bars = _integer(config.get("step_bars"), "step_bars")
    embargo = _integer(config.get("embargo_bars"), "embargo_bars")
    folds: list[WalkForwardFold] = []
    test_start = minimum_train + embargo
    ordinal = 1
    while test_start + test_bars <= bar_count:
        train_end = test_start - embargo
        folds.append(WalkForwardFold(
            fold_id=f"fold-{ordinal:03d}",
            train_start=1,
            train_end=train_end,
            test_start=test_start,
            test_end=test_start + test_bars,
        ))
        ordinal += 1
        test_start += step_bars
    if not folds:
        raise ValueError("样本不足以生成 walk-forward 测试窗")
    return tuple(folds)


def strategy_returns(
    bars: Sequence[ResearchBar],
    targets: Sequence[float],
    one_way_cost_rate: float,
    maximum_gap_seconds: float | None = None,
) -> tuple[float, ...]:
    """以前一决策目标计算下一期成本后收益。"""
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


def _turnovers(
    bars: Sequence[ResearchBar],
    targets: Sequence[float],
    start: int,
    end: int,
    maximum_gap_seconds: float | None,
) -> list[float]:
    """计算含数据断流平仓的单边换手。"""
    values: list[float] = []
    for index in range(start, end):
        held = targets[index - 1]
        previous = targets[index - 2] if index >= 2 else 0.0
        turnover = abs(held - previous)
        gap_seconds = (
            bars[index].open_time - bars[index - 1].open_time
        ).total_seconds()
        if maximum_gap_seconds is not None and gap_seconds > maximum_gap_seconds:
            turnover += abs(held)
        values.append(turnover)
    return values


def _metrics_from_returns(
    bars: Sequence[ResearchBar],
    targets: Sequence[float],
    returns: Sequence[float],
    start: int,
    end: int,
    one_way_cost_rate: float,
    capacity_notional: float,
    maximum_gap_seconds: float | None,
    periods_per_year: float,
) -> PerformanceMetrics:
    """计算指定区段的净成本指标。"""
    if start < 1 or end > len(bars) or start >= end:
        raise ValueError("评估区段非法")
    segment = list(returns[start:end])
    count = len(segment)
    mean = statistics.fmean(segment)
    standard = statistics.pstdev(segment) if count > 1 else 0.0
    sharpe = mean / standard * math.sqrt(periods_per_year) if standard > 0 else 0.0
    annual_return = mean * periods_per_year
    annual_volatility = standard * math.sqrt(periods_per_year)
    cumulative = 0.0
    peak = 0.0
    drawdown = 0.0
    for value in segment:
        cumulative += value
        peak = max(peak, cumulative)
        drawdown = max(drawdown, 1.0 - math.exp(cumulative - peak))
    turnovers = _turnovers(
        bars,
        targets,
        start,
        end,
        maximum_gap_seconds,
    )
    turnover = sum(turnovers)
    cost = turnover * one_way_cost_rate
    hit_rate = sum(value > 0 for value in segment) / count
    exposure = statistics.fmean(abs(targets[index - 1]) for index in range(start, end))
    median_notional = statistics.median(
        bar.quote_volume for bar in bars[start:end]
    )
    capacity_score = (
        min(median_notional / capacity_notional, 1.0)
        if capacity_notional > 0 else 0.0
    )
    return PerformanceMetrics(
        bars=count,
        net_return=sum(segment),
        annual_return=annual_return,
        annual_volatility=annual_volatility,
        sharpe=sharpe,
        maximum_drawdown=drawdown,
        turnover=turnover,
        annual_turnover=turnover / count * periods_per_year,
        hit_rate=hit_rate,
        exposure=exposure,
        cost=cost,
        p_value=_probabilistic_sharpe_p_value(segment),
        capacity_score=capacity_score,
    )


def evaluate_targets(
    bars: Sequence[ResearchBar],
    targets: Sequence[float],
    start: int,
    end: int,
    one_way_cost_rate: float,
    capacity_notional: float,
    maximum_gap_seconds: float | None = None,
    periods_per_year: float = 365.0 * 24.0,
) -> PerformanceMetrics:
    """评估一个候选目标序列。"""
    returns = strategy_returns(
        bars,
        targets,
        one_way_cost_rate,
        maximum_gap_seconds,
    )
    return _metrics_from_returns(
        bars,
        targets,
        returns,
        start,
        end,
        one_way_cost_rate,
        capacity_notional,
        maximum_gap_seconds,
        periods_per_year,
    )


def _trial(
    run_id: str,
    candidate: CandidateSpec,
    fold_id: str,
    segment: str,
    selected: bool,
    metrics: PerformanceMetrics,
    bars: Sequence[ResearchBar],
    start: int,
    end: int,
) -> TrialRecord:
    """构造确定性试验事实。"""
    evaluation_id = stable_identifier("evaluation", {
        "run_id": run_id,
        "candidate_id": candidate.candidate_id,
        "fold_id": fold_id,
        "segment": segment,
    })
    return TrialRecord(
        evaluation_id=evaluation_id,
        candidate=candidate,
        fold_id=fold_id,
        segment=segment,
        start_time=bars[start].decision_time,
        end_time=bars[end - 1].decision_time,
        selected=selected,
        metrics=metrics,
    )


def _fdr_q_values(p_values: Mapping[str, float]) -> dict[str, float]:
    """计算 Benjamini-Hochberg 校正值。"""
    ordered = sorted(p_values.items(), key=lambda item: (item[1], item[0]))
    count = len(ordered)
    result: dict[str, float] = {}
    running = 1.0
    for reverse_index in range(count - 1, -1, -1):
        candidate_id, value = ordered[reverse_index]
        rank = reverse_index + 1
        running = min(running, value * count / rank)
        result[candidate_id] = min(running, 1.0)
    return result


def _masked_metrics(
    bars: Sequence[ResearchBar],
    targets: Sequence[float],
    returns: Sequence[float],
    mask: Sequence[bool],
    one_way_cost_rate: float,
    capacity_notional: float,
    maximum_gap_seconds: float | None,
    periods_per_year: float,
) -> PerformanceMetrics:
    """聚合不相邻的样本外测试段。"""
    indices = [index for index in range(1, len(bars)) if mask[index]]
    if not indices:
        raise ValueError("样本外测试段为空")
    selected_returns = [returns[index] for index in indices]
    count = len(selected_returns)
    mean = statistics.fmean(selected_returns)
    standard = statistics.pstdev(selected_returns) if count > 1 else 0.0
    sharpe = mean / standard * math.sqrt(periods_per_year) if standard > 0 else 0.0
    cumulative = 0.0
    peak = 0.0
    drawdown = 0.0
    for value in selected_returns:
        cumulative += value
        peak = max(peak, cumulative)
        drawdown = max(drawdown, 1.0 - math.exp(cumulative - peak))
    turnover_values = _turnovers(
        bars,
        targets,
        1,
        len(bars),
        maximum_gap_seconds,
    )
    turnover_values = [
        value for index, value in enumerate(turnover_values, start=1)
        if mask[index]
    ]
    turnover = sum(turnover_values)
    median_notional = statistics.median(bars[index].quote_volume for index in indices)
    return PerformanceMetrics(
        bars=count,
        net_return=sum(selected_returns),
        annual_return=mean * periods_per_year,
        annual_volatility=standard * math.sqrt(periods_per_year),
        sharpe=sharpe,
        maximum_drawdown=drawdown,
        turnover=turnover,
        annual_turnover=turnover / count * periods_per_year,
        hit_rate=sum(value > 0 for value in selected_returns) / count,
        exposure=statistics.fmean(abs(targets[index - 1]) for index in indices),
        cost=turnover * one_way_cost_rate,
        p_value=_probabilistic_sharpe_p_value(selected_returns),
        capacity_score=(
            min(median_notional / capacity_notional, 1.0)
            if capacity_notional > 0 else 0.0
        ),
    )


def walk_forward_validate(
    run_id: str,
    bars: Sequence[ResearchBar],
    features: Sequence[FeatureRow],
    candidates: Sequence[CandidateSpec],
    config: Mapping[str, object],
    decision_index: int | None = None,
) -> ValidationResult:
    """按家族训练选择并只消费被选测试路径。"""
    cost = _mapping(config.get("cost_model"), "cost_model")
    cost_bps = sum(_number(cost.get(name), name) for name in (
        "fee_bps_assumption",
        "half_spread_bps_assumption",
        "slippage_bps_assumption",
        "impact_bps_assumption",
    ))
    one_way_cost_rate = cost_bps / 10_000.0
    capacity_notional = _number(
        cost.get("capacity_notional_quote"), "capacity_notional_quote",
    )
    interval = config.get("bar_interval")
    if not isinstance(interval, str) or interval not in _INTERVAL_SECONDS:
        raise ValueError("bar_interval 不支持成本回放")
    feature_config = _mapping(config.get("features"), "features")
    structural_gap_bars = _integer(
        feature_config.get("maximum_structural_gap_bars_assumption"),
        "maximum_structural_gap_bars_assumption",
    )
    maximum_gap_seconds = _INTERVAL_SECONDS[interval] * structural_gap_bars
    periods_per_year = SECONDS_PER_YEAR / _INTERVAL_SECONDS[interval]
    folds = make_folds(
        len(bars),
        _mapping(config.get("walk_forward"), "walk_forward"),
    )
    validation = _mapping(config.get("validation"), "validation")
    complexity_penalty = _number(
        validation.get("complexity_penalty"), "complexity_penalty",
    )
    pbo_split_budget = _integer(
        validation.get("pbo_split_budget"), "pbo_split_budget",
    )
    pbo_random_seed = _integer(
        validation.get("pbo_random_seed"), "pbo_random_seed",
    )
    block_bootstrap_bars = _integer(
        validation.get("block_bootstrap_bars"), "block_bootstrap_bars",
    )
    block_bootstrap_samples = _integer(
        validation.get("block_bootstrap_samples"), "block_bootstrap_samples",
    )
    block_bootstrap_seed = _integer(
        validation.get("block_bootstrap_random_seed"),
        "block_bootstrap_random_seed",
    )
    block_bootstrap_alpha = _number(
        validation.get("block_bootstrap_one_sided_alpha"),
        "block_bootstrap_one_sided_alpha",
    )
    candidate_targets = {
        candidate.candidate_id: generate_targets(
            candidate, bars, features, periods_per_year,
        )
        for candidate in candidates
    }
    candidate_returns = {
        candidate.candidate_id: strategy_returns(
            bars,
            candidate_targets[candidate.candidate_id],
            one_way_cost_rate,
            maximum_gap_seconds,
        )
        for candidate in candidates
    }
    resolved_decision_index = (
        len(bars) - 1 if decision_index is None else decision_index
    )
    if resolved_decision_index < 0 or resolved_decision_index >= len(bars):
        raise ValueError("策略决策索引超出面板")
    common_oos_mask = [False] * len(bars)
    for fold in folds:
        for index in range(fold.test_start, fold.test_end):
            common_oos_mask[index] = True
    families = sorted({candidate.family for candidate in candidates})
    trials: list[TrialRecord] = []
    family_validation_targets: dict[str, tuple[float, ...]] = {}
    candidate_fold_scores: dict[str, tuple[float, ...]] = {}
    pending: list[_PendingFamily] = []
    for family in families:
        family_candidates = tuple(
            candidate for candidate in candidates if candidate.family == family
        )
        oos_returns = [0.0] * len(bars)
        oos_targets = [0.0] * len(bars)
        oos_mask = [False] * len(bars)
        selected_candidate_ids: list[str] = []
        selected_test_metrics: list[PerformanceMetrics] = []
        fold_test_sharpes: dict[str, list[float]] = {
            candidate.candidate_id: [] for candidate in family_candidates
        }
        for fold in folds:
            ranked: list[tuple[float, str, CandidateSpec, PerformanceMetrics]] = []
            for candidate in family_candidates:
                metrics = _metrics_from_returns(
                    bars,
                    candidate_targets[candidate.candidate_id],
                    candidate_returns[candidate.candidate_id],
                    fold.train_start,
                    fold.train_end,
                    one_way_cost_rate,
                    capacity_notional,
                    maximum_gap_seconds,
                    periods_per_year,
                )
                score = metrics.sharpe - complexity_penalty * candidate.complexity
                ranked.append((score, candidate.candidate_id, candidate, metrics))
            ranked.sort(key=lambda item: (-item[0], item[1]))
            selected = ranked[0][2]
            for _score, _identifier, candidate, metrics in ranked:
                trials.append(_trial(
                    run_id,
                    candidate,
                    fold.fold_id,
                    "training",
                    candidate.candidate_id == selected.candidate_id,
                    metrics,
                    bars,
                    fold.train_start,
                    fold.train_end,
                ))
            selected_test_metric: PerformanceMetrics | None = None
            for candidate in family_candidates:
                test_metrics = _metrics_from_returns(
                    bars,
                    candidate_targets[candidate.candidate_id],
                    candidate_returns[candidate.candidate_id],
                    fold.test_start,
                    fold.test_end,
                    one_way_cost_rate,
                    capacity_notional,
                    maximum_gap_seconds,
                    periods_per_year,
                )
                trials.append(_trial(
                    run_id,
                    candidate,
                    fold.fold_id,
                    "testing",
                    candidate.candidate_id == selected.candidate_id,
                    test_metrics,
                    bars,
                    fold.test_start,
                    fold.test_end,
                ))
                if candidate.candidate_id == selected.candidate_id:
                    selected_test_metric = test_metrics
                fold_test_sharpes[candidate.candidate_id].append(
                    test_metrics.sharpe
                )
            if selected_test_metric is None:
                raise ValueError("训练冠军缺少测试段指标")
            selected_candidate_ids.append(selected.candidate_id)
            selected_test_metrics.append(selected_test_metric)
            targets = candidate_targets[selected.candidate_id]
            for index in range(fold.test_start - 1, fold.test_end):
                oos_targets[index] = targets[index]
            for index in range(fold.test_start, fold.test_end):
                oos_mask[index] = True
        oos_returns = list(strategy_returns(
            bars,
            oos_targets,
            one_way_cost_rate,
            maximum_gap_seconds,
        ))
        ranked_full: list[tuple[float, str, CandidateSpec, PerformanceMetrics]] = []
        for candidate in family_candidates:
            metrics = _metrics_from_returns(
                bars,
                candidate_targets[candidate.candidate_id],
                candidate_returns[candidate.candidate_id],
                1,
                len(bars),
                one_way_cost_rate,
                capacity_notional,
                maximum_gap_seconds,
                periods_per_year,
            )
            score = metrics.sharpe - complexity_penalty * candidate.complexity
            ranked_full.append((score, candidate.candidate_id, candidate, metrics))
        ranked_full.sort(key=lambda item: (-item[0], item[1]))
        selected_full = ranked_full[0][2]
        for _score, _identifier, candidate, metrics in ranked_full:
            trials.append(_trial(
                run_id,
                candidate,
                "full",
                "training",
                candidate.candidate_id == selected_full.candidate_id,
                metrics,
                bars,
                1,
                len(bars),
            ))
        metrics = _masked_metrics(
            bars,
            oos_targets,
            oos_returns,
            oos_mask,
            one_way_cost_rate,
            capacity_notional,
            maximum_gap_seconds,
            periods_per_year,
        )
        complexity_by_id = {
            candidate.candidate_id: candidate.complexity
            for candidate in family_candidates
        }
        selected_complexity = statistics.fmean(
            complexity_by_id[candidate_id] for candidate_id in selected_candidate_ids
        )
        adjusted_sharpe = metrics.sharpe - complexity_penalty * selected_complexity
        selection_counts = Counter(selected_candidate_ids)
        positive_fold_ratio = sum(
            item.net_return > 0 for item in selected_test_metrics
        ) / len(selected_test_metrics)
        most_selected_share = max(selection_counts.values()) / len(folds)
        median_selected_sharpe = statistics.median(
            item.sharpe for item in selected_test_metrics
        )
        family_seed = pbo_random_seed ^ int(
            hashlib.sha256(family.encode("utf-8")).hexdigest()[:16],
            16,
        )
        pbo, median_cscv_rank, cscv_split_count = (
            _probability_backtest_overfitting(
                fold_test_sharpes,
                pbo_split_budget,
                family_seed,
            )
        )
        candidate_fold_scores.update({
            candidate_id: tuple(scores)
            for candidate_id, scores in fold_test_sharpes.items()
        })
        bootstrap_seed = block_bootstrap_seed ^ int(
            hashlib.sha256(
                f"{family}:{BLOCK_BOOTSTRAP_METHOD_VERSION}".encode("utf-8")
            ).hexdigest()[:16],
            16,
        )
        compact_oos_returns = tuple(
            oos_returns[index]
            for index in range(1, len(bars))
            if oos_mask[index]
        )
        bootstrap_lower, bootstrap_p, bootstrap_count = (
            _circular_block_bootstrap_sharpe(
                compact_oos_returns,
                block_bootstrap_bars,
                block_bootstrap_samples,
                block_bootstrap_alpha,
                bootstrap_seed,
                periods_per_year,
            )
        )
        family_validation_targets[family] = tuple(oos_targets)
        pending.append(_PendingFamily(
            family=family,
            mode=selected_full.mode,
            deployment_candidate=selected_full,
            fold_selected_candidate_ids=tuple(selected_candidate_ids),
            oos_returns=compact_oos_returns,
            metrics=metrics,
            adjusted_sharpe=adjusted_sharpe,
            positive_fold_ratio=positive_fold_ratio,
            most_selected_share=most_selected_share,
            median_selected_sharpe=median_selected_sharpe,
            pbo=pbo,
            median_cscv_rank=median_cscv_rank,
            cscv_split_count=cscv_split_count,
            bootstrap_lower=bootstrap_lower,
            bootstrap_p=bootstrap_p,
            bootstrap_count=bootstrap_count,
        ))
    candidate_oos_metrics: dict[str, PerformanceMetrics] = {}
    for candidate in candidates:
        candidate_id = candidate.candidate_id
        metrics = _masked_metrics(
            bars,
            candidate_targets[candidate_id],
            candidate_returns[candidate_id],
            common_oos_mask,
            one_way_cost_rate,
            capacity_notional,
            maximum_gap_seconds,
            periods_per_year,
        )
        candidate_oos_metrics[candidate_id] = metrics
        indices = [
            index for index in range(1, len(bars)) if common_oos_mask[index]
        ]
        trials.append(_trial(
            run_id,
            candidate,
            "walk-forward",
            "testing_aggregate",
            False,
            metrics,
            bars,
            indices[0],
            indices[-1] + 1,
        ))
    p_values = {
        candidate_id: metrics.p_value
        for candidate_id, metrics in candidate_oos_metrics.items()
    }
    annualization_scale = math.sqrt(periods_per_year)
    for item in pending:
        p_values[f"family-walk-forward:{item.family}"] = item.metrics.p_value
    q_values = _fdr_q_values(p_values)
    minimum_bars = _integer(validation.get("minimum_oos_bars"), "minimum_oos_bars")
    minimum_sharpe = _number(
        validation.get("minimum_oos_sharpe"), "minimum_oos_sharpe",
    )
    maximum_drawdown = _number(
        validation.get("maximum_drawdown"), "maximum_drawdown",
    )
    maximum_q = _number(validation.get("maximum_fdr_q"), "maximum_fdr_q")
    minimum_positive_fold_ratio = _number(
        validation.get("minimum_positive_fold_ratio"),
        "minimum_positive_fold_ratio",
    )
    maximum_pbo = _number(
        validation.get("maximum_probability_backtest_overfitting"),
        "maximum_probability_backtest_overfitting",
    )
    maximum_bootstrap_p = _number(
        validation.get("maximum_block_bootstrap_p_value"),
        "maximum_block_bootstrap_p_value",
    )
    minimum_deflated_sharpe_probability = _number(
        validation.get("minimum_deflated_sharpe_probability"),
        "minimum_deflated_sharpe_probability",
    )
    if not 0.0 <= minimum_deflated_sharpe_probability <= 1.0:
        raise ValueError("minimum_deflated_sharpe_probability 必须位于零到一")
    deflated_sharpe_gate_trial_count = validation.get(
        "deflated_sharpe_gate_trial_count"
    )
    if deflated_sharpe_gate_trial_count != "effective":
        raise ValueError("deflated_sharpe_gate_trial_count 必须为 effective")
    minimum_parameter_neighbor_count = _integer(
        validation.get("minimum_parameter_neighbor_count"),
        "minimum_parameter_neighbor_count",
    )
    minimum_positive_parameter_neighbor_ratio = _number(
        validation.get("minimum_positive_parameter_neighbor_ratio"),
        "minimum_positive_parameter_neighbor_ratio",
    )
    minimum_neighbor_sharpe_retention = _number(
        validation.get("minimum_median_parameter_neighbor_sharpe_retention"),
        "minimum_median_parameter_neighbor_sharpe_retention",
    )
    if not 0.0 <= minimum_positive_parameter_neighbor_ratio <= 1.0:
        raise ValueError("minimum_positive_parameter_neighbor_ratio 必须位于零到一")
    evaluations: list[FamilyEvaluation] = []
    for item in pending:
        q_value = q_values[f"family-walk-forward:{item.family}"]
        family_candidate_ids = tuple(sorted(
            candidate.candidate_id
            for candidate in candidates
            if candidate.family == item.family
        ))
        raw_trial_count = len(family_candidate_ids)
        effective_trial_count = _effective_trial_count({
            candidate_id: candidate_fold_scores[candidate_id]
            for candidate_id in family_candidate_ids
        })
        candidate_period_sharpes = tuple(
            candidate_oos_metrics[candidate_id].sharpe / annualization_scale
            for candidate_id in family_candidate_ids
        )
        raw_dsr, raw_benchmark = _deflated_sharpe_probability(
            item.oos_returns,
            candidate_period_sharpes,
            float(raw_trial_count),
        )
        effective_dsr, effective_benchmark = _deflated_sharpe_probability(
            item.oos_returns,
            candidate_period_sharpes,
            effective_trial_count,
        )
        neighbors = _parameter_neighbors(
            item.deployment_candidate,
            tuple(
                candidate for candidate in candidates
                if candidate.family == item.family
            ),
        )
        neighbor_metrics = [
            candidate_oos_metrics[candidate.candidate_id]
            for candidate in neighbors
        ]
        positive_neighbor_ratio = (
            sum(
                metrics.sharpe > 0.0 and metrics.net_return > 0.0
                for metrics in neighbor_metrics
            ) / len(neighbor_metrics)
            if neighbor_metrics
            else 0.0
        )
        selected_fixed_sharpe = candidate_oos_metrics[
            item.deployment_candidate.candidate_id
        ].sharpe
        median_neighbor_retention = (
            statistics.median(
                metrics.sharpe / selected_fixed_sharpe
                for metrics in neighbor_metrics
            )
            if neighbor_metrics and selected_fixed_sharpe > 0.0
            else 0.0
        )
        reasons: list[str] = []
        if item.mode != "paper":
            reasons.append("shadow_only")
        if item.metrics.bars < minimum_bars:
            reasons.append("insufficient_oos_bars")
        if item.metrics.sharpe <= minimum_sharpe:
            reasons.append("non_positive_oos_sharpe")
        if item.metrics.net_return <= 0:
            reasons.append("non_positive_oos_net_return")
        if item.metrics.maximum_drawdown > maximum_drawdown:
            reasons.append("maximum_drawdown_exceeded")
        if q_value > maximum_q:
            reasons.append("fdr_threshold_failed")
        if item.positive_fold_ratio < minimum_positive_fold_ratio:
            reasons.append("positive_fold_ratio_failed")
        if item.pbo > maximum_pbo:
            reasons.append("probability_backtest_overfitting_failed")
        if item.bootstrap_p > maximum_bootstrap_p:
            reasons.append("block_bootstrap_sharpe_failed")
        if effective_dsr < minimum_deflated_sharpe_probability:
            reasons.append("deflated_sharpe_probability_failed")
        if len(neighbors) < minimum_parameter_neighbor_count:
            reasons.append("parameter_neighbor_count_failed")
        if (
            positive_neighbor_ratio
            < minimum_positive_parameter_neighbor_ratio
        ):
            reasons.append("positive_parameter_neighbor_ratio_failed")
        if median_neighbor_retention < minimum_neighbor_sharpe_retention:
            reasons.append("parameter_neighbor_sharpe_retention_failed")
        evaluations.append(FamilyEvaluation(
            family=item.family,
            mode=item.mode,
            deployment_candidate=item.deployment_candidate,
            latest_target=candidate_targets[item.deployment_candidate.candidate_id][
                resolved_decision_index
            ],
            deployment_oos_metrics=candidate_oos_metrics[
                item.deployment_candidate.candidate_id
            ],
            deployment_oos_returns=tuple(
                candidate_returns[item.deployment_candidate.candidate_id][index]
                for index in range(1, len(bars))
                if common_oos_mask[index]
            ),
            metrics=item.metrics,
            adjusted_sharpe=item.adjusted_sharpe,
            fdr_q=q_value,
            eligible=not reasons,
            rejection_reasons=tuple(reasons),
            oos_returns=item.oos_returns,
            positive_fold_ratio=item.positive_fold_ratio,
            most_selected_candidate_share=item.most_selected_share,
            median_selected_fold_sharpe=item.median_selected_sharpe,
            probability_backtest_overfitting=item.pbo,
            median_cscv_oos_rank=item.median_cscv_rank,
            cscv_split_count=item.cscv_split_count,
            block_bootstrap_sharpe_lower_bound=item.bootstrap_lower,
            block_bootstrap_p_value=item.bootstrap_p,
            block_bootstrap_sample_count=item.bootstrap_count,
            deflated_sharpe_probability_raw=raw_dsr,
            deflated_sharpe_probability_effective=effective_dsr,
            deflated_sharpe_benchmark_raw=raw_benchmark * annualization_scale,
            deflated_sharpe_benchmark_effective=(
                effective_benchmark * annualization_scale
            ),
            raw_trial_count=raw_trial_count,
            effective_trial_count=effective_trial_count,
            parameter_neighbor_count=len(neighbors),
            positive_parameter_neighbor_ratio=positive_neighbor_ratio,
            median_parameter_neighbor_sharpe_retention=(
                median_neighbor_retention
            ),
            fold_selected_candidate_ids=item.fold_selected_candidate_ids,
            cscv_in_sample_fold_count=len(folds) // 2,
            cscv_out_sample_fold_count=len(folds) - len(folds) // 2,
            periods_per_year=periods_per_year,
        ))
    return ValidationResult(
        families=tuple(sorted(evaluations, key=lambda item: item.family)),
        trials=tuple(trials),
        candidate_targets=candidate_targets,
        folds=folds,
        family_validation_targets=family_validation_targets,
    )


def metrics_payload(metrics: PerformanceMetrics) -> Mapping[str, object]:
    """把绩效指标转换为 JSON 载荷。"""
    return {
        "bars": metrics.bars,
        "net_return": metrics.net_return,
        "annual_return": metrics.annual_return,
        "annual_volatility": metrics.annual_volatility,
        "sharpe": metrics.sharpe,
        "maximum_drawdown": metrics.maximum_drawdown,
        "turnover": metrics.turnover,
        "annual_turnover": metrics.annual_turnover,
        "hit_rate": metrics.hit_rate,
        "exposure": metrics.exposure,
        "cost": metrics.cost,
        "p_value": metrics.p_value,
        "capacity_score": metrics.capacity_score,
    }
