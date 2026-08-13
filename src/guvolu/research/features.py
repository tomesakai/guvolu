"""PIT 研究特征与市场状态。"""
from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from guvolu.strategy.contracts import FeatureRow, ResearchBar

FEATURE_METHOD_VERSION = "research-features-v1"
HOURS_PER_YEAR = 365.0 * 24.0


@dataclass(frozen=True)
class MarketState:
    """可解释的市场状态向量。"""

    trend: float | None
    volatility: float | None
    liquidity: float | None
    flow: float | None
    carry: float | None
    cross_venue: float | None
    relative: float | None
    jump: float | None
    regime: str
    uncertainty: float


def _rolling_mean_std(
    values: Sequence[float],
    prefix: Sequence[float],
    square_prefix: Sequence[float],
    start: int,
    end: int,
) -> tuple[float, float]:
    """由前缀和计算总体均值与标准差。"""
    count = end - start
    if count <= 0:
        return 0.0, 0.0
    total = prefix[end] - prefix[start]
    total_square = square_prefix[end] - square_prefix[start]
    mean = total / count
    variance = max(total_square / count - mean * mean, 0.0)
    return mean, math.sqrt(variance)


def _prefix(values: Sequence[float]) -> tuple[list[float], list[float]]:
    """构造数值与平方前缀和。"""
    totals = [0.0]
    squares = [0.0]
    for value in values:
        totals.append(totals[-1] + value)
        squares.append(squares[-1] + value * value)
    return totals, squares


def _contiguous_prefix(
    bars: Sequence[ResearchBar],
    maximum_structural_gap_bars: int,
) -> list[int]:
    """构造非连续时间边界前缀和。"""
    failures = [0]
    if len(bars) < 2:
        return failures
    expected = bars[1].open_time - bars[0].open_time
    for index in range(1, len(bars)):
        gap = bars[index].open_time - bars[index - 1].open_time
        failures.append(failures[-1] + int(
            gap < expected or gap > expected * maximum_structural_gap_bars
        ))
    return failures


def _window_contiguous(failures: Sequence[int], index: int, window: int) -> bool:
    """判断指定回看窗是否连续。"""
    if index < window or index >= len(failures):
        return False
    return failures[index] - failures[index - window] == 0


def compute_features(
    bars: Sequence[ResearchBar],
    lookbacks: Sequence[int],
    volume_lookback: int,
    maximum_structural_gap_bars: int = 1,
) -> tuple[FeatureRow, ...]:
    """只使用当前及历史柱计算共享特征。"""
    if not bars:
        return ()
    windows = tuple(sorted(set(lookbacks)))
    if not windows or windows[0] <= 1:
        raise ValueError("特征回看窗必须大于一")
    if volume_lookback <= 1:
        raise ValueError("成交额回看窗必须大于一")
    if maximum_structural_gap_bars <= 0:
        raise ValueError("结构性空窗上限必须为正数")
    closes = [bar.close for bar in bars]
    log_prices = [math.log(value) for value in closes]
    returns = [0.0]
    returns.extend(
        log_prices[index] - log_prices[index - 1]
        for index in range(1, len(log_prices))
    )
    log_volumes = [math.log1p(max(bar.quote_volume, 0.0)) for bar in bars]
    return_prefix, return_squares = _prefix(returns)
    price_prefix, price_squares = _prefix(log_prices)
    volume_prefix, volume_squares = _prefix(log_volumes)
    failures = _contiguous_prefix(bars, maximum_structural_gap_bars)
    result: list[FeatureRow] = []
    for index, bar in enumerate(bars):
        trends: dict[int, float | None] = {}
        volatility: dict[int, float | None] = {}
        price_scores: dict[int, float | None] = {}
        prior_highs: dict[int, float | None] = {}
        prior_lows: dict[int, float | None] = {}
        for window in windows:
            valid = _window_contiguous(failures, index, window)
            if not valid:
                trends[window] = None
                volatility[window] = None
                price_scores[window] = None
                prior_highs[window] = None
                prior_lows[window] = None
                continue
            return_mean, return_std = _rolling_mean_std(
                returns,
                return_prefix,
                return_squares,
                index - window + 1,
                index + 1,
            )
            del return_mean
            raw_trend = log_prices[index] - log_prices[index - window]
            scaled = return_std * math.sqrt(window)
            trends[window] = raw_trend / scaled if scaled > 0 else 0.0
            volatility[window] = return_std
            price_mean, price_std = _rolling_mean_std(
                log_prices,
                price_prefix,
                price_squares,
                index - window + 1,
                index + 1,
            )
            price_scores[window] = (
                (log_prices[index] - price_mean) / price_std
                if price_std > 0 else 0.0
            )
            prior = bars[index - window:index]
            prior_highs[window] = max(item.high for item in prior)
            prior_lows[window] = min(item.low for item in prior)
        flow = (
            bar.signed_base_volume / bar.base_volume
            if bar.base_volume > 0 else None
        )
        volume_score: float | None = None
        if _window_contiguous(failures, index, volume_lookback):
            volume_mean, volume_std = _rolling_mean_std(
                log_volumes,
                volume_prefix,
                volume_squares,
                index - volume_lookback + 1,
                index + 1,
            )
            volume_score = (
                (log_volumes[index] - volume_mean) / volume_std
                if volume_std > 0 else 0.0
            )
        shortest = windows[0]
        short_volatility = volatility.get(shortest)
        jump = (
            abs(returns[index]) / short_volatility
            if index > 0 and short_volatility is not None and short_volatility > 0
            else None
        )
        contiguous = all(value is not None for value in trends.values())
        result.append(FeatureRow(
            decision_time=bar.decision_time,
            as_of=bar.latest_available_time,
            return_one=returns[index] if index > 0 else None,
            trend_scores=trends,
            volatility=volatility,
            price_scores=price_scores,
            prior_highs=prior_highs,
            prior_lows=prior_lows,
            flow_imbalance=flow,
            volume_score=volume_score,
            jump_score=jump,
            contiguous=contiguous,
        ))
    return tuple(result)


def classify_market_state(
    feature: FeatureRow,
    state_lookback: int,
    quote_volume_score: float | None,
) -> MarketState:
    """以规则门构造第一版市场状态。"""
    trend = feature.trend_scores.get(state_lookback)
    hourly_volatility = feature.volatility.get(state_lookback)
    annual_volatility = (
        hourly_volatility * math.sqrt(HOURS_PER_YEAR)
        if hourly_volatility is not None else None
    )
    flow = feature.flow_imbalance
    jump = feature.jump_score
    if jump is not None and jump >= 4.0:
        regime = "jump_risk"
    elif trend is not None and trend >= 0.75 and (flow is None or flow >= -0.25):
        regime = "positive_trend"
    elif trend is not None and trend <= -0.75:
        regime = "negative_trend"
    elif trend is not None and abs(trend) <= 0.5:
        regime = "range"
    else:
        regime = "mixed"
    missing = sum(value is None for value in (
        trend,
        annual_volatility,
        quote_volume_score,
        flow,
        jump,
    ))
    return MarketState(
        trend=trend,
        volatility=annual_volatility,
        liquidity=quote_volume_score,
        flow=flow,
        carry=None,
        cross_venue=None,
        relative=None,
        jump=jump,
        regime=regime,
        uncertainty=missing / 8.0,
    )


def feature_payload(feature: FeatureRow) -> Mapping[str, object]:
    """把特征转换为可持久化载荷。"""
    return {
        "decision_time": feature.decision_time.isoformat(),
        "as_of": feature.as_of.isoformat(),
        "return_one": feature.return_one,
        "trend_scores": {str(key): value for key, value in feature.trend_scores.items()},
        "volatility": {str(key): value for key, value in feature.volatility.items()},
        "price_scores": {str(key): value for key, value in feature.price_scores.items()},
        "prior_highs": {str(key): value for key, value in feature.prior_highs.items()},
        "prior_lows": {str(key): value for key, value in feature.prior_lows.items()},
        "flow_imbalance": feature.flow_imbalance,
        "volume_score": feature.volume_score,
        "jump_score": feature.jump_score,
        "contiguous": feature.contiguous,
        "method_version": FEATURE_METHOD_VERSION,
    }
