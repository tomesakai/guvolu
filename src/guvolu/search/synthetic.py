"""确定性合成面板：无数据时的自检输入，固定种子可复现（G-03）。

价格路径沿用 benchmark 的正弦余弦公式，缺失与门禁由种子随机注入。
"""
from __future__ import annotations

import math
import random
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta

from guvolu.strategy.contracts import FeatureRow, ResearchBar

SYNTHETIC_METHOD_VERSION = "searchfast-synthetic-panel-v1"


def _nullable(
    generator: random.Random,
    value: float,
    missing_probability: float,
) -> float | None:
    """按概率把数值替换为缺失。"""
    if generator.random() < missing_probability:
        return None
    return value


def synthetic_panel(
    bars: int,
    lookbacks: Sequence[int],
    seed: int,
    interval_seconds: int = 3600,
    missing_probability: float = 0.02,
    gap_probability: float = 0.01,
) -> tuple[tuple[ResearchBar, ...], tuple[FeatureRow, ...]]:
    """生成固定种子的合成行情柱与特征。"""
    if bars < 2:
        raise ValueError("bars 至少为二")
    windows = tuple(sorted(set(int(value) for value in lookbacks)))
    if not windows:
        raise ValueError("回看窗不得为空")
    generator = random.Random(seed)
    start = datetime(2024, 1, 1, tzinfo=UTC)
    bar_rows: list[ResearchBar] = []
    feature_rows: list[FeatureRow] = []
    price = 100.0
    open_time = start
    for index in range(bars):
        drift = 0.0005 * math.sin((index + 1) * 0.021)
        drift += 0.0002 * math.cos((index + 1) * 0.005)
        noise = generator.gauss(0.0, 0.004)
        previous_price = price
        price = price * math.exp(drift + noise)
        high = max(price, previous_price) * (1.0 + abs(generator.gauss(0.0, 0.001)))
        low = min(price, previous_price) * (1.0 - abs(generator.gauss(0.0, 0.001)))
        base_volume = 1.0 + abs(generator.gauss(0.0, 0.5))
        signed = base_volume * generator.uniform(-0.9, 0.9)
        if index > 0:
            gap_bars = 1
            if generator.random() < gap_probability:
                gap_bars = 2 + generator.randrange(3)
            open_time = open_time + timedelta(seconds=interval_seconds * gap_bars)
        decision_time = open_time + timedelta(seconds=interval_seconds)
        late = generator.random() < 0.01
        as_of = decision_time + timedelta(seconds=60 if late else 0)
        bar_rows.append(ResearchBar(
            open_time=open_time,
            decision_time=decision_time,
            latest_available_time=as_of,
            open=previous_price,
            high=high,
            low=low,
            close=price,
            base_volume=base_volume,
            quote_volume=base_volume * price,
            signed_base_volume=signed,
            trade_count=10 + generator.randrange(50),
        ))
        score_base = math.sin(index * 0.013) + 0.25 * math.cos(index * 0.003)
        trends: dict[int, float | None] = {}
        volatility: dict[int, float | None] = {}
        price_scores: dict[int, float | None] = {}
        prior_highs: dict[int, float | None] = {}
        prior_lows: dict[int, float | None] = {}
        for window in windows:
            scale = 1.0 + window / 100.0
            trends[window] = _nullable(
                generator,
                score_base * scale + generator.gauss(0.0, 0.3),
                missing_probability,
            )
            zero_volatility = generator.random() < 0.01
            volatility[window] = _nullable(
                generator,
                0.0 if zero_volatility else abs(generator.gauss(0.004, 0.002)) + 1e-5,
                missing_probability,
            )
            price_scores[window] = _nullable(
                generator,
                generator.gauss(0.0, 1.2),
                missing_probability,
            )
            prior_highs[window] = _nullable(
                generator,
                price * (1.0 + generator.gauss(0.0, 0.01)),
                missing_probability,
            )
            prior_lows[window] = _nullable(
                generator,
                price * (1.0 - abs(generator.gauss(0.0, 0.01))),
                missing_probability,
            )
        contiguous = generator.random() >= 0.02
        feature_rows.append(FeatureRow(
            decision_time=decision_time,
            as_of=as_of,
            return_one=None if index == 0 else math.log(price / previous_price),
            trend_scores=trends,
            volatility=volatility,
            price_scores=price_scores,
            prior_highs=prior_highs,
            prior_lows=prior_lows,
            flow_imbalance=_nullable(
                generator, signed / base_volume, missing_probability,
            ),
            volume_score=_nullable(
                generator, generator.gauss(0.0, 1.0), missing_probability,
            ),
            jump_score=_nullable(
                generator, abs(generator.gauss(1.0, 2.0)), missing_probability * 5,
            ),
            contiguous=contiguous,
        ))
    return tuple(bar_rows), tuple(feature_rows)


def synthetic_strategy_config(lookbacks: Sequence[int]) -> Mapping[str, object]:
    """返回覆盖六个流派的自检策略配置。"""
    windows = [int(value) for value in sorted(set(lookbacks))]
    return {
        "features": {"lookbacks": windows},
        "strategies": {
            "trend": {
                "lookbacks": windows,
                "entry_scores": [0.5, 1.0, 1.5],
                "exit_score": 0.0,
                "annual_volatility_target": 0.4,
                "maximum_target": 1.0,
            },
            "flow_trend": {
                "lookbacks": windows,
                "entry_scores": [0.5, 1.0],
                "flow_confirmations": [-0.25, 0.0, 0.25],
                "minimum_volume_score": 0.0,
                "exit_score": 0.0,
                "annual_volatility_target": 0.4,
                "maximum_target": 1.0,
            },
            "breakout": {
                "lookbacks": windows,
                "flow_confirmations": [-0.1, 0.1],
                "annual_volatility_target": 0.4,
                "maximum_target": 1.0,
            },
            "price_breakout": {
                "lookbacks": windows,
                "annual_volatility_target": 0.4,
                "maximum_target": 1.0,
            },
            "mean_reversion": {
                "lookbacks": windows,
                "entry_scores": [1.0, 1.5],
                "exit_score": -0.1,
                "trend_limit": 0.75,
                "annual_volatility_target": 0.25,
                "maximum_target": 0.75,
            },
            "grid_shadow": {
                "lookbacks": windows,
                "entry_scores": [1.0, 1.5],
                "maximum_target": 0.5,
            },
        },
    }
