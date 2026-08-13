"""策略纯函数使用的数值研究合同。"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Mapping


@dataclass(frozen=True)
class ResearchBar:
    """从规范化成交构造的 PIT 研究柱。"""

    open_time: datetime
    decision_time: datetime
    latest_available_time: datetime
    open: float
    high: float
    low: float
    close: float
    base_volume: float
    quote_volume: float
    signed_base_volume: float
    trade_count: int


@dataclass(frozen=True)
class FeatureRow:
    """一个决策时点的共享特征。"""

    decision_time: datetime
    as_of: datetime
    return_one: float | None
    trend_scores: Mapping[int, float | None]
    volatility: Mapping[int, float | None]
    price_scores: Mapping[int, float | None]
    prior_highs: Mapping[int, float | None]
    prior_lows: Mapping[int, float | None]
    flow_imbalance: float | None
    volume_score: float | None
    jump_score: float | None
    contiguous: bool


@dataclass(frozen=True)
class CandidateSpec:
    """一个版本化策略参数候选。"""

    candidate_id: str
    family: str
    mode: str
    parameters: Mapping[str, int | float]
    complexity: int
