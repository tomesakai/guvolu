"""SearchFast 测试共享构件：合成面板、f32 取整副本与设备列表。"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace

from guvolu.search.synthetic import synthetic_panel, synthetic_strategy_config
from guvolu.search.tensorize import PanelTensor, round_to_f32, tensorize_panel
from guvolu.search.torch_runtime import cuda_available, torch_module_or_none
from guvolu.strategy.contracts import FeatureRow, ResearchBar
from guvolu.strategy.generation import (
    build_family_batches,
    candidate_search_plan_payload,
)

DEFAULT_LOOKBACKS = (4, 8)


def _round_mapping(values: Mapping[int, float | None]) -> dict[int, float | None]:
    """把回看窗字典的数值按 f32 取整。"""
    return {
        key: None if value is None else round_to_f32(value)
        for key, value in values.items()
    }


def _round_optional(value: float | None) -> float | None:
    """把可空数值按 f32 取整。"""
    return None if value is None else round_to_f32(value)


def round_panel_to_f32(
    bars: Sequence[ResearchBar],
    features: Sequence[FeatureRow],
) -> tuple[tuple[ResearchBar, ...], tuple[FeatureRow, ...]]:
    """生成与张量化输入逐位一致的 f32 取整副本。"""
    rounded_bars = tuple(
        replace(
            bar,
            open=round_to_f32(bar.open),
            high=round_to_f32(bar.high),
            low=round_to_f32(bar.low),
            close=round_to_f32(bar.close),
        )
        for bar in bars
    )
    rounded_features = tuple(
        replace(
            feature,
            trend_scores=_round_mapping(feature.trend_scores),
            volatility=_round_mapping(feature.volatility),
            price_scores=_round_mapping(feature.price_scores),
            prior_highs=_round_mapping(feature.prior_highs),
            prior_lows=_round_mapping(feature.prior_lows),
            flow_imbalance=_round_optional(feature.flow_imbalance),
            volume_score=_round_optional(feature.volume_score),
            jump_score=_round_optional(feature.jump_score),
        )
        for feature in features
    )
    return rounded_bars, rounded_features


@dataclass(frozen=True)
class SearchFixture:
    """一组合成面板、取整副本、计划与张量。"""

    bars: tuple[ResearchBar, ...]
    features: tuple[FeatureRow, ...]
    rounded_bars: tuple[ResearchBar, ...]
    rounded_features: tuple[FeatureRow, ...]
    plan: Mapping[str, object]
    panel: PanelTensor
    lookbacks: tuple[int, ...]


def build_fixture(
    bars: int = 128,
    seed: int = 11,
    lookbacks: Sequence[int] = DEFAULT_LOOKBACKS,
    families: Sequence[str] | None = None,
) -> SearchFixture:
    """构造覆盖六流派的合成搜索输入。"""
    windows = tuple(sorted(set(int(value) for value in lookbacks)))
    bar_rows, feature_rows = synthetic_panel(bars, windows, seed)
    rounded_bars, rounded_features = round_panel_to_f32(bar_rows, feature_rows)
    plan = candidate_search_plan_payload(
        build_family_batches(synthetic_strategy_config(windows), families),
    )
    panel = tensorize_panel(bar_rows, feature_rows, windows)
    return SearchFixture(
        bars=bar_rows,
        features=feature_rows,
        rounded_bars=rounded_bars,
        rounded_features=rounded_features,
        plan=plan,
        panel=panel,
        lookbacks=windows,
    )


def torch_devices() -> tuple[str, ...]:
    """返回可测设备；无 Torch 为空，无 CUDA 只含 cpu。"""
    if torch_module_or_none() is None:
        return ()
    return ("cpu", "cuda") if cuda_available() else ("cpu",)
