"""研究回放与当前运行的质量门禁。"""
from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime

from guvolu.research.contracts import FamilyEvaluation, PanelSnapshot, QualityVector
from guvolu.strategy.contracts import FeatureRow

OPERATIONAL_GATE_METHOD_VERSION = "economic-trade-operational-gate-v1"


def panel_quality(
    panel: PanelSnapshot,
    reference_time: datetime,
    maximum_age_seconds: int,
    minimum_bars: int,
) -> QualityVector:
    """按策略声明检查成交研究面板。"""
    if maximum_age_seconds <= 0:
        raise ValueError("策略新鲜度阈值必须为正数")
    if minimum_bars <= 0:
        raise ValueError("最小面板行数必须为正数")
    reasons: list[str] = []
    integrity = all(
        bar.open > 0
        and bar.high >= max(bar.open, bar.close)
        and bar.low <= min(bar.open, bar.close)
        and bar.low > 0
        and bar.base_volume >= 0
        and bar.quote_volume >= 0
        for bar in panel.bars
    )
    if not integrity:
        reasons.append("panel_integrity_failed")
    available_bars = tuple(
        bar for bar in panel.bars if bar.decision_time <= reference_time
    )
    latest_available = (
        available_bars[-1].latest_available_time
        if available_bars else panel.latest_available_time
    )
    age_seconds = (reference_time - latest_available).total_seconds()
    freshness = 0 <= age_seconds <= maximum_age_seconds
    if age_seconds < 0:
        reasons.append("available_time_after_reference")
    elif age_seconds > maximum_age_seconds:
        reasons.append("strategy_data_stale")
    clock = all(
        bar.open_time.tzinfo is not None
        and bar.decision_time.tzinfo is not None
        and bar.latest_available_time.tzinfo is not None
        and bar.open_time < bar.decision_time
        for bar in panel.bars
    )
    if not clock:
        reasons.append("clock_semantics_failed")
    coverage = len(panel.bars) >= minimum_bars
    if not coverage:
        reasons.append("insufficient_panel_coverage")
    pit = all(
        bar.latest_available_time <= bar.decision_time for bar in panel.bars
    )
    if not pit:
        reasons.append("available_time_after_decision")
    lineage = (
        panel.head_generation.startswith("sha256-")
        and bool(panel.attempt_ids)
        and bool(panel.artifact_ids)
        and bool(panel.normalization_versions)
        and len(panel.panel_sha256) == 64
    )
    if not lineage:
        reasons.append("lineage_incomplete")
    return QualityVector(
        integrity=integrity,
        freshness=freshness,
        clock=clock,
        coverage=coverage,
        pit=pit,
        lineage=lineage,
        reasons=tuple(reasons),
    )


def quality_payload(value: QualityVector) -> dict[str, object]:
    """把质量向量转换为 JSON 载荷。"""
    return {
        "integrity": value.integrity,
        "freshness": value.freshness,
        "clock": value.clock,
        "coverage": value.coverage,
        "pit": value.pit,
        "lineage": value.lineage,
        "eligible": value.eligible,
        "reasons": list(value.reasons),
    }


def gate_feature_snapshot(
    quality: QualityVector,
    feature_decision_time: datetime,
    reference_time: datetime,
    maximum_age_seconds: int,
) -> QualityVector:
    """把策略特征年龄并入新鲜度门禁。"""
    age = (reference_time - feature_decision_time).total_seconds()
    freshness = quality.freshness and 0 <= age <= maximum_age_seconds
    reasons = list(quality.reasons)
    if age < 0:
        reasons.append("feature_time_after_reference")
    elif age > maximum_age_seconds:
        reasons.append("feature_snapshot_stale")
    return QualityVector(
        integrity=quality.integrity,
        freshness=freshness,
        clock=quality.clock,
        coverage=quality.coverage,
        pit=quality.pit,
        lineage=quality.lineage,
        reasons=tuple(sorted(set(reasons))),
    )


def gate_economic_trade_volume(
    quality: QualityVector,
    families: Sequence[FamilyEvaluation],
    feature: FeatureRow,
) -> QualityVector:
    """实际部署集合依赖 flow 时要求当前经济成交窗口可用。"""
    flow_sensitive = any(
        item.eligible
        and item.mode == "paper"
        and item.family in {"flow_trend", "breakout"}
        for item in families
    )
    latest_volume_qualified = (
        feature.volume_qualified
        and feature.volume_score is not None
        and feature.flow_imbalance is not None
    )
    if not flow_sensitive or latest_volume_qualified:
        return quality
    return QualityVector(
        integrity=quality.integrity,
        freshness=quality.freshness,
        clock=quality.clock,
        coverage=False,
        pit=quality.pit,
        lineage=quality.lineage,
        reasons=tuple(sorted({
            *quality.reasons,
            "latest_economic_trade_volume_unqualified",
        })),
    )
