"""研究、验证和分配层的数据合同。"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Mapping

from guvolu.strategy.contracts import CandidateSpec, ResearchBar

HOLDOUT_MANIFEST_SCHEMA_VERSION = 1
HOLDOUT_METHOD_VERSION = "frozen-candidate-holdout-v4"
FROZEN_FORWARD_SCHEMA_VERSION = 1
FROZEN_FORWARD_METHOD_VERSION = "frozen-forward-v2"


@dataclass(frozen=True)
class CodeIdentity:
    """研究代码的可复现身份。"""

    git_hash: str | None
    tree_digest: str
    dirty_digest: str
    dirty: bool
    decision_grade: bool
    reason: str | None


@dataclass(frozen=True)
class FrozenPanelPartition:
    """一个冻结输入文件及其控制面事件覆盖。"""

    path: Path
    row_count: int
    min_event_time: datetime | None
    max_event_time: datetime | None


@dataclass(frozen=True)
class FrozenPanelInputs:
    """活动 head 冻结后的研究输入。"""

    market: Mapping[str, object]
    paths: tuple[Path, ...]
    head_generation: str
    attempt_ids: tuple[str, ...]
    artifact_ids: tuple[str, ...]
    normalization_versions: tuple[str, ...]
    maximum_event_time: datetime
    partitions: tuple[FrozenPanelPartition, ...] = ()
    receipt_path: Path | None = None
    receipt_sha256: str | None = None


@dataclass(frozen=True)
class PanelSnapshot:
    """冻结输入与紧凑研究面板。"""

    market: Mapping[str, object]
    bars: tuple[ResearchBar, ...]
    head_generation: str
    attempt_ids: tuple[str, ...]
    artifact_ids: tuple[str, ...]
    normalization_versions: tuple[str, ...]
    panel_path: Path
    panel_sha256: str
    decision_time: datetime
    latest_available_time: datetime


@dataclass(frozen=True)
class QualityVector:
    """策略声明使用的六维质量门禁。"""

    integrity: bool
    freshness: bool
    clock: bool
    coverage: bool
    pit: bool
    lineage: bool
    reasons: tuple[str, ...]

    @property
    def eligible(self) -> bool:
        """返回全部质量维度是否通过。"""
        return all((
            self.integrity,
            self.freshness,
            self.clock,
            self.coverage,
            self.pit,
            self.lineage,
        ))


@dataclass(frozen=True)
class PerformanceMetrics:
    """一次候选评估的净成本指标。"""

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
    p_value: float
    capacity_score: float


@dataclass(frozen=True)
class TrialRecord:
    """追加式试验台账中的一个事实。"""

    evaluation_id: str
    candidate: CandidateSpec
    fold_id: str
    segment: str
    start_time: datetime
    end_time: datetime
    selected: bool
    metrics: PerformanceMetrics


@dataclass(frozen=True)
class FamilyEvaluation:
    """一个策略家族的 walk-forward 结果。"""

    family: str
    mode: str
    deployment_candidate: CandidateSpec
    latest_target: float
    deployment_oos_metrics: PerformanceMetrics
    deployment_oos_returns: tuple[float, ...]
    metrics: PerformanceMetrics
    adjusted_sharpe: float
    fdr_q: float
    eligible: bool
    rejection_reasons: tuple[str, ...]
    oos_returns: tuple[float, ...]
    positive_fold_ratio: float = 0.0
    most_selected_candidate_share: float = 0.0
    median_selected_fold_sharpe: float = 0.0
    probability_backtest_overfitting: float = 1.0
    median_cscv_oos_rank: float = 0.0
    cscv_split_count: int = 0
    block_bootstrap_sharpe_lower_bound: float = 0.0
    block_bootstrap_p_value: float = 1.0
    block_bootstrap_sample_count: int = 0
    deflated_sharpe_probability_raw: float = 0.0
    deflated_sharpe_probability_effective: float = 0.0
    deflated_sharpe_benchmark_raw: float = 0.0
    deflated_sharpe_benchmark_effective: float = 0.0
    raw_trial_count: int = 0
    effective_trial_count: float = 0.0
    parameter_neighbor_count: int = 0
    positive_parameter_neighbor_ratio: float = 0.0
    median_parameter_neighbor_sharpe_retention: float = 0.0
    fold_selected_candidate_ids: tuple[str, ...] = ()
    cscv_in_sample_fold_count: int = 0
    cscv_out_sample_fold_count: int = 0
    cscv_excluded_fold_count: int = 0
    periods_per_year: float = 365.0 * 24.0


@dataclass(frozen=True)
class AllocationResult:
    """受约束分配器的目标风险权重。"""

    weights: Mapping[str, float]
    reserve: float
    objective: float
    regime: str
    iterations: int
