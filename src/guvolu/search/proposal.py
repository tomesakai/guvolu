"""受约束配置提案：由 GPU 粗筛与 CPU 复算台账生成研究网格的下一版建议。

提案只写 JSON，不改写任何研究配置；采纳须经 `promote_search_results.py`
生成新配置文件，再由 `run_strategy_research.py` 走完整准入。
"""
from __future__ import annotations

import math
import statistics
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol

from guvolu.search.ledger import STAGE_F1_SCREENED
from guvolu.strategy.contracts import CandidateSpec

PROPOSAL_METHOD_VERSION = "search-loop-proposal-v1"
PROPOSAL_SCHEMA_VERSION = 1
STATUS_PROPOSED = "proposed"
STATUS_UNCHANGED = "unchanged_grid"
STATUS_NO_PROPOSAL = "no_proposal"
ARRAY_AXES = {
    "lookback": "lookbacks",
    "entry_score": "entry_scores",
    "flow_confirmation": "flow_confirmations",
}


class CandidateSet(Protocol):
    """提案所需的候选集合视图。"""

    @property
    def candidates(self) -> Mapping[str, CandidateSpec]:
        """候选身份到规格。"""

    @property
    def labels(self) -> Mapping[str, str]:
        """候选身份到计划流派标签。"""

    @property
    def sources(self) -> Mapping[str, str]:
        """候选身份到来源。"""

    @property
    def budgets(self) -> Mapping[str, Mapping[str, int]]:
        """注册流派的预算登记。"""


def _number(value: object, name: str) -> float:
    """验证有限数值。"""
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(float(value))
    ):
        raise ValueError(f"{name} 必须为有限数值")
    return float(value)


@dataclass(frozen=True)
class ProposalThresholds:
    """提案阶段阈值（G-06）：平坦度、轴取值上限与排序指标。"""

    minimum_neighbor_count: int = 1
    minimum_positive_neighbor_ratio: float = 0.5
    minimum_neighbor_sharpe_retention: float = 0.5
    maximum_axis_values: int = 3
    score_metric: str = "oos_sharpe"

    @classmethod
    def from_config(cls, config: Mapping[str, object]) -> "ProposalThresholds":
        """由配置读取阈值。"""
        default = cls()
        count = config.get("minimum_neighbor_count", default.minimum_neighbor_count)
        if not isinstance(count, int) or isinstance(count, bool) or count < 0:
            raise ValueError("proposal.minimum_neighbor_count 必须为非负整数")
        axis = config.get("maximum_axis_values", default.maximum_axis_values)
        if not isinstance(axis, int) or isinstance(axis, bool) or axis <= 0:
            raise ValueError("proposal.maximum_axis_values 必须为正整数")
        metric = str(config.get("score_metric", default.score_metric))
        if metric not in ("oos_sharpe", "bootstrap_sharpe_lower_bound"):
            raise ValueError("proposal.score_metric 不受支持")
        return cls(
            minimum_neighbor_count=count,
            minimum_positive_neighbor_ratio=_number(
                config.get(
                    "minimum_positive_neighbor_ratio",
                    default.minimum_positive_neighbor_ratio,
                ),
                "proposal.minimum_positive_neighbor_ratio",
            ),
            minimum_neighbor_sharpe_retention=_number(
                config.get(
                    "minimum_neighbor_sharpe_retention",
                    default.minimum_neighbor_sharpe_retention,
                ),
                "proposal.minimum_neighbor_sharpe_retention",
            ),
            maximum_axis_values=axis,
            score_metric=metric,
        )

    def payload(self) -> Mapping[str, object]:
        """导出阈值。"""
        return {
            "proposal_method_version": PROPOSAL_METHOD_VERSION,
            "minimum_neighbor_count": self.minimum_neighbor_count,
            "minimum_positive_neighbor_ratio": self.minimum_positive_neighbor_ratio,
            "minimum_neighbor_sharpe_retention": self.minimum_neighbor_sharpe_retention,
            "maximum_axis_values": self.maximum_axis_values,
            "score_metric": self.score_metric,
        }


@dataclass(frozen=True)
class CandidateEvidence:
    """一个候选在循环中的全部证据。"""

    candidate: CandidateSpec
    label: str
    source: str
    evaluation_id: str
    screen_passed: bool
    metrics: Mapping[str, float | int]
    resample: Mapping[str, object] | None
    parity: Mapping[str, object] | None
    exact: bool

    def oos_sharpe(self) -> float:
        """粗筛 OOS Sharpe，缺失记为负无穷。"""
        if self.resample is None:
            return -math.inf
        return float(str(self.resample.get("oos_sharpe")))

    def oos_net_return(self) -> float:
        """粗筛 OOS 净收益，缺失记为负无穷。"""
        if self.resample is None:
            return -math.inf
        return float(str(self.resample.get("oos_net_return")))

    def score(self, metric: str) -> float:
        """按配置指标取排序得分。"""
        if self.resample is None:
            return -math.inf
        return float(str(self.resample.get(metric)))

    def payload(self) -> Mapping[str, object]:
        """导出证据行。"""
        return {
            "candidate_id": self.candidate.candidate_id,
            "evaluation_id": self.evaluation_id,
            "label": self.label,
            "source": self.source,
            "parameters": dict(self.candidate.parameters),
            "screen_passed": self.screen_passed,
            "exact": self.exact,
            "metrics": dict(self.metrics),
            "resample": None if self.resample is None else dict(self.resample),
            "parity": None if self.parity is None else dict(self.parity),
        }


def collect_evidence(
    candidates: Mapping[str, CandidateSpec],
    labels: Mapping[str, str],
    sources: Mapping[str, str],
    trial_rows: Sequence[Mapping[str, object]],
    parity_rows: Sequence[Mapping[str, object]],
) -> Mapping[str, CandidateEvidence]:
    """把试验台账与对照台账合并为逐候选证据。"""
    parity_by_id: dict[str, Mapping[str, object]] = {
        str(row.get("candidate_id")): row for row in parity_rows
    }
    result: dict[str, CandidateEvidence] = {}
    for row in trial_rows:
        if row.get("stage") != STAGE_F1_SCREENED:
            continue
        candidate_id = str(row.get("candidate_id"))
        candidate = candidates.get(candidate_id)
        if candidate is None:
            continue
        metrics = row.get("metrics")
        if not isinstance(metrics, Mapping):
            continue
        resample = row.get("resample")
        parity_row = parity_by_id.get(candidate_id)
        parity = None
        exact = False
        if parity_row is not None:
            raw_parity = parity_row.get("parity")
            parity = dict(raw_parity) if isinstance(raw_parity, Mapping) else None
            exact = bool(parity_row.get("promotable"))
        result[candidate_id] = CandidateEvidence(
            candidate=candidate,
            label=labels[candidate_id],
            source=sources[candidate_id],
            evaluation_id=str(row.get("evaluation_id")),
            screen_passed=bool(row.get("screen_passed")),
            metrics={str(key): value for key, value in metrics.items()
                     if isinstance(value, (int, float))},
            resample=dict(resample) if isinstance(resample, Mapping) else None,
            parity=parity,
            exact=exact,
        )
    return result


def one_axis_neighbors(
    selected: CandidateSpec,
    candidates: Sequence[CandidateSpec],
) -> tuple[CandidateSpec, ...]:
    """其他参数不变时每个数值轴最近的上下邻居，与 validation 同规则。"""
    selected_keys = set(selected.parameters)
    neighbors: dict[str, CandidateSpec] = {}
    for parameter in sorted(selected.parameters):
        selected_value = float(selected.parameters[parameter])
        axis_candidates = [
            candidate for candidate in candidates
            if candidate.candidate_id != selected.candidate_id
            and set(candidate.parameters) == selected_keys
            and all(
                candidate.parameters[name] == selected.parameters[name]
                for name in selected_keys if name != parameter
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
            candidate = max(lower, key=lambda item: (
                float(item.parameters[parameter]), item.candidate_id,
            ))
            neighbors[candidate.candidate_id] = candidate
        if upper:
            candidate = min(upper, key=lambda item: (
                float(item.parameters[parameter]), item.candidate_id,
            ))
            neighbors[candidate.candidate_id] = candidate
    return tuple(neighbors[key] for key in sorted(neighbors))


@dataclass(frozen=True)
class Flatness:
    """一个候选的邻域平坦度。"""

    neighbor_count: int
    positive_ratio: float
    median_retention: float
    flat: bool

    def payload(self) -> Mapping[str, object]:
        """导出平坦度。"""
        return {
            "neighbor_count": self.neighbor_count,
            "positive_neighbor_ratio": self.positive_ratio,
            "median_neighbor_sharpe_retention": self.median_retention,
            "flat": self.flat,
        }


def flatness(
    item: CandidateEvidence,
    family_evidence: Mapping[str, CandidateEvidence],
    thresholds: ProposalThresholds,
) -> Flatness:
    """按粗筛 OOS Sharpe 计算邻域正向比例与中位保留率。"""
    neighbors = one_axis_neighbors(
        item.candidate, [entry.candidate for entry in family_evidence.values()],
    )
    sharpes = [family_evidence[n.candidate_id].oos_sharpe() for n in neighbors]
    nets = [family_evidence[n.candidate_id].oos_net_return() for n in neighbors]
    positive = (
        sum(s > 0.0 and n > 0.0 for s, n in zip(sharpes, nets, strict=True)) / len(sharpes)
        if sharpes else 0.0
    )
    own = item.oos_sharpe()
    retention = (
        statistics.median(value / own for value in sharpes)
        if sharpes and own > 0.0 else 0.0
    )
    flat = (
        len(neighbors) >= thresholds.minimum_neighbor_count
        and positive >= thresholds.minimum_positive_neighbor_ratio
        and retention >= thresholds.minimum_neighbor_sharpe_retention
    )
    return Flatness(len(neighbors), positive, retention, flat)


def _constraint_bounds(
    research_config: Mapping[str, object],
    family: str,
    parameter: str,
) -> tuple[float, float] | None:
    """读取 evolution.constraints 的轴边界。"""
    evolution = research_config.get("evolution")
    if not isinstance(evolution, Mapping):
        return None
    constraints = evolution.get("constraints")
    if not isinstance(constraints, Mapping):
        return None
    family_constraints = constraints.get(family)
    if not isinstance(family_constraints, Mapping):
        return None
    axis = family_constraints.get(parameter)
    if not isinstance(axis, Mapping):
        return None
    return (
        _number(axis.get("minimum"), "constraint.minimum"),
        _number(axis.get("maximum"), "constraint.maximum"),
    )


def _research_budget(research_config: Mapping[str, object], fallback: int) -> int:
    """读取研究配置的每流派候选预算。"""
    evolution = research_config.get("evolution")
    if not isinstance(evolution, Mapping):
        return fallback
    value = evolution.get("maximum_candidates_per_family")
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        return fallback
    return value


def _nearest_values(
    values: Sequence[float],
    anchor: float,
    limit: int,
) -> list[float]:
    """围绕锚点按秩距离保留至多 limit 个取值。"""
    ordered = sorted(values)
    if anchor not in ordered:
        ordered.append(anchor)
        ordered.sort()
    position = ordered.index(anchor)
    keep = [anchor]
    left = position - 1
    right = position + 1
    while len(keep) < limit and (left >= 0 or right < len(ordered)):
        if right < len(ordered):
            keep.append(ordered[right])
            right += 1
        if len(keep) < limit and left >= 0:
            keep.append(ordered[left])
            left -= 1
    return sorted(keep)


def _shrink_to_budget(
    axes: dict[str, list[float]],
    anchor: Mapping[str, int | float],
    budget: int,
) -> None:
    """逐步去掉离锚点最远的取值，直到网格乘积不超过预算。"""
    def product() -> int:
        total = 1
        for values in axes.values():
            total *= max(len(values), 1)
        return total

    while product() > budget:
        name = max(axes, key=lambda key: (len(axes[key]), key))
        values = axes[name]
        if len(values) <= 1:
            raise ValueError("提案网格无法缩减到研究预算内")
        anchor_value = float(anchor[name])
        farthest = max(values, key=lambda value: (abs(value - anchor_value), value))
        values.remove(farthest)


def _family_proposal(
    family: str,
    research_config: Mapping[str, object],
    family_evidence: Mapping[str, CandidateEvidence],
    thresholds: ProposalThresholds,
    fallback_budget: int,
) -> Mapping[str, object]:
    """为一个注册流派产出受约束网格提案。"""
    strategies = research_config.get("strategies")
    strategy = (
        strategies.get(family) if isinstance(strategies, Mapping) else None
    )
    if not isinstance(strategy, Mapping):
        raise ValueError(f"研究配置缺少 strategies.{family}")
    flat_by_id = {
        candidate_id: flatness(item, family_evidence, thresholds)
        for candidate_id, item in family_evidence.items()
    }
    exact = [item for item in family_evidence.values() if item.exact]
    flat_exact = [item for item in exact if flat_by_id[item.candidate.candidate_id].flat]
    summary = {
        "evaluated": len(family_evidence),
        "screen_passed": sum(item.screen_passed for item in family_evidence.values()),
        "exact": len(exact),
        "flat_exact": len(flat_exact),
    }
    if not flat_exact:
        return {
            "status": STATUS_NO_PROPOSAL,
            "reason": "no_exact_flat_candidate" if exact else "no_exact_candidate",
            "summary": summary,
            "current_strategy": dict(strategy),
        }
    metric = thresholds.score_metric
    anchor = max(
        flat_exact,
        key=lambda item: (item.score(metric), item.oos_sharpe(), item.candidate.candidate_id),
    )
    anchor_parameters = dict(anchor.candidate.parameters)
    axes: dict[str, list[float]] = {}
    evidence_by_axis: dict[str, dict[str, Mapping[str, object]]] = {}
    rejected_values: dict[str, list[float]] = {}
    slices: dict[str, dict[float, CandidateEvidence]] = {}
    for parameter, config_key in ARRAY_AXES.items():
        if parameter not in anchor_parameters:
            continue
        if not isinstance(strategy.get(config_key), list):
            continue
        axis_slice: dict[float, CandidateEvidence] = {}
        for item in family_evidence.values():
            parameters = item.candidate.parameters
            same_others = all(
                parameters[name] == anchor_parameters[name]
                for name in anchor_parameters if name != parameter
            )
            if same_others:
                axis_slice[float(parameters[parameter])] = item
        slices[parameter] = axis_slice
        bounds = _constraint_bounds(research_config, family, parameter)
        admissible: list[float] = []
        for value, item in sorted(axis_slice.items()):
            if bounds is not None and (value < bounds[0] or value > bounds[1]):
                rejected_values.setdefault(parameter, []).append(value)
                continue
            if item.candidate.candidate_id != anchor.candidate.candidate_id and (
                item.oos_sharpe() <= 0.0 or item.oos_net_return() <= 0.0
            ):
                continue
            admissible.append(value)
        axes[parameter] = _nearest_values(
            admissible, float(anchor_parameters[parameter]), thresholds.maximum_axis_values,
        )
    budget = _research_budget(research_config, fallback_budget)
    _shrink_to_budget(axes, anchor_parameters, budget)
    for parameter, values in axes.items():
        per_value: dict[str, Mapping[str, object]] = {}
        for value in values:
            slice_item = slices[parameter].get(value)
            per_value[repr(value)] = (
                {"evidence": None} if slice_item is None else {
                    **slice_item.payload(),
                    "flatness": flat_by_id[slice_item.candidate.candidate_id].payload(),
                }
            )
        evidence_by_axis[parameter] = per_value
    proposed_strategy: dict[str, object] = {}
    for key, value in strategy.items():
        axis_parameter = next(
            (name for name, config_key in ARRAY_AXES.items() if config_key == key), None,
        )
        if axis_parameter is not None and axis_parameter in axes:
            integral = axis_parameter == "lookback"
            proposed_strategy[key] = [
                int(round(item)) if integral else item for item in axes[axis_parameter]
            ]
        elif key in anchor_parameters:
            proposed_strategy[key] = anchor_parameters[key]
        else:
            proposed_strategy[key] = value
    grid_count = 1
    for values in axes.values():
        grid_count *= len(values)
    unchanged = _canonical_strategy(proposed_strategy) == _canonical_strategy(strategy)
    return {
        "status": STATUS_UNCHANGED if unchanged else STATUS_PROPOSED,
        "summary": summary,
        "anchor": {
            **anchor.payload(),
            "flatness": flat_by_id[anchor.candidate.candidate_id].payload(),
        },
        "current_strategy": dict(strategy),
        "proposed_strategy": proposed_strategy,
        "proposed_grid_count": grid_count,
        "candidate_budget": budget,
        "axis_evidence": evidence_by_axis,
        "rejected_by_constraint": {
            key: sorted(values) for key, values in rejected_values.items()
        },
        "flat_exact_candidates": [
            {
                **item.payload(),
                "flatness": flat_by_id[item.candidate.candidate_id].payload(),
            }
            for item in sorted(flat_exact, key=lambda entry: entry.candidate.candidate_id)
        ],
    }


def _canonical_strategy(strategy: Mapping[str, object]) -> str:
    """把策略段规范化为可比较文本。"""
    def normalize(value: object) -> object:
        if isinstance(value, list):
            return sorted(float(item) for item in value)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return float(value)
        return value

    return repr(sorted((key, normalize(value)) for key, value in strategy.items()))


def build_proposal(
    search_run_id: str,
    run_body: Mapping[str, object],
    research_config: Mapping[str, object],
    candidates: CandidateSet,
    trial_rows: Sequence[Mapping[str, object]],
    parity_rows: Sequence[Mapping[str, object]],
    thresholds: ProposalThresholds,
) -> Mapping[str, object]:
    """生成完整提案：注册流派网格建议与结构 challenger 证据。"""
    budgets = candidates.budgets
    evidence = collect_evidence(
        candidates.candidates, candidates.labels, candidates.sources,
        trial_rows, parity_rows,
    )
    families: dict[str, Mapping[str, object]] = {}
    structural: dict[str, Mapping[str, object]] = {}
    by_label: dict[str, dict[str, CandidateEvidence]] = {}
    for candidate_id, item in evidence.items():
        by_label.setdefault(item.label, {})[candidate_id] = item
    for family in sorted(budgets):
        families[family] = _family_proposal(
            family,
            research_config,
            by_label.get(family, {}),
            thresholds,
            budgets[family]["candidate_budget"],
        )
    for label, items in sorted(by_label.items()):
        if label in budgets:
            continue
        ordered = sorted(
            items.values(),
            key=lambda item: (-item.oos_sharpe(), item.candidate.candidate_id),
        )
        structural[label] = {
            "parent_family": label.split("~", 1)[0],
            "evaluated": len(ordered),
            "screen_passed": sum(item.screen_passed for item in ordered),
            "exact": sum(item.exact for item in ordered),
            "best": ordered[0].payload() if ordered else None,
            "activation_contract": (
                "structural challenger evidence only; source registration, "
                "clean commit and full ValidationExact required before any use"
            ),
        }
    research = run_body.get("research_config")
    return {
        "schema_version": PROPOSAL_SCHEMA_VERSION,
        "proposal_method_version": PROPOSAL_METHOD_VERSION,
        "status": "proposal_only",
        "search_run_id": search_run_id,
        "bundle_id": run_body.get("bundle_id"),
        "search_result_id": run_body.get("search_result_id"),
        "search_plan_id": run_body.get("search_plan_id"),
        "panel": run_body.get("panel"),
        "synthetic": run_body.get("synthetic"),
        "parent_research_config": research,
        "code_identity": run_body.get("code_identity"),
        "thresholds": thresholds.payload(),
        "families": families,
        "structural_challengers": structural,
        "holdout_consumed": False,
        "activation_contract": (
            "apply with scripts/promote_search_results.py to a new research config "
            "file, then run scripts/run_strategy_research.py; research gates unchanged"
        ),
    }
