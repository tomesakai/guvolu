"""研究运行双方共享的纯序列化证据合同。"""
from __future__ import annotations

import math
from collections.abc import Mapping

from guvolu.research.contracts import AllocationResult, PanelSnapshot
from guvolu.research.features import MarketState
from guvolu.research.provenance import canonical_json
from guvolu.research.validation import (
    PBO_METHOD_VERSION,
    P_VALUE_METHOD_VERSION,
    ValidationResult,
    metrics_payload,
    strategy_returns,
)

PIPELINE_SCHEMA_VERSION = 1
POSITION_CONTRACT_METHOD_VERSION = "risk-weighted-family-target-v1"
SECONDS_PER_YEAR = 365.0 * 24.0 * 60.0 * 60.0
INTERVAL_SECONDS = {
    "5min": 300.0,
    "15min": 900.0,
    "1hour": 3600.0,
    "4hour": 14_400.0,
}


def _mapping(value: object, name: str) -> Mapping[str, object]:
    """验证合同对象。"""
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} 必须为对象")
    return {str(key): item for key, item in value.items()}


def _integer(value: object, name: str) -> int:
    """验证正整数合同。"""
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{name} 必须为正整数")
    return value


def _number(value: object, name: str) -> float:
    """验证数值合同。"""
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValueError(f"{name} 必须为数值")
    return float(value)


def _text(value: object, name: str) -> str:
    """验证非空文本合同。"""
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} 必须为非空文本")
    return value.strip()


def trial_ledger_body(
    validation: ValidationResult,
    research_identity: str,
) -> str:
    """生成包含所有候选事实的规范 JSONL 字节文本。"""
    rows = [canonical_json({
        "record_type": "trial_ledger_header",
        "schema_version": PIPELINE_SCHEMA_VERSION,
        "research_identity": research_identity,
        "candidate_evaluations": len(validation.trials),
        "p_value_method_version": P_VALUE_METHOD_VERSION,
        "pbo_method_version": PBO_METHOD_VERSION,
        "block_bootstrap_method_version": (
            validation.block_bootstrap_method_version
        ),
    })]
    for trial in sorted(validation.trials, key=lambda item: item.evaluation_id):
        selection_role = "none"
        if trial.selected:
            selection_role = (
                "deployment_champion"
                if trial.fold_id == "full"
                else "fold_training_champion"
            )
        rows.append(canonical_json({
            "record_type": "trial",
            "evaluation_id": trial.evaluation_id,
            "candidate_id": trial.candidate.candidate_id,
            "family": trial.candidate.family,
            "mode": trial.candidate.mode,
            "parameters": dict(trial.candidate.parameters),
            "complexity": trial.candidate.complexity,
            "fold_id": trial.fold_id,
            "segment": trial.segment,
            "start_time": trial.start_time.isoformat(),
            "end_time": trial.end_time.isoformat(),
            "selected": trial.selected,
            "selection_role": selection_role,
            "metrics": metrics_payload(trial.metrics),
        }))
    return "\n".join(rows) + "\n"


def cost_replay_body(
    panel: PanelSnapshot,
    validation: ValidationResult,
    config: Mapping[str, object],
    research_identity: str,
) -> str:
    """生成与特征隔离的 label、成本和回放规范 JSONL。"""
    cost_config = _mapping(config.get("cost_model"), "cost_model")
    cost_bps = sum(_number(cost_config.get(name), name) for name in (
        "fee_bps_assumption",
        "half_spread_bps_assumption",
        "slippage_bps_assumption",
        "impact_bps_assumption",
    ))
    cost_rate = cost_bps / 10_000.0
    interval = _text(config.get("bar_interval"), "bar_interval")
    interval_seconds = INTERVAL_SECONDS.get(interval)
    if interval_seconds is None:
        raise ValueError("bar_interval 不支持 label 回放")
    feature_config = _mapping(config.get("features"), "features")
    maximum_gap = interval_seconds * _integer(
        feature_config.get("maximum_structural_gap_bars_assumption"),
        "maximum_structural_gap_bars_assumption",
    )
    deployment_candidates = {
        item.family: item.deployment_candidate for item in validation.families
    }
    deployment_targets = {
        family: validation.candidate_targets[candidate.candidate_id]
        for family, candidate in deployment_candidates.items()
    }
    validation_targets = validation.family_validation_targets
    replay_targets = {
        "deployment": deployment_targets,
        "walk_forward_stitched": validation_targets,
    }
    replay_returns = {
        replay: {
            family: strategy_returns(
                panel.bars,
                family_targets,
                cost_rate,
                maximum_gap,
            )
            for family, family_targets in targets.items()
        }
        for replay, targets in replay_targets.items()
    }
    if set(deployment_targets) != set(validation_targets):
        raise ValueError("部署与 walk-forward 流派范围不一致")
    fold_id_by_return_index = {
        index: fold.fold_id
        for fold in validation.folds
        for index in range(fold.test_start, fold.test_end)
    }
    rows = [canonical_json({
        "record_type": "label_cost_header",
        "schema_version": PIPELINE_SCHEMA_VERSION,
        "research_identity": research_identity,
        "panel_sha256": panel.panel_sha256,
        "cost_bps": cost_bps,
        "maximum_gap_seconds": maximum_gap,
        "deployment_candidates": {
            family: candidate.candidate_id
            for family, candidate in deployment_candidates.items()
        },
        "walk_forward_selection_paths": {
            item.family: list(item.fold_selected_candidate_ids)
            for item in validation.families
        },
        "replay_semantics": {
            "deployment": "one full-history deployment candidate per family",
            "walk_forward_stitched": (
                "fold-selected validation path used by gates; null outside "
                "the declared walk-forward OOS return indices"
            ),
        },
    })]
    for index in range(1, len(panel.bars)):
        previous = panel.bars[index - 1]
        current = panel.bars[index]
        gap_seconds = (current.open_time - previous.open_time).total_seconds()
        hard_gap = gap_seconds > maximum_gap
        market_return = (
            None if hard_gap else math.log(current.close / previous.close)
        )
        walk_forward_fold_id = fold_id_by_return_index.get(index)
        replay_rows: dict[str, object] = {}
        for replay, targets in replay_targets.items():
            if replay == "walk_forward_stitched" and walk_forward_fold_id is None:
                replay_rows[replay] = None
                continue
            family_rows: dict[str, object] = {}
            for family in sorted(targets):
                target = targets[family][index - 1]
                prior_target = targets[family][index - 2] if index >= 2 else 0.0
                turnover = abs(target - prior_target)
                if hard_gap:
                    turnover += abs(target)
                family_rows[family] = {
                    "target_at_decision": target,
                    "turnover": turnover,
                    "cost": turnover * cost_rate,
                    "next_net_return": replay_returns[replay][family][index],
                }
            replay_rows[replay] = family_rows
        rows.append(canonical_json({
            "record_type": "label_cost",
            "decision_time": previous.decision_time.isoformat(),
            "label_available_time": current.decision_time.isoformat(),
            "gap_seconds": gap_seconds,
            "hard_gap": hard_gap,
            "in_walk_forward_oos": walk_forward_fold_id is not None,
            "walk_forward_fold_id": walk_forward_fold_id,
            "next_market_log_return": market_return,
            "replays": replay_rows,
        }))
    return "\n".join(rows) + "\n"


def market_state_payload(value: MarketState) -> Mapping[str, object]:
    """把市场状态转换为稳定 JSON 载荷。"""
    return {
        "trend": value.trend,
        "volatility": value.volatility,
        "liquidity": value.liquidity,
        "flow": value.flow,
        "carry": value.carry,
        "cross_venue": value.cross_venue,
        "relative": value.relative,
        "jump": value.jump,
        "regime": value.regime,
        "uncertainty": value.uncertainty,
    }


def family_payload(validation: ValidationResult) -> list[Mapping[str, object]]:
    """生成稳定家族级评估摘要。"""
    return [{
        "family": item.family,
        "mode": item.mode,
        "deployment_candidate_id": item.deployment_candidate.candidate_id,
        "deployment_parameters": dict(item.deployment_candidate.parameters),
        "walk_forward_selection_path": list(item.fold_selected_candidate_ids),
        "latest_unallocated_target": item.latest_target,
        "validation_metrics": metrics_payload(item.metrics),
        "deployment_oos_metrics": metrics_payload(item.deployment_oos_metrics),
        "metrics": metrics_payload(item.metrics),
        "adjusted_sharpe": item.adjusted_sharpe,
        "fdr_q": item.fdr_q,
        "eligible": item.eligible,
        "rejection_reasons": list(item.rejection_reasons),
        "positive_fold_ratio": item.positive_fold_ratio,
        "most_selected_candidate_share": item.most_selected_candidate_share,
        "median_selected_fold_sharpe": item.median_selected_fold_sharpe,
        "probability_backtest_overfitting": item.probability_backtest_overfitting,
        "median_cscv_oos_rank": item.median_cscv_oos_rank,
        "cscv_split_count": item.cscv_split_count,
        "cscv_in_sample_fold_count": item.cscv_in_sample_fold_count,
        "cscv_out_sample_fold_count": item.cscv_out_sample_fold_count,
        "cscv_excluded_fold_count": item.cscv_excluded_fold_count,
        "block_bootstrap_sharpe_lower_bound": (
            item.block_bootstrap_sharpe_lower_bound
        ),
        "block_bootstrap_p_value": item.block_bootstrap_p_value,
        "block_bootstrap_sample_count": item.block_bootstrap_sample_count,
        "deflated_sharpe_probability_raw": item.deflated_sharpe_probability_raw,
        "deflated_sharpe_probability_effective": (
            item.deflated_sharpe_probability_effective
        ),
        "deflated_sharpe_benchmark_raw": item.deflated_sharpe_benchmark_raw,
        "deflated_sharpe_benchmark_effective": (
            item.deflated_sharpe_benchmark_effective
        ),
        "raw_trial_count": item.raw_trial_count,
        "effective_trial_count": item.effective_trial_count,
        "parameter_neighbor_count": item.parameter_neighbor_count,
        "positive_parameter_neighbor_ratio": (
            item.positive_parameter_neighbor_ratio
        ),
        "median_parameter_neighbor_sharpe_retention": (
            item.median_parameter_neighbor_sharpe_retention
        ),
    } for item in validation.families]


def position_contract_payload(
    validation: ValidationResult,
    allocation: AllocationResult,
) -> Mapping[str, object]:
    """把家族风险权重与方向目标合成为稳定组合目标。"""
    families: list[Mapping[str, object]] = []
    aggregate_target = 0.0
    for item in validation.families:
        weight = float(allocation.weights.get(item.family, 0.0))
        contribution = weight * item.latest_target
        aggregate_target += contribution
        families.append({
            "family": item.family,
            "deployment_candidate_id": item.deployment_candidate.candidate_id,
            "eligible": item.eligible,
            "family_target": item.latest_target,
            "allocation_weight": weight,
            "portfolio_target_contribution": contribution,
        })
    return {
        "method_version": POSITION_CONTRACT_METHOD_VERSION,
        "unit": "risk_weighted_directional_target",
        "aggregate_target": aggregate_target,
        "families": families,
    }
