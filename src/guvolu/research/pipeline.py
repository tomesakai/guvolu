"""完整策略研究、验证、分配与制品发布管线。"""
from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from guvolu.data.durable_io import atomic_write_text
from guvolu.research.allocator import allocate, allocation_payload, flat_allocation
from guvolu.research.config_lineage import (
    load_governed_strategy_config,
    snapshot_verified_config_lineage,
    verified_config_lineage_paths,
)
from guvolu.research.contracts import AllocationResult, PanelSnapshot, QualityVector
from guvolu.research.data_location import data_root_locator
from guvolu.research.features import (
    MarketState,
    classify_market_state,
    compute_features,
    feature_payload,
)
from guvolu.research.governance import (
    GOVERNANCE_METHOD_VERSION,
    register_active_head_receipt,
    register_research_exposure,
)
from guvolu.research.panel import (
    build_panel_snapshot,
    capture_trade_input_receipt,
    panel_inputs_payload,
    parse_time,
)
from guvolu.research.provenance import (
    artifact_record,
    canonical_json,
    code_identity,
    sha256_file,
    sha256_text,
    stable_identifier,
)
from guvolu.research.quality import (
    gate_feature_snapshot,
    panel_quality,
    quality_payload,
)
from guvolu.research.shadow import (
    cross_venue_shadow,
    l2_overlay_from_shadow,
    latest_common_l2_decision,
)
from guvolu.research.validation import (
    BLOCK_BOOTSTRAP_METHOD_VERSION,
    DEFLATED_SHARPE_METHOD_VERSION,
    EFFECTIVE_TRIAL_METHOD_VERSION,
    PARAMETER_STABILITY_METHOD_VERSION,
    PBO_METHOD_VERSION,
    P_VALUE_METHOD_VERSION,
    ValidationResult,
    evaluate_targets,
    metrics_payload,
    strategy_returns,
    walk_forward_validate,
)
from guvolu.strategy.contracts import FeatureRow
from guvolu.strategy.generation import (
    GENERATOR_METHOD_VERSION,
    build_family_batches,
    candidate_registry_payload,
)

PIPELINE_SCHEMA_VERSION = 1
PIPELINE_METHOD_VERSION = "strategy-research-pipeline-v12"
POSITION_CONTRACT_METHOD_VERSION = "risk-weighted-family-target-v1"
SECONDS_PER_YEAR = 365.0 * 24.0 * 60.0 * 60.0
_INTERVAL_SECONDS = {
    "5min": 300.0,
    "15min": 900.0,
    "1hour": 3600.0,
    "4hour": 14_400.0,
}


@dataclass(frozen=True)
class ResearchRunResult:
    """一次完整策略研究运行的输出位置。"""

    run_id: str
    run_directory: Path
    manifest_path: Path
    summary_path: Path
    trial_ledger_path: Path
    target_position_path: Path
    manifest_sha256: str
    decision_grade: bool
    paper_eligible_families: tuple[str, ...]
    operational_nonzero_families: tuple[str, ...]
    family_scope: tuple[str, ...]


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


def _text(value: object, name: str) -> str:
    """验证非空文本配置。"""
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} 必须为非空文本")
    return value.strip()


def _governance_registry_path(
    root: Path,
    config: Mapping[str, object],
) -> tuple[Path, str]:
    """解析并限制研究治理注册表位于项目目录。"""
    raw = config.get("data_governance")
    governance = {} if raw is None else _mapping(raw, "data_governance")
    scope = _text(governance.get("scope", "DEV_ADAPTIVE"), "data_governance.scope")
    if scope != "DEV_ADAPTIVE":
        raise ValueError("普通研究管线只允许 DEV_ADAPTIVE 数据范围")
    relative = _text(
        governance.get("registry", "data/research/governance.sqlite3"),
        "data_governance.registry",
    )
    configured = Path(relative)
    path = configured.resolve() if configured.is_absolute() else (root / configured).resolve()
    try:
        path.relative_to(root)
    except ValueError as error:
        raise ValueError("研究治理注册表必须位于项目目录内") from error
    return path, scope


def _write_content_text(
    directory: Path,
    prefix: str,
    suffix: str,
    body: str,
) -> tuple[Path, str]:
    """以内容散列写入确定性文本制品。"""
    directory.mkdir(parents=True, exist_ok=True)
    digest = sha256_text(body)
    path = directory / f"{prefix}-sha256-{digest}{suffix}"
    if path.exists():
        if sha256_file(path) != digest:
            raise ValueError(f"既有制品散列冲突: {path}")
    else:
        atomic_write_text(path, body)
    return path, digest


def _research_output_paths(
    output_base: Path,
    research_identity: str,
    execution_evaluated_at: datetime,
) -> tuple[str, Path, Path]:
    """分离执行快照目录与可复用的研究制品目录。"""
    run_id = stable_identifier("research-run", {
        "research_identity": research_identity,
        "execution_evaluated_at": execution_evaluated_at.isoformat(),
    })
    return (
        run_id,
        output_base / run_id,
        output_base / "research-artifacts" / research_identity,
    )


def _feature_artifact(
    directory: Path,
    features: Sequence[FeatureRow],
    panel: PanelSnapshot,
) -> tuple[Path, str]:
    """发布带输入血缘的特征 JSONL。"""
    header = canonical_json({
        "record_type": "feature_header",
        "schema_version": PIPELINE_SCHEMA_VERSION,
        "panel_sha256": panel.panel_sha256,
        "head_generation": panel.head_generation,
        "attempt_ids": list(panel.attempt_ids),
        "artifact_ids": list(panel.artifact_ids),
    })
    rows = [header]
    for feature in features:
        rows.append(canonical_json({
            "record_type": "feature",
            **feature_payload(feature),
        }))
    return _write_content_text(
        directory,
        "feature-panel",
        ".jsonl",
        "\n".join(rows) + "\n",
    )


def _trial_artifact(
    directory: Path,
    validation: ValidationResult,
    research_identity: str,
) -> tuple[Path, str]:
    """发布包含所有候选事实的追加式台账。"""
    rows = [canonical_json({
        "record_type": "trial_ledger_header",
        "schema_version": PIPELINE_SCHEMA_VERSION,
        "research_identity": research_identity,
        "candidate_evaluations": len(validation.trials),
        "p_value_method_version": P_VALUE_METHOD_VERSION,
        "pbo_method_version": PBO_METHOD_VERSION,
        "block_bootstrap_method_version": BLOCK_BOOTSTRAP_METHOD_VERSION,
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
    return _write_content_text(
        directory,
        "trial-ledger",
        ".jsonl",
        "\n".join(rows) + "\n",
    )


def _cost_replay_artifact(
    directory: Path,
    panel: PanelSnapshot,
    validation: ValidationResult,
    config: Mapping[str, object],
    research_identity: str,
) -> tuple[Path, str]:
    """发布与特征隔离的 label、成本和回放事实。"""
    cost_config = _mapping(config.get("cost_model"), "cost_model")
    cost_bps = sum(_number(cost_config.get(name), name) for name in (
        "fee_bps_assumption",
        "half_spread_bps_assumption",
        "slippage_bps_assumption",
        "impact_bps_assumption",
    ))
    cost_rate = cost_bps / 10_000.0
    interval = _text(config.get("bar_interval"), "bar_interval")
    interval_seconds = _INTERVAL_SECONDS.get(interval)
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
    return _write_content_text(
        directory,
        "label-cost-replay",
        ".jsonl",
        "\n".join(rows) + "\n",
    )
def _market_state_payload(value: MarketState) -> Mapping[str, object]:
    """把市场状态转换为 JSON 载荷。"""
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


def _family_payload(validation: ValidationResult) -> list[Mapping[str, object]]:
    """生成家族级评估摘要。"""
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
        "deflated_sharpe_probability_raw": (
            item.deflated_sharpe_probability_raw
        ),
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


def _position_contract_payload(
    validation: ValidationResult,
    allocation: AllocationResult,
) -> Mapping[str, object]:
    """把家族风险权重与方向目标合成为可审计的组合目标。"""
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


def _disabled_families() -> list[Mapping[str, object]]:
    """列出缺少事实闭环而固定为零的系列。"""
    return [
        {"family": "market_making", "mode": "disabled", "weight": 0,
         "reasons": ["l3_unavailable", "private_fill_lifecycle_unavailable"]},
        {"family": "queue_reactive", "mode": "disabled", "weight": 0,
         "reasons": ["mbo_unavailable", "queue_position_unobservable"]},
        {"family": "cross_venue_arbitrage", "mode": "shadow", "weight": 0,
         "reasons": ["dual_venue_private_reconciliation_unavailable", "leg_risk_unmodeled"]},
        {"family": "triangular_arbitrage", "mode": "disabled", "weight": 0,
         "reasons": ["synchronous_three_leg_quotes_unavailable"]},
        {"family": "funding_basis", "mode": "disabled", "weight": 0,
         "reasons": ["funding_mark_index_oi_contract_facts_unavailable"]},
        {"family": "cex_dex_arbitrage", "mode": "disabled", "weight": 0,
         "reasons": ["chain_pool_gas_mev_facts_unavailable"]},
        {"family": "liquidation", "mode": "disabled", "weight": 0,
         "reasons": ["liquidation_feed_unavailable"]},
        {"family": "cross_sectional_multifactor", "mode": "disabled", "weight": 0,
         "reasons": ["pit_universe_lifecycle_fx_exposure_model_unavailable"]},
    ]


def _relative(path: Path, root: Path) -> str:
    """生成仓库相对输出位置。"""
    return path.resolve().relative_to(root.resolve()).as_posix()


def _markdown_summary(summary: Mapping[str, Any]) -> str:
    """生成面向人工复核的研究摘要。"""
    family_rows = summary["family_evaluations"]
    if not isinstance(family_rows, list):
        raise TypeError("家族评估摘要非法")
    lines = [
        "# 策略研究运行摘要",
        "",
        f"> 运行标识：`{summary['run_id']}`。本结果仅为研究、paper 或 shadow，未触碰交易端点。",
        "",
        "## 1. 结论",
        "",
        f"- 决策级代码身份：{'可' if summary['decision_grade'] else '不可'}。",
        f"- 冻结面板柱数：{summary['panel']['bars']}。",
        f"- 研究回放质量门禁：{'通过' if summary['research_quality']['eligible'] else '未通过'}。",
        f"- 运行时质量门禁：{'通过' if summary['operational_quality']['eligible'] else '未通过'}。",
        "- 做市、queue、真实套利、衍生品和链上系列权重固定为零。",
        "",
        "## 2. 家族样本外结果",
        "",
        "| 家族 | 模式 | Sharpe | 净对数收益 | 回撤 | FDR q | PBO | Block p | 准入 |",
        "|---|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in family_rows:
        metrics = row["metrics"]
        lines.append(
            f"| {row['family']} | {row['mode']} | {metrics['sharpe']:.4f} | "
            f"{metrics['net_return']:.6f} | {metrics['maximum_drawdown']:.4f} | "
            f"{row['fdr_q']:.6f} | "
            f"{row['probability_backtest_overfitting']:.4f} | "
            f"{row['block_bootstrap_p_value']:.4f} | "
            f"{'可' if row['eligible'] else '不可'} |"
        )
    research = summary["research_position"]
    operational = summary["operational_position"]
    research_contract = summary["research_target_contract"]
    operational_contract = summary["operational_target_contract"]
    lines.extend([
        "",
        "## 3. 资本分配与组合目标",
        "",
        "```json",
        json.dumps({
            "research_allocation_weights": research,
            "operational_allocation_weights": operational,
            "research_target_contract": research_contract,
            "operational_target_contract": operational_contract,
        }, ensure_ascii=False, indent=2),
        "```",
        "",
        "## 4. 主要制品",
        "",
        f"- 紧凑面板：`{summary['artifacts']['panel']['path']}`",
        f"- 特征面板：`{summary['artifacts']['features']['path']}`",
        f"- 标签成本回放：`{summary['artifacts']['label_cost_replay']['path']}`",
        f"- 试验台账：`{summary['artifacts']['trial_ledger']['path']}`",
        f"- 目标位置：`{summary['artifacts']['target_position']['path']}`",
        "",
    ])
    return "\n".join(lines)


def run_research(
    repository_root: Path,
    config_path: Path,
    output_root: Path | None = None,
    family_scope: Sequence[str] | None = None,
    data_root: Path | None = None,
) -> ResearchRunResult:
    """运行完整 CPU 策略研究闭环。"""
    root = repository_root.resolve()
    config_file = config_path.resolve()
    (
        config,
        config_hash,
        lineage_root_config_hash,
        config_lineage_depth,
    ) = load_governed_strategy_config(root, config_file)
    research_data_root = root / "data"
    source_data_root = (data_root or research_data_root).resolve()
    source_data_root_record = data_root_locator(root, source_data_root)
    output_base = (output_root or root / "reports" / "strategy-research").resolve()
    config_source_paths = verified_config_lineage_paths(root, config_file)
    identity = code_identity(root, config_source_paths)
    batches = build_family_batches(config, family_scope)
    resolved_family_scope = tuple(batch.family for batch in batches)
    candidates = tuple(
        candidate for batch in batches for candidate in batch.candidates
    )
    market_id = _text(config.get("market_id"), "market_id")
    inputs = capture_trade_input_receipt(
        source_data_root,
        market_id,
        research_data_root / "research" / "input-receipts",
    )
    if inputs.receipt_path is None or inputs.receipt_sha256 is None:
        raise AssertionError("研究输入没有生成活动 head 收据")
    input_event = inputs.maximum_event_time
    execution_evaluated_at = datetime.now(UTC)
    exposure_start = parse_time(config.get("from_time"), "from_time")
    governance_path, data_scope = _governance_registry_path(root, config)
    research_identity = stable_identifier("research-identity", {
        "pipeline_method_version": PIPELINE_METHOD_VERSION,
        "p_value_method_version": P_VALUE_METHOD_VERSION,
        "pbo_method_version": PBO_METHOD_VERSION,
        "block_bootstrap_method_version": BLOCK_BOOTSTRAP_METHOD_VERSION,
        "deflated_sharpe_method_version": DEFLATED_SHARPE_METHOD_VERSION,
        "effective_trial_method_version": EFFECTIVE_TRIAL_METHOD_VERSION,
        "parameter_stability_method_version": PARAMETER_STABILITY_METHOD_VERSION,
        "position_contract_method_version": POSITION_CONTRACT_METHOD_VERSION,
        "config_hash": config_hash,
        "config_lineage_root_hash": lineage_root_config_hash,
        "config_lineage_depth": config_lineage_depth,
        "head_generation": inputs.head_generation,
        "attempt_ids": inputs.attempt_ids,
        "artifact_ids": inputs.artifact_ids,
        "input_receipt_sha256": inputs.receipt_sha256,
        "code_tree_digest": identity.tree_digest,
        "dirty_digest": identity.dirty_digest,
        "generator_method_version": GENERATOR_METHOD_VERSION,
        "family_scope": resolved_family_scope,
        "candidate_ids": tuple(candidate.candidate_id for candidate in candidates),
        "governance_method_version": GOVERNANCE_METHOD_VERSION,
        "data_scope": data_scope,
    })
    run_id, run_directory, artifact_directory = _research_output_paths(
        output_base,
        research_identity,
        execution_evaluated_at,
    )
    config_snapshot = snapshot_verified_config_lineage(
        root, config_file, artifact_directory,
    )
    exposure = register_research_exposure(
        governance_path,
        research_identity,
        market_id,
        exposure_start,
        input_event,
        recorded_at=execution_evaluated_at,
    )
    receipt_registration = register_active_head_receipt(
        governance_path,
        "research",
        research_identity,
        market_id,
        inputs.head_generation,
        _relative(inputs.receipt_path, root),
        inputs.receipt_sha256,
        repository_root=root,
        data_root=source_data_root,
        recorded_at=execution_evaluated_at,
    )
    panel = build_panel_snapshot(
        inputs,
        research_data_root / "research" / "physical" / market_id,
        _text(config.get("bar_interval"), "bar_interval"),
        exposure_start,
        input_event,
        _integer(config.get("notional_scale"), "notional_scale"),
    )
    feature_config = _mapping(config.get("features"), "features")
    lookbacks_value = feature_config.get("lookbacks")
    if not isinstance(lookbacks_value, list):
        raise ValueError("features.lookbacks 必须为数组")
    lookbacks = tuple(_integer(value, "features.lookbacks") for value in lookbacks_value)
    features = compute_features(
        panel.bars,
        lookbacks,
        _integer(feature_config.get("volume_lookback"), "volume_lookback"),
        _integer(
            feature_config.get("maximum_structural_gap_bars_assumption"),
            "maximum_structural_gap_bars_assumption",
        ),
    )
    state_lookback = _integer(
        feature_config.get("state_lookback"), "state_lookback",
    )
    valid_indices = [
        index for index, feature in enumerate(features)
        if feature.contiguous
        and feature.volume_score is not None
        and feature.trend_scores.get(state_lookback) is not None
    ]
    if not valid_indices:
        raise ValueError("研究面板没有可用策略决策时点")
    decision_index = valid_indices[-1]
    strategy_decision_time = features[decision_index].decision_time
    interval = _text(config.get("bar_interval"), "bar_interval")
    interval_seconds = _INTERVAL_SECONDS.get(interval)
    if interval_seconds is None:
        raise ValueError("不支持的研究节拍")
    periods_per_year = SECONDS_PER_YEAR / interval_seconds
    validation = walk_forward_validate(
        research_identity,
        panel.bars,
        features,
        candidates,
        config,
        decision_index=decision_index,
    )
    maximum_age = _integer(
        config.get("strategy_decision_max_age_seconds"),
        "strategy_decision_max_age_seconds",
    )
    minimum_bars = _integer(
        _mapping(config.get("validation"), "validation").get("minimum_oos_bars"),
        "minimum_oos_bars",
    )
    research_quality = panel_quality(
        panel,
        strategy_decision_time,
        maximum_age,
        minimum_bars,
    )
    operational_quality = gate_feature_snapshot(
        panel_quality(
            panel,
            execution_evaluated_at,
            maximum_age,
            minimum_bars,
        ),
        strategy_decision_time,
        execution_evaluated_at,
        maximum_age,
    )
    if not identity.decision_grade:
        operational_quality = QualityVector(
            integrity=operational_quality.integrity,
            freshness=operational_quality.freshness,
            clock=operational_quality.clock,
            coverage=operational_quality.coverage,
            pit=operational_quality.pit,
            lineage=False,
            reasons=tuple(sorted({
                *operational_quality.reasons,
                f"code_identity_{identity.reason or 'not_decision_grade'}",
            })),
        )
    market_state = classify_market_state(
        features[decision_index],
        state_lookback,
        features[decision_index].volume_score,
        periods_per_year,
    )
    cross_config = _mapping(
        config.get("cross_venue_shadow"), "cross_venue_shadow",
    )
    raw_market_ids = cross_config.get("market_ids")
    if not isinstance(raw_market_ids, list):
        raise ValueError("cross_venue_shadow.market_ids 必须为数组")
    l2_decision = latest_common_l2_decision(
        source_data_root,
        tuple(str(value) for value in raw_market_ids),
    )
    shadow = cross_venue_shadow(source_data_root, l2_decision, cross_config)
    decision_shadow = cross_venue_shadow(
        source_data_root,
        strategy_decision_time,
        cross_config,
    )
    overlay, overlay_evidence = l2_overlay_from_shadow(
        decision_shadow,
        market_id,
    )
    allocation_config = _mapping(config.get("allocation"), "allocation")
    research_position = allocate(
        validation.families,
        market_state,
        research_quality,
        allocation_config,
        # L2 尚无成交 head 的历史收据。
        # 研究和冻结权重只使用
        # 受保护的成交 panel，
        # 并采用可重建的零 overlay。
        l2_overlay=0.0,
    )
    operational_position = allocate(
        validation.families,
        market_state,
        operational_quality,
        allocation_config,
        l2_overlay=overlay,
    )
    research_target_contract = _position_contract_payload(
        validation,
        research_position,
    )
    operational_target_contract = _position_contract_payload(
        validation,
        operational_position,
    )
    no_l2_position = allocate(
        validation.families,
        market_state,
        research_quality,
        allocation_config,
        l2_overlay=0.0,
    )
    unconditional_state = MarketState(
        trend=market_state.trend,
        volatility=market_state.volatility,
        liquidity=market_state.liquidity,
        flow=market_state.flow,
        carry=market_state.carry,
        cross_venue=market_state.cross_venue,
        relative=market_state.relative,
        jump=market_state.jump,
        regime="unconditional",
        uncertainty=market_state.uncertainty,
    )
    no_regime_position = allocate(
        validation.families,
        unconditional_state,
        research_quality,
        allocation_config,
        l2_overlay=0.0,
    )
    cost_config = _mapping(config.get("cost_model"), "cost_model")
    cost_rate = sum(_number(cost_config.get(name), name) for name in (
        "fee_bps_assumption",
        "half_spread_bps_assumption",
        "slippage_bps_assumption",
        "impact_bps_assumption",
    )) / 10_000.0
    maximum_gap = interval_seconds * _integer(
        feature_config.get("maximum_structural_gap_bars_assumption"),
        "maximum_structural_gap_bars_assumption",
    )
    fixed_position_metrics = evaluate_targets(
        panel.bars,
        tuple(1.0 for _bar in panel.bars),
        validation.folds[0].test_start,
        validation.folds[-1].test_end,
        cost_rate,
        _number(
            cost_config.get("capacity_notional_quote"),
            "capacity_notional_quote",
        ),
        maximum_gap,
        periods_per_year,
    )
    ablations = {
        "fixed_long": metrics_payload(fixed_position_metrics),
        "single_strategy": _family_payload(validation),
        "no_l2_overlay": allocation_payload(no_l2_position),
        "no_regime": allocation_payload(no_regime_position),
    }
    feature_path, feature_hash = _feature_artifact(
        artifact_directory,
        features,
        panel,
    )
    trial_path, trial_hash = _trial_artifact(
        artifact_directory,
        validation,
        research_identity,
    )
    replay_path, replay_hash = _cost_replay_artifact(
        artifact_directory,
        panel,
        validation,
        config,
        research_identity,
    )
    registry_path, registry_hash = _write_content_text(
        artifact_directory,
        "candidate-registry",
        ".json",
        canonical_json(candidate_registry_payload(batches, config_hash)) + "\n",
    )
    target_payload = {
        "schema_version": PIPELINE_SCHEMA_VERSION,
        "run_id": run_id,
        "research_identity": research_identity,
        "family_scope": list(resolved_family_scope),
        "decision_time": strategy_decision_time.isoformat(),
        "execution_evaluated_at": execution_evaluated_at.isoformat(),
        "market_id": market_id,
        "market_state": _market_state_payload(market_state),
        "l2_overlay": overlay_evidence,
        "l2_overlay_shadow_decision_time": decision_shadow["decision_time"],
        "research_quality": quality_payload(research_quality),
        "operational_quality": quality_payload(operational_quality),
        "research_replay": allocation_payload(research_position),
        "operational": allocation_payload(operational_position),
        "research_target_contract": research_target_contract,
        "operational_target_contract": operational_target_contract,
        "ablations": ablations,
        "disabled_families": _disabled_families(),
    }
    target_path, target_hash = _write_content_text(
        artifact_directory,
        "target-position",
        ".json",
        canonical_json(target_payload) + "\n",
    )
    shadow_path, shadow_hash = _write_content_text(
        artifact_directory,
        "cross-venue-shadow",
        ".json",
        canonical_json(shadow) + "\n",
    )
    artifacts = {
        "input_receipt": {
            **artifact_record(inputs.receipt_path, "active_trade_head_receipt"),
            "path": _relative(inputs.receipt_path, root),
        },
        "config": {
            **artifact_record(
                config_snapshot.leaf_config_path, "research_config_snapshot",
            ),
            "path": _relative(config_snapshot.leaf_config_path, root),
        },
        "config_lineage": {
            **artifact_record(
                config_snapshot.bundle_path, "research_config_lineage",
            ),
            "path": _relative(config_snapshot.bundle_path, root),
        },
        "panel": {
            **artifact_record(panel.panel_path, "research_physical_panel"),
            "path": _relative(panel.panel_path, root),
        },
        "features": {
            "kind": "feature_panel",
            "path": _relative(feature_path, root),
            "sha256": feature_hash,
            "bytes": feature_path.stat().st_size,
        },
        "trial_ledger": {
            "kind": "trial_ledger",
            "path": _relative(trial_path, root),
            "sha256": trial_hash,
            "bytes": trial_path.stat().st_size,
        },
        "target_position": {
            "kind": "target_position",
            "path": _relative(target_path, root),
            "sha256": target_hash,
            "bytes": target_path.stat().st_size,
        },
        "cross_venue_shadow": {
            "kind": "cross_venue_shadow",
            "path": _relative(shadow_path, root),
            "sha256": shadow_hash,
            "bytes": shadow_path.stat().st_size,
        },
        "label_cost_replay": {
            "kind": "label_cost_replay",
            "path": _relative(replay_path, root),
            "sha256": replay_hash,
            "bytes": replay_path.stat().st_size,
        },
        "candidate_registry": {
            "kind": "candidate_registry",
            "path": _relative(registry_path, root),
            "sha256": registry_hash,
            "bytes": registry_path.stat().st_size,
        },
    }
    summary: dict[str, object] = {
        "schema_version": PIPELINE_SCHEMA_VERSION,
        "pipeline_method_version": PIPELINE_METHOD_VERSION,
        "p_value_method_version": P_VALUE_METHOD_VERSION,
        "pbo_method_version": PBO_METHOD_VERSION,
        "block_bootstrap_method_version": BLOCK_BOOTSTRAP_METHOD_VERSION,
        "deflated_sharpe_method_version": DEFLATED_SHARPE_METHOD_VERSION,
        "effective_trial_method_version": EFFECTIVE_TRIAL_METHOD_VERSION,
        "parameter_stability_method_version": PARAMETER_STABILITY_METHOD_VERSION,
        "position_contract_method_version": POSITION_CONTRACT_METHOD_VERSION,
        "governance_method_version": GOVERNANCE_METHOD_VERSION,
        "run_id": run_id,
        "research_identity": research_identity,
        "generator_method_version": GENERATOR_METHOD_VERSION,
        "family_scope": list(resolved_family_scope),
        "market_id": market_id,
        "decision_time": strategy_decision_time.isoformat(),
        "execution_evaluated_at": execution_evaluated_at.isoformat(),
        "source_data_root": source_data_root_record,
        "decision_grade": identity.decision_grade,
        "code_identity": asdict(identity),
        "config_hash": config_hash,
        "config_lineage_root_hash": lineage_root_config_hash,
        "config_lineage_depth": config_lineage_depth,
        "data_governance": {
            "scope": data_scope,
            "exposure_id": exposure.exposure_id,
            "from_time": exposure.start_time.isoformat(),
            "to_time": exposure.end_time.isoformat(),
            "registry": _relative(governance_path, root),
            "input_receipt_sha256": (
                receipt_registration.receipt_artifact_sha256
            ),
        },
        "input": panel_inputs_payload(inputs),
        "panel": {
            "bars": len(panel.bars),
            "from_time": panel.bars[0].open_time.isoformat(),
            "to_time": panel.bars[-1].decision_time.isoformat(),
            "latest_available_time": panel.latest_available_time.isoformat(),
            "sha256": panel.panel_sha256,
        },
        "strategy_decision": {
            "feature_index": decision_index,
            "decision_time": strategy_decision_time.isoformat(),
            "age_at_latest_input_seconds": (
                input_event - strategy_decision_time
            ).total_seconds(),
        },
        "feature_count": len(features),
        "candidate_count": len(candidates),
        "trial_count": len(validation.trials),
        "folds": [asdict(fold) for fold in validation.folds],
        "market_state": _market_state_payload(market_state),
        "research_quality": quality_payload(research_quality),
        "operational_quality": quality_payload(operational_quality),
        "family_evaluations": _family_payload(validation),
        "research_position": allocation_payload(research_position),
        "operational_position": allocation_payload(operational_position),
        "research_target_contract": research_target_contract,
        "operational_target_contract": operational_target_contract,
        "ablations": ablations,
        "l2_overlay": overlay_evidence,
        "cross_venue_shadow": shadow,
        "decision_aligned_cross_venue_shadow": decision_shadow,
        "disabled_families": _disabled_families(),
        "artifacts": artifacts,
    }
    summary_path = run_directory / "summary.json"
    atomic_write_text(summary_path, canonical_json(summary) + "\n")
    summary_text_path = run_directory / "summary.txt"
    atomic_write_text(summary_text_path, _markdown_summary(summary))
    manifest = {
        "schema_version": PIPELINE_SCHEMA_VERSION,
        "pipeline_method_version": PIPELINE_METHOD_VERSION,
        "p_value_method_version": P_VALUE_METHOD_VERSION,
        "pbo_method_version": PBO_METHOD_VERSION,
        "block_bootstrap_method_version": BLOCK_BOOTSTRAP_METHOD_VERSION,
        "deflated_sharpe_method_version": DEFLATED_SHARPE_METHOD_VERSION,
        "effective_trial_method_version": EFFECTIVE_TRIAL_METHOD_VERSION,
        "parameter_stability_method_version": PARAMETER_STABILITY_METHOD_VERSION,
        "position_contract_method_version": POSITION_CONTRACT_METHOD_VERSION,
        "governance_method_version": GOVERNANCE_METHOD_VERSION,
        "run_id": run_id,
        "research_identity": research_identity,
        "generator_method_version": GENERATOR_METHOD_VERSION,
        "family_scope": list(resolved_family_scope),
        "decision_time": strategy_decision_time.isoformat(),
        "execution_evaluated_at": execution_evaluated_at.isoformat(),
        "source_data_root": source_data_root_record,
        "code_identity": asdict(identity),
        "config_hash": config_hash,
        "config_lineage_root_hash": lineage_root_config_hash,
        "config_lineage_depth": config_lineage_depth,
        "data_scope": data_scope,
        "research_exposure_id": exposure.exposure_id,
        "input_head_generation": panel.head_generation,
        "input_receipt_sha256": inputs.receipt_sha256,
        "input_attempt_ids": list(panel.attempt_ids),
        "input_artifact_ids": list(panel.artifact_ids),
        "normalization_versions": list(panel.normalization_versions),
        "artifacts": {
            **artifacts,
            "summary_json": {
                **artifact_record(summary_path, "research_summary"),
                "path": _relative(summary_path, root),
            },
            "summary_text": {
                **artifact_record(summary_text_path, "research_summary_text"),
                "path": _relative(summary_text_path, root),
            },
        },
    }
    manifest_path = run_directory / "manifest.json"
    atomic_write_text(manifest_path, canonical_json(manifest) + "\n")
    manifest_hash = sha256_file(manifest_path)
    latest = output_base / "latest.json"
    atomic_write_text(latest, canonical_json({
        "run_id": run_id,
        "manifest": _relative(manifest_path, root),
        "manifest_sha256": manifest_hash,
        "summary": _relative(summary_path, root),
    }) + "\n")
    return ResearchRunResult(
        run_id=run_id,
        run_directory=run_directory,
        manifest_path=manifest_path,
        summary_path=summary_path,
        trial_ledger_path=trial_path,
        target_position_path=target_path,
        manifest_sha256=manifest_hash,
        decision_grade=identity.decision_grade,
        paper_eligible_families=tuple(
            item.family for item in validation.families if item.eligible
        ),
        operational_nonzero_families=tuple(
            item.family for item in validation.families
            if abs(
                operational_position.weights.get(item.family, 0.0)
                * item.latest_target
            ) > 1e-12
        ),
        family_scope=resolved_family_scope,
    )
