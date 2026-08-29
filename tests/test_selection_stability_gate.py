"""选择稳定性闸门模式的准入、披露与旧配置零漂移测试（C-15、G-06）。"""
from __future__ import annotations

import copy
import math
from collections.abc import Mapping

import pytest

from guvolu.research.artifact_contracts import family_payload
from guvolu.research.provenance import canonical_json
from guvolu.research.validation import (
    SelectionStabilityGate,
    ValidationResult,
    _selection_stability_reasons,
    parse_selection_stability_gate,
    walk_forward_validate,
)
from guvolu.search.synthetic import synthetic_panel, synthetic_strategy_config
from guvolu.strategy.generation import build_family_batches

LOOKBACKS = (4, 8)
LEGACY_CONFIG: Mapping[str, object] = {
    "bar_interval": "1hour",
    "cost_model": {
        "fee_bps_assumption": 7.0,
        "half_spread_bps_assumption": 0.0,
        "slippage_bps_assumption": 0.0,
        "impact_bps_assumption": 0.0,
        "capacity_notional_quote": 0.0,
    },
    "features": {
        "lookbacks": list(LOOKBACKS),
        "state_lookback": 8,
        "volume_lookback": 8,
        "maximum_structural_gap_bars_assumption": 2,
    },
    "walk_forward": {
        "minimum_train_bars": 300,
        "test_bars": 100,
        "step_bars": 100,
        "embargo_bars": 4,
    },
    "validation": {
        "minimum_oos_bars": 10,
        "minimum_oos_sharpe": 0.0,
        "maximum_drawdown": 1.0,
        "maximum_fdr_q": 1.0,
        "minimum_positive_fold_ratio": 0.0,
        "maximum_probability_backtest_overfitting": 1.0,
        "pbo_split_budget": 64,
        "pbo_random_seed": 7,
        "block_bootstrap_bars": 24,
        "block_bootstrap_samples": 128,
        "block_bootstrap_random_seed": 11,
        "block_bootstrap_one_sided_alpha": 0.05,
        "maximum_block_bootstrap_p_value": 1.0,
        "minimum_deflated_sharpe_probability": 0.0,
        "deflated_sharpe_gate_trial_count": "effective",
        "minimum_parameter_neighbor_count": 1,
        "minimum_positive_parameter_neighbor_ratio": 0.0,
        "minimum_median_parameter_neighbor_sharpe_retention": 0.0,
        "complexity_penalty": 0.0,
    },
}
LEGACY_FAMILY_PAYLOAD_KEYS = frozenset({
    "family",
    "mode",
    "deployment_candidate_id",
    "deployment_parameters",
    "walk_forward_selection_path",
    "latest_unallocated_target",
    "validation_metrics",
    "deployment_oos_metrics",
    "metrics",
    "adjusted_sharpe",
    "fdr_q",
    "eligible",
    "rejection_reasons",
    "positive_fold_ratio",
    "most_selected_candidate_share",
    "median_selected_fold_sharpe",
    "probability_backtest_overfitting",
    "median_cscv_oos_rank",
    "cscv_split_count",
    "cscv_in_sample_fold_count",
    "cscv_out_sample_fold_count",
    "cscv_excluded_fold_count",
    "block_bootstrap_sharpe_lower_bound",
    "block_bootstrap_p_value",
    "block_bootstrap_sample_count",
    "deflated_sharpe_probability_raw",
    "deflated_sharpe_probability_effective",
    "deflated_sharpe_benchmark_raw",
    "deflated_sharpe_benchmark_effective",
    "raw_trial_count",
    "effective_trial_count",
    "parameter_neighbor_count",
    "positive_parameter_neighbor_ratio",
    "median_parameter_neighbor_sharpe_retention",
    "regime_attribution",
})


def _config_with_validation(**overrides: object) -> Mapping[str, object]:
    """构造带 validation 覆盖项的研究配置副本。"""
    config = copy.deepcopy(dict(LEGACY_CONFIG))
    validation = dict(config["validation"])  # type: ignore[arg-type]
    validation.update(overrides)
    config["validation"] = validation
    return config


def _run_validation(config: Mapping[str, object]) -> ValidationResult:
    """在固定种子合成面板上运行趋势家族验证。"""
    bars, features = synthetic_panel(800, LOOKBACKS, 23)
    candidates = build_family_batches(
        synthetic_strategy_config(LOOKBACKS), ("trend",),
    )[0].candidates
    return walk_forward_validate(
        "selection-stability-test", bars, features, candidates, config,
    )


def test_parse_gate_defaults_to_none_for_legacy_config() -> None:
    """未声明模式的旧配置解析为 None 并保持既有行为。"""
    validation = dict(LEGACY_CONFIG["validation"])  # type: ignore[arg-type]
    assert parse_selection_stability_gate(validation, 0.4) is None


def test_parse_gate_validates_mode_and_threshold() -> None:
    """模式取值与秩中位数阈值在配置解析处校验。"""
    with pytest.raises(ValueError):
        parse_selection_stability_gate(
            {"selection_stability_gate_mode": "unknown"}, 0.4,
        )
    with pytest.raises(ValueError):
        # 秩阈值必须伴随显式模式声明。
        parse_selection_stability_gate(
            {"minimum_median_cscv_oos_rank": 0.5}, 0.4,
        )
    with pytest.raises(ValueError):
        parse_selection_stability_gate({
            "selection_stability_gate_mode": "pbo_hard",
            "minimum_median_cscv_oos_rank": 0.5,
        }, 0.4)
    for invalid in (0.0, 1.0, 1.5, -0.2):
        with pytest.raises(ValueError):
            parse_selection_stability_gate({
                "selection_stability_gate_mode": "median_rank",
                "minimum_median_cscv_oos_rank": invalid,
            }, 0.4)
    with pytest.raises(ValueError):
        parse_selection_stability_gate({
            "selection_stability_gate_mode": "median_rank",
            "minimum_median_cscv_oos_rank": "0.5",
        }, 0.4)
    explicit_hard = parse_selection_stability_gate(
        {"selection_stability_gate_mode": "pbo_hard"}, 0.4,
    )
    assert explicit_hard == SelectionStabilityGate(
        mode="pbo_hard",
        maximum_probability_backtest_overfitting=0.4,
    )
    default_rank = parse_selection_stability_gate(
        {"selection_stability_gate_mode": "median_rank"}, 0.4,
    )
    assert default_rank is not None
    assert default_rank.minimum_median_cscv_oos_rank == 0.5
    explicit_rank = parse_selection_stability_gate({
        "selection_stability_gate_mode": "median_rank",
        "minimum_median_cscv_oos_rank": 0.55,
    }, 0.4)
    assert explicit_rank is not None
    assert explicit_rank.minimum_median_cscv_oos_rank == 0.55


def test_reasons_branch_pbo_hard_rejects_and_median_rank_admits() -> None:
    """高 PBO 高秩家族在两种模式下判定相反且披露不变。"""
    median_rank_gate = SelectionStabilityGate(
        mode="median_rank",
        maximum_probability_backtest_overfitting=0.4,
        minimum_median_cscv_oos_rank=0.5,
    )
    # 高 PBO 高秩的实测形态。
    assert _selection_stability_reasons(None, 0.4, 0.4023, 0.643, 512) == (
        "probability_backtest_overfitting_failed",
    )
    hard_gate = SelectionStabilityGate(
        mode="pbo_hard",
        maximum_probability_backtest_overfitting=0.4,
    )
    assert _selection_stability_reasons(hard_gate, 0.4, 0.4023, 0.643, 512) == (
        "probability_backtest_overfitting_failed",
    )
    assert _selection_stability_reasons(
        median_rank_gate, 0.4, 0.4023, 0.643, 512,
    ) == ()
    # PBO 恰等上限时放行。
    assert _selection_stability_reasons(None, 0.4, 0.4, 0.2, 512) == ()


def test_reasons_median_rank_fails_closed_on_missing_rank() -> None:
    """秩中位数缺失、非数或无分割时失败关闭拒绝。"""
    gate = SelectionStabilityGate(
        mode="median_rank",
        maximum_probability_backtest_overfitting=0.4,
        minimum_median_cscv_oos_rank=0.5,
    )
    rejection = ("median_cscv_oos_rank_failed",)
    # 噪声基线不高于阈值即拒绝。
    assert _selection_stability_reasons(gate, 0.4, 0.0, 0.5, 512) == rejection
    assert _selection_stability_reasons(gate, 0.4, 0.0, 0.643, 0) == rejection
    assert _selection_stability_reasons(
        gate, 0.4, 0.0, math.nan, 512,
    ) == rejection
    assert _selection_stability_reasons(gate, 0.4, 0.9, 0.643, 512) == ()
    broken = SelectionStabilityGate(
        mode="median_rank",
        maximum_probability_backtest_overfitting=0.4,
    )
    with pytest.raises(ValueError):
        _selection_stability_reasons(broken, 0.4, 0.0, 0.643, 512)


def test_legacy_config_rebuild_has_zero_drift() -> None:
    """旧配置（无新键）与显式 pbo_hard 行为一致且摘要字节不变。"""
    legacy = _run_validation(LEGACY_CONFIG)
    repeated = _run_validation(copy.deepcopy(dict(LEGACY_CONFIG)))
    explicit = _run_validation(
        _config_with_validation(selection_stability_gate_mode="pbo_hard"),
    )
    assert legacy.selection_stability_gate is None
    assert explicit.selection_stability_gate is not None
    # 家族级准入事实逐项一致。
    assert legacy.families == explicit.families
    legacy_payload = family_payload(legacy)
    assert canonical_json(legacy_payload) == canonical_json(
        family_payload(repeated),
    )
    for record in legacy_payload:
        # 旧摘要键集合固定不得新增。
        assert set(record) == LEGACY_FAMILY_PAYLOAD_KEYS
    explicit_payload = family_payload(explicit)
    stripped = [
        {
            key: value for key, value in record.items()
            if key != "selection_stability_gate"
        }
        for record in explicit_payload
    ]
    assert canonical_json(stripped) == canonical_json(legacy_payload)
    for record in explicit_payload:
        assert record["selection_stability_gate"] == {
            "mode": "pbo_hard",
            "maximum_probability_backtest_overfitting": 1.0,
        }


def test_median_rank_mode_changes_admission_and_discloses_pbo() -> None:
    """median_rank 模式不再受 PBO 阻断且摘要披露模式与阈值。"""
    hard = _run_validation(_config_with_validation(
        selection_stability_gate_mode="pbo_hard",
        maximum_probability_backtest_overfitting=-1.0,
    ))
    rank = _run_validation(_config_with_validation(
        selection_stability_gate_mode="median_rank",
        minimum_median_cscv_oos_rank=0.25,
        maximum_probability_backtest_overfitting=-1.0,
    ))
    assert len(hard.families) == len(rank.families) == 1
    hard_family = hard.families[0]
    rank_family = rank.families[0]
    # PBO 非负，负上限使硬模式必然拒绝。
    assert (
        "probability_backtest_overfitting_failed"
        in hard_family.rejection_reasons
    )
    assert (
        "probability_backtest_overfitting_failed"
        not in rank_family.rejection_reasons
    )
    expected_rejected = (
        rank_family.cscv_split_count <= 0
        or rank_family.median_cscv_oos_rank <= 0.25
    )
    assert (
        "median_cscv_oos_rank_failed" in rank_family.rejection_reasons
    ) == expected_rejected
    payload = family_payload(rank)[0]
    assert payload["selection_stability_gate"] == {
        "mode": "median_rank",
        "maximum_probability_backtest_overfitting": -1.0,
        "minimum_median_cscv_oos_rank": 0.25,
    }
    # 披露值来自同一次计算。
    assert payload["probability_backtest_overfitting"] == (
        rank_family.probability_backtest_overfitting
    )
    assert payload["median_cscv_oos_rank"] == rank_family.median_cscv_oos_rank
