"""P3-2 重采样粗筛与 CPU research.validation 的对照测试（合成面板）。"""
from __future__ import annotations

import math
from collections.abc import Mapping

import pytest

from guvolu.research.validation import (
    _probability_backtest_overfitting,
    _studentized_circular_block_bootstrap_sharpe,
    evaluate_targets,
    make_folds,
    walk_forward_validate,
)
from guvolu.search.kernels import KernelSession
from guvolu.search.metrics import strategy_returns_tensor
from guvolu.search.resample import (
    ResampleScreen,
    ResampleSpec,
    bootstrap_block_starts,
    cscv_subsets,
    family_bootstrap_seed,
    family_cscv_seed,
    resample_chunk,
    resample_screen_from_config,
    resample_spec_from_config,
    resample_tolerance_from_config,
)
from guvolu.search.scan import scan_targets
from guvolu.strategy.generation import build_family_batches
from guvolu.search.synthetic import synthetic_strategy_config
from searchfast_support import build_fixture, torch_devices

torch = pytest.importorskip("torch")
DEVICES = torch_devices()
PERIODS = 8760.0
COST_RATE = 0.0007
GAP = 7200.0
LOOKBACKS = (4, 8)
RESEARCH_CONFIG: Mapping[str, object] = {
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
COST_MODEL = {
    "one_way_cost_rate": COST_RATE,
    "maximum_gap_seconds": GAP,
    "periods_per_year": PERIODS,
}


def _spec() -> ResampleSpec:
    """由测试研究配置读取重采样参数。"""
    return resample_spec_from_config(RESEARCH_CONFIG)


def test_spec_and_tolerance_from_config() -> None:
    """折、bootstrap、CSCV 参数与容差均来自配置。"""
    spec = _spec()
    assert spec.fold_spec_payload()["test_bars"] == 100
    assert spec.bootstrap_payload()["paths"] == 128
    assert spec.payload()["cscv"]["split_budget"] == 64
    tolerance = resample_tolerance_from_config({"pbo_abs": 0.01})
    assert tolerance.pbo_abs == 0.01
    with pytest.raises(ValueError):
        resample_tolerance_from_config({"pbo_abs": -1.0})
    with pytest.raises(ValueError):
        resample_spec_from_config({"walk_forward": {}, "validation": {}})


def test_cscv_subsets_match_validation_enumeration() -> None:
    """子集生成与 validation 的 CSCV 分割同序同集。"""
    exhaustive = cscv_subsets(8, 64, 1)
    assert len(exhaustive) == math.comb(8, 4) // 2
    sampled_a = cscv_subsets(12, 16, 5)
    sampled_b = cscv_subsets(12, 16, 5)
    assert sampled_a == sampled_b and len(sampled_a) == 16
    assert cscv_subsets(3, 16, 5) == ()


def test_bootstrap_block_starts_are_seeded() -> None:
    """块起点由固定种子决定（G-03）。"""
    full_a, rem_a = bootstrap_block_starts(100, 24, 4, 3)
    full_b, rem_b = bootstrap_block_starts(100, 24, 4, 3)
    assert full_a == full_b and rem_a == rem_b
    assert len(full_a) == 4 and len(full_a[0]) == 4 and rem_a is not None
    with pytest.raises(ValueError):
        bootstrap_block_starts(10, 24, 4, 3)


@pytest.mark.parametrize("device", DEVICES)
def test_resample_matches_cpu_validation(device: str) -> None:
    """折内 OOS Sharpe、bootstrap 与 CSCV 与 CPU validation 在容差内一致。"""
    fixture = build_fixture(bars=1200, seed=23, lookbacks=LOOKBACKS, families=("trend",))
    spec = _spec()
    session = KernelSession(fixture.plan, fixture.panel, device)
    family = session.families[0]
    signals = session.evaluate_chunk(family, 0, len(family.parameter_rows))
    targets = scan_targets(session, signals, PERIODS)
    returns, _turnover, _held = strategy_returns_tensor(session, targets, COST_MODEL)
    metrics = resample_chunk(session, family.family, returns, spec, PERIODS)
    rows = metrics.rows()
    folds = make_folds(fixture.panel.bar_count, dict(spec.fold))
    assert [fold.fold_id for fold in metrics.folds] == [fold.fold_id for fold in folds]
    target_rows = targets.cpu().tolist()
    candidates = build_family_batches(
        synthetic_strategy_config(LOOKBACKS), ("trend",),
    )[0].candidates
    by_id = {candidate.candidate_id: candidate for candidate in candidates}
    validation = walk_forward_validate(
        "resample-test",
        fixture.rounded_bars,
        fixture.rounded_features,
        [by_id[candidate_id] for candidate_id in family.candidate_ids],
        RESEARCH_CONFIG,
    )
    cpu_fold_sharpe: dict[tuple[str, str], float] = {}
    for trial in validation.trials:
        if trial.segment == "testing":
            cpu_fold_sharpe[(trial.candidate.candidate_id, trial.fold_id)] = (
                trial.metrics.sharpe
            )
    returns_rows = returns.to(torch.float64).cpu().tolist()
    oos_mask = [False] * fixture.panel.bar_count
    for fold in folds:
        for index in range(fold.test_start, fold.test_end):
            oos_mask[index] = True
    fold_scores: dict[str, tuple[float, ...]] = {}
    for offset, row in enumerate(rows):
        candidate_id = family.candidate_ids[offset]
        fold_test = list(row["fold_test_sharpe"])
        for fold, got in zip(folds, fold_test, strict=True):
            want = cpu_fold_sharpe[(candidate_id, fold.fold_id)]
            assert abs(got - want) <= 1e-3, (candidate_id, fold.fold_id, got, want)
            direct = evaluate_targets(
                fixture.rounded_bars, target_rows[offset], fold.test_start,
                fold.test_end, COST_RATE, 0.0, GAP, PERIODS,
            ).sharpe
            assert abs(got - direct) <= 1e-3
        fold_scores[candidate_id] = tuple(fold_test)
        series = tuple(
            returns_rows[offset][index]
            for index in range(1, fixture.panel.bar_count) if oos_mask[index]
        )
        assert len(series) == row["oos_bars"]
        lower, p_value, paths = _studentized_circular_block_bootstrap_sharpe(
            series,
            spec.bootstrap_block,
            spec.bootstrap_paths,
            spec.bootstrap_one_sided_alpha,
            family_bootstrap_seed(family.family, spec.bootstrap_seed),
            PERIODS,
        )
        assert paths == row["bootstrap_paths"]
        assert abs(float(str(row["bootstrap_sharpe_lower_bound"])) - lower) <= 1e-3
        assert abs(float(str(row["bootstrap_p_value"])) - p_value) <= 2e-3
    pbo, median_rank, split_count = _probability_backtest_overfitting(
        fold_scores, spec.cscv_split_budget,
        family_cscv_seed(family.family, spec.cscv_seed),
    )
    assert split_count == metrics.cscv_split_count
    assert abs(pbo - metrics.pbo) <= 1e-6
    assert abs(median_rank - metrics.cscv_median_rank) <= 1e-6
    for row in rows:
        assert 0.0 <= float(str(row["cscv_median_oos_rank"])) <= 1.0


def test_resample_screen_thresholds() -> None:
    """重采样粗筛阈值来自配置并逐项判定。"""
    screen = resample_screen_from_config({
        "minimum_oos_sharpe": 0.1,
        "maximum_bootstrap_p": 0.2,
    })
    assert screen.minimum_oos_sharpe == 0.1
    row = {
        "oos_sharpe": 0.5,
        "positive_fold_ratio": 0.6,
        "bootstrap_p_value": 0.1,
        "family_pbo": 0.3,
    }
    assert screen.passes(row)
    assert not ResampleScreen(maximum_pbo=0.2).passes(row)
    assert not ResampleScreen(minimum_oos_sharpe=1.0).passes(row)
