"""CPU 精确复算参考与 validation.py 同口径，以及容差比较（纯 CPU）。"""
from __future__ import annotations

import math
from dataclasses import replace

import pytest

from guvolu.research.validation import evaluate_targets, strategy_returns
from guvolu.search.parity import (
    PARITY_TOLERANCE_VERSION,
    ParityTolerance,
    compare_parity,
    exact_reference,
    reference_metrics,
    reference_returns,
    tolerance_from_config,
)
from guvolu.search.synthetic import synthetic_panel, synthetic_strategy_config
from guvolu.strategy.baselines import generate_targets
from guvolu.strategy.generation import build_family_batches

LOOKBACKS = (4, 8)
COST_MODEL = {
    "one_way_cost_rate": 0.0007,
    "maximum_gap_seconds": 7200.0,
    "periods_per_year": 8760.0,
}


def _targets():
    """构造合成面板与一个趋势候选的目标序列。"""
    bars, features = synthetic_panel(300, LOOKBACKS, 61)
    candidate = build_family_batches(
        synthetic_strategy_config(LOOKBACKS), ("trend",),
    )[0].candidates[0]
    targets = generate_targets(candidate, bars, features, 8760.0)
    assert any(value > 0 for value in targets)
    return bars, features, candidate, targets


@pytest.mark.parametrize("maximum_gap", [None, 7200.0])
def test_reference_returns_equal_validation_strategy_returns(
    maximum_gap: float | None,
) -> None:
    """参考收益与 validation.strategy_returns 逐元素相同。"""
    bars, _features, _candidate, targets = _targets()
    expected = strategy_returns(bars, targets, 0.0007, maximum_gap)
    got = reference_returns(bars, targets, 0.0007, maximum_gap)
    assert got == expected


@pytest.mark.parametrize("maximum_gap", [None, 7200.0])
def test_reference_metrics_equal_validation_evaluate_targets(
    maximum_gap: float | None,
) -> None:
    """参考指标与 validation.evaluate_targets 全区段指标一致。"""
    bars, _features, _candidate, targets = _targets()
    expected = evaluate_targets(
        bars, targets, 1, len(bars), 0.0007, 0.0, maximum_gap, 8760.0,
    )
    got = reference_metrics(bars, targets, 0.0007, maximum_gap, 8760.0)
    assert got.bars == expected.bars
    for name in (
        "net_return", "annual_return", "annual_volatility", "sharpe",
        "maximum_drawdown", "turnover", "annual_turnover", "hit_rate",
        "exposure", "cost",
    ):
        assert math.isclose(
            getattr(got, name), getattr(expected, name), rel_tol=1e-12, abs_tol=1e-12,
        ), name


def test_exact_reference_uses_generate_targets() -> None:
    """精确复算的目标序列即 generate_targets 输出。"""
    bars, features, candidate, targets = _targets()
    reference_targets, metrics = exact_reference(candidate, bars, features, COST_MODEL)
    assert reference_targets == targets
    assert metrics.bars == len(bars) - 1


def test_compare_parity_applies_configured_tolerance() -> None:
    """容差为配置：越界任一项即不通过，且记录最大绝对差。"""
    bars, features, candidate, _unused = _targets()
    reference_targets, metrics = exact_reference(candidate, bars, features, COST_MODEL)
    fast_metrics = metrics.payload()
    passed = compare_parity(
        reference_targets, fast_metrics, reference_targets, metrics, ParityTolerance(),
    )
    assert passed.passed and passed.target_max_abs_diff == 0.0
    assert passed.payload()["tolerance"]["tolerance_version"] == PARITY_TOLERANCE_VERSION
    drifted = list(reference_targets)
    drifted[10] += 2e-5
    failed = compare_parity(
        drifted, fast_metrics, reference_targets, metrics, ParityTolerance(),
    )
    assert not failed.passed
    assert math.isclose(failed.target_max_abs_diff, 2e-5)
    loose = compare_parity(
        drifted, fast_metrics, reference_targets, metrics,
        ParityTolerance(target_abs=1e-4),
    )
    assert loose.passed
    worse_sharpe = dict(fast_metrics)
    worse_sharpe["sharpe"] = float(fast_metrics["sharpe"]) + 2e-3
    assert not compare_parity(
        reference_targets, worse_sharpe, reference_targets, metrics, ParityTolerance(),
    ).passed
    worse_turnover = dict(fast_metrics)
    worse_turnover["turnover"] = float(fast_metrics["turnover"]) + 2e-6
    assert not compare_parity(
        reference_targets, worse_turnover, reference_targets, metrics, ParityTolerance(),
    ).passed


def test_tolerance_from_config_validates_values() -> None:
    """容差配置缺省用初值，非法值拒绝。"""
    default = tolerance_from_config(None)
    assert default == ParityTolerance()
    custom = tolerance_from_config({"sharpe_abs": 5e-4})
    assert custom == replace(ParityTolerance(), sharpe_abs=5e-4)
    with pytest.raises(ValueError, match="容差"):
        tolerance_from_config({"target_abs": -1.0})
