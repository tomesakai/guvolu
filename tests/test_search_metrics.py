"""GPU 粗筛指标与 CPU 精确参考的等价测试。"""
from __future__ import annotations

import math

import pytest

from guvolu.search.kernels import KernelSession
from guvolu.search.metrics import METRIC_NAMES, chunk_metrics, strategy_returns_tensor
from guvolu.search.parity import reference_metrics, reference_returns
from guvolu.search.scan import scan_targets
from searchfast_support import build_fixture, torch_devices

torch = pytest.importorskip("torch")
DEVICES = torch_devices()
PERIODS = 8760.0


@pytest.mark.parametrize("device", DEVICES)
@pytest.mark.parametrize("maximum_gap", [None, 7200.0])
def test_chunk_metrics_match_reference_within_tolerance(
    device: str,
    maximum_gap: float | None,
) -> None:
    """收益、Sharpe、换手与回撤与 f64 有序参考在容差内一致。"""
    cost_model = {
        "one_way_cost_rate": 0.0007,
        "maximum_gap_seconds": maximum_gap,
        "periods_per_year": PERIODS,
    }
    fixture = build_fixture(bars=320, seed=67)
    session = KernelSession(fixture.plan, fixture.panel, device)
    compared = 0
    for family in session.families:
        signals = session.evaluate_chunk(family, 0, len(family.parameter_rows))
        targets = scan_targets(session, signals, PERIODS)
        returns, _turnover, _held = strategy_returns_tensor(session, targets, cost_model)
        metrics = chunk_metrics(session, targets, cost_model)
        rows = metrics.rows()
        target_rows = targets.cpu().tolist()
        return_rows = returns.cpu().tolist()
        for row_index, row in enumerate(rows):
            candidate_targets = target_rows[row_index]
            expected_returns = reference_returns(
                fixture.rounded_bars, candidate_targets, 0.0007, maximum_gap,
            )
            for got, want in zip(return_rows[row_index], expected_returns, strict=True):
                assert math.isclose(got, want, rel_tol=1e-4, abs_tol=1e-7)
            expected = reference_metrics(
                fixture.rounded_bars, candidate_targets, 0.0007, maximum_gap, PERIODS,
            )
            assert row["bars"] == expected.bars
            assert abs(row["sharpe"] - expected.sharpe) <= 1e-3
            assert abs(row["turnover"] - expected.turnover) <= 1e-6
            assert abs(row["maximum_drawdown"] - expected.maximum_drawdown) <= 1e-5
            assert abs(row["net_return"] - expected.net_return) <= 1e-5
            assert abs(row["exposure"] - expected.exposure) <= 1e-6
            assert abs(row["hit_rate"] - expected.hit_rate) <= 1e-9
            assert abs(row["cost"] - expected.cost) <= 1e-8
            assert set(row) == set(METRIC_NAMES)
            compared += 1
    assert compared > 0


@pytest.mark.parametrize("device", DEVICES)
def test_cost_model_validation(device: str) -> None:
    """成本模型字段非法必须拒绝。"""
    fixture = build_fixture(bars=16, seed=1, families=("trend",))
    session = KernelSession(fixture.plan, fixture.panel, device)
    family = session.families[0]
    targets = scan_targets(session, session.evaluate_chunk(family, 0, 1), PERIODS)
    with pytest.raises(ValueError, match="one_way_cost_rate"):
        chunk_metrics(session, targets, {"one_way_cost_rate": -1.0, "periods_per_year": 1.0})
    with pytest.raises(ValueError, match="periods_per_year"):
        chunk_metrics(session, targets, {"one_way_cost_rate": 0.0, "periods_per_year": 0.0})
