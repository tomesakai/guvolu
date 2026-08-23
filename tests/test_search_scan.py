"""状态机扫描与 baselines.generate_targets 的逐柱等价测试。"""
from __future__ import annotations

import math

import pytest

from guvolu.search.kernels import KernelSession, candidate_chunks
from guvolu.search.scan import SCAN_METHODS, scan_targets
from guvolu.strategy.baselines import generate_targets
from guvolu.strategy.contracts import CandidateSpec
from guvolu.strategy.generation import build_family_batches
from guvolu.search.synthetic import synthetic_strategy_config
from searchfast_support import build_fixture, torch_devices

torch = pytest.importorskip("torch")
DEVICES = torch_devices()
PERIODS_PER_YEAR = 8760.0


def _candidates(lookbacks: tuple[int, ...]) -> dict[str, CandidateSpec]:
    """按 candidate_id 索引合成配置的全部候选。"""
    batches = build_family_batches(synthetic_strategy_config(lookbacks))
    return {
        candidate.candidate_id: candidate
        for batch in batches
        for candidate in batch.candidates
    }


@pytest.mark.parametrize("device", DEVICES)
@pytest.mark.parametrize("method", SCAN_METHODS)
def test_scan_matches_generate_targets_bar_by_bar(device: str, method: str) -> None:
    """六流派在含缺失、门禁与零波动率的合成面板上逐柱等价。"""
    fixture = build_fixture(bars=160, seed=41)
    candidates = _candidates(fixture.lookbacks)
    session = KernelSession(fixture.plan, fixture.panel, device)
    checked = 0
    nonzero = 0
    for family in session.families:
        count = len(family.parameter_rows)
        signals = session.evaluate_chunk(family, 0, count)
        targets = scan_targets(session, signals, PERIODS_PER_YEAR, method)
        rows = targets.cpu().tolist()
        for row_index, candidate_id in enumerate(family.candidate_ids):
            expected = generate_targets(
                candidates[candidate_id],
                fixture.rounded_bars,
                fixture.rounded_features,
                PERIODS_PER_YEAR,
            )
            assert len(rows[row_index]) == len(expected)
            for got, want in zip(rows[row_index], expected, strict=True):
                assert math.isclose(got, want, rel_tol=1e-6, abs_tol=1e-6), (
                    family.family, candidate_id, got, want,
                )
                checked += 1
                nonzero += want > 0.0
    assert checked > 0 and nonzero > 0


@pytest.mark.parametrize("device", DEVICES)
def test_gate_branches_are_exercised_and_equivalent(device: str) -> None:
    """as_of 晚于决策、非连续、必要字段缺失与零波动率分支均被覆盖。"""
    fixture = build_fixture(bars=400, seed=43, families=("trend", "grid_shadow"))
    late = sum(f.as_of > f.decision_time for f in fixture.features)
    broken = sum(not f.contiguous for f in fixture.features)
    missing_trend = sum(
        f.trend_scores[fixture.lookbacks[0]] is None for f in fixture.features
    )
    zero_volatility = sum(
        (f.volatility[fixture.lookbacks[0]] or 0.0) <= 0.0 for f in fixture.features
    )
    assert late > 0 and broken > 0 and missing_trend > 0 and zero_volatility > 0
    candidates = _candidates(fixture.lookbacks)
    session = KernelSession(fixture.plan, fixture.panel, device)
    for family in session.families:
        signals = session.evaluate_chunk(family, 0, len(family.parameter_rows))
        targets = scan_targets(session, signals, PERIODS_PER_YEAR).cpu().tolist()
        for row_index, candidate_id in enumerate(family.candidate_ids):
            expected = generate_targets(
                candidates[candidate_id],
                fixture.rounded_bars,
                fixture.rounded_features,
                PERIODS_PER_YEAR,
            )
            for got, want in zip(targets[row_index], expected, strict=True):
                assert math.isclose(got, want, rel_tol=1e-6, abs_tol=1e-6)


@pytest.mark.parametrize("device", DEVICES)
def test_parallel_and_sequential_scans_agree_exactly(device: str) -> None:
    """结合律前缀扫描与逐柱顺序扫描逐格相同。"""
    fixture = build_fixture(bars=257, seed=47, families=("flow_trend", "mean_reversion"))
    session = KernelSession(fixture.plan, fixture.panel, device)
    for family in session.families:
        signals = session.evaluate_chunk(family, 0, len(family.parameter_rows))
        parallel = scan_targets(session, signals, PERIODS_PER_YEAR, "parallel")
        sequential = scan_targets(session, signals, PERIODS_PER_YEAR, "sequential")
        assert torch.equal(parallel, sequential)


@pytest.mark.parametrize("device", DEVICES)
def test_scan_chunking_does_not_change_targets(device: str) -> None:
    """候选分块大小不改变目标序列。"""
    fixture = build_fixture(bars=64, seed=53, families=("breakout",))
    session = KernelSession(fixture.plan, fixture.panel, device)
    family = session.families[0]
    count = len(family.parameter_rows)
    whole = scan_targets(
        session, session.evaluate_chunk(family, 0, count), PERIODS_PER_YEAR,
    )
    for start, stop in candidate_chunks(count, 1):
        part = scan_targets(
            session, session.evaluate_chunk(family, start, stop), PERIODS_PER_YEAR,
        )
        assert torch.equal(part, whole[start:stop])


@pytest.mark.parametrize("device", DEVICES)
def test_scan_rejects_unknown_method_and_bad_periods(device: str) -> None:
    """非法扫描方法与非正年化周期必须拒绝。"""
    fixture = build_fixture(bars=16, seed=2, families=("trend",))
    session = KernelSession(fixture.plan, fixture.panel, device)
    family = session.families[0]
    signals = session.evaluate_chunk(family, 0, 1)
    with pytest.raises(ValueError, match="扫描方法"):
        scan_targets(session, signals, PERIODS_PER_YEAR, "other")
    with pytest.raises(ValueError, match="年化周期"):
        scan_targets(session, signals, 0.0)
