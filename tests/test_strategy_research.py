"""策略研究管线的 PIT、成本与门禁测试。"""
from __future__ import annotations

import hashlib
import json
import math
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pytest

import guvolu.research.allocator as allocator_module
from guvolu.research.allocator import _covariance, allocate
from guvolu.research import provenance
from guvolu.research.contracts import (
    AllocationResult,
    FamilyEvaluation,
    FrozenPanelInputs,
    PanelSnapshot,
    PerformanceMetrics,
    QualityVector,
)
from guvolu.research.config_lineage import verify_config_lineage
from guvolu.research.features import MarketState, classify_market_state, compute_features
from guvolu.research.evolution import monitor_family_run
from guvolu.research.panel import compact_trade_panel, load_panel_bars
from guvolu.research.pipeline import _position_contract_payload
from guvolu.research.provenance import canonical_json, stable_identifier
from guvolu.research.quality import gate_feature_snapshot, panel_quality
from guvolu.research.validation import (
    ValidationResult,
    _circular_block_bootstrap_sharpe,
    _deflated_sharpe_probability,
    _effective_trial_count,
    make_folds,
    _parameter_neighbors,
    _probabilistic_sharpe_p_value,
    _probability_backtest_overfitting,
    strategy_returns,
)
from guvolu.research.verification import verify_research_run
from guvolu.research.tuning import (
    propose_family_evolution,
    verify_evolution_config,
)
from guvolu.strategy.baselines import build_candidates, generate_targets
from guvolu.strategy.contracts import CandidateSpec, FeatureRow, ResearchBar
from guvolu.strategy.generation import build_family_batches


def _time(hour: int, minute: int = 0) -> datetime:
    """生成固定 UTC 测试时间。"""
    return datetime(2026, 1, 1, hour, minute, tzinfo=UTC)


def _bar(hour: int, close: float) -> ResearchBar:
    """生成一根小时测试柱。"""
    return ResearchBar(
        open_time=_time(hour),
        decision_time=_time(hour + 1),
        latest_available_time=_time(hour, 59),
        open=close,
        high=close,
        low=close,
        close=close,
        base_volume=1.0,
        quote_volume=close,
        signed_base_volume=1.0,
        trade_count=1,
    )


def _metrics() -> PerformanceMetrics:
    """生成分配器使用的正收益指标。"""
    return PerformanceMetrics(
        bars=10_000,
        net_return=0.2,
        annual_return=0.1,
        annual_volatility=0.2,
        sharpe=0.5,
        maximum_drawdown=0.1,
        turnover=2.0,
        annual_turnover=1.0,
        hit_rate=0.51,
        exposure=0.2,
        cost=0.01,
        p_value=0.02,
        capacity_score=1.0,
    )


@pytest.mark.parametrize("step_bars", [5, 15])
def test_stitched_walk_forward_requires_contiguous_test_windows(
    step_bars: int,
) -> None:
    """共享拼接目标不得静默覆盖重叠窗或遗漏间隔窗。"""
    with pytest.raises(ValueError, match="step_bars 与 test_bars 相等"):
        make_folds(100, {
            "minimum_train_bars": 20,
            "test_bars": 10,
            "step_bars": step_bars,
            "embargo_bars": 2,
        })


def test_compact_panel_enforces_pit_and_integer_projection(tmp_path: Path) -> None:
    """迟到成交不得进入当期柱，整数投影须与 Decimal 一致。"""
    source = tmp_path / "source.parquet"
    db = duckdb.connect()
    try:
        db.execute("""
            CREATE TABLE source(
              observation_id VARCHAR,event_time TIMESTAMPTZ,
              available_time TIMESTAMPTZ,ingest_time TIMESTAMPTZ,
              side VARCHAR,source_side_basis VARCHAR,price VARCHAR,size VARCHAR,
              source_artifact_id VARCHAR,source_row_index BIGINT,
              market_id VARCHAR
            )
        """)
        rows = [
            ("a", _time(0, 10), _time(0, 10), _time(0, 11), "buy", "taker", "100", "1", "x", 0, "m"),
            ("a", _time(0, 10), _time(0, 10), _time(0, 12), "buy", "taker", "100", "1", "y", 1, "m"),
            ("b", _time(0, 20), _time(1, 5), _time(1, 6), "buy", "taker", "120", "1", "x", 2, "m"),
            ("c", _time(1, 10), _time(1, 10), _time(1, 11), "sell", "taker", "110", "2", "x", 3, "m"),
            ("d", _time(1, 20), _time(1, 20), _time(1, 21), "buy", "participant_side_unfiltered", "110", "3", "x", 4, "m"),
        ]
        db.executemany("INSERT INTO source VALUES (?,?,?,?,?,?,?,?,?,?,?)", rows)
        escaped = str(source.resolve()).replace("'", "''")
        db.execute(f"COPY source TO '{escaped}' (FORMAT PARQUET)")
    finally:
        db.close()
    inputs = FrozenPanelInputs(
        market={
            "market_id": "m",
            "mapping_revision": 0,
            "tick_size": "1",
            "size_step": "0.1",
        },
        paths=(source,),
        head_generation="sha256-" + "1" * 64,
        attempt_ids=("attempt",),
        artifact_ids=("artifact",),
        normalization_versions=("v1",),
        maximum_event_time=_time(2),
    )
    panel, _digest = compact_trade_panel(
        inputs,
        tmp_path / "output",
        "1hour",
        _time(0),
        _time(2),
        100_000_000,
    )
    bars = load_panel_bars(panel)
    assert len(bars) == 2
    assert bars[0].close == 100.0
    assert bars[0].trade_count == 1
    assert bars[1].base_volume == 5.0
    assert bars[1].signed_base_volume == -2.0
    check = duckdb.connect()
    try:
        row = check.execute(
            "SELECT close_ticks,base_volume_lots,notional_atoms "
            "FROM read_parquet(?) ORDER BY open_time LIMIT 1",
            (str(panel),),
        ).fetchone()
    finally:
        check.close()
    assert row == (100, 10, 10_000_000_000)


def test_feature_windows_reject_large_gap_and_allow_structural_gap() -> None:
    """结构性空窗上限须显式控制特征有效性。"""
    bars = [_bar(0, 100), _bar(1, 101), _bar(3, 102)]
    strict = compute_features(bars, (2,), 2, 1)
    bounded = compute_features(bars, (2,), 2, 2)
    assert strict[-1].contiguous is False
    assert bounded[-1].contiguous is True
    assert all(row.as_of <= row.decision_time for row in bounded)


def test_market_state_annualization_uses_configured_bar_periods() -> None:
    """市场状态的同一每柱波动率必须按实际节拍年化。"""
    _bar_value, feature = _bar(0, 100), FeatureRow(
        decision_time=_time(1),
        as_of=_time(1),
        return_one=0.0,
        trend_scores={2: 0.0},
        volatility={2: 0.01},
        price_scores={2: 0.0},
        prior_highs={2: 100.0},
        prior_lows={2: 100.0},
        flow_imbalance=0.0,
        volume_score=0.0,
        jump_score=0.0,
        contiguous=True,
    )
    hourly = classify_market_state(feature, 2, 0.0, 365.0 * 24.0)
    four_hour = classify_market_state(feature, 2, 0.0, 365.0 * 6.0)
    assert hourly.volatility == pytest.approx(four_hour.volatility * 2.0)


def test_strategy_return_uses_prior_decision_and_cost() -> None:
    """下一期收益只能使用前一决策目标并扣换手成本。"""
    bars = (_bar(0, 100), _bar(1, 110))
    features = tuple(FeatureRow(
        decision_time=bar.decision_time,
        as_of=bar.latest_available_time,
        return_one=0.0,
        trend_scores={2: 2.0},
        volatility={2: 0.01},
        price_scores={2: 1.0},
        prior_highs={2: 99.0},
        prior_lows={2: 90.0},
        flow_imbalance=1.0,
        volume_score=0.0,
        jump_score=0.0,
        contiguous=True,
    ) for bar in bars)
    candidate = CandidateSpec(
        candidate_id="candidate",
        family="trend",
        mode="paper",
        parameters={
            "lookback": 2,
            "entry_score": 1.0,
            "exit_score": 0.0,
            "annual_volatility_target": 0.4,
            "maximum_target": 1.0,
        },
        complexity=5,
    )
    targets = generate_targets(candidate, bars, features)
    returns = strategy_returns(bars, targets, 0.001)
    assert targets[0] > 0
    assert returns[1] == pytest.approx(
        targets[0] * math.log(1.1) - targets[0] * 0.001,
    )


def test_covariance_rejects_misaligned_oos_series() -> None:
    """组合器不得把未对齐收益静默当成零相关。"""
    with pytest.raises(ValueError, match="长度不一致"):
        _covariance((0.1, 0.2), (0.1,))


def test_nonnormal_sharpe_and_pbo_diagnostics_are_deterministic() -> None:
    """非正态 Sharpe 概率与折块 PBO 必须可复现。"""
    assert _probabilistic_sharpe_p_value((0.01, 0.01, 0.01)) == 0.0
    assert _probabilistic_sharpe_p_value((-0.01, -0.01, -0.01)) == 1.0
    scores = {
        "a": (1.0, 1.0, -1.0, -1.0),
        "b": (-1.0, -1.0, 1.0, 1.0),
    }
    first = _probability_backtest_overfitting(scores, 10, 7)
    second = _probability_backtest_overfitting(scores, 10, 7)
    assert first == second
    assert first == (1.0, 0.5, 3)
    with pytest.raises(ValueError, match="偶数个测试折"):
        _probability_backtest_overfitting(
            {"a": (1.0,) * 5, "b": (-1.0,) * 5}, 10, 7
        )
    tied = _probability_backtest_overfitting(
        {"a": (1.0,) * 4, "b": (1.0,) * 4}, 10, 7
    )
    assert tied == (1.0, 0.5, 3)
    identity_first = _probability_backtest_overfitting({
        "a": (-1.0, -1.0, -1.0, -1.0),
        "b": (-1.0, -1.0, -1.0, 0.0),
    }, 10, 7)
    identity_renamed = _probability_backtest_overfitting({
        "z": (-1.0, -1.0, -1.0, -1.0),
        "b": (-1.0, -1.0, -1.0, 0.0),
    }, 10, 7)
    assert identity_first == identity_renamed


def test_circular_block_bootstrap_sharpe_is_deterministic() -> None:
    """循环折块 Sharpe 下界必须保留依赖结构且可复现。"""
    values = tuple((0.002 if index % 7 else -0.003) for index in range(140))
    first = _circular_block_bootstrap_sharpe(values, 14, 128, 0.05, 19)
    second = _circular_block_bootstrap_sharpe(values, 14, 128, 0.05, 19)
    assert first == second
    assert first[0] > 0.0
    assert 0.0 < first[1] <= 1.0
    assert first[2] == 128
    with pytest.raises(ValueError, match="折块长度"):
        _circular_block_bootstrap_sharpe(values, 141, 10, 0.05, 19)


def test_deflated_sharpe_penalizes_more_trials_and_reports_effective_count() -> None:
    """DSR 须随试验空间扩大而收紧，并显式报告相关性折算。"""
    values = tuple(0.00101 if index % 2 else -0.00099 for index in range(500))
    trial_sharpes = (-0.01, 0.0, 0.01)
    small_probability, small_benchmark = _deflated_sharpe_probability(
        values,
        trial_sharpes,
        3.0,
    )
    large_probability, large_benchmark = _deflated_sharpe_probability(
        values,
        trial_sharpes,
        100.0,
    )
    assert large_benchmark > small_benchmark > 0.0
    assert large_probability < small_probability
    _fractional_probability, fractional_benchmark = _deflated_sharpe_probability(
        values,
        trial_sharpes,
        1.25,
    )
    assert fractional_benchmark >= 0.0
    assert _effective_trial_count({
        "a": (1.0, -1.0, 1.0, -1.0),
        "b": (1.0, 1.0, -1.0, -1.0),
    }) == pytest.approx(2.0)
    assert _effective_trial_count({
        "a": (1.0, -1.0, 1.0, -1.0),
        "b": (1.0, -1.0, 1.0, -1.0),
    }) == pytest.approx(1.0)
    negative_degenerate, _benchmark = _deflated_sharpe_probability(
        (-2.0, -1.0, -1.0),
        (0.0,),
        1.0,
    )
    positive_degenerate, _benchmark = _deflated_sharpe_probability(
        (2.0, 1.0, 1.0),
        (0.0,),
        1.0,
    )
    assert negative_degenerate == 0.0
    assert positive_degenerate == 1.0


def test_parameter_neighbors_select_only_nearest_one_axis_changes() -> None:
    """参数稳定性只比较其他轴固定时最近的上下候选。"""
    selected = CandidateSpec("selected", "trend", "paper", {"x": 2, "y": 10}, 2)
    candidates = (
        selected,
        CandidateSpec("x-1", "trend", "paper", {"x": 1, "y": 10}, 2),
        CandidateSpec("x-3", "trend", "paper", {"x": 3, "y": 10}, 2),
        CandidateSpec("x-4", "trend", "paper", {"x": 4, "y": 10}, 2),
        CandidateSpec("y-20", "trend", "paper", {"x": 2, "y": 20}, 2),
        CandidateSpec("diagonal", "trend", "paper", {"x": 3, "y": 20}, 2),
    )
    assert {
        candidate.candidate_id for candidate in _parameter_neighbors(
            selected,
            candidates,
        )
    } == {"x-1", "x-3", "y-20"}


def test_large_gap_flattens_without_collecting_unobserved_return() -> None:
    """超限断流不得把跨缺口涨跌计入策略收益。"""
    first = _bar(0, 100)
    second = ResearchBar(
        open_time=_time(8),
        decision_time=_time(9),
        latest_available_time=_time(8, 59),
        open=200,
        high=200,
        low=200,
        close=200,
        base_volume=1,
        quote_volume=200,
        signed_base_volume=1,
        trade_count=1,
    )
    returns = strategy_returns(
        (first, second),
        (0.5, 0.0),
        0.001,
        maximum_gap_seconds=4 * 3600,
    )
    assert returns[1] == pytest.approx(-0.001)


def test_quality_failure_forces_zero_allocation(tmp_path: Path) -> None:
    """硬门禁失败后软分配器不得恢复仓位。"""
    bars = tuple(_bar(index, 100 + index) for index in range(3))
    panel_path = tmp_path / "panel.parquet"
    panel_path.write_bytes(b"panel")
    panel = PanelSnapshot(
        market={"market_id": "m"},
        bars=bars,
        head_generation="sha256-" + "1" * 64,
        attempt_ids=("attempt",),
        artifact_ids=("artifact",),
        normalization_versions=("v1",),
        panel_path=panel_path,
        panel_sha256="2" * 64,
        decision_time=bars[-1].decision_time,
        latest_available_time=bars[-1].latest_available_time,
    )
    quality = panel_quality(panel, bars[-1].decision_time, 3600, 3)
    stale = gate_feature_snapshot(
        quality,
        bars[0].decision_time,
        bars[-1].decision_time + timedelta(hours=10),
        3600,
    )
    candidate = CandidateSpec("candidate", "mean_reversion", "paper", {}, 1)
    family = FamilyEvaluation(
        family="mean_reversion",
        mode="paper",
        deployment_candidate=candidate,
        latest_target=0.5,
        deployment_oos_metrics=_metrics(),
        deployment_oos_returns=(0.01, 0.02),
        metrics=_metrics(),
        adjusted_sharpe=0.5,
        fdr_q=0.1,
        eligible=True,
        rejection_reasons=(),
        oos_returns=(0.01, 0.02),
    )
    state = MarketState(0.0, 0.2, 0.0, 0.0, None, None, None, 0.0, "range", 0.0)
    config = json.loads(Path("config/strategy_research.json").read_text())["allocation"]
    result = allocate((family,), state, stale, config)
    assert result.weights["mean_reversion"] == 0
    assert result.reserve == 1


def test_candidate_registry_and_identifiers_are_deterministic() -> None:
    """同配置必须展开同候选身份和确定性 JSON。"""
    config = json.loads(Path("config/strategy_research.json").read_text())
    first = build_candidates(config)
    second = build_candidates(config)
    assert first == second
    assert len(first) == 34
    value = {"b": 2, "a": 1}
    assert canonical_json(value) == '{"a":1,"b":2}'
    assert stable_identifier("x", value) == stable_identifier("x", value)


def test_family_generators_are_independently_scoped() -> None:
    """单流派生成不得携带其他流派候选。"""
    config = json.loads(Path("config/strategy_research.json").read_text())
    batches = build_family_batches(config, ("trend",))
    assert len(batches) == 1
    assert batches[0].family == "trend"
    assert len(batches[0].candidates) == 6
    assert all(item.family == "trend" for item in batches[0].candidates)
    with pytest.raises(ValueError, match="未知策略家族"):
        build_family_batches(config, ("unknown",))


def test_true_quality_respects_family_caps() -> None:
    """状态分配不得突破均值回归和总风险上限。"""
    quality = QualityVector(True, True, True, True, True, True, ())
    candidate = CandidateSpec("candidate", "mean_reversion", "paper", {}, 1)
    family = FamilyEvaluation(
        family="mean_reversion",
        mode="paper",
        deployment_candidate=candidate,
        latest_target=0.5,
        deployment_oos_metrics=_metrics(),
        deployment_oos_returns=(0.01, 0.02, 0.01),
        metrics=_metrics(),
        adjusted_sharpe=0.5,
        fdr_q=0.1,
        eligible=True,
        rejection_reasons=(),
        oos_returns=(0.01, 0.02, 0.01),
    )
    state = MarketState(0.0, 0.2, 0.0, 0.0, None, None, None, 0.0, "range", 0.0)
    config = json.loads(Path("config/strategy_research.json").read_text())["allocation"]
    result = allocate((family,), state, quality, config)
    assert 0 <= result.weights["mean_reversion"] <= 0.25
    assert result.reserve >= 0.15


def test_allocator_uses_fixed_deployment_oos_evidence() -> None:
    """组合器不得用逐折冠军路径替代固定部署候选的收益证据。"""
    quality = QualityVector(True, True, True, True, True, True, ())
    candidate = CandidateSpec("candidate", "trend", "paper", {}, 1)
    family = FamilyEvaluation(
        family="trend",
        mode="paper",
        deployment_candidate=candidate,
        latest_target=1.0,
        deployment_oos_metrics=replace(
            _metrics(),
            annual_return=-0.1,
            annual_volatility=0.0,
        ),
        deployment_oos_returns=(0.0, 0.0, 0.0),
        metrics=replace(_metrics(), annual_return=1.0),
        adjusted_sharpe=0.5,
        fdr_q=0.1,
        eligible=True,
        rejection_reasons=(),
        oos_returns=(0.1, 0.1, 0.1),
    )
    state = MarketState(
        1.0, 0.2, 0.0, 0.0, None, None, None, 0.0, "positive_trend", 0.0,
    )
    config = json.loads(Path("config/strategy_research.json").read_text())["allocation"]
    result = allocate((family,), state, quality, config)
    assert result.weights["trend"] == 0.0


def test_allocator_covariance_uses_deployment_oos_returns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """跨家族协方差不得读取逐折冠军拼接收益。"""
    quality = QualityVector(True, True, True, True, True, True, ())
    deployment_returns = {
        "trend": (0.01, -0.02, 0.03),
        "mean_reversion": (-0.04, 0.05, -0.06),
    }
    stitched_returns = {
        "trend": (0.7, 0.8, 0.9),
        "mean_reversion": (-0.7, -0.8, -0.9),
    }
    evaluations = tuple(FamilyEvaluation(
        family=family,
        mode="paper",
        deployment_candidate=CandidateSpec(
            "candidate-" + family, family, "paper", {}, 1,
        ),
        latest_target=1.0,
        deployment_oos_metrics=_metrics(),
        deployment_oos_returns=deployment_returns[family],
        metrics=_metrics(),
        adjusted_sharpe=0.5,
        fdr_q=0.1,
        eligible=True,
        rejection_reasons=(),
        oos_returns=stitched_returns[family],
    ) for family in deployment_returns)
    observed: list[tuple[tuple[float, ...], tuple[float, ...]]] = []

    def record_covariance(
        left: tuple[float, ...],
        right: tuple[float, ...],
    ) -> float:
        observed.append((left, right))
        return 0.0

    monkeypatch.setattr(allocator_module, "_covariance", record_covariance)
    state = MarketState(
        1.0, 0.2, 0.0, 0.0, None, None, None, 0.0, "mixed", 0.0,
    )
    config = json.loads(Path("config/strategy_research.json").read_text())["allocation"]
    allocate(evaluations, state, quality, config)

    expected = {
        (left, right)
        for left in deployment_returns.values()
        for right in deployment_returns.values()
    }
    assert len(observed) == 4
    assert set(observed) == expected
    assert not any(
        left in stitched_returns.values() or right in stitched_returns.values()
        for left, right in observed
    )


def test_directional_families_share_one_cap() -> None:
    """量价趋势不得绕过趋势与突破共享的方向风险上限。"""
    quality = QualityVector(True, True, True, True, True, True, ())
    evaluations = tuple(FamilyEvaluation(
        family=family,
        mode="paper",
        deployment_candidate=CandidateSpec("candidate-" + family, family, "paper", {}, 1),
        latest_target=1.0,
        deployment_oos_metrics=_metrics(),
        deployment_oos_returns=(0.01, 0.02, 0.01),
        metrics=_metrics(),
        adjusted_sharpe=0.5,
        fdr_q=0.1,
        eligible=True,
        rejection_reasons=(),
        oos_returns=(0.01, 0.02, 0.01),
    ) for family in ("trend", "flow_trend", "breakout"))
    state = MarketState(1.0, 0.2, 0.0, 0.5, None, None, None, 0.0, "positive_trend", 0.0)
    config = json.loads(Path("config/strategy_research.json").read_text())["allocation"]
    result = allocate(evaluations, state, quality, config)
    assert sum(result.weights.values()) <= 0.6 + 1e-12


def test_dirty_git_identity_is_not_decision_grade(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """有 HEAD 的脏工作树也不得获得决策级身份。"""
    monkeypatch.setattr(provenance, "hash_paths", lambda *_args: "tree")

    def fake_git(_root: Path, arguments: tuple[str, ...]) -> str | None:
        return " M src/example.py" if arguments[0] == "status" else "abc123"

    monkeypatch.setattr(provenance, "_git_output", fake_git)
    identity = provenance.code_identity(tmp_path, ())
    assert identity.git_hash == "abc123"
    assert identity.dirty
    assert not identity.decision_grade
    assert identity.reason == "repository_dirty"


def test_position_contract_combines_family_direction_and_risk_weight() -> None:
    """发布目标必须是分配权重与家族方向目标的乘积。"""
    family = FamilyEvaluation(
        family="trend",
        mode="paper",
        deployment_candidate=CandidateSpec("candidate", "trend", "paper", {}, 1),
        latest_target=-0.5,
        deployment_oos_metrics=_metrics(),
        deployment_oos_returns=(0.01,),
        metrics=_metrics(),
        adjusted_sharpe=0.5,
        fdr_q=0.1,
        eligible=True,
        rejection_reasons=(),
        oos_returns=(0.01,),
    )
    validation = ValidationResult((family,), (), {}, ())
    allocation = AllocationResult({"trend": 0.2}, 0.8, 0.1, "mixed", 1)
    payload = _position_contract_payload(validation, allocation)
    assert payload["aggregate_target"] == pytest.approx(-0.1)
    rows = payload["families"]
    assert isinstance(rows, list)
    assert rows[0]["portfolio_target_contribution"] == pytest.approx(-0.1)


def test_manifest_verifier_checks_hashes_and_flat_gate(tmp_path: Path) -> None:
    """复核器须按 manifest 对象形状检查散列与质量清仓不变量。"""
    report = tmp_path / "reports" / "strategy-research" / "run"
    report.mkdir(parents=True)
    summary = report / "summary.json"
    summary.write_text(json.dumps({
        "run_id": "run",
        "decision_grade": False,
        "operational_quality": {"eligible": False},
        "operational_position": {"weights": {"trend": 0.0}},
        "operational_target_contract": {
            "aggregate_target": 0.0,
            "families": [{
                "family": "trend",
                "family_target": 1.0,
                "allocation_weight": 0.0,
                "portfolio_target_contribution": 0.0,
            }],
        },
    }), encoding="utf-8")
    summary_bytes = summary.read_bytes()
    manifest = report / "manifest.json"
    manifest.write_text(json.dumps({
        "run_id": "run",
        "artifacts": {
            "summary_json": {
                "path": summary.relative_to(tmp_path).as_posix(),
                "sha256": hashlib.sha256(summary_bytes).hexdigest(),
                "bytes": len(summary_bytes),
            },
        },
    }), encoding="utf-8")
    result = verify_research_run(tmp_path, manifest)
    assert result.run_id == "run"
    summary.write_text("{}", encoding="utf-8")
    with pytest.raises(ValueError, match="散列不匹配"):
        verify_research_run(tmp_path, manifest)


def test_family_monitor_reports_parameter_search_direction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """监视器须从全候选事实给出独立参数轴方向。"""
    monkeypatch.setattr(
        "guvolu.research.evolution._verified_summary_source",
        lambda _root, path: (
            json.loads(path.read_text(encoding="utf-8")), "m" * 64,
        ),
    )
    ledger = tmp_path / "ledger.jsonl"
    records = []
    for lookback, sharpe in ((24, 0.1), (72, 0.6)):
        records.append(json.dumps({
            "record_type": "trial",
            "family": "trend",
            "fold_id": "walk-forward",
            "segment": "testing_aggregate",
            "parameters": {"lookback": lookback, "entry_score": 0.5},
            "metrics": {"sharpe": sharpe},
        }))
    ledger.write_text("\n".join(records) + "\n", encoding="utf-8")
    summary = tmp_path / "summary.json"
    summary.write_text(json.dumps({
        "run_id": "run",
        "research_identity": "research-one",
        "decision_time": "2026-01-01T00:00:00+00:00",
        "market_id": "market-one",
        "config_hash": "c" * 64,
        "panel": {"sha256": "p" * 64},
        "code_identity": {"tree_digest": "t" * 64},
        "family_evaluations": [{
            "family": "trend",
            "eligible": True,
            "rejection_reasons": [],
            "adjusted_sharpe": 0.5,
            "fdr_q": 0.1,
            "latest_unallocated_target": 1.0,
        }],
        "artifacts": {
            "trial_ledger": {
                "path": ledger.relative_to(tmp_path).as_posix(),
                "sha256": hashlib.sha256(ledger.read_bytes()).hexdigest(),
            },
        },
    }), encoding="utf-8")
    config = json.loads(Path("config/strategy_research.json").read_text())
    payload = monitor_family_run(tmp_path, summary, "trend", config, "c" * 64)
    directions = {
        item["parameter"]: item["direction"]
        for item in payload["parameter_directions"]
    }
    assert directions["lookback"] == "explore_higher_after_preregistration"
    assert directions["entry_score"] == "fixed"
    prior_payload = json.loads(summary.read_text(encoding="utf-8"))
    prior_payload["run_id"] = "prior-instance-one"
    prior_payload["research_identity"] = "same-prior-research"
    prior_payload["decision_time"] = "2025-09-01T00:00:00+00:00"
    first_prior = tmp_path / "prior-one.json"
    first_prior.write_text(json.dumps(prior_payload), encoding="utf-8")
    prior_payload["run_id"] = "prior-instance-two"
    second_prior = tmp_path / "prior-two.json"
    second_prior.write_text(json.dumps(prior_payload), encoding="utf-8")
    deduplicated = monitor_family_run(
        tmp_path,
        summary,
        "trend",
        config,
        "c" * 64,
        (first_prior, second_prior),
    )
    assert deduplicated["monitor_method_version"] == "family-direction-monitor-v4"
    assert len(deduplicated["history"]) == 0
    assert {
        item["reason"] for item in deduplicated["excluded_history"]
    } == {"duplicate_data_vintage", "duplicate_research_identity"}
    assert deduplicated["cross_run_direction"] == "insufficient_history"

    recent_prior = json.loads(summary.read_text(encoding="utf-8"))
    recent_prior["run_id"] = "time-separated-one"
    recent_prior["research_identity"] = "time-separated-research-one"
    recent_prior["decision_time"] = "2025-09-01T00:00:00+00:00"
    recent_prior["panel"]["sha256"] = "q" * 64
    recent_prior["family_evaluations"][0]["adjusted_sharpe"] = 0.1
    recent_prior["family_evaluations"][0]["fdr_q"] = 0.2
    recent_path = tmp_path / "time-separated-one.json"
    recent_path.write_text(json.dumps(recent_prior), encoding="utf-8")
    older_prior = json.loads(json.dumps(recent_prior))
    older_prior["run_id"] = "time-separated-two"
    older_prior["research_identity"] = "time-separated-research-two"
    older_prior["decision_time"] = "2025-05-01T00:00:00+00:00"
    older_prior["panel"]["sha256"] = "r" * 64
    older_path = tmp_path / "time-separated-two.json"
    older_path.write_text(json.dumps(older_prior), encoding="utf-8")
    time_separated = monitor_family_run(
        tmp_path,
        summary,
        "trend",
        config,
        "c" * 64,
        (recent_path, older_path),
    )
    assert len(time_separated["history"]) == 2
    assert time_separated["excluded_history"] == []
    assert time_separated["cross_run_direction"] == "improving"

    other_market = json.loads(json.dumps(older_prior))
    other_market["run_id"] = "other-market"
    other_market["research_identity"] = "other-market-research"
    other_market["market_id"] = "market-two"
    other_market["panel"]["sha256"] = "s" * 64
    other_market_path = tmp_path / "other-market.json"
    other_market_path.write_text(json.dumps(other_market), encoding="utf-8")
    cohort_filtered = monitor_family_run(
        tmp_path,
        summary,
        "trend",
        config,
        "c" * 64,
        (other_market_path,),
    )
    assert cohort_filtered["history"] == []
    assert cohort_filtered["excluded_history"][0]["reason"] == "incomparable_cohort"
    assert cohort_filtered["excluded_history"][0]["source_summary_sha256"]
    assert cohort_filtered["excluded_history"][0]["source_manifest_sha256"] == "m" * 64

    ledger.write_text(ledger.read_text(encoding="utf-8") + "{}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="trial ledger 散列"):
        monitor_family_run(tmp_path, summary, "trend", config, "c" * 64)


def test_evolution_proposal_updates_strategy_and_feature_dependencies(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """扩展回看轴时必须同步共享特征并遵守候选预算。"""
    monkeypatch.setattr(
        "guvolu.research.tuning.verify_monitor_sources",
        lambda *_args: None,
    )
    config = json.loads(Path("config/strategy_research.json").read_text())
    config_content = canonical_json(config) + "\n"
    config_path = tmp_path / "strategy_research.json"
    config_path.write_bytes(config_content.encode("utf-8"))
    parent_hash = hashlib.sha256(config_path.read_bytes()).hexdigest()
    monitor = {
        "family": "trend",
        "run_id": "run",
        "monitor_method_version": "family-direction-monitor-v4",
        "source": {
            "summary_sha256": "a" * 64,
            "trial_ledger_sha256": "b" * 64,
            "config_hash": parent_hash,
        },
        "evolution_action": "eligible_axis_refinement",
        "cross_run_direction": "insufficient_history",
        "history_policy": {"comparison_cohort_id": "cohort"},
        "parameter_directions": [{
            "parameter": "lookback",
            "direction": "explore_higher_after_preregistration",
            "association": 0.8,
        }],
    }
    monitor_content = canonical_json(monitor) + "\n"
    monitor_hash = hashlib.sha256(monitor_content.encode("utf-8")).hexdigest()
    monitor_path = tmp_path / f"family-monitor-sha256-{monitor_hash}.json"
    monitor_path.write_bytes(monitor_content.encode("utf-8"))
    proposal, proposed = propose_family_evolution(
        tmp_path, config_path, monitor_path,
    )
    assert proposal["status"] == "proposed"
    assert proposed is not None
    assert 264 in proposed["strategies"]["trend"]["lookbacks"]
    assert 264 in proposed["features"]["lookbacks"]
    assert len(build_family_batches(proposed, ("trend",))[0].candidates) == 8
    assert proposal["source_monitor_sha256"] == hashlib.sha256(
        monitor_path.read_bytes(),
    ).hexdigest()
    assert proposal["source_monitor_path"] == monitor_path.name
    assert proposed["evolution_parent"]["source_monitor_sha256"] == monitor_hash
    assert proposed["evolution_parent"]["parent_config_path"] == config_path.name
    assert proposed["evolution_parent"]["lineage_depth"] == 1
    assert proposed["evolution_parent"]["lineage_root_config_hash"] == parent_hash
    derived_path = tmp_path / "derived.json"
    derived_path.write_bytes((canonical_json(proposed) + "\n").encode("utf-8"))
    assert verify_config_lineage(tmp_path, derived_path) == (parent_hash, 1)
    verify_evolution_config(tmp_path, derived_path, proposed)
    arbitrary_child = json.loads(derived_path.read_text(encoding="utf-8"))
    arbitrary_child["strategies"]["trend"]["maximum_target"] = 0.25
    arbitrary_path = tmp_path / "arbitrary-child.json"
    arbitrary_path.write_bytes(
        (canonical_json(arbitrary_child) + "\n").encode("utf-8"),
    )
    assert verify_config_lineage(tmp_path, arbitrary_path) == (parent_hash, 1)
    with pytest.raises(ValueError, match="允许的单轴变换"):
        verify_evolution_config(tmp_path, arbitrary_path, arbitrary_child)
    type_changed = json.loads(derived_path.read_text(encoding="utf-8"))
    type_changed["strategies"]["trend"]["lookbacks"][0] = 24.0
    type_changed_path = tmp_path / "type-changed-child.json"
    type_changed_path.write_bytes(
        (canonical_json(type_changed) + "\n").encode("utf-8"),
    )
    assert verify_config_lineage(tmp_path, type_changed_path) == (parent_hash, 1)
    with pytest.raises(ValueError, match="允许的单轴变换"):
        verify_evolution_config(tmp_path, type_changed_path, type_changed)
    boolean_changed = json.loads(derived_path.read_text(encoding="utf-8"))
    boolean_changed["schema_version"] = True
    boolean_path = tmp_path / "boolean-child.json"
    boolean_path.write_bytes(
        (canonical_json(boolean_changed) + "\n").encode("utf-8"),
    )
    with pytest.raises(ValueError, match="允许的单轴变换"):
        verify_evolution_config(tmp_path, boolean_path, boolean_changed)
    non_finite_path = tmp_path / "non-finite.json"
    non_finite_path.write_text('{"maximum_target":NaN}', encoding="utf-8")
    with pytest.raises(ValueError, match="非有限数值"):
        verify_config_lineage(tmp_path, non_finite_path)
    forged_lineage = json.loads(derived_path.read_text(encoding="utf-8"))
    forged_lineage["evolution_parent"]["lineage_root_config_hash"] = "f" * 64
    derived_path.write_bytes(
        (canonical_json(forged_lineage) + "\n").encode("utf-8"),
    )
    with pytest.raises(ValueError, match="谱系根散列"):
        verify_config_lineage(tmp_path, derived_path)

    changed_config = json.loads(config_content)
    changed_config["strategies"]["trend"]["lookbacks"] = [24, 72]
    config_path.write_bytes((canonical_json(changed_config) + "\n").encode("utf-8"))
    with pytest.raises(ValueError, match="来源配置"):
        propose_family_evolution(tmp_path, config_path, monitor_path)
    config_path.write_bytes(config_content.encode("utf-8"))

    rejected_monitor = json.loads(monitor_content)
    rejected_monitor["evolution_action"] = "revise_hypothesis_or_cost_model"
    rejected_content = canonical_json(rejected_monitor) + "\n"
    rejected_hash = hashlib.sha256(rejected_content.encode("utf-8")).hexdigest()
    rejected_path = tmp_path / f"family-monitor-sha256-{rejected_hash}.json"
    rejected_path.write_bytes(rejected_content.encode("utf-8"))
    rejected, rejected_config = propose_family_evolution(
        tmp_path, config_path, rejected_path,
    )
    assert rejected_config is None
    assert rejected["status"] == "no_parameter_proposal"
    assert rejected["reason"] == "revise_hypothesis_or_cost_model"
    assert rejected["source_monitor_sha256"] == rejected_hash
    assert rejected["source_summary_sha256"] == "a" * 64
    assert rejected["source_trial_ledger_sha256"] == "b" * 64

    tampered = json.loads(monitor_path.read_text(encoding="utf-8"))
    tampered["parameter_directions"][0]["direction"] = (
        "explore_lower_after_preregistration"
    )
    monitor_path.write_bytes((canonical_json(tampered) + "\n").encode("utf-8"))
    with pytest.raises(ValueError, match="文件名与实际制品散列"):
        propose_family_evolution(tmp_path, config_path, monitor_path)


def test_monitor_source_verification_recomputes_consumed_direction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """自洽命名的伪造方向也必须被来源事实重算拒绝。"""
    summary = tmp_path / "summary.json"
    ledger = tmp_path / "ledger.jsonl"
    summary.write_text("{}", encoding="utf-8")
    ledger.write_text("{}\n", encoding="utf-8")
    monitor = {
        "family": "trend",
        "run_id": "run",
        "research_identity": "research",
        "source": {
            "summary_path": "summary.json",
            "summary_sha256": hashlib.sha256(summary.read_bytes()).hexdigest(),
            "trial_ledger_path": "ledger.jsonl",
            "trial_ledger_sha256": hashlib.sha256(ledger.read_bytes()).hexdigest(),
            "config_hash": "c" * 64,
        },
    }
    protected_summary = {
        "run_id": "run",
        "research_identity": "research",
        "config_hash": "c" * 64,
        "artifacts": {
            "trial_ledger": {
                "path": "ledger.jsonl",
                "sha256": monitor["source"]["trial_ledger_sha256"],
            },
        },
    }
    monkeypatch.setattr(
        "guvolu.research.tuning.verify_research_run", lambda *_args: None,
    )
    monkeypatch.setattr(
        "guvolu.research.tuning.json.loads", lambda *_args, **_kwargs: protected_summary,
    )
    recomputed = {
        **monitor,
        "evolution_action": "eligible_axis_refinement",
        "parameter_directions": [{
            "parameter": "lookback",
            "direction": "explore_higher_after_preregistration",
        }],
    }
    monkeypatch.setattr(
        "guvolu.research.tuning.monitor_family_run",
        lambda *_args: recomputed,
    )
    forged = {
        **monitor,
        "evolution_action": "eligible_axis_refinement",
        "parameter_directions": [{
            "parameter": "lookback",
            "direction": "explore_lower_after_preregistration",
        }],
    }
    from guvolu.research.tuning import verify_monitor_sources

    with pytest.raises(ValueError, match="parameter_directions"):
        verify_monitor_sources(tmp_path, {}, forged, "c" * 64)


def test_config_lineage_rejects_invalid_and_excessive_chains(
    tmp_path: Path,
) -> None:
    """配置父链须验证路径、SHA、深度、多级递归和最大长度。"""
    base = tmp_path / "base.json"
    base.write_bytes(b'{"schema_version":1}\n')
    root_hash = hashlib.sha256(base.read_bytes()).hexdigest()

    def write_child(
        path: Path,
        parent: Path,
        parent_hash: str,
        depth: int,
    ) -> None:
        path.write_bytes((canonical_json({
            "schema_version": 1,
            "evolution_parent": {
                "parent_config_path": parent.relative_to(tmp_path).as_posix(),
                "parent_config_hash": parent_hash,
                "lineage_root_config_hash": root_hash,
                "lineage_depth": depth,
            },
        }) + "\n").encode("utf-8"))

    first = tmp_path / "first.json"
    write_child(first, base, root_hash, 1)
    first_hash = hashlib.sha256(first.read_bytes()).hexdigest()
    second = tmp_path / "second.json"
    write_child(second, first, first_hash, 2)
    assert verify_config_lineage(tmp_path, second) == (root_hash, 2)

    bad_hash = tmp_path / "bad-hash.json"
    write_child(bad_hash, base, "0" * 64, 1)
    with pytest.raises(ValueError, match="父配置实际散列"):
        verify_config_lineage(tmp_path, bad_hash)
    bad_depth = tmp_path / "bad-depth.json"
    write_child(bad_depth, base, root_hash, 2)
    with pytest.raises(ValueError, match="谱系深度"):
        verify_config_lineage(tmp_path, bad_depth)
    outside = tmp_path.parent / "outside-lineage.json"
    outside.write_bytes(b'{"schema_version":1}\n')
    outside_hash = hashlib.sha256(outside.read_bytes()).hexdigest()
    outside_child = tmp_path / "outside-child.json"
    outside_child.write_bytes((canonical_json({
        "evolution_parent": {
            "parent_config_path": "../outside-lineage.json",
            "parent_config_hash": outside_hash,
            "lineage_root_config_hash": outside_hash,
            "lineage_depth": 1,
        },
    }) + "\n").encode("utf-8"))
    with pytest.raises(ValueError, match="父配置路径越出"):
        verify_config_lineage(tmp_path, outside_child)

    parent = base
    parent_hash = root_hash
    for depth in range(1, 33):
        child = tmp_path / f"deep-{depth:02d}.json"
        write_child(child, parent, parent_hash, depth)
        parent = child
        parent_hash = hashlib.sha256(child.read_bytes()).hexdigest()
    with pytest.raises(ValueError, match="最大深度"):
        verify_config_lineage(tmp_path, parent)
