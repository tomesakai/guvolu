"""策略研究管线的 PIT、成本与门禁测试。"""
from __future__ import annotations

import json
import math
import hashlib
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pytest

from guvolu.research.allocator import allocate
from guvolu.research import provenance
from guvolu.research.contracts import (
    AllocationResult,
    FamilyEvaluation,
    FrozenPanelInputs,
    PanelSnapshot,
    PerformanceMetrics,
    QualityVector,
)
from guvolu.research.features import MarketState, compute_features
from guvolu.research.evolution import monitor_family_run
from guvolu.research.panel import compact_trade_panel, load_panel_bars
from guvolu.research.pipeline import _position_contract_payload
from guvolu.research.provenance import canonical_json, stable_identifier
from guvolu.research.quality import gate_feature_snapshot, panel_quality
from guvolu.research.validation import (
    ValidationResult,
    _circular_block_bootstrap_sharpe,
    _probabilistic_sharpe_p_value,
    _probability_backtest_overfitting,
    strategy_returns,
)
from guvolu.research.verification import verify_research_run
from guvolu.research.tuning import propose_family_evolution
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


def test_compact_panel_enforces_pit_and_integer_projection(tmp_path: Path) -> None:
    """迟到成交不得进入当期柱，整数投影须与 Decimal 一致。"""
    source = tmp_path / "source.parquet"
    db = duckdb.connect()
    try:
        db.execute("""
            CREATE TABLE source(
              observation_id VARCHAR,event_time TIMESTAMPTZ,
              available_time TIMESTAMPTZ,ingest_time TIMESTAMPTZ,
              side VARCHAR,price VARCHAR,size VARCHAR,
              source_artifact_id VARCHAR,source_row_index BIGINT,
              market_id VARCHAR
            )
        """)
        rows = [
            ("a", _time(0, 10), _time(0, 10), _time(0, 11), "buy", "100", "1", "x", 0, "m"),
            ("a", _time(0, 10), _time(0, 10), _time(0, 12), "buy", "100", "1", "y", 1, "m"),
            ("b", _time(0, 20), _time(1, 5), _time(1, 6), "buy", "120", "1", "x", 2, "m"),
            ("c", _time(1, 10), _time(1, 10), _time(1, 11), "sell", "110", "2", "x", 3, "m"),
        ]
        db.executemany("INSERT INTO source VALUES (?,?,?,?,?,?,?,?,?,?)", rows)
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
    odd = _probability_backtest_overfitting(
        {"a": (1.0,) * 5, "b": (-1.0,) * 5}, 10, 7
    )
    assert odd[2] == 10


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


def test_directional_families_share_one_cap() -> None:
    """量价趋势不得绕过趋势与突破共享的方向风险上限。"""
    quality = QualityVector(True, True, True, True, True, True, ())
    evaluations = tuple(FamilyEvaluation(
        family=family,
        mode="paper",
        deployment_candidate=CandidateSpec("candidate-" + family, family, "paper", {}, 1),
        latest_target=1.0,
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


def test_family_monitor_reports_parameter_search_direction(tmp_path: Path) -> None:
    """监视器须从全候选事实给出独立参数轴方向。"""
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
    payload = monitor_family_run(tmp_path, summary, "trend", config)
    directions = {
        item["parameter"]: item["direction"]
        for item in payload["parameter_directions"]
    }
    assert directions["lookback"] == "explore_higher_after_preregistration"
    assert directions["entry_score"] == "fixed"


def test_evolution_proposal_updates_strategy_and_feature_dependencies() -> None:
    """扩展回看轴时必须同步共享特征并遵守候选预算。"""
    config = json.loads(Path("config/strategy_research.json").read_text())
    monitor = {
        "family": "trend",
        "run_id": "run",
        "monitor_method_version": "family-direction-monitor-v1",
        "source": {
            "summary_sha256": "a" * 64,
            "trial_ledger_sha256": "b" * 64,
        },
        "evolution_action": "eligible_axis_refinement",
        "parameter_directions": [{
            "parameter": "lookback",
            "direction": "explore_higher_after_preregistration",
            "association": 0.8,
        }],
    }
    proposal, proposed = propose_family_evolution(config, monitor, "hash")
    assert proposal["status"] == "proposed"
    assert proposed is not None
    assert 264 in proposed["strategies"]["trend"]["lookbacks"]
    assert 264 in proposed["features"]["lookbacks"]
    assert len(build_family_batches(proposed, ("trend",))[0].candidates) == 8
