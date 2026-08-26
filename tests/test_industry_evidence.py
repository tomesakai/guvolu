"""行业稳健性证据生成器的合同、失败关闭与检查器接受度测试。"""
from __future__ import annotations

import json
import random
import sqlite3
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import cast

import pytest

from guvolu.research.industry_evidence import (
    INSUFFICIENT_CROSS_VENUE_COVERAGE,
    INSUFFICIENT_L2_COVERAGE,
    SOURCE_ARTIFACT_NAMES,
    CandidatePath,
    RunIdentity,
    SourceArtifact,
    build_capacity_evidence,
    build_cost_evidence,
    build_generator_attestation,
    build_industry_evidence,
    build_stress_evidence,
    build_tail_evidence,
    content_sha256,
    cost_tier_grid,
    depth_observation,
    evidence_bytes,
    generator_code_sha256,
    ledger_bytes,
    ledger_rows,
    notional_key,
    read_candidate_paths,
    sample_decision_times,
    source_artifact_bytes,
)
from guvolu.research.industry_readiness import (
    evaluate_industry_strategy_readiness,
    load_industry_readiness_policy,
)
from guvolu.research.panel_limit import (
    reject_sealed_conflict,
    resolve_panel_to_time,
)

_BARS = 10_080
_DECISION_TIME = datetime(2026, 8, 23, 9, 0, tzinfo=UTC)
_AVAILABLE_THROUGH = datetime(2026, 8, 23, 8, 0, tzinfo=UTC)
_REGISTRATION_CUTOFF = datetime(2026, 8, 23, 10, 0, tzinfo=UTC)
_GENERATED_AT = datetime(2026, 8, 23, 9, 30, tzinfo=UTC)
_ATTESTED_AT = datetime(2026, 8, 23, 9, 45, tzinfo=UTC)
_NOTIONALS = (100_000.0, 200_000.0, 400_000.0)


def _as_float(value: object) -> float:
    """把未收窄的数值读为浮点。"""
    assert isinstance(value, (int, float))
    return float(value)


def _identity() -> RunIdentity:
    """构造合成研究运行身份。"""
    return RunIdentity(
        run_id="research-run-" + "1" * 64,
        research_identity="research-identity-" + "2" * 64,
        config_hash="3" * 64,
        input_receipt_sha256="4" * 64,
        decision_time=_DECISION_TIME,
        execution_evaluated_at=_REGISTRATION_CUTOFF,
        market_id="mkt__gmo__btc__r0",
        panel_sha256="5" * 64,
        panel_available_through=_AVAILABLE_THROUGH,
        periods_per_year=365.0 * 24.0,
        baseline_cost_bps=10.0,
        cost_components_bps={
            "fee": 5.0, "half_spread": 2.0, "slippage": 2.0, "impact": 1.0,
        },
        block_bootstrap_bars=168,
        block_bootstrap_seed=20260814,
    )


def _path(family: str = "trend") -> CandidatePath:
    """构造固定种子、指标全部达阈的合成目标路径。"""
    generator = random.Random(20260826)
    start = _AVAILABLE_THROUGH - timedelta(hours=_BARS)
    decisions = tuple(
        start + timedelta(hours=index) for index in range(_BARS)
    )
    labels = tuple(value + timedelta(hours=1) for value in decisions)
    market = tuple(
        0.0006 + generator.gauss(0.0, 0.0012) for _index in range(_BARS)
    )
    return CandidatePath(
        family=family,
        candidate_id=f"candidate-{family}",
        decision_times=decisions,
        label_times=labels,
        market_returns=market,
        gross_returns=market,
        turnovers=tuple(0.008 for _index in range(_BARS)),
        fold_ids=tuple(
            f"fold-{index // 2520 + 1:03d}" for index in range(_BARS)
        ),
    )


def _volume_scores() -> tuple[float | None, ...]:
    """构造确定性 PIT 成交量分位序列。"""
    generator = random.Random(11)
    return tuple(generator.gauss(0.0, 1.0) for _index in range(_BARS))


def _cross_venue_spreads() -> tuple[float | None, ...]:
    """构造确定性跨所价差序列。"""
    generator = random.Random(13)
    return tuple(abs(generator.gauss(0.0, 1.0)) for _index in range(_BARS))


def _stress_settings() -> Mapping[str, object]:
    """压力构造规则的版本化设置。"""
    return {
        "volatility_lookback_bars": 24,
        "volatility_quantile": 0.9,
        "volume_quantile": 0.1,
        "cross_venue_quantile": 0.9,
        "minimum_subinterval_bars": 100,
    }


def _capacity_settings() -> Mapping[str, object]:
    """容量构造规则的版本化设置。"""
    return {
        "notional_quote_grid": list(_NOTIONALS),
        "depth_quantile": 0.5,
        "depth_horizon_seconds": 3600,
        "depth_sample_count": 24,
        "depth_levels": 200,
        "minimum_depth_samples": 12,
        "execution_market_id": "mkt__gmo__btc__r0",
    }


def _venue_fact(
    market_id: str,
    *,
    sufficient: bool = True,
    samples: int = 24,
    depth: float = 80_000_000.0,
    impact: float = 1.5,
) -> Mapping[str, object]:
    """构造合成活动 head L2 深度事实。"""
    if not sufficient:
        return {
            "market_id": market_id,
            "sufficient": False,
            "insufficient_reason": "no_active_l2_head",
        }
    observations = [
        {
            "as_of": (_AVAILABLE_THROUGH - timedelta(hours=index)).isoformat(),
            "one_sided_depth_quote": depth,
            "impact_bps": impact,
        }
        for index in range(samples)
    ]
    return {
        "market_id": market_id,
        "head_generation": "sha256-" + "6" * 64,
        "sufficient": True,
        "from_time": (
            _AVAILABLE_THROUGH - timedelta(hours=samples)
        ).isoformat(),
        "to_time": _AVAILABLE_THROUGH.isoformat(),
        "available_through": _AVAILABLE_THROUGH.isoformat(),
        "observation_rows": 1_000_000,
        "distinct_days": 2,
        "expected_samples": samples,
        "resolved_samples": samples,
        "coverage_ratio": 1.0,
        "samples": {
            notional_key(value): {"observations": list(observations)}
            for value in _NOTIONALS
        },
    }


def _venue_facts(samples: int = 24) -> tuple[Mapping[str, object], ...]:
    """构造三所 L2 事实。"""
    return (
        _venue_fact("mkt__gmo__btc__r0", samples=samples),
        _venue_fact("mkt__bitbank__btc_jpy__r0"),
        _venue_fact("mkt__bitflyer__btc_jpy__r0"),
    )


def _evidences(
    identity: RunIdentity,
    paths: Sequence[CandidatePath],
    *,
    probabilities: Sequence[float] = (0.01, 0.025, 0.05),
    multipliers: Mapping[str, object] | None = None,
    cross_venue: Sequence[float | None] | None = None,
    venue_facts: Sequence[Mapping[str, object]] | None = None,
) -> Mapping[str, Mapping[str, object]]:
    """构造四类来源制品载荷。"""
    return {
        "cost": build_cost_evidence(
            identity,
            paths,
            multipliers or {
                "policy_baseline": 1.0, "adverse": 1.5, "severe": 2.0,
            },
            5.0,
            _GENERATED_AT,
        ),
        "tail": build_tail_evidence(
            identity, paths, probabilities, 48, _GENERATED_AT,
        ),
        "stress": build_stress_evidence(
            identity,
            paths,
            _volume_scores(),
            _cross_venue_spreads() if cross_venue is None else cross_venue,
            _stress_settings(),
            {"decision_aligned_series_available": cross_venue is None},
            _GENERATED_AT,
        ),
        "capacity": build_capacity_evidence(
            identity,
            paths,
            _venue_facts() if venue_facts is None else venue_facts,
            "mkt__gmo__btc__r0",
            _capacity_settings(),
            _GENERATED_AT,
        ),
    }


def _sources(
    evidences: Mapping[str, Mapping[str, object]],
) -> dict[str, SourceArtifact]:
    """把来源制品转成身份引用。"""
    result: dict[str, SourceArtifact] = {}
    for kind, payload in evidences.items():
        body = source_artifact_bytes(payload)
        name = SOURCE_ARTIFACT_NAMES[kind]
        result[kind] = SourceArtifact(
            name=name,
            kind=name,
            path=f"reports/industry-evidence/{name}.json",
            sha256=content_sha256(body),
            bytes_count=len(body),
        )
    return result


def _candidate_summary(path: CandidatePath) -> Mapping[str, object]:
    """构造 summary 中的合格候选记录。"""
    metrics = {
        "annual_return": 0.2,
        "annual_turnover": 12.0,
        "annual_volatility": 0.2,
        "bars": _BARS,
        "cost": 0.1,
        "exposure": 0.4,
        "hit_rate": 0.55,
        "maximum_drawdown": 0.2,
        "net_return": 0.5,
        "p_value": 0.01,
        "sharpe": 1.2,
        "turnover": 100.0,
    }
    return {
        "eligible": True,
        "mode": "paper",
        "family": path.family,
        "deployment_candidate_id": path.candidate_id,
        "validation_metrics": dict(metrics),
        "deployment_oos_metrics": dict(metrics),
    }


def _checker_evidence(
    identity: RunIdentity,
    paths: Sequence[CandidatePath],
    evidences: Mapping[str, Mapping[str, object]],
    sources: Mapping[str, SourceArtifact],
) -> dict[str, object]:
    """把生成结果装配成检查器可消费的只读证据。"""
    payload = build_industry_evidence(
        identity, paths, evidences, sources, _GENERATED_AT,
    )
    body = evidence_bytes(payload)
    digest = content_sha256(body)
    attestation = build_generator_attestation(
        identity, payload, digest, sources, _GENERATED_AT, _ATTESTED_AT,
    )
    verified = {
        source.name: {**source.reference(), "snapshot_verified": True}
        for source in sources.values()
    }
    summary = {
        "decision_grade": True,
        "pipeline_method_version": "strategy-research-pipeline-v14",
        "trial_count": 10,
        "deflated_sharpe_method_version": "dsr-v1",
        "pbo_method_version": "pbo-v1",
        "block_bootstrap_method_version": "bootstrap-v1",
        "parameter_stability_method_version": "neighbor-v1",
        "family_evaluations": [_candidate_summary(path) for path in paths],
        "ablations": {"fixed_long": {"sharpe": 0.5}},
    }
    return {
        "research": {
            "verified": True,
            "semantic_verified": True,
            "run_id": identity.run_id,
            "manifest_sha256": "a" * 64,
            "manifest": {
                "code_identity": {"decision_grade": True, "dirty": False},
                "run_id": identity.run_id,
                "research_identity": identity.research_identity,
                "config_hash": identity.config_hash,
                "input_receipt_sha256": identity.input_receipt_sha256,
                "decision_time": identity.decision_time.isoformat(),
                "execution_evaluated_at": (
                    identity.execution_evaluated_at.isoformat()
                ),
            },
            "summary": summary,
            "research_config": {
                "cost_model": {"capacity_notional_quote": 100_000.0},
            },
            "checked_artifacts": [
                "candidate_registry", "config", "industry_evidence",
                "industry_evidence_generator_attestation",
                "summary_json", "trial_ledger",
            ],
            "industry_evidence": {
                "present": True,
                "verified": True,
                "artifact": {
                    "name": "industry_evidence",
                    "kind": "industry_evidence",
                    "path": "reports/industry-evidence/industry-evidence.json",
                    "sha256": digest,
                    "artifact_id": f"sha256-{digest}",
                    "bytes": len(body),
                },
                "payload": dict(payload),
                "generator_attestation_artifact": {
                    "name": "industry_evidence_generator_attestation",
                    "kind": "industry_evidence_generator_attestation",
                    "path": "reports/industry-evidence/attestation.json",
                    "sha256": "9" * 64,
                    "artifact_id": "sha256-" + "9" * 64,
                    "bytes": 100,
                },
                "generator_attestation_payload": dict(attestation),
                "verified_artifacts": verified,
            },
            "trial_ledger": {
                "present": True,
                "header": {
                    "record_type": "trial_ledger_header",
                    "candidate_evaluations": 10,
                },
                "trial_rows": 10,
                "evaluation_id_count": 10,
                "unique_evaluation_id_count": 10,
                "missing_registry_candidate_ids": [],
            },
        },
        "forward": {"registry_present": True, "vintages": []},
        "paper": {"execution_root_provided": False},
        "execution": {"attestation_present": False},
    }


def _robustness_reasons(evidence: Mapping[str, object]) -> tuple[str, ...]:
    """运行正式检查器并取稳健性门禁的原因码。"""
    policy = load_industry_readiness_policy(
        Path("config/industry_strategy_readiness.json")
    )
    result = evaluate_industry_strategy_readiness(
        policy, evidence, evaluated_at=datetime(2026, 8, 26, tzinfo=UTC),
    )
    gates = result["gates"]
    assert isinstance(gates, list)
    gate = next(
        item for item in gates if item["gate_id"] == "robustness_evidence"
    )
    codes = gate["reason_codes"]
    assert isinstance(codes, list)
    return tuple(str(code) for code in codes)


def test_cost_grid_is_strictly_increasing_and_decomposed() -> None:
    """成本档位严格递增、步长达标且分量可加。"""
    identity = _identity()
    grid = cost_tier_grid(
        identity,
        {"policy_baseline": 1.0, "adverse": 1.5, "severe": 2.0},
    )
    assert [tier for tier, _total, _parts in grid] == [
        "policy_baseline", "adverse", "severe",
    ]
    totals = [total for _tier, total, _parts in grid]
    assert totals == [10.0, 15.0, 20.0]
    for _tier, total, parts in grid:
        assert abs(sum(parts.values()) - total) < 1e-9
        assert set(parts) == {"fee", "half_spread", "slippage", "impact"}


def test_cost_grid_rejects_non_increasing_tiers() -> None:
    """成本网格不严格递增时必须被拒。"""
    with pytest.raises(ValueError):
        cost_tier_grid(
            _identity(),
            {"policy_baseline": 1.0, "adverse": 1.0, "severe": 2.0},
        )


def test_cost_evidence_rejects_step_below_policy() -> None:
    """相邻成本档差低于配置步长时必须被拒。"""
    identity = _identity()
    with pytest.raises(ValueError):
        build_cost_evidence(
            identity,
            [_path()],
            {"policy_baseline": 1.0, "adverse": 1.2, "severe": 2.0},
            5.0,
            _GENERATED_AT,
        )


def test_cost_evidence_reproduces_baseline_metrics() -> None:
    """基准档必须复算出与成本敏感性一致的净收益。"""
    identity = _identity()
    path = _path()
    payload = build_cost_evidence(
        identity,
        [path],
        {"policy_baseline": 1.0, "adverse": 1.5, "severe": 2.0},
        5.0,
        _GENERATED_AT,
    )
    assert payload["method_version"] == "fixed-target-cost-sensitivity-v1"
    candidates = payload["candidates"]
    assert isinstance(candidates, list)
    tiers = candidates[0]["tiers"]
    assert isinstance(tiers, list)
    baseline = tiers[0]["metrics"]["net_return"]
    expected = sum(path.gross_returns) - sum(path.turnovers) * 10.0 / 10_000.0
    assert abs(float(baseline) - expected) < 1e-9


def test_tail_evidence_covers_required_probability_grid() -> None:
    """尾部制品必须覆盖政策要求的三个概率并给出期望短缺。"""
    payload = build_tail_evidence(
        _identity(), [_path()], (0.01, 0.025, 0.05), 48, _GENERATED_AT,
    )
    assert payload["method_version"] == "walk-forward-tail-v1"
    candidates = payload["candidates"]
    assert isinstance(candidates, list)
    levels = candidates[0]["levels"]
    assert isinstance(levels, list)
    assert [item["tail_probability"] for item in levels] == [
        0.01, 0.025, 0.05,
    ]
    for level in levels:
        metrics = level["metrics"]
        assert -1.0 <= _as_float(metrics["expected_shortfall"]) <= 0.0
        assert 0.0 <= _as_float(metrics["maximum_drawdown"]) <= 1.0
        assert level["block_length"] == 168


def test_tail_evidence_is_reproducible_under_fixed_seed() -> None:
    """固定种子下尾部数值必须逐位可复现（G-03）。"""
    identity = _identity()
    path = _path()
    first = build_tail_evidence(
        identity, [path], (0.01, 0.025, 0.05), 48, _GENERATED_AT,
    )
    second = build_tail_evidence(
        identity, [path], (0.01, 0.025, 0.05), 48, _GENERATED_AT,
    )
    assert source_artifact_bytes(first) == source_artifact_bytes(second)


def test_tail_evidence_rejects_unordered_probability_grid() -> None:
    """概率网格重复或乱序时必须被拒。"""
    identity = _identity()
    with pytest.raises(ValueError):
        build_tail_evidence(
            identity, [_path()], (0.05, 0.01), 48, _GENERATED_AT,
        )
    with pytest.raises(ValueError):
        build_tail_evidence(
            identity, [_path()], (0.01, 0.01), 48, _GENERATED_AT,
        )


def test_stress_evidence_records_replayable_construction_rule() -> None:
    """压力制品必须写入可复算的构造规则与三类定义。"""
    payload = build_stress_evidence(
        _identity(),
        [_path()],
        _volume_scores(),
        _cross_venue_spreads(),
        _stress_settings(),
        {"decision_aligned_series_available": True},
        _GENERATED_AT,
    )
    rule = payload["construction_rule"]
    assert isinstance(rule, dict)
    assert rule["volatility_spike"]["quantile"] == 0.9
    assert rule["liquidity_gap"]["selection"] == "statistic_le_quantile"
    assert rule["cross_venue_dislocation"]["quantile"] == 0.9
    candidates = payload["candidates"]
    assert isinstance(candidates, list)
    definitions = candidates[0]["definitions"]
    assert isinstance(definitions, list)
    assert [item["stress_definition"] for item in definitions] == [
        "cross_venue_dislocation", "liquidity_gap", "volatility_spike",
    ]
    assert all(item["available"] is True for item in definitions)


def test_stress_evidence_marks_missing_cross_venue_series() -> None:
    """缺跨所序列时必须显式标注而不是外推。"""
    payload = build_stress_evidence(
        _identity(),
        [_path()],
        _volume_scores(),
        [None] * _BARS,
        _stress_settings(),
        {"decision_aligned_series_available": False},
        _GENERATED_AT,
    )
    candidates = payload["candidates"]
    assert isinstance(candidates, list)
    definitions = candidates[0]["definitions"]
    assert isinstance(definitions, list)
    dislocation = definitions[0]
    assert dislocation["available"] is False
    assert dislocation["insufficient_reason"] == (
        INSUFFICIENT_CROSS_VENUE_COVERAGE
    )
    assert dislocation["metrics"] is None


def test_stress_evidence_marks_thin_subinterval() -> None:
    """子区间柱数不足时不得给出指标。"""
    settings = dict(_stress_settings())
    settings["minimum_subinterval_bars"] = 10_000
    payload = build_stress_evidence(
        _identity(),
        [_path()],
        _volume_scores(),
        _cross_venue_spreads(),
        settings,
        {"decision_aligned_series_available": True},
        _GENERATED_AT,
    )
    candidates = payload["candidates"]
    assert isinstance(candidates, list)
    definitions = candidates[0]["definitions"]
    assert isinstance(definitions, list)
    assert all(item["available"] is False for item in definitions)
    assert all(
        item["insufficient_reason"] in {
            "insufficient_subinterval_bars", INSUFFICIENT_CROSS_VENUE_COVERAGE,
        }
        for item in definitions
    )


def test_capacity_evidence_uses_l2_depth_facts() -> None:
    """容量制品必须给出参与率与冲击且不越过政策上限。"""
    payload = build_capacity_evidence(
        _identity(),
        [_path()],
        _venue_facts(),
        "mkt__gmo__btc__r0",
        _capacity_settings(),
        _GENERATED_AT,
    )
    assert payload["method_version"] == "l2-depth-capacity-v1"
    candidates = payload["candidates"]
    assert isinstance(candidates, list)
    scenarios = candidates[0]["scenarios"]
    assert isinstance(scenarios, list)
    assert len(scenarios) == 3
    for scenario in scenarios:
        assert scenario["available"] is True
        assert 0.0 < _as_float(scenario["participation_rate"]) <= 0.05
        assert 0.0 <= _as_float(scenario["impact_bps"]) <= 10.0
        assert _as_float(scenario["notional_quote"]) >= 100_000.0


def test_capacity_evidence_marks_insufficient_l2_coverage() -> None:
    """任一来源 L2 覆盖不足时必须标注而不是外推。"""
    facts = (
        _venue_fact("mkt__gmo__btc__r0"),
        _venue_fact("mkt__bitbank__btc_jpy__r0", sufficient=False),
        _venue_fact("mkt__bitflyer__btc_jpy__r0"),
    )
    payload = build_capacity_evidence(
        _identity(),
        [_path()],
        facts,
        "mkt__gmo__btc__r0",
        _capacity_settings(),
        _GENERATED_AT,
    )
    candidates = payload["candidates"]
    assert isinstance(candidates, list)
    scenarios = candidates[0]["scenarios"]
    assert isinstance(scenarios, list)
    assert all(item["available"] is False for item in scenarios)
    assert all(
        item["insufficient_reason"] == INSUFFICIENT_L2_COVERAGE
        for item in scenarios
    )
    assert all(item["metrics"] is None for item in scenarios)


def test_capacity_evidence_marks_thin_depth_samples() -> None:
    """深度采样数低于配置下限时必须标注不足。"""
    payload = build_capacity_evidence(
        _identity(),
        [_path()],
        _venue_facts(samples=4),
        "mkt__gmo__btc__r0",
        _capacity_settings(),
        _GENERATED_AT,
    )
    candidates = payload["candidates"]
    assert isinstance(candidates, list)
    scenarios = candidates[0]["scenarios"]
    assert isinstance(scenarios, list)
    assert all(
        item["insufficient_reason"] == INSUFFICIENT_L2_COVERAGE
        for item in scenarios
    )


def test_depth_observation_reports_walked_impact() -> None:
    """吃单冲击必须由盘口档位复算。"""
    payload = {
        "mid": "100",
        "asks": [
            {"price": "101", "size": "1", "notional": "101"},
            {"price": "110", "size": "1", "notional": "110"},
        ],
        "bids": [{"price": "99", "size": "5", "notional": "495"}],
    }
    observation = depth_observation(payload, Decimal("101"))
    assert observation is not None
    assert abs(_as_float(observation["impact_bps"]) - 100.0) < 1e-6
    assert _as_float(observation["one_sided_depth_quote"]) == 211.0


def test_depth_observation_fails_closed_on_thin_book() -> None:
    """盘口不足以成交名义规模时必须返回空。"""
    payload = {
        "mid": "100",
        "asks": [{"price": "101", "size": "1", "notional": "101"}],
        "bids": [{"price": "99", "size": "1", "notional": "99"}],
    }
    assert depth_observation(payload, Decimal("100000")) is None


def test_industry_evidence_binds_identity_and_sources() -> None:
    """汇总制品必须绑定研究身份并逐场景引用来源制品。"""
    identity = _identity()
    paths = [_path("trend"), _path("price_breakout")]
    evidences = _evidences(identity, paths)
    sources = _sources(evidences)
    payload = build_industry_evidence(
        identity, paths, evidences, sources, _GENERATED_AT,
    )
    assert payload["method_version"] == "industry-evidence-v2"
    assert payload["run_id"] == identity.run_id
    assert payload["config_hash"] == identity.config_hash
    candidate_evidence = payload["candidate_evidence"]
    assert isinstance(candidate_evidence, list)
    assert len(candidate_evidence) == 2
    for candidate in candidate_evidence:
        assert set(candidate) == {
            "candidate_id", "family", "tail_scenarios", "stress_scenarios",
            "cost_scenarios", "capacity_scenarios",
        }
        for collection, expected in (
            ("tail_scenarios", 3), ("stress_scenarios", 3),
            ("cost_scenarios", 3), ("capacity_scenarios", 3),
        ):
            scenarios = candidate[collection]
            assert isinstance(scenarios, list)
            assert len(scenarios) == expected
            for scenario in scenarios:
                reference = scenario["source_artifact"]
                assert reference["artifact_id"] == (
                    "sha256-" + str(reference["sha256"])
                )


def test_generator_attestation_covers_every_source_artifact() -> None:
    """attestation 必须覆盖全部被引用来源制品身份。"""
    identity = _identity()
    paths = [_path()]
    evidences = _evidences(identity, paths)
    sources = _sources(evidences)
    payload = build_industry_evidence(
        identity, paths, evidences, sources, _GENERATED_AT,
    )
    body = evidence_bytes(payload)
    attestation = build_generator_attestation(
        identity, payload, content_sha256(body), sources,
        _GENERATED_AT, _ATTESTED_AT,
    )
    assert attestation["industry_evidence_sha256"] == content_sha256(body)
    assert attestation["generator_code_sha256"] == generator_code_sha256()
    ids = attestation["source_artifact_ids"]
    assert isinstance(ids, list)
    assert ids == sorted(set(ids))
    assert set(ids) == {
        f"sha256-{source.sha256}" for source in sources.values()
    }


def test_ledger_registers_every_generated_scenario() -> None:
    """台账必须逐场景登记身份与指标（G-07）。"""
    identity = _identity()
    paths = [_path()]
    evidences = _evidences(identity, paths)
    payload = build_industry_evidence(
        identity, paths, evidences, _sources(evidences), _GENERATED_AT,
    )
    rows = ledger_rows(identity, payload, _GENERATED_AT)
    assert rows[0]["record_type"] == "industry_evidence_ledger_header"
    scenarios = [
        row for row in rows
        if row["record_type"] == "industry_evidence_scenario"
    ]
    assert len(scenarios) == 12
    body = ledger_bytes(rows)
    assert len(body.decode("utf-8").splitlines()) == len(rows)
    assert json.loads(body.decode("utf-8").splitlines()[1])["scenario_id"]


def test_generated_evidence_clears_scenario_reason_codes() -> None:
    """端到端：生成结果被检查器接受，四类场景原因码全部消除。"""
    identity = _identity()
    paths = [_path("trend"), _path("price_breakout")]
    evidences = _evidences(identity, paths)
    reasons = _robustness_reasons(
        _checker_evidence(identity, paths, evidences, _sources(evidences))
    )
    assert reasons == (
        "INDUSTRY_EVIDENCE_GENERATOR_NOT_IMPLEMENTED",
        "INDUSTRY_EVIDENCE_SOURCE_REPLAY_NOT_IMPLEMENTED",
    )


def test_missing_tail_probability_keeps_grid_reason_code() -> None:
    """概率网格缺项时检查器必须保留网格不完整原因码。"""
    identity = _identity()
    paths = [_path()]
    evidences = _evidences(identity, paths, probabilities=(0.01, 0.05))
    reasons = _robustness_reasons(
        _checker_evidence(identity, paths, evidences, _sources(evidences))
    )
    assert "TAIL_RISK_SCENARIO_PROBABILITY_GRID_INCOMPLETE" in reasons
    assert "TAIL_RISK_SCENARIO_EVIDENCE_INCOMPLETE" in reasons


def test_missing_cross_venue_series_keeps_stress_reason_code() -> None:
    """跨所序列缺失时检查器必须保留压力定义不完整原因码。"""
    identity = _identity()
    paths = [_path()]
    evidences = _evidences(identity, paths, cross_venue=[None] * _BARS)
    reasons = _robustness_reasons(
        _checker_evidence(identity, paths, evidences, _sources(evidences))
    )
    assert "STRESS_SCENARIO_DEFINITION_SET_INCOMPLETE" in reasons
    assert "STRESS_SCENARIO_EVIDENCE_INCOMPLETE" in reasons


def test_insufficient_l2_coverage_keeps_capacity_reason_code() -> None:
    """L2 覆盖不足时检查器必须保留容量网格不足原因码。"""
    identity = _identity()
    paths = [_path()]
    facts = (
        _venue_fact("mkt__gmo__btc__r0"),
        _venue_fact("mkt__bitbank__btc_jpy__r0", sufficient=False),
        _venue_fact("mkt__bitflyer__btc_jpy__r0"),
    )
    evidences = _evidences(identity, paths, venue_facts=facts)
    reasons = _robustness_reasons(
        _checker_evidence(identity, paths, evidences, _sources(evidences))
    )
    assert "CAPACITY_SCENARIO_NOTIONAL_GRID_INSUFFICIENT" in reasons
    assert "CAPACITY_SCENARIO_EVIDENCE_INCOMPLETE" in reasons


def test_candidate_coverage_must_include_every_deployment_candidate() -> None:
    """候选覆盖不全时检查器必须保留覆盖不完整原因码。"""
    identity = _identity()
    paths = [_path("trend"), _path("price_breakout")]
    evidences = _evidences(identity, paths)
    evidence = _checker_evidence(identity, paths, evidences, _sources(evidences))
    research = cast(dict[str, object], evidence["research"])
    industry = cast(dict[str, object], research["industry_evidence"])
    payload = cast(dict[str, object], industry["payload"])
    candidates = cast(list[object], payload["candidate_evidence"])
    payload["candidate_evidence"] = candidates[:1]
    reasons = _robustness_reasons(evidence)
    assert "INDUSTRY_EVIDENCE_CANDIDATE_COVERAGE_INCOMPLETE" in reasons


def test_missing_industry_artifact_keeps_missing_reason_code() -> None:
    """缺少受保护汇总制品时必须保留制品缺失原因码。"""
    identity = _identity()
    paths = [_path()]
    evidences = _evidences(identity, paths)
    evidence = _checker_evidence(identity, paths, evidences, _sources(evidences))
    research = cast(dict[str, object], evidence["research"])
    research["industry_evidence"] = {"present": False, "verified": False}
    reasons = _robustness_reasons(evidence)
    assert "INDUSTRY_EVIDENCE_ARTIFACT_MISSING" in reasons


def test_panel_to_time_limit_is_enforced_on_replay_rows() -> None:
    """样本外区段越过面板截止上限时必须被拒。"""
    summary = {
        "research_identity": "research-identity-" + "2" * 64,
        "family_evaluations": [{
            "eligible": True,
            "mode": "paper",
            "family": "trend",
            "deployment_candidate_id": "candidate-trend",
        }],
    }
    header = {
        "record_type": "label_cost_header",
        "cost_bps": 10.0,
        "research_identity": summary["research_identity"],
        "deployment_candidates": {"trend": "candidate-trend"},
    }
    row = {
        "record_type": "label_cost",
        "in_walk_forward_oos": True,
        "decision_time": "2026-08-23T09:00:00+00:00",
        "label_available_time": "2026-08-23T10:00:00+00:00",
        "walk_forward_fold_id": "fold-001",
        "hard_gap": False,
        "next_market_log_return": 0.001,
        "replays": {
            "deployment": {
                "trend": {"target_at_decision": 1.0, "turnover": 0.0},
            },
        },
    }
    body = (json.dumps(header) + "\n" + json.dumps(row) + "\n").encode("utf-8")
    with pytest.raises(ValueError):
        read_candidate_paths(
            body, summary, datetime(2026, 8, 23, 9, 0, tzinfo=UTC),
        )


def test_sealed_preflight_rejects_overlapping_window(tmp_path: Path) -> None:
    """面板区间触及未消费封存段时必须失败关闭（G-08）。"""
    registry = tmp_path / "governance.sqlite3"
    connection = sqlite3.connect(registry)
    connection.execute(
        "CREATE TABLE holdout_vintage(vintage_id TEXT,market_id TEXT,"
        "start_time TEXT,end_time TEXT,status TEXT)"
    )
    connection.execute(
        "INSERT INTO holdout_vintage VALUES(?,?,?,?,?)",
        (
            "vintage-sealed", "mkt__gmo__btc__r0",
            "2026-08-24T00:00:00+00:00", "2026-09-24T00:00:00+00:00",
            "sealed",
        ),
    )
    connection.commit()
    connection.close()
    from_time = datetime(2026, 1, 1, tzinfo=UTC)
    limit = resolve_panel_to_time(
        {}, datetime(2026, 8, 25, tzinfo=UTC), from_time,
    )
    with pytest.raises(ValueError):
        reject_sealed_conflict(
            registry,
            "mkt__gmo__btc__r0",
            from_time,
            datetime(2026, 8, 25, tzinfo=UTC),
            limit,
        )
    safe = resolve_panel_to_time(
        {}, datetime(2026, 8, 23, 9, 0, tzinfo=UTC), from_time,
    )
    reject_sealed_conflict(
        registry,
        "mkt__gmo__btc__r0",
        from_time,
        datetime(2026, 8, 23, 9, 0, tzinfo=UTC),
        safe,
    )


def test_sample_decision_times_are_deterministic() -> None:
    """采样时点必须按固定步长确定生成。"""
    end = datetime(2026, 8, 22, 4, 0, tzinfo=UTC)
    times = sample_decision_times(end, 3600, 4)
    assert times[-1] == end
    assert len(times) == 4
    assert times == tuple(sorted(times))
    assert (times[1] - times[0]) == timedelta(hours=1)
