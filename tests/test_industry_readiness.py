"""行业级策略准入检查器的失败关闭与人工门禁测试。"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import pytest

from guvolu.research.industry_readiness import (
    collect_industry_readiness_evidence,
    evaluate_industry_strategy_readiness,
    industry_strategy_readiness,
    load_industry_readiness_policy,
)
from guvolu.research.provenance import stable_identifier


def _policy() -> dict[str, object]:
    """构造较小但完整的项目准入政策。"""
    path = Path("config/industry_strategy_readiness.json")
    return dict(load_industry_readiness_policy(path))


def _metrics() -> dict[str, object]:
    """构造完整候选指标。"""
    return {
        "annual_return": 0.2,
        "annual_turnover": 12.0,
        "annual_volatility": 0.2,
        "bars": 20_000,
        "capacity_score": 1.0,
        "cost": 0.1,
        "exposure": 0.4,
        "hit_rate": 0.55,
        "maximum_drawdown": 0.2,
        "net_return": 0.5,
        "p_value": 0.01,
        "sharpe": 1.2,
        "turnover": 100.0,
    }


def _source_artifact(name: str, digest: str) -> dict[str, object]:
    """构造 integrity verifier 已复核的来源制品身份。"""
    return {
        "name": name,
        "kind": name,
        "path": f"reports/run/{name}.jsonl",
        "sha256": digest,
        "artifact_id": f"sha256-{digest}",
        "bytes": 100,
    }


def _scenario(
    kind: str,
    ordinal: int,
    source: dict[str, object],
) -> dict[str, object]:
    """构造一条规范、候选绑定的场景并复算内容身份。"""
    metrics: dict[str, object] = {
        "maximum_drawdown": 0.2,
        "net_return": 0.5 - ordinal * 0.1,
        "sharpe": 1.0,
        "turnover": 10.0,
    }
    scenario: dict[str, object] = {
        "schema_version": 1,
        "scenario_type": kind,
        "scenario_key": f"{kind}-{ordinal}",
        "family": "trend",
        "candidate_id": "candidate-ready",
        "selection_locked": True,
        "walk_forward_oos_only": True,
        "pit_verified": True,
        "registered_at": "2026-08-25T22:30:00+00:00",
        "coverage": {
            "from_time": "2020-01-01T00:00:00+00:00",
            "to_time": "2026-08-25T22:00:00+00:00",
            "available_through": "2026-08-25T22:00:00+00:00",
            "bars": 20_000,
            "folds": 3,
            "coverage_ratio": 1.0,
        },
        "metrics": metrics,
        "source_artifact": dict(source),
    }
    if kind == "tail":
        scenario["method_version"] = "walk-forward-tail-v1"
        scenario["parameters"] = {
            "tail_probability": (0.01, 0.025, 0.05)[ordinal],
            "block_length": 24,
        }
        metrics["expected_shortfall"] = -0.05
    elif kind == "stress":
        scenario["method_version"] = "walk-forward-stress-v1"
        scenario["parameters"] = {
            "stress_definition": (
                "cross_venue_dislocation", "liquidity_gap", "volatility_spike",
            )[ordinal],
            "severity": float(ordinal + 1),
        }
    elif kind == "cost":
        scenario["method_version"] = "fixed-target-cost-sensitivity-v1"
        scenario["parameters"] = {
            "cost_tier": ("policy_baseline", "adverse", "severe")[ordinal],
        }
        scenario["fixed_target"] = True
        components = (
            {"fee": 5.0, "half_spread": 2.0, "slippage": 2.0, "impact": 1.0},
            {"fee": 5.0, "half_spread": 2.0, "slippage": 10.0, "impact": 3.0},
            {"fee": 5.0, "half_spread": 2.0, "slippage": 18.0, "impact": 5.0},
        )[ordinal]
        scenario["cost_components_bps"] = components
        scenario["total_cost_bps"] = sum(components.values())
    elif kind == "capacity":
        scenario["method_version"] = "l2-depth-capacity-v1"
        scenario["parameters"] = {
            "depth_horizon_seconds": 300,
            "depth_quantile": 0.5,
        }
        notional = float((ordinal + 1) * 100_000)
        scenario.update({
            "notional_quote": notional,
            "participation_rate": 0.05,
            "observed_depth_quote": notional / 0.05,
            "impact_bps": float(ordinal + 2),
        })
    scenario["scenario_id"] = stable_identifier(
        "industry-scenario", scenario,
    )
    return scenario


def _ready_evidence() -> dict[str, object]:
    """构造全部技术门禁通过的纯证据。"""
    candidate = {
        "eligible": True,
        "mode": "paper",
        "family": "trend",
        "deployment_candidate_id": "candidate-ready",
        "validation_metrics": _metrics(),
        "deployment_oos_metrics": _metrics(),
        "fdr_q": 0.01,
        "probability_backtest_overfitting": 0.01,
        "block_bootstrap_p_value": 0.01,
        "block_bootstrap_sharpe_lower_bound": 0.5,
        "block_bootstrap_sample_count": 2_048,
        "deflated_sharpe_probability_effective": 0.99,
        "parameter_neighbor_count": 3,
        "positive_parameter_neighbor_ratio": 0.9,
        "median_parameter_neighbor_sharpe_retention": 0.9,
    }
    summary = {
        "decision_grade": True,
        "pipeline_method_version": "strategy-research-pipeline-v14",
        "trial_count": 10,
        "deflated_sharpe_method_version": "dsr-v1",
        "pbo_method_version": "pbo-v1",
        "block_bootstrap_method_version": "bootstrap-v1",
        "parameter_stability_method_version": "neighbor-v1",
        "family_evaluations": [candidate],
        "ablations": {"fixed_long": {"sharpe": 0.5}},
    }
    tail_source = _source_artifact("tail_risk_evidence", "b" * 64)
    stress_source = _source_artifact("stress_scenario_evidence", "c" * 64)
    replay_source = _source_artifact("fixed_target_cost_replay", "d" * 64)
    capacity_source = _source_artifact(
        "l2_depth_capacity_evidence", "e" * 64,
    )
    candidate_evidence = {
        "family": "trend",
        "candidate_id": "candidate-ready",
        "tail_scenarios": [
            _scenario("tail", index, tail_source) for index in range(3)
        ],
        "stress_scenarios": [
            _scenario("stress", index, stress_source) for index in range(3)
        ],
        "cost_scenarios": [
            _scenario("cost", index, replay_source) for index in range(3)
        ],
        "capacity_scenarios": [
            _scenario("capacity", index, capacity_source) for index in range(3)
        ],
    }
    industry_payload = {
        "schema_version": 1,
        "method_version": "industry-evidence-v2",
        "run_id": "research-run-" + "1" * 64,
        "research_identity": "research-identity-" + "2" * 64,
        "config_hash": "e" * 64,
        "input_receipt_sha256": "f" * 64,
        "decision_time": "2026-08-25T23:00:00+00:00",
        "generated_at": "2026-08-25T23:45:00+00:00",
        "candidate_evidence": [candidate_evidence],
    }
    generator_attestation = {
        "schema_version": 1,
        "method_version": "industry-evidence-generator-attestation-v1",
        "generator_id": "guvolu-independent-industry-evidence-generator-v1",
        "generator_code_sha256": (
            "a0f851208fa6627d61e2dbaca4d727dd1d5fd288dd73075b9a14563338126a90"
        ),
        "independent_from_strategy_search": True,
        "numeric_replay_verified": True,
        "pit_replay_verified": True,
        "run_id": "research-run-" + "1" * 64,
        "research_identity": "research-identity-" + "2" * 64,
        "config_hash": "e" * 64,
        "input_receipt_sha256": "f" * 64,
        "decision_time": "2026-08-25T23:00:00+00:00",
        "generated_at": "2026-08-25T23:45:00+00:00",
        "attested_at": "2026-08-25T23:50:00+00:00",
        "industry_evidence_sha256": "a" * 64,
        "source_artifact_ids": sorted([
            str(tail_source["artifact_id"]),
            str(stress_source["artifact_id"]),
            str(replay_source["artifact_id"]),
            str(capacity_source["artifact_id"]),
        ]),
    }
    generator_attestation["attestation_id"] = stable_identifier(
        "industry-generator-attestation", generator_attestation,
    )
    execution_policy = _policy()["execution"]
    assert isinstance(execution_policy, dict)
    controls = {
        str(name): True
        for name in execution_policy["required_controls"]
    }
    permissions = {
        str(name): True
        for name in execution_policy["required_permissions"]
    }
    return {
        "research": {
            "verified": True,
            "semantic_verified": True,
            "run_id": "research-run-" + "1" * 64,
            "manifest_sha256": "a" * 64,
            "manifest": {
                "code_identity": {"decision_grade": True, "dirty": False},
                "run_id": "research-run-" + "1" * 64,
                "research_identity": "research-identity-" + "2" * 64,
                "config_hash": "e" * 64,
                "input_receipt_sha256": "f" * 64,
                "decision_time": "2026-08-25T23:00:00+00:00",
                "execution_evaluated_at": "2026-08-26T00:00:00+00:00",
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
                    "path": "reports/run/industry-evidence.json",
                    "sha256": "a" * 64,
                    "artifact_id": "sha256-" + "a" * 64,
                    "bytes": 100,
                },
                "payload": industry_payload,
                "generator_attestation_artifact": {
                    "name": "industry_evidence_generator_attestation",
                    "kind": "industry_evidence_generator_attestation",
                    "path": "reports/run/industry-generator-attestation.json",
                    "sha256": "9" * 64,
                    "artifact_id": "sha256-" + "9" * 64,
                    "bytes": 100,
                },
                "generator_attestation_payload": generator_attestation,
                "verified_artifacts": {
                    "tail_risk_evidence": {
                        **tail_source, "snapshot_verified": True,
                    },
                    "stress_scenario_evidence": {
                        **stress_source, "snapshot_verified": True,
                    },
                    "fixed_target_cost_replay": {
                        **replay_source, "snapshot_verified": True,
                    },
                    "l2_depth_capacity_evidence": {
                        **capacity_source, "snapshot_verified": True,
                    },
                },
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
        "forward": {
            "registry_present": True,
            "vintages": [{
                "vintage_id": "vintage-ready",
                "status": "consumed",
                "terminal_verdict": "passed",
                "plan_id": "plan-ready",
                "plan_artifact_state": "verified",
                "decision_grid_valid": True,
                "prediction_coverage_ratio": 1.0,
                "duplicate_prediction_times": 0,
                "prediction_artifact_failures": 0,
            }],
        },
        "paper": {
            "execution_root_provided": True,
            "task_log_present": True,
            "ledger_present": True,
            "reconciliation_present": True,
            "paper_duration_hours": 800,
            "paper_decisions": 600,
            "ledger_rows": 600,
            "reconciled_decisions": 600,
            "paper_error_ratio": 0.0,
            "zero_real_writes_proven": True,
        },
        "execution": {
            "attestation_present": True,
            "attestation": {
                "mode": "paper",
                "live_enabled": False,
                "write_touched": [],
                "controls": controls,
                "permissions": permissions,
                "test_run": {"passed": True},
            },
        },
    }


def _gate(result: dict[str, object], gate_id: str) -> dict[str, object]:
    """按身份读取一个门禁。"""
    gates = result["gates"]
    assert isinstance(gates, list)
    return next(item for item in gates if item["gate_id"] == gate_id)


def _industry_candidate(evidence: dict[str, object]) -> dict[str, object]:
    """读取测试证据中的首个候选行业场景集合。"""
    research = evidence["research"]
    assert isinstance(research, dict)
    industry = research["industry_evidence"]
    assert isinstance(industry, dict)
    payload = industry["payload"]
    assert isinstance(payload, dict)
    candidates = payload["candidate_evidence"]
    assert isinstance(candidates, list)
    candidate = candidates[0]
    assert isinstance(candidate, dict)
    return candidate


def _reidentify(scenario: dict[str, object]) -> None:
    """场景变异后重算规范身份，以隔离被测门禁。"""
    scenario.pop("scenario_id", None)
    scenario["scenario_id"] = stable_identifier(
        "industry-scenario", scenario,
    )


def _reidentify_generator_attestation(evidence: dict[str, object]) -> None:
    """变异 generator attestation 后重算其内容身份。"""
    research = cast(dict[str, object], evidence["research"])
    industry = cast(dict[str, object], research["industry_evidence"])
    attestation = cast(
        dict[str, object], industry["generator_attestation_payload"],
    )
    attestation.pop("attestation_id", None)
    attestation["attestation_id"] = stable_identifier(
        "industry-generator-attestation", attestation,
    )


def _public_result(
    monkeypatch: pytest.MonkeyPatch,
    evidence: dict[str, object],
) -> dict[str, object]:
    """经正式 policy loader 与公共入口评估一份隔离证据。"""
    import guvolu.research.industry_readiness as readiness

    monkeypatch.setattr(
        readiness,
        "collect_industry_readiness_evidence",
        lambda *_args, **_kwargs: evidence,
    )
    return dict(industry_strategy_readiness(
        Path.cwd(),
        Path("config/industry_strategy_readiness.json"),
        reference_time=datetime(2026, 8, 26, tzinfo=UTC),
    ))


def _integrity_fixture_manifest(
    root: Path,
    *,
    industry_path: str = "industry.json",
    industry_hash: str | None = None,
) -> Path:
    """写仅供 artifact-integrity loader 测试的最小 manifest。"""
    summary_body = b"{}\n"
    industry_body = b"{}\n"
    (root / "summary.json").write_bytes(summary_body)
    (root / "industry.json").write_bytes(industry_body)
    summary_hash = hashlib.sha256(summary_body).hexdigest()
    actual_industry_hash = hashlib.sha256(industry_body).hexdigest()
    manifest = {
        "artifacts": {
            "summary_json": {
                "kind": "summary_json",
                "path": "summary.json",
                "sha256": summary_hash,
                "bytes": len(summary_body),
            },
            "industry_evidence": {
                "kind": "industry_evidence",
                "path": industry_path,
                "sha256": industry_hash or actual_industry_hash,
                "bytes": len(industry_body),
            },
        },
    }
    path = root / "manifest.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    return path


def test_self_reported_magic_generator_identity_cannot_ready() -> None:
    """自报正确 magic 三元组也不能替代真实 generator bundle 与重放。"""
    result = dict(evaluate_industry_strategy_readiness(
        _policy(),
        _ready_evidence(),
        evaluated_at=datetime(2026, 8, 26, tzinfo=UTC),
    ))
    assert result["verdict"] == "NOT_READY"
    assert result["blocking_reason_codes"] == [
        "INDUSTRY_EVIDENCE_GENERATOR_NOT_IMPLEMENTED",
        "INDUSTRY_EVIDENCE_SOURCE_REPLAY_NOT_IMPLEMENTED",
    ]
    assert result["live_authorized"] is False
    assert result["automated_promotion_performed"] is False
    assert result["writes_performed"] == []
    external = _gate(result, "external_live_approval")
    assert external["passed"] is False
    assert external["blocking"] is False


def test_summary_inline_scenarios_cannot_replace_verified_artifact() -> None:
    """summary 内任意数组不得绕过独立受保护制品。"""
    evidence = _ready_evidence()
    research = evidence["research"]
    assert isinstance(research, dict)
    summary = research["summary"]
    assert isinstance(summary, dict)
    summary["industry_evidence"] = {
        "tail_scenarios": [{}, {}, {}],
        "stress_scenarios": [{}, {}, {}],
        "cost_scenarios": [{}, {}, {}],
        "capacity_scenarios": [{}, {}, {}],
    }
    research["industry_evidence"] = {
        "present": False,
        "verified": False,
    }
    result = dict(evaluate_industry_strategy_readiness(
        _policy(), evidence, evaluated_at=datetime(2026, 8, 26, tzinfo=UTC),
    ))
    assert result["verdict"] == "NOT_READY"
    reasons = result["blocking_reason_codes"]
    assert isinstance(reasons, list)
    assert "INDUSTRY_EVIDENCE_ARTIFACT_MISSING" in reasons


@pytest.mark.parametrize("invalid", [{}, 0, None])
def test_empty_or_non_object_tail_scenarios_fail_closed(invalid: object) -> None:
    """空对象或非对象不能再以数组长度冒充场景。"""
    evidence = _ready_evidence()
    candidate = _industry_candidate(evidence)
    candidate["tail_scenarios"] = [invalid, invalid, invalid]
    result = dict(evaluate_industry_strategy_readiness(
        _policy(), evidence, evaluated_at=datetime(2026, 8, 26, tzinfo=UTC),
    ))
    reasons = result["blocking_reason_codes"]
    assert isinstance(reasons, list)
    assert "TAIL_RISK_SCENARIO_SCHEMA_INVALID" in reasons
    assert "TAIL_RISK_SCENARIO_EVIDENCE_INCOMPLETE" in reasons


def test_duplicate_scenarios_count_only_once() -> None:
    """相同内容身份或语义键只能计数一次。"""
    evidence = _ready_evidence()
    candidate = _industry_candidate(evidence)
    tails = candidate["tail_scenarios"]
    assert isinstance(tails, list)
    candidate["tail_scenarios"] = [tails[0], tails[0], tails[0]]
    result = dict(evaluate_industry_strategy_readiness(
        _policy(), evidence, evaluated_at=datetime(2026, 8, 26, tzinfo=UTC),
    ))
    reasons = result["blocking_reason_codes"]
    assert isinstance(reasons, list)
    assert "TAIL_RISK_SCENARIO_DUPLICATE" in reasons
    assert "TAIL_RISK_SCENARIO_EVIDENCE_INCOMPLETE" in reasons


def test_industry_artifact_binding_mismatch_fails() -> None:
    """独立制品必须精确绑定受保护 manifest 身份。"""
    evidence = _ready_evidence()
    research = evidence["research"]
    assert isinstance(research, dict)
    industry = research["industry_evidence"]
    assert isinstance(industry, dict)
    payload = industry["payload"]
    assert isinstance(payload, dict)
    payload["run_id"] = "research-run-" + "9" * 64
    result = dict(evaluate_industry_strategy_readiness(
        _policy(), evidence, evaluated_at=datetime(2026, 8, 26, tzinfo=UTC),
    ))
    reasons = result["blocking_reason_codes"]
    assert isinstance(reasons, list)
    assert "INDUSTRY_EVIDENCE_RESEARCH_BINDING_MISMATCH" in reasons


def test_scenario_method_allowlist_and_canonical_identity_are_mandatory() -> None:
    """未知方法或不可复算 scenario_id 必须失败。"""
    evidence = _ready_evidence()
    candidate = _industry_candidate(evidence)
    tails = candidate["tail_scenarios"]
    assert isinstance(tails, list)
    first = tails[0]
    assert isinstance(first, dict)
    first["method_version"] = "unregistered-tail-method"
    first["scenario_id"] = "industry-scenario-" + "0" * 64
    result = dict(evaluate_industry_strategy_readiness(
        _policy(), evidence, evaluated_at=datetime(2026, 8, 26, tzinfo=UTC),
    ))
    reasons = result["blocking_reason_codes"]
    assert isinstance(reasons, list)
    assert "TAIL_RISK_SCENARIO_METHOD_NOT_ACCEPTED" in reasons
    assert "TAIL_RISK_SCENARIO_IDENTITY_INVALID" in reasons


def test_non_finite_metric_and_source_identity_fail() -> None:
    """非有限指标与未受保护来源身份均不得通过。"""
    evidence = _ready_evidence()
    candidate = _industry_candidate(evidence)
    tails = candidate["tail_scenarios"]
    assert isinstance(tails, list)
    first = tails[0]
    assert isinstance(first, dict)
    metrics = first["metrics"]
    assert isinstance(metrics, dict)
    metrics["sharpe"] = float("inf")
    source = first["source_artifact"]
    assert isinstance(source, dict)
    source["sha256"] = "0" * 64
    result = dict(evaluate_industry_strategy_readiness(
        _policy(), evidence, evaluated_at=datetime(2026, 8, 26, tzinfo=UTC),
    ))
    reasons = result["blocking_reason_codes"]
    assert isinstance(reasons, list)
    assert "TAIL_RISK_SCENARIO_METRICS_INVALID" in reasons
    assert "TAIL_RISK_SCENARIO_SOURCE_ARTIFACT_INVALID" in reasons


def test_cost_grid_requires_fixed_target_monotonic_net_return() -> None:
    """成本上升时固定目标净收益不得改善。"""
    evidence = _ready_evidence()
    candidate = _industry_candidate(evidence)
    scenarios = candidate["cost_scenarios"]
    assert isinstance(scenarios, list)
    highest = scenarios[-1]
    assert isinstance(highest, dict)
    metrics = highest["metrics"]
    assert isinstance(metrics, dict)
    metrics["net_return"] = 0.9
    _reidentify(highest)
    result = dict(evaluate_industry_strategy_readiness(
        _policy(), evidence, evaluated_at=datetime(2026, 8, 26, tzinfo=UTC),
    ))
    reasons = result["blocking_reason_codes"]
    assert isinstance(reasons, list)
    assert "COST_SCENARIO_NET_RETURN_MONOTONICITY_VIOLATION" in reasons


def test_missing_verified_capacity_scenarios_fails_despite_proxy_score() -> None:
    """旧 capacity_score=1 也不能替代深度/参与率场景。"""
    evidence = _ready_evidence()
    candidate = _industry_candidate(evidence)
    candidate["capacity_scenarios"] = []
    result = dict(evaluate_industry_strategy_readiness(
        _policy(), evidence, evaluated_at=datetime(2026, 8, 26, tzinfo=UTC),
    ))
    reasons = result["blocking_reason_codes"]
    assert isinstance(reasons, list)
    assert "CAPACITY_SCENARIO_EVIDENCE_INCOMPLETE" in reasons
    robustness = _gate(result, "robustness_evidence")
    facts = robustness["facts"]
    assert isinstance(facts, dict)
    assert facts["legacy_capacity_proxy_is_admission_evidence"] is False


def test_legacy_capacity_score_is_diagnostic_only() -> None:
    """真实容量场景齐备时，不要求旧成交额代理存在。"""
    evidence = _ready_evidence()
    research = evidence["research"]
    assert isinstance(research, dict)
    summary = research["summary"]
    assert isinstance(summary, dict)
    evaluations = summary["family_evaluations"]
    assert isinstance(evaluations, list)
    candidate = evaluations[0]
    assert isinstance(candidate, dict)
    for field in ("validation_metrics", "deployment_oos_metrics"):
        metrics = candidate[field]
        assert isinstance(metrics, dict)
        metrics.pop("capacity_score")
    result = dict(evaluate_industry_strategy_readiness(
        _policy(), evidence, evaluated_at=datetime(2026, 8, 26, tzinfo=UTC),
    ))
    assert result["verdict"] == "NOT_READY"
    reasons = cast(list[object], result["blocking_reason_codes"])
    assert "CAPACITY_SCENARIO_EVIDENCE_INCOMPLETE" not in reasons
    robustness = _gate(result, "robustness_evidence")
    facts = robustness["facts"]
    assert isinstance(facts, dict)
    assert facts["legacy_candidate_capacity_scores"] == [None]


def test_selection_and_registration_cutoff_are_mandatory() -> None:
    """未锁选择或事后登记的场景必须失败。"""
    evidence = _ready_evidence()
    candidate = _industry_candidate(evidence)
    stresses = candidate["stress_scenarios"]
    assert isinstance(stresses, list)
    first = stresses[0]
    assert isinstance(first, dict)
    first["selection_locked"] = False
    first["registered_at"] = "2026-08-26T00:00:01+00:00"
    _reidentify(first)
    result = dict(evaluate_industry_strategy_readiness(
        _policy(), evidence, evaluated_at=datetime(2026, 8, 26, tzinfo=UTC),
    ))
    reasons = result["blocking_reason_codes"]
    assert isinstance(reasons, list)
    assert "STRESS_SCENARIO_SELECTION_NOT_LOCKED" in reasons
    assert "STRESS_SCENARIO_REGISTRATION_CUTOFF_INVALID" in reasons


def test_public_entry_rejects_catastrophic_scenario_outcomes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """公共入口必须按类型结果政策拒绝经济上不可用的伪场景。"""
    evidence = _ready_evidence()
    candidate = _industry_candidate(evidence)
    for collection in ("tail_scenarios", "stress_scenarios", "cost_scenarios"):
        scenarios = candidate[collection]
        assert isinstance(scenarios, list)
        for raw in scenarios:
            assert isinstance(raw, dict)
            metrics = raw["metrics"]
            assert isinstance(metrics, dict)
            metrics.update({
                "net_return": -999.0,
                "sharpe": -999.0,
                "maximum_drawdown": 1.0,
            })
            _reidentify(raw)
    result = _public_result(monkeypatch, evidence)
    reasons = cast(list[object], result["blocking_reason_codes"])
    assert "TAIL_RISK_SCENARIO_OUTCOME_BELOW_POLICY" in reasons
    assert "STRESS_SCENARIO_OUTCOME_BELOW_POLICY" in reasons
    assert "COST_SCENARIO_OUTCOME_BELOW_POLICY" in reasons
    assert "COST_SCENARIO_BENCHMARK_OUTCOME_BELOW_POLICY" in reasons


def test_public_entry_rejects_semantically_duplicate_tail_scenarios(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """改 scenario_key/id 不能把同一尾部参数伪装成三条证据。"""
    evidence = _ready_evidence()
    candidate = _industry_candidate(evidence)
    tails = candidate["tail_scenarios"]
    assert isinstance(tails, list)
    first = tails[0]
    assert isinstance(first, dict)
    duplicates: list[dict[str, object]] = []
    for ordinal in range(3):
        duplicate = deepcopy(first)
        duplicate["scenario_key"] = f"renamed-tail-{ordinal}"
        _reidentify(duplicate)
        duplicates.append(duplicate)
    candidate["tail_scenarios"] = duplicates
    result = _public_result(monkeypatch, evidence)
    reasons = cast(list[object], result["blocking_reason_codes"])
    assert "TAIL_RISK_SCENARIO_SEMANTIC_DUPLICATE" in reasons
    assert "TAIL_RISK_SCENARIO_EVIDENCE_INCOMPLETE" in reasons


def test_public_entry_uses_decision_time_as_registration_cutoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """decision 后、execution 前登记仍属于事后登记。"""
    evidence = _ready_evidence()
    candidate = _industry_candidate(evidence)
    stresses = candidate["stress_scenarios"]
    assert isinstance(stresses, list)
    first = stresses[0]
    assert isinstance(first, dict)
    first["registered_at"] = "2026-08-25T23:30:00+00:00"
    _reidentify(first)
    result = _public_result(monkeypatch, evidence)
    reasons = cast(list[object], result["blocking_reason_codes"])
    assert "STRESS_SCENARIO_REGISTRATION_CUTOFF_INVALID" in reasons


def test_public_entry_revalidates_all_candidate_metric_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """两套候选向量的 NaN、字符串与域外值都必须失败。"""
    evidence = _ready_evidence()
    research = evidence["research"]
    assert isinstance(research, dict)
    summary = research["summary"]
    assert isinstance(summary, dict)
    candidates = summary["family_evaluations"]
    assert isinstance(candidates, list)
    candidate = candidates[0]
    assert isinstance(candidate, dict)
    validation = candidate["validation_metrics"]
    deployment = candidate["deployment_oos_metrics"]
    assert isinstance(validation, dict)
    assert isinstance(deployment, dict)
    validation["annual_return"] = float("nan")
    validation["annual_turnover"] = "12"
    validation["hit_rate"] = 2.0
    for field in tuple(deployment):
        deployment[field] = float("nan")
    result = _public_result(monkeypatch, evidence)
    reasons = cast(list[object], result["blocking_reason_codes"])
    assert "CANDIDATE_VALIDATION_METRICS_INVALID" in reasons
    assert "CANDIDATE_DEPLOYMENT_METRICS_INVALID" in reasons
    assert "CANDIDATE_DEPLOYMENT_OOS_SHARPE_BELOW_POLICY" in reasons


def test_public_entry_rejects_panel_as_capacity_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """普通 panel 不能冒充 L2 depth/capacity 语义来源。"""
    evidence = _ready_evidence()
    research = evidence["research"]
    assert isinstance(research, dict)
    industry = research["industry_evidence"]
    assert isinstance(industry, dict)
    verified = industry["verified_artifacts"]
    assert isinstance(verified, dict)
    panel = _source_artifact("panel", "8" * 64)
    verified["panel"] = dict(panel)
    candidate = _industry_candidate(evidence)
    capacities = candidate["capacity_scenarios"]
    assert isinstance(capacities, list)
    first = capacities[0]
    assert isinstance(first, dict)
    first["source_artifact"] = dict(panel)
    _reidentify(first)
    result = _public_result(monkeypatch, evidence)
    reasons = cast(list[object], result["blocking_reason_codes"])
    assert "CAPACITY_SCENARIO_SOURCE_ARTIFACT_INVALID" in reasons


def test_public_entry_rejects_missing_research_binding_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """manifest/payload 同时缺身份字段时，None==None 不得通过。"""
    evidence = _ready_evidence()
    research = evidence["research"]
    assert isinstance(research, dict)
    manifest = research["manifest"]
    industry = research["industry_evidence"]
    assert isinstance(manifest, dict)
    assert isinstance(industry, dict)
    payload = industry["payload"]
    assert isinstance(payload, dict)
    for field in (
        "run_id", "research_identity", "config_hash", "input_receipt_sha256",
    ):
        manifest.pop(field)
        payload.pop(field)
    result = _public_result(monkeypatch, evidence)
    reasons = cast(list[object], result["blocking_reason_codes"])
    assert "INDUSTRY_EVIDENCE_RESEARCH_IDENTITY_INVALID" in reasons


@pytest.mark.parametrize(
    ("location", "expected_code"),
    [
        ("top", "TAIL_RISK_SCENARIO_SCHEMA_INVALID"),
        ("parameters", "TAIL_RISK_SCENARIO_PARAMETERS_SCHEMA_INVALID"),
        ("metrics", "TAIL_RISK_SCENARIO_METRICS_SCHEMA_INVALID"),
        ("coverage", "TAIL_RISK_SCENARIO_COVERAGE_SCHEMA_INVALID"),
        ("source", "TAIL_RISK_SCENARIO_SOURCE_ARTIFACT_INVALID"),
    ],
)
def test_direct_evaluator_rejects_unknown_scenario_fields(
    location: str,
    expected_code: str,
) -> None:
    """顶层及所有嵌套对象均为 exact schema，未知字段失败关闭。"""
    evidence = _ready_evidence()
    candidate = _industry_candidate(evidence)
    tails = cast(list[object], candidate["tail_scenarios"])
    scenario = cast(dict[str, object], tails[0])
    target = (
        scenario if location == "top"
        else cast(dict[str, object], scenario[
            "source_artifact" if location == "source" else location
        ])
    )
    target["nonce"] = "cannot-create-evidence-diversity"
    _reidentify(scenario)
    result = dict(evaluate_industry_strategy_readiness(
        _policy(), evidence, evaluated_at=datetime(2026, 8, 26, tzinfo=UTC),
    ))
    reasons = cast(list[object], result["blocking_reason_codes"])
    assert expected_code in reasons


def test_public_entry_rejects_unknown_payload_and_candidate_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """公共入口同样拒绝 evidence 顶层和候选层扩展字段。"""
    evidence = _ready_evidence()
    research = cast(dict[str, object], evidence["research"])
    industry = cast(dict[str, object], research["industry_evidence"])
    payload = cast(dict[str, object], industry["payload"])
    payload["nonce"] = 1
    candidate = _industry_candidate(evidence)
    candidate["unknown_collection"] = []
    result = _public_result(monkeypatch, evidence)
    reasons = cast(list[object], result["blocking_reason_codes"])
    assert "INDUSTRY_EVIDENCE_SCHEMA_INVALID" in reasons
    assert "INDUSTRY_EVIDENCE_CANDIDATE_SCHEMA_INVALID" in reasons


def test_coverage_and_metric_noise_cannot_create_tail_diversity() -> None:
    """语义去重仅使用允许的经济输入，覆盖率/结果噪声不计新场景。"""
    evidence = _ready_evidence()
    candidate = _industry_candidate(evidence)
    tails = cast(list[object], candidate["tail_scenarios"])
    first = cast(dict[str, object], tails[0])
    duplicates: list[dict[str, object]] = []
    for ordinal in range(3):
        duplicate = deepcopy(first)
        duplicate["scenario_key"] = f"noise-{ordinal}"
        coverage = cast(dict[str, object], duplicate["coverage"])
        metrics = cast(dict[str, object], duplicate["metrics"])
        coverage["bars"] = 20_000 + ordinal
        metrics["net_return"] = 0.4 - ordinal * 0.01
        _reidentify(duplicate)
        duplicates.append(duplicate)
    candidate["tail_scenarios"] = duplicates
    result = dict(evaluate_industry_strategy_readiness(
        _policy(), evidence, evaluated_at=datetime(2026, 8, 26, tzinfo=UTC),
    ))
    reasons = cast(list[object], result["blocking_reason_codes"])
    assert "TAIL_RISK_SCENARIO_SEMANTIC_DUPLICATE" in reasons
    assert "TAIL_RISK_SCENARIO_EVIDENCE_INCOMPLETE" in reasons


@pytest.mark.parametrize("component", ["fee", "half_spread", "slippage", "impact"])
def test_all_cost_components_must_be_finite_nonnegative(component: str) -> None:
    """费率本身与其余执行成本一样禁止负值。"""
    evidence = _ready_evidence()
    candidate = _industry_candidate(evidence)
    costs = cast(list[object], candidate["cost_scenarios"])
    scenario = cast(dict[str, object], costs[0])
    components = cast(dict[str, object], scenario["cost_components_bps"])
    components[component] = -1.0
    scenario["total_cost_bps"] = sum(cast(float, value) for value in components.values())
    _reidentify(scenario)
    result = dict(evaluate_industry_strategy_readiness(
        _policy(), evidence, evaluated_at=datetime(2026, 8, 26, tzinfo=UTC),
    ))
    reasons = cast(list[object], result["blocking_reason_codes"])
    assert "COST_SCENARIO_COST_DECOMPOSITION_INVALID" in reasons


def test_cost_grid_requires_positive_policy_baseline_and_strict_tiers() -> None:
    """成本网格必须含精确政策基线，并按政策档位严格递增。"""
    evidence = _ready_evidence()
    candidate = _industry_candidate(evidence)
    costs = cast(list[object], candidate["cost_scenarios"])
    baseline = cast(dict[str, object], costs[0])
    baseline_components = cast(
        dict[str, object], baseline["cost_components_bps"],
    )
    baseline_components["impact"] = 2.0
    baseline["total_cost_bps"] = 11.0
    _reidentify(baseline)
    adverse = cast(dict[str, object], costs[1])
    adverse_components = cast(dict[str, object], adverse["cost_components_bps"])
    adverse_components["slippage"] = 1.0
    adverse["total_cost_bps"] = sum(
        cast(float, value) for value in adverse_components.values()
    )
    _reidentify(adverse)
    result = dict(evaluate_industry_strategy_readiness(
        _policy(), evidence, evaluated_at=datetime(2026, 8, 26, tzinfo=UTC),
    ))
    reasons = cast(list[object], result["blocking_reason_codes"])
    assert "COST_SCENARIO_POLICY_BASELINE_MISSING" in reasons
    assert "COST_SCENARIO_COST_GRID_NOT_STRICTLY_INCREASING" in reasons


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("fdr_q", -0.01),
        ("probability_backtest_overfitting", 1.01),
        ("block_bootstrap_p_value", -0.01),
        ("deflated_sharpe_probability_effective", 1.01),
        ("positive_parameter_neighbor_ratio", -0.01),
        ("median_parameter_neighbor_sharpe_retention", 1.01),
    ],
)
def test_direct_evaluator_rejects_probability_and_ratio_domain(
    field: str,
    value: float,
) -> None:
    """所有统计概率/比率均严格落在闭区间 0..1。"""
    evidence = _ready_evidence()
    research = cast(dict[str, object], evidence["research"])
    summary = cast(dict[str, object], research["summary"])
    candidates = cast(list[object], summary["family_evaluations"])
    candidate = cast(dict[str, object], candidates[0])
    candidate[field] = value
    result = dict(evaluate_industry_strategy_readiness(
        _policy(), evidence, evaluated_at=datetime(2026, 8, 26, tzinfo=UTC),
    ))
    reasons = cast(list[object], result["blocking_reason_codes"])
    assert "STATISTICAL_METRIC_DOMAIN_INVALID" in reasons


def test_public_entry_rejects_probability_domain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """正式 loader 后的公共评估不能让域外 DSR/PBO 值通过。"""
    evidence = _ready_evidence()
    research = cast(dict[str, object], evidence["research"])
    summary = cast(dict[str, object], research["summary"])
    candidates = cast(list[object], summary["family_evaluations"])
    candidate = cast(dict[str, object], candidates[0])
    candidate["probability_backtest_overfitting"] = -0.1
    candidate["deflated_sharpe_probability_effective"] = 1.1
    result = _public_result(monkeypatch, evidence)
    assert "STATISTICAL_METRIC_DOMAIN_INVALID" in cast(
        list[object], result["blocking_reason_codes"],
    )


@pytest.mark.parametrize(
    ("mutation", "expected_code"),
    [
        ("to_after_available", "TAIL_RISK_SCENARIO_PIT_INVALID"),
        ("available_after_registered", "TAIL_RISK_SCENARIO_PIT_INVALID"),
        ("registered_after_decision", "TAIL_RISK_SCENARIO_PIT_INVALID"),
    ],
)
def test_full_pit_time_order_is_mandatory(
    mutation: str,
    expected_code: str,
) -> None:
    """PIT 合同固定 from<to<=available<=registered<=decision。"""
    evidence = _ready_evidence()
    scenario = cast(
        dict[str, object],
        cast(list[object], _industry_candidate(evidence)["tail_scenarios"])[0],
    )
    coverage = cast(dict[str, object], scenario["coverage"])
    if mutation == "to_after_available":
        coverage["available_through"] = "2026-08-25T21:59:59+00:00"
    elif mutation == "available_after_registered":
        coverage["available_through"] = "2026-08-25T22:45:00+00:00"
    else:
        scenario["registered_at"] = "2026-08-25T23:01:00+00:00"
    _reidentify(scenario)
    result = dict(evaluate_industry_strategy_readiness(
        _policy(), evidence, evaluated_at=datetime(2026, 8, 26, tzinfo=UTC),
    ))
    assert expected_code in cast(list[object], result["blocking_reason_codes"])


@pytest.mark.parametrize(
    ("expected_shortfall", "expected_code"),
    [
        (-1.01, "TAIL_RISK_SCENARIO_SCHEMA_INVALID"),
        (0.01, "TAIL_RISK_SCENARIO_SCHEMA_INVALID"),
        (-0.25, "TAIL_RISK_SCENARIO_EXPECTED_SHORTFALL_BELOW_POLICY"),
    ],
)
def test_expected_shortfall_return_contract_and_policy(
    expected_shortfall: float,
    expected_code: str,
) -> None:
    """ES 使用收益单位 [-1,0]，且不得低于项目最低收益阈值。"""
    evidence = _ready_evidence()
    scenario = cast(
        dict[str, object],
        cast(list[object], _industry_candidate(evidence)["tail_scenarios"])[0],
    )
    metrics = cast(dict[str, object], scenario["metrics"])
    metrics["expected_shortfall"] = expected_shortfall
    _reidentify(scenario)
    result = dict(evaluate_industry_strategy_readiness(
        _policy(), evidence, evaluated_at=datetime(2026, 8, 26, tzinfo=UTC),
    ))
    assert expected_code in cast(list[object], result["blocking_reason_codes"])


def test_generator_attestation_is_independent_content_addressed_gate() -> None:
    """来源数字不可直接重放时，独立 generator attestation 仍是硬门。"""
    evidence = _ready_evidence()
    research = cast(dict[str, object], evidence["research"])
    industry = cast(dict[str, object], research["industry_evidence"])
    attestation = cast(
        dict[str, object], industry["generator_attestation_payload"],
    )
    attestation["numeric_replay_verified"] = False
    _reidentify_generator_attestation(evidence)
    result = dict(evaluate_industry_strategy_readiness(
        _policy(), evidence, evaluated_at=datetime(2026, 8, 26, tzinfo=UTC),
    ))
    reasons = cast(list[object], result["blocking_reason_codes"])
    assert "INDUSTRY_EVIDENCE_GENERATOR_ATTESTATION_INCOMPLETE" in reasons


def test_generator_attestation_source_set_is_exact() -> None:
    """attestation 必须覆盖且只覆盖每类场景实际绑定的来源身份。"""
    evidence = _ready_evidence()
    research = cast(dict[str, object], evidence["research"])
    industry = cast(dict[str, object], research["industry_evidence"])
    attestation = cast(
        dict[str, object], industry["generator_attestation_payload"],
    )
    source_ids = cast(list[object], attestation["source_artifact_ids"])
    source_ids.pop()
    _reidentify_generator_attestation(evidence)
    result = dict(evaluate_industry_strategy_readiness(
        _policy(), evidence, evaluated_at=datetime(2026, 8, 26, tzinfo=UTC),
    ))
    assert "INDUSTRY_EVIDENCE_GENERATOR_SOURCE_BINDING_INVALID" in cast(
        list[object], result["blocking_reason_codes"],
    )


def test_generator_attestation_rejects_unknown_fields_publicly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """独立 attestation 本身也是 exact schema，不能附带未审计声明。"""
    evidence = _ready_evidence()
    research = cast(dict[str, object], evidence["research"])
    industry = cast(dict[str, object], research["industry_evidence"])
    attestation = cast(
        dict[str, object], industry["generator_attestation_payload"],
    )
    attestation["unknown_claim"] = True
    _reidentify_generator_attestation(evidence)
    result = _public_result(monkeypatch, evidence)
    assert "INDUSTRY_EVIDENCE_GENERATOR_ATTESTATION_SCHEMA_INVALID" in cast(
        list[object], result["blocking_reason_codes"],
    )


@pytest.mark.parametrize(
    "generated_at",
    ["2026-08-25T22:59:59+00:00", "2026-08-26T00:00:01+00:00"],
)
def test_industry_generation_must_fit_manifest_cutoffs(
    generated_at: str,
) -> None:
    """行业结果只能在 research decision 后、manifest 执行截止前生成。"""
    evidence = _ready_evidence()
    research = cast(dict[str, object], evidence["research"])
    industry = cast(dict[str, object], research["industry_evidence"])
    payload = cast(dict[str, object], industry["payload"])
    payload["generated_at"] = generated_at
    result = dict(evaluate_industry_strategy_readiness(
        _policy(), evidence, evaluated_at=datetime(2026, 8, 26, tzinfo=UTC),
    ))
    assert "INDUSTRY_EVIDENCE_GENERATION_CUTOFF_INVALID" in cast(
        list[object], result["blocking_reason_codes"],
    )


def test_industry_loader_rehashes_bytes_after_integrity_snapshot(
    tmp_path: Path,
) -> None:
    """integrity 返回后替换路径内容，行业 loader 必须在解析前发现。"""
    import guvolu.research.industry_readiness as readiness

    industry_path = tmp_path / "industry.json"
    attestation_path = tmp_path / "attestation.json"
    original = b"{}\n"
    industry_path.write_bytes(original)
    attestation_path.write_bytes(original)
    original_hash = hashlib.sha256(original).hexdigest()
    manifest = {
        "artifacts": {
            "industry_evidence": {
                "kind": "industry_evidence",
                "path": "industry.json",
                "sha256": original_hash,
                "bytes": len(original),
            },
            "industry_evidence_generator_attestation": {
                "kind": "industry_evidence_generator_attestation",
                "path": "attestation.json",
                "sha256": original_hash,
                "bytes": len(original),
            },
        },
    }
    industry_path.write_bytes(b'{"replaced":true}\n')
    result = readiness._industry_artifact_evidence(
        manifest,
        {
            "industry_evidence": industry_path,
            "industry_evidence_generator_attestation": attestation_path,
        },
    )
    assert result["verified"] is False
    assert "发生变化" in str(result["error"])


def test_collector_rejects_industry_artifact_hash_mismatch(
    tmp_path: Path,
) -> None:
    """collector 必须复用 integrity verifier 的 SHA 失败结果。"""
    import guvolu.research.industry_readiness as readiness

    manifest = _integrity_fixture_manifest(
        tmp_path,
        industry_hash="0" * 64,
    )
    result = readiness._research_evidence(tmp_path, manifest)
    assert result["verified"] is False
    assert "散列不匹配" in str(result["verification_error"])


def test_collector_rejects_industry_artifact_path_escape(
    tmp_path: Path,
) -> None:
    """collector 不得自行放宽 verifier 的 root containment。"""
    import guvolu.research.industry_readiness as readiness

    manifest = _integrity_fixture_manifest(
        tmp_path,
        industry_path="../industry.json",
    )
    result = readiness._research_evidence(tmp_path, manifest)
    assert result["verified"] is False
    assert "越出项目目录" in str(result["verification_error"])


def test_cli_public_entry_requires_full_semantic_research_verification(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """CLI 的 manifest 路径必须先进入完整 verify_research_run。"""
    import guvolu.research.industry_readiness as readiness
    from scripts.check_industry_strategy_readiness import main

    called: list[Path | None] = []

    def reject_semantics(_root: Path, manifest: Path | None = None) -> object:
        called.append(manifest)
        raise ValueError("semantic derivation mismatch")

    monkeypatch.setattr(readiness, "verify_research_run", reject_semantics)
    manifest = tmp_path / "forged-manifest.json"
    policy = (Path.cwd() / "config/industry_strategy_readiness.json").resolve()
    exit_code = main([
        "--root", str(tmp_path),
        "--policy", str(policy),
        "--manifest", str(manifest),
    ])
    report = json.loads(capsys.readouterr().out)
    assert exit_code == 2
    assert called == [manifest]
    assert report["verdict"] == "NOT_READY"
    assert "RESEARCH_SEMANTIC_VERIFICATION_FAILED" in report[
        "blocking_reason_codes"
    ]


def test_cli_rejects_custom_or_weakened_policy(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """CLI 自定义宽松 policy 永远不能产生正式 READY。"""
    from scripts.check_industry_strategy_readiness import main

    raw = json.loads(Path(
        "config/industry_strategy_readiness.json"
    ).read_text(encoding="utf-8"))
    raw["research"]["minimum_tail_scenarios"] = -1
    custom = tmp_path / "custom-policy.json"
    custom.write_text(json.dumps(raw), encoding="utf-8")
    exit_code = main([
        "--root", str(tmp_path),
        "--policy", str(custom),
    ])
    report = json.loads(capsys.readouterr().out)
    assert exit_code == 2
    assert report["verdict"] == "NOT_READY"
    assert report["blocking_reason_codes"] == [
        "READINESS_INPUT_CONTRACT_INVALID"
    ]


def test_evaluator_rehashes_policy_after_load() -> None:
    """保留获批 marker 的内存宽松化也必须被规范内容散列发现。"""
    policy = _policy()
    research = policy["research"]
    assert isinstance(research, dict)
    research["minimum_tail_scenarios"] = 1
    research["minimum_tail_scenario_sharpe"] = -999.0
    research["maximum_tail_scenario_drawdown"] = 1.0
    result = dict(evaluate_industry_strategy_readiness(
        policy,
        _ready_evidence(),
        evaluated_at=datetime(2026, 8, 26, tzinfo=UTC),
    ))
    assert result["verdict"] == "NOT_READY"
    assert result["blocking_reason_codes"] == [
        "ADMISSION_POLICY_DOMAIN_INVALID", "ADMISSION_POLICY_NOT_APPROVED",
    ]


def test_v4_policy_cannot_enable_unimplemented_generator_by_configuration() -> None:
    """v4 合同不含可启用分支，未来实现必须升级代码与政策版本。"""
    policy = _policy()
    research = cast(dict[str, object], policy["research"])
    research["industry_evidence_generator_status"] = "implemented"
    research["scenario_source_replay_status"] = "implemented"
    research["accepted_generator_attestation_method_versions"] = [
        "industry-evidence-generator-attestation-v1",
    ]
    research["allowed_industry_evidence_generators"] = [{
        "generator_id": "self-reported",
        "method_version": "industry-evidence-generator-attestation-v1",
        "generator_code_sha256": "a" * 64,
    }]
    result = dict(evaluate_industry_strategy_readiness(
        policy,
        _ready_evidence(),
        evaluated_at=datetime(2026, 8, 26, tzinfo=UTC),
    ))
    assert result["blocking_reason_codes"] == [
        "ADMISSION_POLICY_DOMAIN_INVALID", "ADMISSION_POLICY_NOT_APPROVED",
    ]


def test_active_holdout_forces_not_ready() -> None:
    """存在未完成活动封存段时必须失败关闭。"""
    evidence = _ready_evidence()
    forward = evidence["forward"]
    assert isinstance(forward, dict)
    vintages = forward["vintages"]
    assert isinstance(vintages, list)
    vintages.append({
        "vintage_id": "vintage-active",
        "status": "sealed",
        "terminal_verdict": None,
        "plan_id": "plan-active",
        "plan_artifact_state": "verified",
        "decision_grid_valid": True,
        "prediction_coverage_ratio": 0.5,
        "duplicate_prediction_times": 0,
        "prediction_artifact_failures": 0,
    })
    result = dict(evaluate_industry_strategy_readiness(
        _policy(), evidence, evaluated_at=datetime(2026, 8, 26, tzinfo=UTC),
    ))
    assert result["verdict"] == "NOT_READY"
    reasons = cast(list[object], result["blocking_reason_codes"])
    assert "ACTIVE_HOLDOUT_NOT_TERMINAL" in reasons
    assert "FORWARD_PREDICTION_COVERAGE_BELOW_POLICY" in (
        reasons
    )


def test_missing_paper_and_execution_evidence_has_precise_reasons() -> None:
    """缺少模拟运行与执行证明时不得以研究指标代替。"""
    evidence = _ready_evidence()
    evidence["paper"] = {"execution_root_provided": False}
    evidence["execution"] = {"attestation_present": False}
    result = dict(evaluate_industry_strategy_readiness(
        _policy(), evidence, evaluated_at=datetime(2026, 8, 26, tzinfo=UTC),
    ))
    reasons = result["blocking_reason_codes"]
    assert isinstance(reasons, list)
    assert "PAPER_EXECUTION_ROOT_NOT_PROVIDED" in reasons
    assert "PAPER_ZERO_REAL_WRITE_NOT_PROVEN" in reasons
    assert "EXECUTION_SAFETY_ATTESTATION_MISSING" in reasons
    assert "INDEPENDENT_KILL_SWITCH_NOT_PROVEN" in reasons


def test_inherited_bitflyer_trade_credentials_force_not_ready() -> None:
    """任一支持 venue 的 TRADE 环境变量都必须失败关闭。"""
    result = dict(evaluate_industry_strategy_readiness(
        _policy(),
        _ready_evidence(),
        evaluated_at=datetime(2026, 8, 26, tzinfo=UTC),
        inherited_trade_environment_names=("BITFLYER_TRADE_API_KEY",),
    ))
    reasons = result["blocking_reason_codes"]
    assert isinstance(reasons, list)
    assert "RESEARCH_PROCESS_INHERITED_TRADE_CREDENTIALS" in reasons
    assert result["verdict"] == "NOT_READY"
    assert result["live_authorized"] is False


def test_governance_collection_opens_registry_without_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """只读治理检查不得改变数据库字节与时间。"""
    database = tmp_path / "governance.sqlite3"
    connection = sqlite3.connect(database)
    connection.executescript(
        """
        CREATE TABLE holdout_vintage(
          vintage_id TEXT, market_id TEXT, start_time TEXT, end_time TEXT,
          status TEXT, consumed_at TEXT, evaluation_id TEXT, verdict TEXT
        );
        CREATE TABLE frozen_forward_plan(
          plan_id TEXT, vintage_id TEXT, plan_artifact_path TEXT,
          plan_artifact_sha256 TEXT, missing_policy TEXT
        );
        CREATE TABLE frozen_forward_prediction(
          plan_id TEXT, decision_time TEXT, prediction_artifact_path TEXT,
          prediction_artifact_sha256 TEXT
        );
        """
    )
    connection.execute(
        "INSERT INTO holdout_vintage VALUES(?,?,?,?,?,?,?,?)",
        (
            "vintage", "market", "2026-01-01T00:00:00+00:00",
            "2026-01-02T00:00:00+00:00", "sealed", None, None, None,
        ),
    )
    connection.commit()
    connection.close()
    before_bytes = database.read_bytes()
    before_time = database.stat().st_mtime_ns

    import guvolu.research.industry_readiness as readiness

    # 仅替代研究制品读取
    monkeypatch.setattr(
        readiness,
        "_research_evidence",
        lambda _root, _manifest: {"summary": {"market_id": "market"}},
    )
    collect_industry_readiness_evidence(
        tmp_path,
        _policy(),
        governance_registry_path=database,
    )
    assert database.read_bytes() == before_bytes
    assert database.stat().st_mtime_ns == before_time
