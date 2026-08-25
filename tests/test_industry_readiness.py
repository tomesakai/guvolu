"""行业级策略准入检查器的失败关闭与人工门禁测试。"""
from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import pytest

from guvolu.research.industry_readiness import (
    collect_industry_readiness_evidence,
    evaluate_industry_strategy_readiness,
    load_industry_readiness_policy,
)


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
        "industry_evidence": {
            "tail_scenarios": [{}, {}, {}],
            "stress_scenarios": [{}, {}, {}],
            "cost_scenarios": [{}, {}, {}],
        },
    }
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
            "run_id": "research-run-ready",
            "manifest_sha256": "a" * 64,
            "manifest": {
                "code_identity": {"decision_grade": True, "dirty": False},
            },
            "summary": summary,
            "research_config": {
                "cost_model": {"capacity_notional_quote": 100_000.0},
            },
            "checked_artifacts": [
                "candidate_registry", "config", "summary_json", "trial_ledger",
            ],
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


def test_ready_result_still_requires_external_human_approval() -> None:
    """技术门禁全过也不得由检查器授权实盘。"""
    result = dict(evaluate_industry_strategy_readiness(
        _policy(),
        _ready_evidence(),
        evaluated_at=datetime(2026, 8, 26, tzinfo=UTC),
    ))
    assert result["verdict"] == "READY_FOR_EXTERNAL_LIVE_APPROVAL"
    assert result["live_authorized"] is False
    assert result["automated_promotion_performed"] is False
    assert result["writes_performed"] == []
    external = _gate(result, "external_live_approval")
    assert external["passed"] is False
    assert external["blocking"] is False


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
