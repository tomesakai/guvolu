"""研究数据暴露与一次性封存段治理测试。"""
from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from guvolu.research.governance import (
    consume_holdout_vintage,
    get_frozen_forward_plan_for_vintage,
    list_frozen_forward_predictions,
    list_holdout_vintages,
    record_holdout_verdict,
    register_frozen_forward_plan,
    register_frozen_forward_prediction,
    register_research_exposure,
    seal_holdout_vintage,
)
from guvolu.research.holdout import run_holdout_validation
from guvolu.research.contracts import CodeIdentity, FrozenPanelInputs
from guvolu.research.provenance import sha256_file
from guvolu.research.verification import VerificationResult
from guvolu.strategy.expression import (
    EXPRESSION_METHOD_VERSION,
    candidate_identity,
    expression_id,
    strategy_expression,
)


def _time(value: str) -> datetime:
    """构造测试 UTC 时间。"""
    return datetime.fromisoformat(value).replace(tzinfo=UTC)


def test_holdout_vintage_is_unexposed_nonoverlapping_and_single_use(
    tmp_path: Path,
) -> None:
    """封存段必须未暴露、不重叠且只能消费一次。"""
    registry = tmp_path / "governance.sqlite3"
    exposure = register_research_exposure(
        registry,
        "research-identity-one",
        "market-one",
        _time("2025-01-01T00:00:00"),
        _time("2025-06-01T00:00:00"),
        recorded_at=_time("2025-06-02T00:00:00"),
    )
    assert exposure.market_id == "market-one"
    with pytest.raises(ValueError, match="已被自适应研究读取"):
        seal_holdout_vintage(
            registry,
            "market-one",
            _time("2025-05-01T00:00:00"),
            _time("2025-07-01T00:00:00"),
            sealed_at=_time("2025-04-01T00:00:00"),
        )

    vintage = seal_holdout_vintage(
        registry,
        "market-one",
        _time("2025-07-01T00:00:00"),
        _time("2025-08-01T00:00:00"),
        sealed_at=_time("2025-06-03T00:00:00"),
    )
    repeated = seal_holdout_vintage(
        registry,
        "market-one",
        _time("2025-07-01T00:00:00"),
        _time("2025-08-01T00:00:00"),
        sealed_at=_time("2025-06-04T00:00:00"),
    )
    assert repeated == vintage
    with pytest.raises(ValueError, match="既有 vintage 重叠"):
        seal_holdout_vintage(
            registry,
            "market-one",
            _time("2025-07-15T00:00:00"),
            _time("2025-08-15T00:00:00"),
            sealed_at=_time("2025-06-01T00:00:00"),
        )
    with pytest.raises(ValueError, match="未消费封存段重叠"):
        register_research_exposure(
            registry,
            "research-identity-two",
            "market-one",
            _time("2025-07-15T00:00:00"),
            _time("2025-07-20T00:00:00"),
        )

    consumed = consume_holdout_vintage(
        registry,
        vintage.vintage_id,
        "candidate-set-hash",
        "evaluation-id",
        consumed_at=_time("2025-08-02T00:00:00"),
    )
    assert consumed.status == "consumed"
    assert consumed.consumed_at == _time("2025-08-02T00:00:00")
    with pytest.raises(ValueError, match="已经消费"):
        consume_holdout_vintage(
            registry,
            vintage.vintage_id,
            "different-candidate-set",
            "different-evaluation",
        )

    decided = record_holdout_verdict(
        registry,
        vintage.vintage_id,
        "failed",
        recorded_at=_time("2025-08-03T00:00:00"),
    )
    assert decided.verdict == "failed"
    assert record_holdout_verdict(
        registry,
        vintage.vintage_id,
        "failed",
    ).verdict == "failed"
    with pytest.raises(ValueError, match="不可改写"):
        record_holdout_verdict(registry, vintage.vintage_id, "passed")
    assert list_holdout_vintages(registry) == (decided,)


def test_consumed_vintage_can_become_adaptive_but_never_holdout_again(
    tmp_path: Path,
) -> None:
    """消费后的数据可进入开发史，但同一段不能再声称为新 holdout。"""
    registry = tmp_path / "governance.sqlite3"
    vintage = seal_holdout_vintage(
        registry,
        "market-one",
        _time("2025-01-01T00:00:00"),
        _time("2025-02-01T00:00:00"),
        sealed_at=_time("2024-12-01T00:00:00"),
    )
    consume_holdout_vintage(
        registry,
        vintage.vintage_id,
        "candidate-set-hash",
        "evaluation-id",
    )
    register_research_exposure(
        registry,
        "research-after-consumption",
        "market-one",
        _time("2025-01-01T00:00:00"),
        _time("2025-02-01T00:00:00"),
    )
    with pytest.raises(ValueError, match="已被自适应研究读取"):
        seal_holdout_vintage(
            registry,
            "market-one",
            _time("2025-01-01T00:00:00"),
            _time("2025-02-01T00:00:00"),
            sealed_at=_time("2024-12-01T00:00:00"),
        )


def test_holdout_cannot_be_selected_retroactively(tmp_path: Path) -> None:
    """区间开始后才登记的所谓 holdout 必须被拒绝。"""
    with pytest.raises(ValueError, match="禁止事后挑选"):
        seal_holdout_vintage(
            tmp_path / "governance.sqlite3",
            "market-one",
            _time("2025-01-01T00:00:00"),
            _time("2025-02-01T00:00:00"),
            sealed_at=_time("2025-01-02T00:00:00"),
        )


def test_frozen_forward_plan_and_predictions_are_precommitted_and_append_only(
    tmp_path: Path,
) -> None:
    """冻结计划必须先于数据，预测必须及时且同一决策不可改写。"""
    registry = tmp_path / "governance.sqlite3"
    vintage = seal_holdout_vintage(
        registry,
        "market-one",
        _time("2027-01-01T00:00:00"),
        _time("2027-02-01T00:00:00"),
        sealed_at=_time("2026-12-01T00:00:00"),
    )
    plan = register_frozen_forward_plan(
        registry,
        vintage.vintage_id,
        "manifest-hash",
        "candidate-set-hash",
        "config-hash",
        "tree-hash",
        "reports/plan.json",
        "plan-artifact-hash",
        frozen_at=_time("2026-12-15T00:00:00"),
    )
    assert get_frozen_forward_plan_for_vintage(registry, vintage.vintage_id) == plan
    repeated = register_frozen_forward_plan(
        registry,
        vintage.vintage_id,
        "manifest-hash",
        "candidate-set-hash",
        "config-hash",
        "tree-hash",
        "reports/plan.json",
        "plan-artifact-hash",
        frozen_at=_time("2026-12-16T00:00:00"),
    )
    assert repeated == plan
    with pytest.raises(ValueError, match="不同的冻结前向计划"):
        register_frozen_forward_plan(
            registry,
            vintage.vintage_id,
            "different-manifest",
            "candidate-set-hash",
            "config-hash",
            "tree-hash",
            "reports/plan.json",
            "plan-artifact-hash",
            frozen_at=_time("2026-12-16T00:00:00"),
        )
    with pytest.raises(ValueError, match="开始前"):
        register_frozen_forward_plan(
            registry,
            vintage.vintage_id,
            "manifest-hash",
            "candidate-set-hash",
            "config-hash",
            "tree-hash",
            "reports/plan.json",
            "plan-artifact-hash",
            frozen_at=_time("2027-01-01T00:00:01"),
        )

    prediction = register_frozen_forward_prediction(
        registry,
        plan.plan_id,
        _time("2027-01-02T01:00:00"),
        "sha256-head",
        "panel-hash",
        "reports/prediction.json",
        "prediction-hash",
        3900,
        recorded_at=_time("2027-01-02T01:01:00"),
    )
    assert list_frozen_forward_predictions(registry, plan.plan_id) == (prediction,)
    assert register_frozen_forward_prediction(
        registry,
        plan.plan_id,
        _time("2027-01-02T01:00:00"),
        "sha256-head",
        "panel-hash",
        "reports/prediction.json",
        "prediction-hash",
        3900,
        recorded_at=_time("2027-01-02T01:02:00"),
    ) == prediction
    with pytest.raises(ValueError, match="不可改写"):
        register_frozen_forward_prediction(
            registry,
            plan.plan_id,
            _time("2027-01-02T01:00:00"),
            "sha256-different-head",
            "panel-hash",
            "reports/prediction.json",
            "different-prediction-hash",
            3900,
            recorded_at=_time("2027-01-02T01:02:00"),
        )
    with pytest.raises(ValueError, match="时效窗口"):
        register_frozen_forward_prediction(
            registry,
            plan.plan_id,
            _time("2027-01-03T01:00:00"),
            "sha256-head",
            "panel-hash",
            "reports/late.json",
            "late-hash",
            3900,
            recorded_at=_time("2027-01-03T03:00:00"),
        )
    with pytest.raises(ValueError, match="不在绑定 vintage"):
        register_frozen_forward_prediction(
            registry,
            plan.plan_id,
            _time("2027-02-02T01:00:00"),
            "sha256-head",
            "panel-hash",
            "reports/outside.json",
            "outside-hash",
            3900,
            recorded_at=_time("2027-02-02T01:01:00"),
        )


def test_holdout_is_consumed_before_market_data_is_opened(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """评估崩溃也必须烧毁 vintage，不能用失败重跑窥视封存段。"""
    registry_path = tmp_path / "data" / "research" / "governance.sqlite3"
    vintage = seal_holdout_vintage(
        registry_path,
        "market-one",
        _time("2027-01-01T00:00:00"),
        _time("2027-02-01T00:00:00"),
        sealed_at=_time("2026-12-01T00:00:00"),
    )
    config = tmp_path / "config.json"
    config.write_text(
        '{"market_id":"market-one","data_governance":{'
        '"registry":"data/research/governance.sqlite3"},'
        '"bar_interval":"1hour","from_time":"2020-01-01T00:00:00+00:00",'
        '"notional_scale":100000000}',
        encoding="utf-8",
    )
    config_hash = sha256_file(config)
    template = strategy_expression("trend")
    parameters: dict[str, int | float] = {
        "annual_volatility_target": 0.4,
        "entry_score": 0.5,
        "exit_score": 0.0,
        "lookback": 168,
        "maximum_target": 1.0,
    }
    candidate_id = candidate_identity(template, parameters)
    candidate_registry = tmp_path / "reports" / "candidate-registry.json"
    candidate_registry.parent.mkdir(parents=True)
    registry_text = json.dumps({
        "config_hash": config_hash,
        "expression_method_version": EXPRESSION_METHOD_VERSION,
        "candidates": [{
            "candidate_id": candidate_id,
            "complexity": 5,
            "expression_id": expression_id(template),
            "family": "trend",
            "mode": "paper",
            "parameters": parameters,
        }],
    }, sort_keys=True, separators=(",", ":"))
    candidate_registry.write_text(
        registry_text,
        encoding="utf-8",
    )
    registry_record = {
        "path": "reports/candidate-registry.json",
        "sha256": sha256_file(candidate_registry),
        "bytes": candidate_registry.stat().st_size,
    }
    summary = tmp_path / "reports" / "summary.json"
    summary.write_text(json.dumps({
        "pipeline_method_version": "strategy-research-pipeline-v9",
        "run_id": "run-one",
        "research_identity": "research-one",
        "config_hash": config_hash,
        "decision_grade": True,
        "code_identity": {"git_hash": "source-commit", "tree_digest": "tree-one"},
        "family_scope": ["trend", "breakout"],
        "family_evaluations": [{
            "deployment_candidate_id": candidate_id,
            "eligible": True,
            "mode": "paper",
        }],
        "artifacts": {"candidate_registry": registry_record},
    }, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )
    manifest = tmp_path / "reports" / "manifest.json"
    manifest.write_text(json.dumps({
        "run_id": "run-one",
        "research_identity": "research-one",
        "config_hash": config_hash,
        "artifacts": {
            "candidate_registry": registry_record,
            "summary_json": {
                "path": "reports/summary.json",
                "sha256": sha256_file(summary),
                "bytes": summary.stat().st_size,
            },
        },
    }, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "guvolu.research.holdout.verify_research_run",
        lambda _root, _manifest: VerificationResult(
            run_id="run-one",
            manifest_path=manifest,
            manifest_sha256=sha256_file(manifest),
            checked_artifacts=("candidate_registry", "summary_json"),
        ),
    )
    monkeypatch.setattr(
        "guvolu.research.holdout.freeze_trade_inputs",
        lambda _root, _market: FrozenPanelInputs(
            market={"market_id": "market-one"},
            paths=(),
            head_generation="head-one",
            attempt_ids=("attempt-one",),
            artifact_ids=("artifact-one",),
            normalization_versions=("normalization-one",),
            maximum_event_time=_time("2027-03-01T00:00:00"),
        ),
    )
    monkeypatch.setattr(
        "guvolu.research.holdout.code_identity",
        lambda _root, _paths: CodeIdentity(
            git_hash="commit-one",
            tree_digest="tree-one",
            dirty_digest="dirty-one",
            dirty=False,
            decision_grade=True,
            reason=None,
        ),
    )

    candidate_registry.write_text(registry_text + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="candidate registry (散列|字节数)"):
        run_holdout_validation(
            tmp_path,
            config,
            summary,
            vintage.vintage_id,
        )
    assert list_holdout_vintages(registry_path)[0].status == "sealed"
    candidate_registry.write_text(registry_text, encoding="utf-8")

    def fail_after_consumption(*_args: object, **_kwargs: object) -> object:
        raise RuntimeError("simulated panel failure")

    monkeypatch.setattr(
        "guvolu.research.holdout.build_panel_snapshot",
        fail_after_consumption,
    )
    with pytest.raises(RuntimeError, match="simulated panel failure"):
        run_holdout_validation(
            tmp_path,
            config,
            summary,
            vintage.vintage_id,
        )
    consumed = list_holdout_vintages(registry_path)[0]
    assert consumed.status == "consumed"
    assert consumed.evaluation_id is not None
    assert consumed.verdict is None
