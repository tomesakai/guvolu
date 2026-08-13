"""研究数据暴露与一次性封存段治理测试。"""
from __future__ import annotations

import inspect
import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from guvolu.research import clock
from guvolu.research.contracts import (
    FROZEN_FORWARD_METHOD_VERSION,
    FROZEN_FORWARD_SCHEMA_VERSION,
    HOLDOUT_MANIFEST_SCHEMA_VERSION,
    HOLDOUT_METHOD_VERSION,
    CodeIdentity,
    FrozenPanelInputs,
)
from guvolu.research.frozen_forward import (
    freeze_forward_plan,
    run_frozen_forward_prediction,
)
from guvolu.research.governance import (
    GOVERNANCE_METHOD_VERSION,
    finalize_holdout_evaluation,
    get_holdout_evaluation_attempt,
    get_frozen_forward_plan_for_vintage,
    list_frozen_forward_predictions,
    list_holdout_vintages,
    register_frozen_forward_plan,
    register_frozen_forward_prediction,
    register_research_exposure,
    seal_holdout_vintage,
    start_holdout_evaluation_attempt,
)
from guvolu.research.holdout import _score_decision_times, run_holdout_validation
from guvolu.research.provenance import sha256_file, stable_identifier
from guvolu.research.verification import VerificationResult
from guvolu.strategy.expression import (
    EXPRESSION_METHOD_VERSION,
    candidate_identity,
    expression_id,
    strategy_expression,
)


_TEST_PARAMETERS: dict[str, int | float] = {
    "annual_volatility_target": 0.4,
    "entry_score": 0.5,
    "exit_score": 0.0,
    "lookback": 168,
    "maximum_target": 1.0,
}
_TEST_TEMPLATE = strategy_expression("trend")
_TEST_EXPRESSION_ID = expression_id(_TEST_TEMPLATE)
_TEST_CANDIDATE_ID = candidate_identity(_TEST_TEMPLATE, _TEST_PARAMETERS)


@dataclass(frozen=True)
class _DecisionBar:
    decision_time: datetime


def _time(value: str) -> datetime:
    """构造测试 UTC 时间。"""
    return datetime.fromisoformat(value).replace(tzinfo=UTC)


_TEST_NOW = _time("2026-08-14T00:00:00")


@pytest.fixture(autouse=True)
def _authoritative_test_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    """测试通过替换内部壁钟推进时间，生产 API 不接受时间覆盖。"""
    global _TEST_NOW
    _TEST_NOW = _time("2026-08-14T00:00:00")
    monkeypatch.setattr(clock, "utc_now", lambda: _TEST_NOW)


def _set_now(value: datetime) -> None:
    global _TEST_NOW
    _TEST_NOW = value


def test_score_schedule_uses_the_same_previous_bar_indices_as_returns() -> None:
    """多柱评分时点边界必须来自 index-1，不得把标签结束柱混入 schedule。"""
    bars = tuple(
        _DecisionBar(_time(f"2027-01-01T0{hour}:00:00"))
        for hour in range(5)
    )
    decisions = _score_decision_times(bars, 1, 5)
    assert decisions == tuple(bar.decision_time for bar in bars[:4])
    assert decisions[-1] != bars[4].decision_time


def _test_candidate_set(candidate_ids: list[str]) -> tuple[dict[str, object], str]:
    """构造与生产散列合同一致的冻结候选集合身份。"""
    identity: dict[str, object] = {
        "holdout_method_version": HOLDOUT_METHOD_VERSION,
        "source_manifest_sha256": "1" * 64,
        "source_summary_sha256": "2" * 64,
        "candidate_registry_sha256": "3" * 64,
        "candidate_ids": sorted(candidate_ids),
    }
    return identity, stable_identifier("candidate-set", identity)


def _test_evaluation_identity(
    root: Path,
    vintage_id: str,
    candidate_set_hash: str,
    *,
    require_forward_predictions: bool = False,
) -> tuple[dict[str, object], str]:
    """写入冻结 policy 配置并生成与生产一致的 evaluation 身份。"""
    policy = {
        "minimum_bars": 1,
        "minimum_sharpe": 0.0,
        "maximum_drawdown": 0.45,
        "maximum_fdr_q": 0.2,
        "require_frozen_forward_predictions": require_forward_predictions,
    }
    config_path = root / "config" / "holdout-test.json"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(json.dumps({
        "data_governance": {"holdout_policy": policy},
    }, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    identity: dict[str, object] = {
        "holdout_method_version": HOLDOUT_METHOD_VERSION,
        "governance_method_version": GOVERNANCE_METHOD_VERSION,
        "vintage_id": vintage_id,
        "candidate_set_hash": candidate_set_hash,
        "config_hash": sha256_file(config_path),
        "code_tree_digest": "tree-one",
        "input_head_generation": "head-one",
        "input_artifact_ids": ["artifact-one"],
    }
    return identity, stable_identifier("holdout-evaluation", identity)


def _write_forward_plan_artifact(
    root: Path,
    vintage_id: str,
    source_manifest_sha256: str,
    candidate_set_hash: str,
    config_hash: str,
    code_tree_digest: str,
) -> tuple[str, str, str]:
    """写入与注册合同一致的冻结前向计划制品。"""
    plan_id = stable_identifier("frozen-forward-plan", {
        "governance_method_version": GOVERNANCE_METHOD_VERSION,
        "vintage_id": vintage_id,
        "source_manifest_sha256": source_manifest_sha256,
        "candidate_set_hash": candidate_set_hash,
        "config_hash": config_hash,
        "code_tree_digest": code_tree_digest,
    })
    path = root / "reports" / plan_id / "plan.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "schema_version": FROZEN_FORWARD_SCHEMA_VERSION,
        "method_version": FROZEN_FORWARD_METHOD_VERSION,
        "governance_method_version": GOVERNANCE_METHOD_VERSION,
        "scope": "FROZEN_FORWARD",
        "plan_id": plan_id,
        "vintage": {"vintage_id": vintage_id},
        "source": {"manifest_sha256": source_manifest_sha256},
        "candidate_set_hash": candidate_set_hash,
        "config_hash": config_hash,
        "code_tree_digest": code_tree_digest,
        "candidates": [{
            "candidate_id": _TEST_CANDIDATE_ID,
            "family": "trend",
            "mode": "paper",
            "expression_id": _TEST_EXPRESSION_ID,
            "parameters": _TEST_PARAMETERS,
            "complexity": len(_TEST_PARAMETERS),
        }],
        "allocation": {"weights": {"trend": 0.4}, "reserve": 0.6},
    }, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    return plan_id, path.relative_to(root).as_posix(), sha256_file(path)


def _write_forward_prediction_artifact(
    root: Path,
    plan_id: str,
    vintage_id: str,
    decision_time: datetime,
    input_head_generation: str,
    panel_sha256: str,
    config_hash: str,
    code_tree_digest: str,
) -> tuple[str, str]:
    """写入与注册合同一致的冻结前向预测制品。"""
    prediction_id = stable_identifier("frozen-forward-prediction", {
        "governance_method_version": GOVERNANCE_METHOD_VERSION,
        "plan_id": plan_id,
        "decision_time": decision_time.isoformat(),
    })
    stamp = decision_time.strftime("%Y%m%dT%H%M%SZ")
    path = root / "reports" / plan_id / "predictions" / f"{stamp}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "schema_version": FROZEN_FORWARD_SCHEMA_VERSION,
        "method_version": FROZEN_FORWARD_METHOD_VERSION,
        "scope": "FROZEN_FORWARD",
        "prediction_id": prediction_id,
        "plan_id": plan_id,
        "vintage_id": vintage_id,
        "decision_time": decision_time.isoformat(),
        "input_head_generation": input_head_generation,
        "panel_sha256": panel_sha256,
        "config_hash": config_hash,
        "code_identity": {"tree_digest": code_tree_digest},
        "quality": {
            "integrity": True,
            "freshness": True,
            "clock": True,
            "coverage": True,
            "pit": True,
            "lineage": True,
            "eligible": True,
            "reasons": [],
        },
        "families": [{
            "candidate_id": _TEST_CANDIDATE_ID,
            "family": "trend",
            "family_target": 0.5,
            "frozen_allocation_weight": 0.4,
            "portfolio_target_contribution": 0.2,
        }],
        "reserve": 0.6,
        "aggregate_target": 0.2,
        "unit": "risk_weighted_directional_target",
    }, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    return path.relative_to(root).as_posix(), sha256_file(path)


def _write_holdout_evidence(
    root: Path,
    vintage_id: str,
    evaluation_identity: dict[str, object],
    candidate_set_identity: dict[str, object],
    verdict: str,
    score_start: datetime,
    *,
    score_decision_times: tuple[datetime, ...] | None = None,
    forward_plan_id: str | None = None,
    forward_prediction_count: int = 0,
) -> tuple[str, str, str]:
    """写入彼此绑定的完整 holdout panel、result、manifest 与 verdict。"""
    evaluation_id = stable_identifier("holdout-evaluation", evaluation_identity)
    run_directory = root / "reports" / "holdout" / evaluation_id
    run_directory.mkdir(parents=True, exist_ok=True)
    panel_path = run_directory / "panel.parquet"
    panel_path.write_bytes(b"PAR1holdout-test-panelPAR1")
    panel_sha256 = sha256_file(panel_path)
    decisions = score_decision_times or (score_start,)
    schedule_path = run_directory / "score-schedule.json"
    schedule_path.write_text(json.dumps({
        "schema_version": HOLDOUT_MANIFEST_SCHEMA_VERSION,
        "holdout_method_version": HOLDOUT_METHOD_VERSION,
        "evaluation_id": evaluation_id,
        "decision_times": [value.isoformat() for value in decisions],
    }, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    schedule_sha256 = sha256_file(schedule_path)
    raw_candidate_ids = candidate_set_identity["candidate_ids"]
    if not isinstance(raw_candidate_ids, list):
        raise AssertionError("测试 candidate_ids 必须为列表")
    candidate_ids = [str(item) for item in raw_candidate_ids]
    candidate_set_hash = stable_identifier(
        "candidate-set", candidate_set_identity,
    )
    config_path = root / "config" / "holdout-test.json"
    config = json.loads(config_path.read_text(encoding="utf-8"))
    policy = config["data_governance"]["holdout_policy"]
    config_hash = sha256_file(config_path)
    require_forward = policy["require_frozen_forward_predictions"] is True
    passed = verdict == "passed"
    passed_families = ["trend"] if passed else []
    metrics = {
        "net_return": 0.1 if passed else -0.1,
        "sharpe": 1.0 if passed else -1.0,
        "maximum_drawdown": 0.1 if passed else 0.5,
        "p_value": 0.01 if passed else 0.9,
    }
    rejection_reasons = [] if passed else [
        "non_positive_holdout_net_return",
        "holdout_sharpe_failed",
        "holdout_drawdown_failed",
        "holdout_fdr_failed",
    ]
    result_path = run_directory / "result.json"
    result_path.write_text(json.dumps({
        "schema_version": HOLDOUT_MANIFEST_SCHEMA_VERSION,
        "holdout_method_version": HOLDOUT_METHOD_VERSION,
        "evaluation_id": evaluation_id,
        "vintage": {"vintage_id": vintage_id},
        "candidate_set_hash": candidate_set_hash,
        "config_hash": config_hash,
        "panel_sha256": panel_sha256,
        "score_schedule_sha256": schedule_sha256,
        "candidate_results": [{
            "candidate_id": candidate_ids[0],
            "family": "trend",
            "metrics": metrics,
            "fdr_q": metrics["p_value"],
            "passed": passed,
            "rejection_reasons": rejection_reasons,
        }],
        "score_start": score_start.isoformat(),
        "score_end": decisions[-1].isoformat(),
        "score_bars": len(decisions),
        "target_source": (
            "recorded_frozen_forward"
            if require_forward else "end_of_vintage_recompute"
        ),
        "frozen_forward_plan_id": forward_plan_id if require_forward else None,
        "frozen_forward_prediction_count": (
            forward_prediction_count if require_forward else 0
        ),
        "policy": policy,
        "passed_families": passed_families,
        "verdict": verdict,
    }, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    result_sha256 = sha256_file(result_path)
    manifest_path = run_directory / "manifest.json"
    manifest_path.write_text(json.dumps({
        "schema_version": HOLDOUT_MANIFEST_SCHEMA_VERSION,
        "holdout_method_version": HOLDOUT_METHOD_VERSION,
        "evaluation_id": evaluation_id,
        "vintage_id": vintage_id,
        "candidate_set_hash": candidate_set_hash,
        "candidate_set_identity": candidate_set_identity,
        "evaluation_identity": evaluation_identity,
        "verdict": verdict,
        "artifacts": {
            "config": {
                "kind": "holdout_config",
                "path": config_path.relative_to(root).as_posix(),
                "sha256": config_hash,
                "bytes": config_path.stat().st_size,
            },
            "panel": {
                "kind": "holdout_panel",
                "path": panel_path.relative_to(root).as_posix(),
                "sha256": panel_sha256,
                "bytes": panel_path.stat().st_size,
            },
            "score_schedule": {
                "kind": "holdout_score_schedule",
                "path": schedule_path.relative_to(root).as_posix(),
                "sha256": schedule_sha256,
                "bytes": schedule_path.stat().st_size,
            },
            "result": {
                "kind": "holdout_result",
                "path": result_path.relative_to(root).as_posix(),
                "sha256": result_sha256,
                "bytes": result_path.stat().st_size,
            },
        },
    }, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    manifest_sha256 = sha256_file(manifest_path)
    terminal = json.dumps({
        "evaluation_id": evaluation_id,
        "verdict": verdict,
        "candidate_ids": candidate_ids,
        "passed_families": passed_families,
        "result_sha256": result_sha256,
        "manifest_sha256": manifest_sha256,
    }, sort_keys=True, separators=(",", ":"))
    return terminal, manifest_path.relative_to(root).as_posix(), manifest_sha256


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
    _set_now(_time("2025-04-01T00:00:00"))
    with pytest.raises(ValueError, match="已被自适应研究读取"):
        seal_holdout_vintage(
            registry,
            "market-one",
            _time("2025-05-01T00:00:00"),
            _time("2025-07-01T00:00:00"),
        )

    _set_now(_time("2025-06-03T00:00:00"))
    vintage = seal_holdout_vintage(
        registry,
        "market-one",
        _time("2025-07-01T00:00:00"),
        _time("2025-08-01T00:00:00"),
    )
    repeated = seal_holdout_vintage(
        registry,
        "market-one",
        _time("2025-07-01T00:00:00"),
        _time("2025-08-01T00:00:00"),
    )
    assert repeated == vintage
    with pytest.raises(ValueError, match="既有 vintage 重叠"):
        seal_holdout_vintage(
            registry,
            "market-one",
            _time("2025-07-15T00:00:00"),
            _time("2025-08-15T00:00:00"),
        )
    with pytest.raises(ValueError, match="未消费封存段重叠"):
        register_research_exposure(
            registry,
            "research-identity-two",
            "market-one",
            _time("2025-07-15T00:00:00"),
            _time("2025-07-20T00:00:00"),
        )

    candidate_set_identity, candidate_set_hash = _test_candidate_set(
        [_TEST_CANDIDATE_ID]
    )
    evaluation_identity, evaluation_id = _test_evaluation_identity(
        tmp_path, vintage.vintage_id, candidate_set_hash,
    )
    _set_now(_time("2025-08-02T00:00:00"))
    manual_attempt = start_holdout_evaluation_attempt(
        registry,
        vintage.vintage_id,
        candidate_set_hash,
        evaluation_id,
    )
    consumed = list_holdout_vintages(registry)[0]
    assert consumed.status == "consumed"
    assert consumed.consumed_at == _time("2025-08-02T00:00:00")
    assert manual_attempt.status == "incomplete"
    assert manual_attempt.stage == "vintage_consumed"
    with pytest.raises(ValueError, match="已经消费"):
        start_holdout_evaluation_attempt(
            registry,
            vintage.vintage_id,
            "different-candidate-set",
            "different-evaluation",
        )

    terminal, manifest_path, manifest_sha256 = _write_holdout_evidence(
        tmp_path,
        vintage.vintage_id,
        evaluation_identity,
        candidate_set_identity,
        "failed",
        vintage.start_time,
    )
    _set_now(_time("2025-08-03T00:00:00"))
    decided, completed_attempt = finalize_holdout_evaluation(
        registry,
        vintage.vintage_id,
        evaluation_id,
        terminal,
        manifest_path,
        manifest_sha256,
        repository_root=tmp_path,
    )
    assert decided.verdict == terminal
    assert completed_attempt.status == "completed"
    assert completed_attempt.result_manifest_sha256 == manifest_sha256
    with pytest.raises(ValueError, match="已经终结"):
        finalize_holdout_evaluation(
            registry,
            vintage.vintage_id,
            evaluation_id,
            terminal,
            manifest_path,
            manifest_sha256,
            repository_root=tmp_path,
        )
    assert list_holdout_vintages(registry) == (decided,)


def test_consumed_vintage_can_become_adaptive_but_never_holdout_again(
    tmp_path: Path,
) -> None:
    """消费后的数据可进入开发史，但同一段不能再声称为新 holdout。"""
    registry = tmp_path / "governance.sqlite3"
    _set_now(_time("2024-12-01T00:00:00"))
    vintage = seal_holdout_vintage(
        registry,
        "market-one",
        _time("2025-01-01T00:00:00"),
        _time("2025-02-01T00:00:00"),
    )
    _set_now(_time("2025-02-02T00:00:00"))
    start_holdout_evaluation_attempt(
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
    _set_now(_time("2024-12-01T00:00:00"))
    with pytest.raises(ValueError, match="已被自适应研究读取"):
        seal_holdout_vintage(
            registry,
            "market-one",
            _time("2025-01-01T00:00:00"),
            _time("2025-02-01T00:00:00"),
        )


def test_legacy_consumed_vintage_is_migrated_to_incomplete_attempt(
    tmp_path: Path,
) -> None:
    """旧库已消费记录必须回填为明确且不可重跑的 incomplete 尝试。"""
    registry = tmp_path / "governance.sqlite3"
    vintage = seal_holdout_vintage(
        registry,
        "market-one",
        _time("2027-01-01T00:00:00"),
        _time("2027-02-01T00:00:00"),
    )
    _set_now(_time("2027-02-02T00:00:00"))
    start_holdout_evaluation_attempt(
        registry,
        vintage.vintage_id,
        "candidate-set-hash",
        "legacy-evaluation",
    )
    with sqlite3.connect(registry) as connection:
        connection.execute("DELETE FROM holdout_evaluation_attempt")
        connection.execute(
            "UPDATE governance_meta SET value='2' WHERE key='schema_version'"
        )
    migrated = get_holdout_evaluation_attempt(registry, "legacy-evaluation")
    assert migrated.status == "incomplete"
    assert migrated.stage == "legacy_consumed_without_attempt"
    assert migrated.started_at == _time("2027-02-02T00:00:00")
    with sqlite3.connect(registry) as connection:
        version = connection.execute(
            "SELECT value FROM governance_meta WHERE key='schema_version'"
        ).fetchone()
    assert version == ("4",)


def test_legacy_verdict_without_manifest_attempt_is_rejected(tmp_path: Path) -> None:
    """无法证明 manifest 的旧 verdict 不得被伪装成合法 completed 终态。"""
    registry = tmp_path / "governance.sqlite3"
    vintage = seal_holdout_vintage(
        registry,
        "market-one",
        _time("2027-01-01T00:00:00"),
        _time("2027-02-01T00:00:00"),
    )
    _set_now(_time("2027-02-02T00:00:00"))
    start_holdout_evaluation_attempt(
        registry,
        vintage.vintage_id,
        "candidate-set-hash",
        "legacy-evaluation",
    )
    with sqlite3.connect(registry) as connection:
        connection.execute("DELETE FROM holdout_evaluation_attempt")
        connection.execute(
            "UPDATE holdout_vintage SET verdict='passed',verdict_recorded_at=?",
            (_time("2027-02-03T00:00:00").isoformat(),),
        )
        connection.execute(
            "UPDATE governance_meta SET value='3' WHERE key='schema_version'"
        )
    with pytest.raises(ValueError, match="无 manifest attempt"):
        list_holdout_vintages(registry)


def test_holdout_verdict_and_completed_attempt_are_atomic(tmp_path: Path) -> None:
    """成功结论与 completed 尝试必须在同一事务中出现。"""
    registry = tmp_path / "governance.sqlite3"
    vintage = seal_holdout_vintage(
        registry,
        "market-one",
        _time("2027-01-01T00:00:00"),
        _time("2027-02-01T00:00:00"),
    )
    candidate_set_identity, candidate_set_hash = _test_candidate_set(
        [_TEST_CANDIDATE_ID]
    )
    evaluation_identity, evaluation_id = _test_evaluation_identity(
        tmp_path, vintage.vintage_id, candidate_set_hash,
    )
    _set_now(_time("2027-02-02T00:00:00"))
    start_holdout_evaluation_attempt(
        registry,
        vintage.vintage_id,
        candidate_set_hash,
        evaluation_id,
    )
    wrong_terminal, wrong_manifest, wrong_sha256 = _write_holdout_evidence(
        tmp_path,
        "different-vintage",
        evaluation_identity,
        candidate_set_identity,
        "passed",
        vintage.start_time,
    )
    with pytest.raises(ValueError, match="manifest 的 vintage_id 不匹配"):
        finalize_holdout_evaluation(
            registry,
            vintage.vintage_id,
            evaluation_id,
            wrong_terminal,
            wrong_manifest,
            wrong_sha256,
            repository_root=tmp_path,
        )
    unsupported_terminal, unsupported_manifest, unsupported_sha256 = (
        _write_holdout_evidence(
            tmp_path,
            vintage.vintage_id,
            evaluation_identity,
            candidate_set_identity,
            "skipped",
            vintage.start_time,
        )
    )
    with pytest.raises(ValueError, match="只能是 passed 或 failed"):
        finalize_holdout_evaluation(
            registry,
            vintage.vintage_id,
            evaluation_id,
            unsupported_terminal,
            unsupported_manifest,
            unsupported_sha256,
            repository_root=tmp_path,
        )
    truncated_identity, truncated_candidate_hash = _test_candidate_set(
        ["candidate-one", "candidate-two"]
    )
    truncated_evaluation_identity, truncated_evaluation_id = (
        _test_evaluation_identity(
            tmp_path, vintage.vintage_id, truncated_candidate_hash,
        )
    )
    truncated_terminal, truncated_manifest, truncated_sha256 = (
        _write_holdout_evidence(
            tmp_path,
            vintage.vintage_id,
            truncated_evaluation_identity,
            truncated_identity,
            "passed",
            vintage.start_time,
        )
    )
    with pytest.raises(ValueError, match="未覆盖冻结候选全集"):
        finalize_holdout_evaluation(
            registry,
            vintage.vintage_id,
            truncated_evaluation_id,
            truncated_terminal,
            truncated_manifest,
            truncated_sha256,
            repository_root=tmp_path,
        )
    terminal, manifest_path, manifest_sha256 = _write_holdout_evidence(
        tmp_path,
        vintage.vintage_id,
        evaluation_identity,
        candidate_set_identity,
        "passed",
        vintage.start_time,
    )
    manifest_file = tmp_path / manifest_path
    missing_panel = json.loads(manifest_file.read_text(encoding="utf-8"))
    del missing_panel["artifacts"]["panel"]
    manifest_file.write_text(
        json.dumps(missing_panel, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    missing_panel_sha256 = sha256_file(manifest_file)
    missing_panel_terminal = json.loads(terminal)
    missing_panel_terminal["manifest_sha256"] = missing_panel_sha256
    with pytest.raises(ValueError, match="缺少 panel 制品"):
        finalize_holdout_evaluation(
            registry,
            vintage.vintage_id,
            evaluation_id,
            json.dumps(
                missing_panel_terminal,
                sort_keys=True,
                separators=(",", ":"),
            ),
            manifest_path,
            missing_panel_sha256,
            repository_root=tmp_path,
        )
    terminal, manifest_path, manifest_sha256 = _write_holdout_evidence(
        tmp_path,
        vintage.vintage_id,
        evaluation_identity,
        candidate_set_identity,
        "passed",
        vintage.start_time,
    )
    with pytest.raises(ValueError, match="超出仓库范围"):
        finalize_holdout_evaluation(
            registry,
            vintage.vintage_id,
            evaluation_id,
            terminal,
            "../outside-manifest.json",
            manifest_sha256,
            repository_root=tmp_path,
        )
    with pytest.raises(ValueError, match="规范小写十六进制"):
        finalize_holdout_evaluation(
            registry,
            vintage.vintage_id,
            evaluation_id,
            terminal,
            manifest_path,
            "g" * 64,
            repository_root=tmp_path,
        )
    with pytest.raises(ValueError, match="现场 SHA-256 不匹配"):
        finalize_holdout_evaluation(
            registry,
            vintage.vintage_id,
            evaluation_id,
            terminal,
            manifest_path,
            "0" * 64,
            repository_root=tmp_path,
        )
    assert get_holdout_evaluation_attempt(
        registry, evaluation_id,
    ).status == "incomplete"
    finalized_vintage, finalized_attempt = finalize_holdout_evaluation(
        registry,
        vintage.vintage_id,
        evaluation_id,
        terminal,
        manifest_path,
        manifest_sha256,
        repository_root=tmp_path,
    )
    assert finalized_vintage.verdict == terminal
    assert finalized_attempt.status == "completed"
    assert finalized_attempt.result_manifest_sha256 == manifest_sha256
    with pytest.raises(ValueError, match="已经终结"):
        finalize_holdout_evaluation(
            registry,
            vintage.vintage_id,
            evaluation_id,
            terminal,
            manifest_path,
            manifest_sha256,
            repository_root=tmp_path,
        )


def test_registered_forward_plan_prevents_relaxed_policy_finalize(
    tmp_path: Path,
) -> None:
    """已冻结 plan 的 vintage 不得改用关闭前向要求的宽松配置终结。"""
    registry = tmp_path / "governance.sqlite3"
    vintage = seal_holdout_vintage(
        registry,
        "market-one",
        _time("2027-01-01T00:00:00"),
        _time("2027-02-01T00:00:00"),
    )
    candidate_set_identity, candidate_set_hash = _test_candidate_set(
        [_TEST_CANDIDATE_ID]
    )
    evaluation_identity, evaluation_id = _test_evaluation_identity(
        tmp_path,
        vintage.vintage_id,
        candidate_set_hash,
        require_forward_predictions=False,
    )
    config_hash = evaluation_identity["config_hash"]
    assert isinstance(config_hash, str)
    _, plan_path, plan_sha256 = _write_forward_plan_artifact(
        tmp_path,
        vintage.vintage_id,
        "1" * 64,
        candidate_set_hash,
        config_hash,
        "tree-one",
    )
    register_frozen_forward_plan(
        registry,
        vintage.vintage_id,
        "1" * 64,
        candidate_set_hash,
        config_hash,
        "tree-one",
        plan_path,
        plan_sha256,
        repository_root=tmp_path,
    )
    _set_now(_time("2027-02-02T00:00:00"))
    start_holdout_evaluation_attempt(
        registry,
        vintage.vintage_id,
        candidate_set_hash,
        evaluation_id,
    )
    terminal, manifest_path, manifest_sha256 = _write_holdout_evidence(
        tmp_path,
        vintage.vintage_id,
        evaluation_identity,
        candidate_set_identity,
        "passed",
        vintage.start_time,
    )
    with pytest.raises(ValueError, match="冻结前向 plan 与注册表不一致"):
        finalize_holdout_evaluation(
            registry,
            vintage.vintage_id,
            evaluation_id,
            terminal,
            manifest_path,
            manifest_sha256,
            repository_root=tmp_path,
        )
    assert get_holdout_evaluation_attempt(registry, evaluation_id).status == "incomplete"


def test_forward_plan_recomputes_candidate_formula_identity(tmp_path: Path) -> None:
    """plan 内候选 ID 必须由受支持公式和完整参数现场重算。"""
    registry = tmp_path / "governance.sqlite3"
    vintage = seal_holdout_vintage(
        registry,
        "market-one",
        _time("2027-01-01T00:00:00"),
        _time("2027-02-01T00:00:00"),
    )
    _, plan_path, _ = _write_forward_plan_artifact(
        tmp_path,
        vintage.vintage_id,
        "1" * 64,
        "candidate-set-hash",
        "config-hash",
        "tree-one",
    )
    artifact = tmp_path / plan_path
    payload = json.loads(artifact.read_text(encoding="utf-8"))
    payload["candidates"][0]["parameters"]["lookback"] = 72
    artifact.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="候选身份未绑定公式与完整参数"):
        register_frozen_forward_plan(
            registry,
            vintage.vintage_id,
            "1" * 64,
            "candidate-set-hash",
            "config-hash",
            "tree-one",
            plan_path,
            sha256_file(artifact),
            repository_root=tmp_path,
        )


def test_forward_finalize_rejects_plan_candidate_set_substitution(
    tmp_path: Path,
) -> None:
    """顶层 hash 相同也不得用 plan 内另一组候选冒充冻结全集。"""
    registry = tmp_path / "governance.sqlite3"
    vintage = seal_holdout_vintage(
        registry,
        "market-one",
        _time("2027-01-01T00:00:00"),
        _time("2027-02-01T00:00:00"),
    )
    candidate_set_identity, candidate_set_hash = _test_candidate_set(
        ["candidate-substituted"]
    )
    evaluation_identity, evaluation_id = _test_evaluation_identity(
        tmp_path,
        vintage.vintage_id,
        candidate_set_hash,
        require_forward_predictions=True,
    )
    config_hash = evaluation_identity["config_hash"]
    assert isinstance(config_hash, str)
    _, plan_path, plan_sha256 = _write_forward_plan_artifact(
        tmp_path,
        vintage.vintage_id,
        "1" * 64,
        candidate_set_hash,
        config_hash,
        "tree-one",
    )
    plan = register_frozen_forward_plan(
        registry,
        vintage.vintage_id,
        "1" * 64,
        candidate_set_hash,
        config_hash,
        "tree-one",
        plan_path,
        plan_sha256,
        repository_root=tmp_path,
    )
    prediction_path, prediction_sha256 = _write_forward_prediction_artifact(
        tmp_path,
        plan.plan_id,
        vintage.vintage_id,
        vintage.start_time,
        "head-one",
        "panel-one",
        config_hash,
        "tree-one",
    )
    _set_now(vintage.start_time + timedelta(minutes=1))
    register_frozen_forward_prediction(
        registry,
        plan.plan_id,
        vintage.start_time,
        "head-one",
        "panel-one",
        prediction_path,
        prediction_sha256,
        3900,
        repository_root=tmp_path,
    )
    _set_now(_time("2027-02-02T00:00:00"))
    start_holdout_evaluation_attempt(
        registry,
        vintage.vintage_id,
        candidate_set_hash,
        evaluation_id,
    )
    terminal, manifest_path, manifest_sha256 = _write_holdout_evidence(
        tmp_path,
        vintage.vintage_id,
        evaluation_identity,
        candidate_set_identity,
        "passed",
        vintage.start_time,
        forward_plan_id=plan.plan_id,
        forward_prediction_count=1,
    )
    with pytest.raises(ValueError, match="计划候选与 holdout 冻结候选全集"):
        finalize_holdout_evaluation(
            registry,
            vintage.vintage_id,
            evaluation_id,
            terminal,
            manifest_path,
            manifest_sha256,
            repository_root=tmp_path,
        )


def test_forward_required_finalize_matches_registered_prediction_coverage(
    tmp_path: Path,
) -> None:
    """前向硬门必须把 plan、config 与评分区间预测逐项对齐后才可终结。"""
    registry = tmp_path / "governance.sqlite3"
    vintage = seal_holdout_vintage(
        registry,
        "market-one",
        _time("2027-01-01T00:00:00"),
        _time("2027-02-01T00:00:00"),
    )
    candidate_set_identity, candidate_set_hash = _test_candidate_set(
        [_TEST_CANDIDATE_ID]
    )
    evaluation_identity, evaluation_id = _test_evaluation_identity(
        tmp_path,
        vintage.vintage_id,
        candidate_set_hash,
        require_forward_predictions=True,
    )
    config_hash = evaluation_identity["config_hash"]
    assert isinstance(config_hash, str)
    _, plan_path, plan_sha256 = _write_forward_plan_artifact(
        tmp_path,
        vintage.vintage_id,
        "1" * 64,
        candidate_set_hash,
        config_hash,
        "tree-one",
    )
    plan = register_frozen_forward_plan(
        registry,
        vintage.vintage_id,
        "1" * 64,
        candidate_set_hash,
        config_hash,
        "tree-one",
        plan_path,
        plan_sha256,
        repository_root=tmp_path,
    )
    prediction_path, prediction_sha256 = _write_forward_prediction_artifact(
        tmp_path,
        plan.plan_id,
        vintage.vintage_id,
        vintage.start_time,
        "head-one",
        "panel-one",
        config_hash,
        "tree-one",
    )
    _set_now(vintage.start_time + timedelta(minutes=1))
    register_frozen_forward_prediction(
        registry,
        plan.plan_id,
        vintage.start_time,
        "head-one",
        "panel-one",
        prediction_path,
        prediction_sha256,
        3900,
        repository_root=tmp_path,
    )
    second_decision = vintage.start_time + timedelta(hours=1)
    second_path, second_sha256 = _write_forward_prediction_artifact(
        tmp_path,
        plan.plan_id,
        vintage.vintage_id,
        second_decision,
        "head-two",
        "panel-two",
        config_hash,
        "tree-one",
    )
    _set_now(second_decision + timedelta(minutes=1))
    register_frozen_forward_prediction(
        registry,
        plan.plan_id,
        second_decision,
        "head-two",
        "panel-two",
        second_path,
        second_sha256,
        3900,
        repository_root=tmp_path,
    )
    _set_now(_time("2027-02-02T00:00:00"))
    start_holdout_evaluation_attempt(
        registry,
        vintage.vintage_id,
        candidate_set_hash,
        evaluation_id,
    )
    terminal, manifest_path, manifest_sha256 = _write_holdout_evidence(
        tmp_path,
        vintage.vintage_id,
        evaluation_identity,
        candidate_set_identity,
        "passed",
        vintage.start_time,
        score_decision_times=(
            vintage.start_time,
            vintage.start_time + timedelta(hours=2),
        ),
        forward_plan_id=plan.plan_id,
        forward_prediction_count=2,
    )
    with pytest.raises(ValueError, match="时点未逐柱匹配"):
        finalize_holdout_evaluation(
            registry,
            vintage.vintage_id,
            evaluation_id,
            terminal,
            manifest_path,
            manifest_sha256,
            repository_root=tmp_path,
        )
    terminal, manifest_path, manifest_sha256 = _write_holdout_evidence(
        tmp_path,
        vintage.vintage_id,
        evaluation_identity,
        candidate_set_identity,
        "passed",
        vintage.start_time,
        score_decision_times=(vintage.start_time, second_decision),
        forward_plan_id=plan.plan_id,
        forward_prediction_count=2,
    )
    finalized, attempt = finalize_holdout_evaluation(
        registry,
        vintage.vintage_id,
        evaluation_id,
        terminal,
        manifest_path,
        manifest_sha256,
        repository_root=tmp_path,
    )
    assert finalized.verdict == terminal
    assert attempt.status == "completed"


def test_holdout_cannot_be_selected_retroactively(tmp_path: Path) -> None:
    """区间开始后才登记的所谓 holdout 必须被拒绝。"""
    with pytest.raises(ValueError, match="禁止事后挑选"):
        seal_holdout_vintage(
            tmp_path / "governance.sqlite3",
            "market-one",
            _time("2025-01-01T00:00:00"),
            _time("2025-02-01T00:00:00"),
        )


def test_governance_timestamps_are_not_caller_controlled(tmp_path: Path) -> None:
    """封存、计划、预测和消费时刻只能来自进程壁钟。"""
    assert "sealed_at" not in inspect.signature(seal_holdout_vintage).parameters
    assert "frozen_at" not in inspect.signature(freeze_forward_plan).parameters
    assert "frozen_at" not in inspect.signature(register_frozen_forward_plan).parameters
    assert "recorded_at" not in inspect.signature(run_frozen_forward_prediction).parameters
    assert "recorded_at" not in inspect.signature(
        register_frozen_forward_prediction
    ).parameters
    assert "started_at" not in inspect.signature(
        start_holdout_evaluation_attempt
    ).parameters

    registry = tmp_path / "governance.sqlite3"
    vintage = seal_holdout_vintage(
        registry,
        "market-one",
        _time("2027-01-01T00:00:00"),
        _time("2027-02-01T00:00:00"),
    )
    with pytest.raises(ValueError, match="完整结束后"):
        start_holdout_evaluation_attempt(
            registry,
            vintage.vintage_id,
            "candidate-set-hash",
            "evaluation-id",
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
    )
    _, plan_path, plan_sha256 = _write_forward_plan_artifact(
        tmp_path,
        vintage.vintage_id,
        "manifest-hash",
        "candidate-set-hash",
        "config-hash",
        "tree-hash",
    )
    plan = register_frozen_forward_plan(
        registry,
        vintage.vintage_id,
        "manifest-hash",
        "candidate-set-hash",
        "config-hash",
        "tree-hash",
        plan_path,
        plan_sha256,
        repository_root=tmp_path,
    )
    assert get_frozen_forward_plan_for_vintage(registry, vintage.vintage_id) == plan
    repeated = register_frozen_forward_plan(
        registry,
        vintage.vintage_id,
        "manifest-hash",
        "candidate-set-hash",
        "config-hash",
        "tree-hash",
        plan_path,
        plan_sha256,
        repository_root=tmp_path,
    )
    assert repeated == plan
    _, different_path, different_sha256 = _write_forward_plan_artifact(
        tmp_path,
        vintage.vintage_id,
        "different-manifest",
        "candidate-set-hash",
        "config-hash",
        "tree-hash",
    )
    with pytest.raises(ValueError, match="不同的冻结前向计划"):
        register_frozen_forward_plan(
            registry,
            vintage.vintage_id,
            "different-manifest",
            "candidate-set-hash",
            "config-hash",
            "tree-hash",
            different_path,
            different_sha256,
            repository_root=tmp_path,
        )
    _set_now(_time("2027-01-01T00:00:01"))
    with pytest.raises(ValueError, match="开始前"):
        register_frozen_forward_plan(
            registry,
            vintage.vintage_id,
            "manifest-hash",
            "candidate-set-hash",
            "config-hash",
            "tree-hash",
            plan_path,
            plan_sha256,
            repository_root=tmp_path,
        )

    decision = _time("2027-01-02T01:00:00")
    prediction_path, prediction_sha256 = _write_forward_prediction_artifact(
        tmp_path,
        plan.plan_id,
        vintage.vintage_id,
        decision,
        "sha256-head",
        "panel-hash",
        "config-hash",
        "tree-hash",
    )
    _set_now(decision + timedelta(minutes=1))
    prediction = register_frozen_forward_prediction(
        registry,
        plan.plan_id,
        decision,
        "sha256-head",
        "panel-hash",
        prediction_path,
        prediction_sha256,
        3900,
        repository_root=tmp_path,
    )
    assert list_frozen_forward_predictions(registry, plan.plan_id) == (prediction,)
    _set_now(decision + timedelta(minutes=2))
    assert register_frozen_forward_prediction(
        registry,
        plan.plan_id,
        decision,
        "sha256-head",
        "panel-hash",
        prediction_path,
        prediction_sha256,
        3900,
        repository_root=tmp_path,
    ) == prediction
    changed_path, changed_sha256 = _write_forward_prediction_artifact(
        tmp_path,
        plan.plan_id,
        vintage.vintage_id,
        decision,
        "sha256-different-head",
        "panel-hash",
        "config-hash",
        "tree-hash",
    )
    _set_now(decision + timedelta(minutes=2))
    with pytest.raises(ValueError, match="不可改写"):
        register_frozen_forward_prediction(
            registry,
            plan.plan_id,
            decision,
            "sha256-different-head",
            "panel-hash",
            changed_path,
            changed_sha256,
            3900,
            repository_root=tmp_path,
        )
    _set_now(_time("2027-01-03T03:00:00"))
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
            repository_root=tmp_path,
        )
    _set_now(_time("2027-02-02T01:01:00"))
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
            repository_root=tmp_path,
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
    _set_now(_time("2027-02-02T00:00:00"))
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
    attempt = get_holdout_evaluation_attempt(
        registry_path, consumed.evaluation_id,
    )
    assert attempt.status == "incomplete"
    assert attempt.stage == "building_panel"
    assert attempt.result_manifest_path is None
