"""计划级缺预测处置政策 missing_policy 的身份、治理与 holdout 合同测试。"""
from __future__ import annotations

import importlib.util
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType

import pytest

from guvolu.research import clock
from guvolu.research.config_lineage import snapshot_verified_config_lineage
from guvolu.research.contracts import (
    CodeIdentity,
    FROZEN_FORWARD_METHOD_VERSION,
    FROZEN_FORWARD_SCHEMA_VERSION,
    FrozenPanelInputs,
    HOLDOUT_MANIFEST_SCHEMA_VERSION,
    HOLDOUT_METHOD_VERSION,
    PanelSnapshot,
    PerformanceMetrics,
)
from guvolu.research.frozen_forward import (
    load_verified_prediction_targets,
    verify_frozen_forward,
)
from guvolu.research.governance import (
    DEFAULT_MISSING_POLICY,
    GOVERNANCE_METHOD_VERSION,
    GOVERNANCE_SCHEMA_VERSION,
    finalize_holdout_evaluation,
    frozen_forward_plan_identity,
    get_frozen_forward_plan,
    get_frozen_forward_plan_for_vintage,
    get_frozen_forward_prediction_row_set,
    get_holdout_evaluation_attempt,
    list_holdout_vintages,
    register_frozen_forward_plan,
    register_frozen_forward_prediction,
    seal_holdout_vintage,
    start_holdout_evaluation_attempt,
    upgrade_governance_write_ceiling,
)
from guvolu.research.holdout import apply_missing_policy, run_holdout_validation
from guvolu.research.provenance import (
    canonical_json,
    sha256_file,
    stable_identifier,
)
from guvolu.research.verification import VerificationResult
from guvolu.strategy.contracts import ResearchBar
from guvolu.strategy.expression import (
    EXPRESSION_METHOD_VERSION,
    candidate_identity,
    expression_id,
    strategy_expression,
)

_PARAMETERS: dict[str, int | float] = {
    "annual_volatility_target": 0.4,
    "entry_score": 0.5,
    "exit_score": 0.0,
    "lookback": 168,
    "maximum_target": 1.0,
}
_TEMPLATE = strategy_expression("trend")
_EXPRESSION_ID = expression_id(_TEMPLATE)
_CANDIDATE_ID = candidate_identity(_TEMPLATE, _PARAMETERS)
_SEMANTICS: dict[str, object] = {
    "pipeline_method_version": "strategy-research-pipeline-v14",
    "panel_method_version": "trade-bars-pit-v2",
    "panel_schema_version": 2,
    "feature_method_version": "research-features-v2",
    "trade_flow_input_method_version": "economic-trade-basis-v1",
    "trade_input_receipt_method_version": "active-trade-head-receipt-v2",
    "operational_gate_method_version": "economic-trade-operational-gate-v1",
}


def _time(value: str) -> datetime:
    """构造测试 UTC 时间。"""
    return datetime.fromisoformat(value).replace(tzinfo=UTC)


_TEST_NOW = _time("2026-08-14T00:00:00")


@pytest.fixture(autouse=True)
def _authoritative_test_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    """测试只替换内部壁钟；低层测试用最小伪制品。"""
    global _TEST_NOW
    _TEST_NOW = _time("2026-08-14T00:00:00")
    monkeypatch.setattr(clock, "utc_now", lambda: _TEST_NOW)
    monkeypatch.setattr(
        "guvolu.research.frozen_forward.attest_frozen_prediction_artifact",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "guvolu.research.holdout.attest_holdout_terminal_artifacts",
        lambda *_args: None,
    )


def _set_now(value: datetime) -> None:
    global _TEST_NOW
    _TEST_NOW = value


def _write_plan(
    root: Path,
    vintage_id: str,
    source_manifest_sha256: str,
    candidate_set_hash: str,
    config_hash: str,
    code_tree_digest: str,
    *,
    missing_policy: str | None,
    extra: dict[str, object] | None = None,
) -> tuple[str, str, str]:
    """写入冻结前向计划制品；政策为 None 时模拟无字段的旧制品。"""
    plan_id = stable_identifier(
        "frozen-forward-plan",
        frozen_forward_plan_identity(
            vintage_id,
            source_manifest_sha256,
            candidate_set_hash,
            config_hash,
            code_tree_digest,
            _SEMANTICS,
            missing_policy,
        ),
    )
    payload: dict[str, object] = {
        "schema_version": FROZEN_FORWARD_SCHEMA_VERSION,
        "method_version": FROZEN_FORWARD_METHOD_VERSION,
        "governance_method_version": GOVERNANCE_METHOD_VERSION,
        **_SEMANTICS,
        "scope": "FROZEN_FORWARD",
        "plan_id": plan_id,
        "vintage": {"vintage_id": vintage_id},
        "source": {"manifest_sha256": source_manifest_sha256},
        "candidate_set_hash": candidate_set_hash,
        "config_hash": config_hash,
        "code_identity": {"tree_digest": code_tree_digest},
        "code_tree_digest": code_tree_digest,
        "candidates": [{
            "candidate_id": _CANDIDATE_ID,
            "family": "trend",
            "mode": "paper",
            "expression_id": _EXPRESSION_ID,
            "parameters": _PARAMETERS,
            "complexity": len(_PARAMETERS),
        }],
        "allocation": {"weights": {"trend": 0.4}, "reserve": 0.6},
        **(extra or {}),
    }
    if missing_policy is not None:
        payload["missing_policy"] = missing_policy
    path = root / "reports" / plan_id / "plan.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(canonical_json(payload) + "\n", encoding="utf-8")
    return plan_id, path.relative_to(root).as_posix(), sha256_file(path)


def _write_prediction(
    root: Path,
    plan_id: str,
    vintage_id: str,
    decision_time: datetime,
    config_hash: str,
    code_tree_digest: str,
    *,
    target: float = 0.5,
) -> tuple[str, str]:
    """写入与登记合同一致的冻结前向预测制品。"""
    prediction_id = stable_identifier("frozen-forward-prediction", {
        "governance_method_version": GOVERNANCE_METHOD_VERSION,
        "plan_id": plan_id,
        "decision_time": decision_time.isoformat(),
    })
    stamp = decision_time.strftime("%Y%m%dT%H%M%SZ")
    path = root / "reports" / plan_id / "predictions" / f"{stamp}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    receipt_path = (
        root / "data" / "research" / "input-receipts" / f"receipt-{stamp}.json"
    )
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    receipt_path.write_text(json.dumps({"stamp": stamp}) + "\n", encoding="utf-8")
    receipt_sha256 = sha256_file(receipt_path)
    path.write_text(canonical_json({
        "schema_version": FROZEN_FORWARD_SCHEMA_VERSION,
        "method_version": FROZEN_FORWARD_METHOD_VERSION,
        "scope": "FROZEN_FORWARD",
        "prediction_id": prediction_id,
        "plan_id": plan_id,
        "vintage_id": vintage_id,
        "decision_time": decision_time.isoformat(),
        "input_head_generation": "head-one",
        "panel_sha256": f"panel-{stamp}",
        "config_hash": config_hash,
        "input_receipt_sha256": receipt_sha256,
        "input_receipt": {
            "path": receipt_path.relative_to(root).as_posix(),
            "sha256": receipt_sha256,
            "bytes": receipt_path.stat().st_size,
        },
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
            "candidate_id": _CANDIDATE_ID,
            "family": "trend",
            "family_target": target,
            "frozen_allocation_weight": 0.4,
            "portfolio_target_contribution": 0.4 * target,
        }],
        "reserve": 0.6,
        "aggregate_target": 0.4 * target,
        "unit": "risk_weighted_directional_target",
    }) + "\n", encoding="utf-8")
    return path.relative_to(root).as_posix(), sha256_file(path)


def _register_prediction(
    registry: Path,
    root: Path,
    plan_id: str,
    vintage_id: str,
    decision_time: datetime,
    config_hash: str,
    code_tree_digest: str,
) -> None:
    """在登记时效内追加一个冻结预测。"""
    path, sha256 = _write_prediction(
        root, plan_id, vintage_id, decision_time, config_hash, code_tree_digest,
    )
    stamp = decision_time.strftime("%Y%m%dT%H%M%SZ")
    _set_now(decision_time + timedelta(minutes=1))
    register_frozen_forward_prediction(
        registry,
        plan_id,
        decision_time,
        "head-one",
        f"panel-{stamp}",
        path,
        sha256,
        3900,
        repository_root=root,
    )


def test_plan_identity_and_registry_bind_missing_policy(tmp_path: Path) -> None:
    """政策进入 plan_id 身份并登记入治理行；旧制品无字段读为 burn。"""
    base = dict(
        vintage_id="vintage-one",
        source_manifest_sha256="1" * 64,
        candidate_set_hash="candidate-set-one",
        config_hash="2" * 64,
        code_tree_digest="tree-one",
        semantics=_SEMANTICS,
    )
    legacy = frozen_forward_plan_identity(**base, missing_policy=None)
    burn = frozen_forward_plan_identity(**base, missing_policy="burn")
    zero = frozen_forward_plan_identity(**base, missing_policy="zero_exposure")
    assert "missing_policy" not in legacy
    assert len({
        stable_identifier("frozen-forward-plan", legacy),
        stable_identifier("frozen-forward-plan", burn),
        stable_identifier("frozen-forward-plan", zero),
    }) == 3
    with pytest.raises(ValueError, match="missing_policy 取值无效"):
        frozen_forward_plan_identity(**base, missing_policy="retry")

    registry = tmp_path / "governance.sqlite3"
    zero_vintage = seal_holdout_vintage(
        registry, "market-one",
        _time("2027-01-01T00:00:00"), _time("2027-02-01T00:00:00"),
    )
    legacy_vintage = seal_holdout_vintage(
        registry, "market-one",
        _time("2027-03-01T00:00:00"), _time("2027-04-01T00:00:00"),
    )
    zero_plan_id, zero_path, zero_sha = _write_plan(
        tmp_path, zero_vintage.vintage_id, "1" * 64, "candidate-set-one",
        "2" * 64, "tree-one", missing_policy="zero_exposure",
    )
    registered = register_frozen_forward_plan(
        registry, zero_vintage.vintage_id, "1" * 64, "candidate-set-one",
        "2" * 64, "tree-one", zero_path, zero_sha, repository_root=tmp_path,
    )
    assert registered.plan_id == zero_plan_id
    assert registered.missing_policy == "zero_exposure"
    assert get_frozen_forward_plan(registry, zero_plan_id).missing_policy == (
        "zero_exposure"
    )
    legacy_plan_id, legacy_path, legacy_sha = _write_plan(
        tmp_path, legacy_vintage.vintage_id, "1" * 64, "candidate-set-one",
        "2" * 64, "tree-one", missing_policy=None,
    )
    legacy_plan = register_frozen_forward_plan(
        registry, legacy_vintage.vintage_id, "1" * 64, "candidate-set-one",
        "2" * 64, "tree-one", legacy_path, legacy_sha, repository_root=tmp_path,
    )
    assert legacy_plan.plan_id == legacy_plan_id
    assert legacy_plan.missing_policy == DEFAULT_MISSING_POLICY
    with sqlite3.connect(registry) as connection:
        rows = connection.execute(
            "SELECT plan_id,missing_policy FROM frozen_forward_plan "
            "ORDER BY vintage_id"
        ).fetchall()
    assert dict(rows) == {
        zero_plan_id: "zero_exposure", legacy_plan_id: "burn",
    }

    # 事后增补政策改变制品散列与身份，复核拒绝
    legacy_file = tmp_path / legacy_path
    payload = json.loads(legacy_file.read_text(encoding="utf-8"))
    payload["missing_policy"] = "zero_exposure"
    legacy_file.write_text(canonical_json(payload) + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="计划制品散列不匹配"):
        verify_frozen_forward(tmp_path, legacy_plan_id, registry_path=registry)
    _amended_id, amended_path, amended_sha = _write_plan(
        tmp_path, legacy_vintage.vintage_id, "1" * 64, "candidate-set-one",
        "2" * 64, "tree-one", missing_policy="zero_exposure",
    )
    with pytest.raises(ValueError, match="已绑定不同的冻结前向计划"):
        register_frozen_forward_plan(
            registry, legacy_vintage.vintage_id, "1" * 64, "candidate-set-one",
            "2" * 64, "tree-one", amended_path, amended_sha,
            repository_root=tmp_path,
        )
    with pytest.raises(ValueError, match="missing_policy 取值无效"):
        _write_plan(
            tmp_path, "vintage-x", "1" * 64, "candidate-set-one",
            "2" * 64, "tree-one", missing_policy="retry",
        )


def _downgrade_to_v7(registry: Path, *, ceiling: bool) -> None:
    """把治理库物理降回 schema v7，可选固定写入上限。"""
    with sqlite3.connect(registry) as connection:
        connection.execute(
            "ALTER TABLE frozen_forward_plan DROP COLUMN missing_policy"
        )
        connection.execute(
            "UPDATE governance_meta SET value='7' WHERE key='schema_version'"
        )
        if ceiling:
            connection.execute(
                "INSERT INTO governance_meta(key,value) VALUES(?,?)",
                ("schema_write_ceiling", "7"),
            )


def test_governance_v7_rows_migrate_to_burn(tmp_path: Path) -> None:
    """旧 v7 计划行在自动或显式升级后缺省 burn。"""
    registry = tmp_path / "governance.sqlite3"
    vintage = seal_holdout_vintage(
        registry, "market-one",
        _time("2027-01-01T00:00:00"), _time("2027-02-01T00:00:00"),
    )
    plan_id, path, sha256 = _write_plan(
        tmp_path, vintage.vintage_id, "1" * 64, "candidate-set-one",
        "2" * 64, "tree-one", missing_policy=None,
    )
    register_frozen_forward_plan(
        registry, vintage.vintage_id, "1" * 64, "candidate-set-one",
        "2" * 64, "tree-one", path, sha256, repository_root=tmp_path,
    )
    _downgrade_to_v7(registry, ceiling=False)
    assert get_frozen_forward_plan(registry, plan_id).missing_policy == "burn"
    with sqlite3.connect(registry) as connection:
        assert connection.execute(
            "SELECT value FROM governance_meta WHERE key='schema_version'"
        ).fetchone() == (str(GOVERNANCE_SCHEMA_VERSION),)
        assert connection.execute(
            "SELECT missing_policy FROM frozen_forward_plan"
        ).fetchall() == [("burn",)]

    _downgrade_to_v7(registry, ceiling=True)
    pinned = get_frozen_forward_plan_for_vintage(registry, vintage.vintage_id)
    assert pinned is not None and pinned.missing_policy == "burn"
    with sqlite3.connect(registry) as connection:
        assert connection.execute(
            "SELECT value FROM governance_meta WHERE key='schema_version'"
        ).fetchone() == ("7",)
    backup = tmp_path / "governance-v7.sqlite3.bak"
    upgrade_governance_write_ceiling(
        registry, backup, expected_version=7, expected_write_ceiling=7,
    )
    with sqlite3.connect(registry) as connection:
        assert connection.execute(
            "SELECT value FROM governance_meta WHERE key='schema_version'"
        ).fetchone() == (str(GOVERNANCE_SCHEMA_VERSION),)
        assert connection.execute(
            "SELECT missing_policy FROM frozen_forward_plan"
        ).fetchall() == [("burn",)]
    assert get_frozen_forward_plan(registry, plan_id).missing_policy == "burn"


def test_apply_missing_policy_zero_fills_or_burns() -> None:
    """zero_exposure 把缺失柱对全部候选记零；burn 缺柱抛错。"""
    t0, t1, t2 = (
        _time("2027-01-01T00:00:00"),
        _time("2027-01-01T01:00:00"),
        _time("2027-01-01T02:00:00"),
    )
    recorded = {t0: {"a": 0.5, "b": 0.25}, t2: {"a": 1.0, "b": 0.0}}
    filled, missing = apply_missing_policy(
        recorded, (t0, t1, t2), ("a", "b"), "zero_exposure",
    )
    assert missing == (t1,)
    assert filled[t1] == {"a": 0.0, "b": 0.0}
    assert filled[t0] == recorded[t0] and filled[t2] == recorded[t2]
    partial = {t0: {"a": 0.5}, t1: {"a": 0.5, "b": 0.5}}
    filled, missing = apply_missing_policy(
        partial, (t0, t1), ("a", "b"), "zero_exposure",
    )
    assert missing == (t0,) and filled[t0] == {"a": 0.0, "b": 0.0}
    with pytest.raises(ValueError, match="覆盖不完整: missing=1"):
        apply_missing_policy(recorded, (t0, t1, t2), ("a", "b"), "burn")
    complete, none_missing = apply_missing_policy(
        recorded, (t0, t2), ("a", "b"), "burn",
    )
    assert none_missing == () and complete == recorded
    with pytest.raises(ValueError, match="取值无效"):
        apply_missing_policy(recorded, (t0,), ("a",), "retry")


def test_verified_targets_follow_plan_policy(tmp_path: Path) -> None:
    """复核后的目标映射按计划政策补齐缺失柱。"""
    registry = tmp_path / "governance.sqlite3"
    vintage = seal_holdout_vintage(
        registry, "market-one",
        _time("2027-01-01T00:00:00"), _time("2027-02-01T00:00:00"),
    )
    plan_id, path, sha256 = _write_plan(
        tmp_path, vintage.vintage_id, "1" * 64, "candidate-set-one",
        "2" * 64, "tree-one", missing_policy="zero_exposure",
    )
    register_frozen_forward_plan(
        registry, vintage.vintage_id, "1" * 64, "candidate-set-one",
        "2" * 64, "tree-one", path, sha256, repository_root=tmp_path,
    )
    t0 = vintage.start_time
    t1 = t0 + timedelta(hours=1)
    t2 = t0 + timedelta(hours=2)
    for decision in (t0, t2):
        _register_prediction(
            registry, tmp_path, plan_id, vintage.vintage_id, decision,
            "2" * 64, "tree-one",
        )
    verification = verify_frozen_forward(tmp_path, plan_id, registry_path=registry)
    assert verification.prediction_count == 2
    assert verification.missing_policy == "zero_exposure"
    raw = load_verified_prediction_targets(tmp_path, plan_id, registry_path=registry)
    assert set(raw) == {t0, t2}
    filled = load_verified_prediction_targets(
        tmp_path, plan_id, registry_path=registry, decision_times=(t0, t1, t2),
    )
    assert filled[t1] == {_CANDIDATE_ID: 0.0}
    assert filled[t0] == {_CANDIDATE_ID: 0.5}


def _holdout_config(root: Path, *, require_forward: bool) -> tuple[Path, dict[str, object]]:
    """写入带 holdout_policy 的配置。"""
    policy: dict[str, object] = {
        "minimum_bars": 1,
        "minimum_sharpe": 0.0,
        "maximum_drawdown": 0.45,
        "maximum_fdr_q": 0.2,
        "require_frozen_forward_predictions": require_forward,
    }
    config_path = root / "config" / "holdout-test.json"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(canonical_json({
        "data_governance": {"holdout_policy": policy},
    }) + "\n", encoding="utf-8")
    return config_path, policy


def _evaluation_identity(
    root: Path,
    registry: Path,
    vintage_id: str,
    candidate_set_hash: str,
    config_hash: str,
) -> tuple[dict[str, object], str]:
    """生成 evaluation 身份并登记 holdout 活动收据。"""
    receipt_path = root / "data" / "research" / "input-receipts" / "holdout.json"
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    receipt_path.write_text("{}\n", encoding="utf-8")
    receipt_sha256 = sha256_file(receipt_path)
    identity: dict[str, object] = {
        "holdout_method_version": HOLDOUT_METHOD_VERSION,
        "governance_method_version": GOVERNANCE_METHOD_VERSION,
        "vintage_id": vintage_id,
        "candidate_set_hash": candidate_set_hash,
        "config_hash": config_hash,
        "code_tree_digest": "tree-one",
        "input_head_generation": "head-one",
        "input_attempt_ids": ["attempt-one"],
        "input_artifact_ids": ["artifact-one"],
        "normalization_versions": ["normalization-one"],
        "input_receipt_sha256": receipt_sha256,
    }
    evaluation_id = stable_identifier("holdout-evaluation", identity)
    with sqlite3.connect(registry) as connection:
        connection.execute(
            "INSERT OR REPLACE INTO active_head_receipt("
            "consumer_kind,consumer_id,market_id,head_generation,"
            "receipt_artifact_path,receipt_artifact_sha256,recorded_at"
            ") VALUES('holdout',?,?,?,?,?,?)",
            (
                evaluation_id, "market-one", "head-one",
                receipt_path.relative_to(root).as_posix(), receipt_sha256,
                _TEST_NOW.isoformat(),
            ),
        )
    return identity, evaluation_id


def _write_evidence(
    root: Path,
    registry: Path,
    vintage_id: str,
    evaluation_identity: dict[str, object],
    candidate_set_identity: dict[str, object],
    policy: dict[str, object],
    config_path: Path,
    score_decision_times: tuple[datetime, ...],
    plan_id: str,
    *,
    missing_policy: str,
    missing_decision_times: tuple[datetime, ...],
    declare_missing: bool = True,
) -> tuple[str, str, str]:
    """写入互相绑定的 holdout result、schedule、manifest 与 verdict。"""
    evaluation_id = stable_identifier("holdout-evaluation", evaluation_identity)
    run_directory = root / "reports" / "holdout" / evaluation_id
    run_directory.mkdir(parents=True, exist_ok=True)
    panel_path = run_directory / "panel.parquet"
    panel_path.write_bytes(b"PAR1holdout-test-panelPAR1")
    schedule_path = run_directory / "score-schedule.json"
    schedule_path.write_text(canonical_json({
        "schema_version": HOLDOUT_MANIFEST_SCHEMA_VERSION,
        "holdout_method_version": HOLDOUT_METHOD_VERSION,
        "evaluation_id": evaluation_id,
        "decision_times": [value.isoformat() for value in score_decision_times],
    }) + "\n", encoding="utf-8")
    candidate_set_hash = stable_identifier("candidate-set", candidate_set_identity)
    row_set_hash = get_frozen_forward_prediction_row_set(registry, plan_id)[0]
    prediction_count = len(score_decision_times) - len(missing_decision_times)
    metrics = {
        "net_return": 0.1, "sharpe": 1.0, "maximum_drawdown": 0.1, "p_value": 0.01,
    }
    result: dict[str, object] = {
        "schema_version": HOLDOUT_MANIFEST_SCHEMA_VERSION,
        "holdout_method_version": HOLDOUT_METHOD_VERSION,
        "evaluation_id": evaluation_id,
        "vintage": {"vintage_id": vintage_id},
        "candidate_set_hash": candidate_set_hash,
        "config_hash": sha256_file(config_path),
        "panel_sha256": sha256_file(panel_path),
        "score_schedule_sha256": sha256_file(schedule_path),
        "candidate_results": [{
            "candidate_id": _CANDIDATE_ID,
            "family": "trend",
            "metrics": metrics,
            "fdr_q": metrics["p_value"],
            "passed": True,
            "rejection_reasons": [],
        }],
        "score_start": score_decision_times[0].isoformat(),
        "score_end": score_decision_times[-1].isoformat(),
        "score_bars": len(score_decision_times),
        "target_source": "recorded_frozen_forward",
        "frozen_forward_plan_id": plan_id,
        "frozen_forward_prediction_count": prediction_count,
        "frozen_forward_row_set_hash": row_set_hash,
        "policy": policy,
        "passed_families": ["trend"],
        "verdict": "passed",
    }
    if declare_missing:
        result["missing_policy"] = missing_policy
        result["missing_decision_times"] = [
            value.isoformat() for value in missing_decision_times
        ]
        result["missing_decision_count"] = len(missing_decision_times)
    result_path = run_directory / "result.json"
    result_path.write_text(canonical_json(result) + "\n", encoding="utf-8")
    receipt_path = root / "data" / "research" / "input-receipts" / "holdout.json"

    def artifact(path: Path, kind: str) -> dict[str, object]:
        return {
            "kind": kind,
            "path": path.relative_to(root).as_posix(),
            "sha256": sha256_file(path),
            "bytes": path.stat().st_size,
        }

    manifest: dict[str, object] = {
        "schema_version": HOLDOUT_MANIFEST_SCHEMA_VERSION,
        "holdout_method_version": HOLDOUT_METHOD_VERSION,
        "evaluation_id": evaluation_id,
        "vintage_id": vintage_id,
        "candidate_set_hash": candidate_set_hash,
        "candidate_set_identity": candidate_set_identity,
        "evaluation_identity": evaluation_identity,
        "input_head_generation": "head-one",
        "input_attempt_ids": ["attempt-one"],
        "input_artifact_ids": ["artifact-one"],
        "normalization_versions": ["normalization-one"],
        "frozen_forward_row_set_hash": row_set_hash,
        "input_receipt_sha256": sha256_file(receipt_path),
        "verdict": "passed",
        "artifacts": {
            "config": artifact(config_path, "holdout_config"),
            "input_receipt": artifact(receipt_path, "active_trade_head_receipt"),
            "panel": artifact(panel_path, "holdout_panel"),
            "score_schedule": artifact(schedule_path, "holdout_score_schedule"),
            "result": artifact(result_path, "holdout_result"),
        },
    }
    if declare_missing:
        manifest["missing_policy"] = missing_policy
        manifest["missing_decision_count"] = len(missing_decision_times)
    manifest_path = run_directory / "manifest.json"
    manifest_path.write_text(canonical_json(manifest) + "\n", encoding="utf-8")
    terminal = canonical_json({
        "evaluation_id": evaluation_id,
        "verdict": "passed",
        "candidate_ids": [_CANDIDATE_ID],
        "passed_families": ["trend"],
        "result_sha256": sha256_file(result_path),
        "manifest_sha256": sha256_file(manifest_path),
    })
    return terminal, manifest_path.relative_to(root).as_posix(), sha256_file(
        manifest_path,
    )


@pytest.mark.parametrize("missing_policy", ["zero_exposure", "burn"])
def test_finalize_coverage_rule_follows_plan_policy(
    tmp_path: Path, missing_policy: str,
) -> None:
    """终态登记：zero_exposure 接受声明缺失柱，burn 仍要求逐柱覆盖。"""
    registry = tmp_path / "governance.sqlite3"
    vintage = seal_holdout_vintage(
        registry, "market-one",
        _time("2027-01-01T00:00:00"), _time("2027-02-01T00:00:00"),
    )
    candidate_set_identity: dict[str, object] = {
        "holdout_method_version": HOLDOUT_METHOD_VERSION,
        "source_manifest_sha256": "1" * 64,
        "source_summary_sha256": "2" * 64,
        "candidate_registry_sha256": "3" * 64,
        "candidate_ids": [_CANDIDATE_ID],
    }
    candidate_set_hash = stable_identifier("candidate-set", candidate_set_identity)
    config_path, policy = _holdout_config(tmp_path, require_forward=True)
    config_hash = sha256_file(config_path)
    plan_id, path, sha256 = _write_plan(
        tmp_path, vintage.vintage_id, "1" * 64, candidate_set_hash,
        config_hash, "tree-one", missing_policy=missing_policy,
    )
    register_frozen_forward_plan(
        registry, vintage.vintage_id, "1" * 64, candidate_set_hash,
        config_hash, "tree-one", path, sha256, repository_root=tmp_path,
    )
    t0 = vintage.start_time
    t1 = t0 + timedelta(hours=1)
    t2 = t0 + timedelta(hours=2)
    for decision in (t0, t2):
        _register_prediction(
            registry, tmp_path, plan_id, vintage.vintage_id, decision,
            config_hash, "tree-one",
        )
    _set_now(_time("2027-02-02T00:00:00"))
    evaluation_identity, evaluation_id = _evaluation_identity(
        tmp_path, registry, vintage.vintage_id, candidate_set_hash, config_hash,
    )
    start_holdout_evaluation_attempt(
        registry, vintage.vintage_id, candidate_set_hash, evaluation_id,
    )
    if missing_policy == "burn":
        terminal, manifest_path, manifest_sha256 = _write_evidence(
            tmp_path, registry, vintage.vintage_id, evaluation_identity,
            candidate_set_identity, policy, config_path, (t0, t1, t2), plan_id,
            missing_policy=missing_policy, missing_decision_times=(t1,),
        )
        with pytest.raises(ValueError, match="burn 政策不得声明缺失决策柱"):
            finalize_holdout_evaluation(
                registry, vintage.vintage_id, evaluation_id, terminal,
                manifest_path, manifest_sha256, repository_root=tmp_path,
            )
        terminal, manifest_path, manifest_sha256 = _write_evidence(
            tmp_path, registry, vintage.vintage_id, evaluation_identity,
            candidate_set_identity, policy, config_path, (t0, t1, t2), plan_id,
            missing_policy=missing_policy, missing_decision_times=(t1,),
            declare_missing=False,
        )
        with pytest.raises(ValueError, match="必须完整等于评分柱数"):
            finalize_holdout_evaluation(
                registry, vintage.vintage_id, evaluation_id, terminal,
                manifest_path, manifest_sha256, repository_root=tmp_path,
            )
        return
    # 未声明缺失时预测数与评分柱不等，仍拒绝
    undeclared, undeclared_path, undeclared_sha = _write_evidence(
        tmp_path, registry, vintage.vintage_id, evaluation_identity,
        candidate_set_identity, policy, config_path, (t0, t1, t2), plan_id,
        missing_policy=missing_policy, missing_decision_times=(t1,),
        declare_missing=False,
    )
    with pytest.raises(ValueError, match="必须完整等于评分柱数"):
        finalize_holdout_evaluation(
            registry, vintage.vintage_id, evaluation_id, undeclared,
            undeclared_path, undeclared_sha, repository_root=tmp_path,
        )
    # 声明错误缺失时点（已有预测的柱）被拒绝
    wrong, wrong_path, wrong_sha = _write_evidence(
        tmp_path, registry, vintage.vintage_id, evaluation_identity,
        candidate_set_identity, policy, config_path, (t0, t1, t2), plan_id,
        missing_policy=missing_policy, missing_decision_times=(t2,),
    )
    with pytest.raises(ValueError, match="未逐柱匹配"):
        finalize_holdout_evaluation(
            registry, vintage.vintage_id, evaluation_id, wrong,
            wrong_path, wrong_sha, repository_root=tmp_path,
        )
    terminal, manifest_path, manifest_sha256 = _write_evidence(
        tmp_path, registry, vintage.vintage_id, evaluation_identity,
        candidate_set_identity, policy, config_path, (t0, t1, t2), plan_id,
        missing_policy=missing_policy, missing_decision_times=(t1,),
    )
    finalized, attempt = finalize_holdout_evaluation(
        registry, vintage.vintage_id, evaluation_id, terminal,
        manifest_path, manifest_sha256, repository_root=tmp_path,
    )
    assert finalized.verdict == terminal
    assert attempt.status == "completed"


def _build_holdout_fixture(
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    missing_policy: str,
) -> tuple[
    Path, Path, Path, str, tuple[datetime, ...], dict[str, tuple[float, ...]],
]:
    """搭建可真实消费并终态登记的 holdout 运行夹具。"""
    registry = root / "data" / "research" / "governance.sqlite3"
    vintage = seal_holdout_vintage(
        registry, "market-one",
        _time("2027-01-01T00:00:00"), _time("2027-02-01T00:00:00"),
    )
    config = root / "config.json"
    config.write_text(canonical_json({
        "market_id": "market-one",
        "bar_interval": "1hour",
        "from_time": "2020-01-01T00:00:00+00:00",
        "notional_scale": 100_000_000,
        "features": {
            "lookbacks": [1],
            "volume_lookback": 1,
            "maximum_structural_gap_bars_assumption": 1,
        },
        "cost_model": {
            "fee_bps_assumption": 1.0,
            "half_spread_bps_assumption": 1.0,
            "slippage_bps_assumption": 1.0,
            "impact_bps_assumption": 1.0,
            "capacity_notional_quote": 1_000.0,
        },
        "data_governance": {
            "registry": "data/research/governance.sqlite3",
            "holdout_policy": {
                "minimum_bars": 1,
                "minimum_sharpe": 0.0,
                "maximum_drawdown": 0.45,
                "maximum_fdr_q": 0.2,
                "require_frozen_forward_predictions": True,
            },
        },
    }) + "\n", encoding="utf-8")
    config_hash = sha256_file(config)
    config_snapshot = snapshot_verified_config_lineage(
        root, config, root / "reports" / "config-artifacts",
    )
    candidate_registry = root / "reports" / "candidate-registry.json"
    candidate_registry.parent.mkdir(parents=True, exist_ok=True)
    candidate_registry.write_text(canonical_json({
        "config_hash": config_hash,
        "expression_method_version": EXPRESSION_METHOD_VERSION,
        "candidates": [{
            "candidate_id": _CANDIDATE_ID,
            "complexity": 5,
            "expression_id": _EXPRESSION_ID,
            "family": "trend",
            "mode": "paper",
            "parameters": _PARAMETERS,
        }],
    }), encoding="utf-8")
    registry_record = {
        "path": "reports/candidate-registry.json",
        "sha256": sha256_file(candidate_registry),
        "bytes": candidate_registry.stat().st_size,
    }
    summary = root / "reports" / "summary.json"
    summary.write_text(canonical_json({
        "pipeline_method_version": "strategy-research-pipeline-v14",
        "operational_gate_method_version": "economic-trade-operational-gate-v1",
        "run_id": "run-one",
        "research_identity": "research-one",
        "config_hash": config_hash,
        "decision_grade": True,
        "code_identity": {"git_hash": "source-commit", "tree_digest": "tree-one"},
        "family_scope": ["trend", "breakout"],
        "family_evaluations": [{
            "deployment_candidate_id": _CANDIDATE_ID,
            "eligible": True,
            "mode": "paper",
        }],
        "artifacts": {"candidate_registry": registry_record},
    }), encoding="utf-8")
    manifest = root / "reports" / "manifest.json"
    manifest.write_text(canonical_json({
        "run_id": "run-one",
        "research_identity": "research-one",
        "config_hash": config_hash,
        "artifacts": {
            "config": {
                "path": config_snapshot.leaf_config_path.relative_to(root).as_posix(),
                "sha256": config_snapshot.leaf_config_sha256,
                "bytes": config_snapshot.leaf_config_path.stat().st_size,
            },
            "config_lineage": {
                "path": config_snapshot.bundle_path.relative_to(root).as_posix(),
                "sha256": config_snapshot.bundle_sha256,
                "bytes": config_snapshot.bundle_path.stat().st_size,
            },
            "candidate_registry": registry_record,
            "summary_json": {
                "path": "reports/summary.json",
                "sha256": sha256_file(summary),
                "bytes": summary.stat().st_size,
            },
        },
    }), encoding="utf-8")
    monkeypatch.setattr(
        "guvolu.research.holdout.verify_research_run",
        lambda _root, _manifest: VerificationResult(
            run_id="run-one",
            manifest_path=manifest,
            manifest_sha256=sha256_file(manifest),
            checked_artifacts=("candidate_registry", "summary_json"),
        ),
    )
    candidate_set_hash = stable_identifier("candidate-set", {
        "holdout_method_version": HOLDOUT_METHOD_VERSION,
        "source_manifest_sha256": sha256_file(manifest),
        "source_summary_sha256": sha256_file(summary),
        "candidate_registry_sha256": sha256_file(candidate_registry),
        "candidate_ids": [_CANDIDATE_ID],
    })
    plan_id, plan_path, plan_sha = _write_plan(
        root, vintage.vintage_id, sha256_file(manifest), candidate_set_hash,
        config_hash, "tree-one", missing_policy=missing_policy,
    )
    register_frozen_forward_plan(
        registry, vintage.vintage_id, sha256_file(manifest), candidate_set_hash,
        config_hash, "tree-one", plan_path, plan_sha, repository_root=root,
    )
    t0 = vintage.start_time
    decisions = tuple(t0 + timedelta(hours=offset) for offset in range(4))
    # 第二柱 t1 无预测
    for decision in (decisions[0], decisions[2]):
        _register_prediction(
            registry, root, plan_id, vintage.vintage_id, decision,
            config_hash, "tree-one",
        )
    receipt_path = root / "data" / "research" / "input-receipts" / "holdout.json"
    receipt_path.write_text("{}\n", encoding="utf-8")
    receipt_sha256 = sha256_file(receipt_path)
    inputs = FrozenPanelInputs(
        market={"market_id": "market-one"},
        paths=(),
        head_generation="head-one",
        attempt_ids=("attempt-one",),
        artifact_ids=("artifact-one",),
        normalization_versions=("normalization-one",),
        maximum_event_time=_time("2027-03-01T00:00:00"),
        receipt_path=receipt_path,
        receipt_sha256=receipt_sha256,
    )
    monkeypatch.setattr(
        "guvolu.research.holdout.capture_trade_input_receipt",
        lambda _root, _market, _output: inputs,
    )
    monkeypatch.setattr(
        "guvolu.research.panel.attest_trade_input_receipt",
        lambda *_args, **_kwargs: inputs,
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
    panel_file = root / "data" / "research" / "holdout-panel.parquet"
    panel_file.write_bytes(b"PAR1holdout-panelPAR1")
    bars = tuple(
        ResearchBar(
            open_time=decision - timedelta(hours=1),
            decision_time=decision,
            latest_available_time=decision,
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            base_volume=1.0,
            quote_volume=100.0,
            signed_base_volume=0.2,
            trade_count=1,
        )
        for decision in decisions
    )
    monkeypatch.setattr(
        "guvolu.research.holdout.build_panel_snapshot",
        lambda *_args: PanelSnapshot(
            market={"market_id": "market-one"},
            bars=bars,
            head_generation="head-one",
            attempt_ids=("attempt-one",),
            artifact_ids=("artifact-one",),
            normalization_versions=("normalization-one",),
            panel_path=panel_file,
            panel_sha256=sha256_file(panel_file),
            decision_time=decisions[-1],
            latest_available_time=decisions[-1],
        ),
    )
    monkeypatch.setattr(
        "guvolu.research.holdout.compute_features",
        lambda *_args: tuple(object() for _ in decisions),
    )
    metrics = PerformanceMetrics(
        bars=3,
        net_return=0.1,
        annual_return=0.1,
        annual_volatility=0.2,
        sharpe=1.0,
        maximum_drawdown=0.1,
        turnover=1.0,
        annual_turnover=8_760.0,
        hit_rate=0.5,
        exposure=0.5,
        cost=0.01,
        p_value=0.01,
        capacity_score=1.0,
    )
    captured: dict[str, tuple[float, ...]] = {}

    def fake_evaluate(
        _bars: object, targets: tuple[float, ...], *_args: object,
    ) -> PerformanceMetrics:
        captured["targets"] = tuple(targets)
        return metrics

    monkeypatch.setattr("guvolu.research.holdout.evaluate_targets", fake_evaluate)
    _set_now(_time("2027-02-02T00:00:00"))
    return registry, config, summary, vintage.vintage_id, decisions, captured


def test_run_holdout_zero_exposure_scores_missing_bar_as_zero(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """zero_exposure 下缺失柱不烧毁，目标记零并登记缺失时点。"""
    registry, config, summary, vintage_id, decisions, captured = (
        _build_holdout_fixture(tmp_path, monkeypatch, missing_policy="zero_exposure")
    )
    result = run_holdout_validation(tmp_path, config, summary, vintage_id)
    # 缺失柱 t1 目标为零
    assert captured["targets"] == (0.5, 0.0, 0.5, 0.0)
    payload = json.loads(result.result_path.read_text(encoding="utf-8"))
    assert payload["missing_policy"] == "zero_exposure"
    assert payload["missing_decision_times"] == [decisions[1].isoformat()]
    assert payload["missing_decision_count"] == 1
    assert payload["frozen_forward_prediction_count"] == 2
    assert payload["score_bars"] == 3
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["missing_policy"] == "zero_exposure"
    assert manifest["missing_decision_count"] == 1
    consumed = list_holdout_vintages(registry)[0]
    assert consumed.status == "consumed"
    assert consumed.verdict is not None
    assert json.loads(consumed.verdict)["missing_decision_count"] == 1
    attempt = get_holdout_evaluation_attempt(registry, result.evaluation_id)
    assert attempt.status == "completed"


def test_run_holdout_burn_policy_still_burns_on_gap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """burn 政策缺柱仍抛错并永久烧毁 vintage。"""
    registry, config, summary, vintage_id, _decisions, _captured = (
        _build_holdout_fixture(tmp_path, monkeypatch, missing_policy="burn")
    )
    with pytest.raises(ValueError, match="覆盖不完整: missing=1"):
        run_holdout_validation(tmp_path, config, summary, vintage_id)
    consumed = list_holdout_vintages(registry)[0]
    assert consumed.status == "consumed"
    assert consumed.verdict is None
    assert consumed.evaluation_id is not None
    attempt = get_holdout_evaluation_attempt(registry, consumed.evaluation_id)
    assert attempt.status == "incomplete"


def _load_preflight() -> ModuleType:
    """按路径加载只读预检脚本。"""
    path = Path(__file__).resolve().parents[1] / "scripts" / "preflight_holdout.py"
    spec = importlib.util.spec_from_file_location("preflight_holdout", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    ("missing_policy", "expected_status"),
    [("zero_exposure", "degraded"), ("burn", "would_burn")],
)
def test_preflight_coverage_gap_severity_follows_policy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    missing_policy: str,
    expected_status: str,
) -> None:
    """zero_exposure 下覆盖缺口降为 warning，burn 仍为 blocker。"""
    preflight = _load_preflight()
    registry = tmp_path / "governance.sqlite3"
    vintage = seal_holdout_vintage(
        registry, "market-one",
        _time("2027-01-01T00:00:00"), _time("2027-02-01T00:00:00"),
    )
    _plan_id, path, sha256 = _write_plan(
        tmp_path, vintage.vintage_id, "1" * 64, "candidate-set-one",
        "2" * 64, "tree-one", missing_policy=missing_policy,
    )
    register_frozen_forward_plan(
        registry, vintage.vintage_id, "1" * 64, "candidate-set-one",
        "2" * 64, "tree-one", path, sha256, repository_root=tmp_path,
    )
    monkeypatch.setattr(
        preflight, "_utc_now",
        lambda: vintage.start_time + timedelta(hours=6),
    )
    report = preflight.run_preflight(
        tmp_path, registry, vintage.vintage_id, verify_artifacts=False,
    )
    assert report["missing_policy"] == missing_policy
    assert report["status"] == expected_status
    gap_issues = [
        item["issue"] for item in (
            report["warnings"] if missing_policy == "zero_exposure"
            else report["blockers"]
        )
    ]
    assert "prediction_coverage_gap" in gap_issues
    if missing_policy == "zero_exposure":
        assert report["blockers"] == []
    assert report["registered_predictions"] == 0
    assert report["expected_predictions"] > 0
