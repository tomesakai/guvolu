"""研究数据暴露与一次性封存段治理测试。"""
from __future__ import annotations

import inspect
import json
import sqlite3
import threading
import time
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import guvolu.research.governance as governance_module
from guvolu.research import clock
from guvolu.research.contracts import (
    FROZEN_FORWARD_METHOD_VERSION,
    FROZEN_FORWARD_SCHEMA_VERSION,
    HOLDOUT_MANIFEST_SCHEMA_VERSION,
    HOLDOUT_METHOD_VERSION,
    INTERVAL_SUITE_FORWARD_METHOD_VERSION,
    INTERVAL_SUITE_FORWARD_SCHEMA_VERSION,
    INTERVAL_SUITE_PREDICTION_METHOD_VERSION,
    INTERVAL_SUITE_PREDICTION_SCHEMA_VERSION,
    CodeIdentity,
    FrozenPanelInputs,
    PanelSnapshot,
    PerformanceMetrics,
)
from guvolu.research.config_lineage import (
    load_governed_strategy_config_with_paths,
    snapshot_verified_config_lineage,
)
from guvolu.research.data_location import data_root_locator
from guvolu.research.frozen_forward import (
    attest_frozen_forward_batch,
    attest_frozen_prediction_artifact as _actual_forward_attestation,
    freeze_forward_plan,
    run_frozen_forward_prediction,
)
from guvolu.research.governance import (
    ActiveHeadReceiptRegistration,
    GOVERNANCE_METHOD_VERSION,
    GOVERNANCE_SCHEMA_VERSION,
    finalize_holdout_evaluation,
    get_holdout_evaluation_attempt,
    get_holdout_vintage,
    get_frozen_forward_plan_for_vintage,
    get_frozen_forward_prediction_row_set,
    get_interval_suite_forward_plan_for_vintage,
    get_interval_suite_forward_prediction_row_set,
    list_interval_suite_forward_predictions,
    list_frozen_forward_predictions,
    list_holdout_vintages,
    register_frozen_forward_plan,
    register_frozen_forward_prediction,
    register_interval_suite_forward_plan,
    register_interval_suite_forward_prediction,
    register_research_exposure,
    seal_holdout_vintage,
    start_holdout_evaluation_attempt,
    upgrade_governance_write_ceiling,
)
from guvolu.research.holdout import (
    _score_decision_times,
    attest_holdout_terminal_artifacts as _actual_holdout_attestation,
    run_holdout_validation,
)
from guvolu.research.interval_suite import build_interval_suite_plan
from guvolu.research.interval_suite_forward_identity import (
    interval_suite_deployment_contract_id,
    interval_suite_forward_plan_id,
)
from guvolu.research.interval_suite_prediction_identity import (
    interval_suite_forward_prediction_id,
    interval_suite_member_panel_set_hash,
)
from guvolu.research.provenance import (
    canonical_json,
    sha256_file,
    sha256_text,
    stable_identifier,
)
from guvolu.research.verification import VerificationResult
from guvolu.strategy.expression import (
    EXPRESSION_METHOD_VERSION,
    candidate_identity,
    expression_id,
    strategy_expression,
)
from guvolu.strategy.contracts import CandidateSpec


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
    # 低层测试使用最小伪制品。
    # 完整重算由专项测试覆盖。
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
    receipt_path = (
        root / "data" / "research" / "input-receipts" / "holdout-test.json"
    )
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    receipt_path.write_text("{}\n", encoding="utf-8")
    receipt_sha256 = sha256_file(receipt_path)
    identity: dict[str, object] = {
        "holdout_method_version": HOLDOUT_METHOD_VERSION,
        "governance_method_version": GOVERNANCE_METHOD_VERSION,
        "vintage_id": vintage_id,
        "candidate_set_hash": candidate_set_hash,
        "config_hash": sha256_file(config_path),
        "code_tree_digest": "tree-one",
        "input_head_generation": "head-one",
        "input_attempt_ids": ["attempt-one"],
        "input_artifact_ids": ["artifact-one"],
        "normalization_versions": ["normalization-one"],
        "input_receipt_sha256": receipt_sha256,
    }
    evaluation_id = stable_identifier("holdout-evaluation", identity)
    registry_path = root / "governance.sqlite3"
    if registry_path.exists():
        connection = sqlite3.connect(registry_path)
        try:
            connection.execute(
                "INSERT OR REPLACE INTO active_head_receipt("
                "consumer_kind,consumer_id,market_id,head_generation,"
                "receipt_artifact_path,receipt_artifact_sha256,recorded_at"
                ") VALUES('holdout',?,?,?,?,?,?)",
                (
                    evaluation_id,
                    "market-one",
                    "head-one",
                    receipt_path.relative_to(root).as_posix(),
                    receipt_sha256,
                    _TEST_NOW.isoformat(),
                ),
            )
            connection.commit()
        finally:
            connection.close()
    return identity, evaluation_id


def _write_forward_plan_artifact(
    root: Path,
    vintage_id: str,
    source_manifest_sha256: str,
    candidate_set_hash: str,
    config_hash: str,
    code_tree_digest: str,
    *,
    legacy: bool = False,
) -> tuple[str, str, str]:
    """写入与注册合同一致的冻结前向计划制品。"""
    semantics: dict[str, object] = {} if legacy else {
        "pipeline_method_version": "strategy-research-pipeline-v13",
        "panel_method_version": "trade-bars-pit-v2",
        "panel_schema_version": 2,
        "feature_method_version": "research-features-v2",
        "trade_flow_input_method_version": "economic-trade-basis-v1",
        "trade_input_receipt_method_version": "active-trade-head-receipt-v2",
    }
    plan_id = stable_identifier("frozen-forward-plan", {
        "governance_method_version": GOVERNANCE_METHOD_VERSION,
        "vintage_id": vintage_id,
        "source_manifest_sha256": source_manifest_sha256,
        "candidate_set_hash": candidate_set_hash,
        "config_hash": config_hash,
        "code_tree_digest": code_tree_digest,
        **semantics,
    })
    path = root / "reports" / plan_id / "plan.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "schema_version": FROZEN_FORWARD_SCHEMA_VERSION,
        "method_version": FROZEN_FORWARD_METHOD_VERSION,
        "governance_method_version": GOVERNANCE_METHOD_VERSION,
        **semantics,
        "scope": "FROZEN_FORWARD",
        "plan_id": plan_id,
        "vintage": {"vintage_id": vintage_id},
        "source": {"manifest_sha256": source_manifest_sha256},
        "candidate_set_hash": candidate_set_hash,
        "config_hash": config_hash,
        "code_identity": {"tree_digest": code_tree_digest},
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
    receipt_path = (
        root / "data" / "research" / "input-receipts" / f"receipt-{stamp}.json"
    )
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    receipt_path.write_text("{}\n", encoding="utf-8")
    receipt_sha256 = sha256_file(receipt_path)
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


def _write_interval_suite_plan_artifact(
    root: Path,
    vintage_id: str,
    suite_plan_id: str,
    suite_evidence_id: str,
    source_git_hash: str,
    code_tree_digest: str,
) -> tuple[str, str, str, str, str, str]:
    """写入与治理登记合同一致的跨节拍冻结计划。"""
    data_root = root / "data"
    data_root.mkdir(exist_ok=True)
    repository = Path(__file__).resolve().parents[1]
    config_paths: list[Path] = []
    loaded_configs: dict[Path, tuple[
        dict[str, object], str, str, int, tuple[Path, ...],
    ]] = {}
    for interval, source_name in (
        ("1hour", "strategy_research.json"),
        ("4hour", "strategy_research_4hour.json"),
    ):
        source_config, _hash, _root, _depth, _paths = (
            load_governed_strategy_config_with_paths(
                repository, repository / "config" / source_name,
            )
        )
        config = deepcopy(dict(source_config))
        config["market_id"] = "market-one"
        config["pipeline_version"] = f"fixture-{suite_plan_id}"
        governance = deepcopy(dict(config["data_governance"]))
        governance["registry"] = "governance.sqlite3"
        config["data_governance"] = governance
        config_path = root / f"suite-{interval}.json"
        config_path.write_text(
            canonical_json(config) + "\n", encoding="utf-8",
        )
        config_hash = sha256_file(config_path)
        config_paths.append(config_path)
        loaded_configs[config_path] = (
            config, config_hash, config_hash, 0, (config_path,),
        )
    suite_plan = build_interval_suite_plan(
        root, tuple(config_paths), loaded_configs=loaded_configs,
    )
    actual_suite_plan_id = str(suite_plan["suite_plan_id"])
    suite_members = suite_plan["members"]
    assert isinstance(suite_members, list)
    evidence_members = [{
        "member_id": member["member_id"],
        "bar_interval": member["bar_interval"],
    } for member in suite_members if isinstance(member, dict)]
    first_member = evidence_members[0]
    evidence_body = {
        "suite_plan_id": actual_suite_plan_id,
        "source_git_hash": source_git_hash,
        "market_id": "market-one",
        "input_head_generation": "head-one",
        "input_receipt_sha256": "r" * 64,
        "alignment_interval_seconds": 14_400,
        "members": evidence_members,
        "sleeves": [{
            "sleeve_id": "sleeve-one",
            "member_id": first_member["member_id"],
            "bar_interval": first_member["bar_interval"],
            "family": "trend",
            "deployment_candidate_id": _TEST_CANDIDATE_ID,
            "suite_eligible": True,
        }],
        "suite_research_allocation": {
            "weights": {"sleeve-one": 0.4},
            "reserve": 0.6,
            "shared_caps": {"maximum_gross_weight": 0.85},
        },
        "operational_status": f"fixture-{suite_evidence_id}",
    }
    actual_evidence_id = stable_identifier(
        "interval-suite-evidence", evidence_body,
    )
    evidence = {**evidence_body, "suite_evidence_id": actual_evidence_id}
    frozen_members = []
    for member, config_path in zip(
        evidence_members, config_paths, strict=True,
    ):
        config, config_hash, root_hash, depth, source_paths = (
            loaded_configs[config_path]
        )
        frozen_members.append({
            **member,
            "config_source_path": config_path.relative_to(root).as_posix(),
            "config_source_sha256": config_hash,
            "config_lineage_root_sha256": root_hash,
            "config_lineage_depth": depth,
            "config_source_paths": [
                path.relative_to(root).as_posix() for path in source_paths
            ],
            "config_contract": config,
            "config_contract_sha256": sha256_text(canonical_json(config)),
        })
    live_root = data_root_locator(root, data_root)
    decision_grid = {
        "interval_seconds": 14_400,
        "utc_epoch_offset_seconds": 0,
        "maximum_recording_lag_seconds": 3900,
    }
    deployment_contract_id = interval_suite_deployment_contract_id(
        "governance.sqlite3", live_root, frozen_members, decision_grid,
    )
    plan_id = interval_suite_forward_plan_id(
        GOVERNANCE_METHOD_VERSION,
        vintage_id,
        actual_suite_plan_id,
        actual_evidence_id,
        source_git_hash,
        code_tree_digest,
        deployment_contract_id,
    )
    path = root / "reports" / plan_id / "plan.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    evidence_path = path.parent / "suite-evidence.json"
    evidence_path.write_text(canonical_json(evidence) + "\n", encoding="utf-8")
    path.write_text(canonical_json({
        "schema_version": INTERVAL_SUITE_FORWARD_SCHEMA_VERSION,
        "method_version": INTERVAL_SUITE_FORWARD_METHOD_VERSION,
        "governance_method_version": GOVERNANCE_METHOD_VERSION,
        "scope": "INTERVAL_SUITE_FROZEN_FORWARD",
        "governance_registry": "governance.sqlite3",
        "plan_id": plan_id,
        "suite_plan_id": actual_suite_plan_id,
        "suite_evidence_id": actual_evidence_id,
        "source_git_hash": source_git_hash,
        "code_tree_digest": code_tree_digest,
        "deployment_contract_id": deployment_contract_id,
        "live_data_root": live_root,
        "vintage": {
            "vintage_id": vintage_id,
            "market_id": "market-one",
            "start_time": "2027-01-01T00:00:00+00:00",
            "end_time": "2027-02-01T00:00:00+00:00",
        },
        "source_evidence": {
            "path": evidence_path.relative_to(root).as_posix(),
            "sha256": sha256_file(evidence_path),
        },
        "input": {
            "head_generation": "head-one", "receipt_sha256": "r" * 64,
        },
        "decision_grid": decision_grid,
        "members": frozen_members,
        "sleeves": [{
            "sleeve_id": "sleeve-one",
            "member_id": first_member["member_id"],
            "bar_interval": first_member["bar_interval"],
            "family": "trend",
            "candidate": {
                "candidate_id": _TEST_CANDIDATE_ID,
                "family": "trend",
                "mode": "paper",
                "expression_id": _TEST_EXPRESSION_ID,
                "parameters": _TEST_PARAMETERS,
                "complexity": len(_TEST_PARAMETERS),
            },
            "weight": 0.4,
        }],
        "allocation": {
            "weights": {"sleeve-one": 0.4},
            "reserve": 0.6,
            "shared_caps": {"maximum_gross_weight": 0.85},
        },
    }) + "\n", encoding="utf-8")
    return (
        plan_id,
        deployment_contract_id,
        actual_suite_plan_id,
        actual_evidence_id,
        path.relative_to(root).as_posix(),
        sha256_file(path),
    )


def _write_interval_suite_prediction_artifact(
    root: Path,
    plan_path: str,
    plan_id: str,
    suite_plan_id: str,
    suite_evidence_id: str,
    vintage_id: str,
    decision_time: datetime,
) -> tuple[str, str, str, str, str]:
    """写入含完整成员面板和 sleeve 贡献的套件预测制品。"""
    plan = json.loads((root / plan_path).read_text(encoding="utf-8"))
    receipt_path = root / "data" / "research" / "input-receipts" / "suite.json"
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    receipt_path.write_text("{}\n", encoding="utf-8")
    receipt_sha = sha256_file(receipt_path)
    input_head = "sha256-suite-head"
    member_panels: list[dict[str, object]] = []
    selected_member_ids = {
        str(sleeve["member_id"]) for sleeve in plan["sleeves"]
    }
    for member in plan["members"]:
        member_id = str(member["member_id"])
        if member_id not in selected_member_ids:
            continue
        panel_path = root / "data" / "research" / "suite-panels" / f"{member_id}.parquet"
        panel_path.parent.mkdir(parents=True, exist_ok=True)
        panel_path.write_bytes(("PAR1" + member_id + "PAR1").encode())
        member_panels.append({
            "member_id": member_id,
            "bar_interval": member["bar_interval"],
            "panel_path": panel_path.relative_to(root).as_posix(),
            "panel_sha256": sha256_file(panel_path),
            "panel_bytes": panel_path.stat().st_size,
            "decision_time": decision_time.isoformat(),
            "latest_available_time": decision_time.isoformat(),
            "input_head_generation": input_head,
            "attempt_ids": ["attempt-one"],
            "artifact_ids": ["artifact-one"],
            "normalization_versions": ["trade-v1"],
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
        })
    panel_set = interval_suite_member_panel_set_hash(
        plan_id, decision_time, member_panels,
    )
    prediction_id = interval_suite_forward_prediction_id(
        GOVERNANCE_METHOD_VERSION,
        INTERVAL_SUITE_PREDICTION_METHOD_VERSION,
        plan_id,
        decision_time,
    )
    sleeve = plan["sleeves"][0]
    candidate = sleeve["candidate"]
    payload = {
        "schema_version": INTERVAL_SUITE_PREDICTION_SCHEMA_VERSION,
        "method_version": INTERVAL_SUITE_PREDICTION_METHOD_VERSION,
        "governance_method_version": GOVERNANCE_METHOD_VERSION,
        "scope": "INTERVAL_SUITE_FROZEN_FORWARD_PREDICTION",
        "prediction_id": prediction_id,
        "plan_id": plan_id,
        "suite_plan_id": suite_plan_id,
        "suite_evidence_id": suite_evidence_id,
        "deployment_contract_id": plan["deployment_contract_id"],
        "decision_time": decision_time.isoformat(),
        "generated_at": (decision_time + timedelta(minutes=1)).isoformat(),
        "quality_reference_time": (
            decision_time + timedelta(seconds=3900)
        ).isoformat(),
        "vintage": plan["vintage"],
        "input": {
            "head_generation": input_head,
            "receipt_path": receipt_path.relative_to(root).as_posix(),
            "receipt_sha256": receipt_sha,
        },
        "code_identity": {
            "git_hash": plan["source_git_hash"],
            "tree_digest": plan["code_tree_digest"],
            "dirty_digest": "",
            "dirty": False,
            "decision_grade": True,
            "reason": None,
        },
        "member_panel_set_hash": panel_set,
        "member_panels": member_panels,
        "sleeves": [{
            "sleeve_id": sleeve["sleeve_id"],
            "member_id": sleeve["member_id"],
            "bar_interval": sleeve["bar_interval"],
            "family": sleeve["family"],
            "candidate_id": candidate["candidate_id"],
            "weight": 0.4,
            "raw_target": 0.5,
            "operational_target": 0.2,
        }],
        "allocation": {
            "weights": {"sleeve-one": 0.4},
            "reserve": 0.6,
            "aggregate_target": 0.2,
            "unit": "fraction_of_portfolio_capital",
        },
        "operational": {"eligible": True, "reasons": []},
    }
    path = root / "reports" / plan_id / "predictions" / "suite.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(canonical_json(payload) + "\n", encoding="utf-8")
    return (
        prediction_id,
        receipt_path.relative_to(root).as_posix(),
        receipt_sha,
        panel_set,
        path.relative_to(root).as_posix(),
    )


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
    forward_row_set_hash = (
        get_frozen_forward_prediction_row_set(
            root / "governance.sqlite3", str(forward_plan_id),
        )[0]
        if require_forward and forward_plan_id is not None
        else None
    )
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
        "frozen_forward_row_set_hash": forward_row_set_hash,
        "policy": policy,
        "passed_families": passed_families,
        "verdict": verdict,
    }, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    result_sha256 = sha256_file(result_path)
    manifest_path = run_directory / "manifest.json"
    receipt_path = (
        root / "data" / "research" / "input-receipts" / "holdout-test.json"
    )
    receipt_sha256 = sha256_file(receipt_path)
    manifest_path.write_text(json.dumps({
        "schema_version": HOLDOUT_MANIFEST_SCHEMA_VERSION,
        "holdout_method_version": HOLDOUT_METHOD_VERSION,
        "evaluation_id": evaluation_id,
        "vintage_id": vintage_id,
        "candidate_set_hash": candidate_set_hash,
        "candidate_set_identity": candidate_set_identity,
        "evaluation_identity": evaluation_identity,
        "input_head_generation": evaluation_identity[
            "input_head_generation"
        ],
        "input_attempt_ids": evaluation_identity["input_attempt_ids"],
        "input_artifact_ids": evaluation_identity["input_artifact_ids"],
        "normalization_versions": evaluation_identity[
            "normalization_versions"
        ],
        "frozen_forward_row_set_hash": forward_row_set_hash,
        "input_receipt_sha256": receipt_sha256,
        "verdict": verdict,
        "artifacts": {
            "config": {
                "kind": "holdout_config",
                "path": config_path.relative_to(root).as_posix(),
                "sha256": config_hash,
                "bytes": config_path.stat().st_size,
            },
            "input_receipt": {
                "kind": "active_trade_head_receipt",
                "path": receipt_path.relative_to(root).as_posix(),
                "sha256": receipt_sha256,
                "bytes": receipt_path.stat().st_size,
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
    _set_now(_time("2025-06-02T00:00:00"))
    exposure = register_research_exposure(
        registry,
        "research-identity-one",
        "market-one",
        _time("2025-01-01T00:00:00"),
        _time("2025-06-01T00:00:00"),
    )
    assert exposure.market_id == "market-one"
    assert exposure.recorded_at == _time("2025-06-02T00:00:00")
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
    assert version == (str(GOVERNANCE_SCHEMA_VERSION),)


def test_schema_write_ceiling_preserves_legacy_reader_deployment(
    tmp_path: Path,
) -> None:
    """新 reader 可读物理兼容库，但不得突破旧冻结 writer 的版本上限。"""
    registry = tmp_path / "governance.sqlite3"
    vintage = seal_holdout_vintage(
        registry,
        "market-one",
        _time("2027-01-01T00:00:00"),
        _time("2027-02-01T00:00:00"),
    )
    with sqlite3.connect(registry) as connection:
        connection.execute(
            "UPDATE governance_meta SET value='2' WHERE key='schema_version'"
        )
        connection.execute(
            "INSERT INTO governance_meta(key,value) VALUES(?,?)",
            ("schema_write_ceiling", "2"),
        )

    assert list_holdout_vintages(registry) == (vintage,)
    assert get_holdout_vintage(registry, vintage.vintage_id) == vintage
    with sqlite3.connect(registry) as connection:
        version = connection.execute(
            "SELECT value FROM governance_meta WHERE key='schema_version'"
        ).fetchone()
    assert version == ("2",)

    exposure = register_research_exposure(
        registry,
        "compatible-exposure",
        "market-one",
        _time("2026-01-01T00:00:00"),
        _time("2026-02-01T00:00:00"),
    )
    assert exposure.research_identity == "compatible-exposure"
    with pytest.raises(ValueError, match="写入已冻结在版本 2"):
        seal_holdout_vintage(
            registry,
            "market-two",
            _time("2028-01-01T00:00:00"),
            _time("2028-02-01T00:00:00"),
        )
    with pytest.raises(ValueError, match="未消费封存段重叠"):
        register_research_exposure(
            registry,
            "overlapping-exposure",
            "market-one",
            _time("2027-01-15T00:00:00"),
            _time("2027-01-20T00:00:00"),
        )
    with sqlite3.connect(registry) as connection:
        assert connection.execute(
            "SELECT value FROM governance_meta WHERE key='schema_version'"
        ).fetchone() == ("2",)
        assert connection.execute(
            "SELECT research_identity FROM research_exposure"
        ).fetchall() == [("compatible-exposure",)]


def test_explicit_schema_write_ceiling_upgrade_is_backed_up(
    tmp_path: Path,
) -> None:
    """显式旧版本预期匹配时才可备份并开放当前 writer。"""
    registry = tmp_path / "governance.sqlite3"
    seal_holdout_vintage(
        registry,
        "market-one",
        _time("2027-01-01T00:00:00"),
        _time("2027-02-01T00:00:00"),
    )
    with sqlite3.connect(registry) as connection:
        connection.execute("DROP TABLE active_head_receipt")
        connection.execute(
            "UPDATE governance_meta SET value='2' WHERE key='schema_version'"
        )
        connection.execute(
            "INSERT INTO governance_meta(key,value) VALUES(?,?)",
            ("schema_write_ceiling", "2"),
        )

    backup = tmp_path / "governance-v2.sqlite3.bak"
    assert upgrade_governance_write_ceiling(
        registry,
        backup,
        expected_version=2,
        expected_write_ceiling=2,
    ) == backup.resolve()
    with sqlite3.connect(registry) as connection:
        assert connection.execute(
            "SELECT key,value FROM governance_meta ORDER BY key"
        ).fetchall() == [
            ("schema_version", str(GOVERNANCE_SCHEMA_VERSION)),
            ("schema_write_ceiling", str(GOVERNANCE_SCHEMA_VERSION)),
        ]
        assert connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name='active_head_receipt'"
        ).fetchone() == ("active_head_receipt",)
        assert connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name='interval_suite_forward_plan'"
        ).fetchone() == ("interval_suite_forward_plan",)
    with sqlite3.connect(backup) as connection:
        assert connection.execute("PRAGMA integrity_check").fetchall() == [
            ("ok",),
        ]
        assert connection.execute(
            "SELECT value FROM governance_meta WHERE key='schema_version'"
        ).fetchone() == ("2",)
        assert connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name='active_head_receipt'"
        ).fetchone() is None

    with pytest.raises(FileExistsError):
        upgrade_governance_write_ceiling(
            registry,
            backup,
            expected_version=2,
            expected_write_ceiling=2,
        )


def test_governance_upgrade_rejects_malformed_receipt_table(
    tmp_path: Path,
) -> None:
    """同名坏表不得被 IF NOT EXISTS 静默接受或推进版本。"""
    registry = tmp_path / "governance.sqlite3"
    seal_holdout_vintage(
        registry,
        "market-one",
        _time("2027-01-01T00:00:00"),
        _time("2027-02-01T00:00:00"),
    )
    with sqlite3.connect(registry) as connection:
        connection.execute("DROP TABLE active_head_receipt")
        connection.execute(
            "CREATE TABLE active_head_receipt("
            "consumer_kind TEXT NOT NULL,consumer_id TEXT NOT NULL,"
            "market_id TEXT NOT NULL,head_generation TEXT NOT NULL,"
            "receipt_artifact_path TEXT NOT NULL,"
            "receipt_artifact_sha256 TEXT NOT NULL,recorded_at TEXT NOT NULL,"
            "PRIMARY KEY(consumer_kind,consumer_id))"
        )
        connection.execute(
            "UPDATE governance_meta SET value='2' WHERE key='schema_version'"
        )
        connection.execute(
            "INSERT INTO governance_meta(key,value) VALUES(?,?)",
            ("schema_write_ceiling", "2"),
        )

    backup = tmp_path / "governance-malformed-v2.sqlite3.bak"
    with pytest.raises(ValueError, match="缺少必要约束"):
        upgrade_governance_write_ceiling(
            registry,
            backup,
            expected_version=2,
            expected_write_ceiling=2,
        )
    with sqlite3.connect(registry) as connection:
        assert connection.execute(
            "SELECT value FROM governance_meta WHERE key='schema_version'"
        ).fetchone() == ("2",)
    with sqlite3.connect(backup) as connection:
        assert connection.execute("PRAGMA integrity_check").fetchone() == (
            "ok",
        )


def test_explicit_v5_upgrade_creates_current_suite_tables(
    tmp_path: Path,
) -> None:
    """真实 v5 形状须经备份后才获得当前套件写权限。"""
    registry = tmp_path / "governance.sqlite3"
    seal_holdout_vintage(
        registry,
        "market-one",
        _time("2027-01-01T00:00:00"),
        _time("2027-02-01T00:00:00"),
    )
    with sqlite3.connect(registry) as connection:
        connection.execute("DROP TABLE interval_suite_forward_plan")
        connection.execute(
            "UPDATE governance_meta SET value='5' WHERE key='schema_version'"
        )
        connection.execute(
            "INSERT INTO governance_meta(key,value) VALUES(?,?)",
            ("schema_write_ceiling", "5"),
        )
    assert get_interval_suite_forward_plan_for_vintage(
        registry, "missing",
    ) is None
    backup = tmp_path / "governance-v5.sqlite3.bak"
    upgrade_governance_write_ceiling(
        registry, backup, expected_version=5, expected_write_ceiling=5,
    )
    with sqlite3.connect(registry) as connection:
        assert connection.execute(
            "SELECT value FROM governance_meta WHERE key='schema_version'"
        ).fetchone() == (str(GOVERNANCE_SCHEMA_VERSION),)
        assert connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name='interval_suite_forward_plan'"
        ).fetchone() == ("interval_suite_forward_plan",)
        assert connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name='interval_suite_forward_prediction'"
        ).fetchone() == ("interval_suite_forward_prediction",)
    with sqlite3.connect(backup) as connection:
        assert connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name='interval_suite_forward_plan'"
        ).fetchone() is None


def test_explicit_v6_to_v7_upgrade_creates_suite_prediction_table(
    tmp_path: Path,
) -> None:
    """v6 后继库须显式备份后才获得套件预测写权限。"""
    registry = tmp_path / "governance.sqlite3"
    seal_holdout_vintage(
        registry,
        "market-one",
        _time("2027-01-01T00:00:00"),
        _time("2027-02-01T00:00:00"),
    )
    with sqlite3.connect(registry) as connection:
        connection.execute("DROP TABLE interval_suite_forward_prediction")
        connection.execute(
            "UPDATE governance_meta SET value='6' WHERE key='schema_version'"
        )
        connection.execute(
            "INSERT INTO governance_meta(key,value) VALUES(?,?)",
            ("schema_write_ceiling", "6"),
        )
    assert list_holdout_vintages(registry)
    backup = tmp_path / "governance-v6.sqlite3.bak"
    upgrade_governance_write_ceiling(
        registry, backup, expected_version=6, expected_write_ceiling=6,
    )
    with sqlite3.connect(registry) as connection:
        assert connection.execute(
            "SELECT value FROM governance_meta WHERE key='schema_version'"
        ).fetchone() == (str(GOVERNANCE_SCHEMA_VERSION),)
        assert connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name='interval_suite_forward_prediction'"
        ).fetchone() == ("interval_suite_forward_prediction",)
    with sqlite3.connect(backup) as connection:
        assert connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name='interval_suite_forward_prediction'"
        ).fetchone() is None


def test_governance_upgrade_rejects_malformed_suite_plan_table(
    tmp_path: Path,
) -> None:
    """迁移不得接受预存的同名弱约束套件计划表。"""
    registry = tmp_path / "governance.sqlite3"
    seal_holdout_vintage(
        registry,
        "market-one",
        _time("2027-01-01T00:00:00"),
        _time("2027-02-01T00:00:00"),
    )
    with sqlite3.connect(registry) as connection:
        connection.execute("DROP TABLE interval_suite_forward_plan")
        connection.execute(
            "CREATE TABLE interval_suite_forward_plan(plan_id TEXT PRIMARY KEY)"
        )
        connection.execute(
            "UPDATE governance_meta SET value='5' WHERE key='schema_version'"
        )
        connection.execute(
            "INSERT INTO governance_meta(key,value) VALUES(?,?)",
            ("schema_write_ceiling", "5"),
        )
    backup = tmp_path / "malformed-v5.sqlite3.bak"
    with pytest.raises(ValueError, match="表结构不兼容"):
        upgrade_governance_write_ceiling(
            registry, backup, expected_version=5, expected_write_ceiling=5,
        )
    with sqlite3.connect(registry) as connection:
        assert connection.execute(
            "SELECT value FROM governance_meta WHERE key='schema_version'"
        ).fetchone() == ("5",)


def test_governance_upgrade_rejects_malformed_suite_prediction_table(
    tmp_path: Path,
) -> None:
    """v7 迁移不得接受预存的同名弱约束预测表。"""
    registry = tmp_path / "governance.sqlite3"
    seal_holdout_vintage(
        registry,
        "market-one",
        _time("2027-01-01T00:00:00"),
        _time("2027-02-01T00:00:00"),
    )
    with sqlite3.connect(registry) as connection:
        connection.execute("DROP TABLE interval_suite_forward_prediction")
        connection.execute(
            "CREATE TABLE interval_suite_forward_prediction("
            "prediction_id TEXT PRIMARY KEY)"
        )
        connection.execute(
            "UPDATE governance_meta SET value='6' WHERE key='schema_version'"
        )
        connection.execute(
            "INSERT INTO governance_meta(key,value) VALUES(?,?)",
            ("schema_write_ceiling", "6"),
        )
    backup = tmp_path / "malformed-v6.sqlite3.bak"
    with pytest.raises(ValueError, match="表结构不兼容"):
        upgrade_governance_write_ceiling(
            registry, backup, expected_version=6, expected_write_ceiling=6,
        )
    with sqlite3.connect(registry) as connection:
        assert connection.execute(
            "SELECT value FROM governance_meta WHERE key='schema_version'"
        ).fetchone() == ("6",)


def test_governance_upgrade_backup_includes_prior_concurrent_commit(
    tmp_path: Path,
) -> None:
    """迁移锁等待期间完成的旧 writer 提交必须进入恢复副本。"""
    registry = tmp_path / "governance.sqlite3"
    seal_holdout_vintage(
        registry,
        "market-one",
        _time("2027-01-01T00:00:00"),
        _time("2027-02-01T00:00:00"),
    )
    with sqlite3.connect(registry) as connection:
        connection.execute(
            "UPDATE governance_meta SET value='2' WHERE key='schema_version'"
        )
        connection.execute(
            "INSERT INTO governance_meta(key,value) VALUES(?,?)",
            ("schema_write_ceiling", "2"),
        )

    writer = sqlite3.connect(registry, timeout=30.0, isolation_level=None)
    writer.execute("BEGIN IMMEDIATE")
    writer.execute(
        "INSERT INTO research_exposure VALUES(?,?,?,?,?,?)",
        (
            "late-exposure-id",
            "late-research-identity",
            "market-one",
            _time("2026-01-01T00:00:00").isoformat(),
            _time("2026-02-01T00:00:00").isoformat(),
            _time("2026-02-02T00:00:00").isoformat(),
        ),
    )
    backup = tmp_path / "concurrent-v2.sqlite3.bak"
    started = threading.Event()
    errors: list[BaseException] = []

    def migrate() -> None:
        started.set()
        try:
            upgrade_governance_write_ceiling(
                registry,
                backup,
                expected_version=2,
                expected_write_ceiling=2,
            )
        except BaseException as error:
            errors.append(error)

    migration = threading.Thread(target=migrate)
    migration.start()
    assert started.wait(timeout=1.0)
    time.sleep(0.1)
    writer.execute("COMMIT")
    writer.close()
    migration.join(timeout=5.0)
    assert not migration.is_alive()
    assert errors == []
    with sqlite3.connect(backup) as connection:
        assert connection.execute(
            "SELECT research_identity FROM research_exposure "
            "WHERE exposure_id='late-exposure-id'"
        ).fetchone() == ("late-research-identity",)


def test_schema_write_ceiling_rejects_incompatible_physical_schema(
    tmp_path: Path,
) -> None:
    """版本上限不是绕过物理 schema 验证的开关。"""
    registry = tmp_path / "governance.sqlite3"
    with sqlite3.connect(registry) as connection:
        connection.execute(
            "CREATE TABLE governance_meta(key TEXT PRIMARY KEY,value TEXT)"
        )
        connection.executemany(
            "INSERT INTO governance_meta(key,value) VALUES(?,?)",
            (("schema_version", "2"), ("schema_write_ceiling", "2")),
        )
    with pytest.raises(ValueError, match="物理 schema 不兼容"):
        list_holdout_vintages(registry)


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


def test_legacy_forward_plan_cannot_be_newly_registered(tmp_path: Path) -> None:
    registry = tmp_path / "governance.sqlite3"
    vintage = seal_holdout_vintage(
        registry, "market-one", _time("2027-01-01T00:00:00"),
        _time("2027-02-01T00:00:00"),
    )
    _, plan_path, plan_sha256 = _write_forward_plan_artifact(
        tmp_path, vintage.vintage_id, "manifest", "candidates", "config",
        "tree", legacy=True,
    )
    with pytest.raises(ValueError, match="成交语义身份不完整"):
        register_frozen_forward_plan(
            registry, vintage.vintage_id, "manifest", "candidates",
            "config", "tree", plan_path, plan_sha256,
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
    no_replay = pytest.MonkeyPatch()
    no_replay.setattr(
        "guvolu.research.frozen_forward.attest_frozen_prediction_artifact",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("批量复核不得重放多年 panel")
        ),
    )
    try:
        batch = attest_frozen_forward_batch(
            tmp_path, plan.plan_id, registry_path=registry,
        )
    finally:
        no_replay.undo()
    assert batch.verification.prediction_count == 2
    assert batch.decision_times == (vintage.start_time, second_decision)
    assert batch.row_set_hash.startswith("frozen-forward-row-set-")
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
    assert "recorded_at" not in inspect.signature(
        register_research_exposure
    ).parameters
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


def test_prediction_registration_rejects_incomplete_attestation_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """公开注册 API 不得接受缺少配置谱系的自报目标。"""
    monkeypatch.setattr(
        "guvolu.research.frozen_forward.attest_frozen_prediction_artifact",
        _actual_forward_attestation,
    )
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
    with pytest.raises(ValueError, match="plan.config_path"):
        register_frozen_forward_prediction(
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
    assert list_frozen_forward_predictions(registry, plan.plan_id) == ()


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
    (
        _suite_id, suite_deployment_id, suite_plan_id,
        suite_evidence_id, suite_path, suite_sha,
    ) = _write_interval_suite_plan_artifact(
        tmp_path, vintage.vintage_id, "suite-plan", "suite-evidence",
        "a" * 40, "tree-one",
    )
    with pytest.raises(ValueError, match="单成员冻结前向计划"):
        register_interval_suite_forward_plan(
            registry, vintage.vintage_id, suite_plan_id, suite_evidence_id,
            "a" * 40, "tree-one", suite_deployment_id,
            suite_path, suite_sha,
            repository_root=tmp_path,
        )
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


def test_interval_suite_forward_plan_is_precommitted_and_attested(
    tmp_path: Path,
) -> None:
    """跨节拍计划可与单成员计划并存，且同 vintage 不可改写。"""
    registry = tmp_path / "governance.sqlite3"
    vintage = seal_holdout_vintage(
        registry,
        "market-one",
        _time("2027-01-01T00:00:00"),
        _time("2027-02-01T00:00:00"),
    )
    (
        plan_id, deployment_id, suite_plan_id,
        suite_evidence_id, plan_path, plan_sha,
    ) = _write_interval_suite_plan_artifact(
        tmp_path, vintage.vintage_id, "suite-plan", "suite-evidence",
        "a" * 40, "tree-one",
    )
    registered = register_interval_suite_forward_plan(
        Path("governance.sqlite3"),
        vintage.vintage_id,
        suite_plan_id,
        suite_evidence_id,
        "a" * 40, "tree-one", deployment_id, plan_path, plan_sha,
        repository_root=tmp_path,
    )
    assert registered.plan_id == plan_id
    assert get_interval_suite_forward_plan_for_vintage(
        registry, vintage.vintage_id,
    ) == registered
    assert register_interval_suite_forward_plan(
        registry, vintage.vintage_id, suite_plan_id, suite_evidence_id,
        "a" * 40, "tree-one", deployment_id, plan_path, plan_sha,
        repository_root=tmp_path,
    ) == registered
    _legacy_id, legacy_path, legacy_sha = _write_forward_plan_artifact(
        tmp_path,
        vintage.vintage_id,
        "manifest-hash",
        "candidate-set-hash",
        "config-hash",
        "tree-hash",
    )
    with pytest.raises(ValueError, match="跨节拍冻结前向计划"):
        register_frozen_forward_plan(
            registry,
            vintage.vintage_id,
            "manifest-hash",
            "candidate-set-hash",
            "config-hash",
            "tree-hash",
            legacy_path,
            legacy_sha,
            repository_root=tmp_path,
        )
    _set_now(vintage.end_time)
    with pytest.raises(ValueError, match="套件 holdout 入口消费"):
        start_holdout_evaluation_attempt(
            registry,
            vintage.vintage_id,
            "candidate-set-hash",
            "generic-evaluation",
        )
    assert get_holdout_vintage(registry, vintage.vintage_id).status == "sealed"
    _set_now(vintage.start_time)
    with pytest.raises(ValueError, match="开始前"):
        register_interval_suite_forward_plan(
            registry, vintage.vintage_id, suite_plan_id, suite_evidence_id,
            "a" * 40, "tree-one", deployment_id, plan_path, plan_sha,
            repository_root=tmp_path,
        )
    _set_now(_time("2026-08-14T00:00:00"))

    path = tmp_path / plan_path
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["allocation"]["reserve"] = 0.5
    path.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="SHA-256 不匹配"):
        register_interval_suite_forward_plan(
            registry, vintage.vintage_id, suite_plan_id, suite_evidence_id,
            "a" * 40, "tree-one", deployment_id, plan_path, plan_sha,
            repository_root=tmp_path,
        )
    with pytest.raises(ValueError, match="allocation 与 evidence 不一致"):
        register_interval_suite_forward_plan(
            registry, vintage.vintage_id, suite_plan_id, suite_evidence_id,
            "a" * 40, "tree-one", deployment_id,
            plan_path, sha256_file(path),
            repository_root=tmp_path,
        )

    (
        different_id, different_deployment_id, different_suite_plan_id,
        different_evidence_id, different_path, different_sha,
    ) = (
        _write_interval_suite_plan_artifact(
            tmp_path, vintage.vintage_id, "different-suite", "suite-evidence",
            "a" * 40, "tree-one",
        )
    )
    assert different_id != plan_id
    with pytest.raises(ValueError, match="不同的套件冻结前向计划"):
        register_interval_suite_forward_plan(
            registry, vintage.vintage_id,
            different_suite_plan_id, different_evidence_id,
            "a" * 40, "tree-one", different_deployment_id,
            different_path, different_sha,
            repository_root=tmp_path,
        )


def test_interval_suite_prediction_is_append_only_and_row_set_bound(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """共同栅格预测必须绑定活动收据、全部成员面板与聚合仓位。"""
    registry = tmp_path / "governance.sqlite3"
    vintage = seal_holdout_vintage(
        registry,
        "market-one",
        _time("2027-01-01T00:00:00"),
        _time("2027-02-01T00:00:00"),
    )
    (
        plan_id, deployment_id, suite_plan_id,
        suite_evidence_id, plan_path, plan_sha,
    ) = _write_interval_suite_plan_artifact(
        tmp_path, vintage.vintage_id, "suite-plan", "suite-evidence",
        "a" * 40, "tree-one",
    )
    plan = register_interval_suite_forward_plan(
        registry, vintage.vintage_id, suite_plan_id, suite_evidence_id,
        "a" * 40, "tree-one", deployment_id, plan_path, plan_sha,
        repository_root=tmp_path,
    )
    decision = _time("2027-01-01T04:00:00")
    (
        prediction_id, receipt_path, receipt_sha, panel_set, prediction_path,
    ) = _write_interval_suite_prediction_artifact(
        tmp_path,
        plan_path,
        plan_id,
        suite_plan_id,
        suite_evidence_id,
        vintage.vintage_id,
        decision,
    )
    fake_inputs = FrozenPanelInputs(
        market={"market_id": "market-one"},
        paths=(),
        head_generation="sha256-suite-head",
        attempt_ids=("attempt-one",),
        artifact_ids=("artifact-one",),
        normalization_versions=("trade-v1",),
        maximum_event_time=decision,
    )
    monkeypatch.setattr(
        "guvolu.research.panel.attest_trade_input_receipt",
        lambda *_args, **_kwargs: fake_inputs,
    )
    monkeypatch.setattr(
        "guvolu.research.interval_suite_prediction."
        "attest_interval_suite_forward_prediction",
        lambda *_args, **_kwargs: {},
    )
    clean_identity = CodeIdentity(
        git_hash="a" * 40,
        tree_digest="tree-one",
        dirty_digest="",
        dirty=False,
        decision_grade=True,
        reason=None,
    )
    dirty_identity = CodeIdentity(
        git_hash="a" * 40,
        tree_digest="tree-one",
        dirty_digest="dirty-tree",
        dirty=True,
        decision_grade=False,
        reason="repository_dirty",
    )
    identity_sequence = iter((clean_identity, dirty_identity))
    monkeypatch.setattr(
        governance_module,
        "code_identity",
        lambda *_args, **_kwargs: next(identity_sequence),
    )
    _set_now(decision + timedelta(minutes=1))
    prediction_sha = sha256_file(tmp_path / prediction_path)
    with pytest.raises(ValueError, match="代码树不是计划的 clean commit"):
        register_interval_suite_forward_prediction(
            registry,
            plan.plan_id,
            decision,
            "sha256-suite-head",
            receipt_path,
            receipt_sha,
            panel_set,
            prediction_path,
            prediction_sha,
            repository_root=tmp_path,
        )
    assert list_interval_suite_forward_predictions(registry, plan.plan_id) == ()
    monkeypatch.setattr(
        governance_module,
        "code_identity",
        lambda *_args, **_kwargs: clean_identity,
    )
    registered = register_interval_suite_forward_prediction(
        registry,
        plan.plan_id,
        decision,
        "sha256-suite-head",
        receipt_path,
        receipt_sha,
        panel_set,
        prediction_path,
        prediction_sha,
        repository_root=tmp_path,
    )
    assert registered.prediction_id == prediction_id
    assert list_interval_suite_forward_predictions(
        registry, plan.plan_id,
    ) == (registered,)
    row_set, count, decision_times = (
        get_interval_suite_forward_prediction_row_set(
            registry, plan.plan_id,
        )
    )
    assert row_set.startswith("interval-suite-forward-row-set-")
    assert count == 1
    assert decision_times == (decision,)
    assert register_interval_suite_forward_prediction(
        registry,
        plan.plan_id,
        decision,
        "sha256-suite-head",
        receipt_path,
        receipt_sha,
        panel_set,
        prediction_path,
        prediction_sha,
        repository_root=tmp_path,
    ) == registered

    monkeypatch.setattr(
        governance_module,
        "code_identity",
        lambda *_args, **_kwargs: dirty_identity,
    )
    with pytest.raises(ValueError, match="代码树不是计划的 clean commit"):
        register_interval_suite_forward_prediction(
            registry,
            plan.plan_id,
            decision,
            "sha256-suite-head",
            receipt_path,
            receipt_sha,
            panel_set,
            prediction_path,
            prediction_sha,
            repository_root=tmp_path,
        )

    mismatched_identity = CodeIdentity(
        git_hash="a" * 40,
        tree_digest="different-tree",
        dirty_digest="",
        dirty=False,
        decision_grade=True,
        reason=None,
    )
    monkeypatch.setattr(
        governance_module,
        "code_identity",
        lambda *_args, **_kwargs: mismatched_identity,
    )
    with pytest.raises(ValueError, match="代码树不是计划的 clean commit"):
        register_interval_suite_forward_prediction(
            registry,
            plan.plan_id,
            decision,
            "sha256-suite-head",
            receipt_path,
            receipt_sha,
            panel_set,
            prediction_path,
            prediction_sha,
            repository_root=tmp_path,
        )
    monkeypatch.setattr(
        governance_module,
        "code_identity",
        lambda *_args, **_kwargs: clean_identity,
    )
    _set_now(_time("2027-01-01T05:01:00"))
    with pytest.raises(ValueError, match="未对齐共同栅格"):
        register_interval_suite_forward_prediction(
            registry,
            plan.plan_id,
            _time("2027-01-01T05:00:00"),
            "sha256-suite-head",
            receipt_path,
            receipt_sha,
            panel_set,
            prediction_path,
            prediction_sha,
            repository_root=tmp_path,
        )
    _set_now(_time("2027-01-01T05:06:00"))
    with pytest.raises(ValueError, match="时效窗口"):
        register_interval_suite_forward_prediction(
            registry,
            plan.plan_id,
            decision,
            "sha256-suite-head",
            receipt_path,
            receipt_sha,
            panel_set,
            prediction_path,
            prediction_sha,
            repository_root=tmp_path,
        )
    _set_now(decision + timedelta(minutes=1))

    changed_path = tmp_path / "reports" / plan_id / "predictions" / "changed.json"
    changed = json.loads((tmp_path / prediction_path).read_text(encoding="utf-8"))
    changed["sleeves"][0]["operational_target"] = 0.3
    changed_path.write_text(canonical_json(changed) + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="质量硬门"):
        register_interval_suite_forward_prediction(
            registry,
            plan.plan_id,
            decision,
            "sha256-suite-head",
            receipt_path,
            receipt_sha,
            panel_set,
            changed_path.relative_to(tmp_path).as_posix(),
            sha256_file(changed_path),
            repository_root=tmp_path,
        )


def test_interval_suite_plan_artifact_is_validated_under_write_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """制品现场复核必须发生在 BEGIN IMMEDIATE 之后。"""
    registry = tmp_path / "governance.sqlite3"
    vintage = seal_holdout_vintage(
        registry,
        "market-one",
        _time("2027-01-01T00:00:00"),
        _time("2027-02-01T00:00:00"),
    )
    (
        _plan_id, deployment_id, suite_plan_id,
        suite_evidence_id, plan_path, plan_sha,
    ) = _write_interval_suite_plan_artifact(
        tmp_path, vintage.vintage_id, "suite-plan", "suite-evidence",
        "a" * 40, "tree-one",
    )
    lock_state = {"held": False}
    original_begin = governance_module._begin
    original_validate = (
        governance_module._validated_interval_suite_forward_plan_artifact
    )

    def begin(connection: object) -> None:
        original_begin(connection)  # type: ignore[arg-type]
        lock_state["held"] = True

    def validate(*args: object, **kwargs: object) -> str:
        assert lock_state["held"] is True
        return original_validate(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(governance_module, "_begin", begin)
    monkeypatch.setattr(
        governance_module,
        "_validated_interval_suite_forward_plan_artifact",
        validate,
    )
    register_interval_suite_forward_plan(
        registry, vintage.vintage_id, suite_plan_id, suite_evidence_id,
        "a" * 40, "tree-one", deployment_id, plan_path, plan_sha,
        repository_root=tmp_path,
    )


def test_interval_suite_plan_requires_explicit_live_root_and_exact_config_snapshot(
    tmp_path: Path,
) -> None:
    """v2 计划不得缺省活动根，也不能把另一份配置伪装成冻结快照。"""
    for case in ("missing-live-root", "config-drift"):
        root = tmp_path / case
        root.mkdir()
        registry = root / "governance.sqlite3"
        vintage = seal_holdout_vintage(
            registry,
            "market-one",
            _time("2027-01-01T00:00:00"),
            _time("2027-02-01T00:00:00"),
        )
        (
            _plan_id, deployment_id, suite_plan_id,
            suite_evidence_id, plan_path, _plan_sha,
        ) = _write_interval_suite_plan_artifact(
            root, vintage.vintage_id, "suite-plan", "suite-evidence",
            "a" * 40, "tree-one",
        )
        path = root / plan_path
        payload = json.loads(path.read_text(encoding="utf-8"))
        if case == "missing-live-root":
            del payload["live_data_root"]
            path.write_text(canonical_json(payload) + "\n", encoding="utf-8")
            with pytest.raises(ValueError, match="缺少 live_data_root"):
                register_interval_suite_forward_plan(
                    registry, vintage.vintage_id,
                    suite_plan_id, suite_evidence_id,
                    "a" * 40, "tree-one", deployment_id,
                    plan_path, sha256_file(path), repository_root=root,
                )
            continue
        members = payload["members"]
        assert isinstance(members, list)
        member = members[0]
        assert isinstance(member, dict)
        source_path = root / str(member["config_source_path"])
        changed_config = json.loads(source_path.read_text(encoding="utf-8"))
        changed_config["pipeline_version"] = "different-live-config"
        source_path.write_text(
            canonical_json(changed_config) + "\n", encoding="utf-8",
        )
        changed_hash = sha256_file(source_path)
        member["config_source_sha256"] = changed_hash
        member["config_lineage_root_sha256"] = changed_hash
        live_root = payload["live_data_root"]
        decision_grid = payload["decision_grid"]
        assert isinstance(live_root, dict)
        assert isinstance(decision_grid, dict)
        deployment_id = interval_suite_deployment_contract_id(
            "governance.sqlite3", live_root, members, decision_grid,
        )
        plan_id = interval_suite_forward_plan_id(
            GOVERNANCE_METHOD_VERSION,
            vintage.vintage_id,
            suite_plan_id,
            suite_evidence_id,
            "a" * 40,
            "tree-one",
            deployment_id,
        )
        payload["deployment_contract_id"] = deployment_id
        payload["plan_id"] = plan_id
        path.write_text(canonical_json(payload) + "\n", encoding="utf-8")
        with pytest.raises(ValueError, match="配置快照不能由现场谱系重建"):
            register_interval_suite_forward_plan(
                registry, vintage.vintage_id,
                suite_plan_id, suite_evidence_id,
                "a" * 40, "tree-one", deployment_id,
                plan_path, sha256_file(path), repository_root=root,
            )


def test_holdout_attestation_rejects_self_reported_positive_metrics(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """终局指标必须由绑定面板、目标和成本模型重算。"""
    source_manifest = tmp_path / "source-manifest.json"
    source_summary = tmp_path / "source-summary.json"
    candidate_registry = tmp_path / "candidate-registry.json"
    panel = tmp_path / "panel.parquet"
    for path in (source_manifest, source_summary, candidate_registry):
        path.write_text("{}", encoding="utf-8")
    panel.write_bytes(b"panel")
    config = tmp_path / "config.json"
    config.write_text(json.dumps({
        "market_id": "market-one",
        "bar_interval": "1hour",
        "from_time": "2026-01-01T00:00:00+00:00",
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
            "registry": "governance.sqlite3",
            "holdout_policy": {
                "minimum_bars": 1,
                "minimum_sharpe": 0.0,
                "maximum_drawdown": 0.5,
                "maximum_fdr_q": 0.2,
                "require_frozen_forward_predictions": False,
            },
        },
    }), encoding="utf-8")
    config_snapshot = snapshot_verified_config_lineage(
        tmp_path, config, tmp_path / "config-artifacts",
    )
    start = _time("2027-01-01T01:00:00")
    end = _time("2027-01-01T02:00:00")
    schedule = tmp_path / "schedule.json"
    schedule.write_text(json.dumps({
        "decision_times": [start.isoformat()],
    }), encoding="utf-8")
    result = tmp_path / "result.json"
    result.write_text(json.dumps({
        "vintage": {
            "start_time": start.isoformat(),
            "end_time": end.isoformat(),
        },
        "frozen_forward_plan_id": None,
        "candidate_results": [{
            "candidate_id": "candidate-one",
            "family": "trend",
            "parameters": {"lookback": 1},
            "metrics": {
                "net_return": 1.0,
                "sharpe": 9.0,
                "maximum_drawdown": 0.0,
                "p_value": 0.001,
            },
            "fdr_q": 0.001,
            "passed": True,
            "rejection_reasons": [],
        }],
        "passed_families": ["trend"],
        "verdict": "passed",
    }), encoding="utf-8")

    def artifact(path: Path) -> dict[str, object]:
        return {
            "path": path.relative_to(tmp_path).as_posix(),
            "sha256": sha256_file(path),
            "bytes": path.stat().st_size,
        }

    receipt = tmp_path / "input-receipt.json"
    receipt.write_text("{}\n", encoding="utf-8")
    registry = tmp_path / "governance.sqlite3"
    registry.write_bytes(b"")
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({
        "evaluation_id": "evaluation-one",
        "candidate_set_identity": {
            "source_manifest_sha256": sha256_file(source_manifest),
            "source_summary_sha256": sha256_file(source_summary),
            "candidate_registry_sha256": sha256_file(candidate_registry),
        },
        "evaluation_identity": {
            "config_hash": config_snapshot.leaf_config_sha256,
        },
        "input_head_generation": "head-one",
        "input_attempt_ids": ["attempt-one"],
        "input_artifact_ids": ["artifact-one"],
        "normalization_versions": ["normalization-one"],
        "input_receipt_sha256": sha256_file(receipt),
        "artifacts": {
            "source_manifest": artifact(source_manifest),
            "source_summary": artifact(source_summary),
            "candidate_registry": artifact(candidate_registry),
            "config": artifact(config_snapshot.leaf_config_path),
            "config_lineage": artifact(config_snapshot.bundle_path),
            "input_receipt": artifact(receipt),
            "panel": artifact(panel),
            "score_schedule": artifact(schedule),
            "result": artifact(result),
        },
    }), encoding="utf-8")
    candidate = CandidateSpec(
        "candidate-one", "trend", "paper", {"lookback": 1}, 1,
    )
    monkeypatch.setattr(
        "guvolu.research.holdout.load_frozen_candidates",
        lambda *_args: ((candidate,), candidate_registry),
    )
    monkeypatch.setattr(
        "guvolu.research.holdout.get_active_head_receipt",
        lambda *_args: ActiveHeadReceiptRegistration(
            consumer_kind="holdout",
            consumer_id="evaluation-one",
            market_id="market-one",
            head_generation="head-one",
            receipt_artifact_path="input-receipt.json",
            receipt_artifact_sha256=sha256_file(receipt),
            recorded_at=end,
        ),
    )
    monkeypatch.setattr(
        "guvolu.research.holdout.attest_trade_input_receipt",
        lambda *_args, **_kwargs: FrozenPanelInputs(
            market={"market_id": "market-one"},
            paths=(),
            head_generation="head-one",
            attempt_ids=("attempt-one",),
            artifact_ids=("artifact-one",),
            normalization_versions=("normalization-one",),
            maximum_event_time=end,
        ),
    )
    monkeypatch.setattr(
        "guvolu.research.holdout.build_panel_snapshot",
        lambda *_args: PanelSnapshot(
            market={"market_id": "market-one"},
            bars=(),
            head_generation="head-one",
            attempt_ids=("attempt-one",),
            artifact_ids=("artifact-one",),
            normalization_versions=("normalization-one",),
            panel_path=panel,
            panel_sha256=sha256_file(panel),
            decision_time=end,
            latest_available_time=end,
        ),
    )
    monkeypatch.setattr(
        "guvolu.research.holdout.load_panel_bars",
        lambda *_args: (_DecisionBar(start), _DecisionBar(end)),
    )
    monkeypatch.setattr(
        "guvolu.research.holdout.compute_features",
        lambda *_args: (object(), object()),
    )
    monkeypatch.setattr(
        "guvolu.research.holdout.generate_targets",
        lambda *_args: (0.0, 0.0),
    )
    recomputed = PerformanceMetrics(
        bars=1,
        net_return=-0.1,
        annual_return=-0.1,
        annual_volatility=0.2,
        sharpe=-0.5,
        maximum_drawdown=0.1,
        turnover=1.0,
        annual_turnover=8_760.0,
        hit_rate=0.0,
        exposure=0.0,
        cost=0.01,
        p_value=0.8,
        capacity_score=1.0,
    )
    monkeypatch.setattr(
        "guvolu.research.holdout.evaluate_targets",
        lambda *_args: recomputed,
    )
    with pytest.raises(ValueError, match="candidate metrics"):
        _actual_holdout_attestation(tmp_path, manifest)


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
    config_snapshot = snapshot_verified_config_lineage(
        tmp_path, config, tmp_path / "reports" / "config-artifacts",
    )
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
    candidate_registry.parent.mkdir(parents=True, exist_ok=True)
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
        "pipeline_method_version": "strategy-research-pipeline-v13",
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
            "config": {
                "path": config_snapshot.leaf_config_path.relative_to(
                    tmp_path
                ).as_posix(),
                "sha256": config_snapshot.leaf_config_sha256,
                "bytes": config_snapshot.leaf_config_path.stat().st_size,
            },
            "config_lineage": {
                "path": config_snapshot.bundle_path.relative_to(tmp_path).as_posix(),
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
    receipt_path = tmp_path / "data" / "research" / "input-receipts" / "test.json"
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    receipt_path.write_text("{}\n", encoding="utf-8")
    receipt_sha256 = sha256_file(receipt_path)
    monkeypatch.setattr(
        "guvolu.research.holdout.capture_trade_input_receipt",
        lambda _root, _market, _output: FrozenPanelInputs(
            market={"market_id": "market-one"},
            paths=(),
            head_generation="head-one",
            attempt_ids=("attempt-one",),
            artifact_ids=("artifact-one",),
            normalization_versions=("normalization-one",),
            maximum_event_time=_time("2027-03-01T00:00:00"),
            receipt_path=receipt_path,
            receipt_sha256=receipt_sha256,
        ),
    )
    monkeypatch.setattr(
        "guvolu.research.holdout.register_active_head_receipt",
        lambda _registry, kind, consumer_id, market_id, head, path, sha, **_kwargs:
        ActiveHeadReceiptRegistration(
            consumer_kind=kind,
            consumer_id=consumer_id,
            market_id=market_id,
            head_generation=head,
            receipt_artifact_path=path,
            receipt_artifact_sha256=sha,
            recorded_at=_TEST_NOW,
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
