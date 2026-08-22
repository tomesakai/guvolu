"""多节拍研究套件预登记合同测试。"""
from __future__ import annotations

import copy
import json
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path

import pytest

from guvolu.research.config_lineage import (
    load_governed_strategy_config_with_paths,
)
from guvolu.research.interval_suite import build_interval_suite_plan
from guvolu.research.interval_suite_evidence import (
    allocate_interval_sleeves,
    align_returns_to_interval,
    global_fdr_q_values,
    suite_member_code_commit,
    suite_member_input_identity,
)
from guvolu.research.interval_suite_readiness import (
    aggregate_interval_suite_readiness,
    persist_interval_suite_readiness,
)


def _configs() -> tuple[Path, Path, Path]:
    root = Path(__file__).resolve().parents[1]
    return (
        root,
        root / "config" / "strategy_research.json",
        root / "config" / "strategy_research_4hour.json",
    )


def test_interval_suite_pre_registers_one_global_trial_domain() -> None:
    """同一公式在不同节拍下也必须拥有不同的试验身份。"""
    root, hourly, four_hour = _configs()
    first = build_interval_suite_plan(root, (hourly, four_hour))
    second = build_interval_suite_plan(root, (four_hour, hourly))
    assert first == second
    suite_plan_id = first["suite_plan_id"]
    assert isinstance(suite_plan_id, str)
    assert suite_plan_id.startswith("interval-suite-plan-")
    duration = first["duration_contract"]
    assert isinstance(duration, Mapping)
    assert duration["feature_lookback_seconds"] == [86_400, 259_200, 604_800]
    assert duration["state_lookback_seconds"] == 259_200
    assert duration["volume_lookback_seconds"] == 604_800
    assert duration["maximum_structural_gap_seconds"] == 14_400
    assert duration["minimum_train_seconds"] == 31_536_000
    assert duration["test_seconds"] == 7_776_000
    assert duration["step_seconds"] == 7_776_000
    assert duration["embargo_seconds"] == 86_400
    validation = duration["validation"]
    holdout = duration["holdout_policy"]
    strategies = duration["strategies"]
    assert isinstance(validation, Mapping)
    assert isinstance(holdout, Mapping)
    assert isinstance(strategies, Mapping)
    assert validation["minimum_oos_seconds"] == 28_800_000
    assert validation["block_bootstrap_seconds"] == 604_800
    assert validation["maximum_fdr_q"] == 0.2
    assert holdout["minimum_seconds"] == 7_776_000
    trend = strategies["trend"]
    assert isinstance(trend, Mapping)
    assert trend["lookback_seconds"] == [86_400, 259_200, 604_800]
    members = first["members"]
    assert isinstance(members, list)
    assert all(isinstance(member, Mapping) for member in members)
    assert [member["bar_interval"] for member in members] == [
        "1hour", "4hour",
    ]
    assert [member["candidate_count"] for member in members] == [37, 37]
    domain = first["global_multiple_testing_domain"]
    assert isinstance(domain, list)
    assert all(isinstance(trial, Mapping) for trial in domain)
    assert len(domain) == 86
    assert len({trial["trial_id"] for trial in domain}) == 86
    assert sum(trial["role"] == "candidate_oos_path" for trial in domain) == 74
    assert sum(
        trial["role"] == "walk_forward_family_path" for trial in domain
    ) == 12


def test_interval_suite_rejects_duplicate_interval() -> None:
    """重复加入同一节拍不能伪装成更多时间证据。"""
    root, hourly, _four_hour = _configs()
    with pytest.raises(ValueError, match="重复节拍"):
        build_interval_suite_plan(root, (hourly, hourly))


def test_interval_suite_preloaded_configs_are_strict_and_root_relative(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """预加载模式不得受 cwd 影响或在缺键时静默二次读盘。"""
    root, hourly, four_hour = _configs()
    loaded = {
        path.resolve(): load_governed_strategy_config_with_paths(root, path)
        for path in (hourly, four_hour)
    }
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        "guvolu.research.interval_suite.load_governed_strategy_config_with_paths",
        lambda *_args: pytest.fail("预加载模式不得重新读取配置"),
    )
    relative_paths = tuple(path.relative_to(root) for path in (hourly, four_hour))
    plan = build_interval_suite_plan(
        root, relative_paths, loaded_configs=loaded,
    )
    assert plan["suite_plan_id"]
    with pytest.raises(ValueError, match="预加载套件配置未覆盖路径"):
        build_interval_suite_plan(
            root, relative_paths, loaded_configs={hourly.resolve(): loaded[hourly.resolve()]},
        )


@pytest.mark.parametrize(
    ("path", "value", "message"),
    [
        (("walk_forward", "test_bars"), 539, "墙钟回看或 walk-forward"),
        (("market_id",), "mkt__other", "同一 market_id"),
        (("from_time",), "2020-01-01T00:00:00+00:00", "同一 from_time"),
        (("allocation", "risk_aversion"), 4.0, "同一 allocation"),
        (("features", "state_lookback"), 17, "墙钟回看或 walk-forward"),
        (
            ("features", "maximum_structural_gap_bars_assumption"),
            2,
            "墙钟回看或 walk-forward",
        ),
        (("validation", "minimum_oos_bars"), 1999, "墙钟回看或 walk-forward"),
        (("validation", "block_bootstrap_bars"), 41, "墙钟回看或 walk-forward"),
        (("validation", "maximum_fdr_q"), 0.25, "墙钟回看或 walk-forward"),
        (
            ("data_governance", "holdout_policy", "minimum_bars"),
            539,
            "墙钟回看或 walk-forward",
        ),
        (("strategies", "trend", "lookbacks"), [6, 18, 41], "墙钟回看或 walk-forward"),
    ],
)
def test_interval_suite_rejects_incomparable_members(
    tmp_path: Path,
    path: tuple[str, ...],
    value: object,
    message: str,
) -> None:
    """跨节拍比较必须共享市场和等价的墙钟验证合同。"""
    root, hourly, four_hour = _configs()
    hourly_body = json.loads(hourly.read_text(encoding="utf-8"))
    four_hour_body = json.loads(four_hour.read_text(encoding="utf-8"))
    mutated = copy.deepcopy(four_hour_body)
    target = mutated
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    local_hourly = tmp_path / "hourly.json"
    local_four_hour = tmp_path / "four-hour.json"
    local_hourly.write_text(json.dumps(hourly_body), encoding="utf-8")
    local_four_hour.write_text(json.dumps(mutated), encoding="utf-8")
    with pytest.raises(ValueError, match=message):
        build_interval_suite_plan(
            tmp_path,
            (local_hourly, local_four_hour),
        )


def test_interval_suite_global_fdr_counts_every_registered_path() -> None:
    """套件 BH-FDR 必须一次校正所有节拍的候选和家族路径。"""
    q_values = global_fdr_q_values({
        "one": 0.01,
        "two": 0.04,
        "three": 0.03,
        "four": 0.002,
    })
    assert q_values == pytest.approx({
        "one": 0.02,
        "two": 0.04,
        "three": 0.04,
        "four": 0.008,
    })
    with pytest.raises(ValueError, match="零到一"):
        global_fdr_q_values({"invalid": 1.1})


def test_interval_suite_aligns_returns_without_lookahead() -> None:
    """细节拍收益只可累加到同一最粗柱结束时点。"""
    aligned = align_returns_to_interval({
        "hourly": (
            (datetime(2026, 1, 1, 1, tzinfo=UTC), 0.01),
            (datetime(2026, 1, 1, 2, tzinfo=UTC), 0.02),
            (datetime(2026, 1, 1, 3, tzinfo=UTC), -0.01),
            (datetime(2026, 1, 1, 4, tzinfo=UTC), 0.03),
            (datetime(2026, 1, 1, 5, tzinfo=UTC), 0.04),
        ),
        "four-hour": (
            (datetime(2026, 1, 1, 4, tzinfo=UTC), 0.05),
            (datetime(2026, 1, 1, 8, tzinfo=UTC), 0.06),
        ),
    }, 14_400)
    hour_values = list(aligned["hourly"].values())
    four_hour_values = list(aligned["four-hour"].values())
    assert hour_values == pytest.approx([0.05, 0.04])
    assert four_hour_values == pytest.approx([0.05, 0.06])
    with pytest.raises(ValueError, match="重复收益时间"):
        align_returns_to_interval({
            "duplicate": (
                (datetime(2026, 1, 1, 1, tzinfo=UTC), 0.01),
                (datetime(2026, 1, 1, 1, tzinfo=UTC), 0.02),
            ),
        }, 14_400)


def test_interval_suite_allocator_shares_directional_cap() -> None:
    """同一家族跨节拍不得各自获得一份完整方向风险预算。"""
    sleeves = (
        {
            "sleeve_id": "breakout-hour",
            "family": "breakout",
            "suite_eligible": True,
            "latest_unallocated_target": 1.0,
            "metrics": {
                "annual_return": 0.8,
                "annual_volatility": 0.4,
                "capacity_score": 1.0,
                "bars": 1000,
            },
        },
        {
            "sleeve_id": "breakout-four-hour",
            "family": "breakout",
            "suite_eligible": True,
            "latest_unallocated_target": 1.0,
            "metrics": {
                "annual_return": 0.7,
                "annual_volatility": 0.35,
                "capacity_score": 1.0,
                "bars": 1000,
            },
        },
    )
    aligned = {
        "breakout-hour": {1: 0.01, 2: -0.01, 3: 0.02},
        "breakout-four-hour": {1: 0.009, 2: -0.009, 3: 0.018},
    }
    result = allocate_interval_sleeves(sleeves, aligned, {
        "directional_families": ["trend", "flow_trend", "breakout"],
        "maximum_gross_weight": 0.85,
        "trend_breakout_cap": 0.6,
        "mean_reversion_cap": 0.25,
        "minimum_risk_reserve": 0.15,
        "risk_aversion": 3.0,
        "uncertainty_penalty": 0.1,
        "solver_iterations": 20,
        "solver_step": 0.05,
    }, 14_400)
    weights = result["weights"]
    reserve = result["reserve"]
    assert isinstance(weights, Mapping)
    assert isinstance(reserve, float)
    assert sum(float(value) for value in weights.values()) <= 0.6 + 1e-12
    assert reserve >= 0.4 - 1e-12
    assert result["status"] == "research_only"
    flat = allocate_interval_sleeves(
        ({**sleeves[0], "suite_eligible": False},),
        {"breakout-hour": aligned["breakout-hour"]},
        {},
        14_400,
    )
    assert flat["aggregate_target"] == 0.0
    assert flat["reserve"] == 1.0


def test_interval_suite_input_identity_binds_snapshot_and_data_root() -> None:
    """相同成交 receipt 不能掩盖不同的 L2/control-plane 快照。"""
    manifest = {
        "input_head_generation": "sha256-head",
        "input_receipt_sha256": "receipt",
        "source_data_root": {
            "schema_version": 1,
            "kind": "repository_relative",
            "path": "reports/suite-a",
        },
        "source_data_snapshot": {
            "schema_version": 2,
            "method_version": "hardlinked-minimal-control-plane-v2",
            "snapshot_identity": "snapshot-a",
            "manifest_sha256": "manifest-a",
        },
    }
    baseline = suite_member_input_identity(manifest)
    changed_root = copy.deepcopy(manifest)
    changed_root["source_data_root"]["path"] = "reports/suite-b"
    changed_snapshot = copy.deepcopy(manifest)
    changed_snapshot["source_data_snapshot"]["snapshot_identity"] = "snapshot-b"
    assert suite_member_input_identity(changed_root) != baseline
    assert suite_member_input_identity(changed_snapshot) != baseline
    missing = dict(manifest)
    missing.pop("source_data_snapshot")
    with pytest.raises(ValueError, match="source_data_snapshot"):
        suite_member_input_identity(missing)


def test_interval_suite_requires_one_clean_code_commit() -> None:
    """独立有效的成员不能跨 commit 拼成同一套件证据。"""
    manifest = {
        "code_identity": {
            "git_hash": "a" * 40,
            "dirty": False,
            "decision_grade": True,
        },
    }
    assert suite_member_code_commit(manifest) == "a" * 40
    dirty = copy.deepcopy(manifest)
    dirty["code_identity"]["dirty"] = True
    dirty["code_identity"]["decision_grade"] = False
    with pytest.raises(ValueError, match="clean decision-grade"):
        suite_member_code_commit(dirty)


def test_interval_suite_readiness_aggregates_selected_members() -> None:
    """只有被套件准入的成员影响 operational，promotion 另需 suite 冻结计划。"""
    plan = {"suite_plan_id": "plan"}
    evidence = {
        "suite_plan_id": "plan",
        "suite_evidence_id": "evidence",
        "source_git_hash": "a" * 40,
        "market_id": "market",
        "input_head_generation": "head",
        "input_receipt_sha256": "receipt",
        "operational_status": "disabled_pending_suite_readiness_and_holdout",
        "members": [
            {
                "member_id": "hour",
                "bar_interval": "1hour",
                "run_id": "run-hour",
                "manifest_sha256": "h" * 64,
            },
            {
                "member_id": "four",
                "bar_interval": "4hour",
                "run_id": "run-four",
                "manifest_sha256": "f" * 64,
            },
        ],
        "sleeves": [
            {
                "sleeve_id": "selected",
                "member_id": "hour",
                "suite_eligible": True,
            },
            {
                "sleeve_id": "rejected",
                "member_id": "four",
                "suite_eligible": False,
            },
        ],
        "suite_research_allocation": {
            "status": "research_only",
            "aggregate_target": 0.2,
            "reserve": 0.4,
        },
    }
    readiness = {
        "hour": {
            "run_id": "run-hour",
            "manifest_sha256": "h" * 64,
            "operational": {
                "ready": False,
                "blockers": ["feature_snapshot_stale"],
                "next_action": "wait_for_feature_maturity",
                "current_maximum_event_time": "2026-08-15T00:00:00+00:00",
            },
            "promotion": {
                "ready": False,
                "blockers": ["sealed_holdout_vintage_incomplete"],
                "next_action": "wait_for_sealed_vintage_end",
            },
        },
        "four": {
            "run_id": "run-four",
            "manifest_sha256": "f" * 64,
            "operational": {
                "ready": False,
                "blockers": ["source_code_tree_mismatch"],
                "next_action": "repair_operational_blockers",
                "current_maximum_event_time": "2026-08-15T00:00:00+00:00",
            },
            "promotion": {
                "ready": False,
                "blockers": ["source_code_tree_mismatch"],
                "next_action": "refresh_source_research_before_holdout",
            },
        },
    }
    suite_vintages = ({
        "vintage_id": "suite-vintage",
        "market_id": "market",
        "start_time": "2026-07-01T00:00:00+00:00",
        "end_time": "2026-08-14T00:00:00+00:00",
        "sealed_at": "2026-06-01T00:00:00+00:00",
    },)
    blocked = aggregate_interval_suite_readiness(
        plan,
        evidence,
        readiness,
        datetime(2026, 8, 15, tzinfo=UTC),
        suite_vintages=suite_vintages,
    )
    operational = blocked["operational"]
    promotion = blocked["promotion"]
    assert isinstance(operational, Mapping)
    assert isinstance(promotion, Mapping)
    assert operational["ready"] is False
    assert operational["blockers"] == ["selected_member_operational_not_ready"]
    assert promotion["blockers"] == [
        "suite_frozen_forward_plan_not_registered",
    ]
    assert blocked["selected_member_ids"] == ["hour"]

    ready = copy.deepcopy(readiness)
    ready["hour"]["operational"]["ready"] = True
    ready["hour"]["promotion"]["ready"] = True
    ready["hour"]["promotion"]["blockers"] = []
    aggregated = aggregate_interval_suite_readiness(
        plan,
        evidence,
        ready,
        datetime(2026, 8, 15, tzinfo=UTC),
        suite_vintages=suite_vintages,
    )
    operational = aggregated["operational"]
    promotion = aggregated["promotion"]
    assert isinstance(operational, Mapping)
    assert isinstance(promotion, Mapping)
    assert operational["ready"] is True
    assert promotion["ready"] is False
    assert promotion["next_action"] == "register_suite_frozen_forward_plan"

    planned = aggregate_interval_suite_readiness(
        plan,
        evidence,
        ready,
        datetime(2026, 8, 15, tzinfo=UTC),
        ({
            "plan_id": "suite-forward",
            "vintage_id": "suite-vintage",
            "suite_plan_id": "plan",
            "suite_evidence_id": "evidence",
            "source_git_hash": "a" * 40,
            "plan_artifact_sha256": "p" * 64,
            "prediction_row_set_hash": "row-set",
            "prediction_count": 0,
            "expected_prediction_count": 186,
            "prediction_schedule_complete": False,
            "data_complete": False,
        },),
        suite_vintages,
    )
    planned_promotion = planned["promotion"]
    assert isinstance(planned_promotion, Mapping)
    assert planned_promotion["ready"] is False
    assert planned_promotion["blockers"] == [
        "sealed_suite_holdout_vintage_incomplete",
        "suite_forward_predictions_missing",
    ]
    assert (
        planned_promotion["next_action"]
        == "append_suite_forward_predictions"
    )
    assert planned["suite_frozen_forward_plan_ids"] == ["suite-forward"]


def test_suite_readiness_identity_binds_member_facts(tmp_path: Path) -> None:
    """blocker 名不变时，活动 head 与成熟度变化仍必须产生新身份。"""
    plan = {"suite_plan_id": "plan"}
    evidence = {
        "suite_plan_id": "plan",
        "suite_evidence_id": "evidence",
        "source_git_hash": "a" * 40,
        "market_id": "market",
        "input_head_generation": "head",
        "input_receipt_sha256": "receipt",
        "operational_status": "disabled_pending_suite_readiness_and_holdout",
        "members": [{
            "member_id": "hour", "bar_interval": "1hour",
            "run_id": "run", "manifest_sha256": "m" * 64,
        }],
        "sleeves": [{
            "sleeve_id": "selected", "member_id": "hour",
            "suite_eligible": True,
        }],
        "suite_research_allocation": {
            "status": "research_only", "aggregate_target": 0.1,
            "reserve": 0.9,
        },
    }
    readiness = {"hour": {
        "run_id": "run",
        "manifest_sha256": "m" * 64,
        "source": {"current_git_hash": "a" * 40},
        "operational": {
            "ready": False,
            "blockers": ["latest_panel_feature_not_mature"],
            "next_action": "wait_for_feature_maturity",
            "current_head_generation": "head-1",
            "trailing_contiguous_bars": 10,
        },
        "promotion": {
            "ready": False,
            "blockers": ["sealed_holdout_vintage_incomplete"],
            "next_action": "wait_for_sealed_vintage_end",
        },
    }}
    first = aggregate_interval_suite_readiness(
        plan, evidence, readiness, datetime(2026, 8, 15, tzinfo=UTC),
    )
    changed = copy.deepcopy(readiness)
    changed["hour"]["operational"]["current_head_generation"] = "head-2"
    changed["hour"]["operational"]["trailing_contiguous_bars"] = 11
    second = aggregate_interval_suite_readiness(
        plan, evidence, changed, datetime(2026, 8, 15, tzinfo=UTC),
    )
    assert first["operational"] == second["operational"]
    assert first["suite_readiness_id"] != second["suite_readiness_id"]
    output = persist_interval_suite_readiness(tmp_path, first)
    assert output.is_file()
    assert persist_interval_suite_readiness(tmp_path, first) == output
    assert json.loads(output.read_text(encoding="utf-8")) == first
