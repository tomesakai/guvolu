"""跨节拍共同栅格预测的生成、硬门与复核测试。"""
from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from guvolu.research.contracts import (
    CodeIdentity,
    FrozenPanelInputs,
)
from guvolu.research.data_location import data_root_locator
from guvolu.research.governance import (
    GOVERNANCE_METHOD_VERSION,
    IntervalSuiteForwardPlan,
    IntervalSuiteForwardPrediction,
)
from guvolu.research.interval_suite_prediction import (
    attest_interval_suite_forward_prediction,
    run_interval_suite_forward_prediction,
)
from guvolu.research.interval_suite_prediction_identity import (
    interval_suite_forward_prediction_id,
    interval_suite_member_panel_set_hash,
)
from guvolu.research.provenance import canonical_json, sha256_file


def _time(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(UTC)


def _fixture_plan(root: Path) -> tuple[IntervalSuiteForwardPlan, Path]:
    live_root = root / "live-data"
    live_root.mkdir()
    source = root / "config-source.json"
    source.write_text("{}\n", encoding="utf-8")
    plan_id = "interval-suite-forward-plan-one"
    members = [
        {
            "member_id": "member-1h",
            "bar_interval": "1hour",
            "config_source_paths": ["config-source.json"],
            "config_contract": {"bar_interval": "1hour"},
        },
        {
            "member_id": "member-4h",
            "bar_interval": "4hour",
            "config_source_paths": ["config-source.json"],
            "config_contract": {"bar_interval": "4hour"},
        },
    ]
    sleeves = [
        {
            "sleeve_id": "sleeve-1h",
            "member_id": "member-1h",
            "bar_interval": "1hour",
            "family": "trend",
            "candidate": {
                "candidate_id": "candidate-1h",
                "family": "trend",
                "mode": "paper",
                "parameters": {},
                "complexity": 1,
                "expression_id": "expression-1h",
            },
            "weight": 0.3,
        },
        {
            "sleeve_id": "sleeve-4h",
            "member_id": "member-4h",
            "bar_interval": "4hour",
            "family": "mean_reversion",
            "candidate": {
                "candidate_id": "candidate-4h",
                "family": "mean_reversion",
                "mode": "paper",
                "parameters": {},
                "complexity": 1,
                "expression_id": "expression-4h",
            },
            "weight": 0.2,
        },
    ]
    path = root / "reports" / "suite-plan.json"
    path.parent.mkdir(parents=True)
    payload = {
        "governance_registry": "registry.sqlite3",
        "plan_id": plan_id,
        "suite_plan_id": "suite-plan-one",
        "suite_evidence_id": "suite-evidence-one",
        "source_git_hash": "a" * 40,
        "code_tree_digest": "tree-one",
        "deployment_contract_id": "deployment-one",
        "live_data_root": data_root_locator(root, live_root),
        "vintage": {
            "vintage_id": "vintage-one",
            "market_id": "market-one",
            "start_time": "2027-01-01T00:00:00+00:00",
            "end_time": "2027-02-01T00:00:00+00:00",
        },
        "decision_grid": {
            "interval_seconds": 14_400,
            "utc_epoch_offset_seconds": 0,
            "maximum_recording_lag_seconds": 300,
        },
        "members": members,
        "sleeves": sleeves,
        "allocation": {
            "weights": {"sleeve-1h": 0.3, "sleeve-4h": 0.2},
            "reserve": 0.5,
        },
    }
    path.write_text(canonical_json(payload) + "\n", encoding="utf-8")
    plan = IntervalSuiteForwardPlan(
        plan_id=plan_id,
        vintage_id="vintage-one",
        suite_plan_id="suite-plan-one",
        suite_evidence_id="suite-evidence-one",
        source_git_hash="a" * 40,
        code_tree_digest="tree-one",
        plan_artifact_path=path.relative_to(root).as_posix(),
        plan_artifact_sha256=sha256_file(path),
        frozen_at=_time("2026-12-01T00:00:00+00:00"),
    )
    return plan, path


@pytest.mark.parametrize(
    ("failed_member", "expected_target"),
    [(None, 0.10), ("member-4h", 0.0)],
)
def test_suite_prediction_uses_one_common_grid_and_global_quality_gate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failed_member: str | None,
    expected_target: float,
) -> None:
    """全部成员共享 4h 时点，任一成员失败时整个套件归零。"""
    plan, _plan_path = _fixture_plan(tmp_path)
    decision = _time("2027-01-01T04:00:00+00:00")
    evaluated_at = _time("2027-01-01T04:01:00+00:00")
    receipt = tmp_path / "data" / "research" / "input-receipts" / "suite.json"
    receipt.parent.mkdir(parents=True)
    receipt.write_text("{}\n", encoding="utf-8")
    inputs = FrozenPanelInputs(
        market={"market_id": "market-one"},
        paths=(),
        head_generation="sha256-live-head",
        attempt_ids=("attempt-one",),
        artifact_ids=("artifact-one",),
        normalization_versions=("trade-v1",),
        maximum_event_time=_time("2027-01-01T04:01:00+00:00"),
        receipt_path=receipt,
        receipt_sha256=sha256_file(receipt),
    )
    vintage = SimpleNamespace(
        vintage_id="vintage-one",
        market_id="market-one",
        start_time=_time("2027-01-01T00:00:00+00:00"),
        end_time=_time("2027-02-01T00:00:00+00:00"),
        status="sealed",
    )
    monkeypatch.setattr(
        "guvolu.research.interval_suite_prediction.get_interval_suite_forward_plan",
        lambda *_args: plan,
    )
    monkeypatch.setattr(
        "guvolu.research.interval_suite_prediction.get_holdout_vintage",
        lambda *_args: vintage,
    )
    monkeypatch.setattr(
        "guvolu.research.interval_suite_prediction.code_identity",
        lambda *_args: CodeIdentity(
            git_hash="a" * 40,
            tree_digest="tree-one",
            dirty_digest="",
            dirty=False,
            decision_grade=True,
            reason=None,
        ),
    )
    monkeypatch.setattr(
        "guvolu.research.interval_suite_prediction.clock.utc_now",
        lambda: evaluated_at,
    )
    monkeypatch.setattr(
        "guvolu.research.interval_suite_prediction.capture_trade_input_receipt",
        lambda *_args: inputs,
    )
    monkeypatch.setattr(
        "guvolu.research.interval_suite_prediction.list_interval_suite_forward_predictions",
        lambda *_args: (),
    )
    observed_decisions: list[datetime] = []

    def member_result(
        root: Path,
        _payload: object,
        member: object,
        sleeves: object,
        _inputs: object,
        decision_time: datetime,
        _evaluated_at: datetime,
        _output: Path,
        panel_path_override: str | None = None,
    ) -> tuple[dict[str, object], list[dict[str, object]]]:
        assert isinstance(member, dict)
        assert isinstance(sleeves, list)
        member_id = str(member["member_id"])
        observed_decisions.append(decision_time)
        eligible = member_id != failed_member
        panel_path = panel_path_override or f"data/panel-{member_id}.parquet"
        record = {
            "member_id": member_id,
            "bar_interval": member["bar_interval"],
            "panel_path": panel_path,
            "panel_sha256": "1" * 64 if member_id.endswith("1h") else "4" * 64,
            "panel_bytes": 10,
            "decision_time": decision_time.isoformat(),
            "latest_available_time": decision_time.isoformat(),
            "input_head_generation": "sha256-live-head",
            "attempt_ids": ["attempt-one"],
            "artifact_ids": ["artifact-one"],
            "normalization_versions": ["trade-v1"],
            "quality": {
                "eligible": eligible,
                "reasons": [] if eligible else ["strategy_data_stale"],
            },
        }
        targets: list[dict[str, object]] = []
        for raw_sleeve in sleeves:
            assert isinstance(raw_sleeve, dict)
            candidate = raw_sleeve["candidate"]
            assert isinstance(candidate, dict)
            targets.append({
                "sleeve_id": raw_sleeve["sleeve_id"],
                "member_id": member_id,
                "bar_interval": member["bar_interval"],
                "family": raw_sleeve["family"],
                "candidate_id": candidate["candidate_id"],
                "weight": raw_sleeve["weight"],
                "raw_target": 0.5 if member_id.endswith("1h") else -0.25,
            })
        return record, targets

    monkeypatch.setattr(
        "guvolu.research.interval_suite_prediction._member_panel_and_targets",
        member_result,
    )
    registered: list[IntervalSuiteForwardPrediction] = []

    def register(
        _registry: Path,
        plan_id: str,
        decision_time: datetime,
        input_head: str,
        receipt_path: str,
        receipt_sha: str,
        panel_set: str,
        prediction_path: str,
        prediction_sha: str,
        **_kwargs: object,
    ) -> IntervalSuiteForwardPrediction:
        item = IntervalSuiteForwardPrediction(
            prediction_id=interval_suite_forward_prediction_id(
                GOVERNANCE_METHOD_VERSION,
                "interval-suite-frozen-prediction-v1",
                plan_id,
                decision_time,
            ),
            plan_id=plan_id,
            vintage_id="vintage-one",
            decision_time=decision_time,
            input_head_generation=input_head,
            input_receipt_path=receipt_path,
            input_receipt_sha256=receipt_sha,
            member_panel_set_hash=panel_set,
            prediction_artifact_path=prediction_path,
            prediction_artifact_sha256=prediction_sha,
            recorded_at=evaluated_at,
        )
        registered.append(item)
        return item

    monkeypatch.setattr(
        "guvolu.research.interval_suite_prediction.register_interval_suite_forward_prediction",
        register,
    )
    result = run_interval_suite_forward_prediction(
        tmp_path, plan.plan_id, registry_path=Path("registry.sqlite3"),
    )
    assert result.decision_time == decision
    assert observed_decisions == [decision, decision]
    assert result.aggregate_target == pytest.approx(expected_target)
    payload = json.loads(result.prediction_path.read_text(encoding="utf-8"))
    assert payload["operational"]["eligible"] is (failed_member is None)
    assert all(
        sleeve["operational_target"] == 0.0
        for sleeve in payload["sleeves"]
    ) is (failed_member is not None)
    assert payload["member_panel_set_hash"] == interval_suite_member_panel_set_hash(
        plan.plan_id, decision, payload["member_panels"],
    )
    assert registered[0].prediction_id == result.prediction_id

    monkeypatch.setattr(
        "guvolu.research.interval_suite_prediction.attest_trade_input_receipt",
        lambda *_args, **_kwargs: inputs,
    )
    rebuilt = attest_interval_suite_forward_prediction(
        tmp_path, registered[0], plan,
    )
    rebuilt_allocation = rebuilt["allocation"]
    assert isinstance(rebuilt_allocation, dict)
    assert rebuilt_allocation["aggregate_target"] == pytest.approx(
        expected_target,
    )
