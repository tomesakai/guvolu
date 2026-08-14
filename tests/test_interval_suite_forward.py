"""跨节拍冻结计划的高层持久化与历史隔离测试。"""
from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from guvolu.research.contracts import CodeIdentity
from guvolu.research.governance import (
    IntervalSuiteForwardPlan,
    get_interval_suite_forward_plan_for_vintage,
    seal_holdout_vintage,
)
from guvolu.research.interval_suite_forward import (
    attest_interval_suite_forward_plan,
    freeze_interval_suite_forward_plan,
    verified_interval_suite_forward_state,
)
from guvolu.strategy.expression import (
    candidate_identity,
    expression_id,
    strategy_expression,
)


def _suite_contract() -> tuple[dict[str, object], dict[str, object], list[dict[str, object]]]:
    parameters: dict[str, int | float] = {
        "annual_volatility_target": 0.4,
        "entry_score": 0.5,
        "exit_score": 0.0,
        "lookback": 168,
        "maximum_target": 1.0,
    }
    template = strategy_expression("trend")
    candidate_id = candidate_identity(template, parameters)
    sleeve = {
        "sleeve_id": "sleeve-one",
        "member_id": "member-one",
        "bar_interval": "1hour",
        "family": "trend",
        "candidate": {
            "candidate_id": candidate_id,
            "family": "trend",
            "mode": "paper",
            "expression_id": expression_id(template),
            "parameters": parameters,
            "complexity": len(parameters),
        },
        "weight": 0.4,
    }
    plan = {"suite_plan_id": "suite-plan"}
    evidence = {
        "suite_plan_id": "suite-plan",
        "suite_evidence_id": "suite-evidence",
        "source_git_hash": "a" * 40,
        "market_id": "market-one",
        "input_head_generation": "head-one",
        "input_receipt_sha256": "r" * 64,
        "alignment_interval_seconds": 14_400,
        "sleeves": [{
            "sleeve_id": "sleeve-one",
            "member_id": "member-one",
            "bar_interval": "1hour",
            "family": "trend",
            "deployment_candidate_id": candidate_id,
            "suite_eligible": True,
        }],
        "suite_research_allocation": {
            "weights": {"sleeve-one": 0.4},
            "reserve": 0.6,
            "shared_caps": {"maximum_gross_weight": 0.85},
        },
    }
    return plan, evidence, [sleeve]


def test_suite_forward_freezes_evidence_and_serializes_concurrent_writers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """固定输出可被覆盖，但登记计划必须保留不可变副本并幂等。"""
    registry = tmp_path / "suite-governance.sqlite3"
    vintage = seal_holdout_vintage(
        registry,
        "market-one",
        datetime(2027, 1, 1, tzinfo=UTC),
        datetime(2027, 2, 1, tzinfo=UTC),
    )
    plan, evidence, sleeves = _suite_contract()
    evidence_path = tmp_path / "reports" / "interval-suite-evidence-v4.json"
    evidence_path.parent.mkdir(parents=True)
    evidence_path.write_text(
        json.dumps(evidence, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "guvolu.research.interval_suite_forward._config_contract",
        lambda *_args: (
            registry,
            (),
            {"1hour": {"strategy_decision_max_age_seconds": 3900}},
        ),
    )
    monkeypatch.setattr(
        "guvolu.research.interval_suite_forward.build_interval_suite_plan",
        lambda *_args: plan,
    )
    monkeypatch.setattr(
        "guvolu.research.interval_suite_forward.evaluate_interval_suite",
        lambda *_args: evidence,
    )
    monkeypatch.setattr(
        "guvolu.research.interval_suite_forward.code_identity",
        lambda *_args: CodeIdentity(
            git_hash="b" * 40,
            tree_digest="tree-one",
            dirty_digest="",
            dirty=False,
            decision_grade=True,
            reason=None,
        ),
    )
    monkeypatch.setattr(
        "guvolu.research.interval_suite_forward._frozen_sleeves",
        lambda *_args: sleeves,
    )

    def create() -> object:
        return freeze_interval_suite_forward_plan(
            tmp_path, (), (), evidence_path, vintage.vintage_id, registry,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        first, second = tuple(executor.map(lambda _item: create(), range(2)))
    assert first == second
    registered = get_interval_suite_forward_plan_for_vintage(
        registry, vintage.vintage_id,
    )
    assert registered is not None
    payload = json.loads(first.plan_path.read_text(encoding="utf-8"))
    frozen_evidence_path = tmp_path / payload["source_evidence"]["path"]
    assert frozen_evidence_path != evidence_path
    assert frozen_evidence_path.name.startswith("suite-evidence-sha256-")
    evidence_path.write_text("{}\n", encoding="utf-8")
    assert attest_interval_suite_forward_plan(
        tmp_path, registered, plan, evidence,
        expected_registry=Path("suite-governance.sqlite3"),
    )["suite_evidence_id"] == "suite-evidence"


def test_verified_suite_state_isolates_history_and_includes_unbound_future(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """旧 sealed vintage 的合法历史计划不得用当前 evidence 强验。"""
    plan, evidence, _sleeves = _suite_contract()
    historical = IntervalSuiteForwardPlan(
        plan_id="historical-plan",
        vintage_id="historical-vintage",
        suite_plan_id="old-suite-plan",
        suite_evidence_id="old-evidence",
        source_git_hash="c" * 40,
        code_tree_digest="old-tree",
        plan_artifact_path="reports/old-plan.json",
        plan_artifact_sha256="d" * 64,
        frozen_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    historical_vintage = SimpleNamespace(
        status="sealed",
        market_id="market-one",
        vintage_id="historical-vintage",
        start_time=datetime(2026, 1, 1, tzinfo=UTC),
        end_time=datetime(2026, 2, 1, tzinfo=UTC),
        sealed_at=datetime(2025, 12, 1, tzinfo=UTC),
    )
    future_vintage = SimpleNamespace(
        status="sealed",
        market_id="market-one",
        vintage_id="future-vintage",
        start_time=datetime(2027, 1, 1, tzinfo=UTC),
        end_time=datetime(2027, 2, 1, tzinfo=UTC),
        sealed_at=datetime(2026, 8, 1, tzinfo=UTC),
    )
    legacy_future_vintage = SimpleNamespace(
        status="sealed",
        market_id="market-one",
        vintage_id="legacy-future-vintage",
        start_time=datetime(2026, 9, 1, tzinfo=UTC),
        end_time=datetime(2026, 10, 1, tzinfo=UTC),
        sealed_at=datetime(2026, 7, 1, tzinfo=UTC),
    )
    monkeypatch.setattr(
        "guvolu.research.interval_suite_forward._config_contract",
        lambda *_args: (tmp_path / "registry.sqlite3", (), {}),
    )
    monkeypatch.setattr(
        "guvolu.research.interval_suite_forward.list_holdout_vintages",
        lambda *_args: (
            historical_vintage, legacy_future_vintage, future_vintage,
        ),
    )
    monkeypatch.setattr(
        "guvolu.research.interval_suite_forward.get_interval_suite_forward_plan_for_vintage",
        lambda _registry, vintage_id: (
            historical if vintage_id == "historical-vintage" else None
        ),
    )
    monkeypatch.setattr(
        "guvolu.research.interval_suite_forward.get_frozen_forward_plan_for_vintage",
        lambda _registry, vintage_id: (
            object() if vintage_id == "legacy-future-vintage" else None
        ),
    )
    monkeypatch.setattr(
        "guvolu.research.interval_suite_forward.attest_interval_suite_forward_plan",
        lambda *_args: pytest.fail("不应使用当前 evidence 强验历史计划"),
    )
    state = verified_interval_suite_forward_state(
        tmp_path,
        (),
        plan,
        evidence,
        reference_time=datetime(2026, 8, 15, tzinfo=UTC),
    )
    assert state["plans"] == ()
    assert state["sealed_vintages"] == ({
        "vintage_id": "future-vintage",
        "market_id": "market-one",
        "start_time": "2027-01-01T00:00:00+00:00",
        "end_time": "2027-02-01T00:00:00+00:00",
        "sealed_at": "2026-08-01T00:00:00+00:00",
    },)
