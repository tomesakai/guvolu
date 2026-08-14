"""跨节拍冻结计划的高层持久化与历史隔离测试。"""
from __future__ import annotations

import json
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from guvolu.research.contracts import CodeIdentity
from guvolu.research.config_lineage import (
    load_governed_strategy_config_with_paths,
)
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
from guvolu.research.interval_suite import build_interval_suite_plan
from guvolu.research.interval_suite_forward_identity import (
    interval_suite_deployment_contract_id,
    interval_suite_forward_plan_id,
)
from guvolu.research.provenance import stable_identifier
from guvolu.strategy.expression import (
    candidate_identity,
    expression_id,
    strategy_expression,
)


def _write_suite_configs(root: Path, registry: Path) -> tuple[Path, Path]:
    repository = Path(__file__).resolve().parents[1]
    result: list[Path] = []
    for interval, source_name in (
        ("1hour", "strategy_research.json"),
        ("4hour", "strategy_research_4hour.json"),
    ):
        config, _hash, _root, _depth, _paths = (
            load_governed_strategy_config_with_paths(
                repository, repository / "config" / source_name,
            )
        )
        payload = deepcopy(dict(config))
        governance = deepcopy(dict(payload["data_governance"]))
        governance["registry"] = registry.relative_to(root).as_posix()
        payload["data_governance"] = governance
        path = root / f"strategy-{interval}.json"
        path.write_text(
            json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )
        result.append(path)
    return result[0], result[1]


def _suite_contract(
    plan: dict[str, object],
) -> tuple[dict[str, object], list[dict[str, object]]]:
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
    raw_members = plan["members"]
    assert isinstance(raw_members, list)
    first_member = raw_members[0]
    assert isinstance(first_member, dict)
    members = [{
        "member_id": member["member_id"],
        "bar_interval": member["bar_interval"],
    } for member in raw_members if isinstance(member, dict)]
    sleeve["member_id"] = first_member["member_id"]
    sleeve["bar_interval"] = first_member["bar_interval"]
    evidence_body = {
        "suite_plan_id": plan["suite_plan_id"],
        "source_git_hash": "a" * 40,
        "market_id": plan["market_id"],
        "input_head_generation": "head-one",
        "input_receipt_sha256": "r" * 64,
        "alignment_interval_seconds": 14_400,
        "members": members,
        "sleeves": [{
            "sleeve_id": "sleeve-one",
            "member_id": first_member["member_id"],
            "bar_interval": first_member["bar_interval"],
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
    evidence = {
        **evidence_body,
        "suite_evidence_id": stable_identifier(
            "interval-suite-evidence", evidence_body,
        ),
    }
    return evidence, [sleeve]


def test_suite_forward_freezes_evidence_and_serializes_concurrent_writers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """固定输出可被覆盖，但登记计划必须保留不可变副本并幂等。"""
    registry = tmp_path / "suite-governance.sqlite3"
    configs = _write_suite_configs(
        tmp_path, tmp_path / "research-governance.sqlite3",
    )
    plan = dict(build_interval_suite_plan(tmp_path, configs))
    evidence, sleeves = _suite_contract(plan)
    vintage = seal_holdout_vintage(
        registry,
        str(plan["market_id"]),
        datetime(2027, 1, 1, tzinfo=UTC),
        datetime(2027, 2, 1, tzinfo=UTC),
    )
    evidence_path = tmp_path / "reports" / "interval-suite-evidence-v4.json"
    evidence_path.parent.mkdir(parents=True)
    external_live_root = tmp_path.parent / f"{tmp_path.name}-live-data"
    external_live_root.mkdir()
    evidence_path.write_text(
        json.dumps(evidence, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
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
            tmp_path, configs, (), evidence_path, vintage.vintage_id, registry,
            live_data_root=external_live_root,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        first, second = tuple(executor.map(lambda _item: create(), range(2)))
    assert first == second
    registered = get_interval_suite_forward_plan_for_vintage(
        registry, vintage.vintage_id,
    )
    assert registered is not None
    payload = json.loads(first.plan_path.read_text(encoding="utf-8"))
    assert payload["live_data_root"] == {
        "schema_version": 1,
        "kind": "absolute",
        "path": external_live_root.as_posix(),
    }
    assert len(payload["members"]) == 2
    assert all("config_contract" in member for member in payload["members"])
    assert all("config_source_sha256" in member for member in payload["members"])
    frozen_evidence_path = tmp_path / payload["source_evidence"]["path"]
    assert frozen_evidence_path != evidence_path
    assert frozen_evidence_path.name.startswith("suite-evidence-sha256-")
    evidence_path.write_text("{}\n", encoding="utf-8")
    for config_path in configs:
        config_path.write_text("{}\n", encoding="utf-8")
    assert attest_interval_suite_forward_plan(
        tmp_path, registered, plan, evidence,
        expected_registry=Path("suite-governance.sqlite3"),
        expected_live_data_root=external_live_root,
    )["suite_evidence_id"] == evidence["suite_evidence_id"]


def test_suite_forward_identity_binds_live_root_config_and_method() -> None:
    """仅改变部署数据根或配置快照也必须产生不同逻辑计划身份。"""
    member = {
        "member_id": "member-one",
        "bar_interval": "1hour",
        "config_source_path": "config.json",
        "config_source_sha256": "a" * 64,
        "config_lineage_root_sha256": "a" * 64,
        "config_lineage_depth": 0,
        "config_source_paths": ["config.json"],
        "config_contract": {"bar_interval": "1hour"},
        "config_contract_sha256": "b" * 64,
    }
    grid = {"interval_seconds": 3600, "maximum_recording_lag_seconds": 3900}
    first = interval_suite_deployment_contract_id(
        "governance.sqlite3",
        {"schema_version": 1, "kind": "absolute", "path": "C:/live-a"},
        [member],
        grid,
    )
    second = interval_suite_deployment_contract_id(
        "governance.sqlite3",
        {"schema_version": 1, "kind": "absolute", "path": "C:/live-b"},
        [member],
        grid,
    )
    changed_member = {**member, "config_source_sha256": "c" * 64}
    third = interval_suite_deployment_contract_id(
        "governance.sqlite3",
        {"schema_version": 1, "kind": "absolute", "path": "C:/live-a"},
        [changed_member],
        grid,
    )
    assert len({first, second, third}) == 3
    plan_ids = {
        interval_suite_forward_plan_id(
            "research-data-governance-v2", "vintage", "suite", "evidence",
            "a" * 40, "tree", deployment_id,
        )
        for deployment_id in (first, second, third)
    }
    assert len(plan_ids) == 3


def test_verified_suite_state_isolates_history_and_includes_unbound_future(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """旧 sealed vintage 的合法历史计划不得用当前 evidence 强验。"""
    plan = {"suite_plan_id": "suite-plan"}
    evidence = {
        "suite_plan_id": "suite-plan",
        "suite_evidence_id": "suite-evidence",
        "source_git_hash": "a" * 40,
        "market_id": "market-one",
    }
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
        lambda *_args: (tmp_path / "registry.sqlite3", (), {}, {}, {}),
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
