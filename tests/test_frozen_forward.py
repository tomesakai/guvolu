"""冻结候选前向预测的端到端合同测试。"""
from __future__ import annotations

import json
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from guvolu.research import clock
from guvolu.research.config_lineage import snapshot_verified_config_lineage
from guvolu.research.contracts import FrozenPanelInputs, PanelSnapshot
from guvolu.research.frozen_forward import (
    attest_frozen_prediction_artifact,
    run_frozen_forward_prediction,
    verify_frozen_forward,
)
from guvolu.research.governance import (
    GOVERNANCE_METHOD_VERSION,
    register_frozen_forward_plan,
    seal_holdout_vintage,
)
from guvolu.research.provenance import (
    canonical_json,
    code_identity,
    sha256_file,
    stable_identifier,
)
from guvolu.strategy.contracts import FeatureRow, ResearchBar
from guvolu.strategy.expression import candidate_identity, expression_id, strategy_expression


def _time(value: str) -> datetime:
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


def _commit_test_repository(root: Path) -> None:
    """提交配置，并让运行制品保持在 Git 身份之外。"""
    (root / ".gitignore").write_text("data/\nreports/\n", encoding="utf-8")
    for command in (
        ("git", "init"),
        ("git", "config", "user.email", "test@example.invalid"),
        ("git", "config", "user.name", "Guvolu Test"),
        ("git", "add", "--all"),
        ("git", "commit", "-m", "test fixture"),
    ):
        subprocess.run(
            command,
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )


def test_frozen_forward_uses_fixed_weight_and_is_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """前向预测只能计算候选目标乘预冻结资金权重。"""
    registry = tmp_path / "data" / "research" / "governance.sqlite3"
    vintage = seal_holdout_vintage(
        registry,
        "market-one",
        _time("2027-01-01T00:00:00"),
        _time("2027-02-01T00:00:00"),
    )
    config_path = tmp_path / "config" / "strategy.json"
    config_path.parent.mkdir(parents=True)
    config_path.write_text(canonical_json({
        "market_id": "market-one",
        "bar_interval": "1hour",
        "from_time": "2026-01-01T00:00:00+00:00",
        "notional_scale": 100_000_000,
        "strategy_decision_max_age_seconds": 3900,
        "data_governance": {"registry": "data/research/governance.sqlite3"},
        "features": {
            "lookbacks": [1],
            "state_lookback": 1,
            "volume_lookback": 1,
            "maximum_structural_gap_bars_assumption": 1,
        },
        "validation": {"minimum_oos_bars": 1},
    }) + "\n", encoding="utf-8")
    plan_path = (
        tmp_path / "reports" / "strategy-research" / "frozen-forward"
        / vintage.vintage_id / "plan" / "plan.json"
    )
    plan_path.parent.mkdir(parents=True)
    config_hash = sha256_file(config_path)
    config_snapshot = snapshot_verified_config_lineage(
        tmp_path, config_path, tmp_path / "reports" / "config-artifacts",
    )
    _commit_test_repository(tmp_path)
    identity = code_identity(tmp_path, config_snapshot.source_paths)
    assert identity.decision_grade
    assert identity.git_hash is not None
    plan_id = stable_identifier("frozen-forward-plan", {
        "governance_method_version": GOVERNANCE_METHOD_VERSION,
        "vintage_id": vintage.vintage_id,
        "source_manifest_sha256": "manifest-hash",
        "candidate_set_hash": "candidate-set-hash",
        "config_hash": config_hash,
        "code_tree_digest": identity.tree_digest,
        "pipeline_method_version": "strategy-research-pipeline-v13",
        "panel_method_version": "trade-bars-pit-v2",
        "panel_schema_version": 2,
        "feature_method_version": "research-features-v2",
        "trade_flow_input_method_version": "economic-trade-basis-v1",
        "trade_input_receipt_method_version": (
            "active-trade-head-receipt-v2"
        ),
    })
    template = strategy_expression("trend")
    parameters: dict[str, int | float] = {
        "annual_volatility_target": 0.4,
        "entry_score": 0.5,
        "exit_score": 0.0,
        "lookback": 1,
        "maximum_target": 1.0,
    }
    plan_payload = {
        "schema_version": 1,
        "method_version": "frozen-forward-v2",
        "governance_method_version": GOVERNANCE_METHOD_VERSION,
        "pipeline_method_version": "strategy-research-pipeline-v13",
        "panel_method_version": "trade-bars-pit-v2",
        "panel_schema_version": 2,
        "feature_method_version": "research-features-v2",
        "trade_flow_input_method_version": "economic-trade-basis-v1",
        "trade_input_receipt_method_version": (
            "active-trade-head-receipt-v2"
        ),
        "scope": "FROZEN_FORWARD",
        "plan_id": plan_id,
        "vintage": {
            "vintage_id": vintage.vintage_id,
            "start_time": vintage.start_time.isoformat(),
            "end_time": vintage.end_time.isoformat(),
        },
        "source": {"manifest_sha256": "manifest-hash"},
        "candidate_set_hash": "candidate-set-hash",
        "config_hash": config_hash,
        "code_identity": {
            "git_hash": identity.git_hash,
            "tree_digest": identity.tree_digest,
        },
        "code_tree_digest": identity.tree_digest,
        "config_path": config_snapshot.leaf_config_path.relative_to(
            tmp_path
        ).as_posix(),
        "config_lineage_path": config_snapshot.bundle_path.relative_to(
            tmp_path
        ).as_posix(),
        "config_lineage_sha256": config_snapshot.bundle_sha256,
        "candidates": [{
            "candidate_id": candidate_identity(template, parameters),
            "family": "trend",
            "mode": "paper",
            "parameters": parameters,
            "complexity": len(parameters),
            "expression_id": expression_id(template),
        }],
        "allocation": {"weights": {"trend": 0.4}, "reserve": 0.6},
    }
    plan_path.write_text(canonical_json(plan_payload) + "\n", encoding="utf-8")
    plan = register_frozen_forward_plan(
        registry,
        vintage.vintage_id,
        "manifest-hash",
        "candidate-set-hash",
        config_hash,
        identity.tree_digest,
        plan_path.relative_to(tmp_path).as_posix(),
        sha256_file(plan_path),
        repository_root=tmp_path,
    )
    assert plan.plan_id == plan_id
    decision = _time("2027-01-02T01:00:00")
    bar = ResearchBar(
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
    panel_file = tmp_path / "data" / "research" / "panel.parquet"
    panel_file.write_bytes(b"panel")
    receipt_file = tmp_path / "data" / "research" / "receipt.json"
    receipt_file.write_text("{}\n", encoding="utf-8")
    panel = PanelSnapshot(
        market={"market_id": "market-one"},
        bars=(bar,),
        head_generation="sha256-head",
        attempt_ids=("attempt-one",),
        artifact_ids=("artifact-one",),
        normalization_versions=("normalization-one",),
        panel_path=panel_file,
        panel_sha256=sha256_file(panel_file),
        decision_time=decision,
        latest_available_time=decision,
    )
    feature = FeatureRow(
        decision_time=decision,
        as_of=decision,
        return_one=0.01,
        trend_scores={1: 1.0},
        volatility={1: 0.1},
        price_scores={1: 0.0},
        prior_highs={1: 101.0},
        prior_lows={1: 99.0},
        flow_imbalance=0.2,
        volume_score=0.1,
        jump_score=0.0,
        contiguous=True,
    )
    monkeypatch.setattr(
        "guvolu.research.frozen_forward.capture_trade_input_receipt",
        lambda *_args: FrozenPanelInputs(
            market={"market_id": "market-one"}, paths=(),
            head_generation="sha256-head", attempt_ids=("attempt-one",),
            artifact_ids=("artifact-one",),
            normalization_versions=("normalization-one",),
            maximum_event_time=decision,
            receipt_path=receipt_file,
            receipt_sha256=sha256_file(receipt_file),
        ),
    )
    monkeypatch.setattr(
        "guvolu.research.frozen_forward.attest_trade_input_receipt",
        lambda *_args, **_kwargs: FrozenPanelInputs(
            market={"market_id": "market-one"}, paths=(),
            head_generation="sha256-head", attempt_ids=("attempt-one",),
            artifact_ids=("artifact-one",),
            normalization_versions=("normalization-one",),
            maximum_event_time=decision,
            receipt_path=receipt_file,
            receipt_sha256=sha256_file(receipt_file),
        ),
    )
    monkeypatch.setattr(
        "guvolu.research.frozen_forward.build_panel_snapshot",
        lambda *_args: panel,
    )
    monkeypatch.setattr(
        "guvolu.research.frozen_forward.load_panel_bars",
        lambda *_args: (bar,),
    )
    monkeypatch.setattr(
        "guvolu.research.frozen_forward.compute_features",
        lambda *_args: (feature,),
    )
    monkeypatch.setattr(
        "guvolu.research.frozen_forward.generate_targets",
        lambda *_args: (0.5,),
    )

    _set_now(decision + timedelta(minutes=1))
    result = run_frozen_forward_prediction(tmp_path, plan.plan_id)
    assert result.aggregate_target == pytest.approx(0.2)
    content = json.loads(result.prediction_path.read_text(encoding="utf-8"))
    assert content["families"][0]["family_target"] == 0.5
    assert content["families"][0]["frozen_allocation_weight"] == 0.4
    _set_now(decision + timedelta(minutes=2))
    assert run_frozen_forward_prediction(tmp_path, plan.plan_id) == result
    verification = verify_frozen_forward(tmp_path, plan.plan_id)
    assert verification.prediction_count == 1

    forged = json.loads(result.prediction_path.read_text(encoding="utf-8"))
    forged["families"][0]["family_target"] = 0.75
    forged["families"][0]["portfolio_target_contribution"] = 0.3
    forged["aggregate_target"] = 0.3
    forged_path = result.prediction_path.parent / "forged.json"
    forged_path.write_text(canonical_json(forged) + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="目标不能由候选公式"):
        attest_frozen_prediction_artifact(
            tmp_path,
            plan_path,
            forged_path,
            decision + timedelta(minutes=1),
        )

    content["aggregate_target"] = 0.3
    result.prediction_path.write_text(
        canonical_json(content) + "\n", encoding="utf-8",
    )
    with pytest.raises(ValueError, match="预测制品散列"):
        verify_frozen_forward(tmp_path, plan.plan_id)
