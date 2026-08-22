"""策略运行与一次性 promotion 就绪预检。"""
from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

from guvolu.research.contracts import CodeIdentity, FrozenPanelInputs
from guvolu.research.provenance import sha256_file
from guvolu.research.readiness import strategy_readiness, trailing_contiguous_bars
from guvolu.research.verification import VerificationResult
from guvolu.strategy.contracts import ResearchBar


def _bar(hour: int) -> ResearchBar:
    """生成带一小时决策延迟的确定性研究柱。"""
    opened = datetime(2026, 1, 1, tzinfo=UTC) + timedelta(hours=hour)
    return ResearchBar(
        open_time=opened,
        decision_time=opened + timedelta(hours=1),
        latest_available_time=opened + timedelta(hours=1),
        open=100.0 + hour,
        high=101.0 + hour,
        low=99.0 + hour,
        close=100.5 + hour,
        base_volume=1.0,
        quote_volume=100.0 + hour,
        signed_base_volume=0.1,
        trade_count=1,
    )


def test_trailing_contiguous_bars_stops_at_structural_gap() -> None:
    """成熟度只计算最后一个超限断点之后的观测。"""
    bars = tuple(_bar(hour) for hour in (0, 1, 2, 10, 11))
    assert trailing_contiguous_bars(bars, 1) == 2
    assert trailing_contiguous_bars(bars, 8) == 5


def test_readiness_reports_data_waits_without_mutating_governance(
    tmp_path: Path,
    monkeypatch: object,
) -> None:
    """预检须区分特征成熟、活动 head 和未封存 holdout。"""
    parent_config = tmp_path / "parent-config.json"
    parent_config.write_text(json.dumps({
        "bar_interval": "1hour",
        "strategy_decision_max_age_seconds": 120,
        "features": {
            "lookbacks": [2],
            "volume_lookback": 2,
            "state_lookback": 2,
            "maximum_structural_gap_bars_assumption": 1,
        },
        "data_governance": {"registry": "data/research/governance.sqlite3"},
    }), encoding="utf-8")
    config = tmp_path / "config.json"
    config_body = json.loads(parent_config.read_text(encoding="utf-8"))
    config_body["evolution_parent"] = {
        "parent_config_path": parent_config.relative_to(tmp_path).as_posix(),
        "parent_config_hash": sha256_file(parent_config),
        "lineage_root_config_hash": sha256_file(parent_config),
        "lineage_depth": 1,
    }
    config.write_text(json.dumps(config_body), encoding="utf-8")
    reports = tmp_path / "reports"
    reports.mkdir()
    summary = reports / "summary.json"
    summary.write_text(json.dumps({
        "run_id": "run-one",
        "pipeline_method_version": "strategy-research-pipeline-v14",
        "panel_method_version": "trade-bars-pit-v2",
        "feature_method_version": "research-features-v2",
        "trade_flow_input_method_version": "economic-trade-basis-v1",
        "trade_input_receipt_method_version": "active-trade-head-receipt-v2",
        "operational_gate_method_version": "economic-trade-operational-gate-v1",
        "market_id": "market-one",
        "decision_grade": True,
        "config_hash": sha256_file(config),
        "config_lineage_root_hash": sha256_file(parent_config),
        "config_lineage_depth": 1,
        "code_identity": {"git_hash": "source", "tree_digest": "tree-one"},
        "input": {"head_generation": "published-head"},
        "family_scope": ["trend", "flow_trend"],
        "family_evaluations": [{
            "family": "trend",
            "eligible": True,
            "mode": "paper",
        }],
    }), encoding="utf-8")
    manifest = reports / "manifest.json"
    manifest.write_text(json.dumps({
        "run_id": "run-one",
        "artifacts": {
            "summary_json": {"path": "reports/summary.json"},
            "panel": {"path": "reports/panel.parquet", "sha256": "panel-one"},
        },
    }), encoding="utf-8")
    monkeypatch.setattr(  # type: ignore[attr-defined]
        "guvolu.research.readiness.verify_research_run",
        lambda _root, _manifest: VerificationResult(
            run_id="run-one",
            manifest_path=manifest,
            manifest_sha256="manifest-one",
            checked_artifacts=("panel", "summary_json"),
        ),
    )
    monkeypatch.setattr(  # type: ignore[attr-defined]
        "guvolu.research.readiness.freeze_trade_inputs",
        lambda _root, _market: FrozenPanelInputs(
            market={"market_id": "market-one"},
            paths=(),
            head_generation="current-head",
            attempt_ids=(),
            artifact_ids=(),
            normalization_versions=(),
            maximum_event_time=_bar(20).decision_time,
        ),
    )
    monkeypatch.setattr(  # type: ignore[attr-defined]
        "guvolu.research.tuning.verify_evolution_config",
        lambda *_args: None,
    )

    def current_identity(
        _root: Path,
        paths: tuple[Path, ...],
    ) -> CodeIdentity:
        assert paths == (config.resolve(), parent_config.resolve())
        return CodeIdentity(
            git_hash="current",
            tree_digest="tree-one",
            dirty_digest="clean",
            dirty=False,
            decision_grade=True,
            reason=None,
        )

    monkeypatch.setattr(  # type: ignore[attr-defined]
        "guvolu.research.readiness.code_identity",
        current_identity,
    )
    bars = tuple(_bar(hour) for hour in (0, 1, 2, 10)) + (
        replace(
            _bar(11), base_volume=0.0, quote_volume=0.0,
            signed_base_volume=0.0, trade_count=0,
            unqualified_trade_count=1, volume_qualified=False,
        ),
    )
    monkeypatch.setattr(  # type: ignore[attr-defined]
        "guvolu.research.readiness.load_panel_bars", lambda _path: bars,
    )
    monkeypatch.setattr(  # type: ignore[attr-defined]
        "guvolu.research.readiness.list_holdout_vintages", lambda _path: (),
    )

    result = strategy_readiness(
        tmp_path,
        config,
        manifest,
        reference_time=_bar(11).decision_time,
    )

    operational = result["operational"]
    assert isinstance(operational, dict)
    assert operational["ready"] is False
    assert operational["remaining_maturity_bars"] == 1
    assert operational["blockers"] == [
        "latest_panel_feature_not_mature",
        "feature_snapshot_stale",
        "active_input_head_changed",
    ]
    promotion = result["promotion"]
    assert isinstance(promotion, dict)
    assert promotion["ready"] is False
    assert promotion["blockers"] == ["no_sealed_holdout_vintage"]
    assert promotion["next_action"] == "seal_future_vintage_before_its_start"
    assert result["read_only"] is True

    summary_payload = json.loads(summary.read_text(encoding="utf-8"))
    summary_payload["family_evaluations"].append({
        "family": "flow_trend", "eligible": True, "mode": "paper",
    })
    summary.write_text(json.dumps(summary_payload), encoding="utf-8")
    flow_result = strategy_readiness(
        tmp_path,
        config,
        manifest,
        reference_time=_bar(11).decision_time,
    )
    flow_operational = flow_result["operational"]
    assert isinstance(flow_operational, dict)
    assert "latest_economic_trade_volume_unqualified" in (
        flow_operational["blockers"]
    )
    flow_promotion = flow_result["promotion"]
    assert isinstance(flow_promotion, dict)
    assert "source_economic_trade_volume_unqualified" in (
        flow_promotion["blockers"]
    )


def test_readiness_blocks_operational_config_mismatch(
    tmp_path: Path,
    monkeypatch: object,
) -> None:
    """旧运行配置不得被当前配置误报为可执行就绪。"""
    config = tmp_path / "config.json"
    config.write_text(json.dumps({
        "bar_interval": "1hour",
        "strategy_decision_max_age_seconds": 7_200,
        "features": {
            "lookbacks": [2],
            "volume_lookback": 2,
            "state_lookback": 2,
            "maximum_structural_gap_bars_assumption": 1,
        },
        "data_governance": {"registry": "data/research/governance.sqlite3"},
    }), encoding="utf-8")
    reports = tmp_path / "reports"
    reports.mkdir()
    summary = reports / "summary.json"
    summary.write_text(json.dumps({
        "run_id": "run-one",
        "pipeline_method_version": "strategy-research-pipeline-v14",
        "panel_method_version": "trade-bars-pit-v2",
        "feature_method_version": "research-features-v2",
        "trade_flow_input_method_version": "economic-trade-basis-v1",
        "trade_input_receipt_method_version": "active-trade-head-receipt-v2",
        "operational_gate_method_version": "economic-trade-operational-gate-v1",
        "market_id": "market-one",
        "decision_grade": True,
        "config_hash": "different-config",
        "config_lineage_root_hash": sha256_file(config),
        "config_lineage_depth": 0,
        "code_identity": {"git_hash": "source", "tree_digest": "tree-one"},
        "input": {"head_generation": "current-head"},
        "family_evaluations": [{
            "family": "trend",
            "eligible": True,
            "mode": "paper",
        }],
    }), encoding="utf-8")
    manifest = reports / "manifest.json"
    manifest.write_text(json.dumps({
        "run_id": "run-one",
        "artifacts": {
            "summary_json": {"path": "reports/summary.json"},
            "panel": {"path": "reports/panel.parquet", "sha256": "panel-one"},
        },
    }), encoding="utf-8")
    monkeypatch.setattr(  # type: ignore[attr-defined]
        "guvolu.research.readiness.verify_research_run",
        lambda _root, _manifest: VerificationResult(
            run_id="run-one",
            manifest_path=manifest,
            manifest_sha256="manifest-one",
            checked_artifacts=("panel", "summary_json"),
        ),
    )
    current_inputs = FrozenPanelInputs(
        market={"market_id": "market-one"},
        paths=(),
        head_generation="current-head",
        attempt_ids=(),
        artifact_ids=(),
        normalization_versions=(),
        maximum_event_time=_bar(4).decision_time,
    )
    monkeypatch.setattr(  # type: ignore[attr-defined]
        "guvolu.research.readiness.freeze_trade_inputs",
        lambda _root, _market: current_inputs,
    )
    monkeypatch.setattr(  # type: ignore[attr-defined]
        "guvolu.research.readiness.code_identity",
        lambda _root, _paths: CodeIdentity(
            git_hash="current",
            tree_digest="tree-one",
            dirty_digest="clean",
            dirty=False,
            decision_grade=True,
            reason=None,
        ),
    )
    bars = tuple(_bar(hour) for hour in range(5))
    monkeypatch.setattr(  # type: ignore[attr-defined]
        "guvolu.research.readiness.load_panel_bars", lambda _path: bars,
    )
    monkeypatch.setattr(  # type: ignore[attr-defined]
        "guvolu.research.readiness.list_holdout_vintages", lambda _path: (),
    )
    result = strategy_readiness(
        tmp_path, config, manifest, reference_time=_bar(4).decision_time,
    )
    operational = result["operational"]
    assert isinstance(operational, dict)
    assert operational["ready"] is False
    assert operational["blockers"] == ["source_config_mismatch"]
