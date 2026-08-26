"""行业稳健性证据接入研究管线的窗口、开关与失败关闭测试。

数据面用替身，管线装配、证据生成与检查器判定全部真实执行。
"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest

from guvolu.research.contracts import (
    CodeIdentity,
    FamilyEvaluation,
    FrozenPanelInputs,
    PanelSnapshot,
    PerformanceMetrics,
    TrialRecord,
)
from guvolu.research.industry_readiness import (
    evaluate_industry_strategy_readiness,
    load_industry_readiness_policy,
)
from guvolu.research.pipeline import run_research
from guvolu.research.validation import ValidationResult, WalkForwardFold
from guvolu.strategy.contracts import CandidateSpec, ResearchBar

_MARKET = "mkt__gmo__btc__r0"
_FAMILY = "trend"
_CANDIDATE = "candidate-trend-one"
_BARS = 240
_START = datetime(2026, 8, 1, 0, 0, tzinfo=UTC)
_TEST_START = 120
_TEST_END = 230


def _metrics() -> PerformanceMetrics:
    """构造达阈的合成绩效指标。"""
    return PerformanceMetrics(
        bars=_TEST_END - _TEST_START,
        net_return=0.4,
        annual_return=0.2,
        annual_volatility=0.2,
        sharpe=1.2,
        maximum_drawdown=0.2,
        turnover=100.0,
        annual_turnover=12.0,
        hit_rate=0.55,
        exposure=0.4,
        cost=0.1,
        p_value=0.01,
        capacity_score=1.0,
    )


def _candidate() -> CandidateSpec:
    """构造合成部署候选。"""
    return CandidateSpec(
        candidate_id=_CANDIDATE,
        family=_FAMILY,
        mode="paper",
        parameters={"lookback": 8},
        complexity=1,
    )


def _bars() -> tuple[ResearchBar, ...]:
    """构造确定性研究柱序列（G-03）。"""
    rows: list[ResearchBar] = []
    price = 100.0
    for index in range(_BARS):
        price *= 1.0 + 0.0004 * (1 if index % 3 else -1)
        decision = _START + timedelta(hours=index + 1)
        rows.append(ResearchBar(
            open_time=_START + timedelta(hours=index),
            decision_time=decision,
            latest_available_time=decision,
            open=price,
            high=price * 1.002,
            low=price * 0.998,
            close=price,
            base_volume=10.0,
            quote_volume=price * 10.0,
            signed_base_volume=1.0,
            trade_count=50,
            source_trade_count=50,
            unqualified_trade_count=0,
            volume_qualified=True,
        ))
    return tuple(rows)


def _validation(bars: Sequence[ResearchBar], eligible: bool) -> ValidationResult:
    """构造合成 walk-forward 结果。"""
    targets = tuple(
        1.0 if index % 2 else 0.5 for index in range(len(bars))
    )
    evaluation = FamilyEvaluation(
        family=_FAMILY,
        mode="paper",
        deployment_candidate=_candidate(),
        latest_target=1.0,
        deployment_oos_metrics=_metrics(),
        deployment_oos_returns=(0.001, 0.002),
        metrics=_metrics(),
        adjusted_sharpe=1.0,
        fdr_q=0.01,
        eligible=eligible,
        rejection_reasons=(),
        oos_returns=(0.001, 0.002),
        positive_fold_ratio=1.0,
        probability_backtest_overfitting=0.1,
        block_bootstrap_p_value=0.01,
    )
    trial = TrialRecord(
        evaluation_id="evaluation-one",
        candidate=_candidate(),
        fold_id="fold-001",
        segment="test",
        start_time=bars[_TEST_START].decision_time,
        end_time=bars[_TEST_END - 1].decision_time,
        selected=True,
        metrics=_metrics(),
    )
    return ValidationResult(
        families=(evaluation,),
        trials=(trial,),
        candidate_targets={_CANDIDATE: targets},
        folds=(
            WalkForwardFold(
                fold_id="fold-001",
                train_start=0,
                train_end=_TEST_START,
                test_start=_TEST_START,
                test_end=(_TEST_START + _TEST_END) // 2,
            ),
            WalkForwardFold(
                fold_id="fold-002",
                train_start=0,
                train_end=(_TEST_START + _TEST_END) // 2,
                test_start=(_TEST_START + _TEST_END) // 2,
                test_end=_TEST_END,
            ),
        ),
        family_validation_targets={_FAMILY: targets},
    )


def _config(root: Path, switch: object) -> Mapping[str, object]:
    """构造小型研究配置；阈值一律来自配置（G-06）。"""
    research: dict[str, object] = {
        "industry_evidence_config": "config/industry_evidence.json",
    }
    if switch is not None:
        research["generate_industry_evidence"] = switch
    del root
    return {
        "schema_version": 1,
        "market_id": _MARKET,
        "bar_interval": "1hour",
        "from_time": _START.isoformat(),
        "notional_scale": 100_000_000,
        "strategy_decision_max_age_seconds": 3900,
        "research": research,
        "data_governance": {
            "scope": "DEV_ADAPTIVE",
            "registry": "data/research/governance.sqlite3",
        },
        "cost_model": {
            "fee_bps_assumption": 5.0,
            "half_spread_bps_assumption": 2.0,
            "slippage_bps_assumption": 2.0,
            "impact_bps_assumption": 1.0,
            "capacity_notional_quote": 100_000.0,
        },
        "features": {
            "lookbacks": [4, 8],
            "state_lookback": 8,
            "volume_lookback": 8,
            "maximum_structural_gap_bars_assumption": 4,
        },
        "validation": {
            "minimum_oos_bars": 10,
            "block_bootstrap_bars": 8,
            "block_bootstrap_random_seed": 20260814,
        },
        "allocation": {
            "directional_families": [_FAMILY],
            "maximum_gross_weight": 0.85,
            "trend_breakout_cap": 0.6,
            "mean_reversion_cap": 0.25,
            "minimum_risk_reserve": 0.15,
            "l2_overlay_limit": 0.3,
            "risk_aversion": 3.0,
            "turnover_penalty": 0.02,
            "uncertainty_penalty": 0.1,
            "no_trade_band": 0.01,
            "solver_iterations": 50,
            "solver_step": 0.05,
        },
        "cross_venue_shadow": {"market_ids": [_MARKET]},
    }


def _evidence_settings() -> Mapping[str, object]:
    """构造小面板可用的证据阈值网格。"""
    return {
        "schema_version": 1,
        "method_version": "industry-evidence-v2",
        "cost": {
            "tier_multipliers": {
                "policy_baseline": 1.0, "adverse": 1.5, "severe": 2.0,
            },
            "minimum_step_bps": 5.0,
        },
        "tail": {"probabilities": [0.01, 0.025, 0.05],
                 "bootstrap_samples": 16},
        "stress": {
            "volatility_lookback_bars": 8,
            "volatility_quantile": 0.9,
            "volume_quantile": 0.1,
            "cross_venue_quantile": 0.9,
            "minimum_subinterval_bars": 10,
            "cross_venue_market_ids": [_MARKET],
        },
        "capacity": {
            "notional_quote_grid": [100_000.0, 200_000.0, 400_000.0],
            "depth_quantile": 0.5,
            "depth_horizon_seconds": 3600,
            "depth_sample_count": 4,
            "depth_levels": 20,
            "minimum_depth_samples": 2,
            "execution_market_id": _MARKET,
            "venue_market_ids": [_MARKET],
        },
    }


class _Exposure:
    """研究暴露登记结果替身。"""

    exposure_id = "exposure-one"
    start_time = _START
    end_time = _START + timedelta(hours=_BARS)


class _Receipt:
    """活动 head 收据登记结果替身。"""

    receipt_artifact_sha256 = "b" * 64


class _Lineage:
    """配置血缘快照替身。"""

    def __init__(self, path: Path) -> None:
        """记录快照位置。"""
        self.leaf_config_path = path
        self.bundle_path = path


def _prepare(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    switch: object = None,
    eligible: bool = True,
) -> tuple[Path, Path]:
    """搭建替身数据面并返回项目根与配置位置。"""
    root = tmp_path / "repo"
    (root / "config").mkdir(parents=True)
    (root / "data" / "research").mkdir(parents=True)
    config_body = _config(root, switch)
    config_path = root / "config" / "strategy_research.json"
    config_path.write_text(
        json.dumps(config_body, ensure_ascii=False), encoding="utf-8",
    )
    (root / "config" / "industry_evidence.json").write_text(
        json.dumps(_evidence_settings(), ensure_ascii=False), encoding="utf-8",
    )
    receipt_path = root / "data" / "research" / "input-receipt.json"
    receipt_path.write_text("{}\n", encoding="utf-8")
    panel_path = root / "data" / "research" / "panel.parquet"
    panel_path.write_bytes(b"PAR1research-panelPAR1")
    bars = _bars()
    inputs = FrozenPanelInputs(
        market={"market_id": _MARKET},
        paths=(),
        head_generation="head-one",
        attempt_ids=("attempt-one",),
        artifact_ids=("artifact-one",),
        normalization_versions=("normalization-one",),
        maximum_event_time=bars[-1].decision_time,
        receipt_path=receipt_path,
        receipt_sha256="a" * 64,
        source_trade_rows=1000,
        economic_trade_rows=1000,
        unqualified_trade_rows=0,
        volume_qualified=True,
    )
    snapshot = PanelSnapshot(
        market={"market_id": _MARKET},
        bars=bars,
        head_generation="head-one",
        attempt_ids=("attempt-one",),
        artifact_ids=("artifact-one",),
        normalization_versions=("normalization-one",),
        panel_path=panel_path,
        panel_sha256=hashlib.sha256(panel_path.read_bytes()).hexdigest(),
        decision_time=bars[-1].decision_time,
        latest_available_time=bars[-1].decision_time,
    )
    patches: Mapping[str, Any] = {
        "load_governed_strategy_config_with_paths": (
            lambda _root, path: (
                config_body, "c" * 64, "c" * 64, 1, (path,),
            )
        ),
        "snapshot_verified_config_lineage": (
            lambda _root, path, _directory: _Lineage(path)
        ),
        "code_identity": lambda _root, _paths: CodeIdentity(
            git_hash="commit-one",
            tree_digest="tree-one",
            dirty_digest="dirty-one",
            dirty=False,
            decision_grade=True,
            reason=None,
        ),
        "suite_data_snapshot_record": lambda _root: {"snapshot": "one"},
        "data_root_locator": lambda _root, _data: {"kind": "project"},
        "capture_trade_input_receipt": (
            lambda _data, _market, _directory: inputs
        ),
        "build_panel_snapshot": lambda *_args: snapshot,
        "reject_sealed_conflict": lambda *_args: None,
        "register_research_exposure": lambda *_args: _Exposure(),
        "register_active_head_receipt": (
            lambda *_args, **_kwargs: _Receipt()
        ),
        "walk_forward_validate": (
            lambda *_args, **_kwargs: _validation(bars, eligible)
        ),
        "build_family_batches": lambda *_args: (),
        "candidate_registry_payload": lambda *_args: {
            "schema_version": 1, "candidates": [],
        },
        "latest_common_l2_decision": (
            lambda *_args: bars[-1].decision_time
        ),
        "cross_venue_shadow": lambda *_args: {
            "decision_time": bars[-1].decision_time.isoformat(),
        },
        "l2_overlay_from_shadow": lambda *_args: (
            0.0, {"available": False},
        ),
    }
    for name, value in patches.items():
        monkeypatch.setattr(f"guvolu.research.pipeline.{name}", value)
    return root, config_path


def _summary(root: Path, run_directory: Path) -> Mapping[str, object]:
    """读取运行摘要。"""
    del root
    body = (run_directory / "summary.json").read_text(encoding="utf-8")
    return cast(Mapping[str, object], json.loads(body))


def _manifest(run_directory: Path) -> Mapping[str, object]:
    """读取运行清单。"""
    body = (run_directory / "manifest.json").read_text(encoding="utf-8")
    return cast(Mapping[str, object], json.loads(body))


def _artifact_records(
    manifest: Mapping[str, object],
) -> Mapping[str, Mapping[str, object]]:
    """收窄 manifest 制品登记。"""
    artifacts = manifest.get("artifacts")
    assert isinstance(artifacts, Mapping)
    return {
        str(name): cast(Mapping[str, object], record)
        for name, record in artifacts.items()
    }


def _reference(
    name: str, record: Mapping[str, object],
) -> Mapping[str, object]:
    """构造检查器要求的制品身份记录。"""
    return {
        "name": name,
        "kind": record.get("kind"),
        "path": record.get("path"),
        "sha256": record.get("sha256"),
        "artifact_id": f"sha256-{record.get('sha256')}",
        "bytes": record.get("bytes"),
    }


def _checker_evidence(
    root: Path, run_directory: Path,
) -> Mapping[str, object]:
    """把真实运行制品装配成检查器可消费的只读证据。"""
    manifest = _manifest(run_directory)
    summary = _summary(root, run_directory)
    records = _artifact_records(manifest)
    payload = json.loads(
        (root / str(records["industry_evidence"]["path"])).read_text(
            encoding="utf-8",
        )
    )
    attestation = json.loads(
        (
            root
            / str(records["industry_evidence_generator_attestation"]["path"])
        ).read_text(encoding="utf-8")
    )
    verified = {
        name: {**_reference(name, record), "snapshot_verified": True}
        for name, record in records.items()
    }
    return {
        "research": {
            "verified": True,
            "semantic_verified": True,
            "run_id": manifest.get("run_id"),
            "manifest_sha256": "a" * 64,
            "manifest": dict(manifest),
            "summary": dict(summary),
            "research_config": {
                "cost_model": {"capacity_notional_quote": 100_000.0},
            },
            "checked_artifacts": sorted(records),
            "industry_evidence": {
                "present": True,
                "verified": True,
                "artifact": _reference(
                    "industry_evidence", records["industry_evidence"],
                ),
                "payload": payload,
                "generator_attestation_artifact": _reference(
                    "industry_evidence_generator_attestation",
                    records["industry_evidence_generator_attestation"],
                ),
                "generator_attestation_payload": attestation,
                "verified_artifacts": verified,
            },
            "trial_ledger": {
                "present": True,
                "header": {"record_type": "trial_ledger_header"},
                "trial_rows": 1,
                "evaluation_id_count": 1,
                "unique_evaluation_id_count": 1,
                "missing_registry_candidate_ids": [],
            },
        },
        "forward": {"registry_present": True, "vintages": []},
        "paper": {"execution_root_provided": False},
        "execution": {"attestation_present": False},
    }


def _robustness_reasons(evidence: Mapping[str, object]) -> tuple[str, ...]:
    """运行正式检查器并取稳健性门禁原因码。"""
    policy = load_industry_readiness_policy(
        Path("config/industry_strategy_readiness.json")
    )
    result = evaluate_industry_strategy_readiness(
        policy, evidence, evaluated_at=datetime.now(UTC),
    )
    gates = result["gates"]
    assert isinstance(gates, list)
    gate = next(
        item for item in gates if item["gate_id"] == "robustness_evidence"
    )
    codes = gate["reason_codes"]
    assert isinstance(codes, list)
    return tuple(str(code) for code in codes)


def test_generated_evidence_falls_inside_registration_window(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """管线内生成的证据时刻落在注册窗口内，两个截止码不再报出。"""
    root, config_path = _prepare(tmp_path, monkeypatch)
    result = run_research(root, config_path, root / "out")
    manifest = _manifest(result.run_directory)
    evidence = manifest.get("industry_evidence")
    assert isinstance(evidence, Mapping)
    assert evidence.get("status") == "generated"
    decision = datetime.fromisoformat(str(manifest["decision_time"]))
    cutoff = datetime.fromisoformat(str(manifest["execution_evaluated_at"]))
    generated = datetime.fromisoformat(str(evidence["generated_at"]))
    attested = datetime.fromisoformat(str(evidence["attested_at"]))
    assert decision <= generated <= attested <= cutoff
    reasons = _robustness_reasons(_checker_evidence(root, result.run_directory))
    assert "INDUSTRY_EVIDENCE_GENERATION_CUTOFF_INVALID" not in reasons
    assert "INDUSTRY_EVIDENCE_GENERATOR_CUTOFF_INVALID" not in reasons


def test_manifest_and_summary_register_evidence_content_hashes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """证据制品以内容散列登记进 manifest 与 summary。"""
    root, config_path = _prepare(tmp_path, monkeypatch)
    result = run_research(root, config_path, root / "out")
    records = _artifact_records(_manifest(result.run_directory))
    summary = _summary(root, result.run_directory)
    summary_artifacts = summary.get("artifacts")
    assert isinstance(summary_artifacts, Mapping)
    expected = {
        "industry_evidence",
        "industry_evidence_generator_attestation",
        "industry_evidence_ledger",
        "tail_risk_evidence",
        "stress_scenario_evidence",
        "fixed_target_cost_replay",
        "l2_depth_capacity_evidence",
    }
    assert expected <= set(records)
    assert expected <= set(summary_artifacts)
    for name in sorted(expected):
        record = records[name]
        body = (root / str(record["path"])).read_bytes()
        assert hashlib.sha256(body).hexdigest() == record["sha256"]
        assert len(body) == record["bytes"]


def test_switch_off_skips_generation_and_marks_summary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """开关关闭时不生成证据，summary 如实标注。"""
    root, config_path = _prepare(tmp_path, monkeypatch, switch=False)
    result = run_research(root, config_path, root / "out")
    summary = _summary(root, result.run_directory)
    evidence = summary.get("industry_evidence")
    assert isinstance(evidence, Mapping)
    assert evidence.get("status") == "disabled"
    assert "industry_evidence" not in _artifact_records(
        _manifest(result.run_directory)
    )


def test_command_line_switch_can_skip_generation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """命令行开关同样可以跳过生成。"""
    root, config_path = _prepare(tmp_path, monkeypatch)
    result = run_research(
        root,
        config_path,
        root / "out",
        generate_industry_evidence=False,
    )
    summary = _summary(root, result.run_directory)
    evidence = summary.get("industry_evidence")
    assert isinstance(evidence, Mapping)
    assert evidence.get("status") == "disabled"


def test_generator_failure_fails_the_whole_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """生成器故障必须失败关闭，不留下缺证据的 summary。"""
    root, config_path = _prepare(tmp_path, monkeypatch)

    def _explode(*_args: object, **_kwargs: object) -> None:
        """模拟生成器内部故障。"""
        raise ValueError("深度事实解析失败")

    monkeypatch.setattr(
        "guvolu.research.pipeline.generate_run_evidence", _explode,
    )
    with pytest.raises(ValueError, match="行业稳健性证据生成失败"):
        run_research(root, config_path, root / "out")


def test_missing_paper_candidate_is_absence_not_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """没有 paper 可用候选是有效结果，只标注不失败。"""
    root, config_path = _prepare(tmp_path, monkeypatch, eligible=False)
    result = run_research(root, config_path, root / "out")
    summary = _summary(root, result.run_directory)
    evidence = summary.get("industry_evidence")
    assert isinstance(evidence, Mapping)
    assert evidence.get("status") == "absent"
    assert evidence.get("reason") == "no_paper_eligible_candidate"


def test_insufficient_l2_coverage_keeps_run_alive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """L2 覆盖不足时容量证据不达标，但研究运行不失败。"""
    root, config_path = _prepare(tmp_path, monkeypatch)
    result = run_research(root, config_path, root / "out")
    manifest = _manifest(result.run_directory)
    evidence = manifest.get("industry_evidence")
    assert isinstance(evidence, Mapping)
    coverage = evidence.get("venue_l2_coverage")
    assert isinstance(coverage, list)
    assert all(item.get("sufficient") is not True for item in coverage)
    counts = evidence.get("scenario_counts")
    assert isinstance(counts, Mapping)
    assert all(
        cast(Mapping[str, object], item).get("capacity_scenarios") == 0
        for item in counts.values()
    )
    reasons = _robustness_reasons(_checker_evidence(root, result.run_directory))
    assert "CAPACITY_SCENARIO_NOTIONAL_GRID_INSUFFICIENT" in reasons


def test_sealed_precheck_still_blocks_the_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """封存预检仍在生成证据之前生效。"""
    root, config_path = _prepare(tmp_path, monkeypatch)

    def _sealed(*_args: object, **_kwargs: object) -> None:
        """模拟封存段冲突。"""
        raise ValueError("研究面板与封存段冲突")

    monkeypatch.setattr(
        "guvolu.research.pipeline.reject_sealed_conflict", _sealed,
    )
    with pytest.raises(ValueError, match="封存段冲突"):
        run_research(root, config_path, root / "out")
