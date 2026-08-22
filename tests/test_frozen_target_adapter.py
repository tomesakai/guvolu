"""冻结目标适配器单测：第 2 版执行目标字段、目标域与有效期。"""
from __future__ import annotations

import json
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from guvolu.domain.symbols import SpotSymbol
from guvolu.execution.dry_run_executor import load_target_artifact
from guvolu.execution.frozen_target_adapter import (
    ADAPTER_SCHEMA_VERSION,
    TARGET_SEMANTICS,
    FrozenTargetError,
    build_operational_target,
    main,
    persist_operational_target,
)

MARKET = "mkt__gmo__btc__r0"
BTC = SpotSymbol("BTC")
BUDGET = Decimal("500")
DECISION = "2026-08-21T17:00:00+00:00"


def _prediction(
    path: Path,
    *,
    eligible: bool = True,
    target: float = 0.25,
    extra: dict[str, object] | None = None,
) -> None:
    payload: dict[str, object] = {
        "aggregate_target": target,
        "decision_time": DECISION,
        "families": [{"family": "trend", "portfolio_target_contribution": target}],
        "input_head_generation": "sha256-head",
        "plan_id": "frozen-forward-plan-one",
        "prediction_id": "frozen-forward-prediction-one",
        "quality": {
            "clock": True,
            "coverage": True,
            "eligible": eligible,
            "freshness": True,
            "integrity": True,
            "lineage": True,
            "pit": True,
            "reasons": [] if eligible else ["stale"],
        },
        "reserve": 0.6,
        "schema_version": 1,
        "scope": "FROZEN_FORWARD",
        "unit": "risk_weighted_directional_target",
    }
    if extra:
        payload.update(extra)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _build(source: Path, **overrides: object) -> dict[str, object]:
    kwargs: dict[str, object] = {
        "market_id": MARKET,
        "symbol": BTC,
        "risk_budget_jpy": BUDGET,
        "mode": "paper",
    }
    kwargs.update(overrides)
    return build_operational_target(source, **kwargs)  # type: ignore[arg-type]


def test_adapter_builds_content_addressed_v2_target(tmp_path: Path) -> None:
    source = tmp_path / "prediction.json"
    _prediction(source)

    first, first_sha = persist_operational_target(
        source, tmp_path / "targets", market_id=MARKET, symbol=BTC,
        risk_budget_jpy=BUDGET, mode="dry-run",
    )
    second, second_sha = persist_operational_target(
        source, tmp_path / "targets", market_id=MARKET, symbol=BTC,
        risk_budget_jpy=BUDGET, mode="dry-run",
    )
    legacy_view = load_target_artifact(first)
    payload = json.loads(first.read_text(encoding="utf-8"))

    assert first == second
    assert first_sha == second_sha
    assert legacy_view.aggregate_target == 0.25
    assert payload["schema_version"] == ADAPTER_SCHEMA_VERSION
    assert payload["exposure_target"] == 0.25
    assert payload["target_semantics"] == TARGET_SEMANTICS
    assert payload["symbol"] == "BTC"
    assert payload["mode"] == "dry-run"
    assert payload["risk_budget_jpy"] == "500"
    assert payload["valid_from"] == DECISION
    assert payload["valid_until_source"] == "derived"
    assert payload["correlation_id_source"] == "adapter"
    assert payload["correlation_id"].startswith("co")


def test_adapter_derives_valid_until_from_bar_interval(tmp_path: Path) -> None:
    source = tmp_path / "prediction.json"
    _prediction(source)

    default = _build(source)
    four_hour = _build(source, bar_interval="4hour")
    decision = datetime.fromisoformat(DECISION)

    assert datetime.fromisoformat(str(default["valid_until"])) == (
        decision + timedelta(hours=1)
    )
    assert default["bar_interval"] == "1hour"
    assert datetime.fromisoformat(str(four_hour["valid_until"])) == (
        decision + timedelta(hours=4)
    )


def test_adapter_inherits_v2_validity_and_correlation(tmp_path: Path) -> None:
    source = tmp_path / "prediction.json"
    later = (datetime.fromisoformat(DECISION) + timedelta(hours=2)).isoformat()
    _prediction(source, extra={
        "schema_version": 2,
        "valid_until": later,
        "correlation_id": "co1234567890abcdef",
        "target_semantics": dict(TARGET_SEMANTICS),
        "exposure_target": 0.25,
        "decision_input_sha256": "a" * 64,
    })

    payload = _build(source)
    lineage = payload["lineage"]

    assert payload["valid_until"] == later
    assert payload["valid_until_source"] == "prediction"
    assert payload["correlation_id"] == "co1234567890abcdef"
    assert payload["correlation_id_source"] == "prediction"
    assert isinstance(lineage, dict)
    assert lineage["decision_input_sha256"] == "a" * 64


@pytest.mark.parametrize("bad", [-0.1, 1.5])
def test_adapter_rejects_negative_or_out_of_range_target(
    tmp_path: Path, bad: float
) -> None:
    source = tmp_path / "prediction.json"
    _prediction(source, target=bad)

    with pytest.raises(FrozenTargetError, match="aggregate_target"):
        _build(source)


def test_adapter_rejects_quality_failure_and_bad_validity(tmp_path: Path) -> None:
    failing = tmp_path / "failing.json"
    _prediction(failing, eligible=False)
    stale = tmp_path / "stale.json"
    _prediction(stale, extra={"valid_until": DECISION})
    naive = tmp_path / "naive.json"
    _prediction(naive, extra={"valid_until": "2026-08-21T18:00:00"})

    with pytest.raises(FrozenTargetError, match="质量未通过"):
        _build(failing)
    with pytest.raises(FrozenTargetError, match="valid_until"):
        _build(stale)
    with pytest.raises(FrozenTargetError, match="valid_until"):
        _build(naive)


def test_adapter_rejects_budget_over_ceiling_and_bad_mode(tmp_path: Path) -> None:
    source = tmp_path / "prediction.json"
    _prediction(source)

    with pytest.raises(FrozenTargetError, match="risk_budget_jpy"):
        _build(source, risk_budget_jpy=Decimal("1001"))
    with pytest.raises(FrozenTargetError, match="risk_budget_jpy"):
        _build(source, risk_budget_jpy=Decimal("0"))
    with pytest.raises(FrozenTargetError, match="mode"):
        _build(source, mode="real")


def test_adapter_rejects_semantics_mismatch(tmp_path: Path) -> None:
    source = tmp_path / "prediction.json"
    _prediction(source, extra={
        "schema_version": 2,
        "target_semantics": {**TARGET_SEMANTICS, "short_allowed": True},
    })

    with pytest.raises(FrozenTargetError, match="target_semantics"):
        _build(source)


def test_adapter_cli_takes_budget_from_config_not_default(tmp_path: Path) -> None:
    source = tmp_path / "prediction.json"
    _prediction(source)
    config = tmp_path / "paper.json"
    config.write_text(json.dumps({
        "schema_version": 1,
        "market_id": MARKET,
        "symbol": "BTC",
        "bar_interval": "1hour",
        "risk_budget_jpy": "300",
        "no_trade_band": "0.01",
        "taker_fee_fallback_bps": "5",
        "taker_fee_cache_seconds": 86400,
        "overlay": {
            "limit": "0.3",
            "maximum_spread_bps": "10",
            "minimum_top5_depth_base": "0.5",
            "maximum_anchor_age_seconds": 300,
        },
        "ledger_directory": "execution/paper",
    }), encoding="utf-8")

    code = main([
        "--prediction", str(source),
        "--output-directory", str(tmp_path / "out"),
        "--config", str(config),
        "--mode", "paper",
    ])
    produced = list((tmp_path / "out").glob("*.json"))

    assert code == 0
    assert len(produced) == 1
    payload = json.loads(produced[0].read_text(encoding="utf-8"))
    assert payload["risk_budget_jpy"] == "300"
    assert payload["mode"] == "paper"
    with pytest.raises(FrozenTargetError, match="缺少 --config"):
        main([
            "--prediction", str(source),
            "--output-directory", str(tmp_path / "out2"),
            "--market-id", MARKET,
            "--symbol", "BTC",
        ])
