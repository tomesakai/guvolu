from __future__ import annotations

import json
from pathlib import Path

import pytest

from guvolu.execution.dry_run_executor import load_target_artifact
from guvolu.execution.frozen_target_adapter import (
    FrozenTargetError,
    persist_operational_target,
)


def _prediction(path: Path, *, eligible: bool = True) -> None:
    path.write_text(json.dumps({
        "aggregate_target": 0.25,
        "decision_time": "2026-08-21T17:00:00+00:00",
        "families": [{"family": "trend", "portfolio_target_contribution": 0.25}],
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
    }), encoding="utf-8")


def test_adapter_builds_dry_run_consumable_content_addressed_target(
    tmp_path: Path,
) -> None:
    source = tmp_path / "prediction.json"
    _prediction(source)

    first, first_sha = persist_operational_target(
        source, tmp_path / "targets", market_id="mkt__gmo__btc__r0",
    )
    second, second_sha = persist_operational_target(
        source, tmp_path / "targets", market_id="mkt__gmo__btc__r0",
    )
    target = load_target_artifact(first)

    assert first == second
    assert first_sha == second_sha
    assert target.run_id == "frozen-forward-prediction-one"
    assert target.market_id == "mkt__gmo__btc__r0"
    assert target.aggregate_target == 0.25


def test_adapter_rejects_prediction_that_fails_quality(tmp_path: Path) -> None:
    source = tmp_path / "prediction.json"
    _prediction(source, eligible=False)

    with pytest.raises(FrozenTargetError, match="质量未通过"):
        persist_operational_target(
            source, tmp_path / "targets", market_id="mkt__gmo__btc__r0",
        )
