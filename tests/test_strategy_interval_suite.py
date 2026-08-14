"""多节拍研究套件预登记合同测试。"""
from __future__ import annotations

import copy
import json
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path

import pytest

from guvolu.research.interval_suite import build_interval_suite_plan
from guvolu.research.interval_suite_evidence import (
    align_returns_to_interval,
    global_fdr_q_values,
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
    assert first["duration_contract"] == {
        "feature_lookback_seconds": [86_400, 259_200, 604_800],
        "minimum_train_seconds": 31_536_000,
        "test_seconds": 7_776_000,
        "step_seconds": 7_776_000,
        "embargo_seconds": 86_400,
    }
    members = first["members"]
    assert isinstance(members, list)
    assert all(isinstance(member, Mapping) for member in members)
    assert [member["bar_interval"] for member in members] == [
        "1hour", "4hour",
    ]
    assert [member["candidate_count"] for member in members] == [34, 34]
    domain = first["global_multiple_testing_domain"]
    assert isinstance(domain, list)
    assert all(isinstance(trial, Mapping) for trial in domain)
    assert len(domain) == 78
    assert len({trial["trial_id"] for trial in domain}) == 78
    assert sum(trial["role"] == "candidate_oos_path" for trial in domain) == 68
    assert sum(
        trial["role"] == "walk_forward_family_path" for trial in domain
    ) == 10


def test_interval_suite_rejects_duplicate_interval() -> None:
    """重复加入同一节拍不能伪装成更多时间证据。"""
    root, hourly, _four_hour = _configs()
    with pytest.raises(ValueError, match="重复节拍"):
        build_interval_suite_plan(root, (hourly, hourly))


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (("walk_forward", "test_bars", 539), "墙钟回看或 walk-forward"),
        ((None, "market_id", "mkt__other"), "同一 market_id"),
        ((None, "from_time", "2020-01-01T00:00:00+00:00"), "同一 from_time"),
    ],
)
def test_interval_suite_rejects_incomparable_members(
    tmp_path: Path,
    mutation: tuple[str | None, str, object],
    message: str,
) -> None:
    """跨节拍比较必须共享市场和等价的墙钟验证合同。"""
    root, hourly, four_hour = _configs()
    hourly_body = json.loads(hourly.read_text(encoding="utf-8"))
    four_hour_body = json.loads(four_hour.read_text(encoding="utf-8"))
    mutated = copy.deepcopy(four_hour_body)
    section, key, value = mutation
    if section is None:
        mutated[key] = value
    else:
        mutated[section][key] = value
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
