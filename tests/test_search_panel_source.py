"""真实面板接入的 to_time 上限、封存段重叠与注入式加载测试（纯 CPU）。"""
from __future__ import annotations

import sqlite3
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from guvolu.research.contracts import FrozenPanelInputs, PanelSnapshot
from guvolu.search.panel_source import (
    enforce_to_time,
    load_research_panel,
    panel_from_bars,
    resolve_panel_to_time,
    sealed_vintages_overlapping,
)
from guvolu.search.synthetic import synthetic_panel

LOOKBACKS = (4, 8)
RESEARCH_CONFIG = {
    "market_id": "mkt__test__btc",
    "bar_interval": "1hour",
    "from_time": "2024-01-01T00:00:00+00:00",
    "notional_scale": 100000000,
    "features": {
        "lookbacks": [4, 8],
        "state_lookback": 8,
        "volume_lookback": 8,
        "maximum_structural_gap_bars_assumption": 4,
    },
}


def _bars(count: int = 64):
    """生成合成研究柱。"""
    bars, _features = synthetic_panel(count, LOOKBACKS, 3)
    return bars


def _inputs(maximum_event_time: datetime) -> FrozenPanelInputs:
    """构造最小冻结输入。"""
    return FrozenPanelInputs(
        market={"market_id": "mkt__test__btc", "venue_id": "test"},
        paths=(),
        head_generation="head-1",
        attempt_ids=("attempt-1",),
        artifact_ids=("artifact-1",),
        normalization_versions=("v4",),
        maximum_event_time=maximum_event_time,
    )


def _snapshot(inputs: FrozenPanelInputs, bars, path: Path) -> PanelSnapshot:
    """构造面板快照。"""
    return PanelSnapshot(
        market=inputs.market,
        bars=tuple(bars),
        head_generation=inputs.head_generation,
        attempt_ids=inputs.attempt_ids,
        artifact_ids=inputs.artifact_ids,
        normalization_versions=inputs.normalization_versions,
        panel_path=path,
        panel_sha256="a" * 64,
        decision_time=bars[-1].decision_time,
        latest_available_time=bars[-1].latest_available_time,
    )


def test_enforce_to_time_rejects_late_bars() -> None:
    """决策时间或可得时间晚于上限的柱被拒绝。"""
    bars = _bars(16)
    limit = bars[-1].decision_time
    assert len(enforce_to_time(bars, limit)) == 16
    with pytest.raises(ValueError, match="panel_to_time"):
        enforce_to_time(bars, limit - timedelta(hours=1))
    late = bars[:-1] + (replace(
        bars[-1], latest_available_time=limit + timedelta(minutes=1),
    ),)
    with pytest.raises(ValueError, match="可得时间"):
        enforce_to_time(late, limit)


def test_resolve_panel_to_time_requires_earlier_than_sealed_boundary() -> None:
    """面板上限必须早于封存段起点。"""
    boundary = datetime(2026, 8, 24, tzinfo=UTC)
    assert resolve_panel_to_time(
        {"panel_to_time": "2026-08-23T09:00:00Z"}, boundary,
    ) == datetime(2026, 8, 23, 9, tzinfo=UTC)
    with pytest.raises(ValueError):
        resolve_panel_to_time({"panel_to_time": "2026-08-24T00:00:00Z"}, boundary)
    with pytest.raises(ValueError):
        resolve_panel_to_time({})


def test_sealed_vintages_overlapping_reads_registry_read_only(tmp_path: Path) -> None:
    """只读查询 holdout_vintage 的 sealed 行并判断区间重叠。"""
    registry = tmp_path / "governance.sqlite3"
    connection = sqlite3.connect(registry)
    connection.execute(
        "CREATE TABLE holdout_vintage (vintage_id TEXT, market_id TEXT, "
        "start_time TEXT, end_time TEXT, status TEXT)"
    )
    connection.execute(
        "INSERT INTO holdout_vintage VALUES (?,?,?,?,?)",
        ("v-sealed", "m", "2026-08-24T00:00:00+00:00", "2026-12-02T00:00:00+00:00", "sealed"),
    )
    connection.execute(
        "INSERT INTO holdout_vintage VALUES (?,?,?,?,?)",
        ("v-old", "m", "2026-01-01T00:00:00+00:00", "2026-02-01T00:00:00+00:00", "consumed"),
    )
    connection.commit()
    connection.close()
    start = datetime(2019, 1, 1, tzinfo=UTC)
    assert sealed_vintages_overlapping(
        registry, "m", start, datetime(2026, 8, 23, 9, tzinfo=UTC),
    ) == ()
    assert sealed_vintages_overlapping(
        registry, "m", start, datetime(2026, 8, 25, tzinfo=UTC),
    ) == ("v-sealed",)
    assert sealed_vintages_overlapping(
        registry, "other", start, datetime(2026, 8, 25, tzinfo=UTC),
    ) == ()
    assert sealed_vintages_overlapping(tmp_path / "missing.sqlite3", "m", start, start) == ()


def test_load_research_panel_enforces_to_time_and_features(tmp_path: Path) -> None:
    """注入式加载：上限晚于 head 覆盖、面板超界与封存重叠均拒绝。"""
    bars = _bars(64)
    limit = bars[-1].decision_time
    calls: list[tuple[datetime, datetime]] = []

    def freeze(data_root: Path, market_id: str) -> FrozenPanelInputs:
        assert market_id == "mkt__test__btc"
        return _inputs(limit + timedelta(minutes=5))

    def build(inputs, directory, interval, from_time, to_time, scale) -> PanelSnapshot:
        calls.append((from_time, to_time))
        assert interval == "1hour" and scale == 100000000
        return _snapshot(inputs, bars, directory / "panel.parquet")

    panel = load_research_panel(
        tmp_path,
        RESEARCH_CONFIG,
        {"panel_to_time": limit.isoformat()},
        LOOKBACKS,
        tmp_path / "out",
        freeze=freeze,
        build=build,
    )
    assert len(panel.bars) == 64 and len(panel.features) == 64
    assert panel.to_time == limit and calls[0][1] == limit
    assert panel.payload()["inputs"]["head_generation"] == "head-1"
    assert panel.lookbacks == LOOKBACKS

    def build_late(inputs, directory, interval, from_time, to_time, scale) -> PanelSnapshot:
        return _snapshot(inputs, bars, directory / "panel.parquet")

    with pytest.raises(ValueError, match="panel_to_time"):
        load_research_panel(
            tmp_path, RESEARCH_CONFIG,
            {"panel_to_time": (limit - timedelta(hours=2)).isoformat()},
            LOOKBACKS, tmp_path / "out", freeze=freeze, build=build_late,
        )

    def freeze_short(data_root: Path, market_id: str) -> FrozenPanelInputs:
        return _inputs(limit - timedelta(hours=1))

    with pytest.raises(ValueError, match="事件覆盖"):
        load_research_panel(
            tmp_path, RESEARCH_CONFIG, {"panel_to_time": limit.isoformat()},
            LOOKBACKS, tmp_path / "out", freeze=freeze_short, build=build,
        )
    registry = tmp_path / "research" / "governance.sqlite3"
    registry.parent.mkdir(parents=True)
    connection = sqlite3.connect(registry)
    connection.execute(
        "CREATE TABLE holdout_vintage (vintage_id TEXT, market_id TEXT, "
        "start_time TEXT, end_time TEXT, status TEXT)"
    )
    connection.execute(
        "INSERT INTO holdout_vintage VALUES (?,?,?,?,?)",
        ("v-sealed", "mkt__test__btc", "2023-06-01T00:00:00+00:00",
         "2030-01-01T00:00:00+00:00", "sealed"),
    )
    connection.commit()
    connection.close()
    with pytest.raises(ValueError, match="封存段重叠"):
        load_research_panel(
            tmp_path, RESEARCH_CONFIG, {"panel_to_time": limit.isoformat()},
            LOOKBACKS, tmp_path / "out", freeze=freeze, build=build,
        )


def test_panel_from_bars_rejects_empty_or_invalid_lookbacks() -> None:
    """空面板与非法回看窗拒绝。"""
    bars = _bars(8)
    limit = bars[-1].decision_time
    with pytest.raises(ValueError):
        panel_from_bars("m", bars, (1,), 8, 4, bars[0].open_time, limit, "a" * 64, None, {})
    with pytest.raises(ValueError, match="面板为空"):
        panel_from_bars("m", (), LOOKBACKS, 8, 4, limit, limit, "a" * 64, None, {})
