"""研究面板显式截止上限：来源解析、封存预检、暴露终点、身份与命令行。"""
from __future__ import annotations

import importlib.util
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType

import duckdb
import pytest

from guvolu.research import clock
from guvolu.research.contracts import FrozenPanelInputs
from guvolu.research.governance import (
    register_research_exposure,
    seal_holdout_vintage,
)
from guvolu.research.panel import compact_trade_panel, load_panel_bars
from guvolu.research.panel_limit import (
    PanelToTimeLimit,
    reject_sealed_conflict,
    resolve_panel_to_time,
    sealed_vintages_overlapping,
)
from guvolu.research.verification import (
    _panel_to_time_override,
    _recorded_panel_to_time,
)

MARKET = "mkt__test__btc__r0"


def _time(hour: int, minute: int = 0) -> datetime:
    """生成固定 UTC 测试时间。"""
    return datetime(2026, 1, 1, hour, minute, tzinfo=UTC)


def _load_script(name: str) -> ModuleType:
    """按路径加载 scripts 下的命令行模块。"""
    path = Path(__file__).resolve().parents[1] / "scripts" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _write_trades(path: Path) -> None:
    """写三个小时桶的成交 Parquet。"""
    db = duckdb.connect()
    try:
        db.execute("""
            CREATE TABLE source(
              observation_id VARCHAR,event_time TIMESTAMPTZ,
              available_time TIMESTAMPTZ,ingest_time TIMESTAMPTZ,
              side VARCHAR,source_side_basis VARCHAR,price VARCHAR,size VARCHAR,
              source_artifact_id VARCHAR,source_row_index BIGINT,
              market_id VARCHAR,venue_id VARCHAR
            )
        """)
        rows = [
            (
                f"t{hour}", _time(hour, 10), _time(hour, 10), _time(hour, 11),
                "buy", "taker", str(100 + hour), "1", "x", hour, MARKET, "bitbank",
            )
            for hour in range(3)
        ]
        db.executemany(
            "INSERT INTO source VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", rows,
        )
        escaped = str(path.resolve()).replace("'", "''")
        db.execute(f"COPY source TO '{escaped}' (FORMAT PARQUET)")
    finally:
        db.close()


def _inputs(source: Path) -> FrozenPanelInputs:
    """构造覆盖到第三小时末的冻结输入。"""
    return FrozenPanelInputs(
        market={
            "market_id": MARKET,
            "venue_id": "bitbank",
            "mapping_revision": 0,
            "tick_size": "1",
            "size_step": "0.1",
        },
        paths=(source,),
        head_generation="sha256-" + "1" * 64,
        attempt_ids=("attempt",),
        artifact_ids=("artifact",),
        normalization_versions=("v1",),
        maximum_event_time=_time(3),
    )


def _seal(registry: Path, monkeypatch: pytest.MonkeyPatch) -> str:
    """在第一小时封存 [02:00, 05:00) 的未来 vintage。"""
    monkeypatch.setattr(clock, "utc_now", lambda: _time(1))
    return seal_holdout_vintage(registry, MARKET, _time(2), _time(5)).vintage_id


def test_resolve_panel_to_time_sources_and_effective_bound() -> None:
    """无上限保持原行为；配置与命令行来源分明；覆盖只能更早。"""
    none = resolve_panel_to_time({}, None, _time(0))
    assert none.limit is None and none.source == "none"
    assert none.effective_to_time(_time(3)) == _time(3)
    assert dict(none.identity_payload()) == {}

    config = resolve_panel_to_time(
        {"panel_to_time": "2026-01-01T02:00:00Z"}, None, _time(0),
    )
    assert config.limit == _time(2) and config.source == "config"
    assert config.effective_to_time(_time(3)) == _time(2)
    assert config.effective_to_time(_time(1, 30)) == _time(1, 30)
    assert dict(config.identity_payload()) == {}

    cli = resolve_panel_to_time(
        {"panel_to_time": "2026-01-01T02:00:00Z"}, _time(1), _time(0),
    )
    assert cli.limit == _time(1) and cli.source == "cli"
    assert cli.config_limit == _time(2) and cli.cli_limit == _time(1)
    assert dict(cli.identity_payload()) == {
        "panel_to_time_override": _time(1).isoformat(),
    }

    with pytest.raises(ValueError, match="--to-time 不得晚于配置"):
        resolve_panel_to_time(
            {"panel_to_time": "2026-01-01T02:00:00Z"}, _time(2, 30), _time(0),
        )
    with pytest.raises(ValueError, match="必须晚于 from_time"):
        resolve_panel_to_time(
            {"panel_to_time": "2026-01-01T00:00:00Z"}, None, _time(0),
        )
    with pytest.raises(ValueError, match="data_governance.panel_to_time"):
        resolve_panel_to_time({"panel_to_time": 5}, None, _time(0))


def test_sealed_precheck_rejects_limit_after_sealed_start(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """上限晚于任一 sealed 起点即提前拒绝；等于起点可通过。"""
    registry = tmp_path / "governance.sqlite3"
    missing = PanelToTimeLimit(_time(4), "config", _time(4), None)
    reject_sealed_conflict(registry, MARKET, _time(0), _time(4), missing)
    vintage_id = _seal(registry, monkeypatch)
    assert sealed_vintages_overlapping(registry, MARKET, _time(0), _time(2)) == ()
    assert sealed_vintages_overlapping(
        registry, MARKET, _time(0), _time(2, 1),
    ) == (vintage_id,)

    aligned = PanelToTimeLimit(_time(2), "config", _time(2), None)
    reject_sealed_conflict(registry, MARKET, _time(0), _time(2), aligned)
    late = PanelToTimeLimit(_time(2, 30), "cli", _time(3), _time(2, 30))
    with pytest.raises(ValueError, match=r"面板截止上限\(cli\).*晚于封存段起点"):
        reject_sealed_conflict(registry, MARKET, _time(0), _time(2, 30), late)
    none = PanelToTimeLimit(None, "none", None, None)
    with pytest.raises(ValueError, match="请配置 data_governance.panel_to_time"):
        reject_sealed_conflict(registry, MARKET, _time(0), _time(3), none)


def test_capped_panel_and_exposure_end_before_sealed_start(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """配置上限生效：末柱不晚于上限，暴露终点等于末柱且登记成功。"""
    source = tmp_path / "source.parquet"
    _write_trades(source)
    inputs = _inputs(source)
    registry = tmp_path / "governance.sqlite3"
    _seal(registry, monkeypatch)
    limit = resolve_panel_to_time(
        {"panel_to_time": "2026-01-01T02:00:00Z"}, None, _time(0),
    )
    to_time = limit.effective_to_time(inputs.maximum_event_time)
    assert to_time == _time(2)
    reject_sealed_conflict(registry, MARKET, _time(0), to_time, limit)
    panel, _digest = compact_trade_panel(
        inputs, tmp_path / "panel", "1hour", _time(0), to_time, 100_000_000,
    )
    bars = load_panel_bars(panel)
    assert len(bars) == 2
    assert bars[-1].decision_time <= to_time
    assert bars[-1].latest_available_time <= to_time
    exposure = register_research_exposure(
        registry, "research-capped", MARKET, _time(0), bars[-1].decision_time,
    )
    assert exposure.end_time == bars[-1].decision_time <= to_time
    record = dict(limit.payload(to_time, bars[-1].decision_time))
    assert record == {
        "source": "config",
        "limit": _time(2).isoformat(),
        "config_limit": _time(2).isoformat(),
        "cli_override": None,
        "effective_to_time": _time(2).isoformat(),
        "last_decision_time": _time(2).isoformat(),
    }

    uncapped = resolve_panel_to_time({}, None, _time(0))
    with pytest.raises(ValueError, match="未消费封存段重叠"):
        reject_sealed_conflict(
            registry, MARKET, _time(0),
            uncapped.effective_to_time(inputs.maximum_event_time), uncapped,
        )
    full_panel, _full_digest = compact_trade_panel(
        inputs, tmp_path / "full", "1hour", _time(0), _time(3), 100_000_000,
    )
    full_bars = load_panel_bars(full_panel)
    assert len(full_bars) == 3
    with pytest.raises(ValueError, match="未消费封存段重叠"):
        register_research_exposure(
            registry, "research-uncapped", MARKET, _time(0),
            full_bars[-1].decision_time,
        )


def test_verifier_rebuilds_to_time_and_identity_override() -> None:
    """复核器按 manifest 记录重建面板 to_time 与身份覆盖。"""
    assert _recorded_panel_to_time({}, _time(3)) == _time(3)
    assert _panel_to_time_override({}) is None
    record = {
        "source": "cli",
        "limit": _time(2).isoformat(),
        "config_limit": _time(4).isoformat(),
        "cli_override": "2026-01-01T02:00:00Z",
        "effective_to_time": _time(2).isoformat(),
        "last_decision_time": _time(2).isoformat(),
    }
    manifest = {"panel_to_time": record}
    assert _recorded_panel_to_time(manifest, _time(3)) == _time(2)
    assert _panel_to_time_override(manifest) == _time(2).isoformat()
    later_input = {"panel_to_time": {**record, "effective_to_time": _time(1).isoformat()}}
    with pytest.raises(ValueError, match="effective_to_time 不能由上限与输入重建"):
        _recorded_panel_to_time(later_input, _time(3))
    config_only = {"panel_to_time": {**record, "source": "config", "cli_override": None}}
    assert _panel_to_time_override(config_only) is None
    assert _recorded_panel_to_time(
        {"panel_to_time": {**record, "limit": None, "effective_to_time": _time(3).isoformat()}},
        _time(3),
    ) == _time(3)
    with pytest.raises(ValueError, match="与来源标记不一致"):
        _panel_to_time_override({"panel_to_time": {**record, "source": "config"}})


@dataclass(frozen=True)
class _FakeResult:
    """命令行测试用的最小运行结果。"""

    run_id: str = "run"
    manifest_path: Path = Path("manifest.json")
    manifest_sha256: str = "0" * 64
    summary_path: Path = Path("summary.json")
    trial_ledger_path: Path = Path("trial.jsonl")
    target_position_path: Path = Path("target.json")
    decision_grade: bool = False
    paper_eligible_families: tuple[str, ...] = ()
    operational_nonzero_families: tuple[str, ...] = ()
    family_scope: tuple[str, ...] = ()


def test_cli_to_time_is_parsed_and_forwarded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """--to-time 解析为 UTC 并传给管线；缺省为 None。"""
    module = _load_script("run_strategy_research")
    captured: list[object] = []

    def fake_run(*_args: object, **kwargs: object) -> _FakeResult:
        captured.append(kwargs.get("panel_to_time"))
        return _FakeResult()

    monkeypatch.setattr(module, "run_research", fake_run)
    assert module.main(["--root", str(tmp_path)]) == 0
    assert module.main([
        "--root", str(tmp_path), "--to-time", "2026-08-23T09:00:00Z",
    ]) == 0
    assert captured == [None, datetime(2026, 8, 23, 9, tzinfo=UTC)]
    with pytest.raises(ValueError):
        module.main(["--root", str(tmp_path), "--to-time", "昨天"])
    assert captured == [None, datetime(2026, 8, 23, 9, tzinfo=UTC)]
