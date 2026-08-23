"""废弃 vintage 路径的治理、CLI 与预检测试。"""
from __future__ import annotations

import importlib.util
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType

import pytest

from guvolu.research import clock
from guvolu.research.governance import (
    GOVERNANCE_SCHEMA_VERSION,
    abandon_holdout_vintage,
    get_frozen_forward_plan_for_vintage,
    get_holdout_vintage,
    list_holdout_vintages,
    register_frozen_forward_plan,
    register_frozen_forward_prediction,
    register_research_exposure,
    seal_holdout_vintage,
    start_holdout_evaluation_attempt,
    upgrade_governance_write_ceiling,
)
from test_missing_policy import _write_plan, _write_prediction


def _time(value: str) -> datetime:
    """构造测试 UTC 时间。"""
    return datetime.fromisoformat(value).replace(tzinfo=UTC)


_TEST_NOW = _time("2026-01-01T00:00:00")

# v8 封存段表定义，用于物理降级
_V8_VINTAGE_DDL = """
CREATE TABLE holdout_vintage (
  vintage_id TEXT PRIMARY KEY,
  market_id TEXT NOT NULL,
  start_time TEXT NOT NULL,
  end_time TEXT NOT NULL,
  sealed_at TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('sealed','consumed')),
  consumed_at TEXT,
  candidate_set_hash TEXT,
  evaluation_id TEXT,
  verdict TEXT,
  verdict_recorded_at TEXT,
  CHECK(
    (status='sealed' AND consumed_at IS NULL
     AND candidate_set_hash IS NULL AND evaluation_id IS NULL)
    OR
    (status='consumed' AND consumed_at IS NOT NULL
     AND candidate_set_hash IS NOT NULL AND evaluation_id IS NOT NULL)
  )
)
"""
_V8_COLUMNS = (
    "vintage_id,market_id,start_time,end_time,sealed_at,status,consumed_at,"
    "candidate_set_hash,evaluation_id,verdict,verdict_recorded_at"
)


@pytest.fixture(autouse=True)
def _test_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    """测试只替换治理内部壁钟。"""
    global _TEST_NOW
    _TEST_NOW = _time("2026-01-01T00:00:00")
    monkeypatch.setattr(clock, "utc_now", lambda: _TEST_NOW)


def _set_now(value: datetime) -> None:
    global _TEST_NOW
    _TEST_NOW = value


def _load_script(name: str) -> ModuleType:
    """按路径加载 scripts 下的命令行模块。"""
    path = Path(__file__).resolve().parents[1] / "scripts" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _downgrade_vintage_table_to_v8(registry: Path, *, ceiling: bool) -> None:
    """把封存段表物理重建为 v8 形状，可选固定写入上限。"""
    connection = sqlite3.connect(registry, isolation_level=None)
    try:
        connection.execute("BEGIN")
        connection.execute(
            "CREATE TABLE holdout_vintage_v8_copy AS SELECT "
            + _V8_COLUMNS + " FROM holdout_vintage"
        )
        connection.execute("DROP TABLE holdout_vintage")
        connection.execute(_V8_VINTAGE_DDL)
        connection.execute(
            "INSERT INTO holdout_vintage(" + _V8_COLUMNS + ") SELECT "
            + _V8_COLUMNS + " FROM holdout_vintage_v8_copy"
        )
        connection.execute("DROP TABLE holdout_vintage_v8_copy")
        connection.execute(
            "CREATE UNIQUE INDEX holdout_vintage_range "
            "ON holdout_vintage(market_id,start_time,end_time)"
        )
        connection.execute(
            "UPDATE governance_meta SET value='8' WHERE key='schema_version'"
        )
        connection.execute(
            "DELETE FROM governance_meta WHERE key='schema_write_ceiling'"
        )
        if ceiling:
            connection.execute(
                "INSERT INTO governance_meta(key,value) VALUES(?,?)",
                ("schema_write_ceiling", "8"),
            )
        connection.execute("COMMIT")
    finally:
        connection.close()


def _vintage_table_sql(registry: Path) -> str:
    """读取封存段表的规范化建表语句。"""
    with sqlite3.connect(registry) as connection:
        row = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' "
            "AND name='holdout_vintage'"
        ).fetchone()
    return "".join(str(row[0]).lower().split())


def _register_plan(root: Path, registry: Path, vintage_id: str) -> str:
    """为 vintage 登记一个最小冻结前向计划。"""
    plan_id, path, sha256 = _write_plan(
        root, vintage_id, "1" * 64, "candidate-set-one",
        "2" * 64, "tree-one", missing_policy="burn",
    )
    register_frozen_forward_plan(
        registry, vintage_id, "1" * 64, "candidate-set-one",
        "2" * 64, "tree-one", path, sha256, repository_root=root,
    )
    return plan_id


def test_v8_registry_rebuilds_to_v9_and_keeps_rows(tmp_path: Path) -> None:
    """v8 物理表升级为 v9 后旧行、子表外键与索引不变。"""
    registry = tmp_path / "governance.sqlite3"
    planned = seal_holdout_vintage(
        registry, "market-one",
        _time("2027-03-01T00:00:00"), _time("2027-04-01T00:00:00"),
    )
    plan_id = _register_plan(tmp_path, registry, planned.vintage_id)
    consumed = seal_holdout_vintage(
        registry, "market-one",
        _time("2027-05-01T00:00:00"), _time("2027-06-01T00:00:00"),
    )
    _set_now(_time("2027-06-02T00:00:00"))
    start_holdout_evaluation_attempt(
        registry, consumed.vintage_id, "candidate-set-hash", "evaluation-one",
    )
    before = list_holdout_vintages(registry)
    assert {item.status for item in before} == {"sealed", "consumed"}

    _downgrade_vintage_table_to_v8(registry, ceiling=False)
    assert "'abandoned'" not in _vintage_table_sql(registry)
    assert list_holdout_vintages(registry) == before
    assert "'abandoned'" in _vintage_table_sql(registry)
    with sqlite3.connect(registry) as connection:
        assert connection.execute(
            "SELECT value FROM governance_meta WHERE key='schema_version'"
        ).fetchone() == (str(GOVERNANCE_SCHEMA_VERSION),)
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
        columns = {
            str(row[1]) for row in connection.execute(
                "PRAGMA table_info(holdout_vintage)"
            ).fetchall()
        }
        assert {"abandoned_at", "abandon_reason"}.issubset(columns)
        assert connection.execute(
            "SELECT abandoned_at,abandon_reason FROM holdout_vintage"
        ).fetchall() == [(None, None), (None, None)]
        indexes = {
            str(row[1]) for row in connection.execute(
                "PRAGMA index_list(holdout_vintage)"
            ).fetchall()
        }
        assert "holdout_vintage_range" in indexes
        assert connection.execute(
            "SELECT name FROM sqlite_master WHERE name LIKE '%legacy_copy%'"
        ).fetchall() == []
    plan = get_frozen_forward_plan_for_vintage(registry, planned.vintage_id)
    assert plan is not None and plan.plan_id == plan_id

    _downgrade_vintage_table_to_v8(registry, ceiling=True)
    assert list_holdout_vintages(registry) == before
    assert "'abandoned'" not in _vintage_table_sql(registry)
    with pytest.raises(ValueError, match="写入已冻结在版本 8"):
        abandon_holdout_vintage(registry, planned.vintage_id, "运行根失效")
    backup = tmp_path / "governance-v8.sqlite3.bak"
    upgrade_governance_write_ceiling(
        registry, backup, expected_version=8, expected_write_ceiling=8,
    )
    assert "'abandoned'" in _vintage_table_sql(registry)
    assert "'abandoned'" not in "".join(str(sqlite3.connect(backup).execute(
        "SELECT sql FROM sqlite_master WHERE name='holdout_vintage'"
    ).fetchone()[0]).lower().split())
    _set_now(_time("2027-02-01T00:00:00"))
    abandoned = abandon_holdout_vintage(
        registry, planned.vintage_id, "冻结前向运行根失效",
    )
    assert abandoned.status == "abandoned"
    assert abandoned.abandoned_at == _time("2027-02-01T00:00:00")
    assert abandoned.abandon_reason == "冻结前向运行根失效"
    assert get_holdout_vintage(registry, consumed.vintage_id) == before[1]


def test_abandon_preconditions_and_idempotence(tmp_path: Path) -> None:
    """只有 sealed 且未评估的 vintage 可废弃，理由必填且幂等。"""
    registry = tmp_path / "governance.sqlite3"
    vintage = seal_holdout_vintage(
        registry, "market-one",
        _time("2026-02-01T00:00:00"), _time("2026-03-01T00:00:00"),
    )
    with pytest.raises(ValueError, match="写明理由"):
        abandon_holdout_vintage(registry, vintage.vintage_id, "   ")
    with pytest.raises(LookupError, match="封存段不存在"):
        abandon_holdout_vintage(registry, "missing-vintage", "理由")
    assert get_holdout_vintage(registry, vintage.vintage_id).status == "sealed"

    _set_now(_time("2026-01-15T00:00:00"))
    first = abandon_holdout_vintage(
        registry, vintage.vintage_id, " 冻结前向运行根失效 ",
    )
    assert first.status == "abandoned"
    assert first.abandoned_at == _time("2026-01-15T00:00:00")
    assert first.abandon_reason == "冻结前向运行根失效"
    assert first.consumed_at is None and first.evaluation_id is None
    _set_now(_time("2026-01-16T00:00:00"))
    assert abandon_holdout_vintage(
        registry, vintage.vintage_id, "冻结前向运行根失效",
    ) == first
    with pytest.raises(ValueError, match="不同理由"):
        abandon_holdout_vintage(registry, vintage.vintage_id, "其他理由")
    assert get_holdout_vintage(registry, vintage.vintage_id) == first

    consumed = seal_holdout_vintage(
        registry, "market-one",
        _time("2026-04-01T00:00:00"), _time("2026-05-01T00:00:00"),
    )
    _set_now(_time("2026-05-02T00:00:00"))
    start_holdout_evaluation_attempt(
        registry, consumed.vintage_id, "candidate-set-hash", "evaluation-one",
    )
    with pytest.raises(ValueError, match="只有 sealed vintage 可以废弃"):
        abandon_holdout_vintage(registry, consumed.vintage_id, "理由")
    assert get_holdout_vintage(registry, consumed.vintage_id).status == "consumed"


def test_abandoned_vintage_frees_exposure_and_bounds_new_seal(
    tmp_path: Path,
) -> None:
    """废弃后研究可读其余段，新段起点须晚于废弃时刻且不与暴露重叠。"""
    registry = tmp_path / "governance.sqlite3"
    dead = seal_holdout_vintage(
        registry, "market-one",
        _time("2026-02-01T00:00:00"), _time("2026-06-01T00:00:00"),
    )
    _set_now(_time("2026-03-01T05:00:00"))
    abandon_holdout_vintage(registry, dead.vintage_id, "预测永久中断")

    exposure = register_research_exposure(
        registry, "research-after-abandon", "market-one",
        _time("2026-02-01T00:00:00"), _time("2026-02-15T00:00:00"),
    )
    assert exposure.market_id == "market-one"

    _set_now(_time("2026-02-05T00:00:00"))
    with pytest.raises(ValueError, match="已被自适应研究读取"):
        seal_holdout_vintage(
            registry, "market-one",
            _time("2026-02-10T00:00:00"), _time("2026-02-12T00:00:00"),
        )
    _set_now(_time("2026-02-20T00:00:00"))
    with pytest.raises(ValueError, match="早于重叠废弃 vintage 的废弃时刻"):
        seal_holdout_vintage(
            registry, "market-one",
            _time("2026-02-20T00:00:00"), _time("2026-04-01T00:00:00"),
        )
    _set_now(_time("2026-03-01T05:00:00"))
    fresh = seal_holdout_vintage(
        registry, "market-one",
        _time("2026-03-01T05:00:00"), _time("2026-04-01T00:00:00"),
    )
    assert fresh.status == "sealed"
    with pytest.raises(ValueError, match="未消费封存段重叠"):
        register_research_exposure(
            registry, "research-into-fresh", "market-one",
            _time("2026-03-10T00:00:00"), _time("2026-03-12T00:00:00"),
        )
    with pytest.raises(ValueError, match="既有 vintage 重叠"):
        seal_holdout_vintage(
            registry, "market-one",
            _time("2026-03-15T00:00:00"), _time("2026-05-01T00:00:00"),
        )
    statuses = {
        item.vintage_id: item.status for item in list_holdout_vintages(registry)
    }
    assert statuses == {dead.vintage_id: "abandoned", fresh.vintage_id: "sealed"}

    early = seal_holdout_vintage(
        registry, "market-two",
        _time("2026-07-01T00:00:00"), _time("2026-08-01T00:00:00"),
    )
    abandon_holdout_vintage(registry, early.vintage_id, "计划作废")
    with pytest.raises(ValueError, match="不得重新封存"):
        seal_holdout_vintage(
            registry, "market-two",
            _time("2026-07-01T00:00:00"), _time("2026-08-01T00:00:00"),
        )
    shifted = seal_holdout_vintage(
        registry, "market-two",
        _time("2026-07-01T00:00:00"), _time("2026-08-02T00:00:00"),
    )
    assert shifted.status == "sealed" and shifted.vintage_id != early.vintage_id

    consumed = seal_holdout_vintage(
        registry, "market-three",
        _time("2026-04-01T00:00:00"), _time("2026-05-01T00:00:00"),
    )
    _set_now(_time("2026-05-02T00:00:00"))
    start_holdout_evaluation_attempt(
        registry, consumed.vintage_id, "candidate-set-hash", "evaluation-one",
    )
    _set_now(_time("2026-04-10T00:00:00"))
    with pytest.raises(ValueError, match="既有 vintage 重叠"):
        seal_holdout_vintage(
            registry, "market-three",
            _time("2026-04-15T00:00:00"), _time("2026-06-01T00:00:00"),
        )


def test_abandoned_vintage_rejects_plan_prediction_and_evaluation(
    tmp_path: Path,
) -> None:
    """废弃后计划保留但不再接受预测、评估或新计划。"""
    registry = tmp_path / "governance.sqlite3"
    vintage = seal_holdout_vintage(
        registry, "market-one",
        _time("2026-02-01T00:00:00"), _time("2026-03-01T00:00:00"),
    )
    plan_id = _register_plan(tmp_path, registry, vintage.vintage_id)
    _set_now(_time("2026-02-10T00:00:00"))
    abandon_holdout_vintage(registry, vintage.vintage_id, "预测永久中断")

    decision = _time("2026-02-11T00:00:00")
    path, sha256 = _write_prediction(
        tmp_path, plan_id, vintage.vintage_id, decision, "2" * 64, "tree-one",
    )
    _set_now(decision + timedelta(minutes=1))
    with pytest.raises(ValueError, match="已废弃"):
        register_frozen_forward_prediction(
            registry, plan_id, decision, "head-one",
            "panel-" + decision.strftime("%Y%m%dT%H%M%SZ"),
            path, sha256, 3900, repository_root=tmp_path,
        )
    plan = get_frozen_forward_plan_for_vintage(registry, vintage.vintage_id)
    assert plan is not None and plan.plan_id == plan_id

    _set_now(_time("2026-03-02T00:00:00"))
    with pytest.raises(ValueError, match="已废弃"):
        start_holdout_evaluation_attempt(
            registry, vintage.vintage_id, "candidate-set-hash", "evaluation-one",
        )
    with pytest.raises(ValueError, match="已废弃"):
        _register_plan(tmp_path, registry, vintage.vintage_id)
    assert get_holdout_vintage(registry, vintage.vintage_id).status == "abandoned"

