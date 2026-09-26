"""分档放量表：推导规则、草案字段与门槛校验。"""
from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
SCRIPTS = REPO / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import prepare_stage as stage_module  # noqa: E402

from guvolu.domain.config import MAX_DAY_JPY_CEILING, MAX_ORDER_JPY_CEILING  # noqa: E402
from guvolu.execution.authorization_envelope import load_envelope  # noqa: E402
from guvolu.domain.symbols import SpotSymbol  # noqa: E402

TABLE = stage_module.load_stage_table(REPO / "config" / "stage_scaling.json")


def test_committed_table_derives_documented_limits() -> None:
    """仓库放量表推导出 09-26 快照所列的三档取值。"""
    t1 = stage_module.derive_limits(TABLE, TABLE.stage("T1"))
    assert t1 == {
        "order_jpy_max": Decimal("50000"),
        "day_jpy_max": Decimal("200000"),
        "max_position_jpy": Decimal("75000"),
        "max_cumulative_loss_jpy": Decimal("27500"),
        "day_loss_jpy_max": Decimal("7500"),
        "envelope_jpy_total": Decimal("1500000"),
        "canary_first_order_jpy_max": Decimal("30000"),
    }
    s1 = stage_module.derive_limits(TABLE, TABLE.stage("S1"))
    assert s1["max_cumulative_loss_jpy"] == Decimal("8250")
    assert s1["max_position_jpy"] == Decimal("22500")
    # 现行硬顶容得下 S1 与 T1，S2 需先升硬顶
    for name in ("S1", "T1"):
        limits = stage_module.derive_limits(TABLE, TABLE.stage(name))
        assert limits["order_jpy_max"] <= MAX_ORDER_JPY_CEILING
        assert limits["day_jpy_max"] <= MAX_DAY_JPY_CEILING
    s2 = stage_module.derive_limits(TABLE, TABLE.stage("S2"))
    assert s2["order_jpy_max"] > MAX_ORDER_JPY_CEILING


def test_canary_covers_largest_frozen_first_order() -> None:
    """首单压额覆盖冻结计划最大合计目标 0.6，换封后首单不会被拒。"""
    for item in TABLE.stages:
        limits = stage_module.derive_limits(TABLE, item)
        assert limits["canary_first_order_jpy_max"] >= item.risk_budget_jpy * Decimal("0.6")


def test_draft_changes_only_amounts_count_and_validity(tmp_path: Path) -> None:
    """草案以现行信封为模板，只改金额、日笔数与有效期，且可被装载。"""
    current = json.loads((REPO / "config/authorization_envelope.json").read_text(encoding="utf-8"))
    start = datetime(2026, 9, 26, 11, tzinfo=UTC)
    draft = stage_module.build_draft(current, TABLE, TABLE.stage("S1"), valid_from=start)
    changed = {key for key in draft if draft[key] != current.get(key)}
    allowed = set(stage_module.RULE_FIELDS) | {
        "issued_at", "valid_from", "valid_until", "day_count_max",
    }
    assert changed <= allowed
    assert draft["valid_until"] == "2026-10-24T11:00:00Z"
    assert draft["market_risk"] == current["market_risk"]
    path = tmp_path / "draft.json"
    path.write_text(json.dumps(draft, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    envelope = load_envelope(path, whitelist=frozenset({SpotSymbol("BTC"), SpotSymbol("ETH")}))
    assert envelope.max_cumulative_loss_jpy == Decimal("8250")


def test_table_rejects_order_multiple_other_than_one(tmp_path: Path) -> None:
    """单笔倍数必须为 1：预算超过单笔上限时目标适配器会拒绝整轮。"""
    raw = json.loads((REPO / "config/stage_scaling.json").read_text(encoding="utf-8"))
    raw["rules"]["order_jpy_max"] = "0.8"
    path = tmp_path / "table.json"
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(ValueError, match="单笔上限倍数"):
        stage_module.load_stage_table(path)
