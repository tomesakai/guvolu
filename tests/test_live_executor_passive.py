"""live 被动优先执行：Post-only 先挂，作废或届满后以剩余数量吃单。"""
from __future__ import annotations

from decimal import Decimal
from pathlib import Path

from test_live_executor import (
    NOW,
    PRICE,
    RULE,
    _Reader,
    _Sender,
    _artifact,
    _book,
    _runtime,
)

from guvolu.domain.enums import OrderStatus, TimeInForce
from guvolu.execution.dry_run_executor import build_plan
from guvolu.execution.live_executor import run_live_cycle


def _cycle(tmp_path: Path, reader: _Reader, sender: _Sender):  # type: ignore[no-untyped-def]
    runtime = _runtime(tmp_path, reader=reader, sender=sender)
    runtime.passive_wait_seconds = 150
    plan = build_plan(
        _artifact(), rule=RULE, reference_price=PRICE, budget_jpy=Decimal("500"),
    )
    return runtime, run_live_cycle(
        runtime, plan, assets=reader.assets(), price_observed_at=NOW,
        book=_book(), cancel_all=lambda: 0, now=NOW,
    )


def test_passive_fill_ends_cycle(tmp_path: Path) -> None:
    """被动单成交即结束，只发一笔 SOK 意图。"""
    reader = _Reader([OrderStatus.EXECUTED])
    sender = _Sender()
    _runtime_obj, (_code, fragment) = _cycle(tmp_path, reader, sender)
    assert len(sender.sent) == 1
    assert sender.sent[0].time_in_force is TimeInForce.SOK
    assert sender.sent[0].price == Decimal("11999999")
    assert fragment["passive"]["final_order_status"] == "EXECUTED"
    assert fragment["final_order_status"] == "EXECUTED"


def test_passive_expired_falls_back_to_marketable(tmp_path: Path) -> None:
    """Post-only 因会立即成交而作废，随即以对手价发第二笔。"""
    reader = _Reader([OrderStatus.EXPIRED, OrderStatus.EXECUTED])
    sender = _Sender()
    _runtime_obj, (_code, fragment) = _cycle(tmp_path, reader, sender)
    assert len(sender.sent) == 2
    assert sender.sent[0].time_in_force is TimeInForce.SOK
    assert sender.sent[1].time_in_force is None
    assert sender.sent[1].price == Decimal("12000001")
    assert sender.sent[1].size == sender.sent[0].size
    assert fragment["passive"]["final_order_status"] == "EXPIRED"
    assert fragment["final_order_status"] == "EXECUTED"


def test_passive_unfilled_is_cancelled_then_taker(tmp_path: Path) -> None:
    """届满未成交先撤后下：撤单确认后以剩余数量吃单。"""
    reader = _Reader([
        OrderStatus.ORDERED, OrderStatus.CANCELED, OrderStatus.EXECUTED,
    ])
    sender = _Sender()
    _runtime_obj, (_code, fragment) = _cycle(tmp_path, reader, sender)
    assert sender.events == ["send:LIMIT", "cancel", "send:LIMIT"]
    assert fragment["passive"]["final_order_status"] == "CANCELED"
    assert fragment["passive"]["cancel_type"] is None
    assert fragment["final_order_status"] == "EXECUTED"
    assert fragment["intent"]["intent_id"] != fragment["passive"]["intent_id"]


def test_zero_passive_wait_sends_marketable_directly(tmp_path: Path) -> None:
    """未配置被动阶段时行为不变：单笔对手价委托。"""
    reader = _Reader([OrderStatus.EXECUTED])
    sender = _Sender()
    runtime = _runtime(tmp_path, reader=reader, sender=sender)
    plan = build_plan(
        _artifact(), rule=RULE, reference_price=PRICE, budget_jpy=Decimal("500"),
    )
    _code, fragment = run_live_cycle(
        runtime, plan, assets=reader.assets(), price_observed_at=NOW,
        book=_book(), cancel_all=lambda: 0, now=NOW,
    )
    assert len(sender.sent) == 1 and sender.sent[0].time_in_force is None
    assert fragment["passive"] is None
