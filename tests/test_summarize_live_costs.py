"""live 成交成本汇总脚本：委托收集、成本折算与归因。"""
from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from guvolu.domain.models import Execution

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import summarize_live_costs as costs  # noqa: E402


def _execution(
    execution_id: int, order_id: int, side: str, size: str, price: str, fee: str,
) -> Execution:
    return Execution.from_api({
        "executionId": execution_id,
        "orderId": order_id,
        "positionId": 0,
        "symbol": "BTC",
        "side": side,
        "settleType": "OPEN",
        "size": size,
        "price": price,
        "lossGain": "0",
        "fee": fee,
        "timestamp": "2026-09-03T18:20:31.000Z",
    })


def _write_live_fixture(root: Path) -> None:
    live = root / "data/execution/live"
    (live / "reports").mkdir(parents=True)
    ledger = live / "intent_ledger.jsonl"
    rows = [
        {
            "record": "intent", "intent_id": "it1", "symbol": "BTC",
            "side": "SELL", "size": "0.00002", "price": "12609280",
            "prediction_id": "pred-1", "envelope_sha256": "e" * 64,
        },
        {"record": "transition", "intent_id": "it1", "order_id": None},
        {"record": "transition", "intent_id": "it1", "order_id": 8894858272},
    ]
    ledger.write_text(
        "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8",
    )
    report = {
        "kind": "live_execution_report",
        "reference_price": "12614000",
        "intent": {"intent_id": "it1", "order_id": 8894858272},
    }
    (live / "reports" / "pred-1.json").write_text(
        json.dumps(report), encoding="utf-8",
    )
    # 顶层 canary 报告
    canary = {
        "kind": "live_execution_report",
        "reference_price": "12590000",
        "intent": {"intent_id": "it0", "order_id": 8894420923},
    }
    (live / "live-report-sha256-abc.json").write_text(
        json.dumps(canary), encoding="utf-8",
    )
    (live / "live-report-sha256-refusal.json").write_text(
        json.dumps({"kind": "live_refusal_report"}), encoding="utf-8",
    )
    canary_dir = root / "data/execution/canary"
    canary_dir.mkdir(parents=True)
    (canary_dir / "intent_ledger.jsonl").write_text(
        json.dumps({
            "record": "intent", "intent_id": "itc", "symbol": "BTC",
            "side": "BUY", "size": "0.00002", "price": "12591999",
        }) + "\n",
        encoding="utf-8",
    )
    (canary_dir / "canary-report-sha256-x.json").write_text(
        json.dumps({
            "kind": "live_canary_report", "intent_id": "itc",
            "order_id": 7000000001, "reference_price": None,
        }),
        encoding="utf-8",
    )


def test_collect_orders_links_ledger_transitions_and_report_reference(
    tmp_path: Path,
) -> None:
    """账本给出委托号与意图上下文，报告补参考价；无账本的报告也收集。"""
    _write_live_fixture(tmp_path)
    orders = costs.collect_orders(tmp_path)
    assert set(orders) == {8894858272, 8894420923, 7000000001}
    canary_order = orders[7000000001]
    assert canary_order.side == "BUY"
    assert canary_order.limit_price == "12591999"
    assert canary_order.reference_price is None
    sell = orders[8894858272]
    assert sell.intent_id == "it1"
    assert sell.side == "SELL"
    assert sell.reference_price == "12614000"
    assert sell.prediction_id == "pred-1"
    canary = orders[8894420923]
    assert canary.reference_price == "12590000"
    assert canary.side is None


def test_fill_cost_attributes_maker_and_taker_and_signs_slippage() -> None:
    """费率按名义额折算，负费为 maker；滑点买高卖低记正。"""
    context = costs.OrderContext(
        order_id=1, intent_id="it", symbol="BTC", side="SELL", size="0.001",
        limit_price="10000000", reference_price="10000000",
        prediction_id=None, envelope_sha256=None,
    )
    maker = costs.fill_cost(
        _execution(11, 1, "SELL", "0.001", "10010000", "-1"), context,
    )
    assert maker.liquidity == "maker"
    assert Decimal(maker.notional_jpy) == Decimal("10010")
    assert Decimal(maker.fee_bps) == Decimal("-0.999")
    # 卖得高于参考价即有利，滑点为负
    assert Decimal(maker.slippage_bps) == Decimal("-10")
    taker = costs.fill_cost(
        _execution(12, 2, "BUY", "0.001", "10010000", "5"),
        costs.OrderContext(
            order_id=2, intent_id=None, symbol=None, side=None, size=None,
            limit_price=None, reference_price="10000000",
            prediction_id=None, envelope_sha256=None,
        ),
    )
    assert taker.liquidity == "taker"
    assert Decimal(taker.slippage_bps) == Decimal("10")
    unknown = costs.fill_cost(_execution(13, 3, "BUY", "0.001", "1", "0"), None)
    assert unknown.liquidity == "unknown"
    assert unknown.slippage_bps is None


def test_summary_weights_by_notional_and_reports_unfilled_orders() -> None:
    """汇总按名义额加权，未成交委托列出，成交明细按时间排序。"""
    orders = {
        1: costs.OrderContext(
            1, "a", "BTC", "BUY", "0.001", "1", "10000000", None, None,
        ),
        2: costs.OrderContext(
            2, "b", "BTC", "SELL", "0.003", "1", "10000000", None, None,
        ),
        3: costs.OrderContext(3, "c", "BTC", "BUY", "0", "1", None, None, None),
    }
    fills = [
        costs.fill_cost(_execution(2, 2, "SELL", "0.003", "10000000", "-3"), orders[2]),
        costs.fill_cost(_execution(1, 1, "BUY", "0.001", "10010000", "5"), orders[1]),
    ]
    summary = costs.summarize(fills, orders)
    overall = summary["overall"]
    assert overall["fill_count"] == 2
    assert overall["order_count"] == 2
    assert Decimal(overall["notional_jpy"]) == Decimal("40010")
    assert Decimal(overall["maker_notional_share"]) == Decimal("0.7498")
    # 加权费率 2/40010
    assert Decimal(overall["fee_bps_weighted"]) == Decimal("0.4999")
    assert Decimal(summary["by_side"]["BUY"]["slippage_bps_weighted"]) == Decimal("10")
    assert Decimal(summary["by_side"]["SELL"]["slippage_bps_weighted"]) == Decimal("0")
    assert summary["orders_without_fill"] == [3]
    # 同时刻按成交号升序
    assert [row["execution_id"] for row in summary["fills"]] == [1, 2]
    assert datetime.fromisoformat(summary["generated_at"]).tzinfo is UTC
