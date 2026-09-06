"""汇总 live 实测成交成本：费率、滑点与 maker/taker 归因。

只读扫描执行仓的 live 意图账本与 live 报告收集委托号，再以
READ_ONLY 成交明细（GET /v1/executions）逐委托取成交，按名义额
加权得出费率、相对参考价滑点与 maker 占比。本脚本不持 TRADE
密钥（T-02），不写账本；产出 JSON 报告到
`data/execution/live/cost-summary/`，用于以实测替代成本模型中的
固定假设（策略研究文档第 3 节）。

归因口径：GMO 成交明细 `fee` 为正即支付 taker 手续费，为负即
maker 返还；零值记为 unknown（maker 返还逐笔向下取整后也可能为零）。
滑点以报告 `reference_price` 为基准，买入高于参考、卖出低于参考记正
（不利）。

取整口径（2026-09-06 依 GMO 支持页与余额轨迹确认）：taker 费按约定逐笔
向上取整到整数日元，maker 返还逐笔向下取整，被取整的小数部分按品种逐日
合算，每个营业日 06:00 JST 返还或支付整数部分。因此逐笔 `fee` 高于名义，
有效费率须扣除逐日返还：以 06:00 JST 为日界按品种合算残差，取整数部分为
返还估计。
"""
from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from guvolu.api.read_client import ReadClient
from guvolu.domain.config import load_config
from guvolu.domain.models import Execution

# GMO 现物名义费率两档（bp）
NOMINAL_TAKER_BPS = {"BTC": Decimal("5"), "ETH": Decimal("5"), "XRP": Decimal("5"), "DAI": Decimal("5")}
NOMINAL_MAKER_BPS = {"BTC": Decimal("-1"), "ETH": Decimal("-1"), "XRP": Decimal("-1"), "DAI": Decimal("-1")}
DEFAULT_TAKER_BPS = Decimal("9")
DEFAULT_MAKER_BPS = Decimal("-3")
# 返还日界：每个营业日 06:00 JST
REFUND_BOUNDARY = timedelta(hours=9) - timedelta(hours=6)
LIVE_DIRECTORY = Path("data/execution/live")
CANARY_DIRECTORY = Path("data/execution/canary")
SUMMARY_DIRECTORY = LIVE_DIRECTORY / "cost-summary"
REPORT_KINDS = frozenset({"live_execution_report", "live_canary_report"})
BPS = Decimal("10000")


@dataclass(frozen=True)
class OrderContext:
    """一笔已到交易所的委托及其决策上下文。"""

    order_id: int
    intent_id: str | None
    symbol: str | None
    side: str | None
    size: str | None
    limit_price: str | None
    reference_price: str | None
    prediction_id: str | None
    envelope_sha256: str | None


@dataclass(frozen=True)
class FillCost:
    """单笔成交的成本分解。"""

    order_id: int
    execution_id: int
    symbol: str
    side: str
    size: str
    price: str
    notional_jpy: str
    fee_jpy: str
    fee_bps: str
    liquidity: str
    nominal_fee_jpy: str
    rounding_residual_jpy: str
    refund_day: str
    reference_price: str | None
    slippage_bps: str | None
    timestamp: str


def _load_lines(path: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            payload = json.loads(line)
            if isinstance(payload, dict):
                rows.append(payload)
    return rows


def _load_reports(root: Path) -> list[dict[str, object]]:
    live = root / LIVE_DIRECTORY
    candidates = list(live.glob("live-report-sha256-*.json"))
    candidates.extend((live / "reports").glob("*.json"))
    candidates.extend((root / CANARY_DIRECTORY).glob("canary-report-sha256-*.json"))
    reports: list[dict[str, object]] = []
    for path in sorted(candidates):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if isinstance(payload, dict) and payload.get("kind") in REPORT_KINDS:
            reports.append(payload)
    return reports


def _ledgers(root: Path) -> list[Path]:
    paths: list[Path] = []
    for directory in (LIVE_DIRECTORY, CANARY_DIRECTORY):
        paths.extend((root / directory).rglob("intent_ledger*.jsonl"))
    return sorted(paths)


def _report_intent(report: Mapping[str, object]) -> tuple[str | None, int | None]:
    """报告里的意图号与委托号；canary 报告平铺在顶层。"""
    intent = report.get("intent")
    if isinstance(intent, Mapping):
        order = intent.get("order_id")
        return _text(intent.get("intent_id")), (
            None if order is None else int(str(order))
        )
    order = report.get("order_id")
    return _text(report.get("intent_id")), (
        None if order is None else int(str(order))
    )


def _text(value: object) -> str | None:
    return None if value is None else str(value)


def collect_orders(root: Path) -> dict[int, OrderContext]:
    """从账本与报告收集委托号及上下文；报告的参考价按意图关联。"""
    intents: dict[str, dict[str, object]] = {}
    order_by_intent: dict[str, int] = {}
    for ledger in _ledgers(root):
        for row in _load_lines(ledger):
            kind = row.get("record")
            intent_id = _text(row.get("intent_id"))
            if intent_id is None:
                continue
            if kind == "intent":
                intents[intent_id] = row
            elif kind == "transition" and row.get("order_id") is not None:
                order_by_intent[intent_id] = int(str(row["order_id"]))
    reference_by_intent: dict[str, str] = {}
    reference_by_order: dict[int, str] = {}
    for report in _load_reports(root):
        intent_id, order_id = _report_intent(report)
        reference = _text(report.get("reference_price"))
        if intent_id is not None and order_id is not None:
            order_by_intent.setdefault(intent_id, order_id)
        if reference is None:
            continue
        if intent_id is not None:
            reference_by_intent[intent_id] = reference
        if order_id is not None:
            reference_by_order[order_id] = reference
    orders: dict[int, OrderContext] = {}
    for intent_id, order_id in order_by_intent.items():
        row = intents.get(intent_id, {})
        orders[order_id] = OrderContext(
            order_id=order_id,
            intent_id=intent_id,
            symbol=_text(row.get("symbol")),
            side=_text(row.get("side")),
            size=_text(row.get("size")),
            limit_price=_text(row.get("price")),
            reference_price=reference_by_intent.get(
                intent_id, reference_by_order.get(order_id)
            ),
            prediction_id=_text(row.get("prediction_id")),
            envelope_sha256=_text(row.get("envelope_sha256")),
        )
    for order_id, reference in reference_by_order.items():
        orders.setdefault(order_id, OrderContext(
            order_id=order_id, intent_id=None, symbol=None, side=None,
            size=None, limit_price=None, reference_price=reference,
            prediction_id=None, envelope_sha256=None,
        ))
    return orders


def _liquidity(fee: Decimal) -> str:
    if fee < 0:
        return "maker"
    if fee > 0:
        return "taker"
    return "unknown"


def _nominal_fee(symbol: str, notional: Decimal, liquidity: str) -> Decimal:
    """按名义费率的应收费用；未知流动性按 taker 保守计。"""
    if liquidity == "maker":
        rate = NOMINAL_MAKER_BPS.get(symbol, DEFAULT_MAKER_BPS)
    else:
        rate = NOMINAL_TAKER_BPS.get(symbol, DEFAULT_TAKER_BPS)
    return notional * rate / BPS


def refund_day(timestamp: datetime) -> str:
    """返还日：成交所属 06:00 JST 窗口结束的那个早晨（JST 日期）。"""
    window_start = (timestamp.astimezone(UTC) + REFUND_BOUNDARY).date()
    return (window_start + timedelta(days=1)).isoformat()


def fill_cost(fill: Execution, context: OrderContext | None) -> FillCost:
    """把一笔成交折算为名义、费率与滑点。"""
    notional = fill.price * fill.size
    fee_bps = (fill.fee / notional * BPS) if notional else Decimal(0)
    liquidity = _liquidity(fill.fee)
    nominal = _nominal_fee(str(fill.symbol), notional, liquidity)
    # 取整总在交易所一侧，残差非负
    residual = fill.fee - nominal
    reference = (
        Decimal(context.reference_price)
        if context is not None and context.reference_price is not None
        else None
    )
    slippage: Decimal | None = None
    if reference is not None and reference > 0:
        signed = fill.price - reference
        if str(fill.side.value) == "SELL":
            signed = -signed
        slippage = signed / reference * BPS
    return FillCost(
        order_id=fill.order_id,
        execution_id=fill.execution_id,
        symbol=str(fill.symbol),
        side=str(fill.side.value),
        size=str(fill.size),
        price=str(fill.price),
        notional_jpy=str(notional),
        fee_jpy=str(fill.fee),
        fee_bps=str(fee_bps.quantize(Decimal("0.0001"))),
        liquidity=liquidity,
        nominal_fee_jpy=str(nominal.quantize(Decimal("0.000001"))),
        rounding_residual_jpy=str(residual.quantize(Decimal("0.000001"))),
        refund_day=refund_day(fill.timestamp),
        reference_price=None if reference is None else str(reference),
        slippage_bps=(
            None if slippage is None
            else str(slippage.quantize(Decimal("0.0001")))
        ),
        timestamp=fill.timestamp.isoformat(),
    )


def _weighted(rows: Sequence[FillCost], field: str) -> str | None:
    pairs = [
        (Decimal(getattr(row, field)), Decimal(row.notional_jpy))
        for row in rows if getattr(row, field) is not None
    ]
    total = sum((weight for _, weight in pairs), Decimal(0))
    if not pairs or total == 0:
        return None
    value = sum((item * weight for item, weight in pairs), Decimal(0)) / total
    return str(value.quantize(Decimal("0.0001")))


def refund_estimate(rows: Sequence[FillCost]) -> dict[str, str]:
    """按品种与返还日合算取整残差，整数部分即次日返还估计。"""
    residual: dict[tuple[str, str], Decimal] = {}
    for row in rows:
        key = (row.symbol, row.refund_day)
        residual[key] = residual.get(key, Decimal(0)) + Decimal(row.rounding_residual_jpy)
    return {
        f"{symbol}/{day}": str(int(value.to_integral_value(rounding="ROUND_FLOOR")))
        for (symbol, day), value in sorted(residual.items())
    }


def _bucket(rows: Sequence[FillCost], *, refund: bool = True) -> dict[str, object]:
    total = sum((Decimal(row.notional_jpy) for row in rows), Decimal(0))
    fee_total = sum((Decimal(row.fee_jpy) for row in rows), Decimal(0))
    refunds = refund_estimate(rows) if refund else {}
    refund_total = sum((Decimal(value) for value in refunds.values()), Decimal(0))
    effective = fee_total - refund_total
    maker = sum(
        (Decimal(row.notional_jpy) for row in rows if row.liquidity == "maker"),
        Decimal(0),
    )
    fee_bps = _weighted(rows, "fee_bps")
    slippage_bps = _weighted(rows, "slippage_bps")
    total_cost: str | None = None
    if fee_bps is not None and slippage_bps is not None:
        total_cost = str(Decimal(fee_bps) + Decimal(slippage_bps))
    return {
        "fill_count": len(rows),
        "order_count": len({row.order_id for row in rows}),
        "notional_jpy": str(total),
        "fee_jpy": str(fee_total),
        "fee_bps_weighted": fee_bps,
        "nominal_fee_jpy": str(sum((Decimal(row.nominal_fee_jpy) for row in rows), Decimal(0))),
        "rounding_residual_jpy": str(
            sum((Decimal(row.rounding_residual_jpy) for row in rows), Decimal(0))
        ),
        "refund_estimate_jpy": str(refund_total) if refund else None,
        "fee_effective_jpy": str(effective) if refund else None,
        "fee_bps_effective_weighted": (
            None if not refund or total == 0
            else str((effective / total * BPS).quantize(Decimal("0.0001")))
        ),
        "slippage_bps_weighted": slippage_bps,
        "total_cost_bps_weighted": total_cost,
        "maker_notional_share": (
            None if total == 0 else str((maker / total).quantize(Decimal("0.0001")))
        ),
    }


def summarize(
    fills: Iterable[FillCost], orders: Mapping[int, OrderContext]
) -> dict[str, object]:
    """按全体、品种与方向汇总成本。"""
    rows = sorted(fills, key=lambda row: (row.timestamp, row.execution_id))
    by_symbol = {
        symbol: _bucket([row for row in rows if row.symbol == symbol])
        for symbol in sorted({row.symbol for row in rows})
    }
    # 返还按品种逐日合算，按方向拆分无法归属
    by_side = {
        side: _bucket([row for row in rows if row.side == side], refund=False)
        for side in sorted({row.side for row in rows})
    }
    filled_orders = {row.order_id for row in rows}
    return {
        "schema_version": 1,
        "kind": "live_cost_summary",
        "generated_at": datetime.now(UTC).isoformat(),
        "orders_collected": len(orders),
        "orders_without_fill": sorted(set(orders) - filled_orders),
        "overall": _bucket(rows),
        "by_symbol": by_symbol,
        "by_side": by_side,
        "refund_estimate": refund_estimate(rows),
        "rounding_rule": (
            "taker 逐笔向上取整、maker 逐笔向下取整；小数部分按品种逐日合算，"
            "每营业日 06:00 JST 返还或支付整数部分"
        ),
        "fills": [asdict(row) for row in rows],
    }


def fetch_fills(
    client: ReadClient, orders: Mapping[int, OrderContext]
) -> list[FillCost]:
    """逐委托取成交明细（U-01）。"""
    rows: list[FillCost] = []
    for order_id in sorted(orders):
        for fill in client.executions(order_id=order_id):
            rows.append(fill_cost(fill, orders.get(order_id)))
    return rows


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="汇总 live 实测成交成本")
    parser.add_argument(
        "--repository", type=Path, default=Path("."),
        help="执行仓根目录，含 data/execution/live 与 .env",
    )
    parser.add_argument("--env-file", type=Path, default=None)
    parser.add_argument(
        "--output", type=Path, default=None,
        help="报告输出路径；缺省写入执行仓 cost-summary 目录",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    root = args.repository.resolve()
    orders = collect_orders(root)
    config = load_config(args.env_file if args.env_file else root / ".env")
    client = ReadClient.from_config(config)
    summary = summarize(fetch_fills(client, orders), orders)
    output = args.output
    if output is None:
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        output = root / SUMMARY_DIRECTORY / f"cost-summary-{stamp}.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8",
    )
    summary["report_path"] = str(output)
    print(json.dumps(
        {key: value for key, value in summary.items() if key != "fills"},
        ensure_ascii=False, sort_keys=True,
    ))
    return 0


if __name__ == "__main__":
    sys.exit(main())
