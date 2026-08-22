"""差异账按交易日汇总（D-08）。

读取 paper 执行器的差异账，按决策时刻所属交易日聚合决策数、
意图终态、模型成交名义、费用与成本均值、覆盖层 would_apply 次数。
金额与基点以 Decimal 计算并以字符串输出（T-08、D-07）。
"""
from __future__ import annotations

import argparse
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from guvolu.data.paths import data_root
from guvolu.domain.errors import GuvoluError
from guvolu.execution.paper_config import (
    DEFAULT_PAPER_CONFIG_PATH,
    load_paper_config,
)
from guvolu.execution.paper_executor import (
    DIFFERENCE_LEDGER_NAME,
    read_difference_rows,
)
from guvolu.risk.limits import trading_day

SUMMARY_SCHEMA_VERSION = 1


class SummaryError(GuvoluError):
    """差异账行缺少汇总所需字段。"""


@dataclass(slots=True)
class DaySummary:
    """单交易日的聚合视图。"""

    decisions: int = 0
    intents: int = 0
    paper_filled: int = 0
    paper_rejected: int = 0
    gate_rejected: int = 0
    skipped: int = 0
    other: int = 0
    buy_notional_jpy: Decimal = Decimal("0")
    sell_notional_jpy: Decimal = Decimal("0")
    fee_jpy: Decimal = Decimal("0")
    total_cost_bps_sum: Decimal = Decimal("0")
    overlay_would_apply: int = 0
    overlay_incomplete: int = 0
    fee_fallback: int = 0
    statuses: dict[str, int] = field(default_factory=dict)

    def as_record(self) -> dict[str, object]:
        """转为输出记录，金额以字符串表达（D-07）。"""
        mean_cost = (
            None
            if self.paper_filled == 0
            else format(self.total_cost_bps_sum / self.paper_filled, "f")
        )
        return {
            "decisions": self.decisions,
            "intents": self.intents,
            "paper_filled": self.paper_filled,
            "paper_rejected": self.paper_rejected,
            "gate_rejected": self.gate_rejected,
            "skipped": self.skipped,
            "other": self.other,
            "buy_notional_jpy": format(self.buy_notional_jpy, "f"),
            "sell_notional_jpy": format(self.sell_notional_jpy, "f"),
            "fee_jpy": format(self.fee_jpy, "f"),
            "mean_total_cost_bps": mean_cost,
            "overlay_would_apply": self.overlay_would_apply,
            "overlay_incomplete": self.overlay_incomplete,
            "fee_fallback": self.fee_fallback,
            "statuses": dict(sorted(self.statuses.items())),
        }


def _decimal(payload: Mapping[str, object], key: str) -> Decimal:
    raw = payload.get(key)
    if not isinstance(raw, str):
        raise SummaryError(f"差异账字段 {key} 必须为字符串数值")
    try:
        return Decimal(raw)
    except InvalidOperation as exc:
        raise SummaryError(f"差异账字段 {key} 不是合法数值") from exc


def summarize_rows(
    rows: Sequence[Mapping[str, object]],
) -> dict[str, dict[str, object]]:
    """按交易日汇总差异账行，键为 ISO 日期。"""
    days: dict[str, DaySummary] = {}
    for row in rows:
        decision_raw = row.get("decision_time")
        if not isinstance(decision_raw, str):
            raise SummaryError("差异账行缺少 decision_time")
        try:
            decision_time = datetime.fromisoformat(decision_raw)
            day = trading_day(decision_time).isoformat()
        except ValueError as exc:
            raise SummaryError("差异账 decision_time 非法") from exc
        summary = days.setdefault(day, DaySummary())
        summary.decisions += 1
        status = str(row.get("status"))
        summary.statuses[status] = summary.statuses.get(status, 0) + 1
        if status == "PAPER_FILLED":
            summary.intents += 1
            summary.paper_filled += 1
        elif status == "PAPER_REJECTED":
            summary.intents += 1
            summary.paper_rejected += 1
        elif status == "GATE_REJECTED":
            summary.intents += 1
            summary.gate_rejected += 1
        elif status == "skipped":
            summary.skipped += 1
        else:
            summary.other += 1
        fill = row.get("fill")
        cost = row.get("cost")
        if isinstance(fill, Mapping) and isinstance(cost, Mapping):
            notional = _decimal(fill, "notional_jpy")
            if fill.get("side") == "BUY":
                summary.buy_notional_jpy += notional
            else:
                summary.sell_notional_jpy += notional
            summary.fee_jpy += _decimal(fill, "fee_jpy")
            summary.total_cost_bps_sum += _decimal(cost, "total_cost_bps")
        fee = row.get("fee")
        if isinstance(fee, Mapping) and fee.get("source") == "config_fallback":
            summary.fee_fallback += 1
        overlay = row.get("overlay")
        if isinstance(overlay, Mapping):
            if overlay.get("would_apply") is True:
                summary.overlay_would_apply += 1
            if overlay.get("complete") is False:
                summary.overlay_incomplete += 1
    return {day: days[day].as_record() for day in sorted(days)}


def summarize_ledger(path: Path) -> dict[str, object]:
    """读取差异账并生成汇总报告。"""
    rows = read_difference_rows(path)
    return {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "ledger_path": str(path),
        "rows": len(rows),
        "days": summarize_rows(rows),
    }


def main(argv: Sequence[str] | None = None) -> int:
    """命令行入口：按日汇总差异账并输出 JSON。"""
    parser = argparse.ArgumentParser(description="paper 差异账按日汇总")
    parser.add_argument(
        "--ledger", type=Path, default=None,
        help="差异账路径；缺省按配置目录在账目根下解析",
    )
    parser.add_argument(
        "--config", type=Path, default=DEFAULT_PAPER_CONFIG_PATH,
    )
    parser.add_argument("--ledger-root", type=Path, default=None)
    args = parser.parse_args(argv)
    ledger_arg: Path | None = args.ledger
    if ledger_arg is not None:
        path = ledger_arg
    else:
        config_path: Path = args.config
        config = load_paper_config(config_path)
        root_arg: Path | None = args.ledger_root
        root = root_arg if root_arg is not None else data_root()
        path = root / config.ledger_directory / DIFFERENCE_LEDGER_NAME
    print(json.dumps(
        summarize_ledger(path), ensure_ascii=False, indent=2, sort_keys=True,
    ))
    return 0
