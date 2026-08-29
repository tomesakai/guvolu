"""限额用量重放：自意图账本重建当日累计（T-11）。

单发式执行器与浸泡进程的限额闸门只在内存，启动时须按 JST
06:00 交易日边界自意图账本重放当日用量。只统计账本标记为消耗
写预算的意图：零写终态（dry-run 拦截与 paper 结算）不占单日
预算，T-11 的语义边界是真实写请求；无标记的旧版行按消耗计，
保守口径。dry-run、paper 与浸泡入口共用本函数，不各写一份。
"""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from guvolu.data.intent_ledger import IntentLedger
from guvolu.domain.errors import GuvoluError
from guvolu.domain.intent import IntentState
from guvolu.risk.limits import LimitGate, trading_day


class LimitReplayError(GuvoluError):
    """账本内容无法折算为限额用量。"""


def replay_limit_usage(
    limit_gate: LimitGate, ledger: IntentLedger, *, moment: datetime
) -> dict[str, object]:
    """重放当日消耗写预算的用量并预置闸门，返回报告体。

    口径与内存闸门一致：进入 SENDING 及其后状态且消耗写预算的
    意图在过闸时已计入，随后即使超时或被拒也不回退，保守计数。
    """
    day = trading_day(moment)
    total_jpy = Decimal("0")
    order_count = 0
    replayed: list[str] = []
    for intent_id in ledger.intent_ids():
        state = ledger.state(intent_id)
        if state in {IntentState.RECORDED, IntentState.GATE_REJECTED}:
            continue
        if not ledger.consumed_write_budget(intent_id):
            continue
        intent = ledger.intent(intent_id)
        if trading_day(intent.created_at) != day:
            continue
        if intent.price is None:
            raise LimitReplayError(
                f"意图账本含无法折算名义的非限价意图 {intent_id}"
            )
        total_jpy += intent.notional_jpy()
        order_count += 1
        replayed.append(intent_id)
    limit_gate.seed_usage(day, total_jpy, order_count)
    usage = limit_gate.usage()
    return {
        "trading_day": day.isoformat(),
        "total_jpy": format(usage.total_jpy, "f"),
        "order_count": usage.order_count,
        "replayed_intents": replayed,
    }
