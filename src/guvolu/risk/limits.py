"""三限额闸门（T-11）。金额一律 Decimal（T-08）。

限额取自 domain.config.Limits，装载时已按绝对硬顶截取；
本闸门在运行时只允许继续调低（X-05）。超限抛出明确错误，
调用方必须按 T-11 触发熔断而非仅告警。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from guvolu.domain.config import Limits
from guvolu.risk.errors import LimitAdjustmentRejected, LimitExceeded

# 交易日边界（C-11、D-08）
_JST = timezone(timedelta(hours=9))
_TRADING_DAY_START_HOUR = 6


def trading_day(moment: datetime) -> date:
    """按 JST 06:00 边界归属交易日（D-08）。"""
    if moment.tzinfo is None:
        raise ValueError("时刻必须带时区")
    jst = moment.astimezone(_JST)
    return (jst - timedelta(hours=_TRADING_DAY_START_HOUR)).date()


@dataclass(frozen=True, slots=True)
class DayUsage:
    """当日闸门用量视图。"""

    day: date | None
    total_jpy: Decimal
    order_count: int


class LimitGate:
    """单笔金额、单日累计金额、单日笔数三限额（T-11）。

    累计在通过闸门时记入，随后发送即使超时或被拒也不回退，
    保持保守计数（T-06 超时结果未知，宁可多计）。
    """

    def __init__(self, limits: Limits) -> None:
        self._limits = limits
        self._day: date | None = None
        self._total_jpy = Decimal("0")
        self._order_count = 0

    @property
    def limits(self) -> Limits:
        """当前生效限额。"""
        return self._limits

    def usage(self) -> DayUsage:
        """取当日累计用量。"""
        return DayUsage(
            day=self._day,
            total_jpy=self._total_jpy,
            order_count=self._order_count,
        )

    def tighten(self, new_limits: Limits) -> None:
        """运行时调整限额，只可调低（T-11、X-05）。"""
        if (
            new_limits.order_jpy_max <= 0
            or new_limits.day_jpy_max <= 0
            or new_limits.day_count_max <= 0
        ):
            raise LimitAdjustmentRejected("限额必须为正")
        current = self._limits
        if (
            new_limits.order_jpy_max > current.order_jpy_max
            or new_limits.day_jpy_max > current.day_jpy_max
            or new_limits.day_count_max > current.day_count_max
        ):
            raise LimitAdjustmentRejected("限额只可调低，不可调高")
        self._limits = new_limits

    def _roll_day(self, moment: datetime) -> None:
        """跨交易日清零累计。"""
        day = trading_day(moment)
        if self._day != day:
            self._day = day
            self._total_jpy = Decimal("0")
            self._order_count = 0

    def check(self, notional_jpy: Decimal, moment: datetime) -> None:
        """只校验不记账，越限抛出明确错误。"""
        if notional_jpy <= 0:
            raise ValueError("名义金额必须为正")
        self._roll_day(moment)
        limits = self._limits
        if notional_jpy > limits.order_jpy_max:
            raise LimitExceeded(
                f"单笔 {notional_jpy} JPY 超上限 {limits.order_jpy_max} JPY"
            )
        projected_total = self._total_jpy + notional_jpy
        if projected_total > limits.day_jpy_max:
            raise LimitExceeded(
                f"当日累计 {projected_total} JPY 超上限 {limits.day_jpy_max} JPY"
            )
        projected_count = self._order_count + 1
        if projected_count > limits.day_count_max:
            raise LimitExceeded(
                f"当日笔数 {projected_count} 超上限 {limits.day_count_max}"
            )

    def commit(self, notional_jpy: Decimal, moment: datetime) -> None:
        """校验并记入当日累计，通过闸门即计数。"""
        self.check(notional_jpy, moment)
        self._total_jpy += notional_jpy
        self._order_count += 1
