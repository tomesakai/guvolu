"""G-05 转换闸门：float 数值域到 Decimal 金额域的唯一转换点。

研究域目标位置以 float 承载；进入执行域必须经本模块唯一函数
完成 float 到 Decimal 的转换，并按品种 tickSize 与 sizeStep
取整，超界拒绝（G-05）。全项目仅此一处允许两域相接，除此之外
执行域一切入口只接受 Decimal（T-08）。
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from decimal import Decimal

from guvolu.domain.enums import Side
from guvolu.domain.errors import GuvoluError
from guvolu.domain.models import SymbolRule
from guvolu.domain.symbols import SpotSymbol


class ConversionError(GuvoluError):
    """转换被拒：输入非法或越界（G-05）。"""


@dataclass(frozen=True, slots=True)
class MarketRule:
    """转换所需的取引ルール子集，仅现物形态（T-09）。"""

    symbol: SpotSymbol
    tick_size: Decimal
    size_step: Decimal
    min_order_size: Decimal
    max_order_size: Decimal

    def __post_init__(self) -> None:
        for name in (
            "tick_size", "size_step", "min_order_size", "max_order_size"
        ):
            value: Decimal = getattr(self, name)
            if value <= 0:
                raise ConversionError(f"取引ルール {name} 必须为正")
        if self.max_order_size < self.min_order_size:
            raise ConversionError("委托量上限低于下限")

    @classmethod
    def from_symbol_rule(cls, rule: SymbolRule) -> "MarketRule":
        """取自公开 API 取引ルール；杠杆形态被类型拒绝（T-09）。"""
        return cls(
            symbol=SpotSymbol(rule.symbol),
            tick_size=rule.tick_size,
            size_step=rule.size_step,
            min_order_size=rule.min_order_size,
            max_order_size=rule.max_order_size,
        )


@dataclass(frozen=True, slots=True)
class OrderProposal:
    """转换产物：金额域委托参数（T-08）。"""

    symbol: SpotSymbol
    side: Side
    size: Decimal
    price: Decimal
    notional_jpy: Decimal


def _floor_step(value: Decimal, step: Decimal) -> Decimal:
    """向下取整到步长整数倍。输入均为正。"""
    return (value // step) * step


def _ceil_step(value: Decimal, step: Decimal) -> Decimal:
    """向上取整到步长整数倍。输入均为正。"""
    floored = _floor_step(value, step)
    return floored if floored == value else floored + step


def convert_target_to_order(
    target: float,
    *,
    budget_jpy: Decimal,
    reference_price: Decimal,
    rule: MarketRule,
) -> OrderProposal | None:
    """把 float 方向目标转换为 Decimal 委托参数。

    本函数是 G-05 规定的全项目唯一 float 到 Decimal 转换点。
    目标取值域为 [-1, 1]，正买负卖；名义金额为目标绝对值乘
    预算。数量向下取整到 sizeStep，绝不超过目标名义；价格按
    买向下、卖向上取整到 tickSize，使限价不劣于参考价。折算
    数量低于最小委托量时返回 None 表示无需委托；非有限数、
    越界目标与超出委托量上限一律拒绝。
    """
    if not math.isfinite(target):
        raise ConversionError(f"目标非有限数: {target!r}")
    if abs(target) > 1:
        raise ConversionError(f"目标越界: {target!r} 不在 [-1, 1]")
    if budget_jpy <= 0:
        raise ConversionError("预算必须为正")
    if reference_price <= 0:
        raise ConversionError("参考价必须为正")
    if target == 0:
        return None
    # 两域相接的唯一转换（G-05）
    magnitude = abs(Decimal(str(target)))
    side = Side.BUY if target > 0 else Side.SELL
    raw_size = magnitude * budget_jpy / reference_price
    size = _floor_step(raw_size, rule.size_step)
    if size < rule.min_order_size:
        return None
    if size > rule.max_order_size:
        raise ConversionError(
            f"数量 {size} 超过委托量上限 {rule.max_order_size}"
        )
    if side is Side.BUY:
        price = _floor_step(reference_price, rule.tick_size)
    else:
        price = _ceil_step(reference_price, rule.tick_size)
    if price <= 0:
        raise ConversionError("取整后价格非正")
    return OrderProposal(
        symbol=rule.symbol,
        side=side,
        size=size,
        price=price,
        notional_jpy=size * price,
    )
