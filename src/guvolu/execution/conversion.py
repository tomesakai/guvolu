"""G-05 转换闸门：float 数值域到 Decimal 金额域的唯一转换点。

研究域目标位置以 float 承载；进入执行域必须经本模块完成
float 到 Decimal 的转换，并按品种 tickSize 与 sizeStep 取整，
超界拒绝（G-05）。转换语句唯一收敛在 _target_to_decimal，两个
公开入口（一次性折算与差分折算）共用之；全项目仅此一处允许
两域相接，除此之外执行域一切入口只接受 Decimal（T-08）。
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


def _target_to_decimal(target: float) -> Decimal:
    """G-05 规定的唯一 float 到 Decimal 转换语句。

    目标取值域为 [-1, 1]，非有限数与越界一律拒绝。
    """
    if not math.isfinite(target):
        raise ConversionError(f"目标非有限数: {target!r}")
    if abs(target) > 1:
        raise ConversionError(f"目标越界: {target!r} 不在 [-1, 1]")
    # 两域相接的唯一转换（G-05）
    return Decimal(str(target))


def _ensure_positive_money(
    budget_jpy: Decimal, reference_price: Decimal
) -> None:
    """预算与参考价必须为正。"""
    if budget_jpy <= 0:
        raise ConversionError("预算必须为正")
    if reference_price <= 0:
        raise ConversionError("参考价必须为正")


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
    desired = _target_to_decimal(target)
    _ensure_positive_money(budget_jpy, reference_price)
    if desired == 0:
        return None
    magnitude = abs(desired)
    side = Side.BUY if desired > 0 else Side.SELL
    raw_size = magnitude * budget_jpy / reference_price
    size = _floor_step(raw_size, rule.size_step)
    if size < rule.min_order_size:
        return None
    return _build_proposal(size, side, reference_price, rule)


def _build_proposal(
    size: Decimal,
    side: Side,
    reference_price: Decimal,
    rule: MarketRule,
) -> OrderProposal:
    """按取整规则落定价格并构造委托参数。"""
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


@dataclass(frozen=True, slots=True)
class DeltaDecision:
    """差分决策结果，报告义务的数据载体（A-03）。"""

    desired_size: Decimal
    position_size: Decimal
    delta_size: Decimal
    proposal: OrderProposal | None
    skip_reason: str | None


def convert_target_to_delta_order(
    target: float,
    *,
    position_size: Decimal,
    budget_jpy: Decimal,
    reference_price: Decimal,
    rule: MarketRule,
    no_trade_band: Decimal,
) -> DeltaDecision:
    """把 float 方向目标与推算持仓折算为差分委托参数。

    与 convert_target_to_order 共用唯一转换语句（G-05）。目标
    持仓为目标乘预算除参考价（带符号，正多负空）；差分为目标
    持仓减实际持仓。持仓输入只可来自 READ_ONLY 确认的成交事实
    （T-03），由调用方保证。不交易带与研究配置 no_trade_band
    同口径（目标权重空间的比例，见策略研究管线）：差分名义
    金额低于不交易带乘预算时不生成委托，避免尘埃级往返；恰好
    等于边界时生成。数量向下取整到 sizeStep 绝不超过差分名义，
    限价按买向下、卖向上取整到 tickSize；折算数量低于最小委托
    量时不生成委托。
    """
    desired_raw = _target_to_decimal(target)
    _ensure_positive_money(budget_jpy, reference_price)
    if no_trade_band < 0 or no_trade_band >= 1:
        raise ConversionError("不交易带必须在 [0, 1) 内")
    desired = desired_raw * budget_jpy / reference_price
    delta = desired - position_size

    def skip(reason: str) -> DeltaDecision:
        return DeltaDecision(
            desired_size=desired,
            position_size=position_size,
            delta_size=delta,
            proposal=None,
            skip_reason=reason,
        )

    if delta == 0:
        return skip("差分为零，无需委托")
    if abs(delta) * reference_price < no_trade_band * budget_jpy:
        return skip("差分名义在不交易带内，无需委托")
    size = _floor_step(abs(delta), rule.size_step)
    if size < rule.min_order_size:
        return skip("差分数量低于最小委托量，无需委托")
    side = Side.BUY if delta > 0 else Side.SELL
    proposal = _build_proposal(size, side, reference_price, rule)
    return DeltaDecision(
        desired_size=desired,
        position_size=position_size,
        delta_size=delta,
        proposal=proposal,
        skip_reason=None,
    )
