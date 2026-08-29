"""G-05 转换闸门单测：取整边界与超界拒绝。"""
from __future__ import annotations

from decimal import Decimal

import pytest

from guvolu.domain.enums import Side
from guvolu.domain.errors import SymbolError
from guvolu.domain.models import SymbolRule
from guvolu.domain.symbols import SpotSymbol
from guvolu.execution.conversion import (
    ConversionError,
    MarketRule,
    OrderProposal,
    convert_target_to_order,
)

RULE = MarketRule(
    symbol=SpotSymbol("BTC"),
    tick_size=Decimal("1"),
    size_step=Decimal("0.0001"),
    min_order_size=Decimal("0.0001"),
    max_order_size=Decimal("5"),
)


def convert(
    target: float,
    budget: str = "500",
    price: str = "1000000",
    rule: MarketRule = RULE,
) -> OrderProposal | None:
    """以缺省预算与参考价调用转换。"""
    return convert_target_to_order(
        target,
        budget_jpy=Decimal(budget),
        reference_price=Decimal(price),
        rule=rule,
    )


def test_full_target_buy() -> None:
    """满仓目标折算为整步数量与限价。"""
    proposal = convert(1.0)
    assert proposal is not None
    assert proposal.side is Side.BUY
    assert proposal.size == Decimal("0.0005")
    assert proposal.price == Decimal("1000000")
    assert proposal.notional_jpy == Decimal("500")


def test_size_floors_to_step() -> None:
    """数量向下取整到 sizeStep，绝不超过目标名义。"""
    proposal = convert(0.77)
    assert proposal is not None
    # 原始 0.000385 取整
    assert proposal.size == Decimal("0.0003")
    assert proposal.notional_jpy == Decimal("300")


def test_buy_price_floors_to_tick() -> None:
    """买向限价向下取整到 tickSize。"""
    proposal = convert(1.0, price="1234567.4")
    assert proposal is not None
    assert proposal.price == Decimal("1234567")


def test_sell_price_ceils_to_tick() -> None:
    """卖向限价向上取整到 tickSize。"""
    proposal = convert(-1.0, price="1234567.4")
    assert proposal is not None
    assert proposal.side is Side.SELL
    assert proposal.price == Decimal("1234568")


def test_fractional_tick_rounding() -> None:
    """非整数 tickSize 同样按方向取整。"""
    rule = MarketRule(
        symbol=SpotSymbol("XRP"),
        tick_size=Decimal("0.001"),
        size_step=Decimal("1"),
        min_order_size=Decimal("1"),
        max_order_size=Decimal("100000"),
    )
    buy = convert(1.0, budget="500", price="12.3456", rule=rule)
    sell = convert(-1.0, budget="500", price="12.3456", rule=rule)
    assert buy is not None and buy.price == Decimal("12.345")
    assert sell is not None and sell.price == Decimal("12.346")
    assert buy.size == Decimal("40")


def test_exact_tick_price_unchanged() -> None:
    """恰在档位上的参考价不变。"""
    buy = convert(1.0, price="1000000")
    sell = convert(-1.0, price="1000000")
    assert buy is not None and buy.price == Decimal("1000000")
    assert sell is not None and sell.price == Decimal("1000000")


def test_negative_target_sells() -> None:
    """负目标折算为卖向委托。"""
    proposal = convert(-0.5)
    assert proposal is not None
    assert proposal.side is Side.SELL
    assert proposal.size == Decimal("0.0002")


def test_zero_target_yields_none() -> None:
    """零目标无需委托。"""
    assert convert(0.0) is None


def test_below_min_order_size_yields_none() -> None:
    """折算数量低于最小委托量时不生成委托。"""
    assert convert(0.1) is None


def test_above_max_order_size_rejected() -> None:
    """数量超过委托量上限即拒绝。"""
    small_max = MarketRule(
        symbol=SpotSymbol("BTC"),
        tick_size=Decimal("1"),
        size_step=Decimal("0.0001"),
        min_order_size=Decimal("0.0001"),
        max_order_size=Decimal("0.0002"),
    )
    with pytest.raises(ConversionError, match="上限"):
        convert(1.0, rule=small_max)


def test_target_outside_unit_interval_rejected() -> None:
    """目标越出 [-1, 1] 即拒绝。"""
    for target in (1.01, -1.01, 2.0):
        with pytest.raises(ConversionError, match="越界"):
            convert(target)


def test_non_finite_target_rejected() -> None:
    """非有限数一律拒绝。"""
    for target in (float("nan"), float("inf"), float("-inf")):
        with pytest.raises(ConversionError, match="有限"):
            convert(target)


def test_nonpositive_budget_and_price_rejected() -> None:
    """预算与参考价必须为正。"""
    with pytest.raises(ConversionError, match="预算"):
        convert(1.0, budget="0")
    with pytest.raises(ConversionError, match="参考价"):
        convert(1.0, price="-1")


def test_rule_requires_positive_fields() -> None:
    """取引ルール字段必须为正。"""
    with pytest.raises(ConversionError, match="必须为正"):
        MarketRule(
            symbol=SpotSymbol("BTC"),
            tick_size=Decimal("0"),
            size_step=Decimal("0.0001"),
            min_order_size=Decimal("0.0001"),
            max_order_size=Decimal("5"),
        )


def test_rule_rejects_inverted_bounds() -> None:
    """委托量上限低于下限即拒绝。"""
    with pytest.raises(ConversionError, match="上限低于下限"):
        MarketRule(
            symbol=SpotSymbol("BTC"),
            tick_size=Decimal("1"),
            size_step=Decimal("0.0001"),
            min_order_size=Decimal("1"),
            max_order_size=Decimal("0.5"),
        )


def test_leverage_symbol_unreachable_via_rule() -> None:
    """杠杆形态无法进入转换规则（T-09）。"""
    leverage = SymbolRule(
        symbol="BTC_JPY",
        min_order_size=Decimal("0.01"),
        max_order_size=Decimal("5"),
        size_step=Decimal("0.01"),
        tick_size=Decimal("1"),
        taker_fee=Decimal("0"),
        maker_fee=Decimal("0"),
    )
    with pytest.raises(SymbolError):
        MarketRule.from_symbol_rule(leverage)


def test_from_symbol_rule_carries_fields() -> None:
    """取引ルール字段逐项进入转换规则。"""
    spot = SymbolRule(
        symbol="BTC",
        min_order_size=Decimal("0.0001"),
        max_order_size=Decimal("5"),
        size_step=Decimal("0.0001"),
        tick_size=Decimal("1"),
        taker_fee=Decimal("0.0005"),
        maker_fee=Decimal("-0.0001"),
    )
    rule = MarketRule.from_symbol_rule(spot)
    assert rule.symbol == SpotSymbol("BTC")
    assert rule.tick_size == Decimal("1")
    assert rule.size_step == Decimal("0.0001")
    assert rule.min_order_size == Decimal("0.0001")
    assert rule.max_order_size == Decimal("5")
