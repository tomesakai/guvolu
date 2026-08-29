"""差分折算单测：不交易带与取整边界（G-05、C-15）。"""
from __future__ import annotations

from decimal import Decimal

import pytest

from guvolu.domain.enums import Side
from guvolu.domain.symbols import SpotSymbol
from guvolu.execution.conversion import (
    ConversionError,
    MarketRule,
    convert_target_to_delta_order,
)

RULE = MarketRule(
    symbol=SpotSymbol("BTC"),
    tick_size=Decimal("1"),
    size_step=Decimal("0.0001"),
    min_order_size=Decimal("0.0001"),
    max_order_size=Decimal("5"),
)
BUDGET = Decimal("500")
PRICE = Decimal("1000000")
BAND = Decimal("0.01")


def decide(
    target: float,
    position: str,
    band: Decimal = BAND,
    price: Decimal = PRICE,
) -> object:
    """按缺省预算执行一次差分折算。"""
    return convert_target_to_delta_order(
        target,
        position_size=Decimal(position),
        budget_jpy=BUDGET,
        reference_price=price,
        rule=RULE,
        no_trade_band=band,
    )


def test_zero_position_equals_full_conversion() -> None:
    """零持仓时差分等价一次性折算。"""
    decision = decide(0.6, "0")
    assert decision.desired_size == Decimal("0.0003")
    assert decision.delta_size == Decimal("0.0003")
    proposal = decision.proposal
    assert proposal is not None
    assert proposal.side is Side.BUY
    assert proposal.size == Decimal("0.0003")
    assert proposal.price == PRICE


def test_partial_position_orders_only_difference() -> None:
    """已有持仓只补差分数量。"""
    decision = decide(0.6, "0.0002")
    assert decision.delta_size == Decimal("0.0001")
    proposal = decision.proposal
    assert proposal is not None
    assert proposal.side is Side.BUY
    assert proposal.size == Decimal("0.0001")


def test_overshoot_position_sells_difference() -> None:
    """持仓超过目标即反向卖出差分。"""
    decision = decide(0.2, "0.0003")
    assert decision.delta_size == Decimal("-0.0002")
    proposal = decision.proposal
    assert proposal is not None
    assert proposal.side is Side.SELL
    assert proposal.size == Decimal("0.0002")


def test_sell_price_rounds_up_to_tick() -> None:
    """卖向限价向上取整到刻度，不劣于参考价。"""
    decision = decide(0.0, "0.0002", price=Decimal("1000000.5"))
    proposal = decision.proposal
    assert proposal is not None
    assert proposal.side is Side.SELL
    assert proposal.price == Decimal("1000001")


def test_zero_target_unwinds_position() -> None:
    """目标为零时平掉账本推算持仓。"""
    decision = decide(0.0, "0.0002")
    assert decision.desired_size == Decimal("0")
    proposal = decision.proposal
    assert proposal is not None
    assert proposal.side is Side.SELL
    assert proposal.size == Decimal("0.0002")


def test_zero_delta_skips() -> None:
    """差分为零不生成委托。"""
    decision = decide(0.6, "0.0003")
    assert decision.proposal is None
    assert decision.skip_reason == "差分为零，无需委托"


def test_band_swallows_dust_difference() -> None:
    """差分名义低于不交易带乘预算即忽略。"""
    decision = decide(0.6, "0.000296")
    # 名义 4 低于带宽 5
    assert decision.delta_size == Decimal("0.000004")
    assert decision.proposal is None
    assert decision.skip_reason == "差分名义在不交易带内，无需委托"


def test_band_boundary_trades() -> None:
    """差分名义恰等于带宽时生成委托。"""
    decision = decide(0.6, "0.0002", band=Decimal("0.2"))
    # 名义恰等带宽 100
    proposal = decision.proposal
    assert proposal is not None
    assert proposal.size == Decimal("0.0001")


def test_below_min_order_size_skips() -> None:
    """带外差分仍低于最小委托量时不生成委托。"""
    decision = decide(0.6, "0.00025", band=Decimal("0"))
    assert decision.delta_size == Decimal("0.00005")
    assert decision.proposal is None
    assert decision.skip_reason == "差分数量低于最小委托量，无需委托"


def test_size_floors_to_step() -> None:
    """差分数量向下取整到步长，绝不超过差分名义。"""
    decision = decide(0.6, "0.00015")
    proposal = decision.proposal
    assert proposal is not None
    assert proposal.size == Decimal("0.0001")


def test_invalid_inputs_rejected() -> None:
    """越界目标、非法带宽与非正金额一律拒绝。"""
    with pytest.raises(ConversionError, match="越界"):
        decide(1.5, "0")
    with pytest.raises(ConversionError, match="非有限数"):
        decide(float("nan"), "0")
    with pytest.raises(ConversionError, match="不交易带"):
        decide(0.6, "0", band=Decimal("1"))
    with pytest.raises(ConversionError, match="不交易带"):
        decide(0.6, "0", band=Decimal("-0.1"))
    with pytest.raises(ConversionError, match="参考价"):
        decide(0.6, "0", price=Decimal("0"))
    with pytest.raises(ConversionError, match="上限"):
        convert_target_to_delta_order(
            1.0,
            position_size=Decimal("-6"),
            budget_jpy=BUDGET,
            reference_price=PRICE,
            rule=RULE,
            no_trade_band=Decimal("0"),
        )
