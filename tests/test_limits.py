"""三限额闸门单测（T-11、C-15）。全部离线（C-13）。"""
from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from guvolu.domain.config import Limits
from guvolu.risk.errors import LimitAdjustmentRejected, LimitExceeded
from guvolu.risk.limits import LimitGate, trading_day

LIMITS = Limits(
    order_jpy_max=Decimal("500"),
    day_jpy_max=Decimal("2000"),
    day_count_max=50,
)
T0 = datetime(2026, 8, 16, 0, 0, tzinfo=UTC)


def test_order_exactly_at_limit_passes() -> None:
    """单笔恰在上限通过。"""
    gate = LimitGate(LIMITS)
    gate.commit(Decimal("500"), T0)
    assert gate.usage().total_jpy == Decimal("500")
    assert gate.usage().order_count == 1


def test_order_above_limit_rejected() -> None:
    """单笔超上限即拒绝且不记账。"""
    gate = LimitGate(LIMITS)
    with pytest.raises(LimitExceeded, match="单笔"):
        gate.commit(Decimal("500.01"), T0)
    assert gate.usage().order_count == 0


def test_day_total_boundary() -> None:
    """当日累计恰满通过，再加最小额被拒。"""
    gate = LimitGate(LIMITS)
    for _ in range(4):
        gate.commit(Decimal("500"), T0)
    assert gate.usage().total_jpy == Decimal("2000")
    with pytest.raises(LimitExceeded, match="当日累计"):
        gate.commit(Decimal("0.01"), T0)


def test_day_count_boundary() -> None:
    """当日笔数恰满通过，下一笔被拒。"""
    gate = LimitGate(
        Limits(
            order_jpy_max=Decimal("500"),
            day_jpy_max=Decimal("2000"),
            day_count_max=3,
        )
    )
    for _ in range(3):
        gate.commit(Decimal("1"), T0)
    with pytest.raises(LimitExceeded, match="当日笔数"):
        gate.commit(Decimal("1"), T0)


def test_trading_day_boundary_jst_0600() -> None:
    """交易日按 JST 06:00 切换（D-08）。"""
    before = datetime(2026, 8, 15, 20, 59, tzinfo=UTC)
    after = datetime(2026, 8, 15, 21, 0, tzinfo=UTC)
    assert trading_day(before) == date(2026, 8, 15)
    assert trading_day(after) == date(2026, 8, 16)


def test_rollover_resets_day_usage() -> None:
    """跨交易日边界后累计清零。"""
    gate = LimitGate(LIMITS)
    before = datetime(2026, 8, 15, 20, 59, tzinfo=UTC)
    after = datetime(2026, 8, 15, 21, 0, tzinfo=UTC)
    for _ in range(4):
        gate.commit(Decimal("500"), before)
    gate.commit(Decimal("500"), after)
    assert gate.usage().total_jpy == Decimal("500")
    assert gate.usage().order_count == 1
    assert gate.usage().day == date(2026, 8, 16)


def test_tighten_lower_accepted() -> None:
    """限额调低被接受。"""
    gate = LimitGate(LIMITS)
    lower = Limits(
        order_jpy_max=Decimal("100"),
        day_jpy_max=Decimal("500"),
        day_count_max=10,
    )
    gate.tighten(lower)
    assert gate.limits == lower
    with pytest.raises(LimitExceeded, match="单笔"):
        gate.commit(Decimal("101"), T0)


def test_tighten_raise_rejected() -> None:
    """任一维调高即拒绝（X-05）。"""
    gate = LimitGate(LIMITS)
    higher = Limits(
        order_jpy_max=Decimal("500"),
        day_jpy_max=Decimal("2000"),
        day_count_max=51,
    )
    with pytest.raises(LimitAdjustmentRejected, match="只可调低"):
        gate.tighten(higher)
    assert gate.limits == LIMITS


def test_tighten_nonpositive_rejected() -> None:
    """限额调整为零或负即拒绝。"""
    gate = LimitGate(LIMITS)
    zero = Limits(
        order_jpy_max=Decimal("0"),
        day_jpy_max=Decimal("2000"),
        day_count_max=50,
    )
    with pytest.raises(LimitAdjustmentRejected, match="必须为正"):
        gate.tighten(zero)


def test_nonpositive_notional_rejected() -> None:
    """名义金额为零或负即拒绝。"""
    gate = LimitGate(LIMITS)
    with pytest.raises(ValueError, match="必须为正"):
        gate.commit(Decimal("0"), T0)


def test_naive_moment_rejected() -> None:
    """无时区时刻直接拒绝（D-08）。"""
    gate = LimitGate(LIMITS)
    with pytest.raises(ValueError, match="时区"):
        gate.commit(Decimal("1"), datetime(2026, 8, 16, 0, 0))
