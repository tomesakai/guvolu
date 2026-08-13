"""当前盘口快照的确定性派生指标。"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Sequence

from guvolu.domain.models import OrderbookLevel

_BP_FACTOR = Decimal("10000")
_ZERO = Decimal("0")
BAND_WIDTHS_BP = (Decimal("5"), Decimal("10"), Decimal("25"))


@dataclass(frozen=True, slots=True)
class BandDepth:
    """一个 bp 带内的双侧深度。"""

    band_bp: Decimal
    ask_size: Decimal
    bid_size: Decimal
    ask_notional: Decimal
    bid_notional: Decimal
    imbalance_size: Decimal
    imbalance_notional: Decimal


@dataclass(frozen=True, slots=True)
class BookMetrics:
    """最优报价、覆盖与带内深度。"""

    best_ask: Decimal
    best_bid: Decimal
    spread: Decimal
    mid: Decimal
    microprice: Decimal
    ask_coverage_bp: Decimal
    bid_coverage_bp: Decimal
    bands: tuple[BandDepth, ...]


def _imbalance(bid: Decimal, ask: Decimal) -> Decimal:
    total = bid + ask
    return _ZERO if total <= _ZERO else (bid - ask) / total


def _band_depth(
    asks: Sequence[OrderbookLevel],
    bids: Sequence[OrderbookLevel],
    mid: Decimal,
    band_bp: Decimal,
) -> BandDepth:
    distance = mid * band_bp / _BP_FACTOR
    ask_levels = [level for level in asks if level.price - mid <= distance]
    bid_levels = [level for level in bids if mid - level.price <= distance]
    ask_size = sum((level.size for level in ask_levels), _ZERO)
    bid_size = sum((level.size for level in bid_levels), _ZERO)
    ask_notional = sum((level.price * level.size for level in ask_levels), _ZERO)
    bid_notional = sum((level.price * level.size for level in bid_levels), _ZERO)
    return BandDepth(
        band_bp=band_bp,
        ask_size=ask_size,
        bid_size=bid_size,
        ask_notional=ask_notional,
        bid_notional=bid_notional,
        imbalance_size=_imbalance(bid_size, ask_size),
        imbalance_notional=_imbalance(bid_notional, ask_notional),
    )


def snapshot_metrics(
    asks: Sequence[OrderbookLevel], bids: Sequence[OrderbookLevel]
) -> BookMetrics:
    """从已按最优档截取的双侧快照生成指标。"""
    if not asks or not bids:
        raise ValueError("盘口双侧不能为空")
    ordered_asks = tuple(sorted(asks, key=lambda level: level.price))
    ordered_bids = tuple(sorted(bids, key=lambda level: level.price, reverse=True))
    best_ask = ordered_asks[0]
    best_bid = ordered_bids[0]
    spread = best_ask.price - best_bid.price
    mid = (best_ask.price + best_bid.price) / Decimal("2")
    best_total = best_ask.size + best_bid.size
    microprice = (
        mid
        if best_total <= _ZERO
        else (best_ask.price * best_bid.size + best_bid.price * best_ask.size)
        / best_total
    )
    ask_coverage_bp = (ordered_asks[-1].price - mid) * _BP_FACTOR / mid
    bid_coverage_bp = (mid - ordered_bids[-1].price) * _BP_FACTOR / mid
    return BookMetrics(
        best_ask=best_ask.price,
        best_bid=best_bid.price,
        spread=spread,
        mid=mid,
        microprice=microprice,
        ask_coverage_bp=ask_coverage_bp,
        bid_coverage_bp=bid_coverage_bp,
        bands=tuple(
            _band_depth(ordered_asks, ordered_bids, mid, width)
            for width in BAND_WIDTHS_BP
        ),
    )
