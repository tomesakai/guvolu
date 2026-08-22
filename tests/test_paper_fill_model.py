"""paper 成交模型单测：逐档吃单、成本分解与费率来源，全程离线。"""
from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from guvolu.domain.enums import Side
from guvolu.domain.models import SymbolRule
from guvolu.domain.symbols import SpotSymbol
from guvolu.execution.paper_fill_model import (
    FEE_SOURCE_CACHE,
    FEE_SOURCE_FALLBACK,
    FEE_SOURCE_SYMBOLS,
    PUBLIC_ORDERBOOK_BASIS,
    BookLevel,
    BookSnapshot,
    FeeQuote,
    FillModelError,
    InsufficientDepth,
    TakerFeeResolver,
    estimate_taker_fill,
    load_book_snapshot_file,
)

OBSERVED = datetime(2026, 8, 22, 1, 0, tzinfo=UTC)
BTC = SpotSymbol("BTC")


def book() -> BookSnapshot:
    """中间价一百万、价差十基点的盘口。"""
    return BookSnapshot(
        symbol=BTC,
        bids=(
            BookLevel(Decimal("999500"), Decimal("0.0003")),
            BookLevel(Decimal("999000"), Decimal("0.01")),
        ),
        asks=(
            BookLevel(Decimal("1000500"), Decimal("0.0003")),
            BookLevel(Decimal("1001000"), Decimal("0.01")),
        ),
        observed_at=OBSERVED,
        basis=PUBLIC_ORDERBOOK_BASIS,
    )


def fee(bps: str = "5") -> FeeQuote:
    return FeeQuote(bps=Decimal(bps), source=FEE_SOURCE_SYMBOLS, fetched_at=OBSERVED)


def test_buy_walks_depth_and_decomposes_cost() -> None:
    estimate = estimate_taker_fill(
        side=Side.BUY,
        size=Decimal("0.0005"),
        book=book(),
        expected_price=Decimal("1000000"),
        fee=fee(),
    )

    assert estimate.levels_consumed == 2
    assert estimate.notional_jpy == Decimal("500.35")
    assert estimate.model_fill_price == Decimal("1000700")
    assert estimate.fee_jpy == Decimal("500.35") * Decimal("5") / Decimal("10000")
    assert estimate.fee_bps == Decimal("5")
    assert estimate.half_spread_bps == Decimal("5")
    assert estimate.impact_bps == Decimal("2")
    assert estimate.slippage_vs_reference_bps == Decimal("7")
    assert estimate.total_cost_bps == Decimal("12")
    assert estimate.fill_basis == PUBLIC_ORDERBOOK_BASIS
    evidence = estimate.as_evidence()
    assert evidence["model_fill_price"] == "1000700"
    assert set(estimate.cost_record()) == {
        "fee_bps", "half_spread_bps", "impact_bps",
        "slippage_vs_reference_bps", "total_cost_bps",
    }
    assert estimate.fill_record()["side"] == "BUY"


def test_sell_walks_bids_and_adverse_direction_is_positive() -> None:
    estimate = estimate_taker_fill(
        side=Side.SELL,
        size=Decimal("0.0004"),
        book=book(),
        expected_price=Decimal("1000000"),
        fee=fee("0"),
    )

    assert estimate.model_fill_price == Decimal("999375")
    assert estimate.impact_bps == Decimal("1.25")
    assert estimate.slippage_vs_reference_bps == Decimal("6.25")
    assert estimate.total_cost_bps == Decimal("6.25")


def test_insufficient_depth_rejects_instead_of_partial_fill() -> None:
    with pytest.raises(InsufficientDepth):
        estimate_taker_fill(
            side=Side.BUY,
            size=Decimal("1"),
            book=book(),
            expected_price=Decimal("1000000"),
            fee=fee(),
        )


def test_snapshot_rejects_crossed_or_unordered_book() -> None:
    with pytest.raises(FillModelError, match="交叉"):
        BookSnapshot(
            symbol=BTC,
            bids=(BookLevel(Decimal("1000600"), Decimal("1")),),
            asks=(BookLevel(Decimal("1000500"), Decimal("1")),),
            observed_at=OBSERVED,
            basis=PUBLIC_ORDERBOOK_BASIS,
        )
    with pytest.raises(FillModelError, match="升序"):
        BookSnapshot(
            symbol=BTC,
            bids=(BookLevel(Decimal("999500"), Decimal("1")),),
            asks=(
                BookLevel(Decimal("1000500"), Decimal("1")),
                BookLevel(Decimal("1000400"), Decimal("1")),
            ),
            observed_at=OBSERVED,
            basis=PUBLIC_ORDERBOOK_BASIS,
        )


def test_snapshot_metrics_and_file_loading(tmp_path: Path) -> None:
    path = tmp_path / "orderbook.json"
    path.write_text(json.dumps({
        "status": 0,
        "data": {
            "symbol": "BTC",
            "observed_at": OBSERVED.isoformat(),
            "asks": [
                {"price": "1000500", "size": "0.0003"},
                {"price": "1001000", "size": "0.01"},
            ],
            "bids": [
                {"price": "999500", "size": "0.0001"},
                {"price": "999000", "size": "0.01"},
            ],
        },
    }), encoding="utf-8")

    snapshot = load_book_snapshot_file(path, basis=PUBLIC_ORDERBOOK_BASIS)

    assert snapshot.observed_at == OBSERVED
    assert snapshot.mid == Decimal("1000000")
    assert snapshot.spread_bps() == Decimal("10")
    assert snapshot.depth_base(Side.BUY, 5) == Decimal("0.0101")
    assert snapshot.top_imbalance() == Decimal("-0.5")


def _rules(taker: str) -> tuple[SymbolRule, ...]:
    return (
        SymbolRule(
            symbol="BTC",
            min_order_size=Decimal("0.0001"),
            max_order_size=Decimal("5"),
            size_step=Decimal("0.0001"),
            tick_size=Decimal("1"),
            taker_fee=Decimal(taker),
            maker_fee=Decimal("-0.0001"),
        ),
    )


def test_fee_resolver_fetches_caches_and_falls_back(tmp_path: Path) -> None:
    cache = tmp_path / "fee.json"
    resolver = TakerFeeResolver(
        cache, fallback_bps=Decimal("5"), cache_seconds=3600,
    )
    calls: list[int] = []

    def fetch() -> tuple[SymbolRule, ...]:
        calls.append(1)
        return _rules("0.0009")

    def failing() -> tuple[SymbolRule, ...]:
        raise RuntimeError("offline")

    first = resolver.resolve(BTC, fetch)
    second = resolver.resolve(BTC, failing)

    assert first.bps == Decimal("9.0000")
    assert first.source == FEE_SOURCE_SYMBOLS
    assert second.bps == Decimal("9.0000")
    assert second.source == FEE_SOURCE_CACHE
    assert len(calls) == 1

    stale = TakerFeeResolver(
        tmp_path / "absent.json", fallback_bps=Decimal("5"), cache_seconds=0,
    )
    fallback = stale.resolve(BTC, failing)
    assert fallback.bps == Decimal("5")
    assert fallback.source == FEE_SOURCE_FALLBACK
    assert fallback.detail is not None and "offline" in fallback.detail

    missing = resolver.resolve(SpotSymbol("ETH"), lambda: _rules("0.0009"))
    assert missing.source == FEE_SOURCE_FALLBACK
    assert missing.detail == "品种缺失"


def test_fee_cache_expires(tmp_path: Path) -> None:
    cache = tmp_path / "fee.json"
    resolver = TakerFeeResolver(
        cache, fallback_bps=Decimal("5"), cache_seconds=60,
    )
    resolver.resolve(BTC, lambda: _rules("0.0005"))
    payload = json.loads(cache.read_text(encoding="utf-8"))
    payload["BTC"]["fetched_at"] = (
        datetime.now(UTC) - timedelta(seconds=120)
    ).isoformat()
    cache.write_text(json.dumps(payload), encoding="utf-8")

    quote = resolver.resolve(BTC, lambda: _rules("0.0007"))

    assert quote.source == FEE_SOURCE_SYMBOLS
    assert quote.bps == Decimal("7.0000")
