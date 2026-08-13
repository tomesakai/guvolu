"""多来源能力、归一化与回补门槛单测。"""
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

from guvolu.data.audit import (
    CoverageRequirement,
    audit_capabilities,
    audit_coverage,
    minimum_launch_readiness,
)
from guvolu.data.normalization import (
    NormalizationContext,
    NormalizationError,
    UnverifiedMappingError,
    normalize_book_top,
    normalize_trade,
)
from guvolu.data.store import (
    connect,
    insert_book_tops,
    insert_trade_ticks,
    upsert_coverage,
)
from guvolu.venues.registry import CAPABILITY_ROWS, register_all


def _context(venue: str, unit: str = "milliseconds") -> NormalizationContext:
    return NormalizationContext(
        venue_id=venue,
        instrument_id="SPOT:BTC/USD",
        endpoint="trades",
        ingest_time="2026-08-10T03:00:01+00:00",
        raw_source=f"raw/2026-08-10/{venue}/trades.jsonl:1",
        raw_item_index=0,
        timestamp_unit=unit,
    )


def test_capability_registry_is_complete_and_honest(tmp_path: Path) -> None:
    """现行来源四域齐备，L3 候选不冒充已接入。"""
    conn = connect(tmp_path)
    register_all(conn)
    assert conn.execute("SELECT COUNT(*) FROM venue").fetchone()[0] == 12
    assert conn.execute(
        "SELECT COUNT(*) FROM venue_capability_revision"
    ).fetchone()[0] == len(CAPABILITY_ROWS)
    report = audit_capabilities(
        conn,
        ["gmo", "bitbank"],
        now=datetime(2026, 8, 10, tzinfo=UTC),
    )
    assert report["missing"] == []
    assert report["stale"] == []
    assert report["unverified"] == []
    assert "bitbank:book_realtime" not in report["implementation_gaps"]
    l3 = conn.execute(
        "SELECT venue_id,replay_fidelity,implementation_status "
        "FROM venue_capability_revision WHERE domain='book_l3' "
        "ORDER BY venue_id"
    ).fetchall()
    assert l3 == [
        ("bitfinex", "truncated_l3_250_orders", "planned"),
        ("bitstamp", "l3_candidate_unverified", "blocked"),
        ("coinbase", "full_l3", "planned"),
        ("kraken", "depth_bounded_l3", "planned"),
    ]
    conn.close()


def test_archive_market_registry_keeps_unknown_history_rules_null(
    tmp_path: Path,
) -> None:
    """GMO 27 个现货归档全映射，历史未知精度不猜，FX 独立。"""
    conn = connect(tmp_path)
    register_all(conn)
    gmo = conn.execute(
        "SELECT venue_symbol, tick_size, size_step, min_size "
        "FROM instrument_map WHERE venue_id='gmo' "
        "AND instrument_id LIKE 'SPOT:%' ORDER BY venue_symbol"
    ).fetchall()
    assert len(gmo) == 27
    assert sum(row[1] is not None for row in gmo) == 17
    assert {
        row[0] for row in gmo if row[1] is None
    } == {"BAT", "DAI", "ENJ", "MKR", "MONA", "OMG", "QTUM", "XEM", "XTZ", "XYM"}
    assert conn.execute(
        "SELECT instrument_id FROM instrument_map "
        "WHERE venue_id='bitflyer' AND venue_symbol='FX_BTC_JPY'"
    ).fetchone() == ("LEVERAGE:BTC/JPY",)
    assert conn.execute(
        "SELECT COUNT(*) FROM instrument_map WHERE venue_id='bitbank' "
        "AND venue_symbol LIKE '%_jpy'"
    ).fetchone()[0] == 47
    assert conn.execute(
        "SELECT COUNT(*) FROM instrument_map WHERE venue_id='bitflyer' "
        "AND instrument_id LIKE 'SPOT:%/JPY'"
    ).fetchone()[0] == 6
    conn.close()


def test_verified_trade_mappings_and_storage(tmp_path: Path) -> None:
    """侧别、粒度与首尾标识正确富化落库。"""
    bitbank = normalize_trade(
        {
            "transaction_id": 7,
            "executed_at": 1786330800000,
            "side": "buy",
            "price": "61234.50",
            "amount": "0.001",
        },
        _context("bitbank"),
    )
    assert bitbank.side == "buy"
    assert bitbank.match_granularity == "match"
    binance = normalize_trade(
        {
            "a": 20,
            "p": "61234.5",
            "q": "0.002",
            "f": 25,
            "l": 27,
            "T": 1786330800000,
            "m": True,
        },
        _context("binance"),
    )
    assert binance.side == "sell"
    assert binance.sequence_id is None
    assert (binance.first_trade_id, binance.last_trade_id) == ("25", "27")
    coinbase = normalize_trade(
        {
            "trade_id": 99,
            "price": "61200",
            "size": "0.1",
            "side": "buy",
            "time": "2026-08-10T03:00:00Z",
        },
        _context("coinbase", "iso8601"),
    )
    assert coinbase.side == "sell"
    assert coinbase.source_side_basis == "maker"
    conn = connect(tmp_path)
    assert insert_trade_ticks(
        conn, [bitbank.as_row(), binance.as_row(), coinbase.as_row()]
    ) == 3
    assert insert_trade_ticks(conn, [binance.as_row()]) == 0
    conn.close()


def test_unverified_and_float_inputs_fail_closed() -> None:
    """未核映射与浮点金额不得猜测。"""
    with pytest.raises(UnverifiedMappingError):
        normalize_trade({"px": "1"}, _context("hyperliquid"))
    with pytest.raises(NormalizationError, match="浮点"):
        normalize_trade(
            {
                "transaction_id": 1,
                "executed_at": 1786330800000,
                "side": "buy",
                "price": 1.2,
                "amount": "1",
            },
            _context("bitbank"),
        )


def test_gmo_zero_size_print_keeps_price_lineage() -> None:
    """GMO 零量打印保留为事实，供价格路径重建。"""
    trade = normalize_trade(
        {
            "price": "61234.5",
            "size": "0.0000",
            "side": "buy",
            "timestamp": "2026-08-10T03:00:00.000+00:00",
        },
        _context("gmo", "iso8601"),
    )
    assert trade.size == "0.0000"


def test_coincheck_taker_trade_mapping() -> None:
    """Coincheck 公共逐笔保留原生成交号与吃单侧。"""
    trade = normalize_trade(
        {
            "timestamp": "1786330800",
            "trade_id": "88",
            "rate": "61234.5",
            "amount": "0.001",
            "side": "sell",
        },
        _context("coincheck", "seconds"),
    )
    assert trade.venue_trade_id == "88"
    assert trade.side == "sell"
    assert trade.match_granularity == "match"


def test_book_frame_id_avoids_same_time_collision(tmp_path: Path) -> None:
    """同刻多帧按血缘位次保持唯一。"""
    first_context = _context("okx")
    second_context = replace(first_context, raw_item_index=1)
    arguments = {
        "event_time": 1786330800000,
        "bid": "100",
        "bid_size": "2",
        "ask": "101",
        "ask_size": "3",
        "depth_levels": 1,
        "source_depth_levels": 400,
        "sequence_id": "8",
    }
    first = normalize_book_top(context=first_context, **arguments)
    second = normalize_book_top(context=second_context, **arguments)
    assert first.frame_id != second.frame_id
    conn = connect(tmp_path)
    assert insert_book_tops(conn, [first.as_row(), second.as_row()]) == 2
    conn.close()


def test_minimum_launch_gate_separates_missing_and_empty(tmp_path: Path) -> None:
    """未登记日阻断，允许空日不冒充缺失。"""
    conn = connect(tmp_path)
    register_all(conn)
    upsert_coverage(
        conn,
        [
            (
                "bitbank",
                "btc_jpy",
                "trade",
                "20260801",
                10,
                "t1",
                "t2",
                "ok",
                "2026-08-10T00:00:00+00:00",
            ),
            (
                "bitbank",
                "btc_jpy",
                "trade",
                "20260802",
                0,
                None,
                None,
                "empty",
                "2026-08-10T00:00:00+00:00",
            ),
        ],
    )
    requirement = CoverageRequirement(
        "bitbank", "btc_jpy", "trade", "2026-08-01", "2026-08-03"
    )
    coverage = audit_coverage(conn, [requirement])
    row = coverage["requirements"][0]
    assert row["empty"] == ["20260802"]
    assert row["unregistered"] == ["20260803"]
    assert coverage["ready"] is False
    readiness = minimum_launch_readiness(
        conn,
        ["bitbank"],
        [requirement],
        now=datetime(2026, 8, 10, tzinfo=UTC),
    )
    assert readiness["blockers"] == ["archive_coverage"]
    implementation = minimum_launch_readiness(
        conn,
        ["bitbank"],
        [],
        now=datetime(2026, 8, 10, tzinfo=UTC),
        required_implementations=["bitbank:book_realtime"],
    )
    assert implementation["blockers"] == []
    conn.close()
