"""来源与品种登记值（multi-source-data-design 第 3、4 节）。

数值均为实测：GMO 与 bitbank 规则、bitbank 日界与上市起点
见 2026-08-08 bitbank 实测快照；bitFlyer 品种面见
2026-08-07 bitFlyer 实测快照。raw_source 指向证据行。
"""
from __future__ import annotations

import sqlite3

from guvolu.data.store import (
    CapabilityRow,
    InstrumentMapRow,
    InstrumentRow,
    VenueRow,
    register_capabilities,
    register_dimensions,
    register_endpoint_revisions,
)
from guvolu.data.source_contract import registered_realtime_endpoint_revisions

# 探测运行完成时刻
OBSERVED_AT = "2026-08-08T09:28:06+00:00"

_RAW_GMO = "2026-08-08/gmo/symbols.jsonl:1"
_RAW_GMO_ARCHIVE = "docs/2026-08-06-gmo-data-scope-survey.md:35-59"
_RAW_BITBANK = "2026-08-08/bitbank/spot_pairs.jsonl:1"
_RAW_BITFLYER = "2026-08-07/bitflyer/markets.jsonl:1"

# GMO 已实测的官方逐笔现货品种。
# 无 ``_JPY`` 后缀即现物市场。
# 已下架品种保留历史映射。
# 未知历史精度正确留空。
GMO_ARCHIVE_SPOT_SYMBOLS: tuple[str, ...] = (
    "ADA", "ASTR", "ATOM", "BAT", "BCH", "BTC", "DAI", "DOGE",
    "DOT", "ENJ", "ETH", "FCR", "LINK", "LTC", "MKR", "MONA",
    "NAC", "OMG", "QTUM", "SOL", "SUI", "WILD", "XEM", "XLM",
    "XRP", "XTZ", "XYM",
)

# 三项分别为价格、数量与最小数量。
# 数值来自 /v1/symbols 原文。
_GMO_CURRENT_SPOT_RULES: dict[str, tuple[str, str, str]] = {
    "ADA": ("0.001", "1", "1"),
    "ASTR": ("0.001", "1", "10"),
    "ATOM": ("1", "0.01", "0.01"),
    "BCH": ("1", "0.001", "0.01"),
    "BTC": ("1", "0.00001", "0.00001"),
    "DOGE": ("0.001", "1", "10"),
    "DOT": ("1", "0.1", "0.1"),
    "ETH": ("1", "0.0001", "0.001"),
    "FCR": ("0.001", "1", "10"),
    "LINK": ("1", "0.1", "0.1"),
    "LTC": ("1", "0.01", "0.1"),
    "NAC": ("0.001", "0.1", "0.1"),
    "SOL": ("1", "0.01", "0.01"),
    "SUI": ("0.001", "0.1", "0.1"),
    "WILD": ("0.001", "1", "1"),
    "XLM": ("0.001", "1", "1"),
    "XRP": ("0.001", "1", "1"),
}

_GMO_CURRENT_LEVERAGE_RULES: dict[str, tuple[str, str, str]] = {
    "ADA_JPY": ("0.001", "10", "10"),
    "ATOM_JPY": ("1", "1", "1"),
    "BCH_JPY": ("1", "0.1", "0.1"),
    "BTC_JPY": ("1", "0.001", "0.001"),
    "DOGE_JPY": ("0.001", "10", "10"),
    "DOT_JPY": ("1", "1", "1"),
    "ETH_JPY": ("1", "0.01", "0.01"),
    "LINK_JPY": ("1", "1", "1"),
    "LTC_JPY": ("1", "1", "1"),
    "SOL_JPY": ("1", "0.1", "0.1"),
    "SUI_JPY": ("0.001", "1", "1"),
    "XRP_JPY": ("0.001", "10", "10"),
}

# 2026-08-08 pairs 原件。
# 元组是 base、tick。
# 后接步长、最小量。
# 别名无证据，不合并。
BITBANK_JPY_RULES: dict[str, tuple[str, str, str, str]] = {
    "btc_jpy": ("BTC", "1", "0.0001", "0.0001"),
    "xrp_jpy": ("XRP", "0.001", "0.0001", "0.0001"),
    "eth_jpy": ("ETH", "1", "0.0001", "0.0001"),
    "sol_jpy": ("SOL", "0.1", "0.0001", "0.0001"),
    "dot_jpy": ("DOT", "0.001", "0.0001", "0.0001"),
    "doge_jpy": ("DOGE", "0.001", "0.0001", "0.0001"),
    "ltc_jpy": ("LTC", "0.1", "0.0001", "0.0001"),
    "bcc_jpy": ("BCC", "1", "0.0001", "0.0001"),
    "mona_jpy": ("MONA", "0.001", "0.0001", "0.0001"),
    "xlm_jpy": ("XLM", "0.001", "0.0001", "0.0001"),
    "qtum_jpy": ("QTUM", "0.001", "0.0001", "0.0001"),
    "bat_jpy": ("BAT", "0.001", "0.0001", "0.0001"),
    "omg_jpy": ("OMG", "0.001", "0.0001", "0.0001"),
    "xym_jpy": ("XYM", "0.001", "0.0001", "0.0001"),
    "link_jpy": ("LINK", "0.001", "0.0001", "0.0001"),
    "mkr_jpy": ("MKR", "1", "0.0001", "0.0001"),
    "boba_jpy": ("BOBA", "0.001", "0.0001", "0.0001"),
    "enj_jpy": ("ENJ", "0.001", "0.0001", "0.0001"),
    "astr_jpy": ("ASTR", "0.001", "0.0001", "0.0001"),
    "ada_jpy": ("ADA", "0.001", "0.0001", "0.0001"),
    "avax_jpy": ("AVAX", "0.001", "0.0001", "0.0001"),
    "axs_jpy": ("AXS", "0.001", "0.0001", "0.0001"),
    "flr_jpy": ("FLR", "0.001", "0.0001", "0.0001"),
    "sand_jpy": ("SAND", "0.001", "0.0001", "0.0001"),
    "gala_jpy": ("GALA", "0.001", "0.0001", "0.0001"),
    "chz_jpy": ("CHZ", "0.001", "0.0001", "0.0001"),
    "ape_jpy": ("APE", "0.001", "0.0001", "0.0001"),
    "oas_jpy": ("OAS", "0.001", "0.0001", "0.0001"),
    "mana_jpy": ("MANA", "0.001", "0.0001", "0.0001"),
    "grt_jpy": ("GRT", "0.001", "0.0001", "0.0001"),
    "rndr_jpy": ("RNDR", "0.001", "0.0001", "0.0001"),
    "bnb_jpy": ("BNB", "1", "0.0001", "0.0001"),
    "dai_jpy": ("DAI", "0.001", "0.0001", "0.0001"),
    "op_jpy": ("OP", "0.001", "0.0001", "0.0001"),
    "arb_jpy": ("ARB", "0.001", "0.0001", "0.0001"),
    "klay_jpy": ("KLAY", "0.0001", "0.0001", "0.0001"),
    "imx_jpy": ("IMX", "0.001", "0.0001", "0.0001"),
    "mask_jpy": ("MASK", "0.001", "0.0001", "0.0001"),
    "pol_jpy": ("POL", "0.001", "0.0001", "0.0001"),
    "cyber_jpy": ("CYBER", "0.01", "0.0001", "0.0001"),
    "render_jpy": ("RENDER", "0.001", "0.0001", "0.0001"),
    "trx_jpy": ("TRX", "0.0001", "0.0001", "0.0001"),
    "lpt_jpy": ("LPT", "0.01", "0.0001", "0.0001"),
    "atom_jpy": ("ATOM", "0.001", "0.0001", "0.0001"),
    "sui_jpy": ("SUI", "0.001", "0.00000001", "0.0001"),
    "sky_jpy": ("SKY", "0.0001", "0.00000001", "0.0001"),
    "matic_jpy": ("MATIC", "0.001", "0.0001", "0.0001"),
}

BITFLYER_JPY_SPOTS: tuple[str, ...] = (
    "BTC_JPY", "ETH_JPY", "XRP_JPY", "XLM_JPY", "MONA_JPY", "ELF_JPY",
)

VENUE_ROWS: list[VenueRow] = [
    ("gmo", "exchange", "execution", "毫秒", "JST06", 1),
    ("bitbank", "exchange", "market", "毫秒", "UTC00", 0),
    ("bitflyer", "exchange", "market", "毫秒", "UTC00", 0),
    ("coincheck", "exchange", "market", "mixed", "UTC00", 0),
    ("binance", "exchange", "reference", "mixed", "UTC00", 0),
    ("kraken", "exchange", "reference", "mixed", "UTC00", 0),
    ("okx", "exchange", "reference", "mixed", "UTC00", 0),
    ("bybit", "exchange", "reference", "mixed", "UTC00", 0),
    ("coinbase", "exchange", "reference", "mixed", "UTC00", 0),
    ("hyperliquid", "exchange", "reference", "mixed", "UTC00", 0),
    ("bitfinex", "exchange", "reference", "mixed", "UTC00", 0),
    ("bitstamp", "exchange", "reference", "mixed", "UTC00", 0),
]

_JPY_SPOT_BASES = sorted({
    *GMO_ARCHIVE_SPOT_SYMBOLS,
    *(rules[0] for rules in BITBANK_JPY_RULES.values()),
    *(symbol.removesuffix("_JPY") for symbol in BITFLYER_JPY_SPOTS),
})

INSTRUMENT_ROWS: list[InstrumentRow] = [
    *[
        (f"SPOT:{symbol}/JPY", symbol, "JPY", "spot")
        for symbol in _JPY_SPOT_BASES
    ],
    *[
        (f"LEVERAGE:{symbol.removesuffix('_JPY')}/JPY",
         symbol.removesuffix("_JPY"), "JPY", "leverage")
        for symbol in _GMO_CURRENT_LEVERAGE_RULES
    ],
    ("SPOT:BTC/USDT", "BTC", "USDT", "spot"),
]

# bitbank 档位由位数字段换算
INSTRUMENT_MAP_ROWS: list[InstrumentMapRow] = [
    *[
        (
            "gmo",
            symbol,
            f"SPOT:{symbol}/JPY",
            *(_GMO_CURRENT_SPOT_RULES.get(symbol, (None, None, None))),
            0,
            OBSERVED_AT,
            _RAW_GMO if symbol in _GMO_CURRENT_SPOT_RULES
            else _RAW_GMO_ARCHIVE,
        )
        for symbol in GMO_ARCHIVE_SPOT_SYMBOLS
    ],
    *[
        (
            "gmo",
            symbol,
            f"LEVERAGE:{symbol.removesuffix('_JPY')}/JPY",
            *rules,
            0,
            OBSERVED_AT,
            _RAW_GMO,
        )
        for symbol, rules in _GMO_CURRENT_LEVERAGE_RULES.items()
    ],
    *[
        (
            "bitbank", pair, f"SPOT:{rules[0]}/JPY",
            rules[1], rules[2], rules[3], 0, OBSERVED_AT, _RAW_BITBANK,
        )
        for pair, rules in BITBANK_JPY_RULES.items()
    ],
    *[
        (
            "bitflyer", symbol,
            f"SPOT:{symbol.removesuffix('_JPY')}/JPY",
            None, None, None, 0, OBSERVED_AT, _RAW_BITFLYER,
        )
        for symbol in BITFLYER_JPY_SPOTS
    ],
    ("bitflyer", "FX_BTC_JPY", "LEVERAGE:BTC/JPY", None, None, None,
     0, OBSERVED_AT, _RAW_BITFLYER),
    ("coincheck", "btc_jpy", "SPOT:BTC/JPY", None, None, None,
     0, OBSERVED_AT, "official/coincheck-api"),
    ("coincheck", "eth_jpy", "SPOT:ETH/JPY", None, None, None,
     0, OBSERVED_AT, "official/coincheck-api"),
    ("coincheck", "xrp_jpy", "SPOT:XRP/JPY", None, None, None,
     0, OBSERVED_AT, "official/coincheck-api"),
    ("binance", "BTCUSDT", "SPOT:BTC/USDT", None, None, None,
     0, OBSERVED_AT, "official/binance-public-data"),
    ("okx", "BTC-USDT", "SPOT:BTC/USDT", "0.1", "0.00000001", "0.00001",
     0, "2026-08-11T10:00:00+00:00", "official/okx-public-instruments"),
]

# 逐笔端点最早可得日，二分探测实测
BITBANK_LISTING_START: dict[str, str] = {
    "btc_jpy": "20170213",
    "eth_jpy": "20200524",
    "xrp_jpy": "20170525",
}

# 能力证据有效九十日
_SURVEYED = "2026-08-10T00:00:00+00:00"
_VALID_UNTIL = "2026-11-08T00:00:00+00:00"
_VALIDATION = "docs/2026-08-10-multi-source-api-validation.md"


def _cap(
    venue: str,
    domain: str,
    endpoint: str,
    available: int,
    access: str,
    backfill: str,
    fidelity: str,
    integrity: str,
    rate: str,
    unit: str,
    evidence: str,
    implemented: str,
    uri: str = _VALIDATION,
    revision_id: int = 0,
) -> CapabilityRow:
    """装配能力证据版本行。"""
    return (
        venue, domain, endpoint, revision_id, available, access, backfill, fidelity,
        integrity, rate, unit, evidence, implemented, uri, _SURVEYED,
        _VALID_UNTIL, 1,
    )


CAPABILITY_ROWS: list[CapabilityRow] = [
    _cap("gmo", "kline", "/v1/klines", 1, "public", "archive", "none", "none", "fixed", "milliseconds", "measured", "implemented"),
    _cap("gmo", "trade", "trades/archive", 1, "public", "archive", "none", "none", "fixed", "milliseconds", "measured", "implemented"),
    _cap("gmo", "book_realtime", "orderbooks/ws", 1, "public", "none", "snapshot_l2", "snapshot", "fixed", "milliseconds", "measured", "implemented"),
    _cap("gmo", "book_history", "none", 0, "public", "none", "none", "none", "unknown", "unknown", "measured", "blocked"),
    _cap("bitbank", "kline", "candlestick", 1, "public", "archive", "none", "none", "fixed", "milliseconds", "measured", "implemented"),
    _cap("bitbank", "trade", "transactions/{day}", 1, "public", "archive", "none", "none", "fixed", "milliseconds", "measured", "implemented"),
    _cap("bitbank", "book_realtime", "depth_whole/depth_diff", 1, "public", "none", "delta_l2", "monotonic", "fixed", "milliseconds", "documented", "planned"),
    _cap("bitbank", "book_history", "none", 0, "public", "none", "none", "none", "unknown", "unknown", "measured", "blocked"),
    _cap("bitflyer", "kline", "none", 0, "public", "none", "none", "none", "fixed", "unknown", "measured", "blocked"),
    _cap("bitflyer", "trade", "/v1/executions", 1, "public", "window", "none", "none", "fixed", "iso8601", "measured", "implemented"),
    _cap("bitflyer", "book_realtime", "board_snapshot/board", 1, "public", "none", "delta_l2", "snapshot", "fixed", "local", "measured", "planned"),
    _cap("bitflyer", "book_history", "none", 0, "public", "none", "none", "none", "unknown", "unknown", "measured", "blocked"),
    _cap("coincheck", "kline", "none", 0, "public", "none", "none", "none", "unknown", "unknown", "documented", "blocked"),
    _cap("coincheck", "trade", "/api/trades", 1, "public", "unknown", "none", "none", "fixed", "mixed", "documented", "planned"),
    _cap("coincheck", "book_realtime", "{pair}-orderbook", 1, "public", "none", "delta_l2", "none", "fixed", "mixed", "documented", "planned"),
    _cap("coincheck", "book_history", "none", 0, "public", "none", "none", "none", "unknown", "unknown", "documented", "blocked"),
    _cap("binance", "kline", "data.binance.vision/klines", 1, "public", "archive", "none", "checksum", "weight", "mixed", "documented", "planned"),
    _cap("binance", "trade", "data.binance.vision/trades", 1, "public", "archive", "none", "checksum", "weight", "mixed", "documented", "planned"),
    _cap("binance", "book_realtime", "@depth", 1, "public", "none", "delta_l2", "sequence", "weight", "milliseconds", "documented", "planned"),
    _cap("binance", "book_history", "futures/*/daily/bookDepth", 1, "public", "archive", "aggregate", "checksum", "weight", "mixed", "documented", "planned"),
    _cap("kraken", "kline", "/0/public/OHLC", 1, "public", "window", "none", "none", "counter", "seconds", "documented", "planned"),
    _cap("kraken", "trade", "/0/public/Trades", 1, "public", "cursor", "none", "none", "counter", "nanoseconds", "documented", "planned"),
    _cap("kraken", "book_realtime", "book", 1, "public", "none", "delta_l2", "checksum", "counter", "seconds", "documented", "planned"),
    _cap("kraken", "book_l3", "ws-v2/level3", 1, "token", "snapshot", "depth_bounded_l3", "crc32_top10_no_sequence", "subscription_limit", "iso8601", "documented", "planned", "https://docs.kraken.com/api/docs/websocket-v2/level3/"),
    _cap("kraken", "book_history", "none", 0, "public", "none", "none", "none", "unknown", "unknown", "documented", "blocked"),
    _cap("okx", "kline", "history-candles", 1, "public", "cursor", "none", "none", "window", "milliseconds", "documented", "planned"),
    _cap("okx", "trade", "history-trades", 1, "public", "cursor", "none", "none", "window", "milliseconds", "documented", "planned"),
    _cap("okx", "book_realtime", "books", 1, "public", "none", "delta_l2", "sequence", "window", "milliseconds", "documented", "planned"),
    _cap("okx", "book_realtime", "books", 1, "public", "none", "snapshot_native_prev_seq_delta_l2_400", "native_prev_seq+checksum_unsupported_after_2026-06-23", "window", "milliseconds", "measured", "implemented", "https://www.okx.com/docs-v5/en/", 1),
    _cap("okx", "book_history", "historical-data/order-book", 1, "public", "archive", "snapshot_l2", "unknown", "window", "mixed", "documented", "planned"),
    _cap("okx", "book_history", "historical-data/order-book", 1, "public", "archive", "periodic_snapshot_absolute_delta", "archive_sha256+strict_ts+periodic_snapshot", "window", "milliseconds", "measured", "implemented", "docs/2026-08-11-okx-l2-sample-validation.md", 1),
    _cap("bybit", "kline", "/v5/market/kline", 1, "public", "window", "none", "none", "fixed", "milliseconds", "documented", "planned"),
    _cap("bybit", "trade", "history-data/trades", 1, "public", "archive", "none", "unknown", "fixed", "mixed", "documented", "planned"),
    _cap("bybit", "book_realtime", "orderbook.{depth}", 1, "public", "none", "delta_l2", "sequence", "fixed", "milliseconds", "documented", "planned"),
    _cap("bybit", "book_history", "history-data/orderbook", 1, "public", "archive", "snapshot_l2", "unknown", "fixed", "mixed", "documented", "planned"),
    _cap("bybit", "book_history", "history-data/orderbook", 0, "public", "none", "none", "none", "fixed", "unknown", "unverified", "blocked", "https://public.bybit.com/", 1),
    _cap("coinbase", "kline", "/products/{id}/candles", 1, "public", "cursor", "none", "none", "fixed", "iso8601", "documented", "planned"),
    _cap("coinbase", "trade", "/products/{id}/trades", 1, "public", "cursor", "none", "none", "fixed", "iso8601", "documented", "planned"),
    _cap("coinbase", "book_realtime", "level2", 1, "public", "none", "delta_l2", "sequence", "fixed", "iso8601", "documented", "planned"),
    _cap("coinbase", "book_l3", "ws/full+rest/book?level=3", 1, "credential", "snapshot", "full_l3", "global_sequence", "fixed", "iso8601", "documented", "planned", "https://docs.cdp.coinbase.com/exchange/websocket-feed/channels"),
    _cap("coinbase", "book_history", "none", 0, "public", "none", "none", "none", "unknown", "unknown", "documented", "blocked"),
    _cap("hyperliquid", "kline", "candleSnapshot", 1, "public", "unknown", "none", "none", "weight", "milliseconds", "documented", "planned"),
    _cap("hyperliquid", "trade", "trades", 1, "public", "unknown", "none", "none", "weight", "milliseconds", "unverified", "blocked"),
    _cap("hyperliquid", "book_realtime", "l2Book", 1, "public", "none", "snapshot_l2", "snapshot", "weight", "milliseconds", "documented", "planned"),
    _cap("hyperliquid", "book_history", "none", 0, "public", "unknown", "unknown", "none", "weight", "unknown", "unverified", "blocked"),
    _cap("bitfinex", "book_l3", "ws-v2/book?prec=R0", 1, "public", "snapshot", "truncated_l3_250_orders", "optional_sequence_checksum", "fixed", "mixed", "documented", "planned", "https://docs.bitfinex.com/reference/ws-public-raw-books"),
    _cap("bitstamp", "book_l3", "order_book/{market}?group=2", 0, "public", "snapshot", "l3_candidate_unverified", "unverified_recovery", "fixed", "mixed", "unverified", "blocked", "https://www.bitstamp.net/api/"),
    _cap("bitbank", "book_realtime", "depth_whole/depth_diff", 1, "public", "none", "delta_l2", "monotonic", "fixed", "milliseconds", "documented", "implemented", "https://github.com/bitbankinc/bitbank-api-docs/blob/master/public-stream.md", 1),
    _cap("bitbank", "market_status", "circuit_break_info", 1, "public", "none", "event_state", "none", "event", "milliseconds", "documented", "implemented", "https://github.com/bitbankinc/bitbank-api-docs/blob/master/public-stream.md", 0),
    _cap("bitflyer", "book_realtime", "board_snapshot/board", 1, "public", "none", "delta_l2", "snapshot", "fixed", "local", "measured", "implemented", "https://bf-lightning-api.readme.io/docs/realtime-api", 1),
    _cap("gmo", "book_l2_anchor", "/v1/orderbooks", 1, "public", "none", "snapshot_l2", "snapshot", "fixed", "iso8601", "documented", "implemented", "https://api.coin.z.com/docs/en/", 0),
    _cap("bitbank", "book_l2_anchor", "/{pair}/depth", 1, "public", "none", "snapshot_l2", "monotonic", "fixed", "milliseconds", "documented", "implemented", "https://github.com/bitbankinc/bitbank-api-docs/blob/master/public-api.md", 0),
    _cap("bitflyer", "book_l2_anchor", "/v1/getboard", 1, "public", "none", "snapshot_l2", "none", "fixed", "local", "documented", "implemented", "https://lightning.bitflyer.com/docs?lang=en", 0),
    _cap("gmo", "trade_realtime", "trades/ws", 1, "public", "none", "match", "none", "fixed", "iso8601", "measured", "implemented", "https://api.coin.z.com/docs/#public-ws-trades", 0),
    _cap("bitbank", "trade_realtime", "transactions", 1, "public", "none", "match", "none", "fixed", "milliseconds", "documented", "implemented", "https://github.com/bitbankinc/bitbank-api-docs/blob/master/public-stream.md", 0),
    _cap("bitflyer", "trade_realtime", "lightning_executions", 1, "public", "none", "match", "none", "fixed", "iso8601", "measured", "implemented", "https://bf-lightning-api.readme.io/docs/realtime-api", 0),
    _cap("coincheck", "trade", "/api/trades", 1, "public", "unknown", "none", "none", "fixed", "mixed", "documented", "implemented", "https://coincheck.com/documents/exchange/api", 1),
    _cap("coincheck", "book_realtime", "{pair}-orderbook", 1, "public", "none", "delta_l2", "none", "fixed", "mixed", "documented", "implemented", "https://coincheck.com/documents/exchange/api", 1),
    _cap("binance", "trade", "data.binance.vision/aggTrades", 1, "public", "archive", "aggregate", "checksum", "weight", "mixed", "documented", "implemented", "https://github.com/binance/binance-public-data", 1),
]


def register_all(conn: sqlite3.Connection) -> int:
    """登记全部维度行，返回新增行数。"""
    changed = register_dimensions(
        conn, VENUE_ROWS, INSTRUMENT_ROWS, INSTRUMENT_MAP_ROWS
    )
    changed += register_capabilities(conn, CAPABILITY_ROWS)
    return changed + register_endpoint_revisions(
        conn, registered_realtime_endpoint_revisions()
    )
