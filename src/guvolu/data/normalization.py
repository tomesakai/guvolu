"""多来源行情归一化。

只实现已核证字段语义；未核来源直接拒绝。数值保持十进制文本，
时间单位由端点能力显式传入，不按数量级猜测。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Mapping

from guvolu.data.store import BookTopRow, TradeTickRow
from guvolu.domain.ids import sha256_hex

NORMALIZATION_VERSION = "trade-normalization-v1"
BINANCE_AGGTRADE_NORMALIZATION_VERSION = (
    "binance-aggtrade-normalization-v2"
)
NORMALIZED_SCHEMA_VERSION = 1


class NormalizationError(ValueError):
    """输入违反已核证归一化合同。"""


class UnverifiedMappingError(NormalizationError):
    """来源字段语义尚未核证。"""


def trade_normalization_version(venue_id: str) -> str:
    """返回来源对应的成交规范化版本。"""
    if venue_id == "binance":
        return BINANCE_AGGTRADE_NORMALIZATION_VERSION
    return NORMALIZATION_VERSION


@dataclass(frozen=True, slots=True)
class NormalizationContext:
    """原始项目与端点语义。"""

    venue_id: str
    instrument_id: str
    endpoint: str
    ingest_time: str
    raw_source: str
    raw_item_index: int
    timestamp_unit: str
    available_time: str | None = None


@dataclass(frozen=True, slots=True)
class NormalizedTrade:
    """规范化逐笔事实。"""

    venue_id: str
    instrument_id: str
    venue_trade_id: str
    event_time: str
    available_time: str
    ingest_time: str
    side: str
    source_side_basis: str
    price: str
    size: str
    match_granularity: str
    id_origin: str
    sequence_id: str | None
    first_trade_id: str | None
    last_trade_id: str | None
    time_origin: str
    normalization_version: str
    schema_version: int
    revision_id: int
    raw_item_index: int
    raw_source: str

    def as_row(self) -> TradeTickRow:
        """转数据库行。"""
        return (
            self.venue_id,
            self.instrument_id,
            self.venue_trade_id,
            self.event_time,
            self.available_time,
            self.ingest_time,
            self.side,
            self.source_side_basis,
            self.price,
            self.size,
            self.match_granularity,
            self.id_origin,
            self.sequence_id,
            self.first_trade_id,
            self.last_trade_id,
            self.time_origin,
            self.normalization_version,
            self.schema_version,
            self.revision_id,
            self.raw_item_index,
            self.raw_source,
        )


@dataclass(frozen=True, slots=True)
class NormalizedBookTop:
    """规范化盘口顶档帧。"""

    venue_id: str
    instrument_id: str
    frame_id: str
    event_time: str
    available_time: str
    ingest_time: str
    bid: str
    bid_size: str
    ask: str
    ask_size: str
    depth_levels: int
    source_depth_levels: int
    time_origin: str
    sequence_id: str | None
    normalization_version: str
    schema_version: int
    raw_item_index: int
    raw_source: str

    def as_row(self) -> BookTopRow:
        """转数据库行。"""
        return (
            self.venue_id,
            self.instrument_id,
            self.frame_id,
            self.event_time,
            self.available_time,
            self.ingest_time,
            self.bid,
            self.bid_size,
            self.ask,
            self.ask_size,
            self.depth_levels,
            self.source_depth_levels,
            self.time_origin,
            self.sequence_id,
            self.normalization_version,
            self.schema_version,
            self.raw_item_index,
            self.raw_source,
        )


def _required(payload: Mapping[str, object], key: str) -> object:
    value = payload.get(key)
    if value is None or value == "":
        raise NormalizationError(f"缺少字段 {key}")
    return value


def _decimal_text(value: object, key: str, *, allow_zero: bool = False) -> str:
    if isinstance(value, bool) or isinstance(value, float):
        raise NormalizationError(f"{key} 不得用浮点")
    if not isinstance(value, (str, int, Decimal)):
        raise NormalizationError(f"{key} 类型非法")
    try:
        number = Decimal(str(value))
    except InvalidOperation as exc:
        raise NormalizationError(f"{key} 不是十进制数") from exc
    if not number.is_finite() or number < 0 or (number == 0 and not allow_zero):
        raise NormalizationError(f"{key} 必须为正数")
    return format(number, "f")


def _iso_time(value: object, unit: str) -> str:
    if unit == "iso8601":
        if not isinstance(value, str):
            raise NormalizationError("ISO 时刻必须为文本")
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise NormalizationError("ISO 时刻非法") from exc
        if parsed.tzinfo is None:
            raise NormalizationError("ISO 时刻缺时区")
        return parsed.astimezone(UTC).isoformat()
    divisors = {
        "seconds": Decimal("1"),
        "milliseconds": Decimal("1000"),
        "microseconds": Decimal("1000000"),
        "nanoseconds": Decimal("1000000000"),
    }
    divisor = divisors.get(unit)
    if divisor is None or isinstance(value, bool) or isinstance(value, float):
        raise NormalizationError("时间单位未核或值为浮点")
    try:
        micros = Decimal(str(value)) * Decimal("1000000") / divisor
    except InvalidOperation as exc:
        raise NormalizationError("时间戳非法") from exc
    if micros != micros.to_integral_value():
        raise NormalizationError("时间戳精度超出微秒")
    total_micros = int(micros)
    seconds, microseconds = divmod(total_micros, 1_000_000)
    return datetime.fromtimestamp(seconds, UTC).replace(
        microsecond=microseconds
    ).isoformat()


def _side(value: object, *, invert: bool = False) -> str:
    text = str(value).lower()
    aliases = {"buy": "buy", "b": "buy", "sell": "sell", "s": "sell"}
    found = aliases.get(text)
    if found is None:
        raise NormalizationError("侧别非法")
    if invert:
        return "sell" if found == "buy" else "buy"
    return found


def _validate_context(context: NormalizationContext) -> str:
    if not context.raw_source or context.raw_item_index < 0:
        raise NormalizationError("原始血缘不完整")
    return _iso_time(context.ingest_time, "iso8601")


def _available_time(context: NormalizationContext, ingest: str) -> str:
    """取显式发布时刻，缺省为本地摄取。"""
    if context.available_time is None:
        return ingest
    return _iso_time(context.available_time, "iso8601")


def normalize_trade(
    payload: Mapping[str, object], context: NormalizationContext
) -> NormalizedTrade:
    """按已核证来源映射归一成交。"""
    ingest_time = _validate_context(context)
    available_time = _available_time(context, ingest_time)
    venue = context.venue_id
    first_id: str | None = None
    last_id: str | None = None
    sequence_id: str | None = None
    id_origin = "venue"
    granularity = "match"
    side_basis = "taker"
    normalization_version = trade_normalization_version(venue)
    revision_id = 0

    if venue == "gmo":
        price = _decimal_text(_required(payload, "price"), "price")
        size = _decimal_text(
            _required(payload, "size"), "size", allow_zero=True
        )
        side = _side(_required(payload, "side"))
        event = _iso_time(_required(payload, "timestamp"), context.timestamp_unit)
        seed = "|".join(
            (venue, context.instrument_id, event, price, size, side,
             str(context.raw_item_index))
        )
        trade_id = sha256_hex(seed.encode("utf-8"))
        id_origin = "synthetic"
    elif venue == "bitbank":
        price = _decimal_text(_required(payload, "price"), "price")
        size = _decimal_text(_required(payload, "amount"), "amount")
        side = _side(_required(payload, "side"))
        event = _iso_time(_required(payload, "executed_at"), context.timestamp_unit)
        trade_id = str(_required(payload, "transaction_id"))
    elif venue == "bitflyer":
        price = _decimal_text(_required(payload, "price"), "price")
        size = _decimal_text(_required(payload, "size"), "size")
        side = _side(_required(payload, "side"))
        event = _iso_time(_required(payload, "exec_date"), "iso8601")
        trade_id = str(_required(payload, "id"))
    elif venue == "binance":
        price = _decimal_text(_required(payload, "p"), "p")
        size = _decimal_text(_required(payload, "q"), "q")
        maker = _required(payload, "m")
        if not isinstance(maker, bool):
            raise NormalizationError("m 必须为布尔值")
        side = "sell" if maker else "buy"
        event = _iso_time(_required(payload, "T"), context.timestamp_unit)
        trade_id = str(_required(payload, "a"))
        first_id = str(_required(payload, "f"))
        last_id = str(_required(payload, "l"))
        granularity = "aggregate"
        side_basis = "taker_from_buyer_maker"
        normalization_version = BINANCE_AGGTRADE_NORMALIZATION_VERSION
        revision_id = 1
    elif venue == "coincheck":
        price = _decimal_text(_required(payload, "rate"), "rate")
        size = _decimal_text(_required(payload, "amount"), "amount")
        side = _side(_required(payload, "side"))
        event = _iso_time(_required(payload, "timestamp"), context.timestamp_unit)
        trade_id = str(_required(payload, "trade_id"))
    elif venue == "kraken":
        price = _decimal_text(_required(payload, "price"), "price")
        size = _decimal_text(_required(payload, "volume"), "volume")
        side = _side(_required(payload, "side"))
        event = _iso_time(_required(payload, "time"), context.timestamp_unit)
        trade_id = str(_required(payload, "trade_id"))
    elif venue == "okx":
        price = _decimal_text(_required(payload, "px"), "px")
        size = _decimal_text(_required(payload, "sz"), "sz")
        side = _side(_required(payload, "side"))
        event = _iso_time(_required(payload, "ts"), context.timestamp_unit)
        trade_id = str(_required(payload, "tradeId"))
    elif venue == "bybit":
        price = _decimal_text(_required(payload, "p"), "p")
        size = _decimal_text(_required(payload, "v"), "v")
        side = _side(_required(payload, "S"))
        event = _iso_time(_required(payload, "T"), context.timestamp_unit)
        trade_id = str(_required(payload, "i"))
    elif venue == "coinbase":
        price = _decimal_text(_required(payload, "price"), "price")
        size = _decimal_text(_required(payload, "size"), "size")
        side = _side(_required(payload, "side"), invert=True)
        event = _iso_time(_required(payload, "time"), "iso8601")
        trade_id = str(_required(payload, "trade_id"))
        side_basis = "maker"
    else:
        raise UnverifiedMappingError(f"{venue} 成交映射未核证")

    if datetime.fromisoformat(available_time) < datetime.fromisoformat(event):
        raise NormalizationError("可得时刻早于事件时刻")
    return NormalizedTrade(
        venue_id=venue,
        instrument_id=context.instrument_id,
        venue_trade_id=trade_id,
        event_time=event,
        available_time=available_time,
        ingest_time=ingest_time,
        side=side,
        source_side_basis=side_basis,
        price=price,
        size=size,
        match_granularity=granularity,
        id_origin=id_origin,
        sequence_id=sequence_id,
        first_trade_id=first_id,
        last_trade_id=last_id,
        time_origin="venue",
        normalization_version=normalization_version,
        schema_version=NORMALIZED_SCHEMA_VERSION,
        revision_id=revision_id,
        raw_item_index=context.raw_item_index,
        raw_source=context.raw_source,
    )


def normalize_book_top(
    *,
    context: NormalizationContext,
    event_time: object | None,
    bid: object,
    bid_size: object,
    ask: object,
    ask_size: object,
    depth_levels: int,
    source_depth_levels: int,
    sequence_id: object | None,
) -> NormalizedBookTop:
    """校验已提取顶档并生成稳定帧标识。"""
    ingest = _validate_context(context)
    available = _available_time(context, ingest)
    if event_time is None:
        event = ingest
        time_origin = "local"
    else:
        event = _iso_time(event_time, context.timestamp_unit)
        time_origin = "venue"
    if datetime.fromisoformat(available) < datetime.fromisoformat(event):
        available = event
    if depth_levels < 1 or source_depth_levels < depth_levels:
        raise NormalizationError("盘口档数非法")
    bid_text = _decimal_text(bid, "bid")
    ask_text = _decimal_text(ask, "ask")
    if Decimal(bid_text) >= Decimal(ask_text):
        raise NormalizationError("盘口交叉或锁价")
    bid_size_text = _decimal_text(bid_size, "bid_size")
    ask_size_text = _decimal_text(ask_size, "ask_size")
    seq = None if sequence_id is None else str(sequence_id)
    seed = "|".join(
        (
            context.venue_id,
            context.instrument_id,
            event,
            seq or "",
            context.raw_source,
            str(context.raw_item_index),
        )
    )
    return NormalizedBookTop(
        venue_id=context.venue_id,
        instrument_id=context.instrument_id,
        frame_id=sha256_hex(seed.encode("utf-8")),
        event_time=event,
        available_time=available,
        ingest_time=ingest,
        bid=bid_text,
        bid_size=bid_size_text,
        ask=ask_text,
        ask_size=ask_size_text,
        depth_levels=depth_levels,
        source_depth_levels=source_depth_levels,
        time_origin=time_origin,
        sequence_id=seq,
        normalization_version="book-top-normalization-v1",
        schema_version=NORMALIZED_SCHEMA_VERSION,
        raw_item_index=context.raw_item_index,
        raw_source=context.raw_source,
    )
