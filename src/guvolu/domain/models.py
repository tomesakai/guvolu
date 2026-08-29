"""领域数据模型。字段以 2026-08-05 官方文档核实为准（A-04）。

金额与数量一律 Decimal，由字符串直接转换（T-08、D-07）。
时间统一带时区 UTC（D-08）。
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from guvolu.domain.errors import ApiSchemaError
from guvolu.domain.enums import (
    ExecutionType,
    OrderStatus,
    OrderType,
    ServiceStatus,
    SettleType,
    Side,
    TimeInForce,
)

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

Raw = Mapping[str, object]


def _s(data: Raw, key: str) -> str:
    return str(data[key])


def _s_opt(data: Raw, key: str) -> str | None:
    value = data.get(key)
    return None if value is None else str(value)


def _to_decimal(raw: str, key: str) -> Decimal:
    """转换数值字段，不可解析视为响应契约违例。"""
    try:
        return Decimal(raw)
    except ArithmeticError as error:
        raise ApiSchemaError(f"字段不是十进制数值: {key}={raw!r}") from error


def _dec(data: Raw, key: str) -> Decimal:
    return _to_decimal(_s(data, key), key)


def _dec_opt(data: Raw, key: str) -> Decimal | None:
    value = data.get(key)
    return None if value is None else _to_decimal(str(value), key)


def _int(data: Raw, key: str) -> int:
    """转换整数字段，不可解析视为响应契约违例。"""
    raw = _s(data, key)
    try:
        return int(raw)
    except ValueError as error:
        raise ApiSchemaError(f"字段不是整数: {key}={raw!r}") from error


def _int_opt(data: Raw, key: str) -> int | None:
    value = data.get(key)
    if value is None:
        return None
    raw = str(value)
    try:
        return int(raw)
    except ValueError as error:
        raise ApiSchemaError(f"字段不是整数: {key}={raw!r}") from error


def _dt(data: Raw, key: str) -> datetime:
    """转换时间字段，不可解析视为响应契约违例。"""
    raw = _s(data, key)
    try:
        return datetime.fromisoformat(raw)
    except ValueError as error:
        raise ApiSchemaError(f"字段不是时间戳: {key}={raw!r}") from error


def _dt_ms(data: Raw, key: str) -> datetime:
    # unix 毫秒转 UTC 时刻
    return _EPOCH + timedelta(milliseconds=int(_s(data, key)))


@dataclass(frozen=True, slots=True)
class Ticker:
    """最新レート。"""

    ask: Decimal
    bid: Decimal
    high: Decimal
    last: Decimal
    low: Decimal
    symbol: str
    timestamp: datetime
    volume: Decimal

    @classmethod
    def from_api(cls, data: Raw) -> "Ticker":
        return cls(
            ask=_dec(data, "ask"),
            bid=_dec(data, "bid"),
            high=_dec(data, "high"),
            last=_dec(data, "last"),
            low=_dec(data, "low"),
            symbol=_s(data, "symbol"),
            timestamp=_dt(data, "timestamp"),
            volume=_dec(data, "volume"),
        )


@dataclass(frozen=True, slots=True)
class OrderbookLevel:
    """盘口单档。"""

    price: Decimal
    size: Decimal

    @classmethod
    def from_api(cls, data: Raw) -> "OrderbookLevel":
        return cls(price=_dec(data, "price"), size=_dec(data, "size"))


@dataclass(frozen=True, slots=True)
class Orderbook:
    """板情報快照。"""

    symbol: str
    asks: tuple[OrderbookLevel, ...]
    bids: tuple[OrderbookLevel, ...]

    @classmethod
    def from_api(cls, data: Raw) -> "Orderbook":
        asks = data["asks"]
        bids = data["bids"]
        assert isinstance(asks, list) and isinstance(bids, list)
        return cls(
            symbol=_s(data, "symbol"),
            asks=tuple(OrderbookLevel.from_api(level) for level in asks),
            bids=tuple(OrderbookLevel.from_api(level) for level in bids),
        )


@dataclass(frozen=True, slots=True)
class PublicTrade:
    """公开逐笔成交。REST 响应无 symbol，WS 帧含之。"""

    price: Decimal
    side: Side
    size: Decimal
    timestamp: datetime
    symbol: str | None = None

    @classmethod
    def from_api(cls, data: Raw) -> "PublicTrade":
        return cls(
            price=_dec(data, "price"),
            side=Side(_s(data, "side")),
            size=_dec(data, "size"),
            timestamp=_dt(data, "timestamp"),
            symbol=_s_opt(data, "symbol"),
        )


@dataclass(frozen=True, slots=True)
class Kline:
    """KLine 一根。"""

    open_time: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal

    @classmethod
    def from_api(cls, data: Raw) -> "Kline":
        return cls(
            open_time=_dt_ms(data, "openTime"),
            open=_dec(data, "open"),
            high=_dec(data, "high"),
            low=_dec(data, "low"),
            close=_dec(data, "close"),
            volume=_dec(data, "volume"),
        )


@dataclass(frozen=True, slots=True)
class SymbolRule:
    """取引ルール。"""

    symbol: str
    min_order_size: Decimal
    max_order_size: Decimal
    size_step: Decimal
    tick_size: Decimal
    taker_fee: Decimal
    maker_fee: Decimal

    @classmethod
    def from_api(cls, data: Raw) -> "SymbolRule":
        return cls(
            symbol=_s(data, "symbol"),
            min_order_size=_dec(data, "minOrderSize"),
            max_order_size=_dec(data, "maxOrderSize"),
            size_step=_dec(data, "sizeStep"),
            tick_size=_dec(data, "tickSize"),
            taker_fee=_dec(data, "takerFee"),
            maker_fee=_dec(data, "makerFee"),
        )


@dataclass(frozen=True, slots=True)
class Asset:
    """資産残高单币种。amount 与 available 语义区分（U-03）。"""

    amount: Decimal
    available: Decimal
    conversion_rate: Decimal
    symbol: str

    @classmethod
    def from_api(cls, data: Raw) -> "Asset":
        return cls(
            amount=_dec(data, "amount"),
            available=_dec(data, "available"),
            conversion_rate=_dec(data, "conversionRate"),
            symbol=_s(data, "symbol"),
        )


@dataclass(frozen=True, slots=True)
class Margin:
    """余力情報。"""

    actual_profit_loss: Decimal
    available_amount: Decimal
    available_amount_for_spot: Decimal
    crypto_shortfall_status: str | None
    margin: Decimal
    margin_call_status: str | None
    margin_ratio: Decimal | None
    profit_loss: Decimal | None
    transferable_amount: Decimal | None

    @classmethod
    def from_api(cls, data: Raw) -> "Margin":
        return cls(
            actual_profit_loss=_dec(data, "actualProfitLoss"),
            available_amount=_dec(data, "availableAmount"),
            available_amount_for_spot=_dec(data, "availableAmountForSpot"),
            crypto_shortfall_status=_s_opt(data, "cryptoShortfallStatus"),
            margin=_dec(data, "margin"),
            margin_call_status=_s_opt(data, "marginCallStatus"),
            margin_ratio=_dec_opt(data, "marginRatio"),
            profit_loss=_dec_opt(data, "profitLoss"),
            transferable_amount=_dec_opt(data, "transferableAmount"),
        )


@dataclass(frozen=True, slots=True)
class VolumeLimit:
    """单品种当日可交易余量。"""

    symbol: str
    today_limit_open_size: Decimal | None
    today_limit_buy_size: Decimal | None
    today_limit_sell_size: Decimal | None
    taker_fee: Decimal | None
    maker_fee: Decimal | None

    @classmethod
    def from_api(cls, data: Raw) -> "VolumeLimit":
        return cls(
            symbol=_s(data, "symbol"),
            today_limit_open_size=_dec_opt(data, "todayLimitOpenSize"),
            today_limit_buy_size=_dec_opt(data, "todayLimitBuySize"),
            today_limit_sell_size=_dec_opt(data, "todayLimitSellSize"),
            taker_fee=_dec_opt(data, "takerFee"),
            maker_fee=_dec_opt(data, "makerFee"),
        )


@dataclass(frozen=True, slots=True)
class TradingVolume:
    """取引高情報。"""

    jpy_volume: Decimal
    tier_level: int
    limits: tuple[VolumeLimit, ...]

    @classmethod
    def from_api(cls, data: Raw) -> "TradingVolume":
        limit_rows = data.get("limit")
        rows = limit_rows if isinstance(limit_rows, list) else []
        return cls(
            jpy_volume=_dec(data, "jpyVolume"),
            tier_level=_int(data, "tierLevel"),
            limits=tuple(VolumeLimit.from_api(row) for row in rows),
        )


@dataclass(frozen=True, slots=True)
class FiatHistoryItem:
    """日本円入出金履历一条。"""

    amount: Decimal
    fee: Decimal | None
    status: str
    symbol: str
    timestamp: datetime

    @classmethod
    def from_api(cls, data: Raw) -> "FiatHistoryItem":
        return cls(
            amount=_dec(data, "amount"),
            fee=_dec_opt(data, "fee"),
            status=_s(data, "status"),
            symbol=_s(data, "symbol"),
            timestamp=_dt(data, "timestamp"),
        )


@dataclass(frozen=True, slots=True)
class CryptoHistoryItem:
    """暗号資産预入送付履历一条。"""

    address: str
    amount: Decimal
    fee: Decimal | None
    status: str
    symbol: str
    timestamp: datetime
    tx_hash: str | None

    @classmethod
    def from_api(cls, data: Raw) -> "CryptoHistoryItem":
        return cls(
            address=_s(data, "address"),
            amount=_dec(data, "amount"),
            fee=_dec_opt(data, "fee"),
            status=_s(data, "status"),
            symbol=_s(data, "symbol"),
            timestamp=_dt(data, "timestamp"),
            tx_hash=_s_opt(data, "txHash"),
        )


@dataclass(frozen=True, slots=True)
class Order:
    """委托（order）。与成交（execution）不混用（U-01）。"""

    root_order_id: int
    order_id: int
    symbol: str
    side: Side
    order_type: OrderType
    execution_type: ExecutionType
    settle_type: SettleType
    size: Decimal
    executed_size: Decimal
    price: Decimal | None
    losscut_price: Decimal
    status: OrderStatus
    cancel_type: str | None
    time_in_force: TimeInForce
    timestamp: datetime

    @classmethod
    def from_api(cls, data: Raw) -> "Order":
        return cls(
            root_order_id=_int(data, "rootOrderId"),
            order_id=_int(data, "orderId"),
            symbol=_s(data, "symbol"),
            side=Side(_s(data, "side")),
            order_type=OrderType(_s(data, "orderType")),
            execution_type=ExecutionType(_s(data, "executionType")),
            settle_type=SettleType(_s(data, "settleType")),
            size=_dec(data, "size"),
            executed_size=_dec(data, "executedSize"),
            price=_dec_opt(data, "price"),
            losscut_price=_dec(data, "losscutPrice"),
            status=OrderStatus(_s(data, "status")),
            cancel_type=_s_opt(data, "cancelType"),
            time_in_force=TimeInForce(_s(data, "timeInForce")),
            timestamp=_dt(data, "timestamp"),
        )


@dataclass(frozen=True, slots=True)
class Execution:
    """成交（execution）。"""

    execution_id: int
    order_id: int
    position_id: int | None
    symbol: str
    side: Side
    settle_type: SettleType
    size: Decimal
    price: Decimal
    loss_gain: Decimal
    fee: Decimal
    timestamp: datetime

    @classmethod
    def from_api(cls, data: Raw) -> "Execution":
        return cls(
            execution_id=_int(data, "executionId"),
            order_id=_int(data, "orderId"),
            position_id=_int_opt(data, "positionId"),
            symbol=_s(data, "symbol"),
            side=Side(_s(data, "side")),
            settle_type=SettleType(_s(data, "settleType")),
            size=_dec(data, "size"),
            price=_dec(data, "price"),
            loss_gain=_dec(data, "lossGain"),
            fee=_dec(data, "fee"),
            timestamp=_dt(data, "timestamp"),
        )


@dataclass(frozen=True, slots=True)
class Position:
    """持仓（position），杠杆专用。字段 orderdSize 为官方拼写。"""

    position_id: int
    symbol: str
    side: Side
    size: Decimal
    ordered_size: Decimal
    price: Decimal
    loss_gain: Decimal
    leverage: Decimal
    losscut_price: Decimal
    timestamp: datetime

    @classmethod
    def from_api(cls, data: Raw) -> "Position":
        return cls(
            position_id=_int(data, "positionId"),
            symbol=_s(data, "symbol"),
            side=Side(_s(data, "side")),
            size=_dec(data, "size"),
            ordered_size=_dec(data, "orderdSize"),
            price=_dec(data, "price"),
            loss_gain=_dec(data, "lossGain"),
            leverage=_dec(data, "leverage"),
            losscut_price=_dec(data, "losscutPrice"),
            timestamp=_dt(data, "timestamp"),
        )


@dataclass(frozen=True, slots=True)
class PositionSummaryItem:
    """持仓汇总单品种。"""

    average_position_rate: Decimal
    position_loss_gain: Decimal
    side: Side
    sum_order_quantity: Decimal
    sum_position_quantity: Decimal
    symbol: str

    @classmethod
    def from_api(cls, data: Raw) -> "PositionSummaryItem":
        return cls(
            average_position_rate=_dec(data, "averagePositionRate"),
            position_loss_gain=_dec(data, "positionLossGain"),
            side=Side(_s(data, "side")),
            sum_order_quantity=_dec(data, "sumOrderQuantity"),
            sum_position_quantity=_dec(data, "sumPositionQuantity"),
            symbol=_s(data, "symbol"),
        )


@dataclass(frozen=True, slots=True)
class WsOrderEvent:
    """注文情報通知。msgType: NOR/ROR/COR/ER。"""

    order_id: int
    symbol: str
    settle_type: SettleType
    execution_type: ExecutionType
    side: Side
    order_status: OrderStatus
    cancel_type: str | None
    order_timestamp: datetime
    order_price: Decimal | None
    order_size: Decimal
    order_executed_size: Decimal
    losscut_price: Decimal
    time_in_force: TimeInForce
    msg_type: str

    @classmethod
    def from_api(cls, data: Raw) -> "WsOrderEvent":
        return cls(
            order_id=_int(data, "orderId"),
            symbol=_s(data, "symbol"),
            settle_type=SettleType(_s(data, "settleType")),
            execution_type=ExecutionType(_s(data, "executionType")),
            side=Side(_s(data, "side")),
            order_status=OrderStatus(_s(data, "orderStatus")),
            cancel_type=_s_opt(data, "cancelType"),
            order_timestamp=_dt(data, "orderTimestamp"),
            order_price=_dec_opt(data, "orderPrice"),
            order_size=_dec(data, "orderSize"),
            order_executed_size=_dec(data, "orderExecutedSize"),
            losscut_price=_dec(data, "losscutPrice"),
            time_in_force=TimeInForce(_s(data, "timeInForce")),
            msg_type=_s(data, "msgType"),
        )


@dataclass(frozen=True, slots=True)
class WsExecutionEvent:
    """約定情報通知。msgType: ER。"""

    order_id: int
    execution_id: int
    symbol: str
    settle_type: SettleType
    execution_type: ExecutionType
    side: Side
    execution_price: Decimal
    execution_size: Decimal
    position_id: int | None
    order_timestamp: datetime
    execution_timestamp: datetime
    loss_gain: Decimal
    fee: Decimal
    order_price: Decimal | None
    order_size: Decimal
    order_executed_size: Decimal
    time_in_force: TimeInForce
    msg_type: str

    @classmethod
    def from_api(cls, data: Raw) -> "WsExecutionEvent":
        return cls(
            order_id=_int(data, "orderId"),
            execution_id=_int(data, "executionId"),
            symbol=_s(data, "symbol"),
            settle_type=SettleType(_s(data, "settleType")),
            execution_type=ExecutionType(_s(data, "executionType")),
            side=Side(_s(data, "side")),
            execution_price=_dec(data, "executionPrice"),
            execution_size=_dec(data, "executionSize"),
            position_id=_int_opt(data, "positionId"),
            order_timestamp=_dt(data, "orderTimestamp"),
            execution_timestamp=_dt(data, "executionTimestamp"),
            loss_gain=_dec(data, "lossGain"),
            fee=_dec(data, "fee"),
            order_price=_dec_opt(data, "orderPrice"),
            order_size=_dec(data, "orderSize"),
            order_executed_size=_dec(data, "orderExecutedSize"),
            time_in_force=TimeInForce(_s(data, "timeInForce")),
            msg_type=_s(data, "msgType"),
        )


@dataclass(frozen=True, slots=True)
class WsPositionEvent:
    """ポジション情報通知。msgType: OPR/UPR/ULR/CPR。"""

    position_id: int
    symbol: str
    side: Side
    size: Decimal
    ordered_size: Decimal
    price: Decimal
    loss_gain: Decimal
    leverage: Decimal
    losscut_price: Decimal
    timestamp: datetime
    msg_type: str

    @classmethod
    def from_api(cls, data: Raw) -> "WsPositionEvent":
        return cls(
            position_id=_int(data, "positionId"),
            symbol=_s(data, "symbol"),
            side=Side(_s(data, "side")),
            size=_dec(data, "size"),
            ordered_size=_dec(data, "orderdSize"),
            price=_dec(data, "price"),
            loss_gain=_dec(data, "lossGain"),
            leverage=_dec(data, "leverage"),
            losscut_price=_dec(data, "losscutPrice"),
            timestamp=_dt(data, "timestamp"),
            msg_type=_s(data, "msgType"),
        )


@dataclass(frozen=True, slots=True)
class WsPositionSummaryEvent:
    """ポジションサマリー情報通知。msgType: INIT/UPDATE/PERIODIC。"""

    symbol: str
    side: Side
    average_position_rate: Decimal
    position_loss_gain: Decimal
    sum_order_quantity: Decimal
    sum_position_quantity: Decimal
    timestamp: datetime
    msg_type: str

    @classmethod
    def from_api(cls, data: Raw) -> "WsPositionSummaryEvent":
        return cls(
            symbol=_s(data, "symbol"),
            side=Side(_s(data, "side")),
            average_position_rate=_dec(data, "averagePositionRate"),
            position_loss_gain=_dec(data, "positionLossGain"),
            sum_order_quantity=_dec(data, "sumOrderQuantity"),
            sum_position_quantity=_dec(data, "sumPositionQuantity"),
            timestamp=_dt(data, "timestamp"),
            msg_type=_s(data, "msgType"),
        )


def parse_service_status(data: Raw) -> ServiceStatus:
    """解析服务状态响应。"""
    return ServiceStatus(_s(data, "status"))
