"""GMO API 枚举定义，取值与官方文档一致（A-04）。"""
from enum import StrEnum


class Side(StrEnum):
    """売買区分（U-04）。"""

    BUY = "BUY"
    SELL = "SELL"


class ExecutionType(StrEnum):
    """注文タイプ。"""

    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP = "STOP"


class TimeInForce(StrEnum):
    """執行数量条件。SOK 仅限指定品种的 Post-only。"""

    FAK = "FAK"
    FAS = "FAS"
    FOK = "FOK"
    SOK = "SOK"


class OrderStatus(StrEnum):
    """注文ステータス。"""

    WAITING = "WAITING"
    ORDERED = "ORDERED"
    MODIFYING = "MODIFYING"
    CANCELLING = "CANCELLING"
    CANCELED = "CANCELED"
    EXECUTED = "EXECUTED"
    EXPIRED = "EXPIRED"


class OrderType(StrEnum):
    """取引区分。"""

    NORMAL = "NORMAL"
    LOSSCUT = "LOSSCUT"


class SettleType(StrEnum):
    """決済区分。LOSS_CUT 仅出现于 WS 通知。"""

    OPEN = "OPEN"
    CLOSE = "CLOSE"
    LOSS_CUT = "LOSS_CUT"


class ServiceStatus(StrEnum):
    """サービス稼働状態。"""

    MAINTENANCE = "MAINTENANCE"
    PREOPEN = "PREOPEN"
    OPEN = "OPEN"


class KlineInterval(StrEnum):
    """KLine 周期。"""

    MIN_1 = "1min"
    MIN_5 = "5min"
    MIN_10 = "10min"
    MIN_15 = "15min"
    MIN_30 = "30min"
    HOUR_1 = "1hour"
    HOUR_4 = "4hour"
    HOUR_8 = "8hour"
    HOUR_12 = "12hour"
    DAY_1 = "1day"
    WEEK_1 = "1week"
    MONTH_1 = "1month"


class WsChannel(StrEnum):
    """WebSocket 频道名。"""

    TICKER = "ticker"
    ORDERBOOKS = "orderbooks"
    TRADES = "trades"
    EXECUTION_EVENTS = "executionEvents"
    ORDER_EVENTS = "orderEvents"
    POSITION_EVENTS = "positionEvents"
    POSITION_SUMMARY_EVENTS = "positionSummaryEvents"


class RunMode(StrEnum):
    """运行模式（T-04），缺省模拟运行。"""

    DRY_RUN = "dry-run"
    LIVE = "live"
