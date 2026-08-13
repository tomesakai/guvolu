"""READ_ONLY 密钥客户端：只读路径。

本类只持 READ_ONLY 密钥，绝不进入写路径（T-02）。
账户、委托、成交、持仓的唯一真相源即为本类（T-03）。
端点路径与字段以 2026-08-05 官方文档核实为准（A-04）。
"""
from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta

from guvolu.api.envelope import one, rows
from guvolu.api.transport import PrivateTransport, RateLimiter
from guvolu.domain.config import Config
from guvolu.domain.models import (
    Asset,
    CryptoHistoryItem,
    Execution,
    FiatHistoryItem,
    Margin,
    Order,
    Position,
    PositionSummaryItem,
    TradingVolume,
)
from guvolu.domain.symbols import LeverageSymbol

# 履历查询的时间窗口上限
HISTORY_WINDOW = timedelta(minutes=30)
# 委托号与成交号的批量上限
MAX_ID_COUNT = 10

Query = dict[str, str | int]


def format_history_timestamp(value: datetime) -> str:
    """转履历端点时间格式，毫秒三位并以 Z 结尾（C-12）。

    输入必须带时区，内部统一转 UTC（C-11）。
    """
    if value.tzinfo is None:
        raise ValueError("履历时间必须带时区")
    moment = value.astimezone(UTC)
    millis = moment.microsecond // 1000
    return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{millis:03d}Z"


def _paging(symbol: str, page: int | None, count: int | None) -> Query:
    """构造品种与分页参数。"""
    params: Query = {"symbol": symbol}
    if page is not None:
        params["page"] = page
    if count is not None:
        params["count"] = count
    return params


def _id_list(values: Sequence[int], name: str) -> str:
    """校验批量标识数量并以逗号连接。"""
    if not values:
        raise ValueError(f"{name} 至少一个")
    if len(values) > MAX_ID_COUNT:
        raise ValueError(f"{name} 最多 {MAX_ID_COUNT} 个")
    return ",".join(str(value) for value in values)


def _history_params(
    from_ts: datetime, to_ts: datetime | None, symbol: str | None = None
) -> Query:
    """构造履历参数并校验窗口，越界即快速失败。"""
    params: Query = {"fromTimestamp": format_history_timestamp(from_ts)}
    if to_ts is not None:
        to_text = format_history_timestamp(to_ts)
        if to_ts - from_ts > HISTORY_WINDOW:
            raise ValueError("履历查询窗口不得超过三十分钟")
        params["toTimestamp"] = to_text
    if symbol is not None:
        params["symbol"] = symbol
    return params


class ReadClient:
    """只读客户端。写路径由 TradeClient 承担，两者类型不可互换（T-02）。"""

    def __init__(self, transport: PrivateTransport) -> None:
        self._transport = transport

    @classmethod
    def from_config(
        cls, config: Config, limiter: RateLimiter | None = None
    ) -> "ReadClient":
        """按配置构造，只索取 READ_ONLY 密钥（T-02）。

        limiter 供同进程多私有客户端共享限速（R-04 保守取向）。
        """
        api_key, api_secret = config.require_read_credentials()
        shared = limiter if limiter is not None else RateLimiter(config.private_rps)
        return cls(
            PrivateTransport(api_key, api_secret, shared, config.log_dir)
        )

    def _get(self, path: str, params: Query | None = None) -> object:
        """发出只读请求并返回 data。"""
        return self._transport.request("GET", path, params=params)

    def assets(self) -> tuple[Asset, ...]:
        """取資産残高。amount 与 available 语义不同（U-03）。"""
        return tuple(
            Asset.from_api(row) for row in rows(self._get("/v1/account/assets"))
        )

    def margin(self) -> Margin:
        """取余力情報。"""
        path = "/v1/account/margin"
        return Margin.from_api(one(self._get(path), path))

    def trading_volume(self) -> TradingVolume:
        """取取引高情報与当日可交易余量。"""
        path = "/v1/account/tradingVolume"
        return TradingVolume.from_api(one(self._get(path), path))

    def fiat_deposit_history(
        self, from_ts: datetime, to_ts: datetime | None = None
    ) -> tuple[FiatHistoryItem, ...]:
        """取日本円入金履历。"""
        params = _history_params(from_ts, to_ts)
        data = self._get("/v1/account/fiatDeposit/history", params)
        return tuple(FiatHistoryItem.from_api(row) for row in rows(data))

    def fiat_withdrawal_history(
        self, from_ts: datetime, to_ts: datetime | None = None
    ) -> tuple[FiatHistoryItem, ...]:
        """取日本円出金履历。"""
        params = _history_params(from_ts, to_ts)
        data = self._get("/v1/account/fiatWithdrawal/history", params)
        return tuple(FiatHistoryItem.from_api(row) for row in rows(data))

    def deposit_history(
        self, symbol: str, from_ts: datetime, to_ts: datetime | None = None
    ) -> tuple[CryptoHistoryItem, ...]:
        """取暗号資産预入履历。"""
        params = _history_params(from_ts, to_ts, symbol)
        data = self._get("/v1/account/deposit/history", params)
        return tuple(CryptoHistoryItem.from_api(row) for row in rows(data))

    def withdrawal_history(
        self, symbol: str, from_ts: datetime, to_ts: datetime | None = None
    ) -> tuple[CryptoHistoryItem, ...]:
        """取暗号資産送付履历。"""
        params = _history_params(from_ts, to_ts, symbol)
        data = self._get("/v1/account/withdrawal/history", params)
        return tuple(CryptoHistoryItem.from_api(row) for row in rows(data))

    def orders(self, order_ids: Sequence[int]) -> tuple[Order, ...]:
        """按委托号查询委托（U-01）。一次最多十个。"""
        params: Query = {"orderId": _id_list(order_ids, "委托号")}
        data = self._get("/v1/orders", params)
        return tuple(Order.from_api(row) for row in rows(data))

    def active_orders(
        self, symbol: str, page: int | None = None, count: int | None = None
    ) -> tuple[Order, ...]:
        """取挂单一览，品种必填。"""
        params = _paging(symbol, page, count)
        data = self._get("/v1/activeOrders", params)
        return tuple(Order.from_api(row) for row in rows(data))

    def executions(
        self,
        order_id: int | None = None,
        execution_ids: Sequence[int] | None = None,
    ) -> tuple[Execution, ...]:
        """按委托号或成交号查询成交（U-01）。两者恰须提供其一。"""
        if (order_id is None) == (execution_ids is None):
            raise ValueError("委托号与成交号恰须提供其一")
        params: Query = {}
        if order_id is not None:
            params["orderId"] = order_id
        if execution_ids is not None:
            params["executionId"] = _id_list(execution_ids, "成交号")
        data = self._get("/v1/executions", params)
        return tuple(Execution.from_api(row) for row in rows(data))

    def latest_executions(
        self, symbol: str, page: int | None = None, count: int | None = None
    ) -> tuple[Execution, ...]:
        """取最新成交一览。"""
        params = _paging(symbol, page, count)
        data = self._get("/v1/latestExecutions", params)
        return tuple(Execution.from_api(row) for row in rows(data))

    def open_positions(
        self,
        symbol: LeverageSymbol,
        page: int | None = None,
        count: int | None = None,
    ) -> tuple[Position, ...]:
        """取建玉一覧。持仓仅杠杆存在，读取不构成执行路径（T-09）。"""
        params = _paging(str(symbol), page, count)
        data = self._get("/v1/openPositions", params)
        return tuple(Position.from_api(row) for row in rows(data))

    def position_summary(
        self, symbol: LeverageSymbol | None = None
    ) -> tuple[PositionSummaryItem, ...]:
        """取持仓汇总。省略品种时返回全部。"""
        params: Query = {}
        if symbol is not None:
            params["symbol"] = str(symbol)
        data = self._get("/v1/positionSummary", params or None)
        return tuple(PositionSummaryItem.from_api(row) for row in rows(data))
