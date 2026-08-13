"""TRADE 密钥客户端：只写路径。

本类只持 TRADE 密钥，无任何读取能力（T-02）。
响应只证明委托被受理，实际状态一律以 READ_ONLY 为准（T-03）。
写请求超时不得盲目重发，须先查询再决策（T-06）。
端点路径与字段以 2026-08-05 官方文档核实为准（A-04）。
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal

from guvolu.api.transport import PrivateTransport, RateLimiter
from guvolu.domain.config import Config
from guvolu.domain.enums import ExecutionType, RunMode, SettleType, Side, TimeInForce
from guvolu.domain.errors import DryRunBlocked, SymbolError
from guvolu.domain.symbols import SpotSymbol, Symbol

Body = dict[str, object]


@dataclass(frozen=True, slots=True)
class CancelFailure:
    """批量撤单中被拒的单条委托。"""

    order_id: int
    code: str
    message: str


@dataclass(frozen=True, slots=True)
class CancelOrdersResult:
    """批量撤单结果。success 只表示受理（T-03）。"""

    success: tuple[int, ...]
    failed: tuple[CancelFailure, ...]


def _amount(value: Decimal) -> str:
    """金额与数量序列化，绝不经过 float（T-08）。"""
    return format(value, "f")


def _int_list(data: object) -> tuple[int, ...]:
    """解析委托号列表载荷。"""
    if not isinstance(data, list):
        return ()
    return tuple(int(str(item)) for item in data)


class TradeClient:
    """写路径客户端。读路径由 ReadClient 承担（T-02）。"""

    def __init__(
        self,
        transport: PrivateTransport,
        mode: RunMode,
        whitelist: frozenset[SpotSymbol],
    ) -> None:
        self._transport = transport
        self._mode = mode
        self._whitelist = whitelist

    @classmethod
    def from_config(
        cls, config: Config, limiter: RateLimiter | None = None
    ) -> "TradeClient":
        """按配置构造，只索取 TRADE 密钥（T-02）。

        limiter 供同进程多私有客户端共享限速（R-04 保守取向）。
        """
        api_key, api_secret = config.require_trade_credentials()
        shared = limiter if limiter is not None else RateLimiter(config.private_rps)
        return cls(
            PrivateTransport(api_key, api_secret, shared, config.log_dir),
            config.mode,
            config.spot_whitelist,
        )

    def _require_live(self) -> None:
        """模拟运行模式下拒绝建仓类写请求（T-04）。"""
        if self._mode is not RunMode.LIVE:
            raise DryRunBlocked("模拟运行模式拒绝写请求")

    def _require_whitelisted(self, symbol: SpotSymbol) -> None:
        """白名单放行而非黑名单拦截（T-09）。"""
        if symbol not in self._whitelist:
            raise SymbolError(f"品种不在白名单: {symbol!r}")

    def order(
        self,
        symbol: SpotSymbol,
        side: Side,
        execution_type: ExecutionType,
        size: Decimal,
        price: Decimal | None = None,
        time_in_force: TimeInForce | None = None,
        cancel_before: bool = False,
    ) -> int:
        """发出委托，返回交易所委托号（U-01）。

        仅现物路径。杠杆专用参数 losscutPrice 不在本方法暴露，
        平仓权限开通前杠杆执行路径必须保持不可达（T-09）。
        返回值只证明委托被受理，成交与否以 READ_ONLY 为准（T-03）。
        """
        self._require_live()
        self._require_whitelisted(symbol)
        price_text: str | None = None
        if execution_type is ExecutionType.MARKET:
            if price is not None:
                raise ValueError("市价委托不得带价格")
        elif price is None:
            raise ValueError("限价与止损委托必须带价格")
        else:
            price_text = _amount(price)
        body: Body = {
            "symbol": str(symbol),
            "side": side.value,
            "executionType": execution_type.value,
            "size": _amount(size),
        }
        if price_text is not None:
            body["price"] = price_text
        if time_in_force is not None:
            body["timeInForce"] = time_in_force.value
        if cancel_before:
            body["cancelBefore"] = True
        data = self._transport.request("POST", "/v1/order", body=body)
        return int(str(data))

    def change_order(
        self,
        order_id: int,
        price: Decimal,
        losscut_price: Decimal | None = None,
    ) -> None:
        """改单。losscut_price 为杠杆专用，现物路径不传（T-09）。

        目标委托不存在时返回 ERR-5123，按 T-06 查询后决策。
        """
        self._require_live()
        body: Body = {"orderId": order_id, "price": _amount(price)}
        if losscut_price is not None:
            body["losscutPrice"] = _amount(losscut_price)
        self._transport.request("POST", "/v1/changeOrder", body=body)

    def cancel_order(self, order_id: int) -> None:
        """撤单。任何运行模式均允许。

        撤单只减少风险，紧急停止路径必须随时可用（T-07），
        因此不受模拟运行守卫限制（T-04 针对的是建仓类写请求）。
        目标委托不存在时返回 ERR-151，按 T-06 查询后决策。
        """
        self._transport.request(
            "POST", "/v1/cancelOrder", body={"orderId": order_id}
        )

    def cancel_orders(self, order_ids: Sequence[int]) -> CancelOrdersResult:
        """按委托号批量撤单。任何运行模式均允许，理由同 cancel_order。"""
        body: Body = {"orderIds": list(order_ids)}
        data = self._transport.request("POST", "/v1/cancelOrders", body=body)
        if not isinstance(data, Mapping):
            return CancelOrdersResult(success=(), failed=())
        failed: list[CancelFailure] = []
        raw_failed = data.get("failed")
        rows = raw_failed if isinstance(raw_failed, list) else []
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            failed.append(
                CancelFailure(
                    order_id=int(str(row.get("orderId"))),
                    code=str(row.get("message_code", "")),
                    message=str(row.get("message_string", "")),
                )
            )
        return CancelOrdersResult(
            success=_int_list(data.get("success")),
            failed=tuple(failed),
        )

    def cancel_bulk_order(
        self,
        symbols: Sequence[Symbol],
        side: Side | None = None,
        settle_type: SettleType | None = None,
        newer_first: bool | None = None,
    ) -> tuple[int, ...]:
        """按品种全撤挂单，返回受理的委托号。

        紧急停止开关依赖本方法，任何运行模式均允许（T-07）。
        newer_first 映射到官方参数 desc。
        """
        body: Body = {"symbols": [str(item) for item in symbols]}
        if side is not None:
            body["side"] = side.value
        if settle_type is not None:
            body["settleType"] = settle_type.value
        if newer_first is not None:
            body["desc"] = newer_first
        data = self._transport.request("POST", "/v1/cancelBulkOrder", body=body)
        return _int_list(data)
