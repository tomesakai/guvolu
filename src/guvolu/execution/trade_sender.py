"""TradeClient 发送适配：OrderSender 的生产接线（T-02）。

适配器只做一件事：把意图翻译为 TradeClient 的写调用，并把
异常收敛为发送编排可判定的三分类输入（T-06）：

- 明确成功：返回交易所委托号，仅证明被受理（T-03）。
- 明确失败：GmoApiError 原样上抛，编排记为 REJECTED。
- 结果未知：超时与网络错原样上抛；HTTP 层异常、响应形态
  异常与委托号不可解析同样折算为网络错，编排一律记为
  SEND_TIMEOUT，先查询再决策，绝不在本层重试（T-06、C-08）。

模拟运行守卫在 TradeClient 内生效（T-04），拦截异常穿透本层
由编排记入本地终态。撤单透传不受模拟运行限制（T-07）。
"""
from __future__ import annotations

from guvolu.api.trade_client import TradeClient
from guvolu.domain.errors import ApiHttpError, ApiNetworkError, ApiSchemaError
from guvolu.domain.intent import OrderIntent

# 下单端点路径
ORDER_PATH = "/v1/order"


class TradeClientSender:
    """把意图翻译为写路径调用，实现发送编排的 OrderSender。"""

    def __init__(self, client: TradeClient) -> None:
        self._client = client

    def send(self, intent: OrderIntent) -> int:
        """发出意图对应的委托，返回交易所委托号。

        返回值只证明被受理，实际状态以 READ_ONLY 为准（T-03）。
        意图模型已保证市价无价格、限价止损有正价格，本层不再
        校验参数。委托号不可解析时委托可能已被受理，按结果
        未知折算为网络错，交由超时对账判定（T-06）。
        """
        try:
            return self._client.order(
                intent.symbol,
                intent.side,
                intent.execution_type,
                intent.size,
                price=intent.price,
                time_in_force=intent.time_in_force,
            )
        except (ApiHttpError, ApiSchemaError, ValueError) as exc:
            # 写结果未知，折算为超时分类
            raise ApiNetworkError(ORDER_PATH, str(exc)) from exc

    def cancel(self, order_id: int) -> None:
        """撤单透传。任何运行模式均可达（T-07）。"""
        self._client.cancel_order(order_id)
