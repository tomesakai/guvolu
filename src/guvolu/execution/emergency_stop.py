"""熔断触发动作：接入紧急停止开关的全量撤单（T-07、R-02）。

熔断触发从「拒绝新意图」升级为「拒绝新意图 + 全量撤单」：
执行侧把本模块动作登记到熔断器，首次触发时调用
ops.kill_switch.cancel_all 对全品种撤单。kill_switch 自身保持
零依赖、可脱离主进程独立执行（T-07），本模块只是执行链内的
调用方接线。撤单不受模拟运行守卫限制（T-04 针对建仓类写请求），
彩排与实盘均真实触碰撤单端点，报告须列明（A-03）。动作失败
不回滚熔断状态，留待人工处置；撤单受理结果只证明被接受，
实际状态以 READ_ONLY 为准（T-03）。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from guvolu.api.public_client import PublicClient
from guvolu.api.trade_client import TradeClient
from guvolu.domain.errors import GuvoluError
from guvolu.ops import kill_switch
from guvolu.risk.circuit_breaker import CircuitBreaker

# 全撤触碰的端点（A-03）
EMERGENCY_READ_ENDPOINT = "GET /v1/symbols"
EMERGENCY_WRITE_ENDPOINT = "POST /v1/cancelBulkOrder"


@dataclass(frozen=True, slots=True)
class EmergencyStopRecord:
    """一次全撤动作的留痕（R-07）。"""

    at: datetime
    reason: str
    exit_code: int | None
    error: str | None


class EmergencyStopAction:
    """把熔断触发翻译为 kill_switch 全撤调用。"""

    def __init__(self, public: PublicClient, trade: TradeClient) -> None:
        self._public = public
        self._trade = trade
        self._records: list[EmergencyStopRecord] = []

    @property
    def records(self) -> tuple[EmergencyStopRecord, ...]:
        """全部动作留痕，按发生顺序。"""
        return tuple(self._records)

    def __call__(self, reason: str) -> None:
        """执行全量撤单并留痕；异常收敛，不破坏熔断状态。"""
        now = datetime.now(UTC)
        try:
            exit_code = kill_switch.cancel_all(self._public, self._trade)
        except GuvoluError as exc:
            # 撤单失败留痕待人工
            self._records.append(
                EmergencyStopRecord(now, reason, None, str(exc))
            )
            return
        self._records.append(
            EmergencyStopRecord(now, reason, exit_code, None)
        )


def arm_emergency_stop(
    breaker: CircuitBreaker, public: PublicClient, trade: TradeClient
) -> EmergencyStopAction:
    """给熔断器登记全撤动作并返回留痕载体（T-07）。"""
    action = EmergencyStopAction(public, trade)
    breaker.set_trip_action(action)
    return action
