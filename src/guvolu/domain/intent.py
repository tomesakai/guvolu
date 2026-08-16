"""下单意图与状态机（T-05、T-06，D-01 的 intent 层）。

状态迁移的合法集合在此唯一定义，意图账本与发送编排共同复用。
超时态只能携带 READ_ONLY 查询证据离开，绝不盲目重发（T-06）。
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from types import MappingProxyType

from guvolu.domain.enums import ExecutionType, Side, TimeInForce
from guvolu.domain.errors import GuvoluError
from guvolu.domain.symbols import SpotSymbol


class IntentError(GuvoluError):
    """意图字段非法。"""


class IntentTransitionError(GuvoluError):
    """意图状态迁移不合法。"""


class IntentState(StrEnum):
    """意图状态，语义沿用 T-05、T-06。"""

    RECORDED = "RECORDED"
    GATE_REJECTED = "GATE_REJECTED"
    SENDING = "SENDING"
    ACCEPTED = "ACCEPTED"
    REJECTED = "REJECTED"
    SEND_TIMEOUT = "SEND_TIMEOUT"
    FAILED = "FAILED"


# 终态集合，离开即违规
TERMINAL_STATES: frozenset[IntentState] = frozenset(
    {
        IntentState.GATE_REJECTED,
        IntentState.ACCEPTED,
        IntentState.REJECTED,
        IntentState.FAILED,
    }
)
# 在途集合，占用品种发送额度（T-05）
IN_FLIGHT_STATES: frozenset[IntentState] = frozenset(
    {IntentState.SENDING, IntentState.SEND_TIMEOUT}
)
# 离开须带查询证据（T-06）
QUERY_REQUIRED_STATES: frozenset[IntentState] = frozenset(
    {IntentState.SEND_TIMEOUT}
)

_ALLOWED: Mapping[IntentState, frozenset[IntentState]] = MappingProxyType(
    {
        IntentState.RECORDED: frozenset(
            {IntentState.GATE_REJECTED, IntentState.SENDING}
        ),
        IntentState.SENDING: frozenset(
            {
                IntentState.ACCEPTED,
                IntentState.REJECTED,
                IntentState.SEND_TIMEOUT,
            }
        ),
        IntentState.SEND_TIMEOUT: frozenset(
            {IntentState.ACCEPTED, IntentState.FAILED}
        ),
        IntentState.GATE_REJECTED: frozenset(),
        IntentState.ACCEPTED: frozenset(),
        IntentState.REJECTED: frozenset(),
        IntentState.FAILED: frozenset(),
    }
)


def allowed_targets(source: IntentState) -> frozenset[IntentState]:
    """取某状态的合法迁移目标集合。"""
    return _ALLOWED[source]


def ensure_transition(source: IntentState, target: IntentState) -> None:
    """校验迁移合法性，非法即拒绝。"""
    if target not in _ALLOWED[source]:
        raise IntentTransitionError(
            f"非法迁移 {source.value} -> {target.value}"
        )


@dataclass(frozen=True, slots=True)
class OrderIntent:
    """下单意图（U-07）。金额与数量一律 Decimal（T-08）。

    品种固定为现物类型，杠杆执行路径在类型层面不可达（T-09）。
    """

    intent_id: str
    correlation_id: str
    symbol: SpotSymbol
    side: Side
    execution_type: ExecutionType
    size: Decimal
    price: Decimal | None
    time_in_force: TimeInForce | None
    created_at: datetime

    def __post_init__(self) -> None:
        if self.size <= 0:
            raise IntentError("数量必须为正")
        if self.created_at.tzinfo is None:
            raise IntentError("创建时刻必须带时区")
        if self.execution_type is ExecutionType.MARKET:
            if self.price is not None:
                raise IntentError("市价意图不得带价格")
        elif self.price is None or self.price <= 0:
            raise IntentError("限价与止损意图必须带正价格")

    def notional_jpy(self, reference_price: Decimal | None = None) -> Decimal:
        """计算名义金额。市价意图必须给正参考价。"""
        if self.price is not None:
            return self.size * self.price
        if reference_price is None or reference_price <= 0:
            raise IntentError("市价意图缺少正参考价")
        return self.size * reference_price
