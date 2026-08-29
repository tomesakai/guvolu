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
    """意图状态，语义沿用 T-05、T-06。

    DRY_RUN_BLOCKED 是模拟运行守卫在发送边界拦截后的本地
    终态（T-04），未触达任何交易所写端点，与交易所拒绝的
    REJECTED 严格区分（T-03）。PAPER_FILLED 与 PAPER_REJECTED
    是 paper 执行器在发送边界以成交模型替代真实发送后的本地
    终态，同样未触达任何写端点；二者只增不改既有语义（D-06）。
    """

    RECORDED = "RECORDED"
    GATE_REJECTED = "GATE_REJECTED"
    SENDING = "SENDING"
    ACCEPTED = "ACCEPTED"
    REJECTED = "REJECTED"
    SEND_TIMEOUT = "SEND_TIMEOUT"
    FAILED = "FAILED"
    DRY_RUN_BLOCKED = "DRY_RUN_BLOCKED"
    PAPER_FILLED = "PAPER_FILLED"
    PAPER_REJECTED = "PAPER_REJECTED"


# 终态集合，离开即违规
TERMINAL_STATES: frozenset[IntentState] = frozenset(
    {
        IntentState.GATE_REJECTED,
        IntentState.ACCEPTED,
        IntentState.REJECTED,
        IntentState.FAILED,
        IntentState.DRY_RUN_BLOCKED,
        IntentState.PAPER_FILLED,
        IntentState.PAPER_REJECTED,
    }
)
# 本地终态集合，未触达任何写端点
LOCAL_TERMINAL_STATES: frozenset[IntentState] = frozenset(
    {
        IntentState.GATE_REJECTED,
        IntentState.DRY_RUN_BLOCKED,
        IntentState.PAPER_FILLED,
        IntentState.PAPER_REJECTED,
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
                IntentState.DRY_RUN_BLOCKED,
                IntentState.PAPER_FILLED,
                IntentState.PAPER_REJECTED,
            }
        ),
        IntentState.SEND_TIMEOUT: frozenset(
            {IntentState.ACCEPTED, IntentState.FAILED}
        ),
        IntentState.GATE_REJECTED: frozenset(),
        IntentState.ACCEPTED: frozenset(),
        IntentState.REJECTED: frozenset(),
        IntentState.FAILED: frozenset(),
        IntentState.DRY_RUN_BLOCKED: frozenset(),
        IntentState.PAPER_FILLED: frozenset(),
        IntentState.PAPER_REJECTED: frozenset(),
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
    prediction_id 与 decision_time 是回链到决策记录的血缘字段
    （X-08），由执行目标继承；非目标驱动的意图可为空。
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
    prediction_id: str | None = None
    decision_time: datetime | None = None

    def __post_init__(self) -> None:
        if self.size <= 0:
            raise IntentError("数量必须为正")
        if self.created_at.tzinfo is None:
            raise IntentError("创建时刻必须带时区")
        if self.decision_time is not None and self.decision_time.tzinfo is None:
            raise IntentError("决策时刻必须带时区")
        if self.prediction_id is not None and not self.prediction_id:
            raise IntentError("prediction_id 不得为空文本")
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
