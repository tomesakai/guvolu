"""意图模型与状态机纯校验单测（T-05、T-06、C-15）。"""
from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from guvolu.domain.enums import ExecutionType, Side
from guvolu.domain.errors import SymbolError
from guvolu.domain.intent import (
    IN_FLIGHT_STATES,
    QUERY_REQUIRED_STATES,
    TERMINAL_STATES,
    IntentError,
    IntentState,
    IntentTransitionError,
    OrderIntent,
    allowed_targets,
    ensure_transition,
)
from guvolu.domain.symbols import SpotSymbol

CREATED_AT = datetime(2026, 8, 16, 0, 0, tzinfo=UTC)

# 独立誊写迁移表以对照（C-15）
EXPECTED_TRANSITIONS: dict[IntentState, set[IntentState]] = {
    IntentState.RECORDED: {IntentState.GATE_REJECTED, IntentState.SENDING},
    IntentState.SENDING: {
        IntentState.ACCEPTED,
        IntentState.REJECTED,
        IntentState.SEND_TIMEOUT,
        IntentState.DRY_RUN_BLOCKED,
        IntentState.PAPER_FILLED,
        IntentState.PAPER_REJECTED,
    },
    IntentState.SEND_TIMEOUT: {IntentState.ACCEPTED, IntentState.FAILED},
    IntentState.GATE_REJECTED: set(),
    IntentState.ACCEPTED: set(),
    IntentState.REJECTED: set(),
    IntentState.FAILED: set(),
    IntentState.DRY_RUN_BLOCKED: set(),
    IntentState.PAPER_FILLED: set(),
    IntentState.PAPER_REJECTED: set(),
}


def make_intent(
    execution_type: ExecutionType = ExecutionType.LIMIT,
    price: Decimal | None = Decimal("1000000"),
    size: Decimal = Decimal("0.0001"),
) -> OrderIntent:
    """构造合法意图。"""
    return OrderIntent(
        intent_id="it0001",
        correlation_id="co0001",
        symbol=SpotSymbol("BTC"),
        side=Side.BUY,
        execution_type=execution_type,
        size=size,
        price=price,
        time_in_force=None,
        created_at=CREATED_AT,
    )


def test_transition_table_complete() -> None:
    """全状态对偶校验：合法通过，非法拒绝。"""
    for source in IntentState:
        assert allowed_targets(source) == frozenset(
            EXPECTED_TRANSITIONS[source]
        )
        for target in IntentState:
            if target in EXPECTED_TRANSITIONS[source]:
                ensure_transition(source, target)
            else:
                with pytest.raises(IntentTransitionError):
                    ensure_transition(source, target)


def test_state_families() -> None:
    """终态、在途与需证据集合互相自洽。"""
    assert TERMINAL_STATES == frozenset(
        state for state in IntentState if not EXPECTED_TRANSITIONS[state]
    )
    assert IN_FLIGHT_STATES == frozenset(
        {IntentState.SENDING, IntentState.SEND_TIMEOUT}
    )
    assert QUERY_REQUIRED_STATES == frozenset({IntentState.SEND_TIMEOUT})


def test_market_with_price_rejected() -> None:
    """市价意图不得带价格。"""
    with pytest.raises(IntentError, match="市价"):
        make_intent(ExecutionType.MARKET, Decimal("1000000"))


def test_limit_without_price_rejected() -> None:
    """限价意图必须带正价格。"""
    with pytest.raises(IntentError, match="价格"):
        make_intent(ExecutionType.LIMIT, None)
    with pytest.raises(IntentError, match="价格"):
        make_intent(ExecutionType.LIMIT, Decimal("0"))


def test_nonpositive_size_rejected() -> None:
    """数量必须为正。"""
    with pytest.raises(IntentError, match="数量"):
        make_intent(size=Decimal("0"))


def test_naive_created_at_rejected() -> None:
    """创建时刻必须带时区（D-08）。"""
    with pytest.raises(IntentError, match="时区"):
        OrderIntent(
            intent_id="it0002",
            correlation_id="co0001",
            symbol=SpotSymbol("BTC"),
            side=Side.BUY,
            execution_type=ExecutionType.LIMIT,
            size=Decimal("0.0001"),
            price=Decimal("1000000"),
            time_in_force=None,
            created_at=datetime(2026, 8, 16, 0, 0),
        )


def test_leverage_symbol_unreachable() -> None:
    """杠杆形态无法进入意图类型（T-09、U-02）。"""
    with pytest.raises(SymbolError):
        SpotSymbol("BTC_JPY")


def test_notional_from_limit_price() -> None:
    """限价意图名义金额为数量乘限价。"""
    intent = make_intent()
    assert intent.notional_jpy() == Decimal("100")


def test_market_notional_requires_reference() -> None:
    """市价意图必须给正参考价。"""
    intent = make_intent(ExecutionType.MARKET, None)
    with pytest.raises(IntentError, match="参考价"):
        intent.notional_jpy()
    assert intent.notional_jpy(Decimal("2000000")) == Decimal("200")


def make_lineage_intent(
    *, prediction_id: str | None, decision_time: datetime | None,
) -> OrderIntent:
    """构造携带血缘字段的意图（X-08）。"""
    return OrderIntent(
        intent_id="it0003",
        correlation_id="co0001",
        symbol=SpotSymbol("BTC"),
        side=Side.BUY,
        execution_type=ExecutionType.LIMIT,
        size=Decimal("0.0001"),
        price=Decimal("1000000"),
        time_in_force=None,
        created_at=CREATED_AT,
        prediction_id=prediction_id,
        decision_time=decision_time,
    )


def test_naive_decision_time_rejected() -> None:
    """决策时刻必须带时区（D-08）。"""
    with pytest.raises(IntentError, match="决策时刻"):
        make_lineage_intent(
            prediction_id="pred-1", decision_time=datetime(2026, 8, 16, 0, 0),
        )
    aware = make_lineage_intent(prediction_id="pred-1", decision_time=CREATED_AT)
    assert aware.decision_time == CREATED_AT


def test_empty_prediction_id_rejected() -> None:
    """prediction_id 可为空值但不得为空文本。"""
    with pytest.raises(IntentError, match="prediction_id"):
        make_lineage_intent(prediction_id="", decision_time=CREATED_AT)
    absent = make_lineage_intent(prediction_id=None, decision_time=None)
    assert absent.prediction_id is None and absent.decision_time is None
