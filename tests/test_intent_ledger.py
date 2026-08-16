"""意图账本单测：落盘、迁移、映射与恢复（T-05、T-06、R-07）。

全部离线（C-13、C-14），不触发任何真实端点。
"""
from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from guvolu.data.intent_ledger import (
    DuplicateIntent,
    DuplicateOrderId,
    EvidenceRequired,
    InFlightConflict,
    IntentLedger,
    LedgerCorrupt,
    LedgerError,
    UnknownIntent,
)
from guvolu.domain.enums import ExecutionType, Side
from guvolu.domain.intent import IntentState, IntentTransitionError, OrderIntent
from guvolu.domain.symbols import SpotSymbol

CREATED_AT = datetime(2026, 8, 16, 0, 0, tzinfo=UTC)
EVIDENCE = {"path": "/v1/orders", "queried_at": "2026-08-16T00:01:00+00:00"}


def make_intent(intent_id: str, symbol: str = "BTC") -> OrderIntent:
    """构造合法限价意图。"""
    return OrderIntent(
        intent_id=intent_id,
        correlation_id="co0001",
        symbol=SpotSymbol(symbol),
        side=Side.BUY,
        execution_type=ExecutionType.LIMIT,
        size=Decimal("0.0001"),
        price=Decimal("1000000"),
        time_in_force=None,
        created_at=CREATED_AT,
    )


def open_ledger(tmp_path: Path) -> IntentLedger:
    """在临时目录打开账本。"""
    return IntentLedger(tmp_path / "intent_ledger.jsonl")


def test_record_persists_before_send(tmp_path: Path) -> None:
    """意图创建即落盘，先于任何发送（T-05）。"""
    ledger = open_ledger(tmp_path)
    ledger.record_intent(make_intent("it01"))
    assert ledger.state("it01") is IntentState.RECORDED
    body = ledger.path.read_text(encoding="utf-8")
    assert "it01" in body
    assert body.endswith("\n")


def test_duplicate_intent_rejected(tmp_path: Path) -> None:
    """intent_id 不得重复（D-05）。"""
    ledger = open_ledger(tmp_path)
    ledger.record_intent(make_intent("it01"))
    with pytest.raises(DuplicateIntent):
        ledger.record_intent(make_intent("it01"))


def test_unknown_intent_rejected(tmp_path: Path) -> None:
    """未知意图的迁移直接拒绝。"""
    ledger = open_ledger(tmp_path)
    with pytest.raises(UnknownIntent):
        ledger.begin_send("it99")


def test_accept_path_and_order_mapping(tmp_path: Path) -> None:
    """受理路径登记委托号映射（T-05）。"""
    ledger = open_ledger(tmp_path)
    ledger.record_intent(make_intent("it01"))
    ledger.begin_send("it01")
    ledger.accept("it01", 637001)
    assert ledger.state("it01") is IntentState.ACCEPTED
    assert ledger.order_id_of("it01") == 637001
    assert ledger.intent_id_for_order(637001) == "it01"


def test_gate_reject_path(tmp_path: Path) -> None:
    """闸门拒绝为终态。"""
    ledger = open_ledger(tmp_path)
    ledger.record_intent(make_intent("it01"))
    ledger.gate_reject("it01", reason="服务状态 MAINTENANCE 拒绝新意图")
    assert ledger.state("it01") is IntentState.GATE_REJECTED
    with pytest.raises(IntentTransitionError):
        ledger.begin_send("it01")


def test_reject_path(tmp_path: Path) -> None:
    """明确失败路径。"""
    ledger = open_ledger(tmp_path)
    ledger.record_intent(make_intent("it01"))
    ledger.begin_send("it01")
    ledger.reject("it01", reason="ERR-5106")
    assert ledger.state("it01") is IntentState.REJECTED


def test_timeout_resolution_requires_evidence(tmp_path: Path) -> None:
    """离开超时态必须携带查询证据（T-06）。"""
    ledger = open_ledger(tmp_path)
    ledger.record_intent(make_intent("it01"))
    ledger.begin_send("it01")
    ledger.mark_send_timeout("it01", reason="超时")
    with pytest.raises(EvidenceRequired):
        ledger.accept("it01", 637001)
    with pytest.raises(EvidenceRequired):
        ledger.transition("it01", IntentState.FAILED)
    ledger.accept("it01", 637001, evidence=EVIDENCE)
    assert ledger.state("it01") is IntentState.ACCEPTED


def test_timeout_confirmed_absent(tmp_path: Path) -> None:
    """查询确认未受理后转入终态（T-06）。"""
    ledger = open_ledger(tmp_path)
    ledger.record_intent(make_intent("it01"))
    ledger.begin_send("it01")
    ledger.mark_send_timeout("it01", reason="网络错")
    ledger.resolve_timeout_failed("it01", evidence=EVIDENCE)
    assert ledger.state("it01") is IntentState.FAILED


def test_illegal_transitions_rejected(tmp_path: Path) -> None:
    """非法迁移一律拒绝且不落盘。"""
    ledger = open_ledger(tmp_path)
    ledger.record_intent(make_intent("it01"))
    with pytest.raises(IntentTransitionError):
        ledger.accept("it01", 637001)
    ledger.begin_send("it01")
    with pytest.raises(IntentTransitionError):
        ledger.transition("it01", IntentState.FAILED)
    ledger.mark_send_timeout("it01", reason="超时")
    with pytest.raises(IntentTransitionError):
        ledger.transition("it01", IntentState.REJECTED, evidence=EVIDENCE)
    ledger.accept("it01", 637001, evidence=EVIDENCE)
    for target in IntentState:
        with pytest.raises(IntentTransitionError):
            ledger.transition("it01", target, evidence=EVIDENCE)


def test_accept_requires_order_id(tmp_path: Path) -> None:
    """受理迁移必须携带委托号（T-05）。"""
    ledger = open_ledger(tmp_path)
    ledger.record_intent(make_intent("it01"))
    ledger.begin_send("it01")
    with pytest.raises(LedgerError, match="委托号"):
        ledger.transition("it01", IntentState.ACCEPTED)


def test_duplicate_order_id_rejected(tmp_path: Path) -> None:
    """委托号不得重复映射（D-05）。"""
    ledger = open_ledger(tmp_path)
    ledger.record_intent(make_intent("it01"))
    ledger.begin_send("it01")
    ledger.accept("it01", 637001)
    ledger.record_intent(make_intent("it02"))
    ledger.begin_send("it02")
    with pytest.raises(DuplicateOrderId):
        ledger.accept("it02", 637001)


def test_in_flight_conflict_same_symbol(tmp_path: Path) -> None:
    """同品种至多一笔在途写请求（T-05）。"""
    ledger = open_ledger(tmp_path)
    ledger.record_intent(make_intent("it01"))
    ledger.record_intent(make_intent("it02"))
    ledger.begin_send("it01")
    with pytest.raises(InFlightConflict):
        ledger.begin_send("it02")
    ledger.accept("it01", 637001)
    ledger.begin_send("it02")
    assert ledger.state("it02") is IntentState.SENDING


def test_timeout_still_occupies_in_flight(tmp_path: Path) -> None:
    """超时态仍占用在途额度，未定论前不得再发（T-06）。"""
    ledger = open_ledger(tmp_path)
    ledger.record_intent(make_intent("it01"))
    ledger.record_intent(make_intent("it02"))
    ledger.begin_send("it01")
    ledger.mark_send_timeout("it01", reason="超时")
    assert ledger.in_flight(SpotSymbol("BTC")) == ("it01",)
    with pytest.raises(InFlightConflict):
        ledger.begin_send("it02")


def test_other_symbol_not_blocked(tmp_path: Path) -> None:
    """不同品种互不阻塞。"""
    ledger = open_ledger(tmp_path)
    ledger.record_intent(make_intent("it01", "BTC"))
    ledger.record_intent(make_intent("it02", "ETH"))
    ledger.begin_send("it01")
    ledger.begin_send("it02")
    assert set(ledger.in_flight()) == {"it01", "it02"}


def test_reopen_rebuilds_state(tmp_path: Path) -> None:
    """重放全量事件重建状态与映射（R-07）。"""
    ledger = open_ledger(tmp_path)
    ledger.record_intent(make_intent("it01"))
    ledger.begin_send("it01")
    ledger.accept("it01", 637001)
    ledger.record_intent(make_intent("it02"))
    ledger.begin_send("it02")
    ledger.mark_send_timeout("it02", reason="超时")
    reopened = IntentLedger(ledger.path)
    assert reopened.state("it01") is IntentState.ACCEPTED
    assert reopened.intent_id_for_order(637001) == "it01"
    assert reopened.state("it02") is IntentState.SEND_TIMEOUT
    assert reopened.intent("it02").size == Decimal("0.0001")
    assert reopened.intent_ids() == ("it01", "it02")


def test_partial_tail_recovery(tmp_path: Path) -> None:
    """尾部不完整行隔离后截断，历史状态不丢。"""
    ledger = open_ledger(tmp_path)
    ledger.record_intent(make_intent("it01"))
    ledger.begin_send("it01")
    partial = b'{"record":"transition","broken'
    with ledger.path.open("ab") as handle:
        handle.write(partial)
    recovered = IntentLedger(ledger.path)
    assert recovered.state("it01") is IntentState.SENDING
    sidecars = list(tmp_path.glob("intent_ledger.jsonl.partial-*"))
    assert len(sidecars) == 1
    assert sidecars[0].read_bytes() == partial
    assert recovered.path.read_bytes().endswith(b"\n")
    recovered.accept("it01", 637001)
    final = IntentLedger(ledger.path)
    assert final.state("it01") is IntentState.ACCEPTED


def test_partial_only_file_recovers_empty(tmp_path: Path) -> None:
    """仅含不完整行的文件恢复为空账本。"""
    path = tmp_path / "intent_ledger.jsonl"
    path.write_bytes(b'{"record":"intent"')
    ledger = IntentLedger(path)
    assert ledger.intent_ids() == ()
    assert path.read_bytes() == b""


def test_corrupt_complete_line_raises(tmp_path: Path) -> None:
    """换行完结的损坏行不自动修复（C-03）。"""
    path = tmp_path / "intent_ledger.jsonl"
    path.write_bytes(b"not json\n")
    with pytest.raises(LedgerCorrupt):
        IntentLedger(path)


def test_replayed_illegal_history_raises(tmp_path: Path) -> None:
    """历史中的非法迁移在装载时暴露。"""
    ledger = open_ledger(tmp_path)
    ledger.record_intent(make_intent("it01"))
    forged = (
        '{"schema_version":1,"record":"transition",'
        '"at":"2026-08-16T00:00:00+00:00","intent_id":"it01",'
        '"source":"RECORDED","target":"ACCEPTED",'
        '"order_id":1,"reason":null,"evidence":null}\n'
    )
    with ledger.path.open("ab") as handle:
        handle.write(forged.encode("utf-8"))
    with pytest.raises(LedgerCorrupt):
        IntentLedger(ledger.path)


def test_mark_interrupted_sends(tmp_path: Path) -> None:
    """恢复后把 SENDING 意图显式转入超时态（T-06）。"""
    ledger = open_ledger(tmp_path)
    ledger.record_intent(make_intent("it01"))
    ledger.begin_send("it01")
    reopened = IntentLedger(ledger.path)
    assert reopened.interrupted_sends() == ("it01",)
    assert reopened.mark_interrupted_sends() == ("it01",)
    assert reopened.state("it01") is IntentState.SEND_TIMEOUT
    final = IntentLedger(ledger.path)
    assert final.state("it01") is IntentState.SEND_TIMEOUT


def test_default_location_under_data_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """缺省位置在数据根下运行时解析（C-04）。"""
    monkeypatch.setenv("GUVOLU_DATA_ROOT", str(tmp_path))
    ledger = IntentLedger.at_default_location()
    assert ledger.path == tmp_path / "execution" / "intent_ledger.jsonl"
