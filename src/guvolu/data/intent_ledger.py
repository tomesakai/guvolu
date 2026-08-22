"""意图账本：落盘先于发送的追加式事件记录（T-05、R-07）。

每行一条 JSON 事件，写入即 fsync；重启后重放全量事件重建状态
并复验迁移合法性。进程中断留下的尾部不完整行在装载时移入旁证
文件后截断，不静默丢弃字节。真实委托状态一律以 READ_ONLY 对账
为准（T-03），账本只记录本地视角。
"""
from __future__ import annotations

import json
import os
import time
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from guvolu.data.durable_io import (
    atomic_write_bytes,
    durable_append_bytes,
    exclusive_path_lock,
)
from guvolu.data.paths import data_root
from guvolu.domain.enums import ExecutionType, Side, TimeInForce
from guvolu.domain.errors import GuvoluError, SymbolError
from guvolu.domain.intent import (
    IN_FLIGHT_STATES,
    QUERY_REQUIRED_STATES,
    IntentError,
    IntentState,
    IntentTransitionError,
    OrderIntent,
    ensure_transition,
)
from guvolu.domain.symbols import SpotSymbol

# 第 2 版只增血缘字段（D-06）
SCHEMA_VERSION = 2
# 账本在数据根下的相对位置
LEDGER_RELATIVE_PATH = Path("execution") / "intent_ledger.jsonl"


class LedgerError(GuvoluError):
    """意图账本操作被拒。"""


class LedgerCorrupt(LedgerError):
    """账本历史损坏，须人工处置，不自动修复。"""


class DuplicateIntent(LedgerError):
    """intent_id 重复（D-05）。"""


class UnknownIntent(LedgerError):
    """引用的意图不存在。"""


class InFlightConflict(LedgerError):
    """同品种已有在途写请求（T-05）。"""


class EvidenceRequired(LedgerError):
    """离开超时态缺少 READ_ONLY 查询证据（T-06）。"""


class DuplicateOrderId(LedgerError):
    """交易所委托号重复映射（T-05、D-05）。"""


@dataclass(frozen=True, slots=True)
class LedgerEntry:
    """单意图的当前视图。"""

    intent: OrderIntent
    state: IntentState
    order_id: int | None


def _required_text(record: Mapping[str, object], key: str) -> str:
    value = record.get(key)
    if not isinstance(value, str) or not value:
        raise LedgerCorrupt(f"字段 {key} 缺失或非文本")
    return value


def _optional_text(record: Mapping[str, object], key: str) -> str | None:
    value = record.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise LedgerCorrupt(f"字段 {key} 非文本")
    return value


def _intent_record(intent: OrderIntent, at: datetime) -> dict[str, object]:
    """序列化意图创建行，金额与数量落字符串（D-07）。"""
    return {
        "schema_version": SCHEMA_VERSION,
        "record": "intent",
        "at": at.isoformat(),
        "intent_id": intent.intent_id,
        "correlation_id": intent.correlation_id,
        "symbol": str(intent.symbol),
        "side": intent.side.value,
        "execution_type": intent.execution_type.value,
        "size": format(intent.size, "f"),
        "price": None if intent.price is None else format(intent.price, "f"),
        "time_in_force": (
            None
            if intent.time_in_force is None
            else intent.time_in_force.value
        ),
        "created_at": intent.created_at.isoformat(),
        "prediction_id": intent.prediction_id,
        "decision_time": (
            None
            if intent.decision_time is None
            else intent.decision_time.isoformat()
        ),
    }


class IntentLedger:
    """追加式意图账本。单写边界内使用，不做并发仲裁。"""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._entries: dict[str, LedgerEntry] = {}
        self._order_map: dict[int, str] = {}
        self._load()

    @classmethod
    def at_default_location(cls) -> "IntentLedger":
        """在数据根下打开缺省账本，路径运行时解析（C-04）。"""
        return cls(data_root() / LEDGER_RELATIVE_PATH)

    @property
    def path(self) -> Path:
        """账本文件路径。"""
        return self._path

    def record_intent(
        self, intent: OrderIntent, *, at: datetime | None = None
    ) -> None:
        """意图创建先落盘，返回后方可进入闸门与发送（T-05）。"""
        if intent.intent_id in self._entries:
            raise DuplicateIntent(f"重复意图: {intent.intent_id}")
        moment = at if at is not None else datetime.now(UTC)
        self._append(_intent_record(intent, moment))
        self._entries[intent.intent_id] = LedgerEntry(
            intent=intent, state=IntentState.RECORDED, order_id=None
        )

    def transition(
        self,
        intent_id: str,
        target: IntentState,
        *,
        order_id: int | None = None,
        reason: str | None = None,
        evidence: Mapping[str, str] | None = None,
        at: datetime | None = None,
    ) -> None:
        """校验并落盘一次迁移，落盘成功后更新内存视图（R-07）。"""
        entry = self._entries.get(intent_id)
        if entry is None:
            raise UnknownIntent(f"未知意图: {intent_id}")
        self._validate(entry, target, order_id, evidence)
        moment = at if at is not None else datetime.now(UTC)
        record: dict[str, object] = {
            "schema_version": SCHEMA_VERSION,
            "record": "transition",
            "at": moment.isoformat(),
            "intent_id": intent_id,
            "source": entry.state.value,
            "target": target.value,
            "order_id": order_id,
            "reason": reason,
            "evidence": None if evidence is None else dict(evidence),
        }
        self._append(record)
        self._apply(intent_id, entry, target, order_id)

    def begin_send(
        self, intent_id: str, *, at: datetime | None = None
    ) -> None:
        """闸门通过后进入在途（T-05）。"""
        self.transition(intent_id, IntentState.SENDING, at=at)

    def gate_reject(
        self, intent_id: str, *, reason: str, at: datetime | None = None
    ) -> None:
        """风控闸门拒绝，记录理由。"""
        self.transition(
            intent_id, IntentState.GATE_REJECTED, reason=reason, at=at
        )

    def accept(
        self,
        intent_id: str,
        order_id: int,
        *,
        evidence: Mapping[str, str] | None = None,
        at: datetime | None = None,
    ) -> None:
        """受理并登记委托号映射；自超时态受理必须带证据（T-06）。"""
        self.transition(
            intent_id,
            IntentState.ACCEPTED,
            order_id=order_id,
            evidence=evidence,
            at=at,
        )

    def reject(
        self, intent_id: str, *, reason: str, at: datetime | None = None
    ) -> None:
        """交易所明确拒绝，记录错误码文本。"""
        self.transition(
            intent_id, IntentState.REJECTED, reason=reason, at=at
        )

    def mark_send_timeout(
        self, intent_id: str, *, reason: str, at: datetime | None = None
    ) -> None:
        """发送超时或网络错，结果未知，等待查询决策（T-06）。"""
        self.transition(
            intent_id, IntentState.SEND_TIMEOUT, reason=reason, at=at
        )

    def block_dry_run(
        self, intent_id: str, *, reason: str, at: datetime | None = None
    ) -> None:
        """模拟运行守卫在发送边界拦截，本地终态（T-04）。"""
        self.transition(
            intent_id, IntentState.DRY_RUN_BLOCKED, reason=reason, at=at
        )

    def paper_fill(
        self,
        intent_id: str,
        *,
        reason: str,
        evidence: Mapping[str, str],
        at: datetime | None = None,
    ) -> None:
        """paper 成交模型在发送边界结算，本地终态（T-04）。"""
        self.transition(
            intent_id,
            IntentState.PAPER_FILLED,
            reason=reason,
            evidence=evidence,
            at=at,
        )

    def paper_reject(
        self, intent_id: str, *, reason: str, at: datetime | None = None
    ) -> None:
        """paper 成交模型拒绝结算，本地终态（T-04）。"""
        self.transition(
            intent_id, IntentState.PAPER_REJECTED, reason=reason, at=at
        )

    def resolve_timeout_failed(
        self,
        intent_id: str,
        *,
        evidence: Mapping[str, str],
        at: datetime | None = None,
    ) -> None:
        """经 READ_ONLY 查询确认未受理，转入终态（T-06）。"""
        self.transition(
            intent_id, IntentState.FAILED, evidence=evidence, at=at
        )

    def state(self, intent_id: str) -> IntentState:
        """取意图当前状态。"""
        return self._entry(intent_id).state

    def intent(self, intent_id: str) -> OrderIntent:
        """取意图原文。"""
        return self._entry(intent_id).intent

    def order_id_of(self, intent_id: str) -> int | None:
        """取意图映射的交易所委托号。"""
        return self._entry(intent_id).order_id

    def intent_id_for_order(self, order_id: int) -> str | None:
        """按交易所委托号反查意图（T-05 关联键）。"""
        return self._order_map.get(order_id)

    def intent_ids(self) -> tuple[str, ...]:
        """按落盘顺序列出全部意图。"""
        return tuple(self._entries)

    def in_flight(
        self, symbol: SpotSymbol | None = None
    ) -> tuple[str, ...]:
        """列出在途意图，可按品种过滤（T-05）。"""
        return tuple(
            intent_id
            for intent_id, entry in self._entries.items()
            if entry.state in IN_FLIGHT_STATES
            and (symbol is None or entry.intent.symbol == symbol)
        )

    def interrupted_sends(self) -> tuple[str, ...]:
        """列出仍处 SENDING 的意图，恢复后结果未知（T-06）。"""
        return tuple(
            intent_id
            for intent_id, entry in self._entries.items()
            if entry.state is IntentState.SENDING
        )

    def mark_interrupted_sends(
        self,
        *,
        reason: str = "进程恢复，发送结果未知",
        at: datetime | None = None,
    ) -> tuple[str, ...]:
        """把恢复时仍在 SENDING 的意图转入超时态（T-06）。"""
        marked = self.interrupted_sends()
        for intent_id in marked:
            self.mark_send_timeout(intent_id, reason=reason, at=at)
        return marked

    def _entry(self, intent_id: str) -> LedgerEntry:
        entry = self._entries.get(intent_id)
        if entry is None:
            raise UnknownIntent(f"未知意图: {intent_id}")
        return entry

    def _validate(
        self,
        entry: LedgerEntry,
        target: IntentState,
        order_id: int | None,
        evidence: Mapping[str, str] | None,
    ) -> None:
        """迁移守卫：合法性、证据、委托号与在途约束。"""
        ensure_transition(entry.state, target)
        if entry.state in QUERY_REQUIRED_STATES and evidence is None:
            raise EvidenceRequired(
                "离开超时态必须携带 READ_ONLY 查询证据"
            )
        if target is IntentState.ACCEPTED:
            if order_id is None:
                raise LedgerError("受理迁移必须携带交易所委托号")
            if order_id in self._order_map:
                raise DuplicateOrderId(
                    f"委托号 {order_id} 已映射到 {self._order_map[order_id]}"
                )
        if target is IntentState.SENDING:
            conflict = self.in_flight(entry.intent.symbol)
            if conflict:
                raise InFlightConflict(
                    f"品种 {entry.intent.symbol} 已有在途意图 {conflict[0]}"
                )

    def _apply(
        self,
        intent_id: str,
        entry: LedgerEntry,
        target: IntentState,
        order_id: int | None,
    ) -> None:
        new_order = entry.order_id if order_id is None else order_id
        self._entries[intent_id] = replace(
            entry, state=target, order_id=new_order
        )
        if target is IntentState.ACCEPTED and order_id is not None:
            self._order_map[order_id] = intent_id

    def _append(self, record: dict[str, object]) -> None:
        """追加一行并 fsync 后返回（T-05、R-07）。"""
        line = json.dumps(
            record, ensure_ascii=False, separators=(",", ":")
        )
        durable_append_bytes(self._path, (line + "\n").encode("utf-8"))

    def _load(self) -> None:
        """重放全量事件重建状态，尾部不完整行隔离后截断。"""
        if not self._path.exists():
            return
        raw = self._path.read_bytes()
        if not raw:
            return
        body = raw
        partial = b""
        if not raw.endswith(b"\n"):
            cut = raw.rfind(b"\n") + 1
            body, partial = raw[:cut], raw[cut:]
        if partial:
            self._quarantine_partial(body, partial)
        for number, blob in enumerate(body.split(b"\n")[:-1], 1):
            try:
                parsed: object = json.loads(blob)
            except json.JSONDecodeError as exc:
                raise LedgerCorrupt(
                    f"第 {number} 行不是合法 JSON"
                ) from exc
            if not isinstance(parsed, dict):
                raise LedgerCorrupt(f"第 {number} 行不是对象")
            kind = _required_text(parsed, "record")
            if kind == "intent":
                self._replay_intent(parsed, number)
            elif kind == "transition":
                self._replay_transition(parsed, number)
            else:
                raise LedgerCorrupt(f"第 {number} 行类型未知 {kind}")

    def _quarantine_partial(self, body: bytes, partial: bytes) -> None:
        """不完整尾行移入旁证文件后截断主文件，保全字节。"""
        sidecar = self._path.with_name(
            f"{self._path.name}.partial-{time.time_ns()}"
        )
        atomic_write_bytes(sidecar, partial)
        with exclusive_path_lock(self._path):
            with self._path.open("r+b") as handle:
                handle.truncate(len(body))
                handle.flush()
                os.fsync(handle.fileno())

    def _replay_intent(
        self, record: Mapping[str, object], number: int
    ) -> None:
        intent_id = _required_text(record, "intent_id")
        if intent_id in self._entries:
            raise LedgerCorrupt(f"第 {number} 行重复意图 {intent_id}")
        price_text = _optional_text(record, "price")
        tif_text = _optional_text(record, "time_in_force")
        # 第 1 版行无血缘字段，按空值兼容读取
        decision_text = _optional_text(record, "decision_time")
        try:
            intent = OrderIntent(
                intent_id=intent_id,
                correlation_id=_required_text(record, "correlation_id"),
                symbol=SpotSymbol(_required_text(record, "symbol")),
                side=Side(_required_text(record, "side")),
                execution_type=ExecutionType(
                    _required_text(record, "execution_type")
                ),
                size=Decimal(_required_text(record, "size")),
                price=None if price_text is None else Decimal(price_text),
                time_in_force=(
                    None if tif_text is None else TimeInForce(tif_text)
                ),
                created_at=datetime.fromisoformat(
                    _required_text(record, "created_at")
                ),
                prediction_id=_optional_text(record, "prediction_id"),
                decision_time=(
                    None if decision_text is None
                    else datetime.fromisoformat(decision_text)
                ),
            )
        except (ValueError, InvalidOperation, SymbolError, IntentError) as exc:
            raise LedgerCorrupt(f"第 {number} 行意图字段非法") from exc
        self._entries[intent_id] = LedgerEntry(
            intent=intent, state=IntentState.RECORDED, order_id=None
        )

    def _replay_transition(
        self, record: Mapping[str, object], number: int
    ) -> None:
        intent_id = _required_text(record, "intent_id")
        entry = self._entries.get(intent_id)
        if entry is None:
            raise LedgerCorrupt(
                f"第 {number} 行迁移引用未知意图 {intent_id}"
            )
        try:
            source = IntentState(_required_text(record, "source"))
            target = IntentState(_required_text(record, "target"))
        except ValueError as exc:
            raise LedgerCorrupt(f"第 {number} 行状态名非法") from exc
        if source is not entry.state:
            raise LedgerCorrupt(
                f"第 {number} 行迁移源状态与历史不符"
            )
        order_raw = record.get("order_id")
        order_id: int | None
        if order_raw is None:
            order_id = None
        elif isinstance(order_raw, int) and not isinstance(order_raw, bool):
            order_id = order_raw
        else:
            raise LedgerCorrupt(f"第 {number} 行委托号非整数")
        evidence_raw = record.get("evidence")
        evidence: Mapping[str, str] | None
        if evidence_raw is None:
            evidence = None
        elif isinstance(evidence_raw, dict):
            evidence = {
                str(key): str(value) for key, value in evidence_raw.items()
            }
        else:
            raise LedgerCorrupt(f"第 {number} 行证据非键值表")
        try:
            self._validate(entry, target, order_id, evidence)
        except (LedgerError, IntentTransitionError) as exc:
            raise LedgerCorrupt(
                f"第 {number} 行迁移非法: {exc}"
            ) from exc
        self._apply(intent_id, entry, target, order_id)
