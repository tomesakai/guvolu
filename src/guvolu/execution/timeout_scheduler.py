"""超时意图自动查询调度：进入超时态即查询，指数退避至终态（T-06）。

阶段二的人工触发对账在此纳入对账循环自动执行：意图进入
SEND_TIMEOUT 即到期，首查在当轮完成；查询歧义或查询自身失败
时按指数退避排下一次，直到意图离开超时态（受理或判定未受理，
含 WS 通道抢先受理）。查询是只读 GET，自动重试合规（C-08）；
写请求本身仍绝不重发（T-06）。退避参数来自版本化配置（G-06）。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from guvolu.data.intent_ledger import IntentLedger
from guvolu.domain.errors import ApiNetworkError, GmoApiError, GuvoluError
from guvolu.domain.intent import IntentState
from guvolu.execution.reconcile import (
    ReadOnlyOrderReader,
    ReconcileAmbiguity,
    resolve_send_timeout,
    send_timeout_intents,
)


class BackoffError(GuvoluError):
    """退避参数非法。"""


@dataclass(frozen=True, slots=True)
class BackoffPolicy:
    """指数退避参数，数值为 TBD-07 提案（G-06）。"""

    initial_seconds: float
    max_seconds: float
    multiplier: float = 2.0

    def __post_init__(self) -> None:
        if self.initial_seconds <= 0:
            raise BackoffError("初始退避必须为正")
        if self.max_seconds < self.initial_seconds:
            raise BackoffError("退避上限低于初始值")
        if self.multiplier < 1:
            raise BackoffError("退避倍率不得小于一")

    def delay_seconds(self, attempt: int) -> float:
        """第 attempt 次未定论后的等待秒数，从一起计。"""
        if attempt < 1:
            raise BackoffError("尝试次数从一起计")
        delay = self.initial_seconds * self.multiplier ** (attempt - 1)
        return min(delay, self.max_seconds)


@dataclass(frozen=True, slots=True)
class TimeoutQueryOutcome:
    """一次自动查询的结果。"""

    intent_id: str
    disposition: str
    state: IntentState | None
    order_id: int | None
    attempt: int
    next_attempt_at: datetime | None
    detail: str | None


class TimeoutQueryScheduler:
    """超时意图的查询调度器，退避至终态（T-06）。"""

    def __init__(self, policy: BackoffPolicy) -> None:
        self._policy = policy
        self._next_due: dict[str, datetime] = {}
        self._attempts: dict[str, int] = {}

    def sync(self, ledger: IntentLedger, now: datetime) -> tuple[str, ...]:
        """对齐账本：新超时意图立即到期，已离开者出队。"""
        current = set(send_timeout_intents(ledger))
        entered: list[str] = []
        for intent_id in current:
            if intent_id not in self._next_due:
                self._next_due[intent_id] = now
                self._attempts[intent_id] = 0
                entered.append(intent_id)
        for intent_id in list(self._next_due):
            if intent_id not in current:
                del self._next_due[intent_id]
                del self._attempts[intent_id]
        return tuple(entered)

    def pending(self) -> tuple[str, ...]:
        """全部在队意图。"""
        return tuple(self._next_due)

    def due_intents(self, now: datetime) -> tuple[str, ...]:
        """列出已到期的意图。"""
        return tuple(
            intent_id
            for intent_id, due in self._next_due.items()
            if due <= now
        )

    def next_due_at(self, intent_id: str) -> datetime | None:
        """取意图的下次查询时刻。"""
        return self._next_due.get(intent_id)

    def run_due(
        self,
        *,
        ledger: IntentLedger,
        reader: ReadOnlyOrderReader,
        now: datetime | None = None,
    ) -> tuple[TimeoutQueryOutcome, ...]:
        """执行全部到期查询并按结果收队或退避。"""
        moment = now if now is not None else datetime.now(UTC)
        self.sync(ledger, moment)
        outcomes: list[TimeoutQueryOutcome] = []
        for intent_id in self.due_intents(moment):
            attempt = self._attempts[intent_id] + 1
            try:
                resolution = resolve_send_timeout(
                    intent_id, ledger=ledger, reader=reader, moment=moment
                )
            except ReconcileAmbiguity as exc:
                outcomes.append(
                    self._defer(intent_id, attempt, moment, "ambiguous", exc)
                )
            except (ApiNetworkError, GmoApiError) as exc:
                # 查询失败同样退避，绝不放弃在途占用
                outcomes.append(
                    self._defer(
                        intent_id, attempt, moment, "query_error", exc
                    )
                )
            else:
                del self._next_due[intent_id]
                del self._attempts[intent_id]
                outcomes.append(
                    TimeoutQueryOutcome(
                        intent_id=intent_id,
                        disposition="resolved",
                        state=resolution.state,
                        order_id=resolution.order_id,
                        attempt=attempt,
                        next_attempt_at=None,
                        detail=None,
                    )
                )
        return tuple(outcomes)

    def _defer(
        self,
        intent_id: str,
        attempt: int,
        moment: datetime,
        disposition: str,
        error: Exception,
    ) -> TimeoutQueryOutcome:
        """登记一次未定论并排下一次查询。"""
        self._attempts[intent_id] = attempt
        due = moment + timedelta(
            seconds=self._policy.delay_seconds(attempt)
        )
        self._next_due[intent_id] = due
        return TimeoutQueryOutcome(
            intent_id=intent_id,
            disposition=disposition,
            state=IntentState.SEND_TIMEOUT,
            order_id=None,
            attempt=attempt,
            next_attempt_at=due,
            detail=str(error),
        )
