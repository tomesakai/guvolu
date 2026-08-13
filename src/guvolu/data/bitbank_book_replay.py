"""bitbank ``depth_whole``/``depth_diff`` 的官方线序重放规则。"""
from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from datetime import datetime
from typing import Generic, TypeVar


T = TypeVar("T")


def wire_session_sql(
    frame_columns: set[str], segment_expression: str,
) -> str:
    """返回诚实的 wire-session SQL 表达式。

    新事实优先使用接收时记录的 ``connection_id``。旧事实没有连接身份，
    不能把整个 run 猜成一条连续连接；此时把存储 segment 当作保守边界，
    让下游在下一段重新等待 snapshot。
    """

    legacy_candidates = [
        name for name in ("run_id", "source_session_id")
        if name in frame_columns
    ]
    legacy_base = (
        "COALESCE(" + ",".join(
            f"NULLIF({name}, '')" for name in legacy_candidates
        ) + ", 'legacy')"
        if legacy_candidates else "'legacy'"
    )
    legacy_session = (
        f"CONCAT({legacy_base}, '/segment-', "
        f"CAST({segment_expression} AS VARCHAR))"
    )
    if "connection_id" not in frame_columns:
        return legacy_session
    return f"COALESCE(NULLIF(connection_id, ''), {legacy_session})"


@dataclass(frozen=True, slots=True)
class BitbankReplayAction(Generic[T]):
    """一帧在权威 whole 语义下的实际处理动作。"""

    frame: T
    session_changed: bool
    apply_levels: bool
    reset_book: bool
    attribute_changes: bool
    effective_available_time: datetime


def bitbank_replay_actions(
    frames: Iterable[T],
    *,
    message_kind: Callable[[T], str],
    sequence_id: Callable[[T], int | None],
    session_id: Callable[[T], str],
    available_time: Callable[[T], datetime],
) -> Iterator[BitbankReplayAction[T]]:
    """按 wire arrival 解释 whole/diff，而非按来源发布时间排序。

    diff 在首个 whole 前进入缓冲。whole ``S`` 到达时权威替换盘口，只重放
    缓冲中 ``s > S`` 的 diff；``s <= S`` 已被 whole 覆盖，绝不再次应用。
    被缓冲的 diff 在 whole 到达时只恢复状态，不归因成该时刻的新增/撤量。
    """

    current_session: str | None = None
    whole_sequence: int | None = None
    buffered: dict[int, T] = {}
    previous_by_kind: dict[str, int] = {}

    for frame in frames:
        session = session_id(frame)
        changed = current_session is not None and session != current_session
        if current_session is None or changed:
            current_session = session
            whole_sequence = None
            buffered = {}
            previous_by_kind = {}

        kind = message_kind(frame)
        sequence = sequence_id(frame)
        if sequence is None:
            raise ValueError("bitbank whole/diff 缺少可解释 sequence")
        if kind not in {"delta", "snapshot"}:
            raise ValueError(f"bitbank L2 message_kind 不支持: {kind}")
        previous = previous_by_kind.get(kind)
        if previous is not None and sequence <= previous:
            yield BitbankReplayAction(
                frame, changed, False, False, False, available_time(frame),
            )
            continue
        previous_by_kind[kind] = sequence

        if kind == "delta":
            if whole_sequence is None:
                buffered[sequence] = frame
                yield BitbankReplayAction(
                    frame, changed, False, False, False, available_time(frame),
                )
            elif sequence <= whole_sequence:
                yield BitbankReplayAction(
                    frame, changed, False, False, False, available_time(frame),
                )
            else:
                buffered[sequence] = frame
                yield BitbankReplayAction(
                    frame, changed, True, False, True, available_time(frame),
                )
            continue

        whole_sequence = sequence
        snapshot_available = available_time(frame)
        yield BitbankReplayAction(
            frame, changed, True, True, False, snapshot_available,
        )
        buffered = {seq: item for seq, item in buffered.items() if seq > sequence}
        for _seq, item in sorted(buffered.items()):
            yield BitbankReplayAction(
                item, False, True, False, False, snapshot_available,
            )
