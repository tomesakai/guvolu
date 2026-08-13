"""同一市场 L2 来源重叠时的最小裁决契约。"""
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from guvolu.data.book_l2_contract import (
    BOOK_L2_NORMALIZATION_VERSION,
    BOOK_L2_V5_NORMALIZATION_VERSION,
)


class L2OutputWindow(Protocol):
    """裁决所需的活动输出最小投影。"""

    @property
    def dataset(self) -> str: ...

    @property
    def normalization_version(self) -> str: ...

    @property
    def min_event_time(self) -> datetime | None: ...

    @property
    def max_event_time(self) -> datetime | None: ...


@dataclass(frozen=True, slots=True)
class EventCoverage:
    """闭区间事件时间覆盖。"""

    start: datetime
    end: datetime


def okx_live_event_coverage(
    venue_id: str, outputs: Iterable[L2OutputWindow],
) -> tuple[EventCoverage, ...]:
    """返回 OKX live v5 活动输出的安全优先包络。

    仅凭文件 min/max 不能证明内部缺口可安全切换来源，因此所有 live 输出
    组成一个包络；archive v2 只在包络外 fallback，避免会话内来回切源。
    精细内部 fallback 必须另用 snapshot、原生序列与终态 checkpoint 证明。
    """

    if venue_id != "okx":
        return ()
    ranges = sorted(
        (
            EventCoverage(row.min_event_time, row.max_event_time)
            for row in outputs
            if row.dataset == "book_l2_frame"
            and row.normalization_version == BOOK_L2_V5_NORMALIZATION_VERSION
            and row.min_event_time is not None
            and row.max_event_time is not None
        ),
        key=lambda row: (row.start, row.end),
    )
    for current in ranges:
        if current.end < current.start:
            raise ValueError("L2 输出事件时间范围倒置")
    if not ranges:
        return ()
    return (EventCoverage(ranges[0].start, max(row.end for row in ranges)),)


def is_shadowed_okx_archive_frame(
    normalization_version: str | None,
    event_time: datetime,
    live_coverage: Iterable[EventCoverage],
) -> bool:
    """判定一个 OKX archive v2 frame 是否被 live v5 覆盖。"""

    return (
        normalization_version == BOOK_L2_NORMALIZATION_VERSION
        and any(row.start <= event_time <= row.end for row in live_coverage)
    )
