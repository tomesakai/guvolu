"""只读来源协议与公共类型（multi-source-data-design 第 8 节）。

协议原样取自设计文档：类型层面无任何写方法。
适配器只做协议差异（地址、分页、限速、重试），
语义决策一律不在适配器内做。
"""
from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol

# 原样响应包络行
RawEnvelope = dict[str, object]
# 实时流帧，本批不实现
RawFrame = dict[str, object]
# 频道名
Channel = str


@dataclass(frozen=True)
class Window:
    """UTC 日闭区间，两端形态 YYYYMMDD。"""

    start: str
    end: str


def window_days(window: Window) -> list[str]:
    """展开窗内逐日序列，闭区间。"""
    cursor = datetime.strptime(window.start, "%Y%m%d")
    stop = datetime.strptime(window.end, "%Y%m%d")
    out: list[str] = []
    while cursor <= stop:
        out.append(cursor.strftime("%Y%m%d"))
        cursor += timedelta(days=1)
    return out


class VenueRequestError(Exception):
    """来源请求在退避重试后仍失败。"""

    def __init__(self, venue_id: str, url: str, detail: str) -> None:
        super().__init__(f"{venue_id}: {url}: {detail}")
        self.venue_id = venue_id
        self.url = url
        self.detail = detail


class ReadOnlySource(Protocol):
    """只读行情来源，无任何写能力。"""

    venue_id: str

    def instruments(self) -> Sequence[RawEnvelope]: ...

    def klines(self, symbol: str, interval: str,
               window: Window) -> Iterator[RawEnvelope]: ...

    def trades(self, symbol: str,
               window: Window) -> Iterator[RawEnvelope]: ...

    def stream(self, channels: Sequence[Channel]) -> Iterator[RawFrame]: ...
