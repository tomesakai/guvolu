"""K 线回补计划：日期枚举、求缺、时间语义（D-03、D-08）。"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

from guvolu.domain.enums import KlineInterval

# 按年拉取的周期集合
YEARLY_INTERVALS = frozenset(
    {
        KlineInterval.HOUR_4,
        KlineInterval.HOUR_8,
        KlineInterval.HOUR_12,
        KlineInterval.DAY_1,
        KlineInterval.WEEK_1,
        KlineInterval.MONTH_1,
    }
)
# 最早上市年，实测边界
FIRST_LISTING_YEAR = 2018
# 分钟级历史起点（实测边界）
MINUTE_HISTORY_START = "20210415"
# 交易日界见 D-08
TRADING_DAY_OFFSET = timedelta(hours=9 - 6)

_MINUTES: dict[KlineInterval, int] = {
    KlineInterval.MIN_1: 1,
    KlineInterval.MIN_5: 5,
    KlineInterval.MIN_10: 10,
    KlineInterval.MIN_15: 15,
    KlineInterval.MIN_30: 30,
    KlineInterval.HOUR_1: 60,
    KlineInterval.HOUR_4: 240,
    KlineInterval.HOUR_8: 480,
    KlineInterval.HOUR_12: 720,
    KlineInterval.DAY_1: 1440,
    KlineInterval.WEEK_1: 10080,
}


def trading_day(open_time: datetime) -> str:
    """K 线归属交易日（JST 06:00 边界）。"""
    return (open_time.astimezone(UTC) + TRADING_DAY_OFFSET).strftime("%Y%m%d")


def available_time(interval: KlineInterval, open_time: datetime) -> datetime:
    """该根收束时刻，即最早可合法得知时刻（D-04）。"""
    if interval is KlineInterval.MONTH_1:
        year = open_time.year + (open_time.month // 12)
        month = open_time.month % 12 + 1
        return open_time.replace(year=year, month=month, day=1)
    return open_time + timedelta(minutes=_MINUTES[interval])


def yearly_dates(now: datetime) -> list[str]:
    """年参数序列，自最早上市年至今年。"""
    return [str(year) for year in range(FIRST_LISTING_YEAR, now.year + 1)]


def daily_dates(start: str, end: str) -> list[str]:
    """交易日参数序列，闭区间。"""
    cursor = datetime.strptime(start, "%Y%m%d")
    stop = datetime.strptime(end, "%Y%m%d")
    out: list[str] = []
    while cursor <= stop:
        out.append(cursor.strftime("%Y%m%d"))
        cursor += timedelta(days=1)
    return out


def plan_requests(
    symbols: list[str], intervals: list[KlineInterval], dates: list[str]
) -> list[tuple[str, KlineInterval, str]]:
    """展开请求三元组，顺序为品种、周期、日期。"""
    return [
        (symbol, interval, date)
        for symbol in symbols
        for interval in intervals
        for date in dates
    ]


def missing_requests(
    plan: list[tuple[str, KlineInterval, str]],
    fetched: set[tuple[str, str, str]],
    current_period: str,
) -> list[tuple[str, KlineInterval, str]]:
    """求缺：已取齐的历史期跳过，当期一律重取。"""
    out: list[tuple[str, KlineInterval, str]] = []
    for symbol, interval, date in plan:
        if date < current_period[: len(date)] and (
            (symbol, interval.value, date) in fetched
        ):
            continue
        out.append((symbol, interval, date))
    return out
