"""足迹聚合：逐笔按 bar 与价格档分桶（footprint-design 第 3、4 节）。

数据基础见口径快照（2026-08-07）：归档每撮合一行、side 为吃单方向；
WS `trades` 双侧成对打印，须按（timestamp, price, size）成对去重合一，
侧别按 tick 规则推断（上涨 BUY、下跌 SELL、平价沿用前值）。
一切数值 Decimal 字符串运算（T-08），输出全为字符串（D-07）。
按需聚合不预生成，LRU 缓存键为（文件, interval, bin）。
bar 边界按官方 K 线 openTime 对齐（2026-08-10 实测核对）：5min 至 1hour
与 4hour 为时钟对齐，1day 为 JST 06:00（21:00 UTC）对齐，逐周期锚点见
INTERVAL_ANCHOR_SECONDS；不得统一假设单一会话原点。
"""
from __future__ import annotations

import csv
import gzip
import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import ROUND_FLOOR, Decimal, InvalidOperation
from functools import lru_cache
from pathlib import Path

from guvolu.data.kline_plan import daily_dates
from guvolu.data.raw_records import ws_channel, ws_payload
from guvolu.domain.enums import Side

# 交易日界偏移秒（D-08）
SESSION_SHIFT_SECONDS = 10800
# 支持的 bar 周期与秒宽
INTERVAL_SECONDS: Mapping[str, int] = {
    "5min": 300,
    "15min": 900,
    "30min": 1800,
    "1hour": 3600,
    "4hour": 14400,
    "1day": 86400,
}
# 档位序列与目标档数区间
BIN_TIERS: tuple[int, ...] = (500, 1000, 2000, 5000, 10000)
DEFAULT_TIER = 2000
TARGET_BINS_LOW = 8
TARGET_BINS_HIGH = 20
# 价值区覆盖率，惯例常数
VALUE_AREA_RATIO = Decimal("0.70")
# 聚合缓存条目上限
CACHE_SIZE = 256
# 数据源标注
SOURCE_ARCHIVE = "archive"
SOURCE_LIVE = "live"
# 单次请求交易日上限
MAX_FOOTPRINT_DAYS = 62
# 官方锚点表（2026-08-10）
# 仅 1day 取会话原点（D-08）
# 其余周期时钟对齐
# 4hour 官方为 UTC 对齐
# 1day 官方为 06:00 JST
INTERVAL_ANCHOR_SECONDS: Mapping[str, int] = {
    "5min": 0,
    "15min": 0,
    "30min": 0,
    "1hour": 0,
    "4hour": 0,
    "1day": SESSION_SHIFT_SECONDS,
}

# 宽度缺省锚点，仅 1day 特殊
_WIDTH_ANCHOR_SECONDS: Mapping[int, int] = {
    86400: SESSION_SHIFT_SECONDS,
}

_ZERO = Decimal("0")
_HALF = Decimal("2")
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

# 逐笔打印：毫秒时戳、价、量、侧
Print = tuple[int, Decimal, Decimal, str]


@dataclass(frozen=True, slots=True)
class FootprintLevel:
    """单价格档的双侧量，附金额基准（第 4 节双基准）。"""

    price_bin: str
    sell: str
    buy: str
    sell_notional: str
    buy_notional: str


@dataclass(frozen=True, slots=True)
class FootprintBar:
    """单 bar 足迹：档阵列与汇总，数值全为字符串。

    金额基准为逐笔价乘量的 Decimal 精确累计（禁档价近似），
    delta_notional 与 total_notional 与数量基准同口径。
    """

    open_time: str
    open: str
    high: str
    low: str
    close: str
    delta: str
    total: str
    delta_notional: str
    total_notional: str
    unknown_side_count: int
    unknown_side_size: str
    unknown_side_notional: str
    poc: str | None
    vah: str | None
    val: str | None
    source: str
    levels: tuple[FootprintLevel, ...]


def _text(value: Decimal) -> str:
    return format(value, "f")


def bar_open_epoch(
    epoch_seconds: int,
    width_seconds: int,
    anchor_seconds: int | None = None,
) -> int:
    """bar 开始时刻：按周期锚点对齐官方 K 线边界。

    anchor_seconds 为 None 时按宽度缺省锚点（1day 取 JST 06:00，
    其余取 0）；周期调用方应显式传 INTERVAL_ANCHOR_SECONDS。
    """
    if anchor_seconds is None:
        anchor_seconds = _WIDTH_ANCHOR_SECONDS.get(width_seconds, 0)
    shifted = epoch_seconds + anchor_seconds
    return (
        (shifted // width_seconds) * width_seconds - anchor_seconds
    )


def price_bin_of(price: Decimal, bin_size: Decimal) -> Decimal:
    """价格档下沿：向下取整到档宽整数倍。"""
    steps = (price / bin_size).to_integral_value(rounding=ROUND_FLOOR)
    return steps * bin_size


def _value_area(
    levels: Sequence[tuple[Decimal, Decimal]], total: Decimal
) -> tuple[Decimal, Decimal, Decimal]:
    """POC 与价值区：以 POC 为中心扩张至覆盖七成。

    levels 按价格升序，元素为（档价, 双侧合计）。
    POC 取合计最大档，并列时取较低价档；
    扩张逐档比较上下相邻档，取较大者，并列向上。
    返回（poc, vah, val）。
    """
    poc_at = 0
    for at, (_, size) in enumerate(levels):
        if size > levels[poc_at][1]:
            poc_at = at
    target = total * VALUE_AREA_RATIO
    covered = levels[poc_at][1]
    low_at = poc_at
    high_at = poc_at
    while covered < target:
        below = levels[low_at - 1][1] if low_at > 0 else None
        above = levels[high_at + 1][1] if high_at + 1 < len(levels) else None
        if above is None and below is None:
            break
        if below is None or (above is not None and above >= below):
            high_at += 1
            covered += levels[high_at][1]
        else:
            low_at -= 1
            covered += levels[low_at][1]
    return levels[poc_at][0], levels[high_at][0], levels[low_at][0]


def aggregate_prints(
    prints: Sequence[Print],
    width_seconds: int,
    bin_size: Decimal,
    source: str,
    anchor_seconds: int | None = None,
) -> tuple[FootprintBar, ...]:
    """逐笔分桶聚合为足迹 bar 序列（数据层事实）。

    OHLC 计入全部打印（含零量行）；零合计档不输出。
    金额基准逐笔价乘量精确累计（第 4 节双基准）。
    """
    order: list[int] = []
    opens: dict[int, tuple[Decimal, Decimal, Decimal, Decimal]] = {}
    bins: dict[int, dict[Decimal, list[Decimal]]] = {}
    unknowns: dict[int, list[Decimal | int]] = {}
    for epoch_ms, price, size, side in prints:
        open_at = bar_open_epoch(
            epoch_ms // 1000, width_seconds, anchor_seconds
        )
        state = opens.get(open_at)
        if state is None:
            order.append(open_at)
            opens[open_at] = (price, price, price, price)
            bins[open_at] = {}
            unknowns[open_at] = [0, _ZERO, _ZERO]
        else:
            opens[open_at] = (
                state[0], max(state[1], price), min(state[2], price), price
            )
        cell = bins[open_at].setdefault(
            price_bin_of(price, bin_size), [_ZERO, _ZERO, _ZERO, _ZERO]
        )
        if side == Side.BUY.value:
            cell[1] += size
            cell[3] += price * size
        elif side == Side.SELL.value:
            cell[0] += size
            cell[2] += price * size
        else:
            unknown = unknowns[open_at]
            unknown[0] = int(unknown[0]) + 1
            unknown[1] = Decimal(unknown[1]) + size
            unknown[2] = Decimal(unknown[2]) + price * size
    out: list[FootprintBar] = []
    for open_at in sorted(order):
        o, h, low, c = opens[open_at]
        pairs = sorted(bins[open_at].items())
        kept = [
            (price, sell, buy, sell_jpy, buy_jpy)
            for price, (sell, buy, sell_jpy, buy_jpy) in pairs
            if sell + buy > _ZERO
        ]
        sell_sum = sum((row[1] for row in kept), _ZERO)
        buy_sum = sum((row[2] for row in kept), _ZERO)
        sell_jpy_sum = sum((row[3] for row in kept), _ZERO)
        buy_jpy_sum = sum((row[4] for row in kept), _ZERO)
        unknown_count, unknown_size, unknown_notional = unknowns[open_at]
        total = sell_sum + buy_sum
        poc: Decimal | None = None
        vah: Decimal | None = None
        val: Decimal | None = None
        if kept:
            poc, vah, val = _value_area(
                [(row[0], row[1] + row[2]) for row in kept], total
            )
        out.append(
            FootprintBar(
                open_time=(_EPOCH + timedelta(seconds=open_at)).isoformat(),
                open=_text(o),
                high=_text(h),
                low=_text(low),
                close=_text(c),
                delta=_text(buy_sum - sell_sum),
                total=_text(total),
                delta_notional=_text(buy_jpy_sum - sell_jpy_sum),
                total_notional=_text(sell_jpy_sum + buy_jpy_sum),
                unknown_side_count=int(unknown_count),
                unknown_side_size=_text(Decimal(unknown_size)),
                unknown_side_notional=_text(Decimal(unknown_notional)),
                poc=None if poc is None else _text(poc),
                vah=None if vah is None else _text(vah),
                val=None if val is None else _text(val),
                source=source,
                levels=tuple(
                    FootprintLevel(
                        price_bin=_text(row[0]),
                        sell=_text(row[1]),
                        buy=_text(row[2]),
                        sell_notional=_text(row[3]),
                        buy_notional=_text(row[4]),
                    )
                    for row in kept
                ),
            )
        )
    return tuple(out)


def _archive_epoch_ms(stamp: str) -> int:
    """归档时戳转毫秒：格式固定、无时区后缀、UTC。"""
    base = datetime(
        int(stamp[0:4]), int(stamp[5:7]), int(stamp[8:10]),
        int(stamp[11:13]), int(stamp[14:16]), int(stamp[17:19]),
        tzinfo=UTC,
    )
    millis = int(stamp[20:23]) if len(stamp) > 20 else 0
    return int(base.timestamp()) * 1000 + millis


def read_archive_prints(path: Path) -> list[Print]:
    """读归档 csv.gz 为逐笔序列，列序见口径快照第 1 节。"""
    out: list[Print] = []
    with gzip.open(path, "rt", encoding="utf-8", newline="") as fh:
        reader = csv.reader(fh)
        next(reader, None)
        for row in reader:
            if len(row) < 5:
                continue
            _, side, size, price, stamp = row[0], row[1], row[2], row[3], row[4]
            out.append(
                (_archive_epoch_ms(stamp), Decimal(price), Decimal(size), side)
            )
    return out


@lru_cache(maxsize=CACHE_SIZE)
def aggregate_archive_file(
    path_text: str, interval: str, bin_text: str
) -> tuple[FootprintBar, ...]:
    """单归档文件聚合，LRU 键（文件, interval, bin）。"""
    return aggregate_prints(
        read_archive_prints(Path(path_text)),
        INTERVAL_SECONDS[interval],
        Decimal(bin_text),
        SOURCE_ARCHIVE,
        INTERVAL_ANCHOR_SECONDS[interval],
    )


def archive_path(data_root: Path, symbol: str, day: str) -> Path:
    """归档文件路径，按品种与交易日定位。"""
    return (
        data_root / "archive" / "trades" / symbol
        / day[:4] / day[4:6] / f"{day}_{symbol}.csv.gz"
    )


def dedupe_ws_rows(
    rows: Iterable[tuple[str, str, str, str]],
) -> list[tuple[int, str, str]]:
    """双侧成对打印去重合一（口径快照第 2 节）。

    rows 为（timestamp, price, size, side）原文；
    同键组内撮合数取双侧行数较大者，覆盖缺侧的单行打印。
    返回按（时戳毫秒, 首见序）排序的（毫秒, 价, 量）。
    """
    groups: dict[tuple[str, str, str], list[int]] = {}
    seen = 0
    for stamp, price, size, side in rows:
        key = (stamp, price, size)
        state = groups.get(key)
        if state is None:
            groups[key] = [seen, 1 if side == Side.BUY.value else 0,
                           0 if side == Side.BUY.value else 1]
            seen += 1
        elif side == Side.BUY.value:
            state[1] += 1
        else:
            state[2] += 1
    ordered: list[tuple[int, int, str, str, int]] = []
    for (stamp, price, size), (first, buys, sells) in groups.items():
        moment = datetime.fromisoformat(stamp)
        epoch_ms = (
            int(moment.replace(microsecond=0).timestamp()) * 1000
            + moment.microsecond // 1000
        )
        ordered.append((epoch_ms, first, price, size, max(buys, sells)))
    ordered.sort(key=lambda item: (item[0], item[1]))
    out: list[tuple[int, str, str]] = []
    for epoch_ms, _, price, size, matches in ordered:
        out.extend((epoch_ms, price, size) for _ in range(matches))
    return out


def infer_sides(
    matches: Sequence[tuple[int, str, str]],
    seed_price: Decimal | None,
    seed_side: str = Side.BUY.value,
) -> list[Print]:
    """tick 规则推断侧别：上涨 BUY、下跌 SELL、平价沿用。

    无前值可比时侧别取种子侧起始（推断成分，bar 标 live）；
    增量续接时种子侧取上一批末侧保证连续。
    """
    previous = seed_price
    side = seed_side
    out: list[Print] = []
    for epoch_ms, price_text, size_text in matches:
        price = Decimal(price_text)
        if previous is not None:
            if price > previous:
                side = Side.BUY.value
            elif price < previous:
                side = Side.SELL.value
        out.append((epoch_ms, price, Decimal(size_text), side))
        previous = price
    return out


def load_ws_trade_rows(
    data_root: Path, symbol: str, day: str
) -> list[tuple[str, str, str, str]]:
    """读交易日窗内的 WS trades 帧原文。

    交易日 D 覆盖 UTC 前日 21:00 至当日 21:00，
    帧散落于两个日期目录（raw 目录按 UTC 日期）。
    """
    day_start = datetime(
        int(day[:4]), int(day[4:6]), int(day[6:8]), tzinfo=UTC
    ) - timedelta(seconds=SESSION_SHIFT_SECONDS)
    day_end = day_start + timedelta(days=1)
    out: list[tuple[str, str, str, str]] = []
    for offset in (1, 0):
        directory = (day_start + timedelta(days=1 - offset)).strftime("%Y-%m-%d")
        path = data_root / "raw" / directory / "ws_public.jsonl"
        if not path.exists():
            continue
        with path.open(encoding="utf-8") as fh:
            for line in fh:
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(record, Mapping):
                    continue
                payload = ws_payload(record)
                if payload is None or ws_channel(record, payload) != "trades":
                    continue
                if str(payload.get("symbol", "")) != symbol:
                    continue
                stamp = str(payload.get("timestamp", ""))
                try:
                    moment = datetime.fromisoformat(stamp)
                except ValueError:
                    continue
                if day_start <= moment < day_end:
                    out.append(
                        (
                            stamp,
                            str(payload.get("price", "")),
                            str(payload.get("size", "")),
                            str(payload.get("side", "")),
                        )
                    )
    return out


def _tail_blocks(path: Path, block_bytes: int) -> Iterable[bytes]:
    """自文件尾向前逐块读取，供近窗扫描。"""
    with path.open("rb") as fh:
        fh.seek(0, 2)
        end = fh.tell()
        while end > 0:
            start = max(0, end - block_bytes)
            fh.seek(start)
            yield fh.read(end - start)
            end = start


# 尾部扫描块大小字节
TAIL_BLOCK_BYTES = 262144
# 尾扫停机余量秒（入库滞后上界）
TAIL_STOP_MARGIN_SECONDS = 10
_INGEST_MARK = '"ingest_time": "'


def _line_ingest_epoch(line: str) -> float | None:
    """提取行内入库时刻秒，缺失返回空。"""
    at = line.rfind(_INGEST_MARK)
    if at < 0:
        return None
    start = at + len(_INGEST_MARK)
    end = line.find('"', start)
    if end < 0:
        return None
    try:
        return datetime.fromisoformat(line[start:end]).timestamp()
    except ValueError:
        return None


def load_recent_trade_rows(
    data_root: Path,
    symbol: str,
    seconds: int,
    now: datetime,
) -> tuple[list[tuple[str, str, str, str]], Decimal | None]:
    """读近窗 WS trades 帧原文：仅扫 raw 文件尾部。

    自当日（必要时前日）文件尾向前分块扫描，入库时刻
    早于窗口起点减余量即停；返回窗口内行与窗前种子价。
    窗口按撮合时刻过滤，种子价供侧别推断起点。
    """
    cutoff = now - timedelta(seconds=seconds)
    stop_epoch = cutoff.timestamp() - TAIL_STOP_MARGIN_SECONDS
    rows: list[tuple[str, str, str, str]] = []
    seed_price: Decimal | None = None
    seed_epoch: float | None = None

    def consume(line: str) -> bool:
        """处理一行，返回真表示可停止回扫。"""
        nonlocal seed_price, seed_epoch
        ingest = _line_ingest_epoch(line)
        if ingest is not None and ingest < stop_epoch:
            return True
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            return False
        if not isinstance(record, Mapping):
            return False
        payload = ws_payload(record)
        if payload is None or ws_channel(record, payload) != "trades":
            return False
        if str(payload.get("symbol", "")) != symbol:
            return False
        stamp = str(payload.get("timestamp", ""))
        try:
            moment = datetime.fromisoformat(stamp)
        except ValueError:
            return False
        if moment >= cutoff:
            rows.append(
                (
                    stamp,
                    str(payload.get("price", "")),
                    str(payload.get("size", "")),
                    str(payload.get("side", "")),
                )
            )
        elif seed_epoch is None or moment.timestamp() > seed_epoch:
            # 窗前最近一笔作侧别种子
            seed_epoch = moment.timestamp()
            try:
                seed_price = Decimal(str(payload.get("price", "")))
            except InvalidOperation:
                seed_price = None
        return False

    done = False
    for offset in (0, 1):
        if done:
            break
        day = (now - timedelta(days=offset)).strftime("%Y-%m-%d")
        path = data_root / "raw" / day / "ws_public.jsonl"
        if not path.exists():
            continue
        carry = b""
        for block in _tail_blocks(path, TAIL_BLOCK_BYTES):
            merged = block + carry
            pieces = merged.split(b"\n")
            # 块首不完整行进位到更前块
            carry = pieces[0]
            for raw_line in reversed(pieces[1:]):
                if raw_line.strip() and consume(
                    raw_line.decode("utf-8", errors="replace")
                ):
                    done = True
                    break
            if done:
                break
        if not done and carry.strip():
            # 文件首行无前继，同样处理
            consume(carry.decode("utf-8", errors="replace"))
    rows.reverse()
    return rows, seed_price


def choose_tier(bar_ranges: Sequence[Decimal], tick_size: Decimal) -> int:
    """自动档位：bar 范围中位落 8 至 20 档的最小档位。

    全部档位仍超 20 档时取最大档位；范围过窄取最小档位；
    无样本时取缺省档位。
    """
    ranges = sorted(value for value in bar_ranges if value > _ZERO)
    if not ranges:
        return DEFAULT_TIER
    middle = len(ranges) // 2
    if len(ranges) % 2 == 1:
        median = ranges[middle]
    else:
        median = (ranges[middle - 1] + ranges[middle]) / _HALF
    for tier in BIN_TIERS:
        if median / (tick_size * tier) <= TARGET_BINS_HIGH:
            return tier
    return BIN_TIERS[-1]


def _live_bars(
    data_root: Path,
    symbol: str,
    day: str,
    interval: str,
    width_seconds: int,
    bin_size: Decimal,
    seed_price: Decimal | None,
) -> tuple[FootprintBar, ...]:
    """当期 bar：录制流去重推断后聚合，标 live。"""
    rows = load_ws_trade_rows(data_root, symbol, day)
    if not rows:
        return ()
    prints = infer_sides(dedupe_ws_rows(rows), seed_price)
    return aggregate_prints(
        prints,
        width_seconds,
        bin_size,
        SOURCE_LIVE,
        INTERVAL_ANCHOR_SECONDS[interval],
    )


def _day_bars(
    data_root: Path,
    symbol: str,
    days: Sequence[str],
    today: str,
    interval: str,
    bin_size: Decimal,
) -> list[FootprintBar]:
    """逐交易日取 bar：昨日及以前走归档，当日走录制。"""
    bin_text = _text(bin_size)
    width = INTERVAL_SECONDS[interval]
    out: list[FootprintBar] = []
    for day in days:
        if day < today:
            path = archive_path(data_root, symbol, day)
            if path.exists():
                out.extend(aggregate_archive_file(str(path), interval, bin_text))
        elif day == today:
            seed = Decimal(out[-1].close) if out else None
            out.extend(
                _live_bars(
                    data_root, symbol, day, interval, width, bin_size, seed
                )
            )
    return out


def build_footprint(
    data_root: Path,
    symbol: str,
    interval: str,
    from_day: str,
    to_day: str,
    bin_arg: str,
    tick_size: str,
    today: str,
) -> dict[str, object]:
    """构建足迹响应：逐 bar 档阵列加汇总与元信息。

    bin_arg 为 auto 或档位数值；档宽 = tickSize 乘档位。
    纯本地读取（归档与 raw 录制），零上游调用。
    """
    tick = Decimal(tick_size)
    end = min(to_day, today)
    days = daily_dates(from_day, end) if from_day <= end else []
    truncated = len(days) > MAX_FOOTPRINT_DAYS
    days = days[-MAX_FOOTPRINT_DAYS:]
    auto = bin_arg == "auto"
    if auto:
        probe = tick * DEFAULT_TIER
        probe_bars = _day_bars(data_root, symbol, days, today, interval, probe)
        ranges = [
            Decimal(bar.high) - Decimal(bar.low) for bar in probe_bars
        ]
        tier = choose_tier(ranges, tick)
        bars = (
            probe_bars
            if tier == DEFAULT_TIER
            else _day_bars(data_root, symbol, days, today, interval, tick * tier)
        )
    else:
        tier = int(bin_arg)
        bars = _day_bars(data_root, symbol, days, today, interval, tick * tier)
    unknown_side_count = sum(bar.unknown_side_count for bar in bars)
    return {
        "bars": bars,
        "meta": {
            "symbol": symbol,
            "interval": interval,
            "from": days[0] if days else from_day,
            "to": end,
            "today": today,
            "bin": _text(tick * tier),
            "tier": tier,
            "auto": auto,
            "truncated": truncated,
            "coverage_clipped": truncated,
            "side_basis": "by_bar_source:archive_taker|live_tick_rule_inference",
            "unknown_side_count": unknown_side_count,
            "coverage_from": bars[0].open_time if bars else None,
            "coverage_to": bars[-1].open_time if bars else None,
        },
    }
