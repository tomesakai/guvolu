"""采集入口：K 线回补与行情录制（阶段 3，仅公开端点，零密钥）。

回补按求缺增量执行、限速自约束并对频率超限退避（错误处置册）；
录制把 WS 帧与 REST 深盘快照原样落 raw（storage-design 第 6 节层 1 与层 2）。
"""
from __future__ import annotations

import argparse
import asyncio
import calendar
import csv
import gzip
import io
import json
import re
import time
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import requests
import websockets
from websockets.asyncio.client import ClientConnection

from guvolu.api.transport import PublicTransport, RateLimiter
from guvolu.api.ws_common import SHARED_PACER, reconnect_delay_seconds, to_text
from guvolu.api.ws_public import PUBLIC_WS_URL
from guvolu.data.kline_plan import (
    MINUTE_HISTORY_START,
    YEARLY_INTERVALS,
    daily_dates,
    missing_requests,
    plan_requests,
    yearly_dates,
)
from guvolu.data.raw_writer import RawWriter, reconcile_unfinished_runs
from guvolu.data.paths import data_root
from guvolu.data.rebuild import rebuild_klines
from guvolu.data.store import connect, connect_readonly, fetched_periods, kline_counts
from guvolu.domain.enums import KlineInterval

DATA_ROOT = data_root()
# 回补限速，留退避余量
BACKFILL_RPS = 3.0
# 频率超限退避秒
RATE_BACKOFF_SECONDS = (2.0, 4.0, 8.0, 16.0)
RATE_LIMIT_CODE = "ERR-5003"
NO_DATA_CODE = "ERR-5207"

MINUTE_INTERVALS = [
    KlineInterval.MIN_1,
    KlineInterval.MIN_5,
    KlineInterval.MIN_10,
    KlineInterval.MIN_15,
    KlineInterval.MIN_30,
    KlineInterval.HOUR_1,
]


def _codes(payload: Mapping[str, object]) -> set[str]:
    messages = payload.get("messages")
    rows = messages if isinstance(messages, list) else []
    out: set[str] = set()
    for row in rows:
        if isinstance(row, Mapping):
            out.add(str(row.get("message_code", "")))
    return out


def fetch_logged(
    transport: PublicTransport,
    writer: RawWriter,
    name: str,
    path: str,
    params: dict[str, object] | None,
) -> Mapping[str, object]:
    """取完整载荷并落 raw，频率超限自动退避重试。"""
    attempt = 0
    while True:
        started = time.monotonic()
        payload = transport.get_payload(
            path, {k: str(v) for k, v in (params or {}).items()}
        )
        latency = (time.monotonic() - started) * 1000
        writer.rest(
            name, "rest_public", "GET", path, params, 200, payload, latency
        )
        if payload.get("status") == 0 or RATE_LIMIT_CODE not in _codes(payload):
            return payload
        if attempt >= len(RATE_BACKOFF_SECONDS):
            return payload
        time.sleep(RATE_BACKOFF_SECONDS[attempt])
        attempt += 1


def _all_symbols(transport: PublicTransport, writer: RawWriter) -> list[str]:
    payload = fetch_logged(transport, writer, "symbols", "/v1/symbols", None)
    data = payload.get("data")
    rows = data if isinstance(data, list) else []
    return [str(row.get("symbol")) for row in rows if isinstance(row, Mapping)]


def backfill(
    symbols_arg: str, mode: str, since: str, rps: float
) -> None:
    """K 线增量回补。mode 为 yearly 或 minute。"""
    writer = RawWriter(DATA_ROOT)
    transport = PublicTransport(RateLimiter(rps))
    conn = connect(DATA_ROOT)
    now = datetime.now(UTC)
    if symbols_arg == "all":
        symbols = _all_symbols(transport, writer)
    else:
        symbols = [part.strip() for part in symbols_arg.split(",") if part.strip()]
    if mode == "yearly":
        intervals = sorted(YEARLY_INTERVALS, key=lambda item: item.value)
        dates = yearly_dates(now)
        current = now.strftime("%Y")
    else:
        intervals = MINUTE_INTERVALS
        dates = daily_dates(since, now.strftime("%Y%m%d"))
        current = now.strftime("%Y%m%d")
    plan = plan_requests(symbols, intervals, dates)
    todo = missing_requests(plan, fetched_periods(conn), current)
    print(f"计划 {len(plan)} 请求, 求缺后 {len(todo)}")
    done = 0
    skipped = 0
    for symbol, interval, date in todo:
        payload = fetch_logged(
            transport,
            writer,
            "klines",
            "/v1/klines",
            {"symbol": symbol, "interval": interval.value, "date": date},
        )
        if payload.get("status") != 0 and NO_DATA_CODE in _codes(payload):
            skipped += 1
        done += 1
        if done % 100 == 0:
            print(f"进度 {done}/{len(todo)} 无数据跳过 {skipped}")
    stats = rebuild_klines(DATA_ROOT, conn)
    manifest = writer.finish({"backfill_mode": mode, "requests": done, "no_data": skipped, "rebuild": stats})
    print(f"完成 {done} 请求, 无数据 {skipped}, 入库新增 {stats['inserted_rows']}, 清单 {manifest}")


# 无帧静默判定秒，超时即重连
SILENCE_TIMEOUT_SECONDS = 90.0
# 心跳清单周期秒
HEARTBEAT_SECONDS = 300.0


async def record(symbol: str, minutes: float, book_interval: float) -> None:
    """录制 WS 盘口、逐笔、行情并定时拉 REST 深盘快照。

    minutes 为 0 时常驻运行：断线按退避重连并重订阅，
    静默超时强制重连，心跳仅写运行中检查点。
    """
    writer = RawWriter(DATA_ROOT)
    transport = PublicTransport(RateLimiter(2.0))
    endless = minutes <= 0
    deadline = time.monotonic() + minutes * 60.0
    frame_count = 0

    def active() -> bool:
        return endless or time.monotonic() < deadline

    async def rest_poll() -> None:
        while active():
            await asyncio.to_thread(
                fetch_logged,
                transport,
                writer,
                "orderbooks",
                "/v1/orderbooks",
                {"symbol": symbol},
            )
            await asyncio.sleep(book_interval)

    async def heartbeat() -> None:
        while active():
            await asyncio.sleep(HEARTBEAT_SECONDS)
            writer.checkpoint(
                {"record_symbol": symbol, "ws_frames": frame_count, "heartbeat": True}
            )

    async def one_session(connection: ClientConnection) -> None:
        nonlocal frame_count
        for channel in ("orderbooks", "trades", "ticker"):
            await SHARED_PACER.wait_turn()
            await connection.send(
                json.dumps(
                    {"command": "subscribe", "channel": channel, "symbol": symbol}
                )
            )
        while active():
            remain = SILENCE_TIMEOUT_SECONDS
            if not endless:
                remain = min(remain, max(1.0, deadline - time.monotonic()))
            try:
                raw = await asyncio.wait_for(connection.recv(), timeout=remain)
            except TimeoutError:
                if endless or time.monotonic() < deadline:
                    raise ConnectionError("静默超时")
                return
            # 先持久化原始帧。
            # 读取侧兼容新旧格式。
            writer.ws_frame("ws_public", to_text(raw))
            frame_count += 1

    async def ws_loop() -> None:
        attempt = 0
        while active():
            try:
                async with websockets.connect(PUBLIC_WS_URL) as connection:
                    attempt = 0
                    await one_session(connection)
            except (OSError, ConnectionError, websockets.exceptions.WebSocketException):
                attempt += 1
                await asyncio.sleep(reconnect_delay_seconds(attempt))

    tasks = [ws_loop(), rest_poll()]
    if endless:
        tasks.append(heartbeat())
    await asyncio.gather(*tasks)
    manifest = writer.finish(
        {"record_symbol": symbol, "minutes": minutes, "ws_frames": frame_count}
    )
    print(f"录制完成 {frame_count} 帧, 清单 {manifest}")



# 官方逐笔历史归档基址
ARCHIVE_BASE_URL = "https://api.coin.z.com/data/trades"
# 交易所首个成交日
ARCHIVE_START = "20180905"


def _index_names(session: requests.Session, url: str, pattern: str) -> list[str]:
    """从归档索引页提取目录名。"""
    resp = session.get(url, timeout=15)
    resp.raise_for_status()
    return re.findall(pattern, resp.text)


def _symbol_dates(
    session: requests.Session, symbol: str, since: str, until: str
) -> list[str]:
    """按索引列举该品种的可得日期，避免盲扫。"""
    out: list[str] = []
    years = _index_names(
        session, f"{ARCHIVE_BASE_URL}/{symbol}/", r'href="(\d{4})/"'
    )
    for year in sorted(years):
        months = _index_names(
            session, f"{ARCHIVE_BASE_URL}/{symbol}/{year}/", r'href="(\d{2})/"'
        )
        for month in sorted(months):
            last = calendar.monthrange(int(year), int(month))[1]
            for day in daily_dates(f"{year}{month}01", f"{year}{month}{last:02d}"):
                if since <= day <= until:
                    out.append(day)
    return out


# 归档异常登记文件名
ANOMALY_FILE_NAME = "_anomalies.jsonl"
# 交易日界偏移（D-08）
SESSION_SHIFT = timedelta(hours=3)


def archive_rows(payload: bytes) -> list[tuple[str, str, str, str, str]]:
    """解出归档行（symbol, side, size, price, timestamp）。"""
    text = gzip.decompress(payload).decode("utf-8")
    reader = csv.reader(io.StringIO(text, newline=""))
    next(reader, None)
    return [
        (row[0], row[1], row[2], row[3], row[4])
        for row in reader
        if len(row) >= 5
    ]


def archive_anomalies(
    rows: Sequence[tuple[str, str, str, str, str]],
    day_open_close: tuple[str, str] | None,
) -> list[str]:
    """归档后验两断言（口径快照第 7 节）。

    一为首尾价对照当日日 K 开收价；
    二为双侧成对组为零（每撮合一行的单侧结构）。
    日 K 未入库时跳过断言一。
    零成交空日是事实（下架与冷清品种），不属异常。
    """
    out: list[str] = []
    if not rows:
        return []
    if day_open_close is not None:
        k_open, k_close = day_open_close
        if Decimal(rows[0][3]) != Decimal(k_open):
            out.append(f"首价 {rows[0][3]} 不等于日 K 开 {k_open}")
        if Decimal(rows[-1][3]) != Decimal(k_close):
            out.append(f"尾价 {rows[-1][3]} 不等于日 K 收 {k_close}")
    groups: dict[tuple[str, str, str], set[str]] = {}
    for _, side, size, price, stamp in rows:
        groups.setdefault((stamp, price, size), set()).add(side)
    paired = sum(1 for sides in groups.values() if len(sides) > 1)
    if paired > 0:
        out.append(f"双侧成对组 {paired} 应为零")
    return out


def daily_open_close(
    data_root: Path, symbol: str, date: str
) -> tuple[str, str] | None:
    """取该交易日日 K 开收价，未入库返回空。"""
    conn = connect_readonly(data_root)
    if conn is None:
        return None
    open_time = (
        datetime.strptime(date, "%Y%m%d").replace(tzinfo=UTC) - SESSION_SHIFT
    ).isoformat()
    try:
        row = conn.execute(
            "SELECT open, close FROM kline WHERE symbol=? AND interval='1day' "
            "AND open_time=? AND revision_id=0 "
            "AND ingest_time >= available_time",
            (symbol, open_time),
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        return None
    return str(row[0]), str(row[1])


def record_archive_anomaly(
    data_root: Path, symbol: str, date: str, anomalies: Sequence[str]
) -> Path:
    """schema 异常登记：追加式记录，不静默（D-02 旁证）。"""
    target = data_root / "archive" / "trades" / ANOMALY_FILE_NAME
    target.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "symbol": symbol,
        "date": date,
        "anomalies": list(anomalies),
        "checked_at": datetime.now(UTC).isoformat(),
    }
    with target.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False) + "\n")
    return target


def verify_archive_payload(
    data_root: Path, symbol: str, date: str, payload: bytes
) -> list[str]:
    """下载入库路径的归档后验，异常登记并回报。"""
    anomalies = archive_anomalies(
        archive_rows(payload), daily_open_close(data_root, symbol, date)
    )
    if anomalies:
        record_archive_anomaly(data_root, symbol, date, anomalies)
        print(f"归档校验异常 {symbol} {date}: {'; '.join(anomalies)}")
    return anomalies


def archive_trades(symbol_arg: str, since: str, rps: float) -> None:
    """下载官方逐笔历史 CSV 归档，增量跳过已有文件。

    gz 原件即 raw：不解压不改写，缺日以 404 记为无文件。
    品种为 all 时按索引取全部品种，含已下架品种。
    """
    limiter = RateLimiter(rps)
    session = requests.Session()
    until = datetime.now(UTC).strftime("%Y%m%d")
    if symbol_arg == "all":
        symbols = _index_names(
            session, f"{ARCHIVE_BASE_URL}/", r'href="([A-Z0-9]+)/"'
        )
    else:
        symbols = [symbol_arg]
    for symbol in symbols:
        _archive_one(session, limiter, symbol, since, until)


def _archive_one(
    session: requests.Session,
    limiter: RateLimiter,
    symbol: str,
    since: str,
    until: str,
) -> None:
    out_root = DATA_ROOT / "archive" / "trades" / symbol
    dates = _symbol_dates(session, symbol, since, until)
    got = 0
    skipped = 0
    missing = 0
    for date in dates:
        target = out_root / date[:4] / date[4:6] / f"{date}_{symbol}.csv.gz"
        if target.exists():
            skipped += 1
            continue
        limiter.acquire()
        url = (
            f"{ARCHIVE_BASE_URL}/{symbol}/{date[:4]}/{date[4:6]}/"
            f"{date}_{symbol}.csv.gz"
        )
        resp = session.get(url, timeout=30)
        # 403 为当日未生成，同缺文件
        if resp.status_code in (403, 404):
            missing += 1
            continue
        resp.raise_for_status()
        target.parent.mkdir(parents=True, exist_ok=True)
        # gz 原件照常落盘，异常另行登记
        target.write_bytes(resp.content)
        verify_archive_payload(DATA_ROOT, symbol, date, resp.content)
        got += 1
        if got % 200 == 0:
            print(f"归档进度 下载{got} 已有{skipped} 无{missing}")
    print(f"归档完成 {symbol}: 下载{got} 已有{skipped} 无文件{missing}")


def main(argv: Sequence[str] | None = None) -> int:
    """命令行入口。"""
    parser = argparse.ArgumentParser(description="guvolu 采集")
    sub = parser.add_subparsers(dest="command", required=True)
    p_backfill = sub.add_parser("backfill", help="K 线增量回补")
    p_backfill.add_argument("--symbols", default="all")
    p_backfill.add_argument("--mode", choices=["yearly", "minute"], default="yearly")
    p_backfill.add_argument("--since", default=MINUTE_HISTORY_START)
    p_backfill.add_argument("--rps", type=float, default=BACKFILL_RPS)
    p_record = sub.add_parser("record", help="行情录制")
    p_record.add_argument("--symbol", default="BTC")
    p_record.add_argument("--minutes", type=float, default=12.0)
    p_record.add_argument("--book-interval", type=float, default=10.0)
    p_archive = sub.add_parser("archive", help="逐笔历史归档下载")
    p_archive.add_argument("--symbol", default="BTC", help="品种名或 all")
    p_archive.add_argument("--since", default="20180101")
    p_archive.add_argument("--rps", type=float, default=2.0)
    sub.add_parser("rebuild", help="重建 kline 表")
    sub.add_parser("stats", help="库存统计")
    p_reconcile = sub.add_parser("reconcile-raw", help="为静默旧 run 补恢复清单")
    p_reconcile.add_argument("--older-minutes", type=int, default=60)
    args = parser.parse_args(argv)
    if args.command == "backfill":
        backfill(args.symbols, args.mode, args.since, args.rps)
    elif args.command == "record":
        asyncio.run(record(args.symbol, args.minutes, args.book_interval))
    elif args.command == "archive":
        archive_trades(args.symbol, args.since, args.rps)
    elif args.command == "rebuild":
        conn = connect(DATA_ROOT)
        print(rebuild_klines(DATA_ROOT, conn))
    elif args.command == "reconcile-raw":
        paths = reconcile_unfinished_runs(DATA_ROOT, args.older_minutes)
        print(json.dumps({
            "manifests_created": len(paths),
            "paths": [str(path) for path in paths],
        }, ensure_ascii=False, indent=2))
    else:
        conn = connect(DATA_ROOT)
        for row in kline_counts(conn):
            print(row)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
