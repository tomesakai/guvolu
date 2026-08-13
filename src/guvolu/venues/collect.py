"""多源逐笔采集入口（TBD-23 序 1 与序 3 提案实施）。

命令：backfill-bitflyer、backfill-bitbank、scan-gmo-archive、
init-dims、coverage-stats。全部只读公开端点，零密钥。
归档为不可变介质原文落盘；SQLite 仅承载维度与覆盖登记。
崩溃窗口内可能产生重复行：raw 不去重（storage-design 第 4 节），
去重为 normalized 重建职责。
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import sqlite3
import sys
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path

from guvolu.data import store
from guvolu.data.durable_io import atomic_write_bytes, atomic_write_text
from guvolu.data.raw_writer import RawWriter
from guvolu.data.paths import data_root
from guvolu.domain.ids import new_run_id
from guvolu.venues import archive, registry
from guvolu.venues.base import VenueRequestError, Window, window_days
from guvolu.venues.bitbank import BitbankPublicSource
from guvolu.venues.bitbank import (
    PUBLIC_RATE_PER_SECOND as BITBANK_RATE_PER_SECOND,
)
from guvolu.venues.bitflyer import BitflyerPublicSource
from guvolu.venues.bitflyer import (
    PUBLIC_RATE_PER_SECOND as BITFLYER_RATE_PER_SECOND,
)
from guvolu.venues.binance import (
    ARCHIVE_RATE_PER_SECOND as BINANCE_ARCHIVE_RATE_PER_SECOND,
)
from guvolu.venues.binance import BinanceArchiveSource
from guvolu.venues.bitbank_stream import record_public as record_bitbank_public
from guvolu.venues.coincheck import record_public as record_coincheck_public
from guvolu.venues.ratelimit import FixedRateLimiter

DATA_ROOT = data_root()
# 进度行间隔请求数
PROGRESS_EVERY = 100
# 覆盖登记的数据域
DOMAIN_TRADE = "trade"
# 扫描器批量写行数
SCAN_BATCH_ROWS = 500
# 扫描器进度行间隔文件数
SCAN_PROGRESS_EVERY = 2000


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _yesterday() -> str:
    return (datetime.now(UTC) - timedelta(days=1)).strftime("%Y%m%d")


def backfill_bitbank(
    pairs: Sequence[str],
    since: str | None,
    until: str | None,
    rps: float,
    recheck_missing: bool,
) -> None:
    """按日全量回补 bitbank 逐笔，断点续传。"""
    conn = store.connect(DATA_ROOT)
    registry.register_all(conn)
    source = BitbankPublicSource(FixedRateLimiter(rps))
    for pair in pairs:
        start = since if since is not None else (
            registry.BITBANK_LISTING_START.get(pair)
        )
        if start is None:
            raise SystemExit(f"{pair} 无实测起点，须指定 --since")
        end = until if until is not None else _yesterday()
        _backfill_bitbank_pair(conn, source, pair, start, end, recheck_missing)


def _binance_archive_rows(body: bytes) -> int:
    """统计 ZIP 内 CSV 的数据行数，保留 ZIP 原件不改写。"""
    import io
    import zipfile

    with zipfile.ZipFile(io.BytesIO(body)) as archive_file:
        names = [name for name in archive_file.namelist() if name.endswith(".csv")]
        if len(names) != 1:
            raise ValueError("Binance ZIP 必须恰含一个 CSV")
        with archive_file.open(names[0]) as handle:
            rows = sum(1 for _ in handle)
    return rows


def backfill_binance(
    symbols: Sequence[str], since: str, until: str, rps: float
) -> None:
    """按日下载校验 Binance aggTrades 归档并登记覆盖。"""
    conn = store.connect(DATA_ROOT)
    registry.register_all(conn)
    source = BinanceArchiveSource(FixedRateLimiter(rps))
    days = window_days(Window(since, until))
    for symbol in symbols:
        started = _now_iso()
        ok = 0
        missing = 0
        checksum_failures = 0
        rows = 0
        failure: str | None = None
        try:
            for day in days:
                target = archive.binance_aggtrade_path(DATA_ROOT, symbol, day)
                checksum_target = archive.binance_checksum_path(
                    DATA_ROOT, symbol, day
                )
                if target.exists() and checksum_target.exists():
                    expected = checksum_target.read_text(
                        encoding="utf-8"
                    ).strip().split()[0].lower()
                    actual = hashlib.sha256(target.read_bytes()).hexdigest()
                    if actual != expected:
                        checksum_failures += 1
                        raise ValueError(f"{symbol} {day} 本地 CHECKSUM 不匹配")
                    stats = archive.binance_aggtrade_file_stats(target)
                    rows += stats.rows
                    ok += 1
                    store.upsert_coverage(conn, [(
                        "binance", symbol, DOMAIN_TRADE, day, stats.rows,
                        stats.first_ts, stats.last_ts,
                        archive.STATUS_OK, _now_iso(),
                    )])
                    continue
                result = source.fetch_day(symbol, day)
                if result.archive.http_status in (403, 404):
                    missing += 1
                    store.upsert_coverage(conn, [(
                        "binance", symbol, DOMAIN_TRADE, day, None, None,
                        None, archive.STATUS_MISSING, _now_iso(),
                    )])
                    continue
                if result.archive.http_status != 200 or result.checksum.http_status != 200:
                    raise VenueRequestError(
                        "binance", result.archive.url,
                        f"ZIP {result.archive.http_status}; CHECKSUM {result.checksum.http_status}",
                    )
                if not result.verify():
                    checksum_failures += 1
                    raise ValueError(f"{symbol} {day} CHECKSUM 不匹配")
                atomic_write_bytes(target, result.archive.body)
                atomic_write_bytes(checksum_target, result.checksum.body)
                stats = archive.binance_aggtrade_file_stats(target)
                rows += stats.rows
                ok += 1
                store.upsert_coverage(conn, [(
                    "binance", symbol, DOMAIN_TRADE, day, stats.rows,
                    stats.first_ts, stats.last_ts,
                    archive.STATUS_OK, _now_iso(),
                )])
        except (OSError, ValueError, VenueRequestError) as exc:
            failure = str(exc)
        status = "complete" if failure is None else "failed"
        run_row: store.BackfillRunRow = (
            f"binance-{new_run_id()}", "binance", symbol, DOMAIN_TRADE,
            since, until, len(days), ok, missing, 0, rows, checksum_failures,
            status, failure, started, _now_iso(),
            "binance-archive-v1", "working-tree",
        )
        store.insert_backfill_run(conn, run_row)
        if failure is not None:
            raise RuntimeError(failure)


async def record_coincheck(pairs: Sequence[str], seconds: float) -> None:
    """录制 Coincheck 公共旁路流并写运行清单。"""
    writer = RawWriter(DATA_ROOT)
    frames = await record_coincheck_public(writer, pairs, seconds)
    manifest = writer.finish({"venue": "coincheck", "frames": frames})
    print(f"coincheck 帧{frames} 清单 {manifest}", flush=True)


async def record_bitbank(pairs: Sequence[str], seconds: float) -> None:
    """录制 bitbank Socket.IO 公开流并写运行清单。"""
    writer = RawWriter(DATA_ROOT)
    frames = await record_bitbank_public(writer, pairs, seconds)
    manifest = writer.finish({"venue": "bitbank", "frames": frames})
    print(f"bitbank 帧{frames} 清单 {manifest}", flush=True)


def _backfill_bitbank_pair(
    conn: sqlite3.Connection,
    source: BitbankPublicSource,
    pair: str,
    start: str,
    end: str,
    recheck_missing: bool,
) -> None:
    covered = store.coverage_days(conn, "bitbank", pair, DOMAIN_TRADE)
    requests = 0
    written = 0
    missing = 0
    empty = 0
    skipped = 0
    days = window_days(Window(start, end))
    print(
        f"bitbank {pair} 回扫 {start} 至 {end} 共 {len(days)} 日",
        flush=True,
    )
    for day in days:
        path = archive.bitbank_day_path(DATA_ROOT, pair, day)
        status = covered.get(day)
        if path.exists():
            if status not in (archive.STATUS_OK, archive.STATUS_EMPTY):
                # 补登既有文件的覆盖行
                stats = archive.bitbank_file_stats(path)
                _register_bitbank_day(conn, pair, day, stats)
            skipped += 1
            continue
        if (
            status in (archive.STATUS_MISSING, archive.STATUS_EMPTY)
            and not recheck_missing
        ):
            skipped += 1
            continue
        result = source.fetch_day(pair, day)
        requests += 1
        if result.http_status == 404:
            missing += 1
            store.upsert_coverage(
                conn,
                [(
                    "bitbank", pair, DOMAIN_TRADE, day,
                    None, None, None, archive.STATUS_MISSING, _now_iso(),
                )],
            )
        elif result.http_status == 200:
            stats = archive.bitbank_body_stats(result.text())
            archive.write_gzip_atomic(path, result.body)
            written += 1
            if stats.rows == 0:
                empty += 1
            _register_bitbank_day(conn, pair, day, stats)
        else:
            raise VenueRequestError(
                "bitbank", f"/{pair}/transactions/{day}",
                f"HTTP {result.http_status}",
            )
        if requests % PROGRESS_EVERY == 0:
            print(
                f"bitbank {pair} 进度 请求{requests} 落盘{written}"
                f" 缺失{missing} 空日{empty} 跳过{skipped} 当前日{day}",
                flush=True,
            )
    print(
        f"bitbank {pair} 完成 请求{requests} 落盘{written}"
        f" 缺失{missing} 空日{empty} 跳过{skipped}",
        flush=True,
    )


def _register_bitbank_day(
    conn: sqlite3.Connection, pair: str, day: str, stats: archive.FileStats
) -> None:
    status = archive.STATUS_OK if stats.rows else archive.STATUS_EMPTY
    store.upsert_coverage(
        conn,
        [(
            "bitbank", pair, DOMAIN_TRADE, day,
            stats.rows, stats.first_ts, stats.last_ts, status, _now_iso(),
        )],
    )


def _load_cursor(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    loaded = json.loads(path.read_text(encoding="utf-8"))
    return loaded if isinstance(loaded, dict) else None


def _save_cursor(path: Path, state: dict[str, object]) -> None:
    # 先确认成员，再原子推进游标。
    durable_state = {"durability_version": "member-before-cursor-v1", **state}
    atomic_write_text(
        path, json.dumps(durable_state, ensure_ascii=False, indent=2) + "\n"
    )


def backfill_bitflyer(
    products: Sequence[str], rps: float, restart: bool
) -> None:
    """游标回扫 bitFlyer 逐笔至 31 天边界。"""
    conn = store.connect(DATA_ROOT)
    registry.register_all(conn)
    source = BitflyerPublicSource(FixedRateLimiter(rps))
    for product in products:
        _backfill_bitflyer_product(conn, source, product, restart)


def _backfill_bitflyer_product(
    conn: sqlite3.Connection,
    source: BitflyerPublicSource,
    product: str,
    restart: bool,
) -> None:
    cursor_path = archive.bitflyer_cursor_path(DATA_ROOT, product)
    cursor = None if restart else _load_cursor(cursor_path)
    if cursor is not None and cursor.get("completed"):
        print(f"bitflyer {product} 已完结，仅补登覆盖", flush=True)
        _heal_bitflyer_coverage(conn, product)
        return
    before: int | None = None
    requests = 0
    rows_total = 0
    started_at = _now_iso()
    if cursor is not None:
        raw_before = cursor.get("before")
        if raw_before is not None:
            before = int(str(raw_before))
        requests = int(str(cursor.get("requests", 0)))
        rows_total = int(str(cursor.get("rows_total", 0)))
        started_at = str(cursor.get("started_at", started_at))
        print(f"bitflyer {product} 续扫 游标 {before}", flush=True)
    else:
        print(f"bitflyer {product} 自最新开始回扫", flush=True)
    touched: set[str] = set()
    closed: set[str] = set()
    ended_by = "boundary"
    while True:
        page = source.fetch_executions_page(product, before)
        requests += 1
        if page.is_boundary():
            break
        if page.result.http_status != 200:
            raise VenueRequestError(
                "bitflyer", "/v1/executions",
                f"HTTP {page.result.http_status}",
            )
        items = page.rows_text()
        if not items:
            ended_by = "empty"
            break
        batch = archive.split_batch(items)
        for day, texts in batch.grouped.items():
            body = ("\n".join(texts) + "\n").encode("utf-8")
            archive.append_gzip_member(
                archive.bitflyer_day_path(DATA_ROOT, product, day), body
            )
            touched.add(day)
        rows_total += len(items)
        for day in sorted(touched - closed):
            if day > batch.min_day:
                _finalize_bitflyer_day(conn, product, day)
                closed.add(day)
        before = batch.min_id
        _save_cursor(
            cursor_path,
            {
                "product": product,
                "before": before,
                "completed": False,
                "requests": requests,
                "rows_total": rows_total,
                "started_at": started_at,
                "updated_at": _now_iso(),
            },
        )
        if requests % PROGRESS_EVERY == 0:
            print(
                f"bitflyer {product} 进度 请求{requests} 累计行{rows_total}"
                f" 文件{len(touched)} 缺失0 游标日{batch.min_day}",
                flush=True,
            )
    _save_cursor(
        cursor_path,
        {
            "product": product,
            "before": before,
            "completed": True,
            "ended_by": ended_by,
            "requests": requests,
            "rows_total": rows_total,
            "started_at": started_at,
            "updated_at": _now_iso(),
        },
    )
    _heal_bitflyer_coverage(conn, product)
    print(
        f"bitflyer {product} 完成 请求{requests} 累计行{rows_total}"
        f" 边界{ended_by}",
        flush=True,
    )


def _finalize_bitflyer_day(
    conn: sqlite3.Connection, product: str, day: str
) -> None:
    path = archive.bitflyer_day_path(DATA_ROOT, product, day)
    if not path.exists():
        return
    stats = archive.bitflyer_file_stats(path)
    status = archive.STATUS_OK if stats.rows else archive.STATUS_EMPTY
    store.upsert_coverage(
        conn,
        [(
            "bitflyer", product, DOMAIN_TRADE, day,
            stats.rows, stats.first_ts, stats.last_ts, status, _now_iso(),
        )],
    )


def _heal_bitflyer_coverage(conn: sqlite3.Connection, product: str) -> None:
    """以文件为准重登全部覆盖，容断点与崩溃窗。"""
    product_dir = archive.bitflyer_product_dir(DATA_ROOT, product)
    if not product_dir.exists():
        return
    for path in sorted(product_dir.rglob(f"*_{product}.jsonl.gz")):
        day = path.name.split("_", 1)[0]
        _finalize_bitflyer_day(conn, product, day)


def scan_gmo_archive(rescan: bool) -> None:
    """一次性登记 GMO 既有归档覆盖，只读头尾与行数。"""
    conn = store.connect(DATA_ROOT)
    registry.register_all(conn)
    root = DATA_ROOT / "archive" / "trades"
    if not root.exists():
        print("无 GMO 归档目录", flush=True)
        return
    scanned = 0
    skipped = 0
    batch: list[store.CoverageRow] = []
    for symbol_dir in sorted(root.iterdir()):
        if not symbol_dir.is_dir():
            continue
        symbol = symbol_dir.name
        covered = store.coverage_days(conn, "gmo", symbol, DOMAIN_TRADE)
        for path in sorted(symbol_dir.rglob(f"*_{symbol}.csv.gz")):
            day = path.name.split("_", 1)[0]
            if not rescan and day in covered:
                skipped += 1
                continue
            stats = archive.gmo_csv_stats(path)
            status = archive.STATUS_OK if stats.rows else archive.STATUS_EMPTY
            batch.append((
                "gmo", symbol, DOMAIN_TRADE, day,
                stats.rows, stats.first_ts, stats.last_ts, status, _now_iso(),
            ))
            scanned += 1
            if len(batch) >= SCAN_BATCH_ROWS:
                store.upsert_coverage(conn, batch)
                batch = []
            if scanned % SCAN_PROGRESS_EVERY == 0:
                print(
                    f"gmo 归档扫描 {scanned} 跳过 {skipped}"
                    f" 当前 {symbol} {day}",
                    flush=True,
                )
    if batch:
        store.upsert_coverage(conn, batch)
    print(f"gmo 归档扫描完成 登记{scanned} 跳过{skipped}", flush=True)


def print_coverage_stats() -> None:
    """打印覆盖汇总。"""
    conn = store.connect(DATA_ROOT)
    header = (
        "venue", "symbol", "domain", "days", "ok", "missing",
        "empty", "min_day", "max_day", "rows",
    )
    print(" ".join(header), flush=True)
    for row in store.coverage_summary(conn):
        print(" ".join(str(cell) for cell in row), flush=True)


def main(argv: Sequence[str] | None = None) -> int:
    """命令行入口。"""
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    parser = argparse.ArgumentParser(description="guvolu 多源逐笔采集")
    sub = parser.add_subparsers(dest="command", required=True)
    p_bb = sub.add_parser("backfill-bitbank", help="bitbank 按日回补")
    p_bb.add_argument("--pair", default="btc_jpy", help="逗号分隔品种")
    p_bb.add_argument("--since", default=None, help="起始日，缺省为实测起点")
    p_bb.add_argument("--until", default=None, help="终止日，缺省为昨日")
    p_bb.add_argument("--rps", type=float, default=BITBANK_RATE_PER_SECOND)
    p_bb.add_argument("--recheck-missing", action="store_true")
    p_bf = sub.add_parser("backfill-bitflyer", help="bitFlyer 游标回扫")
    p_bf.add_argument("--product", default="BTC_JPY", help="逗号分隔品种")
    p_bf.add_argument("--rps", type=float, default=BITFLYER_RATE_PER_SECOND)
    p_bf.add_argument("--restart", action="store_true")
    p_bn = sub.add_parser("backfill-binance", help="Binance 校验归档回补")
    p_bn.add_argument("--symbol", default="BTCUSDT", help="逗号分隔品种")
    p_bn.add_argument("--since", required=True, help="起始日 YYYYMMDD")
    p_bn.add_argument("--until", required=True, help="终止日 YYYYMMDD")
    p_bn.add_argument("--rps", type=float, default=BINANCE_ARCHIVE_RATE_PER_SECOND)
    p_cc = sub.add_parser("record-coincheck", help="Coincheck 实时旁路录制")
    p_cc.add_argument("--pair", default="btc_jpy", help="逗号分隔品种")
    p_cc.add_argument("--seconds", type=float, default=60.0)
    p_bs = sub.add_parser("record-bitbank", help="bitbank 实时旁路录制")
    p_bs.add_argument("--pair", default="btc_jpy", help="逗号分隔品种")
    p_bs.add_argument("--seconds", type=float, default=60.0)
    p_project = sub.add_parser("project-trades", help="归档逐笔投影到事实表")
    p_project.add_argument("--venue", default="gmo,bitbank,bitflyer")
    p_project.add_argument("--since", default=None, help="起始日 YYYYMMDD")
    p_project.add_argument("--until", default=None, help="终止日 YYYYMMDD")
    p_project.add_argument("--max-partitions", type=int, default=None)
    p_project.add_argument("--full", action="store_true", help="明确执行全量投影")
    p_project.add_argument("--force", action="store_true")
    sub.add_parser("project-recorded", help="投影已录制的旁路流")
    p_scan = sub.add_parser("scan-gmo-archive", help="GMO 归档覆盖登记")
    p_scan.add_argument("--rescan", action="store_true")
    sub.add_parser("init-dims", help="维度表登记")
    sub.add_parser("coverage-stats", help="覆盖汇总")
    args = parser.parse_args(argv)
    if args.command == "backfill-bitbank":
        pairs = [p.strip() for p in str(args.pair).split(",") if p.strip()]
        backfill_bitbank(
            pairs, args.since, args.until, args.rps, args.recheck_missing
        )
    elif args.command == "backfill-bitflyer":
        products = [
            p.strip() for p in str(args.product).split(",") if p.strip()
        ]
        backfill_bitflyer(products, args.rps, args.restart)
    elif args.command == "backfill-binance":
        symbols = [
            symbol.strip().upper() for symbol in str(args.symbol).split(",")
            if symbol.strip()
        ]
        backfill_binance(symbols, args.since, args.until, args.rps)
    elif args.command == "record-coincheck":
        pairs = [
            pair.strip().lower() for pair in str(args.pair).split(",")
            if pair.strip()
        ]
        asyncio.run(record_coincheck(pairs, args.seconds))
    elif args.command == "record-bitbank":
        pairs = [
            pair.strip().lower() for pair in str(args.pair).split(",")
            if pair.strip()
        ]
        asyncio.run(record_bitbank(pairs, args.seconds))
    elif args.command == "project-trades":
        from guvolu.data.projection import (
            project_trade_archives,
            validate_trade_projection,
        )

        venues = [
            venue.strip() for venue in str(args.venue).split(",")
            if venue.strip()
        ]
        if args.max_partitions is None and not args.full:
            raise SystemExit("须指定 --max-partitions 或显式确认 --full")
        conn = store.connect(DATA_ROOT)
        stats = project_trade_archives(
            DATA_ROOT, conn, venue_ids=venues, from_day=args.since,
            to_day=args.until, max_partitions=args.max_partitions,
            force=args.force,
        )
        print(json.dumps({
            "projection": stats.as_dict(),
            "validation": validate_trade_projection(conn, venues).as_dict(),
        }, ensure_ascii=False), flush=True)
    elif args.command == "project-recorded":
        from guvolu.data.projection import (
            project_recorded_books,
            project_recorded_trades,
        )

        conn = store.connect(DATA_ROOT)
        books = project_recorded_books(DATA_ROOT, conn)
        trades = project_recorded_trades(DATA_ROOT, conn)
        print(json.dumps({
            "books": books.as_dict(), "trades": trades.as_dict(),
        }, ensure_ascii=False), flush=True)
    elif args.command == "scan-gmo-archive":
        scan_gmo_archive(args.rescan)
    elif args.command == "init-dims":
        conn = store.connect(DATA_ROOT)
        inserted = registry.register_all(conn)
        print(f"维度登记新增 {inserted} 行", flush=True)
    else:
        print_coverage_stats()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
