"""归档与 K 线库同源同界校验。

依据 docs/2026-08-07-gmo-trade-print-semantics.md：
归档按交易日切割且单侧打印；日 K 开收价与归档首尾价一致；
K 线量为双侧计量，恒不小于归档撮合量。
本地数据缺失时整组跳过。
"""

from __future__ import annotations

import csv
import gzip
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "data" / "guvolu.sqlite3"
ARCHIVE = ROOT / "data" / "archive" / "trades" / "BTC"

pytestmark = pytest.mark.skipif(
    not DB.exists() or not ARCHIVE.exists(), reason="本地数据缺失"
)

# 抽验最近归档文件数
SAMPLE_FILES = 3


def _archive_files() -> list[Path]:
    return sorted(ARCHIVE.glob("*/*/*.csv.gz"))[-SAMPLE_FILES:]


def _read_archive(path: Path) -> list[tuple[str, str, str, str]]:
    with gzip.open(path, "rt", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)
        return [(ts, side, size, price) for _, side, size, price, ts in reader]


def _daily_kline(open_time_utc: str) -> tuple[str, str, str] | None:
    con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    try:
        row = con.execute(
            "select open, close, volume from kline "
            "where symbol='BTC' and interval='1day' and open_time=?",
            (open_time_utc,),
        ).fetchone()
    finally:
        con.close()
    return row


def _norm(price: str) -> Decimal:
    return Decimal(price)


@pytest.mark.parametrize("path", _archive_files(), ids=lambda p: p.stem)
def test_session_boundary_and_prices(path: Path) -> None:
    rows = _read_archive(path)
    first_dt = datetime.strptime(rows[0][0], "%Y-%m-%d %H:%M:%S.%f")
    last_dt = datetime.strptime(rows[-1][0], "%Y-%m-%d %H:%M:%S.%f")
    # 会话界与单日窗校验
    assert first_dt.hour == 21
    assert (last_dt - first_dt) < timedelta(hours=24)
    open_time = first_dt.replace(
        minute=0, second=0, microsecond=0, tzinfo=timezone.utc
    ).isoformat()
    kline = _daily_kline(open_time)
    if kline is None:
        pytest.skip("对应日 K 未入库")
    k_open, k_close, k_vol = kline
    assert _norm(rows[0][3]) == _norm(k_open)
    assert _norm(rows[-1][3]) == _norm(k_close)
    # 双侧量介于一至二倍
    arc_vol = sum(Decimal(size) for _, _, size, _ in rows)
    assert arc_vol <= Decimal(k_vol)
    assert Decimal(k_vol) <= arc_vol * Decimal("2.1")


@pytest.mark.parametrize("path", _archive_files(), ids=lambda p: p.stem)
def test_archive_single_sided(path: Path) -> None:
    rows = _read_archive(path)
    groups: dict[tuple[str, str, str], list[str]] = defaultdict(list)
    for ts, side, size, price in rows:
        groups[(ts, price, size)].append(side)
    paired = sum(1 for v in groups.values() if sorted(v) == ["BUY", "SELL"])
    assert paired == 0


def test_trading_day_column_consistency() -> None:
    con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    try:
        rows = con.execute(
            "select open_time, trading_day from kline "
            "where symbol='BTC' and interval='1day' "
            "order by open_time desc limit 30"
        ).fetchall()
    finally:
        con.close()
    assert rows
    for open_time, trading_day in rows:
        dt = datetime.fromisoformat(open_time)
        # 交易日按加三小时取日
        assert (dt + timedelta(hours=3)).strftime("%Y%m%d") == trading_day
