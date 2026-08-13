"""归档文件布局、原文拆分与覆盖统计读取。

归档是与 GMO 逐笔归档同级的 raw 不可变介质
（storage-design 第 10 节）：只落原文、只追加、不改写。
金额与价格全程原文文本，不经浮点（T-08、D-07）。
"""
from __future__ import annotations

import gzip
import csv
import json
import zipfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from guvolu.data.durable_io import atomic_write_bytes, durable_append_bytes

# 覆盖状态取值
STATUS_OK = "ok"
STATUS_MISSING = "missing"
STATUS_EMPTY = "empty"


def bitbank_day_path(data_root: Path, pair: str, day: str) -> Path:
    """bitbank 单日逐笔归档路径。"""
    return (
        data_root / "archive" / "bitbank" / "trades" / pair
        / day[:4] / f"{day}_{pair}.json.gz"
    )


def binance_aggtrade_path(data_root: Path, symbol: str, day: str) -> Path:
    """Binance 日度聚合逐笔 ZIP 的不可变路径。"""
    return (
        data_root / "archive" / "binance" / "spot" / "aggTrades" / symbol
        / day[:4] / f"{day}_{symbol}.zip"
    )


def binance_checksum_path(data_root: Path, symbol: str, day: str) -> Path:
    """Binance 官方 CHECKSUM 原件路径。"""
    return binance_aggtrade_path(data_root, symbol, day).with_suffix(
        ".zip.CHECKSUM"
    )


def bitflyer_product_dir(data_root: Path, product: str) -> Path:
    """bitFlyer 品种归档目录。"""
    return data_root / "archive" / "bitflyer" / "executions" / product


def bitflyer_day_path(data_root: Path, product: str, day: str) -> Path:
    """bitFlyer 单日逐笔归档路径。"""
    return (
        bitflyer_product_dir(data_root, product)
        / day[:4] / f"{day}_{product}.jsonl.gz"
    )


def bitflyer_cursor_path(data_root: Path, product: str) -> Path:
    """bitFlyer 回扫游标文件路径。"""
    return bitflyer_product_dir(data_root, product) / "cursor.json"


def write_gzip_atomic(path: Path, body: bytes) -> None:
    """原文压缩落盘，先写临时名再改名。"""
    atomic_write_bytes(path, gzip.compress(body))


def append_gzip_member(path: Path, body: bytes) -> None:
    """以独立 gzip 成员单次追加，读取端透明连读。"""
    # 先确认成员，再推进游标。
    durable_append_bytes(path, gzip.compress(body))


def split_json_array_items(text: str) -> list[str]:
    """按原文切分顶层 JSON 数组元素，逐元素原文返回。"""
    decoder = json.JSONDecoder()
    index = text.index("[") + 1
    items: list[str] = []
    while True:
        while index < len(text) and text[index] in " \t\r\n,":
            index += 1
        if index >= len(text):
            raise ValueError("数组未闭合")
        if text[index] == "]":
            return items
        _, end = decoder.raw_decode(text, index)
        items.append(text[index:end])
        index = end


def exec_date_day(exec_date: str) -> str:
    """exec_date 的 UTC 日，形态 YYYYMMDD。"""
    return exec_date[:10].replace("-", "")


@dataclass(frozen=True)
class BatchSplit:
    """一批逐笔按日拆分的结果。"""

    grouped: dict[str, list[str]]
    min_id: int
    min_day: str


def split_batch(row_texts: Sequence[str]) -> BatchSplit:
    """按 exec_date 的 UTC 日分组，保持批内原序。

    单遍解析，同时求批内最小 id 供游标推进。
    """
    grouped: dict[str, list[str]] = {}
    min_id: int | None = None
    for text in row_texts:
        row = json.loads(text)
        if not isinstance(row, Mapping):
            raise ValueError("逐笔元素非对象")
        day = exec_date_day(str(row["exec_date"]))
        grouped.setdefault(day, []).append(text)
        row_id = int(str(row["id"]))
        if min_id is None or row_id < min_id:
            min_id = row_id
    if min_id is None:
        raise ValueError("空批不可拆分")
    return BatchSplit(grouped, min_id, min(grouped))


def ms_to_iso(ms: int) -> str:
    """毫秒时间戳转 UTC ISO 文本，整数运算。"""
    base = datetime.fromtimestamp(ms // 1000, tz=UTC)
    return base.replace(microsecond=ms % 1000 * 1000).isoformat(
        timespec="milliseconds"
    )


@dataclass(frozen=True)
class FileStats:
    """单归档文件的覆盖统计。"""

    rows: int
    first_ts: str | None
    last_ts: str | None


def bitbank_body_stats(body_text: str) -> FileStats:
    """bitbank 单日响应的行数与时间范围。"""
    payload = json.loads(body_text)
    if not isinstance(payload, Mapping):
        raise ValueError("响应非对象")
    data = payload.get("data")
    if not isinstance(data, Mapping):
        raise ValueError("响应缺 data")
    rows = data.get("transactions")
    if not isinstance(rows, list):
        raise ValueError("响应缺 transactions")
    stamps: list[int] = []
    for row in rows:
        if isinstance(row, Mapping):
            stamps.append(int(str(row["executed_at"])))
    if not stamps:
        return FileStats(0, None, None)
    return FileStats(len(stamps), ms_to_iso(min(stamps)), ms_to_iso(max(stamps)))


def bitbank_file_stats(path: Path) -> FileStats:
    """读 bitbank 归档文件并统计。"""
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        return bitbank_body_stats(handle.read())


def bitflyer_file_stats(path: Path) -> FileStats:
    """读 bitFlyer 日文件，统计行数与时间范围。

    行序不保证（回扫批次追加），取字典序最小最大；
    exec_date 无时区后缀，按 UTC 补记（实测快照）。
    """
    rows = 0
    first: str | None = None
    last: str | None = None
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            if not isinstance(row, Mapping):
                raise ValueError("行非对象")
            stamp = str(row["exec_date"])
            rows += 1
            if first is None or stamp < first:
                first = stamp
            if last is None or stamp > last:
                last = stamp
    return FileStats(
        rows,
        f"{first}+00:00" if first is not None else None,
        f"{last}+00:00" if last is not None else None,
    )


def gmo_csv_stats(path: Path) -> FileStats:
    """读 GMO 归档 CSV，仅计行数与首尾时刻。

    首行为表头（symbol,side,size,price,timestamp）；
    时间戳为 UTC 无后缀文本，转 ISO 仅作字符替换。
    """
    rows = 0
    first_line: str | None = None
    last_line: str | None = None
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        handle.readline()
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows += 1
            if first_line is None:
                first_line = line
            last_line = line

    def stamp(line: str | None) -> str | None:
        if line is None:
            return None
        return line.rsplit(",", 1)[1].replace(" ", "T") + "+00:00"

    return FileStats(rows, stamp(first_line), stamp(last_line))


def binance_aggtrade_file_stats(path: Path) -> FileStats:
    """读 Binance aggTrades ZIP，统计无表头 CSV 的行数与时间。"""
    day = path.name[:8]
    unit = 1_000_000 if day >= "20250101" else 1_000
    rows = 0
    first: int | None = None
    last: int | None = None
    with zipfile.ZipFile(path) as archive_file:
        names = [name for name in archive_file.namelist() if name.endswith(".csv")]
        if len(names) != 1:
            raise ValueError("Binance ZIP 必须恰含一个 CSV")
        with archive_file.open(names[0], "r") as handle:
            reader = csv.reader(
                (line.decode("utf-8") for line in handle)
            )
            for row in reader:
                if len(row) != 8:
                    raise ValueError("Binance CSV 字段数非法")
                timestamp = int(row[5])
                rows += 1
                first = timestamp if first is None else min(first, timestamp)
                last = timestamp if last is None else max(last, timestamp)
    if first is None or last is None:
        return FileStats(0, None, None)

    def stamp(value: int) -> str:
        seconds, remainder = divmod(value, unit)
        micros = remainder if unit == 1_000_000 else remainder * 1_000
        return datetime.fromtimestamp(seconds, UTC).replace(
            microsecond=micros
        ).isoformat()

    return FileStats(rows, stamp(first), stamp(last))
