"""盘口快照热力矩阵构建（纯函数，快照近似）。

时间按固定桶分箱，**无帧的桶显式标记空档**（不插值）；
价格按观测区间等分，强度为挂量对数归一到 0 至 255。
"""
from __future__ import annotations

import json
import math
from collections import deque
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path

from guvolu.data.raw_records import ws_channel, ws_payload

# 时间桶秒数
BIN_SECONDS = 5.0
# 价格分箱数
PRICE_ROWS = 64
# 尾部扫描行数上限
TAIL_LINES = 20000


def load_recent_book_frames(
    data_root: Path, symbol: str, minutes: float, now: datetime
) -> list[Mapping[str, object]]:
    """读取近期盘口帧：今日与昨日文件尾部。"""
    frames: deque[str] = deque(maxlen=TAIL_LINES)
    for offset in (1, 0):
        day = (now - timedelta(days=offset)).strftime("%Y-%m-%d")
        path = data_root / "raw" / day / "ws_public.jsonl"
        if not path.exists():
            continue
        with path.open(encoding="utf-8") as fh:
            frames.extend(fh)
    cutoff = now - timedelta(minutes=minutes)
    out: list[Mapping[str, object]] = []
    for line in frames:
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(record, Mapping):
            continue
        payload = ws_payload(record)
        if payload is None or ws_channel(record, payload) != "orderbooks":
            continue
        if str(payload.get("symbol", "")) != symbol:
            continue
        stamp = datetime.fromisoformat(str(record.get("ingest_time")))
        if stamp >= cutoff:
            out.append({"time": stamp, "payload": payload})
    return out


def build_heatmap(
    frames: list[Mapping[str, object]], minutes: float, now: datetime
) -> dict[str, object]:
    """构建热力矩阵。列为时间桶，空档列 gap 为真。"""
    cols = max(1, int(minutes * 60.0 / BIN_SECONDS))
    start = now - timedelta(minutes=minutes)
    prices: list[float] = []
    for frame in frames:
        payload = frame["payload"]
        assert isinstance(payload, Mapping)
        for side in ("asks", "bids"):
            levels = payload.get(side)
            if isinstance(levels, list):
                for level in levels:
                    prices.append(float(level["price"]))
    if not prices:
        return {
            "rows": PRICE_ROWS,
            "cols": [],
            "price_low": None,
            "price_high": None,
            "ask": [],
            "bid": [],
            "mid_row": [],
        }
    low, high = min(prices), max(prices)
    span = high - low or 1.0

    def row_of(value: float) -> int:
        ratio = (high - value) / span
        return min(PRICE_ROWS - 1, max(0, int(ratio * (PRICE_ROWS - 1))))

    ask = [[0.0] * PRICE_ROWS for _ in range(cols)]
    bid = [[0.0] * PRICE_ROWS for _ in range(cols)]
    hit = [0] * cols
    mid_sum = [0.0] * cols
    for frame in frames:
        stamp = frame["time"]
        assert isinstance(stamp, datetime)
        col = int((stamp - start).total_seconds() / BIN_SECONDS)
        if col < 0 or col >= cols:
            continue
        payload = frame["payload"]
        assert isinstance(payload, Mapping)
        asks = payload.get("asks")
        bids = payload.get("bids")
        if not (isinstance(asks, list) and isinstance(bids, list)):
            continue
        for level in asks:
            ask[col][row_of(float(level["price"]))] += float(level["size"])
        for level in bids:
            bid[col][row_of(float(level["price"]))] += float(level["size"])
        best_ask = float(asks[0]["price"])
        best_bid = float(bids[0]["price"])
        mid_sum[col] += (best_ask + best_bid) / 2
        hit[col] += 1

    peak = 1e-9
    for grid in (ask, bid):
        for column in grid:
            for value in column:
                peak = max(peak, math.log1p(value))

    def normalize(grid: list[list[float]]) -> list[list[int]]:
        return [
            [int(math.log1p(value) / peak * 255) for value in column]
            for column in grid
        ]

    col_meta: list[dict[str, object]] = []
    mid_row: list[int | None] = []
    for index in range(cols):
        stamp = start + timedelta(seconds=index * BIN_SECONDS)
        gap = hit[index] == 0
        col_meta.append(
            {"t": stamp.astimezone(UTC).isoformat(), "gap": gap}
        )
        if gap:
            mid_row.append(None)
        else:
            mid_row.append(row_of(mid_sum[index] / hit[index]))
    return {
        "rows": PRICE_ROWS,
        "cols": col_meta,
        "price_low": format(low, "f"),
        "price_high": format(high, "f"),
        "ask": normalize(ask),
        "bid": normalize(bid),
        "mid_row": mid_row,
    }
