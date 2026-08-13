"""盘口快照序列热力图渲染（快照近似，非 L2 回放）。

输入为 raw 层 ws_public 的 orderbooks 与 trades 帧，
输出 PNG：纵轴价格、横轴时间、亮度为挂量对数，
卖侧红、买侧绿、中间价灰线、逐笔成交为亮点。
研究产物，不属产品 UI；颜色语义与设计语言一致。
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

import numpy as np
from PIL import Image

# 价格轴分箱数
PRICE_BINS = 240
# 图幅放大倍率
SCALE_X = 4
SCALE_Y = 3


def load_frames(
    data_root: Path, day: str, run_id: str | None
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """读取当日 ws_public 帧，按运行标识过滤。"""
    books: list[dict[str, object]] = []
    trades: list[dict[str, object]] = []
    path = data_root / "raw" / day / "ws_public.jsonl"
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            record = json.loads(line)
            if run_id is not None and record.get("run_id") != run_id:
                continue
            channel = record.get("channel")
            if channel == "orderbooks":
                books.append(record)
            elif channel == "trades":
                trades.append(record)
    return books, trades


def render(books: list[dict[str, object]], trades: list[dict[str, object]], out: Path) -> dict[str, object]:
    """构建价格乘时间矩阵并写 PNG。"""
    frame_count = len(books)
    prices: list[float] = []
    for record in books:
        payload = record["payload"]
        assert isinstance(payload, dict)
        for side in ("asks", "bids"):
            for level in payload[side]:
                prices.append(float(level["price"]))
    low, high = min(prices), max(prices)
    span = high - low

    def price_row(value: float) -> int:
        ratio = (high - value) / span if span > 0 else 0.5
        return min(PRICE_BINS - 1, max(0, int(ratio * (PRICE_BINS - 1))))

    ask_heat = np.zeros((PRICE_BINS, frame_count), dtype=np.float64)
    bid_heat = np.zeros((PRICE_BINS, frame_count), dtype=np.float64)
    mid_rows: list[int] = []
    stamps: list[str] = []
    for column, record in enumerate(books):
        payload = record["payload"]
        assert isinstance(payload, dict)
        for level in payload["asks"]:
            ask_heat[price_row(float(level["price"])), column] += float(level["size"])
        for level in payload["bids"]:
            bid_heat[price_row(float(level["price"])), column] += float(level["size"])
        best_ask = float(payload["asks"][0]["price"])
        best_bid = float(payload["bids"][0]["price"])
        mid_rows.append(price_row((best_ask + best_bid) / 2))
        stamps.append(str(record.get("ingest_time", "")))

    ask_norm = np.log1p(ask_heat)
    bid_norm = np.log1p(bid_heat)
    peak = max(float(ask_norm.max()), float(bid_norm.max()), 1e-9)
    ask_norm /= peak
    bid_norm /= peak

    image = np.zeros((PRICE_BINS, frame_count, 3), dtype=np.uint8)
    image[..., 0] = np.minimum(235, 8 + ask_norm * 300).astype(np.uint8)
    image[..., 1] = np.minimum(215, 8 + bid_norm * 280).astype(np.uint8)
    image[..., 2] = 10
    for column, row in enumerate(mid_rows):
        image[row, column] = (150, 160, 175)

    trade_points = 0
    for record in trades:
        payload = record["payload"]
        assert isinstance(payload, dict)
        stamp = str(record.get("ingest_time", ""))
        column = int(np.searchsorted(np.array(stamps, dtype="U40"), stamp))
        column = min(frame_count - 1, max(0, column))
        row = price_row(float(payload["price"]))
        color = (90, 235, 120) if payload.get("side") == "BUY" else (245, 110, 100)
        image[max(0, row - 1) : row + 2, column] = color
        trade_points += 1

    picture = Image.fromarray(image, "RGB").resize(
        (frame_count * SCALE_X, PRICE_BINS * SCALE_Y), Image.NEAREST
    )
    picture.save(out)
    begin = stamps[0][11:19] if stamps else ""
    end = stamps[-1][11:19] if stamps else ""
    return {
        "frames": frame_count,
        "trades": trade_points,
        "price_low": low,
        "price_high": high,
        "window_utc": f"{begin}-{end}",
        "out": str(out),
    }


def main() -> int:
    """命令行入口。"""
    parser = argparse.ArgumentParser(description="盘口快照热力图")
    parser.add_argument("--day", default=datetime.now().strftime("%Y-%m-%d"))
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--out", default="data/export/orderbook-heatmap.png")
    args = parser.parse_args()
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    books, trades = load_frames(Path("data"), args.day, args.run_id)
    if not books:
        print("无盘口帧")
        return 1
    print(json.dumps(render(books, trades, out), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
