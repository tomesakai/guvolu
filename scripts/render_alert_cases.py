"""判定案例盘口热力渲染（快照近似，研究产物）。

沿 render_orderbook_heatmap.py 画法基线：纵轴价格、横轴逐帧、
亮度为挂量对数、卖侧红买侧绿、中间价灰线、逐笔成交为亮点；
本脚本增加判定区域描边框（价带乘时窗）与文字标注，供人眼
核验判定形态。案例清单以 JSON 规格文件传入，只读数据。
"""
from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
from PIL import Image, ImageDraw

# 价格轴分箱数
PRICE_BINS = 400
# 纵向放大倍率
SCALE_Y = 2
# 横向目标像素
TARGET_WIDTH = 6000
# 描边灰白色
BOX_COLOR = (225, 228, 235)
# 文字色
TEXT_COLOR = (235, 238, 245)


def epoch_ms_of(stamp: str) -> int:
    """时戳文本转毫秒。"""
    moment = datetime.fromisoformat(stamp)
    return (
        int(moment.replace(microsecond=0).timestamp()) * 1000
        + moment.microsecond // 1000
    )


def load_day(
    path: Path, symbol: str, ranges: list[tuple[int, int]]
) -> tuple[list, list]:
    """读当日帧与逐笔，仅保留案例时窗。"""
    frames: list[tuple[int, list, list]] = []
    seen: set[str] = set()
    trade_groups: dict[tuple[str, str, str], list[int]] = {}
    trade_seen = 0
    def wanted(ms: int) -> bool:
        return any(lo * 1000 <= ms < hi * 1000 for lo, hi in ranges)
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            if '"channel": "orderbooks"' in line:
                record = json.loads(line)
                payload = record.get("payload")
                if not isinstance(payload, dict):
                    continue
                if str(payload.get("symbol", "")) != symbol:
                    continue
                stamp = str(payload.get("timestamp", ""))
                if not stamp or stamp in seen:
                    continue
                seen.add(stamp)
                try:
                    ms = epoch_ms_of(stamp)
                except ValueError:
                    continue
                if not wanted(ms):
                    continue
                frames.append((ms, payload.get("asks"), payload.get("bids")))
            elif '"channel": "trades"' in line:
                record = json.loads(line)
                payload = record.get("payload")
                if not isinstance(payload, dict):
                    continue
                if str(payload.get("symbol", "")) != symbol:
                    continue
                stamp = str(payload.get("timestamp", ""))
                try:
                    ms = epoch_ms_of(stamp)
                except ValueError:
                    continue
                if not wanted(ms):
                    continue
                key = (
                    stamp,
                    str(payload.get("price", "")),
                    str(payload.get("size", "")),
                )
                side = str(payload.get("side", ""))
                state = trade_groups.get(key)
                if state is None:
                    trade_groups[key] = [
                        trade_seen,
                        1 if side == "BUY" else 0,
                        0 if side == "BUY" else 1,
                    ]
                    trade_seen += 1
                elif side == "BUY":
                    state[1] += 1
                else:
                    state[2] += 1
    frames.sort(key=lambda item: item[0])
    ordered = []
    for (stamp, price, size), (first, buys, sells) in trade_groups.items():
        ordered.append(
            (epoch_ms_of(stamp), first, price, size, max(buys, sells))
        )
    ordered.sort(key=lambda item: (item[0], item[1]))
    trades = []
    previous: float | None = None
    side_now = "BUY"
    for ms, _, price_text, size_text, matches in ordered:
        price = float(price_text)
        if previous is not None:
            if price > previous:
                side_now = "BUY"
            elif price < previous:
                side_now = "SELL"
        for _ in range(matches):
            trades.append((ms, price, float(size_text), side_now))
        previous = price
    return frames, trades


def render_case(
    case: dict[str, object], frames: list, trades: list, out_dir: Path
) -> dict[str, object]:
    """单案例渲染：热力、描边、标注。"""
    from_s = int(
        datetime.fromisoformat(str(case["from_ts"])).timestamp()
    )
    to_s = int(datetime.fromisoformat(str(case["to_ts"])).timestamp())
    margin = int(case.get("margin_s", 90))
    lo_ms = (from_s - margin) * 1000
    hi_ms = (to_s + margin) * 1000
    subset = [f for f in frames if lo_ms <= f[0] < hi_ms]
    if not subset:
        return {"name": case["name"], "error": "无帧"}
    zoom_lo = float(str(case["zoom_lo"]))
    zoom_hi = float(str(case["zoom_hi"]))
    span = zoom_hi - zoom_lo

    def price_row(value: float) -> int:
        ratio = (zoom_hi - value) / span if span > 0 else 0.5
        return min(PRICE_BINS - 1, max(0, int(ratio * (PRICE_BINS - 1))))

    count = len(subset)
    ask_heat = np.zeros((PRICE_BINS, count), dtype=np.float64)
    bid_heat = np.zeros((PRICE_BINS, count), dtype=np.float64)
    mid_rows: list[int | None] = []
    epochs = np.array([f[0] for f in subset], dtype=np.int64)
    for column, (_, asks, bids) in enumerate(subset):
        best_ask = best_bid = None
        for level in asks or []:
            price = float(level["price"])
            if best_ask is None:
                best_ask = price
            if zoom_lo <= price <= zoom_hi:
                ask_heat[price_row(price), column] += float(level["size"])
        for level in bids or []:
            price = float(level["price"])
            if best_bid is None:
                best_bid = price
            if zoom_lo <= price <= zoom_hi:
                bid_heat[price_row(price), column] += float(level["size"])
        if best_ask is not None and best_bid is not None:
            mid_rows.append(price_row((best_ask + best_bid) / 2))
        else:
            mid_rows.append(None)
    ask_norm = np.log1p(ask_heat)
    bid_norm = np.log1p(bid_heat)
    peak = max(float(ask_norm.max()), float(bid_norm.max()), 1e-9)
    ask_norm /= peak
    bid_norm /= peak
    image = np.zeros((PRICE_BINS, count, 3), dtype=np.uint8)
    image[..., 0] = np.minimum(235, 8 + ask_norm * 300).astype(np.uint8)
    image[..., 1] = np.minimum(215, 8 + bid_norm * 280).astype(np.uint8)
    image[..., 2] = 10
    for column, row in enumerate(mid_rows):
        if row is not None:
            image[row, column] = (150, 160, 175)
    for ms, price, _size, side in trades:
        if not lo_ms <= ms < hi_ms:
            continue
        if not zoom_lo <= price <= zoom_hi:
            continue
        column = int(np.searchsorted(epochs, ms, side="right")) - 1
        column = min(count - 1, max(0, column))
        row = price_row(price)
        color = (90, 235, 120) if side == "BUY" else (245, 110, 100)
        image[max(0, row - 1): row + 2, column] = color
    scale_x = max(1, min(4, TARGET_WIDTH // max(1, count)))
    picture = Image.fromarray(image, "RGB").resize(
        (count * scale_x, PRICE_BINS * SCALE_Y), Image.NEAREST
    )
    draw = ImageDraw.Draw(picture)
    left = int(np.searchsorted(epochs, from_s * 1000)) * scale_x
    right = int(np.searchsorted(epochs, to_s * 1000)) * scale_x - 1
    band_lo = float(str(case["band_lo"]))
    band_hi = float(str(case["band_hi"]))
    top_in = zoom_lo <= band_hi <= zoom_hi
    low_in = zoom_lo <= band_lo <= zoom_hi
    top = price_row(band_hi) * SCALE_Y if top_in else 0
    bottom = (
        (price_row(band_lo) + 1) * SCALE_Y - 1
        if low_in
        else PRICE_BINS * SCALE_Y - 1
    )
    for offset in (0, 1):
        draw.line(
            [(left + offset, top), (left + offset, bottom)], fill=BOX_COLOR
        )
        draw.line(
            [(right - offset, top), (right - offset, bottom)], fill=BOX_COLOR
        )
        if top_in:
            draw.line(
                [(left, top + offset), (right, top + offset)], fill=BOX_COLOR
            )
        if low_in:
            draw.line(
                [(left, bottom - offset), (right, bottom - offset)],
                fill=BOX_COLOR,
            )
    header = [
        f"{case['name']}  kind={case['kind']}  {case['label']}",
        f"window {case['from_ts']} .. {case['to_ts']} UTC"
        f"  band {case['band_lo']}-{case['band_hi']}",
        f"zoom {int(zoom_lo)}-{int(zoom_hi)}  frames {count}"
        f"  margin {margin}s",
    ]
    for at, line in enumerate(header):
        draw.text((6, 4 + 12 * at), line, fill=TEXT_COLOR)
    if top_in:
        draw.text(
            (left + 4, max(0, top - 12)), str(case["band_hi"]),
            fill=BOX_COLOR,
        )
    if low_in:
        draw.text(
            (left + 4, min(PRICE_BINS * SCALE_Y - 12, bottom + 2)),
            str(case["band_lo"]), fill=BOX_COLOR,
        )
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{case['name']}.png"
    picture.save(out_path)
    return {
        "name": case["name"],
        "frames": count,
        "trades_in_zoom": sum(
            1 for ms, price, _s, _d in trades
            if lo_ms <= ms < hi_ms and zoom_lo <= price <= zoom_hi
        ),
        "out": str(out_path),
    }


def main() -> int:
    """主流程：按规格逐案例渲染。"""
    parser = argparse.ArgumentParser(description="判定案例热力渲染")
    parser.add_argument("--spec", required=True)
    parser.add_argument("--data-root", default="data")
    parser.add_argument("--symbol", default="BTC")
    parser.add_argument("--out-dir", default="data/export/alert-verify")
    args = parser.parse_args()
    spec = json.loads(Path(args.spec).read_text(encoding="utf-8"))
    cases = spec["cases"]
    by_day: dict[str, list[tuple[int, int]]] = {}
    for case in cases:
        from_s = int(
            datetime.fromisoformat(str(case["from_ts"])).timestamp()
        )
        to_s = int(datetime.fromisoformat(str(case["to_ts"])).timestamp())
        margin = int(case.get("margin_s", 90))
        day = datetime.fromtimestamp(from_s, UTC).date().isoformat()
        by_day.setdefault(day, []).append(
            (from_s - margin, to_s + margin)
        )
    root = Path(args.data_root)
    day_data: dict[str, tuple[list, list]] = {}
    for day, ranges in by_day.items():
        path = root / "raw" / day / "ws_public.jsonl"
        day_data[day] = load_day(path, args.symbol, ranges)
    out_dir = Path(args.out_dir)
    for case in cases:
        day = str(case["from_ts"])[:10]
        frames, trades = day_data[day]
        report = render_case(case, frames, trades, out_dir)
        print(json.dumps(report, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
