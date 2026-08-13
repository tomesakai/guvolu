"""盘口热力瓦片金字塔：raw 帧与逐笔预聚合（footprint-design 6.5 节）。

逐（来源, 品种, 桶档, UTC 日期）生成列网格文件落 data/derived/heatmap_tiles/，
行为价格档（tickSize 对齐可复现分档）、列为时间桶，格值为挂量末态与
档级三值分解（净增挂、净撤减、成交消耗，6.2 节口径）：成交消耗自逐笔
精确扣除，净值为挂量差加成交消耗的残差正负两侧。
盘口帧先按 timestamp 去重（双写者窗口重复帧取其一）再按时刻重排；
空档列显式保留（录制空窗事实）；帧距在延载上限内的无帧桶按快照流
语义延载末态并标 carried；超限即标 gap，恢复后基线重置标 reset。
延载列与空档列期间的逐笔记入其自然所属列（版本 2）：延载列对
末知挂量按延载末态扣减计净值并向后续列传递扣减后末态；空档列
只记成交消耗事实，净值不可知记零。底部指标带五条（Spread、OFI、
盘口不平衡、Trade Delta、Depth 分带）随列同步产出，量类两带另附
金额基准（逐笔与帧内 价×量 精确累计，版本 2）；量达分位阈的
大额成交另落成交刻线文件（6.7 节）。
文件为分块 gz（逐列一行 JSON）加 meta JSON，零新依赖（C-05），
幂等重建自 raw；数值一律 Decimal 字符串（T-08、D-07）。
当期侧别由 tick 规则推断（口径快照第 4 节），meta 注明依据。
当日另有增量模式：按 raw 字节偏移游标只处理新增行、追加新列，
游标文件与日瓦片同目录；单一 api 进程即当日唯一写者，
完结日全量重建路径不变，两者不并行触碰同一日。
"""
from __future__ import annotations

import argparse
import gzip
import heapq
import json
import time
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from guvolu.data.durable_io import (
    atomic_write_bytes,
    atomic_write_text,
    durable_append_bytes,
)
from guvolu.data.footprint import Print, dedupe_ws_rows, infer_sides, price_bin_of
from guvolu.data.raw_records import ws_channel, ws_payload
from guvolu.domain.enums import Side
from guvolu.domain.ids import sha256_hex

# 瓦片行格式版本（D-06）
TILE_SCHEMA_VERSION = 2
# 指针以原子替换持久化
TILE_PERSISTENCE_VERSION = "atomic-pointer-v1"
# 来源标识，多源预留
VENUE_GMO = "gmo"
# 派生目录段
DERIVED_SEGMENTS = ("derived", "heatmap_tiles")
# 桶档与秒宽（6.1 节下限一秒）
TILE_BUCKETS: Mapping[str, int] = {"1s": 1, "5s": 5, "1min": 60}
# 行宽为 tickSize 乘档位
TILE_ROW_TIERS: Mapping[str, int] = {"1s": 1, "5s": 2, "1min": 10}
# 帧重排缓冲毫秒（双写者乱序上界）
REORDER_WINDOW_MS = 5000
# 快照延载上限秒，超限判空档
BOOK_CARRY_LIMIT_SECONDS = 30
# 深度带宽序列（bp）
DEPTH_BAND_BPS: tuple[str, ...] = ("5", "10", "25")
# 成交刻线量分位阈，缺省 P95
PRINT_TICK_QUANTILE = Decimal("0.95")
# bp 换算因子
BP_FACTOR = Decimal("10000")
# bp 值保留位
BP_PLACES = Decimal("0.01")
# 比率保留位
RATIO_PLACES = Decimal("0.0001")
# 单响应列数上限
MAX_SLICE_COLUMNS = 4000
# 物理块列数
TILE_CHUNK_COLUMNS = 512
# 档位追踪列数上限
MAX_TRACK_COLUMNS = 7200
# 价格反应前后桶数缺省
PRICE_REACTION_BUCKETS = 10
# 补单回补检视桶数
REPLENISH_LOOKAHEAD_BUCKETS = 3
# 格值字段次序说明
CELL_FIELDS: tuple[str, ...] = (
    "price_bin", "side", "last_qty", "net_add", "net_cancel", "executed"
)
# 侧别依据标注
SIDE_BASIS = "tick_rule_inference"

_ZERO = Decimal("0")
_TWO = Decimal("2")
_DAY_SECONDS = 86400


def tiles_config() -> dict[str, object]:
    """构建参数全集，供散列与追溯（D-09、G-06）。"""
    return {
        "schema_version": TILE_SCHEMA_VERSION,
        "buckets": dict(TILE_BUCKETS),
        "row_tiers": dict(TILE_ROW_TIERS),
        "reorder_window_ms": REORDER_WINDOW_MS,
        "carry_limit_seconds": BOOK_CARRY_LIMIT_SECONDS,
        "depth_band_bps": list(DEPTH_BAND_BPS),
        "print_tick_quantile": str(PRINT_TICK_QUANTILE),
        "side_basis": SIDE_BASIS,
    }


def tiles_config_hash() -> str:
    """构建配置散列。"""
    text = json.dumps(tiles_config(), sort_keys=True, ensure_ascii=False)
    return sha256_hex(text.encode("utf-8"))


def _text(value: Decimal) -> str:
    return format(value, "f")


def tile_dir(data_root: Path, venue: str, symbol: str, bucket: str) -> Path:
    """桶档目录：逐（来源, 品种, 桶档）分层。"""
    return data_root.joinpath(*DERIVED_SEGMENTS) / venue / symbol / bucket


def tile_paths(
    data_root: Path, venue: str, symbol: str, bucket: str, date_text: str
) -> tuple[Path, Path]:
    """列文件与 meta 文件路径，date 为 UTC 日 YYYY-MM-DD。"""
    base = tile_dir(data_root, venue, symbol, bucket)
    return base / f"{date_text}.jsonl.gz", base / f"{date_text}.meta.json"


def tile_chunk_path(
    data_root: Path,
    venue: str,
    symbol: str,
    bucket: str,
    date_text: str,
    generation: str,
    start_s: int,
) -> Path:
    """内容代次下的物理块路径。"""
    return (
        tile_dir(data_root, venue, symbol, bucket)
        / "chunks"
        / date_text
        / generation
        / f"{start_s}.jsonl.gz"
    )


def _chunk_groups(
    columns: Sequence[Mapping[str, object]], bucket_seconds: int
) -> dict[int, list[Mapping[str, object]]]:
    """按全局块界分组列。"""
    span = bucket_seconds * TILE_CHUNK_COLUMNS
    groups: dict[int, list[Mapping[str, object]]] = {}
    for column in columns:
        epoch = column.get("e")
        if not isinstance(epoch, int):
            continue
        start = epoch // span * span
        groups.setdefault(start, []).append(column)
    return groups


def _write_tile_chunks(
    data_root: Path,
    venue: str,
    symbol: str,
    bucket: str,
    date_text: str,
    generation: str,
    columns: Sequence[Mapping[str, object]],
    bucket_seconds: int,
    *,
    append: bool,
) -> None:
    """写独立 gzip 块，查询免扫整日。"""
    for start_s, rows in _chunk_groups(columns, bucket_seconds).items():
        path = tile_chunk_path(
            data_root,
            venue,
            symbol,
            bucket,
            date_text,
            generation,
            start_s,
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        body = gzip.compress(
            "".join(
                json.dumps(column, ensure_ascii=False) + "\n"
                for column in rows
            ).encode("utf-8")
        )
        # 追加为新 gzip 成员，读侧多成员透明
        if append and path.exists():
            durable_append_bytes(path, body)
        else:
            atomic_write_bytes(path, body)


def print_ticks_path(
    data_root: Path, venue: str, symbol: str, date_text: str
) -> Path:
    """成交刻线文件路径（桶档无关，逐日一份）。"""
    return (
        data_root.joinpath(*DERIVED_SEGMENTS)
        / venue / symbol / "print_ticks" / f"{date_text}.json"
    )


@dataclass(frozen=True, slots=True)
class _BookFrame:
    """去重重排后的单帧：毫秒时刻与双侧档。"""

    epoch_ms: int
    asks: tuple[tuple[Decimal, Decimal], ...]
    bids: tuple[tuple[Decimal, Decimal], ...]


def _parse_levels(raw: object) -> tuple[tuple[Decimal, Decimal], ...]:
    """档数组原文转（价, 量）Decimal 序列。"""
    if not isinstance(raw, list):
        return ()
    out: list[tuple[Decimal, Decimal]] = []
    for level in raw:
        if isinstance(level, Mapping):
            out.append(
                (Decimal(str(level["price"])), Decimal(str(level["size"])))
            )
    return tuple(out)


def _frame_epoch_ms(stamp: str) -> int:
    moment = datetime.fromisoformat(stamp)
    return (
        int(moment.replace(microsecond=0).timestamp()) * 1000
        + moment.microsecond // 1000
    )


def _iter_raw_lines(path: Path) -> Iterator[str]:
    """逐行读 raw 文件，兼容隔日压缩件。"""
    if path.suffix == ".gz":
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            yield from fh
    else:
        with path.open(encoding="utf-8") as fh:
            yield from fh


def _raw_file(data_root: Path, date_text: str) -> Path | None:
    """定位当日 raw 公开流文件，无则返回空。"""
    plain = data_root / "raw" / date_text / "ws_public.jsonl"
    if plain.exists():
        return plain
    packed = plain.with_suffix(".jsonl.gz")
    return packed if packed.exists() else None


def scan_trade_prints(path: Path, symbol: str) -> list[Print]:
    """读当日逐笔：双侧成对去重合一后按 tick 规则推断侧别。"""
    rows: list[tuple[str, str, str, str]] = []
    for line in _iter_raw_lines(path):
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
        rows.append(
            (
                str(payload.get("timestamp", "")),
                str(payload.get("price", "")),
                str(payload.get("size", "")),
                str(payload.get("side", "")),
            )
        )
    if not rows:
        return []
    return infer_sides(dedupe_ws_rows(rows), None)


def scan_book_frames(path: Path, symbol: str) -> Iterator[_BookFrame]:
    """流式产出盘口帧：按 timestamp 去重并小窗重排。

    双写者窗口产生的重复帧同刻同容，取首见其一；
    交错造成的轻度乱序以毫秒堆缓冲还原时序。
    """
    seen: set[str] = set()
    heap: list[tuple[int, int, _BookFrame]] = []
    seq = 0
    for line in _iter_raw_lines(path):
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
        stamp = str(payload.get("timestamp", ""))
        if not stamp or stamp in seen:
            continue
        seen.add(stamp)
        try:
            epoch_ms = _frame_epoch_ms(stamp)
        except ValueError:
            continue
        frame = _BookFrame(
            epoch_ms=epoch_ms,
            asks=_parse_levels(payload.get("asks")),
            bids=_parse_levels(payload.get("bids")),
        )
        heapq.heappush(heap, (epoch_ms, seq, frame))
        seq += 1
        while heap and heap[0][0] <= epoch_ms - REORDER_WINDOW_MS:
            yield heapq.heappop(heap)[2]
    while heap:
        yield heapq.heappop(heap)[2]


def _binned(
    frame: _BookFrame, row_bin: Decimal
) -> dict[Decimal, tuple[Decimal, Decimal]]:
    """单帧按行档聚合为档到（卖侧挂量, 买侧挂量）。"""
    out: dict[Decimal, tuple[Decimal, Decimal]] = {}
    for price, size in frame.asks:
        key = price_bin_of(price, row_bin)
        ask, bid = out.get(key, (_ZERO, _ZERO))
        out[key] = (ask + size, bid)
    for price, size in frame.bids:
        key = price_bin_of(price, row_bin)
        ask, bid = out.get(key, (_ZERO, _ZERO))
        out[key] = (ask, bid + size)
    return out


def _frame_bands(frame: _BookFrame) -> dict[str, object]:
    """帧内带值：价差、不平衡、分带深度与部分覆盖旗标。"""
    if not frame.asks or not frame.bids:
        return {"spread_bp": None, "imbalance": None, "mid": None, "depth": []}
    best_ask = frame.asks[0][0]
    best_bid = frame.bids[0][0]
    mid = (best_ask + best_bid) / _TWO
    spread_bp = ((best_ask - best_bid) / mid * BP_FACTOR).quantize(BP_PLACES)
    ask_total = sum((size for _, size in frame.asks), _ZERO)
    bid_total = sum((size for _, size in frame.bids), _ZERO)
    whole = ask_total + bid_total
    imbalance = (
        None if whole == _ZERO
        else ((bid_total - ask_total) / whole).quantize(RATIO_PLACES)
    )
    depth: list[list[object]] = []
    deep_ask = frame.asks[-1][0]
    deep_bid = frame.bids[-1][0]
    for band in DEPTH_BAND_BPS:
        width = mid * Decimal(band) / BP_FACTOR
        upper = mid + width
        lower = mid - width
        total = _ZERO
        notional = _ZERO
        for price, size in frame.asks:
            if price <= upper:
                total += size
                notional += price * size
        for price, size in frame.bids:
            if price >= lower:
                total += size
                notional += price * size
        # 可视窗未及带缘即标部分覆盖
        partial = deep_ask < upper or deep_bid > lower
        depth.append([band, _text(total), partial, _text(notional)])
    return {
        "spread_bp": _text(spread_bp),
        "imbalance": None if imbalance is None else _text(imbalance),
        "mid": _text(mid),
        "depth": depth,
    }


def _ofi_increment(prev: _BookFrame, current: _BookFrame) -> Decimal:
    """最优档增量式 OFI 单步值（6.3 节定义）。"""
    if not (prev.asks and prev.bids and current.asks and current.bids):
        return _ZERO
    pb_price, pb_size = current.bids[0]
    qb_price, qb_size = prev.bids[0]
    pa_price, pa_size = current.asks[0]
    qa_price, qa_size = prev.asks[0]
    value = _ZERO
    if pb_price >= qb_price:
        value += pb_size
    if pb_price <= qb_price:
        value -= qb_size
    if pa_price <= qa_price:
        value -= pa_size
    if pa_price >= qa_price:
        value += qa_size
    return value


@dataclass(slots=True)
class _TierState:
    """单桶档构建态：跨桶携带的末态与桶内累计。"""

    bucket_seconds: int
    row_bin: Decimal
    columns: list[dict[str, object]]
    prev_levels: dict[Decimal, tuple[Decimal, Decimal]] | None = None
    open_index: int | None = None
    open_reset: bool = False
    open_frames: int = 0
    open_ofi: Decimal = _ZERO
    open_last_frame: _BookFrame | None = None
    open_start: dict[Decimal, tuple[Decimal, Decimal]] | None = None
    carried_bands: dict[str, object] | None = None
    last_frame_ms: int | None = None
    print_at: int = 0
    gap_columns: int = 0
    carried_columns: int = 0


class _DayBuilder:
    """单日三桶档同趟构建器。"""

    def __init__(
        self, day_start_s: int, horizon_s: int, tick_size: Decimal,
        prints: Sequence[Print],
    ) -> None:
        self.day_start_s = day_start_s
        self.horizon_s = horizon_s
        self.prints = list(prints)
        self.tiers: dict[str, _TierState] = {
            bucket: _TierState(
                bucket_seconds=seconds,
                row_bin=tick_size * TILE_ROW_TIERS[bucket],
                columns=[],
            )
            for bucket, seconds in TILE_BUCKETS.items()
        }
        self.frames = 0
        self.dropped_frames = 0
        self.prev_frame: _BookFrame | None = None

    def push_frame(self, frame: _BookFrame) -> None:
        """按时序推进一帧至全部桶档。"""
        second = frame.epoch_ms // 1000
        if second < self.day_start_s or second >= self.horizon_s:
            self.dropped_frames += 1
            return
        self.frames += 1
        ofi = _ZERO
        if self.prev_frame is not None:
            gap_ms = frame.epoch_ms - self.prev_frame.epoch_ms
            if gap_ms <= BOOK_CARRY_LIMIT_SECONDS * 1000:
                ofi = _ofi_increment(self.prev_frame, frame)
        self.prev_frame = frame
        for state in self.tiers.values():
            self._advance(state, frame, ofi)

    def _advance(
        self, state: _TierState, frame: _BookFrame, ofi: Decimal
    ) -> None:
        index = (frame.epoch_ms // 1000 - self.day_start_s) // (
            state.bucket_seconds
        )
        if state.open_index is None:
            self._emit_idle(state, index)
            self._open(state, index, frame)
        elif index > state.open_index:
            self._close_open(state)
            self._emit_idle(state, index)
            self._open(state, index, frame)
        state.open_frames += 1
        state.open_ofi += ofi
        state.open_last_frame = frame

    def _open(self, state: _TierState, index: int, frame: _BookFrame) -> None:
        state.open_index = index
        state.open_frames = 0
        state.open_ofi = _ZERO
        state.open_last_frame = None
        # 空档恢复后基线重置为桶内首帧
        if state.prev_levels is None:
            state.open_start = _binned(frame, state.row_bin)
            state.open_reset = True
        else:
            state.open_start = state.prev_levels
            state.open_reset = False

    def _emit_idle(self, state: _TierState, until_index: int) -> None:
        """补齐已发列至目标桶之间的延载列与空档列。

        列严格逐桶追加，下一待发列即列表长度。
        """
        for index in range(len(state.columns), until_index):
            bucket_end_ms = (
                self.day_start_s + (index + 1) * state.bucket_seconds
            ) * 1000
            carried = (
                state.last_frame_ms is not None
                and bucket_end_ms - state.last_frame_ms
                <= BOOK_CARRY_LIMIT_SECONDS * 1000
            )
            if carried:
                self._emit_carried(state, index)
            else:
                self._emit_gap(state, index)

    def _bucket_prints(
        self, state: _TierState, index: int
    ) -> list[Print]:
        """顺序消费落入该桶的逐笔。"""
        start_ms = (self.day_start_s + index * state.bucket_seconds) * 1000
        end_ms = start_ms + state.bucket_seconds * 1000
        out: list[Print] = []
        while state.print_at < len(self.prints):
            item = self.prints[state.print_at]
            if item[0] >= end_ms:
                break
            if item[0] >= start_ms:
                out.append(item)
            state.print_at += 1
        return out

    def _column_time(self, state: _TierState, index: int) -> dict[str, object]:
        epoch = self.day_start_s + index * state.bucket_seconds
        stamp = datetime.fromtimestamp(epoch, UTC).isoformat()
        return {"t": stamp, "e": epoch}

    @staticmethod
    def _trade_delta(prints: Sequence[Print]) -> tuple[Decimal, Decimal]:
        """桶内撮合口径 Delta，数量与金额双基准。"""
        delta = notional = _ZERO
        for _, price, size, side in prints:
            if side == Side.BUY.value:
                delta += size
                notional += price * size
            else:
                delta -= size
                notional -= price * size
        return delta, notional

    @staticmethod
    def _executed_by_bin(
        prints: Sequence[Print], row_bin: Decimal
    ) -> dict[Decimal, tuple[Decimal, Decimal]]:
        """桶内逐笔按档聚合（买向消耗, 卖向消耗）。"""
        out: dict[Decimal, tuple[Decimal, Decimal]] = {}
        for _, price, size, side in prints:
            key = price_bin_of(price, row_bin)
            buy, sell = out.get(key, (_ZERO, _ZERO))
            if side == Side.BUY.value:
                out[key] = (buy + size, sell)
            else:
                out[key] = (buy, sell + size)
        return out

    def _emit_gap(self, state: _TierState, index: int) -> None:
        prints = self._bucket_prints(state, index)
        delta, delta_notional = self._trade_delta(prints)
        # 空档无基线：只记消耗事实，净值记零
        cells = [
            [_text(key), "void", "0", "0", "0", _text(buy + sell)]
            for key, (buy, sell) in sorted(
                self._executed_by_bin(prints, state.row_bin).items()
            )
        ]
        column = self._column_time(state, index)
        column.update(
            {
                "gap": True, "carried": False, "reset": False, "frames": 0,
                "mid": None, "cells": cells,
                "bands": {
                    "spread_bp": None, "ofi": "0", "imbalance": None,
                    "trade_delta": _text(delta),
                    "trade_delta_notional": _text(delta_notional),
                    "depth": [],
                },
            }
        )
        state.columns.append(column)
        state.gap_columns += 1
        # 空档打断末态延载
        state.prev_levels = None

    def _emit_carried(self, state: _TierState, index: int) -> None:
        prints = self._bucket_prints(state, index)
        delta, delta_notional = self._trade_delta(prints)
        eaten_bins = self._executed_by_bin(prints, state.row_bin)
        levels = state.prev_levels if state.prev_levels is not None else {}
        # 消耗按吃单方向扣延载末态，见底不负
        after: dict[Decimal, tuple[Decimal, Decimal]] = {}
        cells: list[list[object]] = []
        for key in sorted(set(levels) | set(eaten_bins)):
            ask0, bid0 = levels.get(key, (_ZERO, _ZERO))
            buy, sell = eaten_bins.get(key, (_ZERO, _ZERO))
            ask1 = max(_ZERO, ask0 - buy)
            bid1 = max(_ZERO, bid0 - sell)
            eaten = buy + sell
            before_total = ask0 + bid0
            after_total = ask1 + bid1
            if before_total == after_total == eaten == _ZERO:
                continue
            residual = after_total - before_total + eaten
            net_add = residual if residual > _ZERO else _ZERO
            net_cancel = -residual if residual < _ZERO else _ZERO
            side = _cell_side(ask1, bid1)
            if side == "void":
                side = _cell_side(ask0, bid0)
            cells.append(
                [
                    _text(key), side, _text(after_total),
                    _text(net_add), _text(net_cancel), _text(eaten),
                ]
            )
            if ask1 > _ZERO or bid1 > _ZERO:
                after[key] = (ask1, bid1)
        bands = dict(state.carried_bands or {})
        mid = bands.pop("mid", None)
        bands["ofi"] = "0"
        bands["trade_delta"] = _text(delta)
        bands["trade_delta_notional"] = _text(delta_notional)
        column = self._column_time(state, index)
        column.update(
            {
                "gap": False, "carried": True, "reset": False, "frames": 0,
                "mid": mid, "cells": cells, "bands": bands,
            }
        )
        state.columns.append(column)
        state.carried_columns += 1
        # 扣减后末态向后续列传递
        state.prev_levels = after

    def _close_open(self, state: _TierState) -> None:
        index = state.open_index
        if index is None:
            return
        frame = state.open_last_frame
        assert frame is not None
        end_levels = _binned(frame, state.row_bin)
        prints = self._bucket_prints(state, index)
        delta, delta_notional = self._trade_delta(prints)
        executed: dict[Decimal, Decimal] = {}
        for _, price, size, _side in prints:
            key = price_bin_of(price, state.row_bin)
            executed[key] = executed.get(key, _ZERO) + size
        start_levels = state.open_start if state.open_start is not None else {}
        keys = sorted(set(start_levels) | set(end_levels) | set(executed))
        cells: list[list[object]] = []
        for key in keys:
            ask0, bid0 = start_levels.get(key, (_ZERO, _ZERO))
            ask1, bid1 = end_levels.get(key, (_ZERO, _ZERO))
            before = ask0 + bid0
            after = ask1 + bid1
            eaten = executed.get(key, _ZERO)
            if before == after == eaten == _ZERO:
                continue
            residual = after - before + eaten
            net_add = residual if residual > _ZERO else _ZERO
            net_cancel = -residual if residual < _ZERO else _ZERO
            side = _cell_side(ask1, bid1)
            if side == "void":
                side = _cell_side(ask0, bid0)
            cells.append(
                [
                    _text(key), side, _text(after),
                    _text(net_add), _text(net_cancel), _text(eaten),
                ]
            )
        frame_bands = _frame_bands(frame)
        mid = frame_bands.pop("mid")
        bands: dict[str, object] = dict(frame_bands)
        bands["ofi"] = _text(state.open_ofi)
        bands["trade_delta"] = _text(delta)
        bands["trade_delta_notional"] = _text(delta_notional)
        column = self._column_time(state, index)
        column.update(
            {
                "gap": False, "carried": False, "reset": state.open_reset,
                "frames": state.open_frames, "mid": mid,
                "cells": cells, "bands": bands,
            }
        )
        state.columns.append(column)
        state.prev_levels = end_levels
        state.carried_bands = dict(frame_bands) | {"mid": mid}
        state.last_frame_ms = frame.epoch_ms
        state.open_index = None
        state.open_start = None
        state.open_last_frame = None

    def finish(self) -> None:
        """收尾：关闭在开桶并补齐至构建视界。"""
        for state in self.tiers.values():
            self._close_open(state)
            total = (self.horizon_s - self.day_start_s) // state.bucket_seconds
            self._emit_idle(state, total)


def _cell_side(ask: Decimal, bid: Decimal) -> str:
    """格侧别：卖侧、买侧、跨档混合或已空。"""
    if ask > _ZERO and bid > _ZERO:
        return "both"
    if ask > _ZERO:
        return "ask"
    if bid > _ZERO:
        return "bid"
    return "void"


def _quantile_threshold(values: Sequence[Decimal], quantile: Decimal) -> Decimal:
    """最近秩法分位值，样本升序。"""
    ranked = sorted(values)
    if not ranked:
        return _ZERO
    position = int((quantile * len(ranked)).to_integral_value(rounding="ROUND_CEILING"))
    position = min(max(position, 1), len(ranked))
    return ranked[position - 1]


def build_print_ticks(
    prints: Sequence[Print], quantile: Decimal
) -> dict[str, object]:
    """成交刻线清单：量达分位阈的大额成交（6.7 节）。"""
    sizes = [size for _, _, size, _ in prints]
    threshold = _quantile_threshold(sizes, quantile)
    ranked = sorted(sizes)
    items: list[dict[str, object]] = []
    if prints and threshold > _ZERO:
        total = len(ranked)
        for epoch_ms, price, size, side in prints:
            if size < threshold:
                continue
            below = _bisect_right(ranked, size)
            rank = (Decimal(below) / Decimal(total)).quantize(RATIO_PLACES)
            items.append(
                {
                    "t": datetime.fromtimestamp(
                        epoch_ms / 1000, UTC
                    ).isoformat(),
                    "price": _text(price),
                    "size": _text(size),
                    "side": side,
                    "size_quantile": _text(rank),
                }
            )
    return {
        "schema_version": TILE_SCHEMA_VERSION,
        "quantile": _text(quantile),
        "threshold": _text(threshold),
        "prints_total": len(prints),
        "side_basis": SIDE_BASIS,
        "items": items,
    }


def _bisect_right(ranked: Sequence[Decimal], value: Decimal) -> int:
    low, high = 0, len(ranked)
    while low < high:
        middle = (low + high) // 2
        if ranked[middle] <= value:
            low = middle + 1
        else:
            high = middle
    return low


def local_tick_size(data_root: Path, symbol: str) -> Decimal | None:
    """从 raw 落盘的取引ルール快照取 tickSize，零上游。"""
    raw_root = data_root / "raw"
    if not raw_root.is_dir():
        return None
    for day in sorted(raw_root.iterdir(), reverse=True):
        path = day / "symbols.jsonl"
        if not path.exists():
            continue
        for line in _iter_raw_lines(path):
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            payload = record.get("payload")
            if not isinstance(payload, Mapping):
                continue
            data = payload.get("data")
            if not isinstance(data, list):
                continue
            for row in data:
                if isinstance(row, Mapping) and row.get("symbol") == symbol:
                    return Decimal(str(row.get("tickSize")))
    return None


def build_day_tiles(
    data_root: Path,
    symbol: str,
    date_text: str,
    tick_size: Decimal,
    *,
    venue: str = VENUE_GMO,
    now: datetime | None = None,
) -> dict[str, object]:
    """构建单（品种, UTC 日）全桶档瓦片与成交刻线，幂等覆写。"""
    started = time.monotonic()
    moment = now if now is not None else datetime.now(UTC)
    raw_path = _raw_file(data_root, date_text)
    if raw_path is None:
        raise FileNotFoundError(f"raw 无当日公开流: {date_text}")
    day_start = int(
        datetime.strptime(date_text, "%Y-%m-%d")
        .replace(tzinfo=UTC)
        .timestamp()
    )
    day_end = day_start + _DAY_SECONDS
    now_s = int(moment.timestamp())
    horizon = min(day_end, now_s)
    complete = horizon == day_end
    prints = scan_trade_prints(raw_path, symbol)
    builder = _DayBuilder(day_start, horizon, tick_size, prints)
    for frame in scan_book_frames(raw_path, symbol):
        builder.push_frame(frame)
    builder.finish()
    built_at = datetime.now(UTC).isoformat()
    chunk_generation = sha256_hex(built_at.encode("utf-8"))[:16]
    config_hash = tiles_config_hash()
    report: dict[str, object] = {
        "venue": venue,
        "symbol": symbol,
        "date": date_text,
        "frames": builder.frames,
        "prints": len(prints),
        "complete": complete,
        "tiers": {},
    }
    tiers_report = report["tiers"]
    assert isinstance(tiers_report, dict)
    for bucket, state in builder.tiers.items():
        gz_path, meta_path = tile_paths(
            data_root, venue, symbol, bucket, date_text
        )
        atomic_write_bytes(
            gz_path,
            gzip.compress(
                "".join(
                    json.dumps(column, ensure_ascii=False) + "\n"
                    for column in state.columns
                ).encode("utf-8")
            ),
        )
        _write_tile_chunks(
            data_root,
            venue,
            symbol,
            bucket,
            date_text,
            chunk_generation,
            state.columns,
            state.bucket_seconds,
            append=False,
        )
        meta = {
            "schema_version": TILE_SCHEMA_VERSION,
            "persistence_version": TILE_PERSISTENCE_VERSION,
            "venue": venue,
            "symbol": symbol,
            "bucket": bucket,
            "bucket_seconds": state.bucket_seconds,
            "date": date_text,
            "tick_size": _text(tick_size),
            "row_tier": TILE_ROW_TIERS[bucket],
            "row_bin": _text(state.row_bin),
            "columns": len(state.columns),
            "gap_columns": state.gap_columns,
            "carried_columns": state.carried_columns,
            "frames": builder.frames,
            "prints": len(prints),
            "complete": complete,
            "built_at": built_at,
            "config_hash": config_hash,
            "source": f"raw/{date_text}/{raw_path.name}",
            "cell_fields": list(CELL_FIELDS),
            "side_basis": SIDE_BASIS,
            "chunk_columns": TILE_CHUNK_COLUMNS,
            "chunk_generation": chunk_generation,
        }
        atomic_write_text(
            meta_path, json.dumps(meta, ensure_ascii=False, indent=1) + "\n"
        )
        tiers_report[bucket] = {
            "columns": len(state.columns),
            "gap_columns": state.gap_columns,
            "carried_columns": state.carried_columns,
            "bytes": gz_path.stat().st_size,
            "path": str(gz_path),
        }
    ticks = build_print_ticks(prints, PRINT_TICK_QUANTILE)
    ticks.update(
        {
            "venue": venue, "symbol": symbol, "date": date_text,
            "built_at": built_at, "config_hash": config_hash,
            "persistence_version": TILE_PERSISTENCE_VERSION,
        }
    )
    tick_path = print_ticks_path(data_root, venue, symbol, date_text)
    tick_path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_text(
        tick_path, json.dumps(ticks, ensure_ascii=False) + "\n"
    )
    tick_items = ticks["items"]
    report["print_ticks"] = {
        "items": len(tick_items) if isinstance(tick_items, list) else 0,
        "path": str(tick_path),
    }
    report["build_ms"] = round((time.monotonic() - started) * 1000, 1)
    return report


def cursor_path(
    data_root: Path, venue: str, symbol: str, date_text: str
) -> Path:
    """当日增量游标文件路径，与日瓦片同目录树。"""
    return (
        data_root.joinpath(*DERIVED_SEGMENTS)
        / venue / symbol / f"{date_text}.cursor.json"
    )


@dataclass(slots=True)
class _PendingTrade:
    """待释放的逐笔原文行。"""

    epoch_ms: int
    seq: int
    row: tuple[str, str, str, str]


class IncrementalTileBuilder:
    """单（品种, UTC 日）当日增量构建器。

    字节偏移游标只读新增行；帧与逐笔各按撮合时刻
    水位释放（重排窗内缓冲），已闭桶列追加写为新
    gz 成员；meta 与成交刻线随刷新重写。首次刷新
    等价全量重建（覆写既有文件），其后仅追加。
    """

    def __init__(
        self,
        data_root: Path,
        symbol: str,
        date_text: str,
        tick_size: Decimal,
        *,
        venue: str = VENUE_GMO,
    ) -> None:
        self.data_root = data_root
        self.symbol = symbol
        self.date_text = date_text
        self.venue = venue
        self.tick_size = tick_size
        self.day_start_s = int(
            datetime.strptime(date_text, "%Y-%m-%d")
            .replace(tzinfo=UTC)
            .timestamp()
        )
        self.builder = _DayBuilder(
            self.day_start_s, self.day_start_s + _DAY_SECONDS, tick_size, []
        )
        self.offset = 0
        self.seq = 0
        self.max_seen_ms: int | None = None
        self.released_ms: int | None = None
        self.trade_pending: list[_PendingTrade] = []
        self.frame_pending: list[tuple[int, int, str, _BookFrame]] = []
        self.pending_stamps: set[str] = set()
        self.seed_price: Decimal | None = None
        self.seed_side: str = Side.BUY.value
        self.flushed: dict[str, int] = {bucket: 0 for bucket in TILE_BUCKETS}
        self.fresh_file = True
        self.finalized = False
        seed = f"{venue}|{symbol}|{date_text}|{time.time_ns()}"
        self.chunk_generation = sha256_hex(seed.encode("utf-8"))[:16]

    def _ingest_line(self, line: str) -> None:
        """单行解析入待释放缓冲，非本品种即弃。"""
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            return
        if not isinstance(record, Mapping):
            return
        payload = ws_payload(record)
        if payload is None:
            return
        channel = ws_channel(record, payload)
        is_trade = channel == "trades"
        is_book = channel == "orderbooks"
        if not is_trade and not is_book:
            return
        if str(payload.get("symbol", "")) != self.symbol:
            return
        stamp = str(payload.get("timestamp", ""))
        try:
            epoch_ms = _frame_epoch_ms(stamp)
        except ValueError:
            return
        if self.max_seen_ms is None or epoch_ms > self.max_seen_ms:
            self.max_seen_ms = epoch_ms
        # 已过水位的迟到行按流语义丢弃
        if self.released_ms is not None and epoch_ms <= self.released_ms:
            return
        self.seq += 1
        if is_trade:
            self.trade_pending.append(
                _PendingTrade(
                    epoch_ms=epoch_ms,
                    seq=self.seq,
                    row=(
                        stamp,
                        str(payload.get("price", "")),
                        str(payload.get("size", "")),
                        str(payload.get("side", "")),
                    ),
                )
            )
        elif is_book:
            if stamp in self.pending_stamps:
                # 双写者重复帧取首见其一
                return
            self.pending_stamps.add(stamp)
            self.frame_pending.append(
                (
                    epoch_ms,
                    self.seq,
                    stamp,
                    _BookFrame(
                        epoch_ms=epoch_ms,
                        asks=_parse_levels(payload.get("asks")),
                        bids=_parse_levels(payload.get("bids")),
                    ),
                )
            )

    def _release(self, watermark_ms: int) -> None:
        """释放水位内条目：先逐笔后帧，保证闭桶取齐。"""
        due_trades = [
            item for item in self.trade_pending if item.epoch_ms <= watermark_ms
        ]
        if due_trades:
            due_trades.sort(key=lambda item: (item.epoch_ms, item.seq))
            self.trade_pending = [
                item
                for item in self.trade_pending
                if item.epoch_ms > watermark_ms
            ]
            matches = dedupe_ws_rows([item.row for item in due_trades])
            prints = infer_sides(matches, self.seed_price, self.seed_side)
            if prints:
                self.seed_price = prints[-1][1]
                self.seed_side = prints[-1][3]
                self.builder.prints.extend(prints)
        due_frames = [
            item for item in self.frame_pending if item[0] <= watermark_ms
        ]
        if due_frames:
            due_frames.sort(key=lambda item: (item[0], item[1]))
            self.frame_pending = [
                item for item in self.frame_pending if item[0] > watermark_ms
            ]
            for _, _, stamp, frame in due_frames:
                self.pending_stamps.discard(stamp)
                self.builder.push_frame(frame)
        if self.released_ms is None or watermark_ms > self.released_ms:
            self.released_ms = watermark_ms

    def _raw_path(self) -> Path:
        return self.data_root / "raw" / self.date_text / "ws_public.jsonl"

    def refresh(self) -> dict[str, object]:
        """读游标后新增行并追加新列，返回刷新报告。"""
        raw_path = self._raw_path()
        report: dict[str, object] = {
            "venue": self.venue,
            "symbol": self.symbol,
            "date": self.date_text,
            "offset": self.offset,
            "appended": {},
        }
        if self.finalized or not raw_path.exists():
            return report
        size = raw_path.stat().st_size
        if size < self.offset:
            raise ValueError(f"raw 文件缩短: {raw_path}")
        if size > self.offset:
            with raw_path.open("rb") as fh:
                fh.seek(self.offset)
                chunk = fh.read(size - self.offset)
            cut = chunk.rfind(b"\n")
            if cut >= 0:
                for raw_line in chunk[: cut + 1].split(b"\n"):
                    if raw_line.strip():
                        self._ingest_line(
                            raw_line.decode("utf-8", errors="replace")
                        )
                self.offset += cut + 1
        if self.max_seen_ms is not None:
            self._release(self.max_seen_ms - REORDER_WINDOW_MS)
        report["appended"] = self._flush(complete=False)
        report["offset"] = self.offset
        self._write_cursor()
        return report

    def finalize(self) -> dict[str, object]:
        """日界收尾：全量释放并补齐至日末，标记完结。"""
        report = self.refresh()
        if self.finalized:
            return report
        self._release(
            self.max_seen_ms
            if self.max_seen_ms is not None
            else self.day_start_s * 1000
        )
        self.builder.finish()
        report["appended"] = self._flush(complete=True)
        self.finalized = True
        self._write_cursor()
        return report

    def _flush(self, complete: bool) -> dict[str, int]:
        """闭桶列落盘：首次覆写，其后追加 gz 成员。"""
        built_at = datetime.now(UTC).isoformat()
        config_hash = tiles_config_hash()
        appended: dict[str, int] = {}
        for bucket, state in self.builder.tiers.items():
            gz_path, meta_path = tile_paths(
                self.data_root, self.venue, self.symbol, bucket, self.date_text
            )
            gz_path.parent.mkdir(parents=True, exist_ok=True)
            fresh = self.fresh_file
            new_columns = state.columns[self.flushed[bucket] :]
            if fresh or new_columns:
                rows = state.columns if fresh else new_columns
                body = gzip.compress(
                    "".join(
                        json.dumps(column, ensure_ascii=False) + "\n"
                        for column in rows
                    ).encode("utf-8")
                )
                if fresh:
                    atomic_write_bytes(gz_path, body)
                else:
                    durable_append_bytes(gz_path, body)
                _write_tile_chunks(
                    self.data_root,
                    self.venue,
                    self.symbol,
                    bucket,
                    self.date_text,
                    self.chunk_generation,
                    rows,
                    state.bucket_seconds,
                    append=not fresh,
                )
            appended[bucket] = len(new_columns) if not fresh else len(
                state.columns
            )
            self.flushed[bucket] = len(state.columns)
            meta = {
                "schema_version": TILE_SCHEMA_VERSION,
                "persistence_version": TILE_PERSISTENCE_VERSION,
                "venue": self.venue,
                "symbol": self.symbol,
                "bucket": bucket,
                "bucket_seconds": state.bucket_seconds,
                "date": self.date_text,
                "tick_size": _text(self.tick_size),
                "row_tier": TILE_ROW_TIERS[bucket],
                "row_bin": _text(state.row_bin),
                "columns": len(state.columns),
                "gap_columns": state.gap_columns,
                "carried_columns": state.carried_columns,
                "frames": self.builder.frames,
                "prints": len(self.builder.prints),
                "complete": complete,
                "built_at": built_at,
                "config_hash": config_hash,
                "source": f"raw/{self.date_text}/ws_public.jsonl",
                "cell_fields": list(CELL_FIELDS),
                "side_basis": SIDE_BASIS,
                "incremental": True,
                "chunk_columns": TILE_CHUNK_COLUMNS,
                "chunk_generation": self.chunk_generation,
            }
            atomic_write_text(
                meta_path,
                json.dumps(meta, ensure_ascii=False, indent=1) + "\n",
            )
        self.fresh_file = False
        ticks = build_print_ticks(
            list(self.builder.prints), PRINT_TICK_QUANTILE
        )
        ticks.update(
            {
                "venue": self.venue,
                "symbol": self.symbol,
                "date": self.date_text,
                "built_at": built_at,
                "config_hash": config_hash,
                "persistence_version": TILE_PERSISTENCE_VERSION,
            }
        )
        tick_path = print_ticks_path(
            self.data_root, self.venue, self.symbol, self.date_text
        )
        tick_path.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_text(
            tick_path, json.dumps(ticks, ensure_ascii=False) + "\n"
        )
        return appended

    def _write_cursor(self) -> None:
        path = cursor_path(
            self.data_root, self.venue, self.symbol, self.date_text
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "schema_version": TILE_SCHEMA_VERSION,
            "persistence_version": TILE_PERSISTENCE_VERSION,
            "venue": self.venue,
            "symbol": self.symbol,
            "date": self.date_text,
            "offset": self.offset,
            "columns": dict(self.flushed),
            "prints": len(self.builder.prints),
            "frames": self.builder.frames,
            "finalized": self.finalized,
            "config_hash": tiles_config_hash(),
            "built_at": datetime.now(UTC).isoformat(),
        }
        atomic_write_text(
            path, json.dumps(payload, ensure_ascii=False, indent=1) + "\n"
        )


class IncrementalTileRegistry:
    """逐品种当日构建器登记，跨日自动收尾换新。"""

    def __init__(
        self,
        data_root: Path,
        symbols: Sequence[str],
        *,
        venue: str = VENUE_GMO,
    ) -> None:
        self.data_root = data_root
        self.symbols = list(symbols)
        self.venue = venue
        self.builders: dict[str, IncrementalTileBuilder] = {}

    def refresh_all(self, now: datetime) -> list[dict[str, object]]:
        """刷新全部品种当日瓦片，日界先收尾昨日。"""
        date_text = now.astimezone(UTC).strftime("%Y-%m-%d")
        reports: list[dict[str, object]] = []
        for symbol in self.symbols:
            held = self.builders.get(symbol)
            if held is not None and held.date_text != date_text:
                reports.append(held.finalize())
                del self.builders[symbol]
                held = None
            if held is None:
                tick = local_tick_size(self.data_root, symbol)
                if tick is None:
                    continue
                held = IncrementalTileBuilder(
                    self.data_root, symbol, date_text, tick, venue=self.venue
                )
                self.builders[symbol] = held
            reports.append(held.refresh())
        return reports


def load_tile_meta(
    data_root: Path, venue: str, symbol: str, bucket: str, date_text: str
) -> dict[str, object] | None:
    """读单日 meta，无文件返回空。"""
    _, meta_path = tile_paths(data_root, venue, symbol, bucket, date_text)
    if not meta_path.exists():
        return None
    loaded = json.loads(meta_path.read_text(encoding="utf-8"))
    return loaded if isinstance(loaded, dict) else None


def iter_tile_columns(
    data_root: Path, venue: str, symbol: str, bucket: str, date_text: str
) -> Iterator[dict[str, object]]:
    """流式读单日列，文件缺失即空序列。"""
    gz_path, _ = tile_paths(data_root, venue, symbol, bucket, date_text)
    if not gz_path.exists():
        return
    with gzip.open(gz_path, "rt", encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            loaded = json.loads(line)
            if isinstance(loaded, dict):
                yield loaded


def iter_tile_columns_range(
    data_root: Path,
    venue: str,
    symbol: str,
    bucket: str,
    date_text: str,
    from_s: int,
    to_s: int,
    meta: Mapping[str, object],
) -> Iterator[dict[str, object]]:
    """优先读取物理块，旧制品回退整日流。"""
    generation = meta.get("chunk_generation")
    chunk_columns = meta.get("chunk_columns")
    bucket_seconds = TILE_BUCKETS.get(bucket)
    if (
        not isinstance(generation, str)
        or chunk_columns != TILE_CHUNK_COLUMNS
        or bucket_seconds is None
    ):
        yield from iter_tile_columns(
            data_root, venue, symbol, bucket, date_text
        )
        return
    span = bucket_seconds * TILE_CHUNK_COLUMNS
    first = from_s // span * span
    for start_s in range(first, to_s, span):
        path = tile_chunk_path(
            data_root,
            venue,
            symbol,
            bucket,
            date_text,
            generation,
            start_s,
        )
        if not path.exists():
            continue
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            for line in fh:
                if not line.strip():
                    continue
                loaded = json.loads(line)
                if isinstance(loaded, dict):
                    yield loaded


def index_day_chunks(
    data_root: Path,
    venue: str,
    symbol: str,
    bucket: str,
    date_text: str,
) -> dict[str, object]:
    """为旧整日制品补物理块，一次扫描后原子切换。"""
    gz_path, meta_path = tile_paths(
        data_root, venue, symbol, bucket, date_text
    )
    meta = load_tile_meta(data_root, venue, symbol, bucket, date_text)
    if meta is None or not gz_path.exists():
        raise FileNotFoundError(f"瓦片制品缺失: {date_text}")
    if (
        isinstance(meta.get("chunk_generation"), str)
        and meta.get("chunk_columns") == TILE_CHUNK_COLUMNS
    ):
        return {"date": date_text, "indexed": False, "chunks": 0}
    stat = gz_path.stat()
    identity = (
        f"{gz_path.as_posix()}|{stat.st_size}|{stat.st_mtime_ns}|"
        f"{meta.get('config_hash', '')}"
    )
    generation = sha256_hex(identity.encode("utf-8"))[:16]
    seconds = TILE_BUCKETS[bucket]
    span = seconds * TILE_CHUNK_COLUMNS
    current_start: int | None = None
    buffered: list[Mapping[str, object]] = []
    chunks = 0

    def flush() -> None:
        nonlocal chunks
        if not buffered:
            return
        _write_tile_chunks(
            data_root,
            venue,
            symbol,
            bucket,
            date_text,
            generation,
            buffered,
            seconds,
            append=False,
        )
        chunks += 1

    for column in iter_tile_columns(
        data_root, venue, symbol, bucket, date_text
    ):
        epoch = column.get("e")
        if not isinstance(epoch, int):
            continue
        start = epoch // span * span
        if current_start is not None and start != current_start:
            flush()
            buffered = []
        current_start = start
        buffered.append(column)
    flush()
    updated = dict(meta)
    updated["chunk_columns"] = TILE_CHUNK_COLUMNS
    updated["chunk_generation"] = generation
    updated["chunk_indexed_at"] = datetime.now(UTC).isoformat()
    updated["persistence_version"] = TILE_PERSISTENCE_VERSION
    atomic_write_text(
        meta_path, json.dumps(updated, ensure_ascii=False, indent=1) + "\n"
    )
    return {"date": date_text, "indexed": True, "chunks": chunks}


def window_dates(from_s: int, to_s: int) -> list[str]:
    """窗口覆盖的 UTC 日期序列。"""
    first = datetime.fromtimestamp(from_s, UTC).date()
    last = datetime.fromtimestamp(max(from_s, to_s - 1), UTC).date()
    out: list[str] = []
    cursor = first
    while cursor <= last:
        out.append(cursor.isoformat())
        cursor += timedelta(days=1)
    return out


def slice_columns(
    data_root: Path,
    venue: str,
    symbol: str,
    bucket: str,
    from_s: int,
    to_s: int,
    max_columns: int = MAX_SLICE_COLUMNS,
) -> dict[str, object]:
    """窗口切片：跨日拼接并按列区间裁剪。"""
    columns: list[dict[str, object]] = []
    missing: list[str] = []
    truncated = False
    meta_seen: dict[str, object] | None = None
    for date_text in window_dates(from_s, to_s):
        meta = load_tile_meta(data_root, venue, symbol, bucket, date_text)
        if meta is None:
            missing.append(date_text)
            continue
        if meta_seen is None:
            meta_seen = meta
        for column in iter_tile_columns_range(
            data_root,
            venue,
            symbol,
            bucket,
            date_text,
            from_s,
            to_s,
            meta,
        ):
            epoch = column.get("e")
            if not isinstance(epoch, int) or not from_s <= epoch < to_s:
                continue
            if len(columns) >= max_columns:
                truncated = True
                break
            columns.append(column)
        if truncated:
            break
    return {
        "columns": columns,
        "meta": {
            "venue": venue,
            "symbol": symbol,
            "bucket": bucket,
            "bucket_seconds": TILE_BUCKETS.get(bucket),
            "row_bin": None if meta_seen is None else meta_seen.get("row_bin"),
            "tick_size": (
                None if meta_seen is None else meta_seen.get("tick_size")
            ),
            "from_ts": datetime.fromtimestamp(from_s, UTC).isoformat(),
            "to_ts": datetime.fromtimestamp(to_s, UTC).isoformat(),
            "columns": len(columns),
            "truncated": truncated,
            "missing_dates": missing,
            "cell_fields": list(CELL_FIELDS),
            "side_basis": SIDE_BASIS,
        },
    }


def _column_qty(column: Mapping[str, object], price_bin: str) -> Decimal | None:
    """列内该档挂量末态，空档列返回空。"""
    if column.get("gap"):
        return None
    cells = column.get("cells")
    if isinstance(cells, list):
        for cell in cells:
            if isinstance(cell, list) and cell and cell[0] == price_bin:
                return Decimal(str(cell[2]))
    return _ZERO


def _cell_values(
    column: Mapping[str, object], price_bin: str
) -> tuple[Decimal, Decimal, Decimal]:
    """列内该档（净增挂, 净撤减, 成交消耗）。"""
    cells = column.get("cells")
    if isinstance(cells, list):
        for cell in cells:
            if isinstance(cell, list) and cell and cell[0] == price_bin:
                return (
                    Decimal(str(cell[3])),
                    Decimal(str(cell[4])),
                    Decimal(str(cell[5])),
                )
    return _ZERO, _ZERO, _ZERO


def level_track(
    columns: Sequence[Mapping[str, object]],
    price_bin: str,
    *,
    reaction_buckets: int = PRICE_REACTION_BUCKETS,
    replenish_lookahead: int = REPLENISH_LOOKAHEAD_BUCKETS,
) -> dict[str, object]:
    """档带追踪（6.4 节）：存续期、挂量史、率值与价格反应。"""
    history: list[dict[str, object]] = []
    quantities: list[Decimal | None] = []
    add_sum = cancel_sum = executed_sum = _ZERO
    adds: list[Decimal] = []
    for column in columns:
        qty = _column_qty(column, price_bin)
        quantities.append(qty)
        history.append(
            {
                "t": column.get("t"),
                "qty": None if qty is None else _text(qty),
            }
        )
        net_add, net_cancel, executed = _cell_values(column, price_bin)
        adds.append(net_add)
        add_sum += net_add
        cancel_sum += net_cancel
        executed_sum += executed
    segments: list[dict[str, object]] = []
    open_at: int | None = None
    vanish_at: int | None = None
    for at, qty in enumerate(quantities):
        present = qty is not None and qty > _ZERO
        if present and open_at is None:
            open_at = at
        elif open_at is not None and qty is not None and qty == _ZERO:
            segments.append(
                {
                    "first_seen": columns[open_at]["t"],
                    "last_seen": columns[at - 1]["t"],
                    "vanished_at": columns[at]["t"],
                    "buckets": at - open_at,
                }
            )
            # 消失即挂量转零的转移桶
            vanish_at = at
            open_at = None
        elif open_at is not None and qty is None:
            # 空档中断存续，不判消失
            segments.append(
                {
                    "first_seen": columns[open_at]["t"],
                    "last_seen": columns[at - 1]["t"],
                    "vanished_at": None,
                    "buckets": at - open_at,
                }
            )
            open_at = None
    if open_at is not None:
        segments.append(
            {
                "first_seen": columns[open_at]["t"],
                "last_seen": columns[len(columns) - 1]["t"],
                "vanished_at": None,
                "buckets": len(columns) - open_at,
            }
        )
    replenish_count = 0
    replenish_size = _ZERO
    for at, column in enumerate(columns):
        _, _, executed = _cell_values(column, price_bin)
        if executed <= _ZERO:
            continue
        for ahead in range(at, min(at + replenish_lookahead + 1, len(columns))):
            if adds[ahead] > _ZERO:
                replenish_count += 1
                replenish_size += adds[ahead]
                break
    reaction: dict[str, object] | None = None
    if vanish_at is not None:
        before_at = vanish_at - reaction_buckets
        after_at = vanish_at + reaction_buckets
        if 0 <= before_at and after_at < len(columns):
            before_mid = columns[before_at].get("mid")
            after_mid = columns[after_at].get("mid")
            if isinstance(before_mid, str) and isinstance(after_mid, str):
                base = Decimal(before_mid)
                change = (
                    (Decimal(after_mid) - base) / base * BP_FACTOR
                ).quantize(BP_PLACES)
                reaction = {
                    "event_t": columns[vanish_at]["t"],
                    "before_mid": before_mid,
                    "after_mid": after_mid,
                    "change_bp": _text(change),
                    "buckets": reaction_buckets,
                }
    cancel_ratio = (
        None if add_sum == _ZERO
        else _text((cancel_sum / add_sum).quantize(RATIO_PLACES))
    )
    return {
        "price_bin": price_bin,
        "history": history,
        "segments": segments,
        "net_add_total": _text(add_sum),
        "net_cancel_total": _text(cancel_sum),
        "executed_total": _text(executed_sum),
        "cancel_ratio": cancel_ratio,
        "replenishment": {
            "count": replenish_count,
            "size": _text(replenish_size),
            "lookahead_buckets": replenish_lookahead,
        },
        "price_reaction": reaction,
    }


def load_print_ticks(
    data_root: Path, venue: str, symbol: str, from_s: int, to_s: int
) -> dict[str, object]:
    """按窗口取成交刻线，跨日拼接。"""
    items: list[dict[str, object]] = []
    missing: list[str] = []
    quantile: str | None = None
    thresholds: dict[str, str] = {}
    for date_text in window_dates(from_s, to_s):
        path = print_ticks_path(data_root, venue, symbol, date_text)
        if not path.exists():
            missing.append(date_text)
            continue
        loaded = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(loaded, dict):
            continue
        quantile = str(loaded.get("quantile"))
        thresholds[date_text] = str(loaded.get("threshold"))
        day_items = loaded.get("items")
        if not isinstance(day_items, list):
            continue
        for item in day_items:
            if not isinstance(item, dict):
                continue
            stamp = item.get("t")
            if not isinstance(stamp, str):
                continue
            epoch = int(datetime.fromisoformat(stamp).timestamp())
            if from_s <= epoch < to_s:
                items.append(item)
    return {
        "items": items,
        "meta": {
            "venue": venue,
            "symbol": symbol,
            "quantile": quantile,
            "thresholds": thresholds,
            "missing_dates": missing,
            "side_basis": SIDE_BASIS,
        },
    }


def raw_dates(data_root: Path) -> list[str]:
    """raw 下具公开流文件的 UTC 日期清单。"""
    root = data_root / "raw"
    if not root.is_dir():
        return []
    out = []
    for day in sorted(root.iterdir()):
        if day.is_dir() and _raw_file(data_root, day.name) is not None:
            out.append(day.name)
    return out


def pending_dates(
    data_root: Path, venue: str, symbol: str
) -> list[str]:
    """求缺：瓦片缺失或未完结的日期。"""
    out = []
    for date_text in raw_dates(data_root):
        metas = [
            load_tile_meta(data_root, venue, symbol, bucket, date_text)
            for bucket in TILE_BUCKETS
        ]
        if any(meta is None or meta.get("complete") is not True for meta in metas):
            out.append(date_text)
    return out


def main(argv: Sequence[str] | None = None) -> int:
    """命令行入口：单日构建或全量求缺。"""
    parser = argparse.ArgumentParser(description="盘口热力瓦片构建")
    sub = parser.add_subparsers(dest="command", required=True)
    p_build = sub.add_parser("build", help="构建瓦片")
    p_build.add_argument("--symbol", default="BTC")
    p_build.add_argument("--venue", default=VENUE_GMO)
    p_build.add_argument("--date", default=None, help="UTC 日 YYYY-MM-DD")
    p_build.add_argument("--tick", default=None, help="tickSize 覆盖")
    p_build.add_argument(
        "--all", action="store_true", help="求缺模式，补全部缺失日"
    )
    p_index = sub.add_parser("index", help="旧瓦片补物理块")
    p_index.add_argument("--symbol", default="BTC")
    p_index.add_argument("--venue", default=VENUE_GMO)
    p_index.add_argument("--bucket", choices=tuple(TILE_BUCKETS), default="1s")
    p_index.add_argument("--date", default=None, help="UTC 日 YYYY-MM-DD")
    p_index.add_argument("--all", action="store_true", help="索引全部旧日")
    args = parser.parse_args(argv)
    data_root = Path("data")
    if args.command == "index":
        if args.all:
            root = tile_dir(data_root, args.venue, args.symbol, args.bucket)
            dates = sorted(
                path.name.removesuffix(".meta.json")
                for path in root.glob("*.meta.json")
            )
        elif args.date is not None:
            dates = [args.date]
        else:
            print("须指定 --date 或 --all")
            return 2
        for date_text in dates:
            report = index_day_chunks(
                data_root,
                args.venue,
                args.symbol,
                args.bucket,
                date_text,
            )
            print(json.dumps(report, ensure_ascii=False))
        return 0
    tick = (
        Decimal(args.tick)
        if args.tick is not None
        else local_tick_size(data_root, args.symbol)
    )
    if tick is None:
        print("无本地取引ルール快照，须以 --tick 指定")
        return 2
    if args.all:
        dates = pending_dates(data_root, args.venue, args.symbol)
    elif args.date is not None:
        dates = [args.date]
    else:
        print("须指定 --date 或 --all")
        return 2
    for date_text in dates:
        report = build_day_tiles(
            data_root, args.symbol, date_text, tick, venue=args.venue
        )
        print(json.dumps(report, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
