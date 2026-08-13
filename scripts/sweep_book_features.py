"""盘口四判定滑窗全量扫描（只读数据，遗漏与假阳对照）。

以固定窗宽与步距系统扫描瓦片全史，逐窗按 footprint-design
6.4 节语义独立重算四类判定（判定实现为本脚本内独立副本，
不引用 guvolu 包），产出全量候选清单、met 事件段聚类、
与库中 book_feature 及 alert_event 的差异对照和分布统计。
价带取窗内中间价中位数上下各带宽 bp，数值以缩放整数精确计算。
基线总体两模式：legacy 为旧语义（全部非空列，含延载列，
达标度均值置信度）；closed 对齐修复后实现语义（基线与窗样本
均排除空档列与延载列，基线样本数下限判据，最弱条件强度公式），
供缺陷 3、4 修复前后 met 率与报警线对照。
"""
from __future__ import annotations

import argparse
import gzip
import json
import sqlite3
import sys
from datetime import UTC, datetime, timedelta
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from pathlib import Path

import numpy as np

ZERO = Decimal("0")
ONE = Decimal("1")
TWO = Decimal("2")
Q2 = Decimal("0.01")
BP_FACTOR = Decimal("10000")
# 量值缩放位数
SCALE = 8
# 合成键档位因子
KEY_BASE = 10**9
# 侧别编码表
SIDE_CODES = {"ask": 0, "bid": 1, "both": 2, "void": 3}

ABSORPTION_EXECUTED_PERCENTILE = Decimal("0.90")
ABSORPTION_REPLENISH_RATIO = Decimal("0.5")
ABSORPTION_MAX_DRIFT_BP = Decimal("5")
PULL_CANCEL_PERCENTILE = Decimal("0.90")
PULL_EXECUTED_RATIO = Decimal("0.25")
SPOOFING_CANCEL_PERCENTILE = Decimal("0.95")
SPOOFING_MAX_LIFETIME_BUCKETS = 60
SWEEP_MIN_LEVELS = 3
SWEEP_MAX_BUCKETS = 3
VACUUM_DEPTH_PERCENTILE = Decimal("0.10")
# closed 模式基线样本数下限
BASELINE_MIN_SAMPLES = 30


def text(value: Decimal) -> str:
    """Decimal 定形输出。"""
    return format(value, "f")


def scaled(value: str) -> int:
    """量值文本转缩放整数。"""
    number = Decimal(value).scaleb(SCALE)
    out = int(number)
    if number != out:
        raise ValueError(f"量值超缩放位: {value}")
    return out


def unscaled(value: int) -> Decimal:
    """缩放整数还原 Decimal。"""
    return Decimal(value).scaleb(-SCALE)


class DayGrid:
    """单日一秒列网格的数组化形态。"""

    def __init__(self, day: str, tile_dir: Path) -> None:
        meta = json.loads(
            (tile_dir / f"{day}.meta.json").read_text(encoding="utf-8")
        )
        self.day = day
        self.columns = int(meta["columns"])
        self.day_start = int(
            datetime.fromisoformat(f"{day}T00:00:00+00:00").timestamp()
        )
        gap = np.zeros(self.columns, dtype=bool)
        carried = np.zeros(self.columns, dtype=bool)
        mid_x10 = np.full(self.columns, -1, dtype=np.int64)
        starts = np.zeros(self.columns + 1, dtype=np.int64)
        bins: list[int] = []
        sides: list[int] = []
        qty: list[int] = []
        add: list[int] = []
        cancel: list[int] = []
        eaten: list[int] = []
        with gzip.open(tile_dir / f"{day}.jsonl.gz", "rt", encoding="utf-8") as fh:
            at = -1
            for line in fh:
                if not line.strip():
                    continue
                at += 1
                column = json.loads(line)
                gap[at] = bool(column.get("gap"))
                carried[at] = bool(column.get("carried"))
                mid = column.get("mid")
                if isinstance(mid, str):
                    mid_x10[at] = int(Decimal(mid).scaleb(1))
                for cell in column.get("cells", []):
                    bins.append(int(cell[0]))
                    sides.append(SIDE_CODES.get(str(cell[1]), 3))
                    qty.append(scaled(cell[2]))
                    add.append(scaled(cell[3]))
                    cancel.append(scaled(cell[4]))
                    eaten.append(scaled(cell[5]))
                starts[at + 1] = len(bins)
        if at + 1 != self.columns:
            raise ValueError(f"{day}: 列数与 meta 不符")
        self.gap = gap
        self.carried = carried
        self.mid_x10 = mid_x10
        self.cell_start = starts
        self.bin = np.array(bins, dtype=np.int64)
        self.side = np.array(sides, dtype=np.int8)
        self.qty = np.array(qty, dtype=np.int64)
        self.add = np.array(add, dtype=np.int64)
        self.cancel = np.array(cancel, dtype=np.int64)
        self.eaten = np.array(eaten, dtype=np.int64)
        positions = np.repeat(
            np.arange(self.columns, dtype=np.int64), np.diff(starts)
        )
        self.pos = positions
        self.comp = positions * KEY_BASE + self.bin
        self.prefix_qty = np.concatenate(
            ([0], np.cumsum(self.qty, dtype=np.int64))
        )
        self.prefix_add = np.concatenate(
            ([0], np.cumsum(self.add, dtype=np.int64))
        )
        self.prefix_cancel = np.concatenate(
            ([0], np.cumsum(self.cancel, dtype=np.int64))
        )
        self.prefix_eaten = np.concatenate(
            ([0], np.cumsum(self.eaten, dtype=np.int64))
        )

    def band_arrays(
        self, lo: int, hi: int
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        """逐列带内四值合计数组。"""
        positions = np.arange(self.columns, dtype=np.int64)
        left = np.searchsorted(self.comp, positions * KEY_BASE + lo)
        right = np.searchsorted(self.comp, positions * KEY_BASE + hi + 1)
        return (
            self.prefix_eaten[right] - self.prefix_eaten[left],
            self.prefix_cancel[right] - self.prefix_cancel[left],
            self.prefix_qty[right] - self.prefix_qty[left],
            self.prefix_add[right] - self.prefix_add[left],
        )


def percentile_rank_int(
    population: np.ndarray, value_num: int, value_den: int
) -> Decimal:
    """不大于分数值的样本占比。"""
    if population.size == 0:
        return ZERO
    below = int(
        np.count_nonzero(population * value_den <= value_num)
    )
    return (Decimal(below) / Decimal(population.size)).quantize(Q2)


def degree_at_least(value: Decimal, threshold: Decimal) -> Decimal:
    """下限阈达标度。"""
    if threshold <= ZERO:
        return ONE
    return min(ONE, value / threshold)


def degree_at_most(value: Decimal, threshold: Decimal) -> Decimal:
    """上限阈达标度。"""
    if value <= threshold:
        return ONE
    if value <= ZERO:
        return ONE
    return min(ONE, threshold / value)


def confidence_of(degrees: list[Decimal]) -> str:
    """legacy 置信度：达标度均值。"""
    if not degrees:
        return "0"
    mean = sum(degrees, ZERO) / Decimal(len(degrees))
    return text(mean.quantize(Q2))


def score_at_least(value: Decimal, threshold: Decimal) -> Decimal:
    """closed 下限条件强度，阈值处为二分之一。"""
    if value < threshold:
        if threshold <= ZERO:
            return ZERO
        return max(ZERO, ONE / 2 * value / threshold)
    if threshold >= ZERO and value <= ONE and threshold < ONE:
        span = ONE - threshold
    else:
        span = max(abs(threshold), ONE)
    return min(ONE, ONE / 2 + (ONE / 2) * (value - threshold) / span)


def score_at_most(value: Decimal, threshold: Decimal) -> Decimal:
    """closed 上限条件强度，阈值处为二分之一。"""
    if value <= threshold:
        if threshold <= ZERO:
            return ONE / 2
        return min(ONE, ONE / 2 + (ONE / 2) * (threshold - value) / threshold)
    if threshold <= ZERO or value <= ZERO:
        return ZERO
    return min(ONE / 2, (ONE / 2) * threshold / value)


def strength_of(scores: list[Decimal]) -> str:
    """closed 置信度：最弱条件强度。"""
    if not scores:
        return "0"
    return text(min(scores).quantize(Q2))


def judge_window(
    grid: DayGrid, start: int, width: int, band_bp: Decimal, closed: bool
) -> dict[str, object] | None:
    """单窗四判定：带自窗内中位中间价推得。

    closed 模式对齐修复后实现：窗样本与基线总体均
    排除空档列与延载列，附基线样本数下限判据与
    最弱条件强度公式；legacy 保持旧语义供对照。
    """
    stop = start + width
    window_gap = grid.gap[start:stop]
    window_carried = grid.carried[start:stop]
    usable = (
        (~window_gap) & (~window_carried) if closed else ~window_gap
    )
    n_samples = int(np.count_nonzero(usable))
    if n_samples == 0:
        return None
    mid_slice = grid.mid_x10[start:stop]
    mid_mask = (mid_slice >= 0) & usable if closed else mid_slice >= 0
    mids = mid_slice[mid_mask]
    if mids.size == 0:
        return None
    center = Decimal(int(np.sort(mids)[mids.size // 2])).scaleb(-1)
    low = center * (ONE - band_bp / BP_FACTOR)
    high = center * (ONE + band_bp / BP_FACTOR)
    lo = int(low.to_integral_value(rounding=ROUND_CEILING))
    hi = int(high.to_integral_value(rounding=ROUND_FLOOR))
    day_eaten, day_cancel, day_qty, day_add = grid.band_arrays(lo, hi)
    keep = (~grid.gap) & (~grid.carried) if closed else ~grid.gap
    n_pop = int(np.count_nonzero(keep))
    pop_eaten = day_eaten[keep]
    pop_cancel = day_cancel[keep]
    pop_depth = day_qty[keep]
    exec_sum = int(day_eaten[start:stop][usable].sum())
    cancel_sum = int(day_cancel[start:stop][usable].sum())
    add_sum = int(day_add[start:stop][usable].sum())
    baseline_ok = (not closed) or n_pop >= BASELINE_MIN_SAMPLES
    baseline_score = score_at_least(
        Decimal(n_pop), Decimal(BASELINE_MIN_SAMPLES)
    )
    # 判定输入列位置序列：legacy 全列，
    # closed 剔空档与延载列
    scan_positions = [
        position
        for position in range(start, stop)
        if not closed
        or (not grid.gap[position] and not grid.carried[position])
    ]
    first_mid = Decimal(int(mids[0])).scaleb(-1)
    last_mid = Decimal(int(mids[-1])).scaleb(-1)
    out: dict[str, object] = {
        "start": grid.day_start + start,
        "n_samples": n_samples,
        "gap_columns": width - n_samples,
        "band": [str(lo), str(hi)],
        "center": text(center),
        "baseline_samples": n_pop,
    }
    rank = percentile_rank_int(pop_eaten, exec_sum, n_samples)
    executed_total = unscaled(exec_sum)
    net_add_total = unscaled(add_sum)
    replenish_need = executed_total * ABSORPTION_REPLENISH_RATIO
    drift_bp = ZERO
    if mids.size >= 2 and first_mid > ZERO:
        drift_bp = abs(
            (last_mid - first_mid) / first_mid * BP_FACTOR
        ).quantize(Q2)
    active = exec_sum > 0
    met_a = (
        active
        and baseline_ok
        and rank >= ABSORPTION_EXECUTED_PERCENTILE
        and net_add_total >= replenish_need
        and drift_bp <= ABSORPTION_MAX_DRIFT_BP
    )
    if not active:
        conf_a = "0"
    elif closed:
        conf_a = strength_of(
            [
                baseline_score,
                score_at_least(rank, ABSORPTION_EXECUTED_PERCENTILE),
                score_at_least(net_add_total, replenish_need),
                score_at_most(drift_bp, ABSORPTION_MAX_DRIFT_BP),
            ]
        )
    else:
        conf_a = confidence_of(
            [
                degree_at_least(rank, ABSORPTION_EXECUTED_PERCENTILE),
                degree_at_least(net_add_total, replenish_need),
                degree_at_most(drift_bp, ABSORPTION_MAX_DRIFT_BP),
            ]
        )
    out["absorption"] = {
        "met": bool(met_a),
        "confidence": conf_a,
        "executed_total": text(executed_total),
        "executed_percentile": text(rank),
        "net_add_total": text(net_add_total),
        "mid_drift_bp": text(drift_bp),
    }
    rank_c = percentile_rank_int(pop_cancel, cancel_sum, n_samples)
    center_band = (low + high) / TWO
    approaching = bool(
        mids.size >= 2
        and abs(last_mid - center_band) < abs(first_mid - center_band)
    )
    net_cancel_total = unscaled(cancel_sum)
    executed_cap = net_cancel_total * PULL_EXECUTED_RATIO
    active_c = cancel_sum > 0
    met_p = (
        active_c
        and baseline_ok
        and approaching
        and rank_c >= PULL_CANCEL_PERCENTILE
        and executed_total <= executed_cap
    )
    if not active_c:
        conf_p = "0"
    elif closed:
        conf_p = strength_of(
            [
                baseline_score,
                ONE if approaching else ZERO,
                score_at_least(rank_c, PULL_CANCEL_PERCENTILE),
                score_at_most(executed_total, executed_cap),
            ]
        )
    else:
        conf_p = confidence_of(
            [
                ONE if approaching else ZERO,
                degree_at_least(rank_c, PULL_CANCEL_PERCENTILE),
                degree_at_most(executed_total, executed_cap),
            ]
        )
    spoofing = _spoofing(grid, scan_positions, lo, hi, pop_cancel)
    out["pull"] = {
        "met": bool(met_p),
        "confidence": conf_p,
        "net_cancel_total": text(net_cancel_total),
        "cancel_percentile": text(rank_c),
        "approaching": approaching,
        "spoofing_suspicion": spoofing,
    }
    best_run, best_span, best_side = _sweep_run(grid, scan_positions, lo, hi)
    met_s = best_run >= SWEEP_MIN_LEVELS and baseline_ok
    if not best_run:
        conf_s = "0"
    elif closed:
        conf_s = strength_of(
            [
                baseline_score,
                score_at_least(
                    Decimal(best_run), Decimal(SWEEP_MIN_LEVELS)
                ),
                score_at_most(
                    Decimal(best_span), Decimal(SWEEP_MAX_BUCKETS)
                ),
            ]
        )
    else:
        conf_s = confidence_of(
            [degree_at_least(Decimal(best_run), Decimal(SWEEP_MIN_LEVELS))]
        )
    out["sweep"] = {
        "met": bool(met_s),
        "confidence": conf_s,
        "levels_broken": str(best_run),
        "bucket_span": str(best_span),
        "side": best_side,
    }
    window_depth = day_qty[start:stop][usable]
    min_depth = int(window_depth.min())
    rank_v = percentile_rank_int(pop_depth, min_depth, 1)
    met_v = rank_v <= VACUUM_DEPTH_PERCENTILE and baseline_ok
    if closed:
        conf_v = strength_of(
            [
                baseline_score,
                score_at_most(rank_v, VACUUM_DEPTH_PERCENTILE),
            ]
        )
    else:
        conf_v = confidence_of(
            [degree_at_most(rank_v, VACUUM_DEPTH_PERCENTILE)]
        )
    out["liquidity_vacuum"] = {
        "met": bool(met_v),
        "confidence": conf_v,
        "min_depth": text(unscaled(min_depth)),
        "depth_percentile": text(rank_v),
    }
    return out


def _cells_of(grid: DayGrid, position: int) -> slice:
    """列的格片段。"""
    return slice(
        int(grid.cell_start[position]), int(grid.cell_start[position + 1])
    )


def _spoofing(
    grid: DayGrid, scan_positions: list[int], lo: int, hi: int,
    pop_cancel: np.ndarray,
) -> bool:
    """嫌疑强形态：大额短存续撤减（列序按入样列）。"""
    best_cancel = 0
    best_bin = -1
    best_at = -1
    for at, position in enumerate(scan_positions):
        cells = _cells_of(grid, position)
        bins = grid.bin[cells]
        cancels = grid.cancel[cells]
        mask = (bins >= lo) & (bins <= hi) & (cancels > 0)
        if not mask.any():
            continue
        masked = np.where(mask, cancels, 0)
        top = int(np.argmax(masked))
        if int(masked[top]) > best_cancel:
            best_cancel = int(masked[top])
            best_bin = int(bins[top])
            best_at = at
    if best_bin < 0 or best_cancel <= 0:
        return False
    rank = percentile_rank_int(pop_cancel, best_cancel, 1)
    if rank < SPOOFING_CANCEL_PERCENTILE:
        return False
    lifetime = 0
    for position in scan_positions[:best_at]:
        cells = _cells_of(grid, position)
        segment = grid.bin[cells]
        found = np.searchsorted(segment, best_bin)
        present = bool(
            found < segment.size
            and segment[found] == best_bin
            and grid.qty[cells][found] > 0
        )
        lifetime = lifetime + 1 if present else 0
    return 0 < lifetime <= SPOOFING_MAX_LIFETIME_BUCKETS


def _sweep_run(
    grid: DayGrid, scan_positions: list[int], lo: int, hi: int
) -> tuple[int, int, str]:
    """带内被击穿连续档的最优游程（列序按入样列）。"""
    swept: dict[int, dict[int, int]] = {0: {}, 1: {}}
    for at, position in enumerate(scan_positions):
        cells = _cells_of(grid, position)
        bins = grid.bin[cells]
        mask = (
            (bins >= lo) & (bins <= hi)
            & (grid.eaten[cells] > 0) & (grid.qty[cells] == 0)
        )
        if not mask.any():
            continue
        for offset in np.nonzero(mask)[0]:
            side = int(grid.side[cells][offset])
            if side in swept:
                swept[side].setdefault(int(bins[offset]), at)
    best_run = 0
    best_span = 0
    best_side = ""
    for side, hits in swept.items():
        bins_sorted = sorted(hits)
        run_start = 0
        for at in range(len(bins_sorted)):
            if at > 0 and bins_sorted[at] - bins_sorted[at - 1] != 1:
                run_start = at
            length = at - run_start + 1
            if length >= 2:
                ats = [hits[b] for b in bins_sorted[run_start: at + 1]]
                span = max(ats) - min(ats) + 1
            else:
                span = 1
            if length > best_run and span <= SWEEP_MAX_BUCKETS:
                best_run = length
                best_span = span
                best_side = "ask" if side == 0 else "bid"
    return best_run, best_span, best_side


def load_db(
    db_path: Path, symbol: str
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """读库中判读事件与报警行。"""
    conn = sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True)
    features = [
        dict(
            zip(
                ("feature_id", "kind", "price_low", "price_high",
                 "from_ts", "to_ts", "metrics"),
                row,
            )
        )
        for row in conn.execute(
            "SELECT feature_id, kind, price_low, price_high, from_ts, "
            "to_ts, metrics FROM book_feature WHERE symbol=? "
            "ORDER BY feature_id",
            (symbol,),
        )
    ]
    alerts = [
        dict(zip(("alert_id", "feature_id", "rule_id"), row))
        for row in conn.execute(
            "SELECT alert_id, feature_id, rule_id FROM alert_event "
            "ORDER BY alert_id"
        )
    ]
    conn.close()
    for row in features:
        row["from_s"] = int(
            datetime.fromisoformat(str(row["from_ts"])).timestamp()
        )
        row["to_s"] = int(
            datetime.fromisoformat(str(row["to_ts"])).timestamp()
        )
    return features, alerts


def load_rules(path: Path) -> list[dict[str, object]]:
    """读报警规则实例配置。"""
    loaded = json.loads(path.read_text(encoding="utf-8"))
    rows = loaded.get("rules") if isinstance(loaded, dict) else None
    return rows if isinstance(rows, list) else []


def rule_floor(rules: list[dict[str, object]], kind: str) -> Decimal | None:
    """该种类启用规则的置信度门槛。"""
    for rule in rules:
        if rule.get("enabled") and rule.get("kind") == kind:
            overrides = rule.get("overrides")
            floor_text = "0"
            if isinstance(overrides, dict):
                floor_text = str(overrides.get("min_confidence", "0"))
            return Decimal(floor_text)
    return None


def cluster_episodes(
    windows: list[dict[str, object]], kind: str, step: int, width: int
) -> list[dict[str, object]]:
    """met 相邻窗聚为事件段。"""
    episodes: list[dict[str, object]] = []
    current: dict[str, object] | None = None
    for window in windows:
        block = window[kind]
        assert isinstance(block, dict)
        if not block["met"]:
            continue
        start = window["start"]
        assert isinstance(start, int)
        conf = Decimal(str(block["confidence"]))
        if current is not None and start - current["last_start"] <= step:
            current["last_start"] = start
            current["windows"] = int(current["windows"]) + 1
            current["max_confidence"] = max(
                Decimal(str(current["max_confidence"])), conf
            )
        else:
            if current is not None:
                episodes.append(current)
            current = {
                "kind": kind,
                "first_start": start,
                "last_start": start,
                "windows": 1,
                "max_confidence": conf,
                "band": window["band"],
            }
    if current is not None:
        episodes.append(current)
    for episode in episodes:
        episode["from_ts"] = datetime.fromtimestamp(
            int(episode["first_start"]), UTC
        ).isoformat()
        episode["to_ts"] = datetime.fromtimestamp(
            int(episode["last_start"]) + width, UTC
        ).isoformat()
        episode["max_confidence"] = str(episode["max_confidence"])
    return episodes


def overlaps(
    window: dict[str, object], width: int, feature: dict[str, object]
) -> bool:
    """窗与库行在时段与价带双向重叠。"""
    start = int(str(window["start"]))
    band = window["band"]
    assert isinstance(band, list)
    lo, hi = int(band[0]), int(band[1])
    f_lo = int(Decimal(str(feature["price_low"])))
    f_hi = int(Decimal(str(feature["price_high"])))
    time_hit = start < feature["to_s"] and start + width > feature["from_s"]
    band_hit = lo <= f_hi and f_lo <= hi
    return time_hit and band_hit


def main() -> int:
    """主流程：逐日扫描、聚类、对照、统计。"""
    parser = argparse.ArgumentParser(description="盘口判定滑窗扫描")
    parser.add_argument("--data-root", default="data")
    parser.add_argument("--symbol", default="BTC")
    parser.add_argument("--from-date", default="2026-08-07")
    parser.add_argument("--to-date", default="2026-08-09")
    parser.add_argument("--window-seconds", type=int, default=300)
    parser.add_argument("--step-seconds", type=int, default=60)
    parser.add_argument("--band-bp", default="25")
    parser.add_argument("--bucket", default="1s")
    parser.add_argument(
        "--baseline-population",
        choices=("legacy", "closed"),
        default="closed",
        help="基线总体：legacy 含延载列，closed 对齐实现",
    )
    parser.add_argument("--out", default="-")
    args = parser.parse_args()
    closed = args.baseline_population == "closed"
    root = Path(args.data_root)
    tile_dir = (
        root / "derived" / "heatmap_tiles" / "gmo" / args.symbol / args.bucket
    )
    band_bp = Decimal(args.band_bp)
    width = args.window_seconds
    step = args.step_seconds
    first = datetime.fromisoformat(f"{args.from_date}T00:00:00+00:00")
    last = datetime.fromisoformat(f"{args.to_date}T00:00:00+00:00")
    windows: list[dict[str, object]] = []
    skipped = 0
    days_used: list[dict[str, object]] = []
    cursor = first
    while cursor <= last:
        day = cursor.date().isoformat()
        cursor += timedelta(days=1)
        if not (tile_dir / f"{day}.meta.json").exists():
            continue
        grid = DayGrid(day, tile_dir)
        evaluated = 0
        for start in range(0, grid.columns - width + 1, step):
            result = judge_window(grid, start, width, band_bp, closed)
            if result is None:
                skipped += 1
                continue
            evaluated += 1
            windows.append(result)
        days_used.append(
            {
                "day": day,
                "columns": grid.columns,
                "gap_columns": int(np.count_nonzero(grid.gap)),
                "windows_evaluated": evaluated,
            }
        )
    kinds = ("absorption", "pull", "sweep", "liquidity_vacuum")
    features, alerts = load_db(root / "guvolu.sqlite3", args.symbol)
    rules = load_rules(Path("config") / "alert_rules.json")
    stats: dict[str, object] = {}
    episodes_all: list[dict[str, object]] = []
    for kind in kinds:
        met_windows = [w for w in windows if w[kind]["met"]]
        confs = sorted(
            Decimal(str(w[kind]["confidence"])) for w in met_windows
        )
        episodes = cluster_episodes(windows, kind, step, width)
        episodes_all.extend(episodes)
        floor = rule_floor(rules, kind)
        rule_hits = [
            w for w in met_windows
            if floor is not None
            and Decimal(str(w[kind]["confidence"])) >= floor
        ]
        hour_hits: dict[str, int] = {}
        for w in met_windows:
            start = int(str(w["start"]))
            hour = datetime.fromtimestamp(start, UTC).strftime(
                "%m-%d %H"
            )
            hour_hits[hour] = hour_hits.get(hour, 0) + 1
        db_rows = [f for f in features if f["kind"] == kind]
        corroborated = []
        uncorroborated = []
        for feature in db_rows:
            hit = any(
                overlaps(w, width, feature) for w in met_windows
            )
            (corroborated if hit else uncorroborated).append(
                feature["feature_id"]
            )
        omitted = [
            e for e in episodes
            if not any(
                int(e["first_start"]) < f["to_s"]
                and int(e["last_start"]) + width > f["from_s"]
                for f in db_rows
            )
        ]
        stats[kind] = {
            "windows_met": len(met_windows),
            "met_rate": (
                text(
                    (
                        Decimal(len(met_windows)) / Decimal(len(windows))
                    ).quantize(Decimal("0.0001"))
                )
                if windows
                else "0"
            ),
            "confidence_min": text(confs[0]) if confs else None,
            "confidence_median": (
                text(confs[len(confs) // 2]) if confs else None
            ),
            "confidence_max": text(confs[-1]) if confs else None,
            "episodes": len(episodes),
            "episodes_omitted_vs_db": len(omitted),
            "db_rows": len(db_rows),
            "db_corroborated": corroborated,
            "db_uncorroborated": uncorroborated,
            "rule_floor": None if floor is None else text(floor),
            "windows_would_alert": len(rule_hits),
            "hour_clusters": dict(
                sorted(hour_hits.items(), key=lambda kv: -kv[1])[:8]
            ),
        }
    actual_by_kind: dict[str, int] = {}
    feature_kind = {f["feature_id"]: f["kind"] for f in features}
    for alert in alerts:
        kind = feature_kind.get(alert["feature_id"], "")
        actual_by_kind[kind] = actual_by_kind.get(kind, 0) + 1
    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "parameters": {
            "symbol": args.symbol,
            "bucket": args.bucket,
            "window_seconds": width,
            "step_seconds": step,
            "band_bp": str(band_bp),
            "band_center": "窗内入样列中间价中位数",
            "window_grid": "自 UTC 日起点按步距对齐，尾部不足整窗弃之",
            "baseline_population": args.baseline_population,
            "population": (
                "同日同带非空非延载列，基线样本数下限判据"
                if closed
                else "同日同带全部非空列（含延载列）"
            ),
            "confidence_formula": (
                "rule-strength-min-v2" if closed else "degree-mean-v1"
            ),
            "from_date": args.from_date,
            "to_date": args.to_date,
        },
        "days": days_used,
        "windows_evaluated": len(windows),
        "windows_skipped_empty": skipped,
        "stats": stats,
        "alert_actual_by_kind": actual_by_kind,
        "episodes": episodes_all,
        "windows": windows,
    }
    body = json.dumps(report, ensure_ascii=False, default=str)
    if args.out == "-":
        sys.stdout.write(body + "\n")
    else:
        Path(args.out).write_text(body + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
