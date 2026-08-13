"""既有 book_feature 与 alert_event 行独立复核（只读数据）。

从 raw 帧与逐笔独立重建一秒列网格，不引用 guvolu 包，
先对瓦片文件做全列恒等式对照，再按 footprint-design 6.4 节
判定语义重算库中各行指标值、met、置信度与配置散列，
并核对报警规则匹配一致性。结果 JSON 输出。
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import sqlite3
import sys
from datetime import UTC, datetime
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from pathlib import Path

ZERO = Decimal("0")
ONE = Decimal("1")
TWO = Decimal("2")
BP_FACTOR = Decimal("10000")
Q2 = Decimal("0.01")
# 快照延载上限毫秒
CARRY_LIMIT_MS = 30000
# 帧重排缓冲毫秒
REORDER_WINDOW_MS = 5000
# 行档宽一日元
ROW_BIN = 1

# 判定阈值独立副本，供散列对照
REGION_CONFIG = {
    "absorption_executed_percentile": "0.90",
    "absorption_replenish_ratio": "0.5",
    "absorption_max_drift_bp": "5",
    "pull_cancel_percentile": "0.90",
    "pull_executed_ratio": "0.25",
    "spoofing_cancel_percentile": "0.95",
    "spoofing_max_lifetime_buckets": "60",
    "sweep_min_levels": "3",
    "sweep_max_buckets": "3",
    "vacuum_depth_percentile": "0.10",
}
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


def text(value: Decimal) -> str:
    """Decimal 定形输出。"""
    return format(value, "f")


def config_hash_expected() -> str:
    """判定配置散列独立重算。"""
    body = json.dumps(REGION_CONFIG, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def epoch_ms_of(stamp: str) -> int:
    """时戳文本转毫秒。"""
    moment = datetime.fromisoformat(stamp)
    return (
        int(moment.replace(microsecond=0).timestamp()) * 1000
        + moment.microsecond // 1000
    )


def parse_day_raw(path: Path, symbol: str) -> dict[str, object]:
    """单趟读当日公开流，帧去重、逐笔成对合一。"""
    frames: list[tuple[int, int, list, list]] = []
    seen: set[str] = set()
    trade_groups: dict[tuple[str, str, str], list[int]] = {}
    trade_seen = 0
    seq = 0
    max_epoch = 0
    stragglers = 0
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            if '"channel": "orderbooks"' in line:
                record = json.loads(line)
                if record.get("channel") != "orderbooks":
                    continue
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
                    epoch_ms = epoch_ms_of(stamp)
                except ValueError:
                    continue
                if max_epoch and epoch_ms < max_epoch - REORDER_WINDOW_MS:
                    stragglers += 1
                max_epoch = max(max_epoch, epoch_ms)
                frames.append(
                    (epoch_ms, seq, payload.get("asks"), payload.get("bids"))
                )
                seq += 1
            elif '"channel": "trades"' in line:
                record = json.loads(line)
                if record.get("channel") != "trades":
                    continue
                payload = record.get("payload")
                if not isinstance(payload, dict):
                    continue
                if str(payload.get("symbol", "")) != symbol:
                    continue
                key = (
                    str(payload.get("timestamp", "")),
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
    frames.sort(key=lambda item: (item[0], item[1]))
    ordered: list[tuple[int, int, str, str, int]] = []
    for (stamp, price, size), (first, buys, sells) in trade_groups.items():
        ordered.append(
            (epoch_ms_of(stamp), first, price, size, max(buys, sells))
        )
    ordered.sort(key=lambda item: (item[0], item[1]))
    prints: list[tuple[int, Decimal, Decimal, str]] = []
    previous: Decimal | None = None
    side_now = "BUY"
    for epoch_ms, _, price_text, size_text, matches in ordered:
        price = Decimal(price_text)
        if previous is not None:
            if price > previous:
                side_now = "BUY"
            elif price < previous:
                side_now = "SELL"
        for _ in range(matches):
            prints.append((epoch_ms, price, Decimal(size_text), side_now))
        previous = price
    return {"frames": frames, "prints": prints, "stragglers": stragglers}


def binned(levels: object) -> dict[int, tuple[Decimal, Decimal]]:
    """帧档按一日元行档聚合双侧。"""
    out: dict[int, tuple[Decimal, Decimal]] = {}
    if isinstance(levels, tuple):
        asks_raw, bids_raw = levels
    else:
        asks_raw, bids_raw = [], []
    for raw, at in ((asks_raw, 0), (bids_raw, 1)):
        if not isinstance(raw, list):
            continue
        for level in raw:
            if not isinstance(level, dict):
                continue
            price = Decimal(str(level["price"]))
            size = Decimal(str(level["size"]))
            key = int(price.to_integral_value(rounding=ROUND_FLOOR))
            ask, bid = out.get(key, (ZERO, ZERO))
            out[key] = (ask + size, bid) if at == 0 else (ask, bid + size)
    return out


def cell_side(ask: Decimal, bid: Decimal) -> str:
    """格侧别判定。"""
    if ask > ZERO and bid > ZERO:
        return "both"
    if ask > ZERO:
        return "ask"
    if bid > ZERO:
        return "bid"
    return "void"


def frame_mid(asks: object, bids: object) -> Decimal | None:
    """帧中间价，缺侧为空。"""
    if not (isinstance(asks, list) and asks and isinstance(bids, list) and bids):
        return None
    best_ask = Decimal(str(asks[0]["price"]))
    best_bid = Decimal(str(bids[0]["price"]))
    return (best_ask + best_bid) / TWO


def build_columns(
    frames: list, prints: list, day_start: int, horizon_s: int
) -> list[dict[str, object]]:
    """独立重建一秒列网格，语义同瓦片构建。"""
    by_second: dict[int, list] = {}
    for epoch_ms, seq, asks, bids in frames:
        second = epoch_ms // 1000
        if second < day_start or second >= horizon_s:
            continue
        by_second.setdefault(second - day_start, []).append(
            (epoch_ms, seq, asks, bids)
        )
    prints_by_second: dict[int, list] = {}
    for epoch_ms, price, size, side in prints:
        second = epoch_ms // 1000
        if second < day_start or second >= horizon_s:
            continue
        prints_by_second.setdefault(second - day_start, []).append(
            (epoch_ms, price, size, side)
        )
    columns: list[dict[str, object]] = []
    prev_levels: dict[int, tuple[Decimal, Decimal]] | None = None
    last_frame_ms: int | None = None
    carried_mid: Decimal | None = None
    for at in range(horizon_s - day_start):
        bucket = by_second.get(at)
        bucket_prints = prints_by_second.get(at, [])
        if bucket:
            last = bucket[-1]
            end_levels = binned((last[2], last[3]))
            reset = prev_levels is None
            if reset:
                first = bucket[0]
                start_levels = binned((first[2], first[3]))
            else:
                start_levels = prev_levels
            executed: dict[int, Decimal] = {}
            for _, price, size, _side in bucket_prints:
                key = int(price.to_integral_value(rounding=ROUND_FLOOR))
                executed[key] = executed.get(key, ZERO) + size
            keys = sorted(set(start_levels) | set(end_levels) | set(executed))
            cells = []
            for key in keys:
                ask0, bid0 = start_levels.get(key, (ZERO, ZERO))
                ask1, bid1 = end_levels.get(key, (ZERO, ZERO))
                before = ask0 + bid0
                after = ask1 + bid1
                eaten = executed.get(key, ZERO)
                if before == after == eaten == ZERO:
                    continue
                residual = after - before + eaten
                net_add = residual if residual > ZERO else ZERO
                net_cancel = -residual if residual < ZERO else ZERO
                side = cell_side(ask1, bid1)
                if side == "void":
                    side = cell_side(ask0, bid0)
                cells.append((key, side, after, net_add, net_cancel, eaten))
            mid = frame_mid(last[2], last[3])
            columns.append(
                {
                    "e": day_start + at,
                    "gap": False,
                    "carried": False,
                    "reset": reset,
                    "frames": len(bucket),
                    "mid": mid,
                    "cells": cells,
                }
            )
            prev_levels = end_levels
            carried_mid = mid
            last_frame_ms = last[0]
        else:
            bucket_end_ms = (day_start + at + 1) * 1000
            carried = (
                last_frame_ms is not None
                and bucket_end_ms - last_frame_ms <= CARRY_LIMIT_MS
            )
            if carried:
                levels = prev_levels if prev_levels is not None else {}
                cells = [
                    (key, cell_side(ask, bid), ask + bid, ZERO, ZERO, ZERO)
                    for key, (ask, bid) in sorted(levels.items())
                ]
                columns.append(
                    {
                        "e": day_start + at,
                        "gap": False,
                        "carried": True,
                        "reset": False,
                        "frames": 0,
                        "mid": carried_mid,
                        "cells": cells,
                    }
                )
            else:
                columns.append(
                    {
                        "e": day_start + at,
                        "gap": True,
                        "carried": False,
                        "reset": False,
                        "frames": 0,
                        "mid": None,
                        "cells": [],
                    }
                )
                prev_levels = None
    return columns


def compare_tiles(
    columns: list[dict[str, object]], tile_path: Path
) -> dict[str, object]:
    """全列恒等式对照瓦片文件。"""
    mismatches: list[str] = []
    count = 0
    with gzip.open(tile_path, "rt", encoding="utf-8") as fh:
        for at, line in enumerate(fh):
            if not line.strip():
                continue
            tile = json.loads(line)
            if at >= len(columns):
                mismatches.append(f"列 {at}: 重建缺列")
                continue
            mine = columns[at]
            count += 1
            for flag in ("gap", "carried", "reset"):
                if bool(tile.get(flag)) != bool(mine[flag]):
                    mismatches.append(f"列 {at}: 旗标 {flag} 不一致")
            tile_mid = tile.get("mid")
            mine_mid = mine["mid"]
            if (tile_mid is None) != (mine_mid is None):
                mismatches.append(f"列 {at}: mid 有无不一致")
            elif tile_mid is not None and Decimal(tile_mid) != mine_mid:
                mismatches.append(f"列 {at}: mid 值不一致")
            tile_cells = {
                str(cell[0]): cell for cell in tile.get("cells", [])
            }
            mine_cells = {str(cell[0]): cell for cell in mine["cells"]}
            if set(tile_cells) != set(mine_cells):
                mismatches.append(f"列 {at}: 档集不一致")
                continue
            for key, tcell in tile_cells.items():
                mcell = mine_cells[key]
                if str(tcell[1]) != mcell[1]:
                    mismatches.append(f"列 {at} 档 {key}: 侧别不一致")
                for slot in (2, 3, 4, 5):
                    if Decimal(str(tcell[slot])) != mcell[slot]:
                        mismatches.append(
                            f"列 {at} 档 {key}: 值 {slot} 不一致"
                        )
            if len(mismatches) > 200:
                break
    return {
        "columns_compared": count,
        "extra_rebuilt": max(0, len(columns) - count),
        "mismatches": len(mismatches),
        "first_mismatches": mismatches[:10],
    }


def band_bounds(low: Decimal, high: Decimal) -> tuple[int, int]:
    """价带整数档界。"""
    lo = int(low.to_integral_value(rounding=ROUND_CEILING))
    hi = int(high.to_integral_value(rounding=ROUND_FLOOR))
    return lo, hi


def band_cells(column: dict[str, object], lo: int, hi: int) -> list:
    """列内带内格序列。"""
    return [cell for cell in column["cells"] if lo <= cell[0] <= hi]


def band_sample(
    column: dict[str, object], lo: int, hi: int
) -> tuple[Decimal, Decimal, Decimal] | None:
    """列带内合计（消耗、净撤减、挂量）。"""
    if column["gap"]:
        return None
    executed = net_cancel = depth = ZERO
    for cell in band_cells(column, lo, hi):
        executed += cell[5]
        net_cancel += cell[4]
        depth += cell[2]
    return executed, net_cancel, depth


def percentile_rank(population: list[Decimal], value: Decimal) -> Decimal:
    """不大于该值的样本占比。"""
    if not population:
        return ZERO
    below = sum(1 for item in population if item <= value)
    return (Decimal(below) / Decimal(len(population))).quantize(Q2)


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
    """置信度为达标度均值。"""
    if not degrees:
        return "0"
    mean = sum(degrees, ZERO) / Decimal(len(degrees))
    return text(mean.quantize(Q2))


def judge_region(
    region_columns: list[dict[str, object]],
    day_columns: list[dict[str, object]],
    low: Decimal,
    high: Decimal,
) -> list[dict[str, object]]:
    """四类判定独立重算，语义同 6.4 节。"""
    lo, hi = band_bounds(low, high)
    samples = [
        sample
        for sample in (band_sample(col, lo, hi) for col in region_columns)
        if sample is not None
    ]
    executed_sum = sum((s[0] for s in samples), ZERO)
    cancel_sum = sum((s[1] for s in samples), ZERO)
    add_sum = ZERO
    for column in region_columns:
        for cell in band_cells(column, lo, hi):
            add_sum += cell[3]
    mids = [col["mid"] for col in region_columns if col["mid"] is not None]
    population = [
        sample
        for sample in (band_sample(col, lo, hi) for col in day_columns)
        if sample is not None
    ]
    exec_population = [s[0] for s in population]
    cancel_population = [s[1] for s in population]
    depth_population = [s[2] for s in population]
    mean_executed = (
        executed_sum / Decimal(len(samples)) if samples else ZERO
    )
    mean_cancel = cancel_sum / Decimal(len(samples)) if samples else ZERO
    out = []
    rank = percentile_rank(exec_population, mean_executed)
    replenish_need = executed_sum * ABSORPTION_REPLENISH_RATIO
    drift_bp = ZERO
    if len(mids) >= 2 and mids[0] > ZERO:
        drift_bp = abs(
            (mids[-1] - mids[0]) / mids[0] * BP_FACTOR
        ).quantize(Q2)
    active = bool(samples) and executed_sum > ZERO
    met = (
        active
        and rank >= ABSORPTION_EXECUTED_PERCENTILE
        and add_sum >= replenish_need
        and drift_bp <= ABSORPTION_MAX_DRIFT_BP
    )
    degrees = [
        degree_at_least(rank, ABSORPTION_EXECUTED_PERCENTILE),
        degree_at_least(add_sum, replenish_need),
        degree_at_most(drift_bp, ABSORPTION_MAX_DRIFT_BP),
    ]
    out.append(
        {
            "kind": "absorption",
            "met": met,
            "confidence": confidence_of(degrees) if active else "0",
            "metrics": {
                "executed_total": text(executed_sum),
                "executed_percentile": text(rank),
                "net_add_total": text(add_sum),
                "replenish_need": text(replenish_need),
                "mid_drift_bp": text(drift_bp),
            },
        }
    )
    rank_c = percentile_rank(cancel_population, mean_cancel)
    center = (low + high) / TWO
    approaching = False
    if len(mids) >= 2:
        approaching = abs(mids[-1] - center) < abs(mids[0] - center)
    executed_cap = cancel_sum * PULL_EXECUTED_RATIO
    active_c = bool(samples) and cancel_sum > ZERO
    met_c = (
        active_c
        and approaching
        and rank_c >= PULL_CANCEL_PERCENTILE
        and executed_sum <= executed_cap
    )
    degrees_c = [
        ONE if approaching else ZERO,
        degree_at_least(rank_c, PULL_CANCEL_PERCENTILE),
        degree_at_most(executed_sum, executed_cap),
    ]
    best_at = -1
    best_bin: int | None = None
    best_cancel = ZERO
    for at, column in enumerate(region_columns):
        for cell in band_cells(column, lo, hi):
            if cell[4] > best_cancel:
                best_cancel = cell[4]
                best_bin = cell[0]
                best_at = at
    spoofing = False
    if best_bin is not None and best_cancel > ZERO:
        srank = percentile_rank(cancel_population, best_cancel)
        if srank >= SPOOFING_CANCEL_PERCENTILE:
            lifetime = 0
            for column in region_columns[:best_at]:
                present = False
                for cell in band_cells(column, lo, hi):
                    if cell[0] == best_bin and cell[2] > ZERO:
                        present = True
                        break
                lifetime = lifetime + 1 if present else 0
            spoofing = 0 < lifetime <= SPOOFING_MAX_LIFETIME_BUCKETS
    out.append(
        {
            "kind": "pull",
            "met": met_c,
            "confidence": confidence_of(degrees_c) if active_c else "0",
            "metrics": {
                "net_cancel_total": text(cancel_sum),
                "cancel_percentile": text(rank_c),
                "executed_total": text(executed_sum),
                "executed_cap": text(executed_cap),
                "approaching": approaching,
                "spoofing_suspicion": spoofing,
            },
        }
    )
    swept: dict[str, dict[int, int]] = {"ask": {}, "bid": {}}
    for at, column in enumerate(region_columns):
        for cell in band_cells(column, lo, hi):
            if cell[5] > ZERO and cell[2] == ZERO and cell[1] in swept:
                swept[cell[1]].setdefault(cell[0], at)
    best_run = 0
    best_span = 0
    best_side = ""
    for side, hits in swept.items():
        bins = sorted(hits)
        run_start = 0
        for at in range(len(bins)):
            if at > 0 and bins[at] - bins[at - 1] != ROW_BIN:
                run_start = at
            length = at - run_start + 1
            if length >= 2:
                span = (
                    max(hits[b] for b in bins[run_start: at + 1])
                    - min(hits[b] for b in bins[run_start: at + 1])
                    + 1
                )
            else:
                span = 1
            if length > best_run and span <= SWEEP_MAX_BUCKETS:
                best_run = length
                best_span = span
                best_side = side
    met_s = best_run >= SWEEP_MIN_LEVELS
    degrees_s = [
        degree_at_least(Decimal(best_run), Decimal(SWEEP_MIN_LEVELS))
    ]
    out.append(
        {
            "kind": "sweep",
            "met": met_s,
            "confidence": confidence_of(degrees_s) if best_run else "0",
            "metrics": {
                "levels_broken": str(best_run),
                "bucket_span": str(best_span),
                "side": best_side,
                "min_levels": str(SWEEP_MIN_LEVELS),
                "max_buckets": str(SWEEP_MAX_BUCKETS),
            },
        }
    )
    if samples:
        low_depth = min(s[2] for s in samples)
        rank_v = percentile_rank(depth_population, low_depth)
        met_v = rank_v <= VACUUM_DEPTH_PERCENTILE
        degrees_v = [degree_at_most(rank_v, VACUUM_DEPTH_PERCENTILE)]
        vacuum_conf = confidence_of(degrees_v)
        depth_text = text(low_depth)
        rank_text = text(rank_v)
    else:
        met_v = False
        vacuum_conf = "0"
        depth_text = None
        rank_text = None
    out.append(
        {
            "kind": "liquidity_vacuum",
            "met": met_v,
            "confidence": vacuum_conf,
            "metrics": {
                "min_depth": depth_text,
                "depth_percentile": rank_text,
                "threshold_percentile": text(VACUUM_DEPTH_PERCENTILE),
            },
        }
    )
    return out


def load_rules(path: Path) -> list[dict[str, object]]:
    """读报警规则实例配置。"""
    loaded = json.loads(path.read_text(encoding="utf-8"))
    rows = loaded.get("rules") if isinstance(loaded, dict) else None
    return rows if isinstance(rows, list) else []


def expected_rule_ids(
    rules: list[dict[str, object]], kind: str, symbol: str, conf: Decimal
) -> list[str]:
    """应触发的规则集合。"""
    out = []
    for rule in rules:
        if not rule.get("enabled") or rule.get("kind") != kind:
            continue
        if rule.get("symbol") != symbol:
            continue
        overrides = rule.get("overrides")
        floor_text = "0"
        if isinstance(overrides, dict):
            floor_text = str(overrides.get("min_confidence", "0"))
        if conf >= Decimal(floor_text):
            out.append(str(rule.get("rule_id", "")))
    return out


def day_of_epoch(epoch_s: int) -> str:
    """秒时戳所属 UTC 日。"""
    return datetime.fromtimestamp(epoch_s, UTC).date().isoformat()


def main() -> int:
    """主流程：重建、恒等对照、逐行复核。"""
    parser = argparse.ArgumentParser(description="报警派生行独立复核")
    parser.add_argument("--data-root", default="data")
    parser.add_argument("--symbol", default="BTC")
    parser.add_argument("--out", default="-")
    args = parser.parse_args()
    root = Path(args.data_root)
    conn = sqlite3.connect(
        f"file:{(root / 'guvolu.sqlite3').as_posix()}?mode=ro", uri=True
    )
    features = [
        dict(
            zip(
                (
                    "feature_id", "kind", "venue_id", "symbol", "price_low",
                    "price_high", "from_ts", "to_ts", "metrics",
                    "config_hash", "created_at",
                ),
                row,
            )
        )
        for row in conn.execute(
            "SELECT feature_id, kind, venue_id, symbol, price_low, "
            "price_high, from_ts, to_ts, metrics, config_hash, created_at "
            "FROM book_feature WHERE symbol=? ORDER BY feature_id",
            (args.symbol,),
        )
    ]
    alerts = [
        dict(zip(("alert_id", "feature_id", "rule_id", "triggered_at",
                  "acked_at"), row))
        for row in conn.execute(
            "SELECT alert_id, feature_id, rule_id, triggered_at, acked_at "
            "FROM alert_event ORDER BY alert_id"
        )
    ]
    conn.close()
    rules = load_rules(Path("config") / "alert_rules.json")
    days_needed: set[str] = set()
    for row in features:
        from_s = int(datetime.fromisoformat(row["from_ts"]).timestamp())
        to_s = int(datetime.fromisoformat(row["to_ts"]).timestamp())
        row["from_s"] = from_s
        row["to_s"] = to_s
        days = {day_of_epoch(from_s), day_of_epoch(to_s - 1)}
        row["days"] = sorted(days)
        days_needed.update(days)
    day_columns: dict[str, list] = {}
    day_reports: dict[str, dict[str, object]] = {}
    for day in sorted(days_needed):
        raw = root / "raw" / day / "ws_public.jsonl"
        if not raw.exists():
            raw = raw.with_suffix(".jsonl.gz")
        meta_path = (
            root / "derived" / "heatmap_tiles" / "gmo" / args.symbol
            / "1s" / f"{day}.meta.json"
        )
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        horizon_cols = int(meta["columns"])
        day_start = int(
            datetime.fromisoformat(f"{day}T00:00:00+00:00").timestamp()
        )
        parsed = parse_day_raw(raw, args.symbol)
        columns = build_columns(
            parsed["frames"], parsed["prints"], day_start,
            day_start + horizon_cols,
        )
        tile_gz = meta_path.with_name(f"{day}.jsonl.gz")
        identity = compare_tiles(columns, tile_gz)
        identity["day"] = day
        identity["frames_parsed"] = len(parsed["frames"])
        identity["prints_parsed"] = len(parsed["prints"])
        identity["stragglers"] = parsed["stragglers"]
        identity["tile_built_at"] = meta.get("built_at")
        identity["tile_columns"] = horizon_cols
        day_columns[day] = columns
        day_reports[day] = identity
    groups: dict[tuple, list[dict]] = {}
    for row in features:
        key = (
            row["price_low"], row["price_high"], row["from_ts"],
            row["to_ts"], row["created_at"],
        )
        groups.setdefault(key, []).append(row)
    checks = []
    for key, rows in sorted(groups.items(), key=lambda kv: kv[1][0]["feature_id"]):
        low = Decimal(key[0])
        high = Decimal(key[1])
        first = rows[0]
        day = first["days"][0]
        columns = day_columns[day]
        day_start = columns[0]["e"]
        horizon_end = columns[-1]["e"] + 1
        lo_rel = max(0, first["from_s"] - day_start)
        hi_rel = max(lo_rel, min(first["to_s"], horizon_end) - day_start)
        region = columns[lo_rel:hi_rel]
        judged = judge_region(region, columns, low, high)
        judged_by_kind = {j["kind"]: j for j in judged}
        met_kinds = sorted(j["kind"] for j in judged if j["met"])
        stored_kinds = sorted(row["kind"] for row in rows)
        for row in rows:
            mine = judged_by_kind[row["kind"]]
            stored = json.loads(row["metrics"])
            stored_conf = stored.pop("confidence", None)
            diffs = []
            for name, value in stored.items():
                got = mine["metrics"].get(name)
                if isinstance(value, bool) or isinstance(got, bool):
                    same = bool(value) == bool(got)
                elif value is None or got is None:
                    same = value == got
                else:
                    try:
                        same = Decimal(str(value)) == Decimal(str(got))
                    except ArithmeticError:
                        same = str(value) == str(got)
                if not same:
                    diffs.append(
                        {"metric": name, "stored": value, "recomputed": got}
                    )
            conf_same = stored_conf == mine["confidence"]
            feature_alerts = [
                a for a in alerts if a["feature_id"] == row["feature_id"]
            ]
            expect = expected_rule_ids(
                rules, row["kind"], row["symbol"],
                Decimal(str(stored_conf)) if stored_conf else ZERO,
            )
            got_rules = sorted(a["rule_id"] for a in feature_alerts)
            trig_ok = all(
                a["triggered_at"] == row["created_at"]
                for a in feature_alerts
            )
            band_round = (
                int(low) % 1000 == 0 and int(high) % 1000 == 0
            )
            window_round = (
                first["from_s"] % 60 == 0 and first["to_s"] % 60 == 0
            )
            checks.append(
                {
                    "feature_id": row["feature_id"],
                    "kind": row["kind"],
                    "day": day,
                    "band": [key[0], key[1]],
                    "window": [row["from_ts"], row["to_ts"]],
                    "met_recomputed": mine["met"],
                    "confidence_stored": stored_conf,
                    "confidence_recomputed": mine["confidence"],
                    "confidence_match": conf_same,
                    "metric_diffs": diffs,
                    "metrics_recomputed": mine["metrics"],
                    "config_hash_match": row["config_hash"]
                    == config_hash_expected(),
                    "group_stored_kinds": stored_kinds,
                    "group_met_kinds_recomputed": met_kinds,
                    "alerts_expected_rules": sorted(expect),
                    "alerts_actual_rules": got_rules,
                    "alerts_rule_match": sorted(expect) == got_rules,
                    "alert_trigger_time_match": trig_ok,
                    "band_round_thousand": band_round,
                    "window_round_minute": window_round,
                    "created_at": row["created_at"],
                }
            )
    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "symbol": args.symbol,
        "config_hash_expected": config_hash_expected(),
        "day_identity": [day_reports[d] for d in sorted(day_reports)],
        "feature_checks": checks,
        "feature_rows": len(features),
        "alert_rows": len(alerts),
    }
    body = json.dumps(report, ensure_ascii=False, indent=1, default=str)
    if args.out == "-":
        sys.stdout.write(body + "\n")
    else:
        Path(args.out).write_text(body + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
