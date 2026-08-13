"""raw 快照分析脚本：统计量级，为存储选型提供依据。

只读取 data/raw/<UTC 日期>/ 下的 JSONL 与清单，不访问网络。
输出为人读摘要，配合拉取脚本构成可复现证据链（D-09 取向）。
"""
from __future__ import annotations

import json
import statistics
import sys
from collections.abc import Iterator, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
RAW_ROOT = REPO_ROOT / "data" / "raw"


def read_lines(path: Path) -> Iterator[dict[str, Any]]:
    """逐行读取 JSONL。"""
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                yield json.loads(line)


def rows_of(data: object) -> list[Mapping[str, object]]:
    """取列表载荷，兼容 list 包络。"""
    if isinstance(data, list):
        return [row for row in data if isinstance(row, Mapping)]
    if isinstance(data, Mapping):
        inner = data.get("list")
        if isinstance(inner, list):
            return [row for row in inner if isinstance(row, Mapping)]
    return []


def payload_data(record: Mapping[str, Any]) -> object:
    """从记录取信封 data，失败返回 None。"""
    payload = record.get("payload")
    if isinstance(payload, Mapping) and payload.get("status") == 0:
        return payload.get("data")
    return None


def error_code(record: Mapping[str, Any]) -> str | None:
    """从失败记录取首个错误码。"""
    payload = record.get("payload")
    if not isinstance(payload, Mapping) or payload.get("status") == 0:
        return None
    messages = payload.get("messages")
    if isinstance(messages, list) and messages:
        first = messages[0]
        if isinstance(first, Mapping):
            return str(first.get("message_code"))
    return "UNKNOWN"


def fmt_bytes(size: float) -> str:
    """字节数转人读文本。"""
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024 or unit == "GB":
            return f"{size:.1f}{unit}"
        size /= 1024
    return f"{size:.1f}GB"


def parse_iso(text: str) -> datetime:
    """解析 ISO 时刻。"""
    return datetime.fromisoformat(text)


def section(title: str) -> None:
    print(f"\n== {title} ==")


def analyze_symbols(day: Path) -> list[str]:
    section("品种与取引ルール")
    symbols: list[str] = []
    for record in read_lines(day / "symbols.jsonl"):
        rows = rows_of(payload_data(record))
        spot = [r for r in rows if "_JPY" not in str(r.get("symbol"))]
        lev = [r for r in rows if "_JPY" in str(r.get("symbol"))]
        print(f"品种总数 {len(rows)} 现物 {len(spot)} 杠杆 {len(lev)}")
        symbols = [str(r.get("symbol")) for r in rows]
        for row in rows:
            print(
                f"  {row.get('symbol')}: min={row.get('minOrderSize')}"
                f" step={row.get('sizeStep')} tick={row.get('tickSize')}"
                f" taker={row.get('takerFee')} maker={row.get('makerFee')}"
            )
    return symbols


def analyze_ticker(day: Path) -> None:
    section("最新レート概览")
    for record in read_lines(day / "ticker.jsonl"):
        rows = rows_of(payload_data(record))
        print(f"ticker 品种数 {len(rows)}")
        for row in rows[:5]:
            print(
                f"  {row.get('symbol')}: last={row.get('last')}"
                f" volume={row.get('volume')}"
            )
        if len(rows) > 5:
            print(f"  其余 {len(rows) - 5} 品种略")


def analyze_orderbooks(day: Path) -> None:
    section("盘口快照深度")
    depths: list[tuple[str, int, int, int]] = []
    for record in read_lines(day / "orderbooks.jsonl"):
        data = payload_data(record)
        if not isinstance(data, Mapping):
            continue
        asks = data.get("asks")
        bids = data.get("bids")
        n_a = len(asks) if isinstance(asks, list) else 0
        n_b = len(bids) if isinstance(bids, list) else 0
        size = len(json.dumps(data, ensure_ascii=False).encode())
        depths.append((str(data.get("symbol")), n_a, n_b, size))
    for symbol, n_a, n_b, size in depths:
        print(f"  {symbol}: 卖 {n_a} 档 买 {n_b} 档 {fmt_bytes(size)}")
    if depths:
        avg = statistics.mean(d[3] for d in depths)
        print(f"快照均值 {fmt_bytes(avg)} 品种数 {len(depths)}")


def analyze_trades(day: Path) -> None:
    section("逐笔成交速率估计")
    per_symbol: dict[str, list[Mapping[str, object]]] = {}
    for record in read_lines(day / "trades.jsonl"):
        params = record.get("params") or {}
        symbol = str(params.get("symbol"))
        rows = rows_of(payload_data(record))
        per_symbol.setdefault(symbol, []).extend(rows)
    for symbol, rows in sorted(per_symbol.items()):
        if not rows:
            print(f"  {symbol}: 无成交记录")
            continue
        times = sorted(
            parse_iso(str(r.get("timestamp"))) for r in rows if r.get("timestamp")
        )
        span = (times[-1] - times[0]).total_seconds() if len(times) > 1 else 0.0
        rate = len(rows) / span * 60 if span > 0 else 0.0
        print(
            f"  {symbol}: 样本 {len(rows)} 笔 跨度 {span:.0f} 秒"
            f" 约 {rate:.1f} 笔/分"
        )


def analyze_klines(day: Path) -> None:
    section("KLine 历史深度与量级")
    agg: dict[tuple[str, str], dict[str, Any]] = {}
    probe_lines: list[str] = []
    for record in read_lines(day / "klines.jsonl"):
        params = record.get("params") or {}
        symbol = str(params.get("symbol"))
        interval = str(params.get("interval"))
        date = str(params.get("date"))
        data = payload_data(record)
        code = error_code(record)
        if code is not None:
            probe_lines.append(f"  探测 {symbol} {interval} date={date}: {code}")
            continue
        rows = rows_of(data)
        if not rows:
            continue
        entry = agg.setdefault(
            (symbol, interval),
            {"rows": 0, "bytes": 0, "first": None, "last": None, "dates": 0},
        )
        entry["rows"] += len(rows)
        entry["dates"] += 1
        entry["bytes"] += len(json.dumps(rows, ensure_ascii=False).encode())
        first_ms = int(str(rows[0].get("openTime")))
        last_ms = int(str(rows[-1].get("openTime")))
        first = datetime.fromtimestamp(first_ms / 1000, tz=UTC)
        last = datetime.fromtimestamp(last_ms / 1000, tz=UTC)
        if entry["first"] is None or first < entry["first"]:
            entry["first"] = first
        if entry["last"] is None or last > entry["last"]:
            entry["last"] = last
    by_interval: dict[str, list[tuple[str, dict[str, Any]]]] = {}
    for (symbol, interval), entry in agg.items():
        by_interval.setdefault(interval, []).append((symbol, entry))
    for interval in sorted(by_interval):
        items = by_interval[interval]
        total_rows = sum(e["rows"] for _, e in items)
        total_bytes = sum(e["bytes"] for _, e in items)
        print(
            f"  {interval}: 品种 {len(items)} 合计 {total_rows} 根"
            f" 载荷 {fmt_bytes(total_bytes)}"
            f" 均 {total_bytes / max(total_rows, 1):.0f}B/根"
        )
        if interval == "1day":
            for symbol, entry in sorted(items):
                first = entry["first"].date() if entry["first"] else "?"
                last = entry["last"].date() if entry["last"] else "?"
                print(
                    f"    {symbol}: {entry['rows']} 根 {first} 至 {last}"
                )
    if probe_lines:
        print("  参数形态探测:")
        for line in probe_lines:
            print(line)


def analyze_private(day: Path) -> None:
    section("账户与私有数据现状")
    for record in read_lines(day / "account_assets.jsonl"):
        rows = rows_of(payload_data(record))
        for row in rows:
            print(
                f"  資産 {row.get('symbol')}: amount={row.get('amount')}"
                f" available={row.get('available')}"
            )
    for record in read_lines(day / "account_margin.jsonl"):
        data = payload_data(record)
        if isinstance(data, Mapping):
            print(
                f"  余力: available={data.get('availableAmount')}"
                f" 现物可用={data.get('availableAmountForSpot')}"
            )
    for record in read_lines(day / "account_tradingVolume.jsonl"):
        data = payload_data(record)
        if isinstance(data, Mapping):
            print(
                f"  取引高: jpyVolume={data.get('jpyVolume')}"
                f" tier={data.get('tierLevel')}"
            )

    def count_group(name: str, label: str) -> None:
        calls = 0
        rows_total = 0
        codes: dict[str, int] = {}
        for record in read_lines(day / f"{name}.jsonl"):
            calls += 1
            code = error_code(record)
            if code is not None:
                codes[code] = codes.get(code, 0) + 1
                continue
            rows_total += len(rows_of(payload_data(record)))
        code_text = f" 错误 {codes}" if codes else ""
        print(f"  {label}: 调用 {calls} 记录 {rows_total}{code_text}")

    count_group("activeOrders", "挂单")
    count_group("latestExecutions", "最新成交")
    count_group("openPositions", "建玉一覧")
    count_group("positionSummary", "持仓汇总")
    count_group("orders", "委托按号查询")
    count_group("executions", "成交按号查询")
    count_group("account_fiatDeposit_history", "日本円入金履历")
    count_group("account_fiatWithdrawal_history", "日本円出金履历")
    count_group("account_deposit_history", "暗号資産预入履历")
    count_group("account_withdrawal_history", "暗号資産送付履历")

    for name, label in (
        ("account_fiatDeposit_history", "入金"),
        ("account_fiatWithdrawal_history", "出金"),
    ):
        for record in read_lines(day / f"{name}.jsonl"):
            rows = rows_of(payload_data(record))
            for row in rows:
                print(
                    f"  {label}记录: {row.get('timestamp')}"
                    f" amount={row.get('amount')} status={row.get('status')}"
                )


def analyze_latency(day: Path) -> None:
    section("请求延迟统计")
    by_source: dict[str, list[float]] = {}
    for path in sorted(day.glob("*.jsonl")):
        if path.name == "ws_public.jsonl":
            continue
        for record in read_lines(path):
            latency = record.get("latency_ms")
            source = str(record.get("source"))
            if isinstance(latency, (int, float)):
                by_source.setdefault(source, []).append(float(latency))
    for source, values in sorted(by_source.items()):
        values.sort()
        mid = statistics.median(values)
        p95 = values[int(len(values) * 0.95) - 1] if len(values) > 1 else values[0]
        print(
            f"  {source}: n={len(values)} 中位 {mid:.0f}ms"
            f" p95 {p95:.0f}ms 最大 {values[-1]:.0f}ms"
        )


def analyze_ws(day: Path) -> None:
    section("公开 WS 采样速率")
    manifests = sorted(day.glob("manifest-*.json"))
    for manifest_path in manifests:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        sample = manifest.get("ws_sample")
        if not isinstance(sample, Mapping):
            continue
        seconds = float(sample.get("sample_seconds", 0) or 0)
        print(f"  采样 {seconds:.0f} 秒 总帧 {sample.get('frames')}")
        by_channel = sample.get("by_channel")
        if not isinstance(by_channel, Mapping):
            continue
        for key in sorted(by_channel):
            entry = by_channel[key]
            if not isinstance(entry, Mapping):
                continue
            frames = float(entry.get("frames", 0) or 0)
            size = float(entry.get("bytes", 0) or 0)
            rate = frames / seconds if seconds else 0.0
            bps = size / seconds if seconds else 0.0
            day_bytes = bps * 86400
            print(
                f"  {key}: {frames:.0f} 帧 {rate:.2f} 帧/秒"
                f" {fmt_bytes(bps)}/秒 折合 {fmt_bytes(day_bytes)}/日"
            )


def analyze_footprint(day: Path) -> None:
    section("本次落盘体积")
    total = 0
    for path in sorted(day.iterdir()):
        size = path.stat().st_size
        total += size
        print(f"  {path.name}: {fmt_bytes(size)}")
    print(f"合计 {fmt_bytes(total)}")


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    day_name = sys.argv[1] if len(sys.argv) > 1 else datetime.now(UTC).strftime(
        "%Y-%m-%d"
    )
    day = RAW_ROOT / day_name
    if not day.exists():
        print(f"目录不存在 {day}")
        return 1
    print(f"分析 {day}")
    analyze_symbols(day)
    analyze_ticker(day)
    analyze_orderbooks(day)
    analyze_trades(day)
    analyze_klines(day)
    analyze_private(day)
    analyze_latency(day)
    analyze_ws(day)
    analyze_footprint(day)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
