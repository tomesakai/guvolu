"""bitbank 公开 API 只读探测脚本（A-04、C-14）。

仅调用公开端点，无密钥、无写请求。响应按 storage-design 第 4 节
行格式落盘 data/raw/<UTC 日期>/{bitbank,bitflyer,gmo}/<组>.jsonl。
超过阈值的载荷以摘要形式落盘并标记 payload_form=digest，
摘要保留行数、首尾元素与字节数，判定所需统计在内存中完成。

探测项：逐笔字段语义、时间戳单位、日界归属、序号连续性、
side 语义 tick 检验、上市起点二分探测、K 线年界、
bitFlyer executions 字段与 31 天边界复核、GMO 品种规则。
"""
from __future__ import annotations

import json
import secrets
import subprocess
import sys
import time
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
RAW_ROOT = REPO_ROOT / "data" / "raw"

BITBANK_PUBLIC_BASE = "https://public.bitbank.cc"
BITBANK_REST_BASE = "https://api.bitbank.cc/v1"
BITFLYER_BASE = "https://api.bitflyer.com"
GMO_PUBLIC_BASE = "https://api.coin.z.com/public"

SCHEMA_VERSION = 1
TIMEOUT_SECONDS = 30.0
# 自我限速三分之一秒
REQUEST_INTERVAL = 1.0 / 3.0
# 载荷全文落盘上限字节
DIGEST_LIMIT_BYTES = 262144
# 频率超限退避秒
BACKOFF_SECONDS = (2.0, 4.0, 8.0, 16.0)

PAIRS = ("btc_jpy", "eth_jpy", "xrp_jpy")
# 二分下界，确认无数据
SEARCH_FLOOR = "20161001"


def utc_now_iso() -> str:
    """当前 UTC 时刻 ISO 文本。"""
    return datetime.now(UTC).isoformat()


def ms_to_iso(ms: int) -> str:
    """毫秒时间戳转 UTC ISO 文本。"""
    return datetime.fromtimestamp(ms / 1000, tz=UTC).isoformat(
        timespec="milliseconds"
    )


def day_str(moment: datetime) -> str:
    return moment.strftime("%Y%m%d")


def parse_day(day: str) -> datetime:
    return datetime.strptime(day, "%Y%m%d").replace(tzinfo=UTC)


class RawWriter:
    """raw 层落盘器，按来源与组分文件（D-02）。"""

    def __init__(self, root: Path, run_id: str) -> None:
        self._root = root
        self._run_id = run_id
        self._counts: dict[str, int] = {}

    @property
    def counts(self) -> dict[str, int]:
        return dict(self._counts)

    def write(self, venue: str, group: str, record: dict[str, Any]) -> None:
        directory = self._root / venue
        directory.mkdir(parents=True, exist_ok=True)
        line = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
        with (directory / f"{group}.jsonl").open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")
        key = f"{venue}/{group}"
        self._counts[key] = self._counts.get(key, 0) + 1


def digest_payload(payload: object, size: int) -> dict[str, Any]:
    """构造载荷摘要：行数、首尾元素、字节数。"""
    rows: list[object] | None = None
    if isinstance(payload, Mapping):
        data = payload.get("data")
        if isinstance(data, Mapping):
            transactions = data.get("transactions")
            if isinstance(transactions, list):
                rows = transactions
    if isinstance(payload, list):
        rows = payload
    out: dict[str, Any] = {"payload_form": "digest", "bytes": size}
    if rows is not None:
        out["rows"] = len(rows)
        if rows:
            out["first"] = rows[0]
            out["last"] = rows[-1]
    return out


class Prober:
    """只读探测器，限速与退避内置。"""

    def __init__(self, writer: RawWriter, run_id: str) -> None:
        self._writer = writer
        self._run_id = run_id
        self._session = requests.Session()
        self._last_request = 0.0
        self.request_total = 0
        self.http_429_total = 0
        self.http_5xx_total = 0

    def _throttle(self) -> None:
        wait = self._last_request + REQUEST_INTERVAL - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        self._last_request = time.monotonic()

    def get(
        self,
        base: str,
        path: str,
        *,
        venue: str,
        group: str,
        params: dict[str, str | int] | None = None,
        force_digest: bool = False,
    ) -> tuple[int | None, object | None, int]:
        """GET 一次并落盘，返回状态、载荷、字节数。"""
        for attempt in range(len(BACKOFF_SECONDS) + 1):
            self._throttle()
            started = time.monotonic()
            http_status: int | None = None
            payload: object | None = None
            error: str | None = None
            size = 0
            try:
                response = self._session.get(
                    base + path,
                    params=params,
                    timeout=TIMEOUT_SECONDS,
                )
                http_status = response.status_code
                size = len(response.content)
                try:
                    payload = response.json()
                except ValueError:
                    error = "非 JSON 响应"
            except requests.RequestException as exc:
                error = f"{type(exc).__name__}: {exc}"
            latency = (time.monotonic() - started) * 1000
            self.request_total += 1
            if http_status == 429:
                self.http_429_total += 1
            if http_status is not None and http_status >= 500:
                self.http_5xx_total += 1
            stored: object | None = payload
            if payload is not None and (force_digest or size > DIGEST_LIMIT_BYTES):
                stored = digest_payload(payload, size)
            self._writer.write(
                venue,
                group,
                {
                    "schema_version": SCHEMA_VERSION,
                    "run_id": self._run_id,
                    "source": "rest_public",
                    "method": "GET",
                    "path": path,
                    "params": params,
                    "ingest_time": utc_now_iso(),
                    "latency_ms": round(latency, 1),
                    "http_status": http_status,
                    "payload": stored,
                    "network_error": error,
                },
            )
            retryable = http_status == 429 or (
                http_status is not None and http_status >= 500
            ) or (http_status is None)
            if not retryable or attempt >= len(BACKOFF_SECONDS):
                return http_status, payload, size
            time.sleep(BACKOFF_SECONDS[attempt])
        return None, None, 0


def transactions_rows(payload: object) -> list[dict[str, Any]] | None:
    """从响应载荷取逐笔数组。"""
    if not isinstance(payload, Mapping):
        return None
    data = payload.get("data")
    if not isinstance(data, Mapping):
        return None
    rows = data.get("transactions")
    if not isinstance(rows, list):
        return None
    return [row for row in rows if isinstance(row, dict)]


def fetch_day(
    prober: Prober, pair: str, day: str, *, force_digest: bool
) -> tuple[int | None, list[dict[str, Any]] | None, int]:
    """拉取单日全量逐笔。"""
    status, payload, size = prober.get(
        BITBANK_PUBLIC_BASE,
        f"/{pair}/transactions/{day}",
        venue="bitbank",
        group="transactions_day",
        force_digest=force_digest,
    )
    return status, transactions_rows(payload), size


def tick_test(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """side 语义 tick 检验（打印口径快照第 4 节法）。"""
    ordered = sorted(
        rows, key=lambda r: (int(r["executed_at"]), int(r["transaction_id"]))
    )
    total = 0
    buys = 0
    upticks = 0
    upticks_buy = 0
    downticks = 0
    downticks_sell = 0
    prev: Decimal | None = None
    for row in ordered:
        price = Decimal(str(row["price"]))
        side = str(row["side"])
        total += 1
        if side == "buy":
            buys += 1
        if prev is not None and price != prev:
            if price > prev:
                upticks += 1
                if side == "buy":
                    upticks_buy += 1
            else:
                downticks += 1
                if side == "sell":
                    downticks_sell += 1
        prev = price
    def rate(part: int, whole: int) -> float | None:
        return round(part / whole, 4) if whole else None
    return {
        "rows": total,
        "buy_base_rate": rate(buys, total),
        "upticks": upticks,
        "uptick_buy_rate": rate(upticks_buy, upticks),
        "downticks": downticks,
        "downtick_sell_rate": rate(downticks_sell, downticks),
    }


def id_stats(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """序号连续性统计。"""
    ids = sorted(int(row["transaction_id"]) for row in rows)
    diffs = [b - a for a, b in zip(ids, ids[1:], strict=False)]
    gaps = [d for d in diffs if d != 1]
    return {
        "count": len(ids),
        "id_min": ids[0] if ids else None,
        "id_max": ids[-1] if ids else None,
        "contiguous_pairs": len(diffs) - len(gaps),
        "gap_pairs": len(gaps),
        "max_diff": max(diffs) if diffs else None,
        "duplicate_pairs": sum(1 for d in diffs if d == 0),
    }


def time_stats(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """时间范围与载荷内排序观测。"""
    stamps = [int(row["executed_at"]) for row in rows]
    ascending = all(a <= b for a, b in zip(stamps, stamps[1:], strict=False))
    descending = all(a >= b for a, b in zip(stamps, stamps[1:], strict=False))
    return {
        "ts_min": min(stamps) if stamps else None,
        "ts_min_iso": ms_to_iso(min(stamps)) if stamps else None,
        "ts_max": max(stamps) if stamps else None,
        "ts_max_iso": ms_to_iso(max(stamps)) if stamps else None,
        "payload_order": (
            "ascending" if ascending else "descending" if descending else "mixed"
        ),
    }


def find_first_day(prober: Prober, pair: str, hi_day: str) -> dict[str, Any]:
    """二分探测最早可得日，末段逐日核验。"""
    lo = parse_day(SEARCH_FLOOR)
    hi = parse_day(hi_day)
    checks = 0
    status, _, _ = fetch_day(prober, pair, day_str(lo), force_digest=True)
    checks += 1
    floor_missing = status == 404
    while (hi - lo).days > 1:
        mid = lo + timedelta(days=(hi - lo).days // 2)
        status, _, _ = fetch_day(prober, pair, day_str(mid), force_digest=True)
        checks += 1
        if status == 200:
            hi = mid
        else:
            lo = mid
    verify: dict[str, int | None] = {}
    for back in range(1, 8):
        day = day_str(hi - timedelta(days=back))
        status, _, _ = fetch_day(prober, pair, day, force_digest=True)
        checks += 1
        verify[day] = status
    first_status, first_rows, _ = fetch_day(
        prober, pair, day_str(hi), force_digest=False
    )
    return {
        "pair": pair,
        "floor_day": SEARCH_FLOOR,
        "floor_missing": floor_missing,
        "first_day": day_str(hi),
        "first_day_status": first_status,
        "first_day_rows": len(first_rows) if first_rows is not None else None,
        "first_day_head": first_rows[0] if first_rows else None,
        "prior_seven_status": verify,
        "requests": checks + 1,
    }


def probe_bitbank(prober: Prober) -> dict[str, Any]:
    """bitbank 全部探测项。"""
    summary: dict[str, Any] = {}
    now = datetime.now(UTC)
    recent = day_str(now - timedelta(days=1))
    prior = day_str(now - timedelta(days=2))

    print("拉取 spot/pairs 品种规则")
    _, pairs_payload, _ = prober.get(
        BITBANK_REST_BASE, "/spot/pairs", venue="bitbank", group="spot_pairs"
    )
    rules: dict[str, Any] = {}
    if isinstance(pairs_payload, Mapping):
        data = pairs_payload.get("data")
        if isinstance(data, Mapping):
            entries = data.get("pairs")
            if isinstance(entries, list):
                for entry in entries:
                    if isinstance(entry, Mapping) and entry.get("name") in PAIRS:
                        rules[str(entry["name"])] = dict(entry)
    summary["pair_rules"] = rules

    print("拉取无日期逐笔与近二日全量")
    status, latest_rows, size = prober.get(
        BITBANK_PUBLIC_BASE,
        "/btc_jpy/transactions",
        venue="bitbank",
        group="transactions_latest",
    )
    rows = transactions_rows(latest_rows)
    if rows:
        summary["latest_no_date"] = {
            "rows": len(rows),
            "field_keys": sorted(rows[0].keys()),
            "order": time_stats(rows)["payload_order"],
        }
    day_reports: dict[str, Any] = {}
    day_rows: dict[str, list[dict[str, Any]]] = {}
    for pair in PAIRS:
        for day in (prior, recent):
            force = not (pair == "btc_jpy")
            status, rows, size = fetch_day(prober, pair, day, force_digest=force)
            if rows is None:
                day_reports[f"{pair}:{day}"] = {"http_status": status}
                continue
            day_rows[f"{pair}:{day}"] = rows
            day_reports[f"{pair}:{day}"] = {
                "http_status": status,
                "bytes": size,
                "rows": len(rows),
                "time": time_stats(rows),
                "ids": id_stats(rows),
                "tick_test": tick_test(rows),
            }
    summary["days"] = day_reports

    cross: dict[str, Any] = {}
    for pair in PAIRS:
        a = day_rows.get(f"{pair}:{prior}")
        b = day_rows.get(f"{pair}:{recent}")
        if a and b:
            last_id = max(int(r["transaction_id"]) for r in a)
            first_id = min(int(r["transaction_id"]) for r in b)
            cross[pair] = {
                "prior_max_id": last_id,
                "recent_min_id": first_id,
                "cross_day_gap": first_id - last_id,
            }
    summary["cross_day_ids"] = cross

    print("探测缺失日与未来日行为")
    behaviors: dict[str, Any] = {}
    future_day = day_str(now + timedelta(days=150))
    for label, day in (("ancient", "20170101"), ("future", future_day)):
        status, payload, _ = prober.get(
            BITBANK_PUBLIC_BASE,
            f"/btc_jpy/transactions/{day}",
            venue="bitbank",
            group="transactions_day",
        )
        entry: dict[str, Any] = {"day": day, "http_status": status}
        if isinstance(payload, Mapping):
            entry["success"] = payload.get("success")
            data = payload.get("data")
            entry["error_body"] = data if isinstance(data, Mapping) else None
        behaviors[label] = entry
    summary["missing_behavior"] = behaviors

    print("K 线年界探测")
    kline: dict[str, Any] = {}
    for year in ("2016", "2017"):
        status, payload, size = prober.get(
            BITBANK_PUBLIC_BASE,
            f"/btc_jpy/candlestick/1day/{year}",
            venue="bitbank",
            group="candlestick_year",
        )
        entry: dict[str, Any] = {"http_status": status, "bytes": size}
        if isinstance(payload, Mapping):
            entry["success"] = payload.get("success")
            data = payload.get("data")
            if isinstance(data, Mapping):
                sticks = data.get("candlestick")
                if isinstance(sticks, list) and sticks:
                    first = sticks[0]
                    if isinstance(first, Mapping):
                        ohlcv = first.get("ohlcv")
                        if isinstance(ohlcv, list) and ohlcv:
                            entry["first_open_ms"] = ohlcv[0][5]
                            entry["candles"] = len(ohlcv)
            else:
                entry["data"] = data
        kline[year] = entry
    summary["kline_year_boundary"] = kline

    print("上市起点二分探测（三对）")
    listing: dict[str, Any] = {}
    for pair in PAIRS:
        listing[pair] = find_first_day(prober, pair, recent)
        print(f"  {pair} 起点 {listing[pair]['first_day']}")
    summary["listing_start"] = listing
    return summary


def probe_bitflyer(prober: Prober) -> dict[str, Any]:
    """bitFlyer executions 字段与边界复核。"""
    summary: dict[str, Any] = {}
    fields: dict[str, Any] = {}
    for product in ("BTC_JPY", "FX_BTC_JPY"):
        _, payload, _ = prober.get(
            BITFLYER_BASE,
            "/v1/executions",
            params={"product_code": product, "count": 5},
            venue="bitflyer",
            group="executions_fields",
        )
        if isinstance(payload, list) and payload:
            first = payload[0]
            if isinstance(first, Mapping):
                fields[product] = {
                    "field_keys": sorted(str(k) for k in first),
                    "sample": dict(first),
                    "ids_descending": all(
                        int(a["id"]) > int(b["id"])
                        for a, b in zip(payload, payload[1:], strict=False)
                        if isinstance(a, Mapping) and isinstance(b, Mapping)
                    ),
                }
    summary["fields"] = fields
    status, payload, _ = prober.get(
        BITFLYER_BASE,
        "/v1/executions",
        params={"product_code": "BTC_JPY", "count": 5, "before": 1000},
        venue="bitflyer",
        group="executions_boundary",
    )
    summary["boundary_before_1000"] = {
        "http_status": status,
        "payload": payload if isinstance(payload, Mapping) else None,
    }
    return summary


def probe_gmo(prober: Prober) -> dict[str, Any]:
    """GMO 品种规则拉取，供映射登记。"""
    _, payload, _ = prober.get(
        GMO_PUBLIC_BASE, "/v1/symbols", venue="gmo", group="symbols"
    )
    rules: dict[str, Any] = {}
    if isinstance(payload, Mapping):
        data = payload.get("data")
        if isinstance(data, list):
            for row in data:
                if isinstance(row, Mapping) and row.get("symbol") in (
                    "BTC",
                    "ETH",
                    "XRP",
                ):
                    rules[str(row["symbol"])] = dict(row)
    return {"spot_rules": rules}


def code_version() -> str | None:
    """当前 git 提交散列，无提交时为空。"""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return None
    text = result.stdout.strip()
    return text if result.returncode == 0 and text else None


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    run_id = "runbb" + secrets.token_hex(6)
    started = utc_now_iso()
    day_dir = RAW_ROOT / datetime.now(UTC).strftime("%Y-%m-%d")
    writer = RawWriter(day_dir, run_id)
    prober = Prober(writer, run_id)
    print(f"run_id {run_id} 输出目录 {day_dir}")

    bitbank_summary = probe_bitbank(prober)
    bitflyer_summary = probe_bitflyer(prober)
    gmo_summary = probe_gmo(prober)

    finished = utc_now_iso()
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "code_version": code_version(),
        "started_at": started,
        "finished_at": finished,
        "request_total": prober.request_total,
        "http_429_total": prober.http_429_total,
        "http_5xx_total": prober.http_5xx_total,
        "record_counts": writer.counts,
        "bitbank": bitbank_summary,
        "bitflyer": bitflyer_summary,
        "gmo": gmo_summary,
    }
    manifest_path = day_dir / "bitbank" / f"manifest-{run_id}.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    print(f"完成 请求 {prober.request_total} 429 {prober.http_429_total}")
    print(f"清单 {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
