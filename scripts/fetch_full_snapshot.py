"""GMO API 全范围只读数据拉取脚本。

仅调用公开端点与 READ_ONLY 私有读取端点，绝不触碰写端点（T-02）。
进程装载配置后立即剥离 TRADE 密钥，全程不持有（T-13 取向）。
响应连同包络原样落盘 data/raw/<UTC 日期>/<端点>.jsonl，
每行含 schema_version、run_id、ingest_time、latency_ms（D-02、D-03、D-09）。
公开 WS 行情另做定时采样，用于评估消息速率与字节量（TBD-03 证据）。
本脚本属一次性探测脚本，落盘布局为 TBD-02 的提案试运行。
"""
from __future__ import annotations

import asyncio
import dataclasses
import json
import subprocess
import sys
import time
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import requests
import websockets

from guvolu.api.signing import sign_request
from guvolu.api.transport import PRIVATE_BASE_URL, PUBLIC_BASE_URL, RateLimiter
from guvolu.domain.config import Config, load_config
from guvolu.domain.ids import new_run_id

REPO_ROOT = Path(__file__).resolve().parents[1]
RAW_ROOT = REPO_ROOT / "data" / "raw"

PUBLIC_WS_URL = "wss://api.coin.z.com/ws/public/v1"

SCHEMA_VERSION = 1
TIMEOUT_SECONDS = 15.0
RETRY_MAX = 3
BACKOFF_BASE_SECONDS = 0.5

# 短周期日参数，长周期年参数
SHORT_INTERVALS = ("1min", "5min", "10min", "15min", "30min", "1hour")
LONG_INTERVALS = ("4hour", "8hour", "12hour", "1day", "1week", "1month")

# 年份探测范围
PROBE_YEARS = tuple(str(y) for y in range(2017, 2027))
# 分钟级采样品种
INTRADAY_SYMBOLS = ("BTC", "ETH", "XRP", "BTC_JPY")
# 分钟级采样交易日
INTRADAY_DATES = ("20260804", "20260805", "20260806")
INTRADAY_INTERVALS = ("1min", "5min", "1hour")

# 履历扫描回看天数
HISTORY_SCAN_DAYS = 7
HISTORY_WINDOW_MINUTES = 30

# WS 采样时长与品种
WS_SAMPLE_SECONDS = 60.0
WS_SYMBOLS = ("BTC", "BTC_JPY")
WS_CHANNELS = ("ticker", "trades", "orderbooks")
# 订阅命令间隔（R-04）
WS_COMMAND_INTERVAL = 1.1

Params = dict[str, str | int]


def utc_now_iso() -> str:
    """当前 UTC 时刻 ISO 文本。"""
    return datetime.now(UTC).isoformat()


def history_timestamp(value: datetime) -> str:
    """履历端点时间格式，毫秒三位（C-12）。"""
    moment = value.astimezone(UTC)
    millis = moment.microsecond // 1000
    return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{millis:03d}Z"


class RawWriter:
    """raw 层落盘器，按端点分文件，只追加（D-02）。"""

    def __init__(self, root: Path, run_id: str) -> None:
        self._root = root
        self._run_id = run_id
        self._counts: dict[str, int] = {}
        self._root.mkdir(parents=True, exist_ok=True)

    @property
    def counts(self) -> dict[str, int]:
        return dict(self._counts)

    def write(self, group: str, record: dict[str, Any]) -> None:
        line = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
        path = self._root / f"{group}.jsonl"
        with path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")
        self._counts[group] = self._counts.get(group, 0) + 1


def group_for_path(path: str) -> str:
    """端点路径转文件组名。"""
    return path.removeprefix("/v1/").replace("/", "_")


class Fetcher:
    """只读拉取器：公开与 READ_ONLY 私有 GET。"""

    def __init__(
        self,
        config: Config,
        writer: RawWriter,
        run_id: str,
    ) -> None:
        api_key, api_secret = config.require_read_credentials()
        self._api_key = api_key
        self._api_secret = api_secret
        self._writer = writer
        self._run_id = run_id
        self._session = requests.Session()
        self._public_limiter = RateLimiter(config.public_rps)
        self._private_limiter = RateLimiter(config.private_rps)
        self.request_total = 0
        self.error_total = 0

    def _headers(self, path: str) -> dict[str, str]:
        timestamp_ms = str(int(time.time() * 1000))
        signature = sign_request(
            self._api_secret, timestamp_ms, "GET", path, ""
        )
        return {
            "API-KEY": self._api_key,
            "API-TIMESTAMP": timestamp_ms,
            "API-SIGN": signature,
        }

    def _get_once(
        self, base: str, path: str, params: Params | None, private: bool
    ) -> tuple[int | None, object | None, str | None, float]:
        started = time.monotonic()
        try:
            response = self._session.get(
                base + path,
                params=params or None,
                headers=self._headers(path) if private else None,
                timeout=TIMEOUT_SECONDS,
            )
        except requests.RequestException as exc:
            return None, None, f"{type(exc).__name__}: {exc}", (
                time.monotonic() - started
            )
        latency = time.monotonic() - started
        try:
            payload = response.json()
        except ValueError:
            return response.status_code, None, "非 JSON 响应", latency
        return response.status_code, payload, None, latency

    def get(self, path: str, params: Params | None = None, *,
            private: bool = False, group: str | None = None) -> object | None:
        """GET 一次并原样落盘，返回 data 或 None。"""
        base = PRIVATE_BASE_URL if private else PUBLIC_BASE_URL
        limiter = self._private_limiter if private else self._public_limiter
        http_status: int | None = None
        payload: object | None = None
        error: str | None = None
        latency = 0.0
        for attempt in range(RETRY_MAX):
            limiter.acquire()
            http_status, payload, error, latency = self._get_once(
                base, path, params, private
            )
            retryable = error is not None or (
                http_status is not None and http_status >= 500
            )
            if not retryable:
                break
            delay = BACKOFF_BASE_SECONDS * (2**attempt)
            time.sleep(delay)
        self.request_total += 1
        record: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "run_id": self._run_id,
            "source": "rest_private" if private else "rest_public",
            "method": "GET",
            "path": path,
            "params": params,
            "ingest_time": utc_now_iso(),
            "latency_ms": round(latency * 1000, 1),
            "http_status": http_status,
            "payload": payload,
            "network_error": error,
        }
        self._writer.write(group or group_for_path(path), record)
        if error is not None:
            self.error_total += 1
            return None
        if isinstance(payload, Mapping) and payload.get("status") == 0:
            return payload.get("data")
        self.error_total += 1
        return None


def rows_of(data: object) -> list[Mapping[str, object]]:
    """取列表载荷，兼容 list 包络。"""
    if isinstance(data, list):
        return [row for row in data if isinstance(row, Mapping)]
    if isinstance(data, Mapping):
        inner = data.get("list")
        if isinstance(inner, list):
            return [row for row in inner if isinstance(row, Mapping)]
    return []


def fetch_public(fetcher: Fetcher) -> tuple[list[str], list[str]]:
    """拉取公开端点，返回现物与杠杆品种表。"""
    print("拉取 status / symbols / ticker")
    fetcher.get("/v1/status")
    symbol_data = fetcher.get("/v1/symbols")
    fetcher.get("/v1/ticker")

    spot: list[str] = []
    leverage: list[str] = []
    for row in rows_of(symbol_data):
        name = str(row.get("symbol", ""))
        if not name:
            continue
        if name.endswith("_JPY"):
            leverage.append(name)
        else:
            spot.append(name)
    all_symbols = spot + leverage
    print(f"品种数 现物 {len(spot)} 杠杆 {len(leverage)}")

    print("拉取全品种盘口与逐笔成交")
    for symbol in all_symbols:
        fetcher.get("/v1/orderbooks", {"symbol": symbol})
        fetcher.get("/v1/trades", {"symbol": symbol, "count": 100})
    for symbol in ("BTC", "BTC_JPY"):
        for page in range(2, 6):
            fetcher.get(
                "/v1/trades", {"symbol": symbol, "count": 100, "page": page}
            )
    return spot, leverage


def fetch_klines(fetcher: Fetcher, spot: list[str], leverage: list[str]) -> None:
    """拉取 KLine：年份探测、全品种日线、代表性分钟线。"""
    print("探测 BTC 日线历史年份")
    years_with_data: list[str] = []
    for year in PROBE_YEARS:
        data = fetcher.get(
            "/v1/klines", {"symbol": "BTC", "interval": "1day", "date": year}
        )
        if rows_of(data):
            years_with_data.append(year)
    start_year = years_with_data[0] if years_with_data else PROBE_YEARS[0]
    print(f"BTC 日线有数据年份 {years_with_data}")

    all_symbols = spot + leverage
    span = [y for y in PROBE_YEARS if y >= start_year]
    print(f"拉取全品种日线 {span[0]} 至 {span[-1]}")
    for symbol in all_symbols:
        if symbol == "BTC":
            continue
        for year in span:
            fetcher.get(
                "/v1/klines",
                {"symbol": symbol, "interval": "1day", "date": year},
            )

    print("拉取 BTC 长周期与代表性分钟线")
    for interval in ("4hour", "1week", "1month"):
        for year in span:
            fetcher.get(
                "/v1/klines",
                {"symbol": "BTC", "interval": interval, "date": year},
            )
    for symbol in INTRADAY_SYMBOLS:
        for interval in INTRADAY_INTERVALS:
            for date in INTRADAY_DATES:
                fetcher.get(
                    "/v1/klines",
                    {"symbol": symbol, "interval": interval, "date": date},
                )

    print("探测 date 参数形态约束")
    fetcher.get("/v1/klines", {"symbol": "BTC", "interval": "1hour", "date": "2026"})
    fetcher.get(
        "/v1/klines", {"symbol": "BTC", "interval": "1day", "date": "20260805"}
    )


def fetch_private(fetcher: Fetcher, spot: list[str], leverage: list[str]) -> None:
    """拉取 13 个 READ_ONLY 端点全量现状。"""
    print("拉取账户四端点")
    fetcher.get("/v1/account/assets", private=True)
    fetcher.get("/v1/account/margin", private=True)
    fetcher.get("/v1/account/tradingVolume", private=True)
    fetcher.get("/v1/positionSummary", private=True)

    all_symbols = spot + leverage
    print("拉取全品种挂单与最新成交")
    for symbol in all_symbols:
        fetcher.get("/v1/activeOrders", {"symbol": symbol}, private=True)
        fetcher.get(
            "/v1/latestExecutions", {"symbol": symbol, "count": 100}, private=True
        )
    print("拉取杠杆品种建玉一覧")
    for symbol in leverage:
        fetcher.get("/v1/openPositions", {"symbol": symbol}, private=True)

    print("探测委托与成交按号查询")
    fetcher.get("/v1/orders", {"orderId": "1"}, private=True)
    fetcher.get("/v1/executions", {"orderId": "1"}, private=True)

    print(f"扫描日本円入出金履历 {HISTORY_SCAN_DAYS} 天")
    now = datetime.now(UTC)
    step = timedelta(minutes=HISTORY_WINDOW_MINUTES)
    aligned = now.replace(minute=now.minute // 30 * 30, second=0, microsecond=0)
    start = aligned - timedelta(days=HISTORY_SCAN_DAYS)
    for endpoint in (
        "/v1/account/fiatDeposit/history",
        "/v1/account/fiatWithdrawal/history",
    ):
        cursor = start
        while cursor <= aligned:
            fetcher.get(
                endpoint,
                {"fromTimestamp": history_timestamp(cursor)},
                private=True,
            )
            cursor += step

    print("探测暗号資産履历与窗口上限")
    recent = history_timestamp(now - step)
    fetcher.get(
        "/v1/account/deposit/history",
        {"symbol": "BTC", "fromTimestamp": recent},
        private=True,
    )
    fetcher.get(
        "/v1/account/withdrawal/history",
        {"symbol": "BTC", "fromTimestamp": recent},
        private=True,
    )
    fetcher.get(
        "/v1/account/fiatDeposit/history",
        {
            "fromTimestamp": history_timestamp(now - timedelta(minutes=120)),
            "toTimestamp": history_timestamp(now),
        },
        private=True,
    )


async def sample_public_ws(writer: RawWriter, run_id: str) -> dict[str, Any]:
    """采样公开 WS 行情，统计各频道消息速率。"""
    stats: dict[str, dict[str, float]] = {}
    frames = 0
    connect_at = utc_now_iso()
    async with websockets.connect(PUBLIC_WS_URL, max_size=2**23) as conn:
        for channel in WS_CHANNELS:
            for symbol in WS_SYMBOLS:
                command = json.dumps(
                    {"command": "subscribe", "channel": channel, "symbol": symbol}
                )
                await conn.send(command)
                await asyncio.sleep(WS_COMMAND_INTERVAL)
        deadline = time.monotonic() + WS_SAMPLE_SECONDS
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                raw = await asyncio.wait_for(conn.recv(), timeout=remaining)
            except TimeoutError:
                break
            text = raw if isinstance(raw, str) else raw.decode("utf-8")
            frames += 1
            received = utc_now_iso()
            try:
                payload = json.loads(text)
            except ValueError:
                payload = None
            channel = ""
            symbol = ""
            if isinstance(payload, Mapping):
                channel = str(payload.get("channel", ""))
                symbol = str(payload.get("symbol", ""))
            key = f"{channel}:{symbol}" if channel else "unknown"
            entry = stats.setdefault(key, {"frames": 0, "bytes": 0})
            entry["frames"] += 1
            entry["bytes"] += len(text.encode("utf-8"))
            writer.write(
                "ws_public",
                {
                    "schema_version": SCHEMA_VERSION,
                    "run_id": run_id,
                    "source": "ws_public",
                    "channel": channel or None,
                    "symbol": symbol or None,
                    "ingest_time": received,
                    "payload": payload if payload is not None else text,
                },
            )
    return {
        "connect_at": connect_at,
        "sample_seconds": WS_SAMPLE_SECONDS,
        "frames": frames,
        "by_channel": stats,
    }


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
    run_id = new_run_id()
    started = utc_now_iso()
    config = load_config(REPO_ROOT / ".env")
    # 立即剥离 TRADE 密钥，本进程只读
    config = dataclasses.replace(
        config, trade_api_key=None, trade_api_secret=None
    )
    day_dir = RAW_ROOT / datetime.now(UTC).strftime("%Y-%m-%d")
    writer = RawWriter(day_dir, run_id)
    fetcher = Fetcher(config, writer, run_id)
    print(f"run_id {run_id} 输出目录 {day_dir}")

    spot, leverage = fetch_public(fetcher)
    fetch_klines(fetcher, spot, leverage)
    fetch_private(fetcher, spot, leverage)

    print(f"WS 采样 {WS_SAMPLE_SECONDS} 秒")
    ws_stats = asyncio.run(sample_public_ws(writer, run_id))

    finished = utc_now_iso()
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "code_version": code_version(),
        "started_at": started,
        "finished_at": finished,
        "request_total": fetcher.request_total,
        "error_total": fetcher.error_total,
        "record_counts": writer.counts,
        "ws_sample": ws_stats,
        "layout_note": "raw 按 UTC 日期分目录、端点分文件（TBD-02 提案试运行）",
    }
    manifest_path = day_dir / f"manifest-{run_id}.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"完成 请求 {fetcher.request_total} 错误 {fetcher.error_total}")
    print(f"清单 {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
