"""bitFlyer API 只读验证脚本。

仅调用公开端点与私有读取端点，绝不触碰写端点（C-14）。
两把密钥各自调用 getpermissions 枚举能力，探测法核实（A-04）。
响应原样落盘 data/raw/<UTC 日期>/bitflyer/<组>.jsonl，
行格式沿用 storage-design 第 4 节（D-02、D-03、D-09）。
公开 WS 采样验证盘口帧无序号无校验和，并测 snapshot 推送节奏。
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import secrets
import subprocess
import sys
import time
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import requests
import websockets

REPO_ROOT = Path(__file__).resolve().parents[1]
RAW_ROOT = REPO_ROOT / "data" / "raw"

REST_BASE = "https://api.bitflyer.com"
WS_URL = "wss://ws.lightstream.bitflyer.com/json-rpc"

SCHEMA_VERSION = 1
TIMEOUT_SECONDS = 15.0
# 自我限速 2 次每秒
REQUEST_INTERVAL = 0.5

WS_SAMPLE_SECONDS = 90.0
WS_PRODUCTS = ("BTC_JPY", "FX_BTC_JPY")
WS_CHANNEL_PREFIXES = (
    "lightning_board_snapshot_",
    "lightning_board_",
    "lightning_ticker_",
    "lightning_executions_",
)

Params = dict[str, str | int]


def utc_now_iso() -> str:
    """当前 UTC 时刻 ISO 文本。"""
    return datetime.now(UTC).isoformat()


def new_run_id() -> str:
    """探测运行标识。"""
    return "runbf" + secrets.token_hex(6)


def load_env_values(path: Path) -> dict[str, str]:
    """解析 .env 键值表，仅本进程使用（T-01）。"""
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip().strip('"')
    return values


class RawWriter:
    """raw 层落盘器，按组分文件，只追加（D-02）。"""

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


class Prober:
    """只读探测器：公开与私有 GET。"""

    def __init__(self, writer: RawWriter, run_id: str) -> None:
        self._writer = writer
        self._run_id = run_id
        self._session = requests.Session()
        self._last_request = 0.0
        self.request_total = 0
        self.error_total = 0

    def _throttle(self) -> None:
        wait = self._last_request + REQUEST_INTERVAL - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        self._last_request = time.monotonic()

    def get(
        self,
        path: str,
        params: Params | None = None,
        *,
        credentials: tuple[str, str] | None = None,
        key_label: str | None = None,
        group: str | None = None,
    ) -> object | None:
        """GET 一次并原样落盘，返回载荷或 None。"""
        query = urlencode(params) if params else ""
        request_path = path + ("?" + query if query else "")
        headers: dict[str, str] = {}
        if credentials is not None:
            api_key, api_secret = credentials
            timestamp = str(int(time.time()))
            text = timestamp + "GET" + request_path
            signature = hmac.new(
                api_secret.encode(), text.encode(), hashlib.sha256
            ).hexdigest()
            headers = {
                "ACCESS-KEY": api_key,
                "ACCESS-TIMESTAMP": timestamp,
                "ACCESS-SIGN": signature,
            }
        self._throttle()
        started = time.monotonic()
        http_status: int | None = None
        payload: object | None = None
        error: str | None = None
        try:
            response = self._session.get(
                REST_BASE + request_path,
                headers=headers or None,
                timeout=TIMEOUT_SECONDS,
            )
            http_status = response.status_code
            try:
                payload = response.json()
            except ValueError:
                error = "非 JSON 响应"
        except requests.RequestException as exc:
            error = f"{type(exc).__name__}: {exc}"
        latency = time.monotonic() - started
        self.request_total += 1
        record: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "run_id": self._run_id,
            "source": "rest_private" if credentials else "rest_public",
            "method": "GET",
            "path": path,
            "params": params,
            "key_label": key_label,
            "ingest_time": utc_now_iso(),
            "latency_ms": round(latency * 1000, 1),
            "http_status": http_status,
            "payload": payload,
            "network_error": error,
        }
        self._writer.write(group or group_for_path(path), record)
        if error is not None or http_status != 200:
            self.error_total += 1
            return None
        return payload


def probe_public(prober: Prober) -> dict[str, Any]:
    """公开端点全量探测，返回观测摘要。"""
    summary: dict[str, Any] = {}
    print("拉取 markets 两形态")
    markets = prober.get("/v1/markets")
    prober.get("/v1/getmarkets")
    if isinstance(markets, list):
        summary["product_codes"] = [
            row.get("product_code")
            for row in markets
            if isinstance(row, Mapping)
        ]

    print("拉取盘口并测档数")
    board_depth: dict[str, dict[str, int]] = {}
    for product in WS_PRODUCTS:
        board = prober.get("/v1/board", {"product_code": product})
        if isinstance(board, Mapping):
            bids = board.get("bids")
            asks = board.get("asks")
            board_depth[product] = {
                "bids": len(bids) if isinstance(bids, list) else 0,
                "asks": len(asks) if isinstance(asks, list) else 0,
            }
    summary["board_depth"] = board_depth

    print("拉取 ticker 与状态端点")
    for product in WS_PRODUCTS:
        prober.get("/v1/ticker", {"product_code": product})
        prober.get("/v1/getboardstate", {"product_code": product})
        prober.get("/v1/gethealth", {"product_code": product})
    prober.get("/v1/getfundingrate", {"product_code": "FX_BTC_JPY"})
    prober.get("/v1/getcorporateleverage")

    print("探测 executions 返回上限与历史边界")
    executions = prober.get(
        "/v1/executions", {"product_code": "BTC_JPY", "count": 500}
    )
    if isinstance(executions, list):
        summary["executions_count_500"] = len(executions)
        ids = [
            row.get("id") for row in executions if isinstance(row, Mapping)
        ]
        numeric = [i for i in ids if isinstance(i, int)]
        summary["executions_id_range"] = (
            [min(numeric), max(numeric)] if numeric else None
        )
    over = prober.get(
        "/v1/executions", {"product_code": "BTC_JPY", "count": 1000}
    )
    if isinstance(over, list):
        summary["executions_count_1000"] = len(over)
    # 极旧 id 应触发 31 天边界错误
    prober.get(
        "/v1/executions",
        {"product_code": "BTC_JPY", "count": 10, "before": 1000},
    )
    return summary


def probe_private(
    prober: Prober,
    read_credentials: tuple[str, str],
    trade_credentials: tuple[str, str] | None,
) -> dict[str, Any]:
    """私有读取端点探测，两把密钥各枚举权限。"""
    summary: dict[str, Any] = {}

    print("READ_ONLY 密钥权限枚举")
    permissions = prober.get(
        "/v1/me/getpermissions",
        credentials=read_credentials,
        key_label="read_only",
        group="me_getpermissions_read",
    )
    if isinstance(permissions, list):
        summary["read_only_permissions"] = permissions

    print("READ_ONLY 私有读取端点")
    prober.get(
        "/v1/me/getbalance", credentials=read_credentials, key_label="read_only"
    )
    prober.get(
        "/v1/me/getcollateral",
        credentials=read_credentials,
        key_label="read_only",
    )
    for path, params in (
        ("/v1/me/getchildorders", {"product_code": "BTC_JPY", "count": 10}),
        ("/v1/me/getparentorders", {"product_code": "BTC_JPY", "count": 10}),
        ("/v1/me/getexecutions", {"product_code": "BTC_JPY", "count": 10}),
        ("/v1/me/getpositions", {"product_code": "FX_BTC_JPY"}),
        ("/v1/me/getbalancehistory", {"currency_code": "JPY", "count": 10}),
        ("/v1/me/gettradingcommission", {"product_code": "BTC_JPY"}),
        ("/v1/me/getdeposits", {"count": 10}),
        ("/v1/me/getwithdrawals", {"count": 10}),
        ("/v1/me/getcoinins", {"count": 10}),
        ("/v1/me/getcoinouts", {"count": 10}),
        ("/v1/me/getaddresses", None),
    ):
        prober.get(
            path,
            dict(params) if params else None,
            credentials=read_credentials,
            key_label="read_only",
        )

    if trade_credentials is not None:
        print("TRADE 密钥权限枚举（仅权限端点，探测法）")
        trade_permissions = prober.get(
            "/v1/me/getpermissions",
            credentials=trade_credentials,
            key_label="trade",
            group="me_getpermissions_trade",
        )
        if isinstance(trade_permissions, list):
            summary["trade_permissions"] = trade_permissions
        # 核实 TRADE 密钥读取边界
        prober.get(
            "/v1/me/getbalance",
            credentials=trade_credentials,
            key_label="trade",
            group="me_getbalance_trade",
        )
    return summary


async def sample_public_ws(writer: RawWriter, run_id: str) -> dict[str, Any]:
    """公开 WS 采样：频率、字节量、字段键集合。"""
    stats: dict[str, dict[str, float]] = {}
    key_sets: dict[str, set[str]] = {}
    snapshot_times: dict[str, list[float]] = {}
    frames = 0
    connect_at = utc_now_iso()
    async with websockets.connect(WS_URL, max_size=2**24) as conn:
        request_id = 0
        for prefix in WS_CHANNEL_PREFIXES:
            for product in WS_PRODUCTS:
                request_id += 1
                await conn.send(
                    json.dumps(
                        {
                            "jsonrpc": "2.0",
                            "method": "subscribe",
                            "params": {"channel": prefix + product},
                            "id": request_id,
                        }
                    )
                )
                await asyncio.sleep(0.2)
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
            message: object = None
            if isinstance(payload, Mapping):
                params = payload.get("params")
                if isinstance(params, Mapping):
                    channel = str(params.get("channel", ""))
                    message = params.get("message")
            key = channel or "control"
            entry = stats.setdefault(key, {"frames": 0, "bytes": 0})
            entry["frames"] += 1
            entry["bytes"] += len(text.encode("utf-8"))
            if isinstance(message, Mapping):
                key_sets.setdefault(key, set()).update(
                    str(name) for name in message
                )
            if "board_snapshot" in key:
                snapshot_times.setdefault(key, []).append(time.monotonic())
            writer.write(
                "ws_public",
                {
                    "schema_version": SCHEMA_VERSION,
                    "run_id": run_id,
                    "source": "ws_public",
                    "channel": channel or None,
                    "symbol": None,
                    "ingest_time": received,
                    "payload": payload if payload is not None else text,
                },
            )
    intervals: dict[str, list[float]] = {}
    for key, moments in snapshot_times.items():
        intervals[key] = [
            round(later - earlier, 2)
            for earlier, later in zip(moments, moments[1:], strict=False)
        ]
    return {
        "connect_at": connect_at,
        "sample_seconds": WS_SAMPLE_SECONDS,
        "frames": frames,
        "by_channel": stats,
        "message_keys": {k: sorted(v) for k, v in key_sets.items()},
        "snapshot_intervals": intervals,
    }


async def check_private_ws(
    read_credentials: tuple[str, str], writer: RawWriter, run_id: str
) -> dict[str, Any]:
    """私有 WS 认证与订阅能力核实，只订阅不产生消息。"""
    api_key, api_secret = read_credentials
    timestamp = int(time.time() * 1000)
    nonce = secrets.token_hex(16)
    signature = hmac.new(
        api_secret.encode(), f"{timestamp}{nonce}".encode(), hashlib.sha256
    ).hexdigest()
    outcome: dict[str, Any] = {"auth": None, "subscriptions": {}}
    async with websockets.connect(WS_URL, max_size=2**24) as conn:
        await conn.send(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "method": "auth",
                    "params": {
                        "api_key": api_key,
                        "timestamp": timestamp,
                        "nonce": nonce,
                        "signature": signature,
                    },
                    "id": 1,
                }
            )
        )
        auth_raw = await asyncio.wait_for(conn.recv(), timeout=10)
        auth_text = (
            auth_raw if isinstance(auth_raw, str) else auth_raw.decode("utf-8")
        )
        auth_payload = json.loads(auth_text)
        outcome["auth"] = auth_payload.get("result", auth_payload.get("error"))
        writer.write(
            "ws_private",
            {
                "schema_version": SCHEMA_VERSION,
                "run_id": run_id,
                "source": "ws_private",
                "channel": "auth",
                "symbol": None,
                "ingest_time": utc_now_iso(),
                # 认证响应不含密钥内容，原样落盘
                "payload": auth_payload,
            },
        )
        for index, channel in enumerate(
            ("child_order_events", "parent_order_events"), start=2
        ):
            await conn.send(
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "method": "subscribe",
                        "params": {"channel": channel},
                        "id": index,
                    }
                )
            )
            reply_raw = await asyncio.wait_for(conn.recv(), timeout=10)
            reply_text = (
                reply_raw
                if isinstance(reply_raw, str)
                else reply_raw.decode("utf-8")
            )
            reply = json.loads(reply_text)
            outcome["subscriptions"][channel] = reply.get(
                "result", reply.get("error")
            )
            writer.write(
                "ws_private",
                {
                    "schema_version": SCHEMA_VERSION,
                    "run_id": run_id,
                    "source": "ws_private",
                    "channel": channel,
                    "symbol": None,
                    "ingest_time": utc_now_iso(),
                    "payload": reply,
                },
            )
    return outcome


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
    values = load_env_values(REPO_ROOT / ".env")
    read_key = values.get("BITFLYER_READ_ONLY_API_KEY", "")
    read_secret = values.get("BITFLYER_READ_ONLY_API_SECRET", "")
    trade_key = values.get("BITFLYER_TRADE_API_KEY", "")
    trade_secret = values.get("BITFLYER_TRADE_API_SECRET", "")
    if not read_key or not read_secret:
        print("缺少 BITFLYER_READ_ONLY 密钥")
        return 1
    trade_credentials = (
        (trade_key, trade_secret) if trade_key and trade_secret else None
    )

    run_id = new_run_id()
    started = utc_now_iso()
    day_dir = RAW_ROOT / datetime.now(UTC).strftime("%Y-%m-%d") / "bitflyer"
    writer = RawWriter(day_dir, run_id)
    prober = Prober(writer, run_id)
    print(f"run_id {run_id} 输出目录 {day_dir}")

    public_summary = probe_public(prober)
    private_summary = probe_private(
        prober, (read_key, read_secret), trade_credentials
    )

    print(f"公开 WS 采样 {WS_SAMPLE_SECONDS} 秒")
    ws_stats = asyncio.run(sample_public_ws(writer, run_id))

    print("私有 WS 认证核实")
    try:
        private_ws = asyncio.run(
            check_private_ws((read_key, read_secret), writer, run_id)
        )
    except (OSError, TimeoutError, ValueError) as exc:
        private_ws = {"error": f"{type(exc).__name__}: {exc}"}

    finished = utc_now_iso()
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "code_version": code_version(),
        "started_at": started,
        "finished_at": finished,
        "request_total": prober.request_total,
        "error_total": prober.error_total,
        "record_counts": writer.counts,
        "public_summary": public_summary,
        "private_summary": private_summary,
        "ws_sample": ws_stats,
        "private_ws": private_ws,
    }
    manifest_path = day_dir / f"manifest-{run_id}.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"完成 请求 {prober.request_total} 错误 {prober.error_total}")
    print(f"清单 {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
