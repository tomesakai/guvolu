"""GMO 外国為替FX 公共 ticker 的轮询分段采集（TBD-32 实时旁路）。

端点无需密钥。按固定间隔 GET，把整份响应原文作为 raw v3 帧落盘；
多个 symbol 共用同一次请求，各自写入独立 run 目录，不裁剪原文（D-02）。
响应形态依据 2026-09-04 单次 GET 实测（A-04）：
``{"status":0,"data":[{"symbol","ask","bid","timestamp","status"}],"responsetime"}``。
"""
from __future__ import annotations

import argparse
import asyncio
import json
import re
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Protocol

import requests

from guvolu.api.ws_common import reconnect_delay_seconds
from guvolu.data.paths import data_root as configured_data_root
from guvolu.data.segmented_raw import (
    SegmentedRawWriter,
    recover_open_segments,
    supervise_capture_tasks,
)

FX_RATE_DOMAIN = "fx_rate"
FX_VENUE_ID = "gmo_fx"
FX_TICKER_URL = "https://forex-api.coin.z.com/public/v1/ticker"
FX_SOURCE_ENDPOINT = "public/v1/ticker"
FX_TRANSPORT = "https"
FX_DATA_CHANNEL = "ticker"
FX_CONTROL_CHANNEL = "protocol_control"
ENDPOINT_BINDING = ("EP-0076", 0)
DEFAULT_SYMBOLS: tuple[str, ...] = ("USD_JPY",)
DEFAULT_INTERVAL_SECONDS = 60.0
# 公共端点，命令行轮询下限
MIN_INTERVAL_SECONDS = 10.0
HTTP_TIMEOUT_SECONDS = 15.0
HTTP_OK = 200
HTTP_TOO_MANY_REQUESTS = 429
# 数据静默预算，超出即重建会话
DATA_SILENCE_TIMEOUT_SECONDS = 300.0
CHECKPOINT_SECONDS = 60.0
MAX_BACKOFF_EXPONENT = 63
FX_DECIMAL_TEXT_MAX_CHARS = 64
USER_AGENT = "guvolu-fx-capture/1"
FX_QUOTE_STATUSES = frozenset({"OPEN", "CLOSE"})
_SYMBOL = re.compile(r"^[A-Z]{3}_[A-Z]{3}$")


@dataclass(frozen=True, slots=True)
class TickerQuote:
    """一条 ticker 报价；价格为 Decimal（T-08）。"""

    symbol: str
    bid: Decimal
    ask: Decimal
    event_time: datetime
    status: str


@dataclass(frozen=True, slots=True)
class TickerFrame:
    """一帧 status 为 0 的 ticker 响应。"""

    quotes: Mapping[str, TickerQuote]
    response_time: datetime | None


def validate_symbol(symbol: str) -> str:
    """校验 FX symbol 形态，如 ``USD_JPY``。"""
    if _SYMBOL.fullmatch(symbol) is None:
        raise ValueError(f"FX symbol 形态非法: {symbol!r}")
    return symbol


def _decimal_text(value: object) -> Decimal:
    """字符串直接进 Decimal，不经过 float（T-08）。"""
    if isinstance(value, Decimal):
        parsed = value
    elif (
        isinstance(value, str)
        and 0 < len(value) <= FX_DECIMAL_TEXT_MAX_CHARS
    ):
        try:
            parsed = Decimal(value)
        except (InvalidOperation, ValueError) as exc:
            raise ValueError(f"报价非十进制文本: {value!r}") from exc
    else:
        raise ValueError(f"报价类型非法: {value!r}")
    if not parsed.is_finite() or parsed <= 0:
        raise ValueError(f"报价须为有限正数: {value!r}")
    return parsed


def _aware_datetime(value: object, label: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{label} 缺失")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{label} 非法: {value!r}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{label} 缺少时区")
    return parsed.astimezone(UTC)


def parse_ticker_quote(item: object) -> TickerQuote:
    """严格解析一项报价；任一字段非法即抛 ValueError。"""
    if not isinstance(item, Mapping):
        raise ValueError("ticker 项非对象")
    symbol = item.get("symbol")
    if not isinstance(symbol, str):
        raise ValueError("ticker symbol 缺失")
    status = item.get("status")
    if not isinstance(status, str) or status not in FX_QUOTE_STATUSES:
        raise ValueError(f"ticker status 非法: {status!r}")
    return TickerQuote(
        symbol=validate_symbol(symbol),
        bid=_decimal_text(item.get("bid")),
        ask=_decimal_text(item.get("ask")),
        event_time=_aware_datetime(item.get("timestamp"), "ticker timestamp"),
        status=status,
    )


def _status_zero(value: object) -> bool:
    """T-10：只有整数 0 才是成功。"""
    return isinstance(value, int) and not isinstance(value, bool) and value == 0


def fx_channel_id(payload_raw: str) -> str:
    """按内容判定数据帧；采集与物化共用同一规则。"""
    try:
        loaded = json.loads(payload_raw)
    except ValueError:
        return FX_CONTROL_CHANNEL
    if (
        not isinstance(loaded, Mapping)
        or not _status_zero(loaded.get("status"))
        or not isinstance(loaded.get("data"), list)
    ):
        return FX_CONTROL_CHANNEL
    return FX_DATA_CHANNEL


def parse_ticker_frame(payload_raw: str) -> TickerFrame | None:
    """解析数据帧全部报价；非数据帧返回 None，坏项抛 ValueError。"""
    if fx_channel_id(payload_raw) != FX_DATA_CHANNEL:
        return None
    loaded = json.loads(payload_raw, parse_float=Decimal)
    if not isinstance(loaded, Mapping):
        raise ValueError("ticker 响应非对象")
    quotes: dict[str, TickerQuote] = {}
    for item in loaded["data"]:
        quote = parse_ticker_quote(item)
        if quote.symbol in quotes:
            raise ValueError(f"ticker symbol 重复: {quote.symbol}")
        quotes[quote.symbol] = quote
    response_time = loaded.get("responsetime")
    return TickerFrame(
        quotes=quotes,
        response_time=(
            None if response_time is None
            else _aware_datetime(response_time, "responsetime")
        ),
    )


class FxFetchError(Exception):
    """HTTP 层失败；调用方按退避重试。"""


@dataclass(frozen=True, slots=True)
class FetchResult:
    """一次 GET 的原文、状态码与双接收时钟。"""

    text: str
    http_status: int
    request_time: str
    recv_ts_utc: str
    recv_ts_mono_ns: int
    elapsed_seconds: float


class FetchSession(Protocol):
    """可重建的抓取会话；测试以脚本会话替换。"""

    def fetch(self) -> FetchResult: ...

    def close(self) -> None: ...


def _receive_clock() -> tuple[str, int]:
    return datetime.now(UTC).isoformat(), time.monotonic_ns()


class HttpFetchSession:
    """requests 会话封装；只读 GET，不持有任何密钥。"""

    def __init__(self, url: str = FX_TICKER_URL) -> None:
        self._url = url
        self._session = requests.Session()
        self._session.headers["User-Agent"] = USER_AGENT

    def fetch(self) -> FetchResult:
        request_time = datetime.now(UTC).isoformat()
        started = time.monotonic()
        try:
            response = self._session.get(
                self._url, timeout=HTTP_TIMEOUT_SECONDS,
            )
        except requests.RequestException as exc:
            raise FxFetchError(f"{type(exc).__name__}: {exc}") from exc
        recv_ts_utc, recv_ts_mono_ns = _receive_clock()
        elapsed = round(time.monotonic() - started, 3)
        try:
            text = response.content.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise FxFetchError(f"响应非 UTF-8: {exc}") from exc
        return FetchResult(
            text, int(response.status_code), request_time,
            recv_ts_utc, recv_ts_mono_ns, elapsed,
        )

    def close(self) -> None:
        self._session.close()


@dataclass
class FxCaptureStats:
    """一个 fx_rate 轮询 run 的健康计数。"""

    venue_id: str
    venue_symbols: tuple[str, ...]
    fetch_attempts: int = 0
    wire_frames: int = 0
    data_frames: int = 0
    control_frames: int = 0
    sessions: int = 0
    reconnects: int = 0
    consecutive_failures: int = 0
    data_silence_resets: int = 0
    last_request_time: str | None = None
    last_wire_time: str | None = None
    last_data_time: str | None = None
    last_http_status: int | None = None
    last_fetch_seconds: float | None = None
    last_failure: str | None = None


def _active(deadline: float | None) -> bool:
    return deadline is None or time.monotonic() < deadline


def _bounded(delay: float, deadline: float | None) -> float:
    if deadline is None:
        return max(0.0, delay)
    return max(0.0, min(delay, deadline - time.monotonic()))


def _failure_delay(consecutive_failures: int, interval_seconds: float) -> float:
    """失败退避以轮询间隔为上限。"""
    exponent = min(max(0, consecutive_failures), MAX_BACKOFF_EXPONENT)
    return min(interval_seconds, reconnect_delay_seconds(exponent))


def _healthy_frame(text: str, symbols: Sequence[str]) -> bool:
    """只有含全部目标 symbol 的合法数据帧才刷新 watchdog。"""
    try:
        frame = parse_ticker_frame(text)
    except ValueError:
        return False
    return frame is not None and all(symbol in frame.quotes for symbol in symbols)


def _report(event: str, **fields: object) -> None:
    print(json.dumps({"event": event, **fields}, ensure_ascii=False), flush=True)


async def _record_fx(
    writers: Sequence[SegmentedRawWriter],
    stats: FxCaptureStats,
    deadline: float | None,
    *,
    interval_seconds: float,
    data_silence_seconds: float,
    session_factory: Callable[[], FetchSession],
) -> None:
    """轮询 ticker；HTTP 失败退避，数据静默超预算则重建会话。"""
    session = session_factory()
    stats.sessions += 1
    last_data_clock = time.monotonic()
    try:
        while _active(deadline):
            if time.monotonic() - last_data_clock >= data_silence_seconds:
                session.close()
                session = session_factory()
                stats.sessions += 1
                stats.reconnects += 1
                stats.data_silence_resets += 1
                last_data_clock = time.monotonic()
                _report(
                    "fx_rate_data_silence_reset",
                    sessions=stats.sessions,
                    consecutive_failures=stats.consecutive_failures,
                    last_failure=stats.last_failure,
                )
            stats.fetch_attempts += 1
            try:
                result = await asyncio.to_thread(session.fetch)
            except FxFetchError as exc:
                stats.consecutive_failures += 1
                stats.last_failure = str(exc)
                await asyncio.sleep(_bounded(
                    _failure_delay(stats.consecutive_failures, interval_seconds),
                    deadline,
                ))
                continue
            channel_id = fx_channel_id(result.text)
            for writer in writers:
                writer.write_frame(
                    result.text, FX_SOURCE_ENDPOINT, FX_TRANSPORT,
                    connection_id=f"{writer.run_id}-c{stats.sessions:06d}",
                    channel_id=channel_id,
                    recv_ts_utc=result.recv_ts_utc,
                    recv_ts_mono_ns=result.recv_ts_mono_ns,
                )
            stats.wire_frames += 1
            stats.last_wire_time = result.recv_ts_utc
            stats.last_request_time = result.request_time
            stats.last_http_status = result.http_status
            stats.last_fetch_seconds = result.elapsed_seconds
            healthy = (
                result.http_status == HTTP_OK
                and channel_id == FX_DATA_CHANNEL
                and _healthy_frame(result.text, stats.venue_symbols)
            )
            if healthy:
                stats.data_frames += 1
                stats.last_data_time = result.recv_ts_utc
                stats.consecutive_failures = 0
                last_data_clock = time.monotonic()
                delay = interval_seconds
            else:
                stats.control_frames += 1
                stats.consecutive_failures += 1
                stats.last_failure = (
                    f"http={result.http_status} channel={channel_id}"
                )
                delay = _failure_delay(
                    stats.consecutive_failures, interval_seconds,
                )
                if result.http_status == HTTP_TOO_MANY_REQUESTS:
                    delay = interval_seconds
            await asyncio.sleep(_bounded(delay, deadline))
    finally:
        session.close()


def _progress(symbol: str) -> Callable[[Mapping[str, object]], None]:
    def report(segment: Mapping[str, object]) -> None:
        print(
            "SEGMENT "
            f"{FX_VENUE_ID}/{symbol} {FX_RATE_DOMAIN} "
            f"#{segment['segment_sequence']} "
            f"rows={int(str(segment['record_count'])):,} "
            f"bytes={int(str(segment['byte_count'])):,} "
            f"sha256={str(segment['sha256'])[:12]}",
            flush=True,
        )

    return report


def _finish_all(
    writers: Sequence[SegmentedRawWriter],
    extra: Mapping[str, object],
    status: str,
) -> tuple[Path, ...]:
    return tuple(writer.finish(extra, status=status) for writer in writers)


async def record_fx_rates(
    root: Path,
    symbols: Sequence[str],
    minutes: float,
    segment_seconds: float,
    segment_max_bytes: int,
    *,
    interval_seconds: float = DEFAULT_INTERVAL_SECONDS,
    data_silence_seconds: float = DATA_SILENCE_TIMEOUT_SECONDS,
    session_factory: Callable[[], FetchSession] = HttpFetchSession,
) -> tuple[FxCaptureStats, tuple[Path, ...]]:
    """录制若干 FX symbol；零分钟表示常驻。"""
    validated = tuple(dict.fromkeys(
        validate_symbol(symbol) for symbol in symbols
    ))
    if not validated:
        raise ValueError("至少需要一个 FX symbol")
    if interval_seconds <= 0:
        raise ValueError("interval_seconds 必须为正数")
    if data_silence_seconds <= interval_seconds:
        raise ValueError("data_silence_seconds 须大于轮询间隔")
    endpoint_id, endpoint_revision = ENDPOINT_BINDING
    writers = [
        SegmentedRawWriter(
            root, FX_VENUE_ID, symbol, domain=FX_RATE_DOMAIN,
            endpoint_id=endpoint_id, endpoint_revision=endpoint_revision,
            segment_seconds=segment_seconds,
            segment_max_bytes=segment_max_bytes,
            on_segment_sealed=_progress(symbol),
        )
        for symbol in validated
    ]
    stats = FxCaptureStats(FX_VENUE_ID, validated)
    deadline = None if minutes <= 0 else time.monotonic() + minutes * 60

    async def checkpoint_loop() -> None:
        while True:
            if not _active(deadline):
                await asyncio.Event().wait()
            await asyncio.sleep(_bounded(CHECKPOINT_SECONDS, deadline))
            if _active(deadline):
                for writer in writers:
                    writer.checkpoint(asdict(stats))

    try:
        await supervise_capture_tasks(
            _record_fx(
                writers, stats, deadline,
                interval_seconds=interval_seconds,
                data_silence_seconds=data_silence_seconds,
                session_factory=session_factory,
            ),
            checkpoint_loop(),
        )
    except BaseException as exc:
        if isinstance(exc, asyncio.CancelledError):
            status = "interrupted"
            failure: str | None = None
        else:
            status = "failed"
            failure = f"{type(exc).__name__}: {exc}"
        try:
            _finish_all(
                writers, {**asdict(stats), "failure_detail": failure}, status,
            )
        except BaseException as finish_error:
            exc.add_note(
                "writer.finish 未替换采集主异常: "
                f"{type(finish_error).__name__}: {finish_error}"
            )
        raise
    manifests = _finish_all(
        writers, {**asdict(stats), "failure_detail": None}, "complete",
    )
    return stats, manifests


def main(argv: Sequence[str] | None = None) -> int:
    """命令行入口。"""
    parser = argparse.ArgumentParser(
        description="GMO 外国為替FX ticker 轮询分段原文采集",
    )
    parser.add_argument("--data-root", type=Path, default=None)
    sub = parser.add_subparsers(dest="command", required=True)
    record = sub.add_parser("record")
    record.add_argument(
        "--symbol", action="append", default=None,
        help="可重复；缺省 USD_JPY",
    )
    record.add_argument("--minutes", type=float, default=1.0)
    record.add_argument(
        "--interval-seconds", type=float, default=DEFAULT_INTERVAL_SECONDS,
    )
    record.add_argument(
        "--data-silence-seconds", type=float,
        default=DATA_SILENCE_TIMEOUT_SECONDS,
    )
    record.add_argument("--segment-seconds", type=float, default=300.0)
    record.add_argument("--segment-max-mib", type=int, default=32)
    recover = sub.add_parser("recover")
    recover.add_argument("--older-minutes", type=int, default=60)
    args = parser.parse_args(argv)
    root = (args.data_root or configured_data_root()).resolve()
    if args.command == "recover":
        paths = recover_open_segments(
            root, int(args.older_minutes), domain=FX_RATE_DOMAIN,
        )
        print(json.dumps({
            "recovered": len(paths),
            "manifests": [path.as_posix() for path in paths],
        }, ensure_ascii=False, indent=2))
        return 0
    interval = float(args.interval_seconds)
    if interval < MIN_INTERVAL_SECONDS:
        raise ValueError(
            f"interval-seconds 不得小于 {MIN_INTERVAL_SECONDS:g}"
        )
    symbols: tuple[str, ...] = (
        tuple(str(symbol) for symbol in args.symbol)
        if args.symbol else DEFAULT_SYMBOLS
    )
    try:
        stats, manifests = asyncio.run(record_fx_rates(
            root, symbols, float(args.minutes),
            float(args.segment_seconds),
            int(args.segment_max_mib) * 1024 * 1024,
            interval_seconds=interval,
            data_silence_seconds=float(args.data_silence_seconds),
        ))
    except KeyboardInterrupt:
        print("FX 采集已中断；当前片段已封口", flush=True)
        return 130
    print(json.dumps({
        **asdict(stats),
        "run_manifests": [path.as_posix() for path in manifests],
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
