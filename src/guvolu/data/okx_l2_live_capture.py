"""OKX public ``books`` raw v3 segmented capture."""
from __future__ import annotations

import argparse
import asyncio
import json
import time
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol, cast

import websockets
from websockets.exceptions import WebSocketException

from guvolu.api.ws_common import reconnect_delay_seconds, to_text
from guvolu.data.paths import data_root as configured_data_root
from guvolu.data.segmented_raw import (
    SegmentedRawWriter,
    recover_open_segments,
    supervise_capture_tasks,
)

OKX_PUBLIC_WS_URL = "wss://ws.okx.com:8443/ws/v5/public"
OKX_ENDPOINT = "books"
OKX_ENDPOINT_ID = "EP-0032"
OKX_ENDPOINT_REVISION = 0
OKX_RAW_DOMAIN = "book_l2_okx_live"
OKX_DEFAULT_SYMBOL = "BTC-USDT"
SILENCE_TIMEOUT_SECONDS = 90.0
PING_REPLY_TIMEOUT_SECONDS = 10.0
MAX_VALIDATION_MINUTES = 15.0
CHECKPOINT_SECONDS = 60.0
MAX_MESSAGE_BYTES = 8 * 1024 * 1024


class _Connection(Protocol):
    async def send(self, message: str) -> None: ...

    async def recv(self) -> str | bytes: ...


@dataclass
class CaptureStats:
    """Minimal counters persisted with an OKX raw run."""

    venue_symbol: str
    wire_frames: int = 0
    data_frames: int = 0
    control_frames: int = 0
    connection_attempts: int = 0
    successful_sessions: int = 0
    disconnects: int = 0
    reconnects: int = 0
    protocol_errors: int = 0
    notices: int = 0
    application_pings: int = 0
    consecutive_failures: int = 0


def _receive_clock() -> tuple[str, int]:
    return datetime.now(UTC).isoformat(), time.monotonic_ns()


def _active(deadline: float | None) -> bool:
    return deadline is None or time.monotonic() < deadline


def _remaining(deadline: float | None) -> float:
    if deadline is None:
        return SILENCE_TIMEOUT_SECONDS
    return min(SILENCE_TIMEOUT_SECONDS, max(0.05, deadline - time.monotonic()))


def _bounded_reconnect_delay(consecutive_failures: int) -> float:
    return reconnect_delay_seconds(min(max(0, consecutive_failures), 63))


def _json_mapping(text: str) -> Mapping[str, object] | None:
    try:
        loaded = json.loads(text)
    except json.JSONDecodeError:
        return None
    if not isinstance(loaded, Mapping):
        return None
    return cast(Mapping[str, object], loaded)


def _native_channel(text: str, venue_symbol: str) -> str:
    payload = _json_mapping(text)
    if payload is None:
        return "protocol_control"
    arg = payload.get("arg")
    if not isinstance(arg, Mapping):
        return "protocol_control"
    channel = arg.get("channel")
    symbol = arg.get("instId")
    if channel == OKX_ENDPOINT and symbol == venue_symbol:
        return f"{OKX_ENDPOINT}:{venue_symbol}"
    return "protocol_control"


def _is_books_data(text: str, venue_symbol: str) -> bool:
    payload = _json_mapping(text)
    if payload is None or payload.get("action") not in {"snapshot", "update"}:
        return False
    arg = payload.get("arg")
    data = payload.get("data")
    return (
        isinstance(arg, Mapping)
        and arg.get("channel") == OKX_ENDPOINT
        and arg.get("instId") == venue_symbol
        and isinstance(data, list)
    )


def _subscription(venue_symbol: str) -> str:
    return json.dumps(
        {
            "op": "subscribe",
            "args": [{"channel": OKX_ENDPOINT, "instId": venue_symbol}],
        },
        separators=(",", ":"),
    )


def _opened_connection(writer: SegmentedRawWriter, stats: CaptureStats) -> str:
    stats.successful_sessions += 1
    return f"{writer.run_id}-c{stats.successful_sessions:06d}"


def _record_received(
    writer: SegmentedRawWriter,
    stats: CaptureStats,
    connection_id: str,
    text: str,
) -> None:
    received, monotonic_ns = _receive_clock()
    writer.write_frame(
        text,
        OKX_ENDPOINT,
        connection_id=connection_id,
        channel_id=_native_channel(text, writer.venue_symbol),
        recv_ts_utc=received,
        recv_ts_mono_ns=monotonic_ns,
    )
    stats.wire_frames += 1
    payload = _json_mapping(text)
    if _is_books_data(text, writer.venue_symbol):
        stats.data_frames += 1
        stats.consecutive_failures = 0
        return
    stats.control_frames += 1
    if isinstance(payload, Mapping) and payload.get("event") == "error":
        stats.protocol_errors += 1
        raise ConnectionError("OKX books subscription returned an error")
    if isinstance(payload, Mapping) and payload.get("event") == "notice":
        stats.notices += 1
        raise ConnectionError("OKX books connection received a notice")


async def _consume_connection(
    connection: _Connection,
    writer: SegmentedRawWriter,
    stats: CaptureStats,
    connection_id: str,
    deadline: float | None,
) -> None:
    await connection.send(_subscription(writer.venue_symbol))
    while _active(deadline):
        try:
            raw = await asyncio.wait_for(connection.recv(), _remaining(deadline))
        except TimeoutError:
            if not _active(deadline):
                return
            await connection.send("ping")
            stats.application_pings += 1
            try:
                raw = await asyncio.wait_for(
                    connection.recv(), PING_REPLY_TIMEOUT_SECONDS
                )
            except TimeoutError as exc:
                raise ConnectionError("OKX books silence timeout") from exc
            text = to_text(raw)
            _record_received(writer, stats, connection_id, text)
            if text != "pong":
                raise ConnectionError("OKX books ping reply was not pong")
            continue
        _record_received(writer, stats, connection_id, to_text(raw))


async def _record_loop(
    writer: SegmentedRawWriter,
    stats: CaptureStats,
    deadline: float | None,
) -> None:
    while _active(deadline):
        stats.connection_attempts += 1
        session_opened = False
        try:
            async with websockets.connect(
                OKX_PUBLIC_WS_URL,
                ping_interval=None,
                max_size=MAX_MESSAGE_BYTES,
            ) as websocket:
                session_opened = True
                connection_id = _opened_connection(writer, stats)
                await _consume_connection(
                    cast(_Connection, websocket), writer, stats,
                    connection_id, deadline,
                )
        except (OSError, ConnectionError, WebSocketException):
            if not _active(deadline):
                return
            if session_opened:
                stats.disconnects += 1
            stats.consecutive_failures += 1
            stats.reconnects += 1
            await asyncio.sleep(_bounded_reconnect_delay(stats.consecutive_failures))


async def record_books(
    root: Path,
    venue_symbol: str = OKX_DEFAULT_SYMBOL,
    *,
    minutes: float = 1.0,
    segment_seconds: float = 300.0,
    segment_max_bytes: int = 128 * 1024 * 1024,
) -> tuple[CaptureStats, Path]:
    """Record one bounded OKX books validation run."""

    if minutes <= 0 or minutes > MAX_VALIDATION_MINUTES:
        raise ValueError(
            f"OKX books 尚未开放常驻采集；minutes 必须在 (0, "
            f"{MAX_VALIDATION_MINUTES:g}]"
        )
    if venue_symbol != OKX_DEFAULT_SYMBOL:
        raise ValueError("Only the verified BTC-USDT mapping is enabled")

    def progress(segment: Mapping[str, object]) -> None:
        print(
            "SEGMENT okx/"
            f"{venue_symbol} #{segment['segment_sequence']} "
            f"rows={int(str(segment['record_count'])):,} "
            f"bytes={int(str(segment['byte_count'])):,} "
            f"sha256={str(segment['sha256'])[:12]}",
            flush=True,
        )

    writer = SegmentedRawWriter(
        root,
        "okx",
        venue_symbol,
        domain=OKX_RAW_DOMAIN,
        endpoint_id=OKX_ENDPOINT_ID,
        endpoint_revision=OKX_ENDPOINT_REVISION,
        segment_seconds=segment_seconds,
        segment_max_bytes=segment_max_bytes,
        on_segment_sealed=progress,
    )
    stats = CaptureStats(venue_symbol)
    deadline = None if minutes <= 0 else time.monotonic() + minutes * 60
    status = "complete"
    failure: str | None = None

    async def checkpoint_loop() -> None:
        while True:
            if not _active(deadline):
                await asyncio.Event().wait()
            await asyncio.sleep(min(CHECKPOINT_SECONDS, _remaining(deadline)))
            if _active(deadline):
                writer.checkpoint(asdict(stats))

    primary: BaseException | None = None
    manifest: Path | None = None
    try:
        await supervise_capture_tasks(
            _record_loop(writer, stats, deadline), checkpoint_loop(),
        )
    except BaseException as exc:
        primary = exc
        if isinstance(exc, asyncio.CancelledError):
            status = "interrupted"
        else:
            status = "failed"
            failure = f"{type(exc).__name__}: {exc}"
    try:
        manifest = writer.finish(
            {**asdict(stats), "failure_detail": failure}, status=status
        )
    except BaseException as finish_error:
        if primary is None:
            raise
        primary.add_note(
            "writer.finish 未替换采集主异常: "
            f"{type(finish_error).__name__}: {finish_error}"
        )
    if primary is not None:
        raise primary
    assert manifest is not None
    return stats, manifest


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="OKX public books raw capture")
    parser.add_argument("--data-root", type=Path, default=None)
    sub = parser.add_subparsers(dest="command", required=True)
    record = sub.add_parser("record")
    record.add_argument("--symbol", default=OKX_DEFAULT_SYMBOL)
    record.add_argument("--minutes", type=float, default=1.0)
    record.add_argument("--segment-seconds", type=float, default=300.0)
    record.add_argument("--segment-max-mib", type=int, default=128)
    recover = sub.add_parser("recover")
    recover.add_argument("--older-minutes", type=int, default=60)
    args = parser.parse_args(argv)
    root = (args.data_root or configured_data_root()).resolve()
    if args.command == "recover":
        paths = recover_open_segments(
            root, int(args.older_minutes), domain=OKX_RAW_DOMAIN
        )
        print(json.dumps({
            "recovered": len(paths),
            "manifests": [path.as_posix() for path in paths],
        }, ensure_ascii=False, indent=2))
        return 0
    try:
        stats, manifest = asyncio.run(record_books(
            root,
            str(args.symbol),
            minutes=float(args.minutes),
            segment_seconds=float(args.segment_seconds),
            segment_max_bytes=int(args.segment_max_mib) * 1024 * 1024,
        ))
    except KeyboardInterrupt:
        print("Capture interrupted; the active segment was sealed", flush=True)
        return 130
    print(json.dumps({
        **asdict(stats), "run_manifest": manifest.as_posix(),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
