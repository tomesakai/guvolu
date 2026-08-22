"""GMO、bitbank、bitFlyer 的公开逐笔分段采集。"""
from __future__ import annotations

import argparse
import asyncio
import json
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import websockets
from websockets.exceptions import WebSocketException

from guvolu.api.ws_common import reconnect_delay_seconds, to_text
from guvolu.api.ws_public import PUBLIC_WS_URL as GMO_WS_URL
from guvolu.data.paths import data_root as configured_data_root
from guvolu.data.segmented_raw import SegmentedRawWriter, recover_open_segments
from guvolu.venues.bitbank_stream import PUBLIC_WS_URL as BITBANK_WS_URL

BITFLYER_WS_URL = "wss://ws.lightstream.bitflyer.com/json-rpc"
SILENCE_TIMEOUT_SECONDS = 90.0
CHECKPOINT_SECONDS = 60.0
ENDPOINT_BINDINGS = {
    "gmo": ("EP-0007", 1),
    "bitbank": ("EP-0075", 0),
    "bitflyer": ("EP-0002", 0),
}


@dataclass
class CaptureStats:
    """一个实时逐笔 run 的健康计数。"""

    venue_id: str
    venue_symbol: str
    wire_frames: int = 0
    data_frames: int = 0
    control_frames: int = 0
    sessions: int = 0
    reconnects: int = 0
    connection_attempts: int = 0
    successful_sessions: int = 0
    disconnects: int = 0
    consecutive_failures: int = 0
    last_wire_time: str | None = None
    last_data_time: str | None = None


def _receive_clock() -> tuple[str, int]:
    return datetime.now(UTC).isoformat(), time.monotonic_ns()


def _opened_connection(writer: SegmentedRawWriter, stats: CaptureStats) -> str:
    stats.sessions += 1
    stats.successful_sessions += 1
    return f"{writer.run_id}-c{stats.successful_sessions:06d}"


def _json_mapping(text: str) -> Mapping[str, object] | None:
    try:
        loaded = json.loads(text)
    except json.JSONDecodeError:
        return None
    if not isinstance(loaded, Mapping):
        return None
    return cast(Mapping[str, object], loaded)


def _mapping_channel(
    payload: Mapping[str, object] | None, fallback: str,
) -> str:
    if payload is None:
        return fallback
    channel = payload.get("channel")
    return channel if isinstance(channel, str) and channel else fallback


def _bitbank_channel_id(text: str) -> str:
    if not text.startswith("42"):
        return "protocol_control"
    try:
        packet = json.loads(text[2:])
    except json.JSONDecodeError:
        return "protocol_control"
    if not isinstance(packet, list) or len(packet) < 2:
        return "protocol_control"
    envelope = packet[1]
    if not isinstance(envelope, Mapping):
        return "protocol_control"
    room = envelope.get("room_name")
    return room if isinstance(room, str) and room else "protocol_control"


def _bitflyer_channel_id(payload: Mapping[str, object] | None) -> str:
    if payload is None:
        return "protocol_control"
    params = payload.get("params")
    if not isinstance(params, Mapping):
        return "protocol_control"
    channel = params.get("channel")
    return channel if isinstance(channel, str) and channel else "protocol_control"


def _observed_data(stats: CaptureStats, recv_ts_utc: str) -> None:
    stats.data_frames += 1
    stats.last_data_time = recv_ts_utc
    stats.consecutive_failures = 0


def _active(deadline: float | None) -> bool:
    return deadline is None or time.monotonic() < deadline


def _remaining(deadline: float | None) -> float:
    if deadline is None:
        return SILENCE_TIMEOUT_SECONDS
    return min(SILENCE_TIMEOUT_SECONDS, max(0.05, deadline - time.monotonic()))


def _gmo_error_frame(payload: Mapping[str, object] | None) -> bool:
    """识别 GMO 错误响应；调用方必须先持久化原帧再触发重连。"""
    if payload is None:
        return False
    error = payload.get("error")
    errors = payload.get("errors")
    return error not in (None, False, "", []) or errors not in (
        None, False, "", []
    )


async def _record_gmo(
    writer: SegmentedRawWriter, stats: CaptureStats, deadline: float | None,
) -> None:
    while _active(deadline):
        stats.connection_attempts += 1
        session_opened = False
        try:
            async with websockets.connect(GMO_WS_URL) as connection:
                session_opened = True
                connection_id = _opened_connection(writer, stats)
                await connection.send(json.dumps({
                    "command": "subscribe", "channel": "trades",
                    "symbol": writer.venue_symbol, "option": "TAKER_ONLY",
                }))
                while _active(deadline):
                    try:
                        raw = await asyncio.wait_for(
                            connection.recv(), _remaining(deadline)
                        )
                    except TimeoutError:
                        if not _active(deadline):
                            return
                        raise ConnectionError("GMO trades 静默超时")
                    recv_ts_utc, recv_ts_mono_ns = _receive_clock()
                    text = to_text(raw)
                    payload = _json_mapping(text)
                    writer.write_frame(
                        text, "trades/ws", connection_id=connection_id,
                        channel_id=_mapping_channel(payload, "trades"),
                        recv_ts_utc=recv_ts_utc,
                        recv_ts_mono_ns=recv_ts_mono_ns,
                    )
                    stats.wire_frames += 1
                    stats.last_wire_time = recv_ts_utc
                    if _gmo_error_frame(payload):
                        stats.control_frames += 1
                        raise ConnectionError("GMO trades 返回错误帧")
                    if payload is None:
                        stats.control_frames += 1
                    else:
                        if payload.get("channel") == "trades":
                            _observed_data(stats, recv_ts_utc)
                        else:
                            stats.control_frames += 1
        except (OSError, ConnectionError, WebSocketException):
            if not _active(deadline):
                return
            if session_opened:
                stats.disconnects += 1
            stats.consecutive_failures += 1
            stats.reconnects += 1
            await asyncio.sleep(
                reconnect_delay_seconds(stats.consecutive_failures)
            )


async def _bitbank_handshake(
    connection: object, writer: SegmentedRawWriter, stats: CaptureStats,
    connection_id: str,
) -> None:
    for expected in ("0", "40"):
        raw = await connection.recv()  # type: ignore[attr-defined]
        recv_ts_utc, recv_ts_mono_ns = _receive_clock()
        text = to_text(raw)
        writer.write_frame(
            text, "transactions", connection_id=connection_id,
            channel_id="protocol_control", recv_ts_utc=recv_ts_utc,
            recv_ts_mono_ns=recv_ts_mono_ns,
        )
        stats.wire_frames += 1
        stats.last_wire_time = recv_ts_utc
        stats.control_frames += 1
        if not text.startswith(expected):
            raise ConnectionError(f"bitbank 缺少 Socket.IO 握手包 {expected}")
        if expected == "0":
            await connection.send("40")  # type: ignore[attr-defined]


async def _record_bitbank(
    writer: SegmentedRawWriter, stats: CaptureStats, deadline: float | None,
) -> None:
    room = f"transactions_{writer.venue_symbol}"
    while _active(deadline):
        stats.connection_attempts += 1
        session_opened = False
        try:
            async with websockets.connect(
                BITBANK_WS_URL, ping_interval=None
            ) as connection:
                session_opened = True
                connection_id = _opened_connection(writer, stats)
                await _bitbank_handshake(
                    connection, writer, stats, connection_id
                )
                await connection.send(
                    "42" + json.dumps(["join-room", room], separators=(",", ":"))
                )
                while _active(deadline):
                    if deadline is None:
                        raw = await connection.recv()
                    else:
                        try:
                            raw = await asyncio.wait_for(
                                connection.recv(), _remaining(deadline)
                            )
                        except TimeoutError:
                            return
                    recv_ts_utc, recv_ts_mono_ns = _receive_clock()
                    text = to_text(raw)
                    channel_id = _bitbank_channel_id(text)
                    writer.write_frame(
                        text, "transactions", connection_id=connection_id,
                        channel_id=channel_id, recv_ts_utc=recv_ts_utc,
                        recv_ts_mono_ns=recv_ts_mono_ns,
                    )
                    stats.wire_frames += 1
                    stats.last_wire_time = recv_ts_utc
                    if text == "2":
                        await connection.send("3")
                        stats.control_frames += 1
                    elif channel_id != "protocol_control":
                        _observed_data(stats, recv_ts_utc)
                    else:
                        stats.control_frames += 1
        except (OSError, ConnectionError, WebSocketException):
            if not _active(deadline):
                return
            if session_opened:
                stats.disconnects += 1
            stats.consecutive_failures += 1
            stats.reconnects += 1
            await asyncio.sleep(
                reconnect_delay_seconds(stats.consecutive_failures)
            )


async def _record_bitflyer(
    writer: SegmentedRawWriter, stats: CaptureStats, deadline: float | None,
) -> None:
    channel = f"lightning_executions_{writer.venue_symbol}"
    while _active(deadline):
        stats.connection_attempts += 1
        session_opened = False
        try:
            async with websockets.connect(BITFLYER_WS_URL) as connection:
                session_opened = True
                connection_id = _opened_connection(writer, stats)
                await connection.send(json.dumps({
                    "jsonrpc": "2.0", "method": "subscribe",
                    "params": {"channel": channel}, "id": 1,
                }))
                while _active(deadline):
                    if deadline is None:
                        raw = await connection.recv()
                    else:
                        try:
                            raw = await asyncio.wait_for(
                                connection.recv(), _remaining(deadline)
                            )
                        except TimeoutError:
                            return
                    recv_ts_utc, recv_ts_mono_ns = _receive_clock()
                    text = to_text(raw)
                    payload = _json_mapping(text)
                    channel_id = _bitflyer_channel_id(payload)
                    writer.write_frame(
                        text, "lightning_executions",
                        connection_id=connection_id, channel_id=channel_id,
                        recv_ts_utc=recv_ts_utc,
                        recv_ts_mono_ns=recv_ts_mono_ns,
                    )
                    stats.wire_frames += 1
                    stats.last_wire_time = recv_ts_utc
                    if payload is None:
                        stats.control_frames += 1
                    else:
                        params = payload.get("params")
                        if (
                            payload.get("method") == "channelMessage"
                            and isinstance(params, Mapping)
                            and str(params.get("channel", "")).startswith(
                                "lightning_executions_"
                            )
                        ):
                            _observed_data(stats, recv_ts_utc)
                        else:
                            stats.control_frames += 1
        except (OSError, ConnectionError, WebSocketException):
            if not _active(deadline):
                return
            if session_opened:
                stats.disconnects += 1
            stats.consecutive_failures += 1
            stats.reconnects += 1
            await asyncio.sleep(
                reconnect_delay_seconds(stats.consecutive_failures)
            )


_RECORDERS: dict[
    str, Callable[[SegmentedRawWriter, CaptureStats, float | None], Awaitable[None]]
] = {
    "gmo": _record_gmo,
    "bitbank": _record_bitbank,
    "bitflyer": _record_bitflyer,
}


async def record_trades(
    root: Path, venue_id: str, venue_symbol: str, minutes: float,
    segment_seconds: float, segment_max_bytes: int,
) -> tuple[CaptureStats, Path]:
    """录制一个市场；零分钟表示常驻。"""
    recorder = _RECORDERS.get(venue_id)
    if recorder is None:
        raise ValueError(f"暂不支持逐笔分段采集: {venue_id}")

    def progress(segment: Mapping[str, object]) -> None:
        print(
            "SEGMENT "
            f"{venue_id}/{venue_symbol} trades #{segment['segment_sequence']} "
            f"rows={int(str(segment['record_count'])):,} "
            f"bytes={int(str(segment['byte_count'])):,} "
            f"sha256={str(segment['sha256'])[:12]}",
            flush=True,
        )

    endpoint_id, endpoint_revision = ENDPOINT_BINDINGS[venue_id]
    writer = SegmentedRawWriter(
        root, venue_id, venue_symbol, domain="trade_realtime",
        endpoint_id=endpoint_id,
        endpoint_revision=endpoint_revision,
        segment_seconds=segment_seconds, segment_max_bytes=segment_max_bytes,
        on_segment_sealed=progress,
    )
    stats = CaptureStats(venue_id, venue_symbol)
    deadline = None if minutes <= 0 else time.monotonic() + minutes * 60
    status = "complete"
    failure: str | None = None

    async def checkpoint_loop() -> None:
        while _active(deadline):
            await asyncio.sleep(min(CHECKPOINT_SECONDS, _remaining(deadline)))
            if _active(deadline):
                writer.checkpoint(asdict(stats))

    checkpoint_task = asyncio.create_task(checkpoint_loop())
    try:
        await recorder(writer, stats, deadline)
    except asyncio.CancelledError:
        status = "interrupted"
        raise
    except Exception as exc:
        status = "failed"
        failure = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        checkpoint_task.cancel()
        try:
            await checkpoint_task
        except asyncio.CancelledError:
            pass
        manifest = writer.finish(
            {**asdict(stats), "failure_detail": failure}, status=status
        )
    return stats, manifest


def main(argv: Sequence[str] | None = None) -> int:
    """命令行入口。"""
    parser = argparse.ArgumentParser(description="三所实时逐笔分段原文采集")
    parser.add_argument("--data-root", type=Path, default=None)
    sub = parser.add_subparsers(dest="command", required=True)
    record = sub.add_parser("record")
    record.add_argument("--venue", choices=sorted(_RECORDERS), required=True)
    record.add_argument("--symbol", required=True)
    record.add_argument("--minutes", type=float, default=1.0)
    record.add_argument("--segment-seconds", type=float, default=300.0)
    record.add_argument("--segment-max-mib", type=int, default=32)
    recover = sub.add_parser("recover")
    recover.add_argument("--older-minutes", type=int, default=60)
    args = parser.parse_args(argv)
    root = (args.data_root or configured_data_root()).resolve()
    if args.command == "recover":
        paths = recover_open_segments(
            root, int(args.older_minutes), domain="trade_realtime"
        )
        print(json.dumps({
            "recovered": len(paths),
            "manifests": [path.as_posix() for path in paths],
        }, ensure_ascii=False, indent=2))
        return 0
    try:
        stats, manifest = asyncio.run(record_trades(
            root, str(args.venue), str(args.symbol), float(args.minutes),
            float(args.segment_seconds), int(args.segment_max_mib) * 1024 * 1024,
        ))
    except KeyboardInterrupt:
        print("逐笔采集已中断；当前片段已封口", flush=True)
        return 130
    print(json.dumps({
        **asdict(stats), "run_manifest": manifest.as_posix(),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
