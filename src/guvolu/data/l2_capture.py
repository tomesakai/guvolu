"""GMO、bitbank、bitFlyer 的公开 L2 run-scoped 分段采集。"""
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
from guvolu.data.book_l2_anchor import RestAnchorWorker
from guvolu.data.paths import data_root as configured_data_root
from guvolu.data.segmented_raw import SegmentedRawWriter, recover_open_segments
from guvolu.venues.bitbank_stream import PUBLIC_WS_URL as BITBANK_WS_URL

BITFLYER_WS_URL = "wss://ws.lightstream.bitflyer.com/json-rpc"
SILENCE_TIMEOUT_SECONDS = 90.0
CHECKPOINT_SECONDS = 60.0
ANCHOR_PERIODIC_SECONDS = 300.0
ENDPOINT_BINDINGS = {
    "gmo": ("EP-0007", 0),
    # r1 增加状态频道。
    # r0 原件保持不变。
    "bitbank": ("EP-0005", 1),
    "bitflyer": ("EP-0002", 0),
}
BITBANK_SOURCE_SCOPE = "depth_whole/depth_diff/circuit_break_info"


@dataclass
class CaptureStats:
    """一个实时 run 的最小健康计数。

    ``anchor_completed`` 表示已入队任务完成独立持久化，即使远端返回
    不可用；``anchor_failed`` 仅表示已入队任务执行异常；队列拒绝只计入
    ``anchor_triggers_dropped``，三者不重叠。
    """

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
    anchor_triggers_enqueued: int = 0
    anchor_triggers_dropped: int = 0
    anchor_completed: int = 0
    anchor_failed: int = 0


AnchorSubmit = Callable[[str | None, str], bool]


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


def _observed_data(stats: CaptureStats) -> None:
    stats.data_frames += 1
    stats.consecutive_failures = 0


def _checkpoint_anchor_settlement(
    writer: SegmentedRawWriter,
    stats: CaptureStats,
    completed: int,
    failed: int,
) -> None:
    """在采集事件循环中同步一次后台锚点结算。"""
    stats.anchor_completed = completed
    stats.anchor_failed = failed
    writer.checkpoint(asdict(stats))


def _trigger_anchor(
    submit: AnchorSubmit | None,
    stats: CaptureStats,
    connection_id: str,
) -> None:
    """按连接序号非阻塞投递 REST 锚点。"""
    if submit is None:
        return
    reason = (
        "connection_open" if stats.successful_sessions == 1 else "reconnect"
    )
    if submit(connection_id, reason):
        stats.anchor_triggers_enqueued += 1
    else:
        stats.anchor_triggers_dropped += 1


def _active(deadline: float | None) -> bool:
    return deadline is None or time.monotonic() < deadline


def _remaining(deadline: float | None) -> float:
    if deadline is None:
        return SILENCE_TIMEOUT_SECONDS
    return min(SILENCE_TIMEOUT_SECONDS, max(0.05, deadline - time.monotonic()))


async def _periodic_anchor_loop(
    submit: AnchorSubmit,
    stats: CaptureStats,
    deadline: float | None,
    *,
    interval_seconds: float = ANCHOR_PERIODIC_SECONDS,
) -> None:
    """定期旁路观察；不阻塞 WS，也不补写 L2。"""
    if interval_seconds <= 0:
        raise ValueError("REST anchor interval must be positive")
    while _active(deadline):
        remaining = (
            interval_seconds if deadline is None
            else max(0.0, deadline - time.monotonic())
        )
        await asyncio.sleep(min(interval_seconds, remaining))
        if _active(deadline):
            if submit(None, "periodic"):
                stats.anchor_triggers_enqueued += 1
            else:
                stats.anchor_triggers_dropped += 1


async def _record_gmo(
    writer: SegmentedRawWriter, stats: CaptureStats, deadline: float | None,
    anchor_submit: AnchorSubmit | None = None,
) -> None:
    while _active(deadline):
        stats.connection_attempts += 1
        session_opened = False
        try:
            async with websockets.connect(GMO_WS_URL) as connection:
                session_opened = True
                connection_id = _opened_connection(writer, stats)
                await connection.send(json.dumps({
                    "command": "subscribe",
                    "channel": "orderbooks",
                    "symbol": writer.venue_symbol,
                }))
                _trigger_anchor(anchor_submit, stats, connection_id)
                while _active(deadline):
                    try:
                        raw = await asyncio.wait_for(
                            connection.recv(), _remaining(deadline)
                        )
                    except TimeoutError:
                        if not _active(deadline):
                            return
                        raise ConnectionError("GMO L2 静默超时")
                    recv_ts_utc, recv_ts_mono_ns = _receive_clock()
                    text = to_text(raw)
                    payload = _json_mapping(text)
                    writer.write_frame(
                        text, "orderbooks/ws", connection_id=connection_id,
                        channel_id=_mapping_channel(payload, "orderbooks"),
                        recv_ts_utc=recv_ts_utc,
                        recv_ts_mono_ns=recv_ts_mono_ns,
                    )
                    stats.wire_frames += 1
                    if payload is None:
                        stats.control_frames += 1
                    else:
                        if payload.get("channel") == "orderbooks":
                            _observed_data(stats)
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
    first_raw = await connection.recv()  # type: ignore[attr-defined]
    recv_ts_utc, recv_ts_mono_ns = _receive_clock()
    first = to_text(first_raw)
    writer.write_frame(
        first, BITBANK_SOURCE_SCOPE, connection_id=connection_id,
        channel_id="protocol_control", recv_ts_utc=recv_ts_utc,
        recv_ts_mono_ns=recv_ts_mono_ns,
    )
    stats.wire_frames += 1
    stats.control_frames += 1
    if not first.startswith("0"):
        raise ConnectionError("bitbank 缺少 Engine.IO open 包")
    await connection.send("40")  # type: ignore[attr-defined]
    opened_raw = await connection.recv()  # type: ignore[attr-defined]
    recv_ts_utc, recv_ts_mono_ns = _receive_clock()
    opened = to_text(opened_raw)
    writer.write_frame(
        opened, BITBANK_SOURCE_SCOPE, connection_id=connection_id,
        channel_id="protocol_control", recv_ts_utc=recv_ts_utc,
        recv_ts_mono_ns=recv_ts_mono_ns,
    )
    stats.wire_frames += 1
    stats.control_frames += 1
    if not opened.startswith("40"):
        raise ConnectionError("bitbank 缺少 Socket.IO open 包")


async def _record_bitbank(
    writer: SegmentedRawWriter, stats: CaptureStats, deadline: float | None,
    anchor_submit: AnchorSubmit | None = None,
) -> None:
    rooms = (
        f"depth_whole_{writer.venue_symbol}",
        f"depth_diff_{writer.venue_symbol}",
        f"circuit_break_info_{writer.venue_symbol}",
    )
    while _active(deadline):
        stats.connection_attempts += 1
        session_opened = False
        try:
            async with websockets.connect(BITBANK_WS_URL, ping_interval=None) as connection:
                session_opened = True
                connection_id = _opened_connection(writer, stats)
                await _bitbank_handshake(
                    connection, writer, stats, connection_id
                )
                for room in rooms:
                    await connection.send(
                        "42" + json.dumps(["join-room", room], separators=(",", ":"))
                    )
                _trigger_anchor(anchor_submit, stats, connection_id)
                while _active(deadline):
                    try:
                        raw = await asyncio.wait_for(
                            connection.recv(), _remaining(deadline)
                        )
                    except TimeoutError:
                        if not _active(deadline):
                            return
                        raise ConnectionError("bitbank L2 静默超时")
                    recv_ts_utc, recv_ts_mono_ns = _receive_clock()
                    text = to_text(raw)
                    channel_id = _bitbank_channel_id(text)
                    writer.write_frame(
                        text, BITBANK_SOURCE_SCOPE,
                        connection_id=connection_id, channel_id=channel_id,
                        recv_ts_utc=recv_ts_utc,
                        recv_ts_mono_ns=recv_ts_mono_ns,
                    )
                    stats.wire_frames += 1
                    if text == "2":
                        await connection.send("3")
                        stats.control_frames += 1
                    elif channel_id != "protocol_control":
                        _observed_data(stats)
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
    anchor_submit: AnchorSubmit | None = None,
) -> None:
    channels = (
        f"lightning_board_snapshot_{writer.venue_symbol}",
        f"lightning_board_{writer.venue_symbol}",
    )
    while _active(deadline):
        stats.connection_attempts += 1
        session_opened = False
        try:
            async with websockets.connect(BITFLYER_WS_URL) as connection:
                session_opened = True
                connection_id = _opened_connection(writer, stats)
                for request_id, channel in enumerate(channels, start=1):
                    await connection.send(json.dumps({
                        "jsonrpc": "2.0",
                        "method": "subscribe",
                        "params": {"channel": channel},
                        "id": request_id,
                    }))
                _trigger_anchor(anchor_submit, stats, connection_id)
                while _active(deadline):
                    try:
                        raw = await asyncio.wait_for(
                            connection.recv(), _remaining(deadline)
                        )
                    except TimeoutError:
                        if not _active(deadline):
                            return
                        raise ConnectionError("bitFlyer L2 静默超时")
                    recv_ts_utc, recv_ts_mono_ns = _receive_clock()
                    text = to_text(raw)
                    payload = _json_mapping(text)
                    channel_id = _bitflyer_channel_id(payload)
                    writer.write_frame(
                        text, "board_snapshot/board",
                        connection_id=connection_id,
                        channel_id=channel_id,
                        recv_ts_utc=recv_ts_utc,
                        recv_ts_mono_ns=recv_ts_mono_ns,
                    )
                    stats.wire_frames += 1
                    if payload is None:
                        stats.control_frames += 1
                    else:
                        if (
                            payload.get("method") == "channelMessage"
                            and channel_id != "protocol_control"
                        ):
                            _observed_data(stats)
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
    str,
    Callable[
        [SegmentedRawWriter, CaptureStats, float | None, AnchorSubmit | None],
        Awaitable[None],
    ],
] = {
    "gmo": _record_gmo,
    "bitbank": _record_bitbank,
    "bitflyer": _record_bitflyer,
}


async def record_l2(
    root: Path,
    venue_id: str,
    venue_symbol: str,
    minutes: float,
    segment_seconds: float,
    segment_max_bytes: int,
) -> tuple[CaptureStats, Path]:
    """录制一个市场；有限时长正常结束，0 表示常驻。"""
    recorder = _RECORDERS.get(venue_id)
    if recorder is None:
        raise ValueError(f"暂不支持 L2 分段采集: {venue_id}")

    def progress(segment: Mapping[str, object]) -> None:
        print(
            "SEGMENT "
            f"{venue_id}/{venue_symbol} #{segment['segment_sequence']} "
            f"rows={int(str(segment['record_count'])):,} "
            f"bytes={int(str(segment['byte_count'])):,} "
            f"sha256={str(segment['sha256'])[:12]}",
            flush=True,
        )

    endpoint_id, endpoint_revision = ENDPOINT_BINDINGS[venue_id]
    writer = SegmentedRawWriter(
        root, venue_id, venue_symbol,
        endpoint_id=endpoint_id,
        endpoint_revision=endpoint_revision,
        segment_seconds=segment_seconds,
        segment_max_bytes=segment_max_bytes,
        on_segment_sealed=progress,
    )
    stats = CaptureStats(venue_id, venue_symbol)
    anchor_worker = RestAnchorWorker(
        root, venue_id, venue_symbol,
        on_settled=lambda completed, failed: _checkpoint_anchor_settlement(
            writer, stats, completed, failed
        ),
    )
    anchor_worker.start()
    deadline = None if minutes <= 0 else time.monotonic() + minutes * 60
    status = "complete"
    failure: str | None = None

    async def checkpoint_loop() -> None:
        while _active(deadline):
            await asyncio.sleep(
                min(CHECKPOINT_SECONDS, _remaining(deadline))
            )
            if _active(deadline):
                writer.checkpoint(asdict(stats))

    checkpoint_task = asyncio.create_task(checkpoint_loop())
    anchor_task = asyncio.create_task(
        _periodic_anchor_loop(anchor_worker.submit, stats, deadline)
    )
    try:
        await recorder(writer, stats, deadline, anchor_worker.submit)
    except asyncio.CancelledError:
        status = "interrupted"
        raise
    except Exception as exc:
        status = "failed"
        failure = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        checkpoint_task.cancel()
        anchor_task.cancel()
        try:
            await checkpoint_task
        except asyncio.CancelledError:
            pass
        try:
            await anchor_task
        except asyncio.CancelledError:
            pass
        await anchor_worker.close()
        stats.anchor_completed = anchor_worker.completed
        stats.anchor_failed = anchor_worker.failed
        manifest = writer.finish(
            {**asdict(stats), "failure_detail": failure}, status=status
        )
    return stats, manifest


def main(argv: Sequence[str] | None = None) -> int:
    """命令行入口。"""
    parser = argparse.ArgumentParser(description="三所 L2 分段原文采集")
    parser.add_argument("--data-root", type=Path, default=None)
    sub = parser.add_subparsers(dest="command", required=True)
    record = sub.add_parser("record", help="录制一个 venue/market L2")
    record.add_argument("--venue", choices=sorted(_RECORDERS), required=True)
    record.add_argument("--symbol", required=True)
    record.add_argument("--minutes", type=float, default=1.0)
    record.add_argument("--segment-seconds", type=float, default=300.0)
    record.add_argument("--segment-max-mib", type=int, default=128)
    recover = sub.add_parser("recover", help="封口静默且逐行完整的崩溃片段")
    recover.add_argument("--older-minutes", type=int, default=60)
    args = parser.parse_args(argv)
    root = (args.data_root or configured_data_root()).resolve()
    if args.command == "recover":
        paths = recover_open_segments(root, int(args.older_minutes))
        print(json.dumps({
            "recovered": len(paths),
            "manifests": [path.as_posix() for path in paths],
        }, ensure_ascii=False, indent=2))
        return 0
    try:
        stats, manifest = asyncio.run(record_l2(
            root, str(args.venue), str(args.symbol), float(args.minutes),
            float(args.segment_seconds), int(args.segment_max_mib) * 1024 * 1024,
        ))
    except KeyboardInterrupt:
        print("采集已中断；当前片段已由 finally 封口", flush=True)
        return 130
    print(json.dumps({
        **asdict(stats), "run_manifest": manifest.as_posix(),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
