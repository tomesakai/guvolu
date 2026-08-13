"""bitbank Socket.IO 4.x 公共流 wire 采集。"""
from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Sequence
from typing import Protocol

import websockets
from websockets.exceptions import ConnectionClosed

from guvolu.data.raw_writer import RawWriter

VENUE_ID = "bitbank"
PUBLIC_WS_URL = "wss://stream.bitbank.cc/socket.io/?EIO=4&transport=websocket"


class SocketConnection(Protocol):
    """本采集器所需的 WebSocket 最小接口。"""

    async def recv(self) -> str | bytes: ...

    async def send(self, message: str) -> None: ...


def public_rooms(pairs: Sequence[str]) -> list[str]:
    """构造逐笔、全量/差分深度与市场状态房间。"""
    rooms: list[str] = []
    for pair in pairs:
        rooms.extend((
            f"transactions_{pair}",
            f"depth_whole_{pair}",
            f"depth_diff_{pair}",
            f"circuit_break_info_{pair}",
        ))
    return rooms


def join_packet(room: str) -> str:
    """生成 Socket.IO 4.x 加入房间数据包。"""
    return "42" + json.dumps(["join-room", room], separators=(",", ":"))


def _text(message: str | bytes) -> str:
    """将 WebSocket 二进制帧限定为 UTF-8 文本。"""
    return message.decode("utf-8") if isinstance(message, bytes) else message


async def _handshake(connection: SocketConnection, writer: RawWriter) -> None:
    """完成 Engine.IO 与 Socket.IO 握手，逐帧先落原文。"""
    first = _text(await connection.recv())
    writer.ws_frame("bitbank/ws_public", first)
    if not first.startswith("0"):
        raise ValueError("bitbank 缺少 Engine.IO open 包")
    await connection.send("40")
    opened = _text(await connection.recv())
    writer.ws_frame("bitbank/ws_public", opened)
    if not opened.startswith("40"):
        raise ValueError("bitbank 缺少 Socket.IO open 包")


async def record_public(
    writer: RawWriter, pairs: Sequence[str], seconds: float
) -> int:
    """按 Socket.IO 4.x 协议采集公开成交与盘口原始帧。"""
    if seconds <= 0:
        raise ValueError("seconds 必须为正数")
    deadline = time.monotonic() + seconds
    frames = 0
    while True:
        try:
            async with websockets.connect(
                PUBLIC_WS_URL, ping_interval=None
            ) as connection:
                await _handshake(connection, writer)
                for room in public_rooms(pairs):
                    await connection.send(join_packet(room))
                while True:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return frames
                    try:
                        message = await asyncio.wait_for(connection.recv(), remaining)
                    except TimeoutError:
                        return frames
                    text = _text(message)
                    writer.ws_frame("bitbank/ws_public", text)
                    if text == "2":
                        await connection.send("3")
                        continue
                    if text.startswith("42"):
                        frames += 1
        except (ConnectionClosed, OSError):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return frames
            await asyncio.sleep(min(1.0, remaining))
