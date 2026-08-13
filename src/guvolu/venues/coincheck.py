"""Coincheck 公共实时行情旁路采集。"""
from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Sequence

import websockets
from websockets.exceptions import ConnectionClosed

from guvolu.data.raw_writer import RawWriter

VENUE_ID = "coincheck"
PUBLIC_WS_URL = "wss://ws-api.coincheck.com"


def public_channels(pairs: Sequence[str]) -> list[str]:
    """构造逐笔与盘口频道，保持输入顺序。"""
    channels: list[str] = []
    for pair in pairs:
        channels.extend((f"{pair}-trades", f"{pair}-orderbook"))
    return channels


def subscribe_message(channel: str) -> str:
    """生成官方单频道订阅报文。"""
    return json.dumps({"type": "subscribe", "channel": channel}, separators=(",", ":"))


async def record_public(
    writer: RawWriter, pairs: Sequence[str], seconds: float
) -> int:
    """在给定时长内持久化 Coincheck 公开 wire 帧。"""
    if seconds <= 0:
        raise ValueError("seconds 必须为正数")
    deadline = time.monotonic() + seconds
    frames = 0
    while True:
        try:
            async with websockets.connect(
                PUBLIC_WS_URL, ping_interval=20, ping_timeout=60
            ) as connection:
                for channel in public_channels(pairs):
                    await connection.send(subscribe_message(channel))
                while True:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return frames
                    try:
                        message = await asyncio.wait_for(connection.recv(), remaining)
                    except TimeoutError:
                        return frames
                    text = (
                        message.decode("utf-8")
                        if isinstance(message, bytes) else message
                    )
                    writer.ws_frame("coincheck/ws_public", text)
                    frames += 1
        except (ConnectionClosed, OSError):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return frames
            await asyncio.sleep(min(1.0, remaining))
