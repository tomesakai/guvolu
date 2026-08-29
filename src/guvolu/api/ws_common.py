"""WS 共通构件：帧解码、命令节奏、重连退避（W-07 消除重复）。

订阅与退订限速为每秒一次且按 IP 计（R-04，官方文档），
因此节奏器缺省为进程内共享实例，公开与私有客户端共用。
"""
from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Awaitable, Callable
from typing import Literal

from websockets.asyncio.client import ClientConnection

from guvolu.domain.errors import WsError, extract_error_code
from guvolu.domain.models import Raw

# 订阅与退订限速（R-04，按 IP）
SUBSCRIBE_INTERVAL_SECONDS = 1.1

# 权限类错误码，重连无效（C-09）
PERMISSION_WS_ERROR_CODES: frozenset[str] = frozenset({"ERR-5012"})

# 断线重连退避下限与上限
RECONNECT_BASE_SECONDS = 1.0
RECONNECT_MAX_SECONDS = 60.0

WsCommand = Literal["subscribe", "unsubscribe"]
Sender = Callable[[str], Awaitable[None]]
Clock = Callable[[], float]


def decode_frame(text: str) -> Raw:
    """解码帧并检出错误帧。

    权限等错误以 {"error": ...} 消息帧返回且不断开连接，
    必须在此抛出，否则将持续收不到数据（C-09）。
    错误码提取进 WsError.code，处置见 docs/error-catalog.md。
    """
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise WsError("WS 帧不是合法 JSON") from exc
    if not isinstance(payload, dict):
        raise WsError("WS 帧结构不是对象")
    if "error" in payload:
        detail = str(payload["error"])
        raise WsError(f"WS 错误帧: {detail}", code=extract_error_code(detail))
    return payload


def is_permission_ws_error(error: WsError) -> bool:
    """判定权限类错误帧（C-09）。

    此类错误重连无用，处置见 docs/error-catalog.md，
    连接循环必须把它上抛给调用方而非退避重连。
    """
    return error.code in PERMISSION_WS_ERROR_CODES


def command_wait_seconds(last_sent_at: float | None, now: float) -> float:
    """计算下一条命令需等待的秒数（R-04）。"""
    if last_sent_at is None:
        return 0.0
    return max(0.0, last_sent_at + SUBSCRIBE_INTERVAL_SECONDS - now)


def reconnect_delay_seconds(attempt: int) -> float:
    """断线重连退避秒数，指数增长并设上限。"""
    if attempt <= 0:
        return RECONNECT_BASE_SECONDS
    return min(RECONNECT_MAX_SECONDS, RECONNECT_BASE_SECONDS * 2.0**attempt)


def text_sender(connection: ClientConnection) -> Sender:
    """把连接包装为文本发送函数。"""

    async def send(text: str) -> None:
        await connection.send(text)

    return send


def to_text(raw: str | bytes) -> str:
    """WS 帧统一转为文本。"""
    return raw if isinstance(raw, str) else raw.decode("utf-8")


class CommandPacer:
    """命令节奏器。限速按 IP 计，跨客户端必须共享同一实例（R-04）。

    clock 注入点供测试提供确定性时钟。
    """

    def __init__(
        self,
        interval_seconds: float = SUBSCRIBE_INTERVAL_SECONDS,
        clock: Clock = time.monotonic,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("命令间隔必须为正")
        self._interval = interval_seconds
        self._clock = clock
        self._last_at: float | None = None
        self._lock = asyncio.Lock()

    async def wait_turn(self) -> None:
        """必要时等待到本命令的发送时隙。"""
        async with self._lock:
            now = self._clock()
            if self._last_at is not None:
                wait = max(0.0, self._last_at + self._interval - now)
                if wait > 0:
                    await asyncio.sleep(wait)
            self._last_at = self._clock()


# 进程内共享节奏器，两类客户端缺省共用
SHARED_PACER = CommandPacer()
