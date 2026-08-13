"""私有 WebSocket 客户端：令牌管理、事件帧解析与频道订阅。

设计遵循 C-02：报文解析写成纯函数，可在无网络下单测；
连接循环只做 IO，不含业务判断。
订阅与退订限速每秒一次（R-04）。
订阅后的权限错误以消息帧返回且不断连，必须解析并抛出（C-09）。
重连后由上层以 REST 全量快照对账（C-10、R-08）。
私有频道实测仅 READ_ONLY 密钥可订阅，令牌须由该密钥签发（T-02）。
"""
from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
import websockets
from websockets.exceptions import WebSocketException

from guvolu.api.transport import PrivateTransport
from guvolu.domain.enums import WsChannel
from guvolu.api.ws_common import (
    RECONNECT_BASE_SECONDS as RECONNECT_BASE_SECONDS,
    RECONNECT_MAX_SECONDS as RECONNECT_MAX_SECONDS,
    SUBSCRIBE_INTERVAL_SECONDS as SUBSCRIBE_INTERVAL_SECONDS,
    SHARED_PACER,
    CommandPacer,
    Sender,
    WsCommand,
    command_wait_seconds as command_wait_seconds,
    decode_frame as decode_frame,
    reconnect_delay_seconds as reconnect_delay_seconds,
    text_sender,
    to_text,
)
from guvolu.domain.errors import WsError
from guvolu.domain.models import (
    Raw,
    WsExecutionEvent,
    WsOrderEvent,
    WsPositionEvent,
    WsPositionSummaryEvent,
)

WS_AUTH_PATH = "/v1/ws-auth"
# 地址漏 /v1 会得到 404
PRIVATE_WS_URL_TEMPLATE = "wss://api.coin.z.com/ws/private/v1/{token}"

# 持仓汇总频道可选值
PERIODIC = "PERIODIC"

PRIVATE_CHANNELS: frozenset[WsChannel] = frozenset(
    {
        WsChannel.ORDER_EVENTS,
        WsChannel.EXECUTION_EVENTS,
        WsChannel.POSITION_EVENTS,
        WsChannel.POSITION_SUMMARY_EVENTS,
    }
)

PrivateEvent = (
    WsOrderEvent | WsExecutionEvent | WsPositionEvent | WsPositionSummaryEvent
)

_PRIVATE_PARSERS: Mapping[str, Callable[[Raw], PrivateEvent]] = {
    WsChannel.ORDER_EVENTS.value: WsOrderEvent.from_api,
    WsChannel.EXECUTION_EVENTS.value: WsExecutionEvent.from_api,
    WsChannel.POSITION_EVENTS.value: WsPositionEvent.from_api,
    WsChannel.POSITION_SUMMARY_EVENTS.value: WsPositionSummaryEvent.from_api,
}


def create_ws_token(transport: PrivateTransport) -> str:
    """签发 WS 令牌，有效期六十分钟。

    传输层已按 T-10 校验 status 并取出 data，此处只校验形态。
    """
    data = transport.request("POST", WS_AUTH_PATH, body={})
    if not isinstance(data, str):
        raise WsError("ws-auth 返回的令牌不是字符串")
    return data


def extend_ws_token(transport: PrivateTransport, token: str) -> None:
    """延长令牌有效期。该方法的签名串不含 body，例外已在签名层处理（C-07）。"""
    transport.request("PUT", WS_AUTH_PATH, body={"token": token})


def revoke_ws_token(transport: PrivateTransport, token: str) -> None:
    """撤销令牌。该方法的签名串不含 body，例外已在签名层处理（C-07）。"""
    transport.request("DELETE", WS_AUTH_PATH, body={"token": token})


async def keepalive(
    transport: PrivateTransport, token: str, interval_seconds: float
) -> None:
    """周期延长令牌有效期，直到被取消。

    间隔由调用方给定，不设缺省魔数（C-04）。
    延长为阻塞的 HTTP 写请求，放入线程执行以免阻塞事件循环。
    """
    if interval_seconds <= 0:
        raise ValueError("保活间隔必须为正")
    while True:
        await asyncio.sleep(interval_seconds)
        await asyncio.to_thread(extend_ws_token, transport, token)


def private_ws_url(token: str) -> str:
    """拼接私有 WS 地址。令牌为凭据，不得写入日志（T-01）。"""
    return PRIVATE_WS_URL_TEMPLATE.format(token=token)


def parse_private_message(text: str) -> PrivateEvent:
    """解析私有事件帧为领域模型。

    纯函数，不涉及 IO（C-02）。金额字段经 from_api 转 Decimal（T-08）。
    """
    payload = decode_frame(text)
    channel = str(payload.get("channel", ""))
    parser = _PRIVATE_PARSERS.get(channel)
    if parser is None:
        raise WsError(f"未知私有频道: {channel}")
    return parser(payload)


def build_private_command(
    command: WsCommand, channel: WsChannel, option: str | None = None
) -> str:
    """构造私有频道命令报文，非私有频道拒绝。"""
    if channel not in PRIVATE_CHANNELS:
        raise ValueError(f"非私有频道: {channel.value}")
    payload: dict[str, str] = {"command": command, "channel": channel.value}
    if option is not None:
        payload["option"] = option
    return json.dumps(payload)


def replay_commands(subscriptions: Mapping[WsChannel, str | None]) -> tuple[str, ...]:
    """重连后需重放的订阅命令，按记录顺序。"""
    return tuple(
        build_private_command("subscribe", channel, option)
        for channel, option in subscriptions.items()
    )


class PrivateWsClient:
    """私有事件 WS 客户端。

    连接循环保持薄，解析与命令构造均在模块级纯函数（C-02）。
    run 负责连接与重连并把已解析事件投入队列，events 负责消费，
    两者需并发运行。
    """

    def __init__(
        self,
        token: str,
        *,
        sender: Sender | None = None,
        pacer: CommandPacer | None = None,
    ) -> None:
        """token 由 create_ws_token 取得；pacer 缺省用进程共享节奏器（R-04）。"""
        self._url = private_ws_url(token)
        self._sender = sender
        self._pacer = pacer if pacer is not None else SHARED_PACER
        self._subscriptions: dict[WsChannel, str | None] = {}
        self._connected_once = False
        self._queue: asyncio.Queue[PrivateEvent] = asyncio.Queue()

    @property
    def subscriptions(self) -> frozenset[WsChannel]:
        """已订阅频道集合。"""
        return frozenset(self._subscriptions)

    async def subscribe(self, channel: WsChannel, option: str | None = None) -> None:
        """订阅私有频道并记入已订阅集合。

        频道非四个私有频道之一时抛 ValueError。
        未连接时只记录，待 run 连接后重放。
        """
        command = build_private_command("subscribe", channel, option)
        self._subscriptions[channel] = option
        await self._send_command(command)

    async def unsubscribe(self, channel: WsChannel) -> None:
        """退订私有频道。服务端对退订无响应体。"""
        command = build_private_command("unsubscribe", channel)
        self._subscriptions.pop(channel, None)
        await self._send_command(command)

    async def events(self) -> AsyncIterator[PrivateEvent]:
        """产出已解析的私有事件，需与 run 并发消费。"""
        while True:
            yield await self._queue.get()

    async def run(
        self, on_reconnect: Callable[[], Awaitable[None]] | None = None
    ) -> None:
        """连接、订阅、接收，断线按退避重连。

        重连成功后先执行 on_reconnect，由上层以 REST 全量快照对账
        （C-10、R-08），随后重放订阅；不得假设增量连续。
        """
        attempt = 0
        while True:
            try:
                await self._session(on_reconnect)
            except (WebSocketException, OSError):
                attempt += 1
            else:
                attempt = 0
            await asyncio.sleep(reconnect_delay_seconds(attempt))

    async def _session(
        self, on_reconnect: Callable[[], Awaitable[None]] | None
    ) -> None:
        """建立一次连接并接收，直到断开。"""
        async with websockets.connect(self._url) as connection:
            # 服务端 ping，库自动回 pong
            self._sender = text_sender(connection)
            reconnected = self._connected_once
            self._connected_once = True
            try:
                if reconnected and on_reconnect is not None:
                    await on_reconnect()
                await self._replay_subscriptions()
                async for raw in connection:
                    await self._queue.put(parse_private_message(to_text(raw)))
            finally:
                self._sender = None

    async def _replay_subscriptions(self) -> None:
        """重连后重放全部已记录订阅。"""
        for command in replay_commands(dict(self._subscriptions)):
            await self._send_command(command)

    async def _send_command(self, command: str) -> None:
        """经共享节奏器控制间隔后发送；未连接时不发送。"""
        sender = self._sender
        if sender is None:
            return
        await self._pacer.wait_turn()
        await sender(command)
