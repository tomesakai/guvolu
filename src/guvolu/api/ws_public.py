"""公开 WebSocket 客户端：行情频道订阅与帧解析。

设计遵循 C-02：报文解析写成纯函数，可在无网络下单测；
连接循环只做 IO，不含业务判断。
订阅与退订限速每秒一次（R-04）。
订阅后的错误帧以消息返回且不断连，必须解析并抛出（C-09）。
重连后由上层以 REST 全量快照对账（C-10、R-08）。
"""
from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
import websockets
from websockets.exceptions import WebSocketException

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
from guvolu.domain.models import Orderbook, PublicTrade, Raw, Ticker
from guvolu.domain.symbols import Symbol

PUBLIC_WS_URL = "wss://api.coin.z.com/ws/public/v1"

# 逐笔成交频道可选值
TAKER_ONLY = "TAKER_ONLY"

PUBLIC_CHANNELS: frozenset[WsChannel] = frozenset(
    {WsChannel.TICKER, WsChannel.ORDERBOOKS, WsChannel.TRADES}
)

PublicMessage = Ticker | Orderbook | PublicTrade

_PUBLIC_PARSERS: Mapping[str, Callable[[Raw], PublicMessage]] = {
    WsChannel.TICKER.value: Ticker.from_api,
    WsChannel.ORDERBOOKS.value: Orderbook.from_api,
    WsChannel.TRADES.value: PublicTrade.from_api,
}


def parse_public_message(text: str) -> PublicMessage:
    """解析公开行情帧为领域模型。

    纯函数，不涉及 IO（C-02）。金额字段经 from_api 转 Decimal（T-08）。
    """
    payload = decode_frame(text)
    channel = str(payload.get("channel", ""))
    parser = _PUBLIC_PARSERS.get(channel)
    if parser is None:
        raise WsError(f"未知公开频道: {channel}")
    return parser(payload)


@dataclass(frozen=True, slots=True)
class PublicSubscription:
    """公开订阅项：频道、品种与可选项。"""

    channel: WsChannel
    symbol: Symbol
    option: str | None = None


def build_public_command(command: WsCommand, subscription: PublicSubscription) -> str:
    """构造公开频道命令报文，非公开频道拒绝。"""
    if subscription.channel not in PUBLIC_CHANNELS:
        raise ValueError(f"非公开频道: {subscription.channel.value}")
    payload: dict[str, str] = {
        "command": command,
        "channel": subscription.channel.value,
        "symbol": str(subscription.symbol),
    }
    if subscription.option is not None:
        payload["option"] = subscription.option
    return json.dumps(payload)


def replay_commands(subscriptions: Sequence[PublicSubscription]) -> tuple[str, ...]:
    """重连后需重放的订阅命令，按记录顺序。"""
    return tuple(build_public_command("subscribe", item) for item in subscriptions)


class PublicWsClient:
    """公开行情 WS 客户端。

    连接循环保持薄，解析与命令构造均在模块级纯函数（C-02）。
    run 负责连接与重连并把已解析消息投入队列，events 负责消费，
    两者需并发运行。
    """

    def __init__(
        self,
        *,
        url: str = PUBLIC_WS_URL,
        sender: Sender | None = None,
        pacer: CommandPacer | None = None,
    ) -> None:
        """sender 为发送函数注入点；pacer 缺省用进程共享节奏器（R-04）。"""
        self._url = url
        self._sender = sender
        self._pacer = pacer if pacer is not None else SHARED_PACER
        self._subscriptions: dict[tuple[WsChannel, str], PublicSubscription] = {}
        self._connected_once = False
        self._queue: asyncio.Queue[PublicMessage] = asyncio.Queue()

    @property
    def subscriptions(self) -> tuple[PublicSubscription, ...]:
        """已记录的订阅对，按加入顺序。"""
        return tuple(self._subscriptions.values())

    async def subscribe(
        self, channel: WsChannel, symbol: Symbol, option: str | None = None
    ) -> None:
        """订阅公开频道并记录订阅对。

        频道非法时抛 ValueError。未连接时只记录，待 run 连接后重放。
        """
        item = PublicSubscription(channel=channel, symbol=symbol, option=option)
        command = build_public_command("subscribe", item)
        self._subscriptions[(channel, str(symbol))] = item
        await self._send_command(command)

    async def unsubscribe(self, channel: WsChannel, symbol: Symbol) -> None:
        """退订公开频道。服务端对退订无响应体。"""
        item = PublicSubscription(channel=channel, symbol=symbol)
        command = build_public_command("unsubscribe", item)
        self._subscriptions.pop((channel, str(symbol)), None)
        await self._send_command(command)

    async def events(self) -> AsyncIterator[PublicMessage]:
        """产出已解析的行情消息，需与 run 并发消费。"""
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
                    await self._queue.put(parse_public_message(to_text(raw)))
            finally:
                self._sender = None

    async def _replay_subscriptions(self) -> None:
        """重连后重放全部已记录订阅。"""
        for command in replay_commands(self.subscriptions):
            await self._send_command(command)

    async def _send_command(self, command: str) -> None:
        """经共享节奏器控制间隔后发送；未连接时不发送。"""
        sender = self._sender
        if sender is None:
            return
        await self._pacer.wait_turn()
        await sender(command)
