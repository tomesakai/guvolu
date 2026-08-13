"""公开 WS 单测：帧解析、命令构造、限速与退避。

全部离线，绝不建立连接（C-13）。
"""
import asyncio
import json
from decimal import Decimal

import pytest

from guvolu.api.ws_public import (
    PUBLIC_WS_URL,
    RECONNECT_BASE_SECONDS,
    RECONNECT_MAX_SECONDS,
    SUBSCRIBE_INTERVAL_SECONDS,
    TAKER_ONLY,
    PublicSubscription,
    PublicWsClient,
    build_public_command,
    command_wait_seconds,
    parse_public_message,
    reconnect_delay_seconds,
    replay_commands,
)
from guvolu.api.ws_common import CommandPacer
from guvolu.domain.enums import Side, WsChannel
from guvolu.domain.errors import WsError
from guvolu.domain.models import Orderbook, PublicTrade, Ticker
from guvolu.domain.symbols import SpotSymbol

TICKER_FRAME = json.dumps(
    {
        "channel": "ticker",
        "ask": "750760",
        "bid": "750600",
        "high": "762302",
        "last": "756662",
        "low": "704874",
        "symbol": "BTC",
        "timestamp": "2018-03-30T12:34:56.789Z",
        "volume": "194785.8484",
    }
)

ORDERBOOKS_FRAME = json.dumps(
    {
        "channel": "orderbooks",
        "asks": [{"price": "455659", "size": "0.1"}],
        "bids": [{"price": "455659", "size": "0.3"}],
        "symbol": "BTC",
    }
)

TRADES_FRAME = json.dumps(
    {
        "channel": "trades",
        "price": "750760",
        "side": "BUY",
        "size": "0.1",
        "timestamp": "2018-03-30T12:34:56.789Z",
        "symbol": "BTC",
    }
)

ERROR_FRAME = json.dumps({"error": "ERR-5012 Invalid permissions for action"})


class _Collector:
    """收集报文的假发送函数，不触网（C-13）。"""

    def __init__(self) -> None:
        self.sent: list[str] = []

    async def __call__(self, text: str) -> None:
        self.sent.append(text)


def test_parse_ticker_frame() -> None:
    """ticker 帧解析为 Decimal 与带时区时刻（T-08）。"""
    message = parse_public_message(TICKER_FRAME)
    assert isinstance(message, Ticker)
    assert message.ask == Decimal("750760")
    assert isinstance(message.volume, Decimal)
    assert message.symbol == "BTC"
    assert message.timestamp.tzinfo is not None


def test_parse_orderbooks_frame() -> None:
    """orderbooks 帧解析为盘口档位。"""
    message = parse_public_message(ORDERBOOKS_FRAME)
    assert isinstance(message, Orderbook)
    assert message.asks[0].price == Decimal("455659")
    assert message.bids[0].size == Decimal("0.3")
    assert isinstance(message.asks[0].size, Decimal)


def test_parse_trades_frame() -> None:
    """trades 帧解析为逐笔成交，方向为枚举。"""
    message = parse_public_message(TRADES_FRAME)
    assert isinstance(message, PublicTrade)
    assert message.side is Side.BUY
    assert message.price == Decimal("750760")
    assert isinstance(message.size, Decimal)
    assert message.timestamp.tzinfo is not None


def test_parse_error_frame_raises() -> None:
    """错误帧抛 WsError，不静默丢弃（C-09）。"""
    with pytest.raises(WsError):
        parse_public_message(ERROR_FRAME)


def test_parse_unknown_channel_raises() -> None:
    """未知频道抛 WsError。"""
    with pytest.raises(WsError):
        parse_public_message(json.dumps({"channel": "orderEvents"}))


def test_parse_non_json_raises() -> None:
    """非 JSON 帧抛 WsError。"""
    with pytest.raises(WsError):
        parse_public_message("not json")


def test_build_subscribe_command_shape() -> None:
    """订阅报文形状符合官方约定。"""
    item = PublicSubscription(channel=WsChannel.TICKER, symbol=SpotSymbol("BTC"))
    assert json.loads(build_public_command("subscribe", item)) == {
        "command": "subscribe",
        "channel": "ticker",
        "symbol": "BTC",
    }


def test_build_command_with_option() -> None:
    """逐笔成交频道可携带可选项。"""
    item = PublicSubscription(
        channel=WsChannel.TRADES, symbol=SpotSymbol("BTC"), option=TAKER_ONLY
    )
    assert json.loads(build_public_command("subscribe", item)) == {
        "command": "subscribe",
        "channel": "trades",
        "symbol": "BTC",
        "option": "TAKER_ONLY",
    }


def test_build_unsubscribe_command() -> None:
    """退订报文形状正确。"""
    item = PublicSubscription(channel=WsChannel.ORDERBOOKS, symbol=SpotSymbol("BTC"))
    payload = json.loads(build_public_command("unsubscribe", item))
    assert payload["command"] == "unsubscribe"
    assert payload["channel"] == "orderbooks"


def test_build_command_rejects_private_channel() -> None:
    """公开客户端拒绝私有频道。"""
    item = PublicSubscription(
        channel=WsChannel.ORDER_EVENTS, symbol=SpotSymbol("BTC")
    )
    with pytest.raises(ValueError):
        build_public_command("subscribe", item)


def test_subscribe_rejects_private_channel() -> None:
    """订阅私有频道抛 ValueError 且不记录。"""
    client = PublicWsClient(pacer=CommandPacer())

    async def scenario() -> None:
        with pytest.raises(ValueError):
            await client.subscribe(WsChannel.POSITION_EVENTS, SpotSymbol("BTC"))

    asyncio.run(scenario())
    assert client.subscriptions == ()


def test_subscribe_records_and_sends() -> None:
    """订阅记录订阅对并发送报文。"""
    collector = _Collector()
    client = PublicWsClient(sender=collector, pacer=CommandPacer())

    async def scenario() -> None:
        await client.subscribe(WsChannel.TICKER, SpotSymbol("BTC"))

    asyncio.run(scenario())
    assert client.subscriptions == (
        PublicSubscription(channel=WsChannel.TICKER, symbol=SpotSymbol("BTC")),
    )
    assert json.loads(collector.sent[0])["channel"] == "ticker"


def test_subscribe_without_connection_only_records() -> None:
    """未连接时只记录订阅，待重连重放。"""
    client = PublicWsClient(pacer=CommandPacer())

    async def scenario() -> None:
        await client.subscribe(WsChannel.TRADES, SpotSymbol("BTC"), TAKER_ONLY)

    asyncio.run(scenario())
    assert len(client.subscriptions) == 1
    assert client.subscriptions[0].option == TAKER_ONLY


def test_unsubscribe_removes_record() -> None:
    """退订后订阅记录被移除。"""
    collector = _Collector()
    client = PublicWsClient(sender=collector, pacer=CommandPacer())

    async def scenario() -> None:
        await client.subscribe(WsChannel.TICKER, SpotSymbol("BTC"))
        await client.unsubscribe(WsChannel.TICKER, SpotSymbol("BTC"))

    asyncio.run(scenario())
    assert client.subscriptions == ()
    assert json.loads(collector.sent[1])["command"] == "unsubscribe"


def test_replay_commands_cover_all_subscriptions() -> None:
    """重连重放覆盖全部订阅（C-10）。"""
    items = (
        PublicSubscription(channel=WsChannel.TICKER, symbol=SpotSymbol("BTC")),
        PublicSubscription(
            channel=WsChannel.TRADES, symbol=SpotSymbol("BTC"), option=TAKER_ONLY
        ),
    )
    commands = [json.loads(text) for text in replay_commands(items)]
    assert [item["channel"] for item in commands] == ["ticker", "trades"]
    assert commands[1]["option"] == TAKER_ONLY


def test_command_wait_seconds() -> None:
    """命令间隔计算（R-04）。"""
    assert command_wait_seconds(None, 100.0) == 0.0
    assert command_wait_seconds(100.0, 100.0) == pytest.approx(
        SUBSCRIBE_INTERVAL_SECONDS
    )
    assert command_wait_seconds(100.0, 200.0) == 0.0


def test_subscribe_interval_enforced(monkeypatch: pytest.MonkeyPatch) -> None:
    """连续订阅之间插入等待（R-04）。"""
    waits: list[float] = []

    async def fake_sleep(delay: float) -> None:
        waits.append(delay)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    collector = _Collector()
    client = PublicWsClient(sender=collector, pacer=CommandPacer())

    async def scenario() -> None:
        await client.subscribe(WsChannel.TICKER, SpotSymbol("BTC"))
        await client.subscribe(WsChannel.ORDERBOOKS, SpotSymbol("BTC"))
        await client.subscribe(WsChannel.TRADES, SpotSymbol("BTC"))

    asyncio.run(scenario())
    assert len(collector.sent) == 3
    assert len(waits) == 2
    for value in waits:
        assert value == pytest.approx(SUBSCRIBE_INTERVAL_SECONDS, abs=0.05)


def test_reconnect_delay_growth() -> None:
    """重连退避指数增长且封顶。"""
    assert reconnect_delay_seconds(0) == RECONNECT_BASE_SECONDS
    assert reconnect_delay_seconds(1) == RECONNECT_BASE_SECONDS * 2
    assert reconnect_delay_seconds(3) == RECONNECT_BASE_SECONDS * 8
    assert reconnect_delay_seconds(99) == RECONNECT_MAX_SECONDS


def test_public_url_constant() -> None:
    """公开地址含 /v1 版本段。"""
    assert PUBLIC_WS_URL == "wss://api.coin.z.com/ws/public/v1"
