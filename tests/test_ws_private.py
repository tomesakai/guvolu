"""私有 WS 单测：令牌管理、事件帧解析、频道校验与限速。

全部离线，绝不建立连接、绝不打真实端点（C-13、C-14）。
"""
import asyncio
import json
import logging
from collections.abc import Mapping
from decimal import Decimal
from pathlib import Path

import pytest
import websockets

from guvolu.api.transport import HttpMethod, Params, PrivateTransport, RateLimiter
from guvolu.api.ws_private import (
    PERIODIC,
    RECONNECT_BASE_SECONDS,
    RECONNECT_MAX_SECONDS,
    SUBSCRIBE_INTERVAL_SECONDS,
    WS_AUTH_PATH,
    PrivateWsClient,
    build_private_command,
    command_wait_seconds,
    create_ws_token,
    extend_ws_token,
    keepalive,
    parse_private_message,
    private_ws_url,
    reconnect_delay_seconds,
    replay_commands,
    revoke_ws_token,
)
from guvolu.api.ws_common import CommandPacer
from guvolu.domain.enums import (
    ExecutionType,
    OrderStatus,
    SettleType,
    Side,
    TimeInForce,
    WsChannel,
)
from guvolu.domain.errors import WsError
from guvolu.domain.models import (
    WsExecutionEvent,
    WsOrderEvent,
    WsPositionEvent,
    WsPositionSummaryEvent,
)

ORDER_EVENT_FRAME = json.dumps(
    {
        "channel": "orderEvents",
        "orderId": 123456789,
        "symbol": "BTC",
        "settleType": "OPEN",
        "executionType": "LIMIT",
        "side": "BUY",
        "orderStatus": "ORDERED",
        "orderTimestamp": "2019-10-24T15:22:06.665Z",
        "orderPrice": "876045",
        "orderSize": "0.9761",
        "orderExecutedSize": "0",
        "losscutPrice": "0",
        "timeInForce": "FAS",
        "msgType": "NOR",
    }
)

EXECUTION_EVENT_FRAME = json.dumps(
    {
        "channel": "executionEvents",
        "orderId": 123456789,
        "executionId": 72123911,
        "symbol": "BTC",
        "settleType": "OPEN",
        "executionType": "LIMIT",
        "side": "BUY",
        "executionPrice": "877404",
        "executionSize": "0.5",
        "positionId": 1234567,
        "orderTimestamp": "2019-10-24T15:22:06.665Z",
        "executionTimestamp": "2019-10-24T15:22:06.687Z",
        "lossGain": "0",
        "fee": "323",
        "orderPrice": "877200",
        "orderSize": "0.7361",
        "orderExecutedSize": "0.5",
        "timeInForce": "FAS",
        "msgType": "ER",
    }
)

POSITION_EVENT_FRAME = json.dumps(
    {
        "channel": "positionEvents",
        "positionId": 1234567,
        "symbol": "BTC_JPY",
        "side": "BUY",
        "size": "0.22",
        "orderdSize": "0",
        "price": "876045",
        "lossGain": "14",
        "leverage": "4",
        "losscutPrice": "766540",
        "timestamp": "2019-10-24T15:22:06.665Z",
        "msgType": "OPR",
    }
)

POSITION_SUMMARY_EVENT_FRAME = json.dumps(
    {
        "channel": "positionSummaryEvents",
        "symbol": "BTC_JPY",
        "side": "BUY",
        "averagePositionRate": "715656",
        "positionLossGain": "250675",
        "sumOrderQuantity": "2",
        "sumPositionQuantity": "11.6999",
        "timestamp": "2019-11-24T20:14:41.773Z",
        "msgType": "INIT",
    }
)

ERROR_FRAME = json.dumps({"error": "ERR-5012 Invalid permissions for action"})

TOKEN = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"


class FakePrivateTransport(PrivateTransport):
    """离线传输替身，记录调用而不发请求（C-13）。"""

    def __init__(self, log_dir: Path, data: object) -> None:
        super().__init__("k", "s", RateLimiter(1000.0), log_dir)
        self.calls: list[tuple[str, str, Mapping[str, object] | None]] = []
        self._data = data

    def request(
        self,
        method: HttpMethod,
        path: str,
        *,
        params: Params | None = None,
        body: Mapping[str, object] | None = None,
    ) -> object:
        self.calls.append((method, path, body))
        return self._data


class _Collector:
    """收集报文的假发送函数，不触网（C-13）。"""

    def __init__(self) -> None:
        self.sent: list[str] = []

    async def __call__(self, text: str) -> None:
        self.sent.append(text)


class _StopKeepalive(Exception):
    """用于结束保活循环的测试信号。"""


def test_parse_order_event_frame() -> None:
    """委托事件解析为 Decimal 与枚举（T-08、U-01）。"""
    event = parse_private_message(ORDER_EVENT_FRAME)
    assert isinstance(event, WsOrderEvent)
    assert event.order_id == 123456789
    assert event.order_price == Decimal("876045")
    assert isinstance(event.order_size, Decimal)
    assert event.side is Side.BUY
    assert event.order_status is OrderStatus.ORDERED
    assert event.settle_type is SettleType.OPEN
    assert event.execution_type is ExecutionType.LIMIT
    assert event.time_in_force is TimeInForce.FAS
    assert event.order_timestamp.tzinfo is not None


def test_parse_execution_event_frame() -> None:
    """成交事件解析，与委托事件语义不混（U-01）。"""
    event = parse_private_message(EXECUTION_EVENT_FRAME)
    assert isinstance(event, WsExecutionEvent)
    assert event.execution_id == 72123911
    assert event.execution_price == Decimal("877404")
    assert event.fee == Decimal("323")
    assert isinstance(event.loss_gain, Decimal)
    assert event.position_id == 1234567
    assert event.execution_timestamp.tzinfo is not None


def test_parse_position_event_frame() -> None:
    """持仓事件解析，字段 orderdSize 为官方拼写。"""
    event = parse_private_message(POSITION_EVENT_FRAME)
    assert isinstance(event, WsPositionEvent)
    assert event.position_id == 1234567
    assert event.ordered_size == Decimal("0")
    assert event.leverage == Decimal("4")
    assert isinstance(event.losscut_price, Decimal)
    assert event.msg_type == "OPR"
    assert event.timestamp.tzinfo is not None


def test_parse_position_summary_event_frame() -> None:
    """持仓汇总事件解析。"""
    event = parse_private_message(POSITION_SUMMARY_EVENT_FRAME)
    assert isinstance(event, WsPositionSummaryEvent)
    assert event.average_position_rate == Decimal("715656")
    assert event.sum_position_quantity == Decimal("11.6999")
    assert isinstance(event.position_loss_gain, Decimal)
    assert event.side is Side.BUY
    assert event.timestamp.tzinfo is not None


def test_parse_error_frame_raises() -> None:
    """权限错误帧抛 WsError，且连接不被误判为正常（C-09）。"""
    with pytest.raises(WsError):
        parse_private_message(ERROR_FRAME)


def test_parse_unknown_channel_raises() -> None:
    """未知频道抛 WsError。"""
    with pytest.raises(WsError):
        parse_private_message(json.dumps({"channel": "ticker"}))


def test_parse_non_json_raises() -> None:
    """非 JSON 帧抛 WsError。"""
    with pytest.raises(WsError):
        parse_private_message("<html>")


def test_create_ws_token(tmp_path: Path) -> None:
    """令牌签发使用 POST 与空 body。"""
    transport = FakePrivateTransport(tmp_path, TOKEN)
    assert create_ws_token(transport) == TOKEN
    assert transport.calls == [("POST", WS_AUTH_PATH, {})]


def test_create_ws_token_rejects_non_string(tmp_path: Path) -> None:
    """令牌形态非字符串时抛 WsError。"""
    transport = FakePrivateTransport(tmp_path, {"token": TOKEN})
    with pytest.raises(WsError):
        create_ws_token(transport)


def test_extend_ws_token(tmp_path: Path) -> None:
    """令牌延长使用 PUT 与 token body。"""
    transport = FakePrivateTransport(tmp_path, None)
    extend_ws_token(transport, TOKEN)
    assert transport.calls == [("PUT", WS_AUTH_PATH, {"token": TOKEN})]


def test_revoke_ws_token(tmp_path: Path) -> None:
    """令牌撤销使用 DELETE 与 token body。"""
    transport = FakePrivateTransport(tmp_path, None)
    revoke_ws_token(transport, TOKEN)
    assert transport.calls == [("DELETE", WS_AUTH_PATH, {"token": TOKEN})]


def test_private_ws_url_keeps_version_segment() -> None:
    """私有地址必须含 /v1，否则得 404。"""
    assert private_ws_url(TOKEN) == f"wss://api.coin.z.com/ws/private/v1/{TOKEN}"


def test_build_subscribe_command_shape() -> None:
    """订阅报文形状符合官方约定。"""
    payload = json.loads(build_private_command("subscribe", WsChannel.ORDER_EVENTS))
    assert payload == {"command": "subscribe", "channel": "orderEvents"}


def test_build_command_with_periodic_option() -> None:
    """持仓汇总频道可携带周期推送可选项。"""
    payload = json.loads(
        build_private_command(
            "subscribe", WsChannel.POSITION_SUMMARY_EVENTS, PERIODIC
        )
    )
    assert payload["option"] == "PERIODIC"
    assert payload["channel"] == "positionSummaryEvents"


def test_build_unsubscribe_command() -> None:
    """退订报文形状正确。"""
    payload = json.loads(
        build_private_command("unsubscribe", WsChannel.EXECUTION_EVENTS)
    )
    assert payload["command"] == "unsubscribe"
    assert payload["channel"] == "executionEvents"


def test_build_command_rejects_public_channel() -> None:
    """私有客户端拒绝公开频道。"""
    with pytest.raises(ValueError):
        build_private_command("subscribe", WsChannel.TICKER)


def test_subscribe_rejects_public_channel() -> None:
    """订阅公开频道抛 ValueError 且不记录。"""
    client = PrivateWsClient(TOKEN, pacer=CommandPacer())

    async def scenario() -> None:
        with pytest.raises(ValueError):
            await client.subscribe(WsChannel.ORDERBOOKS)

    asyncio.run(scenario())
    assert client.subscriptions == frozenset()


def test_subscribe_records_and_sends() -> None:
    """订阅记入已订阅集合并发送报文。"""
    collector = _Collector()
    client = PrivateWsClient(TOKEN, sender=collector, pacer=CommandPacer())

    async def scenario() -> None:
        await client.subscribe(WsChannel.ORDER_EVENTS)

    asyncio.run(scenario())
    assert client.subscriptions == frozenset({WsChannel.ORDER_EVENTS})
    assert json.loads(collector.sent[0])["channel"] == "orderEvents"


def test_unsubscribe_removes_record() -> None:
    """退订后已订阅集合被移除。"""
    collector = _Collector()
    client = PrivateWsClient(TOKEN, sender=collector, pacer=CommandPacer())

    async def scenario() -> None:
        await client.subscribe(WsChannel.EXECUTION_EVENTS)
        await client.unsubscribe(WsChannel.EXECUTION_EVENTS)

    asyncio.run(scenario())
    assert client.subscriptions == frozenset()
    assert json.loads(collector.sent[1])["command"] == "unsubscribe"


def test_replay_commands_cover_all_subscriptions() -> None:
    """重连重放覆盖全部订阅并保留可选项（C-10）。"""
    commands = [
        json.loads(text)
        for text in replay_commands(
            {
                WsChannel.ORDER_EVENTS: None,
                WsChannel.POSITION_SUMMARY_EVENTS: PERIODIC,
            }
        )
    ]
    assert [item["channel"] for item in commands] == [
        "orderEvents",
        "positionSummaryEvents",
    ]
    assert commands[1]["option"] == PERIODIC


def test_command_wait_seconds() -> None:
    """命令间隔计算（R-04）。"""
    assert command_wait_seconds(None, 10.0) == 0.0
    assert command_wait_seconds(10.0, 10.0) == pytest.approx(
        SUBSCRIBE_INTERVAL_SECONDS
    )
    assert command_wait_seconds(10.0, 99.0) == 0.0


def test_subscribe_interval_enforced(monkeypatch: pytest.MonkeyPatch) -> None:
    """连续订阅之间插入等待（R-04）。"""
    waits: list[float] = []

    async def fake_sleep(delay: float) -> None:
        waits.append(delay)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    collector = _Collector()
    client = PrivateWsClient(TOKEN, sender=collector, pacer=CommandPacer())

    async def scenario() -> None:
        await client.subscribe(WsChannel.ORDER_EVENTS)
        await client.subscribe(WsChannel.EXECUTION_EVENTS)
        await client.subscribe(WsChannel.POSITION_EVENTS)

    asyncio.run(scenario())
    assert len(collector.sent) == 3
    assert len(waits) == 2
    for value in waits:
        assert value == pytest.approx(SUBSCRIBE_INTERVAL_SECONDS, abs=0.05)


def test_reconnect_delay_growth() -> None:
    """重连退避指数增长且封顶。"""
    assert reconnect_delay_seconds(0) == RECONNECT_BASE_SECONDS
    assert reconnect_delay_seconds(2) == RECONNECT_BASE_SECONDS * 4
    assert reconnect_delay_seconds(99) == RECONNECT_MAX_SECONDS


def test_keepalive_extends_periodically(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """保活按给定间隔反复延长令牌。"""
    waits: list[float] = []

    async def fake_sleep(delay: float) -> None:
        waits.append(delay)
        if len(waits) > 2:
            raise _StopKeepalive

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    transport = FakePrivateTransport(tmp_path, None)

    async def scenario() -> None:
        await keepalive(transport, TOKEN, 60.0)

    with pytest.raises(_StopKeepalive):
        asyncio.run(scenario())
    assert waits[:2] == [60.0, 60.0]
    assert transport.calls == [
        ("PUT", WS_AUTH_PATH, {"token": TOKEN}),
        ("PUT", WS_AUTH_PATH, {"token": TOKEN}),
    ]


def test_keepalive_rejects_non_positive_interval(tmp_path: Path) -> None:
    """保活间隔非正时抛 ValueError。"""
    transport = FakePrivateTransport(tmp_path, None)

    async def scenario() -> None:
        await keepalive(transport, TOKEN, 0.0)

    with pytest.raises(ValueError):
        asyncio.run(scenario())


class _FakeConnection:
    """离线连接替身：按脚本产出帧后结束会话（C-13）。"""

    def __init__(self, frames: list[str]) -> None:
        self._frames = iter(frames)
        self.sent: list[str] = []

    async def send(self, text: str) -> None:
        self.sent.append(text)

    def __aiter__(self) -> "_FakeConnection":
        return self

    async def __anext__(self) -> str:
        try:
            return next(self._frames)
        except StopIteration:
            raise StopAsyncIteration from None


class _FakeConnect:
    """离线连接上下文替身，不触网（C-13）。"""

    def __init__(self, connection: _FakeConnection) -> None:
        self._connection = connection

    async def __aenter__(self) -> _FakeConnection:
        return self._connection

    async def __aexit__(self, *args: object) -> None:
        return None


class _StopRun(Exception):
    """终止 run 循环的测试信号。"""


def test_run_reraises_permission_error_frame(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ERR-5012 权限错误帧重连无用，必须上抛调用方（C-09）。"""
    connection = _FakeConnection([ERROR_FRAME])
    monkeypatch.setattr(
        websockets, "connect", lambda _: _FakeConnect(connection)
    )
    client = PrivateWsClient(TOKEN, pacer=CommandPacer())

    with pytest.raises(WsError) as caught:
        asyncio.run(client.run())
    assert caught.value.code == "ERR-5012"


def test_run_reconnects_after_non_permission_ws_error(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
) -> None:
    """未知频道等非权限 WsError 记录后按退避重连。"""
    connection = _FakeConnection([json.dumps({"channel": "ticker"})])
    connect_calls = 0

    def connect(_: str) -> _FakeConnect:
        nonlocal connect_calls
        connect_calls += 1
        if connect_calls > 1:
            raise _StopRun
        return _FakeConnect(connection)

    delays: list[float] = []

    async def fake_sleep(delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr(websockets, "connect", connect)
    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    client = PrivateWsClient(TOKEN, pacer=CommandPacer())

    with caplog.at_level(logging.WARNING, logger="guvolu.api.ws_private"):
        with pytest.raises(_StopRun):
            asyncio.run(client.run())

    assert connect_calls == 2
    assert delays == [reconnect_delay_seconds(1)]
    assert any("未知私有频道" in message for message in caplog.messages)
