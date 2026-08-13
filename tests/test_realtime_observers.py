"""日元旁路实时采集的协议报文单测。"""
import asyncio

from guvolu.data.raw_writer import RawWriter
from guvolu.venues.bitbank_stream import _handshake, join_packet, public_rooms
from guvolu.venues.coincheck import public_channels, subscribe_message


class FakeConnection:
    """最小 Socket.IO 握手替身。"""

    def __init__(self) -> None:
        self.received = ["0{\"sid\":\"x\"}", "40{\"sid\":\"x\"}"]
        self.sent: list[str] = []

    async def recv(self) -> str:
        """返回下一条协议帧。"""
        return self.received.pop(0)

    async def send(self, message: str) -> None:
        """记录发送帧。"""
        self.sent.append(message)


def test_coincheck_subscriptions_are_single_channel() -> None:
    """Coincheck 每个频道单独订阅。"""
    assert public_channels(["btc_jpy"]) == [
        "btc_jpy-trades", "btc_jpy-orderbook"
    ]
    assert subscribe_message("btc_jpy-trades") == (
        '{"type":"subscribe","channel":"btc_jpy-trades"}'
    )


def test_bitbank_socketio_handshake_and_rooms(tmp_path) -> None:
    """bitbank 使用 Engine.IO 4.x 再加入公开数据房间。"""
    connection = FakeConnection()
    writer = RawWriter(tmp_path, run_id="bitbanktest")
    asyncio.run(_handshake(connection, writer))
    assert connection.sent == ["40"]
    assert public_rooms(["btc_jpy"]) == [
        "transactions_btc_jpy", "depth_whole_btc_jpy", "depth_diff_btc_jpy",
        "circuit_break_info_btc_jpy",
    ]
    assert join_packet("transactions_btc_jpy") == (
        '42["join-room","transactions_btc_jpy"]'
    )
