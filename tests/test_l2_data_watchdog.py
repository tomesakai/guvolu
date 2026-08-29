"""L2 数据级静默看门狗单测：控制帧不得续期。全程离线（C-13）。"""
from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Sequence
from pathlib import Path

import pytest
import websockets

from guvolu.data import l2_capture
from guvolu.data.segmented_raw import SegmentedRawWriter


class _StopRecorder(RuntimeError):
    """终止测试用的外部异常；生产重连捕获不应吞掉它。"""


class _Connection:
    def __init__(
        self, frames: Sequence[str | tuple[float, str]], *, silent: bool = False,
    ) -> None:
        self._frames = iter(frames)
        self._silent = silent
        self.sent: list[str] = []

    async def send(self, message: str) -> None:
        self.sent.append(message)

    async def recv(self) -> str:
        try:
            item = next(self._frames)
        except StopIteration:
            if self._silent:
                await asyncio.Event().wait()
            raise _StopRecorder from None
        if isinstance(item, tuple):
            delay, frame = item
            await asyncio.sleep(delay)
            return frame
        return item


class _ConnectionContext:
    def __init__(self, connection: _Connection) -> None:
        self._connection = connection

    async def __aenter__(self) -> _Connection:
        return self._connection

    async def __aexit__(self, *args: object) -> None:
        return None


def _writer(
    root: Path, venue_id: str, symbol: str, run_id: str,
) -> SegmentedRawWriter:
    endpoint_id, endpoint_revision = l2_capture.ENDPOINT_BINDINGS[venue_id]
    return SegmentedRawWriter(
        root, venue_id, symbol, run_id=run_id,
        endpoint_id=endpoint_id, endpoint_revision=endpoint_revision,
        segment_seconds=3600, segment_max_bytes=1024 * 1024,
    )


def _install_connect(
    monkeypatch: pytest.MonkeyPatch, connection: _Connection,
) -> None:
    connect_calls = 0

    def connect(_: str, **kwargs: object) -> _ConnectionContext:
        nonlocal connect_calls
        del kwargs
        connect_calls += 1
        if connect_calls > 1:
            raise _StopRecorder
        return _ConnectionContext(connection)

    monkeypatch.setattr(websockets, "connect", connect)
    monkeypatch.setattr(l2_capture, "reconnect_delay_seconds", lambda _: 0.0)
    monkeypatch.setattr(l2_capture, "DATA_SILENCE_TIMEOUT_SECONDS", 0.3)


_BITBANK_DEPTH = "42" + json.dumps(
    ["message", {"room_name": "depth_diff_btc_jpy"}],
    separators=(",", ":"),
)
_GMO_BOOK = json.dumps({"channel": "orderbooks", "symbol": "BTC"})
_GMO_CONTROL = json.dumps({"channel": "ticker", "symbol": "BTC"})
_BITFLYER_BOARD = json.dumps({
    "method": "channelMessage",
    "params": {"channel": "lightning_board_BTC_JPY"},
})
_BITFLYER_CONTROL = json.dumps({"jsonrpc": "2.0", "id": 1, "result": True})


def test_bitbank_l2_heartbeats_do_not_extend_data_watchdog(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Engine.IO 心跳照答 pong，但数据静默仍触发重连。"""
    heartbeats = [(0.02, "2") for _ in range(100)]
    connection = _Connection(["0", "40", *heartbeats])
    _install_connect(monkeypatch, connection)
    writer = _writer(tmp_path, "bitbank", "btc_jpy", "run-l2-bitbank-hb")
    stats = l2_capture.CaptureStats("bitbank", "btc_jpy")
    started = time.monotonic()

    with pytest.raises(_StopRecorder):
        asyncio.run(l2_capture._record_bitbank(writer, stats, None))
    writer.finish(status="interrupted")

    assert time.monotonic() - started < 5.0
    assert stats.data_frames == 0
    assert stats.disconnects == stats.reconnects == 1
    assert stats.consecutive_failures == 1
    pongs = [item for item in connection.sent if item == "3"]
    assert 2 <= len(pongs) < len(heartbeats)


def test_bitbank_l2_depth_frames_extend_data_watchdog(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """真实盘口数据帧续期看门狗，跨越预算也不误重连。"""
    depth_frames = [(0.15, _BITBANK_DEPTH) for _ in range(4)]
    connection = _Connection(["0", "40", *depth_frames])
    _install_connect(monkeypatch, connection)
    writer = _writer(tmp_path, "bitbank", "btc_jpy", "run-l2-bitbank-depth")
    stats = l2_capture.CaptureStats("bitbank", "btc_jpy")

    with pytest.raises(_StopRecorder):
        asyncio.run(l2_capture._record_bitbank(writer, stats, None))
    writer.finish(status="interrupted")

    assert stats.data_frames == 4
    assert stats.disconnects == stats.reconnects == 0


def test_gmo_l2_control_frames_do_not_extend_data_watchdog(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """非盘口频道帧只计控制帧，数据静默仍触发重连。"""
    frames = [(0.02, _GMO_CONTROL) for _ in range(100)]
    connection = _Connection(frames)
    _install_connect(monkeypatch, connection)
    writer = _writer(tmp_path, "gmo", "BTC", "run-l2-gmo-control")
    stats = l2_capture.CaptureStats("gmo", "BTC")
    started = time.monotonic()

    with pytest.raises(_StopRecorder):
        asyncio.run(l2_capture._record_gmo(writer, stats, None))
    writer.finish(status="interrupted")

    assert time.monotonic() - started < 5.0
    assert stats.data_frames == 0
    assert stats.control_frames >= 2
    assert stats.disconnects == stats.reconnects == 1


def test_gmo_l2_book_frames_extend_data_watchdog(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """盘口数据帧续期看门狗，跨越预算也不误重连。"""
    frames = [(0.15, _GMO_BOOK) for _ in range(4)]
    connection = _Connection(frames)
    _install_connect(monkeypatch, connection)
    writer = _writer(tmp_path, "gmo", "BTC", "run-l2-gmo-book")
    stats = l2_capture.CaptureStats("gmo", "BTC")

    with pytest.raises(_StopRecorder):
        asyncio.run(l2_capture._record_gmo(writer, stats, None))
    writer.finish(status="interrupted")

    assert stats.data_frames == 4
    assert stats.disconnects == stats.reconnects == 0


def test_bitflyer_l2_control_frames_do_not_extend_data_watchdog(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """订阅确认等控制帧不得续期，数据静默仍触发重连。"""
    frames = [(0.02, _BITFLYER_CONTROL) for _ in range(100)]
    connection = _Connection(frames)
    _install_connect(monkeypatch, connection)
    writer = _writer(tmp_path, "bitflyer", "BTC_JPY", "run-l2-bitflyer-ctrl")
    stats = l2_capture.CaptureStats("bitflyer", "BTC_JPY")
    started = time.monotonic()

    with pytest.raises(_StopRecorder):
        asyncio.run(l2_capture._record_bitflyer(writer, stats, None))
    writer.finish(status="interrupted")

    assert time.monotonic() - started < 5.0
    assert stats.data_frames == 0
    assert stats.disconnects == stats.reconnects == 1


def test_bitflyer_l2_board_frames_extend_data_watchdog(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """盘口数据帧续期看门狗，跨越预算也不误重连。"""
    frames = [(0.15, _BITFLYER_BOARD) for _ in range(4)]
    connection = _Connection(frames)
    _install_connect(monkeypatch, connection)
    writer = _writer(tmp_path, "bitflyer", "BTC_JPY", "run-l2-bitflyer-board")
    stats = l2_capture.CaptureStats("bitflyer", "BTC_JPY")

    with pytest.raises(_StopRecorder):
        asyncio.run(l2_capture._record_bitflyer(writer, stats, None))
    writer.finish(status="interrupted")

    assert stats.data_frames == 4
    assert stats.disconnects == stats.reconnects == 0
