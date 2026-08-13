from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest
import websockets

from guvolu.data import trade_capture
from guvolu.data.segmented_raw import SegmentedRawWriter


class _StopRecorder(RuntimeError):
    """终止测试用的外部异常；生产重连捕获不应吞掉它。"""


class _Connection:
    def __init__(self, frames: list[str], *, silent: bool = False) -> None:
        self._frames = iter(frames)
        self._silent = silent
        self.sent: list[str] = []

    async def send(self, message: str) -> None:
        self.sent.append(message)

    async def recv(self) -> str:
        if self._silent:
            await asyncio.Event().wait()
            raise AssertionError("unreachable")
        try:
            return next(self._frames)
        except StopIteration:
            raise _StopRecorder from None


class _ConnectionContext:
    def __init__(self, connection: _Connection) -> None:
        self._connection = connection

    async def __aenter__(self) -> _Connection:
        return self._connection

    async def __aexit__(self, *args: object) -> None:
        return None


def _writer(root: Path, run_id: str) -> SegmentedRawWriter:
    endpoint_id, endpoint_revision = trade_capture.ENDPOINT_BINDINGS["gmo"]
    return SegmentedRawWriter(
        root, "gmo", "BTC", domain="trade_realtime", run_id=run_id,
        endpoint_id=endpoint_id, endpoint_revision=endpoint_revision,
        segment_seconds=3600, segment_max_bytes=1024 * 1024,
    )


def _raw_rows(writer: SegmentedRawWriter) -> list[dict[str, Any]]:
    writer.finish()
    paths = sorted(writer.directory.glob("segment-*.jsonl"))
    return [
        json.loads(line)
        for path in paths
        for line in path.read_text(encoding="utf-8").splitlines()
    ]


async def _no_delay(_: float) -> None:
    return None


def test_gmo_error_frame_is_persisted_before_reconnect(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    error = json.dumps({
        "error": {"code": "ERR-5003", "message": "Requests are too many."}
    })
    connection = _Connection([error])
    connect_calls = 0

    def connect(_: str) -> _ConnectionContext:
        nonlocal connect_calls
        connect_calls += 1
        if connect_calls > 1:
            raise _StopRecorder
        return _ConnectionContext(connection)

    monkeypatch.setattr(websockets, "connect", connect)
    monkeypatch.setattr(asyncio, "sleep", _no_delay)
    writer = _writer(tmp_path, "run-gmo-error")
    stats = trade_capture.CaptureStats("gmo", "BTC")

    with pytest.raises(_StopRecorder):
        asyncio.run(trade_capture._record_gmo(writer, stats, None))
    rows = _raw_rows(writer)

    assert connect_calls == 2
    assert [row["payload_raw"] for row in rows] == [error]
    assert stats.wire_frames == stats.control_frames == 1
    assert stats.data_frames == 0
    assert stats.disconnects == stats.reconnects == 1
    assert stats.consecutive_failures == 1
    assert json.loads(connection.sent[0]) == {
        "command": "subscribe",
        "channel": "trades",
        "symbol": "BTC",
        "option": "TAKER_ONLY",
    }


def test_gmo_endless_capture_reconnects_after_silence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _Connection([], silent=True)
    connect_calls = 0

    def connect(_: str) -> _ConnectionContext:
        nonlocal connect_calls
        connect_calls += 1
        if connect_calls > 1:
            raise _StopRecorder
        return _ConnectionContext(connection)

    monkeypatch.setattr(websockets, "connect", connect)
    monkeypatch.setattr(asyncio, "sleep", _no_delay)
    monkeypatch.setattr(trade_capture, "SILENCE_TIMEOUT_SECONDS", 0.001)
    writer = _writer(tmp_path, "run-gmo-silence")
    stats = trade_capture.CaptureStats("gmo", "BTC")

    with pytest.raises(_StopRecorder):
        asyncio.run(trade_capture._record_gmo(writer, stats, None))
    rows = _raw_rows(writer)

    assert connect_calls == 2
    assert rows == []
    assert stats.wire_frames == stats.data_frames == 0
    assert stats.disconnects == stats.reconnects == 1
    assert stats.consecutive_failures == 1
    assert json.loads(connection.sent[0])["option"] == "TAKER_ONLY"


def test_gmo_trade_clears_consecutive_failure_after_persist(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    trade = json.dumps({
        "channel": "trades", "price": "17000000", "size": "0.01",
        "side": "BUY", "timestamp": "2026-08-12T00:00:00.000Z",
    })
    connection = _Connection([trade])
    monkeypatch.setattr(
        websockets, "connect",
        lambda _: _ConnectionContext(connection),
    )
    writer = _writer(tmp_path, "run-gmo-success")
    stats = trade_capture.CaptureStats("gmo", "BTC", consecutive_failures=4)

    with pytest.raises(_StopRecorder):
        asyncio.run(trade_capture._record_gmo(writer, stats, None))
    rows = _raw_rows(writer)

    assert [row["payload_raw"] for row in rows] == [trade]
    assert stats.wire_frames == stats.data_frames == 1
    assert stats.control_frames == 0
    assert stats.consecutive_failures == 0
    assert stats.last_data_time == rows[0]["recv_ts_utc"]
    assert json.loads(connection.sent[0])["option"] == "TAKER_ONLY"
