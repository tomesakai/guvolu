from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import time
from collections.abc import Awaitable, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
import websockets
from websockets.asyncio.client import ClientConnection
from websockets.client import ClientProtocol
from websockets.protocol import State
from websockets.uri import parse_uri

from guvolu.data import trade_capture
from guvolu.data.segmented_raw import SegmentedRawWriter


class _StopRecorder(RuntimeError):
    """终止测试用的外部异常；生产重连捕获不应吞掉它。"""


class _FatalRecorder(BaseException):
    """验证非 CancelledError BaseException 必须形成 failed 终态。"""


class _Transport:
    def __init__(self, abort_error: BaseException | None = None) -> None:
        self.abort_calls = 0
        self.aborted = asyncio.Event()
        self._abort_error = abort_error

    def abort(self) -> None:
        self.abort_calls += 1
        self.aborted.set()
        if self._abort_error is not None:
            raise self._abort_error


class _Connection:
    def __init__(
        self,
        frames: Sequence[str | bytes | tuple[float, str | bytes]],
        *,
        silent: bool = False,
        blocked_sends: frozenset[str] = frozenset(),
        terminal_error: BaseException | None = None,
        abort_error: BaseException | None = None,
        pong_mode: str = "error",
    ) -> None:
        self._frames = iter(frames)
        self._silent = silent
        self._blocked_sends = blocked_sends
        self._terminal_error = terminal_error
        self._pong_mode = pong_mode
        self.sent: list[str] = []
        self.ping_calls = 0
        self.transport = _Transport(abort_error)

    async def send(self, message: str) -> None:
        self.sent.append(message)
        if message in self._blocked_sends:
            await asyncio.Event().wait()

    async def recv(self) -> str | bytes:
        try:
            item = next(self._frames)
        except StopIteration:
            if self._silent:
                await asyncio.Event().wait()
                raise AssertionError("unreachable")
            if self._terminal_error is not None:
                raise self._terminal_error
            raise _StopRecorder from None
        if isinstance(item, tuple):
            delay, frame = item
            await asyncio.sleep(delay)
            return frame
        return item

    async def ping(self) -> Awaitable[float]:
        self.ping_calls += 1
        if self._pong_mode == "error":
            raise ConnectionError("injected pong failure")
        waiter: asyncio.Future[float] = asyncio.get_running_loop().create_future()
        if self._pong_mode == "success":
            waiter.set_result(0.001)
        elif self._pong_mode != "silent":
            raise AssertionError(f"unknown pong mode: {self._pong_mode}")
        return waiter


class _ConnectionContext:
    def __init__(
        self,
        connection: _Connection,
        *,
        enter_silent: bool = False,
        exit_error: BaseException | None = None,
        exit_mode: str = "normal",
    ) -> None:
        self._connection = connection
        self._enter_silent = enter_silent
        self._exit_error = exit_error
        self._exit_mode = exit_mode
        self.exit_calls = 0
        self.exit_started = asyncio.Event()
        self.cleanup_release = asyncio.Event()

    async def __aenter__(self) -> _Connection:
        if self._enter_silent:
            await asyncio.Event().wait()
        return self._connection

    async def __aexit__(self, *args: object) -> None:
        self.exit_calls += 1
        self.exit_started.set()
        if self._exit_error is not None:
            raise self._exit_error
        if self._exit_mode == "ignore_cancel_until_abort":
            while not self._connection.transport.aborted.is_set():
                try:
                    await self._connection.transport.aborted.wait()
                except asyncio.CancelledError:
                    continue
        elif self._exit_mode == "delay_cancel_one_turn":
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await asyncio.sleep(0)
        elif self._exit_mode == "ignore_cancel_and_abort":
            while not self.cleanup_release.is_set():
                try:
                    await self.cleanup_release.wait()
                except asyncio.CancelledError:
                    continue
        return None


def _writer(
    root: Path, run_id: str, *, symbol: str = "BTC",
) -> SegmentedRawWriter:
    endpoint_id, endpoint_revision = trade_capture.ENDPOINT_BINDINGS["gmo"]
    return SegmentedRawWriter(
        root, "gmo", symbol, domain="trade_realtime", run_id=run_id,
        endpoint_id=endpoint_id, endpoint_revision=endpoint_revision,
        segment_seconds=3600, segment_max_bytes=1024 * 1024,
    )


def _bitbank_writer(root: Path, run_id: str) -> SegmentedRawWriter:
    endpoint_id, endpoint_revision = trade_capture.ENDPOINT_BINDINGS["bitbank"]
    return SegmentedRawWriter(
        root, "bitbank", "btc_jpy", domain="trade_realtime", run_id=run_id,
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


_ENGINE_OPEN = (
    '0{"sid":"engine-sid","upgrades":[],"pingInterval":25000,'
    '"pingTimeout":20000,"maxPayload":1000000}'
)
_SOCKET_CONNECT = '40{"sid":"socket-sid"}'
_JOIN = '42["join-room","transactions_btc_jpy"]'


def _transaction(**overrides: object) -> dict[str, object]:
    transaction: dict[str, object] = {
        "transaction_id": 1,
        "side": "buy",
        "price": "12500000",
        "amount": "0.001",
        "executed_at": time.time_ns() // 1_000_000,
    }
    transaction.update(overrides)
    return transaction


def _trade_frame(
    *,
    event: str = "message",
    room: str = "transactions_btc_jpy",
    transactions: object | None = None,
) -> str:
    payload = [_transaction()] if transactions is None else transactions
    return "42" + json.dumps([event, {
        "room_name": room,
        "message": {"data": {"transactions": payload}},
    }], separators=(",", ":"))


def _classify_trade_frame(
    frame: str,
    *,
    received_at: datetime | None = None,
    after_transaction_id: int | None = None,
) -> tuple[str, bool, int | None]:
    return trade_capture._bitbank_trade_frame(
        frame,
        "transactions_btc_jpy",
        datetime.now(UTC) if received_at is None else received_at,
        after_transaction_id,
    )


def _assert_bitbank_connect_options(
    ping_interval: float | None,
    open_timeout: float | None,
    close_timeout: float | None,
) -> None:
    assert ping_interval is None
    assert open_timeout == trade_capture.SILENCE_TIMEOUT_SECONDS
    assert close_timeout == trade_capture.BITBANK_TEARDOWN_TIMEOUT_SECONDS


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
    assert connection.ping_calls == 1
    assert stats.transport_probes == stats.transport_probe_failures == 1
    assert stats.transport_probe_successes == 0
    assert json.loads(connection.sent[0])["option"] == "TAKER_ONLY"


def test_gmo_low_activity_pong_keeps_subscription_without_faking_data(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _Connection([], silent=True, pong_mode="success")
    connect_calls = 0

    def connect(_: str) -> _ConnectionContext:
        nonlocal connect_calls
        connect_calls += 1
        return _ConnectionContext(connection)

    monkeypatch.setattr(websockets, "connect", connect)
    monkeypatch.setattr(trade_capture, "SILENCE_TIMEOUT_SECONDS", 0.02)
    monkeypatch.setattr(
        trade_capture, "GMO_TRANSPORT_PROBE_TIMEOUT_SECONDS", 0.02,
    )
    writer = _writer(tmp_path, "run-gmo-low-activity", symbol="DOGE")
    stats = trade_capture.CaptureStats("gmo", "DOGE")
    started = time.monotonic()

    asyncio.run(trade_capture._record_gmo(
        writer, stats, time.monotonic() + 0.12,
    ))
    writer.finish()

    assert time.monotonic() - started < 0.5
    assert connect_calls == 1
    assert stats.sessions == stats.successful_sessions == 1
    assert stats.disconnects == stats.reconnects == 0
    assert stats.wire_frames == stats.data_frames == stats.control_frames == 0
    assert stats.last_wire_time is None
    assert stats.last_data_time is None
    assert stats.transport_probes >= 2
    assert stats.transport_probe_successes == stats.transport_probes
    assert stats.transport_probe_failures == 0
    assert stats.last_transport_health_time is not None


def test_gmo_pong_timeout_reconnects_without_data_freshness(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _Connection([], silent=True, pong_mode="silent")
    connect_calls = 0

    def connect(_: str) -> _ConnectionContext:
        nonlocal connect_calls
        connect_calls += 1
        if connect_calls > 1:
            raise _StopRecorder
        return _ConnectionContext(connection)

    monkeypatch.setattr(websockets, "connect", connect)
    monkeypatch.setattr(trade_capture, "reconnect_delay_seconds", lambda _: 0.0)
    monkeypatch.setattr(trade_capture, "SILENCE_TIMEOUT_SECONDS", 0.01)
    monkeypatch.setattr(
        trade_capture, "GMO_TRANSPORT_PROBE_TIMEOUT_SECONDS", 0.01,
    )
    writer = _writer(tmp_path, "run-gmo-pong-timeout", symbol="DOGE")
    stats = trade_capture.CaptureStats("gmo", "DOGE")

    with pytest.raises(_StopRecorder):
        asyncio.run(trade_capture._record_gmo(writer, stats, None))
    writer.finish(status="interrupted")

    assert connect_calls == 2
    assert connection.ping_calls == 1
    assert stats.transport_probes == stats.transport_probe_failures == 1
    assert stats.transport_probe_successes == 0
    assert stats.data_frames == 0
    assert stats.last_data_time is None
    assert stats.disconnects == stats.reconnects == 1


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


@pytest.mark.parametrize(
    ("frames", "wire_frames", "successful_sessions", "sent"),
    [
        ([], 0, 0, []),
        ([_ENGINE_OPEN], 1, 0, ["40"]),
        ([_ENGINE_OPEN, _SOCKET_CONNECT], 2, 1, ["40", _JOIN]),
    ],
)
def test_bitbank_trade_reconnects_on_each_wire_silence_stage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    frames: list[str],
    wire_frames: int,
    successful_sessions: int,
    sent: list[str],
) -> None:
    connection = _Connection(frames, silent=True)
    connect_calls = 0

    def connect(
        _: str,
        *,
        ping_interval: float | None = None,
        open_timeout: float | None = None,
        close_timeout: float | None = None,
    ) -> _ConnectionContext:
        nonlocal connect_calls
        _assert_bitbank_connect_options(
            ping_interval, open_timeout, close_timeout,
        )
        connect_calls += 1
        if connect_calls > 1:
            raise _StopRecorder
        return _ConnectionContext(connection)

    monkeypatch.setattr(websockets, "connect", connect)
    monkeypatch.setattr(asyncio, "sleep", _no_delay)
    monkeypatch.setattr(trade_capture, "SILENCE_TIMEOUT_SECONDS", 0.05)
    writer = _bitbank_writer(tmp_path, "run-bitbank-wire-silence")
    stats = trade_capture.CaptureStats("bitbank", "btc_jpy")

    with pytest.raises(_StopRecorder):
        asyncio.run(trade_capture._record_bitbank(writer, stats, None))
    rows = _raw_rows(writer)

    assert connect_calls == stats.connection_attempts == 2
    assert [row["payload_raw"] for row in rows] == frames
    assert stats.wire_frames == stats.control_frames == wire_frames
    assert stats.data_frames == 0
    assert stats.sessions == 1
    assert stats.successful_sessions == successful_sessions
    assert stats.disconnects == stats.reconnects == 1
    assert stats.consecutive_failures == 1
    assert connection.sent == sent


@pytest.mark.parametrize(
    "frame",
    [
        "0{}",
        '0{"sid":"x","upgrades":[],"pingInterval":25000}',
        '0{"sid":"x","upgrades":[],"pingInterval":true,'
        '"pingTimeout":20000,"maxPayload":1000000}',
        "00" + _ENGINE_OPEN[1:],
    ],
)
def test_bitbank_engine_open_requires_exact_json_contract(frame: str) -> None:
    assert not trade_capture._bitbank_engine_open(frame)
    assert trade_capture._bitbank_engine_open(_ENGINE_OPEN)


@pytest.mark.parametrize(
    "frame",
    ["40", "400", "40{}", '40{"sid":""}', "40not-json"],
)
def test_bitbank_socket_connect_requires_exact_root_ack(frame: str) -> None:
    assert not trade_capture._bitbank_socket_connect(frame)
    assert trade_capture._bitbank_socket_connect(_SOCKET_CONNECT)


def test_bitbank_handshake_gives_each_frame_a_fresh_wire_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    trade = _trade_frame()
    connection = _Connection([
        (0.06, _ENGINE_OPEN), (0.06, _SOCKET_CONNECT), trade,
    ])

    def connect(
        _: str,
        *,
        ping_interval: float | None = None,
        open_timeout: float | None = None,
        close_timeout: float | None = None,
    ) -> _ConnectionContext:
        _assert_bitbank_connect_options(
            ping_interval, open_timeout, close_timeout,
        )
        return _ConnectionContext(connection)

    monkeypatch.setattr(websockets, "connect", connect)
    monkeypatch.setattr(trade_capture, "SILENCE_TIMEOUT_SECONDS", 0.1)
    writer = _bitbank_writer(tmp_path, "run-bitbank-frame-budget")
    stats = trade_capture.CaptureStats("bitbank", "btc_jpy")

    with pytest.raises(_StopRecorder):
        asyncio.run(trade_capture._record_bitbank(writer, stats, None))
    rows = _raw_rows(writer)

    assert [row["payload_raw"] for row in rows] == [
        _ENGINE_OPEN, _SOCKET_CONNECT, trade,
    ]
    assert stats.successful_sessions == 1
    assert stats.data_frames == 1


@pytest.mark.parametrize(
    "frame",
    [
        _trade_frame(event="trades"),
        _trade_frame(room="transactions_eth_jpy"),
        _trade_frame(transactions=[]),
        _trade_frame(transactions=[{}]),
        _trade_frame(transactions=["not-an-object"]),
        '42["message",{"room_name":"transactions_btc_jpy",'
        '"message":{"data":{}}}]',
        '42["message",{"room_name":"transactions_btc_jpy",'
        '"message":{"data":{"transactions":[{}]}}},"extra"]',
    ],
)
def test_bitbank_non_target_or_malformed_frames_are_not_trade_data(
    frame: str,
) -> None:
    _, is_trade, cursor = _classify_trade_frame(frame)
    assert not is_trade
    assert cursor is None


@pytest.mark.parametrize(
    "transaction",
    [
        {key: None for key in _transaction()},
        _transaction(transaction_id=True),
        _transaction(transaction_id=0),
        _transaction(transaction_id=-1),
        _transaction(side="hold"),
        _transaction(side=None),
        _transaction(side=[]),
        _transaction(price=True),
        _transaction(price=1),
        _transaction(price=1.0),
        _transaction(price="NaN"),
        _transaction(price="Infinity"),
        _transaction(price="1e999999"),
        _transaction(price="9" * 65),
        _transaction(price="+1"),
        _transaction(price=" 1"),
        _transaction(price="-1"),
        _transaction(price="0"),
        _transaction(amount=True),
        _transaction(amount=1),
        _transaction(amount="NaN"),
        _transaction(amount="1e999999"),
        _transaction(amount="9" * 65),
        _transaction(amount="-1"),
        _transaction(amount="0"),
        _transaction(executed_at=True),
        _transaction(executed_at="1787673600000"),
        _transaction(executed_at=1_787_673_600),
        _transaction(executed_at=-1),
        _transaction(executed_at=0),
        _transaction(
            executed_at=(time.time_ns() // 1_000_000) + 86_400_000,
        ),
        _transaction(executed_at=1_483_228_799_999),
        _transaction(executed_at=99_999_999_999_999_999_999),
        {
            "transaction_id": 0,
            "side": "hold",
            "price": "0",
            "amount": "0",
            "executed_at": 0,
        },
    ],
)
def test_bitbank_transaction_requires_complete_semantically_valid_schema(
    transaction: dict[str, object],
) -> None:
    assert not trade_capture._bitbank_transaction(transaction)
    assert trade_capture._bitbank_transaction(_transaction())


def test_bitbank_mixed_valid_and_invalid_transactions_are_all_control(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    mixed = _trade_frame(transactions=[
        _transaction(), _transaction(amount="NaN"),
    ])
    connection = _Connection([
        _ENGINE_OPEN, _SOCKET_CONNECT, mixed,
    ])

    def connect(
        _: str,
        *,
        ping_interval: float | None = None,
        open_timeout: float | None = None,
        close_timeout: float | None = None,
    ) -> _ConnectionContext:
        _assert_bitbank_connect_options(
            ping_interval, open_timeout, close_timeout,
        )
        return _ConnectionContext(connection)

    monkeypatch.setattr(websockets, "connect", connect)
    writer = _bitbank_writer(tmp_path, "run-bitbank-mixed-invalid")
    stats = trade_capture.CaptureStats(
        "bitbank", "btc_jpy", consecutive_failures=4,
    )

    with pytest.raises(_StopRecorder):
        asyncio.run(trade_capture._record_bitbank(writer, stats, None))
    rows = _raw_rows(writer)

    assert [row["payload_raw"] for row in rows] == [
        _ENGINE_OPEN, _SOCKET_CONNECT, mixed,
    ]
    assert rows[-1]["channel_id"] == "transactions_btc_jpy"
    assert stats.wire_frames == stats.control_frames == 3
    assert stats.data_frames == 0
    assert stats.last_data_time is None
    assert stats.consecutive_failures == 4


def test_bitbank_health_requires_fresh_cursor_progress_but_persists_all(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    received_at = datetime.now(UTC)
    fresh_ms = int(received_at.timestamp() * 1000)
    stale_transaction = _transaction(
        transaction_id=101,
        executed_at=int((received_at - timedelta(hours=75)).timestamp() * 1000),
    )
    first = _trade_frame(transactions=[
        _transaction(transaction_id=100, executed_at=fresh_ms),
    ])
    duplicate = first
    stale = _trade_frame(transactions=[stale_transaction])
    future = _trade_frame(transactions=[_transaction(
        transaction_id=104,
        executed_at=int((received_at + timedelta(minutes=2)).timestamp() * 1000),
    )])
    reverse_with_progress = _trade_frame(transactions=[
        _transaction(transaction_id=103, executed_at=fresh_ms),
        _transaction(transaction_id=102, executed_at=fresh_ms),
    ])
    frames = [first, duplicate, stale, future, reverse_with_progress]
    connection = _Connection([_ENGINE_OPEN, _SOCKET_CONNECT, *frames])

    def connect(
        _: str,
        *,
        ping_interval: float | None = None,
        open_timeout: float | None = None,
        close_timeout: float | None = None,
    ) -> _ConnectionContext:
        _assert_bitbank_connect_options(
            ping_interval, open_timeout, close_timeout,
        )
        return _ConnectionContext(connection)

    monkeypatch.setattr(websockets, "connect", connect)
    writer = _bitbank_writer(tmp_path, "run-bitbank-health-cursor")
    stats = trade_capture.CaptureStats(
        "bitbank", "btc_jpy", consecutive_failures=4,
    )

    with pytest.raises(_StopRecorder):
        asyncio.run(trade_capture._record_bitbank(writer, stats, None))
    rows = _raw_rows(writer)

    assert trade_capture._bitbank_transaction(stale_transaction)
    assert [row["payload_raw"] for row in rows] == [
        _ENGINE_OPEN, _SOCKET_CONNECT, *frames,
    ]
    assert stats.wire_frames == 7
    assert stats.data_frames == 2
    assert stats.control_frames == 5
    assert stats.consecutive_failures == 0
    assert stats.last_data_time == rows[-1]["recv_ts_utc"]


def test_bitbank_transaction_cursor_survives_reconnect_and_rejects_replay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    trade = _trade_frame(transactions=[_transaction(transaction_id=500)])
    first = _Connection(
        [_ENGINE_OPEN, _SOCKET_CONNECT, trade],
        terminal_error=ConnectionError("first connection dropped"),
    )
    second = _Connection([_ENGINE_OPEN, _SOCKET_CONNECT, trade])
    contexts = [
        _ConnectionContext(first),
        _ConnectionContext(second),
    ]
    connect_calls = 0

    def connect(
        _: str,
        *,
        ping_interval: float | None = None,
        open_timeout: float | None = None,
        close_timeout: float | None = None,
    ) -> _ConnectionContext:
        nonlocal connect_calls
        _assert_bitbank_connect_options(
            ping_interval, open_timeout, close_timeout,
        )
        context = contexts[connect_calls]
        connect_calls += 1
        return context

    monkeypatch.setattr(websockets, "connect", connect)
    monkeypatch.setattr(trade_capture, "reconnect_delay_seconds", lambda _: 0.0)
    writer = _bitbank_writer(tmp_path, "run-bitbank-reconnect-replay")
    stats = trade_capture.CaptureStats("bitbank", "btc_jpy")

    with pytest.raises(_StopRecorder):
        asyncio.run(trade_capture._record_bitbank(writer, stats, None))
    rows = _raw_rows(writer)

    assert connect_calls == stats.connection_attempts == 2
    assert [row["payload_raw"] for row in rows] == [
        _ENGINE_OPEN, _SOCKET_CONNECT, trade,
        _ENGINE_OPEN, _SOCKET_CONNECT, trade,
    ]
    assert stats.sessions == stats.successful_sessions == 2
    assert stats.reconnects == stats.disconnects == 1
    assert stats.wire_frames == 6
    assert stats.data_frames == 1
    assert stats.control_frames == 5
    assert stats.consecutive_failures == 1
    assert stats.last_data_time == rows[2]["recv_ts_utc"]


def test_bitbank_huge_id_then_fresh_low_epoch_requires_two_frames(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    base_ms = (time.time_ns() // 1_000_000) - 1_000
    huge = _trade_frame(transactions=[_transaction(
        transaction_id=10**100, executed_at=base_ms,
    )])
    low_first = _trade_frame(transactions=[_transaction(
        transaction_id=100, executed_at=base_ms + 10,
    )])
    low_second = _trade_frame(transactions=[_transaction(
        transaction_id=101, executed_at=base_ms + 20,
    )])
    first = _Connection(
        [_ENGINE_OPEN, _SOCKET_CONNECT, huge],
        terminal_error=ConnectionError("epoch boundary reconnect"),
    )
    second = _Connection([
        _ENGINE_OPEN, _SOCKET_CONNECT, low_first, low_second, low_second,
    ])
    contexts = [_ConnectionContext(first), _ConnectionContext(second)]
    connect_calls = 0

    def connect(
        _: str,
        *,
        ping_interval: float | None = None,
        open_timeout: float | None = None,
        close_timeout: float | None = None,
    ) -> _ConnectionContext:
        nonlocal connect_calls
        _assert_bitbank_connect_options(
            ping_interval, open_timeout, close_timeout,
        )
        context = contexts[connect_calls]
        connect_calls += 1
        return context

    monkeypatch.setattr(websockets, "connect", connect)
    monkeypatch.setattr(trade_capture, "reconnect_delay_seconds", lambda _: 0.0)
    writer = _bitbank_writer(tmp_path, "run-bitbank-epoch-reset")
    stats = trade_capture.CaptureStats("bitbank", "btc_jpy")

    with pytest.raises(_StopRecorder):
        asyncio.run(trade_capture._record_bitbank(writer, stats, None))
    rows = _raw_rows(writer)

    assert trade_capture._bitbank_transaction(_transaction(
        transaction_id=10**100,
    ))
    assert [row["payload_raw"] for row in rows] == [
        _ENGINE_OPEN, _SOCKET_CONNECT, huge,
        _ENGINE_OPEN, _SOCKET_CONNECT, low_first, low_second, low_second,
    ]
    # huge 是合法首证据；
    # 第一份低位帧只建候选；
    # 第二份确认新 epoch；
    # 重放确认帧不再刷新。
    assert stats.data_frames == 2
    assert stats.control_frames == 6
    assert stats.reconnects == stats.disconnects == 1


def test_bitbank_invalid_utf8_binary_is_reversibly_persisted_then_reconnects(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_binary = b"\xff\xfe\x00bitbank"
    trade = _trade_frame(transactions=[_transaction(transaction_id=700)])
    first = _Connection([_ENGINE_OPEN, _SOCKET_CONNECT, raw_binary])
    second = _Connection([_ENGINE_OPEN, _SOCKET_CONNECT, trade])
    contexts = [_ConnectionContext(first), _ConnectionContext(second)]
    connect_calls = 0

    def connect(
        _: str,
        *,
        ping_interval: float | None = None,
        open_timeout: float | None = None,
        close_timeout: float | None = None,
    ) -> _ConnectionContext:
        nonlocal connect_calls
        _assert_bitbank_connect_options(
            ping_interval, open_timeout, close_timeout,
        )
        context = contexts[connect_calls]
        connect_calls += 1
        return context

    monkeypatch.setattr(websockets, "connect", connect)
    monkeypatch.setattr(trade_capture, "reconnect_delay_seconds", lambda _: 0.0)
    writer = _bitbank_writer(tmp_path, "run-bitbank-binary-evidence")
    stats = trade_capture.CaptureStats("bitbank", "btc_jpy")

    with pytest.raises(_StopRecorder):
        asyncio.run(trade_capture._record_bitbank(writer, stats, None))
    rows = _raw_rows(writer)
    envelope = json.loads(rows[2]["payload_raw"])

    assert rows[2]["channel_id"] == "protocol_control"
    assert envelope == {
        "wire_envelope_version": 1,
        "opcode": "binary",
        "encoding": "base64",
        "byte_count": len(raw_binary),
        "payload_sha256": hashlib.sha256(raw_binary).hexdigest(),
        "payload_base64": base64.b64encode(raw_binary).decode("ascii"),
    }
    assert base64.b64decode(envelope["payload_base64"]) == raw_binary
    assert connect_calls == stats.connection_attempts == 2
    assert stats.reconnects == stats.disconnects == 1
    assert stats.wire_frames == 6
    assert stats.data_frames == 1
    assert stats.control_frames == 5


def test_bitbank_backoff_caps_exponent_before_helper_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[int] = []

    def delay(attempt: int) -> float:
        seen.append(attempt)
        return 0.0

    monkeypatch.setattr(trade_capture, "reconnect_delay_seconds", delay)
    assert asyncio.run(trade_capture._bitbank_backoff(10**100, None))
    assert seen == [63]


def test_record_trades_checkpoint_failure_is_primary_and_cancels_recorder(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorder_cancelled = False

    async def recorder(
        writer: SegmentedRawWriter,
        stats: trade_capture.CaptureStats,
        deadline: float | None,
    ) -> None:
        nonlocal recorder_cancelled
        del stats, deadline
        writer.write_frame('{"channel":"trades"}', "trades/ws")
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            recorder_cancelled = True
            raise

    def fail_checkpoint(*args: object, **kwargs: object) -> Path:
        del args, kwargs
        raise OSError("checkpoint disk failure")

    monkeypatch.setitem(trade_capture._RECORDERS, "gmo", recorder)
    monkeypatch.setattr(trade_capture, "CHECKPOINT_SECONDS", 0.001)
    monkeypatch.setattr(
        SegmentedRawWriter, "checkpoint", fail_checkpoint,
    )

    with pytest.raises(OSError, match="checkpoint disk failure"):
        asyncio.run(trade_capture.record_trades(
            tmp_path, "gmo", "BTC", 0.0, 3600, 1024 * 1024,
        ))

    run = json.loads(next(
        tmp_path.rglob("run.manifest.json")
    ).read_text(encoding="utf-8"))
    assert recorder_cancelled
    assert run["status"] == "failed"
    assert run["completion_claim"] is False
    assert run["failure_detail"] == "OSError: checkpoint disk failure"


def test_record_trades_baseexception_is_failed_and_finish_cannot_mask_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def recorder(
        writer: SegmentedRawWriter,
        stats: trade_capture.CaptureStats,
        deadline: float | None,
    ) -> None:
        del writer, stats, deadline
        raise _FatalRecorder("fatal recorder failure")

    original_finish = SegmentedRawWriter.finish

    def fail_after_finish(
        writer: SegmentedRawWriter,
        extra: dict[str, object] | None = None,
        status: str = "complete",
    ) -> Path:
        original_finish(writer, extra, status)
        raise OSError("finish reporting failure")

    monkeypatch.setitem(trade_capture._RECORDERS, "gmo", recorder)
    monkeypatch.setattr(SegmentedRawWriter, "finish", fail_after_finish)

    with pytest.raises(_FatalRecorder, match="fatal recorder failure") as caught:
        asyncio.run(trade_capture.record_trades(
            tmp_path, "gmo", "BTC", 0.0, 3600, 1024 * 1024,
        ))

    assert any(
        "finish reporting failure" in note
        for note in getattr(caught.value, "__notes__", [])
    )
    run = json.loads(next(
        tmp_path.rglob("run.manifest.json")
    ).read_text(encoding="utf-8"))
    assert run["status"] == "failed"
    assert run["completion_claim"] is False


def test_bitbank_nonqualifying_frames_do_not_reset_data_watchdog(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    frames = [
        _trade_frame(event="trades"),
        _trade_frame(room="transactions_eth_jpy"),
        _trade_frame(transactions=[]),
        '42["message",{"room_name":"transactions_btc_jpy",'
        '"message":{"data":{}}}]',
    ]
    connection = _Connection(
        [_ENGINE_OPEN, _SOCKET_CONNECT, *frames], silent=True,
    )
    connect_calls = 0

    def connect(
        _: str,
        *,
        ping_interval: float | None = None,
        open_timeout: float | None = None,
        close_timeout: float | None = None,
    ) -> _ConnectionContext:
        nonlocal connect_calls
        _assert_bitbank_connect_options(
            ping_interval, open_timeout, close_timeout,
        )
        connect_calls += 1
        if connect_calls > 1:
            raise _StopRecorder
        return _ConnectionContext(connection)

    monkeypatch.setattr(websockets, "connect", connect)
    monkeypatch.setattr(trade_capture, "reconnect_delay_seconds", lambda _: 0.0)
    monkeypatch.setattr(
        trade_capture, "BITBANK_DATA_SILENCE_TIMEOUT_SECONDS", 0.3,
    )
    writer = _bitbank_writer(tmp_path, "run-bitbank-invalid-data")
    stats = trade_capture.CaptureStats("bitbank", "btc_jpy")

    with pytest.raises(_StopRecorder):
        asyncio.run(trade_capture._record_bitbank(writer, stats, None))
    rows = _raw_rows(writer)

    assert [row["payload_raw"] for row in rows] == [
        _ENGINE_OPEN, _SOCKET_CONNECT, *frames,
    ]
    assert stats.wire_frames == stats.control_frames == 2 + len(frames)
    assert stats.data_frames == 0
    assert stats.successful_sessions == 1
    assert stats.disconnects == stats.reconnects == 1


def test_bitbank_continuous_control_heartbeats_do_not_fake_data_health(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    heartbeats = [(0.02, "2") for _ in range(100)]
    connection = _Connection([_ENGINE_OPEN, _SOCKET_CONNECT, *heartbeats])
    connect_calls = 0

    def connect(
        _: str,
        *,
        ping_interval: float | None = None,
        open_timeout: float | None = None,
        close_timeout: float | None = None,
    ) -> _ConnectionContext:
        nonlocal connect_calls
        _assert_bitbank_connect_options(
            ping_interval, open_timeout, close_timeout,
        )
        connect_calls += 1
        if connect_calls > 1:
            raise _StopRecorder
        return _ConnectionContext(connection)

    monkeypatch.setattr(websockets, "connect", connect)
    monkeypatch.setattr(trade_capture, "reconnect_delay_seconds", lambda _: 0.0)
    monkeypatch.setattr(
        trade_capture, "BITBANK_DATA_SILENCE_TIMEOUT_SECONDS", 0.3,
    )
    writer = _bitbank_writer(tmp_path, "run-bitbank-control-only")
    stats = trade_capture.CaptureStats("bitbank", "btc_jpy")

    with pytest.raises(_StopRecorder):
        asyncio.run(trade_capture._record_bitbank(writer, stats, None))
    rows = _raw_rows(writer)

    persisted_heartbeats = len(rows) - 2
    assert 2 <= persisted_heartbeats < len(heartbeats)
    assert stats.wire_frames == stats.control_frames == len(rows)
    assert stats.data_frames == 0
    assert connection.sent == ["40", _JOIN, *(["3"] * persisted_heartbeats)]
    assert stats.disconnects == stats.reconnects == 1


def test_bitbank_trade_data_frame_resets_failure_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    trade = _trade_frame()
    connection = _Connection([_ENGINE_OPEN, _SOCKET_CONNECT, trade])

    def connect(
        _: str,
        *,
        ping_interval: float | None = None,
        open_timeout: float | None = None,
        close_timeout: float | None = None,
    ) -> _ConnectionContext:
        _assert_bitbank_connect_options(
            ping_interval, open_timeout, close_timeout,
        )
        return _ConnectionContext(connection)

    monkeypatch.setattr(websockets, "connect", connect)
    writer = _bitbank_writer(tmp_path, "run-bitbank-data")
    stats = trade_capture.CaptureStats(
        "bitbank", "btc_jpy", consecutive_failures=4,
    )

    with pytest.raises(_StopRecorder):
        asyncio.run(trade_capture._record_bitbank(writer, stats, None))
    rows = _raw_rows(writer)

    assert [row["payload_raw"] for row in rows] == [
        _ENGINE_OPEN, _SOCKET_CONNECT, trade,
    ]
    assert stats.wire_frames == 3
    assert stats.control_frames == 2
    assert stats.data_frames == 1
    assert stats.sessions == stats.successful_sessions == 1
    assert stats.consecutive_failures == 0
    assert stats.last_data_time == rows[-1]["recv_ts_utc"]


def test_bitbank_join_room_send_is_bounded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _Connection(
        [_ENGINE_OPEN, _SOCKET_CONNECT], blocked_sends=frozenset({_JOIN}),
    )
    connect_calls = 0

    def connect(
        _: str,
        *,
        ping_interval: float | None = None,
        open_timeout: float | None = None,
        close_timeout: float | None = None,
    ) -> _ConnectionContext:
        nonlocal connect_calls
        _assert_bitbank_connect_options(
            ping_interval, open_timeout, close_timeout,
        )
        connect_calls += 1
        if connect_calls > 1:
            raise _StopRecorder
        return _ConnectionContext(connection)

    monkeypatch.setattr(websockets, "connect", connect)
    monkeypatch.setattr(asyncio, "sleep", _no_delay)
    monkeypatch.setattr(trade_capture, "SILENCE_TIMEOUT_SECONDS", 0.05)
    writer = _bitbank_writer(tmp_path, "run-bitbank-join-timeout")
    stats = trade_capture.CaptureStats("bitbank", "btc_jpy")

    with pytest.raises(_StopRecorder):
        asyncio.run(trade_capture._record_bitbank(writer, stats, None))
    writer.finish(status="interrupted")

    assert connection.sent == ["40", _JOIN]
    assert stats.sessions == 1
    assert stats.successful_sessions == 0
    assert stats.disconnects == stats.reconnects == 1


def test_bitbank_pong_send_is_bounded_by_data_watchdog(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _Connection(
        [_ENGINE_OPEN, _SOCKET_CONNECT, "2"],
        blocked_sends=frozenset({"3"}),
    )
    connect_calls = 0

    def connect(
        _: str,
        *,
        ping_interval: float | None = None,
        open_timeout: float | None = None,
        close_timeout: float | None = None,
    ) -> _ConnectionContext:
        nonlocal connect_calls
        _assert_bitbank_connect_options(
            ping_interval, open_timeout, close_timeout,
        )
        connect_calls += 1
        if connect_calls > 1:
            raise _StopRecorder
        return _ConnectionContext(connection)

    monkeypatch.setattr(websockets, "connect", connect)
    monkeypatch.setattr(trade_capture, "reconnect_delay_seconds", lambda _: 0.0)
    monkeypatch.setattr(
        trade_capture, "BITBANK_DATA_SILENCE_TIMEOUT_SECONDS", 0.05,
    )
    writer = _bitbank_writer(tmp_path, "run-bitbank-pong-timeout")
    stats = trade_capture.CaptureStats("bitbank", "btc_jpy")

    with pytest.raises(_StopRecorder):
        asyncio.run(trade_capture._record_bitbank(writer, stats, None))
    writer.finish(status="interrupted")

    assert connection.sent == ["40", _JOIN, "3"]
    assert stats.wire_frames == stats.control_frames == 3
    assert stats.data_frames == 0
    assert stats.successful_sessions == 1
    assert stats.disconnects == stats.reconnects == 1


def test_bitbank_finite_deadline_bounds_connect(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _Connection([])
    context = _ConnectionContext(connection, enter_silent=True)

    def connect(
        _: str,
        *,
        ping_interval: float | None = None,
        open_timeout: float | None = None,
        close_timeout: float | None = None,
    ) -> _ConnectionContext:
        _assert_bitbank_connect_options(
            ping_interval, open_timeout, close_timeout,
        )
        return context

    monkeypatch.setattr(websockets, "connect", connect)
    writer = _bitbank_writer(tmp_path, "run-bitbank-connect-deadline")
    stats = trade_capture.CaptureStats("bitbank", "btc_jpy")
    started = time.monotonic()

    asyncio.run(trade_capture._record_bitbank(
        writer, stats, time.monotonic() + 0.1,
    ))

    assert time.monotonic() - started < 0.5
    assert stats.connection_attempts == 1
    assert stats.sessions == stats.successful_sessions == 0
    assert stats.reconnects == stats.disconnects == 0
    assert context.exit_calls == 0


def test_bitbank_finite_deadline_bounds_join_send_without_false_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _Connection(
        [_ENGINE_OPEN, _SOCKET_CONNECT], blocked_sends=frozenset({_JOIN}),
    )
    context = _ConnectionContext(connection)

    def connect(
        _: str,
        *,
        ping_interval: float | None = None,
        open_timeout: float | None = None,
        close_timeout: float | None = None,
    ) -> _ConnectionContext:
        _assert_bitbank_connect_options(
            ping_interval, open_timeout, close_timeout,
        )
        return context

    monkeypatch.setattr(websockets, "connect", connect)
    writer = _bitbank_writer(tmp_path, "run-bitbank-send-deadline")
    stats = trade_capture.CaptureStats("bitbank", "btc_jpy")

    asyncio.run(trade_capture._record_bitbank(
        writer, stats, time.monotonic() + 0.5,
    ))
    writer.finish(status="interrupted")

    assert connection.sent == ["40", _JOIN]
    assert stats.sessions == 1
    assert stats.successful_sessions == 0
    assert stats.reconnects == stats.disconnects == 0
    assert context.exit_calls == 1


def test_bitbank_finite_deadline_bounds_backoff_and_counts_no_reconnect(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _Connection(["0{}"])
    connect_calls = 0

    def connect(
        _: str,
        *,
        ping_interval: float | None = None,
        open_timeout: float | None = None,
        close_timeout: float | None = None,
    ) -> _ConnectionContext:
        nonlocal connect_calls
        _assert_bitbank_connect_options(
            ping_interval, open_timeout, close_timeout,
        )
        connect_calls += 1
        return _ConnectionContext(connection)

    monkeypatch.setattr(websockets, "connect", connect)
    monkeypatch.setattr(
        trade_capture, "reconnect_delay_seconds", lambda _: 3600.0,
    )
    writer = _bitbank_writer(tmp_path, "run-bitbank-backoff-deadline")
    stats = trade_capture.CaptureStats("bitbank", "btc_jpy")
    started = time.monotonic()

    asyncio.run(trade_capture._record_bitbank(
        writer, stats, time.monotonic() + 0.1,
    ))
    writer.finish(status="interrupted")

    assert time.monotonic() - started < 0.5
    assert connect_calls == stats.connection_attempts == 1
    assert stats.sessions == 1
    assert stats.successful_sessions == 0
    assert stats.disconnects == 1
    assert stats.reconnects == 0
    assert stats.consecutive_failures == 1


def test_bitbank_underlying_timeout_is_network_failure_not_run_deadline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _Connection(
        [_ENGINE_OPEN, _SOCKET_CONNECT],
        terminal_error=TimeoutError("underlying socket timeout"),
    )
    connect_calls = 0

    def connect(
        _: str,
        *,
        ping_interval: float | None = None,
        open_timeout: float | None = None,
        close_timeout: float | None = None,
    ) -> _ConnectionContext:
        nonlocal connect_calls
        _assert_bitbank_connect_options(
            ping_interval, open_timeout, close_timeout,
        )
        connect_calls += 1
        if connect_calls > 1:
            raise _StopRecorder
        return _ConnectionContext(connection)

    monkeypatch.setattr(websockets, "connect", connect)
    monkeypatch.setattr(trade_capture, "reconnect_delay_seconds", lambda _: 0.0)
    writer = _bitbank_writer(tmp_path, "run-bitbank-underlying-timeout")
    stats = trade_capture.CaptureStats("bitbank", "btc_jpy")

    with pytest.raises(_StopRecorder):
        asyncio.run(trade_capture._record_bitbank(
            writer, stats, time.monotonic() + 1.0,
        ))

    assert connect_calls == stats.connection_attempts == 2
    assert stats.sessions == stats.successful_sessions == 1
    assert stats.disconnects == stats.reconnects == 1
    assert stats.consecutive_failures == 1
    assert stats.cleanup_failures == 0
    writer.finish()


@pytest.mark.parametrize(
    "exit_mode", [
        "ignore_cancel_until_abort",
        "delay_cancel_one_turn",
        "ignore_cancel_and_abort",
    ],
)
def test_bitbank_teardown_hard_bound_aborts_and_reaps_cancel_resistant_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    exit_mode: str,
) -> None:
    connection = _Connection([_ENGINE_OPEN, _SOCKET_CONNECT])
    context = _ConnectionContext(connection, exit_mode=exit_mode)

    def connect(
        _: str,
        *,
        ping_interval: float | None = None,
        open_timeout: float | None = None,
        close_timeout: float | None = None,
    ) -> _ConnectionContext:
        _assert_bitbank_connect_options(
            ping_interval, open_timeout, close_timeout,
        )
        return context

    monkeypatch.setattr(websockets, "connect", connect)
    monkeypatch.setattr(
        trade_capture, "BITBANK_TEARDOWN_TIMEOUT_SECONDS", 0.02,
    )
    writer = _bitbank_writer(tmp_path, f"run-bitbank-hard-close-{exit_mode}")
    stats = trade_capture.CaptureStats("bitbank", "btc_jpy")

    async def run_probe() -> None:
        capture_started = time.monotonic()
        with pytest.raises(_StopRecorder):
            await trade_capture._record_bitbank(writer, stats, None)
        assert time.monotonic() - capture_started < 0.5
        if exit_mode == "ignore_cancel_and_abort":
            assert trade_capture._BITBANK_CLEANUP_TASKS
            context.cleanup_release.set()
        for _ in range(10):
            if not trade_capture._BITBANK_CLEANUP_TASKS:
                break
            await asyncio.sleep(0)
        assert not trade_capture._BITBANK_CLEANUP_TASKS

    started = time.monotonic()
    asyncio.run(run_probe(), debug=True)
    elapsed = time.monotonic() - started

    assert elapsed < 0.5
    assert context.exit_calls == 1
    assert connection.transport.abort_calls == 1
    assert stats.cleanup_failures == 1
    assert stats.last_cleanup_error == (
        "TimeoutError: bitbank cleanup 超过 teardown 预算"
    )
    writer.finish()


def test_bitbank_cancel_and_abort_resistant_cleanup_stops_reconnect_growth(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _Connection(
        [_ENGINE_OPEN, _SOCKET_CONNECT],
        terminal_error=ConnectionError("network dropped"),
    )
    context = _ConnectionContext(
        connection, exit_mode="ignore_cancel_and_abort",
    )
    connect_calls = 0

    def connect(
        _: str,
        *,
        ping_interval: float | None = None,
        open_timeout: float | None = None,
        close_timeout: float | None = None,
    ) -> _ConnectionContext:
        nonlocal connect_calls
        _assert_bitbank_connect_options(
            ping_interval, open_timeout, close_timeout,
        )
        connect_calls += 1
        return context

    monkeypatch.setattr(websockets, "connect", connect)
    monkeypatch.setattr(trade_capture, "reconnect_delay_seconds", lambda _: 0.0)
    monkeypatch.setattr(
        trade_capture, "BITBANK_TEARDOWN_TIMEOUT_SECONDS", 0.02,
    )
    writer = _bitbank_writer(tmp_path, "run-bitbank-no-cleanup-growth")
    stats = trade_capture.CaptureStats("bitbank", "btc_jpy")

    async def run_probe() -> None:
        with pytest.raises(
            trade_capture._BitbankCleanupInvariantFailure,
            match="transport.abort",
        ):
            await trade_capture._record_bitbank(writer, stats, None)
        assert len(trade_capture._BITBANK_CLEANUP_TASKS) == 1
        context.cleanup_release.set()
        for _ in range(3):
            await asyncio.sleep(0)
        assert not trade_capture._BITBANK_CLEANUP_TASKS

    started = time.monotonic()
    asyncio.run(run_probe(), debug=True)

    assert time.monotonic() - started < 0.5
    assert connect_calls == stats.connection_attempts == 1
    assert stats.sessions == stats.successful_sessions == 1
    assert stats.disconnects == 1
    assert stats.reconnects == 0
    assert stats.consecutive_failures == 1
    assert connection.transport.abort_calls == 1
    writer.finish()


def test_bitbank_abort_unblocks_real_websockets_close_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """无网络地验证 websockets 16 close 等待由 Transport.abort 终止。"""

    async def run_probe() -> None:
        protocol = ClientProtocol(
            parse_uri("wss://stream.bitbank.cc/socket.io/"),
            state=State.OPEN,
        )
        connection = ClientConnection(
            protocol, ping_interval=None, close_timeout=3600,
        )

        class AbortTransport(asyncio.Transport):
            def __init__(self) -> None:
                super().__init__()
                self.abort_calls = 0
                self.writes: list[bytes] = []
                self._closing = False

            def abort(self) -> None:
                self.abort_calls += 1
                if not self._closing:
                    self._closing = True
                    asyncio.get_running_loop().call_soon(
                        connection.connection_lost, None,
                    )

            def close(self) -> None:
                self.abort()

            def is_closing(self) -> bool:
                return self._closing

            def write(
                self, data: bytes | bytearray | memoryview[Any],
            ) -> None:
                self.writes.append(bytes(data))

            def can_write_eof(self) -> bool:
                return False

            def pause_reading(self) -> None:
                return None

            def resume_reading(self) -> None:
                return None

            def set_write_buffer_limits(
                self, high: int | None = None, low: int | None = None,
            ) -> None:
                return None

        transport = AbortTransport()
        connection.connection_made(transport)

        class RealCloseContext:
            def __init__(self) -> None:
                self.cancel_seen = False

            async def __aenter__(self) -> ClientConnection:
                return connection

            async def __aexit__(self, *args: object) -> None:
                close_task = asyncio.create_task(connection.close())
                try:
                    await asyncio.shield(close_task)
                except asyncio.CancelledError:
                    self.cancel_seen = True
                    await close_task

        context = RealCloseContext()
        stats = trade_capture.CaptureStats("bitbank", "btc_jpy")
        started = time.monotonic()

        await trade_capture._close_bitbank_connection(
            context, connection, stats, (None, None, None),
        )
        for _ in range(10):
            if not trade_capture._BITBANK_CLEANUP_TASKS:
                break
            await asyncio.sleep(0)

        assert time.monotonic() - started < 0.5
        assert context.cancel_seen
        assert transport.abort_calls == 1
        assert transport.writes
        assert connection.protocol.state is State.CLOSED
        assert connection.connection_lost_waiter.done()
        assert not trade_capture._BITBANK_CLEANUP_TASKS
        assert stats.cleanup_failures == 1
        assert stats.last_cleanup_error == (
            "TimeoutError: bitbank cleanup 超过 teardown 预算"
        )

    monkeypatch.setattr(
        trade_capture, "BITBANK_TEARDOWN_TIMEOUT_SECONDS", 0.02,
    )
    asyncio.run(run_probe(), debug=True)


def test_bitbank_external_cancel_during_cleanup_is_not_swallowed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _Connection(
        [_ENGINE_OPEN, _SOCKET_CONNECT],
        abort_error=RuntimeError("abort must not mask cancel"),
    )
    context = _ConnectionContext(
        connection, exit_mode="ignore_cancel_until_abort",
    )

    def connect(
        _: str,
        *,
        ping_interval: float | None = None,
        open_timeout: float | None = None,
        close_timeout: float | None = None,
    ) -> _ConnectionContext:
        _assert_bitbank_connect_options(
            ping_interval, open_timeout, close_timeout,
        )
        return context

    monkeypatch.setattr(websockets, "connect", connect)
    writer = _bitbank_writer(tmp_path, "run-bitbank-cancel-during-cleanup")
    stats = trade_capture.CaptureStats("bitbank", "btc_jpy")

    async def cancel_cleanup() -> None:
        task = asyncio.create_task(
            trade_capture._record_bitbank(writer, stats, None)
        )
        await context.exit_started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        for _ in range(3):
            await asyncio.sleep(0)
        assert not trade_capture._BITBANK_CLEANUP_TASKS

    asyncio.run(cancel_cleanup(), debug=True)

    assert context.exit_calls == 1
    assert connection.transport.abort_calls == 1
    assert stats.cleanup_failures == 1
    assert stats.last_cleanup_error == (
        "RuntimeError: abort must not mask cancel"
    )
    writer.finish()


@pytest.mark.parametrize(
    "abort_error",
    [
        RuntimeError("abort must not mask storage"),
        SystemExit("abort base must not mask storage"),
    ],
    ids=["runtime-error", "system-exit"],
)
def test_bitbank_abort_error_does_not_mask_writer_oserror(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    abort_error: BaseException,
) -> None:
    connection = _Connection(
        [_ENGINE_OPEN],
        abort_error=abort_error,
    )
    context = _ConnectionContext(
        connection, exit_mode="ignore_cancel_until_abort",
    )

    def connect(
        _: str,
        *,
        ping_interval: float | None = None,
        open_timeout: float | None = None,
        close_timeout: float | None = None,
    ) -> _ConnectionContext:
        _assert_bitbank_connect_options(
            ping_interval, open_timeout, close_timeout,
        )
        return context

    def fail_write(*args: Any, **kwargs: Any) -> None:
        raise OSError("disk still unavailable")

    monkeypatch.setattr(websockets, "connect", connect)
    monkeypatch.setattr(
        trade_capture, "BITBANK_TEARDOWN_TIMEOUT_SECONDS", 0.02,
    )
    writer = _bitbank_writer(tmp_path, "run-bitbank-storage-abort-error")
    monkeypatch.setattr(writer, "write_frame", fail_write)
    stats = trade_capture.CaptureStats("bitbank", "btc_jpy")

    async def run_probe() -> None:
        with pytest.raises(OSError, match="disk still unavailable"):
            await trade_capture._record_bitbank(writer, stats, None)
        for _ in range(3):
            await asyncio.sleep(0)
        assert not trade_capture._BITBANK_CLEANUP_TASKS

    started = time.monotonic()
    asyncio.run(run_probe(), debug=True)

    assert time.monotonic() - started < 0.5
    assert context.exit_calls == 1
    assert connection.transport.abort_calls == 1
    assert stats.connection_attempts == stats.sessions == 1
    assert stats.successful_sessions == stats.reconnects == 0
    assert stats.cleanup_failures == 2
    assert stats.last_cleanup_error == (
        f"{type(abort_error).__name__}: {abort_error}"
    )


@pytest.mark.parametrize(
    "cleanup_error",
    [
        SystemExit("cleanup system exit"),
        KeyboardInterrupt("cleanup keyboard interrupt"),
    ],
    ids=["system-exit", "keyboard-interrupt"],
)
def test_bitbank_cleanup_baseexception_never_masks_writer_primary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    cleanup_error: BaseException,
) -> None:
    connection = _Connection([_ENGINE_OPEN])
    context = _ConnectionContext(connection, exit_error=cleanup_error)

    def connect(
        _: str,
        *,
        ping_interval: float | None = None,
        open_timeout: float | None = None,
        close_timeout: float | None = None,
    ) -> _ConnectionContext:
        _assert_bitbank_connect_options(
            ping_interval, open_timeout, close_timeout,
        )
        return context

    def fail_write(*args: Any, **kwargs: Any) -> None:
        raise OSError("primary disk failure")

    monkeypatch.setattr(websockets, "connect", connect)
    writer = _bitbank_writer(tmp_path, "run-bitbank-cleanup-base-primary")
    monkeypatch.setattr(writer, "write_frame", fail_write)
    stats = trade_capture.CaptureStats("bitbank", "btc_jpy")

    with pytest.raises(OSError, match="primary disk failure"):
        asyncio.run(trade_capture._record_bitbank(writer, stats, None))

    assert context.exit_calls == 1
    assert stats.cleanup_failures == 1
    assert stats.last_cleanup_error == (
        f"{type(cleanup_error).__name__}: {cleanup_error}"
    )


def test_bitbank_cleanup_systemexit_never_masks_cancel_primary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _Connection(
        [_ENGINE_OPEN, _SOCKET_CONNECT], silent=True,
    )
    context = _ConnectionContext(
        connection, exit_error=SystemExit("cleanup must not mask cancel"),
    )

    def connect(
        _: str,
        *,
        ping_interval: float | None = None,
        open_timeout: float | None = None,
        close_timeout: float | None = None,
    ) -> _ConnectionContext:
        _assert_bitbank_connect_options(
            ping_interval, open_timeout, close_timeout,
        )
        return context

    monkeypatch.setattr(websockets, "connect", connect)
    writer = _bitbank_writer(tmp_path, "run-bitbank-cleanup-base-cancel")
    stats = trade_capture.CaptureStats("bitbank", "btc_jpy")

    async def cancel_capture() -> None:
        task = asyncio.create_task(
            trade_capture._record_bitbank(writer, stats, None)
        )
        while stats.successful_sessions == 0:
            await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(cancel_capture())

    assert context.exit_calls == 1
    assert stats.cleanup_failures == 1
    assert stats.last_cleanup_error == (
        "SystemExit: cleanup must not mask cancel"
    )
    writer.finish()


@pytest.mark.parametrize(
    "cleanup_error",
    [
        SystemExit("cleanup system exit without primary"),
        asyncio.CancelledError("cleanup cancelled without primary"),
    ],
    ids=["system-exit", "cancelled-error"],
)
def test_bitbank_cleanup_baseexception_propagates_without_body_primary(
    cleanup_error: BaseException,
) -> None:
    connection = _Connection([])
    context = _ConnectionContext(connection, exit_error=cleanup_error)
    stats = trade_capture.CaptureStats("bitbank", "btc_jpy")

    async def close() -> None:
        await trade_capture._close_bitbank_connection(
            context, connection, stats, (None, None, None),
        )

    with pytest.raises(type(cleanup_error), match=str(cleanup_error)):
        asyncio.run(close())

    assert context.exit_calls == 1
    assert connection.transport.abort_calls == 0
    assert stats.cleanup_failures == 1
    assert stats.last_cleanup_error == (
        f"{type(cleanup_error).__name__}: {cleanup_error}"
    )


def test_bitbank_cleanup_exception_is_recorded_without_body_primary() -> None:
    connection = _Connection([])
    context = _ConnectionContext(
        connection, exit_error=RuntimeError("ordinary cleanup failure"),
    )
    stats = trade_capture.CaptureStats("bitbank", "btc_jpy")

    detached = asyncio.run(trade_capture._close_bitbank_connection(
        context, connection, stats, (None, None, None),
    ))

    assert not detached
    assert context.exit_calls == 1
    assert connection.transport.abort_calls == 0
    assert stats.cleanup_failures == 1
    assert stats.last_cleanup_error == (
        "RuntimeError: ordinary cleanup failure"
    )


def test_bitbank_cleanup_ownership_isolated_across_concurrent_symbols() -> None:
    async def probe() -> None:
        releases = (asyncio.Event(), asyncio.Event())
        owners = (
            trade_capture._bitbank_cleanup_owner("btc_jpy"),
            trade_capture._bitbank_cleanup_owner("eth_jpy"),
        )
        stats = (
            trade_capture.CaptureStats("bitbank", "btc_jpy"),
            trade_capture.CaptureStats("bitbank", "eth_jpy"),
        )

        async def cleanup(release: asyncio.Event) -> BaseException | None:
            await release.wait()
            return None

        tasks = tuple(
            asyncio.create_task(cleanup(release)) for release in releases
        )
        for task, owner, capture_stats in zip(tasks, owners, stats, strict=True):
            trade_capture._observe_bitbank_cleanup_task(
                task, capture_stats, owner,
                own_cancel=False, primary_present=True,
            )
        assert all(
            trade_capture._bitbank_cleanup_pending(owner) for owner in owners
        )
        assert owners[0].tasks == {tasks[0]}
        assert owners[1].tasks == {tasks[1]}
        releases[0].set()
        await tasks[0]
        await asyncio.sleep(0)
        assert not trade_capture._bitbank_cleanup_pending(owners[0])
        assert trade_capture._bitbank_cleanup_pending(owners[1])
        releases[1].set()
        await tasks[1]
        await asyncio.sleep(0)
        assert not trade_capture._BITBANK_CLEANUP_TASKS

    asyncio.run(probe(), debug=True)


def test_bitbank_cleanup_owner_never_crosses_event_loops() -> None:
    def one_loop() -> tuple[asyncio.AbstractEventLoop, object]:
        async def probe() -> tuple[asyncio.AbstractEventLoop, object]:
            owner = trade_capture._bitbank_cleanup_owner("btc_jpy")

            async def cleanup() -> BaseException | None:
                return None

            task = asyncio.create_task(cleanup())
            trade_capture._observe_bitbank_cleanup_task(
                task,
                trade_capture.CaptureStats("bitbank", "btc_jpy"),
                owner,
                own_cancel=False,
                primary_present=True,
            )
            await task
            await asyncio.sleep(0)
            assert not trade_capture._bitbank_cleanup_pending(owner)
            return owner.loop, owner.token

        return asyncio.run(probe(), debug=True)

    first_loop, first_token = one_loop()
    second_loop, second_token = one_loop()
    assert first_loop is not second_loop
    assert first_token is not second_token
    assert not trade_capture._BITBANK_CLEANUP_TASKS


def test_bitbank_abort_baseexception_propagates_without_body_primary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _Connection(
        [], abort_error=SystemExit("abort system exit without primary"),
    )
    context = _ConnectionContext(
        connection, exit_mode="ignore_cancel_and_abort",
    )
    stats = trade_capture.CaptureStats("bitbank", "btc_jpy")
    monkeypatch.setattr(
        trade_capture, "BITBANK_TEARDOWN_TIMEOUT_SECONDS", 0.02,
    )

    async def close() -> None:
        with pytest.raises(SystemExit, match="abort system exit"):
            await trade_capture._close_bitbank_connection(
                context, connection, stats, (None, None, None),
            )
        assert len(trade_capture._BITBANK_CLEANUP_TASKS) == 1
        context.cleanup_release.set()
        for _ in range(3):
            await asyncio.sleep(0)
        assert not trade_capture._BITBANK_CLEANUP_TASKS

    asyncio.run(close(), debug=True)

    assert connection.transport.abort_calls == 1
    assert stats.cleanup_failures == 2
    assert stats.last_cleanup_error == (
        "SystemExit: abort system exit without primary"
    )


def test_bitbank_writer_oserror_propagates_without_network_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _Connection([_ENGINE_OPEN])
    context = _ConnectionContext(
        connection, exit_error=RuntimeError("cleanup must not mask storage"),
    )
    connect_calls = 0

    def connect(
        _: str,
        *,
        ping_interval: float | None = None,
        open_timeout: float | None = None,
        close_timeout: float | None = None,
    ) -> _ConnectionContext:
        nonlocal connect_calls
        _assert_bitbank_connect_options(
            ping_interval, open_timeout, close_timeout,
        )
        connect_calls += 1
        return context

    def fail_write(*args: Any, **kwargs: Any) -> None:
        raise OSError("disk unavailable")

    monkeypatch.setattr(websockets, "connect", connect)
    writer = _bitbank_writer(tmp_path, "run-bitbank-storage-error")
    monkeypatch.setattr(writer, "write_frame", fail_write)
    stats = trade_capture.CaptureStats("bitbank", "btc_jpy")

    with pytest.raises(OSError, match="disk unavailable"):
        asyncio.run(trade_capture._record_bitbank(writer, stats, None))

    assert connect_calls == stats.connection_attempts == 1
    assert stats.sessions == 1
    assert stats.successful_sessions == 0
    assert stats.wire_frames == stats.control_frames == 0
    assert stats.reconnects == stats.disconnects == 0
    assert stats.consecutive_failures == 0
    assert stats.cleanup_failures == 1
    assert stats.last_cleanup_error == (
        "RuntimeError: cleanup must not mask storage"
    )


def test_bitbank_cancelled_error_propagates_without_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _Connection(
        [_ENGINE_OPEN, _SOCKET_CONNECT], silent=True,
    )
    context = _ConnectionContext(
        connection, exit_error=RuntimeError("cleanup must not mask cancel"),
    )

    def connect(
        _: str,
        *,
        ping_interval: float | None = None,
        open_timeout: float | None = None,
        close_timeout: float | None = None,
    ) -> _ConnectionContext:
        _assert_bitbank_connect_options(
            ping_interval, open_timeout, close_timeout,
        )
        return context

    monkeypatch.setattr(websockets, "connect", connect)
    writer = _bitbank_writer(tmp_path, "run-bitbank-cancel")
    stats = trade_capture.CaptureStats("bitbank", "btc_jpy")

    async def cancel_capture() -> None:
        task = asyncio.create_task(
            trade_capture._record_bitbank(writer, stats, None)
        )
        while stats.successful_sessions == 0:
            await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(cancel_capture())

    assert stats.connection_attempts == 1
    assert stats.sessions == stats.successful_sessions == 1
    assert stats.reconnects == stats.disconnects == 0
    assert stats.consecutive_failures == 0
    assert context.exit_calls == 1
    assert stats.cleanup_failures == 1
    assert stats.last_cleanup_error == (
        "RuntimeError: cleanup must not mask cancel"
    )
    writer.finish()
