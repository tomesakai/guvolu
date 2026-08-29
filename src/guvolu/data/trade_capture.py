"""GMO、bitbank、bitFlyer 的公开逐笔分段采集。"""
from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import json
import re
import sys
import time
import weakref
from collections import deque
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from types import TracebackType
from typing import Protocol, TypeVar, cast, runtime_checkable

import websockets
from websockets.exceptions import WebSocketException

from guvolu.api.ws_common import reconnect_delay_seconds, to_text
from guvolu.api.ws_public import PUBLIC_WS_URL as GMO_WS_URL
from guvolu.data.paths import data_root as configured_data_root
from guvolu.data.segmented_raw import (
    SegmentedRawWriter,
    recover_open_segments,
    supervise_capture_tasks,
)
from guvolu.venues.bitbank_stream import PUBLIC_WS_URL as BITBANK_WS_URL

BITFLYER_WS_URL = "wss://ws.lightstream.bitflyer.com/json-rpc"
# bitbank 的
# wire-silence 预算按每个
# connect/recv/send 操作
# 重新开始；有限 run
# deadline 始终是更严格的上限。
# 业务数据预算独立计算，协议心跳不续期。
SILENCE_TIMEOUT_SECONDS = 90.0
GMO_TRANSPORT_PROBE_TIMEOUT_SECONDS = 15.0
BITBANK_DATA_SILENCE_TIMEOUT_SECONDS = 300.0
# run deadline 停止业务
# I/O 后，仅允许这段独立、有界的
# 连接清理宽限。
BITBANK_TEARDOWN_TIMEOUT_SECONDS = 1.0
# transport.abort 后仍给
# 公开 websockets close
# 路径一个明确、有限的完成宽限。
BITBANK_ABORT_COMPLETION_GRACE_SECONDS = 0.25
BITBANK_MIN_EVENT_TIME = datetime(2017, 1, 1, tzinfo=UTC)
# 超过业务静默预算的成交
# 不能证明当前流仍健康；
# 30 秒仅容纳小幅时钟偏差。
BITBANK_MAX_EVENT_AGE = timedelta(
    seconds=BITBANK_DATA_SILENCE_TIMEOUT_SECONDS,
)
BITBANK_MAX_FUTURE_SKEW = timedelta(seconds=30)
BITBANK_DECIMAL_TEXT_MAX_CHARS = 64
BITBANK_RECENT_TRANSACTION_LIMIT = 4096
BITBANK_EPOCH_RESET_CONFIRMATION_FRAMES = 2
BITBANK_BINARY_ENVELOPE_VERSION = 1
_BITBANK_DECIMAL_TEXT = re.compile(
    r"(?:0|[1-9][0-9]*)(?:\.[0-9]+)?", re.ASCII,
)
CHECKPOINT_SECONDS = 60.0
ENDPOINT_BINDINGS = {
    "gmo": ("EP-0007", 1),
    "bitbank": ("EP-0075", 0),
    "bitflyer": ("EP-0002", 0),
}


@dataclass
class CaptureStats:
    """一个实时逐笔 run 的健康计数。"""

    venue_id: str
    venue_symbol: str
    wire_frames: int = 0
    data_frames: int = 0
    control_frames: int = 0
    sessions: int = 0
    reconnects: int = 0
    connection_attempts: int = 0
    successful_sessions: int = 0
    disconnects: int = 0
    consecutive_failures: int = 0
    last_wire_time: str | None = None
    last_data_time: str | None = None
    transport_probes: int = 0
    transport_probe_successes: int = 0
    transport_probe_failures: int = 0
    last_transport_health_time: str | None = None
    cleanup_failures: int = 0
    last_cleanup_error: str | None = None


def _receive_clock() -> tuple[str, int]:
    return datetime.now(UTC).isoformat(), time.monotonic_ns()


def _opened_connection(writer: SegmentedRawWriter, stats: CaptureStats) -> str:
    stats.sessions += 1
    stats.successful_sessions += 1
    return f"{writer.run_id}-c{stats.successful_sessions:06d}"


def _json_mapping(text: str) -> Mapping[str, object] | None:
    try:
        loaded = json.loads(text)
    except json.JSONDecodeError:
        return None
    if not isinstance(loaded, Mapping):
        return None
    return cast(Mapping[str, object], loaded)


def _mapping_channel(
    payload: Mapping[str, object] | None, fallback: str,
) -> str:
    if payload is None:
        return fallback
    channel = payload.get("channel")
    return channel if isinstance(channel, str) and channel else fallback


def _bitbank_channel_id(text: str) -> str:
    if not text.startswith("42"):
        return "protocol_control"
    try:
        packet = json.loads(text[2:])
    except ValueError:
        return "protocol_control"
    if not isinstance(packet, list) or len(packet) < 2:
        return "protocol_control"
    envelope = packet[1]
    if not isinstance(envelope, Mapping):
        return "protocol_control"
    room = envelope.get("room_name")
    return room if isinstance(room, str) and room else "protocol_control"


def _positive_protocol_integer(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _bitbank_engine_open(text: str) -> bool:
    """严格识别 bitbank 当前 Engine.IO v4 open packet。"""
    if not text.startswith("0{"):
        return False
    try:
        packet = json.loads(text[1:])
    except ValueError:
        return False
    if not isinstance(packet, Mapping):
        return False
    sid = packet.get("sid")
    upgrades = packet.get("upgrades")
    return (
        isinstance(sid, str)
        and bool(sid)
        and isinstance(upgrades, list)
        and all(isinstance(item, str) for item in upgrades)
        and _positive_protocol_integer(packet.get("pingInterval"))
        and _positive_protocol_integer(packet.get("pingTimeout"))
        and _positive_protocol_integer(packet.get("maxPayload"))
    )


def _bitbank_socket_connect(text: str) -> bool:
    """严格识别 bitbank root namespace 的 Socket.IO connect ack。"""
    if not text.startswith("40{"):
        return False
    try:
        packet = json.loads(text[2:])
    except ValueError:
        return False
    if not isinstance(packet, Mapping):
        return False
    sid = packet.get("sid")
    return isinstance(sid, str) and bool(sid)


def _bitbank_positive_decimal(value: object) -> bool:
    """验证 bitbank 合同中的有界、正、有限十进制定点字符串。"""
    if not isinstance(value, str):
        return False
    if not 0 < len(value) <= BITBANK_DECIMAL_TEXT_MAX_CHARS:
        return False
    if _BITBANK_DECIMAL_TEXT.fullmatch(value) is None:
        return False
    try:
        parsed = Decimal(value)
    except (InvalidOperation, ValueError):
        return False
    return parsed.is_finite() and parsed > 0


def _bitbank_event_datetime(
    value: object, *, maximum: datetime,
) -> datetime | None:
    """解析正整数 epoch milliseconds，并在时间转换前做有界检查。"""
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        return None
    minimum_ms = int(BITBANK_MIN_EVENT_TIME.timestamp()) * 1000
    maximum_ms = int(maximum.timestamp() * 1000)
    if not minimum_ms <= value <= maximum_ms:
        return None
    seconds, milliseconds = divmod(value, 1000)
    try:
        event_time = datetime.fromtimestamp(seconds, UTC) + timedelta(
            milliseconds=milliseconds,
        )
    except (OSError, OverflowError, ValueError):
        return None
    if not BITBANK_MIN_EVENT_TIME <= event_time <= maximum:
        return None
    return event_time


def _bitbank_event_time(value: object) -> bool:
    """验证合同中的 epoch 毫秒及很小的未来时钟偏差。"""
    return _bitbank_event_datetime(
        value, maximum=datetime.now(UTC) + BITBANK_MAX_FUTURE_SKEW,
    ) is not None


def _bitbank_transaction_details(
    item: object, *, maximum_event_time: datetime,
) -> tuple[int, datetime] | None:
    """返回健康游标所需字段；任一业务字段非法则整项失败。"""
    if not isinstance(item, Mapping):
        return None
    transaction_id = item.get("transaction_id")
    side = item.get("side")
    if not (
        isinstance(transaction_id, int)
        and not isinstance(transaction_id, bool)
        and transaction_id > 0
        and isinstance(side, str)
        and side in {"buy", "sell"}
        and _bitbank_positive_decimal(item.get("price"))
        and _bitbank_positive_decimal(item.get("amount"))
    ):
        return None
    event_time = _bitbank_event_datetime(
        item.get("executed_at"), maximum=maximum_event_time,
    )
    if event_time is None:
        return None
    return transaction_id, event_time


def _bitbank_transaction(item: object) -> bool:
    """按当前 bitbank transactions stream 的逐笔 schema 完整验证。"""
    return _bitbank_transaction_details(
        item,
        maximum_event_time=datetime.now(UTC) + BITBANK_MAX_FUTURE_SKEW,
    ) is not None


@dataclass(slots=True)
class _BitbankEpochCandidate:
    """低位新 epoch 的连续、时间前进证据。"""

    maximum_id: int
    latest_event_time: datetime
    frames: int


@dataclass(slots=True)
class _BitbankProgressState:
    """有界去重与受控 epoch reset；不假设 transaction_id 数值上界。"""

    high_id: int | None = None
    latest_event_time: datetime | None = None
    epoch: int = 0
    reset_candidate: _BitbankEpochCandidate | None = None
    _recent_order: deque[tuple[int, datetime]] = field(default_factory=deque)
    _recent: set[tuple[int, datetime]] = field(default_factory=set)

    def _remember(self, identities: Sequence[tuple[int, datetime]]) -> None:
        for identity in identities:
            if identity in self._recent:
                continue
            self._recent.add(identity)
            self._recent_order.append(identity)
            while len(self._recent_order) > BITBANK_RECENT_TRANSACTION_LIMIT:
                expired = self._recent_order.popleft()
                self._recent.discard(expired)

    def observe(
        self,
        details: Sequence[tuple[int, datetime]],
        *,
        oldest_healthy_time: datetime,
    ) -> bool:
        """只让新鲜、未见且有序证据刷新业务 watchdog。"""
        fresh = [
            detail for detail in details
            if detail[1] >= oldest_healthy_time
        ]
        if not fresh:
            self.reset_candidate = None
            return False
        if any(identity in self._recent for identity in fresh):
            # 重连 replay 或候选帧重放
            # 不能成为第二份 reset 证据。
            self.reset_candidate = None
            self._remember(fresh)
            return False

        ids = [identity[0] for identity in fresh]
        event_times = [identity[1] for identity in fresh]
        maximum_id = max(ids)
        latest_event_time = max(event_times)
        minimum_event_time = min(event_times)
        if self.high_id is None or maximum_id > self.high_id:
            self.high_id = maximum_id
            if (
                self.latest_event_time is None
                or latest_event_time > self.latest_event_time
            ):
                self.latest_event_time = latest_event_time
            self.reset_candidate = None
            self._remember(fresh)
            return True

        # 全部 ID 都落在当前高水位以下。
        # 只有 event time 严格前进、
        # 且两帧低位 ID 连续推进，
        # 才承认交易所 epoch reset。
        # 单个异常大 ID 因此最多
        # 压制一份候选帧，而 replay
        # 不能虚假续期。
        if (
            self.latest_event_time is None
            or minimum_event_time <= self.latest_event_time
        ):
            self.reset_candidate = None
            self._remember(fresh)
            return False
        candidate = self.reset_candidate
        if candidate is None:
            self.reset_candidate = _BitbankEpochCandidate(
                maximum_id=maximum_id,
                latest_event_time=latest_event_time,
                frames=1,
            )
            self._remember(fresh)
            return False
        if (
            minimum_event_time <= candidate.latest_event_time
            or maximum_id <= candidate.maximum_id
        ):
            self.reset_candidate = None
            self._remember(fresh)
            return False
        frames = candidate.frames + 1
        self._remember(fresh)
        if frames < BITBANK_EPOCH_RESET_CONFIRMATION_FRAMES:
            self.reset_candidate = _BitbankEpochCandidate(
                maximum_id=maximum_id,
                latest_event_time=latest_event_time,
                frames=frames,
            )
            return False
        self.epoch += 1
        self.high_id = maximum_id
        self.latest_event_time = latest_event_time
        self.reset_candidate = None
        return True


def _bitbank_trade_frame(
    text: str,
    room: str,
    received_at: datetime,
    after_transaction_id: int | None,
    *,
    progress_state: _BitbankProgressState | None = None,
) -> tuple[str, bool, int | None]:
    """仅以新鲜且推进进程内游标的完整目标帧证明数据健康。"""
    channel_id = _bitbank_channel_id(text)
    if not text.startswith("42["):
        return channel_id, False, after_transaction_id
    try:
        packet = json.loads(text[2:])
    except ValueError:
        return channel_id, False, after_transaction_id
    if not isinstance(packet, list) or len(packet) != 2:
        return channel_id, False, after_transaction_id
    event, envelope = packet
    if event != "message" or not isinstance(envelope, Mapping):
        return channel_id, False, after_transaction_id
    if envelope.get("room_name") != room:
        return channel_id, False, after_transaction_id
    message = envelope.get("message")
    data = message.get("data") if isinstance(message, Mapping) else None
    transactions = data.get("transactions") if isinstance(data, Mapping) else None
    if not isinstance(transactions, list) or not transactions:
        return channel_id, False, after_transaction_id
    if received_at.tzinfo is None:
        return channel_id, False, after_transaction_id
    received_utc = received_at.astimezone(UTC)
    maximum_event_time = received_utc + BITBANK_MAX_FUTURE_SKEW
    details: list[tuple[int, datetime]] = []
    for item in transactions:
        detail = _bitbank_transaction_details(
            item, maximum_event_time=maximum_event_time,
        )
        if detail is None:
            # 混合列表必须整帧 fail，
            # 且绝不能推进游标。
            return channel_id, False, after_transaction_id
        details.append(detail)
    oldest_healthy_time = received_utc - BITBANK_MAX_EVENT_AGE
    state = progress_state
    if state is None:
        state = _BitbankProgressState(high_id=after_transaction_id)
    progressed = state.observe(
        details, oldest_healthy_time=oldest_healthy_time,
    )
    return channel_id, progressed, state.high_id


def _bitflyer_channel_id(payload: Mapping[str, object] | None) -> str:
    if payload is None:
        return "protocol_control"
    params = payload.get("params")
    if not isinstance(params, Mapping):
        return "protocol_control"
    channel = params.get("channel")
    return channel if isinstance(channel, str) and channel else "protocol_control"


def _observed_data(stats: CaptureStats, recv_ts_utc: str) -> None:
    stats.data_frames += 1
    stats.last_data_time = recv_ts_utc
    stats.consecutive_failures = 0


def _active(deadline: float | None) -> bool:
    return deadline is None or time.monotonic() < deadline


def _remaining(deadline: float | None) -> float:
    if deadline is None:
        return SILENCE_TIMEOUT_SECONDS
    return min(SILENCE_TIMEOUT_SECONDS, max(0.05, deadline - time.monotonic()))


def _strict_operation_budget(
    deadline: float | None, maximum_seconds: float,
) -> float:
    if deadline is None:
        return max(0.0, maximum_seconds)
    return min(max(0.0, maximum_seconds), max(0.0, deadline - time.monotonic()))


def _bounded_reconnect_delay(consecutive_failures: int) -> float:
    """在 ws_common 求指数前先截断，连续故障不会数值溢出。"""
    return reconnect_delay_seconds(min(max(0, consecutive_failures), 63))


def _gmo_error_frame(payload: Mapping[str, object] | None) -> bool:
    """识别 GMO 错误响应；调用方必须先持久化原帧再触发重连。"""
    if payload is None:
        return False
    error = payload.get("error")
    errors = payload.get("errors")
    return error not in (None, False, "", []) or errors not in (
        None, False, "", []
    )


class _GmoConnection(Protocol):
    async def recv(self) -> str | bytes: ...

    async def send(self, message: str) -> None: ...

    async def ping(self) -> Awaitable[float]: ...


async def _gmo_transport_probe(
    connection: _GmoConnection,
    stats: CaptureStats,
    deadline: float | None,
) -> bool:
    """有界等待 ping/pong；真表示 transport 健康，假仅表示 run 到期。"""
    budget = _strict_operation_budget(
        deadline, GMO_TRANSPORT_PROBE_TIMEOUT_SECONDS,
    )
    if budget <= 0:
        return False
    stats.transport_probes += 1
    timeout_scope = asyncio.timeout(budget)
    try:
        async with timeout_scope:
            pong_waiter = await connection.ping()
            await pong_waiter
    except TimeoutError:
        if timeout_scope.expired() and not _active(deadline):
            return False
        stats.transport_probe_failures += 1
        raise ConnectionError("GMO trades transport ping/pong 超时") from None
    except (OSError, ConnectionError, WebSocketException) as exc:
        stats.transport_probe_failures += 1
        raise ConnectionError(
            f"GMO trades transport ping/pong 失败: {type(exc).__name__}: {exc}"
        ) from exc
    stats.transport_probe_successes += 1
    stats.last_transport_health_time = datetime.now(UTC).isoformat()
    return True


async def _record_gmo(
    writer: SegmentedRawWriter, stats: CaptureStats, deadline: float | None,
) -> None:
    while _active(deadline):
        stats.connection_attempts += 1
        session_opened = False
        try:
            async with websockets.connect(GMO_WS_URL) as websocket:
                connection = cast(_GmoConnection, websocket)
                session_opened = True
                connection_id = _opened_connection(writer, stats)
                await connection.send(json.dumps({
                    "command": "subscribe", "channel": "trades",
                    "symbol": writer.venue_symbol, "option": "TAKER_ONLY",
                }))
                while _active(deadline):
                    try:
                        recv_budget = _strict_operation_budget(
                            deadline, SILENCE_TIMEOUT_SECONDS,
                        )
                        if recv_budget <= 0:
                            return
                        raw = await asyncio.wait_for(
                            connection.recv(), recv_budget,
                        )
                    except TimeoutError:
                        if not _active(deadline):
                            return
                        if await _gmo_transport_probe(
                            connection, stats, deadline,
                        ):
                            # transport pong 不属于
                            # 业务数据，也不刷新 wire/data
                            # freshness；低活跃市场
                            # 保持订阅等待下一帧。
                            continue
                        return
                    recv_ts_utc, recv_ts_mono_ns = _receive_clock()
                    text = to_text(raw)
                    payload = _json_mapping(text)
                    writer.write_frame(
                        text, "trades/ws", connection_id=connection_id,
                        channel_id=_mapping_channel(payload, "trades"),
                        recv_ts_utc=recv_ts_utc,
                        recv_ts_mono_ns=recv_ts_mono_ns,
                    )
                    stats.wire_frames += 1
                    stats.last_wire_time = recv_ts_utc
                    if _gmo_error_frame(payload):
                        stats.control_frames += 1
                        raise ConnectionError("GMO trades 返回错误帧")
                    if payload is None:
                        stats.control_frames += 1
                    else:
                        if payload.get("channel") == "trades":
                            _observed_data(stats, recv_ts_utc)
                        else:
                            stats.control_frames += 1
        except (OSError, ConnectionError, WebSocketException):
            if not _active(deadline):
                return
            if session_opened:
                stats.disconnects += 1
            stats.consecutive_failures += 1
            stats.reconnects += 1
            await asyncio.sleep(
                _bounded_reconnect_delay(stats.consecutive_failures)
            )


class _BitbankConnection(Protocol):
    async def recv(self) -> str | bytes: ...

    async def send(self, message: str) -> None: ...


@runtime_checkable
class _BitbankAbortableTransport(Protocol):
    def abort(self) -> None: ...


@runtime_checkable
class _BitbankConnectionWithTransport(Protocol):
    @property
    def transport(self) -> _BitbankAbortableTransport: ...


class _BitbankConnectionManager(Protocol):
    async def __aenter__(self) -> _BitbankConnection: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None: ...


_T = TypeVar("_T")
_BITBANK_CLEANUP_TASKS: weakref.WeakSet[
    asyncio.Task[BaseException | None]
] = weakref.WeakSet()


@dataclass(slots=True)
class _BitbankCleanupOwner:
    """把尾部 cleanup 严格绑定到 event loop、symbol 与 recorder。"""

    loop: asyncio.AbstractEventLoop
    symbol: str
    token: object = field(default_factory=object)
    tasks: set[asyncio.Task[BaseException | None]] = field(default_factory=set)


def _bitbank_cleanup_owner(symbol: str) -> _BitbankCleanupOwner:
    loop = asyncio.get_running_loop()
    return _BitbankCleanupOwner(loop, symbol)


def _bitbank_cleanup_pending(owner: _BitbankCleanupOwner) -> bool:
    return bool(owner.tasks)


class _BitbankDeadlineElapsed(Exception):
    """有限采集窗口耗尽；这不是网络故障或重连。"""


class _BitbankPersistenceFailure(Exception):
    """隔离本地持久化 OSError，防止外层网络重试吞掉它。"""

    def __init__(self, error: OSError) -> None:
        super().__init__(str(error))
        self.error = error


class _BitbankCleanupInvariantFailure(RuntimeError):
    """transport.abort 后 cleanup 仍悬挂；禁止继续重连累积任务。"""


def _bitbank_operation_budget(
    deadline: float | None, maximum_seconds: float,
) -> float:
    """计算单次 I/O 上限；不以最小 sleep 偷越有限 run deadline。"""
    maximum = max(0.0, maximum_seconds)
    if deadline is None:
        return maximum
    return min(maximum, max(0.0, deadline - time.monotonic()))


async def _bitbank_wait(
    operation: Awaitable[_T], deadline: float | None, label: str,
    *, maximum_seconds: float | None = None,
) -> _T:
    """为一个 connect/recv/send 操作同时施加 wire 与 run 预算。"""
    maximum = (
        SILENCE_TIMEOUT_SECONDS
        if maximum_seconds is None
        else maximum_seconds
    )
    run_remaining = (
        None if deadline is None else max(0.0, deadline - time.monotonic())
    )
    run_limited = run_remaining is not None and run_remaining <= max(0.0, maximum)
    timeout = _bitbank_operation_budget(deadline, maximum)
    timeout_scope = asyncio.timeout(timeout)
    try:
        async with timeout_scope:
            return await operation
    except TimeoutError:
        # Timeout.expired 是
        # 来源标签；底层自己抛出的
        # TimeoutError 必须原样进入
        # 网络故障路径，不能被有限
        # run deadline 静默吞掉。
        if not timeout_scope.expired():
            raise
        if run_limited:
            raise _BitbankDeadlineElapsed from None
        raise ConnectionError(f"bitbank trades {label}超时") from None


def _write_bitbank_frame(
    writer: SegmentedRawWriter,
    text: str,
    connection_id: str,
    channel_id: str,
    recv_ts_utc: str,
    recv_ts_mono_ns: int,
) -> None:
    """持久化 bitbank wire 原文；存储失败必须越过网络重试层。"""
    try:
        writer.write_frame(
            text, "transactions", connection_id=connection_id,
            channel_id=channel_id, recv_ts_utc=recv_ts_utc,
            recv_ts_mono_ns=recv_ts_mono_ns,
        )
    except OSError as exc:
        raise _BitbankPersistenceFailure(exc) from exc


def _bitbank_raw_evidence(raw: str | bytes) -> tuple[str, bool]:
    """在任何 UTF-8 解码前把 binary frame 转成可逆原始层 envelope。"""
    if isinstance(raw, str):
        return raw, False
    encoded = base64.b64encode(raw).decode("ascii")
    envelope = {
        "wire_envelope_version": BITBANK_BINARY_ENVELOPE_VERSION,
        "opcode": "binary",
        "encoding": "base64",
        "byte_count": len(raw),
        "payload_sha256": hashlib.sha256(raw).hexdigest(),
        "payload_base64": encoded,
    }
    return json.dumps(envelope, separators=(",", ":"), sort_keys=True), True


def _record_bitbank_cleanup_failure(
    stats: CaptureStats, error: BaseException,
) -> None:
    stats.cleanup_failures += 1
    stats.last_cleanup_error = f"{type(error).__name__}: {error}"


def _abort_bitbank_transport(
    connection: _BitbankConnection,
    stats: CaptureStats,
    *,
    primary_present: bool,
) -> None:
    """以 asyncio Transport.abort 的公开同步接口强制终止底层连接。"""
    if not isinstance(connection, _BitbankConnectionWithTransport):
        _record_bitbank_cleanup_failure(
            stats, RuntimeError("bitbank connection 缺少可强制终止 transport"),
        )
        return
    try:
        connection.transport.abort()
    except Exception as exc:
        _record_bitbank_cleanup_failure(stats, exc)
    except BaseException as exc:
        _record_bitbank_cleanup_failure(stats, exc)
        if not primary_present:
            raise


async def _capture_bitbank_cleanup(
    manager: _BitbankConnectionManager,
    exc_info: tuple[
        type[BaseException] | None, BaseException | None, TracebackType | None,
    ],
) -> BaseException | None:
    """在子任务内部捕获全部 BaseException，避免其抢占主任务异常。"""
    try:
        await manager.__aexit__(*exc_info)
    except BaseException as exc:
        return exc
    return None


def _consume_bitbank_cleanup_task(
    task: asyncio.Task[BaseException | None],
    stats: CaptureStats,
    *,
    own_cancel: bool,
    primary_present: bool,
) -> None:
    """普通异常记账；cleanup BaseException 绝不替换已有 primary。"""
    try:
        error = task.result()
    except asyncio.CancelledError as exc:
        error = exc
    except BaseException as exc:
        # 捕获包装器理论上不会走到这里；
        # 仍以同一仲裁合同 fail-safe。
        error = exc
    if error is None:
        return
    if isinstance(error, asyncio.CancelledError) and own_cancel:
        return
    if isinstance(error, Exception) or primary_present:
        _record_bitbank_cleanup_failure(stats, error)
        return
    raise error


def _observe_bitbank_cleanup_task(
    task: asyncio.Task[BaseException | None],
    stats: CaptureStats,
    owner: _BitbankCleanupOwner,
    *,
    own_cancel: bool,
    primary_present: bool,
) -> None:
    """观察已强制 abort 后的尾部任务，防止异常未取回。"""
    if task.get_loop() is not owner.loop:
        raise RuntimeError("bitbank cleanup task 与 owner event loop 不一致")
    owner.tasks.add(task)
    _BITBANK_CLEANUP_TASKS.add(task)

    def completed(
        completed_task: asyncio.Task[BaseException | None],
    ) -> None:
        owner.tasks.discard(completed_task)
        _BITBANK_CLEANUP_TASKS.discard(completed_task)
        try:
            _consume_bitbank_cleanup_task(
                completed_task,
                stats,
                own_cancel=own_cancel,
                primary_present=primary_present,
            )
        except BaseException as exc:
            completed_task.get_loop().call_exception_handler({
                "message": "bitbank detached cleanup raised BaseException",
                "exception": exc,
                "task": completed_task,
            })

    task.add_done_callback(completed)


async def _close_bitbank_connection(
    manager: _BitbankConnectionManager,
    connection: _BitbankConnection,
    stats: CaptureStats,
    exc_info: tuple[
        type[BaseException] | None, BaseException | None, TracebackType | None,
    ],
    owner: _BitbankCleanupOwner | None = None,
) -> bool:
    """竞速 cleanup 与 timer；超时先 abort，再停止等待并观察尾部任务。"""
    primary_present = exc_info[1] is not None
    cleanup_owner = owner or _bitbank_cleanup_owner("direct-close")
    cleanup_task = asyncio.create_task(
        _capture_bitbank_cleanup(manager, exc_info)
    )
    timer_task = asyncio.create_task(
        asyncio.sleep(BITBANK_TEARDOWN_TIMEOUT_SECONDS)
    )
    abort_attempted = False
    try:
        done, _ = await asyncio.wait(
            {cleanup_task, timer_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if cleanup_task in done:
            timer_task.cancel()
            try:
                await timer_task
            except asyncio.CancelledError:
                pass
            _consume_bitbank_cleanup_task(
                cleanup_task,
                stats,
                own_cancel=False,
                primary_present=primary_present,
            )
            return False

        timer_task.result()
        _record_bitbank_cleanup_failure(
            stats, TimeoutError("bitbank cleanup 超过 teardown 预算"),
        )
        abort_attempted = True
        _abort_bitbank_transport(
            connection, stats, primary_present=primary_present,
        )
        cleanup_task.cancel()
        completed, _ = await asyncio.wait(
            {cleanup_task},
            timeout=BITBANK_ABORT_COMPLETION_GRACE_SECONDS,
        )
        if cleanup_task in completed:
            _consume_bitbank_cleanup_task(
                cleanup_task,
                stats,
                own_cancel=True,
                primary_present=primary_present,
            )
            return False
        else:
            _observe_bitbank_cleanup_task(
                cleanup_task,
                stats,
                cleanup_owner,
                own_cancel=True,
                primary_present=primary_present,
            )
            return True
    except BaseException:
        timer_task.cancel()
        try:
            await timer_task
        except asyncio.CancelledError:
            pass
        if not cleanup_task.done():
            if not abort_attempted:
                _abort_bitbank_transport(
                    connection, stats, primary_present=True,
                )
            cleanup_task.cancel()
            completed, _ = await asyncio.wait(
                {cleanup_task},
                timeout=BITBANK_ABORT_COMPLETION_GRACE_SECONDS,
            )
            if cleanup_task in completed:
                _consume_bitbank_cleanup_task(
                    cleanup_task,
                    stats,
                    own_cancel=True,
                    primary_present=True,
                )
            else:
                _observe_bitbank_cleanup_task(
                    cleanup_task,
                    stats,
                    cleanup_owner,
                    own_cancel=True,
                    # 当前主任务异常（包括外部
                    # cancellation）优先。
                    primary_present=True,
                )
        else:
            _consume_bitbank_cleanup_task(
                cleanup_task,
                stats,
                own_cancel=abort_attempted,
                primary_present=True,
            )
        raise


async def _bitbank_backoff(
    consecutive_failures: int, deadline: float | None,
) -> bool:
    """等待一次退避；仅在仍可真正发起下一次连接时返回真。"""
    # ws_common 的指数函数在
    # min() 之前求幂；先截断指数，
    # 确保任意长连续故障都不会在退避路径
    # 触发 OverflowError。
    delay = max(0.0, _bounded_reconnect_delay(consecutive_failures))
    if deadline is None:
        await asyncio.sleep(delay)
        return True
    remaining = max(0.0, deadline - time.monotonic())
    if remaining == 0.0:
        return False
    try:
        await asyncio.wait_for(asyncio.sleep(min(delay, remaining)), remaining)
    except TimeoutError:
        return False
    return _active(deadline)


async def _bitbank_handshake(
    connection: _BitbankConnection,
    writer: SegmentedRawWriter,
    stats: CaptureStats,
    connection_id: str,
    deadline: float | None,
) -> None:
    """完成严格握手；两次 recv 各自拥有完整 wire-silence 预算。"""
    validators: tuple[tuple[str, Callable[[str], bool]], ...] = (
        ("Engine.IO open", _bitbank_engine_open),
        ("Socket.IO connect", _bitbank_socket_connect),
    )
    for index, (label, validator) in enumerate(validators):
        raw = await _bitbank_wait(connection.recv(), deadline, label)
        recv_ts_utc, recv_ts_mono_ns = _receive_clock()
        text, binary = _bitbank_raw_evidence(raw)
        _write_bitbank_frame(
            writer, text, connection_id, "protocol_control", recv_ts_utc,
            recv_ts_mono_ns,
        )
        stats.wire_frames += 1
        stats.last_wire_time = recv_ts_utc
        stats.control_frames += 1
        if binary:
            raise ConnectionError(
                "bitbank 握手收到 binary frame；原始字节已持久化"
            )
        if not validator(text):
            raise ConnectionError(f"bitbank 缺少合法 {label} 包")
        if index == 0:
            await _bitbank_wait(
                connection.send("40"), deadline, "Socket.IO connect send",
            )


async def _record_bitbank(
    writer: SegmentedRawWriter, stats: CaptureStats, deadline: float | None,
) -> None:
    """录制 bitbank 逐笔，并分别监督逐帧 wire 与真实交易数据静默。"""
    room = f"transactions_{writer.venue_symbol}"
    cleanup_owner = _bitbank_cleanup_owner(writer.venue_symbol)
    attempts_in_call = 0
    progress_state = _BitbankProgressState()
    while _active(deadline):
        if attempts_in_call:
            # reconnects 是真正开始的
            # 后续连接尝试，而不是已安排
            # 但未执行的退避。
            stats.reconnects += 1
        attempts_in_call += 1
        stats.connection_attempts += 1
        session_opened = False
        cleanup_detached = False
        try:
            manager = cast(
                _BitbankConnectionManager,
                websockets.connect(
                    BITBANK_WS_URL, ping_interval=None,
                    open_timeout=SILENCE_TIMEOUT_SECONDS,
                    close_timeout=BITBANK_TEARDOWN_TIMEOUT_SECONDS,
                ),
            )
            entered = False
            try:
                connection = await _bitbank_wait(
                    manager.__aenter__(), deadline, "WebSocket connect",
                )
                entered = True
                session_opened = True
                stats.sessions += 1
                connection_id = f"{writer.run_id}-c{stats.sessions:06d}"
                await _bitbank_handshake(
                    connection, writer, stats, connection_id, deadline,
                )
                await _bitbank_wait(
                    connection.send(
                        "42" + json.dumps(
                            ["join-room", room], separators=(",", ":"),
                        )
                    ),
                    deadline,
                    "join-room send",
                )
                # bitbank 不发送订阅确认；
                # 这里只表示 join-room
                # 已送入本地 WebSocket 连接，
                # 并不声称服务端已订阅或业务流已健康。
                stats.successful_sessions += 1
                data_deadline = (
                    time.monotonic() + BITBANK_DATA_SILENCE_TIMEOUT_SECONDS
                )
                while _active(deadline):
                    data_remaining = max(
                        0.0, data_deadline - time.monotonic(),
                    )
                    try:
                        raw = await _bitbank_wait(
                            connection.recv(), deadline, "wire silence",
                            maximum_seconds=min(
                                SILENCE_TIMEOUT_SECONDS, data_remaining,
                            ),
                        )
                    except ConnectionError:
                        if time.monotonic() >= data_deadline:
                            raise ConnectionError(
                                "bitbank trades 数据静默超时"
                            ) from None
                        raise
                    recv_ts_utc, recv_ts_mono_ns = _receive_clock()
                    text, binary = _bitbank_raw_evidence(raw)
                    channel_id = (
                        "protocol_control"
                        if binary else _bitbank_channel_id(text)
                    )
                    _write_bitbank_frame(
                        writer, text, connection_id, channel_id, recv_ts_utc,
                        recv_ts_mono_ns,
                    )
                    stats.wire_frames += 1
                    stats.last_wire_time = recv_ts_utc
                    if binary:
                        stats.control_frames += 1
                        raise ConnectionError(
                            "bitbank 收到 binary frame；原始字节已持久化"
                        )
                    _, is_trade, next_transaction_cursor = (
                        _bitbank_trade_frame(
                            text,
                            room,
                            datetime.fromisoformat(recv_ts_utc),
                            progress_state.high_id,
                            progress_state=progress_state,
                        )
                    )
                    if text == "2":
                        stats.control_frames += 1
                        await _bitbank_wait(
                            connection.send("3"), deadline, "pong send",
                            maximum_seconds=min(
                                SILENCE_TIMEOUT_SECONDS,
                                max(0.0, data_deadline - time.monotonic()),
                            ),
                        )
                    elif is_trade:
                        assert next_transaction_cursor == progress_state.high_id
                        _observed_data(stats, recv_ts_utc)
                        data_deadline = (
                            time.monotonic()
                            + BITBANK_DATA_SILENCE_TIMEOUT_SECONDS
                        )
                    else:
                        stats.control_frames += 1
                    if time.monotonic() >= data_deadline:
                        raise ConnectionError(
                            "bitbank trades 数据静默超时"
                        )
            finally:
                if entered:
                    cleanup_detached = await _close_bitbank_connection(
                        manager, connection, stats, sys.exc_info(), cleanup_owner,
                    )
        except _BitbankDeadlineElapsed:
            return
        except _BitbankPersistenceFailure as exc:
            raise exc.error
        except (OSError, ConnectionError, WebSocketException) as exc:
            if not _active(deadline):
                return
            if session_opened:
                stats.disconnects += 1
            stats.consecutive_failures += 1
            if cleanup_detached:
                # asyncio 无法强杀拒绝
                # cancellation 的
                # 任意 coroutine。
                # websockets 16 的
                # 公开 asyncio
                # transport.abort 合同
                # 应触发 connection_lost
                # 并完成 close；再给一个调度
                # 轮次仍不满足此不变量时
                # 停止重连，确保本调用最多
                # 留下一个观察任务。
                if _bitbank_cleanup_pending(cleanup_owner):
                    raise _BitbankCleanupInvariantFailure(
                        "bitbank cleanup 违反 transport.abort 终止不变量"
                    ) from exc
            if not await _bitbank_backoff(
                stats.consecutive_failures, deadline,
            ):
                return


async def _record_bitflyer(
    writer: SegmentedRawWriter, stats: CaptureStats, deadline: float | None,
) -> None:
    channel = f"lightning_executions_{writer.venue_symbol}"
    while _active(deadline):
        stats.connection_attempts += 1
        session_opened = False
        try:
            async with websockets.connect(BITFLYER_WS_URL) as connection:
                session_opened = True
                connection_id = _opened_connection(writer, stats)
                await connection.send(json.dumps({
                    "jsonrpc": "2.0", "method": "subscribe",
                    "params": {"channel": channel}, "id": 1,
                }))
                while _active(deadline):
                    try:
                        # 常驻模式同样有界等待，
                        # 静默走重连路径（C-10）。
                        raw = await asyncio.wait_for(
                            connection.recv(), _remaining(deadline)
                        )
                    except TimeoutError:
                        if not _active(deadline):
                            return
                        raise ConnectionError("bitFlyer trades 静默超时")
                    recv_ts_utc, recv_ts_mono_ns = _receive_clock()
                    text = to_text(raw)
                    payload = _json_mapping(text)
                    channel_id = _bitflyer_channel_id(payload)
                    writer.write_frame(
                        text, "lightning_executions",
                        connection_id=connection_id, channel_id=channel_id,
                        recv_ts_utc=recv_ts_utc,
                        recv_ts_mono_ns=recv_ts_mono_ns,
                    )
                    stats.wire_frames += 1
                    stats.last_wire_time = recv_ts_utc
                    if payload is None:
                        stats.control_frames += 1
                    else:
                        params = payload.get("params")
                        if (
                            payload.get("method") == "channelMessage"
                            and isinstance(params, Mapping)
                            and str(params.get("channel", "")).startswith(
                                "lightning_executions_"
                            )
                        ):
                            _observed_data(stats, recv_ts_utc)
                        else:
                            stats.control_frames += 1
        except (OSError, ConnectionError, WebSocketException):
            if not _active(deadline):
                return
            if session_opened:
                stats.disconnects += 1
            stats.consecutive_failures += 1
            stats.reconnects += 1
            await asyncio.sleep(
                _bounded_reconnect_delay(stats.consecutive_failures)
            )


_RECORDERS: dict[
    str, Callable[[SegmentedRawWriter, CaptureStats, float | None], Awaitable[None]]
] = {
    "gmo": _record_gmo,
    "bitbank": _record_bitbank,
    "bitflyer": _record_bitflyer,
}


async def record_trades(
    root: Path, venue_id: str, venue_symbol: str, minutes: float,
    segment_seconds: float, segment_max_bytes: int,
) -> tuple[CaptureStats, Path]:
    """录制一个市场；零分钟表示常驻。"""
    recorder = _RECORDERS.get(venue_id)
    if recorder is None:
        raise ValueError(f"暂不支持逐笔分段采集: {venue_id}")

    def progress(segment: Mapping[str, object]) -> None:
        print(
            "SEGMENT "
            f"{venue_id}/{venue_symbol} trades #{segment['segment_sequence']} "
            f"rows={int(str(segment['record_count'])):,} "
            f"bytes={int(str(segment['byte_count'])):,} "
            f"sha256={str(segment['sha256'])[:12]}",
            flush=True,
        )

    endpoint_id, endpoint_revision = ENDPOINT_BINDINGS[venue_id]
    writer = SegmentedRawWriter(
        root, venue_id, venue_symbol, domain="trade_realtime",
        endpoint_id=endpoint_id,
        endpoint_revision=endpoint_revision,
        segment_seconds=segment_seconds, segment_max_bytes=segment_max_bytes,
        on_segment_sealed=progress,
    )
    stats = CaptureStats(venue_id, venue_symbol)
    deadline = None if minutes <= 0 else time.monotonic() + minutes * 60
    status = "complete"
    failure: str | None = None

    async def checkpoint_loop() -> None:
        while True:
            if not _active(deadline):
                await asyncio.Event().wait()
            await asyncio.sleep(min(CHECKPOINT_SECONDS, _remaining(deadline)))
            if _active(deadline):
                writer.checkpoint(asdict(stats))

    primary: BaseException | None = None
    manifest: Path | None = None
    try:
        await supervise_capture_tasks(
            recorder(writer, stats, deadline), checkpoint_loop(),
        )
    except BaseException as exc:
        primary = exc
        if isinstance(exc, asyncio.CancelledError):
            status = "interrupted"
        else:
            status = "failed"
            failure = f"{type(exc).__name__}: {exc}"
        try:
            manifest = writer.finish(
                {**asdict(stats), "failure_detail": failure}, status=status
            )
        except BaseException as finish_error:
            primary.add_note(
                "writer.finish 未替换采集主异常: "
                f"{type(finish_error).__name__}: {finish_error}"
            )
        raise
    try:
        manifest = writer.finish(
            {**asdict(stats), "failure_detail": failure}, status=status
        )
    except BaseException:
        raise
    assert primary is None
    assert manifest is not None
    return stats, manifest


def main(argv: Sequence[str] | None = None) -> int:
    """命令行入口。"""
    parser = argparse.ArgumentParser(description="三所实时逐笔分段原文采集")
    parser.add_argument("--data-root", type=Path, default=None)
    sub = parser.add_subparsers(dest="command", required=True)
    record = sub.add_parser("record")
    record.add_argument("--venue", choices=sorted(_RECORDERS), required=True)
    record.add_argument("--symbol", required=True)
    record.add_argument("--minutes", type=float, default=1.0)
    record.add_argument("--segment-seconds", type=float, default=300.0)
    record.add_argument("--segment-max-mib", type=int, default=32)
    recover = sub.add_parser("recover")
    recover.add_argument("--older-minutes", type=int, default=60)
    args = parser.parse_args(argv)
    root = (args.data_root or configured_data_root()).resolve()
    if args.command == "recover":
        paths = recover_open_segments(
            root, int(args.older_minutes), domain="trade_realtime"
        )
        print(json.dumps({
            "recovered": len(paths),
            "manifests": [path.as_posix() for path in paths],
        }, ensure_ascii=False, indent=2))
        return 0
    try:
        stats, manifest = asyncio.run(record_trades(
            root, str(args.venue), str(args.symbol), float(args.minutes),
            float(args.segment_seconds), int(args.segment_max_mib) * 1024 * 1024,
        ))
    except KeyboardInterrupt:
        print("逐笔采集已中断；当前片段已封口", flush=True)
        return 130
    print(json.dumps({
        **asdict(stats), "run_manifest": manifest.as_posix(),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
