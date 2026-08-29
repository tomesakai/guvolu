"""浸泡进程单测：全程离线，绝无网络调用（C-13、C-14）。"""
from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from guvolu.api.transport import (
    PrivateTransport,
    PublicTransport,
    RateLimiter,
)
from guvolu.api.public_client import PublicClient
from guvolu.api.trade_client import TradeClient
from guvolu.data.intent_ledger import IntentLedger
from guvolu.domain.config import Limits
from guvolu.domain.enums import RunMode, ServiceStatus, WsChannel
from guvolu.domain.errors import ApiNetworkError
from guvolu.domain.models import Order
from guvolu.domain.symbols import SpotSymbol
from guvolu.execution.conversion import MarketRule
from guvolu.execution.dual_reconcile import PrivateEvent
from guvolu.execution.emergency_stop import arm_emergency_stop
from guvolu.execution.reconcile_session import ReconcileSession
from guvolu.execution.soak_runner import (
    RACE_REST_FIRST_EXECUTION,
    RACE_REST_ONLY,
    RACE_WS_ONLY,
    MarketInputs,
    RecordingSnapshotReader,
    SoakError,
    SoakPaths,
    SoakRunner,
    ensure_dry_run,
    main,
    run_soak,
)
import guvolu.execution.soak_runner as soak_runner
from guvolu.execution.timeout_scheduler import BackoffPolicy
from guvolu.execution.trade_sender import TradeClientSender
from guvolu.risk.circuit_breaker import (
    BreakerThresholds,
    CircuitBreaker,
)
from guvolu.risk.limits import LimitGate
from test_dry_run_executor import write_artifact, write_rules
from test_order_state import order_event, rest_execution, rest_order
from test_reconcile_session import SessionReader, forbid_network

ROOT = Path(__file__).resolve().parents[1]
BREAKER_CONFIG = ROOT / "config" / "circuit_breaker.json"
SESSION_CONFIG = ROOT / "config" / "reconcile_session.json"
MOMENT = datetime(2026, 8, 16, 0, 0, tzinfo=UTC)
ROUND2 = MOMENT + timedelta(seconds=30)
ROUND3 = MOMENT + timedelta(seconds=60)
POLICY = BackoffPolicy(initial_seconds=5.0, max_seconds=300.0)
THRESHOLDS = BreakerThresholds(
    schema_version=1,
    consecutive_failure_limit=3,
    stream_gap_seconds=90,
    asset_deviation_ratio=Decimal("0.01"),
    asset_deviation_floor_jpy=Decimal("30"),
)
RULE = MarketRule(
    symbol=SpotSymbol("BTC"),
    tick_size=Decimal("1"),
    size_step=Decimal("0.0001"),
    min_order_size=Decimal("0.0001"),
    max_order_size=Decimal("5"),
)


class StaticMarketSource:
    """静态市场输入替身，不触碰任何端点。"""

    def __init__(
        self, status: ServiceStatus = ServiceStatus.OPEN
    ) -> None:
        self._inputs = MarketInputs(
            rule=RULE,
            reference_price=Decimal("1000000"),
            service_status=status,
        )

    def current(self) -> MarketInputs:
        return self._inputs

    def touched(self) -> tuple[str, ...]:
        return ()


class FailingReader(SessionReader):
    """挂单查询固定失败的替身。"""

    def active_orders(
        self, symbol: str, page: int | None = None, count: int | None = None
    ) -> tuple[Order, ...]:
        raise ApiNetworkError("/v1/activeOrders", "注入的网络错")


class FakeStream:
    """离线 WS 替身：可注入事件帧与重连信号。"""

    def __init__(self) -> None:
        self.subscribed: list[WsChannel] = []
        self._reconnects: asyncio.Queue[None] = asyncio.Queue()
        self._events: asyncio.Queue[PrivateEvent] = asyncio.Queue()

    async def subscribe(
        self, channel: WsChannel, option: str | None = None
    ) -> None:
        self.subscribed.append(channel)

    async def events(self) -> AsyncIterator[PrivateEvent]:
        while True:
            yield await self._events.get()

    async def run(
        self,
        on_reconnect: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        while True:
            await self._reconnects.get()
            if on_reconnect is not None:
                await on_reconnect()

    def trigger_reconnect(self) -> None:
        self._reconnects.put_nowait(None)

    def push(self, event: PrivateEvent) -> None:
        self._events.put_nowait(event)


def make_trade(tmp_path: Path) -> TradeClient:
    """构造模拟运行模式的写路径客户端，发送必被拦截。"""
    transport = PrivateTransport(
        "dummy-key", "dummy-secret", RateLimiter(10.0), tmp_path / "logs"
    )
    return TradeClient(
        transport, RunMode.DRY_RUN, frozenset({SpotSymbol("BTC")})
    )


def make_paths(tmp_path: Path) -> SoakPaths:
    """构造临时落盘路径。"""
    return SoakPaths(
        report=tmp_path / "soak_report.jsonl",
        checkpoint=tmp_path / "soak_checkpoint.json",
        heartbeat=tmp_path / "soak_heartbeat.json",
        stop_file=tmp_path / "soak.stop",
    )


def make_runner(
    tmp_path: Path,
    *,
    reader: SessionReader | None = None,
    target_path: Path | None = None,
    mode: RunMode = RunMode.DRY_RUN,
) -> tuple[SoakRunner, SoakPaths, CircuitBreaker, SessionReader]:
    """构造被测浸泡状态机与其落盘路径。"""
    inner = reader if reader is not None else SessionReader()
    recording = RecordingSnapshotReader(inner)
    ledger = IntentLedger(tmp_path / "intent_ledger.jsonl")
    breaker = CircuitBreaker(THRESHOLDS)
    session = ReconcileSession(
        ledger=ledger,
        reader=recording,
        breaker=breaker,
        symbol=SpotSymbol("BTC"),
        policy=POLICY,
    )
    public = PublicClient(PublicTransport(RateLimiter(3.0)))
    trade = make_trade(tmp_path)
    emergency = arm_emergency_stop(breaker, public, trade)
    paths = make_paths(tmp_path)
    runner = SoakRunner(
        mode=mode,
        session=session,
        reader=recording,
        ledger=ledger,
        breaker=breaker,
        emergency=emergency,
        symbol=SpotSymbol("BTC"),
        market_source=StaticMarketSource(),
        limit_gate=LimitGate(
            Limits(
                order_jpy_max=Decimal("500"),
                day_jpy_max=Decimal("2000"),
                day_count_max=50,
            )
        ),
        whitelist=frozenset({SpotSymbol("BTC")}),
        sender=TradeClientSender(trade),
        paths=paths,
        target_path=target_path,
        budget_jpy=Decimal("500"),
        no_trade_band=Decimal("0.01"),
        started_at=MOMENT,
    )
    return runner, paths, breaker, inner


def read_report(paths: SoakPaths) -> list[dict[str, object]]:
    """读取报告 JSONL 全部行。"""
    lines = paths.report.read_text(encoding="utf-8").splitlines()
    return [json.loads(line) for line in lines]


def read_json(path: Path) -> dict[str, object]:
    """读取单个 JSON 文件。"""
    loaded = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(loaded, dict)
    return loaded


def test_live_mode_refused(tmp_path: Path) -> None:
    """live 配置拒绝启动（T-04、A-01）。"""
    with pytest.raises(SoakError):
        ensure_dry_run(RunMode.LIVE)
    with pytest.raises(SoakError):
        make_runner(tmp_path, mode=RunMode.LIVE)
    ensure_dry_run(RunMode.DRY_RUN)


def test_cli_live_config_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """命令行在构造任何客户端前拒绝 live 配置。"""
    forbid_network(monkeypatch)
    monkeypatch.setenv("GUVOLU_MODE", "live")
    code = main(["--env-file", str(tmp_path / "absent.env")])
    assert code == 2


def test_cli_dynamic_target_refused_before_config_or_clients(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """动态目标在任何账本、客户端、WS token 或报告副作用前失败。"""
    forbid_network(monkeypatch)
    target = write_artifact(tmp_path, 0.6)

    def poison(*args: object, **kwargs: object) -> object:
        raise AssertionError("动态目标拒绝前不得构造运行资源")

    monkeypatch.setattr(soak_runner, "load_config", poison)
    monkeypatch.setattr(soak_runner, "IntentLedger", poison)
    monkeypatch.setattr(soak_runner, "create_ws_token", poison)
    monkeypatch.setattr(soak_runner, "atomic_write_text", poison)

    code = main(
        [
            "--target", str(target),
            "--ledger", str(tmp_path / "intent_ledger.jsonl"),
            "--report", str(tmp_path / "soak_report.jsonl"),
        ]
    )
    assert code == 2
    assert not (tmp_path / "intent_ledger.jsonl").exists()
    assert not (tmp_path / "soak_report.jsonl").exists()


def test_rounds_progress_with_zero_target(tmp_path: Path) -> None:
    """多轮推进：首轮基线、次轮稳态，零目标不生成委托。"""
    runner, paths, _breaker, _reader = make_runner(tmp_path)
    first = runner.run_round(MOMENT)
    second = runner.run_round(ROUND2)
    assert runner.rounds_completed == 2
    assert first["round"] == 1 and second["round"] == 2
    report = read_report(paths)
    assert [item["record"] for item in report] == ["round", "round"]
    snapshot_first = report[0]["snapshot"]
    snapshot_second = report[1]["snapshot"]
    assert isinstance(snapshot_first, dict)
    assert isinstance(snapshot_second, dict)
    assert snapshot_first["mode"] == "baseline"
    assert snapshot_second["mode"] == "audit"
    delta = report[1]["delta"]
    assert isinstance(delta, dict)
    assert delta["proposal"] is None
    assert report[1]["intent"] is None
    endpoints = report[1]["endpoints"]
    assert isinstance(endpoints, dict)
    assert endpoints["write_planned"] == []
    assert endpoints["write_touched"] == []
    assert "GET /v1/activeOrders" in endpoints["read_touched"]
    assert "GET /v1/account/assets" in endpoints["read_touched"]


def test_checkpoint_and_heartbeat_persisted(tmp_path: Path) -> None:
    """每轮落 checkpoint 与心跳，停止后写终态。"""
    runner, paths, _breaker, _reader = make_runner(tmp_path)
    runner.run_round(MOMENT)
    checkpoint = read_json(paths.checkpoint)
    assert checkpoint["status"] == "running"
    assert checkpoint["rounds_completed"] == 1
    assert checkpoint["stop_reason"] is None
    heartbeat = read_json(paths.heartbeat)
    assert heartbeat["rounds_completed"] == 1
    assert heartbeat["stop_reason"] is None
    runner.finalize("stop-file", ROUND2)
    checkpoint = read_json(paths.checkpoint)
    assert checkpoint["status"] == "stopped"
    assert checkpoint["stop_reason"] == "stop-file"
    heartbeat = read_json(paths.heartbeat)
    assert heartbeat["stop_reason"] == "stop-file"


def test_heartbeat_throttled(tmp_path: Path) -> None:
    """心跳缺省节流，间隔不足不重写。"""
    runner, paths, _breaker, _reader = make_runner(tmp_path)
    runner.heartbeat(MOMENT, force=True)
    first = read_json(paths.heartbeat)
    runner.heartbeat(MOMENT + timedelta(seconds=5))
    assert read_json(paths.heartbeat) == first
    runner.heartbeat(MOMENT + timedelta(seconds=11))
    assert read_json(paths.heartbeat) != first


def test_stop_marker_detected(tmp_path: Path) -> None:
    """停止标记文件是第二停止通道。"""
    runner, paths, _breaker, _reader = make_runner(tmp_path)
    runner.run_round(MOMENT)
    assert runner.check_stop() is None
    paths.stop_file.touch()
    assert runner.check_stop() == "stop-file"


def test_reconnect_forces_realign_round(tmp_path: Path) -> None:
    """重连后下一轮强制全量快照且不计数（C-10）。"""
    runner, paths, breaker, _reader = make_runner(tmp_path)
    runner.run_round(MOMENT)
    runner.apply_events((order_event(637010),), MOMENT)
    runner.note_reconnect()
    assert runner.realign_pending()
    second = runner.run_round(ROUND2)
    assert second["realign"] is True
    snapshot = second["snapshot"]
    assert isinstance(snapshot, dict)
    assert snapshot["mode"] == "realign"
    assert snapshot["counted_into_breaker"] == 0
    assert breaker.consecutive_failures == 0
    assert runner.reconnects == 1
    assert not runner.realign_pending()
    ws_channel = second["ws_channel"]
    assert isinstance(ws_channel, dict)
    assert ws_channel["events_applied"] == 1
    third = runner.run_round(ROUND3)
    snapshot_third = third["snapshot"]
    assert isinstance(snapshot_third, dict)
    assert snapshot_third["mode"] == "audit"


def test_dynamic_target_soak_is_disabled(tmp_path: Path) -> None:
    """没有版本化目标头与逐轮血缘前，动态目标不得成为浸泡证据。"""
    target = write_artifact(tmp_path, 0.6)
    with pytest.raises(SoakError, match="暂不接受动态目标"):
        make_runner(tmp_path, target_path=target)


def test_race_observation_classifies_channels(tmp_path: Path) -> None:
    """稳态轮记录竞态分类计数，不改变熔断计数口径。"""
    reader = SessionReader()
    runner, paths, breaker, _reader = make_runner(tmp_path, reader=reader)
    first = runner.run_round(MOMENT)
    assert first["race"] == {
        "round": None,
        "cumulative": runner_cumulative_zero(),
    }
    runner.apply_events((order_event(637010),), MOMENT)
    reader.active = (rest_order(637011),)
    reader.latest = (rest_execution(637001, 900001),)
    second = runner.run_round(ROUND2)
    race = second["race"]
    assert isinstance(race, dict)
    round_body = race["round"]
    assert isinstance(round_body, dict)
    assert round_body[RACE_WS_ONLY]["count"] == 1
    assert round_body[RACE_REST_ONLY]["count"] == 1
    assert round_body[RACE_REST_FIRST_EXECUTION]["count"] == 1
    assert isinstance(
        round_body[RACE_WS_ONLY]["mean_seconds"], float
    )
    cumulative = race["cumulative"]
    assert isinstance(cumulative, dict)
    assert cumulative["audit_rounds"] == 1
    assert cumulative[RACE_WS_ONLY]["count"] == 1
    snapshot = second["snapshot"]
    assert isinstance(snapshot, dict)
    assert snapshot["counted_into_breaker"] == 2
    assert breaker.consecutive_failures == 2


def runner_cumulative_zero() -> dict[str, object]:
    """空累计摘要的期待形态。"""
    empty = {
        "count": 0,
        "min_seconds": None,
        "mean_seconds": None,
        "max_seconds": None,
    }
    return {
        "audit_rounds": 0,
        RACE_WS_ONLY: dict(empty),
        RACE_REST_ONLY: dict(empty),
        RACE_REST_FIRST_EXECUTION: dict(empty),
    }


def test_run_soak_max_rounds(tmp_path: Path) -> None:
    """驻留循环按轮次上限停止并消费 WS 事件。"""
    runner, paths, _breaker, _reader = make_runner(tmp_path)
    client = FakeStream()
    client.push(order_event(637010))

    async def scenario() -> str:
        return await run_soak(
            runner,
            client,
            interval_seconds=0.01,
            max_rounds=2,
            poll_seconds=0.01,
        )

    reason = asyncio.run(scenario())
    assert reason == "max-rounds"
    assert runner.rounds_completed == 2
    assert client.subscribed == [
        WsChannel.ORDER_EVENTS,
        WsChannel.EXECUTION_EVENTS,
    ]
    report = read_report(paths)
    assert len(report) == 2
    ws_second = report[1]["ws_channel"]
    assert isinstance(ws_second, dict)
    assert ws_second["events_applied"] == 1
    checkpoint = read_json(paths.checkpoint)
    assert checkpoint["status"] == "stopped"
    assert checkpoint["stop_reason"] == "max-rounds"
    assert checkpoint["ws_events_applied"] == 1


def test_run_soak_stop_file_interrupts_wait(tmp_path: Path) -> None:
    """停止标记在轮间等待中即时生效，完成当轮后停。"""
    runner, paths, _breaker, _reader = make_runner(tmp_path)
    client = FakeStream()

    async def scenario() -> str:
        task = asyncio.create_task(
            run_soak(
                runner,
                client,
                interval_seconds=60.0,
                poll_seconds=0.01,
            )
        )
        await asyncio.sleep(0.05)
        paths.stop_file.touch()
        return await task

    reason = asyncio.run(scenario())
    assert reason == "stop-file"
    assert runner.rounds_completed == 1
    checkpoint = read_json(paths.checkpoint)
    assert checkpoint["status"] == "stopped"
    assert checkpoint["stop_reason"] == "stop-file"


def test_run_soak_reconnect_wakes_realign(tmp_path: Path) -> None:
    """重连唤醒等待并立即执行强制全量快照轮。"""
    runner, paths, _breaker, _reader = make_runner(tmp_path)
    client = FakeStream()

    async def scenario() -> str:
        task = asyncio.create_task(
            run_soak(
                runner,
                client,
                interval_seconds=60.0,
                poll_seconds=0.01,
            )
        )
        await asyncio.sleep(0.05)
        client.trigger_reconnect()
        await asyncio.sleep(0.05)
        paths.stop_file.touch()
        return await task

    reason = asyncio.run(scenario())
    assert reason == "stop-file"
    assert runner.rounds_completed == 2
    assert runner.reconnects == 1
    report = read_report(paths)
    assert report[0]["realign"] is False
    assert report[1]["realign"] is True
    snapshot = report[1]["snapshot"]
    assert isinstance(snapshot, dict)
    assert snapshot["mode"] == "realign"


def test_run_soak_stops_after_round_errors(tmp_path: Path) -> None:
    """连续轮内错误达上限即停止，错误逐条留痕。"""
    runner, paths, _breaker, _reader = make_runner(
        tmp_path, reader=FailingReader()
    )
    client = FakeStream()

    async def scenario() -> str:
        return await run_soak(
            runner,
            client,
            interval_seconds=0.01,
            poll_seconds=0.01,
        )

    reason = asyncio.run(scenario())
    assert reason == "round-errors"
    assert runner.rounds_completed == 0
    report = read_report(paths)
    assert [item["record"] for item in report] == ["round_error"] * 3
    assert report[-1]["consecutive_errors"] == 3
    checkpoint = read_json(paths.checkpoint)
    assert checkpoint["stop_reason"] == "round-errors"


def test_cli_offline_max_rounds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """命令行离线两轮：注入替身，令牌与 WS 均不触网。"""
    forbid_network(monkeypatch)
    monkeypatch.setenv("GMO_COIN_READ_ONLY_API_KEY", "dummy-key")
    monkeypatch.setenv("GMO_COIN_READ_ONLY_API_SECRET", "dummy-secret")
    monkeypatch.setenv("GMO_COIN_TRADE_API_KEY", "dummy-key")
    monkeypatch.setenv("GMO_COIN_TRADE_API_SECRET", "dummy-secret")
    monkeypatch.delenv("GUVOLU_MODE", raising=False)
    monkeypatch.setenv("GUVOLU_LOG_DIR", str(tmp_path / "logs"))
    reader = SessionReader()

    def fake_read_client(transport: object) -> SessionReader:
        return reader

    monkeypatch.setattr(soak_runner, "ReadClient", fake_read_client)
    monkeypatch.setattr(
        soak_runner, "create_ws_token", lambda transport: "tok"
    )
    monkeypatch.setattr(
        soak_runner, "revoke_ws_token", lambda transport, token: None
    )
    monkeypatch.setattr(
        soak_runner, "PrivateWsClient", lambda token: FakeStream()
    )
    paths = make_paths(tmp_path)
    code = main(
        [
            "--rules", str(write_rules(tmp_path)),
            "--reference-price", "1000000",
            "--service-status", "OPEN",
            "--ledger", str(tmp_path / "intent_ledger.jsonl"),
            "--breaker-config", str(BREAKER_CONFIG),
            "--session-config", str(SESSION_CONFIG),
            "--env-file", str(tmp_path / "absent.env"),
            "--report", str(paths.report),
            "--checkpoint", str(paths.checkpoint),
            "--heartbeat", str(paths.heartbeat),
            "--stop-file", str(paths.stop_file),
            "--interval-seconds", "0.01",
            "--max-rounds", "2",
        ]
    )
    assert code == 0
    report = read_report(paths)
    assert len(report) == 2
    assert report[0]["intent"] is None
    endpoints = report[1]["endpoints"]
    assert isinstance(endpoints, dict)
    assert endpoints["write_planned"] == []
    assert endpoints["write_touched"] == []
    assert endpoints["auth_lifecycle"] == [
        "POST /v1/ws-auth",
        "PUT /v1/ws-auth",
        "DELETE /v1/ws-auth",
    ]
    checkpoint = read_json(paths.checkpoint)
    assert checkpoint["status"] == "stopped"
    assert checkpoint["stop_reason"] == "max-rounds"
    heartbeat = read_json(paths.heartbeat)
    assert heartbeat["rounds_completed"] == 2
