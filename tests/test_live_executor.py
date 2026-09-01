"""live 执行器的离线单测（C-13、C-14：全替身，不触任何真实端点）。"""
from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from guvolu.data.intent_ledger import IntentLedger
from guvolu.domain.config import Config, Limits
from guvolu.domain.enums import (
    ExecutionType,
    OrderStatus,
    OrderType,
    RunMode,
    ServiceStatus,
    SettleType,
    Side,
    TimeInForce,
)
from guvolu.domain.errors import ApiNetworkError
from guvolu.domain.intent import IntentState, OrderIntent
from guvolu.domain.models import Asset, Execution, Order
from guvolu.domain.symbols import SpotSymbol
from guvolu.execution.authorization_envelope import (
    EnvelopeState,
    EnvelopeStateStore,
    EnvelopeUsage,
    load_envelope,
)
from guvolu.execution.conversion import MarketRule
from guvolu.execution.dry_run_executor import TargetArtifact, build_plan
from guvolu.execution.live_executor import (
    EXIT_ANOMALY,
    EXIT_OK,
    EXIT_REFUSED,
    LiveRuntime,
    main,
    merged_breaker_thresholds,
    run_live_cycle,
)
from guvolu.execution.paper_fill_model import BookLevel, BookSnapshot
from guvolu.risk.circuit_breaker import BreakerThresholds, CircuitBreaker
from guvolu.risk.limits import LimitGate

BTC = SpotSymbol("BTC")
NOW = datetime(2026, 9, 2, 12, 0, tzinfo=UTC)
PRICE = Decimal("12000000")
RULE = MarketRule(
    symbol=BTC,
    tick_size=Decimal("1"),
    size_step=Decimal("0.00001"),
    min_order_size=Decimal("0.00001"),
    max_order_size=Decimal("5"),
)


def _envelope_body() -> dict[str, object]:
    return {
        "schema_version": 1,
        "issued_at": "2026-09-01T01:00:00Z",
        "valid_from": "2026-09-01T00:00:00Z",
        "valid_until": "2026-10-01T00:00:00Z",
        "symbols": ["BTC"],
        "order_jpy_max": "10000",
        "day_jpy_max": "10000",
        "day_count_max": 48,
        "envelope_jpy_total": "100000",
        "max_position_jpy": "30000",
        "max_cumulative_loss_jpy": "10000",
        "day_loss_jpy_max": "3000",
        "canary_first_order_jpy_max": "500",
        "max_prediction_age_minutes": 55,
        "market_risk": {
            "price_move_pause": {
                "window_seconds": 300,
                "threshold_bp": "500",
                "pause_seconds": 3600,
            },
            "spread_skip_bp": "50",
            "min_book_depth_ratio": "3",
            "stream_gap_seconds": 90,
        },
        "ops_breaker": {
            "consecutive_failure_limit": 3,
            "asset_deviation_ratio": "0.01",
            "asset_deviation_floor_jpy": "100",
        },
        "on_trip": "cancel_and_flatten",
    }


def _write_envelope(tmp_path: Path) -> Path:
    path = tmp_path / "envelope.json"
    path.write_text(
        json.dumps(_envelope_body(), ensure_ascii=False), encoding="utf-8"
    )
    return path


def _config() -> Config:
    return Config(
        mode=RunMode.LIVE,
        read_api_key=None,
        read_api_secret=None,
        trade_api_key=None,
        trade_api_secret=None,
        bitflyer_read_api_key=None,
        bitflyer_read_api_secret=None,
        bitflyer_private_rps=1.0,
        spot_whitelist=frozenset({BTC}),
        limits=Limits(
            order_jpy_max=Decimal("10000"),
            day_jpy_max=Decimal("10000"),
            day_count_max=48,
        ),
        log_dir=Path("logs"),
        private_rps=1.0,
        public_rps=1.0,
        recent_trades_max_seconds=60,
        tile_refresh_seconds=60,
    )


def _artifact(target: float = 0.8) -> TargetArtifact:
    return TargetArtifact(
        path=Path("target-test.json"),
        sha256="0" * 64,
        payload={"bar_interval": "1hour"},
        run_id="prediction-live-0001",
        decision_time=NOW - timedelta(minutes=5),
        correlation_id="co" + "a" * 16,
        market_id="mkt__gmo__btc__r0",
        unit="fraction_of_risk_budget",
        aggregate_target=target,
        symbol=BTC,
        risk_budget_jpy=Decimal("500"),
        mode="live",
        valid_from=NOW - timedelta(minutes=5),
        valid_until=NOW + timedelta(minutes=55),
    )


def _book() -> BookSnapshot:
    return BookSnapshot(
        symbol=BTC,
        bids=(BookLevel(Decimal("11999999"), Decimal("1")),),
        asks=(BookLevel(Decimal("12000001"), Decimal("1")),),
        observed_at=NOW,
        basis="orderbooks-rest",
    )


def _order(
    order_id: int,
    status: OrderStatus,
    *,
    size: Decimal = Decimal("0.00003"),
    executed: Decimal | None = None,
) -> Order:
    if executed is None:
        executed = size if status is OrderStatus.EXECUTED else Decimal("0")
    return Order(
        root_order_id=order_id,
        order_id=order_id,
        symbol="BTC",
        side=Side.BUY,
        order_type=OrderType.NORMAL,
        execution_type=ExecutionType.LIMIT,
        settle_type=SettleType.OPEN,
        size=size,
        executed_size=executed,
        price=PRICE,
        losscut_price=Decimal("0"),
        status=status,
        cancel_type=None,
        time_in_force=TimeInForce.FAS,
        timestamp=NOW,
    )


def _execution(order_id: int, size: Decimal) -> Execution:
    return Execution(
        execution_id=order_id * 10,
        order_id=order_id,
        position_id=None,
        symbol="BTC",
        side=Side.BUY,
        settle_type=SettleType.OPEN,
        size=size,
        price=PRICE,
        loss_gain=Decimal("0"),
        fee=Decimal("0"),
        timestamp=NOW,
    )


def _assets(
    jpy: str = "100000", btc: str = "0"
) -> tuple[Asset, ...]:
    return (
        Asset(
            amount=Decimal(jpy),
            available=Decimal(jpy),
            conversion_rate=Decimal("1"),
            symbol="JPY",
        ),
        Asset(
            amount=Decimal(btc),
            available=Decimal(btc),
            conversion_rate=PRICE,
            symbol="BTC",
        ),
    )


class _Reader:
    """READ_ONLY 替身：委托快照按脚本给出。"""

    def __init__(
        self,
        script: list[OrderStatus] | None = None,
        *,
        assets: tuple[Asset, ...] = _assets(),
        active: tuple[Order, ...] = (),
        executions: tuple[Execution, ...] = (),
    ) -> None:
        self.script = script or []
        self.calls = 0
        self._assets = assets
        self._active = active
        self._executions = executions

    def orders(self, order_ids: Sequence[int]) -> tuple[Order, ...]:
        if not self.script:
            return ()
        status = self.script[min(self.calls, len(self.script) - 1)]
        self.calls += 1
        return (_order(order_ids[0], status),)

    def active_orders(
        self, symbol: str, page: int | None = None, count: int | None = None,
    ) -> tuple[Order, ...]:
        return self._active

    def latest_executions(
        self, symbol: str, page: int | None = None, count: int | None = None,
    ) -> tuple[Execution, ...]:
        return self._executions

    def assets(self) -> tuple[Asset, ...]:
        return self._assets


class _Sender:
    """发送替身：记录事件顺序并断言意图先落盘（T-05）。"""

    consumes_write_budget = True

    def __init__(
        self,
        events: list[str] | None = None,
        *,
        ledger: IntentLedger | None = None,
        fail_network: bool = False,
    ) -> None:
        self.events = events if events is not None else []
        self.ledger = ledger
        self.fail_network = fail_network
        self.sent: list[OrderIntent] = []
        self.cancelled: list[int] = []

    def send(self, intent: OrderIntent) -> int:
        if self.ledger is not None:
            # 发送时意图必须已在账本且处于在途
            assert self.ledger.state(intent.intent_id) is IntentState.SENDING
        self.sent.append(intent)
        self.events.append(f"send:{intent.execution_type.value}")
        if self.fail_network:
            raise ApiNetworkError("/v1/order", "模拟网络超时")
        return 4242 + len(self.sent)

    def cancel(self, order_id: int) -> None:
        self.cancelled.append(order_id)
        self.events.append("cancel")


def _clock() -> object:
    class _Ticker:
        def __init__(self) -> None:
            self.value = 0.0

        def __call__(self) -> float:
            self.value += 1000.0
            return self.value

    return _Ticker()


def _runtime(
    tmp_path: Path,
    *,
    reader: _Reader,
    sender: _Sender,
    state: EnvelopeState | None = None,
) -> LiveRuntime:
    envelope = load_envelope(
        _write_envelope(tmp_path), whitelist=frozenset({BTC})
    )
    env_dir = tmp_path / "envelope-state"
    store = EnvelopeStateStore.for_envelope(envelope, directory=env_dir)
    if state is not None:
        store.save(state)
    ledger = IntentLedger(tmp_path / "live_intent_ledger.jsonl")
    if sender.ledger is None:
        sender.ledger = ledger
    config = _config()
    clock = _clock()
    assert callable(clock)
    return LiveRuntime(
        config=config,
        envelope=envelope,
        usage=EnvelopeUsage.for_envelope(envelope, directory=env_dir),
        state_store=store,
        state=store.load(),
        ledger=ledger,
        limit_gate=LimitGate(config.limits),
        breaker=CircuitBreaker(BreakerThresholds(
            schema_version=1,
            consecutive_failure_limit=3,
            stream_gap_seconds=90,
            asset_deviation_ratio=Decimal("0.01"),
            asset_deviation_floor_jpy=Decimal("100"),
        )),
        reader=reader,
        sender=sender,
        service_status=ServiceStatus.OPEN,
        rule=RULE,
        inflight_dir=tmp_path / ".inflight",
        max_wait_seconds=0,
        clock=clock,
        sleep=lambda seconds: None,
    )


def _cleared_state() -> EnvelopeState:
    return EnvelopeState(first_order_cleared=True)


def test_limit_send_poll_cancel_path(tmp_path: Path) -> None:
    """限价发送、受理、轮询、届满撤单并确认终态（R-01、T-03）。"""
    reader = _Reader([OrderStatus.ORDERED, OrderStatus.CANCELED])
    sender = _Sender()
    runtime = _runtime(tmp_path, reader=reader, sender=sender)
    plan = build_plan(
        _artifact(), rule=RULE, reference_price=PRICE,
        budget_jpy=Decimal("500"),
    )
    assert plan.proposal is not None
    exit_code, fragment = run_live_cycle(
        runtime, plan,
        assets=_assets(),
        price_observed_at=NOW,
        book=_book(),
        cancel_all=lambda: 0,
        now=NOW,
    )
    assert exit_code == EXIT_OK
    assert fragment["gate_verdict"] == "allow"
    assert fragment["resolution"] == "届满撤单并确认终态"
    assert fragment["cancel_requested"] is True
    assert fragment["final_order_status"] == "CANCELED"
    assert sender.cancelled == [4243]
    intent_view = fragment["intent"]
    assert isinstance(intent_view, dict)
    assert intent_view["state"] == "ACCEPTED"
    assert runtime.ledger.state(str(intent_view["intent_id"])) is (
        IntentState.ACCEPTED
    )
    # 消耗写预算即追加信封用量行
    assert runtime.usage.total_jpy() == Decimal("360")
    assert "POST /v1/order" in runtime.write_touched
    assert "POST /v1/cancelOrder" in runtime.write_touched
    # 首单对账通过解除压额（T-12）
    assert runtime.state_store.load().first_order_cleared is True
    verification = fragment["first_order_verification"]
    assert isinstance(verification, dict)
    assert verification["consistent"] is True


def test_first_order_canary_clamps_notional(tmp_path: Path) -> None:
    """首单未解除时超过 canary 上限的委托被拒且零发送。"""
    reader = _Reader()
    sender = _Sender()
    runtime = _runtime(tmp_path, reader=reader, sender=sender)
    plan = build_plan(
        _artifact(1.0), rule=RULE, reference_price=PRICE,
        budget_jpy=Decimal("10000"),
    )
    assert plan.proposal is not None
    assert plan.proposal.notional_jpy > Decimal("500")
    exit_code, fragment = run_live_cycle(
        runtime, plan,
        assets=_assets(),
        price_observed_at=NOW,
        book=_book(),
        cancel_all=lambda: 0,
        now=NOW,
    )
    assert exit_code == EXIT_OK
    assert fragment["gate_verdict"] == "reject"
    reason = fragment["gate_reason"]
    assert isinstance(reason, str)
    assert reason.startswith("first_order_canary")
    assert sender.sent == []
    assert runtime.usage.total_jpy() == Decimal("0")


def test_send_timeout_resolved_as_accepted(tmp_path: Path) -> None:
    """SEND_TIMEOUT 经 READ_ONLY 查询判定为已受理（T-06）。"""
    matched = _order(9001, OrderStatus.ORDERED)
    reader = _Reader(
        [OrderStatus.EXECUTED],
        active=(matched,),
        executions=(_execution(9001, Decimal("0.00003")),),
    )
    sender = _Sender(fail_network=True)
    runtime = _runtime(
        tmp_path, reader=reader, sender=sender, state=_cleared_state()
    )
    plan = build_plan(
        _artifact(), rule=RULE, reference_price=PRICE,
        budget_jpy=Decimal("500"),
    )
    exit_code, fragment = run_live_cycle(
        runtime, plan,
        assets=_assets(),
        price_observed_at=NOW,
        book=_book(),
        cancel_all=lambda: 0,
        now=NOW,
    )
    assert exit_code == EXIT_OK
    assert fragment["resolution"] == "窗口内到达终态"
    assert fragment["final_order_status"] == "EXECUTED"
    intent_view = fragment["intent"]
    assert isinstance(intent_view, dict)
    assert runtime.ledger.state(str(intent_view["intent_id"])) is (
        IntentState.ACCEPTED
    )
    assert runtime.ledger.order_id_of(str(intent_view["intent_id"])) == 9001
    # 超时结果未知仍保守计入用量
    assert runtime.usage.total_jpy() == Decimal("360")


def test_send_timeout_resolved_as_failed(tmp_path: Path) -> None:
    """SEND_TIMEOUT 查询零候选判定未受理，按异常码退出（T-06）。"""
    reader = _Reader()
    sender = _Sender(fail_network=True)
    runtime = _runtime(
        tmp_path, reader=reader, sender=sender, state=_cleared_state()
    )
    plan = build_plan(
        _artifact(), rule=RULE, reference_price=PRICE,
        budget_jpy=Decimal("500"),
    )
    exit_code, fragment = run_live_cycle(
        runtime, plan,
        assets=_assets(),
        price_observed_at=NOW,
        book=_book(),
        cancel_all=lambda: 0,
        now=NOW,
    )
    assert exit_code == EXIT_ANOMALY
    assert fragment["resolution"] == "超时对账判定为未受理（FAILED）"
    intent_view = fragment["intent"]
    assert isinstance(intent_view, dict)
    assert runtime.ledger.state(str(intent_view["intent_id"])) is (
        IntentState.FAILED
    )
    # 保守计数不回退（T-11 口径）
    assert runtime.usage.total_jpy() == Decimal("360")


def test_trip_cancel_and_flatten_order(tmp_path: Path) -> None:
    """熔断动作顺序：先全撤后市价清仓，意图先落盘（T-05、T-07）。"""
    events: list[str] = []
    reader = _Reader(assets=_assets(jpy="1000", btc="0.000216"))
    sender = _Sender(events)
    runtime = _runtime(
        tmp_path, reader=reader, sender=sender, state=_cleared_state()
    )
    plan = build_plan(
        _artifact(), rule=RULE, reference_price=PRICE,
        budget_jpy=Decimal("500"),
    )

    def cancel_all() -> int:
        events.append("cancel_all")
        return 0

    exit_code, fragment = run_live_cycle(
        runtime, plan,
        assets=reader.assets(),
        price_observed_at=NOW - timedelta(seconds=120),
        book=_book(),
        cancel_all=cancel_all,
        now=NOW,
    )
    assert exit_code == EXIT_ANOMALY
    assert fragment["gate_verdict"] == "trip"
    # 先撤后清仓
    assert events == ["cancel_all", "send:MARKET"]
    trip = fragment["trip"]
    assert isinstance(trip, dict)
    assert trip["on_trip"] == "cancel_and_flatten"
    flatten = trip["flatten"]
    assert isinstance(flatten, dict)
    assert flatten["status"] == "accepted"
    # 数量按 sizeStep 向下取整
    assert flatten["size"] == "0.00021"
    flatten_intent = sender.sent[0]
    assert flatten_intent.side is Side.SELL
    assert flatten_intent.execution_type is ExecutionType.MARKET
    assert runtime.ledger.state(flatten_intent.intent_id) is (
        IntentState.ACCEPTED
    )
    # 清仓豁免额度门仍记录用量
    assert runtime.usage.total_jpy() == Decimal("0.00021") * PRICE
    # 熔断锁定持久化，重启不重置
    persisted = runtime.state_store.load()
    assert persisted.tripped_at is not None
    assert "POST /v1/cancelBulkOrder" in runtime.write_touched


def test_trip_cancel_only_skips_flatten(tmp_path: Path) -> None:
    """on_trip 为 cancel_only 时不清仓。"""
    events: list[str] = []
    reader = _Reader(assets=_assets(btc="0.001"))
    sender = _Sender(events)
    runtime = _runtime(
        tmp_path, reader=reader, sender=sender, state=_cleared_state()
    )
    from guvolu.execution.authorization_envelope import OnTrip
    from dataclasses import replace

    runtime.envelope = replace(runtime.envelope, on_trip=OnTrip.CANCEL_ONLY)
    plan = build_plan(
        _artifact(), rule=RULE, reference_price=PRICE,
        budget_jpy=Decimal("500"),
    )
    exit_code, fragment = run_live_cycle(
        runtime, plan,
        assets=reader.assets(),
        price_observed_at=NOW - timedelta(seconds=120),
        book=_book(),
        cancel_all=lambda: events.append("cancel_all") or 0,
        now=NOW,
    )
    assert exit_code == EXIT_ANOMALY
    assert events == ["cancel_all"]
    trip = fragment["trip"]
    assert isinstance(trip, dict)
    assert "flatten" not in trip
    assert sender.sent == []


def test_merged_breaker_takes_stricter_values(tmp_path: Path) -> None:
    """熔断阈值取版本化配置与信封的逐项更严者（G-06）。"""
    envelope = load_envelope(
        _write_envelope(tmp_path), whitelist=frozenset({BTC})
    )
    base = BreakerThresholds(
        schema_version=1,
        consecutive_failure_limit=5,
        stream_gap_seconds=60,
        asset_deviation_ratio=Decimal("0.02"),
        asset_deviation_floor_jpy=Decimal("30"),
    )
    merged = merged_breaker_thresholds(base, envelope)
    assert merged.consecutive_failure_limit == 3
    assert merged.stream_gap_seconds == 60
    assert merged.asset_deviation_ratio == Decimal("0.01")
    assert merged.asset_deviation_floor_jpy == Decimal("30")


def test_main_refuses_non_live_mode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """非 live 配置退出码 2，且不打印任何密钥（T-04、T-01）。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("GUVOLU_MODE", raising=False)
    monkeypatch.setenv("GMO_COIN_TRADE_API_KEY", "sk_live_sentinel_key")
    monkeypatch.setenv("GMO_COIN_TRADE_API_SECRET", "sk_live_sentinel_secret")
    exit_code = main([
        "--target", str(tmp_path / "missing-target.json"),
        "--source-prediction", str(tmp_path / "missing-prediction.json"),
        "--source-prediction-sha256", "0" * 64,
    ])
    captured = capsys.readouterr()
    assert exit_code == EXIT_REFUSED
    assert "not_live" in captured.out
    assert "sk_live_sentinel" not in captured.out
    assert "sk_live_sentinel" not in captured.err


def test_main_refuses_tripped_envelope(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """信封熔断锁定后停机复核，重启不重置。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GUVOLU_MODE", "live")
    monkeypatch.setenv("GUVOLU_DATA_ROOT", str(tmp_path / "data"))
    envelope_path = _write_envelope(tmp_path)
    envelope = load_envelope(envelope_path, whitelist=frozenset({BTC}))
    store = EnvelopeStateStore.for_envelope(
        envelope, directory=tmp_path / "data" / "execution" / "envelope"
    )
    store.save(EnvelopeState(tripped_at=NOW, trip_reason="测试锁定"))
    exit_code = main([
        "--target", str(tmp_path / "missing-target.json"),
        "--source-prediction", str(tmp_path / "missing-prediction.json"),
        "--source-prediction-sha256", "0" * 64,
        "--envelope", str(envelope_path),
    ], moment=NOW)
    captured = capsys.readouterr()
    assert exit_code == EXIT_REFUSED
    assert "envelope_tripped" in captured.out


def test_main_refuses_exhausted_envelope(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """信封总额耗尽即停机复核。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GUVOLU_MODE", "live")
    monkeypatch.setenv("GUVOLU_DATA_ROOT", str(tmp_path / "data"))
    envelope_path = _write_envelope(tmp_path)
    envelope = load_envelope(envelope_path, whitelist=frozenset({BTC}))
    usage = EnvelopeUsage.for_envelope(
        envelope, directory=tmp_path / "data" / "execution" / "envelope"
    )
    usage.append(
        intent_id="in0000000000000009",
        notional_jpy=Decimal("100000"),
        at=NOW,
    )
    exit_code = main([
        "--target", str(tmp_path / "missing-target.json"),
        "--source-prediction", str(tmp_path / "missing-prediction.json"),
        "--source-prediction-sha256", "0" * 64,
        "--envelope", str(envelope_path),
    ], moment=NOW)
    captured = capsys.readouterr()
    assert exit_code == EXIT_REFUSED
    assert "envelope_exhausted" in captured.out
