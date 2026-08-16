"""对账会话单测：全程离线，绝无网络调用（C-13、C-14）。"""
from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
import requests

from guvolu.data.intent_ledger import IntentLedger
from guvolu.domain.enums import ExecutionType, OrderStatus, Side
from guvolu.domain.intent import IntentState, OrderIntent
from guvolu.domain.models import Asset, Execution, Order
from guvolu.domain.symbols import SpotSymbol
from guvolu.execution.dual_reconcile import SnapshotMode
from guvolu.execution.reconcile_session import (
    ReconcileSession,
    SessionSettings,
    load_session_settings,
    main,
)
from guvolu.execution.timeout_scheduler import BackoffPolicy
from guvolu.risk.circuit_breaker import (
    BreakerState,
    BreakerThresholds,
    CircuitBreaker,
)
from test_dry_run_executor import write_artifact, write_rules
from test_order_state import order_event, rest_execution, rest_order

ROOT = Path(__file__).resolve().parents[1]
BREAKER_CONFIG = ROOT / "config" / "circuit_breaker.json"
SESSION_CONFIG = ROOT / "config" / "reconcile_session.json"
MOMENT = datetime(2026, 8, 16, 0, 0, tzinfo=UTC)
LATER = MOMENT + timedelta(seconds=30)
POLICY = BackoffPolicy(initial_seconds=5.0, max_seconds=300.0)
THRESHOLDS = BreakerThresholds(
    schema_version=1,
    consecutive_failure_limit=3,
    stream_gap_seconds=90,
    asset_deviation_ratio=Decimal("0.01"),
    asset_deviation_floor_jpy=Decimal("30"),
)


def make_asset(symbol: str, amount: str, rate: str = "1") -> Asset:
    """构造資産残高一条。"""
    return Asset(
        amount=Decimal(amount),
        available=Decimal(amount),
        conversion_rate=Decimal(rate),
        symbol=symbol,
    )


class SessionReader:
    """会话只读替身，数据可逐轮改写。"""

    def __init__(self) -> None:
        self.active: tuple[Order, ...] = ()
        self.latest: tuple[Execution, ...] = ()
        self.asset_rows: tuple[Asset, ...] = (make_asset("JPY", "3009"),)
        self.lookup: tuple[Order, ...] = ()
        self.per_order: dict[int, tuple[Execution, ...]] = {}
        self.calls: list[str] = []

    def active_orders(
        self, symbol: str, page: int | None = None, count: int | None = None
    ) -> tuple[Order, ...]:
        self.calls.append("activeOrders")
        return self.active

    def latest_executions(
        self, symbol: str, page: int | None = None, count: int | None = None
    ) -> tuple[Execution, ...]:
        self.calls.append("latestExecutions")
        return self.latest

    def orders(self, order_ids: Sequence[int]) -> tuple[Order, ...]:
        self.calls.append("orders")
        return tuple(
            order for order in self.lookup if order.order_id in order_ids
        )

    def executions(
        self,
        order_id: int | None = None,
        execution_ids: Sequence[int] | None = None,
    ) -> tuple[Execution, ...]:
        self.calls.append("executions")
        assert order_id is not None
        return self.per_order.get(order_id, ())

    def assets(self) -> tuple[Asset, ...]:
        self.calls.append("assets")
        return self.asset_rows


def accepted_ledger(tmp_path: Path) -> IntentLedger:
    """构造一笔已受理并映射委托号的账本。"""
    ledger = IntentLedger(tmp_path / "intent_ledger.jsonl")
    intent = OrderIntent(
        intent_id="it01",
        correlation_id="co0001",
        symbol=SpotSymbol("BTC"),
        side=Side.BUY,
        execution_type=ExecutionType.LIMIT,
        size=Decimal("0.0002"),
        price=Decimal("1000000"),
        time_in_force=None,
        created_at=MOMENT,
    )
    ledger.record_intent(intent, at=MOMENT)
    ledger.begin_send("it01", at=MOMENT)
    ledger.accept("it01", 637001, at=MOMENT)
    return ledger


def build_session(
    ledger: IntentLedger, reader: SessionReader
) -> tuple[ReconcileSession, CircuitBreaker]:
    """构造会话与熔断器。"""
    breaker = CircuitBreaker(THRESHOLDS)
    session = ReconcileSession(
        ledger=ledger,
        reader=reader,
        breaker=breaker,
        symbol=SpotSymbol("BTC"),
        policy=POLICY,
    )
    return session, breaker


def test_baseline_backfills_position_from_read_only(
    tmp_path: Path,
) -> None:
    """基线按映射委托回填成交，持仓来自 READ_ONLY 事实（T-03）。"""
    ledger = accepted_ledger(tmp_path)
    reader = SessionReader()
    reader.per_order = {637001: (rest_execution(637001, 900001),)}
    session, _breaker = build_session(ledger, reader)
    outcome = session.snapshot_round(MOMENT)
    assert outcome.reconcile.mode is SnapshotMode.BASELINE
    assert outcome.backfilled_orders == 1
    assert session.position_size() == Decimal("0.0001")
    assert "GET /v1/executions" in session.read_endpoints()
    assert "GET /v1/account/assets" in session.read_endpoints()


def test_acceptance_receipt_alone_is_not_position(tmp_path: Path) -> None:
    """仅有受理回执不构成持仓，差分按零持仓折算（T-03）。"""
    ledger = accepted_ledger(tmp_path)
    session, _breaker = build_session(ledger, SessionReader())
    session.snapshot_round(MOMENT)
    assert session.position_size() == Decimal("0")


def test_steady_state_snapshot_counts_mismatch(tmp_path: Path) -> None:
    """稳态轮不一致以 REST 为准并计入熔断计数（R-08）。"""
    ledger = accepted_ledger(tmp_path)
    reader = SessionReader()
    session, breaker = build_session(ledger, reader)
    session.snapshot_round(MOMENT)
    session.ingest_ws_events((order_event(637010),), MOMENT)
    outcome = session.snapshot_round(LATER)
    assert outcome.reconcile.mode is SnapshotMode.AUDIT
    kinds = [item.kind for item in outcome.reconcile.mismatches]
    assert kinds == ["stale_active_order"]
    assert outcome.reconcile.counted_into_breaker == 1
    assert breaker.consecutive_failures == 1
    view = session.store.order(637010)
    assert view is not None
    assert not view.is_active


def test_reconnect_forces_full_snapshot_without_counting(
    tmp_path: Path,
) -> None:
    """重连后强制全量快照对账，只对齐不计数（C-10）。"""
    ledger = accepted_ledger(tmp_path)
    reader = SessionReader()
    session, breaker = build_session(ledger, reader)
    session.snapshot_round(MOMENT)
    session.ingest_ws_events((order_event(637010),), MOMENT)
    reader.active = (rest_order(637011),)
    calls_before = len(reader.calls)
    outcome = session.on_ws_reconnect(LATER)
    assert outcome.reconcile.mode is SnapshotMode.REALIGN
    kinds = sorted(item.kind for item in outcome.reconcile.mismatches)
    assert kinds == ["stale_active_order", "ws_missing_order"]
    assert outcome.reconcile.counted_into_breaker == 0
    assert breaker.consecutive_failures == 0
    assert len(reader.calls) > calls_before
    assert session.store.order(637011) is not None


def test_unexplained_asset_gap_trips_breaker(tmp_path: Path) -> None:
    """未解释资产差额达阈值即触发熔断（R-02、TBD-10 口径）。"""
    ledger = accepted_ledger(tmp_path)
    reader = SessionReader()
    session, breaker = build_session(ledger, reader)
    fired: list[str] = []
    breaker.set_trip_action(fired.append)
    session.snapshot_round(MOMENT)
    reader.asset_rows = (make_asset("JPY", "2959"),)
    outcome = session.snapshot_round(LATER)
    assert outcome.asset_unexplained_jpy == Decimal("50")
    assert breaker.state is BreakerState.TRIPPED
    assert breaker.trip_reason is not None
    assert "资产异动" in breaker.trip_reason
    assert len(fired) == 1


def test_explained_execution_keeps_breaker_normal(tmp_path: Path) -> None:
    """账本内成交解释的资产变动不触发熔断。"""
    ledger = accepted_ledger(tmp_path)
    reader = SessionReader()
    reader.asset_rows = (
        make_asset("JPY", "3009"),
        make_asset("BTC", "0", rate="1000000"),
    )
    session, breaker = build_session(ledger, reader)
    session.snapshot_round(MOMENT)
    reader.latest = (rest_execution(637001, 900001),)
    reader.asset_rows = (
        make_asset("JPY", "2909"),
        make_asset("BTC", "0.0001", rate="1000000"),
    )
    outcome = session.snapshot_round(LATER)
    assert outcome.asset_unexplained_jpy == Decimal("0")
    assert breaker.state is BreakerState.NORMAL
    assert session.position_size() == Decimal("0.0001")


def load_settings() -> SessionSettings:
    """装载仓库内会话配置。"""
    return load_session_settings(SESSION_CONFIG)


def test_session_settings_loaded_from_versioned_config() -> None:
    """会话参数来自版本化配置（G-06、TBD-07 提案值）。"""
    settings = load_settings()
    assert settings.schema_version == 1
    assert settings.snapshot_interval_seconds == 30
    assert settings.timeout_query_initial_seconds == 5
    assert settings.timeout_query_max_seconds == 300
    assert settings.no_trade_band == Decimal("0.01")


def forbid_network(monkeypatch: pytest.MonkeyPatch) -> None:
    """封锁 requests 会话层，任何真实请求立即失败（C-14）。"""

    def explode(*args: object, **kwargs: object) -> object:
        raise AssertionError("测试内禁止真实网络调用")

    monkeypatch.setattr(requests.Session, "request", explode)
    monkeypatch.setattr(requests.Session, "get", explode)


def prepare_cli(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    reader: SessionReader,
) -> None:
    """CLI 离线前置：封网、注入替身、给定假密钥。"""
    forbid_network(monkeypatch)
    monkeypatch.setenv("GMO_COIN_TRADE_API_KEY", "dummy-key")
    monkeypatch.setenv("GMO_COIN_TRADE_API_SECRET", "dummy-secret")
    monkeypatch.delenv("GUVOLU_MODE", raising=False)
    monkeypatch.setenv("GUVOLU_LOG_DIR", str(tmp_path / "logs"))

    class FakeReadClient:
        """替身工厂，屏蔽真实构造。"""

        @classmethod
        def from_config(cls, config: object) -> SessionReader:
            return reader

    monkeypatch.setattr(
        "guvolu.execution.reconcile_session.ReadClient", FakeReadClient
    )


def write_ws_events(tmp_path: Path) -> Path:
    """写出匹配超时意图的委托事件帧。"""
    frame = {
        "channel": "orderEvents",
        "orderId": 637001,
        "symbol": "BTC",
        "settleType": "OPEN",
        "executionType": "LIMIT",
        "side": "BUY",
        "orderStatus": "ORDERED",
        "orderTimestamp": "2026-08-16T00:00:01.000+00:00",
        "orderPrice": "1000000",
        "orderSize": "0.0002",
        "orderExecutedSize": "0",
        "losscutPrice": "0",
        "timeInForce": "FAS",
        "msgType": "NOR",
    }
    path = tmp_path / "ws-events.jsonl"
    path.write_text(json.dumps(frame) + "\n", encoding="utf-8")
    return path


def timeout_ledger(tmp_path: Path) -> Path:
    """构造含一笔超时意图的账本文件。"""
    ledger_path = tmp_path / "intent_ledger.jsonl"
    ledger = IntentLedger(ledger_path)
    intent = OrderIntent(
        intent_id="it01",
        correlation_id="co0001",
        symbol=SpotSymbol("BTC"),
        side=Side.BUY,
        execution_type=ExecutionType.LIMIT,
        size=Decimal("0.0002"),
        price=Decimal("1000000"),
        time_in_force=None,
        created_at=MOMENT,
    )
    ledger.record_intent(intent, at=MOMENT)
    ledger.begin_send("it01", at=MOMENT)
    ledger.mark_send_timeout("it01", reason="发送超时", at=MOMENT)
    return ledger_path


def test_cli_full_round_offline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """单命令一轮：WS 受理、快照裁决、差分决策与端点报告。"""
    reader = SessionReader()
    reader.lookup = (
        rest_order(637001, status=OrderStatus.CANCELED),
    )
    prepare_cli(tmp_path, monkeypatch, reader)
    ledger_path = timeout_ledger(tmp_path)
    report_path = tmp_path / "report.json"
    code = main(
        [
            "--ws-events", str(write_ws_events(tmp_path)),
            "--target", str(write_artifact(tmp_path, 0.6)),
            "--rules", str(write_rules(tmp_path)),
            "--reference-price", "1000000",
            "--service-status", "OPEN",
            "--ledger", str(ledger_path),
            "--breaker-config", str(BREAKER_CONFIG),
            "--session-config", str(SESSION_CONFIG),
            "--env-file", str(tmp_path / "absent.env"),
            "--report", str(report_path),
        ]
    )
    assert code == 0
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["mode"] == "dry-run"
    assert report["ws_channel"]["accepted_intents"] == ["it01"]
    assert report["snapshot"]["mode"] == "audit"
    kinds = [item["kind"] for item in report["snapshot"]["mismatches"]]
    assert kinds == ["stale_active_order"]
    assert report["snapshot"]["counted_into_breaker"] == 1
    assert report["timeouts"] == []
    assert report["position"]["size"] == "0"
    assert report["delta"]["proposal"]["side"] == "BUY"
    assert report["delta"]["proposal"]["size"] == "0.0003"
    assert report["delta"]["no_trade_band"] == "0.01"
    assert report["intent"]["state"] == "DRY_RUN_BLOCKED"
    reads = report["endpoints"]["read_touched"]
    for endpoint in (
        "GET /v1/activeOrders",
        "GET /v1/latestExecutions",
        "GET /v1/account/assets",
        "GET /v1/orders",
    ):
        assert endpoint in reads
    assert report["endpoints"]["write_planned"] == ["POST /v1/order"]
    assert report["endpoints"]["write_touched"] == []
    assert report["breaker"]["state"] == "NORMAL"
    assert report["breaker"]["emergency_stop"] == []
    assert report["settings"]["snapshot_interval_seconds"] == 30
    reloaded = IntentLedger(ledger_path)
    assert reloaded.state("it01") is IntentState.ACCEPTED
    assert reloaded.intent_id_for_order(637001) == "it01"


def test_cli_resolves_timeout_and_band_skips(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """无 WS 事实时基线快照，超时查询自动判定，差分带内跳过。"""
    reader = SessionReader()
    prepare_cli(tmp_path, monkeypatch, reader)
    ledger_path = timeout_ledger(tmp_path)
    report_path = tmp_path / "report.json"
    code = main(
        [
            "--target", str(write_artifact(tmp_path, 0.6)),
            "--rules", str(write_rules(tmp_path)),
            "--reference-price", "1000000",
            "--service-status", "OPEN",
            "--no-trade-band", "0.99",
            "--ledger", str(ledger_path),
            "--breaker-config", str(BREAKER_CONFIG),
            "--session-config", str(SESSION_CONFIG),
            "--env-file", str(tmp_path / "absent.env"),
            "--report", str(report_path),
        ]
    )
    assert code == 0
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["snapshot"]["mode"] == "baseline"
    assert report["snapshot"]["counted_into_breaker"] == 0
    assert report["timeouts"][0]["disposition"] == "resolved"
    assert report["timeouts"][0]["state"] == "FAILED"
    assert report["delta"]["proposal"] is None
    assert "不交易带" in report["delta"]["skip_reason"]
    assert report["intent"] is None
    assert report["endpoints"]["write_planned"] == []
    reloaded = IntentLedger(ledger_path)
    assert reloaded.state("it01") is IntentState.FAILED


def test_cli_ambiguous_timeout_returns_one(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """歧义候选拒绝自动判定，退避排队并返回非零。"""
    reader = SessionReader()
    reader.active = (rest_order(637001), rest_order(637002))
    reader.lookup = reader.active
    prepare_cli(tmp_path, monkeypatch, reader)
    ledger_path = timeout_ledger(tmp_path)
    report_path = tmp_path / "report.json"
    code = main(
        [
            "--service-status", "OPEN",
            "--ledger", str(ledger_path),
            "--breaker-config", str(BREAKER_CONFIG),
            "--session-config", str(SESSION_CONFIG),
            "--env-file", str(tmp_path / "absent.env"),
            "--report", str(report_path),
        ]
    )
    assert code == 1
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["timeouts"][0]["disposition"] == "ambiguous"
    assert report["timeouts"][0]["next_attempt_at"] is not None
    assert report["delta"] is None
    reloaded = IntentLedger(ledger_path)
    assert reloaded.state("it01") is IntentState.SEND_TIMEOUT
