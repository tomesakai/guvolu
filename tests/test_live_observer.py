"""live 伴随观察单测：零写巡视、宽容账本扫描与告警判定。

全部离线（C-13、C-14），不触发任何真实端点。
"""
from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from guvolu.domain.enums import (
    ExecutionType,
    OrderStatus,
    OrderType,
    SettleType,
    Side,
    TimeInForce,
)
from guvolu.domain.models import Asset, Execution, Order, Ticker
from guvolu.domain.symbols import SpotSymbol
from guvolu.execution.authorization_envelope import (
    EnvelopeState,
    load_envelope,
)
from guvolu.execution.live_observer import (
    observe_once,
    scan_ledger_pending,
)

NOW = datetime(2026, 9, 2, 0, 0, tzinfo=UTC)
BTC = SpotSymbol("BTC")


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


def _envelope(tmp_path: Path):
    path = tmp_path / "envelope.json"
    path.write_text(
        json.dumps(_envelope_body(), ensure_ascii=False), encoding="utf-8"
    )
    return load_envelope(path, whitelist=frozenset({BTC}))


def _order(order_id: int, *, age_seconds: float) -> Order:
    return Order(
        root_order_id=order_id,
        order_id=order_id,
        symbol="BTC",
        side=Side.BUY,
        order_type=OrderType.NORMAL,
        execution_type=ExecutionType.LIMIT,
        settle_type=SettleType.OPEN,
        size=Decimal("0.0001"),
        executed_size=Decimal("0"),
        price=Decimal("12000000"),
        losscut_price=Decimal("0"),
        status=OrderStatus.ORDERED,
        cancel_type=None,
        time_in_force=TimeInForce.FAS,
        timestamp=NOW - timedelta(seconds=age_seconds),
    )


class _Reader:
    """READ_ONLY 替身。"""

    def __init__(
        self,
        *,
        active: tuple[Order, ...] = (),
        assets: tuple[Asset, ...] = (),
    ) -> None:
        self._active = active
        self._assets = assets

    def active_orders(
        self, symbol: str, page: int | None = None, count: int | None = None,
    ) -> tuple[Order, ...]:
        return self._active

    def assets(self) -> tuple[Asset, ...]:
        return self._assets

    def orders(self, order_ids: Sequence[int]) -> tuple[Order, ...]:
        return ()

    def executions(
        self,
        order_id: int | None = None,
        execution_ids: Sequence[int] | None = None,
    ) -> tuple[Execution, ...]:
        return ()


class _Public:
    """公开端点替身：固定最新レート。"""

    def __init__(self, last: Decimal = Decimal("12000000")) -> None:
        self._last = last

    def ticker(self, symbol: str) -> tuple[Ticker, ...]:
        return (Ticker(
            symbol=symbol,
            ask=self._last,
            bid=self._last,
            high=self._last,
            low=self._last,
            last=self._last,
            volume=Decimal("1"),
            timestamp=NOW,
        ),)


def _assets(btc: str = "0") -> tuple[Asset, ...]:
    return (
        Asset(
            symbol="JPY",
            amount=Decimal("100000"),
            available=Decimal("100000"),
            conversion_rate=Decimal("1"),
        ),
        Asset(
            symbol="BTC",
            amount=Decimal(btc),
            available=Decimal(btc),
            conversion_rate=Decimal("12000000"),
        ),
    )


def _scheduler_row(
    started: datetime, market: str, *, exit_code: int, live_status: str | None,
    tail: str = "",
) -> str:
    # 失败轮只有回溯文本
    if live_status is not None:
        output = json.dumps({
            "status": "completed",
            "live": {"status": live_status, "returncode": 0},
        })
    else:
        output = "Traceback (most recent call last):\n  ...\n" + tail
    return json.dumps({
        "started_at": started.isoformat(),
        "completed_at": (started + timedelta(minutes=15)).isoformat(),
        "market_id": market,
        "exit_code": exit_code,
        "output": output,
    })


def test_scheduler_health_flags_consecutive_failures_and_silence(
    tmp_path: Path,
) -> None:
    """连续三轮未完成的市场告警；正常市场不告警；长时间无轮次告警。"""
    from guvolu.execution.live_observer import scan_scheduler_health

    now = datetime(2026, 9, 13, 12, 0, tzinfo=UTC)
    lines = [
        _scheduler_row(now - timedelta(hours=3), "", exit_code=0, live_status="completed"),
        _scheduler_row(now - timedelta(hours=2), "", exit_code=0, live_status="reused"),
        _scheduler_row(now - timedelta(hours=1), "", exit_code=0, live_status="completed"),
        _scheduler_row(
            now - timedelta(hours=3), "mkt__gmo__eth__r0", exit_code=1,
            live_status=None, tail="ValueError: GMO panel 输入缺少逐文件控制合同",
        ),
        _scheduler_row(
            now - timedelta(hours=2), "mkt__gmo__eth__r0", exit_code=0,
            live_status="refused",
        ),
        _scheduler_row(
            now - timedelta(hours=1), "mkt__gmo__eth__r0", exit_code=1,
            live_status=None, tail="ValueError: 冻结预测过期: 6443.7s",
        ),
        _scheduler_row(
            now - timedelta(hours=9), "mkt__gmo__xrp__r0", exit_code=0,
            live_status="completed",
        ),
    ]
    log = tmp_path / "live-scheduler.jsonl"
    log.write_text("﻿" + "\n".join(lines) + "\n坏行\n", encoding="utf-8")
    health, alerts = scan_scheduler_health(log, now=now)
    assert health["primary"]["consecutive_incomplete"] == 0
    assert health["mkt__gmo__eth__r0"]["consecutive_incomplete"] == 3
    assert health["mkt__gmo__eth__r0"]["last_reason"] == "ValueError: 冻结预测过期: 6443.7s"
    assert any("ETH" in text and "连续 3 轮" in text for text in alerts)
    assert any("XRP" in text and "无调度轮次" in text for text in alerts)
    assert not any("primary" in text for text in alerts)
    # 缺日志文件不告警
    none_health, none_alerts = scan_scheduler_health(
        tmp_path / "missing.jsonl", now=now
    )
    assert none_health == {} and none_alerts == []


def test_quiet_cycle_reports_ok(tmp_path: Path) -> None:
    """无挂单、无在途、无持仓超限时零告警。"""
    cycle = observe_once(
        reader=_Reader(assets=_assets()),
        public=_Public(),
        envelope=_envelope(tmp_path),
        state=EnvelopeState(),
        ledger_path=tmp_path / "missing_ledger.jsonl",
        now=NOW,
    )
    assert cycle.alerts == ()
    assert cycle.record["status"] == "ok"


def test_stale_active_order_raises_alert(tmp_path: Path) -> None:
    """存续超过执行器闭环时限的挂单必须告警。"""
    cycle = observe_once(
        reader=_Reader(
            active=(_order(9001, age_seconds=600),), assets=_assets(),
        ),
        public=_Public(),
        envelope=_envelope(tmp_path),
        state=EnvelopeState(),
        ledger_path=tmp_path / "missing_ledger.jsonl",
        now=NOW,
    )
    assert any("9001" in alert for alert in cycle.alerts)
    assert cycle.record["status"] == "alert"


def test_position_over_envelope_limit_raises_alert(tmp_path: Path) -> None:
    """持仓名义超过信封 max_position_jpy 必须告警。"""
    cycle = observe_once(
        reader=_Reader(assets=_assets(btc="0.01")),
        public=_Public(),
        envelope=_envelope(tmp_path),
        state=EnvelopeState(),
        ledger_path=tmp_path / "missing_ledger.jsonl",
        now=NOW,
    )
    assert any("持仓名义" in alert for alert in cycle.alerts)


def test_tripped_envelope_state_raises_alert(tmp_path: Path) -> None:
    """信封熔断锁定状态在观察记录中告警留痕。"""
    cycle = observe_once(
        reader=_Reader(assets=_assets()),
        public=_Public(),
        envelope=_envelope(tmp_path),
        state=EnvelopeState(tripped_at=NOW, trip_reason="测试锁定"),
        ledger_path=tmp_path / "missing_ledger.jsonl",
        now=NOW,
    )
    assert any("熔断锁定" in alert for alert in cycle.alerts)


def test_ledger_scan_flags_stale_inflight_without_mutation(
    tmp_path: Path,
) -> None:
    """宽容扫描发现超龄在途意图，且绝不改写账本字节。"""
    path = tmp_path / "intent_ledger.jsonl"
    created = (NOW - timedelta(seconds=900)).isoformat()
    rows = [
        {
            "schema_version": 4, "record": "intent", "at": created,
            "intent_id": "it01", "correlation_id": "co0001",
            "symbol": "BTC", "side": "BUY", "execution_type": "LIMIT",
            "size": "0.0001", "price": "1000000", "time_in_force": None,
            "created_at": created, "prediction_id": None,
            "decision_time": None, "envelope_sha256": None,
        },
        {
            "schema_version": 4, "record": "transition", "at": created,
            "intent_id": "it01", "source": "RECORDED", "target": "SENDING",
            "order_id": None, "reason": None, "evidence": None,
            "write_budget": "consumed",
        },
    ]
    body = "".join(
        json.dumps(row, ensure_ascii=False) + "\n" for row in rows
    )
    # 尾部模拟执行器写到一半的行
    path.write_text(body + '{"record":"tra', encoding="utf-8")
    before = path.read_bytes()
    pending, lines = scan_ledger_pending(
        path, now=NOW, stale_age_seconds=420.0
    )
    assert lines == 2
    assert [item["intent_id"] for item in pending] == ["it01"]
    assert pending[0]["state"] == "SENDING"
    # 不隔离不截断
    assert path.read_bytes() == before
    assert not list(tmp_path.glob("*.partial-*"))
