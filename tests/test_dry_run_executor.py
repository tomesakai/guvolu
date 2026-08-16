"""dry-run 执行器端到端单测：全程离线，绝无网络调用（C-13、C-14）。"""
from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
import requests

from guvolu.api.trade_client import TradeClient
from guvolu.api.transport import (
    HttpMethod,
    Params,
    PrivateTransport,
    RateLimiter,
)
from guvolu.data.intent_ledger import IntentLedger
from guvolu.domain.config import load_config
from guvolu.domain.enums import ExecutionType, RunMode, ServiceStatus, Side
from guvolu.domain.intent import IntentState, OrderIntent
from guvolu.domain.symbols import SpotSymbol
from guvolu.execution.dry_run_executor import (
    ExecutorError,
    build_plan,
    execute_plan,
    load_market_rule,
    load_target_artifact,
    main,
    render_report,
)
from guvolu.execution.trade_sender import TradeClientSender
from guvolu.risk.circuit_breaker import BreakerThresholds, CircuitBreaker
from guvolu.risk.limits import LimitGate

ROOT = Path(__file__).resolve().parents[1]
BREAKER_CONFIG = ROOT / "config" / "circuit_breaker.json"
WHITELIST = frozenset({SpotSymbol("BTC")})
MOMENT = datetime(2026, 8, 16, 0, 0, tzinfo=UTC)
THRESHOLDS = BreakerThresholds(
    schema_version=1,
    consecutive_failure_limit=3,
    stream_gap_seconds=90,
    asset_deviation_ratio=Decimal("0.01"),
    asset_deviation_floor_jpy=Decimal("30"),
)


class RecordingTransport(PrivateTransport):
    """私有传输替身，记录调用，绝不发真实请求（C-14）。"""

    def __init__(self, tmp_path: Path) -> None:
        super().__init__("k", "s", RateLimiter(1000.0), tmp_path)
        self.calls: list[tuple[str, str]] = []

    def request(
        self,
        method: HttpMethod,
        path: str,
        *,
        params: Params | None = None,
        body: Mapping[str, object] | None = None,
    ) -> object:
        self.calls.append((method, path))
        return None


def write_artifact(tmp_path: Path, target: float) -> Path:
    """写出与研究管线同构的 target-position 制品样例。"""
    payload = {
        "schema_version": 12,
        "run_id": "run0testsample01",
        "decision_time": "2026-08-15T21:00:00+00:00",
        "execution_evaluated_at": "2026-08-15T21:05:00+00:00",
        "market_id": "gmo:BTC:spot",
        "research_target_contract": {
            "method_version": 3,
            "unit": "risk_weighted_directional_target",
            "aggregate_target": 0.0,
            "families": [],
        },
        "operational_target_contract": {
            "method_version": 3,
            "unit": "risk_weighted_directional_target",
            "aggregate_target": target,
            "families": [
                {
                    "family": "trend",
                    "deployment_candidate_id": "cand01",
                    "eligible": True,
                    "family_target": target,
                    "allocation_weight": 1.0,
                    "portfolio_target_contribution": target,
                }
            ],
        },
    }
    path = tmp_path / "target-position-sample.json"
    path.write_text(
        json.dumps(payload, ensure_ascii=False), encoding="utf-8"
    )
    return path


def write_rules(tmp_path: Path) -> Path:
    """写出取引ルール快照样例。"""
    rows = [
        {
            "symbol": "BTC",
            "minOrderSize": "0.0001",
            "maxOrderSize": "5",
            "sizeStep": "0.0001",
            "tickSize": "1",
            "takerFee": "0.0005",
            "makerFee": "-0.0001",
        },
        {
            "symbol": "BTC_JPY",
            "minOrderSize": "0.01",
            "maxOrderSize": "5",
            "sizeStep": "0.01",
            "tickSize": "1",
            "takerFee": "0",
            "makerFee": "0",
        },
    ]
    path = tmp_path / "symbol-rules.json"
    path.write_text(json.dumps(rows), encoding="utf-8")
    return path


def read_ledger_records(path: Path) -> list[dict[str, Any]]:
    """读取账本全部事件行。"""
    lines = path.read_text(encoding="utf-8").splitlines()
    return [json.loads(line) for line in lines]


def forbid_network(monkeypatch: pytest.MonkeyPatch) -> None:
    """封锁 requests 会话层，任何真实请求立即失败（C-14）。"""

    def explode(*args: object, **kwargs: object) -> object:
        raise AssertionError("测试内禁止真实网络调用")

    monkeypatch.setattr(requests.Session, "request", explode)
    monkeypatch.setattr(requests.Session, "get", explode)


def test_load_target_artifact_reads_operational_contract(
    tmp_path: Path,
) -> None:
    """装载制品并取运行快照目标。"""
    artifact = load_target_artifact(write_artifact(tmp_path, 0.6))
    assert artifact.run_id == "run0testsample01"
    assert artifact.market_id == "gmo:BTC:spot"
    assert artifact.aggregate_target == 0.6


def test_load_target_artifact_rejects_wrong_unit(tmp_path: Path) -> None:
    """目标口径不符即拒绝。"""
    path = tmp_path / "bad.json"
    payload = json.loads(write_artifact(tmp_path, 0.6).read_text("utf-8"))
    payload["operational_target_contract"]["unit"] = "raw_position"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ExecutorError, match="口径"):
        load_target_artifact(path)


def test_load_target_artifact_rejects_missing_contract(
    tmp_path: Path,
) -> None:
    """缺少运行快照契约即拒绝。"""
    path = tmp_path / "empty.json"
    path.write_text("{}", encoding="utf-8")
    with pytest.raises(ExecutorError, match="operational_target_contract"):
        load_target_artifact(path)


def test_load_market_rule_finds_spot_symbol(tmp_path: Path) -> None:
    """快照中按品种取规则，缺失即拒绝。"""
    rules_path = write_rules(tmp_path)
    rule = load_market_rule(rules_path, SpotSymbol("BTC"))
    assert rule.tick_size == Decimal("1")
    assert rule.size_step == Decimal("0.0001")
    with pytest.raises(ExecutorError, match="缺少品种"):
        load_market_rule(rules_path, SpotSymbol("ETH"))


def test_end_to_end_dry_run_blocks_at_send_boundary(
    tmp_path: Path,
) -> None:
    """端到端彩排：制品到发送边界，账本留痕，零网络调用。"""
    artifact = load_target_artifact(write_artifact(tmp_path, 0.6))
    rule = load_market_rule(write_rules(tmp_path), SpotSymbol("BTC"))
    plan = build_plan(
        artifact,
        rule=rule,
        reference_price=Decimal("1000000"),
        budget_jpy=Decimal("500"),
    )
    assert plan.proposal is not None
    assert plan.proposal.side is Side.BUY
    assert plan.proposal.size == Decimal("0.0003")
    ledger_path = tmp_path / "ledger" / "intent_ledger.jsonl"
    ledger = IntentLedger(ledger_path)
    transport = RecordingTransport(tmp_path)
    sender = TradeClientSender(
        TradeClient(transport, RunMode.DRY_RUN, WHITELIST)
    )
    outcome = execute_plan(
        plan,
        ledger=ledger,
        limit_gate=LimitGate(load_config(tmp_path / "absent.env").limits),
        breaker=CircuitBreaker(THRESHOLDS),
        service_status=ServiceStatus.OPEN,
        whitelist=WHITELIST,
        sender=sender,
        moment=MOMENT,
    )
    assert outcome is not None
    intent, result = outcome
    assert result.state is IntentState.DRY_RUN_BLOCKED
    assert transport.calls == []
    records = read_ledger_records(ledger_path)
    assert [record["record"] for record in records] == [
        "intent", "transition", "transition"
    ]
    assert records[0]["intent_id"] == intent.intent_id
    assert records[0]["size"] == "0.0003"
    assert (records[1]["source"], records[1]["target"]) == (
        "RECORDED", "SENDING"
    )
    assert (records[2]["source"], records[2]["target"]) == (
        "SENDING", "DRY_RUN_BLOCKED"
    )
    report = render_report(
        plan,
        outcome,
        mode=RunMode.DRY_RUN,
        service_status=ServiceStatus.OPEN,
        ledger_path=ledger_path,
        read_endpoints=(),
    )
    endpoints = report["endpoints"]
    assert isinstance(endpoints, dict)
    assert endpoints["write_planned"] == ["POST /v1/order"]
    assert endpoints["write_touched"] == []
    proposal = report["proposal"]
    assert isinstance(proposal, dict)
    assert proposal["symbol"] == "BTC"
    assert proposal["side"] == "BUY"
    assert proposal["size"] == "0.0003"
    assert Decimal(proposal["notional_jpy"]) == Decimal("300")


def test_cli_dry_run_offline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """命令行离线彩排：预期终点返回零，报告与账本齐备。"""
    forbid_network(monkeypatch)
    monkeypatch.setenv("GMO_COIN_TRADE_API_KEY", "dummy-key")
    monkeypatch.setenv("GMO_COIN_TRADE_API_SECRET", "dummy-secret")
    monkeypatch.delenv("GUVOLU_MODE", raising=False)
    monkeypatch.setenv("GUVOLU_LOG_DIR", str(tmp_path / "logs"))
    target = write_artifact(tmp_path, 0.6)
    rules = write_rules(tmp_path)
    ledger_path = tmp_path / "intent_ledger.jsonl"
    report_path = tmp_path / "report.json"
    code = main(
        [
            "--target", str(target),
            "--rules", str(rules),
            "--reference-price", "1000000",
            "--service-status", "OPEN",
            "--ledger", str(ledger_path),
            "--breaker-config", str(BREAKER_CONFIG),
            "--env-file", str(tmp_path / "absent.env"),
            "--dry-run-report", str(report_path),
        ]
    )
    assert code == 0
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["mode"] == "dry-run"
    assert report["artifact"]["run_id"] == "run0testsample01"
    assert report["proposal"]["side"] == "BUY"
    assert report["intent"]["state"] == "DRY_RUN_BLOCKED"
    assert report["endpoints"]["read_touched"] == []
    assert report["endpoints"]["write_planned"] == ["POST /v1/order"]
    assert report["endpoints"]["write_touched"] == []
    records = read_ledger_records(ledger_path)
    assert records[-1]["target"] == "DRY_RUN_BLOCKED"


def test_cli_gate_reject_returns_one(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """限额拒绝不是预期终点，返回非零（T-11）。"""
    forbid_network(monkeypatch)
    monkeypatch.setenv("GMO_COIN_TRADE_API_KEY", "dummy-key")
    monkeypatch.setenv("GMO_COIN_TRADE_API_SECRET", "dummy-secret")
    monkeypatch.delenv("GUVOLU_MODE", raising=False)
    monkeypatch.delenv("GUVOLU_ORDER_JPY_MAX", raising=False)
    monkeypatch.setenv("GUVOLU_LOG_DIR", str(tmp_path / "logs"))
    ledger_path = tmp_path / "intent_ledger.jsonl"
    report_path = tmp_path / "report.json"
    code = main(
        [
            "--target", str(write_artifact(tmp_path, 1.0)),
            "--rules", str(write_rules(tmp_path)),
            "--reference-price", "1000000",
            "--service-status", "OPEN",
            "--budget-jpy", "600",
            "--ledger", str(ledger_path),
            "--breaker-config", str(BREAKER_CONFIG),
            "--env-file", str(tmp_path / "absent.env"),
            "--dry-run-report", str(report_path),
        ]
    )
    assert code == 1
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["intent"]["state"] == "GATE_REJECTED"
    assert "限额" in report["intent"]["reason"] or "超上限" in report[
        "intent"
    ]["reason"]
    records = read_ledger_records(ledger_path)
    assert records[-1]["target"] == "GATE_REJECTED"


def test_cli_zero_target_skips_intent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """零目标不生成意图，账本不落盘。"""
    forbid_network(monkeypatch)
    monkeypatch.setenv("GMO_COIN_TRADE_API_KEY", "dummy-key")
    monkeypatch.setenv("GMO_COIN_TRADE_API_SECRET", "dummy-secret")
    monkeypatch.delenv("GUVOLU_MODE", raising=False)
    monkeypatch.setenv("GUVOLU_LOG_DIR", str(tmp_path / "logs"))
    ledger_path = tmp_path / "intent_ledger.jsonl"
    report_path = tmp_path / "report.json"
    code = main(
        [
            "--target", str(write_artifact(tmp_path, 0.0)),
            "--rules", str(write_rules(tmp_path)),
            "--reference-price", "1000000",
            "--service-status", "OPEN",
            "--ledger", str(ledger_path),
            "--breaker-config", str(BREAKER_CONFIG),
            "--env-file", str(tmp_path / "absent.env"),
            "--dry-run-report", str(report_path),
        ]
    )
    assert code == 0
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["proposal"] is None
    assert report["intent"] is None
    assert report["skip_reason"] == "目标为零，无需委托"
    assert report["endpoints"]["write_planned"] == []
    assert not ledger_path.exists()


def test_cli_resolve_timeouts_offline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """对账模式经注入只读替身处置超时意图（T-06）。"""
    forbid_network(monkeypatch)
    ledger_path = tmp_path / "intent_ledger.jsonl"
    ledger = IntentLedger(ledger_path)
    intent = OrderIntent(
        intent_id="it01",
        correlation_id="co0001",
        symbol=SpotSymbol("BTC"),
        side=Side.BUY,
        execution_type=ExecutionType.LIMIT,
        size=Decimal("0.0001"),
        price=Decimal("1000000"),
        time_in_force=None,
        created_at=MOMENT,
    )
    ledger.record_intent(intent, at=MOMENT)
    ledger.begin_send("it01", at=MOMENT)
    ledger.mark_send_timeout("it01", reason="发送超时", at=MOMENT)

    class EmptyReader:
        """只读替身：无挂单亦无成交。"""

        def active_orders(
            self,
            symbol: str,
            page: int | None = None,
            count: int | None = None,
        ) -> tuple[object, ...]:
            return ()

        def latest_executions(
            self,
            symbol: str,
            page: int | None = None,
            count: int | None = None,
        ) -> tuple[object, ...]:
            return ()

    class FakeReadClient:
        """替身工厂，屏蔽真实构造。"""

        @classmethod
        def from_config(cls, config: object) -> EmptyReader:
            return EmptyReader()

    monkeypatch.setattr(
        "guvolu.execution.dry_run_executor.ReadClient", FakeReadClient
    )
    code = main(
        [
            "--resolve-timeouts",
            "--ledger", str(ledger_path),
            "--env-file", str(tmp_path / "absent.env"),
        ]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["ambiguous"] == []
    assert payload["resolved"][0]["intent_id"] == "it01"
    assert payload["resolved"][0]["state"] == "FAILED"
    reloaded = IntentLedger(ledger_path)
    assert reloaded.state("it01") is IntentState.FAILED
