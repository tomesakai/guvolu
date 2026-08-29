"""dry-run 执行器端到端单测：全程离线，绝无网络调用（C-13、C-14）。"""
from __future__ import annotations

import hashlib
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


def write_v2_target(
    tmp_path: Path,
    *,
    decision_time: str = "2026-08-15T23:30:00+00:00",
    market_id: str = "mkt__gmo__btc__r0",
    symbol: str = "BTC",
    target: float = 0.6,
    risk_budget_jpy: Decimal = Decimal("500"),
) -> Path:
    """经公共 adapter 写出真实内容寻址 v2 目标。"""
    from guvolu.execution.frozen_target_adapter import (
        persist_operational_target,
    )

    prediction_id = "frozen-forward-prediction-" + "1" * 64
    prediction = {
        "schema_version": 1,
        "scope": "FROZEN_FORWARD",
        "prediction_id": prediction_id,
        "plan_id": "frozen-forward-plan-" + "2" * 64,
        "decision_time": decision_time,
        "input_head_generation": "sha256-" + "3" * 64,
        "aggregate_target": target,
        "unit": "risk_weighted_directional_target",
        "quality": {
            "clock": True,
            "coverage": True,
            "eligible": True,
            "freshness": True,
            "integrity": True,
            "lineage": True,
            "pit": True,
            "reasons": [],
        },
        "families": [],
        "reserve": 0.4,
    }
    prediction_path = tmp_path / "prediction.json"
    prediction_path.write_text(
        json.dumps(prediction, ensure_ascii=False), encoding="utf-8"
    )
    return persist_operational_target(
        prediction_path,
        tmp_path / "targets",
        market_id=market_id,
        symbol=SpotSymbol(symbol),
        risk_budget_jpy=risk_budget_jpy,
        mode="dry-run",
    )[0]


def rewrite_content_addressed_target(
    path: Path, payload: Mapping[str, object],
) -> Path:
    """按 adapter canonical 编码重写篡改反例的正确内容文件名。"""
    raw = (
        json.dumps(
            dict(payload), ensure_ascii=False,
            separators=(",", ":"), sort_keys=True,
        ) + "\n"
    ).encode("utf-8")
    digest = hashlib.sha256(raw).hexdigest()
    target = path.parent / f"target-{digest}.json"
    target.write_bytes(raw)
    return target


def source_prediction_arguments(target: Path) -> list[str]:
    """从合法目标定位来源，但 SHA 由编排侧直接对来源字节计算。"""
    payload = json.loads(target.read_text(encoding="utf-8"))
    source = Path(payload["lineage"]["source_prediction_path"])
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    return [
        "--source-prediction", str(source),
        "--source-prediction-sha256", digest,
    ]


def write_target_config(tmp_path: Path, *, budget: str = "500") -> Path:
    """复制受版本控制配置，只覆盖测试需要的名义预算。"""
    payload = json.loads(
        (ROOT / "config" / "paper_executor.json").read_text(encoding="utf-8")
    )
    payload["risk_budget_jpy"] = budget
    path = tmp_path / f"paper-executor-{budget}.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
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
    assert artifact.decision_time == datetime(
        2026, 8, 15, 21, 0, tzinfo=UTC
    )
    digest = hashlib.sha256(
        b"guvolu-prediction:run0testsample01"
    ).hexdigest()
    assert artifact.correlation_id == f"co{digest[:16]}"
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


@pytest.mark.parametrize("target", [-0.1, 1.1, float("inf"), float("nan")])
def test_load_target_artifact_rejects_invalid_target(
    tmp_path: Path, target: float,
) -> None:
    """执行目标只能是 [0,1] 内有限数值。"""
    with pytest.raises(ExecutorError, match="有限数值"):
        load_target_artifact(write_artifact(tmp_path, target))


def test_load_target_artifact_rejects_missing_contract(
    tmp_path: Path,
) -> None:
    """缺少运行快照契约即拒绝。"""
    path = tmp_path / "empty.json"
    path.write_text("{}", encoding="utf-8")
    with pytest.raises(ExecutorError, match="operational_target_contract"):
        load_target_artifact(path)


def test_load_target_artifact_rejects_naive_decision_time(
    tmp_path: Path,
) -> None:
    """决策血缘时刻没有时区时失败关闭。"""
    path = write_artifact(tmp_path, 0.6)
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["decision_time"] = "2026-08-15T21:00:00"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ExecutorError, match="缺少时区"):
        load_target_artifact(path)


def test_load_target_artifact_derives_stable_legacy_correlation(
    tmp_path: Path,
) -> None:
    """旧目标没有 correlation_id 时仍由 run_id 稳定回链。"""
    path = write_artifact(tmp_path, 0.6)
    payload = json.loads(path.read_text(encoding="utf-8"))
    path.write_text(json.dumps(payload), encoding="utf-8")
    artifact = load_target_artifact(path)
    digest = hashlib.sha256(
        b"guvolu-prediction:run0testsample01"
    ).hexdigest()
    assert artifact.correlation_id == f"co{digest[:16]}"


@pytest.mark.parametrize("value", [None, "", "   ", 7])
def test_load_target_artifact_rejects_explicit_invalid_correlation(
    tmp_path: Path, value: object,
) -> None:
    """显式 null、空白或错型不得伪装成 legacy 缺省。"""
    path = write_artifact(tmp_path, 0.6)
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["correlation_id"] = value
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ExecutorError, match="correlation_id"):
        load_target_artifact(path)


def test_v2_target_cannot_downgrade_to_legacy_by_removing_markers(
    tmp_path: Path,
) -> None:
    """保留任一 v2 专属字段时必须走严格 v2 合同。"""
    target = write_v2_target(tmp_path)
    payload = json.loads(target.read_text(encoding="utf-8"))
    payload["schema_version"] = 1
    del payload["artifact_kind"]
    downgraded = tmp_path / "downgraded.json"
    downgraded.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ExecutorError, match="v2 执行目标结构"):
        load_target_artifact(downgraded)


def test_v2_adapter_correlation_is_rebuilt_from_run_id(
    tmp_path: Path,
) -> None:
    """合法形态但错误的 adapter correlation 仍失败关闭。"""
    target = write_v2_target(tmp_path)
    payload = json.loads(target.read_text(encoding="utf-8"))
    payload["correlation_id"] = "coffffffffffffffff"
    tampered = rewrite_content_addressed_target(target, payload)
    with pytest.raises(ExecutorError, match="correlation_id 与 run_id"):
        load_target_artifact(tampered)


def test_public_main_rejects_unknown_target_semantics_before_side_effect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """重寻址目标也不得用未知语义字段扩展 adapter 合同。"""
    forbid_network(monkeypatch)
    monkeypatch.delenv("GUVOLU_MODE", raising=False)
    target = write_v2_target(tmp_path)
    payload = json.loads(target.read_text(encoding="utf-8"))
    payload["target_semantics"]["future_short_override"] = True
    tampered = rewrite_content_addressed_target(target, payload)
    ledger_path = tmp_path / "intent_ledger.jsonl"
    report_path = tmp_path / "report.json"
    with pytest.raises(ExecutorError, match="target_semantics"):
        main(
            [
                "--target", str(tampered),
                *source_prediction_arguments(tampered),
                "--target-config", str(ROOT / "config" / "paper_executor.json"),
                "--rules", str(tmp_path / "must-not-read-rules.json"),
                "--reference-price", "1000000",
                "--service-status", "OPEN",
                "--ledger", str(ledger_path),
                "--breaker-config", str(BREAKER_CONFIG),
                "--env-file", str(tmp_path / "absent.env"),
                "--dry-run-report", str(report_path),
            ],
            moment=MOMENT,
        )
    assert not ledger_path.exists()
    assert not report_path.exists()


@pytest.mark.parametrize("attack", ["source_flip", "coherent_run_tamper"])
def test_public_main_rebuilds_v2_identity_from_source_prediction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    attack: str,
) -> None:
    """重寻址目标仍须与编排侧固定的来源预测逐字段同构。"""
    forbid_network(monkeypatch)
    monkeypatch.delenv("GUVOLU_MODE", raising=False)
    target = write_v2_target(tmp_path)
    payload = json.loads(target.read_text(encoding="utf-8"))
    if attack == "source_flip":
        payload["correlation_id_source"] = "prediction"
        payload["correlation_id"] = "coffffffffffffffff"
        message = "correlation 血缘"
    else:
        forged_id = "frozen-forward-prediction-" + "9" * 64
        payload["run_id"] = forged_id
        payload["lineage"]["prediction_id"] = forged_id
        digest = hashlib.sha256(
            f"guvolu-prediction:{forged_id}".encode("utf-8")
        ).hexdigest()
        payload["correlation_id"] = f"co{digest[:16]}"
        message = "身份/时点"
    tampered = rewrite_content_addressed_target(target, payload)
    ledger_path = tmp_path / "intent_ledger.jsonl"
    report_path = tmp_path / "report.json"
    with pytest.raises(ExecutorError, match=message):
        main(
            [
                "--target", str(tampered),
                *source_prediction_arguments(tampered),
                "--target-config", str(ROOT / "config" / "paper_executor.json"),
                "--rules", str(tmp_path / "must-not-read-rules.json"),
                "--reference-price", "1000000",
                "--service-status", "OPEN",
                "--ledger", str(ledger_path),
                "--breaker-config", str(BREAKER_CONFIG),
                "--env-file", str(tmp_path / "absent.env"),
                "--dry-run-report", str(report_path),
            ],
            moment=MOMENT,
        )
    assert not ledger_path.exists()
    assert not report_path.exists()


@pytest.mark.parametrize("attack", ["semantics", "exposure"])
def test_public_main_reuses_adapter_source_contract_validation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    attack: str,
) -> None:
    """来源升级为 v2 后，执行入口不得遗漏 adapter 的语义合同。"""
    forbid_network(monkeypatch)
    monkeypatch.delenv("GUVOLU_MODE", raising=False)
    target = write_v2_target(tmp_path)
    target_payload = json.loads(target.read_text(encoding="utf-8"))
    source = Path(target_payload["lineage"]["source_prediction_path"])
    source_payload = json.loads(source.read_text(encoding="utf-8"))
    source_payload["schema_version"] = 2
    source_payload["target_semantics"] = {
        "domain": "long_only_spot",
        "range": [0, 1],
        "reference": "fraction_of_risk_budget",
        "short_allowed": attack == "semantics",
    }
    source_payload["exposure_target"] = (
        0.9 if attack == "exposure" else source_payload["aggregate_target"]
    )
    source.write_text(json.dumps(source_payload), encoding="utf-8")
    source_sha = hashlib.sha256(source.read_bytes()).hexdigest()
    target_payload["lineage"]["source_prediction_sha256"] = source_sha
    tampered = rewrite_content_addressed_target(target, target_payload)
    ledger_path = tmp_path / "intent_ledger.jsonl"
    report_path = tmp_path / "report.json"
    expected = "target_semantics" if attack == "semantics" else "exposure_target"
    with pytest.raises(ExecutorError, match=expected):
        main(
            [
                "--target", str(tampered),
                *source_prediction_arguments(tampered),
                "--target-config", str(ROOT / "config" / "paper_executor.json"),
                "--rules", str(tmp_path / "must-not-read-rules.json"),
                "--reference-price", "1000000",
                "--service-status", "OPEN",
                "--ledger", str(ledger_path),
                "--breaker-config", str(BREAKER_CONFIG),
                "--env-file", str(tmp_path / "absent.env"),
                "--dry-run-report", str(report_path),
            ],
            moment=MOMENT,
        )
    assert not ledger_path.exists()
    assert not report_path.exists()


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
    assert records[0]["correlation_id"] == artifact.correlation_id
    assert records[0]["prediction_id"] == artifact.run_id
    assert records[0]["decision_time"] == artifact.decision_time.isoformat()
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


def test_adapter_v2_target_reaches_ledger_with_exact_lineage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """公共 adapter v2 经 main 到 ledger 保持同一决策身份。"""
    forbid_network(monkeypatch)
    monkeypatch.delenv("GUVOLU_MODE", raising=False)
    monkeypatch.setenv("GUVOLU_LOG_DIR", str(tmp_path / "logs"))
    prediction_id = "frozen-forward-prediction-" + "1" * 64
    target_path = write_v2_target(tmp_path)
    artifact = load_target_artifact(target_path)
    assert artifact.run_id == prediction_id
    assert artifact.symbol == SpotSymbol("BTC")
    assert artifact.risk_budget_jpy == Decimal("500")
    ledger_path = tmp_path / "ledger" / "intent_ledger.jsonl"
    report_path = tmp_path / "report.json"
    code = main(
        [
            "--target", str(target_path),
            *source_prediction_arguments(target_path),
            "--target-config", str(ROOT / "config" / "paper_executor.json"),
            "--rules", str(write_rules(tmp_path)),
            "--reference-price", "1000000",
            "--service-status", "OPEN",
            "--ledger", str(ledger_path),
            "--breaker-config", str(BREAKER_CONFIG),
            "--env-file", str(tmp_path / "absent.env"),
            "--dry-run-report", str(report_path),
        ],
        moment=MOMENT,
    )
    assert code == 0
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["mode"] == "dry-run"
    assert report["intent"]["state"] == "DRY_RUN_BLOCKED"
    assert report["endpoints"]["write_touched"] == []
    records = read_ledger_records(ledger_path)
    assert records[0]["prediction_id"] == prediction_id
    assert records[0]["decision_time"] == "2026-08-15T23:30:00+00:00"
    assert records[0]["correlation_id"] == artifact.correlation_id


@pytest.mark.parametrize("target_version", ["legacy", "v2"])
def test_public_main_rejects_live_mode_before_any_side_effect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    target_version: str,
) -> None:
    """无论目标版本，live 环境都不得构造发送链或留下执行制品。"""
    forbid_network(monkeypatch)
    monkeypatch.setenv("GUVOLU_MODE", "live")
    monkeypatch.setenv("GUVOLU_LOG_DIR", str(tmp_path / "logs"))
    target_path = (
        write_v2_target(tmp_path)
        if target_version == "v2"
        else write_artifact(tmp_path, 0.6)
    )
    ledger_path = tmp_path / "ledger" / "intent_ledger.jsonl"
    report_path = tmp_path / "report.json"
    with pytest.raises(ExecutorError, match="拒绝非 dry-run"):
        main(
            [
                "--target", str(target_path),
                "--target-config", str(ROOT / "config" / "paper_executor.json"),
                "--rules", str(tmp_path / "must-not-read-rules.json"),
                "--reference-price", "1000000",
                "--service-status", "OPEN",
                "--ledger", str(ledger_path),
                "--breaker-config", str(BREAKER_CONFIG),
                "--env-file", str(tmp_path / "absent.env"),
                "--dry-run-report", str(report_path),
            ],
            moment=MOMENT,
        )
    assert not report_path.exists()
    assert not ledger_path.exists()
    assert not (tmp_path / "logs").exists()


def test_public_main_rejects_legacy_target_as_execution_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """旧目标只保留为库级兼容输入，公共执行入口不接受其作为证据。"""
    forbid_network(monkeypatch)
    monkeypatch.delenv("GUVOLU_MODE", raising=False)
    ledger_path = tmp_path / "intent_ledger.jsonl"
    report_path = tmp_path / "report.json"
    with pytest.raises(ExecutorError, match="只接受可重建来源血缘"):
        main(
            [
                "--target", str(write_artifact(tmp_path, 0.6)),
                "--rules", str(tmp_path / "must-not-read-rules.json"),
                "--reference-price", "1000000",
                "--service-status", "OPEN",
                "--ledger", str(ledger_path),
                "--env-file", str(tmp_path / "absent.env"),
                "--dry-run-report", str(report_path),
            ],
            moment=MOMENT,
        )
    assert not ledger_path.exists()
    assert not report_path.exists()


@pytest.mark.parametrize(
    "execution_moment",
    [
        datetime(2026, 8, 16, 0, 30, tzinfo=UTC),
        datetime(2026, 8, 27, 0, 0, tzinfo=UTC),
    ],
)
def test_public_main_rejects_expired_v2_before_any_side_effect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    execution_moment: datetime,
) -> None:
    """v2 有效期为半开区间，终点本身及其后都失败关闭。"""
    forbid_network(monkeypatch)
    monkeypatch.delenv("GUVOLU_MODE", raising=False)
    monkeypatch.setenv("GUVOLU_LOG_DIR", str(tmp_path / "logs"))
    ledger_path = tmp_path / "ledger" / "intent_ledger.jsonl"
    report_path = tmp_path / "report.json"
    target_path = write_v2_target(tmp_path)
    with pytest.raises(ExecutorError, match="已经过期"):
        main(
            [
                "--target", str(target_path),
                *source_prediction_arguments(target_path),
                "--target-config", str(ROOT / "config" / "paper_executor.json"),
                "--rules", str(tmp_path / "must-not-read-rules.json"),
                "--reference-price", "1000000",
                "--service-status", "OPEN",
                "--ledger", str(ledger_path),
                "--breaker-config", str(BREAKER_CONFIG),
                "--env-file", str(tmp_path / "absent.env"),
                "--dry-run-report", str(report_path),
            ],
            moment=execution_moment,
        )
    assert not report_path.exists()
    assert not ledger_path.exists()


def test_public_main_rejects_v2_market_mismatch_before_any_side_effect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """其他 venue 的目标不能流入版本化 GMO 执行配置。"""
    forbid_network(monkeypatch)
    monkeypatch.delenv("GUVOLU_MODE", raising=False)
    monkeypatch.setenv("GUVOLU_LOG_DIR", str(tmp_path / "logs"))
    target_path = write_v2_target(
        tmp_path, market_id="mkt__kraken__btc__r0",
    )
    ledger_path = tmp_path / "ledger" / "intent_ledger.jsonl"
    report_path = tmp_path / "report.json"
    with pytest.raises(ExecutorError, match="market/symbol"):
        main(
            [
                "--target", str(target_path),
                *source_prediction_arguments(target_path),
                "--target-config", str(ROOT / "config" / "paper_executor.json"),
                "--rules", str(tmp_path / "must-not-read-rules.json"),
                "--reference-price", "1000000",
                "--service-status", "OPEN",
                "--ledger", str(ledger_path),
                "--breaker-config", str(BREAKER_CONFIG),
                "--env-file", str(tmp_path / "absent.env"),
                "--dry-run-report", str(report_path),
            ],
            moment=MOMENT,
        )
    assert not report_path.exists()
    assert not ledger_path.exists()


def test_cli_dry_run_offline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """命令行离线彩排：预期终点返回零，报告与账本齐备。"""
    forbid_network(monkeypatch)
    monkeypatch.delenv("GMO_COIN_TRADE_API_KEY", raising=False)
    monkeypatch.delenv("GMO_COIN_TRADE_API_SECRET", raising=False)
    monkeypatch.delenv("GUVOLU_MODE", raising=False)
    monkeypatch.setenv("GUVOLU_LOG_DIR", str(tmp_path / "logs"))
    target = write_v2_target(tmp_path, target=0.6)
    artifact = load_target_artifact(target)
    rules = write_rules(tmp_path)
    ledger_path = tmp_path / "intent_ledger.jsonl"
    report_path = tmp_path / "reports" / "report.json"
    code = main(
        [
            "--target", str(target),
            *source_prediction_arguments(target),
            "--target-config", str(ROOT / "config" / "paper_executor.json"),
            "--rules", str(rules),
            "--reference-price", "1000000",
            "--service-status", "OPEN",
            "--ledger", str(ledger_path),
            "--breaker-config", str(BREAKER_CONFIG),
            "--env-file", str(tmp_path / "absent.env"),
            "--dry-run-report", str(report_path),
        ],
        moment=MOMENT,
    )
    assert code == 0
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["mode"] == "dry-run"
    assert report["artifact"]["run_id"] == artifact.run_id
    assert report["proposal"]["side"] == "BUY"
    assert report["intent"]["state"] == "DRY_RUN_BLOCKED"
    assert report["intent"]["correlation_id"] == artifact.correlation_id
    assert report["endpoints"]["read_touched"] == []
    assert report["endpoints"]["write_planned"] == ["POST /v1/order"]
    assert report["endpoints"]["write_touched"] == []
    records = read_ledger_records(ledger_path)
    assert records[0]["prediction_id"] == artifact.run_id
    assert records[0]["decision_time"] == artifact.decision_time.isoformat()
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
    target = write_v2_target(
        tmp_path, target=1.0, risk_budget_jpy=Decimal("600"),
    )
    target_config = write_target_config(tmp_path, budget="600")
    code = main(
        [
            "--target", str(target),
            *source_prediction_arguments(target),
            "--target-config", str(target_config),
            "--rules", str(write_rules(tmp_path)),
            "--reference-price", "1000000",
            "--service-status", "OPEN",
            "--budget-jpy", "600",
            "--ledger", str(ledger_path),
            "--breaker-config", str(BREAKER_CONFIG),
            "--env-file", str(tmp_path / "absent.env"),
            "--dry-run-report", str(report_path),
        ],
        moment=MOMENT,
    )
    assert code == 1
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["intent"]["state"] == "GATE_REJECTED"
    assert "限额" in report["intent"]["reason"] or "超上限" in report[
        "intent"
    ]["reason"]
    records = read_ledger_records(ledger_path)
    assert records[-1]["target"] == "GATE_REJECTED"


def seed_budget_intents(ledger_path: Path, *, consumed: bool) -> None:
    """预置四笔各 500 JPY 的当日意图，按参数标记写预算。"""
    ledger = IntentLedger(ledger_path)
    for index in range(4):
        intent_id = f"it-seed-{index}"
        ledger.record_intent(
            OrderIntent(
                intent_id=intent_id,
                correlation_id="co-seed",
                symbol=SpotSymbol("BTC"),
                side=Side.BUY,
                execution_type=ExecutionType.LIMIT,
                size=Decimal("0.0005"),
                price=Decimal("1000000"),
                time_in_force=None,
                created_at=MOMENT,
            ),
            at=MOMENT,
        )
        ledger.begin_send(
            intent_id, consumes_write_budget=consumed, at=MOMENT
        )
        if consumed:
            ledger.accept(intent_id, 637100 + index, at=MOMENT)
        else:
            ledger.block_dry_run(intent_id, reason="模拟拦截", at=MOMENT)


def run_cli_with_seeded_ledger(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    consumed: bool,
) -> tuple[int, dict[str, Any]]:
    """预置账本后离线跑一次命令行，返回退出码与报告。"""
    forbid_network(monkeypatch)
    monkeypatch.delenv("GMO_COIN_TRADE_API_KEY", raising=False)
    monkeypatch.delenv("GMO_COIN_TRADE_API_SECRET", raising=False)
    monkeypatch.delenv("GUVOLU_MODE", raising=False)
    monkeypatch.delenv("GUVOLU_DAY_JPY_MAX", raising=False)
    monkeypatch.delenv("GUVOLU_DAY_COUNT_MAX", raising=False)
    monkeypatch.delenv("GUVOLU_ORDER_JPY_MAX", raising=False)
    monkeypatch.setenv("GUVOLU_LOG_DIR", str(tmp_path / "logs"))
    ledger_path = tmp_path / "intent_ledger.jsonl"
    seed_budget_intents(ledger_path, consumed=consumed)
    target = write_v2_target(tmp_path, target=0.6)
    report_path = tmp_path / "report.json"
    code = main(
        [
            "--target", str(target),
            *source_prediction_arguments(target),
            "--target-config", str(ROOT / "config" / "paper_executor.json"),
            "--rules", str(write_rules(tmp_path)),
            "--reference-price", "1000000",
            "--service-status", "OPEN",
            "--ledger", str(ledger_path),
            "--breaker-config", str(BREAKER_CONFIG),
            "--env-file", str(tmp_path / "absent.env"),
            "--dry-run-report", str(report_path),
        ],
        moment=MOMENT,
    )
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert isinstance(report, dict)
    return code, report


def test_cli_replays_consumed_day_usage_and_rejects_over_budget(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """启动重放当日已消耗用量，超预算即闸门拒绝（T-11）。"""
    code, report = run_cli_with_seeded_ledger(
        tmp_path, monkeypatch, consumed=True
    )
    assert code == 1
    assert report["intent"]["state"] == "GATE_REJECTED"
    assert "当日累计" in report["intent"]["reason"]


def test_cli_replay_ignores_zero_write_terminal_intents(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """零写终态不占预算：同额度旧行不再触发限额拒绝（T-11）。"""
    code, report = run_cli_with_seeded_ledger(
        tmp_path, monkeypatch, consumed=False
    )
    assert code == 0
    assert report["intent"]["state"] == "DRY_RUN_BLOCKED"


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
    target = write_v2_target(tmp_path, target=0.0)
    code = main(
        [
            "--target", str(target),
            *source_prediction_arguments(target),
            "--target-config", str(ROOT / "config" / "paper_executor.json"),
            "--rules", str(write_rules(tmp_path)),
            "--reference-price", "1000000",
            "--service-status", "OPEN",
            "--ledger", str(ledger_path),
            "--breaker-config", str(BREAKER_CONFIG),
            "--env-file", str(tmp_path / "absent.env"),
            "--dry-run-report", str(report_path),
        ],
        moment=MOMENT,
    )
    assert code == 0
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["proposal"] is None
    assert report["intent"] is None
    assert report["skip_reason"] == "目标为零，无需委托"
    assert report["endpoints"]["write_planned"] == []
    assert not ledger_path.exists()


def test_cli_zero_target_does_not_repair_existing_partial_ledger(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """零目标不装载账本，既有不完整尾行保持逐字节不变。"""
    forbid_network(monkeypatch)
    monkeypatch.delenv("GUVOLU_MODE", raising=False)
    monkeypatch.setenv("GUVOLU_LOG_DIR", str(tmp_path / "logs"))
    ledger_path = tmp_path / "intent_ledger.jsonl"
    ledger_path.write_bytes(b'{"bad":')
    report_path = tmp_path / "report.json"
    before = ledger_path.read_bytes()
    target = write_v2_target(tmp_path, target=0.0)
    code = main(
        [
            "--target", str(target),
            *source_prediction_arguments(target),
            "--target-config", str(ROOT / "config" / "paper_executor.json"),
            "--rules", str(write_rules(tmp_path)),
            "--reference-price", "1000000",
            "--service-status", "OPEN",
            "--ledger", str(ledger_path),
            "--breaker-config", str(BREAKER_CONFIG),
            "--env-file", str(tmp_path / "absent.env"),
            "--dry-run-report", str(report_path),
        ],
        moment=MOMENT,
    )
    assert code == 0
    assert ledger_path.read_bytes() == before
    assert list(tmp_path.glob("intent_ledger.jsonl.partial-*")) == []


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
