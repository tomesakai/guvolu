"""paper 执行器端到端单测：全程离线，零写端点（C-13、C-14、T-04）。"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from guvolu.data.intent_ledger import IntentLedger
from guvolu.domain.config import Limits
from guvolu.domain.enums import ExecutionType, ServiceStatus, Side
from guvolu.domain.errors import ConfigError
from guvolu.domain.intent import IntentState, OrderIntent
from guvolu.domain.models import SymbolRule
from guvolu.domain.symbols import SpotSymbol
from guvolu.execution.conversion import MarketRule
from guvolu.execution.frozen_target_adapter import persist_operational_target
from guvolu.execution.paper_executor import (
    CLAIM_LEDGER_NAME,
    DIFFERENCE_LEDGER_NAME,
    INTENT_LEDGER_NAME,
    NEEDS_RECONCILIATION,
    POSITION_LEDGER_NAME,
    DifferenceLedger,
    PaperExecutorError,
    PaperPositionLedger,
    PaperRuntime,
    PredictionClaims,
    StaticBookSource,
    evaluate_overlay,
    load_execution_target,
    main,
    read_difference_rows,
    replay_limit_usage,
    run_paper_decision,
)
from guvolu.execution.paper_fill_model import (
    FEE_SOURCE_FALLBACK,
    FEE_SOURCE_SYMBOLS,
    PUBLIC_ORDERBOOK_BASIS,
    BookLevel,
    BookSnapshot,
    TakerFeeResolver,
)
from guvolu.execution.paper_ledger_summary import main as summarize_main
from guvolu.execution.paper_ledger_summary import summarize_ledger
from guvolu.execution.paper_config import load_paper_config
from guvolu.risk.circuit_breaker import BreakerThresholds, CircuitBreaker
from guvolu.risk.errors import LimitExceeded
from guvolu.risk.limits import LimitGate

MARKET = "mkt__gmo__btc__r0"
BTC = SpotSymbol("BTC")
DECISION = datetime(2026, 8, 22, 1, 0, tzinfo=UTC)
MOMENT = DECISION + timedelta(minutes=5)
THRESHOLDS = BreakerThresholds(
    schema_version=1,
    consecutive_failure_limit=3,
    stream_gap_seconds=90,
    asset_deviation_ratio=Decimal("0.01"),
    asset_deviation_floor_jpy=Decimal("30"),
)
RULE = MarketRule(
    symbol=BTC,
    tick_size=Decimal("1"),
    size_step=Decimal("0.0001"),
    min_order_size=Decimal("0.0001"),
    max_order_size=Decimal("5"),
)
RULE_ROWS = [
    {
        "symbol": "BTC",
        "minOrderSize": "0.0001",
        "maxOrderSize": "5",
        "sizeStep": "0.0001",
        "tickSize": "1",
        "takerFee": "0.0005",
        "makerFee": "-0.0001",
    },
]
BOOK_PAYLOAD = {
    "symbol": "BTC",
    "observed_at": MOMENT.isoformat(),
    "asks": [
        {"price": "1000500", "size": "0.0003"},
        {"price": "1001000", "size": "0.01"},
    ],
    "bids": [
        {"price": "999500", "size": "0.0003"},
        {"price": "999000", "size": "0.01"},
    ],
}


def as_mapping(value: object) -> Mapping[str, Any]:
    assert isinstance(value, dict)
    return value


def write_config(tmp_path: Path, **overrides: object) -> Path:
    payload: dict[str, Any] = {
        "schema_version": 1,
        "market_id": MARKET,
        "symbol": "BTC",
        "bar_interval": "1hour",
        "risk_budget_jpy": "500",
        "no_trade_band": "0.01",
        "taker_fee_fallback_bps": "5",
        "taker_fee_cache_seconds": 86400,
        "overlay": {
            "limit": "0.3",
            "maximum_spread_bps": "10",
            "minimum_top5_depth_base": "0.01",
            "maximum_anchor_age_seconds": 300,
        },
        "ledger_directory": "execution/paper",
    }
    payload.update(overrides)
    path = tmp_path / "paper_executor.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-Infinity"])
def test_paper_config_rejects_non_finite_decimals(
    tmp_path: Path, value: str,
) -> None:
    with pytest.raises(ConfigError, match="有限数值"):
        load_paper_config(write_config(tmp_path, risk_budget_jpy=value))


def write_prediction(
    tmp_path: Path, *, target: float, prediction_id: str, name: str = "p.json",
) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps({
        "aggregate_target": target,
        "decision_time": DECISION.isoformat(),
        "families": [{"family": "trend", "portfolio_target_contribution": target}],
        "input_head_generation": "sha256-head",
        "plan_id": "frozen-forward-plan-one",
        "prediction_id": prediction_id,
        "quality": {
            "clock": True, "coverage": True, "eligible": True,
            "freshness": True, "integrity": True, "lineage": True,
            "pit": True, "reasons": [],
        },
        "reserve": 0.6,
        "schema_version": 1,
        "scope": "FROZEN_FORWARD",
        "unit": "risk_weighted_directional_target",
    }), encoding="utf-8")
    return path


def write_target(
    tmp_path: Path,
    *,
    target: float,
    prediction_id: str = "prediction-one",
    mode: str = "paper",
    market_id: str = MARKET,
    symbol: SpotSymbol = BTC,
    budget: Decimal = Decimal("500"),
) -> Path:
    prediction = write_prediction(
        tmp_path, target=target, prediction_id=prediction_id,
        name=f"{prediction_id}.json",
    )
    path, _ = persist_operational_target(
        prediction, tmp_path / "targets", market_id=market_id, symbol=symbol,
        risk_budget_jpy=budget, mode=mode,
    )
    return path


def source_prediction_arguments(target: Path) -> list[str]:
    payload = json.loads(target.read_text(encoding="utf-8"))
    source = Path(payload["lineage"]["source_prediction_path"])
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    return [
        "--source-prediction", str(source),
        "--source-prediction-sha256", digest,
    ]


def rewrite_content_addressed_target(
    original: Path, payload: Mapping[str, object],
) -> Path:
    raw = (
        json.dumps(
            dict(payload), ensure_ascii=False, separators=(",", ":"), sort_keys=True,
        ) + "\n"
    ).encode("utf-8")
    digest = hashlib.sha256(raw).hexdigest()
    path = original.parent / f"target-{digest}.json"
    path.write_bytes(raw)
    return path


def book_snapshot() -> BookSnapshot:
    return BookSnapshot(
        symbol=BTC,
        bids=(
            BookLevel(Decimal("999500"), Decimal("0.0003")),
            BookLevel(Decimal("999000"), Decimal("0.01")),
        ),
        asks=(
            BookLevel(Decimal("1000500"), Decimal("0.0003")),
            BookLevel(Decimal("1001000"), Decimal("0.01")),
        ),
        observed_at=MOMENT,
        basis=PUBLIC_ORDERBOOK_BASIS,
    )


def rules() -> tuple[SymbolRule, ...]:
    return tuple(SymbolRule.from_api(row) for row in RULE_ROWS)


def failing_rules() -> tuple[SymbolRule, ...]:
    raise RuntimeError("offline")


def build_runtime(
    tmp_path: Path,
    *,
    anchor_age: int | None = 10,
    fee_fetch: Any = rules,
    status: ServiceStatus = ServiceStatus.OPEN,
) -> PaperRuntime:
    config = load_paper_config(write_config(tmp_path))
    directory = tmp_path / "root" / config.ledger_directory
    return PaperRuntime(
        config=config,
        rule=RULE,
        book_source=StaticBookSource(book_snapshot()),
        fee_resolver=TakerFeeResolver(
            directory / "fee.json",
            fallback_bps=config.taker_fee_fallback_bps,
            cache_seconds=config.taker_fee_cache_seconds,
        ),
        fee_fetch=fee_fetch,
        service_status=status,
        ledger_directory=directory,
        limit_gate=LimitGate(Limits(
            order_jpy_max=Decimal("500"),
            day_jpy_max=Decimal("2000"),
            day_count_max=50,
        )),
        breaker=CircuitBreaker(THRESHOLDS),
        whitelist=frozenset({BTC}),
        anchor_age_seconds=anchor_age,
    )


def ledger_rows(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line
    ]


def test_full_exposure_buys_from_zero_and_records_everything(
    tmp_path: Path,
) -> None:
    runtime = build_runtime(tmp_path)
    target = load_execution_target(write_target(tmp_path, target=1.0))

    outcome = run_paper_decision(target, runtime, moment=MOMENT)

    assert outcome.status == IntentState.PAPER_FILLED.value
    assert outcome.write_touched == ()
    assert outcome.intent is not None and outcome.intent.side is Side.BUY
    assert outcome.intent.size == Decimal("0.0005")
    assert outcome.intent.prediction_id == "prediction-one"
    assert outcome.intent.decision_time == DECISION
    assert outcome.intent.correlation_id == target.correlation_id
    assert outcome.estimate is not None
    assert outcome.estimate.model_fill_price == Decimal("1000700")
    assert outcome.estimate.fee_source == FEE_SOURCE_SYMBOLS
    assert outcome.position_after == Decimal("0.0005")

    intent_rows = ledger_rows(runtime.ledger_directory / INTENT_LEDGER_NAME)
    created = intent_rows[0]
    assert created["schema_version"] == 2
    assert created["prediction_id"] == "prediction-one"
    assert created["decision_time"] == DECISION.isoformat()
    assert created["correlation_id"] == target.correlation_id
    assert [row["target"] for row in intent_rows[1:]] == [
        "SENDING", "PAPER_FILLED",
    ]
    assert intent_rows[-1]["evidence"]["fill_basis"] == PUBLIC_ORDERBOOK_BASIS

    positions = ledger_rows(runtime.ledger_directory / POSITION_LEDGER_NAME)
    assert positions[0]["position_after"] == "0.0005"
    assert PaperPositionLedger(
        runtime.ledger_directory / POSITION_LEDGER_NAME
    ).position_size(BTC) == Decimal("0.0005")

    diff_rows = read_difference_rows(
        runtime.ledger_directory / DIFFERENCE_LEDGER_NAME
    )
    assert len(diff_rows) == 1
    row = diff_rows[0]
    assert row["prediction_id"] == "prediction-one"
    assert row["correlation_id"] == target.correlation_id
    assert row["exposure_target"] == 1.0
    assert row["target_notional_jpy"] == "500.0000"
    assert as_mapping(as_mapping(row["delta"])["proposal"])["side"] == "BUY"
    assert as_mapping(row["fill"])["model_fill_price"] == "1000700"
    assert Decimal(as_mapping(row["cost"])["total_cost_bps"]) == Decimal("12")
    overlay = as_mapping(row["overlay"])
    assert overlay["applied"] is False
    assert overlay["would_apply"] is True
    assert overlay["limit"] == "0.3"
    endpoints = as_mapping(row["endpoints"])
    assert endpoints["write_touched"] == []
    assert endpoints["write_planned"] == []


def test_reduced_exposure_sells_down_never_below_position(
    tmp_path: Path,
) -> None:
    runtime = build_runtime(tmp_path)
    run_paper_decision(
        load_execution_target(write_target(tmp_path, target=1.0)),
        runtime, moment=MOMENT,
    )

    lower = load_execution_target(write_target(
        tmp_path, target=0.5, prediction_id="prediction-two",
    ))
    outcome = run_paper_decision(lower, runtime, moment=MOMENT)

    assert outcome.status == IntentState.PAPER_FILLED.value
    assert outcome.intent is not None and outcome.intent.side is Side.SELL
    assert outcome.intent.size == Decimal("0.0002")
    assert outcome.position_after == Decimal("0.0003")

    flat = load_execution_target(write_target(
        tmp_path, target=0.0, prediction_id="prediction-three",
    ))
    closed = run_paper_decision(flat, runtime, moment=MOMENT)
    assert closed.intent is not None and closed.intent.side is Side.SELL
    assert closed.intent.size == Decimal("0.0003")
    assert closed.position_after == Decimal("0")


def test_zero_exposure_with_zero_inventory_never_opens_sell(
    tmp_path: Path,
) -> None:
    runtime = build_runtime(tmp_path)
    target = load_execution_target(write_target(tmp_path, target=0.0))

    outcome = run_paper_decision(target, runtime, moment=MOMENT)

    assert outcome.status == "skipped"
    assert outcome.intent is None
    assert outcome.delta is not None and outcome.delta.skip_reason is not None
    assert not (runtime.ledger_directory / INTENT_LEDGER_NAME).exists()
    row = read_difference_rows(
        runtime.ledger_directory / DIFFERENCE_LEDGER_NAME
    )[0]
    assert row["intent"] is None
    assert row["position_after"] == "0"


def test_same_prediction_is_not_replayed(tmp_path: Path) -> None:
    runtime = build_runtime(tmp_path)
    target = load_execution_target(write_target(tmp_path, target=1.0))

    run_paper_decision(target, runtime, moment=MOMENT)
    again = run_paper_decision(target, runtime, moment=MOMENT)

    assert again.status == "duplicate_prediction"
    ledger = IntentLedger(runtime.ledger_directory / INTENT_LEDGER_NAME)
    assert len(ledger.intent_ids()) == 1
    assert len(read_difference_rows(
        runtime.ledger_directory / DIFFERENCE_LEDGER_NAME
    )) == 1


def test_expired_target_is_rejected_before_any_ledger_write(
    tmp_path: Path,
) -> None:
    runtime = build_runtime(tmp_path)
    target = load_execution_target(write_target(tmp_path, target=1.0))

    with pytest.raises(PaperExecutorError, match="越期"):
        run_paper_decision(
            target, runtime, moment=DECISION + timedelta(hours=1),
        )
    with pytest.raises(PaperExecutorError, match="尚未生效"):
        run_paper_decision(
            target, runtime, moment=DECISION - timedelta(seconds=1),
        )
    assert not (runtime.ledger_directory / DIFFERENCE_LEDGER_NAME).exists()


def test_market_symbol_mode_and_budget_mismatch_rejected(
    tmp_path: Path,
) -> None:
    runtime = build_runtime(tmp_path)

    other_market = load_execution_target(write_target(
        tmp_path, target=0.5, prediction_id="m", market_id="mkt__gmo__eth__r0",
    ))
    with pytest.raises(PaperExecutorError, match="market_id"):
        run_paper_decision(other_market, runtime, moment=MOMENT)

    other_symbol = load_execution_target(write_target(
        tmp_path, target=0.5, prediction_id="s", symbol=SpotSymbol("ETH"),
    ))
    with pytest.raises(PaperExecutorError, match="symbol"):
        run_paper_decision(other_symbol, runtime, moment=MOMENT)

    dry = load_execution_target(write_target(
        tmp_path, target=0.5, prediction_id="d", mode="dry-run",
    ))
    with pytest.raises(PaperExecutorError, match="mode"):
        run_paper_decision(dry, runtime, moment=MOMENT)

    rich = load_execution_target(write_target(
        tmp_path, target=0.5, prediction_id="r", budget=Decimal("800"),
    ))
    with pytest.raises(PaperExecutorError, match="risk_budget_jpy"):
        run_paper_decision(rich, runtime, moment=MOMENT)


def test_target_loader_rejects_out_of_range_exposure_and_v1(
    tmp_path: Path,
) -> None:
    path = write_target(tmp_path, target=0.5)
    payload = json.loads(path.read_text(encoding="utf-8"))

    bad = rewrite_content_addressed_target(
        path, {**payload, "exposure_target": 1.2},
    )
    with pytest.raises(PaperExecutorError, match="exposure_target"):
        load_execution_target(bad)

    negative = rewrite_content_addressed_target(
        path, {**payload, "exposure_target": -0.1},
    )
    with pytest.raises(PaperExecutorError, match="exposure_target"):
        load_execution_target(negative)

    legacy = rewrite_content_addressed_target(
        path, {**payload, "schema_version": 1},
    )
    with pytest.raises(PaperExecutorError, match="结构"):
        load_execution_target(legacy)

    semantics = rewrite_content_addressed_target(path, {
        **payload,
        "target_semantics": {**payload["target_semantics"], "short_allowed": True},
    })
    with pytest.raises(PaperExecutorError, match="target_semantics"):
        load_execution_target(semantics)


def test_fee_fetch_failure_degrades_to_config_and_is_labelled(
    tmp_path: Path,
) -> None:
    runtime = build_runtime(tmp_path, fee_fetch=failing_rules)
    target = load_execution_target(write_target(tmp_path, target=1.0))

    outcome = run_paper_decision(target, runtime, moment=MOMENT)

    assert outcome.status == IntentState.PAPER_FILLED.value
    assert outcome.fee is not None
    assert outcome.fee.source == FEE_SOURCE_FALLBACK
    assert outcome.fee.bps == Decimal("5")
    row = read_difference_rows(
        runtime.ledger_directory / DIFFERENCE_LEDGER_NAME
    )[0]
    fee = as_mapping(row["fee"])
    assert fee["source"] == FEE_SOURCE_FALLBACK
    assert "offline" in fee["detail"]


def test_overlay_records_gates_and_marks_unavailable_inputs(
    tmp_path: Path,
) -> None:
    config = load_paper_config(write_config(tmp_path))

    complete = evaluate_overlay(
        book_snapshot(),
        quality_eligible=True,
        service_status=ServiceStatus.OPEN,
        thresholds=config.overlay,
        anchor_age_seconds=30,
    )
    partial = evaluate_overlay(
        book_snapshot(),
        quality_eligible=True,
        service_status=ServiceStatus.OPEN,
        thresholds=config.overlay,
        anchor_age_seconds=None,
    )
    no_book = evaluate_overlay(
        None,
        quality_eligible=False,
        service_status=ServiceStatus.MAINTENANCE,
        thresholds=config.overlay,
        anchor_age_seconds=30,
    )

    assert complete["applied"] is False
    assert complete["would_apply"] is True
    assert Decimal(str(complete["multiplier"])) == Decimal("0")
    assert Decimal(str(complete["top_imbalance"])) == Decimal("0")
    complete_gates_raw = complete["gates"]
    assert isinstance(complete_gates_raw, list)
    complete_gates = [as_mapping(gate) for gate in complete_gates_raw]
    names = [gate["name"] for gate in complete_gates]
    assert names == [
        "quality_eligible", "service_status", "rest_anchor_age_seconds",
        "best_spread_bps", "top5_depth_base",
    ]
    assert partial["would_apply"] is False
    assert partial["complete"] is False
    partial_gates_raw = partial["gates"]
    assert isinstance(partial_gates_raw, list)
    partial_gates = [as_mapping(gate) for gate in partial_gates_raw]
    anchor = partial_gates[2]
    assert anchor["status"] == "unavailable" and anchor["passed"] is None
    assert no_book["would_apply"] is False
    assert no_book["multiplier"] is None
    no_book_gates_raw = no_book["gates"]
    assert isinstance(no_book_gates_raw, list)
    no_book_gates = [as_mapping(gate) for gate in no_book_gates_raw]
    assert no_book_gates[0]["passed"] is False
    assert no_book_gates[1]["passed"] is False
    assert no_book_gates[3]["status"] == "unavailable"


def test_maintenance_status_gate_rejects_without_touching_endpoints(
    tmp_path: Path,
) -> None:
    runtime = build_runtime(tmp_path, status=ServiceStatus.MAINTENANCE)
    target = load_execution_target(write_target(tmp_path, target=1.0))

    outcome = run_paper_decision(target, runtime, moment=MOMENT)

    assert outcome.status == IntentState.GATE_REJECTED.value
    assert outcome.write_touched == ()
    assert outcome.position_after == Decimal("0")


def test_position_ledger_rejects_discontinuous_history(tmp_path: Path) -> None:
    path = tmp_path / "positions.jsonl"
    path.write_text(json.dumps({
        "symbol": "BTC", "side": "SELL", "fill_size": "0.1",
        "position_after": "-0.1",
    }) + "\n", encoding="utf-8")

    with pytest.raises(PaperExecutorError, match="不连续"):
        PaperPositionLedger(path)


def test_cli_end_to_end_offline_and_summary(tmp_path: Path) -> None:
    config_path = write_config(tmp_path)
    rules_path = tmp_path / "rules.json"
    rules_path.write_text(json.dumps(RULE_ROWS), encoding="utf-8")
    book_path = tmp_path / "book.json"
    book_path.write_text(json.dumps({"data": BOOK_PAYLOAD}), encoding="utf-8")
    target = write_target(tmp_path, target=1.0)
    report_path = tmp_path / "report.json"
    root = tmp_path / "root"

    code = main([
        "--target", str(target),
        *source_prediction_arguments(target),
        "--config", str(config_path),
        "--rules", str(rules_path),
        "--book", str(book_path),
        "--service-status", "OPEN",
        "--anchor-age-seconds", "20",
        "--ledger-root", str(root),
        "--env-file", str(tmp_path / "absent.env"),
        "--now", MOMENT.isoformat(),
        "--report", str(report_path),
    ])
    report = json.loads(report_path.read_text(encoding="utf-8"))

    assert code == 0
    assert report["status"] == "PAPER_FILLED"
    assert report["endpoints"]["write_touched"] == []
    assert report["endpoints"]["read_touched"] == []
    assert report["fill"]["fill_basis"] == PUBLIC_ORDERBOOK_BASIS
    assert report["fee"]["source"] == FEE_SOURCE_SYMBOLS
    assert Path(report["ledger_paths"]["intent_ledger"]).exists()

    again = main([
        "--target", str(target),
        *source_prediction_arguments(target),
        "--config", str(config_path),
        "--rules", str(rules_path),
        "--book", str(book_path),
        "--service-status", "OPEN",
        "--ledger-root", str(root),
        "--env-file", str(tmp_path / "absent.env"),
        "--now", MOMENT.isoformat(),
        "--report", str(tmp_path / "again.json"),
    ])
    assert again == 0
    again_report = json.loads((tmp_path / "again.json").read_text(encoding="utf-8"))
    assert again_report["status"] == "duplicate_prediction"

    summary = summarize_ledger(
        root / "execution" / "paper" / DIFFERENCE_LEDGER_NAME
    )
    day = as_mapping(as_mapping(summary["days"])["2026-08-22"])
    assert summary["rows"] == 1
    assert day["paper_filled"] == 1
    assert Decimal(day["buy_notional_jpy"]) == Decimal("500.35")
    assert Decimal(day["mean_total_cost_bps"]) == Decimal("12")
    assert day["overlay_would_apply"] == 1
    assert summarize_main([
        "--config", str(config_path), "--ledger-root", str(root),
    ]) == 0


def crash_append(self: DifferenceLedger, row: Mapping[str, object]) -> None:
    """模拟差异行写入前进程中断。"""
    raise RuntimeError("模拟进程中断")


def seeded_intent(
    intent_id: str, *, prediction_id: str, created_at: datetime = MOMENT,
) -> OrderIntent:
    return OrderIntent(
        intent_id=intent_id,
        correlation_id="co-seed",
        symbol=BTC,
        side=Side.BUY,
        execution_type=ExecutionType.LIMIT,
        size=Decimal("0.0001"),
        price=Decimal("1000000"),
        time_in_force=None,
        created_at=created_at,
        prediction_id=prediction_id,
        decision_time=DECISION,
    )


def cli_args(
    tmp_path: Path, target: Path, *, report: str, env_file: Path | None = None,
) -> list[str]:
    config_path = write_config(tmp_path)
    rules_path = tmp_path / "rules.json"
    rules_path.write_text(json.dumps(RULE_ROWS), encoding="utf-8")
    book_path = tmp_path / "book.json"
    book_path.write_text(json.dumps({"data": BOOK_PAYLOAD}), encoding="utf-8")
    env = env_file if env_file is not None else tmp_path / "absent.env"
    return [
        "--target", str(target),
        *source_prediction_arguments(target),
        "--config", str(config_path),
        "--rules", str(rules_path),
        "--book", str(book_path),
        "--service-status", "OPEN",
        "--ledger-root", str(tmp_path / "root"),
        "--env-file", str(env),
        "--now", MOMENT.isoformat(),
        "--report", str(tmp_path / report),
    ]


@pytest.mark.parametrize("attack", ["source_flip", "coherent_run_tamper"])
def test_cli_rebuilds_target_identity_before_any_paper_side_effect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    attack: str,
) -> None:
    monkeypatch.delenv("GUVOLU_MODE", raising=False)
    target = write_target(tmp_path, target=0.5)
    payload = json.loads(target.read_text(encoding="utf-8"))
    if attack == "source_flip":
        payload["correlation_id_source"] = "prediction"
        payload["correlation_id"] = "coffffffffffffffff"
        message = "correlation 血缘"
    else:
        forged_id = "prediction-forged"
        payload["run_id"] = forged_id
        payload["lineage"]["prediction_id"] = forged_id
        digest = hashlib.sha256(
            f"guvolu-prediction:{forged_id}".encode("utf-8")
        ).hexdigest()
        payload["correlation_id"] = f"co{digest[:16]}"
        message = "身份/时点"
    tampered = rewrite_content_addressed_target(target, payload)

    with pytest.raises(PaperExecutorError, match=message):
        main(cli_args(tmp_path, tampered, report="tampered.json"))
    assert not (tmp_path / "root").exists()
    assert not (tmp_path / "tampered.json").exists()


def test_cli_rejects_unknown_target_semantics_before_paper_side_effect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GUVOLU_MODE", raising=False)
    target = write_target(tmp_path, target=0.5)
    payload = json.loads(target.read_text(encoding="utf-8"))
    payload["target_semantics"]["future_short_override"] = True
    tampered = rewrite_content_addressed_target(target, payload)

    with pytest.raises(PaperExecutorError, match="target_semantics"):
        main(cli_args(tmp_path, tampered, report="unknown-semantics.json"))
    assert not (tmp_path / "root").exists()
    assert not (tmp_path / "unknown-semantics.json").exists()


def test_cli_rejects_live_process_environment_before_paper_side_effect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GUVOLU_MODE", "live")
    target = write_target(tmp_path, target=0.5)

    with pytest.raises(PaperExecutorError, match="拒绝非 dry-run"):
        main(cli_args(tmp_path, target, report="live.json"))
    assert not (tmp_path / "root").exists()
    assert not (tmp_path / "live.json").exists()


def test_interrupt_after_settlement_before_difference_row_is_not_replayed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """意图账与持仓账已落盘、差异行未写即中断，重跑不生成第二笔意图。"""
    runtime = build_runtime(tmp_path)
    target = load_execution_target(write_target(tmp_path, target=1.0))
    original = DifferenceLedger.append
    monkeypatch.setattr(DifferenceLedger, "append", crash_append)
    with pytest.raises(RuntimeError, match="中断"):
        run_paper_decision(target, runtime, moment=MOMENT)
    monkeypatch.setattr(DifferenceLedger, "append", original)

    intent_path = runtime.ledger_directory / INTENT_LEDGER_NAME
    first_ids = IntentLedger(intent_path).intent_ids()
    assert len(first_ids) == 1
    assert not (runtime.ledger_directory / DIFFERENCE_LEDGER_NAME).exists()
    claims = PredictionClaims(runtime.ledger_directory / CLAIM_LEDGER_NAME)
    assert claims.has_claim("prediction-one")

    again = run_paper_decision(target, runtime, moment=MOMENT)

    assert again.status == NEEDS_RECONCILIATION
    assert again.intent is None and again.dispatch is None
    assert again.reconciliation is not None
    assert again.reconciliation["intents"] == [
        {"intent_id": first_ids[0], "state": "PAPER_FILLED"},
    ]
    assert again.reconciliation["claim"] is not None
    assert IntentLedger(intent_path).intent_ids() == first_ids
    assert len(ledger_rows(claims.path)) == 1
    assert not (runtime.ledger_directory / DIFFERENCE_LEDGER_NAME).exists()
    assert again.position_before == Decimal("0.0005")
    assert again.position_after == Decimal("0.0005")
    with pytest.raises(PaperExecutorError, match="已认领"):
        claims.claim(
            prediction_id="prediction-one", correlation_id="x", at=MOMENT,
        )


def test_interrupt_after_claim_before_intent_reports_reconciliation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """认领已落盘、意图未落盘即中断，重跑同样拒绝自动生成。"""
    runtime = build_runtime(tmp_path)
    target = load_execution_target(write_target(tmp_path, target=1.0))

    def crash_dispatch(*args: object, **kwargs: object) -> None:
        raise RuntimeError("模拟进程中断")

    monkeypatch.setattr(
        "guvolu.execution.paper_executor.dispatch_order_intent", crash_dispatch,
    )
    with pytest.raises(RuntimeError, match="中断"):
        run_paper_decision(target, runtime, moment=MOMENT)
    monkeypatch.undo()

    again = run_paper_decision(target, runtime, moment=MOMENT)

    assert again.status == NEEDS_RECONCILIATION
    assert again.reconciliation is not None
    assert again.reconciliation["intents"] == []
    assert as_mapping(again.reconciliation["claim"])["prediction_id"] == (
        "prediction-one"
    )
    assert not (runtime.ledger_directory / INTENT_LEDGER_NAME).exists()


def test_cli_needs_reconciliation_returns_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = write_target(tmp_path, target=1.0)
    original = DifferenceLedger.append
    monkeypatch.setattr(DifferenceLedger, "append", crash_append)
    with pytest.raises(RuntimeError, match="中断"):
        main(cli_args(tmp_path, target, report="crash.json"))
    monkeypatch.setattr(DifferenceLedger, "append", original)

    code = main(cli_args(tmp_path, target, report="again.json"))
    report = json.loads((tmp_path / "again.json").read_text(encoding="utf-8"))

    assert code == 1
    assert report["status"] == NEEDS_RECONCILIATION
    assert report["reconciliation"]["intents"][0]["state"] == "PAPER_FILLED"
    assert report["endpoints"]["write_touched"] == []
    assert Path(report["ledger_paths"]["claim_ledger"]).exists()


def test_cli_startup_recovers_interrupted_sending_and_reports(
    tmp_path: Path,
) -> None:
    """上次遗留 SENDING 在启动时结清为 PAPER_REJECTED 并列入报告。"""
    ledger_path = tmp_path / "root" / "execution" / "paper" / INTENT_LEDGER_NAME
    ledger_path.parent.mkdir(parents=True)
    seeded = IntentLedger(ledger_path)
    seeded.record_intent(
        seeded_intent("it-stale", prediction_id="prediction-zero"), at=MOMENT,
    )
    seeded.begin_send("it-stale", at=MOMENT)
    target = write_target(tmp_path, target=1.0)

    code = main(cli_args(tmp_path, target, report="report.json"))
    report = json.loads((tmp_path / "report.json").read_text(encoding="utf-8"))

    assert code == 0
    assert report["status"] == "PAPER_FILLED"
    assert report["startup"]["recovered_sends"]["intent_ids"] == ["it-stale"]
    assert report["startup"]["recovered_sends"]["state"] == "PAPER_REJECTED"
    assert report["startup"]["limit_usage"]["order_count"] == 1
    assert report["startup"]["limit_usage"]["replayed_intents"] == ["it-stale"]
    reopened = IntentLedger(ledger_path)
    assert reopened.state("it-stale") is IntentState.PAPER_REJECTED
    assert reopened.in_flight(BTC) == ()
    assert len(reopened.intent_ids()) == 2

    stale_target = write_target(
        tmp_path, target=1.0, prediction_id="prediction-zero",
    )
    again = main(cli_args(tmp_path, stale_target, report="stale.json"))
    stale_report = json.loads((tmp_path / "stale.json").read_text(encoding="utf-8"))
    assert again == 1
    assert stale_report["status"] == NEEDS_RECONCILIATION
    assert stale_report["reconciliation"]["claim"] is None
    assert stale_report["reconciliation"]["intents"] == [
        {"intent_id": "it-stale", "state": "PAPER_REJECTED"},
    ]


def test_replay_limit_usage_counts_only_gated_intents_of_the_day(
    tmp_path: Path,
) -> None:
    ledger = IntentLedger(tmp_path / INTENT_LEDGER_NAME)
    ledger.record_intent(seeded_intent("a", prediction_id="pa"), at=MOMENT)
    ledger.begin_send("a", at=MOMENT)
    ledger.paper_fill("a", reason="结算", evidence={"fill_basis": "x"}, at=MOMENT)
    ledger.record_intent(seeded_intent("b", prediction_id="pb"), at=MOMENT)
    ledger.begin_send("b", at=MOMENT)
    ledger.paper_reject("b", reason="深度不足", at=MOMENT)
    ledger.record_intent(seeded_intent("c", prediction_id="pc"), at=MOMENT)
    ledger.gate_reject("c", reason="维护", at=MOMENT)
    earlier = MOMENT - timedelta(days=2)
    ledger.record_intent(
        seeded_intent("d", prediction_id="pd", created_at=earlier), at=earlier,
    )
    ledger.begin_send("d", at=earlier)
    ledger.paper_fill("d", reason="结算", evidence={"fill_basis": "x"}, at=earlier)
    gate = LimitGate(Limits(
        order_jpy_max=Decimal("500"),
        day_jpy_max=Decimal("300"),
        day_count_max=3,
    ))

    usage = replay_limit_usage(gate, ledger, moment=MOMENT)

    assert usage["replayed_intents"] == ["a", "b"]
    assert usage["order_count"] == 2
    assert Decimal(str(usage["total_jpy"])) == Decimal("200")
    assert gate.usage().order_count == 2
    with pytest.raises(LimitExceeded, match="当日累计"):
        gate.commit(Decimal("101"), MOMENT)
    gate.commit(Decimal("50"), MOMENT)
    with pytest.raises(LimitExceeded, match="当日笔数"):
        gate.commit(Decimal("1"), MOMENT)


@pytest.mark.parametrize(
    ("env_line", "match"),
    [
        ("GUVOLU_DAY_COUNT_MAX=1", "当日笔数"),
        ("GUVOLU_DAY_JPY_MAX=600", "当日累计"),
    ],
)
def test_cli_day_limits_accumulate_across_invocations(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, env_line: str, match: str,
) -> None:
    """逐小时单发命令行，当日限额用量跨进程累计（T-11）。"""
    for name in ("GUVOLU_DAY_COUNT_MAX", "GUVOLU_DAY_JPY_MAX"):
        monkeypatch.delenv(name, raising=False)
    env_file = tmp_path / "limits.env"
    env_file.write_text(env_line + "\n", encoding="utf-8")

    first = main(cli_args(
        tmp_path, write_target(tmp_path, target=1.0),
        report="first.json", env_file=env_file,
    ))
    first_report = json.loads((tmp_path / "first.json").read_text(encoding="utf-8"))
    assert first == 0
    assert first_report["status"] == "PAPER_FILLED"
    assert first_report["startup"]["limit_usage"]["order_count"] == 0

    second = main(cli_args(
        tmp_path,
        write_target(tmp_path, target=0.5, prediction_id="prediction-two"),
        report="second.json", env_file=env_file,
    ))
    second_report = json.loads(
        (tmp_path / "second.json").read_text(encoding="utf-8")
    )

    assert second == 1
    assert second_report["status"] == "GATE_REJECTED"
    assert match in second_report["intent"]["reason"]
    usage = second_report["startup"]["limit_usage"]
    assert usage["order_count"] == 1
    assert Decimal(usage["total_jpy"]) == Decimal("500")
    assert usage["replayed_intents"] == [first_report["intent"]["intent_id"]]
    assert second_report["endpoints"]["write_touched"] == []


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), "0.5", True])
def test_target_loader_rejects_non_finite_and_non_numeric_exposure(
    tmp_path: Path, bad: object,
) -> None:
    path = write_target(tmp_path, target=0.5)
    payload = json.loads(path.read_text(encoding="utf-8"))
    broken = rewrite_content_addressed_target(
        path, {**payload, "exposure_target": bad},
    )

    with pytest.raises(PaperExecutorError, match="exposure_target"):
        load_execution_target(broken)
