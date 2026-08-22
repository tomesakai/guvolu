"""paper 执行器端到端单测：全程离线，零写端点（C-13、C-14、T-04）。"""
from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from guvolu.data.intent_ledger import IntentLedger
from guvolu.domain.config import Limits
from guvolu.domain.enums import ServiceStatus, Side
from guvolu.domain.intent import IntentState
from guvolu.domain.models import SymbolRule
from guvolu.domain.symbols import SpotSymbol
from guvolu.execution.conversion import MarketRule
from guvolu.execution.frozen_target_adapter import persist_operational_target
from guvolu.execution.paper_executor import (
    DIFFERENCE_LEDGER_NAME,
    INTENT_LEDGER_NAME,
    POSITION_LEDGER_NAME,
    PaperExecutorError,
    PaperPositionLedger,
    PaperRuntime,
    StaticBookSource,
    evaluate_overlay,
    load_execution_target,
    main,
    read_difference_rows,
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
    assert row["delta"]["proposal"]["side"] == "BUY"
    assert row["fill"]["model_fill_price"] == "1000700"
    assert Decimal(row["cost"]["total_cost_bps"]) == Decimal("12")
    assert row["overlay"]["applied"] is False
    assert row["overlay"]["would_apply"] is True
    assert row["overlay"]["limit"] == "0.3"
    assert row["endpoints"]["write_touched"] == []
    assert row["endpoints"]["write_planned"] == []


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

    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({**payload, "exposure_target": 1.2}), encoding="utf-8")
    with pytest.raises(PaperExecutorError, match="exposure_target"):
        load_execution_target(bad)

    negative = tmp_path / "negative.json"
    negative.write_text(
        json.dumps({**payload, "exposure_target": -0.1}), encoding="utf-8"
    )
    with pytest.raises(PaperExecutorError, match="exposure_target"):
        load_execution_target(negative)

    legacy = tmp_path / "legacy.json"
    legacy.write_text(json.dumps({**payload, "schema_version": 1}), encoding="utf-8")
    with pytest.raises(PaperExecutorError, match="schema_version"):
        load_execution_target(legacy)

    semantics = tmp_path / "semantics.json"
    semantics.write_text(json.dumps({
        **payload,
        "target_semantics": {**payload["target_semantics"], "short_allowed": True},
    }), encoding="utf-8")
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
    assert row["fee"]["source"] == FEE_SOURCE_FALLBACK
    assert "offline" in row["fee"]["detail"]


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
    names = [gate["name"] for gate in complete["gates"]]
    assert names == [
        "quality_eligible", "service_status", "rest_anchor_age_seconds",
        "best_spread_bps", "top5_depth_base",
    ]
    assert partial["would_apply"] is False
    assert partial["complete"] is False
    anchor = partial["gates"][2]
    assert anchor["status"] == "unavailable" and anchor["passed"] is None
    assert no_book["would_apply"] is False
    assert no_book["multiplier"] is None
    assert no_book["gates"][0]["passed"] is False
    assert no_book["gates"][1]["passed"] is False
    assert no_book["gates"][3]["status"] == "unavailable"


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
    day = summary["days"]["2026-08-22"]
    assert summary["rows"] == 1
    assert day["paper_filled"] == 1
    assert Decimal(day["buy_notional_jpy"]) == Decimal("500.35")
    assert Decimal(day["mean_total_cost_bps"]) == Decimal("12")
    assert day["overlay_would_apply"] == 1
    assert summarize_main([
        "--config", str(config_path), "--ledger-root", str(root),
    ]) == 0
