"""授权信封装载、用量重放与门禁判定的离线单测（C-13、C-14）。"""
from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from guvolu.domain.enums import Side
from guvolu.domain.symbols import SpotSymbol
from guvolu.execution.authorization_envelope import (
    VERDICT_ALLOW,
    VERDICT_HALT,
    VERDICT_PAUSE,
    VERDICT_REJECT,
    VERDICT_SKIP,
    VERDICT_TRIP,
    AuthorizationEnvelope,
    EnvelopeError,
    EnvelopeState,
    EnvelopeStateStore,
    EnvelopeUsage,
    GateInputs,
    OnTrip,
    PriceObservation,
    ValuationBaseline,
    apply_price_move_gate,
    check_day_budget,
    check_envelope_total,
    check_first_order_canary,
    check_order_max,
    check_position_cap,
    check_prediction_age,
    check_spread,
    check_stream_freshness,
    evaluate_envelope_gates,
    load_envelope,
    observe_price,
)

REPO = Path(__file__).resolve().parents[1]
BTC = SpotSymbol("BTC")
WHITELIST = frozenset({BTC})
NOW = datetime(2026, 9, 2, 12, 0, tzinfo=UTC)


def _body() -> dict[str, object]:
    """一份可通过校验的信封载荷。"""
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


def _write(tmp_path: Path, body: dict[str, object]) -> Path:
    path = tmp_path / "envelope.json"
    path.write_text(
        json.dumps(body, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return path


def _load(tmp_path: Path, body: dict[str, object]) -> AuthorizationEnvelope:
    return load_envelope(_write(tmp_path, body), whitelist=WHITELIST)


def test_load_issued_envelope_and_identity() -> None:
    """已签发首封可装载，身份为文件字节 SHA-256。"""
    path = REPO / "config" / "authorization_envelope.json"
    envelope = load_envelope(path, whitelist=WHITELIST)
    assert envelope.sha256 == hashlib.sha256(path.read_bytes()).hexdigest()
    assert envelope.sha12 == envelope.sha256[:12]
    assert envelope.symbols == frozenset({BTC})
    assert envelope.order_jpy_max == Decimal("10000")
    assert envelope.canary_first_order_jpy_max == Decimal("500")
    assert envelope.on_trip is OnTrip.CANCEL_AND_FLATTEN
    assert envelope.market_risk.stream_gap_seconds == 90
    assert envelope.ops_breaker.consecutive_failure_limit == 3
    thresholds = envelope.breaker_thresholds()
    assert thresholds.stream_gap_seconds == 90
    assert thresholds.asset_deviation_floor_jpy == Decimal("100")


@pytest.mark.parametrize(
    "mutate,message",
    [
        (lambda b: b.pop("on_trip"), "字段集合"),
        (lambda b: b.update(extra=1), "字段集合"),
        (lambda b: b.update(schema_version=2), "schema_version"),
        (lambda b: b.update(order_jpy_max=10000), "字符串数值"),
        (lambda b: b.update(order_jpy_max="20000"), "硬顶"),
        (lambda b: b.update(day_jpy_max="10001"), "硬顶"),
        (lambda b: b.update(day_count_max=51), "硬顶"),
        (lambda b: b.update(day_count_max=0), "正整数"),
        (lambda b: b.update(envelope_jpy_total="-1"), "必须为正"),
        (lambda b: b.update(symbols=[]), "非空列表"),
        (lambda b: b.update(symbols=["ETH"]), "白名单"),
        (lambda b: b.update(symbols=["BTC_JPY"]), "现物"),
        (lambda b: b.update(symbols=["BTC", "BTC"]), "重复"),
        (lambda b: b.update(valid_until="2026-08-01T00:00:00Z"), "有效期"),
        (lambda b: b.update(valid_from="2026-09-01T00:00:00"), "时区"),
        (lambda b: b.update(on_trip="halt"), "on_trip"),
    ],
)
def test_load_rejects_invalid_fields(
    tmp_path: Path,
    mutate: object,
    message: str,
) -> None:
    """全字段校验：任一失败抛配置错误。"""
    body = _body()
    mutate(body)  # type: ignore[operator]
    with pytest.raises(EnvelopeError, match=message):
        _load(tmp_path, body)


def test_load_rejects_bad_nested_sets(tmp_path: Path) -> None:
    """嵌套结构字段集合同样受校验。"""
    body = _body()
    market = body["market_risk"]
    assert isinstance(market, dict)
    market.pop("spread_skip_bp")
    with pytest.raises(EnvelopeError, match="market_risk"):
        _load(tmp_path, body)


def test_usage_replay_across_restart(tmp_path: Path) -> None:
    """用量账重放跨重启：总额与当日口径不重置。"""
    envelope = _load(tmp_path, _body())
    usage = EnvelopeUsage.for_envelope(envelope, directory=tmp_path)
    # 越过 JST 06:00 即属次日
    first_at = datetime(2026, 9, 2, 11, 59, tzinfo=UTC)
    second_at = datetime(2026, 9, 2, 21, 30, tzinfo=UTC)
    usage.append(
        intent_id="in0000000000000001",
        notional_jpy=Decimal("450"), at=first_at,
    )
    usage.append(
        intent_id="in0000000000000002",
        notional_jpy=Decimal("300"), at=second_at,
    )
    reopened = EnvelopeUsage.for_envelope(envelope, directory=tmp_path)
    assert reopened.path.name == f"usage-{envelope.sha12}.jsonl"
    assert reopened.total_jpy() == Decimal("750")
    from guvolu.risk.limits import trading_day

    assert reopened.day_jpy(trading_day(first_at)) == Decimal("450")
    assert reopened.day_jpy(trading_day(second_at)) == Decimal("300")
    assert reopened.day_count(trading_day(first_at)) == 1


def test_usage_rejects_bad_rows(tmp_path: Path) -> None:
    """损坏的用量行拒绝装载，不静默丢弃。"""
    envelope = _load(tmp_path, _body())
    path = tmp_path / f"usage-{envelope.sha12}.jsonl"
    path.write_text('{"record":"usage"}\n', encoding="utf-8")
    with pytest.raises(EnvelopeError, match="字段非法"):
        EnvelopeUsage(path)


def test_state_store_roundtrip(tmp_path: Path) -> None:
    """状态原子覆写并完整重载（重启不重置）。"""
    envelope = _load(tmp_path, _body())
    store = EnvelopeStateStore.for_envelope(envelope, directory=tmp_path)
    assert store.load() == EnvelopeState()
    baseline = ValuationBaseline(
        at=NOW,
        jpy_amount=Decimal("3009"),
        btc_amount=Decimal("0.0001"),
        reference_price=Decimal("12000000"),
    )
    state = EnvelopeState(
        first_order_cleared=True,
        loss_baseline=baseline,
        day_baseline=baseline,
        day_baseline_day=NOW.date(),
        price_history=(PriceObservation(at=NOW, price=Decimal("12000000")),),
        paused_until=NOW + timedelta(hours=1),
        tripped_at=None,
        trip_reason=None,
    )
    store.save(state)
    assert store.load() == state
    assert baseline.value_jpy() == Decimal("3009") + Decimal("1200")


def _inputs(**overrides: object) -> GateInputs:
    base: dict[str, object] = {
        "now": NOW,
        "price_observed_at": NOW,
        "used_total_jpy": Decimal("0"),
        "day_used_jpy": Decimal("0"),
        "day_order_count": 0,
        "current_value_jpy": Decimal("100000"),
        "position_notional_jpy": Decimal("0"),
        "order_side": Side.BUY,
        "order_notional_jpy": Decimal("400"),
        "spread_bp": Decimal("10"),
        "opposite_depth_jpy": Decimal("100000"),
        "decision_time": NOW - timedelta(minutes=5),
    }
    base.update(overrides)
    return GateInputs(**base)  # type: ignore[arg-type]


def _cleared_state() -> EnvelopeState:
    baseline = ValuationBaseline(
        at=NOW, jpy_amount=Decimal("100000"),
        btc_amount=Decimal("0"), reference_price=Decimal("12000000"),
    )
    return EnvelopeState(
        first_order_cleared=True,
        loss_baseline=baseline,
        day_baseline=baseline,
        day_baseline_day=NOW.date(),
    )


def test_gate_boundaries(tmp_path: Path) -> None:
    """各门禁边界值：恰在边界通过，越界按语义处置。"""
    envelope = _load(tmp_path, _body())
    state = _cleared_state()
    assert check_order_max(
        envelope, order_notional_jpy=Decimal("10000")
    ).passed
    rejected = check_order_max(
        envelope, order_notional_jpy=Decimal("10000.01")
    )
    assert not rejected.passed and rejected.verdict == VERDICT_REJECT
    assert check_day_budget(
        envelope, day_used_jpy=Decimal("9600"), day_order_count=47,
        order_notional_jpy=Decimal("400"),
    ).passed
    tripped = check_day_budget(
        envelope, day_used_jpy=Decimal("9601"), day_order_count=0,
        order_notional_jpy=Decimal("400"),
    )
    assert not tripped.passed and tripped.verdict == VERDICT_TRIP
    counted = check_day_budget(
        envelope, day_used_jpy=Decimal("0"), day_order_count=48,
        order_notional_jpy=Decimal("400"),
    )
    assert not counted.passed and counted.verdict == VERDICT_TRIP
    assert check_envelope_total(
        envelope, used_total_jpy=Decimal("99600"),
        order_notional_jpy=Decimal("400"),
    ).passed
    exhausted = check_envelope_total(
        envelope, used_total_jpy=Decimal("100000"), order_notional_jpy=None,
    )
    assert not exhausted.passed and exhausted.verdict == VERDICT_HALT
    assert check_position_cap(
        envelope, order_side=Side.BUY,
        position_notional_jpy=Decimal("29600"),
        order_notional_jpy=Decimal("400"),
    ).passed
    capped = check_position_cap(
        envelope, order_side=Side.BUY,
        position_notional_jpy=Decimal("29601"),
        order_notional_jpy=Decimal("400"),
    )
    assert not capped.passed and capped.verdict == VERDICT_REJECT
    assert check_position_cap(
        envelope, order_side=Side.SELL,
        position_notional_jpy=Decimal("999999"),
        order_notional_jpy=Decimal("400"),
    ).passed
    assert check_spread(envelope, spread_bp=Decimal("50")).passed
    wide = check_spread(envelope, spread_bp=Decimal("50.1"))
    assert not wide.passed and wide.verdict == VERDICT_SKIP
    assert check_stream_freshness(
        envelope, price_observed_at=NOW - timedelta(seconds=90), now=NOW,
    ).passed
    stale = check_stream_freshness(
        envelope, price_observed_at=NOW - timedelta(seconds=91), now=NOW,
    )
    assert not stale.passed and stale.verdict == VERDICT_TRIP
    assert check_prediction_age(
        envelope, decision_time=NOW - timedelta(minutes=55), now=NOW,
    ).passed
    aged = check_prediction_age(
        envelope, decision_time=NOW - timedelta(minutes=56), now=NOW,
    )
    assert not aged.passed and aged.verdict == VERDICT_SKIP
    del state


def test_depth_gate_requires_ratio(tmp_path: Path) -> None:
    """深度门：对手侧一档不足名义倍数即跳过。"""
    from guvolu.execution.authorization_envelope import check_depth

    envelope = _load(tmp_path, _body())
    assert check_depth(
        envelope,
        opposite_depth_jpy=Decimal("1200"),
        order_notional_jpy=Decimal("400"),
    ).passed
    thin = check_depth(
        envelope,
        opposite_depth_jpy=Decimal("1199"),
        order_notional_jpy=Decimal("400"),
    )
    assert not thin.passed and thin.verdict == VERDICT_SKIP
    missing = check_depth(
        envelope, opposite_depth_jpy=None, order_notional_jpy=Decimal("400"),
    )
    assert not missing.passed and missing.verdict == VERDICT_SKIP


def test_first_order_canary_clamp_and_release(tmp_path: Path) -> None:
    """首单 canary：未解除压额到 canary 上限，解除后回到单笔限。"""
    envelope = _load(tmp_path, _body())
    fresh = EnvelopeState()
    assert check_first_order_canary(
        envelope, fresh, order_notional_jpy=Decimal("500")
    ).passed
    clamped = check_first_order_canary(
        envelope, fresh, order_notional_jpy=Decimal("501")
    )
    assert not clamped.passed and clamped.verdict == VERDICT_REJECT
    cleared = _cleared_state()
    assert check_first_order_canary(
        envelope, cleared, order_notional_jpy=Decimal("501")
    ).passed


def test_price_move_pause_set_and_release(tmp_path: Path) -> None:
    """急变门：窗口涨跌幅超阈设定暂停，届满自动解除。"""
    envelope = _load(tmp_path, _body())
    state = _cleared_state()
    state = observe_price(
        state, price=Decimal("10000000"), at=NOW - timedelta(seconds=60)
    )
    state = observe_price(state, price=Decimal("10510000"), at=NOW)
    paused, move = apply_price_move_gate(envelope, state, now=NOW)
    assert move is not None and move > Decimal("500")
    assert paused.paused_until == NOW + timedelta(seconds=3600)
    decision, new_state = evaluate_envelope_gates(
        envelope, state, _inputs()
    )
    assert decision.verdict == VERDICT_PAUSE
    assert new_state.paused_until == NOW + timedelta(seconds=3600)
    # 暂停期内继续拒发新单
    later = NOW + timedelta(seconds=1800)
    quiet = replace_history(new_state, price=Decimal("10510000"), at=later)
    decision2, _ = evaluate_envelope_gates(
        envelope, quiet, _inputs(now=later, price_observed_at=later)
    )
    assert decision2.verdict == VERDICT_PAUSE
    # 暂停届满且价稳后放行
    after = NOW + timedelta(seconds=3601)
    calm = replace_history(new_state, price=Decimal("10510000"), at=after)
    decision3, _ = evaluate_envelope_gates(
        envelope, calm, _inputs(
            now=after, price_observed_at=after,
            decision_time=after - timedelta(minutes=5),
        )
    )
    assert decision3.verdict == VERDICT_ALLOW


def replace_history(
    state: EnvelopeState, *, price: Decimal, at: datetime
) -> EnvelopeState:
    """以单点观测替换历史，模拟窗口滚动后的平稳价。"""
    from dataclasses import replace

    return replace(
        state, price_history=(PriceObservation(at=at, price=price),)
    )


def test_within_threshold_move_does_not_pause(tmp_path: Path) -> None:
    """恰等于阈值的涨跌幅不触发暂停。"""
    envelope = _load(tmp_path, _body())
    state = _cleared_state()
    state = observe_price(
        state, price=Decimal("10000000"), at=NOW - timedelta(seconds=60)
    )
    state = observe_price(state, price=Decimal("10500000"), at=NOW)
    paused, move = apply_price_move_gate(envelope, state, now=NOW)
    assert move == Decimal("500")
    assert paused.paused_until is None


def test_loss_gates_and_precedence(tmp_path: Path) -> None:
    """亏损门语义与裁决次序：熔断优先于跳过。"""
    envelope = _load(tmp_path, _body())
    state = _cleared_state()
    # 累计亏损达阈值即熔断
    decision, _ = evaluate_envelope_gates(
        envelope, state, _inputs(current_value_jpy=Decimal("90000"))
    )
    assert decision.verdict == VERDICT_TRIP
    assert decision.reason is not None
    assert decision.reason.startswith("cumulative_loss")
    # 当日亏损达阈值当日停机
    decision2, _ = evaluate_envelope_gates(
        envelope, state, _inputs(current_value_jpy=Decimal("97000"))
    )
    assert decision2.verdict == VERDICT_HALT
    # 行情陈旧熔断优先于点差跳过
    decision3, _ = evaluate_envelope_gates(
        envelope, state, _inputs(
            price_observed_at=NOW - timedelta(seconds=120),
            spread_bp=Decimal("999"),
        ),
    )
    assert decision3.verdict == VERDICT_TRIP
    assert decision3.reason is not None
    assert decision3.reason.startswith("stream_freshness")
    # 全部通过时放行且记录全部门禁
    decision4, _ = evaluate_envelope_gates(envelope, state, _inputs())
    assert decision4.verdict == VERDICT_ALLOW
    assert {row.name for row in decision4.gates} >= {
        "validity", "trip_lock", "stream_freshness", "price_move",
        "cumulative_loss", "day_loss", "envelope_total", "prediction_age",
        "day_budget", "order_max", "first_order_canary", "position_cap",
        "spread", "book_depth",
    }


def test_validity_and_trip_lock_halt(tmp_path: Path) -> None:
    """期外与熔断锁定都以停机语义拒绝。"""
    envelope = _load(tmp_path, _body())
    state = _cleared_state()
    outside = datetime(2026, 10, 1, 0, 0, tzinfo=UTC)
    decision, _ = evaluate_envelope_gates(
        envelope, state,
        _inputs(now=outside, price_observed_at=outside),
    )
    assert decision.verdict == VERDICT_HALT
    from dataclasses import replace

    locked = replace(state, tripped_at=NOW, trip_reason="测试")
    decision2, _ = evaluate_envelope_gates(envelope, locked, _inputs())
    assert decision2.verdict == VERDICT_HALT
    assert decision2.reason is not None
    assert decision2.reason.startswith("trip_lock")
