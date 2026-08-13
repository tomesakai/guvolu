"""L2 被动网格 shadow 的成交边界与质量门测试。"""
from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

from guvolu.research.passive_grid_shadow import (
    PassiveBucket,
    PassiveCandidate,
    PassiveFill,
    SimulationMetrics,
    TradeAtPrice,
    _trade_quality,
    simulate_candidate,
    verify_passive_grid_shadow,
)
from guvolu.research.provenance import canonical_json, stable_identifier
from guvolu.ui.query_catalog import ActiveOutput, ActiveOutputSnapshot


def _bucket(
    index: int,
    *,
    clean: bool = True,
    buys: tuple[tuple[str, str], ...] = (),
    sells: tuple[tuple[str, str], ...] = (),
) -> PassiveBucket:
    start = datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=5 * index)
    return PassiveBucket(
        bucket_epoch=int(start.timestamp()),
        bucket_start=start,
        bucket_end=start + timedelta(seconds=5),
        clean=clean,
        best_bid=Decimal("100") if clean else None,
        best_ask=Decimal("102") if clean else None,
        taker_buys=tuple(
            TradeAtPrice(Decimal(price), Decimal(size)) for price, size in buys
        ),
        taker_sells=tuple(
            TradeAtPrice(Decimal(price), Decimal(size)) for price, size in sells
        ),
    )


def _simulate(
    buckets: tuple[PassiveBucket, ...], rule: str,
) -> tuple[SimulationMetrics, tuple[PassiveFill, ...]]:
    metrics, fills = simulate_candidate(
        buckets,
        PassiveCandidate("candidate", 0, 2),
        price_row_size=Decimal("1"),
        order_size=Decimal("0.1"),
        latency_buckets=0,
        maker_fee_bps=Decimal("0"),
        terminal_rebalance_bps=Decimal("0"),
        markout_horizons_seconds=(5,),
        rule=rule,
    )
    return metrics, fills


def test_touch_is_only_an_optimistic_fill() -> None:
    """只触价不得进入悲观路径，乐观路径也不得超过观察成交量。"""
    buckets = (
        _bucket(0),
        _bucket(
            1,
            buys=(("102", "0.04"),),
            sells=(("100", "0.03"),),
        ),
    )
    lower, lower_fills = _simulate(buckets, "trade_through_pessimistic")
    upper, upper_fills = _simulate(buckets, "touch_queue_optimistic")
    assert lower.fill_events == 0
    assert lower_fills == ()
    assert upper.fill_events == 2
    assert sorted(item.size for item in upper_fills) == [
        Decimal("0.03"), Decimal("0.04"),
    ]


def test_trade_through_fills_both_bounds_and_respects_inventory() -> None:
    """严格穿价视作报价前队列已清；库存路径仍不得越界。"""
    buckets = (
        _bucket(0),
        _bucket(1, buys=(("103", "0.001"),), sells=(("99", "0.001"),)),
        _bucket(2, buys=(("103", "0.001"),)),
        _bucket(3, buys=(("103", "0.001"),)),
    )
    lower, lower_fills = _simulate(buckets, "trade_through_pessimistic")
    upper, upper_fills = _simulate(buckets, "touch_queue_optimistic")
    assert lower.fill_events == upper.fill_events
    assert len(lower_fills) == len(upper_fills) == 3
    assert lower.minimum_inventory == Decimal("0")
    assert lower.maximum_inventory <= Decimal("0.2")


def test_gap_cancels_pending_quote_and_resets_segment() -> None:
    """报价和库存损益不得跨越不可观察盘口缺口。"""
    buckets = (
        _bucket(0),
        _bucket(1, clean=False),
        _bucket(2, buys=(("103", "1"),), sells=(("99", "1"),)),
        _bucket(3),
    )
    lower, fills = _simulate(buckets, "trade_through_pessimistic")
    assert fills == ()
    assert lower.fill_events == 0
    assert lower.segments == 1
    assert lower.terminal_excess_pnl_quote == 0


def test_mirrored_trade_quality_detects_two_sided_public_feed(
    tmp_path: Path,
) -> None:
    """同一成交键出现相反 side 时必须被质量门计为镜像行。"""
    path = tmp_path / "trades.parquet"
    db = duckdb.connect(":memory:")
    try:
        db.execute("""
          CREATE TABLE trades(
            observation_id VARCHAR,market_id VARCHAR,event_time TIMESTAMPTZ,
            available_time TIMESTAMPTZ,ingest_time TIMESTAMPTZ,
            source_artifact_id VARCHAR,price VARCHAR,size VARCHAR,side VARCHAR,
            source_side_basis VARCHAR
          )
        """)
        moment = datetime(2026, 1, 1, tzinfo=UTC)
        db.executemany(
            "INSERT INTO trades VALUES (?,?,?,?,?,?,?,?,?,?)",
            [
                ("a", "m", moment, moment, moment, "x", "100", "1", "buy", "taker"),
                ("b", "m", moment, moment, moment, "x", "100", "1", "sell", "taker"),
                ("c", "m", moment, moment, moment, "x", "101", "1", "buy", "taker"),
            ],
        )
        db.execute("COPY trades TO ? (FORMAT PARQUET)", [str(path)])
    finally:
        db.close()
    output = ActiveOutput(
        domain="trade_realtime",
        partition_key="p",
        normalization_version="v",
        attempt_id="attempt",
        dataset="trade_observation",
        artifact_id="sha256-" + "1" * 64,
        path=path,
        row_count=3,
        min_event_time=moment,
        max_event_time=moment,
    )
    quality = _trade_quality(ActiveOutputSnapshot(
        market={"market_id": "m"},
        outputs=(output,),
        head_generation="sha256-" + "2" * 64,
    ))
    assert quality["rows"] == 3
    assert quality["mirrored_rows"] == 2
    assert quality["mirrored_trade_ratio"] == 2 / 3


def test_verifier_recomputes_run_identity_and_checks_latest_hash(
    tmp_path: Path,
) -> None:
    """重写 manifest 与活动指针散列也不能绕过输入身份复算。"""
    input_identity = {"method_version": "test", "artifact": "a"}
    run_id = stable_identifier("passive-grid-shadow", input_identity)
    output = tmp_path / "reports/passive-grid-shadow" / run_id
    output.mkdir(parents=True)
    fills = output / "fills.jsonl"
    fills.write_text("", encoding="utf-8")

    def record(path: Path, kind: str) -> dict[str, object]:
        content = path.read_bytes()
        return {
            "kind": kind,
            "path": path.relative_to(tmp_path).as_posix(),
            "sha256": hashlib.sha256(content).hexdigest(),
            "bytes": len(content),
        }

    fills_record = record(fills, "passive_grid_fills")
    summary = output / "summary.json"
    summary.write_text(canonical_json({
        "run_id": run_id,
        "input_identity": input_identity,
        "fills_artifact": fills_record,
    }) + "\n", encoding="utf-8")
    manifest = output / "manifest.json"
    body = {
        "run_id": run_id,
        "status": "complete",
        "input_identity": input_identity,
        "summary": record(summary, "passive_grid_summary"),
        "fills": fills_record,
    }
    manifest.write_text(canonical_json(body) + "\n", encoding="utf-8")
    latest = tmp_path / "reports/passive-grid-shadow/latest.json"
    latest.write_text(canonical_json({
        "run_id": run_id,
        "manifest": manifest.relative_to(tmp_path).as_posix(),
        "manifest_sha256": hashlib.sha256(manifest.read_bytes()).hexdigest(),
    }) + "\n", encoding="utf-8")
    assert verify_passive_grid_shadow(tmp_path, run_id)["verified"] is True

    body["input_identity"] = {"method_version": "tampered"}
    manifest.write_text(canonical_json(body) + "\n", encoding="utf-8")
    latest_body = json.loads(latest.read_text(encoding="utf-8"))
    latest_body["manifest_sha256"] = hashlib.sha256(manifest.read_bytes()).hexdigest()
    latest.write_text(canonical_json(latest_body) + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="运行身份"):
        verify_passive_grid_shadow(tmp_path, run_id)
