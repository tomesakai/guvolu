"""L2 被动网格 shadow 的成交边界与质量门测试。"""
from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

from guvolu.research.passive_grid_shadow import (
    _trade_gap_spans,
    PassiveBucket,
    PassiveCandidate,
    PassiveFill,
    SimulationMetrics,
    TradeAtPrice,
    _exclude_participant_side_buckets,
    _participant_side_spans,
    _trade_gates,
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


def _trade_snapshot(path: Path, rows: list[tuple[object, ...]]) -> ActiveOutputSnapshot:
    db = duckdb.connect(":memory:")
    try:
        db.execute("""
          CREATE TABLE trades(
            observation_id VARCHAR,market_id VARCHAR,event_time TIMESTAMPTZ,
            available_time TIMESTAMPTZ,ingest_time TIMESTAMPTZ,
            source_artifact_id VARCHAR,price VARCHAR,size VARCHAR,side VARCHAR,
            source_side_basis VARCHAR,run_id VARCHAR,connection_id VARCHAR
          )
        """)
        if rows:
            db.executemany(
                "INSERT INTO trades VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", rows,
            )
        db.execute("COPY trades TO ? (FORMAT PARQUET)", [str(path)])
    finally:
        db.close()
    moment = datetime(2026, 1, 1, tzinfo=UTC)
    output = ActiveOutput(
        domain="trade_realtime",
        partition_key="p",
        normalization_version="v",
        attempt_id="attempt",
        dataset="trade_observation",
        artifact_id="sha256-" + "1" * 64,
        path=path,
        row_count=len(rows),
        min_event_time=moment,
        max_event_time=moment,
    )
    return ActiveOutputSnapshot(
        market={"market_id": "m"},
        outputs=(output,),
        head_generation="sha256-" + "2" * 64,
    )


def test_mirrored_trade_quality_separates_feed_property_from_duplication(
    tmp_path: Path,
) -> None:
    """双侧参与方行情按运行剔除，真实重复只数跨连接同键投递。"""
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    t1 = t0 + timedelta(seconds=30)
    t2 = t0 + timedelta(seconds=40)
    t3 = t0 + timedelta(seconds=50)

    def row(
        identity: str, moment: datetime, price: str, size: str, side: str,
        run_id: str, connection_id: str, basis: str = "taker",
    ) -> tuple[object, ...]:
        return (
            identity, "m", moment, moment, moment, "x", price, size, side,
            basis, run_id, connection_id,
        )

    snapshot = _trade_snapshot(tmp_path / "trades.parquet", [
        row("a", t0, "100", "1", "buy", "r0", "r0-c1"),
        row("b", t0, "100", "1", "sell", "r0", "r0-c1"),
        row("c", t0, "101", "1", "buy", "r0", "r0-c1"),
        row("d", t1, "100", "1", "buy", "r1", "r1-c1"),
        row("e", t1, "100", "1", "buy", "r1", "r1-c1"),
        row("f", t1, "102", "1", "sell", "r1", "r1-c1"),
        row("g", t1, "102", "1", "sell", "r1", "r1-c2"),
        row("h", t2, "103", "2", "buy", "r1", "r1-c1"),
        row("i", t3, "104", "1", "buy", "r2", "r2-c1", "participant_side_unfiltered"),
    ])
    quality = _trade_quality(snapshot, 0.5)
    assert quality["rows"] == 9
    assert quality["mirrored_rows"] == 2
    assert quality["mirrored_trade_ratio"] == 2 / 9
    feed = quality["participant_side_feed"]
    assert isinstance(feed, dict)
    assert feed["rows"] == 4
    assert set(feed["runs"]) == {"r0", "r2"}
    assert feed["runs"]["r0"] == {
        "rows": 3,
        "mirrored_rows": 2,
        "mirrored_trade_ratio": 2 / 3,
        "non_taker_rows": 0,
        "from": t0.isoformat(),
        "to": t0.isoformat(),
    }
    assert feed["runs"]["r2"]["non_taker_rows"] == 1
    assert quality["taker_rows"] == 5
    assert quality["taker_mirrored_rows"] == 0
    assert quality["duplicate_rows"] == 1
    assert quality["duplicate_trade_ratio"] == 1 / 5
    assert quality["repeated_key_rows"] == 1
    assert quality["source_side_basis"] == {
        "participant_side_unfiltered": 1, "taker": 8,
    }
    assert _trade_gates(quality, 0.01) == (
        False, True, ["duplicate_trade_ratio_exceeded"],
    )
    assert _trade_gates(quality, 0.5) == (True, True, [])
    assert _participant_side_spans(quality) == ((t0, t0), (t3, t3))


def test_taker_only_feed_with_repeated_keys_passes_gates(
    tmp_path: Path,
) -> None:
    """同连接同键复现是多笔撮合，不计为镜像或重复。"""
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    snapshot = _trade_snapshot(tmp_path / "trades.parquet", [
        ("a", "m", t0, t0, t0, "x", "100", "1", "buy", "taker", "r1", "c1"),
        ("b", "m", t0, t0, t0, "x", "100", "1", "buy", "taker", "r1", "c1"),
        ("c", "m", t0, t0, t0, "x", "101", "1", "sell", "taker", "r1", "c1"),
    ])
    quality = _trade_quality(snapshot, 0.5)
    assert quality["taker_rows"] == 3
    feed = quality["participant_side_feed"]
    assert isinstance(feed, dict)
    assert feed["rows"] == 0
    assert quality["duplicate_rows"] == 0
    assert quality["repeated_key_rows"] == 1
    assert _trade_gates(quality, 0.01) == (True, True, [])


def test_empty_trade_snapshot_leaves_side_basis_unproven(
    tmp_path: Path,
) -> None:
    """没有 taker 行时方向门不得通过。"""
    snapshot = _trade_snapshot(tmp_path / "trades.parquet", [])
    quality = _trade_quality(snapshot, 0.5)
    assert quality["rows"] == 0
    assert quality["taker_rows"] == 0
    assert _trade_gates(quality, 0.01) == (
        True, False, ["taker_side_basis_unproven"],
    )
    assert _participant_side_spans(quality) == ()


def test_participant_side_spans_mark_overlapping_buckets_unclean() -> None:
    """参与方行情跨度只把相交的桶标为不可信。"""
    buckets = tuple(_bucket(index) for index in range(4))
    start = buckets[0].bucket_start
    spans = ((start + timedelta(seconds=7), start + timedelta(seconds=12)),)
    excluded, count = _exclude_participant_side_buckets(buckets, spans)
    assert count == 2
    assert [item.clean for item in excluded] == [True, False, False, True]
    assert excluded[0] == buckets[0]
    assert _exclude_participant_side_buckets(buckets, ()) == (buckets, 0)
    already_unclean = (replace(buckets[1], clean=False),)
    assert _exclude_participant_side_buckets(already_unclean, spans) == (
        already_unclean, 0,
    )


def test_verifier_recomputes_run_identity_and_checks_latest_hash(
    tmp_path: Path,
) -> None:
    """重写 manifest 与活动指针散列也不能绕过输入身份复算。"""
    data_root = tmp_path / "data"
    data_root.mkdir()
    frozen_input = data_root / "input.parquet"
    frozen_bytes = b"frozen-parquet-bytes"
    frozen_input.write_bytes(frozen_bytes)
    frozen_sha256 = hashlib.sha256(frozen_bytes).hexdigest()
    frozen_record = {
        "attempt_id": "attempt-one",
        "artifact_id": f"sha256-{frozen_sha256}",
        "dataset": "orderflow_tile_column",
        "path": "input.parquet",
        "sha256": frozen_sha256,
        "bytes": len(frozen_bytes),
    }
    input_files = {"tiles": [frozen_record], "trades": [frozen_record]}
    input_identity = {
        "method_version": "test",
        "artifact": "a",
        "input_file_set_id": stable_identifier("sha256", input_files),
    }
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
        "input_files": input_files,
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
    assert verify_passive_grid_shadow(
        tmp_path, run_id, data_root,
    )["verified"] is True

    frozen_input.write_bytes(b"corrupted")
    with pytest.raises(ValueError, match="(字节数|散列)不匹配"):
        verify_passive_grid_shadow(tmp_path, run_id, data_root)
    frozen_input.write_bytes(frozen_bytes)

    body["input_identity"] = {"method_version": "tampered"}
    manifest.write_text(canonical_json(body) + "\n", encoding="utf-8")
    latest_body = json.loads(latest.read_text(encoding="utf-8"))
    latest_body["manifest_sha256"] = hashlib.sha256(manifest.read_bytes()).hexdigest()
    latest.write_text(canonical_json(latest_body) + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="运行身份"):
        verify_passive_grid_shadow(tmp_path, run_id, data_root)


def test_trade_gap_tiles_map_to_hour_spans() -> None:
    """无逐笔依赖的 tile 按分区键折成整小时跨度，其余 tile 不受影响。"""
    from types import SimpleNamespace

    tiles = SimpleNamespace(outputs=[
        SimpleNamespace(attempt_id="a", partition_key="2026-08-22T01/5s"),
        SimpleNamespace(attempt_id="a", partition_key="2026-08-22T01/5s"),
        SimpleNamespace(attempt_id="b", partition_key="2026-08-22T02/5s"),
    ])
    spans = _trade_gap_spans(tiles, ("a",))  # type: ignore[arg-type]
    assert spans == ((
        datetime(2026, 8, 22, 1, tzinfo=UTC),
        datetime(2026, 8, 22, 2, tzinfo=UTC),
    ),)
    assert _trade_gap_spans(tiles, ()) == ()  # type: ignore[arg-type]

