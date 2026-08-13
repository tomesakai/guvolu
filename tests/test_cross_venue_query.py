"""跨所盘口顶层的 PIT、质量、quorum 与身份边界。"""
from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest
from fastapi.testclient import TestClient

from guvolu.data import store
from guvolu.data.materialize import ensure_markets
from guvolu.domain.config import load_config
from guvolu.ui.cross_venue_query import (
    CrossVenueCompatibilityError,
    CrossVenueQuery,
)
from guvolu.ui.query_catalog import (
    ActiveOutput,
    ActiveOutputSnapshot,
    MultiMarketOutputSnapshot,
    QueryCatalog,
)
from guvolu.ui.query_service import create_app
from guvolu.venues import registry

DECISION = datetime(2026, 8, 13, 1, 0, tzinfo=UTC)


def _snapshot(
    market_id: str, venue: str, *, quote: str = "JPY", market_kind: str = "spot",
) -> ActiveOutputSnapshot:
    attempt = f"attempt-{venue}"
    market = {
        "market_id": market_id, "venue_id": venue, "venue_symbol": "BTC",
        "instrument_id": f"SPOT:BTC/{quote}", "mapping_revision": 0,
        "market_kind": market_kind, "base_currency": "BTC",
        "quote_currency": quote, "instrument_kind": "spot",
        "tick_size": "1", "size_step": "0.001", "min_size": "0.001",
    }
    outputs = tuple(
        ActiveOutput(
            domain="book_l2", partition_key="p1",
            normalization_version="book-l2-normalization-v5",
            attempt_id=attempt, dataset=dataset,
            artifact_id=f"artifact-{venue}-{dataset}",
            path=Path(f"{venue}-{dataset}.parquet"), row_count=1,
            min_event_time=DECISION - timedelta(seconds=20),
            max_event_time=DECISION - timedelta(seconds=10),
        )
        for dataset in ("book_l2_frame", "book_l2_level")
    )
    return ActiveOutputSnapshot(market, outputs, f"head-{venue}")


def _quality(venue: str, **changes: Any) -> dict[str, Any]:
    result: dict[str, Any] = {
        "quality_version": "l2-quality-v1", "window_start": "w1",
        "computed_at": (DECISION - timedelta(seconds=5)).isoformat(),
        "status": "ok", "reasons": [],
        "source_attempt_ids": [f"attempt-{venue}"],
        "source_attempt_count": 1,
        "materialized_freshness_status": "fresh",
        "sequence_duplicates": 0, "sequence_regressions": 0,
        "checksum_failures": 0, "unanchored_before_snapshot_frames": 0,
        "anchor_unknown_frames": 0,
    }
    result.update(changes)
    return result


class _Catalog:
    def __init__(self, frozen: MultiMarketOutputSnapshot) -> None:
        self.frozen = frozen
        self.decisions: list[datetime] = []

    def active_outputs_many(self, *_: Any, **kwargs: Any) -> MultiMarketOutputSnapshot:
        self.decisions.append(kwargs["decision_time"])
        return self.frozen


class _Materialized:
    def __init__(
        self, snapshots: list[ActiveOutputSnapshot], qualities: list[dict[str, Any]],
    ) -> None:
        self.catalog = _Catalog(MultiMarketOutputSnapshot(
            decision_time=DECISION, markets=tuple(snapshots),
            qualities=tuple(
                (str(snapshot.market["market_id"]), quality)
                for snapshot, quality in zip(snapshots, qualities, strict=True)
            ),
            head_generation="frozen-head",
        ))
        self.books: dict[str, dict[str, Any]] = {}
        self.decisions: list[datetime] = []

    def latest_l2_from_snapshot(
        self, snapshot: ActiveOutputSnapshot, _depth: int, **kwargs: Any,
    ) -> tuple[dict[str, Any], str]:
        self.decisions.append(kwargs["decision_time"])
        return self.books[str(snapshot.market["market_id"])], '"tag"'


def _book(
    bid: str, ask: str, *, age_seconds: int = 10,
) -> dict[str, Any]:
    available = DECISION - timedelta(seconds=age_seconds)
    return {
        "best_bid": bid, "best_ask": ask,
        "bids": [{"price": bid, "size": "2"}],
        "asks": [{"price": ask, "size": "3"}],
        "meta": {
            "as_of_event_time": (available - timedelta(seconds=1)).isoformat(),
            "as_of_available_time": available.isoformat(),
            "state_source": "book_state_checkpoint", "integrity_mode": "ok",
            "as_of_frame_id": "frame", "snapshot_frame_id": "snapshot",
            "source_attempt_id": "source-attempt",
            "source_artifact_id": "source-artifact",
            "state_attempt_id": "state-attempt",
            "state_artifact_id": "state-artifact",
        },
    }


def _query(
    specs: list[tuple[str, str]], *, qualities: list[dict[str, Any]] | None = None,
    quote: str = "JPY",
) -> tuple[CrossVenueQuery, _Materialized]:
    snapshots = [_snapshot(market, venue, quote=quote) for market, venue in specs]
    selected_qualities = qualities or [_quality(venue) for _, venue in specs]
    materialized = _Materialized(snapshots, selected_qualities)
    for index, (market, _) in enumerate(specs):
        materialized.books[market] = _book(
            str(100 + index * 2), str(102 + index * 2),
        )
    return CrossVenueQuery(cast(Any, materialized)), materialized


def test_three_venue_crossed_bbo_median_and_lineage() -> None:
    query, source = _query([("a", "gmo"), ("b", "bitbank"), ("c", "bitflyer")])
    source.books["a"] = _book("102", "104")
    source.books["b"] = _book("101", "102")
    source.books["c"] = _book("105", "107")

    payload = query.latest_top(["a", "b", "c"], decision_time=DECISION)

    assert payload["quality_state"] == "ok"
    assert payload["consolidated_bbo"] == {
        "bid": "105", "ask": "102", "spread": "-3", "crossed": True,
        "bid_contributors": ["c"], "ask_contributors": ["b"],
    }
    assert payload["robust_mid_reference"]["price"] == "103"
    assert payload["robust_mid_reference"]["robustness"] == "strong"
    assert payload["contributors"][0]["state_artifact_id"] == "state-artifact"
    assert payload["contributors"][0]["source_attempt_count"] == 1
    assert payload["contributors"][0]["source_attempt_set_hash"].startswith(
        "sha256-"
    )
    assert source.decisions == [DECISION, DECISION, DECISION]


def test_dynamic_staleness_excludes_stored_fresh_source_but_two_meet_quorum() -> None:
    query, source = _query([("a", "gmo"), ("b", "bitbank"), ("c", "bitflyer")])
    source.books["c"] = _book("104", "106", age_seconds=721)

    payload = query.latest_top(["a", "b", "c"], decision_time=DECISION)

    assert payload["quorum"] == {
        "required": 2, "eligible": 2, "contributing": 2, "met": True,
    }
    assert payload["quality_state"] == "degraded"
    assert payload["robust_mid_reference"]["robustness"] == "weak"
    assert payload["excluded"][0]["reasons"] == [
        "materialized_state_stale_at_decision"
    ]


def test_single_fallback_never_claims_quorum_or_robustness() -> None:
    qualities = [_quality("gmo"), _quality("bitbank", status="failed")]
    query, _ = _query([("a", "gmo"), ("b", "bitbank")], qualities=qualities)

    payload = query.latest_top(["a", "b"], decision_time=DECISION)

    assert payload["quorum"]["met"] is False
    assert payload["quality_state"] == "unavailable"
    assert payload["robust_mid_reference"]["robustness"] == "single_fallback"
    assert payload["robust_mid_reference"]["estimator"] == "single_source_mid"


def test_quality_attempt_mismatch_is_hard_exclusion() -> None:
    qualities = [_quality("gmo"), _quality("bitbank", source_attempt_ids=["old"])]
    query, _ = _query([("a", "gmo"), ("b", "bitbank")], qualities=qualities)

    payload = query.latest_top(["a", "b"], decision_time=DECISION, min_quorum=1)

    assert payload["excluded"][0]["reasons"] == [
        "quality_source_attempts_do_not_match_active_heads"
    ]


def test_sequence_failure_is_hard_but_clock_skew_is_soft() -> None:
    qualities = [
        _quality("gmo", sequence_regressions=1),
        _quality(
            "bitbank", status="degraded",
            reasons=["negative_recv_source_offset_clock_skew"],
        ),
    ]
    query, _ = _query([("a", "gmo"), ("b", "bitbank")], qualities=qualities)

    payload = query.latest_top(["a", "b"], decision_time=DECISION, min_quorum=1)

    assert payload["excluded"][0]["reasons"] == [
        "sequence_regressions_observed"
    ]
    assert payload["contributors"][0]["quality_state"] == "degraded"
    assert payload["contributors"][0]["quality_reasons"] == [
        "negative_recv_source_offset_clock_skew"
    ]


def test_historical_not_applicable_quality_is_not_live_contributor() -> None:
    qualities = [
        _quality("gmo"),
        _quality("bitbank", materialized_freshness_status="not_applicable"),
    ]
    query, _ = _query([("a", "gmo"), ("b", "bitbank")], qualities=qualities)

    payload = query.latest_top(["a", "b"], decision_time=DECISION, min_quorum=1)

    assert payload["excluded"][0]["reasons"] == [
        "historical_freshness_not_applicable_to_latest_aggregate"
    ]


def test_jpy_and_usdt_are_rejected_without_fx() -> None:
    snapshots = [_snapshot("a", "gmo"), _snapshot("b", "okx", quote="USDT")]
    materialized = _Materialized(snapshots, [_quality("gmo"), _quality("okx")])
    query = CrossVenueQuery(cast(Any, materialized))

    with pytest.raises(CrossVenueCompatibilityError, match="JPY 与 USDT"):
        query.latest_top(["a", "b"], decision_time=DECISION)


def test_spot_and_leverage_market_kinds_are_rejected() -> None:
    snapshots = [
        _snapshot("a", "gmo"),
        _snapshot("b", "bitflyer", market_kind="leverage"),
    ]
    materialized = _Materialized(
        snapshots, [_quality("gmo"), _quality("bitflyer")],
    )
    query = CrossVenueQuery(cast(Any, materialized))

    with pytest.raises(CrossVenueCompatibilityError, match="同 base/quote"):
        query.latest_top(["a", "b"], decision_time=DECISION)


def test_distinct_instrument_ids_are_rejected_even_when_symbols_match() -> None:
    snapshots = [_snapshot("a", "gmo"), _snapshot("b", "bitbank")]
    snapshots[1].market["instrument_id"] = "SPOT:BTC/JPY:alternate"
    materialized = _Materialized(
        snapshots, [_quality("gmo"), _quality("bitbank")],
    )
    query = CrossVenueQuery(cast(Any, materialized))

    with pytest.raises(CrossVenueCompatibilityError, match="同 base/quote"):
        query.latest_top(["a", "b"], decision_time=DECISION)


def test_lineage_response_is_compact_and_deterministic() -> None:
    query, source = _query([("a", "gmo"), ("b", "bitbank"), ("c", "bitflyer")])
    snapshots = list(source.catalog.frozen.markets)
    expanded: list[ActiveOutputSnapshot] = []
    qualities: list[dict[str, Any]] = []
    for snapshot, (_, quality) in zip(
        snapshots, source.catalog.frozen.qualities, strict=True,
    ):
        outputs = tuple(
            ActiveOutput(
                domain="book_l2", partition_key=f"p{index}",
                normalization_version="book-l2-normalization-v5",
                attempt_id=f"{snapshot.market['venue_id']}-attempt-{index:04d}",
                dataset=dataset,
                artifact_id=(
                    f"sha256-{index:064x}" if dataset == "book_l2_frame"
                    else f"sha256-{index + 10_000:064x}"
                ),
                path=Path(f"{index}-{dataset}.parquet"), row_count=1,
                min_event_time=DECISION - timedelta(seconds=20),
                max_event_time=DECISION - timedelta(seconds=10),
            )
            for index in range(400)
            for dataset in ("book_l2_frame", "book_l2_level")
        )
        expanded.append(ActiveOutputSnapshot(
            snapshot.market, outputs, snapshot.head_generation,
        ))
        attempts = sorted({row.attempt_id for row in outputs})
        qualities.append({
            **quality,
            "source_attempt_ids": attempts,
            "source_attempt_count": len(attempts),
        })
    source.catalog.frozen = MultiMarketOutputSnapshot(
        DECISION, tuple(expanded), tuple(
            (str(snapshot.market["market_id"]), quality)
            for snapshot, quality in zip(expanded, qualities, strict=True)
        ), "frozen-head",
    )

    first = query.latest_top(["a", "b", "c"], decision_time=DECISION)
    second = query.latest_top(["a", "b", "c"], decision_time=DECISION)
    encoded = json.dumps(first, sort_keys=True, separators=(",", ":"))

    assert first == second
    assert len(encoded.encode()) < 15_000
    assert "source_attempt_ids" not in encoded
    assert first["contributors"][0]["source_attempt_count"] == 400


def test_requested_market_order_is_canonical() -> None:
    query, _ = _query([("a", "gmo"), ("b", "bitbank"), ("c", "bitflyer")])

    forward = query.latest_top(["a", "b", "c"], decision_time=DECISION)
    reverse = query.latest_top(["c", "b", "a"], decision_time=DECISION)

    assert reverse == forward
    assert forward["expected_market_ids"] == ["a", "b", "c"]


def test_catalog_freezes_multiple_market_heads_with_one_connection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    market_ids = ["mkt__gmo__btc__r0", "mkt__bitbank__btc_jpy__r0"]
    conn = store.connect(tmp_path)
    try:
        registry.register_all(conn)
        ensure_markets(conn)
        for index, market_id in enumerate(market_ids):
            path = tmp_path / f"frame-{index}.parquet"
            path.write_bytes(b"registered immutable fixture")
            attempt = f"attempt-{index}"
            digest = hashlib.sha256(attempt.encode()).hexdigest()
            artifact = "sha256-" + digest
            conn.execute(
                "INSERT INTO partition_attempt (attempt_id,market_id,domain,"
                "partition_key,normalization_version,input_set_hash,status,"
                "source_rows,normalized_rows,ignored_rows,rejected_rows,started_at,"
                "finished_at,code_version,config_hash) VALUES "
                "(?,?,'book_l2','p1','l2-v1',?,'complete',1,1,0,0,?,?, 't','c')",
                (attempt, market_id, digest, DECISION.isoformat(), DECISION.isoformat()),
            )
            conn.execute(
                "INSERT INTO artifact VALUES (?, 'materialized_parquet', ?, ?, 1,"
                "?,?, 'sha256-file-v1', ?)",
                (artifact, path.name, digest, DECISION.isoformat(),
                 DECISION.isoformat(), path.stat().st_size),
            )
            conn.execute(
                "INSERT INTO materialization_output VALUES "
                "(?,?,'book_l2_frame',1,?,?,?)",
                (attempt, artifact, DECISION.isoformat(), DECISION.isoformat(),
                 DECISION.isoformat()),
            )
            conn.execute(
                "INSERT INTO materialization_partition_head VALUES "
                "(?,'book_l2','p1','l2-v1',?,?)",
                (market_id, attempt, DECISION.isoformat()),
            )
        conn.commit()
    finally:
        conn.close()
    import guvolu.ui.query_catalog as catalog_module
    original = catalog_module.connect_readonly
    calls = 0

    def tracked(root: Path) -> Any:
        nonlocal calls
        calls += 1
        return original(root)

    monkeypatch.setattr(catalog_module, "connect_readonly", tracked)
    frozen = QueryCatalog(tmp_path).active_outputs_many(
        market_ids, domains=("book_l2",), datasets=("book_l2_frame",),
        decision_time=DECISION,
    )

    assert calls == 1
    assert [item.market["market_id"] for item in frozen.markets] == market_ids
    assert frozen.decision_time == DECISION


def test_aggregate_endpoint_is_no_store(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        CrossVenueQuery,
        "latest_top",
        lambda self, market_ids, **kwargs: {
            "market_ids": list(market_ids), "quality_state": "ok",
        },
    )
    app = create_app(
        load_config(env_file=tmp_path / "absent.env"),
        object(), object(), "token", data_root=tmp_path,
    )
    client = TestClient(
        app, base_url="http://127.0.0.1",
        headers={"X-Guvolu-Token": "token"},
    )

    response = client.get(
        "/api/v2/aggregates/book/top?market_id=a&market_id=b"
    )

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    assert response.json()["market_ids"] == ["a", "b"]
