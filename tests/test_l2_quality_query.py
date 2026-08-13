"""L2 物化质量的只读查询与 API 契约。"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from fastapi.testclient import TestClient

from guvolu.data import store
from guvolu.data.l2_quality import L2QualityWindow, upsert_quality_windows
from guvolu.data.materialize import ensure_markets
from guvolu.domain.config import load_config
from guvolu.ui.query_catalog import QueryCatalog
from guvolu.ui.query_service import create_app
from guvolu.venues import registry

MARKET_ID = "mkt__gmo__btc__r0"


def _quality_root(root: Path) -> None:
    conn = store.connect(root)
    try:
        registry.register_all(conn)
        ensure_markets(conn)
        row = L2QualityWindow(
            market_id=MARKET_ID,
            window_start="2026-08-12T00:00:00+00:00",
            window_end="2026-08-12T00:05:00+00:00",
            quality_version="l2-quality-v1",
            source_head_generation="sha256-" + "a" * 64,
            source_attempt_ids='["attempt-1"]', source_attempt_count=1,
            source_normalization_versions='["book-l2-normalization-v4"]',
            window_clock_basis="ingest", frames=12, snapshot_frames=1,
            delta_frames=11, connection_count=1, channel_count=1,
            identity_unknown_frames=0,
            first_observation_time="2026-08-12T00:00:01+00:00",
            last_observation_time="2026-08-12T00:04:59+00:00",
            first_event_time="2026-08-12T00:00:00+00:00",
            last_event_time="2026-08-12T00:04:58+00:00",
            first_available_time="2026-08-12T00:00:01+00:00",
            last_available_time="2026-08-12T00:04:59+00:00",
            first_ingest_time="2026-08-12T00:00:01+00:00",
            last_ingest_time="2026-08-12T00:04:59+00:00",
            max_observed_interarrival_ms=1000.0,
            observed_silence_gt_30s=0, sequence_duplicates=0,
            sequence_regressions=1, predecessor_unknown_frames=0,
            unanchored_before_snapshot_frames=2, anchor_unknown_frames=0,
            untrusted_frames=0, fact_untrusted_flag_conflicts=0,
            checksum_status="unsupported", checksum_observed_frames=0,
            checksum_checked_frames=None, checksum_failures=None,
            recv_source_offset_samples=12, recv_source_offset_p50_ms=-2.0,
            recv_source_offset_p95_ms=3.0, latency_status="clock_skewed",
            latest_materialized_observation_time=(
                "2026-08-12T00:04:59+00:00"
            ),
            materialized_freshness_seconds=1.0,
            materialized_freshness_status="fresh", window_complete=1,
            status="failed",
            reasons='["negative_recv_source_offset_clock_skew",'
            '"sequence_regression"]',
            computed_at="2026-08-12T00:05:00+00:00",
        )
        upsert_quality_windows(conn, [row])
    finally:
        conn.close()


def test_missing_v19_table_returns_explicit_unknown(tmp_path: Path) -> None:
    conn = sqlite3.connect(tmp_path / store.DB_FILE_NAME)
    conn.execute("CREATE TABLE legacy_only (id INTEGER)")
    conn.close()
    payload = QueryCatalog(tmp_path).latest_l2_quality(MARKET_ID)
    assert payload["status"] == "unknown"
    assert payload["checksum_status"] == "unknown"
    assert payload["freshness_scope"] == "materialized_only"
    assert payload["wire_freshness_included"] is False


def test_latest_quality_preserves_complete_control_fields(tmp_path: Path) -> None:
    _quality_root(tmp_path)
    payload = QueryCatalog(tmp_path).latest_l2_quality(MARKET_ID)
    assert payload["status"] == "failed"
    assert payload["reasons"] == [
        "negative_recv_source_offset_clock_skew", "sequence_regression",
    ]
    assert payload["checksum_status"] == "unsupported"
    assert payload["unanchored_before_snapshot_frames"] == 2
    assert payload["sequence_regressions"] == 1
    assert payload["latency_status"] == "clock_skewed"
    assert payload["freshness_threshold_seconds"] == 720
    assert payload["checkpoint_freshness_included"] is False


def test_quality_endpoint_is_readonly_and_no_store(tmp_path: Path) -> None:
    _quality_root(tmp_path)
    app = create_app(
        load_config(env_file=tmp_path / "absent.env"),
        object(), object(), "token", data_root=tmp_path,
    )
    client = TestClient(
        app, base_url="http://127.0.0.1",
        headers={"X-Guvolu-Token": "token"},
    )
    response = client.get(
        f"/api/v2/markets/{MARKET_ID}/book/l2/quality"
    )
    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    assert response.json()["freshness_basis"] == (
        "latest_materialized_observation_time"
    )
