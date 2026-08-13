import assert from "node:assert/strict";
import test from "node:test";
import type { L2QualityResponse } from "./api.ts";
import { l2QualityView } from "./l2-quality-view.ts";

function quality(
  patch: Partial<L2QualityResponse>,
): L2QualityResponse {
  return {
    schema_version: 1, market_id: "mkt", quality_version: "l2-quality-v1",
    status: "ok", reasons: [], window_start: null, window_end: null,
    window_clock_basis: "ingest", frames: 10, snapshot_frames: 1,
    delta_frames: 9, checksum_status: "unsupported",
    checksum_observed_frames: 0, checksum_checked_frames: null,
    checksum_failures: null, unanchored_before_snapshot_frames: 0,
    anchor_unknown_frames: 0, sequence_duplicates: 0,
    sequence_regressions: 0, predecessor_unknown_frames: 0,
    latency_status: "measurable", recv_source_offset_samples: 10,
    recv_source_offset_p50_ms: 1,
    recv_source_offset_p95_ms: 2,
    latest_materialized_observation_time: "2026-08-12T00:00:00Z",
    materialized_freshness_seconds: 2,
    materialized_freshness_status: "fresh",
    freshness_basis: "latest_materialized_observation_time",
    freshness_threshold_seconds: 720, freshness_scope: "materialized_only",
    wire_freshness_included: false, checkpoint_freshness_included: false,
    computed_at: "2026-08-12T00:00:02Z", ...patch,
  };
}

test("quality view preserves unsupported, hard counts and clock skew", () => {
  const view = l2QualityView(quality({
    status: "degraded", unanchored_before_snapshot_frames: 2,
    sequence_regressions: 1, latency_status: "clock_skewed",
  }));
  assert.equal(view.tone, "warning");
  assert.equal(view.checksum, "unsupported");
  assert.equal(view.anchorHard, "2");
  assert.equal(view.sequenceHard, "1");
  assert.equal(view.clockSkewed, true);
});

test("missing quality maps to explicit unknown", () => {
  const view = l2QualityView(null);
  assert.equal(view.status, "unknown");
  assert.equal(view.checksum, "unknown");
});
