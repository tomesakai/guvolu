import type { L2QualityResponse } from "./api.ts";
import { EMPTY_TEXT, jstStamp } from "./format.ts";

export interface L2QualityView {
  readonly tone: string;
  readonly status: string;
  readonly freshness: string;
  readonly checksum: string;
  readonly anchorHard: string;
  readonly sequenceHard: string;
  readonly clockSkewed: boolean;
}

function hardCount(values: readonly (number | null)[]): string {
  return values.some((value) => value === null)
    ? EMPTY_TEXT
    : String(values.reduce<number>((total, value) => total + (value ?? 0), 0));
}

export function l2QualityView(
  quality: L2QualityResponse | null,
): L2QualityView {
  if (quality === null) {
    return {
      tone: "muted", status: "unknown", freshness: "unknown",
      checksum: "unknown", anchorHard: EMPTY_TEXT,
      sequenceHard: EMPTY_TEXT, clockSkewed: false,
    };
  }
  const tone = quality.status === "ok"
    ? "positive"
    : quality.status === "degraded"
      ? "warning"
      : quality.status === "failed"
        ? "danger"
        : "muted";
  return {
    tone,
    status: quality.status,
    freshness: `${quality.materialized_freshness_status} @ ${jstStamp(
      quality.latest_materialized_observation_time,
    )}`,
    checksum: quality.checksum_status,
    anchorHard: hardCount([quality.unanchored_before_snapshot_frames]),
    sequenceHard: hardCount([
      quality.sequence_duplicates,
      quality.sequence_regressions,
    ]),
    clockSkewed:
      quality.latency_status === "clock_skewed" ||
      quality.materialized_freshness_status === "clock_skewed",
  };
}
