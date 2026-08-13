export type TimeRange = readonly [number, number];

/** 从目标段扣除已有覆盖，返回未覆盖子段。 */
export function uncoveredRanges(
  fromS: number,
  toS: number,
  coverage: readonly TimeRange[],
): TimeRange[] {
  if (fromS >= toS) {
    return [];
  }
  const ordered = coverage
    .map(([low, high]) => [Math.max(fromS, low), Math.min(toS, high)] as const)
    .filter(([low, high]) => low < high)
    .sort(([left], [right]) => left - right);
  const out: TimeRange[] = [];
  let cursor = fromS;
  for (const [low, high] of ordered) {
    if (low > cursor) {
      out.push([cursor, low]);
    }
    cursor = Math.max(cursor, high);
    if (cursor >= toS) {
      break;
    }
  }
  if (cursor < toS) {
    out.push([cursor, toS]);
  }
  return out;
}
