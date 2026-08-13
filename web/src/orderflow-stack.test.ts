import assert from "node:assert/strict";
import test from "node:test";
import type { FootprintBarItem } from "./api.ts";
import {
  buildOrderFlowStack,
  footprintCoverageClipped,
} from "./orderflow-stack.ts";

function bar(
  time: string,
  sell: string,
  buy: string,
  overrides: Partial<FootprintBarItem> = {},
): FootprintBarItem {
  const sellNumber = Number(sell);
  const buyNumber = Number(buy);
  return {
    open_time: time,
    open: "100",
    high: "101",
    low: "99",
    close: "100",
    delta: String(buyNumber - sellNumber),
    total: String(buyNumber + sellNumber),
    delta_notional: String((buyNumber - sellNumber) * 100),
    total_notional: String((buyNumber + sellNumber) * 100),
    poc: "100",
    vah: "100",
    val: "100",
    source: "archive",
    levels: [
      {
        price_bin: "100",
        sell,
        buy,
        sell_notional: String(sellNumber * 100),
        buy_notional: String(buyNumber * 100),
      },
    ],
    ...overrides,
  };
}

test("builds declared-side components without delta inference", () => {
  const made = buildOrderFlowStack(
    [bar("2026-08-11T00:00:00+00:00", "2", "3")],
    "5min",
    false,
  );
  assert.equal(made.validBars, 1);
  assert.equal(made.unclassifiedBars, 0);
  assert.deepEqual(made.data[0], {
    time: 1786406400,
    values: [2, 3],
    breakBefore: false,
  });
});

test("leaves mismatched or unknown side bars empty and breaks the next area", () => {
  const made = buildOrderFlowStack(
    [
      bar("2026-08-11T00:00:00+00:00", "2", "3"),
      bar("2026-08-11T00:05:00+00:00", "0", "0", {
        unknown_side_count: 1,
        unknown_side_size: "7",
        unknown_side_notional: "700",
      }),
      bar("2026-08-11T00:10:00+00:00", "1", "4"),
    ],
    "5min",
    false,
  );
  assert.equal(made.validBars, 2);
  assert.equal(made.unclassifiedBars, 1);
  assert.equal(made.gapBreaks, 1);
  assert.deepEqual(made.data[1], { time: 1786406700 });
  assert.deepEqual(made.data[2], {
    time: 1786407000,
    values: [1, 4],
    breakBefore: true,
  });
});

test("uses source notional fields and preserves explicit zero bars", () => {
  const made = buildOrderFlowStack(
    [
      bar("2026-08-11T00:00:00+00:00", "0", "0", {
        levels: [],
      }),
      bar("2026-08-11T00:05:00+00:00", "2", "3"),
    ],
    "5min",
    true,
  );
  assert.equal(made.explicitZeroBars, 1);
  assert.deepEqual(made.data[1], {
    time: 1786406700,
    values: [200, 300],
    breakBefore: false,
  });
});

test("breaks the area across a genuinely missing interval without adding zero", () => {
  const made = buildOrderFlowStack(
    [
      bar("2026-08-11T00:00:00+00:00", "2", "3"),
      bar("2026-08-11T00:10:00+00:00", "1", "4"),
    ],
    "5min",
    false,
  );
  assert.equal(made.data.length, 2);
  assert.equal(made.gapBreaks, 1);
  assert.deepEqual(made.data[1], {
    time: 1786407000,
    values: [1, 4],
    breakBefore: true,
  });
});

test("does not confuse missing coverage with explicit backend clipping", () => {
  assert.equal(
    footprintCoverageClipped({ truncated: false } as never),
    false,
  );
  assert.equal(
    footprintCoverageClipped({
      truncated: false,
      coverage_clipped: true,
    } as never),
    true,
  );
});
