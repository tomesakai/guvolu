import assert from "node:assert/strict";
import test from "node:test";

import { uncoveredRanges } from "../web/src/ofl-ranges.ts";

test("OFL 双层隐式缺列只返回未覆盖子段", () => {
  assert.deepEqual(
    uncoveredRanges(0, 60, [
      [40, 50],
      [10, 30],
      [25, 40],
    ]),
    [
      [0, 10],
      [50, 60],
    ],
  );
});

test("OFL 相邻底层列构成完整覆盖", () => {
  assert.deepEqual(
    uncoveredRanges(0, 60, [
      [0, 20],
      [20, 60],
    ]),
    [],
  );
});
