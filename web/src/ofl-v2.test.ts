import assert from "node:assert/strict";
import test from "node:test";
import type { OrderflowTileColumnV2 } from "./api.ts";
import {
  applyOrderflowColumnV2,
  visibleOrderflowColumnsV2,
} from "./ofl-v2.ts";
import {
  reconcileOrderflowColumnsV2,
  replayOrderflowColumnsV2,
} from "./ofl-replay.ts";

function column(
  patch: Partial<OrderflowTileColumnV2>,
): OrderflowTileColumnV2 {
  return {
    column_id: "c", bucket_epoch: 0, bucket_start: "2026-08-01T00:00:00Z",
    bucket_end: "2026-08-01T00:00:05Z", coverage_state: "ok",
    is_anchor: false, is_reset: false, is_carried: false, is_gap: false,
    context_only: false, frame_count: 1, trade_count: 0,
    last_event_time: null, last_available_time: null, integrity_mode: "test",
    source_generation: "g", method_version: "v4", row_size: "2",
    price_quantum_basis: "instrument_map_tick_size", cells: [], ...patch,
  };
}

test("anchor, sparse change, trade-only and gap preserve contract", () => {
  let state = applyOrderflowColumnV2(
    { trusted: false, levels: new Map() },
    column({
      is_anchor: true, context_only: true,
      cells: [{
        book_side: "ask", price_key: 50, price: "100", row_size: "2",
        price_quantum_basis: "instrument_map_tick_size", book_end_size: "3",
        net_increase: "0", net_decrease_unknown: "0", taker_buy_size: "0",
        taker_sell_size: "0", state_role: "anchor",
      }],
    }),
  );
  assert.equal(state.trusted, true);
  assert.equal(state.levels.get("ask|50")?.size, "3");

  state = applyOrderflowColumnV2(state, column({
    cells: [{
      book_side: "ask", price_key: 50, price: "100", row_size: "2",
      price_quantum_basis: "instrument_map_tick_size", book_end_size: "3",
      net_increase: "0", net_decrease_unknown: "0", taker_buy_size: "1",
      taker_sell_size: "0", state_role: "trade",
    }],
  }));
  assert.equal(state.levels.get("ask|50")?.size, "3");

  state = applyOrderflowColumnV2(state, column({
    cells: [{
      book_side: "ask", price_key: 50, price: "100", row_size: "2",
      price_quantum_basis: "instrument_map_tick_size", book_end_size: null,
      net_increase: "0", net_decrease_unknown: "3", taker_buy_size: "0",
      taker_sell_size: "0", state_role: "change",
    }],
  }));
  assert.equal(state.levels.size, 0);

  state = applyOrderflowColumnV2(state, column({ is_gap: true }));
  assert.equal(state.trusted, false);
  assert.equal(state.levels.size, 0);
});

test("context columns are hidden only after reconstruction", () => {
  assert.equal(visibleOrderflowColumnsV2([
    column({ context_only: true }), column({ context_only: false }),
  ]).length, 1);
});

test("method or row contract change cannot continue sparse replay", () => {
  const replayed = replayOrderflowColumnsV2([
    column({
      column_id: "anchor-v6", bucket_epoch: 0, context_only: false,
      method_version: "v6", row_size: "2", is_anchor: true,
      cells: [{
        book_side: "ask", price_key: 50, price: "100", row_size: "2",
        price_quantum_basis: "instrument_map_tick_size", book_end_size: "3",
        net_increase: "0", net_decrease_unknown: "0", taker_buy_size: "0",
        taker_sell_size: "0", state_role: "anchor",
      }],
    }),
    column({
      column_id: "unsafe-v7", bucket_epoch: 5,
      method_version: "v7", row_size: "2",
      cells: [{
        book_side: "ask", price_key: 50, price: "100", row_size: "2",
        price_quantum_basis: "instrument_map_tick_size", book_end_size: "4",
        net_increase: "1", net_decrease_unknown: "0", taker_buy_size: "0",
        taker_sell_size: "0", state_role: "change",
      }],
    }),
    column({
      column_id: "anchor-v7", bucket_epoch: 10,
      method_version: "v7", row_size: "4", is_anchor: true,
      cells: [{
        book_side: "ask", price_key: 25, price: "100", row_size: "4",
        price_quantum_basis: "instrument_map_tick_size", book_end_size: "5",
        net_increase: "0", net_decrease_unknown: "0", taker_buy_size: "0",
        taker_sell_size: "0", state_role: "anchor",
      }],
    }),
  ]);

  assert.equal(replayed[1]?.gap, true);
  assert.equal(replayed[1]?.cells.length, 0);
  assert.equal(replayed[1]?.contract_break, true);
  assert.equal(replayed[2]?.gap, false);
  assert.equal(replayed[2]?.reset, true);
  assert.equal(replayed[2]?.contract_break, true);
  assert.equal(replayed[2]?.cells[0]?.[2], "5");
});

test("chunk assembly breaks only incompatible column contracts", () => {
  const [v6] = replayOrderflowColumnsV2([
    column({
      column_id: "v6", bucket_epoch: 0, method_version: "v6",
      row_size: "2", is_anchor: true,
    }),
  ]);
  const [v7] = replayOrderflowColumnsV2([
    column({
      column_id: "v7", bucket_epoch: 5, method_version: "v7",
      row_size: "2", is_anchor: true, is_carried: true,
    }),
  ]);
  assert.ok(v6 !== undefined && v7 !== undefined);

  const mixed = reconcileOrderflowColumnsV2([v6, v7]);
  assert.deepEqual(mixed.methodVersions, ["v6", "v7"]);
  assert.equal(mixed.contractBreaks, 1);
  assert.equal(mixed.columns[1]?.reset, true);
  assert.equal(mixed.columns[1]?.carried, false);

  const sameContract = reconcileOrderflowColumnsV2([
    v6, { ...v6, e: 5 },
  ]);
  assert.equal(sameContract.contractBreaks, 0);
  assert.equal(sameContract.columns[1]?.reset, false);
});
