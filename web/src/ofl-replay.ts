import type {
  OrderflowTileCellV2,
  OrderflowTileColumnV2,
  TileBands,
  TileColumn,
} from "./api.ts";
import {
  applyOrderflowColumnV2,
  visibleOrderflowColumnsV2,
} from "./ofl-v2.ts";
import type { OrderflowBookStateV2 } from "./ofl-v2.ts";

function decimal(value: string | null | undefined): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function cellKey(cell: OrderflowTileCellV2): string {
  return `${cell.book_side}|${String(cell.price_key)}`;
}

function orderflowContractKey(
  column: Pick<OrderflowTileColumnV2, "method_version" | "row_size">,
): string {
  return `${column.method_version}\u0000${column.row_size ?? "<null>"}`;
}

function replayedContractKey(column: TileColumn): string | null {
  if (column.method_version === undefined) {
    return null;
  }
  return `${column.method_version}\u0000${column.row_size ?? "<null>"}`;
}

function bandValues(
  column: OrderflowTileColumnV2,
  state: OrderflowBookStateV2,
): { readonly mid: string | null; readonly bands: TileBands } {
  let bestBid = Number.NEGATIVE_INFINITY;
  let bestAsk = Number.POSITIVE_INFINITY;
  let bidDepth = 0;
  let askDepth = 0;
  for (const level of state.levels.values()) {
    const price = decimal(level.price);
    const size = decimal(level.size);
    if (level.bookSide === "bid") {
      bestBid = Math.max(bestBid, price);
      bidDepth += size;
    } else {
      bestAsk = Math.min(bestAsk, price);
      askDepth += size;
    }
  }
  const hasMid = Number.isFinite(bestBid) && Number.isFinite(bestAsk);
  const mid = hasMid ? (bestBid + bestAsk) / 2 : null;
  let ofi = 0;
  let buy = 0;
  let sell = 0;
  for (const cell of column.cells) {
    const direction = cell.book_side === "bid" ? 1 : -1;
    ofi += direction *
      (decimal(cell.net_increase) - decimal(cell.net_decrease_unknown));
    buy += decimal(cell.taker_buy_size);
    sell += decimal(cell.taker_sell_size);
  }
  const depth = [5, 10, 25].map((bp) => {
    if (mid === null || mid <= 0) {
      return [String(bp), "0", true, "0"] as const;
    }
    const reach = mid * bp / 10000;
    let size = 0;
    let notional = 0;
    for (const level of state.levels.values()) {
      const price = decimal(level.price);
      if (Math.abs(price - mid) <= reach) {
        const quantity = decimal(level.size);
        size += quantity;
        notional += quantity * price;
      }
    }
    return [String(bp), String(size), true, String(notional)] as const;
  });
  return {
    mid: mid === null ? null : String(mid),
    bands: {
      spread_bp:
        mid === null || mid <= 0
          ? null
          : String(((bestAsk - bestBid) / mid) * 10000),
      ofi: String(ofi),
      imbalance:
        bidDepth + askDepth <= 0
          ? null
          : String((bidDepth - askDepth) / (bidDepth + askDepth)),
      trade_delta: String(buy - sell),
      trade_delta_notional: String(
        column.cells.reduce(
          (total, cell) =>
            total +
            decimal(cell.price) *
              (decimal(cell.taker_buy_size) - decimal(cell.taker_sell_size)),
          0,
        ),
      ),
      depth,
    },
  };
}

/** v2 sparse columns replayed only within one method and row-grid contract. */
export function replayOrderflowColumnsV2(
  source: readonly OrderflowTileColumnV2[],
): readonly TileColumn[] {
  let state: OrderflowBookStateV2 = { trusted: false, levels: new Map() };
  const made = new Map<string, TileColumn>();
  let previousContract: string | null = null;
  for (const column of source) {
    const contract = orderflowContractKey(column);
    const contractBreak =
      previousContract !== null && previousContract !== contract;
    if (contractBreak) {
      // 新契约重置状态。
      state = { trusted: false, levels: new Map() };
    }
    state = applyOrderflowColumnV2(state, column);
    previousContract = contract;
    if (column.context_only) {
      continue;
    }
    const changes = new Map(column.cells.map((cell) => [cellKey(cell), cell]));
    const cells: string[][] = [];
    if (state.trusted) {
      for (const [key, level] of state.levels) {
        const change = changes.get(key);
        cells.push([
          level.price,
          level.bookSide,
          level.size,
          change?.net_increase ?? "0",
          change?.net_decrease_unknown ?? "0",
          String(
            decimal(change?.taker_buy_size) +
              decimal(change?.taker_sell_size),
          ),
        ]);
      }
    }
    const summary = bandValues(column, state);
    made.set(column.column_id, {
      t: column.bucket_start,
      e: column.bucket_epoch,
      gap: column.is_gap || !state.trusted,
      carried: column.is_carried && !contractBreak,
      reset: column.is_reset || (contractBreak && state.trusted),
      method_version: column.method_version,
      row_size: column.row_size,
      contract_break: contractBreak,
      frames: column.frame_count,
      mid: summary.mid,
      cells,
      bands: summary.bands,
    });
  }
  const visible = new Set(
    visibleOrderflowColumnsV2(source).map((column) => column.column_id),
  );
  return [...made.entries()]
    .filter(([columnId]) => visible.has(columnId))
    .map(([, column]) => column)
    .sort((left, right) => left.e - right.e);
}

export interface OrderflowColumnAssemblyV2 {
  readonly columns: readonly TileColumn[];
  readonly methodVersions: readonly string[];
  readonly rowSizes: readonly string[];
  readonly contractBreaks: number;
}

/**
 * Reconcile independently replayed chunks. Non-overlapping head generations
 * are intentionally irrelevant; only the per-column contract controls state.
 */
export function reconcileOrderflowColumnsV2(
  source: readonly TileColumn[],
): OrderflowColumnAssemblyV2 {
  const ordered = [...source].sort((left, right) => left.e - right.e);
  const columns: TileColumn[] = [];
  const methodVersions = new Set<string>();
  const rowSizes = new Set<string>();
  let previousContract: string | null = null;
  let contractBreaks = 0;
  for (const column of ordered) {
    const contract = replayedContractKey(column);
    if (column.method_version !== undefined) {
      methodVersions.add(column.method_version);
    }
    if (column.row_size !== undefined && column.row_size !== null) {
      rowSizes.add(column.row_size);
    }
    const crossed =
      contract !== null &&
      previousContract !== null &&
      contract !== previousContract;
    const contractBreak = column.contract_break === true || crossed;
    const made =
      crossed && column.contract_break !== true
        ? {
            ...column,
            carried: false,
            reset: column.gap ? column.reset : true,
            contract_break: true,
          }
        : column;
    if (contractBreak) {
      contractBreaks += 1;
    }
    columns.push(made);
    if (contract !== null) {
      previousContract = contract;
    }
  }
  return {
    columns,
    methodVersions: [...methodVersions],
    rowSizes: [...rowSizes],
    contractBreaks,
  };
}
