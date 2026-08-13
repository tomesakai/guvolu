import type {
  OrderflowTileCellV2,
  OrderflowTileColumnV2,
} from "./api";

export interface OrderflowBookLevelV2 {
  readonly bookSide: "ask" | "bid";
  readonly priceKey: number;
  readonly price: string;
  readonly size: string;
}

export interface OrderflowBookStateV2 {
  readonly trusted: boolean;
  readonly levels: ReadonlyMap<string, OrderflowBookLevelV2>;
}

function levelKey(cell: OrderflowTileCellV2): string {
  return `${cell.book_side}|${String(cell.price_key)}`;
}

/**
 * 把一列稀疏 tile 应用到当前簿。
 * gap 清空可信状态；anchor 全量替换；trade-only cell 永不改写盘口。
 */
export function applyOrderflowColumnV2(
  previous: OrderflowBookStateV2,
  column: OrderflowTileColumnV2,
): OrderflowBookStateV2 {
  if (column.is_gap) {
    return { trusted: false, levels: new Map() };
  }
  const levels = column.is_anchor
    ? new Map<string, OrderflowBookLevelV2>()
    : new Map(previous.levels);
  const trusted = column.is_anchor || previous.trusted;
  if (!trusted) {
    return { trusted: false, levels };
  }
  for (const cell of column.cells) {
    if (cell.state_role === "trade") {
      continue;
    }
    const key = levelKey(cell);
    if (cell.book_end_size === null || cell.book_end_size === "0") {
      levels.delete(key);
      continue;
    }
    levels.set(key, {
      bookSide: cell.book_side,
      priceKey: cell.price_key,
      price: cell.price,
      size: cell.book_end_size,
    });
  }
  return { trusted: true, levels };
}

/** 上下文列参与重建但不进入用户请求区间。 */
export function visibleOrderflowColumnsV2(
  columns: readonly OrderflowTileColumnV2[],
): readonly OrderflowTileColumnV2[] {
  return columns.filter((column) => !column.context_only);
}
