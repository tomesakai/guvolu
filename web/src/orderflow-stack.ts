import type { Time, UTCTimestamp, WhitespaceData } from "lightweight-charts";
import type { FootprintBarItem, FootprintMeta } from "./api";
import type { StackedAreaData } from "./plugins/stacked-area/data";

const MS_PER_SECOND = 1000;

/** 买卖构成窗格可直接交给 lightweight-charts 的序列数据。 */
export type OrderFlowStackDatum =
  | StackedAreaData
  | WhitespaceData<Time>;

export interface OrderFlowStackView {
  readonly data: OrderFlowStackDatum[];
  readonly validBars: number;
  readonly unclassifiedBars: number;
  readonly explicitZeroBars: number;
  readonly gapBreaks: number;
}

export const EMPTY_ORDER_FLOW_STACK: OrderFlowStackView = {
  data: [],
  validBars: 0,
  unclassifiedBars: 0,
  explicitZeroBars: 0,
  gapBreaks: 0,
};

function epochOf(value: string): UTCTimestamp | null {
  const millis = Date.parse(value);
  if (!Number.isFinite(millis)) {
    return null;
  }
  return Math.floor(millis / MS_PER_SECOND) as UTCTimestamp;
}

function nonNegative(value: string): number | null {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : null;
}

function signed(value: string): number | null {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

/** 浮点仅用于画图；用相对容差核对服务端十进制汇总是否仍闭合。 */
function near(left: number, right: number): boolean {
  const scale = Math.max(1, Math.abs(left), Math.abs(right));
  return Math.abs(left - right) <= scale * 1e-9;
}

function intervalSeconds(interval: string): number | null {
  const matched = /^(\d+)(min|hour|day)$/.exec(interval);
  if (matched === null) {
    return null;
  }
  const amount = Number(matched[1]);
  const unit = matched[2];
  if (!Number.isInteger(amount) || amount <= 0 || unit === undefined) {
    return null;
  }
  const multiplier = unit === "min" ? 60 : unit === "hour" ? 3600 : 86400;
  return amount * multiplier;
}

interface Components {
  readonly sell: number;
  readonly buy: number;
  readonly explicitZero: boolean;
}

/**
 * 只接受可由明确 taker sell/buy 档汇总闭合的 bar。
 *
 * total/delta 只用于核对，绝不用 (total±delta)/2 反推缺失侧，
 * 因此 unknown/malformed side 会成为空档，不会被静默分配。
 */
function componentsOf(
  bar: FootprintBarItem,
  notional: boolean,
): Components | null {
  if ((bar.unknown_side_count ?? 0) > 0) {
    return null;
  }
  const total = nonNegative(notional ? bar.total_notional : bar.total);
  const delta = signed(notional ? bar.delta_notional : bar.delta);
  if (total === null || delta === null) {
    return null;
  }
  let sell = 0;
  let buy = 0;
  for (const level of bar.levels) {
    const sellPart = nonNegative(
      notional ? level.sell_notional : level.sell,
    );
    const buyPart = nonNegative(notional ? level.buy_notional : level.buy);
    if (sellPart === null || buyPart === null) {
      return null;
    }
    sell += sellPart;
    buy += buyPart;
  }
  if (!near(sell + buy, total) || !near(buy - sell, delta)) {
    return null;
  }
  return { sell, buy, explicitZero: total === 0 };
}

/**
 * 足迹 bar 转非负主动卖/主动买堆叠序列。
 *
 * 缺 bar、非法 bar 与时间跳跃都显式断线；不会前向填充或补零。
 */
export function buildOrderFlowStack(
  bars: readonly FootprintBarItem[],
  interval: string,
  notional: boolean,
): OrderFlowStackView {
  const data: OrderFlowStackDatum[] = [];
  const width = intervalSeconds(interval);
  let previous: number | null = null;
  let breakNext = false;
  let validBars = 0;
  let unclassifiedBars = 0;
  let explicitZeroBars = 0;
  let gapBreaks = 0;

  for (const bar of bars) {
    const time = epochOf(bar.open_time);
    if (time === null || (previous !== null && time <= previous)) {
      unclassifiedBars += 1;
      breakNext = true;
      continue;
    }
    const components = componentsOf(bar, notional);
    if (components === null) {
      data.push({ time });
      unclassifiedBars += 1;
      breakNext = true;
      previous = time;
      continue;
    }
    const timedGap =
      previous !== null && width !== null && time !== previous + width;
    const breakBefore = breakNext || timedGap;
    if (breakBefore && validBars > 0) {
      gapBreaks += 1;
    }
    data.push({
      time,
      values: [components.sell, components.buy],
      breakBefore,
    });
    validBars += 1;
    if (components.explicitZero) {
      explicitZeroBars += 1;
    }
    breakNext = false;
    previous = time;
  }

  return {
    data,
    validBars,
    unclassifiedBars,
    explicitZeroBars,
    gapBreaks,
  };
}

/** 后端显式裁剪标记优先；旧契约以 truncated 等价表达。 */
export function footprintCoverageClipped(meta: FootprintMeta | null): boolean {
  return meta?.coverage_clipped ?? meta?.truncated ?? false;
}
