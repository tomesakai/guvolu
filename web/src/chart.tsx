import { memo, useEffect, useMemo, useRef } from "react";
import type { ReactElement } from "react";
import { LineStyle, PriceScaleMode } from "lightweight-charts";
import type {
  AreaData,
  CandlestickData,
  HistogramData,
  IChartApi,
  IPriceLine,
  IRange,
  ISeriesApi,
  ISeriesMarkersPluginApi,
  LineData,
  MouseEventParams,
  SeriesMarker,
  SeriesType,
  Time,
  UTCTimestamp,
  WhitespaceData,
} from "lightweight-charts";
import type {
  FootprintBarItem,
  FootprintResponse,
  KlineItem,
  OrderbookLevel,
  OrderbookResponse,
} from "./api";
import type {
  DecimalFormat,
  FootprintSeriesApi,
  RgbaChannels,
  StackedAreaSeriesApi,
} from "./lwc";
import {
  INTENSITY_STEPS,
  addArea,
  addBars,
  addBaseline,
  addCandles,
  addFootprint,
  addLine,
  addStackedArea,
  addVolume,
  applyPaneRatio,
  attachMarkers,
  channelFill,
  chartChannels,
  chartColor,
  createThemedChart,
} from "./lwc";
import type {
  FootprintBarDatum,
  FootprintLevelDatum,
  FootprintSessionDatum,
} from "./plugins/footprint/data";
import {
  EMPTY_ORDER_FLOW_STACK,
  buildOrderFlowStack,
} from "./orderflow-stack";
import type { OrderFlowStackView } from "./orderflow-stack";
import {
  axisPriceText,
  axisTotalText,
  BASIS_NOTIONAL,
  NOTIONAL_STEP,
  decimalPlaces,
  jstStamp,
  parseUtcIso,
  priceDiffText,
  priceText,
  plotNumber,
  rawText,
  sizeText,
  totalText,
} from "./format";
import type { FormatSwitches } from "./format";

// 副窗格占比，开 CVD 时三分
const VOLUME_PANE_RATIO = 0.28;
const VOLUME_PANE_RATIO_SPLIT = 0.2;
const CVD_PANE_RATIO = 0.16;
const VOLUME_PANE_INDEX = 1;
const CVD_PANE_INDEX = 2;
const MAIN_PANE_INDEX = 0;
const MS_PER_SECOND = 1000;
const HALF = 2;
const TOOLTIP_GAP = 5;
// 提示框六行：时刻与五项行情字段
const TIP_LABELS = ["时刻", "open", "high", "low", "close", "量"];
// 足迹提示汇总行
const FOOT_TIP_LABELS = ["时刻", "delta", "合计"];
// 极值开时附加两行：距离入放大镜
const EXT_TIP_LABELS = ["距会话高", "距会话低"];
// 轴投影刻线边距与最短长度像素
const AXIS_TICK_PAD = 2;
const AXIS_TICK_MIN = 3;
// 交易日界偏移秒（JST 06:00）
const SESSION_SHIFT_SECONDS = 10800;
const DAY_SECONDS = 86400;
// 量分位阶梯切点，档数与强度阶梯对齐
const QUANTILE_RATIOS: readonly number[] = [0.3, 0.5, 0.7, 0.85, 0.95];
// 价值区覆盖率，惯例常数
const VALUE_AREA_RATIO = 0.7;

/** 无标记的共用空数组，避免每次渲染新建。 */
export const NO_MARKERS: readonly SeriesMarker<Time>[] = [];

export interface ChartKindOption {
  readonly key: string;
  readonly label: string;
}

export interface ChartKindGroup {
  readonly label: string;
  readonly options: readonly ChartKindOption[];
}

export const FOOTPRINT_KIND = "footprint";

// 主图五型分组供设置页下拉。
export const PRICE_CHART_KIND_GROUPS: readonly ChartKindGroup[] = [
  {
    label: "OHLC",
    options: [
      { key: "candles", label: "蜡烛" },
      { key: "bars", label: "美国线" },
    ],
  },
  {
    label: "单值",
    options: [
      { key: "line", label: "线" },
      { key: "area", label: "面积" },
      { key: "baseline", label: "基线" },
    ],
  },
];

export const DEFAULT_CHART_KIND = "candles";
const OHLC_KINDS: readonly string[] = ["candles", "bars"];

// 足迹副窗格口径二选一
export const FOOT_SUB_VOLUME = "volume";
export const FOOT_SUB_DELTA = "delta";
export const FOOT_SUB_STACKED = "stacked";

export type ChartNavigationAction = "latest" | "fit";

export interface ChartNavigationCommand {
  readonly id: number;
  readonly action: ChartNavigationAction;
}

function toEpoch(iso: string): UTCTimestamp | null {
  const date = parseUtcIso(iso);
  if (date === null) {
    return null;
  }
  return Math.floor(date.getTime() / MS_PER_SECOND) as UTCTimestamp;
}

/** 由品种规则推出显示精度，非法取值返回空。 */
function decimalFormat(step: string): DecimalFormat | null {
  const trimmed = step.trim();
  if (trimmed === "") {
    return null;
  }
  const minMove = Number(trimmed);
  if (!Number.isFinite(minMove) || minMove <= 0) {
    return null;
  }
  return { precision: decimalPlaces(trimmed), minMove, raw: trimmed };
}

interface ChartData {
  readonly candles: CandlestickData<Time>[];
  readonly values: LineData<Time>[];
  readonly volumes: HistogramData<Time>[];
  readonly index: Map<number, KlineItem>;
  readonly baseValue: number | null;
}

const EMPTY_DATA: ChartData = {
  candles: [],
  values: [],
  volumes: [],
  index: new Map<number, KlineItem>(),
  baseValue: null,
};

interface FootprintView {
  readonly bars: FootprintBarDatum[];
  readonly volumes: HistogramData<Time>[];
  readonly deltas: HistogramData<Time>[];
  readonly cvd: (AreaData<Time> | WhitespaceData<Time>)[];
  readonly sessions: FootprintSessionDatum[];
  readonly index: Map<number, FootprintBarItem>;
  readonly binSize: number;
}

const EMPTY_FOOT: FootprintView = {
  bars: [],
  volumes: [],
  deltas: [],
  cvd: [],
  sessions: [],
  index: new Map<number, FootprintBarItem>(),
  binSize: 1,
};

/** 量分位阶梯切点：对合并样本取分位值。 */
function quantileCuts(values: readonly number[]): number[] {
  if (values.length === 0) {
    return [];
  }
  const sorted = [...values].sort((left, right) => left - right);
  return QUANTILE_RATIOS.map(
    (ratio) =>
      sorted[Math.min(sorted.length - 1, Math.floor(ratio * sorted.length))] ??
      0,
  );
}

/** 值落入的阶梯档序，切点即档界。 */
function stepOf(value: number, cuts: readonly number[]): number {
  let step = 0;
  for (const cut of cuts) {
    if (value >= cut) {
      step += 1;
    }
  }
  return Math.min(step, INTENSITY_STEPS - 1);
}

/** bar 归属交易日键（JST 06:00 界）。 */
function sessionKeyOf(epoch: number): number {
  return Math.floor((epoch + SESSION_SHIFT_SECONDS) / DAY_SECONDS);
}

interface ExtremeMark {
  readonly value: number;
  readonly text: string;
}

interface SessionExtremes {
  readonly high: ExtremeMark;
  readonly low: ExtremeMark;
}

/** 当前会话高低点，锚定交易日界。 */
function sessionExtremesOf(
  items: readonly KlineItem[],
): SessionExtremes | null {
  const nowKey = sessionKeyOf(Math.floor(Date.now() / MS_PER_SECOND));
  let high: ExtremeMark | null = null;
  let low: ExtremeMark | null = null;
  for (const item of items) {
    const time = toEpoch(item.open_time);
    if (time === null || sessionKeyOf(time) !== nowKey) {
      continue;
    }
    const highNum = plotNumber(item.high);
    const lowNum = plotNumber(item.low);
    if (highNum === null || lowNum === null) {
      continue;
    }
    if (high === null || highNum > high.value) {
      high = { value: highNum, text: item.high };
    }
    if (low === null || lowNum < low.value) {
      low = { value: lowNum, text: item.low };
    }
  }
  return high === null || low === null ? null : { high, low };
}

/** 不大于该值的样本占比，样本须升序。 */
function rankRatio(sorted: readonly number[], value: number): number {
  let low = 0;
  let high = sorted.length;
  while (low < high) {
    const middle = (low + high) >> 1;
    if ((sorted[middle] ?? 0) <= value) {
      low = middle + 1;
    } else {
      high = middle;
    }
  }
  return sorted.length === 0 ? 0 : low / sorted.length;
}

/** 会话剖面：合并档量后求 POC 与价值区边界。 */
function sessionProfile(
  totals: ReadonlyMap<number, number>,
): { poc: number | null; vah: number | null; val: number | null } {
  const levels = [...totals.entries()].sort((left, right) => left[0] - right[0]);
  if (levels.length === 0) {
    return { poc: null, vah: null, val: null };
  }
  let pocAt = 0;
  let total = 0;
  levels.forEach(([, size], at) => {
    total += size;
    if (size > (levels[pocAt]?.[1] ?? 0)) {
      pocAt = at;
    }
  });
  const target = total * VALUE_AREA_RATIO;
  let covered = levels[pocAt]?.[1] ?? 0;
  let lowAt = pocAt;
  let highAt = pocAt;
  while (covered < target) {
    const below = lowAt > 0 ? (levels[lowAt - 1]?.[1] ?? 0) : null;
    const above =
      highAt + 1 < levels.length ? (levels[highAt + 1]?.[1] ?? 0) : null;
    if (below === null && above === null) {
      break;
    }
    if (below === null || (above !== null && above >= below)) {
      highAt += 1;
      covered += levels[highAt]?.[1] ?? 0;
    } else {
      lowAt -= 1;
      covered += levels[lowAt]?.[1] ?? 0;
    }
  }
  return {
    poc: levels[pocAt]?.[0] ?? null,
    vah: levels[highAt]?.[0] ?? null,
    val: levels[lowAt]?.[0] ?? null,
  };
}

interface ParsedFootLevel {
  readonly price: number;
  readonly sell: number;
  readonly buy: number;
  readonly sellShow: number;
  readonly buyShow: number;
  readonly sellText: string;
  readonly buyText: string;
}

interface ParsedFootBar {
  readonly time: UTCTimestamp;
  readonly item: FootprintBarItem;
  readonly open: number;
  readonly high: number;
  readonly low: number;
  readonly close: number;
  readonly delta: number;
  readonly total: number;
  readonly levels: readonly ParsedFootLevel[];
}

/** 解析足迹响应为数值形态，时序必须严格递增。

金额基准下格值、Delta 与合计取服务端逐笔精确
累计字段（细则 17），前端零换算；文本走量变换。
*/
function parseFootBars(
  items: readonly FootprintBarItem[],
  notional: boolean,
): ParsedFootBar[] {
  const out: ParsedFootBar[] = [];
  let last = Number.NEGATIVE_INFINITY;
  for (const item of items) {
    const time = toEpoch(item.open_time);
    const open = plotNumber(item.open);
    const high = plotNumber(item.high);
    const low = plotNumber(item.low);
    const close = plotNumber(item.close);
    const delta = plotNumber(notional ? item.delta_notional : item.delta);
    const total = plotNumber(notional ? item.total_notional : item.total);
    if (
      time === null ||
      open === null ||
      high === null ||
      low === null ||
      close === null ||
      delta === null ||
      total === null ||
      time <= last
    ) {
      continue;
    }
    out.push({
      time,
      item,
      open,
      high,
      low,
      close,
      delta,
      total,
      levels: item.levels.flatMap((level) => {
        const price = plotNumber(level.price_bin);
        const sell = plotNumber(level.sell);
        const buy = plotNumber(level.buy);
        const sellShow = notional
          ? plotNumber(level.sell_notional)
          : sell;
        const buyShow = notional ? plotNumber(level.buy_notional) : buy;
        if (
          price === null ||
          sell === null ||
          buy === null ||
          sellShow === null ||
          buyShow === null
        ) {
          return [];
        }
        return [
          {
            price,
            sell,
            buy,
            sellShow,
            buyShow,
            sellText: notional
              ? totalText(level.sell_notional, NOTIONAL_STEP)
              : level.sell,
            buyText: notional
              ? totalText(level.buy_notional, NOTIONAL_STEP)
              : level.buy,
          },
        ];
      }),
    });
    last = time;
  }
  return out;
}

/** 足迹响应转图层数据：分位阶梯、会话派生、窗格序列。 */
function toFootprintView(
  data: FootprintResponse,
  riseSoft: string,
  fallSoft: string,
  notional: boolean,
): FootprintView {
  const parsed = parseFootBars(data.bars, notional);
  const binSize = plotNumber(data.meta.bin) ?? 1;
  const sideValues: number[] = [];
  const deltaValues: number[] = [];
  for (const bar of parsed) {
    deltaValues.push(Math.abs(bar.delta));
    for (const level of bar.levels) {
      if (level.sellShow > 0) {
        sideValues.push(level.sellShow);
      }
      if (level.buyShow > 0) {
        sideValues.push(level.buyShow);
      }
    }
  }
  const sideCuts = quantileCuts(sideValues);
  const deltaCuts = quantileCuts(deltaValues);
  const bars: FootprintBarDatum[] = [];
  const volumes: HistogramData<Time>[] = [];
  const deltas: HistogramData<Time>[] = [];
  const cvd: (AreaData<Time> | WhitespaceData<Time>)[] = [];
  const sessions: FootprintSessionDatum[] = [];
  const index = new Map<number, FootprintBarItem>();
  let sessionKey: number | null = null;
  let sessionTotals = new Map<number, number>();
  let sessionStart = 0;
  let cum = 0;
  const closeSession = (endIndex: number): void => {
    if (sessionKey === null) {
      return;
    }
    const profile = sessionProfile(sessionTotals);
    sessions.push({
      startIndex: sessionStart,
      endIndex,
      poc: profile.poc,
      vah: profile.vah,
      val: profile.val,
      nakedEndIndex: null,
    });
  };
  parsed.forEach((bar, at) => {
    const key = sessionKeyOf(bar.time);
    if (key !== sessionKey) {
      closeSession(at - 1);
      sessionKey = key;
      sessionTotals = new Map<number, number>();
      sessionStart = at;
      cum = 0;
      // 会话首根置空档，跨日断开
      cvd.push({ time: bar.time });
    } else {
      cvd.push({ time: bar.time, value: cum + bar.delta });
    }
    cum += bar.delta;
    for (const level of bar.levels) {
      sessionTotals.set(
        level.price,
        (sessionTotals.get(level.price) ?? 0) + level.sell + level.buy,
      );
    }
    const levels: FootprintLevelDatum[] = bar.levels.map((level) => ({
      price: level.price,
      sellText: level.sellText,
      buyText: level.buyText,
      sellStep: level.sellShow > 0 ? stepOf(level.sellShow, sideCuts) : -1,
      buyStep: level.buyShow > 0 ? stepOf(level.buyShow, sideCuts) : -1,
    }));
    bars.push({
      time: bar.time,
      open: bar.open,
      high: bar.high,
      low: bar.low,
      close: bar.close,
      deltaRise: bar.delta >= 0,
      deltaStep: stepOf(Math.abs(bar.delta), deltaCuts),
      levels,
    });
    volumes.push({
      time: bar.time,
      value: bar.total,
      color: bar.close >= bar.open ? riseSoft : fallSoft,
    });
    deltas.push({
      time: bar.time,
      value: bar.delta,
      color: bar.delta >= 0 ? riseSoft : fallSoft,
    });
    index.set(bar.time, bar.item);
  });
  closeSession(parsed.length - 1);
  // 裸 POC：向后扫描首个触及 bar
  const naked = sessions.map((session) => {
    if (session.poc === null) {
      return session;
    }
    for (let at = session.endIndex + 1; at < parsed.length; at += 1) {
      const bar = parsed[at];
      if (bar !== undefined && bar.low <= session.poc && session.poc <= bar.high) {
        return { ...session, nakedEndIndex: at };
      }
    }
    return session;
  });
  return { bars, volumes, deltas, cvd, sessions: naked, index, binSize };
}

/** 建当前图型的主系列，取色全部走 token。 */
function createMain(
  chart: IChartApi,
  kind: string,
  tickSize: string,
  baseValue: number,
): ISeriesApi<SeriesType> {
  if (kind === "bars") {
    return addBars(chart, MAIN_PANE_INDEX);
  }
  if (kind === "line") {
    return addLine(chart, MAIN_PANE_INDEX);
  }
  if (kind === "area") {
    return addArea(chart, MAIN_PANE_INDEX);
  }
  if (kind === "baseline") {
    return addBaseline(chart, MAIN_PANE_INDEX, baseValue);
  }
  if (kind === FOOTPRINT_KIND) {
    return addFootprint(chart, MAIN_PANE_INDEX, decimalFormat(tickSize));
  }
  return addCandles(chart, MAIN_PANE_INDEX, decimalFormat(tickSize));
}

/** 足迹系列的类型细化，仅足迹型下调用。 */
function footApi(series: ISeriesApi<SeriesType>): FootprintSeriesApi {
  return series as unknown as FootprintSeriesApi;
}

/** 主系列共通项：最新价标签与虚线价位线为唯一常驻指示物。 */
function applyMainOptions(
  series: ISeriesApi<SeriesType>,
  tickSize: string,
): void {
  const format = decimalFormat(tickSize);
  series.applyOptions({
    lastValueVisible: true,
    priceLineVisible: true,
    priceLineColor: chartColor("--border-focus"),
    priceLineStyle: LineStyle.Dashed,
    priceLineWidth: 1,
  });
  if (format !== null) {
    series.applyOptions({
      priceFormat: {
        type: "custom",
        minMove: format.minMove,
        formatter: (price: number) => axisPriceText(price, format.raw),
      },
    });
  }
}

/** 按图型装填主系列：足迹走档阵列，两型 OHLC，其余收盘单值。 */
function fillMain(
  series: ISeriesApi<SeriesType>,
  kind: string,
  data: ChartData,
  foot: FootprintView,
): void {
  if (kind === FOOTPRINT_KIND) {
    (series as ISeriesApi<"Custom">).setData(foot.bars);
    return;
  }
  if (OHLC_KINDS.includes(kind)) {
    series.setData(data.candles);
    return;
  }
  series.setData(data.values);
}

function toChartData(
  items: KlineItem[],
  riseSoft: string,
  fallSoft: string,
): ChartData {
  const candles: CandlestickData<Time>[] = [];
  const values: LineData<Time>[] = [];
  const volumes: HistogramData<Time>[] = [];
  const index = new Map<number, KlineItem>();
  let baseValue: number | null = null;
  let last = Number.NEGATIVE_INFINITY;
  for (const item of items) {
    const time = toEpoch(item.open_time);
    const open = plotNumber(item.open);
    const high = plotNumber(item.high);
    const low = plotNumber(item.low);
    const close = plotNumber(item.close);
    if (
      time === null ||
      open === null ||
      high === null ||
      low === null ||
      close === null ||
      time <= last
    ) {
      // 时序必须严格递增
      continue;
    }
    candles.push({ time, open, high, low, close });
    values.push({ time, value: close });
    if (baseValue === null) {
      // 基线基准取本段首根收盘
      baseValue = close;
    }
    const volume = plotNumber(item.volume);
    if (volume !== null) {
      volumes.push({
        time,
        value: volume,
        color: close >= open ? riseSoft : fallSoft,
      });
    }
    index.set(time, item);
    last = time;
  }
  return { candles, values, volumes, index, baseValue };
}

export interface KlineChartProps {
  items: KlineItem[];
  footprint: FootprintResponse | null;
  viewKey: string;
  kind: string;
  subKind: string;
  showCvd: boolean;
  showPoc: boolean;
  showVa: boolean;
  showExtremes: boolean;
  showBookAxis: boolean;
  orderbook: OrderbookResponse | null;
  logScale: boolean;
  markers: readonly SeriesMarker<Time>[];
  tickSize: string;
  sizeStep: string;
  display: FormatSwitches;
  footprintPending: boolean;
  footprintStale: boolean;
  footprintCoverageClipped: boolean;
  navigationCommand: ChartNavigationCommand | null;
  onAwayFromLatestChange: (away: boolean) => void;
}

interface StackLegendProps {
  readonly visible: boolean;
  readonly symbol: string | null;
  readonly notional: boolean;
  readonly pending: boolean;
  readonly stale: boolean;
  readonly coverageClipped: boolean;
  readonly view: OrderFlowStackView;
  readonly sideBasis: string | null;
}

function sideBasisText(value: string | null): string {
  if (value === "by_bar_source:archive_taker|live_tick_rule_inference") {
    return "归档 taker / 实时 tick-rule";
  }
  return value ?? "side 未声明";
}

/** 堆叠窗格的身份、单位与质量状态，颜色之外仍有文字语义。 */
function StackLegend({
  visible,
  symbol,
  notional,
  pending,
  stale,
  coverageClipped,
  view,
  sideBasis,
}: StackLegendProps): ReactElement | null {
  if (!visible) {
    return null;
  }
  const states: string[] = [];
  if (pending) {
    states.push(view.validBars > 0 ? "更新中·旧值保留" : "读取中");
  }
  if (stale) {
    states.push("陈旧");
  }
  if (coverageClipped) {
    states.push("覆盖已裁剪");
  }
  if (view.unclassifiedBars > 0) {
    states.push(`未分类 ${String(view.unclassifiedBars)} 根留空`);
  }
  if (view.gapBreaks > 0) {
    states.push(`断点 ${String(view.gapBreaks)}`);
  }
  if (view.validBars === 0 && !pending) {
    states.push("无可分类逐笔");
  }
  return (
    <div className="flow-stack-legend" role="status" aria-live="polite">
      <span className="flow-stack-legend__item">
        <i className="flow-stack-legend__swatch flow-stack-legend__swatch--sell" />
        主动卖
      </span>
      <span className="flow-stack-legend__item">
        <i className="flow-stack-legend__swatch flow-stack-legend__swatch--buy" />
        主动买
      </span>
      <span className="flow-stack-legend__unit">
        {notional ? "JPY/周期" : `${symbol ?? "数量"}/周期`}
      </span>
      <span className="flow-stack-legend__basis">
        {sideBasisText(sideBasis)} · 不补零
      </span>
      {states.length > 0 ? (
        <span className="flow-stack-legend__state">{states.join(" · ")}</span>
      ) : null}
    </div>
  );
}

/**
 * K 线图：主窗格六型可切，副窗格量、Delta 或买卖构成，CVD 可关。
 * 足迹模式内一切量为撮合口径，绝不显示官方 K 线量。
 * 派生层开关：会话极值虚线（距离只入放大镜）、盘口轴投影。
 */
function KlineChartImpl({
  items,
  footprint,
  viewKey,
  kind,
  subKind,
  showCvd,
  showPoc,
  showVa,
  showExtremes,
  showBookAxis,
  orderbook,
  logScale,
  markers,
  tickSize,
  sizeStep,
  display,
  footprintPending,
  footprintStale,
  footprintCoverageClipped,
  navigationCommand,
  onAwayFromLatestChange,
}: KlineChartProps): ReactElement {
  const stackView = useMemo<OrderFlowStackView>(
    () =>
      footprint === null
        ? EMPTY_ORDER_FLOW_STACK
        : buildOrderFlowStack(
            footprint.bars,
            footprint.meta.interval,
            display.valueBasis === BASIS_NOTIONAL,
          ),
    [footprint, display.valueBasis],
  );
  const holder = useRef<HTMLDivElement | null>(null);
  const tip = useRef<HTMLDivElement | null>(null);
  const cells = useRef<HTMLSpanElement[]>([]);
  const footTipLevels = useRef<HTMLDivElement | null>(null);
  const footTipItem = useRef<FootprintBarItem | null>(null);
  const pinnedTime = useRef<number | null>(null);
  const tipMode = useRef<string>("");
  const chart = useRef<IChartApi | null>(null);
  const main = useRef<ISeriesApi<SeriesType> | null>(null);
  const volume = useRef<ISeriesApi<"Histogram"> | null>(null);
  const stacked = useRef<StackedAreaSeriesApi | null>(null);
  const cvdLine = useRef<ISeriesApi<"Area"> | null>(null);
  const marks = useRef<ISeriesMarkersPluginApi<Time> | null>(null);
  const index = useRef<Map<number, KlineItem>>(new Map());
  const view = useRef<ChartData>(EMPTY_DATA);
  const footView = useRef<FootprintView>(EMPTY_FOOT);
  const stackViewRef = useRef<OrderFlowStackView>(stackView);
  stackViewRef.current = stackView;
  const loadedKey = useRef<string>("");
  // 换型待恢复的时间视窗，数据未达时挂起
  const keepRange = useRef<IRange<Time> | null>(null);
  const kindRef = useRef<string>(kind);
  kindRef.current = kind;
  const subRef = useRef<string>(subKind);
  subRef.current = subKind;
  const rule = useRef<{ tick: string; step: string }>({
    tick: tickSize,
    step: sizeStep,
  });
  rule.current = { tick: tickSize, step: sizeStep };
  // 极值与轴投影经引用供闭包读取
  const overlay = useRef<HTMLCanvasElement | null>(null);
  const extremes = useRef<SessionExtremes | null>(null);
  const extremeLines = useRef<IPriceLine[]>([]);
  const showExtRef = useRef<boolean>(showExtremes);
  showExtRef.current = showExtremes;
  const showAxisRef = useRef<boolean>(showBookAxis);
  showAxisRef.current = showBookAxis;
  const obRef = useRef<OrderbookResponse | null>(orderbook);
  obRef.current = orderbook;
  const awayCallbackRef = useRef(onAwayFromLatestChange);
  awayCallbackRef.current = onAwayFromLatestChange;

  /** 极值虚线同步：清旧建新，关则清空。 */
  const syncExtremeLines = (): void => {
    const series = main.current;
    if (series === null) {
      return;
    }
    for (const line of extremeLines.current) {
      series.removePriceLine(line);
    }
    extremeLines.current = [];
    const found = extremes.current;
    if (!showExtRef.current || found === null) {
      return;
    }
    const ink = chartColor("--text-secondary");
    const background = chartColor("--background-elevated");
    for (const mark of [found.high, found.low]) {
      extremeLines.current.push(
        series.createPriceLine({
          price: mark.value,
          color: ink,
          lineWidth: 1,
          lineStyle: LineStyle.Dashed,
          axisLabelVisible: true,
          axisLabelColor: background,
          axisLabelTextColor: ink,
          title: "",
        }),
      );
    }
  };

  /** 盘口轴投影：价格轴右缘短刻线，长度取挂量分位。 */
  const paintBookAxis = (): void => {
    const target = overlay.current;
    const instance = chart.current;
    const series = main.current;
    if (target === null || instance === null || series === null) {
      return;
    }
    const scale = window.devicePixelRatio;
    const axisWidth = instance.priceScale("right").width();
    const pane = instance.panes()[MAIN_PANE_INDEX];
    const paneHeight = pane === undefined ? 0 : pane.getHeight();
    if (axisWidth <= 0 || paneHeight <= 0) {
      return;
    }
    target.style.width = `${String(axisWidth)}px`;
    target.style.height = `${String(paneHeight)}px`;
    target.width = Math.max(1, Math.round(axisWidth * scale));
    target.height = Math.max(1, Math.round(paneHeight * scale));
    const context = target.getContext("2d");
    if (context === null) {
      return;
    }
    context.clearRect(0, 0, target.width, target.height);
    const book = obRef.current;
    if (!showAxisRef.current || book === null) {
      return;
    }
    const sizes: number[] = [];
    for (const level of [...book.asks, ...book.bids]) {
      const value = plotNumber(level.size);
      if (value !== null && value > 0) {
        sizes.push(value);
      }
    }
    if (sizes.length === 0) {
      return;
    }
    sizes.sort((left, right) => left - right);
    const maxLength = Math.max(AXIS_TICK_MIN, axisWidth - AXIS_TICK_PAD * HALF);
    const drawSide = (
      levels: readonly OrderbookLevel[],
      channels: RgbaChannels,
    ): void => {
      context.strokeStyle = channelFill(channels);
      context.lineWidth = scale;
      context.beginPath();
      for (const level of levels) {
        const price = plotNumber(level.price);
        const size = plotNumber(level.size);
        if (price === null || size === null || size <= 0) {
          continue;
        }
        const coordinate = series.priceToCoordinate(price);
        if (coordinate === null || coordinate < 0 || coordinate > paneHeight) {
          continue;
        }
        const length = Math.max(
          AXIS_TICK_MIN,
          rankRatio(sizes, size) * maxLength,
        );
        const y = Math.round(coordinate * scale) + scale / HALF;
        const right = target.width - AXIS_TICK_PAD * scale;
        context.moveTo(right, y);
        context.lineTo(right - length * scale, y);
      }
      context.stroke();
    };
    drawSide(book.asks, chartChannels("--state-negative"));
    drawSide(book.bids, chartChannels("--state-positive"));
  };
  const paintBookAxisRef = useRef<() => void>(paintBookAxis);
  paintBookAxisRef.current = paintBookAxis;

  /** 窗格高按比例分摊，CVD 在场时三分。 */
  const syncPaneRatios = (): void => {
    const instance = chart.current;
    const element = holder.current;
    if (instance === null || element === null) {
      return;
    }
    const split = cvdLine.current !== null;
    applyPaneRatio(
      instance,
      element,
      split ? VOLUME_PANE_RATIO_SPLIT : VOLUME_PANE_RATIO,
      VOLUME_PANE_INDEX,
    );
    if (split) {
      applyPaneRatio(instance, element, CVD_PANE_RATIO, CVD_PANE_INDEX);
    }
  };

  /** 副窗格装填：足迹走撮合口径，其余走官方量。 */
  const syncSubPane = (): void => {
    const series = volume.current;
    const stackSeries = stacked.current;
    if (series === null || stackSeries === null) {
      return;
    }
    if (kindRef.current === FOOTPRINT_KIND) {
      if (subRef.current === FOOT_SUB_STACKED) {
        series.setData([]);
        stackSeries.setData(stackViewRef.current.data);
        return;
      }
      stackSeries.setData([]);
      series.setData(
        subRef.current === FOOT_SUB_DELTA
          ? footView.current.deltas
          : footView.current.volumes,
      );
      return;
    }
    stackSeries.setData([]);
    series.setData(view.current.volumes);
  };

  /** 提示框行按模式重建，只在模式切换时执行。 */
  const buildTip = (): void => {
    const tipElement = tip.current;
    if (tipElement === null) {
      return;
    }
    const mode =
      kindRef.current === FOOTPRINT_KIND
        ? FOOTPRINT_KIND
        : showExtRef.current
          ? "kline-ext"
          : "kline";
    if (tipMode.current === mode) {
      return;
    }
    tipMode.current = mode;
    tipElement.replaceChildren();
    pinnedTime.current = null;
    tipElement.classList.remove("is-pinned", "is-visible");
    tipElement.classList.toggle("chart-tip--footprint", mode === FOOTPRINT_KIND);
    footTipLevels.current = null;
    footTipItem.current = null;
    const labels =
      mode === FOOTPRINT_KIND
        ? FOOT_TIP_LABELS
        : mode === "kline-ext"
          ? [...TIP_LABELS, ...EXT_TIP_LABELS]
          : TIP_LABELS;
    cells.current = labels.map((label) => {
      const row = document.createElement("div");
      row.className = "tip-row";
      const name = document.createElement("span");
      name.className = "micro-label";
      name.textContent = label;
      const value = document.createElement("span");
      value.className = "tip-value";
      row.append(name, value);
      tipElement.append(row);
      return value;
    });
    if (mode === FOOTPRINT_KIND) {
      const levels = document.createElement("div");
      levels.className = "foot-tip-levels";
      tipElement.append(levels);
      footTipLevels.current = levels;
    }
  };

  useEffect(() => {
    const element = holder.current;
    const tipElement = tip.current;
    if (element === null || tipElement === null) {
      return;
    }
    const instance = createThemedChart(element);
    chart.current = instance;
    volume.current = addVolume(
      instance,
      VOLUME_PANE_INDEX,
      decimalFormat(rule.current.step),
    );
    stacked.current = addStackedArea(instance, VOLUME_PANE_INDEX, [
      "--state-negative",
      "--state-positive",
    ]);
    buildTip();
    syncPaneRatios();

    const setCell = (position: number, text: string): void => {
      const cell = cells.current[position];
      if (cell !== undefined) {
        cell.textContent = text;
      }
    };

    /** 足迹提示：按整根蜡烛列出全部价格档，不再由游标纵坐标命中单档。 */
    const footTip = (item: FootprintBarItem): void => {
      setCell(0, jstStamp(item.open_time));
      setCell(1, rawText(item.delta));
      setCell(2, rawText(item.total));
      const levelsElement = footTipLevels.current;
      if (levelsElement === null || footTipItem.current === item) {
        return;
      }
      footTipItem.current = item;
      levelsElement.replaceChildren();
      const fragment = document.createDocumentFragment();
      for (const label of ["价格档", "主动卖", "主动买"]) {
        const head = document.createElement("span");
        head.className = "foot-tip-levels__head";
        head.textContent = label;
        fragment.append(head);
      }
      const sorted = [...item.levels].sort((left, right) => {
        return (
          (plotNumber(right.price_bin) ?? Number.NEGATIVE_INFINITY) -
          (plotNumber(left.price_bin) ?? Number.NEGATIVE_INFINITY)
        );
      });
      if (sorted.length === 0) {
        const empty = document.createElement("span");
        empty.className = "foot-tip-levels__empty";
        empty.textContent = "无成交档";
        fragment.append(empty);
      } else {
        for (const level of sorted) {
          const values = [
            priceText(level.price_bin, rule.current.tick),
            rawText(level.sell),
            rawText(level.buy),
          ];
          for (const value of values) {
            const cell = document.createElement("span");
            cell.className = "foot-tip-levels__value";
            cell.textContent = value;
            fragment.append(cell);
          }
        }
      }
      levelsElement.append(fragment);
    };

    const itemAt = (
      time: number,
    ): { item: KlineItem | FootprintBarItem; foot: boolean } | null => {
      const foot = kindRef.current === FOOTPRINT_KIND;
      const item = foot
        ? footView.current.index.get(time)
        : index.current.get(time);
      return item === undefined ? null : { item, foot };
    };

    const clearPin = (): void => {
      pinnedTime.current = null;
      tipElement.classList.remove("is-pinned");
    };

    const positionTip = (
      time: number,
      item: KlineItem | FootprintBarItem,
      foot: boolean,
      fallbackPoint?: { x: number; y: number },
    ): void => {
      const width = element.clientWidth;
      const height = element.clientHeight;
      const timeCoordinate = instance
        .timeScale()
        .timeToCoordinate(time as UTCTimestamp);
      const anchorX = timeCoordinate ?? fallbackPoint?.x;
      if (anchorX === undefined || anchorX < 0 || anchorX > width) {
        tipElement.classList.remove("is-visible");
        return;
      }
      tipElement.classList.add("is-visible");
      const tipWidth = tipElement.offsetWidth;
      const left = Math.max(
        0,
        Math.min(width - tipWidth, anchorX - tipWidth / HALF),
      );
      tipElement.style.left = `${String(Math.round(left))}px`;
      if (foot) {
        const high = plotNumber((item as FootprintBarItem).high);
        const highCoordinate =
          high === null ? null : main.current?.priceToCoordinate(high);
        const anchorY = highCoordinate ?? fallbackPoint?.y;
        if (anchorY === undefined || anchorY < 0 || anchorY > height) {
          tipElement.classList.remove("is-visible");
          return;
        }
        // 底边锚定蜡烛高点
        // 明细表只向上展开
        const bottom = Math.max(0, height - anchorY + TOOLTIP_GAP);
        const pinned = pinnedTime.current === time;
        const desiredTop = anchorY - tipElement.offsetHeight - TOOLTIP_GAP;
        if (pinned && desiredTop < TOOLTIP_GAP) {
          // 锁定态优先可操作
          // 空间不足时贴图顶
          tipElement.style.top = `${String(TOOLTIP_GAP)}px`;
          tipElement.style.bottom = "auto";
        } else {
          tipElement.style.top = "auto";
          tipElement.style.bottom = `${String(Math.round(bottom))}px`;
        }
      } else {
        tipElement.style.top = "";
        tipElement.style.bottom = "";
      }
    };

    const renderTip = (
      time: number,
      item: KlineItem | FootprintBarItem,
      foot: boolean,
      fallbackPoint?: { x: number; y: number },
    ): void => {
      if (foot) {
        footTip(item as FootprintBarItem);
      } else {
        const bar = item as KlineItem;
        // 提示值按规则精度归一分组，不缩写不换算。
        setCell(0, jstStamp(bar.open_time));
        setCell(1, priceText(bar.open, rule.current.tick));
        setCell(2, priceText(bar.high, rule.current.tick));
        setCell(3, priceText(bar.low, rule.current.tick));
        setCell(4, priceText(bar.close, rule.current.tick));
        setCell(5, sizeText(bar.volume, rule.current.step));
        const found = extremes.current;
        if (showExtRef.current && found !== null) {
          setCell(
            6,
            priceDiffText(found.high.text, bar.close, rule.current.tick),
          );
          setCell(
            7,
            priceDiffText(bar.close, found.low.text, rule.current.tick),
          );
        }
      }
      positionTip(time, item, foot, fallbackPoint);
    };

    const onCrosshair = (param: MouseEventParams<Time>): void => {
      if (pinnedTime.current !== null) {
        return;
      }
      const point = param.point;
      const time = param.time;
      const width = element.clientWidth;
      const height = element.clientHeight;
      if (
        point === undefined ||
        typeof time !== "number" ||
        point.x < 0 ||
        point.x > width ||
        point.y < 0 ||
        point.y > height
      ) {
        tipElement.classList.remove("is-visible");
        return;
      }
      const found = itemAt(time);
      if (found === null) {
        tipElement.classList.remove("is-visible");
        return;
      }
      renderTip(time, found.item, found.foot, point);
    };

    const onClick = (param: MouseEventParams<Time>): void => {
      if (param.hoveredInfo?.objectId !== undefined) {
        return;
      }
      const time = param.time;
      if (typeof time !== "number") {
        clearPin();
        tipElement.classList.remove("is-visible");
        return;
      }
      const found = itemAt(time);
      if (found === null) {
        clearPin();
        tipElement.classList.remove("is-visible");
        return;
      }
      if (pinnedTime.current === time) {
        clearPin();
        renderTip(time, found.item, found.foot, param.point);
        return;
      }
      pinnedTime.current = time;
      tipElement.classList.add("is-pinned");
      renderTip(time, found.item, found.foot, param.point);
    };

    const positionPinned = (): void => {
      const time = pinnedTime.current;
      if (time === null) {
        return;
      }
      const found = itemAt(time);
      if (found === null) {
        clearPin();
        tipElement.classList.remove("is-visible");
        return;
      }
      renderTip(time, found.item, found.foot);
    };

    const hideHoverDuringInteraction = (): void => {
      if (pinnedTime.current !== null) {
        return;
      }
      tipElement.classList.remove("is-visible");
      instance.clearCrosshairPosition();
    };
    const onPointerDown = (event: PointerEvent): void => {
      if (event.button !== 0) {
        return;
      }
      element.classList.add("is-interacting");
      hideHoverDuringInteraction();
    };
    const stopInteraction = (): void => {
      element.classList.remove("is-interacting");
    };
    const onWheel = (): void => {
      hideHoverDuringInteraction();
    };
    let pinRepositionFrame = 0;
    const onPointerMove = (): void => {
      if (pinnedTime.current === null || pinRepositionFrame !== 0) {
        return;
      }
      pinRepositionFrame = window.requestAnimationFrame(() => {
        pinRepositionFrame = 0;
        positionPinned();
      });
    };
    const onKeyDown = (event: KeyboardEvent): void => {
      if (event.key !== "Escape") {
        return;
      }
      clearPin();
      tipElement.classList.remove("is-visible");
    };

    instance.subscribeCrosshairMove(onCrosshair);
    instance.subscribeClick(onClick);
    element.addEventListener("pointerdown", onPointerDown);
    element.addEventListener("pointermove", onPointerMove);
    element.addEventListener("wheel", onWheel, { passive: true });
    window.addEventListener("pointerup", stopInteraction);
    window.addEventListener("pointercancel", stopInteraction);
    window.addEventListener("keydown", onKeyDown);
    // 视窗变化同步交互态
    const repaintAxis = (): void => {
      paintBookAxisRef.current();
      awayCallbackRef.current(
        Math.abs(instance.timeScale().scrollPosition()) > 0.5,
      );
      positionPinned();
    };
    instance.timeScale().subscribeVisibleLogicalRangeChange(repaintAxis);
    const observer = new ResizeObserver(() => {
      syncPaneRatios();
      repaintAxis();
    });
    observer.observe(element);
    return () => {
      observer.disconnect();
      instance.timeScale().unsubscribeVisibleLogicalRangeChange(repaintAxis);
      instance.unsubscribeCrosshairMove(onCrosshair);
      instance.unsubscribeClick(onClick);
      element.removeEventListener("pointerdown", onPointerDown);
      element.removeEventListener("pointermove", onPointerMove);
      element.removeEventListener("wheel", onWheel);
      window.removeEventListener("pointerup", stopInteraction);
      window.removeEventListener("pointercancel", stopInteraction);
      window.removeEventListener("keydown", onKeyDown);
      window.cancelAnimationFrame(pinRepositionFrame);
      instance.remove();
      tipElement.replaceChildren();
      cells.current = [];
      footTipLevels.current = null;
      footTipItem.current = null;
      pinnedTime.current = null;
      tipMode.current = "";
      chart.current = null;
      main.current = null;
      volume.current = null;
      stacked.current = null;
      cvdLine.current = null;
      marks.current = null;
      extremeLines.current = [];
      loadedKey.current = "";
    };
    // 初始化只执行一次
  }, []);

  useEffect(() => {
    pinnedTime.current = null;
    tip.current?.classList.remove("is-pinned", "is-visible");
  }, [viewKey]);

  useEffect(() => {
    const instance = chart.current;
    if (instance === null || navigationCommand === null) {
      return;
    }
    pinnedTime.current = null;
    tip.current?.classList.remove("is-pinned", "is-visible");
    if (navigationCommand.action === "latest") {
      instance.timeScale().scrollToPosition(0, true);
      return;
    }
    instance.timeScale().fitContent();
    for (let paneIndex = 0; paneIndex < instance.panes().length; paneIndex += 1) {
      instance.priceScale("right", paneIndex).applyOptions({ autoScale: true });
    }
  }, [navigationCommand]);

  useEffect(() => {
    const instance = chart.current;
    if (instance === null) {
      return;
    }
    // 换型保留视窗：记时间区间，跨数据源可映射
    const timeScale = instance.timeScale();
    const visible = timeScale.getVisibleRange();
    if (visible !== null) {
      keepRange.current = visible;
    }
    const previous = main.current;
    // 先建后删，主窗格不因空置而被并掉
    const series = createMain(
      instance,
      kind,
      rule.current.tick,
      view.current.baseValue ?? 0,
    );
    if (previous !== null) {
      instance.removeSeries(previous);
    }
    applyMainOptions(series, rule.current.tick);
    series.priceScale().applyOptions({
      mode: logScale ? PriceScaleMode.Logarithmic : PriceScaleMode.Normal,
    });
    main.current = series;
    marks.current = attachMarkers(series, markers);
    // 换系列后极值线随新系列重建
    extremeLines.current = [];
    syncExtremeLines();
    if (kind === FOOTPRINT_KIND) {
      footApi(series).applyOptions({
        binSize: footView.current.binSize,
        sessions: footView.current.sessions,
        showPoc,
        showVa,
      });
    }
    buildTip();
    fillMain(series, kind, view.current, footView.current);
    syncSubPane();
    const filled =
      kind === FOOTPRINT_KIND
        ? footView.current.bars.length
        : view.current.candles.length;
    if (keepRange.current !== null && filled > 0) {
      // 目标数据在场即恢复，否则挂起待数据
      timeScale.setVisibleRange(keepRange.current);
      keepRange.current = null;
    }
    syncPaneRatios();
    // 图型切换重建主系列
  }, [kind]);

  useEffect(() => {
    const instance = chart.current;
    const mainSeries = main.current;
    const element = holder.current;
    if (instance === null || mainSeries === null || element === null) {
      return;
    }
    const data = toChartData(
      items,
      chartColor("--state-positive-soft"),
      chartColor("--state-negative-soft"),
    );
    index.current = data.index;
    view.current = data;
    // 会话极值随数据刷新
    extremes.current = sessionExtremesOf(items);
    syncExtremeLines();
    if (kindRef.current === FOOTPRINT_KIND) {
      // 足迹模式不触官方口径序列
      return;
    }
    const timeScale = instance.timeScale();
    // 同一视图键内保留用户视窗
    const sameView = loadedKey.current === viewKey;
    if (!sameView) {
      keepRange.current = null;
    }
    const pending = keepRange.current;
    const wasAtLatest =
      sameView &&
      pending === null &&
      Math.abs(timeScale.scrollPosition()) <= 0.5;
    const kept =
      sameView && pending === null
        ? timeScale.getVisibleLogicalRange()
        : null;
    fillMain(mainSeries, kindRef.current, data, footView.current);
    syncSubPane();
    if (kindRef.current === "baseline" && data.baseValue !== null) {
      mainSeries.applyOptions({
        baseValue: { type: "price", price: data.baseValue },
      });
    }
    if (pending !== null && data.candles.length > 0) {
      // 换型挂起的时间视窗随数据到达恢复
      timeScale.setVisibleRange(pending);
      keepRange.current = null;
    } else if (wasAtLatest) {
      // 仅用户原本位于最新端时随新数据推进。
      timeScale.scrollToPosition(0, false);
    } else if (kept !== null) {
      timeScale.setVisibleLogicalRange(kept);
    } else if (pending === null) {
      timeScale.fitContent();
    }
    loadedKey.current = viewKey;
    syncPaneRatios();
    // 数据装填由键与数据驱动
  }, [items, viewKey]);

  useEffect(() => {
    const instance = chart.current;
    const mainSeries = main.current;
    if (instance === null || mainSeries === null) {
      return;
    }
    // 空值含未定义，热替换过渡态也兜底
    footView.current =
      footprint == null
        ? EMPTY_FOOT
        : toFootprintView(
            footprint,
            chartColor("--state-positive-soft"),
            chartColor("--state-negative-soft"),
            display.valueBasis === BASIS_NOTIONAL,
          );
    if (kindRef.current !== FOOTPRINT_KIND) {
      return;
    }
    const timeScale = instance.timeScale();
    const sameView = loadedKey.current === viewKey;
    if (!sameView) {
      keepRange.current = null;
    }
    const pending = keepRange.current;
    const wasAtLatest =
      sameView &&
      pending === null &&
      Math.abs(timeScale.scrollPosition()) <= 0.5;
    const kept =
      sameView && pending === null
        ? timeScale.getVisibleLogicalRange()
        : null;
    footApi(mainSeries).applyOptions({
      binSize: footView.current.binSize,
      sessions: footView.current.sessions,
      showPoc,
      showVa,
    });
    fillMain(mainSeries, kindRef.current, view.current, footView.current);
    syncSubPane();
    cvdLine.current?.setData(footView.current.cvd);
    if (pending !== null && footView.current.bars.length > 0) {
      // 换型挂起的时间视窗随数据到达恢复
      timeScale.setVisibleRange(pending);
      keepRange.current = null;
    } else if (wasAtLatest) {
      timeScale.scrollToPosition(0, false);
    } else if (kept !== null) {
      timeScale.setVisibleLogicalRange(kept);
    } else if (pending === null) {
      timeScale.fitContent();
    }
    loadedKey.current = viewKey;
    syncPaneRatios();
    // 足迹数据装填与派生层刷新，随基准重建
  }, [footprint, viewKey, display.valueBasis, stackView]);

  useEffect(() => {
    const series = main.current;
    if (kindRef.current !== FOOTPRINT_KIND || series === null) {
      return;
    }
    footApi(series).applyOptions({ showPoc, showVa });
  }, [showPoc, showVa]);

  useEffect(() => {
    // 极值开关切换重建线与提示行
    syncExtremeLines();
    buildTip();
  }, [showExtremes]);

  useEffect(() => {
    // 盘口数据或开关变化重绘投影
    paintBookAxisRef.current();
  }, [orderbook, showBookAxis, kind, logScale, items, footprint]);

  useEffect(() => {
    syncSubPane();
    // 口径切换只换副窗格数据
  }, [subKind]);

  useEffect(() => {
    const instance = chart.current;
    if (instance === null) {
      return;
    }
    const wanted = kind === FOOTPRINT_KIND && showCvd;
    if (wanted && cvdLine.current === null) {
      const series = addArea(instance, CVD_PANE_INDEX);
      series.setData(footView.current.cvd);
      cvdLine.current = series;
    }
    if (!wanted && cvdLine.current !== null) {
      instance.removeSeries(cvdLine.current);
      cvdLine.current = null;
      const pane = instance.panes()[CVD_PANE_INDEX];
      if (pane !== undefined && pane.getSeries().length === 0) {
        instance.removePane(CVD_PANE_INDEX);
      }
    }
    syncPaneRatios();
    // CVD 窗格开合
  }, [kind, showCvd, footprint]);

  useEffect(() => {
    const series = main.current;
    if (series !== null) {
      applyMainOptions(series, tickSize);
    }
  }, [tickSize, display]);

  useEffect(() => {
    // 金额基准下量轴按 JPY 整数步长
    const step =
      kind === FOOTPRINT_KIND && display.valueBasis === BASIS_NOTIONAL
        ? NOTIONAL_STEP
        : sizeStep;
    const format = decimalFormat(step);
    if (format === null) {
      return;
    }
    volume.current?.applyOptions({
      priceFormat: {
        type: "custom",
        minMove: format.minMove,
        formatter: (price: number) => axisTotalText(price, format.raw),
      },
    });
    stacked.current?.applyOptions({
      priceFormat: {
        type: "custom",
        minMove: format.minMove,
        formatter: (price: number) => axisTotalText(price, format.raw),
      },
    });
  }, [sizeStep, display, kind]);

  useEffect(() => {
    main.current?.priceScale().applyOptions({
      mode: logScale ? PriceScaleMode.Logarithmic : PriceScaleMode.Normal,
    });
  }, [logScale]);

  useEffect(() => {
    marks.current?.setMarkers([...markers]);
  }, [markers]);

  return (
    <div className="chart-wrap">
      <div
        className="chart"
        ref={holder}
        tabIndex={0}
        aria-label="价格图表：拖动平移时间，滚轮缩放，单击锁定提示框"
      />
      <canvas className="chart-axis-overlay" ref={overlay} />
      <div className="chart-tip" ref={tip} role="tooltip" />
      <StackLegend
        visible={kind === FOOTPRINT_KIND && subKind === FOOT_SUB_STACKED}
        symbol={footprint?.meta.symbol ?? null}
        notional={display.valueBasis === BASIS_NOTIONAL}
        pending={footprintPending}
        stale={footprintStale}
        coverageClipped={footprintCoverageClipped}
        view={stackView}
        sideBasis={footprint?.meta.side_basis ?? null}
      />
    </div>
  );
}

/** memo 边界：入参实变才重渲，画布终生只挂一次。 */
export const KlineChart = memo(KlineChartImpl);
