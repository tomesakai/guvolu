import {
  AreaSeries,
  BarSeries,
  BaselineSeries,
  CandlestickSeries,
  ColorType,
  CrosshairMode,
  HistogramSeries,
  LineSeries,
  LineStyle,
  createChart,
  createSeriesMarkers,
} from "lightweight-charts";
import type {
  CrosshairLineOptions,
  DeepPartial,
  IChartApi,
  ISeriesApi,
  ISeriesMarkersPluginApi,
  SeriesMarker,
  SeriesPartialOptions,
  SeriesType,
  Time,
  TickMarkType,
  WhitespaceData,
} from "lightweight-charts";
import type { FootprintBarDatum } from "./plugins/footprint/data";
import type { FootprintSeriesOptions } from "./plugins/footprint/options";
import type { StackedAreaData } from "./plugins/stacked-area/data";
import type { StackedAreaSeriesOptions } from "./plugins/stacked-area/options";
import { StackedAreaSeries } from "./plugins/stacked-area/stacked-area-series";
import { FootprintSeries } from "./plugins/footprint/footprint-series";
import {
  axisPriceText,
  axisTotalText,
  EMPTY_TEXT,
  epochToIso,
  jstHourMinute,
  jstMonthDay,
  jstStamp,
  jstYear,
  jstYearMonth,
} from "./format";

// 变量缺失时的兜底字号
const FALLBACK_FONT_SIZE = 11;
// 刻度类型：年、月、日、时、时分秒
const TICK_YEAR = 0;
const TICK_MONTH = 1;
const TICK_DAY = 2;

/** 读取设计 token 的取值，禁止在此层写死颜色。 */
export function tokenValue(name: string): string {
  return getComputedStyle(document.documentElement)
    .getPropertyValue(name)
    .trim();
}

/** 读取 token 中的像素字号并转为数值。 */
export function tokenSize(name: string): number {
  const parsed = Number.parseInt(tokenValue(name), 10);
  return Number.isFinite(parsed) ? parsed : FALLBACK_FONT_SIZE;
}

/** 颜色的四通道取值，供画布按强度直接合成。 */
export interface RgbaChannels {
  readonly r: number;
  readonly g: number;
  readonly b: number;
  readonly a: number;
}

// 单通道满值
export const MAX_CHANNEL = 255;
const ALPHA_DIGITS = 3;
const BLANK: RgbaChannels = { r: 0, g: 0, b: 0, a: 0 };

const channelCache = new Map<string, RgbaChannels>();
let colorProbe: CanvasRenderingContext2D | null = null;

/** 浏览器解析任意 CSS 颜色并回读通道。

引擎自带解析器不识别 color-mix 等新式函数，
故经一像素画布让浏览器求值后取回具体色。
本函数输出属运行时转换，不属设计常量。
*/
function probeChannels(input: string): RgbaChannels {
  const cached = channelCache.get(input);
  if (cached !== undefined) {
    return cached;
  }
  if (colorProbe === null) {
    const canvas = document.createElement("canvas");
    canvas.width = 1;
    canvas.height = 1;
    colorProbe = canvas.getContext("2d", { willReadFrequently: true });
  }
  if (colorProbe === null) {
    return BLANK;
  }
  colorProbe.clearRect(0, 0, 1, 1);
  colorProbe.fillStyle = input;
  colorProbe.fillRect(0, 0, 1, 1);
  const [r, g, b, a] = colorProbe.getImageData(0, 0, 1, 1).data;
  const found: RgbaChannels = {
    r: r ?? 0,
    g: g ?? 0,
    b: b ?? 0,
    a: (a ?? 0) / MAX_CHANNEL,
  };
  channelCache.set(input, found);
  return found;
}

/** 读取颜色 token 并解析为引擎可用形态。 */
export function chartColor(name: string): string {
  const found = probeChannels(tokenValue(name));
  const alpha = found.a.toFixed(ALPHA_DIGITS);
  return `rgba(${String(found.r)}, ${String(found.g)}, ${String(found.b)}, ${alpha})`;
}

/** 读取颜色 token 的通道值，供画布逐像素合成。 */
export function chartChannels(name: string): RgbaChannels {
  return probeChannels(tokenValue(name));
}

/** 通道值转不透明填充色，供画布描边与填充。 */
export function channelFill(color: RgbaChannels): string {
  return `rgb(${String(color.r)}, ${String(color.g)}, ${String(color.b)})`;
}

function toIso(time: Time): string | null {
  return typeof time === "number" ? epochToIso(time) : null;
}

function timeFormatter(time: Time): string {
  const iso = toIso(time);
  return iso === null ? EMPTY_TEXT : jstStamp(iso);
}

function tickMarkFormatter(time: Time, tickMarkType: TickMarkType): string {
  const iso = toIso(time);
  if (iso === null) {
    return EMPTY_TEXT;
  }
  if (tickMarkType === TICK_YEAR) {
    return jstYear(iso);
  }
  if (tickMarkType === TICK_MONTH) {
    return jstYearMonth(iso);
  }
  if (tickMarkType === TICK_DAY) {
    return jstMonthDay(iso);
  }
  return jstHourMinute(iso);
}

/**
 * 建图并注入设计语言主题：背景透明融入面板，
 * 网格与边框走默认边框色，十字线走焦点色，时间显示转 JST。
 */
export function createThemedChart(container: HTMLElement): IChartApi {
  const border = chartColor("--border-default");
  const focus = chartColor("--border-focus");
  const elevated = chartColor("--background-elevated");
  // 纵线保留、标签交由提示框，横线整体隐藏
  const vertLine: DeepPartial<CrosshairLineOptions> = {
    color: focus,
    width: 1,
    style: LineStyle.Dashed,
    labelVisible: false,
    labelBackgroundColor: elevated,
  };
  const horzLine: DeepPartial<CrosshairLineOptions> = {
    visible: false,
    labelVisible: false,
  };
  return createChart(container, {
    autoSize: true,
    layout: {
      background: { type: ColorType.Solid, color: "transparent" },
      textColor: chartColor("--text-secondary"),
      fontSize: tokenSize("--fs-badge"),
      fontFamily: tokenValue("--font-mono"),
      attributionLogo: false,
      panes: {
        separatorColor: border,
        separatorHoverColor: focus,
        enableResize: true,
      },
    },
    grid: {
      vertLines: { visible: false },
      horzLines: { color: border, style: LineStyle.Solid },
    },
    crosshair: {
      mode: CrosshairMode.Normal,
      vertLine,
      horzLine,
    },
    rightPriceScale: { borderColor: border, ticksVisible: true },
    timeScale: {
      borderColor: border,
      timeVisible: true,
      secondsVisible: false,
      tickMarkFormatter,
      minBarSpacing: 0.05,
    },
    // 显式固定交互语义
    // 画布与横滚平移时间
    // 纵滚缩放，轴拖调尺度
    handleScroll: {
      mouseWheel: true,
      pressedMouseMove: true,
      horzTouchDrag: true,
      // 单指纵滑留给页面
      vertTouchDrag: false,
    },
    handleScale: {
      mouseWheel: true,
      pinch: true,
      axisPressedMouseMove: { time: true, price: true },
      axisDoubleClickReset: { time: true, price: true },
    },
    kineticScroll: { mouse: false, touch: true },
    localization: { timeFormatter },
  });
}

/** 补齐窗格，使目标窗格序号可用。 */
function ensurePane(chart: IChartApi, paneIndex: number): void {
  while (chart.panes().length <= paneIndex) {
    chart.addPane(true);
  }
}

/** 价格精度：小数位数与最小变动，取自品种规则。 */
export interface DecimalFormat {
  readonly precision: number;
  readonly minMove: number;
  // 原始规则串，供轴格式器推导
  readonly raw: string;
}

/** 蜡烛：价格主图，兼作回放页事件叠加的基底。 */
export function addCandles(
  chart: IChartApi,
  paneIndex: number,
  format: DecimalFormat | null = null,
): ISeriesApi<"Candlestick"> {
  ensurePane(chart, paneIndex);
  const rise = chartColor("--state-positive");
  const fall = chartColor("--state-negative");
  return chart.addSeries(
    CandlestickSeries,
    {
      priceFormat:
        format === null
          ? { type: "price" }
          : {
              type: "custom",
              minMove: format.minMove,
              formatter: (price: number) => axisPriceText(price, format.raw),
            },
      upColor: rise,
      downColor: fall,
      borderUpColor: rise,
      borderDownColor: fall,
      wickUpColor: rise,
      wickDownColor: fall,
      borderVisible: true,
      wickVisible: true,
      lastValueVisible: true,
      priceLineVisible: true,
      priceLineColor: chartColor("--border-focus"),
      priceLineStyle: LineStyle.Dashed,
      priceLineWidth: 1,
    },
    paneIndex,
  );
}

/** 成交量：逐根按涨跌取语义色透明档，另用于换手。 */
export function addVolume(
  chart: IChartApi,
  paneIndex: number,
  format: DecimalFormat | null = null,
): ISeriesApi<"Histogram"> {
  ensurePane(chart, paneIndex);
  return chart.addSeries(
    HistogramSeries,
    {
      color: chartColor("--state-positive-soft"),
      priceFormat:
        format === null
          ? { type: "volume" }
          : {
              type: "custom",
              minMove: format.minMove,
              formatter: (price: number) => axisTotalText(price, format.raw),
            },
      priceLineVisible: false,
      lastValueVisible: false,
    },
    paneIndex,
  );
}

/** 条形：蜡烛的等价异形，仅特殊对比场景，缺省不用。 */
export function addBars(chart: IChartApi, paneIndex: number): ISeriesApi<"Bar"> {
  ensurePane(chart, paneIndex);
  return chart.addSeries(
    BarSeries,
    {
      upColor: chartColor("--state-positive"),
      downColor: chartColor("--state-negative"),
    },
    paneIndex,
  );
}

/** 折线：中间价与价差时序、IC 时序、滚动 Sharpe、跨所基差。 */
export function addLine(
  chart: IChartApi,
  paneIndex: number,
  colorToken = "--border-focus",
): ISeriesApi<"Line"> {
  ensurePane(chart, paneIndex);
  return chart.addSeries(
    LineSeries,
    {
      color: chartColor(colorToken),
      lineWidth: 1,
      priceLineVisible: false,
      lastValueVisible: true,
    },
    paneIndex,
  );
}

/** 面积：累计语义专用，如 CVD、累计手续费、采集量。 */
export function addArea(
  chart: IChartApi,
  paneIndex: number,
  lineToken = "--state-info",
  fillToken = "--state-info-soft",
): ISeriesApi<"Area"> {
  ensurePane(chart, paneIndex);
  const fill = chartColor(fillToken);
  return chart.addSeries(
    AreaSeries,
    {
      lineColor: chartColor(lineToken),
      topColor: fill,
      bottomColor: fill,
      lineWidth: 1,
      priceLineVisible: false,
    },
    paneIndex,
  );
}

/** 基线：正负分色的基准偏离，如净值曲线、超额、浮动盈亏。 */
export function addBaseline(
  chart: IChartApi,
  paneIndex: number,
  baseValue: number,
): ISeriesApi<"Baseline"> {
  ensurePane(chart, paneIndex);
  const rise = chartColor("--state-positive");
  const fall = chartColor("--state-negative");
  const riseFill = chartColor("--state-positive-soft");
  const fallFill = chartColor("--state-negative-soft");
  return chart.addSeries(
    BaselineSeries,
    {
      baseValue: { type: "price", price: baseValue },
      topLineColor: rise,
      topFillColor1: riseFill,
      topFillColor2: riseFill,
      bottomLineColor: fall,
      bottomFillColor1: fallFill,
      bottomFillColor2: fallFill,
      lineWidth: 1,
      priceLineVisible: false,
    },
    paneIndex,
  );
}

/** 柱状：日度盈亏分布、IC 分布等分布类图形。 */
export function addHistogram(
  chart: IChartApi,
  paneIndex: number,
  colorToken = "--border-focus",
): ISeriesApi<"Histogram"> {
  ensurePane(chart, paneIndex);
  return chart.addSeries(
    HistogramSeries,
    {
      color: chartColor(colorToken),
      priceLineVisible: false,
      lastValueVisible: false,
    },
    paneIndex,
  );
}

/**
 * 堆叠面积：多项非负分量的累计形态，当前用于主动买卖构成。
 * 颜色由此处经 token 解析后注入，插件层不写死取值。
 */
export type StackedAreaSeriesApi = ISeriesApi<
  "Custom",
  Time,
  StackedAreaData | WhitespaceData<Time>,
  StackedAreaSeriesOptions,
  SeriesPartialOptions<StackedAreaSeriesOptions>
>;

export function addStackedArea(
  chart: IChartApi,
  paneIndex: number,
  colorTokens: readonly string[],
): StackedAreaSeriesApi {
  ensurePane(chart, paneIndex);
  const colors = colorTokens.map((token) => ({
    line: chartColor(token),
    area: chartColor(`${token}-soft`),
  }));
  return chart.addCustomSeries(
    new StackedAreaSeries(),
    { colors, priceLineVisible: false },
    paneIndex,
  );
}

// 量分位透明度阶梯，首档对齐 soft
const INTENSITY_ALPHAS: readonly number[] = [0.14, 0.26, 0.4, 0.56, 0.74, 0.92];
/** 强度阶梯档数，分位映射据此分档。 */
export const INTENSITY_STEPS = INTENSITY_ALPHAS.length;
// 细节度切换的像素阈值两级
const LOD_TEXT_PX = 72;
const LOD_CELL_PX = 36;

/** token 色转强度阶梯：逐档透明度合成，色相不变。 */
function alphaRamp(token: string): string[] {
  const color = chartChannels(token);
  return INTENSITY_ALPHAS.map(
    (alpha) =>
      `rgba(${String(color.r)}, ${String(color.g)}, ${String(color.b)}, ${String(alpha)})`,
  );
}

/** 足迹系列的完整类型形态，样式项可增量套用。 */
export type FootprintSeriesApi = ISeriesApi<
  "Custom",
  Time,
  FootprintBarDatum | WhitespaceData<Time>,
  FootprintSeriesOptions,
  SeriesPartialOptions<FootprintSeriesOptions>
>;

/**
 * 足迹：主图第六型，bar 展开为价格档乘主动方向量矩阵。
 * 颜色与字体全部经 token 解析后注入，插件层零颜色常量。
 */
export function addFootprint(
  chart: IChartApi,
  paneIndex: number,
  format: DecimalFormat | null = null,
): FootprintSeriesApi {
  ensurePane(chart, paneIndex);
  return chart.addCustomSeries(
    new FootprintSeries(),
    {
      priceFormat:
        format === null
          ? { type: "price" }
          : {
              type: "custom",
              minMove: format.minMove,
              formatter: (price: number) => axisPriceText(price, format.raw),
            },
      lodTextPx: LOD_TEXT_PX,
      lodCellPx: LOD_CELL_PX,
      fontFamily: tokenValue("--font-mono"),
      fontSize: tokenSize("--fs-formula"),
      sellInk: chartColor("--state-negative"),
      buyInk: chartColor("--state-positive"),
      neutralInk: chartColor("--text-muted"),
      skeletonInk: chartColor("--text-secondary"),
      riseInk: chartColor("--state-positive"),
      fallInk: chartColor("--state-negative"),
      sellRamp: alphaRamp("--state-negative"),
      buyRamp: alphaRamp("--state-positive"),
      riseRamp: alphaRamp("--state-positive"),
      fallRamp: alphaRamp("--state-negative"),
      pocInk: chartColor("--border-focus"),
      vaInk: chartColor("--text-muted"),
      nakedInk: chartColor("--border-focus"),
      priceLineVisible: true,
      lastValueVisible: true,
    },
    paneIndex,
  );
}

/** 系列标记：回放页委托与成交事件叠加的入口（阶段 8）。 */
export function attachMarkers(
  series: ISeriesApi<SeriesType, Time>,
  markers: readonly SeriesMarker<Time>[],
): ISeriesMarkersPluginApi<Time> {
  return createSeriesMarkers(series, [...markers]);
}

/** 按比例分配副窗格高度，返回所设值供调用方记忆。 */
export function applyPaneRatio(
  chart: IChartApi,
  container: HTMLElement,
  subRatio: number,
  paneIndex = 1,
): number | null {
  const sub = chart.panes()[paneIndex];
  const height = container.clientHeight;
  if (sub === undefined || height <= 0) {
    return null;
  }
  const target = Math.round(height * subRatio);
  sub.setHeight(target);
  return target;
}
