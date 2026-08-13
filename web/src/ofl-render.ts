import type {
  BookFeatureItem,
  OrderbookResponse,
  PrintTickItem,
  TileColumn,
} from "./api";
import {
  BASIS_NOTIONAL,
  jstHourMinute,
  jstMonthDay,
  parseUtcIso,
  plotNumber,
  priceText,
} from "./format";
import type { RgbaChannels } from "./lwc";
import { channelFill, chartChannels, tokenSize, tokenValue } from "./lwc";
import { uncoveredRanges } from "./ofl-ranges";

// 几何常量全部具名，单位 css 像素
const BOOK_COL_WIDTH = 56;
const AXIS_PAD = 6;
const BAND_BORDER = 1;
const EVENT_BAND_HEIGHT = 16;
const BAND_HEIGHT = 36;
const DEPTH_BAND_HEIGHT = 40;
const TIME_STRIP_HEIGHT = 16;
const LABEL_PAD = 3;
const EVENT_MARK_HEIGHT = 6;
const EVENT_MARK_MIN_WIDTH = 3;
const PRINT_TICK_MIN = 6;
const PRINT_TICK_SPAN = 18;
const HATCH_STEP = 7;
const HALF = 2;
// 对数色模的伽马参数
const LOG_GAMMA = 0.8;
// 弱披与弱纹透明度
const VEIL_ALPHA = 0.2;
const WEAK_ALPHA = 0.35;
const PARTIAL_ALPHA = 0.12;
const RAMP_ALPHA_MIN = 0.04;
const RAMP_ALPHA_MAX = 0.86;
// 轴刻度数与 bp 标尺档
const AXIS_TICKS = 5;
const BP_RULER: readonly number[] = [-25, -10, 10, 25];
const BP_FACTOR = 10000;
// 轮廓角括边长
const BRACKET_LEN = 8;
// 强度查找表档数
const LUT_STEPS = 64;

export interface Rect {
  readonly x: number;
  readonly y: number;
  readonly w: number;
  readonly h: number;
}

export interface OflView {
  readonly fromS: number;
  readonly toS: number;
  readonly bucketS: number;
  readonly yMode: string;
  readonly colorMode: string;
}

export interface OflLayers {
  readonly printTicks: readonly PrintTickItem[];
  readonly features: readonly BookFeatureItem[];
  readonly alertFeatures: ReadonlySet<number>;
  readonly flashFeature: number | null;
  readonly selected: BookFeatureItem | null;
  readonly orderbook: OrderbookResponse | null;
}

export interface EventHit {
  readonly rect: Rect;
  readonly feature: BookFeatureItem;
}

export interface BandHit {
  readonly key: string;
  readonly label: string;
  readonly rect: Rect;
}

export interface TickHit {
  readonly x: number;
  readonly y: number;
  readonly item: PrintTickItem;
}

export interface OflPaintResult {
  readonly plot: Rect;
  readonly yLow: number;
  readonly yHigh: number;
  readonly rowBin: number;
  readonly events: readonly EventHit[];
  readonly bands: readonly BandHit[];
  readonly ticks: readonly TickHit[];
}

export const Y_MODE_ABS = "abs";
export const Y_MODE_BP = "bp";
export const COLOR_LINEAR = "linear";
export const COLOR_LOG = "log";
export const COLOR_PCT = "pct";

/** ISO 时刻转秒时戳，非法返回空。 */
export function epochOf(iso: string | null | undefined): number | null {
  const date = parseUtcIso(iso);
  return date === null ? null : Math.floor(date.getTime() / 1000);
}

interface Palette {
  readonly base: RgbaChannels;
  readonly panel: RgbaChannels;
  readonly elevated: RgbaChannels;
  readonly border: RgbaChannels;
  readonly focus: RgbaChannels;
  readonly muted: RgbaChannels;
  readonly secondary: RgbaChannels;
  readonly pos: RgbaChannels;
  readonly neg: RgbaChannels;
  readonly disabled: RgbaChannels;
  readonly warning: RgbaChannels;
  readonly info: RgbaChannels;
}

let paletteCache: Palette | null = null;

/** 调色板一律经静态 token 通道读取。 */
function palette(): Palette {
  if (paletteCache !== null) {
    return paletteCache;
  }
  paletteCache = {
    base: chartChannels("--background-base"),
    panel: chartChannels("--background-panel"),
    elevated: chartChannels("--background-elevated"),
    border: chartChannels("--border-default"),
    focus: chartChannels("--border-focus"),
    muted: chartChannels("--text-muted"),
    secondary: chartChannels("--text-secondary"),
    pos: chartChannels("--state-positive"),
    neg: chartChannels("--state-negative"),
    disabled: chartChannels("--state-disabled"),
    warning: chartChannels("--state-warning"),
    info: chartChannels("--state-info"),
  };
  return paletteCache;
}

function rgba(color: RgbaChannels, alpha: number): string {
  return `rgba(${String(color.r)}, ${String(color.g)}, ${String(color.b)}, ${String(alpha)})`;
}

// 空档纹理图样缓存，键为缩放与透明度
const hatchCache = new Map<string, CanvasPattern | null>();

/** 空档纹理图样：斜线区别于零挂量的黑。 */
function hatchPattern(
  context: CanvasRenderingContext2D,
  ink: RgbaChannels,
  scale: number,
  alpha: number,
): CanvasPattern | null {
  const key = [scale, alpha, ink.r, ink.g, ink.b].join("|");
  const held = hatchCache.get(key);
  if (held !== undefined) {
    return held;
  }
  const tile = document.createElement("canvas");
  const step = Math.max(4, Math.round(HATCH_STEP * scale));
  tile.width = step;
  tile.height = step;
  const tileContext = tile.getContext("2d");
  if (tileContext === null) {
    return null;
  }
  tileContext.strokeStyle = rgba(ink, alpha);
  tileContext.lineWidth = scale;
  tileContext.beginPath();
  tileContext.moveTo(0, step);
  tileContext.lineTo(step, 0);
  tileContext.stroke();
  const made = context.createPattern(tile, "repeat");
  hatchCache.set(key, made);
  return made;
}

interface CellShape {
  readonly price: number;
  readonly bin: string;
  readonly side: string;
  readonly qty: number;
}

// 列格值形态缓存，装配不变即复用
const shapeCache = new WeakMap<TileColumn, CellShape[]>();

/** 列格值转数值形态，供绘制与命中。 */
function cellShapes(column: TileColumn): CellShape[] {
  const held = shapeCache.get(column);
  if (held !== undefined) {
    return held;
  }
  const out: CellShape[] = [];
  for (const cell of column.cells) {
    const price = plotNumber(cell[0]);
    const qty = plotNumber(cell[2]);
    if (price === null || qty === null || qty <= 0) {
      continue;
    }
    out.push({ price, bin: cell[0] ?? "", side: cell[1] ?? "", qty });
  }
  shapeCache.set(column, out);
  return out;
}

/**
 * 强度标尺：数据或色模变化时预计算查找表，
 * 逐格绘制只做档位索引，杜绝逐帧排序。
 */
export interface IntensityScale {
  readonly colorMode: string;
  readonly peak: number;
  readonly logPeak: number;
  readonly sorted: readonly number[];
  readonly askStyles: readonly string[];
  readonly bidStyles: readonly string[];
  readonly bothStyles: readonly string[];
}

/** 构建强度标尺（调用方按数据与色模记忆化）。 */
export function buildScale(
  fine: readonly TileColumn[],
  base: readonly TileColumn[],
  colorMode: string,
  fineSpanS = 1,
  baseSpanS = 60,
  fromS = Number.NEGATIVE_INFINITY,
  toS = Number.POSITIVE_INFINITY,
): IntensityScale {
  const values: number[] = [];
  let peak = 0;
  const feed = (columns: readonly TileColumn[]): void => {
    for (const column of columns) {
      if (column.gap) {
        continue;
      }
      for (const shape of cellShapes(column)) {
        peak = Math.max(peak, shape.qty);
        if (colorMode === COLOR_PCT) {
          values.push(shape.qty);
        }
      }
    }
  };
  feed(fine);
  const missing = missingRanges(fine, fromS, toS, fineSpanS);
  feed(
    base.filter((column) =>
      missing.some(
        ([low, high]) => column.e + baseSpanS > low && column.e < high,
      ),
    ),
  );
  if (colorMode === COLOR_PCT) {
    values.sort((left, right) => left - right);
  }
  const colors = palette();
  const ramp = (ink: RgbaChannels): string[] => {
    const out: string[] = [];
    for (let at = 0; at < LUT_STEPS; at += 1) {
      const ratio = at / Math.max(1, LUT_STEPS - 1);
      out.push(
        rgba(
          ink,
          RAMP_ALPHA_MIN + ratio * (RAMP_ALPHA_MAX - RAMP_ALPHA_MIN),
        ),
      );
    }
    return out;
  };
  return {
    colorMode,
    peak,
    logPeak: Math.log1p(peak),
    sorted: values,
    askStyles: ramp(colors.neg),
    bidStyles: ramp(colors.pos),
    bothStyles: ramp(colors.secondary),
  };
}

/** 强度档位：线性、对数（带伽马）、百分位三模。 */
function stepOf(value: number, scale: IntensityScale): number {
  if (value <= 0 || scale.peak <= 0) {
    return -1;
  }
  let level: number;
  if (scale.colorMode === COLOR_PCT) {
    let low = 0;
    let high = scale.sorted.length;
    while (low < high) {
      const middle = (low + high) >> 1;
      if ((scale.sorted[middle] ?? 0) <= value) {
        low = middle + 1;
      } else {
        high = middle;
      }
    }
    level = scale.sorted.length === 0 ? 0 : low / scale.sorted.length;
  } else if (scale.colorMode === COLOR_LOG) {
    const ratio = scale.logPeak <= 0 ? 0 : Math.log1p(value) / scale.logPeak;
    level = Math.pow(Math.min(1, ratio), LOG_GAMMA);
  } else {
    level = Math.min(1, value / scale.peak);
  }
  if (level <= 0) {
    return -1;
  }
  return Math.min(LUT_STEPS - 1, Math.floor(level * LUT_STEPS));
}

function sideStyles(side: string, scale: IntensityScale): readonly string[] {
  if (side === "ask") {
    return scale.askStyles;
  }
  if (side === "bid") {
    return scale.bidStyles;
  }
  return scale.bothStyles;
}

interface BandSpec {
  readonly key: string;
  readonly label: string;
  readonly height: number;
}

const BAND_SPECS: readonly BandSpec[] = [
  { key: "event", label: "EVENT", height: EVENT_BAND_HEIGHT },
  { key: "spread", label: "SPREAD BP", height: BAND_HEIGHT },
  { key: "ofi", label: "OFI", height: BAND_HEIGHT },
  { key: "imbalance", label: "IMBALANCE", height: BAND_HEIGHT },
  { key: "delta", label: "TRADE DELTA", height: BAND_HEIGHT },
  {
    key: "depth",
    label: "DEPTH 5/10/25BP · 斜纹=部分覆盖",
    height: DEPTH_BAND_HEIGHT,
  },
];

/** 五带加事件带的总高，供布局预留。 */
export function bandsHeight(): number {
  let total = TIME_STRIP_HEIGHT;
  for (const spec of BAND_SPECS) {
    total += spec.height;
  }
  return total;
}

interface Series {
  readonly at: number;
  readonly epoch: number;
  readonly value: number;
}

function bandSeries(
  columns: readonly TileColumn[],
  pick: (column: TileColumn) => string | null,
): Series[] {
  const out: Series[] = [];
  columns.forEach((column, at) => {
    if (column.gap) {
      return;
    }
    const text = pick(column);
    if (text === null) {
      return;
    }
    const value = plotNumber(text);
    if (value !== null) {
      out.push({ at, epoch: column.e, value });
    }
  });
  return out;
}

/** 折线带：单值序列，空值断开。 */
function paintLineBand(
  context: CanvasRenderingContext2D,
  rect: Rect,
  series: readonly Series[],
  xOf: (at: number) => number,
  ink: string,
  scale: number,
  bucketS: number,
): void {
  if (series.length === 0) {
    return;
  }
  let low = Number.POSITIVE_INFINITY;
  let high = Number.NEGATIVE_INFINITY;
  for (const point of series) {
    low = Math.min(low, point.value);
    high = Math.max(high, point.value);
  }
  if (high === low) {
    high = low + 1;
  }
  const pad = LABEL_PAD * scale;
  const yOf = (value: number): number =>
    rect.y + pad + ((high - value) / (high - low)) * (rect.h - pad * HALF);
  context.strokeStyle = ink;
  context.lineWidth = scale;
  context.beginPath();
  let previousAt = -2;
  let previousEpoch: number | null = null;
  for (const point of series) {
    const x = xOf(point.at);
    const y = yOf(point.value);
    if (
      point.at === previousAt + 1 &&
      previousEpoch !== null &&
      point.epoch === previousEpoch + bucketS
    ) {
      context.lineTo(x, y);
    } else {
      context.moveTo(x, y);
    }
    previousAt = point.at;
    previousEpoch = point.epoch;
  }
  context.stroke();
}

/** 柱带：正负分色，零线居中。 */
function paintBarBand(
  context: CanvasRenderingContext2D,
  rect: Rect,
  series: readonly Series[],
  xOf: (at: number) => number,
  columnWidth: number,
  colors: Palette,
  scale: number,
): void {
  let peak = 0;
  for (const point of series) {
    peak = Math.max(peak, Math.abs(point.value));
  }
  const zero = rect.y + rect.h / HALF;
  context.strokeStyle = rgba(colors.border, 1);
  context.lineWidth = scale;
  context.beginPath();
  context.moveTo(rect.x, zero);
  context.lineTo(rect.x + rect.w, zero);
  context.stroke();
  if (peak <= 0) {
    return;
  }
  const span = rect.h / HALF - LABEL_PAD * scale;
  const width = Math.max(scale, columnWidth - scale);
  for (const point of series) {
    if (point.value === 0) {
      continue;
    }
    const size = (Math.abs(point.value) / peak) * span;
    const ink = point.value > 0 ? colors.pos : colors.neg;
    context.fillStyle = rgba(ink, WEAK_ALPHA);
    const x = xOf(point.at) - width / HALF;
    if (point.value > 0) {
      context.fillRect(x, zero - size, width, size);
    } else {
      context.fillRect(x, zero, width, size);
    }
  }
}

/** 深度带：三带宽折线，部分覆盖弱纹。 */
function paintDepthBand(
  context: CanvasRenderingContext2D,
  rect: Rect,
  columns: readonly TileColumn[],
  xOf: (at: number) => number,
  columnWidth: number,
  colors: Palette,
  scale: number,
  notional: boolean,
): void {
  const bandAlphas: readonly number[] = [0.92, 0.56, 0.3];
  const partial: number[] = [];
  interface DepthPoint {
    readonly at: number;
    readonly values: (number | null)[];
  }
  const points: DepthPoint[] = [];
  let peak = 0;
  columns.forEach((column, at) => {
    const depth = column.bands.depth;
    if (depth.length === 0) {
      return;
    }
    const values: (number | null)[] = [];
    let anyPartial = false;
    for (const entry of depth) {
      // 金额基准取第四元，旧瓦片回退数量
      const value = plotNumber(
        notional ? (entry[3] ?? entry[1]) : entry[1],
      );
      values.push(value);
      if (value !== null) {
        peak = Math.max(peak, value);
      }
      if (entry[2] === true) {
        anyPartial = true;
      }
    }
    if (anyPartial) {
      partial.push(at);
    }
    points.push({ at, values });
  });
  if (peak <= 0) {
    return;
  }
  const pad = LABEL_PAD * scale;
  const yOf = (value: number): number =>
    rect.y + pad + ((peak - value) / peak) * (rect.h - pad * HALF);
  for (let band = 0; band < bandAlphas.length; band += 1) {
    context.strokeStyle = rgba(colors.info, bandAlphas[band] ?? 1);
    context.lineWidth = scale;
    context.beginPath();
    let previousAt = -2;
    for (const point of points) {
      const value = point.values[band];
      if (value === null || value === undefined) {
        previousAt = -2;
        continue;
      }
      const x = xOf(point.at);
      const y = yOf(value);
      if (point.at === previousAt + 1) {
        context.lineTo(x, y);
      } else {
        context.moveTo(x, y);
      }
      previousAt = point.at;
    }
    context.stroke();
  }
  // 部分覆盖列打弱纹
  const pattern = hatchPattern(context, colors.info, scale, PARTIAL_ALPHA);
  if (pattern !== null && partial.length > 0) {
    context.fillStyle = pattern;
    for (const at of partial) {
      context.fillRect(
        xOf(at) - columnWidth / HALF,
        rect.y,
        columnWidth,
        rect.h,
      );
    }
  }
}

export interface OflPaintInput {
  readonly view: OflView;
  readonly fine: readonly TileColumn[];
  readonly base: readonly TileColumn[];
  readonly baseSpanS: number;
  readonly rowBinText: string | null;
  readonly tickSize: string;
  readonly dataVersion: number;
  readonly lockDomain: { readonly low: number; readonly high: number } | null;
  readonly scale: IntensityScale;
  readonly basis: string;
  readonly layers: OflLayers;
}

interface PlotCache {
  key: string;
  fromS: number;
  toS: number;
  canvas: HTMLCanvasElement;
}

interface TargetState {
  buffer: HTMLCanvasElement;
  plot: PlotCache | null;
  scratch: HTMLCanvasElement;
}

// 双缓冲与位图缓存，按可见画布挂靠
const targetStates = new WeakMap<HTMLCanvasElement, TargetState>();

function targetState(target: HTMLCanvasElement): TargetState {
  const held = targetStates.get(target);
  if (held !== undefined) {
    return held;
  }
  const made: TargetState = {
    buffer: document.createElement("canvas"),
    plot: null,
    scratch: document.createElement("canvas"),
  };
  targetStates.set(target, made);
  return made;
}

interface Mapping {
  readonly fromS: number;
  readonly toS: number;
  readonly bucketS: number;
  readonly bp: boolean;
  readonly low: number;
  readonly high: number;
  readonly rowBin: number;
  readonly widthPx: number;
  readonly heightPx: number;
}

/** 值域：锁定优先，否则窗内格值推导。 */
function domainOf(
  input: OflPaintInput,
  bp: boolean,
): { low: number; high: number; meaningful: boolean } {
  if (input.lockDomain !== null) {
    return { ...input.lockDomain, meaningful: true };
  }
  const rowBin = plotNumber(input.rowBinText) ?? 0;
  let low = Number.POSITIVE_INFINITY;
  let high = Number.NEGATIVE_INFINITY;
  let observed = false;
  const feed = (columns: readonly TileColumn[]): void => {
    for (const column of columns) {
      if (column.e < input.view.fromS || column.e >= input.view.toS) {
        continue;
      }
      const mid = plotNumber(column.mid);
      if (mid !== null && mid > 0) {
        const middle = bp ? 0 : mid;
        low = Math.min(low, middle);
        high = Math.max(high, middle);
        observed = true;
      }
      for (const shape of cellShapes(column)) {
        let top = shape.price + rowBin;
        let bottom = shape.price;
        if (bp) {
          if (mid === null || mid <= 0) {
            continue;
          }
          top = ((shape.price + rowBin) / mid - 1) * BP_FACTOR;
          bottom = (shape.price / mid - 1) * BP_FACTOR;
        }
        low = Math.min(low, bottom);
        high = Math.max(high, top);
        observed = true;
      }
    }
  };
  feed(input.fine);
  const missing = missingRanges(
    input.fine,
    input.view.fromS,
    input.view.toS,
    input.view.bucketS,
  );
  feed(
    input.base.filter((column) =>
      missing.some(
        ([rangeLow, rangeHigh]) =>
          column.e + input.baseSpanS > rangeLow && column.e < rangeHigh,
      ),
    ),
  );
  const book = input.layers.orderbook;
  const bookMid = plotNumber(book?.mid);
  if (book !== null) {
    for (const level of [...book.asks, ...book.bids]) {
      const price = plotNumber(level.price);
      if (price === null) {
        continue;
      }
      const value =
        bp && bookMid !== null && bookMid > 0
          ? (price / bookMid - 1) * BP_FACTOR
          : price;
      low = Math.min(low, value);
      high = Math.max(high, value);
      observed = true;
    }
  }
  if (!Number.isFinite(low) || !Number.isFinite(high) || high <= low) {
    if (observed && Number.isFinite(low)) {
      const pad = Math.max(Math.abs(rowBin), Math.abs(low) * 0.0001, 1);
      return { low: low - pad, high: low + pad, meaningful: true };
    }
    return { low: 0, high: 1, meaningful: false };
  }
  return { low, high, meaningful: true };
}

/** 细层未覆盖的时间段，供金字塔底层显现。 */
function missingRanges(
  fine: readonly TileColumn[],
  fromS: number,
  toS: number,
  bucketS: number,
): [number, number][] {
  const out: [number, number][] = [];
  let cursor = fromS;
  for (const column of fine) {
    if (column.e >= toS) {
      break;
    }
    if (column.e > cursor) {
      out.push([cursor, column.e]);
    }
    cursor = Math.max(cursor, column.e + bucketS);
  }
  if (cursor < toS) {
    out.push([cursor, toS]);
  }
  return out;
}

/** 时戳二分定位所属细列。 */
function columnAtEpoch(
  columns: readonly TileColumn[],
  epoch: number,
  bucketS: number,
): TileColumn | null {
  let low = 0;
  let high = columns.length;
  while (low < high) {
    const middle = (low + high) >> 1;
    if ((columns[middle]?.e ?? Number.POSITIVE_INFINITY) <= epoch) {
      low = middle + 1;
    } else {
      high = middle;
    }
  }
  const found = columns[low - 1];
  return found !== undefined && epoch < found.e + bucketS ? found : null;
}

/** 绘制单列格值到指定画布（列宽由跨度定）。 */
function paintCells(
  context: CanvasRenderingContext2D,
  column: TileColumn,
  spanS: number,
  map: Mapping,
  scale: IntensityScale,
  colors: Palette,
  device: number,
): void {
  const pxPerSec = map.widthPx / (map.toS - map.fromS);
  const x0 = (column.e - map.fromS) * pxPerSec;
  const width = Math.max(device, spanS * pxPerSec);
  if (column.gap) {
    const fill = hatchPattern(context, colors.disabled, device, 1);
    if (fill !== null) {
      context.fillStyle = fill;
      context.fillRect(x0, 0, width, map.heightPx);
    }
    return;
  }
  const mid = plotNumber(column.mid);
  const yOf = (value: number): number =>
    ((map.high - value) / (map.high - map.low)) * map.heightPx;
  for (const shape of cellShapes(column)) {
    let top = shape.price + map.rowBin;
    let bottom = shape.price;
    if (map.bp) {
      if (mid === null || mid <= 0) {
        continue;
      }
      top = ((shape.price + map.rowBin) / mid - 1) * BP_FACTOR;
      bottom = (shape.price / mid - 1) * BP_FACTOR;
    }
    const step = stepOf(shape.qty, scale);
    if (step < 0) {
      continue;
    }
    const styles = sideStyles(shape.side, scale);
    context.fillStyle = styles[step] ?? styles[styles.length - 1] ?? "";
    const yTop = yOf(top);
    context.fillRect(
      x0,
      yTop,
      width,
      Math.max(device, yOf(bottom) - yTop),
    );
  }
  if (column.carried) {
    // 延载列弱披
    context.fillStyle = rgba(colors.disabled, VEIL_ALPHA);
    context.fillRect(x0, 0, width, map.heightPx);
  }
}

/** 绘制一段窗口的格值层与中间价线与刻线。 */
function paintPlotRange(
  context: CanvasRenderingContext2D,
  input: OflPaintInput,
  map: Mapping,
  colors: Palette,
  device: number,
  rangeFromS: number,
  rangeToS: number,
): void {
  const pxPerSec = map.widthPx / (map.toS - map.fromS);
  // 金字塔底：细层缺段先铺 1min 列
  const missing = missingRanges(
    input.fine,
    Math.max(map.fromS, rangeFromS),
    Math.min(map.toS, rangeToS),
    map.bucketS,
  );
  const implicitPattern = hatchPattern(
    context,
    colors.disabled,
    device,
    WEAK_ALPHA,
  );
  for (const [gapFrom, gapTo] of missing) {
    const covered: [number, number][] = [];
    for (const column of input.base) {
      if (column.e + input.baseSpanS <= gapFrom || column.e >= gapTo) {
        continue;
      }
      covered.push([
        Math.max(gapFrom, column.e),
        Math.min(gapTo, column.e + input.baseSpanS),
      ]);
      paintCells(
        context, column, input.baseSpanS, map, input.scale, colors, device,
      );
    }
    if (implicitPattern !== null) {
      context.fillStyle = implicitPattern;
      for (const [emptyFrom, emptyTo] of uncoveredRanges(
        gapFrom,
        gapTo,
        covered,
      )) {
        const x0 = (emptyFrom - map.fromS) * pxPerSec;
        context.fillRect(
          x0,
          0,
          Math.max(device, (emptyTo - emptyFrom) * pxPerSec),
          map.heightPx,
        );
      }
    }
  }
  for (const column of input.fine) {
    if (column.e + map.bucketS <= rangeFromS || column.e >= rangeToS) {
      continue;
    }
    paintCells(
      context, column, map.bucketS, map, input.scale, colors, device,
    );
  }
  // 中间价点线，空档断开
  const yOf = (value: number): number =>
    ((map.high - value) / (map.high - map.low)) * map.heightPx;
  context.strokeStyle = channelFill(colors.secondary);
  context.lineWidth = device;
  context.beginPath();
  let open = false;
  for (const column of input.fine) {
    if (column.e + map.bucketS <= rangeFromS - map.bucketS) {
      continue;
    }
    if (column.e >= rangeToS + map.bucketS) {
      break;
    }
    const mid = plotNumber(column.mid);
    if (mid === null) {
      open = false;
      continue;
    }
    const x = (column.e + map.bucketS / HALF - map.fromS) * pxPerSec;
    const y = map.bp ? yOf(0) : yOf(mid);
    if (open) {
      context.lineTo(x, y);
    } else {
      context.moveTo(x, y);
    }
    open = true;
  }
  context.stroke();
  // 成交刻线：长度取量分位，色取吃单方向
  for (const tick of input.layers.printTicks) {
    const epoch = epochOf(tick.t);
    const price = plotNumber(tick.price);
    const rank = plotNumber(tick.size_quantile) ?? 0;
    if (epoch === null || price === null) {
      continue;
    }
    if (epoch < rangeFromS || epoch >= rangeToS) {
      continue;
    }
    let value = price;
    if (map.bp) {
      const column = columnAtEpoch(input.fine, epoch, map.bucketS);
      const mid = column === null ? null : plotNumber(column.mid);
      if (mid === null || mid <= 0) {
        continue;
      }
      value = (price / mid - 1) * BP_FACTOR;
    }
    const y = yOf(value);
    const x = (epoch - map.fromS) * pxPerSec;
    const length =
      (PRINT_TICK_MIN + Math.max(0, rank - 0.95) * (PRINT_TICK_SPAN / 0.05)) *
      device;
    context.strokeStyle = channelFill(
      tick.side === "BUY" ? colors.pos : colors.neg,
    );
    context.lineWidth = HALF * device;
    context.beginPath();
    context.moveTo(x - length / HALF, y);
    context.lineTo(x + length / HALF, y);
    context.stroke();
  }
}

/** 位图层：整窗渲染或平移复用只补边缘。 */
function plotBitmap(
  state: TargetState,
  input: OflPaintInput,
  map: Mapping,
  colors: Palette,
  device: number,
  printsVersion: number,
): HTMLCanvasElement {
  const key = [
    String(input.dataVersion),
    String(printsVersion),
    input.view.colorMode,
    input.view.yMode,
    String(map.bucketS),
    String(map.toS - map.fromS),
    map.low.toFixed(6),
    map.high.toFixed(6),
    String(Math.round(map.widthPx)),
    String(Math.round(map.heightPx)),
  ].join("|");
  const held = state.plot;
  const fresh =
    held === null ||
    held.key !== key ||
    held.canvas.width !== Math.round(map.widthPx) ||
    held.canvas.height !== Math.round(map.heightPx);
  // 双位图轮换，避免逐帧新建画布
  const canvas = state.scratch;
  const width = Math.max(1, Math.round(map.widthPx));
  const height = Math.max(1, Math.round(map.heightPx));
  if (canvas.width !== width || canvas.height !== height) {
    canvas.width = width;
    canvas.height = height;
  }
  const context = canvas.getContext("2d");
  if (context === null) {
    return canvas;
  }
  context.fillStyle = channelFill(colors.base);
  context.fillRect(0, 0, canvas.width, canvas.height);
  const pxPerSec = map.widthPx / (map.toS - map.fromS);
  if (!fresh && held !== null && held.fromS !== map.fromS) {
    // 平移：位图整段搬移，边缘补画
    const shift = (held.fromS - map.fromS) * pxPerSec;
    context.drawImage(held.canvas, shift, 0);
    if (map.fromS < held.fromS) {
      paintPlotRange(
        context, input, map, colors, device,
        map.fromS, Math.min(held.fromS, map.toS),
      );
    }
    if (map.toS > held.toS) {
      paintPlotRange(
        context, input, map, colors, device,
        Math.max(held.toS, map.fromS), map.toS,
      );
    }
  } else if (!fresh && held !== null) {
    // 同窗同键：位图原样复用
    context.drawImage(held.canvas, 0, 0);
  } else {
    paintPlotRange(
      context, input, map, colors, device, map.fromS, map.toS,
    );
  }
  state.scratch =
    held === null ? document.createElement("canvas") : held.canvas;
  state.plot = { key, fromS: map.fromS, toS: map.toS, canvas };
  return canvas;
}

const printVersions = new WeakMap<readonly PrintTickItem[], number>();
let printVersionCounter = 0;

/** 逐笔数组内容稳定时复用位图代次。 */
function printVersionOf(items: readonly PrintTickItem[]): number {
  const held = printVersions.get(items);
  if (held !== undefined) {
    return held;
  }
  printVersionCounter += 1;
  printVersions.set(items, printVersionCounter);
  return printVersionCounter;
}

/**
 * 订单流主画布整幅重绘（双缓冲单次换页）。
 * 主热力图、右缘当前盘口列、底部五带加事件带同 canvas 共享 x。
 * 返回几何与命中区，供指针交互反查；hover 不经此路径。
 */
export function paintOfl(
  target: HTMLCanvasElement,
  wrap: HTMLElement,
  input: OflPaintInput,
): OflPaintResult | null {
  const cssWidth = wrap.clientWidth;
  const cssHeight = wrap.clientHeight;
  if (cssWidth <= 0 || cssHeight <= 0) {
    return null;
  }
  const view = input.view;
  const device = window.devicePixelRatio;
  const state = targetState(target);
  const buffer = state.buffer;
  const deviceW = Math.max(1, Math.round(cssWidth * device));
  const deviceH = Math.max(1, Math.round(cssHeight * device));
  if (buffer.width !== deviceW || buffer.height !== deviceH) {
    buffer.width = deviceW;
    buffer.height = deviceH;
  }
  const context = buffer.getContext("2d");
  if (context === null) {
    return null;
  }
  const colors = palette();
  context.fillStyle = channelFill(colors.base);
  context.fillRect(0, 0, deviceW, deviceH);
  const font = tokenSize("--fs-badge") * device;
  const mono = tokenValue("--font-mono");
  context.font = `${String(font)}px ${mono}`;

  const rowBin = plotNumber(input.rowBinText) ?? 0;
  const bp = view.yMode === Y_MODE_BP;
  const domain = domainOf(input, bp);
  const low = domain.low;
  const high = domain.high;
  const labels: string[] = [];
  if (domain.meaningful) {
    for (let at = 0; at < AXIS_TICKS; at += 1) {
      const value = high - ((high - low) * at) / (AXIS_TICKS - 1);
      labels.push(
        bp
          ? `${value.toFixed(1)}bp`
          : priceText(value.toFixed(0), input.tickSize),
      );
    }
  }
  let axisWidth = 0;
  for (const text of labels) {
    axisWidth = Math.max(axisWidth, context.measureText(text).width);
  }
  axisWidth = labels.length === 0 ? 0 : axisWidth / device + AXIS_PAD * HALF;

  const bandsTotal = bandsHeight();
  const plot: Rect = {
    x: 0,
    y: 0,
    w: Math.max(1, cssWidth - BOOK_COL_WIDTH - axisWidth),
    h: Math.max(1, cssHeight - bandsTotal),
  };
  const spanS = Math.max(1, view.toS - view.fromS);
  const xOfEpoch = (epoch: number): number =>
    (plot.x + ((epoch - view.fromS) / spanS) * plot.w) * device;
  const columnWidth = ((view.bucketS / spanS) * plot.w) * device;
  const yOfValue = (value: number): number =>
    (plot.y + ((high - value) / (high - low)) * plot.h) * device;
  const plotDevice: Rect = {
    x: plot.x * device,
    y: plot.y * device,
    w: plot.w * device,
    h: plot.h * device,
  };

  const map: Mapping = {
    fromS: view.fromS,
    toS: view.toS,
    bucketS: view.bucketS,
    bp,
    low,
    high,
    rowBin,
    widthPx: plotDevice.w,
    heightPx: plotDevice.h,
  };
  const printsVersion = printVersionOf(input.layers.printTicks);
  const bitmap = plotBitmap(
    state, input, map, colors, device, printsVersion,
  );
  context.drawImage(bitmap, plotDevice.x, plotDevice.y);
  const fineRecorded = input.fine.some(
    (column) =>
      !column.gap && column.e < view.toS && column.e + view.bucketS > view.fromS,
  );
  const baseRecorded = input.base.some(
    (column) =>
      !column.gap &&
      column.e < view.toS &&
      column.e + input.baseSpanS > view.fromS,
  );
  const recordedInView = fineRecorded || baseRecorded;
  if (!recordedInView) {
    const fill = hatchPattern(context, colors.disabled, device, WEAK_ALPHA);
    if (fill !== null) {
      context.fillStyle = fill;
      context.fillRect(
        plotDevice.x,
        plotDevice.y,
        plotDevice.w,
        plotDevice.h,
      );
    }
  }

  // 就地轮廓：选中事件角括描边
  const selected = input.layers.selected;
  const ticks: TickHit[] = [];
  for (const tick of input.layers.printTicks) {
    const epoch = epochOf(tick.t);
    const price = plotNumber(tick.price);
    if (epoch === null || price === null) {
      continue;
    }
    if (epoch < view.fromS || epoch >= view.toS) {
      continue;
    }
    let value = price;
    if (bp) {
      const column = columnAtEpoch(input.fine, epoch, view.bucketS);
      const mid = column === null ? null : plotNumber(column.mid);
      if (mid === null || mid <= 0) {
        continue;
      }
      value = (price / mid - 1) * BP_FACTOR;
    }
    ticks.push({
      x: xOfEpoch(epoch) / device,
      y: yOfValue(value) / device,
      item: tick,
    });
  }
  if (selected !== null && !bp) {
    const fromE = epochOf(selected.from_ts);
    const toE = epochOf(selected.to_ts);
    const lowPrice = plotNumber(selected.price_low);
    const highPrice = plotNumber(selected.price_high);
    if (
      fromE !== null &&
      toE !== null &&
      lowPrice !== null &&
      highPrice !== null
    ) {
      context.save();
      context.beginPath();
      context.rect(plotDevice.x, plotDevice.y, plotDevice.w, plotDevice.h);
      context.clip();
      const x0 = xOfEpoch(fromE);
      const x1 = xOfEpoch(toE);
      const y0 = yOfValue(highPrice + rowBin);
      const y1 = yOfValue(lowPrice);
      const heavy = input.layers.flashFeature === selected.feature_id;
      context.strokeStyle = channelFill(colors.focus);
      context.lineWidth = (heavy ? HALF + 1 : 1) * device;
      const arm = BRACKET_LEN * device;
      context.beginPath();
      for (const [cx, cy, dx, dy] of [
        [x0, y0, 1, 1],
        [x1, y0, -1, 1],
        [x0, y1, 1, -1],
        [x1, y1, -1, -1],
      ] as const) {
        context.moveTo(cx + dx * arm, cy);
        context.lineTo(cx, cy);
        context.lineTo(cx, cy + dy * arm);
      }
      context.stroke();
      context.restore();
    }
  }

  // 右缘当前盘口列：与价格轴逐像素对齐
  const bookRect: Rect = {
    x: plot.x + plot.w,
    y: plot.y,
    w: BOOK_COL_WIDTH,
    h: plot.h,
  };
  context.strokeStyle = channelFill(colors.border);
  context.lineWidth = device;
  context.beginPath();
  context.moveTo(bookRect.x * device, plotDevice.y);
  context.lineTo(bookRect.x * device, plotDevice.y + plotDevice.h);
  context.stroke();
  const book = input.layers.orderbook;
  if (book !== null) {
    let bookPeak = 0;
    for (const level of [...book.asks, ...book.bids]) {
      bookPeak = Math.max(bookPeak, plotNumber(level.size) ?? 0);
    }
    const bookMid = plotNumber(book.mid);
    if (bookPeak > 0) {
      const rowPx = Math.max(
        device,
        bp && bookMid !== null && bookMid > 0
          ? Math.abs(
              yOfValue(0) - yOfValue((rowBin / bookMid) * BP_FACTOR),
            )
          : Math.abs(yOfValue(0) - yOfValue(rowBin)),
      );
      const drawLevels = (
        levels: readonly { price: string; size: string }[],
        ink: RgbaChannels,
      ): void => {
        for (const level of levels) {
          const price = plotNumber(level.price);
          const size = plotNumber(level.size);
          if (price === null || size === null || size <= 0) {
            continue;
          }
          let value = price;
          if (bp) {
            if (bookMid === null || bookMid <= 0) {
              continue;
            }
            value = (price / bookMid - 1) * BP_FACTOR;
          }
          const y = yOfValue(value);
          if (y < plotDevice.y || y > plotDevice.y + plotDevice.h) {
            continue;
          }
          const length =
            Math.max(0.08, size / bookPeak) * (BOOK_COL_WIDTH - AXIS_PAD) *
            device;
          context.fillStyle = rgba(ink, 0.7);
          context.fillRect(
            (bookRect.x + HALF) * device,
            y - rowPx / HALF,
            length,
            Math.max(device, rowPx - device),
          );
        }
      };
      drawLevels(book.asks, colors.neg);
      drawLevels(book.bids, colors.pos);
    }
    context.fillStyle = channelFill(colors.muted);
    context.textAlign = "left";
    context.textBaseline = "top";
    context.fillText(
      "当前盘口",
      (bookRect.x + HALF) * device,
      (bookRect.y + LABEL_PAD) * device,
    );
  }

  // 价格轴标签，绝对价附标尺
  const axisX = (bookRect.x + bookRect.w) * device;
  context.fillStyle = channelFill(colors.muted);
  context.textAlign = "left";
  context.textBaseline = "middle";
  labels.forEach((text, at) => {
    const ratio = at / (AXIS_TICKS - 1);
    const y = Math.min(
      plotDevice.y + plotDevice.h - font / HALF,
      Math.max(plotDevice.y + font / HALF, plotDevice.y + ratio * plotDevice.h),
    );
    context.fillText(text, axisX + AXIS_PAD * device, y);
  });
  if (domain.meaningful && !bp && book !== null) {
    const bookMid = plotNumber(book.mid);
    if (bookMid !== null && bookMid > 0) {
      context.strokeStyle = channelFill(colors.border);
      context.fillStyle = channelFill(colors.muted);
      const smallFont = tokenSize("--fs-formula") * device;
      context.font = `${String(smallFont)}px ${mono}`;
      for (const step of BP_RULER) {
        const value = bookMid * (1 + step / BP_FACTOR);
        const y = yOfValue(value);
        if (y < plotDevice.y || y > plotDevice.y + plotDevice.h) {
          continue;
        }
        context.lineWidth = device;
        context.beginPath();
        context.moveTo(axisX, y);
        context.lineTo(axisX + LABEL_PAD * device, y);
        context.stroke();
        context.fillText(
          `${step > 0 ? "+" : ""}${String(step)}bp`,
          axisX + LABEL_PAD * HALF * device,
          y,
        );
      }
      context.font = `${String(font)}px ${mono}`;
    }
  }

  // 底部带组共享 x 变换
  const xOfIndex = (at: number): number => {
    const column = input.fine[at];
    return column === undefined
      ? plotDevice.x
      : xOfEpoch(column.e + view.bucketS / HALF);
  };
  let cursorY = plot.h;
  const events: EventHit[] = [];
  const bandRects: BandHit[] = [];
  const notional = input.basis === BASIS_NOTIONAL;
  for (const spec of BAND_SPECS) {
    // 量类两带随全局基准，标签声明单位
    const label =
      notional && (spec.key === "delta" || spec.key === "depth")
        ? `${spec.label} · JPY`
        : spec.label;
    const rect: Rect = {
      x: plot.x * device,
      y: cursorY * device,
      w: (plot.w + BOOK_COL_WIDTH) * device,
      h: spec.height * device,
    };
    bandRects.push({
      key: spec.key,
      label,
      rect: {
        x: plot.x,
        y: cursorY,
        w: plot.w + BOOK_COL_WIDTH,
        h: spec.height,
      },
    });
    context.strokeStyle = channelFill(colors.border);
    context.lineWidth = BAND_BORDER * device;
    context.beginPath();
    context.moveTo(rect.x, rect.y);
    context.lineTo(rect.x + rect.w, rect.y);
    context.stroke();
    if (spec.key === "event") {
      for (const feature of input.layers.features) {
        const fromE = epochOf(feature.from_ts);
        const toE = epochOf(feature.to_ts);
        if (fromE === null || toE === null || toE < view.fromS) {
          continue;
        }
        if (fromE > view.toS) {
          continue;
        }
        const x0 = xOfEpoch(Math.max(fromE, view.fromS));
        const x1 = xOfEpoch(Math.min(toE, view.toS));
        const width = Math.max(EVENT_MARK_MIN_WIDTH * device, x1 - x0);
        const alerted = input.layers.alertFeatures.has(feature.feature_id);
        const flashing = input.layers.flashFeature === feature.feature_id;
        const markHeight =
          (alerted ? EVENT_MARK_HEIGHT + HALF : EVENT_MARK_HEIGHT) * device;
        const y = rect.y + (rect.h - markHeight) / HALF;
        context.fillStyle = channelFill(
          flashing ? colors.warning : alerted ? colors.warning : colors.focus,
        );
        context.fillRect(x0, y, width, markHeight);
        if (flashing) {
          context.strokeStyle = channelFill(colors.warning);
          context.lineWidth = device;
          context.strokeRect(
            x0 - HALF * device,
            y - HALF * device,
            width + HALF * HALF * device,
            markHeight + HALF * HALF * device,
          );
        }
        events.push({
          rect: {
            x: x0 / device,
            y: rect.y / device,
            w: width / device,
            h: rect.h / device,
          },
          feature,
        });
      }
    } else if (spec.key === "spread") {
      paintLineBand(
        context,
        rect,
        bandSeries(input.fine, (column) => column.bands.spread_bp),
        xOfIndex,
        channelFill(colors.focus),
        device,
        view.bucketS,
      );
    } else if (spec.key === "ofi") {
      paintBarBand(
        context,
        rect,
        bandSeries(input.fine, (column) => column.bands.ofi),
        xOfIndex,
        columnWidth,
        colors,
        device,
      );
    } else if (spec.key === "imbalance") {
      paintBarBand(
        context,
        rect,
        bandSeries(input.fine, (column) => column.bands.imbalance),
        xOfIndex,
        columnWidth,
        colors,
        device,
      );
    } else if (spec.key === "delta") {
      paintBarBand(
        context,
        rect,
        bandSeries(input.fine, (column) =>
          notional
            ? (column.bands.trade_delta_notional ?? column.bands.trade_delta)
            : column.bands.trade_delta,
        ),
        xOfIndex,
        columnWidth,
        colors,
        device,
      );
    } else {
      paintDepthBand(
        context,
        rect,
        input.fine,
        xOfIndex,
        columnWidth,
        colors,
        device,
        notional,
      );
    }
    // 带名微标签压左上角
    context.fillStyle = channelFill(colors.muted);
    context.textAlign = "left";
    context.textBaseline = "top";
    context.fillText(
      label,
      rect.x + LABEL_PAD * device,
      rect.y + LABEL_PAD * device,
    );
    cursorY += spec.height;
  }

  // 时间轴：起止时刻两端
  const stampY = (cursorY + TIME_STRIP_HEIGHT / HALF) * device;
  context.fillStyle = channelFill(colors.muted);
  context.textBaseline = "middle";
  context.textAlign = "left";
  const fromIso = new Date(view.fromS * 1000).toISOString();
  const toIso = new Date(view.toS * 1000).toISOString();
  context.fillText(
    `${jstMonthDay(fromIso)} ${jstHourMinute(fromIso)}`,
    plotDevice.x + LABEL_PAD * device,
    stampY,
  );
  context.textAlign = "right";
  context.fillText(
    `${jstMonthDay(toIso)} ${jstHourMinute(toIso)}`,
    plotDevice.x + plotDevice.w - LABEL_PAD * device,
    stampY,
  );

  // 单次换页：可见画布仅接受整幅位图
  if (target.width !== deviceW || target.height !== deviceH) {
    target.width = deviceW;
    target.height = deviceH;
  }
  const visible = target.getContext("2d");
  if (visible !== null) {
    visible.drawImage(buffer, 0, 0);
  }

  return {
    plot,
    yLow: low,
    yHigh: high,
    rowBin,
    events,
    bands: bandRects,
    ticks,
  };
}

export interface NavPaintResult {
  readonly rect: Rect;
  readonly fromS: number;
  readonly toS: number;
}

/**
 * 导航条：绝对时间锚定的覆盖范围与视窗框。
 * 像素只是时间量的投影；拖动期间调用方传入
 * 覆盖快照冻结坐标系。双缓冲单次换页。
 */
export function paintNav(
  target: HTMLCanvasElement,
  wrap: HTMLElement,
  columns: readonly TileColumn[],
  navFromS: number,
  navToS: number,
  windowFromS: number,
  windowToS: number,
): NavPaintResult | null {
  const cssWidth = wrap.clientWidth;
  const cssHeight = wrap.clientHeight;
  if (cssWidth <= 0 || cssHeight <= 0) {
    return null;
  }
  const device = window.devicePixelRatio;
  const state = targetState(target);
  const buffer = state.buffer;
  const deviceW = Math.max(1, Math.round(cssWidth * device));
  const deviceH = Math.max(1, Math.round(cssHeight * device));
  if (buffer.width !== deviceW || buffer.height !== deviceH) {
    buffer.width = deviceW;
    buffer.height = deviceH;
  }
  const context = buffer.getContext("2d");
  if (context === null) {
    return null;
  }
  const colors = palette();
  context.fillStyle = channelFill(colors.panel);
  context.fillRect(0, 0, deviceW, deviceH);
  const span = Math.max(1, navToS - navFromS);
  const xOf = (epoch: number): number =>
    ((epoch - navFromS) / span) * deviceW;
  // 覆盖刻度：非空档列即有录制
  context.fillStyle = rgba(colors.secondary, 0.6);
  for (const column of columns) {
    if (column.gap) {
      continue;
    }
    const x0 = xOf(column.e);
    const x1 = xOf(column.e + 60);
    context.fillRect(x0, deviceH / HALF - device, x1 - x0, device * HALF);
  }
  // 视窗框：半透明填充加缘手柄
  const w0 = Math.max(0, xOf(windowFromS));
  const w1 = Math.min(deviceW, xOf(windowToS));
  context.fillStyle = rgba(colors.focus, 0.18);
  context.fillRect(w0, 0, Math.max(device, w1 - w0), deviceH);
  context.strokeStyle = channelFill(colors.focus);
  context.lineWidth = device;
  context.strokeRect(
    w0, device, Math.max(device, w1 - w0), deviceH - device * HALF,
  );
  context.fillStyle = channelFill(colors.focus);
  context.fillRect(w0, 0, device * HALF, deviceH);
  context.fillRect(w1 - device * HALF, 0, device * HALF, deviceH);
  if (target.width !== deviceW || target.height !== deviceH) {
    target.width = deviceW;
    target.height = deviceH;
  }
  const visible = target.getContext("2d");
  if (visible !== null) {
    visible.drawImage(buffer, 0, 0);
  }
  return {
    rect: { x: 0, y: 0, w: cssWidth, h: cssHeight },
    fromS: navFromS,
    toS: navToS,
  };
}
