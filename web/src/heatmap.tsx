import { memo, useEffect, useRef } from "react";
import type { ReactElement } from "react";
import type {
  HeatmapColumn,
  HeatmapResponse,
  OpsProcessesResponse,
} from "./api";
import { EMPTY_TEXT, jstHourMinute, plotNumber, priceText } from "./format";
import { EmptyBlock, ProcessStartButton } from "./panels";
import {
  MAX_CHANNEL,
  channelFill,
  chartChannels,
  tokenSize,
  tokenValue,
} from "./lwc";

// 像素四通道
const CHANNELS = 4;
const HALF = 2;
// 落点取格中心
const CELL_CENTER = 0.5;
// 中间价连线与点的尺寸
const MID_LINE_WIDTH = 1;
const MID_DOT_SIZE = 2;
// 伽马提亮中低挂量
const INTENSITY_GAMMA = 0.8;
// 价格刻度三个：上界、中点、下界
const AXIS_TICKS = 3;
const AXIS_PAD = 6;
const STRIP_PAD = 3;
const GAP_HATCH_STEP = 7;
const GAP_LABEL_MIN_WIDTH = 72;
const GAP_ALPHA = 0.55;

/** 是否存在已录制的列，全列空档即为否。 */
function hasRecorded(data: HeatmapResponse): boolean {
  return data.cols.some((column: HeatmapColumn) => !column.gap);
}

/** 末列是否空档，即录制当前静默。 */
function tailGap(data: HeatmapResponse): boolean {
  const last = data.cols[data.cols.length - 1];
  return last !== undefined && last.gap;
}

/** 强度合成一个通道：底色加双侧语义色按强度叠加。 */
function blend(
  base: number,
  askChannel: number,
  askLevel: number,
  bidChannel: number,
  bidLevel: number,
): number {
  const value = base + askChannel * askLevel + bidChannel * bidLevel;
  return Math.min(MAX_CHANNEL, Math.round(value));
}

interface ColumnSpan {
  readonly start: number;
  readonly end: number;
}

/** 空档列的连续区间，供整段绘制缺口纹理。 */
function gapSpans(columns: readonly HeatmapColumn[]): ColumnSpan[] {
  const spans: ColumnSpan[] = [];
  let start = -1;
  columns.forEach((column, index) => {
    if (column.gap && start < 0) {
      start = index;
    }
    if (!column.gap && start >= 0) {
      spans.push({ start, end: index });
      start = -1;
    }
  });
  if (start >= 0) {
    spans.push({ start, end: columns.length });
  }
  return spans;
}

/** 强度矩阵转像素：ask 进红通道、bid 进绿通道。 */
function toPixels(
  context: CanvasRenderingContext2D,
  data: HeatmapResponse,
): ImageData {
  const cols = data.cols.length;
  const rows = data.rows;
  const cells = context.createImageData(cols, rows);
  const base = chartChannels("--background-base");
  const ask = chartChannels("--state-negative");
  const bid = chartChannels("--state-positive");
  for (let col = 0; col < cols; col += 1) {
    const askColumn = data.ask[col];
    const bidColumn = data.bid[col];
    for (let row = 0; row < rows; row += 1) {
      // 伽马提亮中低挂量，不改零值
      const askLevel = Math.pow(
        (askColumn?.[row] ?? 0) / MAX_CHANNEL,
        INTENSITY_GAMMA,
      );
      const bidLevel = Math.pow(
        (bidColumn?.[row] ?? 0) / MAX_CHANNEL,
        INTENSITY_GAMMA,
      );
      const at = (row * cols + col) * CHANNELS;
      cells.data[at] = blend(base.r, ask.r, askLevel, bid.r, bidLevel);
      cells.data[at + 1] = blend(base.g, ask.g, askLevel, bid.g, bidLevel);
      cells.data[at + 2] = blend(base.b, ask.b, askLevel, bid.b, bidLevel);
      cells.data[at + 3] = MAX_CHANNEL;
    }
  }
  return cells;
}

function rgba(
  color: { readonly r: number; readonly g: number; readonly b: number },
  alpha: number,
): string {
  return `rgba(${String(color.r)}, ${String(color.g)}, ${String(color.b)}, ${String(alpha)})`;
}

/** 空档使用斜纹而非实色，避免被误读成某一档挂量。 */
function paintGaps(
  context: CanvasRenderingContext2D,
  data: HeatmapResponse,
  width: number,
  height: number,
  scale: number,
): void {
  const columnWidth = width / data.cols.length;
  const tile = document.createElement("canvas");
  const step = Math.max(4, Math.round(GAP_HATCH_STEP * scale));
  tile.width = step;
  tile.height = step;
  const tileContext = tile.getContext("2d");
  if (tileContext === null) {
    return;
  }
  const disabled = chartChannels("--state-disabled");
  tileContext.strokeStyle = rgba(disabled, GAP_ALPHA);
  tileContext.lineWidth = scale;
  tileContext.beginPath();
  tileContext.moveTo(0, step);
  tileContext.lineTo(step, 0);
  tileContext.stroke();
  const pattern = context.createPattern(tile, "repeat");
  if (pattern === null) {
    return;
  }
  for (const span of gapSpans(data.cols)) {
    const left = span.start * columnWidth;
    const spanWidth = (span.end - span.start) * columnWidth;
    context.fillStyle = pattern;
    context.fillRect(left, 0, spanWidth, height);
    if (spanWidth >= GAP_LABEL_MIN_WIDTH * scale) {
      context.fillStyle = channelFill(chartChannels("--text-muted"));
      context.textAlign = "center";
      context.textBaseline = "top";
      context.fillText("未录制", left + spanWidth / HALF, STRIP_PAD * scale);
    }
  }
}

/** 中间价灰点连线，空档处断开不插值。 */
function paintMid(
  context: CanvasRenderingContext2D,
  data: HeatmapResponse,
  width: number,
  height: number,
  scale: number,
): void {
  const columnWidth = width / data.cols.length;
  const rowHeight = height / data.rows;
  const stroke = channelFill(chartChannels("--text-secondary"));
  const dot = MID_DOT_SIZE * scale;
  const points: { x: number; y: number }[] = [];
  context.strokeStyle = stroke;
  context.lineWidth = MID_LINE_WIDTH * scale;
  context.beginPath();
  let open = false;
  for (let col = 0; col < data.cols.length; col += 1) {
    const row = data.mid_row[col];
    if (row === null || row === undefined) {
      // 空档断开，绝不插值
      open = false;
      continue;
    }
    const x = (col + CELL_CENTER) * columnWidth;
    const y = (row + CELL_CENTER) * rowHeight;
    if (open) {
      context.lineTo(x, y);
    } else {
      context.moveTo(x, y);
    }
    open = true;
    points.push({ x, y });
  }
  context.stroke();
  context.fillStyle = stroke;
  for (const point of points) {
    context.fillRect(point.x - dot / HALF, point.y - dot / HALF, dot, dot);
  }
}

/** 价格刻度文本：上界、中点、下界，走变换器千分位。 */
function axisLabels(data: HeatmapResponse, tickSize: string): string[] {
  const high = plotNumber(data.price_high);
  const low = plotNumber(data.price_low);
  if (high === null || low === null) {
    return [];
  }
  const out: string[] = [];
  for (let at = 0; at < AXIS_TICKS; at += 1) {
    // 刻度取值仅供绘图，不回流数据
    const value = high - ((high - low) * at) / (AXIS_TICKS - 1);
    out.push(priceText(value.toString(), tickSize));
  }
  return out;
}

/** 右缘价格刻度与左右下角起止时刻。 */
function paintAxes(
  context: CanvasRenderingContext2D,
  data: HeatmapResponse,
  labels: readonly string[],
  box: { plotWidth: number; plotHeight: number; height: number; font: number },
  scale: number,
): void {
  context.fillStyle = channelFill(chartChannels("--text-muted"));
  context.textBaseline = "middle";
  context.textAlign = "left";
  const half = box.font / HALF;
  labels.forEach((text, at) => {
    const ratio = at / (AXIS_TICKS - 1);
    const y = Math.min(
      box.plotHeight - half,
      Math.max(half, ratio * box.plotHeight),
    );
    context.fillText(text, box.plotWidth + AXIS_PAD * scale, y);
  });
  const bottom = box.height - STRIP_PAD * scale;
  context.textBaseline = "bottom";
  context.fillText(
    jstHourMinute(data.cols[0]?.t),
    STRIP_PAD * scale,
    bottom,
  );
  context.textAlign = "right";
  context.fillText(
    jstHourMinute(data.cols[data.cols.length - 1]?.t),
    box.plotWidth - STRIP_PAD * scale,
    bottom,
  );
}

/** 整幅重绘：底色、强度矩阵、空档竖带、中间价、轴刻度。 */
function paint(
  target: HTMLCanvasElement,
  wrap: HTMLElement,
  data: HeatmapResponse,
  tickSize: string,
): void {
  const width = wrap.clientWidth;
  const height = wrap.clientHeight;
  if (width <= 0 || height <= 0) {
    return;
  }
  const scale = window.devicePixelRatio;
  const pixelWidth = Math.max(1, Math.round(width * scale));
  const pixelHeight = Math.max(1, Math.round(height * scale));
  target.width = pixelWidth;
  target.height = pixelHeight;
  const context = target.getContext("2d");
  if (context === null) {
    return;
  }
  context.fillStyle = channelFill(chartChannels("--background-base"));
  context.fillRect(0, 0, pixelWidth, pixelHeight);
  if (data.cols.length === 0 || data.rows === 0) {
    return;
  }
  const font = tokenSize("--fs-badge") * scale;
  context.font = `${String(font)}px ${tokenValue("--font-mono")}`;
  const labels = axisLabels(data, tickSize);
  let widest = 0;
  for (const text of labels) {
    widest = Math.max(widest, context.measureText(text).width);
  }
  const gutter = labels.length === 0 ? 0 : widest + AXIS_PAD * HALF * scale;
  const strip = font + STRIP_PAD * HALF * scale;
  const plotWidth = Math.max(1, pixelWidth - gutter);
  const plotHeight = Math.max(1, pixelHeight - strip);
  const buffer = document.createElement("canvas");
  buffer.width = data.cols.length;
  buffer.height = data.rows;
  const bufferContext = buffer.getContext("2d");
  if (bufferContext === null) {
    return;
  }
  bufferContext.putImageData(toPixels(bufferContext, data), 0, 0);
  context.imageSmoothingEnabled = false;
  context.drawImage(buffer, 0, 0, plotWidth, plotHeight);
  paintGaps(context, data, plotWidth, plotHeight, scale);
  paintMid(context, data, plotWidth, plotHeight, scale);
  paintAxes(
    context,
    data,
    labels,
    { plotWidth, plotHeight, height: pixelHeight, font },
    scale,
  );
}

/** 画布本体：随容器尺寸、数据与显示开关重绘。 */
function HeatmapCanvas({
  data,
  tickSize,
  grouping,
}: {
  data: HeatmapResponse;
  tickSize: string;
  grouping: boolean;
}): ReactElement {
  const holder = useRef<HTMLDivElement | null>(null);
  const canvas = useRef<HTMLCanvasElement | null>(null);
  const frame = useRef<{ data: HeatmapResponse; tick: string }>({
    data,
    tick: tickSize,
  });
  frame.current = { data, tick: tickSize };

  useEffect(() => {
    const wrap = holder.current;
    const target = canvas.current;
    if (wrap === null || target === null) {
      return;
    }
    const observer = new ResizeObserver(() => {
      paint(target, wrap, frame.current.data, frame.current.tick);
    });
    observer.observe(wrap);
    return () => {
      observer.disconnect();
    };
  }, []);

  useEffect(() => {
    const wrap = holder.current;
    const target = canvas.current;
    if (wrap === null || target === null) {
      return;
    }
    paint(target, wrap, data, tickSize);
  }, [data, tickSize, grouping]);

  return (
    <div className="hmp-wrap" ref={holder}>
      <canvas className="hmp-canvas" ref={canvas} />
    </div>
  );
}

export interface BookHeatmapProps {
  data: HeatmapResponse | null;
  symbol: string;
  tickSize: string;
  grouping: boolean;
  processes: OpsProcessesResponse | null;
}

/**
 * 快照序列热力图。
 * 空态与陈旧态显示拉起按钮，替代命令文本（就地拉起）。
 */
function BookHeatmapImpl({
  data,
  symbol,
  tickSize,
  grouping,
  processes,
}: BookHeatmapProps): ReactElement {
  if (data === null) {
    return <EmptyBlock text={EMPTY_TEXT} />;
  }
  const processName = `record-${symbol.toLowerCase()}`;
  if (data.cols.length === 0 || !hasRecorded(data)) {
    return (
      <div className="hmp-note">
        <ProcessStartButton name={processName} processes={processes} />
      </div>
    );
  }
  return (
    <div className="hmp-stack">
      {tailGap(data) ? (
        <div className="hmp-pull">
          <ProcessStartButton name={processName} processes={processes} />
        </div>
      ) : null}
      <HeatmapCanvas data={data} tickSize={tickSize} grouping={grouping} />
    </div>
  );
}

/** memo 边界：入参实变才重渲。 */
export const BookHeatmap = memo(BookHeatmapImpl);
