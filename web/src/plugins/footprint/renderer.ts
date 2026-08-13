import type {
  ICustomSeriesPaneRenderer,
  PaneRendererCustomData,
  PriceToCoordinateConverter,
  Time,
} from "lightweight-charts";
import type { CanvasRenderingTarget2D } from "fancy-canvas";
import type { FootprintBarDatum, FootprintSessionDatum } from "./data";
import type { FootprintSeriesOptions } from "./options";

// 空侧占位符
const EMPTY_MARK = "—";
const SIDE_MARK = "×";
const HALF = 2;
// 格宽收边与蜡烛实体占比
const CELL_GAP = 1;
const BODY_RATIO = 0.7;
// 文本可读的最小格高富余
const TEXT_PAD = 2;
// 骨架开收刻横向占比
const TICK_RATIO = 0.5;
// 派生线虚线样与裸线样
const DASH_LEVEL: readonly number[] = [4, 3];
const DASH_NAKED: readonly number[] = [2, 4];

/**
 * 足迹渲染器：三级细节度按 barSpacing 确定性切换。
 * 全数字格、色块格、Delta 蜡烛三态，另绘 OHLC 骨架
 * 与会话 POC、价值区虚线及裸 POC 延伸线。
 */
export class FootprintRenderer implements ICustomSeriesPaneRenderer {
  private data: PaneRendererCustomData<Time, FootprintBarDatum> | null = null;
  private options: FootprintSeriesOptions | null = null;

  /** 接收最新数据与样式，供下一次绘制使用。 */
  update(
    data: PaneRendererCustomData<Time, FootprintBarDatum>,
    options: FootprintSeriesOptions,
  ): void {
    this.data = data;
    this.options = options;
  }

  draw(
    target: CanvasRenderingTarget2D,
    priceConverter: PriceToCoordinateConverter,
  ): void {
    target.useMediaCoordinateSpace((scope) => {
      this.paint(scope.context, priceConverter, scope.mediaSize.width);
    });
  }

  private paint(
    context: CanvasRenderingContext2D,
    convert: PriceToCoordinateConverter,
    width: number,
  ): void {
    const data = this.data;
    const options = this.options;
    if (data === null || options === null || data.visibleRange === null) {
      return;
    }
    const spacing = data.barSpacing;
    for (let at = data.visibleRange.from; at < data.visibleRange.to; at += 1) {
      const bar = data.bars[at];
      if (bar === undefined) {
        continue;
      }
      const item = bar.originalData;
      if (spacing < options.lodCellPx) {
        this.paintDeltaCandle(context, convert, bar.x, spacing, item, options);
        continue;
      }
      this.paintLevels(
        context,
        convert,
        bar.x,
        spacing,
        item,
        options,
        spacing >= options.lodTextPx,
      );
      this.paintSkeleton(context, convert, bar.x, spacing, item, options);
    }
    this.paintSessionLines(context, convert, width, data, options);
  }

  /** 细节度三：Delta 蜡烛，涨跌色改 Delta 正负。 */
  private paintDeltaCandle(
    context: CanvasRenderingContext2D,
    convert: PriceToCoordinateConverter,
    x: number,
    spacing: number,
    item: FootprintBarDatum,
    options: FootprintSeriesOptions,
  ): void {
    const ink = item.deltaRise ? options.riseInk : options.fallInk;
    const ramp = item.deltaRise ? options.riseRamp : options.fallRamp;
    const fill = ramp[Math.min(item.deltaStep, ramp.length - 1)] ?? ink;
    const top = convert(Math.max(item.open, item.close)) ?? 0;
    const bottom = convert(Math.min(item.open, item.close)) ?? 0;
    const high = convert(item.high) ?? 0;
    const low = convert(item.low) ?? 0;
    context.strokeStyle = ink;
    context.lineWidth = 1;
    context.beginPath();
    context.moveTo(x, high);
    context.lineTo(x, low);
    context.stroke();
    const body = Math.max(1, spacing * BODY_RATIO);
    const height = Math.max(1, bottom - top);
    context.fillStyle = fill;
    context.fillRect(x - body / HALF, top, body, height);
    context.strokeRect(x - body / HALF, top, body, height);
  }

  /** 细节度一与二：逐档全数字格或量分位色块格。 */
  private paintLevels(
    context: CanvasRenderingContext2D,
    convert: PriceToCoordinateConverter,
    x: number,
    spacing: number,
    item: FootprintBarDatum,
    options: FootprintSeriesOptions,
    textMode: boolean,
  ): void {
    const width = Math.max(1, spacing - CELL_GAP);
    const half = width / HALF;
    context.font = `${String(options.fontSize)}px ${options.fontFamily}`;
    context.textBaseline = "middle";
    for (const level of item.levels) {
      const yLow = convert(level.price) ?? 0;
      const yHigh = convert(level.price + options.binSize) ?? 0;
      const top = Math.min(yLow, yHigh);
      const height = Math.max(1, Math.abs(yLow - yHigh) - 1);
      const middle = top + height / HALF;
      if (textMode && height >= options.fontSize + TEXT_PAD) {
        this.paintCellText(context, x, middle, level, options);
        continue;
      }
      // 格高不足时退回色块
      if (level.sellStep >= 0) {
        context.fillStyle =
          options.sellRamp[
            Math.min(level.sellStep, options.sellRamp.length - 1)
          ] ?? options.sellInk;
        context.fillRect(x - half, top, half, height);
      }
      if (level.buyStep >= 0) {
        context.fillStyle =
          options.buyRamp[
            Math.min(level.buyStep, options.buyRamp.length - 1)
          ] ?? options.buyInk;
        context.fillRect(x, top, half, height);
      }
    }
  }

  /** 全数字格：卖左买右，单侧以破折号占位。 */
  private paintCellText(
    context: CanvasRenderingContext2D,
    x: number,
    middle: number,
    level: {
      readonly sellText: string;
      readonly buyText: string;
      readonly sellStep: number;
      readonly buyStep: number;
    },
    options: FootprintSeriesOptions,
  ): void {
    context.textAlign = "center";
    context.fillStyle = options.neutralInk;
    context.fillText(SIDE_MARK, x, middle);
    const gap = context.measureText(SIDE_MARK).width;
    context.textAlign = "right";
    context.fillStyle =
      level.sellStep >= 0 ? options.sellInk : options.neutralInk;
    context.fillText(
      level.sellStep >= 0 ? level.sellText : EMPTY_MARK,
      x - gap,
      middle,
    );
    context.textAlign = "left";
    context.fillStyle = level.buyStep >= 0 ? options.buyInk : options.neutralInk;
    context.fillText(
      level.buyStep >= 0 ? level.buyText : EMPTY_MARK,
      x + gap,
      middle,
    );
  }

  /** OHLC 细骨架：高低竖线加开收短刻。 */
  private paintSkeleton(
    context: CanvasRenderingContext2D,
    convert: PriceToCoordinateConverter,
    x: number,
    spacing: number,
    item: FootprintBarDatum,
    options: FootprintSeriesOptions,
  ): void {
    const high = convert(item.high) ?? 0;
    const low = convert(item.low) ?? 0;
    const openY = convert(item.open) ?? 0;
    const closeY = convert(item.close) ?? 0;
    const reach = (spacing / HALF) * TICK_RATIO;
    context.strokeStyle = options.skeletonInk;
    context.lineWidth = 1;
    context.beginPath();
    context.moveTo(x, high);
    context.lineTo(x, low);
    context.moveTo(x - reach, openY);
    context.lineTo(x, openY);
    context.moveTo(x, closeY);
    context.lineTo(x + reach, closeY);
    context.stroke();
  }

  /** 会话派生线：POC 与价值区虚线、裸 POC 延伸。 */
  private paintSessionLines(
    context: CanvasRenderingContext2D,
    convert: PriceToCoordinateConverter,
    width: number,
    data: PaneRendererCustomData<Time, FootprintBarDatum>,
    options: FootprintSeriesOptions,
  ): void {
    if (!options.showPoc && !options.showVa) {
      return;
    }
    const half = data.barSpacing / HALF;
    for (const session of options.sessions) {
      const first = data.bars[session.startIndex];
      const last = data.bars[session.endIndex];
      if (first === undefined || last === undefined) {
        continue;
      }
      const from = first.x - half;
      const to = last.x + half;
      if (to < 0 || from > width) {
        continue;
      }
      if (options.showVa) {
        this.dashLine(context, from, to, convert(session.vah ?? 0), options.vaInk, DASH_LEVEL, session.vah !== null);
        this.dashLine(context, from, to, convert(session.val ?? 0), options.vaInk, DASH_LEVEL, session.val !== null);
      }
      if (options.showPoc && session.poc !== null) {
        const y = convert(session.poc);
        this.dashLine(context, from, to, y, options.pocInk, DASH_LEVEL, true);
        this.paintNaked(context, convert, width, data, session, to, options);
      }
    }
    context.setLineDash([]);
  }

  /** 裸 POC：自会话末延伸至触及 bar 或右缘。 */
  private paintNaked(
    context: CanvasRenderingContext2D,
    convert: PriceToCoordinateConverter,
    width: number,
    data: PaneRendererCustomData<Time, FootprintBarDatum>,
    session: FootprintSessionDatum,
    sessionEnd: number,
    options: FootprintSeriesOptions,
  ): void {
    if (session.poc === null) {
      return;
    }
    let until = width;
    if (session.nakedEndIndex !== null) {
      const touched = data.bars[session.nakedEndIndex];
      if (touched === undefined) {
        return;
      }
      until = touched.x;
    }
    if (until <= sessionEnd) {
      return;
    }
    this.dashLine(
      context,
      sessionEnd,
      until,
      convert(session.poc),
      options.nakedInk,
      DASH_NAKED,
      true,
    );
  }

  /** 单条水平虚线。 */
  private dashLine(
    context: CanvasRenderingContext2D,
    from: number,
    to: number,
    y: number | null,
    ink: string,
    dash: readonly number[],
    enabled: boolean,
  ): void {
    if (!enabled || y === null) {
      return;
    }
    context.strokeStyle = ink;
    context.lineWidth = 1;
    context.setLineDash([...dash]);
    context.beginPath();
    context.moveTo(from, y);
    context.lineTo(to, y);
    context.stroke();
  }
}
