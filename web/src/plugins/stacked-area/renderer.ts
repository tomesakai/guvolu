import type {
  ICustomSeriesPaneRenderer,
  PaneRendererCustomData,
  PriceToCoordinateConverter,
  Time,
} from "lightweight-charts";
import type { CanvasRenderingTarget2D } from "fancy-canvas";
import type { StackedAreaData } from "./data";
import type { StackedAreaSeriesOptions } from "./options";

interface BandPoint {
  readonly x: number;
  readonly y: number;
}

type BandSegment = readonly BandPoint[];

/**
 * 堆叠面积渲染器：逐分量自下而上累加，
 * 每层在上一层轮廓与本层轮廓之间填充，再描本层上沿。
 */
export class StackedAreaRenderer
  implements ICustomSeriesPaneRenderer
{
  private data: PaneRendererCustomData<Time, StackedAreaData> | null = null;
  private options: StackedAreaSeriesOptions | null = null;

  /** 接收最新数据与样式，供下一次绘制使用。 */
  update(
    data: PaneRendererCustomData<Time, StackedAreaData>,
    options: StackedAreaSeriesOptions,
  ): void {
    this.data = data;
    this.options = options;
  }

  draw(
    target: CanvasRenderingTarget2D,
    priceConverter: PriceToCoordinateConverter,
  ): void {
    target.useMediaCoordinateSpace((scope) => {
      this.paint(scope.context, priceConverter);
    });
  }

  /** 累计各分量并逐层填充；缺口与非法值均切断面积。 */
  private paint(
    context: CanvasRenderingContext2D,
    priceConverter: PriceToCoordinateConverter,
  ): void {
    const data = this.data;
    const options = this.options;
    if (data === null || options === null || data.visibleRange === null) {
      return;
    }
    const bands = options.colors.length;
    if (bands === 0) {
      return;
    }
    const zero = priceConverter(0) ?? 0;
    const segments: StackedAreaData[][] = [];
    let segment: StackedAreaData[] = [];
    for (let at = data.visibleRange.from; at < data.visibleRange.to; at += 1) {
      const bar = data.bars[at];
      const values = bar?.originalData.values;
      const valid =
        Array.isArray(values) &&
        values.length >= bands &&
        values.slice(0, bands).every((value) => Number.isFinite(value) && value >= 0);
      if (bar === undefined || !valid) {
        if (segment.length > 0) {
          segments.push(segment);
          segment = [];
        }
        continue;
      }
      if (bar.originalData.breakBefore === true && segment.length > 0) {
        segments.push(segment);
        segment = [];
      }
      segment.push(bar.originalData);
    }
    if (segment.length > 0) {
      segments.push(segment);
    }

    // 以 WeakMap 保留时轴坐标。
    const coordinates = new WeakMap<StackedAreaData, number>();
    for (let at = data.visibleRange.from; at < data.visibleRange.to; at += 1) {
      const bar = data.bars[at];
      if (bar !== undefined) {
        coordinates.set(bar.originalData, bar.x);
      }
    }

    let lowerBySegment: BandSegment[] = segments.map(() => []);
    for (let band = 0; band < bands; band += 1) {
      const colors = options.colors[band];
      const upperBySegment = segments.map((rows) =>
        rows.flatMap((row) => {
          const x = coordinates.get(row);
          if (x === undefined) {
            return [];
          }
          let total = 0;
          for (let part = 0; part <= band; part += 1) {
            total += row.values[part] ?? 0;
          }
          return [{ x, y: priceConverter(total) ?? zero }];
        }),
      );
      if (colors !== undefined) {
        upperBySegment.forEach((upper, at) => {
          if (upper.length === 0) {
            return;
          }
          this.fillBand(
            context,
            upper,
            lowerBySegment[at] ?? [],
            zero,
            colors.area,
          );
          this.strokeBand(context, upper, colors.line, options.lineWidth);
        });
      }
      lowerBySegment = upperBySegment;
    }
  }

  /** 在本层轮廓与下层轮廓之间填色，首层以零线为底。 */
  private fillBand(
    context: CanvasRenderingContext2D,
    upper: readonly BandPoint[],
    lower: readonly BandPoint[],
    zero: number,
    fill: string,
  ): void {
    const head = upper[0];
    if (head === undefined) {
      return;
    }
    context.beginPath();
    context.moveTo(head.x, head.y);
    for (const point of upper) {
      context.lineTo(point.x, point.y);
    }
    if (lower.length === upper.length) {
      for (let at = lower.length - 1; at >= 0; at -= 1) {
        const point = lower[at];
        if (point !== undefined) {
          context.lineTo(point.x, point.y);
        }
      }
    } else {
      const tail = upper[upper.length - 1];
      if (tail !== undefined) {
        context.lineTo(tail.x, zero);
      }
      context.lineTo(head.x, zero);
    }
    context.closePath();
    context.fillStyle = fill;
    context.fill();
  }

  /** 描本层上沿。 */
  private strokeBand(
    context: CanvasRenderingContext2D,
    upper: readonly BandPoint[],
    stroke: string,
    width: number,
  ): void {
    const head = upper[0];
    if (head === undefined) {
      return;
    }
    context.beginPath();
    context.moveTo(head.x, head.y);
    for (const point of upper) {
      context.lineTo(point.x, point.y);
    }
    context.strokeStyle = stroke;
    context.lineWidth = width;
    context.stroke();
  }
}
