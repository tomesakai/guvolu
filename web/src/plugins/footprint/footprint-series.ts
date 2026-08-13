import type {
  CustomSeriesPricePlotValues,
  CustomSeriesWhitespaceData,
  ICustomSeriesPaneRenderer,
  ICustomSeriesPaneView,
  PaneRendererCustomData,
  Time,
} from "lightweight-charts";
import type { FootprintBarDatum } from "./data";
import type { FootprintSeriesOptions } from "./options";
import { footprintDefaultOptions } from "./options";
import { FootprintRenderer } from "./renderer";

/**
 * 足迹自定义系列：bar 展开为价格档乘主动方向量矩阵，
 * 替换 bar 本体渲染并叠 OHLC 细骨架（footprint-design 第 5 节）。
 * 沿 stacked-area 移植模式，颜色一律由 lwc.ts 工厂注入。
 */
export class FootprintSeries
  implements ICustomSeriesPaneView<Time, FootprintBarDatum, FootprintSeriesOptions>
{
  private readonly painter: FootprintRenderer = new FootprintRenderer();

  renderer(): ICustomSeriesPaneRenderer {
    return this.painter;
  }

  update(
    data: PaneRendererCustomData<Time, FootprintBarDatum>,
    options: FootprintSeriesOptions,
  ): void {
    this.painter.update(data, options);
  }

  /** 自动缩放取整根范围，末位收盘供最新价标签。 */
  priceValueBuilder(plotRow: FootprintBarDatum): CustomSeriesPricePlotValues {
    return [plotRow.low, plotRow.high, plotRow.close];
  }

  /** 无档阵列即视为空档，如实留空不插值。 */
  isWhitespace(
    data: FootprintBarDatum | CustomSeriesWhitespaceData<Time>,
  ): data is CustomSeriesWhitespaceData<Time> {
    return !Array.isArray((data as Partial<FootprintBarDatum>).levels);
  }

  defaultOptions(): FootprintSeriesOptions {
    return footprintDefaultOptions;
  }
}
