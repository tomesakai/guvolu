import type {
  CustomSeriesPricePlotValues,
  CustomSeriesWhitespaceData,
  ICustomSeriesPaneRenderer,
  ICustomSeriesPaneView,
  PaneRendererCustomData,
  Time,
} from "lightweight-charts";
import type { StackedAreaData } from "./data";
import type { StackedAreaSeriesOptions } from "./options";
import { stackedAreaDefaultOptions } from "./options";
import { StackedAreaRenderer } from "./renderer";

/**
 * 堆叠面积自定义系列：多分量在同一时刻自下而上堆叠。
 *
 * 移植自 lightweight-charts plugin-examples 的 stacked-area-series
 * （Apache 2.0，归属见 THIRD_PARTY_NOTICES.txt）。
 * 当前用于每周期主动卖/主动买的非负构成。
 */
export class StackedAreaSeries
  implements ICustomSeriesPaneView<Time, StackedAreaData, StackedAreaSeriesOptions>
{
  private readonly painter: StackedAreaRenderer = new StackedAreaRenderer();

  renderer(): ICustomSeriesPaneRenderer {
    return this.painter;
  }

  update(
    data: PaneRendererCustomData<Time, StackedAreaData>,
    options: StackedAreaSeriesOptions,
  ): void {
    this.painter.update(data, options);
  }

  /** 自动缩放取累计总和，末位为当前值。 */
  priceValueBuilder(plotRow: StackedAreaData): CustomSeriesPricePlotValues {
    let total = 0;
    for (const value of plotRow.values) {
      total += value;
    }
    return [0, total];
  }

  /** 无分量值即视为空档，如实留空不插值。 */
  isWhitespace(
    data: StackedAreaData | CustomSeriesWhitespaceData<Time>,
  ): data is CustomSeriesWhitespaceData<Time> {
    const values = (data as Partial<StackedAreaData>).values;
    return !Array.isArray(values) || values.length === 0;
  }

  defaultOptions(): StackedAreaSeriesOptions {
    return stackedAreaDefaultOptions;
  }
}
