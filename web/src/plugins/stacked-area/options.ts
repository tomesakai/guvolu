import { customSeriesDefaultOptions } from "lightweight-charts";
import type { CustomSeriesOptions } from "lightweight-charts";
import type { StackedAreaBandColors } from "./data";

/** 堆叠面积的样式项：逐分量的线色与填充色，另加线宽。 */
export interface StackedAreaSeriesOptions extends CustomSeriesOptions {
  colors: readonly StackedAreaBandColors[];
  lineWidth: number;
}

/**
 * 缺省样式不含任何颜色常量。
 * 颜色一律由 lwc.ts 的工厂经 chartColor 读 token 后注入，
 * 此处留空以免在插件层写死取值。
 */
export const stackedAreaDefaultOptions: StackedAreaSeriesOptions = {
  ...customSeriesDefaultOptions,
  colors: [],
  lineWidth: 1,
} as const;
