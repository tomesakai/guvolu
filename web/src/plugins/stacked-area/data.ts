import type { CustomData } from "lightweight-charts";

/**
 * 堆叠面积的一条数据：同一时刻上各分量的值，自下而上堆叠。
 *
 * 移植自 lightweight-charts plugin-examples 的 stacked-area-series
 * （Apache 2.0，归属见 THIRD_PARTY_NOTICES.txt）。
 * 当前用于每周期主动卖/主动买的非负构成。
 */
export interface StackedAreaData extends CustomData {
  values: number[];
  /** 上一有效点不可与本点连线，用于真实缺口与来源异常留空。 */
  breakBefore?: boolean;
}

/** 分量的线色与填充色成对，取值由调用方从设计 token 解析后传入。 */
export interface StackedAreaBandColors {
  readonly line: string;
  readonly area: string;
}
