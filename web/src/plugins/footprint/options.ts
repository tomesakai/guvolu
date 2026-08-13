import { customSeriesDefaultOptions } from "lightweight-charts";
import type { CustomSeriesOptions } from "lightweight-charts";
import type { FootprintSessionDatum } from "./data";

/**
 * 足迹系列样式项：档宽、细节度阈值、墨色与强度阶梯。
 * 颜色一律由 lwc.ts 经 token 解析后注入，插件层零颜色常量。
 */
export interface FootprintSeriesOptions extends CustomSeriesOptions {
  binSize: number;
  lodTextPx: number;
  lodCellPx: number;
  fontFamily: string;
  fontSize: number;
  sellInk: string;
  buyInk: string;
  neutralInk: string;
  skeletonInk: string;
  riseInk: string;
  fallInk: string;
  sellRamp: readonly string[];
  buyRamp: readonly string[];
  riseRamp: readonly string[];
  fallRamp: readonly string[];
  pocInk: string;
  vaInk: string;
  nakedInk: string;
  showPoc: boolean;
  showVa: boolean;
  sessions: readonly FootprintSessionDatum[];
}

/** 缺省样式不含任何颜色取值，全部由工厂注入。 */
export const footprintDefaultOptions: FootprintSeriesOptions = {
  ...customSeriesDefaultOptions,
  binSize: 1,
  lodTextPx: 72,
  lodCellPx: 36,
  fontFamily: "",
  fontSize: 9,
  sellInk: "",
  buyInk: "",
  neutralInk: "",
  skeletonInk: "",
  riseInk: "",
  fallInk: "",
  sellRamp: [],
  buyRamp: [],
  riseRamp: [],
  fallRamp: [],
  pocInk: "",
  vaInk: "",
  nakedInk: "",
  showPoc: true,
  showVa: true,
  sessions: [],
} as const;
