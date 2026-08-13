import type { CustomData } from "lightweight-charts";

/**
 * 足迹单档：档下沿价、双侧量文本与强度分位档。
 * 强度档为量分位映射的阶梯序号，-1 表示该侧无量。
 * 文本与档序由调用方预计算，插件层只承担绘制。
 */
export interface FootprintLevelDatum {
  readonly price: number;
  readonly sellText: string;
  readonly buyText: string;
  readonly sellStep: number;
  readonly buyStep: number;
}

/**
 * 足迹单 bar：OHLC 骨架值、Delta 形态与档阵列。
 * deltaStep 为 |Delta| 分位阶梯，供降级蜡烛实体饱和度。
 */
export interface FootprintBarDatum extends CustomData {
  readonly open: number;
  readonly high: number;
  readonly low: number;
  readonly close: number;
  readonly deltaRise: boolean;
  readonly deltaStep: number;
  readonly levels: readonly FootprintLevelDatum[];
}

/**
 * 会话级派生值：POC 与价值区边界加裸 POC 延伸终点。
 * 序号为 bar 数组下标；nakedEndIndex 空值表示延伸至右缘。
 */
export interface FootprintSessionDatum {
  readonly startIndex: number;
  readonly endIndex: number;
  readonly poc: number | null;
  readonly vah: number | null;
  readonly val: number | null;
  readonly nakedEndIndex: number | null;
}
