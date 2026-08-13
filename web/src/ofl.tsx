import { memo, useEffect, useMemo, useRef, useState } from "react";
import type {
  PointerEvent as ReactPointerEvent,
  ReactElement,
  WheelEvent as ReactWheelEvent,
} from "react";
import {
  DEFAULT_TILE_BUCKET,
  FEATURES_POLL_INTERVAL_MS,
  PRINT_TICKS_POLL_INTERVAL_MS,
  TILE_BUCKETS,
  TILE_BUCKET_SECONDS,
  UNSUPPORTED_TILE_BUCKETS,
  apiGet,
  apiPost,
  bookFeaturesPath,
  levelTrackPath,
  printTicksPath,
  regionPath,
  usePolling,
  useStable,
} from "./api";
import type {
  AlertItem,
  BookFeatureItem,
  BookFeaturesResponse,
  LevelTrackResponse,
  L2QualityResponse,
  OpsProcessesResponse,
  OrderbookResponse,
  PollMeta,
  PrintTicksResponse,
  RegionResponse,
  TileColumn,
} from "./api";
import { useOrderflowTileWindowV2, useTileWindow } from "./ofl-data";
import {
  DRAWER_ALERTS,
  DRAWER_REGION,
  DRAWER_TRACK,
  OflDrawer,
} from "./ofl-drawer";
import type { FetchState } from "./ofl-drawer";
import {
  COLOR_LINEAR,
  COLOR_LOG,
  COLOR_PCT,
  Y_MODE_ABS,
  Y_MODE_BP,
  buildScale,
  epochOf,
  paintNav,
  paintOfl,
} from "./ofl-render";
import type { OflPaintResult, Rect } from "./ofl-render";
import {
  BASIS_NOTIONAL,
  EMPTY_TEXT,
  bpDiffText,
  decimalPlaces,
  jstStamp,
  plotNumber,
  rawText,
  tallyText,
} from "./format";
import { Panel, ProcessStartButton } from "./panels";
import { L2QualityStrip } from "./l2-quality";

// 视窗跨度缺省按桶档取
const DEFAULT_SPAN_S: Readonly<Record<string, number>> = {
  "1s": 1800,
  "5s": 7200,
  "1min": 86400,
};
// 视窗跨度上下限秒
const MIN_SPAN_S = 60;
const MAX_SPAN_S = 172800;
// 导航覆盖范围两日
const NAV_SPAN_S = 172800;
// 导航桶秒宽
const NAV_BUCKET_S = 60;
// 缩放步进倍率
const ZOOM_IN = 0.8;
const ZOOM_OUT = 1.25;
// 拖拽判定阈像素
const DRAG_THRESHOLD = 4;
// 缘手柄命中半宽像素
const EDGE_GRIP = 5;
// 闪烁一次即停毫秒
const FLASH_MS = 700;
// 实时盘口响应最大展示年龄
const LIVE_BOOK_MAX_AGE_MS = 15000;
// 事件跳转视窗放大倍数
const JUMP_PAD = 3;
const HALF = 2;
// 拖动静置后取数毫秒
const QUERY_SETTLE_MS = 150;
// 跟随推进与覆盖延伸周期毫秒
const FOLLOW_TICK_MS = 5000;
const COVER_GROW_MS = 60000;
// 格命中容差像素与刻线命中半径
const HIT_TOLERANCE_PX = 6;
const TICK_HOVER_PX = 4;
const MS_PER_SECOND = 1000;
// 合帧兜底间隔毫秒
const PAINT_FALLBACK_MS = 32;
// 提示框行组：格、刻线、事件、带
const TIP_CELL_LABELS: readonly string[] = [
  "档价",
  "时刻",
  "挂量",
  "净增挂",
  "净减挂（归因未知）",
  "主动成交（独立逐笔）",
  "距中间价",
  "流动性分位",
  "列态",
];
const TIP_TICK_LABELS: readonly string[] = [
  "时刻",
  "价格",
  "数量",
  "方向",
  "量分位",
];
const TIP_EVENT_LABELS: readonly string[] = ["种类", "价格带", "时段"];
const TIP_BAND_LABELS: readonly string[] = ["带", "时刻", "值"];

interface TimeWindow {
  readonly fromS: number;
  readonly toS: number;
}

/** 对齐桶界，窗口参数可复现。 */
function quantize(window: TimeWindow, bucketS: number): TimeWindow {
  const fromS = Math.floor(window.fromS / bucketS) * bucketS;
  const toS = Math.max(
    fromS + bucketS,
    Math.ceil(window.toS / bucketS) * bucketS,
  );
  return { fromS, toS };
}

/** 最新对齐时刻：跟随态视窗右端。 */
function latestEdge(bucketS: number): number {
  return Math.ceil(Date.now() / MS_PER_SECOND / bucketS) * bucketS;
}

/** 缺省视窗：终于当下，跨度按桶档。 */
function defaultWindow(bucket: string): TimeWindow {
  const bucketS = TILE_BUCKET_SECONDS[bucket] ?? 1;
  const span = DEFAULT_SPAN_S[bucket] ?? 1800;
  const now = latestEdge(bucketS);
  return { fromS: now - span, toS: now };
}

/** 数值贴档：按行档向下取整为文本。 */
function snapPrice(
  value: number,
  rowBinText: string,
  roundUp = false,
): string | null {
  const bin = plotNumber(rowBinText);
  if (bin === null || bin <= 0) {
    return null;
  }
  const places = decimalPlaces(rowBinText);
  const snapped = (roundUp ? Math.ceil(value / bin) : Math.floor(value / bin)) *
    bin;
  return snapped.toFixed(places);
}

/** 列态术语：空档、延载、重置。 */
function columnState(column: TileColumn): string {
  if (column.gap) {
    return "空档";
  }
  if (column.carried) {
    return "延载";
  }
  if (column.reset) {
    return "重置";
  }
  return EMPTY_TEXT;
}

interface RowIndex {
  readonly minBin: number;
  readonly arr: Int32Array;
}

// 列内行索引惰性构建，装配不变即复用
const rowIndexCache = new WeakMap<TileColumn, RowIndex | null>();

/** 列内 O(1) 行索引：档价算术直取格序。 */
function rowIndexOf(column: TileColumn, rowBin: number): RowIndex | null {
  const held = rowIndexCache.get(column);
  if (held !== undefined) {
    return held;
  }
  if (rowBin <= 0 || column.cells.length === 0) {
    rowIndexCache.set(column, null);
    return null;
  }
  let minBin = Number.POSITIVE_INFINITY;
  let maxBin = Number.NEGATIVE_INFINITY;
  for (const cell of column.cells) {
    const at = plotNumber(cell[0]);
    if (at !== null) {
      minBin = Math.min(minBin, at);
      maxBin = Math.max(maxBin, at);
    }
  }
  if (!Number.isFinite(minBin) || !Number.isFinite(maxBin)) {
    rowIndexCache.set(column, null);
    return null;
  }
  const slots = Math.round((maxBin - minBin) / rowBin) + 1;
  const arr = new Int32Array(slots).fill(-1);
  column.cells.forEach((cell, index) => {
    const at = plotNumber(cell[0]);
    if (at === null) {
      return;
    }
    const slot = Math.round((at - minBin) / rowBin);
    if (slot >= 0 && slot < slots) {
      arr[slot] = index;
    }
  });
  const made: RowIndex = { minBin, arr };
  rowIndexCache.set(column, made);
  return made;
}

/** 档价直取格，容差内向邻档就近。 */
function cellNear(
  column: TileColumn,
  price: number,
  rowBin: number,
  toleranceBins: number,
): readonly string[] | null {
  const index = rowIndexOf(column, rowBin);
  if (index === null) {
    return null;
  }
  const slot = Math.round(
    (Math.floor(price / rowBin) * rowBin - index.minBin) / rowBin,
  );
  const reach = Math.max(0, Math.ceil(toleranceBins));
  let best: number = -1;
  let bestGap = Number.POSITIVE_INFINITY;
  for (let step = -reach; step <= reach; step += 1) {
    const at = slot + step;
    if (at < 0 || at >= index.arr.length) {
      continue;
    }
    const cellAt = index.arr[at] ?? -1;
    if (cellAt < 0) {
      continue;
    }
    const gap = Math.abs(step);
    if (gap < bestGap) {
      bestGap = gap;
      best = cellAt;
    }
  }
  return best < 0 ? null : (column.cells[best] ?? null);
}

/** 同档挂量对可视列集的百分位。 */
function liquidityRank(
  distributions: ReadonlyMap<string, readonly number[]>,
  bin: string,
  qty: number,
): string {
  const values = distributions.get(bin);
  if (values === undefined || values.length === 0) {
    return EMPTY_TEXT;
  }
  let low = 0;
  let high = values.length;
  while (low < high) {
    const middle = (low + high) >> 1;
    if ((values[middle] ?? Number.POSITIVE_INFINITY) <= qty) {
      low = middle + 1;
    } else {
      high = middle;
    }
  }
  return (low / values.length).toFixed(2);
}

/** 仅用真实帧构建档价挂量分布。 */
function liquidityDistributions(
  columns: readonly TileColumn[],
): ReadonlyMap<string, readonly number[]> {
  const out = new Map<string, number[]>();
  for (const column of columns) {
    if (column.gap || column.carried) {
      continue;
    }
    for (const cell of column.cells) {
      const bin = cell[0];
      const value = plotNumber(cell[2]);
      if (bin === undefined || value === null) {
        continue;
      }
      const held = out.get(bin) ?? [];
      held.push(value);
      out.set(bin, held);
    }
  }
  for (const values of out.values()) {
    values.sort((left, right) => left - right);
  }
  return out;
}

/** 当前盘口仅在跟随态且响应新鲜时显示。 */
function freshBook(
  book: OrderbookResponse | null,
  followLatest: boolean,
): OrderbookResponse | null {
  if (!followLatest || book === null) {
    return null;
  }
  const asOf = Date.parse(book.as_of);
  return Number.isFinite(asOf) && Date.now() - asOf <= LIVE_BOOK_MAX_AGE_MS
    ? book
    : null;
}

type DragMode =
  | { kind: "idle" }
  | { kind: "press"; x: number; y: number }
  | { kind: "box"; x: number; y: number; toX: number; toY: number }
  | { kind: "nav-pan"; grabS: number; cover: TimeWindow }
  | { kind: "nav-from"; cover: TimeWindow }
  | { kind: "nav-to"; cover: TimeWindow };

export interface OflPageProps {
  marketId: string | null;
  symbol: string | null;
  tickSize: string;
  sizeStep: string;
  basis: string;
  orderbook: OrderbookResponse | null;
  alerts: readonly AlertItem[];
  unackedIds: ReadonlySet<number>;
  onAck: (alertId: number) => void;
  intentTab: string | null;
  intentSeq: number;
  processes: OpsProcessesResponse | null;
  l2Quality: L2QualityResponse | null;
}

/**
 * 订单流页：主热力图、当前盘口列、底部五带加事件带、
 * 导航条与右侧四页签抽屉。框选仅此页主图。
 * 画布终生只挂一次，一切切换走命令式重绘；
 * 视窗为时间量，像素只是投影；hover 走单例提示管线。
 */
function OflPageImpl({
  marketId,
  symbol,
  tickSize,
  sizeStep,
  basis,
  orderbook,
  alerts,
  unackedIds,
  onAck,
  intentTab,
  intentSeq,
  processes,
  l2Quality,
}: OflPageProps): ReactElement {
  const [bucket, setBucket] = useState<string>(DEFAULT_TILE_BUCKET);
  const bucketS = TILE_BUCKET_SECONDS[bucket] ?? 1;
  // 视窗真值在引用，查询窗入状态
  const windowRef = useRef<TimeWindow>(defaultWindow(DEFAULT_TILE_BUCKET));
  const [query, setQuery] = useState<TimeWindow>(windowRef.current);
  const [followLatest, setFollowLatest] = useState<boolean>(true);
  const [yMode, setYMode] = useState<string>(Y_MODE_ABS);
  const [colorMode, setColorMode] = useState<string>(COLOR_LOG);
  const [drawerOpen, setDrawerOpen] = useState<boolean>(false);
  const [drawerTab, setDrawerTab] = useState<string>(DRAWER_TRACK);
  const [track, setTrack] = useState<FetchState<LevelTrackResponse>>({
    pending: false,
    error: null,
    data: null,
  });
  const [region, setRegion] = useState<FetchState<RegionResponse>>({
    pending: false,
    error: null,
    data: null,
  });
  const [selected, setSelected] = useState<BookFeatureItem | null>(null);
  const [flash, setFlash] = useState<number | null>(null);
  const trackRequest = useRef<number>(0);
  const regionRequest = useRef<number>(0);
  const dragRef = useRef<DragMode>({ kind: "idle" });
  // 拖动中值域冻结，松手恢复
  const lockDomainRef = useRef<{ low: number; high: number } | null>(null);

  // 覆盖范围：起点定锚，右端按分钟延伸
  const coverFromRef = useRef<number>(
    Math.floor((Date.now() / MS_PER_SECOND - NAV_SPAN_S) / NAV_BUCKET_S) *
      NAV_BUCKET_S,
  );
  const [coverTo, setCoverTo] = useState<number>(latestEdge(NAV_BUCKET_S));
  useEffect(() => {
    const grow = (): void => {
      setCoverTo(latestEdge(NAV_BUCKET_S));
    };
    const timer = window.setInterval(grow, COVER_GROW_MS);
    return () => {
      window.clearInterval(timer);
    };
  }, []);

  // 数据装配：细层按查询窗、底层覆盖全程
  const fine = useOrderflowTileWindowV2(
    marketId,
    bucket,
    bucketS,
    query.fromS,
    query.toS,
  );
  const base = useTileWindow(
    symbol,
    "1min",
    NAV_BUCKET_S,
    coverFromRef.current,
    coverTo,
  );
  const navigatorColumns =
    base.columns.length > 0 ? base.columns : fine.columns;
  const featuresPoll = usePolling<BookFeaturesResponse>(
    bookFeaturesPath(symbol),
    FEATURES_POLL_INTERVAL_MS,
  );
  const printsPoll = usePolling<PrintTicksResponse>(
    printTicksPath(symbol, query.fromS, query.toS),
    PRINT_TICKS_POLL_INTERVAL_MS,
  );
  const featureItems = useStable(featuresPoll.data?.items ?? []);
  const printItems = useStable(printsPoll.data?.items ?? []);
  const alertFeatureKey = alerts.map((item) => item.feature_id).join(",");
  const alertFeatures = useMemo<ReadonlySet<number>>(
    () => new Set(alerts.map((item) => item.feature_id)),
    // 内容键隔离时戳噪音
    [alertFeatureKey],
  );

  const baseInView = useMemo(
    () =>
      base.columns.filter(
        (column) =>
          column.e + NAV_BUCKET_S > query.fromS && column.e < query.toS,
      ),
    [base.columns, query.fromS, query.toS],
  );
  const scale = useMemo(
    () =>
      buildScale(
        fine.columns,
        baseInView,
        colorMode,
        bucketS,
        NAV_BUCKET_S,
        query.fromS,
        query.toS,
      ),
    [
      fine.columns,
      baseInView,
      colorMode,
      bucketS,
      query.fromS,
      query.toS,
    ],
  );
  const liquidityIndex = useMemo(
    () => liquidityDistributions(fine.columns),
    [fine.columns],
  );
  const dataVersion = fine.version * 1000003 + base.version;

  // 新报警：落点加重并单闪即停
  const knownAlerts = useRef<Set<number> | null>(null);
  useEffect(() => {
    if (alerts.length === 0 && knownAlerts.current === null) {
      return;
    }
    const previous = knownAlerts.current;
    knownAlerts.current = new Set(alerts.map((item) => item.alert_id));
    if (previous === null) {
      return;
    }
    const fresh = alerts.find(
      (item) => !previous.has(item.alert_id) && item.acked_at === null,
    );
    if (fresh === undefined) {
      return;
    }
    setFlash(fresh.feature_id);
    const timer = window.setTimeout(() => {
      setFlash(null);
    }, FLASH_MS);
    return () => {
      window.clearTimeout(timer);
    };
  }, [alerts]);

  // 状态条跳转意图：切页签并开抽屉
  useEffect(() => {
    if (intentSeq > 0 && intentTab !== null) {
      setDrawerTab(intentTab);
      setDrawerOpen(true);
    }
  }, [intentSeq, intentTab]);

  const wrap = useRef<HTMLDivElement | null>(null);
  const canvas = useRef<HTMLCanvasElement | null>(null);
  const navWrap = useRef<HTMLDivElement | null>(null);
  const navCanvas = useRef<HTMLCanvasElement | null>(null);
  const hit = useRef<OflPaintResult | null>(null);
  const boxEl = useRef<HTMLElement | null>(null);
  const tipEl = useRef<HTMLDivElement | null>(null);
  const tipCells = useRef<HTMLSpanElement[]>([]);
  const tipKind = useRef<string>("");

  /** 主画布整幅重绘并存命中几何。 */
  const paintMain = (): void => {
    const target = canvas.current;
    const holder = wrap.current;
    if (target === null || holder === null) {
      return;
    }
    hit.current = paintOfl(target, holder, {
      view: {
        fromS: windowRef.current.fromS,
        toS: windowRef.current.toS,
        bucketS,
        yMode,
        colorMode,
      },
      fine: fine.columns,
      base: baseInView,
      baseSpanS: NAV_BUCKET_S,
      rowBinText: fine.meta?.row_bin ?? null,
      tickSize,
      dataVersion,
      lockDomain: lockDomainRef.current,
      scale,
      basis,
      layers: {
        printTicks: printItems,
        features: featureItems,
        alertFeatures,
        flashFeature: flash,
        selected,
        orderbook: freshBook(orderbook, followLatest),
      },
    });
  };
  const paintMainRef = useRef<() => void>(paintMain);
  paintMainRef.current = paintMain;

  const paintNavigator = (): void => {
    const target = navCanvas.current;
    const holder = navWrap.current;
    if (target === null || holder === null) {
      return;
    }
    // 拖动中用覆盖快照冻结坐标系
    const drag = dragRef.current;
    const cover =
      drag.kind === "nav-pan" ||
      drag.kind === "nav-from" ||
      drag.kind === "nav-to"
        ? drag.cover
        : { fromS: coverFromRef.current, toS: coverTo };
    paintNav(
      target,
      holder,
      navigatorColumns,
      cover.fromS,
      cover.toS,
      windowRef.current.fromS,
      windowRef.current.toS,
    );
  };
  const paintNavRef = useRef<() => void>(paintNavigator);
  paintNavRef.current = paintNavigator;

  // rAF 合帧：每交互至多一次重绘；
  // 遮蔽态定时器兜底，仍保合帧
  const paintQueued = useRef<boolean>(false);
  const schedulePaint = (): void => {
    if (paintQueued.current) {
      return;
    }
    paintQueued.current = true;
    const run = (): void => {
      if (!paintQueued.current) {
        return;
      }
      paintQueued.current = false;
      paintMainRef.current();
      paintNavRef.current();
    };
    window.requestAnimationFrame(run);
    window.setTimeout(run, PAINT_FALLBACK_MS);
  };
  const schedulePaintRef = useRef<() => void>(schedulePaint);
  schedulePaintRef.current = schedulePaint;

  // 静置取数：停 150ms 再落查询
  const bucketSRef = useRef<number>(bucketS);
  bucketSRef.current = bucketS;
  const settleTimer = useRef<number | null>(null);
  const queueSettle = (): void => {
    if (settleTimer.current !== null) {
      window.clearTimeout(settleTimer.current);
    }
    settleTimer.current = window.setTimeout(() => {
      settleTimer.current = null;
      setQuery(quantize(windowRef.current, bucketSRef.current));
    }, QUERY_SETTLE_MS);
  };
  const queueSettleRef = useRef<() => void>(queueSettle);
  queueSettleRef.current = queueSettle;

  /** 视窗移动统一入口：重绘加静置取数。 */
  const moveWindow = (next: TimeWindow): void => {
    windowRef.current = next;
    schedulePaintRef.current();
    queueSettleRef.current();
  };
  const moveWindowRef = useRef<(next: TimeWindow) => void>(moveWindow);
  moveWindowRef.current = moveWindow;

  // 数据或图层实变即重绘（rAF 合帧）
  useEffect(() => {
    schedulePaintRef.current();
  }, [
    fine.columns,
    baseInView,
    scale,
    printItems,
    featureItems,
    alertFeatures,
    flash,
    selected,
    orderbook,
    followLatest,
    yMode,
    colorMode,
    basis,
    tickSize,
    query,
    coverTo,
  ]);

  // 切换品种回缺省视窗与空抽屉
  useEffect(() => {
    windowRef.current = defaultWindow(bucket);
    setQuery(quantize(windowRef.current, bucketS));
    setFollowLatest(true);
    setSelected(null);
    setTrack({ pending: false, error: null, data: null });
    setRegion({ pending: false, error: null, data: null });
    trackRequest.current += 1;
    regionRequest.current += 1;
    coverFromRef.current =
      Math.floor((Date.now() / MS_PER_SECOND - NAV_SPAN_S) / NAV_BUCKET_S) *
      NAV_BUCKET_S;
    setCoverTo(latestEdge(NAV_BUCKET_S));
    // 桶档保持，品种驱动重置
  }, [marketId, symbol]);

  // 跟随态：视窗右端贴最新，覆盖延伸不移框
  useEffect(() => {
    if (!followLatest || marketId === null) {
      return;
    }
    const advance = (): void => {
      const latest = latestEdge(bucketS);
      const held = windowRef.current;
      if (latest !== held.toS) {
        moveWindowRef.current({
          fromS: latest - (held.toS - held.fromS),
          toS: latest,
        });
      }
    };
    advance();
    const timer = window.setInterval(advance, FOLLOW_TICK_MS);
    return () => {
      window.clearInterval(timer);
    };
  }, [followLatest, marketId, bucketS]);

  useEffect(() => {
    const holder = wrap.current;
    const navHolder = navWrap.current;
    if (holder === null) {
      return;
    }
    const observer = new ResizeObserver(() => {
      schedulePaintRef.current();
    });
    observer.observe(holder);
    if (navHolder !== null) {
      observer.observe(navHolder);
    }
    return () => {
      observer.disconnect();
    };
  }, []);

  /** 视窗跳转至事件足迹并高亮轮廓。 */
  const jumpToFeature = (feature: BookFeatureItem): void => {
    const fromE = epochOf(feature.from_ts);
    const toE = epochOf(feature.to_ts);
    if (fromE === null || toE === null) {
      return;
    }
    const span = Math.max(toE - fromE, bucketS) * JUMP_PAD;
    setFollowLatest(false);
    moveWindowRef.current(
      quantize({ fromS: fromE - span, toS: toE + span }, bucketS),
    );
    setSelected(feature);
    setDrawerOpen(true);
  };
  const jumpRef = useRef<(feature: BookFeatureItem) => void>(jumpToFeature);
  jumpRef.current = jumpToFeature;

  /** 点击档带唤出流动性追踪。 */
  const openTrack = (priceBin: string): void => {
    if (symbol === null) {
      return;
    }
    setDrawerOpen(true);
    setDrawerTab(DRAWER_TRACK);
    setTrack({ pending: true, error: null, data: null });
    const requestId = trackRequest.current + 1;
    trackRequest.current = requestId;
    const held = windowRef.current;
    apiGet<LevelTrackResponse>(
      levelTrackPath(symbol, priceBin, bucket, held.fromS, held.toS),
    )
      .then((data) => {
        if (trackRequest.current === requestId) {
          setTrack({ pending: false, error: null, data });
        }
      })
      .catch((error: unknown) => {
        if (trackRequest.current === requestId) {
          setTrack({
            pending: false,
            error: error instanceof Error ? error.message : "读取失败",
            data: null,
          });
        }
      });
  };

  /** 框选矩形提交区域分析。 */
  const openRegion = (box: Rect): void => {
    const geometry = hit.current;
    const rowBinText = fine.meta?.row_bin ?? null;
    if (symbol === null || geometry === null || rowBinText === null) {
      return;
    }
    const held = windowRef.current;
    const plot = geometry.plot;
    const clamp = (value: number, low: number, high: number): number =>
      Math.max(low, Math.min(high, value));
    const x0 = clamp(box.x, plot.x, plot.x + plot.w);
    const x1 = clamp(box.x + box.w, plot.x, plot.x + plot.w);
    const y0 = clamp(box.y, plot.y, plot.y + plot.h);
    const y1 = clamp(box.y + box.h, plot.y, plot.y + plot.h);
    const spanS = held.toS - held.fromS;
    const fromS =
      Math.floor(
        (held.fromS + ((x0 - plot.x) / plot.w) * spanS) / bucketS,
      ) * bucketS;
    const toS =
      Math.ceil(
        (held.fromS + ((x1 - plot.x) / plot.w) * spanS) / bucketS,
      ) * bucketS;
    const valueOf = (y: number): number =>
      geometry.yHigh -
      ((y - plot.y) / plot.h) * (geometry.yHigh - geometry.yLow);
    let highValue = valueOf(y0);
    let lowValue = valueOf(y1);
    if (yMode === Y_MODE_BP) {
      // 相对框转为逐列绝对价包络
      let envelopeLow = Number.POSITIVE_INFINITY;
      let envelopeHigh = Number.NEGATIVE_INFINITY;
      for (const column of fine.columns) {
        if (column.gap || column.e < fromS || column.e >= toS) {
          continue;
        }
        const mid = plotNumber(column.mid);
        if (mid === null || mid <= 0) {
          continue;
        }
        envelopeLow = Math.min(envelopeLow, (1 + lowValue / 10000) * mid);
        envelopeHigh = Math.max(
          envelopeHigh,
          (1 + highValue / 10000) * mid,
        );
      }
      if (!Number.isFinite(envelopeLow) || !Number.isFinite(envelopeHigh)) {
        return;
      }
      lowValue = envelopeLow;
      highValue = envelopeHigh;
    }
    const priceLo = snapPrice(Math.min(lowValue, highValue), rowBinText);
    const priceHi = snapPrice(
      Math.max(lowValue, highValue),
      rowBinText,
      true,
    );
    if (priceLo === null || priceHi === null || toS <= fromS) {
      return;
    }
    setDrawerOpen(true);
    setDrawerTab(DRAWER_REGION);
    setRegion({ pending: true, error: null, data: null });
    const requestId = regionRequest.current + 1;
    regionRequest.current = requestId;
    apiPost<RegionResponse>(
      regionPath(symbol, priceLo, priceHi, bucket, fromS, toS),
    )
      .then((data) => {
        if (regionRequest.current === requestId) {
          setRegion({ pending: false, error: null, data });
        }
      })
      .catch((error: unknown) => {
        if (regionRequest.current === requestId) {
          setRegion({
            pending: false,
            error: error instanceof Error ? error.message : "分析失败",
            data: null,
          });
        }
      });
  };

  const localPoint = (
    event: ReactPointerEvent<HTMLElement>,
  ): { x: number; y: number } => {
    const bounds = event.currentTarget.getBoundingClientRect();
    return { x: event.clientX - bounds.left, y: event.clientY - bounds.top };
  };

  const boxOf = (drag: DragMode): Rect | null =>
    drag.kind !== "box"
      ? null
      : {
          x: Math.min(drag.x, drag.toX),
          y: Math.min(drag.y, drag.toY),
          w: Math.abs(drag.toX - drag.x),
          h: Math.abs(drag.toY - drag.y),
        };

  /** 框选矩形直写样式，不入渲染树状态。 */
  const syncBox = (rect: Rect | null): void => {
    const element = boxEl.current;
    if (element === null) {
      return;
    }
    if (rect === null) {
      element.style.setProperty("display", "none");
      return;
    }
    element.style.setProperty("display", "block");
    element.style.setProperty("left", `${String(rect.x)}px`);
    element.style.setProperty("top", `${String(rect.y)}px`);
    element.style.setProperty("width", `${String(rect.w)}px`);
    element.style.setProperty("height", `${String(rect.h)}px`);
  };

  /** 提示框行组按命中种类重建。 */
  const buildTip = (kind: string, labels: readonly string[]): void => {
    const element = tipEl.current;
    if (element === null || tipKind.current === kind) {
      return;
    }
    tipKind.current = kind;
    element.replaceChildren();
    tipCells.current = labels.map((label) => {
      const row = document.createElement("div");
      row.className = "tip-row";
      const name = document.createElement("span");
      name.className = "micro-label";
      name.textContent = label;
      const value = document.createElement("span");
      value.className = "tip-value";
      row.append(name, value);
      element.append(row);
      return value;
    });
  };

  const hideTip = (): void => {
    tipEl.current?.classList.remove("is-visible");
  };

  const showTipAt = (left: number, rows: readonly string[]): void => {
    const element = tipEl.current;
    if (element === null) {
      return;
    }
    rows.forEach((text, at) => {
      const cell = tipCells.current[at];
      if (cell !== undefined) {
        cell.textContent = text;
      }
    });
    element.style.setProperty("left", `${String(Math.round(left))}px`);
    element.classList.add("is-visible");
  };

  /** 悬浮命中一帧：分区路由，纯 DOM 写出。 */
  const hoverFrame = (x: number, y: number): void => {
    const geometry = hit.current;
    if (geometry === null) {
      hideTip();
      return;
    }
    const held = windowRef.current;
    const plot = geometry.plot;
    if (
      x >= plot.x &&
      x <= plot.x + plot.w &&
      y >= plot.y &&
      y <= plot.y + plot.h
    ) {
      // 刻线近邻优先：大额成交 kv
      for (const tick of geometry.ticks) {
        if (
          Math.abs(tick.y - y) <= TICK_HOVER_PX &&
          Math.abs(tick.x - x) <= TICK_HOVER_PX * HALF * HALF
        ) {
          buildTip("tick", TIP_TICK_LABELS);
          showTipAt(x, [
            jstStamp(tick.item.t),
            rawText(tick.item.price),
            rawText(tick.item.size),
            rawText(tick.item.side),
            rawText(tick.item.size_quantile),
          ]);
          return;
        }
      }
      const epoch =
        held.fromS + ((x - plot.x) / plot.w) * (held.toS - held.fromS);
      const slot = Math.floor(epoch / bucketS) * bucketS;
      const column = colIndex.get(slot);
      if (column === undefined) {
        hideTip();
        return;
      }
      const value =
        geometry.yHigh -
        ((y - plot.y) / plot.h) * (geometry.yHigh - geometry.yLow);
      const mid = plotNumber(column.mid);
      const bpMode = yMode === Y_MODE_BP;
      const price =
        bpMode && mid !== null ? (1 + value / 10000) * mid : value;
      const domain = geometry.yHigh - geometry.yLow;
      const valueSpan = (HIT_TOLERANCE_PX / Math.max(1, plot.h)) * domain;
      const priceSpan =
        bpMode && mid !== null ? (valueSpan / 10000) * mid : valueSpan;
      const cell =
        geometry.rowBin <= 0
          ? null
          : cellNear(
              column,
              price,
              geometry.rowBin,
              priceSpan / geometry.rowBin,
            );
      buildTip("cell", TIP_CELL_LABELS);
      showTipAt(x, [
        cell === null ? EMPTY_TEXT : rawText(cell[0]),
        jstStamp(column.t),
        cell === null ? EMPTY_TEXT : rawText(cell[2]),
        cell === null ? EMPTY_TEXT : rawText(cell[3]),
        cell === null ? EMPTY_TEXT : rawText(cell[4]),
        cell === null ? EMPTY_TEXT : rawText(cell[5]),
        cell === null || column.mid === null
          ? EMPTY_TEXT
          : `${bpDiffText(cell[0] ?? "", column.mid)}bp`,
        cell === null
          ? EMPTY_TEXT
          : liquidityRank(
              liquidityIndex,
              cell[0] ?? "",
              plotNumber(cell[2]) ?? 0,
            ),
        columnState(column),
      ]);
      return;
    }
    // 带分区：事件带走事件，余走值
    for (const band of geometry.bands) {
      const rect = band.rect;
      if (y < rect.y || y > rect.y + rect.h || x < rect.x) {
        continue;
      }
      if (band.key === "event") {
        for (const eventHit of geometry.events) {
          const box = eventHit.rect;
          if (x >= box.x && x <= box.x + box.w) {
            const feature = eventHit.feature;
            buildTip("event", TIP_EVENT_LABELS);
            showTipAt(x, [
              feature.label,
              `${rawText(feature.price_low)}–${rawText(feature.price_high)}`,
              `${jstStamp(feature.from_ts)}–${jstStamp(feature.to_ts)}`,
            ]);
            return;
          }
        }
        hideTip();
        return;
      }
      const epoch =
        held.fromS + ((x - plot.x) / plot.w) * (held.toS - held.fromS);
      const column = colIndex.get(Math.floor(epoch / bucketS) * bucketS);
      if (column === undefined) {
        hideTip();
        return;
      }
      let text: string = EMPTY_TEXT;
      if (column.gap) {
        text = "空档";
      } else if (band.key === "spread") {
        text = rawText(column.bands.spread_bp);
      } else if (band.key === "ofi") {
        text = rawText(column.bands.ofi);
      } else if (band.key === "imbalance") {
        text = rawText(column.bands.imbalance);
      } else if (band.key === "delta") {
        // 量类带随全局基准取值
        text = rawText(
          basis === BASIS_NOTIONAL
            ? (column.bands.trade_delta_notional ?? column.bands.trade_delta)
            : column.bands.trade_delta,
        );
      } else {
        text = column.bands.depth
          .map(
            (entry) =>
              `${entry[0]}bp ${
                basis === BASIS_NOTIONAL
                  ? rawText(entry[3] ?? entry[1])
                  : entry[1]
              }`,
          )
          .join(" · ");
      }
      if (column.carried && text !== EMPTY_TEXT) {
        text = `延载 · ${text}`;
      }
      buildTip("band", TIP_BAND_LABELS);
      showTipAt(x, [band.label, jstStamp(column.t), text]);
      return;
    }
    hideTip();
  };

  // 列索引：桶界取列 O(1)
  const colIndex = useMemo(() => {
    const map = new Map<number, TileColumn>();
    for (const column of fine.columns) {
      map.set(column.e, column);
    }
    return map;
  }, [fine.columns]);

  // 悬浮 rAF 合帧，绝不触发画布重绘
  const hoverPoint = useRef<{ x: number; y: number } | null>(null);
  const hoverQueued = useRef<boolean>(false);
  const hoverFrameRef = useRef<(x: number, y: number) => void>(hoverFrame);
  hoverFrameRef.current = hoverFrame;
  const scheduleHover = (): void => {
    if (hoverQueued.current) {
      return;
    }
    hoverQueued.current = true;
    const run = (): void => {
      if (!hoverQueued.current) {
        return;
      }
      hoverQueued.current = false;
      const point = hoverPoint.current;
      if (point !== null) {
        hoverFrameRef.current(point.x, point.y);
      }
    };
    window.requestAnimationFrame(run);
    window.setTimeout(run, PAINT_FALLBACK_MS);
  };

  const onMove = (event: ReactPointerEvent<HTMLDivElement>): void => {
    const point = localPoint(event);
    const drag = dragRef.current;
    if (drag.kind === "press") {
      const distance = Math.max(
        Math.abs(point.x - drag.x),
        Math.abs(point.y - drag.y),
      );
      if (distance > DRAG_THRESHOLD) {
        dragRef.current = {
          kind: "box",
          x: drag.x,
          y: drag.y,
          toX: point.x,
          toY: point.y,
        };
        syncBox(boxOf(dragRef.current));
      }
      return;
    }
    if (drag.kind === "box") {
      dragRef.current = { ...drag, toX: point.x, toY: point.y };
      syncBox(boxOf(dragRef.current));
      return;
    }
    hoverPoint.current = point;
    scheduleHover();
  };

  const onDown = (event: ReactPointerEvent<HTMLDivElement>): void => {
    const point = localPoint(event);
    try {
      event.currentTarget.setPointerCapture(event.pointerId);
    } catch {
      // 合成指针无捕获能力时忽略
    }
    dragRef.current = { kind: "press", x: point.x, y: point.y };
  };

  const onUp = (event: ReactPointerEvent<HTMLDivElement>): void => {
    const point = localPoint(event);
    const geometry = hit.current;
    const drag = dragRef.current;
    dragRef.current = { kind: "idle" };
    if (drag.kind === "box") {
      const chosen = boxOf(drag);
      syncBox(null);
      if (chosen !== null) {
        openRegion(chosen);
      }
      return;
    }
    syncBox(null);
    if (geometry === null) {
      return;
    }
    // 事件带落点点击即跳转
    for (const eventHit of geometry.events) {
      const rect = eventHit.rect;
      if (
        point.x >= rect.x &&
        point.x <= rect.x + rect.w &&
        point.y >= rect.y &&
        point.y <= rect.y + rect.h
      ) {
        jumpRef.current(eventHit.feature);
        return;
      }
    }
    const plot = geometry.plot;
    if (
      point.x < plot.x ||
      point.x > plot.x + plot.w ||
      point.y < plot.y ||
      point.y > plot.y + plot.h
    ) {
      return;
    }
    const held = windowRef.current;
    const epoch =
      held.fromS + ((point.x - plot.x) / plot.w) * (held.toS - held.fromS);
    const column = colIndex.get(Math.floor(epoch / bucketS) * bucketS);
    if (column === undefined || geometry.rowBin <= 0) {
      return;
    }
    const value =
      geometry.yHigh -
      ((point.y - plot.y) / plot.h) * (geometry.yHigh - geometry.yLow);
    const mid = plotNumber(column.mid);
    const price =
      yMode === Y_MODE_BP && mid !== null ? (1 + value / 10000) * mid : value;
    const domain = geometry.yHigh - geometry.yLow;
    const priceSpan =
      ((HIT_TOLERANCE_PX / Math.max(1, plot.h)) * domain *
        (yMode === Y_MODE_BP && mid !== null ? mid / 10000 : 1));
    const cell = cellNear(
      column,
      price,
      geometry.rowBin,
      priceSpan / geometry.rowBin,
    );
    if (cell !== null) {
      openTrack(cell[0] ?? "");
    }
  };

  const onWheel = (event: ReactWheelEvent<HTMLDivElement>): void => {
    const geometry = hit.current;
    if (geometry === null) {
      return;
    }
    const bounds = event.currentTarget.getBoundingClientRect();
    const x = event.clientX - bounds.left;
    const plot = geometry.plot;
    const ratio = Math.max(
      0,
      Math.min(1, (x - plot.x) / Math.max(1, plot.w)),
    );
    const held = windowRef.current;
    const span = held.toS - held.fromS;
    const factor = event.deltaY > 0 ? ZOOM_OUT : ZOOM_IN;
    const nextSpan = Math.max(
      MIN_SPAN_S,
      Math.min(MAX_SPAN_S, Math.round(span * factor)),
    );
    const anchor = held.fromS + span * ratio;
    setFollowLatest(false);
    moveWindowRef.current(
      quantize(
        {
          fromS: anchor - nextSpan * ratio,
          toS: anchor + nextSpan * (1 - ratio),
        },
        bucketS,
      ),
    );
  };

  // 导航条：时间量为真值，像素只是投影
  const navHit = (
    event: ReactPointerEvent<HTMLDivElement>,
    cover: TimeWindow,
  ): { x: number; epoch: number; width: number } => {
    const bounds = event.currentTarget.getBoundingClientRect();
    const x = event.clientX - bounds.left;
    const span = cover.toS - cover.fromS;
    return {
      x,
      epoch: cover.fromS + (x / Math.max(1, bounds.width)) * span,
      width: bounds.width,
    };
  };

  const onNavDown = (event: ReactPointerEvent<HTMLDivElement>): void => {
    // 拖动起点覆盖快照，冻结映射
    const cover: TimeWindow = {
      fromS: coverFromRef.current,
      toS: coverTo,
    };
    const at = navHit(event, cover);
    try {
      event.currentTarget.setPointerCapture(event.pointerId);
    } catch {
      // 合成指针无捕获能力时忽略
    }
    setFollowLatest(false);
    const geometry = hit.current;
    lockDomainRef.current =
      geometry === null
        ? null
        : { low: geometry.yLow, high: geometry.yHigh };
    const held = windowRef.current;
    const span = cover.toS - cover.fromS;
    const pxOf = (epoch: number): number =>
      ((epoch - cover.fromS) / span) * at.width;
    const fromX = pxOf(held.fromS);
    const toX = pxOf(held.toS);
    if (Math.abs(at.x - fromX) <= EDGE_GRIP) {
      dragRef.current = { kind: "nav-from", cover };
      return;
    }
    if (Math.abs(at.x - toX) <= EDGE_GRIP) {
      dragRef.current = { kind: "nav-to", cover };
      return;
    }
    if (at.x > fromX && at.x < toX) {
      dragRef.current = {
        kind: "nav-pan",
        grabS: at.epoch - held.fromS,
        cover,
      };
      return;
    }
    // 框外点击即居中跳转
    const width = held.toS - held.fromS;
    dragRef.current = { kind: "nav-pan", grabS: width / HALF, cover };
    moveWindowRef.current(
      quantize(
        { fromS: at.epoch - width / HALF, toS: at.epoch + width / HALF },
        bucketS,
      ),
    );
  };

  const onNavMove = (event: ReactPointerEvent<HTMLDivElement>): void => {
    const drag = dragRef.current;
    if (
      drag.kind !== "nav-pan" &&
      drag.kind !== "nav-from" &&
      drag.kind !== "nav-to"
    ) {
      return;
    }
    const at = navHit(event, drag.cover);
    const held = windowRef.current;
    if (drag.kind === "nav-pan") {
      const width = held.toS - held.fromS;
      const fromS = at.epoch - drag.grabS;
      moveWindowRef.current(
        quantize({ fromS, toS: fromS + width }, bucketS),
      );
      return;
    }
    if (drag.kind === "nav-from") {
      const fromS = Math.min(at.epoch, held.toS - MIN_SPAN_S);
      moveWindowRef.current(quantize({ fromS, toS: held.toS }, bucketS));
      return;
    }
    const toS = Math.max(at.epoch, held.fromS + MIN_SPAN_S);
    moveWindowRef.current(quantize({ fromS: held.fromS, toS }, bucketS));
  };

  const onNavUp = (): void => {
    dragRef.current = { kind: "idle" };
    // 松手解锁值域并吸收覆盖新增
    lockDomainRef.current = null;
    schedulePaintRef.current();
  };

  const changeBucket = (next: string): void => {
    if (UNSUPPORTED_TILE_BUCKETS.includes(next)) {
      return;
    }
    const nextS = TILE_BUCKET_SECONDS[next] ?? 1;
    const span = DEFAULT_SPAN_S[next] ?? 1800;
    const held = windowRef.current;
    setBucket(next);
    if (followLatest) {
      const latest = latestEdge(nextS);
      windowRef.current = { fromS: latest - span, toS: latest };
    } else {
      const center = (held.fromS + held.toS) / HALF;
      windowRef.current = quantize(
        { fromS: center - span / HALF, toS: center + span / HALF },
        nextS,
      );
    }
    setQuery(quantize(windowRef.current, nextS));
    schedulePaintRef.current();
  };

  /** 回到最新：恢复跟随并贴最新列。 */
  const backToLatest = (): void => {
    const latest = latestEdge(bucketS);
    const held = windowRef.current;
    windowRef.current = {
      fromS: latest - (held.toS - held.fromS),
      toS: latest,
    };
    setFollowLatest(true);
    setQuery(quantize(windowRef.current, bucketS));
    schedulePaintRef.current();
  };

  const toolbar = (
    <>
      <span className="chip-group">
        {TILE_BUCKETS.map((item) => (
          <button
            key={item}
            type="button"
            className={item === bucket ? "chip is-active" : "chip"}
            aria-pressed={item === bucket}
            disabled={UNSUPPORTED_TILE_BUCKETS.includes(item)}
            title={
              UNSUPPORTED_TILE_BUCKETS.includes(item) ? "来源不支撑" : "桶档"
            }
            onClick={() => {
              changeBucket(item);
            }}
          >
            {item}
          </button>
        ))}
      </span>
      <span className="chip-group">
        <button
          type="button"
          className={yMode === Y_MODE_ABS ? "chip is-active" : "chip"}
          aria-pressed={yMode === Y_MODE_ABS}
          title="价格轴模式"
          onClick={() => {
            setYMode(Y_MODE_ABS);
          }}
        >
          绝对价
        </button>
        <button
          type="button"
          className={yMode === Y_MODE_BP ? "chip is-active" : "chip"}
          aria-pressed={yMode === Y_MODE_BP}
          title="价格轴模式"
          onClick={() => {
            setYMode(Y_MODE_BP);
          }}
        >
          相对bp
        </button>
      </span>
      <span className="chip-group">
        {[
          { key: COLOR_LINEAR, label: "窗内线性" },
          { key: COLOR_LOG, label: "窗内对数" },
          { key: COLOR_PCT, label: "窗内百分位" },
        ].map((item) => (
          <button
            key={item.key}
            type="button"
            className={colorMode === item.key ? "chip is-active" : "chip"}
            aria-pressed={colorMode === item.key}
            title="仅按当前窗口挂量归一化，移动窗口会重新标定"
            onClick={() => {
              setColorMode(item.key);
            }}
          >
            {item.label}
          </button>
        ))}
      </span>
      {followLatest ? null : (
        <button
          type="button"
          className="chip chip--nano"
          title="恢复跟随并贴最新"
          onClick={backToLatest}
        >
          回到最新
        </button>
      )}
    </>
  );

  const recorded =
    fine.columns.some((column) => !column.gap) ||
    baseInView.some((column) => !column.gap);
  const lastGap =
    fine.columns.length > 0 &&
    fine.columns[fine.columns.length - 1]?.gap === true;
  const processName =
    symbol === null ? null : `record-${symbol.toLowerCase()}`;
  const bookIsStale =
    followLatest && orderbook !== null && freshBook(orderbook, true) === null;
  const showProcessAction = followLatest && (!recorded || lastGap);
  const missing = fine.missingDates;
  const contractBreaks = fine.meta?.contract_breaks ?? 0;
  const methodVersions = fine.meta?.method_versions ?? [];
  const shortMethods = methodVersions.map((version) =>
    version.replace(/^orderflow-tile-sparse-/, ""),
  );
  const contractFact =
    contractBreaks === 0
      ? null
      : `口径断点 ${tallyText(contractBreaks)}${
          shortMethods.length > 1 ? ` · ${shortMethods.join("/")}` : ""
        }`;
  const gapText =
    missing.length > 0
      ? `录制缺口 · ${missing.slice(0, 2).join(" / ")}${missing.length > 2 ? " …" : ""}`
      : "当前窗口无录制";

  const panelMeta = useMemo<PollMeta>(
    () => ({
      lastUpdatedAt: fine.lastLandedAt,
      stale: fine.stale || bookIsStale,
      error: fine.error,
      errorCode: fine.error === null ? null : "TILE",
      pending: fine.pending,
    }),
    [fine.lastLandedAt, fine.stale, fine.error, fine.pending, bookIsStale],
  );

  return (
    <Panel
      title="订单流"
      area="page"
      meta={panelMeta}
      facts={[
        contractFact,
        missing.length === 0 ? null : `缺 ${tallyText(missing.length)} 日`,
      ].filter((item): item is string => item !== null)}
      source="store"
      toolbar={toolbar}
      flush
      danger={false}
    >
      <div className="ofl">
        <div className="ofl-main">
          <L2QualityStrip quality={l2Quality} />
          {showProcessAction && processName !== null ? (
            <div className="ofl-pull">
              <ProcessStartButton name={processName} processes={processes} />
            </div>
          ) : null}
          <div
            className="ofl-canvas-wrap"
            ref={wrap}
            onPointerMove={onMove}
            onPointerDown={onDown}
            onPointerUp={onUp}
            onPointerLeave={() => {
              hoverPoint.current = null;
              hideTip();
            }}
            onWheel={onWheel}
          >
            <canvas className="ofl-canvas" ref={canvas} />
            {recorded ? null : (
              <p className="empty ofl-empty">
                {fine.pending ? "读取中" : gapText}
              </p>
            )}
            <i
              className="ofl-box"
              ref={boxEl}
              style={{ display: "none" }}
              aria-hidden="true"
            />
            <div className="chart-tip" ref={tipEl} />
          </div>
          <div
            className="ofl-nav"
            ref={navWrap}
            onPointerDown={onNavDown}
            onPointerMove={onNavMove}
            onPointerUp={onNavUp}
          >
            <canvas className="ofl-canvas" ref={navCanvas} />
          </div>
        </div>
        {drawerOpen ? (
          <OflDrawer
            tab={drawerTab}
            onTab={setDrawerTab}
            onClose={() => {
              setDrawerOpen(false);
              setSelected(null);
            }}
            track={track}
            region={region}
            features={featureItems}
            alerts={alerts}
            unackedIds={unackedIds}
            selected={selected}
            tickSize={tickSize}
            sizeStep={sizeStep}
            onAck={onAck}
            onJump={jumpToFeature}
          />
        ) : null}
      </div>
    </Panel>
  );
}

/** memo 边界：入参实变才重渲。 */
export const OflPage = memo(OflPageImpl);

export { DRAWER_ALERTS };
