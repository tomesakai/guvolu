import { useCallback, useEffect, useMemo, useState } from "react";
import type { ReactElement } from "react";
import {
  ALERTS_PATH,
  ALERTS_POLL_INTERVAL_MS,
  API_PATHS,
  DEFAULT_FOOTPRINT_BIN,
  DEFAULT_FOOTPRINT_INTERVAL,
  DEFAULT_HEATMAP_MINUTES,
  DEFAULT_KLINE_INTERVAL,
  FOOTPRINT_BINS,
  FOOTPRINT_INTERVALS,
  FOOTPRINT_POLL_INTERVAL_MS,
  HEATMAP_MINUTES,
  HEATMAP_POLL_INTERVAL_MS,
  KLINE_INTERVALS,
  KLINE_POLL_INTERVAL_MS,
  OPS_POLL_INTERVAL_MS,
  OPS_PROCESSES_PATH,
  ORDERBOOK_DEPTH,
  ORDERBOOK_POLL_INTERVAL_MS,
  POLL_INTERVAL_MS,
  alertAckPath,
  apiPost,
  marketFootprintV2Path,
  heatmapPath,
  marketKlinesV2Path,
  marketL2LatestV2Path,
  marketL2QualityV2Path,
  presetsFor,
  resolveRange,
  symbolPath,
  usePolling,
  useStable,
} from "./api";
import type {
  ActiveOrdersResponse,
  AlertsResponse,
  AssetsResponse,
  CapabilitiesResponse,
  DayRange,
  ExecutionsResponse,
  FootprintBarItem,
  FootprintResponse,
  HealthResponse,
  HeatmapResponse,
  KlineItem,
  KlinesResponse,
  L2QualityResponse,
  MarketsCatalogResponse,
  OpsProcessesResponse,
  OrderbookResponse,
  PollMeta,
  ServiceStatusResponse,
  SymbolsResponse,
  TickerResponse,
} from "./api";
import {
  DEFAULT_CHART_KIND,
  FOOTPRINT_KIND,
  FOOT_SUB_DELTA,
  FOOT_SUB_STACKED,
  FOOT_SUB_VOLUME,
  KlineChart,
  NO_MARKERS,
} from "./chart";
import type { ChartNavigationAction, ChartNavigationCommand } from "./chart";
import { footprintCoverageClipped } from "./orderflow-stack";
import { BookHeatmap } from "./heatmap";
import { DRAWER_ALERTS, OflPage } from "./ofl";
import { OB_BAND_WIDTHS, OrderbookLadder, obBandLabel } from "./orderbook";
import { L2QualityStrip } from "./l2-quality";
import {
  ActiveOrdersTable,
  AssetsTable,
  Badge,
  CapabilitiesPanel,
  EmptyBlock,
  ExecutionsTable,
  FreshnessBadge,
  MetricCell,
  OpsProcessesSection,
  Panel,
  assetZeroCount,
} from "./panels";
import {
  BASIS_NOTIONAL,
  EMPTY_TEXT,
  JST_LABEL,
  META_SEPARATOR,
  applyFormatSwitches,
  driftExceeded,
  driftText,
  notionalText,
  priceText,
  rawText,
  relativeAge,
  tallyText,
  totalText,
  unitCountText,
} from "./format";
import type { FormatSwitches } from "./format";
import {
  SettingsPage,
  loadDisplaySettings,
  storeDisplaySettings,
} from "./settings";
import type { DisplaySettings } from "./settings";

// 前端固定为查看态，不承载任何状态
const FRONTEND_MODE = "view";
// 空清单共用引用，避免逐渲新建
const NO_KLINES: KlineItem[] = [];
const NO_FOOT_BARS: FootprintBarItem[] = [];
const MONITOR_PAGE = "MON";
const ORDER_FLOW_PAGE = "OFL";
const CAPABILITY_PAGE = "CAP";
const SETTINGS_PAGE = "SET";
const KILL_SWITCH_COMMAND = "python -m guvolu.ops.kill_switch";
// 热力图数据只来自本地录制库存
const HEATMAP_SOURCE = "store";
const MS_PER_SECOND = 1000;
// 状态条声明刷新周期区间
const REFRESH_TEXT = `${String(ORDERBOOK_POLL_INTERVAL_MS / MS_PER_SECOND)}s–${String(
  KLINE_POLL_INTERVAL_MS / MS_PER_SECOND,
)}s`;
const PRICE_UNIT = "JPY";
// 范围起止连接符
const RANGE_DASH = "–";
// 热力图时间跨度单位
const MINUTE_UNIT = "min";

interface ModeState {
  readonly key: string;
  readonly label: string;
  readonly tone: string;
  readonly hint: string;
}

// 运行三态常驻可见
const MODE_STATES: readonly ModeState[] = [
  { key: FRONTEND_MODE, label: "查看", tone: "view", hint: "前端模式" },
  { key: "dry-run", label: "模拟运行", tone: "dry", hint: "后端模式" },
  { key: "live", label: "实盘", tone: "live", hint: "后端模式" },
];

interface RailItem {
  readonly code: string;
  readonly hint: string;
  readonly disabled: boolean;
}

type DockView = "assets" | "orders" | "executions" | "heatmap";

// 模块 rail 即未来功能预留位
const RAIL_MAIN: readonly RailItem[] = [
  { code: MONITOR_PAGE, hint: "监控", disabled: false },
  { code: "OPS", hint: "操作面 阶段 6b", disabled: true },
  { code: "RPL", hint: "回放 阶段 8", disabled: true },
  { code: "RSC", hint: "研究 阶段 9", disabled: true },
  { code: ORDER_FLOW_PAGE, hint: "订单流", disabled: false },
];

function serviceTone(status: string | null): string {
  if (status === "OPEN") {
    return "positive";
  }
  if (status === "PREOPEN") {
    return "warning";
  }
  if (status === "MAINTENANCE") {
    return "danger";
  }
  return "muted";
}

function countText(size: number | undefined, unit = "行"): string {
  return size === undefined ? EMPTY_TEXT : `${tallyText(size)} ${unit}`;
}

// 任一端缺失即整体空值，不拼破折号
function spanText(
  low: string | null | undefined,
  high: string | null | undefined,
  tickSize: string,
): string {
  const start = priceText(low, tickSize);
  const end = priceText(high, tickSize);
  if (start === EMPTY_TEXT || end === EMPTY_TEXT) {
    return EMPTY_TEXT;
  }
  return `${start}${RANGE_DASH}${end}`;
}

function ModeBadges({ health }: { health: HealthResponse | null }): ReactElement {
  const backendMode = health === null ? null : health.mode;
  const mismatched = health !== null && health.frontend_mode !== FRONTEND_MODE;
  return (
    <>
      {MODE_STATES.map((state) => {
        const on = state.key === FRONTEND_MODE || state.key === backendMode;
        const broken = mismatched && state.key === FRONTEND_MODE;
        const tone = broken ? "danger" : on ? state.tone : "off";
        const hint = broken ? "前端模式与后端声明不一致" : state.hint;
        return (
          <Badge key={state.key} tone={tone} text={state.label} hint={hint} />
        );
      })}
    </>
  );
}

function ConnectionBadge({ polls }: { polls: readonly PollMeta[] }): ReactElement {
  const failed = polls.find((item) => item.stale && item.pending);
  if (failed !== undefined) {
    return (
      <Badge
        tone="danger"
        text={failed.errorCode ?? EMPTY_TEXT}
        hint="查询服务连接"
      />
    );
  }
  const stale = polls.find((item) => item.stale);
  if (stale !== undefined) {
    return (
      <Badge
        tone="warning"
        text={`陈旧 ${relativeAge(stale.lastUpdatedAt)}`}
        hint="查询服务连接"
      />
    );
  }
  return <Badge tone="positive" text="连接" hint="查询服务连接" />;
}

export default function App(): ReactElement {
  const health = usePolling<HealthResponse>(API_PATHS.health, POLL_INTERVAL_MS);
  const serviceStatus = usePolling<ServiceStatusResponse>(
    API_PATHS.serviceStatus,
    POLL_INTERVAL_MS,
  );
  const assets = usePolling<AssetsResponse>(API_PATHS.assets, POLL_INTERVAL_MS);
  const symbols = usePolling<SymbolsResponse>(API_PATHS.symbols, POLL_INTERVAL_MS);
  const capabilities = usePolling<CapabilitiesResponse>(
    API_PATHS.capabilities,
    POLL_INTERVAL_MS,
  );
  const marketCatalog = usePolling<MarketsCatalogResponse>(
    API_PATHS.marketsV2,
    POLL_INTERVAL_MS,
  );

  const [page, setPage] = useState<string>(MONITOR_PAGE);
  const [selectedMarketId, setSelectedMarketId] = useState<string | null>(null);
  const [assetSource, setAssetSource] = useState<string>("all");
  const [assetShowZero, setAssetShowZero] = useState<boolean>(false);
  const [killOpen, setKillOpen] = useState<boolean>(false);
  // 显示设置：装载即写入变换器开关
  const [settings, setSettings] = useState<DisplaySettings>(() => {
    const loaded = loadDisplaySettings();
    applyFormatSwitches(loaded.display);
    return loaded;
  });
  const display = settings.display;
  const [chartInterval, setChartInterval] = useState<string>(
    DEFAULT_KLINE_INTERVAL,
  );
  const [presetKey, setPresetKey] = useState<string>(
    () => presetsFor(DEFAULT_KLINE_INTERVAL)[0]?.key ?? "",
  );
  const [tradingDay, setTradingDay] = useState<string | null>(null);
  const [logScale, setLogScale] = useState<boolean>(false);
  const [heatMinutes, setHeatMinutes] = useState<number>(
    DEFAULT_HEATMAP_MINUTES,
  );
  const chartKind = settings.chartKind;
  // 足迹状态：档位、口径、派生层开关
  const [footBin, setFootBin] = useState<string>(DEFAULT_FOOTPRINT_BIN);
  const [footSub, setFootSub] = useState<string>(FOOT_SUB_VOLUME);
  const [footCvd, setFootCvd] = useState<boolean>(true);
  const [footPoc, setFootPoc] = useState<boolean>(true);
  const [footVa, setFootVa] = useState<boolean>(true);
  // 主图派生层两开关，缺省关
  const [showExtremes, setShowExtremes] = useState<boolean>(false);
  const [showBookAxis, setShowBookAxis] = useState<boolean>(false);
  const [chartAwayFromLatest, setChartAwayFromLatest] =
    useState<boolean>(false);
  const [chartNavigation, setChartNavigation] =
    useState<ChartNavigationCommand | null>(null);
  // 盘口带内宽度与等档显示
  const [obBand, setObBand] = useState<string>("10");
  const [obGapScaled, setObGapScaled] = useState<boolean>(false);
  // 底部停靠栏：同一时刻只展开一个次级数据域
  const [dockView, setDockView] = useState<DockView | null>(null);
  // 报警确认的本地先行覆盖
  const [ackedLocal, setAckedLocal] = useState<ReadonlySet<number>>(
    () => new Set<number>(),
  );
  // 状态条跳转 OFL 报警页签的意图
  const [oflIntent, setOflIntent] = useState<{ tab: string | null; seq: number }>(
    { tab: null, seq: 0 },
  );

  // 仅列含活动 head 的市场。
  const availableMarkets = useMemo(
    () =>
      marketCatalog.data?.markets.filter(
        (item) => Object.keys(item.domains).length > 0,
      ) ?? [],
    [marketCatalog.data],
  );
  const selectedMarket =
    availableMarkets.find((item) => item.market_id === selectedMarketId) ?? null;
  const selected = selectedMarket?.venue_symbol ?? null;
  const legacyGmoSymbol = selectedMarket?.venue_id === "gmo" ? selected : null;

  useEffect(() => {
    if (availableMarkets.length === 0) {
      return;
    }
    if (!availableMarkets.some((item) => item.market_id === selectedMarketId)) {
      const preferred = availableMarkets.find(
        (item) => item.market_id === "mkt__gmo__btc__r0",
      );
      setSelectedMarketId(preferred?.market_id ?? availableMarkets[0]?.market_id ?? null);
    }
  }, [availableMarkets, selectedMarketId]);

  const changeDisplay = (next: FormatSwitches): void => {
    // 先写开关再触发重渲染，全应用即时生效
    applyFormatSwitches(next);
    setSettings((current) => {
      const updated = { ...current, display: next };
      storeDisplaySettings(updated);
      return updated;
    });
  };

  const changeChartKind = (next: string): void => {
    setSettings((current) => {
      const updated = { ...current, chartKind: next };
      storeDisplaySettings(updated);
      return updated;
    });
  };

  const presets = presetsFor(chartInterval);
  const preset = presets.find((item) => item.key === presetKey) ?? presets[0];
  const range: DayRange | null =
    tradingDay === null || preset === undefined
      ? null
      : resolveRange(preset, tradingDay);

  const fallbackRule =
    symbols.data?.rules.find((item) => item.symbol === selected) ?? null;
  const tickSize = selectedMarket?.tick_size ?? fallbackRule?.tick_size ?? "";
  const sizeStep = selectedMarket?.size_step ?? fallbackRule?.size_step ?? "";
  const isFootprint = chartKind === FOOTPRINT_KIND;

  const chartPath = marketKlinesV2Path(selectedMarketId, chartInterval, range);
  const footPath = isFootprint
    ? marketFootprintV2Path(selectedMarketId, chartInterval, range, footBin)
    : null;
  // 视图键与端点无关，六型往返保持视窗
  const chartViewKey = `${selectedMarketId ?? ""}|${chartInterval}|${range?.from ?? ""}-${range?.to ?? ""}`;

  useEffect(() => {
    setChartAwayFromLatest(false);
  }, [chartViewKey]);

  const ticker = usePolling<TickerResponse>(
    symbolPath("ticker", legacyGmoSymbol),
    POLL_INTERVAL_MS,
  );
  const klines = usePolling<KlinesResponse>(chartPath, KLINE_POLL_INTERVAL_MS);
  const footprint = usePolling<FootprintResponse>(
    footPath,
    FOOTPRINT_POLL_INTERVAL_MS,
  );
  const activeOrders = usePolling<ActiveOrdersResponse>(
    symbolPath("active-orders", legacyGmoSymbol),
    POLL_INTERVAL_MS,
  );
  const executions = usePolling<ExecutionsResponse>(
    symbolPath("latest-executions", legacyGmoSymbol),
    POLL_INTERVAL_MS,
  );
  const orderbook = usePolling<OrderbookResponse>(
    marketL2LatestV2Path(selectedMarketId, ORDERBOOK_DEPTH),
    ORDERBOOK_POLL_INTERVAL_MS,
  );
  const l2Quality = usePolling<L2QualityResponse>(
    marketL2QualityV2Path(selectedMarketId),
    ORDERBOOK_POLL_INTERVAL_MS,
  );
  const heatmap = usePolling<HeatmapResponse>(
    heatmapPath(legacyGmoSymbol, heatMinutes),
    HEATMAP_POLL_INTERVAL_MS,
  );
  // 报警与采集进程为全局横切数据
  const alerts = usePolling<AlertsResponse>(ALERTS_PATH, ALERTS_POLL_INTERVAL_MS);
  const ops = usePolling<OpsProcessesResponse>(
    OPS_PROCESSES_PATH,
    OPS_POLL_INTERVAL_MS,
  );

  // 报警清单按内容稳定引用，隔离轮询时戳噪音
  const alertItems = useStable(alerts.data?.items ?? []);
  const unackedIds = useMemo<ReadonlySet<number>>(
    () =>
      new Set(
        alertItems
          .filter(
            (item) => item.acked_at === null && !ackedLocal.has(item.alert_id),
          )
          .map((item) => item.alert_id),
      ),
    [alertItems, ackedLocal],
  );

  const ackAlert = useCallback((alertId: number): void => {
    // 确认是唯一交互写动作，仅改呈现
    apiPost<unknown>(alertAckPath(alertId))
      .then(() => {
        setAckedLocal((previous) => {
          const next = new Set(previous);
          next.add(alertId);
          return next;
        });
      })
      .catch(() => {
        // 失败保持未确认态
      });
  }, []);

  // 按 market_id 隔离旧响应。
  const currentKlines =
    klines.data?.market?.market_id === selectedMarketId ? klines.data : null;
  const currentFootprint =
    footprint.data?.market?.market_id === selectedMarketId ? footprint.data : null;
  const currentOrderbook =
    orderbook.data?.market?.market_id === selectedMarketId ? orderbook.data : null;
  const currentL2Quality =
    !l2Quality.stale && l2Quality.data?.market_id === selectedMarketId
      ? l2Quality.data
      : null;
  const klineMeta = currentKlines?.meta;
  const metaToday = klineMeta?.today;

  useEffect(() => {
    if (metaToday !== undefined && metaToday !== tradingDay) {
      setTradingDay(metaToday);
    }
  }, [metaToday, tradingDay]);

  const status = serviceStatus.data?.status ?? null;
  const drift = serviceStatus.data?.clock_drift_seconds ?? null;
  const klineItems = currentKlines?.items ?? NO_KLINES;
  const symbolText = selectedMarket === null
    ? EMPTY_TEXT
    : `${selectedMarket.venue_id} ${selectedMarket.instrument_id}`;
  const footMeta = currentFootprint?.meta;
  const footBars = currentFootprint?.bars ?? NO_FOOT_BARS;
  const activeMeta = isFootprint ? footMeta : klineMeta;
  const rangeText =
    activeMeta === undefined
      ? EMPTY_TEXT
      : `${activeMeta.from}${RANGE_DASH}${activeMeta.to}`;
  // 页脚右槽：档宽值加数据源标注
  const footSources = [...new Set(footBars.map((bar) => bar.source))];
  const footSourceText =
    footSources.length === 0 ? null : footSources.sort().join("+");
  const chartSource = isFootprint
    ? footMeta?.bin == null
      ? null
      : `${footMeta.bin}${footSourceText === null ? "" : META_SEPARATOR + footSourceText}`
    : (klineMeta?.source ?? null);

  const allPolls: readonly PollMeta[] = [
    health,
    serviceStatus,
    assets,
    symbols,
    capabilities,
    marketCatalog,
    ticker,
    klines,
    footprint,
    activeOrders,
    executions,
    orderbook,
    heatmap,
    alerts,
    ops,
  ];

  // 选择器元素记忆化供 memo 边界
  const symbolSelect = useMemo(
    () => (
      <select
        className="select"
        aria-label="品种"
        value={selectedMarketId ?? ""}
        disabled={availableMarkets.length === 0}
        onChange={(event) => {
          setSelectedMarketId(event.target.value);
        }}
      >
        {availableMarkets.length === 0 ? (
          <option value="">{EMPTY_TEXT}</option>
        ) : null}
        {availableMarkets.map((item) => (
          <option key={item.market_id} value={item.market_id}>
            {item.venue_id} · {item.instrument_id}
          </option>
        ))}
      </select>
    ),
    [selectedMarketId, availableMarkets],
  );

  const changeInterval = (next: string): void => {
    setChartInterval(next);
    const allowed = presetsFor(next);
    if (!allowed.some((item) => item.key === presetKey)) {
      const fallback = allowed[0];
      if (fallback !== undefined) {
        setPresetKey(fallback.key);
      }
    }
  };

  const navigateChart = (action: ChartNavigationAction): void => {
    setChartNavigation((current) => ({
      id: (current?.id ?? 0) + 1,
      action,
    }));
  };

  const footprintControls = (
    <>
      <span className="chip-group">
        {FOOTPRINT_BINS.map((item) => (
          <button
            key={item}
            type="button"
            className={item === footBin ? "chip is-active" : "chip"}
            aria-pressed={item === footBin}
            title="档位"
            onClick={() => {
              setFootBin(item);
            }}
          >
            {item === "auto" ? "自动" : item}
          </button>
        ))}
      </span>
      <span className="chip-group">
        <button
          type="button"
          className={footPoc ? "chip is-active" : "chip"}
          aria-pressed={footPoc}
          title="会话控制价位线"
          onClick={() => {
            setFootPoc((previous) => !previous);
          }}
        >
          POC
        </button>
        <button
          type="button"
          className={footVa ? "chip is-active" : "chip"}
          aria-pressed={footVa}
          title="价值区边界线"
          onClick={() => {
            setFootVa((previous) => !previous);
          }}
        >
          VA
        </button>
        <button type="button" className="chip" disabled title="判读标记 未实现">
          判读
        </button>
      </span>
      <span className="chip-group">
        <button
          type="button"
          className={footSub === FOOT_SUB_VOLUME ? "chip is-active" : "chip"}
          aria-pressed={footSub === FOOT_SUB_VOLUME}
          title="副窗格口径"
          onClick={() => {
            setFootSub(FOOT_SUB_VOLUME);
          }}
        >
          量
        </button>
        <button
          type="button"
          className={footSub === FOOT_SUB_DELTA ? "chip is-active" : "chip"}
          aria-pressed={footSub === FOOT_SUB_DELTA}
          title="副窗格口径"
          onClick={() => {
            setFootSub(FOOT_SUB_DELTA);
          }}
        >
          Delta
        </button>
        <button
          type="button"
          className={footSub === FOOT_SUB_STACKED ? "chip is-active" : "chip"}
          aria-pressed={footSub === FOOT_SUB_STACKED}
          title="每周期主动卖与主动买的非负构成；未知侧留空"
          onClick={() => {
            setFootSub(FOOT_SUB_STACKED);
          }}
        >
          买卖构成
        </button>
        <button
          type="button"
          className={footCvd ? "chip is-active" : "chip"}
          aria-pressed={footCvd}
          disabled={chartInterval === "1day"}
          title="会话内累计 Delta 窗格"
          onClick={() => {
            setFootCvd((previous) => !previous);
          }}
        >
          CVD
        </button>
      </span>
    </>
  );

  const chartControls = (
    <>
      <span className="select-wrap">
        <select
          className="select"
          aria-label="周期"
          value={chartInterval}
          onChange={(event) => {
            changeInterval(event.target.value);
          }}
        >
          {(isFootprint ? FOOTPRINT_INTERVALS : KLINE_INTERVALS).map((item) => (
            <option key={item} value={item}>
              {item}
            </option>
          ))}
        </select>
      </span>
      <span className="chip-group">
        {presets.map((item) => (
          <button
            key={item.key}
            type="button"
            className={item.key === presetKey ? "chip is-active" : "chip"}
            aria-pressed={item.key === presetKey}
            onClick={() => {
              setPresetKey(item.key);
            }}
          >
            {item.label}
          </button>
        ))}
      </span>
      <button
        type="button"
        className={isFootprint ? "chip is-active" : "chip"}
        aria-pressed={isFootprint}
        title="足迹图型"
        onClick={() => {
          if (isFootprint) {
            changeChartKind(DEFAULT_CHART_KIND);
            return;
          }
          if (!FOOTPRINT_INTERVALS.includes(chartInterval)) {
            // 足迹不支持的周期回缺省档
            changeInterval(DEFAULT_FOOTPRINT_INTERVAL);
          }
          setSettings((current) => ({ ...current, chartKind: FOOTPRINT_KIND }));
        }}
      >
        足迹
      </button>
      <span className="chip-group">
        <button
          type="button"
          className={logScale ? "chip" : "chip is-active"}
          aria-pressed={!logScale}
          title="价格轴刻度"
          onClick={() => {
            setLogScale(false);
          }}
        >
          线性
        </button>
        <button
          type="button"
          className={logScale ? "chip is-active" : "chip"}
          aria-pressed={logScale}
          title="价格轴刻度"
          onClick={() => {
            setLogScale(true);
          }}
        >
          对数
        </button>
      </span>
      <span className="chip-group">
        <button
          type="button"
          className={showExtremes ? "chip is-active" : "chip"}
          aria-pressed={showExtremes}
          title="查询区间高低线"
          onClick={() => {
            setShowExtremes((previous) => !previous);
          }}
        >
          高低
        </button>
        <button
          type="button"
          className={showBookAxis ? "chip is-active" : "chip"}
          aria-pressed={showBookAxis}
          title="盘口轴投影"
          onClick={() => {
            setShowBookAxis((previous) => !previous);
          }}
        >
          盘口
        </button>
      </span>
      <span className="chip-group">
        <button
          type="button"
          className={`chip chart-nav-action${chartAwayFromLatest ? "" : " is-hidden"}`}
          aria-hidden={!chartAwayFromLatest}
          disabled={!chartAwayFromLatest}
          title="回到最新端并保留当前时间尺度"
          onClick={() => {
            navigateChart("latest");
          }}
        >
          最新
        </button>
        <button
          type="button"
          className="chip"
          title="适配当前查询范围并恢复价格自动尺度"
          onClick={() => {
            navigateChart("fit");
          }}
        >
          适配
        </button>
      </span>
      {isFootprint ? footprintControls : null}
      {activeMeta?.truncated === true ? (
        <Badge tone="warning" text="截断" hint="范围超上限，仅保留最新段" />
      ) : null}
    </>
  );

  // 带内宽度与等档显示
  const obToolbar = (
    <>
      <span className="chip-group">
        {OB_BAND_WIDTHS.map((item) => (
          <button
            key={item}
            type="button"
            className={item === obBand ? "chip is-active" : "chip"}
            aria-pressed={item === obBand}
            title="带内宽度"
            onClick={() => {
              setObBand(item);
            }}
          >
            {obBandLabel(item)}
          </button>
        ))}
      </span>
      <button
        type="button"
        className={obGapScaled ? "chip chip--nano is-active" : "chip chip--nano"}
        aria-pressed={obGapScaled}
        title="按实际 bp 距离显示相邻报价之间的空档"
        onClick={() => {
          setObGapScaled((current) => !current);
        }}
      >
        等档
      </button>
    </>
  );
  const obSpreadTicks =
    currentOrderbook === null
      ? EMPTY_TEXT
      : unitCountText(currentOrderbook.spread, tickSize);

  const zeroAssetCount = assetZeroCount(assets.data, assetSource);
  const assetToolbar = (
    <>
      <span className="select-wrap">
        <select
          className="select"
          aria-label="资产来源"
          value={assetSource}
          onChange={(event) => {
            setAssetSource(event.target.value);
            setAssetShowZero(false);
          }}
        >
          <option value="all">总览</option>
          {(assets.data?.sources ?? []).map((source) => (
            <option key={source.id} value={source.id}>
              {source.label}
            </option>
          ))}
        </select>
      </span>
      {zeroAssetCount > 0 ? (
        <button
          type="button"
          className={assetShowZero ? "chip is-active" : "chip"}
          aria-expanded={assetShowZero}
          title="零余额资产"
          onClick={() => {
            setAssetShowZero((previous) => !previous);
          }}
        >
          {`${assetShowZero ? "▾" : "▸"} 0 ${String(zeroAssetCount)}`}
        </button>
      ) : null}
    </>
  );
  const assetInlineHeader = (
    <span className="asset-inline-header">
      <span className="micro-label">资产</span>
      {assetToolbar}
      <span className="badge-slot">
        <FreshnessBadge meta={assets} />
      </span>
    </span>
  );

  const heatControls = (
    <>
      <span className="chip-group">
        {HEATMAP_MINUTES.map((item) => (
          <button
            key={item}
            type="button"
            className={item === heatMinutes ? "chip is-active" : "chip"}
            aria-pressed={item === heatMinutes}
            title="时间跨度"
            onClick={() => {
              setHeatMinutes(item);
            }}
          >
            {`${String(item)}${MINUTE_UNIT}`}
          </button>
        ))}
      </span>
      <button
        type="button"
        className="chip"
        title="订单流"
        onClick={() => {
          setPage(ORDER_FLOW_PAGE);
        }}
      >
        {ORDER_FLOW_PAGE}
      </button>
    </>
  );

  const toggleDockView = (next: DockView): void => {
    setDockView((current) => (current === next ? null : next));
  };
  const dockDetail =
    dockView === "assets" ? (
      <AssetsTable
        data={assets.data}
        source={assetSource}
        showZero={assetShowZero}
        header={assetInlineHeader}
      />
    ) : dockView === "orders" ? (
      <ActiveOrdersTable
        data={activeOrders.data}
        tickSize={tickSize}
        sizeStep={sizeStep}
      />
    ) : dockView === "executions" ? (
      <ExecutionsTable
        data={executions.data}
        tickSize={tickSize}
        sizeStep={sizeStep}
        basis={display.valueBasis}
      />
    ) : dockView === "heatmap" ? (
      <BookHeatmap
        data={heatmap.data}
        symbol={symbolText}
        tickSize={tickSize}
        grouping={display.groupDigits}
        processes={ops.data}
      />
    ) : null;
  const dockTitle =
    dockView === "assets"
      ? "资产"
      : dockView === "orders"
      ? "挂单"
      : dockView === "executions"
        ? "成交"
        : "热力";
  const dockMeta =
    dockView === "assets"
      ? assets
      : dockView === "orders"
      ? activeOrders
      : dockView === "executions"
        ? executions
        : heatmap;
  const monitorDock = (
    <section
      className={
        dockView === null ? "monitor-dock" : "monitor-dock is-expanded"
      }
      aria-label="底部停靠栏"
    >
      <div className="monitor-dock__tabs" aria-label="次级面板">
        <button
          type="button"
          className={dockView === "assets" ? "monitor-dock__tab is-active" : "monitor-dock__tab"}
          aria-pressed={dockView === "assets"}
          title="资产"
          onClick={() => {
            toggleDockView("assets");
          }}
        >
          <span aria-hidden="true">¥</span>
          <span className="visually-hidden">资产</span>
        </button>
        <button
          type="button"
          className={dockView === "orders" ? "monitor-dock__tab is-active" : "monitor-dock__tab"}
          aria-pressed={dockView === "orders"}
          title="挂单"
          onClick={() => {
            toggleDockView("orders");
          }}
        >
          <span aria-hidden="true">▤</span>
          <span className="visually-hidden">挂单</span>
        </button>
        <button
          type="button"
          className={dockView === "executions" ? "monitor-dock__tab is-active" : "monitor-dock__tab"}
          aria-pressed={dockView === "executions"}
          title="成交"
          onClick={() => {
            toggleDockView("executions");
          }}
        >
          <span aria-hidden="true">⇄</span>
          <span className="visually-hidden">成交</span>
        </button>
        <button
          type="button"
          className={dockView === "heatmap" ? "monitor-dock__tab is-active" : "monitor-dock__tab"}
          aria-pressed={dockView === "heatmap"}
          title="热力图"
          onClick={() => {
            toggleDockView("heatmap");
          }}
        >
          <span aria-hidden="true">▦</span>
          <span className="visually-hidden">热力图</span>
        </button>
      </div>
      {dockDetail === null ? null : (
        <div className={`monitor-dock__detail monitor-dock__detail--${dockView}`}>
          {dockView === "assets" ? null : (
            <div className="monitor-dock__detail-head">
              <span className="micro-label">{dockTitle}</span>
              {dockView === "heatmap" ? (
                <span className="monitor-dock__detail-controls">{heatControls}</span>
              ) : null}
              <span className="monitor-dock__detail-meta">
                {dockView === "orders"
                  ? countText(activeOrders.data?.items.length)
                  : dockView === "executions"
                    ? countText(executions.data?.items.length)
                    : `价区 ${spanText(heatmap.data?.price_low, heatmap.data?.price_high, tickSize)}`}
                {dockView === "heatmap" ? <span>{HEATMAP_SOURCE}</span> : null}
                <FreshnessBadge meta={dockMeta} />
              </span>
            </div>
          )}
          <div className="monitor-dock__detail-body">{dockDetail}</div>
        </div>
      )}
    </section>
  );

  const killPanel = killOpen ? (
    <Panel
      title="紧急停止"
      area="kill"
      meta={null}
      facts={[]}
      source={null}
      toolbar={null}
      flush={false}
      danger
    >
      <code className="command-text">{KILL_SWITCH_COMMAND}</code>
    </Panel>
  ) : null;

  // 能力、设置与订单流为整幅单面板页
  const singlePage =
    page === CAPABILITY_PAGE ||
    page === SETTINGS_PAGE ||
    page === ORDER_FLOW_PAGE;
  const workspaceClass = [
    "workspace",
    singlePage ? "workspace--page" : "workspace--mon",
  ];
  if (killOpen) {
    workspaceClass.push("has-kill");
  }

  return (
    <div className="shell">
      <header className="command-bar">
        <span className="wordmark">guvolu</span>
        <input
          className="input command-input"
          type="text"
          disabled
          aria-label="命令面板"
          placeholder="命令面板 阶段 6b"
        />
        <span className="command-bar__symbol">{symbolSelect}</span>
      </header>

      <nav className="rail" aria-label="模块">
        {RAIL_MAIN.map((item) => (
          <button
            key={item.code}
            type="button"
            className={page === item.code ? "rail-btn is-active" : "rail-btn"}
            title={item.hint}
            disabled={item.disabled}
            onClick={() => {
              setPage(item.code);
            }}
          >
            {item.code}
          </button>
        ))}
        <button
          type="button"
          className={
            page === CAPABILITY_PAGE
              ? "rail-btn rail-btn--cluster is-active"
              : "rail-btn rail-btn--cluster"
          }
          title="能力范围"
          onClick={() => {
            setPage(CAPABILITY_PAGE);
          }}
        >
          {CAPABILITY_PAGE}
        </button>
        <button
          type="button"
          className={
            page === SETTINGS_PAGE ? "rail-btn is-active" : "rail-btn"
          }
          title="设置"
          onClick={() => {
            setPage(SETTINGS_PAGE);
          }}
        >
          {SETTINGS_PAGE}
        </button>
        <button
          type="button"
          className={
            killOpen
              ? "rail-btn rail-btn--danger is-active"
              : "rail-btn rail-btn--danger"
          }
          title="紧急停止"
          aria-pressed={killOpen}
          onClick={() => {
            setKillOpen((previous) => !previous);
          }}
        >
          KILL
        </button>
      </nav>

      <main className={workspaceClass.join(" ")}>
        {killPanel}

        {page === SETTINGS_PAGE ? (
          <Panel
            title="设置"
            area="page"
            meta={null}
            facts={[]}
            source={null}
            toolbar={null}
            flush={false}
            danger={false}
          >
            <SettingsPage
              display={display}
              chartKind={isFootprint ? DEFAULT_CHART_KIND : chartKind}
              onChange={changeDisplay}
              onChartKindChange={changeChartKind}
            />
          </Panel>
        ) : page === CAPABILITY_PAGE ? (
          <Panel
            title="能力"
            area="page"
            meta={capabilities}
            facts={[]}
            source={null}
            toolbar={null}
            flush={false}
            danger={false}
          >
            <OpsProcessesSection data={ops.data} />
            <CapabilitiesPanel data={capabilities.data} />
          </Panel>
        ) : page === ORDER_FLOW_PAGE ? (
          <OflPage
            marketId={selectedMarketId}
            symbol={legacyGmoSymbol}
            tickSize={tickSize}
            sizeStep={sizeStep}
            basis={display.valueBasis}
            orderbook={currentOrderbook}
            alerts={alertItems}
            unackedIds={unackedIds}
            onAck={ackAlert}
            intentTab={oflIntent.tab}
            intentSeq={oflIntent.seq}
            processes={ops.data}
            l2Quality={currentL2Quality}
          />
        ) : (
          <>
            <Panel
              title="最新价"
              area="mkt"
              meta={ticker}
              facts={[]}
              source={null}
              toolbar={null}
              flush={false}
              danger={false}
              header={false}
            >
              {ticker.data === null ? (
                <EmptyBlock text={EMPTY_TEXT} />
              ) : (
                <div className="market-row">
                  <MetricCell
                    label="last"
                    value={priceText(ticker.data.last, tickSize)}
                  />
                  <MetricCell
                    label="ask"
                    value={priceText(ticker.data.ask, tickSize)}
                  />
                  <MetricCell
                    label="bid"
                    value={priceText(ticker.data.bid, tickSize)}
                  />
                  <MetricCell
                    label="high"
                    value={priceText(ticker.data.high, tickSize)}
                  />
                  <MetricCell
                    label="low"
                    value={priceText(ticker.data.low, tickSize)}
                  />
                  <MetricCell
                    label="volume"
                    value={
                      display.valueBasis === BASIS_NOTIONAL
                        ? notionalText(
                            ticker.data.last,
                            ticker.data.volume,
                            tickSize,
                            sizeStep,
                          )
                        : totalText(ticker.data.volume, sizeStep)
                    }
                  />
                </div>
              )}
            </Panel>

            <Panel
              title={isFootprint ? "足迹" : ""}
              area="cht"
              meta={isFootprint ? footprint : klines}
              facts={[
                `范围 ${rangeText}`,
                PRICE_UNIT,
              ]}
              source={chartSource}
              toolbar={chartControls}
              flush={false}
              danger={false}
            >
              {klineItems.length > 0 || footBars.length > 0 ? (
                <KlineChart
                  items={klineItems}
                  footprint={isFootprint ? currentFootprint : null}
                  viewKey={chartViewKey}
                  kind={chartKind}
                  subKind={footSub}
                  showCvd={footCvd && chartInterval !== "1day"}
                  showPoc={footPoc}
                  showVa={footVa}
                  showExtremes={showExtremes}
                  showBookAxis={showBookAxis}
                  orderbook={currentOrderbook}
                  logScale={logScale}
                  markers={NO_MARKERS}
                  tickSize={tickSize}
                  sizeStep={sizeStep}
                  display={display}
                  footprintPending={footprint.pending}
                  footprintStale={footprint.stale}
                  footprintCoverageClipped={footprintCoverageClipped(
                    currentFootprint?.meta ?? null,
                  )}
                  navigationCommand={chartNavigation}
                  onAwayFromLatestChange={setChartAwayFromLatest}
                />
              ) : (isFootprint ? footprint : klines).pending &&
                !(isFootprint ? footprint : klines).stale ? (
                <EmptyBlock text="读取中" />
              ) : (isFootprint ? currentFootprint : currentKlines) === null ? (
                <EmptyBlock text={EMPTY_TEXT} />
              ) : (
                <EmptyBlock text={isFootprint ? "无逐笔数据" : "无 K 线数据"} />
              )}
            </Panel>

            <Panel
              title=""
              area="ob"
              meta={orderbook}
              facts={[
                obSpreadTicks === EMPTY_TEXT
                  ? "spread —"
                  : `spread ${obSpreadTicks}t`,
              ]}
              source={currentOrderbook?.source ?? null}
              toolbar={obToolbar}
              flush
              danger={false}
            >
              <div className="l2-quality-stack">
                <L2QualityStrip quality={currentL2Quality} />
                <OrderbookLadder
                  data={currentOrderbook}
                  tickSize={tickSize}
                  sizeStep={sizeStep}
                  bandBp={obBand}
                  basis={display.valueBasis}
                  gapScaled={obGapScaled}
                />
              </div>
            </Panel>

            {monitorDock}
          </>
        )}
      </main>

      <footer className="statusbar" aria-label="全局状态">
        <div className="statusbar__cluster" aria-label="安全与横切件">
          <ModeBadges health={health.data} />
          <Badge
            tone={serviceTone(status)}
            text={rawText(status)}
            hint="GMO 服务状态"
          />
          <ConnectionBadge polls={allPolls} />
          <button
            type="button"
            className="alert-slot"
            title="报警"
            onClick={() => {
              setPage(ORDER_FLOW_PAGE);
              setOflIntent((previous) => ({
                tab: DRAWER_ALERTS,
                seq: previous.seq + 1,
              }));
            }}
          >
            <Badge
              tone={unackedIds.size > 0 ? "warning" : "muted"}
              text={`报警 ${tallyText(unackedIds.size)}`}
              hint="未确认报警计数"
            />
          </button>
        </div>
        <div className="statusbar__cluster statusbar__cluster--right" aria-label="声明与计数">
          <span
            className="status-item status-item--run"
            title={rawText(health.data?.run_id)}
          >
            <span className="micro-label">run</span>
            <span className="value">{rawText(health.data?.run_id)}</span>
          </span>
          <span className="status-item">
            <span className="micro-label">时区</span>
            <span className="value">{JST_LABEL}</span>
          </span>
          {display.valueBasis === BASIS_NOTIONAL ? (
            <span className="status-item status-item--basis">
              <span className="micro-label">基准</span>
              <span className="value">JPY</span>
            </span>
          ) : null}
          <span className="status-item status-item--refresh">
            <span className="micro-label">刷新</span>
            <span className="value">{REFRESH_TEXT}</span>
          </span>
          <span className="status-item status-item--drift">
            <span className="micro-label">偏移</span>
            <span className={driftExceeded(drift) ? "value warn" : "value"}>
              {driftText(drift)}
            </span>
          </span>
        </div>
      </footer>
    </div>
  );
}
