import { useEffect, useRef, useState } from "react";
import type { Dispatch, SetStateAction } from "react";
import { shiftDays, shiftYears } from "./format";

// 轮询间隔五秒
export const POLL_INTERVAL_MS = 5000;
// 五秒重验活动物化头。
export const ORDERBOOK_POLL_INTERVAL_MS = 5000;
// K 线范围大，轮询放宽留限速余量
export const KLINE_POLL_INTERVAL_MS = 60000;
// 热力图矩阵重，轮询十五秒
export const HEATMAP_POLL_INTERVAL_MS = 15000;

const API_BASE = "/api";

export interface HealthResponse {
  mode: string;
  frontend_mode: string;
  server_time: string;
  run_id: string;
}

export interface CapabilityItem {
  name: string;
  detail: string;
  phase: string;
}

export interface PendingCapabilityItem {
  name: string;
  phase: string;
  blocker: string;
}

export interface CapabilitiesResponse {
  implemented: CapabilityItem[];
  pending: PendingCapabilityItem[];
  generated_at: string;
}

export interface MarketDatasetCoverage {
  files: number;
  rows: number;
  coverage_from: string | null;
  coverage_to: string | null;
}

export interface MarketDomainCoverage {
  coverage_state: "available" | "empty";
  coverage_from: string | null;
  coverage_to: string | null;
  partition_count: number;
  normalization_versions: string[];
  head_generation: string;
  activated_at: string | null;
  datasets: Record<string, MarketDatasetCoverage>;
}

export interface MarketCatalogItem {
  market_id: string;
  venue_id: string;
  venue_symbol: string;
  instrument_id: string;
  mapping_revision: number;
  market_kind: string;
  base_currency: string;
  quote_currency: string;
  instrument_kind: string;
  tick_size: string | null;
  size_step: string | null;
  min_size: string | null;
  domains: Record<string, MarketDomainCoverage>;
}

export interface VenueSourceCapability {
  domain: string;
  endpoint: string;
  revision_id: number;
  available: boolean;
  access_mode: string;
  backfill_mode: string;
  replay_fidelity: string;
  integrity: string;
  timestamp_unit: string;
  evidence_level: string;
  implementation_status: string;
  surveyed_at: string;
  valid_until: string;
}

export interface VenueCatalogItem {
  venue_id: string;
  capabilities: VenueSourceCapability[];
}

export interface MarketsCatalogResponse {
  schema_version: number;
  generated_at: string;
  markets: MarketCatalogItem[];
  venues: VenueCatalogItem[];
}

export interface ServiceStatusResponse {
  status: string;
  clock_drift_seconds: number;
}

export interface AssetItem {
  symbol: string;
  amount: string;
  available: string;
  /** 旧查询服务尚未重启时没有该字段，前端退化为 GMO 单来源。 */
  venues?: Record<string, AssetVenueAmount>;
}

export interface AssetVenueAmount {
  amount: string;
  available: string;
}

export interface AssetSource {
  id: string;
  label: string;
  status: string;
  error?: string;
}

export interface AssetsResponse {
  items: AssetItem[];
  /** 旧查询服务尚未重启时没有该字段，前端退化为 GMO 单来源。 */
  sources?: AssetSource[];
  as_of: string;
}

export interface SymbolRule {
  symbol: string;
  min_order_size: string;
  size_step: string;
  tick_size: string;
}

export interface SymbolsResponse {
  whitelist: string[];
  rules: SymbolRule[];
}

export interface TickerResponse {
  symbol: string;
  last: string;
  ask: string;
  bid: string;
  high: string;
  low: string;
  volume: string;
  timestamp: string;
}

export interface KlineItem {
  open_time: string;
  open: string;
  high: string;
  low: string;
  close: string;
  volume: string;
}

export interface KlinesMeta {
  interval: string;
  from: string;
  to: string;
  today: string;
  requests: number;
  truncated: boolean;
  source: string;
}

export interface KlinesResponse {
  market?: MarketCatalogItem;
  items: KlineItem[];
  meta: KlinesMeta;
}

export interface ActiveOrderItem {
  order_id: number;
  side: string;
  execution_type: string;
  price: string;
  size: string;
  executed_size: string;
  status: string;
  time_in_force: string;
  timestamp: string;
}

export interface ActiveOrdersResponse {
  items: ActiveOrderItem[];
  as_of: string;
}

export interface ExecutionItem {
  execution_id: number;
  order_id: number;
  side: string;
  price: string;
  size: string;
  fee: string;
  timestamp: string;
}

export interface ExecutionsResponse {
  items: ExecutionItem[];
  as_of: string;
}

export interface RecentTradeItem {
  e: number;
  t: string;
  price: string;
  size: string;
  side: string;
}

export interface RecentTradesResponse {
  items: RecentTradeItem[];
  meta: {
    symbol: string;
    seconds: number;
    side_basis: string;
    as_of: string;
  };
}

export interface OrderbookLevel {
  price: string;
  size: string;
  notional: string;
}

export interface OrderbookBand {
  band_bp: string;
  ask_complete: boolean;
  bid_complete: boolean;
  complete: boolean;
  ask_size: string | null;
  bid_size: string | null;
  ask_notional: string | null;
  bid_notional: string | null;
  imbalance_size: string | null;
  imbalance_notional: string | null;
}

export interface OrderbookResponse {
  market?: MarketCatalogItem;
  symbol: string;
  source: string;
  asks: OrderbookLevel[];
  bids: OrderbookLevel[];
  best_ask: string;
  best_bid: string;
  spread: string;
  mid: string;
  microprice: string;
  coverage: {
    ask_bp: string;
    bid_bp: string;
  };
  source_coverage: {
    ask_bp: string;
    bid_bp: string;
  };
  bands: OrderbookBand[];
  ask_total: string;
  bid_total: string;
  as_of: string;
}

export type L2QualityStatus = "ok" | "degraded" | "failed" | "unknown";

export interface L2QualityResponse {
  schema_version: number;
  market_id: string;
  quality_version: string;
  status: L2QualityStatus;
  reasons: string[];
  window_start: string | null;
  window_end: string | null;
  window_clock_basis: string | null;
  frames: number | null;
  snapshot_frames: number | null;
  delta_frames: number | null;
  checksum_status: "passed" | "failed" | "unsupported" | "unknown";
  checksum_observed_frames: number | null;
  checksum_checked_frames: number | null;
  checksum_failures: number | null;
  unanchored_before_snapshot_frames: number | null;
  anchor_unknown_frames: number | null;
  sequence_duplicates: number | null;
  sequence_regressions: number | null;
  predecessor_unknown_frames: number | null;
  latency_status: "measurable" | "clock_skewed" | "unmeasurable" | "unknown";
  recv_source_offset_samples: number | null;
  recv_source_offset_p50_ms: number | null;
  recv_source_offset_p95_ms: number | null;
  latest_materialized_observation_time: string | null;
  materialized_freshness_seconds: number | null;
  materialized_freshness_status:
    | "fresh"
    | "stale"
    | "clock_skewed"
    | "unknown"
    | "not_applicable";
  freshness_basis: "latest_materialized_observation_time";
  freshness_threshold_seconds: number;
  freshness_scope: "materialized_only";
  wire_freshness_included: false;
  checkpoint_freshness_included: false;
  computed_at: string | null;
}

export interface FootprintLevelItem {
  price_bin: string;
  sell: string;
  buy: string;
  sell_notional: string;
  buy_notional: string;
}

export interface FootprintBarItem {
  open_time: string;
  open: string;
  high: string;
  low: string;
  close: string;
  delta: string;
  total: string;
  delta_notional: string;
  total_notional: string;
  unknown_side_count?: number;
  unknown_side_size?: string;
  unknown_side_notional?: string;
  poc: string | null;
  vah: string | null;
  val: string | null;
  source: string;
  levels: FootprintLevelItem[];
}

export interface FootprintMeta {
  symbol: string;
  interval: string;
  from: string;
  to: string;
  today: string;
  bin: string | null;
  tier: number | null;
  auto: boolean;
  truncated: boolean;
  coverage_clipped?: boolean;
  side_basis?: string;
  unknown_side_count?: number;
  coverage_from: string | null;
  coverage_to: string | null;
}

export interface FootprintResponse {
  market?: MarketCatalogItem;
  bars: FootprintBarItem[];
  meta: FootprintMeta;
}

export interface HeatmapColumn {
  t: string;
  gap: boolean;
}

export interface HeatmapResponse {
  rows: number;
  cols: HeatmapColumn[];
  price_low: string | null;
  price_high: string | null;
  ask: number[][];
  bid: number[][];
  mid_row: (number | null)[];
}

// 瓦片格值六元组，含三值分解
export type TileCell = readonly string[];

export interface TileDepthEntry extends ReadonlyArray<string | boolean> {
  readonly 0: string;
  readonly 1: string;
  readonly 2: boolean;
  // 金额基准（瓦片 schema 2 起）
  readonly 3?: string;
}

export interface TileBands {
  spread_bp: string | null;
  ofi: string;
  imbalance: string | null;
  trade_delta: string;
  // 金额基准（瓦片 schema 2 起）
  trade_delta_notional?: string;
  depth: TileDepthEntry[];
}

export interface TileColumn {
  t: string;
  e: number;
  gap: boolean;
  carried: boolean;
  reset: boolean;
  /** market OFL v2 only: materializer contract carried with every column. */
  method_version?: string;
  /** market OFL v2 only: price-row contract carried with every column. */
  row_size?: string | null;
  /** Synthetic or source boundary at which prior column state must not flow. */
  contract_break?: boolean;
  frames: number;
  mid: string | null;
  cells: TileCell[];
  bands: TileBands;
}

export interface TilesMeta {
  venue: string;
  symbol: string;
  bucket: string;
  bucket_seconds: number | null;
  row_bin: string | null;
  tick_size: string | null;
  from_ts: string;
  to_ts: string;
  columns: number;
  truncated: boolean;
  missing_dates: string[];
  side_basis: string;
  // 视界截断旗标，档带追踪附带
  coverage_clipped?: boolean;
  coverage_from?: string | null;
  coverage_to?: string | null;
  /** Active materializer methods represented by the assembled client window. */
  method_versions?: string[];
  /** Active row sizes represented by the assembled client window. */
  row_sizes?: string[];
  /** Column-level replay boundaries inserted for incompatible contracts. */
  contract_breaks?: number;
}

export interface TilesResponse {
  columns: TileColumn[];
  meta: TilesMeta;
}

/** market-scoped 稀疏 OFL cell；L2 减量与逐笔成交禁止合并命名。 */
export interface OrderflowTileCellV2 {
  book_side: "ask" | "bid";
  price_key: number;
  price: string;
  row_size: string | null;
  price_quantum_basis: "instrument_map_tick_size" | "observed_decimal_quantum" | string;
  book_end_size: string | null;
  net_increase: string;
  net_decrease_unknown: string;
  taker_buy_size: string;
  taker_sell_size: string;
  state_role: "anchor" | "change" | "reset" | "trade" | string;
}

export interface OrderflowTileColumnV2 {
  column_id: string;
  bucket_epoch: number;
  bucket_start: string;
  bucket_end: string;
  coverage_state: "ok" | "carried" | "reset" | "gap" | string;
  is_anchor: boolean;
  is_reset: boolean;
  is_carried: boolean;
  is_gap: boolean;
  context_only: boolean;
  frame_count: number;
  trade_count: number;
  last_event_time: string | null;
  last_available_time: string | null;
  integrity_mode: string;
  source_generation: string;
  method_version: string;
  row_size: string | null;
  price_quantum_basis: string;
  cells: OrderflowTileCellV2[];
}

export interface OrderflowTilesMetaV2 {
  market_id: string;
  domain: "orderflow_tile";
  head_generation: string;
  etag: string;
  bucket: string;
  requested_from: string;
  requested_to: string;
  coverage_from: string | null;
  coverage_to: string | null;
  truncated: boolean;
  attribution: "l2_change_and_trade_kept_separate";
  sparse_contract: "periodic_anchor_plus_changes";
  anchor_context_columns: number;
  context_from: string | null;
}

export interface OrderflowTilesResponseV2 {
  schema_version: number;
  market: MarketCatalogItem;
  columns: OrderflowTileColumnV2[];
  meta: OrderflowTilesMetaV2;
}

export interface TrackHistoryPoint {
  t: string;
  qty: string | null;
}

export interface TrackSegment {
  first_seen: string;
  last_seen: string;
  vanished_at: string | null;
  buckets: number;
}

export interface TrackReaction {
  event_t: string;
  before_mid: string;
  after_mid: string;
  change_bp: string;
  buckets: number;
}

export interface LevelTrack {
  price_bin: string;
  history: TrackHistoryPoint[];
  segments: TrackSegment[];
  net_add_total: string;
  net_cancel_total: string;
  executed_total: string;
  cancel_ratio: string | null;
  replenishment: { count: number; size: string; lookahead_buckets: number };
  price_reaction: TrackReaction | null;
}

export interface LevelTrackResponse {
  track: LevelTrack;
  meta: TilesMeta;
}

export interface RegionJudgment {
  kind: string;
  label: string;
  met: boolean;
  confidence: string;
  confidence_version: string;
  criteria: readonly {
    name: string;
    observed: string | boolean;
    comparator: string;
    threshold: string | boolean;
    met: boolean;
    score: string;
  }[];
  metrics: Record<string, unknown>;
  metric_labels: Record<string, string>;
  feature_id?: number;
  alert_id?: number;
}

export interface RuleEvaluation {
  rule_id: string;
  kind: string;
  band_source: string;
  band_bp?: string;
  evaluable?: boolean;
  price_low?: string;
  price_high?: string;
  met?: boolean;
  confidence?: string;
  matched?: boolean;
  feature_id?: number;
  alert_id?: number;
}

export interface RegionMeta {
  symbol: string;
  bucket: string;
  price_lo: string;
  price_hi: string;
  from_ts: string;
  to_ts: string;
  context_samples: number;
  columns: number;
  run_id: string;
  baseline_hash: string;
  source_hash: string;
  code_version: string;
  confidence_version: string;
  basis: string;
  coverage_clipped: boolean;
  coverage_from: string | null;
  coverage_to: string | null;
  rule_evaluations: RuleEvaluation[];
}

export interface RegionResponse {
  judgments: RegionJudgment[];
  meta: RegionMeta;
}

export interface PrintTickItem {
  t: string;
  price: string;
  size: string;
  side: string;
  size_quantile: string;
}

export interface PrintTicksResponse {
  items: PrintTickItem[];
  meta: {
    quantile: string | null;
    missing_dates: string[];
    side_basis: string;
  };
}

export interface BookFeatureItem {
  feature_id: number;
  kind: string;
  label: string;
  symbol: string;
  price_low: string;
  price_high: string;
  from_ts: string;
  to_ts: string;
  metrics: Record<string, unknown>;
  metric_labels: Record<string, string>;
  created_at: string;
}

export interface BookFeaturesResponse {
  items: BookFeatureItem[];
  as_of: string;
}

export interface AlertItem {
  alert_id: number;
  feature_id: number;
  rule_id: string;
  triggered_at: string;
  acked_at: string | null;
  kind: string;
  label: string;
  symbol: string;
  price_low: string;
  price_high: string;
  from_ts: string;
  to_ts: string;
  metrics: Record<string, unknown>;
  metric_labels: Record<string, string>;
}

export interface AlertsResponse {
  items: AlertItem[];
  unacked: number;
  as_of: string;
}

export interface OpsProcessItem {
  name: string;
  status: string;
  pid: number | null;
  external: boolean;
  started_at: string | null;
  heartbeat_at: string | null;
  restart_count: number;
  consecutive_failures: number;
  last_exit_code: number | null;
  resume_at: string | null;
  auto_restart: boolean;
}

export interface OpsProcessesResponse {
  items: OpsProcessItem[];
  as_of: string;
}

export interface OpsActionResponse {
  item: OpsProcessItem;
  as_of: string;
}

export const API_PATHS = {
  health: `${API_BASE}/health`,
  capabilities: `${API_BASE}/capabilities`,
  marketsV2: `${API_BASE}/v2/markets`,
  serviceStatus: `${API_BASE}/service-status`,
  assets: `${API_BASE}/assets`,
  symbols: `${API_BASE}/symbols`,
} as const;

/** 拼接带品种参数的路径，品种未定时返回空值。 */
export function symbolPath(endpoint: string, symbol: string | null): string | null {
  if (symbol === null) {
    return null;
  }
  return `${API_BASE}/${endpoint}?symbol=${encodeURIComponent(symbol)}`;
}

// 盘口梯形档深，双侧各取
export const ORDERBOOK_DEPTH = 15;

// 成交强度计取数窗秒
export const RECENT_TRADES_SECONDS = 60;

/** 拼接近窗逐笔路径，品种未定时返回空值。 */
export function recentTradesPath(
  symbol: string | null,
  seconds: number,
): string | null {
  const base = symbolPath("recent-trades", symbol);
  if (base === null) {
    return null;
  }
  return `${base}&seconds=${String(seconds)}`;
}

/** 拼接盘口路径，品种未定时返回空值。 */
export function orderbookPath(
  symbol: string | null,
  depth: number,
): string | null {
  const base = symbolPath("orderbooks", symbol);
  if (base === null) {
    return null;
  }
  return `${base}&depth=${String(depth)}`;
}

// 热力图时间跨度三档，单位分钟
export const HEATMAP_MINUTES: readonly number[] = [5, 15, 60];
export const DEFAULT_HEATMAP_MINUTES = 15;

/** 拼接热力图路径，品种未定时返回空值。 */
export function heatmapPath(
  symbol: string | null,
  minutes: number,
): string | null {
  const base = symbolPath("book-heatmap", symbol);
  if (base === null) {
    return null;
  }
  return `${base}&minutes=${String(minutes)}`;
}

// 周期十二档，逐日拉取六档在前
export const KLINE_INTERVALS: readonly string[] = [
  "1min",
  "5min",
  "10min",
  "15min",
  "30min",
  "1hour",
  "4hour",
  "8hour",
  "12hour",
  "1day",
  "1week",
  "1month",
];

export const DEFAULT_KLINE_INTERVAL = "1hour";

// 交易所最早上市日，全范围起点
const LISTING_DAY = "20180905";
const MINUTE_INTERVALS: readonly string[] = [
  "1min",
  "5min",
  "10min",
  "15min",
  "30min",
];

export interface RangePreset {
  readonly key: string;
  readonly label: string;
  readonly days: number;
  readonly years: number;
  readonly fixedFrom: string;
}

const MINUTE_PRESETS: readonly RangePreset[] = [
  { key: "d1", label: "当日", days: 1, years: 0, fixedFrom: "" },
  { key: "d3", label: "3日", days: 3, years: 0, fixedFrom: "" },
  { key: "d7", label: "7日", days: 7, years: 0, fixedFrom: "" },
];

const HOUR_PRESETS: readonly RangePreset[] = [
  { key: "d7", label: "7日", days: 7, years: 0, fixedFrom: "" },
  { key: "d14", label: "14日", days: 14, years: 0, fixedFrom: "" },
  { key: "d24", label: "24日", days: 24, years: 0, fixedFrom: "" },
];

const YEAR_PRESETS: readonly RangePreset[] = [
  { key: "y1", label: "1年", days: 0, years: 1, fixedFrom: "" },
  { key: "y3", label: "3年", days: 0, years: 3, fixedFrom: "" },
  { key: "all", label: "全部", days: 0, years: 0, fixedFrom: LISTING_DAY },
];

/** 按周期档位给出合法范围预设。 */
export function presetsFor(interval: string): readonly RangePreset[] {
  if (MINUTE_INTERVALS.includes(interval)) {
    return MINUTE_PRESETS;
  }
  if (interval === DEFAULT_KLINE_INTERVAL) {
    return HOUR_PRESETS;
  }
  return YEAR_PRESETS;
}

export interface DayRange {
  readonly from: string;
  readonly to: string;
}

/** 由预设与后端交易日推出起止日。 */
export function resolveRange(preset: RangePreset, today: string): DayRange {
  if (preset.fixedFrom !== "") {
    return { from: preset.fixedFrom, to: today };
  }
  if (preset.years > 0) {
    return { from: shiftYears(today, preset.years), to: today };
  }
  return { from: shiftDays(today, Math.max(0, preset.days - 1)), to: today };
}

/** 拼接 K 线路径，交易日未知时不带范围参数。 */
export function klinesPath(
  symbol: string | null,
  interval: string,
  range: DayRange | null,
): string | null {
  const base = symbolPath("klines", symbol);
  if (base === null) {
    return null;
  }
  const withInterval = `${base}&interval=${encodeURIComponent(interval)}`;
  if (range === null) {
    return withInterval;
  }
  return `${withInterval}&from=${range.from}&to=${range.to}`;
}

function compactDayStart(day: string): string {
  const year = Number(day.slice(0, 4));
  const month = Number(day.slice(4, 6)) - 1;
  const date = Number(day.slice(6, 8));
  return new Date(Date.UTC(year, month, date)).toISOString();
}

function compactDayEnd(day: string): string {
  const year = Number(day.slice(0, 4));
  const month = Number(day.slice(4, 6)) - 1;
  const date = Number(day.slice(6, 8));
  return new Date(Date.UTC(year, month, date + 1)).toISOString();
}

/** 活动物化 K 线；半开 UTC 窗口由旧的日范围视图参数确定。 */
export function marketKlinesV2Path(
  marketId: string | null,
  interval: string,
  range: DayRange | null,
): string | null {
  if (marketId === null) {
    return null;
  }
  const base = `${API_BASE}/v2/markets/${encodeURIComponent(marketId)}/klines`;
  const query = `interval=${encodeURIComponent(interval)}`;
  if (range === null) {
    return `${base}?${query}`;
  }
  return (
    `${base}?${query}&from=${encodeURIComponent(compactDayStart(range.from))}` +
    `&to=${encodeURIComponent(compactDayEnd(range.to))}`
  );
}

// 足迹轮询：当期 bar 增量刷新
export const FOOTPRINT_POLL_INTERVAL_MS = 15000;
// 足迹支持的周期档
export const FOOTPRINT_INTERVALS: readonly string[] = [
  "5min",
  "15min",
  "30min",
  "1hour",
  "4hour",
  "1day",
];
export const DEFAULT_FOOTPRINT_INTERVAL = "15min";
// 档位 chip：自动加五档
export const FOOTPRINT_BINS: readonly string[] = [
  "auto",
  "500",
  "1000",
  "2000",
  "5000",
  "10000",
];
export const DEFAULT_FOOTPRINT_BIN = "auto";

/** 拼接足迹路径，品种或品种规则未定时返回空值。 */
export function footprintPath(
  symbol: string | null,
  interval: string,
  range: DayRange | null,
  bin: string,
  tick: string,
): string | null {
  const base = symbolPath("footprint", symbol);
  if (base === null || tick === "") {
    return null;
  }
  const withArgs = `${base}&interval=${encodeURIComponent(interval)}&bin=${encodeURIComponent(bin)}&tick=${encodeURIComponent(tick)}`;
  if (range === null) {
    return withArgs;
  }
  return `${withArgs}&from=${range.from}&to=${range.to}`;
}

/** 活动成交事实派生 Footprint；auto 由后端按市场 tick 决定。 */
export function marketFootprintV2Path(
  marketId: string | null,
  interval: string,
  range: DayRange | null,
  bin: string,
): string | null {
  if (marketId === null) {
    return null;
  }
  const base = `${API_BASE}/v2/markets/${encodeURIComponent(marketId)}/footprint`;
  let query = `interval=${encodeURIComponent(interval)}&price_bin=${encodeURIComponent(bin)}`;
  if (range !== null) {
    query += `&from=${encodeURIComponent(compactDayStart(range.from))}`;
    query += `&to=${encodeURIComponent(compactDayEnd(range.to))}`;
  }
  return `${base}?${query}`;
}

/** 最近活动 L2 快照；服务端按 head_generation 缓存确定性重放结果。 */
export function marketL2LatestV2Path(
  marketId: string | null,
  depth: number,
): string | null {
  if (marketId === null) {
    return null;
  }
  return (
    `${API_BASE}/v2/markets/${encodeURIComponent(marketId)}/book/l2/latest` +
    `?depth=${String(depth)}`
  );
}

/** 最新 L2 物化质量窗；不表示 wire 或 checkpoint 新鲜度。 */
export function marketL2QualityV2Path(marketId: string | null): string | null {
  if (marketId === null) {
    return null;
  }
  return `${API_BASE}/v2/markets/${encodeURIComponent(marketId)}/book/l2/quality`;
}

// 事件与成交刻线轮询十五秒
export const FEATURES_POLL_INTERVAL_MS = 15000;
export const PRINT_TICKS_POLL_INTERVAL_MS = 15000;
// 报警与进程状态轮询五秒
export const ALERTS_POLL_INTERVAL_MS = 5000;
export const OPS_POLL_INTERVAL_MS = 5000;

// 桶档 chip 序列，亚秒档无来源支撑
export const TILE_BUCKETS: readonly string[] = ["100ms", "1s", "5s", "1min"];
export const TILE_BUCKET_SECONDS: Readonly<Record<string, number>> = {
  "1s": 1,
  "5s": 5,
  "1min": 60,
};
// 生产仅持续发布 5s OFL。
export const DEFAULT_TILE_BUCKET = "5s";
export const UNSUPPORTED_TILE_BUCKETS: readonly string[] = [
  "100ms",
  "1s",
  "1min",
];

/** 秒时戳转 ISO 参数文本。 */
function epochParam(seconds: number): string {
  return encodeURIComponent(new Date(seconds * 1000).toISOString());
}

/** 拼接瓦片窗口路径。 */
export function tilesPath(
  symbol: string | null,
  bucket: string,
  fromS: number,
  toS: number,
): string | null {
  const base = symbolPath("heatmap-tiles", symbol);
  if (base === null || !(bucket in TILE_BUCKET_SECONDS) || fromS >= toS) {
    return null;
  }
  return `${base}&bucket=${bucket}&from_ts=${epochParam(fromS)}&to_ts=${epochParam(toS)}`;
}

/** market-scoped 稀疏 OFL tile；窗口前 anchor 由服务端自动附带。 */
export function marketOrderflowTilesV2Path(
  marketId: string | null,
  bucket: string,
  fromS: number,
  toS: number,
  limit = 4000,
): string | null {
  if (
    marketId === null || !(bucket in TILE_BUCKET_SECONDS) || fromS >= toS
  ) {
    return null;
  }
  return (
    `${API_BASE}/v2/markets/${encodeURIComponent(marketId)}/orderflow/tiles` +
    `?bucket=${encodeURIComponent(bucket)}` +
    `&from=${epochParam(fromS)}&to=${epochParam(toS)}` +
    `&limit=${String(limit)}`
  );
}

/** 拼接档带追踪路径。 */
export function levelTrackPath(
  symbol: string,
  priceBin: string,
  bucket: string,
  fromS: number,
  toS: number,
): string {
  return (
    `${API_BASE}/level-track?symbol=${encodeURIComponent(symbol)}` +
    `&price_bin=${encodeURIComponent(priceBin)}&bucket=${bucket}` +
    `&from_ts=${epochParam(fromS)}&to_ts=${epochParam(toS)}`
  );
}

/** 拼接区域分析路径。 */
export function regionPath(
  symbol: string,
  priceLo: string,
  priceHi: string,
  bucket: string,
  fromS: number,
  toS: number,
): string {
  return (
    `${API_BASE}/region-analysis?symbol=${encodeURIComponent(symbol)}` +
    `&price_lo=${encodeURIComponent(priceLo)}&price_hi=${encodeURIComponent(priceHi)}` +
    `&bucket=${bucket}&from_ts=${epochParam(fromS)}&to_ts=${epochParam(toS)}`
  );
}

/** 拼接成交刻线路径。 */
export function printTicksPath(
  symbol: string | null,
  fromS: number,
  toS: number,
): string | null {
  const base = symbolPath("print-ticks", symbol);
  if (base === null || fromS >= toS) {
    return null;
  }
  return `${base}&from_ts=${epochParam(fromS)}&to_ts=${epochParam(toS)}`;
}

/** 拼接判读事件清单路径。 */
export function bookFeaturesPath(symbol: string | null): string | null {
  return symbolPath("book-features", symbol);
}

export const ALERTS_PATH = `${API_BASE}/alerts`;
export const OPS_PROCESSES_PATH = `${API_BASE}/ops/processes`;

/** 报警确认路径。 */
export function alertAckPath(alertId: number): string {
  return `${API_BASE}/alerts/${String(alertId)}/ack`;
}

/** 采集进程拉起与停止路径。 */
export function opsActionPath(name: string, action: string): string {
  return `${OPS_PROCESSES_PATH}/${encodeURIComponent(name)}/${action}`;
}

export interface PollMeta {
  readonly lastUpdatedAt: string | null;
  readonly stale: boolean;
  readonly error: string | null;
  readonly errorCode: string | null;
  readonly pending: boolean;
}

export interface PollResult<T> extends PollMeta {
  readonly data: T | null;
}

interface Failure {
  readonly code: string;
  readonly message: string;
}

const MESSAGE_LIMIT = 120;
const NETWORK_FAILURE_CODE = "无响应";

function idleState<T>(): PollResult<T> {
  return {
    data: null,
    lastUpdatedAt: null,
    stale: false,
    error: null,
    errorCode: null,
    pending: true,
  };
}

function markStale<T>(
  update: Dispatch<SetStateAction<PollResult<T>>>,
  failure: Failure,
): void {
  update((previous) => ({
    data: previous.data,
    lastUpdatedAt: previous.lastUpdatedAt,
    stale: true,
    error: failure.message,
    errorCode: failure.code,
    pending: previous.data === null,
  }));
}

async function describeFailure(response: Response): Promise<Failure> {
  let text = "";
  try {
    text = await response.text();
  } catch {
    // 响应体不可读时忽略
    text = "";
  }
  const status = `HTTP ${String(response.status)}`;
  if (text === "") {
    return { code: status, message: status };
  }
  let body: unknown = null;
  try {
    body = JSON.parse(text);
  } catch {
    // 非 JSON 响应按原文截断
    return { code: status, message: `${status} ${text.slice(0, MESSAGE_LIMIT)}` };
  }
  const detail = (body as { detail?: unknown }).detail;
  if (typeof detail === "string") {
    return { code: status, message: `${status} ${detail}` };
  }
  if (typeof detail === "object" && detail !== null) {
    const shaped = detail as { code?: unknown; message?: unknown };
    const code = typeof shaped.code === "string" ? shaped.code : status;
    const message = typeof shaped.message === "string" ? shaped.message : "";
    return { code, message: `${status} ${code} ${message}`.trimEnd() };
  }
  return { code: status, message: status };
}

function describeNetworkError(error: unknown): Failure {
  const detail = error instanceof Error ? ` ${error.message}` : "";
  return {
    code: NETWORK_FAILURE_CODE,
    message: `读取失败${detail}`,
  };
}

/** 一次性读取，非 2xx 抛出失败信息。 */
export async function apiGet<T>(path: string): Promise<T> {
  const response = await fetch(path, {
    headers: { Accept: "application/json" },
    cache: "no-store",
  });
  if (!response.ok) {
    const failure = await describeFailure(response);
    throw new Error(failure.message);
  }
  return (await response.json()) as T;
}

/** 一次性写动作：报警确认、采集进程操作与区域分析三类。 */
export async function apiPost<T>(path: string): Promise<T> {
  const response = await fetch(path, {
    method: "POST",
    headers: { Accept: "application/json" },
    cache: "no-store",
  });
  if (!response.ok) {
    const failure = await describeFailure(response);
    throw new Error(failure.message);
  }
  return (await response.json()) as T;
}

/**
 * 通用读取：定时轮询，失败保留旧数据并标注陈旧。
 * path 为空值时不发请求，用于品种尚未确定的场合。
 * intervalMs 为轮询周期，重范围端点可放宽以留限速余量。
 * 旧数据在换参与重取期间保持可见（加载态同错误态纪律）；
 * 载荷等值时保留原数据引用，下游依赖不因轮询而重算。
 */
export function usePolling<T>(
  path: string | null,
  intervalMs: number,
): PollResult<T> {
  const [state, setState] = useState<PollResult<T>>(() => idleState<T>());
  const lastText = useRef<string | null>(null);

  useEffect(() => {
    if (path === null) {
      lastText.current = null;
      setState(idleState<T>());
      return;
    }
    let active = true;
    let loading = false;
    // 换参保旧数据，仅转待取态
    setState((previous) => ({ ...previous, pending: true }));

    const load = async (): Promise<void> => {
      if (loading) {
        return;
      }
      loading = true;
      try {
        const response = await fetch(path, {
          headers: { Accept: "application/json" },
          // v2 重验，旧代理禁缓存。
          cache: path.startsWith(`${API_BASE}/v2/`) ? "no-cache" : "no-store",
        });
        if (!response.ok) {
          const failure = await describeFailure(response);
          if (active) {
            markStale(setState, failure);
          }
          return;
        }
        const text = await response.text();
        if (!active) {
          return;
        }
        const stamp = new Date().toISOString();
        if (text === lastText.current) {
          // 载荷未变：保数据引用只更新鲜度
          setState((previous) => ({
            data: previous.data,
            lastUpdatedAt: stamp,
            stale: false,
            error: null,
            errorCode: null,
            pending: false,
          }));
          return;
        }
        const body = JSON.parse(text) as T;
        lastText.current = text;
        setState({
          data: body,
          lastUpdatedAt: stamp,
          stale: false,
          error: null,
          errorCode: null,
          pending: false,
        });
      } catch (error) {
        if (active) {
          markStale(setState, describeNetworkError(error));
        }
      } finally {
        loading = false;
      }
    };

    void load();
    const timer = window.setInterval(() => {
      void load();
    }, intervalMs);

    return () => {
      active = false;
      window.clearInterval(timer);
    };
  }, [path, intervalMs]);

  return state;
}

/**
 * 内容稳定引用：值序列化相等时返回上一引用，
 * 供依赖比较与 memo 边界隔离无关刷新。
 */
export function useStable<T>(value: T): T {
  const held = useRef<{ text: string; value: T } | null>(null);
  const text = JSON.stringify(value);
  if (held.current === null || held.current.text !== text) {
    held.current = { text, value };
  }
  return held.current.value;
}
