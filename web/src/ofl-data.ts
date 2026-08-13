import { useEffect, useMemo, useState } from "react";
import type {
  OrderflowTilesResponseV2,
  TileColumn,
  TilesMeta,
  TilesResponse,
} from "./api";
import { marketOrderflowTilesV2Path, tilesPath } from "./api";
import {
  reconcileOrderflowColumnsV2,
  replayOrderflowColumnsV2,
} from "./ofl-replay";

export { reconcileOrderflowColumnsV2, replayOrderflowColumnsV2 };

// 每块列数：块界对齐保证复用
export const CHUNK_COLUMNS = 512;
// 客户端块缓存上限（LRU）
const CACHE_MAX_CHUNKS = 96;
// 近缘活块重取周期毫秒
const LIVE_CHUNK_TTL_MS = 15000;
// 非近缘未完结块重取周期毫秒
const STALE_CHUNK_TTL_MS = 60000;
// 并发取块上限
const FETCH_PARALLEL = 4;
const MS_PER_SECOND = 1000;

interface TileChunk {
  readonly key: string;
  readonly fromS: number;
  readonly toS: number;
  readonly columns: readonly TileColumn[];
  readonly meta: TilesMeta;
  readonly immutable: boolean;
  readonly fetchedAt: number;
  readonly version: number;
}

// 模块级缓存：跨组件与页签复用
const chunkCache = new Map<string, TileChunk>();
const inflight = new Set<string>();
// 失败计数供陈旧标记（X-06）
const chunkFailures = new Map<string, number>();
const chunkRetryAt = new Map<string, number>();
const listeners = new Set<() => void>();
let versionCounter = 0;

interface OrderflowChunkV2 {
  readonly key: string;
  readonly fromS: number;
  readonly toS: number;
  readonly columns: readonly TileColumn[];
  readonly meta: TilesMeta;
  readonly headGeneration: string;
  readonly etag: string;
  readonly fetchedAt: number;
  readonly version: number;
}

const orderflowChunkCache = new Map<string, OrderflowChunkV2>();
const orderflowInflight = new Set<string>();
const orderflowFailures = new Map<string, number>();
const orderflowRetryAt = new Map<string, number>();
const orderflowListeners = new Set<() => void>();

function chunkKey(symbol: string, bucket: string, startS: number): string {
  return `${symbol}|${bucket}|${String(startS)}`;
}

function orderflowChunkKey(
  marketId: string,
  bucket: string,
  startS: number,
): string {
  return `${marketId}|${bucket}|${String(startS)}`;
}

function notifyAll(): void {
  for (const listener of listeners) {
    listener();
  }
}

function notifyOrderflow(): void {
  for (const listener of orderflowListeners) {
    listener();
  }
}

function touchOrderflowChunk(key: string): OrderflowChunkV2 | undefined {
  const held = orderflowChunkCache.get(key);
  if (held !== undefined) {
    orderflowChunkCache.delete(key);
    orderflowChunkCache.set(key, held);
  }
  return held;
}

function evictOrderflowOver(needed: ReadonlySet<string>): void {
  if (orderflowChunkCache.size <= CACHE_MAX_CHUNKS) {
    return;
  }
  for (const key of orderflowChunkCache.keys()) {
    if (orderflowChunkCache.size <= CACHE_MAX_CHUNKS) {
      return;
    }
    if (!needed.has(key) && !orderflowInflight.has(key)) {
      orderflowChunkCache.delete(key);
    }
  }
}

/** 取用即移至末位，Map 序即近用序。 */
function touchChunk(key: string): TileChunk | undefined {
  const held = chunkCache.get(key);
  if (held !== undefined) {
    chunkCache.delete(key);
    chunkCache.set(key, held);
  }
  return held;
}

/** 逐出最久未用块，在用键跳过。 */
function evictOver(needed: ReadonlySet<string>): void {
  if (chunkCache.size <= CACHE_MAX_CHUNKS) {
    return;
  }
  for (const key of chunkCache.keys()) {
    if (chunkCache.size <= CACHE_MAX_CHUNKS) {
      return;
    }
    if (!needed.has(key)) {
      chunkCache.delete(key);
    }
  }
}

/** 块是否到期须重取：不可变永不过期。 */
function chunkExpired(chunk: TileChunk, chunkSpanS: number): boolean {
  if (chunk.immutable) {
    return false;
  }
  const nowS = Date.now() / MS_PER_SECOND;
  const nearEdge = chunk.toS >= nowS - chunkSpanS;
  const ttl = nearEdge ? LIVE_CHUNK_TTL_MS : STALE_CHUNK_TTL_MS;
  return Date.now() - chunk.fetchedAt > ttl;
}

async function fetchChunk(
  symbol: string,
  bucket: string,
  bucketS: number,
  startS: number,
): Promise<void> {
  const key = chunkKey(symbol, bucket, startS);
  if (inflight.has(key)) {
    return;
  }
  inflight.add(key);
  try {
    const path = tilesPath(
      symbol,
      bucket,
      startS,
      startS + bucketS * CHUNK_COLUMNS,
    );
    if (path === null) {
      return;
    }
    // 不加 no-store，完结块走缓存
    const response = await fetch(path, {
      headers: { Accept: "application/json" },
    });
    if (!response.ok) {
      const failures = (chunkFailures.get(key) ?? 0) + 1;
      chunkFailures.set(key, failures);
      chunkRetryAt.set(
        key,
        Date.now() + Math.min(LIVE_CHUNK_TTL_MS, 2 ** failures * MS_PER_SECOND),
      );
      return;
    }
    const frozen = (response.headers.get("cache-control") ?? "").includes(
      "immutable",
    );
    const body = (await response.json()) as TilesResponse;
    chunkFailures.delete(key);
    chunkRetryAt.delete(key);
    versionCounter += 1;
    chunkCache.set(key, {
      key,
      fromS: startS,
      toS: startS + bucketS * CHUNK_COLUMNS,
      columns: body.columns,
      meta: body.meta,
      immutable: frozen,
      fetchedAt: Date.now(),
      version: versionCounter,
    });
  } catch {
    // 网络失败保留旧块，下轮再试
    const failures = (chunkFailures.get(key) ?? 0) + 1;
    chunkFailures.set(key, failures);
    chunkRetryAt.set(
      key,
      Date.now() + Math.min(LIVE_CHUNK_TTL_MS, 2 ** failures * MS_PER_SECOND),
    );
  } finally {
    inflight.delete(key);
    // 释放槽位后唤醒新视窗泵
    notifyAll();
  }
}

function orderflowExpired(chunk: OrderflowChunkV2, chunkSpanS: number): boolean {
  const nearEdge = chunk.toS >= Date.now() / MS_PER_SECOND - chunkSpanS;
  const ttl = nearEdge ? LIVE_CHUNK_TTL_MS : STALE_CHUNK_TTL_MS;
  return Date.now() - chunk.fetchedAt > ttl;
}

async function fetchOrderflowChunk(
  marketId: string,
  bucket: string,
  bucketS: number,
  startS: number,
): Promise<void> {
  const key = orderflowChunkKey(marketId, bucket, startS);
  if (orderflowInflight.has(key)) {
    return;
  }
  const path = marketOrderflowTilesV2Path(
    marketId,
    bucket,
    startS,
    startS + bucketS * CHUNK_COLUMNS,
    CHUNK_COLUMNS,
  );
  if (path === null) {
    return;
  }
  orderflowInflight.add(key);
  try {
    const held = touchOrderflowChunk(key);
    const headers: Record<string, string> = { Accept: "application/json" };
    if (held?.etag !== undefined && held.etag !== "") {
      headers["If-None-Match"] = held.etag;
    }
    const response = await fetch(path, { headers });
    if (response.status === 304 && held !== undefined) {
      orderflowChunkCache.set(key, { ...held, fetchedAt: Date.now() });
      orderflowFailures.delete(key);
      orderflowRetryAt.delete(key);
      return;
    }
    if (!response.ok) {
      throw new Error(`orderflow tile ${String(response.status)}`);
    }
    const body = (await response.json()) as OrderflowTilesResponseV2;
    const columns = replayOrderflowColumnsV2(body.columns);
    const rowBin =
      body.columns.find((column) => column.row_size !== null)?.row_size ??
      body.market.tick_size;
    const meta: TilesMeta = {
      venue: body.market.venue_id,
      symbol: body.market.venue_symbol,
      bucket,
      bucket_seconds: bucketS,
      row_bin: rowBin,
      tick_size: body.market.tick_size,
      from_ts: body.meta.requested_from,
      to_ts: body.meta.requested_to,
      columns: columns.length,
      truncated: body.meta.truncated,
      missing_dates: [],
      side_basis: body.meta.attribution,
      coverage_clipped: body.meta.truncated,
      coverage_from: body.meta.coverage_from,
      coverage_to: body.meta.coverage_to,
    };
    versionCounter += 1;
    orderflowChunkCache.set(key, {
      key,
      fromS: startS,
      toS: startS + bucketS * CHUNK_COLUMNS,
      columns,
      meta,
      headGeneration: body.meta.head_generation,
      etag: response.headers.get("etag") ?? body.meta.etag,
      fetchedAt: Date.now(),
      version: versionCounter,
    });
    orderflowFailures.delete(key);
    orderflowRetryAt.delete(key);
  } catch {
    const failures = (orderflowFailures.get(key) ?? 0) + 1;
    orderflowFailures.set(key, failures);
    orderflowRetryAt.set(
      key,
      Date.now() + Math.min(LIVE_CHUNK_TTL_MS, 2 ** failures * MS_PER_SECOND),
    );
  } finally {
    orderflowInflight.delete(key);
    notifyOrderflow();
  }
}

/** 窗口覆盖的块起点序列。 */
function chunkStarts(
  fromS: number,
  toS: number,
  bucketS: number,
): number[] {
  const span = bucketS * CHUNK_COLUMNS;
  const first = Math.floor(fromS / span) * span;
  const out: number[] = [];
  for (let at = first; at < toS; at += span) {
    out.push(at);
  }
  return out;
}

export interface TileWindow {
  readonly columns: readonly TileColumn[];
  readonly meta: TilesMeta | null;
  readonly missingDates: readonly string[];
  readonly pending: boolean;
  readonly stale: boolean;
  readonly error: string | null;
  readonly version: number;
  readonly lastLandedAt: string | null;
}

const EMPTY_WINDOW: TileWindow = {
  columns: [],
  meta: null,
  missingDates: [],
  pending: false,
  stale: false,
  error: null,
  version: 0,
  lastLandedAt: null,
};

/**
 * 瓦片窗口装配：按块取缺、旧块保持可见（加载不清空）。
 * 视窗变化只取缺失块；近缘活块按周期重取；LRU 逐出。
 */
export function useTileWindow(
  symbol: string | null,
  bucket: string,
  bucketS: number,
  fromS: number,
  toS: number,
): TileWindow {
  const [tick, setTick] = useState<number>(0);

  useEffect(() => {
    const bump = (): void => {
      setTick((previous) => previous + 1);
    };
    listeners.add(bump);
    return () => {
      listeners.delete(bump);
    };
  }, []);

  // 取缺：缺块与到期块入队，限并发
  useEffect(() => {
    if (symbol === null) {
      return;
    }
    let disposed = false;
    const pump = (): void => {
      if (disposed) {
        return;
      }
      let slots = FETCH_PARALLEL - inflight.size;
      for (const startS of chunkStarts(fromS, toS, bucketS)) {
        if (slots <= 0) {
          break;
        }
        const key = chunkKey(symbol, bucket, startS);
        const held = touchChunk(key);
        if (
          (held === undefined || chunkExpired(held, bucketS * CHUNK_COLUMNS))
          && !inflight.has(key)
          && Date.now() >= (chunkRetryAt.get(key) ?? 0)
        ) {
          slots -= 1;
          void fetchChunk(symbol, bucket, bucketS, startS).then(pump);
        }
      }
    };
    pump();
    const timer = window.setInterval(pump, LIVE_CHUNK_TTL_MS);
    return () => {
      disposed = true;
      window.clearInterval(timer);
    };
  }, [symbol, bucket, bucketS, fromS, toS, tick]);

  return useMemo<TileWindow>(() => {
    if (symbol === null) {
      return EMPTY_WINDOW;
    }
    const needed = chunkStarts(fromS, toS, bucketS);
    const neededKeys = new Set(
      needed.map((startS) => chunkKey(symbol, bucket, startS)),
    );
    evictOver(neededKeys);
    const columns: TileColumn[] = [];
    const missing = new Set<string>();
    let meta: TilesMeta | null = null;
    let pending = false;
    let failedChunks = 0;
    let signature = 0;
    let latestFetchedAt = 0;
    for (const startS of needed) {
      const key = chunkKey(symbol, bucket, startS);
      const held = chunkCache.get(key);
      if ((chunkFailures.get(key) ?? 0) > 0) {
        failedChunks += 1;
      }
      if (held === undefined) {
        pending = true;
        continue;
      }
      if (chunkExpired(held, bucketS * CHUNK_COLUMNS)) {
        pending = true;
      }
      latestFetchedAt = Math.max(latestFetchedAt, held.fetchedAt);
      signature += held.version;
      if (meta === null && held.meta.row_bin !== null) {
        meta = held.meta;
      }
      for (const date of held.meta.missing_dates) {
        missing.add(date);
      }
      for (const column of held.columns) {
        if (column.e >= fromS && column.e < toS) {
          columns.push(column);
        }
      }
    }
    return {
      columns,
      meta,
      missingDates: [...missing].sort(),
      pending,
      stale: failedChunks > 0 && columns.length > 0,
      error: failedChunks === 0 ? null : `${String(failedChunks)} 块瓦片读取失败`,
      version: signature,
      lastLandedAt:
        latestFetchedAt === 0 ? null : new Date(latestFetchedAt).toISOString(),
    };
    // 装配随块落地推进，tick 驱动
  }, [symbol, bucket, bucketS, fromS, toS, tick]);
}

/**
 * market-scoped OFL v2 窗口。块内附带锚点只参与重放；缓存记录活动头
 * generation，并以 ETag 条件请求安全替换同一市场窗口的旧 generation。
 */
export function useOrderflowTileWindowV2(
  marketId: string | null,
  bucket: string,
  bucketS: number,
  fromS: number,
  toS: number,
): TileWindow {
  const [tick, setTick] = useState<number>(0);

  useEffect(() => {
    const bump = (): void => {
      setTick((previous) => previous + 1);
    };
    orderflowListeners.add(bump);
    return () => {
      orderflowListeners.delete(bump);
    };
  }, []);

  useEffect(() => {
    if (marketId === null) {
      return;
    }
    let disposed = false;
    const pump = (): void => {
      if (disposed) {
        return;
      }
      let slots = FETCH_PARALLEL - orderflowInflight.size;
      for (const startS of chunkStarts(fromS, toS, bucketS)) {
        if (slots <= 0) {
          break;
        }
        const key = orderflowChunkKey(marketId, bucket, startS);
        const held = touchOrderflowChunk(key);
        if (
          (held === undefined ||
            orderflowExpired(held, bucketS * CHUNK_COLUMNS)) &&
          !orderflowInflight.has(key) &&
          Date.now() >= (orderflowRetryAt.get(key) ?? 0)
        ) {
          slots -= 1;
          void fetchOrderflowChunk(marketId, bucket, bucketS, startS).then(pump);
        }
      }
    };
    pump();
    const timer = window.setInterval(pump, LIVE_CHUNK_TTL_MS);
    return () => {
      disposed = true;
      window.clearInterval(timer);
    };
  }, [marketId, bucket, bucketS, fromS, toS, tick]);

  return useMemo<TileWindow>(() => {
    if (marketId === null) {
      return EMPTY_WINDOW;
    }
    const starts = chunkStarts(fromS, toS, bucketS);
    const needed = new Set(
      starts.map((startS) => orderflowChunkKey(marketId, bucket, startS)),
    );
    evictOrderflowOver(needed);
    const columns: TileColumn[] = [];
    let meta: TilesMeta | null = null;
    let pending = false;
    let failedChunks = 0;
    let signature = 0;
    let latestFetchedAt = 0;
    for (const startS of starts) {
      const key = orderflowChunkKey(marketId, bucket, startS);
      const held = touchOrderflowChunk(key);
      if ((orderflowFailures.get(key) ?? 0) > 0) {
        failedChunks += 1;
      }
      if (held === undefined) {
        pending = true;
        continue;
      }
      if (orderflowExpired(held, bucketS * CHUNK_COLUMNS)) {
        pending = true;
      }
      meta ??= held.meta;
      signature += held.version;
      latestFetchedAt = Math.max(latestFetchedAt, held.fetchedAt);
      for (const column of held.columns) {
        if (column.e >= fromS && column.e < toS) {
          columns.push(column);
        }
      }
    }
    const assembled = reconcileOrderflowColumnsV2(columns);
    const observedMeta =
      meta === null
        ? null
        : {
            ...meta,
            columns: assembled.columns.length,
            method_versions: [...assembled.methodVersions],
            row_sizes: [...assembled.rowSizes],
            contract_breaks: assembled.contractBreaks,
          };
    return {
      columns: assembled.columns,
      meta: observedMeta,
      missingDates: [],
      pending,
      stale: failedChunks > 0 && columns.length > 0,
      error:
        failedChunks === 0
          ? null
          : `${String(failedChunks)} 块 market OFL 瓦片读取失败`,
      version: signature,
      lastLandedAt:
        latestFetchedAt === 0 ? null : new Date(latestFetchedAt).toISOString(),
    };
  }, [marketId, bucket, bucketS, fromS, toS, tick]);
}
