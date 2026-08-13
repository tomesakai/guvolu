"""活动物化 head 驱动的市场级成品查询。

SQLite 只负责冻结活动输出清单；DuckDB 直接查询清单中的不可变 Parquet。
本模块不扫描目录，也不把来源 API、raw 或旧兼容事实混进 v2 响应。
"""
from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable

import duckdb

from guvolu.data.bitbank_book_replay import (
    bitbank_replay_actions,
    wire_session_sql,
)
from guvolu.data.okx_l2_terminal_checkpoint import (
    TERMINAL_CHECKPOINT_DATASET,
    LoadedTerminalCheckpoint,
    TerminalCheckpointError,
    load_latest_terminal_checkpoint,
)
from guvolu.data.store import connect_readonly
from guvolu.ui.query_catalog import (
    ActiveOutputSnapshot,
    QueryCatalog,
    materialization_input_set_hash,
    select_l2_checkpoint_inputs,
)

QUERY_SCHEMA_VERSION = 1
MAX_KLINES = 20_000
MAX_TRADES = 20_000
MAX_FOOTPRINT_DAYS = 31
MAX_L2_REPLAY_FRAMES = 100_000
MAX_TILE_COLUMNS = 4_000
MAX_TILE_ANCHOR_CONTEXT = 128
DEFAULT_FOOTPRINT_TIER = 2_000

FOOTPRINT_INTERVALS: dict[str, int] = {
    "1min": 60,
    "5min": 300,
    "15min": 900,
    "30min": 1800,
    "1hour": 3600,
    "4hour": 14_400,
    "1day": 86_400,
}


class MaterializedQueryError(ValueError):
    """活动成品缺失或不能按请求契约安全解释。"""


def _decimal_text(value: object) -> str:
    if value is None:
        return "0"
    return format(Decimal(str(value)), "f")


def _iso(value: object) -> str:
    if not isinstance(value, datetime):
        raise MaterializedQueryError(f"查询结果时间类型非法: {type(value).__name__}")
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()


def _paths(snapshot: ActiveOutputSnapshot, dataset: str) -> list[str]:
    return [str(row.path) for row in snapshot.outputs if row.dataset == dataset]


def _path_key(value: object) -> str:
    return Path(str(value)).resolve().as_posix().casefold()


def _etag(generation: str, contract: str, args: Iterable[object]) -> str:
    body = json.dumps(
        [QUERY_SCHEMA_VERSION, generation, contract, *map(str, args)],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return '"sha256-' + hashlib.sha256(body.encode("utf-8")).hexdigest() + '"'


def _day_text(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y%m%d")


def _value_area(levels: list[dict[str, Any]]) -> tuple[str | None, str | None, str | None]:
    """按数量覆盖 70% 计算 POC/VA，平量时保持低价优先。"""
    if not levels:
        return None, None, None
    sizes = [
        Decimal(row["buy_volume"]) + Decimal(row["sell_volume"])
        for row in levels
    ]
    total = sum(sizes, Decimal(0))
    if total <= 0:
        return None, None, None
    poc_at = max(range(len(levels)), key=lambda index: sizes[index])
    low = high = poc_at
    covered = sizes[poc_at]
    target = total * Decimal("0.70")
    while covered < target and (low > 0 or high + 1 < len(levels)):
        below = sizes[low - 1] if low > 0 else Decimal("-1")
        above = sizes[high + 1] if high + 1 < len(levels) else Decimal("-1")
        if above > below:
            high += 1; covered += sizes[high]
        else:
            low -= 1; covered += sizes[low]
    return levels[poc_at]["price"], levels[high]["price"], levels[low]["price"]


def _connection() -> Any:
    db: Any = duckdb.connect(":memory:")
    db.execute("SET TimeZone='UTC'")
    db.execute("SET threads=2")
    return db


def _parquet_columns(db: Any, files: list[str]) -> set[str]:
    return {
        str(row[0])
        for row in db.execute(
            "DESCRIBE SELECT * FROM read_parquet(?, union_by_name=true)",
            [files],
        ).fetchall()
    }


def _sequence_integer(value: object) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(str(value))
    except ValueError:
        return None


def _decision_time(value: datetime | None) -> datetime:
    result = datetime.now(UTC) if value is None else value
    if result.tzinfo is None:
        raise MaterializedQueryError("decision_time 必须带时区")
    return result.astimezone(UTC)


class MaterializedQuery:
    """市场级 K线、成交、Footprint 与 L2 只读查询。"""

    def __init__(self, data_root: Path) -> None:
        self.catalog = QueryCatalog(data_root)
        self._l2_cache: dict[tuple[str, str], dict[str, Any]] = {}

    def latest_l2_quality(self, market_id: str) -> dict[str, Any]:
        """读取控制面质量窗口，不以旧 stream_health 补值。"""
        return self.catalog.latest_l2_quality(market_id)

    def klines(
        self,
        market_id: str,
        interval: str,
        from_time: datetime,
        to_time: datetime,
        limit: int = MAX_KLINES,
    ) -> tuple[dict[str, Any], str]:
        snapshot = self.catalog.active_outputs(
            market_id, domains=("kline",), datasets=("market_kline",),
            from_time=from_time, to_time=to_time,
        )
        files = _paths(snapshot, "market_kline")
        tag = _etag(snapshot.head_generation, "klines", (
            market_id, interval, from_time, to_time, limit,
        ))
        if not files:
            return self._empty(
                snapshot, "kline", tag, interval=interval, truncated=False,
                source="materialized_active_head",
                **{"from": _day_text(from_time), "to": _day_text(to_time),
                   "today": _day_text(datetime.now(UTC))},
            ), tag
        db = _connection()
        try:
            rows = db.execute(
                """
                WITH ranked AS (
                  SELECT open_time,close_time,open,high,low,close,volume,
                    available_time,is_closed,origin,revision_ordinal,
                    row_number() OVER (
                      PARTITION BY market_id,interval,open_time,origin
                      ORDER BY is_closed DESC,revision_ordinal DESC,
                               available_time DESC,value_revision DESC
                    ) AS selected
                  FROM read_parquet(?, union_by_name=true)
                  WHERE market_id=? AND interval=? AND open_time>=? AND open_time<?
                    AND available_time<=?
                )
                SELECT open_time,close_time,open,high,low,close,volume,
                       available_time,is_closed,origin,revision_ordinal
                FROM ranked WHERE selected=1 ORDER BY open_time LIMIT ?
                """,
                [files, market_id, interval, from_time, to_time,
                 datetime.now(UTC), limit + 1],
            ).fetchall()
        finally:
            db.close()
        truncated = len(rows) > limit
        rows = rows[:limit]
        items = [{
            "open_time": _iso(row[0]), "close_time": _iso(row[1]),
            "open": str(row[2]), "high": str(row[3]), "low": str(row[4]),
            "close": str(row[5]), "volume": str(row[6]),
            "available_time": _iso(row[7]), "is_closed": bool(row[8]),
            "origin": str(row[9]), "revision_ordinal": int(row[10]),
        } for row in rows]
        return {
            "schema_version": QUERY_SCHEMA_VERSION,
            "market": snapshot.market,
            "items": items,
            "meta": self._meta(snapshot, "kline", tag, from_time, to_time,
                               interval=interval, truncated=truncated,
                               source="materialized_active_head",
                               **{"from": _day_text(from_time),
                                  "to": _day_text(to_time),
                                  "today": _day_text(datetime.now(UTC))}),
        }, tag

    def trades(
        self,
        market_id: str,
        from_time: datetime,
        to_time: datetime,
        limit: int = 5_000,
    ) -> tuple[dict[str, Any], str]:
        snapshot = self._trade_snapshot(market_id, from_time, to_time)
        files = _paths(snapshot, "trade_observation")
        tag = _etag(snapshot.head_generation, "trades", (
            market_id, from_time, to_time, limit,
        ))
        if not files:
            return self._empty(snapshot, "trade", tag), tag
        db = _connection()
        try:
            rows = db.execute(
                """
                WITH selected AS (
                  SELECT *,row_number() OVER (
                    PARTITION BY observation_id
                    ORDER BY available_time,ingest_time,normalization_version
                  ) AS duplicate_ordinal
                  FROM read_parquet(?, union_by_name=true)
                  WHERE market_id=? AND event_time>=? AND event_time<?
                    AND available_time<=?
                )
                SELECT observation_id,venue_trade_id,event_time,available_time,
                       side,price,size,match_granularity,source_side_basis
                FROM selected WHERE duplicate_ordinal=1
                ORDER BY event_time,observation_id LIMIT ?
                """,
                [files, market_id, from_time, to_time, datetime.now(UTC), limit + 1],
            ).fetchall()
        finally:
            db.close()
        truncated = len(rows) > limit
        rows = rows[:limit]
        items = [{
            "observation_id": str(row[0]), "venue_trade_id": str(row[1]),
            "event_time": _iso(row[2]), "available_time": _iso(row[3]),
            "side": str(row[4]), "price": str(row[5]), "size": str(row[6]),
            "match_granularity": str(row[7]),
            "source_side_basis": str(row[8]),
        } for row in rows]
        return {
            "schema_version": QUERY_SCHEMA_VERSION,
            "market": snapshot.market,
            "items": items,
            "meta": self._meta(snapshot, "trade", tag, from_time, to_time,
                               truncated=truncated),
        }, tag

    def footprint(
        self,
        market_id: str,
        interval: str,
        price_bin: str | None,
        from_time: datetime,
        to_time: datetime,
    ) -> tuple[dict[str, Any], str]:
        seconds = FOOTPRINT_INTERVALS.get(interval)
        if seconds is None:
            raise MaterializedQueryError("Footprint 周期不支持")
        if (to_time - from_time).total_seconds() > MAX_FOOTPRINT_DAYS * 86_400:
            raise MaterializedQueryError(f"Footprint 单次窗口不得超过 {MAX_FOOTPRINT_DAYS} 日")
        snapshot = self._trade_snapshot(market_id, from_time, to_time)
        auto = price_bin in (None, "auto")
        tick_text = snapshot.market.get("tick_size")
        bin_text = price_bin if not auto else (
            None if tick_text is None
            else format(Decimal(str(tick_text)) * DEFAULT_FOOTPRINT_TIER, "f")
        )
        try:
            bin_value = Decimal(str(bin_text))
        except (InvalidOperation, TypeError):
            raise MaterializedQueryError("市场无有效 tick_size，必须显式指定 price_bin") from None
        if bin_value <= 0:
            raise MaterializedQueryError("price_bin 必须大于零")
        tag = _etag(snapshot.head_generation, "footprint", (
            market_id, interval, bin_value, from_time, to_time,
        ))
        files = _paths(snapshot, "trade_observation")
        if not files:
            return self._empty(
                snapshot, "trade", tag, interval=interval,
                price_bin=format(bin_value, "f"), side_basis="source_taker",
                symbol=snapshot.market["venue_symbol"],
                **{"bin": format(bin_value, "f"),
                   "tier": DEFAULT_FOOTPRINT_TIER if auto else None,
                   "auto": auto, "truncated": False,
                   "coverage_clipped": False, "unknown_side_count": 0,
                   "from": _day_text(from_time), "to": _day_text(to_time),
                   "today": _day_text(datetime.now(UTC))},
            ), tag
        db = _connection()
        try:
            db.execute(
                """
                CREATE TEMP TABLE selected_trade AS
                WITH deduped AS (
                  SELECT observation_id,event_time,side,
                    CAST(price AS DECIMAL(38,12)) AS price_value,
                    CAST(size AS DECIMAL(38,12)) AS size_value,
                    row_number() OVER (
                      PARTITION BY observation_id
                      ORDER BY available_time,ingest_time,normalization_version
                    ) AS duplicate_ordinal
                  FROM read_parquet(?, union_by_name=true)
                  WHERE market_id=? AND event_time>=? AND event_time<?
                    AND available_time<=? AND side IN ('buy','sell')
                )
                SELECT *,to_timestamp(floor(epoch(event_time)/?)*?) AS bar_time,
                  floor(price_value/CAST(? AS DECIMAL(38,12)))
                    * CAST(? AS DECIMAL(38,12)) AS price_level
                FROM deduped WHERE duplicate_ordinal=1
                """,
                [files, market_id, from_time, to_time, datetime.now(UTC),
                 seconds, seconds, bin_value, bin_value],
            )
            bars = db.execute(
                """
                SELECT bar_time,
                  first(price_value ORDER BY event_time,observation_id),
                  max(price_value),min(price_value),
                  last(price_value ORDER BY event_time,observation_id),
                  sum(size_value),sum(price_value*size_value),
                  sum(size_value) FILTER (WHERE side='buy'),
                  sum(size_value) FILTER (WHERE side='sell'),
                  sum(price_value*size_value) FILTER (WHERE side='buy'),
                  sum(price_value*size_value) FILTER (WHERE side='sell'),
                  count(*),count(*) FILTER (WHERE side='buy'),
                  count(*) FILTER (WHERE side='sell')
                FROM selected_trade GROUP BY bar_time ORDER BY bar_time
                """
            ).fetchall()
            levels = db.execute(
                """
                SELECT bar_time,price_level,
                  coalesce(sum(size_value) FILTER (WHERE side='buy'),0),
                  coalesce(sum(size_value) FILTER (WHERE side='sell'),0),
                  count(*) FILTER (WHERE side='buy'),
                  count(*) FILTER (WHERE side='sell')
                FROM selected_trade GROUP BY bar_time,price_level
                ORDER BY bar_time,price_level
                """
            ).fetchall()
        finally:
            db.close()
        by_bar: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in levels:
            price = _decimal_text(row[1])
            buy_volume = _decimal_text(row[2]); sell_volume = _decimal_text(row[3])
            by_bar[_iso(row[0])].append({
                "price": price, "price_bin": price,
                "buy_volume": buy_volume, "sell_volume": sell_volume,
                "buy": buy_volume, "sell": sell_volume,
                "buy_notional": format(Decimal(price) * Decimal(buy_volume), "f"),
                "sell_notional": format(Decimal(price) * Decimal(sell_volume), "f"),
                "buy_count": int(row[4]), "sell_count": int(row[5]),
            })
        items: list[dict[str, Any]] = []
        for row in bars:
            key = _iso(row[0])
            buy = Decimal(str(row[7] or 0)); sell = Decimal(str(row[8] or 0))
            buy_notional = Decimal(str(row[9] or 0))
            sell_notional = Decimal(str(row[10] or 0))
            bar_levels = by_bar.get(key, [])
            poc, vah, val = _value_area(bar_levels)
            items.append({
                "open_time": key,
                "close_time": _iso(row[0] + timedelta(seconds=seconds)),
                "open": _decimal_text(row[1]), "high": _decimal_text(row[2]),
                "low": _decimal_text(row[3]), "close": _decimal_text(row[4]),
                "volume": _decimal_text(row[5]), "notional": _decimal_text(row[6]),
                "buy_volume": format(buy, "f"), "sell_volume": format(sell, "f"),
                "delta": format(buy - sell, "f"), "trade_count": int(row[11]),
                "total": _decimal_text(row[5]),
                "delta_notional": format(buy_notional - sell_notional, "f"),
                "total_notional": _decimal_text(row[6]),
                "buy_count": int(row[12]), "sell_count": int(row[13]),
                "unknown_side_count": 0,
                "poc": poc, "vah": vah, "val": val,
                "source": "materialized_active_head",
                "levels": bar_levels,
            })
        return {
            "schema_version": QUERY_SCHEMA_VERSION,
            "market": snapshot.market,
            "bars": items,
            "meta": self._meta(
                snapshot, "trade", tag, from_time, to_time,
                interval=interval, price_bin=format(bin_value, "f"),
                side_basis="source_taker", unknown_side_count=0,
                symbol=snapshot.market["venue_symbol"],
                **{"bin": format(bin_value, "f"),
                   "tier": DEFAULT_FOOTPRINT_TIER if auto else None,
                   "auto": auto, "truncated": False,
                   "coverage_clipped": False,
                   "from": _day_text(from_time), "to": _day_text(to_time),
                   "today": _day_text(datetime.now(UTC))},
            ),
        }, tag

    def latest_l2(
        self, market_id: str, depth: int, *, decision_time: datetime | None = None,
    ) -> tuple[dict[str, Any], str]:
        resolved_time = _decision_time(decision_time)
        snapshot = self.catalog.active_outputs(
            market_id, domains=("book_l2",),
            datasets=("book_l2_frame", "book_l2_level"),
        )
        return self.latest_l2_from_snapshot(
            snapshot,
            depth,
            decision_time=resolved_time,
            use_cache=decision_time is None,
        )

    def latest_l2_from_snapshot(
        self,
        snapshot: ActiveOutputSnapshot,
        depth: int,
        *,
        decision_time: datetime,
        checkpoint_snapshot: ActiveOutputSnapshot | None = None,
        use_cache: bool = False,
    ) -> tuple[dict[str, Any], str]:
        """只使用调用方冻结的 head，在显式 PIT 上解析 L2。"""
        if depth <= 0:
            raise MaterializedQueryError("L2 depth 必须为正数")
        resolved_time = _decision_time(decision_time)
        market_id = str(snapshot.market["market_id"])
        tag = _etag(snapshot.head_generation, "book-l2-latest", (
            market_id, depth, resolved_time.isoformat(),
        ))
        cache_key = (market_id, snapshot.head_generation)
        state = self._l2_cache.get(cache_key) if use_cache else None
        if state is None:
            if str(snapshot.market.get("venue_id")) == "okx":
                state = self.replay_l2_state_from_snapshot(
                    snapshot, decision_time=resolved_time,
                )
            else:
                state = self._checkpoint_state(
                    snapshot,
                    decision_time=resolved_time,
                    checkpoint_snapshot=checkpoint_snapshot,
                )
                if state is None:
                    state = self._replay_l2(snapshot, decision_time=resolved_time)
            if use_cache:
                if len(self._l2_cache) >= 16:
                    self._l2_cache.pop(next(iter(self._l2_cache)))
                self._l2_cache[cache_key] = state
        full_asks = [{
            **row,
            "notional": format(Decimal(row["price"]) * Decimal(row["size"]), "f"),
        } for row in state["asks"]]
        full_bids = [{
            **row,
            "notional": format(Decimal(row["price"]) * Decimal(row["size"]), "f"),
        } for row in state["bids"]]
        asks = full_asks[:depth]
        bids = full_bids[:depth]
        best_ask = Decimal(full_asks[0]["price"])
        best_bid = Decimal(full_bids[0]["price"])
        ask_best_size = Decimal(full_asks[0]["size"])
        bid_best_size = Decimal(full_bids[0]["size"])
        spread = best_ask - best_bid; mid = (best_ask + best_bid) / 2
        microprice = (
            best_ask * bid_best_size + best_bid * ask_best_size
        ) / (ask_best_size + bid_best_size)
        ask_total = sum((Decimal(row["size"]) for row in asks), Decimal(0))
        bid_total = sum((Decimal(row["size"]) for row in bids), Decimal(0))
        source_ask_bp = (Decimal(full_asks[-1]["price"]) / mid - 1) * 10_000
        source_bid_bp = (1 - Decimal(full_bids[-1]["price"]) / mid) * 10_000
        bands: list[dict[str, object]] = []
        for width in (5, 10, 25, 50):
            ask_edge = mid * (Decimal(1) + Decimal(width) / Decimal(10_000))
            bid_edge = mid * (Decimal(1) - Decimal(width) / Decimal(10_000))
            ask_band = [
                row for row in full_asks if Decimal(row["price"]) <= ask_edge
            ]
            bid_band = [
                row for row in full_bids if Decimal(row["price"]) >= bid_edge
            ]
            ask_size = sum((Decimal(row["size"]) for row in ask_band), Decimal(0))
            bid_size = sum((Decimal(row["size"]) for row in bid_band), Decimal(0))
            ask_notional = sum((Decimal(row["notional"]) for row in ask_band), Decimal(0))
            bid_notional = sum((Decimal(row["notional"]) for row in bid_band), Decimal(0))
            size_sum = ask_size + bid_size; notional_sum = ask_notional + bid_notional
            ask_complete = source_ask_bp >= width
            bid_complete = source_bid_bp >= width
            complete = ask_complete and bid_complete
            bands.append({
                "band_bp": str(width),
                "ask_complete": ask_complete, "bid_complete": bid_complete,
                "complete": complete,
                "ask_size": format(ask_size, "f") if ask_complete else None,
                "bid_size": format(bid_size, "f") if bid_complete else None,
                "ask_notional": (
                    format(ask_notional, "f") if ask_complete else None
                ),
                "bid_notional": (
                    format(bid_notional, "f") if bid_complete else None
                ),
                "imbalance_size": None if not complete else format(
                    Decimal(0) if size_sum == 0 else (bid_size - ask_size) / size_sum,
                    "f",
                ),
                "imbalance_notional": None if not complete else format(
                    Decimal(0) if notional_sum == 0
                    else (bid_notional - ask_notional) / notional_sum,
                    "f",
                ),
            })
        payload = {
            "schema_version": QUERY_SCHEMA_VERSION,
            "market": snapshot.market,
            "symbol": snapshot.market["venue_symbol"],
            "source": "materialized_active_head",
            "asks": asks, "bids": bids,
            "best_ask": format(best_ask, "f"),
            "best_bid": format(best_bid, "f"),
            "spread": format(spread, "f"), "mid": format(mid, "f"),
            "microprice": format(microprice, "f"),
            "coverage": {
                "ask_bp": format((Decimal(asks[-1]["price"]) / mid - 1) * 10_000, "f"),
                "bid_bp": format((1 - Decimal(bids[-1]["price"]) / mid) * 10_000, "f"),
            },
            "source_coverage": {
                "ask_bp": format(source_ask_bp, "f"),
                "bid_bp": format(source_bid_bp, "f"),
            },
            "bands": bands,
            "ask_total": format(ask_total, "f"),
            "bid_total": format(bid_total, "f"),
            "as_of": state["as_of_available_time"],
            "meta": {
                **self._meta(snapshot, "book_l2", tag),
                "as_of_event_time": state["as_of_event_time"],
                "as_of_available_time": state["as_of_available_time"],
                "snapshot_event_time": state["snapshot_event_time"],
                "replay_frames": state["replay_frames"],
                "integrity_mode": state["integrity_mode"],
                "source_depth_levels": state["source_depth_levels"],
                "state_source": state["state_source"],
                "decision_time": resolved_time.isoformat(),
                "as_of_frame_id": state.get("as_of_frame_id"),
                "snapshot_frame_id": state.get("snapshot_frame_id"),
                "source_attempt_id": state.get("source_attempt_id"),
                "source_partition_key": state.get("source_partition_key"),
                "source_artifact_id": state.get("source_artifact_id"),
                "state_attempt_id": state.get("state_attempt_id"),
                "state_artifact_id": state.get("state_artifact_id"),
                "source_attempt_ids": state.get(
                    "source_attempt_ids",
                    sorted({row.attempt_id for row in snapshot.outputs}),
                ),
                "source_artifact_ids": state.get(
                    "source_artifact_ids",
                    sorted({row.artifact_id for row in snapshot.outputs}),
                ),
                "returned_depth": depth,
                "returned_depth_clipped": (
                    len(full_asks) > depth or len(full_bids) > depth
                ),
                "band_basis": "full_replayed_state",
            },
        }
        return payload, tag

    def replay_l2_state_from_snapshot(
        self,
        snapshot: ActiveOutputSnapshot,
        *,
        decision_time: datetime,
    ) -> dict[str, Any]:
        """按显式 PIT 解析成品状态，不读取 book_state。"""
        resolved_time = _decision_time(decision_time)
        if str(snapshot.market.get("venue_id")) == "okx":
            terminal = self._okx_terminal_state(
                snapshot, decision_time=resolved_time,
            )
            if terminal is not None:
                return terminal
        return self._replay_l2(snapshot, decision_time=resolved_time)

    def orderflow_tiles(
        self,
        market_id: str,
        bucket: str,
        from_time: datetime,
        to_time: datetime,
        limit: int = MAX_TILE_COLUMNS,
    ) -> tuple[dict[str, Any], str]:
        """读取活动小时 tile；变化量与逐笔成交保持两个独立口径。"""
        snapshot = self.catalog.active_outputs(
            market_id, domains=("orderflow_tile",),
            datasets=("orderflow_tile_column", "orderflow_tile_cell"),
            from_time=from_time, to_time=to_time,
        )
        column_files = _paths(snapshot, "orderflow_tile_column")
        cell_files = _paths(snapshot, "orderflow_tile_cell")
        tag = _etag(snapshot.head_generation, "orderflow-tiles", (
            market_id, bucket, from_time, to_time, limit,
        ))
        if not column_files or not cell_files:
            return {
                "schema_version": QUERY_SCHEMA_VERSION,
                "market": snapshot.market,
                "columns": [],
                "meta": self._meta(
                    snapshot, "orderflow_tile", tag, from_time, to_time,
                    bucket=bucket, truncated=False,
                    attribution="l2_change_and_trade_kept_separate",
                ),
            }, tag
        db = _connection()
        try:
            column_schema = _parquet_columns(db, column_files)
            cell_schema = _parquet_columns(db, cell_files)
            column_row_size = "row_size" if "row_size" in column_schema else "NULL"
            column_basis = (
                "price_quantum_basis" if "price_quantum_basis" in column_schema
                else "'legacy_unspecified'"
            )
            cell_row_size = "row_size" if "row_size" in cell_schema else "NULL"
            cell_basis = (
                "price_quantum_basis" if "price_quantum_basis" in cell_schema
                else "'legacy_unspecified'"
            )
            anchor_row = db.execute(
                "SELECT MAX(bucket_epoch) FROM read_parquet(?, union_by_name=true) "
                "WHERE market_id=? AND bucket=? AND is_anchor AND bucket_start<=?",
                [column_files, market_id, bucket, from_time],
            ).fetchone()
            anchor_epoch = (
                None if anchor_row is None or anchor_row[0] is None
                else int(anchor_row[0])
            )
            query_from = (
                from_time if anchor_epoch is None
                else datetime.fromtimestamp(anchor_epoch, UTC)
            )
            rows = db.execute(
                f"""
                SELECT column_id,bucket_epoch,bucket_start,bucket_end,
                       coverage_state,is_anchor,is_reset,is_carried,is_gap,
                       frame_count,trade_count,last_event_time,last_available_time,
                       integrity_mode,source_generation,method_version,
                       {column_row_size} AS row_size,
                       {column_basis} AS price_quantum_basis
                FROM read_parquet(?, union_by_name=true)
                WHERE market_id=? AND bucket=? AND bucket_start>=? AND bucket_start<?
                ORDER BY bucket_epoch LIMIT ?
                """,
                [column_files, market_id, bucket, query_from, to_time,
                 limit + MAX_TILE_ANCHOR_CONTEXT + 1],
            ).fetchall()
            context_rows = [row for row in rows if row[2] < from_time]
            requested_rows = [row for row in rows if row[2] >= from_time]
            truncated = len(requested_rows) > limit
            rows = [*context_rows, *requested_rows[:limit]]
            column_ids = [str(row[0]) for row in rows]
            cell_rows = [] if not column_ids else db.execute(
                f"""
                SELECT column_id,book_side,price_key,price,book_end_size,
                       net_increase,net_decrease_unknown,taker_buy_size,
                       taker_sell_size,state_role,{cell_row_size} AS row_size,
                       {cell_basis} AS price_quantum_basis
                FROM read_parquet(?, union_by_name=true)
                WHERE market_id=? AND bucket=? AND column_id=ANY(?)
                ORDER BY bucket_epoch,book_side,price_key
                """,
                [cell_files, market_id, bucket, column_ids],
            ).fetchall()
        finally:
            db.close()
        cells: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in cell_rows:
            cells[str(row[0])].append({
                "book_side": str(row[1]), "price_key": int(row[2]),
                "price": str(row[3]),
                "book_end_size": None if row[4] is None else str(row[4]),
                "net_increase": str(row[5]),
                "net_decrease_unknown": str(row[6]),
                "taker_buy_size": str(row[7]), "taker_sell_size": str(row[8]),
                "state_role": str(row[9]),
                "row_size": None if row[10] is None else str(row[10]),
                "price_quantum_basis": str(row[11]),
            })
        columns = [{
            "column_id": str(row[0]), "bucket_epoch": int(row[1]),
            "bucket_start": _iso(row[2]), "bucket_end": _iso(row[3]),
            "coverage_state": str(row[4]), "is_anchor": bool(row[5]),
            "is_reset": bool(row[6]), "is_carried": bool(row[7]),
            "is_gap": bool(row[8]), "frame_count": int(row[9]),
            "trade_count": int(row[10]),
            "last_event_time": None if row[11] is None else _iso(row[11]),
            "last_available_time": None if row[12] is None else _iso(row[12]),
            "integrity_mode": str(row[13]),
            "source_generation": str(row[14]), "method_version": str(row[15]),
            "row_size": None if row[16] is None else str(row[16]),
            "price_quantum_basis": str(row[17]),
            "context_only": bool(row[2] < from_time),
            "cells": cells.get(str(row[0]), []),
        } for row in rows]
        return {
            "schema_version": QUERY_SCHEMA_VERSION,
            "market": snapshot.market,
            "columns": columns,
            "meta": self._meta(
                snapshot, "orderflow_tile", tag, from_time, to_time,
                bucket=bucket, truncated=truncated,
                attribution="l2_change_and_trade_kept_separate",
                sparse_contract="periodic_anchor_plus_changes",
                anchor_context_columns=len(context_rows),
                context_from=None if not rows else _iso(rows[0][2]),
            ),
        }, tag

    def _trade_snapshot(
        self, market_id: str, from_time: datetime, to_time: datetime,
    ) -> ActiveOutputSnapshot:
        return self.catalog.active_outputs(
            market_id, domains=("trade", "trade_realtime"),
            datasets=("trade_observation",),
            from_time=from_time, to_time=to_time,
        )

    @staticmethod
    def _meta(
        snapshot: ActiveOutputSnapshot, domain: str, etag: str,
        from_time: datetime | None = None, to_time: datetime | None = None,
        **extra: object,
    ) -> dict[str, Any]:
        outputs = [row for row in snapshot.outputs if row.domain == domain or (
            domain == "trade" and row.domain == "trade_realtime"
        )]
        coverage_lows = [
            row.min_event_time for row in outputs if row.min_event_time is not None
        ]
        coverage_highs = [
            row.max_event_time for row in outputs if row.max_event_time is not None
        ]
        return {
            "market_id": snapshot.market["market_id"],
            "domain": domain,
            "head_generation": snapshot.head_generation,
            "etag": etag,
            "normalization_versions": sorted({row.normalization_version for row in outputs}),
            "partition_count": len({(row.domain, row.partition_key) for row in outputs}),
            "coverage_from": min(coverage_lows).isoformat() if coverage_lows else None,
            "coverage_to": max(coverage_highs).isoformat() if coverage_highs else None,
            "requested_from": None if from_time is None else from_time.isoformat(),
            "requested_to": None if to_time is None else to_time.isoformat(),
            **extra,
        }

    def _empty(
        self, snapshot: ActiveOutputSnapshot, domain: str, etag: str,
        **extra: object,
    ) -> dict[str, Any]:
        key = "bars" if "interval" in extra and domain == "trade" else "items"
        metadata = self._meta(snapshot, domain, etag)
        metadata.update(extra)
        return {
            "schema_version": QUERY_SCHEMA_VERSION,
            "market": snapshot.market,
            key: [],
            "meta": metadata,
        }

    def _okx_terminal_state(
        self,
        snapshot: ActiveOutputSnapshot,
        *,
        decision_time: datetime,
    ) -> dict[str, Any] | None:
        """以最新可见终态为基座，仅重放同连接尾部。"""
        conn = connect_readonly(self.catalog.data_root)
        if conn is None:
            return None
        try:
            try:
                loaded = load_latest_terminal_checkpoint(
                    self.catalog.data_root,
                    conn,
                    str(snapshot.market["market_id"]),
                    decision_time=decision_time,
                )
            except TerminalCheckpointError:
                # 损坏制品不得参与；回退事实重放。
                return None
        finally:
            conn.close()
        if loaded is None or not loaded.checkpoint.trusted:
            return None
        checkpoint = loaded.checkpoint
        frame_outputs = [
            row for row in snapshot.outputs
            if row.dataset == "book_l2_frame"
            and row.normalization_version == checkpoint.source_normalization_version
            and row.partition_key.startswith("live/")
        ]
        level_outputs = [
            row for row in snapshot.outputs
            if row.dataset == "book_l2_level"
            and row.normalization_version == checkpoint.source_normalization_version
            and row.partition_key.startswith("live/")
        ]
        source_attempts = {row.attempt_id for row in frame_outputs}
        if (
            checkpoint.source_attempt_id not in source_attempts
            or checkpoint.as_of_frame_id is None
            or checkpoint.as_of_event_time is None
            or checkpoint.as_of_available_time is None
            or checkpoint.as_of_ingest_time is None
            or checkpoint.snapshot_frame_id is None
            or checkpoint.snapshot_event_time is None
            or checkpoint.sequence_id is None
        ):
            return None
        candidate_frames = [
            row for row in frame_outputs
            if (
                row.partition_key.split("/")[1] != checkpoint.run_id
                or (
                    row.partition_key.split("/")[2].startswith("segment-")
                    and int(row.partition_key.rsplit("-", 1)[1])
                    > checkpoint.segment_sequence
                )
            )
        ]
        if not candidate_frames:
            return self._terminal_base_state(snapshot, loaded)
        frame_files = [str(row.path) for row in candidate_frames]
        db = _connection()
        try:
            columns = _parquet_columns(db, frame_files)
            required = {
                "connection_id", "run_id", "segment_sequence",
                "source_row_index", "prev_sequence_id",
            }
            if not required <= columns:
                return None
            frames = db.execute(
                """
                WITH deduped AS (
                  SELECT frame_id,message_kind,event_time,available_time,
                         ingest_time,sequence_id,prev_sequence_id,run_id,
                         connection_id,segment_sequence,source_row_index,
                         integrity_mode,source_depth_levels,
                         filename AS source_file,
                         row_number() OVER (
                           PARTITION BY frame_id
                           ORDER BY available_time,ingest_time
                         ) AS selected
                  FROM read_parquet(?, union_by_name=true, filename=true)
                  WHERE market_id=? AND available_time<=? AND (
                    (run_id=? AND segment_sequence>?) OR
                    (run_id<>? AND ingest_time>?)
                  )
                ), ordered AS (
                  SELECT * EXCLUDE(selected),min(ingest_time) OVER (
                    PARTITION BY run_id
                  ) AS run_started
                  FROM deduped WHERE selected=1
                )
                SELECT frame_id,message_kind,event_time,available_time,
                       ingest_time,sequence_id,prev_sequence_id,run_id,
                       connection_id,segment_sequence,source_row_index,
                       integrity_mode,source_depth_levels,source_file,run_started
                FROM ordered
                ORDER BY run_started,run_id,segment_sequence,source_row_index,
                         ingest_time,frame_id
                LIMIT ?
                """,
                [
                    frame_files, snapshot.market["market_id"], decision_time,
                    checkpoint.run_id, checkpoint.segment_sequence,
                    checkpoint.run_id, checkpoint.as_of_ingest_time,
                    MAX_L2_REPLAY_FRAMES + 1,
                ],
            ).fetchall()
            if len(frames) > MAX_L2_REPLAY_FRAMES:
                raise MaterializedQueryError("OKX 终态后的增量超过安全重放上限")
            if not frames:
                return self._terminal_base_state(snapshot, loaded)
            latest_run = str(frames[-1][7])
            run_frames = [row for row in frames if str(row[7]) == latest_run]
            latest_connection = str(run_frames[-1][8])
            connection_frames = [
                row for row in run_frames if str(row[8]) == latest_connection
            ]
            uses_terminal = (
                latest_run == checkpoint.run_id
                and latest_connection == checkpoint.connection_id
            )
            if uses_terminal:
                replay_frames = connection_frames
            else:
                anchor_at = max(
                    (
                        index for index, row in enumerate(connection_frames)
                        if str(row[1]) == "snapshot"
                    ),
                    default=-1,
                )
                if anchor_at < 0:
                    raise MaterializedQueryError(
                        "OKX 连接切换后尚无可见 snapshot"
                    )
                replay_frames = connection_frames[anchor_at:]
            used_frame_paths = {_path_key(row[13]) for row in replay_frames}
            used_frame_outputs = [
                row for row in candidate_frames
                if _path_key(row.path) in used_frame_paths
            ]
            used_attempts = {row.attempt_id for row in used_frame_outputs}
            used_level_outputs = [
                row for row in level_outputs if row.attempt_id in used_attempts
            ]
            level_files = [str(row.path) for row in used_level_outputs]
            if not level_files:
                raise MaterializedQueryError("OKX 尾部 frame 缺少同尝试 level")
            frame_ids = [str(row[0]) for row in replay_frames]
            levels = db.execute(
                """
                SELECT frame_id,side,price,size,action,source_level_index
                FROM read_parquet(?, union_by_name=true)
                WHERE frame_id=ANY(?)
                ORDER BY frame_id,side,source_level_index
                """,
                [level_files, frame_ids],
            ).fetchall()
        finally:
            db.close()
        levels_by_frame: dict[str, list[tuple[Any, ...]]] = defaultdict(list)
        for row in levels:
            levels_by_frame[str(row[0])].append(row)
        asks = (
            {Decimal(row.price): Decimal(row.size) for row in checkpoint.asks}
            if uses_terminal else {}
        )
        bids = (
            {Decimal(row.price): Decimal(row.size) for row in checkpoint.bids}
            if uses_terminal else {}
        )
        current_sequence = checkpoint.sequence_id if uses_terminal else None
        snapshot_frame = checkpoint.snapshot_frame_id if uses_terminal else None
        snapshot_event = checkpoint.snapshot_event_time if uses_terminal else None
        state_available = (
            checkpoint.checkpoint_available_time if uses_terminal else None
        )
        for frame in replay_frames:
            kind = str(frame[1])
            sequence = _sequence_integer(frame[5])
            previous = _sequence_integer(frame[6])
            if sequence is None or previous is None:
                raise MaterializedQueryError("OKX 尾部缺少原生序列")
            if kind == "snapshot":
                asks.clear(); bids.clear()
                snapshot_frame = str(frame[0]); snapshot_event = frame[2]
            elif current_sequence is None or previous != current_sequence:
                raise MaterializedQueryError("OKX 终态尾部序列不连续")
            for level in levels_by_frame.get(str(frame[0]), []):
                book = asks if str(level[1]) == "ask" else bids
                price = Decimal(str(level[2])); size = Decimal(str(level[3]))
                if str(level[4]) == "delete" or size == 0:
                    book.pop(price, None)
                else:
                    book[price] = size
            current_sequence = sequence
            available = frame[3]
            state_available = (
                available if state_available is None
                else max(state_available, available)
            )
        if not asks or not bids or snapshot_frame is None or snapshot_event is None:
            raise MaterializedQueryError("OKX 终态重放后盘口不完整")
        last = replay_frames[-1]
        frame_by_path = {
            _path_key(row.path): row for row in used_frame_outputs
        }
        source_output = frame_by_path.get(_path_key(last[13]))
        if source_output is None:
            raise MaterializedQueryError("OKX 尾部末帧无法绑定来源 attempt")
        lineage_outputs = [*used_frame_outputs, *used_level_outputs]
        lineage_attempts = {row.attempt_id for row in lineage_outputs}
        lineage_artifacts = {row.artifact_id for row in lineage_outputs}
        if uses_terminal:
            lineage_attempts.add(loaded.attempt_id)
            lineage_artifacts.add(loaded.artifact_id)
        assert state_available is not None
        return {
            "asks": [
                {"price": format(price, "f"), "size": format(asks[price], "f")}
                for price in sorted(asks)
            ],
            "bids": [
                {"price": format(price, "f"), "size": format(bids[price], "f")}
                for price in sorted(bids, reverse=True)
            ],
            "as_of_event_time": _iso(last[2]),
            "as_of_available_time": _iso(state_available),
            "snapshot_event_time": _iso(snapshot_event),
            "as_of_frame_id": str(last[0]),
            "snapshot_frame_id": snapshot_frame,
            "source_attempt_id": source_output.attempt_id,
            "source_partition_key": source_output.partition_key,
            "source_artifact_id": source_output.artifact_id,
            "state_attempt_id": loaded.attempt_id if uses_terminal else None,
            "state_artifact_id": loaded.artifact_id if uses_terminal else None,
            "source_attempt_ids": sorted(lineage_attempts),
            "source_artifact_ids": sorted(lineage_artifacts),
            "replay_frames": len(replay_frames),
            "integrity_mode": str(last[11]),
            "source_depth_levels": (
                None if last[12] is None else int(last[12])
            ),
            "state_source": (
                f"{TERMINAL_CHECKPOINT_DATASET}_plus_tail"
                if uses_terminal
                else "l2_wire_order_snapshot_delta_replay"
            ),
        }

    @staticmethod
    def _terminal_base_state(
        snapshot: ActiveOutputSnapshot,
        loaded: LoadedTerminalCheckpoint,
    ) -> dict[str, Any]:
        """把可信终态转换为查询基座。"""
        checkpoint = loaded.checkpoint
        frame_source = next(
            (
                row for row in snapshot.outputs
                if row.attempt_id == checkpoint.source_attempt_id
                and row.dataset == "book_l2_frame"
            ),
            None,
        )
        if (
            frame_source is None
            or checkpoint.as_of_event_time is None
            or checkpoint.snapshot_event_time is None
            or checkpoint.as_of_frame_id is None
            or checkpoint.snapshot_frame_id is None
        ):
            raise MaterializedQueryError("OKX 终态来源身份不完整")
        return {
            "asks": [
                {"price": row.price, "size": row.size}
                for row in checkpoint.asks
            ],
            "bids": [
                {"price": row.price, "size": row.size}
                for row in checkpoint.bids
            ],
            "as_of_event_time": checkpoint.as_of_event_time.isoformat(),
            "as_of_available_time": checkpoint.checkpoint_available_time.isoformat(),
            "snapshot_event_time": checkpoint.snapshot_event_time.isoformat(),
            "as_of_frame_id": checkpoint.as_of_frame_id,
            "snapshot_frame_id": checkpoint.snapshot_frame_id,
            "source_attempt_id": checkpoint.source_attempt_id,
            "source_partition_key": frame_source.partition_key,
            "source_artifact_id": frame_source.artifact_id,
            "state_attempt_id": loaded.attempt_id,
            "state_artifact_id": loaded.artifact_id,
            "source_attempt_ids": [loaded.attempt_id],
            "source_artifact_ids": [loaded.artifact_id],
            "replay_frames": 0,
            "integrity_mode": (
                "native_prev_seq+checksum_unsupported+terminal_checkpoint"
            ),
            "source_depth_levels": max(
                len(checkpoint.asks), len(checkpoint.bids),
            ),
            "state_source": TERMINAL_CHECKPOINT_DATASET,
        }

    @staticmethod
    def _replay_l2(
        snapshot: ActiveOutputSnapshot, *, decision_time: datetime,
    ) -> dict[str, Any]:
        decision_time = _decision_time(decision_time)
        frame_outputs = [
            row for row in snapshot.outputs if row.dataset == "book_l2_frame"
        ]
        level_outputs = [
            row for row in snapshot.outputs if row.dataset == "book_l2_level"
        ]
        if not frame_outputs or not level_outputs:
            raise MaterializedQueryError("市场没有活动 L2 frame/level 成品")
        floor_time = datetime.min.replace(tzinfo=UTC)
        ordered_frames = sorted(
            frame_outputs,
            key=lambda row: (row.max_event_time or floor_time, row.partition_key),
            reverse=True,
        )
        newest_time = ordered_frames[0].max_event_time
        selected_outputs = [
            row for row in ordered_frames if row.max_event_time == newest_time
        ]
        remaining_outputs = [
            row for row in ordered_frames if row.max_event_time != newest_time
        ]
        is_bitbank = str(snapshot.market.get("venue_id")) == "bitbank"
        frame_files = [str(row.path) for row in selected_outputs]
        db = _connection()
        try:
            frames: list[tuple[Any, ...]] = []
            while True:
                frame_columns = _parquet_columns(db, frame_files)
                depth_expr = (
                    "source_depth_levels"
                    if "source_depth_levels" in frame_columns
                    else "depth_limit" if "depth_limit" in frame_columns
                    else "greatest(book_bid_levels,book_ask_levels)"
                    if {"book_bid_levels", "book_ask_levels"} <= frame_columns
                    else "NULL"
                )
                segment_expr = (
                    "segment_sequence" if "segment_sequence" in frame_columns else "0"
                )
                session_expr = wire_session_sql(frame_columns, segment_expr)
                base = f"""
                    WITH deduped AS (
                      SELECT frame_id,message_kind,event_time,available_time,
                             ingest_time,sequence_id,{session_expr} AS wire_session,
                             {segment_expr} AS wire_segment,source_row_index,
                              integrity_mode,{depth_expr} AS source_depth_levels,
                              filename AS source_file,
                             row_number() OVER (
                               PARTITION BY frame_id
                               ORDER BY available_time,ingest_time
                             ) AS selected
                       FROM read_parquet(?, union_by_name=true, filename=true)
                      WHERE market_id=? AND available_time<=?
                    ), ordered AS (
                      SELECT * EXCLUDE(selected),min(ingest_time) OVER (
                        PARTITION BY wire_session
                      ) AS session_started
                      FROM deduped WHERE selected=1
                    )
                """
                if is_bitbank:
                    frames = db.execute(
                        base + """
                    SELECT frame_id,message_kind,event_time,available_time,
                           ingest_time,sequence_id,wire_session,wire_segment,
                           source_row_index,integrity_mode,source_depth_levels,
                           source_file
                    FROM ordered
                    ORDER BY session_started,wire_session,wire_segment,
                             source_row_index,ingest_time,frame_id
                    LIMIT ?
                    """, [
                            frame_files, snapshot.market["market_id"],
                            decision_time, MAX_L2_REPLAY_FRAMES + 1,
                        ],
                    ).fetchall()
                    if frames:
                        latest_session = str(frames[-1][6])
                        snapshot_count = sum(
                            str(row[6]) == latest_session
                            and str(row[1]) == "snapshot"
                            for row in frames
                        )
                        # 两个 whole 界定完整缓冲窗。
                        if snapshot_count >= 2 or not remaining_outputs:
                            break
                else:
                    parameters = [
                        frame_files, snapshot.market["market_id"],
                        decision_time,
                    ]
                    latest = db.execute(
                        base + """
                        SELECT wire_session,wire_segment,source_row_index
                        FROM ordered
                        ORDER BY session_started DESC,wire_session DESC,
                                 wire_segment DESC,source_row_index DESC,
                                 ingest_time DESC,frame_id DESC LIMIT 1
                        """, parameters,
                    ).fetchone()
                    tail_anchor = None if latest is None else db.execute(
                        base + """
                        SELECT wire_segment,source_row_index
                        FROM ordered WHERE wire_session=?
                          AND message_kind='snapshot'
                        ORDER BY wire_segment DESC,source_row_index DESC,
                                 ingest_time DESC,frame_id DESC LIMIT 1
                        """, [*parameters, str(latest[0])],
                    ).fetchone()
                    if latest is not None and tail_anchor is not None:
                        frames = db.execute(
                            base + """
                            SELECT frame_id,message_kind,event_time,available_time,
                                   ingest_time,sequence_id,wire_session,wire_segment,
                                   source_row_index,integrity_mode,
                                   source_depth_levels,source_file
                            FROM ordered WHERE wire_session=? AND (
                              wire_segment>? OR (
                                wire_segment=? AND source_row_index>=?
                              )
                            )
                            ORDER BY wire_segment,source_row_index,
                                     ingest_time,frame_id LIMIT ?
                            """, [
                                *parameters, str(latest[0]), int(tail_anchor[0]),
                                int(tail_anchor[0]), int(tail_anchor[1]),
                                MAX_L2_REPLAY_FRAMES + 1,
                            ],
                        ).fetchall()
                        break
                if not remaining_outputs:
                    break
                selected_outputs.append(remaining_outputs.pop(0))
                frame_files = [str(row.path) for row in selected_outputs]
            if len(frames) > MAX_L2_REPLAY_FRAMES:
                raise MaterializedQueryError("L2 快照后的增量超过安全重放上限")
            if not frames:
                raise MaterializedQueryError("市场没有合法可见的 L2 帧")
            latest_session = str(frames[-1][6])
            frames = [row for row in frames if str(row[6]) == latest_session]
            frame_ids = [str(row[0]) for row in frames]
            attempts = {row.attempt_id for row in selected_outputs}
            level_files = [
                str(row.path) for row in level_outputs if row.attempt_id in attempts
            ]
            if not level_files:
                raise MaterializedQueryError("L2 活动 frame 缺少同尝试 level 输出")
            levels = db.execute(
                """
                SELECT l.frame_id,l.side,l.price,l.size,l.action,l.source_level_index
                FROM read_parquet(?, union_by_name=true) l
                WHERE l.frame_id=ANY(?)
                ORDER BY l.frame_id,l.side,l.source_level_index
                """,
                [level_files, frame_ids],
            ).fetchall()
        finally:
            db.close()
        levels_by_frame: dict[str, list[tuple[Any, ...]]] = defaultdict(list)
        for row in levels:
            levels_by_frame[str(row[0])].append(row)
        ask: dict[Decimal, Decimal] = {}
        bid: dict[Decimal, Decimal] = {}
        anchor: tuple[Any, ...] | None = None
        last: tuple[Any, ...] | None = None
        state_available: datetime | None = None
        replay_frames = 0
        applied_frame_paths: set[str] = set()

        def apply_frame(frame: tuple[Any, ...], *, reset: bool) -> None:
            applied_frame_paths.add(_path_key(frame[11]))
            if reset:
                ask.clear(); bid.clear()
            for level in levels_by_frame.get(str(frame[0]), []):
                book = ask if str(level[1]) == "ask" else bid
                price = Decimal(str(level[2])); size = Decimal(str(level[3]))
                if str(level[4]) == "delete" or size == 0:
                    book.pop(price, None)
                else:
                    book[price] = size

        if is_bitbank:
            actions = bitbank_replay_actions(
                frames,
                message_kind=lambda row: str(row[1]),
                sequence_id=lambda row: _sequence_integer(row[5]),
                session_id=lambda row: str(row[6]),
                available_time=lambda row: row[3],
            )
            for action in actions:
                if not action.apply_levels:
                    continue
                apply_frame(action.frame, reset=action.reset_book)
                if action.reset_book:
                    anchor = action.frame
                last = action.frame
                state_available = action.effective_available_time
                replay_frames += 1
        else:
            anchor_at = max(
                (index for index, row in enumerate(frames)
                 if str(row[1]) == "snapshot"),
                default=-1,
            )
            for frame in frames[anchor_at:]:
                reset = str(frame[1]) == "snapshot"
                apply_frame(frame, reset=reset)
                if reset:
                    anchor = frame
                last = frame
                state_available = frame[3]
                replay_frames += 1
        if last is None or anchor is None or state_available is None or not ask or not bid:
            raise MaterializedQueryError("L2 重放后盘口为空或缺少单侧")
        source_outputs = {
            _path_key(row.path): row for row in selected_outputs
        }
        source_output = source_outputs.get(_path_key(last[11]))
        if source_output is None:
            raise MaterializedQueryError("L2 末帧无法绑定来源 attempt")
        used_frame_outputs = [
            row for row in selected_outputs
            if _path_key(row.path) in applied_frame_paths
        ]
        used_attempts = {row.attempt_id for row in used_frame_outputs}
        used_level_outputs = [
            row for row in level_outputs if row.attempt_id in used_attempts
        ]
        lineage_outputs = [*used_frame_outputs, *used_level_outputs]
        return {
            "asks": [{"price": format(price, "f"), "size": format(ask[price], "f")}
                     for price in sorted(ask)],
            "bids": [{"price": format(price, "f"), "size": format(bid[price], "f")}
                     for price in sorted(bid, reverse=True)],
            "as_of_event_time": _iso(last[2]),
            "as_of_available_time": _iso(state_available),
            "snapshot_event_time": _iso(anchor[2]),
            "as_of_frame_id": str(last[0]),
            "snapshot_frame_id": str(anchor[0]),
            "source_attempt_id": source_output.attempt_id,
            "source_partition_key": source_output.partition_key,
            "source_artifact_id": source_output.artifact_id,
            "state_attempt_id": None,
            "state_artifact_id": None,
            "source_attempt_ids": sorted({
                row.attempt_id for row in lineage_outputs
            }),
            "source_artifact_ids": sorted({
                row.artifact_id for row in lineage_outputs
            }),
            "replay_frames": replay_frames,
            "integrity_mode": str(last[9]),
            "source_depth_levels": (
                None if last[10] is None else int(last[10])
            ),
            "state_source": "l2_wire_order_snapshot_delta_replay",
        }

    def _checkpoint_state(
        self,
        l2_snapshot: ActiveOutputSnapshot,
        *,
        decision_time: datetime,
        checkpoint_snapshot: ActiveOutputSnapshot | None = None,
    ) -> dict[str, Any] | None:
        """只接受散列、attempt 与制品集合均精确绑定的 checkpoint。"""
        decision_time = _decision_time(decision_time)
        if checkpoint_snapshot is None:
            try:
                checkpoint = self.catalog.active_outputs(
                    str(l2_snapshot.market["market_id"]),
                    domains=("book_state",), datasets=("book_state_checkpoint",),
                )
            except (LookupError, FileNotFoundError):
                return None
        else:
            checkpoint = checkpoint_snapshot
        current_inputs = select_l2_checkpoint_inputs(l2_snapshot)
        expected_attempts = frozenset(
            row.attempt_id for row in current_inputs.outputs
        )
        expected_artifacts = frozenset(
            row.artifact_id for row in current_inputs.outputs
        )
        expected_hash = materialization_input_set_hash(current_inputs.outputs)
        if not expected_attempts or not expected_artifacts:
            return None
        candidates = sorted(
            checkpoint.outputs,
            key=lambda row: row.max_event_time or datetime.min.replace(tzinfo=UTC),
            reverse=True,
        )
        for candidate in candidates:
            if candidate.normalization_version not in {
                "book-state-checkpoint-v2", "book-state-checkpoint-v3",
            }:
                continue
            lineage = self.catalog.attempt_lineage(candidate.attempt_id)
            if (
                lineage is None
                or lineage.input_set_hash != expected_hash
                or lineage.upstream_attempt_ids != expected_attempts
                or lineage.input_artifact_ids != expected_artifacts
            ):
                continue
            db = _connection()
            try:
                rows = db.execute(
                    "SELECT source_attempt_id,as_of_frame_id,event_time,"
                    "available_time,snapshot_frame_id,snapshot_event_time,"
                    "integrity_mode,replay_frames,side,price,size,"
                    "source_depth_levels FROM read_parquet(?) "
                    "ORDER BY side,CAST(price AS DECIMAL(38,12))",
                    [str(candidate.path)],
                ).fetchall()
            finally:
                db.close()
            if not rows:
                continue
            checkpoint_available = rows[0][3]
            if not isinstance(checkpoint_available, datetime):
                raise MaterializedQueryError("checkpoint available_time 类型非法")
            if checkpoint_available.tzinfo is None:
                checkpoint_available = checkpoint_available.replace(tzinfo=UTC)
            if checkpoint_available.astimezone(UTC) > decision_time:
                continue
            source_attempt = str(rows[0][0])
            if source_attempt not in expected_attempts:
                continue
            if any(str(row[0]) != source_attempt for row in rows):
                raise MaterializedQueryError("checkpoint 混入多个上游 attempt")
            asks = [
                {"price": str(row[9]), "size": str(row[10])}
                for row in rows if str(row[8]) == "ask"
            ]
            bids = [
                {"price": str(row[9]), "size": str(row[10])}
                for row in reversed(rows) if str(row[8]) == "bid"
            ]
            if not asks or not bids:
                raise MaterializedQueryError("checkpoint 盘口缺少单侧")
            first = rows[0]
            source_outputs = [
                row for row in l2_snapshot.outputs
                if row.attempt_id == source_attempt
            ]
            frame_source = next(
                (row for row in source_outputs
                 if row.dataset == "book_l2_frame"),
                None,
            )
            if frame_source is None:
                continue
            return {
                "asks": asks, "bids": bids,
                "as_of_event_time": _iso(first[2]),
                "as_of_available_time": _iso(first[3]),
                "snapshot_event_time": _iso(first[5]),
                "as_of_frame_id": str(first[1]),
                "snapshot_frame_id": str(first[4]),
                "source_attempt_id": source_attempt,
                "source_partition_key": frame_source.partition_key,
                "source_artifact_id": frame_source.artifact_id,
                "state_attempt_id": candidate.attempt_id,
                "state_artifact_id": candidate.artifact_id,
                "source_attempt_ids": sorted(expected_attempts),
                "source_artifact_ids": sorted(expected_artifacts),
                "replay_frames": int(first[7]),
                "integrity_mode": str(first[6]),
                "source_depth_levels": (
                    None if first[11] is None else int(first[11])
                ),
                "state_source": "book_state_checkpoint",
            }
        return None


def replay_l2_snapshot(
    snapshot: ActiveOutputSnapshot, *, decision_time: datetime | None = None,
) -> dict[str, Any]:
    """供 checkpoint 物化器复用与在线查询完全相同的重放语义。"""
    return MaterializedQuery._replay_l2(
        snapshot, decision_time=_decision_time(decision_time),
    )
