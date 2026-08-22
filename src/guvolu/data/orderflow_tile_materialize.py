"""活动 L2/逐笔事实到 market-scoped 稀疏 OFL tile 的小时物化。"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sqlite3
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, ROUND_FLOOR
from pathlib import Path
from typing import Any, Sequence

import duckdb

from guvolu.data.durable_io import atomic_write_text
from guvolu.data.book_l2_contract import BOOK_L2_NORMALIZATION_VERSION
from guvolu.data.bitbank_book_replay import (
    bitbank_replay_actions,
    wire_session_sql,
)
from guvolu.data.l2_source_precedence import okx_live_event_coverage
from guvolu.data.materialize import (
    _register_content_artifact,
    _relative_storage_path,
    artifact_id,
    register_materialization_manifest,
    sha256_file,
    utc_now,
)
from guvolu.data.materialization_publication import (
    UnpromotedOutput,
    settle_failed_publication,
)
from guvolu.data.paths import data_root as configured_data_root
from guvolu.data.sqlite_writer_lock import sqlite_writer_lock
from guvolu.data.store import connect
from guvolu.data.watch_connection import connect_with_retry
from guvolu.ui.query_catalog import ActiveOutputSnapshot, QueryCatalog

DOMAIN = "orderflow_tile"
DATASET_COLUMN = "orderflow_tile_column"
DATASET_CELL = "orderflow_tile_cell"
SCHEMA_VERSION = 2
METHOD_VERSION = "orderflow-tile-sparse-v8"
BUCKET_SECONDS = {"1s": 1, "5s": 5, "1min": 60}
ROW_TICK_TIERS = {"1s": 1, "5s": 2, "1min": 10}
ANCHOR_COLUMNS = 128
CARRY_LIMIT_SECONDS = 30
ZERO = Decimal(0)


@dataclass(frozen=True)
class Level:
    side: str
    price: Decimal
    size: Decimal
    action: str


@dataclass(frozen=True)
class Frame:
    frame_id: str
    message_kind: str
    event_time: datetime
    available_time: datetime
    integrity_mode: str
    source_run: str
    levels: tuple[Level, ...]
    sequence_id: int | None = None
    wire_segment: int = 0
    wire_row: int = 0
    attribute_changes: bool = True


@dataclass(frozen=True)
class TileBuild:
    columns: tuple[tuple[object, ...], ...]
    cells: tuple[tuple[object, ...], ...]
    coverage_from: datetime
    coverage_to: datetime


@dataclass(frozen=True)
class TileResult:
    attempt_id: str
    market_id: str
    partition_key: str
    column_rows: int
    cell_rows: int
    reused: bool


def _hour(value: datetime) -> datetime:
    value = value.astimezone(UTC)
    return value.replace(minute=0, second=0, microsecond=0)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat()


def _paths(snapshot: ActiveOutputSnapshot, dataset: str) -> list[str]:
    return [str(row.path) for row in snapshot.outputs if row.dataset == dataset]


def _columns(db: Any, files: list[str]) -> set[str]:
    return {
        str(row[0]) for row in db.execute(
            "DESCRIBE SELECT * FROM read_parquet(?, union_by_name=true)",
            [files],
        ).fetchall()
    }


def _source_snapshot(
    root: Path, market_id: str, start: datetime, end: datetime,
) -> ActiveOutputSnapshot:
    catalog = QueryCatalog(root)
    l2 = catalog.active_outputs(
        market_id, domains=("book_l2",),
        datasets=("book_l2_frame", "book_l2_level"),
        from_time=start - timedelta(hours=1), to_time=end,
    )
    paired: dict[str, set[str]] = defaultdict(set)
    for row in l2.outputs:
        paired[row.attempt_id].add(row.dataset)
    complete = {
        attempt for attempt, datasets in paired.items()
        if datasets == {"book_l2_frame", "book_l2_level"}
    }
    l2_outputs = tuple(row for row in l2.outputs if row.attempt_id in complete)
    if not l2_outputs:
        raise ValueError(f"小时没有成对的活动 L2 输出: {market_id}/{_iso(start)}")
    trade = catalog.active_outputs(
        market_id, domains=("trade", "trade_realtime"),
        datasets=("trade_observation",), from_time=start, to_time=end,
    )
    outputs = tuple(sorted(
        (*l2_outputs, *trade.outputs),
        key=lambda row: (row.domain, row.partition_key, row.dataset, row.artifact_id),
    ))
    generation_body = json.dumps(
        [(row.dataset, row.artifact_id, row.attempt_id) for row in outputs],
        separators=(",", ":"),
    )
    return ActiveOutputSnapshot(
        market=l2.market, outputs=outputs,
        head_generation="sha256-" + hashlib.sha256(generation_body.encode()).hexdigest(),
    )


def _load_frames(
    snapshot: ActiveOutputSnapshot, start: datetime, end: datetime,
) -> list[Frame]:
    frame_files = _paths(snapshot, "book_l2_frame")
    level_files = _paths(snapshot, "book_l2_level")
    if not frame_files or not level_files:
        raise ValueError("tile 输入缺少 L2 frame/level")
    db: Any = duckdb.connect(":memory:")
    db.execute("SET TimeZone='UTC'")
    try:
        frame_columns = _columns(db, frame_files)
        segment_expr = "segment_sequence" if "segment_sequence" in frame_columns else "0"
        run_expr = wire_session_sql(frame_columns, segment_expr)
        sequence_expr = "sequence_id" if "sequence_id" in frame_columns else "NULL"
        venue_id = str(snapshot.market.get("venue_id"))
        window_clock = "available_time" if venue_id == "bitbank" else "event_time"
        live_coverage = okx_live_event_coverage(venue_id, snapshot.outputs)
        precedence_sql = ""
        precedence_params: list[object] = []
        if live_coverage:
            if "normalization_version" not in frame_columns:
                raise ValueError("OKX live 输出缺少 normalization_version")
            coverage_sql = " OR ".join(
                "event_time BETWEEN ? AND ?" for _ in live_coverage
            )
            precedence_sql = (
                " AND NOT (normalization_version=? AND (" + coverage_sql + "))"
            )
            precedence_params.append(BOOK_L2_NORMALIZATION_VERSION)
            for coverage in live_coverage:
                precedence_params.extend((coverage.start, coverage.end))
        db.execute(
            f"""
            CREATE TEMP TABLE selected_frame AS
            WITH deduped AS (
              SELECT frame_id,message_kind,event_time,available_time,ingest_time,
                     integrity_mode,{run_expr} AS source_run,
                     {sequence_expr} AS sequence_id,
                     {segment_expr} AS source_segment,
                     source_row_index,row_number() OVER (
                       PARTITION BY frame_id ORDER BY available_time,ingest_time
                     ) AS selected
              FROM read_parquet(?, union_by_name=true)
              WHERE market_id=? AND {window_clock}<? AND available_time<=?
                    {precedence_sql}
            ), ordered AS (
              SELECT * EXCLUDE(selected),min(ingest_time) OVER (
                PARTITION BY source_run
              ) AS session_started
              FROM deduped WHERE selected=1
            )
            SELECT * EXCLUDE(session_started),row_number() OVER (
              ORDER BY session_started,source_run,source_segment,
                       source_row_index,ingest_time,frame_id
            ) AS ordinal
            FROM ordered
            """,
            [
                frame_files, snapshot.market["market_id"], end,
                datetime.now(UTC), *precedence_params,
            ],
        )
        if venue_id == "bitbank":
            anchor_ordinal = db.execute(
                "SELECT min(ordinal) FROM selected_frame "
                "HAVING count(*) FILTER (WHERE message_kind='snapshot')>0"
            ).fetchone()
        else:
            anchor_ordinal = db.execute(
                "SELECT coalesce("
                "max(ordinal) FILTER (WHERE message_kind='snapshot' AND event_time<=?),"
                "min(ordinal) FILTER (WHERE message_kind='snapshot')) "
                "FROM selected_frame",
                [start],
            ).fetchone()
        if anchor_ordinal is None or anchor_ordinal[0] is None:
            raise ValueError("tile 小时内及其前置窗口没有 snapshot 锚点")
        db.execute(
            "CREATE TEMP TABLE replay_frame AS "
            "SELECT * FROM selected_frame WHERE ordinal>=?",
            [int(anchor_ordinal[0])],
        )
        frame_rows = db.execute(
            "SELECT frame_id,message_kind,event_time,available_time,integrity_mode,"
            "source_run,sequence_id,source_segment,source_row_index,ordinal "
            "FROM replay_frame ORDER BY ordinal"
        ).fetchall()
        level_rows = db.execute(
            "SELECT f.ordinal,l.frame_id,l.side,l.price,l.size,l.action,l.source_level_index "
            "FROM replay_frame f JOIN read_parquet(?, union_by_name=true) l "
            "ON l.frame_id=f.frame_id "
            "ORDER BY f.ordinal,l.side,l.source_level_index",
            [level_files],
        ).fetchall()
    finally:
        db.close()
    by_ordinal: dict[int, list[Level]] = defaultdict(list)
    for ordinal, _frame_id, side, price, size, action, _index in level_rows:
        by_ordinal[int(ordinal)].append(Level(
            str(side), Decimal(str(price)), Decimal(str(size)), str(action),
        ))
    return [
        Frame(
            str(row[0]), str(row[1]), row[2], row[3], str(row[4]),
            "" if row[5] is None else str(row[5]),
            tuple(by_ordinal[int(row[9])]),
            None if row[6] is None else int(str(row[6])),
            int(row[7]), int(row[8]),
        )
        for row in frame_rows
    ]


def _load_trades(
    snapshot: ActiveOutputSnapshot, start: datetime, end: datetime,
    bucket_seconds: int, row_size: Decimal,
) -> dict[tuple[int, str, int], tuple[Decimal, int]]:
    files = _paths(snapshot, "trade_observation")
    if not files:
        return {}
    db: Any = duckdb.connect(":memory:")
    db.execute("SET TimeZone='UTC'")
    try:
        rows = db.execute(
            """
            WITH accepted AS (
              SELECT *,row_number() OVER (
                PARTITION BY observation_id ORDER BY available_time,ingest_time
              ) AS selected
              FROM read_parquet(?, union_by_name=true)
              WHERE market_id=? AND event_time>=? AND event_time<?
                AND available_time<=?
            )
            SELECT (floor(epoch(event_time)/?)*?)::BIGINT AS bucket_epoch,side,
                   floor(try_cast(price AS DECIMAL(38,12))/?::DECIMAL(38,12))::BIGINT,
                   SUM(try_cast(size AS DECIMAL(38,12))),COUNT(*)
            FROM accepted
            WHERE selected=1 AND side IN ('buy','sell')
              AND source_side_basis LIKE 'taker%'
            GROUP BY 1,2,3 ORDER BY 1,2,3
            """,
            [files, snapshot.market["market_id"], start, end, datetime.now(UTC),
             bucket_seconds, bucket_seconds, str(row_size)],
        ).fetchall()
    finally:
        db.close()
    return {
        (int(bucket), str(side), int(key)): (Decimal(str(size)), int(count))
        for bucket, side, key, size, count in rows
    }


def _price_key(price: Decimal, row_size: Decimal) -> int:
    return int((price / row_size).to_integral_value(rounding=ROUND_FLOOR))


def _price_quantum(
    market: dict[str, Any], frames: Sequence[Frame],
) -> tuple[Decimal, str]:
    recorded = market.get("tick_size")
    if recorded not in (None, ""):
        tick = Decimal(str(recorded))
        if tick > 0:
            return tick, "instrument_map_tick_size"
    exponents: list[int] = [
        int(level.price.normalize().as_tuple().exponent)
        for frame in frames for level in frame.levels
        if level.price > 0
    ]
    if not exponents:
        raise ValueError(f"市场缺少价格量子证据: {market['market_id']}")
    return Decimal(1).scaleb(min(exponents)), "observed_decimal_quantum"


def _column_id(market_id: str, bucket: str, epoch: int) -> str:
    return hashlib.sha256(f"{market_id}|{bucket}|{epoch}|{METHOD_VERSION}".encode()).hexdigest()


def build_tiles(
    market: dict[str, Any], frames: Sequence[Frame],
    trades: dict[tuple[int, str, int], tuple[Decimal, int]],
    start: datetime, end: datetime, bucket: str,
    source_generation: str,
) -> TileBuild:
    """以周期锚点加稀疏变化构造一小时 tile；不伪造撤单归因。"""
    bucket_seconds = BUCKET_SECONDS[bucket]
    use_available_buckets = str(market.get("venue_id")) == "bitbank"
    if use_available_buckets:
        replayed: list[Frame] = []
        for action in bitbank_replay_actions(
            frames,
            message_kind=lambda frame: frame.message_kind,
            sequence_id=lambda frame: frame.sequence_id,
            session_id=lambda frame: frame.source_run,
            available_time=lambda frame: frame.available_time,
        ):
            if not action.apply_levels:
                if action.session_changed:
                    replayed.append(Frame(
                        action.frame.frame_id,
                        "delta",
                        action.frame.available_time,
                        action.frame.available_time,
                        action.frame.integrity_mode,
                        action.frame.source_run,
                        (),
                        action.frame.sequence_id,
                        action.frame.wire_segment,
                        action.frame.wire_row,
                        False,
                    ))
                continue
            replayed.append(Frame(
                action.frame.frame_id,
                "snapshot" if action.reset_book else action.frame.message_kind,
                action.effective_available_time,
                action.effective_available_time,
                action.frame.integrity_mode,
                action.frame.source_run,
                action.frame.levels,
                action.frame.sequence_id,
                action.frame.wire_segment,
                action.frame.wire_row,
                action.attribute_changes,
            ))
        frames = tuple(replayed)
    quantum, quantum_basis = _price_quantum(market, frames)
    row_size = quantum * ROW_TICK_TIERS[bucket]
    start_epoch = int(start.timestamp())
    end_epoch = int(end.timestamp())
    books: dict[str, dict[Decimal, Decimal]] = {"ask": {}, "bid": {}}
    bins: dict[tuple[str, int], Decimal] = defaultdict(Decimal)
    frame_index = 0
    last_frame_time: datetime | None = None
    last_available: datetime | None = None
    last_source_run: str | None = None
    integrity_mode = "unknown"
    trusted = False
    anchor_pending = False

    def apply(
        frame: Frame, changes: dict[tuple[str, int], list[Decimal]] | None,
    ) -> tuple[bool, bool, bool]:
        nonlocal last_frame_time, last_available, last_source_run
        nonlocal integrity_mode, trusted
        reset = frame.message_kind == "snapshot"
        adjacent_gap = False
        if not reset and last_frame_time is not None:
            event_gap = (frame.event_time - last_frame_time).total_seconds()
            available_gap = (
                None if last_available is None else
                (frame.available_time - last_available).total_seconds()
            )
            source_changed = (
                last_source_run is not None and frame.source_run != last_source_run
            )
            adjacent_gap = (
                event_gap > CARRY_LIMIT_SECONDS or
                (available_gap is not None and available_gap > CARRY_LIMIT_SECONDS) or
                source_changed
            )
            if adjacent_gap:
                trusted = False
        reanchor = reset and not trusted
        previous_bins = dict(bins) if reset and changes is not None else {}
        if reset:
            books["ask"].clear()
            books["bid"].clear()
            bins.clear()
            trusted = True
        for level in frame.levels:
            if level.side not in books:
                continue
            side_book = books[level.side]
            old = side_book.get(level.price, ZERO)
            new = ZERO if level.action == "delete" or level.size == 0 else level.size
            key = (level.side, _price_key(level.price, row_size))
            if old:
                bins[key] -= old
            if new:
                bins[key] += new
                side_book[level.price] = new
            else:
                side_book.pop(level.price, None)
            if bins[key] == 0:
                bins.pop(key, None)
            if (
                changes is not None and not reset and trusted
                and frame.attribute_changes
            ):
                change = changes.setdefault(key, [ZERO, ZERO])
                difference = new - old
                if difference > 0:
                    change[0] += difference
                elif difference < 0:
                    change[1] -= difference
        last_frame_time = frame.event_time
        last_available = frame.available_time
        last_source_run = frame.source_run
        integrity_mode = frame.integrity_mode
        if reset and changes is not None and not reanchor:
            for key in previous_bins.keys() | bins.keys():
                if previous_bins.get(key, ZERO) != bins.get(key, ZERO):
                    changes.setdefault(key, [ZERO, ZERO])
        return reset, reanchor, adjacent_gap

    while frame_index < len(frames) and frames[frame_index].event_time < start:
        apply(frames[frame_index], None)
        frame_index += 1

    columns: list[tuple[object, ...]] = []
    cells: list[tuple[object, ...]] = []
    for column_index, epoch in enumerate(range(start_epoch, end_epoch, bucket_seconds)):
        bucket_start = datetime.fromtimestamp(epoch, UTC)
        bucket_end = datetime.fromtimestamp(min(epoch + bucket_seconds, end_epoch), UTC)
        changes: dict[tuple[str, int], list[Decimal]] = {}
        frame_count = 0
        reset = False
        reanchor = False
        adjacent_gap = False
        while frame_index < len(frames) and (
            frames[frame_index].available_time if use_available_buckets
            else frames[frame_index].event_time
        ) < bucket_end:
            frame_reset, frame_reanchor, frame_gap = apply(
                frames[frame_index], changes,
            )
            reset = frame_reset or reset
            reanchor = frame_reanchor or reanchor
            adjacent_gap = frame_gap or adjacent_gap
            frame_index += 1
            frame_count += 1
        age = None if last_frame_time is None else (bucket_end - last_frame_time).total_seconds()
        stale_gap = age is None or age > CARRY_LIMIT_SECONDS
        if stale_gap:
            trusted = False
        gap = adjacent_gap or stale_gap or not trusted
        carried = frame_count == 0 and not gap and trusted
        anchor = trusted and not gap and (
            column_index % ANCHOR_COLUMNS == 0 or reanchor or anchor_pending
        )
        trade_keys = {
            ("ask" if side == "buy" else "bid", key)
            for trade_epoch, side, key in trades if trade_epoch == epoch
        }
        selected_keys = (set(bins) | set(changes)) if anchor else set(changes)
        selected_keys |= trade_keys
        column_id = _column_id(str(market["market_id"]), bucket, epoch)
        trade_count = sum(
            count for (trade_epoch, _side, _key), (_size, count) in trades.items()
            if trade_epoch == epoch
        )
        state = "gap" if gap else "reset" if reset else "carried" if carried else "ok"
        columns.append((
            column_id, market["market_id"], market["venue_id"], bucket,
            format(row_size, "f"), quantum_basis, epoch,
            bucket_start, bucket_end, state, anchor, reset, carried, gap,
            frame_count, trade_count,
            None if last_frame_time is None else last_frame_time,
            None if last_available is None else last_available,
            integrity_mode, source_generation, METHOD_VERSION, SCHEMA_VERSION,
        ))
        for side, key in sorted(selected_keys):
            change = changes.get((side, key), [ZERO, ZERO])
            buy = trades.get((epoch, "buy", key), (ZERO, 0))[0] if side == "ask" else ZERO
            sell = trades.get((epoch, "sell", key), (ZERO, 0))[0] if side == "bid" else ZERO
            end_size = bins.get((side, key)) if trusted and not gap else None
            role = "anchor" if anchor and (side, key) in bins else "change"
            if role != "anchor" and (side, key) not in changes:
                role = "trade"
            elif reset and change == [ZERO, ZERO] and role != "anchor":
                role = "reset"
            cell_id = hashlib.sha256(
                f"{column_id}|{side}|{key}|{METHOD_VERSION}".encode()
            ).hexdigest()
            cells.append((
                cell_id, column_id, market["market_id"], bucket,
                format(row_size, "f"), quantum_basis, epoch, side, key,
                format(Decimal(key) * row_size, "f"),
                None if end_size is None else format(end_size, "f"),
                format(change[0], "f"), format(change[1], "f"),
                format(buy, "f"), format(sell, "f"), role,
                METHOD_VERSION, SCHEMA_VERSION,
            ))
        # 桶内断流仍标缺口。
        # 下一干净桶完整重锚。
        anchor_pending = gap and reset and trusted
    if not columns:
        raise ValueError("tile 窗口没有完整时间桶")
    return TileBuild(tuple(columns), tuple(cells), start, end)


def _write_outputs(
    root: Path, snapshot: ActiveOutputSnapshot, build: TileBuild,
    start: datetime, bucket: str, attempt_id: str,
) -> list[tuple[str, Path, str, int]]:
    output_dir = (
        root / "materialized" / "orderflow_tile"
        / f"schema_version={SCHEMA_VERSION}"
        / f"method_version={METHOD_VERSION}"
        / f"venue_id={snapshot.market['venue_id']}"
        / f"market_id={snapshot.market['market_id']}"
        / f"event_day={start:%Y-%m-%d}" / f"event_hour={start:%H}"
        / f"bucket={bucket}"
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    db: Any = duckdb.connect(":memory:")
    db.execute("SET TimeZone='UTC'")
    try:
        db.execute("""
          CREATE TABLE tile_column (
            column_id VARCHAR,market_id VARCHAR,venue_id VARCHAR,bucket VARCHAR,
            row_size VARCHAR,price_quantum_basis VARCHAR,
            bucket_epoch BIGINT,bucket_start TIMESTAMPTZ,bucket_end TIMESTAMPTZ,
            coverage_state VARCHAR,is_anchor BOOLEAN,is_reset BOOLEAN,
            is_carried BOOLEAN,is_gap BOOLEAN,frame_count INTEGER,trade_count INTEGER,
            last_event_time TIMESTAMPTZ,last_available_time TIMESTAMPTZ,
            integrity_mode VARCHAR,source_generation VARCHAR,method_version VARCHAR,
            schema_version INTEGER
          )
        """)
        db.execute("""
          CREATE TABLE tile_cell (
            cell_id VARCHAR,column_id VARCHAR,market_id VARCHAR,bucket VARCHAR,
            row_size VARCHAR,price_quantum_basis VARCHAR,
            bucket_epoch BIGINT,book_side VARCHAR,price_key BIGINT,price VARCHAR,
            book_end_size VARCHAR,net_increase VARCHAR,net_decrease_unknown VARCHAR,
            taker_buy_size VARCHAR,taker_sell_size VARCHAR,state_role VARCHAR,
            method_version VARCHAR,schema_version INTEGER
          )
        """)
        for table, rows in (("tile_column", build.columns), ("tile_cell", build.cells)):
            # 空数据保留声明表。
            # 输出合法零行制品。
            if not rows:
                continue
            staging = output_dir / f".{attempt_id}.{table}.csv"
            with staging.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle, lineterminator="\n")
                writer.writerows(
                    tuple("\\N" if value is None else value for value in row)
                    for row in rows
                )
                handle.flush()
                os.fsync(handle.fileno())
            escaped_staging = staging.as_posix().replace("'", "''")
            db.execute(
                f"COPY {table} FROM '{escaped_staging}' "
                "(FORMAT CSV,HEADER false,NULL '\\N')"
            )
            staging.unlink()
        outputs: list[tuple[str, Path, str, int]] = []
        for dataset, table, order, row_count in (
            (DATASET_COLUMN, "tile_column", "bucket_epoch", len(build.columns)),
            (DATASET_CELL, "tile_cell", "bucket_epoch,book_side,price_key", len(build.cells)),
        ):
            temp = output_dir / f".{attempt_id}.{dataset}.tmp.parquet"
            escaped = temp.as_posix().replace("'", "''")
            db.execute(
                f"COPY (SELECT * FROM {table} ORDER BY {order}) TO '{escaped}' "
                "(FORMAT PARQUET,COMPRESSION ZSTD)"
            )
            with temp.open("rb+") as handle:
                handle.flush()
                os.fsync(handle.fileno())
            sha = sha256_file(temp)
            final = output_dir / f"{dataset}-{sha[:12]}.parquet"
            if final.exists():
                if sha256_file(final) != sha:
                    raise ValueError(f"tile 输出散列冲突: {final}")
                temp.unlink()
            else:
                os.replace(temp, final)
            outputs.append((dataset, final, sha, row_count))
    finally:
        db.close()
    return outputs


def _input_hash(
    snapshot: ActiveOutputSnapshot, start: datetime, end: datetime, bucket: str,
) -> str:
    body = json.dumps({
        "market_id": snapshot.market["market_id"], "hour": _iso(start),
        "bucket": bucket, "coverage_to": _iso(end),
        "method_version": METHOD_VERSION,
        "inputs": sorted((row.dataset, row.artifact_id, row.attempt_id)
                         for row in snapshot.outputs),
    }, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(body.encode()).hexdigest()


def materialize_hour(
    root: Path, conn: sqlite3.Connection, market_id: str,
    start: datetime, bucket: str = "5s",
) -> TileResult:
    if bucket not in BUCKET_SECONDS:
        raise ValueError(f"不支持的 tile bucket: {bucket}")
    start = _hour(start)
    hour_end = start + timedelta(hours=1)
    now = datetime.now(UTC)
    if hour_end <= now:
        end = hour_end
    else:
        bucket_seconds = BUCKET_SECONDS[bucket]
        visible_epoch = int(now.timestamp()) // bucket_seconds * bucket_seconds
        end = datetime.fromtimestamp(
            max(int(start.timestamp()) + bucket_seconds, visible_epoch), UTC,
        )
        end = min(end, hour_end)
    snapshot = _source_snapshot(root, market_id, start, end)
    print(
        f"[SOURCE] {market_id} {start:%Y-%m-%dT%H}:00Z {bucket} "
        f"artifacts={len(snapshot.outputs)} attempts="
        f"{len({row.attempt_id for row in snapshot.outputs})}", flush=True,
    )
    input_hash = _input_hash(snapshot, start, end, bucket)
    partition_key = f"{start:%Y-%m-%dT%H}/{bucket}"
    completed = conn.execute(
        "SELECT attempt_id FROM partition_attempt WHERE market_id=? AND domain=? "
        "AND partition_key=? AND normalization_version=? AND input_set_hash=? "
        "AND status='complete' ORDER BY finished_at DESC LIMIT 1",
        (market_id, DOMAIN, partition_key, METHOD_VERSION, input_hash),
    ).fetchone()
    if completed is not None:
        counts = dict(conn.execute(
            "SELECT dataset,row_count FROM materialization_output WHERE attempt_id=?",
            (str(completed[0]),),
        ).fetchall())
        return TileResult(str(completed[0]), market_id, partition_key,
                          int(counts.get(DATASET_COLUMN, 0)),
                          int(counts.get(DATASET_CELL, 0)), True)
    frames = _load_frames(snapshot, start, end)
    print(f"[FRAME] {market_id} frames={len(frames):,}", flush=True)
    quantum, _quantum_basis = _price_quantum(snapshot.market, frames)
    row_size = quantum * ROW_TICK_TIERS[bucket]
    trades = _load_trades(snapshot, start, end, BUCKET_SECONDS[bucket], row_size)
    print(
        f"[TRADE] {market_id} grouped_cells={len(trades):,}", flush=True,
    )
    build = build_tiles(snapshot.market, frames, trades, start, end, bucket,
                        snapshot.head_generation)
    print(
        f"[BUILD] {market_id} columns={len(build.columns):,} "
        f"cells={len(build.cells):,}", flush=True,
    )
    attempt_id = "orderflow-tile-" + uuid.uuid4().hex
    started = utc_now()
    source_rows = sum(row.row_count for row in snapshot.outputs)
    conn.execute(
        "INSERT INTO partition_attempt (attempt_id,market_id,domain,partition_key,"
        "normalization_version,input_set_hash,status,source_rows,normalized_rows,"
        "ignored_rows,rejected_rows,started_at,code_version,config_hash) "
        "VALUES (?,?,?,?,?,?,'running',?,0,0,0,?,'working-tree',?)",
        (attempt_id, market_id, DOMAIN, partition_key, METHOD_VERSION, input_hash,
         source_rows, started, hashlib.sha256(METHOD_VERSION.encode()).hexdigest()),
    )
    upstream_attempts = sorted({row.attempt_id for row in snapshot.outputs})
    conn.executemany(
        "INSERT INTO materialization_dependency VALUES (?,?,?,?)",
        [(attempt_id, upstream, "active-head", started) for upstream in upstream_attempts],
    )
    try:
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    outputs: list[tuple[str, Path, str, int]] = []
    manifest: Path | None = None
    manifest_body: dict[str, object] | None = None
    try:
        outputs = _write_outputs(root, snapshot, build, start, bucket, attempt_id)
        finished = utc_now()
        manifest = outputs[0][1].parent / f"manifest-{attempt_id}.json"
        manifest_body = {
            "attempt_id": attempt_id, "status": "complete", "market_id": market_id,
            "partition_key": partition_key, "method_version": METHOD_VERSION,
            "upstream_attempt_ids": upstream_attempts,
            "input_artifact_ids": sorted(row.artifact_id for row in snapshot.outputs),
            "column_rows": len(build.columns), "cell_rows": len(build.cells),
            "coverage_from": _iso(build.coverage_from),
            "coverage_to": _iso(build.coverage_to),
        }
        atomic_write_text(
            manifest,
            json.dumps(manifest_body, ensure_ascii=False, indent=2) + "\n",
        )
        conn.execute("BEGIN IMMEDIATE")
        for dataset, path, sha, rows in outputs:
            storage = _relative_storage_path(root, path)
            identity = artifact_id(sha)
            _register_content_artifact(
                conn, identity, "materialized_parquet", storage, sha,
                path.stat().st_size, finished, SCHEMA_VERSION,
            )
            conn.execute(
                "INSERT INTO materialization_output VALUES (?,?,?,?,?,?,?)",
                (attempt_id, identity, dataset, rows, _iso(build.coverage_from),
                 _iso(build.coverage_to), finished),
            )
        register_materialization_manifest(
            root, conn, manifest, SCHEMA_VERSION, finished,
        )
        artifact_totals: dict[str, int] = defaultdict(int)
        location_totals: dict[tuple[str, str], int] = defaultdict(int)
        for source in snapshot.outputs:
            storage = _relative_storage_path(root, source.path)
            artifact_totals[source.artifact_id] += source.row_count
            location_totals[(source.artifact_id, storage)] += source.row_count
        for source_artifact_id, rows in artifact_totals.items():
            conn.execute(
                "INSERT INTO partition_input "
                "(attempt_id,artifact_id,source_rows,normalized_rows,"
                "ignored_rows,rejected_rows) VALUES (?,?,?,?,?,?)",
                (attempt_id, source_artifact_id, rows, rows, 0, 0),
            )
        for (source_artifact_id, storage), rows in location_totals.items():
            conn.execute(
                "INSERT INTO partition_input_binding "
                "(attempt_id,artifact_id,storage_path,source_rows,"
                "normalized_rows,ignored_rows,rejected_rows) "
                "VALUES (?,?,?,?,?,?,?)",
                (attempt_id, source_artifact_id, storage, rows, rows, 0, 0),
            )
        normalized = len(build.columns) + len(build.cells)
        conn.execute(
            "UPDATE partition_attempt SET status='complete',normalized_rows=?,"
            "finished_at=? WHERE attempt_id=?", (normalized, finished, attempt_id),
        )
        conn.execute(
            "INSERT INTO materialization_partition_head VALUES (?,?,?,?,?,?) "
            "ON CONFLICT(market_id,domain,partition_key) DO UPDATE SET "
            "normalization_version=excluded.normalization_version,"
            "attempt_id=excluded.attempt_id,activated_at=excluded.activated_at",
            (market_id, DOMAIN, partition_key, METHOD_VERSION, attempt_id, finished),
        )
        conn.commit()
    except Exception as exc:
        conn.rollback()
        if outputs and manifest is not None and manifest_body is not None:
            failed = settle_failed_publication(
                root,
                conn,
                attempt_id,
                manifest,
                manifest_body,
                tuple(
                    UnpromotedOutput(
                        dataset, path, sha, rows, SCHEMA_VERSION,
                    )
                    for dataset, path, sha, rows in outputs
                ),
                exc,
                SCHEMA_VERSION,
            )
            if not failed:
                return TileResult(
                    attempt_id, market_id, partition_key,
                    len(build.columns), len(build.cells), False,
                )
            raise
        conn.execute(
            "UPDATE partition_attempt SET status='failed',finished_at=?,failure_detail=? "
            "WHERE attempt_id=? AND status='running'",
            (utc_now(), str(exc)[:2000], attempt_id),
        )
        conn.commit()
        raise
    return TileResult(attempt_id, market_id, partition_key,
                      len(build.columns), len(build.cells), False)


def audit(root: Path, conn: sqlite3.Connection) -> dict[str, object]:
    rows = conn.execute(
        "SELECT h.market_id,h.partition_key,h.attempt_id,o.dataset,o.row_count,"
        "a.storage_path,a.sha256 FROM materialization_partition_head h "
        "JOIN materialization_output o ON o.attempt_id=h.attempt_id "
        "JOIN artifact a ON a.artifact_id=o.artifact_id WHERE h.domain=? "
        "ORDER BY h.market_id,h.partition_key,o.dataset", (DOMAIN,),
    ).fetchall()
    errors: list[str] = []
    db: Any = duckdb.connect(":memory:")
    try:
        for market, partition, attempt, dataset, expected, storage, sha in rows:
            path = root / str(storage)
            if not path.is_file() or sha256_file(path) != str(sha):
                errors.append(f"tile 缺失或散列错误: {attempt}/{dataset}")
                continue
            count = int(db.execute("SELECT COUNT(*) FROM read_parquet(?)", [str(path)]).fetchone()[0])
            if count != int(expected):
                errors.append(f"tile 行数错误: {attempt}/{dataset}")
            dependencies = conn.execute(
                "SELECT COUNT(*) FROM materialization_dependency WHERE attempt_id=?",
                (attempt,),
            ).fetchone()[0]
            if not dependencies:
                errors.append(f"tile 缺少上游依赖: {attempt}")
    finally:
        db.close()
    return {"outputs": len(rows), "errors": errors, "ok": not errors}


def recent_l2_markets(
    conn: sqlite3.Connection, now: datetime,
) -> list[str]:
    """只选仍在两小时活动窗内推进的 L2 市场。"""
    rows = conn.execute(
        "SELECT h.market_id,MAX(o.max_event_time) "
        "FROM materialization_partition_head h "
        "JOIN materialization_output o ON o.attempt_id=h.attempt_id "
        "AND o.dataset='book_l2_frame' WHERE h.domain='book_l2' "
        "GROUP BY h.market_id ORDER BY h.market_id"
    ).fetchall()
    threshold = now.astimezone(UTC) - timedelta(hours=2)
    selected: list[str] = []
    for market_id, stamp in rows:
        if stamp is None:
            continue
        parsed = datetime.fromisoformat(str(stamp).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        if parsed.astimezone(UTC) >= threshold:
            selected.append(str(market_id))
    return selected


def _watch(root: Path, bucket: str, poll_seconds: float) -> int:
    """持续物化活动小时；启动锁竞争只延后本轮。"""
    def report_connect_error(exc: Exception, elapsed: float) -> None:
        print(json.dumps({
            "event": "orderflow_tile_startup_error",
            "error": f"{type(exc).__name__}: {exc}",
            "elapsed_seconds": round(elapsed, 3),
            "retry_seconds": poll_seconds,
        }, ensure_ascii=False), flush=True)

    conn: sqlite3.Connection | None = None
    try:
        conn = connect_with_retry(
            root,
            retry_seconds=poll_seconds,
            connector=connect,
            report_error=report_connect_error,
        )
        while True:
            cycle_started = time.monotonic()
            created = reused = failed = 0
            try:
                cycle_now = datetime.now(UTC)
                # 活动市场在短锁内冻结。
                # 后续小时任务不合并。
                with sqlite_writer_lock(root):
                    markets = recent_l2_markets(conn, cycle_now)
                hours = (
                    _hour(cycle_now) - timedelta(hours=1),
                    _hour(cycle_now),
                )
                for market_id in markets:
                    for hour_start in hours:
                        try:
                            # 每小时独立取得短锁。
                            # 小时之间释放锁。
                            # 避免阻塞实时登记。
                            with sqlite_writer_lock(root):
                                result = materialize_hour(
                                    root, conn, market_id, hour_start, bucket,
                                )
                            reused += int(result.reused)
                            created += int(not result.reused)
                        except TimeoutError:
                            # 文件锁竞争按周期退避。
                            # 未执行任务不记作失败。
                            raise
                        except (
                            OSError, sqlite3.Error, ValueError, duckdb.Error,
                        ) as exc:
                            failed += 1
                            print(json.dumps({
                                "event": "orderflow_tile_task_error",
                                "market_id": market_id,
                                "hour": _iso(hour_start),
                                "error": f"{type(exc).__name__}: {exc}",
                            }, ensure_ascii=False), flush=True)
                print(json.dumps({
                    "event": "orderflow_tile_cycle",
                    "markets": len(markets),
                    "hours_per_market": 2,
                    "created": created,
                    "reused": reused,
                    "failed": failed,
                    "elapsed_seconds": round(
                        time.monotonic() - cycle_started, 3
                    ),
                }, ensure_ascii=False), flush=True)
            except (
                TimeoutError, OSError, sqlite3.Error, ValueError, duckdb.Error,
            ) as exc:
                print(json.dumps({
                    "event": "orderflow_tile_cycle_error",
                    "error": f"{type(exc).__name__}: {exc}",
                    "elapsed_seconds": round(
                        time.monotonic() - cycle_started, 3
                    ),
                }, ensure_ascii=False), flush=True)
            time.sleep(poll_seconds)
    except KeyboardInterrupt:
        print("OFL tile watcher 已停止", flush=True)
        return 0
    finally:
        if conn is not None:
            conn.close()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="market-scoped OFL tile 小时物化")
    parser.add_argument("--data-root", type=Path)
    sub = parser.add_subparsers(dest="command", required=True)
    build = sub.add_parser("hour")
    build.add_argument("--market-id", required=True)
    build.add_argument("--hour", required=True, help="带时区 ISO 时刻")
    build.add_argument("--bucket", choices=tuple(BUCKET_SECONDS), default="5s")
    sub.add_parser("audit")
    watch = sub.add_parser("watch")
    watch.add_argument("--bucket", choices=tuple(BUCKET_SECONDS), default="5s")
    watch.add_argument("--poll-seconds", type=float, default=300.0)
    args = parser.parse_args(argv)
    root = (args.data_root or configured_data_root()).resolve()
    if args.command == "watch":
        if args.poll_seconds < 30:
            raise ValueError("poll-seconds 不得小于 30")
        return _watch(root, args.bucket, float(args.poll_seconds))

    conn = connect(root)
    try:
        if args.command == "audit":
            with sqlite_writer_lock(root):
                report = audit(root, conn)
            print(json.dumps(report, ensure_ascii=False, indent=2))
            return 0 if report["ok"] else 1
        if args.command == "hour":
            when = datetime.fromisoformat(args.hour.replace("Z", "+00:00"))
            if when.tzinfo is None:
                raise ValueError("hour 必须带时区")
            with sqlite_writer_lock(root):
                result = materialize_hour(root, conn, args.market_id, when, args.bucket)
            print(json.dumps(result.__dict__, ensure_ascii=False, indent=2))
            return 0
        raise AssertionError(f"未知命令: {args.command}")
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
