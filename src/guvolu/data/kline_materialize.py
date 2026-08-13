"""GMO REST K 线原件逐数组项物化。

核心事实按不同 OHLCV 状态去重；``fact_source_evidence`` 保留每个
``artifact + JSONL line + data[] item`` 的逐项证据，因此重复观察不丢失。
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sqlite3
import uuid
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

from guvolu.data import store
from guvolu.data.durable_io import atomic_write_text
from guvolu.data.materialize import (
    SourceArtifact,
    _input_set_hash,
    _market_row,
    _register_content_artifact,
    _relative_storage_path,
    artifact_id,
    ensure_markets,
    sha256_file,
    utc_now,
)
from guvolu.data.paths import data_root as configured_data_root
from guvolu.venues import registry

KLINE_DATASET = "market_kline"
EVIDENCE_DATASET = "fact_source_evidence"
KLINE_SCHEMA_VERSION = 1
KLINE_NORMALIZATION_VERSION = "gmo-kline-normalization-v1"
PARTITION_KEY = "all-local-history"


@dataclass(frozen=True)
class KlineResult:
    """一个 GMO market 的全本地 K 线物化结果。"""

    attempt_id: str
    market_id: str
    venue_symbol: str
    status: str
    source_items: int
    fact_rows: int
    evidence_rows: int
    conflicting_revisions: int
    provisional_facts: int
    fact_path: str
    evidence_path: str
    reused: bool


def _raw_inputs(root: Path) -> list[SourceArtifact]:
    inputs: list[SourceArtifact] = []
    for path in sorted((root / "raw").glob("*/klines.jsonl")):
        sha = sha256_file(path)
        storage = path.relative_to(root).as_posix()
        inputs.append(SourceArtifact(
            artifact_id=artifact_id(sha), storage_path=storage,
            absolute_path=path, source_rows=0, normalized_rows=0,
            rejected_rows=0,
        ))
    if not inputs:
        raise ValueError("没有 GMO klines.jsonl 原件")
    return inputs


def _register_inputs(conn: sqlite3.Connection, inputs: Sequence[SourceArtifact]) -> None:
    for item in inputs:
        created = datetime.fromtimestamp(item.absolute_path.stat().st_mtime, UTC).isoformat()
        _register_content_artifact(
            conn, item.artifact_id, "raw_jsonl", item.storage_path,
            item.artifact_id.removeprefix("sha256-"),
            item.absolute_path.stat().st_size, created, 1,
        )
    conn.commit()


def _symbols_and_counts(inputs: Sequence[SourceArtifact]) -> Counter[str]:
    counts: Counter[str] = Counter()
    for item in inputs:
        with item.absolute_path.open(encoding="utf-8") as handle:
            for line in handle:
                record = json.loads(line)
                if not isinstance(record, Mapping):
                    continue
                payload = record.get("payload")
                params = record.get("params")
                if not isinstance(payload, Mapping) or not isinstance(params, Mapping):
                    continue
                data = payload.get("data")
                if (
                    record.get("http_status") == 200
                    and payload.get("status") == 0
                    and isinstance(data, list)
                ):
                    counts[str(params.get("symbol", ""))] += len(data)
    counts.pop("", None)
    return counts


def _completed(
    conn: sqlite3.Connection, market_id: str, input_hash: str
) -> KlineResult | None:
    row = conn.execute(
        "SELECT a.attempt_id,a.status,a.normalized_rows,"
        "MAX(CASE WHEN o.dataset=? THEN o.row_count END),"
        "MAX(CASE WHEN o.dataset=? THEN o.row_count END),"
        "MAX(CASE WHEN o.dataset=? THEN r.storage_path END),"
        "MAX(CASE WHEN o.dataset=? THEN r.storage_path END) "
        "FROM partition_attempt a JOIN materialization_output o "
        "ON o.attempt_id=a.attempt_id JOIN artifact r ON r.artifact_id=o.artifact_id "
        "WHERE a.market_id=? AND a.domain='kline' AND a.partition_key=? "
        "AND a.normalization_version=? AND a.input_set_hash=? "
        "AND a.status IN ('complete','complete_with_rejections') "
        "GROUP BY a.attempt_id,a.status,a.normalized_rows LIMIT 1",
        (KLINE_DATASET, EVIDENCE_DATASET, KLINE_DATASET, EVIDENCE_DATASET,
         market_id, PARTITION_KEY, KLINE_NORMALIZATION_VERSION, input_hash),
    ).fetchone()
    if row is None or not row[5] or not row[6]:
        return None
    details = conn.execute(
        "SELECT failure_detail FROM partition_attempt WHERE attempt_id=?",
        (str(row[0]),),
    ).fetchone()
    metadata: dict[str, object] = {}
    if details and details[0]:
        try:
            loaded = json.loads(str(details[0]))
            if isinstance(loaded, dict): metadata = loaded
        except json.JSONDecodeError:
            pass
    symbol_row = conn.execute(
        "SELECT venue_symbol FROM market WHERE market_id=?", (market_id,)
    ).fetchone()
    return KlineResult(
        str(row[0]), market_id, str(symbol_row[0]), str(row[1]),
        int(row[2]), int(row[3]), int(row[4]),
        int(str(metadata.get("conflicting_revisions", 0))),
        int(str(metadata.get("provisional_facts", 0))),
        str(row[5]), str(row[6]), True,
    )


def _write_stage(
    inputs: Sequence[SourceArtifact], stage_path: Path,
) -> tuple[int, dict[tuple[str, str], int]]:
    """单次顺序扫描原件，精确保留 JSONL 行与数组项索引。"""
    total = 0
    by_artifact_symbol: Counter[tuple[str, str]] = Counter()
    with stage_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        for item in inputs:
            with item.absolute_path.open(encoding="utf-8") as source:
                for line_index, line in enumerate(source, start=1):
                    record = json.loads(line)
                    if not isinstance(record, Mapping):
                        continue
                    payload = record.get("payload")
                    params = record.get("params")
                    if not isinstance(payload, Mapping) or not isinstance(params, Mapping):
                        continue
                    data = payload.get("data")
                    if (
                        record.get("http_status") != 200
                        or payload.get("status") != 0
                        or not isinstance(data, list)
                    ):
                        continue
                    symbol = str(params.get("symbol", ""))
                    interval = str(params.get("interval", ""))
                    ingest = str(record.get("ingest_time", ""))
                    if not symbol or not interval or not ingest:
                        raise ValueError(
                            f"K 线成功响应缺请求身份: {item.storage_path}:{line_index}"
                        )
                    for item_index, bar in enumerate(data):
                        if not isinstance(bar, Mapping):
                            raise ValueError(
                                f"K 线数组项非对象: {item.storage_path}:"
                                f"{line_index}:{item_index}"
                            )
                        writer.writerow((
                            item.artifact_id, item.storage_path, line_index,
                            item_index, symbol, interval, ingest,
                            str(bar["openTime"]), str(bar["open"]),
                            str(bar["high"]), str(bar["low"]),
                            str(bar["close"]), str(bar["volume"]),
                        ))
                        total += 1
                        by_artifact_symbol[(item.artifact_id, symbol)] += 1
        handle.flush(); os.fsync(handle.fileno())
    return total, dict(by_artifact_symbol)


def _load_stage(db: Any, stage_path: Path) -> None:
    db.execute("""
        CREATE TABLE raw_kline_observation (
          source_artifact_id VARCHAR, source_storage_path VARCHAR,
          source_line_index BIGINT, source_item_index INTEGER,
          venue_symbol VARCHAR, kline_interval VARCHAR,
          ingest_time TIMESTAMPTZ, open_time_ms BIGINT,
          open VARCHAR, high VARCHAR, low VARCHAR, close VARCHAR, volume VARCHAR
        )
    """)
    escaped = stage_path.as_posix().replace("'", "''")
    db.execute(
        f"COPY raw_kline_observation FROM '{escaped}' "
        "(FORMAT CSV, HEADER false)"
    )


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _derived_sql(symbol: str, market_id: str) -> str:
    s = _sql_string(symbol); market = _sql_string(market_id)
    version = _sql_string(KLINE_NORMALIZATION_VERSION)
    return f"""
      WITH source AS (
        SELECT *, CAST(epoch_ms(open_time_ms) AS TIMESTAMPTZ) AS open_time,
          CASE kline_interval
            WHEN '1min' THEN CAST(epoch_ms(open_time_ms) AS TIMESTAMPTZ) + INTERVAL '1 minute'
            WHEN '5min' THEN CAST(epoch_ms(open_time_ms) AS TIMESTAMPTZ) + INTERVAL '5 minutes'
            WHEN '10min' THEN CAST(epoch_ms(open_time_ms) AS TIMESTAMPTZ) + INTERVAL '10 minutes'
            WHEN '15min' THEN CAST(epoch_ms(open_time_ms) AS TIMESTAMPTZ) + INTERVAL '15 minutes'
            WHEN '30min' THEN CAST(epoch_ms(open_time_ms) AS TIMESTAMPTZ) + INTERVAL '30 minutes'
            WHEN '1hour' THEN CAST(epoch_ms(open_time_ms) AS TIMESTAMPTZ) + INTERVAL '1 hour'
            WHEN '4hour' THEN CAST(epoch_ms(open_time_ms) AS TIMESTAMPTZ) + INTERVAL '4 hours'
            WHEN '8hour' THEN CAST(epoch_ms(open_time_ms) AS TIMESTAMPTZ) + INTERVAL '8 hours'
            WHEN '12hour' THEN CAST(epoch_ms(open_time_ms) AS TIMESTAMPTZ) + INTERVAL '12 hours'
            WHEN '1day' THEN CAST(epoch_ms(open_time_ms) AS TIMESTAMPTZ) + INTERVAL '1 day'
            WHEN '1week' THEN CAST(epoch_ms(open_time_ms) AS TIMESTAMPTZ) + INTERVAL '7 days'
            WHEN '1month' THEN date_trunc('month', CAST(epoch_ms(open_time_ms) AS TIMESTAMPTZ)) + INTERVAL '1 month'
            ELSE NULL
          END AS close_time,
          'sha256-' || sha256(concat_ws('|',open,high,low,close,volume)) AS value_revision
        FROM raw_kline_observation WHERE venue_symbol={s}
      ), identified AS (
        SELECT *, 'sha256-' || sha256(concat_ws('|',{market},kline_interval,
          CAST(open_time_ms AS VARCHAR),'venue',value_revision,{version})) AS kline_id
        FROM source WHERE close_time IS NOT NULL
      )
    """


def _fact_query(symbol: str, market_id: str) -> str:
    base = _derived_sql(symbol, market_id)
    return base + f"""
      , grouped AS (
        SELECT kline_id, { _sql_string(market_id) } AS market_id,
          { _sql_string(symbol) } AS venue_symbol, kline_interval AS interval,
          open_time, close_time, 'venue' AS origin, value_revision,
          open, high, low, close, volume, 'base' AS volume_unit,
          MIN(ingest_time) AS available_time,
          MIN(ingest_time) AS first_seen_at, MAX(ingest_time) AS last_seen_at,
          MIN(ingest_time) FILTER (WHERE ingest_time >= close_time) AS closed_available_time,
          COUNT(*) AS evidence_count
        FROM identified GROUP BY kline_id,kline_interval,open_time,close_time,
          value_revision,open,high,low,close,volume
      )
      SELECT *, closed_available_time IS NOT NULL AS is_closed,
        dense_rank() OVER (PARTITION BY market_id,interval,open_time,origin
          ORDER BY first_seen_at,value_revision) AS revision_ordinal,
        'native_sparse' AS gap_policy,
        { _sql_string(KLINE_NORMALIZATION_VERSION) } AS normalization_version,
        {KLINE_SCHEMA_VERSION} AS schema_version
      FROM grouped
    """


def _evidence_query(symbol: str, market_id: str) -> str:
    base = _derived_sql(symbol, market_id)
    return base + f"""
      SELECT 'sha256-' || sha256(concat_ws('|',source_artifact_id,
          CAST(source_line_index AS VARCHAR),CAST(source_item_index AS VARCHAR)))
          AS observation_id,
        kline_id, { _sql_string(market_id) } AS market_id,
        source_artifact_id, source_storage_path, source_line_index,
        source_item_index, ingest_time,
        ingest_time >= close_time AS observed_is_closed,
        { _sql_string(KLINE_NORMALIZATION_VERSION) } AS normalization_version,
        {KLINE_SCHEMA_VERSION} AS schema_version
      FROM identified
    """


def _copy_query(db: Any, query: str, path: Path, order: str) -> tuple[Path, str]:
    temp = path.with_name("." + path.name + ".tmp")
    escaped = temp.as_posix().replace("'", "''")
    db.execute(
        f"COPY (SELECT * FROM ({query}) q ORDER BY {order}) "
        f"TO '{escaped}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880)"
    )
    with temp.open("rb+") as handle:
        handle.flush(); os.fsync(handle.fileno())
    sha = sha256_file(temp)
    final = path.with_name(f"part-{sha[:12]}.parquet")
    if final.exists():
        if sha256_file(final) != sha:
            raise ValueError(f"K 线输出散列名冲突: {final}")
        temp.unlink()
    else:
        os.replace(temp, final)
    return final, sha


def _bind_capability(conn: sqlite3.Connection, attempt_id: str) -> None:
    row = conn.execute(
        "SELECT revision_id FROM venue_capability_revision WHERE venue_id='gmo' "
        "AND domain='kline' AND endpoint='/v1/klines' AND available=1 "
        "AND implementation_status='implemented' ORDER BY revision_id DESC LIMIT 1"
    ).fetchone()
    if row is None:
        raise ValueError("GMO K 线能力未登记为 implemented")
    conn.execute(
        "INSERT INTO partition_capability_binding "
        "(attempt_id,venue_id,domain,endpoint,revision_id,binding_basis,bound_at) "
        "VALUES (?,'gmo','kline','/v1/klines',?,'recorded',?)",
        (attempt_id, int(row[0]), utc_now()),
    )


def _materialize_symbol(
    root: Path, conn: sqlite3.Connection, db: Any,
    inputs: list[SourceArtifact], input_hash: str,
    symbol: str, source_items: int,
    source_counts: Mapping[tuple[str, str], int],
) -> KlineResult:
    market_id, _instrument_id, _mapping_revision = _market_row(
        conn, "gmo", symbol, None
    )
    reused = _completed(conn, market_id, input_hash)
    if reused is not None:
        return reused
    attempt_id = f"kline-{uuid.uuid4().hex}"
    config_hash = hashlib.sha256(json.dumps({
        "dataset": [KLINE_DATASET, EVIDENCE_DATASET],
        "normalization_version": KLINE_NORMALIZATION_VERSION,
        "schema_version": KLINE_SCHEMA_VERSION,
        "source_unit": "jsonl-line-data-item-v1",
    }, sort_keys=True).encode()).hexdigest()
    conn.execute(
        "INSERT INTO partition_attempt "
        "(attempt_id,market_id,domain,partition_key,normalization_version,"
        "input_set_hash,status,source_rows,normalized_rows,ignored_rows,"
        "rejected_rows,started_at,code_version,config_hash) "
        "VALUES (?,?,?,?,?,?,'running',?,0,0,0,?,'working-tree',?)",
        (attempt_id, market_id, "kline", PARTITION_KEY,
         KLINE_NORMALIZATION_VERSION, input_hash, source_items,
         utc_now(), config_hash),
    )
    _bind_capability(conn, attempt_id); conn.commit()
    output_dir = (
        root / "materialized" / "market_kline"
        / f"schema_version={KLINE_SCHEMA_VERSION}"
        / f"normalization_version={KLINE_NORMALIZATION_VERSION}"
        / "venue_id=gmo" / f"market_id={market_id}"
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    fact_query = _fact_query(symbol, market_id)
    evidence_query = _evidence_query(symbol, market_id)
    try:
        metrics = db.execute(
            "SELECT (SELECT COUNT(*) FROM (" + fact_query + ") f),"
            "(SELECT COUNT(*) FROM (" + evidence_query + ") e),"
            "(SELECT COUNT(*) FROM (" + fact_query + ") f WHERE NOT is_closed),"
            "(SELECT SUM(revisions-1) FROM (SELECT COUNT(*) revisions FROM (" +
            fact_query + ") f GROUP BY market_id,interval,open_time,origin) x),"
            "(SELECT SUM(CASE WHEN available_time<open_time OR "
            "(closed_available_time IS NOT NULL AND closed_available_time<close_time) "
            "THEN 1 ELSE 0 END) FROM (" + fact_query + ") f)"
        ).fetchone()
        if metrics is None:
            raise ValueError("GMO K 线统计不可读")
        fact_rows, evidence_rows = int(metrics[0]), int(metrics[1])
        provisional = int(metrics[2]); conflicts = int(metrics[3] or 0)
        if evidence_rows != source_items or int(metrics[4] or 0):
            raise ValueError("GMO K 线来源计数或 PIT 契约不符")
        evidence_unique = db.execute(
            "SELECT COUNT(*)-COUNT(DISTINCT observation_id) FROM (" +
            evidence_query + ")"
        ).fetchone()
        if evidence_unique is None or int(evidence_unique[0]):
            raise ValueError("GMO K 线 evidence identity 重复")
        fact_path, fact_sha = _copy_query(
            db, fact_query, output_dir / "facts.parquet",
            "interval,open_time,value_revision",
        )
        evidence_path, evidence_sha = _copy_query(
            db, evidence_query, output_dir / "evidence.parquet",
            "source_artifact_id,source_line_index,source_item_index",
        )
        minmax = db.execute(
            "SELECT MIN(open_time),MAX(open_time) FROM (" + fact_query + ")"
        ).fetchone()
        min_event = minmax[0].isoformat() if minmax and minmax[0] else None
        max_event = minmax[1].isoformat() if minmax and minmax[1] else None
        finished = utc_now()
        fact_storage = _relative_storage_path(root, fact_path)
        evidence_storage = _relative_storage_path(root, evidence_path)
        manifest = {
            "attempt_id": attempt_id, "status": "complete",
            "market_id": market_id, "venue_symbol": symbol,
            "partition_key": PARTITION_KEY,
            "normalization_version": KLINE_NORMALIZATION_VERSION,
            "input_artifact_ids": [item.artifact_id for item in inputs],
            "source_items": source_items, "fact_rows": fact_rows,
            "evidence_rows": evidence_rows,
            "conflicting_revisions": conflicts,
            "provisional_facts": provisional,
            "outputs": {
                KLINE_DATASET: fact_storage,
                EVIDENCE_DATASET: evidence_storage,
            },
        }
        manifest_path = output_dir / f"manifest-{attempt_id}.json"
        atomic_write_text(
            manifest_path, json.dumps(manifest, ensure_ascii=False, indent=2) + "\n"
        )
        conn.execute("BEGIN IMMEDIATE")
        for identity, dataset, path, sha, count in (
            (artifact_id(fact_sha), KLINE_DATASET, fact_path, fact_sha, fact_rows),
            (artifact_id(evidence_sha), EVIDENCE_DATASET, evidence_path,
             evidence_sha, evidence_rows),
        ):
            storage = _relative_storage_path(root, path)
            _register_content_artifact(
                conn, identity, "materialized_parquet", storage, sha,
                path.stat().st_size, finished, KLINE_SCHEMA_VERSION,
            )
            conn.execute(
                "INSERT INTO materialization_output VALUES (?,?,?,?,?,?,?)",
                (attempt_id, identity, dataset, count,
                 min_event, max_event, finished),
            )
        manifest_sha = sha256_file(manifest_path)
        _register_content_artifact(
            conn, artifact_id(manifest_sha), "materialization_manifest",
            _relative_storage_path(root, manifest_path), manifest_sha,
            manifest_path.stat().st_size, finished, 1,
        )
        for item in inputs:
            count = int(source_counts.get((item.artifact_id, symbol), 0))
            conn.execute(
                "INSERT INTO partition_input "
                "(attempt_id,artifact_id,source_rows,normalized_rows,ignored_rows,rejected_rows) "
                "VALUES (?,?,?,?,0,0)",
                (attempt_id, item.artifact_id, count, count),
            )
            conn.execute(
                "INSERT INTO partition_input_binding "
                "(attempt_id,artifact_id,storage_path,source_rows,normalized_rows,"
                "ignored_rows,rejected_rows) VALUES (?,?,?,?,?,0,0)",
                (attempt_id, item.artifact_id, item.storage_path, count, count),
            )
        metadata = json.dumps({
            "conflicting_revisions": conflicts,
            "provisional_facts": provisional,
        }, separators=(",", ":"))
        conn.execute(
            "UPDATE partition_attempt SET status='complete',normalized_rows=?,"
            "finished_at=?,failure_detail=? WHERE attempt_id=?",
            (evidence_rows, finished, metadata, attempt_id),
        )
        conn.execute(
            "INSERT INTO materialization_partition_head VALUES (?,?,?,?,?,?) "
            "ON CONFLICT(market_id,domain,partition_key) DO UPDATE SET "
            "normalization_version=excluded.normalization_version,"
            "attempt_id=excluded.attempt_id,activated_at=excluded.activated_at",
            (market_id, "kline", PARTITION_KEY,
             KLINE_NORMALIZATION_VERSION, attempt_id, finished),
        )
        conn.commit()
        return KlineResult(
            attempt_id, market_id, symbol, "complete", source_items,
            fact_rows, evidence_rows, conflicts, provisional,
            fact_storage, evidence_storage, False,
        )
    except Exception as exc:
        conn.rollback()
        conn.execute(
            "UPDATE partition_attempt SET status='failed',finished_at=?,"
            "failure_detail=? WHERE attempt_id=? AND status='running'",
            (utc_now(), str(exc)[:2000], attempt_id),
        ); conn.commit()
        raise


def materialize_all(root: Path, conn: sqlite3.Connection) -> list[KlineResult]:
    """断点复用地物化本地四份 GMO K 线原件。"""
    registry.register_all(conn); ensure_markets(conn)
    inputs = _raw_inputs(root); _register_inputs(conn, inputs)
    input_hash = _input_set_hash(inputs)
    counts = _symbols_and_counts(inputs)
    symbols = sorted(counts)
    reusable: dict[str, KlineResult] = {}
    for symbol in symbols:
        market_id, _, _ = _market_row(conn, "gmo", symbol, None)
        result = _completed(conn, market_id, input_hash)
        if result is not None: reusable[symbol] = result
    if len(reusable) == len(symbols):
        return [reusable[symbol] for symbol in symbols]
    staging = root / "materialized" / ".staging"
    staging.mkdir(parents=True, exist_ok=True)
    stage_path = staging / f"gmo-kline-{input_hash[:12]}.csv"
    print(f"STAGE source JSONL -> {stage_path}", flush=True)
    total, source_counts = _write_stage(inputs, stage_path)
    if total != sum(counts.values()):
        raise ValueError("K 线预扫与 stage 数组项计数不符")
    print(f"STAGE COMPLETE source_items={total:,}", flush=True)
    db_path = staging / f"gmo-kline-{input_hash[:12]}.duckdb"
    if db_path.exists(): db_path.unlink()
    db: Any = duckdb.connect(str(db_path)); db.execute("SET TimeZone='UTC'")
    try:
        _load_stage(db, stage_path)
        db.execute("CREATE INDEX idx_raw_kline_symbol ON raw_kline_observation(venue_symbol)")
        results: list[KlineResult] = []
        for index, symbol in enumerate(symbols, start=1):
            if symbol in reusable:
                result = reusable[symbol]
            else:
                result = _materialize_symbol(
                    root, conn, db, inputs, input_hash, symbol,
                    int(counts[symbol]), source_counts,
                )
            results.append(result)
            print(
                f"[{index}/{len(symbols)}] {'REUSED' if result.reused else 'DONE'} "
                f"{symbol} facts={result.fact_rows:,} evidence={result.evidence_rows:,} "
                f"revisions+={result.conflicting_revisions:,} "
                f"provisional={result.provisional_facts:,}",
                flush=True,
            )
        return results
    finally:
        db.close()
        if stage_path.exists(): stage_path.unlink()
        if db_path.exists(): db_path.unlink()
        wal = db_path.with_suffix(db_path.suffix + ".wal")
        if wal.exists(): wal.unlink()


def audit_klines(root: Path, conn: sqlite3.Connection) -> dict[str, object]:
    """审计活动 GMO K 线事实/证据及逐项来源守恒。"""
    errors: list[str] = []
    rows = conn.execute(
        "SELECT a.attempt_id,a.market_id,a.normalized_rows,"
        "MAX(CASE WHEN o.dataset=? THEN r.storage_path END),"
        "MAX(CASE WHEN o.dataset=? THEN r.storage_path END) "
        "FROM materialization_partition_head h JOIN partition_attempt a "
        "ON a.attempt_id=h.attempt_id JOIN materialization_output o "
        "ON o.attempt_id=a.attempt_id JOIN artifact r ON r.artifact_id=o.artifact_id "
        "WHERE h.domain='kline' GROUP BY a.attempt_id,a.market_id,a.normalized_rows",
        (KLINE_DATASET, EVIDENCE_DATASET),
    ).fetchall()
    total_facts = total_evidence = conflicts = provisional = 0
    db: Any = duckdb.connect(":memory:"); db.execute("SET TimeZone='UTC'")
    try:
        for attempt, market, normalized, fact_storage, evidence_storage in rows:
            if not fact_storage or not evidence_storage:
                errors.append(f"K 线双输出缺失: {attempt}"); continue
            facts = root / str(fact_storage); evidence = root / str(evidence_storage)
            fact = db.execute(
                "SELECT COUNT(*),COUNT(*)-COUNT(DISTINCT kline_id),"
                "SUM(available_time<open_time),"
                "SUM(CASE WHEN closed_available_time IS NOT NULL AND "
                "closed_available_time<close_time THEN 1 ELSE 0 END),"
                "SUM(NOT is_closed),SUM(evidence_count),"
                "COUNT(*)-COUNT(DISTINCT (market_id,interval,open_time,origin)) "
                "FROM read_parquet(?)", [str(facts)],
            ).fetchone()
            ev = db.execute(
                "SELECT COUNT(*),COUNT(*)-COUNT(DISTINCT observation_id),"
                "COUNT(DISTINCT market_id),MIN(market_id) FROM read_parquet(?)",
                [str(evidence)],
            ).fetchone()
            if fact is None or ev is None: errors.append(f"K 线输出不可读: {attempt}"); continue
            total_facts += int(fact[0]); total_evidence += int(ev[0])
            provisional += int(fact[4] or 0); conflicts += int(fact[6] or 0)
            if int(fact[1] or 0) or int(fact[2] or 0) or int(fact[3] or 0):
                errors.append(f"K 线键/PIT 失败: {attempt}")
            if int(ev[0]) != int(normalized) or int(ev[1] or 0):
                errors.append(f"K 线 evidence 计数/键失败: {attempt}")
            if int(fact[5] or 0) != int(ev[0]):
                errors.append(f"K 线 fact/evidence 不守恒: {attempt}")
            if int(ev[2]) != 1 or str(ev[3]) != str(market):
                errors.append(f"K 线 market 绑定失败: {attempt}")
    finally:
        db.close()
    return {
        "markets": len(rows), "fact_rows": total_facts,
        "evidence_rows": total_evidence,
        "conflicting_revisions": conflicts,
        "provisional_facts": provisional,
        "errors": errors, "ok": not errors,
    }


def main(argv: Sequence[str] | None = None) -> int:
    """命令行入口。"""
    parser = argparse.ArgumentParser(description="GMO K 线 P2 物化")
    parser.add_argument("--data-root", type=Path, default=None)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("all", help="物化全部本地 GMO K 线")
    sub.add_parser("audit", help="审计活动 GMO K 线")
    args = parser.parse_args(argv)
    root = (args.data_root or configured_data_root()).resolve()
    conn = store.connect(root)
    try:
        if args.command == "all":
            result: object = [asdict(item) for item in materialize_all(root, conn)]
            code = 0
        else:
            result = audit_klines(root, conn)
            code = 0 if bool(result["ok"]) else 1
    finally:
        conn.close()
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
