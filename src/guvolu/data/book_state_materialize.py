"""活动 L2 分区末态到 ``book_state_checkpoint`` 的可恢复物化。"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Sequence

import duckdb

from guvolu.data.durable_io import atomic_write_text
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
from guvolu.data.store import connect, connect_readonly
from guvolu.data.watch_connection import connect_with_retry
from guvolu.data.okx_l2_terminal_checkpoint import TERMINAL_CHECKPOINT_DATASET
from guvolu.ui.materialized_query import MaterializedQuery, replay_l2_snapshot
from guvolu.ui.query_catalog import (
    ActiveOutput,
    ActiveOutputSnapshot,
    QueryCatalog,
    materialization_input_set_hash,
    select_l2_checkpoint_inputs,
)

DATASET = "book_state_checkpoint"
DOMAIN = "book_state"
SCHEMA_VERSION = 1
NORMALIZATION_VERSION = "book-state-checkpoint-v3"
PARTITION_KEY = "latest"


@dataclass(frozen=True)
class CheckpointResult:
    attempt_id: str
    market_id: str
    partition_key: str
    rows: int
    replay_frames: int
    storage_path: str
    reused: bool


def _source_partitions(root: Path) -> list[ActiveOutputSnapshot]:
    conn = connect_readonly(root)
    if conn is None:
        return []
    try:
        markets = [
            str(row[0]) for row in conn.execute(
                "SELECT DISTINCT market_id FROM materialization_partition_head "
                "WHERE domain='book_l2' ORDER BY market_id"
            )
        ]
    finally:
        conn.close()
    out: list[ActiveOutputSnapshot] = []
    catalog = QueryCatalog(root)
    for market_id in markets:
        snapshot = catalog.active_outputs(
            market_id, domains=("book_l2",),
            datasets=("book_l2_frame", "book_l2_level"),
        )
        out.append(
            snapshot if str(snapshot.market.get("venue_id")) == "okx"
            else select_l2_checkpoint_inputs(snapshot)
        )
    return out


def _timestamp(value: object) -> datetime | None:
    if value is None:
        return None
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _resolved_input(
    root: Path,
    conn: sqlite3.Connection,
    snapshot: ActiveOutputSnapshot,
) -> tuple[ActiveOutputSnapshot, dict[str, Any]]:
    """冻结实际参与状态重放的制品集合。"""
    if str(snapshot.market.get("venue_id")) != "okx":
        return snapshot, replay_l2_snapshot(snapshot)
    state = MaterializedQuery(root).replay_l2_state_from_snapshot(
        snapshot, decision_time=datetime.now(UTC),
    )
    artifact_ids = {
        str(value) for value in state.get("source_artifact_ids", [])
    }
    selected = [
        row for row in snapshot.outputs if row.artifact_id in artifact_ids
    ]
    state_artifact = state.get("state_artifact_id")
    if state_artifact is not None:
        row = conn.execute(
            "SELECT p.domain,p.partition_key,p.normalization_version,p.attempt_id,"
            "o.dataset,o.artifact_id,a.storage_path,o.row_count,o.min_event_time,"
            "o.max_event_time FROM materialization_output o "
            "JOIN partition_attempt p ON p.attempt_id=o.attempt_id "
            "JOIN artifact a ON a.artifact_id=o.artifact_id "
            "WHERE o.artifact_id=? AND o.dataset=?",
            (str(state_artifact), TERMINAL_CHECKPOINT_DATASET),
        ).fetchone()
        if row is None:
            raise ValueError("OKX 终态血缘制品未登记")
        selected.append(ActiveOutput(
            domain=str(row[0]), partition_key=str(row[1]),
            normalization_version=str(row[2]), attempt_id=str(row[3]),
            dataset=str(row[4]), artifact_id=str(row[5]),
            path=root / str(row[6]), row_count=int(row[7]),
            min_event_time=_timestamp(row[8]), max_event_time=_timestamp(row[9]),
        ))
    found = {row.artifact_id for row in selected}
    if found != artifact_ids:
        raise ValueError("OKX 状态血缘与活动制品集合不一致")
    return ActiveOutputSnapshot(
        market=snapshot.market, outputs=tuple(selected),
        head_generation=snapshot.head_generation,
    ), state


def _completed(
    root: Path, conn: sqlite3.Connection, market_id: str, partition_key: str,
    input_hash: str,
) -> CheckpointResult | None:
    row = conn.execute(
        "SELECT a.attempt_id,a.normalized_rows,r.storage_path "
        "FROM partition_attempt a JOIN materialization_output o "
        "ON o.attempt_id=a.attempt_id JOIN artifact r ON r.artifact_id=o.artifact_id "
        "WHERE a.market_id=? AND a.domain=? AND a.partition_key=? "
        "AND a.normalization_version=? AND a.input_set_hash=? "
        "AND a.status='complete' AND o.dataset=? ORDER BY a.finished_at DESC LIMIT 1",
        (market_id, DOMAIN, partition_key, NORMALIZATION_VERSION, input_hash, DATASET),
    ).fetchone()
    if row is None:
        return None
    db: Any = duckdb.connect(":memory:")
    try:
        replay_row = db.execute(
            "SELECT MAX(replay_frames) FROM read_parquet(?)",
            [str(root / str(row[2]))],
        ).fetchone()
    finally:
        db.close()
    replay = 0 if replay_row is None or replay_row[0] is None else int(replay_row[0])
    return CheckpointResult(
        str(row[0]), market_id, partition_key, int(row[1]), replay,
        str(row[2]), True,
    )


def _write_checkpoint(
    root: Path, snapshot: ActiveOutputSnapshot, state: dict[str, Any],
    attempt_id: str,
) -> tuple[Path, str, int]:
    market = snapshot.market
    output_dir = (
        root / "materialized" / DATASET
        / f"schema_version={SCHEMA_VERSION}"
        / f"normalization_version={NORMALIZATION_VERSION}"
        / f"venue_id={market['venue_id']}"
        / f"market_id={market['market_id']}"
        / f"source_attempt={state['source_attempt_id']}"
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    temp = output_dir / f".{attempt_id}.tmp.parquet"
    rows = [
        (
            hashlib.sha256(
                f"{market['market_id']}|{state['as_of_frame_id']}".encode()
            ).hexdigest(),
            market["market_id"], market["venue_id"], market["instrument_id"],
            state["source_attempt_id"], state["as_of_frame_id"],
            state["as_of_event_time"], state["as_of_available_time"],
            state["snapshot_frame_id"], state["snapshot_event_time"],
            state["integrity_mode"], state["replay_frames"], side,
            level["price"], level["size"], state["source_depth_levels"],
            NORMALIZATION_VERSION, SCHEMA_VERSION,
        )
        for side, levels in (("ask", state["asks"]), ("bid", state["bids"]))
        for level in levels
    ]
    db: Any = duckdb.connect(":memory:")
    db.execute("SET TimeZone='UTC'")
    try:
        db.execute("""
          CREATE TABLE checkpoint (
            checkpoint_id VARCHAR,market_id VARCHAR,venue_id VARCHAR,
            instrument_id VARCHAR,source_attempt_id VARCHAR,as_of_frame_id VARCHAR,
            event_time TIMESTAMPTZ,available_time TIMESTAMPTZ,
            snapshot_frame_id VARCHAR,snapshot_event_time TIMESTAMPTZ,
            integrity_mode VARCHAR,replay_frames INTEGER,side VARCHAR,
            price VARCHAR,size VARCHAR,source_depth_levels INTEGER,
            normalization_version VARCHAR,schema_version INTEGER
          )
        """)
        db.executemany("INSERT INTO checkpoint VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
        escaped = temp.as_posix().replace("'", "''")
        db.execute(
            f"COPY (SELECT * FROM checkpoint ORDER BY side,CAST(price AS DECIMAL(38,12))) "
            f"TO '{escaped}' (FORMAT PARQUET,COMPRESSION ZSTD)"
        )
    finally:
        db.close()
    with temp.open("rb+") as handle:
        handle.flush()
        os.fsync(handle.fileno())
    sha = sha256_file(temp)
    final = output_dir / f"part-{sha[:12]}.parquet"
    if final.exists():
        if sha256_file(final) != sha:
            raise ValueError(f"checkpoint 散列命名冲突: {final}")
        temp.unlink()
    else:
        os.replace(temp, final)
    return final, sha, len(rows)


def materialize_partition(
    root: Path, conn: sqlite3.Connection, snapshot: ActiveOutputSnapshot,
) -> CheckpointResult:
    market_id = str(snapshot.market["market_id"])
    partition_key = PARTITION_KEY
    inputs, state = _resolved_input(root, conn, snapshot)
    upstream_attempts = sorted({row.attempt_id for row in inputs.outputs})
    input_hash = materialization_input_set_hash(inputs.outputs)
    reused = _completed(root, conn, market_id, partition_key, input_hash)
    if reused is not None:
        return reused
    attempt_id = "book-state-" + uuid.uuid4().hex
    started = utc_now()
    config_hash = hashlib.sha256(json.dumps({
        "dataset": DATASET, "schema_version": SCHEMA_VERSION,
        "normalization_version": NORMALIZATION_VERSION,
        "replay_contract": "wire-order-or-terminal-tail-v3",
    }, sort_keys=True).encode()).hexdigest()
    source_rows = sum(row.row_count for row in inputs.outputs)
    conn.execute(
        "INSERT INTO partition_attempt "
        "(attempt_id,market_id,domain,partition_key,normalization_version,"
        "input_set_hash,status,source_rows,normalized_rows,ignored_rows,"
        "rejected_rows,started_at,code_version,config_hash) "
        "VALUES (?,?,?,?,?,?,'running',?,0,0,0,?,'working-tree',?)",
        (attempt_id, market_id, DOMAIN, partition_key,
         NORMALIZATION_VERSION, input_hash, source_rows, started, config_hash),
    )
    conn.executemany(
        "INSERT INTO materialization_dependency VALUES (?,?,?,?)",
        [(attempt_id, source, "active-head", started)
         for source in upstream_attempts],
    )
    try:
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    output: Path | None = None
    output_sha: str | None = None
    output_rows = 0
    manifest: Path | None = None
    manifest_body: dict[str, object] | None = None
    try:
        output, sha, rows = _write_checkpoint(root, snapshot, state, attempt_id)
        output_sha = sha
        output_rows = rows
        finished = utc_now()
        storage_path = _relative_storage_path(root, output)
        manifest = output.parent / f"manifest-{attempt_id}.json"
        manifest_body = {
            "attempt_id": attempt_id, "status": "complete",
            "market_id": market_id, "partition_key": partition_key,
            "upstream_attempt_ids": upstream_attempts,
            "source_attempt_id": state["source_attempt_id"],
            "input_artifact_ids": sorted(row.artifact_id for row in inputs.outputs),
            "terminal_checkpoint_artifact_id": state.get("state_artifact_id"),
            "normalization_version": NORMALIZATION_VERSION,
            "rows": rows, "replay_frames": state["replay_frames"],
            "as_of_event_time": state["as_of_event_time"],
            "output": storage_path,
        }
        atomic_write_text(
            manifest,
            json.dumps(manifest_body, ensure_ascii=False, indent=2) + "\n",
        )
        conn.execute("BEGIN IMMEDIATE")
        output_id = artifact_id(sha)
        _register_content_artifact(
            conn, output_id, "materialized_parquet", storage_path, sha,
            output.stat().st_size, finished, SCHEMA_VERSION,
        )
        register_materialization_manifest(
            root, conn, manifest, SCHEMA_VERSION, finished,
        )
        conn.execute(
            "INSERT INTO materialization_output VALUES (?,?,?,?,?,?,?)",
            (attempt_id, output_id, DATASET, rows,
             state["as_of_event_time"], state["as_of_event_time"], finished),
        )
        for source in inputs.outputs:
            source_path = _relative_storage_path(root, source.path)
            conn.execute(
                "INSERT INTO partition_input "
                "(attempt_id,artifact_id,source_rows,normalized_rows,"
                "ignored_rows,rejected_rows) VALUES (?,?,?,?,?,?)",
                (attempt_id, source.artifact_id, source.row_count,
                 source.row_count, 0, 0),
            )
            conn.execute(
                "INSERT INTO partition_input_binding "
                "(attempt_id,artifact_id,storage_path,source_rows,"
                "normalized_rows,ignored_rows,rejected_rows) "
                "VALUES (?,?,?,?,?,?,?)",
                (attempt_id, source.artifact_id, source_path,
                 source.row_count, source.row_count, 0, 0),
            )
        conn.execute(
            "UPDATE partition_attempt SET status='complete',normalized_rows=?,"
            "finished_at=? WHERE attempt_id=?",
            (rows, finished, attempt_id),
        )
        conn.execute(
            "INSERT INTO materialization_partition_head VALUES (?,?,?,?,?,?) "
            "ON CONFLICT(market_id,domain,partition_key) DO UPDATE SET "
            "normalization_version=excluded.normalization_version,"
            "attempt_id=excluded.attempt_id,activated_at=excluded.activated_at",
            (market_id, DOMAIN, partition_key, NORMALIZATION_VERSION,
             attempt_id, finished),
        )
        conn.commit()
        return CheckpointResult(
            attempt_id, market_id, partition_key, rows,
            int(state["replay_frames"]), storage_path, False,
        )
    except Exception as exc:
        conn.rollback()
        if (
            output is not None
            and output_sha is not None
            and manifest is not None
            and manifest_body is not None
        ):
            failed = settle_failed_publication(
                root,
                conn,
                attempt_id,
                manifest,
                manifest_body,
                (UnpromotedOutput(
                    DATASET, output, output_sha, output_rows, SCHEMA_VERSION,
                ),),
                exc,
                SCHEMA_VERSION,
            )
            if not failed:
                return CheckpointResult(
                    attempt_id, market_id, partition_key, output_rows,
                    int(state["replay_frames"]),
                    _relative_storage_path(root, output), False,
                )
            raise
        conn.execute(
            "UPDATE partition_attempt SET status='failed',finished_at=?,"
            "failure_detail=? WHERE attempt_id=? AND status='running'",
            (utc_now(), str(exc)[:2000], attempt_id),
        )
        conn.commit()
        raise


def materialize_all(root: Path, conn: sqlite3.Connection) -> list[CheckpointResult]:
    # 旧分段头不代表连续状态。
    # 仅撤销活动指针。
    # 其余制品保留审计。
    with sqlite_writer_lock(root):
        conn.execute(
            "DELETE FROM materialization_partition_head WHERE domain=? "
            "AND partition_key<>? AND normalization_version=?",
            (DOMAIN, PARTITION_KEY, NORMALIZATION_VERSION),
        )
        conn.commit()
    snapshots = _source_partitions(root)
    results: list[CheckpointResult] = []
    for index, snapshot in enumerate(snapshots, start=1):
        # 重放与本市场提交同锁。
        # 防止活动头中途换代。
        # 市场之间释放锁。
        # 为其他 writer 留出窗口。
        with sqlite_writer_lock(root):
            result = materialize_partition(root, conn, snapshot)
        results.append(result)
        print(
            f"[{index}/{len(snapshots)}] "
            f"{'REUSED' if result.reused else 'DONE'} {result.market_id} "
            f"{result.partition_key} rows={result.rows:,} "
            f"replay_frames={result.replay_frames:,}", flush=True,
        )
    return results


def audit(root: Path, conn: sqlite3.Connection) -> dict[str, object]:
    rows = conn.execute(
        "SELECT h.market_id,h.partition_key,h.attempt_id,o.row_count,a.storage_path,"
        "a.sha256 FROM materialization_partition_head h "
        "JOIN materialization_output o ON o.attempt_id=h.attempt_id AND o.dataset=? "
        "JOIN artifact a ON a.artifact_id=o.artifact_id "
        "WHERE h.domain=? ORDER BY h.market_id,h.partition_key",
        (DATASET, DOMAIN),
    ).fetchall()
    errors: list[str] = []
    current_inputs = {
        str(snapshot.market["market_id"]): snapshot
        for snapshot in _source_partitions(root)
    }
    catalog = QueryCatalog(root)
    db: Any = duckdb.connect(":memory:")
    try:
        for market_id, partition, attempt, expected, storage, sha in rows:
            path = root / str(storage)
            if not path.is_file() or sha256_file(path) != str(sha):
                errors.append(f"checkpoint 文件缺失或散列错误: {attempt}")
                continue
            facts = db.execute(
                "SELECT COUNT(*),COUNT(DISTINCT side),COUNT(DISTINCT market_id),"
                "MIN(size::DECIMAL(38,12)),COUNT(DISTINCT source_attempt_id) "
                "FROM read_parquet(?)", [str(path)],
            ).fetchone()
            if facts != (int(expected), 2, 1, facts[3], 1) or facts[3] <= 0:
                errors.append(f"checkpoint 结构/数量错误: {attempt}")
            current = current_inputs.get(str(market_id))
            lineage = catalog.attempt_lineage(str(attempt))
            resolved = None
            if current is not None:
                resolved, _state = _resolved_input(root, conn, current)
            expected_attempts = frozenset(
                row.attempt_id for row in resolved.outputs
            ) if resolved is not None else frozenset()
            expected_artifacts = frozenset(
                row.artifact_id for row in resolved.outputs
            ) if resolved is not None else frozenset()
            expected_hash = (
                materialization_input_set_hash(resolved.outputs)
                if resolved is not None else None
            )
            if (
                lineage is None
                or not expected_attempts or not expected_artifacts
                or lineage.upstream_attempt_ids != expected_attempts
                or lineage.input_artifact_ids != expected_artifacts
                or lineage.input_set_hash != expected_hash
            ):
                errors.append(f"checkpoint 上游 head 已变化: {attempt}")
    finally:
        db.close()
    return {"checkpoints": len(rows), "errors": errors, "ok": not errors}


def _watch(root: Path, poll_seconds: float) -> int:
    """持续刷新末态检查点；启动锁竞争只延后本轮。"""
    def report_connect_error(exc: Exception, elapsed: float) -> None:
        print(json.dumps({
            "event": "book_state_materialization_startup_error",
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
            try:
                results = materialize_all(root, conn)
                # 复核只读，不占写锁
                report = audit(root, conn)
                print(json.dumps({
                    "event": "book_state_materialization_cycle",
                    "markets": len(results),
                    "materialized_now": sum(not row.reused for row in results),
                    "reused": sum(row.reused for row in results),
                    "audit_ok": report["ok"],
                    "audit_errors": report["errors"],
                    "elapsed_seconds": round(
                        time.monotonic() - cycle_started, 3
                    ),
                }, ensure_ascii=False), flush=True)
            except (OSError, sqlite3.Error, ValueError, duckdb.Error) as exc:
                print(json.dumps({
                    "event": "book_state_materialization_error",
                    "error": f"{type(exc).__name__}: {exc}",
                    "elapsed_seconds": round(
                        time.monotonic() - cycle_started, 3
                    ),
                }, ensure_ascii=False), flush=True)
            time.sleep(poll_seconds)
    except KeyboardInterrupt:
        print("盘口末态物化已停止", flush=True)
        return 0
    finally:
        if conn is not None:
            conn.close()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", type=Path)
    parser.add_argument("command", choices=("all", "audit", "watch"))
    parser.add_argument("--poll-seconds", type=float, default=30.0)
    args = parser.parse_args(argv)
    root = (args.data_root or configured_data_root()).resolve()
    if args.command == "watch":
        return _watch(root, max(5.0, float(args.poll_seconds)))

    conn = connect(root)
    try:
        if args.command == "audit":
            report = audit(root, conn)
            print(json.dumps(report, ensure_ascii=False, indent=2))
            return 0 if report["ok"] else 1
        materialize_all(root, conn)
        audit(root, conn)
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
