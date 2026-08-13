"""bitbank ``circuit_break_info`` 的独立、可审计增量物化。

输入仍是 L2 采集器写出的同一 raw v3 segment；输出却属于独立
``market_status`` 域。盘口频道、协议帧只计入该域的 compact ignore 总数，
不会变成市场状态事实或 SQLite 的逐行膨胀记录。
"""
from __future__ import annotations

import csv
import hashlib
import json
import os
import sqlite3
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from decimal import InvalidOperation
from pathlib import Path
from typing import Any

import duckdb

from guvolu.data import store
from guvolu.data.durable_io import atomic_write_text
from guvolu.data.l2_materialize import (
    SegmentInput,
    _DESCRIPTORS,
    _available,
    _decimal_text,
    _field,
    _iso_millis,
    _raw_metadata,
    _register_source,
    _sealed_inputs,
    _time,
)
from guvolu.data.market_status_contract import (
    MARKET_STATUS_DATASET,
    MARKET_STATUS_NORMALIZATION_VERSION,
    MARKET_STATUS_SCHEMA_VERSION,
    create_market_status_table,
)
from guvolu.data.materialize import (
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
from guvolu.data.realtime_control import (
    RealtimeChannelObservation,
    register_materialized_raw_v3_observations,
)
from guvolu.data.sqlite_writer_lock import sqlite_writer_lock
from guvolu.venues import registry


STATUS_DOMAIN = "market_status"
STATUS_ENDPOINT = "circuit_break_info"
STATUS_ENDPOINT_ID = "EP-0005"
STATUS_ENDPOINT_REVISION = 1
STATUS_PAYLOAD_SCHEMA = "bitbank-circuit-break-info-stream-v1"
_MODES = frozenset({
    "NONE", "CIRCUIT_BREAK", "FULL_RANGE_CIRCUIT_BREAK",
    "RESUMPTION", "LISTING",
})
_FEE_TYPES = frozenset({"NORMAL", "SELL_MAKER", "BUY_MAKER", "DYNAMIC"})


@dataclass(frozen=True)
class MarketStatusResult:
    """一个包含市场状态消息的 raw segment 的物化结果。"""

    attempt_id: str
    market_id: str
    partition_key: str
    status: str
    source_rows: int
    observation_rows: int
    ignored_rows: int
    rejected_rows: int
    output_path: str
    reused: bool


def _observation_id(
    market_id: str, source_artifact_id: str, source_row_index: int,
) -> str:
    body = (
        f"bitbank|{market_id}|{source_artifact_id}|{source_row_index}|"
        "circuit_break_info"
    )
    return "sha256-" + hashlib.sha256(body.encode("utf-8")).hexdigest()


def _optional_decimal(value: object) -> str | None:
    return None if value is None else _decimal_text(value)


def _optional_millis(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool) or int(str(value)) <= 0:
        raise ValueError("reopen_timestamp 必须为正毫秒时间或 null")
    return _iso_millis(value)


def _parse_status(payload_raw: str) -> dict[str, object] | None:
    """只解析状态频道；其他频道和协议帧返回 ``None``。"""
    if not payload_raw.startswith("42"):
        return None
    packet = json.loads(payload_raw[2:])
    if (
        not isinstance(packet, list) or len(packet) < 2
        or not isinstance(packet[1], Mapping)
    ):
        return None
    envelope = packet[1]
    room = str(envelope.get("room_name", ""))
    if not room.startswith("circuit_break_info_"):
        return None
    message = envelope.get("message")
    data = message.get("data") if isinstance(message, Mapping) else None
    if not isinstance(data, Mapping):
        raise ValueError("bitbank circuit_break_info message.data 缺失")
    mode = str(data.get("mode", ""))
    fee_type = str(data.get("fee_type", ""))
    if mode not in _MODES:
        raise ValueError(f"bitbank circuit_break_info mode 非法: {mode!r}")
    if fee_type not in _FEE_TYPES:
        raise ValueError(
            f"bitbank circuit_break_info fee_type 非法: {fee_type!r}"
        )
    timestamp = data.get("timestamp")
    if timestamp is None:
        raise ValueError("bitbank circuit_break_info timestamp 缺失")
    event_time = _iso_millis(timestamp)
    return {
        "channel": room,
        "event_time": event_time,
        "mode": mode,
        "fee_type": fee_type,
        "estimated_auction_price": _optional_decimal(
            data.get("estimated_itayose_price")
        ),
        "estimated_auction_amount": _optional_decimal(
            data.get("estimated_itayose_amount")
        ),
        "auction_upper_price": _optional_decimal(
            data.get("itayose_upper_price")
        ),
        "auction_lower_price": _optional_decimal(
            data.get("itayose_lower_price")
        ),
        "upper_trigger_price": _optional_decimal(
            data.get("upper_trigger_price")
        ),
        "lower_trigger_price": _optional_decimal(
            data.get("lower_trigger_price")
        ),
        "reopen_time": _optional_millis(data.get("reopen_timestamp")),
    }


def _status_candidate_count(item: SegmentInput) -> int:
    """流式确认行数并统计可能的状态频道行。"""
    marker = b"circuit_break_info_"
    rows = candidates = 0
    with item.artifact.absolute_path.open("rb") as handle:
        for line in handle:
            rows += 1
            candidates += int(marker in line)
    if rows != item.artifact.source_rows:
        raise ValueError("market_status 扫描行数与 sealed manifest 不符")
    return candidates


def sealed_status_inputs(
    root: Path, conn: sqlite3.Connection,
) -> list[SegmentInput]:
    """返回含状态候选的 r1 输入，并持久化阴性扫描断点。

    EP-0005 r0 在接入该频道之前已经封口，故不扫描、不建空事实，也不把
    “未订阅”错误解释为状态缺失。r1 阴性段只留一行小型扫描证据，重启后
    不会反复读完整 raw segment。
    """
    selected: list[SegmentInput] = []
    registry.register_all(conn)
    for item in _sealed_inputs(root):
        if (
            item.raw_schema_version != 3
            or item.endpoint_id != STATUS_ENDPOINT_ID
            or item.endpoint_revision != STATUS_ENDPOINT_REVISION
        ):
            continue
        _register_source(conn, item)
        recorded = conn.execute(
            "SELECT source_rows,candidate_rows,endpoint_id,endpoint_revision "
            "FROM market_status_input_scan WHERE artifact_id=? "
            "AND normalization_version=?",
            (item.artifact.artifact_id, MARKET_STATUS_NORMALIZATION_VERSION),
        ).fetchone()
        if recorded is None:
            candidates = _status_candidate_count(item)
            conn.execute(
                "INSERT INTO market_status_input_scan VALUES (?,?,?,?,?,?,?)",
                (
                    item.artifact.artifact_id,
                    MARKET_STATUS_NORMALIZATION_VERSION,
                    STATUS_ENDPOINT_ID, STATUS_ENDPOINT_REVISION,
                    item.artifact.source_rows, candidates, utc_now(),
                ),
            )
        else:
            expected = (
                item.artifact.source_rows, STATUS_ENDPOINT_ID,
                STATUS_ENDPOINT_REVISION,
            )
            if (int(recorded[0]), str(recorded[2]), int(recorded[3])) != expected:
                raise ValueError("market_status 扫描台账与 raw 身份冲突")
            candidates = int(recorded[1])
        if candidates:
            selected.append(item)
    conn.commit()
    return selected


def _capability_revision(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT revision_id FROM venue_capability_revision "
        "WHERE venue_id='bitbank' AND domain=? AND endpoint=? "
        "AND available=1 AND implementation_status='implemented' "
        "ORDER BY revision_id DESC LIMIT 1",
        (STATUS_DOMAIN, STATUS_ENDPOINT),
    ).fetchone()
    if row is None:
        raise ValueError("bitbank circuit_break_info 能力尚未登记为 implemented")
    return int(row[0])


def _bind_capability(
    conn: sqlite3.Connection, attempt_id: str, revision: int,
) -> None:
    conn.execute(
        "INSERT INTO partition_capability_binding "
        "(attempt_id,venue_id,domain,endpoint,revision_id,binding_basis,bound_at) "
        "VALUES (?,'bitbank',?,?,?,'recorded',?)",
        (attempt_id, STATUS_DOMAIN, STATUS_ENDPOINT, revision, utc_now()),
    )


def _stage(
    item: SegmentInput,
    market_id: str,
    instrument_id: str,
    mapping_revision: int,
    capability_revision: int,
    output_csv: Path,
) -> tuple[
    int, int, list[tuple[int, str]], str, str,
    tuple[RealtimeChannelObservation, ...],
]:
    descriptor = _DESCRIPTORS["bitbank"]
    observations = 0
    ignored = 0
    rejected: list[tuple[int, str]] = []
    min_event = ""
    max_event = ""
    channel_observations: list[RealtimeChannelObservation] = []
    previous_record_sequence: int | None = None
    with (
        item.artifact.absolute_path.open(encoding="utf-8") as source,
        output_csv.open("w", encoding="utf-8", newline="") as output,
    ):
        writer = csv.writer(output, lineterminator="\n")
        for source_row, line in enumerate(source, start=1):
            try:
                envelope = json.loads(line)
                if not isinstance(envelope, Mapping):
                    raise ValueError("segment 行不是对象")
                if (
                    envelope.get("venue_id") != "bitbank"
                    or envelope.get("run_id") != item.run_id
                    or int(str(envelope.get("segment_sequence")))
                    != item.segment_sequence
                ):
                    raise ValueError("segment 行身份与路径/manifest 不符")
                venue_symbol = str(envelope.get("venue_symbol", ""))
                payload_raw, raw = _raw_metadata(
                    envelope, item, descriptor
                )
                if raw.record_sequence is None:
                    raise ValueError("market_status 只接受 raw v3")
                if (
                    previous_record_sequence is not None
                    and raw.record_sequence <= previous_record_sequence
                ):
                    raise ValueError("raw v3 record_sequence 未严格递增")
                previous_record_sequence = raw.record_sequence
                parsed = _parse_status(payload_raw)
                if parsed is None:
                    ignored += 1
                    continue
                channel = str(parsed["channel"])
                if channel != f"circuit_break_info_{venue_symbol}":
                    raise ValueError("状态频道与 venue_symbol 不一致")
                if raw.channel_id != channel:
                    raise ValueError("raw v3 channel_id 与状态 payload 不一致")
                if (
                    raw.endpoint_id != STATUS_ENDPOINT_ID
                    or raw.endpoint_revision != STATUS_ENDPOINT_REVISION
                    or raw.connection_id is None
                    or raw.recv_ts_mono_ns is None
                ):
                    raise ValueError("状态事实缺少 EP-0005 r1 采集身份")
                event = _time(parsed["event_time"], "event_time").isoformat()
                available = _available(event, raw.ingest_time)
                reopen = parsed["reopen_time"]
                if reopen is not None:
                    reopen = _time(reopen, "reopen_time").isoformat()
                writer.writerow([_field(value) for value in (
                    _observation_id(
                        market_id, item.artifact.artifact_id, source_row
                    ),
                    "bitbank", venue_symbol, market_id, mapping_revision,
                    capability_revision, instrument_id,
                    raw.endpoint_id, raw.endpoint_revision,
                    STATUS_PAYLOAD_SCHEMA, event, available, raw.ingest_time,
                    raw.recv_ts_mono_ns, "venue", parsed["mode"],
                    parsed["fee_type"], parsed["estimated_auction_price"],
                    parsed["estimated_auction_amount"],
                    parsed["auction_upper_price"],
                    parsed["auction_lower_price"],
                    parsed["upper_trigger_price"],
                    parsed["lower_trigger_price"], reopen, item.run_id,
                    raw.connection_id, raw.channel_id,
                    item.artifact.absolute_path.name,
                    raw.raw_payload_sha256,
                    json.dumps(
                        sorted(set(raw.quality_flags)), separators=(",", ":")
                    ),
                    item.artifact.artifact_id, source_row,
                    MARKET_STATUS_NORMALIZATION_VERSION,
                    MARKET_STATUS_SCHEMA_VERSION,
                )])
                channel_observations.append(RealtimeChannelObservation(
                    raw.connection_id, raw.channel_id, raw.ingest_time,
                ))
                observations += 1
                min_event = event if not min_event else min(min_event, event)
                max_event = event if not max_event else max(max_event, event)
            except (
                KeyError, TypeError, ValueError, InvalidOperation,
                json.JSONDecodeError,
            ) as exc:
                rejected.append((source_row, f"{type(exc).__name__}: {exc}"))
        output.flush()
        os.fsync(output.fileno())
    if item.artifact.source_rows != observations + ignored + len(rejected):
        raise ValueError("market_status 来源行分类不守恒")
    return (
        observations, ignored, rejected, min_event, max_event,
        tuple(channel_observations),
    )


def _finalize(temp: Path) -> tuple[Path, str]:
    sha = sha256_file(temp)
    final = temp.with_name(f"part-{sha[:12]}.parquet")
    if final.exists():
        if sha256_file(final) != sha:
            raise ValueError(f"输出散列命名冲突: {final}")
        temp.unlink()
    else:
        os.replace(temp, final)
    return final, sha


def materialize_segment(
    root: Path, conn: sqlite3.Connection, item: SegmentInput,
) -> MarketStatusResult:
    """物化一个确含 ``circuit_break_info`` 的 r1 segment。"""
    if (
        item.raw_schema_version != 3
        or item.endpoint_id != STATUS_ENDPOINT_ID
        or item.endpoint_revision != STATUS_ENDPOINT_REVISION
    ):
        raise ValueError("输入不是 EP-0005 r1 raw v3 segment")
    scan = conn.execute(
        "SELECT candidate_rows FROM market_status_input_scan "
        "WHERE artifact_id=? AND normalization_version=?",
        (item.artifact.artifact_id, MARKET_STATUS_NORMALIZATION_VERSION),
    ).fetchone()
    if scan is None or int(scan[0]) <= 0:
        raise ValueError("输入没有已登记的 market_status 候选扫描")
    parts = item.artifact.absolute_path.parts
    venue_symbol = next(
        part.split("=", 1)[1]
        for part in parts if part.startswith("venue_symbol=")
    )
    registry.register_all(conn)
    ensure_markets(conn)
    market_id, instrument_id, mapping_revision = _market_row(
        conn, "bitbank", venue_symbol, None
    )
    capability_revision = _capability_revision(conn)
    _register_source(conn, item)
    conn.commit()
    input_hash = _input_set_hash([item.artifact])
    existing = conn.execute(
        "SELECT a.attempt_id,a.status,a.source_rows,a.normalized_rows,"
        "a.ignored_rows,a.rejected_rows,r.storage_path "
        "FROM partition_attempt a JOIN materialization_output o "
        "ON o.attempt_id=a.attempt_id JOIN artifact r "
        "ON r.artifact_id=o.artifact_id "
        "WHERE a.market_id=? AND a.domain=? AND a.partition_key=? "
        "AND a.normalization_version=? AND a.input_set_hash=? "
        "AND o.dataset=? AND a.status IN "
        "('complete','complete_with_rejections') LIMIT 1",
        (
            market_id, STATUS_DOMAIN, item.partition_key,
            MARKET_STATUS_NORMALIZATION_VERSION, input_hash,
            MARKET_STATUS_DATASET,
        ),
    ).fetchone()
    if existing is not None:
        return MarketStatusResult(
            str(existing[0]), market_id, item.partition_key, str(existing[1]),
            int(existing[2]), int(existing[3]), int(existing[4]),
            int(existing[5]), str(existing[6]), True,
        )

    attempt_id = f"market-status-{uuid.uuid4().hex}"
    config_hash = hashlib.sha256(json.dumps({
        "dataset": MARKET_STATUS_DATASET,
        "schema_version": MARKET_STATUS_SCHEMA_VERSION,
        "normalization_version": MARKET_STATUS_NORMALIZATION_VERSION,
        "input_endpoint_binding": {
            "endpoint_id": STATUS_ENDPOINT_ID,
            "endpoint_revision": STATUS_ENDPOINT_REVISION,
        },
        "ignored_detail": "other_channel_or_protocol_frame_compact_count",
    }, sort_keys=True).encode()).hexdigest()
    conn.execute(
        "INSERT INTO partition_attempt "
        "(attempt_id,market_id,domain,partition_key,normalization_version,"
        "input_set_hash,status,source_rows,normalized_rows,ignored_rows,"
        "rejected_rows,started_at,code_version,config_hash) "
        "VALUES (?,?,?,?,?,?,'running',?,0,0,0,?,'working-tree',?)",
        (
            attempt_id, market_id, STATUS_DOMAIN, item.partition_key,
            MARKET_STATUS_NORMALIZATION_VERSION, input_hash,
            item.artifact.source_rows, utc_now(), config_hash,
        ),
    )
    _bind_capability(conn, attempt_id, capability_revision)
    conn.commit()
    output_dir = (
        root / "materialized" / STATUS_DOMAIN
        / f"schema_version={MARKET_STATUS_SCHEMA_VERSION}"
        / f"normalization_version={MARKET_STATUS_NORMALIZATION_VERSION}"
        / "venue_id=bitbank" / f"market_id={market_id}"
        / f"run_id={item.run_id}"
        / f"segment={item.segment_sequence:06d}"
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    output_csv = output_dir / f".{attempt_id}.csv"
    output_temp = output_dir / f".{attempt_id}.parquet"
    try:
        (
            rows, ignored, rejected, min_event, max_event,
            channel_observations,
        ) = _stage(
            item, market_id, instrument_id, mapping_revision,
            capability_revision, output_csv,
        )
        db: Any = duckdb.connect(":memory:")
        db.execute("SET TimeZone='UTC'")
        try:
            create_market_status_table(db)
            if rows:
                escaped = output_csv.as_posix().replace("'", "''")
                db.execute(
                    f"COPY {MARKET_STATUS_DATASET} FROM '{escaped}' "
                    "(FORMAT CSV, HEADER false, NULL '\\N')"
                )
            check = db.execute(
                f"SELECT COUNT(*),COUNT(*)-COUNT(DISTINCT observation_id),"
                "SUM(available_time<event_time),"
                "SUM(CASE WHEN endpoint_id!='EP-0005' OR endpoint_revision!=1 "
                "OR connection_id IS NULL OR channel_id NOT LIKE "
                "'circuit_break_info_%' OR raw_payload_sha256 IS NULL OR "
                "length(raw_payload_sha256)!=64 OR source_artifact_id!=? "
                "THEN 1 ELSE 0 END),COUNT(DISTINCT market_id),MIN(market_id),"
                "COUNT(DISTINCT normalization_version),"
                "MIN(normalization_version) "
                f"FROM {MARKET_STATUS_DATASET}",
                [item.artifact.artifact_id],
            ).fetchone()
            if (
                check is None or int(check[0]) != rows
                or int(check[1] or 0) or int(check[2] or 0)
                or int(check[3] or 0)
                or (rows and (
                    int(check[4]) != 1 or str(check[5]) != market_id
                    or int(check[6]) != 1
                    or str(check[7]) != MARKET_STATUS_NORMALIZATION_VERSION
                ))
            ):
                raise ValueError("market_status 键/PIT/来源身份契约失败")
            escaped_output = output_temp.as_posix().replace("'", "''")
            db.execute(
                f"COPY (SELECT * FROM {MARKET_STATUS_DATASET} "
                "ORDER BY event_time,observation_id) "
                f"TO '{escaped_output}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
        finally:
            db.close()
        output_csv.unlink()
        output_path, output_sha = _finalize(output_temp)
        finished = utc_now()
        status = "complete_with_rejections" if rejected else "complete"
        output_storage = _relative_storage_path(root, output_path)
        manifest = {
            "attempt_id": attempt_id,
            "status": status,
            "market_id": market_id,
            "partition_key": item.partition_key,
            "dataset": MARKET_STATUS_DATASET,
            "normalization_version": MARKET_STATUS_NORMALIZATION_VERSION,
            "schema_version": MARKET_STATUS_SCHEMA_VERSION,
            "capability_revision": capability_revision,
            "input_artifact_id": item.artifact.artifact_id,
            "input_endpoint_binding": {
                "endpoint_id": item.endpoint_id,
                "endpoint_revision": item.endpoint_revision,
            },
            "source_rows": item.artifact.source_rows,
            "observation_rows": rows,
            "ignored_rows": ignored,
            "ignored_detail": "other_channel_or_protocol_frame_compact_count",
            "rejected_rows": len(rejected),
            "output": output_storage,
        }
        manifest_path = output_dir / f"manifest-{attempt_id}.json"
        atomic_write_text(
            manifest_path,
            json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        )
        conn.execute("BEGIN IMMEDIATE")
        output_identity = artifact_id(output_sha)
        _register_content_artifact(
            conn, output_identity, "materialized_parquet", output_storage,
            output_sha, output_path.stat().st_size, finished,
            MARKET_STATUS_SCHEMA_VERSION,
        )
        conn.execute(
            "INSERT INTO materialization_output VALUES (?,?,?,?,?,?,?)",
            (
                attempt_id, output_identity, MARKET_STATUS_DATASET, rows,
                min_event or None, max_event or None, finished,
            ),
        )
        manifest_sha = sha256_file(manifest_path)
        _register_content_artifact(
            conn, artifact_id(manifest_sha), "materialization_manifest",
            _relative_storage_path(root, manifest_path), manifest_sha,
            manifest_path.stat().st_size, finished,
            MARKET_STATUS_SCHEMA_VERSION,
        )
        conn.execute(
            "INSERT INTO partition_input "
            "(attempt_id,artifact_id,source_rows,normalized_rows,ignored_rows,"
            "rejected_rows) VALUES (?,?,?,?,?,?)",
            (
                attempt_id, item.artifact.artifact_id,
                item.artifact.source_rows, rows, ignored, len(rejected),
            ),
        )
        conn.execute(
            "INSERT INTO partition_input_binding "
            "(attempt_id,artifact_id,storage_path,source_rows,normalized_rows,"
            "ignored_rows,rejected_rows) VALUES (?,?,?,?,?,?,?)",
            (
                attempt_id, item.artifact.artifact_id,
                item.artifact.storage_path, item.artifact.source_rows,
                rows, ignored, len(rejected),
            ),
        )
        conn.executemany(
            "INSERT INTO materialization_rejection VALUES (?,?,?,?,?,?)",
            [(
                attempt_id, item.artifact.artifact_id, source_row,
                f"{item.artifact.storage_path}:{source_row}", reason, finished,
            ) for source_row, reason in rejected],
        )
        conn.execute(
            "UPDATE partition_attempt SET status=?,normalized_rows=?,"
            "ignored_rows=?,rejected_rows=?,finished_at=? WHERE attempt_id=?",
            (status, rows, ignored, len(rejected), finished, attempt_id),
        )
        conn.execute(
            "INSERT INTO materialization_partition_head VALUES (?,?,?,?,?,?) "
            "ON CONFLICT(market_id,domain,partition_key) DO UPDATE SET "
            "normalization_version=excluded.normalization_version,"
            "attempt_id=excluded.attempt_id,activated_at=excluded.activated_at",
            (
                market_id, STATUS_DOMAIN, item.partition_key,
                MARKET_STATUS_NORMALIZATION_VERSION, attempt_id, finished,
            ),
        )
        register_materialized_raw_v3_observations(
            conn,
            endpoint_id=STATUS_ENDPOINT_ID,
            endpoint_revision=STATUS_ENDPOINT_REVISION,
            run_id=item.run_id,
            market_id=market_id,
            capability_venue_id="bitbank",
            capability_domain=STATUS_DOMAIN,
            capability_endpoint=STATUS_ENDPOINT,
            capability_revision=capability_revision,
            observations=channel_observations,
        )
        conn.commit()
        return MarketStatusResult(
            attempt_id, market_id, item.partition_key, status,
            item.artifact.source_rows, rows, ignored, len(rejected),
            output_storage, False,
        )
    except Exception as exc:
        conn.rollback()
        for path in (output_csv, output_temp):
            if path.exists():
                path.unlink()
        conn.execute(
            "UPDATE partition_attempt SET status='failed',finished_at=?,"
            "failure_detail=? WHERE attempt_id=? AND status='running'",
            (utc_now(), str(exc)[:2000], attempt_id),
        )
        conn.commit()
        raise


def materialize_all(
    root: Path, conn: sqlite3.Connection, *, report_reused: bool = True,
) -> list[MarketStatusResult]:
    """断点复用地物化所有含状态消息的 r1 封口段。"""
    inputs = sealed_status_inputs(root, conn)
    results: list[MarketStatusResult] = []
    for index, item in enumerate(inputs, start=1):
        result = materialize_segment(root, conn, item)
        results.append(result)
        if report_reused or not result.reused:
            print(
                f"[{index}/{len(inputs)}] "
                f"{'REUSED' if result.reused else 'DONE'} market_status "
                f"{result.market_id} {result.partition_key} "
                f"observations={result.observation_rows:,} "
                f"ignored={result.ignored_rows:,} "
                f"rejected={result.rejected_rows:,}",
                flush=True,
            )
    return results


def audit_market_status(
    root: Path, conn: sqlite3.Connection,
) -> dict[str, object]:
    """审计活动市场状态输出、来源分类与端点/频道绑定。"""
    errors: list[str] = []
    rows = conn.execute(
        "SELECT a.attempt_id,a.market_id,a.source_rows,a.normalized_rows,"
        "a.ignored_rows,a.rejected_rows,r.storage_path "
        "FROM materialization_partition_head h JOIN partition_attempt a "
        "ON a.attempt_id=h.attempt_id JOIN materialization_output o "
        "ON o.attempt_id=a.attempt_id JOIN artifact r "
        "ON r.artifact_id=o.artifact_id "
        "WHERE h.domain=? AND o.dataset=? ORDER BY a.attempt_id",
        (STATUS_DOMAIN, MARKET_STATUS_DATASET),
    ).fetchall()
    total = 0
    db: Any = duckdb.connect(":memory:")
    db.execute("SET TimeZone='UTC'")
    try:
        for row in rows:
            attempt, market = str(row[0]), str(row[1])
            source, normalized, ignored, rejected = map(int, row[2:6])
            if source != normalized + ignored + rejected:
                errors.append(f"来源分类不守恒: {attempt}")
            path = root / str(row[6])
            result = db.execute(
                "SELECT COUNT(*),COUNT(*)-COUNT(DISTINCT observation_id),"
                "SUM(available_time<event_time),COUNT(DISTINCT market_id),"
                "MIN(market_id),SUM(CASE WHEN endpoint_id!='EP-0005' OR "
                "endpoint_revision!=1 OR channel_id NOT LIKE "
                "'circuit_break_info_%' THEN 1 ELSE 0 END) "
                "FROM read_parquet(?)",
                [str(path)],
            ).fetchone()
            if result is None:
                errors.append(f"输出不可读: {attempt}")
                continue
            count = int(result[0])
            total += count
            if (
                count != normalized or int(result[1] or 0)
                or int(result[2] or 0) or int(result[5] or 0)
                or (count and (
                    int(result[3]) != 1 or str(result[4]) != market
                ))
            ):
                errors.append(f"事实键/PIT/身份失败: {attempt}")
    finally:
        db.close()
    return {
        "attempts": len(rows), "observations": total,
        "errors": errors, "ok": not errors,
    }


def main(argv: Sequence[str] | None = None) -> int:
    """独立维护/审计入口；常驻运行由 L2 watcher 调用。"""
    import argparse

    parser = argparse.ArgumentParser(description="bitbank 市场状态物化")
    parser.add_argument("--data-root", type=Path, default=None)
    parser.add_argument("command", choices=("all", "audit"))
    args = parser.parse_args(argv)
    root = (args.data_root or configured_data_root()).resolve()
    conn = store.connect(root)
    try:
        if args.command == "all":
            with sqlite_writer_lock(root):
                result: object = [
                    asdict(item) for item in materialize_all(root, conn)
                ]
            code = 0
        else:
            result = audit_market_status(root, conn)
            code = 0 if bool(result["ok"]) else 1
    finally:
        conn.close()
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
