"""为多节拍研究构造同一收据的最小只读数据根。"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path

from guvolu.data.durable_io import atomic_write_text
from guvolu.data.store import DB_FILE_NAME, connect_readonly
from guvolu.research.panel import (
    attest_trade_input_receipt,
    capture_trade_input_receipt,
)
from guvolu.research.provenance import canonical_json, sha256_file

SUITE_DATA_SNAPSHOT_METHOD_VERSION = "hardlinked-minimal-control-plane-v1"
_TABLES = (
    "instrument",
    "instrument_map",
    "market",
    "partition_attempt",
    "artifact",
    "materialization_output",
    "materialization_partition_head",
    "l2_quality_window",
)


def _mapping(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} 必须为对象")
    return {str(key): item for key, item in value.items()}


def _text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} 必须为非空文本")
    return value.strip()


def _strings(value: object, name: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{name} 必须为非空数组")
    result = tuple(_text(item, name) for item in value)
    if result != tuple(sorted(set(result))):
        raise ValueError(f"{name} 必须有序且不重复")
    return result


def _create_table(
    source: sqlite3.Connection,
    target: sqlite3.Connection,
    table: str,
) -> None:
    row = source.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    if row is None or not isinstance(row[0], str):
        raise ValueError(f"源控制面缺少表: {table}")
    target.execute(row[0])


def _copy_rows(
    source: sqlite3.Connection,
    target: sqlite3.Connection,
    table: str,
    where: str,
    parameters: Sequence[object],
) -> int:
    rows = source.execute(
        f"SELECT * FROM {table} WHERE {where}", parameters,
    ).fetchall()
    if not rows:
        return 0
    placeholders = ",".join("?" for _ in range(len(rows[0])))
    target.executemany(
        f"INSERT INTO {table} VALUES ({placeholders})", rows,
    )
    return len(rows)


def _marks(values: Sequence[str]) -> str:
    return ",".join("?" for _ in values)


def _build_minimal_control_plane(
    source_root: Path,
    target_root: Path,
    market_ids: Sequence[str],
    entries: Sequence[Mapping[str, object]],
) -> Mapping[str, int]:
    ordered_markets = tuple(sorted(set(market_ids)))
    attempt_ids = tuple(sorted({
        _text(entry.get("attempt_id"), "attempt_id") for entry in entries
    }))
    artifact_ids = tuple(sorted({
        _text(entry.get("artifact_id"), "artifact_id") for entry in entries
    }))
    source = connect_readonly(source_root)
    if source is None:
        raise LookupError("源数据根缺少控制面数据库")
    source.row_factory = sqlite3.Row
    target_path = target_root / DB_FILE_NAME
    target = sqlite3.connect(target_path)
    counts: dict[str, int] = {}
    try:
        target.execute("PRAGMA foreign_keys=OFF")
        for table in _TABLES:
            _create_table(source, target, table)
        market_rows = source.execute(
            f"SELECT market_id,venue_id,venue_symbol,instrument_id,"
            f"mapping_revision FROM market WHERE market_id IN "
            f"({_marks(ordered_markets)})",
            ordered_markets,
        ).fetchall()
        if len(market_rows) != len(ordered_markets):
            raise LookupError("源控制面缺少 suite 市场")
        instrument_ids = tuple(sorted({
            str(row["instrument_id"]) for row in market_rows
        }))
        venue_ids = tuple(sorted({str(row["venue_id"]) for row in market_rows}))
        counts["instrument"] = _copy_rows(
            source,
            target,
            "instrument",
            f"instrument_id IN ({_marks(instrument_ids)})",
            instrument_ids,
        )
        counts["instrument_map"] = _copy_rows(
            source,
            target,
            "instrument_map",
            f"venue_id IN ({_marks(venue_ids)})",
            venue_ids,
        )
        counts["market"] = _copy_rows(
            source,
            target,
            "market",
            f"market_id IN ({_marks(ordered_markets)})",
            ordered_markets,
        )
        counts["partition_attempt"] = _copy_rows(
            source,
            target,
            "partition_attempt",
            f"attempt_id IN ({_marks(attempt_ids)})",
            attempt_ids,
        )
        counts["artifact"] = _copy_rows(
            source,
            target,
            "artifact",
            f"artifact_id IN ({_marks(artifact_ids)})",
            artifact_ids,
        )
        counts["materialization_output"] = _copy_rows(
            source,
            target,
            "materialization_output",
            f"attempt_id IN ({_marks(attempt_ids)}) "
            f"AND artifact_id IN ({_marks(artifact_ids)})",
            (*attempt_ids, *artifact_ids),
        )
        counts["materialization_partition_head"] = _copy_rows(
            source,
            target,
            "materialization_partition_head",
            f"market_id IN ({_marks(ordered_markets)}) "
            f"AND attempt_id IN ({_marks(attempt_ids)})",
            (*ordered_markets, *attempt_ids),
        )
        counts["l2_quality_window"] = _copy_rows(
            source,
            target,
            "l2_quality_window",
            f"market_id IN ({_marks(ordered_markets)})",
            ordered_markets,
        )
        if (
            counts["instrument"] != len(instrument_ids)
            or counts["market"] != len(ordered_markets)
            or counts["partition_attempt"] != len(attempt_ids)
            or counts["artifact"] != len(artifact_ids)
        ):
            raise ValueError("最小控制面没有精确覆盖输入收据")
        user_version = int(source.execute("PRAGMA user_version").fetchone()[0])
        target.execute(f"PRAGMA user_version={user_version}")
        target.commit()
        check = target.execute("PRAGMA integrity_check").fetchone()
        if check is None or check[0] != "ok":
            raise ValueError("快照控制面 integrity_check 失败")
    finally:
        target.close()
        source.close()
    return counts


def _active_shadow_entries(
    source_root: Path,
    market_ids: Sequence[str],
) -> tuple[Mapping[str, object], ...]:
    """冻结完整管线读取的 L2、book-state 活动输出。"""
    ordered = tuple(sorted(set(market_ids)))
    if not ordered:
        return ()
    connection = connect_readonly(source_root)
    if connection is None:
        raise LookupError("源数据根缺少控制面数据库")
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            "SELECT h.market_id,h.domain,h.partition_key,"
            "h.normalization_version,h.attempt_id,o.dataset,o.artifact_id,"
            "a.storage_path,a.sha256,a.byte_count,o.row_count,"
            "o.min_event_time,o.max_event_time "
            "FROM materialization_partition_head h "
            "JOIN materialization_output o ON o.attempt_id=h.attempt_id "
            "JOIN artifact a ON a.artifact_id=o.artifact_id "
            f"WHERE h.market_id IN ({_marks(ordered)}) "
            "AND h.domain IN ('book_l2','book_state') "
            "AND o.dataset IN ('book_l2_frame','book_l2_level',"
            "'book_state_checkpoint')",
            ordered,
        ).fetchall()
    finally:
        connection.close()
    entries: list[Mapping[str, object]] = []
    for row in rows:
        path = (source_root / str(row["storage_path"])).resolve()
        digest = str(row["sha256"])
        if (
            not path.is_relative_to(source_root)
            or not path.is_file()
            or sha256_file(path) != digest
            or str(row["artifact_id"]) != f"sha256-{digest}"
            or path.stat().st_size != int(row["byte_count"])
        ):
            raise ValueError("L2 suite 快照源制品身份不一致")
        entries.append({str(key): row[key] for key in row.keys()})
    if not entries:
        raise LookupError("suite 市场没有 L2/book-state 活动输出")
    return tuple(entries)


def _link_artifacts(
    source_root: Path,
    target_root: Path,
    entries: Sequence[Mapping[str, object]],
) -> tuple[Mapping[str, object], ...]:
    records: list[Mapping[str, object]] = []
    seen: set[str] = set()
    for entry in entries:
        relative = _text(entry.get("storage_path"), "storage_path")
        if relative in seen:
            continue
        seen.add(relative)
        source = (source_root / relative).resolve()
        target = (target_root / relative).resolve()
        if not source.is_relative_to(source_root) or not target.is_relative_to(
            target_root
        ):
            raise ValueError("输入收据路径越出数据根")
        if not source.is_file():
            raise ValueError("输入收据源制品不存在")
        digest = _text(entry.get("sha256"), "entry.sha256")
        if sha256_file(source) != digest:
            raise ValueError("输入收据源制品散列不一致")
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.link(source, target)
        except OSError as error:
            raise OSError("suite 数据快照要求同卷硬链接") from error
        if sha256_file(target) != digest:
            raise ValueError("硬链接后的制品散列不一致")
        records.append({
            "storage_path": relative,
            "sha256": digest,
            "bytes": target.stat().st_size,
        })
    return tuple(sorted(records, key=lambda item: str(item["storage_path"])))


def create_suite_data_snapshot(
    source_data_root: Path,
    market_id: str,
    output_base: Path,
    shadow_market_ids: Sequence[str] = (),
) -> Path:
    """冻结当前 trade head，并发布可供多个节拍复用的数据根。"""
    source_root = source_data_root.resolve()
    output = output_base.resolve()
    output.mkdir(parents=True, exist_ok=True)
    receipt_directory = output / "source-receipts"
    inputs = capture_trade_input_receipt(
        source_root, market_id, receipt_directory,
    )
    if inputs.receipt_path is None or inputs.receipt_sha256 is None:
        raise AssertionError("活动 head 没有生成输入收据")
    attest_trade_input_receipt(
        source_root, inputs.receipt_path, require_current_head=True,
    )
    receipt = _mapping(json.loads(
        inputs.receipt_path.read_text(encoding="utf-8"),
    ), "receipt")
    raw_trade_entries = receipt.get("entries")
    if not isinstance(raw_trade_entries, list):
        raise ValueError("输入收据缺少 entries")
    trade_entries = tuple(
        _mapping(entry, "receipt.entry") for entry in raw_trade_entries
    )
    selected_markets = tuple(sorted(set((market_id, *shadow_market_ids))))
    shadow_entries = _active_shadow_entries(source_root, selected_markets)
    all_entries = (*trade_entries, *shadow_entries)
    snapshot_input = {
        "trade_receipt_sha256": inputs.receipt_sha256,
        "shadow_market_ids": selected_markets,
        "shadow_outputs": sorted((
            {
                "market_id": entry.get("market_id"),
                "domain": entry.get("domain"),
                "partition_key": entry.get("partition_key"),
                "dataset": entry.get("dataset"),
                "attempt_id": entry.get("attempt_id"),
                "artifact_id": entry.get("artifact_id"),
                "sha256": entry.get("sha256"),
            }
            for entry in shadow_entries
        ), key=lambda item: tuple(str(item[key]) for key in sorted(item))),
    }
    snapshot_digest = hashlib.sha256(
        canonical_json(snapshot_input).encode("utf-8"),
    ).hexdigest()
    snapshot_name = f"suite-data-snapshot-sha256-{snapshot_digest}"
    snapshot_root = output / snapshot_name
    if snapshot_root.exists():
        recaptured = capture_trade_input_receipt(
            snapshot_root, market_id, snapshot_root / "receipts",
        )
        if recaptured.receipt_sha256 != inputs.receipt_sha256:
            raise ValueError("既有 suite 数据快照与当前收据身份不一致")
        return snapshot_root
    with tempfile.TemporaryDirectory(
        dir=output, prefix=f".{snapshot_name}.tmp-",
    ) as temporary:
        temporary_root = Path(temporary)
        counts = _build_minimal_control_plane(
            source_root, temporary_root, selected_markets, all_entries,
        )
        artifacts = _link_artifacts(source_root, temporary_root, all_entries)
        recaptured = capture_trade_input_receipt(
            temporary_root, market_id, temporary_root / "receipts",
        )
        if recaptured.receipt_sha256 != inputs.receipt_sha256:
            raise ValueError("suite 数据快照不能重建相同活动 head 收据")
        body: dict[str, object] = {
            "schema_version": 1,
            "method_version": SUITE_DATA_SNAPSHOT_METHOD_VERSION,
            "market_id": market_id,
            "head_generation": inputs.head_generation,
            "input_receipt_sha256": inputs.receipt_sha256,
            "shadow_market_ids": selected_markets,
            "snapshot_input": snapshot_input,
            "control_plane_rows": counts,
            "artifacts": artifacts,
        }
        body["snapshot_identity"] = "suite-data-snapshot-" + hashlib.sha256(
            canonical_json(body).encode("utf-8"),
        ).hexdigest()
        atomic_write_text(
            temporary_root / "snapshot-manifest.json",
            canonical_json(body) + "\n",
        )
        os.replace(temporary_root, snapshot_root)
    return snapshot_root
