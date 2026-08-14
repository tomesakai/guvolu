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
    receipt: Mapping[str, object],
) -> Mapping[str, int]:
    market_id = _text(receipt.get("market_id"), "market_id")
    attempt_ids = _strings(receipt.get("attempt_ids"), "attempt_ids")
    artifact_ids = _strings(receipt.get("artifact_ids"), "artifact_ids")
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
        market = source.execute(
            "SELECT market_id,venue_id,venue_symbol,instrument_id,"
            "mapping_revision FROM market WHERE market_id=?",
            (market_id,),
        ).fetchone()
        if market is None:
            raise LookupError(f"源控制面缺少市场: {market_id}")
        counts["instrument"] = _copy_rows(
            source, target, "instrument", "instrument_id=?",
            (market["instrument_id"],),
        )
        counts["instrument_map"] = _copy_rows(
            source,
            target,
            "instrument_map",
            "venue_id=? AND venue_symbol=? AND revision_id=?",
            (
                market["venue_id"], market["venue_symbol"],
                market["mapping_revision"],
            ),
        )
        counts["market"] = _copy_rows(
            source, target, "market", "market_id=?", (market_id,),
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
            f"market_id=? AND attempt_id IN ({_marks(attempt_ids)})",
            (market_id, *attempt_ids),
        )
        if (
            counts["instrument"] != 1
            or counts["market"] != 1
            or counts["partition_attempt"] != len(attempt_ids)
            or counts["artifact"] != len(artifact_ids)
            or counts["materialization_output"] != len(artifact_ids)
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


def _link_artifacts(
    source_root: Path,
    target_root: Path,
    receipt: Mapping[str, object],
) -> tuple[Mapping[str, object], ...]:
    raw_entries = receipt.get("entries")
    if not isinstance(raw_entries, list) or not raw_entries:
        raise ValueError("输入收据缺少 entries")
    records: list[Mapping[str, object]] = []
    seen: set[str] = set()
    for raw_entry in raw_entries:
        entry = _mapping(raw_entry, "receipt.entry")
        relative = _text(entry.get("storage_path"), "storage_path")
        if relative in seen:
            raise ValueError("输入收据包含重复 storage_path")
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
    snapshot_name = f"suite-data-snapshot-sha256-{inputs.receipt_sha256}"
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
            source_root, temporary_root, receipt,
        )
        artifacts = _link_artifacts(source_root, temporary_root, receipt)
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
