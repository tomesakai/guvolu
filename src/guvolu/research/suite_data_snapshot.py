"""为多节拍研究构造同一收据的最小只读数据根。"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path

from guvolu.data.durable_io import atomic_write_text
from guvolu.data.store import DB_FILE_NAME, connect_readonly
from guvolu.research.panel import (
    attest_trade_input_receipt,
    capture_trade_input_receipt,
)
from guvolu.research.provenance import canonical_json, sha256_file

SUITE_DATA_SNAPSHOT_SCHEMA_VERSION = 2
SUITE_DATA_SNAPSHOT_METHOD_VERSION = "hardlinked-minimal-control-plane-v2"
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


def _read_snapshot_manifest(snapshot_root: Path) -> Mapping[str, object]:
    manifest_path = snapshot_root / "snapshot-manifest.json"
    if not manifest_path.is_file():
        raise ValueError("suite 数据快照缺少 snapshot-manifest.json")
    text = manifest_path.read_text(encoding="utf-8")
    manifest = _mapping(json.loads(text), "snapshot_manifest")
    if text != canonical_json(manifest) + "\n":
        raise ValueError("suite 数据快照 manifest 不是规范 JSON")
    return manifest


def attest_suite_data_snapshot(snapshot_root: Path) -> Mapping[str, object]:
    """逐字节验明共享控制面、全部硬链接制品和成交收据。"""
    root = snapshot_root.resolve()
    manifest = _read_snapshot_manifest(root)
    if manifest.get("schema_version") != SUITE_DATA_SNAPSHOT_SCHEMA_VERSION:
        raise ValueError("suite 数据快照 schema 不受支持")
    if manifest.get("method_version") != SUITE_DATA_SNAPSHOT_METHOD_VERSION:
        raise ValueError("suite 数据快照方法版本不受支持")
    recorded_identity = _text(
        manifest.get("snapshot_identity"), "snapshot_identity",
    )
    identity_body = dict(manifest)
    identity_body.pop("snapshot_identity", None)
    expected_identity = "suite-data-snapshot-" + hashlib.sha256(
        canonical_json(identity_body).encode("utf-8"),
    ).hexdigest()
    if recorded_identity != expected_identity:
        raise ValueError("suite 数据快照身份散列不一致")
    snapshot_input = _mapping(manifest.get("snapshot_input"), "snapshot_input")
    expected_directory = "suite-data-snapshot-sha256-" + hashlib.sha256(
        canonical_json(snapshot_input).encode("utf-8"),
    ).hexdigest()
    if root.name != expected_directory:
        raise ValueError("suite 数据快照目录身份不一致")

    raw_artifacts = manifest.get("artifacts")
    if not isinstance(raw_artifacts, list) or not raw_artifacts:
        raise ValueError("suite 数据快照缺少制品清单")
    artifact_records = tuple(
        _mapping(value, "snapshot.artifact") for value in raw_artifacts
    )
    paths = tuple(
        _text(record.get("storage_path"), "artifact.storage_path")
        for record in artifact_records
    )
    if paths != tuple(sorted(set(paths))):
        raise ValueError("suite 数据快照制品路径必须有序且不重复")
    for record, relative in zip(artifact_records, paths, strict=True):
        path = (root / relative).resolve()
        if not path.is_relative_to(root) or not path.is_file():
            raise ValueError("suite 数据快照制品不存在或越出数据根")
        digest = _text(record.get("sha256"), "artifact.sha256")
        byte_count = record.get("bytes")
        if (
            not isinstance(byte_count, int)
            or isinstance(byte_count, bool)
            or byte_count < 0
            or path.stat().st_size != byte_count
            or sha256_file(path) != digest
        ):
            raise ValueError("suite 数据快照制品完整性不一致")

    control = _mapping(manifest.get("control_plane"), "control_plane")
    control_path = (root / _text(control.get("path"), "control_plane.path")).resolve()
    if not control_path.is_relative_to(root) or not control_path.is_file():
        raise ValueError("suite 数据快照缺少控制面数据库")
    control_bytes = control.get("bytes")
    if (
        not isinstance(control_bytes, int)
        or isinstance(control_bytes, bool)
        or control_bytes <= 0
        or control_path.stat().st_size != control_bytes
        or sha256_file(control_path)
        != _text(control.get("sha256"), "control_plane.sha256")
    ):
        raise ValueError("suite 数据快照控制面散列不一致")
    connection = sqlite3.connect(f"file:{control_path.as_posix()}?mode=ro", uri=True)
    try:
        check = connection.execute("PRAGMA integrity_check").fetchone()
        if check is None or check[0] != "ok":
            raise ValueError("suite 数据快照控制面 integrity_check 失败")
        expected_counts = _mapping(
            manifest.get("control_plane_rows"), "control_plane_rows",
        )
        for table in _TABLES:
            expected = expected_counts.get(table)
            actual = int(connection.execute(
                f"SELECT COUNT(*) FROM {table}",
            ).fetchone()[0])
            if not isinstance(expected, int) or isinstance(expected, bool):
                raise ValueError("suite 数据快照控制面行数合同无效")
            if actual != expected:
                raise ValueError("suite 数据快照控制面行数不一致")
    finally:
        connection.close()

    market_id = _text(manifest.get("market_id"), "market_id")
    with tempfile.TemporaryDirectory(prefix="guvolu-suite-attest-") as temporary:
        recaptured = capture_trade_input_receipt(
            root, market_id, Path(temporary) / "receipts",
        )
    if (
        recaptured.receipt_sha256
        != _text(manifest.get("input_receipt_sha256"), "input_receipt_sha256")
        or recaptured.head_generation
        != _text(manifest.get("head_generation"), "head_generation")
    ):
        raise ValueError("suite 数据快照成交活动 head 不一致")
    return manifest


def suite_data_snapshot_record(snapshot_root: Path) -> Mapping[str, object] | None:
    """若数据根是 suite 快照，返回可写入研究 manifest 的受保护身份。"""
    root = snapshot_root.resolve()
    manifest_path = root / "snapshot-manifest.json"
    if not manifest_path.exists():
        return None
    manifest = attest_suite_data_snapshot(root)
    return {
        "schema_version": SUITE_DATA_SNAPSHOT_SCHEMA_VERSION,
        "method_version": manifest["method_version"],
        "snapshot_identity": manifest["snapshot_identity"],
        "manifest_sha256": sha256_file(manifest_path),
    }


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
    return _insert_rows(target, table, rows)


def _insert_rows(
    target: sqlite3.Connection, table: str, rows: Sequence[sqlite3.Row],
) -> int:
    if not rows:
        return 0
    placeholders = ",".join("?" for _ in range(len(rows[0])))
    target.executemany(
        f"INSERT INTO {table} VALUES ({placeholders})", rows,
    )
    return len(rows)


# 单条查询的键参数分块上限
_IN_CHUNK_SIZE = 800


def _copy_rows_chunked(
    source: sqlite3.Connection,
    target: sqlite3.Connection,
    table: str,
    key_column: str,
    values: Sequence[str],
    *,
    row_filter: Callable[[sqlite3.Row], bool] | None = None,
) -> int:
    """按键分块复制，绕开 SQLite 变量数上限。

    复合条件由 ``row_filter`` 在读出后判定，语义与整句 AND 相同；
    键集合去重排序由调用方保证。
    """
    total = 0
    for start in range(0, len(values), _IN_CHUNK_SIZE):
        chunk = tuple(values[start:start + _IN_CHUNK_SIZE])
        rows = source.execute(
            f"SELECT * FROM {table} WHERE {key_column} IN ({_marks(chunk)})",
            chunk,
        ).fetchall()
        if row_filter is not None:
            rows = [row for row in rows if row_filter(row)]
        total += _insert_rows(target, table, rows)
    return total


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
        counts["partition_attempt"] = _copy_rows_chunked(
            source, target, "partition_attempt", "attempt_id", attempt_ids,
        )
        counts["artifact"] = _copy_rows_chunked(
            source, target, "artifact", "artifact_id", artifact_ids,
        )
        artifact_id_set = frozenset(artifact_ids)
        counts["materialization_output"] = _copy_rows_chunked(
            source, target, "materialization_output", "attempt_id",
            attempt_ids,
            row_filter=lambda row: str(row["artifact_id"]) in artifact_id_set,
        )
        market_id_set = frozenset(ordered_markets)
        counts["materialization_partition_head"] = _copy_rows_chunked(
            source, target, "materialization_partition_head", "attempt_id",
            attempt_ids,
            row_filter=lambda row: str(row["market_id"]) in market_id_set,
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
        "method_version": SUITE_DATA_SNAPSHOT_METHOD_VERSION,
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
        existing = attest_suite_data_snapshot(snapshot_root)
        if canonical_json(existing.get("snapshot_input")) != canonical_json(
            snapshot_input
        ):
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
        control_path = temporary_root / DB_FILE_NAME
        body: dict[str, object] = {
            "schema_version": SUITE_DATA_SNAPSHOT_SCHEMA_VERSION,
            "method_version": SUITE_DATA_SNAPSHOT_METHOD_VERSION,
            "market_id": market_id,
            "head_generation": inputs.head_generation,
            "input_receipt_sha256": inputs.receipt_sha256,
            "shadow_market_ids": selected_markets,
            "snapshot_input": snapshot_input,
            "control_plane_rows": counts,
            "control_plane": {
                "path": DB_FILE_NAME,
                "sha256": sha256_file(control_path),
                "bytes": control_path.stat().st_size,
            },
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
    attest_suite_data_snapshot(snapshot_root)
    return snapshot_root
