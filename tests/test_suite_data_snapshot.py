"""多节拍共享输入数据快照测试。"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from pathlib import Path

from guvolu.data.store import DB_FILE_NAME
from guvolu.research.panel import capture_trade_input_receipt
from guvolu.research.suite_data_snapshot import create_suite_data_snapshot


def _source_data_root(root: Path) -> tuple[Path, Path]:
    data_root = root / "source"
    data_root.mkdir()
    artifact = data_root / "materialized" / "trade" / "part.parquet"
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(b"immutable-parquet-fixture")
    digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
    connection = sqlite3.connect(data_root / DB_FILE_NAME)
    connection.executescript("""
        CREATE TABLE instrument(
            instrument_id TEXT PRIMARY KEY, base TEXT, quote TEXT, kind TEXT
        );
        CREATE TABLE instrument_map(
            venue_id TEXT, venue_symbol TEXT, revision_id INTEGER,
            tick_size TEXT, size_step TEXT, min_size TEXT
        );
        CREATE TABLE market(
            market_id TEXT PRIMARY KEY, venue_id TEXT, venue_symbol TEXT,
            instrument_id TEXT, mapping_revision INTEGER, market_kind TEXT
        );
        CREATE TABLE partition_attempt(
            attempt_id TEXT PRIMARY KEY, market_id TEXT, domain TEXT,
            partition_key TEXT, normalization_version TEXT, status TEXT
        );
        CREATE TABLE artifact(
            artifact_id TEXT PRIMARY KEY, storage_path TEXT, sha256 TEXT,
            byte_count INTEGER
        );
        CREATE TABLE materialization_output(
            attempt_id TEXT, dataset TEXT, artifact_id TEXT, row_count INTEGER,
            min_event_time TEXT, max_event_time TEXT
        );
        CREATE TABLE materialization_partition_head(
            market_id TEXT, domain TEXT, partition_key TEXT,
            normalization_version TEXT, attempt_id TEXT
        );
    """)
    connection.execute(
        "INSERT INTO instrument VALUES (?,?,?,?)",
        ("instrument", "BTC", "JPY", "spot"),
    )
    connection.execute(
        "INSERT INTO instrument_map VALUES (?,?,?,?,?,?)",
        ("venue", "BTC_JPY", 0, "1", "0.0001", "0.0001"),
    )
    connection.execute(
        "INSERT INTO market VALUES (?,?,?,?,?,?)",
        ("market", "venue", "BTC_JPY", "instrument", 0, "spot"),
    )
    connection.execute(
        "INSERT INTO partition_attempt VALUES (?,?,?,?,?,?)",
        ("attempt", "market", "trade", "20260101", "trade-v1", "complete"),
    )
    connection.execute(
        "INSERT INTO artifact VALUES (?,?,?,?)",
        (
            f"sha256-{digest}", "materialized/trade/part.parquet",
            digest, artifact.stat().st_size,
        ),
    )
    connection.execute(
        "INSERT INTO materialization_output VALUES (?,?,?,?,?,?)",
        (
            "attempt", "trade_observation", f"sha256-{digest}", 1,
            "2026-01-01T00:00:00+00:00", "2026-01-01T00:00:00+00:00",
        ),
    )
    connection.execute(
        "INSERT INTO materialization_partition_head VALUES (?,?,?,?,?)",
        ("market", "trade", "20260101", "trade-v1", "attempt"),
    )
    connection.commit()
    connection.close()
    return data_root, artifact


def test_suite_data_snapshot_is_idempotent_and_hardlinked(
    tmp_path: Path,
) -> None:
    """快照必须重建相同 head，且不复制不可变 Parquet 字节。"""
    source, source_artifact = _source_data_root(tmp_path)
    output = tmp_path / "snapshots"
    first = create_suite_data_snapshot(source, "market", output)
    second = create_suite_data_snapshot(source, "market", output)
    assert first == second
    manifest = json.loads(
        (first / "snapshot-manifest.json").read_text(encoding="utf-8"),
    )
    assert manifest["control_plane_rows"] == {
        "instrument": 1,
        "instrument_map": 1,
        "market": 1,
        "partition_attempt": 1,
        "artifact": 1,
        "materialization_output": 1,
        "materialization_partition_head": 1,
    }
    linked = first / "materialized" / "trade" / "part.parquet"
    assert os.path.samefile(source_artifact, linked)
    recaptured = capture_trade_input_receipt(
        first, "market", first / "second-receipts",
    )
    assert recaptured.receipt_sha256 == manifest["input_receipt_sha256"]
