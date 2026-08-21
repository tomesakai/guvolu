"""持久化审计、故障检测和隔离恢复探针。"""
import gzip
import json
import multiprocessing
import os
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path

import pytest

from guvolu.data import raw_writer
from guvolu.data.durable_io import atomic_write_text
from guvolu.data.heatmap_tiles import tile_chunk_path, tile_paths
from guvolu.data.persistence_audit import (
    AuditReport,
    audit_raw,
    audit_persistence,
    inspect_gzip_members,
    probe_recovery,
    recover_gzip_prefix,
)
from guvolu.data.raw_writer import RawWriter
from guvolu.data.store import connect, upsert_coverage
from guvolu.venues import archive
from guvolu.venues.collect import _save_cursor


def _concurrent_raw_worker(root: str, run_id: str, records: int) -> None:
    writer = RawWriter(Path(root), run_id=run_id)
    for at in range(records):
        writer.ws("nested/ws_public", "orderbooks", "BTC", {"at": at})
    writer.finish()


def _atomic_write_worker(path: str, body: str, writes: int) -> None:
    for _ in range(writes):
        atomic_write_text(Path(path), body)


def _write_valid_tile(root: Path) -> tuple[Path, Path]:
    rows = [
        {"e": 1786320000, "gap": False, "carried": False, "cells": []},
        {"e": 1786320001, "gap": True, "carried": False, "cells": []},
    ]
    daily, meta_path = tile_paths(root, "gmo", "BTC", "1s", "2026-08-10")
    daily.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(daily, "wt", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")
    chunk = tile_chunk_path(
        root, "gmo", "BTC", "1s", "2026-08-10", "generation-a", 1786319872
    )
    chunk.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(chunk, "wt", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")
    meta_path.write_text(
        json.dumps(
            {
                "date": "2026-08-10",
                "columns": 2,
                "gap_columns": 1,
                "carried_columns": 0,
                "chunk_columns": 512,
                "chunk_generation": "generation-a",
            }
        ),
        encoding="utf-8",
    )
    return meta_path, chunk


def _write_bitbank_archive(root: Path) -> None:
    body = json.dumps(
        {
            "data": {
                "transactions": [
                    {
                        "transaction_id": 1,
                        "executed_at": 1786320000000,
                        "side": "buy",
                        "price": "100",
                        "amount": "1",
                    }
                ]
            }
        }
    ).encode()
    path = archive.bitbank_day_path(root, "btc_jpy", "20260810")
    archive.write_gzip_atomic(path, body)
    stats = archive.bitbank_file_stats(path)
    conn = connect(root)
    upsert_coverage(
        conn,
        [
            (
                "bitbank",
                "btc_jpy",
                "trade",
                "20260810",
                stats.rows,
                stats.first_ts,
                stats.last_ts,
                "ok",
                "2026-08-10T00:00:00+00:00",
            )
        ],
    )
    conn.close()


def test_full_audit_cross_checks_raw_archive_db_and_tiles(tmp_path: Path) -> None:
    writer = RawWriter(tmp_path, run_id="run-persist")
    writer.rest("public", "gmo", "GET", "/ticker", None, 200, {"x": 1}, 1)
    writer.rest("public", "gmo", "GET", "/ticker", None, 200, {"x": 2}, 1)
    writer.finish()
    _write_bitbank_archive(tmp_path)
    _write_valid_tile(tmp_path)

    report = audit_persistence(tmp_path, "full")

    assert report.loss_detected is False
    assert report.counters["raw_records"] == 2
    assert report.counters["coverage_rows"] == 1
    assert report.counters["heatmap_columns"] == 2
    assert report.fully_proven is False
    assert {issue.code for issue in report.issues} >= {
        "archive_checksum_absent",
        "heatmap_source_hash_absent",
        "sqlite_cross_commit_recoverable",
    }
    assert not any(
        issue.code == "raw_legacy_no_durable_ack" for issue in report.issues
    )


def test_audit_detects_manifest_archive_and_generation_loss(tmp_path: Path) -> None:
    writer = RawWriter(tmp_path, run_id="run-damage")
    writer.rest("public", "gmo", "GET", "/ticker", None, 200, {}, 1)
    manifest = writer.finish()
    loaded = json.loads(manifest.read_text(encoding="utf-8"))
    loaded["record_counts"]["public"] = 2
    manifest.write_text(json.dumps(loaded), encoding="utf-8")
    _write_bitbank_archive(tmp_path)
    meta_path, chunk = _write_valid_tile(tmp_path)
    blob = chunk.read_bytes()
    chunk.write_bytes(blob[:-4])

    report = audit_persistence(tmp_path, "full")

    codes = {issue.code for issue in report.issues if issue.severity == "error"}
    assert "raw_manifest_count_shortfall" in codes
    assert "heatmap_corrupt" in codes
    assert report.loss_detected is True
    assert meta_path.exists()


def test_audit_detects_coverage_file_removed(tmp_path: Path) -> None:
    connect(tmp_path).close()
    conn = connect(tmp_path)
    upsert_coverage(
        conn,
        [
            (
                "gmo",
                "BTC",
                "trade",
                "20260810",
                1,
                "a",
                "b",
                "ok",
                "2026-08-10T00:00:00+00:00",
            )
        ],
    )
    conn.close()

    report = audit_persistence(tmp_path, "quick")

    assert any(issue.code == "coverage_file_missing" for issue in report.issues)


def test_gzip_torn_member_is_detected_and_prefix_recovered(tmp_path: Path) -> None:
    source = tmp_path / "joined.gz"
    source.write_bytes(gzip.compress(b"one\n") + gzip.compress(b"two\n")[:-3])

    state = inspect_gzip_members(source)
    recovered = tmp_path / "recovered.gz"
    result = recover_gzip_prefix(source, recovered)

    assert state.intact is False
    assert state.complete_members == 1
    assert result == state
    assert gzip.decompress(recovered.read_bytes()) == b"one\n"


def test_isolated_recovery_probe_is_repeatable(tmp_path: Path) -> None:
    first = probe_recovery(tmp_path, records=250)
    second = probe_recovery(tmp_path, records=250)

    assert first == second
    assert first["ok"] is True
    assert first["records_persisted"] == 250
    assert list(tmp_path.iterdir()) == []


def test_raw_count_only_advances_after_durable_append(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    writer = RawWriter(tmp_path, run_id="run-fsync")

    def fail_append(path: Path, body: bytes) -> None:
        del path, body
        raise OSError("simulated fsync failure")

    monkeypatch.setattr(raw_writer, "durable_append_bytes", fail_append)
    with pytest.raises(OSError, match="fsync"):
        writer.ws("ws_public", "orderbooks", "BTC", {"x": 1})
    monkeypatch.undo()

    manifest = json.loads(writer.finish().read_text(encoding="utf-8"))
    assert manifest["record_counts"] == {}
    assert manifest["durability_version"] == "fsync-per-record-v1"


def test_atomic_pointers_cursor_and_sqlite_foreign_keys(tmp_path: Path) -> None:
    pointer = tmp_path / "pointer.json"
    atomic_write_text(pointer, '{"generation":"a"}\n')
    atomic_write_text(pointer, '{"generation":"b"}\n')
    assert json.loads(pointer.read_text(encoding="utf-8")) == {
        "generation": "b"
    }
    assert not pointer.with_name(pointer.name + ".tmp").exists()

    cursor = tmp_path / "cursor.json"
    _save_cursor(cursor, {"before": 7})
    assert json.loads(cursor.read_text(encoding="utf-8")) == {
        "durability_version": "member-before-cursor-v1",
        "before": 7,
    }

    conn = connect(tmp_path / "db")
    assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
    assert conn.execute("PRAGMA synchronous").fetchone()[0] == 2
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO alert_event "
            "(feature_id, rule_id, triggered_at) VALUES (999, 'x', 't')"
        )
    conn.close()


def test_atomic_write_supports_parallel_writers(tmp_path: Path) -> None:
    """不同流派并行发布同一收据时不得争用临时文件。"""
    pointer = tmp_path / "shared-receipt.json"
    bodies = tuple(f'{{"writer":{index}}}\n' for index in range(32))

    with ThreadPoolExecutor(max_workers=8) as executor:
        tuple(executor.map(lambda body: atomic_write_text(pointer, body), bodies))

    assert pointer.read_text(encoding="utf-8") in bodies
    assert tuple(tmp_path.glob(".shared-receipt.json.tmp-*")) == ()
    if os.name != "nt":
        assert pointer.stat().st_mode & 0o777 == 0o644


def test_atomic_write_is_cross_process_serialized(tmp_path: Path) -> None:
    """不同进程同时替换一个指针时不得留下坏字节或临时文件。"""
    pointer = tmp_path / "shared-pointer.json"
    bodies = ('{"writer":"a"}\n', '{"writer":"b"}\n')
    context = multiprocessing.get_context("spawn")
    processes = [
        context.Process(
            target=_atomic_write_worker,
            args=(str(pointer), body, 24),
        )
        for body in bodies
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=30)
        assert process.exitcode == 0
    assert pointer.read_text(encoding="utf-8") in bodies
    assert tuple(tmp_path.glob(".shared-pointer.json.tmp-*")) == ()


def test_raw_append_is_cross_process_serialized(tmp_path: Path) -> None:
    context = multiprocessing.get_context("spawn")
    processes = [
        context.Process(
            target=_concurrent_raw_worker,
            args=(str(tmp_path), f"run-{at}", 12),
        )
        for at in range(3)
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=30)
        assert process.exitcode == 0

    report = AuditReport("full", datetime.now(UTC).isoformat())
    audit_raw(tmp_path, report)
    assert report.counters["raw_records"] == 36
    assert not [issue for issue in report.issues if issue.severity == "error"]
