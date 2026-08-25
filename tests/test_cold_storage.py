from __future__ import annotations

import gzip
import hashlib
import io
import json
import shutil
import sqlite3
import sys
import tarfile
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from pathlib import Path
from typing import Any, Callable, cast

import pytest

from guvolu.data import store
from guvolu.data.cold_storage import (
    activate_plan,
    copy_plan,
    create_plan,
    release_hot_plan,
    restore_hot_from_raw_plan,
    restore_hot_plan,
    rollback_plan,
    verify_plan,
)
from guvolu.data.cold_storage import main as cold_storage_main
from guvolu.data.materialize import (
    artifact_id,
    materialize_archive_trade_month,
)
from guvolu.data.okx_l2_materialize import (
    materialize_archive as materialize_okx_l2_archive,
    sealed_input as okx_sealed_input,
)
from guvolu.data.store import upsert_coverage
from guvolu.data.storage_paths import MARKER_FILE_NAME, StoragePathError


def _write_json(path: Path, body: object) -> str:
    payload = (json.dumps(body, sort_keys=True) + "\n").encode()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return hashlib.sha256(payload).hexdigest()


def _fixture(tmp_path: Path) -> tuple[Path, Path, str]:
    root = tmp_path / "data"
    cold = tmp_path / "cold"
    root.mkdir()
    marker_sha = _write_json(cold / MARKER_FILE_NAME, {
        "role": "test",
        "schema_version": 1,
        "storage_root_id": "storage-root__test__v1",
    })
    prefix = "materialized/archive"
    _write_json(root / "storage-roots.json", {
        "routes": [{
            "logical_prefix": prefix,
            "physical_prefix": "artifacts/archive",
            "status": "planned",
            "storage_root_id": "storage-root__test__v1",
        }],
        "roots": [{
            "filesystem": None,
            "marker_sha256": marker_sha,
            "mount_path": str(cold),
            "partition_guid": None,
            "role": "test",
            "storage_root_id": "storage-root__test__v1",
            "volume_guid": None,
            "volume_label": None,
        }],
        "schema_version": 1,
    })
    conn = store.connect(root)
    for name, payload, kind in (
        ("a.parquet", b"aaa", "materialized_parquet"),
        ("b.json", b"bbb", "materialization_manifest"),
    ):
        path = root / prefix / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        sha = hashlib.sha256(payload).hexdigest()
        store.register_artifact(conn, (
            artifact_id(sha), kind, f"{prefix}/{name}",
            sha, len(payload), "2026-08-21T00:00:00+00:00",
            "2026-08-21T00:00:00+00:00", "sha256", 1,
        ))
    conn.close()
    return root, cold, prefix


def test_copy_verify_activate_and_rollback_are_replayable(tmp_path: Path) -> None:
    root, cold, prefix = _fixture(tmp_path)
    plan, path = create_plan(root, prefix)

    first = copy_plan(root, path)
    second = copy_plan(root, path)
    assert first["copied_bytes"] == plan.total_bytes
    assert second["reused_bytes"] == plan.total_bytes
    assert verify_plan(root, path, side="both")["status"] == "verified"

    assert activate_plan(root, path)["status"] == "active"
    assert (cold / "artifacts/archive/a.parquet").is_file()
    assert rollback_plan(root, path)["status"] == "planned"


def test_hot_release_is_dry_run_first_and_keeps_manifest(tmp_path: Path) -> None:
    root, cold, prefix = _fixture(tmp_path)
    plan, path = create_plan(root, prefix)
    copy_plan(root, path)
    activate_plan(root, path)

    dry_run = release_hot_plan(root, path)
    assert dry_run["releasable_bytes"] == 3
    assert (root / prefix / "a.parquet").is_file()
    with pytest.raises(StoragePathError, match="精确确认"):
        release_hot_plan(root, path, apply=True)

    applied = release_hot_plan(
        root, path, apply=True, confirm_migration_id=plan.migration_id,
    )

    assert applied["released_bytes"] == 3
    assert not (root / prefix / "a.parquet").exists()
    assert (root / prefix / "b.json").is_file()
    assert (cold / "artifacts/archive/a.parquet").is_file()
    assert release_hot_plan(
        root, path, apply=True, confirm_migration_id=plan.migration_id,
    )["absent_items"] == 1


def test_plan_rejects_unregistered_file(tmp_path: Path) -> None:
    root, _, prefix = _fixture(tmp_path)
    (root / prefix / "orphan.parquet").write_bytes(b"orphan")

    with pytest.raises(StoragePathError, match="不闭合"):
        create_plan(root, prefix)


def test_restore_hot_recovers_released_parquet_then_rollback_works(
    tmp_path: Path,
) -> None:
    root, cold, prefix = _fixture(tmp_path)
    plan, path = create_plan(root, prefix)
    copy_plan(root, path)
    activate_plan(root, path)
    release_hot_plan(
        root, path, apply=True, confirm_migration_id=plan.migration_id,
    )
    assert not (root / prefix / "a.parquet").exists()

    dry_run = restore_hot_plan(root, path)
    assert dry_run["restorable_bytes"] == 3
    assert not (root / prefix / "a.parquet").exists()

    applied = restore_hot_plan(root, path, apply=True)
    assert applied["restored_items"] == 1
    assert (root / prefix / "a.parquet").read_bytes() == b"aaa"
    # 重复恢复幂等
    assert restore_hot_plan(root, path, apply=True)["present_items"] == 1
    assert verify_plan(root, path, side="both")["status"] == "verified"
    assert rollback_plan(root, path)["status"] == "planned"


def test_restore_from_raw_shard_filters_candidates(tmp_path: Path) -> None:
    root, _, prefix = _fixture(tmp_path)
    _, path = create_plan(root, prefix)
    # 单候选两分片校验
    with pytest.raises(StoragePathError, match="没有可恢复"):
        restore_hot_from_raw_plan(root, path, shard=(1, 2))
    with pytest.raises(StoragePathError, match="分片参数非法"):
        restore_hot_from_raw_plan(root, path, shard=(2, 2))
    result = restore_hot_from_raw_plan(root, path, shard=(0, 2))
    assert result["shard"] == [0, 2]
    assert result["candidate_items"] == 1


def test_rollback_allows_missing_superseded_non_head_items(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, _, prefix = _fixture(tmp_path)
    plan, path = create_plan(root, prefix)
    copy_plan(root, path)
    activate_plan(root, path)
    release_hot_plan(
        root, path, apply=True, confirm_migration_id=plan.migration_id,
    )
    a_id = artifact_id(hashlib.sha256(b"aaa").hexdigest())

    # 缺省回滚仍要求全部热副本
    with pytest.raises(StoragePathError, match="缺失"):
        rollback_plan(root, path)
    # 活动 head 制品缺失即拒绝
    monkeypatch.setattr(
        "guvolu.data.cold_storage._active_head_artifact_ids",
        lambda *_args: {a_id},
    )
    with pytest.raises(StoragePathError, match="活动 head"):
        rollback_plan(root, path, allow_missing_superseded=True)
    # 非活动 head 制品允许缺失并登记
    monkeypatch.setattr(
        "guvolu.data.cold_storage._active_head_artifact_ids",
        lambda *_args: set(),
    )
    result = rollback_plan(root, path, allow_missing_superseded=True)
    assert result["status"] == "planned"
    assert result["superseded_missing_items"] == 1
    assert result["superseded_missing"] == [f"{prefix}/a.parquet"]


def test_activate_rejects_overlapping_active_route_without_persisting(
    tmp_path: Path,
) -> None:
    root, _, prefix = _fixture(tmp_path)
    plan, path = create_plan(root, prefix)
    copy_plan(root, path)

    # 注入重叠的活动父前缀
    config_path = root / "storage-roots.json"
    body = json.loads(config_path.read_text(encoding="utf-8"))
    body["routes"].append({
        "logical_prefix": "materialized",
        "physical_prefix": "artifacts/all",
        "status": "active",
        "storage_root_id": "storage-root__test__v1",
    })
    config_path.write_text(
        json.dumps(body, sort_keys=True) + "\n", encoding="utf-8",
    )

    with pytest.raises(StoragePathError, match="重叠"):
        activate_plan(root, path)

    after = json.loads(config_path.read_text(encoding="utf-8"))
    target_route = next(
        item for item in after["routes"]
        if item["logical_prefix"] == prefix
    )
    # 校验先于落盘不改坏配置
    assert target_route["status"] == "planned"


def test_copy_rejects_changed_catalog_and_wrong_target(tmp_path: Path) -> None:
    root, cold, prefix = _fixture(tmp_path)
    _, path = create_plan(root, prefix)
    target = cold / "artifacts/archive/a.parquet"
    target.parent.mkdir(parents=True)
    target.write_bytes(b"wrong")
    with pytest.raises(StoragePathError, match="目标已存在"):
        copy_plan(root, path)

    target.unlink()
    conn = sqlite3.connect(root / store.DB_FILE_NAME)
    conn.execute(
        "UPDATE artifact_location SET observed_at=? WHERE storage_path=?",
        ("2026-08-22T00:00:00+00:00", f"{prefix}/a.parquet"),
    )
    conn.commit()
    conn.close()
    copied_bytes = copy_plan(root, path)["copied_bytes"]
    assert isinstance(copied_bytes, int)
    assert copied_bytes > 0


def _archive_fixture(tmp_path: Path) -> tuple[Path, str, Path, Path]:
    """完整物化一个归档月并登记规划中的冷路由。"""
    root = tmp_path / "data"
    cold = tmp_path / "cold"
    root.mkdir()
    marker_sha = _write_json(cold / MARKER_FILE_NAME, {
        "role": "test",
        "schema_version": 1,
        "storage_root_id": "storage-root__test__v1",
    })
    prefix = (
        "materialized/trade_observation/schema_version=1/"
        "normalization_version=trade-normalization-v1"
    )
    _write_json(root / "storage-roots.json", {
        "routes": [{
            "logical_prefix": prefix,
            "physical_prefix": "artifacts/trade_v1",
            "status": "planned",
            "storage_root_id": "storage-root__test__v1",
        }],
        "roots": [{
            "filesystem": None,
            "marker_sha256": marker_sha,
            "mount_path": str(cold),
            "partition_guid": None,
            "role": "test",
            "storage_root_id": "storage-root__test__v1",
            "volume_guid": None,
            "volume_label": None,
        }],
        "schema_version": 1,
    })
    archive = (
        root / "archive" / "bitflyer" / "executions" / "BTC_JPY"
        / "2026" / "20260807_BTC_JPY.jsonl.gz"
    )
    archive.parent.mkdir(parents=True)
    rows = [
        {
            "id": 1, "side": "BUY", "price": 100, "size": "0.01",
            "exec_date": "2026-08-07T00:00:01.000",
        },
        {
            "id": 2, "side": "", "price": 101, "size": "0.02",
            "exec_date": "2026-08-07T00:00:02.000",
        },
        {
            "id": 3, "side": "SELL", "price": 102, "size": "0.03",
            "exec_date": "2026-08-07T00:00:03.000",
        },
    ]
    body = "".join(json.dumps(row) + "\n" for row in rows)
    archive.write_bytes(gzip.compress(body.encode("utf-8")))
    conn = store.connect(root)
    upsert_coverage(conn, [
        (
            "bitflyer", "BTC_JPY", "trade", "20260807", 3,
            "2026-08-07T00:00:01+00:00",
            "2026-08-07T00:00:03+00:00", "ok",
            "2026-08-11T00:00:00+00:00",
        )
    ])
    result = materialize_archive_trade_month(
        root, conn, "bitflyer", "BTC_JPY", "2026-08"
    )
    conn.close()
    assert result.row_count == 2
    # 锁文件不是制品，规划前清除
    for lock in (root / prefix).rglob("*.lock"):
        lock.unlink()
    return root, prefix, root / result.output_path, archive


def test_restore_hot_from_raw_rebuilds_byte_identical_parquet(
    tmp_path: Path,
) -> None:
    root, prefix, hot, _ = _archive_fixture(tmp_path)
    original = hot.read_bytes()
    plan, path = create_plan(root, prefix)
    item = next(row for row in plan.items if row.logical_path.endswith(".parquet"))
    assert item.sha256 == hashlib.sha256(original).hexdigest()
    hot.unlink()

    dry_run = restore_hot_from_raw_plan(root, path)
    assert dry_run["restorable_bytes"] == len(original)
    assert dry_run["status"] == "planned"
    assert not hot.exists()
    assert not path.with_name(
        path.name.replace(".plan.json", ".hot-restored-from-raw.json")
    ).exists()

    applied = restore_hot_from_raw_plan(root, path, apply=True)
    assert applied["restored_items"] == 1
    assert applied["mismatched_items"] == 0
    assert applied["failed_items"] == 0
    assert applied["status"] == "restored"
    assert applied["source"] == "raw"
    assert hot.read_bytes() == original
    assert not list(hot.parent.glob(".*rebuild*"))
    assert path.with_name(
        path.name.replace(".plan.json", ".hot-restored-from-raw.json")
    ).is_file()
    # 重复恢复幂等
    replay = restore_hot_from_raw_plan(root, path, apply=True)
    assert replay["present_items"] == 1
    assert replay["restored_items"] == 0
    assert verify_plan(root, path, side="hot")["status"] == "verified"


def test_restore_hot_from_raw_refuses_mismatched_rebuild(
    tmp_path: Path,
) -> None:
    root, prefix, hot, _ = _archive_fixture(tmp_path)
    plan, path = create_plan(root, prefix)
    hot.unlink()
    # 改写登记封口时刻使重算字节不同
    conn = sqlite3.connect(root / store.DB_FILE_NAME)
    conn.execute(
        "UPDATE artifact SET sealed_at='2020-01-01T00:00:00+00:00' "
        "WHERE artifact_kind='source_archive'"
    )
    conn.commit()
    conn.close()

    applied = restore_hot_from_raw_plan(root, path, apply=True)

    assert applied["restored_items"] == 0
    assert applied["mismatched_items"] == 1
    assert applied["status"] == "partial"
    assert not hot.exists()
    assert not list(hot.parent.glob(".*rebuild*"))
    mismatches = cast(list[dict[str, object]], applied["mismatched"])
    mismatch = mismatches[0]
    assert str(mismatch["logical_path"]).endswith(".parquet")
    assert mismatch["actual_sha256"] != mismatch["expected_sha256"]
    assert plan.migration_id == applied["migration_id"]


def test_restore_hot_from_raw_counts_bad_or_missing_raw_input(
    tmp_path: Path,
) -> None:
    root, prefix, hot, archive = _archive_fixture(tmp_path)
    _, path = create_plan(root, prefix)
    hot.unlink()
    original_raw = archive.read_bytes()
    archive.write_bytes(original_raw + b"\n")

    tampered = restore_hot_from_raw_plan(root, path, apply=True)
    assert tampered["failed_items"] == 1
    tampered_failures = cast(
        list[dict[str, object]], tampered["failures"],
    )
    assert "不符" in str(tampered_failures[0]["reason"])
    assert not hot.exists()

    archive.unlink()
    missing = restore_hot_from_raw_plan(root, path, apply=True)
    assert missing["failed_items"] == 1
    missing_failures = cast(
        list[dict[str, object]], missing["failures"],
    )
    assert "缺失" in str(missing_failures[0]["reason"])
    assert missing["restorable_bytes"] == 0
    assert not hot.exists()

    archive.write_bytes(original_raw)
    recovered = restore_hot_from_raw_plan(root, path, apply=True)
    assert recovered["restored_items"] == 1
    assert hot.is_file()


def _okx_l2_archive_fixture(
    tmp_path: Path,
) -> tuple[Path, Path, Path, Path, Path]:
    """物化一个 OKX L2 日并冻结冷迁移计划。"""
    root = tmp_path / "data"
    cold = tmp_path / "cold"
    root.mkdir()
    marker_sha = _write_json(cold / MARKER_FILE_NAME, {
        "role": "test",
        "schema_version": 1,
        "storage_root_id": "storage-root__test__v1",
    })
    prefix = (
        "materialized/book_l2/schema_version=2/"
        "normalization_version=book-l2-normalization-v2/"
        "venue_id=okx/market_id=mkt__okx__btc_usdt__r0"
    )
    _write_json(root / "storage-roots.json", {
        "routes": [{
            "logical_prefix": prefix,
            "physical_prefix": "artifacts/okx_l2_v2",
            "status": "planned",
            "storage_root_id": "storage-root__test__v1",
        }],
        "roots": [{
            "filesystem": None,
            "marker_sha256": marker_sha,
            "mount_path": str(cold),
            "partition_guid": None,
            "role": "test",
            "storage_root_id": "storage-root__test__v1",
            "volume_guid": None,
            "volume_label": None,
        }],
        "schema_version": 1,
    })
    day = "2026-08-07"
    day_value = datetime.fromisoformat(day).replace(tzinfo=UTC)
    start_millis = int(day_value.timestamp() * 1000)
    raw_directory = (
        root / "raw/archive/okx/book_l2/"
        f"venue_symbol=BTC-USDT/day={day}"
    )
    raw_directory.mkdir(parents=True)
    filename = f"BTC-USDT-L2orderbook-400lv-{day}.tar.gz"
    archive = raw_directory / filename
    rows = [
        {
            "instId": "BTC-USDT", "action": "snapshot",
            "ts": str(start_millis),
            "asks": [["101", "1", "2"], ["102", "2", "1"]],
            "bids": [["100", "1", "1"], ["99", "3", "2"]],
        },
        {
            "instId": "BTC-USDT", "action": "update",
            "ts": str(start_millis + 10),
            "asks": [["101", "0", "0"], ["103", "1.5", "1"]],
            "bids": [["100", "2", "2"]],
        },
        {
            "instId": "BTC-USDT", "action": "snapshot",
            "ts": str(start_millis + 20),
            "asks": [["102", "2", "1"], ["103", "1.5", "1"]],
            "bids": [["100", "2", "2"], ["99", "3", "2"]],
        },
    ]
    body = b"".join(
        json.dumps(row, separators=(",", ":")).encode() + b"\n"
        for row in rows
    )
    with tarfile.open(archive, "w:gz") as stream:
        info = tarfile.TarInfo(
            f"BTC-USDT-L2orderbook-400lv-{day}.data"
        )
        info.size = len(body)
        stream.addfile(info, io.BytesIO(body))
    archive_sha = hashlib.sha256(archive.read_bytes()).hexdigest()
    manifest = raw_directory / f"{filename}.manifest.json"
    _write_json(manifest, {
        "status": "sealed",
        "completion_claim": True,
        "sha256": archive_sha,
        "byte_count": archive.stat().st_size,
        "venue_symbol": "BTC-USDT",
        "endpoint": "historical-data/order-book",
        "storage_path": archive.relative_to(root).as_posix(),
        "day": day,
        "depth_levels": 2,
        "source_last_modified": format_datetime(
            day_value + timedelta(days=1, seconds=47), usegmt=True,
        ),
        "sealed_at": "2026-08-11T10:00:00+00:00",
    })
    conn = store.connect(root)
    try:
        sealed = okx_sealed_input(root, manifest)
        assert sealed is not None
        result = materialize_okx_l2_archive(
            root, conn, sealed, require_full_day=False,
        )
    finally:
        conn.close()
    for lock in (root / prefix).rglob("*.lock"):
        lock.unlink()
    _, plan_path = create_plan(root, prefix)
    return (
        root,
        cold,
        plan_path,
        root / result.frame_path,
        root / result.level_path,
    )


def test_restore_okx_l2_from_raw_is_day_gated_and_control_readonly(
    tmp_path: Path,
) -> None:
    root, cold, plan_path, frame, level = _okx_l2_archive_fixture(tmp_path)
    original = {frame: frame.read_bytes(), level: level.read_bytes()}
    frame.unlink()
    level.unlink()
    shutil.rmtree(cold)
    config_before = (root / "storage-roots.json").read_bytes()
    conn = store.connect_readonly(root)
    assert conn is not None
    try:
        control_before = (
            conn.execute("SELECT COUNT(*) FROM partition_attempt").fetchone(),
            conn.execute(
                "SELECT * FROM materialization_partition_head"
            ).fetchall(),
        )
    finally:
        conn.close()

    with pytest.raises(StoragePathError, match="没有可恢复"):
        restore_hot_from_raw_plan(root, plan_path, shard=(1, 2))
    dry_run = restore_hot_from_raw_plan(root, plan_path, shard=(0, 2))

    assert dry_run["candidate_items"] == 2
    assert dry_run["okx_l2_days"] == 1
    assert dry_run["shard"] == [0, 2]
    assert dry_run["trade_items"] == 0
    assert dry_run["status"] == "planned"
    assert dry_run["restorable_bytes"] == sum(map(len, original.values()))
    assert dry_run["restored_items"] == 0
    assert not frame.exists()
    assert not level.exists()
    assert not list(frame.parent.glob(".okx-l2-*.rebuild"))
    receipt = plan_path.with_name(
        plan_path.name.replace(
            ".plan.json", ".hot-restored-from-raw.json",
        )
    )
    assert not receipt.exists()

    applied = restore_hot_from_raw_plan(root, plan_path, apply=True)

    assert applied["status"] == "restored"
    assert applied["restored_items"] == 2
    assert applied["mismatched_items"] == 0
    assert applied["failed_items"] == 0
    assert frame.read_bytes() == original[frame]
    assert level.read_bytes() == original[level]
    assert receipt.is_file()
    assert (root / "storage-roots.json").read_bytes() == config_before
    conn = store.connect_readonly(root)
    assert conn is not None
    try:
        control_after = (
            conn.execute("SELECT COUNT(*) FROM partition_attempt").fetchone(),
            conn.execute(
                "SELECT * FROM materialization_partition_head"
            ).fetchall(),
        )
    finally:
        conn.close()
    assert control_after == control_before


def test_restore_okx_l2_from_raw_mismatch_writes_neither_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, _, plan_path, frame, level = _okx_l2_archive_fixture(tmp_path)
    frame.unlink()
    level.unlink()
    from guvolu.data import cold_storage as cold_storage_module

    original_write = cast(
        Callable[[Any, str, Path], None],
        getattr(cold_storage_module, "_okx_write_parquet"),
    )

    def mismatched_write(db: Any, query: str, path: Path) -> None:
        original_write(db, query, path)
        if "book_l2_level" in query:
            with path.open("ab") as stream:
                stream.write(b"mismatch")

    monkeypatch.setattr(
        cold_storage_module, "_okx_write_parquet", mismatched_write,
    )

    dry_run = restore_hot_from_raw_plan(root, plan_path)
    assert dry_run["status"] == "blocked"
    assert dry_run["mismatched_items"] == 1

    applied = restore_hot_from_raw_plan(root, plan_path, apply=True)

    assert applied["restorable_bytes"] == 0
    assert applied["restored_items"] == 0
    assert applied["mismatched_items"] == 1
    mismatches = cast(list[dict[str, object]], applied["mismatched"])
    assert mismatches[0]["dataset"] == "book_l2_level"
    assert not frame.exists()
    assert not level.exists()
    assert not list(frame.parent.glob(".okx-l2-*.rebuild"))


def test_restore_okx_l2_from_raw_rejects_changed_source_manifest(
    tmp_path: Path,
) -> None:
    root, _, plan_path, frame, level = _okx_l2_archive_fixture(tmp_path)
    frame.unlink()
    level.unlink()
    manifest = next(root.rglob("*.tar.gz.manifest.json"))
    manifest.write_bytes(manifest.read_bytes() + b"\n")

    dry_run = restore_hot_from_raw_plan(root, plan_path)
    assert dry_run["status"] == "blocked"
    assert dry_run["failed_items"] == 2

    applied = restore_hot_from_raw_plan(root, plan_path, apply=True)

    assert applied["restorable_bytes"] == 0
    assert applied["restored_items"] == 0
    assert applied["failed_items"] == 2
    failures = cast(list[dict[str, object]], applied["failures"])
    assert all("manifest 与登记不符" in str(row["reason"]) for row in failures)
    assert not frame.exists()
    assert not level.exists()


def test_restore_okx_l2_compensates_second_landing_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, _, plan_path, frame, level = _okx_l2_archive_fixture(tmp_path)
    frame.unlink()
    level.unlink()
    from guvolu.data import cold_storage as cold_storage_module

    storage_os: Any = getattr(cold_storage_module, "os")
    original_replace = cast(
        Callable[[Any, Any], None],
        getattr(storage_os, "replace"),
    )
    hot_targets = {frame.resolve(), level.resolve()}
    landing_calls = 0

    def fail_second_landing(source: Any, target: Any) -> None:
        nonlocal landing_calls
        if Path(target).resolve() in hot_targets:
            landing_calls += 1
            if landing_calls == 2:
                raise OSError("injected second landing failure")
        original_replace(source, target)

    monkeypatch.setattr(
        storage_os, "replace", fail_second_landing,
    )

    applied = restore_hot_from_raw_plan(root, plan_path, apply=True)

    assert landing_calls == 2
    assert applied["status"] == "partial"
    assert applied["restored_items"] == 0
    assert applied["landing_failure_count"] == 1
    assert applied["rollback_failure_count"] == 0
    failures = cast(list[dict[str, object]], applied["failures"])
    assert [row["phase"] for row in failures] == ["landing"]
    assert not frame.exists()
    assert not level.exists()


def test_restore_hot_cli_from_raw_flag(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    root, prefix, hot, _ = _archive_fixture(tmp_path)
    _, path = create_plan(root, prefix)
    hot.unlink()
    monkeypatch.setattr(sys, "argv", [
        "cold_storage", "--data-root", str(root), "restore-hot",
        "--plan", str(path), "--from-raw", "--apply",
    ])

    cold_storage_main()

    body = json.loads(capsys.readouterr().out)
    assert body["source"] == "raw"
    assert body["restored_items"] == 1
    assert hot.is_file()
