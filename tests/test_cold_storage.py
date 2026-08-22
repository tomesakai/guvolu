from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

import pytest

from guvolu.data import store
from guvolu.data.cold_storage import (
    activate_plan,
    copy_plan,
    create_plan,
    release_hot_plan,
    restore_hot_plan,
    rollback_plan,
    verify_plan,
)
from guvolu.data.materialize import artifact_id
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
