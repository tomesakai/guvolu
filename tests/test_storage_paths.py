from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from guvolu.data.materialize import _output_directory
from guvolu.data.storage_paths import (
    MARKER_FILE_NAME,
    StoragePathError,
    relative_storage_path,
    resolve_storage_path,
    storage_resolver,
)


def _write_json(path: Path, body: object) -> str:
    payload = (json.dumps(body, sort_keys=True) + "\n").encode()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return hashlib.sha256(payload).hexdigest()


def _configured(
    tmp_path: Path,
    *,
    route_status: str = "planned",
    logical_prefix: str = "materialized/archive",
) -> tuple[Path, Path]:
    data_root = tmp_path / "data"
    cold_root = tmp_path / "cold"
    data_root.mkdir()
    marker = {
        "role": "test",
        "schema_version": 1,
        "storage_root_id": "storage-root__test__v1",
    }
    marker_sha = _write_json(cold_root / MARKER_FILE_NAME, marker)
    _write_json(data_root / "storage-roots.json", {
        "routes": [{
            "logical_prefix": logical_prefix,
            "physical_prefix": "artifacts/archive",
            "status": route_status,
            "storage_root_id": "storage-root__test__v1",
        }],
        "roots": [{
            "filesystem": None,
            "marker_sha256": marker_sha,
            "mount_path": str(cold_root),
            "partition_guid": None,
            "role": "test",
            "storage_root_id": "storage-root__test__v1",
            "volume_guid": None,
            "volume_label": None,
        }],
        "schema_version": 1,
    })
    return data_root, cold_root


def test_planned_route_does_not_redirect_reads(tmp_path: Path) -> None:
    data_root, cold_root = _configured(tmp_path)
    logical = "materialized/archive/part.parquet"
    hot = data_root / logical
    cold = cold_root / "artifacts/archive/part.parquet"
    hot.parent.mkdir(parents=True)
    cold.parent.mkdir(parents=True)
    hot.write_bytes(b"hot")
    cold.write_bytes(b"cold")

    assert resolve_storage_path(data_root, logical) == hot.resolve()
    assert relative_storage_path(data_root, cold) == logical


def test_active_route_resolves_only_verified_cold_root(tmp_path: Path) -> None:
    data_root, cold_root = _configured(tmp_path, route_status="active")
    logical = "materialized/archive/part.parquet"
    cold = cold_root / "artifacts/archive/part.parquet"
    cold.parent.mkdir(parents=True)
    cold.write_bytes(b"cold")

    assert resolve_storage_path(data_root, logical) == cold.resolve()
    assert relative_storage_path(data_root, cold) == logical


def test_materializer_writes_directly_to_active_storage_route(
    tmp_path: Path,
) -> None:
    prefix = (
        "materialized/trade_observation/schema_version=1/"
        "normalization_version=trade-normalization-v1"
    )
    data_root, cold_root = _configured(
        tmp_path, route_status="active", logical_prefix=prefix,
    )

    output = _output_directory(
        data_root, "gmo", "mkt__gmo__btc__r0", "2026-08",
        "trade-normalization-v1",
    )

    expected = (
        cold_root / "artifacts" / "archive"
        / "venue_id=gmo" / "market_id=mkt__gmo__btc__r0"
        / "event_year=2026" / "event_month=08"
    )
    assert output == expected.resolve()


def test_hot_bulk_root_uses_same_verified_route_contract(tmp_path: Path) -> None:
    data_root, bulk_root = _configured(tmp_path, route_status="active")
    marker_path = bulk_root / MARKER_FILE_NAME
    marker = json.loads(marker_path.read_text(encoding="utf-8"))
    marker["role"] = "hot_bulk"
    marker_sha = _write_json(marker_path, marker)
    config_path = data_root / "storage-roots.json"
    config = json.loads(config_path.read_text(encoding="utf-8"))
    config["roots"][0]["role"] = "hot_bulk"
    config["roots"][0]["marker_sha256"] = marker_sha
    _write_json(config_path, config)

    logical = "materialized/archive/segment.jsonl"
    physical = bulk_root / "artifacts/archive/segment.jsonl"
    physical.parent.mkdir(parents=True)
    physical.write_bytes(b"sealed")

    assert resolve_storage_path(data_root, logical) == physical.resolve()
    assert relative_storage_path(data_root, physical) == logical


def test_active_route_rejects_changed_marker(tmp_path: Path) -> None:
    data_root, cold_root = _configured(tmp_path, route_status="active")
    (cold_root / MARKER_FILE_NAME).write_text("{}\n", encoding="utf-8")

    with pytest.raises(StoragePathError, match="散列不符"):
        resolve_storage_path(data_root, "materialized/archive/part.parquet")


def test_path_escape_and_overlapping_active_routes_are_rejected(
    tmp_path: Path,
) -> None:
    data_root, _ = _configured(tmp_path)
    with pytest.raises(StoragePathError, match="安全相对路径"):
        resolve_storage_path(data_root, "../outside.parquet")

    body = json.loads((data_root / "storage-roots.json").read_text())
    body["routes"] = [
        {**body["routes"][0], "status": "active"},
        {
            **body["routes"][0],
            "logical_prefix": "materialized/archive/subset",
            "status": "active",
        },
    ]
    _write_json(data_root / "storage-roots.json", body)
    with pytest.raises(StoragePathError, match="前缀重叠"):
        storage_resolver(data_root)
