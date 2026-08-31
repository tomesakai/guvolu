from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import time
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import duckdb
import pytest

import guvolu.data.l2_materialize as l2_module
from guvolu.data import store
from guvolu.data.book_l2_contract import BOOK_L2_V3_NORMALIZATION_VERSION
from guvolu.data.l2_materialize import (
    L2_NORMALIZATION_VERSION,
    L2_SCHEMA_VERSION,
    L2Result,
    _DESCRIPTORS,
    _raw_metadata,
    _sealed_inputs,
    _latest_run_inputs,
    audit_l2,
    materialize_segment,
)
from guvolu.data.storage_paths import MARKER_FILE_NAME


BASE = datetime(2026, 8, 12, tzinfo=UTC)


def _bitbank_payload(kind: str, sequence: int, offset_ms: int) -> str:
    millis = int(BASE.timestamp() * 1000) + offset_ms
    if kind == "snapshot":
        room = "depth_whole_btc_jpy"
        data = {
            "bids": [["100", "1"]], "asks": [["101", "1"]],
            "sequenceId": str(sequence), "timestamp": str(millis),
        }
    else:
        room = "depth_diff_btc_jpy"
        data = {
            "b": [["100", "2"]], "a": [["102", "1"]],
            "s": str(sequence), "t": str(millis),
        }
    return "42" + json.dumps([
        "message", {"room_name": room, "message": {"data": data}},
    ], separators=(",", ":"))


def _gmo_payload() -> str:
    return json.dumps({
        "channel": "orderbooks", "symbol": "BTC",
        "timestamp": BASE.isoformat(),
        "bids": [{"price": "100", "size": "1"}],
        "asks": [{"price": "101", "size": "2"}],
    }, separators=(",", ":"))


def _bitflyer_payload() -> str:
    return json.dumps({
        "jsonrpc": "2.0", "method": "channelMessage",
        "params": {
            "channel": "lightning_board_snapshot_BTC_JPY",
            "message": {
                "mid_price": 100,
                "bids": [
                    {"price": 99, "size": 0},
                    {"price": 98, "size": 1},
                ],
                "asks": [{"price": 101, "size": 2}],
            },
        },
    }, separators=(",", ":"))


def _write_segment(
    root: Path,
    venue: str,
    symbol: str,
    run_id: str,
    payloads: Sequence[tuple[str, str | None, str | None]],
    *,
    schema_version: int,
    segment_sequence: int = 1,
    sealed_at: datetime | None = None,
    status: str = "sealed",
    completion_claim: bool = True,
) -> None:
    descriptor = _DESCRIPTORS[venue]
    directory = (
        root / "raw/realtime/book_l2" / f"venue_id={venue}"
        / f"venue_symbol={symbol}" / f"run_id={run_id}"
    )
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"segment-{segment_sequence:06d}.jsonl"
    rows: list[dict[str, object]] = []
    for index, (payload, connection_id, channel_id) in enumerate(
        payloads, start=1
    ):
        received = (BASE + timedelta(seconds=10, milliseconds=index)).isoformat()
        row: dict[str, object] = {
            "schema_version": schema_version,
            "run_id": run_id, "segment_sequence": segment_sequence,
            "record_sequence": index, "venue_id": venue,
            "venue_symbol": symbol, "domain": "book_l2",
            "source": "websocket",
            "source_endpoint": descriptor.endpoint,
            "payload_raw": payload, "ingest_time": received,
        }
        if schema_version in {2, 3}:
            row.update({
                "endpoint_id": descriptor.endpoint_id,
                "connection_id": connection_id, "channel_id": channel_id,
                "recv_ts_utc": received,
                "recv_ts_mono_ns": 10_000_000 + index,
                "raw_payload_sha256": hashlib.sha256(
                    payload.encode("utf-8")
                ).hexdigest(),
            })
        if schema_version == 3:
            row["endpoint_revision"] = descriptor.endpoint_revision
        rows.append(row)
    path.write_text(
        "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8"
    )
    sha = hashlib.sha256(path.read_bytes()).hexdigest()
    seal_time = sealed_at or BASE + timedelta(minutes=segment_sequence)
    manifest = {
        "schema_version": schema_version, "status": status,
        "completion_claim": completion_claim,
        "artifact_id": f"sha256-{sha}",
        "sha256": sha, "byte_count": path.stat().st_size,
        "record_count": len(rows), "run_id": run_id,
        "segment_sequence": segment_sequence, "venue_id": venue,
        "venue_symbol": symbol, "domain": "book_l2",
        "endpoint_id": (
            descriptor.endpoint_id if schema_version in {2, 3} else None
        ),
        "endpoint_revision": (
            descriptor.endpoint_revision if schema_version == 3 else None
        ),
        "storage_path": path.relative_to(root).as_posix(),
        "started_at": (seal_time - timedelta(seconds=2)).isoformat(),
        "first_ingest_time": (seal_time - timedelta(seconds=2)).isoformat(),
        "last_ingest_time": (seal_time - timedelta(seconds=1)).isoformat(),
        "sealed_at": seal_time.isoformat(),
    }
    path.with_name(
        f"segment-{segment_sequence:06d}.manifest.json"
    ).write_text(
        json.dumps(manifest), encoding="utf-8"
    )


def _write_run_checkpoint(
    root: Path,
    venue: str,
    symbol: str,
    run_id: str,
    *,
    started_at: datetime = BASE - timedelta(hours=1),
) -> Path:
    directory = (
        root / "raw/realtime/book_l2" / f"venue_id={venue}"
        / f"venue_symbol={symbol}" / f"run_id={run_id}"
    )
    manifests = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in sorted(directory.glob("segment-*.manifest.json"))
    ]
    latest_seal = max(
        (
            datetime.fromisoformat(str(body["sealed_at"]))
            for body in manifests
        ),
        default=started_at,
    )
    checkpoint = {
        "schema_version": 3,
        "status": "open",
        "run_id": run_id,
        "venue_id": venue,
        "venue_symbol": symbol,
        "domain": "book_l2",
        "endpoint_id": (
            manifests[0].get("endpoint_id") if manifests else None
        ),
        "endpoint_revision": (
            manifests[0].get("endpoint_revision") if manifests else None
        ),
        "started_at": started_at.isoformat(),
        "checkpoint_at": (latest_seal + timedelta(seconds=1)).isoformat(),
        "sealed_segments": len(manifests),
        "records": sum(int(body["record_count"]) for body in manifests),
    }
    path = directory / "checkpoint.json"
    path.write_text(json.dumps(checkpoint), encoding="utf-8")
    return path


def _materialize(root: Path) -> L2Result:
    conn = store.connect(root)
    try:
        result = materialize_segment(root, conn, _sealed_inputs(root)[0])
    finally:
        conn.close()
    return result


def _directory_link(link: Path, target: Path) -> None:
    """创建测试用目录联接，Windows 不依赖开发者模式。"""
    link.parent.mkdir(parents=True, exist_ok=True)
    target.mkdir(parents=True, exist_ok=True)
    if os.name == "nt":
        subprocess.run(
            [
                "powershell.exe",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "New-Item -ItemType Junction -Path '"
                + str(link).replace("'", "''")
                + "' -Target '"
                + str(target).replace("'", "''")
                + "' | Out-Null",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
    else:
        link.symlink_to(target, target_is_directory=True)


def _register_hot_bulk_route(
    data_root: Path, bulk_root: Path, physical_prefix: str,
) -> None:
    """登记联接目标为已验证热批量根。"""
    marker = {
        "role": "hot_bulk",
        "schema_version": 1,
        "storage_root_id": "storage-root__hot-bulk__junction__v1",
    }
    marker_payload = (json.dumps(marker, sort_keys=True) + "\n").encode()
    (bulk_root / MARKER_FILE_NAME).write_bytes(marker_payload)
    config = {
        "roots": [{
            "filesystem": None,
            "marker_sha256": hashlib.sha256(marker_payload).hexdigest(),
            "mount_path": str(bulk_root),
            "partition_guid": None,
            "role": "hot_bulk",
            "storage_root_id": "storage-root__hot-bulk__junction__v1",
            "volume_guid": None,
            "volume_label": None,
        }],
        "routes": [{
            "logical_prefix": "raw/realtime/book_l2",
            "physical_prefix": physical_prefix,
            "status": "active",
            "storage_root_id": "storage-root__hot-bulk__junction__v1",
        }],
        "schema_version": 1,
    }
    (data_root / "storage-roots.json").write_text(
        json.dumps(config), encoding="utf-8",
    )


def test_sealed_input_allows_only_the_declared_logical_junction_root(
    tmp_path: Path,
) -> None:
    """联接只作兼容入口，manifest 不能借 junction 逃逸逻辑目录。"""
    root = tmp_path / "repo" / "data"
    bulk_root = tmp_path / "physical"
    target = bulk_root / "book_l2"
    _directory_link(root / "raw" / "realtime" / "book_l2", target)
    _register_hot_bulk_route(root, bulk_root, "book_l2")
    _write_segment(
        root,
        "gmo",
        "BTC",
        "run-junction-v3",
        [(_gmo_payload(), "run-junction-v3-c000001", "orderbooks")],
        schema_version=3,
    )

    inputs = _sealed_inputs(root)

    assert len(inputs) == 1
    assert inputs[0].artifact.absolute_path.is_relative_to(target.resolve())
    manifest = next(target.rglob("segment-000001.manifest.json"))
    body = json.loads(manifest.read_text(encoding="utf-8"))
    body["storage_path"] = "raw/realtime/book_l2/../outside.jsonl"
    manifest.write_text(json.dumps(body), encoding="utf-8")
    with pytest.raises(ValueError, match="segment 逻辑路径非法"):
        _sealed_inputs(root)

    body["storage_path"] = (
        "raw/realtime/book_l2/venue_id=gmo/venue_symbol=BTC/"
        "run_id=run-junction-v3/segment-000001.jsonl"
    )
    manifest.write_text(json.dumps(body), encoding="utf-8")
    outside = tmp_path / "outside-run"
    nested = (
        target / "venue_id=gmo" / "venue_symbol=BTC" /
        "run_id=z"
    )
    _directory_link(nested, outside)
    escaped_segment = outside / "segment-000001.jsonl"
    escaped_segment.write_bytes(
        manifest.with_name("segment-000001.jsonl").read_bytes()
    )
    escaped_sha = hashlib.sha256(escaped_segment.read_bytes()).hexdigest()
    escaped_body = body | {
        "run_id": "z",
        "sha256": escaped_sha,
        "artifact_id": f"sha256-{escaped_sha}",
        "byte_count": escaped_segment.stat().st_size,
        "storage_path": (
            "raw/realtime/book_l2/venue_id=gmo/venue_symbol=BTC/"
            "run_id=z/segment-000001.jsonl"
        ),
    }
    (outside / "segment-000001.manifest.json").write_text(
        json.dumps(escaped_body), encoding="utf-8"
    )
    with pytest.raises(ValueError, match="segment 路径越界"):
        _sealed_inputs(root)


def test_sealed_input_can_reside_on_verified_hot_bulk_root(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    bulk_root = tmp_path / "bulk"
    data_root.mkdir()
    _write_segment(
        bulk_root, "gmo", "BTC", "run-hot-bulk",
        [(_gmo_payload(), None, None)], schema_version=1,
    )
    marker = {
        "role": "hot_bulk",
        "schema_version": 1,
        "storage_root_id": "storage-root__hot-bulk__v1",
    }
    marker_path = bulk_root / MARKER_FILE_NAME
    marker_payload = (json.dumps(marker, sort_keys=True) + "\n").encode()
    marker_path.write_bytes(marker_payload)
    config = {
        "roots": [{
            "filesystem": None,
            "marker_sha256": hashlib.sha256(marker_payload).hexdigest(),
            "mount_path": str(bulk_root),
            "partition_guid": None,
            "role": "hot_bulk",
            "storage_root_id": "storage-root__hot-bulk__v1",
            "volume_guid": None,
            "volume_label": None,
        }],
        "routes": [{
            "logical_prefix": "raw/realtime/book_l2",
            "physical_prefix": "raw/realtime/book_l2",
            "status": "active",
            "storage_root_id": "storage-root__hot-bulk__v1",
        }],
        "schema_version": 1,
    }
    (data_root / "storage-roots.json").write_text(
        json.dumps(config), encoding="utf-8",
    )

    item = _sealed_inputs(data_root)[0]

    assert item.artifact.absolute_path.is_relative_to(bulk_root)
    assert item.artifact.storage_path.startswith("raw/realtime/book_l2/")


def test_latest_run_filter_keeps_complete_newest_run(tmp_path: Path) -> None:
    _write_segment(
        tmp_path, "gmo", "BTC", "run-old",
        [(_gmo_payload(), None, None)], schema_version=1,
    )
    _write_segment(
        tmp_path, "gmo", "BTC", "run-new",
        [(_gmo_payload(), None, None)], schema_version=1,
    )

    selected = _latest_run_inputs(_sealed_inputs(tmp_path))

    assert [item.run_id for item in selected] == ["run-new"]


def test_latest_run_filter_hashes_only_selected_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_segment(
        tmp_path, "gmo", "BTC", "run-old",
        [(_gmo_payload(), None, None)], schema_version=1,
    )
    _write_segment(
        tmp_path, "gmo", "BTC", "run-new",
        [(_gmo_payload(), None, None)], schema_version=1,
    )
    hashed: list[Path] = []
    original = cast(
        Callable[[Path], str], getattr(l2_module, "sha256_file"),
    )

    def tracked(path: Path) -> str:
        hashed.append(path)
        return original(path)

    monkeypatch.setattr(l2_module, "sha256_file", tracked)

    selected = _sealed_inputs(tmp_path, latest_run_only=True)

    assert [item.run_id for item in selected] == ["run-new"]
    assert len(hashed) == 1
    assert "run_id=run-new" in hashed[0].as_posix()


def test_latest_run_filter_reads_only_selected_manifests(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_segment(
        tmp_path, "gmo", "BTC", "run-old",
        [(_gmo_payload(), None, None)], schema_version=1,
    )
    _write_segment(
        tmp_path, "gmo", "BTC", "run-new",
        [(_gmo_payload(), None, None)], schema_version=1,
    )
    read_manifests: list[Path] = []
    original = Path.read_text

    def tracked(
        path: Path, encoding: str | None = None, errors: str | None = None,
    ) -> str:
        if path.name.endswith(".manifest.json"):
            read_manifests.append(path)
        return original(path, encoding=encoding, errors=errors)

    monkeypatch.setattr(Path, "read_text", tracked)

    selected = _sealed_inputs(tmp_path, latest_run_only=True)

    assert [item.run_id for item in selected] == ["run-new"]
    assert len(read_manifests) == 1
    assert "run_id=run-new" in read_manifests[0].as_posix()


def test_latest_run_filter_prefers_open_run_over_newer_closed_run(
    tmp_path: Path,
) -> None:
    _write_segment(
        tmp_path, "gmo", "BTC", "run-open",
        [(_gmo_payload(), None, None)], schema_version=1,
    )
    open_directory = (
        tmp_path / "raw/realtime/book_l2/venue_id=gmo/venue_symbol=BTC"
        / "run_id=run-open"
    )
    (open_directory / "checkpoint.json").write_text("{}", encoding="utf-8")
    (open_directory / "segment-000002.jsonl.open").write_text(
        "{}\n", encoding="utf-8",
    )
    _write_segment(
        tmp_path, "gmo", "BTC", "run-closed-newer",
        [(_gmo_payload(), None, None)], schema_version=1,
    )

    selected = _sealed_inputs(tmp_path, latest_run_only=True)

    assert [item.run_id for item in selected] == ["run-open"]


def test_legacy_latest_run_only_remains_one_run_per_venue(
    tmp_path: Path,
) -> None:
    _write_segment(
        tmp_path, "gmo", "BTC", "run-btc",
        [(_gmo_payload(), None, None)], schema_version=1,
    )
    _write_segment(
        tmp_path, "gmo", "ETH", "run-eth",
        [(_gmo_payload(), None, None)], schema_version=1,
    )
    btc_run = (
        tmp_path / "raw/realtime/book_l2/venue_id=gmo/venue_symbol=BTC"
        / "run_id=run-btc"
    )
    eth_run = (
        tmp_path / "raw/realtime/book_l2/venue_id=gmo/venue_symbol=ETH"
        / "run_id=run-eth"
    )
    os.utime(btc_run, ns=(1_000_000_000, 1_000_000_000))
    os.utime(eth_run, ns=(2_000_000_000, 2_000_000_000))

    selected = _sealed_inputs(tmp_path, latest_run_only=True)

    assert [item.run_id for item in selected] == ["run-eth"]
    assert len(_sealed_inputs(tmp_path)) == 2


def test_bounded_freshness_selects_latest_n_per_stream_repeatably(
    tmp_path: Path,
) -> None:
    for venue, symbol, count in (
        ("gmo", "BTC", 4),
        ("gmo", "ETH", 3),
        ("bitbank", "btc_jpy", 3),
    ):
        for sequence in range(1, count + 1):
            _write_segment(
                tmp_path,
                venue,
                symbol,
                f"run-{venue}",
                [(_gmo_payload(), None, None)],
                schema_version=1,
                segment_sequence=sequence,
                sealed_at=BASE + timedelta(minutes=sequence),
            )
        _write_run_checkpoint(
            tmp_path, venue, symbol, f"run-{venue}",
        )

    first = _sealed_inputs(
        tmp_path, latest_sealed_segments_per_stream=2,
    )
    second = _sealed_inputs(
        tmp_path, latest_sealed_segments_per_stream=2,
    )

    identity = [
        (
            next(
                part.split("=", 1)[1]
                for part in item.manifest_path.parts
                if part.startswith("venue_id=")
            ),
            next(
                part.split("=", 1)[1]
                for part in item.manifest_path.parts
                if part.startswith("venue_symbol=")
            ),
            item.segment_sequence,
        )
        for item in first
    ]
    assert identity == [
        ("bitbank", "btc_jpy", 2),
        ("bitbank", "btc_jpy", 3),
        ("gmo", "BTC", 3),
        ("gmo", "BTC", 4),
        ("gmo", "ETH", 2),
        ("gmo", "ETH", 3),
    ]
    assert [item.manifest_path for item in first] == [
        item.manifest_path for item in second
    ]
    assert len(_sealed_inputs(tmp_path)) == 10


def test_bounded_freshness_rejects_nonmonotonic_segment_seal_time(
    tmp_path: Path,
) -> None:
    for sequence, minute in ((1, 30), (2, 10), (3, 20), (4, 20)):
        _write_segment(
            tmp_path,
            "gmo",
            "BTC",
            "run-current",
            [(_gmo_payload(), None, None)],
            schema_version=1,
            segment_sequence=sequence,
            sealed_at=BASE + timedelta(minutes=minute),
        )

    _write_run_checkpoint(tmp_path, "gmo", "BTC", "run-current")

    with pytest.raises(ValueError, match="时间非单调"):
        _sealed_inputs(
            tmp_path, latest_sealed_segments_per_stream=3,
        )


def test_bounded_freshness_rejects_incomplete_without_fallback(
    tmp_path: Path,
) -> None:
    _write_segment(
        tmp_path,
        "gmo",
        "BTC",
        "run-current",
        [(_gmo_payload(), None, None)],
        schema_version=1,
        segment_sequence=1,
    )
    _write_segment(
        tmp_path,
        "gmo",
        "BTC",
        "run-current",
        [(_gmo_payload(), None, None)],
        schema_version=1,
        segment_sequence=2,
        status="recovered_incomplete",
        completion_claim=False,
    )
    run_directory = (
        tmp_path / "raw/realtime/book_l2/venue_id=gmo/venue_symbol=BTC"
        / "run_id=run-current"
    )
    (run_directory / "segment-000003.jsonl.open").write_text(
        "{}\n", encoding="utf-8",
    )
    _write_run_checkpoint(tmp_path, "gmo", "BTC", "run-current")

    with pytest.raises(ValueError, match="非完整 segment"):
        _sealed_inputs(
            tmp_path, latest_sealed_segments_per_stream=10,
        )
    only_unsealed = tmp_path / "only-unsealed"
    _write_segment(
        only_unsealed,
        "gmo",
        "BTC",
        "run-open",
        [(_gmo_payload(), None, None)],
        schema_version=1,
        status="open",
        completion_claim=False,
    )
    _write_run_checkpoint(only_unsealed, "gmo", "BTC", "run-open")
    with pytest.raises(ValueError, match="非完整 segment"):
        _sealed_inputs(
            only_unsealed, latest_sealed_segments_per_stream=1,
        )

    no_run_fallback = tmp_path / "no-run-fallback"
    _write_segment(
        no_run_fallback,
        "gmo",
        "BTC",
        "run-old",
        [(_gmo_payload(), None, None)],
        schema_version=1,
    )
    _write_run_checkpoint(
        no_run_fallback, "gmo", "BTC", "run-old",
        started_at=BASE - timedelta(hours=2),
    )
    latest_directory = (
        no_run_fallback
        / "raw/realtime/book_l2/venue_id=gmo/venue_symbol=BTC"
        / "run_id=run-current"
    )
    latest_directory.mkdir(parents=True)
    (latest_directory / "checkpoint.json").write_text(json.dumps({
        "schema_version": 3,
        "status": "open",
        "run_id": "run-current",
        "venue_id": "gmo",
        "venue_symbol": "BTC",
        "domain": "book_l2",
        "endpoint_id": None,
        "endpoint_revision": None,
        "started_at": BASE.isoformat(),
        "checkpoint_at": (BASE + timedelta(minutes=1)).isoformat(),
        "sealed_segments": 0,
        "records": 0,
    }), encoding="utf-8")
    (latest_directory / "segment-000001.jsonl.open").write_text(
        "{}\n", encoding="utf-8",
    )
    assert _sealed_inputs(
        no_run_fallback, latest_sealed_segments_per_stream=1,
    ) == []


def test_bounded_freshness_fails_closed_on_invalid_selection_or_metadata(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="必须为正整数"):
        _sealed_inputs(tmp_path, latest_sealed_segments_per_stream=0)
    with pytest.raises(ValueError, match="互斥"):
        _sealed_inputs(
            tmp_path,
            latest_run_only=True,
            latest_sealed_segments_per_stream=1,
        )

    _write_segment(
        tmp_path,
        "gmo",
        "BTC",
        "run-current",
        [(_gmo_payload(), None, None)],
        schema_version=1,
    )
    _write_run_checkpoint(tmp_path, "gmo", "BTC", "run-current")
    manifest = next(tmp_path.rglob("segment-000001.manifest.json"))
    body = json.loads(manifest.read_text(encoding="utf-8"))
    del body["sealed_at"]
    manifest.write_text(json.dumps(body), encoding="utf-8")
    with pytest.raises(ValueError, match="缺 sealed_at"):
        _sealed_inputs(
            tmp_path, latest_sealed_segments_per_stream=1,
        )

    invalid_shape = tmp_path / "invalid-shape"
    _write_segment(
        invalid_shape,
        "gmo",
        "BTC",
        "run-current",
        [(_gmo_payload(), None, None)],
        schema_version=1,
    )
    _write_run_checkpoint(
        invalid_shape, "gmo", "BTC", "run-current",
    )
    invalid_manifest = next(
        invalid_shape.rglob("segment-000001.manifest.json")
    )
    invalid_manifest.write_text("[]", encoding="utf-8")
    with pytest.raises(ValueError, match="结构非法"):
        _sealed_inputs(
            invalid_shape, latest_sealed_segments_per_stream=1,
        )


def test_bounded_freshness_rejects_string_encoded_counts(
    tmp_path: Path,
) -> None:
    """Bounded 状态合同不得把字符串数字冒充规范整数。"""
    _write_segment(
        tmp_path,
        "gmo",
        "BTC",
        "run-current",
        [(_gmo_payload(), None, None)],
        schema_version=1,
    )
    checkpoint = _write_run_checkpoint(
        tmp_path, "gmo", "BTC", "run-current",
    )
    body = json.loads(checkpoint.read_text(encoding="utf-8"))
    body["sealed_segments"] = str(body["sealed_segments"])
    checkpoint.write_text(json.dumps(body), encoding="utf-8")

    with pytest.raises(ValueError, match="sealed_segments 必须为整数"):
        _sealed_inputs(
            tmp_path, latest_sealed_segments_per_stream=1,
        )


@pytest.mark.parametrize(
    ("run_started", "segment_started", "first_ingest"),
    (
        (
            BASE + timedelta(seconds=59),
            BASE + timedelta(seconds=58),
            BASE + timedelta(seconds=58),
        ),
        (
            BASE + timedelta(seconds=58),
            BASE + timedelta(seconds=59),
            BASE + timedelta(seconds=57),
        ),
    ),
)
def test_bounded_freshness_rejects_events_before_run_start(
    tmp_path: Path,
    run_started: datetime,
    segment_started: datetime,
    first_ingest: datetime,
) -> None:
    _write_segment(
        tmp_path, "gmo", "BTC", "run-current",
        [(_gmo_payload(), None, None)], schema_version=1,
    )
    manifest = next(tmp_path.rglob("segment-000001.manifest.json"))
    manifest_body = json.loads(manifest.read_text(encoding="utf-8"))
    manifest_body["started_at"] = segment_started.isoformat()
    manifest_body["first_ingest_time"] = first_ingest.isoformat()
    manifest.write_text(json.dumps(manifest_body), encoding="utf-8")
    _write_run_checkpoint(
        tmp_path, "gmo", "BTC", "run-current",
        started_at=run_started,
    )

    with pytest.raises(ValueError, match="时序倒置"):
        _sealed_inputs(
            tmp_path, latest_sealed_segments_per_stream=1,
        )


def test_bounded_freshness_rejects_checkpoint_before_included_seal(
    tmp_path: Path,
) -> None:
    _write_segment(
        tmp_path, "gmo", "BTC", "run-current",
        [(_gmo_payload(), None, None)], schema_version=1,
    )
    checkpoint = _write_run_checkpoint(
        tmp_path, "gmo", "BTC", "run-current",
    )
    body = json.loads(checkpoint.read_text(encoding="utf-8"))
    body["checkpoint_at"] = (BASE + timedelta(seconds=59)).isoformat()
    checkpoint.write_text(json.dumps(body), encoding="utf-8")

    with pytest.raises(ValueError, match="checkpoint 早于 segment seal"):
        _sealed_inputs(
            tmp_path, latest_sealed_segments_per_stream=1,
        )


@pytest.mark.parametrize(
    ("target", "field"),
    (
        ("manifest", "segment_sequence"),
        ("manifest", "byte_count"),
        ("manifest", "schema_version"),
        ("checkpoint", "schema_version"),
    ),
)
def test_bounded_freshness_rejects_string_encoded_identity_integers(
    tmp_path: Path,
    target: str,
    field: str,
) -> None:
    _write_segment(
        tmp_path, "gmo", "BTC", "run-current",
        [(_gmo_payload(), None, None)], schema_version=1,
    )
    checkpoint = _write_run_checkpoint(
        tmp_path, "gmo", "BTC", "run-current",
    )
    path = (
        next(tmp_path.rglob("segment-000001.manifest.json"))
        if target == "manifest" else checkpoint
    )
    body = json.loads(path.read_text(encoding="utf-8"))
    body[field] = str(body[field])
    path.write_text(json.dumps(body), encoding="utf-8")

    with pytest.raises(ValueError, match="必须为整数"):
        _sealed_inputs(
            tmp_path, latest_sealed_segments_per_stream=1,
        )


def test_bounded_freshness_cli_rejects_zero_and_mutually_exclusive_modes(
    tmp_path: Path,
) -> None:
    with pytest.raises(SystemExit) as zero:
        l2_module.main([
            "--data-root", str(tmp_path), "all",
            "--latest-sealed-segments-per-stream", "0",
        ])
    assert zero.value.code == 2

    with pytest.raises(SystemExit) as conflicting:
        l2_module.main([
            "--data-root", str(tmp_path), "all",
            "--latest-run-only",
            "--latest-sealed-segments-per-stream", "1",
        ])
    assert conflicting.value.code == 2


def test_l2_cli_rejects_abbreviated_identity_and_selection_options(
    tmp_path: Path,
) -> None:
    for argv in (
        ["--data-ro", str(tmp_path), "audit"],
        ["--data-root", str(tmp_path), "all", "--latest-run"],
        [
            "--data-root", str(tmp_path), "watch",
            "--latest-sealed-segments-per-str", "1",
        ],
    ):
        with pytest.raises(SystemExit) as rejected:
            l2_module.main(argv)
        assert rejected.value.code == 2


def test_l2_watch_owner_record_is_fixed_and_removed(tmp_path: Path) -> None:
    root = tmp_path / "data"
    root.mkdir()

    with l2_module._l2_watch_owner(root, selection="latest_run") as owner:
        owner_path = root / ".locks/l2-materializer-owner.json"
        recorded = json.loads(owner_path.read_text(encoding="utf-8"))
        assert recorded == owner
        assert recorded["schema_version"] == 1
        assert recorded["pid"] == os.getpid()
        assert recorded["selection"] == "latest_run"
        assert Path(recorded["data_root"]) == root.resolve()
        assert Path(recorded["executable_path"]) == Path(
            getattr(sys, "_base_executable", sys.executable)
        ).resolve()
        assert len(recorded["nonce"]) == 32

    assert not owner_path.exists()


@pytest.mark.skipif(os.name != "nt", reason="验证 Windows byte lock")
def test_direct_python_watch_rejects_concurrent_singleton_owner(
    tmp_path: Path,
) -> None:
    root = tmp_path / "data"
    root.mkdir()
    command = [
        sys.executable,
        "-m",
        "guvolu.data.l2_materialize",
        "--data-root",
        str(root),
        "watch",
        "--interval-seconds",
        "10",
    ]
    first = subprocess.Popen(
        command,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    owner_path = root / ".locks/l2-materializer-owner.json"
    owner_pid: int | None = None
    try:
        deadline = time.monotonic() + 15
        while not owner_path.exists() and time.monotonic() < deadline:
            time.sleep(0.05)
        owner = json.loads(owner_path.read_text(encoding="utf-8"))
        owner_pid = int(owner["pid"])
        assert owner["selection"] == "all"

        second = subprocess.run(
            [*command, "--latest-run-only"],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=15,
        )

        assert second.returncode != 0
        assert "singleton is already owned" in (
            second.stdout + second.stderr
        )
        unchanged = json.loads(owner_path.read_text(encoding="utf-8"))
        assert unchanged["pid"] == owner_pid
        assert unchanged["selection"] == "all"
    finally:
        if owner_pid is not None:
            subprocess.run(
                ["taskkill.exe", "/PID", str(owner_pid), "/T", "/F"],
                check=False,
                capture_output=True,
                text=True,
            )
        if first.poll() is None:
            first.terminate()
            first.wait(timeout=5)


def test_bounded_freshness_cli_materializes_only_selected_receipt(
    tmp_path: Path,
) -> None:
    for sequence in range(1, 4):
        _write_segment(
            tmp_path,
            "gmo",
            "BTC",
            "run-current",
            [(_gmo_payload(), None, None)],
            schema_version=1,
            segment_sequence=sequence,
        )
    _write_run_checkpoint(tmp_path, "gmo", "BTC", "run-current")
    newest_manifest = json.loads((
        tmp_path / "raw/realtime/book_l2/venue_id=gmo/venue_symbol=BTC"
        / "run_id=run-current/segment-000003.manifest.json"
    ).read_text(encoding="utf-8"))

    assert l2_module.main([
        "--data-root", str(tmp_path), "all",
        "--latest-sealed-segments-per-stream", "1",
    ]) == 0

    conn = store.connect(tmp_path)
    try:
        rows = conn.execute(
            "SELECT a.partition_key,a.input_set_hash,a.status,i.artifact_id "
            "FROM partition_attempt a JOIN partition_input i "
            "ON i.attempt_id=a.attempt_id WHERE a.domain='book_l2'"
        ).fetchall()
    finally:
        conn.close()
    assert len(rows) == 1
    assert rows[0][0] == "run-current/segment-000003"
    assert len(str(rows[0][1])) == 64
    int(str(rows[0][1]), 16)
    assert rows[0][2] in {"complete", "complete_with_rejections"}
    assert rows[0][3] == newest_manifest["artifact_id"]


def test_bounded_freshness_rechecks_selected_content_receipt(
    tmp_path: Path,
) -> None:
    _write_segment(
        tmp_path,
        "gmo",
        "BTC",
        "run-current",
        [(_gmo_payload(), None, None)],
        schema_version=1,
    )
    _write_run_checkpoint(tmp_path, "gmo", "BTC", "run-current")
    segment = next(tmp_path.rglob("segment-000001.jsonl"))
    original = segment.read_bytes()
    # 等长篡改，绕过字节数复核，专测散列复核。
    segment.write_bytes(b"X" * (len(original) - 1) + b"\n")

    with pytest.raises(ValueError, match="segment 散列不符"):
        _sealed_inputs(
            tmp_path, latest_sealed_segments_per_stream=1,
        )


def test_legacy_raw_v1_becomes_explicit_nullable_v3_fact(tmp_path: Path) -> None:
    _write_segment(
        tmp_path, "gmo", "BTC", "run-legacy-v1",
        [(_gmo_payload(), None, None)], schema_version=1,
    )

    result = _materialize(tmp_path)

    assert result.status == "complete"
    db = duckdb.connect(":memory:")
    try:
        row = db.execute(
            "SELECT schema_version,normalization_version,"
            "capability_revision,endpoint,endpoint_id,endpoint_revision,"
            "connection_id,channel_id,"
            "recv_ts_mono_ns,raw_payload_sha256,data_quality,source_level,"
            "source_publish_time,available_time>=event_time,"
            "source_session_id=run_id FROM read_parquet(?)",
            [str(tmp_path / result.frame_path)],
        ).fetchone()
    finally:
        db.close()
    assert row is not None
    assert row[:9] == (
        L2_SCHEMA_VERSION, L2_NORMALIZATION_VERSION, 0, "orderbooks/ws",
        None, None, None, None, None,
    )
    assert row[9] == hashlib.sha256(_gmo_payload().encode()).hexdigest()
    quality = set(json.loads(row[10]))
    assert {
        "connection_boundary_unknown", "raw_payload_hash_derived",
        "recv_ts_mono_missing", "endpoint_revision_unrecorded",
    } <= quality
    assert row[11:] == ("L2", BASE, True, True)
    manifest = json.loads(next(
        (tmp_path / result.frame_path).parent.glob("manifest-*.json")
    ).read_text(encoding="utf-8"))
    assert manifest["schema_version"] == 3
    assert manifest["normalization_version"] == "book-l2-normalization-v5"
    assert L2_NORMALIZATION_VERSION != BOOK_L2_V3_NORMALIZATION_VERSION
    assert manifest["input_schema_version"] == 1
    assert manifest["input_endpoint_binding"] == {
        "endpoint_id": None, "endpoint_revision": None,
    }
    conn = store.connect(tmp_path)
    try:
        audit = audit_l2(tmp_path, conn)
        control_counts = conn.execute(
            "SELECT (SELECT COUNT(*) FROM collection_connection),"
            "(SELECT COUNT(*) FROM collection_channel)"
        ).fetchone()
    finally:
        conn.close()
    assert audit["ok"] is True
    assert audit["frames"] == 1
    assert control_counts == (0, 0)


def test_bitbank_source_sequence_is_preserved_without_local_fact_inference(
    tmp_path: Path,
) -> None:
    run_id = "run-bitbank-v2"
    c1, c2 = f"{run_id}-c000001", f"{run_id}-c000002"
    payloads = [
        (_bitbank_payload("snapshot", 100, 0), c1, "depth_whole_btc_jpy"),
        (_bitbank_payload("delta", 100, 1), c1, "depth_diff_btc_jpy"),
        (_bitbank_payload("snapshot", 100, 2), c1, "depth_whole_btc_jpy"),
        (_bitbank_payload("delta", 101, 3), c1, "depth_diff_btc_jpy"),
        (_bitbank_payload("delta", 101, 4), c1, "depth_diff_btc_jpy"),
        (_bitbank_payload("delta", 102, 5), c1, "depth_diff_btc_jpy"),
        (_bitbank_payload("snapshot", 102, 6), c1, "depth_whole_btc_jpy"),
        (_bitbank_payload("snapshot", 102, 7), c1, "depth_whole_btc_jpy"),
        (_bitbank_payload("delta", 103, 8), c1, "depth_diff_btc_jpy"),
        (_bitbank_payload("delta", 200, 9), c2, "depth_diff_btc_jpy"),
        (_bitbank_payload("snapshot", 200, 10), c2, "depth_whole_btc_jpy"),
        (_bitbank_payload("delta", 201, 11), c2, "depth_diff_btc_jpy"),
        (_bitbank_payload("delta", 199, 12), c2, "depth_diff_btc_jpy"),
    ]
    _write_segment(
        tmp_path, "bitbank", "btc_jpy", run_id, payloads,
        schema_version=2,
    )

    result = _materialize(tmp_path)

    assert result.status == "complete"
    assert (result.source_rows, result.frame_rows, result.rejected_rows) == (
        13, 13, 0,
    )
    db = duckdb.connect(":memory:")
    try:
        rows = db.execute(
            "SELECT connection_id,sequence_id,prev_sequence_id,data_quality,"
            "endpoint_id,endpoint_revision,channel_id,recv_ts_mono_ns "
            "FROM read_parquet(?) ORDER BY source_row_index",
            [str(tmp_path / result.frame_path)],
        ).fetchall()
    finally:
        db.close()
    assert [row[1] for row in rows] == [
        "100", "100", "100", "101", "101", "102", "102",
        "102", "103", "200", "200", "201", "199",
    ]
    assert all(row[2] is None for row in rows)
    local_flags = {
        "cross_room_same_sequence_observed",
        "replay_untrusted_until_snapshot",
        "sequence_predecessor_untrusted",
    }
    assert all(not (set(json.loads(row[3])) & local_flags) for row in rows)
    assert all(row[4] == "EP-0005" for row in rows)
    assert all(row[5] is None for row in rows)
    assert all("endpoint_revision_unrecorded" in json.loads(row[3]) for row in rows)
    assert all(row[6] and row[7] is not None for row in rows)
def test_bitbank_delayed_whole_sequence_is_independent_authority(
    tmp_path: Path,
) -> None:
    run_id = "run-bitbank-delayed-whole-v3"
    connection = f"{run_id}-c000001"
    payloads = [
        (_bitbank_payload("delta", 6, 1), connection, "depth_diff_btc_jpy"),
        (_bitbank_payload("delta", 8, 2), connection, "depth_diff_btc_jpy"),
        (_bitbank_payload("snapshot", 5, 3), connection, "depth_whole_btc_jpy"),
        (_bitbank_payload("delta", 9, 4), connection, "depth_diff_btc_jpy"),
    ]
    _write_segment(
        tmp_path, "bitbank", "btc_jpy", run_id, payloads,
        schema_version=3,
    )

    result = _materialize(tmp_path)

    assert result.status == "complete"
    assert (result.source_rows, result.frame_rows, result.rejected_rows) == (
        4, 4, 0,
    )
    db = duckdb.connect(":memory:")
    try:
        rows = db.execute(
            "SELECT message_kind,sequence_id,prev_sequence_id,data_quality "
            "FROM read_parquet(?) ORDER BY source_row_index",
            [str(tmp_path / result.frame_path)],
        ).fetchall()
    finally:
        db.close()
    assert [(row[0], row[1], row[2]) for row in rows] == [
        ("delta", "6", None),
        ("delta", "8", None),
        ("snapshot", "5", None),
        ("delta", "9", None),
    ]
    local_flags = {
        "cross_room_same_sequence_observed",
        "replay_untrusted_until_snapshot",
        "sequence_predecessor_untrusted",
    }
    assert all(not (set(json.loads(row[3])) & local_flags) for row in rows)


def test_bitflyer_snapshot_zero_levels_are_ignored_not_set(
    tmp_path: Path,
) -> None:
    run_id = "run-bitflyer-v2"
    _write_segment(
        tmp_path, "bitflyer", "BTC_JPY", run_id,
        [(
            _bitflyer_payload(), f"{run_id}-c000001",
            "lightning_board_snapshot_BTC_JPY",
        )],
        schema_version=2,
    )

    result = _materialize(tmp_path)

    assert (result.source_rows, result.frame_rows, result.level_rows) == (1, 1, 2)
    db = duckdb.connect(":memory:")
    try:
        levels = db.execute(
            "SELECT size,action,order_count FROM read_parquet(?) "
            "ORDER BY side,source_level_index",
            [str(tmp_path / result.level_path)],
        ).fetchall()
        quality = db.execute(
            "SELECT data_quality,source_publish_time FROM read_parquet(?)",
            [str(tmp_path / result.frame_path)],
        ).fetchone()
    finally:
        db.close()
    assert levels == [("2", "set", None), ("1", "set", None)]
    assert quality is not None and quality[1] is None
    assert {
        "snapshot_zero_levels_ignored", "source_publish_time_missing",
        "raw_payload_hash_verified", "endpoint_revision_unrecorded",
    } <= set(json.loads(quality[0]))


def test_raw_v3_binds_recorded_endpoint_revision_without_latest_lookup(
    tmp_path: Path,
) -> None:
    """v3 尚未投产，可在保持 schema=3 时补齐端点修订必填列。"""
    run_id = "run-gmo-v3-endpoint-r0"
    _write_segment(
        tmp_path, "gmo", "BTC", run_id,
        [(_gmo_payload(), f"{run_id}-c000001", "orderbooks")],
        schema_version=3,
    )

    result = _materialize(tmp_path)

    db = duckdb.connect(":memory:")
    try:
        row = db.execute(
            "SELECT endpoint_id,endpoint_revision,data_quality "
            "FROM read_parquet(?)", [str(tmp_path / result.frame_path)],
        ).fetchone()
    finally:
        db.close()
    assert row is not None
    assert row[:2] == ("EP-0007", 0)
    assert "endpoint_revision_unrecorded" not in json.loads(row[2])
    manifest = json.loads(next(
        (tmp_path / result.frame_path).parent.glob("manifest-*.json")
    ).read_text(encoding="utf-8"))
    assert manifest["input_endpoint_binding"] == {
        "endpoint_id": "EP-0007", "endpoint_revision": 0,
    }
    conn = store.connect(tmp_path)
    try:
        connection = conn.execute(
            "SELECT endpoint_id,endpoint_revision,collection_run_id,"
            "connection_ordinal,opened_at_basis FROM collection_connection"
        ).fetchone()
        channel = conn.execute(
            "SELECT channel_id,market_id,subscribed_at_basis,"
            "capability_domain,capability_endpoint,capability_revision "
            "FROM collection_channel"
        ).fetchone()
    finally:
        conn.close()
    assert connection == (
        "EP-0007", 0, run_id, 1,
        "first_successfully_materialized_raw_v3_frame",
    )
    assert channel == (
        "orderbooks", "mkt__gmo__btc__r0",
        "first_successfully_materialized_raw_v3_frame",
        "book_realtime", "orderbooks/ws", 0,
    )


def test_raw_v2_payload_hash_is_verified() -> None:
    descriptor = _DESCRIPTORS["gmo"]
    envelope = {
        "schema_version": 2, "run_id": "run-hash-v2",
        "segment_sequence": 1, "venue_id": "gmo", "venue_symbol": "BTC",
        "domain": "book_l2", "source_endpoint": descriptor.endpoint,
        "endpoint_id": descriptor.endpoint_id,
        "connection_id": "run-hash-v2-c000001", "channel_id": "orderbooks",
        "payload_raw": _gmo_payload(), "raw_payload_sha256": "0" * 64,
        "recv_ts_utc": BASE.isoformat(), "ingest_time": BASE.isoformat(),
        "recv_ts_mono_ns": 1,
    }
    item = type("Item", (), {
        "raw_schema_version": 2,
        "endpoint_id": descriptor.endpoint_id,
        "run_id": "run-hash-v2",
    })()

    with pytest.raises(ValueError, match="SHA-256"):
        _raw_metadata(envelope, item, descriptor)


def test_raw_v3_rejects_endpoint_revision_not_recorded_by_manifest() -> None:
    descriptor = _DESCRIPTORS["gmo"]
    envelope = {
        "schema_version": 3, "run_id": "run-endpoint-r0",
        "segment_sequence": 1, "venue_id": "gmo", "venue_symbol": "BTC",
        "domain": "book_l2", "source_endpoint": descriptor.endpoint,
        "endpoint_id": descriptor.endpoint_id, "endpoint_revision": 1,
        "connection_id": "run-endpoint-r0-c000001", "channel_id": "orderbooks",
        "payload_raw": _gmo_payload(),
        "raw_payload_sha256": hashlib.sha256(
            _gmo_payload().encode("utf-8")
        ).hexdigest(),
        "recv_ts_utc": BASE.isoformat(), "ingest_time": BASE.isoformat(),
        "recv_ts_mono_ns": 1,
    }
    item = type("Item", (), {
        "raw_schema_version": 3,
        "endpoint_id": descriptor.endpoint_id,
        "endpoint_revision": 0,
        "run_id": "run-endpoint-r0",
    })()

    with pytest.raises(ValueError, match="endpoint_revision"):
        _raw_metadata(envelope, item, descriptor)


def _count_hashes(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """统计 sha256_file 实际调用次数。"""
    calls = [0]
    original = l2_module.sha256_file

    def counted(path: Path) -> str:
        calls[0] += 1
        return original(path)

    monkeypatch.setattr(l2_module, "sha256_file", counted)
    return calls


def test_l2_registered_hash_prefilter_reuses_completed_input_hash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """已完成 L2 attempt 的输入不再重算散列。"""
    _write_segment(
        tmp_path, "gmo", "BTC", "run-prefilter",
        [(_gmo_payload(), "run-prefilter-c000001", "orderbooks")],
        schema_version=3,
    )
    conn = store.connect(tmp_path)
    try:
        assert len(
            l2_module.materialize_all(tmp_path, conn, report_reused=False)
        ) == 1
        calls = _count_hashes(monkeypatch)
        results, stats = l2_module._materialize_cycle(
            tmp_path, conn, report_reused=False,
        )
        assert [result.reused for result in results] == [True]
        assert calls[0] == 0
        assert (stats.hash_reused, stats.hash_recomputed) == (1, 0)
        assert stats.scanned_manifests == 1
        assert stats.elapsed_scan_seconds >= 0
    finally:
        conn.close()


def test_l2_verify_all_hashes_restores_full_recompute(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """审计开关关闭预筛，回到逐个重算。"""
    _write_segment(
        tmp_path, "gmo", "BTC", "run-audit",
        [(_gmo_payload(), "run-audit-c000001", "orderbooks")],
        schema_version=3,
    )
    conn = store.connect(tmp_path)
    try:
        l2_module.materialize_all(tmp_path, conn, report_reused=False)
        calls = _count_hashes(monkeypatch)
        _, stats = l2_module._materialize_cycle(
            tmp_path, conn, report_reused=False, verify_all_hashes=True,
        )
        assert calls[0] == 1
        assert (stats.hash_reused, stats.hash_recomputed) == (0, 1)
    finally:
        conn.close()


def test_l2_prefilter_still_fails_closed_on_size_mismatch(
    tmp_path: Path,
) -> None:
    """预筛不得让磁盘字节数漂移逃过失败关闭。"""
    _write_segment(
        tmp_path, "gmo", "BTC", "run-size-drift",
        [(_gmo_payload(), "run-size-drift-c000001", "orderbooks")],
        schema_version=3,
    )
    conn = store.connect(tmp_path)
    try:
        l2_module.materialize_all(tmp_path, conn, report_reused=False)
        registered = l2_module._registered_input_hashes(conn)
        assert len(registered) == 1
        segment = tmp_path / next(iter(registered))
        with segment.open("ab") as stream:
            stream.write(b"\n")
        with pytest.raises(ValueError, match="字节数不符"):
            _sealed_inputs(tmp_path, registered_hashes=registered)
    finally:
        conn.close()


def test_l2_prefilter_rejects_manifest_rewritten_against_registry(
    tmp_path: Path,
) -> None:
    """manifest 与登记散列不符时预筛抛错，不静默复用。"""
    _write_segment(
        tmp_path, "gmo", "BTC", "run-rewritten",
        [(_gmo_payload(), "run-rewritten-c000001", "orderbooks")],
        schema_version=3,
    )
    conn = store.connect(tmp_path)
    try:
        l2_module.materialize_all(tmp_path, conn, report_reused=False)
        registered = l2_module._registered_input_hashes(conn)
        manifest_path = next(
            tmp_path.rglob("segment-000001.manifest.json")
        )
        body = json.loads(manifest_path.read_text(encoding="utf-8"))
        forged = hashlib.sha256(b"forged").hexdigest()
        body.update({"sha256": forged, "artifact_id": f"sha256-{forged}"})
        manifest_path.write_text(json.dumps(body), encoding="utf-8")
        with pytest.raises(ValueError, match="登记散列不符"):
            _sealed_inputs(tmp_path, registered_hashes=registered)
    finally:
        conn.close()


def _write_legacy_run_manifest(root: Path, venue: str, symbol: str, run_id: str) -> Path:
    """写一个 v3 之前的终态 run 状态合同。"""
    directory = (
        root / "raw/realtime/book_l2" / f"venue_id={venue}"
        / f"venue_symbol={symbol}" / f"run_id={run_id}"
    )
    directory.mkdir(parents=True, exist_ok=True)
    body = {
        "schema_version": 1, "status": "complete", "completion_claim": True,
        "run_id": run_id, "venue_id": venue, "venue_symbol": symbol,
        "domain": "book_l2",
        "started_at": (BASE - timedelta(days=10)).isoformat(),
        "finished_at": (BASE - timedelta(days=10, minutes=-1)).isoformat(),
        "record_count": 0, "segment_count": 0, "segments": [],
    }
    path = directory / "run.manifest.json"
    path.write_text(json.dumps(body), encoding="utf-8")
    return directory


def test_bounded_selection_skips_legacy_run_directories(
    tmp_path: Path,
) -> None:
    """旧版 run 不参选，不再使整轮 bounded 选择失败。"""
    _write_legacy_run_manifest(tmp_path, "gmo", "BTC", "run-legacy-v1x")
    _write_segment(
        tmp_path, "gmo", "BTC", "run-live",
        [(_gmo_payload(), "run-live-c000001", "orderbooks")],
        schema_version=3,
    )
    _write_run_checkpoint(tmp_path, "gmo", "BTC", "run-live")
    selected = _sealed_inputs(tmp_path, latest_sealed_segments_per_stream=1)
    assert [item.run_id for item in selected] == ["run-live"]


def test_bounded_selection_fails_when_stream_has_only_legacy_runs(
    tmp_path: Path,
) -> None:
    """全旧版流没有 v3 候选，仍响亮失败不静默跳过。"""
    _write_legacy_run_manifest(tmp_path, "gmo", "BTC", "run-legacy-only")
    with pytest.raises(ValueError):
        _sealed_inputs(tmp_path, latest_sealed_segments_per_stream=1)
