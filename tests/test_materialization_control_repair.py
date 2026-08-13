from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

import pytest

from guvolu.data import materialize, store
from guvolu.data.materialize import (
    _register_content_artifact,
    artifact_id,
    audit_materializations,
    ensure_markets,
    repair_materialization_controls,
)
from guvolu.venues import registry


def _write_schema_parquet(
    path: Path, schema_version: int = 1, marker: str = "fixture",
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    db = materialize.open_analytics()
    try:
        db.execute(
            f"COPY (SELECT {schema_version}::INTEGER AS schema_version, "
            f"'{marker}'::VARCHAR AS marker) "
            f"TO '{path.as_posix()}' (FORMAT PARQUET)"
        )
    finally:
        db.close()


def _attempt(
    conn: sqlite3.Connection, attempt_id: str, market_id: str, status: str,
    domain: str = "orderflow_tile",
) -> None:
    conn.execute(
        "INSERT INTO partition_attempt "
        "(attempt_id,market_id,domain,partition_key,normalization_version,"
        "input_set_hash,status,source_rows,normalized_rows,ignored_rows,"
        "rejected_rows,started_at,finished_at,code_version,config_hash) "
        "VALUES (?,?,?,?,'fixture-v1',?, ?,1,1,0,0,?,?,"
        "'fixture',?)",
        (
            attempt_id, market_id, domain, attempt_id, "a" * 64, status,
            "2026-08-13T00:00:00+00:00",
            "2026-08-13T00:00:01+00:00", "b" * 64,
        ),
    )


def _book_state_manifest_fixture(
    tmp_path: Path, *, domain: str = "book_state",
) -> tuple[Path, sqlite3.Connection, Path, dict[str, object]]:
    root = tmp_path / "data"
    conn = store.connect(root)
    registry.register_all(conn)
    ensure_markets(conn)
    market_id = str(conn.execute(
        "SELECT market_id FROM market WHERE venue_id='gmo' ORDER BY market_id"
    ).fetchone()[0])
    attempt_id = "fixture-invalid-manifest"
    _attempt(conn, attempt_id, market_id, "complete", domain)

    source = root / "archive" / "invalid-manifest-source.bin"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"invalid manifest source")
    source_sha = hashlib.sha256(source.read_bytes()).hexdigest()
    source_id = artifact_id(source_sha)
    _register_content_artifact(
        conn, source_id, "raw_archive",
        source.relative_to(root).as_posix(), source_sha,
        source.stat().st_size, "2026-08-13T00:00:00+00:00", 1,
    )
    conn.execute(
        "INSERT INTO partition_input "
        "(attempt_id,artifact_id,source_rows,normalized_rows,ignored_rows,"
        "rejected_rows) VALUES (?,?,?,?,?,?)",
        (attempt_id, source_id, 1, 1, 0, 0),
    )

    output = (
        root / "materialized" / "book_state_checkpoint"
        / "schema_version=1" / "part-invalid-manifest.parquet"
    )
    _write_schema_parquet(output)
    output_sha = hashlib.sha256(output.read_bytes()).hexdigest()
    output_id = artifact_id(output_sha)
    output_storage = output.relative_to(root).as_posix()
    _register_content_artifact(
        conn, output_id, "materialized_parquet", output_storage, output_sha,
        output.stat().st_size, "2026-08-13T00:00:01+00:00", 1,
    )
    conn.execute(
        "INSERT INTO materialization_output "
        "(attempt_id,artifact_id,dataset,row_count,min_event_time,"
        "max_event_time,created_at) VALUES (?,?,?,?,?,?,?)",
        (
            attempt_id, output_id, "book_state_checkpoint", 1,
            "2026-08-13T00:00:00+00:00",
            "2026-08-13T00:00:01+00:00",
            "2026-08-13T00:00:01+00:00",
        ),
    )
    manifest = (
        output.parent / f"manifest-{attempt_id}.json"
    )
    manifest.parent.mkdir(parents=True, exist_ok=True)
    body: dict[str, object] = {
        "attempt_id": attempt_id,
        "status": "complete",
        "market_id": market_id,
        "partition_key": attempt_id,
        "normalization_version": "fixture-v1",
        "upstream_attempt_ids": [],
        "input_artifact_ids": [source_id],
        "rows": 1,
        "output": output_storage,
    }
    conn.commit()
    return root, conn, manifest, body


@pytest.mark.parametrize(
    ("fault", "message"),
    [
        ("market", "物化清单市场与尝试不符"),
        ("version", "物化清单规范化版本不符"),
        ("input", "物化清单输入集合与台账不符"),
        ("output_rows", "物化清单输出计数或位置不符"),
        ("unknown_domain", "物化清单修复域不受支持"),
    ],
)
def test_control_repair_rejects_untrusted_complete_manifest_before_write(
    tmp_path: Path, fault: str, message: str,
) -> None:
    domain = "trade" if fault == "unknown_domain" else "book_state"
    root, conn, manifest, body = _book_state_manifest_fixture(
        tmp_path, domain=domain,
    )
    if fault == "market":
        body["market_id"] = "invalid-market"
    elif fault == "version":
        body["normalization_version"] = "fixture-v2"
    elif fault == "input":
        body["input_artifact_ids"] = []
    elif fault == "output_rows":
        body["rows"] = 2
    manifest.write_text(json.dumps(body) + "\n", encoding="utf-8")

    with pytest.raises(ValueError, match=message):
        repair_materialization_controls(root, conn, apply=True)

    storage_path = manifest.relative_to(root).as_posix()
    assert conn.execute(
        "SELECT COUNT(*) FROM artifact WHERE artifact_kind IN "
        "('materialization_manifest','failed_materialization_manifest')"
    ).fetchone()[0] == 0
    assert conn.execute(
        "SELECT COUNT(*) FROM artifact_location WHERE storage_path=?",
        (storage_path,),
    ).fetchone()[0] == 0
    conn.close()


def test_control_repair_is_verified_additive_and_idempotent(
    tmp_path: Path,
) -> None:
    root = tmp_path / "data"
    conn = store.connect(root)
    registry.register_all(conn)
    ensure_markets(conn)
    market_id = str(conn.execute(
        "SELECT market_id FROM market WHERE venue_id='gmo' ORDER BY market_id"
    ).fetchone()[0])
    complete = "fixture-complete"
    failed = "fixture-failed"
    _attempt(conn, complete, market_id, "complete", "book_state")
    _attempt(conn, failed, market_id, "failed")

    source = root / "archive" / "fixture.bin"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"verified source")
    source_sha = hashlib.sha256(source.read_bytes()).hexdigest()
    source_id = artifact_id(source_sha)
    _register_content_artifact(
        conn, source_id, "raw_archive",
        source.relative_to(root).as_posix(), source_sha,
        source.stat().st_size, "2026-08-13T00:00:00+00:00", 1,
    )
    conn.execute(
        "INSERT INTO partition_input VALUES (?,?,?,?,?,?)",
        (complete, source_id, 1, 1, 0, 0),
    )
    output = (
        root / "materialized" / "book_state_checkpoint"
        / "schema_version=1" / "part-fixture-complete.parquet"
    )
    _write_schema_parquet(output)
    output_sha = hashlib.sha256(output.read_bytes()).hexdigest()
    output_id = artifact_id(output_sha)
    output_storage = output.relative_to(root).as_posix()
    _register_content_artifact(
        conn, output_id, "materialized_parquet", output_storage, output_sha,
        output.stat().st_size, "2026-08-13T00:00:01+00:00", 1,
    )
    conn.execute(
        "INSERT INTO materialization_output VALUES (?,?,?,?,?,?,?)",
        (
            complete, output_id, "book_state_checkpoint", 1,
            "2026-08-13T00:00:00+00:00",
            "2026-08-13T00:00:01+00:00",
            "2026-08-13T00:00:01+00:00",
        ),
    )
    failed_outputs: list[dict[str, object]] = []
    failed_parent = (
        root / "materialized" / "orderflow_tile" / "schema_version=1"
        / "fixture"
    )
    for dataset in ("orderflow_tile_column", "orderflow_tile_cell"):
        failed_output = failed_parent / f"{dataset}-fixture.parquet"
        _write_schema_parquet(failed_output, marker=dataset)
        failed_sha = hashlib.sha256(failed_output.read_bytes()).hexdigest()
        failed_id = artifact_id(failed_sha)
        failed_storage = failed_output.relative_to(root).as_posix()
        _register_content_artifact(
            conn, failed_id, "materialized_parquet", failed_storage,
            failed_sha, failed_output.stat().st_size,
            "2026-08-13T00:00:01+00:00", 1,
        )
        failed_outputs.append({
            "artifact_id": failed_id,
            "dataset": dataset,
            "output": failed_storage,
            "row_count": 1,
            "schema_version": 1,
            "sha256": failed_sha,
        })
    for attempt_id in (complete, failed):
        parent = output.parent if attempt_id == complete else failed_parent
        path = parent / f"manifest-{attempt_id}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        body: dict[str, object] = {
            "attempt_id": attempt_id, "status": "complete",
        }
        if attempt_id == complete:
            body.update({
                "market_id": market_id, "partition_key": complete,
                "normalization_version": "fixture-v1",
                "upstream_attempt_ids": [],
                "input_artifact_ids": [source_id], "rows": 1,
                "output": output_storage,
            })
        else:
            body.update({
                "status": "failed",
                "market_id": market_id,
                "partition_key": failed,
                "method_version": "fixture-v1",
                "upstream_attempt_ids": [],
                "input_artifact_ids": [],
                "failed_at": "2026-08-13T00:00:01+00:00",
                "failure_detail": "fixture failure",
                "non_promoted_outputs": failed_outputs,
            })
        path.write_text(json.dumps(body) + "\n", encoding="utf-8")
    conn.commit()

    plan = repair_materialization_controls(root, conn)
    assert plan == {
        "applied": False,
        "unregistered_manifests_found": 2,
        "complete_attempt_manifests_found": 1,
        "failed_attempt_manifests_found": 1,
        "legacy_input_bindings_found": 1,
        "manifest_artifacts_repaired": 0,
        "legacy_input_bindings_repaired": 0,
    }
    applied = repair_materialization_controls(root, conn, apply=True)
    assert applied["manifest_artifacts_repaired"] == 2
    assert applied["legacy_input_bindings_repaired"] == 1
    assert conn.execute(
        "SELECT COUNT(*) FROM artifact WHERE artifact_kind="
        "'materialization_manifest'"
    ).fetchone()[0] == 1
    assert conn.execute(
        "SELECT COUNT(*) FROM artifact WHERE artifact_kind="
        "'failed_materialization_manifest'"
    ).fetchone()[0] == 1
    assert conn.execute(
        "SELECT storage_path,source_rows,normalized_rows,ignored_rows,"
        "rejected_rows FROM partition_input_binding WHERE attempt_id=?",
        (complete,),
    ).fetchone() == (
        "archive/fixture.bin", 1, 1, 0, 0,
    )

    alternate = root / "archive" / "fixture-alternate.bin"
    alternate.write_bytes(source.read_bytes())
    _register_content_artifact(
        conn, source_id, "raw_archive",
        alternate.relative_to(root).as_posix(), source_sha,
        alternate.stat().st_size, "2026-08-13T00:00:00+00:00", 1,
    )
    conn.execute(
        "DELETE FROM partition_input_binding WHERE attempt_id=?", (complete,),
    )
    conn.execute(
        "INSERT INTO partition_input_binding "
        "(attempt_id,artifact_id,storage_path,source_rows,normalized_rows,"
        "ignored_rows,rejected_rows) VALUES (?,?,?,?,?,?,?)",
        (
            complete, source_id, "archive/fixture-alternate.bin",
            1, 1, 0, 0,
        ),
    )
    conn.execute(
        "UPDATE partition_input SET source_rows=2,ignored_rows=2,"
        "rejected_rows=3 "
        "WHERE attempt_id=?", (complete,),
    )
    conn.commit()
    with pytest.raises(
        ValueError,
        match=f"旧输入内容台账与尝试计数不符: {complete}",
    ):
        repair_materialization_controls(root, conn)
    conn.execute(
        "UPDATE partition_attempt SET source_rows=2,ignored_rows=2,"
        "rejected_rows=3 WHERE attempt_id=?", (complete,),
    )
    conn.commit()
    mismatch = repair_materialization_controls(root, conn)
    assert mismatch["legacy_input_bindings_found"] == 1
    corrected = repair_materialization_controls(root, conn, apply=True)
    assert corrected["legacy_input_bindings_repaired"] == 1
    assert conn.execute(
        "SELECT storage_path,source_rows,normalized_rows,ignored_rows,"
        "rejected_rows FROM partition_input_binding "
        "WHERE attempt_id=?", (complete,),
    ).fetchone() == (
        "archive/fixture-alternate.bin", 2, 1, 2, 3,
    )
    assert conn.execute(
        "SELECT COUNT(*) FROM partition_input_binding WHERE attempt_id=?",
        (complete,),
    ).fetchone()[0] == 1
    replay = repair_materialization_controls(root, conn, apply=True)
    assert replay["unregistered_manifests_found"] == 0
    assert replay["legacy_input_bindings_found"] == 0
    assert replay["manifest_artifacts_repaired"] == 0
    assert replay["legacy_input_bindings_repaired"] == 0

    conn.execute(
        "UPDATE partition_input SET normalized_rows=2 WHERE attempt_id=?",
        (complete,),
    )
    conn.commit()
    cardinality_change = repair_materialization_controls(root, conn)
    assert cardinality_change["legacy_input_bindings_found"] == 1
    conn.close()


def test_control_repair_rejects_direct_domain_normalized_mismatch(
    tmp_path: Path,
) -> None:
    root = tmp_path / "data"
    conn = store.connect(root)
    registry.register_all(conn)
    ensure_markets(conn)
    market_id = str(conn.execute(
        "SELECT market_id FROM market WHERE venue_id='gmo' ORDER BY market_id"
    ).fetchone()[0])
    attempt_id = "fixture-direct-mismatch"
    _attempt(conn, attempt_id, market_id, "complete", "trade")
    source = root / "archive" / "direct-mismatch.bin"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"direct mismatch")
    source_sha = hashlib.sha256(source.read_bytes()).hexdigest()
    source_id = artifact_id(source_sha)
    _register_content_artifact(
        conn, source_id, "raw_archive",
        source.relative_to(root).as_posix(), source_sha,
        source.stat().st_size, "2026-08-13T00:00:00+00:00", 1,
    )
    conn.execute(
        "INSERT INTO partition_input "
        "(attempt_id,artifact_id,source_rows,normalized_rows,ignored_rows,"
        "rejected_rows) VALUES (?,?,?,?,?,?)",
        (attempt_id, source_id, 1, 2, 0, 0),
    )
    conn.commit()

    with pytest.raises(
        ValueError,
        match=f"旧输入内容台账与尝试计数不符: {attempt_id}",
    ):
        repair_materialization_controls(root, conn)
    conn.close()


def test_audit_uses_closing_registration_watermark(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "data"
    conn = store.connect(root)
    registry.register_all(conn)
    ensure_markets(conn)
    market_id = str(conn.execute(
        "SELECT market_id FROM market WHERE venue_id='gmo' ORDER BY market_id"
    ).fetchone()[0])
    attempt_id = "fixture-concurrent"
    _attempt(conn, attempt_id, market_id, "failed")
    seed = root / "archive" / "seed.bin"
    seed.parent.mkdir(parents=True)
    seed.write_bytes(b"seed")
    seed_sha = hashlib.sha256(seed.read_bytes()).hexdigest()
    _register_content_artifact(
        conn, artifact_id(seed_sha), "raw_archive",
        seed.relative_to(root).as_posix(), seed_sha, seed.stat().st_size,
        "2026-08-13T00:00:00+00:00", 1,
    )
    manifest = (
        root / "materialized" / "fixture" / "schema_version=1"
        / f"manifest-{attempt_id}.json"
    )
    manifest.parent.mkdir(parents=True)
    manifest.write_text(json.dumps({
        "attempt_id": attempt_id,
        "status": "complete",
    }) + "\n", encoding="utf-8")
    conn.commit()

    original = materialize.sha256_file
    registered = False

    def register_during_hash(path: Path) -> str:
        nonlocal registered
        if not registered:
            registered = True
            materialize.register_materialization_manifest(
                root, conn, manifest, 1,
                "2026-08-13T00:00:00+00:00",
            )
            conn.commit()
        return original(path)

    monkeypatch.setattr(materialize, "sha256_file", register_during_hash)
    report = audit_materializations(root, conn)
    assert not any(
        error.startswith("未登记物化终态文件:") for error in report.errors
    )
    assert any(
        warning.startswith("审计期间新增制品登记:")
        for warning in report.warnings
    )
    conn.close()


def _raw_anchor_file(
    root: Path, body: bytes, *, filename_sha: str | None = None,
) -> tuple[Path, str]:
    """写入最小内容寻址 REST 锚点原件。"""
    sha = hashlib.sha256(body).hexdigest()
    name_sha = filename_sha or sha
    path = (
        root / "raw" / "rest" / "book_l2_anchor"
        / "schema_version=3" / "venue_id=gmo" / "venue_symbol=BTC"
        / "day=2026-08-13" / f"sha256-{name_sha}.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(body)
    return path, sha


def test_audit_requires_raw_anchor_content_registration(
    tmp_path: Path,
) -> None:
    root = tmp_path / "data"
    conn = store.connect(root)
    path, sha = _raw_anchor_file(root, b'{"fixture":"orphan"}\n')
    recorded = path.relative_to(root).as_posix()

    missing = audit_materializations(root, conn)

    assert f"未登记 REST 锚点 raw: {recorded}" in missing.errors
    _register_content_artifact(
        conn, artifact_id(sha), "raw_rest_l2_anchor", recorded, sha,
        path.stat().st_size, "2026-08-13T00:00:00+00:00", 3,
    )
    conn.commit()

    registered = audit_materializations(root, conn)

    assert not registered.errors
    conn.close()


@pytest.mark.parametrize(
    ("fault", "expected"),
    [
        ("filename", "REST 锚点 raw 文件名散列不符:"),
        ("identity", "REST 锚点 raw 制品身份不符:"),
        ("kind", "REST 锚点 raw 制品类型不符:"),
        ("bytes", "REST 锚点 raw 制品字节数不符:"),
    ],
)
def test_audit_validates_raw_anchor_content_contract(
    tmp_path: Path, fault: str, expected: str,
) -> None:
    root = tmp_path / "data"
    conn = store.connect(root)
    filename_sha = "0" * 64 if fault == "filename" else None
    path, sha = _raw_anchor_file(
        root, b'{"fixture":"contract"}\n', filename_sha=filename_sha,
    )
    recorded = path.relative_to(root).as_posix()
    registered_sha = (
        hashlib.sha256(b'{"fixture":"different"}\n').hexdigest()
        if fault == "identity" else sha
    )
    kind = "raw_archive" if fault == "kind" else "raw_rest_l2_anchor"
    byte_count = path.stat().st_size + (1 if fault == "bytes" else 0)
    _register_content_artifact(
        conn, artifact_id(registered_sha), kind, recorded, registered_sha,
        byte_count, "2026-08-13T00:00:00+00:00", 3,
    )
    conn.commit()

    report = audit_materializations(root, conn)

    assert any(error.startswith(expected) for error in report.errors)
    conn.close()


def test_raw_anchor_audit_uses_stable_closing_watermark(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "data"
    conn = store.connect(root)
    seed = root / "archive" / "seed.bin"
    seed.parent.mkdir(parents=True)
    seed.write_bytes(b"seed")
    seed_sha = hashlib.sha256(seed.read_bytes()).hexdigest()
    _register_content_artifact(
        conn, artifact_id(seed_sha), "raw_archive",
        seed.relative_to(root).as_posix(), seed_sha, seed.stat().st_size,
        "2026-08-13T00:00:00+00:00", 1,
    )
    late_path, late_sha = _raw_anchor_file(
        root, b'{"fixture":"late-registration"}\n',
    )
    late_recorded = late_path.relative_to(root).as_posix()
    conn.commit()

    original = materialize.sha256_file
    added = False
    new_path: Path | None = None

    def publish_during_hash(path: Path) -> str:
        nonlocal added, new_path
        if not added:
            added = True
            _register_content_artifact(
                conn, artifact_id(late_sha), "raw_rest_l2_anchor",
                late_recorded, late_sha, late_path.stat().st_size,
                "2026-08-13T00:00:00+00:00", 3,
            )
            conn.commit()
            new_path, _ = _raw_anchor_file(
                root, b'{"fixture":"after-opening-watermark"}\n',
            )
        return original(path)

    monkeypatch.setattr(materialize, "sha256_file", publish_during_hash)

    report = audit_materializations(root, conn)

    assert not report.errors
    assert any(
        warning.startswith("审计期间新增制品登记:")
        for warning in report.warnings
    )
    assert new_path is not None

    followup = audit_materializations(root, conn)

    new_recorded = new_path.relative_to(root).as_posix()
    assert f"未登记 REST 锚点 raw: {new_recorded}" in followup.errors
    conn.close()
