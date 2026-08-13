from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path

from guvolu.data import store
from guvolu.data.materialize import (
    _register_content_artifact,
    artifact_id,
    audit_materializations,
    ensure_markets,
)
from guvolu.venues import registry


def _attempt(
    conn: sqlite3.Connection,
    attempt_id: str,
    market_id: str,
    domain: str,
) -> None:
    conn.execute(
        "INSERT INTO partition_attempt "
        "(attempt_id,market_id,domain,partition_key,normalization_version,"
        "input_set_hash,status,source_rows,normalized_rows,ignored_rows,"
        "rejected_rows,started_at,finished_at,code_version,config_hash) "
        "VALUES (?,?,?,?,?,'fixture-input','complete',1,1,0,0,?,?,'fixture',"
        "'fixture-config')",
        (
            attempt_id, market_id, domain, attempt_id, "fixture-v1",
            "2026-08-13T00:00:00+00:00",
            "2026-08-13T00:00:01+00:00",
        ),
    )


def _artifact(
    root: Path,
    conn: sqlite3.Connection,
    relative: str,
    body: bytes,
    kind: str,
) -> str:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(body)
    sha = hashlib.sha256(body).hexdigest()
    identity = artifact_id(sha)
    _register_content_artifact(
        conn, identity, kind, relative, sha, len(body),
        "2026-08-13T00:00:00+00:00", 1,
    )
    return identity


def _fixture(
    root: Path,
) -> tuple[sqlite3.Connection, str, str, str]:
    conn = store.connect(root)
    registry.register_all(conn)
    ensure_markets(conn)
    market_id = str(conn.execute(
        "SELECT market_id FROM market WHERE venue_id='gmo' ORDER BY market_id"
    ).fetchone()[0])
    root_attempt = "fixture-root"
    derived_attempt = "fixture-derived"
    _attempt(conn, root_attempt, market_id, "trade")
    _attempt(conn, derived_attempt, market_id, "book_state")
    raw_id = _artifact(
        root, conn, "archive/dependency-fixture.raw", b"raw", "raw_archive",
    )
    output_path = "materialized/fixture/schema_version=1/output.bin"
    output_id = _artifact(
        root, conn, output_path, b"normalized", "materialized_parquet",
    )
    conn.execute(
        "INSERT INTO partition_input VALUES (?,?,?,?,?,?)",
        (root_attempt, raw_id, 1, 1, 0, 0),
    )
    conn.execute(
        "INSERT INTO partition_input_binding VALUES (?,?,?,?,?,?,?)",
        (
            root_attempt, raw_id, "archive/dependency-fixture.raw",
            1, 1, 0, 0,
        ),
    )
    conn.execute(
        "INSERT INTO materialization_output VALUES (?,?,?,?,?,?,?)",
        (
            root_attempt, output_id, "fixture_output", 1, None, None,
            "2026-08-13T00:00:01+00:00",
        ),
    )
    conn.execute(
        "INSERT INTO partition_input VALUES (?,?,?,?,?,?)",
        (derived_attempt, output_id, 1, 1, 0, 0),
    )
    conn.execute(
        "INSERT INTO partition_input_binding VALUES (?,?,?,?,?,?,?)",
        (derived_attempt, output_id, output_path, 1, 1, 0, 0),
    )
    conn.execute(
        "INSERT INTO materialization_dependency VALUES (?,?,?,?)",
        (
            derived_attempt, root_attempt, "active-head",
            "2026-08-13T00:00:01+00:00",
        ),
    )
    capability = conn.execute(
        "SELECT revision_id FROM venue_capability_revision "
        "WHERE venue_id='gmo' AND domain='trade' "
        "AND endpoint='trades/archive' AND available=1 "
        "AND implementation_status='implemented' "
        "ORDER BY revision_id DESC LIMIT 1"
    ).fetchone()
    assert capability is not None
    conn.execute(
        "INSERT INTO partition_capability_binding VALUES "
        "(?,?,?,?,?,'recorded',?)",
        (
            root_attempt, "gmo", "trade", "trades/archive",
            int(capability[0]), "2026-08-13T00:00:01+00:00",
        ),
    )
    conn.commit()
    return conn, root_attempt, derived_attempt, output_id


def _add_second_producer(
    conn: sqlite3.Connection,
    market_id: str,
    root_attempt: str,
    output_id: str,
) -> str:
    """登记同一内容制品的第二个直接生产尝试。"""
    second = "fixture-root-second"
    _attempt(conn, second, market_id, "trade")
    raw = conn.execute(
        "SELECT artifact_id FROM partition_input WHERE attempt_id=?",
        (root_attempt,),
    ).fetchone()
    assert raw is not None
    storage = conn.execute(
        "SELECT storage_path FROM partition_input_binding WHERE attempt_id=?",
        (root_attempt,),
    ).fetchone()
    assert storage is not None
    conn.execute(
        "INSERT INTO partition_input "
        "(attempt_id,artifact_id,source_rows,normalized_rows,ignored_rows,"
        "rejected_rows) VALUES (?,?,?,?,?,?)",
        (second, str(raw[0]), 1, 1, 0, 0),
    )
    conn.execute(
        "INSERT INTO partition_input_binding "
        "(attempt_id,artifact_id,storage_path,source_rows,normalized_rows,"
        "ignored_rows,rejected_rows) VALUES (?,?,?,?,?,?,?)",
        (second, str(raw[0]), str(storage[0]), 1, 1, 0, 0),
    )
    conn.execute(
        "INSERT INTO materialization_output "
        "(attempt_id,artifact_id,dataset,row_count,min_event_time,"
        "max_event_time,created_at) VALUES (?,?,?,1,NULL,NULL,?)",
        (
            second, output_id, "fixture_output",
            "2026-08-13T00:00:01+00:00",
        ),
    )
    conn.execute(
        "INSERT INTO partition_capability_binding "
        "(attempt_id,venue_id,domain,endpoint,revision_id,binding_basis,"
        "bound_at) SELECT ?,venue_id,domain,endpoint,revision_id,"
        "binding_basis,bound_at FROM partition_capability_binding "
        "WHERE attempt_id=?",
        (second, root_attempt),
    )
    return second


def test_derived_capability_uses_exact_dependency_closure(
    tmp_path: Path,
) -> None:
    root = tmp_path / "data"
    conn, _root_attempt, derived_attempt, _output_id = _fixture(root)

    report = audit_materializations(root, conn)

    assert not report.errors
    assert conn.execute(
        "SELECT COUNT(*) FROM partition_capability_binding "
        "WHERE attempt_id=?", (derived_attempt,),
    ).fetchone()[0] == 0
    conn.close()


def test_audit_rejects_missing_producer_and_dependency_cycle(
    tmp_path: Path,
) -> None:
    root = tmp_path / "data"
    conn, root_attempt, derived_attempt, output_id = _fixture(root)
    conn.execute(
        "DELETE FROM materialization_dependency WHERE attempt_id=?",
        (derived_attempt,),
    )
    conn.commit()

    missing = audit_materializations(root, conn)

    assert (
        f"完成血缘根缺少能力修订绑定: {derived_attempt}"
        in missing.errors
    )
    assert (
        "物化输入生产依赖数量不符: "
        f"{derived_attempt} {output_id} source_rows=1 producers=0"
        in missing.errors
    )
    conn.execute(
        "INSERT INTO materialization_dependency VALUES (?,?,?,?)",
        (
            derived_attempt, root_attempt, "active-head",
            "2026-08-13T00:00:01+00:00",
        ),
    )
    conn.execute(
        "INSERT INTO materialization_dependency VALUES (?,?,?,?)",
        (
            root_attempt, derived_attempt, "active-head",
            "2026-08-13T00:00:01+00:00",
        ),
    )
    conn.commit()

    cyclic = audit_materializations(root, conn)

    assert f"物化依赖存在循环: {root_attempt}" in cyclic.errors
    assert f"物化依赖存在循环: {derived_attempt}" in cyclic.errors
    conn.close()


def test_audit_requires_unique_producer_for_nonempty_input(
    tmp_path: Path,
) -> None:
    root = tmp_path / "data"
    conn, root_attempt, derived_attempt, output_id = _fixture(root)
    market_id = str(conn.execute(
        "SELECT market_id FROM partition_attempt WHERE attempt_id=?",
        (root_attempt,),
    ).fetchone()[0])
    second = _add_second_producer(
        conn, market_id, root_attempt, output_id,
    )
    conn.execute(
        "INSERT INTO materialization_dependency "
        "(attempt_id,upstream_attempt_id,binding_basis,bound_at) "
        "VALUES (?,?,?,?)",
        (
            derived_attempt, second, "active-head",
            "2026-08-13T00:00:01+00:00",
        ),
    )
    conn.commit()

    report = audit_materializations(root, conn)

    assert (
        "物化输入生产依赖数量不符: "
        f"{derived_attempt} {output_id} source_rows=1 producers=2"
        in report.errors
    )
    conn.close()


def test_audit_allows_multiple_producers_for_empty_input(
    tmp_path: Path,
) -> None:
    root = tmp_path / "data"
    conn, root_attempt, derived_attempt, output_id = _fixture(root)
    market_id = str(conn.execute(
        "SELECT market_id FROM partition_attempt WHERE attempt_id=?",
        (root_attempt,),
    ).fetchone()[0])
    second = _add_second_producer(
        conn, market_id, root_attempt, output_id,
    )
    conn.execute(
        "UPDATE partition_attempt SET source_rows=0,normalized_rows=0 "
        "WHERE attempt_id=?", (derived_attempt,),
    )
    conn.execute(
        "UPDATE partition_input SET source_rows=0,normalized_rows=0 "
        "WHERE attempt_id=?", (derived_attempt,),
    )
    conn.execute(
        "UPDATE partition_input_binding SET source_rows=0,normalized_rows=0 "
        "WHERE attempt_id=?", (derived_attempt,),
    )
    conn.execute(
        "INSERT INTO materialization_dependency "
        "(attempt_id,upstream_attempt_id,binding_basis,bound_at) "
        "VALUES (?,?,?,?)",
        (
            derived_attempt, second, "active-head",
            "2026-08-13T00:00:01+00:00",
        ),
    )
    conn.commit()

    report = audit_materializations(root, conn)

    assert not any(
        error.startswith(
            "物化输入生产依赖数量不符: "
            f"{derived_attempt} {output_id}"
        )
        for error in report.errors
    )
    conn.close()


def test_normalized_count_exemption_is_closed_by_domain(
    tmp_path: Path,
) -> None:
    root = tmp_path / "data"
    conn, _root_attempt, derived_attempt, _output_id = _fixture(root)
    conn.execute(
        "UPDATE partition_attempt SET normalized_rows=2 WHERE attempt_id=?",
        (derived_attempt,),
    )
    conn.commit()

    book_state = audit_materializations(root, conn)

    assert f"输入计数台账不符: {derived_attempt}" not in book_state.errors
    conn.execute(
        "UPDATE partition_attempt SET domain='orderflow_tile' "
        "WHERE attempt_id=?", (derived_attempt,),
    )
    conn.commit()

    orderflow_tile = audit_materializations(root, conn)

    assert (
        f"输入计数台账不符: {derived_attempt}"
        not in orderflow_tile.errors
    )
    conn.execute(
        "UPDATE partition_attempt SET domain='fixture_derived' "
        "WHERE attempt_id=?", (derived_attempt,),
    )
    conn.commit()

    arbitrary_derived = audit_materializations(root, conn)

    assert (
        f"输入计数台账不符: {derived_attempt}"
        in arbitrary_derived.errors
    )
    conn.close()
