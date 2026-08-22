"""冻结预测运行根输入快照的精准测试。"""
from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path

from scripts.refresh_frozen_runtime import refresh_runtime


def _source(root: Path) -> Path:
    data = root / "source"
    fact = data / "materialized" / "trade.parquet"
    fact.parent.mkdir(parents=True)
    fact.write_bytes(b"sealed-trade-fact")
    digest = hashlib.sha256(fact.read_bytes()).hexdigest()
    conn = sqlite3.connect(data / "guvolu.sqlite3")
    try:
        conn.executescript("""
            CREATE TABLE materialization_partition_head(
              market_id TEXT,domain TEXT,attempt_id TEXT
            );
            CREATE TABLE materialization_output(
              attempt_id TEXT,dataset TEXT,artifact_id TEXT
            );
            CREATE TABLE artifact(
              artifact_id TEXT,storage_path TEXT,sha256 TEXT,byte_count INTEGER
            );
        """)
        conn.execute(
            "INSERT INTO materialization_partition_head VALUES(?,?,?)",
            ("market-one", "trade_realtime", "attempt-one"),
        )
        conn.execute(
            "INSERT INTO materialization_output VALUES(?,?,?)",
            ("attempt-one", "trade_observation", f"sha256-{digest}"),
        )
        conn.execute(
            "INSERT INTO artifact VALUES(?,?,?,?)",
            (f"sha256-{digest}", "materialized/trade.parquet", digest,
             fact.stat().st_size),
        )
        conn.commit()
    finally:
        conn.close()
    return data


def test_refresh_runtime_is_verified_and_idempotent(tmp_path: Path) -> None:
    source = _source(tmp_path)
    runtime = tmp_path / "runtime"
    first = refresh_runtime(source, runtime, "market-one")
    second = refresh_runtime(source, runtime, "market-one")
    assert first["quick_check"] == "ok"
    assert first["inputs"] == 1
    assert second["methods"] == {"copied": 0, "hardlinked": 0, "reused": 1}
    assert not (runtime / ".locks").exists()
    assert (
        runtime / "data" / "materialized" / "trade.parquet"
    ).read_bytes() == b"sealed-trade-fact"
