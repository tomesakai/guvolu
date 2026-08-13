from __future__ import annotations

import hashlib
import io
import json
import tarfile
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from pathlib import Path

import duckdb

from guvolu.data import store
from guvolu.data.l2_materialize import audit_l2 as audit_realtime_l2
from guvolu.data.okx_l2_materialize import (
    audit_okx_l2,
    materialize_archive,
    sealed_inputs,
    source_descriptor,
)


def _write_archive(root: Path, day: str = "2026-08-07") -> None:
    day_value = datetime.fromisoformat(day).replace(tzinfo=UTC)
    start_millis = int(day_value.timestamp() * 1000)
    directory = (
        root / "raw/archive/okx/book_l2/"
        f"venue_symbol=BTC-USDT/day={day}"
    )
    directory.mkdir(parents=True)
    filename = f"BTC-USDT-L2orderbook-400lv-{day}.tar.gz"
    archive = directory / filename
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
    with tarfile.open(archive, "w:gz") as tf:
        info = tarfile.TarInfo(
            f"BTC-USDT-L2orderbook-400lv-{day}.data"
        )
        info.size = len(body)
        tf.addfile(info, io.BytesIO(body))
    sha = hashlib.sha256(archive.read_bytes()).hexdigest()
    manifest = directory / (filename + ".manifest.json")
    manifest.write_text(json.dumps({
        "status": "sealed",
        "completion_claim": True,
        "sha256": sha,
        "byte_count": archive.stat().st_size,
        "venue_symbol": "BTC-USDT",
        "endpoint": "historical-data/order-book",
        "storage_path": archive.relative_to(root).as_posix(),
        "day": day,
        "depth_levels": 2,
        "source_last_modified": format_datetime(
            day_value + timedelta(days=1, seconds=47), usegmt=True
        ),
        "sealed_at": "2026-08-11T10:00:00+00:00",
    }), encoding="utf-8")


def test_okx_l2_archive_materializes_replayable_v2_facts(
    tmp_path: Path,
) -> None:
    _write_archive(tmp_path)
    conn = store.connect(tmp_path)
    try:
        item = sealed_inputs(tmp_path)[0]
        result = materialize_archive(
            tmp_path, conn, item, require_full_day=False
        )

        assert result.status == "complete"
        assert result.frame_rows == 3
        assert result.level_rows == 11
        db = duckdb.connect(":memory:")
        try:
            frame = db.execute(
                "SELECT COUNT(*),MIN(schema_version),"
                "COUNT(DISTINCT source_artifact_id),"
                "SUM(available_time<event_time),"
                "LIST(message_kind ORDER BY event_time) "
                "FROM read_parquet(?)",
                [str(tmp_path / result.frame_path)],
            ).fetchone()
            level = db.execute(
                "SELECT COUNT(*),SUM(action='delete'),SUM(order_count) "
                "FROM read_parquet(?)",
                [str(tmp_path / result.level_path)],
            ).fetchone()
        finally:
            db.close()
        assert frame == (3, 2, 1, 0, ["snapshot", "delta", "snapshot"])
        assert level == (11, 1, 15)
        audit = audit_okx_l2(tmp_path, conn)
        assert audit["ok"] is True
        assert audit["frames"] == 3
        assert audit["levels"] == 11
        realtime_audit = audit_realtime_l2(tmp_path, conn)
        assert realtime_audit["ok"] is True
        assert realtime_audit["attempts"] == 0
    finally:
        conn.close()


def test_okx_history_descriptor_does_not_claim_sequence_integrity() -> None:
    descriptor = source_descriptor(400)

    assert descriptor.endpoint == "historical-data/order-book"
    assert descriptor.sequence_policy == "none"
    assert descriptor.checksum_policy == "archive_sha256"
    assert descriptor.availability_policy == "source_last_modified"


def test_okx_l2_audit_covers_every_active_day(tmp_path: Path) -> None:
    _write_archive(tmp_path, "2026-08-07")
    _write_archive(tmp_path, "2026-08-08")
    conn = store.connect(tmp_path)
    try:
        for item in sealed_inputs(tmp_path):
            materialize_archive(
                tmp_path, conn, item, require_full_day=False
            )

        audit = audit_okx_l2(tmp_path, conn)

        assert audit["ok"] is True
        assert audit["partition_count"] == 2
        assert audit["frames"] == 6
        assert audit["levels"] == 22
    finally:
        conn.close()
