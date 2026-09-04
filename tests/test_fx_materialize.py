"""fx_rate 物化单测：封口段到 Parquet、活动 head 与审计（C-13、C-15）。"""
from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest

from guvolu.data import store
from guvolu.data.fx_capture import fx_channel_id
from guvolu.data.fx_materialize import (
    FX_RATE_NORMALIZATION_VERSION,
    _sealed_inputs,
    audit_fx_rates,
    materialize_all,
)
from guvolu.data.materialize import audit_materializations
from guvolu.data.segmented_raw import SegmentedRawWriter

FIXTURE = Path(__file__).parent / "fixtures" / "gmo_fx_ticker_2026-09-04.json"
SAMPLE = FIXTURE.read_text(encoding="utf-8").strip()
CONTROL = json.dumps({
    "status": 5,
    "messages": [{"message_code": "ERR-5201", "message_string": "maintenance"}],
})
EVENT_TIME = datetime(2026, 9, 4, 2, 19, 26, 944780, tzinfo=UTC)
TIMESTAMP_COLUMNS = ("event_time", "available_time", "ingest_time")


def _write_segment(
    root: Path, payloads: list[str], run_id: str, *, symbol: str = "USD_JPY",
) -> Path:
    writer = SegmentedRawWriter(
        root, "gmo_fx", symbol, domain="fx_rate", run_id=run_id,
        endpoint_id="EP-0076", endpoint_revision=0,
        segment_seconds=3600, segment_max_bytes=1024 * 1024,
    )
    for payload in payloads:
        writer.write_frame(
            payload, "public/v1/ticker", "https",
            connection_id=f"{run_id}-c000001",
            channel_id=fx_channel_id(payload),
        )
    writer.finish()
    return next(writer.directory.glob("segment-*.manifest.json"))


def _without_symbol(symbol: str) -> str:
    body = json.loads(SAMPLE)
    body["data"] = [item for item in body["data"] if item["symbol"] != symbol]
    return json.dumps(body, separators=(",", ":"))


def test_sealed_segment_becomes_active_fx_rate_head(tmp_path: Path) -> None:
    """一段封口 ticker 物化为带 PIT、Decimal 文本与活动 head 的事实。"""
    _write_segment(tmp_path, [SAMPLE], "run-fx-usd-jpy")
    conn = store.connect(tmp_path)
    try:
        results = materialize_all(tmp_path, conn, report_reused=False)
        assert len(results) == 1
        result = results[0]
        assert result.status == "complete"
        assert result.market_id == "mkt__gmo_fx__usd_jpy__r0"
        assert (result.rate_rows, result.ignored_rows, result.rejected_rows) == (
            1, 0, 0,
        )
        head = conn.execute(
            "SELECT domain,normalization_version,attempt_id "
            "FROM materialization_partition_head WHERE market_id=?",
            (result.market_id,),
        ).fetchone()
        assert head == ("fx_rate", FX_RATE_NORMALIZATION_VERSION, result.attempt_id)
        binding = conn.execute(
            "SELECT venue_id,domain,endpoint,binding_basis "
            "FROM partition_capability_binding WHERE attempt_id=?",
            (result.attempt_id,),
        ).fetchone()
        assert binding == ("gmo_fx", "fx_rate", "public/v1/ticker", "recorded")
        assert conn.execute(
            "SELECT COUNT(*) FROM collection_connection "
            "WHERE endpoint_id='EP-0076' AND endpoint_revision=0"
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT channel_id,market_id,capability_domain FROM collection_channel"
        ).fetchall() == [("ticker", result.market_id, "fx_rate")]
        assert conn.execute(
            "SELECT dataset,row_count FROM materialization_output "
            "WHERE attempt_id=?",
            (result.attempt_id,),
        ).fetchone() == ("fx_rate", 1)
        assert audit_fx_rates(tmp_path, conn)["ok"] is True
        audit = audit_materializations(tmp_path, conn)
        assert audit.errors == ()
        assert audit.artifacts_checked >= 3
        reused = materialize_all(tmp_path, conn, report_reused=False)
        assert [item.reused for item in reused] == [True]
        parquet = tmp_path / result.output_path
    finally:
        conn.close()

    db = duckdb.connect(":memory:")
    try:
        db.execute("SET TimeZone='UTC'")
        types = {
            str(row[0]): str(row[1])
            for row in db.execute(
                "DESCRIBE SELECT * FROM read_parquet(?)", [str(parquet)],
            ).fetchall()
        }
        row = db.execute(
            "SELECT symbol,instrument_id,bid,ask,mid,status,event_time,"
            "available_time,ingest_time,endpoint_id,endpoint_revision,"
            "raw_schema_version,normalization_version,schema_version,"
            "source_row_index,observation_id,venue_id,channel_id "
            "FROM read_parquet(?)",
            [str(parquet)],
        ).fetchone()
    finally:
        db.close()
    assert {types[column] for column in TIMESTAMP_COLUMNS} == {
        "TIMESTAMP WITH TIME ZONE",
    }
    assert row is not None
    assert row[:6] == (
        "USD_JPY", "FX:USD/JPY", "155.943", "155.948", "155.9455", "OPEN",
    )
    event_time, available_time, ingest_time = row[6:9]
    for value in (event_time, available_time, ingest_time):
        assert value.tzinfo is not None and value.utcoffset() is not None
    assert event_time == EVENT_TIME
    assert available_time == max(event_time, ingest_time)
    assert row[9:14] == ("EP-0076", 0, 3, FX_RATE_NORMALIZATION_VERSION, 1)
    assert row[14] == 1
    assert str(row[15]).startswith(
        "gmo_fx|USD_JPY|2026-09-04T02:19:26.944780+00:00|sha256-",
    )
    assert row[16:] == ("gmo_fx", "ticker")


def test_control_frames_ignored_and_missing_symbol_rejected(
    tmp_path: Path,
) -> None:
    """控制帧计 ignore，缺目标 symbol 的数据帧整帧 reject。"""
    _write_segment(
        tmp_path, [CONTROL, _without_symbol("USD_JPY"), SAMPLE], "run-fx-mixed",
    )
    conn = store.connect(tmp_path)
    try:
        result = materialize_all(tmp_path, conn, report_reused=False)[0]
        assert result.status == "complete_with_rejections"
        assert (
            result.source_rows, result.data_frames, result.rate_rows,
            result.ignored_rows, result.rejected_rows,
        ) == (3, 1, 1, 1, 1)
        assert conn.execute(
            "SELECT source_row_index,reason FROM materialization_ignore "
            "WHERE attempt_id=?",
            (result.attempt_id,),
        ).fetchall() == [(1, "protocol_control_frame")]
        rejection = conn.execute(
            "SELECT source_row_index,reason FROM materialization_rejection "
            "WHERE attempt_id=?",
            (result.attempt_id,),
        ).fetchone()
        assert rejection is not None
        assert rejection[0] == 2
        assert "USD_JPY" in rejection[1]
        assert audit_fx_rates(tmp_path, conn)["ok"] is True
        assert audit_materializations(tmp_path, conn).errors == ()
    finally:
        conn.close()


def test_unregistered_symbol_fails_closed_without_attempt(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    """未登记市场的 symbol 不物化，也不留下 attempt。"""
    _write_segment(tmp_path, [SAMPLE], "run-fx-eur", symbol="EUR_JPY")
    conn = store.connect(tmp_path)
    try:
        assert materialize_all(tmp_path, conn, report_reused=False) == []
        assert conn.execute(
            "SELECT COUNT(*) FROM partition_attempt"
        ).fetchone()[0] == 0
    finally:
        conn.close()
    assert "FAILED" in capsys.readouterr().out


def test_bad_payload_hash_never_becomes_rate_fact(tmp_path: Path) -> None:
    """即使 manifest 已重算，错误的逐帧 payload hash 仍被拒绝。"""
    manifest_path = _write_segment(tmp_path, [SAMPLE], "run-fx-bad-hash")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    segment = tmp_path / manifest["storage_path"]
    record = json.loads(segment.read_text(encoding="utf-8"))
    record["raw_payload_sha256"] = "0" * 64
    segment.write_text(json.dumps(record) + "\n", encoding="utf-8")
    sha = hashlib.sha256(segment.read_bytes()).hexdigest()
    manifest.update({
        "artifact_id": f"sha256-{sha}",
        "sha256": sha,
        "byte_count": segment.stat().st_size,
    })
    manifest_path.write_text(json.dumps(manifest) + "\n", encoding="utf-8")
    conn = store.connect(tmp_path)
    try:
        result = materialize_all(tmp_path, conn, report_reused=False)[0]
        assert (result.rate_rows, result.rejected_rows) == (0, 1)
        assert conn.execute(
            "SELECT COUNT(*) FROM collection_channel"
        ).fetchone()[0] == 0
        assert audit_fx_rates(tmp_path, conn)["ok"] is True
        assert audit_materializations(tmp_path, conn).errors == ()
    finally:
        conn.close()


def test_manifest_revision_must_be_json_integer(tmp_path: Path) -> None:
    manifest_path = _write_segment(tmp_path, [SAMPLE], "run-fx-string-rev")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["endpoint_revision"] = "0"
    manifest_path.write_text(json.dumps(manifest) + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="endpoint_revision 非整数"):
        _sealed_inputs(tmp_path)
