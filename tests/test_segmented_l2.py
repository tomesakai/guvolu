from __future__ import annotations

import hashlib
import json
import os
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from guvolu.data import l2_capture, trade_capture
from guvolu.data.segmented_raw import (
    SEGMENT_SCHEMA_VERSION,
    SegmentedRawWriter,
    recover_open_segments,
)


def test_segment_writer_seals_content_addressed_parts(tmp_path: Path) -> None:
    writer = SegmentedRawWriter(
        tmp_path, "gmo", "BTC", run_id="run-segment-test",
        endpoint_id="EP-0007", endpoint_revision=0,
        segment_seconds=3600, segment_max_bytes=1,
    )
    writer.write_frame('{"channel":"orderbooks","n":1}', "orderbooks/ws")
    writer.write_frame('{"channel":"orderbooks","n":2}', "orderbooks/ws")
    run_manifest = writer.finish({"data_frames": 2})

    run = json.loads(run_manifest.read_text(encoding="utf-8"))
    assert run["status"] == "complete"
    assert run["record_count"] == 2
    assert run["segment_count"] == 2
    assert not list(writer.directory.glob("*.open"))
    for segment in run["segments"]:
        path = tmp_path / segment["storage_path"]
        assert hashlib.sha256(path.read_bytes()).hexdigest() == segment["sha256"]
        rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
        assert len(rows) == 1
        assert rows[0]["venue_id"] == "gmo"
        assert rows[0]["venue_symbol"] == "BTC"


def test_segment_v3_requires_explicit_endpoint_binding(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="endpoint_id"):
        SegmentedRawWriter(
            tmp_path, "gmo", "BTC", endpoint_revision=0,
        )


def test_segment_v3_persists_receive_and_endpoint_revision(tmp_path: Path) -> None:
    payload = '{"channel":"orderbooks","note":"日本円"}'
    writer = SegmentedRawWriter(
        tmp_path, "gmo", "BTC", run_id="run-v3-contract",
        endpoint_id="EP-0007", endpoint_revision=0, segment_seconds=3600,
        segment_max_bytes=1024 * 1024,
    )
    before_mono = time.monotonic_ns()
    writer.write_frame(
        payload, "orderbooks/ws",
        connection_id="run-v3-contract-c000001",
        channel_id="orderbooks",
    )
    after_mono = time.monotonic_ns()
    checkpoint = json.loads(
        writer.checkpoint({"connection_attempts": 1}).read_text(encoding="utf-8")
    )
    run_manifest_path = writer.finish()

    run = json.loads(run_manifest_path.read_text(encoding="utf-8"))
    segment_path = tmp_path / run["segments"][0]["storage_path"]
    row = json.loads(segment_path.read_text(encoding="utf-8"))
    segment_manifest_path = segment_path.with_name(
        segment_path.name.removesuffix(".jsonl") + ".manifest.json"
    )
    segment_manifest = json.loads(
        segment_manifest_path.read_text(encoding="utf-8")
    )

    assert row["schema_version"] == SEGMENT_SCHEMA_VERSION == 3
    assert row["endpoint_id"] == "EP-0007"
    assert row["endpoint_revision"] == 0
    assert row["connection_id"] == "run-v3-contract-c000001"
    assert row["channel_id"] == "orderbooks"
    assert row["recv_ts_utc"] == row["ingest_time"]
    assert datetime.fromisoformat(row["recv_ts_utc"]).tzinfo is not None
    assert before_mono <= row["recv_ts_mono_ns"] <= after_mono
    assert row["raw_payload_sha256"] == hashlib.sha256(
        payload.encode("utf-8")
    ).hexdigest()
    assert checkpoint["endpoint_id"] == "EP-0007"
    assert checkpoint["endpoint_revision"] == 0
    assert checkpoint["connection_attempts"] == 1
    assert segment_manifest["endpoint_id"] == "EP-0007"
    assert segment_manifest["endpoint_revision"] == 0
    assert run["endpoint_id"] == "EP-0007"
    assert run["endpoint_revision"] == 0


def test_capture_contract_uses_native_channels_and_stable_sessions(
    tmp_path: Path,
) -> None:
    assert l2_capture.ENDPOINT_BINDINGS == {
        "gmo": ("EP-0007", 0),
        "bitbank": ("EP-0005", 1),
        "bitflyer": ("EP-0002", 0),
    }
    assert trade_capture.ENDPOINT_BINDINGS == {
        "gmo": ("EP-0007", 1),
        "bitbank": ("EP-0075", 0),
        "bitflyer": ("EP-0002", 0),
    }
    bitbank = "42" + json.dumps([
        "message", {"room_name": "depth_diff_btc_jpy"},
    ])
    bitflyer = {
        "method": "channelMessage",
        "params": {"channel": "lightning_board_BTC_JPY"},
    }
    assert l2_capture._bitbank_channel_id(bitbank) == "depth_diff_btc_jpy"
    assert trade_capture._bitbank_channel_id("2") == "protocol_control"
    assert (
        l2_capture._bitflyer_channel_id(bitflyer)
        == "lightning_board_BTC_JPY"
    )

    writer = SegmentedRawWriter(
        tmp_path, "bitbank", "btc_jpy", run_id="run-session-contract",
        endpoint_id="EP-0005", endpoint_revision=0,
    )
    stats = l2_capture.CaptureStats("bitbank", "btc_jpy")
    stats.consecutive_failures = 3
    first = l2_capture._opened_connection(writer, stats)
    second = l2_capture._opened_connection(writer, stats)
    l2_capture._observed_data(stats)
    writer.finish()

    assert first == "run-session-contract-c000001"
    assert second == "run-session-contract-c000002"
    assert stats.sessions == stats.successful_sessions == 2
    assert stats.data_frames == 1
    assert stats.consecutive_failures == 0


def test_recovery_only_seals_complete_silent_json_lines(tmp_path: Path) -> None:
    directory = (
        tmp_path / "raw/realtime/book_l2/venue_id=gmo/"
        "venue_symbol=BTC/run_id=run-recovery-test"
    )
    directory.mkdir(parents=True)
    record = {
        "run_id": "run-recovery-test",
        "segment_sequence": 1,
        "venue_id": "gmo",
        "venue_symbol": "BTC",
        "domain": "book_l2",
        "ingest_time": "2026-08-11T00:00:00+00:00",
        "payload_raw": "{}",
    }
    open_path = directory / "segment-000001.jsonl.open"
    open_path.write_text(json.dumps(record) + "\n", encoding="utf-8")
    old = (datetime.now(UTC) - timedelta(hours=2)).timestamp()
    os.utime(open_path, (old, old))

    manifests = recover_open_segments(tmp_path, older_minutes=60)

    assert len(manifests) == 1
    body = json.loads(manifests[0].read_text(encoding="utf-8"))
    assert body["schema_version"] == 1
    assert body["status"] == "recovered_incomplete"
    assert body["completion_claim"] is False
    assert body["endpoint_id"] is None
    assert body["endpoint_revision"] is None
    assert not open_path.exists()
    assert open_path.with_suffix("").exists()


def test_recovery_leaves_partial_line_open(tmp_path: Path) -> None:
    directory = (
        tmp_path / "raw/realtime/book_l2/venue_id=gmo/"
        "venue_symbol=BTC/run_id=run-partial-test"
    )
    directory.mkdir(parents=True)
    open_path = directory / "segment-000001.jsonl.open"
    open_path.write_bytes(b'{"run_id":"run-partial-test"}')
    old = (datetime.now(UTC) - timedelta(hours=2)).timestamp()
    os.utime(open_path, (old, old))

    assert recover_open_segments(tmp_path, older_minutes=60) == ()
    assert open_path.exists()


def test_recovery_skips_old_segment_with_fresh_checkpoint(
    tmp_path: Path,
) -> None:
    directory = (
        tmp_path / "raw/realtime/trade_realtime/venue_id=bitflyer/"
        "venue_symbol=BTC_JPY/run_id=run-live-sparse"
    )
    directory.mkdir(parents=True)
    open_path = directory / "segment-000001.jsonl.open"
    open_path.write_text(
        json.dumps({
            "run_id": "run-live-sparse", "segment_sequence": 1,
            "venue_id": "bitflyer", "venue_symbol": "BTC_JPY",
            "domain": "trade_realtime",
            "ingest_time": "2026-08-11T00:00:00+00:00",
        }) + "\n",
        encoding="utf-8",
    )
    old = (datetime.now(UTC) - timedelta(hours=2)).timestamp()
    os.utime(open_path, (old, old))
    (directory / "checkpoint.json").write_text(
        '{"status":"open"}\n', encoding="utf-8"
    )

    assert recover_open_segments(
        tmp_path, older_minutes=60, domain="trade_realtime"
    ) == ()
    assert open_path.exists()
