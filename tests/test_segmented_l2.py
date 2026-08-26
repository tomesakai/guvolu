from __future__ import annotations

import asyncio
import hashlib
import json
import os
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import BinaryIO, cast

import pytest

from guvolu.data import durable_io, l2_capture, segmented_raw, trade_capture
from guvolu.data.segmented_raw import (
    SEGMENT_SCHEMA_VERSION,
    SegmentedRawWriter,
    recover_open_segments,
    supervise_capture_tasks,
)


class _FaultingBinaryHandle:
    """保留真实文件证据，同时注入 write/truncate 故障。"""

    def __init__(
        self, handle: BinaryIO, *, short: bool = False,
        raise_after_partial: bool = False, fail_truncate: bool = False,
    ) -> None:
        self._handle = handle
        self._short = short
        self._raise_after_partial = raise_after_partial
        self._fail_truncate = fail_truncate

    def tell(self) -> int:
        return self._handle.tell()

    def write(self, body: bytes) -> int:
        partial = max(1, len(body) // 2)
        written = self._handle.write(body[:partial])
        if self._raise_after_partial:
            raise OSError("injected partial write")
        if self._short:
            return written
        return written

    def truncate(self, size: int | None = None) -> int:
        if self._fail_truncate:
            raise OSError("injected rollback failure")
        return self._handle.truncate(size)

    def seek(self, offset: int, whence: int = 0) -> int:
        return self._handle.seek(offset, whence)

    def flush(self) -> None:
        self._handle.flush()

    def fileno(self) -> int:
        return self._handle.fileno()

    def close(self) -> None:
        self._handle.close()


def _install_faulting_handle(
    writer: SegmentedRawWriter,
    *,
    short: bool = False,
    raise_after_partial: bool = False,
    fail_truncate: bool = False,
) -> None:
    if writer._segment_handle is None:
        writer._open_segment(datetime.now(UTC))
    handle = writer._segment_handle
    assert handle is not None
    writer._segment_handle = cast(BinaryIO, _FaultingBinaryHandle(
        handle,
        short=short,
        raise_after_partial=raise_after_partial,
        fail_truncate=fail_truncate,
    ))


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


def test_short_write_rolls_back_without_advancing_and_downgrades_run(
    tmp_path: Path,
) -> None:
    writer = SegmentedRawWriter(
        tmp_path, "gmo", "BTC", run_id="run-short-write",
        endpoint_id="EP-0007", endpoint_revision=0,
    )
    writer.write_frame('{"n":1}', "orderbooks/ws")
    _install_faulting_handle(writer, short=True)

    with pytest.raises(OSError, match="short write"):
        writer.write_frame('{"n":2}', "orderbooks/ws")

    assert writer.record_count == 1
    manifest_path = writer.finish()
    assert writer.finish() == manifest_path
    run = json.loads(manifest_path.read_text(encoding="utf-8"))
    segment_manifest = json.loads(next(
        writer.directory.glob("segment-*.manifest.json")
    ).read_text(encoding="utf-8"))
    assert run["status"] == "failed"
    assert run["completion_claim"] is False
    assert (run["record_count"], run["segment_count"]) == (1, 1)
    assert segment_manifest["status"] == "recovered_incomplete"
    assert segment_manifest["completion_claim"] is False
    assert segment_manifest["record_count"] == 1


def test_partial_first_write_and_failed_rollback_never_seal_completion(
    tmp_path: Path,
) -> None:
    writer = SegmentedRawWriter(
        tmp_path, "gmo", "BTC", run_id="run-partial-write",
        endpoint_id="EP-0007", endpoint_revision=0,
    )
    _install_faulting_handle(
        writer, raise_after_partial=True, fail_truncate=True,
    )

    with pytest.raises(OSError, match="partial write"):
        writer.write_frame('{"n":1}', "orderbooks/ws")

    manifest_path = writer.finish()
    run = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert writer.record_count == 0
    assert run["status"] == "failed"
    assert run["completion_claim"] is False
    assert (run["record_count"], run["segment_count"]) == (0, 0)
    assert not list(writer.directory.glob("segment-*.manifest.json"))
    assert next(writer.directory.glob("segment-*.jsonl.open")).stat().st_size > 0


def test_hash_failure_after_replace_rolls_final_back_to_open(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    writer = SegmentedRawWriter(
        tmp_path, "gmo", "BTC", run_id="run-hash-failure",
        endpoint_id="EP-0007", endpoint_revision=0,
    )
    writer.write_frame('{"n":1}', "orderbooks/ws")

    def fail_hash(_: Path) -> str:
        raise OSError("injected hash failure")

    original_hash = segmented_raw._sha256_file
    monkeypatch.setattr(segmented_raw, "_sha256_file", fail_hash)
    with pytest.raises(OSError, match="hash failure"):
        writer.finish()

    assert list(writer.directory.glob("segment-*.jsonl.open"))
    assert not list(writer.directory.glob("segment-*.jsonl"))
    run = json.loads(
        (writer.directory / "run.manifest.json").read_text(encoding="utf-8")
    )
    assert run["status"] == "failed"
    assert run["completion_claim"] is False
    monkeypatch.setattr(segmented_raw, "_sha256_file", original_hash)


def test_manifest_and_rollback_failure_final_orphan_is_recoverable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    writer = SegmentedRawWriter(
        tmp_path, "gmo", "BTC", run_id="run-final-orphan",
        endpoint_id="EP-0007", endpoint_revision=0,
    )
    writer.write_frame('{"n":1}', "orderbooks/ws")
    original_atomic = durable_io.atomic_write_text
    original_replace = os.replace

    def fail_segment_manifest(path: Path, body: str) -> None:
        if path.name.startswith("segment-") and path.name.endswith(
            ".manifest.json"
        ):
            raise OSError("injected manifest failure")
        original_atomic(path, body)

    def fail_final_rollback(source: Path | str, target: Path | str) -> None:
        source_path = Path(source)
        target_path = Path(target)
        if source_path.suffix == ".jsonl" and target_path.suffix == ".open":
            raise OSError("injected final rollback failure")
        original_replace(source, target)

    monkeypatch.setattr(
        "guvolu.data.segmented_raw.atomic_write_text", fail_segment_manifest,
    )
    monkeypatch.setattr(os, "replace", fail_final_rollback)
    with pytest.raises(OSError, match="manifest failure"):
        writer.finish()

    final_path = next(writer.directory.glob("segment-*.jsonl"))
    assert not list(writer.directory.glob("segment-*.manifest.json"))
    old = (datetime.now(UTC) - timedelta(hours=2)).timestamp()
    os.utime(final_path, (old, old))
    monkeypatch.setattr(
        "guvolu.data.segmented_raw.atomic_write_text", original_atomic,
    )
    monkeypatch.setattr(os, "replace", original_replace)

    recovered = recover_open_segments(tmp_path, older_minutes=60)
    assert len(recovered) == 1
    body = json.loads(recovered[0].read_text(encoding="utf-8"))
    assert body["status"] == "recovered_incomplete"
    assert body["completion_claim"] is False
    assert body["recovery_basis"] == (
        "orphan-final-without-manifest-after-silence-v1"
    )


def test_repeated_finish_is_idempotent_but_conflicting_terminal_fails(
    tmp_path: Path,
) -> None:
    writer = SegmentedRawWriter(
        tmp_path, "gmo", "BTC", run_id="run-idempotent-finish",
        endpoint_id="EP-0007", endpoint_revision=0,
    )
    writer.write_frame('{"n":1}', "orderbooks/ws")
    first = writer.finish({"data_frames": 1})

    assert writer.finish({"data_frames": 1}) == first
    with pytest.raises(RuntimeError, match="不同终态"):
        writer.finish({"data_frames": 2})


def test_complete_run_rejects_records_without_segment_receipts(
    tmp_path: Path,
) -> None:
    writer = SegmentedRawWriter(
        tmp_path, "gmo", "BTC", run_id="run-missing-receipt",
        endpoint_id="EP-0007", endpoint_revision=0,
    )
    writer.write_frame('{"n":1}', "orderbooks/ws")
    writer.seal_segment()
    writer._segments.clear()

    with pytest.raises(RuntimeError, match="计数不一致"):
        writer.finish()


def test_record_l2_checkpoint_failure_cancels_recorder_and_closes_anchor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorder_cancelled = False
    anchor_closed = False

    class FakeAnchorWorker:
        def __init__(self, *args: object, **kwargs: object) -> None:
            del args, kwargs
            self.completed = 0
            self.failed = 0

        def start(self) -> None:
            return None

        def submit(self, connection_id: str | None, reason: str) -> bool:
            del connection_id, reason
            return True

        async def close(self) -> None:
            nonlocal anchor_closed
            anchor_closed = True

    async def recorder(
        writer: SegmentedRawWriter,
        stats: l2_capture.CaptureStats,
        deadline: float | None,
        anchor_submit: l2_capture.AnchorSubmit | None = None,
    ) -> None:
        nonlocal recorder_cancelled
        del stats, deadline, anchor_submit
        writer.write_frame('{"channel":"orderbooks"}', "orderbooks/ws")
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            recorder_cancelled = True
            raise

    def fail_checkpoint(*args: object, **kwargs: object) -> Path:
        del args, kwargs
        raise OSError("l2 checkpoint disk failure")

    monkeypatch.setattr(l2_capture, "RestAnchorWorker", FakeAnchorWorker)
    monkeypatch.setitem(l2_capture._RECORDERS, "gmo", recorder)
    monkeypatch.setattr(l2_capture, "CHECKPOINT_SECONDS", 0.001)
    monkeypatch.setattr(
        SegmentedRawWriter, "checkpoint", fail_checkpoint,
    )

    with pytest.raises(OSError, match="l2 checkpoint disk failure"):
        asyncio.run(l2_capture.record_l2(
            tmp_path, "gmo", "BTC", 0.0, 3600, 1024 * 1024,
        ))

    run = json.loads(next(
        tmp_path.rglob("run.manifest.json")
    ).read_text(encoding="utf-8"))
    assert recorder_cancelled
    assert anchor_closed
    assert run["status"] == "failed"
    assert run["completion_claim"] is False
    assert run["failure_detail"] == "OSError: l2 checkpoint disk failure"


def test_checkpoint_failure_remains_primary_over_recorder_cancel_cleanup() -> None:
    async def recorder() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            raise RuntimeError("recorder cancellation cleanup failed") from None

    async def checkpoint() -> None:
        raise OSError("checkpoint primary")

    with pytest.raises(OSError, match="checkpoint primary") as caught:
        asyncio.run(supervise_capture_tasks(recorder(), checkpoint()))

    assert any(
        "recorder cancellation cleanup failed" in note
        for note in getattr(caught.value, "__notes__", [])
    )
