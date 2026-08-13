"""Materialize sealed OKX public ``books`` raw segments to L2 facts."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sqlite3
import time
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import duckdb

from guvolu.data import store
from guvolu.data.book_l2_contract import (
    BOOK_L2_FRAME_DATASET,
    BOOK_L2_LEVEL_DATASET,
    BOOK_L2_V5_NORMALIZATION_VERSION,
    BOOK_L2_V5_SCHEMA_VERSION,
    create_book_l2_v5_tables,
)
from guvolu.data.durable_io import atomic_write_text
from guvolu.data.materialize import (
    SourceArtifact,
    _input_set_hash,
    _market_row,
    _register_content_artifact,
    _relative_storage_path,
    artifact_id,
    ensure_markets,
    sha256_file,
    utc_now,
)
from guvolu.data.okx_l2_live_capture import (
    OKX_DEFAULT_SYMBOL,
    OKX_ENDPOINT,
    OKX_ENDPOINT_ID,
    OKX_ENDPOINT_REVISION,
    OKX_RAW_DOMAIN,
)
from guvolu.data.okx_l2_terminal_checkpoint import (
    TERMINAL_CHECKPOINT_DATASET,
    TERMINAL_CHECKPOINT_SCHEMA_VERSION,
    OkxTerminalCheckpoint,
    TerminalCheckpointError,
    TerminalLevel,
    checkpoint_body,
    load_terminal_checkpoint_for_attempt,
)
from guvolu.data.paths import data_root as configured_data_root
from guvolu.data.realtime_control import (
    RealtimeChannelObservation,
    register_materialized_raw_v3_observations,
)
from guvolu.data.sqlite_writer_lock import sqlite_writer_lock
from guvolu.venues import registry

L2_SCHEMA_VERSION = BOOK_L2_V5_SCHEMA_VERSION
L2_NORMALIZATION_VERSION = BOOK_L2_V5_NORMALIZATION_VERSION
OKX_PAYLOAD_SCHEMA_VERSION = "okx-ws-v5-books@2026-08-13"
OKX_BOOK_MODE = "absolute_level_update"
OKX_REPLAY_FIDELITY = "snapshot_native_prev_seq_delta"
OKX_INTEGRITY_MODE = "native_prev_seq+checksum_unsupported_after_2026-06-23"
OKX_DEPTH_LIMIT = 400
# 兼容内部旧名。
REPLAY_STATE_DATASET = TERMINAL_CHECKPOINT_DATASET
WATCH_POLL_SECONDS = 5.0


@dataclass(frozen=True, slots=True)
class LiveSegmentInput:
    """One verified sealed OKX live segment."""

    manifest_path: Path
    run_id: str
    segment_sequence: int
    venue_symbol: str
    artifact: SourceArtifact

    @property
    def partition_key(self) -> str:
        return f"live/{self.run_id}/segment-{self.segment_sequence:06d}"


@dataclass(frozen=True, slots=True)
class LiveL2Result:
    """One immutable OKX live L2 materialization result."""

    attempt_id: str
    market_id: str
    partition_key: str
    status: str
    source_rows: int
    frame_rows: int
    level_rows: int
    ignored_rows: int
    rejected_rows: int
    frame_path: str
    level_path: str
    reused: bool


@dataclass(frozen=True, slots=True)
class _RawMetadata:
    ingest_time: str
    connection_id: str
    channel_id: str
    recv_ts_mono_ns: int
    raw_payload_sha256: str
    record_sequence: int


@dataclass(frozen=True, slots=True)
class _Level:
    index: int
    side: str
    price: str
    size: str
    order_count: int
    action: str


@dataclass(frozen=True, slots=True)
class _ParsedFrame:
    kind: str
    event_time: str
    sequence_id: int
    prev_sequence_id: int
    asks: tuple[_Level, ...]
    bids: tuple[_Level, ...]
    heartbeat: bool


@dataclass(slots=True)
class _SequenceState:
    last_sequence_id: int | None = None
    snapshot_seen: bool = False
    last_recv_mono_ns: int | None = None
    checkpoint_available_time: str | None = None
    channel_id: str | None = None
    as_of_frame_id: str | None = None
    as_of_event_time: str | None = None
    as_of_available_time: str | None = None
    as_of_ingest_time: str | None = None
    snapshot_frame_id: str | None = None
    snapshot_event_time: str | None = None
    snapshot_available_time: str | None = None
    asks: dict[str, TerminalLevel] = field(default_factory=dict)
    bids: dict[str, TerminalLevel] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class _TerminalBookState:
    connection_id: str
    last_sequence_id: int | None
    snapshot_seen: bool
    last_recv_mono_ns: int | None
    checkpoint_available_time: str
    channel_id: str
    as_of_frame_id: str | None
    as_of_event_time: str | None
    as_of_available_time: str | None
    as_of_ingest_time: str | None
    snapshot_frame_id: str | None
    snapshot_event_time: str | None
    snapshot_available_time: str | None
    asks: tuple[TerminalLevel, ...]
    bids: tuple[TerminalLevel, ...]
    trusted: bool
    trust_reason: str


@dataclass(frozen=True, slots=True)
class _ReplayContext:
    upstream_attempt_id: str
    artifact: SourceArtifact
    checkpoint: OkxTerminalCheckpoint


@dataclass(frozen=True, slots=True)
class _StageProfile:
    source_rows: int
    frames: int
    levels: int
    snapshots: int
    updates: int
    heartbeats: int
    ignored: tuple[tuple[int, str], ...]
    rejected: tuple[tuple[int, str], ...]
    min_event_time: str
    max_event_time: str
    observations: tuple[RealtimeChannelObservation, ...]
    terminal_record_sequence: int
    terminal_state: _TerminalBookState


@dataclass(frozen=True, slots=True)
class _PreparedAttempt:
    attempt_id: str
    market_id: str
    instrument_id: str
    mapping_revision: int
    capability_revision: int
    context: _ReplayContext | None
    contract_artifact: SourceArtifact
    reused: LiveL2Result | None


def _time(value: object, label: str) -> datetime:
    text = str(value)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{label} is not ISO-8601: {text!r}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{label} is missing a UTC offset")
    return parsed.astimezone(UTC)


def _iso_millis(value: object) -> str:
    if isinstance(value, (bool, float)):
        raise ValueError("OKX ts must be an integer millisecond value")
    millis = int(str(value))
    if millis <= 0:
        raise ValueError("OKX ts must be positive")
    return datetime.fromtimestamp(millis / 1000, UTC).isoformat()


def _integer(value: object, field: str, *, allow_negative: bool) -> int:
    if isinstance(value, (bool, float)):
        raise ValueError(f"{field} must be an integer")
    number = int(str(value))
    if not allow_negative and number < 0:
        raise ValueError(f"{field} must be non-negative")
    return number


def _decimal_text(
    value: object,
    field: str,
    *,
    allow_zero: bool,
) -> str:
    if isinstance(value, (bool, float)):
        raise ValueError(f"{field} must not be bool or float")
    text = str(value)
    number = Decimal(text)
    if not number.is_finite() or number < 0 or (not allow_zero and number == 0):
        raise ValueError(f"{field} has an invalid value: {text!r}")
    return text


def _field(value: object) -> object:
    return "\\N" if value is None else value


def _max_time_text(*values: str | None) -> str:
    present = [value for value in values if value is not None]
    if not present:
        raise ValueError("time maximum has no values")
    return max(present, key=lambda value: _time(value, "time maximum"))


def _available_time(event_time: str, ingest_time: str) -> tuple[str, bool]:
    event = _time(event_time, "event_time")
    ingest = _time(ingest_time, "ingest_time")
    return max(event, ingest).isoformat(), event > ingest


def _frame_id(market_id: str, artifact_identity: str, source_row: int) -> str:
    body = f"okx-live|{market_id}|{artifact_identity}|{source_row}"
    return "sha256-" + hashlib.sha256(body.encode("ascii")).hexdigest()


def sealed_inputs(root: Path) -> list[LiveSegmentInput]:
    """Discover and verify all complete OKX live segment manifests."""
    base = root / "raw" / "realtime" / OKX_RAW_DOMAIN
    if not base.is_dir():
        return []
    inputs: list[LiveSegmentInput] = []
    root_resolved = root.resolve()
    for manifest_path in sorted(base.rglob("segment-*.manifest.json")):
        body = json.loads(manifest_path.read_text(encoding="utf-8"))
        if not isinstance(body, Mapping):
            raise ValueError(f"segment manifest is not an object: {manifest_path}")
        if body.get("status") != "sealed" or body.get("completion_claim") is not True:
            continue
        if body.get("schema_version") != 3:
            raise ValueError(f"OKX live raw schema must be v3: {manifest_path}")
        if body.get("venue_id") != "okx" or body.get("domain") != OKX_RAW_DOMAIN:
            raise ValueError(f"OKX live manifest identity mismatch: {manifest_path}")
        if (
            body.get("endpoint_id") != OKX_ENDPOINT_ID
            or body.get("endpoint_revision") != OKX_ENDPOINT_REVISION
        ):
            raise ValueError(f"OKX endpoint binding mismatch: {manifest_path}")
        venue_symbol = str(body.get("venue_symbol", ""))
        if venue_symbol != OKX_DEFAULT_SYMBOL:
            raise ValueError(f"unverified OKX live symbol: {venue_symbol!r}")
        recorded = str(body["storage_path"])
        path = (root / recorded).resolve()
        try:
            path.relative_to(root_resolved)
        except ValueError as exc:
            raise ValueError(f"segment path escapes data root: {recorded}") from exc
        expected = manifest_path.with_name(
            manifest_path.name.removesuffix(".manifest.json") + ".jsonl"
        ).resolve()
        if path != expected:
            raise ValueError(f"manifest and segment paths disagree: {recorded}")
        sha = sha256_file(path)
        if sha != str(body.get("sha256")):
            raise ValueError(f"segment SHA-256 mismatch: {recorded}")
        if path.stat().st_size != int(str(body.get("byte_count"))):
            raise ValueError(f"segment byte count mismatch: {recorded}")
        if body.get("artifact_id") != artifact_id(sha):
            raise ValueError(f"segment artifact identity mismatch: {recorded}")
        source_rows = int(str(body.get("record_count")))
        inputs.append(LiveSegmentInput(
            manifest_path=manifest_path,
            run_id=str(body["run_id"]),
            segment_sequence=int(str(body["segment_sequence"])),
            venue_symbol=venue_symbol,
            artifact=SourceArtifact(
                artifact_id=artifact_id(sha),
                storage_path=recorded,
                absolute_path=path,
                source_rows=source_rows,
                normalized_rows=0,
                rejected_rows=0,
            ),
        ))
    inputs.sort(key=lambda item: (item.run_id, item.segment_sequence))
    seen: set[tuple[str, int]] = set()
    for item in inputs:
        identity = (item.run_id, item.segment_sequence)
        if identity in seen:
            raise ValueError(f"duplicate OKX live segment: {identity}")
        seen.add(identity)
    return inputs


def _raw_metadata(
    envelope: Mapping[str, object], item: LiveSegmentInput,
) -> tuple[str, _RawMetadata]:
    if envelope.get("schema_version") != 3:
        raise ValueError("raw row schema_version is not 3")
    expected: dict[str, object] = {
        "run_id": item.run_id,
        "segment_sequence": item.segment_sequence,
        "venue_id": "okx",
        "venue_symbol": item.venue_symbol,
        "domain": OKX_RAW_DOMAIN,
        "endpoint_id": OKX_ENDPOINT_ID,
        "endpoint_revision": OKX_ENDPOINT_REVISION,
        "source_endpoint": OKX_ENDPOINT,
    }
    for field_name, value in expected.items():
        if envelope.get(field_name) != value:
            raise ValueError(
                f"raw row {field_name} does not match the manifest"
            )
    if envelope.get("source") != "websocket":
        raise ValueError("raw row transport is not websocket")
    payload = envelope.get("payload_raw")
    if not isinstance(payload, str):
        raise ValueError("raw payload is not text")
    connection_id = envelope.get("connection_id")
    if (
        not isinstance(connection_id, str)
        or not connection_id.startswith(f"{item.run_id}-c")
    ):
        raise ValueError("raw connection_id is outside the current run")
    channel_id = envelope.get("channel_id")
    if not isinstance(channel_id, str) or not channel_id:
        raise ValueError("raw channel_id is missing")
    ingest_time = str(envelope.get("ingest_time", ""))
    recv_time = str(envelope.get("recv_ts_utc", ""))
    if _time(ingest_time, "ingest_time") != _time(recv_time, "recv_ts_utc"):
        raise ValueError("raw receive and ingest clocks differ")
    recv_mono = envelope.get("recv_ts_mono_ns")
    if (
        isinstance(recv_mono, bool)
        or not isinstance(recv_mono, int)
        or recv_mono < 0
        or recv_mono > 2**64 - 1
    ):
        raise ValueError("raw monotonic receive clock is invalid")
    recorded_hash = envelope.get("raw_payload_sha256")
    computed_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    if recorded_hash != computed_hash:
        raise ValueError("raw payload SHA-256 mismatch")
    record_sequence = envelope.get("record_sequence")
    if (
        isinstance(record_sequence, bool)
        or not isinstance(record_sequence, int)
        or record_sequence <= 0
    ):
        raise ValueError("raw record_sequence is invalid")
    return payload, _RawMetadata(
        ingest_time=recv_time,
        connection_id=connection_id,
        channel_id=channel_id,
        recv_ts_mono_ns=recv_mono,
        raw_payload_sha256=computed_hash,
        record_sequence=record_sequence,
    )


def _parse_levels(rows: object, side: str, kind: str) -> tuple[_Level, ...]:
    if not isinstance(rows, list):
        raise ValueError(f"OKX {side} levels are not an array")
    levels: list[_Level] = []
    prices: set[Decimal] = set()
    for index, row in enumerate(rows):
        if not isinstance(row, list) or len(row) != 4:
            raise ValueError(f"OKX {side} level must have four fields")
        price = _decimal_text(row[0], "price", allow_zero=False)
        size = _decimal_text(row[1], "size", allow_zero=True)
        deprecated = _decimal_text(row[2], "deprecated", allow_zero=True)
        if Decimal(deprecated) != 0:
            raise ValueError("OKX deprecated level field is not zero")
        order_count = _integer(row[3], "orderCount", allow_negative=False)
        size_is_zero = Decimal(size) == 0
        if kind == "snapshot" and size_is_zero:
            raise ValueError("OKX snapshot contains a zero-size level")
        if size_is_zero != (order_count == 0):
            raise ValueError("OKX size and orderCount zero semantics disagree")
        price_number = Decimal(price)
        if price_number in prices:
            raise ValueError(f"OKX {side} contains a duplicate price")
        prices.add(price_number)
        levels.append(_Level(
            index=index,
            side=side,
            price=price,
            size=size,
            order_count=order_count,
            action="delete" if kind == "delta" and size_is_zero else "set",
        ))
    if len(levels) > OKX_DEPTH_LIMIT:
        raise ValueError(f"OKX {side} depth exceeds {OKX_DEPTH_LIMIT}")
    return tuple(levels)


def _parse_payload(
    payload_raw: str, venue_symbol: str,
) -> tuple[_ParsedFrame | None, str | None]:
    if payload_raw == "pong":
        return None, "protocol_pong"
    loaded = json.loads(payload_raw)
    if not isinstance(loaded, Mapping):
        raise ValueError("OKX websocket payload is not an object")
    event = loaded.get("event")
    if isinstance(event, str):
        return None, f"protocol_{event}_frame"
    action = loaded.get("action")
    if action not in {"snapshot", "update"}:
        return None, "protocol_control_frame"
    arg = loaded.get("arg")
    if not isinstance(arg, Mapping):
        raise ValueError("OKX books arg is missing")
    if arg.get("channel") != OKX_ENDPOINT or arg.get("instId") != venue_symbol:
        raise ValueError("OKX books channel identity mismatch")
    data = loaded.get("data")
    if not isinstance(data, list) or len(data) != 1 or not isinstance(data[0], Mapping):
        raise ValueError("OKX books data must contain exactly one object")
    body = data[0]
    kind = "snapshot" if action == "snapshot" else "delta"
    asks = _parse_levels(body.get("asks"), "ask", kind)
    bids = _parse_levels(body.get("bids"), "bid", kind)
    checksum = _integer(body.get("checksum"), "checksum", allow_negative=True)
    if checksum != 0:
        raise ValueError("OKX books checksum must be fixed at zero")
    sequence = _integer(body.get("seqId"), "seqId", allow_negative=False)
    previous = _integer(body.get("prevSeqId"), "prevSeqId", allow_negative=True)
    heartbeat = kind == "delta" and not asks and not bids
    if kind == "snapshot" and not asks and not bids:
        raise ValueError("OKX books snapshot is empty")
    return _ParsedFrame(
        kind=kind,
        event_time=_iso_millis(body.get("ts")),
        sequence_id=sequence,
        prev_sequence_id=previous,
        asks=asks,
        bids=bids,
        heartbeat=heartbeat,
    ), None


def _validate_sequence(
    parsed: _ParsedFrame,
    state: _SequenceState,
    quality: list[str],
) -> None:
    if parsed.kind == "snapshot":
        state.last_sequence_id = parsed.sequence_id
        state.snapshot_seen = True
        return
    if state.last_sequence_id is None:
        quality.append("connection_sequence_anchor_missing")
    elif parsed.prev_sequence_id != state.last_sequence_id:
        raise ValueError(
            "OKX prevSeqId does not match the prior seqId in this connection"
        )
    if parsed.heartbeat:
        if parsed.sequence_id != parsed.prev_sequence_id:
            raise ValueError("OKX empty update must keep seqId equal to prevSeqId")
        quality.append("empty_update_heartbeat")
    elif parsed.sequence_id <= parsed.prev_sequence_id:
        raise ValueError("OKX non-empty update must advance seqId")
    if not state.snapshot_seen:
        quality.append("delta_before_connection_snapshot")
    state.last_sequence_id = parsed.sequence_id


def _snapshot_mid(parsed: _ParsedFrame) -> str | None:
    if parsed.kind != "snapshot" or not parsed.asks or not parsed.bids:
        return None
    best_ask = min(Decimal(level.price) for level in parsed.asks)
    best_bid = max(Decimal(level.price) for level in parsed.bids)
    if best_bid >= best_ask:
        raise ValueError("OKX snapshot is crossed")
    return str((best_ask + best_bid) / 2)


def _state_from_checkpoint(checkpoint: OkxTerminalCheckpoint) -> _SequenceState:
    """Restore only a same-connection trusted book base."""

    trusted = checkpoint.trusted
    return _SequenceState(
        last_sequence_id=checkpoint.sequence_id,
        snapshot_seen=trusted,
        last_recv_mono_ns=checkpoint.last_recv_mono_ns,
        checkpoint_available_time=checkpoint.checkpoint_available_time.isoformat(),
        channel_id=checkpoint.channel_id,
        as_of_frame_id=checkpoint.as_of_frame_id,
        as_of_event_time=(
            None
            if checkpoint.as_of_event_time is None
            else checkpoint.as_of_event_time.isoformat()
        ),
        as_of_available_time=(
            None
            if checkpoint.as_of_available_time is None
            else checkpoint.as_of_available_time.isoformat()
        ),
        as_of_ingest_time=(
            None
            if checkpoint.as_of_ingest_time is None
            else checkpoint.as_of_ingest_time.isoformat()
        ),
        snapshot_frame_id=(checkpoint.snapshot_frame_id if trusted else None),
        snapshot_event_time=(
            checkpoint.snapshot_event_time.isoformat()
            if trusted and checkpoint.snapshot_event_time is not None else None
        ),
        snapshot_available_time=(
            checkpoint.snapshot_available_time.isoformat()
            if trusted and checkpoint.snapshot_available_time is not None else None
        ),
        asks={row.price: row for row in checkpoint.asks} if trusted else {},
        bids={row.price: row for row in checkpoint.bids} if trusted else {},
    )


def _trim_book(state: _SequenceState) -> None:
    """Keep the advertised native 400 levels per side."""

    if len(state.asks) > OKX_DEPTH_LIMIT:
        keep = sorted(state.asks, key=Decimal)[:OKX_DEPTH_LIMIT]
        state.asks = {price: state.asks[price] for price in keep}
    if len(state.bids) > OKX_DEPTH_LIMIT:
        keep = sorted(state.bids, key=Decimal, reverse=True)[:OKX_DEPTH_LIMIT]
        state.bids = {price: state.bids[price] for price in keep}


def _apply_book_frame(
    state: _SequenceState,
    parsed: _ParsedFrame,
    *,
    frame_id: str,
    available_time: str,
    ingest_time: str,
) -> None:
    """Apply absolute updates only after a same-connection snapshot."""

    if parsed.kind == "snapshot":
        state.asks.clear()
        state.bids.clear()
        state.snapshot_frame_id = frame_id
        state.snapshot_event_time = parsed.event_time
        state.snapshot_available_time = available_time
    if state.snapshot_seen:
        for level in (*parsed.asks, *parsed.bids):
            book = state.asks if level.side == "ask" else state.bids
            if level.action == "delete":
                book.pop(level.price, None)
            else:
                book[level.price] = TerminalLevel(
                    level.price, level.size, level.order_count
                )
        _trim_book(state)
    state.as_of_frame_id = frame_id
    state.as_of_event_time = parsed.event_time
    state.as_of_available_time = available_time
    state.as_of_ingest_time = ingest_time
    state.checkpoint_available_time = _max_time_text(
        state.checkpoint_available_time, available_time, ingest_time,
    )


def _terminal_book_state(
    states: Mapping[str, _SequenceState],
    connection_id: str | None,
    *,
    rejected: bool,
) -> _TerminalBookState:
    if connection_id is None or connection_id not in states:
        raise ValueError("OKX segment has no terminal connection identity")
    state = states[connection_id]
    if state.checkpoint_available_time is None or state.channel_id is None:
        raise ValueError("OKX terminal connection lacks raw availability identity")
    complete = bool(state.snapshot_seen and state.asks and state.bids)
    if rejected:
        trusted = False
        reason = "segment_rejection"
    elif complete:
        trusted = True
        reason = "snapshot_anchored_same_connection"
    else:
        trusted = False
        reason = "connection_without_snapshot"
    asks = (
        tuple(sorted(state.asks.values(), key=lambda row: Decimal(row.price)))
        if trusted else ()
    )
    bids = (
        tuple(sorted(
            state.bids.values(), key=lambda row: Decimal(row.price), reverse=True,
        )) if trusted else ()
    )
    return _TerminalBookState(
        connection_id=connection_id,
        last_sequence_id=state.last_sequence_id,
        snapshot_seen=state.snapshot_seen,
        last_recv_mono_ns=state.last_recv_mono_ns,
        checkpoint_available_time=state.checkpoint_available_time,
        channel_id=state.channel_id,
        as_of_frame_id=state.as_of_frame_id,
        as_of_event_time=state.as_of_event_time,
        as_of_available_time=state.as_of_available_time,
        as_of_ingest_time=state.as_of_ingest_time,
        snapshot_frame_id=state.snapshot_frame_id if trusted else None,
        snapshot_event_time=state.snapshot_event_time if trusted else None,
        snapshot_available_time=state.snapshot_available_time if trusted else None,
        asks=asks,
        bids=bids,
        trusted=trusted,
        trust_reason=reason,
    )


def _stage(
    item: LiveSegmentInput,
    context: _ReplayContext | None,
    *,
    market_id: str,
    mapping_revision: int,
    capability_revision: int,
    instrument_id: str,
    frame_csv: Path,
    level_csv: Path,
) -> _StageProfile:
    states: dict[str, _SequenceState] = {}
    if context is not None:
        prior = context.checkpoint
        states[prior.connection_id] = _state_from_checkpoint(prior)
    previous_record = (
        None if context is None else context.checkpoint.terminal_record_sequence
    )
    terminal_connection_id: str | None = None
    frames = levels = snapshots = updates = heartbeats = 0
    ignored: list[tuple[int, str]] = []
    rejected: list[tuple[int, str]] = []
    observations: list[RealtimeChannelObservation] = []
    min_event = max_event = ""
    source_rows = 0
    with (
        item.artifact.absolute_path.open(encoding="utf-8") as source,
        frame_csv.open("w", encoding="utf-8", newline="") as frame_handle,
        level_csv.open("w", encoding="utf-8", newline="") as level_handle,
    ):
        frame_writer = csv.writer(frame_handle, lineterminator="\n")
        level_writer = csv.writer(level_handle, lineterminator="\n")
        for source_row, line in enumerate(source, start=1):
            source_rows = source_row
            try:
                envelope = json.loads(line)
                if not isinstance(envelope, Mapping):
                    raise ValueError("raw row is not an object")
                payload, raw = _raw_metadata(envelope, item)
                expected_record = 1 if previous_record is None else previous_record + 1
                if raw.record_sequence != expected_record:
                    raise ValueError("OKX raw record_sequence is not contiguous")
                previous_record = raw.record_sequence
                state = states.setdefault(raw.connection_id, _SequenceState())
                terminal_connection_id = raw.connection_id
                state.channel_id = f"{OKX_ENDPOINT}:{item.venue_symbol}"
                state.checkpoint_available_time = _max_time_text(
                    state.checkpoint_available_time, raw.ingest_time,
                )
                if (
                    state.last_recv_mono_ns is not None
                    and raw.recv_ts_mono_ns < state.last_recv_mono_ns
                ):
                    raise ValueError("OKX monotonic receive clock regressed")
                state.last_recv_mono_ns = raw.recv_ts_mono_ns
                parsed, ignore_reason = _parse_payload(payload, item.venue_symbol)
                if parsed is None:
                    ignored.append((source_row, ignore_reason or "protocol_control_frame"))
                    continue
                expected_channel = f"{OKX_ENDPOINT}:{item.venue_symbol}"
                if raw.channel_id != expected_channel:
                    raise ValueError("data frame raw channel identity mismatch")
                quality = [
                    "checksum_unsupported_fixed_zero",
                    "raw_payload_hash_verified",
                ]
                _validate_sequence(parsed, state, quality)
                available, source_ahead = _available_time(
                    parsed.event_time, raw.ingest_time
                )
                if source_ahead:
                    quality.append("source_clock_ahead_of_receive_clock")
                identity = _frame_id(
                    market_id, item.artifact.artifact_id, source_row
                )
                _apply_book_frame(
                    state,
                    parsed,
                    frame_id=identity,
                    available_time=available,
                    ingest_time=raw.ingest_time,
                )
                asks = parsed.asks
                bids = parsed.bids
                if parsed.kind == "snapshot":
                    snapshots += 1
                    book_bid_levels: int | None = len(bids)
                    book_ask_levels: int | None = len(asks)
                else:
                    updates += 1
                    book_bid_levels = None
                    book_ask_levels = None
                if parsed.heartbeat:
                    heartbeats += 1
                frame_writer.writerow([_field(value) for value in (
                    identity,
                    "okx",
                    item.venue_symbol,
                    market_id,
                    mapping_revision,
                    capability_revision,
                    instrument_id,
                    OKX_ENDPOINT,
                    OKX_ENDPOINT_ID,
                    OKX_ENDPOINT_REVISION,
                    OKX_PAYLOAD_SCHEMA_VERSION,
                    parsed.kind,
                    OKX_BOOK_MODE,
                    OKX_REPLAY_FIDELITY,
                    parsed.event_time,
                    parsed.event_time,
                    available,
                    raw.ingest_time,
                    raw.recv_ts_mono_ns,
                    "venue",
                    str(parsed.sequence_id),
                    str(parsed.prev_sequence_id),
                    None,
                    OKX_INTEGRITY_MODE,
                    len(bids),
                    len(asks),
                    book_bid_levels,
                    book_ask_levels,
                    OKX_DEPTH_LIMIT,
                    len(bids) + len(asks),
                    _snapshot_mid(parsed),
                    item.run_id,
                    raw.connection_id,
                    raw.channel_id,
                    item.artifact.absolute_path.name,
                    raw.raw_payload_sha256,
                    json.dumps(sorted(set(quality)), separators=(",", ":")),
                    "L2",
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    item.run_id,
                    item.segment_sequence,
                    item.artifact.artifact_id,
                    source_row,
                    L2_NORMALIZATION_VERSION,
                    L2_SCHEMA_VERSION,
                )])
                for level in (*asks, *bids):
                    level_writer.writerow((
                        identity,
                        market_id,
                        level.side,
                        level.index,
                        level.price,
                        level.size,
                        level.order_count,
                        level.action,
                        "limit",
                        item.artifact.artifact_id,
                        source_row,
                        L2_NORMALIZATION_VERSION,
                        L2_SCHEMA_VERSION,
                    ))
                    levels += 1
                frames += 1
                min_event = (
                    parsed.event_time if not min_event
                    else min(min_event, parsed.event_time)
                )
                max_event = max(max_event, parsed.event_time)
                observations.append(RealtimeChannelObservation(
                    raw.connection_id, raw.channel_id, raw.ingest_time
                ))
            except (
                InvalidOperation,
                json.JSONDecodeError,
                KeyError,
                TypeError,
                UnicodeError,
                ValueError,
            ) as exc:
                rejected.append((source_row, f"{type(exc).__name__}: {exc}"))
        for handle in (frame_handle, level_handle):
            handle.flush()
            os.fsync(handle.fileno())
    if source_rows != item.artifact.source_rows:
        raise ValueError("target segment row count mismatch")
    if source_rows != frames + len(ignored) + len(rejected):
        raise ValueError("OKX live source classification is not conserved")
    return _StageProfile(
        source_rows=source_rows,
        frames=frames,
        levels=levels,
        snapshots=snapshots,
        updates=updates,
        heartbeats=heartbeats,
        ignored=tuple(ignored),
        rejected=tuple(rejected),
        min_event_time=min_event,
        max_event_time=max_event,
        observations=tuple(observations),
        terminal_record_sequence=previous_record or 0,
        terminal_state=_terminal_book_state(
            states,
            terminal_connection_id,
            rejected=bool(rejected),
        ),
    )


def _register_source(conn: sqlite3.Connection, item: LiveSegmentInput) -> None:
    created = datetime.fromtimestamp(
        item.artifact.absolute_path.stat().st_mtime, UTC
    ).isoformat()
    _register_content_artifact(
        conn,
        item.artifact.artifact_id,
        "raw_realtime_segment",
        item.artifact.storage_path,
        item.artifact.artifact_id.removeprefix("sha256-"),
        item.artifact.absolute_path.stat().st_size,
        created,
        3,
    )


def _checkpoint_contract_artifact(
    root: Path, conn: sqlite3.Connection, *, mapping_revision: int,
    capability_revision: int,
) -> SourceArtifact:
    """Persist the immutable terminal-checkpoint normalization contract."""

    body = json.dumps({
        "contract": "okx-live-l2-terminal-checkpoint",
        "checkpoint_schema_version": TERMINAL_CHECKPOINT_SCHEMA_VERSION,
        "checkpoint_dataset": TERMINAL_CHECKPOINT_DATASET,
        "source_normalization_version": L2_NORMALIZATION_VERSION,
        "mapping_revision": mapping_revision,
        "capability_revision": capability_revision,
        "endpoint_id": OKX_ENDPOINT_ID,
        "endpoint_revision": OKX_ENDPOINT_REVISION,
        "book_mode": OKX_BOOK_MODE,
        "depth_per_side": OKX_DEPTH_LIMIT,
        "connection_rule": "same_connection_snapshot_required",
        "pit_rule": "checkpoint_available_gte_as_of_available",
    }, sort_keys=True, separators=(",", ":")) + "\n"
    body_sha = hashlib.sha256(body.encode("utf-8")).hexdigest()
    path = (
        root / "control" / "contracts"
        / f"okx-live-terminal-{body_sha[:12]}.json"
    )
    if path.exists():
        if path.read_text(encoding="utf-8") != body:
            raise ValueError("OKX terminal checkpoint contract bytes changed")
    else:
        atomic_write_text(path, body)
    sha = sha256_file(path)
    identity = artifact_id(sha)
    storage_path = _relative_storage_path(root, path)
    _register_content_artifact(
        conn, identity, "normalization_contract", storage_path, sha,
        path.stat().st_size, datetime.fromtimestamp(
            path.stat().st_mtime, UTC
        ).isoformat(), TERMINAL_CHECKPOINT_SCHEMA_VERSION,
    )
    return SourceArtifact(
        artifact_id=identity, storage_path=storage_path, absolute_path=path,
        source_rows=0, normalized_rows=0, rejected_rows=0,
    )


def _capability_revision(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT revision_id FROM venue_capability_revision "
        "WHERE venue_id='okx' AND domain='book_realtime' AND endpoint=? "
        "AND available=1 AND evidence_level='measured' "
        "AND implementation_status='implemented' "
        "ORDER BY revision_id DESC LIMIT 1",
        (OKX_ENDPOINT,),
    ).fetchone()
    if row is None:
        raise ValueError("OKX live books capability is not measured and implemented")
    return int(row[0])


def _load_replay_context(
    root: Path,
    conn: sqlite3.Connection,
    market_id: str,
    item: LiveSegmentInput,
) -> _ReplayContext | None:
    if item.segment_sequence == 1:
        return None
    previous_key = (
        f"live/{item.run_id}/segment-{item.segment_sequence - 1:06d}"
    )
    rows = conn.execute(
        "SELECT h.attempt_id FROM materialization_partition_head h "
        "WHERE h.market_id=? AND h.domain='book_l2' AND h.partition_key=? "
        "AND h.normalization_version=?",
        (
            market_id,
            previous_key,
            L2_NORMALIZATION_VERSION,
        ),
    ).fetchall()
    if len(rows) != 1:
        raise ValueError(
            f"previous OKX live terminal checkpoint is unavailable: {previous_key}"
        )
    try:
        loaded = load_terminal_checkpoint_for_attempt(
            root, conn, str(rows[0][0])
        )
    except TerminalCheckpointError as exc:
        raise ValueError(f"previous OKX terminal checkpoint failed: {exc}") from exc
    checkpoint = loaded.checkpoint
    if (
        checkpoint.market_id != market_id
        or checkpoint.run_id != item.run_id
        or checkpoint.segment_sequence != item.segment_sequence - 1
        or checkpoint.source_attempt_id != str(rows[0][0])
    ):
        raise ValueError("OKX terminal checkpoint identity mismatch")
    return _ReplayContext(
        upstream_attempt_id=str(rows[0][0]),
        artifact=SourceArtifact(
            artifact_id=loaded.artifact_id,
            storage_path=loaded.storage_path,
            absolute_path=root / loaded.storage_path,
            source_rows=0,
            normalized_rows=0,
            rejected_rows=0,
        ),
        checkpoint=checkpoint,
    )


def _existing_result(
    root: Path,
    conn: sqlite3.Connection,
    market_id: str,
    partition_key: str,
    input_hash: str,
) -> LiveL2Result | None:
    row = conn.execute(
        "SELECT a.attempt_id,a.status,a.source_rows,a.normalized_rows,"
        "a.ignored_rows,a.rejected_rows,"
        "MAX(CASE WHEN o.dataset=? THEN r.storage_path END),"
        "MAX(CASE WHEN o.dataset=? THEN r.storage_path END),"
        "MAX(CASE WHEN o.dataset=? THEN r.storage_path END),"
        "MAX(CASE WHEN o.dataset=? THEN o.row_count END) "
        "FROM partition_attempt a JOIN materialization_output o "
        "ON o.attempt_id=a.attempt_id JOIN artifact r "
        "ON r.artifact_id=o.artifact_id "
        "WHERE a.market_id=? AND a.domain='book_l2' "
        "AND a.partition_key=? AND a.normalization_version=? "
        "AND a.input_set_hash=? "
        "AND a.status IN ('complete','complete_with_rejections') "
        "GROUP BY a.attempt_id,a.status,a.source_rows,a.normalized_rows,"
        "a.ignored_rows,a.rejected_rows LIMIT 1",
        (
            BOOK_L2_FRAME_DATASET,
            BOOK_L2_LEVEL_DATASET,
            REPLAY_STATE_DATASET,
            BOOK_L2_LEVEL_DATASET,
            market_id,
            partition_key,
            L2_NORMALIZATION_VERSION,
            input_hash,
        ),
    ).fetchone()
    if row is None or not row[6] or not row[7] or not row[8]:
        return None
    try:
        load_terminal_checkpoint_for_attempt(root, conn, str(row[0]))
    except TerminalCheckpointError as exc:
        raise ValueError(f"existing OKX terminal checkpoint failed: {exc}") from exc
    return LiveL2Result(
        attempt_id=str(row[0]),
        market_id=market_id,
        partition_key=partition_key,
        status=str(row[1]),
        source_rows=int(row[2]),
        frame_rows=int(row[3]),
        level_rows=int(row[9]),
        ignored_rows=int(row[4]),
        rejected_rows=int(row[5]),
        frame_path=str(row[6]),
        level_path=str(row[7]),
        reused=True,
    )


def _prepare_attempt(
    root: Path,
    conn: sqlite3.Connection,
    item: LiveSegmentInput,
) -> _PreparedAttempt:
    with sqlite_writer_lock(root):
        registry.register_all(conn)
        ensure_markets(conn)
        market_id, instrument_id, mapping_revision = _market_row(
            conn, "okx", item.venue_symbol, None
        )
        capability_revision = _capability_revision(conn)
        context = _load_replay_context(root, conn, market_id, item)
        contract_artifact = _checkpoint_contract_artifact(
            root, conn, mapping_revision=mapping_revision,
            capability_revision=capability_revision,
        )
        artifacts = [item.artifact, contract_artifact]
        if context is not None:
            artifacts.append(context.artifact)
        input_hash = _input_set_hash(artifacts)
        _register_source(conn, item)
        conn.commit()
        reused = _existing_result(
            root, conn, market_id, item.partition_key, input_hash
        )
        if reused is not None:
            return _PreparedAttempt(
                reused.attempt_id,
                market_id,
                instrument_id,
                mapping_revision,
                capability_revision,
                context,
                contract_artifact,
                reused,
            )
        attempt_id = f"okx-live-l2-{uuid.uuid4().hex}"
        config_hash = hashlib.sha256(json.dumps({
            "dataset": [
                BOOK_L2_FRAME_DATASET,
                BOOK_L2_LEVEL_DATASET,
                TERMINAL_CHECKPOINT_DATASET,
            ],
            "normalization_version": L2_NORMALIZATION_VERSION,
            "schema_version": L2_SCHEMA_VERSION,
            "endpoint_id": OKX_ENDPOINT_ID,
            "endpoint_revision": OKX_ENDPOINT_REVISION,
            "payload_schema_version": OKX_PAYLOAD_SCHEMA_VERSION,
            "depth_limit": OKX_DEPTH_LIMIT,
            "mapping_revision": mapping_revision,
            "capability_revision": capability_revision,
            "context_artifact_id": (
                None if context is None else context.artifact.artifact_id
            ),
        }, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        conn.execute(
            "INSERT INTO partition_attempt "
            "(attempt_id,market_id,domain,partition_key,normalization_version,"
            "input_set_hash,status,source_rows,normalized_rows,ignored_rows,"
            "rejected_rows,started_at,code_version,config_hash) "
            "VALUES (?,?,?,?,?,?,'running',?,0,0,0,?,'working-tree',?)",
            (
                attempt_id,
                market_id,
                "book_l2",
                item.partition_key,
                L2_NORMALIZATION_VERSION,
                input_hash,
                item.artifact.source_rows,
                utc_now(),
                config_hash,
            ),
        )
        conn.execute(
            "INSERT INTO partition_capability_binding "
            "(attempt_id,venue_id,domain,endpoint,revision_id,binding_basis,bound_at) "
            "VALUES (?,'okx','book_realtime',?,?,'recorded',?)",
            (attempt_id, OKX_ENDPOINT, capability_revision, utc_now()),
        )
        if context is not None:
            conn.execute(
                "INSERT INTO materialization_dependency VALUES "
                "(?,?,'explicit-replay',?)",
                (attempt_id, context.upstream_attempt_id, utc_now()),
            )
        conn.commit()
        return _PreparedAttempt(
            attempt_id,
            market_id,
            instrument_id,
            mapping_revision,
            capability_revision,
            context,
            contract_artifact,
            None,
        )


def _copy_csv(db: Any, table: str, path: Path) -> None:
    escaped = path.as_posix().replace("'", "''")
    db.execute(
        f"COPY {table} FROM '{escaped}' "
        "(FORMAT CSV, HEADER false, NULL '\\N')"
    )


def _write_parquet(db: Any, query: str, path: Path) -> None:
    escaped = path.as_posix().replace("'", "''")
    db.execute(
        f"COPY ({query}) TO '{escaped}' "
        "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880)"
    )
    with path.open("rb+") as handle:
        handle.flush()
        os.fsync(handle.fileno())


def _finalize(temp: Path) -> tuple[Path, str]:
    sha = sha256_file(temp)
    final = temp.with_name(f"part-{sha[:12]}.parquet")
    if final.exists():
        if sha256_file(final) != sha:
            raise ValueError(f"output hash name collision: {final}")
        temp.unlink()
    else:
        os.replace(temp, final)
    return final, sha


def _write_terminal_checkpoint(
    output_dir: Path,
    attempt_id: str,
    market_id: str,
    item: LiveSegmentInput,
    context: _ReplayContext | None,
    profile: _StageProfile,
) -> tuple[Path, str]:
    temp = output_dir / f".{attempt_id}.terminal.json"
    terminal = profile.terminal_state
    body = checkpoint_body(
        market_id=market_id,
        venue_symbol=item.venue_symbol,
        run_id=item.run_id,
        segment_sequence=item.segment_sequence,
        connection_id=terminal.connection_id,
        channel_id=terminal.channel_id,
        trusted=terminal.trusted,
        trust_reason=terminal.trust_reason,
        sequence_id=terminal.last_sequence_id,
        terminal_record_sequence=profile.terminal_record_sequence,
        last_recv_mono_ns=terminal.last_recv_mono_ns,
        checkpoint_available_time=terminal.checkpoint_available_time,
        as_of_frame_id=terminal.as_of_frame_id,
        as_of_event_time=terminal.as_of_event_time,
        as_of_available_time=terminal.as_of_available_time,
        as_of_ingest_time=terminal.as_of_ingest_time,
        snapshot_frame_id=terminal.snapshot_frame_id,
        snapshot_event_time=terminal.snapshot_event_time,
        snapshot_available_time=terminal.snapshot_available_time,
        asks=terminal.asks,
        bids=terminal.bids,
        source_attempt_id=attempt_id,
        source_artifact_id=item.artifact.artifact_id,
        upstream_attempt_id=(
            None if context is None else context.upstream_attempt_id
        ),
        upstream_checkpoint_artifact_id=(
            None if context is None else context.artifact.artifact_id
        ),
    )
    atomic_write_text(
        temp,
        json.dumps(
            body, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ) + "\n",
    )
    sha = sha256_file(temp)
    final = output_dir / f"terminal-{sha[:12]}.json"
    if final.exists():
        if sha256_file(final) != sha:
            raise ValueError(f"terminal checkpoint hash name collision: {final}")
        temp.unlink()
    else:
        os.replace(temp, final)
    return final, sha


def _validate_tables(
    db: Any,
    profile: _StageProfile,
    *,
    market_id: str,
    artifact_identity: str,
    capability_revision: int,
) -> None:
    frame = db.execute(
        "SELECT COUNT(*),COUNT(*)-COUNT(DISTINCT frame_id),"
        "SUM(available_time<event_time),SUM(source_depth_levels),"
        "COUNT(DISTINCT market_id),MIN(market_id),"
        "COUNT(DISTINCT source_artifact_id),MIN(source_artifact_id),"
        "SUM(CASE WHEN endpoint<>? OR endpoint_id<>? OR endpoint_revision<>? "
        "OR normalization_version<>? OR schema_version<>? "
        "OR checksum IS NOT NULL OR source_level<>'L2' "
        "OR capability_revision<>? THEN 1 ELSE 0 END) "
        f"FROM {BOOK_L2_FRAME_DATASET}",
        [
            OKX_ENDPOINT,
            OKX_ENDPOINT_ID,
            OKX_ENDPOINT_REVISION,
            L2_NORMALIZATION_VERSION,
            L2_SCHEMA_VERSION,
            capability_revision,
        ],
    ).fetchone()
    level = db.execute(
        "SELECT COUNT(*),COUNT(*)-COUNT(DISTINCT "
        "(frame_id,side,source_level_index)),"
        "SUM(CASE WHEN side NOT IN ('bid','ask') "
        "OR action NOT IN ('set','delete') OR order_count<0 "
        "OR normalization_version<>? OR schema_version<>? "
        "OR ((size='0')<>(order_count=0)) THEN 1 ELSE 0 END) "
        f"FROM {BOOK_L2_LEVEL_DATASET}",
        [L2_NORMALIZATION_VERSION, L2_SCHEMA_VERSION],
    ).fetchone()
    if frame is None or level is None:
        raise ValueError("OKX live staging tables are unreadable")
    if (
        int(frame[0]) != profile.frames
        or int(frame[1] or 0)
        or int(frame[2] or 0)
        or int(frame[3] or 0) != profile.levels
        or (profile.frames and (
            int(frame[4]) != 1
            or str(frame[5]) != market_id
            or int(frame[6]) != 1
            or str(frame[7]) != artifact_identity
            or int(frame[8] or 0)
        ))
    ):
        raise ValueError("OKX live frame identity, PIT, or counts failed")
    if (
        int(level[0]) != profile.levels
        or int(level[1] or 0)
        or int(level[2] or 0)
    ):
        raise ValueError("OKX live level key or semantics failed")
    orphan = db.execute(
        f"SELECT COUNT(*) FROM {BOOK_L2_LEVEL_DATASET} l LEFT JOIN "
        f"{BOOK_L2_FRAME_DATASET} f ON f.frame_id=l.frame_id "
        "WHERE f.frame_id IS NULL"
    ).fetchone()
    if orphan is None or int(orphan[0]):
        raise ValueError("OKX live levels contain orphan rows")


def _mark_failed(
    root: Path,
    conn: sqlite3.Connection,
    attempt_id: str,
    exc: Exception,
) -> None:
    conn.rollback()
    with sqlite_writer_lock(root):
        conn.execute(
            "UPDATE partition_attempt SET status='failed',finished_at=?,"
            "failure_detail=? WHERE attempt_id=? AND status='running'",
            (utc_now(), str(exc)[:2000], attempt_id),
        )
        conn.commit()


def _commit(
    root: Path,
    conn: sqlite3.Connection,
    item: LiveSegmentInput,
    context: _ReplayContext | None,
    prepared: _PreparedAttempt,
    profile: _StageProfile,
    frame_path: Path,
    frame_sha: str,
    level_path: Path,
    level_sha: str,
    checkpoint_path: Path,
    checkpoint_sha: str,
    manifest_path: Path,
) -> None:
    finished = utc_now()
    status = "complete_with_rejections" if profile.rejected else "complete"
    with sqlite_writer_lock(root):
        conn.execute("BEGIN IMMEDIATE")
        for identity, dataset, path, sha, rows in (
            (
                artifact_id(frame_sha), BOOK_L2_FRAME_DATASET,
                frame_path, frame_sha, profile.frames,
            ),
            (
                artifact_id(level_sha), BOOK_L2_LEVEL_DATASET,
                level_path, level_sha, profile.levels,
            ),
            (
                artifact_id(checkpoint_sha), REPLAY_STATE_DATASET,
                checkpoint_path, checkpoint_sha,
                len(profile.terminal_state.asks) + len(profile.terminal_state.bids),
            ),
        ):
            _register_content_artifact(
                conn,
                identity,
                (
                    "materialized_terminal_book_checkpoint"
                    if dataset == REPLAY_STATE_DATASET
                    else "materialized_parquet"
                ),
                _relative_storage_path(root, path),
                sha,
                path.stat().st_size,
                finished,
                (
                    TERMINAL_CHECKPOINT_SCHEMA_VERSION
                    if dataset == REPLAY_STATE_DATASET else L2_SCHEMA_VERSION
                ),
            )
            conn.execute(
                "INSERT INTO materialization_output VALUES (?,?,?,?,?,?,?)",
                (
                    prepared.attempt_id,
                    identity,
                    dataset,
                    rows,
                    (
                        profile.terminal_state.as_of_event_time
                        if dataset == REPLAY_STATE_DATASET
                        else profile.min_event_time or None
                    ),
                    (
                        profile.terminal_state.as_of_event_time
                        if dataset == REPLAY_STATE_DATASET
                        else profile.max_event_time or None
                    ),
                    finished,
                ),
            )
        manifest_sha = sha256_file(manifest_path)
        _register_content_artifact(
            conn,
            artifact_id(manifest_sha),
            "materialization_manifest",
            _relative_storage_path(root, manifest_path),
            manifest_sha,
            manifest_path.stat().st_size,
            finished,
            1,
        )
        if context is not None:
            conn.execute(
                "INSERT INTO partition_input "
                "(attempt_id,artifact_id,source_rows,normalized_rows,"
                "ignored_rows,rejected_rows) VALUES (?,?,0,0,0,0)",
                (prepared.attempt_id, context.artifact.artifact_id),
            )
            conn.execute(
                "INSERT INTO partition_input_binding "
                "(attempt_id,artifact_id,storage_path,source_rows,"
                "normalized_rows,ignored_rows,rejected_rows) "
                "VALUES (?,?,?,0,0,0,0)",
                (
                    prepared.attempt_id,
                    context.artifact.artifact_id,
                    context.artifact.storage_path,
                ),
            )
        conn.execute(
            "INSERT INTO partition_input "
            "(attempt_id,artifact_id,source_rows,normalized_rows,"
            "ignored_rows,rejected_rows) VALUES (?,?,0,0,0,0)",
            (prepared.attempt_id, prepared.contract_artifact.artifact_id),
        )
        conn.execute(
            "INSERT INTO partition_input_binding "
            "(attempt_id,artifact_id,storage_path,source_rows,"
            "normalized_rows,ignored_rows,rejected_rows) "
            "VALUES (?,?,?,0,0,0,0)",
            (
                prepared.attempt_id,
                prepared.contract_artifact.artifact_id,
                prepared.contract_artifact.storage_path,
            ),
        )
        conn.execute(
            "INSERT INTO partition_input "
            "(attempt_id,artifact_id,source_rows,normalized_rows,"
            "ignored_rows,rejected_rows) VALUES (?,?,?,?,?,?)",
            (
                prepared.attempt_id,
                item.artifact.artifact_id,
                profile.source_rows,
                profile.frames,
                len(profile.ignored),
                len(profile.rejected),
            ),
        )
        conn.execute(
            "INSERT INTO partition_input_binding "
            "(attempt_id,artifact_id,storage_path,source_rows,"
            "normalized_rows,ignored_rows,rejected_rows) "
            "VALUES (?,?,?,?,?,?,?)",
            (
                prepared.attempt_id,
                item.artifact.artifact_id,
                item.artifact.storage_path,
                profile.source_rows,
                profile.frames,
                len(profile.ignored),
                len(profile.rejected),
            ),
        )
        conn.executemany(
            "INSERT INTO materialization_ignore VALUES (?,?,?,?,?,?,?)",
            [
                (
                    prepared.attempt_id,
                    item.artifact.artifact_id,
                    row,
                    -1,
                    f"{item.artifact.storage_path}:{row}",
                    reason,
                    finished,
                )
                for row, reason in profile.ignored
            ],
        )
        conn.executemany(
            "INSERT INTO materialization_rejection VALUES (?,?,?,?,?,?)",
            [
                (
                    prepared.attempt_id,
                    item.artifact.artifact_id,
                    row,
                    f"{item.artifact.storage_path}:{row}",
                    reason,
                    finished,
                )
                for row, reason in profile.rejected
            ],
        )
        conn.execute(
            "UPDATE partition_attempt SET status=?,normalized_rows=?,"
            "ignored_rows=?,rejected_rows=?,finished_at=? WHERE attempt_id=?",
            (
                status,
                profile.frames,
                len(profile.ignored),
                len(profile.rejected),
                finished,
                prepared.attempt_id,
            ),
        )
        conn.execute(
            "INSERT INTO materialization_partition_head VALUES (?,?,?,?,?,?) "
            "ON CONFLICT(market_id,domain,partition_key) DO UPDATE SET "
            "normalization_version=excluded.normalization_version,"
            "attempt_id=excluded.attempt_id,activated_at=excluded.activated_at",
            (
                prepared.market_id,
                "book_l2",
                item.partition_key,
                L2_NORMALIZATION_VERSION,
                prepared.attempt_id,
                finished,
            ),
        )
        if profile.observations:
            register_materialized_raw_v3_observations(
                conn,
                endpoint_id=OKX_ENDPOINT_ID,
                endpoint_revision=OKX_ENDPOINT_REVISION,
                run_id=item.run_id,
                market_id=prepared.market_id,
                capability_venue_id="okx",
                capability_domain="book_realtime",
                capability_endpoint=OKX_ENDPOINT,
                capability_revision=prepared.capability_revision,
                observations=profile.observations,
            )
        conn.commit()


def materialize_segment(
    root: Path,
    conn: sqlite3.Connection,
    item: LiveSegmentInput,
) -> LiveL2Result:
    """Materialize one segment with an explicit terminal-book dependency."""
    prepared = _prepare_attempt(root, conn, item)
    if prepared.reused is not None:
        return prepared.reused
    attempt_id = prepared.attempt_id
    output_dir = (
        root / "materialized" / "book_l2"
        / f"schema_version={L2_SCHEMA_VERSION}"
        / f"normalization_version={L2_NORMALIZATION_VERSION}"
        / "venue_id=okx"
        / f"market_id={prepared.market_id}"
        / f"run_id={item.run_id}"
        / f"segment={item.segment_sequence:06d}"
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    frame_csv = output_dir / f".{attempt_id}.frames.csv"
    level_csv = output_dir / f".{attempt_id}.levels.csv"
    frame_tmp = output_dir / f".{attempt_id}.frames.parquet"
    level_tmp = output_dir / f".{attempt_id}.levels.parquet"
    try:
        profile = _stage(
            item,
            prepared.context,
            market_id=prepared.market_id,
            mapping_revision=prepared.mapping_revision,
            capability_revision=prepared.capability_revision,
            instrument_id=prepared.instrument_id,
            frame_csv=frame_csv,
            level_csv=level_csv,
        )
        db: Any = duckdb.connect(":memory:")
        db.execute("SET TimeZone='UTC'")
        try:
            create_book_l2_v5_tables(db)
            if profile.frames:
                _copy_csv(db, BOOK_L2_FRAME_DATASET, frame_csv)
            if profile.levels:
                _copy_csv(db, BOOK_L2_LEVEL_DATASET, level_csv)
            _validate_tables(
                db,
                profile,
                market_id=prepared.market_id,
                artifact_identity=item.artifact.artifact_id,
                capability_revision=prepared.capability_revision,
            )
            _write_parquet(
                db,
                f"SELECT * FROM {BOOK_L2_FRAME_DATASET} "
                "ORDER BY source_row_index,frame_id",
                frame_tmp,
            )
            _write_parquet(
                db,
                f"SELECT * FROM {BOOK_L2_LEVEL_DATASET} "
                "ORDER BY frame_id,side,source_level_index",
                level_tmp,
            )
        finally:
            db.close()
        frame_csv.unlink()
        level_csv.unlink()
        frame_path, frame_sha = _finalize(frame_tmp)
        level_path, level_sha = _finalize(level_tmp)
        checkpoint_path, checkpoint_sha = _write_terminal_checkpoint(
            output_dir,
            attempt_id,
            prepared.market_id,
            item,
            prepared.context,
            profile,
        )
        status = "complete_with_rejections" if profile.rejected else "complete"
        manifest_body = {
            "attempt_id": attempt_id,
            "status": status,
            "market_id": prepared.market_id,
            "partition_key": item.partition_key,
            "normalization_version": L2_NORMALIZATION_VERSION,
            "schema_version": L2_SCHEMA_VERSION,
            "endpoint_id": OKX_ENDPOINT_ID,
            "endpoint_revision": OKX_ENDPOINT_REVISION,
            "capability_revision": prepared.capability_revision,
            "input_artifact_id": item.artifact.artifact_id,
            "context_artifact_id": (
                None
                if prepared.context is None
                else prepared.context.artifact.artifact_id
            ),
            "contract_artifact_id": prepared.contract_artifact.artifact_id,
            "profile": {
                **asdict(profile),
                "observations": [asdict(row) for row in profile.observations],
            },
            "outputs": {
                BOOK_L2_FRAME_DATASET: _relative_storage_path(root, frame_path),
                BOOK_L2_LEVEL_DATASET: _relative_storage_path(root, level_path),
                REPLAY_STATE_DATASET: _relative_storage_path(
                    root, checkpoint_path
                ),
            },
        }
        manifest_path = output_dir / f"manifest-{attempt_id}.json"
        atomic_write_text(
            manifest_path,
            json.dumps(manifest_body, ensure_ascii=False, indent=2) + "\n",
        )
        _commit(
            root,
            conn,
            item,
            prepared.context,
            prepared,
            profile,
            frame_path,
            frame_sha,
            level_path,
            level_sha,
            checkpoint_path,
            checkpoint_sha,
            manifest_path,
        )
        return LiveL2Result(
            attempt_id=attempt_id,
            market_id=prepared.market_id,
            partition_key=item.partition_key,
            status=status,
            source_rows=profile.source_rows,
            frame_rows=profile.frames,
            level_rows=profile.levels,
            ignored_rows=len(profile.ignored),
            rejected_rows=len(profile.rejected),
            frame_path=_relative_storage_path(root, frame_path),
            level_path=_relative_storage_path(root, level_path),
            reused=False,
        )
    except Exception as exc:
        for path in (frame_csv, level_csv, frame_tmp, level_tmp):
            try:
                path.unlink()
            except FileNotFoundError:
                pass
        _mark_failed(root, conn, attempt_id, exc)
        raise


def materialize_all(
    root: Path,
    conn: sqlite3.Connection,
    *,
    report_reused: bool = True,
) -> list[LiveL2Result]:
    """Materialize every sealed OKX live segment in run order."""
    inputs = sealed_inputs(root)
    results: list[LiveL2Result] = []
    for index, item in enumerate(inputs, start=1):
        result = materialize_segment(root, conn, item)
        results.append(result)
        if report_reused or not result.reused:
            print(
                f"[{index}/{len(inputs)}] "
                f"{'REUSED' if result.reused else 'DONE'} "
                f"{result.market_id} {result.partition_key} "
                f"frames={result.frame_rows:,} levels={result.level_rows:,} "
                f"ignored={result.ignored_rows} rejected={result.rejected_rows}",
                flush=True,
            )
    return results


def _audit_sequences(db: Any, frame_paths: list[str]) -> list[str]:
    errors: list[str] = []
    if not frame_paths:
        return errors
    rows = db.execute(
        "SELECT run_id,connection_id,segment_sequence,source_row_index,"
        "message_kind,sequence_id,prev_sequence_id,changed_bid_levels,"
        "changed_ask_levels,data_quality FROM read_parquet(?, union_by_name=true) "
        "ORDER BY run_id,connection_id,segment_sequence,source_row_index",
        [frame_paths],
    ).fetchall()
    state: dict[tuple[str, str], tuple[int, bool]] = {}
    for row in rows:
        run_id, connection_id = str(row[0]), str(row[1])
        key = (run_id, connection_id)
        kind = str(row[4])
        sequence = int(str(row[5]))
        previous = int(str(row[6]))
        changed = int(row[7]) + int(row[8])
        quality = set(json.loads(str(row[9])))
        last, snapshot_seen = state.get(key, (previous, False))
        if kind == "snapshot":
            state[key] = (sequence, True)
            continue
        if key in state and previous != last:
            errors.append(
                f"sequence discontinuity: {run_id}/{connection_id}/"
                f"{row[2]}/{row[3]}"
            )
        if changed == 0 and sequence != previous:
            errors.append(
                f"empty update advances sequence: {run_id}/{connection_id}/"
                f"{row[2]}/{row[3]}"
            )
        if changed == 0 and "empty_update_heartbeat" not in quality:
            errors.append(
                f"empty update lacks heartbeat flag: {run_id}/{connection_id}/"
                f"{row[2]}/{row[3]}"
            )
        if changed > 0 and sequence <= previous:
            errors.append(
                f"non-empty update does not advance: {run_id}/{connection_id}/"
                f"{row[2]}/{row[3]}"
            )
        if not snapshot_seen and "delta_before_connection_snapshot" not in quality:
            errors.append(
                f"pre-snapshot delta is not flagged: {run_id}/{connection_id}/"
                f"{row[2]}/{row[3]}"
            )
        if key not in state and "connection_sequence_anchor_missing" not in quality:
            errors.append(
                f"first delta lacks sequence-anchor flag: {run_id}/{connection_id}/"
                f"{row[2]}/{row[3]}"
            )
        state[key] = (sequence, snapshot_seen)
    return errors


def audit_live_l2(root: Path, conn: sqlite3.Connection) -> dict[str, object]:
    """Audit active OKX live heads, lineage, hashes, and sequence continuity."""
    errors: list[str] = []
    rows = conn.execute(
        "SELECT a.attempt_id,a.market_id,a.partition_key,a.source_rows,"
        "a.normalized_rows,a.ignored_rows,a.rejected_rows,"
        "MAX(CASE WHEN o.dataset=? THEN r.storage_path END),"
        "MAX(CASE WHEN o.dataset=? THEN r.sha256 END),"
        "MAX(CASE WHEN o.dataset=? THEN r.storage_path END),"
        "MAX(CASE WHEN o.dataset=? THEN r.sha256 END),"
        "MAX(CASE WHEN o.dataset=? THEN o.row_count END) "
        "FROM materialization_partition_head h JOIN partition_attempt a "
        "ON a.attempt_id=h.attempt_id JOIN market m ON m.market_id=a.market_id "
        "JOIN materialization_output o ON o.attempt_id=a.attempt_id "
        "JOIN artifact r ON r.artifact_id=o.artifact_id "
        "WHERE h.domain='book_l2' AND m.venue_id='okx' "
        "AND a.partition_key LIKE 'live/%' "
        "AND a.normalization_version=? "
        "GROUP BY a.attempt_id,a.market_id,a.partition_key,a.source_rows,"
        "a.normalized_rows,a.ignored_rows,a.rejected_rows "
        "ORDER BY a.partition_key",
        (
            BOOK_L2_FRAME_DATASET,
            BOOK_L2_FRAME_DATASET,
            BOOK_L2_LEVEL_DATASET,
            BOOK_L2_LEVEL_DATASET,
            BOOK_L2_LEVEL_DATASET,
            L2_NORMALIZATION_VERSION,
        ),
    ).fetchall()
    if not rows:
        return {"ok": False, "attempts": 0, "errors": ["no active OKX live L2 heads"]}
    db: Any = duckdb.connect(":memory:")
    db.execute("SET TimeZone='UTC'")
    frame_paths: list[str] = []
    total_frames = total_levels = 0
    try:
        for row in rows:
            attempt = str(row[0])
            prefix = f"{row[1]}/{row[2]}"
            source, normalized, ignored, rejected = map(int, row[3:7])
            if source != normalized + ignored + rejected:
                errors.append(f"{prefix}: source classification is not conserved")
            if rejected:
                errors.append(f"{prefix}: {rejected} rejected raw rows")
            if not row[7] or not row[9]:
                errors.append(f"{prefix}: frame or level output is missing")
                continue
            frame_path = root / str(row[7])
            level_path = root / str(row[9])
            if not frame_path.is_file() or not level_path.is_file():
                errors.append(f"{prefix}: output file is missing")
                continue
            if sha256_file(frame_path) != str(row[8]):
                errors.append(f"{prefix}: frame SHA-256 mismatch")
            if sha256_file(level_path) != str(row[10]):
                errors.append(f"{prefix}: level SHA-256 mismatch")
            input_rows = conn.execute(
                "SELECT artifact_id,storage_path,source_rows,normalized_rows,"
                "ignored_rows,rejected_rows FROM partition_input_binding "
                "WHERE attempt_id=? ORDER BY artifact_id,storage_path",
                (attempt,),
            ).fetchall()
            input_body = "\n".join(sorted(
                f"{entry[0]}|{entry[1]}" for entry in input_rows
            ))
            expected_input_hash = hashlib.sha256(
                input_body.encode("ascii")
            ).hexdigest()
            recorded_input_hash = conn.execute(
                "SELECT input_set_hash FROM partition_attempt WHERE attempt_id=?",
                (attempt,),
            ).fetchone()
            if (
                recorded_input_hash is None
                or str(recorded_input_hash[0]) != expected_input_hash
            ):
                errors.append(f"{prefix}: exact input set binding failed")
            classified_inputs = [entry for entry in input_rows if int(entry[2]) > 0]
            if (
                len(classified_inputs) != 1
                or tuple(map(int, classified_inputs[0][2:]))
                != (source, normalized, ignored, rejected)
            ):
                errors.append(f"{prefix}: target/context input counts failed")
            capability = conn.execute(
                "SELECT revision_id,binding_basis FROM partition_capability_binding "
                "WHERE attempt_id=? AND venue_id='okx' "
                "AND domain='book_realtime' AND endpoint=?",
                (attempt, OKX_ENDPOINT),
            ).fetchall()
            capability_revision = (
                int(capability[0][0]) if len(capability) == 1 else -1
            )
            if (
                len(capability) != 1
                or capability_revision < 0
                or str(capability[0][1]) != "recorded"
            ):
                errors.append(f"{prefix}: capability binding failed")
            checkpoint_rows = conn.execute(
                "SELECT row_count FROM materialization_output "
                "WHERE attempt_id=? AND dataset=?",
                (attempt, REPLAY_STATE_DATASET),
            ).fetchall()
            if len(checkpoint_rows) != 1:
                errors.append(f"{prefix}: terminal checkpoint output is missing")
            else:
                try:
                    loaded_checkpoint = load_terminal_checkpoint_for_attempt(
                        root, conn, attempt
                    )
                    checkpoint = loaded_checkpoint.checkpoint
                    segment = int(str(row[2]).rsplit("-", 1)[1])
                    expected_source_artifact = (
                        str(classified_inputs[0][0])
                        if len(classified_inputs) == 1 else ""
                    )
                    if (
                        checkpoint.market_id != str(row[1])
                        or checkpoint.run_id != str(row[2]).split("/")[1]
                        or checkpoint.segment_sequence != segment
                        or checkpoint.source_artifact_id != expected_source_artifact
                        or int(checkpoint_rows[0][0])
                        != len(checkpoint.asks) + len(checkpoint.bids)
                    ):
                        errors.append(
                            f"{prefix}: terminal checkpoint identity failed"
                        )
                except (TerminalCheckpointError, ValueError) as exc:
                    errors.append(f"{prefix}: terminal checkpoint failed: {exc}")
            frame = db.execute(
                "SELECT COUNT(*),COUNT(*)-COUNT(DISTINCT frame_id),"
                "SUM(available_time<event_time),SUM(source_depth_levels),"
                "COUNT(DISTINCT capability_revision),MIN(capability_revision),"
                "SUM(CASE WHEN endpoint<>? OR endpoint_id<>? "
                "OR endpoint_revision<>? OR checksum IS NOT NULL "
                "OR normalization_version<>? OR schema_version<>? "
                "OR sequence_id IS NULL OR prev_sequence_id IS NULL "
                "OR payload_schema_version<>? OR replay_fidelity<>? "
                "OR integrity_mode<>? "
                "OR strpos(data_quality,'checksum_unsupported_fixed_zero')=0 "
                "THEN 1 ELSE 0 END) FROM read_parquet(?)",
                [
                    OKX_ENDPOINT,
                    OKX_ENDPOINT_ID,
                    OKX_ENDPOINT_REVISION,
                    L2_NORMALIZATION_VERSION,
                    L2_SCHEMA_VERSION,
                    OKX_PAYLOAD_SCHEMA_VERSION,
                    OKX_REPLAY_FIDELITY,
                    OKX_INTEGRITY_MODE,
                    str(frame_path),
                ],
            ).fetchone()
            level = db.execute(
                "SELECT COUNT(*),COUNT(*)-COUNT(DISTINCT "
                "(frame_id,side,source_level_index)),"
                "SUM(CASE WHEN side NOT IN ('bid','ask') "
                "OR action NOT IN ('set','delete') OR order_count<0 "
                "OR ((size='0')<>(order_count=0)) THEN 1 ELSE 0 END) "
                "FROM read_parquet(?)",
                [str(level_path)],
            ).fetchone()
            if frame is None or level is None:
                errors.append(f"{prefix}: Parquet output is unreadable")
                continue
            frame_count, level_count = int(frame[0]), int(level[0])
            total_frames += frame_count
            total_levels += level_count
            if (
                frame_count != normalized
                or int(frame[1] or 0)
                or int(frame[2] or 0)
                or int(frame[3] or 0) != level_count
                or (frame_count and (
                    int(frame[4]) != 1
                    or int(frame[5]) != capability_revision
                ))
                or int(frame[6] or 0)
            ):
                errors.append(f"{prefix}: frame identity, PIT, or counts failed")
            if (
                level_count != int(row[11])
                or int(level[1] or 0)
                or int(level[2] or 0)
            ):
                errors.append(f"{prefix}: level key or semantics failed")
            if frame_count:
                control = conn.execute(
                    "SELECT COUNT(*) FROM collection_channel c "
                    "JOIN collection_connection n ON n.connection_id=c.connection_id "
                    "WHERE c.market_id=? AND n.collection_run_id=? "
                    "AND n.endpoint_id=? AND n.endpoint_revision=? "
                    "AND c.capability_domain='book_realtime' "
                    "AND c.capability_endpoint=? AND c.capability_revision=?",
                    (
                        str(row[1]),
                        str(row[2]).split("/")[1],
                        OKX_ENDPOINT_ID,
                        OKX_ENDPOINT_REVISION,
                        OKX_ENDPOINT,
                        capability_revision,
                    ),
                ).fetchone()
                if control is None or int(control[0]) == 0:
                    errors.append(f"{prefix}: connection/channel binding failed")
            frame_paths.append(str(frame_path))
        errors.extend(_audit_sequences(db, frame_paths))
    finally:
        db.close()
    foreign_keys = conn.execute("PRAGMA foreign_key_check").fetchall()
    if foreign_keys:
        errors.append(f"SQLite foreign key errors: {len(foreign_keys)}")
    return {
        "ok": not errors,
        "attempts": len(rows),
        "frames": total_frames,
        "levels": total_levels,
        "errors": errors,
    }


def watch(
    root: Path,
    conn: sqlite3.Connection,
    *,
    poll_seconds: float = WATCH_POLL_SECONDS,
) -> None:
    """Continuously project newly sealed segments with idempotent reuse."""
    if poll_seconds <= 0:
        raise ValueError("poll_seconds must be positive")
    while True:
        results = materialize_all(root, conn, report_reused=False)
        changed = [result for result in results if not result.reused]
        if changed:
            audit = audit_live_l2(root, conn)
            print(json.dumps({
                "event": "okx_live_l2_cycle",
                "new_partitions": len(changed),
                "audit_ok": audit["ok"],
                "frames": audit.get("frames", 0),
                "levels": audit.get("levels", 0),
                "errors": audit.get("errors", []),
            }, ensure_ascii=False), flush=True)
        time.sleep(poll_seconds)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="OKX live books L2 materializer")
    parser.add_argument("--data-root", type=Path, default=None)
    parser.add_argument("command", choices=("all", "watch", "audit"))
    parser.add_argument("--poll-seconds", type=float, default=WATCH_POLL_SECONDS)
    args = parser.parse_args(argv)
    root = (args.data_root or configured_data_root()).resolve()
    conn = store.connect(root)
    try:
        if args.command == "all":
            result: object = [asdict(row) for row in materialize_all(root, conn)]
            code = 0
        elif args.command == "audit":
            result = audit_live_l2(root, conn)
            code = 0 if bool(result["ok"]) else 1
        else:
            try:
                watch(root, conn, poll_seconds=float(args.poll_seconds))
            except KeyboardInterrupt:
                return 130
            return 0
    finally:
        conn.close()
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
