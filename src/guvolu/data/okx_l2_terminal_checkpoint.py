"""OKX live L2 segment terminal-book checkpoint contract and loader."""
from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping

from guvolu.data.book_l2_contract import BOOK_L2_V5_NORMALIZATION_VERSION

TERMINAL_CHECKPOINT_DATASET = "book_l2_terminal_checkpoint"
TERMINAL_CHECKPOINT_SCHEMA_VERSION = 1
TERMINAL_CHECKPOINT_VERSION = "okx-book-terminal-v1"
MAX_LEVELS_PER_SIDE = 400


class TerminalCheckpointError(ValueError):
    """A terminal checkpoint failed identity, integrity, or PIT validation."""


@dataclass(frozen=True, slots=True)
class TerminalLevel:
    """One live terminal-book level."""

    price: str
    size: str
    order_count: int


@dataclass(frozen=True, slots=True)
class OkxTerminalCheckpoint:
    """One complete or explicitly untrusted segment terminal state."""

    market_id: str
    venue_symbol: str
    run_id: str
    segment_sequence: int
    connection_id: str
    channel_id: str
    trusted: bool
    trust_reason: str
    sequence_id: int | None
    terminal_record_sequence: int
    last_recv_mono_ns: int | None
    checkpoint_available_time: datetime
    as_of_frame_id: str | None
    as_of_event_time: datetime | None
    as_of_available_time: datetime | None
    as_of_ingest_time: datetime | None
    snapshot_frame_id: str | None
    snapshot_event_time: datetime | None
    snapshot_available_time: datetime | None
    asks: tuple[TerminalLevel, ...]
    bids: tuple[TerminalLevel, ...]
    state_sha256: str
    source_attempt_id: str
    source_artifact_id: str
    upstream_attempt_id: str | None
    upstream_checkpoint_artifact_id: str | None
    source_normalization_version: str
    checkpoint_version: str = TERMINAL_CHECKPOINT_VERSION
    schema_version: int = TERMINAL_CHECKPOINT_SCHEMA_VERSION

    def as_book_state(self) -> dict[str, Any]:
        """Return a query-ready immutable base without hiding trust."""

        return {
            "market_id": self.market_id,
            "asks": [
                {"price": row.price, "size": row.size,
                 "order_count": row.order_count}
                for row in self.asks
            ],
            "bids": [
                {"price": row.price, "size": row.size,
                 "order_count": row.order_count}
                for row in self.bids
            ],
            "trusted": self.trusted,
            "trust_reason": self.trust_reason,
            "connection_id": self.connection_id,
            "sequence_id": self.sequence_id,
            "as_of_frame_id": self.as_of_frame_id,
            "as_of_event_time": _iso(self.as_of_event_time),
            "as_of_available_time": _iso(self.as_of_available_time),
            "source_attempt_id": self.source_attempt_id,
            "source_artifact_id": self.source_artifact_id,
            "state_sha256": self.state_sha256,
            "state_source": TERMINAL_CHECKPOINT_DATASET,
        }


@dataclass(frozen=True, slots=True)
class LoadedTerminalCheckpoint:
    """A checkpoint plus its independently registered file artifact."""

    checkpoint: OkxTerminalCheckpoint
    attempt_id: str
    artifact_id: str
    storage_path: str
    file_sha256: str
    byte_count: int


def _iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).isoformat()


def _time(value: object, field: str, *, optional: bool = False) -> datetime | None:
    if value is None and optional:
        return None
    if not isinstance(value, str):
        raise TerminalCheckpointError(f"{field} is not an ISO-8601 string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise TerminalCheckpointError(f"{field} is not ISO-8601") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise TerminalCheckpointError(f"{field} lacks a UTC offset")
    return parsed.astimezone(UTC)


def _integer(
    value: object, field: str, *, optional: bool = False,
) -> int | None:
    if value is None and optional:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise TerminalCheckpointError(f"{field} is not a non-negative integer")
    return value


def _level(value: object, side: str) -> TerminalLevel:
    if not isinstance(value, Mapping) or set(value) != {
        "price", "size", "order_count",
    }:
        raise TerminalCheckpointError(f"{side} terminal level shape is invalid")
    price = value["price"]
    size = value["size"]
    count = value["order_count"]
    if not isinstance(price, str) or not isinstance(size, str):
        raise TerminalCheckpointError(f"{side} terminal level decimals are not strings")
    if isinstance(count, bool) or not isinstance(count, int) or count <= 0:
        raise TerminalCheckpointError(f"{side} terminal order_count is invalid")
    try:
        price_number = Decimal(price)
        size_number = Decimal(size)
    except InvalidOperation as exc:
        raise TerminalCheckpointError(f"{side} terminal decimal is invalid") from exc
    if (
        not price_number.is_finite() or price_number <= 0
        or not size_number.is_finite() or size_number <= 0
    ):
        raise TerminalCheckpointError(f"{side} terminal level is non-positive")
    return TerminalLevel(price, size, count)


def _semantic_body(body: Mapping[str, object]) -> dict[str, object]:
    semantic = dict(body)
    semantic.pop("state_sha256", None)
    return semantic


def terminal_state_sha256(body: Mapping[str, object]) -> str:
    """Hash the canonical semantic state, excluding its own hash field."""

    encoded = json.dumps(
        _semantic_body(body), ensure_ascii=False, sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return "sha256-" + hashlib.sha256(encoded).hexdigest()


def checkpoint_body(
    *,
    market_id: str,
    venue_symbol: str,
    run_id: str,
    segment_sequence: int,
    connection_id: str,
    channel_id: str,
    trusted: bool,
    trust_reason: str,
    sequence_id: int | None,
    terminal_record_sequence: int,
    last_recv_mono_ns: int | None,
    checkpoint_available_time: str,
    as_of_frame_id: str | None,
    as_of_event_time: str | None,
    as_of_available_time: str | None,
    as_of_ingest_time: str | None,
    snapshot_frame_id: str | None,
    snapshot_event_time: str | None,
    snapshot_available_time: str | None,
    asks: tuple[TerminalLevel, ...],
    bids: tuple[TerminalLevel, ...],
    source_attempt_id: str,
    source_artifact_id: str,
    upstream_attempt_id: str | None,
    upstream_checkpoint_artifact_id: str | None,
) -> dict[str, object]:
    """Build a canonical checkpoint body with a non-self-referential state SHA."""

    body: dict[str, object] = {
        "schema_version": TERMINAL_CHECKPOINT_SCHEMA_VERSION,
        "checkpoint_version": TERMINAL_CHECKPOINT_VERSION,
        "source_normalization_version": BOOK_L2_V5_NORMALIZATION_VERSION,
        "market_id": market_id,
        "venue_symbol": venue_symbol,
        "run_id": run_id,
        "segment_sequence": segment_sequence,
        "connection_id": connection_id,
        "channel_id": channel_id,
        "trusted": trusted,
        "trust_reason": trust_reason,
        "sequence_id": sequence_id,
        "terminal_record_sequence": terminal_record_sequence,
        "last_recv_mono_ns": last_recv_mono_ns,
        "checkpoint_available_time": checkpoint_available_time,
        "as_of_frame_id": as_of_frame_id,
        "as_of_event_time": as_of_event_time,
        "as_of_available_time": as_of_available_time,
        "as_of_ingest_time": as_of_ingest_time,
        "snapshot_frame_id": snapshot_frame_id,
        "snapshot_event_time": snapshot_event_time,
        "snapshot_available_time": snapshot_available_time,
        "asks": [
            {"price": row.price, "size": row.size,
             "order_count": row.order_count}
            for row in asks
        ],
        "bids": [
            {"price": row.price, "size": row.size,
             "order_count": row.order_count}
            for row in bids
        ],
        "source_attempt_id": source_attempt_id,
        "source_artifact_id": source_artifact_id,
        "upstream_attempt_id": upstream_attempt_id,
        "upstream_checkpoint_artifact_id": upstream_checkpoint_artifact_id,
    }
    body["state_sha256"] = terminal_state_sha256(body)
    return body


def _parse_body(body: object) -> OkxTerminalCheckpoint:
    if not isinstance(body, Mapping):
        raise TerminalCheckpointError("terminal checkpoint body is not an object")
    if body.get("schema_version") != TERMINAL_CHECKPOINT_SCHEMA_VERSION:
        raise TerminalCheckpointError("terminal checkpoint schema is unsupported")
    if body.get("checkpoint_version") != TERMINAL_CHECKPOINT_VERSION:
        raise TerminalCheckpointError("terminal checkpoint version is unsupported")
    if body.get("source_normalization_version") != BOOK_L2_V5_NORMALIZATION_VERSION:
        raise TerminalCheckpointError("terminal checkpoint source version is invalid")
    recorded_sha = body.get("state_sha256")
    if not isinstance(recorded_sha, str) or recorded_sha != terminal_state_sha256(body):
        raise TerminalCheckpointError("terminal checkpoint state SHA failed")
    strings: dict[str, str] = {}
    for field in (
        "market_id", "venue_symbol", "run_id", "connection_id", "channel_id",
        "trust_reason", "source_attempt_id", "source_artifact_id",
    ):
        value = body.get(field)
        if not isinstance(value, str) or not value:
            raise TerminalCheckpointError(f"terminal checkpoint {field} is invalid")
        strings[field] = value
    if not strings["connection_id"].startswith(strings["run_id"] + "-c"):
        raise TerminalCheckpointError("terminal checkpoint connection/run mismatch")
    if strings["channel_id"] != f"books:{strings['venue_symbol']}":
        raise TerminalCheckpointError("terminal checkpoint channel is invalid")
    source_artifact = strings["source_artifact_id"]
    if not source_artifact.startswith("sha256-") or len(source_artifact) != 71:
        raise TerminalCheckpointError("terminal checkpoint source artifact is invalid")
    trusted = body.get("trusted")
    if not isinstance(trusted, bool):
        raise TerminalCheckpointError("terminal checkpoint trust is invalid")
    segment = _integer(body.get("segment_sequence"), "segment_sequence")
    terminal_record = _integer(
        body.get("terminal_record_sequence"), "terminal_record_sequence",
    )
    if segment is None or segment <= 0 or terminal_record is None or terminal_record <= 0:
        raise TerminalCheckpointError("terminal checkpoint terminal identity is invalid")
    sequence = _integer(body.get("sequence_id"), "sequence_id", optional=True)
    recv_mono = _integer(
        body.get("last_recv_mono_ns"), "last_recv_mono_ns", optional=True,
    )
    checkpoint_available = _time(
        body.get("checkpoint_available_time"), "checkpoint_available_time",
    )
    assert checkpoint_available is not None
    event = _time(body.get("as_of_event_time"), "as_of_event_time", optional=True)
    available = _time(
        body.get("as_of_available_time"), "as_of_available_time", optional=True,
    )
    ingest = _time(body.get("as_of_ingest_time"), "as_of_ingest_time", optional=True)
    snapshot_event = _time(
        body.get("snapshot_event_time"), "snapshot_event_time", optional=True,
    )
    snapshot_available = _time(
        body.get("snapshot_available_time"), "snapshot_available_time", optional=True,
    )
    as_of_frame = body.get("as_of_frame_id")
    snapshot_frame = body.get("snapshot_frame_id")
    for field, value in (
        ("as_of_frame_id", as_of_frame), ("snapshot_frame_id", snapshot_frame),
    ):
        if value is not None and (not isinstance(value, str) or not value):
            raise TerminalCheckpointError(f"terminal checkpoint {field} is invalid")
    as_of_values = (as_of_frame, event, available, ingest)
    if any(value is None for value in as_of_values) != all(
        value is None for value in as_of_values
    ):
        raise TerminalCheckpointError("terminal checkpoint as-of quartet is partial")
    snapshot_values = (snapshot_frame, snapshot_event, snapshot_available)
    if any(value is None for value in snapshot_values) != all(
        value is None for value in snapshot_values
    ):
        raise TerminalCheckpointError("terminal checkpoint snapshot identity is partial")
    if event is not None and available is not None and available < event:
        raise TerminalCheckpointError("terminal checkpoint violates PIT ordering")
    if available is not None and checkpoint_available < available:
        raise TerminalCheckpointError("terminal checkpoint availability regressed")
    if (
        snapshot_event is not None and snapshot_available is not None
        and snapshot_available < snapshot_event
    ):
        raise TerminalCheckpointError("terminal checkpoint snapshot PIT is invalid")
    raw_asks = body.get("asks")
    raw_bids = body.get("bids")
    if not isinstance(raw_asks, list) or not isinstance(raw_bids, list):
        raise TerminalCheckpointError("terminal checkpoint book is missing")
    asks = tuple(_level(row, "ask") for row in raw_asks)
    bids = tuple(_level(row, "bid") for row in raw_bids)
    if len(asks) > MAX_LEVELS_PER_SIDE or len(bids) > MAX_LEVELS_PER_SIDE:
        raise TerminalCheckpointError("terminal checkpoint exceeds native depth")
    if len({row.price for row in asks}) != len(asks) or len(
        {row.price for row in bids}
    ) != len(bids):
        raise TerminalCheckpointError("terminal checkpoint has duplicate prices")
    if asks != tuple(sorted(asks, key=lambda row: Decimal(row.price))):
        raise TerminalCheckpointError("terminal asks are not ascending")
    if bids != tuple(sorted(bids, key=lambda row: Decimal(row.price), reverse=True)):
        raise TerminalCheckpointError("terminal bids are not descending")
    if trusted:
        if (
            not asks or not bids or sequence is None or as_of_frame is None
            or snapshot_frame is None
        ):
            raise TerminalCheckpointError("trusted terminal checkpoint is incomplete")
        if Decimal(bids[0].price) >= Decimal(asks[0].price):
            raise TerminalCheckpointError("trusted terminal checkpoint is crossed")
    elif asks or bids:
        raise TerminalCheckpointError("untrusted terminal checkpoint exposes a book")
    upstream_attempt = body.get("upstream_attempt_id")
    upstream_artifact = body.get("upstream_checkpoint_artifact_id")
    if (upstream_attempt is None) != (upstream_artifact is None):
        raise TerminalCheckpointError("terminal checkpoint upstream binding is partial")
    if upstream_attempt is not None and (
        not isinstance(upstream_attempt, str) or not upstream_attempt
        or not isinstance(upstream_artifact, str)
        or not upstream_artifact.startswith("sha256-")
    ):
        raise TerminalCheckpointError("terminal checkpoint upstream binding is invalid")
    return OkxTerminalCheckpoint(
        market_id=strings["market_id"], venue_symbol=strings["venue_symbol"],
        run_id=strings["run_id"], segment_sequence=segment,
        connection_id=strings["connection_id"], channel_id=strings["channel_id"],
        trusted=trusted, trust_reason=strings["trust_reason"],
        sequence_id=sequence, terminal_record_sequence=terminal_record,
        last_recv_mono_ns=recv_mono,
        checkpoint_available_time=checkpoint_available,
        as_of_frame_id=None if as_of_frame is None else str(as_of_frame),
        as_of_event_time=event, as_of_available_time=available,
        as_of_ingest_time=ingest,
        snapshot_frame_id=None if snapshot_frame is None else str(snapshot_frame),
        snapshot_event_time=snapshot_event,
        snapshot_available_time=snapshot_available,
        asks=asks, bids=bids, state_sha256=recorded_sha,
        source_attempt_id=strings["source_attempt_id"],
        source_artifact_id=source_artifact,
        upstream_attempt_id=(
            None if upstream_attempt is None else str(upstream_attempt)
        ),
        upstream_checkpoint_artifact_id=(
            None if upstream_artifact is None else str(upstream_artifact)
        ),
        source_normalization_version=BOOK_L2_V5_NORMALIZATION_VERSION,
    )


def read_terminal_checkpoint(
    path: Path,
    *,
    expected_file_sha256: str | None = None,
    decision_time: datetime | None = None,
    require_trusted: bool = False,
) -> OkxTerminalCheckpoint:
    """Read one immutable checkpoint with file, state, and PIT validation."""

    if not path.is_file():
        raise TerminalCheckpointError(f"terminal checkpoint is missing: {path}")
    data = path.read_bytes()
    actual = hashlib.sha256(data).hexdigest()
    if expected_file_sha256 is not None and actual != expected_file_sha256:
        raise TerminalCheckpointError("terminal checkpoint file SHA failed")
    try:
        body = json.loads(data)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TerminalCheckpointError("terminal checkpoint JSON is invalid") from exc
    checkpoint = _parse_body(body)
    if decision_time is not None:
        if decision_time.tzinfo is None or decision_time.utcoffset() is None:
            raise TerminalCheckpointError("decision_time lacks a UTC offset")
        if checkpoint.checkpoint_available_time > decision_time.astimezone(UTC):
            raise TerminalCheckpointError("terminal checkpoint is not PIT-visible")
    if require_trusted and not checkpoint.trusted:
        raise TerminalCheckpointError(
            f"terminal checkpoint is untrusted: {checkpoint.trust_reason}"
        )
    return checkpoint


def load_terminal_checkpoint_for_attempt(
    root: Path,
    conn: sqlite3.Connection,
    attempt_id: str,
    *,
    decision_time: datetime | None = None,
    require_trusted: bool = False,
) -> LoadedTerminalCheckpoint:
    """Load one registered terminal checkpoint by its source attempt."""

    rows = conn.execute(
        "SELECT o.artifact_id,a.storage_path,a.sha256,a.byte_count,a.schema_version "
        "FROM materialization_output o JOIN artifact a "
        "ON a.artifact_id=o.artifact_id WHERE o.attempt_id=? AND o.dataset=?",
        (attempt_id, TERMINAL_CHECKPOINT_DATASET),
    ).fetchall()
    if len(rows) != 1:
        raise TerminalCheckpointError("attempt has no unique terminal checkpoint")
    row = rows[0]
    if int(row[4]) != TERMINAL_CHECKPOINT_SCHEMA_VERSION:
        raise TerminalCheckpointError("registered terminal checkpoint schema is invalid")
    resolved_root = root.resolve()
    path = (resolved_root / str(row[1])).resolve()
    try:
        path.relative_to(resolved_root)
    except ValueError as exc:
        raise TerminalCheckpointError("terminal checkpoint path escapes data root") from exc
    if not path.is_file() or path.stat().st_size != int(row[3]):
        raise TerminalCheckpointError("terminal checkpoint byte count failed")
    checkpoint = read_terminal_checkpoint(
        path, expected_file_sha256=str(row[2]), decision_time=decision_time,
        require_trusted=require_trusted,
    )
    if checkpoint.source_attempt_id != attempt_id:
        raise TerminalCheckpointError("terminal checkpoint source attempt mismatch")
    return LoadedTerminalCheckpoint(
        checkpoint=checkpoint, attempt_id=attempt_id, artifact_id=str(row[0]),
        storage_path=str(row[1]), file_sha256=str(row[2]), byte_count=int(row[3]),
    )


def load_latest_terminal_checkpoint(
    root: Path,
    conn: sqlite3.Connection,
    market_id: str,
    *,
    decision_time: datetime,
    require_trusted: bool = False,
) -> LoadedTerminalCheckpoint | None:
    """Select the latest active live checkpoint visible at an explicit PIT."""

    if decision_time.tzinfo is None or decision_time.utcoffset() is None:
        raise TerminalCheckpointError("decision_time lacks a UTC offset")
    candidates = conn.execute(
        "SELECT h.attempt_id,h.activated_at FROM materialization_partition_head h "
        "JOIN materialization_output o ON o.attempt_id=h.attempt_id "
        "JOIN market m ON m.market_id=h.market_id "
        "WHERE h.market_id=? AND h.domain='book_l2' "
        "AND h.partition_key LIKE 'live/%' AND m.venue_id='okx' "
        "AND h.normalization_version=? AND o.dataset=? "
        "ORDER BY h.activated_at DESC,h.partition_key DESC",
        (
            market_id,
            BOOK_L2_V5_NORMALIZATION_VERSION,
            TERMINAL_CHECKPOINT_DATASET,
        ),
    ).fetchall()
    visible: list[LoadedTerminalCheckpoint] = []
    for row in candidates:
        loaded = load_terminal_checkpoint_for_attempt(
            root, conn, str(row[0]),
        )
        if loaded.checkpoint.checkpoint_available_time <= decision_time.astimezone(UTC):
            visible.append(loaded)
    if not visible:
        return None
    selected = max(
        visible,
        key=lambda row: (
            row.checkpoint.checkpoint_available_time,
            row.checkpoint.run_id,
            row.checkpoint.segment_sequence,
            row.attempt_id,
        ),
    )
    if require_trusted and not selected.checkpoint.trusted:
        raise TerminalCheckpointError(
            f"latest terminal checkpoint is untrusted: "
            f"{selected.checkpoint.trust_reason}"
        )
    return selected
