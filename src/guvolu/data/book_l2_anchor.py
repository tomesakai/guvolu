"""REST L2 锚点采集、规范化与 WS 旁路对照。

该模块把一次公开 REST 请求保存为独立不可变 raw 制品，再生成
``book_l2_anchor_observation`` 与完整档位。WS 对照另存 reconciliation
事实，只更新低基数 SQLite 摘要，绝不修改 ``book_l2`` 或补写断流窗口。
"""
from __future__ import annotations

import asyncio
import argparse
import base64
import hashlib
import json
import os
import re
import sqlite3
import sys
from collections.abc import Callable, Mapping, Sequence
from contextlib import suppress
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import duckdb

from guvolu.data import store
from guvolu.data.book_l2_anchor_contract import (
    ANCHOR_LEVEL_COLUMNS,
    ANCHOR_LEVEL_DATASET,
    ANCHOR_NORMALIZATION_VERSION,
    ANCHOR_OBSERVATION_COLUMNS,
    ANCHOR_OBSERVATION_DATASET,
    ANCHOR_RECONCILIATION_COLUMNS,
    ANCHOR_RECONCILIATION_DATASET,
    ANCHOR_RECONCILIATION_VERSION,
    ANCHOR_SCHEMA_VERSION,
    create_anchor_tables,
)
from guvolu.data.durable_io import (
    atomic_write_bytes,
    atomic_write_text,
    exclusive_path_lock,
)
from guvolu.data.materialize import (
    _market_row,
    _register_content_artifact,
    _relative_storage_path,
    artifact_id,
    ensure_markets,
    sha256_file,
    utc_now,
)
from guvolu.data.sqlite_writer_lock import sqlite_writer_lock
from guvolu.venues import registry
from guvolu.venues.l2_anchor import (
    ANCHOR_ENDPOINTS,
    AnchorFetch,
    PublicRestAnchorAdapter,
    _request_url as _anchor_request_url,
    anchor_adapter,
)

ANCHOR_DOMAIN = "book_l2_anchor"
RECONCILIATION_DOMAIN = "book_l2_anchor_reconciliation"
LATEST_PARTITION = "latest"
RAW_SCHEMA_VERSION = 1
MAX_WS_CHECKPOINT_AGE_SECONDS = 5.0
ANCHOR_QUEUE_SIZE = 8
ANCHOR_SHUTDOWN_SECONDS = 20.0
ANCHOR_REGISTRATION_ATTEMPTS = 4
_SAFE_PATH_VALUE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_TRIGGER_REASONS = frozenset({"connection_open", "reconnect", "periodic"})
_TRUSTED_WS_INTEGRITY = frozenset({
    "snapshot_no_sequence",
    "snapshot_plus_monotonic_delta",
    "snapshot_plus_unsequenced_delta",
    "snapshot_plus_absolute_delta",
})
_CAPABILITY_ENDPOINTS = {
    "gmo": "/v1/orderbooks",
    "bitbank": "/{pair}/depth",
    "bitflyer": "/v1/getboard",
}
_RAW_RECORD_FIELDS = frozenset({
    "schema_version", "domain", "request_id", "venue_id",
    "venue_symbol", "endpoint_id", "endpoint_key", "endpoint_revision",
    "documentation_uri", "method", "request_url", "request_sha256",
    "requested_at", "trigger_reason", "connection_id", "http_status",
    "response_received_at", "response_sha256", "response_body_base64",
    "network_error_kind", "network_error_detail", "ingest_time",
})


@dataclass(frozen=True, slots=True)
class AnchorLevel:
    """REST 快照的一档价格级。"""

    side: str
    source_level_index: int
    price: str
    size: str


@dataclass(frozen=True, slots=True)
class ParsedAnchor:
    """一份 REST 原件的纯规范化结果。"""

    event_time: str
    available_time: str
    ingest_time: str
    time_origin: str
    receive_source_offset_ms: float | None
    availability_basis: str
    sequence_id: str | None
    levels: tuple[AnchorLevel, ...]
    best_bid: str | None
    best_ask: str | None
    bid_levels: int | None
    ask_levels: int | None
    bid_depth: str | None
    ask_depth: str | None
    book_hash: str | None
    anchor_availability: str
    failure_reason: str | None


@dataclass(frozen=True, slots=True)
class WsCheckpoint:
    """可被 REST 锚点旁路比较的最新可信 WS 末态。"""

    attempt_id: str
    artifact_id: str
    as_of_frame_id: str
    event_time: str
    available_time: str
    sequence_id: str | None
    best_bid: str
    best_ask: str
    bid_levels: int
    ask_levels: int
    bid_depth: str
    ask_depth: str
    book_hash: str


@dataclass(frozen=True, slots=True)
class AnchorComparison:
    """REST 与可信 WS 末态的独立对照裁决。"""

    status: str
    basis: str
    reason: str
    checkpoint: WsCheckpoint | None
    lag_ms: float | None
    best_bid_match: bool | None
    best_ask_match: bool | None
    depth_match: bool | None
    book_hash_match: bool | None
    full_book_comparable: bool


@dataclass(frozen=True, slots=True)
class AnchorMaterializationResult:
    """一次锚点闭环的持久化结果。"""

    observation_id: str
    market_id: str
    status: str
    comparison_status: str
    raw_artifact_id: str
    observation_artifact_id: str
    level_artifact_id: str
    reconciliation_artifact_id: str
    anchor_attempt_id: str
    reconciliation_attempt_id: str
    level_rows: int


@dataclass(frozen=True, slots=True)
class RawAnchorRecoveryResult:
    """一份 raw 原件的恢复检查或执行结果。"""

    raw_storage_path: str
    raw_artifact_id: str
    venue_id: str
    outcome: str
    artifact_registered_before: bool
    attempt_ids_before: tuple[str, ...]
    head_bindings_before: tuple[tuple[str, str, str], ...]
    anchor_attempt_id: str | None
    reconciliation_attempt_id: str | None
    materialization: AnchorMaterializationResult | None


@dataclass(frozen=True, slots=True)
class AnchorJob:
    """由 WS 连接边界投递的非阻塞锚点任务。"""

    connection_id: str | None
    trigger_reason: str


def _aware(value: object, field: str) -> datetime:
    text = str(value)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field} 不是 ISO-8601 时间") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field} 缺少时区")
    return parsed.astimezone(UTC)


def _iso(value: object, field: str) -> str:
    if isinstance(value, datetime):
        parsed = value
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError(f"{field} 缺少时区")
        return parsed.astimezone(UTC).isoformat()
    return _aware(value, field).isoformat()


def _millis(value: object, field: str) -> str:
    if isinstance(value, bool):
        raise ValueError(f"{field} 不是毫秒时间")
    try:
        stamp = int(str(value))
    except ValueError as exc:
        raise ValueError(f"{field} 不是毫秒时间") from exc
    if stamp <= 0:
        raise ValueError(f"{field} 必须为正数")
    return datetime.fromtimestamp(stamp / 1000, UTC).isoformat()


def _available_time(
    event_time: str, response_received_at: str, ingest_time: str,
) -> tuple[str, str, float]:
    """按 PIT 上界计算可见时刻并保留有符号来源偏移。"""
    event = _aware(event_time, "event_time")
    received = _aware(response_received_at, "response_received_at")
    ingested = _aware(ingest_time, "ingest_time")
    candidates = (
        (event, "event_time"),
        (received, "response_receive"),
        (ingested, "ingest_time"),
    )
    visible, basis = max(candidates, key=lambda item: item[0])
    offset_ms = (received - event).total_seconds() * 1000
    return visible.isoformat(), basis, offset_ms


def _decimal(value: object, field: str, *, positive: bool = True) -> Decimal:
    if isinstance(value, bool):
        raise ValueError(f"{field} 不是金额数值")
    try:
        number = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"{field} 不是十进制定点数") from exc
    if not number.is_finite() or (number <= 0 if positive else number < 0):
        raise ValueError(f"{field} 超出允许范围")
    return number


def _decimal_text(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if text in {"", "-0"} else text


def _levels(rows: object, side: str) -> tuple[AnchorLevel, ...]:
    if not isinstance(rows, list):
        raise ValueError(f"{side} 价位不是数组")
    output: list[AnchorLevel] = []
    prices: set[str] = set()
    for index, row in enumerate(rows):
        if isinstance(row, Mapping):
            raw_price, raw_size = row.get("price"), row.get("size")
        elif isinstance(row, list) and len(row) >= 2:
            raw_price, raw_size = row[0], row[1]
        else:
            raise ValueError(f"{side} 价位结构非法")
        price = _decimal_text(_decimal(raw_price, f"{side}.price"))
        size = _decimal_text(_decimal(raw_size, f"{side}.size"))
        if price in prices:
            raise ValueError(f"{side} 存在重复价格档")
        prices.add(price)
        output.append(AnchorLevel(side, index, price, size))
    return tuple(output)


def _canonical_levels(
    levels: Sequence[AnchorLevel],
) -> tuple[tuple[str, str, str], ...]:
    asks = sorted(
        (level for level in levels if level.side == "ask"),
        key=lambda item: Decimal(item.price),
    )
    bids = sorted(
        (level for level in levels if level.side == "bid"),
        key=lambda item: Decimal(item.price), reverse=True,
    )
    return tuple(
        (level.side, level.price, level.size) for level in (*asks, *bids)
    )


def _book_summary(
    levels: Sequence[AnchorLevel],
) -> tuple[str, str, int, int, str, str, str]:
    canonical = _canonical_levels(levels)
    asks = [row for row in canonical if row[0] == "ask"]
    bids = [row for row in canonical if row[0] == "bid"]
    if not asks or not bids:
        raise ValueError("REST 盘口必须同时含买卖两侧")
    ask_depth = sum((Decimal(row[2]) for row in asks), Decimal(0))
    bid_depth = sum((Decimal(row[2]) for row in bids), Decimal(0))
    payload = json.dumps(
        canonical, ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")
    return (
        bids[0][1], asks[0][1], len(bids), len(asks),
        _decimal_text(bid_depth), _decimal_text(ask_depth),
        hashlib.sha256(payload).hexdigest(),
    )


def _success_payload(fetch: AnchorFetch) -> Mapping[str, object]:
    if fetch.error_kind is not None or fetch.http_status != 200:
        raise ValueError(fetch.error_detail or "REST 锚点不可用")
    body = fetch.response_body
    if body is None:
        raise ValueError("HTTP 200 缺少响应字节")
    loaded = json.loads(body, parse_float=Decimal, parse_int=Decimal)
    if not isinstance(loaded, Mapping):
        raise ValueError("REST 盘口响应不是对象")
    return loaded


def _parse_available(
    fetch: AnchorFetch, ingest_time: str,
) -> ParsedAnchor:
    payload = _success_payload(fetch)
    venue = fetch.endpoint.venue_id
    sequence: str | None = None
    if venue == "gmo":
        if payload.get("status") != 0:
            raise ValueError("GMO REST status 非零")
        data = payload.get("data")
        if not isinstance(data, Mapping):
            raise ValueError("GMO REST data 缺失")
        if str(data.get("symbol", "")) != fetch.venue_symbol:
            raise ValueError("GMO REST symbol 不一致")
        event_time = _iso(payload.get("responsetime"), "responsetime")
        time_origin = "venue_response"
    elif venue == "bitbank":
        if payload.get("success") != 1:
            raise ValueError("bitbank REST success 非一")
        data = payload.get("data")
        if not isinstance(data, Mapping):
            raise ValueError("bitbank REST data 缺失")
        event_time = _millis(data.get("timestamp"), "timestamp")
        raw_sequence = data.get("sequenceId")
        if raw_sequence is None or isinstance(raw_sequence, bool):
            raise ValueError("bitbank REST sequenceId 缺失")
        sequence_number = int(str(raw_sequence))
        if sequence_number < 0:
            raise ValueError("bitbank REST sequenceId 非法")
        sequence = str(sequence_number)
        time_origin = "venue"
    elif venue == "bitflyer":
        data = payload
        event_time = _iso(
            fetch.response_received_at, "response_received_at"
        )
        time_origin = "receive_proxy"
    else:
        raise ValueError(f"未知 REST 锚点来源: {venue}")
    asks = _levels(data.get("asks"), "ask")
    bids = _levels(data.get("bids"), "bid")
    levels = (*asks, *bids)
    (
        best_bid, best_ask, bid_levels, ask_levels,
        bid_depth, ask_depth, book_hash,
    ) = _book_summary(levels)
    available, basis, offset_ms = _available_time(
        event_time, fetch.response_received_at, ingest_time
    )
    return ParsedAnchor(
        event_time=event_time,
        available_time=available,
        ingest_time=_iso(ingest_time, "ingest_time"),
        time_origin=time_origin,
        receive_source_offset_ms=offset_ms,
        availability_basis=basis,
        sequence_id=sequence,
        levels=tuple(levels),
        best_bid=best_bid,
        best_ask=best_ask,
        bid_levels=bid_levels,
        ask_levels=ask_levels,
        bid_depth=bid_depth,
        ask_depth=ask_depth,
        book_hash=book_hash,
        anchor_availability="available",
        failure_reason=None,
    )


def parse_anchor(fetch: AnchorFetch, ingest_time: str) -> ParsedAnchor:
    """按来源格式规范化；失败形成可审计 unavailable 观察。"""
    try:
        return _parse_available(fetch, ingest_time)
    except (
        InvalidOperation, KeyError, TypeError, UnicodeError, ValueError,
        json.JSONDecodeError,
    ) as exc:
        received = _iso(
            fetch.response_received_at, "response_received_at"
        )
        ingested = _iso(ingest_time, "ingest_time")
        available, basis, _ = _available_time(
            received, received, ingested
        )
        return ParsedAnchor(
            event_time=received,
            available_time=available,
            ingest_time=ingested,
            time_origin="receive_proxy",
            receive_source_offset_ms=None,
            availability_basis=basis,
            sequence_id=None,
            levels=(),
            best_bid=None,
            best_ask=None,
            bid_levels=None,
            ask_levels=None,
            bid_depth=None,
            ask_depth=None,
            book_hash=None,
            anchor_availability="unavailable",
            failure_reason=f"{type(exc).__name__}: {exc}"[:1000],
        )


def _safe_path(value: str, field: str) -> str:
    if _SAFE_PATH_VALUE.fullmatch(value) is None:
        raise ValueError(f"{field} 不是安全目录值")
    return value


def _persist_raw(
    root: Path,
    fetch: AnchorFetch,
    trigger_reason: str,
    connection_id: str | None,
    ingest_time: str,
    request_id: str,
) -> tuple[Path, str, str]:
    response_body = fetch.response_body
    record = {
        "schema_version": RAW_SCHEMA_VERSION,
        "domain": ANCHOR_DOMAIN,
        "request_id": request_id,
        "venue_id": fetch.endpoint.venue_id,
        "venue_symbol": fetch.venue_symbol,
        "endpoint_id": fetch.endpoint.endpoint_id,
        "endpoint_key": fetch.endpoint.endpoint_key,
        "endpoint_revision": fetch.endpoint.endpoint_revision,
        "documentation_uri": fetch.endpoint.documentation_uri,
        "method": "GET",
        "request_url": fetch.request_url,
        "request_sha256": fetch.request_sha256,
        "requested_at": fetch.requested_at,
        "trigger_reason": trigger_reason,
        "connection_id": connection_id,
        "http_status": fetch.http_status,
        "response_received_at": fetch.response_received_at,
        "response_sha256": fetch.response_sha256,
        "response_body_base64": (
            None if response_body is None
            else base64.b64encode(response_body).decode("ascii")
        ),
        "network_error_kind": fetch.error_kind,
        "network_error_detail": fetch.error_detail,
        "ingest_time": ingest_time,
    }
    encoded = (
        json.dumps(
            record, ensure_ascii=False, separators=(",", ":"), sort_keys=True
        ) + "\n"
    ).encode("utf-8")
    sha = hashlib.sha256(encoded).hexdigest()
    day = _aware(ingest_time, "ingest_time").strftime("%Y-%m-%d")
    path = (
        root / "raw" / "rest" / ANCHOR_DOMAIN
        / f"schema_version={RAW_SCHEMA_VERSION}"
        / f"venue_id={_safe_path(fetch.endpoint.venue_id, 'venue_id')}"
        / f"venue_symbol={_safe_path(fetch.venue_symbol, 'venue_symbol')}"
        / f"day={day}" / f"sha256-{sha}.json"
    )
    if path.exists():
        if path.read_bytes() != encoded:
            raise ValueError("REST 锚点 raw 内容寻址冲突")
    else:
        atomic_write_bytes(path, encoded)
    return path, sha, artifact_id(sha)


def _raw_string(
    record: Mapping[str, object], field: str, *, optional: bool = False,
) -> str | None:
    """读取 raw 字符串并拒绝隐式类型转换。"""
    value = record[field]
    if value is None and optional:
        return None
    if not isinstance(value, str) or not value:
        raise ValueError(f"REST 锚点 raw {field} 必须为非空字符串")
    return value


def _raw_fetch(
    root: Path, raw_path: Path,
) -> tuple[AnchorFetch, str, str, str | None, str, str]:
    """校验 raw 原件并重建原始 GET 结果。"""
    resolved_root = root.resolve()
    resolved_path = raw_path.resolve(strict=True)
    encoded = resolved_path.read_bytes()
    raw_sha = hashlib.sha256(encoded).hexdigest()
    if resolved_path.name != f"sha256-{raw_sha}.json":
        raise ValueError("REST 锚点 raw 文件名散列不匹配")
    try:
        loaded = json.loads(encoded)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("REST 锚点 raw 不是有效 UTF-8 JSON") from exc
    if not isinstance(loaded, Mapping):
        raise ValueError("REST 锚点 raw 根节点不是对象")
    record = dict(loaded)
    if set(record) != _RAW_RECORD_FIELDS:
        raise ValueError("REST 锚点 raw schema 字段不匹配")
    canonical = (
        json.dumps(
            record, ensure_ascii=False, separators=(",", ":"), sort_keys=True,
        ) + "\n"
    ).encode("utf-8")
    if canonical != encoded:
        raise ValueError("REST 锚点 raw 不是规范序列化字节")
    if type(record["schema_version"]) is not int or (
        record["schema_version"] != RAW_SCHEMA_VERSION
    ):
        raise ValueError("REST 锚点 raw schema_version 不支持")
    if record["domain"] != ANCHOR_DOMAIN or record["method"] != "GET":
        raise ValueError("REST 锚点 raw 域或方法不匹配")
    venue_id = _raw_string(record, "venue_id")
    venue_symbol = _raw_string(record, "venue_symbol")
    assert venue_id is not None and venue_symbol is not None
    try:
        endpoint = ANCHOR_ENDPOINTS[venue_id]
    except KeyError as exc:
        raise ValueError(f"REST 锚点 raw 来源不支持: {venue_id}") from exc
    endpoint_revision = record["endpoint_revision"]
    if type(endpoint_revision) is not int:
        raise ValueError("REST 锚点 raw endpoint_revision 不是整数")
    endpoint_identity = (
        record["endpoint_id"], record["endpoint_key"], endpoint_revision,
        record["documentation_uri"],
    )
    expected_endpoint = (
        endpoint.endpoint_id, endpoint.endpoint_key, endpoint.endpoint_revision,
        endpoint.documentation_uri,
    )
    if endpoint_identity != expected_endpoint:
        raise ValueError("REST 锚点 raw endpoint 身份不匹配")
    request_url = _raw_string(record, "request_url")
    request_sha = _raw_string(record, "request_sha256")
    assert request_url is not None and request_sha is not None
    if request_url != _anchor_request_url(endpoint, venue_symbol):
        raise ValueError("REST 锚点 raw 请求 URL 与端点不匹配")
    expected_request_sha = hashlib.sha256(
        f"GET\n{request_url}\n".encode("utf-8")
    ).hexdigest()
    if request_sha != expected_request_sha:
        raise ValueError("REST 锚点 raw 请求散列不匹配")
    body_text = _raw_string(record, "response_body_base64", optional=True)
    try:
        response_body = (
            None if body_text is None
            else base64.b64decode(body_text, validate=True)
        )
    except (UnicodeEncodeError, ValueError) as exc:
        raise ValueError("REST 锚点 raw 响应不是有效 Base64") from exc
    response_sha = _raw_string(record, "response_sha256", optional=True)
    expected_response_sha = (
        None if response_body is None
        else hashlib.sha256(response_body).hexdigest()
    )
    if response_sha != expected_response_sha:
        raise ValueError("REST 锚点 raw 响应散列不匹配")
    if body_text is not None:
        assert response_body is not None
        if base64.b64encode(response_body).decode("ascii") != body_text:
            raise ValueError("REST 锚点 raw 响应 Base64 不是规范形式")
    requested_at = _raw_string(record, "requested_at")
    received_at = _raw_string(record, "response_received_at")
    ingest_time = _raw_string(record, "ingest_time")
    request_id = _raw_string(record, "request_id")
    trigger_reason = _raw_string(record, "trigger_reason")
    connection_id = _raw_string(record, "connection_id", optional=True)
    assert requested_at is not None and received_at is not None
    assert ingest_time is not None and request_id is not None
    assert trigger_reason is not None
    _aware(requested_at, "requested_at")
    _aware(received_at, "response_received_at")
    _aware(ingest_time, "ingest_time")
    if trigger_reason not in _TRIGGER_REASONS:
        raise ValueError("REST 锚点 raw 触发原因不支持")
    if trigger_reason != "periodic" and connection_id is None:
        raise ValueError("REST 锚点 raw 连接触发缺少 connection_id")
    http_status = record["http_status"]
    if http_status is not None and (
        type(http_status) is not int or not 100 <= http_status <= 599
    ):
        raise ValueError("REST 锚点 raw HTTP 状态非法")
    error_kind = _raw_string(record, "network_error_kind", optional=True)
    error_detail = _raw_string(record, "network_error_detail", optional=True)
    if error_kind is None and (
        http_status != 200 or response_body is None or error_detail is not None
    ):
        raise ValueError("REST 锚点 raw 成功状态字段不一致")
    day = _aware(ingest_time, "ingest_time").strftime("%Y-%m-%d")
    expected_path = (
        resolved_root / "raw" / "rest" / ANCHOR_DOMAIN
        / f"schema_version={RAW_SCHEMA_VERSION}"
        / f"venue_id={_safe_path(venue_id, 'venue_id')}"
        / f"venue_symbol={_safe_path(venue_symbol, 'venue_symbol')}"
        / f"day={day}" / f"sha256-{raw_sha}.json"
    ).resolve()
    if resolved_path != expected_path:
        raise ValueError("REST 锚点 raw 分区路径不匹配")
    fetch = AnchorFetch(
        endpoint, venue_symbol, request_url, request_sha, requested_at,
        received_at, http_status, response_body, error_kind, error_detail,
    )
    return (
        fetch, request_id, trigger_reason, connection_id, ingest_time, raw_sha,
    )


def _raw_registry_state(
    root: Path, raw_artifact_id: str,
) -> tuple[bool, tuple[str, ...], tuple[tuple[str, str, str], ...]]:
    """只读检查 raw 制品、attempt 与活动头登记。"""
    database = root.resolve() / "guvolu.sqlite3"
    if not database.is_file():
        return False, (), ()
    conn = sqlite3.connect(f"file:{database.as_posix()}?mode=ro", uri=True)
    try:
        artifact_registered = conn.execute(
            "SELECT 1 FROM artifact WHERE artifact_id=?", (raw_artifact_id,),
        ).fetchone() is not None
        attempts = tuple(str(row[0]) for row in conn.execute(
            "SELECT p.attempt_id FROM partition_input i "
            "JOIN partition_attempt p ON p.attempt_id=i.attempt_id "
            "WHERE i.artifact_id=? ORDER BY p.attempt_id",
            (raw_artifact_id,),
        ))
        if not attempts:
            return artifact_registered, (), ()
        placeholders = ",".join("?" for _ in attempts)
        heads = tuple(
            (str(row[0]), str(row[1]), str(row[2]))
            for row in conn.execute(
                "SELECT market_id,domain,attempt_id "
                "FROM materialization_partition_head "
                f"WHERE attempt_id IN ({placeholders}) "
                "ORDER BY market_id,domain,attempt_id",
                attempts,
            )
        )
        return artifact_registered, attempts, heads
    finally:
        conn.close()


def _completed_recovery(
    root: Path, raw_artifact_id: str,
) -> tuple[str, str] | None:
    """识别已完整恢复的事实闭环。"""
    database = root.resolve() / "guvolu.sqlite3"
    if not database.is_file():
        return None
    conn = sqlite3.connect(f"file:{database.as_posix()}?mode=ro", uri=True)
    try:
        anchors = conn.execute(
            "SELECT p.attempt_id,p.status FROM partition_input i "
            "JOIN partition_attempt p ON p.attempt_id=i.attempt_id "
            "WHERE i.artifact_id=? AND p.domain=? ORDER BY p.attempt_id",
            (raw_artifact_id, ANCHOR_DOMAIN),
        ).fetchall()
        if not anchors:
            return None
        if len(anchors) != 1 or str(anchors[0][1]) != "complete":
            raise ValueError("REST 锚点 raw 已绑定不完整旧 attempt")
        anchor_attempt = str(anchors[0][0])
        outputs = {
            str(row[0]) for row in conn.execute(
                "SELECT dataset FROM materialization_output WHERE attempt_id=?",
                (anchor_attempt,),
            )
        }
        if outputs != {ANCHOR_OBSERVATION_DATASET, ANCHOR_LEVEL_DATASET}:
            raise ValueError("REST 锚点 raw 的事实输出不完整")
        reconciliations = conn.execute(
            "SELECT p.attempt_id,p.status FROM materialization_dependency d "
            "JOIN partition_attempt p ON p.attempt_id=d.attempt_id "
            "JOIN materialization_output o ON o.attempt_id=p.attempt_id "
            "WHERE d.upstream_attempt_id=? AND p.domain=? "
            "AND o.dataset=? ORDER BY p.attempt_id",
            (
                anchor_attempt, RECONCILIATION_DOMAIN,
                ANCHOR_RECONCILIATION_DATASET,
            ),
        ).fetchall()
        if len(reconciliations) != 1 or str(reconciliations[0][1]) != "complete":
            raise ValueError("REST 锚点 raw 的对照 attempt 不完整")
        observation_storage = conn.execute(
            "SELECT a.storage_path FROM materialization_output o "
            "JOIN artifact a ON a.artifact_id=o.artifact_id "
            "WHERE o.attempt_id=? AND o.dataset=?",
            (anchor_attempt, ANCHOR_OBSERVATION_DATASET),
        ).fetchone()
        if observation_storage is None:
            raise ValueError("REST 锚点 raw 的观察制品缺失")
        manifest_path = (
            root.resolve() / str(observation_storage[0])
        ).parent / f"manifest-{anchor_attempt}.json"
        if not manifest_path.is_file():
            raise ValueError("REST 锚点 raw 的 manifest 文件缺失")
        manifest_sha = sha256_file(manifest_path)
        manifest_registered = conn.execute(
            "SELECT 1 FROM artifact WHERE artifact_id=? "
            "AND artifact_kind='materialization_manifest' AND sha256=?",
            (artifact_id(manifest_sha), manifest_sha),
        ).fetchone()
        if manifest_registered is None:
            raise ValueError("REST 锚点 raw 的 manifest 未登记")
        return anchor_attempt, str(reconciliations[0][0])
    finally:
        conn.close()


def recover_raw_anchor(
    root: Path, raw_path: Path, *, check_only: bool = False,
) -> RawAnchorRecoveryResult:
    """从一份不可变 raw 原件幂等恢复完整事实闭环。"""
    root = root.resolve()
    fetch, request_id, trigger_reason, connection_id, ingest_time, raw_sha = (
        _raw_fetch(root, raw_path)
    )
    raw_artifact_id = artifact_id(raw_sha)
    raw_storage = _relative_storage_path(root, raw_path.resolve())
    lock_path = root / ".locks" / f"recover-{raw_artifact_id}.lock"
    with exclusive_path_lock(lock_path):
        registered, attempts, heads = _raw_registry_state(
            root, raw_artifact_id
        )
        completed = _completed_recovery(root, raw_artifact_id)
        if completed is not None:
            return RawAnchorRecoveryResult(
                raw_storage, raw_artifact_id, fetch.endpoint.venue_id,
                "already_recovered", registered, attempts, heads,
                completed[0], completed[1], None,
            )
        if check_only:
            return RawAnchorRecoveryResult(
                raw_storage, raw_artifact_id, fetch.endpoint.venue_id,
                "pending", registered, attempts, heads, None, None, None,
            )
        materialization = persist_anchor_fetch(
            root, fetch, trigger_reason=trigger_reason,
            connection_id=connection_id, request_id=request_id,
            ingest_time=ingest_time, recovery_mode=True,
        )
        return RawAnchorRecoveryResult(
            raw_storage, raw_artifact_id, fetch.endpoint.venue_id,
            "recovered", registered, attempts, heads,
            materialization.anchor_attempt_id,
            materialization.reconciliation_attempt_id, materialization,
        )


def _ws_sequence(
    root: Path,
    conn: sqlite3.Connection,
    source_attempt_id: str,
    frame_id: str,
) -> str | None:
    row = conn.execute(
        "SELECT a.storage_path FROM materialization_output o "
        "JOIN artifact a ON a.artifact_id=o.artifact_id "
        "WHERE o.attempt_id=? AND o.dataset='book_l2_frame' LIMIT 1",
        (source_attempt_id,),
    ).fetchone()
    if row is None:
        return None
    path = root / str(row[0])
    if not path.is_file():
        return None
    db: Any = duckdb.connect(":memory:")
    try:
        values = db.execute(
            "SELECT sequence_id FROM read_parquet(?,union_by_name=true) "
            "WHERE frame_id=? LIMIT 2",
            [str(path), frame_id],
        ).fetchall()
    finally:
        db.close()
    if len(values) != 1 or values[0][0] is None:
        return None
    return str(values[0][0])


def _load_ws_checkpoint(
    root: Path, conn: sqlite3.Connection, market_id: str,
) -> tuple[WsCheckpoint | None, str]:
    row = conn.execute(
        "SELECT h.attempt_id,o.artifact_id,a.storage_path "
        "FROM materialization_partition_head h "
        "JOIN partition_attempt p ON p.attempt_id=h.attempt_id "
        "JOIN materialization_output o ON o.attempt_id=h.attempt_id "
        "JOIN artifact a ON a.artifact_id=o.artifact_id "
        "WHERE h.market_id=? AND h.domain='book_state' "
        "AND h.partition_key='latest' AND p.status='complete' "
        "AND o.dataset='book_state_checkpoint' LIMIT 1",
        (market_id,),
    ).fetchone()
    if row is None:
        return None, "没有活动 WS book-state checkpoint"
    path = root / str(row[2])
    if not path.is_file():
        return None, "WS book-state checkpoint 文件缺失"
    db: Any = duckdb.connect(":memory:")
    db.execute("SET TimeZone='UTC'")
    try:
        rows = db.execute(
            "SELECT source_attempt_id,as_of_frame_id,event_time,available_time,"
            "snapshot_frame_id,integrity_mode,side,price,size "
            "FROM read_parquet(?) ORDER BY side,CAST(price AS DECIMAL(38,12))",
            [str(path)],
        ).fetchall()
    finally:
        db.close()
    if not rows:
        return None, "WS book-state checkpoint 为空"
    identity = tuple(rows[0][:6])
    if any(tuple(item[:6]) != identity for item in rows):
        return None, "WS book-state checkpoint 混合身份"
    if identity[4] is None:
        return None, "WS book-state checkpoint 无 snapshot 锚点"
    if str(identity[5]) not in _TRUSTED_WS_INTEGRITY:
        return None, "WS book-state checkpoint 完整性模式不可信"
    try:
        levels = tuple(
            AnchorLevel(
                str(item[6]), index,
                _decimal_text(_decimal(item[7], "ws.price")),
                _decimal_text(_decimal(item[8], "ws.size")),
            )
            for index, item in enumerate(rows)
        )
    except (InvalidOperation, ValueError) as exc:
        return None, f"WS book-state checkpoint 档位非法: {exc}"
    try:
        (
            best_bid, best_ask, bid_levels, ask_levels,
            bid_depth, ask_depth, book_hash,
        ) = _book_summary(levels)
    except (InvalidOperation, ValueError) as exc:
        return None, f"WS book-state checkpoint 档位非法: {exc}"
    source_attempt_id = str(identity[0])
    frame_id = str(identity[1])
    sequence = _ws_sequence(root, conn, source_attempt_id, frame_id)
    return WsCheckpoint(
        attempt_id=str(row[0]),
        artifact_id=str(row[1]),
        as_of_frame_id=frame_id,
        event_time=_iso(identity[2], "ws_event_time"),
        available_time=_iso(identity[3], "ws_available_time"),
        sequence_id=sequence,
        best_bid=best_bid,
        best_ask=best_ask,
        bid_levels=bid_levels,
        ask_levels=ask_levels,
        bid_depth=bid_depth,
        ask_depth=ask_depth,
        book_hash=book_hash,
    ), "ok"


def compare_anchor(
    venue_id: str,
    anchor: ParsedAnchor,
    checkpoint: WsCheckpoint | None,
    unavailable_reason: str,
) -> AnchorComparison:
    """只裁决可比较样本；时点不明时保持 unknown。"""
    if anchor.anchor_availability != "available":
        return AnchorComparison(
            "unknown", "rest_unavailable",
            anchor.failure_reason or "REST 锚点不可用",
            None, None, None, None, None, None, False,
        )
    if checkpoint is None:
        return AnchorComparison(
            "unknown", "checkpoint_unavailable", unavailable_reason,
            None, None, None, None, None, None, False,
        )
    lag = (
        _aware(anchor.available_time, "anchor.available_time")
        - _aware(checkpoint.available_time, "ws.available_time")
    ).total_seconds() * 1000
    best_bid = anchor.best_bid == checkpoint.best_bid
    best_ask = anchor.best_ask == checkpoint.best_ask
    depth = (
        anchor.bid_levels == checkpoint.bid_levels
        and anchor.ask_levels == checkpoint.ask_levels
        and anchor.bid_depth == checkpoint.bid_depth
        and anchor.ask_depth == checkpoint.ask_depth
    )
    book_hash = anchor.book_hash == checkpoint.book_hash
    same_sequence = (
        venue_id == "bitbank"
        and anchor.sequence_id is not None
        and checkpoint.sequence_id is not None
        and anchor.sequence_id == checkpoint.sequence_id
    )
    same_depth_scope = (
        anchor.bid_levels == checkpoint.bid_levels
        and anchor.ask_levels == checkpoint.ask_levels
    )
    recent = 0 <= lag <= MAX_WS_CHECKPOINT_AGE_SECONDS * 1000
    if same_sequence and not same_depth_scope:
        return AnchorComparison(
            "unknown", "bitbank_equal_sequence_depth_scope_mismatch",
            "同序样本的发布深度范围不同，禁止裁决全簿一致性",
            checkpoint, lag, best_bid, best_ask, depth, book_hash, False,
        )
    if same_sequence:
        status = (
            "match" if all((best_bid, best_ask, depth, book_hash))
            else "mismatch"
        )
        reason = (
            "同序且同深度范围的最优档、深度与全簿散列一致"
            if status == "match"
            else "同序且同深度范围的盘口字段或全簿散列不一致"
        )
        return AnchorComparison(
            status, "bitbank_equal_sequence", reason, checkpoint, lag,
            best_bid, best_ask, depth, book_hash, True,
        )
    if recent:
        return AnchorComparison(
            "unknown", "approximate_recent_prior_ws_checkpoint",
            "近邻样本没有相同来源时点身份，只保留字段差异诊断",
            checkpoint, lag, best_bid, best_ask, depth, book_hash, False,
        )
    else:
        return AnchorComparison(
            "unknown", "temporal_identity_unbound",
            "REST 与 WS 没有同序或足够近的先验时点绑定",
            checkpoint, lag, best_bid, best_ask, depth, book_hash, False,
        )


def _write_parquet(
    db: Any,
    table: str,
    temp: Path,
    order_by: str,
) -> tuple[Path, str]:
    temp.parent.mkdir(parents=True, exist_ok=True)
    escaped = temp.as_posix().replace("'", "''")
    db.execute(
        f"COPY (SELECT * FROM {table} ORDER BY {order_by}) "
        f"TO '{escaped}' (FORMAT PARQUET,COMPRESSION ZSTD)"
    )
    with temp.open("rb+") as handle:
        handle.flush()
        os.fsync(handle.fileno())
    sha = sha256_file(temp)
    final = temp.with_name(f"part-{sha[:12]}.parquet")
    if final.exists():
        if sha256_file(final) != sha:
            raise ValueError("REST 锚点 Parquet 内容寻址冲突")
        temp.unlink()
    else:
        os.replace(temp, final)
    return final, sha


def _table_insert(db: Any, table: str, columns: Sequence[str], row: tuple[object, ...]) -> None:
    if len(columns) != len(row):
        raise ValueError(f"{table} 行列数不一致")
    placeholders = ",".join("?" for _ in columns)
    db.execute(f"INSERT INTO {table} VALUES ({placeholders})", row)


def _input_hash(artifact_identity: str) -> str:
    return hashlib.sha256(
        json.dumps([artifact_identity], separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _capability_revision(
    conn: sqlite3.Connection, fetch: AnchorFetch,
) -> int:
    """解析该请求实际使用的已实现能力修订。"""
    endpoint = _CAPABILITY_ENDPOINTS[fetch.endpoint.venue_id]
    row = conn.execute(
        "SELECT revision_id FROM venue_capability_revision "
        "WHERE venue_id=? AND domain=? AND endpoint=? AND available=1 "
        "AND implementation_status='implemented' "
        "ORDER BY revision_id DESC LIMIT 1",
        (fetch.endpoint.venue_id, ANCHOR_DOMAIN, endpoint),
    ).fetchone()
    if row is None:
        raise ValueError(
            "REST L2 锚点能力尚未登记为 implemented: "
            f"{fetch.endpoint.venue_id}/{endpoint}"
        )
    return int(row[0])


def _bind_capability(
    conn: sqlite3.Connection,
    attempt_id: str,
    fetch: AnchorFetch,
    capability_revision: int,
    bound_at: str,
) -> None:
    """把事实尝试绑定到记录时使用的能力证据。"""
    endpoint = _CAPABILITY_ENDPOINTS[fetch.endpoint.venue_id]
    conn.execute(
        "INSERT OR IGNORE INTO partition_capability_binding "
        "(attempt_id,venue_id,domain,endpoint,revision_id,binding_basis,"
        "bound_at) VALUES (?,?,?,?,?,'recorded',?)",
        (
            attempt_id, fetch.endpoint.venue_id, ANCHOR_DOMAIN, endpoint,
            capability_revision, bound_at,
        ),
    )
    stored = conn.execute(
        "SELECT revision_id,binding_basis FROM partition_capability_binding "
        "WHERE attempt_id=? AND venue_id=? AND domain=? AND endpoint=?",
        (attempt_id, fetch.endpoint.venue_id, ANCHOR_DOMAIN, endpoint),
    ).fetchone()
    if stored is None or tuple(stored) != (capability_revision, "recorded"):
        raise ValueError("REST L2 锚点 capability binding 身份冲突")


def _register_attempts(
    root: Path,
    conn: sqlite3.Connection,
    *,
    market_id: str,
    fetch: AnchorFetch,
    parsed: ParsedAnchor,
    comparison: AnchorComparison,
    trigger_reason: str,
    connection_id: str | None,
    observation_id: str,
    raw_path: Path,
    raw_sha: str,
    raw_artifact_id: str,
    observation_path: Path,
    observation_sha: str,
    level_path: Path,
    level_sha: str,
    reconciliation_path: Path,
    reconciliation_sha: str,
    manifest_path: Path,
    anchor_attempt_id: str,
    reconciliation_attempt_id: str,
) -> AnchorMaterializationResult:
    created_at = utc_now()
    capability_revision = _capability_revision(conn, fetch)
    raw_storage = _relative_storage_path(root, raw_path)
    observation_storage = _relative_storage_path(root, observation_path)
    observation_artifact_id = artifact_id(observation_sha)
    level_artifact_id = artifact_id(level_sha)
    reconciliation_artifact_id = artifact_id(reconciliation_sha)
    manifest_sha = sha256_file(manifest_path)
    with sqlite_writer_lock(root):
        conn.execute("BEGIN IMMEDIATE")
        try:
            current_status = conn.execute(
                "SELECT available_time FROM l2_anchor_status WHERE market_id=?",
                (market_id,),
            ).fetchone()
            advance_heads = (
                current_status is None
                or _aware(parsed.available_time, "anchor.available_time")
                > _aware(current_status[0], "current_anchor.available_time")
            )
            for identity, kind, path, sha, version in (
                (
                    raw_artifact_id, "raw_rest_l2_anchor", raw_path,
                    raw_sha, RAW_SCHEMA_VERSION,
                ),
                (
                    observation_artifact_id, "materialized_parquet",
                    observation_path, observation_sha, ANCHOR_SCHEMA_VERSION,
                ),
                (
                    level_artifact_id, "materialized_parquet", level_path,
                    level_sha, ANCHOR_SCHEMA_VERSION,
                ),
                (
                    reconciliation_artifact_id, "materialized_parquet",
                    reconciliation_path, reconciliation_sha,
                    ANCHOR_SCHEMA_VERSION,
                ),
                (
                    artifact_id(manifest_sha), "materialization_manifest",
                    manifest_path, manifest_sha, ANCHOR_SCHEMA_VERSION,
                ),
            ):
                _register_content_artifact(
                    conn, identity, kind, _relative_storage_path(root, path),
                    sha, path.stat().st_size, created_at, version,
                )
            norm_config = hashlib.sha256(json.dumps({
                "normalization_version": ANCHOR_NORMALIZATION_VERSION,
                "endpoint_id": fetch.endpoint.endpoint_id,
                "endpoint_key": fetch.endpoint.endpoint_key,
                "endpoint_revision": fetch.endpoint.endpoint_revision,
                "ws_fact_mutation": False,
            }, sort_keys=True).encode()).hexdigest()
            conn.execute(
                "INSERT OR IGNORE INTO partition_attempt "
                "(attempt_id,market_id,domain,partition_key,"
                "normalization_version,input_set_hash,status,source_rows,"
                "normalized_rows,ignored_rows,rejected_rows,started_at,"
                "finished_at,code_version,config_hash) "
                "VALUES (?,?,?,?,?,?,'complete',1,1,0,0,?,?,"
                "'working-tree',?)",
                (
                    anchor_attempt_id, market_id, ANCHOR_DOMAIN,
                    LATEST_PARTITION, ANCHOR_NORMALIZATION_VERSION,
                    _input_hash(raw_artifact_id), parsed.ingest_time,
                    created_at, norm_config,
                ),
            )
            anchor_identity = conn.execute(
                "SELECT market_id,domain,partition_key,normalization_version,"
                "input_set_hash,status FROM partition_attempt "
                "WHERE attempt_id=?",
                (anchor_attempt_id,),
            ).fetchone()
            expected_anchor = (
                market_id, ANCHOR_DOMAIN, LATEST_PARTITION,
                ANCHOR_NORMALIZATION_VERSION,
                _input_hash(raw_artifact_id), "complete",
            )
            if anchor_identity is None or tuple(anchor_identity) != expected_anchor:
                raise ValueError("REST 锚点规范化 attempt 身份冲突")
            _bind_capability(
                conn, anchor_attempt_id, fetch, capability_revision, created_at
            )
            conn.executemany(
                "INSERT OR IGNORE INTO materialization_output "
                "VALUES (?,?,?,?,?,?,?)",
                (
                    (
                        anchor_attempt_id, observation_artifact_id,
                        ANCHOR_OBSERVATION_DATASET, 1, parsed.event_time,
                        parsed.event_time, created_at,
                    ),
                    (
                        anchor_attempt_id, level_artifact_id,
                        ANCHOR_LEVEL_DATASET, len(parsed.levels),
                        parsed.event_time if parsed.levels else None,
                        parsed.event_time if parsed.levels else None,
                        created_at,
                    ),
                ),
            )
            conn.execute(
                "INSERT OR IGNORE INTO partition_input "
                "(attempt_id,artifact_id,source_rows,normalized_rows,"
                "ignored_rows,rejected_rows) VALUES (?,?,?,?,?,?)",
                (anchor_attempt_id, raw_artifact_id, 1, 1, 0, 0),
            )
            conn.execute(
                "INSERT OR IGNORE INTO partition_input_binding "
                "(attempt_id,artifact_id,storage_path,source_rows,"
                "normalized_rows,ignored_rows,rejected_rows) "
                "VALUES (?,?,?,?,?,?,?)",
                (
                    anchor_attempt_id, raw_artifact_id, raw_storage,
                    1, 1, 0, 0,
                ),
            )
            if advance_heads:
                conn.execute(
                    "INSERT INTO materialization_partition_head VALUES "
                    "(?,?,?,?,?,?) ON CONFLICT(market_id,domain,partition_key) "
                    "DO UPDATE SET normalization_version=excluded."
                    "normalization_version,attempt_id=excluded.attempt_id,"
                    "activated_at=excluded.activated_at",
                    (
                        market_id, ANCHOR_DOMAIN, LATEST_PARTITION,
                        ANCHOR_NORMALIZATION_VERSION, anchor_attempt_id,
                        created_at,
                    ),
                )
            reconcile_config = hashlib.sha256(json.dumps({
                "normalization_version": ANCHOR_RECONCILIATION_VERSION,
                "endpoint_id": fetch.endpoint.endpoint_id,
                "endpoint_revision": fetch.endpoint.endpoint_revision,
                "max_ws_checkpoint_age_seconds": (
                    MAX_WS_CHECKPOINT_AGE_SECONDS
                ),
                "bitbank_same_sequence_preferred": True,
                "ws_fact_mutation": False,
            }, sort_keys=True).encode()).hexdigest()
            conn.execute(
                "INSERT OR IGNORE INTO partition_attempt "
                "(attempt_id,market_id,domain,partition_key,"
                "normalization_version,input_set_hash,status,source_rows,"
                "normalized_rows,ignored_rows,rejected_rows,started_at,"
                "finished_at,code_version,config_hash) "
                "VALUES (?,?,?,?,?,?,'complete',1,1,0,0,?,?,"
                "'working-tree',?)",
                (
                    reconciliation_attempt_id, market_id,
                    RECONCILIATION_DOMAIN, LATEST_PARTITION,
                    ANCHOR_RECONCILIATION_VERSION,
                    _input_hash(observation_artifact_id), parsed.ingest_time,
                    created_at, reconcile_config,
                ),
            )
            reconciliation_identity = conn.execute(
                "SELECT market_id,domain,partition_key,normalization_version,"
                "input_set_hash,status FROM partition_attempt "
                "WHERE attempt_id=?",
                (reconciliation_attempt_id,),
            ).fetchone()
            expected_reconciliation = (
                market_id, RECONCILIATION_DOMAIN, LATEST_PARTITION,
                ANCHOR_RECONCILIATION_VERSION,
                _input_hash(observation_artifact_id), "complete",
            )
            if (
                reconciliation_identity is None
                or tuple(reconciliation_identity) != expected_reconciliation
            ):
                raise ValueError("REST 锚点对照 attempt 身份冲突")
            _bind_capability(
                conn, reconciliation_attempt_id, fetch,
                capability_revision, created_at,
            )
            conn.execute(
                "INSERT OR IGNORE INTO materialization_output "
                "VALUES (?,?,?,?,?,?,?)",
                (
                    reconciliation_attempt_id, reconciliation_artifact_id,
                    ANCHOR_RECONCILIATION_DATASET, 1, parsed.event_time,
                    parsed.event_time, created_at,
                ),
            )
            conn.execute(
                "INSERT OR IGNORE INTO partition_input "
                "(attempt_id,artifact_id,source_rows,normalized_rows,"
                "ignored_rows,rejected_rows) VALUES (?,?,?,?,?,?)",
                (
                    reconciliation_attempt_id, observation_artifact_id,
                    1, 1, 0, 0,
                ),
            )
            conn.execute(
                "INSERT OR IGNORE INTO partition_input_binding "
                "(attempt_id,artifact_id,storage_path,source_rows,"
                "normalized_rows,ignored_rows,rejected_rows) "
                "VALUES (?,?,?,?,?,?,?)",
                (
                    reconciliation_attempt_id, observation_artifact_id,
                    observation_storage, 1, 1, 0, 0,
                ),
            )
            dependencies = [(
                reconciliation_attempt_id, anchor_attempt_id,
                "explicit-replay", created_at,
            )]
            if comparison.checkpoint is not None:
                dependencies.append((
                    reconciliation_attempt_id,
                    comparison.checkpoint.attempt_id,
                    "explicit-replay", created_at,
                ))
            conn.executemany(
                "INSERT OR IGNORE INTO materialization_dependency "
                "VALUES (?,?,?,?)",
                dependencies,
            )
            if advance_heads:
                conn.execute(
                    "INSERT INTO materialization_partition_head VALUES "
                    "(?,?,?,?,?,?) ON CONFLICT(market_id,domain,partition_key) "
                    "DO UPDATE SET normalization_version=excluded."
                    "normalization_version,attempt_id=excluded.attempt_id,"
                    "activated_at=excluded.activated_at",
                    (
                        market_id, RECONCILIATION_DOMAIN, LATEST_PARTITION,
                        ANCHOR_RECONCILIATION_VERSION,
                        reconciliation_attempt_id, created_at,
                    ),
                )
            if parsed.anchor_availability == "unavailable":
                status = "unavailable"
            elif comparison.status == "match":
                status = "fresh"
            elif comparison.status == "mismatch":
                status = "mismatch"
            else:
                status = "unknown"
            checkpoint_attempt = (
                None if comparison.checkpoint is None
                else comparison.checkpoint.attempt_id
            )
            summary = (
                market_id, observation_id, status, comparison.status,
                trigger_reason, connection_id, fetch.endpoint.endpoint_key,
                fetch.endpoint.endpoint_revision, parsed.event_time,
                parsed.available_time, raw_artifact_id,
                observation_artifact_id, reconciliation_artifact_id,
                anchor_attempt_id, reconciliation_attempt_id,
                checkpoint_attempt, comparison.lag_ms,
                comparison.reason, created_at,
            )
            if advance_heads:
                conn.execute(
                    "INSERT INTO l2_anchor_status VALUES (?,?,?,?,?,?,?,?,?,?,"
                    "?,?,?,?,?,?,?,?,?) ON CONFLICT(market_id) DO UPDATE SET "
                    "observation_id=excluded.observation_id,"
                    "status=excluded.status,"
                    "comparison_status=excluded.comparison_status,"
                    "trigger_reason=excluded.trigger_reason,"
                    "connection_id=excluded.connection_id,"
                    "endpoint_key=excluded.endpoint_key,"
                    "endpoint_revision=excluded.endpoint_revision,"
                    "event_time=excluded.event_time,"
                    "available_time=excluded.available_time,"
                    "source_artifact_id=excluded.source_artifact_id,"
                    "observation_artifact_id=excluded.observation_artifact_id,"
                    "reconciliation_artifact_id="
                    "excluded.reconciliation_artifact_id,"
                    "anchor_attempt_id=excluded.anchor_attempt_id,"
                    "reconciliation_attempt_id="
                    "excluded.reconciliation_attempt_id,"
                    "ws_checkpoint_attempt_id="
                    "excluded.ws_checkpoint_attempt_id,"
                    "comparison_lag_ms=excluded.comparison_lag_ms,"
                    "reason=excluded.reason,updated_at=excluded.updated_at "
                    "WHERE excluded.available_time>"
                    "l2_anchor_status.available_time",
                    summary,
                )
            conn.commit()
        except (
            OSError, sqlite3.Error, TypeError, ValueError,
        ):
            conn.rollback()
            raise
    return AnchorMaterializationResult(
        observation_id=observation_id,
        market_id=market_id,
        status=status,
        comparison_status=comparison.status,
        raw_artifact_id=raw_artifact_id,
        observation_artifact_id=observation_artifact_id,
        level_artifact_id=level_artifact_id,
        reconciliation_artifact_id=reconciliation_artifact_id,
        anchor_attempt_id=anchor_attempt_id,
        reconciliation_attempt_id=reconciliation_attempt_id,
        level_rows=len(parsed.levels),
    )


def persist_anchor_fetch(
    root: Path,
    fetch: AnchorFetch,
    *,
    trigger_reason: str,
    connection_id: str | None,
    request_id: str | None = None,
    ingest_time: str | None = None,
    recovery_mode: bool = False,
) -> AnchorMaterializationResult:
    """把一次成功或失败 GET 完整落为 raw、事实与控制摘要。"""
    if trigger_reason not in _TRIGGER_REASONS:
        raise ValueError(f"未知 REST 锚点触发原因: {trigger_reason}")
    if trigger_reason != "periodic" and not connection_id:
        raise ValueError("连接触发的 REST 锚点必须绑定 connection_id")
    root = root.resolve()
    persisted_at = ingest_time or utc_now()
    unique_request = request_id or hashlib.sha256((
        f"{fetch.requested_at}|{fetch.request_url}|{connection_id}|"
        f"{trigger_reason}"
    ).encode("utf-8")).hexdigest()
    raw_path, raw_sha, raw_artifact_id = _persist_raw(
        root, fetch, trigger_reason, connection_id, persisted_at,
        unique_request,
    )
    parsed = parse_anchor(fetch, persisted_at)
    conn: sqlite3.Connection | None = None
    try:
        # 初始化也会取得 writer 锁。
        # 与登记共享重试预算。
        # 避免复用前提前返回。
        for connect_attempt in range(ANCHOR_REGISTRATION_ATTEMPTS):
            try:
                conn = store.connect(root)
                break
            except TimeoutError:
                if connect_attempt + 1 >= ANCHOR_REGISTRATION_ATTEMPTS:
                    raise
                print(json.dumps({
                    "event": "l2_anchor_registration_retry",
                    "venue_id": fetch.endpoint.venue_id,
                    "venue_symbol": fetch.venue_symbol,
                    "anchor_attempt_id": None,
                    "phase": "connect",
                    "retry": connect_attempt + 1,
                }, ensure_ascii=False), flush=True)
        if conn is None:
            raise AssertionError("REST 锚点连接重试循环未返回")
        registry.register_all(conn)
        ensure_markets(conn)
        endpoint_owner = conn.execute(
            "SELECT venue_id FROM endpoint_revision "
            "WHERE endpoint_id=? AND revision_id=?",
            (
                fetch.endpoint.endpoint_id,
                fetch.endpoint.endpoint_revision,
            ),
        ).fetchone()
        if endpoint_owner != (fetch.endpoint.venue_id,):
            raise ValueError("REST 锚点 endpoint revision 未登记或来源冲突")
        market_id, instrument_id, mapping_revision = _market_row(
            conn, fetch.endpoint.venue_id, fetch.venue_symbol, None
        )
        observation_hash = hashlib.sha256((
            f"{raw_artifact_id}|{market_id}|{trigger_reason}|"
            f"{connection_id or ''}"
        ).encode("utf-8")).hexdigest()
        observation_id = artifact_id(observation_hash)
        try:
            if recovery_mode:
                checkpoint = None
                checkpoint_reason = (
                    "恢复原件没有可证明的 PIT WS checkpoint"
                )
            else:
                checkpoint, checkpoint_reason = _load_ws_checkpoint(
                    root, conn, market_id
                )
        except (OSError, sqlite3.Error, ValueError, duckdb.Error) as exc:
            checkpoint = None
            checkpoint_reason = (
                f"WS checkpoint 读取失败: {type(exc).__name__}: {exc}"
            )[:1000]
        comparison = compare_anchor(
            fetch.endpoint.venue_id, parsed, checkpoint, checkpoint_reason
        )
        if recovery_mode and checkpoint is None and (
            parsed.anchor_availability == "available"
        ):
            comparison = AnchorComparison(
                "unknown", "recovery_no_pit_ws_checkpoint",
                checkpoint_reason, None, None, None, None, None, None, False,
            )
        checkpoint_row = comparison.checkpoint
        observation_row: tuple[object, ...] = (
            observation_id, fetch.endpoint.venue_id, fetch.venue_symbol,
            market_id, mapping_revision, instrument_id,
            fetch.endpoint.endpoint_id,
            fetch.endpoint.endpoint_key, fetch.endpoint.endpoint_revision,
            "GET", fetch.request_url, fetch.request_sha256,
            fetch.response_sha256, fetch.http_status, trigger_reason,
            connection_id, parsed.event_time, parsed.available_time,
            parsed.ingest_time, parsed.time_origin,
            parsed.receive_source_offset_ms, parsed.availability_basis,
            parsed.sequence_id,
            parsed.best_bid, parsed.best_ask, parsed.bid_levels,
            parsed.ask_levels, parsed.bid_depth, parsed.ask_depth,
            parsed.book_hash, parsed.anchor_availability,
            parsed.failure_reason, raw_artifact_id,
            _relative_storage_path(root, raw_path),
            ANCHOR_NORMALIZATION_VERSION, ANCHOR_SCHEMA_VERSION,
        )
        anchor_attempt_id = "book-l2-anchor-" + observation_hash[:32]
        reconciliation_hash = hashlib.sha256((
            f"{observation_id}|"
            f"{checkpoint_row.attempt_id if checkpoint_row else ''}|"
            f"{comparison.status}|{comparison.basis}"
        ).encode("utf-8")).hexdigest()
        reconciliation_id = artifact_id(reconciliation_hash)
        reconciliation_attempt_id = (
            "book-l2-anchor-reconciliation-" + reconciliation_hash[:32]
        )
        day = _aware(parsed.available_time, "available_time").strftime(
            "%Y-%m-%d"
        )
        common_parts = (
            f"schema_version={ANCHOR_SCHEMA_VERSION}",
            f"venue_id={_safe_path(fetch.endpoint.venue_id, 'venue_id')}",
            f"market_id={_safe_path(market_id, 'market_id')}",
            f"day={day}",
        )
        observation_dir = root / "materialized" / ANCHOR_OBSERVATION_DATASET
        level_dir = root / "materialized" / ANCHOR_LEVEL_DATASET
        reconciliation_dir = (
            root / "materialized" / ANCHOR_RECONCILIATION_DATASET
        )
        for part in common_parts:
            observation_dir /= part
            level_dir /= part
            reconciliation_dir /= part
        db: Any = duckdb.connect(":memory:")
        db.execute("SET TimeZone='UTC'")
        try:
            create_anchor_tables(db)
            _table_insert(
                db, ANCHOR_OBSERVATION_DATASET,
                ANCHOR_OBSERVATION_COLUMNS, observation_row,
            )
            if parsed.levels:
                level_rows = [(
                    observation_id, market_id, level.side,
                    level.source_level_index, level.price, level.size,
                    raw_artifact_id, ANCHOR_NORMALIZATION_VERSION,
                    ANCHOR_SCHEMA_VERSION,
                ) for level in parsed.levels]
                placeholders = ",".join("?" for _ in ANCHOR_LEVEL_COLUMNS)
                db.executemany(
                    f"INSERT INTO {ANCHOR_LEVEL_DATASET} VALUES "
                    f"({placeholders})", level_rows,
                )
            observation_path, observation_sha = _write_parquet(
                db, ANCHOR_OBSERVATION_DATASET,
                observation_dir / f".{observation_hash}.tmp.parquet",
                "observation_id",
            )
            level_path, level_sha = _write_parquet(
                db, ANCHOR_LEVEL_DATASET,
                level_dir / f".{observation_hash}.tmp.parquet",
                "side,source_level_index",
            )
            observation_artifact = artifact_id(observation_sha)
            reconciliation_row: tuple[object, ...] = (
                reconciliation_id, observation_id, market_id,
                fetch.endpoint.venue_id, fetch.endpoint.endpoint_id,
                fetch.endpoint.endpoint_revision, parsed.available_time,
                parsed.sequence_id, comparison.status, comparison.basis,
                comparison.reason,
                None if checkpoint_row is None else checkpoint_row.attempt_id,
                None if checkpoint_row is None else checkpoint_row.artifact_id,
                None if checkpoint_row is None else checkpoint_row.as_of_frame_id,
                None if checkpoint_row is None else checkpoint_row.event_time,
                None if checkpoint_row is None else checkpoint_row.available_time,
                None if checkpoint_row is None else checkpoint_row.sequence_id,
                comparison.lag_ms, parsed.best_bid, parsed.best_ask,
                parsed.bid_levels, parsed.ask_levels, parsed.bid_depth,
                parsed.ask_depth, parsed.book_hash,
                None if checkpoint_row is None else checkpoint_row.best_bid,
                None if checkpoint_row is None else checkpoint_row.best_ask,
                None if checkpoint_row is None else checkpoint_row.bid_levels,
                None if checkpoint_row is None else checkpoint_row.ask_levels,
                None if checkpoint_row is None else checkpoint_row.bid_depth,
                None if checkpoint_row is None else checkpoint_row.ask_depth,
                None if checkpoint_row is None else checkpoint_row.book_hash,
                comparison.best_bid_match, comparison.best_ask_match,
                comparison.depth_match, comparison.book_hash_match,
                comparison.full_book_comparable,
                anchor_attempt_id, observation_artifact,
                ANCHOR_RECONCILIATION_VERSION, ANCHOR_SCHEMA_VERSION,
            )
            _table_insert(
                db, ANCHOR_RECONCILIATION_DATASET,
                ANCHOR_RECONCILIATION_COLUMNS, reconciliation_row,
            )
            reconciliation_path, reconciliation_sha = _write_parquet(
                db, ANCHOR_RECONCILIATION_DATASET,
                reconciliation_dir / f".{reconciliation_hash}.tmp.parquet",
                "reconciliation_id",
            )
        finally:
            db.close()
        reconciliation_artifact = artifact_id(reconciliation_sha)
        manifest = {
            "schema_version": ANCHOR_SCHEMA_VERSION,
            "status": "complete",
            "observation_id": observation_id,
            "market_id": market_id,
            "trigger_reason": trigger_reason,
            "connection_id": connection_id,
            "endpoint_id": fetch.endpoint.endpoint_id,
            "endpoint_key": fetch.endpoint.endpoint_key,
            "endpoint_revision": fetch.endpoint.endpoint_revision,
            "raw_artifact_id": raw_artifact_id,
            "anchor_attempt_id": anchor_attempt_id,
            "reconciliation_attempt_id": reconciliation_attempt_id,
            "observation_artifact_id": observation_artifact,
            "level_artifact_id": artifact_id(level_sha),
            "reconciliation_artifact_id": reconciliation_artifact,
            "anchor_availability": parsed.anchor_availability,
            "comparison_status": comparison.status,
            "comparison_basis": comparison.basis,
            "ws_checkpoint_attempt_id": (
                None if checkpoint_row is None else checkpoint_row.attempt_id
            ),
            "ws_fact_mutation": False,
            "historical_gap_fill_claim": False,
        }
        manifest_path = observation_dir / (
            f"manifest-{anchor_attempt_id}.json"
        )
        atomic_write_text(
            manifest_path,
            json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        )
        for registration_attempt in range(ANCHOR_REGISTRATION_ATTEMPTS):
            try:
                return _register_attempts(
                    root, conn, market_id=market_id, fetch=fetch,
                    parsed=parsed, comparison=comparison,
                    trigger_reason=trigger_reason,
                    connection_id=connection_id,
                    observation_id=observation_id,
                    raw_path=raw_path, raw_sha=raw_sha,
                    raw_artifact_id=raw_artifact_id,
                    observation_path=observation_path,
                    observation_sha=observation_sha, level_path=level_path,
                    level_sha=level_sha,
                    reconciliation_path=reconciliation_path,
                    reconciliation_sha=reconciliation_sha,
                    manifest_path=manifest_path,
                    anchor_attempt_id=anchor_attempt_id,
                    reconciliation_attempt_id=reconciliation_attempt_id,
                )
            except TimeoutError:
                if registration_attempt + 1 >= ANCHOR_REGISTRATION_ATTEMPTS:
                    raise
                print(json.dumps({
                    "event": "l2_anchor_registration_retry",
                    "venue_id": fetch.endpoint.venue_id,
                    "venue_symbol": fetch.venue_symbol,
                    "anchor_attempt_id": anchor_attempt_id,
                    "phase": "register",
                    "retry": registration_attempt + 1,
                }, ensure_ascii=False), flush=True)
        raise AssertionError("REST 锚点登记重试循环未返回")
    finally:
        if conn is not None:
            conn.close()


def capture_and_persist_anchor(
    root: Path,
    venue_id: str,
    venue_symbol: str,
    *,
    trigger_reason: str,
    connection_id: str | None,
    adapter: PublicRestAnchorAdapter | None = None,
) -> AnchorMaterializationResult:
    """执行一次公开 GET 并完成独立持久化闭环。"""
    source = adapter if adapter is not None else anchor_adapter(venue_id)
    if source.endpoint.venue_id != venue_id:
        raise ValueError("REST 锚点适配器来源不一致")
    fetch = source.fetch(venue_symbol)
    return persist_anchor_fetch(
        root, fetch, trigger_reason=trigger_reason,
        connection_id=connection_id,
    )


class RestAnchorWorker:
    """串行消费有界队列，HTTP 延迟不阻塞 WS 接收。"""

    def __init__(
        self,
        root: Path,
        venue_id: str,
        venue_symbol: str,
        *,
        on_settled: Callable[[int, int], None] | None = None,
    ) -> None:
        self.root = root
        self.venue_id = venue_id
        self.venue_symbol = venue_symbol
        self._adapter = anchor_adapter(venue_id)
        self._on_settled = on_settled
        self._queue: asyncio.Queue[AnchorJob | None] = asyncio.Queue(
            maxsize=ANCHOR_QUEUE_SIZE
        )
        self._task: asyncio.Task[None] | None = None
        self.enqueued = 0
        self.dropped = 0
        self.completed = 0
        self.failed = 0

    def start(self) -> None:
        """在当前事件循环启动单一后台消费者。"""
        if self._task is not None:
            raise RuntimeError("REST 锚点 worker 已启动")
        self._task = asyncio.create_task(self._run())

    def submit(self, connection_id: str | None, trigger_reason: str) -> bool:
        """非阻塞投递；队列满时返回假并显式计数。"""
        if trigger_reason not in _TRIGGER_REASONS:
            raise ValueError(f"未知 REST 锚点触发原因: {trigger_reason}")
        try:
            self._queue.put_nowait(AnchorJob(connection_id, trigger_reason))
        except asyncio.QueueFull:
            self.dropped += 1
            print(json.dumps({
                "event": "l2_anchor_queue_full",
                "venue_id": self.venue_id,
                "venue_symbol": self.venue_symbol,
                "connection_id": connection_id,
                "trigger_reason": trigger_reason,
            }, ensure_ascii=False), flush=True)
            return False
        self.enqueued += 1
        return True

    def _notify_settled(self) -> None:
        """在事件循环线程发布已入队任务的结算计数。"""
        callback = self._on_settled
        if callback is None:
            return
        try:
            callback(self.completed, self.failed)
        except OSError as exc:
            print(json.dumps({
                "event": "l2_anchor_stats_checkpoint_error",
                "venue_id": self.venue_id,
                "venue_symbol": self.venue_symbol,
                "error": f"{type(exc).__name__}: {exc}",
            }, ensure_ascii=False), flush=True)

    async def _run(self) -> None:
        while True:
            job = await self._queue.get()
            try:
                if job is None:
                    return
                try:
                    result = await asyncio.to_thread(
                        capture_and_persist_anchor,
                        self.root, self.venue_id, self.venue_symbol,
                        trigger_reason=job.trigger_reason,
                        connection_id=job.connection_id,
                        adapter=self._adapter,
                    )
                except (
                    OSError, sqlite3.Error, TypeError, ValueError,
                    duckdb.Error,
                ) as exc:
                    self.failed += 1
                    print(json.dumps({
                        "event": "l2_anchor_persistence_error",
                        "venue_id": self.venue_id,
                        "venue_symbol": self.venue_symbol,
                        "connection_id": job.connection_id,
                        "trigger_reason": job.trigger_reason,
                        "error": f"{type(exc).__name__}: {exc}",
                    }, ensure_ascii=False), flush=True)
                else:
                    self.completed += 1
                    print(json.dumps({
                        "event": "l2_anchor_complete",
                        **asdict(result),
                    }, ensure_ascii=False), flush=True)
                self._notify_settled()
            finally:
                self._queue.task_done()

    async def close(self) -> None:
        """有界等待在途任务，再停止后台消费者。"""
        task = self._task
        if task is None:
            return
        try:
            await asyncio.wait_for(
                self._queue.join(), timeout=ANCHOR_SHUTDOWN_SECONDS
            )
        except TimeoutError:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        else:
            await self._queue.put(None)
            await task
        self._task = None


def main(argv: Sequence[str] | None = None) -> int:
    """运行单次安全样本；缺省写入显式指定的数据根。"""
    arguments = list(sys.argv[1:] if argv is None else argv)
    if arguments and arguments[0] == "recover-raw":
        arguments = arguments[1:]
        recovery_parser = argparse.ArgumentParser(
            description="从不可变 REST L2 raw 原件恢复事实闭环"
        )
        recovery_parser.add_argument("--data-root", type=Path, required=True)
        recovery_parser.add_argument("--raw-path", type=Path, required=True)
        recovery_parser.add_argument("--check-only", action="store_true")
        recovery_args = recovery_parser.parse_args(arguments)
        try:
            recovery_result = recover_raw_anchor(
                recovery_args.data_root.resolve(),
                recovery_args.raw_path.resolve(),
                check_only=bool(recovery_args.check_only),
            )
        except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
            print(json.dumps({
                "outcome": "failed",
                "error": f"{type(exc).__name__}: {exc}",
            }, ensure_ascii=False), file=sys.stderr)
            return 2
        print(json.dumps(asdict(recovery_result), ensure_ascii=False, indent=2))
        return 0
    parser = argparse.ArgumentParser(description="三所 REST L2 独立锚点")
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument(
        "--venue", choices=sorted(("gmo", "bitbank", "bitflyer")),
        required=True,
    )
    parser.add_argument("--symbol", required=True)
    parser.add_argument(
        "--trigger-reason",
        choices=sorted(_TRIGGER_REASONS), default="periodic",
    )
    parser.add_argument("--connection-id")
    args = parser.parse_args(arguments)
    result = capture_and_persist_anchor(
        args.data_root.resolve(), str(args.venue), str(args.symbol),
        trigger_reason=str(args.trigger_reason),
        connection_id=(
            None if args.connection_id is None else str(args.connection_id)
        ),
    )
    print(json.dumps(asdict(result), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
