"""三来源归档到规范化逐笔事实的可续跑投影。"""
from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import sqlite3
import zipfile
from collections import defaultdict
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from guvolu.data import store
from guvolu.data.normalization import (
    NormalizationContext,
    NormalizationError,
    normalize_book_top,
    normalize_trade,
    trade_normalization_version,
)
from guvolu.domain.ids import new_run_id, sha256_hex
from guvolu.venues import archive, registry

DOMAIN_TRADE = "trade"
BATCH_ROWS = 5_000
PROJECTABLE_VENUES = ("gmo", "bitbank", "bitflyer")


@dataclass(frozen=True, slots=True)
class ArchivePartition:
    """一个不可变归档文件对应的事实投影分区。"""

    venue_id: str
    venue_symbol: str
    day: str
    path: Path


@dataclass(slots=True)
class ProjectionStats:
    """一次投影的可审计计数。"""

    partitions_seen: int = 0
    partitions_complete: int = 0
    partitions_skipped: int = 0
    partitions_failed: int = 0
    partitions_quarantined: int = 0
    partitions_empty: int = 0
    unmapped_partitions: int = 0
    raw_rows: int = 0
    normalized_rows: int = 0
    inserted_rows: int = 0
    rejected_rows: int = 0
    errors: list[str] = field(default_factory=list)
    by_venue: dict[str, dict[str, int]] = field(default_factory=dict)

    def add(self, venue_id: str, key: str, value: int = 1) -> None:
        """累计来源维度计数。"""
        self.by_venue.setdefault(venue_id, {})[key] = (
            self.by_venue.setdefault(venue_id, {}).get(key, 0) + value
        )

    def as_dict(self) -> dict[str, object]:
        """转为稳定的 JSON 形态。"""
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ProjectionValidation:
    """投影结果的跨层验证结论。"""

    ready: bool
    trade_rows: dict[str, int]
    partitions: dict[str, int]
    errors: list[str]

    def as_dict(self) -> dict[str, object]:
        """转为稳定的 JSON 形态。"""
        return asdict(self)


@dataclass(slots=True)
class BookProjectionStats:
    """实时与快照盘口顶档投影计数。"""

    frames_seen: int = 0
    inserted_rows: int = 0
    rejected_frames: int = 0
    by_venue: dict[str, int] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, object]:
        """转为稳定的 JSON 形态。"""
        return asdict(self)


@dataclass(slots=True)
class LiveTradeProjectionStats:
    """实时旁路逐笔投影计数。"""

    frames_seen: int = 0
    normalized_rows: int = 0
    inserted_rows: int = 0
    rejected_rows: int = 0
    by_venue: dict[str, int] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, object]:
        """转为稳定的 JSON 形态。"""
        return asdict(self)


def _now_iso() -> str:
    """当前 UTC 时刻。"""
    return datetime.now(UTC).isoformat()


def _day_in_range(day: str, from_day: str | None, to_day: str | None) -> bool:
    """按闭区间筛选日期。"""
    return ((from_day is None or day >= from_day) and
            (to_day is None or day <= to_day))


def _relative_source(data_root: Path, path: Path, line_no: int) -> str:
    """生成跨机器稳定的归档血缘位置。"""
    return f"{path.relative_to(data_root).as_posix()}:{line_no}"


def _sha256_file(path: Path) -> str:
    """以压缩原件字节计算内容散列。"""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1_048_576):
            digest.update(chunk)
    return digest.hexdigest()


def archive_partitions(
    data_root: Path,
    venue_ids: Sequence[str],
    from_day: str | None,
    to_day: str | None,
) -> Iterator[ArchivePartition]:
    """按来源与日期稳定枚举三家逐笔归档。"""
    for venue_id in venue_ids:
        if venue_id == "gmo":
            root = data_root / "archive" / "trades"
            for path in sorted(root.rglob("*.csv.gz")):
                day = path.name[:8]
                if len(day) != 8 or not _day_in_range(day, from_day, to_day):
                    continue
                yield ArchivePartition(venue_id, path.parents[2].name, day, path)
        elif venue_id == "bitbank":
            root = data_root / "archive" / "bitbank" / "trades"
            for path in sorted(root.rglob("*.json.gz")):
                day = path.name[:8]
                if len(day) != 8 or not _day_in_range(day, from_day, to_day):
                    continue
                yield ArchivePartition(venue_id, path.parents[1].name, day, path)
        elif venue_id == "bitflyer":
            root = data_root / "archive" / "bitflyer" / "executions"
            for path in sorted(root.rglob("*.jsonl.gz")):
                day = path.name[:8]
                if len(day) != 8 or not _day_in_range(day, from_day, to_day):
                    continue
                yield ArchivePartition(venue_id, path.parents[1].name, day, path)
        elif venue_id == "binance":
            root = data_root / "archive" / "binance" / "spot" / "aggTrades"
            for path in sorted(root.rglob("*.zip")):
                day = path.name[:8]
                if len(day) != 8 or not _day_in_range(day, from_day, to_day):
                    continue
                yield ArchivePartition(venue_id, path.parents[1].name, day, path)
        else:
            raise ValueError(f"不支持归档来源 {venue_id}")


def _instrument_maps(conn: sqlite3.Connection) -> dict[tuple[str, str], str]:
    """读取每个来源品种的最新规范化映射。"""
    rows = conn.execute(
        "SELECT m.venue_id, m.venue_symbol, m.instrument_id "
        "FROM instrument_map m JOIN ("
        "SELECT venue_id, venue_symbol, MAX(revision_id) revision_id "
        "FROM instrument_map GROUP BY venue_id, venue_symbol"
        ") latest ON latest.venue_id=m.venue_id "
        "AND latest.venue_symbol=m.venue_symbol "
        "AND latest.revision_id=m.revision_id"
    )
    return {(str(venue), str(symbol)): str(instrument)
            for venue, symbol, instrument in rows}


def _utc_iso(text: object) -> str:
    """补全来源明确为 UTC 的无时区文本。"""
    value = str(text).replace(" ", "T")
    return value if value.endswith(("Z", "+00:00")) else f"{value}+00:00"


def _millis_iso(value: object) -> str:
    """毫秒整数转 UTC ISO 文本。"""
    millis = int(str(value))
    seconds, remainder = divmod(millis, 1_000)
    return datetime.fromtimestamp(seconds, UTC).replace(
        microsecond=remainder * 1_000
    ).isoformat(timespec="milliseconds")


def _epoch_iso(value: object, unit: str) -> str:
    """整数 epoch 转 UTC ISO，拒绝超过微秒的精度损失。"""
    divisors = {
        "seconds": 1,
        "milliseconds": 1_000,
        "microseconds": 1_000_000,
    }
    divisor = divisors.get(unit)
    if divisor is None:
        raise ValueError(f"不支持时间单位 {unit}")
    total_micros = int(str(value)) * 1_000_000
    if total_micros % divisor:
        raise ValueError("时间戳精度超出微秒")
    seconds, micros = divmod(total_micros // divisor, 1_000_000)
    return datetime.fromtimestamp(seconds, UTC).replace(
        microsecond=micros
    ).isoformat()


def rows_from_partition(
    data_root: Path,
    partition: ArchivePartition,
    instrument_id: str,
    ingest_time: str,
) -> Iterator[tuple[Mapping[str, object], NormalizationContext]]:
    """将三种归档形态转为已核证的映射输入。"""
    path = partition.path
    if partition.venue_id == "gmo":
        with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            expected = {"symbol", "side", "size", "price", "timestamp"}
            if reader.fieldnames is None or set(reader.fieldnames) != expected:
                raise ValueError(f"GMO CSV 表头不符 {path}")
            for index, row in enumerate(reader):
                payload: Mapping[str, object] = {
                    "side": row["side"],
                    "size": row["size"],
                    "price": row["price"],
                    "timestamp": _utc_iso(row["timestamp"]),
                }
                context = NormalizationContext(
                    venue_id="gmo",
                    instrument_id=instrument_id,
                    endpoint="trades/archive",
                    ingest_time=ingest_time,
                    raw_source=_relative_source(data_root, path, index + 2),
                    raw_item_index=index,
                    timestamp_unit="iso8601",
                    available_time=str(payload["timestamp"]),
                )
                yield payload, context
        return
    if partition.venue_id == "bitbank":
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            body = json.load(handle, parse_float=Decimal)
        if not isinstance(body, Mapping):
            raise ValueError(f"bitbank 归档非对象 {path}")
        data = body.get("data")
        if not isinstance(data, Mapping):
            raise ValueError(f"bitbank 归档缺 data {path}")
        transactions = data.get("transactions")
        if not isinstance(transactions, list):
            raise ValueError(f"bitbank 归档缺 transactions {path}")
        for index, row in enumerate(transactions):
            if not isinstance(row, Mapping):
                raise ValueError(f"bitbank 逐笔非对象 {path}:{index + 1}")
            event_time = _millis_iso(row["executed_at"])
            context = NormalizationContext(
                venue_id="bitbank",
                instrument_id=instrument_id,
                endpoint="transactions/{day}",
                ingest_time=ingest_time,
                raw_source=_relative_source(data_root, path, index + 1),
                raw_item_index=index,
                timestamp_unit="milliseconds",
                available_time=event_time,
            )
            yield row, context
        return
    if partition.venue_id == "bitflyer":
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for index, line in enumerate(handle):
                if not line.strip():
                    continue
                row = json.loads(line, parse_float=Decimal)
                if not isinstance(row, Mapping):
                    raise ValueError(f"bitFlyer 逐笔非对象 {path}:{index + 1}")
                payload = dict(row)
                payload["exec_date"] = _utc_iso(payload["exec_date"])
                context = NormalizationContext(
                    venue_id="bitflyer",
                    instrument_id=instrument_id,
                    endpoint="/v1/executions",
                    ingest_time=ingest_time,
                    raw_source=_relative_source(data_root, path, index + 1),
                    raw_item_index=index,
                    timestamp_unit="iso8601",
                    available_time=str(payload["exec_date"]),
                )
                yield payload, context
        return
    if partition.venue_id == "binance":
        timestamp_unit = (
            "microseconds" if partition.day >= "20250101" else "milliseconds"
        )
        with zipfile.ZipFile(path) as archive_file:
            names = [name for name in archive_file.namelist() if name.endswith(".csv")]
            if len(names) != 1:
                raise ValueError(f"Binance ZIP CSV 数量非法 {path}")
            with archive_file.open(names[0]) as body:
                text = io.TextIOWrapper(body, encoding="utf-8", newline="")
                binance_reader = csv.reader(text)
                for index, binance_row in enumerate(binance_reader):
                    if len(binance_row) != 8:
                        raise ValueError(f"Binance 行字段数非法 {path}:{index + 1}")
                    maker_text = binance_row[6].lower()
                    if maker_text not in {"true", "false"}:
                        raise ValueError(f"Binance m 非布尔 {path}:{index + 1}")
                    binance_payload: Mapping[str, object] = {
                        "a": binance_row[0], "p": binance_row[1],
                        "q": binance_row[2], "f": binance_row[3],
                        "l": binance_row[4], "T": binance_row[5],
                        "m": maker_text == "true",
                    }
                    event_time = _epoch_iso(
                        binance_payload["T"], timestamp_unit
                    )
                    context = NormalizationContext(
                        venue_id="binance",
                        instrument_id=instrument_id,
                        endpoint="data.binance.vision/aggTrades",
                        ingest_time=ingest_time,
                        raw_source=_relative_source(data_root, path, index + 1),
                        raw_item_index=index,
                        timestamp_unit=timestamp_unit,
                        available_time=event_time,
                    )
                    yield binance_payload, context
        return
    raise ValueError(f"不支持归档来源 {partition.venue_id}")


def _partition_matches(
    conn: sqlite3.Connection,
    partition: ArchivePartition,
    source_sha256: str,
) -> bool:
    """相同内容且已成功的分区无需重放。"""
    stored = conn.execute(
        "SELECT source_sha256, status, normalization_version "
        "FROM normalized_partition WHERE venue_id=? AND venue_symbol=? "
        "AND domain=? AND day=? AND raw_source=?",
        (
            partition.venue_id, partition.venue_symbol, DOMAIN_TRADE,
            partition.day, partition.path.as_posix(),
        ),
    ).fetchone()
    return (
        stored is not None
        and str(stored[0]) == source_sha256
        and str(stored[1]) in {
            "complete", "complete_with_rejections", "empty",
        }
        and str(stored[2]) == trade_normalization_version(partition.venue_id)
    )


def _record_backfill_runs(
    conn: sqlite3.Connection,
    parts: Sequence[ArchivePartition],
    stats: ProjectionStats,
    config_hash: str,
    code_version: str,
    started_at: str,
) -> None:
    """为本次归档投影补齐可审计的回补终态。"""
    grouped: dict[tuple[str, str], list[ArchivePartition]] = defaultdict(list)
    for part in parts:
        grouped[(part.venue_id, part.venue_symbol)].append(part)
    for (venue_id, venue_symbol), group in grouped.items():
        finished_at = _now_iso()
        completed = 0
        failed = 0
        empty = 0
        rows = 0
        for part in group:
            stored = store.normalized_partition(
                conn,
                part.venue_id,
                part.venue_symbol,
                DOMAIN_TRADE,
                part.day,
                part.path.as_posix(),
            )
            if stored is None:
                failed += 1
                continue
            _, status, _, normalized_rows, _ = stored
            rows += normalized_rows
            if status == "empty":
                empty += 1
            elif status in {"complete", "complete_with_rejections"}:
                completed += 1
            else:
                failed += 1
        planned = len(group)
        status = "complete" if failed == 0 else "failed"
        detail = None
        if failed:
            detail = "; ".join(stats.errors[-3:])[:1_000]
        row: store.BackfillRunRow = (
            new_run_id(), venue_id, venue_symbol, DOMAIN_TRADE,
            min(part.day for part in group), max(part.day for part in group),
            planned, completed, 0, empty, rows, 0,
            status, detail, started_at, finished_at, config_hash, code_version,
        )
        store.insert_backfill_run(conn, row)


def project_trade_archives(
    data_root: Path,
    conn: sqlite3.Connection,
    *,
    venue_ids: Sequence[str] = PROJECTABLE_VENUES,
    from_day: str | None = None,
    to_day: str | None = None,
    max_partitions: int | None = None,
    force: bool = False,
    code_version: str = "working-tree",
) -> ProjectionStats:
    """投影三家归档逐笔；内容不变的完整分区自动跳过。"""
    if max_partitions is not None and max_partitions < 1:
        raise ValueError("max_partitions 必须为正数")
    registry.register_all(conn)
    mappings = _instrument_maps(conn)
    all_parts = list(archive_partitions(data_root, venue_ids, from_day, to_day))
    selected: list[ArchivePartition] = []
    stats = ProjectionStats()
    for part in all_parts:
        if (part.venue_id, part.venue_symbol) not in mappings:
            stats.unmapped_partitions += 1
            stats.add(part.venue_id, "unmapped")
            continue
        if max_partitions is not None and len(selected) >= max_partitions:
            break
        selected.append(part)
    started_at = _now_iso()
    config = {
        "venue_ids": list(venue_ids),
        "from_day": from_day,
        "to_day": to_day,
        "force": force,
        "normalization_versions": {
            venue_id: trade_normalization_version(venue_id)
            for venue_id in venue_ids
        },
    }
    config_hash = sha256_hex(json.dumps(config, sort_keys=True).encode("utf-8"))
    for part in selected:
        stats.partitions_seen += 1
        stats.add(part.venue_id, "partitions_seen")
        source_sha256 = _sha256_file(part.path)
        raw_source = part.path.as_posix()
        if part.venue_id == "binance":
            checksum_path = archive.binance_checksum_path(
                data_root, part.venue_symbol, part.day
            )
            if not checksum_path.exists():
                raise ValueError(f"Binance 缺少 CHECKSUM {checksum_path}")
            expected = checksum_path.read_text(encoding="utf-8").split()[0].lower()
            if source_sha256 != expected:
                raise ValueError(f"Binance CHECKSUM 不匹配 {part.path}")
        if not force and _partition_matches(conn, part, source_sha256):
            stats.partitions_skipped += 1
            stats.add(part.venue_id, "skipped")
            continue
        begin = _now_iso()
        raw_rows = 0
        normalized_rows = 0
        inserted_rows = 0
        rejected_rows = 0
        batch: list[store.TradeTickRow] = []
        failure: str | None = None
        rejection_detail: str | None = None
        try:
            instrument_id = mappings[(part.venue_id, part.venue_symbol)]
            for payload, context in rows_from_partition(
                data_root, part, instrument_id, begin
            ):
                raw_rows += 1
                try:
                    batch.append(normalize_trade(payload, context).as_row())
                    normalized_rows += 1
                except NormalizationError as exc:
                    rejected_rows += 1
                    rejection_detail = f"{context.raw_source}: {exc}"
                if len(batch) >= BATCH_ROWS:
                    inserted_rows += store.insert_trade_ticks(conn, batch)
                    batch = []
            if batch:
                inserted_rows += store.insert_trade_ticks(conn, batch)
        except (OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
            failure = f"{raw_source}: {exc}"
        status = "complete"
        if raw_rows == 0 and failure is None:
            status = "empty"
        elif failure is not None:
            status = "failed"
        elif rejected_rows:
            status = "complete_with_rejections"
        finished = _now_iso()
        partition_row: store.NormalizedPartitionRow = (
            part.venue_id, part.venue_symbol, DOMAIN_TRADE, part.day,
            raw_source, source_sha256, raw_rows, normalized_rows, rejected_rows,
            begin, finished, trade_normalization_version(part.venue_id),
            1, status,
            failure if failure is not None else rejection_detail,
        )
        store.upsert_normalized_partition(conn, partition_row)
        stats.raw_rows += raw_rows
        stats.normalized_rows += normalized_rows
        stats.inserted_rows += inserted_rows
        stats.rejected_rows += rejected_rows
        stats.add(part.venue_id, "raw_rows", raw_rows)
        stats.add(part.venue_id, "normalized_rows", normalized_rows)
        stats.add(part.venue_id, "inserted_rows", inserted_rows)
        stats.add(part.venue_id, "rejected_rows", rejected_rows)
        if status == "complete":
            stats.partitions_complete += 1
            stats.add(part.venue_id, "complete")
        elif status == "empty":
            stats.partitions_empty += 1
            stats.add(part.venue_id, "empty")
        elif status == "complete_with_rejections":
            stats.partitions_quarantined += 1
            stats.add(part.venue_id, "quarantined")
            if rejection_detail is not None:
                stats.errors.append(rejection_detail)
        else:
            stats.partitions_failed += 1
            stats.add(part.venue_id, "failed")
            if failure is not None:
                stats.errors.append(failure)
    _record_backfill_runs(
        conn, selected, stats, config_hash, code_version, started_at
    )
    return stats


def project_binance_archives(
    data_root: Path,
    conn: sqlite3.Connection,
    *,
    from_day: str | None = None,
    to_day: str | None = None,
    max_partitions: int | None = None,
    force: bool = False,
    code_version: str = "working-tree",
) -> ProjectionStats:
    """投影经 CHECKSUM 核验的 Binance 聚合逐笔归档。"""
    return project_trade_archives(
        data_root, conn, venue_ids=("binance",), from_day=from_day,
        to_day=to_day, max_partitions=max_partitions, force=force,
        code_version=code_version,
    )


def validate_trade_projection(
    conn: sqlite3.Connection,
    venue_ids: Sequence[str],
) -> ProjectionValidation:
    """校验事实域、金额形态、时间顺序与分区终态。"""
    errors: list[str] = []
    trade_rows: dict[str, int] = {}
    partitions: dict[str, int] = {}
    for venue_id in venue_ids:
        trade_rows[venue_id] = int(conn.execute(
            "SELECT COUNT(*) FROM trade_tick WHERE venue_id=?", (venue_id,)
        ).fetchone()[0])
        partitions[venue_id] = int(conn.execute(
            "SELECT COUNT(*) FROM normalized_partition WHERE venue_id=? "
            "AND domain=? AND status IN "
            "('complete', 'complete_with_rejections', 'empty')",
            (venue_id, DOMAIN_TRADE),
        ).fetchone()[0])
        if trade_rows[venue_id] == 0:
            errors.append(f"{venue_id}: trade_tick 为空")
        if partitions[venue_id] == 0:
            errors.append(f"{venue_id}: 完成分区为空")
    invalid_money = int(conn.execute(
        "SELECT COUNT(*) FROM trade_tick WHERE typeof(price) != 'text' "
        "OR typeof(size) != 'text'"
    ).fetchone()[0])
    if invalid_money:
        errors.append(f"金额非文本 {invalid_money} 行")
    invalid_time = int(conn.execute(
        "SELECT COUNT(*) FROM trade_tick WHERE available_time < event_time"
    ).fetchone()[0])
    if invalid_time:
        errors.append(f"可得时刻早于事件 {invalid_time} 行")
    broken_origin = int(conn.execute(
        "SELECT COUNT(*) FROM trade_tick WHERE raw_source='' "
        "OR raw_item_index < 0"
    ).fetchone()[0])
    if broken_origin:
        errors.append(f"原始血缘缺失 {broken_origin} 行")
    failed_partitions = int(conn.execute(
        "SELECT COUNT(*) FROM normalized_partition WHERE domain=? "
        "AND status='failed'", (DOMAIN_TRADE,)
    ).fetchone()[0])
    if failed_partitions:
        errors.append(f"失败分区 {failed_partitions}")
    return ProjectionValidation(not errors, trade_rows, partitions, errors)


def _raw_records(path: Path) -> Iterator[tuple[int, Mapping[str, object]]]:
    """逐行读取 raw，数值以 Decimal 保留原始精度。"""
    with path.open(encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                record = json.loads(line, parse_float=Decimal)
            except json.JSONDecodeError:
                continue
            if not isinstance(record, Mapping):
                raise ValueError(f"raw 行非对象 {path}:{line_no}")
            yield line_no, record


def _payload(record: Mapping[str, object]) -> Mapping[str, object] | None:
    """兼容旧解析帧与新 wire 帧。"""
    value = record.get("payload")
    if isinstance(value, Mapping):
        return value
    wire = record.get("payload_raw")
    if not isinstance(wire, str):
        return None
    decoded = json.loads(wire, parse_float=Decimal)
    return decoded if isinstance(decoded, Mapping) else None


def _positive_top(
    levels: object, *, bid: bool
) -> tuple[object, object] | None:
    """从任意顺序的档位中提取最佳有效价量。"""
    if not isinstance(levels, list):
        return None
    candidates: list[tuple[Decimal, object, object]] = []
    for level in levels:
        if isinstance(level, Mapping):
            price = level.get("price")
            size = level.get("size")
        elif isinstance(level, list) and len(level) >= 2:
            price, size = level[0], level[1]
        else:
            continue
        if isinstance(price, bool) or isinstance(size, bool):
            continue
        try:
            price_number = Decimal(str(price))
            size_number = Decimal(str(size))
        except ArithmeticError:
            continue
        if not price_number.is_finite() or not size_number.is_finite():
            continue
        if price_number <= 0 or size_number <= 0:
            continue
        candidates.append((price_number, price, size))
    if not candidates:
        return None
    chosen = (
        max(candidates, key=lambda item: item[0])
        if bid else min(candidates, key=lambda item: item[0])
    )
    return chosen[1], chosen[2]


def _book_context(
    venue_id: str,
    instrument_id: str,
    endpoint: str,
    ingest_time: object,
    raw_source: str,
    timestamp_unit: str,
) -> NormalizationContext:
    """装配盘口归一化上下文。"""
    return NormalizationContext(
        venue_id=venue_id,
        instrument_id=instrument_id,
        endpoint=endpoint,
        ingest_time=str(ingest_time),
        raw_source=raw_source,
        raw_item_index=0,
        timestamp_unit=timestamp_unit,
    )


def _record_book_top(
    conn: sqlite3.Connection,
    batch: list[store.BookTopRow],
    context: NormalizationContext,
    event_time: object | None,
    bids: object,
    asks: object,
    sequence_id: object | None,
) -> tuple[store.BookTopRow | None, str | None]:
    """提取顶档并附加到批次，返回错误供台账使用。"""
    top_bid = _positive_top(bids, bid=True)
    top_ask = _positive_top(asks, bid=False)
    if top_bid is None or top_ask is None:
        return None, "无有效双侧顶档"
    source_depth = min(len(bids), len(asks)) if isinstance(bids, list) and isinstance(asks, list) else 0
    if source_depth < 1:
        return None, "盘口档数非法"
    try:
        normalized = normalize_book_top(
            context=context,
            event_time=event_time,
            bid=top_bid[0],
            bid_size=top_bid[1],
            ask=top_ask[0],
            ask_size=top_ask[1],
            depth_levels=1,
            source_depth_levels=source_depth,
            sequence_id=sequence_id,
        )
    except NormalizationError as exc:
        return None, str(exc)
    batch.append(normalized.as_row())
    if len(batch) >= BATCH_ROWS:
        store.insert_book_tops(conn, batch)
        batch.clear()
    return normalized.as_row(), None


def _window_start(raw_source: str) -> str:
    """从 raw 日期分区生成健康窗口起点。"""
    day = raw_source.split("/")[1]
    return f"{day}T00:00:00+00:00"


def _health_add(
    health: dict[tuple[str, str, str, str], list[object]],
    venue_id: str,
    channel: str,
    instrument_id: str,
    raw_source: str,
    event_time: str,
    status: str,
    sequence_id: object | None = None,
) -> None:
    """汇总流或快照的健康窗口。"""
    key = (venue_id, channel, instrument_id, _window_start(raw_source))
    current = health.setdefault(key, [0, None, status, None, 0, 0])
    frames = current[0]
    assert isinstance(frames, int)
    current[0] = frames + 1
    if current[1] is None or str(current[1]) < event_time:
        current[1] = event_time
    if sequence_id is None:
        return
    try:
        sequence = int(str(sequence_id))
    except ValueError:
        return
    previous = current[3]
    assert previous is None or isinstance(previous, int)
    if previous is not None and sequence < previous:
        regressions = current[5]
        assert isinstance(regressions, int)
        current[5] = regressions + 1
    current[3] = sequence


def _project_gmo_books(
    data_root: Path,
    conn: sqlite3.Connection,
    mappings: Mapping[tuple[str, str], str],
    batch: list[store.BookTopRow],
    health: dict[tuple[str, str, str, str], list[object]],
    stats: BookProjectionStats,
) -> None:
    """投影 GMO 公开 WS 的 L2 快照顶档。"""
    for path in sorted((data_root / "raw").glob("*/ws_public.jsonl")):
        for line_no, record in _raw_records(path):
            payload = _payload(record)
            if payload is None or payload.get("channel") != "orderbooks":
                continue
            symbol = str(payload.get("symbol", record.get("symbol", "")))
            instrument_id = mappings.get(("gmo", symbol))
            if instrument_id is None:
                continue
            raw_source = _relative_source(data_root, path, line_no)
            context = _book_context(
                "gmo", instrument_id, "orderbooks/ws", record["ingest_time"],
                raw_source, "iso8601",
            )
            row, error = _record_book_top(
                conn, batch, context, payload.get("timestamp"),
                payload.get("bids"), payload.get("asks"), None,
            )
            stats.frames_seen += 1
            if error is not None:
                stats.rejected_frames += 1
                stats.errors.append(f"{raw_source}: {error}")
                continue
            assert row is not None
            stats.by_venue["gmo"] = stats.by_venue.get("gmo", 0) + 1
            _health_add(
                health, "gmo", "orderbooks", instrument_id, raw_source,
                row[3], "snapshot",
            )


def _project_bitbank_books(
    data_root: Path,
    conn: sqlite3.Connection,
    mappings: Mapping[tuple[str, str], str],
    batch: list[store.BookTopRow],
    health: dict[tuple[str, str, str, str], list[object]],
    stats: BookProjectionStats,
) -> None:
    """投影 bitbank REST 深度快照顶档。"""
    for path in sorted((data_root / "raw").glob("*/bitbank/depth.jsonl")):
        for line_no, record in _raw_records(path):
            payload = _payload(record)
            if payload is None or not isinstance(payload.get("data"), Mapping):
                continue
            endpoint = str(record.get("path", ""))
            parts = [part for part in endpoint.split("/") if part]
            if len(parts) < 2:
                continue
            symbol = parts[0]
            instrument_id = mappings.get(("bitbank", symbol))
            if instrument_id is None:
                continue
            data = payload["data"]
            assert isinstance(data, Mapping)
            raw_source = _relative_source(data_root, path, line_no)
            context = _book_context(
                "bitbank", instrument_id, "depth", record["ingest_time"],
                raw_source, "milliseconds",
            )
            row, error = _record_book_top(
                conn, batch, context, data.get("timestamp"), data.get("bids"),
                data.get("asks"), None,
            )
            stats.frames_seen += 1
            if error is not None:
                stats.rejected_frames += 1
                stats.errors.append(f"{raw_source}: {error}")
                continue
            assert row is not None
            stats.by_venue["bitbank"] = stats.by_venue.get("bitbank", 0) + 1
            _health_add(
                health, "bitbank", "depth_rest", instrument_id, raw_source,
                row[3], "rest_snapshot",
            )
    for path in sorted((data_root / "raw").glob("*/bitbank/ws_public.jsonl")):
        for line_no, record in _raw_records(path):
            wire = record.get("payload_raw")
            if not isinstance(wire, str) or not wire.startswith("42"):
                continue
            packet = json.loads(wire[2:], parse_float=Decimal)
            if not isinstance(packet, list) or len(packet) != 2:
                continue
            envelope = packet[1]
            if not isinstance(envelope, Mapping):
                continue
            room = str(envelope.get("room_name", ""))
            message = envelope.get("message")
            if not isinstance(message, Mapping):
                continue
            data = message.get("data")
            if not isinstance(data, Mapping):
                continue
            if room.startswith("depth_whole_"):
                symbol = room.removeprefix("depth_whole_")
            elif room.startswith("depth_diff_"):
                symbol = room.removeprefix("depth_diff_")
            else:
                continue
            instrument_id = mappings.get(("bitbank", symbol))
            if instrument_id is None:
                continue
            raw_source = _relative_source(data_root, path, line_no)
            event_time = data.get("timestamp", data.get("t"))
            context = _book_context(
                "bitbank", instrument_id, room, record["ingest_time"],
                raw_source, "milliseconds",
            )
            if room.startswith("depth_whole_"):
                row, error = _record_book_top(
                    conn, batch, context, event_time, data.get("bids"),
                    data.get("asks"), data.get("sequenceId"),
                )
                stats.frames_seen += 1
                if error is not None:
                    stats.rejected_frames += 1
                    stats.errors.append(f"{raw_source}: {error}")
                    continue
                assert row is not None
                stats.by_venue["bitbank"] = stats.by_venue.get("bitbank", 0) + 1
                observed = row[3]
            else:
                stats.frames_seen += 1
                observed = (
                    _epoch_iso(event_time, "milliseconds")
                    if event_time is not None else str(record["ingest_time"])
                )
            _health_add(
                health, "bitbank", "book", instrument_id, raw_source,
                observed, "monotonic", data.get("sequenceId", data.get("s")),
            )


def _project_bitflyer_books(
    data_root: Path,
    conn: sqlite3.Connection,
    mappings: Mapping[tuple[str, str], str],
    batch: list[store.BookTopRow],
    health: dict[tuple[str, str, str, str], list[object]],
    stats: BookProjectionStats,
) -> None:
    """投影 bitFlyer 公开 WS 的盘口与 ticker 顶档。"""
    for path in sorted((data_root / "raw").glob("*/bitflyer/ws_public.jsonl")):
        for line_no, record in _raw_records(path):
            payload = _payload(record)
            if payload is None or not isinstance(payload.get("params"), Mapping):
                continue
            params = payload["params"]
            assert isinstance(params, Mapping)
            channel = str(params.get("channel", ""))
            message = params.get("message")
            if not isinstance(message, Mapping):
                continue
            if channel.startswith("lightning_ticker_"):
                symbol = str(message.get("product_code", channel.removeprefix("lightning_ticker_")))
                bids: object = [[message.get("best_bid"), message.get("best_bid_size")]]
                asks: object = [[message.get("best_ask"), message.get("best_ask_size")]]
                event_time = message.get("timestamp")
                sequence_id = message.get("tick_id")
            elif channel.startswith("lightning_board_snapshot_"):
                symbol = channel.removeprefix("lightning_board_snapshot_")
                bids = message.get("bids")
                asks = message.get("asks")
                event_time = None
                sequence_id = None
            else:
                continue
            instrument_id = mappings.get(("bitflyer", symbol))
            if instrument_id is None:
                continue
            raw_source = _relative_source(data_root, path, line_no)
            context = _book_context(
                "bitflyer", instrument_id, channel, record["ingest_time"],
                raw_source, "iso8601",
            )
            row, error = _record_book_top(
                conn, batch, context, event_time, bids, asks, sequence_id
            )
            stats.frames_seen += 1
            if error is not None:
                stats.rejected_frames += 1
                stats.errors.append(f"{raw_source}: {error}")
                continue
            assert row is not None
            stats.by_venue["bitflyer"] = stats.by_venue.get("bitflyer", 0) + 1
            _health_add(
                health, "bitflyer", channel, instrument_id, raw_source,
                row[3], "snapshot",
            )


def _project_coincheck_health(
    data_root: Path,
    mappings: Mapping[tuple[str, str], str],
    health: dict[tuple[str, str, str, str], list[object]],
    stats: BookProjectionStats,
) -> None:
    """登记 Coincheck 无序号旁路流的可观测健康窗口。"""
    for path in sorted((data_root / "raw").glob("*/coincheck/ws_public.jsonl")):
        for line_no, record in _raw_records(path):
            wire = record.get("payload_raw")
            if not isinstance(wire, str):
                continue
            frame = json.loads(wire, parse_float=Decimal)
            raw_source = _relative_source(data_root, path, line_no)
            if isinstance(frame, list) and frame and isinstance(frame[0], list):
                for item in frame:
                    if not isinstance(item, list) or len(item) < 3:
                        continue
                    symbol = str(item[2])
                    instrument_id = mappings.get(("coincheck", symbol))
                    if instrument_id is None:
                        continue
                    event_time = _epoch_iso(item[0], "seconds")
                    _health_add(
                        health, "coincheck", "trades", instrument_id,
                        raw_source, event_time, "none",
                    )
                    stats.frames_seen += 1
                continue
            if (
                isinstance(frame, list) and len(frame) == 2
                and isinstance(frame[0], str) and isinstance(frame[1], Mapping)
            ):
                instrument_id = mappings.get(("coincheck", frame[0]))
                if instrument_id is None:
                    continue
                stamp = frame[1].get("last_update_at")
                event_time = (
                    _epoch_iso(stamp, "seconds")
                    if stamp is not None else str(record["ingest_time"])
                )
                _health_add(
                    health, "coincheck", "orderbook", instrument_id,
                    raw_source, event_time, "none",
                )
                stats.frames_seen += 1


def project_recorded_books(
    data_root: Path, conn: sqlite3.Connection
) -> BookProjectionStats:
    """把已有三家盘口记录投影为顶档事实和健康窗口。"""
    registry.register_all(conn)
    mappings = _instrument_maps(conn)
    before = int(conn.execute("SELECT COUNT(*) FROM book_top").fetchone()[0])
    batch: list[store.BookTopRow] = []
    health: dict[tuple[str, str, str, str], list[object]] = {}
    stats = BookProjectionStats()
    _project_gmo_books(data_root, conn, mappings, batch, health, stats)
    _project_bitbank_books(data_root, conn, mappings, batch, health, stats)
    _project_bitflyer_books(data_root, conn, mappings, batch, health, stats)
    _project_coincheck_health(data_root, mappings, health, stats)
    if batch:
        store.insert_book_tops(conn, batch)
    health_rows: list[store.StreamHealthRow] = []
    for (venue, channel, instrument, window), values in health.items():
        frames, last_event, status, _, gaps, regressions = values
        assert isinstance(frames, int)
        assert last_event is None or isinstance(last_event, str)
        assert isinstance(status, str)
        assert isinstance(gaps, int)
        assert isinstance(regressions, int)
        health_rows.append((
            venue, channel, instrument, window, last_event, frames,
            gaps, regressions, 0, 0, 0, status,
        ))
    store.upsert_stream_health(conn, health_rows)
    after = int(conn.execute("SELECT COUNT(*) FROM book_top").fetchone()[0])
    stats.inserted_rows = after - before
    return stats


def _insert_live_trade(
    conn: sqlite3.Connection,
    batch: list[store.TradeTickRow],
    payload: Mapping[str, object],
    context: NormalizationContext,
    stats: LiveTradeProjectionStats,
) -> None:
    """归一化一笔旁路逐笔，错误只计入隔离计数。"""
    stats.frames_seen += 1
    try:
        batch.append(normalize_trade(payload, context).as_row())
    except NormalizationError as exc:
        stats.rejected_rows += 1
        stats.errors.append(f"{context.raw_source}: {exc}")
        return
    stats.normalized_rows += 1
    stats.by_venue[context.venue_id] = (
        stats.by_venue.get(context.venue_id, 0) + 1
    )
    if len(batch) >= BATCH_ROWS:
        stats.inserted_rows += store.insert_trade_ticks(conn, batch)
        batch.clear()


def _project_coincheck_trades(
    data_root: Path,
    conn: sqlite3.Connection,
    mappings: Mapping[tuple[str, str], str],
    batch: list[store.TradeTickRow],
    stats: LiveTradeProjectionStats,
) -> None:
    """投影 Coincheck 公开逐笔二维数组。"""
    for path in sorted((data_root / "raw").glob("*/coincheck/ws_public.jsonl")):
        for line_no, record in _raw_records(path):
            wire = record.get("payload_raw")
            if not isinstance(wire, str):
                continue
            frame = json.loads(wire, parse_float=Decimal)
            if not isinstance(frame, list) or not frame:
                continue
            if not isinstance(frame[0], list):
                continue
            for item_index, item in enumerate(frame):
                if not isinstance(item, list) or len(item) < 6:
                    continue
                symbol = str(item[2])
                instrument_id = mappings.get(("coincheck", symbol))
                if instrument_id is None:
                    continue
                payload: Mapping[str, object] = {
                    "timestamp": item[0], "trade_id": item[1], "rate": item[3],
                    "amount": item[4], "side": item[5],
                }
                event_time = _epoch_iso(item[0], "seconds")
                context = NormalizationContext(
                    venue_id="coincheck",
                    instrument_id=instrument_id,
                    endpoint=f"{symbol}-trades",
                    ingest_time=str(record["ingest_time"]),
                    raw_source=_relative_source(data_root, path, line_no),
                    raw_item_index=item_index,
                    timestamp_unit="seconds",
                    available_time=event_time,
                )
                _insert_live_trade(conn, batch, payload, context, stats)


def _project_bitbank_stream_trades(
    data_root: Path,
    conn: sqlite3.Connection,
    mappings: Mapping[tuple[str, str], str],
    batch: list[store.TradeTickRow],
    stats: LiveTradeProjectionStats,
) -> None:
    """投影 bitbank Socket.IO transactions 房间。"""
    for path in sorted((data_root / "raw").glob("*/bitbank/ws_public.jsonl")):
        for line_no, record in _raw_records(path):
            wire = record.get("payload_raw")
            if not isinstance(wire, str) or not wire.startswith("42"):
                continue
            packet = json.loads(wire[2:], parse_float=Decimal)
            if not isinstance(packet, list) or len(packet) != 2:
                continue
            envelope = packet[1]
            if not isinstance(envelope, Mapping):
                continue
            room = str(envelope.get("room_name", ""))
            if not room.startswith("transactions_"):
                continue
            symbol = room.removeprefix("transactions_")
            instrument_id = mappings.get(("bitbank", symbol))
            if instrument_id is None:
                continue
            message = envelope.get("message")
            if not isinstance(message, Mapping):
                continue
            data = message.get("data")
            if not isinstance(data, Mapping):
                continue
            transactions = data.get("transactions")
            if not isinstance(transactions, list):
                continue
            for item_index, item in enumerate(transactions):
                if not isinstance(item, Mapping):
                    continue
                event_time = _millis_iso(item["executed_at"])
                context = NormalizationContext(
                    venue_id="bitbank",
                    instrument_id=instrument_id,
                    endpoint=room,
                    ingest_time=str(record["ingest_time"]),
                    raw_source=_relative_source(data_root, path, line_no),
                    raw_item_index=item_index,
                    timestamp_unit="milliseconds",
                    available_time=event_time,
                )
                _insert_live_trade(conn, batch, item, context, stats)


def project_recorded_trades(
    data_root: Path, conn: sqlite3.Connection
) -> LiveTradeProjectionStats:
    """投影 Coincheck 与 bitbank 已录制的实时逐笔。"""
    registry.register_all(conn)
    mappings = _instrument_maps(conn)
    before = int(conn.execute("SELECT COUNT(*) FROM trade_tick").fetchone()[0])
    batch: list[store.TradeTickRow] = []
    stats = LiveTradeProjectionStats()
    _project_coincheck_trades(data_root, conn, mappings, batch, stats)
    _project_bitbank_stream_trades(data_root, conn, mappings, batch, stats)
    if batch:
        store.insert_trade_ticks(conn, batch)
    after = int(conn.execute("SELECT COUNT(*) FROM trade_tick").fetchone()[0])
    stats.inserted_rows = after - before
    return stats
