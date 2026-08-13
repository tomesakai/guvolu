"""研究数据暴露与一次性封存段治理。"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from guvolu.research.provenance import stable_identifier

GOVERNANCE_SCHEMA_VERSION = 1
GOVERNANCE_METHOD_VERSION = "research-data-governance-v1"
_VINTAGE_STATUSES = ("sealed", "consumed")


@dataclass(frozen=True)
class ResearchExposure:
    """一次自适应研究已读取的数据区间。"""

    exposure_id: str
    research_identity: str
    market_id: str
    start_time: datetime
    end_time: datetime
    recorded_at: datetime


@dataclass(frozen=True)
class HoldoutVintage:
    """一个封存或已消费的数据区间。"""

    vintage_id: str
    market_id: str
    start_time: datetime
    end_time: datetime
    sealed_at: datetime
    status: str
    consumed_at: datetime | None
    candidate_set_hash: str | None
    evaluation_id: str | None
    verdict: str | None
    verdict_recorded_at: datetime | None


def _utc(value: datetime) -> datetime:
    """把时间统一为有时区的 UTC。"""
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _timestamp(value: datetime) -> str:
    """生成可按文本排序的 UTC 时间。"""
    return _utc(value).isoformat(timespec="microseconds")


def _parse_timestamp(value: str) -> datetime:
    """读取注册表中的 UTC 时间。"""
    return _utc(datetime.fromisoformat(value))


def _validate_range(start_time: datetime, end_time: datetime) -> tuple[datetime, datetime]:
    """验证左闭右开时间区间。"""
    start = _utc(start_time)
    end = _utc(end_time)
    if start >= end:
        raise ValueError("研究数据区间必须满足 start_time < end_time")
    return start, end


def _connect(path: Path) -> sqlite3.Connection:
    """打开治理注册表并初始化固定 schema。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30.0, isolation_level=None)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS governance_meta (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS research_exposure (
          exposure_id TEXT PRIMARY KEY,
          research_identity TEXT NOT NULL UNIQUE,
          market_id TEXT NOT NULL,
          start_time TEXT NOT NULL,
          end_time TEXT NOT NULL,
          recorded_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS holdout_vintage (
          vintage_id TEXT PRIMARY KEY,
          market_id TEXT NOT NULL,
          start_time TEXT NOT NULL,
          end_time TEXT NOT NULL,
          sealed_at TEXT NOT NULL,
          status TEXT NOT NULL CHECK(status IN ('sealed','consumed')),
          consumed_at TEXT,
          candidate_set_hash TEXT,
          evaluation_id TEXT,
          verdict TEXT,
          verdict_recorded_at TEXT,
          CHECK(
            (status='sealed' AND consumed_at IS NULL
             AND candidate_set_hash IS NULL AND evaluation_id IS NULL)
            OR
            (status='consumed' AND consumed_at IS NOT NULL
             AND candidate_set_hash IS NOT NULL AND evaluation_id IS NOT NULL)
          )
        );
        CREATE UNIQUE INDEX IF NOT EXISTS holdout_vintage_range
          ON holdout_vintage(market_id,start_time,end_time);
        """
    )
    existing = connection.execute(
        "SELECT value FROM governance_meta WHERE key='schema_version'"
    ).fetchone()
    if existing is None:
        connection.execute(
            "INSERT INTO governance_meta(key,value) VALUES('schema_version',?)",
            (str(GOVERNANCE_SCHEMA_VERSION),),
        )
    elif existing["value"] != str(GOVERNANCE_SCHEMA_VERSION):
        connection.close()
        raise ValueError("不支持的研究治理注册表 schema_version")
    return connection


def _begin(connection: sqlite3.Connection) -> None:
    """以写锁开始原子治理事务。"""
    connection.execute("BEGIN IMMEDIATE")


def _overlap_clause() -> str:
    """返回左闭右开区间重叠条件。"""
    return "market_id=? AND start_time<? AND end_time>?"


def register_research_exposure(
    registry_path: Path,
    research_identity: str,
    market_id: str,
    start_time: datetime,
    end_time: datetime,
    *,
    recorded_at: datetime | None = None,
) -> ResearchExposure:
    """登记开发研究暴露；未消费封存段与研究读取互斥。"""
    start, end = _validate_range(start_time, end_time)
    recorded = _utc(recorded_at or datetime.now(UTC))
    exposure_id = stable_identifier("research-exposure", {
        "governance_method_version": GOVERNANCE_METHOD_VERSION,
        "research_identity": research_identity,
        "market_id": market_id,
        "start_time": start.isoformat(),
        "end_time": end.isoformat(),
    })
    connection = _connect(registry_path)
    try:
        _begin(connection)
        protected = connection.execute(
            "SELECT vintage_id FROM holdout_vintage WHERE status='sealed' AND "
            + _overlap_clause() + " LIMIT 1",
            (market_id, _timestamp(end), _timestamp(start)),
        ).fetchone()
        if protected is not None:
            raise ValueError(
                "开发研究区间与未消费封存段重叠: " + str(protected["vintage_id"])
            )
        existing = connection.execute(
            "SELECT * FROM research_exposure WHERE research_identity=?",
            (research_identity,),
        ).fetchone()
        if existing is not None:
            expected = (
                exposure_id,
                market_id,
                _timestamp(start),
                _timestamp(end),
            )
            actual = (
                existing["exposure_id"],
                existing["market_id"],
                existing["start_time"],
                existing["end_time"],
            )
            if actual != expected:
                raise ValueError("同一 research_identity 的数据暴露身份不一致")
            connection.execute("COMMIT")
            return _exposure_from_row(existing)
        connection.execute(
            "INSERT INTO research_exposure("
            "exposure_id,research_identity,market_id,start_time,end_time,recorded_at"
            ") VALUES(?,?,?,?,?,?)",
            (
                exposure_id,
                research_identity,
                market_id,
                _timestamp(start),
                _timestamp(end),
                _timestamp(recorded),
            ),
        )
        connection.execute("COMMIT")
    except BaseException:
        if connection.in_transaction:
            connection.execute("ROLLBACK")
        raise
    finally:
        connection.close()
    return ResearchExposure(
        exposure_id=exposure_id,
        research_identity=research_identity,
        market_id=market_id,
        start_time=start,
        end_time=end,
        recorded_at=recorded,
    )


def seal_holdout_vintage(
    registry_path: Path,
    market_id: str,
    start_time: datetime,
    end_time: datetime,
    *,
    sealed_at: datetime | None = None,
) -> HoldoutVintage:
    """封存尚未被任何自适应研究读取且不重叠的新数据段。"""
    start, end = _validate_range(start_time, end_time)
    sealed = _utc(sealed_at or datetime.now(UTC))
    if sealed > start:
        raise ValueError("封存段必须在区间开始前登记，禁止事后挑选 holdout")
    vintage_id = stable_identifier("holdout-vintage", {
        "governance_method_version": GOVERNANCE_METHOD_VERSION,
        "market_id": market_id,
        "start_time": start.isoformat(),
        "end_time": end.isoformat(),
    })
    connection = _connect(registry_path)
    try:
        _begin(connection)
        exposed = connection.execute(
            "SELECT exposure_id FROM research_exposure WHERE "
            + _overlap_clause() + " LIMIT 1",
            (market_id, _timestamp(end), _timestamp(start)),
        ).fetchone()
        if exposed is not None:
            raise ValueError(
                "封存段已被自适应研究读取: " + str(exposed["exposure_id"])
            )
        overlap = connection.execute(
            "SELECT vintage_id FROM holdout_vintage WHERE "
            + _overlap_clause() + " LIMIT 1",
            (market_id, _timestamp(end), _timestamp(start)),
        ).fetchone()
        if overlap is not None:
            existing = connection.execute(
                "SELECT * FROM holdout_vintage WHERE vintage_id=?",
                (vintage_id,),
            ).fetchone()
            if existing is not None:
                connection.execute("COMMIT")
                return _vintage_from_row(existing)
            raise ValueError("封存段与既有 vintage 重叠: " + str(overlap["vintage_id"]))
        connection.execute(
            "INSERT INTO holdout_vintage("
            "vintage_id,market_id,start_time,end_time,sealed_at,status"
            ") VALUES(?,?,?,?,?,'sealed')",
            (
                vintage_id,
                market_id,
                _timestamp(start),
                _timestamp(end),
                _timestamp(sealed),
            ),
        )
        row = connection.execute(
            "SELECT * FROM holdout_vintage WHERE vintage_id=?",
            (vintage_id,),
        ).fetchone()
        connection.execute("COMMIT")
    except BaseException:
        if connection.in_transaction:
            connection.execute("ROLLBACK")
        raise
    finally:
        connection.close()
    if row is None:
        raise RuntimeError("封存段写入后不可见")
    return _vintage_from_row(row)


def consume_holdout_vintage(
    registry_path: Path,
    vintage_id: str,
    candidate_set_hash: str,
    evaluation_id: str,
    *,
    consumed_at: datetime | None = None,
) -> HoldoutVintage:
    """原子消费封存段；事务提交后永久禁止第二次消费。"""
    if not candidate_set_hash or not evaluation_id:
        raise ValueError("消费封存段必须绑定 candidate_set_hash 与 evaluation_id")
    consumed = _utc(consumed_at or datetime.now(UTC))
    connection = _connect(registry_path)
    try:
        _begin(connection)
        row = connection.execute(
            "SELECT * FROM holdout_vintage WHERE vintage_id=?",
            (vintage_id,),
        ).fetchone()
        if row is None:
            raise LookupError(f"封存段不存在: {vintage_id}")
        if row["status"] != "sealed":
            raise ValueError(f"封存段已经消费: {vintage_id}")
        connection.execute(
            "UPDATE holdout_vintage SET status='consumed',consumed_at=?,"
            "candidate_set_hash=?,evaluation_id=? WHERE vintage_id=? AND status='sealed'",
            (_timestamp(consumed), candidate_set_hash, evaluation_id, vintage_id),
        )
        updated = connection.execute(
            "SELECT * FROM holdout_vintage WHERE vintage_id=?",
            (vintage_id,),
        ).fetchone()
        connection.execute("COMMIT")
    except BaseException:
        if connection.in_transaction:
            connection.execute("ROLLBACK")
        raise
    finally:
        connection.close()
    if updated is None:
        raise RuntimeError("消费后封存段不可见")
    return _vintage_from_row(updated)


def record_holdout_verdict(
    registry_path: Path,
    vintage_id: str,
    verdict: str,
    *,
    recorded_at: datetime | None = None,
) -> HoldoutVintage:
    """为已消费封存段一次性记录最终结论。"""
    normalized = verdict.strip()
    if not normalized:
        raise ValueError("holdout verdict 不得为空")
    recorded = _utc(recorded_at or datetime.now(UTC))
    connection = _connect(registry_path)
    try:
        _begin(connection)
        row = connection.execute(
            "SELECT * FROM holdout_vintage WHERE vintage_id=?",
            (vintage_id,),
        ).fetchone()
        if row is None:
            raise LookupError(f"封存段不存在: {vintage_id}")
        if row["status"] != "consumed":
            raise ValueError("封存段尚未消费，不能记录 verdict")
        if row["verdict"] is not None:
            if row["verdict"] != normalized:
                raise ValueError("holdout verdict 已登记且不可改写")
            connection.execute("COMMIT")
            return _vintage_from_row(row)
        connection.execute(
            "UPDATE holdout_vintage SET verdict=?,verdict_recorded_at=? "
            "WHERE vintage_id=? AND verdict IS NULL",
            (normalized, _timestamp(recorded), vintage_id),
        )
        updated = connection.execute(
            "SELECT * FROM holdout_vintage WHERE vintage_id=?",
            (vintage_id,),
        ).fetchone()
        connection.execute("COMMIT")
    except BaseException:
        if connection.in_transaction:
            connection.execute("ROLLBACK")
        raise
    finally:
        connection.close()
    if updated is None:
        raise RuntimeError("verdict 写入后封存段不可见")
    return _vintage_from_row(updated)


def list_holdout_vintages(registry_path: Path) -> tuple[HoldoutVintage, ...]:
    """按时间列出所有封存段，包括已消费历史。"""
    connection = _connect(registry_path)
    try:
        rows = connection.execute(
            "SELECT * FROM holdout_vintage ORDER BY start_time,vintage_id"
        ).fetchall()
    finally:
        connection.close()
    return tuple(_vintage_from_row(row) for row in rows)


def get_holdout_vintage(
    registry_path: Path,
    vintage_id: str,
) -> HoldoutVintage:
    """按不可变身份读取一个封存段。"""
    connection = _connect(registry_path)
    try:
        row = connection.execute(
            "SELECT * FROM holdout_vintage WHERE vintage_id=?",
            (vintage_id,),
        ).fetchone()
    finally:
        connection.close()
    if row is None:
        raise LookupError(f"封存段不存在: {vintage_id}")
    return _vintage_from_row(row)


def get_research_exposure(
    registry_path: Path,
    exposure_id: str,
) -> ResearchExposure:
    """按不可变身份读取一条研究暴露。"""
    connection = _connect(registry_path)
    try:
        row = connection.execute(
            "SELECT * FROM research_exposure WHERE exposure_id=?",
            (exposure_id,),
        ).fetchone()
    finally:
        connection.close()
    if row is None:
        raise LookupError(f"研究暴露不存在: {exposure_id}")
    return _exposure_from_row(row)


def _exposure_from_row(row: sqlite3.Row) -> ResearchExposure:
    """把 SQLite 行转换为暴露合同。"""
    return ResearchExposure(
        exposure_id=str(row["exposure_id"]),
        research_identity=str(row["research_identity"]),
        market_id=str(row["market_id"]),
        start_time=_parse_timestamp(str(row["start_time"])),
        end_time=_parse_timestamp(str(row["end_time"])),
        recorded_at=_parse_timestamp(str(row["recorded_at"])),
    )


def _optional_time(value: object) -> datetime | None:
    """读取 SQLite 可空时间。"""
    return None if value is None else _parse_timestamp(str(value))


def _optional_text(value: object) -> str | None:
    """读取 SQLite 可空文本。"""
    return None if value is None else str(value)


def _vintage_from_row(row: sqlite3.Row) -> HoldoutVintage:
    """把 SQLite 行转换为 vintage 合同。"""
    status = str(row["status"])
    if status not in _VINTAGE_STATUSES:
        raise ValueError(f"未知 holdout vintage 状态: {status}")
    return HoldoutVintage(
        vintage_id=str(row["vintage_id"]),
        market_id=str(row["market_id"]),
        start_time=_parse_timestamp(str(row["start_time"])),
        end_time=_parse_timestamp(str(row["end_time"])),
        sealed_at=_parse_timestamp(str(row["sealed_at"])),
        status=status,
        consumed_at=_optional_time(row["consumed_at"]),
        candidate_set_hash=_optional_text(row["candidate_set_hash"]),
        evaluation_id=_optional_text(row["evaluation_id"]),
        verdict=_optional_text(row["verdict"]),
        verdict_recorded_at=_optional_time(row["verdict_recorded_at"]),
    )
