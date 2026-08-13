"""来源能力与回补上线门槛审计。"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Sequence

EXPECTED_DOMAINS = ("kline", "trade", "book_realtime", "book_history")


@dataclass(frozen=True, slots=True)
class CoverageRequirement:
    """上线前必须完整的分区范围。"""

    venue_id: str
    venue_symbol: str
    domain: str
    from_day: str
    to_day: str
    allow_empty: bool = True


def _days(first: str, last: str) -> list[str]:
    start = date.fromisoformat(first)
    end = date.fromisoformat(last)
    if end < start:
        raise ValueError("覆盖范围倒置")
    out: list[str] = []
    cursor = start
    while cursor <= end:
        out.append(cursor.strftime("%Y%m%d"))
        cursor += timedelta(days=1)
    return out


def audit_capabilities(
    conn: sqlite3.Connection,
    venue_ids: Sequence[str],
    *,
    now: datetime | None = None,
) -> dict[str, object]:
    """核对能力覆盖、证据有效期与实现状态。"""
    at = (now or datetime.now(UTC)).astimezone(UTC).isoformat()
    rows = conn.execute(
        "SELECT c.venue_id, c.domain, c.endpoint, c.available, "
        "c.evidence_level, c.implementation_status, c.valid_until "
        "FROM venue_capability_revision c JOIN ("
        "SELECT venue_id, domain, endpoint, MAX(revision_id) revision_id "
        "FROM venue_capability_revision GROUP BY venue_id, domain, endpoint"
        ") latest ON latest.venue_id=c.venue_id AND latest.domain=c.domain "
        "AND latest.endpoint=c.endpoint AND latest.revision_id=c.revision_id"
    ).fetchall()
    latest = {(str(row[0]), str(row[1])): row for row in rows}
    missing: list[str] = []
    stale: list[str] = []
    unverified: list[str] = []
    implementation_gaps: list[str] = []
    for venue_id in venue_ids:
        for domain in EXPECTED_DOMAINS:
            key = (venue_id, domain)
            row = latest.get(key)
            label = f"{venue_id}:{domain}"
            if row is None:
                missing.append(label)
                continue
            if str(row[6]) < at:
                stale.append(label)
            if str(row[4]) == "unverified":
                unverified.append(label)
            if int(row[3]) and str(row[5]) != "implemented":
                implementation_gaps.append(label)
    return {
        "checked_at": at,
        "expected": len(venue_ids) * len(EXPECTED_DOMAINS),
        "registered": sum(
            1 for venue_id, _ in latest if venue_id in set(venue_ids)
        ),
        "missing": missing,
        "stale": stale,
        "unverified": unverified,
        "implementation_gaps": implementation_gaps,
        "evidence_ready": not missing and not stale and not unverified,
    }


def audit_coverage(
    conn: sqlite3.Connection,
    requirements: Sequence[CoverageRequirement],
) -> dict[str, object]:
    """逐日核对回补覆盖，不混淆缺失与空日。"""
    results: list[dict[str, object]] = []
    blockers: list[str] = []
    for requirement in requirements:
        expected = _days(requirement.from_day, requirement.to_day)
        rows = conn.execute(
            "SELECT day, status, COALESCE(rows, 0) FROM archive_coverage "
            "WHERE venue_id=? AND venue_symbol=? AND domain=? "
            "AND day>=? AND day<=?",
            (
                requirement.venue_id,
                requirement.venue_symbol,
                requirement.domain,
                requirement.from_day.replace("-", ""),
                requirement.to_day.replace("-", ""),
            ),
        ).fetchall()
        statuses = {str(day): (str(status), int(count)) for day, status, count in rows}
        unregistered = [day for day in expected if day not in statuses]
        missing = [day for day in expected if statuses.get(day, ("", 0))[0] == "missing"]
        empty = [day for day in expected if statuses.get(day, ("", 0))[0] == "empty"]
        ok = [day for day in expected if statuses.get(day, ("", 0))[0] == "ok"]
        label = (
            f"{requirement.venue_id}:{requirement.venue_symbol}:"
            f"{requirement.domain}"
        )
        if unregistered or missing or (empty and not requirement.allow_empty):
            blockers.append(label)
        results.append(
            {
                "key": label,
                "planned": len(expected),
                "ok": len(ok),
                "missing": missing,
                "empty": empty,
                "unregistered": unregistered,
                "rows": sum(count for _, count in statuses.values()),
            }
        )
    return {"requirements": results, "blockers": blockers, "ready": not blockers}


def minimum_launch_readiness(
    conn: sqlite3.Connection,
    venue_ids: Sequence[str],
    coverage: Sequence[CoverageRequirement],
    *,
    now: datetime | None = None,
    required_implementations: Sequence[str] = (),
) -> dict[str, object]:
    """合并最小稳健上线门槛。"""
    capability = audit_capabilities(conn, venue_ids, now=now)
    coverage_result = audit_coverage(conn, coverage)
    blockers: list[str] = []
    if capability["evidence_ready"] is not True:
        blockers.append("capability_evidence")
    raw_implementation_gaps = capability["implementation_gaps"]
    if not isinstance(raw_implementation_gaps, list):
        raise TypeError("capability audit returned invalid implementation_gaps")
    implementation_gaps = {str(item) for item in raw_implementation_gaps}
    required_gaps = sorted(implementation_gaps.intersection(required_implementations))
    if required_gaps:
        blockers.append("required_implementation")
    if coverage_result["ready"] is not True:
        blockers.append("archive_coverage")
    return {
        "ready": not blockers,
        "blockers": blockers,
        "capability": capability,
        "required_implementation_gaps": required_gaps,
        "coverage": coverage_result,
    }
