"""研究面板显式截止上限：配置与命令行来源、封存段前置检查。

上限只收窄面板区间，使研究暴露在封存段之前结束（G-08）；
没有上限时面板仍延伸到活动 head 最大事件时点。
"""
from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from guvolu.research.panel import parse_time

PANEL_TO_TIME_CONFIG_KEY = "panel_to_time"
PANEL_TO_TIME_SOURCES = ("config", "cli", "none")


def _utc(value: datetime) -> datetime:
    """统一为 UTC。"""
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


@dataclass(frozen=True)
class PanelToTimeLimit:
    """面板截止上限及其来源。"""

    limit: datetime | None
    source: str
    config_limit: datetime | None
    cli_limit: datetime | None

    def effective_to_time(self, maximum_event_time: datetime) -> datetime:
        """取上限与活动 head 最大事件时点的较早者。"""
        latest = _utc(maximum_event_time)
        if self.limit is None:
            return latest
        return min(self.limit, latest)

    def identity_payload(self) -> Mapping[str, object]:
        """只有命令行覆盖才进入研究身份。"""
        if self.cli_limit is None:
            return {}
        return {"panel_to_time_override": self.cli_limit.isoformat()}

    def payload(
        self,
        effective_to_time: datetime,
        last_decision_time: datetime,
    ) -> Mapping[str, object]:
        """写入 summary 与 manifest 的截止上限记录。"""
        return {
            "source": self.source,
            "limit": None if self.limit is None else self.limit.isoformat(),
            "config_limit": (
                None if self.config_limit is None
                else self.config_limit.isoformat()
            ),
            "cli_override": (
                None if self.cli_limit is None else self.cli_limit.isoformat()
            ),
            "effective_to_time": _utc(effective_to_time).isoformat(),
            "last_decision_time": _utc(last_decision_time).isoformat(),
        }


def resolve_panel_to_time(
    governance: Mapping[str, object],
    cli_override: datetime | None,
    from_time: datetime,
) -> PanelToTimeLimit:
    """解析配置上限与命令行覆盖；覆盖只能更早。"""
    raw = governance.get(PANEL_TO_TIME_CONFIG_KEY)
    config_limit = (
        None if raw is None
        else parse_time(raw, "data_governance.panel_to_time")
    )
    cli_limit = None if cli_override is None else _utc(cli_override)
    if (
        config_limit is not None
        and cli_limit is not None
        and cli_limit > config_limit
    ):
        raise ValueError(
            "--to-time 不得晚于配置 data_governance.panel_to_time: "
            f"{cli_limit.isoformat()} > {config_limit.isoformat()}"
        )
    limit = cli_limit if cli_limit is not None else config_limit
    if limit is not None and limit <= _utc(from_time):
        raise ValueError("panel_to_time 必须晚于 from_time")
    if cli_limit is not None:
        source = "cli"
    elif config_limit is not None:
        source = "config"
    else:
        source = "none"
    return PanelToTimeLimit(
        limit=limit,
        source=source,
        config_limit=config_limit,
        cli_limit=cli_limit,
    )


def sealed_vintages_overlapping(
    registry_path: Path,
    market_id: str,
    start_time: datetime,
    end_time: datetime,
) -> tuple[str, ...]:
    """以只读连接列出与区间重叠的未消费封存段。"""
    if not registry_path.exists():
        return ()
    uri = f"file:{registry_path.resolve().as_posix()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    try:
        present = connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name='holdout_vintage'"
        ).fetchone()
        if present is None:
            return ()
        rows = connection.execute(
            "SELECT vintage_id,start_time,end_time FROM holdout_vintage "
            "WHERE market_id=? AND status='sealed' ORDER BY start_time,vintage_id",
            (market_id,),
        ).fetchall()
    finally:
        connection.close()
    start = _utc(start_time)
    end = _utc(end_time)
    overlapping: list[str] = []
    for vintage_id, raw_start, raw_end in rows:
        vintage_start = _utc(datetime.fromisoformat(str(raw_start)))
        vintage_end = _utc(datetime.fromisoformat(str(raw_end)))
        if vintage_start < end and start < vintage_end:
            overlapping.append(str(vintage_id))
    return tuple(overlapping)


def reject_sealed_conflict(
    registry_path: Path,
    market_id: str,
    from_time: datetime,
    to_time: datetime,
    limit: PanelToTimeLimit,
) -> None:
    """打开面板前只读预检：面板区间不得触及未消费封存段。"""
    overlapping = sealed_vintages_overlapping(
        registry_path, market_id, from_time, to_time,
    )
    if not overlapping:
        return
    vintages = ",".join(overlapping)
    if limit.limit is None:
        raise ValueError(
            "研究面板区间与未消费封存段重叠，请配置 "
            "data_governance.panel_to_time 或 --to-time: " + vintages
        )
    raise ValueError(
        f"面板截止上限({limit.source}) {_utc(to_time).isoformat()} "
        "晚于封存段起点: " + vintages
    )
