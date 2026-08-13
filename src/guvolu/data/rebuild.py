"""normalized 重建：扫描 raw K 线行入库（D-01 单向、可反复执行）。"""
from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

from guvolu.data.kline_plan import available_time, trading_day
from guvolu.data.store import KlineRow, upsert_klines
from guvolu.domain.enums import KlineInterval

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def _rows_from_line(
    line: str, source_ref: str
) -> list[KlineRow]:
    record = json.loads(line)
    payload = record.get("payload")
    if record.get("http_status") != 200 or not isinstance(payload, dict):
        return []
    if payload.get("status") != 0:
        return []
    params = record.get("params") or {}
    symbol = str(params.get("symbol", ""))
    try:
        interval = KlineInterval(str(params.get("interval", "")))
    except ValueError:
        return []
    data = payload.get("data")
    if not symbol or not isinstance(data, list):
        return []
    ingest = str(record.get("ingest_time", ""))
    rows: list[KlineRow] = []
    for bar in data:
        if not isinstance(bar, dict):
            continue
        open_time = _EPOCH + timedelta(milliseconds=int(str(bar["openTime"])))
        rows.append(
            (
                symbol,
                interval.value,
                open_time.isoformat(),
                available_time(interval, open_time).isoformat(),
                ingest,
                trading_day(open_time),
                str(bar["open"]),
                str(bar["high"]),
                str(bar["low"]),
                str(bar["close"]),
                str(bar["volume"]),
                0,
                source_ref,
            )
        )
    return rows


def rebuild_klines(data_root: Path, conn: sqlite3.Connection) -> dict[str, int]:
    """全量扫描各日期目录的 klines.jsonl，幂等入库。"""
    scanned = 0
    inserted = 0
    for path in sorted((data_root / "raw").glob("*/klines.jsonl")):
        day_dir = path.parent.name
        batch: list[KlineRow] = []
        with path.open(encoding="utf-8") as fh:
            for lineno, line in enumerate(fh, 1):
                scanned += 1
                batch.extend(
                    _rows_from_line(line, f"{day_dir}/klines.jsonl:{lineno}")
                )
                if len(batch) >= 5000:
                    inserted += upsert_klines(conn, batch)
                    batch = []
        if batch:
            inserted += upsert_klines(conn, batch)
    return {"scanned_lines": scanned, "inserted_rows": inserted}
