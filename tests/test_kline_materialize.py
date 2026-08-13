from __future__ import annotations

import json
from pathlib import Path

from guvolu.data import store
from guvolu.data.kline_materialize import audit_klines, materialize_all


def _raw_line(values: list[dict[str, str]], ingest: str) -> str:
    return json.dumps({
        "http_status": 200,
        "params": {"symbol": "BTC", "interval": "1min", "date": "20200101"},
        "payload": {"status": 0, "data": values},
        "ingest_time": ingest,
    })


def test_kline_materialization_preserves_duplicate_evidence_and_revisions(
    tmp_path: Path,
) -> None:
    raw = tmp_path / "raw/2026-08-11/klines.jsonl"
    raw.parent.mkdir(parents=True)
    first = {
        "openTime": "1577836800000", "open": "100", "high": "110",
        "low": "90", "close": "105", "volume": "2",
    }
    revised = {**first, "close": "106"}
    raw.write_text(
        _raw_line([first, first], "2020-01-01T00:02:00+00:00") + "\n"
        + _raw_line([revised], "2020-01-01T00:03:00+00:00") + "\n",
        encoding="utf-8",
    )
    conn = store.connect(tmp_path)
    try:
        results = materialize_all(tmp_path, conn)
        assert len(results) == 1
        result = results[0]
        assert result.source_items == 3
        assert result.fact_rows == 2
        assert result.evidence_rows == 3
        assert result.conflicting_revisions == 1
        assert result.provisional_facts == 0
        audit = audit_klines(tmp_path, conn)
        assert audit["ok"] is True
        assert audit["fact_rows"] == 2
        assert audit["evidence_rows"] == 3
    finally:
        conn.close()
