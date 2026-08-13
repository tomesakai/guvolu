"""采集与覆盖登记单测：拆日、断点、覆盖表、扫描器。全程离线（C-13、C-14）。"""
import gzip
import json
from pathlib import Path

import pytest

from guvolu.data import store
from guvolu.venues import archive, collect
from guvolu.venues.bitbank import FetchResult
from guvolu.venues.bitflyer import ExecutionsPage


def test_exec_date_day() -> None:
    """exec_date 归 UTC 日。"""
    assert archive.exec_date_day("2026-08-08T09:28:01.117") == "20260808"


def test_split_batch_groups_and_cursor() -> None:
    """跨日批拆分保序并给出游标。"""
    texts = [
        '{"id":100,"exec_date":"2026-08-08T00:00:01.2","price":1.0}',
        '{"id":99,"exec_date":"2026-08-07T23:59:59.9","price":2.0}',
        '{"id":98,"exec_date":"2026-08-07T23:59:58.5","price":3.0}',
    ]
    batch = archive.split_batch(texts)
    assert set(batch.grouped) == {"20260808", "20260807"}
    assert batch.grouped["20260807"] == [texts[1], texts[2]]
    assert batch.min_id == 98
    assert batch.min_day == "20260807"


def test_append_gzip_member_readable(tmp_path: Path) -> None:
    """多成员追加后可透明连读。"""
    path = tmp_path / "a.jsonl.gz"
    archive.append_gzip_member(path, b"one\n")
    archive.append_gzip_member(path, b"two\n")
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        assert handle.read() == "one\ntwo\n"


def test_write_gzip_atomic(tmp_path: Path) -> None:
    """原文压缩落盘且无临时残留。"""
    path = tmp_path / "d" / "b.json.gz"
    archive.write_gzip_atomic(path, b'{"x":1}')
    assert gzip.decompress(path.read_bytes()) == b'{"x":1}'
    assert list(path.parent.glob("*.tmp")) == []


def test_bitbank_body_stats_ms_to_iso() -> None:
    """行数与毫秒时间范围统计。"""
    body = json.dumps(
        {
            "success": 1,
            "data": {
                "transactions": [
                    {"transaction_id": 2, "executed_at": 1786060803317},
                    {"transaction_id": 1, "executed_at": 1786060791288},
                ]
            },
        }
    )
    stats = archive.bitbank_body_stats(body)
    assert stats.rows == 2
    assert stats.first_ts == "2026-08-06T23:59:51.288+00:00"
    assert stats.last_ts == "2026-08-07T00:00:03.317+00:00"


def test_gmo_csv_stats(tmp_path: Path) -> None:
    """CSV 首行表头，统计行数与首尾时刻。"""
    lines = (
        "symbol,side,size,price,timestamp\n"
        "BTC,BUY,0.01,10000000,2026-08-05 21:00:03.610\n"
        "BTC,SELL,0.02,10000001,2026-08-06 20:59:58.681\n"
    )
    path = tmp_path / "20260806_BTC.csv.gz"
    path.write_bytes(gzip.compress(lines.encode("utf-8")))
    stats = archive.gmo_csv_stats(path)
    assert stats.rows == 2
    assert stats.first_ts == "2026-08-05T21:00:03.610+00:00"
    assert stats.last_ts == "2026-08-06T20:59:58.681+00:00"


def test_bitflyer_file_stats_orderless(tmp_path: Path) -> None:
    """行序无关的最小最大 exec_date。"""
    path = tmp_path / "20260808_BTC_JPY.jsonl.gz"
    rows = [
        '{"id":2,"exec_date":"2026-08-08T02:00:00.5"}',
        '{"id":1,"exec_date":"2026-08-08T01:00:00.5"}',
    ]
    archive.append_gzip_member(path, ("\n".join(rows) + "\n").encode())
    stats = archive.bitflyer_file_stats(path)
    assert stats.rows == 2
    assert stats.first_ts == "2026-08-08T01:00:00.5+00:00"
    assert stats.last_ts == "2026-08-08T02:00:00.5+00:00"


def test_dimensions_and_coverage_tables(tmp_path: Path) -> None:
    """维度只增不改，覆盖幂等可更新。"""
    conn = store.connect(tmp_path)
    from guvolu.venues import registry

    first = registry.register_all(conn)
    assert first > 0
    assert registry.register_all(conn) == 0
    row: store.CoverageRow = (
        "bitbank", "btc_jpy", "trade", "20260807",
        10, "a", "b", "ok", "2026-08-08T00:00:00+00:00",
    )
    assert store.upsert_coverage(conn, [row]) == 1
    updated: store.CoverageRow = (
        "bitbank", "btc_jpy", "trade", "20260807",
        11, "a", "c", "ok", "2026-08-08T01:00:00+00:00",
    )
    store.upsert_coverage(conn, [updated])
    days = store.coverage_days(conn, "bitbank", "btc_jpy", "trade")
    assert days == {"20260807": "ok"}
    summary = store.coverage_summary(conn)
    assert summary[0][:7] == ("bitbank", "btc_jpy", "trade", 1, 1, 0, 0)
    assert summary[0][9] == 11


class FakeBitbank:
    """按日返回脚本响应的仿真来源。"""

    def __init__(self, script: dict[str, FetchResult]) -> None:
        self.script = script
        self.calls: list[str] = []

    def fetch_day(self, pair: str, day: str) -> FetchResult:
        self.calls.append(day)
        return self.script[day]


def _bitbank_body(rows: list[dict[str, int]]) -> bytes:
    return json.dumps(
        {"success": 1, "data": {"transactions": rows}}
    ).encode("utf-8")


def test_backfill_bitbank_pair_and_resume(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """落盘、缺失、空日登记与断点跳过。"""
    monkeypatch.setattr(collect, "DATA_ROOT", tmp_path)
    conn = store.connect(tmp_path)
    body = _bitbank_body(
        [{"transaction_id": 1, "executed_at": 1786060791288}]
    )
    script = {
        "20260805": FetchResult("u", 200, body, 1.0),
        "20260806": FetchResult("u", 404, b'{"success":0}', 1.0),
        "20260807": FetchResult("u", 200, _bitbank_body([]), 1.0),
    }
    fake = FakeBitbank(script)
    collect._backfill_bitbank_pair(
        conn, fake, "btc_jpy", "20260805", "20260807", False
    )
    assert fake.calls == ["20260805", "20260806", "20260807"]
    stored = archive.bitbank_day_path(tmp_path, "btc_jpy", "20260805")
    assert gzip.decompress(stored.read_bytes()) == body
    days = store.coverage_days(conn, "bitbank", "btc_jpy", "trade")
    assert days == {
        "20260805": "ok", "20260806": "missing", "20260807": "empty",
    }
    fake.calls.clear()
    collect._backfill_bitbank_pair(
        conn, fake, "btc_jpy", "20260805", "20260807", False
    )
    assert fake.calls == []


class FakeBitflyer:
    """按序出页的仿真来源，可注入中断。"""

    def __init__(self, pages: list[ExecutionsPage | Exception]) -> None:
        self.pages = list(pages)
        self.befores: list[int | None] = []

    def fetch_executions_page(
        self, product: str, before: int | None
    ) -> ExecutionsPage:
        self.befores.append(before)
        item = self.pages.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def _page(rows: list[dict[str, object]]) -> ExecutionsPage:
    return ExecutionsPage(
        FetchResult("u", 200, json.dumps(rows).encode("utf-8"), 1.0)
    )


def _boundary_page() -> ExecutionsPage:
    return ExecutionsPage(
        FetchResult("u", 400, b'{"status":-156,"data":null}', 1.0)
    )


def test_backfill_bitflyer_split_and_complete(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """跨日拆分落盘、游标推进、完结覆盖。"""
    monkeypatch.setattr(collect, "DATA_ROOT", tmp_path)
    conn = store.connect(tmp_path)
    rows = [
        {"id": 100, "exec_date": "2026-08-08T00:00:01.2", "price": 1.5},
        {"id": 99, "exec_date": "2026-08-07T23:59:59.9", "price": 2.5},
        {"id": 98, "exec_date": "2026-08-07T23:59:58.5", "price": 3.5},
    ]
    fake = FakeBitflyer([_page(rows), _boundary_page()])
    collect._backfill_bitflyer_product(conn, fake, "BTC_JPY", False)
    assert fake.befores == [None, 98]
    day_a = archive.bitflyer_day_path(tmp_path, "BTC_JPY", "20260808")
    day_b = archive.bitflyer_day_path(tmp_path, "BTC_JPY", "20260807")
    with gzip.open(day_b, "rt", encoding="utf-8") as handle:
        lines = handle.read().splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0])["id"] == 99
    cursor = json.loads(
        archive.bitflyer_cursor_path(tmp_path, "BTC_JPY").read_text(
            encoding="utf-8"
        )
    )
    assert cursor["completed"] is True
    assert cursor["rows_total"] == 3
    days = store.coverage_days(conn, "bitflyer", "BTC_JPY", "trade")
    assert days == {"20260808": "ok", "20260807": "ok"}
    assert day_a.exists()


def test_backfill_bitflyer_resume_from_cursor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """中断后按游标续扫并完结。"""
    monkeypatch.setattr(collect, "DATA_ROOT", tmp_path)
    conn = store.connect(tmp_path)
    rows = [{"id": 50, "exec_date": "2026-08-08T00:00:01.2", "price": 1.5}]
    broken = FakeBitflyer([_page(rows), RuntimeError("断网")])
    with pytest.raises(RuntimeError):
        collect._backfill_bitflyer_product(conn, broken, "BTC_JPY", False)
    resumed = FakeBitflyer([_boundary_page()])
    collect._backfill_bitflyer_product(conn, resumed, "BTC_JPY", False)
    assert resumed.befores == [50]
    days = store.coverage_days(conn, "bitflyer", "BTC_JPY", "trade")
    assert days == {"20260808": "ok"}


def test_scan_gmo_archive_registers_and_skips(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """扫描登记覆盖，重跑跳过已登记日。"""
    monkeypatch.setattr(collect, "DATA_ROOT", tmp_path)
    target = tmp_path / "archive" / "trades" / "BTC" / "2026" / "08"
    target.mkdir(parents=True)
    lines = (
        "symbol,side,size,price,timestamp\n"
        "BTC,BUY,0.01,10000000,2026-08-05 21:00:03.610\n"
    )
    (target / "20260806_BTC.csv.gz").write_bytes(
        gzip.compress(lines.encode("utf-8"))
    )
    collect.scan_gmo_archive(False)
    conn = store.connect(tmp_path)
    days = store.coverage_days(conn, "gmo", "BTC", "trade")
    assert days == {"20260806": "ok"}
    capsys.readouterr()
    collect.scan_gmo_archive(False)
    assert "登记0 跳过1" in capsys.readouterr().out
