"""足迹聚合单测：恒等式、价值区、去重推断、缓存、拆桶。全程离线（C-13）。"""
import gzip
import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from guvolu.api.public_client import PublicClient
from guvolu.api.read_client import ReadClient
from guvolu.data.collect import (
    archive_anomalies,
    archive_rows,
    daily_open_close,
    record_archive_anomaly,
    verify_archive_payload,
)
from guvolu.data.footprint import (
    aggregate_archive_file,
    aggregate_prints,
    archive_path,
    bar_open_epoch,
    build_footprint,
    choose_tier,
    dedupe_ws_rows,
    infer_sides,
    price_bin_of,
)
from guvolu.data.store import connect, upsert_klines
from guvolu.domain.config import load_config
from guvolu.ui.query_service import create_app

_TOKEN = "test-token"


def _epoch_ms(iso: str) -> int:
    return int(datetime.fromisoformat(iso).timestamp() * 1000)


def _print_row(
    iso: str, price: str, size: str, side: str
) -> tuple[int, Decimal, Decimal, str]:
    return (_epoch_ms(iso), Decimal(price), Decimal(size), side)


def _write_archive(data_root: Path, symbol: str, day: str, lines: list[str]) -> Path:
    target = archive_path(data_root, symbol, day)
    target.parent.mkdir(parents=True, exist_ok=True)
    body = "symbol,side,size,price,timestamp\n" + "\n".join(lines) + "\n"
    target.write_bytes(gzip.compress(body.encode("utf-8")))
    return target


def _write_ws_trades(
    data_root: Path,
    directory: str,
    frames: list[dict[str, str]],
    ingest_time: str = "2026-08-07T09:00:00+00:00",
) -> None:
    day_dir = data_root / "raw" / directory
    day_dir.mkdir(parents=True, exist_ok=True)
    lines = [
        json.dumps(
            {
                "schema_version": 1,
                "run_id": "runtest",
                "source": "ws_public",
                "channel": "trades",
                "symbol": frame["symbol"],
                "payload": {"channel": "trades", **frame},
                "ingest_time": ingest_time,
            }
        )
        for frame in frames
    ]
    (day_dir / "ws_public.jsonl").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )


def test_bar_open_alignment_session_anchor() -> None:
    """bar 按官方 K 线 openTime 分周期对齐（D-08）。

    1day 锚定 JST 06:00；4hour 与更小周期为时钟对齐。
    """
    at = int(datetime(2026, 8, 5, 21, 0, 3, tzinfo=UTC).timestamp())
    day_open = bar_open_epoch(at, 86400)
    assert datetime.fromtimestamp(day_open, UTC) == datetime(
        2026, 8, 5, 21, 0, tzinfo=UTC
    )
    before = int(datetime(2026, 8, 6, 20, 59, 59, tzinfo=UTC).timestamp())
    assert bar_open_epoch(before, 86400) == day_open
    after = int(datetime(2026, 8, 6, 21, 0, tzinfo=UTC).timestamp())
    assert bar_open_epoch(after, 86400) == day_open + 86400
    quarter = int(datetime(2026, 8, 5, 21, 7, tzinfo=UTC).timestamp())
    assert datetime.fromtimestamp(bar_open_epoch(quarter, 900), UTC) == datetime(
        2026, 8, 5, 21, 0, tzinfo=UTC
    )
    four = int(datetime(2026, 8, 8, 1, 30, tzinfo=UTC).timestamp())
    assert datetime.fromtimestamp(bar_open_epoch(four, 14400), UTC) == datetime(
        2026, 8, 8, 0, 0, tzinfo=UTC
    )


def test_price_bin_flooring() -> None:
    """价格档向下取整，含小数 tick 品种。"""
    assert price_bin_of(Decimal("10223472.000"), Decimal("2000")) == Decimal(
        "10222000"
    )
    assert price_bin_of(Decimal("36.612"), Decimal("0.5")) == Decimal("36.5")
    assert price_bin_of(Decimal("2000"), Decimal("2000")) == Decimal("2000")


def test_aggregate_identity_sum() -> None:
    """恒等式：格值合计等于逐笔合计，双侧各自成立。"""
    prints = [
        _print_row("2026-08-05T21:00:01+00:00", "10000100", "0.01", "BUY"),
        _print_row("2026-08-05T21:00:02+00:00", "10000100", "0.02", "SELL"),
        _print_row("2026-08-05T21:03:00+00:00", "10002500", "0.03", "BUY"),
        _print_row("2026-08-05T21:20:00+00:00", "10004000", "0.04", "SELL"),
        _print_row("2026-08-05T21:25:00+00:00", "10001000", "0.05", "BUY"),
    ]
    bars = aggregate_prints(prints, 900, Decimal("2000"), "archive")
    buy_sum = sum(
        (Decimal(level.buy) for bar in bars for level in bar.levels), Decimal(0)
    )
    sell_sum = sum(
        (Decimal(level.sell) for bar in bars for level in bar.levels), Decimal(0)
    )
    assert buy_sum == Decimal("0.09")
    assert sell_sum == Decimal("0.06")
    assert len(bars) == 2
    assert [bar.open_time for bar in bars] == [
        "2026-08-05T21:00:00+00:00",
        "2026-08-05T21:15:00+00:00",
    ]
    first = bars[0]
    assert first.delta == "0.02"
    assert first.total == "0.06"
    assert first.open == "10000100" and first.close == "10002500"


def test_unknown_side_is_counted_not_assigned_to_sell() -> None:
    """未知侧保留质量计数，不得静默分配到卖侧。"""
    bars = aggregate_prints(
        [
            _print_row(
                "2026-08-05T21:00:01+00:00", "100", "2", "UNKNOWN"
            ),
            _print_row(
                "2026-08-05T21:00:02+00:00", "101", "3", "BUY"
            ),
        ],
        900,
        Decimal("1"),
        "archive",
    )
    assert len(bars) == 1
    assert bars[0].unknown_side_count == 1
    assert bars[0].unknown_side_size == "2"
    assert bars[0].unknown_side_notional == "200"
    assert bars[0].total == "3"
    assert all(level.sell == "0" for level in bars[0].levels)


def test_aggregate_zero_size_keeps_ohlc_not_levels() -> None:
    """零量行计入 OHLC，不产生价格档。"""
    prints = [
        _print_row("2026-08-05T21:00:01+00:00", "100", "0.01", "BUY"),
        _print_row("2026-08-05T21:00:02+00:00", "999", "0.0000", "BUY"),
    ]
    bars = aggregate_prints(prints, 900, Decimal("1"), "archive")
    assert bars[0].high == "999"
    assert [level.price_bin for level in bars[0].levels] == ["100"]


def test_value_area_expansion() -> None:
    """价值区以 POC 为中心扩张至覆盖七成。"""
    prints = [
        _print_row("2026-08-05T21:00:01+00:00", "100", "1", "SELL"),
        _print_row("2026-08-05T21:00:02+00:00", "101", "2", "SELL"),
        _print_row("2026-08-05T21:00:03+00:00", "102", "10", "BUY"),
        _print_row("2026-08-05T21:00:04+00:00", "103", "3", "BUY"),
        _print_row("2026-08-05T21:00:05+00:00", "104", "1", "BUY"),
    ]
    bars = aggregate_prints(prints, 900, Decimal("1"), "archive")
    bar = bars[0]
    assert bar.poc == "102"
    assert bar.vah == "103"
    assert bar.val == "102"
    covered = Decimal("10") + Decimal("3")
    assert covered >= Decimal(bar.total) * Decimal("0.70")


def test_dedupe_pairs_and_singles() -> None:
    """双侧成对合一，单行与同侧重复按行数保留。"""
    rows = [
        ("2026-08-07T09:00:00.100Z", "100", "0.01", "BUY"),
        ("2026-08-07T09:00:00.100Z", "100", "0.01", "SELL"),
        ("2026-08-07T09:00:01.200Z", "101", "0.02", "BUY"),
        ("2026-08-07T09:00:02.300Z", "102", "0.03", "BUY"),
        ("2026-08-07T09:00:02.300Z", "102", "0.03", "SELL"),
        ("2026-08-07T09:00:02.300Z", "102", "0.03", "BUY"),
    ]
    matches = dedupe_ws_rows(rows)
    assert [(price, size) for _, price, size in matches] == [
        ("100", "0.01"),
        ("101", "0.02"),
        ("102", "0.03"),
        ("102", "0.03"),
    ]


def test_infer_sides_tick_rule() -> None:
    """上涨 BUY、下跌 SELL、平价沿用前值。"""
    matches = [
        (1, "100", "1"),
        (2, "101", "1"),
        (3, "101", "1"),
        (4, "99", "1"),
        (5, "99", "1"),
    ]
    sides = [side for _, _, _, side in infer_sides(matches, None)]
    assert sides == ["BUY", "BUY", "BUY", "SELL", "SELL"]
    seeded = [side for _, _, _, side in infer_sides(matches, Decimal("102"))]
    assert seeded == ["SELL", "BUY", "BUY", "SELL", "SELL"]


def test_choose_tier_band() -> None:
    """档位取使中位档数落 8 至 20 的最小值。"""
    tick = Decimal("1")
    assert choose_tier([Decimal("30000")], tick) == 2000
    assert choose_tier([Decimal("300000")], tick) == 10000
    assert choose_tier([Decimal("3000")], tick) == 500
    assert choose_tier([], tick) == 2000
    assert choose_tier([Decimal("30")], Decimal("0.001")) == 2000


def test_archive_cache_lru(tmp_path: Path) -> None:
    """同键第二次命中缓存，不重读文件。"""
    lines = ["BTC,BUY,0.01,10000000.000,2026-08-05 21:00:03.610"]
    target = _write_archive(tmp_path, "BTC", "20260806", lines)
    aggregate_archive_file.cache_clear()
    first = aggregate_archive_file(str(target), "15min", "2000")
    again = aggregate_archive_file(str(target), "15min", "2000")
    assert first is again
    info = aggregate_archive_file.cache_info()
    assert info.hits == 1 and info.misses == 1


def _fixture_root(tmp_path: Path) -> Path:
    """归档一日加当期录制流的标准夹具。"""
    lines = [
        "BTC,BUY,0.0002,10223472.000,2026-08-05 21:00:03.610",
        "BTC,SELL,0.0100,10221000.000,2026-08-05 21:05:00.000",
        "BTC,BUY,0.0300,10225000.000,2026-08-06 20:59:00.000",
    ]
    _write_archive(tmp_path, "BTC", "20260806", lines)
    frames = [
        {
            "symbol": "BTC",
            "price": "10226000",
            "size": "0.01",
            "side": "BUY",
            "timestamp": "2026-08-07T09:00:00.100Z",
        },
        {
            "symbol": "BTC",
            "price": "10226000",
            "size": "0.01",
            "side": "SELL",
            "timestamp": "2026-08-07T09:00:00.100Z",
        },
        {
            "symbol": "BTC",
            "price": "10224000",
            "size": "0.02",
            "side": "SELL",
            "timestamp": "2026-08-07T09:01:00.000Z",
        },
    ]
    _write_ws_trades(tmp_path, "2026-08-07", frames)
    return tmp_path


def test_build_footprint_archive_and_live(tmp_path: Path) -> None:
    """归档标 archive，当期标 live，去重后侧别推断。"""
    root = _fixture_root(tmp_path)
    aggregate_archive_file.cache_clear()
    built = build_footprint(
        root, "BTC", "1day", "20260806", "20260807", "2000", "1", "20260807"
    )
    bars = built["bars"]
    assert isinstance(bars, list)
    assert [bar.source for bar in bars] == ["archive", "live"]
    live = bars[1]
    # 首笔涨于种子价推断 BUY
    assert Decimal(live.total) == Decimal("0.03")
    assert Decimal(live.delta) == Decimal("-0.01")
    meta = built["meta"]
    assert isinstance(meta, dict)
    assert meta["bin"] == "2000"
    assert meta["coverage_from"] == "2026-08-05T21:00:00+00:00"


def test_build_footprint_auto_bin(tmp_path: Path) -> None:
    """自动档位由 bar 范围中位决定并写入元信息。"""
    root = _fixture_root(tmp_path)
    aggregate_archive_file.cache_clear()
    built = build_footprint(
        root, "BTC", "1day", "20260806", "20260806", "auto", "1", "20260807"
    )
    meta = built["meta"]
    assert isinstance(meta, dict)
    # 日 bar 范围四千合八档
    assert meta["tier"] == 500
    assert meta["auto"] is True


def test_footprint_endpoint(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """端点校验与序列化：全字符串数值加元信息。"""
    from test_query_service import _FakePrivateTransport, _FakePublicTransport

    for name in ("GUVOLU_MODE", "GUVOLU_SPOT_WHITELIST"):
        monkeypatch.delenv(name, raising=False)
    root = _fixture_root(tmp_path)
    aggregate_archive_file.cache_clear()
    config = load_config(env_file=tmp_path / "absent.env")
    app = create_app(
        config,
        PublicClient(_FakePublicTransport()),
        ReadClient(_FakePrivateTransport(tmp_path)),
        _TOKEN,
        data_root=root,
    )
    client = TestClient(
        app, base_url="http://127.0.0.1", headers={"X-Guvolu-Token": _TOKEN}
    )
    payload = client.get(
        "/api/footprint",
        params={
            "symbol": "BTC",
            "interval": "1day",
            "from": "20260806",
            "to": "20260806",
            "bin": "2000",
            "tick": "1",
        },
    ).json()
    assert payload["meta"]["bin"] == "2000"
    bar = payload["bars"][0]
    assert bar["source"] == "archive"
    assert isinstance(bar["delta"], str)
    level = bar["levels"][0]
    assert set(level) == {
        "price_bin", "sell", "buy", "sell_notional", "buy_notional"
    }
    assert client.get(
        "/api/footprint", params={"symbol": "BTC", "interval": "8hour"}
    ).status_code == 400
    assert client.get(
        "/api/footprint", params={"symbol": "BTC", "bin": "123"}
    ).status_code == 400
    assert client.get(
        "/api/footprint", params={"symbol": "B/C"}
    ).status_code == 400
    assert client.get(
        "/api/footprint", params={"symbol": "BTC", "tick": "0"}
    ).status_code == 400


def test_archive_anomaly_checks(tmp_path: Path) -> None:
    """后验两断言：成对组与首尾价对照，异常落档。"""
    clean = [
        ("BTC", "BUY", "0.01", "100.000", "2026-08-05 21:00:00.000"),
        ("BTC", "SELL", "0.02", "105.000", "2026-08-06 20:00:00.000"),
    ]
    assert archive_anomalies(clean, ("100", "105")) == []
    wrong_price = archive_anomalies(clean, ("101", "105"))
    assert len(wrong_price) == 1 and "开" in wrong_price[0]
    paired = clean + [
        ("BTC", "BUY", "0.02", "105.000", "2026-08-06 20:00:00.000")
    ]
    found = archive_anomalies(paired, None)
    assert len(found) == 1 and "成对" in found[0]
    assert archive_anomalies([], None) == []
    target = record_archive_anomaly(tmp_path, "BTC", "20260806", found)
    line = json.loads(target.read_text(encoding="utf-8").splitlines()[0])
    assert line["symbol"] == "BTC" and line["anomalies"] == found


def test_verify_archive_payload_with_store(tmp_path: Path) -> None:
    """入库路径整链：解 gz、对照日 K、登记异常。"""
    conn = connect(tmp_path)
    row = (
        "BTC", "1day", "2026-08-05T21:00:00+00:00",
        "2026-08-06T21:00:00+00:00", "2026-08-07T00:00:00+00:00", "20260806",
        "100", "110", "90", "105", "5", 0, "d/klines.jsonl:1",
    )
    upsert_klines(conn, [row])
    conn.close()
    assert daily_open_close(tmp_path, "BTC", "20260806") == ("100", "105")
    body = (
        "symbol,side,size,price,timestamp\n"
        "BTC,BUY,0.01,100.000,2026-08-05 21:00:00.000\n"
        "BTC,SELL,0.02,105.000,2026-08-06 20:00:00.000\n"
    )
    payload = gzip.compress(body.encode("utf-8"))
    assert archive_rows(payload)[0][3] == "100.000"
    assert verify_archive_payload(tmp_path, "BTC", "20260806", payload) == []
    bad_body = body.replace("105.000", "104.000")
    bad = verify_archive_payload(
        tmp_path, "BTC", "20260806", gzip.compress(bad_body.encode("utf-8"))
    )
    assert len(bad) == 1 and "收" in bad[0]
    anomaly_file = tmp_path / "archive" / "trades" / "_anomalies.jsonl"
    assert anomaly_file.exists()


def test_aggregate_notional_identity() -> None:
    """双基准恒等式：金额=Σ价×量，格与 bar 两层成立。"""
    prints = [
        _print_row("2026-08-05T21:00:01+00:00", "10000100", "0.01", "BUY"),
        _print_row("2026-08-05T21:00:02+00:00", "10000700", "0.02", "SELL"),
        _print_row("2026-08-05T21:03:00+00:00", "10002500", "0.03", "BUY"),
        _print_row("2026-08-05T21:20:00+00:00", "10004000", "0.04", "SELL"),
    ]
    bin_size = Decimal("2000")
    bars = aggregate_prints(prints, 900, bin_size, "archive")
    for bar in bars:
        open_ms = _epoch_ms(bar.open_time)
        in_bar = [
            row for row in prints
            if open_ms <= row[0] < open_ms + 900_000
        ]
        expected_total = sum((row[1] * row[2] for row in in_bar), Decimal(0))
        expected_delta = sum(
            (
                row[1] * row[2] * (1 if row[3] == "BUY" else -1)
                for row in in_bar
            ),
            Decimal(0),
        )
        assert Decimal(bar.total_notional) == expected_total
        assert Decimal(bar.delta_notional) == expected_delta
        for level in bar.levels:
            in_cell = [
                row for row in in_bar
                if price_bin_of(row[1], bin_size) == Decimal(level.price_bin)
            ]
            cell_total = sum(
                (row[1] * row[2] for row in in_cell), Decimal(0)
            )
            assert (
                Decimal(level.sell_notional) + Decimal(level.buy_notional)
                == cell_total
            )


def test_load_recent_trade_rows_window_and_seed(tmp_path: Path) -> None:
    """尾扫窗口过滤：窗内行入选，窗前最近价作种子。"""
    from datetime import timedelta

    from guvolu.data.footprint import load_recent_trade_rows

    now = datetime(2026, 8, 7, 9, 0, 30, tzinfo=UTC)
    frames = [
        {
            "symbol": "BTC", "price": "10220000", "size": "0.01",
            "side": "BUY", "timestamp": "2026-08-07T08:58:00.000Z",
        },
        {
            "symbol": "BTC", "price": "10221000", "size": "0.02",
            "side": "BUY", "timestamp": "2026-08-07T09:00:05.000Z",
        },
        {
            "symbol": "BTC", "price": "10221000", "size": "0.02",
            "side": "SELL", "timestamp": "2026-08-07T09:00:05.000Z",
        },
        {
            "symbol": "BTC", "price": "10219000", "size": "0.03",
            "side": "SELL", "timestamp": "2026-08-07T09:00:20.000Z",
        },
    ]
    _write_ws_trades(
        tmp_path, "2026-08-07", frames,
        ingest_time=(now - timedelta(seconds=1)).isoformat(),
    )
    rows, seed = load_recent_trade_rows(tmp_path, "BTC", 60, now)
    assert seed == Decimal("10220000")
    assert [row[0] for row in rows] == [
        "2026-08-07T09:00:05.000Z",
        "2026-08-07T09:00:05.000Z",
        "2026-08-07T09:00:20.000Z",
    ]
    matches = dedupe_ws_rows(rows)
    assert len(matches) == 2
    prints = infer_sides(matches, seed)
    # 涨于种子推断 BUY，回落推断 SELL
    assert [row[3] for row in prints] == ["BUY", "SELL"]


def test_recent_trades_endpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """近窗逐笔端点：去重合一、侧别推断、窗上限钳制。"""
    from datetime import timedelta

    from test_query_service import _FakePrivateTransport, _FakePublicTransport

    for name in ("GUVOLU_MODE", "GUVOLU_SPOT_WHITELIST"):
        monkeypatch.delenv(name, raising=False)
    now = datetime.now(UTC)
    stamp = (now - timedelta(seconds=5)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    frames = [
        {
            "symbol": "BTC", "price": "10226000", "size": "0.01",
            "side": "BUY", "timestamp": stamp,
        },
        {
            "symbol": "BTC", "price": "10226000", "size": "0.01",
            "side": "SELL", "timestamp": stamp,
        },
    ]
    _write_ws_trades(
        tmp_path, now.strftime("%Y-%m-%d"), frames,
        ingest_time=now.isoformat(),
    )
    config = load_config(env_file=tmp_path / "absent.env")
    app = create_app(
        config,
        PublicClient(_FakePublicTransport()),
        ReadClient(_FakePrivateTransport(tmp_path)),
        _TOKEN,
        data_root=tmp_path,
    )
    client = TestClient(
        app, base_url="http://127.0.0.1", headers={"X-Guvolu-Token": _TOKEN}
    )
    payload = client.get(
        "/api/recent-trades", params={"symbol": "BTC", "seconds": 60}
    ).json()
    assert len(payload["items"]) == 1
    item = payload["items"][0]
    assert item["price"] == "10226000" and item["size"] == "0.01"
    assert item["side"] in {"BUY", "SELL"}
    assert isinstance(item["e"], int)
    assert payload["meta"]["side_basis"] == "tick_rule_inference"
    capped = client.get(
        "/api/recent-trades", params={"symbol": "BTC", "seconds": 999999}
    ).json()
    assert capped["meta"]["seconds"] == config.recent_trades_max_seconds
    assert client.get(
        "/api/recent-trades", params={"symbol": "B/C"}
    ).status_code == 400
