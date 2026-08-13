"""三来源归档投影与台账单测。"""
import gzip
import hashlib
import io
import json
from pathlib import Path
import zipfile

from guvolu.data import store
from guvolu.data.projection import (
    project_recorded_books,
    project_binance_archives,
    project_trade_archives,
    validate_trade_projection,
)


def _write_gzip(path: Path, text: str) -> None:
    """写入测试归档。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(gzip.compress(text.encode("utf-8")))


def test_three_venue_archive_projection_is_idempotent(tmp_path: Path) -> None:
    """三种归档均入事实表，内容散列使重跑跳过。"""
    _write_gzip(
        tmp_path / "archive" / "trades" / "BTC" / "2026" / "08"
        / "20260807_BTC.csv.gz",
        "symbol,side,size,price,timestamp\n"
        "BTC,BUY,0.01,10000000,2026-08-07 00:00:01.123\n",
    )
    _write_gzip(
        tmp_path / "archive" / "bitbank" / "trades" / "btc_jpy" / "2026"
        / "20260807_btc_jpy.json.gz",
        json.dumps({"success": 1, "data": {"transactions": [{
            "transaction_id": 7,
            "executed_at": 1786060801000,
            "side": "sell",
            "price": "10000001",
            "amount": "0.02",
        }]}}),
    )
    _write_gzip(
        tmp_path / "archive" / "bitflyer" / "executions" / "BTC_JPY" / "2026"
        / "20260807_BTC_JPY.jsonl.gz",
        '{"id":8,"side":"BUY","price":10000002.0,"size":0.03,'
        '"exec_date":"2026-08-07T00:00:01.2"}\n',
    )
    conn = store.connect(tmp_path)
    first = project_trade_archives(
        tmp_path, conn, from_day="20260807", to_day="20260807"
    )
    assert first.partitions_complete == 3
    assert first.inserted_rows == 3
    validation = validate_trade_projection(conn, ("gmo", "bitbank", "bitflyer"))
    assert validation.ready is True
    assert validation.trade_rows == {"gmo": 1, "bitbank": 1, "bitflyer": 1}
    assert conn.execute("SELECT COUNT(*) FROM backfill_run").fetchone()[0] == 3
    second = project_trade_archives(
        tmp_path, conn, from_day="20260807", to_day="20260807"
    )
    assert second.partitions_skipped == 3
    assert second.inserted_rows == 0
    conn.close()


def test_three_venue_book_projection_records_health(tmp_path: Path) -> None:
    """三家顶档均带来源与健康窗口落库。"""
    raw = tmp_path / "raw" / "2026-08-07"
    raw.mkdir(parents=True)
    gmo = {
        "ingest_time": "2026-08-07T00:00:02+00:00",
        "payload": {
            "channel": "orderbooks", "symbol": "BTC",
            "timestamp": "2026-08-07T00:00:01Z",
            "bids": [{"price": "100", "size": "1"}],
            "asks": [{"price": "101", "size": "2"}],
        },
    }
    (raw / "ws_public.jsonl").write_text(json.dumps(gmo) + "\n", encoding="utf-8")
    bitbank = {
        "ingest_time": "2026-08-07T00:00:02+00:00",
        "path": "/btc_jpy/depth",
        "payload": {"data": {
            "timestamp": 1786060801000,
            "bids": [["100", "1"]],
            "asks": [["101", "2"]],
        }},
    }
    bitbank_dir = raw / "bitbank"
    bitbank_dir.mkdir()
    (bitbank_dir / "depth.jsonl").write_text(
        json.dumps(bitbank) + "\n", encoding="utf-8"
    )
    bitflyer = {
        "ingest_time": "2026-08-07T00:00:02+00:00",
        "payload": {"params": {
            "channel": "lightning_ticker_BTC_JPY",
            "message": {
                "product_code": "BTC_JPY",
                "timestamp": "2026-08-07T00:00:01Z",
                "tick_id": 9,
                "best_bid": 100,
                "best_bid_size": 1,
                "best_ask": 101,
                "best_ask_size": 2,
            },
        }},
    }
    bitflyer_dir = raw / "bitflyer"
    bitflyer_dir.mkdir()
    (bitflyer_dir / "ws_public.jsonl").write_text(
        json.dumps(bitflyer) + "\n", encoding="utf-8"
    )
    conn = store.connect(tmp_path)
    stats = project_recorded_books(tmp_path, conn)
    assert stats.inserted_rows == 3
    assert stats.by_venue == {"gmo": 1, "bitbank": 1, "bitflyer": 1}
    assert conn.execute("SELECT COUNT(*) FROM stream_health").fetchone()[0] == 3
    assert conn.execute("SELECT COUNT(*) FROM book_top").fetchone()[0] == 3
    conn.close()


def test_binance_archive_projection_requires_checksum(tmp_path: Path) -> None:
    """Binance 微秒聚合逐笔仅在 ZIP 校验后入库。"""
    target = (
        tmp_path / "archive" / "binance" / "spot" / "aggTrades" / "BTCUSDT"
        / "2025" / "20250102_BTCUSDT.zip"
    )
    target.parent.mkdir(parents=True)
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(
            "BTCUSDT-aggTrades-2025-01-02.csv",
            "1,100.1,0.01,1,1,1735776000000001,False,True\n",
        )
    body = buffer.getvalue()
    target.write_bytes(body)
    target.with_suffix(".zip.CHECKSUM").write_text(
        f"{hashlib.sha256(body).hexdigest()} file.zip\n", encoding="utf-8"
    )
    conn = store.connect(tmp_path)
    stats = project_binance_archives(
        tmp_path, conn, from_day="20250102", to_day="20250102"
    )
    assert stats.inserted_rows == 1
    row = conn.execute(
        "SELECT venue_id, instrument_id, side, price, size, "
        "match_granularity, source_side_basis, normalization_version, "
        "revision_id FROM trade_tick"
    ).fetchone()
    assert row == (
        "binance", "SPOT:BTC/USDT", "buy", "100.1", "0.01",
        "aggregate", "taker_from_buyer_maker",
        "binance-aggtrade-normalization-v2", 1,
    )
    conn.close()
