"""Binance 归档校验与 ZIP 行数单测。"""
import hashlib
import io
import zipfile

import pytest

from guvolu.venues.bitbank import FetchResult
from guvolu.venues.binance import ArchiveDay
from guvolu.venues.collect import _binance_archive_rows


def _zip_csv(text: str) -> bytes:
    """构造单 CSV ZIP 夹具。"""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("BTCUSDT-aggTrades-2026-08-07.csv", text)
    return buffer.getvalue()


def test_archive_day_verifies_official_checksum() -> None:
    """必须比对 ZIP 原字节而非解压文本。"""
    body = _zip_csv("1,2\n")
    digest = hashlib.sha256(body).hexdigest()
    day = ArchiveDay(
        "BTCUSDT", "20260807", FetchResult("zip", 200, body, 1.0),
        FetchResult("checksum", 200, f"{digest} file.zip\n".encode(), 1.0),
    )
    assert day.verify() is True
    assert _binance_archive_rows(body) == 1


def test_archive_day_rejects_invalid_checksum() -> None:
    """校验文件格式不明时拒绝归档。"""
    day = ArchiveDay(
        "BTCUSDT", "20260807", FetchResult("zip", 200, b"x", 1.0),
        FetchResult("checksum", 200, b"invalid\n", 1.0),
    )
    with pytest.raises(ValueError, match="CHECKSUM"):
        day.expected_sha256()
