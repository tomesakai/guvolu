"""GMO 逐笔历史归档下载的原子写单测。全程离线（C-13）。

写一半崩溃不得留下目标文件，否则增量逻辑永久跳过该日（D-02 旁证）。
"""
from __future__ import annotations

import gzip
import os
from pathlib import Path

import pytest

from guvolu.api.transport import RateLimiter
from guvolu.data import collect

_SYMBOL = "BTC"
_DAY = "20180905"
_ARCHIVE_URL = (
    f"{collect.ARCHIVE_BASE_URL}/{_SYMBOL}/2018/09/{_DAY}_{_SYMBOL}.csv.gz"
)
_PAYLOAD = gzip.compress(
    b"symbol,side,size,price,timestamp\n"
    b"BTC,BUY,0.1,800000,2018-09-05 00:00:00.000\n"
)


class _FakeResponse:
    def __init__(self, status_code: int, content: bytes, text: str = "") -> None:
        self.status_code = status_code
        self.content = content
        self.text = text

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _FakeSession:
    """离线会话替身，按地址表应答（C-13）。"""

    def __init__(self) -> None:
        self._responses = {
            f"{collect.ARCHIVE_BASE_URL}/{_SYMBOL}/": _FakeResponse(
                200, b"", '<a href="2018/">2018/</a>'
            ),
            f"{collect.ARCHIVE_BASE_URL}/{_SYMBOL}/2018/": _FakeResponse(
                200, b"", '<a href="09/">09/</a>'
            ),
            _ARCHIVE_URL: _FakeResponse(200, _PAYLOAD),
        }
        self.calls: list[str] = []

    def get(self, url: str, timeout: float) -> _FakeResponse:
        del timeout
        self.calls.append(url)
        return self._responses[url]


def _run_archive(session: _FakeSession) -> None:
    collect._archive_one(
        session, RateLimiter(10000.0), _SYMBOL, _DAY, _DAY,
    )


def _target(root: Path) -> Path:
    return (
        root / "archive" / "trades" / _SYMBOL / "2018" / "09"
        / f"{_DAY}_{_SYMBOL}.csv.gz"
    )


def test_archive_download_is_atomic_and_resumable_after_crash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """替换前崩溃不留目标文件，重跑仍会补下该日。"""
    monkeypatch.setattr(collect, "DATA_ROOT", tmp_path)
    session = _FakeSession()
    target = _target(tmp_path)
    original_replace = os.replace

    def crash_replace(source: object, destination: object) -> None:
        del source, destination
        raise OSError("simulated crash before replace")

    with monkeypatch.context() as patched:
        patched.setattr(
            "guvolu.data.durable_io.os.replace", crash_replace
        )
        with pytest.raises(OSError, match="simulated crash"):
            _run_archive(session)

    assert os.replace is original_replace
    assert not target.exists()
    assert list(target.parent.glob(f".{target.name}.tmp-*")) == []

    _run_archive(session)

    assert target.read_bytes() == _PAYLOAD
    anomaly_file = (
        tmp_path / "archive" / "trades" / collect.ANOMALY_FILE_NAME
    )
    assert not anomaly_file.exists()


def test_archive_existing_file_is_skipped_on_rerun(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """已落盘文件按增量逻辑跳过，不重复下载。"""
    monkeypatch.setattr(collect, "DATA_ROOT", tmp_path)
    session = _FakeSession()

    _run_archive(session)
    _run_archive(session)

    downloads = [url for url in session.calls if url == _ARCHIVE_URL]
    assert len(downloads) == 1
    assert _target(tmp_path).read_bytes() == _PAYLOAD
