from __future__ import annotations

import multiprocessing
import time
from pathlib import Path

import pytest

from guvolu.data.sqlite_writer_lock import sqlite_writer_lock


def _hold_lock(root: str, ready: multiprocessing.synchronize.Event) -> None:
    with sqlite_writer_lock(Path(root)):
        ready.set()
        time.sleep(1.0)


def test_sqlite_writer_lock_serializes_processes(tmp_path: Path) -> None:
    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    process = context.Process(target=_hold_lock, args=(str(tmp_path), ready))
    process.start()
    try:
        assert ready.wait(timeout=5.0)
        with pytest.raises(TimeoutError):
            with sqlite_writer_lock(tmp_path, timeout_seconds=0.1):
                pass
    finally:
        process.join(timeout=5.0)
        if process.is_alive():
            process.kill()
    assert process.exitcode == 0
