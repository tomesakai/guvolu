"""跨进程串行化共享 SQLite 账本的写周期。"""
from __future__ import annotations

import os
import time
from importlib import import_module
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, BinaryIO


def _try_lock(stream: BinaryIO) -> bool:
    stream.seek(0)
    if os.name == "nt":
        import msvcrt

        try:
            msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError:
            return False
        return True

    fcntl: Any = import_module("fcntl")

    try:
        fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return False
    return True


def _unlock(stream: BinaryIO) -> None:
    stream.seek(0)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
        return

    fcntl: Any = import_module("fcntl")

    fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


@contextmanager
def sqlite_writer_lock(
    data_root: Path, *, timeout_seconds: float = 120.0,
) -> Iterator[None]:
    """取得同一数据根目录的独占写锁；超时而不是无限等待。"""
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds 必须大于 0")
    lock_dir = data_root / ".locks"
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_path = lock_dir / "sqlite-writer.lock"
    deadline = time.monotonic() + timeout_seconds
    with lock_path.open("a+b") as stream:
        if lock_path.stat().st_size == 0:
            stream.write(b"\0")
            stream.flush()
        while not _try_lock(stream):
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"等待 SQLite 写锁超时: {lock_path}"
                )
            time.sleep(0.05)
        try:
            yield
        finally:
            _unlock(stream)
