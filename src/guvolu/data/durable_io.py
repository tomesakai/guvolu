"""小型持久化原语：同目录原子替换与可确认追加。"""
from __future__ import annotations

import os
import importlib
from contextlib import contextmanager
from collections.abc import Iterator
from pathlib import Path
from typing import Protocol, cast


class _FcntlModule(Protocol):
    LOCK_EX: int
    LOCK_UN: int

    def flock(self, fd: int, operation: int) -> None: ...


def _fsync_parent(path: Path) -> None:
    """尽力同步目录项；Windows 不允许打开目录时由 replace 保证原子性。"""
    try:
        descriptor = os.open(path.parent, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def atomic_write_bytes(path: Path, body: bytes) -> None:
    """写同目录临时文件，fsync 后原子替换目标。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".tmp")
    try:
        with temp.open("wb") as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, path)
        _fsync_parent(path)
    finally:
        try:
            temp.unlink()
        except FileNotFoundError:
            pass


def atomic_write_text(path: Path, text: str) -> None:
    """UTF-8 文本的 durable atomic replace。"""
    atomic_write_bytes(path, text.encode("utf-8"))


@contextmanager
def exclusive_path_lock(path: Path) -> Iterator[None]:
    """跨进程串行同一持久化目标。

    锁文件与目标同目录，锁随进程句柄释放，不依赖删除锁文件来
    判断存活性。调用方仍必须在持锁后重新检查目标状态。
    """
    lock_path = path.with_name(path.name + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+b") as guard:
        guard.seek(0, os.SEEK_END)
        if guard.tell() == 0:
            guard.write(b"\0")
            guard.flush()
        guard.seek(0)
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(guard.fileno(), msvcrt.LK_LOCK, 1)
            try:
                yield
            finally:
                guard.seek(0)
                msvcrt.locking(guard.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            fcntl = cast(_FcntlModule, importlib.import_module("fcntl"))

            fcntl.flock(guard.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(guard.fileno(), fcntl.LOCK_UN)


def durable_append_bytes(path: Path, body: bytes) -> None:
    """追加并在返回前 fsync；调用方只可在返回后推进 durable 水位。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    with exclusive_path_lock(path):
        with path.open("ab") as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
