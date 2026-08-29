"""品种级跨进程在途独占锁（T-05）。

同品种同一时刻至多一笔在途写请求：单本意图账本内由账本守卫，
多进程各持独立账本（主账本、shadow 账本、paper 账本）时以本
模块的文件锁扩展到进程之间。锁文件位于数据根
`execution/.inflight/<symbol>.lock`，机制参照
`data.durable_io.exclusive_path_lock`：msvcrt 或 fcntl 锁随
进程句柄释放，不依赖删除锁文件判断存活。取不到锁立即返回空，
不阻塞等待，由发送编排按闸门拒绝。零写发送路径不经本模块。
"""
from __future__ import annotations

import importlib
import os
from pathlib import Path
from typing import IO, Protocol, cast

from guvolu.data.paths import data_root
from guvolu.domain.symbols import SpotSymbol

# 数据根下的锁目录（C-04）
INFLIGHT_LOCK_RELATIVE_DIR = Path("execution") / ".inflight"


class _FcntlModule(Protocol):
    LOCK_EX: int
    LOCK_NB: int
    LOCK_UN: int

    def flock(self, fd: int, operation: int) -> None: ...


class SymbolInFlightLock:
    """已取得的品种锁；release 解锁并关闭句柄。"""

    def __init__(self, handle: IO[bytes]) -> None:
        self._handle: IO[bytes] | None = handle

    def release(self) -> None:
        """解锁并关闭句柄，重复调用无害。"""
        handle = self._handle
        if handle is None:
            return
        self._handle = None
        try:
            if os.name == "nt":
                import msvcrt

                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl = cast(
                    _FcntlModule, importlib.import_module("fcntl")
                )
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()


def acquire_symbol_inflight_lock(
    symbol: SpotSymbol, *, directory: Path | None = None
) -> SymbolInFlightLock | None:
    """非阻塞取品种独占锁；已被持有即返回空。"""
    base = (
        directory
        if directory is not None
        else data_root() / INFLIGHT_LOCK_RELATIVE_DIR
    )
    base.mkdir(parents=True, exist_ok=True)
    lock_path = base / f"{symbol}.lock"
    handle = lock_path.open("a+b")
    try:
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"\0")
            handle.flush()
        handle.seek(0)
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            fcntl = cast(_FcntlModule, importlib.import_module("fcntl"))
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        # 锁被占用或句柄异常，按取锁失败
        handle.close()
        return None
    return SymbolInFlightLock(handle)
