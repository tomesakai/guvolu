"""为隔离的冻结预测运行根生成可审计的成交输入快照。"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
from pathlib import Path
from typing import Sequence

from guvolu.data.sqlite_writer_lock import sqlite_writer_lock
from guvolu.data.storage_paths import resolve_storage_path


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(8 * 1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _backup(source: Path, destination: Path) -> None:
    """单步在线备份：一次读事务内整库复制，不需静默生产写者。

    分步备份会被并发提交反复重启，故此前持生产写锁约十三分钟
    （2026-09-02 实测，3.2 GB），拖垮所有物化器；单步备份在 WAL
    下与写者并行，快照仍是一致的读事务视图。临时库关闭回滚日志
    与同步落盘：它随后整体原子替换，中途失败即弃；开日志时逐页
    写放大使 3.2 GB 备份耗时约七分半，关闭后约十秒（同日实测）。
    """
    source_conn = sqlite3.connect(f"file:{source}?mode=ro", uri=True)
    destination_conn = sqlite3.connect(destination)
    try:
        destination_conn.execute("PRAGMA journal_mode=OFF")
        destination_conn.execute("PRAGMA synchronous=OFF")
        source_conn.backup(destination_conn, pages=-1)
    finally:
        destination_conn.close()
        source_conn.close()


def _remove_sidecars(database: Path) -> None:
    """清理备份临时库遗留的 WAL 伴随文件。"""
    for suffix in ("-shm", "-wal", "-journal"):
        database.with_name(database.name + suffix).unlink(missing_ok=True)


def _active_inputs(
    snapshot: Path, market_id: str,
) -> tuple[tuple[str, str, int], ...]:
    conn = sqlite3.connect(f"file:{snapshot}?mode=ro", uri=True)
    try:
        conn.execute("PRAGMA query_only=ON")
        rows = conn.execute(
            "SELECT DISTINCT a.storage_path,a.sha256,a.byte_count "
            "FROM materialization_partition_head h "
            "JOIN materialization_output o ON o.attempt_id=h.attempt_id "
            "JOIN artifact a ON a.artifact_id=o.artifact_id "
            "WHERE h.market_id=? AND h.domain IN ('trade','trade_realtime') "
            "AND o.dataset='trade_observation' ORDER BY a.storage_path",
            (market_id,),
        ).fetchall()
    finally:
        conn.close()
    return tuple((str(row[0]), str(row[1]), int(row[2])) for row in rows)


def _copy_verified(
    source: Path,
    destination: Path,
    digest: str,
    *,
    verify_reused: bool,
) -> str:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        if destination.stat().st_size != source.stat().st_size:
            raise ValueError(f"既有冻结输入字节数不符: {destination}")
        # 复用件散列复核走全量模式
        if verify_reused and _sha256(destination) != digest:
            raise ValueError(f"既有冻结输入散列不符: {destination}")
        return "reused"
    if source.drive.casefold() == destination.drive.casefold():
        os.link(source, destination)
        method = "hardlinked"
    else:
        temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
        try:
            with source.open("rb") as reader, temporary.open("xb") as writer:
                shutil.copyfileobj(reader, writer, 8 * 1024 * 1024)
                writer.flush()
                os.fsync(writer.fileno())
            os.replace(temporary, destination)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
        method = "copied"
    if destination.stat().st_size != source.stat().st_size:
        raise ValueError(f"冻结输入字节数校验失败: {destination}")
    if _sha256(destination) != digest:
        raise ValueError(f"冻结输入散列校验失败: {destination}")
    return method


def refresh_runtime(
    source_data_root: Path,
    runtime_root: Path,
    market_id: str,
    *,
    verify_all: bool = False,
) -> dict[str, object]:
    """冻结一代活动成交头，先备齐制品再原子替换 SQLite 快照。

    每小时缺省只校验新落地制品与外键闭合；`verify_all` 追加复用件
    散列复核与整库 quick_check（3.2 GB 约九十秒），留给日常复核。
    """
    source_root = source_data_root.resolve()
    runtime = runtime_root.resolve()
    runtime_data = (runtime / "data").resolve()
    runtime_data.mkdir(parents=True, exist_ok=True)
    if not runtime_data.is_relative_to(runtime):
        raise ValueError("冻结数据根越界")
    source_db = source_root / "guvolu.sqlite3"
    if not source_db.is_file():
        raise FileNotFoundError(f"源控制库不存在: {source_db}")
    temporary_db = runtime_data / f".guvolu.refresh.{os.getpid()}.sqlite3"
    final_db = runtime_data / "guvolu.sqlite3"
    temporary_db.unlink(missing_ok=True)
    _remove_sidecars(temporary_db)
    counters = {"copied": 0, "hardlinked": 0, "reused": 0}
    total_bytes = 0
    quick_check = "skipped"
    with sqlite_writer_lock(runtime_data, timeout_seconds=120.0):
        try:
            # 单步备份不持生产写锁
            _backup(source_db, temporary_db)
            inputs = _active_inputs(temporary_db, market_id)
            if not inputs:
                raise LookupError(f"市场没有活动成交输出: {market_id}")
            for recorded, expected_sha, expected_bytes in inputs:
                if len(expected_sha) != 64:
                    raise ValueError(f"制品 SHA-256 非法: {recorded}")
                source = resolve_storage_path(source_root, recorded).resolve()
                destination = (runtime_data / Path(recorded)).resolve()
                if not destination.is_relative_to(runtime_data):
                    raise ValueError(f"冻结输入路径越界: {recorded}")
                if source.suffix.lower() != ".parquet" or not source.is_file():
                    raise FileNotFoundError(f"活动成交制品缺失: {recorded}")
                if source.stat().st_size != expected_bytes:
                    raise ValueError(f"源制品字节数不符: {recorded}")
                # 源散列只在落地时校验
                if (
                    (verify_all or not destination.exists())
                    and _sha256(source) != expected_sha
                ):
                    raise ValueError(f"源制品散列不符: {recorded}")
                method = _copy_verified(
                    source, destination, expected_sha,
                    verify_reused=verify_all,
                )
                counters[method] += 1
                total_bytes += expected_bytes
            check = sqlite3.connect(f"file:{temporary_db}?mode=ro", uri=True)
            try:
                if verify_all:
                    quick_check = str(
                        check.execute("PRAGMA quick_check").fetchone()[0]
                    )
                foreign_key_errors = len(check.execute(
                    "PRAGMA foreign_key_check"
                ).fetchall())
            finally:
                check.close()
            if quick_check not in ("ok", "skipped") or foreign_key_errors:
                raise ValueError("冻结控制库完整性校验失败")
            os.replace(temporary_db, final_db)
        except BaseException:
            temporary_db.unlink(missing_ok=True)
            raise
        finally:
            _remove_sidecars(temporary_db)
    return {
        "market_id": market_id,
        "snapshot_path": str(final_db),
        "inputs": sum(counters.values()),
        "input_bytes": total_bytes,
        "methods": counters,
        "quick_check": quick_check,
        "foreign_key_errors": 0,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="刷新冻结预测运行根")
    parser.add_argument("--source-data-root", type=Path, required=True)
    parser.add_argument("--runtime-root", type=Path, required=True)
    parser.add_argument("--market-id", required=True)
    parser.add_argument(
        "--verify-all", action="store_true",
        help="复核复用件散列并整库 quick_check（日常复核用）",
    )
    args = parser.parse_args(argv)
    print(json.dumps(refresh_runtime(
        args.source_data_root, args.runtime_root, str(args.market_id),
        verify_all=bool(args.verify_all),
    ), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
