"""规划、复制并验证冷热存储迁移。"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import sys
import tempfile
from contextlib import ExitStack
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any, Literal, cast

import duckdb

from guvolu.data import store
from guvolu.data.book_l2_contract import (
    BOOK_L2_FRAME_DATASET,
    BOOK_L2_LEVEL_DATASET,
    BOOK_L2_NORMALIZATION_VERSION,
    BOOK_L2_SCHEMA_VERSION,
    create_book_l2_tables,
)
from guvolu.data.durable_io import exclusive_path_lock
from guvolu.data.materialize import (
    ArchiveInput,
    ArchiveMonthKey,
    SourceArtifact,
    rebuild_archive_trade_month_parquet,
)
from guvolu.data.okx_l2_archive import OKX_BOOK_HISTORY_ENDPOINT
from guvolu.data.okx_l2_materialize import (
    OKX_PAYLOAD_SCHEMA_VERSION,
    OkxArchiveInput,
    _copy_csv as _okx_copy_csv,
    _stage as _okx_stage,
    _validate_tables as _okx_validate_tables,
    _write_parquet as _okx_write_parquet,
    sealed_input as okx_sealed_input,
)
from guvolu.data.projection import ArchivePartition
from guvolu.data.sqlite_writer_lock import sqlite_writer_lock
from guvolu.data.storage_paths import (
    CONFIG_FILE_NAME,
    StoragePathError,
    StorageResolver,
    StorageRouteSpec,
    storage_resolver,
    storage_status,
)


PLAN_SCHEMA_VERSION = 1
PLAN_DIRECTORY = Path("migrations") / "cold"
COPY_BLOCK_BYTES = 8 * 1024 * 1024


@dataclass(frozen=True)
class MigrationItem:
    """保存一个待迁移位置的内容证据。"""

    logical_path: str
    artifact_id: str
    artifact_kind: str
    sha256: str
    byte_count: int
    schema_version: int


@dataclass(frozen=True)
class MigrationPlan:
    """冻结一个逻辑前缀的迁移输入集合。"""

    schema_version: int
    migration_id: str
    created_at: str
    data_root: str
    storage_root_id: str
    logical_prefix: str
    physical_prefix: str
    item_count: int
    total_bytes: int
    input_set_sha256: str
    items: tuple[MigrationItem, ...]


def _canonical_bytes(value: object) -> bytes:
    """生成稳定 JSON 字节。"""
    return (
        json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        + "\n"
    ).encode("utf-8")


def _sha256(path: Path) -> str:
    """逐块计算文件散列。"""
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(COPY_BLOCK_BYTES):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_json(path: Path, value: object) -> None:
    """原子持久化控制 JSON。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    payload = _canonical_bytes(value)
    with temporary.open("wb", buffering=0) as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def _route(
    resolver: StorageResolver,
    logical_prefix: str,
    *,
    statuses: set[str],
) -> StorageRouteSpec:
    """读取唯一的指定路由。"""
    normalized = PurePosixPath(logical_prefix.replace("\\", "/")).as_posix()
    found = [
        row for row in resolver.routes
        if row.logical_prefix.as_posix() == normalized and row.status in statuses
    ]
    if len(found) != 1:
        raise StoragePathError(
            f"逻辑前缀未唯一命中路由: {logical_prefix} status={sorted(statuses)}"
        )
    return found[0]


def _root_path(resolver: StorageResolver, route: StorageRouteSpec) -> Path:
    """读取并验证路由的物理前缀。"""
    roots = {
        row.spec.storage_root_id: row for row in resolver.verify_all_roots()
    }
    root = roots[route.storage_root_id].resolved_path
    physical = root.joinpath(*route.physical_prefix.parts).resolve()
    if not physical.is_relative_to(root):
        raise StoragePathError(f"路由物理前缀逃逸: {route.logical_prefix}")
    return physical


def _catalog_items(data_root: Path, logical_prefix: str) -> tuple[MigrationItem, ...]:
    """从只读控制面冻结前缀内全部位置。"""
    conn = store.connect_readonly(data_root)
    if conn is None:
        raise FileNotFoundError(data_root / store.DB_FILE_NAME)
    prefix = logical_prefix.rstrip("/")
    try:
        conn.execute("PRAGMA query_only=ON")
        conn.execute("BEGIN")
        rows = conn.execute(
            "SELECT l.storage_path,l.artifact_id,a.artifact_kind,a.sha256,"
            "a.byte_count,a.schema_version FROM artifact_location l "
            "JOIN artifact a ON a.artifact_id=l.artifact_id "
            "WHERE l.storage_path=? OR substr(l.storage_path,1,?)=? "
            "ORDER BY l.storage_path",
            (prefix, len(prefix) + 1, prefix + "/"),
        ).fetchall()
    finally:
        conn.close()
    if not rows:
        raise StoragePathError(f"逻辑前缀没有已登记制品: {prefix}")
    return tuple(MigrationItem(
        logical_path=str(row[0]),
        artifact_id=str(row[1]),
        artifact_kind=str(row[2]),
        sha256=str(row[3]),
        byte_count=int(row[4]),
        schema_version=int(row[5]),
    ) for row in rows)


def _source_path(data_root: Path, logical_path: str) -> Path:
    """解析迁移前的热物理路径。"""
    parts = PurePosixPath(logical_path).parts
    path = data_root.resolve().joinpath(*parts).resolve()
    if not path.is_relative_to(data_root.resolve()):
        raise StoragePathError(f"热来源路径逃逸: {logical_path}")
    return path


def _target_path(
    physical_prefix: Path,
    logical_prefix: str,
    logical_path: str,
) -> Path:
    """生成冷根内的目标路径。"""
    prefix = PurePosixPath(logical_prefix)
    logical = PurePosixPath(logical_path)
    suffix = logical.relative_to(prefix)
    target = physical_prefix.joinpath(*suffix.parts).resolve()
    if not target.is_relative_to(physical_prefix.resolve()):
        raise StoragePathError(f"冷目标路径逃逸: {logical_path}")
    return target


def _plan_identity(
    route: StorageRouteSpec, items: tuple[MigrationItem, ...],
) -> tuple[str, str]:
    """计算冻结输入集合与迁移身份。"""
    input_body = [
        {
            "artifact_id": row.artifact_id,
            "byte_count": row.byte_count,
            "logical_path": row.logical_path,
            "sha256": row.sha256,
        }
        for row in items
    ]
    input_sha = hashlib.sha256(_canonical_bytes(input_body)).hexdigest()
    identity_body = {
        "input_set_sha256": input_sha,
        "logical_prefix": route.logical_prefix.as_posix(),
        "physical_prefix": route.physical_prefix.as_posix(),
        "storage_root_id": route.storage_root_id,
    }
    digest = hashlib.sha256(_canonical_bytes(identity_body)).hexdigest()
    return f"cold-migration-{digest}", input_sha


def _plan_path(data_root: Path, migration_id: str) -> Path:
    """生成迁移计划路径。"""
    return data_root.resolve() / PLAN_DIRECTORY / f"{migration_id}.plan.json"


def _progress_path(plan_path: Path) -> Path:
    """生成复制进度路径。"""
    return plan_path.with_name(plan_path.name.replace(".plan.json", ".progress.json"))


def _receipt_path(plan_path: Path, phase: str) -> Path:
    """生成阶段回执路径。"""
    return plan_path.with_name(plan_path.name.replace(".plan.json", f".{phase}.json"))


def create_plan(data_root: Path, logical_prefix: str) -> tuple[MigrationPlan, Path]:
    """冻结迁移集合并写入计划。"""
    root = data_root.resolve()
    resolver = storage_resolver(root)
    route = _route(resolver, logical_prefix, statuses={"planned"})
    _root_path(resolver, route)
    items = _catalog_items(root, route.logical_prefix.as_posix())
    catalog_paths = {row.logical_path for row in items}
    source_root = _source_path(root, route.logical_prefix.as_posix())
    if not source_root.is_dir():
        raise StoragePathError(f"热来源前缀不存在: {route.logical_prefix}")
    physical_paths = {
        PurePosixPath(route.logical_prefix, path.relative_to(source_root).as_posix())
        .as_posix()
        for path in source_root.rglob("*") if path.is_file()
    }
    extras = sorted(physical_paths - catalog_paths)
    missing = sorted(catalog_paths - physical_paths)
    if extras or missing:
        raise StoragePathError(
            f"迁移集合与物理前缀不闭合: extras={len(extras)} missing={len(missing)}"
        )
    for item in items:
        source = _source_path(root, item.logical_path)
        if source.stat().st_size != item.byte_count:
            raise StoragePathError(f"来源字节数不符: {item.logical_path}")
    migration_id, input_sha = _plan_identity(route, items)
    plan = MigrationPlan(
        schema_version=PLAN_SCHEMA_VERSION,
        migration_id=migration_id,
        created_at=datetime.now(UTC).isoformat(),
        data_root=str(root),
        storage_root_id=route.storage_root_id,
        logical_prefix=route.logical_prefix.as_posix(),
        physical_prefix=route.physical_prefix.as_posix(),
        item_count=len(items),
        total_bytes=sum(row.byte_count for row in items),
        input_set_sha256=input_sha,
        items=items,
    )
    path = _plan_path(root, migration_id)
    body = asdict(plan)
    if path.is_file():
        existing = load_plan(path)
        if asdict(existing) != body:
            raise StoragePathError(f"同迁移身份的计划内容冲突: {migration_id}")
    else:
        _atomic_json(path, body)
    return plan, path


def load_plan(path: Path) -> MigrationPlan:
    """读取并复算迁移计划。"""
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise StoragePathError(f"不能读取迁移计划: {exc}") from exc
    if not isinstance(raw, dict) or raw.get("schema_version") != PLAN_SCHEMA_VERSION:
        raise StoragePathError("迁移计划版本不受支持")
    raw_items = raw.get("items")
    if not isinstance(raw_items, list):
        raise StoragePathError("迁移计划缺少 items")
    try:
        items = tuple(MigrationItem(
            logical_path=str(row["logical_path"]),
            artifact_id=str(row["artifact_id"]),
            artifact_kind=str(row["artifact_kind"]),
            sha256=str(row["sha256"]),
            byte_count=int(row["byte_count"]),
            schema_version=int(row["schema_version"]),
        ) for row in raw_items if isinstance(row, dict))
        plan = MigrationPlan(
            schema_version=int(raw["schema_version"]),
            migration_id=str(raw["migration_id"]),
            created_at=str(raw["created_at"]),
            data_root=str(raw["data_root"]),
            storage_root_id=str(raw["storage_root_id"]),
            logical_prefix=str(raw["logical_prefix"]),
            physical_prefix=str(raw["physical_prefix"]),
            item_count=int(raw["item_count"]),
            total_bytes=int(raw["total_bytes"]),
            input_set_sha256=str(raw["input_set_sha256"]),
            items=items,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise StoragePathError(f"迁移计划字段非法: {exc}") from exc
    if len(items) != len(raw_items) or plan.item_count != len(items):
        raise StoragePathError("迁移计划 item_count 不符")
    route = StorageRouteSpec(
        logical_prefix=PurePosixPath(plan.logical_prefix),
        storage_root_id=plan.storage_root_id,
        physical_prefix=PurePosixPath(plan.physical_prefix),
        status="planned",
    )
    expected_id, expected_input = _plan_identity(route, items)
    if plan.migration_id != expected_id or plan.input_set_sha256 != expected_input:
        raise StoragePathError("迁移计划身份复算不符")
    if plan.total_bytes != sum(row.byte_count for row in items):
        raise StoragePathError("迁移计划 total_bytes 不符")
    return plan


def _validate_catalog(plan: MigrationPlan, data_root: Path) -> None:
    """确认计划仍与当前控制面一致。"""
    current = _catalog_items(data_root, plan.logical_prefix)
    route = StorageRouteSpec(
        logical_prefix=PurePosixPath(plan.logical_prefix),
        storage_root_id=plan.storage_root_id,
        physical_prefix=PurePosixPath(plan.physical_prefix),
        status="planned",
    )
    current_id, current_input = _plan_identity(route, current)
    if current_id != plan.migration_id or current_input != plan.input_set_sha256:
        raise StoragePathError("控制面输入集合已经变化，必须重新规划")


def copy_plan(data_root: Path, plan_path: Path) -> dict[str, object]:
    """可断点复制并逐文件验证迁移计划。"""
    root = data_root.resolve()
    plan = load_plan(plan_path)
    if Path(plan.data_root) != root:
        raise StoragePathError("迁移计划数据根不符")
    _validate_catalog(plan, root)
    resolver = storage_resolver(root)
    route = _route(resolver, plan.logical_prefix, statuses={"planned"})
    if route.storage_root_id != plan.storage_root_id or (
        route.physical_prefix.as_posix() != plan.physical_prefix
    ):
        raise StoragePathError("迁移计划与当前路由不符")
    physical_prefix = _root_path(resolver, route)
    progress_path = _progress_path(plan_path)
    completed: dict[str, str] = {}
    if progress_path.is_file():
        body = json.loads(progress_path.read_text(encoding="utf-8"))
        if not isinstance(body, dict) or body.get("migration_id") != plan.migration_id:
            raise StoragePathError("复制进度身份不符")
        raw_completed = body.get("completed")
        if isinstance(raw_completed, dict):
            completed = {
                str(key): str(value) for key, value in raw_completed.items()
            }
    copied_bytes = 0
    reused_bytes = 0
    report_step = max(1, plan.item_count // 20)
    for index, item in enumerate(plan.items, start=1):
        source = _source_path(root, item.logical_path)
        if source.stat().st_size != item.byte_count or _sha256(source) != item.sha256:
            raise StoragePathError(f"来源制品验证失败: {item.logical_path}")
        target = _target_path(
            physical_prefix, plan.logical_prefix, item.logical_path,
        )
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.is_file():
            if target.stat().st_size != item.byte_count or _sha256(target) != item.sha256:
                raise StoragePathError(f"目标已存在但身份不符: {item.logical_path}")
            reused_bytes += item.byte_count
        else:
            temporary = target.with_name(f".{target.name}.{plan.migration_id}.partial")
            temporary.unlink(missing_ok=True)
            with source.open("rb") as incoming, temporary.open("xb", buffering=0) as outgoing:
                shutil.copyfileobj(incoming, outgoing, COPY_BLOCK_BYTES)
                outgoing.flush()
                os.fsync(outgoing.fileno())
            if temporary.stat().st_size != item.byte_count or (
                _sha256(temporary) != item.sha256
            ):
                raise StoragePathError(f"目标临时文件验证失败: {item.logical_path}")
            os.replace(temporary, target)
            copied_bytes += item.byte_count
        completed[item.logical_path] = item.sha256
        if index == plan.item_count or index % report_step == 0:
            _atomic_json(progress_path, {
                "completed": completed,
                "migration_id": plan.migration_id,
                "updated_at": datetime.now(UTC).isoformat(),
            })
            print(json.dumps({
                "copied_or_reused_items": index,
                "migration_id": plan.migration_id,
                "percent": round(index * 100 / plan.item_count, 2),
                "total_items": plan.item_count,
            }, sort_keys=True), file=sys.stderr, flush=True)
    result: dict[str, object] = {
        "completed_items": len(completed),
        "copied_bytes": copied_bytes,
        "migration_id": plan.migration_id,
        "reused_bytes": reused_bytes,
        "total_bytes": plan.total_bytes,
    }
    _atomic_json(_receipt_path(plan_path, "copied"), {
        **result,
        "completed_at": datetime.now(UTC).isoformat(),
    })
    return result


def verify_plan(
    data_root: Path,
    plan_path: Path,
    *,
    side: Literal["hot", "cold", "both"] = "both",
) -> dict[str, object]:
    """逐文件复核迁移来源和目标。"""
    root = data_root.resolve()
    plan = load_plan(plan_path)
    _validate_catalog(plan, root)
    resolver = storage_resolver(root)
    route = _route(
        resolver, plan.logical_prefix, statuses={"planned", "active"},
    )
    physical_prefix = _root_path(resolver, route)
    checked_bytes = 0
    for item in plan.items:
        candidates: list[Path] = []
        if side in {"hot", "both"}:
            candidates.append(_source_path(root, item.logical_path))
        if side in {"cold", "both"}:
            candidates.append(_target_path(
                physical_prefix, plan.logical_prefix, item.logical_path,
            ))
        for candidate in candidates:
            if not candidate.is_file() or candidate.stat().st_size != item.byte_count:
                raise StoragePathError(f"迁移文件缺失或字节不符: {candidate}")
            if _sha256(candidate) != item.sha256:
                raise StoragePathError(f"迁移文件散列不符: {candidate}")
            checked_bytes += item.byte_count
    result: dict[str, object] = {
        "checked_bytes": checked_bytes,
        "checked_sides": side,
        "item_count": plan.item_count,
        "migration_id": plan.migration_id,
        "status": "verified",
    }
    _atomic_json(_receipt_path(plan_path, f"verified-{side}"), {
        **result,
        "verified_at": datetime.now(UTC).isoformat(),
    })
    return result


def _assert_no_active_overlap(body: dict[str, object]) -> None:
    """落盘前拦截活动路由前缀重叠，避免坏配置瘫痪全部解析。"""
    routes = body.get("routes")
    if not isinstance(routes, list):
        raise StoragePathError("存储配置结构非法")
    active: list[PurePosixPath] = []
    for raw in cast(list[object], routes):
        if not isinstance(raw, dict) or raw.get("status") != "active":
            continue
        prefix_value = raw.get("logical_prefix")
        if not isinstance(prefix_value, str):
            raise StoragePathError("路由 logical_prefix 非法")
        logical = PurePosixPath(prefix_value)
        for existing in active:
            if logical == existing or logical.is_relative_to(existing) or (
                existing.is_relative_to(logical)
            ):
                raise StoragePathError(f"活动路由前缀重叠: {logical}")
        active.append(logical)


def _set_route_status(
    data_root: Path,
    plan: MigrationPlan,
    *,
    from_status: str,
    to_status: str,
) -> str:
    """原子切换单个路由状态。"""
    root = data_root.resolve()
    config_path = root / CONFIG_FILE_NAME
    # 跨进程互斥防并发丢更新
    with sqlite_writer_lock(root):
        body = json.loads(config_path.read_text(encoding="utf-8"))
        if not isinstance(body, dict) or not isinstance(body.get("routes"), list):
            raise StoragePathError("存储配置结构非法")
        matched = 0
        for raw in cast(list[object], body["routes"]):
            if not isinstance(raw, dict):
                continue
            if raw.get("logical_prefix") == plan.logical_prefix:
                if raw.get("storage_root_id") != plan.storage_root_id or (
                    raw.get("physical_prefix") != plan.physical_prefix
                ):
                    raise StoragePathError("迁移计划与路由身份不符")
                if raw.get("status") != from_status:
                    raise StoragePathError(
                        f"路由状态不是 {from_status}: {plan.logical_prefix}"
                    )
                raw["status"] = to_status
                matched += 1
        if matched != 1:
            raise StoragePathError("路由未唯一命中")
        _assert_no_active_overlap(body)
        _atomic_json(config_path, body)
        return hashlib.sha256(config_path.read_bytes()).hexdigest()


def activate_plan(data_root: Path, plan_path: Path) -> dict[str, object]:
    """完整验证后启用冷路由。"""
    root = data_root.resolve()
    plan = load_plan(plan_path)
    verify_plan(root, plan_path, side="both")
    config_sha = _set_route_status(
        root, plan, from_status="planned", to_status="active",
    )
    resolver = StorageResolver(root)
    for item in plan.items:
        actual = resolver.resolve(item.logical_path)
        if not actual.is_file() or _sha256(actual) != item.sha256:
            raise StoragePathError(f"启用后解析验证失败: {item.logical_path}")
    result: dict[str, object] = {
        "activated_at": datetime.now(UTC).isoformat(),
        "config_sha256": config_sha,
        "migration_id": plan.migration_id,
        "status": "active",
    }
    _atomic_json(_receipt_path(plan_path, "activated"), result)
    return result


def _active_head_artifact_ids(data_root: Path, logical_prefix: str) -> set[str]:
    """只读列出前缀内仍被活动 head 引用的制品。"""
    conn = store.connect_readonly(data_root)
    if conn is None:
        raise FileNotFoundError(data_root / store.DB_FILE_NAME)
    prefix = logical_prefix.rstrip("/")
    try:
        conn.execute("PRAGMA query_only=ON")
        rows = conn.execute(
            "SELECT o.artifact_id FROM materialization_partition_head h "
            "JOIN materialization_output o ON o.attempt_id=h.attempt_id "
            "JOIN artifact a ON a.artifact_id=o.artifact_id "
            "WHERE a.storage_path=? OR substr(a.storage_path,1,?)=?",
            (prefix, len(prefix) + 1, prefix + "/"),
        ).fetchall()
    finally:
        conn.close()
    return {str(row[0]) for row in rows}


def rollback_plan(
    data_root: Path,
    plan_path: Path,
    *,
    allow_missing_superseded: bool = False,
) -> dict[str, object]:
    """验证热副本后停用冷路由；可容忍非活动 head 制品缺失。"""
    root = data_root.resolve()
    plan = load_plan(plan_path)
    superseded_missing: list[str] = []
    if allow_missing_superseded:
        # 不触碰冷根，只验证热层
        _validate_catalog(plan, root)
        active = _active_head_artifact_ids(root, plan.logical_prefix)
        for item in plan.items:
            hot = _source_path(root, item.logical_path)
            if not hot.exists():
                if item.artifact_id in active:
                    raise StoragePathError(
                        f"活动 head 制品热副本缺失: {item.logical_path}"
                    )
                superseded_missing.append(item.logical_path)
                continue
            if not hot.is_file() or hot.stat().st_size != item.byte_count or (
                _sha256(hot) != item.sha256
            ):
                raise StoragePathError(f"热副本验证失败: {item.logical_path}")
    else:
        verify_plan(root, plan_path, side="hot")
    config_sha = _set_route_status(
        root, plan, from_status="active", to_status="planned",
    )
    resolver = StorageResolver(root)
    skipped = set(superseded_missing)
    for item in plan.items:
        if item.logical_path in skipped:
            continue
        actual = resolver.resolve(item.logical_path)
        if not actual.is_file() or _sha256(actual) != item.sha256:
            raise StoragePathError(f"回滚后解析验证失败: {item.logical_path}")
    result: dict[str, object] = {
        "config_sha256": config_sha,
        "migration_id": plan.migration_id,
        "rolled_back_at": datetime.now(UTC).isoformat(),
        "status": "planned",
        "superseded_missing": superseded_missing,
        "superseded_missing_items": len(superseded_missing),
    }
    _atomic_json(_receipt_path(plan_path, "rolled-back"), result)
    return result


def release_hot_plan(
    data_root: Path,
    plan_path: Path,
    *,
    apply: bool = False,
    confirm_migration_id: str | None = None,
) -> dict[str, object]:
    """只释放已有等字节冷副本的热 Parquet。"""
    root = data_root.resolve()
    plan = load_plan(plan_path)
    _validate_catalog(plan, root)
    resolver = storage_resolver(root)
    route = _route(resolver, plan.logical_prefix, statuses={"active"})
    physical_prefix = _root_path(resolver, route)
    candidates = tuple(
        item for item in plan.items
        if item.artifact_kind == "materialized_parquet"
        and item.logical_path.endswith(".parquet")
    )
    if not candidates:
        raise StoragePathError("迁移计划没有可释放的热 Parquet")
    if apply and confirm_migration_id != plan.migration_id:
        raise StoragePathError("释放热副本必须精确确认 migration_id")
    released_items = released_bytes = absent_items = 0
    releasable_bytes = 0
    for item in candidates:
        cold = _target_path(
            physical_prefix, plan.logical_prefix, item.logical_path,
        )
        resolved = resolver.resolve(item.logical_path)
        if resolved != cold or not cold.is_file():
            raise StoragePathError(f"活动路由未指向冷副本: {item.logical_path}")
        if cold.stat().st_size != item.byte_count or _sha256(cold) != item.sha256:
            raise StoragePathError(f"冷副本验证失败: {item.logical_path}")
        hot = _source_path(root, item.logical_path)
        if not hot.exists():
            absent_items += 1
            continue
        if not hot.is_file() or hot.stat().st_size != item.byte_count:
            raise StoragePathError(f"热副本字节验证失败: {item.logical_path}")
        if _sha256(hot) != item.sha256:
            raise StoragePathError(f"热副本散列验证失败: {item.logical_path}")
        releasable_bytes += item.byte_count
        if apply:
            hot.unlink()
            released_items += 1
            released_bytes += item.byte_count
    result: dict[str, object] = {
        "absent_items": absent_items,
        "apply": apply,
        "candidate_items": len(candidates),
        "migration_id": plan.migration_id,
        "releasable_bytes": releasable_bytes,
        "released_bytes": released_bytes,
        "released_items": released_items,
        "retained_non_parquet_items": plan.item_count - len(candidates),
        "status": "released" if apply else "planned",
    }
    if apply:
        _atomic_json(_receipt_path(plan_path, "hot-released"), {
            **result,
            "released_at": datetime.now(UTC).isoformat(),
        })
    return result


def restore_hot_plan(
    data_root: Path,
    plan_path: Path,
    *,
    apply: bool = False,
) -> dict[str, object]:
    """由已验证冷副本恢复缺失的热 Parquet，供回滚或热读取。"""
    root = data_root.resolve()
    plan = load_plan(plan_path)
    _validate_catalog(plan, root)
    resolver = storage_resolver(root)
    route = _route(resolver, plan.logical_prefix, statuses={"active", "planned"})
    physical_prefix = _root_path(resolver, route)
    candidates = tuple(
        item for item in plan.items
        if item.artifact_kind == "materialized_parquet"
        and item.logical_path.endswith(".parquet")
    )
    if not candidates:
        raise StoragePathError("迁移计划没有可恢复的热 Parquet")
    restored_items = restored_bytes = present_items = 0
    restorable_bytes = 0
    for item in candidates:
        cold = _target_path(
            physical_prefix, plan.logical_prefix, item.logical_path,
        )
        if not cold.is_file():
            raise StoragePathError(f"冷副本缺失: {item.logical_path}")
        if cold.stat().st_size != item.byte_count or _sha256(cold) != item.sha256:
            raise StoragePathError(f"冷副本验证失败: {item.logical_path}")
        hot = _source_path(root, item.logical_path)
        if hot.exists():
            if not hot.is_file() or hot.stat().st_size != item.byte_count or (
                _sha256(hot) != item.sha256
            ):
                raise StoragePathError(f"热副本存在但身份不符: {item.logical_path}")
            present_items += 1
            continue
        restorable_bytes += item.byte_count
        if not apply:
            continue
        hot.parent.mkdir(parents=True, exist_ok=True)
        temporary = hot.with_name(f".{hot.name}.{plan.migration_id}.restore")
        temporary.unlink(missing_ok=True)
        with cold.open("rb") as incoming, temporary.open("xb", buffering=0) as outgoing:
            shutil.copyfileobj(incoming, outgoing, COPY_BLOCK_BYTES)
            outgoing.flush()
            os.fsync(outgoing.fileno())
        if temporary.stat().st_size != item.byte_count or (
            _sha256(temporary) != item.sha256
        ):
            temporary.unlink(missing_ok=True)
            raise StoragePathError(f"热恢复临时文件验证失败: {item.logical_path}")
        os.replace(temporary, hot)
        restored_items += 1
        restored_bytes += item.byte_count
    result: dict[str, object] = {
        "apply": apply,
        "candidate_items": len(candidates),
        "migration_id": plan.migration_id,
        "present_items": present_items,
        "restorable_bytes": restorable_bytes,
        "restored_bytes": restored_bytes,
        "restored_items": restored_items,
        "status": "restored" if apply else "planned",
    }
    if apply:
        _atomic_json(_receipt_path(plan_path, "hot-restored"), {
            **result,
            "restored_at": datetime.now(UTC).isoformat(),
        })
    return result


@dataclass(frozen=True)
class _RawRebuildSource:
    """一个 Parquet 项反查得到的重算来源。"""

    attempt_id: str
    key: ArchiveMonthKey
    inputs: tuple[ArchiveInput, ...]


@dataclass(frozen=True)
class _OkxL2OutputBinding:
    """一个计划项对应的 OKX L2 输出血缘。"""

    item: MigrationItem
    attempt_id: str
    dataset: str
    market_id: str
    partition_key: str
    venue_symbol: str
    instrument_id: str
    mapping_revision: int
    config_hash: str


@dataclass(frozen=True)
class _OkxL2RebuildSource:
    """一个 OKX L2 日级重算单元。"""

    attempt_id: str
    market_id: str
    instrument_id: str
    mapping_revision: int
    capability_revision: int
    require_full_day: bool
    archive: OkxArchiveInput
    outputs: tuple[_OkxL2OutputBinding, ...]


def _okx_l2_output_binding(
    conn: sqlite3.Connection,
    item: MigrationItem,
) -> _OkxL2OutputBinding | None:
    """识别计划项是否为唯一完成态 OKX L2 输出。"""
    rows = conn.execute(
        "SELECT p.attempt_id,p.market_id,p.partition_key,"
        "p.normalization_version,p.status,o.dataset,m.venue_id,"
        "m.venue_symbol,m.instrument_id,m.mapping_revision,p.config_hash "
        "FROM materialization_output o "
        "JOIN partition_attempt p ON p.attempt_id=o.attempt_id "
        "JOIN market m ON m.market_id=p.market_id "
        "WHERE o.artifact_id=? AND p.domain='book_l2' "
        "ORDER BY p.finished_at DESC,p.attempt_id",
        (item.artifact_id,),
    ).fetchall()
    if not rows:
        return None
    attempts = {str(row[0]) for row in rows}
    if len(attempts) != 1:
        raise StoragePathError(
            f"OKX L2 输出血缘不唯一: {item.logical_path}"
        )
    row = rows[0]
    if str(row[4]) != "complete":
        raise StoragePathError(
            f"OKX L2 输出不是完成态: {item.logical_path}"
        )
    if str(row[6]) != "okx":
        raise StoragePathError(
            f"book_l2 原始恢复仅支持 OKX: {item.logical_path}"
        )
    dataset = str(row[5])
    if dataset not in {BOOK_L2_FRAME_DATASET, BOOK_L2_LEVEL_DATASET}:
        raise StoragePathError(f"OKX L2 输出数据集非法: {dataset}")
    if str(row[3]) != BOOK_L2_NORMALIZATION_VERSION:
        raise StoragePathError(
            f"OKX L2 规范化版本不受支持: {row[3]}"
        )
    return _OkxL2OutputBinding(
        item=item,
        attempt_id=str(row[0]),
        market_id=str(row[1]),
        partition_key=str(row[2]),
        dataset=dataset,
        venue_symbol=str(row[7]),
        instrument_id=str(row[8]),
        mapping_revision=int(row[9]),
        config_hash=str(row[10]),
    )


def _raw_rebuild_units(
    conn: sqlite3.Connection,
    candidates: tuple[MigrationItem, ...],
) -> tuple[
    tuple[tuple[MigrationItem, ...], ...],
    dict[str, _OkxL2OutputBinding],
]:
    """把 OKX 双输出合并为不可拆分的日级工作单元。"""
    units: list[list[MigrationItem]] = []
    unit_by_attempt: dict[str, int] = {}
    bindings: dict[str, _OkxL2OutputBinding] = {}
    for item in candidates:
        binding = _okx_l2_output_binding(conn, item)
        if binding is None:
            units.append([item])
            continue
        bindings[item.artifact_id] = binding
        position = unit_by_attempt.get(binding.attempt_id)
        if position is None:
            unit_by_attempt[binding.attempt_id] = len(units)
            units.append([item])
        else:
            units[position].append(item)
    return tuple(tuple(unit) for unit in units), bindings


def _okx_l2_rebuild_source(
    conn: sqlite3.Connection,
    root: Path,
    outputs: tuple[_OkxL2OutputBinding, ...],
) -> _OkxL2RebuildSource:
    """核对一个日级双输出及其封口 raw 输入。"""
    if not outputs:
        raise StoragePathError("OKX L2 日级输出为空")
    first = outputs[0]
    if any(row.attempt_id != first.attempt_id for row in outputs):
        raise StoragePathError("OKX L2 日级输出尝试不一致")
    by_dataset = {row.dataset: row for row in outputs}
    expected_datasets = {BOOK_L2_FRAME_DATASET, BOOK_L2_LEVEL_DATASET}
    if len(by_dataset) != len(outputs) or set(by_dataset) != expected_datasets:
        raise StoragePathError(
            f"OKX L2 计划未成对登记双输出: {first.attempt_id}"
        )
    registered = conn.execute(
        "SELECT dataset,artifact_id FROM materialization_output "
        "WHERE attempt_id=? AND dataset IN (?,?) ORDER BY dataset",
        (
            first.attempt_id,
            BOOK_L2_FRAME_DATASET,
            BOOK_L2_LEVEL_DATASET,
        ),
    ).fetchall()
    expected_outputs = {
        row.dataset: row.item.artifact_id for row in outputs
    }
    actual_outputs = {str(row[0]): str(row[1]) for row in registered}
    if len(registered) != 2 or actual_outputs != expected_outputs:
        raise StoragePathError(
            f"OKX L2 计划与尝试双输出不一致: {first.attempt_id}"
        )
    if any(
        row.market_id != first.market_id
        or row.partition_key != first.partition_key
        or row.venue_symbol != first.venue_symbol
        or row.instrument_id != first.instrument_id
        or row.mapping_revision != first.mapping_revision
        or row.config_hash != first.config_hash
        for row in outputs
    ):
        raise StoragePathError(
            f"OKX L2 日级输出元数据不一致: {first.attempt_id}"
        )
    capability_rows = conn.execute(
        "SELECT venue_id,domain,endpoint,revision_id,binding_basis "
        "FROM partition_capability_binding WHERE attempt_id=?",
        (first.attempt_id,),
    ).fetchall()
    if len(capability_rows) != 1:
        raise StoragePathError(
            f"OKX L2 能力绑定不唯一: {first.attempt_id}"
        )
    capability = capability_rows[0]
    if (
        str(capability[0]) != "okx"
        or str(capability[1]) != "book_history"
        or str(capability[2]) != OKX_BOOK_HISTORY_ENDPOINT
        or str(capability[4]) != "recorded"
    ):
        raise StoragePathError(
            f"OKX L2 能力绑定不受支持: {first.attempt_id}"
        )
    raw_rows = conn.execute(
        "SELECT b.artifact_id,b.storage_path,a.artifact_kind,a.sha256,"
        "a.byte_count FROM partition_input_binding b "
        "JOIN artifact a ON a.artifact_id=b.artifact_id "
        "WHERE b.attempt_id=? ORDER BY b.storage_path",
        (first.attempt_id,),
    ).fetchall()
    if len(raw_rows) != 1:
        raise StoragePathError(
            f"OKX L2 尝试必须唯一绑定 raw 日档: {first.attempt_id}"
        )
    raw = raw_rows[0]
    raw_path = _source_path(root, str(raw[1]))
    if str(raw[2]) != "source_archive":
        raise StoragePathError(f"OKX L2 输入不是 raw 归档: {raw[1]}")
    if not raw_path.is_file():
        raise StoragePathError(f"OKX L2 raw 日档热副本缺失: {raw[1]}")
    if raw_path.stat().st_size != int(raw[4]) or _sha256(raw_path) != str(raw[3]):
        raise StoragePathError(f"OKX L2 raw 日档与登记不符: {raw[1]}")
    manifest_storage = f"{raw[1]}.manifest.json"
    manifest_rows = conn.execute(
        "SELECT a.artifact_kind,a.sha256,a.byte_count "
        "FROM artifact_location l JOIN artifact a "
        "ON a.artifact_id=l.artifact_id WHERE l.storage_path=?",
        (manifest_storage,),
    ).fetchall()
    if len(manifest_rows) != 1 or str(manifest_rows[0][0]) != "source_manifest":
        raise StoragePathError(
            f"OKX L2 raw manifest 未唯一登记: {manifest_storage}"
        )
    manifest_path = _source_path(root, manifest_storage)
    if not manifest_path.is_file():
        raise StoragePathError(
            f"OKX L2 raw manifest 热副本缺失: {manifest_storage}"
        )
    if (
        manifest_path.stat().st_size != int(manifest_rows[0][2])
        or _sha256(manifest_path) != str(manifest_rows[0][1])
    ):
        raise StoragePathError(
            f"OKX L2 raw manifest 与登记不符: {manifest_storage}"
        )
    try:
        archive = okx_sealed_input(root, manifest_path)
    except (OSError, UnicodeError, ValueError, KeyError) as exc:
        raise StoragePathError(
            f"OKX L2 raw manifest 不能封口复核: {manifest_storage}: {exc}"
        ) from exc
    if archive is None:
        raise StoragePathError(
            f"OKX L2 raw manifest 不是完成态: {manifest_storage}"
        )
    if (
        archive.artifact.artifact_id != str(raw[0])
        or archive.artifact.storage_path != str(raw[1])
        or archive.venue_symbol != first.venue_symbol
        or archive.day != first.partition_key
    ):
        raise StoragePathError(
            f"OKX L2 raw 输入与尝试身份不一致: {first.attempt_id}"
        )
    matching_modes: list[bool] = []
    for require_full_day in (False, True):
        config_body = {
            "dataset": [BOOK_L2_FRAME_DATASET, BOOK_L2_LEVEL_DATASET],
            "normalization_version": BOOK_L2_NORMALIZATION_VERSION,
            "schema_version": BOOK_L2_SCHEMA_VERSION,
            "payload_schema_version": OKX_PAYLOAD_SCHEMA_VERSION,
            "depth_limit": archive.depth_limit,
            "require_full_day": require_full_day,
        }
        config_hash = hashlib.sha256(
            json.dumps(config_body, sort_keys=True).encode()
        ).hexdigest()
        if config_hash == first.config_hash:
            matching_modes.append(require_full_day)
    if len(matching_modes) != 1:
        raise StoragePathError(
            f"OKX L2 原始物化配置不可复算: {first.attempt_id}"
        )
    return _OkxL2RebuildSource(
        attempt_id=first.attempt_id,
        market_id=first.market_id,
        instrument_id=first.instrument_id,
        mapping_revision=first.mapping_revision,
        capability_revision=int(capability[3]),
        require_full_day=matching_modes[0],
        archive=archive,
        outputs=tuple(sorted(outputs, key=lambda row: row.dataset)),
    )


def _rebuild_okx_l2_day(
    source: _OkxL2RebuildSource,
    temporary: Path,
) -> dict[str, Path]:
    """只在日级临时目录重放并写出双 Parquet。"""
    frame_csv = temporary / "frames.csv"
    level_csv = temporary / "levels.csv"
    frame_parquet = temporary / "frame.parquet"
    level_parquet = temporary / "level.parquet"
    profile = _okx_stage(
        source.archive,
        market_id=source.market_id,
        mapping_revision=source.mapping_revision,
        capability_revision=source.capability_revision,
        instrument_id=source.instrument_id,
        frame_csv=frame_csv,
        level_csv=level_csv,
        require_full_day=source.require_full_day,
    )
    db: Any = duckdb.connect(":memory:")
    try:
        db.execute("SET TimeZone='UTC'")
        create_book_l2_tables(db)
        _okx_copy_csv(db, BOOK_L2_FRAME_DATASET, frame_csv)
        _okx_copy_csv(db, BOOK_L2_LEVEL_DATASET, level_csv)
        _okx_validate_tables(
            db,
            profile=profile,
            market_id=source.market_id,
            artifact_identity=source.archive.artifact.artifact_id,
        )
        _okx_write_parquet(
            db,
            f"SELECT * FROM {BOOK_L2_FRAME_DATASET} "
            "ORDER BY event_time,frame_id",
            frame_parquet,
        )
        _okx_write_parquet(
            db,
            f"SELECT * FROM {BOOK_L2_LEVEL_DATASET} "
            "ORDER BY frame_id,side,source_level_index",
            level_parquet,
        )
    finally:
        db.close()
    return {
        BOOK_L2_FRAME_DATASET: frame_parquet,
        BOOK_L2_LEVEL_DATASET: level_parquet,
    }


def _rebuild_source(
    conn: sqlite3.Connection, root: Path, item: MigrationItem,
) -> _RawRebuildSource:
    """由控制面反查完成态尝试与热层 raw 输入并逐个核验。"""
    attempt = conn.execute(
        "SELECT p.attempt_id,p.market_id,p.partition_key,p.normalization_version "
        "FROM materialization_output o "
        "JOIN partition_attempt p ON p.attempt_id=o.attempt_id "
        "WHERE o.artifact_id=? AND p.domain='trade' "
        "AND p.status IN ('complete','complete_with_rejections') "
        "ORDER BY p.finished_at DESC LIMIT 1",
        (item.artifact_id,),
    ).fetchone()
    if attempt is None:
        raise StoragePathError("没有完成态物化尝试输出该制品")
    attempt_id = str(attempt[0])
    market = conn.execute(
        "SELECT venue_id,venue_symbol,instrument_id,mapping_revision "
        "FROM market WHERE market_id=?",
        (str(attempt[1]),),
    ).fetchone()
    if market is None:
        raise StoragePathError(f"尝试市场未登记: {attempt[1]}")
    venue_id, venue_symbol = str(market[0]), str(market[1])
    rows = conn.execute(
        "SELECT b.artifact_id,b.storage_path,b.source_rows,a.artifact_kind,"
        "a.sha256,a.byte_count,a.sealed_at FROM partition_input_binding b "
        "JOIN artifact a ON a.artifact_id=b.artifact_id "
        "WHERE b.attempt_id=? ORDER BY b.storage_path",
        (attempt_id,),
    ).fetchall()
    if not rows:
        # 旧尝试无位置绑定时退回内容台账
        rows = conn.execute(
            "SELECT i.artifact_id,a.storage_path,i.source_rows,a.artifact_kind,"
            "a.sha256,a.byte_count,a.sealed_at FROM partition_input i "
            "JOIN artifact a ON a.artifact_id=i.artifact_id "
            "WHERE i.attempt_id=? ORDER BY a.storage_path",
            (attempt_id,),
        ).fetchall()
    if not rows:
        raise StoragePathError(f"尝试没有输入原件: {attempt_id}")
    inputs: list[ArchiveInput] = []
    for row in rows:
        storage_path = str(row[1])
        if str(row[3]) != "source_archive":
            raise StoragePathError(f"输入不是 raw 归档: {storage_path}")
        path = _source_path(root, storage_path)
        if not path.is_file():
            raise StoragePathError(f"raw 归档热副本缺失: {storage_path}")
        if path.stat().st_size != int(row[5]) or _sha256(path) != str(row[4]):
            raise StoragePathError(f"raw 归档与登记不符: {storage_path}")
        partition = ArchivePartition(venue_id, venue_symbol, path.name[:8], path)
        artifact = SourceArtifact(
            str(row[0]), storage_path, path, int(row[2]), 0, 0,
        )
        # 摄取时刻取登记封口时刻
        inputs.append(ArchiveInput(partition, artifact, str(row[6])))
    key = ArchiveMonthKey(
        venue_id=venue_id,
        venue_symbol=venue_symbol,
        market_id=str(attempt[1]),
        instrument_id=str(market[2]),
        mapping_revision=int(market[3]),
        event_month=str(attempt[2]),
        normalization_version=str(attempt[3]),
    )
    return _RawRebuildSource(attempt_id, key, tuple(inputs))


@dataclass(frozen=True)
class _RawRestoreResult:
    """一个重算单元的恢复计数与拒绝证据。"""

    present_items: int
    restorable_bytes: int
    restored_items: int
    restored_bytes: int
    mismatched: tuple[dict[str, object], ...]
    failures: tuple[dict[str, object], ...]


@dataclass(frozen=True)
class _OkxL2Landing:
    """一个已验证 OKX L2 临时输出及其热目标。"""

    item: MigrationItem
    rebuilt_path: Path
    hot_path: Path


def _land_okx_l2_outputs(
    landings: tuple[_OkxL2Landing, ...],
) -> tuple[int, int, tuple[dict[str, object], ...]]:
    """持锁落盘双输出，并补偿可恢复的中途失败。"""
    ordered = tuple(sorted(
        landings, key=lambda row: row.hot_path.as_posix().casefold(),
    ))
    failures: list[dict[str, object]] = []
    landed: list[_OkxL2Landing] = []
    lock_target: _OkxL2Landing | None = None
    failure_phase = "lock"
    try:
        with ExitStack() as locks:
            for lock_target in ordered:
                locks.enter_context(exclusive_path_lock(lock_target.hot_path))
            failure_phase = "landing_precondition"
            for row in ordered:
                if not row.hot_path.exists():
                    continue
                if (
                    row.hot_path.is_file()
                    and row.hot_path.stat().st_size == row.item.byte_count
                    and _sha256(row.hot_path) == row.item.sha256
                ):
                    reason = "热恢复目标在持锁重验时已由并发恢复"
                else:
                    reason = "热恢复目标在持锁重验时出现且身份不符"
                failures.append({
                    "logical_path": row.item.logical_path,
                    "phase": "landing_precondition",
                    "reason": reason,
                })
            if failures:
                failure_phase = "lock_release"
                return 0, 0, tuple(failures)
            failure_phase = "landing"
            for row in ordered:
                try:
                    os.replace(row.rebuilt_path, row.hot_path)
                except OSError as exc:
                    failures.append({
                        "logical_path": row.item.logical_path,
                        "phase": "landing",
                        "reason": f"{type(exc).__name__}: {exc}",
                    })
                    retained = list(landed)
                    for previous in reversed(landed):
                        try:
                            os.replace(
                                previous.hot_path,
                                previous.rebuilt_path,
                            )
                        except OSError as rollback_exc:
                            failures.append({
                                "logical_path": previous.item.logical_path,
                                "phase": "rollback",
                                "reason": (
                                    f"{type(rollback_exc).__name__}: "
                                    f"{rollback_exc}"
                                ),
                            })
                        else:
                            retained.remove(previous)
                    landed = retained
                    failure_phase = "lock_release"
                    return (
                        len(retained),
                        sum(row.item.byte_count for row in retained),
                        tuple(failures),
                    )
                landed.append(row)
            failure_phase = "lock_release"
    except OSError as exc:
        logical_path = (
            "" if lock_target is None else lock_target.item.logical_path
        )
        failures.append({
            "logical_path": logical_path,
            "phase": failure_phase,
            "reason": f"{type(exc).__name__}: {exc}",
        })
        return (
            len(landed),
            sum(row.item.byte_count for row in landed),
            tuple(failures),
        )
    return (
        len(ordered),
        sum(row.item.byte_count for row in ordered),
        (),
    )


def _missing_hot_items(
    root: Path,
    items: tuple[MigrationItem, ...],
) -> tuple[int, tuple[MigrationItem, ...]]:
    """复核已有热副本并返回缺失项。"""
    present = 0
    missing: list[MigrationItem] = []
    for item in items:
        hot = _source_path(root, item.logical_path)
        if not hot.exists():
            missing.append(item)
            continue
        if not hot.is_file() or hot.stat().st_size != item.byte_count or (
            _sha256(hot) != item.sha256
        ):
            raise StoragePathError(
                f"热副本存在但身份不符: {item.logical_path}"
            )
        present += 1
    return present, tuple(missing)


def _raw_failure_rows(
    items: tuple[MigrationItem, ...],
    reason: str,
) -> tuple[dict[str, object], ...]:
    """为一组未恢复项生成稳定失败证据。"""
    rows: list[dict[str, object]] = []
    for item in items:
        rows.append({
            "logical_path": item.logical_path,
            "reason": reason,
        })
    return tuple(rows)


def _restore_trade_from_raw(
    conn: sqlite3.Connection,
    root: Path,
    plan: MigrationPlan,
    item: MigrationItem,
    *,
    apply: bool,
) -> _RawRestoreResult:
    """保持原有逐笔月档恢复语义。"""
    present, missing = _missing_hot_items(root, (item,))
    if not missing:
        return _RawRestoreResult(present, 0, 0, 0, (), ())
    try:
        source = _rebuild_source(conn, root, item)
    except StoragePathError as exc:
        return _RawRestoreResult(0, 0, 0, 0, (), ({
            "logical_path": item.logical_path,
            "reason": str(exc),
        },))
    if not apply:
        return _RawRestoreResult(0, item.byte_count, 0, 0, (), ())
    hot = _source_path(root, item.logical_path)
    hot.parent.mkdir(parents=True, exist_ok=True)
    temp_dir = Path(tempfile.mkdtemp(
        prefix=f".{hot.name}.{plan.migration_id}.rebuild-",
        dir=hot.parent,
    ))
    try:
        rebuilt = rebuild_archive_trade_month_parquet(
            root, source.inputs, source.key, temp_dir,
        )
        actual_bytes = rebuilt.stat().st_size
        actual_sha = _sha256(rebuilt)
        if actual_bytes != item.byte_count or actual_sha != item.sha256:
            return _RawRestoreResult(0, item.byte_count, 0, 0, ({
                "actual_byte_count": actual_bytes,
                "actual_sha256": actual_sha,
                "attempt_id": source.attempt_id,
                "expected_byte_count": item.byte_count,
                "expected_sha256": item.sha256,
                "logical_path": item.logical_path,
            },), ())
        os.replace(rebuilt, hot)
    except (OSError, ValueError, sqlite3.Error, duckdb.Error) as exc:
        return _RawRestoreResult(0, item.byte_count, 0, 0, (), ({
            "logical_path": item.logical_path,
            "reason": f"{type(exc).__name__}: {exc}",
        },))
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
    return _RawRestoreResult(
        0, item.byte_count, 1, item.byte_count, (), (),
    )


def _restore_okx_l2_from_raw(
    conn: sqlite3.Connection,
    root: Path,
    plan: MigrationPlan,
    bindings: tuple[_OkxL2OutputBinding, ...],
    *,
    apply: bool,
) -> _RawRestoreResult:
    """成对复算一个 OKX L2 日并门禁恢复。"""
    items = tuple(row.item for row in bindings)
    present, missing = _missing_hot_items(root, items)
    if not missing:
        return _RawRestoreResult(present, 0, 0, 0, (), ())
    try:
        source = _okx_l2_rebuild_source(conn, root, bindings)
    except StoragePathError as exc:
        source_failures = _raw_failure_rows(missing, str(exc))
        return _RawRestoreResult(
            present, 0, 0, 0, (), source_failures,
        )
    parents = {
        _source_path(root, row.item.logical_path).parent for row in bindings
    }
    if len(parents) != 1:
        parent_failures = _raw_failure_rows(
            missing, "OKX L2 日级双输出不在同一目录",
        )
        return _RawRestoreResult(
            present, 0, 0, 0, (), parent_failures,
        )
    parent = next(iter(parents))
    parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(
        prefix=f".okx-l2-{source.attempt_id}-",
        suffix=".rebuild",
        dir=parent,
    ))
    try:
        rebuilt = _rebuild_okx_l2_day(source, temporary)
        actual: dict[str, tuple[Path, int, str]] = {}
        for dataset, path in rebuilt.items():
            actual[dataset] = (path, path.stat().st_size, _sha256(path))
        mismatched: list[dict[str, object]] = []
        for binding in source.outputs:
            _, actual_bytes, actual_sha = actual[binding.dataset]
            if (
                actual_bytes != binding.item.byte_count
                or actual_sha != binding.item.sha256
            ):
                mismatched.append({
                    "actual_byte_count": actual_bytes,
                    "actual_sha256": actual_sha,
                    "attempt_id": source.attempt_id,
                    "dataset": binding.dataset,
                    "expected_byte_count": binding.item.byte_count,
                    "expected_sha256": binding.item.sha256,
                    "logical_path": binding.item.logical_path,
                })
        if mismatched:
            # 任一不等时整日双输出均不落盘
            return _RawRestoreResult(
                present, 0, 0, 0, tuple(mismatched), (),
            )
        restorable = sum(item.byte_count for item in missing)
        if not apply:
            return _RawRestoreResult(
                present, restorable, 0, 0, (), (),
            )
        missing_ids = {item.artifact_id for item in missing}
        landings: list[_OkxL2Landing] = []
        for binding in source.outputs:
            if binding.item.artifact_id not in missing_ids:
                continue
            landings.append(_OkxL2Landing(
                item=binding.item,
                rebuilt_path=actual[binding.dataset][0],
                hot_path=_source_path(root, binding.item.logical_path),
            ))
        restored_items, restored_bytes, landing_failures = (
            _land_okx_l2_outputs(tuple(landings))
        )
        return _RawRestoreResult(
            present,
            restorable,
            restored_items,
            restored_bytes,
            (),
            landing_failures,
        )
    except (
        OSError,
        UnicodeError,
        ValueError,
        KeyError,
        sqlite3.Error,
        duckdb.Error,
    ) as exc:
        rebuild_failures = _raw_failure_rows(
            missing, f"{type(exc).__name__}: {exc}",
        )
        return _RawRestoreResult(
            present, 0, 0, 0, (), rebuild_failures,
        )
    finally:
        shutil.rmtree(temporary, ignore_errors=True)


def restore_hot_from_raw_plan(
    data_root: Path,
    plan_path: Path,
    *,
    apply: bool = False,
    shard: tuple[int, int] | None = None,
) -> dict[str, object]:
    """不读冷盘，由热层 raw 归档重算并恢复缺失的热 Parquet。"""
    root = data_root.resolve()
    plan = load_plan(plan_path)
    _validate_catalog(plan, root)
    resolver = storage_resolver(root)
    _route(resolver, plan.logical_prefix, statuses={"active", "planned"})
    all_candidates = tuple(
        item for item in plan.items
        if item.artifact_kind == "materialized_parquet"
        and item.logical_path.endswith(".parquet")
    )
    if not all_candidates:
        raise StoragePathError("迁移计划没有可恢复的热 Parquet")
    conn = store.connect_readonly(root)
    if conn is None:
        raise FileNotFoundError(root / store.DB_FILE_NAME)
    restored_items = restored_bytes = present_items = 0
    restorable_bytes = 0
    mismatched: list[dict[str, object]] = []
    failures: list[dict[str, object]] = []
    okx_l2_days = trade_items = 0
    try:
        conn.execute("PRAGMA query_only=ON")
        units, okx_bindings = _raw_rebuild_units(conn, all_candidates)
        if shard is not None:
            shard_index, shard_count = shard
            if shard_count <= 0 or not 0 <= shard_index < shard_count:
                raise StoragePathError(
                    f"分片参数非法: {shard_index}/{shard_count}"
                )
            # OKX 双输出按日保持在同一分片
            units = tuple(
                unit for position, unit in enumerate(units)
                if position % shard_count == shard_index
            )
        if not units:
            raise StoragePathError("迁移计划没有可恢复的热 Parquet")
        candidates = tuple(item for unit in units for item in unit)
        processed_items = 0
        report_step = max(1, len(candidates) // 20)
        next_report = report_step
        for unit in units:
            bindings = tuple(
                okx_bindings[item.artifact_id]
                for item in unit if item.artifact_id in okx_bindings
            )
            if bindings:
                if len(bindings) != len(unit):
                    raise StoragePathError("原始恢复工作单元域混合")
                unit_result = _restore_okx_l2_from_raw(
                    conn, root, plan, bindings, apply=apply,
                )
                okx_l2_days += 1
            else:
                if len(unit) != 1:
                    raise StoragePathError("逐笔恢复工作单元必须为单项")
                unit_result = _restore_trade_from_raw(
                    conn, root, plan, unit[0], apply=apply,
                )
                trade_items += 1
            present_items += unit_result.present_items
            restorable_bytes += unit_result.restorable_bytes
            restored_items += unit_result.restored_items
            restored_bytes += unit_result.restored_bytes
            mismatched.extend(unit_result.mismatched)
            failures.extend(unit_result.failures)
            processed_items += len(unit)
            if processed_items >= next_report or processed_items == len(candidates):
                print(json.dumps({
                    "failed_items": len(failures),
                    "migration_id": plan.migration_id,
                    "mismatched_items": len(mismatched),
                    "percent": round(
                        processed_items * 100 / len(candidates), 2,
                    ),
                    "restored_items": restored_items,
                    "total_items": len(candidates),
                }, sort_keys=True), file=sys.stderr, flush=True)
                next_report += report_step
    finally:
        conn.close()
    if not apply:
        status = "blocked" if mismatched or failures else "planned"
    elif mismatched or failures:
        status = "partial"
    else:
        status = "restored"
    landing_failure_count = sum(
        row.get("phase") in {
            "lock", "lock_release", "landing_precondition", "landing",
        }
        for row in failures
    )
    rollback_failure_count = sum(
        row.get("phase") == "rollback" for row in failures
    )
    result: dict[str, object] = {
        "apply": apply,
        "candidate_items": len(candidates),
        "failed_items": len(failures),
        "failures": failures,
        "landing_failure_count": landing_failure_count,
        "migration_id": plan.migration_id,
        "mismatched": mismatched,
        "mismatched_items": len(mismatched),
        "okx_l2_days": okx_l2_days,
        "present_items": present_items,
        "shard": None if shard is None else [shard[0], shard[1]],
        "restorable_bytes": restorable_bytes,
        "restored_bytes": restored_bytes,
        "restored_items": restored_items,
        "rollback_failure_count": rollback_failure_count,
        "source": "raw",
        "status": status,
        "trade_items": trade_items,
    }
    if apply:
        receipt_phase = "hot-restored-from-raw" if shard is None else (
            f"hot-restored-from-raw-{shard[0]}of{shard[1]}"
        )
        _atomic_json(_receipt_path(plan_path, receipt_phase), {
            **result,
            "restored_at": datetime.now(UTC).isoformat(),
        })
    return result


def _json_result(value: object) -> None:
    """输出稳定 JSON。"""
    print(json.dumps(value, ensure_ascii=False, sort_keys=True))


def main() -> None:
    """运行冷热存储迁移命令。"""
    parser = argparse.ArgumentParser(description="冷热存储迁移")
    parser.add_argument("--data-root", type=Path, default=Path("data"))
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("status")
    plan_parser = subparsers.add_parser("plan")
    plan_parser.add_argument("--logical-prefix", required=True)
    for command in ("copy", "activate"):
        child = subparsers.add_parser(command)
        child.add_argument("--plan", type=Path, required=True)
    rollback_parser = subparsers.add_parser("rollback")
    rollback_parser.add_argument("--plan", type=Path, required=True)
    rollback_parser.add_argument(
        "--allow-missing-superseded", action="store_true",
    )
    release_parser = subparsers.add_parser("release-hot")
    release_parser.add_argument("--plan", type=Path, required=True)
    release_parser.add_argument("--apply", action="store_true")
    release_parser.add_argument("--confirm-migration-id")
    restore_parser = subparsers.add_parser("restore-hot")
    restore_parser.add_argument("--plan", type=Path, required=True)
    restore_parser.add_argument("--apply", action="store_true")
    restore_parser.add_argument("--from-raw", action="store_true")
    restore_parser.add_argument("--shard", default=None)
    verify_parser = subparsers.add_parser("verify")
    verify_parser.add_argument("--plan", type=Path, required=True)
    verify_parser.add_argument(
        "--side", choices=("hot", "cold", "both"), default="both",
    )
    args = parser.parse_args()
    root = args.data_root.resolve()
    try:
        if args.command == "status":
            _json_result(storage_status(root))
        elif args.command == "plan":
            plan, path = create_plan(root, str(args.logical_prefix))
            _json_result({
                "item_count": plan.item_count,
                "migration_id": plan.migration_id,
                "plan_path": str(path),
                "total_bytes": plan.total_bytes,
            })
        elif args.command == "copy":
            _json_result(copy_plan(root, args.plan.resolve()))
        elif args.command == "verify":
            _json_result(verify_plan(
                root, args.plan.resolve(),
                side=cast(Literal["hot", "cold", "both"], args.side),
            ))
        elif args.command == "activate":
            _json_result(activate_plan(root, args.plan.resolve()))
        elif args.command == "rollback":
            _json_result(rollback_plan(
                root,
                args.plan.resolve(),
                allow_missing_superseded=bool(args.allow_missing_superseded),
            ))
        elif args.command == "release-hot":
            _json_result(release_hot_plan(
                root,
                args.plan.resolve(),
                apply=bool(args.apply),
                confirm_migration_id=args.confirm_migration_id,
            ))
        elif args.command == "restore-hot" and args.from_raw:
            shard: tuple[int, int] | None = None
            if args.shard is not None:
                index_text, count_text = str(args.shard).split("/", 1)
                shard = (int(index_text), int(count_text))
            _json_result(restore_hot_from_raw_plan(
                root, args.plan.resolve(), apply=bool(args.apply), shard=shard,
            ))
        elif args.command == "restore-hot":
            _json_result(restore_hot_plan(
                root, args.plan.resolve(), apply=bool(args.apply),
            ))
    except (OSError, sqlite3.Error, StoragePathError, ValueError) as exc:
        parser.exit(2, f"cold_storage_error: {type(exc).__name__}: {exc}\n")


if __name__ == "__main__":
    main()
