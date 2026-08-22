"""规划、复制并验证冷热存储迁移。"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import sys
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Literal, cast

from guvolu.data import store
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
            with source.open("rb") as incoming, temporary.open("wb", buffering=0) as outgoing:
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


def _set_route_status(
    data_root: Path,
    plan: MigrationPlan,
    *,
    from_status: str,
    to_status: str,
) -> str:
    """原子切换单个路由状态。"""
    config_path = data_root.resolve() / CONFIG_FILE_NAME
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


def rollback_plan(data_root: Path, plan_path: Path) -> dict[str, object]:
    """验证热副本后停用冷路由。"""
    root = data_root.resolve()
    plan = load_plan(plan_path)
    verify_plan(root, plan_path, side="hot")
    config_sha = _set_route_status(
        root, plan, from_status="active", to_status="planned",
    )
    resolver = StorageResolver(root)
    for item in plan.items:
        actual = resolver.resolve(item.logical_path)
        if not actual.is_file() or _sha256(actual) != item.sha256:
            raise StoragePathError(f"回滚后解析验证失败: {item.logical_path}")
    result: dict[str, object] = {
        "config_sha256": config_sha,
        "migration_id": plan.migration_id,
        "rolled_back_at": datetime.now(UTC).isoformat(),
        "status": "planned",
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
    for command in ("copy", "activate", "rollback"):
        child = subparsers.add_parser(command)
        child.add_argument("--plan", type=Path, required=True)
    release_parser = subparsers.add_parser("release-hot")
    release_parser.add_argument("--plan", type=Path, required=True)
    release_parser.add_argument("--apply", action="store_true")
    release_parser.add_argument("--confirm-migration-id")
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
            _json_result(rollback_plan(root, args.plan.resolve()))
        elif args.command == "release-hot":
            _json_result(release_hot_plan(
                root,
                args.plan.resolve(),
                apply=bool(args.apply),
                confirm_migration_id=args.confirm_migration_id,
            ))
    except (OSError, sqlite3.Error, StoragePathError, ValueError) as exc:
        parser.exit(2, f"cold_storage_error: {type(exc).__name__}: {exc}\n")


if __name__ == "__main__":
    main()
