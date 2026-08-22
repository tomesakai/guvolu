"""验证存储根并解析冷热逻辑路径。"""
from __future__ import annotations

import ctypes
import hashlib
import json
import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path, PurePosixPath
from typing import Any, Literal, cast


CONFIG_FILE_NAME = "storage-roots.json"
MARKER_FILE_NAME = ".guvolu-storage-root.json"
CONFIG_SCHEMA_VERSION = 1


class StoragePathError(ValueError):
    """表示存储身份或路径不符合合同。"""


@dataclass(frozen=True)
class StorageRootSpec:
    """描述一个物理存储根。"""

    storage_root_id: str
    role: Literal["cold", "hot_bulk", "test"]
    mount_path: Path
    marker_sha256: str
    volume_guid: str | None
    partition_guid: str | None
    volume_label: str | None
    filesystem: str | None


@dataclass(frozen=True)
class StorageRouteSpec:
    """把一个逻辑前缀映射到物理存储根。"""

    logical_prefix: PurePosixPath
    storage_root_id: str
    physical_prefix: PurePosixPath
    status: Literal["planned", "active", "retired"]


@dataclass(frozen=True)
class VerifiedStorageRoot:
    """保存已经通过身份校验的存储根。"""

    spec: StorageRootSpec
    resolved_path: Path
    marker: dict[str, object]


def _safe_relative(value: str, field: str) -> PurePosixPath:
    """校验可移植的相对逻辑路径。"""
    normalized = value.replace("\\", "/").strip("/")
    path = PurePosixPath(normalized)
    if not normalized or path.is_absolute() or any(
        part in {"", ".", ".."} for part in path.parts
    ):
        raise StoragePathError(f"{field} 不是安全相对路径: {value!r}")
    return path


def _sha256(path: Path) -> str:
    """逐块计算文件散列。"""
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _windows_volume_guid(path: Path) -> str | None:
    """读取路径所在卷的稳定 GUID。"""
    if os.name != "nt":
        return None
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    volume_path = ctypes.create_unicode_buffer(1024)
    get_path = kernel32.GetVolumePathNameW
    get_path.argtypes = [ctypes.c_wchar_p, ctypes.c_wchar_p, ctypes.c_uint32]
    get_path.restype = ctypes.c_int
    if not get_path(str(path), volume_path, len(volume_path)):
        raise StoragePathError(
            f"不能读取卷挂载点: Windows error {ctypes.get_last_error()}"
        )
    volume_name = ctypes.create_unicode_buffer(1024)
    get_name = kernel32.GetVolumeNameForVolumeMountPointW
    get_name.argtypes = [ctypes.c_wchar_p, ctypes.c_wchar_p, ctypes.c_uint32]
    get_name.restype = ctypes.c_int
    if not get_name(volume_path.value, volume_name, len(volume_name)):
        raise StoragePathError(
            f"不能读取卷 GUID: Windows error {ctypes.get_last_error()}"
        )
    return volume_name.value


def _windows_volume_info(path: Path) -> tuple[str, str] | None:
    """读取 Windows 卷标与文件系统。"""
    if os.name != "nt":
        return None
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    volume_path = ctypes.create_unicode_buffer(1024)
    get_path = kernel32.GetVolumePathNameW
    get_path.argtypes = [ctypes.c_wchar_p, ctypes.c_wchar_p, ctypes.c_uint32]
    get_path.restype = ctypes.c_int
    if not get_path(str(path), volume_path, len(volume_path)):
        raise StoragePathError(
            f"不能读取卷挂载点: Windows error {ctypes.get_last_error()}"
        )
    label = ctypes.create_unicode_buffer(1024)
    filesystem = ctypes.create_unicode_buffer(1024)
    get_info = kernel32.GetVolumeInformationW
    get_info.argtypes = [
        ctypes.c_wchar_p,
        ctypes.c_wchar_p,
        ctypes.c_uint32,
        ctypes.POINTER(ctypes.c_uint32),
        ctypes.POINTER(ctypes.c_uint32),
        ctypes.POINTER(ctypes.c_uint32),
        ctypes.c_wchar_p,
        ctypes.c_uint32,
    ]
    get_info.restype = ctypes.c_int
    serial = ctypes.c_uint32()
    maximum = ctypes.c_uint32()
    flags = ctypes.c_uint32()
    if not get_info(
        volume_path.value,
        label,
        len(label),
        ctypes.byref(serial),
        ctypes.byref(maximum),
        ctypes.byref(flags),
        filesystem,
        len(filesystem),
    ):
        raise StoragePathError(
            f"不能读取卷信息: Windows error {ctypes.get_last_error()}"
        )
    return label.value, filesystem.value


def _read_mapping(value: object, field: str) -> dict[str, object]:
    """收窄 JSON 对象类型。"""
    if not isinstance(value, dict) or not all(
        isinstance(key, str) for key in value
    ):
        raise StoragePathError(f"{field} 必须是 JSON 对象")
    return value


def _read_string(value: object, field: str) -> str:
    """读取非空字符串字段。"""
    if not isinstance(value, str) or not value:
        raise StoragePathError(f"{field} 必须是非空字符串")
    return value


def load_storage_specs(
    data_root: Path,
) -> tuple[tuple[StorageRootSpec, ...], tuple[StorageRouteSpec, ...]]:
    """读取数据根内的机器存储配置。"""
    config_path = data_root.resolve() / CONFIG_FILE_NAME
    if not config_path.is_file():
        return (), ()
    try:
        body = _read_mapping(
            json.loads(config_path.read_text(encoding="utf-8")), "配置",
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise StoragePathError(f"不能读取存储配置: {exc}") from exc
    if body.get("schema_version") != CONFIG_SCHEMA_VERSION:
        raise StoragePathError("存储配置版本不受支持")
    roots_raw = body.get("roots")
    routes_raw = body.get("routes")
    if not isinstance(roots_raw, list) or not isinstance(routes_raw, list):
        raise StoragePathError("存储配置缺少 roots 或 routes 数组")
    roots: list[StorageRootSpec] = []
    seen_roots: set[str] = set()
    for index, raw in enumerate(roots_raw):
        row = _read_mapping(raw, f"roots[{index}]")
        identity = _read_string(row.get("storage_root_id"), "storage_root_id")
        if identity in seen_roots:
            raise StoragePathError(f"存储根重复: {identity}")
        seen_roots.add(identity)
        role = row.get("role")
        if role not in {"cold", "hot_bulk", "test"}:
            raise StoragePathError(f"存储根角色非法: {identity}")
        marker_sha = _read_string(row.get("marker_sha256"), "marker_sha256")
        if len(marker_sha) != 64 or any(
            char not in "0123456789abcdef" for char in marker_sha
        ):
            raise StoragePathError(f"存储根 marker SHA 非法: {identity}")
        roots.append(StorageRootSpec(
            storage_root_id=identity,
            role=cast(Literal["cold", "hot_bulk", "test"], role),
            mount_path=Path(_read_string(row.get("mount_path"), "mount_path")),
            marker_sha256=marker_sha,
            volume_guid=cast(str, row["volume_guid"])
            if isinstance(row.get("volume_guid"), str) else None,
            partition_guid=cast(str, row["partition_guid"])
            if isinstance(row.get("partition_guid"), str) else None,
            volume_label=cast(str, row["volume_label"])
            if isinstance(row.get("volume_label"), str) else None,
            filesystem=cast(str, row["filesystem"])
            if isinstance(row.get("filesystem"), str) else None,
        ))
    routes: list[StorageRouteSpec] = []
    active_prefixes: list[PurePosixPath] = []
    for index, raw in enumerate(routes_raw):
        row = _read_mapping(raw, f"routes[{index}]")
        root_id = _read_string(row.get("storage_root_id"), "storage_root_id")
        if root_id not in seen_roots:
            raise StoragePathError(f"路由引用未知存储根: {root_id}")
        status = row.get("status")
        if status not in {"planned", "active", "retired"}:
            raise StoragePathError(f"路由状态非法: routes[{index}]")
        logical = _safe_relative(
            _read_string(row.get("logical_prefix"), "logical_prefix"),
            "logical_prefix",
        )
        physical = _safe_relative(
            _read_string(row.get("physical_prefix"), "physical_prefix"),
            "physical_prefix",
        )
        if status == "active":
            for existing in active_prefixes:
                if logical == existing or logical.is_relative_to(existing) or (
                    existing.is_relative_to(logical)
                ):
                    raise StoragePathError(f"活动路由前缀重叠: {logical}")
            active_prefixes.append(logical)
        routes.append(StorageRouteSpec(
            logical_prefix=logical,
            storage_root_id=root_id,
            physical_prefix=physical,
            status=cast(Literal["planned", "active", "retired"], status),
        ))
    return tuple(roots), tuple(routes)


def verify_storage_root(spec: StorageRootSpec) -> VerifiedStorageRoot:
    """验证物理根、哨兵和 Windows 卷身份。"""
    root = spec.mount_path.resolve(strict=True)
    if not root.is_dir():
        raise StoragePathError(f"存储根不是目录: {spec.mount_path}")
    marker_path = root / MARKER_FILE_NAME
    if not marker_path.is_file():
        raise StoragePathError(f"存储根缺少哨兵: {spec.storage_root_id}")
    actual_sha = _sha256(marker_path)
    if actual_sha != spec.marker_sha256:
        raise StoragePathError(f"存储根哨兵散列不符: {spec.storage_root_id}")
    try:
        marker = _read_mapping(
            json.loads(marker_path.read_text(encoding="utf-8")), "哨兵",
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise StoragePathError(f"不能读取存储根哨兵: {exc}") from exc
    required = {
        "storage_root_id": spec.storage_root_id,
        "role": spec.role,
    }
    for field, expected in required.items():
        if marker.get(field) != expected:
            raise StoragePathError(f"存储根哨兵字段不符: {field}")
    if spec.volume_guid is not None:
        current_guid = _windows_volume_guid(root)
        if current_guid is None or current_guid.casefold() != spec.volume_guid.casefold():
            raise StoragePathError(f"存储根卷 GUID 不符: {spec.storage_root_id}")
        if marker.get("volume_guid") != spec.volume_guid:
            raise StoragePathError("存储根哨兵 volume_guid 不符")
    if spec.partition_guid is not None and (
        marker.get("partition_guid") != spec.partition_guid
    ):
        raise StoragePathError("存储根哨兵 partition_guid 不符")
    volume_info = _windows_volume_info(root)
    if volume_info is not None:
        label, filesystem = volume_info
        if spec.volume_label is not None and label != spec.volume_label:
            raise StoragePathError(f"存储根卷标不符: {spec.storage_root_id}")
        if spec.filesystem is not None and filesystem.casefold() != (
            spec.filesystem.casefold()
        ):
            raise StoragePathError(f"存储根文件系统不符: {spec.storage_root_id}")
    return VerifiedStorageRoot(spec=spec, resolved_path=root, marker=marker)


class StorageResolver:
    """按已验证路由解析逻辑存储路径。"""

    def __init__(self, data_root: Path) -> None:
        self.data_root = data_root.resolve()
        roots, routes = load_storage_specs(self.data_root)
        self._root_specs = {row.storage_root_id: row for row in roots}
        self.routes = routes
        self._verified: dict[str, VerifiedStorageRoot] = {}

    def verify_all_roots(self) -> tuple[VerifiedStorageRoot, ...]:
        """验证配置内全部存储根。"""
        return tuple(self._verified_root(identity) for identity in self._root_specs)

    def _verified_root(self, identity: str) -> VerifiedStorageRoot:
        """按需验证并缓存根身份。"""
        found = self._verified.get(identity)
        if found is None:
            found = verify_storage_root(self._root_specs[identity])
            self._verified[identity] = found
        return found

    def _active_route(self, logical: PurePosixPath) -> StorageRouteSpec | None:
        """选择唯一活动路由。"""
        matches = [
            row for row in self.routes
            if row.status == "active" and (
                logical == row.logical_prefix
                or logical.is_relative_to(row.logical_prefix)
            )
        ]
        if not matches:
            return None
        return max(matches, key=lambda row: len(row.logical_prefix.parts))

    def resolve(self, recorded: str) -> Path:
        """把逻辑路径解析为经过身份校验的物理路径。"""
        logical = _safe_relative(recorded, "storage_path")
        route = self._active_route(logical)
        if route is None:
            resolved = self.data_root.joinpath(*logical.parts).resolve()
            if not resolved.is_relative_to(self.data_root):
                raise StoragePathError(f"热路径超出数据根: {recorded}")
            return resolved
        verified = self._verified_root(route.storage_root_id)
        suffix = logical.relative_to(route.logical_prefix)
        resolved = verified.resolved_path.joinpath(
            *route.physical_prefix.parts, *suffix.parts,
        ).resolve()
        expected_root = verified.resolved_path.joinpath(
            *route.physical_prefix.parts,
        ).resolve()
        if not resolved.is_relative_to(expected_root):
            raise StoragePathError(f"冷路径逃逸: {recorded}")
        return resolved

    def relative(self, path: Path) -> str:
        """把热路径或已登记冷路径还原为逻辑路径。"""
        resolved = path.resolve()
        if resolved.is_relative_to(self.data_root):
            return resolved.relative_to(self.data_root).as_posix()
        for route in self.routes:
            if route.status not in {"planned", "active"}:
                continue
            verified = self._verified_root(route.storage_root_id)
            physical_root = verified.resolved_path.joinpath(
                *route.physical_prefix.parts,
            ).resolve()
            if resolved.is_relative_to(physical_root):
                suffix = resolved.relative_to(physical_root)
                return PurePosixPath(route.logical_prefix, suffix.as_posix()).as_posix()
        raise StoragePathError(f"路径未登记到任何存储根: {path}")


@lru_cache(maxsize=16)
def _cached_resolver(
    root_text: str, config_mtime_ns: int, config_size: int,
) -> StorageResolver:
    """按配置文件身份缓存解析器。"""
    del config_mtime_ns, config_size
    return StorageResolver(Path(root_text))


def storage_resolver(data_root: Path) -> StorageResolver:
    """读取配置身份对应的解析器。"""
    root = data_root.resolve()
    config = root / CONFIG_FILE_NAME
    if config.is_file():
        stat = config.stat()
        return _cached_resolver(str(root), stat.st_mtime_ns, stat.st_size)
    return _cached_resolver(str(root), 0, 0)


def resolve_storage_path(data_root: Path, recorded: str) -> Path:
    """解析一个逻辑存储路径。"""
    return storage_resolver(data_root).resolve(recorded)


def relative_storage_path(data_root: Path, path: Path) -> str:
    """生成一个逻辑存储路径。"""
    return storage_resolver(data_root).relative(path)


def storage_status(data_root: Path) -> dict[str, Any]:
    """生成可序列化的存储根验证结果。"""
    resolver = storage_resolver(data_root)
    roots = resolver.verify_all_roots()
    return {
        "config_schema_version": CONFIG_SCHEMA_VERSION,
        "data_root": str(data_root.resolve()),
        "roots": [
            {
                "storage_root_id": row.spec.storage_root_id,
                "role": row.spec.role,
                "mount_path": str(row.resolved_path),
                "volume_guid": row.spec.volume_guid,
                "partition_guid": row.spec.partition_guid,
                "volume_label": row.spec.volume_label,
                "filesystem": row.spec.filesystem,
                "marker_sha256": row.spec.marker_sha256,
            }
            for row in roots
        ],
        "routes": [
            {
                "logical_prefix": row.logical_prefix.as_posix(),
                "storage_root_id": row.storage_root_id,
                "physical_prefix": row.physical_prefix.as_posix(),
                "status": row.status,
            }
            for row in resolver.routes
        ],
    }
