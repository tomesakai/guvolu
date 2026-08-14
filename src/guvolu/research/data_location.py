"""研究市场数据根的可移植定位合同。"""
from __future__ import annotations

from pathlib import Path
from typing import Mapping


DATA_ROOT_LOCATOR_SCHEMA_VERSION = 1


def data_root_locator(repository_root: Path, data_root: Path) -> dict[str, object]:
    """记录仓库内相对路径或显式外部绝对路径。"""
    repository = repository_root.resolve()
    source = data_root.resolve()
    try:
        relative = source.relative_to(repository).as_posix()
    except ValueError:
        return {
            "schema_version": DATA_ROOT_LOCATOR_SCHEMA_VERSION,
            "kind": "absolute",
            "path": source.as_posix(),
        }
    return {
        "schema_version": DATA_ROOT_LOCATOR_SCHEMA_VERSION,
        "kind": "repository_relative",
        "path": relative,
    }


def resolve_data_root_locator(
    repository_root: Path,
    raw: object,
) -> Path:
    """解析数据根；缺失字段兼容旧制品的仓库内 data。"""
    repository = repository_root.resolve()
    if raw is None:
        return repository / "data"
    if not isinstance(raw, Mapping):
        raise ValueError("source_data_root 必须是路径定位对象")
    if raw.get("schema_version") != DATA_ROOT_LOCATOR_SCHEMA_VERSION:
        raise ValueError("source_data_root schema 不受支持")
    kind = raw.get("kind")
    value = raw.get("path")
    if not isinstance(value, str) or not value.strip():
        raise ValueError("source_data_root.path 必须是非空字符串")
    path = Path(value)
    if kind == "repository_relative":
        if path.is_absolute():
            raise ValueError("仓库相对数据根不得是绝对路径")
        resolved = (repository / path).resolve()
        try:
            resolved.relative_to(repository)
        except ValueError as error:
            raise ValueError("仓库相对数据根越出项目目录") from error
        return resolved
    if kind == "absolute":
        if not path.is_absolute():
            raise ValueError("外部数据根必须是绝对路径")
        return path.resolve()
    raise ValueError("source_data_root.kind 不受支持")
