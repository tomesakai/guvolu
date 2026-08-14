"""策略研究配置的可验证父链。"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path

_MAX_LINEAGE_FILES = 32


def _object(value: object, name: str) -> Mapping[str, object]:
    """验证 JSON 对象。"""
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} 必须为对象")
    return {str(key): item for key, item in value.items()}


def _text(value: object, name: str) -> str:
    """验证非空文本。"""
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} 必须为非空文本")
    return value


def _reject_non_finite(value: str) -> object:
    """拒绝不属于 JSON 标准的 NaN 与 Infinity。"""
    raise ValueError(f"配置包含非有限数值: {value}")


def load_verified_config_lineage(
    repository_root: Path,
    config_path: Path,
) -> tuple[Mapping[str, object], str, str, int]:
    """从单次字节快照解析配置并递归验证父链。"""
    root = repository_root.resolve()
    seen: set[Path] = set()

    def visit(
        path: Path,
        expected_hash: str | None = None,
    ) -> tuple[Mapping[str, object], str, str, int]:
        resolved = path.resolve()
        try:
            resolved.relative_to(root)
        except ValueError as error:
            raise ValueError("配置谱系路径越出项目目录") from error
        if len(seen) >= _MAX_LINEAGE_FILES:
            raise ValueError("配置谱系超过最大深度")
        if resolved in seen:
            raise ValueError("配置谱系存在循环")
        seen.add(resolved)
        raw = resolved.read_bytes()
        current_hash = hashlib.sha256(raw).hexdigest()
        if expected_hash is not None and current_hash != expected_hash:
            raise ValueError("父配置实际散列与谱系声明不一致")
        body = _object(json.loads(
            raw.decode("utf-8"),
            parse_constant=_reject_non_finite,
        ), "config")
        raw_parent = body.get("evolution_parent")
        if raw_parent is None:
            return body, current_hash, current_hash, 0
        parent = _object(raw_parent, "evolution_parent")
        parent_path = (root / _text(
            parent.get("parent_config_path"),
            "evolution_parent.parent_config_path",
        )).resolve()
        try:
            parent_path.relative_to(root)
        except ValueError as error:
            raise ValueError("父配置路径越出项目目录") from error
        declared_parent_hash = _text(
            parent.get("parent_config_hash"),
            "evolution_parent.parent_config_hash",
        )
        _parent, _parent_hash, root_hash, parent_depth = visit(
            parent_path, declared_parent_hash,
        )
        declared_root = _text(
            parent.get("lineage_root_config_hash"),
            "evolution_parent.lineage_root_config_hash",
        )
        if declared_root != root_hash:
            raise ValueError("配置谱系根散列不一致")
        declared_depth = parent.get("lineage_depth")
        if (
            not isinstance(declared_depth, int)
            or isinstance(declared_depth, bool)
            or declared_depth != parent_depth + 1
        ):
            raise ValueError("配置谱系深度不一致")
        return body, current_hash, root_hash, declared_depth

    return visit(config_path)


def verify_config_lineage(
    repository_root: Path,
    config_path: Path,
) -> tuple[str, int]:
    """递归验证父配置路径、散列、深度和根身份。"""
    _config, _config_hash, root_hash, depth = load_verified_config_lineage(
        repository_root, config_path,
    )
    return root_hash, depth
