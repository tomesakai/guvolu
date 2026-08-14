"""策略研究配置的可验证父链。"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from guvolu.data.durable_io import atomic_write_bytes, atomic_write_text

_MAX_LINEAGE_FILES = 32
CONFIG_LINEAGE_SNAPSHOT_METHOD_VERSION = "content-addressed-config-lineage-v1"


@dataclass(frozen=True)
class ConfigLineageSnapshot:
    """完整配置父链的不可变内容寻址快照。"""

    leaf_config_path: Path
    leaf_config_sha256: str
    bundle_path: Path
    bundle_sha256: str
    source_paths: tuple[Path, ...]


@dataclass(frozen=True)
class _LineageEntry:
    path: Path
    sha256: str
    raw: bytes
    body: Mapping[str, object]


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


def _load_verified_config_lineage_details(
    repository_root: Path,
    config_path: Path,
) -> tuple[Mapping[str, object], str, str, int, tuple[_LineageEntry, ...]]:
    """从单次字节快照解析配置并递归验证父链。"""
    root = repository_root.resolve()
    seen: set[Path] = set()

    def visit(
        path: Path,
        expected_hash: str | None = None,
    ) -> tuple[Mapping[str, object], str, str, int, tuple[_LineageEntry, ...]]:
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
        entry = _LineageEntry(resolved, current_hash, raw, body)
        raw_parent = body.get("evolution_parent")
        if raw_parent is None:
            return body, current_hash, current_hash, 0, (entry,)
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
        _parent, _parent_hash, root_hash, parent_depth, parent_entries = visit(
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
        return body, current_hash, root_hash, declared_depth, (
            entry, *parent_entries,
        )

    return visit(config_path)


def load_verified_config_lineage(
    repository_root: Path,
    config_path: Path,
) -> tuple[Mapping[str, object], str, str, int]:
    """从单次字节快照解析配置并递归验证父链。"""
    config, config_hash, root_hash, depth, _entries = (
        _load_verified_config_lineage_details(repository_root, config_path)
    )
    return config, config_hash, root_hash, depth


def verified_config_lineage_paths(
    repository_root: Path,
    config_path: Path,
) -> tuple[Path, ...]:
    """返回已经完整验证、由叶到根排列的配置源路径。"""
    *_identity, entries = _load_verified_config_lineage_details(
        repository_root, config_path,
    )
    return tuple(entry.path for entry in entries)


def snapshot_verified_config_lineage(
    repository_root: Path,
    config_path: Path,
    output_directory: Path,
) -> ConfigLineageSnapshot:
    """把完整配置父链逐字节复制为内容寻址制品并发布索引。"""
    root = repository_root.resolve()
    output = output_directory.resolve()
    try:
        output.relative_to(root)
    except ValueError as error:
        raise ValueError("配置谱系快照目录越出项目目录") from error
    _, leaf_hash, root_hash, depth, entries = _load_verified_config_lineage_details(
        root, config_path,
    )
    records: list[dict[str, object]] = []
    artifact_paths: list[Path] = []
    for entry in entries:
        artifact_path = output / f"config-sha256-{entry.sha256}.json"
        if artifact_path.exists():
            if hashlib.sha256(artifact_path.read_bytes()).hexdigest() != entry.sha256:
                raise ValueError("既有配置快照发生内容寻址冲突")
        else:
            atomic_write_bytes(artifact_path, entry.raw)
        artifact_paths.append(artifact_path)
        records.append({
            "repository_path": entry.path.relative_to(root).as_posix(),
            "artifact_path": artifact_path.relative_to(root).as_posix(),
            "sha256": entry.sha256,
            "bytes": len(entry.raw),
        })
    payload = {
        "schema_version": 1,
        "method_version": CONFIG_LINEAGE_SNAPSHOT_METHOD_VERSION,
        "leaf_config_sha256": leaf_hash,
        "root_config_sha256": root_hash,
        "lineage_depth": depth,
        "entries": records,
    }
    text = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        allow_nan=False,
    ) + "\n"
    bundle_sha256 = hashlib.sha256(text.encode("utf-8")).hexdigest()
    bundle_path = output / f"config-lineage-sha256-{bundle_sha256}.json"
    if bundle_path.exists():
        if hashlib.sha256(bundle_path.read_bytes()).hexdigest() != bundle_sha256:
            raise ValueError("既有配置谱系索引发生内容寻址冲突")
    else:
        atomic_write_text(bundle_path, text)
    return ConfigLineageSnapshot(
        leaf_config_path=artifact_paths[0],
        leaf_config_sha256=leaf_hash,
        bundle_path=bundle_path,
        bundle_sha256=bundle_sha256,
        source_paths=tuple(entry.path for entry in entries),
    )


def attest_config_lineage_snapshot(
    repository_root: Path,
    bundle_path: Path,
    leaf_config_path: Path,
) -> tuple[
    Mapping[str, object], str, str, int, tuple[Path, ...], tuple[Path, ...],
]:
    """只依赖不可变快照重建父链身份，并返回原始 Git 路径。"""
    root = repository_root.resolve()
    bundle = _object(json.loads(
        bundle_path.read_text(encoding="utf-8"),
        parse_constant=_reject_non_finite,
    ), "config_lineage_snapshot")
    if (
        bundle.get("schema_version") != 1
        or bundle.get("method_version") != CONFIG_LINEAGE_SNAPSHOT_METHOD_VERSION
    ):
        raise ValueError("配置谱系快照方法版本不受支持")
    raw_records = bundle.get("entries")
    if not isinstance(raw_records, list) or not raw_records:
        raise ValueError("配置谱系快照缺少 entries")
    if len(raw_records) > _MAX_LINEAGE_FILES:
        raise ValueError("配置谱系快照超过最大深度")
    entries: list[_LineageEntry] = []
    artifact_paths: list[Path] = []
    for index, raw_record in enumerate(raw_records):
        record = _object(raw_record, f"entries.{index}")
        if set(record) != {"repository_path", "artifact_path", "sha256", "bytes"}:
            raise ValueError("配置谱系快照 entry 字段不完整")
        repository_path = (root / _text(
            record.get("repository_path"), "repository_path",
        )).resolve()
        artifact_path = (root / _text(
            record.get("artifact_path"), "artifact_path",
        )).resolve()
        try:
            repository_path.relative_to(root)
            artifact_path.relative_to(root)
        except ValueError as error:
            raise ValueError("配置谱系快照路径越出项目目录") from error
        raw = artifact_path.read_bytes()
        sha256 = _text(record.get("sha256"), "sha256")
        byte_count = record.get("bytes")
        if (
            hashlib.sha256(raw).hexdigest() != sha256
            or not isinstance(byte_count, int)
            or isinstance(byte_count, bool)
            or byte_count != len(raw)
        ):
            raise ValueError("配置谱系快照内容身份不匹配")
        body = _object(json.loads(
            raw.decode("utf-8"), parse_constant=_reject_non_finite,
        ), f"entries.{index}.config")
        entries.append(_LineageEntry(repository_path, sha256, raw, body))
        artifact_paths.append(artifact_path)
    if artifact_paths[0] != leaf_config_path.resolve():
        raise ValueError("配置谱系叶配置与 manifest config 制品不一致")
    root_hash = entries[-1].sha256
    depth = len(entries) - 1
    for index, entry in enumerate(entries):
        raw_parent = entry.body.get("evolution_parent")
        if index == len(entries) - 1:
            if raw_parent is not None:
                raise ValueError("配置谱系快照未包含完整根配置")
            continue
        parent = _object(raw_parent, "evolution_parent")
        expected_parent = entries[index + 1]
        parent_path = (root / _text(
            parent.get("parent_config_path"), "parent_config_path",
        )).resolve()
        if (
            parent_path != expected_parent.path
            or parent.get("parent_config_hash") != expected_parent.sha256
            or parent.get("lineage_root_config_hash") != root_hash
            or parent.get("lineage_depth") != depth - index
        ):
            raise ValueError("配置谱系快照父链声明不一致")
    if (
        bundle.get("leaf_config_sha256") != entries[0].sha256
        or bundle.get("root_config_sha256") != root_hash
        or bundle.get("lineage_depth") != depth
    ):
        raise ValueError("配置谱系快照顶层身份不一致")
    return (
        entries[0].body,
        entries[0].sha256,
        root_hash,
        depth,
        tuple(entry.path for entry in entries),
        tuple(artifact_paths),
    )


def verify_config_lineage(
    repository_root: Path,
    config_path: Path,
) -> tuple[str, int]:
    """递归验证父配置路径、散列、深度和根身份。"""
    _config, _config_hash, root_hash, depth = load_verified_config_lineage(
        repository_root, config_path,
    )
    return root_hash, depth


def load_governed_strategy_config(
    repository_root: Path,
    config_path: Path,
) -> tuple[Mapping[str, object], str, str, int]:
    """统一加载配置谱系，并重放派生配置的单轴演进合同。"""
    config, config_hash, root_hash, depth, _source_paths = (
        load_governed_strategy_config_with_paths(repository_root, config_path)
    )
    return config, config_hash, root_hash, depth


def load_governed_strategy_config_with_paths(
    repository_root: Path,
    config_path: Path,
) -> tuple[Mapping[str, object], str, str, int, tuple[Path, ...]]:
    """单次读取配置谱系，同时返回已验证的叶到根源路径。"""
    config, config_hash, root_hash, depth, entries = (
        _load_verified_config_lineage_details(repository_root, config_path)
    )
    # 局部导入避免初始化环。
    from guvolu.research.tuning import verify_evolution_config

    verify_evolution_config(repository_root, config_path, config)
    return (
        config,
        config_hash,
        root_hash,
        depth,
        tuple(entry.path for entry in entries),
    )
