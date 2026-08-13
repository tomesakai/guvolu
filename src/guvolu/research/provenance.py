"""研究制品的散列与代码身份。"""
from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path
from typing import Mapping, Sequence

from guvolu.research.contracts import CodeIdentity


def canonical_json(value: object) -> str:
    """生成确定性 JSON 文本。"""
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def sha256_text(value: str) -> str:
    """计算 UTF-8 文本散列。"""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def sha256_file(path: Path) -> str:
    """流式计算文件散列。"""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def stable_identifier(prefix: str, value: object) -> str:
    """生成带业务前缀的内容标识。"""
    return f"{prefix}-{sha256_text(canonical_json(value))}"


def hash_paths(root: Path, paths: Sequence[Path]) -> str:
    """按相对路径和内容计算目录身份。"""
    digest = hashlib.sha256()
    for path in sorted(paths, key=lambda item: item.as_posix()):
        resolved = path.resolve()
        relative = resolved.relative_to(root.resolve()).as_posix()
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(bytes.fromhex(sha256_file(resolved)))
    return digest.hexdigest()


def _git_output(root: Path, arguments: Sequence[str]) -> str | None:
    """读取 Git 输出，未建版本时返回空。"""
    try:
        completed = subprocess.run(
            ["git", *arguments],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        return None
    return completed.stdout.strip()


def code_identity(root: Path, config_paths: Sequence[Path]) -> CodeIdentity:
    """记录 Git 与研究源文件的双重身份。"""
    source_paths = list((root / "src" / "guvolu").rglob("*.py"))
    source_paths.extend((root / "scripts").rglob("*.py"))
    source_paths.extend((root / "tests").rglob("*.py"))
    source_paths.extend(config_paths)
    tree_digest = hash_paths(root, tuple(source_paths))
    status = _git_output(root, ("status", "--porcelain=v1", "--untracked-files=all"))
    dirty = bool(status)
    dirty_digest = tree_digest if dirty else sha256_text("")
    git_hash = _git_output(root, ("rev-parse", "HEAD"))
    if git_hash is None:
        return CodeIdentity(
            git_hash=None,
            tree_digest=tree_digest,
            dirty_digest=dirty_digest,
            dirty=dirty,
            decision_grade=False,
            reason="repository_has_no_commit",
        )
    return CodeIdentity(
        git_hash=git_hash,
        tree_digest=tree_digest,
        dirty_digest=dirty_digest,
        dirty=dirty,
        decision_grade=not dirty,
        reason="repository_dirty" if dirty else None,
    )


def artifact_record(path: Path, kind: str) -> Mapping[str, object]:
    """生成输出制品的内容身份。"""
    return {
        "kind": kind,
        "path": path.as_posix(),
        "sha256": sha256_file(path),
        "bytes": path.stat().st_size,
    }
