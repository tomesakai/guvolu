"""研究制品的散列与代码身份。"""
from __future__ import annotations

import hashlib
import json
import re
import subprocess
from pathlib import Path
from typing import Mapping, Sequence

from guvolu.research.contracts import CodeIdentity


CODE_IDENTITY_METHOD_VERSION = "research-code-identity-v2"
_EXECUTION_SOURCE_SUFFIXES = {
    ".c",
    ".cc",
    ".cpp",
    ".cu",
    ".cuh",
    ".h",
    ".hpp",
    ".ps1",
    ".py",
    ".rs",
}
_BUILD_CONTRACT_FILES = (
    "Cargo.lock",
    "Cargo.toml",
    "pyproject.toml",
    "uv.lock",
)
_GIT_COMMIT_PATTERN = re.compile(r"[0-9a-f]{40}")


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
    source_paths = [
        path
        for directory in (root / "src", root / "scripts", root / "tests")
        if directory.is_dir()
        for path in directory.rglob("*")
        if path.is_file() and path.suffix.lower() in _EXECUTION_SOURCE_SUFFIXES
    ]
    source_paths.extend(
        path for name in _BUILD_CONTRACT_FILES
        if (path := root / name).is_file()
    )
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


def code_tree_digest_at_commit(
    root: Path,
    git_hash: str,
    config_paths: Sequence[Path],
) -> str:
    """从 Git 对象库重建 clean 决策运行的研究代码树身份。"""
    if _GIT_COMMIT_PATTERN.fullmatch(git_hash) is None:
        raise ValueError("Git commit 必须是规范 40 位小写十六进制")
    repository = root.resolve()
    try:
        listing = subprocess.run(
            ["git", "ls-tree", "-r", "--name-only", git_hash],
            cwd=repository,
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
        ).stdout.splitlines()
    except (FileNotFoundError, subprocess.CalledProcessError) as error:
        raise ValueError("记录的 Git commit 无法从本地对象库复核") from error
    tracked = {Path(name).as_posix() for name in listing if name}
    selected = {
        name for name in tracked
        if (
            Path(name).parts
            and Path(name).parts[0] in {"src", "scripts", "tests"}
            and Path(name).suffix.lower() in _EXECUTION_SOURCE_SUFFIXES
        ) or name in _BUILD_CONTRACT_FILES
    }
    for path in config_paths:
        resolved = path.resolve()
        try:
            relative = resolved.relative_to(repository).as_posix()
        except ValueError as error:
            raise ValueError("研究配置越出 Git 仓库") from error
        if relative not in tracked:
            raise ValueError("决策级研究配置未由记录的 Git commit 跟踪")
        selected.add(relative)
    digest = hashlib.sha256()
    for relative in sorted(selected):
        try:
            content = subprocess.run(
                ["git", "show", f"{git_hash}:{relative}"],
                cwd=repository,
                check=True,
                capture_output=True,
            ).stdout
        except (FileNotFoundError, subprocess.CalledProcessError) as error:
            raise ValueError(f"Git commit 缺少研究代码文件: {relative}") from error
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(hashlib.sha256(content).digest())
    return digest.hexdigest()


def verify_artifacts_match_commit(
    root: Path,
    git_hash: str,
    source_artifact_pairs: Sequence[tuple[Path, Path]],
) -> None:
    """逐字节证明内容寻址配置快照等于记录 commit 中的 Git blob。"""
    if _GIT_COMMIT_PATTERN.fullmatch(git_hash) is None:
        raise ValueError("Git commit 必须是规范 40 位小写十六进制")
    repository = root.resolve()
    for source_path, artifact_path in source_artifact_pairs:
        try:
            relative = source_path.resolve().relative_to(repository).as_posix()
        except ValueError as error:
            raise ValueError("Git blob 源路径越出仓库") from error
        try:
            committed = subprocess.run(
                ["git", "show", f"{git_hash}:{relative}"],
                cwd=repository,
                check=True,
                capture_output=True,
            ).stdout
        except (FileNotFoundError, subprocess.CalledProcessError) as error:
            raise ValueError(f"记录 commit 缺少配置 Git blob: {relative}") from error
        if committed != artifact_path.read_bytes():
            raise ValueError(f"配置快照与记录 commit 的 Git blob 不一致: {relative}")


def artifact_record(path: Path, kind: str) -> Mapping[str, object]:
    """生成输出制品的内容身份。"""
    return {
        "kind": kind,
        "path": path.as_posix(),
        "sha256": sha256_file(path),
        "bytes": path.stat().st_size,
    }
