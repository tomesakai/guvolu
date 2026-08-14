"""完整研究验证的内容寻址性能 attestation。"""
from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

from guvolu.data.durable_io import atomic_write_text
from guvolu.research.provenance import (
    canonical_json,
    code_identity,
    sha256_file,
    sha256_text,
)
from guvolu.research.verification import (
    VerificationResult,
    verify_research_artifact_integrity,
    verify_research_run,
    verify_research_runtime_invariants,
)

VERIFICATION_ATTESTATION_METHOD_VERSION = "research-verification-attestation-v1"


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


def _read_object(path: Path, name: str) -> Mapping[str, object]:
    """读取并验证 UTF-8 JSON 对象。"""
    try:
        return _object(json.loads(path.read_text(encoding="utf-8")), name)
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"无法读取 {name}: {path}") from error


def _relative_path(root: Path, path: Path, name: str) -> str:
    """把路径限制在项目目录并返回 POSIX 相对路径。"""
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError as error:
        raise ValueError(f"{name} 越出项目目录") from error


def _attestation_directory(root: Path, manifest_sha256: str) -> Path:
    """返回一个 manifest 身份的固定 attestation 目录。"""
    return (
        root / "reports" / "strategy-research"
        / "verification-attestations" / manifest_sha256
    )


def _write_attestation(
    root: Path,
    result: VerificationResult,
    verified_at: datetime | None = None,
) -> None:
    """只为当前 clean 验证器写入可失效的性能 attestation。"""
    identity = code_identity(root, ())
    if not identity.decision_grade or identity.git_hash is None:
        return
    manifest_relative = _relative_path(
        root, result.manifest_path, "研究 manifest",
    )
    timestamp = verified_at or datetime.now(UTC)
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=UTC)
    else:
        timestamp = timestamp.astimezone(UTC)
    payload = {
        "schema_version": 1,
        "verification_attestation_method_version": (
            VERIFICATION_ATTESTATION_METHOD_VERSION
        ),
        "verified_at": timestamp.isoformat(),
        "manifest": {
            "path": manifest_relative,
            "sha256": result.manifest_sha256,
            "run_id": result.run_id,
            "checked_artifacts": list(result.checked_artifacts),
        },
        "verifier": {
            "git_hash": identity.git_hash,
            "tree_digest": identity.tree_digest,
            "decision_grade": True,
        },
        "scope": "read_only_adaptive_performance_cache",
        "promotion_evidence": False,
    }
    content = canonical_json(payload) + "\n"
    digest = sha256_text(content)
    directory = _attestation_directory(root, result.manifest_sha256)
    attestation_path = directory / f"verification-attestation-sha256-{digest}.json"
    atomic_write_text(attestation_path, content)
    pointer = {
        "schema_version": 1,
        "verification_attestation_method_version": (
            VERIFICATION_ATTESTATION_METHOD_VERSION
        ),
        "manifest_sha256": result.manifest_sha256,
        "attestation": {
            "path": _relative_path(root, attestation_path, "attestation"),
            "sha256": digest,
            "bytes": attestation_path.stat().st_size,
        },
    }
    atomic_write_text(directory / "latest.json", canonical_json(pointer) + "\n")


def _cached_verification(
    root: Path,
    manifest_path: Path | None,
) -> VerificationResult | None:
    """完整性、代码树或治理变化时拒绝复用旧 attestation。"""
    try:
        integrity = verify_research_artifact_integrity(root, manifest_path)
        identity = code_identity(root, ())
        if not identity.decision_grade:
            return None
        directory = _attestation_directory(root, integrity.manifest_sha256)
        pointer = _read_object(directory / "latest.json", "attestation pointer")
        if (
            pointer.get("verification_attestation_method_version")
            != VERIFICATION_ATTESTATION_METHOD_VERSION
            or pointer.get("manifest_sha256") != integrity.manifest_sha256
        ):
            return None
        record = _object(pointer.get("attestation"), "attestation record")
        relative = _text(record.get("path"), "attestation path")
        attestation_path = (root / relative).resolve()
        if attestation_path.parent != directory.resolve():
            return None
        expected_hash = _text(record.get("sha256"), "attestation sha256")
        if sha256_file(attestation_path) != expected_hash:
            return None
        expected_bytes = record.get("bytes")
        if (
            not isinstance(expected_bytes, int)
            or isinstance(expected_bytes, bool)
            or attestation_path.stat().st_size != expected_bytes
        ):
            return None
        attestation = _read_object(attestation_path, "verification attestation")
        manifest = _object(attestation.get("manifest"), "attestation manifest")
        verifier = _object(attestation.get("verifier"), "attestation verifier")
        checked = manifest.get("checked_artifacts")
        if not isinstance(checked, list):
            return None
        if (
            attestation.get("verification_attestation_method_version")
            != VERIFICATION_ATTESTATION_METHOD_VERSION
            or attestation.get("scope")
            != "read_only_adaptive_performance_cache"
            or attestation.get("promotion_evidence") is not False
            or manifest.get("sha256") != integrity.manifest_sha256
            or manifest.get("path") != _relative_path(
                root, integrity.manifest_path, "研究 manifest",
            )
            or tuple(checked) != integrity.checked_artifacts
            or verifier.get("decision_grade") is not True
            or verifier.get("tree_digest") != identity.tree_digest
        ):
            return None
        run_id = _text(integrity.manifest.get("run_id"), "manifest.run_id")
        if manifest.get("run_id") != run_id:
            return None
        verify_research_runtime_invariants(root, integrity.summary)
        return VerificationResult(
            run_id=run_id,
            manifest_path=integrity.manifest_path,
            manifest_sha256=integrity.manifest_sha256,
            checked_artifacts=integrity.checked_artifacts,
            cache_hit=True,
        )
    except (OSError, ValueError):
        return None


def verify_research_run_cached(
    root: Path,
    manifest_path: Path | None = None,
    verified_at: datetime | None = None,
) -> VerificationResult:
    """优先复用 clean 验证器 attestation，否则执行一次完整证明。"""
    resolved_root = root.resolve()
    cached = _cached_verification(resolved_root, manifest_path)
    if cached is not None:
        return cached
    verified = verify_research_run(resolved_root, manifest_path)
    _write_attestation(resolved_root, verified, verified_at)
    return replace(verified, cache_hit=False)
