"""研究完整验证性能 attestation 的命中与失效测试。"""
from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from guvolu.research.contracts import CodeIdentity
from guvolu.research.provenance import sha256_file
from guvolu.research.verification import VerificationResult
from guvolu.research.verification_attestation import verify_research_run_cached


def _clean_identity(tree_digest: str = "verifier-tree") -> CodeIdentity:
    """返回可写和复用 attestation 的 clean 验证器身份。"""
    return CodeIdentity(
        git_hash="verifier-commit",
        tree_digest=tree_digest,
        dirty_digest="clean",
        dirty=False,
        decision_grade=True,
        reason=None,
    )


def _write_minimal_run(root: Path) -> tuple[Path, Path]:
    """写一个只需轻量合同复核的固定研究运行。"""
    reports = root / "reports"
    reports.mkdir()
    summary = reports / "summary.json"
    summary.write_text(json.dumps({
        "pipeline_method_version": "cache-test-pipeline",
        "decision_grade": True,
        "operational_quality": {"eligible": False},
        "operational_position": {
            "weights": {"trend": 0.0},
            "reserve": 1.0,
        },
        "operational_target_contract": {
            "aggregate_target": 0.0,
            "families": [{
                "family": "trend",
                "family_target": 1.0,
                "allocation_weight": 0.0,
                "portfolio_target_contribution": 0.0,
            }],
        },
    }), encoding="utf-8")
    manifest = reports / "manifest.json"
    manifest.write_text(json.dumps({
        "run_id": "research-run-cache-test",
        "artifacts": {
            "summary_json": {
                "path": "reports/summary.json",
                "sha256": sha256_file(summary),
                "bytes": summary.stat().st_size,
            },
        },
    }), encoding="utf-8")
    return manifest, summary


def test_cached_verification_reuses_clean_content_addressed_attestation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """相同 manifest、制品与验证器代码树只做一次完整证明。"""
    manifest, _summary = _write_minimal_run(tmp_path)
    calls = 0

    def full_verify(_root: Path, requested: Path | None) -> VerificationResult:
        nonlocal calls
        calls += 1
        assert requested == manifest
        return VerificationResult(
            run_id="research-run-cache-test",
            manifest_path=manifest,
            manifest_sha256=sha256_file(manifest),
            checked_artifacts=("summary_json",),
        )

    monkeypatch.setattr(
        "guvolu.research.verification_attestation.verify_research_run",
        full_verify,
    )
    monkeypatch.setattr(
        "guvolu.research.verification_attestation.code_identity",
        lambda _root, _paths: _clean_identity(),
    )
    first = verify_research_run_cached(
        tmp_path,
        manifest,
        verified_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    second = verify_research_run_cached(tmp_path, manifest)
    assert calls == 1
    assert first.cache_hit is False
    assert second.cache_hit is True
    pointers = tuple((
        tmp_path / "reports" / "strategy-research"
        / "verification-attestations"
    ).rglob("latest.json"))
    assert len(pointers) == 1
    pointer = json.loads(pointers[0].read_text(encoding="utf-8"))
    attestation = tmp_path / pointer["attestation"]["path"]
    payload = json.loads(attestation.read_text(encoding="utf-8"))
    assert payload["promotion_evidence"] is False
    assert payload["scope"] == "read_only_adaptive_performance_cache"


@pytest.mark.parametrize(
    "change", ("artifact", "manifest_identity", "verifier_tree", "dirty"),
)
def test_cached_verification_invalidates_changed_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    change: str,
) -> None:
    """制品、验证器代码树或 dirty 状态变化都必须退回完整证明。"""
    manifest, summary = _write_minimal_run(tmp_path)
    identity = _clean_identity()

    def initial_verify(_root: Path, _manifest: Path | None) -> VerificationResult:
        return VerificationResult(
            run_id="research-run-cache-test",
            manifest_path=manifest,
            manifest_sha256=sha256_file(manifest),
            checked_artifacts=("summary_json",),
        )

    monkeypatch.setattr(
        "guvolu.research.verification_attestation.verify_research_run",
        initial_verify,
    )
    monkeypatch.setattr(
        "guvolu.research.verification_attestation.code_identity",
        lambda _root, _paths: identity,
    )
    verify_research_run_cached(tmp_path, manifest)
    if change == "artifact":
        summary.write_text(
            summary.read_text(encoding="utf-8") + "\n",
            encoding="utf-8",
        )
    elif change == "manifest_identity":
        body = json.loads(manifest.read_text(encoding="utf-8"))
        body["input_head_generation"] = "changed-head"
        manifest.write_text(json.dumps(body), encoding="utf-8")
    elif change == "verifier_tree":
        identity = _clean_identity("changed-verifier-tree")
    else:
        identity = CodeIdentity(
            git_hash="verifier-commit",
            tree_digest="verifier-tree",
            dirty_digest="dirty",
            dirty=True,
            decision_grade=False,
            reason="repository_dirty",
        )

    def reject_full(_root: Path, _manifest: Path | None) -> VerificationResult:
        raise ValueError("完整证明已重新触发")

    monkeypatch.setattr(
        "guvolu.research.verification_attestation.verify_research_run",
        reject_full,
    )
    with pytest.raises(ValueError, match="完整证明已重新触发"):
        verify_research_run_cached(tmp_path, manifest)
