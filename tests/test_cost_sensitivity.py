"""固定目标成本敏感性制品测试。"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from guvolu.research.cost_sensitivity import (
    build_fixed_target_cost_sensitivity,
    _metrics,
    cost_sensitivity_bytes,
    fixed_target_cost_sensitivity,
)
from guvolu.research.provenance import canonical_json, sha256_file
from guvolu.research.verification import VerificationResult


def _fixture(tmp_path: Path) -> tuple[
    Path, dict[str, object], dict[str, object], Path,
]:
    config = tmp_path / "config.json"
    config.write_text('{"bar_interval":"1hour"}\n', encoding="utf-8")
    replay = tmp_path / "replay.jsonl"
    header = {
        "record_type": "label_cost_header",
        "schema_version": 1,
        "research_identity": "research-one",
        "panel_sha256": "p" * 64,
        "cost_bps": 10.0,
        "maximum_gap_seconds": 14_400.0,
        "deployment_candidates": {"trend": "candidate-one"},
        "walk_forward_selection_paths": {"trend": ["candidate-one"]},
        "replay_semantics": {},
    }
    rows = [header]
    for ordinal, (target, turnover, market_return) in enumerate((
        (1.0, 1.0, 0.01),
        (0.5, 0.5, 0.02),
    )):
        gross = target * market_return
        family = {
            "target_at_decision": target,
            "turnover": turnover,
            "cost": turnover * 0.001,
            "next_net_return": gross - turnover * 0.001,
        }
        rows.append({
            "record_type": "label_cost",
            "decision_time": f"2026-01-01T0{ordinal}:00:00+00:00",
            "label_available_time": f"2026-01-01T0{ordinal + 1}:00:00+00:00",
            "gap_seconds": 3600.0,
            "hard_gap": False,
            "in_walk_forward_oos": True,
            "walk_forward_fold_id": "fold-001",
            "next_market_log_return": market_return,
            "replays": {
                "deployment": {"trend": family},
                "walk_forward_stitched": {"trend": family},
            },
        })
    replay.write_text(
        "".join(canonical_json(row) + "\n" for row in rows),
        encoding="utf-8",
    )
    base = _metrics((0.01, 0.01), (1.0, 0.5), 10.0, 8760.0)
    source_metrics = {
        "bars": base["bars"],
        "net_return": base["net_log_return"],
        "sharpe": base["sharpe"],
        "maximum_drawdown": base["maximum_drawdown"],
        "turnover": base["turnover"],
        "annual_turnover": base["annual_turnover"],
        "cost": base["cost"],
    }
    summary: dict[str, object] = {
        "family_evaluations": [{
            "family": "trend",
            "metrics": source_metrics,
            "deployment_oos_metrics": source_metrics,
        }],
    }
    summary_path = tmp_path / "summary.json"
    summary_path.write_text(json.dumps(summary), encoding="utf-8")
    manifest: dict[str, object] = {
        "run_id": "run-one",
        "research_identity": "research-one",
        "artifacts": {
            "config": {"path": "config.json", "sha256": sha256_file(config)},
            "summary_json": {
                "path": "summary.json",
                "sha256": sha256_file(summary_path),
            },
            "label_cost_replay": {
                "path": "replay.jsonl",
                "sha256": sha256_file(replay),
            },
        },
    }
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    return manifest_path, manifest, summary, replay


def test_fixed_target_cost_scan_does_not_reselect_candidates(
    tmp_path: Path,
) -> None:
    manifest_path, manifest, summary, replay = _fixture(tmp_path)

    result = fixed_target_cost_sensitivity(
        manifest_path,
        sha256_file(manifest_path),
        manifest,
        summary,
        replay,
        replay.read_bytes(),
        {"bar_interval": "1hour"},
        "trend",
        (20.0, 0.0, 10.0, 10.0),
        tmp_path,
    )

    assert result["selection_locked"] is True
    assert result["cost_bps_grid"] == [0.0, 10.0, 20.0]
    deployment = result["results"]["deployment"]
    assert deployment["break_even_one_way_bps"] == pytest.approx(
        133.33333333333334,
    )
    assert [row["net_log_return"] for row in deployment["curve"]] == pytest.approx([
        0.02, 0.0185, 0.017,
    ])
    assert result["source"]["deployment_candidate_id"] == "candidate-one"
    assert cost_sensitivity_bytes(result).endswith(b"\n")


def test_fixed_target_cost_scan_rejects_tampered_replay(tmp_path: Path) -> None:
    manifest_path, manifest, summary, replay = _fixture(tmp_path)
    rows = replay.read_text(encoding="utf-8").splitlines()
    tampered = json.loads(rows[1])
    tampered["replays"]["deployment"]["trend"]["next_net_return"] = 99.0
    rows[1] = canonical_json(tampered)
    replay.write_text("\n".join(rows) + "\n", encoding="utf-8")
    manifest["artifacts"]["label_cost_replay"]["sha256"] = sha256_file(replay)

    with pytest.raises(ValueError, match="单行成本不能重建"):
        fixed_target_cost_sensitivity(
            manifest_path,
            sha256_file(manifest_path),
            manifest,
            summary,
            replay,
            replay.read_bytes(),
            {"bar_interval": "1hour"},
            "trend",
            (10.0,),
            tmp_path,
        )


@pytest.mark.parametrize(
    ("target", "message"),
    (
        ("manifest", "manifest 字节发生变化"),
        ("summary", "summary 字节发生变化"),
        ("config", "config 字节发生变化"),
        ("replay", "replay 字节发生变化"),
    ),
)
def test_cost_scan_rejects_post_verification_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    target: str,
    message: str,
) -> None:
    """完整复验返回后任何实际消费制品被替换都必须拒绝。"""
    manifest_path, manifest, _summary, _replay = _fixture(tmp_path)
    paths = {
        "manifest": manifest_path,
        "summary": tmp_path / "summary.json",
        "config": tmp_path / "config.json",
        "replay": tmp_path / "replay.jsonl",
    }
    verified_sha = sha256_file(manifest_path)

    def verified_then_replaced(
        _root: Path, _manifest: Path,
    ) -> VerificationResult:
        paths[target].write_bytes(paths[target].read_bytes() + b" ")
        return VerificationResult("run-one", manifest_path, verified_sha, ())

    monkeypatch.setattr(
        "guvolu.research.cost_sensitivity.verify_research_run",
        verified_then_replaced,
    )
    with pytest.raises(ValueError, match=message):
        build_fixed_target_cost_sensitivity(
            tmp_path,
            manifest_path,
            "trend",
            (10.0,),
        )
