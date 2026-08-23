"""搜索循环配置、候选生成与合成端到端测试。"""
from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from guvolu.research.config_lineage import load_governed_strategy_config
from guvolu.search.ledger import read_ledger
from guvolu.search.loop import (
    SOURCE_NEIGHBORHOOD,
    SOURCE_REGISTERED_GRID,
    SOURCE_STRUCTURAL,
    axis_values,
    generate_loop_candidates,
    load_loop_config,
    loop_cost_model,
    neighborhood_candidates,
)
from guvolu.search.promote import promoted_config, research_command, write_promoted_config
from guvolu.search.torch_runtime import torch_module_or_none
from guvolu.strategy.generation import build_family_batches

ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = ROOT / "config"


def _prepare_root(tmp_path: Path) -> Path:
    """把自检配置复制到临时项目根。"""
    root = tmp_path / "repo"
    (root / "config").mkdir(parents=True)
    for name in ("search_loop_selfcheck.json", "strategy_research_selfcheck.json"):
        shutil.copy(CONFIG_DIR / name, root / "config" / name)
    return root


def test_axis_values_expands_ranges_and_lists() -> None:
    """邻域轴支持数组与 minimum/maximum/step 区间。"""
    assert axis_values({"minimum": 24, "maximum": 96, "step": 24}, "x", True) == (24, 48, 72, 96)
    assert axis_values([1.0, 0.5, 0.5], "x", False) == (0.5, 1.0)
    with pytest.raises(ValueError):
        axis_values({"minimum": 10, "maximum": 5, "step": 1}, "x", True)
    with pytest.raises(ValueError):
        axis_values("bad", "x", False)


def test_loop_config_and_candidates(tmp_path: Path) -> None:
    """循环配置解析、邻域网格、结构 challenger 与预算登记。"""
    root = _prepare_root(tmp_path)
    loop = load_loop_config(root, root / "config" / "search_loop_selfcheck.json")
    assert loop.family_scope == (
        "breakout", "flow_trend", "grid_shadow", "mean_reversion", "price_breakout", "trend",
    )
    research, config_hash, _root_hash, _depth = load_governed_strategy_config(
        root, loop.research_config_path,
    )
    base = build_family_batches(research, ("trend",))[0].candidates
    neighborhood = neighborhood_candidates(
        "trend", research, loop.neighborhood["trend"], base,
    )
    assert len(neighborhood) == 5 * 5 * 1 * 3 * 1
    assert all(candidate.family == "trend" for candidate in neighborhood)
    candidates = generate_loop_candidates(research, config_hash, loop)
    sources = set(candidates.sources.values())
    assert sources == {SOURCE_REGISTERED_GRID, SOURCE_NEIGHBORHOOD, SOURCE_STRUCTURAL}
    assert candidates.budgets["trend"]["registered_grid"] == 6
    assert candidates.budgets["trend"]["structural_challengers"] == 2
    assert set(candidates.lookbacks) >= {24, 48, 72, 96, 168}
    labels = {label for label in candidates.templates if "~" in label}
    assert labels and all(label.split("~")[0] in loop.family_scope for label in labels)
    registry = candidates.registry_payload()
    assert registry["candidate_count"] == len(candidates.candidates)
    assert candidates.plan["search_plan_id"].startswith("search-plan-")
    cost = loop_cost_model(research)
    assert cost["one_way_cost_rate"] == pytest.approx(0.001)
    assert cost["maximum_gap_seconds"] == 4 * 3600.0


def test_loop_budget_violation_rejected(tmp_path: Path) -> None:
    """邻域网格超过每流派预算即拒绝。"""
    root = _prepare_root(tmp_path)
    path = root / "config" / "search_loop_selfcheck.json"
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw["candidate_budget_per_family"] = 8
    path.write_text(json.dumps(raw), encoding="utf-8")
    loop = load_loop_config(root, path)
    research, config_hash, _root_hash, _depth = load_governed_strategy_config(
        root, loop.research_config_path,
    )
    with pytest.raises(ValueError, match="循环预算"):
        generate_loop_candidates(research, config_hash, loop)


@pytest.mark.skipif(torch_module_or_none() is None, reason="torch 不可用")
def test_synthetic_loop_end_to_end_and_promotion(tmp_path: Path) -> None:
    """合成模式端到端：台账全量、manifest、proposal 与提案采纳。"""
    from guvolu.search.loop import run_search_loop

    root = _prepare_root(tmp_path)
    path = root / "config" / "search_loop_selfcheck.json"
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw["family_scope"] = ["trend", "price_breakout"]
    raw["synthetic"] = {"bars": 2560, "seed": 20260823}
    raw["structural_challengers"]["limit_per_family"] = 1
    raw["parity_candidate_limit"] = 16
    path.write_text(json.dumps(raw), encoding="utf-8")
    result = run_search_loop(root, path, synthetic=True, device="cpu")
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["search_run_id"] == result.search_run_id
    assert manifest["synthetic"] is True
    assert manifest["runtime"]["torch_version"]
    counts = manifest["stage_counts"]
    registry = json.loads(
        (result.run_directory / "candidate-registry.json").read_text(encoding="utf-8"),
    )
    assert counts["total"] == registry["candidate_count"]
    assert counts["F0_rejected"] + counts["F1_screened"] == counts["total"]
    assert counts["F3_checked"] <= 16
    _header, rows = read_ledger(result.result_directory / manifest["artifacts"]["trial_ledger"])
    assert len(rows) == counts["total"]
    assert all("resample" in row for row in rows if row["stage"] == "F1_screened")
    proposal = json.loads(result.proposal_path.read_text(encoding="utf-8"))
    assert proposal["search_run_id"] == result.search_run_id
    assert proposal["status"] == "proposal_only"
    assert set(proposal["families"]) == {"trend", "price_breakout"}
    assert proposal["structural_challengers"]
    for family, item in proposal["families"].items():
        assert item["status"] in ("proposed", "unchanged_grid", "no_proposal")
        if item["status"] == "proposed":
            assert item["proposed_grid_count"] <= item["candidate_budget"]
            assert item["anchor"]["exact"] is True
            assert item["anchor"]["flatness"]["flat"] is True
            for axis in item["axis_evidence"].values():
                assert axis
    proposed = [
        family for family, item in proposal["families"].items()
        if item["status"] == "proposed"
    ]
    if proposed:
        promotion = promoted_config(root, result.proposal_path, proposed)
        assert promotion.applied_families == tuple(sorted(proposed))
        output = write_promoted_config(root, promotion, root / "config")
        assert output.name.startswith("strategy_research_candidate_")
        config, _hash, _root_hash, depth = load_governed_strategy_config(root, output)
        assert depth == 0 and "evolution_parent" not in config
        assert config["search_loop_source"]["search_run_id"] == result.search_run_id
        assert research_command(root, output).startswith("python scripts/run_strategy_research.py")
    else:
        with pytest.raises(ValueError, match="没有可采纳"):
            promoted_config(root, result.proposal_path)
