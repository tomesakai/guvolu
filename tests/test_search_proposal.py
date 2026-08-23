"""受约束提案与 promote 的合同测试（纯 CPU，手工证据）。"""
from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from guvolu.search.promote import load_proposal, promoted_config, write_promoted_config
from guvolu.search.proposal import (
    PROPOSAL_METHOD_VERSION,
    STATUS_NO_PROPOSAL,
    STATUS_PROPOSED,
    ProposalThresholds,
    build_proposal,
    one_axis_neighbors,
)
from guvolu.strategy.contracts import CandidateSpec
from guvolu.strategy.expression import candidate_identity, expression_id, strategy_expression

ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = strategy_expression("trend")


def _candidate(lookback: int, entry: float) -> CandidateSpec:
    """构造趋势候选。"""
    parameters = {
        "lookback": lookback,
        "entry_score": entry,
        "exit_score": 0.0,
        "annual_volatility_target": 0.4,
        "maximum_target": 1.0,
    }
    return CandidateSpec(
        candidate_id=candidate_identity(TEMPLATE, parameters),
        family="trend",
        mode="paper",
        parameters=parameters,
        complexity=5,
        expression_id=expression_id(TEMPLATE),
    )


class _Candidates:
    """最小候选集合视图。"""

    def __init__(self, items: list[CandidateSpec]) -> None:
        self.candidates = {item.candidate_id: item for item in items}
        self.labels = {item.candidate_id: "trend" for item in items}
        self.sources = {item.candidate_id: "neighborhood_grid" for item in items}
        self.budgets = {"trend": {"candidate_budget": 64}}


def _rows(items: list[CandidateSpec], sharpes: dict[str, float], exact: set[str]):
    """构造试验台账与对照台账行。"""
    trial = []
    parity = []
    for item in items:
        sharpe = sharpes[item.candidate_id]
        trial.append({
            "record_type": "search_trial",
            "stage": "F1_screened",
            "candidate_id": item.candidate_id,
            "evaluation_id": "evaluation-" + item.candidate_id[-8:],
            "family": "trend",
            "screen_passed": sharpe > 0,
            "metrics": {"sharpe": sharpe, "turnover": 1.0},
            "resample": {
                "oos_sharpe": sharpe,
                "oos_net_return": sharpe,
                "bootstrap_sharpe_lower_bound": sharpe - 0.5,
                "bootstrap_p_value": 0.01,
                "positive_fold_ratio": 0.7,
                "family_pbo": 0.2,
            },
        })
        if item.candidate_id in exact:
            parity.append({
                "candidate_id": item.candidate_id,
                "promotable": True,
                "parity": {"passed": True},
            })
    return trial, parity


RESEARCH = {
    "strategies": {
        "trend": {
            "lookbacks": [24, 72],
            "entry_scores": [0.5, 1.0],
            "exit_score": 0.0,
            "annual_volatility_target": 0.4,
            "maximum_target": 1.0,
        },
    },
    "evolution": {
        "maximum_candidates_per_family": 9,
        "constraints": {"trend": {"lookback": {"minimum": 12, "maximum": 200}}},
    },
}


def test_one_axis_neighbors_follow_validation_rule() -> None:
    """邻居为其他参数不变时各轴最近的上下取值。"""
    items = [_candidate(l, e) for l in (24, 48, 72) for e in (0.5, 1.0)]
    selected = _candidate(48, 0.5)
    neighbors = one_axis_neighbors(selected, items)
    pairs = sorted((c.parameters["lookback"], c.parameters["entry_score"]) for c in neighbors)
    assert pairs == [(24, 0.5), (48, 1.0), (72, 0.5)]


def test_build_proposal_constrained_grid_and_budget() -> None:
    """提案取锚点一轴切片上的正向取值，受 constraints 与研究预算约束。"""
    lookbacks = (24, 48, 72, 96, 240)
    entries = (0.5, 1.0, 1.5)
    items = [_candidate(l, e) for l in lookbacks for e in entries]
    sharpes = {
        item.candidate_id: (
            2.0 if item.parameters["lookback"] == 48 and item.parameters["entry_score"] == 1.0
            else 1.2 if item.parameters["lookback"] in (24, 72, 240)
            or item.parameters["entry_score"] in (0.5, 1.5)
            else -0.5
        )
        for item in items
    }
    anchor = _candidate(48, 1.0)
    sharpes[_candidate(96, 1.0).candidate_id] = -1.0
    exact = {anchor.candidate_id, _candidate(24, 1.0).candidate_id}
    trial, parity = _rows(items, sharpes, exact)
    thresholds = ProposalThresholds(maximum_axis_values=3)
    proposal = build_proposal(
        "search-run-test",
        {"research_config": {"path": "config/x.json", "sha256": "0" * 64}},
        RESEARCH,
        _Candidates(items),
        trial,
        parity,
        thresholds,
    )
    assert proposal["proposal_method_version"] == PROPOSAL_METHOD_VERSION
    trend = proposal["families"]["trend"]
    assert trend["status"] == STATUS_PROPOSED
    strategy = trend["proposed_strategy"]
    assert strategy["lookbacks"] == [24, 48, 72]
    assert strategy["entry_scores"] == [0.5, 1.0, 1.5]
    assert trend["proposed_grid_count"] == 9
    assert trend["rejected_by_constraint"] == {"lookback": [240.0]}
    assert trend["anchor"]["candidate_id"] == anchor.candidate_id
    assert set(trend["axis_evidence"]) == {"lookback", "entry_score"}
    assert "48" in trend["axis_evidence"]["lookback"] or "48.0" in trend["axis_evidence"]["lookback"]
    out_of_bounds = {anchor.candidate_id, _candidate(240, 1.0).candidate_id}
    sharpes[_candidate(240, 1.0).candidate_id] = 5.0
    trial_b, parity_b = _rows(items, sharpes, out_of_bounds)
    bounded = build_proposal(
        "search-run-test",
        {"research_config": {"path": "config/x.json", "sha256": "0" * 64}},
        RESEARCH,
        _Candidates(items),
        trial_b,
        parity_b,
        thresholds,
    )
    assert bounded["families"]["trend"]["anchor"]["candidate_id"] == anchor.candidate_id
    assert bounded["families"]["trend"]["summary"]["exact_within_constraints"] == 1
    no_exact = build_proposal(
        "search-run-test",
        {"research_config": {"path": "config/x.json", "sha256": "0" * 64}},
        RESEARCH,
        _Candidates(items),
        trial,
        [],
        thresholds,
    )
    assert no_exact["families"]["trend"]["status"] == STATUS_NO_PROPOSAL


def test_promote_writes_lineage_root_with_source(tmp_path: Path) -> None:
    """promote 生成新谱系根配置并登记来源，过期提案拒绝。"""
    root = tmp_path / "repo"
    (root / "config").mkdir(parents=True)
    parent = root / "config" / "strategy_research_selfcheck.json"
    shutil.copy(ROOT / "config" / "strategy_research_selfcheck.json", parent)
    import hashlib
    parent_hash = hashlib.sha256(parent.read_bytes()).hexdigest()
    proposal = {
        "schema_version": 1,
        "proposal_method_version": PROPOSAL_METHOD_VERSION,
        "search_run_id": "search-run-test",
        "bundle_id": "search-bundle-test",
        "search_result_id": "search-result-test",
        "parent_research_config": {
            "path": "config/strategy_research_selfcheck.json",
            "sha256": parent_hash,
        },
        "families": {
            "trend": {
                "status": STATUS_PROPOSED,
                "proposed_strategy": {
                    "lookbacks": [48, 72, 96],
                    "entry_scores": [0.5, 0.75],
                    "exit_score": 0.0,
                    "annual_volatility_target": 0.8,
                    "maximum_target": 1.0,
                },
            },
            "breakout": {"status": STATUS_NO_PROPOSAL},
        },
    }
    proposal_path = root / "reports" / "proposal.json"
    proposal_path.parent.mkdir(parents=True)
    proposal_path.write_text(json.dumps(proposal), encoding="utf-8")
    loaded, digest = load_proposal(proposal_path)
    assert loaded["search_run_id"] == "search-run-test" and len(digest) == 64
    result = promoted_config(root, proposal_path)
    assert result.applied_families == ("trend",)
    assert result.skipped_families == {"breakout": STATUS_NO_PROPOSAL}
    assert result.config["strategies"]["trend"]["lookbacks"] == [48, 72, 96]
    assert result.config["features"]["lookbacks"] == [24, 48, 72, 96, 168]
    assert "evolution_parent" not in result.config
    source = result.config["search_loop_source"]
    assert source["parent_config_sha256"] == parent_hash
    assert source["proposal_sha256"] == digest
    output = write_promoted_config(root, result)
    assert output.parent == root / "config"
    assert output.name.startswith("strategy_research_candidate_")
    with pytest.raises(ValueError, match="提案不含流派"):
        promoted_config(root, proposal_path, ["grid_shadow"])
    with pytest.raises(ValueError, match="没有可采纳"):
        promoted_config(root, proposal_path, ["breakout"])
    parent.write_text(parent.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="提案已过期"):
        promoted_config(root, proposal_path)
    proposal["families"]["trend"]["proposed_strategy"]["lookbacks"] = list(range(24, 24 * 40, 24))
    proposal["parent_research_config"]["sha256"] = hashlib.sha256(parent.read_bytes()).hexdigest()
    proposal_path.write_text(json.dumps(proposal), encoding="utf-8")
    with pytest.raises(ValueError, match="预算"):
        promoted_config(root, proposal_path)
