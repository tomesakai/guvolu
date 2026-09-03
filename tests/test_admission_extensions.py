"""准入扩展：部署候选规则、基准超额门与搜索试验计入 DSR。"""
from __future__ import annotations

import copy
import json
import random
from collections import Counter
from collections.abc import Mapping
from pathlib import Path

import pytest

from guvolu.research.artifact_contracts import family_payload
from guvolu.research.validation import (
    BENCHMARK_EXCESS_REASON,
    _effective_trial_count,
    parse_admission_extensions,
    walk_forward_validate,
)
from guvolu.search.promote import (
    effective_trial_count_from_fold_scores,
    family_trial_evidence,
)
from guvolu.search.synthetic import synthetic_panel, synthetic_strategy_config
from guvolu.strategy.generation import build_family_batches

LOOKBACKS = (4, 8)
BASE_CONFIG: Mapping[str, object] = {
    "bar_interval": "1hour",
    "cost_model": {
        "fee_bps_assumption": 7.0,
        "half_spread_bps_assumption": 0.0,
        "slippage_bps_assumption": 0.0,
        "impact_bps_assumption": 0.0,
        "capacity_notional_quote": 0.0,
    },
    "features": {
        "lookbacks": list(LOOKBACKS),
        "state_lookback": 8,
        "volume_lookback": 8,
        "maximum_structural_gap_bars_assumption": 2,
    },
    "walk_forward": {
        "minimum_train_bars": 300,
        "test_bars": 100,
        "step_bars": 100,
        "embargo_bars": 4,
    },
    "validation": {
        "minimum_oos_bars": 10,
        "minimum_oos_sharpe": -100.0,
        "maximum_drawdown": 1.0,
        "maximum_fdr_q": 1.0,
        "minimum_positive_fold_ratio": 0.0,
        "maximum_probability_backtest_overfitting": 1.0,
        "pbo_split_budget": 64,
        "pbo_random_seed": 7,
        "block_bootstrap_bars": 24,
        "block_bootstrap_samples": 128,
        "block_bootstrap_random_seed": 11,
        "block_bootstrap_one_sided_alpha": 0.05,
        "maximum_block_bootstrap_p_value": 1.0,
        "minimum_deflated_sharpe_probability": 0.0,
        "deflated_sharpe_gate_trial_count": "effective",
        "minimum_parameter_neighbor_count": 1,
        "minimum_positive_parameter_neighbor_ratio": 0.0,
        "minimum_median_parameter_neighbor_sharpe_retention": -100.0,
        "complexity_penalty": 0.0,
    },
}


def _config(**validation_overrides: object) -> dict[str, object]:
    config = copy.deepcopy(dict(BASE_CONFIG))
    validation = dict(config["validation"])  # type: ignore[arg-type]
    validation.update(validation_overrides)
    config["validation"] = validation
    return config


def _validate(config: Mapping[str, object]):  # type: ignore[no-untyped-def]
    bars, features = synthetic_panel(800, LOOKBACKS, 23)
    candidates = build_family_batches(
        synthetic_strategy_config(LOOKBACKS), ("trend",),
    )[0].candidates
    return walk_forward_validate(
        "admission-test", bars, features, candidates, config,
    )


def test_legacy_config_has_no_admission_extension() -> None:
    """未声明扩展时解析为 None，摘要不新增键。"""
    assert parse_admission_extensions(BASE_CONFIG["validation"], None) is None  # type: ignore[arg-type]
    result = _validate(BASE_CONFIG)
    assert result.admission_extensions is None
    for record in family_payload(result):
        assert "admission_extensions" not in record
    family = result.families[0]
    assert family.deployment_candidate_rule is None
    assert family.search_trial_count == 0


def test_most_selected_rule_picks_fold_champion_mode() -> None:
    """部署候选为各折训练冠军的众数，不取全样本冠军。"""
    result = _validate(_config(
        deployment_candidate_rule="most_selected_fold_champion",
    ))
    family = result.families[0]
    counts = Counter(family.fold_selected_candidate_ids)
    top = max(counts.values())
    expected = min(
        identifier for identifier, value in counts.items() if value == top
    )
    assert family.deployment_candidate.candidate_id == expected
    assert family.deployment_candidate_rule == "most_selected_fold_champion"
    payload = family_payload(result)[0]
    assert payload["admission_extensions"]["deployment_candidate_rule"] == (
        "most_selected_fold_champion"
    )


def test_benchmark_excess_gate_uses_fixed_long_on_same_mask() -> None:
    """基准超额门以同掩码固定多头为对照，阈值不可达即拒绝。"""
    loose = _validate(_config(minimum_benchmark_sharpe_excess=-100.0))
    strict = _validate(_config(minimum_benchmark_sharpe_excess=100.0))
    loose_family = loose.families[0]
    strict_family = strict.families[0]
    assert loose_family.benchmark_sharpe == strict_family.benchmark_sharpe
    assert loose_family.benchmark_sharpe_excess == pytest.approx(
        loose_family.metrics.sharpe - loose_family.benchmark_sharpe  # type: ignore[operator]
    )
    assert BENCHMARK_EXCESS_REASON not in loose_family.rejection_reasons
    assert BENCHMARK_EXCESS_REASON in strict_family.rejection_reasons
    payload = family_payload(strict)[0]["admission_extensions"]
    assert payload["benchmark_sharpe"] == strict_family.benchmark_sharpe


def test_external_trials_raise_trial_count_and_dsr_benchmark() -> None:
    """搜索循环试验计入原始与有效试验数，并抬高 DSR 基准。"""
    plain = _validate(_config(deployment_candidate_rule="full_sample"))
    config = _config(deployment_candidate_rule="full_sample")
    config["search_loop_source"] = {
        "family_trials": {
            "trend": {
                "evaluated": 2000,
                "screen_passed": 900,
                "effective_trial_count": 400.0,
                "annual_sharpe_std": 0.8,
                "ledger_sha256": "a" * 64,
            },
        },
    }
    counted = _validate(config)
    before = plain.families[0]
    after = counted.families[0]
    assert after.raw_trial_count == before.raw_trial_count + 2000
    assert after.effective_trial_count >= 400.0
    assert after.search_trial_count == 2000
    assert after.search_effective_trial_count == 400.0
    assert after.deflated_sharpe_benchmark_effective > (
        before.deflated_sharpe_benchmark_effective
    )
    assert after.deflated_sharpe_probability_effective <= (
        before.deflated_sharpe_probability_effective
    )
    with pytest.raises(ValueError, match="effective_trial_count"):
        parse_admission_extensions({}, {"family_trials": {"trend": {
            "evaluated": 10, "screen_passed": 1,
            "effective_trial_count": 11.0, "annual_sharpe_std": 0.1,
            "ledger_sha256": "a" * 64,
        }}})


def test_gram_effective_trial_count_matches_pairwise() -> None:
    """Gram 算法与逐对相关算法结果一致，含常量向量。"""
    generator = random.Random(5)
    vectors = [
        [generator.gauss(0.0, 1.0) for _ in range(7)] for _ in range(40)
    ]
    vectors.append([0.3] * 7)
    vectors.append([0.3] * 7)
    vectors.append([-1.0] * 7)
    fast = effective_trial_count_from_fold_scores(vectors)
    slow = _effective_trial_count({
        f"c{index:03d}": tuple(vector) for index, vector in enumerate(vectors)
    })
    assert fast == pytest.approx(slow, rel=1e-9)


def test_family_trial_evidence_reads_search_ledger(tmp_path: Path) -> None:
    """从搜索台账汇总评估数、粗筛数、有效试验数与离散度。"""
    run = tmp_path / "reports" / "strategy-search" / "search-run-x"
    result_dir = run / "search-result-y"
    result_dir.mkdir(parents=True)
    rows = [{
        "record_type": "search_trial_ledger_header",
    }]
    for index in range(6):
        rows.append({
            "record_type": "search_trial",
            "family": "trend" if index < 4 else "trend~abc",
            "screen_passed": index % 2 == 0,
            "metrics": {"sharpe": 0.5 + 0.1 * index},
            "resample": {"fold_test_sharpe": [0.1 * index, 0.2, -0.1 * index]},
        })
    ledger = result_dir / "trial-ledger-sha256-z.jsonl"
    ledger.write_text(
        "\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8",
    )
    proposal_path = run / "proposal.json"
    proposal_path.write_text("{}", encoding="utf-8")
    evidence = family_trial_evidence(
        tmp_path, proposal_path, {"search_result_id": "search-result-y"},
        ["trend"],
    )
    trend = evidence["trend"]
    # 结构 challenger 计入父流派
    assert trend["evaluated"] == 6
    assert trend["screen_passed"] == 3
    assert 1.0 <= trend["effective_trial_count"] <= 4.0  # type: ignore[operator]
    assert trend["annual_sharpe_std"] > 0
    assert len(str(trend["ledger_sha256"])) == 64
    with pytest.raises(ValueError, match="没有流派试验"):
        family_trial_evidence(
            tmp_path, proposal_path, {"search_result_id": "search-result-y"},
            ["breakout"],
        )
