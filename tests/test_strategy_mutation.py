"""有界 typed 结构 challenger 的身份、类型与预算测试。"""
from __future__ import annotations

import math
from datetime import UTC, datetime

import pytest

from guvolu.strategy.contracts import FeatureRow, ResearchBar
from guvolu.strategy.expression import (
    evaluate_expression,
    expression_id,
    strategy_expression,
    validate_strategy_expression,
)
from guvolu.strategy.mutation import (
    bounded_typed_crossovers,
    bounded_typed_mutations,
    structural_challenger_registry_payload,
)


FAMILIES = (
    "breakout",
    "flow_trend",
    "grid_shadow",
    "mean_reversion",
    "trend",
)


def _runtime_row() -> tuple[ResearchBar, FeatureRow]:
    """构造覆盖五流派字段的固定 CPU reference 输入。"""
    opened = datetime(2026, 1, 1, tzinfo=UTC)
    decided = datetime(2026, 1, 1, 1, tzinfo=UTC)
    return ResearchBar(
        open_time=opened,
        decision_time=decided,
        latest_available_time=decided,
        open=110.0,
        high=111.0,
        low=109.0,
        close=110.0,
        base_volume=1.0,
        quote_volume=110.0,
        signed_base_volume=1.0,
        trade_count=1,
    ), FeatureRow(
        decision_time=decided,
        as_of=decided,
        return_one=0.0,
        trend_scores={24: 1.0},
        volatility={24: 0.01},
        price_scores={24: -1.0},
        prior_highs={24: 100.0},
        prior_lows={24: 90.0},
        flow_imbalance=1.0,
        volume_score=1.0,
        jump_score=0.0,
        contiguous=True,
    )


def _runtime_parameters() -> dict[str, int | float]:
    """返回所有首版模板都能解析的参数域。"""
    return {
        "lookback": 24,
        "entry_score": 1.0,
        "exit_score": 0.0,
        "trend_limit": 0.75,
        "flow_confirmation": 0.0,
        "minimum_volume_score": 0.0,
        "annual_volatility_target": 0.4,
        "maximum_target": 1.0,
    }


@pytest.mark.parametrize("family", FAMILIES)
def test_bounded_mutations_are_typed_unique_and_deterministic(family: str) -> None:
    """每个流派的单点结构变异必须可验证、唯一且可重建。"""
    template = strategy_expression(family)
    first = bounded_typed_mutations(template, limit=16)
    second = bounded_typed_mutations(template, limit=16)
    assert first == second
    assert first
    assert len({item.expression_id for item in first}) == len(first)
    for challenger in first:
        validate_strategy_expression(challenger.expression)
        assert challenger.family == family
        assert challenger.parent_expression_id == expression_id(template)
        assert challenger.expression_id == expression_id(challenger.expression)
        assert challenger.expression_id != challenger.parent_expression_id


def test_typed_crossover_reuses_only_recipient_parameter_schema() -> None:
    """交叉子树必须在 recipient 参数域中重新通过类型检查。"""
    recipient = strategy_expression("flow_trend")
    donor = strategy_expression("trend")
    challengers = bounded_typed_crossovers(recipient, donor, limit=8)
    assert challengers
    assert challengers == bounded_typed_crossovers(recipient, donor, limit=8)
    for challenger in challengers:
        validate_strategy_expression(challenger.expression)
        assert challenger.family == "flow_trend"
        assert challenger.donor_family == "trend"
        assert challenger.donor_expression_id == expression_id(donor)
        assert challenger.donor_path is not None


@pytest.mark.parametrize("family", FAMILIES)
def test_structural_challengers_execute_in_cpu_reference(family: str) -> None:
    """类型通过的结构挑战者还必须能由独立 CPU reference 实际求值。"""
    bar, feature = _runtime_row()
    parameters = _runtime_parameters()
    template = strategy_expression(family)
    challengers = list(bounded_typed_mutations(template, limit=16))
    if family == "flow_trend":
        challengers.extend(bounded_typed_crossovers(
            template,
            strategy_expression("trend"),
            limit=4,
        ))
    assert challengers
    for challenger in challengers:
        roots = (
            *challenger.expression.required,
            challenger.expression.entry,
            challenger.expression.exit,
            challenger.expression.target,
        )
        for root in roots:
            if root is None:
                continue
            value = evaluate_expression(root, parameters, bar, feature)
            assert value is None or isinstance(value, (bool, float))
            if isinstance(value, float):
                assert math.isfinite(value)


def test_structural_registry_enforces_projected_candidate_budget() -> None:
    """结构数乘参数网格后的总候选数不得突破流派预算。"""
    template = strategy_expression("breakout")
    challengers = bounded_typed_mutations(template, limit=3)
    assert len(challengers) == 3
    payload = structural_challenger_registry_payload(
        "breakout",
        "config-hash",
        source_candidate_ids=tuple(f"candidate-{index}" for index in range(6)),
        source_search_plan_id="search-plan-source",
        generator_method_version="generator-test",
        candidate_budget=24,
        challengers=challengers,
    )
    assert payload["status"] == "unregistered_structural_challengers"
    assert payload["projected_candidate_count"] == 24
    assert payload["source"] == {
        "generator_method_version": "generator-test",
        "search_plan_id": "search-plan-source",
        "candidate_ids": [f"candidate-{index}" for index in range(6)],
    }
    assert payload["holdout_consumed"] is False
    assert "cannot be promoted" in str(payload["activation_contract"])
    with pytest.raises(ValueError, match="投影候选数超过预算"):
        structural_challenger_registry_payload(
            "breakout",
            "config-hash",
            source_candidate_ids=tuple(
                f"candidate-{index}" for index in range(6)
            ),
            source_search_plan_id="search-plan-source",
            generator_method_version="generator-test",
            candidate_budget=18,
            challengers=challengers,
        )


def test_structural_search_rejects_unknown_operator_and_negative_limit() -> None:
    """调用方不能注入未登记算子或绕过非负预算。"""
    template = strategy_expression("trend")
    with pytest.raises(ValueError, match="未知结构变异算子"):
        bounded_typed_mutations(template, ("unknown",), 1)
    with pytest.raises(ValueError, match="不得为负"):
        bounded_typed_crossovers(
            template,
            strategy_expression("flow_trend"),
            -1,
        )
