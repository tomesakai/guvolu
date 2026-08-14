"""类型化策略表达式、候选身份与独立流派生成测试。"""
from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

import pytest

from guvolu.strategy.baselines import generate_targets
from guvolu.strategy.contracts import FeatureRow, ResearchBar
from guvolu.strategy.expression import (
    ExpressionNode,
    Unit,
    candidate_identity,
    evaluate_expression,
    expression_id,
    strategy_expression,
    strategy_expression_payload,
    validate_strategy_expression,
)
from guvolu.strategy.generation import (
    build_family_batches,
    candidate_registry_payload,
    candidate_search_plan_payload,
)
from guvolu.strategy.search_plan import evaluate_search_plan_candidate


FAMILIES = (
    "breakout",
    "flow_trend",
    "grid_shadow",
    "mean_reversion",
    "trend",
)


def _trend_only_config() -> dict[str, object]:
    """返回不依赖其他流派配置的最小趋势生成配置。"""
    return {
        "features": {"lookbacks": [24]},
        "strategies": {
            "trend": {
                "lookbacks": [24],
                "entry_scores": [1.0],
                "exit_score": 0.0,
                "annual_volatility_target": 0.4,
                "maximum_target": 1.0,
            },
        },
    }


def _row(
    *,
    trend: float,
    price_score: float,
    prior_high: float,
    flow: float | None = 1.0,
    volume: float | None = 1.0,
    jump: float | None = 0.0,
) -> tuple[ResearchBar, FeatureRow]:
    """构造一行可直接判断信号的固定研究数据。"""
    opened = datetime(2026, 1, 1, tzinfo=UTC)
    decided = datetime(2026, 1, 1, 1, tzinfo=UTC)
    bar = ResearchBar(
        open_time=opened,
        decision_time=decided,
        latest_available_time=decided,
        open=110.0,
        high=110.0,
        low=110.0,
        close=110.0,
        base_volume=1.0,
        quote_volume=110.0,
        signed_base_volume=1.0,
        trade_count=1,
    )
    feature = FeatureRow(
        decision_time=decided,
        as_of=decided,
        return_one=0.0,
        trend_scores={24: trend},
        volatility={24: 0.001},
        price_scores={24: price_score},
        prior_highs={24: prior_high},
        prior_lows={24: 90.0},
        flow_imbalance=flow,
        volume_score=volume,
        jump_score=jump,
        contiguous=True,
    )
    return bar, feature


def test_registered_expressions_validate_and_have_unique_identity() -> None:
    """每个流派必须有独立、可验证的表达式身份。"""
    identifiers = []
    for family in FAMILIES:
        template = strategy_expression(family)
        validate_strategy_expression(template)
        payload = strategy_expression_payload(template)
        assert payload["family"] == family
        assert payload["expression_method_version"] == "typed-signal-expression-v1"
        identifiers.append(expression_id(template))
    assert len(set(identifiers)) == len(FAMILIES)


def test_expression_identity_canonicalizes_sets_and_commutative_and() -> None:
    """必要字段和 AND 子句换序不得改变表达式身份。"""
    template = strategy_expression("breakout")
    assert template.entry is not None
    reordered = replace(
        template,
        required=tuple(reversed(template.required)),
        entry=replace(template.entry, args=tuple(reversed(template.entry.args))),
    )
    assert expression_id(reordered) == expression_id(template)


def test_expression_type_error_reports_stable_ast_path() -> None:
    """单位错误必须在解析期拒绝并给出稳定 AST 路径。"""
    template = strategy_expression("trend")
    invalid = replace(
        template,
        entry=ExpressionNode(
            "ge",
            (
                ExpressionNode("close"),
                ExpressionNode("constant", value=0.0, unit=Unit.DIMENSIONLESS),
            ),
        ),
    )
    with pytest.raises(ValueError, match=r"^E_COMPARE_TYPE:entry:"):
        validate_strategy_expression(invalid)


def test_candidate_identity_normalizes_equivalent_numeric_parameters() -> None:
    """同义整数、浮点与正负零不得产生重复候选身份。"""
    template = strategy_expression("trend")
    integers = {
        "lookback": 24,
        "entry_score": 1,
        "exit_score": -0.0,
        "annual_volatility_target": 1,
        "maximum_target": 1,
    }
    floats = {
        "lookback": 24,
        "entry_score": 1.0,
        "exit_score": 0.0,
        "annual_volatility_target": 1.0,
        "maximum_target": 1.0,
    }
    assert candidate_identity(template, integers) == candidate_identity(
        template,
        floats,
    )


def test_single_family_generation_does_not_require_unrelated_configs() -> None:
    """单流派脚本生成不得被其他流派配置耦合。"""
    batches = build_family_batches(_trend_only_config(), ("trend",))
    assert len(batches) == 1
    assert batches[0].family == "trend"
    assert len(batches[0].candidates) == 1
    candidate = batches[0].candidates[0]
    assert candidate.expression_id == expression_id(strategy_expression("trend"))
    registry = candidate_registry_payload(batches, "config-hash")
    assert registry["schema_version"] == 2
    family = registry["families"][0]  # type: ignore[index]
    assert family["expression_id"] == candidate.expression_id  # type: ignore[index]
    assert family["expression"]["family"] == "trend"  # type: ignore[index]
    assert family["candidate_budget"] == 1  # type: ignore[index]
    search_plan = registry["search_plan"]
    assert search_plan["search_plan_id"].startswith("search-plan-")  # type: ignore[index, union-attr]
    legacy = candidate_registry_payload(
        batches,
        "config-hash",
        "scripted-typed-family-grid-v3",
    )
    assert legacy["schema_version"] == 1
    assert "search_plan" not in legacy
    legacy_family = legacy["families"][0]  # type: ignore[index]
    assert "candidate_budget" not in legacy_family


def test_search_plan_deduplicates_typed_nodes_and_is_topological() -> None:
    """多流派计划必须共享同型子表达式并保持子节点先于父节点。"""
    config = _trend_only_config()
    strategies = config["strategies"]
    assert isinstance(strategies, dict)
    strategies["flow_trend"] = {
        "lookbacks": [24],
        "entry_scores": [1.0],
        "flow_confirmations": [0.0],
        "minimum_volume_score": 0.0,
        "exit_score": 0.0,
        "annual_volatility_target": 0.4,
        "maximum_target": 1.0,
    }
    batches = build_family_batches(config, ("flow_trend", "trend"))
    plan = candidate_search_plan_payload(batches)
    nodes = plan["nodes"]
    order = plan["evaluation_order"]
    assert isinstance(nodes, list)
    assert isinstance(order, list)
    trend_nodes = [node for node in nodes if node["op"] == "trend_score"]
    assert len(trend_nodes) == 1
    positions = {str(node_id): index for index, node_id in enumerate(order)}
    for node in nodes:
        assert isinstance(node, dict)
        for child_id in node["args"]:
            assert positions[str(child_id)] < positions[str(node["node_id"])]
    reversed_plan = candidate_search_plan_payload(tuple(reversed(batches)))
    assert reversed_plan == plan


def test_family_generation_rejects_candidate_budget_overflow() -> None:
    """参数轴在生成阶段即不得突破预登记候选预算。"""
    config = _trend_only_config()
    strategies = config["strategies"]
    assert isinstance(strategies, dict)
    trend = strategies["trend"]
    assert isinstance(trend, dict)
    trend["entry_scores"] = [0.5, 1.0]
    config["evolution"] = {"maximum_candidates_per_family": 1}
    with pytest.raises(ValueError, match="策略家族候选超过预算"):
        build_family_batches(config, ("trend",))


@pytest.mark.parametrize(
    ("family", "feature_values"),
    (
        ("trend", {"trend": 10.0, "price_score": 1.0, "prior_high": 100.0}),
        (
            "flow_trend",
            {"trend": 10.0, "price_score": 1.0, "prior_high": 100.0},
        ),
        ("breakout", {"trend": 1.0, "price_score": 1.0, "prior_high": 100.0}),
        (
            "mean_reversion",
            {"trend": 0.0, "price_score": -10.0, "prior_high": 100.0},
        ),
        (
            "grid_shadow",
            {"trend": 0.0, "price_score": -10.0, "prior_high": 100.0},
        ),
    ),
)
def test_cpu_reference_generates_targets_for_each_family(
    family: str,
    feature_values: dict[str, float],
) -> None:
    """五个流派必须通过同一个 CPU reference 产生有限目标。"""
    config = _trend_only_config()
    if family != "trend":
        strategies = config["strategies"]
        assert isinstance(strategies, dict)
        strategies.clear()
        strategies[family] = {
            "lookbacks": [24],
            "entry_scores": [1.0],
            "exit_score": 0.0,
            "trend_limit": 1.0,
            "flow_confirmations": [0.0],
            "minimum_volume_score": 0.0,
            "annual_volatility_target": 0.4,
            "maximum_target": 1.0,
        }
    candidate = build_family_batches(config, (family,))[0].candidates[0]
    bar, feature = _row(**feature_values)
    targets = generate_targets(candidate, (bar,), (feature,), periods_per_year=1.0)
    assert len(targets) == 1
    assert 0.0 < targets[0] <= float(candidate.parameters["maximum_target"])
    plan = candidate_search_plan_payload(build_family_batches(config, (family,)))
    compiled = evaluate_search_plan_candidate(
        plan, candidate.candidate_id, bar, feature,
    )
    template = strategy_expression(family)
    assert compiled["required"] == tuple(
        evaluate_expression(node, candidate.parameters, bar, feature)
        for node in template.required
    )
    for name, node in (
        ("entry", template.entry),
        ("exit", template.exit),
        ("target", template.target),
    ):
        expected = (
            None if node is None
            else evaluate_expression(node, candidate.parameters, bar, feature)
        )
        assert compiled[name] == expected


def test_generate_targets_rejects_tampered_expression_identity() -> None:
    """注册表表达式身份被篡改后不得进入回测或发布。"""
    candidate = build_family_batches(_trend_only_config(), ("trend",))[0].candidates[0]
    tampered = replace(candidate, expression_id="expression-" + "0" * 64)
    bar, feature = _row(trend=10.0, price_score=1.0, prior_high=100.0)
    with pytest.raises(ValueError, match="候选表达式身份"):
        generate_targets(tampered, (bar,), (feature,), periods_per_year=1.0)
