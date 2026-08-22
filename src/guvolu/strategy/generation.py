"""可独立运行的策略家族候选生成合同。"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from guvolu.strategy.baselines import (
    STRATEGY_METHOD_VERSION,
    SUPPORTED_FAMILIES,
    build_candidates,
)
from guvolu.strategy.contracts import CandidateSpec
from guvolu.strategy.expression import (
    EXPRESSION_METHOD_VERSION,
    ExpressionNode,
    ExpressionType,
    expression_complexity,
    expression_id,
    infer_expression_type,
    strategy_expression,
    strategy_expression_payload,
)

LEGACY_GENERATOR_METHOD_VERSION = "scripted-typed-family-grid-v3"
GENERATOR_METHOD_VERSION = "scripted-typed-family-grid-v4"
SEARCH_PLAN_METHOD_VERSION = "typed-common-subexpression-dag-v1"


@dataclass(frozen=True)
class FamilyCandidateBatch:
    """一个流派独立生成的一批候选。"""

    family: str
    mode: str
    generator_method_version: str
    candidate_budget: int
    candidates: tuple[CandidateSpec, ...]


def _positive_integer(value: object, name: str) -> int:
    """验证正整数生成预算。"""
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{name} 必须为正整数")
    return value


def _candidate_budget(config: Mapping[str, object], fallback: int) -> int:
    """读取可选全局候选预算；最小配置退化为当前批次大小。"""
    raw_evolution = config.get("evolution")
    if raw_evolution is None:
        return max(fallback, 1)
    if not isinstance(raw_evolution, Mapping):
        raise ValueError("evolution 必须为对象")
    return _positive_integer(
        raw_evolution.get("maximum_candidates_per_family"),
        "evolution.maximum_candidates_per_family",
    )


def build_family_batches(
    config: Mapping[str, object],
    family_scope: Sequence[str] | None = None,
) -> tuple[FamilyCandidateBatch, ...]:
    """生成指定流派的确定性候选批次。"""
    requested = (
        SUPPORTED_FAMILIES
        if family_scope is None
        else tuple(sorted(set(family_scope)))
    )
    candidates = build_candidates(config, requested)
    raw_features = config.get("features")
    if not isinstance(raw_features, Mapping):
        raise ValueError("features 必须为对象")
    raw_lookbacks = raw_features.get("lookbacks")
    if not isinstance(raw_lookbacks, list):
        raise ValueError("features.lookbacks 必须为数组")
    feature_lookbacks = {int(value) for value in raw_lookbacks}
    missing_lookbacks = sorted({
        int(candidate.parameters["lookback"])
        for candidate in candidates
        if "lookback" in candidate.parameters
        and int(candidate.parameters["lookback"]) not in feature_lookbacks
    })
    if missing_lookbacks:
        raise ValueError(
            "策略回看窗缺少共享特征: "
            + ",".join(str(value) for value in missing_lookbacks)
        )
    batches: list[FamilyCandidateBatch] = []
    for family in requested:
        family_candidates = tuple(
            candidate for candidate in candidates if candidate.family == family
        )
        modes = {candidate.mode for candidate in family_candidates}
        if len(modes) != 1:
            raise ValueError(f"策略家族模式不唯一: {family}")
        budget = _candidate_budget(config, len(family_candidates))
        if len(family_candidates) > budget:
            raise ValueError(
                f"策略家族候选超过预算: {family}:"
                f"{len(family_candidates)}>{budget}"
            )
        batches.append(FamilyCandidateBatch(
            family=family,
            mode=next(iter(modes)),
            generator_method_version=GENERATOR_METHOD_VERSION,
            candidate_budget=budget,
            candidates=family_candidates,
        ))
    return tuple(batches)


def _canonical_bytes(value: object) -> bytes:
    """生成策略层内部使用的规范 JSON 字节。"""
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _type_payload(value: ExpressionType) -> Mapping[str, object]:
    """生成表达式节点的完整执行类型。"""
    return {
        "shape": value.shape.value,
        "unit": value.unit.value,
        "frequency": value.frequency,
        "availability": value.availability,
        "missing_policy": value.missing_policy,
        "numeric_domain": value.numeric_domain,
    }


def _compile_node(
    node: ExpressionNode,
    parameters: Mapping[str, ExpressionType],
    records: dict[str, Mapping[str, object]],
    depths: dict[str, int],
) -> str:
    """递归编译一个节点，并按类型化结构身份合并公共子表达式。"""
    child_ids = [
        _compile_node(child, parameters, records, depths)
        for child in node.args
    ]
    if node.op == "and":
        child_ids.sort()
    identity: dict[str, object] = {
        "op": node.op,
        "args": child_ids,
        "type": _type_payload(infer_expression_type(node, parameters)),
    }
    if node.value is not None:
        value = node.value
        if isinstance(value, float) and value == 0.0:
            value = 0.0
        identity["value"] = value
    if node.unit is not None:
        identity["unit"] = node.unit.value
    node_id = "expression-node-" + hashlib.sha256(
        _canonical_bytes(identity),
    ).hexdigest()
    depth = 0 if not child_ids else 1 + max(depths[item] for item in child_ids)
    record = {"node_id": node_id, **identity, "depth": depth}
    existing = records.get(node_id)
    if existing is not None and existing != record:
        raise ValueError("表达式节点身份冲突")
    records[node_id] = record
    depths[node_id] = depth
    return node_id


def candidate_search_plan_payload(
    batches: Sequence[FamilyCandidateBatch],
) -> Mapping[str, object]:
    """把多流派 AST 编译为 CPU/GPU 共享的类型化公共子表达式 DAG。"""
    ordered = tuple(sorted(batches, key=lambda item: item.family))
    if len({batch.family for batch in ordered}) != len(ordered):
        raise ValueError("搜索计划不能包含重复策略家族")
    records: dict[str, Mapping[str, object]] = {}
    depths: dict[str, int] = {}
    families: list[Mapping[str, object]] = []
    for batch in ordered:
        template = strategy_expression(batch.family)

        def compile_optional(node: ExpressionNode | None) -> str | None:
            return None if node is None else _compile_node(
                node, template.parameter_types, records, depths,
            )

        required = sorted(
            _compile_node(node, template.parameter_types, records, depths)
            for node in template.required
        )
        parameter_names = sorted(template.parameter_types)
        candidate_rows = []
        for candidate in sorted(
            batch.candidates,
            key=lambda item: item.candidate_id,
        ):
            if candidate.expression_id != expression_id(template):
                raise ValueError("候选表达式身份与搜索计划不一致")
            candidate_rows.append({
                "candidate_id": candidate.candidate_id,
                "values": [candidate.parameters[name] for name in parameter_names],
            })
        families.append({
            "family": batch.family,
            "mode": batch.mode,
            "expression_id": expression_id(template),
            "sizing": template.sizing,
            "candidate_count": len(batch.candidates),
            "candidate_budget": batch.candidate_budget,
            "parameter_names": parameter_names,
            "candidate_parameter_rows": candidate_rows,
            "roots": {
                "required": required,
                "entry": compile_optional(template.entry),
                "exit": compile_optional(template.exit),
                "target": compile_optional(template.target),
            },
        })
    evaluation_order = sorted(records, key=lambda item: (depths[item], item))
    body: dict[str, object] = {
        "schema_version": 1,
        "search_plan_method_version": SEARCH_PLAN_METHOD_VERSION,
        "expression_method_version": EXPRESSION_METHOD_VERSION,
        "evaluation_order": evaluation_order,
        "nodes": [records[item] for item in evaluation_order],
        "families": families,
    }
    return {
        **body,
        "search_plan_id": "search-plan-" + hashlib.sha256(
            _canonical_bytes(body),
        ).hexdigest(),
    }


def candidate_registry_payload(
    batches: Sequence[FamilyCandidateBatch],
    config_hash: str,
    generator_method_version: str = GENERATOR_METHOD_VERSION,
) -> Mapping[str, object]:
    """生成可持久化候选注册表。"""
    ordered = tuple(sorted(batches, key=lambda item: item.family))
    if generator_method_version not in {
        LEGACY_GENERATOR_METHOD_VERSION,
        GENERATOR_METHOD_VERSION,
    }:
        raise ValueError("候选注册表生成方法版本不受支持")
    families = [{
        "family": batch.family,
        "mode": batch.mode,
        "candidate_count": len(batch.candidates),
        "candidate_ids": [
            candidate.candidate_id for candidate in batch.candidates
        ],
        "expression_id": expression_id(strategy_expression(batch.family)),
        "expression_complexity": expression_complexity(
            strategy_expression(batch.family),
        ),
        "expression": strategy_expression_payload(
            strategy_expression(batch.family),
        ),
    } for batch in ordered]
    candidates = [{
        "candidate_id": candidate.candidate_id,
        "family": candidate.family,
        "mode": candidate.mode,
        "parameters": dict(candidate.parameters),
        "complexity": candidate.complexity,
        "expression_id": candidate.expression_id,
    } for batch in ordered for candidate in batch.candidates]
    if generator_method_version == LEGACY_GENERATOR_METHOD_VERSION:
        return {
            "schema_version": 1,
            "generator_method_version": LEGACY_GENERATOR_METHOD_VERSION,
            "strategy_method_version": STRATEGY_METHOD_VERSION,
            "expression_method_version": EXPRESSION_METHOD_VERSION,
            "config_hash": config_hash,
            "family_scope": [batch.family for batch in ordered],
            "candidate_count": sum(len(batch.candidates) for batch in ordered),
            "families": families,
            "candidates": candidates,
        }
    current_families = [
        {**family, "candidate_budget": batch.candidate_budget}
        for family, batch in zip(families, ordered, strict=True)
    ]
    return {
        "schema_version": 2,
        "generator_method_version": generator_method_version,
        "strategy_method_version": STRATEGY_METHOD_VERSION,
        "expression_method_version": EXPRESSION_METHOD_VERSION,
        "config_hash": config_hash,
        "family_scope": [batch.family for batch in ordered],
        "candidate_count": sum(len(batch.candidates) for batch in ordered),
        "search_plan": candidate_search_plan_payload(ordered),
        "families": current_families,
        "candidates": candidates,
    }
