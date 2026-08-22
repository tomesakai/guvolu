"""有界强类型结构变异与交叉候选。"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace

from guvolu.strategy.expression import (
    ExpressionNode,
    StrategyExpression,
    expression_id,
    infer_expression_type,
    strategy_expression_payload,
    validate_strategy_expression,
)

STRUCTURAL_SEARCH_METHOD_VERSION = "bounded-typed-structure-search-v1"
MUTATION_OPERATORS = (
    "comparison_strictness",
    "drop_conjunct",
    "numeric_bound_swap",
)


@dataclass(frozen=True)
class StructuralChallenger:
    """尚未注册到生产公式目录的结构 challenger。"""

    family: str
    parent_expression_id: str
    expression_id: str
    operator: str
    source_path: str
    expression: StrategyExpression
    donor_family: str | None = None
    donor_expression_id: str | None = None
    donor_path: str | None = None


def _walk(node: ExpressionNode, path: str) -> tuple[tuple[str, ExpressionNode], ...]:
    """以前序稳定路径枚举一棵表达式树。"""
    rows = [(path, node)]
    for index, child in enumerate(node.args):
        rows.extend(_walk(child, f"{path}.{index}"))
    return tuple(rows)


def _roots(
    template: StrategyExpression,
) -> tuple[tuple[str, ExpressionNode], ...]:
    """枚举可改变的信号根；required 事实声明不参与结构搜索。"""
    return tuple(
        (name, node)
        for name, node in (
            ("entry", template.entry),
            ("exit", template.exit),
            ("target", template.target),
        )
        if node is not None
    )


def _replace_node(
    node: ExpressionNode,
    indices: tuple[int, ...],
    replacement: ExpressionNode,
) -> ExpressionNode:
    """按稳定索引路径替换一个不可变节点。"""
    if not indices:
        return replacement
    index = indices[0]
    if index < 0 or index >= len(node.args):
        raise ValueError("结构搜索节点路径越界")
    children = list(node.args)
    children[index] = _replace_node(children[index], indices[1:], replacement)
    return replace(node, args=tuple(children))


def _replace_root_path(
    template: StrategyExpression,
    path: str,
    replacement: ExpressionNode,
) -> StrategyExpression:
    """替换 entry/exit/target 下的一个节点。"""
    parts = path.split(".")
    root_name = parts[0]
    if root_name not in {"entry", "exit", "target"}:
        raise ValueError("结构搜索根路径不受支持")
    root = getattr(template, root_name)
    if not isinstance(root, ExpressionNode):
        raise ValueError("结构搜索根不存在")
    indices = tuple(int(value) for value in parts[1:])
    changed = _replace_node(root, indices, replacement)
    if root_name == "entry":
        return replace(template, entry=changed)
    if root_name == "exit":
        return replace(template, exit=changed)
    return replace(template, target=changed)


def _challenger(
    parent: StrategyExpression,
    expression: StrategyExpression,
    operator: str,
    source_path: str,
    donor: StrategyExpression | None = None,
    donor_path: str | None = None,
) -> StructuralChallenger | None:
    """验证、去除同义结构并生成挑战者身份。"""
    validate_strategy_expression(expression)
    parent_id = expression_id(parent)
    challenger_id = expression_id(expression)
    if challenger_id == parent_id:
        return None
    return StructuralChallenger(
        family=parent.family,
        parent_expression_id=parent_id,
        expression_id=challenger_id,
        operator=operator,
        source_path=source_path,
        expression=expression,
        donor_family=None if donor is None else donor.family,
        donor_expression_id=None if donor is None else expression_id(donor),
        donor_path=donor_path,
    )


def bounded_typed_mutations(
    template: StrategyExpression,
    operators: Sequence[str] = MUTATION_OPERATORS,
    limit: int = 4,
) -> tuple[StructuralChallenger, ...]:
    """枚举单点、同类型、确定性的结构变异并按预算截断。"""
    if limit < 0:
        raise ValueError("结构变异 limit 不得为负")
    requested = tuple(sorted(set(operators)))
    unknown = sorted(set(requested) - set(MUTATION_OPERATORS))
    if unknown:
        raise ValueError("未知结构变异算子: " + ",".join(unknown))
    candidates: list[StructuralChallenger] = []
    for root_name, root in _roots(template):
        for path, node in _walk(root, root_name):
            replacements: list[tuple[str, str, ExpressionNode]] = []
            if "comparison_strictness" in requested:
                comparison = {"ge": "gt", "gt": "ge", "le": "lt", "lt": "le"}
                if node.op in comparison:
                    replacements.append((
                        "comparison_strictness",
                        path,
                        replace(node, op=comparison[node.op]),
                    ))
            if "numeric_bound_swap" in requested and node.op in {"min", "max"}:
                replacements.append((
                    "numeric_bound_swap",
                    path,
                    replace(node, op="max" if node.op == "min" else "min"),
                ))
            if "drop_conjunct" in requested and node.op == "and" and len(node.args) > 2:
                for index in range(len(node.args)):
                    replacements.append((
                        "drop_conjunct",
                        f"{path}.drop.{index}",
                        replace(node, args=node.args[:index] + node.args[index + 1:]),
                    ))
            for operator, source_path, replacement in replacements:
                replace_path = source_path.split(".drop.", 1)[0]
                candidate = _challenger(
                    template,
                    _replace_root_path(template, replace_path, replacement),
                    operator,
                    source_path,
                )
                if candidate is not None:
                    candidates.append(candidate)
    unique = {candidate.expression_id: candidate for candidate in candidates}
    ordered = sorted(
        unique.values(),
        key=lambda item: (item.operator, item.source_path, item.expression_id),
    )
    return tuple(ordered[:limit])


def _parameter_names(node: ExpressionNode) -> set[str]:
    """收集子树依赖的参数名。"""
    names = {str(node.value)} if node.op == "parameter" else set()
    for child in node.args:
        names.update(_parameter_names(child))
    return names


def bounded_typed_crossovers(
    recipient: StrategyExpression,
    donor: StrategyExpression,
    limit: int = 4,
) -> tuple[StructuralChallenger, ...]:
    """以同输出类型且参数可解析的 donor 子树替换 recipient 单个子树。"""
    if limit < 0:
        raise ValueError("结构交叉 limit 不得为负")
    if donor.family == recipient.family and expression_id(donor) == expression_id(
        recipient,
    ):
        return ()
    recipient_nodes = tuple(
        (path, node)
        for root_name, root in _roots(recipient)
        for path, node in _walk(root, root_name)
        if node.args
    )
    donor_nodes = tuple(
        (path, node)
        for root_name, root in _roots(donor)
        for path, node in _walk(root, root_name)
        if node.args
        and _parameter_names(node).issubset(recipient.parameter_types)
    )
    candidates: list[StructuralChallenger] = []
    for recipient_path, recipient_node in recipient_nodes:
        recipient_type = infer_expression_type(
            recipient_node, recipient.parameter_types, recipient_path,
        )
        for donor_path, donor_node in donor_nodes:
            try:
                donor_type = infer_expression_type(
                    donor_node, recipient.parameter_types, donor_path,
                )
            except ValueError:
                continue
            if donor_type != recipient_type:
                continue
            candidate = _challenger(
                recipient,
                _replace_root_path(recipient, recipient_path, donor_node),
                "typed_subtree_crossover",
                recipient_path,
                donor,
                donor_path,
            )
            if candidate is not None:
                candidates.append(candidate)
    unique = {candidate.expression_id: candidate for candidate in candidates}
    ordered = sorted(
        unique.values(),
        key=lambda item: (
            item.source_path,
            item.donor_path or "",
            item.expression_id,
        ),
    )
    return tuple(ordered[:limit])


def structural_challenger_registry_payload(
    family: str,
    config_hash: str,
    source_candidate_ids: Sequence[str],
    source_search_plan_id: str,
    generator_method_version: str,
    candidate_budget: int,
    challengers: Sequence[StructuralChallenger],
) -> Mapping[str, object]:
    """生成明确停在注册前边界的内容寻址 challenger 注册表。"""
    candidate_ids = tuple(source_candidate_ids)
    if (
        not candidate_ids
        or len(set(candidate_ids)) != len(candidate_ids)
        or any(not isinstance(item, str) or not item for item in candidate_ids)
        or candidate_budget <= 0
    ):
        raise ValueError("来源候选身份与预算无效")
    if not source_search_plan_id.startswith("search-plan-"):
        raise ValueError("来源 SearchPlan 身份无效")
    if not generator_method_version:
        raise ValueError("来源生成器方法版本无效")
    ordered = tuple(sorted(
        challengers,
        key=lambda item: (item.operator, item.source_path, item.expression_id),
    ))
    if any(item.family != family for item in ordered):
        raise ValueError("结构 challenger 不能跨 recipient 流派")
    if len({item.expression_id for item in ordered}) != len(ordered):
        raise ValueError("结构 challenger 表达式身份重复")
    projected = len(candidate_ids) * (1 + len(ordered))
    if projected > candidate_budget:
        raise ValueError("结构 challenger 投影候选数超过预算")
    return {
        "schema_version": 1,
        "structural_search_method_version": STRUCTURAL_SEARCH_METHOD_VERSION,
        "status": "unregistered_structural_challengers",
        "family": family,
        "config_hash": config_hash,
        "source": {
            "generator_method_version": generator_method_version,
            "search_plan_id": source_search_plan_id,
            "candidate_ids": list(candidate_ids),
        },
        "base_candidate_count": len(candidate_ids),
        "structural_challenger_count": len(ordered),
        "projected_candidate_count": projected,
        "candidate_budget": candidate_budget,
        "activation_contract": (
            "consume through a verified family monitor proposal, register the "
            "canonical expression in source, create a clean commit, then rerun "
            "full ValidationExact; this artifact cannot be promoted"
        ),
        "holdout_consumed": False,
        "challengers": [{
            "family": item.family,
            "parent_expression_id": item.parent_expression_id,
            "expression_id": item.expression_id,
            "operator": item.operator,
            "source_path": item.source_path,
            "donor_family": item.donor_family,
            "donor_expression_id": item.donor_expression_id,
            "donor_path": item.donor_path,
            "expression": strategy_expression_payload(item.expression),
        } for item in ordered],
    }
