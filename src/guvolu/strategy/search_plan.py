"""类型化 SearchPlan 的独立 CPU 解释器。"""
from __future__ import annotations

import math
from collections.abc import Mapping

from guvolu.strategy.contracts import FeatureRow, ResearchBar
from guvolu.strategy.generation import SEARCH_PLAN_METHOD_VERSION


def _object(value: object, name: str) -> Mapping[str, object]:
    """验证 SearchPlan 对象。"""
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} 必须为对象")
    return {str(key): item for key, item in value.items()}


def _array(value: object, name: str) -> list[object]:
    """验证 SearchPlan 数组。"""
    if not isinstance(value, list):
        raise ValueError(f"{name} 必须为数组")
    return list(value)


def _text(value: object, name: str) -> str:
    """验证非空文本。"""
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} 必须为非空文本")
    return value


def _number(value: object, name: str) -> float:
    """验证有限数值。"""
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(float(value))
    ):
        raise ValueError(f"{name} 必须为有限数值")
    return float(value)


def _node_value(
    node: Mapping[str, object],
    arguments: list[float | bool | None],
    parameters: Mapping[str, float],
    bar: ResearchBar,
    feature: FeatureRow,
) -> float | bool | None:
    """按登记顺序执行一个已类型检查节点。"""
    op = _text(node.get("op"), "node.op")
    if op == "parameter":
        name = _text(node.get("value"), "parameter.value")
        if name not in parameters:
            raise ValueError(f"SearchPlan 候选缺少参数: {name}")
        return parameters[name]
    if op == "constant":
        return _number(node.get("value"), "constant.value")
    if op == "close":
        return bar.close
    if op in ("trend_score", "price_score", "prior_high"):
        if len(arguments) != 1 or isinstance(arguments[0], bool):
            return None
        raw_lookback = arguments[0]
        if raw_lookback is None:
            return None
        lookback = int(raw_lookback)
        source = {
            "trend_score": feature.trend_scores,
            "price_score": feature.price_scores,
            "prior_high": feature.prior_highs,
        }[op]
        return source.get(lookback)
    if op == "flow_imbalance":
        return feature.flow_imbalance
    if op == "volume_score":
        return feature.volume_score
    if op == "jump_score":
        return feature.jump_score
    if op == "missing_or_lt":
        if len(arguments) != 2:
            raise ValueError("SearchPlan missing_or_lt 参数数量错误")
        if arguments[0] is None:
            return True
        if arguments[1] is None:
            return None
        if isinstance(arguments[0], bool) or isinstance(arguments[1], bool):
            return None
        return float(arguments[0]) < float(arguments[1])
    if op == "and":
        if any(value is False for value in arguments):
            return False
        return True if all(value is True for value in arguments) else None
    if any(value is None or isinstance(value, bool) for value in arguments):
        return None
    numeric = [float(value) for value in arguments if value is not None]
    if op == "neg":
        return -numeric[0]
    if op == "abs":
        return abs(numeric[0])
    if op == "mul":
        return numeric[0] * numeric[1]
    if op == "div_strict":
        return None if numeric[1] == 0.0 else numeric[0] / numeric[1]
    if op == "min":
        return min(numeric)
    if op == "max":
        return max(numeric)
    if op == "gt":
        return numeric[0] > numeric[1]
    if op == "ge":
        return numeric[0] >= numeric[1]
    if op == "lt":
        return numeric[0] < numeric[1]
    if op == "le":
        return numeric[0] <= numeric[1]
    raise ValueError(f"SearchPlan 操作不受支持: {op}")


def _reachable_nodes(
    plan: Mapping[str, object],
    roots: Mapping[str, object],
) -> set[str]:
    """返回所选流派根节点可达的节点集合。"""
    arguments_by_id: dict[str, list[str]] = {}
    for raw_node in _array(plan.get("nodes"), "nodes"):
        node = _object(raw_node, "node")
        arguments_by_id[_text(node.get("node_id"), "node.node_id")] = [
            _text(value, "node.arg")
            for value in _array(node.get("args"), "node.args")
        ]
    stack = [
        _text(value, "roots.required")
        for value in _array(roots.get("required"), "roots.required")
    ]
    for name in ("entry", "exit", "target"):
        value = roots.get(name)
        if value is not None:
            stack.append(_text(value, f"roots.{name}"))
    reachable: set[str] = set()
    while stack:
        node_id = stack.pop()
        if node_id in reachable:
            continue
        reachable.add(node_id)
        if node_id not in arguments_by_id:
            raise ValueError(f"SearchPlan 根节点未登记: {node_id}")
        stack.extend(arguments_by_id[node_id])
    return reachable


def evaluate_search_plan_candidate(
    plan: Mapping[str, object],
    candidate_id: str,
    bar: ResearchBar,
    feature: FeatureRow,
) -> Mapping[str, object]:
    """执行一个已登记候选的 required/entry/exit/target 根节点。"""
    if plan.get("search_plan_method_version") != SEARCH_PLAN_METHOD_VERSION:
        raise ValueError("SearchPlan 方法版本不受支持")
    selected_family: Mapping[str, object] | None = None
    selected_row: Mapping[str, object] | None = None
    for raw_family in _array(plan.get("families"), "families"):
        family = _object(raw_family, "family")
        for raw_row in _array(
            family.get("candidate_parameter_rows"),
            "candidate_parameter_rows",
        ):
            row = _object(raw_row, "candidate_parameter_row")
            if row.get("candidate_id") != candidate_id:
                continue
            if selected_row is not None:
                raise ValueError("SearchPlan candidate_id 不唯一")
            selected_family = family
            selected_row = row
    if selected_family is None or selected_row is None:
        raise ValueError("SearchPlan 不包含候选")
    parameter_names = [
        _text(value, "parameter_name")
        for value in _array(
            selected_family.get("parameter_names"), "parameter_names",
        )
    ]
    parameter_values = [
        _number(value, "parameter_value")
        for value in _array(selected_row.get("values"), "parameter_values")
    ]
    if len(parameter_names) != len(parameter_values):
        raise ValueError("SearchPlan 参数列与数值数量不一致")
    parameters = dict(zip(parameter_names, parameter_values, strict=True))
    roots = _object(selected_family.get("roots"), "roots")
    reachable = _reachable_nodes(plan, roots)
    computed: dict[str, float | bool | None] = {}
    seen: set[str] = set()
    for raw_node in _array(plan.get("nodes"), "nodes"):
        node = _object(raw_node, "node")
        node_id = _text(node.get("node_id"), "node.node_id")
        if node_id in seen:
            raise ValueError("SearchPlan node_id 重复")
        seen.add(node_id)
        if node_id not in reachable:
            continue
        argument_ids = [
            _text(value, "node.arg")
            for value in _array(node.get("args"), "node.args")
        ]
        try:
            arguments = [computed[value] for value in argument_ids]
        except KeyError as error:
            raise ValueError("SearchPlan 不是子节点优先顺序") from error
        computed[node_id] = _node_value(
            node, arguments, parameters, bar, feature,
        )

    def root_value(name: str) -> float | bool | None:
        value = roots.get(name)
        if value is None:
            return None
        return computed[_text(value, f"roots.{name}")]

    required = tuple(
        computed[_text(value, "roots.required")]
        for value in _array(roots.get("required"), "roots.required")
    )
    return {
        "family": _text(selected_family.get("family"), "family"),
        "candidate_id": candidate_id,
        "parameters": parameters,
        "required": required,
        "entry": root_value("entry"),
        "exit": root_value("exit"),
        "target": root_value("target"),
    }
