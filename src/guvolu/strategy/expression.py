"""策略信号的强类型表达式、规范身份与 CPU 参考求值。"""
from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from enum import StrEnum
from typing import Mapping

from guvolu.strategy.contracts import FeatureRow, ResearchBar

EXPRESSION_SCHEMA_VERSION = 1
EXPRESSION_METHOD_VERSION = "typed-signal-expression-v1"


class ValueShape(StrEnum):
    """表达式值的形状。"""

    SCALAR = "scalar"
    SERIES = "time_series"
    BOOLEAN_SERIES = "boolean_time_series"


class Unit(StrEnum):
    """首版时序信号使用的单位。"""

    DIMENSIONLESS = "dimensionless"
    PRICE = "price"
    WINDOW = "window"


@dataclass(frozen=True)
class ExpressionType:
    """节点的强类型合同。"""

    shape: ValueShape
    unit: Unit
    frequency: str = "configured_bar"
    availability: str = "decision_time"
    missing_policy: str = "strict_invalid"
    numeric_domain: str = "finite_f64"


@dataclass(frozen=True)
class ExpressionNode:
    """一个不可变表达式节点。"""

    op: str
    args: tuple[ExpressionNode, ...] = ()
    value: str | int | float | None = None
    unit: Unit | None = None


@dataclass(frozen=True)
class StrategyExpression:
    """一个流派的参数化信号模板。"""

    family: str
    mode: str
    parameter_types: Mapping[str, ExpressionType]
    required: tuple[ExpressionNode, ...]
    entry: ExpressionNode | None
    exit: ExpressionNode | None
    target: ExpressionNode | None
    sizing: str


_SCALAR_DIMENSIONLESS = ExpressionType(ValueShape.SCALAR, Unit.DIMENSIONLESS)
_SCALAR_WINDOW = ExpressionType(ValueShape.SCALAR, Unit.WINDOW)
_SERIES_DIMENSIONLESS = ExpressionType(ValueShape.SERIES, Unit.DIMENSIONLESS)
_SERIES_PRICE = ExpressionType(ValueShape.SERIES, Unit.PRICE)
_BOOLEAN_SERIES = ExpressionType(ValueShape.BOOLEAN_SERIES, Unit.DIMENSIONLESS)


def _parameter(name: str) -> ExpressionNode:
    return ExpressionNode("parameter", value=name)


def _constant(value: float, unit: Unit = Unit.DIMENSIONLESS) -> ExpressionNode:
    return ExpressionNode("constant", value=value, unit=unit)


def _field(name: str, lookback: ExpressionNode | None = None) -> ExpressionNode:
    return ExpressionNode(name, () if lookback is None else (lookback,))


def _unary(op: str, value: ExpressionNode) -> ExpressionNode:
    return ExpressionNode(op, (value,))


def _binary(op: str, left: ExpressionNode, right: ExpressionNode) -> ExpressionNode:
    return ExpressionNode(op, (left, right))


def _and(*values: ExpressionNode) -> ExpressionNode:
    return ExpressionNode("and", tuple(values))


def _parameter_schema(names: Mapping[str, Unit]) -> Mapping[str, ExpressionType]:
    """构造模板参数类型。"""
    return {
        name: (_SCALAR_WINDOW if unit is Unit.WINDOW else ExpressionType(
            ValueShape.SCALAR,
            unit,
        ))
        for name, unit in names.items()
    }


_LOOKBACK = _parameter("lookback")
_TREND = _field("trend_score", _LOOKBACK)
_PRICE_SCORE = _field("price_score", _LOOKBACK)
_PRIOR_HIGH = _field("prior_high", _LOOKBACK)
_FLOW = _field("flow_imbalance")
_VOLUME = _field("volume_score")
_JUMP = _field("jump_score")
_CLOSE = _field("close")


_TEMPLATES: Mapping[str, StrategyExpression] = {
    "trend": StrategyExpression(
        family="trend",
        mode="paper",
        parameter_types=_parameter_schema({
            "lookback": Unit.WINDOW,
            "entry_score": Unit.DIMENSIONLESS,
            "exit_score": Unit.DIMENSIONLESS,
            "annual_volatility_target": Unit.DIMENSIONLESS,
            "maximum_target": Unit.DIMENSIONLESS,
        }),
        required=(_TREND,),
        entry=_binary("ge", _TREND, _parameter("entry_score")),
        exit=_binary("le", _TREND, _parameter("exit_score")),
        target=None,
        sizing="volatility_target",
    ),
    "flow_trend": StrategyExpression(
        family="flow_trend",
        mode="paper",
        parameter_types=_parameter_schema({
            "lookback": Unit.WINDOW,
            "entry_score": Unit.DIMENSIONLESS,
            "flow_confirmation": Unit.DIMENSIONLESS,
            "minimum_volume_score": Unit.DIMENSIONLESS,
            "exit_score": Unit.DIMENSIONLESS,
            "annual_volatility_target": Unit.DIMENSIONLESS,
            "maximum_target": Unit.DIMENSIONLESS,
        }),
        required=(_TREND,),
        entry=_and(
            _binary("ge", _TREND, _parameter("entry_score")),
            _binary("ge", _FLOW, _parameter("flow_confirmation")),
            _binary("ge", _VOLUME, _parameter("minimum_volume_score")),
        ),
        exit=_binary("le", _TREND, _parameter("exit_score")),
        target=None,
        sizing="volatility_target",
    ),
    "breakout": StrategyExpression(
        family="breakout",
        mode="paper",
        parameter_types=_parameter_schema({
            "lookback": Unit.WINDOW,
            "flow_confirmation": Unit.DIMENSIONLESS,
            "annual_volatility_target": Unit.DIMENSIONLESS,
            "maximum_target": Unit.DIMENSIONLESS,
        }),
        required=(_PRIOR_HIGH, _PRICE_SCORE),
        entry=_and(
            _binary("gt", _CLOSE, _PRIOR_HIGH),
            _binary("ge", _FLOW, _parameter("flow_confirmation")),
        ),
        exit=_binary("le", _PRICE_SCORE, _constant(0.0)),
        target=None,
        sizing="volatility_target",
    ),
    "mean_reversion": StrategyExpression(
        family="mean_reversion",
        mode="paper",
        parameter_types=_parameter_schema({
            "lookback": Unit.WINDOW,
            "entry_score": Unit.DIMENSIONLESS,
            "exit_score": Unit.DIMENSIONLESS,
            "trend_limit": Unit.DIMENSIONLESS,
            "annual_volatility_target": Unit.DIMENSIONLESS,
            "maximum_target": Unit.DIMENSIONLESS,
        }),
        required=(_PRICE_SCORE, _TREND),
        entry=_and(
            _binary("le", _PRICE_SCORE, _unary("neg", _parameter("entry_score"))),
            _binary("le", _unary("abs", _TREND), _parameter("trend_limit")),
            _binary("missing_or_lt", _JUMP, _constant(4.0)),
        ),
        exit=_binary("ge", _PRICE_SCORE, _parameter("exit_score")),
        target=None,
        sizing="volatility_target",
    ),
    "grid_shadow": StrategyExpression(
        family="grid_shadow",
        mode="shadow",
        parameter_types=_parameter_schema({
            "lookback": Unit.WINDOW,
            "entry_score": Unit.DIMENSIONLESS,
            "maximum_target": Unit.DIMENSIONLESS,
        }),
        required=(_PRICE_SCORE,),
        entry=None,
        exit=None,
        target=_binary(
            "min",
            _binary(
                "mul",
                _binary(
                    "max",
                    _binary(
                        "div_strict",
                        _unary("neg", _PRICE_SCORE),
                        _parameter("entry_score"),
                    ),
                    _constant(0.0),
                ),
                _parameter("maximum_target"),
            ),
            _parameter("maximum_target"),
        ),
        sizing="expression_target",
    ),
}


def strategy_expression(family: str) -> StrategyExpression:
    """返回已登记流派的表达式模板。"""
    try:
        template = _TEMPLATES[family]
    except KeyError as error:
        raise ValueError(f"未知策略表达式流派: {family}") from error
    validate_strategy_expression(template)
    return template


def _same_numeric(left: ExpressionType, right: ExpressionType) -> bool:
    """判断数值节点是否可按单位广播。"""
    return (
        left.unit is right.unit
        and left.shape in (ValueShape.SCALAR, ValueShape.SERIES)
        and right.shape in (ValueShape.SCALAR, ValueShape.SERIES)
        and ValueShape.SERIES in (left.shape, right.shape)
    )


def infer_expression_type(
    node: ExpressionNode,
    parameters: Mapping[str, ExpressionType],
    path: str = "root",
) -> ExpressionType:
    """递归推断类型，错误包含稳定 AST path。"""
    if node.op == "parameter":
        name = str(node.value)
        if name not in parameters:
            raise ValueError(f"E_PARAM_UNKNOWN:{path}:{name}")
        return parameters[name]
    if node.op == "constant":
        if not isinstance(node.value, (int, float)) or isinstance(node.value, bool):
            raise ValueError(f"E_CONSTANT_TYPE:{path}")
        if not math.isfinite(float(node.value)):
            raise ValueError(f"E_CONSTANT_FINITE:{path}")
        return ExpressionType(ValueShape.SCALAR, node.unit or Unit.DIMENSIONLESS)
    if node.op == "close":
        return _SERIES_PRICE
    if node.op == "prior_high":
        _require_window(node, parameters, path)
        return _SERIES_PRICE
    if node.op in ("trend_score", "price_score"):
        _require_window(node, parameters, path)
        return _SERIES_DIMENSIONLESS
    if node.op in ("flow_imbalance", "volume_score", "jump_score"):
        if node.args:
            raise ValueError(f"E_ARITY:{path}:{node.op}")
        return _SERIES_DIMENSIONLESS
    if node.op in ("neg", "abs"):
        _require_arity(node, 1, path)
        value_type = infer_expression_type(node.args[0], parameters, path + ".0")
        if value_type.shape is ValueShape.BOOLEAN_SERIES:
            raise ValueError(f"E_NUMERIC_EXPECTED:{path}")
        return value_type
    if node.op in ("mul", "div_strict", "min", "max"):
        _require_arity(node, 2, path)
        left = infer_expression_type(node.args[0], parameters, path + ".0")
        right = infer_expression_type(node.args[1], parameters, path + ".1")
        if not _same_numeric(left, right):
            raise ValueError(f"E_TYPE_MISMATCH:{path}:{left}:{right}")
        return ExpressionType(ValueShape.SERIES, left.unit)
    if node.op in ("gt", "ge", "lt", "le", "missing_or_lt"):
        _require_arity(node, 2, path)
        left = infer_expression_type(node.args[0], parameters, path + ".0")
        right = infer_expression_type(node.args[1], parameters, path + ".1")
        if not _same_numeric(left, right):
            raise ValueError(f"E_COMPARE_TYPE:{path}:{left}:{right}")
        return _BOOLEAN_SERIES
    if node.op == "and":
        if len(node.args) < 2:
            raise ValueError(f"E_ARITY:{path}:and")
        for index, child in enumerate(node.args):
            child_type = infer_expression_type(
                child,
                parameters,
                f"{path}.{index}",
            )
            if child_type is not _BOOLEAN_SERIES and child_type != _BOOLEAN_SERIES:
                raise ValueError(f"E_BOOLEAN_EXPECTED:{path}.{index}")
        return _BOOLEAN_SERIES
    raise ValueError(f"E_OPCODE_UNKNOWN:{path}:{node.op}")


def _require_arity(node: ExpressionNode, count: int, path: str) -> None:
    if len(node.args) != count:
        raise ValueError(f"E_ARITY:{path}:{node.op}:{count}")


def _require_window(
    node: ExpressionNode,
    parameters: Mapping[str, ExpressionType],
    path: str,
) -> None:
    _require_arity(node, 1, path)
    window_type = infer_expression_type(node.args[0], parameters, path + ".0")
    if window_type != _SCALAR_WINDOW:
        raise ValueError(f"E_WINDOW_EXPECTED:{path}")


def validate_strategy_expression(template: StrategyExpression) -> None:
    """验证模板的参数、必要字段和输出合同。"""
    for index, required in enumerate(template.required):
        value_type = infer_expression_type(
            required,
            template.parameter_types,
            f"required.{index}",
        )
        if value_type.shape is not ValueShape.SERIES:
            raise ValueError(f"E_REQUIRED_SERIES:required.{index}")
    for name, expression in (("entry", template.entry), ("exit", template.exit)):
        if expression is None:
            continue
        if infer_expression_type(expression, template.parameter_types, name) != _BOOLEAN_SERIES:
            raise ValueError(f"E_SIGNAL_BOOLEAN:{name}")
    if template.target is not None:
        target_type = infer_expression_type(
            template.target,
            template.parameter_types,
            "target",
        )
        if target_type != _SERIES_DIMENSIONLESS:
            raise ValueError("E_TARGET_DIMENSIONLESS")
    if template.sizing == "volatility_target":
        required_parameters = {
            "lookback",
            "annual_volatility_target",
            "maximum_target",
        }
        if not required_parameters.issubset(template.parameter_types):
            raise ValueError("E_SIZING_PARAMETERS")
    elif template.sizing != "expression_target":
        raise ValueError(f"E_SIZING_UNKNOWN:{template.sizing}")


def _node_payload(node: ExpressionNode) -> Mapping[str, object]:
    """生成规范 AST 节点。"""
    children = [_node_payload(child) for child in node.args]
    if node.op == "and":
        children.sort(key=lambda item: json.dumps(
            item,
            sort_keys=True,
            separators=(",", ":"),
        ))
    payload: dict[str, object] = {"op": node.op}
    if children:
        payload["args"] = children
    if node.value is not None:
        value = node.value
        if isinstance(value, float) and value == 0.0:
            value = 0.0
        payload["value"] = value
    if node.unit is not None:
        payload["unit"] = node.unit.value
    return payload


def strategy_expression_payload(template: StrategyExpression) -> Mapping[str, object]:
    """生成用于身份和注册表的规范模板。"""
    validate_strategy_expression(template)
    required = [_node_payload(node) for node in template.required]
    required.sort(key=lambda item: json.dumps(
        item,
        sort_keys=True,
        separators=(",", ":"),
    ))
    return {
        "schema_version": EXPRESSION_SCHEMA_VERSION,
        "expression_method_version": EXPRESSION_METHOD_VERSION,
        "family": template.family,
        "mode": template.mode,
        "parameter_types": {
            name: {
                "shape": value.shape.value,
                "unit": value.unit.value,
                "frequency": value.frequency,
                "availability": value.availability,
                "missing_policy": value.missing_policy,
                "numeric_domain": value.numeric_domain,
            }
            for name, value in sorted(template.parameter_types.items())
        },
        "required": required,
        "entry": None if template.entry is None else _node_payload(template.entry),
        "exit": None if template.exit is None else _node_payload(template.exit),
        "target": None if template.target is None else _node_payload(template.target),
        "sizing": template.sizing,
    }


def expression_id(template: StrategyExpression) -> str:
    """由规范 AST 字节生成表达式身份。"""
    body = json.dumps(
        strategy_expression_payload(template),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return "expression-" + hashlib.sha256(body).hexdigest()


def validate_parameters(
    template: StrategyExpression,
    parameters: Mapping[str, int | float],
) -> None:
    """验证解析后的候选参数恰好匹配模板。"""
    if set(parameters) != set(template.parameter_types):
        missing = sorted(set(template.parameter_types) - set(parameters))
        extra = sorted(set(parameters) - set(template.parameter_types))
        raise ValueError(f"E_PARAMETER_SET:missing={missing}:extra={extra}")
    for name, value in parameters.items():
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"E_PARAMETER_NUMERIC:{name}")
        if not math.isfinite(float(value)):
            raise ValueError(f"E_PARAMETER_FINITE:{name}")
        expected = template.parameter_types[name]
        if expected.unit is Unit.WINDOW and (
            not isinstance(value, int) or value <= 0
        ):
            raise ValueError(f"E_PARAMETER_WINDOW:{name}")


def candidate_identity(
    template: StrategyExpression,
    parameters: Mapping[str, int | float],
) -> str:
    """绑定表达式身份与完整解析参数。"""
    validate_parameters(template, parameters)
    normalized_parameters: dict[str, int | float] = {}
    for name, expected in sorted(template.parameter_types.items()):
        value = parameters[name]
        if expected.unit is Unit.WINDOW:
            normalized_parameters[name] = int(value)
        else:
            numeric = float(value)
            normalized_parameters[name] = 0.0 if numeric == 0.0 else numeric
    body = json.dumps(
        {
            "expression_id": expression_id(template),
            "parameters": normalized_parameters,
        },
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return "candidate-" + hashlib.sha256(body).hexdigest()


def evaluate_expression(
    node: ExpressionNode,
    parameters: Mapping[str, int | float],
    bar: ResearchBar,
    feature: FeatureRow,
) -> float | bool | None:
    """以固定 f64 顺序执行单行 CPU reference。"""
    if node.op == "parameter":
        return float(parameters[str(node.value)])
    if node.op == "constant":
        return float(node.value)  # type: ignore[arg-type]
    if node.op == "close":
        return bar.close
    if node.op in ("trend_score", "price_score", "prior_high"):
        lookback_value = evaluate_expression(node.args[0], parameters, bar, feature)
        if not isinstance(lookback_value, float):
            return None
        lookback = int(lookback_value)
        source = {
            "trend_score": feature.trend_scores,
            "price_score": feature.price_scores,
            "prior_high": feature.prior_highs,
        }[node.op]
        return source.get(lookback)
    if node.op == "flow_imbalance":
        return feature.flow_imbalance
    if node.op == "volume_score":
        return feature.volume_score
    if node.op == "jump_score":
        return feature.jump_score
    values = [evaluate_expression(child, parameters, bar, feature) for child in node.args]
    if node.op == "missing_or_lt":
        if values[0] is None:
            return True
        if values[1] is None:
            return None
        left, right = values
        if (
            left is None
            or right is None
            or isinstance(left, bool)
            or isinstance(right, bool)
        ):
            return None
        return float(left) < float(right)
    if node.op == "and":
        if any(value is False for value in values):
            return False
        return True if all(value is True for value in values) else None
    if any(value is None or isinstance(value, bool) for value in values):
        return None
    numeric: list[float] = []
    for value in values:
        if value is None or isinstance(value, bool):
            return None
        numeric.append(float(value))
    if node.op == "neg":
        return -numeric[0]
    if node.op == "abs":
        return abs(numeric[0])
    if node.op == "mul":
        return numeric[0] * numeric[1]
    if node.op == "div_strict":
        return None if numeric[1] == 0.0 else numeric[0] / numeric[1]
    if node.op == "min":
        return min(numeric)
    if node.op == "max":
        return max(numeric)
    if node.op == "gt":
        return numeric[0] > numeric[1]
    if node.op == "ge":
        return numeric[0] >= numeric[1]
    if node.op == "lt":
        return numeric[0] < numeric[1]
    if node.op == "le":
        return numeric[0] <= numeric[1]
    raise ValueError(f"E_OPCODE_UNKNOWN:runtime:{node.op}")


def expression_complexity(template: StrategyExpression) -> int:
    """返回首版静态 AST 节点数。"""
    seen: set[ExpressionNode] = set()

    def visit(node: ExpressionNode) -> None:
        if node in seen:
            return
        seen.add(node)
        for child in node.args:
            visit(child)

    for required_node in template.required:
        visit(required_node)
    for root_node in (template.entry, template.exit, template.target):
        if root_node is not None:
            visit(root_node)
    return len(seen)
