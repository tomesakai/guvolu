"""按 SearchPlan DAG 拓扑序求值的 Torch 核（CPU/CUDA 同一实现）。

节点张量为候选分块乘柱数；参数无关节点在会话内共享缓存。
三态逻辑以 int8 的 1、0、-1 表示真、假、未知，逐条与
`evaluate_expression` 等价。
"""
from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import ModuleType

from guvolu.search.tensorize import (
    BOOLEAN_FALSE,
    BOOLEAN_TRUE,
    BOOLEAN_UNKNOWN,
    PanelTensor,
    window_column,
)
from guvolu.search.torch_runtime import Tensor, torch_module
from guvolu.strategy.generation import SEARCH_PLAN_METHOD_VERSION

WINDOW_OPS = ("trend_score", "price_score", "prior_high")
SCALAR_FIELD_OPS = ("flow_imbalance", "volume_score", "jump_score")
DEFAULT_CANDIDATE_CHUNK = 1024
MINIMUM_CANDIDATE_CHUNK = 1
MAXIMUM_CANDIDATE_CHUNK = 1024


@dataclass(frozen=True)
class NodeValue:
    """一个 DAG 节点的张量值。"""

    kind: str
    values: Tensor
    valid: Tensor
    parameter_dependent: bool


@dataclass(frozen=True)
class FamilyPlan:
    """一个流派在 SearchPlan 中的登记。"""

    index: int
    family: str
    mode: str
    sizing: str
    parameter_names: tuple[str, ...]
    candidate_ids: tuple[str, ...]
    parameter_rows: tuple[tuple[float, ...], ...]
    required: tuple[str, ...]
    entry: str | None
    exit: str | None
    target: str | None
    node_order: tuple[str, ...]


@dataclass(frozen=True)
class ChunkSignals:
    """一个候选分块的根信号。"""

    family: FamilyPlan
    start: int
    candidate_count: int
    parameters: Tensor
    required_valid: Tensor
    entry: Tensor | None
    exit: Tensor | None
    target: NodeValue | None


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


def parse_plan_nodes(
    plan: Mapping[str, object],
) -> tuple[dict[str, Mapping[str, object]], tuple[str, ...]]:
    """读取节点登记与求值顺序。"""
    if plan.get("search_plan_method_version") != SEARCH_PLAN_METHOD_VERSION:
        raise ValueError("SearchPlan 方法版本不受支持")
    nodes: dict[str, Mapping[str, object]] = {}
    order: list[str] = []
    for raw in _array(plan.get("nodes"), "nodes"):
        node = _object(raw, "node")
        node_id = _text(node.get("node_id"), "node.node_id")
        if node_id in nodes:
            raise ValueError("SearchPlan node_id 重复")
        for argument in _array(node.get("args"), "node.args"):
            if _text(argument, "node.arg") not in nodes:
                raise ValueError("SearchPlan 不是子节点优先顺序")
        nodes[node_id] = node
        order.append(node_id)
    return nodes, tuple(order)


def _reachable(
    nodes: Mapping[str, Mapping[str, object]],
    order: Sequence[str],
    roots: Sequence[str],
) -> tuple[str, ...]:
    """返回根集合可达节点的拓扑子序列。"""
    needed: set[str] = set()
    stack = list(roots)
    while stack:
        node_id = stack.pop()
        if node_id in needed:
            continue
        needed.add(node_id)
        stack.extend(
            _text(item, "node.arg")
            for item in _array(nodes[node_id].get("args"), "node.args")
        )
    return tuple(node_id for node_id in order if node_id in needed)


def parse_family_plans(plan: Mapping[str, object]) -> tuple[FamilyPlan, ...]:
    """读取每个流派的根、参数列与候选参数行。"""
    nodes, order = parse_plan_nodes(plan)
    families: list[FamilyPlan] = []
    for index, raw_family in enumerate(_array(plan.get("families"), "families")):
        family = _object(raw_family, "family")
        roots = _object(family.get("roots"), "roots")
        required = tuple(
            _text(item, "roots.required")
            for item in _array(roots.get("required"), "roots.required")
        )
        optional: dict[str, str | None] = {}
        for name in ("entry", "exit", "target"):
            value = roots.get(name)
            optional[name] = None if value is None else _text(value, f"roots.{name}")
        parameter_names = tuple(
            _text(item, "parameter_name")
            for item in _array(family.get("parameter_names"), "parameter_names")
        )
        candidate_ids: list[str] = []
        rows: list[tuple[float, ...]] = []
        for raw_row in _array(
            family.get("candidate_parameter_rows"), "candidate_parameter_rows",
        ):
            row = _object(raw_row, "candidate_parameter_row")
            values = tuple(
                _number(item, "parameter_value")
                for item in _array(row.get("values"), "parameter_values")
            )
            if len(values) != len(parameter_names):
                raise ValueError("SearchPlan 参数列与数值数量不一致")
            candidate_ids.append(_text(row.get("candidate_id"), "candidate_id"))
            rows.append(values)
        root_ids = list(required) + [
            value for value in optional.values() if value is not None
        ]
        families.append(FamilyPlan(
            index=index,
            family=_text(family.get("family"), "family"),
            mode=_text(family.get("mode"), "mode"),
            sizing=_text(family.get("sizing"), "sizing"),
            parameter_names=parameter_names,
            candidate_ids=tuple(candidate_ids),
            parameter_rows=tuple(rows),
            required=required,
            entry=optional["entry"],
            exit=optional["exit"],
            target=optional["target"],
            node_order=_reachable(nodes, order, root_ids),
        ))
    return tuple(families)


class DevicePanel:
    """驻留设备的面板列与回看窗堆叠。"""

    def __init__(self, torch: ModuleType, panel: PanelTensor, device: str) -> None:
        self.torch = torch
        self.device = device
        self.bar_count = panel.bar_count
        self.lookbacks = tuple(panel.lookbacks)
        self._columns: dict[str, tuple[Tensor, Tensor]] = {}
        for name in panel.columns:
            values, masks = panel.column(name)
            value_tensor = torch.tensor(
                values.tolist(), dtype=torch.float32, device=device,
            ).reshape(1, -1)
            valid_tensor = torch.tensor(
                [bool(item) for item in masks], dtype=torch.bool, device=device,
            ).reshape(1, -1)
            self._columns[name] = (value_tensor, valid_tensor)
        self._stacks: dict[str, tuple[Tensor, Tensor]] = {}

    def column(self, name: str) -> tuple[Tensor, Tensor]:
        """返回 [1×B] 的数值与有效性。"""
        try:
            return self._columns[name]
        except KeyError as error:
            raise KeyError(f"面板缺少列: {name}") from error

    def window_stack(self, field: str) -> tuple[Tensor, Tensor]:
        """返回 [K+1×B] 的回看窗堆叠，末行为缺失哨兵。"""
        cached = self._stacks.get(field)
        if cached is not None:
            return cached
        torch = self.torch
        values = [
            self.column(window_column(field, lookback))[0]
            for lookback in self.lookbacks
        ]
        valids = [
            self.column(window_column(field, lookback))[1]
            for lookback in self.lookbacks
        ]
        sentinel_values = torch.full(
            (1, self.bar_count), math.nan, dtype=torch.float32, device=self.device,
        )
        sentinel_valid = torch.zeros(
            (1, self.bar_count), dtype=torch.bool, device=self.device,
        )
        stacked = (
            torch.cat(values + [sentinel_values], dim=0),
            torch.cat(valids + [sentinel_valid], dim=0),
        )
        self._stacks[field] = stacked
        return stacked

    def lookback_index(self, lookback_values: Tensor) -> Tensor:
        """把回看窗数值映射为堆叠行号，未知窗映射为哨兵行。"""
        torch = self.torch
        integer = lookback_values.to(torch.int64)
        index = torch.full_like(integer, len(self.lookbacks))
        for position, lookback in enumerate(self.lookbacks):
            index = torch.where(integer == lookback, position, index)
        return index


class KernelSession:
    """一个搜索束上的 DAG 求值会话，缓存参数无关节点。"""

    def __init__(
        self,
        plan: Mapping[str, object],
        panel: PanelTensor,
        device: str,
    ) -> None:
        self.torch = torch_module()
        self.device = device
        self.nodes, self.order = parse_plan_nodes(plan)
        self.families = parse_family_plans(plan)
        self.panel = DevicePanel(self.torch, panel, device)
        self._shared: dict[str, NodeValue] = {}

    def family(self, name: str) -> FamilyPlan:
        """按流派名取登记。"""
        for family in self.families:
            if family.family == name:
                return family
        raise ValueError(f"SearchPlan 不包含流派: {name}")

    def evaluate_chunk(
        self,
        family: FamilyPlan,
        start: int,
        stop: int,
    ) -> ChunkSignals:
        """求值一个候选分块的根信号。"""
        if start < 0 or stop > len(family.parameter_rows) or start >= stop:
            raise ValueError("候选分块范围非法")
        torch = self.torch
        parameters = torch.tensor(
            [list(row) for row in family.parameter_rows[start:stop]],
            dtype=torch.float32,
            device=self.device,
        )
        computed = self.evaluate_nodes(family, parameters)
        required_valid = torch.ones(
            (stop - start, self.panel.bar_count),
            dtype=torch.bool,
            device=self.device,
        )
        for node_id in family.required:
            required_valid = required_valid & computed[node_id].valid
        return ChunkSignals(
            family=family,
            start=start,
            candidate_count=stop - start,
            parameters=parameters,
            required_valid=required_valid,
            entry=None if family.entry is None else computed[family.entry].values,
            exit=None if family.exit is None else computed[family.exit].values,
            target=None if family.target is None else computed[family.target],
        )

    def evaluate_nodes(
        self,
        family: FamilyPlan,
        parameters: Tensor,
    ) -> dict[str, NodeValue]:
        """按拓扑序求值流派可达节点，返回全部节点值。"""
        computed: dict[str, NodeValue] = {}
        for node_id in family.node_order:
            shared = self._shared.get(node_id)
            if shared is not None:
                computed[node_id] = shared
                continue
            node = self.nodes[node_id]
            arguments = [
                computed[_text(item, "node.arg")]
                for item in _array(node.get("args"), "node.args")
            ]
            value = self._evaluate_node(node, arguments, family, parameters)
            computed[node_id] = value
            if not value.parameter_dependent:
                self._shared[node_id] = value
        return computed

    def _true(self) -> Tensor:
        """返回可广播的全有效标记。"""
        return self.torch.ones((1, 1), dtype=self.torch.bool, device=self.device)

    def _evaluate_node(
        self,
        node: Mapping[str, object],
        arguments: Sequence[NodeValue],
        family: FamilyPlan,
        parameters: Tensor,
    ) -> NodeValue:
        """求值单个节点，语义与 CPU 解释器逐条对应。"""
        torch = self.torch
        op = _text(node.get("op"), "node.op")
        if op == "parameter":
            name = _text(node.get("value"), "parameter.value")
            if name not in family.parameter_names:
                raise ValueError(f"SearchPlan 候选缺少参数: {name}")
            column = family.parameter_names.index(name)
            values = parameters[:, column:column + 1]
            return NodeValue("numeric", values, self._true(), True)
        if op == "constant":
            value = _number(node.get("value"), "constant.value")
            tensor = torch.tensor(
                [[value]], dtype=torch.float32, device=self.device,
            )
            return NodeValue("numeric", tensor, self._true(), False)
        if op == "close":
            values, valid = self.panel.column("close")
            return NodeValue("numeric", values, valid, False)
        if op in WINDOW_OPS:
            if len(arguments) != 1 or arguments[0].kind != "numeric":
                raise ValueError(f"SearchPlan {op} 回看窗参数非法")
            lookback = arguments[0]
            stack_values, stack_valid = self.panel.window_stack(op)
            index = self.panel.lookback_index(lookback.values).reshape(-1)
            values = stack_values[index]
            valid = stack_valid[index] & lookback.valid
            return NodeValue("numeric", values, valid, lookback.parameter_dependent)
        if op in SCALAR_FIELD_OPS:
            values, valid = self.panel.column(op)
            return NodeValue("numeric", values, valid, False)
        dependent = any(item.parameter_dependent for item in arguments)
        if op == "missing_or_lt":
            left, right = _numeric_pair(arguments, op)
            unknown = torch.full_like(
                left.values < right.values, BOOLEAN_UNKNOWN, dtype=torch.int8,
            )
            comparison = (left.values < right.values).to(torch.int8)
            values = torch.where(
                ~left.valid,
                torch.tensor(BOOLEAN_TRUE, dtype=torch.int8, device=self.device),
                torch.where(~right.valid, unknown, comparison),
            )
            return NodeValue("boolean", values, self._true(), dependent)
        if op == "and":
            if len(arguments) < 2 or any(
                item.kind != "boolean" for item in arguments
            ):
                raise ValueError("SearchPlan and 参数非法")
            any_false = arguments[0].values == BOOLEAN_FALSE
            all_true = arguments[0].values == BOOLEAN_TRUE
            for item in arguments[1:]:
                any_false = any_false | (item.values == BOOLEAN_FALSE)
                all_true = all_true & (item.values == BOOLEAN_TRUE)
            values = torch.where(
                any_false,
                torch.tensor(BOOLEAN_FALSE, dtype=torch.int8, device=self.device),
                torch.where(
                    all_true,
                    torch.tensor(BOOLEAN_TRUE, dtype=torch.int8, device=self.device),
                    torch.tensor(
                        BOOLEAN_UNKNOWN, dtype=torch.int8, device=self.device,
                    ),
                ),
            )
            return NodeValue("boolean", values, self._true(), dependent)
        if op in ("neg", "abs"):
            if len(arguments) != 1 or arguments[0].kind != "numeric":
                raise ValueError(f"SearchPlan {op} 参数非法")
            source = arguments[0]
            values = -source.values if op == "neg" else torch.abs(source.values)
            return NodeValue("numeric", values, source.valid, dependent)
        if op in ("mul", "div_strict", "min", "max"):
            left, right = _numeric_pair(arguments, op)
            valid = left.valid & right.valid
            if op == "mul":
                values = left.values * right.values
            elif op == "div_strict":
                valid = valid & (right.values != 0.0)
                values = left.values / right.values
            elif op == "min":
                values = torch.minimum(left.values, right.values)
            else:
                values = torch.maximum(left.values, right.values)
            return NodeValue("numeric", values, valid, dependent)
        if op in ("gt", "ge", "lt", "le"):
            left, right = _numeric_pair(arguments, op)
            if op == "gt":
                comparison = left.values > right.values
            elif op == "ge":
                comparison = left.values >= right.values
            elif op == "lt":
                comparison = left.values < right.values
            else:
                comparison = left.values <= right.values
            values = torch.where(
                left.valid & right.valid,
                comparison.to(torch.int8),
                torch.tensor(BOOLEAN_UNKNOWN, dtype=torch.int8, device=self.device),
            )
            return NodeValue("boolean", values, self._true(), dependent)
        raise ValueError(f"SearchPlan 操作不受支持: {op}")


def _numeric_pair(
    arguments: Sequence[NodeValue],
    op: str,
) -> tuple[NodeValue, NodeValue]:
    """验证二元数值参数。"""
    if len(arguments) != 2 or any(item.kind != "numeric" for item in arguments):
        raise ValueError(f"SearchPlan {op} 参数非法")
    return arguments[0], arguments[1]


def candidate_chunks(
    count: int,
    chunk: int = DEFAULT_CANDIDATE_CHUNK,
) -> tuple[tuple[int, int], ...]:
    """按分块大小切分候选区间，保证单核短时执行。"""
    if chunk < MINIMUM_CANDIDATE_CHUNK or chunk > MAXIMUM_CANDIDATE_CHUNK:
        raise ValueError("候选分块大小越界")
    return tuple(
        (start, min(start + chunk, count))
        for start in range(0, count, chunk)
    )
