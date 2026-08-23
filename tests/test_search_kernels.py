"""DAG 核与 search_plan.py 解释器的逐节点对照、三态真值表与分块不变性。"""
from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta

import pytest

from guvolu.search.kernels import (
    KernelSession,
    candidate_chunks,
    parse_family_plans,
)
from guvolu.search.tensorize import (
    BOOLEAN_FALSE,
    BOOLEAN_TRUE,
    BOOLEAN_UNKNOWN,
    tensorize_panel,
)
from guvolu.strategy.contracts import FeatureRow, ResearchBar
from guvolu.strategy.generation import SEARCH_PLAN_METHOD_VERSION
from guvolu.strategy.search_plan import (
    _node_value,
    evaluate_search_plan_candidate,
)
from searchfast_support import build_fixture, torch_devices

torch = pytest.importorskip("torch")
DEVICES = torch_devices()


def _reference_nodes(
    plan: dict[str, object],
    family_index: int,
    row_index: int,
    bar: ResearchBar,
    feature: FeatureRow,
    node_order: tuple[str, ...],
) -> dict[str, float | bool | None]:
    """以 CPU 解释器逐节点求值一个候选与一根柱。"""
    families = plan["families"]
    assert isinstance(families, list)
    family = families[family_index]
    names = family["parameter_names"]
    values = family["candidate_parameter_rows"][row_index]["values"]
    parameters = dict(zip(names, (float(value) for value in values), strict=True))
    computed: dict[str, float | bool | None] = {}
    nodes = plan["nodes"]
    assert isinstance(nodes, list)
    for node in nodes:
        if node["node_id"] not in node_order:
            continue
        arguments = [computed[item] for item in node["args"]]
        computed[node["node_id"]] = _node_value(
            node, arguments, parameters, bar, feature,
        )
    return computed


@pytest.mark.parametrize("device", DEVICES)
def test_kernels_match_cpu_interpreter_node_by_node(device: str) -> None:
    """随机合成面板与参数行上，每个节点三态或数值均与 CPU 解释器一致。"""
    fixture = build_fixture(bars=96, seed=23)
    plan = dict(fixture.plan)
    session = KernelSession(plan, fixture.panel, device)
    for family in session.families:
        count = len(family.parameter_rows)
        signals = session.evaluate_chunk(family, 0, count)
        computed = session.evaluate_nodes(family, signals.parameters)
        for row_index in range(count):
            for bar_index in range(fixture.panel.bar_count):
                reference = _reference_nodes(
                    plan,
                    family.index,
                    row_index,
                    fixture.rounded_bars[bar_index],
                    fixture.rounded_features[bar_index],
                    family.node_order,
                )
                for node_id in family.node_order:
                    value = computed[node_id]
                    expected = reference[node_id]
                    rows = value.values.shape[0]
                    row = row_index if rows > 1 else 0
                    column = bar_index if value.values.shape[1] > 1 else 0
                    if value.kind == "boolean":
                        got = int(value.values[row, column])
                        expected_code = (
                            BOOLEAN_UNKNOWN if expected is None
                            else BOOLEAN_TRUE if expected is True
                            else BOOLEAN_FALSE
                        )
                        assert got == expected_code, (family.family, node_id)
                        continue
                    valid_row = row if value.valid.shape[0] > 1 else 0
                    valid_column = column if value.valid.shape[1] > 1 else 0
                    valid = bool(value.valid[valid_row, valid_column])
                    if expected is None:
                        assert not valid, (family.family, node_id)
                        continue
                    assert valid, (family.family, node_id)
                    assert not isinstance(expected, bool)
                    got_value = float(value.values[row, column])
                    assert math.isclose(
                        got_value, float(expected), rel_tol=1e-5, abs_tol=1e-6,
                    ), (family.family, node_id, got_value, expected)


@pytest.mark.parametrize("device", DEVICES)
def test_root_signals_match_search_plan_candidate_evaluation(device: str) -> None:
    """根信号 required/entry/exit/target 与 evaluate_search_plan_candidate 一致。"""
    fixture = build_fixture(bars=64, seed=29)
    session = KernelSession(fixture.plan, fixture.panel, device)
    for family in session.families:
        count = len(family.parameter_rows)
        signals = session.evaluate_chunk(family, 0, count)
        for row_index, candidate_id in enumerate(family.candidate_ids):
            for bar_index in range(fixture.panel.bar_count):
                expected = evaluate_search_plan_candidate(
                    fixture.plan,
                    candidate_id,
                    fixture.rounded_bars[bar_index],
                    fixture.rounded_features[bar_index],
                )
                required = expected["required"]
                assert isinstance(required, tuple)
                assert bool(signals.required_valid[row_index, bar_index]) == all(
                    value is not None for value in required
                )
                for name, tensor in (("entry", signals.entry), ("exit", signals.exit)):
                    value = expected[name]
                    if tensor is None:
                        assert value is None
                        continue
                    code = int(tensor[row_index, bar_index])
                    assert code == (
                        BOOLEAN_UNKNOWN if value is None
                        else BOOLEAN_TRUE if value is True else BOOLEAN_FALSE
                    )
                if signals.target is not None:
                    target = expected["target"]
                    valid = bool(signals.target.valid[row_index, bar_index])
                    if target is None:
                        assert not valid
                    else:
                        assert valid
                        assert isinstance(target, float)
                        assert math.isclose(
                            float(signals.target.values[row_index, bar_index]),
                            target,
                            rel_tol=1e-5,
                            abs_tol=1e-6,
                        )


def _manual_plan() -> dict[str, object]:
    """手工构造覆盖 and、div_strict、missing_or_lt 的最小计划。"""
    nodes = [
        {"node_id": "n-flow", "op": "flow_imbalance", "args": []},
        {"node_id": "n-volume", "op": "volume_score", "args": []},
        {"node_id": "n-jump", "op": "jump_score", "args": []},
        {"node_id": "n-zero", "op": "constant", "args": [], "value": 0.0},
        {"node_id": "n-four", "op": "constant", "args": [], "value": 4.0},
        {"node_id": "n-flow-pos", "op": "gt", "args": ["n-flow", "n-zero"]},
        {"node_id": "n-volume-pos", "op": "gt", "args": ["n-volume", "n-zero"]},
        {"node_id": "n-and", "op": "and", "args": ["n-flow-pos", "n-volume-pos"]},
        {"node_id": "n-div", "op": "div_strict", "args": ["n-flow", "n-volume"]},
        {"node_id": "n-mol-const", "op": "missing_or_lt", "args": ["n-jump", "n-four"]},
        {"node_id": "n-mol-series", "op": "missing_or_lt", "args": ["n-jump", "n-volume"]},
    ]
    return {
        "search_plan_method_version": SEARCH_PLAN_METHOD_VERSION,
        "search_plan_id": "search-plan-manual",
        "nodes": nodes,
        "families": [{
            "family": "manual",
            "mode": "paper",
            "sizing": "expression_target",
            "parameter_names": ["lookback"],
            "candidate_parameter_rows": [{"candidate_id": "candidate-m", "values": [4]}],
            "roots": {
                "required": ["n-mol-series"],
                "entry": "n-and",
                "exit": "n-mol-const",
                "target": "n-div",
            },
        }],
    }


def _truth_table_panel():
    """构造三态真值表所需的九种组合面板。"""
    combos = [
        (1.0, 1.0), (1.0, -1.0), (1.0, None),
        (-1.0, 1.0), (-1.0, -1.0), (-1.0, None),
        (None, 1.0), (None, -1.0), (None, None),
    ]
    jumps = [3.0, 5.0, None, 3.0, 5.0, None, 3.0, 5.0, None]
    zero_volume = [False, False, False, False, False, False, True, False, False]
    start = datetime(2024, 1, 1, tzinfo=UTC)
    bars = []
    features = []
    for index, ((flow, volume), jump, zero) in enumerate(
        zip(combos, jumps, zero_volume, strict=True),
    ):
        open_time = start + timedelta(hours=index)
        bars.append(ResearchBar(
            open_time=open_time,
            decision_time=open_time + timedelta(hours=1),
            latest_available_time=open_time + timedelta(hours=1),
            open=100.0, high=101.0, low=99.0, close=100.0,
            base_volume=1.0, quote_volume=100.0, signed_base_volume=0.0,
            trade_count=1,
        ))
        features.append(FeatureRow(
            decision_time=open_time + timedelta(hours=1),
            as_of=open_time + timedelta(hours=1),
            return_one=None,
            trend_scores={4: 0.0},
            volatility={4: 0.01},
            price_scores={4: 0.0},
            prior_highs={4: 100.0},
            prior_lows={4: 100.0},
            flow_imbalance=flow,
            volume_score=0.0 if zero else volume,
            jump_score=jump,
            contiguous=True,
        ))
    return tuple(bars), tuple(features), combos, jumps, zero_volume


@pytest.mark.parametrize("device", DEVICES)
def test_three_state_truth_tables(device: str) -> None:
    """and、div_strict、missing_or_lt 的三态真值表逐格正确。"""
    bars, features, combos, jumps, zero_volume = _truth_table_panel()
    panel = tensorize_panel(bars, features, (4,))
    plan = _manual_plan()
    session = KernelSession(plan, panel, device)
    family = session.families[0]
    computed = session.evaluate_nodes(
        family, torch.tensor([[4.0]], dtype=torch.float32, device=device),
    )
    for index, ((flow, volume), jump, zero) in enumerate(
        zip(combos, jumps, zero_volume, strict=True),
    ):
        flow_state = None if flow is None else flow > 0
        volume_value = 0.0 if zero else volume
        volume_state = None if volume_value is None else volume_value > 0
        if flow_state is False or volume_state is False:
            expected_and = BOOLEAN_FALSE
        elif flow_state is True and volume_state is True:
            expected_and = BOOLEAN_TRUE
        else:
            expected_and = BOOLEAN_UNKNOWN
        assert int(computed["n-and"].values[0, index]) == expected_and
        division_valid = bool(computed["n-div"].valid[0, index])
        expected_valid = (
            flow is not None and volume_value is not None and volume_value != 0.0
        )
        assert division_valid == expected_valid
        if expected_valid:
            assert flow is not None and volume_value is not None
            assert math.isclose(
                float(computed["n-div"].values[0, index]), flow / volume_value,
            )
        expected_constant = (
            BOOLEAN_TRUE if jump is None
            else BOOLEAN_TRUE if jump < 4.0 else BOOLEAN_FALSE
        )
        assert int(computed["n-mol-const"].values[0, index]) == expected_constant
        if jump is None:
            expected_series = BOOLEAN_TRUE
        elif volume_value is None:
            expected_series = BOOLEAN_UNKNOWN
        else:
            expected_series = BOOLEAN_TRUE if jump < volume_value else BOOLEAN_FALSE
        assert int(computed["n-mol-series"].values[0, index]) == expected_series
    assert computed["n-and"].parameter_dependent is False


@pytest.mark.parametrize("device", DEVICES)
def test_unknown_lookback_window_is_invalid_not_filled(device: str) -> None:
    """候选回看窗不在面板中时节点无效，不得补值。"""
    fixture = build_fixture(bars=16, seed=3, lookbacks=(4, 8), families=("trend",))
    narrow_panel = tensorize_panel(fixture.bars, fixture.features, (4,))
    session = KernelSession(fixture.plan, narrow_panel, device)
    family = session.families[0]
    lookbacks = [row[family.parameter_names.index("lookback")] for row in family.parameter_rows]
    signals = session.evaluate_chunk(family, 0, len(family.parameter_rows))
    for row_index, lookback in enumerate(lookbacks):
        valid_any = bool(signals.required_valid[row_index].any())
        if int(lookback) == 8:
            assert not valid_any
        else:
            assert valid_any


@pytest.mark.parametrize("device", DEVICES)
def test_candidate_chunking_does_not_change_results(device: str) -> None:
    """不同分块大小下根信号逐格一致（TDR 分块不改变结果）。"""
    fixture = build_fixture(bars=48, seed=5, families=("flow_trend",))
    session = KernelSession(fixture.plan, fixture.panel, device)
    family = session.families[0]
    count = len(family.parameter_rows)
    assert count >= 4
    whole = session.evaluate_chunk(family, 0, count)
    for chunk in (1, 3):
        for start, stop in candidate_chunks(count, chunk):
            part = session.evaluate_chunk(family, start, stop)
            assert torch.equal(part.required_valid, whole.required_valid[start:stop])
            assert part.entry is not None and whole.entry is not None
            assert torch.equal(part.entry, whole.entry[start:stop])
            assert part.exit is not None and whole.exit is not None
            assert torch.equal(part.exit, whole.exit[start:stop])
    with pytest.raises(ValueError, match="分块大小越界"):
        candidate_chunks(count, 0)
    with pytest.raises(ValueError, match="分块大小越界"):
        candidate_chunks(count, 4096)


def test_parse_family_plans_reads_roots_and_reachable_nodes() -> None:
    """流派登记须含根、参数列与可达节点拓扑子序列。"""
    fixture = build_fixture(bars=8, seed=1, families=("grid_shadow", "trend"))
    families = parse_family_plans(fixture.plan)
    assert [item.family for item in families] == ["grid_shadow", "trend"]
    grid = families[0]
    assert grid.sizing == "expression_target"
    assert grid.target is not None and grid.entry is None
    assert grid.target in grid.node_order
    trend = families[1]
    assert trend.sizing == "volatility_target"
    assert trend.entry in trend.node_order and trend.exit in trend.node_order
    nodes = fixture.plan["nodes"]
    assert isinstance(nodes, list)
    assert len(trend.node_order) < len(nodes)
