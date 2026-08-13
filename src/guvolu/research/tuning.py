"""由流派监视证据生成受约束的下一代配置提案。"""
from __future__ import annotations

import json
from collections.abc import Mapping

from guvolu.research.provenance import canonical_json, sha256_text
from guvolu.strategy.generation import build_family_batches


def _object(value: object, name: str) -> Mapping[str, object]:
    """验证对象。"""
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} 必须为对象")
    return {str(key): item for key, item in value.items()}


def _number(value: object, name: str) -> float:
    """验证数值。"""
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValueError(f"{name} 必须为数值")
    return float(value)


def _singular(name: str) -> str:
    """把配置数组名映射为候选参数名。"""
    if name.endswith("ies"):
        return name[:-3] + "y"
    if name.endswith("s"):
        return name[:-1]
    return name


def _axis_map(strategy: Mapping[str, object]) -> Mapping[str, str]:
    """构造可进化的候选参数到配置数组映射。"""
    result: dict[str, str] = {}
    for key, value in strategy.items():
        if isinstance(value, list) and value and all(
            isinstance(item, (int, float)) and not isinstance(item, bool)
            for item in value
        ):
            result[_singular(key)] = key
    return result


def propose_family_evolution(
    config: Mapping[str, object],
    monitor: Mapping[str, object],
    parent_config_hash: str,
) -> tuple[Mapping[str, object], Mapping[str, object] | None]:
    """扩展一个预登记数值轴，不直接覆盖基准配置。"""
    family = str(monitor.get("family"))
    monitor_hash = sha256_text(canonical_json(monitor))
    source = _object(monitor.get("source"), "monitor.source")
    source_summary_hash = source.get("summary_sha256")
    source_ledger_hash = source.get("trial_ledger_sha256")
    if not isinstance(source_summary_hash, str) or len(source_summary_hash) != 64:
        raise ValueError("monitor 缺少合法 source summary hash")
    if not isinstance(source_ledger_hash, str) or len(source_ledger_hash) != 64:
        raise ValueError("monitor 缺少合法 source trial ledger hash")
    action = monitor.get("evolution_action")
    if action != "eligible_axis_refinement":
        return ({
            "schema_version": 1,
            "family": family,
            "status": "no_parameter_proposal",
            "reason": action,
            "parent_config_hash": parent_config_hash,
        }, None)
    strategies = _object(config.get("strategies"), "strategies")
    strategy = _object(strategies.get(family), f"strategies.{family}")
    axes = _axis_map(strategy)
    raw_directions = monitor.get("parameter_directions")
    if not isinstance(raw_directions, list):
        raise ValueError("monitor.parameter_directions 必须为数组")
    directions = [
        _object(item, "parameter_direction") for item in raw_directions
        if isinstance(item, Mapping)
        and str(item.get("parameter")) in axes
        and str(item.get("direction")) in {
            "explore_higher_after_preregistration",
            "explore_lower_after_preregistration",
        }
    ]
    if not directions:
        return ({
            "schema_version": 1,
            "family": family,
            "status": "no_parameter_proposal",
            "reason": "no_boundary_direction",
            "parent_config_hash": parent_config_hash,
        }, None)
    directions.sort(key=lambda item: (-abs(_number(
        item.get("association"), "association",
    )), str(item.get("parameter"))))
    chosen = directions[0]
    parameter = str(chosen["parameter"])
    config_key = axes[parameter]
    raw_values = strategy[config_key]
    if not isinstance(raw_values, list):
        raise ValueError("进化轴必须为数组")
    values = sorted(float(item) for item in raw_values)
    if len(values) < 2:
        raise ValueError("进化轴至少需要两个已登记值")
    direction = str(chosen["direction"])
    if direction == "explore_higher_after_preregistration":
        proposed_value = values[-1] + (values[-1] - values[-2])
    else:
        proposed_value = values[0] - (values[1] - values[0])
    evolution = _object(config.get("evolution"), "evolution")
    constraints = _object(evolution.get("constraints"), "evolution.constraints")
    family_constraints = _object(
        constraints.get(family), f"evolution.constraints.{family}",
    )
    axis_constraint = _object(
        family_constraints.get(parameter),
        f"evolution.constraints.{family}.{parameter}",
    )
    minimum = _number(axis_constraint.get("minimum"), "constraint.minimum")
    maximum = _number(axis_constraint.get("maximum"), "constraint.maximum")
    if proposed_value < minimum or proposed_value > maximum:
        return ({
            "schema_version": 1,
            "family": family,
            "status": "no_parameter_proposal",
            "reason": "configured_axis_boundary_reached",
            "parameter": parameter,
            "proposed_value": proposed_value,
            "parent_config_hash": parent_config_hash,
        }, None)
    proposed = json.loads(json.dumps(config))
    proposed_strategy = proposed["strategies"][family]
    original_items = strategy[config_key]
    integral = isinstance(original_items, list) and all(
        isinstance(item, int) and not isinstance(item, bool)
        for item in original_items
    )
    stored_value: int | float = int(round(proposed_value)) if integral else proposed_value
    proposed_strategy[config_key] = sorted(set([
        *proposed_strategy[config_key], stored_value,
    ]))
    if parameter == "lookback":
        proposed["features"]["lookbacks"] = sorted(set([
            *proposed["features"]["lookbacks"], stored_value,
        ]))
    proposed["evolution_parent"] = {
        "parent_config_hash": parent_config_hash,
        "family": family,
        "parameter": parameter,
        "direction": direction,
        "proposed_value": stored_value,
        "source_run_id": monitor.get("run_id"),
        "source_monitor_method_version": monitor.get("monitor_method_version"),
        "source_monitor_sha256": monitor_hash,
        "source_summary_sha256": source_summary_hash,
        "source_trial_ledger_sha256": source_ledger_hash,
        "holdout_consumed": False,
    }
    maximum_candidates = int(_number(
        evolution.get("maximum_candidates_per_family"),
        "maximum_candidates_per_family",
    ))
    candidate_count = len(build_family_batches(proposed, (family,))[0].candidates)
    if candidate_count > maximum_candidates:
        return ({
            "schema_version": 1,
            "family": family,
            "status": "no_parameter_proposal",
            "reason": "candidate_budget_exceeded",
            "candidate_count": candidate_count,
            "candidate_budget": maximum_candidates,
            "parent_config_hash": parent_config_hash,
        }, None)
    proposal = {
        "schema_version": 1,
        "family": family,
        "status": "proposed",
        "parameter": parameter,
        "direction": direction,
        "proposed_value": stored_value,
        "candidate_count": candidate_count,
        "candidate_budget": maximum_candidates,
        "parent_config_hash": parent_config_hash,
        "source_run_id": monitor.get("run_id"),
        "source_monitor_sha256": monitor_hash,
        "source_summary_sha256": source_summary_hash,
        "source_trial_ledger_sha256": source_ledger_hash,
        "holdout_consumed": False,
    }
    return proposal, proposed
