"""由流派监视证据生成受约束的下一代配置提案。"""
from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path

from guvolu.research.config_lineage import load_verified_config_lineage
from guvolu.research.evolution import monitor_family_run
from guvolu.research.provenance import canonical_json, sha256_file
from guvolu.research.verification import verify_research_run
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


def _text(value: object, name: str) -> str:
    """验证非空文本。"""
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} 必须为非空文本")
    return value


def _source_path(root: Path, value: object, name: str) -> Path:
    """解析并限制监视来源路径位于项目目录。"""
    path = (root / _text(value, name)).resolve()
    try:
        path.relative_to(root)
    except ValueError as error:
        raise ValueError(f"{name} 越出项目目录") from error
    return path


def _load_monitor_artifact(
    repository_root: Path,
    monitor_path: Path,
) -> tuple[Mapping[str, object], str, str]:
    """读取并绑定一个内容寻址的监视器文件。"""
    root = repository_root.resolve()
    path = monitor_path.resolve()
    try:
        relative = path.relative_to(root).as_posix()
    except ValueError as error:
        raise ValueError("monitor 制品必须位于项目目录内") from error
    monitor_hash = sha256_file(path)
    expected_name = f"family-monitor-sha256-{monitor_hash}.json"
    if path.name != expected_name:
        raise ValueError("monitor 文件名与实际制品散列不一致")
    monitor = _object(
        json.loads(path.read_text(encoding="utf-8")), "monitor",
    )
    return monitor, relative, monitor_hash


def _load_config_artifact(
    repository_root: Path,
    config_path: Path,
) -> tuple[Mapping[str, object], str, str, int, str]:
    """从项目内文件读取父配置并计算其唯一散列。"""
    root = repository_root.resolve()
    path = config_path.resolve()
    try:
        path.relative_to(root)
    except ValueError as error:
        raise ValueError("父配置必须位于项目目录内") from error
    config, config_hash, lineage_root_hash, lineage_depth = (
        load_verified_config_lineage(root, path)
    )
    return (
        config,
        config_hash,
        lineage_root_hash,
        lineage_depth,
        path.relative_to(root).as_posix(),
    )


def verify_evolution_config(
    repository_root: Path,
    config_path: Path,
    config: Mapping[str, object],
) -> None:
    """从父配置和监视制品重建并精确核对派生配置。"""
    raw_parent = config.get("evolution_parent")
    if raw_parent is None:
        return
    root = repository_root.resolve()
    parent = _object(raw_parent, "evolution_parent")
    parent_path = _source_path(
        root,
        parent.get("parent_config_path"),
        "evolution_parent.parent_config_path",
    )
    monitor_path = _source_path(
        root,
        parent.get("source_monitor_path"),
        "evolution_parent.source_monitor_path",
    )
    if sha256_file(monitor_path) != parent.get("source_monitor_sha256"):
        raise ValueError("派生配置 source monitor 散列不匹配")
    _proposal, rebuilt = propose_family_evolution(
        root,
        parent_path,
        monitor_path,
    )
    if rebuilt is None or canonical_json(rebuilt) != canonical_json(config):
        raise ValueError("派生配置不是父配置与监视证据允许的单轴变换")


def verify_monitor_sources(
    repository_root: Path,
    config: Mapping[str, object],
    monitor: Mapping[str, object],
    parent_config_hash: str,
) -> None:
    """验证监视来源并重算提案实际消费的单 vintage 证据。"""
    root = repository_root.resolve()
    source = _object(monitor.get("source"), "monitor.source")
    if source.get("config_hash") != parent_config_hash:
        raise ValueError("monitor 来源配置与父配置散列不一致")
    summary_path = _source_path(
        root, source.get("summary_path"), "monitor.source.summary_path",
    )
    ledger_path = _source_path(
        root, source.get("trial_ledger_path"),
        "monitor.source.trial_ledger_path",
    )
    if sha256_file(summary_path) != source.get("summary_sha256"):
        raise ValueError("monitor 来源 summary 散列不匹配")
    if sha256_file(ledger_path) != source.get("trial_ledger_sha256"):
        raise ValueError("monitor 来源 trial ledger 散列不匹配")
    verify_research_run(root, summary_path.parent / "manifest.json")
    summary = _object(
        json.loads(summary_path.read_text(encoding="utf-8")), "source summary",
    )
    if summary.get("config_hash") != parent_config_hash:
        raise ValueError("monitor 来源 summary 未绑定父配置")
    if summary.get("run_id") != monitor.get("run_id"):
        raise ValueError("monitor 与来源 summary 的 run_id 不一致")
    if summary.get("research_identity") != monitor.get("research_identity"):
        raise ValueError("monitor 与来源 summary 的研究身份不一致")
    artifacts = _object(summary.get("artifacts"), "summary.artifacts")
    ledger = _object(artifacts.get("trial_ledger"), "summary.trial_ledger")
    if ledger.get("path") != ledger_path.relative_to(root).as_posix():
        raise ValueError("monitor trial ledger 路径与 summary 不一致")
    if ledger.get("sha256") != source.get("trial_ledger_sha256"):
        raise ValueError("monitor trial ledger 身份与 summary 不一致")
    family = _text(monitor.get("family"), "monitor.family")
    recomputed = monitor_family_run(
        root,
        summary_path,
        family,
        config,
        parent_config_hash,
    )
    consumed_fields = (
        "monitor_method_version",
        "run_id",
        "research_identity",
        "data_vintage_id",
        "decision_time",
        "family",
        "eligible",
        "rejection_reasons",
        "adjusted_sharpe",
        "fdr_q",
        "latest_unallocated_target",
        "candidate_count",
        "best_fixed_candidate_sharpe",
        "evolution_action",
        "parameter_directions",
        "source",
    )
    mismatches = [
        field for field in consumed_fields
        if monitor.get(field) != recomputed.get(field)
    ]
    if mismatches:
        raise ValueError(
            "monitor 与来源事实重算不一致: " + ", ".join(mismatches)
        )


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
    repository_root: Path,
    config_path: Path,
    monitor_path: Path,
) -> tuple[Mapping[str, object], Mapping[str, object] | None]:
    """扩展一个预登记数值轴，不直接覆盖基准配置。"""
    (
        config,
        parent_config_hash,
        lineage_root_config_hash,
        lineage_depth,
        parent_config_path,
    ) = _load_config_artifact(
        repository_root, config_path,
    )
    verify_evolution_config(repository_root, config_path, config)
    monitor, monitor_relative, monitor_hash = _load_monitor_artifact(
        repository_root, monitor_path,
    )
    verify_monitor_sources(repository_root, config, monitor, parent_config_hash)
    family = str(monitor.get("family"))
    source = _object(monitor.get("source"), "monitor.source")
    source_summary_hash = source.get("summary_sha256")
    source_ledger_hash = source.get("trial_ledger_sha256")
    if not isinstance(parent_config_hash, str) or len(parent_config_hash) != 64:
        raise ValueError("父配置散列必须是 SHA-256")
    if source.get("config_hash") != parent_config_hash:
        raise ValueError("monitor 来源配置与父配置散列不一致")
    if not isinstance(source_summary_hash, str) or len(source_summary_hash) != 64:
        raise ValueError("monitor 缺少合法 source summary hash")
    if not isinstance(source_ledger_hash, str) or len(source_ledger_hash) != 64:
        raise ValueError("monitor 缺少合法 source trial ledger hash")
    evidence = {
        "proposal_method_version": "family-evolution-proposal-v2",
        "parent_config_hash": parent_config_hash,
        "source_run_id": monitor.get("run_id"),
        "source_monitor_method_version": monitor.get("monitor_method_version"),
        "source_monitor_path": monitor_relative,
        "source_monitor_sha256": monitor_hash,
        "source_summary_sha256": source_summary_hash,
        "source_trial_ledger_sha256": source_ledger_hash,
        "source_manifest_sha256": source.get("manifest_sha256"),
        "source_panel_sha256": source.get("panel_sha256"),
        "source_code_identity": source.get("code_identity"),
        "evidence_scope": "single_vintage_candidate_axis",
        "holdout_consumed": False,
    }

    def no_proposal(
        reason: object,
        **details: object,
    ) -> tuple[Mapping[str, object], None]:
        """发布同样受来源约束的拒绝结论。"""
        return ({
            "schema_version": 1,
            "family": family,
            "status": "no_parameter_proposal",
            "reason": reason,
            **details,
            **evidence,
        }, None)

    action = monitor.get("evolution_action")
    if action != "eligible_axis_refinement":
        return no_proposal(action)
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
        return no_proposal("no_boundary_direction")
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
        return no_proposal(
            "configured_axis_boundary_reached",
            parameter=parameter,
            proposed_value=proposed_value,
        )
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
        "parent_config_path": parent_config_path,
        "lineage_root_config_hash": lineage_root_config_hash,
        "lineage_depth": lineage_depth + 1,
        "family": family,
        "parameter": parameter,
        "direction": direction,
        "proposed_value": stored_value,
        "source_run_id": monitor.get("run_id"),
        "source_monitor_method_version": monitor.get("monitor_method_version"),
        "source_monitor_path": monitor_relative,
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
        return no_proposal(
            "candidate_budget_exceeded",
            candidate_count=candidate_count,
            candidate_budget=maximum_candidates,
        )
    proposal = {
        "schema_version": 1,
        "family": family,
        "status": "proposed",
        "parameter": parameter,
        "direction": direction,
        "proposed_value": stored_value,
        "candidate_count": candidate_count,
        "candidate_budget": maximum_candidates,
        **evidence,
    }
    return proposal, proposed
