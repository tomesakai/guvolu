"""策略家族候选方向与跨运行衰减监视。"""
from __future__ import annotations

import json
import statistics
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path

from guvolu.research.provenance import sha256_file, stable_identifier
from guvolu.research.verification import verify_research_run

_INTERVAL_SECONDS = {
    "5min": 300.0,
    "15min": 900.0,
    "1hour": 3600.0,
    "4hour": 14_400.0,
}


def _object(value: object, name: str) -> Mapping[str, object]:
    """验证 JSON 对象。"""
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} 必须为对象")
    return {str(key): item for key, item in value.items()}


def _number(value: object, name: str) -> float:
    """验证数值。"""
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValueError(f"{name} 必须为数值")
    return float(value)


def _integer(value: object, name: str) -> int:
    """验证正整数。"""
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{name} 必须为正整数")
    return value


def _text(value: object, name: str) -> str:
    """验证非空文本。"""
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} 必须为非空文本")
    return value


def _decision_time(summary: Mapping[str, object]) -> datetime:
    """读取带时区的研究决策时点。"""
    value = datetime.fromisoformat(_text(summary.get("decision_time"), "decision_time"))
    if value.tzinfo is None:
        raise ValueError("decision_time 必须带时区")
    return value


def _data_vintage(summary: Mapping[str, object]) -> tuple[str, str]:
    """以冻结面板内容定义一次数据 vintage，而非以运行或代码身份代替。"""
    panel = _object(summary.get("panel"), "summary.panel")
    panel_sha256 = _text(panel.get("sha256"), "summary.panel.sha256")
    market_id = _text(summary.get("market_id"), "summary.market_id")
    return stable_identifier("data-vintage", {
        "market_id": market_id,
        "panel_sha256": panel_sha256,
    }), panel_sha256


def _verified_summary_source(
    root: Path,
    summary_path: Path,
) -> tuple[Mapping[str, object], str]:
    """验证 summary 是完整 research manifest 保护的制品。"""
    path = summary_path.resolve()
    try:
        path.relative_to(root)
    except ValueError as error:
        raise ValueError("监视来源 summary 必须位于项目目录内") from error
    manifest_path = path.parent / "manifest.json"
    verified = verify_research_run(root, manifest_path)
    manifest = _object(
        json.loads(manifest_path.read_text(encoding="utf-8")), "manifest",
    )
    artifacts = _object(manifest.get("artifacts"), "manifest.artifacts")
    summary_record = _object(artifacts.get("summary_json"), "summary_json")
    protected = (root / _text(summary_record.get("path"), "summary_json.path")).resolve()
    if protected != path:
        raise ValueError("监视来源 summary 不是 manifest 保护的制品")
    summary = _object(json.loads(path.read_text(encoding="utf-8")), "summary")
    if summary.get("run_id") != verified.run_id:
        raise ValueError("监视来源 summary 与 manifest 运行身份不一致")
    return summary, verified.manifest_sha256


def _history_entry(
    summary: Mapping[str, object],
    evaluation: Mapping[str, object],
    identity: str,
) -> Mapping[str, object]:
    """构造可审计的跨运行历史条目。"""
    vintage_id, panel_sha256 = _data_vintage(summary)
    code_identity = _object(summary.get("code_identity"), "summary.code_identity")
    return {
        "research_identity": identity,
        "run_id": summary.get("run_id"),
        "decision_time": summary.get("decision_time"),
        "data_vintage_id": vintage_id,
        "panel_sha256": panel_sha256,
        "config_hash": summary.get("config_hash"),
        "code_tree_digest": code_identity.get("tree_digest"),
        "adjusted_sharpe": evaluation.get("adjusted_sharpe"),
        "fdr_q": evaluation.get("fdr_q"),
        "eligible": evaluation.get("eligible"),
    }


def _correlation(left: Sequence[float], right: Sequence[float]) -> float:
    """计算候选参数与指标的线性关联，仅作为搜索方向证据。"""
    if len(left) != len(right) or len(left) < 2:
        return 0.0
    left_mean = statistics.fmean(left)
    right_mean = statistics.fmean(right)
    covariance = statistics.fmean(
        (x - left_mean) * (y - right_mean)
        for x, y in zip(left, right, strict=True)
    )
    left_std = statistics.pstdev(left)
    right_std = statistics.pstdev(right)
    if left_std <= 0 or right_std <= 0:
        return 0.0
    return covariance / (left_std * right_std)


def _aggregate_trials(path: Path, family: str) -> list[Mapping[str, object]]:
    """读取一个流派的固定候选聚合样本外事实。"""
    rows: list[Mapping[str, object]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            item = _object(json.loads(line), "trial")
            if (
                item.get("record_type") == "trial"
                and item.get("family") == family
                and item.get("fold_id") == "walk-forward"
                and item.get("segment") == "testing_aggregate"
            ):
                rows.append(item)
    if not rows:
        raise ValueError(f"试验台账没有流派聚合记录: {family}")
    return rows


def _parameter_directions(
    rows: Sequence[Mapping[str, object]],
    threshold: float,
) -> list[Mapping[str, object]]:
    """从完整候选网格估计每个数值轴的搜索方向。"""
    values: dict[str, list[tuple[float, float]]] = defaultdict(list)
    for row in rows:
        parameters = _object(row.get("parameters"), "trial.parameters")
        metrics = _object(row.get("metrics"), "trial.metrics")
        sharpe = _number(metrics.get("sharpe"), "trial.metrics.sharpe")
        for name, raw_value in parameters.items():
            if isinstance(raw_value, (int, float)) and not isinstance(raw_value, bool):
                values[name].append((float(raw_value), sharpe))
    result: list[Mapping[str, object]] = []
    for name, observations in sorted(values.items()):
        distinct = sorted({value for value, _score in observations})
        if len(distinct) < 2:
            result.append({
                "parameter": name,
                "direction": "fixed",
                "association": 0.0,
                "best_value": distinct[0],
                "observed_values": distinct,
            })
            continue
        grouped: dict[float, list[float]] = defaultdict(list)
        for value, score in observations:
            grouped[value].append(score)
        medians = {
            value: statistics.median(scores) for value, scores in grouped.items()
        }
        best_value = max(sorted(medians), key=lambda value: (medians[value], -value))
        association = _correlation(
            [value for value, _score in observations],
            [score for _value, score in observations],
        )
        if abs(association) < threshold:
            direction = "hold_or_interaction"
        elif best_value == distinct[-1] and association > 0:
            direction = "explore_higher_after_preregistration"
        elif best_value == distinct[0] and association < 0:
            direction = "explore_lower_after_preregistration"
        else:
            direction = "refine_near_best_after_preregistration"
        result.append({
            "parameter": name,
            "direction": direction,
            "association": association,
            "best_value": best_value,
            "best_median_sharpe": medians[best_value],
            "observed_values": distinct,
        })
    return result


def _family_evaluation(
    summary: Mapping[str, object],
    family: str,
) -> Mapping[str, object]:
    """读取一个流派的家族级验证结果。"""
    raw = summary.get("family_evaluations")
    if not isinstance(raw, list):
        raise ValueError("summary.family_evaluations 必须为数组")
    matches = [
        _object(item, "family_evaluation") for item in raw
        if isinstance(item, Mapping) and item.get("family") == family
    ]
    if len(matches) != 1:
        raise ValueError(f"流派验证结果不唯一: {family}")
    return matches[0]


def monitor_family_run(
    repository_root: Path,
    summary_path: Path,
    family: str,
    config: Mapping[str, object],
    config_hash: str,
    prior_summary_paths: Sequence[Path] = (),
) -> Mapping[str, object]:
    """生成单流派参数方向与跨运行健康监视制品。"""
    root = repository_root.resolve()
    summary_path = summary_path.resolve()
    try:
        summary_relative = summary_path.relative_to(root).as_posix()
    except ValueError as error:
        raise ValueError("监视来源 summary 必须位于项目目录内") from error
    summary, manifest_sha256 = _verified_summary_source(root, summary_path)
    evaluation = _family_evaluation(summary, family)
    artifacts = _object(summary.get("artifacts"), "summary.artifacts")
    ledger = _object(artifacts.get("trial_ledger"), "artifacts.trial_ledger")
    ledger_path = (root / str(ledger.get("path"))).resolve()
    try:
        ledger_relative = ledger_path.relative_to(root).as_posix()
    except ValueError as error:
        raise ValueError("监视来源 trial ledger 必须位于项目目录内") from error
    expected_ledger_hash = _text(
        ledger.get("sha256"), "artifacts.trial_ledger.sha256",
    )
    if sha256_file(ledger_path) != expected_ledger_hash:
        raise ValueError("监视来源 trial ledger 散列不匹配")
    if summary.get("config_hash") != config_hash:
        raise ValueError("监视来源 summary 与当前配置散列不一致")
    monitor_config = _object(
        config.get("evolution_monitor"), "evolution_monitor",
    )
    association_threshold = _number(
        monitor_config.get("parameter_association_threshold"),
        "parameter_association_threshold",
    )
    rows = _aggregate_trials(ledger_path, family)
    best_candidate_sharpe = max(
        _number(
            _object(row.get("metrics"), "trial.metrics").get("sharpe"),
            "trial.metrics.sharpe",
        )
        for row in rows
    )
    raw_reasons = evaluation.get("rejection_reasons")
    reasons = tuple(str(item) for item in raw_reasons) if isinstance(raw_reasons, list) else ()
    if evaluation.get("eligible") is True:
        evolution_action = "eligible_axis_refinement"
    elif "shadow_only" in reasons:
        evolution_action = "improve_fill_model_before_parameter_evolution"
    elif "non_positive_oos_net_return" in reasons or best_candidate_sharpe <= 0:
        evolution_action = "revise_hypothesis_or_cost_model"
    else:
        evolution_action = "stabilize_validation_before_parameter_evolution"
    current_identity = _text(
        summary.get("research_identity") or summary.get("run_id"),
        "research_identity",
    )
    current_time = _decision_time(summary)
    current_vintage_id, current_panel_sha256 = _data_vintage(summary)
    interval = _text(config.get("bar_interval"), "bar_interval")
    interval_seconds = _INTERVAL_SECONDS.get(interval)
    if interval_seconds is None:
        raise ValueError("bar_interval 不支持演进历史间隔")
    walk_forward = _object(config.get("walk_forward"), "walk_forward")
    minimum_spacing_bars = _integer(
        walk_forward.get("step_bars"), "walk_forward.step_bars",
    )
    minimum_spacing_seconds = minimum_spacing_bars * interval_seconds
    history_by_identity: dict[str, Mapping[str, object]] = {}
    excluded_history: list[Mapping[str, object]] = []
    for path in prior_summary_paths:
        prior, _prior_manifest_sha256 = _verified_summary_source(root, path)
        identity = _text(
            prior.get("research_identity") or prior.get("run_id"),
            "prior.research_identity",
        )
        prior_evaluation = _family_evaluation(prior, family)
        entry = _history_entry(prior, prior_evaluation, identity)
        if identity == current_identity or identity in history_by_identity:
            excluded_history.append({**entry, "reason": "duplicate_research_identity"})
            continue
        history_by_identity[identity] = entry
    candidates = sorted(
        history_by_identity.values(),
        key=lambda item: (str(item.get("decision_time")), str(item.get("run_id"))),
        reverse=True,
    )
    accepted_reverse: list[Mapping[str, object]] = []
    anchor_time = current_time
    seen_vintages = {current_vintage_id}
    for entry in candidates:
        vintage_id = _text(entry.get("data_vintage_id"), "data_vintage_id")
        if vintage_id in seen_vintages:
            excluded_history.append({**entry, "reason": "duplicate_data_vintage"})
            continue
        seen_vintages.add(vintage_id)
        entry_time = datetime.fromisoformat(
            _text(entry.get("decision_time"), "history.decision_time"),
        )
        if entry_time.tzinfo is None:
            raise ValueError("history.decision_time 必须带时区")
        if entry_time >= current_time:
            excluded_history.append({
                **entry,
                "reason": "not_before_current_decision_time",
            })
            continue
        if (anchor_time - entry_time).total_seconds() < minimum_spacing_seconds:
            excluded_history.append({
                **entry,
                "reason": "insufficient_temporal_spacing",
            })
            continue
        accepted_reverse.append(entry)
        anchor_time = entry_time
    history = list(reversed(accepted_reverse))
    excluded_history.sort(
        key=lambda item: (
            str(item.get("decision_time")),
            str(item.get("run_id")),
            str(item.get("reason")),
        ),
    )
    current_sharpe = _number(
        evaluation.get("adjusted_sharpe"), "adjusted_sharpe",
    )
    current_q = _number(evaluation.get("fdr_q"), "fdr_q")
    direction = "insufficient_history"
    minimum_history = _integer(
        monitor_config.get("minimum_history_runs"), "minimum_history_runs",
    )
    if len(history) + 1 >= minimum_history and history:
        previous_sharpe = _number(history[-1]["adjusted_sharpe"], "prior.sharpe")
        previous_q = _number(history[-1]["fdr_q"], "prior.fdr_q")
        sharpe_decay = _number(
            monitor_config.get("sharpe_decay_threshold"),
            "sharpe_decay_threshold",
        )
        q_deterioration = _number(
            monitor_config.get("fdr_q_deterioration_threshold"),
            "fdr_q_deterioration_threshold",
        )
        if current_sharpe < previous_sharpe - sharpe_decay or current_q > previous_q + q_deterioration:
            direction = "decaying"
        elif current_sharpe > previous_sharpe + sharpe_decay and current_q <= previous_q:
            direction = "improving"
        else:
            direction = "stable"
    return {
        "schema_version": 1,
        "monitor_method_version": "family-direction-monitor-v3",
        "run_id": summary.get("run_id"),
        "research_identity": current_identity,
        "data_vintage_id": current_vintage_id,
        "decision_time": summary.get("decision_time"),
        "family": family,
        "eligible": evaluation.get("eligible"),
        "rejection_reasons": evaluation.get("rejection_reasons"),
        "adjusted_sharpe": current_sharpe,
        "fdr_q": current_q,
        "latest_unallocated_target": evaluation.get("latest_unallocated_target"),
        "candidate_count": len(rows),
        "best_fixed_candidate_sharpe": best_candidate_sharpe,
        "evolution_action": evolution_action,
        "cross_run_direction": direction,
        "history": history,
        "excluded_history": excluded_history,
        "history_policy": {
            "method": "reverse_chronological_time_separated_vintages",
            "minimum_history_runs": minimum_history,
            "minimum_spacing_bars": minimum_spacing_bars,
            "minimum_spacing_seconds": minimum_spacing_seconds,
            "bar_interval": interval,
        },
        "source": {
            "summary_sha256": sha256_file(summary_path),
            "summary_path": summary_relative,
            "manifest_sha256": manifest_sha256,
            "config_hash": config_hash,
            "panel_sha256": current_panel_sha256,
            "code_identity": summary.get("code_identity"),
            "trial_ledger_path": ledger_relative,
            "trial_ledger_sha256": expected_ledger_hash,
        },
        "parameter_directions": _parameter_directions(
            rows,
            association_threshold,
        ),
        "interpretation": (
            "参数方向是候选网格内的关联证据，不是因果结论；扩展轴必须先登记新版本，"
            "不得复用一次性封存段。跨运行方向只使用至少相隔一个 walk-forward step 的"
            "冻结数据 vintage；同面板重复运行和时间过近的累计样本不计入历史。"
        ),
    }
