"""策略家族候选方向与跨运行衰减监视。"""
from __future__ import annotations

import json
import statistics
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path

from guvolu.research.provenance import sha256_file, stable_identifier
from guvolu.research.verification_attestation import (
    verify_research_run_cached as verify_research_run,
)

_INTERVAL_SECONDS = {
    "5min": 300.0,
    "15min": 900.0,
    "1hour": 3600.0,
    "4hour": 14_400.0,
}

_COMPARISON_METHOD_FIELDS = (
    "pipeline_method_version",
    "generator_method_version",
    "p_value_method_version",
    "pbo_method_version",
    "block_bootstrap_method_version",
    "regime_attribution_method_version",
    "deflated_sharpe_method_version",
    "effective_trial_method_version",
    "parameter_stability_method_version",
    "panel_method_version",
    "panel_schema_version",
    "feature_method_version",
    "trade_flow_input_method_version",
    "trade_input_receipt_method_version",
)


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
    summary_path: Path,
    root: Path,
    manifest_sha256: str,
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
        "config_lineage_root_hash": (
            summary.get("config_lineage_root_hash") or summary.get("config_hash")
        ),
        "code_tree_digest": code_identity.get("tree_digest"),
        "comparison_cohort_id": _comparison_cohort_id(summary),
        "source_summary_path": summary_path.resolve().relative_to(root).as_posix(),
        "source_summary_sha256": sha256_file(summary_path),
        "source_manifest_sha256": manifest_sha256,
        "adjusted_sharpe": evaluation.get("adjusted_sharpe"),
        "fdr_q": evaluation.get("fdr_q"),
        "eligible": evaluation.get("eligible"),
    }


def _history_selection_key(entry: Mapping[str, object]) -> tuple[str, ...]:
    """用内容事实确定重复历史的唯一代表，不依赖 CLI 输入顺序。"""
    return (
        str(entry.get("config_hash")),
        str(entry.get("code_tree_digest")),
        str(entry.get("research_identity")),
        str(entry.get("source_summary_sha256")),
        str(entry.get("source_manifest_sha256")),
        str(entry.get("source_summary_path")),
    )


def _comparison_cohort_payload(
    summary: Mapping[str, object],
) -> Mapping[str, object]:
    """定义可比较的市场、配置谱系、试验范围和指标方法。"""
    raw_scope = summary.get("family_scope")
    scope = sorted(str(item) for item in raw_scope) if isinstance(raw_scope, list) else []
    return {
        "market_id": summary.get("market_id"),
        "family_scope": scope,
        "config_lineage_root_hash": (
            summary.get("config_lineage_root_hash") or summary.get("config_hash")
        ),
        "method_versions": {
            field: summary.get(field)
            for field in _COMPARISON_METHOD_FIELDS
            if field in summary
        },
    }


def _comparison_cohort_id(summary: Mapping[str, object]) -> str:
    """生成跨 vintage 指标可比性的内容身份。"""
    return stable_identifier("evolution-cohort", _comparison_cohort_payload(summary))


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


def _failure_attribution(
    evaluation: Mapping[str, object],
    best_candidate_sharpe: float,
) -> Mapping[str, object]:
    """区分信号、执行成本、成交模型与验证稳定性失败。"""
    raw_reasons = evaluation.get("rejection_reasons")
    reasons = tuple(
        str(item) for item in raw_reasons
    ) if isinstance(raw_reasons, list) else ()
    if evaluation.get("eligible") is True:
        return {"category": "eligible_performance"}
    if "shadow_only" in reasons:
        return {"category": "fill_model_unverified"}
    if "non_positive_oos_net_return" in reasons:
        raw_metrics = evaluation.get("validation_metrics")
        if raw_metrics is None:
            raw_metrics = evaluation.get("metrics")
        if not isinstance(raw_metrics, Mapping):
            return {"category": "unresolved_net_return_loss"}
        metrics = _object(raw_metrics, "family_evaluation.validation_metrics")
        net_return = _number(metrics.get("net_return"), "metrics.net_return")
        cost = _number(metrics.get("cost"), "metrics.cost")
        if cost < 0.0:
            raise ValueError("metrics.cost 不得为负")
        gross_return = net_return + cost
        category = (
            "execution_cost_dominated"
            if gross_return > 0.0 and net_return <= 0.0
            else "signal_edge_non_positive"
        )
        return {
            "category": category,
            "net_return": net_return,
            "estimated_gross_return_before_cost": gross_return,
            "cost": cost,
        }
    if best_candidate_sharpe <= 0.0:
        return {"category": "candidate_signal_non_positive"}
    return {"category": "validation_instability"}


def _monitor_family_run(
    repository_root: Path,
    summary_path: Path,
    family: str,
    config: Mapping[str, object],
    config_hash: str,
    prior_summary_paths: Sequence[Path] = (),
    *,
    monitor_method_version: str = "family-direction-monitor-v8",
) -> Mapping[str, object]:
    """用已经确定的完整历史集合重算流派监视制品。"""
    if monitor_method_version not in {
        "family-direction-monitor-v5",
        "family-direction-monitor-v6",
        "family-direction-monitor-v7",
        "family-direction-monitor-v8",
    }:
        raise ValueError("监视器方法版本不受支持")
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
    failure_attribution = _failure_attribution(evaluation, best_candidate_sharpe)
    raw_reasons = evaluation.get("rejection_reasons")
    reasons = tuple(
        str(item) for item in raw_reasons
    ) if isinstance(raw_reasons, list) else ()
    if monitor_method_version != "family-direction-monitor-v8":
        if evaluation.get("eligible") is True:
            evolution_action = "eligible_axis_refinement"
        elif "shadow_only" in reasons:
            evolution_action = "improve_fill_model_before_parameter_evolution"
        elif "non_positive_oos_net_return" in reasons or best_candidate_sharpe <= 0:
            evolution_action = "revise_hypothesis_or_cost_model"
        else:
            evolution_action = "stabilize_validation_before_parameter_evolution"
    else:
        category = str(failure_attribution["category"])
        if category == "eligible_performance":
            evolution_action = "eligible_axis_refinement"
        elif category == "fill_model_unverified":
            evolution_action = "improve_fill_model_before_parameter_evolution"
        elif category == "execution_cost_dominated":
            evolution_action = (
                "reduce_turnover_or_improve_execution_before_parameter_evolution"
            )
        elif category in {
            "signal_edge_non_positive",
            "candidate_signal_non_positive",
        }:
            evolution_action = "revise_hypothesis_before_parameter_evolution"
        elif category == "unresolved_net_return_loss":
            evolution_action = "revise_hypothesis_or_cost_model"
        else:
            evolution_action = "stabilize_validation_before_parameter_evolution"
    current_identity = _text(
        summary.get("research_identity") or summary.get("run_id"),
        "research_identity",
    )
    current_time = _decision_time(summary)
    current_vintage_id, current_panel_sha256 = _data_vintage(summary)
    current_cohort_payload = _comparison_cohort_payload(summary)
    current_cohort_id = _comparison_cohort_id(summary)
    interval = _text(config.get("bar_interval"), "bar_interval")
    interval_seconds = _INTERVAL_SECONDS.get(interval)
    if interval_seconds is None:
        raise ValueError("bar_interval 不支持演进历史间隔")
    walk_forward = _object(config.get("walk_forward"), "walk_forward")
    minimum_spacing_bars = _integer(
        walk_forward.get("step_bars"), "walk_forward.step_bars",
    )
    minimum_spacing_seconds = minimum_spacing_bars * interval_seconds
    comparable_by_identity: dict[str, list[Mapping[str, object]]] = defaultdict(list)
    history_sources: dict[tuple[str, str, str], Mapping[str, object]] = {}
    excluded_history: list[Mapping[str, object]] = []
    for path in sorted(
        {item.resolve() for item in prior_summary_paths},
        key=lambda item: item.as_posix(),
    ):
        try:
            source_relative = path.relative_to(root).as_posix()
        except ValueError as error:
            raise ValueError("监视历史 summary 必须位于项目目录内") from error
        try:
            prior, prior_manifest_sha256 = _verified_summary_source(root, path)
        except (OSError, RecursionError, ValueError) as error:
            exclusion: dict[str, object] = {
                "source_summary_path": source_relative,
                "reason": "unreadable_or_invalid_summary_artifact",
                "detail": str(error),
            }
            try:
                exclusion["source_summary_sha256"] = sha256_file(path)
            except OSError:
                pass
            excluded_history.append(exclusion)
            continue
        if _decision_time(prior) >= current_time:
            # 忽略非历史运行，保持旧证据稳定。
            continue
        source_sha256 = sha256_file(path)
        history_sources[
            (source_relative, source_sha256, prior_manifest_sha256)
        ] = {
            "summary_path": source_relative,
            "summary_sha256": source_sha256,
            "manifest_sha256": prior_manifest_sha256,
        }
        identity = _text(
            prior.get("research_identity") or prior.get("run_id"),
            "prior.research_identity",
        )
        prior_evaluation = _family_evaluation(prior, family)
        entry = _history_entry(
            prior,
            prior_evaluation,
            identity,
            path,
            root,
            prior_manifest_sha256,
        )
        if entry["comparison_cohort_id"] != current_cohort_id:
            excluded_history.append({**entry, "reason": "incomparable_cohort"})
            continue
        if identity == current_identity:
            excluded_history.append({**entry, "reason": "duplicate_research_identity"})
            continue
        comparable_by_identity[identity].append(entry)
    history_by_identity: dict[str, Mapping[str, object]] = {}
    for identity in sorted(comparable_by_identity):
        entries = sorted(
            comparable_by_identity[identity], key=_history_selection_key,
        )
        history_by_identity[identity] = entries[0]
        excluded_history.extend(
            {**entry, "reason": "duplicate_research_identity"}
            for entry in entries[1:]
        )
    comparable_by_vintage: dict[str, list[Mapping[str, object]]] = defaultdict(list)
    for entry in history_by_identity.values():
        vintage_id = _text(entry.get("data_vintage_id"), "data_vintage_id")
        if vintage_id == current_vintage_id:
            excluded_history.append({**entry, "reason": "duplicate_data_vintage"})
            continue
        comparable_by_vintage[vintage_id].append(entry)
    candidates: list[Mapping[str, object]] = []
    for vintage_id in sorted(comparable_by_vintage):
        entries = sorted(
            comparable_by_vintage[vintage_id], key=_history_selection_key,
        )
        candidates.append(entries[0])
        excluded_history.extend(
            {**entry, "reason": "duplicate_data_vintage"}
            for entry in entries[1:]
        )
    candidates.sort(key=_history_selection_key)
    candidates.sort(key=lambda item: str(item.get("decision_time")), reverse=True)
    accepted_reverse: list[Mapping[str, object]] = []
    anchor_time = current_time
    for entry in candidates:
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
            str(item.get("reason")),
            _history_selection_key(item),
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
        "monitor_method_version": monitor_method_version,
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
        "failure_attribution": failure_attribution,
        "evolution_action": evolution_action,
        "cross_run_direction": direction,
        "history": history,
        "excluded_history": excluded_history,
        "history_policy": {
            "method": (
                "content_deduplicated_reverse_chronological_"
                "time_separated_vintages"
            ),
            "comparison_cohort_id": current_cohort_id,
            "comparison_cohort": current_cohort_payload,
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
            "history_summaries": [
                history_sources[key] for key in sorted(history_sources)
            ],
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


def _canonical_summary_paths(root: Path, family: str) -> tuple[Path, ...]:
    """发现单流派目录中的 canonical 研究摘要。"""
    if Path(family).name != family:
        raise ValueError("family 不能用于 canonical 历史目录")
    reports = root / "reports" / "strategy-research"
    paths = set(
        (reports / "families" / family).glob("research-run-*/summary.json"),
    )
    return tuple(sorted(
        (path.resolve() for path in paths),
        key=lambda path: path.as_posix(),
    ))


def monitor_family_run(
    repository_root: Path,
    summary_path: Path,
    family: str,
    config: Mapping[str, object],
    config_hash: str,
    prior_summary_paths: Sequence[Path] = (),
    *,
    monitor_method_version: str = "family-direction-monitor-v8",
) -> Mapping[str, object]:
    """发现 canonical 全历史后生成单流派方向与健康监视制品。"""
    root = repository_root.resolve()
    current = summary_path.resolve()
    governed_paths = tuple(sorted(
        {
            *(path.resolve() for path in prior_summary_paths),
            *(
                path for path in _canonical_summary_paths(root, family)
                if path != current
            ),
        },
        key=lambda path: path.as_posix(),
    ))
    return _monitor_family_run(
        root,
        current,
        family,
        config,
        config_hash,
        governed_paths,
        monitor_method_version=monitor_method_version,
    )
