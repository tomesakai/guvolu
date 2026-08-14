"""只读检查策略运行与一次性 promotion 的外部数据就绪状态。"""
from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path

from guvolu.research.config_lineage import (
    load_governed_strategy_config,
    verified_config_lineage_paths,
)
from guvolu.research.data_location import resolve_data_root_locator
from guvolu.research.features import compute_features
from guvolu.research.governance import (
    HoldoutVintage,
    get_frozen_forward_plan_for_vintage,
    list_holdout_vintages,
)
from guvolu.research.panel import freeze_trade_inputs, load_panel_bars
from guvolu.research.provenance import code_identity
from guvolu.research.verification import verify_research_run
from guvolu.strategy.contracts import ResearchBar

READINESS_METHOD_VERSION = "strategy-readiness-v3"
_INTERVAL_SECONDS = {
    "5min": 300,
    "15min": 900,
    "1hour": 3600,
    "4hour": 14_400,
}


def _object(value: object, name: str) -> Mapping[str, object]:
    """验证 JSON 对象。"""
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} 必须为对象")
    return {str(key): item for key, item in value.items()}


def _text(value: object, name: str) -> str:
    """验证非空文本。"""
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} 必须为非空文本")
    return value


def _integer(value: object, name: str) -> int:
    """验证正整数。"""
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{name} 必须为正整数")
    return value


def _read_object(path: Path, name: str) -> Mapping[str, object]:
    """读取并验证 UTF-8 JSON 对象。"""
    try:
        return _object(json.loads(path.read_text(encoding="utf-8")), name)
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"无法读取 {name}: {path}") from error


def _resolve(root: Path, value: object, name: str) -> Path:
    """解析仓库相对路径。"""
    path = Path(_text(value, name))
    return path if path.is_absolute() else root / path


def trailing_contiguous_bars(
    bars: Sequence[ResearchBar],
    maximum_structural_gap_bars: int,
) -> int:
    """计算最新结构性断点之后的连续观测柱数。"""
    if not bars:
        return 0
    if maximum_structural_gap_bars <= 0:
        raise ValueError("结构性空窗上限必须为正数")
    if len(bars) == 1:
        return 1
    expected = bars[1].open_time - bars[0].open_time
    count = 1
    for index in range(len(bars) - 1, 0, -1):
        gap = bars[index].open_time - bars[index - 1].open_time
        if gap < expected or gap > expected * maximum_structural_gap_bars:
            break
        count += 1
    return count


def _vintage_payload(
    vintage: HoldoutVintage,
    maximum_event_time: datetime,
) -> Mapping[str, object]:
    """生成不消费 vintage 的就绪事实。"""
    return {
        "vintage_id": vintage.vintage_id,
        "market_id": vintage.market_id,
        "start_time": vintage.start_time.isoformat(),
        "end_time": vintage.end_time.isoformat(),
        "sealed_at": vintage.sealed_at.isoformat(),
        "status": vintage.status,
        "data_complete": maximum_event_time >= vintage.end_time,
        "consumed_at": (
            vintage.consumed_at.isoformat()
            if vintage.consumed_at is not None else None
        ),
        "verdict": vintage.verdict,
    }


def strategy_readiness(
    repository_root: Path,
    config_path: Path,
    manifest_path: Path | None = None,
    reference_time: datetime | None = None,
) -> Mapping[str, object]:
    """验证当前研究来源，并报告 operational 与 holdout 就绪度。"""
    root = repository_root.resolve()
    config_file = config_path if config_path.is_absolute() else root / config_path
    (
        config,
        config_hash,
        config_lineage_root_hash,
        config_lineage_depth,
    ) = load_governed_strategy_config(root, config_file)
    config_source_paths = verified_config_lineage_paths(root, config_file)
    verified = verify_research_run(root, manifest_path)
    manifest = _read_object(verified.manifest_path, "research manifest")
    source_data_root = resolve_data_root_locator(
        root, manifest.get("source_data_root"),
    )
    artifacts = _object(manifest.get("artifacts"), "manifest.artifacts")
    summary_record = _object(
        artifacts.get("summary_json"), "manifest.artifacts.summary_json",
    )
    summary_path = _resolve(root, summary_record.get("path"), "summary path")
    summary = _read_object(summary_path, "research summary")
    market_id = _text(summary.get("market_id"), "summary.market_id")
    current_inputs = freeze_trade_inputs(source_data_root, market_id)
    identity = code_identity(root, config_source_paths)
    source_identity = _object(summary.get("code_identity"), "summary.code_identity")
    source_tree_digest = _text(
        source_identity.get("tree_digest"), "source tree digest",
    )
    tree_matches = source_tree_digest == identity.tree_digest
    config_matches = (
        summary.get("config_hash") == config_hash
        and summary.get("config_lineage_root_hash")
        == config_lineage_root_hash
        and summary.get("config_lineage_depth") == config_lineage_depth
    )

    panel_record = _object(artifacts.get("panel"), "manifest.artifacts.panel")
    panel_path = _resolve(root, panel_record.get("path"), "panel path")
    bars = load_panel_bars(panel_path)
    features_config = _object(config.get("features"), "features")
    raw_lookbacks = features_config.get("lookbacks")
    if not isinstance(raw_lookbacks, list):
        raise ValueError("features.lookbacks 必须为数组")
    lookbacks = tuple(_integer(value, "features.lookbacks") for value in raw_lookbacks)
    volume_lookback = _integer(
        features_config.get("volume_lookback"), "features.volume_lookback",
    )
    state_lookback = _integer(
        features_config.get("state_lookback"), "features.state_lookback",
    )
    maximum_gap = _integer(
        features_config.get("maximum_structural_gap_bars_assumption"),
        "features.maximum_structural_gap_bars_assumption",
    )
    features = compute_features(
        bars,
        lookbacks,
        volume_lookback,
        maximum_gap,
    )
    valid_indices = [
        index for index, feature in enumerate(features)
        if feature.contiguous
        and feature.volume_score is not None
        and feature.trend_scores.get(state_lookback) is not None
    ]
    if not valid_indices:
        raise ValueError("研究面板没有任何可用策略决策时点")
    latest_valid_index = valid_indices[-1]
    latest_valid_time = features[latest_valid_index].decision_time
    latest_panel_time = bars[-1].decision_time
    required_contiguous_bars = max((*lookbacks, volume_lookback, state_lookback)) + 1
    contiguous_bars = trailing_contiguous_bars(bars, maximum_gap)
    remaining_bars = max(0, required_contiguous_bars - contiguous_bars)
    interval = _text(config.get("bar_interval"), "bar_interval")
    interval_seconds = _INTERVAL_SECONDS.get(interval)
    if interval_seconds is None:
        raise ValueError("bar_interval 不支持 readiness 估算")
    evaluated_at = reference_time or datetime.now(UTC)
    if evaluated_at.tzinfo is None:
        evaluated_at = evaluated_at.replace(tzinfo=UTC)
    else:
        evaluated_at = evaluated_at.astimezone(UTC)
    maximum_age = _integer(
        config.get("strategy_decision_max_age_seconds"),
        "strategy_decision_max_age_seconds",
    )
    feature_age_seconds = (evaluated_at - latest_valid_time).total_seconds()
    input_summary = _object(summary.get("input"), "summary.input")
    head_matches = input_summary.get("head_generation") == current_inputs.head_generation
    operational_blockers: list[str] = []
    if latest_valid_index != len(features) - 1:
        operational_blockers.append("latest_panel_feature_not_mature")
    if feature_age_seconds < 0:
        operational_blockers.append("feature_time_after_reference")
    elif feature_age_seconds > maximum_age:
        operational_blockers.append("feature_snapshot_stale")
    if not head_matches:
        operational_blockers.append("active_input_head_changed")
    if not identity.decision_grade:
        operational_blockers.append("current_code_not_decision_grade")
    if not tree_matches:
        operational_blockers.append("source_code_tree_mismatch")
    if not config_matches:
        operational_blockers.append("source_config_mismatch")

    governance = _object(config.get("data_governance"), "data_governance")
    registry_path = _resolve(root, governance.get("registry"), "governance registry")
    vintages = list_holdout_vintages(registry_path)
    market_vintages = tuple(item for item in vintages if item.market_id == market_id)
    sealed = tuple(item for item in market_vintages if item.status == "sealed")
    sealed_payloads = tuple(
        _vintage_payload(item, current_inputs.maximum_event_time) for item in sealed
    )
    completed_vintages = tuple(
        item for item in sealed_payloads if item["data_complete"] is True
    )
    unplanned_vintage_ids = tuple(
        item.vintage_id for item in sealed
        if get_frozen_forward_plan_for_vintage(
            registry_path, item.vintage_id,
        ) is None
    )
    raw_evaluations = summary.get("family_evaluations")
    if not isinstance(raw_evaluations, list):
        raise ValueError("summary.family_evaluations 必须为数组")
    eligible_families = tuple(
        str(item.get("family"))
        for item in raw_evaluations
        if isinstance(item, Mapping)
        and item.get("eligible") is True
        and item.get("mode") == "paper"
    )
    promotion_blockers: list[str] = []
    if summary.get("decision_grade") is not True:
        promotion_blockers.append("source_run_not_decision_grade")
    if not identity.decision_grade:
        promotion_blockers.append("current_code_not_decision_grade")
    if not tree_matches:
        promotion_blockers.append("source_code_tree_mismatch")
    if not config_matches:
        promotion_blockers.append("source_config_mismatch")
    if not eligible_families:
        promotion_blockers.append("no_frozen_paper_eligible_family")
    if not sealed:
        promotion_blockers.append("no_sealed_holdout_vintage")
    elif unplanned_vintage_ids:
        promotion_blockers.append("sealed_vintage_has_no_frozen_forward_plan")
    elif not completed_vintages:
        promotion_blockers.append("sealed_holdout_vintage_incomplete")

    if "no_sealed_holdout_vintage" in promotion_blockers:
        promotion_next_action = "seal_future_vintage_before_its_start"
    elif "sealed_vintage_has_no_frozen_forward_plan" in promotion_blockers:
        promotion_next_action = "freeze_forward_plan_before_vintage_start"
    elif "sealed_holdout_vintage_incomplete" in promotion_blockers:
        promotion_next_action = "wait_for_sealed_vintage_end"
    elif promotion_blockers:
        promotion_next_action = "refresh_source_research_before_holdout"
    else:
        promotion_next_action = "run_holdout_validation_once"
    if "latest_panel_feature_not_mature" in operational_blockers:
        operational_next_action = "wait_for_feature_maturity"
    elif "active_input_head_changed" in operational_blockers:
        operational_next_action = "rerun_research_at_bar_boundary"
    elif "feature_snapshot_stale" in operational_blockers:
        operational_next_action = "run_within_freshness_window_after_bar_close"
    elif operational_blockers:
        operational_next_action = "repair_operational_blockers"
    else:
        operational_next_action = "operational_snapshot_ready"

    return {
        "schema_version": 1,
        "readiness_method_version": READINESS_METHOD_VERSION,
        "evaluated_at": evaluated_at.isoformat(),
        "run_id": verified.run_id,
        "manifest_sha256": verified.manifest_sha256,
        "summary_path": summary_path.relative_to(root).as_posix(),
        "market_id": market_id,
        "source": {
            "data_root": manifest.get("source_data_root"),
            "decision_grade": summary.get("decision_grade"),
            "eligible_families": list(eligible_families),
            "config_matches": config_matches,
            "source_config_hash": summary.get("config_hash"),
            "current_config_hash": config_hash,
            "source_config_lineage_root_hash": (
                summary.get("config_lineage_root_hash")
            ),
            "current_config_lineage_root_hash": config_lineage_root_hash,
            "source_config_lineage_depth": summary.get("config_lineage_depth"),
            "current_config_lineage_depth": config_lineage_depth,
            "source_git_hash": source_identity.get("git_hash"),
            "current_git_hash": identity.git_hash,
            "source_tree_digest": source_tree_digest,
            "current_tree_digest": identity.tree_digest,
            "tree_matches": tree_matches,
            "current_dirty": identity.dirty,
            "current_decision_grade": identity.decision_grade,
        },
        "operational": {
            "ready": not operational_blockers,
            "blockers": operational_blockers,
            "next_action": operational_next_action,
            "published_panel_sha256": panel_record.get("sha256"),
            "published_panel_bars": len(bars),
            "published_panel_decision_time": latest_panel_time.isoformat(),
            "latest_valid_feature_time": latest_valid_time.isoformat(),
            "feature_age_seconds": feature_age_seconds,
            "maximum_feature_age_seconds": maximum_age,
            "required_contiguous_bars": required_contiguous_bars,
            "trailing_contiguous_bars": contiguous_bars,
            "remaining_maturity_bars": remaining_bars,
            "earliest_maturity_if_each_bar_arrives": (
                latest_panel_time + timedelta(
                    seconds=remaining_bars * interval_seconds,
                )
            ).isoformat(),
            "published_head_generation": input_summary.get("head_generation"),
            "current_head_generation": current_inputs.head_generation,
            "head_matches": head_matches,
            "current_maximum_event_time": (
                current_inputs.maximum_event_time.isoformat()
            ),
        },
        "promotion": {
            "ready": not promotion_blockers,
            "blockers": promotion_blockers,
            "next_action": promotion_next_action,
            "registry": registry_path.relative_to(root).as_posix(),
            "sealed_vintages": list(sealed_payloads),
            "completed_vintage_ids": [
                str(item["vintage_id"]) for item in completed_vintages
            ],
            "unplanned_vintage_ids": list(unplanned_vintage_ids),
            "consumed_vintage_count": sum(
                item.status == "consumed" for item in market_vintages
            ),
        },
        "read_only": True,
    }
