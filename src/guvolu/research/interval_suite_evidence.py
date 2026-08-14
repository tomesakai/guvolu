"""从已验证单节拍研究运行构造套件级统计证据。"""
from __future__ import annotations

import json
import math
import statistics
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path

from guvolu.research.provenance import (
    canonical_json,
    sha256_file,
    stable_identifier,
)
from guvolu.research.verification import (
    ArtifactIntegrityResult,
    verify_research_artifact_integrity,
)
from guvolu.research.verification_attestation import verify_research_run_cached

INTERVAL_SUITE_EVIDENCE_METHOD_VERSION = (
    "verified-global-fdr-aligned-oos-v2"
)


def _mapping(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} 必须为对象")
    return {str(key): item for key, item in value.items()}


def _list(value: object, name: str) -> list[object]:
    if not isinstance(value, list):
        raise ValueError(f"{name} 必须为数组")
    return value


def _text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} 必须为非空文本")
    return value.strip()


def _number(value: object, name: str) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValueError(f"{name} 必须为数值")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{name} 必须为有限数值")
    return result


def _positive_integer(value: object, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{name} 必须为正整数")
    return value


def suite_member_input_identity(manifest: Mapping[str, object]) -> tuple[str, ...]:
    """绑定成交 head、数据根和完整 suite 快照身份。"""
    source_root = _mapping(
        manifest.get("source_data_root"), "manifest.source_data_root",
    )
    snapshot = _mapping(
        manifest.get("source_data_snapshot"), "manifest.source_data_snapshot",
    )
    return (
        _text(manifest.get("input_head_generation"), "input_head_generation"),
        _text(manifest.get("input_receipt_sha256"), "input_receipt_sha256"),
        canonical_json(source_root),
        canonical_json(snapshot),
    )


def _string_list(value: object, name: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{name} 必须为非空数组")
    return tuple(_text(item, name) for item in value)


def global_fdr_q_values(p_values: Mapping[str, float]) -> Mapping[str, float]:
    """对预登记套件中的全部候选与家族路径执行 BH-FDR。"""
    if not p_values:
        raise ValueError("全局 FDR 试验域不能为空")
    ordered = sorted(
        (
            (_text(key, "trial_id"), _number(value, f"p_value.{key}"))
            for key, value in p_values.items()
        ),
        key=lambda item: (item[1], item[0]),
    )
    if any(value < 0.0 or value > 1.0 for _key, value in ordered):
        raise ValueError("p_value 必须位于零到一")
    count = len(ordered)
    result: dict[str, float] = {}
    running = 1.0
    for reverse_index in range(count - 1, -1, -1):
        trial_id, value = ordered[reverse_index]
        running = min(running, value * count / (reverse_index + 1))
        result[trial_id] = min(max(running, 0.0), 1.0)
    return result


def align_returns_to_interval(
    points: Mapping[str, Sequence[tuple[datetime, float]]],
    interval_seconds: int,
) -> Mapping[str, Mapping[int, float]]:
    """把各节拍 stitched OOS 对齐到共同最粗 UTC 结束栅格。"""
    if interval_seconds <= 0:
        raise ValueError("interval_seconds 必须为正数")
    aligned: dict[str, Mapping[int, float]] = {}
    for sleeve_id, rows in sorted(points.items()):
        buckets: dict[int, float] = {}
        seen_times: set[datetime] = set()
        for timestamp, value in rows:
            if timestamp.tzinfo is None or timestamp.utcoffset() is None:
                raise ValueError("收益时间必须带时区")
            if timestamp in seen_times:
                raise ValueError("同一 sleeve 包含重复收益时间")
            seen_times.add(timestamp)
            epoch = int(timestamp.timestamp())
            bucket_end = ((epoch - 1) // interval_seconds + 1) * interval_seconds
            buckets[bucket_end] = buckets.get(bucket_end, 0.0) + _number(
                value, f"returns.{sleeve_id}",
            )
        aligned[sleeve_id] = dict(sorted(buckets.items()))
    return aligned


def _read_json(path: Path) -> Mapping[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    return _mapping(value, str(path))


def _read_jsonl(path: Path) -> list[Mapping[str, object]]:
    records: list[Mapping[str, object]] = []
    for line_number, line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), start=1,
    ):
        if not line:
            continue
        records.append(_mapping(
            json.loads(line), f"{path.name}:{line_number}",
        ))
    return records


def _candidate_p_values(
    integrity: ArtifactIntegrityResult,
) -> Mapping[str, float]:
    path = integrity.artifact_paths.get("trial_ledger")
    if path is None:
        raise ValueError("研究运行缺少 trial_ledger")
    result: dict[str, float] = {}
    for record in _read_jsonl(path):
        if (
            record.get("record_type") != "trial"
            or record.get("segment") != "testing_aggregate"
        ):
            continue
        candidate_id = _text(record.get("candidate_id"), "candidate_id")
        if candidate_id in result:
            raise ValueError("testing_aggregate 包含重复 candidate_id")
        metrics = _mapping(record.get("metrics"), "trial.metrics")
        result[candidate_id] = _number(metrics.get("p_value"), "p_value")
    if not result:
        raise ValueError("trial_ledger 缺少 testing_aggregate")
    return result


def _family_rows(
    summary: Mapping[str, object],
) -> Mapping[str, Mapping[str, object]]:
    result: dict[str, Mapping[str, object]] = {}
    for raw in _list(summary.get("family_evaluations"), "family_evaluations"):
        row = _mapping(raw, "family_evaluation")
        family = _text(row.get("family"), "family")
        if family in result:
            raise ValueError("family_evaluations 包含重复流派")
        result[family] = row
    return result


def _stitched_return_points(
    integrity: ArtifactIntegrityResult,
    member_id: str,
) -> Mapping[str, Sequence[tuple[datetime, float]]]:
    path = integrity.artifact_paths.get("label_cost_replay")
    if path is None:
        raise ValueError("研究运行缺少 label_cost_replay")
    result: dict[str, list[tuple[datetime, float]]] = {}
    for record in _read_jsonl(path):
        if (
            record.get("record_type") != "label_cost"
            or record.get("in_walk_forward_oos") is not True
        ):
            continue
        label_time = datetime.fromisoformat(_text(
            record.get("label_available_time"), "label_available_time",
        ))
        replays = _mapping(record.get("replays"), "replays")
        stitched = _mapping(
            replays.get("walk_forward_stitched"), "walk_forward_stitched",
        )
        for family, raw_family in stitched.items():
            family_row = _mapping(raw_family, f"stitched.{family}")
            sleeve_id = stable_identifier("interval-sleeve", {
                "member_id": member_id,
                "family": family,
            })
            result.setdefault(sleeve_id, []).append((
                label_time,
                _number(family_row.get("next_net_return"), "next_net_return"),
            ))
    return result


def _correlation(
    left: Mapping[int, float],
    right: Mapping[int, float],
) -> tuple[int, float | None]:
    common = sorted(set(left).intersection(right))
    if len(common) < 2:
        return len(common), None
    left_values = [left[key] for key in common]
    right_values = [right[key] for key in common]
    left_mean = statistics.fmean(left_values)
    right_mean = statistics.fmean(right_values)
    left_std = statistics.pstdev(left_values)
    right_std = statistics.pstdev(right_values)
    if left_std == 0.0 or right_std == 0.0:
        return len(common), None
    covariance = statistics.fmean(
        (left_value - left_mean) * (right_value - right_mean)
        for left_value, right_value in zip(
            left_values, right_values, strict=True,
        )
    )
    return len(common), covariance / (left_std * right_std)


def _covariance(
    left: Mapping[int, float],
    right: Mapping[int, float],
) -> tuple[int, float]:
    common = sorted(set(left).intersection(right))
    if len(common) < 2:
        return len(common), 0.0
    left_values = [left[key] for key in common]
    right_values = [right[key] for key in common]
    left_mean = statistics.fmean(left_values)
    right_mean = statistics.fmean(right_values)
    return len(common), statistics.fmean(
        (left_value - left_mean) * (right_value - right_mean)
        for left_value, right_value in zip(
            left_values, right_values, strict=True,
        )
    )


def _project_suite_weights(
    weights: dict[str, float],
    sleeve_by_id: Mapping[str, Mapping[str, object]],
    gross_cap: float,
    directional_cap: float,
    reversion_cap: float,
    directional_families: Sequence[str],
) -> None:
    for sleeve_id in weights:
        weights[sleeve_id] = max(weights[sleeve_id], 0.0)
    directional = set(directional_families)
    directional_ids = [
        sleeve_id for sleeve_id in weights
        if sleeve_by_id[sleeve_id].get("family") in directional
    ]
    directional_total = sum(weights[sleeve_id] for sleeve_id in directional_ids)
    if directional_total > directional_cap:
        scale = directional_cap / directional_total
        for sleeve_id in directional_ids:
            weights[sleeve_id] *= scale
    reversion_ids = [
        sleeve_id for sleeve_id in weights
        if sleeve_by_id[sleeve_id].get("family") == "mean_reversion"
    ]
    reversion_total = sum(weights[sleeve_id] for sleeve_id in reversion_ids)
    if reversion_total > reversion_cap:
        scale = reversion_cap / reversion_total
        for sleeve_id in reversion_ids:
            weights[sleeve_id] *= scale
    gross = sum(weights.values())
    if gross > gross_cap:
        scale = gross_cap / gross
        for sleeve_id in weights:
            weights[sleeve_id] *= scale


def allocate_interval_sleeves(
    sleeves: Sequence[Mapping[str, object]],
    aligned_returns: Mapping[str, Mapping[int, float]],
    allocation: Mapping[str, object],
    interval_seconds: int,
) -> Mapping[str, object]:
    """在共同栅格上分配 suite-eligible sleeve，结果仅供研究。"""
    eligible = {
        _text(sleeve.get("sleeve_id"), "sleeve_id"): sleeve
        for sleeve in sleeves if sleeve.get("suite_eligible") is True
    }
    if not eligible:
        return {
            "status": "research_only",
            "weights": {},
            "reserve": 1.0,
            "aggregate_target": 0.0,
            "objective": 0.0,
            "alignment_interval_seconds": interval_seconds,
        }
    maximum_gross = _number(
        allocation.get("maximum_gross_weight"), "maximum_gross_weight",
    )
    minimum_reserve = _number(
        allocation.get("minimum_risk_reserve"), "minimum_risk_reserve",
    )
    maximum_gross = min(maximum_gross, 1.0 - minimum_reserve)
    directional_cap = _number(
        allocation.get("trend_breakout_cap"), "trend_breakout_cap",
    )
    reversion_cap = _number(
        allocation.get("mean_reversion_cap"), "mean_reversion_cap",
    )
    directional_families = _string_list(
        allocation.get("directional_families"), "directional_families",
    )
    risk_aversion = _number(allocation.get("risk_aversion"), "risk_aversion")
    uncertainty_penalty = _number(
        allocation.get("uncertainty_penalty"), "uncertainty_penalty",
    )
    iterations = _positive_integer(
        allocation.get("solver_iterations"), "solver_iterations",
    )
    step = _number(allocation.get("solver_step"), "solver_step")
    keys = tuple(sorted(eligible))
    expected: dict[str, float] = {}
    uncertainty: dict[str, float] = {}
    for sleeve_id in keys:
        metrics = _mapping(eligible[sleeve_id].get("metrics"), "metrics")
        expected[sleeve_id] = max(
            _number(metrics.get("annual_return"), "annual_return"), 0.0,
        ) * _number(metrics.get("capacity_score"), "capacity_score")
        uncertainty[sleeve_id] = _number(
            metrics.get("annual_volatility"), "annual_volatility",
        ) / math.sqrt(max(_number(metrics.get("bars"), "bars"), 1.0))
    periods_per_year = 365.0 * 24.0 * 60.0 * 60.0 / interval_seconds
    covariance: dict[tuple[str, str], float] = {}
    common_bars: dict[tuple[str, str], int] = {}
    for left in keys:
        for right in keys:
            count, value = _covariance(
                aligned_returns[left], aligned_returns[right],
            )
            common_bars[(left, right)] = count
            covariance[(left, right)] = value * periods_per_year
    weights = {key: 0.0 for key in keys}
    for _ordinal in range(iterations):
        updated = {}
        for key in keys:
            risk_gradient = 2.0 * risk_aversion * sum(
                covariance[(key, other)] * weights[other]
                for other in keys
            )
            updated[key] = weights[key] + step * (
                expected[key]
                - risk_gradient
                - uncertainty_penalty * uncertainty[key]
            )
        weights = updated
        _project_suite_weights(
            weights,
            eligible,
            maximum_gross,
            directional_cap,
            reversion_cap,
            directional_families,
        )
    contributions = {
        key: weights[key] * _number(
            eligible[key].get("latest_unallocated_target"), "latest target",
        )
        for key in keys
    }
    objective = sum(expected[key] * weights[key] for key in keys)
    objective -= risk_aversion * sum(
        weights[left] * covariance[(left, right)] * weights[right]
        for left in keys for right in keys
    )
    objective -= uncertainty_penalty * sum(
        uncertainty[key] * weights[key] for key in keys
    )
    return {
        "status": "research_only",
        "weights": weights,
        "reserve": max(1.0 - sum(weights.values()), 0.0),
        "portfolio_target_contributions": contributions,
        "aggregate_target": sum(contributions.values()),
        "objective": objective,
        "alignment_interval_seconds": interval_seconds,
        "minimum_pairwise_common_bars": min(common_bars.values()),
        "shared_caps": {
            "maximum_gross_weight": maximum_gross,
            "directional_weight": directional_cap,
            "mean_reversion_weight": reversion_cap,
        },
    }


def evaluate_interval_suite(
    repository_root: Path,
    plan: Mapping[str, object],
    manifest_paths: Sequence[Path],
) -> Mapping[str, object]:
    """完整复核成员运行，重算套件级 FDR 与跨节拍相关性。"""
    root = repository_root.resolve()
    raw_members = _list(plan.get("members"), "plan.members")
    members = {
        _text(_mapping(raw, "member").get("config_hash"), "config_hash"):
        _mapping(raw, "member")
        for raw in raw_members
    }
    if len(members) != len(raw_members):
        raise ValueError("plan.members 包含重复 config_hash")
    if len(manifest_paths) != len(members):
        raise ValueError("manifest 数量与套件成员不一致")
    domain = {
        _text(_mapping(raw, "trial").get("trial_id"), "trial_id"):
        _mapping(raw, "trial")
        for raw in _list(
            plan.get("global_multiple_testing_domain"), "trial_domain",
        )
    }
    if len(domain) != len(_list(
        plan.get("global_multiple_testing_domain"), "trial_domain",
    )):
        raise ValueError("全局试验域包含重复 trial_id")
    p_values: dict[str, float] = {}
    sleeves: list[dict[str, object]] = []
    return_points: dict[str, Sequence[tuple[datetime, float]]] = {}
    member_evidence: list[Mapping[str, object]] = []
    common_input_identity: tuple[str, ...] | None = None
    seen_configs: set[str] = set()
    for raw_manifest_path in manifest_paths:
        manifest_path = raw_manifest_path.resolve()
        verified = verify_research_run_cached(root, manifest_path)
        integrity = verify_research_artifact_integrity(root, manifest_path)
        manifest = integrity.manifest
        summary = integrity.summary
        config_hash = _text(manifest.get("config_hash"), "manifest.config_hash")
        member = members.get(config_hash)
        if member is None or config_hash in seen_configs:
            raise ValueError("manifest 不属于套件或重复")
        seen_configs.add(config_hash)
        member_id = _text(member.get("member_id"), "member_id")
        interval = _text(member.get("bar_interval"), "bar_interval")
        if summary.get("market_id") != plan.get("market_id"):
            raise ValueError("summary.market_id 与套件不一致")
        input_identity = suite_member_input_identity(manifest)
        if common_input_identity is None:
            common_input_identity = input_identity
        elif input_identity != common_input_identity:
            raise ValueError("套件成员没有使用同一 suite 快照与活动 head 收据")
        registry_path = integrity.artifact_paths.get("candidate_registry")
        if (
            registry_path is None
            or sha256_file(registry_path)
            != member.get("candidate_registry_sha256")
        ):
            raise ValueError("候选注册表与套件计划不一致")
        config_path = integrity.artifact_paths.get("config")
        if config_path is None:
            raise ValueError("manifest 缺少配置制品")
        config = _read_json(config_path)
        validation = _mapping(config.get("validation"), "validation")
        maximum_q = _number(validation.get("maximum_fdr_q"), "maximum_fdr_q")
        candidate_p = _candidate_p_values(integrity)
        family_rows = _family_rows(summary)
        family_trials = {
            _text(_mapping(raw, "family_trial").get("family"), "family"):
            _mapping(raw, "family_trial")
            for raw in _list(member.get("family_trials"), "family_trials")
        }
        candidate_domain = {
            _text(trial.get("candidate_id"), "candidate_id"):
            trial_id
            for trial_id, trial in domain.items()
            if trial.get("member_id") == member_id
            and trial.get("role") == "candidate_oos_path"
        }
        if set(candidate_domain) != set(candidate_p):
            raise ValueError("候选 p-value 与预登记试验域不一致")
        for candidate_id, value in candidate_p.items():
            p_values[candidate_domain[candidate_id]] = value
        for family, row in sorted(family_rows.items()):
            family_trial = family_trials.get(family)
            if family_trial is None:
                raise ValueError("家族结果不属于预登记试验域")
            trial_id = _text(family_trial.get("trial_id"), "family_trial_id")
            metrics = _mapping(row.get("metrics"), "family.metrics")
            p_values[trial_id] = _number(metrics.get("p_value"), "family.p_value")
            sleeve_id = stable_identifier("interval-sleeve", {
                "member_id": member_id,
                "family": family,
            })
            sleeves.append({
                "sleeve_id": sleeve_id,
                "member_id": member_id,
                "bar_interval": interval,
                "family": family,
                "family_trial_id": trial_id,
                "member_eligible": row.get("eligible") is True,
                "member_fdr_q": _number(row.get("fdr_q"), "family.fdr_q"),
                "maximum_fdr_q": maximum_q,
                "latest_unallocated_target": _number(
                    row.get("latest_unallocated_target"), "latest target",
                ),
                "metrics": metrics,
            })
        points = _stitched_return_points(integrity, member_id)
        if set(points) != {
            str(sleeve["sleeve_id"])
            for sleeve in sleeves if sleeve["member_id"] == member_id
        }:
            raise ValueError("stitched OOS 流派与摘要不一致")
        return_points.update(points)
        member_evidence.append({
            "member_id": member_id,
            "bar_interval": interval,
            "run_id": verified.run_id,
            "manifest_path": integrity.manifest_path.relative_to(root).as_posix(),
            "manifest_sha256": integrity.manifest_sha256,
            "decision_grade": summary.get("decision_grade") is True,
        })
    if set(p_values) != set(domain):
        raise ValueError("实际 p-value 没有完整覆盖预登记全局试验域")
    q_values = global_fdr_q_values(p_values)
    for sleeve in sleeves:
        trial_id = str(sleeve["family_trial_id"])
        suite_q = q_values[trial_id]
        sleeve["suite_fdr_q"] = suite_q
        sleeve["suite_eligible"] = (
            sleeve["member_eligible"] is True
            and suite_q <= _number(
                sleeve["maximum_fdr_q"], "sleeve.maximum_fdr_q",
            )
        )
    interval_seconds = max(
        int(_number(member.get("interval_seconds"), "interval_seconds"))
        for member in members.values()
    )
    aligned = align_returns_to_interval(return_points, interval_seconds)
    correlations: list[Mapping[str, object]] = []
    sleeve_ids = sorted(aligned)
    for left_index, left in enumerate(sleeve_ids):
        for right in sleeve_ids[left_index + 1:]:
            bars, correlation = _correlation(aligned[left], aligned[right])
            correlations.append({
                "left_sleeve_id": left,
                "right_sleeve_id": right,
                "common_bars": bars,
                "correlation": correlation,
            })
    allocation_contract = _mapping(
        plan.get("allocation_contract"), "allocation_contract",
    )
    suite_allocation = allocate_interval_sleeves(
        sleeves, aligned, allocation_contract, interval_seconds,
    )
    evidence: dict[str, object] = {
        "schema_version": 1,
        "method_version": INTERVAL_SUITE_EVIDENCE_METHOD_VERSION,
        "suite_plan_id": plan.get("suite_plan_id"),
        "market_id": plan.get("market_id"),
        "input_head_generation": common_input_identity[0]
        if common_input_identity else None,
        "input_receipt_sha256": common_input_identity[1]
        if common_input_identity else None,
        "alignment_interval_seconds": interval_seconds,
        "members": sorted(member_evidence, key=lambda item: str(item["bar_interval"])),
        "global_fdr": [
            {
                "trial_id": trial_id,
                "p_value": p_values[trial_id],
                "q_value": q_values[trial_id],
            }
            for trial_id in sorted(p_values)
        ],
        "sleeves": sorted(
            sleeves, key=lambda item: (str(item["family"]), str(item["bar_interval"])),
        ),
        "pairwise_correlations": correlations,
        "suite_research_allocation": suite_allocation,
        "operational_status": "disabled_pending_suite_readiness_and_holdout",
    }
    return {
        **evidence,
        "suite_evidence_id": stable_identifier("interval-suite-evidence", evidence),
    }
