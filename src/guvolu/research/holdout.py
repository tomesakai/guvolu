"""冻结候选在一次性封存段上的精确评估。"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Mapping, Sequence

from guvolu.data.durable_io import atomic_write_text
from guvolu.research.features import compute_features
from guvolu.research.governance import (
    GOVERNANCE_METHOD_VERSION,
    HoldoutVintage,
    consume_holdout_vintage,
    get_holdout_vintage,
    record_holdout_verdict,
)
from guvolu.research.panel import build_panel_snapshot, freeze_trade_inputs, parse_time
from guvolu.research.provenance import (
    artifact_record,
    canonical_json,
    code_identity,
    sha256_file,
    stable_identifier,
)
from guvolu.research.validation import evaluate_targets, metrics_payload
from guvolu.research.verification import verify_research_run
from guvolu.strategy.baselines import generate_targets
from guvolu.strategy.contracts import CandidateSpec
from guvolu.strategy.expression import (
    EXPRESSION_METHOD_VERSION,
    candidate_identity,
    expression_id,
    strategy_expression,
)

HOLDOUT_METHOD_VERSION = "frozen-candidate-holdout-v2"
SECONDS_PER_YEAR = 365.0 * 24.0 * 60.0 * 60.0
_INTERVAL_SECONDS = {
    "5min": 300.0,
    "15min": 900.0,
    "1hour": 3600.0,
    "4hour": 14_400.0,
}


@dataclass(frozen=True)
class HoldoutRunResult:
    """一次已经永久消费 vintage 的评估结果。"""

    evaluation_id: str
    run_directory: Path
    manifest_path: Path
    result_path: Path
    manifest_sha256: str
    verdict: str


def _object(value: object, name: str) -> Mapping[str, object]:
    """验证 JSON 对象。"""
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} 必须为对象")
    return {str(key): item for key, item in value.items()}


def _text(value: object, name: str) -> str:
    """验证非空文本。"""
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} 必须为非空文本")
    return value.strip()


def _number(value: object, name: str) -> float:
    """验证 JSON 数值。"""
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValueError(f"{name} 必须为数值")
    return float(value)


def _integer(value: object, name: str) -> int:
    """验证正整数。"""
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{name} 必须为正整数")
    return value


def _load(path: Path) -> Mapping[str, object]:
    """读取 JSON 对象。"""
    return _object(json.loads(path.read_text(encoding="utf-8")), path.as_posix())


def _project_path(root: Path, value: Path, name: str) -> Path:
    """解析并限制治理输入输出位于项目目录。"""
    path = value.resolve() if value.is_absolute() else (root / value).resolve()
    try:
        path.relative_to(root)
    except ValueError as error:
        raise ValueError(f"{name} 必须位于项目目录内") from error
    return path


def _verified_source_manifest(
    root: Path,
    summary_file: Path,
) -> tuple[Mapping[str, object], Path]:
    """完整复核来源 manifest，并证明调用方 summary 正是受保护制品。"""
    manifest_file = summary_file.parent / "manifest.json"
    verification = verify_research_run(root, manifest_file)
    manifest = _load(manifest_file)
    artifacts = _object(manifest.get("artifacts"), "manifest.artifacts")
    summary_record = _object(artifacts.get("summary_json"), "summary_json")
    protected_summary = _project_path(
        root,
        Path(_text(summary_record.get("path"), "summary_json.path")),
        "manifest summary",
    )
    if protected_summary != summary_file:
        raise ValueError("source summary 不是 manifest 保护的 summary_json")
    if verification.run_id != manifest.get("run_id"):
        raise ValueError("来源运行标识与 manifest 复核结果不一致")
    return manifest, manifest_file


def _candidate_set(
    root: Path,
    source_summary: Mapping[str, object],
    source_manifest: Mapping[str, object],
) -> tuple[tuple[CandidateSpec, ...], Path]:
    """从已发布组合运行冻结 paper eligible 部署候选。"""
    if source_summary.get("pipeline_method_version") != "strategy-research-pipeline-v9":
        raise ValueError("holdout 只接受带表达式身份的 v9 来源运行")
    for field in ("run_id", "research_identity", "config_hash"):
        if source_summary.get(field) != source_manifest.get(field):
            raise ValueError(f"source summary 与 manifest 的 {field} 不一致")
    if source_summary.get("decision_grade") is not True:
        raise ValueError("holdout 候选必须来自 decision_grade 组合运行")
    scope = source_summary.get("family_scope")
    if not isinstance(scope, list) or len(scope) < 2:
        raise ValueError("holdout 候选必须来自包含多个流派的组合运行")
    evaluations = source_summary.get("family_evaluations")
    if not isinstance(evaluations, list):
        raise ValueError("source summary 缺少 family_evaluations")
    selected_ids = {
        _text(_object(item, "family_evaluation").get("deployment_candidate_id"),
              "deployment_candidate_id")
        for item in evaluations
        if _object(item, "family_evaluation").get("eligible") is True
        and _object(item, "family_evaluation").get("mode") == "paper"
    }
    if not selected_ids:
        raise ValueError("组合运行没有可进入 holdout 的 paper eligible 候选")
    artifacts = _object(source_summary.get("artifacts"), "artifacts")
    registry_record = _object(artifacts.get("candidate_registry"), "candidate_registry")
    manifest_artifacts = _object(
        source_manifest.get("artifacts"),
        "manifest.artifacts",
    )
    manifest_registry = _object(
        manifest_artifacts.get("candidate_registry"),
        "manifest.candidate_registry",
    )
    for field in ("path", "sha256", "bytes"):
        if registry_record.get(field) != manifest_registry.get(field):
            raise ValueError(
                f"summary 与 manifest 的 candidate_registry.{field} 不一致"
            )
    registry_path = _project_path(
        root,
        Path(_text(registry_record.get("path"), "registry.path")),
        "candidate registry",
    )
    registry = _load(registry_path)
    expected_registry_hash = _text(
        registry_record.get("sha256"),
        "candidate_registry.sha256",
    )
    if sha256_file(registry_path) != expected_registry_hash:
        raise ValueError("candidate registry 散列不匹配")
    expected_registry_bytes = registry_record.get("bytes")
    if (
        not isinstance(expected_registry_bytes, int)
        or isinstance(expected_registry_bytes, bool)
        or registry_path.stat().st_size != expected_registry_bytes
    ):
        raise ValueError("candidate registry 字节数不匹配")
    if registry.get("config_hash") != source_summary.get("config_hash"):
        raise ValueError("candidate registry 与来源配置身份不一致")
    if registry.get("expression_method_version") != EXPRESSION_METHOD_VERSION:
        raise ValueError("candidate registry 缺少受支持的表达式方法身份")
    raw_candidates = registry.get("candidates")
    if not isinstance(raw_candidates, list):
        raise ValueError("candidate registry 缺少 candidates")
    candidates: list[CandidateSpec] = []
    for raw in raw_candidates:
        record = _object(raw, "candidate")
        candidate_id = _text(record.get("candidate_id"), "candidate_id")
        if candidate_id not in selected_ids:
            continue
        parameters = _object(record.get("parameters"), "candidate.parameters")
        numeric_parameters: dict[str, int | float] = {}
        for name, value in parameters.items():
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                raise ValueError(f"候选参数必须为数值: {name}")
            numeric_parameters[name] = value
        family = _text(record.get("family"), "candidate.family")
        candidate_expression_id = _text(
            record.get("expression_id"),
            "candidate.expression_id",
        )
        template = strategy_expression(family)
        if candidate_expression_id != expression_id(template):
            raise ValueError("冻结候选表达式身份与当前执行器不一致")
        if candidate_id != candidate_identity(template, numeric_parameters):
            raise ValueError("冻结候选身份未绑定表达式与完整参数")
        candidates.append(CandidateSpec(
            candidate_id=candidate_id,
            family=family,
            mode=_text(record.get("mode"), "candidate.mode"),
            parameters=numeric_parameters,
            complexity=_integer(record.get("complexity"), "candidate.complexity"),
            expression_id=candidate_expression_id,
        ))
    if {candidate.candidate_id for candidate in candidates} != selected_ids:
        raise ValueError("部署候选与 candidate registry 不一致")
    return tuple(sorted(candidates, key=lambda item: item.candidate_id)), registry_path


def _fdr(p_values: Mapping[str, float]) -> Mapping[str, float]:
    """对固定候选集执行 Benjamini-Hochberg 校正。"""
    ordered = sorted(p_values.items(), key=lambda item: (item[1], item[0]))
    result: dict[str, float] = {}
    running = 1.0
    count = len(ordered)
    for index in range(count - 1, -1, -1):
        candidate_id, value = ordered[index]
        running = min(running, value * count / (index + 1))
        result[candidate_id] = min(running, 1.0)
    return result


def _vintage_payload(value: HoldoutVintage) -> Mapping[str, object]:
    """把 vintage dataclass 转换为规范 JSON 对象。"""
    raw = asdict(value)
    return {
        key: item.isoformat() if isinstance(item, datetime) else item
        for key, item in raw.items()
    }


def run_holdout_validation(
    root: Path,
    config_path: Path,
    source_summary_path: Path,
    vintage_id: str,
    output_root: Path | None = None,
) -> HoldoutRunResult:
    """先原子消费 vintage，再只评估冻结的部署候选，绝不重新选择。"""
    repository = root.resolve()
    config_file = _project_path(repository, config_path, "holdout config")
    summary_file = _project_path(repository, source_summary_path, "source summary")
    config = _load(config_file)
    config_hash = sha256_file(config_file)
    source_manifest, source_manifest_path = _verified_source_manifest(
        repository,
        summary_file,
    )
    source_summary = _load(summary_file)
    if source_summary.get("config_hash") != config_hash:
        raise ValueError("holdout 配置与来源研究冻结配置不一致")
    candidates, candidate_registry_path = _candidate_set(
        repository,
        source_summary,
        source_manifest,
    )
    market_id = _text(config.get("market_id"), "market_id")
    governance = _object(config.get("data_governance"), "data_governance")
    registry_path = _project_path(
        repository,
        Path(_text(governance.get("registry"), "registry")),
        "governance registry",
    )
    vintage = get_holdout_vintage(registry_path, vintage_id)
    if vintage.market_id != market_id:
        raise ValueError("vintage 与配置市场不一致")
    if vintage.status != "sealed":
        raise ValueError("vintage 已经消费，禁止重跑 holdout")
    inputs = freeze_trade_inputs(repository / "data", market_id)
    if inputs.maximum_event_time < vintage.end_time:
        raise ValueError("封存段尚未完整到达，不能提前消费")
    identity = code_identity(repository, (config_file,))
    if not identity.decision_grade:
        raise ValueError("holdout 只允许 clean commit 的决策级代码")
    source_code_identity = _object(
        source_summary.get("code_identity"),
        "source_summary.code_identity",
    )
    if source_code_identity.get("tree_digest") != identity.tree_digest:
        raise ValueError("holdout 必须使用来源运行冻结的同一研究代码树身份")
    candidate_set_hash = stable_identifier("candidate-set", {
        "holdout_method_version": HOLDOUT_METHOD_VERSION,
        "source_manifest_sha256": sha256_file(source_manifest_path),
        "source_summary_sha256": sha256_file(summary_file),
        "candidate_registry_sha256": sha256_file(candidate_registry_path),
        "candidate_ids": [candidate.candidate_id for candidate in candidates],
    })
    evaluation_id = stable_identifier("holdout-evaluation", {
        "holdout_method_version": HOLDOUT_METHOD_VERSION,
        "governance_method_version": GOVERNANCE_METHOD_VERSION,
        "vintage_id": vintage_id,
        "candidate_set_hash": candidate_set_hash,
        "config_hash": config_hash,
        "code_tree_digest": identity.tree_digest,
        "input_head_generation": inputs.head_generation,
        "input_artifact_ids": inputs.artifact_ids,
    })
    consume_holdout_vintage(
        registry_path,
        vintage_id,
        candidate_set_hash,
        evaluation_id,
    )
    output_base = _project_path(
        repository,
        output_root or Path("reports/strategy-research/holdout"),
        "holdout output",
    )
    run_directory = output_base / evaluation_id
    panel = build_panel_snapshot(
        inputs,
        repository / "data" / "research" / "holdout" / vintage_id,
        _text(config.get("bar_interval"), "bar_interval"),
        parse_time(config.get("from_time"), "from_time"),
        vintage.end_time,
        _integer(config.get("notional_scale"), "notional_scale"),
    )
    feature_config = _object(config.get("features"), "features")
    raw_lookbacks = feature_config.get("lookbacks")
    if not isinstance(raw_lookbacks, list):
        raise ValueError("features.lookbacks 必须为数组")
    features = compute_features(
        panel.bars,
        tuple(_integer(value, "lookback") for value in raw_lookbacks),
        _integer(feature_config.get("volume_lookback"), "volume_lookback"),
        _integer(
            feature_config.get("maximum_structural_gap_bars_assumption"),
            "maximum_structural_gap_bars_assumption",
        ),
    )
    indices = [
        index for index in range(1, len(panel.bars))
        if panel.bars[index - 1].decision_time >= vintage.start_time
        and panel.bars[index].decision_time <= vintage.end_time
    ]
    if not indices or indices != list(range(indices[0], indices[-1] + 1)):
        raise ValueError("封存段没有连续可评分的研究柱")
    start, end = indices[0], indices[-1] + 1
    policy = _object(governance.get("holdout_policy"), "holdout_policy")
    minimum_bars = _integer(policy.get("minimum_bars"), "minimum_bars")
    if end - start < minimum_bars:
        raise ValueError("封存段评分柱数低于预登记门槛")
    interval = _text(config.get("bar_interval"), "bar_interval")
    interval_seconds = _INTERVAL_SECONDS.get(interval)
    if interval_seconds is None:
        raise ValueError("holdout 不支持该研究节拍")
    periods_per_year = SECONDS_PER_YEAR / interval_seconds
    maximum_gap = interval_seconds * _integer(
        feature_config.get("maximum_structural_gap_bars_assumption"),
        "maximum_structural_gap_bars_assumption",
    )
    cost_config = _object(config.get("cost_model"), "cost_model")
    cost_rate = sum(_number(cost_config.get(name), name) for name in (
        "fee_bps_assumption",
        "half_spread_bps_assumption",
        "slippage_bps_assumption",
        "impact_bps_assumption",
    )) / 10_000.0
    capacity = _number(
        cost_config.get("capacity_notional_quote"),
        "capacity_notional_quote",
    )
    raw_results: list[dict[str, object]] = []
    p_values: dict[str, float] = {}
    for candidate in candidates:
        targets = generate_targets(candidate, panel.bars, features, periods_per_year)
        metrics = evaluate_targets(
            panel.bars,
            targets,
            start,
            end,
            cost_rate,
            capacity,
            maximum_gap,
            periods_per_year,
        )
        p_values[candidate.candidate_id] = metrics.p_value
        raw_results.append({
            "candidate_id": candidate.candidate_id,
            "family": candidate.family,
            "parameters": dict(candidate.parameters),
            "metrics": metrics_payload(metrics),
        })
    q_values = _fdr(p_values)
    passed_families: list[str] = []
    candidate_results: list[dict[str, object]] = []
    for result in raw_results:
        metric_record = _object(result["metrics"], "metrics")
        candidate_id = _text(result["candidate_id"], "candidate_id")
        reasons: list[str] = []
        if _number(metric_record.get("net_return"), "net_return") <= 0:
            reasons.append("non_positive_holdout_net_return")
        if _number(metric_record.get("sharpe"), "sharpe") < _number(
            policy.get("minimum_sharpe"), "minimum_sharpe"
        ):
            reasons.append("holdout_sharpe_failed")
        if _number(metric_record.get("maximum_drawdown"), "maximum_drawdown") > _number(
            policy.get("maximum_drawdown"), "maximum_drawdown"
        ):
            reasons.append("holdout_drawdown_failed")
        if q_values[candidate_id] > _number(policy.get("maximum_fdr_q"), "maximum_fdr_q"):
            reasons.append("holdout_fdr_failed")
        passed = not reasons
        if passed:
            passed_families.append(_text(result["family"], "family"))
        candidate_results.append({
            **result,
            "fdr_q": q_values[candidate_id],
            "passed": passed,
            "rejection_reasons": reasons,
        })
    verdict = "passed" if len(passed_families) == len(candidates) else "failed"
    result_payload = {
        "schema_version": 1,
        "holdout_method_version": HOLDOUT_METHOD_VERSION,
        "evaluation_id": evaluation_id,
        "vintage": _vintage_payload(vintage),
        "candidate_set_hash": candidate_set_hash,
        "source_summary_sha256": sha256_file(summary_file),
        "config_hash": sha256_file(config_file),
        "code_identity": asdict(identity),
        "panel_sha256": panel.panel_sha256,
        "score_start": panel.bars[start - 1].decision_time.isoformat(),
        "score_end": panel.bars[end - 1].decision_time.isoformat(),
        "score_bars": end - start,
        "policy": dict(policy),
        "candidate_results": candidate_results,
        "passed_families": sorted(passed_families),
        "verdict": verdict,
        "evaluated_at": datetime.now(UTC).isoformat(),
    }
    result_path = run_directory / "result.json"
    atomic_write_text(result_path, canonical_json(result_payload) + "\n")
    record_holdout_verdict(
        registry_path,
        vintage_id,
        canonical_json({
            "evaluation_id": evaluation_id,
            "verdict": verdict,
            "passed_families": sorted(passed_families),
            "result_sha256": sha256_file(result_path),
        }),
    )
    manifest = {
        "schema_version": 1,
        "holdout_method_version": HOLDOUT_METHOD_VERSION,
        "evaluation_id": evaluation_id,
        "vintage_id": vintage_id,
        "candidate_set_hash": candidate_set_hash,
        "artifacts": {
            "panel": {
                **artifact_record(panel.panel_path, "holdout_panel"),
                "path": panel.panel_path.resolve().relative_to(repository).as_posix(),
            },
            "result": {
                **artifact_record(result_path, "holdout_result"),
                "path": result_path.resolve().relative_to(repository).as_posix(),
            },
        },
    }
    manifest_path = run_directory / "manifest.json"
    atomic_write_text(manifest_path, canonical_json(manifest) + "\n")
    return HoldoutRunResult(
        evaluation_id=evaluation_id,
        run_directory=run_directory,
        manifest_path=manifest_path,
        result_path=result_path,
        manifest_sha256=sha256_file(manifest_path),
        verdict=verdict,
    )
