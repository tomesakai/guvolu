"""冻结候选在一次性封存段上的精确评估。"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Mapping, Sequence

from guvolu.data.durable_io import atomic_write_text
from guvolu.research.config_lineage import attest_config_lineage_snapshot
from guvolu.research.contracts import (
    HOLDOUT_MANIFEST_SCHEMA_VERSION,
    HOLDOUT_METHOD_VERSION,
)
from guvolu.research.data_location import resolve_data_root_locator
from guvolu.research.features import compute_features
from guvolu.research.governance import (
    GOVERNANCE_METHOD_VERSION,
    HoldoutVintage,
    finalize_holdout_evaluation,
    get_active_head_receipt,
    get_frozen_forward_plan_for_vintage,
    get_holdout_vintage,
    register_active_head_receipt,
    start_holdout_evaluation_attempt,
    update_holdout_evaluation_attempt,
)
from guvolu.research.panel import (
    attest_trade_input_receipt,
    build_panel_snapshot,
    capture_trade_input_receipt,
    load_panel_bars,
    parse_time,
)
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


def verified_source_manifest(
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


def load_frozen_candidates(
    root: Path,
    source_summary: Mapping[str, object],
    source_manifest: Mapping[str, object],
) -> tuple[tuple[CandidateSpec, ...], Path]:
    """从已发布组合运行冻结 paper eligible 部署候选。"""
    if source_summary.get("pipeline_method_version") != "strategy-research-pipeline-v12":
        raise ValueError("holdout v4 只接受可现场重建全部证据的 v12 来源运行")
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


def frozen_candidate_set_identity(
    source_manifest_path: Path,
    source_summary_path: Path,
    candidate_registry_path: Path,
    candidates: Sequence[CandidateSpec],
) -> Mapping[str, object]:
    """生成可由终态 manifest 复核的冻结候选集合身份载荷。"""
    return {
        "holdout_method_version": HOLDOUT_METHOD_VERSION,
        "source_manifest_sha256": sha256_file(source_manifest_path),
        "source_summary_sha256": sha256_file(source_summary_path),
        "candidate_registry_sha256": sha256_file(candidate_registry_path),
        "candidate_ids": [candidate.candidate_id for candidate in candidates],
    }


def frozen_candidate_set_hash(
    source_manifest_path: Path,
    source_summary_path: Path,
    candidate_registry_path: Path,
    candidates: Sequence[CandidateSpec],
) -> str:
    """生成由完整来源制品和执行候选共同决定的冻结集合散列。"""
    return stable_identifier("candidate-set", frozen_candidate_set_identity(
        source_manifest_path,
        source_summary_path,
        candidate_registry_path,
        candidates,
    ))


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


def _score_decision_times(
    bars: Sequence[object],
    start: int,
    end: int,
) -> tuple[datetime, ...]:
    """返回与 evaluate_targets 前一柱持仓索引完全相同的决策时点。"""
    decisions: list[datetime] = []
    for index in range(start, end):
        decision_time = getattr(bars[index - 1], "decision_time", None)
        if not isinstance(decision_time, datetime):
            raise ValueError("评分柱缺少 decision_time")
        decisions.append(decision_time)
    if not decisions:
        raise ValueError("评分时点表不得为空")
    return tuple(decisions)


def _attested_artifact_path(
    root: Path,
    artifacts: Mapping[str, object],
    name: str,
) -> Path:
    """解析并复核 holdout manifest 中的一个内容寻址制品。"""
    record = _object(artifacts.get(name), f"manifest.artifacts.{name}")
    path = _project_path(
        root,
        Path(_text(record.get("path"), f"manifest.artifacts.{name}.path")),
        f"holdout {name}",
    )
    if sha256_file(path) != record.get("sha256"):
        raise ValueError(f"holdout {name} 制品 SHA-256 不匹配")
    size = record.get("bytes")
    if (
        not isinstance(size, int)
        or isinstance(size, bool)
        or size != path.stat().st_size
    ):
        raise ValueError(f"holdout {name} 制品字节数不匹配")
    return path


def attest_holdout_terminal_artifacts(
    repository_root: Path,
    manifest_path: Path,
) -> None:
    """从冻结候选、面板、前向目标和成本配置现场重算终局指标。"""
    root = repository_root.resolve()
    manifest = _load(manifest_path)
    source_data_root = resolve_data_root_locator(
        root, manifest.get("source_data_root"),
    )
    artifacts = _object(manifest.get("artifacts"), "manifest.artifacts")
    source_manifest_path = _attested_artifact_path(
        root, artifacts, "source_manifest",
    )
    source_summary_path = _attested_artifact_path(
        root, artifacts, "source_summary",
    )
    candidate_registry_path = _attested_artifact_path(
        root, artifacts, "candidate_registry",
    )
    candidate_set_identity = _object(
        manifest.get("candidate_set_identity"), "candidate_set_identity",
    )
    source_hashes = {
        "source_manifest_sha256": sha256_file(source_manifest_path),
        "source_summary_sha256": sha256_file(source_summary_path),
        "candidate_registry_sha256": sha256_file(candidate_registry_path),
    }
    for name, value in source_hashes.items():
        if candidate_set_identity.get(name) != value:
            raise ValueError(f"holdout {name} 未直接绑定冻结候选集合")
    config_path = _attested_artifact_path(root, artifacts, "config")
    config_lineage_path = _attested_artifact_path(
        root, artifacts, "config_lineage",
    )
    receipt_path = _attested_artifact_path(root, artifacts, "input_receipt")
    panel_path = _attested_artifact_path(root, artifacts, "panel")
    schedule_path = _attested_artifact_path(root, artifacts, "score_schedule")
    result_path = _attested_artifact_path(root, artifacts, "result")
    source_manifest = _load(source_manifest_path)
    source_summary = _load(source_summary_path)
    candidates, protected_registry_path = load_frozen_candidates(
        root, source_summary, source_manifest,
    )
    if protected_registry_path != candidate_registry_path:
        raise ValueError("holdout candidate registry 与冻结来源不一致")
    config, config_hash, _root_hash, _depth, _sources, _snapshots = (
        attest_config_lineage_snapshot(root, config_lineage_path, config_path)
    )
    evaluation_identity = _object(
        manifest.get("evaluation_identity"), "evaluation_identity",
    )
    if config_hash != evaluation_identity.get("config_hash"):
        raise ValueError("holdout 配置谱系与 evaluation_identity 不一致")
    result = _load(result_path)
    schedule = _load(schedule_path)
    raw_attempt_ids = manifest.get("input_attempt_ids")
    raw_artifact_ids = manifest.get("input_artifact_ids")
    raw_normalizations = manifest.get("normalization_versions")
    if (
        not isinstance(raw_attempt_ids, list)
        or not isinstance(raw_artifact_ids, list)
        or not isinstance(raw_normalizations, list)
    ):
        raise ValueError("holdout manifest 缺少完整 panel 输入身份")
    attempt_ids = tuple(_text(item, "input_attempt_id") for item in raw_attempt_ids)
    artifact_ids = tuple(_text(item, "input_artifact_id") for item in raw_artifact_ids)
    normalizations = tuple(
        _text(item, "normalization_version") for item in raw_normalizations
    )
    market_id = _text(config.get("market_id"), "market_id")
    governance = _object(config.get("data_governance"), "data_governance")
    registry_path = _project_path(
        root,
        Path(_text(governance.get("registry"), "data_governance.registry")),
        "governance registry",
    )
    evaluation_id = _text(manifest.get("evaluation_id"), "evaluation_id")
    receipt_sha256 = sha256_file(receipt_path)
    if manifest.get("input_receipt_sha256") != receipt_sha256:
        raise ValueError("holdout manifest 活动输入收据散列不匹配")
    registration = get_active_head_receipt(
        registry_path, "holdout", evaluation_id,
    )
    normalized_receipt_path = receipt_path.relative_to(root).as_posix()
    if (
        registration.market_id != market_id
        or registration.head_generation != manifest.get("input_head_generation")
        or registration.receipt_artifact_path != normalized_receipt_path
        or registration.receipt_artifact_sha256 != receipt_sha256
    ):
        raise ValueError("holdout 活动输入收据与治理登记不一致")
    registered_inputs = attest_trade_input_receipt(
        source_data_root, receipt_path, require_current_head=False,
    )
    if (
        registered_inputs.head_generation != manifest.get("input_head_generation")
        or registered_inputs.attempt_ids != attempt_ids
        or registered_inputs.artifact_ids != artifact_ids
        or registered_inputs.normalization_versions != normalizations
    ):
        raise ValueError("holdout panel 血缘不能由控制面注册表重建")
    raw_vintage = _object(result.get("vintage"), "result.vintage")
    vintage_start = parse_time(raw_vintage.get("start_time"), "vintage.start_time")
    vintage_end = parse_time(raw_vintage.get("end_time"), "vintage.end_time")
    with TemporaryDirectory(prefix="guvolu-holdout-attest-") as temporary:
        rebuilt = build_panel_snapshot(
            registered_inputs,
            Path(temporary),
            _text(config.get("bar_interval"), "bar_interval"),
            parse_time(config.get("from_time"), "from_time"),
            vintage_end,
            _integer(config.get("notional_scale"), "notional_scale"),
        )
    if rebuilt.panel_sha256 != sha256_file(panel_path):
        raise ValueError("holdout panel 不能由注册输入和版本化查询重建")
    bars = load_panel_bars(panel_path)
    feature_config = _object(config.get("features"), "features")
    raw_lookbacks = feature_config.get("lookbacks")
    if not isinstance(raw_lookbacks, list):
        raise ValueError("features.lookbacks 必须为数组")
    features = compute_features(
        bars,
        tuple(_integer(value, "lookback") for value in raw_lookbacks),
        _integer(feature_config.get("volume_lookback"), "volume_lookback"),
        _integer(
            feature_config.get("maximum_structural_gap_bars_assumption"),
            "maximum_structural_gap_bars_assumption",
        ),
    )
    indices = [
        index for index in range(1, len(bars))
        if bars[index - 1].decision_time >= vintage_start
        and bars[index].decision_time <= vintage_end
    ]
    if not indices or indices != list(range(indices[0], indices[-1] + 1)):
        raise ValueError("holdout 绑定面板没有连续可评分区间")
    start, end = indices[0], indices[-1] + 1
    expected_schedule = [
        value.isoformat() for value in _score_decision_times(bars, start, end)
    ]
    if schedule.get("decision_times") != expected_schedule:
        raise ValueError("holdout score schedule 不能由绑定面板重建")
    governance = _object(config.get("data_governance"), "data_governance")
    policy = _object(governance.get("holdout_policy"), "holdout_policy")
    if end - start < _integer(policy.get("minimum_bars"), "minimum_bars"):
        raise ValueError("holdout 绑定面板低于预登记评分柱门槛")
    interval = _text(config.get("bar_interval"), "bar_interval")
    interval_seconds = _INTERVAL_SECONDS.get(interval)
    if interval_seconds is None:
        raise ValueError("holdout 配置的研究节拍不受支持")
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
    frozen_targets: Mapping[datetime, Mapping[str, float]] | None = None
    if policy.get("require_frozen_forward_predictions") is True:
        from guvolu.research.frozen_forward import load_verified_prediction_targets

        registry_path = _project_path(
            root,
            Path(_text(governance.get("registry"), "data_governance.registry")),
            "governance registry",
        )
        frozen_targets = load_verified_prediction_targets(
            root,
            _text(result.get("frozen_forward_plan_id"), "frozen_forward_plan_id"),
            registry_path=registry_path,
        )
    raw_results: list[dict[str, object]] = []
    p_values: dict[str, float] = {}
    for candidate in candidates:
        if frozen_targets is None:
            targets = generate_targets(candidate, bars, features, periods_per_year)
        else:
            missing = [
                bars[index - 1].decision_time
                for index in range(start, end)
                if candidate.candidate_id not in frozen_targets.get(
                    bars[index - 1].decision_time, {},
                )
            ]
            if missing:
                raise ValueError("holdout 冻结前向目标未覆盖绑定评分区间")
            mutable_targets = [0.0] * len(bars)
            for index in range(start, end):
                target_time = bars[index - 1].decision_time
                mutable_targets[index - 1] = frozen_targets[target_time][
                    candidate.candidate_id
                ]
            targets = tuple(mutable_targets)
        metrics = evaluate_targets(
            bars,
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
    candidate_results: list[dict[str, object]] = []
    passed_families: list[str] = []
    for raw_result in raw_results:
        metric_record = _object(raw_result.get("metrics"), "metrics")
        candidate_id = _text(raw_result.get("candidate_id"), "candidate_id")
        reasons: list[str] = []
        if _number(metric_record.get("net_return"), "net_return") <= 0:
            reasons.append("non_positive_holdout_net_return")
        if _number(metric_record.get("sharpe"), "sharpe") < _number(
            policy.get("minimum_sharpe"), "minimum_sharpe",
        ):
            reasons.append("holdout_sharpe_failed")
        if _number(
            metric_record.get("maximum_drawdown"), "maximum_drawdown",
        ) > _number(policy.get("maximum_drawdown"), "maximum_drawdown"):
            reasons.append("holdout_drawdown_failed")
        if q_values[candidate_id] > _number(
            policy.get("maximum_fdr_q"), "maximum_fdr_q",
        ):
            reasons.append("holdout_fdr_failed")
        passed = not reasons
        if passed:
            passed_families.append(_text(raw_result.get("family"), "family"))
        candidate_results.append({
            **raw_result,
            "fdr_q": q_values[candidate_id],
            "passed": passed,
            "rejection_reasons": reasons,
        })
    expected_passed = sorted(passed_families)
    expected_verdict = (
        "passed" if len(expected_passed) == len(candidates) else "failed"
    )
    if canonical_json(result.get("candidate_results")) != canonical_json(
        candidate_results
    ):
        raise ValueError("holdout candidate metrics 不能由冻结证据现场重建")
    if result.get("passed_families") != expected_passed:
        raise ValueError("holdout passed_families 不能由重算指标推导")
    if result.get("verdict") != expected_verdict:
        raise ValueError("holdout verdict 不能由重算指标推导")


def run_holdout_validation(
    root: Path,
    config_path: Path,
    source_summary_path: Path,
    vintage_id: str,
    output_root: Path | None = None,
) -> HoldoutRunResult:
    """先原子消费 vintage，再只评估冻结的部署候选，绝不重新选择。"""
    repository = root.resolve()
    requested_config_file = _project_path(repository, config_path, "holdout config")
    summary_file = _project_path(repository, source_summary_path, "source summary")
    source_manifest, source_manifest_path = verified_source_manifest(
        repository,
        summary_file,
    )
    source_summary = _load(summary_file)
    source_artifacts = _object(
        source_manifest.get("artifacts"), "source manifest artifacts",
    )
    source_config_record = _object(
        source_artifacts.get("config"), "source config artifact",
    )
    source_lineage_record = _object(
        source_artifacts.get("config_lineage"), "source config lineage artifact",
    )
    config_file = _project_path(
        repository,
        Path(_text(source_config_record.get("path"), "source config path")),
        "source config artifact",
    )
    config_lineage_file = _project_path(
        repository,
        Path(_text(source_lineage_record.get("path"), "source lineage path")),
        "source config lineage artifact",
    )
    (
        config,
        config_hash,
        _lineage_root_hash,
        _lineage_depth,
        config_source_paths,
        _config_artifact_paths,
    ) = attest_config_lineage_snapshot(
        repository, config_lineage_file, config_file,
    )
    if sha256_file(requested_config_file) != config_hash:
        raise ValueError("holdout 调用配置与来源研究配置快照不一致")
    if source_summary.get("config_hash") != config_hash:
        raise ValueError("holdout 配置与来源研究冻结配置不一致")
    source_data_root_record = source_summary.get("source_data_root")
    source_data_root = resolve_data_root_locator(
        repository, source_data_root_record,
    )
    candidates, candidate_registry_path = load_frozen_candidates(
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
    inputs = capture_trade_input_receipt(
        source_data_root,
        market_id,
        repository / "data" / "research" / "input-receipts",
    )
    if inputs.receipt_path is None or inputs.receipt_sha256 is None:
        raise RuntimeError("holdout 活动输入收据未生成")
    if inputs.maximum_event_time < vintage.end_time:
        raise ValueError("封存段尚未完整到达，不能提前消费")
    identity = code_identity(repository, config_source_paths)
    if not identity.decision_grade:
        raise ValueError("holdout 只允许 clean commit 的决策级代码")
    source_code_identity = _object(
        source_summary.get("code_identity"),
        "source_summary.code_identity",
    )
    if source_code_identity.get("tree_digest") != identity.tree_digest:
        raise ValueError("holdout 必须使用来源运行冻结的同一研究代码树身份")
    candidate_set_identity = frozen_candidate_set_identity(
        source_manifest_path,
        summary_file,
        candidate_registry_path,
        candidates,
    )
    candidate_set_hash = stable_identifier("candidate-set", candidate_set_identity)
    raw_policy = governance.get("holdout_policy")
    require_forward_predictions = (
        isinstance(raw_policy, Mapping)
        and raw_policy.get("require_frozen_forward_predictions") is True
    )
    frozen_targets: Mapping[datetime, Mapping[str, float]] | None = None
    forward_plan_id: str | None = None
    forward_prediction_count = 0
    forward_prediction_row_set_hash: str | None = None
    if require_forward_predictions:
        from guvolu.research.frozen_forward import (
            attest_frozen_forward_batch,
        )

        plan = get_frozen_forward_plan_for_vintage(registry_path, vintage_id)
        if plan is None:
            raise ValueError("holdout 要求预冻结前向计划")
        forward_plan_id = plan.plan_id
        if plan.source_manifest_sha256 != sha256_file(source_manifest_path):
            raise ValueError("前向计划与 holdout 来源 manifest 不一致")
        if plan.candidate_set_hash != candidate_set_hash:
            raise ValueError("前向计划与 holdout 候选集合不一致")
        if plan.config_hash != config_hash or plan.code_tree_digest != identity.tree_digest:
            raise ValueError("前向计划与 holdout 配置或代码树不一致")
        batch = attest_frozen_forward_batch(
            repository, plan.plan_id, registry_path=registry_path,
        )
        verification = batch.verification
        if verification.prediction_count == 0:
            raise ValueError("holdout 没有预先记录的冻结前向预测")
        forward_prediction_count = verification.prediction_count
        frozen_targets = batch.targets
        forward_prediction_row_set_hash = batch.row_set_hash
    evaluation_identity = {
        "holdout_method_version": HOLDOUT_METHOD_VERSION,
        "governance_method_version": GOVERNANCE_METHOD_VERSION,
        "vintage_id": vintage_id,
        "candidate_set_hash": candidate_set_hash,
        "config_hash": config_hash,
        "code_tree_digest": identity.tree_digest,
        "input_head_generation": inputs.head_generation,
        "input_attempt_ids": inputs.attempt_ids,
        "input_artifact_ids": inputs.artifact_ids,
        "normalization_versions": inputs.normalization_versions,
        "input_receipt_sha256": inputs.receipt_sha256,
    }
    evaluation_id = stable_identifier("holdout-evaluation", evaluation_identity)
    receipt_registration = register_active_head_receipt(
        registry_path,
        "holdout",
        evaluation_id,
        market_id,
        inputs.head_generation,
        inputs.receipt_path.resolve().relative_to(repository).as_posix(),
        inputs.receipt_sha256,
        repository_root=repository,
        data_root=source_data_root,
    )
    start_holdout_evaluation_attempt(
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
    update_holdout_evaluation_attempt(
        registry_path, evaluation_id, "building_panel",
    )
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
        if frozen_targets is None:
            targets = generate_targets(candidate, panel.bars, features, periods_per_year)
        else:
            missing = [
                panel.bars[index - 1].decision_time
                for index in range(start, end)
                if candidate.candidate_id not in frozen_targets.get(
                    panel.bars[index - 1].decision_time, {},
                )
            ]
            if missing:
                raise ValueError(
                    "holdout 冻结前向预测覆盖不完整: "
                    f"{candidate.candidate_id} missing={len(missing)}"
                )
            mutable_targets = [0.0] * len(panel.bars)
            for index in range(start, end):
                target_time = panel.bars[index - 1].decision_time
                mutable_targets[index - 1] = frozen_targets[target_time][
                    candidate.candidate_id
                ]
            targets = tuple(mutable_targets)
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
    update_holdout_evaluation_attempt(
        registry_path, evaluation_id, "scored_candidates",
    )
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
    score_decision_times = _score_decision_times(panel.bars, start, end)
    score_schedule = [value.isoformat() for value in score_decision_times]
    schedule_path = run_directory / "score-schedule.json"
    atomic_write_text(schedule_path, canonical_json({
        "schema_version": HOLDOUT_MANIFEST_SCHEMA_VERSION,
        "holdout_method_version": HOLDOUT_METHOD_VERSION,
        "evaluation_id": evaluation_id,
        "decision_times": score_schedule,
    }) + "\n")
    schedule_sha256 = sha256_file(schedule_path)
    result_payload = {
        "schema_version": HOLDOUT_MANIFEST_SCHEMA_VERSION,
        "holdout_method_version": HOLDOUT_METHOD_VERSION,
        "evaluation_id": evaluation_id,
        "vintage": _vintage_payload(vintage),
        "candidate_set_hash": candidate_set_hash,
        "target_source": (
            "recorded_frozen_forward" if frozen_targets is not None
            else "end_of_vintage_recompute"
        ),
        "frozen_forward_plan_id": forward_plan_id,
        "frozen_forward_prediction_count": forward_prediction_count,
        "frozen_forward_row_set_hash": forward_prediction_row_set_hash,
        "source_summary_sha256": sha256_file(summary_file),
        "config_hash": sha256_file(config_file),
        "code_identity": asdict(identity),
        "source_data_root": source_data_root_record,
        "panel_sha256": panel.panel_sha256,
        "score_schedule_sha256": schedule_sha256,
        "score_start": score_schedule[0],
        "score_end": score_schedule[-1],
        "score_bars": end - start,
        "policy": dict(policy),
        "candidate_results": candidate_results,
        "passed_families": sorted(passed_families),
        "verdict": verdict,
        "evaluated_at": datetime.now(UTC).isoformat(),
    }
    result_path = run_directory / "result.json"
    atomic_write_text(result_path, canonical_json(result_payload) + "\n")
    manifest = {
        "schema_version": HOLDOUT_MANIFEST_SCHEMA_VERSION,
        "holdout_method_version": HOLDOUT_METHOD_VERSION,
        "evaluation_id": evaluation_id,
        "vintage_id": vintage_id,
        "candidate_set_hash": candidate_set_hash,
        "candidate_set_identity": candidate_set_identity,
        "evaluation_identity": evaluation_identity,
        "verdict": verdict,
        "input_head_generation": inputs.head_generation,
        "input_attempt_ids": list(inputs.attempt_ids),
        "input_artifact_ids": list(inputs.artifact_ids),
        "normalization_versions": list(inputs.normalization_versions),
        "input_receipt_sha256": receipt_registration.receipt_artifact_sha256,
        "source_data_root": source_data_root_record,
        "frozen_forward_row_set_hash": forward_prediction_row_set_hash,
        "artifacts": {
            "source_manifest": {
                **artifact_record(source_manifest_path, "research_manifest"),
                "path": source_manifest_path.resolve().relative_to(
                    repository
                ).as_posix(),
            },
            "source_summary": {
                **artifact_record(summary_file, "research_summary"),
                "path": summary_file.resolve().relative_to(repository).as_posix(),
            },
            "candidate_registry": {
                **artifact_record(candidate_registry_path, "candidate_registry"),
                "path": candidate_registry_path.resolve().relative_to(
                    repository
                ).as_posix(),
            },
            "config": {
                **artifact_record(config_file, "holdout_config"),
                "path": config_file.resolve().relative_to(repository).as_posix(),
            },
            "config_lineage": {
                **artifact_record(config_lineage_file, "holdout_config_lineage"),
                "path": config_lineage_file.resolve().relative_to(
                    repository
                ).as_posix(),
            },
            "input_receipt": {
                **artifact_record(inputs.receipt_path, "active_trade_head_receipt"),
                "path": receipt_registration.receipt_artifact_path,
            },
            "panel": {
                **artifact_record(panel.panel_path, "holdout_panel"),
                "path": panel.panel_path.resolve().relative_to(repository).as_posix(),
            },
            "score_schedule": {
                **artifact_record(schedule_path, "holdout_score_schedule"),
                "path": schedule_path.resolve().relative_to(repository).as_posix(),
            },
            "result": {
                **artifact_record(result_path, "holdout_result"),
                "path": result_path.resolve().relative_to(repository).as_posix(),
            },
        },
    }
    manifest_path = run_directory / "manifest.json"
    atomic_write_text(manifest_path, canonical_json(manifest) + "\n")
    manifest_sha256 = sha256_file(manifest_path)
    final_verdict = canonical_json({
            "evaluation_id": evaluation_id,
            "verdict": verdict,
            "candidate_ids": [candidate.candidate_id for candidate in candidates],
            "passed_families": sorted(passed_families),
            "result_sha256": sha256_file(result_path),
            "manifest_sha256": manifest_sha256,
        })
    finalize_holdout_evaluation(
        registry_path,
        vintage_id,
        evaluation_id,
        final_verdict,
        manifest_path.resolve().relative_to(repository).as_posix(),
        manifest_sha256,
        repository_root=repository,
    )
    return HoldoutRunResult(
        evaluation_id=evaluation_id,
        run_directory=run_directory,
        manifest_path=manifest_path,
        result_path=result_path,
        manifest_sha256=manifest_sha256,
        verdict=verdict,
    )
