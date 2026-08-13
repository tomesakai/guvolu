"""预冻结候选与权重在 sealed vintage 内产生不可变前向预测。"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Mapping

from guvolu.data.durable_io import atomic_write_text
from guvolu.research.contracts import (
    FROZEN_FORWARD_METHOD_VERSION,
    FROZEN_FORWARD_SCHEMA_VERSION,
    QualityVector,
)
from guvolu.research.features import compute_features
from guvolu.research.governance import (
    GOVERNANCE_METHOD_VERSION,
    FrozenForwardPlan,
    get_frozen_forward_plan,
    get_frozen_forward_plan_for_vintage,
    get_holdout_vintage,
    list_frozen_forward_predictions,
    register_frozen_forward_plan,
    register_frozen_forward_prediction,
)
from guvolu.research.holdout import (
    frozen_candidate_set_hash,
    load_frozen_candidates,
    verified_source_manifest,
)
from guvolu.research.panel import build_panel_snapshot, freeze_trade_inputs, parse_time
from guvolu.research.provenance import (
    canonical_json,
    code_identity,
    sha256_file,
    stable_identifier,
)
from guvolu.research.quality import gate_feature_snapshot, panel_quality, quality_payload
from guvolu.strategy.baselines import generate_targets
from guvolu.strategy.contracts import CandidateSpec

SECONDS_PER_YEAR = 365.0 * 24.0 * 60.0 * 60.0
_INTERVAL_SECONDS = {
    "5min": 300.0,
    "15min": 900.0,
    "1hour": 3600.0,
    "4hour": 14_400.0,
}


@dataclass(frozen=True)
class FrozenPlanResult:
    """一个已持久化并登记的冻结前向计划。"""

    plan_id: str
    plan_path: Path
    plan_sha256: str


@dataclass(frozen=True)
class FrozenPredictionResult:
    """一个已持久化并登记的冻结前向预测。"""

    prediction_id: str
    prediction_path: Path
    prediction_sha256: str
    decision_time: datetime
    aggregate_target: float


@dataclass(frozen=True)
class FrozenForwardVerification:
    """冻结计划及其全部预测制品的复核结果。"""

    plan_id: str
    plan_sha256: str
    prediction_count: int


def _object(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} 必须为对象")
    return {str(key): item for key, item in value.items()}


def _text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} 必须为非空文本")
    return value.strip()


def _number(value: object, name: str) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValueError(f"{name} 必须为数值")
    return float(value)


def _integer(value: object, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{name} 必须为正整数")
    return value


def _load(path: Path) -> Mapping[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    return _object(value, path.as_posix())


def _project_path(root: Path, value: Path, name: str) -> Path:
    path = value.resolve() if value.is_absolute() else (root / value).resolve()
    try:
        path.relative_to(root)
    except ValueError as error:
        raise ValueError(f"{name} 必须位于项目目录内") from error
    return path


def _relative(root: Path, path: Path) -> str:
    return path.resolve().relative_to(root.resolve()).as_posix()


def _governance_path(root: Path, config: Mapping[str, object]) -> Path:
    governance = _object(config.get("data_governance"), "data_governance")
    return _project_path(
        root,
        Path(_text(governance.get("registry"), "data_governance.registry")),
        "governance registry",
    )


def _source_weights(
    summary: Mapping[str, object],
    candidates: tuple[CandidateSpec, ...],
) -> tuple[dict[str, float], float]:
    """从研究回放合同冻结资金权重，不采用可能因新鲜度清零的运行权重。"""
    candidate_by_family = {candidate.family: candidate for candidate in candidates}
    contract = _object(summary.get("research_target_contract"), "research_target_contract")
    raw_families = contract.get("families")
    if not isinstance(raw_families, list):
        raise ValueError("research_target_contract.families 必须为数组")
    weights: dict[str, float] = {}
    for raw in raw_families:
        record = _object(raw, "research_target_contract.family")
        family = _text(record.get("family"), "family")
        if family not in candidate_by_family:
            continue
        if record.get("eligible") is not True:
            raise ValueError("冻结候选在研究目标合同中不是 eligible")
        if record.get("deployment_candidate_id") != candidate_by_family[family].candidate_id:
            raise ValueError("研究目标合同与冻结部署候选不一致")
        weight = _number(record.get("allocation_weight"), "allocation_weight")
        if weight < -1e-12:
            raise ValueError("冻结前向计划不支持负资金权重")
        weights[family] = max(weight, 0.0)
    if set(weights) != set(candidate_by_family):
        raise ValueError("研究目标合同没有覆盖全部冻结候选")
    position = _object(summary.get("research_position"), "research_position")
    reserve = _number(position.get("reserve"), "research_position.reserve")
    if reserve < -1e-12 or sum(weights.values()) + reserve > 1.0 + 1e-9:
        raise ValueError("冻结权重与风险余量合同不成立")
    return weights, max(reserve, 0.0)


def freeze_forward_plan(
    root: Path,
    config_path: Path,
    source_summary_path: Path,
    vintage_id: str,
    *,
    frozen_at: datetime | None = None,
) -> FrozenPlanResult:
    """在 vintage 开始前冻结来源、公式、参数和资金权重。"""
    repository = root.resolve()
    config_file = _project_path(repository, config_path, "config")
    summary_file = _project_path(repository, source_summary_path, "source summary")
    config = _load(config_file)
    registry_path = _governance_path(repository, config)
    existing = get_frozen_forward_plan_for_vintage(registry_path, vintage_id)
    if existing is not None:
        path = _project_path(repository, Path(existing.plan_artifact_path), "plan artifact")
        if sha256_file(path) != existing.plan_artifact_sha256:
            raise ValueError("已登记冻结前向计划制品散列不匹配")
        return FrozenPlanResult(existing.plan_id, path, existing.plan_artifact_sha256)
    source_manifest, source_manifest_path = verified_source_manifest(
        repository, summary_file,
    )
    summary = _load(summary_file)
    candidates, candidate_registry_path = load_frozen_candidates(
        repository, summary, source_manifest,
    )
    identity = code_identity(repository, (config_file,))
    if not identity.decision_grade:
        raise ValueError("冻结前向计划只允许 clean commit 的决策级代码")
    source_identity = _object(summary.get("code_identity"), "code_identity")
    if source_identity.get("tree_digest") != identity.tree_digest:
        raise ValueError("冻结前向计划必须使用来源运行的同一代码树")
    vintage = get_holdout_vintage(registry_path, vintage_id)
    if vintage.market_id != _text(config.get("market_id"), "market_id"):
        raise ValueError("vintage 与配置市场不一致")
    if vintage.status != "sealed":
        raise ValueError("冻结前向计划只能绑定未消费 vintage")
    timestamp = (frozen_at or datetime.now(UTC)).astimezone(UTC)
    if timestamp > vintage.start_time:
        raise ValueError("冻结前向计划必须在 vintage 开始前创建")
    candidate_set_hash = frozen_candidate_set_hash(
        source_manifest_path, summary_file, candidate_registry_path, candidates,
    )
    source_manifest_hash = sha256_file(source_manifest_path)
    config_hash = sha256_file(config_file)
    weights, reserve = _source_weights(summary, candidates)
    plan_id = stable_identifier("frozen-forward-plan", {
        "governance_method_version": GOVERNANCE_METHOD_VERSION,
        "vintage_id": vintage_id,
        "source_manifest_sha256": source_manifest_hash,
        "candidate_set_hash": candidate_set_hash,
        "config_hash": config_hash,
        "code_tree_digest": identity.tree_digest,
    })
    plan_path = (
        repository / "reports" / "strategy-research" / "frozen-forward"
        / vintage_id / plan_id / "plan.json"
    )
    payload = {
        "schema_version": FROZEN_FORWARD_SCHEMA_VERSION,
        "method_version": FROZEN_FORWARD_METHOD_VERSION,
        "governance_method_version": GOVERNANCE_METHOD_VERSION,
        "scope": "FROZEN_FORWARD",
        "plan_id": plan_id,
        "vintage": {
            "vintage_id": vintage.vintage_id,
            "market_id": vintage.market_id,
            "start_time": vintage.start_time.isoformat(),
            "end_time": vintage.end_time.isoformat(),
        },
        "frozen_at": timestamp.isoformat(),
        "source": {
            "run_id": summary.get("run_id"),
            "research_identity": summary.get("research_identity"),
            "manifest_path": _relative(repository, source_manifest_path),
            "manifest_sha256": source_manifest_hash,
            "summary_path": _relative(repository, summary_file),
            "summary_sha256": sha256_file(summary_file),
            "candidate_registry_sha256": sha256_file(candidate_registry_path),
        },
        "config_path": _relative(repository, config_file),
        "config_hash": config_hash,
        "code_identity": asdict(identity),
        "code_tree_digest": identity.tree_digest,
        "candidate_set_hash": candidate_set_hash,
        "candidates": [asdict(candidate) for candidate in candidates],
        "allocation": {"weights": weights, "reserve": reserve},
    }
    atomic_write_text(plan_path, canonical_json(payload) + "\n")
    plan_hash = sha256_file(plan_path)
    registered = register_frozen_forward_plan(
        registry_path,
        vintage_id,
        source_manifest_hash,
        candidate_set_hash,
        config_hash,
        identity.tree_digest,
        _relative(repository, plan_path),
        plan_hash,
        repository_root=repository,
        frozen_at=timestamp,
    )
    if registered.plan_id != plan_id:
        raise RuntimeError("治理注册表返回了不同的冻结计划身份")
    return FrozenPlanResult(plan_id, plan_path, plan_hash)


def _candidate_from_payload(raw: object) -> CandidateSpec:
    record = _object(raw, "plan.candidate")
    parameters = _object(record.get("parameters"), "candidate.parameters")
    numeric: dict[str, int | float] = {}
    for key, value in parameters.items():
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise ValueError(f"冻结候选参数必须为数值: {key}")
        numeric[key] = value
    return CandidateSpec(
        candidate_id=_text(record.get("candidate_id"), "candidate_id"),
        family=_text(record.get("family"), "family"),
        mode=_text(record.get("mode"), "mode"),
        parameters=numeric,
        complexity=_integer(record.get("complexity"), "complexity"),
        expression_id=_text(record.get("expression_id"), "expression_id"),
    )


def _maturity_gate(quality: QualityVector, mature: bool) -> QualityVector:
    if mature:
        return quality
    return QualityVector(
        integrity=quality.integrity,
        freshness=quality.freshness,
        clock=quality.clock,
        coverage=quality.coverage,
        pit=quality.pit,
        lineage=False,
        reasons=tuple(sorted({*quality.reasons, "latest_panel_feature_not_mature"})),
    )


def run_frozen_forward_prediction(
    root: Path,
    plan_id: str,
    *,
    registry_path: Path | None = None,
    recorded_at: datetime | None = None,
) -> FrozenPredictionResult:
    """用固定候选和固定权重为 sealed vintage 的最新决策时点生成预测。"""
    repository = root.resolve()
    now = (recorded_at or datetime.now(UTC)).astimezone(UTC)
    selected_registry = _project_path(
        repository,
        registry_path or Path("data/research/governance.sqlite3"),
        "governance registry",
    )
    plan = get_frozen_forward_plan(selected_registry, plan_id)
    plan_path = _project_path(repository, Path(plan.plan_artifact_path), "plan artifact")
    if sha256_file(plan_path) != plan.plan_artifact_sha256:
        raise ValueError("冻结前向计划制品散列不匹配")
    payload = _load(plan_path)
    if payload.get("plan_id") != plan_id or payload.get("scope") != "FROZEN_FORWARD":
        raise ValueError("冻结前向计划合同不一致")
    config_path = _project_path(
        repository, Path(_text(payload.get("config_path"), "config_path")), "config",
    )
    if sha256_file(config_path) != plan.config_hash:
        raise ValueError("冻结前向配置散列不匹配")
    config = _load(config_path)
    registry_path = _governance_path(repository, config)
    if registry_path != selected_registry:
        raise ValueError("计划配置指向的治理注册表与调用参数不一致")
    identity = code_identity(repository, (config_path,))
    if not identity.decision_grade or identity.tree_digest != plan.code_tree_digest:
        raise ValueError("预测执行器必须是冻结计划的 clean code tree")
    vintage = get_holdout_vintage(registry_path, plan.vintage_id)
    if vintage.status != "sealed":
        raise ValueError("已消费 vintage 不得追加前向预测")
    market_id = _text(config.get("market_id"), "market_id")
    inputs = freeze_trade_inputs(repository / "data", market_id)
    if inputs.maximum_event_time < vintage.start_time:
        raise ValueError("vintage 尚未开始，没有可生成的前向决策")
    interval = _text(config.get("bar_interval"), "bar_interval")
    interval_seconds = _INTERVAL_SECONDS.get(interval)
    if interval_seconds is None:
        raise ValueError("冻结前向不支持该研究节拍")
    panel = build_panel_snapshot(
        inputs,
        repository / "data" / "research" / "frozen-forward" / plan_id,
        interval,
        parse_time(config.get("from_time"), "from_time"),
        min(inputs.maximum_event_time, vintage.end_time),
        _integer(config.get("notional_scale"), "notional_scale"),
    )
    decision_time = panel.bars[-1].decision_time
    if not vintage.start_time <= decision_time < vintage.end_time:
        raise ValueError("最新完整研究柱不在计划绑定的 vintage 内")
    existing = {
        item.decision_time: item
        for item in list_frozen_forward_predictions(registry_path, plan_id)
    }.get(decision_time)
    if existing is not None:
        path = _project_path(
            repository, Path(existing.prediction_artifact_path), "prediction artifact",
        )
        if sha256_file(path) != existing.prediction_artifact_sha256:
            raise ValueError("已登记冻结预测制品散列不匹配")
        content = _load(path)
        return FrozenPredictionResult(
            existing.prediction_id,
            path,
            existing.prediction_artifact_sha256,
            existing.decision_time,
            _number(content.get("aggregate_target"), "aggregate_target"),
        )
    maximum_age = _integer(
        config.get("strategy_decision_max_age_seconds"),
        "strategy_decision_max_age_seconds",
    )
    lag = (now - decision_time).total_seconds()
    if lag < 0 or lag > maximum_age:
        raise ValueError("最新研究柱不在冻结预测登记时效窗口内")
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
    latest_feature = features[-1]
    state_lookback = _integer(feature_config.get("state_lookback"), "state_lookback")
    mature = (
        latest_feature.contiguous
        and latest_feature.volume_score is not None
        and latest_feature.trend_scores.get(state_lookback) is not None
    )
    validation = _object(config.get("validation"), "validation")
    quality = _maturity_gate(
        gate_feature_snapshot(
            panel_quality(
                panel,
                now,
                maximum_age,
                _integer(validation.get("minimum_oos_bars"), "minimum_oos_bars"),
            ),
            decision_time,
            now,
            maximum_age,
        ),
        mature,
    )
    candidates_raw = payload.get("candidates")
    if not isinstance(candidates_raw, list):
        raise ValueError("冻结前向计划缺少 candidates")
    candidates = tuple(_candidate_from_payload(raw) for raw in candidates_raw)
    allocation = _object(payload.get("allocation"), "allocation")
    weights_raw = _object(allocation.get("weights"), "allocation.weights")
    weights = {family: _number(value, f"weights.{family}") for family, value in weights_raw.items()}
    periods_per_year = SECONDS_PER_YEAR / interval_seconds
    families: list[dict[str, object]] = []
    aggregate = 0.0
    for candidate in candidates:
        target = generate_targets(candidate, panel.bars, features, periods_per_year)[-1]
        weight = weights.get(candidate.family)
        if weight is None:
            raise ValueError("冻结权重没有覆盖候选流派")
        effective_target = target if quality.eligible else 0.0
        contribution = weight * effective_target
        aggregate += contribution
        families.append({
            "family": candidate.family,
            "candidate_id": candidate.candidate_id,
            "family_target": effective_target,
            "frozen_allocation_weight": weight,
            "portfolio_target_contribution": contribution,
        })
    prediction_id = stable_identifier("frozen-forward-prediction", {
        "governance_method_version": GOVERNANCE_METHOD_VERSION,
        "plan_id": plan_id,
        "decision_time": decision_time.isoformat(),
    })
    prediction = {
        "schema_version": FROZEN_FORWARD_SCHEMA_VERSION,
        "method_version": FROZEN_FORWARD_METHOD_VERSION,
        "scope": "FROZEN_FORWARD",
        "prediction_id": prediction_id,
        "plan_id": plan_id,
        "vintage_id": vintage.vintage_id,
        "decision_time": decision_time.isoformat(),
        "input_head_generation": inputs.head_generation,
        "panel_sha256": panel.panel_sha256,
        "config_hash": plan.config_hash,
        "code_identity": {
            "git_hash": identity.git_hash,
            "tree_digest": identity.tree_digest,
            "decision_grade": identity.decision_grade,
        },
        "quality": quality_payload(quality),
        "families": sorted(families, key=lambda item: str(item["family"])),
        "reserve": _number(allocation.get("reserve"), "allocation.reserve"),
        "aggregate_target": aggregate,
        "unit": "risk_weighted_directional_target",
    }
    stamp = decision_time.strftime("%Y%m%dT%H%M%SZ")
    prediction_path = plan_path.parent / "predictions" / f"{stamp}.json"
    atomic_write_text(prediction_path, canonical_json(prediction) + "\n")
    prediction_hash = sha256_file(prediction_path)
    registered = register_frozen_forward_prediction(
        registry_path,
        plan_id,
        decision_time,
        inputs.head_generation,
        panel.panel_sha256,
        _relative(repository, prediction_path),
        prediction_hash,
        maximum_age,
        repository_root=repository,
        recorded_at=now,
    )
    return FrozenPredictionResult(
        registered.prediction_id,
        prediction_path,
        prediction_hash,
        decision_time,
        aggregate,
    )


def verify_frozen_forward(
    root: Path,
    plan_id: str,
    *,
    registry_path: Path | None = None,
) -> FrozenForwardVerification:
    """离线复核计划、固定权重、预测散列和组合目标恒等式。"""
    repository = root.resolve()
    registry = _project_path(
        repository,
        registry_path or Path("data/research/governance.sqlite3"),
        "governance registry",
    )
    plan = get_frozen_forward_plan(registry, plan_id)
    plan_path = _project_path(repository, Path(plan.plan_artifact_path), "plan artifact")
    if sha256_file(plan_path) != plan.plan_artifact_sha256:
        raise ValueError("冻结前向计划制品散列不匹配")
    payload = _load(plan_path)
    if payload.get("plan_id") != plan.plan_id:
        raise ValueError("冻结计划身份与注册表不一致")
    if payload.get("candidate_set_hash") != plan.candidate_set_hash:
        raise ValueError("冻结候选集合身份与注册表不一致")
    if payload.get("config_hash") != plan.config_hash:
        raise ValueError("冻结配置身份与注册表不一致")
    if payload.get("code_tree_digest") != plan.code_tree_digest:
        raise ValueError("冻结代码树身份与注册表不一致")
    code = _object(payload.get("code_identity"), "code_identity")
    if code.get("tree_digest") != plan.code_tree_digest:
        raise ValueError("冻结计划 code_identity 与注册表不一致")
    allocation = _object(payload.get("allocation"), "allocation")
    raw_weights = _object(allocation.get("weights"), "allocation.weights")
    weights = {
        family: _number(value, f"allocation.weights.{family}")
        for family, value in raw_weights.items()
    }
    predictions = list_frozen_forward_predictions(registry, plan_id)
    for prediction in predictions:
        path = _project_path(
            repository,
            Path(prediction.prediction_artifact_path),
            "prediction artifact",
        )
        if sha256_file(path) != prediction.prediction_artifact_sha256:
            raise ValueError("冻结前向预测制品散列不匹配")
        record = _load(path)
        expected = {
            "prediction_id": prediction.prediction_id,
            "plan_id": prediction.plan_id,
            "vintage_id": prediction.vintage_id,
            "decision_time": prediction.decision_time.isoformat(),
            "input_head_generation": prediction.input_head_generation,
            "panel_sha256": prediction.panel_sha256,
        }
        for field, value in expected.items():
            if record.get(field) != value:
                raise ValueError(f"冻结预测 {field} 与注册表不一致")
        if record.get("config_hash") != plan.config_hash:
            raise ValueError("冻结预测配置身份与计划不一致")
        prediction_code = _object(record.get("code_identity"), "prediction.code_identity")
        if prediction_code.get("tree_digest") != plan.code_tree_digest:
            raise ValueError("冻结预测代码树身份与计划不一致")
        quality = _object(record.get("quality"), "prediction.quality")
        eligible = quality.get("eligible")
        if not isinstance(eligible, bool):
            raise ValueError("冻结预测 quality.eligible 必须为布尔值")
        raw_families = record.get("families")
        if not isinstance(raw_families, list):
            raise ValueError("冻结预测 families 必须为数组")
        contribution_total = 0.0
        for raw in raw_families:
            family_record = _object(raw, "prediction.family")
            family = _text(family_record.get("family"), "family")
            weight = _number(
                family_record.get("frozen_allocation_weight"),
                "frozen_allocation_weight",
            )
            if family not in weights or abs(weight - weights[family]) > 1e-12:
                raise ValueError("冻结预测使用了计划外资金权重")
            target = _number(family_record.get("family_target"), "family_target")
            contribution = _number(
                family_record.get("portfolio_target_contribution"),
                "portfolio_target_contribution",
            )
            if abs(contribution - target * weight) > 1e-12:
                raise ValueError("冻结预测的流派贡献计算不一致")
            if not eligible and (abs(target) > 1e-12 or abs(contribution) > 1e-12):
                raise ValueError("冻结预测质量失败但存在非零目标")
            contribution_total += contribution
        aggregate = _number(record.get("aggregate_target"), "aggregate_target")
        if abs(aggregate - contribution_total) > 1e-12:
            raise ValueError("冻结预测的组合目标聚合不一致")
        if not eligible and abs(aggregate) > 1e-12:
            raise ValueError("冻结预测质量失败但组合目标非零")
    return FrozenForwardVerification(
        plan_id=plan.plan_id,
        plan_sha256=plan.plan_artifact_sha256,
        prediction_count=len(predictions),
    )


def load_verified_prediction_targets(
    root: Path,
    plan_id: str,
    *,
    registry_path: Path | None = None,
) -> Mapping[datetime, Mapping[str, float]]:
    """复核后按决策时间返回 candidate_id 到当时目标的不可变映射。"""
    repository = root.resolve()
    registry = _project_path(
        repository,
        registry_path or Path("data/research/governance.sqlite3"),
        "governance registry",
    )
    verify_frozen_forward(repository, plan_id, registry_path=registry)
    result: dict[datetime, dict[str, float]] = {}
    for prediction in list_frozen_forward_predictions(registry, plan_id):
        path = _project_path(
            repository,
            Path(prediction.prediction_artifact_path),
            "prediction artifact",
        )
        record = _load(path)
        raw_families = record.get("families")
        if not isinstance(raw_families, list):
            raise ValueError("冻结预测 families 必须为数组")
        targets: dict[str, float] = {}
        for raw in raw_families:
            family = _object(raw, "prediction.family")
            candidate_id = _text(family.get("candidate_id"), "candidate_id")
            if candidate_id in targets:
                raise ValueError("冻结预测包含重复 candidate_id")
            targets[candidate_id] = _number(family.get("family_target"), "family_target")
        result[prediction.decision_time] = targets
    return result
