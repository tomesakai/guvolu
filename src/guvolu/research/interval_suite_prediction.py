"""在共同决策栅格生成并复核跨节拍冻结预测。"""
from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Mapping, Sequence

from guvolu.data.durable_io import atomic_write_text
from guvolu.research import clock
from guvolu.research.contracts import (
    INTERVAL_SUITE_PREDICTION_METHOD_VERSION,
    INTERVAL_SUITE_PREDICTION_SCHEMA_VERSION,
    FrozenPanelInputs,
    QualityVector,
)
from guvolu.research.data_location import resolve_data_root_locator
from guvolu.research.features import compute_features
from guvolu.research.governance import (
    GOVERNANCE_METHOD_VERSION,
    IntervalSuiteForwardPlan,
    IntervalSuiteForwardPrediction,
    get_holdout_vintage,
    get_interval_suite_forward_plan,
    list_interval_suite_forward_predictions,
    register_interval_suite_forward_prediction,
)
from guvolu.research.interval_suite_prediction_identity import (
    interval_suite_forward_prediction_id,
    interval_suite_member_panel_set_hash,
)
from guvolu.research.panel import (
    attest_trade_input_receipt,
    build_panel_snapshot,
    capture_trade_input_receipt,
    parse_time,
)
from guvolu.research.provenance import (
    canonical_json,
    code_identity,
    sha256_file,
    sha256_text,
)
from guvolu.research.quality import (
    gate_feature_snapshot,
    panel_quality,
    quality_payload,
)
from guvolu.strategy.baselines import generate_targets
from guvolu.strategy.contracts import CandidateSpec

SECONDS_PER_YEAR = 365.0 * 24.0 * 60.0 * 60.0
_INTERVAL_SECONDS = {
    "5min": 300,
    "15min": 900,
    "1hour": 3600,
    "4hour": 14_400,
}


@dataclass(frozen=True)
class IntervalSuitePredictionResult:
    """一个已持久化并登记的共同栅格预测。"""

    prediction_id: str
    prediction_path: Path
    prediction_sha256: str
    decision_time: datetime
    aggregate_target: float


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


def _integer(value: object, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{name} 必须为正整数")
    return value


def _number(value: object, name: str) -> float:
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(float(value))
    ):
        raise ValueError(f"{name} 必须为有限数值")
    return float(value)


def _load(path: Path, name: str) -> Mapping[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ValueError(f"{name} 不是可读 JSON") from error
    return _mapping(value, name)


def _project_path(root: Path, value: Path, name: str) -> Path:
    path = value.resolve() if value.is_absolute() else (root / value).resolve()
    try:
        path.relative_to(root)
    except ValueError as error:
        raise ValueError(f"{name} 必须位于项目目录内") from error
    return path


def _relative(root: Path, path: Path) -> str:
    return path.resolve().relative_to(root.resolve()).as_posix()


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _candidate(raw: object) -> CandidateSpec:
    value = _mapping(raw, "suite sleeve candidate")
    parameters = _mapping(value.get("parameters"), "candidate.parameters")
    numeric: dict[str, int | float] = {}
    for name, parameter in parameters.items():
        if not isinstance(parameter, (int, float)) or isinstance(parameter, bool):
            raise ValueError(f"候选参数必须为数值: {name}")
        numeric[name] = parameter
    return CandidateSpec(
        candidate_id=_text(value.get("candidate_id"), "candidate_id"),
        family=_text(value.get("family"), "family"),
        mode=_text(value.get("mode"), "mode"),
        parameters=numeric,
        complexity=_integer(value.get("complexity"), "complexity"),
        expression_id=_text(value.get("expression_id"), "expression_id"),
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
        reasons=tuple(sorted({
            *quality.reasons, "latest_panel_feature_not_mature",
        })),
    )


def _decision_time(
    maximum_event_time: datetime,
    vintage_end: datetime,
    interval_seconds: int,
    offset_seconds: int,
) -> datetime:
    """取得活动输入已覆盖且严格早于 vintage end 的最新共同栅格。"""
    upper = min(
        _utc(maximum_event_time),
        _utc(vintage_end) - timedelta(microseconds=1),
    )
    epoch = int(upper.timestamp())
    aligned = epoch - ((epoch - offset_seconds) % interval_seconds)
    return datetime.fromtimestamp(aligned, tz=UTC)


def _member_panel_and_targets(
    root: Path,
    plan_payload: Mapping[str, object],
    member: Mapping[str, object],
    sleeves: Sequence[Mapping[str, object]],
    inputs: FrozenPanelInputs,
    decision_time: datetime,
    quality_reference_time: datetime,
    output_directory: Path,
    panel_path_override: str | None = None,
) -> tuple[Mapping[str, object], list[Mapping[str, object]]]:
    """以冻结成员配置构建一个面板并计算该成员 sleeve 原始目标。"""
    member_id = _text(member.get("member_id"), "member_id")
    interval = _text(member.get("bar_interval"), "bar_interval")
    config = _mapping(member.get("config_contract"), "config_contract")
    if config.get("bar_interval") != interval:
        raise ValueError("冻结成员配置节拍不一致")
    panel = build_panel_snapshot(
        inputs,
        output_directory,
        interval,
        parse_time(config.get("from_time"), "from_time"),
        decision_time,
        _integer(config.get("notional_scale"), "notional_scale"),
    )
    if panel.decision_time != decision_time:
        raise ValueError("成员面板未结束于共同决策栅格")
    feature_config = _mapping(config.get("features"), "features")
    lookbacks = tuple(
        _integer(value, "features.lookback")
        for value in _list(feature_config.get("lookbacks"), "features.lookbacks")
    )
    features = compute_features(
        panel.bars,
        lookbacks,
        _integer(feature_config.get("volume_lookback"), "volume_lookback"),
        _integer(
            feature_config.get("maximum_structural_gap_bars_assumption"),
            "maximum_structural_gap_bars_assumption",
        ),
    )
    state_lookback = _integer(
        feature_config.get("state_lookback"), "state_lookback",
    )
    latest_feature = features[-1]
    mature = (
        latest_feature.contiguous
        and latest_feature.volume_score is not None
        and latest_feature.trend_scores.get(state_lookback) is not None
    )
    maximum_age = _integer(
        config.get("strategy_decision_max_age_seconds"),
        "strategy_decision_max_age_seconds",
    )
    validation = _mapping(config.get("validation"), "validation")
    quality = _maturity_gate(
        gate_feature_snapshot(
            panel_quality(
                panel,
                quality_reference_time,
                maximum_age,
                _integer(validation.get("minimum_oos_bars"), "minimum_oos_bars"),
            ),
            decision_time,
            quality_reference_time,
            maximum_age,
        ),
        mature,
    )
    interval_seconds = _INTERVAL_SECONDS.get(interval)
    if interval_seconds is None:
        raise ValueError("冻结成员包含不受支持的研究节拍")
    periods_per_year = SECONDS_PER_YEAR / interval_seconds
    raw_targets: list[Mapping[str, object]] = []
    for sleeve in sleeves:
        candidate = _candidate(sleeve.get("candidate"))
        if candidate.family != sleeve.get("family"):
            raise ValueError("冻结 sleeve 候选流派不一致")
        raw_target = generate_targets(
            candidate, panel.bars, features, periods_per_year,
        )[-1]
        raw_targets.append({
            "sleeve_id": sleeve.get("sleeve_id"),
            "member_id": member_id,
            "bar_interval": interval,
            "family": sleeve.get("family"),
            "candidate_id": candidate.candidate_id,
            "weight": _number(sleeve.get("weight"), "sleeve.weight"),
            "raw_target": raw_target,
        })
    panel_record = {
        "member_id": member_id,
        "bar_interval": interval,
        "panel_path": (
            panel_path_override
            if panel_path_override is not None
            else _relative(root, panel.panel_path)
        ),
        "panel_sha256": panel.panel_sha256,
        "panel_bytes": panel.panel_path.stat().st_size,
        "decision_time": panel.decision_time.isoformat(),
        "latest_available_time": panel.latest_available_time.isoformat(),
        "input_head_generation": panel.head_generation,
        "attempt_ids": list(panel.attempt_ids),
        "artifact_ids": list(panel.artifact_ids),
        "normalization_versions": list(panel.normalization_versions),
        "quality": quality_payload(quality),
    }
    return panel_record, raw_targets


def _evaluate(
    root: Path,
    plan_payload: Mapping[str, object],
    inputs: FrozenPanelInputs,
    decision_time: datetime,
    quality_reference_time: datetime,
    output_root: Path,
    panel_paths: Mapping[str, str] | None = None,
) -> tuple[
    list[Mapping[str, object]],
    list[Mapping[str, object]],
    Mapping[str, object],
    Mapping[str, object],
]:
    raw_members = _list(plan_payload.get("members"), "plan.members")
    raw_sleeves = _list(plan_payload.get("sleeves"), "plan.sleeves")
    sleeves_by_member: dict[str, list[Mapping[str, object]]] = {}
    for raw_sleeve in raw_sleeves:
        sleeve = _mapping(raw_sleeve, "plan.sleeve")
        member_id = _text(sleeve.get("member_id"), "sleeve.member_id")
        sleeves_by_member.setdefault(member_id, []).append(sleeve)
    member_panels: list[Mapping[str, object]] = []
    raw_targets: list[Mapping[str, object]] = []
    for raw_member in raw_members:
        member = _mapping(raw_member, "plan.member")
        member_id = _text(member.get("member_id"), "member.member_id")
        member_sleeves = sleeves_by_member.get(member_id)
        if not member_sleeves:
            continue
        panel_record, member_targets = _member_panel_and_targets(
            root,
            plan_payload,
            member,
            member_sleeves,
            inputs,
            decision_time,
            quality_reference_time,
            output_root / member_id,
            None if panel_paths is None else panel_paths.get(member_id),
        )
        member_panels.append(panel_record)
        raw_targets.extend(member_targets)
    if set(sleeves_by_member) != {
        str(item.get("member_id")) for item in member_panels
    }:
        raise ValueError("冻结 sleeve 引用了未知成员")
    member_panels.sort(key=lambda item: str(item.get("member_id")))
    all_ready = all(
        _mapping(item.get("quality"), "member.quality").get("eligible") is True
        for item in member_panels
    )
    reasons = sorted({
        f"{item.get('member_id')}:{reason}"
        for item in member_panels
        for reason in _list(
            _mapping(item.get("quality"), "member.quality").get("reasons"),
            "quality.reasons",
        )
        if isinstance(reason, str) and reason
    })
    if not all_ready and not reasons:
        reasons = ["selected_member_quality_not_ready"]
    aggregate = 0.0
    sleeves: list[Mapping[str, object]] = []
    for raw_target in sorted(
        raw_targets, key=lambda item: str(item.get("sleeve_id")),
    ):
        weight = _number(raw_target.get("weight"), "sleeve.weight")
        target = _number(raw_target.get("raw_target"), "sleeve.raw_target")
        operational_target = weight * target if all_ready else 0.0
        aggregate += operational_target
        sleeves.append({**raw_target, "operational_target": operational_target})
    plan_allocation = _mapping(plan_payload.get("allocation"), "allocation")
    allocation = {
        "weights": plan_allocation.get("weights"),
        "reserve": plan_allocation.get("reserve"),
        "aggregate_target": aggregate,
        "unit": "fraction_of_portfolio_capital",
    }
    operational = {"eligible": all_ready, "reasons": reasons}
    return member_panels, sleeves, allocation, operational


def run_interval_suite_forward_prediction(
    repository_root: Path,
    plan_id: str,
    *,
    registry_path: Path = Path("data/research/governance.sqlite3"),
) -> IntervalSuitePredictionResult:
    """从一个活动 head 为全部套件成员生成同栅格预测。"""
    root = repository_root.resolve()
    registry = _project_path(root, registry_path, "suite governance registry")
    plan = get_interval_suite_forward_plan(registry, plan_id)
    plan_path = _project_path(
        root, Path(plan.plan_artifact_path), "suite forward plan",
    )
    if sha256_file(plan_path) != plan.plan_artifact_sha256:
        raise ValueError("套件冻结计划现场 SHA-256 不匹配")
    plan_payload = _load(plan_path, "suite forward plan")
    if plan_payload.get("plan_id") != plan_id:
        raise ValueError("套件冻结计划身份不一致")
    if plan_payload.get("governance_registry") != _relative(root, registry):
        raise ValueError("套件冻结计划治理注册表不一致")
    vintage = get_holdout_vintage(registry, plan.vintage_id)
    if vintage.status != "sealed":
        raise ValueError("已消费 vintage 不得追加套件预测")
    live_root = resolve_data_root_locator(
        root, plan_payload.get("live_data_root"),
    )
    members = _list(plan_payload.get("members"), "plan.members")
    source_paths = tuple(sorted({
        _project_path(root, Path(_text(path, "config source")), "config source")
        for raw_member in members
        for path in _list(
            _mapping(raw_member, "plan.member").get("config_source_paths"),
            "config_source_paths",
        )
    }))
    identity = code_identity(root, source_paths)
    if (
        not identity.decision_grade
        or identity.git_hash != plan.source_git_hash
        or identity.tree_digest != plan.code_tree_digest
    ):
        raise ValueError("套件预测执行器必须是冻结计划的 clean code tree")
    evaluated_at = _utc(clock.utc_now())
    inputs = capture_trade_input_receipt(
        live_root,
        vintage.market_id,
        root / "data" / "research" / "input-receipts",
    )
    if inputs.receipt_path is None or inputs.receipt_sha256 is None:
        raise AssertionError("套件预测没有生成活动 head 收据")
    grid = _mapping(plan_payload.get("decision_grid"), "decision_grid")
    interval_seconds = _integer(grid.get("interval_seconds"), "interval_seconds")
    offset = grid.get("utc_epoch_offset_seconds")
    if not isinstance(offset, int) or isinstance(offset, bool):
        raise ValueError("utc_epoch_offset_seconds 必须为整数")
    decision = _decision_time(
        inputs.maximum_event_time, vintage.end_time, interval_seconds, offset,
    )
    if not vintage.start_time <= decision < vintage.end_time:
        raise ValueError("活动输入尚未覆盖套件 vintage 的可用共同栅格")
    existing = {
        item.decision_time: item
        for item in list_interval_suite_forward_predictions(registry, plan_id)
    }.get(decision)
    if existing is not None:
        path = _project_path(
            root, Path(existing.prediction_artifact_path), "suite prediction",
        )
        if sha256_file(path) != existing.prediction_artifact_sha256:
            raise ValueError("已登记套件预测现场 SHA-256 不匹配")
        existing_payload = _load(path, "suite prediction")
        allocation = _mapping(
            existing_payload.get("allocation"), "allocation",
        )
        return IntervalSuitePredictionResult(
            existing.prediction_id,
            path,
            existing.prediction_artifact_sha256,
            existing.decision_time,
            _number(allocation.get("aggregate_target"), "aggregate_target"),
        )
    maximum_lag = _integer(
        grid.get("maximum_recording_lag_seconds"),
        "maximum_recording_lag_seconds",
    )
    lag = (evaluated_at - decision).total_seconds()
    if lag < 0 or lag > maximum_lag:
        raise ValueError("最新共同栅格不在套件预测登记时效窗口内")
    prediction_id = interval_suite_forward_prediction_id(
        GOVERNANCE_METHOD_VERSION,
        INTERVAL_SUITE_PREDICTION_METHOD_VERSION,
        plan_id,
        decision,
    )
    output_root = (
        root / "data" / "research" / "interval-suite-frozen-forward"
        / plan_id / prediction_id
    )
    quality_reference_time = decision + timedelta(seconds=maximum_lag)
    member_panels, sleeves, allocation, operational = _evaluate(
        root,
        plan_payload,
        inputs,
        decision,
        quality_reference_time,
        output_root,
    )
    panel_set_hash = interval_suite_member_panel_set_hash(
        plan_id, decision, member_panels,
    )
    payload: dict[str, object] = {
        "schema_version": INTERVAL_SUITE_PREDICTION_SCHEMA_VERSION,
        "method_version": INTERVAL_SUITE_PREDICTION_METHOD_VERSION,
        "governance_method_version": GOVERNANCE_METHOD_VERSION,
        "scope": "INTERVAL_SUITE_FROZEN_FORWARD_PREDICTION",
        "prediction_id": prediction_id,
        "plan_id": plan_id,
        "suite_plan_id": plan.suite_plan_id,
        "suite_evidence_id": plan.suite_evidence_id,
        "deployment_contract_id": plan_payload.get("deployment_contract_id"),
        "decision_time": decision.isoformat(),
        "generated_at": evaluated_at.isoformat(),
        "quality_reference_time": quality_reference_time.isoformat(),
        "vintage": {
            "vintage_id": vintage.vintage_id,
            "market_id": vintage.market_id,
            "start_time": vintage.start_time.isoformat(),
            "end_time": vintage.end_time.isoformat(),
        },
        "input": {
            "head_generation": inputs.head_generation,
            "receipt_path": _relative(root, inputs.receipt_path),
            "receipt_sha256": inputs.receipt_sha256,
        },
        "code_identity": asdict(identity),
        "member_panel_set_hash": panel_set_hash,
        "member_panels": member_panels,
        "sleeves": sleeves,
        "allocation": allocation,
        "operational": operational,
    }
    prediction_text = canonical_json(payload) + "\n"
    prediction_directory = (
        root / "reports" / "strategy-research"
        / "interval-suite-frozen-forward" / vintage.vintage_id / plan_id
        / "predictions"
    )
    prediction_path = prediction_directory / (
        "prediction-sha256-"
        + sha256_text(prediction_text)
        + ".json"
    )
    if prediction_path.exists():
        if prediction_path.read_text(encoding="utf-8") != prediction_text:
            raise ValueError("内容寻址套件预测发生身份冲突")
    else:
        atomic_write_text(prediction_path, prediction_text)
    prediction_sha = sha256_file(prediction_path)
    registered = register_interval_suite_forward_prediction(
        registry,
        plan_id,
        decision,
        inputs.head_generation,
        _relative(root, inputs.receipt_path),
        inputs.receipt_sha256,
        panel_set_hash,
        _relative(root, prediction_path),
        prediction_sha,
        repository_root=root,
    )
    if registered.prediction_id != prediction_id:
        raise RuntimeError("治理注册表返回不同的套件预测身份")
    return IntervalSuitePredictionResult(
        prediction_id,
        prediction_path,
        prediction_sha,
        decision,
        _number(allocation.get("aggregate_target"), "aggregate_target"),
    )


def attest_interval_suite_forward_prediction(
    repository_root: Path,
    registered: IntervalSuiteForwardPrediction,
    plan: IntervalSuiteForwardPlan,
    *,
    require_current_head: bool = False,
) -> Mapping[str, object]:
    """从冻结计划、收据和成员公式独立重建一个套件预测。"""
    root = repository_root.resolve()
    plan_path = _project_path(
        root, Path(plan.plan_artifact_path), "suite forward plan",
    )
    prediction_path = _project_path(
        root, Path(registered.prediction_artifact_path), "suite prediction",
    )
    if sha256_file(plan_path) != plan.plan_artifact_sha256:
        raise ValueError("套件冻结计划现场 SHA-256 不匹配")
    if sha256_file(prediction_path) != registered.prediction_artifact_sha256:
        raise ValueError("套件预测现场 SHA-256 不匹配")
    plan_payload = _load(plan_path, "suite forward plan")
    payload = _load(prediction_path, "suite prediction")
    expected = {
        "schema_version": INTERVAL_SUITE_PREDICTION_SCHEMA_VERSION,
        "method_version": INTERVAL_SUITE_PREDICTION_METHOD_VERSION,
        "governance_method_version": GOVERNANCE_METHOD_VERSION,
        "scope": "INTERVAL_SUITE_FROZEN_FORWARD_PREDICTION",
        "prediction_id": registered.prediction_id,
        "plan_id": plan.plan_id,
        "suite_plan_id": plan.suite_plan_id,
        "suite_evidence_id": plan.suite_evidence_id,
        "decision_time": registered.decision_time.isoformat(),
        "member_panel_set_hash": registered.member_panel_set_hash,
    }
    for field, value in expected.items():
        if payload.get(field) != value:
            raise ValueError(f"套件预测 {field} 与治理登记不一致")
    input_record = _mapping(payload.get("input"), "prediction.input")
    if any((
        input_record.get("head_generation")
        != registered.input_head_generation,
        input_record.get("receipt_path") != registered.input_receipt_path,
        input_record.get("receipt_sha256")
        != registered.input_receipt_sha256,
    )):
        raise ValueError("套件预测 input 与治理登记不一致")
    receipt_path = _project_path(
        root, Path(registered.input_receipt_path), "suite input receipt",
    )
    if sha256_file(receipt_path) != registered.input_receipt_sha256:
        raise ValueError("套件预测输入收据现场 SHA-256 不匹配")
    live_root = resolve_data_root_locator(
        root, plan_payload.get("live_data_root"),
    )
    inputs = attest_trade_input_receipt(
        live_root, receipt_path, require_current_head=require_current_head,
    )
    quality_reference_time = parse_time(
        payload.get("quality_reference_time"), "quality_reference_time",
    )
    recorded_members = _list(payload.get("member_panels"), "member_panels")
    recorded_panel_paths = {
        _text(_mapping(item, "member panel").get("member_id"), "member_id"):
        _text(_mapping(item, "member panel").get("panel_path"), "panel_path")
        for item in recorded_members
    }
    with TemporaryDirectory(prefix="suite-prediction-attest-") as temporary:
        rebuilt_members, rebuilt_sleeves, rebuilt_allocation, rebuilt_operational = (
            _evaluate(
                root,
                plan_payload,
                inputs,
                registered.decision_time,
                quality_reference_time,
                Path(temporary),
                recorded_panel_paths,
            )
        )
    rebuilt_by_id = {
        _text(item.get("member_id"), "member_id"): item
        for item in rebuilt_members
    }
    normalized_rebuilt: list[Mapping[str, object]] = []
    for raw_recorded in recorded_members:
        recorded = _mapping(raw_recorded, "member panel")
        member_id = _text(recorded.get("member_id"), "member_id")
        rebuilt = dict(rebuilt_by_id.get(member_id, {}))
        if not rebuilt or rebuilt.get("panel_sha256") != recorded.get("panel_sha256"):
            raise ValueError("套件预测成员面板不能由冻结收据重建")
        rebuilt["panel_path"] = recorded.get("panel_path")
        if canonical_json(rebuilt) != canonical_json(recorded):
            raise ValueError("套件预测成员面板事实不能完整重建")
        normalized_rebuilt.append(rebuilt)
    panel_set_hash = interval_suite_member_panel_set_hash(
        plan.plan_id, registered.decision_time, normalized_rebuilt,
    )
    if panel_set_hash != registered.member_panel_set_hash:
        raise ValueError("套件预测成员面板 row-set 身份不一致")
    if canonical_json(rebuilt_sleeves) != canonical_json(payload.get("sleeves")):
        raise ValueError("套件预测 sleeve 目标不能由冻结公式重建")
    if canonical_json(rebuilt_allocation) != canonical_json(payload.get("allocation")):
        raise ValueError("套件预测聚合仓位不能由 sleeve 贡献重建")
    if canonical_json(rebuilt_operational) != canonical_json(
        payload.get("operational")
    ):
        raise ValueError("套件预测质量硬门不能由成员事实重建")
    return payload
