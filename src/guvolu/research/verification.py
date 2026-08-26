"""已发布研究运行的内容与安全不变量复核。"""
from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from guvolu.research.allocator import allocate, allocation_payload
from guvolu.research.artifact_contracts import (
    INTERVAL_SECONDS,
    SECONDS_PER_YEAR,
    cost_replay_body,
    family_payload,
    market_state_payload,
    position_contract_payload,
    trial_ledger_body,
)
from guvolu.research.config_lineage import attest_config_lineage_snapshot
from guvolu.research.contracts import FamilyEvaluation, PanelSnapshot, QualityVector
from guvolu.research.data_location import resolve_data_root_locator
from guvolu.research.features import classify_market_state, compute_features
from guvolu.research.features import FEATURE_METHOD_VERSION
from guvolu.research.governance import (
    get_active_head_receipt,
    get_research_exposure,
)
from guvolu.research.panel import (
    PANEL_METHOD_VERSION,
    PANEL_SCHEMA_VERSION,
    TRADE_INPUT_RECEIPT_METHOD_VERSION,
    attest_trade_input_receipt,
    build_panel_snapshot,
    parse_time,
)
from guvolu.data.trade_economics import TRADE_FLOW_INPUT_METHOD_VERSION
from guvolu.research.provenance import (
    canonical_json,
    code_tree_digest_at_commit,
    sha256_file,
    sha256_text,
    stable_identifier,
    verify_artifacts_match_commit,
)
from guvolu.research.quality import (
    OPERATIONAL_GATE_METHOD_VERSION,
    gate_economic_trade_volume,
    gate_feature_snapshot,
    panel_quality,
    quality_payload,
)
from guvolu.research.validation import walk_forward_validate
from guvolu.strategy.generation import (
    build_family_batches,
    candidate_registry_payload,
)
from guvolu.strategy.contracts import FeatureRow


_RUN_IDENTITY_FIELDS = (
    "schema_version",
    "pipeline_method_version",
    "p_value_method_version",
    "pbo_method_version",
    "block_bootstrap_method_version",
    "regime_attribution_method_version",
    "deflated_sharpe_method_version",
    "effective_trial_method_version",
    "parameter_stability_method_version",
    "position_contract_method_version",
    "panel_method_version",
    "panel_schema_version",
    "feature_method_version",
    "trade_flow_input_method_version",
    "trade_input_receipt_method_version",
    "operational_gate_method_version",
    "governance_method_version",
    "run_id",
    "research_identity",
    "generator_method_version",
    "family_scope",
    "decision_time",
    "execution_evaluated_at",
    "source_data_root",
    "source_data_snapshot",
    "code_identity",
    "config_hash",
    "config_lineage_root_hash",
    "config_lineage_depth",
    "panel_to_time",
)


@dataclass(frozen=True)
class VerificationResult:
    """一次研究运行复核的结果。"""

    run_id: str
    manifest_path: Path
    manifest_sha256: str
    checked_artifacts: tuple[str, ...]
    cache_hit: bool = False


@dataclass(frozen=True)
class ArtifactIntegrityResult:
    """manifest 与全部受保护制品的逐字节完整性结果。"""

    manifest_path: Path
    manifest_sha256: str
    manifest: Mapping[str, object]
    summary: Mapping[str, object]
    candidate_registry: Mapping[str, object] | None
    artifact_paths: Mapping[str, Path]
    checked_artifacts: tuple[str, ...]


def _object(value: object, name: str) -> Mapping[str, object]:
    """验证 JSON 对象。"""
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} 必须为对象")
    return {str(key): item for key, item in value.items()}


def _text(value: object, name: str) -> str:
    """验证非空字符串。"""
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} 必须为非空字符串")
    return value


def _number(value: object, name: str) -> float:
    """验证 JSON 数值。"""
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValueError(f"{name} 必须为数值")
    return float(value)


def _integer(value: object, name: str) -> int:
    """验证 JSON 整数。"""
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"{name} 必须为整数")
    return value


def _text_tuple(value: object, name: str) -> tuple[str, ...]:
    """验证有序且不重复的非空文本数组。"""
    if not isinstance(value, (list, tuple)) or not value:
        raise ValueError(f"{name} 必须为非空文本数组")
    result = tuple(_text(item, name) for item in value)
    if result != tuple(sorted(result)) or len(set(result)) != len(result):
        raise ValueError(f"{name} 必须有序且不重复")
    return result


def _text_sequence(value: object, name: str) -> tuple[str, ...]:
    """验证保持业务顺序且不重复的非空文本数组。"""
    if not isinstance(value, (list, tuple)) or not value:
        raise ValueError(f"{name} 必须为非空文本数组")
    result = tuple(_text(item, name) for item in value)
    if len(set(result)) != len(result):
        raise ValueError(f"{name} 不能重复")
    return result


def _read_json(path: Path) -> Mapping[str, object]:
    """读取并验证 JSON 对象。"""
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"无法读取 JSON: {path}") from error
    return _object(value, path.as_posix())


def _resolve_manifest(root: Path, manifest_path: Path | None) -> tuple[Path, str | None]:
    """解析显式 manifest 或活动运行指针。"""
    if manifest_path is not None:
        resolved = manifest_path if manifest_path.is_absolute() else root / manifest_path
        return resolved.resolve(), None
    latest_path = root / "reports" / "strategy-research" / "latest.json"
    latest = _read_json(latest_path)
    relative = _text(latest.get("manifest"), "latest.manifest")
    expected = _text(latest.get("manifest_sha256"), "latest.manifest_sha256")
    return (root / relative).resolve(), expected


def _recorded_panel_to_time(
    manifest: Mapping[str, object],
    maximum_event_time: datetime,
) -> datetime:
    """按 manifest 记录的截止上限重建面板 to_time。"""
    raw = manifest.get("panel_to_time")
    if raw is None:
        return maximum_event_time
    record = _object(raw, "manifest.panel_to_time")
    limit = record.get("limit")
    to_time = (
        maximum_event_time if limit is None
        else min(parse_time(limit, "panel_to_time.limit"), maximum_event_time)
    )
    if record.get("effective_to_time") != to_time.isoformat():
        raise ValueError("panel_to_time.effective_to_time 不能由上限与输入重建")
    return to_time


def _panel_to_time_override(manifest: Mapping[str, object]) -> str | None:
    """读取进入研究身份的命令行截止覆盖。"""
    raw = manifest.get("panel_to_time")
    if raw is None:
        return None
    record = _object(raw, "manifest.panel_to_time")
    override = record.get("cli_override")
    if override is None:
        return None
    if record.get("source") != "cli":
        raise ValueError("panel_to_time.cli_override 与来源标记不一致")
    return parse_time(override, "panel_to_time.cli_override").isoformat()


def _artifact_path(root: Path, record: Mapping[str, object], name: str) -> Path:
    """解析并限制制品位于项目目录内。"""
    relative = _text(record.get("path"), f"artifacts.{name}.path")
    path = (root / relative).resolve()
    try:
        path.relative_to(root.resolve())
    except ValueError as error:
        raise ValueError(f"制品路径越出项目目录: {name}") from error
    return path


def _verify_operational_gate(summary: Mapping[str, object]) -> None:
    """质量失败时运行仓位必须全零。"""
    quality = _object(summary.get("operational_quality"), "operational_quality")
    position = _object(summary.get("operational_position"), "operational_position")
    weights = _object(position.get("weights"), "operational_position.weights")
    eligible = quality.get("eligible")
    if not isinstance(eligible, bool):
        raise ValueError("operational_quality.eligible 必须为布尔值")
    reserve = _number(
        position.get("reserve"), "operational_position.reserve",
    )
    numeric_weights: list[float] = []
    for family, value in weights.items():
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise ValueError(f"运行仓位不是数值: {family}")
        numeric_weights.append(float(value))
    if summary.get("pipeline_method_version") == "strategy-research-pipeline-v13":
        panel = _object(summary.get("panel"), "panel")
        evaluations = summary.get("family_evaluations")
        if not isinstance(evaluations, list):
            raise ValueError("v13 family_evaluations 必须为数组")
        flow_sensitive = any(
            isinstance(item, Mapping)
            and item.get("eligible") is True
            and item.get("mode") == "paper"
            and item.get("family") in {"flow_trend", "breakout"}
            for item in evaluations
        )
        if (
            flow_sensitive
            and panel.get("latest_economic_volume_qualified") is not True
            and (eligible or any(abs(value) > 1e-12 for value in numeric_weights))
        ):
            raise ValueError("v13 flow 运行门禁语义过期")
    if not eligible and any(abs(value) > 1e-12 for value in numeric_weights):
        raise ValueError("运行质量失败但存在非零仓位")
    decision_grade = summary.get("decision_grade")
    if not isinstance(decision_grade, bool):
        raise ValueError("decision_grade 必须为布尔值")
    if not decision_grade and any(abs(value) > 1e-12 for value in numeric_weights):
        raise ValueError("代码身份非决策级但存在非零仓位")
    if (not eligible or not decision_grade) and abs(reserve - 1.0) > 1e-12:
        raise ValueError("运行门禁失败但风险余量不是一")
    contract = _object(
        summary.get("operational_target_contract"),
        "operational_target_contract",
    )
    aggregate = _number(
        contract.get("aggregate_target"),
        "operational_target_contract.aggregate_target",
    )
    families = contract.get("families")
    if not isinstance(families, list):
        raise ValueError("operational_target_contract.families 必须为数组")
    contribution_total = 0.0
    for index, raw_family in enumerate(families):
        record = _object(raw_family, f"operational_target_contract.families.{index}")
        name = _text(record.get("family"), "target family")
        target_value = _number(record.get("family_target"), f"{name}.family_target")
        weight_value = _number(
            record.get("allocation_weight"), f"{name}.allocation_weight",
        )
        contribution_value = _number(
            record.get("portfolio_target_contribution"),
            f"{name}.portfolio_target_contribution",
        )
        if abs(contribution_value - target_value * weight_value) > 1e-12:
            raise ValueError(f"运行目标贡献计算不一致: {name}")
        if name not in weights or abs(
            weight_value - _number(weights.get(name), f"weights.{name}")
        ) > 1e-12:
            raise ValueError(f"运行目标权重与分配器不一致: {name}")
        contribution_total += contribution_value
    if abs(contribution_total - aggregate) > 1e-12:
        raise ValueError("运行目标合同聚合值不一致")
    if not eligible and abs(aggregate) > 1e-12:
        raise ValueError("运行质量失败但组合目标非零")
    if not decision_grade and abs(aggregate) > 1e-12:
        raise ValueError("代码身份非决策级但组合目标非零")


def _gate_operational_quality_for_pipeline(
    pipeline_method_version: str,
    quality: QualityVector,
    families: Sequence[FamilyEvaluation],
    feature: FeatureRow,
) -> QualityVector:
    """按发布版本重建经济成交运行门，避免改写历史质量对象。"""
    if pipeline_method_version == "strategy-research-pipeline-v13":
        return quality
    if pipeline_method_version == "strategy-research-pipeline-v14":
        return gate_economic_trade_volume(quality, families, feature)
    raise ValueError("不受支持的 operational gate 管线版本")


def _attest_panel_volume_qualification(
    panel: PanelSnapshot,
    features: Sequence[FeatureRow],
    decision_index: int,
    summary: Mapping[str, object],
) -> None:
    """从受保护 panel/features 重算研究期与当前窗口成交资格。"""
    panel_summary = _object(summary.get("panel"), "summary.panel")
    expected_research = all(bar.volume_qualified for bar in panel.bars)
    decision_feature = features[decision_index]
    expected_latest = (
        decision_feature.volume_qualified
        and decision_feature.volume_score is not None
        and decision_feature.flow_imbalance is not None
    )
    if (
        panel_summary.get("research_economic_volume_qualified")
        is not expected_research
        or panel_summary.get("latest_economic_volume_qualified")
        is not expected_latest
    ):
        raise ValueError("v13 panel 经济成交资格摘要不能由受保护证据重建")


def _attest_v12_decision_evidence(
    panel: PanelSnapshot,
    config: Mapping[str, object],
    family_scope: tuple[str, ...],
    research_identity: str,
    summary: Mapping[str, object],
    artifact_paths: Mapping[str, Path],
    block_bootstrap_method_version: str,
    regime_attribution_method_version: str | None,
    pipeline_method_version: str,
) -> None:
    """从受保护 panel/config 重建候选选择、资格、资金权重和回放。"""
    batches = build_family_batches(config, family_scope)
    candidates = tuple(
        candidate for batch in batches for candidate in batch.candidates
    )
    feature_config = _object(config.get("features"), "features")
    raw_lookbacks = feature_config.get("lookbacks")
    if not isinstance(raw_lookbacks, list):
        raise ValueError("v12 features.lookbacks 必须为数组")
    lookbacks = tuple(_integer(value, "features.lookbacks") for value in raw_lookbacks)
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
    valid_indices = [
        index for index, feature in enumerate(features)
        if feature.contiguous
        and feature.trend_scores.get(state_lookback) is not None
    ]
    if not valid_indices:
        raise ValueError("v12 受保护 panel 没有可用决策时点")
    decision_index = valid_indices[-1]
    decision_time = features[decision_index].decision_time
    _attest_panel_volume_qualification(
        panel, features, decision_index, summary,
    )
    validation = walk_forward_validate(
        research_identity,
        panel.bars,
        features,
        candidates,
        config,
        decision_index=decision_index,
        block_bootstrap_method_version=block_bootstrap_method_version,
        regime_attribution_method_version=regime_attribution_method_version,
    )
    interval = _text(config.get("bar_interval"), "bar_interval")
    interval_seconds = INTERVAL_SECONDS.get(interval)
    if interval_seconds is None:
        raise ValueError("v12 bar_interval 不受支持")
    periods_per_year = SECONDS_PER_YEAR / interval_seconds
    minimum_bars = _integer(
        _object(config.get("validation"), "validation").get("minimum_oos_bars"),
        "minimum_oos_bars",
    )
    research_quality = panel_quality(
        panel,
        decision_time,
        _integer(
            config.get("strategy_decision_max_age_seconds"),
            "strategy_decision_max_age_seconds",
        ),
        minimum_bars,
    )
    market_state = classify_market_state(
        features[decision_index],
        state_lookback,
        features[decision_index].volume_score,
        periods_per_year,
    )
    research_position = allocate(
        validation.families,
        market_state,
        research_quality,
        _object(config.get("allocation"), "allocation"),
        l2_overlay=0.0,
    )
    expected = {
        "decision_time": decision_time.isoformat(),
        "market_state": market_state_payload(market_state),
        "research_quality": quality_payload(research_quality),
        "family_evaluations": family_payload(validation),
        "research_position": allocation_payload(research_position),
        "research_target_contract": position_contract_payload(
            validation, research_position,
        ),
    }
    for name, value in expected.items():
        if canonical_json(summary.get(name)) != canonical_json(value):
            raise ValueError(f"v12 {name} 不能由受保护决策证据重建")
    execution_evaluated_at = parse_time(
        summary.get("execution_evaluated_at"), "execution_evaluated_at",
    )
    maximum_age = _integer(
        config.get("strategy_decision_max_age_seconds"),
        "strategy_decision_max_age_seconds",
    )
    operational_quality = gate_feature_snapshot(
        panel_quality(panel, execution_evaluated_at, maximum_age, minimum_bars),
        decision_time,
        execution_evaluated_at,
        maximum_age,
    )
    # v13 重建旧质量对象。
    # 不安全权重由独立门拒绝。
    # v14 才纳入新成交门。
    operational_quality = _gate_operational_quality_for_pipeline(
        pipeline_method_version,
        operational_quality,
        validation.families,
        features[decision_index],
    )
    code = _object(summary.get("code_identity"), "code_identity")
    if code.get("decision_grade") is not True:
        operational_quality = QualityVector(
            integrity=operational_quality.integrity,
            freshness=operational_quality.freshness,
            clock=operational_quality.clock,
            coverage=operational_quality.coverage,
            pit=operational_quality.pit,
            lineage=False,
            reasons=tuple(sorted({
                *operational_quality.reasons,
                f"code_identity_{code.get('reason') or 'not_decision_grade'}",
            })),
        )
    if canonical_json(summary.get("operational_quality")) != canonical_json(
        quality_payload(operational_quality)
    ):
        version = "v13" if pipeline_method_version.endswith("v13") else "v14"
        raise ValueError(
            f"{version} operational_quality 不能由受保护证据重建"
        )
    strategy_decision = _object(
        summary.get("strategy_decision"), "strategy_decision",
    )
    if (
        strategy_decision.get("feature_index") != decision_index
        or strategy_decision.get("decision_time") != decision_time.isoformat()
    ):
        raise ValueError("v12 strategy_decision 不能由受保护特征重建")
    target_path = artifact_paths.get("target_position")
    trial_path = artifact_paths.get("trial_ledger")
    replay_path = artifact_paths.get("label_cost_replay")
    if target_path is None or trial_path is None or replay_path is None:
        raise ValueError("v12 缺少决策、trial 或成本回放制品")
    target = _read_json(target_path)
    for name, value in (
        ("research_replay", allocation_payload(research_position)),
        (
            "research_target_contract",
            position_contract_payload(validation, research_position),
        ),
    ):
        if canonical_json(target.get(name)) != canonical_json(value):
            raise ValueError(f"v12 target_position.{name} 现场重建不一致")
    rebuilt_trial = trial_ledger_body(validation, research_identity).encode("utf-8")
    rebuilt_replay = cost_replay_body(
        panel, validation, config, research_identity,
    ).encode("utf-8")
    if rebuilt_trial != trial_path.read_bytes():
        raise ValueError("v12 trial ledger 不能由验证结果重建")
    if rebuilt_replay != replay_path.read_bytes():
        raise ValueError("v12 成本回放不能由验证路径重建")


def _verify_run_identity(
    root: Path,
    manifest: Mapping[str, object],
    summary: Mapping[str, object],
    candidate_registry: Mapping[str, object] | None,
    artifact_paths: Mapping[str, Path],
    source_data_root: Path,
) -> None:
    """绑定摘要、manifest、代码身份与 v11/v12 研究身份。"""
    for field in _RUN_IDENTITY_FIELDS:
        if manifest.get(field) != summary.get(field):
            raise ValueError(f"summary 与 manifest 的 {field} 不一致")
    code = _object(manifest.get("code_identity"), "manifest.code_identity")
    dirty = code.get("dirty")
    decision_grade = code.get("decision_grade")
    if not isinstance(dirty, bool) or not isinstance(decision_grade, bool):
        raise ValueError("code_identity 的 dirty/decision_grade 必须为布尔值")
    git_hash = code.get("git_hash")
    if git_hash is not None and (not isinstance(git_hash, str) or not git_hash):
        raise ValueError("code_identity.git_hash 必须为空或非空字符串")
    expected_grade = git_hash is not None and not dirty
    if decision_grade is not expected_grade:
        raise ValueError("code_identity.decision_grade 与 Git/dirty 状态不一致")
    if summary.get("decision_grade") is not decision_grade:
        raise ValueError("summary.decision_grade 与 code_identity 不一致")
    method = manifest.get("pipeline_method_version")
    if method not in {
        "strategy-research-pipeline-v11",
        "strategy-research-pipeline-v12",
        "strategy-research-pipeline-v13",
        "strategy-research-pipeline-v14",
    }:
        return
    if candidate_registry is None:
        raise ValueError("v11/v12 manifest 缺少 candidate_registry 制品")
    if method == "strategy-research-pipeline-v12":
        raise ValueError(
            "v12 仅允许制品完整性与旧收据只读复核；"
            "当前验证器不会把旧成交语义声明为决策级证据"
        )
    if method in {
        "strategy-research-pipeline-v13",
        "strategy-research-pipeline-v14",
    }:
        config_path = artifact_paths.get("config")
        config_lineage_path = artifact_paths.get("config_lineage")
        panel_path = artifact_paths.get("panel")
        receipt_path = artifact_paths.get("input_receipt")
        if (
            config_path is None
            or config_lineage_path is None
            or panel_path is None
            or receipt_path is None
        ):
            raise ValueError("v12 manifest 缺少配置谱系、panel 或输入收据制品")
        artifacts = _object(manifest.get("artifacts"), "manifest.artifacts")
        expected_kinds = {
            "config": "research_config_snapshot",
            "config_lineage": "research_config_lineage",
            "panel": "research_physical_panel",
            "candidate_registry": "candidate_registry",
            "input_receipt": "active_trade_head_receipt",
        }
        for name, kind in expected_kinds.items():
            record = _object(artifacts.get(name), f"artifacts.{name}")
            if record.get("kind") != kind:
                raise ValueError(f"v12 {name} 制品类型不匹配")
        (
            config,
            config_hash,
            lineage_root_hash,
            lineage_depth,
            config_source_paths,
            config_artifact_paths,
        ) = attest_config_lineage_snapshot(
            root, config_lineage_path, config_path,
        )
        if (
            manifest.get("config_hash") != config_hash
            or manifest.get("config_lineage_root_hash") != lineage_root_hash
            or manifest.get("config_lineage_depth") != lineage_depth
        ):
            raise ValueError("v12 配置身份不能由受保护配置谱系重建")
        family_scope = _text_sequence(manifest.get("family_scope"), "family_scope")
        generator_method_version = _text(
            manifest.get("generator_method_version"),
            "generator_method_version",
        )
        if method in {
            "strategy-research-pipeline-v13",
            "strategy-research-pipeline-v14",
        }:
            expected_registry = candidate_registry_payload(
                build_family_batches(config, family_scope),
                config_hash,
                generator_method_version,
            )
            if canonical_json(candidate_registry) != canonical_json(
                expected_registry
            ):
                raise ValueError(
                    "v13 candidate registry 不能由配置与公式注册表重建"
                )
        attempt_ids = _text_tuple(
            manifest.get("input_attempt_ids"), "input_attempt_ids",
        )
        artifact_ids = _text_tuple(
            manifest.get("input_artifact_ids"), "input_artifact_ids",
        )
        normalizations = _text_tuple(
            manifest.get("normalization_versions"), "normalization_versions",
        )
        receipt_sha256 = _text(
            manifest.get("input_receipt_sha256"), "input_receipt_sha256",
        )
        if sha256_file(receipt_path) != receipt_sha256:
            raise ValueError("v12 输入收据制品与 manifest 身份不一致")
        governance = _object(config.get("data_governance"), "data_governance")
        registry_path = _artifact_path(
            root,
            {"path": _text(governance.get("registry"), "registry")},
            "governance registry",
        )
        registration = get_active_head_receipt(
            registry_path, "research",
            _text(manifest.get("research_identity"), "research_identity"),
        )
        relative_receipt = receipt_path.relative_to(root).as_posix()
        if (
            registration.receipt_artifact_path != relative_receipt
            or registration.receipt_artifact_sha256 != receipt_sha256
        ):
            raise ValueError("v12 输入收据未由治理库绑定研究身份")
        registered = attest_trade_input_receipt(
            source_data_root, receipt_path, require_current_head=False,
        )
        if (
            registered.head_generation != manifest.get("input_head_generation")
            or registered.attempt_ids != attempt_ids
            or registered.artifact_ids != artifact_ids
            or registered.normalization_versions != normalizations
        ):
            raise ValueError("v12 panel 输入身份不能由控制面注册表重建")
        if method in {
            "strategy-research-pipeline-v13",
            "strategy-research-pipeline-v14",
        }:
            expected_methods = {
                "panel_method_version": PANEL_METHOD_VERSION,
                "panel_schema_version": PANEL_SCHEMA_VERSION,
                "feature_method_version": FEATURE_METHOD_VERSION,
                "trade_flow_input_method_version": (
                    TRADE_FLOW_INPUT_METHOD_VERSION
                ),
                "trade_input_receipt_method_version": (
                    TRADE_INPUT_RECEIPT_METHOD_VERSION
                ),
            }
            for field, expected in expected_methods.items():
                if manifest.get(field) != expected:
                    raise ValueError(f"v13 {field} 不受支持")
            if method == "strategy-research-pipeline-v14" and manifest.get(
                "operational_gate_method_version"
            ) != OPERATIONAL_GATE_METHOD_VERSION:
                raise ValueError("v14 operational gate 方法不受支持")
            qualification = _object(
                manifest.get("trade_input_qualification"),
                "trade_input_qualification",
            )
            if qualification != {
                "source_trade_rows": registered.source_trade_rows,
                "economic_trade_rows": registered.economic_trade_rows,
                "unqualified_trade_rows": registered.unqualified_trade_rows,
                "volume_qualified": registered.volume_qualified,
            }:
                raise ValueError("v13 经济成交资格不能由输入收据重建")
        # 只有 v13/v14 可按当前语义重放。
        # v12 已在上方拒绝，禁止部分复验。
        if method in {
            "strategy-research-pipeline-v13",
            "strategy-research-pipeline-v14",
        }:
            with TemporaryDirectory(
                prefix="guvolu-research-attest-"
            ) as temporary:
                rebuilt = build_panel_snapshot(
                    registered,
                    Path(temporary),
                    _text(config.get("bar_interval"), "bar_interval"),
                    parse_time(config.get("from_time"), "from_time"),
                    _recorded_panel_to_time(
                        manifest, registered.maximum_event_time,
                    ),
                    _integer(config.get("notional_scale"), "notional_scale"),
                )
            if rebuilt.panel_sha256 != sha256_file(panel_path):
                raise ValueError(
                    "v13 panel 不能由注册输入和版本化查询重建"
                )
            raw_regime_attribution_method = manifest.get(
                "regime_attribution_method_version"
            )
            regime_attribution_method = (
                None
                if raw_regime_attribution_method is None
                else _text(
                    raw_regime_attribution_method,
                    "regime_attribution_method_version",
                )
            )
            _attest_v12_decision_evidence(
                rebuilt,
                config,
                family_scope,
                _text(
                    manifest.get("research_identity"), "research_identity"
                ),
                summary,
                artifact_paths,
                _text(
                    manifest.get("block_bootstrap_method_version"),
                    "block_bootstrap_method_version",
                ),
                regime_attribution_method,
                _text(method, "pipeline_method_version"),
            )
        if decision_grade:
            assert isinstance(git_hash, str)
            commit_digest = code_tree_digest_at_commit(
                root, git_hash, config_source_paths,
            )
            if code.get("tree_digest") != commit_digest:
                raise ValueError("v12 code tree 不能由记录的 clean commit 重建")
            if code.get("dirty_digest") != sha256_text(""):
                raise ValueError("v12 clean run 的 dirty digest 不为空")
            verify_artifacts_match_commit(
                root,
                git_hash,
                tuple(zip(config_source_paths, config_artifact_paths, strict=True)),
            )
    raw_candidates = candidate_registry.get("candidates")
    if not isinstance(raw_candidates, list):
        raise ValueError("candidate_registry.candidates 必须为数组")
    candidate_ids = tuple(
        _text(
            _object(candidate, f"candidate_registry.candidates.{index}").get(
                "candidate_id"
            ),
            f"candidate_registry.candidates.{index}.candidate_id",
        )
        for index, candidate in enumerate(raw_candidates)
    )
    identity_payload: dict[str, object] = {
        "pipeline_method_version": manifest.get("pipeline_method_version"),
        "p_value_method_version": manifest.get("p_value_method_version"),
        "pbo_method_version": manifest.get("pbo_method_version"),
        "block_bootstrap_method_version": manifest.get(
            "block_bootstrap_method_version"
        ),
        "deflated_sharpe_method_version": manifest.get(
            "deflated_sharpe_method_version"
        ),
        "effective_trial_method_version": manifest.get(
            "effective_trial_method_version"
        ),
        "parameter_stability_method_version": manifest.get(
            "parameter_stability_method_version"
        ),
        "position_contract_method_version": manifest.get(
            "position_contract_method_version"
        ),
        "config_hash": manifest.get("config_hash"),
        "config_lineage_root_hash": manifest.get("config_lineage_root_hash"),
        "config_lineage_depth": manifest.get("config_lineage_depth"),
        "head_generation": manifest.get("input_head_generation"),
        "attempt_ids": manifest.get("input_attempt_ids"),
        "artifact_ids": manifest.get("input_artifact_ids"),
        "code_tree_digest": code.get("tree_digest"),
        "dirty_digest": code.get("dirty_digest"),
        "generator_method_version": manifest.get("generator_method_version"),
        "family_scope": manifest.get("family_scope"),
        "candidate_ids": candidate_ids,
        "governance_method_version": manifest.get("governance_method_version"),
        "data_scope": manifest.get("data_scope"),
    }
    if "regime_attribution_method_version" in manifest:
        identity_payload["regime_attribution_method_version"] = manifest.get(
            "regime_attribution_method_version"
        )
    if method in {
        "strategy-research-pipeline-v12",
        "strategy-research-pipeline-v13",
        "strategy-research-pipeline-v14",
    }:
        identity_payload["input_receipt_sha256"] = manifest.get(
            "input_receipt_sha256"
        )
        if method in {
            "strategy-research-pipeline-v13",
            "strategy-research-pipeline-v14",
        }:
            identity_payload.update({
                "panel_method_version": manifest.get("panel_method_version"),
                "panel_schema_version": manifest.get("panel_schema_version"),
                "feature_method_version": manifest.get(
                    "feature_method_version"
                ),
                "trade_flow_input_method_version": manifest.get(
                    "trade_flow_input_method_version"
                ),
                "trade_input_receipt_method_version": manifest.get(
                    "trade_input_receipt_method_version"
                ),
            })
            identity_payload["trade_input_qualification"] = manifest.get(
                "trade_input_qualification"
            )
            if method == "strategy-research-pipeline-v14":
                identity_payload["operational_gate_method_version"] = manifest.get(
                    "operational_gate_method_version"
                )
        if "source_data_snapshot" in manifest:
            identity_payload["source_data_snapshot"] = manifest.get(
                "source_data_snapshot"
            )
    override = _panel_to_time_override(manifest)
    if override is not None:
        identity_payload["panel_to_time_override"] = override
    research_identity = stable_identifier("research-identity", identity_payload)
    if manifest.get("research_identity") != research_identity:
        raise ValueError("manifest.research_identity 无法由受保护证据重建")
    run_started_at = manifest.get("run_started_at")
    run_payload = (
        {
            "research_identity": research_identity,
            "run_started_at": run_started_at,
        }
        if run_started_at is not None
        # 早期运行没有起始时点
        else {
            "research_identity": research_identity,
            "execution_evaluated_at": manifest.get("execution_evaluated_at"),
        }
    )
    if manifest.get("run_id") != stable_identifier("research-run", run_payload):
        raise ValueError("manifest.run_id 无法由研究身份和运行时点重建")


def _verify_data_governance(root: Path, summary: Mapping[str, object]) -> None:
    """复核 v8 开发运行绑定的不可变数据暴露。"""
    if summary.get("pipeline_method_version") not in (
        "strategy-research-pipeline-v8",
        "strategy-research-pipeline-v9",
        "strategy-research-pipeline-v10",
        "strategy-research-pipeline-v11",
        "strategy-research-pipeline-v12",
        "strategy-research-pipeline-v13",
        "strategy-research-pipeline-v14",
    ):
        return
    governance = _object(summary.get("data_governance"), "data_governance")
    if governance.get("scope") != "DEV_ADAPTIVE":
        raise ValueError("普通研究运行的数据范围必须为 DEV_ADAPTIVE")
    relative = _text(governance.get("registry"), "data_governance.registry")
    registry = (root / relative).resolve()
    try:
        registry.relative_to(root)
    except ValueError as error:
        raise ValueError("研究治理注册表越出项目目录") from error
    exposure_id = _text(
        governance.get("exposure_id"),
        "data_governance.exposure_id",
    )
    exposure = get_research_exposure(registry, exposure_id)
    if exposure.research_identity != summary.get("research_identity"):
        raise ValueError("研究暴露与 summary 的 research_identity 不一致")
    if exposure.market_id != summary.get("market_id"):
        raise ValueError("研究暴露与 summary 的 market_id 不一致")
    panel = _object(summary.get("panel"), "panel")
    if exposure.start_time.isoformat() != governance.get("from_time"):
        raise ValueError("研究暴露起点不一致")
    if exposure.end_time.isoformat() != governance.get("to_time"):
        raise ValueError("研究暴露终点不一致")
    panel_from = _text(panel.get("from_time"), "panel.from_time")
    panel_to = _text(panel.get("to_time"), "panel.to_time")
    if panel_from < exposure.start_time.isoformat():
        raise ValueError("研究面板早于已登记暴露区间")
    if panel_to > exposure.end_time.isoformat():
        raise ValueError("研究面板晚于已登记暴露区间")


def verify_research_artifact_integrity(
    root: Path,
    manifest_path: Path | None = None,
) -> ArtifactIntegrityResult:
    """复核 manifest 指针、全部制品散列与字节数。"""
    resolved_root = root.resolve()
    resolved_manifest, expected_manifest_hash = _resolve_manifest(
        resolved_root,
        manifest_path,
    )
    manifest_hash = sha256_file(resolved_manifest)
    if expected_manifest_hash is not None and manifest_hash != expected_manifest_hash:
        raise ValueError("latest 指针中的 manifest 散列不匹配")
    manifest = _read_json(resolved_manifest)
    artifacts = _object(manifest.get("artifacts"), "manifest.artifacts")
    checked: list[str] = []
    summary: Mapping[str, object] | None = None
    candidate_registry: Mapping[str, object] | None = None
    artifact_paths: dict[str, Path] = {}
    for name, raw_record in sorted(artifacts.items()):
        record = _object(raw_record, f"artifacts.{name}")
        path = _artifact_path(resolved_root, record, name)
        if not path.is_file():
            raise ValueError(f"制品不存在: {name}")
        expected_hash = _text(record.get("sha256"), f"artifacts.{name}.sha256")
        if sha256_file(path) != expected_hash:
            raise ValueError(f"制品散列不匹配: {name}")
        expected_bytes = record.get("bytes")
        if not isinstance(expected_bytes, int) or isinstance(expected_bytes, bool):
            raise ValueError(f"制品字节数非法: {name}")
        if path.stat().st_size != expected_bytes:
            raise ValueError(f"制品字节数不匹配: {name}")
        if name == "summary_json":
            summary = _read_json(path)
        elif name == "candidate_registry":
            candidate_registry = _read_json(path)
        artifact_paths[name] = path
        checked.append(name)
    if summary is None:
        raise ValueError("manifest 缺少 summary_json 制品")
    return ArtifactIntegrityResult(
        manifest_path=resolved_manifest,
        manifest_sha256=manifest_hash,
        manifest=manifest,
        summary=summary,
        candidate_registry=candidate_registry,
        artifact_paths=artifact_paths,
        checked_artifacts=tuple(checked),
    )


def verify_research_runtime_invariants(
    root: Path,
    summary: Mapping[str, object],
) -> None:
    """复核无需重建历史面板的运行门禁与治理登记。"""
    _verify_operational_gate(summary)
    _verify_data_governance(root.resolve(), summary)


def verify_research_run(
    root: Path,
    manifest_path: Path | None = None,
) -> VerificationResult:
    """完整重建研究决策证据，并复核运行质量硬门禁。"""
    resolved_root = root.resolve()
    integrity = verify_research_artifact_integrity(
        resolved_root, manifest_path,
    )
    manifest = integrity.manifest
    summary = integrity.summary
    source_data_root = resolve_data_root_locator(
        resolved_root, manifest.get("source_data_root"),
    )
    run_id = _text(manifest.get("run_id"), "manifest.run_id")
    _verify_run_identity(
        resolved_root,
        manifest,
        summary,
        integrity.candidate_registry,
        integrity.artifact_paths,
        source_data_root,
    )
    verify_research_runtime_invariants(resolved_root, summary)
    return VerificationResult(
        run_id=run_id,
        manifest_path=integrity.manifest_path,
        manifest_sha256=integrity.manifest_sha256,
        checked_artifacts=integrity.checked_artifacts,
    )
