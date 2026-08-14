"""研究数据暴露与一次性封存段治理。"""
from __future__ import annotations

import json
import math
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from guvolu.research import clock
from guvolu.research.contracts import (
    FROZEN_FORWARD_METHOD_VERSION,
    FROZEN_FORWARD_SCHEMA_VERSION,
    HOLDOUT_MANIFEST_SCHEMA_VERSION,
    HOLDOUT_METHOD_VERSION,
)
from guvolu.research.provenance import sha256_file, stable_identifier
from guvolu.strategy.expression import (
    candidate_identity,
    expression_id,
    strategy_expression,
)

GOVERNANCE_SCHEMA_VERSION = 5
SCHEMA_WRITE_CEILING_KEY = "schema_write_ceiling"
GOVERNANCE_METHOD_VERSION = "research-data-governance-v2"
_VINTAGE_STATUSES = ("sealed", "consumed")


@dataclass(frozen=True)
class ResearchExposure:
    """一次自适应研究已读取的数据区间。"""

    exposure_id: str
    research_identity: str
    market_id: str
    start_time: datetime
    end_time: datetime
    recorded_at: datetime


@dataclass(frozen=True)
class HoldoutVintage:
    """一个封存或已消费的数据区间。"""

    vintage_id: str
    market_id: str
    start_time: datetime
    end_time: datetime
    sealed_at: datetime
    status: str
    consumed_at: datetime | None
    candidate_set_hash: str | None
    evaluation_id: str | None
    verdict: str | None
    verdict_recorded_at: datetime | None


@dataclass(frozen=True)
class HoldoutEvaluationAttempt:
    """一次烧毁 vintage 后不可重跑的评估尝试状态。"""

    evaluation_id: str
    vintage_id: str
    candidate_set_hash: str
    status: str
    stage: str
    started_at: datetime
    updated_at: datetime
    completed_at: datetime | None
    result_manifest_path: str | None
    result_manifest_sha256: str | None


@dataclass(frozen=True)
class FrozenForwardPlan:
    """在 vintage 开始前冻结的候选、公式与资金权重计划。"""

    plan_id: str
    vintage_id: str
    source_manifest_sha256: str
    candidate_set_hash: str
    config_hash: str
    code_tree_digest: str
    plan_artifact_path: str
    plan_artifact_sha256: str
    frozen_at: datetime


@dataclass(frozen=True)
class FrozenForwardPrediction:
    """按决策时间追加且不可改写的冻结计划预测。"""

    prediction_id: str
    plan_id: str
    vintage_id: str
    decision_time: datetime
    input_head_generation: str
    panel_sha256: str
    prediction_artifact_path: str
    prediction_artifact_sha256: str
    recorded_at: datetime


@dataclass(frozen=True)
class ActiveHeadReceiptRegistration:
    """治理库中绑定某个消费者的完整活动输入收据。"""

    consumer_kind: str
    consumer_id: str
    market_id: str
    head_generation: str
    receipt_artifact_path: str
    receipt_artifact_sha256: str
    recorded_at: datetime


@dataclass(frozen=True)
class _TerminalEvidence:
    """已经现场复核、可与治理注册表交叉核对的终态证据。"""

    manifest_path: str
    candidate_set_hash: str
    candidate_ids: tuple[str, ...]
    config_hash: str
    input_head_generation: str
    input_receipt_path: str
    input_receipt_sha256: str
    require_forward_predictions: bool
    forward_plan_id: str | None
    forward_prediction_count: int
    forward_prediction_row_set_hash: str | None
    score_start: datetime
    score_end: datetime
    score_bars: int
    score_decision_times: tuple[datetime, ...]


@dataclass(frozen=True)
class _ForwardPlanEvidence:
    """现场复核后的冻结候选与资金权重合同。"""

    path: Path
    normalized_path: str
    candidate_ids: tuple[str, ...]
    candidate_families: tuple[tuple[str, str], ...]
    weights: tuple[tuple[str, float], ...]
    reserve: float


def _utc(value: datetime) -> datetime:
    """把时间统一为有时区的 UTC。"""
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _timestamp(value: datetime) -> str:
    """生成可按文本排序的 UTC 时间。"""
    return _utc(value).isoformat(timespec="microseconds")


def _parse_timestamp(value: str) -> datetime:
    """读取注册表中的 UTC 时间。"""
    return _utc(datetime.fromisoformat(value))


def _canonical_sha256(value: str) -> bool:
    """检查小写十六进制 SHA-256 的规范文本。"""
    return len(value) == 64 and all(
        character in "0123456789abcdef" for character in value
    )


def _evidence_file(root: Path, value: str, label: str) -> tuple[Path, str]:
    """把证据路径约束在仓库内并返回规范相对路径。"""
    repository = root.resolve()
    relative = Path(value)
    if relative.is_absolute():
        raise ValueError(f"{label} 必须使用仓库内相对路径")
    resolved = (repository / relative).resolve()
    try:
        normalized = resolved.relative_to(repository).as_posix()
    except ValueError as error:
        raise ValueError(f"{label} 超出仓库范围") from error
    if not resolved.is_file():
        raise ValueError(f"{label} 文件不存在")
    return resolved, normalized


def _json_file(path: Path, label: str) -> dict[str, object]:
    """读取必须为 JSON 对象的终态证据。"""
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ValueError(f"{label} 不是可读 JSON") from error
    if not isinstance(value, dict):
        raise ValueError(f"{label} 必须为 JSON 对象")
    return {str(key): item for key, item in value.items()}


def _validated_artifact(
    repository_root: Path,
    artifacts: dict[object, object],
    name: str,
    expected_kind: str,
) -> tuple[Path, str]:
    """现场复核 manifest 中一个必需制品的完整内容身份。"""
    value = artifacts.get(name)
    if not isinstance(value, dict):
        raise ValueError(f"holdout manifest 缺少 {name} 制品")
    record = {str(key): item for key, item in value.items()}
    path_value = record.get("path")
    sha256 = record.get("sha256")
    byte_count = record.get("bytes")
    if record.get("kind") != expected_kind:
        raise ValueError(f"holdout {name} 制品 kind 不匹配")
    if not isinstance(path_value, str) or not isinstance(sha256, str):
        raise ValueError(f"holdout {name} 制品身份不完整")
    if not isinstance(byte_count, int) or isinstance(byte_count, bool) or byte_count <= 0:
        raise ValueError(f"holdout {name} 制品字节数无效")
    if not _canonical_sha256(sha256):
        raise ValueError(f"holdout {name} SHA-256 必须是规范小写十六进制")
    path, _ = _evidence_file(repository_root, path_value, f"holdout {name}")
    if path.stat().st_size != byte_count:
        raise ValueError(f"holdout {name} 现场字节数不匹配")
    if sha256_file(path) != sha256:
        raise ValueError(f"holdout {name} 现场 SHA-256 不匹配")
    return path, sha256


def _validated_forward_plan_artifact(
    repository_root: Path,
    plan_id: str,
    vintage_id: str,
    source_manifest_sha256: str,
    candidate_set_hash: str,
    config_hash: str,
    code_tree_digest: str,
    artifact_path: str,
    artifact_sha256: str,
    expected_candidate_ids: tuple[str, ...] | None = None,
) -> _ForwardPlanEvidence:
    """现场复核冻结前向计划制品与注册身份。"""
    if not _canonical_sha256(artifact_sha256):
        raise ValueError("冻结前向计划 SHA-256 无效")
    path, normalized = _evidence_file(
        repository_root, artifact_path, "frozen forward plan",
    )
    if sha256_file(path) != artifact_sha256:
        raise ValueError("冻结前向计划现场 SHA-256 不匹配")
    payload = _json_file(path, "frozen forward plan")
    expected = {
        "schema_version": FROZEN_FORWARD_SCHEMA_VERSION,
        "method_version": FROZEN_FORWARD_METHOD_VERSION,
        "governance_method_version": GOVERNANCE_METHOD_VERSION,
        "scope": "FROZEN_FORWARD",
        "plan_id": plan_id,
        "config_hash": config_hash,
        "code_tree_digest": code_tree_digest,
        "candidate_set_hash": candidate_set_hash,
    }
    for field, value in expected.items():
        if payload.get(field) != value:
            raise ValueError(f"冻结前向计划 {field} 与注册身份不一致")
    vintage = payload.get("vintage")
    if not isinstance(vintage, dict) or vintage.get("vintage_id") != vintage_id:
        raise ValueError("冻结前向计划 vintage_id 与注册身份不一致")
    source = payload.get("source")
    if (
        not isinstance(source, dict)
        or source.get("manifest_sha256") != source_manifest_sha256
    ):
        raise ValueError("冻结前向计划来源 manifest 与注册身份不一致")
    raw_candidates = payload.get("candidates")
    if not isinstance(raw_candidates, list) or not raw_candidates:
        raise ValueError("冻结前向计划缺少 candidates")
    candidate_families: list[tuple[str, str]] = []
    for raw in raw_candidates:
        if not isinstance(raw, dict):
            raise ValueError("冻结前向计划 candidate 合同无效")
        candidate = {str(key): item for key, item in raw.items()}
        candidate_id = candidate.get("candidate_id")
        family = candidate.get("family")
        mode = candidate.get("mode")
        expression_identity = candidate.get("expression_id")
        parameters = candidate.get("parameters")
        complexity = candidate.get("complexity")
        if (
            not isinstance(candidate_id, str) or not candidate_id
            or not isinstance(family, str) or not family
            or not isinstance(mode, str) or not mode
            or not isinstance(expression_identity, str) or not expression_identity
            or not isinstance(parameters, dict)
            or not isinstance(complexity, int) or isinstance(complexity, bool)
            or complexity <= 0
        ):
            raise ValueError("冻结前向计划 candidate 合同无效")
        if any(
            not isinstance(name, str) or not name
            or not isinstance(value, (int, float)) or isinstance(value, bool)
            or not math.isfinite(float(value))
            for name, value in parameters.items()
        ):
            raise ValueError("冻结前向计划 candidate 参数无效")
        numeric_parameters = {
            str(name): value for name, value in parameters.items()
            if isinstance(value, (int, float)) and not isinstance(value, bool)
        }
        try:
            template = strategy_expression(family)
        except ValueError as error:
            raise ValueError("冻结前向计划包含不受支持的策略流派") from error
        if expression_id(template) != expression_identity:
            raise ValueError("冻结前向计划表达式身份不匹配")
        if candidate_identity(template, numeric_parameters) != candidate_id:
            raise ValueError("冻结前向计划候选身份未绑定公式与完整参数")
        if mode != "paper":
            raise ValueError("冻结前向计划只允许 paper 候选")
        candidate_families.append((candidate_id, family))
    candidate_ids = tuple(item[0] for item in candidate_families)
    families = tuple(item[1] for item in candidate_families)
    if (
        candidate_ids != tuple(sorted(candidate_ids))
        or len(set(candidate_ids)) != len(candidate_ids)
        or len(set(families)) != len(families)
    ):
        raise ValueError("冻结前向计划候选必须有序且 candidate/family 唯一")
    if expected_candidate_ids is not None and candidate_ids != expected_candidate_ids:
        raise ValueError("冻结前向计划候选与 holdout 冻结候选全集不一致")
    allocation = payload.get("allocation")
    if not isinstance(allocation, dict) or not isinstance(allocation.get("weights"), dict):
        raise ValueError("冻结前向计划缺少 allocation")
    weights_value = allocation["weights"]
    weights: dict[str, float] = {}
    for family, value in weights_value.items():
        if not isinstance(family, str):
            raise ValueError("冻结前向计划权重流派无效")
        weight = _finite_number(value, f"frozen allocation.{family}")
        if weight < 0.0:
            raise ValueError("冻结前向计划权重不得为负")
        weights[family] = weight
    if set(weights) != set(families):
        raise ValueError("冻结前向计划权重未覆盖候选流派")
    reserve = _finite_number(allocation.get("reserve"), "frozen allocation.reserve")
    if reserve < 0.0 or sum(weights.values()) + reserve > 1.0 + 1e-9:
        raise ValueError("冻结前向计划资金权重与 reserve 合同不成立")
    return _ForwardPlanEvidence(
        path=path,
        normalized_path=normalized,
        candidate_ids=candidate_ids,
        candidate_families=tuple(candidate_families),
        weights=tuple(sorted(weights.items())),
        reserve=reserve,
    )


def _validated_forward_prediction_artifact(
    repository_root: Path,
    prediction_id: str,
    plan_id: str,
    vintage_id: str,
    decision_time: datetime,
    input_head_generation: str,
    panel_sha256: str,
    config_hash: str,
    code_tree_digest: str,
    plan_evidence: _ForwardPlanEvidence,
    artifact_path: str,
    artifact_sha256: str,
    reference_time: datetime,
) -> tuple[Path, str, str, str]:
    """现场复核冻结预测制品的计划、时点、输入和代码身份。"""
    if not _canonical_sha256(artifact_sha256):
        raise ValueError("冻结前向预测 SHA-256 无效")
    path, normalized = _evidence_file(
        repository_root, artifact_path, "frozen forward prediction",
    )
    if sha256_file(path) != artifact_sha256:
        raise ValueError("冻结前向预测现场 SHA-256 不匹配")
    payload = _json_file(path, "frozen forward prediction")
    expected = {
        "schema_version": FROZEN_FORWARD_SCHEMA_VERSION,
        "method_version": FROZEN_FORWARD_METHOD_VERSION,
        "scope": "FROZEN_FORWARD",
        "prediction_id": prediction_id,
        "plan_id": plan_id,
        "vintage_id": vintage_id,
        "decision_time": decision_time.isoformat(),
        "input_head_generation": input_head_generation,
        "panel_sha256": panel_sha256,
        "config_hash": config_hash,
    }
    for field, value in expected.items():
        if payload.get(field) != value:
            raise ValueError(f"冻结前向预测 {field} 与注册身份不一致")
    code = payload.get("code_identity")
    if not isinstance(code, dict) or code.get("tree_digest") != code_tree_digest:
        raise ValueError("冻结前向预测代码树与计划不一致")
    quality = payload.get("quality")
    quality_fields = ("integrity", "freshness", "clock", "coverage", "pit", "lineage")
    if not isinstance(quality, dict) or any(
        not isinstance(quality.get(field), bool) for field in quality_fields
    ):
        raise ValueError("冻结前向预测 quality 合同无效")
    eligible = quality.get("eligible")
    if eligible is not all(bool(quality[field]) for field in quality_fields):
        raise ValueError("冻结前向预测 quality.eligible 推导不一致")
    reasons = quality.get("reasons")
    if not isinstance(reasons, list) or any(not isinstance(item, str) for item in reasons):
        raise ValueError("冻结前向预测 quality.reasons 合同无效")
    raw_families = payload.get("families")
    if not isinstance(raw_families, list):
        raise ValueError("冻结前向预测缺少 families")
    expected_candidates = dict(plan_evidence.candidate_families)
    expected_weights = dict(plan_evidence.weights)
    seen: set[str] = set()
    contribution_total = 0.0
    for raw in raw_families:
        if not isinstance(raw, dict):
            raise ValueError("冻结前向预测 family 合同无效")
        record = {str(key): item for key, item in raw.items()}
        candidate_id = record.get("candidate_id")
        family = record.get("family")
        if (
            not isinstance(candidate_id, str)
            or candidate_id in seen
            or not isinstance(family, str)
            or expected_candidates.get(candidate_id) != family
        ):
            raise ValueError("冻结前向预测 candidate/family 与计划不一致")
        seen.add(candidate_id)
        weight = _finite_number(
            record.get("frozen_allocation_weight"), "frozen prediction weight",
        )
        if not math.isclose(
            weight, expected_weights[family], rel_tol=1e-12, abs_tol=1e-12,
        ):
            raise ValueError("冻结前向预测使用了计划外资金权重")
        target = _finite_number(record.get("family_target"), "frozen family target")
        contribution = _finite_number(
            record.get("portfolio_target_contribution"), "frozen contribution",
        )
        if not math.isclose(
            contribution, target * weight, rel_tol=1e-12, abs_tol=1e-12,
        ):
            raise ValueError("冻结前向预测流派贡献计算不一致")
        if eligible is False and (
            not math.isclose(target, 0.0, abs_tol=1e-12)
            or not math.isclose(contribution, 0.0, abs_tol=1e-12)
        ):
            raise ValueError("冻结前向预测质量失败但存在非零目标")
        contribution_total += contribution
    if seen != set(plan_evidence.candidate_ids):
        raise ValueError("冻结前向预测未覆盖计划候选全集")
    reserve = _finite_number(payload.get("reserve"), "frozen prediction reserve")
    if not math.isclose(
        reserve, plan_evidence.reserve, rel_tol=1e-12, abs_tol=1e-12,
    ):
        raise ValueError("冻结前向预测 reserve 与计划不一致")
    aggregate = _finite_number(payload.get("aggregate_target"), "aggregate_target")
    if not math.isclose(
        aggregate, contribution_total, rel_tol=1e-12, abs_tol=1e-12,
    ):
        raise ValueError("冻结前向预测组合目标聚合不一致")
    if eligible is False and not math.isclose(aggregate, 0.0, abs_tol=1e-12):
        raise ValueError("冻结前向预测质量失败但组合目标非零")
    if payload.get("unit") != "risk_weighted_directional_target":
        raise ValueError("冻结前向预测 unit 不受支持")
    from guvolu.research.frozen_forward import attest_frozen_prediction_artifact

    attest_frozen_prediction_artifact(
        repository_root,
        plan_evidence.path,
        path,
        reference_time,
        require_current_head=True,
    )
    receipt = payload.get("input_receipt")
    if not isinstance(receipt, dict):
        raise ValueError("冻结前向预测缺少活动输入收据")
    receipt_path, normalized_receipt = _evidence_file(
        repository_root,
        str(receipt.get("path") or ""),
        "冻结预测活动输入收据",
    )
    receipt_sha256 = str(receipt.get("sha256") or "")
    if (
        not _canonical_sha256(receipt_sha256)
        or sha256_file(receipt_path) != receipt_sha256
        or payload.get("input_receipt_sha256") != receipt_sha256
    ):
        raise ValueError("冻结前向预测活动输入收据散列不匹配")
    return path, normalized, normalized_receipt, receipt_sha256


def _validated_candidate_set_identity(
    manifest: dict[str, object],
    candidate_set_hash: str,
) -> list[str]:
    """复算冻结候选全集身份并返回规范有序 candidate IDs。"""
    value = manifest.get("candidate_set_identity")
    if not isinstance(value, dict):
        raise ValueError("holdout manifest 缺少 candidate_set_identity")
    identity = {str(key): item for key, item in value.items()}
    required = {
        "holdout_method_version",
        "source_manifest_sha256",
        "source_summary_sha256",
        "candidate_registry_sha256",
        "candidate_ids",
    }
    if set(identity) != required:
        raise ValueError("holdout candidate_set_identity 字段不完整")
    if identity.get("holdout_method_version") != HOLDOUT_METHOD_VERSION:
        raise ValueError("holdout candidate_set_identity 方法版本不匹配")
    for name in (
        "source_manifest_sha256",
        "source_summary_sha256",
        "candidate_registry_sha256",
    ):
        sha256 = identity.get(name)
        if not isinstance(sha256, str) or not _canonical_sha256(sha256):
            raise ValueError(f"holdout candidate_set_identity.{name} 无效")
    raw_ids = identity.get("candidate_ids")
    if (
        not isinstance(raw_ids, list)
        or not raw_ids
        or any(not isinstance(item, str) or not item for item in raw_ids)
    ):
        raise ValueError("holdout candidate_set_identity.candidate_ids 无效")
    candidate_ids = [str(item) for item in raw_ids]
    if candidate_ids != sorted(candidate_ids) or len(set(candidate_ids)) != len(candidate_ids):
        raise ValueError("holdout candidate IDs 必须有序且不重复")
    if stable_identifier("candidate-set", identity) != candidate_set_hash:
        raise ValueError("holdout candidate_set_hash 现场复算不匹配")
    return candidate_ids


def _validated_evaluation_identity(
    manifest: dict[str, object],
    vintage_id: str,
    evaluation_id: str,
    candidate_set_hash: str,
) -> str:
    """复算消费前已冻结且包含 config hash 的 evaluation 身份。"""
    value = manifest.get("evaluation_identity")
    if not isinstance(value, dict):
        raise ValueError("holdout manifest 缺少 evaluation_identity")
    identity = {str(key): item for key, item in value.items()}
    required = {
        "holdout_method_version",
        "governance_method_version",
        "vintage_id",
        "candidate_set_hash",
        "config_hash",
        "code_tree_digest",
        "input_head_generation",
        "input_attempt_ids",
        "input_artifact_ids",
        "normalization_versions",
        "input_receipt_sha256",
    }
    if set(identity) != required:
        raise ValueError("holdout evaluation_identity 字段不完整")
    if identity.get("holdout_method_version") != HOLDOUT_METHOD_VERSION:
        raise ValueError("holdout evaluation_identity 方法版本不匹配")
    if identity.get("governance_method_version") != GOVERNANCE_METHOD_VERSION:
        raise ValueError("holdout evaluation_identity 治理版本不匹配")
    if identity.get("vintage_id") != vintage_id:
        raise ValueError("holdout evaluation_identity vintage_id 不匹配")
    if identity.get("candidate_set_hash") != candidate_set_hash:
        raise ValueError("holdout evaluation_identity candidate set 不匹配")
    config_hash = identity.get("config_hash")
    if not isinstance(config_hash, str) or not _canonical_sha256(config_hash):
        raise ValueError("holdout evaluation_identity config_hash 无效")
    for name in ("code_tree_digest", "input_head_generation"):
        item = identity.get(name)
        if not isinstance(item, str) or not item:
            raise ValueError(f"holdout evaluation_identity.{name} 无效")
    receipt_sha256 = identity.get("input_receipt_sha256")
    if not isinstance(receipt_sha256, str) or not _canonical_sha256(receipt_sha256):
        raise ValueError("holdout evaluation_identity.input_receipt_sha256 无效")
    for name in (
        "input_attempt_ids", "input_artifact_ids", "normalization_versions",
    ):
        values = identity.get(name)
        if (
            not isinstance(values, (list, tuple))
            or not values
            or any(not isinstance(item, str) or not item for item in values)
        ):
            raise ValueError(f"holdout evaluation_identity {name} 无效")
        if list(values) != sorted(values) or len(set(values)) != len(values):
            raise ValueError(f"holdout evaluation_identity {name} 必须有序且不重复")
    if stable_identifier("holdout-evaluation", identity) != evaluation_id:
        raise ValueError("holdout evaluation_id 现场复算不匹配")
    return config_hash


def _finite_number(value: object, label: str) -> float:
    """读取可复核政策和指标中的有限数值。"""
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValueError(f"{label} 必须为数值")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"{label} 必须为有限数值")
    return number


def _holdout_fdr(p_values: dict[str, float]) -> dict[str, float]:
    """按冻结候选全集重算 Benjamini-Hochberg q 值。"""
    ordered = sorted(p_values.items(), key=lambda item: (item[1], item[0]))
    result: dict[str, float] = {}
    running = 1.0
    count = len(ordered)
    for index in range(count - 1, -1, -1):
        candidate_id, p_value = ordered[index]
        rank = index + 1
        running = min(running, p_value * count / rank)
        result[candidate_id] = min(max(running, 0.0), 1.0)
    return result


def _validated_terminal_evidence(
    repository_root: Path,
    vintage_id: str,
    evaluation_id: str,
    verdict: str,
    result_manifest_path: str,
    result_manifest_sha256: str,
) -> _TerminalEvidence:
    """现场复核 manifest、result 与最终 verdict 的同一业务身份。"""
    if not _canonical_sha256(result_manifest_sha256):
        raise ValueError("holdout manifest SHA-256 必须是规范小写十六进制")
    manifest_path, normalized_path = _evidence_file(
        repository_root, result_manifest_path, "holdout manifest",
    )
    if sha256_file(manifest_path) != result_manifest_sha256:
        raise ValueError("holdout manifest 现场 SHA-256 不匹配")
    manifest = _json_file(manifest_path, "holdout manifest")
    if manifest.get("schema_version") != HOLDOUT_MANIFEST_SCHEMA_VERSION:
        raise ValueError("holdout manifest schema_version 不受支持")
    if manifest.get("holdout_method_version") != HOLDOUT_METHOD_VERSION:
        raise ValueError("holdout manifest method version 不匹配")
    try:
        verdict_value = json.loads(verdict)
    except json.JSONDecodeError as error:
        raise ValueError("holdout verdict 必须为 JSON 对象") from error
    if not isinstance(verdict_value, dict):
        raise ValueError("holdout verdict 必须为 JSON 对象")
    terminal = {str(key): item for key, item in verdict_value.items()}
    if manifest.get("vintage_id") != vintage_id:
        raise ValueError("holdout manifest 的 vintage_id 不匹配")
    if manifest.get("evaluation_id") != evaluation_id:
        raise ValueError("holdout manifest 的 evaluation_id 不匹配")
    if terminal.get("evaluation_id") != evaluation_id:
        raise ValueError("holdout verdict 的 evaluation_id 不匹配")
    if terminal.get("manifest_sha256") != result_manifest_sha256:
        raise ValueError("holdout verdict 未绑定现场 manifest SHA-256")
    terminal_verdict = terminal.get("verdict")
    if terminal_verdict not in ("passed", "failed"):
        raise ValueError("holdout verdict 只能是 passed 或 failed")
    if manifest.get("verdict") != terminal_verdict:
        raise ValueError("holdout manifest 与最终 verdict 不匹配")
    candidate_set_hash = manifest.get("candidate_set_hash")
    if not isinstance(candidate_set_hash, str) or not candidate_set_hash:
        raise ValueError("holdout manifest 缺少 candidate_set_hash")
    candidate_ids = _validated_candidate_set_identity(manifest, candidate_set_hash)
    config_hash = _validated_evaluation_identity(
        manifest, vintage_id, evaluation_id, candidate_set_hash,
    )
    evaluation_identity = manifest["evaluation_identity"]
    assert isinstance(evaluation_identity, dict)
    for name in (
        "input_head_generation", "input_attempt_ids", "input_artifact_ids",
        "normalization_versions",
    ):
        if manifest.get(name) != evaluation_identity.get(name):
            raise ValueError(f"holdout manifest 的 {name} 未绑定 evaluation_identity")
    if terminal.get("candidate_ids") != candidate_ids:
        raise ValueError("holdout verdict 未绑定冻结 candidate IDs")
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, dict):
        raise ValueError("holdout manifest 缺少 artifacts")
    config_path, config_artifact_sha256 = _validated_artifact(
        repository_root, artifacts, "config", "holdout_config",
    )
    if config_artifact_sha256 != config_hash:
        raise ValueError("holdout config 制品与 evaluation_identity 不匹配")
    receipt_path, receipt_sha256 = _validated_artifact(
        repository_root,
        artifacts,
        "input_receipt",
        "active_trade_head_receipt",
    )
    if (
        receipt_sha256 != evaluation_identity.get("input_receipt_sha256")
        or receipt_sha256 != manifest.get("input_receipt_sha256")
    ):
        raise ValueError("holdout 活动输入收据未绑定 evaluation_identity")
    _, panel_sha256 = _validated_artifact(
        repository_root, artifacts, "panel", "holdout_panel",
    )
    schedule_path, schedule_sha256 = _validated_artifact(
        repository_root,
        artifacts,
        "score_schedule",
        "holdout_score_schedule",
    )
    result_path, result_sha256 = _validated_artifact(
        repository_root, artifacts, "result", "holdout_result",
    )
    if terminal.get("result_sha256") != result_sha256:
        raise ValueError("holdout verdict 未绑定现场 result SHA-256")
    result = _json_file(result_path, "holdout result")
    if result.get("schema_version") != HOLDOUT_MANIFEST_SCHEMA_VERSION:
        raise ValueError("holdout result schema_version 不受支持")
    if result.get("holdout_method_version") != HOLDOUT_METHOD_VERSION:
        raise ValueError("holdout result method version 不匹配")
    vintage = result.get("vintage")
    if not isinstance(vintage, dict) or vintage.get("vintage_id") != vintage_id:
        raise ValueError("holdout result 的 vintage_id 不匹配")
    if result.get("evaluation_id") != evaluation_id:
        raise ValueError("holdout result 的 evaluation_id 不匹配")
    if result.get("candidate_set_hash") != candidate_set_hash:
        raise ValueError("holdout result 的 candidate_set_hash 不匹配")
    if result.get("config_hash") != config_hash:
        raise ValueError("holdout result 的 config_hash 不匹配")
    if result.get("panel_sha256") != panel_sha256:
        raise ValueError("holdout result 未绑定现场 panel SHA-256")
    if result.get("score_schedule_sha256") != schedule_sha256:
        raise ValueError("holdout result 未绑定现场 score schedule")
    if result.get("verdict") != terminal_verdict:
        raise ValueError("holdout result 与最终 verdict 不匹配")
    candidate_results = result.get("candidate_results")
    if not isinstance(candidate_results, list) or not candidate_results:
        raise ValueError("holdout result 缺少候选评估结果")
    if len(candidate_results) != len(candidate_ids):
        raise ValueError("holdout candidate_results 未覆盖冻结候选全集")
    config = _json_file(config_path, "holdout config")
    governance = config.get("data_governance")
    if not isinstance(governance, dict):
        raise ValueError("holdout config 缺少 data_governance")
    frozen_policy = governance.get("holdout_policy")
    if not isinstance(frozen_policy, dict):
        raise ValueError("holdout config 缺少 holdout_policy")
    policy = result.get("policy")
    if not isinstance(policy, dict) or policy != frozen_policy:
        raise ValueError("holdout result 缺少固定 policy")
    minimum_bars = policy.get("minimum_bars")
    if (
        not isinstance(minimum_bars, int)
        or isinstance(minimum_bars, bool)
        or minimum_bars <= 0
    ):
        raise ValueError("holdout policy.minimum_bars 无效")
    score_bars = result.get("score_bars")
    if (
        not isinstance(score_bars, int)
        or isinstance(score_bars, bool)
        or score_bars < minimum_bars
    ):
        raise ValueError("holdout score_bars 低于冻结门槛")
    score_start_value = result.get("score_start")
    score_end_value = result.get("score_end")
    if not isinstance(score_start_value, str) or not isinstance(score_end_value, str):
        raise ValueError("holdout score 时间范围无效")
    score_start = _parse_timestamp(score_start_value)
    score_end = _parse_timestamp(score_end_value)
    if score_start > score_end:
        raise ValueError("holdout score 时间范围倒置")
    schedule = _json_file(schedule_path, "holdout score schedule")
    if schedule.get("schema_version") != HOLDOUT_MANIFEST_SCHEMA_VERSION:
        raise ValueError("holdout score schedule schema_version 不受支持")
    if schedule.get("holdout_method_version") != HOLDOUT_METHOD_VERSION:
        raise ValueError("holdout score schedule method version 不匹配")
    if schedule.get("evaluation_id") != evaluation_id:
        raise ValueError("holdout score schedule evaluation_id 不匹配")
    raw_decision_times = schedule.get("decision_times")
    if (
        not isinstance(raw_decision_times, list)
        or len(raw_decision_times) != score_bars
        or any(not isinstance(item, str) for item in raw_decision_times)
    ):
        raise ValueError("holdout score schedule 未完整覆盖评分柱")
    decision_times = tuple(
        _parse_timestamp(str(item)) for item in raw_decision_times
    )
    if (
        decision_times != tuple(sorted(set(decision_times)))
        or decision_times[0] != score_start
        or decision_times[-1] != score_end
    ):
        raise ValueError("holdout score schedule 时点无序、重复或边界不一致")
    require_forward_predictions = policy.get("require_frozen_forward_predictions")
    if not isinstance(require_forward_predictions, bool):
        raise ValueError("holdout policy 必须明确冻结前向预测要求")
    target_source = result.get("target_source")
    forward_plan_id = result.get("frozen_forward_plan_id")
    forward_prediction_count = result.get("frozen_forward_prediction_count")
    forward_prediction_row_set_hash = result.get("frozen_forward_row_set_hash")
    if (
        forward_prediction_row_set_hash
        != manifest.get("frozen_forward_row_set_hash")
    ):
        raise ValueError("holdout 冻结预测 row-set 未绑定 manifest")
    if (
        not isinstance(forward_prediction_count, int)
        or isinstance(forward_prediction_count, bool)
        or forward_prediction_count < 0
    ):
        raise ValueError("holdout 冻结预测数量无效")
    if require_forward_predictions:
        if target_source != "recorded_frozen_forward":
            raise ValueError("holdout 必须使用预先记录的冻结前向目标")
        if not isinstance(forward_plan_id, str) or not forward_plan_id:
            raise ValueError("holdout 缺少冻结前向 plan_id")
        if forward_prediction_count != score_bars:
            raise ValueError("holdout 冻结预测数量必须完整等于评分柱数")
        if (
            not isinstance(forward_prediction_row_set_hash, str)
            or not forward_prediction_row_set_hash.startswith(
                "frozen-forward-row-set-"
            )
        ):
            raise ValueError("holdout 缺少冻结预测 row-set 身份")
    else:
        if target_source != "end_of_vintage_recompute":
            raise ValueError("holdout target_source 与冻结 policy 不一致")
        if forward_plan_id is not None or forward_prediction_count != 0:
            raise ValueError("holdout 非冻结前向模式不得声称预测覆盖")
        if forward_prediction_row_set_hash is not None:
            raise ValueError("holdout 非冻结前向模式不得绑定预测 row-set")
    minimum_sharpe = _finite_number(
        policy.get("minimum_sharpe"), "holdout policy.minimum_sharpe",
    )
    maximum_drawdown = _finite_number(
        policy.get("maximum_drawdown"), "holdout policy.maximum_drawdown",
    )
    maximum_fdr_q = _finite_number(
        policy.get("maximum_fdr_q"), "holdout policy.maximum_fdr_q",
    )
    if maximum_drawdown < 0.0 or not 0.0 <= maximum_fdr_q <= 1.0:
        raise ValueError("holdout policy 阈值范围无效")
    records: list[tuple[str, str, dict[str, object], dict[str, object]]] = []
    p_values: dict[str, float] = {}
    for item in candidate_results:
        if not isinstance(item, dict):
            raise ValueError("holdout candidate_results 合同无效")
        record = {str(key): value for key, value in item.items()}
        candidate_id = record.get("candidate_id")
        family = record.get("family")
        metrics_value = record.get("metrics")
        if (
            not isinstance(candidate_id, str)
            or not isinstance(family, str)
            or not family
            or not isinstance(metrics_value, dict)
        ):
            raise ValueError("holdout candidate_results 合同无效")
        metrics = {str(key): value for key, value in metrics_value.items()}
        p_value = _finite_number(
            metrics.get("p_value"), f"holdout {candidate_id}.p_value",
        )
        if not 0.0 <= p_value <= 1.0:
            raise ValueError("holdout candidate p_value 范围无效")
        if candidate_id in p_values:
            raise ValueError("holdout candidate_results 包含重复 candidate_id")
        p_values[candidate_id] = p_value
        records.append((candidate_id, family, metrics, record))
    if [record[0] for record in records] != candidate_ids:
        raise ValueError("holdout candidate_results 与冻结候选全集不一致")
    q_values = _holdout_fdr(p_values)
    candidate_outcomes: list[tuple[str, bool]] = []
    for candidate_id, family, metrics, record in records:
        reasons: list[str] = []
        if _finite_number(
            metrics.get("net_return"), f"holdout {candidate_id}.net_return",
        ) <= 0.0:
            reasons.append("non_positive_holdout_net_return")
        if _finite_number(
            metrics.get("sharpe"), f"holdout {candidate_id}.sharpe",
        ) < minimum_sharpe:
            reasons.append("holdout_sharpe_failed")
        if _finite_number(
            metrics.get("maximum_drawdown"),
            f"holdout {candidate_id}.maximum_drawdown",
        ) > maximum_drawdown:
            reasons.append("holdout_drawdown_failed")
        if q_values[candidate_id] > maximum_fdr_q:
            reasons.append("holdout_fdr_failed")
        stored_q = _finite_number(
            record.get("fdr_q"), f"holdout {candidate_id}.fdr_q",
        )
        if not math.isclose(
            stored_q, q_values[candidate_id], rel_tol=1e-12, abs_tol=1e-12,
        ):
            raise ValueError("holdout candidate fdr_q 现场重算不匹配")
        passed = not reasons
        if record.get("passed") is not passed:
            raise ValueError("holdout candidate passed 现场重算不匹配")
        if record.get("rejection_reasons") != reasons:
            raise ValueError("holdout candidate rejection_reasons 不匹配")
        candidate_outcomes.append((family, passed))
    passed_families = sorted(
        family for family, passed in candidate_outcomes if passed
    )
    expected_verdict = "passed" if all(
        passed for _, passed in candidate_outcomes
    ) else "failed"
    if expected_verdict != terminal_verdict:
        raise ValueError("holdout verdict 与候选评估结果不一致")
    if result.get("passed_families") != passed_families:
        raise ValueError("holdout result 的 passed_families 不一致")
    if terminal.get("passed_families") != passed_families:
        raise ValueError("holdout verdict 的 passed_families 不一致")
    from guvolu.research.holdout import attest_holdout_terminal_artifacts

    attest_holdout_terminal_artifacts(repository_root, manifest_path)
    return _TerminalEvidence(
        manifest_path=normalized_path,
        candidate_set_hash=candidate_set_hash,
        candidate_ids=tuple(candidate_ids),
        config_hash=config_hash,
        input_head_generation=str(evaluation_identity["input_head_generation"]),
        input_receipt_path=receipt_path.resolve().relative_to(
            repository_root.resolve()
        ).as_posix(),
        input_receipt_sha256=receipt_sha256,
        require_forward_predictions=require_forward_predictions,
        forward_plan_id=forward_plan_id,
        forward_prediction_count=forward_prediction_count,
        forward_prediction_row_set_hash=forward_prediction_row_set_hash,
        score_start=score_start,
        score_end=score_end,
        score_bars=score_bars,
        score_decision_times=decision_times,
    )


def _validate_range(start_time: datetime, end_time: datetime) -> tuple[datetime, datetime]:
    """验证左闭右开时间区间。"""
    start = _utc(start_time)
    end = _utc(end_time)
    if start >= end:
        raise ValueError("研究数据区间必须满足 start_time < end_time")
    return start, end


def _terminal_invariant_violation(
    connection: sqlite3.Connection,
) -> sqlite3.Row | None:
    """查找 vintage 与 evaluation attempt 的跨表非法终态。"""
    row: sqlite3.Row | None = connection.execute(
        """
        SELECT v.vintage_id
        FROM holdout_vintage AS v
        LEFT JOIN holdout_evaluation_attempt AS a
          ON a.vintage_id=v.vintage_id
        WHERE
          (v.status='sealed' AND a.evaluation_id IS NOT NULL)
          OR
          (v.status='consumed' AND (
            a.evaluation_id IS NULL
            OR a.evaluation_id<>v.evaluation_id
            OR a.candidate_set_hash<>v.candidate_set_hash
            OR (v.verdict IS NULL AND a.status<>'incomplete')
            OR (v.verdict IS NOT NULL AND a.status<>'completed')
            OR ((v.verdict IS NULL)<>(v.verdict_recorded_at IS NULL))
          ))
        LIMIT 1
        """
    ).fetchone()
    return row


def _upgrade_governance_state(
    connection: sqlite3.Connection,
    existing_version: str | None,
) -> None:
    """原子升级治理库，并把旧 consumed 记录变成可解释 incomplete 尝试。"""
    supported = {None, "1", "2", "3", "4", str(GOVERNANCE_SCHEMA_VERSION)}
    if existing_version not in supported:
        raise ValueError("不支持的研究治理注册表 schema_version")
    orphan_count = int(connection.execute(
        """
        SELECT COUNT(*)
        FROM holdout_vintage AS v
        LEFT JOIN holdout_evaluation_attempt AS a
          ON a.vintage_id=v.vintage_id
        WHERE v.status='consumed' AND a.evaluation_id IS NULL
        """
    ).fetchone()[0])
    needs_upgrade = existing_version != str(GOVERNANCE_SCHEMA_VERSION)
    if needs_upgrade or orphan_count:
        owns_transaction = not connection.in_transaction
        if owns_transaction:
            connection.execute("BEGIN IMMEDIATE")
        try:
            invalid_legacy = connection.execute(
                """
                SELECT v.vintage_id
                FROM holdout_vintage AS v
                LEFT JOIN holdout_evaluation_attempt AS a
                  ON a.vintage_id=v.vintage_id
                WHERE v.status='consumed' AND a.evaluation_id IS NULL
                  AND v.verdict IS NOT NULL
                LIMIT 1
                """
            ).fetchone()
            if invalid_legacy is not None:
                raise ValueError(
                    "旧治理库存在无 manifest attempt 的已判定 consumed vintage: "
                    + str(invalid_legacy["vintage_id"])
                )
            connection.execute(
                """
                INSERT INTO holdout_evaluation_attempt(
                  evaluation_id,vintage_id,candidate_set_hash,status,stage,
                  started_at,updated_at
                )
                SELECT
                  v.evaluation_id,v.vintage_id,v.candidate_set_hash,
                  'incomplete','legacy_consumed_without_attempt',
                  v.consumed_at,v.consumed_at
                FROM holdout_vintage AS v
                LEFT JOIN holdout_evaluation_attempt AS a
                  ON a.vintage_id=v.vintage_id
                WHERE v.status='consumed' AND v.verdict IS NULL
                  AND a.evaluation_id IS NULL
                """
            )
            violation = _terminal_invariant_violation(connection)
            if violation is not None:
                raise ValueError(
                    "治理库存在不一致的 holdout 终态: "
                    + str(violation["vintage_id"])
                )
            if existing_version is None:
                connection.execute(
                    "INSERT INTO governance_meta(key,value) "
                    "VALUES('schema_version',?)",
                    (str(GOVERNANCE_SCHEMA_VERSION),),
                )
            else:
                connection.execute(
                    "UPDATE governance_meta SET value=? WHERE key='schema_version'",
                    (str(GOVERNANCE_SCHEMA_VERSION),),
                )
            if owns_transaction:
                connection.execute("COMMIT")
        except BaseException:
            if owns_transaction and connection.in_transaction:
                connection.execute("ROLLBACK")
            raise
    violation = _terminal_invariant_violation(connection)
    if violation is not None:
        raise ValueError(
            "治理库存在不一致的 holdout 终态: "
            + str(violation["vintage_id"])
        )


def _schema_write_ceiling(
    connection: sqlite3.Connection,
) -> int | None:
    """读取可选的部署写入上限；旧 reader 会忽略这个附加元数据。"""
    table = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' "
        "AND name='governance_meta'"
    ).fetchone()
    if table is None:
        return None
    row = connection.execute(
        "SELECT value FROM governance_meta WHERE key=?",
        (SCHEMA_WRITE_CEILING_KEY,),
    ).fetchone()
    if row is None:
        return None
    try:
        ceiling = int(str(row["value"]))
    except ValueError as error:
        raise ValueError("治理库 schema 写入上限不是整数") from error
    if ceiling < 1 or ceiling > GOVERNANCE_SCHEMA_VERSION:
        raise ValueError("治理库 schema 写入上限超出支持范围")
    return ceiling


def _validate_active_head_receipt_schema(
    connection: sqlite3.Connection,
    *,
    probe_constraints: bool = False,
) -> None:
    """验证收据表列、复合主键和两项关键 CHECK 约束。"""
    rows = connection.execute(
        "PRAGMA table_info(active_head_receipt)"
    ).fetchall()
    expected = (
        ("consumer_kind", "TEXT", 1, 1),
        ("consumer_id", "TEXT", 1, 2),
        ("market_id", "TEXT", 1, 0),
        ("head_generation", "TEXT", 1, 0),
        ("receipt_artifact_path", "TEXT", 1, 0),
        ("receipt_artifact_sha256", "TEXT", 1, 0),
        ("recorded_at", "TEXT", 1, 0),
    )
    actual = tuple(
        (str(row[1]), str(row[2]).upper(), int(row[3]), int(row[5]))
        for row in rows
    )
    if actual != expected:
        raise ValueError("治理库 active_head_receipt 表结构不兼容")
    schema_row = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' "
        "AND name='active_head_receipt'"
    ).fetchone()
    normalized_schema = "" if schema_row is None else "".join(
        str(schema_row[0]).lower().split()
    )
    required_constraints = (
        "check(consumer_kindin('research','frozen_forward','holdout'))",
        "check(length(receipt_artifact_sha256)=64)",
    )
    if any(item not in normalized_schema for item in required_constraints):
        raise ValueError("治理库 active_head_receipt 缺少必要约束")
    if not probe_constraints:
        return

    def rejects(values: tuple[str, ...], suffix: str) -> bool:
        savepoint = f"active_head_receipt_probe_{suffix}"
        connection.execute(f"SAVEPOINT {savepoint}")
        try:
            try:
                connection.execute(
                    "INSERT INTO active_head_receipt("
                    "consumer_kind,consumer_id,market_id,head_generation,"
                    "receipt_artifact_path,receipt_artifact_sha256,recorded_at"
                    ") VALUES(?,?,?,?,?,?,?)",
                    values,
                )
            except sqlite3.IntegrityError:
                return True
            return False
        finally:
            connection.execute(f"ROLLBACK TO {savepoint}")
            connection.execute(f"RELEASE {savepoint}")

    probe = f"__guvolu_schema_probe_{id(connection)}"
    common = (probe, probe, probe)
    try:
        kind_rejected = rejects(
            ("invalid", f"{probe}_kind", *common, "0" * 64, probe),
            "kind",
        )
        hash_rejected = rejects(
            ("research", f"{probe}_hash", *common, "short", probe),
            "hash",
        )
    except sqlite3.DatabaseError as error:
        raise ValueError(
            "治理库 active_head_receipt 约束探针失败"
        ) from error
    if not kind_rejected or not hash_rejected:
        raise ValueError("治理库 active_head_receipt 缺少必要约束")


def upgrade_governance_write_ceiling(
    registry_path: Path,
    backup_path: Path,
    *,
    expected_version: int,
    expected_write_ceiling: int,
) -> Path:
    """备份并显式升级一个被旧部署固定写入版本的治理库。"""
    registry = registry_path.resolve()
    backup = backup_path.resolve()
    if not registry.is_file():
        raise FileNotFoundError(f"治理库不存在: {registry}")
    if backup == registry:
        raise ValueError("治理库备份路径不得指向原库")
    if backup.exists():
        raise FileExistsError(f"治理库备份已存在: {backup}")
    if expected_version < 1 or expected_version >= GOVERNANCE_SCHEMA_VERSION:
        raise ValueError("expected_version 必须是低于当前版本的正整数")
    if expected_write_ceiling != expected_version:
        raise ValueError("旧 schema 版本与写入上限必须一致")

    backup.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(registry, timeout=30.0, isolation_level=None)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    try:
        connection.execute("BEGIN IMMEDIATE")
        try:
            existing = connection.execute(
                "SELECT value FROM governance_meta WHERE key='schema_version'"
            ).fetchone()
            actual_version = (
                None if existing is None else int(str(existing["value"]))
            )
            actual_ceiling = _schema_write_ceiling(connection)
            if actual_version != expected_version:
                raise ValueError(
                    "治理库 schema 版本与显式预期不一致: "
                    f"expected={expected_version}, actual={actual_version}"
                )
            if actual_ceiling != expected_write_ceiling:
                raise ValueError(
                    "治理库写入上限与显式预期不一致: "
                    f"expected={expected_write_ceiling}, actual={actual_ceiling}"
                )
            _validate_pinned_read_schema(
                connection,
                str(actual_version),
                actual_ceiling,
            )

            backup_source = sqlite3.connect(registry, timeout=30.0)
            backup_connection = sqlite3.connect(backup)
            try:
                backup_source.backup(backup_connection)
                integrity = backup_connection.execute(
                    "PRAGMA integrity_check"
                ).fetchall()
                if integrity != [("ok",)]:
                    raise ValueError("治理库备份完整性检查失败")
            except BaseException:
                backup_connection.close()
                backup_source.close()
                if backup.exists():
                    backup.unlink()
                raise
            else:
                backup_connection.close()
                backup_source.close()

            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS active_head_receipt (
                  consumer_kind TEXT NOT NULL CHECK(
                    consumer_kind IN ('research','frozen_forward','holdout')
                  ),
                  consumer_id TEXT NOT NULL,
                  market_id TEXT NOT NULL,
                  head_generation TEXT NOT NULL,
                  receipt_artifact_path TEXT NOT NULL,
                  receipt_artifact_sha256 TEXT NOT NULL CHECK(
                    length(receipt_artifact_sha256)=64
                  ),
                  recorded_at TEXT NOT NULL,
                  PRIMARY KEY(consumer_kind,consumer_id)
                )
                """
            )
            _validate_active_head_receipt_schema(
                connection, probe_constraints=True,
            )
            _upgrade_governance_state(connection, str(actual_version))
            connection.execute(
                "UPDATE governance_meta SET value=? WHERE key=?",
                (str(GOVERNANCE_SCHEMA_VERSION), SCHEMA_WRITE_CEILING_KEY),
            )
            connection.execute("COMMIT")
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise
    finally:
        connection.close()

    verified = _connect(registry)
    verified.close()
    return backup


def _validate_pinned_read_schema(
    connection: sqlite3.Connection,
    existing_version: str | None,
    ceiling: int,
) -> None:
    """证明低版本标记下的物理表足以供当前 reader 无副作用读取。"""
    if existing_version != str(ceiling):
        raise ValueError("治理库 schema 标记与写入上限不一致")
    probes = (
        "SELECT key,value FROM governance_meta LIMIT 0",
        "SELECT exposure_id,research_identity,market_id,start_time,end_time,"
        "recorded_at FROM research_exposure LIMIT 0",
        "SELECT vintage_id,market_id,start_time,end_time,sealed_at,status,"
        "consumed_at,candidate_set_hash,evaluation_id,verdict,"
        "verdict_recorded_at FROM holdout_vintage LIMIT 0",
        "SELECT plan_id,vintage_id,source_manifest_sha256,candidate_set_hash,"
        "config_hash,code_tree_digest,plan_artifact_path,plan_artifact_sha256,"
        "frozen_at FROM frozen_forward_plan LIMIT 0",
        "SELECT evaluation_id,vintage_id,candidate_set_hash,status,stage,"
        "started_at,updated_at,completed_at,result_manifest_path,"
        "result_manifest_sha256 FROM holdout_evaluation_attempt LIMIT 0",
        "SELECT prediction_id,plan_id,vintage_id,decision_time,"
        "input_head_generation,panel_sha256,prediction_artifact_path,"
        "prediction_artifact_sha256,recorded_at "
        "FROM frozen_forward_prediction LIMIT 0",
    )
    try:
        for statement in probes:
            connection.execute(statement)
        violation = _terminal_invariant_violation(connection)
    except sqlite3.DatabaseError as error:
        raise ValueError(
            "治理库写入已冻结，但物理 schema 不兼容当前只读代码"
        ) from error
    if violation is not None:
        raise ValueError(
            "治理库存在不一致的 holdout 终态: "
            + str(violation["vintage_id"])
        )


def _connect(
    path: Path,
    *,
    write: bool = False,
    compatible_schema: int | None = None,
) -> sqlite3.Connection:
    """打开治理库；版本固定时只允许已证明的向下兼容操作。"""
    if compatible_schema is not None and not write:
        raise ValueError("compatible_schema 只适用于治理写入")
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30.0, isolation_level=None)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    try:
        ceiling = _schema_write_ceiling(connection)
        if ceiling is not None and GOVERNANCE_SCHEMA_VERSION > ceiling:
            existing = connection.execute(
                "SELECT value FROM governance_meta WHERE key='schema_version'"
            ).fetchone()
            if write and compatible_schema != ceiling:
                raise ValueError(
                    "治理库 schema 写入已冻结在版本 " + str(ceiling)
                )
            _validate_pinned_read_schema(
                connection,
                None if existing is None else str(existing["value"]),
                ceiling,
            )
            if write:
                connection.execute("PRAGMA synchronous=FULL")
            return connection
    except BaseException:
        connection.close()
        raise
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS governance_meta (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS research_exposure (
          exposure_id TEXT PRIMARY KEY,
          research_identity TEXT NOT NULL UNIQUE,
          market_id TEXT NOT NULL,
          start_time TEXT NOT NULL,
          end_time TEXT NOT NULL,
          recorded_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS holdout_vintage (
          vintage_id TEXT PRIMARY KEY,
          market_id TEXT NOT NULL,
          start_time TEXT NOT NULL,
          end_time TEXT NOT NULL,
          sealed_at TEXT NOT NULL,
          status TEXT NOT NULL CHECK(status IN ('sealed','consumed')),
          consumed_at TEXT,
          candidate_set_hash TEXT,
          evaluation_id TEXT,
          verdict TEXT,
          verdict_recorded_at TEXT,
          CHECK(
            (status='sealed' AND consumed_at IS NULL
             AND candidate_set_hash IS NULL AND evaluation_id IS NULL)
            OR
            (status='consumed' AND consumed_at IS NOT NULL
             AND candidate_set_hash IS NOT NULL AND evaluation_id IS NOT NULL)
          )
        );
        CREATE UNIQUE INDEX IF NOT EXISTS holdout_vintage_range
          ON holdout_vintage(market_id,start_time,end_time);
        CREATE TABLE IF NOT EXISTS frozen_forward_plan (
          plan_id TEXT PRIMARY KEY,
          vintage_id TEXT NOT NULL UNIQUE,
          source_manifest_sha256 TEXT NOT NULL,
          candidate_set_hash TEXT NOT NULL,
          config_hash TEXT NOT NULL,
          code_tree_digest TEXT NOT NULL,
          plan_artifact_path TEXT NOT NULL,
          plan_artifact_sha256 TEXT NOT NULL,
          frozen_at TEXT NOT NULL,
          FOREIGN KEY(vintage_id) REFERENCES holdout_vintage(vintage_id)
        );
        CREATE TABLE IF NOT EXISTS holdout_evaluation_attempt (
          evaluation_id TEXT PRIMARY KEY,
          vintage_id TEXT NOT NULL UNIQUE,
          candidate_set_hash TEXT NOT NULL,
          status TEXT NOT NULL CHECK(status IN ('incomplete','completed')),
          stage TEXT NOT NULL,
          started_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          completed_at TEXT,
          result_manifest_path TEXT,
          result_manifest_sha256 TEXT,
          CHECK(
            (status='incomplete' AND completed_at IS NULL
             AND result_manifest_path IS NULL
             AND result_manifest_sha256 IS NULL)
            OR
            (status='completed' AND completed_at IS NOT NULL
             AND result_manifest_path IS NOT NULL
             AND result_manifest_sha256 IS NOT NULL
             AND length(result_manifest_sha256)=64)
          ),
          FOREIGN KEY(vintage_id) REFERENCES holdout_vintage(vintage_id)
        );
        CREATE TABLE IF NOT EXISTS frozen_forward_prediction (
          prediction_id TEXT PRIMARY KEY,
          plan_id TEXT NOT NULL,
          vintage_id TEXT NOT NULL,
          decision_time TEXT NOT NULL,
          input_head_generation TEXT NOT NULL,
          panel_sha256 TEXT NOT NULL,
          prediction_artifact_path TEXT NOT NULL,
          prediction_artifact_sha256 TEXT NOT NULL,
          recorded_at TEXT NOT NULL,
          FOREIGN KEY(plan_id) REFERENCES frozen_forward_plan(plan_id),
          FOREIGN KEY(vintage_id) REFERENCES holdout_vintage(vintage_id),
          UNIQUE(plan_id,decision_time)
        );
        CREATE TABLE IF NOT EXISTS active_head_receipt (
          consumer_kind TEXT NOT NULL CHECK(
            consumer_kind IN ('research','frozen_forward','holdout')
          ),
          consumer_id TEXT NOT NULL,
          market_id TEXT NOT NULL,
          head_generation TEXT NOT NULL,
          receipt_artifact_path TEXT NOT NULL,
          receipt_artifact_sha256 TEXT NOT NULL CHECK(
            length(receipt_artifact_sha256)=64
          ),
          recorded_at TEXT NOT NULL,
          PRIMARY KEY(consumer_kind,consumer_id)
        );
        """
    )
    _validate_active_head_receipt_schema(connection)
    existing = connection.execute(
        "SELECT value FROM governance_meta WHERE key='schema_version'"
    ).fetchone()
    try:
        _upgrade_governance_state(
            connection,
            None if existing is None else str(existing["value"]),
        )
    except BaseException:
        connection.close()
        raise
    return connection


def _begin(connection: sqlite3.Connection) -> None:
    """以写锁开始原子治理事务。"""
    connection.execute("BEGIN IMMEDIATE")


def _overlap_clause() -> str:
    """返回左闭右开区间重叠条件。"""
    return "market_id=? AND start_time<? AND end_time>?"


def _active_head_receipt_from_row(
    row: sqlite3.Row,
) -> ActiveHeadReceiptRegistration:
    """把治理库行转换为活动输入收据登记。"""
    return ActiveHeadReceiptRegistration(
        consumer_kind=str(row["consumer_kind"]),
        consumer_id=str(row["consumer_id"]),
        market_id=str(row["market_id"]),
        head_generation=str(row["head_generation"]),
        receipt_artifact_path=str(row["receipt_artifact_path"]),
        receipt_artifact_sha256=str(row["receipt_artifact_sha256"]),
        recorded_at=_parse_timestamp(str(row["recorded_at"])),
    )


def _frozen_forward_prediction_row_set(
    connection: sqlite3.Connection,
    plan_id: str,
) -> tuple[str, tuple[datetime, ...]]:
    """从不可改写预测行及其活动收据生成确定性 row-set 身份。"""
    rows = connection.execute(
        "SELECT * FROM frozen_forward_prediction WHERE plan_id=? "
        "ORDER BY decision_time",
        (plan_id,),
    ).fetchall()
    records: list[dict[str, object]] = []
    decision_times: list[datetime] = []
    for row in rows:
        prediction_id = str(row["prediction_id"])
        receipt = connection.execute(
            "SELECT * FROM active_head_receipt "
            "WHERE consumer_kind='frozen_forward' AND consumer_id=?",
            (prediction_id,),
        ).fetchone()
        if receipt is None:
            raise ValueError("冻结预测 row-set 缺少活动输入收据")
        decision_time = _parse_timestamp(str(row["decision_time"]))
        decision_times.append(decision_time)
        records.append({
            "prediction_id": prediction_id,
            "decision_time": decision_time.isoformat(),
            "input_head_generation": str(row["input_head_generation"]),
            "panel_sha256": str(row["panel_sha256"]),
            "prediction_artifact_path": str(row["prediction_artifact_path"]),
            "prediction_artifact_sha256": str(row["prediction_artifact_sha256"]),
            "recorded_at": _parse_timestamp(str(row["recorded_at"])).isoformat(),
            "receipt_artifact_path": str(receipt["receipt_artifact_path"]),
            "receipt_artifact_sha256": str(receipt["receipt_artifact_sha256"]),
        })
    identity = {
        "method_version": "frozen-forward-registered-row-set-v1",
        "plan_id": plan_id,
        "rows": records,
    }
    return stable_identifier("frozen-forward-row-set", identity), tuple(decision_times)


def get_frozen_forward_prediction_row_set(
    registry_path: Path,
    plan_id: str,
) -> tuple[str, int, tuple[datetime, ...]]:
    """读取已登记预测全集的确定性散列、数量与有序决策时点。"""
    connection = _connect(registry_path)
    try:
        row_set_hash, decision_times = _frozen_forward_prediction_row_set(
            connection, plan_id,
        )
    finally:
        connection.close()
    return row_set_hash, len(decision_times), decision_times


def register_active_head_receipt(
    registry_path: Path,
    consumer_kind: str,
    consumer_id: str,
    market_id: str,
    head_generation: str,
    receipt_artifact_path: str,
    receipt_artifact_sha256: str,
    *,
    repository_root: Path,
    data_root: Path,
) -> ActiveHeadReceiptRegistration:
    """仅在收据等于完整当前 head 时将其不可改写地绑定消费者。"""
    if consumer_kind not in {"research", "frozen_forward", "holdout"}:
        raise ValueError("活动输入收据 consumer_kind 不受支持")
    if not consumer_id:
        raise ValueError("活动输入收据 consumer_id 不能为空")
    if not _canonical_sha256(receipt_artifact_sha256):
        raise ValueError("活动输入收据 SHA-256 必须为规范小写文本")
    receipt_path, normalized_path = _evidence_file(
        repository_root, receipt_artifact_path, "活动输入收据",
    )
    if sha256_file(receipt_path) != receipt_artifact_sha256:
        raise ValueError("活动输入收据现场 SHA-256 不匹配")
    from guvolu.research.panel import attest_trade_input_receipt

    inputs = attest_trade_input_receipt(
        data_root, receipt_path, require_current_head=True,
    )
    if (
        str(inputs.market.get("market_id")) != market_id
        or inputs.head_generation != head_generation
    ):
        raise ValueError("活动输入收据与消费者声明的市场或 head 不一致")
    recorded = _utc(clock.utc_now())
    expected = ActiveHeadReceiptRegistration(
        consumer_kind=consumer_kind,
        consumer_id=consumer_id,
        market_id=market_id,
        head_generation=head_generation,
        receipt_artifact_path=normalized_path,
        receipt_artifact_sha256=receipt_artifact_sha256,
        recorded_at=recorded,
    )
    connection = _connect(registry_path, write=True)
    try:
        _begin(connection)
        existing = connection.execute(
            "SELECT * FROM active_head_receipt "
            "WHERE consumer_kind=? AND consumer_id=?",
            (consumer_kind, consumer_id),
        ).fetchone()
        if existing is not None:
            registered = _active_head_receipt_from_row(existing)
            if (
                registered.consumer_kind != expected.consumer_kind
                or registered.consumer_id != expected.consumer_id
                or registered.market_id != expected.market_id
                or registered.head_generation != expected.head_generation
                or registered.receipt_artifact_path
                != expected.receipt_artifact_path
                or registered.receipt_artifact_sha256
                != expected.receipt_artifact_sha256
            ):
                raise ValueError("消费者已绑定另一份活动输入收据")
            connection.execute("COMMIT")
            return registered
        connection.execute(
            "INSERT INTO active_head_receipt("
            "consumer_kind,consumer_id,market_id,head_generation,"
            "receipt_artifact_path,receipt_artifact_sha256,recorded_at"
            ") VALUES(?,?,?,?,?,?,?)",
            (
                consumer_kind, consumer_id, market_id, head_generation,
                normalized_path, receipt_artifact_sha256, _timestamp(recorded),
            ),
        )
        connection.execute("COMMIT")
        return expected
    except BaseException:
        if connection.in_transaction:
            connection.execute("ROLLBACK")
        raise
    finally:
        connection.close()


def get_active_head_receipt(
    registry_path: Path,
    consumer_kind: str,
    consumer_id: str,
) -> ActiveHeadReceiptRegistration:
    """读取消费者已经不可改写登记的活动输入收据。"""
    connection = _connect(registry_path)
    try:
        row = connection.execute(
            "SELECT * FROM active_head_receipt "
            "WHERE consumer_kind=? AND consumer_id=?",
            (consumer_kind, consumer_id),
        ).fetchone()
    finally:
        connection.close()
    if row is None:
        raise LookupError("消费者没有登记活动输入收据")
    return _active_head_receipt_from_row(row)


def register_research_exposure(
    registry_path: Path,
    research_identity: str,
    market_id: str,
    start_time: datetime,
    end_time: datetime,
) -> ResearchExposure:
    """登记开发研究暴露；未消费封存段与研究读取互斥。"""
    start, end = _validate_range(start_time, end_time)
    recorded = _utc(clock.utc_now())
    exposure_id = stable_identifier("research-exposure", {
        "governance_method_version": GOVERNANCE_METHOD_VERSION,
        "research_identity": research_identity,
        "market_id": market_id,
        "start_time": start.isoformat(),
        "end_time": end.isoformat(),
    })
    connection = _connect(
        registry_path,
        write=True,
        compatible_schema=2,
    )
    try:
        _begin(connection)
        protected = connection.execute(
            "SELECT vintage_id FROM holdout_vintage WHERE status='sealed' AND "
            + _overlap_clause() + " LIMIT 1",
            (market_id, _timestamp(end), _timestamp(start)),
        ).fetchone()
        if protected is not None:
            raise ValueError(
                "开发研究区间与未消费封存段重叠: " + str(protected["vintage_id"])
            )
        existing = connection.execute(
            "SELECT * FROM research_exposure WHERE research_identity=?",
            (research_identity,),
        ).fetchone()
        if existing is not None:
            expected = (
                exposure_id,
                market_id,
                _timestamp(start),
                _timestamp(end),
            )
            actual = (
                existing["exposure_id"],
                existing["market_id"],
                existing["start_time"],
                existing["end_time"],
            )
            if actual != expected:
                raise ValueError("同一 research_identity 的数据暴露身份不一致")
            connection.execute("COMMIT")
            return _exposure_from_row(existing)
        connection.execute(
            "INSERT INTO research_exposure("
            "exposure_id,research_identity,market_id,start_time,end_time,recorded_at"
            ") VALUES(?,?,?,?,?,?)",
            (
                exposure_id,
                research_identity,
                market_id,
                _timestamp(start),
                _timestamp(end),
                _timestamp(recorded),
            ),
        )
        connection.execute("COMMIT")
    except BaseException:
        if connection.in_transaction:
            connection.execute("ROLLBACK")
        raise
    finally:
        connection.close()
    return ResearchExposure(
        exposure_id=exposure_id,
        research_identity=research_identity,
        market_id=market_id,
        start_time=start,
        end_time=end,
        recorded_at=recorded,
    )


def seal_holdout_vintage(
    registry_path: Path,
    market_id: str,
    start_time: datetime,
    end_time: datetime,
) -> HoldoutVintage:
    """封存尚未被任何自适应研究读取且不重叠的新数据段。"""
    start, end = _validate_range(start_time, end_time)
    sealed = clock.utc_now()
    if sealed > start:
        raise ValueError("封存段必须在区间开始前登记，禁止事后挑选 holdout")
    vintage_id = stable_identifier("holdout-vintage", {
        "governance_method_version": GOVERNANCE_METHOD_VERSION,
        "market_id": market_id,
        "start_time": start.isoformat(),
        "end_time": end.isoformat(),
    })
    connection = _connect(registry_path, write=True)
    try:
        _begin(connection)
        exposed = connection.execute(
            "SELECT exposure_id FROM research_exposure WHERE "
            + _overlap_clause() + " LIMIT 1",
            (market_id, _timestamp(end), _timestamp(start)),
        ).fetchone()
        if exposed is not None:
            raise ValueError(
                "封存段已被自适应研究读取: " + str(exposed["exposure_id"])
            )
        overlap = connection.execute(
            "SELECT vintage_id FROM holdout_vintage WHERE "
            + _overlap_clause() + " LIMIT 1",
            (market_id, _timestamp(end), _timestamp(start)),
        ).fetchone()
        if overlap is not None:
            existing = connection.execute(
                "SELECT * FROM holdout_vintage WHERE vintage_id=?",
                (vintage_id,),
            ).fetchone()
            if existing is not None:
                connection.execute("COMMIT")
                return _vintage_from_row(existing)
            raise ValueError("封存段与既有 vintage 重叠: " + str(overlap["vintage_id"]))
        connection.execute(
            "INSERT INTO holdout_vintage("
            "vintage_id,market_id,start_time,end_time,sealed_at,status"
            ") VALUES(?,?,?,?,?,'sealed')",
            (
                vintage_id,
                market_id,
                _timestamp(start),
                _timestamp(end),
                _timestamp(sealed),
            ),
        )
        row = connection.execute(
            "SELECT * FROM holdout_vintage WHERE vintage_id=?",
            (vintage_id,),
        ).fetchone()
        connection.execute("COMMIT")
    except BaseException:
        if connection.in_transaction:
            connection.execute("ROLLBACK")
        raise
    finally:
        connection.close()
    if row is None:
        raise RuntimeError("封存段写入后不可见")
    return _vintage_from_row(row)


def start_holdout_evaluation_attempt(
    registry_path: Path,
    vintage_id: str,
    candidate_set_hash: str,
    evaluation_id: str,
) -> HoldoutEvaluationAttempt:
    """原子烧毁 vintage 并登记不可重跑的评估尝试。"""
    if not candidate_set_hash or not evaluation_id:
        raise ValueError("评估封存段必须绑定 candidate_set_hash 与 evaluation_id")
    started = clock.utc_now()
    connection = _connect(registry_path, write=True)
    try:
        _begin(connection)
        vintage = connection.execute(
            "SELECT * FROM holdout_vintage WHERE vintage_id=?", (vintage_id,),
        ).fetchone()
        if vintage is None:
            raise LookupError(f"封存段不存在: {vintage_id}")
        if vintage["status"] != "sealed":
            raise ValueError(f"封存段已经消费: {vintage_id}")
        if started < _parse_timestamp(str(vintage["end_time"])):
            raise ValueError("封存段必须完整结束后才能开始评估")
        connection.execute(
            "UPDATE holdout_vintage SET status='consumed',consumed_at=?,"
            "candidate_set_hash=?,evaluation_id=? WHERE vintage_id=? AND status='sealed'",
            (_timestamp(started), candidate_set_hash, evaluation_id, vintage_id),
        )
        connection.execute(
            "INSERT INTO holdout_evaluation_attempt("
            "evaluation_id,vintage_id,candidate_set_hash,status,stage,started_at,updated_at"
            ") VALUES(?,?,?,'incomplete','vintage_consumed',?,?)",
            (
                evaluation_id,
                vintage_id,
                candidate_set_hash,
                _timestamp(started),
                _timestamp(started),
            ),
        )
        row = connection.execute(
            "SELECT * FROM holdout_evaluation_attempt WHERE evaluation_id=?",
            (evaluation_id,),
        ).fetchone()
        connection.execute("COMMIT")
    except BaseException:
        if connection.in_transaction:
            connection.execute("ROLLBACK")
        raise
    finally:
        connection.close()
    if row is None:
        raise RuntimeError("holdout 评估尝试写入后不可见")
    return _attempt_from_row(row)


def update_holdout_evaluation_attempt(
    registry_path: Path,
    evaluation_id: str,
    stage: str,
) -> HoldoutEvaluationAttempt:
    """持久化 incomplete 尝试最后到达的评估阶段。"""
    normalized = stage.strip()
    if not normalized:
        raise ValueError("holdout 评估阶段不得为空")
    updated = clock.utc_now()
    connection = _connect(registry_path, write=True)
    try:
        _begin(connection)
        row = connection.execute(
            "SELECT * FROM holdout_evaluation_attempt WHERE evaluation_id=?",
            (evaluation_id,),
        ).fetchone()
        if row is None:
            raise LookupError(f"holdout 评估尝试不存在: {evaluation_id}")
        if row["status"] != "incomplete":
            raise ValueError("已完成 holdout 评估尝试不可修改")
        connection.execute(
            "UPDATE holdout_evaluation_attempt SET stage=?,updated_at=? "
            "WHERE evaluation_id=? AND status='incomplete'",
            (normalized, _timestamp(updated), evaluation_id),
        )
        current = connection.execute(
            "SELECT * FROM holdout_evaluation_attempt WHERE evaluation_id=?",
            (evaluation_id,),
        ).fetchone()
        connection.execute("COMMIT")
    except BaseException:
        if connection.in_transaction:
            connection.execute("ROLLBACK")
        raise
    finally:
        connection.close()
    if current is None:
        raise RuntimeError("holdout 评估阶段更新后不可见")
    return _attempt_from_row(current)


def finalize_holdout_evaluation(
    registry_path: Path,
    vintage_id: str,
    evaluation_id: str,
    verdict: str,
    result_manifest_path: str,
    result_manifest_sha256: str,
    *,
    repository_root: Path,
) -> tuple[HoldoutVintage, HoldoutEvaluationAttempt]:
    """现场复核终态证据，再原子写入 verdict 与 completed attempt。"""
    normalized = verdict.strip()
    if not normalized or not result_manifest_path:
        raise ValueError("完成 holdout 必须绑定 verdict 与 manifest 身份")
    evidence = _validated_terminal_evidence(
        repository_root,
        vintage_id,
        evaluation_id,
        normalized,
        result_manifest_path,
        result_manifest_sha256,
    )
    completed = clock.utc_now()
    connection = _connect(registry_path, write=True)
    try:
        _begin(connection)
        vintage = connection.execute(
            "SELECT * FROM holdout_vintage WHERE vintage_id=?", (vintage_id,),
        ).fetchone()
        attempt = connection.execute(
            "SELECT * FROM holdout_evaluation_attempt WHERE evaluation_id=?",
            (evaluation_id,),
        ).fetchone()
        if vintage is None or attempt is None:
            raise LookupError("holdout vintage 或评估尝试不存在")
        if vintage["status"] != "consumed" or vintage["evaluation_id"] != evaluation_id:
            raise ValueError("holdout vintage 与评估尝试身份不一致")
        if attempt["vintage_id"] != vintage_id:
            raise ValueError("holdout 评估尝试绑定了不同 vintage")
        if (
            vintage["candidate_set_hash"] != evidence.candidate_set_hash
            or attempt["candidate_set_hash"] != evidence.candidate_set_hash
        ):
            raise ValueError("holdout manifest 的 candidate_set_hash 与注册表不匹配")
        receipt = connection.execute(
            "SELECT * FROM active_head_receipt "
            "WHERE consumer_kind='holdout' AND consumer_id=?",
            (evaluation_id,),
        ).fetchone()
        if (
            receipt is None
            or receipt["market_id"] != vintage["market_id"]
            or receipt["head_generation"] != evidence.input_head_generation
            or receipt["receipt_artifact_path"] != evidence.input_receipt_path
            or receipt["receipt_artifact_sha256"] != evidence.input_receipt_sha256
        ):
            raise ValueError("holdout 终态缺少匹配的活动输入收据登记")
        vintage_start = _parse_timestamp(str(vintage["start_time"]))
        vintage_end = _parse_timestamp(str(vintage["end_time"]))
        if evidence.score_start < vintage_start or evidence.score_end > vintage_end:
            raise ValueError("holdout score 时间范围超出绑定 vintage")
        plan = connection.execute(
            "SELECT * FROM frozen_forward_plan WHERE vintage_id=?",
            (vintage_id,),
        ).fetchone()
        if plan is not None:
            if (
                not evidence.require_forward_predictions
                or plan["plan_id"] != evidence.forward_plan_id
            ):
                raise ValueError("holdout 冻结前向 plan 与注册表不一致")
            assert evidence.forward_plan_id is not None
            if (
                plan["candidate_set_hash"] != evidence.candidate_set_hash
                or plan["config_hash"] != evidence.config_hash
            ):
                raise ValueError("holdout 冻结前向 plan 身份不匹配")
            _validated_forward_plan_artifact(
                repository_root,
                str(plan["plan_id"]),
                vintage_id,
                str(plan["source_manifest_sha256"]),
                str(plan["candidate_set_hash"]),
                str(plan["config_hash"]),
                str(plan["code_tree_digest"]),
                str(plan["plan_artifact_path"]),
                str(plan["plan_artifact_sha256"]),
                evidence.candidate_ids,
            )
            row_set_hash, prediction_times = _frozen_forward_prediction_row_set(
                connection, evidence.forward_plan_id,
            )
            if len(prediction_times) != evidence.forward_prediction_count:
                raise ValueError("holdout 冻结预测数量与注册表不一致")
            if prediction_times != evidence.score_decision_times:
                raise ValueError("holdout 冻结预测时点未逐柱匹配评分面板")
            if row_set_hash != evidence.forward_prediction_row_set_hash:
                raise ValueError("holdout 冻结预测 row-set 与注册表不一致")
        elif evidence.require_forward_predictions:
            raise ValueError("holdout 要求冻结前向预测但注册表没有 plan")
        if vintage["verdict"] is not None or attempt["status"] != "incomplete":
            raise ValueError("holdout 已经终结且不可改写")
        connection.execute(
            "UPDATE holdout_vintage SET verdict=?,verdict_recorded_at=? "
            "WHERE vintage_id=? AND verdict IS NULL",
            (normalized, _timestamp(completed), vintage_id),
        )
        connection.execute(
            "UPDATE holdout_evaluation_attempt SET status='completed',stage='completed',"
            "updated_at=?,completed_at=?,result_manifest_path=?,"
            "result_manifest_sha256=? WHERE evaluation_id=? AND status='incomplete'",
            (
                _timestamp(completed),
                _timestamp(completed),
                evidence.manifest_path,
                result_manifest_sha256,
                evaluation_id,
            ),
        )
        final_vintage = connection.execute(
            "SELECT * FROM holdout_vintage WHERE vintage_id=?", (vintage_id,),
        ).fetchone()
        final_attempt = connection.execute(
            "SELECT * FROM holdout_evaluation_attempt WHERE evaluation_id=?",
            (evaluation_id,),
        ).fetchone()
        connection.execute("COMMIT")
    except BaseException:
        if connection.in_transaction:
            connection.execute("ROLLBACK")
        raise
    finally:
        connection.close()
    if final_vintage is None or final_attempt is None:
        raise RuntimeError("holdout 终态写入后不可见")
    return _vintage_from_row(final_vintage), _attempt_from_row(final_attempt)


def get_holdout_evaluation_attempt(
    registry_path: Path,
    evaluation_id: str,
) -> HoldoutEvaluationAttempt:
    """读取评估尝试，包括永久 incomplete 状态。"""
    connection = _connect(registry_path)
    try:
        row = connection.execute(
            "SELECT * FROM holdout_evaluation_attempt WHERE evaluation_id=?",
            (evaluation_id,),
        ).fetchone()
    finally:
        connection.close()
    if row is None:
        raise LookupError(f"holdout 评估尝试不存在: {evaluation_id}")
    return _attempt_from_row(row)


def list_holdout_vintages(registry_path: Path) -> tuple[HoldoutVintage, ...]:
    """按时间列出所有封存段，包括已消费历史。"""
    connection = _connect(registry_path)
    try:
        rows = connection.execute(
            "SELECT * FROM holdout_vintage ORDER BY start_time,vintage_id"
        ).fetchall()
    finally:
        connection.close()
    return tuple(_vintage_from_row(row) for row in rows)


def get_holdout_vintage(
    registry_path: Path,
    vintage_id: str,
) -> HoldoutVintage:
    """按不可变身份读取一个封存段。"""
    connection = _connect(registry_path)
    try:
        row = connection.execute(
            "SELECT * FROM holdout_vintage WHERE vintage_id=?",
            (vintage_id,),
        ).fetchone()
    finally:
        connection.close()
    if row is None:
        raise LookupError(f"封存段不存在: {vintage_id}")
    return _vintage_from_row(row)


def get_research_exposure(
    registry_path: Path,
    exposure_id: str,
) -> ResearchExposure:
    """按不可变身份读取一条研究暴露。"""
    connection = _connect(registry_path)
    try:
        row = connection.execute(
            "SELECT * FROM research_exposure WHERE exposure_id=?",
            (exposure_id,),
        ).fetchone()
    finally:
        connection.close()
    if row is None:
        raise LookupError(f"研究暴露不存在: {exposure_id}")
    return _exposure_from_row(row)


def register_frozen_forward_plan(
    registry_path: Path,
    vintage_id: str,
    source_manifest_sha256: str,
    candidate_set_hash: str,
    config_hash: str,
    code_tree_digest: str,
    plan_artifact_path: str,
    plan_artifact_sha256: str,
    *,
    repository_root: Path,
) -> FrozenForwardPlan:
    """在 vintage 开始前原子登记唯一冻结前向计划。"""
    values = (
        source_manifest_sha256,
        candidate_set_hash,
        config_hash,
        code_tree_digest,
        plan_artifact_path,
        plan_artifact_sha256,
    )
    if any(not value.strip() for value in values):
        raise ValueError("冻结前向计划身份字段不得为空")
    frozen = clock.utc_now()
    plan_id = stable_identifier("frozen-forward-plan", {
        "governance_method_version": GOVERNANCE_METHOD_VERSION,
        "vintage_id": vintage_id,
        "source_manifest_sha256": source_manifest_sha256,
        "candidate_set_hash": candidate_set_hash,
        "config_hash": config_hash,
        "code_tree_digest": code_tree_digest,
    })
    plan_evidence = _validated_forward_plan_artifact(
        repository_root,
        plan_id,
        vintage_id,
        source_manifest_sha256,
        candidate_set_hash,
        config_hash,
        code_tree_digest,
        plan_artifact_path,
        plan_artifact_sha256,
    )
    values = (
        source_manifest_sha256,
        candidate_set_hash,
        config_hash,
        code_tree_digest,
        plan_evidence.normalized_path,
        plan_artifact_sha256,
    )
    connection = _connect(registry_path, write=True)
    try:
        _begin(connection)
        vintage = connection.execute(
            "SELECT * FROM holdout_vintage WHERE vintage_id=?", (vintage_id,),
        ).fetchone()
        if vintage is None:
            raise LookupError(f"封存段不存在: {vintage_id}")
        if vintage["status"] != "sealed":
            raise ValueError("冻结前向计划只能绑定未消费 vintage")
        if frozen > _parse_timestamp(str(vintage["start_time"])):
            raise ValueError("冻结前向计划必须在 vintage 开始前登记")
        existing = connection.execute(
            "SELECT * FROM frozen_forward_plan WHERE vintage_id=?", (vintage_id,),
        ).fetchone()
        if existing is not None:
            expected = (plan_id, *values)
            actual = (
                existing["plan_id"], existing["source_manifest_sha256"],
                existing["candidate_set_hash"], existing["config_hash"],
                existing["code_tree_digest"], existing["plan_artifact_path"],
                existing["plan_artifact_sha256"],
            )
            if actual != expected:
                raise ValueError("vintage 已绑定不同的冻结前向计划")
            connection.execute("COMMIT")
            return _plan_from_row(existing)
        connection.execute(
            "INSERT INTO frozen_forward_plan("
            "plan_id,vintage_id,source_manifest_sha256,candidate_set_hash,"
            "config_hash,code_tree_digest,plan_artifact_path,"
            "plan_artifact_sha256,frozen_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (plan_id, vintage_id, *values, _timestamp(frozen)),
        )
        row = connection.execute(
            "SELECT * FROM frozen_forward_plan WHERE plan_id=?", (plan_id,),
        ).fetchone()
        connection.execute("COMMIT")
    except BaseException:
        if connection.in_transaction:
            connection.execute("ROLLBACK")
        raise
    finally:
        connection.close()
    if row is None:
        raise RuntimeError("冻结前向计划写入后不可见")
    return _plan_from_row(row)


def register_frozen_forward_prediction(
    registry_path: Path,
    plan_id: str,
    decision_time: datetime,
    input_head_generation: str,
    panel_sha256: str,
    prediction_artifact_path: str,
    prediction_artifact_sha256: str,
    maximum_recording_lag_seconds: int,
    *,
    repository_root: Path,
) -> FrozenForwardPrediction:
    """原子追加一个及时生成的预测；同一时点内容永久不可改写。"""
    if maximum_recording_lag_seconds <= 0:
        raise ValueError("预测登记时效阈值必须为正数")
    identity_values = (input_head_generation, panel_sha256, prediction_artifact_path)
    if any(not value.strip() for value in identity_values):
        raise ValueError("冻结前向预测身份字段不得为空")
    decision = _utc(decision_time)
    recorded = clock.utc_now()
    lag = (recorded - decision).total_seconds()
    if lag < 0 or lag > maximum_recording_lag_seconds:
        raise ValueError("冻结前向预测未在预登记时效窗口内生成")
    connection = _connect(registry_path, write=True)
    try:
        _begin(connection)
        plan = connection.execute(
            "SELECT * FROM frozen_forward_plan WHERE plan_id=?", (plan_id,),
        ).fetchone()
        if plan is None:
            raise LookupError(f"冻结前向计划不存在: {plan_id}")
        vintage_id = str(plan["vintage_id"])
        vintage = connection.execute(
            "SELECT * FROM holdout_vintage WHERE vintage_id=?", (vintage_id,),
        ).fetchone()
        if vintage is None or vintage["status"] != "sealed":
            raise ValueError("冻结前向预测只能写入未消费 vintage")
        start = _parse_timestamp(str(vintage["start_time"]))
        end = _parse_timestamp(str(vintage["end_time"]))
        if not start <= decision < end:
            raise ValueError("预测决策时间不在绑定 vintage 内")
        plan_evidence = _validated_forward_plan_artifact(
            repository_root,
            str(plan["plan_id"]),
            vintage_id,
            str(plan["source_manifest_sha256"]),
            str(plan["candidate_set_hash"]),
            str(plan["config_hash"]),
            str(plan["code_tree_digest"]),
            str(plan["plan_artifact_path"]),
            str(plan["plan_artifact_sha256"]),
        )
        prediction_id = stable_identifier("frozen-forward-prediction", {
            "governance_method_version": GOVERNANCE_METHOD_VERSION,
            "plan_id": plan_id,
            "decision_time": decision.isoformat(),
        })
        (
            _,
            normalized_artifact_path,
            normalized_receipt_path,
            receipt_sha256,
        ) = _validated_forward_prediction_artifact(
            repository_root,
            prediction_id,
            plan_id,
            vintage_id,
            decision,
            input_head_generation,
            panel_sha256,
            str(plan["config_hash"]),
            str(plan["code_tree_digest"]),
            plan_evidence,
            prediction_artifact_path,
            prediction_artifact_sha256,
            recorded,
        )
        values = (
            input_head_generation,
            panel_sha256,
            normalized_artifact_path,
            prediction_artifact_sha256,
        )
        existing = connection.execute(
            "SELECT * FROM frozen_forward_prediction "
            "WHERE plan_id=? AND decision_time=?",
            (plan_id, _timestamp(decision)),
        ).fetchone()
        if existing is not None:
            expected = (prediction_id, *values)
            actual = (
                existing["prediction_id"], existing["input_head_generation"],
                existing["panel_sha256"], existing["prediction_artifact_path"],
                existing["prediction_artifact_sha256"],
            )
            if actual != expected:
                raise ValueError("该决策时间的冻结前向预测不可改写")
            receipt = connection.execute(
                "SELECT * FROM active_head_receipt "
                "WHERE consumer_kind='frozen_forward' AND consumer_id=?",
                (prediction_id,),
            ).fetchone()
            if (
                receipt is None
                or receipt["receipt_artifact_path"] != normalized_receipt_path
                or receipt["receipt_artifact_sha256"] != receipt_sha256
            ):
                raise ValueError("冻结预测登记缺少匹配的活动输入收据")
            connection.execute("COMMIT")
            return _prediction_from_row(existing)
        connection.execute(
            "INSERT INTO active_head_receipt("
            "consumer_kind,consumer_id,market_id,head_generation,"
            "receipt_artifact_path,receipt_artifact_sha256,recorded_at"
            ") VALUES('frozen_forward',?,?,?,?,?,?)",
            (
                prediction_id, str(vintage["market_id"]), input_head_generation,
                normalized_receipt_path, receipt_sha256, _timestamp(recorded),
            ),
        )
        connection.execute(
            "INSERT INTO frozen_forward_prediction("
            "prediction_id,plan_id,vintage_id,decision_time,"
            "input_head_generation,panel_sha256,prediction_artifact_path,"
            "prediction_artifact_sha256,recorded_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (prediction_id, plan_id, vintage_id, _timestamp(decision),
             *values, _timestamp(recorded)),
        )
        row = connection.execute(
            "SELECT * FROM frozen_forward_prediction WHERE prediction_id=?",
            (prediction_id,),
        ).fetchone()
        connection.execute("COMMIT")
    except BaseException:
        if connection.in_transaction:
            connection.execute("ROLLBACK")
        raise
    finally:
        connection.close()
    if row is None:
        raise RuntimeError("冻结前向预测写入后不可见")
    return _prediction_from_row(row)


def get_frozen_forward_plan(
    registry_path: Path, plan_id: str,
) -> FrozenForwardPlan:
    """读取一个冻结前向计划。"""
    connection = _connect(registry_path)
    try:
        row = connection.execute(
            "SELECT * FROM frozen_forward_plan WHERE plan_id=?", (plan_id,),
        ).fetchone()
    finally:
        connection.close()
    if row is None:
        raise LookupError(f"冻结前向计划不存在: {plan_id}")
    return _plan_from_row(row)


def get_frozen_forward_plan_for_vintage(
    registry_path: Path, vintage_id: str,
) -> FrozenForwardPlan | None:
    """读取 vintage 的唯一冻结前向计划。"""
    connection = _connect(registry_path)
    try:
        row = connection.execute(
            "SELECT * FROM frozen_forward_plan WHERE vintage_id=?", (vintage_id,),
        ).fetchone()
    finally:
        connection.close()
    return None if row is None else _plan_from_row(row)


def list_frozen_forward_predictions(
    registry_path: Path, plan_id: str,
) -> tuple[FrozenForwardPrediction, ...]:
    """按决策时间读取计划的不可变预测历史。"""
    connection = _connect(registry_path)
    try:
        rows = connection.execute(
            "SELECT * FROM frozen_forward_prediction WHERE plan_id=? "
            "ORDER BY decision_time", (plan_id,),
        ).fetchall()
    finally:
        connection.close()
    return tuple(_prediction_from_row(row) for row in rows)


def _exposure_from_row(row: sqlite3.Row) -> ResearchExposure:
    """把 SQLite 行转换为暴露合同。"""
    return ResearchExposure(
        exposure_id=str(row["exposure_id"]),
        research_identity=str(row["research_identity"]),
        market_id=str(row["market_id"]),
        start_time=_parse_timestamp(str(row["start_time"])),
        end_time=_parse_timestamp(str(row["end_time"])),
        recorded_at=_parse_timestamp(str(row["recorded_at"])),
    )


def _optional_time(value: object) -> datetime | None:
    """读取 SQLite 可空时间。"""
    return None if value is None else _parse_timestamp(str(value))


def _optional_text(value: object) -> str | None:
    """读取 SQLite 可空文本。"""
    return None if value is None else str(value)


def _vintage_from_row(row: sqlite3.Row) -> HoldoutVintage:
    """把 SQLite 行转换为 vintage 合同。"""
    status = str(row["status"])
    if status not in _VINTAGE_STATUSES:
        raise ValueError(f"未知 holdout vintage 状态: {status}")
    return HoldoutVintage(
        vintage_id=str(row["vintage_id"]),
        market_id=str(row["market_id"]),
        start_time=_parse_timestamp(str(row["start_time"])),
        end_time=_parse_timestamp(str(row["end_time"])),
        sealed_at=_parse_timestamp(str(row["sealed_at"])),
        status=status,
        consumed_at=_optional_time(row["consumed_at"]),
        candidate_set_hash=_optional_text(row["candidate_set_hash"]),
        evaluation_id=_optional_text(row["evaluation_id"]),
        verdict=_optional_text(row["verdict"]),
        verdict_recorded_at=_optional_time(row["verdict_recorded_at"]),
    )


def _attempt_from_row(row: sqlite3.Row) -> HoldoutEvaluationAttempt:
    """把 SQLite 行转换为 holdout 评估尝试合同。"""
    status = str(row["status"])
    if status not in ("incomplete", "completed"):
        raise ValueError(f"未知 holdout 评估尝试状态: {status}")
    return HoldoutEvaluationAttempt(
        evaluation_id=str(row["evaluation_id"]),
        vintage_id=str(row["vintage_id"]),
        candidate_set_hash=str(row["candidate_set_hash"]),
        status=status,
        stage=str(row["stage"]),
        started_at=_parse_timestamp(str(row["started_at"])),
        updated_at=_parse_timestamp(str(row["updated_at"])),
        completed_at=_optional_time(row["completed_at"]),
        result_manifest_path=_optional_text(row["result_manifest_path"]),
        result_manifest_sha256=_optional_text(row["result_manifest_sha256"]),
    )


def _plan_from_row(row: sqlite3.Row) -> FrozenForwardPlan:
    """把 SQLite 行转换为冻结计划合同。"""
    return FrozenForwardPlan(
        plan_id=str(row["plan_id"]),
        vintage_id=str(row["vintage_id"]),
        source_manifest_sha256=str(row["source_manifest_sha256"]),
        candidate_set_hash=str(row["candidate_set_hash"]),
        config_hash=str(row["config_hash"]),
        code_tree_digest=str(row["code_tree_digest"]),
        plan_artifact_path=str(row["plan_artifact_path"]),
        plan_artifact_sha256=str(row["plan_artifact_sha256"]),
        frozen_at=_parse_timestamp(str(row["frozen_at"])),
    )


def _prediction_from_row(row: sqlite3.Row) -> FrozenForwardPrediction:
    """把 SQLite 行转换为冻结预测合同。"""
    return FrozenForwardPrediction(
        prediction_id=str(row["prediction_id"]),
        plan_id=str(row["plan_id"]),
        vintage_id=str(row["vintage_id"]),
        decision_time=_parse_timestamp(str(row["decision_time"])),
        input_head_generation=str(row["input_head_generation"]),
        panel_sha256=str(row["panel_sha256"]),
        prediction_artifact_path=str(row["prediction_artifact_path"]),
        prediction_artifact_sha256=str(row["prediction_artifact_sha256"]),
        recorded_at=_parse_timestamp(str(row["recorded_at"])),
    )
