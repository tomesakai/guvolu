"""以只读、失败关闭方式检查策略是否达到项目准入政策。

本模块只给出 ``NOT_READY`` 或 ``READY_FOR_EXTERNAL_LIVE_APPROVAL``。
它不写治理库、配置或执行账，不执行晋级，不授权实盘，也不访问网络。
配置阈值是 guvolu 项目政策，不是量化行业的普遍真理（G-06、A-01）。
v4 对尚未实现的独立场景生成与来源重放保持硬失败，不能产生 READY。
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from guvolu.research.provenance import (
    canonical_json,
    sha256_file,
    sha256_text,
    stable_identifier,
)
from guvolu.research.verification import (
    verify_research_artifact_integrity,
    verify_research_run,
)

INDUSTRY_READINESS_METHOD_VERSION = "industry-strategy-readiness-v4"
POLICY_SCOPE = "project_admission_policy_not_universal_truth"
APPROVED_POLICY_ID = "guvolu-industry-strategy-admission-v4"
# 绑定唯一正式政策，变更需同步评审。
APPROVED_POLICY_SHA256 = (
    "8374dfe37fdc15e56ced3a23a705975145f9f8cca833f4caa33645d8f95caef8"
)
APPROVED_POLICY_CANONICAL_SHA256 = (
    "fe2a053a6ee32c53114b2161830a3797662d343cb5167a0b642b524bbcd0098c"
)
_REQUIRED_ARTIFACTS = {
    "candidate_registry", "config", "industry_evidence",
    "industry_evidence_generator_attestation", "summary_json", "trial_ledger",
}
_REQUIRED_CANDIDATE_METRICS = {
    "annual_return", "annual_turnover", "annual_volatility", "bars",
    "cost", "exposure", "hit_rate", "maximum_drawdown", "net_return",
    "p_value", "sharpe", "turnover",
}
_INDUSTRY_EVIDENCE_ARTIFACT = "industry_evidence"
_GENERATOR_ATTESTATION_ARTIFACT = "industry_evidence_generator_attestation"
_SCENARIO_KINDS = ("tail", "stress", "cost", "capacity")
_SCENARIO_COLLECTIONS = {
    "tail": "tail_scenarios",
    "stress": "stress_scenarios",
    "cost": "cost_scenarios",
    "capacity": "capacity_scenarios",
}
_SCENARIO_METHOD_POLICY_FIELDS = {
    "tail": "accepted_tail_scenario_method_versions",
    "stress": "accepted_stress_scenario_method_versions",
    "cost": "accepted_cost_scenario_method_versions",
    "capacity": "accepted_capacity_scenario_method_versions",
}
_SCENARIO_MINIMUM_POLICY_FIELDS = {
    "tail": "minimum_tail_scenarios",
    "stress": "minimum_stress_scenarios",
    "cost": "minimum_cost_scenarios",
    "capacity": "minimum_capacity_scenarios",
}
_SCENARIO_REASON_PREFIX = {
    "tail": "TAIL_RISK",
    "stress": "STRESS",
    "cost": "COST",
    "capacity": "CAPACITY",
}
_SCENARIO_SOURCE_POLICY_FIELDS = {
    "tail": "allowed_tail_source_artifacts",
    "stress": "allowed_stress_source_artifacts",
    "cost": "allowed_cost_source_artifacts",
    "capacity": "allowed_capacity_source_artifacts",
}
_COMMON_SCENARIO_METRICS = {
    "maximum_drawdown", "net_return", "sharpe", "turnover",
}
_CANDIDATE_BOUNDED_METRICS = {
    "exposure": (0.0, 1.0),
    "hit_rate": (0.0, 1.0),
    "maximum_drawdown": (0.0, 1.0),
    "p_value": (0.0, 1.0),
}
_CANDIDATE_NONNEGATIVE_METRICS = {
    "annual_turnover", "annual_volatility", "cost", "turnover",
}
_CANDIDATE_MINIMUM_METRICS = {"annual_return": -1.0, "net_return": -1.0}
_SOURCE_REFERENCE_FIELDS = {
    "artifact_id", "bytes", "kind", "name", "path", "sha256",
}
_COVERAGE_FIELDS = {
    "available_through", "bars", "coverage_ratio", "folds", "from_time",
    "to_time",
}
_COMMON_SCENARIO_FIELDS = {
    "candidate_id", "coverage", "family", "method_version", "metrics",
    "parameters", "pit_verified", "registered_at", "scenario_id",
    "scenario_key", "scenario_type", "schema_version", "selection_locked",
    "source_artifact", "walk_forward_oos_only",
}
_SCENARIO_FIELDS = {
    "tail": _COMMON_SCENARIO_FIELDS,
    "stress": _COMMON_SCENARIO_FIELDS,
    "cost": _COMMON_SCENARIO_FIELDS | {
        "cost_components_bps", "fixed_target", "total_cost_bps",
    },
    "capacity": _COMMON_SCENARIO_FIELDS | {
        "impact_bps", "notional_quote", "observed_depth_quote",
        "participation_rate",
    },
}
_SCENARIO_PARAMETER_FIELDS = {
    "tail": {"block_length", "tail_probability"},
    "stress": {"severity", "stress_definition"},
    "cost": {"cost_tier"},
    "capacity": {"depth_horizon_seconds", "depth_quantile"},
}
_SCENARIO_METRIC_FIELDS = {
    "tail": _COMMON_SCENARIO_METRICS | {"expected_shortfall"},
    "stress": _COMMON_SCENARIO_METRICS,
    "cost": _COMMON_SCENARIO_METRICS,
    "capacity": _COMMON_SCENARIO_METRICS,
}
_COST_COMPONENT_FIELDS = {"fee", "half_spread", "impact", "slippage"}
_CANDIDATE_EVIDENCE_FIELDS = {
    "candidate_id", "capacity_scenarios", "cost_scenarios", "family",
    "stress_scenarios", "tail_scenarios",
}
_INDUSTRY_EVIDENCE_FIELDS = {
    "candidate_evidence", "config_hash", "decision_time", "generated_at",
    "input_receipt_sha256", "method_version", "research_identity", "run_id",
    "schema_version",
}
_GENERATOR_ATTESTATION_FIELDS = {
    "attestation_id", "attested_at", "config_hash", "decision_time",
    "generated_at", "generator_code_sha256", "generator_id",
    "independent_from_strategy_search", "industry_evidence_sha256",
    "input_receipt_sha256", "method_version", "numeric_replay_verified",
    "pit_replay_verified", "research_identity", "run_id", "schema_version",
    "source_artifact_ids",
}


@dataclass(frozen=True)
class GateResult:
    """一个准入门禁的机器可读结果。"""

    gate_id: str
    passed: bool
    reason_codes: tuple[str, ...]
    facts: Mapping[str, object]
    blocking: bool = True

    def as_record(self) -> Mapping[str, object]:
        """转换为稳定 JSON 记录。"""
        return {
            "gate_id": self.gate_id,
            "passed": self.passed,
            "blocking": self.blocking,
            "reason_codes": list(self.reason_codes),
            "facts": dict(self.facts),
        }


def _mapping(value: object) -> Mapping[str, object]:
    """把对象收窄为字符串键映射。"""
    if not isinstance(value, Mapping):
        return {}
    return {str(key): item for key, item in value.items()}


def _sequence(value: object) -> Sequence[object]:
    """把对象收窄为非文本序列。"""
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return ()
    return value


def _number(value: object) -> float | None:
    """读取有限数值，布尔值不视为数值。"""
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return None
    result = float(value)
    if result != result or result in (float("inf"), float("-inf")):
        return None
    return result


def _integer(value: object) -> int | None:
    """读取整数，布尔值不视为整数。"""
    if not isinstance(value, int) or isinstance(value, bool):
        return None
    return value


def _text(value: object) -> str | None:
    """读取非空文本。"""
    if not isinstance(value, str) or not value.strip():
        return None
    return value.strip()


def _sha256_text(value: object) -> str | None:
    """读取规范小写 SHA-256。"""
    text = _text(value)
    if text is None or len(text) != 64:
        return None
    if any(character not in "0123456789abcdef" for character in text):
        return None
    return text


def _stable_id(value: object, prefix: str) -> str | None:
    """读取 ``prefix + 64hex`` 形式的内容身份。"""
    text = _text(value)
    if text is None or not text.startswith(prefix):
        return None
    return text if _sha256_text(text[len(prefix):]) is not None else None


def _unique(values: Sequence[str]) -> tuple[str, ...]:
    """按字典序去重原因码。"""
    return tuple(sorted(set(values)))


def _read_object(path: Path) -> Mapping[str, object]:
    """读取 UTF-8 JSON 对象。"""
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError(f"JSON 制品不是对象: {path}")
    return {str(key): item for key, item in payload.items()}


def _read_json_lines(path: Path) -> tuple[Mapping[str, object], ...]:
    """读取 JSONL，并拒绝任何损坏行。"""
    rows: list[Mapping[str, object]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, raw in enumerate(handle, start=1):
            if not raw.strip():
                continue
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError as error:
                raise ValueError(
                    f"JSONL 第 {line_number} 行损坏: {path}"
                ) from error
            if not isinstance(payload, Mapping):
                raise ValueError(f"JSONL 第 {line_number} 行不是对象: {path}")
            rows.append({str(key): item for key, item in payload.items()})
    return tuple(rows)


def _parse_time(value: object) -> datetime | None:
    """解析带时区的 ISO 时间。"""
    if not isinstance(value, str):
        return None
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if result.tzinfo is None:
        return None
    return result.astimezone(UTC)


def _resolve(root: Path, value: object) -> Path | None:
    """解析配置中的相对路径。"""
    text = _text(value)
    if text is None:
        return None
    path = Path(text)
    return path.resolve() if path.is_absolute() else (root / path).resolve()


def _policy_section(policy: Mapping[str, object], name: str) -> Mapping[str, object]:
    """读取必需政策章节。"""
    section = _mapping(policy.get(name))
    if not section:
        raise ValueError(f"准入政策缺少 {name} 章节")
    return section


def _policy_contract_errors(policy: Mapping[str, object]) -> tuple[str, ...]:
    """验证正式准入政策的数值域，拒绝负门槛与空 allowlist。"""
    errors: list[str] = []
    research = _mapping(policy.get("research"))
    forward = _mapping(policy.get("forward"))
    paper = _mapping(policy.get("paper"))
    execution = _mapping(policy.get("execution"))
    evidence_paths = _mapping(policy.get("evidence_paths"))

    list_fields = (
        "accepted_cpu_pipeline_versions",
        "accepted_industry_evidence_method_versions",
        *_SCENARIO_METHOD_POLICY_FIELDS.values(),
    )
    for field in list_fields:
        values = _sequence(research.get(field))
        if not values or any(_text(value) is None for value in values):
            errors.append(f"research.{field}")

    for kind, field in _SCENARIO_SOURCE_POLICY_FIELDS.items():
        records = _sequence(research.get(field))
        if not records:
            errors.append(f"research.{field}")
            continue
        identities: set[tuple[str, str]] = set()
        for raw in records:
            record = _mapping(raw)
            name = _text(record.get("name"))
            artifact_kind = _text(record.get("kind"))
            if (
                set(record) != {"name", "kind"}
                or name is None or artifact_kind is None
            ):
                errors.append(f"research.{field}.{kind}")
                continue
            identities.add((name, artifact_kind))
        if len(identities) != len(records):
            errors.append(f"research.{field}.duplicate")

    generator_records = _sequence(
        research.get("allowed_industry_evidence_generators")
    )
    generator_identities: set[tuple[str, str, str]] = set()
    for raw in generator_records:
        record = _mapping(raw)
        generator_id = _text(record.get("generator_id"))
        method = _text(record.get("method_version"))
        code_hash = _sha256_text(record.get("generator_code_sha256"))
        if (
            set(record) != {
                "generator_id", "method_version", "generator_code_sha256",
            }
            or generator_id is None or method is None or code_hash is None
        ):
            errors.append("research.allowed_industry_evidence_generators")
            continue
        generator_identities.add((generator_id, method, code_hash))
    if len(generator_identities) != len(generator_records):
        errors.append("research.allowed_industry_evidence_generators")
    accepted_attestation_methods = {
        _text(value) for value in _sequence(
            research.get("accepted_generator_attestation_method_versions")
        )
    }
    if any(
        method not in accepted_attestation_methods
        for _, method, _ in generator_identities
    ):
        errors.append("research.allowed_industry_evidence_generators.method")
    generator_status = _text(
        research.get("industry_evidence_generator_status")
    )
    source_replay_status = _text(
        research.get("scenario_source_replay_status")
    )
    if generator_status != "not_implemented":
        errors.append("research.industry_evidence_generator_status")
    if source_replay_status != "not_implemented":
        errors.append("research.scenario_source_replay_status")
    if (
        accepted_attestation_methods or generator_identities
    ):
        errors.append("research.industry_evidence_generator_registration")

    positive_integers = (
        "minimum_oos_bars", "minimum_block_bootstrap_samples",
        "minimum_parameter_neighbor_count", "minimum_scenario_bars",
        "minimum_scenario_folds", "minimum_tail_scenarios",
        "minimum_stress_scenarios", "minimum_cost_scenarios",
        "minimum_capacity_scenarios",
    )
    for field in positive_integers:
        integer_value = _integer(research.get(field))
        if integer_value is None or integer_value <= 0:
            errors.append(f"research.{field}")

    probabilities = (
        "maximum_fdr_q", "maximum_pbo", "maximum_block_bootstrap_p_value",
        "minimum_deflated_sharpe_probability",
        "minimum_positive_parameter_neighbor_ratio",
        "minimum_parameter_neighbor_sharpe_retention",
        "minimum_scenario_coverage_ratio",
    )
    for field in probabilities:
        probability_value = _number(research.get(field))
        if probability_value is None or not 0.0 <= probability_value <= 1.0:
            errors.append(f"research.{field}")

    drawdown_fields = (
        "maximum_oos_drawdown", "maximum_tail_scenario_drawdown",
        "maximum_stress_scenario_drawdown", "maximum_cost_scenario_drawdown",
    )
    for field in drawdown_fields:
        drawdown_value = _number(research.get(field))
        if drawdown_value is None or not 0.0 <= drawdown_value <= 1.0:
            errors.append(f"research.{field}")

    nonnegative_fields = (
        "minimum_benchmark_sharpe_excess",
        "minimum_cost_scenario_benchmark_sharpe_excess",
        "minimum_capacity_notional_quote", "maximum_capacity_impact_bps",
        "policy_baseline_total_cost_bps", "minimum_cost_grid_step_bps",
        "maximum_tail_scenario_turnover",
        "maximum_stress_scenario_turnover",
        "maximum_cost_scenario_turnover",
    )
    for field in nonnegative_fields:
        nonnegative_value = _number(research.get(field))
        if nonnegative_value is None or nonnegative_value < 0.0:
            errors.append(f"research.{field}")

    finite_fields = (
        "minimum_oos_sharpe", "minimum_block_bootstrap_sharpe_lower_bound",
        "minimum_tail_scenario_sharpe", "minimum_tail_scenario_net_return",
        "minimum_stress_scenario_sharpe", "minimum_stress_scenario_net_return",
        "minimum_cost_scenario_sharpe", "minimum_cost_scenario_net_return",
    )
    for field in finite_fields:
        if _number(research.get(field)) is None:
            errors.append(f"research.{field}")
    expected_shortfall = _number(research.get("minimum_tail_expected_shortfall"))
    if expected_shortfall is None or not -1.0 <= expected_shortfall <= 0.0:
        errors.append("research.minimum_tail_expected_shortfall")
    raw_tail_probabilities = _sequence(
        research.get("required_tail_probabilities")
    )
    required_tail_probabilities: list[float] = []
    invalid_tail_probability = False
    for raw_probability in raw_tail_probabilities:
        probability = _number(raw_probability)
        if probability is None or not 0.0 < probability < 0.5:
            invalid_tail_probability = True
        else:
            required_tail_probabilities.append(probability)
    if (
        not required_tail_probabilities
        or invalid_tail_probability
        or sorted(required_tail_probabilities) != required_tail_probabilities
        or len(set(required_tail_probabilities)) != len(required_tail_probabilities)
    ):
        errors.append("research.required_tail_probabilities")
    for field in ("required_stress_definitions", "required_cost_tiers"):
        raw_values = _sequence(research.get(field))
        values = tuple(_text(value) for value in raw_values)
        if (
            not values or any(value is None for value in values)
            or len(set(values)) != len(values)
        ):
            errors.append(f"research.{field}")
    minimum_tail = _integer(research.get("minimum_tail_scenarios"))
    minimum_stress = _integer(research.get("minimum_stress_scenarios"))
    minimum_cost = _integer(research.get("minimum_cost_scenarios"))
    if minimum_tail is None or len(required_tail_probabilities) != minimum_tail:
        errors.append("research.required_tail_probabilities.count")
    if minimum_stress is None or len(
        _sequence(research.get("required_stress_definitions"))
    ) != minimum_stress:
        errors.append("research.required_stress_definitions.count")
    if minimum_cost is None or len(
        _sequence(research.get("required_cost_tiers"))
    ) != minimum_cost:
        errors.append("research.required_cost_tiers.count")
    baseline_cost = _number(research.get("policy_baseline_total_cost_bps"))
    minimum_step = _number(research.get("minimum_cost_grid_step_bps"))
    if baseline_cost is None or baseline_cost <= 0.0:
        errors.append("research.policy_baseline_total_cost_bps")
    if minimum_step is None or minimum_step <= 0.0:
        errors.append("research.minimum_cost_grid_step_bps")
    participation = _number(research.get("maximum_capacity_participation_rate"))
    if participation is None or not 0.0 < participation <= 1.0:
        errors.append("research.maximum_capacity_participation_rate")

    interval = _integer(forward.get("decision_interval_seconds"))
    coverage = _number(forward.get("minimum_prediction_coverage_ratio"))
    if interval is None or interval <= 0:
        errors.append("forward.decision_interval_seconds")
    if coverage is None or not 0.0 < coverage <= 1.0:
        errors.append("forward.minimum_prediction_coverage_ratio")
    if _text(forward.get("required_terminal_verdict")) is None:
        errors.append("forward.required_terminal_verdict")

    for field in (
        "minimum_duration_hours", "minimum_decisions", "minimum_ledger_rows",
        "minimum_reconciled_decisions",
    ):
        paper_value = _number(paper.get(field))
        if paper_value is None or paper_value <= 0.0:
            errors.append(f"paper.{field}")
    maximum_error = _number(paper.get("maximum_error_ratio"))
    if maximum_error is None or not 0.0 <= maximum_error <= 1.0:
        errors.append("paper.maximum_error_ratio")
    if not _sequence(paper.get("accepted_reconciliation_statuses")):
        errors.append("paper.accepted_reconciliation_statuses")
    for field in ("required_controls", "required_permissions"):
        if not _sequence(execution.get(field)):
            errors.append(f"execution.{field}")
    if any(_text(value) is None for value in evidence_paths.values()) or not evidence_paths:
        errors.append("evidence_paths")
    return _unique(errors)


def _policy_content_hash(policy: Mapping[str, object]) -> str | None:
    """复算政策语义散列，防止加载后仍保留 marker 的内存篡改。"""
    payload = {
        key: value for key, value in policy.items() if not key.startswith("_")
    }
    try:
        return sha256_text(canonical_json(payload))
    except (TypeError, ValueError):
        return None


def load_industry_readiness_policy(path: Path) -> Mapping[str, object]:
    """读取且绑定仓库唯一获批的版本化项目准入政策。"""
    policy = _read_object(path)
    if policy.get("schema_version") != 1:
        raise ValueError("准入政策 schema_version 必须为 1")
    if policy.get("policy_scope") != POLICY_SCOPE:
        raise ValueError("准入政策必须声明其不是普遍真理")
    if policy.get("policy_id") != APPROVED_POLICY_ID:
        raise ValueError("准入政策 policy_id 未获批准")
    for name in ("research", "forward", "paper", "execution", "evidence_paths"):
        _policy_section(policy, name)
    errors = _policy_contract_errors(policy)
    if errors:
        raise ValueError(f"准入政策数值域非法: {', '.join(errors)}")
    policy_sha256 = sha256_file(path)
    if policy_sha256 != APPROVED_POLICY_SHA256:
        raise ValueError("准入政策文件散列未获批准；自定义政策只能用于诊断")
    if _policy_content_hash(policy) != APPROVED_POLICY_CANONICAL_SHA256:
        raise ValueError("准入政策规范内容散列未获批准")
    return {**policy, "_approved_policy_sha256": policy_sha256}


def _verified_artifact_index(
    manifest: Mapping[str, object],
    artifact_paths: Mapping[str, Path],
) -> Mapping[str, Mapping[str, object]]:
    """把 integrity verifier 已复核的路径转成最小身份索引。"""
    records = _mapping(manifest.get("artifacts"))
    result: dict[str, Mapping[str, object]] = {}
    for name, path in artifact_paths.items():
        record = _mapping(records.get(name))
        digest = _sha256_text(record.get("sha256"))
        byte_count = _integer(record.get("bytes"))
        kind = _text(record.get("kind"))
        relative_path = _text(record.get("path"))
        if (
            digest is None or byte_count is None or byte_count < 0
            or kind is None or relative_path is None
        ):
            continue
        result[name] = {
            "name": name,
            "kind": kind,
            "path": relative_path,
            "resolved_path": str(path),
            "sha256": digest,
            "artifact_id": f"sha256-{digest}",
            "bytes": byte_count,
        }
    return result


def _read_verified_json_snapshot(
    path: Path,
    identity: Mapping[str, object],
) -> Mapping[str, object]:
    """单次读取受保护字节并复核身份，封闭 verifier 后路径替换窗口。"""
    raw = path.read_bytes()
    expected_bytes = _integer(identity.get("bytes"))
    expected_hash = _sha256_text(identity.get("sha256"))
    if expected_bytes is None or len(raw) != expected_bytes:
        raise ValueError("受保护 JSON 制品字节数在复核后发生变化")
    if expected_hash is None or hashlib.sha256(raw).hexdigest() != expected_hash:
        raise ValueError("受保护 JSON 制品散列在复核后发生变化")
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("受保护 JSON 制品不是规范 UTF-8 JSON") from error
    if not isinstance(payload, Mapping):
        raise ValueError("受保护 JSON 制品不是对象")
    return {str(key): value for key, value in payload.items()}


def _read_verified_json_lines_snapshot(
    path: Path,
    identity: Mapping[str, object],
) -> tuple[Mapping[str, object], ...]:
    """从一次受身份复核的字节快照解析 JSONL。"""
    raw = path.read_bytes()
    expected_bytes = _integer(identity.get("bytes"))
    expected_hash = _sha256_text(identity.get("sha256"))
    if expected_bytes is None or len(raw) != expected_bytes:
        raise ValueError("受保护 JSONL 制品字节数在复核后发生变化")
    if expected_hash is None or hashlib.sha256(raw).hexdigest() != expected_hash:
        raise ValueError("受保护 JSONL 制品散列在复核后发生变化")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ValueError("受保护 JSONL 制品不是 UTF-8") from error
    rows: list[Mapping[str, object]] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as error:
            raise ValueError(
                f"受保护 JSONL 第 {line_number} 行损坏"
            ) from error
        if not isinstance(payload, Mapping):
            raise ValueError(f"受保护 JSONL 第 {line_number} 行不是对象")
        rows.append({str(key): value for key, value in payload.items()})
    return tuple(rows)


def _read_manifest_snapshot(path: Path, expected_hash: str) -> Mapping[str, object]:
    """按 verify_research_run 返回的散列固定 manifest 字节快照。"""
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != expected_hash:
        raise ValueError("research manifest 在语义复核后发生变化")
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("research manifest 不是 UTF-8 JSON 对象") from error
    if not isinstance(payload, Mapping):
        raise ValueError("research manifest 不是对象")
    return {str(key): value for key, value in payload.items()}


def _file_snapshot_matches(path: Path, identity: Mapping[str, object]) -> bool:
    """从同一文件句柄重算字节数与散列，不依赖稍早的路径状态。"""
    expected_bytes = _integer(identity.get("bytes"))
    expected_hash = _sha256_text(identity.get("sha256"))
    if expected_bytes is None or expected_hash is None:
        return False
    digest = hashlib.sha256()
    byte_count = 0
    try:
        with path.open("rb") as handle:
            while block := handle.read(1024 * 1024):
                digest.update(block)
                byte_count += len(block)
    except OSError:
        return False
    return byte_count == expected_bytes and digest.hexdigest() == expected_hash


def _scenario_source_names(payload: Mapping[str, object]) -> set[str]:
    """从未信任 payload 收窄出声明的来源制品名称。"""
    result: set[str] = set()
    for raw_candidate in _sequence(payload.get("candidate_evidence")):
        candidate = _mapping(raw_candidate)
        for collection in _SCENARIO_COLLECTIONS.values():
            for raw_scenario in _sequence(candidate.get(collection)):
                source = _mapping(_mapping(raw_scenario).get("source_artifact"))
                if (name := _text(source.get("name"))) is not None:
                    result.add(name)
    return result


def _industry_artifact_evidence(
    manifest: Mapping[str, object],
    artifact_paths: Mapping[str, Path],
) -> Mapping[str, object]:
    """只从 integrity verifier 返回的受保护路径加载行业证据。"""
    verified = _verified_artifact_index(manifest, artifact_paths)
    identity = _mapping(verified.get(_INDUSTRY_EVIDENCE_ARTIFACT))
    attestation_identity = _mapping(
        verified.get(_GENERATOR_ATTESTATION_ARTIFACT)
    )
    path = artifact_paths.get(_INDUSTRY_EVIDENCE_ARTIFACT)
    attestation_path = artifact_paths.get(_GENERATOR_ATTESTATION_ARTIFACT)
    if path is None or not identity:
        return {
            "present": False,
            "verified": False,
            "verified_artifacts": dict(verified),
        }
    if identity.get("kind") != "industry_evidence":
        return {
            "present": True,
            "verified": False,
            "artifact": dict(identity),
            "error": "industry_evidence artifact kind 非法",
            "verified_artifacts": dict(verified),
        }
    if attestation_path is None or not attestation_identity:
        return {
            "present": True,
            "verified": False,
            "artifact": dict(identity),
            "error": "独立 generator attestation 制品缺失",
            "verified_artifacts": dict(verified),
        }
    if attestation_identity.get("kind") != "industry_evidence_generator_attestation":
        return {
            "present": True,
            "verified": False,
            "artifact": dict(identity),
            "generator_attestation_artifact": dict(attestation_identity),
            "error": "generator attestation artifact kind 非法",
            "verified_artifacts": dict(verified),
        }
    try:
        payload = _read_verified_json_snapshot(path, identity)
        attestation_payload = _read_verified_json_snapshot(
            attestation_path,
            attestation_identity,
        )
    except (OSError, ValueError, json.JSONDecodeError) as error:
        return {
            "present": True,
            "verified": False,
            "artifact": dict(identity),
            "error": f"{type(error).__name__}: {error}",
            "verified_artifacts": dict(verified),
        }
    snapshot_index = {
        name: dict(record) for name, record in verified.items()
    }
    for name in _scenario_source_names(payload):
        source_identity = _mapping(snapshot_index.get(name))
        source_path = artifact_paths.get(name)
        snapshot_index[name] = {
            **source_identity,
            "snapshot_verified": (
                source_path is not None
                and bool(source_identity)
                and _file_snapshot_matches(source_path, source_identity)
            ),
        }
    return {
        "present": True,
        "verified": True,
        "artifact": dict(identity),
        "payload": dict(payload),
        "generator_attestation_artifact": dict(attestation_identity),
        "generator_attestation_payload": dict(attestation_payload),
        "verified_artifacts": snapshot_index,
    }


def _research_evidence(
    root: Path,
    manifest_path: Path | None,
) -> Mapping[str, object]:
    """完整语义重建研究运行，再收集受完整性保护的准入证据。"""
    try:
        semantic = verify_research_run(root, manifest_path)
        integrity = verify_research_artifact_integrity(
            root, semantic.manifest_path,
        )
        if (
            semantic.run_id != integrity.manifest.get("run_id")
            or semantic.manifest_sha256 != integrity.manifest_sha256
            or set(semantic.checked_artifacts) != set(integrity.checked_artifacts)
        ):
            raise ValueError("完整语义复核结果与完整性复核结果不一致")
    except (OSError, ValueError) as error:
        return {
            "verified": False,
            "semantic_verified": False,
            "verification_error": f"{type(error).__name__}: {error}",
            "summary": {},
            "research_config": {},
            "trial_ledger": {},
            "manifest": {},
            "checked_artifacts": [],
            "industry_evidence": {
                "present": False,
                "verified": False,
            },
        }
    verified_index = _verified_artifact_index(
        integrity.manifest,
        integrity.artifact_paths,
    )
    try:
        manifest = _read_manifest_snapshot(
            integrity.manifest_path,
            semantic.manifest_sha256,
        )
        if manifest != integrity.manifest:
            raise ValueError("manifest 字节快照与完整性复核对象不一致")
        summary_path = integrity.artifact_paths.get("summary_json")
        summary_identity = _mapping(verified_index.get("summary_json"))
        if summary_path is None or not summary_identity:
            raise ValueError("manifest 缺少 summary_json 制品")
        summary = _read_verified_json_snapshot(summary_path, summary_identity)
        if summary != integrity.summary:
            raise ValueError("summary 字节快照与完整性复核对象不一致")
    except (OSError, ValueError) as error:
        return {
            "verified": False,
            "semantic_verified": False,
            "verification_error": f"{type(error).__name__}: {error}",
            "summary": {},
            "research_config": {},
            "trial_ledger": {},
            "manifest": {},
            "checked_artifacts": [],
            "industry_evidence": {
                "present": False,
                "verified": False,
            },
        }
    config_payload: Mapping[str, object] = {}
    trial_payload: Mapping[str, object] = {}
    config_path = integrity.artifact_paths.get("config")
    ledger_path = integrity.artifact_paths.get("trial_ledger")
    registry_path = integrity.artifact_paths.get("candidate_registry")
    try:
        if config_path is not None:
            config_payload = _read_verified_json_snapshot(
                config_path,
                _mapping(verified_index.get("config")),
            )
        if ledger_path is not None:
            rows = _read_verified_json_lines_snapshot(
                ledger_path,
                _mapping(verified_index.get("trial_ledger")),
            )
            header = rows[0] if rows else {}
            trials = tuple(row for row in rows if row.get("record_type") == "trial")
            evaluation_ids = [
                value for row in trials
                if (value := _text(row.get("evaluation_id"))) is not None
            ]
            registry = (
                _read_verified_json_snapshot(
                    registry_path,
                    _mapping(verified_index.get("candidate_registry")),
                )
                if registry_path is not None else {}
            )
            if registry != _mapping(integrity.candidate_registry):
                raise ValueError("candidate registry 快照不一致")
            raw_candidates = _sequence(registry.get("candidates"))
            registry_ids = {
                value for raw in raw_candidates
                if (value := _text(_mapping(raw).get("candidate_id"))) is not None
            }
            ledger_ids = {
                value for row in trials
                if (value := _text(row.get("candidate_id"))) is not None
            }
            trial_payload = {
                "present": True,
                "header": dict(header),
                "trial_rows": len(trials),
                "evaluation_id_count": len(evaluation_ids),
                "unique_evaluation_id_count": len(set(evaluation_ids)),
                "registry_candidate_count": len(registry_ids),
                "ledger_candidate_count": len(ledger_ids),
                "missing_registry_candidate_ids": sorted(registry_ids - ledger_ids),
            }
    except (OSError, ValueError) as error:
        trial_payload = {
            "present": ledger_path is not None,
            "error": f"{type(error).__name__}: {error}",
        }
    return {
        "verified": True,
        "semantic_verified": True,
        "run_id": manifest.get("run_id"),
        "manifest_path": str(integrity.manifest_path),
        "manifest_sha256": integrity.manifest_sha256,
        "manifest": dict(manifest),
        "summary": dict(summary),
        "research_config": dict(config_payload),
        "trial_ledger": dict(trial_payload),
        "checked_artifacts": list(integrity.checked_artifacts),
        "industry_evidence": _industry_artifact_evidence(
            manifest,
            integrity.artifact_paths,
        ),
    }


def _read_only_connection(path: Path) -> sqlite3.Connection:
    """以 SQLite 只读 URI 打开治理库。"""
    connection = sqlite3.connect(
        path.resolve().as_uri() + "?mode=ro",
        uri=True,
        timeout=30.0,
        isolation_level=None,
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    return connection


def _verdict_value(raw: object) -> str | None:
    """解析治理库的规范终态 verdict。"""
    text = _text(raw)
    if text is None:
        return None
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return None
    return _text(_mapping(payload).get("verdict"))


def _artifact_state(root: Path, raw_path: object, raw_hash: object) -> str:
    """复核治理制品的存在性与散列。"""
    path = _resolve(root, raw_path)
    expected = _text(raw_hash)
    if path is None or expected is None or not path.is_file():
        return "missing"
    return "verified" if sha256_file(path) == expected else "hash_mismatch"


def _forward_evidence(
    root: Path,
    registry_path: Path | None,
    market_id: str | None,
    interval_seconds: int,
) -> Mapping[str, object]:
    """通过治理 schema 的只读视图收集封存前向事实。"""
    if registry_path is None:
        return {"registry_present": False, "vintages": []}
    if not registry_path.is_file():
        return {
            "registry_present": False,
            "registry_path": str(registry_path),
            "vintages": [],
        }
    connection: sqlite3.Connection | None = None
    try:
        connection = _read_only_connection(registry_path)
        parameters: tuple[object, ...] = ()
        statement = (
            "SELECT vintage_id,market_id,start_time,end_time,status,consumed_at,"
            "evaluation_id,verdict FROM holdout_vintage"
        )
        if market_id is not None:
            statement += " WHERE market_id=?"
            parameters = (market_id,)
        statement += " ORDER BY start_time,vintage_id"
        rows = connection.execute(statement, parameters).fetchall()
        vintages: list[Mapping[str, object]] = []
        for row in rows:
            vintage_id = str(row["vintage_id"])
            start = _parse_time(row["start_time"])
            end = _parse_time(row["end_time"])
            grid_valid = (
                start is not None
                and end is not None
                and end > start
                and int((end - start).total_seconds()) % interval_seconds == 0
            )
            expected = (
                0 if not grid_valid or start is None or end is None
                else int((end - start).total_seconds()) // interval_seconds
            )
            plan = connection.execute(
                "SELECT plan_id,plan_artifact_path,plan_artifact_sha256,"
                "missing_policy FROM frozen_forward_plan WHERE vintage_id=?",
                (vintage_id,),
            ).fetchone()
            prediction_count = 0
            unique_count = 0
            artifact_failures = 0
            duplicate_times = 0
            plan_id: str | None = None
            plan_artifact_state = "missing"
            missing_policy: str | None = None
            if plan is not None:
                plan_id = str(plan["plan_id"])
                missing_policy = _text(plan["missing_policy"])
                plan_artifact_state = _artifact_state(
                    root, plan["plan_artifact_path"], plan["plan_artifact_sha256"],
                )
                predictions = connection.execute(
                    "SELECT decision_time,prediction_artifact_path,"
                    "prediction_artifact_sha256 FROM frozen_forward_prediction "
                    "WHERE plan_id=? ORDER BY decision_time",
                    (plan_id,),
                ).fetchall()
                prediction_count = len(predictions)
                decision_times = [str(item["decision_time"]) for item in predictions]
                unique_count = len(set(decision_times))
                duplicate_times = prediction_count - unique_count
                artifact_failures = sum(
                    _artifact_state(
                        root,
                        item["prediction_artifact_path"],
                        item["prediction_artifact_sha256"],
                    ) != "verified"
                    for item in predictions
                )
            coverage = 0.0 if expected <= 0 else min(1.0, unique_count / expected)
            vintages.append({
                "vintage_id": vintage_id,
                "market_id": row["market_id"],
                "start_time": row["start_time"],
                "end_time": row["end_time"],
                "status": row["status"],
                "consumed_at": row["consumed_at"],
                "evaluation_id": row["evaluation_id"],
                "terminal_verdict": _verdict_value(row["verdict"]),
                "plan_id": plan_id,
                "missing_policy": missing_policy,
                "plan_artifact_state": plan_artifact_state,
                "prediction_count": prediction_count,
                "unique_prediction_count": unique_count,
                "duplicate_prediction_times": duplicate_times,
                "prediction_artifact_failures": artifact_failures,
                "expected_prediction_count": expected,
                "prediction_coverage_ratio": coverage,
                "decision_grid_valid": grid_valid,
            })
        return {
            "registry_present": True,
            "registry_path": str(registry_path),
            "vintages": vintages,
        }
    except (OSError, sqlite3.DatabaseError, ValueError) as error:
        return {
            "registry_present": True,
            "registry_path": str(registry_path),
            "read_error": f"{type(error).__name__}: {error}",
            "vintages": [],
        }
    finally:
        if connection is not None:
            connection.close()


def _paper_evidence(
    execution_root: Path | None,
    policy: Mapping[str, object],
) -> Mapping[str, object]:
    """读取现有 paper 任务、差异账与对账证据。"""
    if execution_root is None:
        return {"execution_root_provided": False}
    paths = _policy_section(policy, "evidence_paths")
    task_path = _resolve(execution_root, paths.get("paper_task_log"))
    ledger_path = _resolve(execution_root, paths.get("paper_difference_ledger"))
    reconcile_path = _resolve(
        execution_root, paths.get("paper_reconciliation_ledger"),
    )
    result: dict[str, object] = {
        "execution_root_provided": True,
        "execution_root": str(execution_root),
        "task_log_present": task_path is not None and task_path.is_file(),
        "ledger_present": ledger_path is not None and ledger_path.is_file(),
        "reconciliation_present": (
            reconcile_path is not None and reconcile_path.is_file()
        ),
    }
    try:
        tasks = () if task_path is None or not task_path.is_file() else (
            _read_json_lines(task_path)
        )
        paper_records: list[Mapping[str, object]] = []
        error_count = 0
        write_proven = True
        decision_times: list[datetime] = []
        for task in tasks:
            paper = _mapping(task.get("paper"))
            if task.get("status") == "failed" or paper.get("status") == "failed":
                error_count += 1
            if not paper:
                continue
            if paper.get("status") not in {"completed", "reused"}:
                continue
            paper_records.append(paper)
            timestamp = _parse_time(task.get("decision_time"))
            if timestamp is not None:
                decision_times.append(timestamp)
            if task.get("write_touched") != [] or paper.get("write_touched") != []:
                write_proven = False
        if not paper_records:
            write_proven = False
        unique_times = sorted(set(decision_times))
        duration = (
            0.0 if len(unique_times) < 2
            else (unique_times[-1] - unique_times[0]).total_seconds() / 3600.0
        )
        ledger_rows = () if ledger_path is None or not ledger_path.is_file() else (
            _read_json_lines(ledger_path)
        )
        ledger_times = {
            timestamp for row in ledger_rows
            if (timestamp := _parse_time(row.get("decision_time"))) is not None
        }
        reconciliations = (
            () if reconcile_path is None or not reconcile_path.is_file()
            else _read_json_lines(reconcile_path)
        )
        paper_policy = _policy_section(policy, "paper")
        accepted = {
            str(item) for item in _sequence(
                paper_policy.get("accepted_reconciliation_statuses")
            )
        }
        reconciled = sum(
            row.get("matched") is True or row.get("status") in accepted
            for row in reconciliations
        )
        attempts = len(paper_records) + error_count
        result.update({
            "paper_decisions": len(unique_times),
            "paper_duration_hours": duration,
            "paper_task_successes": len(paper_records),
            "paper_task_errors": error_count,
            "paper_error_ratio": (
                1.0 if attempts == 0 else error_count / attempts
            ),
            "ledger_rows": len(ledger_rows),
            "ledger_unique_decisions": len(ledger_times),
            "reconciled_decisions": reconciled,
            "zero_real_writes_proven": write_proven,
        })
    except (OSError, ValueError) as error:
        result["read_error"] = f"{type(error).__name__}: {error}"
    return result


def _execution_evidence(
    execution_root: Path | None,
    policy: Mapping[str, object],
) -> Mapping[str, object]:
    """读取独立执行安全 attestation。"""
    if execution_root is None:
        return {"execution_root_provided": False, "attestation_present": False}
    paths = _policy_section(policy, "evidence_paths")
    path = _resolve(execution_root, paths.get("execution_safety_attestation"))
    if path is None or not path.is_file():
        return {
            "execution_root_provided": True,
            "attestation_present": False,
            "attestation_path": None if path is None else str(path),
        }
    try:
        payload = _read_object(path)
    except (OSError, ValueError) as error:
        return {
            "execution_root_provided": True,
            "attestation_present": True,
            "attestation_path": str(path),
            "read_error": f"{type(error).__name__}: {error}",
        }
    return {
        "execution_root_provided": True,
        "attestation_present": True,
        "attestation_path": str(path),
        "attestation": dict(payload),
    }


def collect_industry_readiness_evidence(
    repository_root: Path,
    policy: Mapping[str, object],
    manifest_path: Path | None = None,
    governance_registry_path: Path | None = None,
    governance_artifact_root: Path | None = None,
    execution_root: Path | None = None,
) -> Mapping[str, object]:
    """只读收集准入所需证据，不创建任何制品。"""
    root = repository_root.resolve()
    research = _research_evidence(root, manifest_path)
    summary = _mapping(research.get("summary"))
    governance = _mapping(summary.get("data_governance"))
    registry = governance_registry_path
    if registry is None:
        registry = _resolve(root, governance.get("registry"))
    forward_root = (
        root if governance_artifact_root is None
        else governance_artifact_root.resolve()
    )
    forward_policy = _policy_section(policy, "forward")
    interval = _integer(forward_policy.get("decision_interval_seconds"))
    if interval is None or interval <= 0:
        raise ValueError("forward.decision_interval_seconds 必须为正整数")
    market_id = _text(summary.get("market_id"))
    resolved_execution = None if execution_root is None else execution_root.resolve()
    return {
        "research": research,
        "forward": _forward_evidence(forward_root, registry, market_id, interval),
        "paper": _paper_evidence(resolved_execution, policy),
        "execution": _execution_evidence(resolved_execution, policy),
    }


def _policy_gate(policy: Mapping[str, object]) -> GateResult:
    """正式准入只接受代码登记的项目政策身份与严格数值域。"""
    reasons: list[str] = []
    if (
        policy.get("policy_id") != APPROVED_POLICY_ID
        or policy.get("_approved_policy_sha256") != APPROVED_POLICY_SHA256
        or _policy_content_hash(policy) != APPROVED_POLICY_CANONICAL_SHA256
    ):
        reasons.append("ADMISSION_POLICY_NOT_APPROVED")
    domain_errors = _policy_contract_errors(policy)
    if domain_errors:
        reasons.append("ADMISSION_POLICY_DOMAIN_INVALID")
    return GateResult(
        "admission_policy",
        not reasons,
        _unique(reasons),
        {
            "policy_id": policy.get("policy_id"),
            "policy_sha256": policy.get("_approved_policy_sha256"),
            "approved_policy_id": APPROVED_POLICY_ID,
            "approved_policy_sha256": APPROVED_POLICY_SHA256,
            "policy_canonical_sha256": _policy_content_hash(policy),
            "approved_policy_canonical_sha256": (
                APPROVED_POLICY_CANONICAL_SHA256
            ),
            "domain_errors": list(domain_errors),
        },
    )


def _manifest_gate(evidence: Mapping[str, object], policy: Mapping[str, object]) -> GateResult:
    """检查 clean、decision-grade 与 CPU 精确管线证据。"""
    reasons: list[str] = []
    summary = _mapping(evidence.get("summary"))
    manifest = _mapping(evidence.get("manifest"))
    code = _mapping(manifest.get("code_identity"))
    checked = {str(item) for item in _sequence(evidence.get("checked_artifacts"))}
    if evidence.get("verified") is not True:
        reasons.append("RESEARCH_MANIFEST_VERIFICATION_FAILED")
    if evidence.get("semantic_verified") is not True:
        reasons.append("RESEARCH_SEMANTIC_VERIFICATION_FAILED")
    if code.get("decision_grade") is not True:
        reasons.append("RESEARCH_MANIFEST_NOT_DECISION_GRADE")
    if code.get("dirty") is not False:
        reasons.append("RESEARCH_MANIFEST_CODE_NOT_CLEAN")
    if summary.get("decision_grade") is not True:
        reasons.append("RESEARCH_SUMMARY_NOT_DECISION_GRADE")
    if not _REQUIRED_ARTIFACTS.issubset(checked):
        reasons.append("RESEARCH_REQUIRED_ARTIFACTS_INCOMPLETE")
    research_policy = _policy_section(policy, "research")
    accepted = {
        str(item) for item in _sequence(
            research_policy.get("accepted_cpu_pipeline_versions")
        )
    }
    pipeline = _text(summary.get("pipeline_method_version"))
    if pipeline not in accepted:
        reasons.append("CPU_EXACT_PIPELINE_NOT_ACCEPTED")
    return GateResult(
        "research_manifest",
        not reasons,
        _unique(reasons),
        {
            "run_id": evidence.get("run_id"),
            "manifest_sha256": evidence.get("manifest_sha256"),
            "pipeline_method_version": pipeline,
            "checked_artifacts": sorted(checked),
            "verification_error": evidence.get("verification_error"),
            "semantic_verified": evidence.get("semantic_verified") is True,
        },
    )


def _eligible_candidates(summary: Mapping[str, object]) -> tuple[Mapping[str, object], ...]:
    """读取被研究 summary 标记为 paper eligible 的候选。"""
    return tuple(
        item for raw in _sequence(summary.get("family_evaluations"))
        if (item := _mapping(raw)).get("eligible") is True
        and item.get("mode") == "paper"
    )


def _candidate_metric_errors(
    metrics: Mapping[str, object],
) -> tuple[list[str], list[str]]:
    """逐字段验证候选指标的有限性、类型与业务数值域。"""
    missing = sorted(_REQUIRED_CANDIDATE_METRICS - set(metrics))
    invalid: list[str] = []
    for field in sorted(_REQUIRED_CANDIDATE_METRICS):
        raw = metrics.get(field)
        if field == "bars":
            bars_value = _integer(raw)
            if bars_value is None or bars_value <= 0:
                invalid.append(field)
            continue
        numeric_value = _number(raw)
        if numeric_value is None:
            invalid.append(field)
            continue
        bounds = _CANDIDATE_BOUNDED_METRICS.get(field)
        if bounds is not None and not bounds[0] <= numeric_value <= bounds[1]:
            invalid.append(field)
        elif field in _CANDIDATE_NONNEGATIVE_METRICS and numeric_value < 0.0:
            invalid.append(field)
        elif (
            field in _CANDIDATE_MINIMUM_METRICS
            and numeric_value < _CANDIDATE_MINIMUM_METRICS[field]
        ):
            invalid.append(field)
    return missing, invalid


def _candidate_gate(summary: Mapping[str, object], policy: Mapping[str, object]) -> GateResult:
    """检查 validation/deployment 完整候选指标与项目数值政策。"""
    reasons: list[str] = []
    details: list[Mapping[str, object]] = []
    candidates = _eligible_candidates(summary)
    research = _policy_section(policy, "research")
    minimum_sharpe = _number(research.get("minimum_oos_sharpe"))
    maximum_drawdown = _number(research.get("maximum_oos_drawdown"))
    minimum_bars = _integer(research.get("minimum_oos_bars"))
    if not candidates:
        reasons.append("PAPER_ELIGIBLE_CANDIDATE_SET_EMPTY")
    seen_families: set[str] = set()
    for candidate in candidates:
        family = _text(candidate.get("family")) or "unknown"
        local: list[str] = []
        if family in seen_families:
            local.append("CANDIDATE_FAMILY_DUPLICATE")
        seen_families.add(family)
        if _text(candidate.get("deployment_candidate_id")) is None:
            local.append("CANDIDATE_ID_MISSING")
        validation = _mapping(candidate.get("validation_metrics"))
        deployment = _mapping(candidate.get("deployment_oos_metrics"))
        missing_validation, invalid_validation = _candidate_metric_errors(validation)
        missing_deployment, invalid_deployment = _candidate_metric_errors(deployment)
        if missing_validation or missing_deployment:
            local.append("CANDIDATE_METRICS_INCOMPLETE")
        if invalid_validation:
            local.append("CANDIDATE_VALIDATION_METRICS_INVALID")
        if invalid_deployment:
            local.append("CANDIDATE_DEPLOYMENT_METRICS_INVALID")
        vector_facts: dict[str, object] = {}
        for label, metrics in (
            ("VALIDATION", validation), ("DEPLOYMENT", deployment),
        ):
            sharpe = _number(metrics.get("sharpe"))
            drawdown = _number(metrics.get("maximum_drawdown"))
            bars = _integer(metrics.get("bars"))
            if minimum_sharpe is None or sharpe is None or sharpe < minimum_sharpe:
                local.append(f"CANDIDATE_{label}_OOS_SHARPE_BELOW_POLICY")
            if (
                maximum_drawdown is None or drawdown is None
                or drawdown > maximum_drawdown
            ):
                local.append(f"CANDIDATE_{label}_OOS_DRAWDOWN_ABOVE_POLICY")
            if minimum_bars is None or bars is None or bars < minimum_bars:
                local.append(f"CANDIDATE_{label}_OOS_BARS_BELOW_POLICY")
            vector_facts[label.lower()] = {
                "sharpe": sharpe,
                "maximum_drawdown": drawdown,
                "bars": bars,
            }
        reasons.extend(local)
        details.append({
            "family": family,
            "candidate_id": candidate.get("deployment_candidate_id"),
            "metric_vectors": vector_facts,
            "missing_validation_metrics": missing_validation,
            "missing_deployment_metrics": missing_deployment,
            "invalid_validation_metrics": invalid_validation,
            "invalid_deployment_metrics": invalid_deployment,
            "reason_codes": list(_unique(local)),
        })
    return GateResult(
        "candidate_metrics", not reasons, _unique(reasons),
        {"candidate_count": len(candidates), "candidates": details},
    )


def _statistics_gate(
    summary: Mapping[str, object],
    research_evidence: Mapping[str, object],
    policy: Mapping[str, object],
) -> GateResult:
    """检查全局台账、多重检验、bootstrap 与邻域稳定性。"""
    reasons: list[str] = []
    details: list[Mapping[str, object]] = []
    ledger = _mapping(research_evidence.get("trial_ledger"))
    header = _mapping(ledger.get("header"))
    trial_rows = _integer(ledger.get("trial_rows"))
    summary_trials = _integer(summary.get("trial_count"))
    header_trials = _integer(header.get("candidate_evaluations"))
    if ledger.get("present") is not True:
        reasons.append("GLOBAL_TRIAL_LEDGER_MISSING")
    if header.get("record_type") != "trial_ledger_header":
        reasons.append("GLOBAL_TRIAL_LEDGER_HEADER_INVALID")
    if (
        trial_rows is None or summary_trials is None or header_trials is None
        or trial_rows <= 0 or summary_trials <= 0 or header_trials <= 0
        or trial_rows != summary_trials or header_trials != summary_trials
    ):
        reasons.append("GLOBAL_TRIAL_LEDGER_COUNT_MISMATCH")
    evaluation_count = _integer(ledger.get("evaluation_id_count"))
    unique_evaluation_count = _integer(
        ledger.get("unique_evaluation_id_count")
    )
    if (
        evaluation_count is None or unique_evaluation_count is None
        or evaluation_count < 0 or unique_evaluation_count < 0
        or evaluation_count != unique_evaluation_count
        or trial_rows is None or evaluation_count != trial_rows
    ):
        reasons.append("GLOBAL_TRIAL_LEDGER_EVALUATION_ID_DUPLICATE")
    if _sequence(ledger.get("missing_registry_candidate_ids")):
        reasons.append("GLOBAL_TRIAL_LEDGER_CANDIDATE_COVERAGE_INCOMPLETE")
    method_fields = {
        "deflated_sharpe_method_version": "DSR_METHOD_EVIDENCE_MISSING",
        "pbo_method_version": "PBO_METHOD_EVIDENCE_MISSING",
        "block_bootstrap_method_version": "BOOTSTRAP_METHOD_EVIDENCE_MISSING",
        "parameter_stability_method_version": "NEIGHBOR_METHOD_EVIDENCE_MISSING",
    }
    for field, code in method_fields.items():
        if _text(summary.get(field)) is None:
            reasons.append(code)
    research = _policy_section(policy, "research")
    for candidate in _eligible_candidates(summary):
        family = _text(candidate.get("family")) or "unknown"
        local: list[str] = []
        statistic_domain_errors: list[str] = []
        for field in (
            "fdr_q", "probability_backtest_overfitting",
            "block_bootstrap_p_value",
            "deflated_sharpe_probability_effective",
            "positive_parameter_neighbor_ratio",
            "median_parameter_neighbor_sharpe_retention",
        ):
            probability = _number(candidate.get(field))
            if probability is None or not 0.0 <= probability <= 1.0:
                statistic_domain_errors.append(field)
        lower_bound = _number(candidate.get("block_bootstrap_sharpe_lower_bound"))
        if lower_bound is None:
            statistic_domain_errors.append("block_bootstrap_sharpe_lower_bound")
        if statistic_domain_errors:
            local.append("STATISTICAL_METRIC_DOMAIN_INVALID")
        checks = (
            ("fdr_q", "maximum_fdr_q", False, "FDR_Q_ABOVE_POLICY"),
            ("probability_backtest_overfitting", "maximum_pbo", False,
             "PBO_ABOVE_POLICY"),
            ("block_bootstrap_p_value", "maximum_block_bootstrap_p_value",
             False, "BOOTSTRAP_P_VALUE_ABOVE_POLICY"),
            ("block_bootstrap_sharpe_lower_bound",
             "minimum_block_bootstrap_sharpe_lower_bound", True,
             "BOOTSTRAP_LOWER_BOUND_BELOW_POLICY"),
            ("deflated_sharpe_probability_effective",
             "minimum_deflated_sharpe_probability", True,
             "DSR_PROBABILITY_BELOW_POLICY"),
            ("positive_parameter_neighbor_ratio",
             "minimum_positive_parameter_neighbor_ratio", True,
             "NEIGHBOR_POSITIVE_RATIO_BELOW_POLICY"),
            ("median_parameter_neighbor_sharpe_retention",
             "minimum_parameter_neighbor_sharpe_retention", True,
             "NEIGHBOR_SHARPE_RETENTION_BELOW_POLICY"),
        )
        for field, threshold_name, minimum, code in checks:
            value = _number(candidate.get(field))
            threshold = _number(research.get(threshold_name))
            if value is None or threshold is None or (
                value < threshold if minimum else value > threshold
            ):
                local.append(code)
        samples = _integer(candidate.get("block_bootstrap_sample_count"))
        minimum_samples = _integer(research.get("minimum_block_bootstrap_samples"))
        if (
            samples is None or samples <= 0 or minimum_samples is None
            or samples < minimum_samples
        ):
            local.append("BOOTSTRAP_SAMPLE_COUNT_BELOW_POLICY")
        neighbors = _integer(candidate.get("parameter_neighbor_count"))
        minimum_neighbors = _integer(
            research.get("minimum_parameter_neighbor_count")
        )
        if (
            neighbors is None or neighbors < 0 or minimum_neighbors is None
            or neighbors < minimum_neighbors
        ):
            local.append("NEIGHBOR_COUNT_BELOW_POLICY")
        reasons.extend(local)
        details.append({
            "family": family,
            "fdr_q": candidate.get("fdr_q"),
            "pbo": candidate.get("probability_backtest_overfitting"),
            "bootstrap_p_value": candidate.get("block_bootstrap_p_value"),
            "bootstrap_sharpe_lower_bound": candidate.get(
                "block_bootstrap_sharpe_lower_bound"
            ),
            "deflated_sharpe_probability": candidate.get(
                "deflated_sharpe_probability_effective"
            ),
            "parameter_neighbor_count": neighbors,
            "statistic_domain_errors": statistic_domain_errors,
            "reason_codes": list(_unique(local)),
        })
    facts = {
        "trial_ledger": dict(ledger),
        "candidate_statistics": details,
    }
    return GateResult(
        "statistical_governance", not reasons, _unique(reasons), facts,
    )


def _scenario_code(kind: str, suffix: str) -> str:
    """生成类型专属的稳定场景原因码。"""
    return f"{_SCENARIO_REASON_PREFIX[kind]}_SCENARIO_{suffix}"


def _source_artifact_matches(
    raw: object,
    verified_artifacts: Mapping[str, object],
    allowed_sources: Sequence[object],
) -> bool:
    """要求场景绑定已复核且类型专属的另一份语义来源制品。"""
    source = _mapping(raw)
    if set(source) != _SOURCE_REFERENCE_FIELDS:
        return False
    name = _text(source.get("name"))
    artifact_kind = _text(source.get("kind"))
    digest = _sha256_text(source.get("sha256"))
    artifact_id = _stable_id(source.get("artifact_id"), "sha256-")
    byte_count = _integer(source.get("bytes"))
    if (
        name is None or name in {
            _INDUSTRY_EVIDENCE_ARTIFACT, _GENERATOR_ATTESTATION_ARTIFACT,
        }
        or artifact_kind is None or _text(source.get("path")) is None
        or digest is None or artifact_id != f"sha256-{digest}"
        or byte_count is None or byte_count <= 0
    ):
        return False
    expected = _mapping(verified_artifacts.get(name))
    if not expected or expected.get("snapshot_verified") is not True:
        return False
    allowed = {
        (_text(record.get("name")), _text(record.get("kind")))
        for raw_record in allowed_sources
        if (record := _mapping(raw_record))
    }
    if (name, artifact_kind) not in allowed:
        return False
    return all(
        source.get(field) == expected.get(field)
        for field in ("name", "kind", "path", "sha256", "artifact_id", "bytes")
    )


def _scenario_identity(scenario: Mapping[str, object]) -> str | None:
    """按除 scenario_id 外的规范载荷复算内容身份。"""
    payload = {
        key: value for key, value in scenario.items() if key != "scenario_id"
    }
    try:
        return stable_identifier("industry-scenario", payload)
    except (TypeError, ValueError):
        return None


def _scenario_semantic_identity(
    kind: str,
    scenario: Mapping[str, object],
) -> str | None:
    """按类型经济参数复算语义身份，排除可随意改名的展示字段。"""
    parameters = _mapping(scenario.get("parameters"))
    payload: dict[str, object] = {
        "scenario_type": kind,
        "method_version": scenario.get("method_version"),
        "family": scenario.get("family"),
        "candidate_id": scenario.get("candidate_id"),
    }
    if kind == "tail":
        payload["tail_probability"] = parameters.get("tail_probability")
        payload["block_length"] = parameters.get("block_length")
    elif kind == "stress":
        payload["stress_definition"] = parameters.get("stress_definition")
        payload["severity"] = parameters.get("severity")
    elif kind == "cost":
        payload.update({
            "cost_tier": parameters.get("cost_tier"),
            "cost_components_bps": scenario.get("cost_components_bps"),
            "total_cost_bps": scenario.get("total_cost_bps"),
        })
    elif kind == "capacity":
        payload.update({
            "notional_quote": scenario.get("notional_quote"),
            "participation_rate": scenario.get("participation_rate"),
            "observed_depth_quote": scenario.get("observed_depth_quote"),
            "impact_bps": scenario.get("impact_bps"),
        })
    try:
        return stable_identifier(f"industry-{kind}-semantic", payload)
    except (TypeError, ValueError):
        return None


def _common_scenario_checks(
    raw: object,
    *,
    kind: str,
    family: str,
    candidate_id: str,
    policy: Mapping[str, object],
    registration_cutoff: datetime | None,
    decision_cutoff: datetime | None,
    verified_artifacts: Mapping[str, object],
) -> tuple[Mapping[str, object], tuple[str, ...], Mapping[str, object]]:
    """验证一条不可变、候选绑定且仅用 OOS/PIT 数据的场景。"""
    prefix = _SCENARIO_REASON_PREFIX[kind]
    if not isinstance(raw, Mapping) or not raw:
        return {}, (f"{prefix}_SCENARIO_SCHEMA_INVALID",), {}
    scenario = {str(key): value for key, value in raw.items()}
    reasons: list[str] = []
    if (
        any(not isinstance(key, str) for key in raw)
        or set(scenario) != _SCENARIO_FIELDS[kind]
    ):
        reasons.append(_scenario_code(kind, "SCHEMA_INVALID"))
    if scenario.get("schema_version") != 1:
        reasons.append(_scenario_code(kind, "SCHEMA_INVALID"))
    if scenario.get("scenario_type") != kind:
        reasons.append(_scenario_code(kind, "SCHEMA_INVALID"))
    if (
        scenario.get("family") != family
        or scenario.get("candidate_id") != candidate_id
    ):
        reasons.append(_scenario_code(kind, "CANDIDATE_BINDING_MISMATCH"))
    if _text(scenario.get("family")) is None or _text(
        scenario.get("candidate_id")
    ) is None:
        reasons.append(_scenario_code(kind, "SCHEMA_INVALID"))
    method = _text(scenario.get("method_version"))
    accepted_methods = {
        str(value) for value in _sequence(
            policy.get(_SCENARIO_METHOD_POLICY_FIELDS[kind])
        )
    }
    if method is None or method not in accepted_methods:
        reasons.append(_scenario_code(kind, "METHOD_NOT_ACCEPTED"))
    scenario_key = _text(scenario.get("scenario_key"))
    if scenario_key is None:
        reasons.append(_scenario_code(kind, "SCHEMA_INVALID"))
    if scenario.get("selection_locked") is not True:
        reasons.append(_scenario_code(kind, "SELECTION_NOT_LOCKED"))
    if scenario.get("walk_forward_oos_only") is not True:
        reasons.append(_scenario_code(kind, "NOT_WALK_FORWARD_OOS"))
    if scenario.get("pit_verified") is not True:
        reasons.append(_scenario_code(kind, "PIT_INVALID"))

    registered_at = _parse_time(scenario.get("registered_at"))

    coverage = _mapping(scenario.get("coverage"))
    if set(coverage) != _COVERAGE_FIELDS:
        reasons.append(_scenario_code(kind, "COVERAGE_SCHEMA_INVALID"))
    coverage_from = _parse_time(coverage.get("from_time"))
    coverage_to = _parse_time(coverage.get("to_time"))
    available_through = _parse_time(coverage.get("available_through"))
    bars = _integer(coverage.get("bars"))
    folds = _integer(coverage.get("folds"))
    coverage_ratio = _number(coverage.get("coverage_ratio"))
    minimum_bars = _integer(policy.get("minimum_scenario_bars"))
    minimum_folds = _integer(policy.get("minimum_scenario_folds"))
    minimum_ratio = _number(policy.get("minimum_scenario_coverage_ratio"))
    if (
        coverage_from is None or coverage_to is None
        or coverage_from >= coverage_to
        or bars is None or minimum_bars is None or bars < minimum_bars
        or folds is None or minimum_folds is None or folds < minimum_folds
        or coverage_ratio is None or minimum_ratio is None
        or not 0.0 <= coverage_ratio <= 1.0
        or coverage_ratio < minimum_ratio
    ):
        reasons.append(_scenario_code(kind, "COVERAGE_INVALID"))
    if (
        available_through is None or registration_cutoff is None
        or decision_cutoff is None or coverage_from is None
        or coverage_to is None or registered_at is None
        or not (
            coverage_from < coverage_to <= available_through
            <= registered_at <= decision_cutoff <= registration_cutoff
        )
    ):
        reasons.append(_scenario_code(kind, "PIT_INVALID"))
    if (
        registered_at is None or decision_cutoff is None
        or registration_cutoff is None
        or registered_at > decision_cutoff
        or registered_at > registration_cutoff
    ):
        reasons.append(_scenario_code(kind, "REGISTRATION_CUTOFF_INVALID"))

    metrics = _mapping(scenario.get("metrics"))
    if set(metrics) != _SCENARIO_METRIC_FIELDS[kind]:
        reasons.append(_scenario_code(kind, "METRICS_SCHEMA_INVALID"))
    numeric_metrics = {
        name: _number(metrics.get(name)) for name in _COMMON_SCENARIO_METRICS
    }
    drawdown = numeric_metrics.get("maximum_drawdown")
    turnover = numeric_metrics.get("turnover")
    if (
        any(value is None for value in numeric_metrics.values())
        or drawdown is None or not 0.0 <= drawdown <= 1.0
        or turnover is None or turnover < 0.0
    ):
        reasons.append(_scenario_code(kind, "METRICS_INVALID"))
    if not _source_artifact_matches(
        scenario.get("source_artifact"),
        verified_artifacts,
        _sequence(policy.get(_SCENARIO_SOURCE_POLICY_FIELDS[kind])),
    ):
        reasons.append(_scenario_code(kind, "SOURCE_ARTIFACT_INVALID"))
    scenario_id = _text(scenario.get("scenario_id"))
    if scenario_id is None or scenario_id != _scenario_identity(scenario):
        reasons.append(_scenario_code(kind, "IDENTITY_INVALID"))
    facts: dict[str, object] = {
        "scenario_id": scenario_id,
        "scenario_key": scenario_key,
        "method_version": method,
        "net_return": numeric_metrics.get("net_return"),
        "bars": bars,
        "folds": folds,
        "coverage_ratio": coverage_ratio,
        "registered_at": (
            registered_at.isoformat() if registered_at is not None else None
        ),
        "available_through": (
            available_through.isoformat()
            if available_through is not None else None
        ),
    }
    return scenario, _unique(reasons), facts


def _type_specific_scenario_checks(
    scenario: Mapping[str, object],
    facts: Mapping[str, object],
    *,
    kind: str,
    policy: Mapping[str, object],
    benchmark_sharpe: float | None,
) -> tuple[tuple[str, ...], Mapping[str, object]]:
    """验证 tail/stress/cost/capacity 各自不可替代的事实。"""
    reasons: list[str] = []
    details = dict(facts)
    parameters = _mapping(scenario.get("parameters"))
    metrics = _mapping(scenario.get("metrics"))
    if set(parameters) != _SCENARIO_PARAMETER_FIELDS[kind]:
        reasons.append(_scenario_code(kind, "PARAMETERS_SCHEMA_INVALID"))
    if kind in {"tail", "stress", "cost"}:
        sharpe = _number(metrics.get("sharpe"))
        net_return = _number(metrics.get("net_return"))
        drawdown = _number(metrics.get("maximum_drawdown"))
        turnover = _number(metrics.get("turnover"))
        minimum_sharpe = _number(
            policy.get(f"minimum_{kind}_scenario_sharpe")
        )
        minimum_return = _number(
            policy.get(f"minimum_{kind}_scenario_net_return")
        )
        maximum_drawdown = _number(
            policy.get(f"maximum_{kind}_scenario_drawdown")
        )
        maximum_turnover = _number(
            policy.get(f"maximum_{kind}_scenario_turnover")
        )
        if (
            sharpe is None or minimum_sharpe is None
            or sharpe < minimum_sharpe
            or net_return is None or minimum_return is None
            or net_return < minimum_return
            or drawdown is None or maximum_drawdown is None
            or drawdown > maximum_drawdown
            or turnover is None or maximum_turnover is None
            or turnover > maximum_turnover
        ):
            reasons.append(_scenario_code(kind, "OUTCOME_BELOW_POLICY"))
        details.update({
            "sharpe": sharpe,
            "maximum_drawdown": drawdown,
            "minimum_policy_sharpe": minimum_sharpe,
            "minimum_policy_net_return": minimum_return,
            "maximum_policy_drawdown": maximum_drawdown,
            "turnover": turnover,
            "maximum_policy_turnover": maximum_turnover,
        })
        if kind == "cost":
            minimum_excess = _number(
                policy.get("minimum_cost_scenario_benchmark_sharpe_excess")
            )
            if (
                benchmark_sharpe is None or sharpe is None
                or minimum_excess is None
                or sharpe - benchmark_sharpe < minimum_excess
            ):
                reasons.append(
                    "COST_SCENARIO_BENCHMARK_OUTCOME_BELOW_POLICY"
                )
            details["benchmark_sharpe"] = benchmark_sharpe
    if kind == "tail":
        probability = _number(parameters.get("tail_probability"))
        block_length = _integer(parameters.get("block_length"))
        expected_shortfall = _number(metrics.get("expected_shortfall"))
        minimum_expected_shortfall = _number(
            policy.get("minimum_tail_expected_shortfall")
        )
        required_probabilities = {
            _number(value)
            for value in _sequence(policy.get("required_tail_probabilities"))
        }
        if (
            probability is None or probability not in required_probabilities
            or block_length is None or block_length <= 0
            or expected_shortfall is None
            or not -1.0 <= expected_shortfall <= 0.0
        ):
            reasons.append(_scenario_code(kind, "SCHEMA_INVALID"))
        if (
            expected_shortfall is None or minimum_expected_shortfall is None
            or expected_shortfall < minimum_expected_shortfall
        ):
            reasons.append(_scenario_code(kind, "EXPECTED_SHORTFALL_BELOW_POLICY"))
        details.update({
            "tail_probability": probability,
            "block_length": block_length,
            "expected_shortfall": expected_shortfall,
            "minimum_policy_expected_shortfall": minimum_expected_shortfall,
        })
    elif kind == "stress":
        definition = _text(parameters.get("stress_definition"))
        severity = _number(parameters.get("severity"))
        accepted_definitions = {
            _text(value)
            for value in _sequence(policy.get("required_stress_definitions"))
        }
        if (
            definition is None or definition not in accepted_definitions
            or severity is None or severity <= 0.0
        ):
            reasons.append(_scenario_code(kind, "SCHEMA_INVALID"))
        details.update({
            "stress_definition": definition,
            "severity": severity,
        })
    elif kind == "cost":
        components = _mapping(scenario.get("cost_components_bps"))
        values = {
            name: _number(components.get(name)) for name in (
                "fee", "half_spread", "slippage", "impact",
            )
        }
        total = _number(scenario.get("total_cost_bps"))
        cost_tier = _text(parameters.get("cost_tier"))
        accepted_tiers = {
            _text(value)
            for value in _sequence(policy.get("required_cost_tiers"))
        }
        if (
            scenario.get("fixed_target") is not True
            or set(components) != _COST_COMPONENT_FIELDS
            or cost_tier is None or cost_tier not in accepted_tiers
            or any(value is None for value in values.values())
            or any(
                value is None or value < 0.0 for value in values.values()
            )
            or total is None or total <= 0.0
            or abs(total - sum(
                value for value in values.values() if value is not None
            )) > 1e-9
        ):
            reasons.append(_scenario_code(kind, "COST_DECOMPOSITION_INVALID"))
        details["total_cost_bps"] = total
        details["cost_tier"] = cost_tier
    elif kind == "capacity":
        depth_horizon = _integer(parameters.get("depth_horizon_seconds"))
        depth_quantile = _number(parameters.get("depth_quantile"))
        notional = _number(scenario.get("notional_quote"))
        participation = _number(scenario.get("participation_rate"))
        depth = _number(scenario.get("observed_depth_quote"))
        impact = _number(scenario.get("impact_bps"))
        minimum_notional = _number(policy.get("minimum_capacity_notional_quote"))
        maximum_participation = _number(
            policy.get("maximum_capacity_participation_rate")
        )
        maximum_impact = _number(policy.get("maximum_capacity_impact_bps"))
        if (
            depth_horizon is None or depth_horizon <= 0
            or depth_quantile is None or not 0.0 < depth_quantile <= 1.0
            or
            notional is None or minimum_notional is None
            or notional < minimum_notional
            or participation is None or maximum_participation is None
            or not 0.0 < participation <= maximum_participation
            or depth is None or depth <= 0.0
            or impact is None or maximum_impact is None
            or not 0.0 <= impact <= maximum_impact
            or notional > depth * participation + 1e-9
        ):
            reasons.append(_scenario_code(kind, "ECONOMICS_INVALID"))
        details.update({
            "notional_quote": notional,
            "participation_rate": participation,
            "observed_depth_quote": depth,
            "impact_bps": impact,
            "depth_horizon_seconds": depth_horizon,
            "depth_quantile": depth_quantile,
        })
    return _unique(reasons), details


def _validate_candidate_scenarios(
    candidate: Mapping[str, object],
    evidence: Mapping[str, object],
    *,
    policy: Mapping[str, object],
    registration_cutoff: datetime | None,
    decision_cutoff: datetime | None,
    verified_artifacts: Mapping[str, object],
    benchmark_sharpe: float | None,
) -> tuple[tuple[str, ...], Mapping[str, object]]:
    """逐候选验证四类场景，并只统计完全有效且语义唯一的记录。"""
    family = _text(candidate.get("family")) or "unknown"
    candidate_id = _text(candidate.get("deployment_candidate_id")) or "unknown"
    reasons: list[str] = []
    counts: dict[str, int] = {}
    scenario_facts: dict[str, object] = {}
    cost_points: list[tuple[str, float, float]] = []
    for kind in _SCENARIO_KINDS:
        collection = _SCENARIO_COLLECTIONS[kind]
        raw_scenarios = _sequence(evidence.get(collection))
        seen_ids: set[str] = set()
        seen_keys: set[str] = set()
        seen_semantics: set[str] = set()
        valid: list[Mapping[str, object]] = []
        for raw in raw_scenarios:
            scenario, common_reasons, facts = _common_scenario_checks(
                raw,
                kind=kind,
                family=family,
                candidate_id=candidate_id,
                policy=policy,
                registration_cutoff=registration_cutoff,
                decision_cutoff=decision_cutoff,
                verified_artifacts=verified_artifacts,
            )
            local = list(common_reasons)
            specific_reasons, details = _type_specific_scenario_checks(
                scenario,
                facts,
                kind=kind,
                policy=policy,
                benchmark_sharpe=benchmark_sharpe,
            )
            local.extend(specific_reasons)
            scenario_id = _text(facts.get("scenario_id"))
            scenario_key = _text(facts.get("scenario_key"))
            semantic_id = _scenario_semantic_identity(kind, scenario)
            if (
                scenario_id is not None and scenario_id in seen_ids
                or scenario_key is not None and scenario_key in seen_keys
            ):
                local.append(_scenario_code(kind, "DUPLICATE"))
            if semantic_id is None:
                local.append(_scenario_code(kind, "SEMANTIC_IDENTITY_INVALID"))
            elif semantic_id in seen_semantics:
                local.append(_scenario_code(kind, "SEMANTIC_DUPLICATE"))
            if scenario_id is not None:
                seen_ids.add(scenario_id)
            if scenario_key is not None:
                seen_keys.add(scenario_key)
            if semantic_id is not None:
                seen_semantics.add(semantic_id)
            if not local:
                details = {**details, "semantic_id": semantic_id}
                valid.append(details)
                if kind == "cost":
                    total = _number(details.get("total_cost_bps"))
                    net_return = _number(details.get("net_return"))
                    tier = _text(details.get("cost_tier"))
                    if (
                        total is not None and net_return is not None
                        and tier is not None
                    ):
                        cost_points.append((tier, total, net_return))
            reasons.extend(local)
        minimum = _integer(policy.get(_SCENARIO_MINIMUM_POLICY_FIELDS[kind]))
        counts[kind] = len(valid)
        scenario_facts[kind] = valid
        if minimum is None or len(valid) < minimum:
            reasons.append(f"{_SCENARIO_REASON_PREFIX[kind]}_SCENARIO_EVIDENCE_INCOMPLETE")
    distinct_costs = {point[1] for point in cost_points}
    minimum_cost = _integer(policy.get("minimum_cost_scenarios"))
    if minimum_cost is None or len(distinct_costs) < minimum_cost:
        reasons.append("COST_SCENARIO_COST_GRID_INSUFFICIENT")
    required_tiers = tuple(
        value for raw in _sequence(policy.get("required_cost_tiers"))
        if (value := _text(raw)) is not None
    )
    cost_by_tier: dict[str, tuple[float, float]] = {}
    duplicate_cost_tiers = False
    for tier, total, net_return in cost_points:
        if tier in cost_by_tier:
            duplicate_cost_tiers = True
        cost_by_tier[tier] = (total, net_return)
    if duplicate_cost_tiers:
        reasons.append("COST_SCENARIO_TIER_DUPLICATE")
    if set(cost_by_tier) != set(required_tiers):
        reasons.append("COST_SCENARIO_REQUIRED_TIERS_INCOMPLETE")
    baseline = _number(policy.get("policy_baseline_total_cost_bps"))
    baseline_point = cost_by_tier.get("policy_baseline")
    if (
        baseline is None or baseline_point is None
        or abs(baseline_point[0] - baseline) > 1e-9
    ):
        reasons.append("COST_SCENARIO_POLICY_BASELINE_MISSING")
    ordered_costs = [
        cost_by_tier[tier] for tier in required_tiers if tier in cost_by_tier
    ]
    minimum_step = _number(policy.get("minimum_cost_grid_step_bps"))
    if (
        minimum_step is None or len(ordered_costs) != len(required_tiers)
        or any(
            current[0] - previous[0] < minimum_step
            for previous, current in zip(ordered_costs, ordered_costs[1:])
        )
    ):
        reasons.append("COST_SCENARIO_COST_GRID_NOT_STRICTLY_INCREASING")
    if any(
        current[1] > previous[1] + 1e-12
        for previous, current in zip(ordered_costs, ordered_costs[1:])
    ):
        reasons.append("COST_SCENARIO_NET_RETURN_MONOTONICITY_VIOLATION")
    tail_probabilities = {
        _number(item.get("tail_probability"))
        for item in _sequence(scenario_facts.get("tail"))
        if isinstance(item, Mapping)
    }
    required_tail_probabilities = {
        _number(value)
        for value in _sequence(policy.get("required_tail_probabilities"))
    }
    if tail_probabilities != required_tail_probabilities:
        reasons.append("TAIL_RISK_SCENARIO_PROBABILITY_GRID_INCOMPLETE")
    stress_definitions = {
        _text(item.get("stress_definition"))
        for item in _sequence(scenario_facts.get("stress"))
        if isinstance(item, Mapping)
    }
    required_stress_definitions = {
        _text(value)
        for value in _sequence(policy.get("required_stress_definitions"))
    }
    if stress_definitions != required_stress_definitions:
        reasons.append("STRESS_SCENARIO_DEFINITION_SET_INCOMPLETE")
    capacity_notionals = {
        _number(item.get("notional_quote"))
        for item in _sequence(scenario_facts.get("capacity"))
        if isinstance(item, Mapping)
    }
    minimum_capacity = _integer(policy.get("minimum_capacity_scenarios"))
    if minimum_capacity is None or len(capacity_notionals) < minimum_capacity:
        reasons.append("CAPACITY_SCENARIO_NOTIONAL_GRID_INSUFFICIENT")
    return _unique(reasons), {
        "family": family,
        "candidate_id": candidate_id,
        "valid_scenario_counts": counts,
        "valid_scenarios": scenario_facts,
    }


def _industry_evidence_bindings(
    payload: Mapping[str, object],
    artifact_evidence: Mapping[str, object],
    research_evidence: Mapping[str, object],
    policy: Mapping[str, object],
) -> tuple[tuple[str, ...], datetime | None, datetime | None]:
    """把行业制品及独立 generator attestation 绑定到 research manifest。"""
    reasons: list[str] = []
    manifest = _mapping(research_evidence.get("manifest"))
    if set(payload) != _INDUSTRY_EVIDENCE_FIELDS:
        reasons.append("INDUSTRY_EVIDENCE_SCHEMA_INVALID")
    if payload.get("schema_version") != 1:
        reasons.append("INDUSTRY_EVIDENCE_SCHEMA_INVALID")
    accepted = {
        str(value) for value in _sequence(
            policy.get("accepted_industry_evidence_method_versions")
        )
    }
    if _text(payload.get("method_version")) not in accepted:
        reasons.append("INDUSTRY_EVIDENCE_METHOD_NOT_ACCEPTED")
    identity_fields = {
        "run_id": lambda value: _stable_id(value, "research-run-"),
        "research_identity": lambda value: _stable_id(
            value, "research-identity-",
        ),
        "config_hash": _sha256_text,
        "input_receipt_sha256": _sha256_text,
    }
    for field, validator in identity_fields.items():
        manifest_value = validator(manifest.get(field))
        payload_value = validator(payload.get(field))
        if manifest_value is None or payload_value is None:
            reasons.append("INDUSTRY_EVIDENCE_RESEARCH_IDENTITY_INVALID")
        elif payload_value != manifest_value:
            reasons.append("INDUSTRY_EVIDENCE_RESEARCH_BINDING_MISMATCH")
    registration_cutoff = _parse_time(manifest.get("execution_evaluated_at"))
    decision_cutoff = _parse_time(manifest.get("decision_time"))
    payload_decision = _parse_time(payload.get("decision_time"))
    generated_at = _parse_time(payload.get("generated_at"))
    if (
        generated_at is None or registration_cutoff is None
        or decision_cutoff is None or payload_decision != decision_cutoff
        or not decision_cutoff <= generated_at <= registration_cutoff
    ):
        reasons.append("INDUSTRY_EVIDENCE_GENERATION_CUTOFF_INVALID")
    if decision_cutoff is None:
        reasons.append("INDUSTRY_EVIDENCE_PIT_CUTOFF_INVALID")

    attestation = _mapping(
        artifact_evidence.get("generator_attestation_payload")
    )
    attestation_artifact = _mapping(
        artifact_evidence.get("generator_attestation_artifact")
    )
    if set(attestation) != _GENERATOR_ATTESTATION_FIELDS:
        reasons.append("INDUSTRY_EVIDENCE_GENERATOR_ATTESTATION_SCHEMA_INVALID")
    if attestation.get("schema_version") != 1:
        reasons.append("INDUSTRY_EVIDENCE_GENERATOR_ATTESTATION_SCHEMA_INVALID")
    accepted_attestation_methods = {
        _text(value)
        for value in _sequence(
            policy.get("accepted_generator_attestation_method_versions")
        )
    }
    attestation_method = _text(attestation.get("method_version"))
    generator_implemented = (
        policy.get("industry_evidence_generator_status") == "implemented"
    )
    if not generator_implemented:
        reasons.append("INDUSTRY_EVIDENCE_GENERATOR_NOT_IMPLEMENTED")
    elif attestation_method not in accepted_attestation_methods:
        reasons.append("INDUSTRY_EVIDENCE_GENERATOR_ATTESTATION_METHOD_INVALID")
    generator_id = _text(attestation.get("generator_id"))
    generator_code_hash = _sha256_text(
        attestation.get("generator_code_sha256")
    )
    allowed_generators = {
        (
            _text(record.get("generator_id")),
            _text(record.get("method_version")),
            _sha256_text(record.get("generator_code_sha256")),
        )
        for raw in _sequence(policy.get("allowed_industry_evidence_generators"))
        if (record := _mapping(raw))
    }
    if generator_implemented and (
        generator_id, attestation_method, generator_code_hash
    ) not in allowed_generators:
        reasons.append("INDUSTRY_EVIDENCE_GENERATOR_NOT_APPROVED")
    if policy.get("scenario_source_replay_status") != "implemented":
        reasons.append("INDUSTRY_EVIDENCE_SOURCE_REPLAY_NOT_IMPLEMENTED")
    if (
        attestation.get("independent_from_strategy_search") is not True
        or attestation.get("numeric_replay_verified") is not True
        or attestation.get("pit_replay_verified") is not True
    ):
        reasons.append("INDUSTRY_EVIDENCE_GENERATOR_ATTESTATION_INCOMPLETE")
    attestation_id = _text(attestation.get("attestation_id"))
    try:
        expected_attestation_id = stable_identifier(
            "industry-generator-attestation",
            {
                key: value for key, value in attestation.items()
                if key != "attestation_id"
            },
        )
    except (TypeError, ValueError):
        expected_attestation_id = None
    if attestation_id is None or attestation_id != expected_attestation_id:
        reasons.append("INDUSTRY_EVIDENCE_GENERATOR_ATTESTATION_IDENTITY_INVALID")
    for field, validator in identity_fields.items():
        manifest_value = validator(manifest.get(field))
        attestation_value = validator(attestation.get(field))
        if manifest_value is None or attestation_value is None:
            reasons.append("INDUSTRY_EVIDENCE_GENERATOR_BINDING_INVALID")
        elif attestation_value != manifest_value:
            reasons.append("INDUSTRY_EVIDENCE_GENERATOR_BINDING_INVALID")
    attestation_decision = _parse_time(attestation.get("decision_time"))
    attestation_generated = _parse_time(attestation.get("generated_at"))
    attested_at = _parse_time(attestation.get("attested_at"))
    if (
        decision_cutoff is None or generated_at is None
        or registration_cutoff is None
        or attestation_decision != decision_cutoff
        or attestation_generated != generated_at
        or attested_at is None
        or not generated_at <= attested_at <= registration_cutoff
    ):
        reasons.append("INDUSTRY_EVIDENCE_GENERATOR_CUTOFF_INVALID")
    evidence_artifact = _mapping(artifact_evidence.get("artifact"))
    evidence_hash = _sha256_text(evidence_artifact.get("sha256"))
    if (
        evidence_hash is None
        or attestation.get("industry_evidence_sha256") != evidence_hash
        or attestation_artifact.get("kind")
        != "industry_evidence_generator_attestation"
    ):
        reasons.append("INDUSTRY_EVIDENCE_GENERATOR_ARTIFACT_BINDING_INVALID")
    source_ids = sorted({
        artifact_id
        for raw_candidate in _sequence(payload.get("candidate_evidence"))
        for collection in _SCENARIO_COLLECTIONS.values()
        for raw_scenario in _sequence(_mapping(raw_candidate).get(collection))
        if (
            artifact_id := _stable_id(
                _mapping(_mapping(raw_scenario).get("source_artifact")).get(
                    "artifact_id"
                ),
                "sha256-",
            )
        ) is not None
    })
    raw_attested_source_ids = _sequence(attestation.get("source_artifact_ids"))
    attested_source_ids = tuple(
        value for raw in raw_attested_source_ids
        if (value := _stable_id(raw, "sha256-")) is not None
    )
    if (
        len(attested_source_ids) != len(raw_attested_source_ids)
        or tuple(sorted(set(attested_source_ids))) != attested_source_ids
        or list(attested_source_ids) != source_ids
    ):
        reasons.append("INDUSTRY_EVIDENCE_GENERATOR_SOURCE_BINDING_INVALID")
    return _unique(reasons), registration_cutoff, decision_cutoff


def _robustness_gate(
    summary: Mapping[str, object],
    research_evidence: Mapping[str, object],
    policy: Mapping[str, object],
) -> GateResult:
    """只接受受保护独立制品中的尾部、压力、成本与容量场景。"""
    reasons: list[str] = []
    research = _policy_section(policy, "research")
    artifact_evidence = _mapping(research_evidence.get("industry_evidence"))
    payload = _mapping(artifact_evidence.get("payload"))
    if artifact_evidence.get("present") is not True:
        reasons.append("INDUSTRY_EVIDENCE_ARTIFACT_MISSING")
    elif artifact_evidence.get("verified") is not True:
        reasons.append("INDUSTRY_EVIDENCE_ARTIFACT_UNVERIFIED")
    manifest = _mapping(research_evidence.get("manifest"))
    registration_cutoff = _parse_time(manifest.get("execution_evaluated_at"))
    decision_cutoff = _parse_time(manifest.get("decision_time"))
    if artifact_evidence.get("verified") is True:
        binding_reasons, registration_cutoff, decision_cutoff = (
            _industry_evidence_bindings(
                payload, artifact_evidence, research_evidence, research,
            )
        )
        reasons.extend(binding_reasons)
    verified_artifacts = _mapping(
        artifact_evidence.get("verified_artifacts")
    )
    raw_candidate_evidence = _sequence(payload.get("candidate_evidence"))
    evidence_by_key: dict[tuple[str, str], Mapping[str, object]] = {}
    duplicate_candidate_binding = False
    for raw in raw_candidate_evidence:
        item = _mapping(raw)
        if (
            not isinstance(raw, Mapping)
            or set(item) != _CANDIDATE_EVIDENCE_FIELDS
        ):
            reasons.append("INDUSTRY_EVIDENCE_CANDIDATE_SCHEMA_INVALID")
        family = _text(item.get("family"))
        candidate_id = _text(item.get("candidate_id"))
        if family is None or candidate_id is None:
            reasons.append("INDUSTRY_EVIDENCE_CANDIDATE_SCHEMA_INVALID")
            continue
        key = (family, candidate_id)
        if key in evidence_by_key:
            duplicate_candidate_binding = True
        evidence_by_key[key] = item
    if duplicate_candidate_binding:
        reasons.append("INDUSTRY_EVIDENCE_CANDIDATE_DUPLICATE")
    candidates = _eligible_candidates(summary)
    expected_keys = {
        (
            _text(candidate.get("family")) or "unknown",
            _text(candidate.get("deployment_candidate_id")) or "unknown",
        )
        for candidate in candidates
    }
    if set(evidence_by_key) != expected_keys:
        reasons.append("INDUSTRY_EVIDENCE_CANDIDATE_COVERAGE_INCOMPLETE")
    benchmark = _mapping(_mapping(summary.get("ablations")).get("fixed_long"))
    benchmark_sharpe = _number(benchmark.get("sharpe"))
    candidate_facts: list[Mapping[str, object]] = []
    for candidate in candidates:
        key = (
            _text(candidate.get("family")) or "unknown",
            _text(candidate.get("deployment_candidate_id")) or "unknown",
        )
        candidate_reasons, facts = _validate_candidate_scenarios(
            candidate,
            evidence_by_key.get(key, {}),
            policy=research,
            registration_cutoff=registration_cutoff,
            decision_cutoff=decision_cutoff,
            verified_artifacts=verified_artifacts,
            benchmark_sharpe=benchmark_sharpe,
        )
        reasons.extend(candidate_reasons)
        candidate_facts.append({
            **facts,
            "reason_codes": list(candidate_reasons),
        })
    minimum_excess = _number(research.get("minimum_benchmark_sharpe_excess"))
    candidate_sharpes = [
        _number(_mapping(item.get("validation_metrics")).get("sharpe"))
        for item in candidates
    ]
    if benchmark_sharpe is None:
        reasons.append("BENCHMARK_EVIDENCE_MISSING")
    elif (
        minimum_excess is None or not candidate_sharpes
        or any(
            value is None or value - benchmark_sharpe < minimum_excess
            for value in candidate_sharpes
        )
    ):
        reasons.append("BENCHMARK_SHARPE_EXCESS_BELOW_POLICY")
    config = _mapping(research_evidence.get("research_config"))
    configured_capacity = _number(
        _mapping(config.get("cost_model")).get("capacity_notional_quote")
    )
    legacy_capacity_scores = [
        _number(_mapping(item.get("validation_metrics")).get("capacity_score"))
        for item in candidates
    ]
    return GateResult(
        "robustness_evidence",
        not reasons,
        _unique(reasons),
        {
            "industry_evidence_artifact": dict(
                _mapping(artifact_evidence.get("artifact"))
            ),
            "industry_evidence_error": artifact_evidence.get("error"),
            "generator_attestation_artifact": dict(_mapping(
                artifact_evidence.get("generator_attestation_artifact")
            )),
            "generator_attestation_method_version": _mapping(
                artifact_evidence.get("generator_attestation_payload")
            ).get("method_version"),
            "scenario_numbers_replayed_by_checker": False,
            "independent_generator_attestation_required": True,
            "generator_attestation_is_explicit_trust_boundary": True,
            "industry_evidence_generator_status": research.get(
                "industry_evidence_generator_status"
            ),
            "scenario_source_replay_status": research.get(
                "scenario_source_replay_status"
            ),
            "candidate_evidence": candidate_facts,
            "benchmark_sharpe": benchmark_sharpe,
            "candidate_oos_sharpes": candidate_sharpes,
            "legacy_capacity_proxy_is_admission_evidence": False,
            "legacy_configured_capacity_notional_quote": configured_capacity,
            "legacy_candidate_capacity_scores": legacy_capacity_scores,
        },
    )


def _forward_gate(evidence: Mapping[str, object], policy: Mapping[str, object]) -> GateResult:
    """要求封存前向终态通过且完整覆盖。"""
    reasons: list[str] = []
    vintages = tuple(_mapping(item) for item in _sequence(evidence.get("vintages")))
    if evidence.get("registry_present") is not True:
        reasons.append("GOVERNANCE_REGISTRY_MISSING")
    if evidence.get("read_error") is not None:
        reasons.append("GOVERNANCE_REGISTRY_READ_FAILED")
    if not vintages:
        reasons.append("HOLDOUT_VINTAGE_MISSING")
    active = [item for item in vintages if item.get("status") == "sealed"]
    consumed = [item for item in vintages if item.get("status") == "consumed"]
    if active:
        reasons.append("ACTIVE_HOLDOUT_NOT_TERMINAL")
    if not consumed:
        reasons.append("CONSUMED_HOLDOUT_MISSING")
    forward = _policy_section(policy, "forward")
    required_verdict = _text(forward.get("required_terminal_verdict"))
    minimum_coverage = _number(forward.get("minimum_prediction_coverage_ratio"))
    for item in (*active, *consumed):
        if item.get("plan_id") is None:
            reasons.append("FROZEN_FORWARD_PLAN_MISSING")
        if item.get("plan_artifact_state") != "verified":
            reasons.append("FROZEN_FORWARD_PLAN_ARTIFACT_INVALID")
        if item.get("decision_grid_valid") is not True:
            reasons.append("FORWARD_DECISION_GRID_INVALID")
        coverage = _number(item.get("prediction_coverage_ratio"))
        if (
            coverage is None or minimum_coverage is None
            or coverage < minimum_coverage
        ):
            reasons.append("FORWARD_PREDICTION_COVERAGE_BELOW_POLICY")
        if item.get("duplicate_prediction_times") != 0:
            reasons.append("FORWARD_PREDICTION_TIME_DUPLICATE")
        if item.get("prediction_artifact_failures") != 0:
            reasons.append("FORWARD_PREDICTION_ARTIFACT_INVALID")
    if any(item.get("terminal_verdict") != required_verdict for item in consumed):
        reasons.append("HOLDOUT_TERMINAL_VERDICT_NOT_PASSED")
    return GateResult(
        "sealed_forward", not reasons, _unique(reasons), {
            "registry_path": evidence.get("registry_path"),
            "active_vintage_count": len(active),
            "consumed_vintage_count": len(consumed),
            "vintages": [dict(item) for item in vintages],
            "read_error": evidence.get("read_error"),
        },
    )


def _paper_gate(evidence: Mapping[str, object], policy: Mapping[str, object]) -> GateResult:
    """检查 paper 浸泡时长、决策、账本、对账与零真实写。"""
    reasons: list[str] = []
    paper = _policy_section(policy, "paper")
    if evidence.get("execution_root_provided") is not True:
        reasons.append("PAPER_EXECUTION_ROOT_NOT_PROVIDED")
    if evidence.get("task_log_present") is not True:
        reasons.append("PAPER_TASK_LOG_MISSING")
    if evidence.get("ledger_present") is not True:
        reasons.append("PAPER_DIFFERENCE_LEDGER_MISSING")
    if evidence.get("reconciliation_present") is not True:
        reasons.append("PAPER_RECONCILIATION_LEDGER_MISSING")
    if evidence.get("read_error") is not None:
        reasons.append("PAPER_EVIDENCE_READ_FAILED")
    comparisons = (
        ("paper_duration_hours", "minimum_duration_hours",
         "PAPER_DURATION_BELOW_POLICY"),
        ("paper_decisions", "minimum_decisions",
         "PAPER_DECISION_COUNT_BELOW_POLICY"),
        ("ledger_rows", "minimum_ledger_rows",
         "PAPER_LEDGER_ROWS_BELOW_POLICY"),
        ("reconciled_decisions", "minimum_reconciled_decisions",
         "PAPER_RECONCILIATION_COUNT_BELOW_POLICY"),
    )
    for fact_name, threshold_name, code in comparisons:
        value = _number(evidence.get(fact_name))
        threshold = _number(paper.get(threshold_name))
        if value is None or threshold is None or value < threshold:
            reasons.append(code)
    error_ratio = _number(evidence.get("paper_error_ratio"))
    maximum_error = _number(paper.get("maximum_error_ratio"))
    if error_ratio is None or maximum_error is None or error_ratio > maximum_error:
        reasons.append("PAPER_ERROR_RATIO_ABOVE_POLICY")
    if evidence.get("zero_real_writes_proven") is not True:
        reasons.append("PAPER_ZERO_REAL_WRITE_NOT_PROVEN")
    return GateResult(
        "paper_soak", not reasons, _unique(reasons), dict(evidence),
    )


def _execution_gate(
    evidence: Mapping[str, object],
    policy: Mapping[str, object],
    inherited_trade_environment_names: Sequence[str],
) -> GateResult:
    """检查执行风控、紧急停止开关和权限隔离证据。"""
    reasons: list[str] = []
    if inherited_trade_environment_names:
        reasons.append("RESEARCH_PROCESS_INHERITED_TRADE_CREDENTIALS")
    if evidence.get("attestation_present") is not True:
        reasons.append("EXECUTION_SAFETY_ATTESTATION_MISSING")
    if evidence.get("read_error") is not None:
        reasons.append("EXECUTION_SAFETY_ATTESTATION_READ_FAILED")
    attestation = _mapping(evidence.get("attestation"))
    controls = _mapping(attestation.get("controls"))
    permissions = _mapping(attestation.get("permissions"))
    execution = _policy_section(policy, "execution")
    missing_controls = sorted(
        str(name) for name in _sequence(execution.get("required_controls"))
        if controls.get(str(name)) is not True
    )
    missing_permissions = sorted(
        str(name) for name in _sequence(execution.get("required_permissions"))
        if permissions.get(str(name)) is not True
    )
    if missing_controls:
        reasons.append("EXECUTION_REQUIRED_CONTROL_NOT_PROVEN")
    if "independent_kill_switch" in missing_controls:
        reasons.append("INDEPENDENT_KILL_SWITCH_NOT_PROVEN")
    if missing_permissions:
        reasons.append("EXECUTION_PERMISSION_ISOLATION_NOT_PROVEN")
    test_run = _mapping(attestation.get("test_run"))
    if test_run.get("passed") is not True:
        reasons.append("EXECUTION_SAFETY_TEST_RUN_NOT_PASSED")
    if attestation.get("mode") not in {"dry-run", "paper"}:
        reasons.append("EXECUTION_ATTESTATION_MODE_NOT_SAFE")
    if attestation.get("write_touched") != []:
        reasons.append("EXECUTION_ZERO_REAL_WRITE_NOT_PROVEN")
    if attestation.get("live_enabled") is not False:
        reasons.append("EXECUTION_LIVE_DISABLED_NOT_PROVEN")
    return GateResult(
        "execution_safety", not reasons, _unique(reasons), {
            "attestation_path": evidence.get("attestation_path"),
            "missing_controls": missing_controls,
            "missing_permissions": missing_permissions,
            "inherited_trade_environment_names": sorted(
                set(inherited_trade_environment_names)
            ),
            "attested_mode": attestation.get("mode"),
            "test_run": dict(test_run),
        },
    )


def evaluate_industry_strategy_readiness(
    policy: Mapping[str, object],
    evidence: Mapping[str, object],
    *,
    evaluated_at: datetime,
    inherited_trade_environment_names: Sequence[str] = (),
) -> Mapping[str, object]:
    """纯函数评估项目准入门禁，人工实盘授权始终在外部。"""
    reference = (
        evaluated_at.replace(tzinfo=UTC)
        if evaluated_at.tzinfo is None
        else evaluated_at.astimezone(UTC)
    )
    research_evidence = _mapping(evidence.get("research"))
    summary = _mapping(research_evidence.get("summary"))
    policy_gate = _policy_gate(policy)
    gates = (
        (
            policy_gate,
            _manifest_gate(research_evidence, policy),
            _candidate_gate(summary, policy),
            _statistics_gate(summary, research_evidence, policy),
            _robustness_gate(summary, research_evidence, policy),
            _forward_gate(_mapping(evidence.get("forward")), policy),
            _paper_gate(_mapping(evidence.get("paper")), policy),
            _execution_gate(
                _mapping(evidence.get("execution")), policy,
                inherited_trade_environment_names,
            ),
        ) if policy_gate.passed else (policy_gate,)
    )
    blocking_failures = tuple(gate for gate in gates if gate.blocking and not gate.passed)
    technically_ready = not blocking_failures
    external_gate = GateResult(
        "external_live_approval",
        False,
        ("LIVE_APPROVAL_REMAINS_EXTERNAL",),
        {
            "required": True,
            "satisfied_by_checker": False,
            "authority": "human_only",
        },
        blocking=False,
    )
    all_gates = (*gates, external_gate)
    return {
        "schema_version": 1,
        "method_version": INDUSTRY_READINESS_METHOD_VERSION,
        "evaluated_at": reference.isoformat(),
        "policy_id": policy.get("policy_id"),
        "policy_scope": policy.get("policy_scope"),
        "verdict": (
            "READY_FOR_EXTERNAL_LIVE_APPROVAL" if technically_ready
            else "NOT_READY"
        ),
        "technically_ready_for_external_live_approval": technically_ready,
        "live_authorized": False,
        "automated_promotion_performed": False,
        "read_only": True,
        "network_used": False,
        "writes_performed": [],
        "blocking_reason_codes": list(_unique([
            code for gate in blocking_failures for code in gate.reason_codes
        ])),
        "gates": [gate.as_record() for gate in all_gates],
    }


def industry_strategy_readiness(
    repository_root: Path,
    policy_path: Path,
    *,
    manifest_path: Path | None = None,
    governance_registry_path: Path | None = None,
    governance_artifact_root: Path | None = None,
    execution_root: Path | None = None,
    reference_time: datetime | None = None,
    inherited_trade_environment_names: Sequence[str] = (),
) -> Mapping[str, object]:
    """加载政策、只读收集证据并执行失败关闭评估。"""
    policy = load_industry_readiness_policy(policy_path)
    evidence = collect_industry_readiness_evidence(
        repository_root,
        policy,
        manifest_path,
        governance_registry_path,
        governance_artifact_root,
        execution_root,
    )
    return evaluate_industry_strategy_readiness(
        policy,
        evidence,
        evaluated_at=reference_time or datetime.now(UTC),
        inherited_trade_environment_names=inherited_trade_environment_names,
    )
