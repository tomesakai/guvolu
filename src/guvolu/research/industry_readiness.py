"""以只读、失败关闭方式检查策略是否达到项目准入政策。

本模块只给出 ``NOT_READY`` 或 ``READY_FOR_EXTERNAL_LIVE_APPROVAL``。
它不写治理库、配置或执行账，不执行晋级，不授权实盘，也不访问网络。
配置阈值是 guvolu 项目政策，不是量化行业的普遍真理（G-06、A-01）。
"""
from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from guvolu.research.provenance import sha256_file
from guvolu.research.verification import verify_research_artifact_integrity

INDUSTRY_READINESS_METHOD_VERSION = "industry-strategy-readiness-v1"
POLICY_SCOPE = "project_admission_policy_not_universal_truth"
_REQUIRED_ARTIFACTS = {
    "candidate_registry", "config", "summary_json", "trial_ledger",
}
_REQUIRED_CANDIDATE_METRICS = {
    "annual_return", "annual_turnover", "annual_volatility", "bars",
    "capacity_score", "cost", "exposure", "hit_rate", "maximum_drawdown",
    "net_return", "p_value", "sharpe", "turnover",
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


def load_industry_readiness_policy(path: Path) -> Mapping[str, object]:
    """读取并验证版本化项目准入政策。"""
    policy = _read_object(path)
    if policy.get("schema_version") != 1:
        raise ValueError("准入政策 schema_version 必须为 1")
    if policy.get("policy_scope") != POLICY_SCOPE:
        raise ValueError("准入政策必须声明其不是普遍真理")
    if _text(policy.get("policy_id")) is None:
        raise ValueError("准入政策缺少 policy_id")
    for name in ("research", "forward", "paper", "execution", "evidence_paths"):
        _policy_section(policy, name)
    return policy


def _research_evidence(
    root: Path,
    manifest_path: Path | None,
) -> Mapping[str, object]:
    """只读复核 manifest，并收集 summary 与全局试验台账。"""
    try:
        integrity = verify_research_artifact_integrity(root, manifest_path)
    except (OSError, ValueError) as error:
        return {
            "verified": False,
            "verification_error": f"{type(error).__name__}: {error}",
            "summary": {},
            "research_config": {},
            "trial_ledger": {},
            "manifest": {},
            "checked_artifacts": [],
        }
    manifest = integrity.manifest
    summary = integrity.summary
    config_payload: Mapping[str, object] = {}
    trial_payload: Mapping[str, object] = {}
    config_path = integrity.artifact_paths.get("config")
    ledger_path = integrity.artifact_paths.get("trial_ledger")
    try:
        if config_path is not None:
            config_payload = _read_object(config_path)
        if ledger_path is not None:
            rows = _read_json_lines(ledger_path)
            header = rows[0] if rows else {}
            trials = tuple(row for row in rows if row.get("record_type") == "trial")
            evaluation_ids = [
                value for row in trials
                if (value := _text(row.get("evaluation_id"))) is not None
            ]
            registry = _mapping(integrity.candidate_registry)
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
        "run_id": manifest.get("run_id"),
        "manifest_path": str(integrity.manifest_path),
        "manifest_sha256": integrity.manifest_sha256,
        "manifest": dict(manifest),
        "summary": dict(summary),
        "research_config": dict(config_payload),
        "trial_ledger": dict(trial_payload),
        "checked_artifacts": list(integrity.checked_artifacts),
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


def _manifest_gate(evidence: Mapping[str, object], policy: Mapping[str, object]) -> GateResult:
    """检查 clean、decision-grade 与 CPU 精确管线证据。"""
    reasons: list[str] = []
    summary = _mapping(evidence.get("summary"))
    manifest = _mapping(evidence.get("manifest"))
    code = _mapping(manifest.get("code_identity"))
    checked = {str(item) for item in _sequence(evidence.get("checked_artifacts"))}
    if evidence.get("verified") is not True:
        reasons.append("RESEARCH_MANIFEST_VERIFICATION_FAILED")
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
        },
    )


def _eligible_candidates(summary: Mapping[str, object]) -> tuple[Mapping[str, object], ...]:
    """读取被研究 summary 标记为 paper eligible 的候选。"""
    return tuple(
        item for raw in _sequence(summary.get("family_evaluations"))
        if (item := _mapping(raw)).get("eligible") is True
        and item.get("mode") == "paper"
    )


def _candidate_gate(summary: Mapping[str, object], policy: Mapping[str, object]) -> GateResult:
    """检查完整候选指标与项目数值政策。"""
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
        missing_validation = sorted(_REQUIRED_CANDIDATE_METRICS - set(validation))
        missing_deployment = sorted(_REQUIRED_CANDIDATE_METRICS - set(deployment))
        if missing_validation or missing_deployment:
            local.append("CANDIDATE_METRICS_INCOMPLETE")
        sharpe = _number(validation.get("sharpe"))
        drawdown = _number(validation.get("maximum_drawdown"))
        bars = _integer(validation.get("bars"))
        if minimum_sharpe is None or sharpe is None or sharpe < minimum_sharpe:
            local.append("CANDIDATE_OOS_SHARPE_BELOW_POLICY")
        if (
            maximum_drawdown is None or drawdown is None
            or drawdown > maximum_drawdown
        ):
            local.append("CANDIDATE_OOS_DRAWDOWN_ABOVE_POLICY")
        if minimum_bars is None or bars is None or bars < minimum_bars:
            local.append("CANDIDATE_OOS_BARS_BELOW_POLICY")
        reasons.extend(local)
        details.append({
            "family": family,
            "candidate_id": candidate.get("deployment_candidate_id"),
            "sharpe": sharpe,
            "maximum_drawdown": drawdown,
            "bars": bars,
            "missing_validation_metrics": missing_validation,
            "missing_deployment_metrics": missing_deployment,
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
        or trial_rows != summary_trials or header_trials != summary_trials
    ):
        reasons.append("GLOBAL_TRIAL_LEDGER_COUNT_MISMATCH")
    if ledger.get("evaluation_id_count") != ledger.get("unique_evaluation_id_count"):
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
        if samples is None or minimum_samples is None or samples < minimum_samples:
            local.append("BOOTSTRAP_SAMPLE_COUNT_BELOW_POLICY")
        neighbors = _integer(candidate.get("parameter_neighbor_count"))
        minimum_neighbors = _integer(
            research.get("minimum_parameter_neighbor_count")
        )
        if (
            neighbors is None or minimum_neighbors is None
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
            "reason_codes": list(_unique(local)),
        })
    facts = {
        "trial_ledger": dict(ledger),
        "candidate_statistics": details,
    }
    return GateResult(
        "statistical_governance", not reasons, _unique(reasons), facts,
    )


def _robustness_gate(summary: Mapping[str, object], research_evidence: Mapping[str, object], policy: Mapping[str, object]) -> GateResult:
    """检查尾部、容量、压力、基准与成本场景证据。"""
    reasons: list[str] = []
    research = _policy_section(policy, "research")
    industry = _mapping(summary.get("industry_evidence"))
    tail = _sequence(industry.get("tail_scenarios"))
    stress = _sequence(industry.get("stress_scenarios"))
    cost_scenarios = _sequence(industry.get("cost_scenarios"))
    minimum_tail = _integer(research.get("minimum_tail_scenarios"))
    minimum_stress = _integer(research.get("minimum_stress_scenarios"))
    minimum_cost = _integer(research.get("minimum_cost_scenarios"))
    if minimum_tail is None or len(tail) < minimum_tail:
        reasons.append("TAIL_RISK_SCENARIO_EVIDENCE_INCOMPLETE")
    if minimum_stress is None or len(stress) < minimum_stress:
        reasons.append("STRESS_SCENARIO_EVIDENCE_INCOMPLETE")
    if minimum_cost is None or len(cost_scenarios) < minimum_cost:
        reasons.append("COST_SCENARIO_EVIDENCE_INCOMPLETE")
    config = _mapping(research_evidence.get("research_config"))
    cost_model = _mapping(config.get("cost_model"))
    capacity_notional = _number(cost_model.get("capacity_notional_quote"))
    minimum_notional = _number(research.get("minimum_capacity_notional_quote"))
    candidate_capacity = [
        _number(_mapping(item.get("validation_metrics")).get("capacity_score"))
        for item in _eligible_candidates(summary)
    ]
    minimum_capacity = _number(research.get("minimum_capacity_score"))
    if (
        capacity_notional is None or minimum_notional is None
        or capacity_notional < minimum_notional
        or minimum_capacity is None
        or not candidate_capacity
        or any(value is None or value < minimum_capacity for value in candidate_capacity)
    ):
        reasons.append("CAPACITY_EVIDENCE_BELOW_POLICY")
    benchmark = _mapping(_mapping(summary.get("ablations")).get("fixed_long"))
    benchmark_sharpe = _number(benchmark.get("sharpe"))
    minimum_excess = _number(research.get("minimum_benchmark_sharpe_excess"))
    candidate_sharpes = [
        _number(_mapping(item.get("validation_metrics")).get("sharpe"))
        for item in _eligible_candidates(summary)
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
    return GateResult(
        "robustness_evidence", not reasons, _unique(reasons), {
            "tail_scenario_count": len(tail),
            "stress_scenario_count": len(stress),
            "cost_scenario_count": len(cost_scenarios),
            "capacity_notional_quote": capacity_notional,
            "candidate_capacity_scores": candidate_capacity,
            "benchmark_sharpe": benchmark_sharpe,
            "candidate_oos_sharpes": candidate_sharpes,
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
    gates = (
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
