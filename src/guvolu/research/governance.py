"""研究数据暴露与一次性封存段治理。"""
from __future__ import annotations

import json
import math
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from guvolu.research.contracts import (
    HOLDOUT_MANIFEST_SCHEMA_VERSION,
    HOLDOUT_METHOD_VERSION,
)
from guvolu.research.provenance import sha256_file, stable_identifier

GOVERNANCE_SCHEMA_VERSION = 4
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
) -> tuple[str, str]:
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
    if terminal.get("candidate_ids") != candidate_ids:
        raise ValueError("holdout verdict 未绑定冻结 candidate IDs")
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, dict):
        raise ValueError("holdout manifest 缺少 artifacts")
    _, panel_sha256 = _validated_artifact(
        repository_root, artifacts, "panel", "holdout_panel",
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
    if result.get("panel_sha256") != panel_sha256:
        raise ValueError("holdout result 未绑定现场 panel SHA-256")
    if result.get("verdict") != terminal_verdict:
        raise ValueError("holdout result 与最终 verdict 不匹配")
    candidate_results = result.get("candidate_results")
    if not isinstance(candidate_results, list) or not candidate_results:
        raise ValueError("holdout result 缺少候选评估结果")
    if len(candidate_results) != len(candidate_ids):
        raise ValueError("holdout candidate_results 未覆盖冻结候选全集")
    policy = result.get("policy")
    if not isinstance(policy, dict):
        raise ValueError("holdout result 缺少固定 policy")
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
    return normalized_path, candidate_set_hash


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
    supported = {None, "1", "2", "3", str(GOVERNANCE_SCHEMA_VERSION)}
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
            connection.execute("COMMIT")
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise
    violation = _terminal_invariant_violation(connection)
    if violation is not None:
        raise ValueError(
            "治理库存在不一致的 holdout 终态: "
            + str(violation["vintage_id"])
        )


def _connect(path: Path) -> sqlite3.Connection:
    """打开治理注册表并初始化固定 schema。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30.0, isolation_level=None)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
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
        """
    )
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


def register_research_exposure(
    registry_path: Path,
    research_identity: str,
    market_id: str,
    start_time: datetime,
    end_time: datetime,
    *,
    recorded_at: datetime | None = None,
) -> ResearchExposure:
    """登记开发研究暴露；未消费封存段与研究读取互斥。"""
    start, end = _validate_range(start_time, end_time)
    recorded = _utc(recorded_at or datetime.now(UTC))
    exposure_id = stable_identifier("research-exposure", {
        "governance_method_version": GOVERNANCE_METHOD_VERSION,
        "research_identity": research_identity,
        "market_id": market_id,
        "start_time": start.isoformat(),
        "end_time": end.isoformat(),
    })
    connection = _connect(registry_path)
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
    *,
    sealed_at: datetime | None = None,
) -> HoldoutVintage:
    """封存尚未被任何自适应研究读取且不重叠的新数据段。"""
    start, end = _validate_range(start_time, end_time)
    sealed = _utc(sealed_at or datetime.now(UTC))
    if sealed > start:
        raise ValueError("封存段必须在区间开始前登记，禁止事后挑选 holdout")
    vintage_id = stable_identifier("holdout-vintage", {
        "governance_method_version": GOVERNANCE_METHOD_VERSION,
        "market_id": market_id,
        "start_time": start.isoformat(),
        "end_time": end.isoformat(),
    })
    connection = _connect(registry_path)
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


def consume_holdout_vintage(
    registry_path: Path,
    vintage_id: str,
    candidate_set_hash: str,
    evaluation_id: str,
    *,
    consumed_at: datetime | None = None,
) -> HoldoutVintage:
    """原子消费封存段；事务提交后永久禁止第二次消费。"""
    if not candidate_set_hash or not evaluation_id:
        raise ValueError("消费封存段必须绑定 candidate_set_hash 与 evaluation_id")
    consumed = _utc(consumed_at or datetime.now(UTC))
    connection = _connect(registry_path)
    try:
        _begin(connection)
        row = connection.execute(
            "SELECT * FROM holdout_vintage WHERE vintage_id=?",
            (vintage_id,),
        ).fetchone()
        if row is None:
            raise LookupError(f"封存段不存在: {vintage_id}")
        if row["status"] != "sealed":
            raise ValueError(f"封存段已经消费: {vintage_id}")
        connection.execute(
            "UPDATE holdout_vintage SET status='consumed',consumed_at=?,"
            "candidate_set_hash=?,evaluation_id=? WHERE vintage_id=? AND status='sealed'",
            (_timestamp(consumed), candidate_set_hash, evaluation_id, vintage_id),
        )
        connection.execute(
            "INSERT INTO holdout_evaluation_attempt("
            "evaluation_id,vintage_id,candidate_set_hash,status,stage,started_at,updated_at"
            ") VALUES(?,?,?,'incomplete','manually_consumed',?,?)",
            (
                evaluation_id,
                vintage_id,
                candidate_set_hash,
                _timestamp(consumed),
                _timestamp(consumed),
            ),
        )
        updated = connection.execute(
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
    if updated is None:
        raise RuntimeError("消费后封存段不可见")
    return _vintage_from_row(updated)


def start_holdout_evaluation_attempt(
    registry_path: Path,
    vintage_id: str,
    candidate_set_hash: str,
    evaluation_id: str,
    *,
    started_at: datetime | None = None,
) -> HoldoutEvaluationAttempt:
    """原子烧毁 vintage 并登记不可重跑的评估尝试。"""
    started = _utc(started_at or datetime.now(UTC))
    connection = _connect(registry_path)
    try:
        _begin(connection)
        vintage = connection.execute(
            "SELECT * FROM holdout_vintage WHERE vintage_id=?", (vintage_id,),
        ).fetchone()
        if vintage is None:
            raise LookupError(f"封存段不存在: {vintage_id}")
        if vintage["status"] != "sealed":
            raise ValueError(f"封存段已经消费: {vintage_id}")
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
    *,
    updated_at: datetime | None = None,
) -> HoldoutEvaluationAttempt:
    """持久化 incomplete 尝试最后到达的评估阶段。"""
    normalized = stage.strip()
    if not normalized:
        raise ValueError("holdout 评估阶段不得为空")
    updated = _utc(updated_at or datetime.now(UTC))
    connection = _connect(registry_path)
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
    completed_at: datetime | None = None,
) -> tuple[HoldoutVintage, HoldoutEvaluationAttempt]:
    """现场复核终态证据，再原子写入 verdict 与 completed attempt。"""
    normalized = verdict.strip()
    if not normalized or not result_manifest_path:
        raise ValueError("完成 holdout 必须绑定 verdict 与 manifest 身份")
    normalized_manifest_path, manifest_candidate_set_hash = (
        _validated_terminal_evidence(
            repository_root,
            vintage_id,
            evaluation_id,
            normalized,
            result_manifest_path,
            result_manifest_sha256,
        )
    )
    completed = _utc(completed_at or datetime.now(UTC))
    connection = _connect(registry_path)
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
            vintage["candidate_set_hash"] != manifest_candidate_set_hash
            or attempt["candidate_set_hash"] != manifest_candidate_set_hash
        ):
            raise ValueError("holdout manifest 的 candidate_set_hash 与注册表不匹配")
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
                normalized_manifest_path,
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
    frozen_at: datetime | None = None,
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
    frozen = _utc(frozen_at or datetime.now(UTC))
    plan_id = stable_identifier("frozen-forward-plan", {
        "governance_method_version": GOVERNANCE_METHOD_VERSION,
        "vintage_id": vintage_id,
        "source_manifest_sha256": source_manifest_sha256,
        "candidate_set_hash": candidate_set_hash,
        "config_hash": config_hash,
        "code_tree_digest": code_tree_digest,
    })
    connection = _connect(registry_path)
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
    recorded_at: datetime | None = None,
) -> FrozenForwardPrediction:
    """原子追加一个及时生成的预测；同一时点内容永久不可改写。"""
    if maximum_recording_lag_seconds <= 0:
        raise ValueError("预测登记时效阈值必须为正数")
    values = (
        input_head_generation, panel_sha256,
        prediction_artifact_path, prediction_artifact_sha256,
    )
    if any(not value.strip() for value in values):
        raise ValueError("冻结前向预测身份字段不得为空")
    decision = _utc(decision_time)
    recorded = _utc(recorded_at or datetime.now(UTC))
    lag = (recorded - decision).total_seconds()
    if lag < 0 or lag > maximum_recording_lag_seconds:
        raise ValueError("冻结前向预测未在预登记时效窗口内生成")
    connection = _connect(registry_path)
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
        prediction_id = stable_identifier("frozen-forward-prediction", {
            "governance_method_version": GOVERNANCE_METHOD_VERSION,
            "plan_id": plan_id,
            "decision_time": decision.isoformat(),
        })
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
            connection.execute("COMMIT")
            return _prediction_from_row(existing)
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
