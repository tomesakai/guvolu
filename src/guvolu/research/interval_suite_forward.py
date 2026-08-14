"""冻结跨节拍 sleeve、候选和权重，并登记到 sealed vintage。"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Mapping, Sequence

from guvolu.data.durable_io import atomic_write_text, exclusive_path_lock
from guvolu.research import clock
from guvolu.research.config_lineage import (
    load_governed_strategy_config_with_paths,
)
from guvolu.research.contracts import (
    INTERVAL_SUITE_FORWARD_METHOD_VERSION,
    INTERVAL_SUITE_FORWARD_SCHEMA_VERSION,
)
from guvolu.research.governance import (
    GOVERNANCE_METHOD_VERSION,
    IntervalSuiteForwardPlan,
    get_frozen_forward_plan_for_vintage,
    get_holdout_vintage,
    get_interval_suite_forward_plan_for_vintage,
    list_holdout_vintages,
    register_interval_suite_forward_plan,
)
from guvolu.research.interval_suite import build_interval_suite_plan
from guvolu.research.interval_suite_evidence import evaluate_interval_suite
from guvolu.research.provenance import (
    canonical_json,
    code_identity,
    sha256_file,
    sha256_text,
    stable_identifier,
)


@dataclass(frozen=True)
class IntervalSuiteForwardPlanResult:
    """一个已持久化并登记的跨节拍冻结计划。"""

    plan_id: str
    plan_path: Path
    plan_sha256: str


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


def _positive_integer(value: object, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{name} 必须为正整数")
    return value


def _load(path: Path, name: str) -> Mapping[str, object]:
    try:
        return _mapping(json.loads(path.read_text(encoding="utf-8")), name)
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ValueError(f"无法读取 {name}: {path}") from error


def _project_path(root: Path, path: Path, name: str) -> Path:
    resolved = path.resolve() if path.is_absolute() else (root / path).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as error:
        raise ValueError(f"{name} 必须位于项目目录内") from error
    return resolved


def _relative(root: Path, path: Path) -> str:
    resolved = path.resolve() if path.is_absolute() else (root / path).resolve()
    return resolved.relative_to(root.resolve()).as_posix()


def _config_contract(
    root: Path, config_paths: Sequence[Path],
) -> tuple[Path, tuple[Path, ...], Mapping[str, Mapping[str, object]]]:
    registry: Path | None = None
    source_paths: set[Path] = set()
    configs: dict[str, Mapping[str, object]] = {}
    for raw in config_paths:
        path = _project_path(root, raw, "suite config")
        config, _hash, _root_hash, _depth, lineage_paths = (
            load_governed_strategy_config_with_paths(root, path)
        )
        interval = _text(config.get("bar_interval"), "bar_interval")
        governance = _mapping(config.get("data_governance"), "data_governance")
        current_registry = _project_path(
            root, Path(_text(governance.get("registry"), "registry")),
            "governance registry",
        )
        if registry is None:
            registry = current_registry
        elif current_registry != registry:
            raise ValueError("多节拍配置没有共享治理注册表")
        configs[interval] = config
        source_paths.update(lineage_paths)
    if registry is None:
        raise ValueError("套件配置不能为空")
    return registry, tuple(sorted(source_paths)), configs


def _candidate_indexes(
    root: Path, evidence: Mapping[str, object],
) -> Mapping[str, Mapping[str, object]]:
    candidates: dict[str, Mapping[str, object]] = {}
    for raw_member in _list(evidence.get("members"), "evidence.members"):
        member = _mapping(raw_member, "evidence.member")
        manifest_path = _project_path(
            root, Path(_text(member.get("manifest_path"), "manifest_path")),
            "member manifest",
        )
        if sha256_file(manifest_path) != member.get("manifest_sha256"):
            raise ValueError("套件成员 manifest 现场 SHA-256 不匹配")
        manifest = _load(manifest_path, "member manifest")
        artifacts = _mapping(manifest.get("artifacts"), "manifest.artifacts")
        registry_record = _mapping(
            artifacts.get("candidate_registry"), "candidate_registry",
        )
        candidate_path = _project_path(
            root, Path(_text(registry_record.get("path"), "candidate path")),
            "candidate registry",
        )
        if sha256_file(candidate_path) != registry_record.get("sha256"):
            raise ValueError("候选注册表现场 SHA-256 不匹配")
        payload = _load(candidate_path, "candidate registry")
        for raw_candidate in _list(payload.get("candidates"), "candidates"):
            candidate = _mapping(raw_candidate, "candidate")
            candidate_id = _text(candidate.get("candidate_id"), "candidate_id")
            existing = candidates.get(candidate_id)
            if existing is not None and existing != candidate:
                raise ValueError("候选身份在成员注册表间冲突")
            candidates[candidate_id] = candidate
    return candidates


def _frozen_sleeves(
    root: Path, evidence: Mapping[str, object],
) -> list[Mapping[str, object]]:
    allocation = _mapping(
        evidence.get("suite_research_allocation"), "suite allocation",
    )
    weights = _mapping(allocation.get("weights"), "suite weights")
    candidates = _candidate_indexes(root, evidence)
    sleeves: list[Mapping[str, object]] = []
    for raw in _list(evidence.get("sleeves"), "evidence.sleeves"):
        sleeve = _mapping(raw, "evidence.sleeve")
        if sleeve.get("suite_eligible") is not True:
            continue
        sleeve_id = _text(sleeve.get("sleeve_id"), "sleeve_id")
        candidate_id = _text(
            sleeve.get("deployment_candidate_id"), "deployment_candidate_id",
        )
        candidate = candidates.get(candidate_id)
        if candidate is None:
            raise ValueError("suite sleeve 的部署候选不在受保护注册表")
        weight = weights.get(sleeve_id)
        if not isinstance(weight, (int, float)) or isinstance(weight, bool):
            raise ValueError("suite allocation 未覆盖被准入 sleeve")
        sleeves.append({
            "sleeve_id": sleeve_id,
            "member_id": sleeve.get("member_id"),
            "bar_interval": sleeve.get("bar_interval"),
            "family": sleeve.get("family"),
            "candidate": candidate,
            "weight": float(weight),
        })
    if not sleeves:
        raise ValueError("套件没有可冻结的 eligible sleeve")
    return sorted(sleeves, key=lambda item: str(item["sleeve_id"]))


def _expected_plan_id(
    vintage_id: str,
    suite_plan_id: str,
    suite_evidence_id: str,
    source_git_hash: str,
    code_tree_digest: str,
) -> str:
    return stable_identifier("interval-suite-forward-plan", {
        "governance_method_version": GOVERNANCE_METHOD_VERSION,
        "vintage_id": vintage_id,
        "suite_plan_id": suite_plan_id,
        "suite_evidence_id": suite_evidence_id,
        "source_git_hash": source_git_hash,
        "code_tree_digest": code_tree_digest,
    })


def freeze_interval_suite_forward_plan(
    repository_root: Path,
    config_paths: Sequence[Path],
    manifest_paths: Sequence[Path],
    evidence_path: Path,
    vintage_id: str,
    registry_path: Path | None = None,
) -> IntervalSuiteForwardPlanResult:
    """重建 suite evidence 后冻结跨节拍部署合同。"""
    root = repository_root.resolve()
    configured_registry, source_paths, configs = _config_contract(
        root, config_paths,
    )
    registry = (
        configured_registry if registry_path is None
        else _project_path(root, registry_path, "suite governance registry")
    )
    suite_plan = build_interval_suite_plan(root, config_paths)
    evidence = evaluate_interval_suite(root, suite_plan, manifest_paths)
    evidence_file = _project_path(root, evidence_path, "suite evidence")
    persisted_evidence = _load(evidence_file, "suite evidence")
    if canonical_json(persisted_evidence) != canonical_json(evidence):
        raise ValueError("持久化 suite evidence 不能由当前输入完整重建")
    identity = code_identity(root, source_paths)
    if not identity.decision_grade or identity.git_hash is None:
        raise ValueError("套件冻结前向计划必须在 clean decision-grade commit 创建")
    suite_plan_id = _text(suite_plan.get("suite_plan_id"), "suite_plan_id")
    suite_evidence_id = _text(
        evidence.get("suite_evidence_id"), "suite_evidence_id",
    )
    source_git_hash = _text(evidence.get("source_git_hash"), "source_git_hash")
    vintage = get_holdout_vintage(registry, vintage_id)
    if vintage.status != "sealed" or vintage.market_id != evidence.get("market_id"):
        raise ValueError("套件冻结计划必须绑定同市场的 sealed vintage")
    if get_frozen_forward_plan_for_vintage(registry, vintage_id) is not None:
        raise ValueError("套件冻结计划不得追溯接管单成员计划 vintage")
    if clock.utc_now() >= vintage.start_time:
        raise ValueError("套件冻结前向计划必须在 vintage 开始前创建")
    plan_id = _expected_plan_id(
        vintage_id, suite_plan_id, suite_evidence_id,
        source_git_hash, identity.tree_digest,
    )
    sleeves = _frozen_sleeves(root, evidence)
    allocation = _mapping(
        evidence.get("suite_research_allocation"), "suite allocation",
    )
    maximum_lag = min(
        _positive_integer(
            config.get("strategy_decision_max_age_seconds"),
            "strategy_decision_max_age_seconds",
        )
        for config in configs.values()
    )
    plan_directory = (
        root / "reports" / "strategy-research"
        / "interval-suite-frozen-forward" / vintage_id / plan_id
    )
    with exclusive_path_lock(plan_directory / "registration"):
        existing = get_interval_suite_forward_plan_for_vintage(
            registry, vintage_id,
        )
        if existing is not None:
            if existing.plan_id != plan_id:
                raise ValueError("vintage 已绑定不同的套件冻结前向计划")
            plan_path = root / existing.plan_artifact_path
            attest_interval_suite_forward_plan(
                root, existing, suite_plan, evidence,
                expected_registry=registry,
            )
            return IntervalSuiteForwardPlanResult(
                existing.plan_id, plan_path, existing.plan_artifact_sha256,
            )
        evidence_text = canonical_json(persisted_evidence) + "\n"
        evidence_sha = sha256_text(evidence_text)
        frozen_evidence = plan_directory / (
            f"suite-evidence-sha256-{evidence_sha}.json"
        )
        if frozen_evidence.exists():
            if frozen_evidence.read_text(encoding="utf-8") != evidence_text:
                raise ValueError("内容寻址 suite evidence 发生身份冲突")
        else:
            atomic_write_text(frozen_evidence, evidence_text)
        payload: dict[str, object] = {
            "schema_version": INTERVAL_SUITE_FORWARD_SCHEMA_VERSION,
            "method_version": INTERVAL_SUITE_FORWARD_METHOD_VERSION,
            "governance_method_version": GOVERNANCE_METHOD_VERSION,
            "scope": "INTERVAL_SUITE_FROZEN_FORWARD",
            "governance_registry": _relative(root, registry),
            "plan_id": plan_id,
            "suite_plan_id": suite_plan_id,
            "suite_evidence_id": suite_evidence_id,
            "source_git_hash": source_git_hash,
            "code_identity": asdict(identity),
            "code_tree_digest": identity.tree_digest,
            "frozen_at": clock.utc_now().isoformat(),
            "vintage": {
                "vintage_id": vintage.vintage_id,
                "market_id": vintage.market_id,
                "start_time": vintage.start_time.isoformat(),
                "end_time": vintage.end_time.isoformat(),
            },
            "source_evidence": {
                "path": _relative(root, frozen_evidence),
                "sha256": evidence_sha,
            },
            "input": {
                "head_generation": evidence.get("input_head_generation"),
                "receipt_sha256": evidence.get("input_receipt_sha256"),
            },
            "decision_grid": {
                "interval_seconds": evidence.get("alignment_interval_seconds"),
                "utc_epoch_offset_seconds": 0,
                "maximum_recording_lag_seconds": maximum_lag,
            },
            "sleeves": sleeves,
            "allocation": {
                "weights": allocation.get("weights"),
                "reserve": allocation.get("reserve"),
                "shared_caps": allocation.get("shared_caps"),
            },
        }
        plan_text = canonical_json(payload) + "\n"
        plan_sha = sha256_text(plan_text)
        plan_path = plan_directory / f"plan-sha256-{plan_sha}.json"
        if plan_path.exists():
            if plan_path.read_text(encoding="utf-8") != plan_text:
                raise ValueError("内容寻址 suite plan 发生身份冲突")
        else:
            atomic_write_text(plan_path, plan_text)
        registered = register_interval_suite_forward_plan(
            registry, vintage_id, suite_plan_id, suite_evidence_id,
            source_git_hash, identity.tree_digest, _relative(root, plan_path),
            plan_sha, repository_root=root,
        )
        if registered.plan_id != plan_id:
            raise RuntimeError("治理注册表返回不同的套件冻结计划身份")
        return IntervalSuiteForwardPlanResult(plan_id, plan_path, plan_sha)


def attest_interval_suite_forward_plan(
    repository_root: Path,
    registered: IntervalSuiteForwardPlan,
    suite_plan: Mapping[str, object],
    evidence: Mapping[str, object],
    *,
    expected_registry: Path,
) -> Mapping[str, object]:
    """复核登记行、计划文件及当前重建 suite evidence 一致。"""
    root = repository_root.resolve()
    path = _project_path(
        root, Path(registered.plan_artifact_path), "suite forward plan",
    )
    if sha256_file(path) != registered.plan_artifact_sha256:
        raise ValueError("套件冻结前向计划现场 SHA-256 不匹配")
    payload = _load(path, "suite forward plan")
    expected = {
        "plan_id": registered.plan_id,
        "suite_plan_id": suite_plan.get("suite_plan_id"),
        "suite_evidence_id": evidence.get("suite_evidence_id"),
        "source_git_hash": evidence.get("source_git_hash"),
        "code_tree_digest": registered.code_tree_digest,
    }
    for key, value in expected.items():
        if payload.get(key) != value:
            raise ValueError(f"套件冻结前向计划 {key} 与当前证据不一致")
    if payload.get("governance_registry") != _relative(root, expected_registry):
        raise ValueError("套件冻结前向计划 governance_registry 不一致")
    source_evidence = _mapping(
        payload.get("source_evidence"), "source_evidence",
    )
    evidence_path = _project_path(
        root,
        Path(_text(source_evidence.get("path"), "source evidence path")),
        "source evidence",
    )
    if (
        sha256_file(evidence_path) != source_evidence.get("sha256")
        or canonical_json(_load(evidence_path, "source evidence"))
        != canonical_json(evidence)
    ):
        raise ValueError("套件冻结计划的持久化 evidence 不可重建")
    expected_sleeves = _frozen_sleeves(root, evidence)
    if canonical_json(payload.get("sleeves")) != canonical_json(expected_sleeves):
        raise ValueError("套件冻结前向 sleeve 合同与 evidence 不一致")
    allocation = _mapping(
        evidence.get("suite_research_allocation"), "suite allocation",
    )
    frozen_allocation = _mapping(payload.get("allocation"), "frozen allocation")
    if (
        canonical_json(frozen_allocation.get("weights"))
        != canonical_json(allocation.get("weights"))
        or frozen_allocation.get("reserve") != allocation.get("reserve")
    ):
        raise ValueError("套件冻结前向资金权重与 evidence 不一致")
    decision_grid = _mapping(payload.get("decision_grid"), "decision_grid")
    if decision_grid.get("interval_seconds") != evidence.get(
        "alignment_interval_seconds"
    ):
        raise ValueError("套件冻结前向共同决策栅格与 evidence 不一致")
    return payload


def verified_interval_suite_forward_plans(
    repository_root: Path,
    config_paths: Sequence[Path],
    suite_plan: Mapping[str, object],
    evidence: Mapping[str, object],
    registry_path: Path | None = None,
) -> tuple[Mapping[str, object], ...]:
    """兼容返回当前 suite identity 的已复核计划。"""
    state = verified_interval_suite_forward_state(
        repository_root, config_paths, suite_plan, evidence, registry_path,
    )
    plans = state.get("plans")
    if not isinstance(plans, tuple):
        raise AssertionError("suite forward state plans 合同无效")
    return plans


def verified_interval_suite_forward_state(
    repository_root: Path,
    config_paths: Sequence[Path],
    suite_plan: Mapping[str, object],
    evidence: Mapping[str, object],
    registry_path: Path | None = None,
    *,
    reference_time: datetime | None = None,
) -> Mapping[str, object]:
    """只读复核 suite registry 的 vintage 与当前身份计划。"""
    root = repository_root.resolve()
    configured_registry, _paths, _configs = _config_contract(root, config_paths)
    registry = (
        configured_registry if registry_path is None
        else _project_path(root, registry_path, "suite governance registry")
    )
    observed_at = clock.utc_now() if reference_time is None else reference_time
    reference = (
        observed_at.replace(tzinfo=UTC)
        if observed_at.tzinfo is None else observed_at.astimezone(UTC)
    )
    result: list[Mapping[str, object]] = []
    sealed_vintages: list[Mapping[str, object]] = []
    for vintage in list_holdout_vintages(registry):
        if vintage.status != "sealed" or vintage.market_id != evidence.get("market_id"):
            continue
        registered = get_interval_suite_forward_plan_for_vintage(
            registry, vintage.vintage_id,
        )
        if registered is None:
            if get_frozen_forward_plan_for_vintage(
                registry, vintage.vintage_id,
            ) is not None:
                continue
            if vintage.start_time <= reference:
                continue
            sealed_vintages.append({
                "vintage_id": vintage.vintage_id,
                "market_id": vintage.market_id,
                "start_time": vintage.start_time.isoformat(),
                "end_time": vintage.end_time.isoformat(),
                "sealed_at": vintage.sealed_at.isoformat(),
            })
            continue
        if (
            registered.suite_plan_id != suite_plan.get("suite_plan_id")
            or registered.suite_evidence_id != evidence.get("suite_evidence_id")
            or registered.source_git_hash != evidence.get("source_git_hash")
        ):
            continue
        sealed_vintages.append({
            "vintage_id": vintage.vintage_id,
            "market_id": vintage.market_id,
            "start_time": vintage.start_time.isoformat(),
            "end_time": vintage.end_time.isoformat(),
            "sealed_at": vintage.sealed_at.isoformat(),
        })
        attest_interval_suite_forward_plan(
            root, registered, suite_plan, evidence,
            expected_registry=registry,
        )
        result.append({
            "plan_id": registered.plan_id,
            "vintage_id": registered.vintage_id,
            "suite_plan_id": registered.suite_plan_id,
            "suite_evidence_id": registered.suite_evidence_id,
            "source_git_hash": registered.source_git_hash,
            "plan_artifact_sha256": registered.plan_artifact_sha256,
        })
    return {
        "registry": _relative(root, registry),
        "sealed_vintages": tuple(sorted(
            sealed_vintages, key=lambda item: str(item["start_time"]),
        )),
        "plans": tuple(sorted(
            result, key=lambda item: str(item["vintage_id"]),
        )),
    }
