"""只读聚合多节拍成员的 operational 与 promotion 就绪事实。"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path

from guvolu.data.durable_io import atomic_write_text
from guvolu.research.interval_suite import build_interval_suite_plan
from guvolu.research.interval_suite_evidence import evaluate_interval_suite
from guvolu.research.provenance import canonical_json, stable_identifier
from guvolu.research.readiness import strategy_readiness


INTERVAL_SUITE_READINESS_METHOD_VERSION = "interval-suite-readiness-v2"
_LEGACY_MEMBER_GOVERNANCE_BLOCKERS = {
    "no_sealed_holdout_vintage",
    "sealed_vintage_has_no_frozen_forward_plan",
    "sealed_holdout_vintage_incomplete",
}


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


def _boolean(value: object, name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{name} 必须为布尔值")
    return value


def _unique(values: Sequence[str]) -> list[str]:
    return sorted(set(values))


def aggregate_interval_suite_readiness(
    plan: Mapping[str, object],
    evidence: Mapping[str, object],
    member_readiness: Mapping[str, Mapping[str, object]],
    evaluated_at: datetime,
    suite_forward_plans: Sequence[Mapping[str, object]] = (),
    suite_vintages: Sequence[Mapping[str, object]] = (),
    suite_registry: str | None = None,
) -> Mapping[str, object]:
    """从已验证 evidence 与成员 readiness 构造纯套件门禁。"""
    if evidence.get("suite_plan_id") != plan.get("suite_plan_id"):
        raise ValueError("套件 evidence 与预登记 plan 身份不一致")
    if evaluated_at.tzinfo is None:
        reference = evaluated_at.replace(tzinfo=UTC)
    else:
        reference = evaluated_at.astimezone(UTC)

    raw_members = _list(evidence.get("members"), "evidence.members")
    evidence_members = {
        _text(_mapping(raw, "evidence.member").get("member_id"), "member_id"):
        _mapping(raw, "evidence.member")
        for raw in raw_members
    }
    if len(evidence_members) != len(raw_members):
        raise ValueError("套件 evidence 包含重复 member_id")
    if set(member_readiness) != set(evidence_members):
        raise ValueError("成员 readiness 没有完整覆盖套件 evidence")

    raw_sleeves = _list(evidence.get("sleeves"), "evidence.sleeves")
    selected_sleeves = tuple(
        _mapping(raw, "evidence.sleeve") for raw in raw_sleeves
        if _mapping(raw, "evidence.sleeve").get("suite_eligible") is True
    )
    selected_member_ids = tuple(sorted({
        _text(sleeve.get("member_id"), "sleeve.member_id")
        for sleeve in selected_sleeves
    }))
    allocation = _mapping(
        evidence.get("suite_research_allocation"), "suite_research_allocation",
    )
    research_blockers: list[str] = []
    if not selected_sleeves:
        research_blockers.append("no_suite_eligible_sleeve")
    if allocation.get("status") != "research_only":
        research_blockers.append("suite_allocation_not_research_only")
    if evidence.get("operational_status") != (
        "disabled_pending_suite_readiness_and_holdout"
    ):
        research_blockers.append("suite_operational_status_contract_mismatch")

    member_payloads: list[Mapping[str, object]] = []
    operational_blockers = list(research_blockers)
    promotion_blockers = list(research_blockers)
    for member_id in sorted(evidence_members):
        member = evidence_members[member_id]
        readiness = _mapping(
            member_readiness[member_id], f"member_readiness.{member_id}",
        )
        operational = _mapping(readiness.get("operational"), "operational")
        promotion = _mapping(readiness.get("promotion"), "promotion")
        raw_promotion_blockers = _list(
            promotion.get("blockers"), "promotion.blockers",
        )
        suite_promotion_blockers = sorted(
            _text(item, "promotion.blocker")
            for item in raw_promotion_blockers
            if item not in _LEGACY_MEMBER_GOVERNANCE_BLOCKERS
        )
        selected = member_id in selected_member_ids
        if readiness.get("run_id") != member.get("run_id"):
            raise ValueError("成员 readiness 与 evidence run_id 不一致")
        if readiness.get("manifest_sha256") != member.get("manifest_sha256"):
            raise ValueError("成员 readiness 与 evidence manifest_sha256 不一致")
        if selected and not _boolean(operational.get("ready"), "operational.ready"):
            operational_blockers.append("selected_member_operational_not_ready")
        _boolean(promotion.get("ready"), "promotion.ready")
        if selected and suite_promotion_blockers:
            promotion_blockers.append("selected_member_promotion_not_ready")
        readiness_facts = {
            "run_id": readiness.get("run_id"),
            "manifest_sha256": readiness.get("manifest_sha256"),
            "source": readiness.get("source"),
            "operational": readiness.get("operational"),
            "promotion": readiness.get("promotion"),
        }
        member_payloads.append({
            "member_id": member_id,
            "bar_interval": member.get("bar_interval"),
            "run_id": member.get("run_id"),
            "manifest_sha256": member.get("manifest_sha256"),
            "selected_by_suite": selected,
            "operational_ready": operational.get("ready"),
            "operational_blockers": operational.get("blockers"),
            "operational_next_action": operational.get("next_action"),
            "promotion_ready": promotion.get("ready"),
            "promotion_blockers": promotion.get("blockers"),
            "promotion_next_action": promotion.get("next_action"),
            "suite_promotion_ready": not suite_promotion_blockers,
            "suite_promotion_blockers": suite_promotion_blockers,
            "readiness_facts_id": stable_identifier(
                "strategy-readiness-facts", readiness_facts,
            ),
        })

    valid_suite_plans = [
        plan_record for plan_record in suite_forward_plans
        if plan_record.get("suite_plan_id") == plan.get("suite_plan_id")
        and plan_record.get("suite_evidence_id")
        == evidence.get("suite_evidence_id")
        and plan_record.get("source_git_hash")
        == evidence.get("source_git_hash")
    ]
    if not valid_suite_plans:
        promotion_blockers.append("suite_frozen_forward_plan_not_registered")
    suite_vintage_payloads: list[Mapping[str, object]] = []
    for raw_vintage in suite_vintages:
        vintage = _mapping(raw_vintage, "suite vintage")
        suite_vintage_payloads.append({
            **vintage,
            "data_complete": None,
            "data_completion_basis": (
                "suite_forward_prediction_receipts_not_implemented"
            ),
        })
    if not suite_vintage_payloads:
        promotion_blockers.append("no_sealed_suite_holdout_vintage")
    planned_vintage_ids = {
        _text(item.get("vintage_id"), "suite plan vintage_id")
        for item in valid_suite_plans
    }
    suite_vintage_ids = {
        _text(item.get("vintage_id"), "suite vintage_id")
        for item in suite_vintage_payloads
    }
    if suite_vintage_ids - planned_vintage_ids:
        promotion_blockers.append("suite_frozen_forward_plan_not_registered")
    if any(item.get("data_complete") is False for item in suite_vintage_payloads):
        promotion_blockers.append("sealed_suite_holdout_vintage_incomplete")
    if valid_suite_plans and not (suite_vintage_ids - planned_vintage_ids):
        promotion_blockers.append(
            "suite_frozen_forward_prediction_pipeline_not_implemented"
        )
    operational_blockers = _unique(operational_blockers)
    promotion_blockers = _unique(promotion_blockers)
    if "selected_member_operational_not_ready" in operational_blockers:
        operational_next_action = "wait_for_all_selected_members"
    elif operational_blockers:
        operational_next_action = "repair_suite_operational_contract"
    else:
        operational_next_action = "publish_fresh_suite_operational_snapshot"
    if "selected_member_promotion_not_ready" in promotion_blockers:
        promotion_next_action = "wait_for_member_holdout_readiness"
    elif "no_sealed_suite_holdout_vintage" in promotion_blockers:
        promotion_next_action = "seal_new_suite_vintage_in_successor_registry"
    elif "suite_frozen_forward_plan_not_registered" in promotion_blockers:
        promotion_next_action = "register_suite_frozen_forward_plan"
    elif (
        "suite_frozen_forward_prediction_pipeline_not_implemented"
        in promotion_blockers
    ):
        promotion_next_action = "implement_suite_frozen_forward_predictions"
    elif "sealed_suite_holdout_vintage_incomplete" in promotion_blockers:
        promotion_next_action = "wait_for_sealed_suite_vintage_end"
    else:
        promotion_next_action = "run_suite_holdout_validation_once"

    body: dict[str, object] = {
        "schema_version": 1,
        "method_version": INTERVAL_SUITE_READINESS_METHOD_VERSION,
        "evaluated_at": reference.isoformat(),
        "suite_plan_id": plan.get("suite_plan_id"),
        "suite_evidence_id": evidence.get("suite_evidence_id"),
        "source_git_hash": evidence.get("source_git_hash"),
        "market_id": evidence.get("market_id"),
        "input_head_generation": evidence.get("input_head_generation"),
        "input_receipt_sha256": evidence.get("input_receipt_sha256"),
        "selected_sleeve_ids": sorted(
            _text(sleeve.get("sleeve_id"), "sleeve_id")
            for sleeve in selected_sleeves
        ),
        "selected_member_ids": list(selected_member_ids),
        "research": {
            "ready": not research_blockers,
            "blockers": _unique(research_blockers),
            "allocation_status": allocation.get("status"),
            "aggregate_target": allocation.get("aggregate_target"),
            "reserve": allocation.get("reserve"),
        },
        "members": member_payloads,
        "suite_frozen_forward_plan_ids": sorted(
            _text(item.get("plan_id"), "suite forward plan_id")
            for item in valid_suite_plans
        ),
        "suite_frozen_forward_plans": sorted(
            (
                {
                    "plan_id": item.get("plan_id"),
                    "vintage_id": item.get("vintage_id"),
                    "plan_artifact_sha256": item.get(
                        "plan_artifact_sha256"
                    ),
                }
                for item in valid_suite_plans
            ),
            key=lambda item: str(item["plan_id"]),
        ),
        "suite_governance": {
            "registry": suite_registry,
            "sealed_vintages": sorted(
                suite_vintage_payloads,
                key=lambda item: str(item.get("start_time")),
            ),
        },
        "operational": {
            "ready": not operational_blockers,
            "blockers": operational_blockers,
            "next_action": operational_next_action,
        },
        "promotion": {
            "ready": not promotion_blockers,
            "blockers": promotion_blockers,
            "next_action": promotion_next_action,
        },
        "read_only": True,
    }
    return {
        **body,
        "suite_readiness_id": stable_identifier(
            "interval-suite-readiness", body,
        ),
    }


def interval_suite_readiness(
    repository_root: Path,
    config_paths: Sequence[Path],
    manifest_paths: Sequence[Path],
    reference_time: datetime | None = None,
    suite_registry_path: Path | None = None,
) -> Mapping[str, object]:
    """完整复核 suite evidence，再聚合每个成员的只读 readiness。"""
    root = repository_root.resolve()
    evaluated_at = reference_time or datetime.now(UTC)
    plan = build_interval_suite_plan(root, config_paths)
    evidence = evaluate_interval_suite(root, plan, manifest_paths)
    plan_members = {
        _text(_mapping(raw, "plan.member").get("member_id"), "member_id"):
        _mapping(raw, "plan.member")
        for raw in _list(plan.get("members"), "plan.members")
    }
    evidence_members = {
        _text(_mapping(raw, "evidence.member").get("member_id"), "member_id"):
        _mapping(raw, "evidence.member")
        for raw in _list(evidence.get("members"), "evidence.members")
    }
    readiness: dict[str, Mapping[str, object]] = {}
    for member_id, member in evidence_members.items():
        plan_member = plan_members.get(member_id)
        if plan_member is None:
            raise ValueError("evidence member 不属于 suite plan")
        config_path = root / _text(plan_member.get("config_path"), "config_path")
        manifest_path = root / _text(member.get("manifest_path"), "manifest_path")
        readiness[member_id] = strategy_readiness(
            root, config_path, manifest_path, evaluated_at,
        )
    from guvolu.research.interval_suite_forward import (
        verified_interval_suite_forward_state,
    )

    suite_forward_state = verified_interval_suite_forward_state(
        root, config_paths, plan, evidence, suite_registry_path,
        reference_time=evaluated_at,
    )
    raw_plans = suite_forward_state.get("plans")
    raw_vintages = suite_forward_state.get("sealed_vintages")
    if not isinstance(raw_plans, tuple) or not isinstance(raw_vintages, tuple):
        raise AssertionError("suite forward state 合同无效")
    return aggregate_interval_suite_readiness(
        plan,
        evidence,
        readiness,
        evaluated_at,
        raw_plans,
        raw_vintages,
        _text(suite_forward_state.get("registry"), "suite registry"),
    )


def persist_interval_suite_readiness(
    repository_root: Path,
    readiness: Mapping[str, object],
    output_directory: Path | None = None,
) -> Path:
    """把只读结果写成内容寻址证据，不覆盖既有身份。"""
    root = repository_root.resolve()
    readiness_id = _text(
        readiness.get("suite_readiness_id"), "suite_readiness_id",
    )
    body = dict(readiness)
    body.pop("suite_readiness_id", None)
    if readiness_id != stable_identifier("interval-suite-readiness", body):
        raise ValueError("suite_readiness_id 与规范 readiness 内容不一致")
    directory = (
        output_directory.resolve()
        if output_directory is not None and output_directory.is_absolute()
        else root / (
            output_directory
            or Path("reports/strategy-research/interval-suite-readiness")
        )
    )
    output = directory / f"{readiness_id}.json"
    payload = canonical_json(readiness) + "\n"
    if output.exists():
        if output.read_text(encoding="utf-8") != payload:
            raise ValueError("相同 suite_readiness_id 的制品内容不一致")
        return output
    atomic_write_text(output, payload)
    return output
