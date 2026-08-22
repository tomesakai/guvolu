"""跨节拍冻结前向计划的内容身份合同。"""
from __future__ import annotations

from collections.abc import Mapping, Sequence

from guvolu.research.contracts import (
    INTERVAL_SUITE_FORWARD_METHOD_VERSION,
    INTERVAL_SUITE_FORWARD_SCHEMA_VERSION,
)
from guvolu.research.provenance import stable_identifier


def interval_suite_deployment_contract_id(
    governance_registry: str,
    live_data_root: Mapping[str, object],
    members: Sequence[Mapping[str, object]],
    decision_grid: Mapping[str, object],
) -> str:
    """绑定预测所需的活动数据根、配置快照和共同决策栅格。"""
    return stable_identifier("interval-suite-deployment-contract", {
        "schema_version": INTERVAL_SUITE_FORWARD_SCHEMA_VERSION,
        "method_version": INTERVAL_SUITE_FORWARD_METHOD_VERSION,
        "governance_registry": governance_registry,
        "live_data_root": live_data_root,
        "members": list(members),
        "decision_grid": decision_grid,
    })


def interval_suite_forward_plan_id(
    governance_method_version: str,
    vintage_id: str,
    suite_plan_id: str,
    suite_evidence_id: str,
    source_git_hash: str,
    code_tree_digest: str,
    deployment_contract_id: str,
) -> str:
    """生成同时绑定方法版本与部署输入的逻辑计划身份。"""
    return stable_identifier("interval-suite-forward-plan", {
        "schema_version": INTERVAL_SUITE_FORWARD_SCHEMA_VERSION,
        "method_version": INTERVAL_SUITE_FORWARD_METHOD_VERSION,
        "governance_method_version": governance_method_version,
        "vintage_id": vintage_id,
        "suite_plan_id": suite_plan_id,
        "suite_evidence_id": suite_evidence_id,
        "source_git_hash": source_git_hash,
        "code_tree_digest": code_tree_digest,
        "deployment_contract_id": deployment_contract_id,
    })
