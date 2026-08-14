"""跨节拍冻结预测的纯内容身份。"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime

from guvolu.research.provenance import stable_identifier


def interval_suite_forward_prediction_id(
    governance_method_version: str,
    prediction_method_version: str,
    plan_id: str,
    decision_time: datetime,
) -> str:
    """绑定计划、方法和共同决策时点。"""
    return stable_identifier("interval-suite-forward-prediction", {
        "governance_method_version": governance_method_version,
        "prediction_method_version": prediction_method_version,
        "plan_id": plan_id,
        "decision_time": decision_time.isoformat(),
    })


def interval_suite_member_panel_set_hash(
    plan_id: str,
    decision_time: datetime,
    members: Sequence[Mapping[str, object]],
) -> str:
    """绑定共同输入下全部成员面板的规范有序集合。"""
    normalized = sorted(
        ({str(key): value for key, value in member.items()} for member in members),
        key=lambda item: str(item.get("member_id")),
    )
    return stable_identifier("interval-suite-member-panel-set", {
        "method_version": "interval-suite-member-panel-set-v1",
        "plan_id": plan_id,
        "decision_time": decision_time.isoformat(),
        "members": normalized,
    })
