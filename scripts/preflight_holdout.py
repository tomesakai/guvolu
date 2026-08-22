"""holdout 消费前只读预检。

复核冻结前向预测对封存段的覆盖状况与身份一致性，绝不消费封存段、
绝不写入任何注册表或制品。判定「今日消费是否会因覆盖缺口被烧毁」，
把 holdout 先消费后校验的终局风险提前暴露为每日可见信号。

只读边界：本工具不构建研究面板、不读取封存窗口内的价格或收益，
仅使用时间戳、计数、散列与制品身份字段。
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

from guvolu.research.frozen_forward import verify_frozen_forward
from guvolu.research.governance import (
    FrozenForwardPrediction,
    get_frozen_forward_plan_for_vintage,
    list_frozen_forward_predictions,
    list_holdout_vintages,
)
from guvolu.research.panel import freeze_trade_inputs
from guvolu.research.provenance import code_identity, sha256_file

# 支持的研究节拍秒数
_INTERVAL_SECONDS = {
    "5min": 300,
    "1hour": 3_600,
    "4hour": 14_400,
    "1day": 86_400,
}

# 网格尾部宽限秒数
_TAIL_GRACE_SECONDS = 7_200

# 数据陈旧告警秒数
_DATA_STALE_SECONDS = 7_200


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _load_json(path: Path) -> dict[str, object]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"制品不是 JSON 对象: {path}")
    return payload


def _expected_grid(
    start: datetime,
    end: datetime,
    now: datetime,
    interval_seconds: int,
) -> list[datetime]:
    """封存段内当前应已存在预测的决策时点。"""
    cap = min(end, now - timedelta(seconds=_TAIL_GRACE_SECONDS + interval_seconds))
    ticks: list[datetime] = []
    tick = start
    while tick < cap:
        ticks.append(tick)
        tick = tick + timedelta(seconds=interval_seconds)
    return ticks


def _candidate_coverage(
    repository: Path,
    predictions: list[FrozenForwardPrediction],
    plan_candidates: set[str],
) -> list[dict[str, object]]:
    """逐制品核对候选目标齐全与散列一致。"""
    issues: list[dict[str, object]] = []
    for item in predictions:
        path = repository / str(item.prediction_artifact_path)
        if not path.exists():
            issues.append({
                "decision_time": _iso(item.decision_time),
                "issue": "prediction_artifact_missing",
            })
            continue
        if sha256_file(path) != item.prediction_artifact_sha256:
            issues.append({
                "decision_time": _iso(item.decision_time),
                "issue": "prediction_artifact_hash_mismatch",
            })
            continue
        payload = _load_json(path)
        families = payload.get("families")
        present = {
            str(row.get("candidate_id"))
            for row in families
            if isinstance(row, dict)
        } if isinstance(families, list) else set()
        missing = sorted(plan_candidates - present)
        if missing:
            issues.append({
                "decision_time": _iso(item.decision_time),
                "issue": "candidate_targets_incomplete",
                "missing": missing,
            })
    return issues


def run_preflight(
    root: Path,
    registry: Path,
    vintage_id: str | None,
    verify_artifacts: bool,
) -> dict[str, object]:
    repository = root.resolve()
    now = _utc_now()
    vintages = [
        item for item in list_holdout_vintages(registry)
        if vintage_id is None or item.vintage_id == vintage_id
    ]
    sealed = [item for item in vintages if item.status == "sealed"]
    if not sealed:
        raise LookupError("注册表中没有匹配的 sealed vintage")
    if len(sealed) > 1 and vintage_id is None:
        raise LookupError("存在多个 sealed vintage，须显式指定 vintage_id")
    vintage = sealed[0]
    plan = get_frozen_forward_plan_for_vintage(registry, vintage.vintage_id)
    if plan is None:
        raise LookupError("sealed vintage 没有登记冻结前向计划")

    blockers: list[dict[str, object]] = []
    warnings: list[dict[str, object]] = []

    plan_path = repository / plan.plan_artifact_path
    plan_hash_ok = plan_path.exists() and (
        sha256_file(plan_path) == plan.plan_artifact_sha256
    )
    if not plan_hash_ok:
        blockers.append({"issue": "plan_artifact_missing_or_hash_mismatch"})
        payload: dict[str, object] = {}
    else:
        payload = _load_json(plan_path)

    config_rel = payload.get("config_path")
    config_hash_ok: bool | None = None
    config_payload: dict[str, object] = {}
    if isinstance(config_rel, str):
        config_path = repository / config_rel
        config_hash_ok = (
            config_path.exists() and sha256_file(config_path) == plan.config_hash
        )
        if config_hash_ok:
            config_payload = _load_json(config_path)
        else:
            blockers.append({"issue": "config_hash_mismatch"})

    identity = None
    if isinstance(config_rel, str):
        identity = code_identity(repository, (repository / config_rel,))
        if not identity.decision_grade:
            warnings.append({"issue": "current_tree_not_decision_grade"})
        elif identity.tree_digest != plan.code_tree_digest:
            warnings.append({"issue": "code_tree_digest_mismatch"})

    interval = str(config_payload.get("bar_interval", "1hour"))
    interval_seconds = _INTERVAL_SECONDS.get(interval)
    if interval_seconds is None:
        raise ValueError(f"不支持的研究节拍: {interval}")

    market_id = config_payload.get("market_id")
    data_state: dict[str, object] = {}
    if isinstance(market_id, str):
        try:
            inputs = freeze_trade_inputs(repository / "data", market_id)
            data_state = {
                "maximum_event_time": _iso(inputs.maximum_event_time),
                "head_generation": inputs.head_generation,
            }
            lag = (now - inputs.maximum_event_time).total_seconds()
            if lag > _DATA_STALE_SECONDS:
                warnings.append({
                    "issue": "trade_head_stale",
                    "lag_seconds": int(lag),
                })
        except (LookupError, ValueError) as error:
            warnings.append({"issue": "trade_head_unavailable", "detail": str(error)})

    predictions = list(list_frozen_forward_predictions(registry, plan.plan_id))
    predicted_times = {item.decision_time for item in predictions}
    expected = _expected_grid(
        vintage.start_time, vintage.end_time, now, interval_seconds,
    )
    missing = sorted(tick for tick in expected if tick not in predicted_times)
    if missing:
        coverage_gap = {
            "issue": "prediction_coverage_gap",
            "missing_count": len(missing),
            "first_missing": _iso(missing[0]),
            "last_missing": _iso(missing[-1]),
        }
        # 零暴露政策下缺口降为告警
        if plan.missing_policy == "zero_exposure":
            warnings.append(coverage_gap)
        else:
            blockers.append(coverage_gap)

    candidates = payload.get("candidates")
    plan_candidate_ids = {
        str(item.get("candidate_id"))
        for item in candidates
        if isinstance(item, dict)
    } if isinstance(candidates, list) else set()
    artifact_issues = _candidate_coverage(repository, predictions, plan_candidate_ids)
    for item in artifact_issues:
        blockers.append(item)

    verify_state: str | None = None
    if verify_artifacts:
        try:
            verify_frozen_forward(
                repository, plan.plan_id, registry_path=registry,
            )
            verify_state = "passed"
        except (ValueError, LookupError, OSError) as error:
            verify_state = f"failed: {error}"
            blockers.append({"issue": "verify_frozen_forward_failed",
                             "detail": str(error)})

    started = now >= vintage.start_time
    if not started:
        status = "waiting"
    elif blockers:
        status = "would_burn"
    elif warnings:
        status = "degraded"
    else:
        status = "healthy"

    return {
        "generated_at": _iso(now),
        "read_only": True,
        "vintage_id": vintage.vintage_id,
        "vintage_window": [_iso(vintage.start_time), _iso(vintage.end_time)],
        "vintage_status": vintage.status,
        "plan_id": plan.plan_id,
        "missing_policy": plan.missing_policy,
        "interval": interval,
        "vintage_started": started,
        "expected_predictions": len(expected),
        "registered_predictions": len(predictions),
        "missing_decision_times": [_iso(tick) for tick in missing[:48]],
        "data_state": data_state,
        "verify_frozen_forward": verify_state,
        "warnings": warnings,
        "blockers": blockers,
        "status": status,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument(
        "--registry", type=Path,
        default=Path("data/research/governance.sqlite3"),
    )
    parser.add_argument("--vintage-id", default=None)
    parser.add_argument("--no-verify", action="store_true")
    parser.add_argument("--json-output", type=Path, default=None)
    args = parser.parse_args()
    root = args.root.resolve()
    registry = args.registry
    if not registry.is_absolute():
        registry = root / registry
    report = run_preflight(
        root, registry, args.vintage_id, verify_artifacts=not args.no_verify,
    )
    text = json.dumps(report, ensure_ascii=False, indent=2)
    if args.json_output is not None:
        args.json_output.parent.mkdir(parents=True, exist_ok=True)
        args.json_output.write_text(text + "\n", encoding="utf-8")
    print(text)
    status = report["status"]
    if status == "would_burn":
        return 2
    if status in ("degraded",):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
