"""锁定研究目标路径后的成本敏感性诊断。"""
from __future__ import annotations

import hashlib
import json
import math
import statistics
from collections.abc import Mapping, Sequence
from pathlib import Path

from guvolu.research.artifact_contracts import INTERVAL_SECONDS, SECONDS_PER_YEAR
from guvolu.research.provenance import canonical_json
from guvolu.research.verification import verify_research_run

COST_SENSITIVITY_METHOD_VERSION = "fixed-target-cost-sensitivity-v1"
_REPLAYS = ("deployment", "walk_forward_stitched")


def _object(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} 必须为对象")
    return {str(key): item for key, item in value.items()}


def _text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} 必须为非空文本")
    return value


def _number(value: object, name: str) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValueError(f"{name} 必须为数值")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{name} 必须为有限数值")
    return result


def _cost_grid(values: Sequence[float]) -> tuple[float, ...]:
    result = tuple(sorted(set(float(value) for value in values)))
    if not result or any(not math.isfinite(value) or value < 0 for value in result):
        raise ValueError("成本网格必须包含有序非负有限数值")
    return result


def _metrics(
    gross_returns: Sequence[float],
    turnovers: Sequence[float],
    cost_bps: float,
    periods_per_year: float,
) -> Mapping[str, object]:
    rate = cost_bps / 10_000.0
    returns = tuple(
        gross - turnover * rate
        for gross, turnover in zip(gross_returns, turnovers, strict=True)
    )
    if not returns:
        raise ValueError("成本扫描没有 walk-forward OOS 收益")
    mean = statistics.fmean(returns)
    standard = statistics.pstdev(returns) if len(returns) > 1 else 0.0
    cumulative = 0.0
    peak = 0.0
    drawdown = 0.0
    for value in returns:
        cumulative += value
        peak = max(peak, cumulative)
        drawdown = max(drawdown, 1.0 - math.exp(cumulative - peak))
    turnover = sum(turnovers)
    return {
        "cost_bps": cost_bps,
        "bars": len(returns),
        "gross_log_return": sum(gross_returns),
        "net_log_return": sum(returns),
        "annual_return": mean * periods_per_year,
        "annual_volatility": standard * math.sqrt(periods_per_year),
        "sharpe": (
            mean / standard * math.sqrt(periods_per_year)
            if standard > 0 else 0.0
        ),
        "maximum_drawdown": drawdown,
        "turnover": turnover,
        "annual_turnover": turnover / len(returns) * periods_per_year,
        "cost": turnover * rate,
        "hit_rate": sum(value > 0 for value in returns) / len(returns),
    }


def _summary_family(
    summary: Mapping[str, object], family: str,
) -> Mapping[str, object]:
    raw = summary.get("family_evaluations")
    if not isinstance(raw, list):
        raise ValueError("summary.family_evaluations 必须为数组")
    matches = [
        _object(value, "family_evaluation") for value in raw
        if isinstance(value, Mapping) and value.get("family") == family
    ]
    if len(matches) != 1:
        raise ValueError("成本扫描流派必须在 summary 中唯一")
    return matches[0]


def _attest_base_metrics(
    replay: str,
    computed: Mapping[str, object],
    evaluation: Mapping[str, object],
) -> None:
    source_name = "deployment_oos_metrics" if replay == "deployment" else "metrics"
    source = _object(evaluation.get(source_name), source_name)
    field_map = {
        "bars": "bars",
        "net_log_return": "net_return",
        "sharpe": "sharpe",
        "maximum_drawdown": "maximum_drawdown",
        "turnover": "turnover",
        "annual_turnover": "annual_turnover",
        "cost": "cost",
    }
    mismatches: list[str] = []
    for computed_name, source_name in field_map.items():
        left = computed.get(computed_name)
        right = source.get(source_name)
        if computed_name == "bars":
            if left != right:
                mismatches.append(computed_name)
            continue
        if abs(
            _number(left, f"computed.{computed_name}")
            - _number(right, f"source.{source_name}")
        ) > 1e-10:
            mismatches.append(computed_name)
    if mismatches:
        raise ValueError(
            f"{replay} 基准成本不能重建 summary: " + ",".join(mismatches)
        )


def fixed_target_cost_sensitivity(
    manifest_path: Path,
    manifest_sha256: str,
    manifest: Mapping[str, object],
    summary: Mapping[str, object],
    replay_path: Path,
    replay_body: bytes,
    config: Mapping[str, object],
    family: str,
    cost_bps: Sequence[float],
    repository_root: Path,
) -> Mapping[str, object]:
    """从受保护回放构造不重选候选的 OOS 成本曲线。"""
    grid = _cost_grid(cost_bps)
    root = repository_root.resolve()
    try:
        manifest_relative = manifest_path.resolve().relative_to(root).as_posix()
        replay_relative = replay_path.resolve().relative_to(root).as_posix()
    except ValueError as error:
        raise ValueError("成本扫描来源必须位于项目目录内") from error
    artifacts = _object(manifest.get("artifacts"), "manifest.artifacts")
    replay_record = _object(artifacts.get("label_cost_replay"), "label_cost_replay")
    replay_sha = _text(replay_record.get("sha256"), "label replay sha256")
    if hashlib.sha256(replay_body).hexdigest() != replay_sha:
        raise ValueError("label cost replay 散列不一致")
    interval = _text(config.get("bar_interval"), "bar_interval")
    interval_seconds = INTERVAL_SECONDS.get(interval)
    if interval_seconds is None:
        raise ValueError("成本扫描不支持该 bar_interval")
    periods_per_year = SECONDS_PER_YEAR / interval_seconds

    gross: dict[str, list[float]] = {name: [] for name in _REPLAYS}
    turnovers: dict[str, list[float]] = {name: [] for name in _REPLAYS}
    header: Mapping[str, object] | None = None
    for line in replay_body.decode("utf-8").splitlines():
        row = _object(json.loads(line), "label cost row")
        if row.get("record_type") == "label_cost_header":
            if header is not None:
                raise ValueError("label cost replay 含重复 header")
            header = row
            continue
        if row.get("record_type") != "label_cost":
            raise ValueError("label cost replay 行类型无效")
        if row.get("in_walk_forward_oos") is not True:
            continue
        hard_gap = row.get("hard_gap") is True
        market_return = row.get("next_market_log_return")
        replays = _object(row.get("replays"), "replays")
        for replay in _REPLAYS:
            family_rows = _object(replays.get(replay), replay)
            family_row = _object(family_rows.get(family), f"{replay}.{family}")
            target = _number(
                family_row.get("target_at_decision"), "target_at_decision",
            )
            turnover = _number(family_row.get("turnover"), "turnover")
            if turnover < 0:
                raise ValueError("换手不得为负")
            gross_return = 0.0 if hard_gap else target * _number(
                market_return, "next_market_log_return",
            )
            baseline_bps = _number(
                header.get("cost_bps") if header else None,
                "header.cost_bps",
            )
            expected = gross_return - turnover * baseline_bps / 10_000.0
            observed = _number(
                family_row.get("next_net_return"), "next_net_return",
            )
            if abs(expected - observed) > 1e-12:
                raise ValueError("label cost replay 单行成本不能重建")
            gross[replay].append(gross_return)
            turnovers[replay].append(turnover)
    if header is None:
        raise ValueError("label cost replay 缺少 header")
    if header.get("research_identity") != manifest.get("research_identity"):
        raise ValueError("label cost replay 研究身份不一致")
    if family not in _object(
        header.get("deployment_candidates"), "deployment_candidates",
    ):
        raise ValueError("成本扫描流派不在固定部署候选中")

    evaluation = _summary_family(summary, family)
    baseline_bps = _number(header.get("cost_bps"), "header.cost_bps")
    results: dict[str, object] = {}
    for replay in _REPLAYS:
        curve = tuple(
            _metrics(gross[replay], turnovers[replay], value, periods_per_year)
            for value in grid
        )
        baseline = _metrics(
            gross[replay], turnovers[replay], baseline_bps, periods_per_year,
        )
        _attest_base_metrics(replay, baseline, evaluation)
        total_turnover = sum(turnovers[replay])
        results[replay] = {
            "break_even_one_way_bps": (
                sum(gross[replay]) / total_turnover * 10_000.0
                if total_turnover > 0 else None
            ),
            "curve": list(curve),
        }
    return {
        "schema_version": 1,
        "cost_sensitivity_method_version": COST_SENSITIVITY_METHOD_VERSION,
        "diagnostic_only": True,
        "selection_locked": True,
        "evaluation_scope": "walk_forward_oos_only",
        "family": family,
        "cost_bps_grid": list(grid),
        "source": {
            "run_id": manifest.get("run_id"),
            "research_identity": manifest.get("research_identity"),
            "manifest_path": manifest_relative,
            "manifest_sha256": manifest_sha256,
            "label_cost_replay_path": replay_relative,
            "label_cost_replay_sha256": replay_sha,
            "base_cost_bps": baseline_bps,
            "deployment_candidate_id": _object(
                header.get("deployment_candidates"), "deployment_candidates",
            )[family],
            "walk_forward_selection_path": _object(
                header.get("walk_forward_selection_paths"),
                "walk_forward_selection_paths",
            ).get(family),
        },
        "results": results,
        "interpretation": (
            "固定目标路径成本诊断，不重新选择候选，不构成推广或调参证据。"
        ),
    }


def build_fixed_target_cost_sensitivity(
    repository_root: Path,
    manifest_path: Path,
    family: str,
    cost_bps: Sequence[float],
) -> Mapping[str, object]:
    """完整复验研究运行后构造固定目标成本曲线。"""
    root = repository_root.resolve()
    verified = verify_research_run(root, manifest_path)
    manifest_body = verified.manifest_path.read_bytes()
    if hashlib.sha256(manifest_body).hexdigest() != verified.manifest_sha256:
        raise ValueError("完整复验后 manifest 字节发生变化")
    manifest = _object(
        json.loads(manifest_body),
        "manifest",
    )
    artifacts = _object(manifest.get("artifacts"), "manifest.artifacts")
    config_record = _object(artifacts.get("config"), "config")
    summary_record = _object(artifacts.get("summary_json"), "summary_json")
    replay_record = _object(artifacts.get("label_cost_replay"), "label_cost_replay")
    config_path = (root / _text(config_record.get("path"), "config.path")).resolve()
    summary_path = (root / _text(summary_record.get("path"), "summary.path")).resolve()
    replay_path = (root / _text(replay_record.get("path"), "replay.path")).resolve()
    snapshots: dict[str, bytes] = {}
    for name, path, record in (
        ("config", config_path, config_record),
        ("summary", summary_path, summary_record),
        ("replay", replay_path, replay_record),
    ):
        try:
            path.relative_to(root)
        except ValueError as error:
            raise ValueError(f"成本扫描 {name} 路径越出项目目录") from error
        body = path.read_bytes()
        expected = _text(record.get("sha256"), f"{name}.sha256")
        if hashlib.sha256(body).hexdigest() != expected:
            raise ValueError(f"完整复验后 {name} 字节发生变化")
        snapshots[name] = body
    config = _object(json.loads(snapshots["config"]), "config")
    summary = _object(json.loads(snapshots["summary"]), "summary")
    return fixed_target_cost_sensitivity(
        verified.manifest_path,
        verified.manifest_sha256,
        manifest,
        summary,
        replay_path,
        snapshots["replay"],
        config,
        family,
        cost_bps,
        root,
    )


def cost_sensitivity_bytes(payload: Mapping[str, object]) -> bytes:
    """返回成本制品规范字节。"""
    return (canonical_json(payload) + "\n").encode("utf-8")
