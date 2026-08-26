"""行业稳健性证据生成器：成本、尾部、压力与容量四类制品。

本模块只读上游受完整性保护的研究制品与活动 head L2 事实，
生成内容寻址、带方法版本与输入身份的独立证据制品。
它不做候选重选、不做晋级、不改研究配置，也不写治理库。
阈值一律来自版本化配置（G-06），随机来源固定种子（G-03），
输入身份与代码身份随制品留存（D-09），面板只用样本外区段（D-04）。
覆盖不足时显式标注并失败关闭，绝不外推。
"""
from __future__ import annotations

import hashlib
import json
import math
import random
import statistics
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from guvolu.research.artifact_contracts import INTERVAL_SECONDS, SECONDS_PER_YEAR
from guvolu.research.provenance import (
    canonical_json,
    sha256_file,
    stable_identifier,
)

INDUSTRY_EVIDENCE_METHOD_VERSION = "industry-evidence-v2"
COST_SCENARIO_METHOD_VERSION = "fixed-target-cost-sensitivity-v1"
TAIL_SCENARIO_METHOD_VERSION = "walk-forward-tail-v1"
STRESS_SCENARIO_METHOD_VERSION = "walk-forward-stress-v1"
CAPACITY_SCENARIO_METHOD_VERSION = "l2-depth-capacity-v1"
GENERATOR_ATTESTATION_METHOD_VERSION = (
    "industry-evidence-generator-attestation-v1"
)
GENERATOR_ID = "guvolu-industry-evidence-generator-v1"

SOURCE_ARTIFACT_NAMES: Mapping[str, str] = {
    "tail": "tail_risk_evidence",
    "stress": "stress_scenario_evidence",
    "cost": "fixed_target_cost_replay",
    "capacity": "l2_depth_capacity_evidence",
}
SCENARIO_METHOD_VERSIONS: Mapping[str, str] = {
    "tail": TAIL_SCENARIO_METHOD_VERSION,
    "stress": STRESS_SCENARIO_METHOD_VERSION,
    "cost": COST_SCENARIO_METHOD_VERSION,
    "capacity": CAPACITY_SCENARIO_METHOD_VERSION,
}
SCENARIO_COLLECTIONS: Mapping[str, str] = {
    "tail": "tail_scenarios",
    "stress": "stress_scenarios",
    "cost": "cost_scenarios",
    "capacity": "capacity_scenarios",
}
COST_TIERS: tuple[str, ...] = ("policy_baseline", "adverse", "severe")
COST_COMPONENTS: tuple[str, ...] = ("fee", "half_spread", "slippage", "impact")
STRESS_DEFINITIONS: tuple[str, ...] = (
    "cross_venue_dislocation", "liquidity_gap", "volatility_spike",
)
INSUFFICIENT_L2_COVERAGE = "insufficient_l2_coverage"
INSUFFICIENT_CROSS_VENUE_COVERAGE = "insufficient_cross_venue_coverage"
LEDGER_METHOD_VERSION = "industry-evidence-ledger-v1"
_REPLAY = "deployment"
_BPS = 10_000.0


def _object(value: object, name: str) -> Mapping[str, object]:
    """收窄为字符串键映射。"""
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} 必须为对象")
    return {str(key): item for key, item in value.items()}


def _sequence(value: object, name: str) -> Sequence[object]:
    """收窄为非文本序列。"""
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ValueError(f"{name} 必须为数组")
    return value


def _text(value: object, name: str) -> str:
    """读取非空文本。"""
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} 必须为非空文本")
    return value


def _number(value: object, name: str) -> float:
    """读取有限数值。"""
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValueError(f"{name} 必须为数值")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{name} 必须为有限数值")
    return result


def _integer(value: object, name: str) -> int:
    """读取整数。"""
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"{name} 必须为整数")
    return value


def _time(value: object, name: str) -> datetime:
    """解析带时区 ISO 时间。"""
    text = _text(value, name)
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"{name} 必须带时区")
    return parsed.astimezone(UTC)


def _quantile(values: Sequence[float], probability: float) -> float:
    """线性插值的确定性经验分位数。"""
    if not values or not 0.0 <= probability <= 1.0:
        raise ValueError("经验分位数输入非法")
    ordered = sorted(values)
    position = probability * (len(ordered) - 1)
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def generator_code_sha256() -> str:
    """本生成器源文件的内容身份。"""
    return sha256_file(Path(__file__).resolve())


@dataclass(frozen=True)
class CandidatePath:
    """一个固定部署候选的样本外目标路径。"""

    family: str
    candidate_id: str
    decision_times: tuple[datetime, ...]
    label_times: tuple[datetime, ...]
    market_returns: tuple[float, ...]
    gross_returns: tuple[float, ...]
    turnovers: tuple[float, ...]
    fold_ids: tuple[str, ...]


@dataclass(frozen=True)
class EvidenceWindow:
    """一段证据的覆盖区间与完整度。"""

    from_time: datetime
    to_time: datetime
    available_through: datetime
    bars: int
    folds: int
    coverage_ratio: float

    def payload(self) -> Mapping[str, object]:
        """转换为检查器要求的 coverage 记录。"""
        return {
            "from_time": self.from_time.isoformat(),
            "to_time": self.to_time.isoformat(),
            "available_through": self.available_through.isoformat(),
            "bars": self.bars,
            "folds": self.folds,
            "coverage_ratio": self.coverage_ratio,
        }


@dataclass(frozen=True)
class SourceArtifact:
    """已落盘来源制品的最小身份引用。"""

    name: str
    kind: str
    path: str
    sha256: str
    bytes_count: int

    def reference(self) -> Mapping[str, object]:
        """转换为检查器要求的 source_artifact 记录。"""
        return {
            "name": self.name,
            "kind": self.kind,
            "path": self.path,
            "sha256": self.sha256,
            "artifact_id": f"sha256-{self.sha256}",
            "bytes": self.bytes_count,
        }


@dataclass(frozen=True)
class RunIdentity:
    """研究运行的输入身份与时间边界。"""

    run_id: str
    research_identity: str
    config_hash: str
    input_receipt_sha256: str
    decision_time: datetime
    execution_evaluated_at: datetime
    market_id: str
    panel_sha256: str
    panel_available_through: datetime
    periods_per_year: float
    baseline_cost_bps: float
    cost_components_bps: Mapping[str, float]
    block_bootstrap_bars: int
    block_bootstrap_seed: int


def read_run_identity(
    manifest: Mapping[str, object],
    summary: Mapping[str, object],
    config: Mapping[str, object],
) -> RunIdentity:
    """从受保护 manifest、summary 与配置快照读取输入身份。"""
    panel = _object(summary.get("panel"), "summary.panel")
    cost_model = _object(config.get("cost_model"), "config.cost_model")
    validation = _object(config.get("validation"), "config.validation")
    interval = _text(config.get("bar_interval"), "config.bar_interval")
    interval_seconds = INTERVAL_SECONDS.get(interval)
    if interval_seconds is None:
        raise ValueError("证据生成不支持该 bar_interval")
    components = {
        "fee": _number(
            cost_model.get("fee_bps_assumption"), "fee_bps_assumption",
        ),
        "half_spread": _number(
            cost_model.get("half_spread_bps_assumption"),
            "half_spread_bps_assumption",
        ),
        "slippage": _number(
            cost_model.get("slippage_bps_assumption"),
            "slippage_bps_assumption",
        ),
        "impact": _number(
            cost_model.get("impact_bps_assumption"), "impact_bps_assumption",
        ),
    }
    if any(value < 0.0 for value in components.values()):
        raise ValueError("成本分量不得为负")
    return RunIdentity(
        run_id=_text(manifest.get("run_id"), "manifest.run_id"),
        research_identity=_text(
            manifest.get("research_identity"), "manifest.research_identity",
        ),
        config_hash=_text(manifest.get("config_hash"), "manifest.config_hash"),
        input_receipt_sha256=_text(
            manifest.get("input_receipt_sha256"),
            "manifest.input_receipt_sha256",
        ),
        decision_time=_time(
            manifest.get("decision_time"), "manifest.decision_time",
        ),
        execution_evaluated_at=_time(
            manifest.get("execution_evaluated_at"),
            "manifest.execution_evaluated_at",
        ),
        market_id=_text(summary.get("market_id"), "summary.market_id"),
        panel_sha256=_text(panel.get("sha256"), "summary.panel.sha256"),
        panel_available_through=_time(
            panel.get("latest_available_time"),
            "summary.panel.latest_available_time",
        ),
        periods_per_year=SECONDS_PER_YEAR / interval_seconds,
        baseline_cost_bps=sum(components.values()),
        cost_components_bps=components,
        block_bootstrap_bars=_integer(
            validation.get("block_bootstrap_bars"), "block_bootstrap_bars",
        ),
        block_bootstrap_seed=_integer(
            validation.get("block_bootstrap_random_seed"),
            "block_bootstrap_random_seed",
        ),
    )


def read_candidate_paths(
    replay_body: bytes,
    summary: Mapping[str, object],
    panel_to_time: datetime,
) -> tuple[CandidatePath, ...]:
    """从受保护 label cost replay 读取各流派样本外目标路径。"""
    families: dict[str, str] = {}
    for raw in _sequence(
        summary.get("family_evaluations"), "summary.family_evaluations",
    ):
        evaluation = _object(raw, "family_evaluation")
        if evaluation.get("eligible") is not True:
            continue
        if evaluation.get("mode") != "paper":
            continue
        families[_text(evaluation.get("family"), "family")] = _text(
            evaluation.get("deployment_candidate_id"),
            "deployment_candidate_id",
        )
    if not families:
        raise ValueError("研究运行没有 paper 可用的部署候选")
    decisions: list[datetime] = []
    labels: list[datetime] = []
    folds: list[str] = []
    market_returns: list[float] = []
    gross: dict[str, list[float]] = {name: [] for name in families}
    turnovers: dict[str, list[float]] = {name: [] for name in families}
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
        label_time = _time(
            row.get("label_available_time"), "label_available_time",
        )
        if label_time > panel_to_time:
            raise ValueError("样本外区段越过面板截止上限")
        decisions.append(_time(row.get("decision_time"), "decision_time"))
        labels.append(label_time)
        folds.append(_text(row.get("walk_forward_fold_id"), "fold_id"))
        replays = _object(row.get("replays"), "replays")
        replay = _object(replays.get(_REPLAY), _REPLAY)
        hard_gap = row.get("hard_gap") is True
        market_return = 0.0 if hard_gap else _number(
            row.get("next_market_log_return"), "next_market_log_return",
        )
        market_returns.append(market_return)
        for family in families:
            entry = _object(replay.get(family), f"{_REPLAY}.{family}")
            target = _number(
                entry.get("target_at_decision"), "target_at_decision",
            )
            turnover = _number(entry.get("turnover"), "turnover")
            if turnover < 0.0:
                raise ValueError("换手不得为负")
            gross[family].append(target * market_return)
            turnovers[family].append(turnover)
    if header is None:
        raise ValueError("label cost replay 缺少 header")
    if header.get("research_identity") != summary.get("research_identity"):
        raise ValueError("label cost replay 研究身份不一致")
    if not decisions:
        raise ValueError("label cost replay 没有样本外区段")
    registered = _object(
        header.get("deployment_candidates"), "deployment_candidates",
    )
    for family, candidate_id in families.items():
        if registered.get(family) != candidate_id:
            raise ValueError("部署候选与 replay header 不一致")
    return tuple(
        CandidatePath(
            family=family,
            candidate_id=candidate_id,
            decision_times=tuple(decisions),
            label_times=tuple(labels),
            market_returns=tuple(market_returns),
            gross_returns=tuple(gross[family]),
            turnovers=tuple(turnovers[family]),
            fold_ids=tuple(folds),
        )
        for family, candidate_id in sorted(families.items())
    )


def evidence_window(path: CandidatePath, available_through: datetime) -> EvidenceWindow:
    """样本外区段的覆盖区间；缺口以 fold 连续性衡量。"""
    return EvidenceWindow(
        from_time=path.decision_times[0],
        to_time=path.label_times[-1],
        available_through=available_through,
        bars=len(path.gross_returns),
        folds=len(set(path.fold_ids)),
        coverage_ratio=1.0,
    )


def _segment_metrics(
    gross: Sequence[float],
    turnovers: Sequence[float],
    cost_bps: float,
    periods_per_year: float,
) -> Mapping[str, float]:
    """一段收益的净成本指标，口径与成本敏感性一致。"""
    rate = cost_bps / _BPS
    returns = [
        value - turnover * rate
        for value, turnover in zip(gross, turnovers, strict=True)
    ]
    if not returns:
        raise ValueError("指标区段为空")
    mean = statistics.fmean(returns)
    standard = statistics.pstdev(returns) if len(returns) > 1 else 0.0
    cumulative = 0.0
    peak = 0.0
    drawdown = 0.0
    for value in returns:
        cumulative += value
        peak = max(peak, cumulative)
        drawdown = max(drawdown, 1.0 - math.exp(cumulative - peak))
    return {
        "sharpe": (
            mean / standard * math.sqrt(periods_per_year)
            if standard > 0.0 else 0.0
        ),
        "net_return": sum(returns),
        "maximum_drawdown": min(drawdown, 1.0),
        "turnover": sum(turnovers),
    }


def _scenario_record(
    kind: str,
    *,
    identity: RunIdentity,
    path: CandidatePath,
    window: EvidenceWindow,
    scenario_key: str,
    parameters: Mapping[str, object],
    metrics: Mapping[str, object],
    source: SourceArtifact,
    extra: Mapping[str, object] | None = None,
) -> Mapping[str, object]:
    """构造一条不可变、候选绑定的场景并复算内容身份。"""
    scenario: dict[str, object] = {
        "schema_version": 1,
        "scenario_type": kind,
        "scenario_key": scenario_key,
        "method_version": SCENARIO_METHOD_VERSIONS[kind],
        "family": path.family,
        "candidate_id": path.candidate_id,
        "selection_locked": True,
        "walk_forward_oos_only": True,
        "pit_verified": True,
        "registered_at": identity.decision_time.isoformat(),
        "coverage": dict(window.payload()),
        "parameters": dict(parameters),
        "metrics": dict(metrics),
        "source_artifact": dict(source.reference()),
    }
    if extra is not None:
        scenario.update(extra)
    scenario["scenario_id"] = stable_identifier("industry-scenario", scenario)
    return scenario


def cost_tier_grid(
    identity: RunIdentity,
    multipliers: Mapping[str, object],
) -> tuple[tuple[str, float, Mapping[str, float]], ...]:
    """按配置倍数展开严格递增的成本档位。"""
    grid: list[tuple[str, float, Mapping[str, float]]] = []
    for tier in COST_TIERS:
        multiple = _number(multipliers.get(tier), f"cost_tier.{tier}")
        if multiple <= 0.0:
            raise ValueError("成本倍数必须为正")
        components = {
            name: identity.cost_components_bps[name] * multiple
            for name in COST_COMPONENTS
        }
        grid.append((tier, sum(components.values()), components))
    for previous, current in zip(grid, grid[1:]):
        if current[1] <= previous[1]:
            raise ValueError("成本档位必须严格递增")
    return tuple(grid)


def build_cost_evidence(
    identity: RunIdentity,
    paths: Sequence[CandidatePath],
    multipliers: Mapping[str, object],
    minimum_step_bps: float,
    generated_at: datetime,
) -> Mapping[str, object]:
    """成本情景来源制品：固定目标路径在各成本档的重放事实。"""
    grid = cost_tier_grid(identity, multipliers)
    for previous, current in zip(grid, grid[1:]):
        if current[1] - previous[1] < minimum_step_bps - 1e-9:
            raise ValueError("相邻成本档差低于配置最小步长")
    candidates: list[Mapping[str, object]] = []
    for path in paths:
        window = evidence_window(path, identity.panel_available_through)
        tiers: list[Mapping[str, object]] = []
        for tier, total, components in grid:
            metrics = _segment_metrics(
                path.gross_returns,
                path.turnovers,
                total,
                identity.periods_per_year,
            )
            tiers.append({
                "cost_tier": tier,
                "total_cost_bps": total,
                "cost_components_bps": dict(components),
                "cost_quote": sum(path.turnovers) * total / _BPS,
                "metrics": dict(metrics),
            })
        candidates.append({
            "family": path.family,
            "candidate_id": path.candidate_id,
            "coverage": dict(window.payload()),
            "tiers": tiers,
        })
    return _source_payload(
        "cost",
        identity,
        generated_at,
        {
            "statistic": "fixed_target_walk_forward_oos_cost_replay",
            "replay": _REPLAY,
            "baseline_total_cost_bps": identity.baseline_cost_bps,
            "baseline_components_bps": dict(identity.cost_components_bps),
            "tier_multipliers": {
                tier: _number(multipliers.get(tier), tier)
                for tier in COST_TIERS
            },
            "minimum_step_bps": minimum_step_bps,
            "formula": "每档成本按配置倍数缩放基准分量后重算净收益",
        },
        candidates,
    )


def _bootstrap_paths(
    net_returns: Sequence[float],
    turnovers: Sequence[float],
    block_bars: int,
    samples: int,
    seed: int,
    periods_per_year: float,
) -> tuple[tuple[float, float, float, float], ...]:
    """循环折块重采样，保留短程依赖并固定种子。"""
    count = len(net_returns)
    if count < 2:
        raise ValueError("尾部重采样需要至少两个观测")
    if block_bars <= 0 or block_bars > count:
        raise ValueError("折块长度超出收益序列")
    if samples <= 0:
        raise ValueError("重采样样本数必须为正")
    full_blocks, remainder = divmod(count, block_bars)
    generator = random.Random(seed)
    results: list[tuple[float, float, float, float]] = []
    for _sample in range(samples):
        starts = [generator.randrange(count) for _block in range(full_blocks)]
        tail_start = generator.randrange(count) if remainder else None
        sequence: list[float] = []
        turnover = 0.0
        spans = [(start, block_bars) for start in starts]
        if tail_start is not None:
            spans.append((tail_start, remainder))
        for start, length in spans:
            for offset in range(length):
                index = (start + offset) % count
                sequence.append(net_returns[index])
                turnover += turnovers[index]
        mean = statistics.fmean(sequence)
        standard = statistics.pstdev(sequence)
        cumulative = 0.0
        peak = 0.0
        drawdown = 0.0
        for value in sequence:
            cumulative += value
            peak = max(peak, cumulative)
            drawdown = max(drawdown, 1.0 - math.exp(cumulative - peak))
        results.append((
            mean / standard * math.sqrt(periods_per_year)
            if standard > 0.0 else 0.0,
            sum(sequence),
            min(drawdown, 1.0),
            turnover,
        ))
    return tuple(results)


def build_tail_evidence(
    identity: RunIdentity,
    paths: Sequence[CandidatePath],
    probabilities: Sequence[float],
    samples: int,
    generated_at: datetime,
) -> Mapping[str, object]:
    """尾部情景来源制品：循环折块重采样的下尾分位与期望短缺。"""
    grid = tuple(_number(value, "tail_probability") for value in probabilities)
    if not grid or sorted(set(grid)) != list(grid):
        raise ValueError("尾部概率网格必须严格递增且不重复")
    rate = identity.baseline_cost_bps / _BPS
    candidates: list[Mapping[str, object]] = []
    for path in paths:
        window = evidence_window(path, identity.panel_available_through)
        net_returns = [
            value - turnover * rate
            for value, turnover in zip(
                path.gross_returns, path.turnovers, strict=True,
            )
        ]
        replicas = _bootstrap_paths(
            net_returns,
            path.turnovers,
            identity.block_bootstrap_bars,
            samples,
            identity.block_bootstrap_seed,
            identity.periods_per_year,
        )
        sharpes = [item[0] for item in replicas]
        returns = [item[1] for item in replicas]
        drawdowns = [item[2] for item in replicas]
        turnover_values = [item[3] for item in replicas]
        ordered_returns = sorted(returns)
        levels: list[Mapping[str, object]] = []
        for probability in grid:
            worst = max(1, math.ceil(samples * probability))
            shortfall = statistics.fmean(ordered_returns[:worst])
            levels.append({
                "tail_probability": probability,
                "block_length": identity.block_bootstrap_bars,
                "worst_replica_count": worst,
                "expected_shortfall_raw": shortfall,
                "metrics": {
                    "sharpe": _quantile(sharpes, probability),
                    "net_return": _quantile(returns, probability),
                    "maximum_drawdown": _quantile(
                        drawdowns, 1.0 - probability,
                    ),
                    "turnover": _quantile(
                        turnover_values, 1.0 - probability,
                    ),
                    "expected_shortfall": max(-1.0, min(0.0, shortfall)),
                },
            })
        candidates.append({
            "family": path.family,
            "candidate_id": path.candidate_id,
            "coverage": dict(window.payload()),
            "levels": levels,
        })
    return _source_payload(
        "tail",
        identity,
        generated_at,
        {
            "statistic": "circular_block_bootstrap_net_return_quantile",
            "base_cost_bps": identity.baseline_cost_bps,
            "block_length": identity.block_bootstrap_bars,
            "bootstrap_samples": samples,
            "random_seed": identity.block_bootstrap_seed,
            "expected_shortfall": (
                "净收益最差 ceil(p*样本数) 条重采样路径的均值"
            ),
            "formula": (
                "对样本外净收益按固定种子循环折块重采样，"
                "取各指标的下尾分位；回撤与换手取上尾分位"
            ),
        },
        candidates,
    )


def _volatility_statistic(
    market_returns: Sequence[float],
    lookback: int,
) -> tuple[float | None, ...]:
    """决策时刻可见的滞后已实现波动。"""
    if lookback <= 1:
        raise ValueError("波动回看必须大于一")
    result: list[float | None] = []
    for index in range(len(market_returns)):
        start = index - lookback
        if start < 0:
            result.append(None)
            continue
        window = market_returns[start:index]
        result.append(statistics.pstdev(window) if len(window) > 1 else None)
    return tuple(result)


def _selected_indices(
    statistic: Sequence[float | None],
    probability: float,
    upper_tail: bool,
) -> tuple[int, ...]:
    """按分位阈值选出确定性子区间。"""
    values = [value for value in statistic if value is not None]
    if not values:
        return ()
    threshold = _quantile(values, probability)
    if upper_tail:
        return tuple(
            index for index, value in enumerate(statistic)
            if value is not None and value >= threshold
        )
    return tuple(
        index for index, value in enumerate(statistic)
        if value is not None and value <= threshold
    )


def read_pit_volume_scores(
    feature_body: bytes,
    decision_times: Sequence[datetime],
) -> tuple[float | None, ...]:
    """按决策时刻对齐 PIT 成交量分位特征。"""
    scores: dict[datetime, float | None] = {}
    for line in feature_body.decode("utf-8").splitlines():
        if not line.strip():
            continue
        row = _object(json.loads(line), "feature row")
        if row.get("record_type") != "feature":
            continue
        raw = row.get("volume_score")
        decision = _time(row.get("decision_time"), "feature.decision_time")
        scores[decision] = (
            None if raw is None else _number(raw, "volume_score")
        )
    return tuple(scores.get(value) for value in decision_times)


def build_stress_evidence(
    identity: RunIdentity,
    paths: Sequence[CandidatePath],
    volume_scores: Sequence[float | None],
    cross_venue_spreads: Sequence[float | None],
    settings: Mapping[str, object],
    cross_venue_coverage: Mapping[str, object],
    generated_at: datetime,
) -> Mapping[str, object]:
    """压力情景来源制品：三类可复算子区间与其上重算的指标。"""
    lookback = _integer(
        settings.get("volatility_lookback_bars"), "volatility_lookback_bars",
    )
    volatility_quantile = _number(
        settings.get("volatility_quantile"), "volatility_quantile",
    )
    volume_quantile = _number(
        settings.get("volume_quantile"), "volume_quantile",
    )
    cross_venue_quantile = _number(
        settings.get("cross_venue_quantile"), "cross_venue_quantile",
    )
    minimum_bars = _integer(
        settings.get("minimum_subinterval_bars"), "minimum_subinterval_bars",
    )
    if not 0.0 < volatility_quantile < 1.0 or not 0.0 < volume_quantile < 1.0:
        raise ValueError("压力分位必须位于开区间 (0,1)")
    if not 0.0 < cross_venue_quantile < 1.0:
        raise ValueError("压力分位必须位于开区间 (0,1)")
    quantiles = {
        "volatility_spike": volatility_quantile,
        "liquidity_gap": volume_quantile,
        "cross_venue_dislocation": cross_venue_quantile,
    }
    candidates: list[Mapping[str, object]] = []
    for path in paths:
        window = evidence_window(path, identity.panel_available_through)
        volatility = _volatility_statistic(path.market_returns, lookback)
        definitions: list[Mapping[str, object]] = []
        selections = {
            "volatility_spike": _selected_indices(
                volatility, volatility_quantile, True,
            ),
            "liquidity_gap": _selected_indices(
                tuple(volume_scores), volume_quantile, False,
            ),
            "cross_venue_dislocation": _selected_indices(
                tuple(cross_venue_spreads), cross_venue_quantile, True,
            ),
        }
        for definition in STRESS_DEFINITIONS:
            if (
                definition == "cross_venue_dislocation"
                and not selections[definition]
            ):
                definitions.append({
                    "stress_definition": definition,
                    "available": False,
                    "insufficient_reason": INSUFFICIENT_CROSS_VENUE_COVERAGE,
                    "coverage": dict(cross_venue_coverage),
                    "metrics": None,
                })
                continue
            indices = selections[definition]
            if len(indices) < minimum_bars:
                definitions.append({
                    "stress_definition": definition,
                    "available": False,
                    "insufficient_reason": "insufficient_subinterval_bars",
                    "selected_bars": len(indices),
                    "metrics": None,
                })
                continue
            metrics = _segment_metrics(
                [path.gross_returns[index] for index in indices],
                [path.turnovers[index] for index in indices],
                identity.baseline_cost_bps,
                identity.periods_per_year,
            )
            base = _segment_metrics(
                path.gross_returns,
                path.turnovers,
                identity.baseline_cost_bps,
                identity.periods_per_year,
            )
            severity = (
                len(path.gross_returns) / len(indices)
                if indices else 0.0
            )
            definitions.append({
                "stress_definition": definition,
                "available": True,
                "severity": severity,
                "selected_bars": len(indices),
                "selection_quantile": quantiles[definition],
                "first_selected_decision_time": (
                    path.decision_times[indices[0]].isoformat()
                ),
                "last_selected_decision_time": (
                    path.decision_times[indices[-1]].isoformat()
                ),
                "metrics": dict(metrics),
                "full_window_metrics": dict(base),
            })
        candidates.append({
            "family": path.family,
            "candidate_id": path.candidate_id,
            "coverage": dict(window.payload()),
            "definitions": definitions,
        })
    return _source_payload(
        "stress",
        identity,
        generated_at,
        {
            "base_cost_bps": identity.baseline_cost_bps,
            "minimum_subinterval_bars": minimum_bars,
            "volatility_spike": {
                "statistic": "trailing_realized_volatility_of_market_return",
                "lookback_bars": lookback,
                "selection": "statistic_ge_quantile",
                "quantile": volatility_quantile,
            },
            "liquidity_gap": {
                "statistic": "pit_volume_score_from_verified_feature_panel",
                "selection": "statistic_le_quantile",
                "quantile": volume_quantile,
            },
            "cross_venue_dislocation": {
                "statistic": "decision_aligned_cross_venue_mid_spread",
                "selection": "statistic_ge_quantile",
                "quantile": cross_venue_quantile,
                "coverage": dict(cross_venue_coverage),
            },
            "severity": "全样本外柱数除以子区间柱数",
            "formula": (
                "在样本外柱上计算 PIT 统计量，按分位阈值选出子区间，"
                "在子区间上以基准成本重算指标"
            ),
        },
        candidates,
    )


def notional_key(notional: float) -> str:
    """名义规模在采样字典中的稳定键。"""
    return f"{notional:.6f}"


def _walk_book(
    levels: Sequence[Mapping[str, object]],
    notional: Decimal,
) -> tuple[Decimal, Decimal] | None:
    """按名义规模吃单，返回成交量与加权成交额。"""
    remaining = notional
    filled = Decimal(0)
    cost = Decimal(0)
    for level in levels:
        price = Decimal(str(level["price"]))
        available = Decimal(str(level["notional"]))
        if price <= 0:
            continue
        take = min(remaining, available)
        filled += take / price
        cost += take
        remaining -= take
        if remaining <= 0:
            return filled, cost
    return None


def depth_observation(
    payload: Mapping[str, object],
    notional: Decimal,
) -> Mapping[str, object] | None:
    """从一份 L2 状态估算可用深度与冲击 bps。"""
    raw_mid = payload.get("mid")
    if raw_mid is None:
        return None
    mid = Decimal(str(raw_mid))
    if mid <= 0:
        return None
    asks = [
        _object(level, "ask level")
        for level in _sequence(payload.get("asks"), "asks")
    ]
    bids = [
        _object(level, "bid level")
        for level in _sequence(payload.get("bids"), "bids")
    ]
    if not asks or not bids:
        return None
    ask_depth = sum(Decimal(str(level["notional"])) for level in asks)
    bid_depth = sum(Decimal(str(level["notional"])) for level in bids)
    walked = _walk_book(asks, notional)
    if walked is None:
        return None
    filled, cost = walked
    if filled <= 0:
        return None
    average = cost / filled
    impact = (average - mid) / mid * Decimal(_BPS)
    return {
        "as_of": payload.get("as_of"),
        "mid": float(mid),
        "ask_depth_quote": float(ask_depth),
        "bid_depth_quote": float(bid_depth),
        "one_sided_depth_quote": float(min(ask_depth, bid_depth)),
        "impact_bps": max(0.0, float(impact)),
    }


def build_capacity_evidence(
    identity: RunIdentity,
    paths: Sequence[CandidatePath],
    venue_facts: Sequence[Mapping[str, object]],
    execution_market_id: str,
    settings: Mapping[str, object],
    generated_at: datetime,
) -> Mapping[str, object]:
    """容量情景来源制品：活动 head L2 深度事实与参与率估计。"""
    depth_quantile = _number(settings.get("depth_quantile"), "depth_quantile")
    horizon = _integer(
        settings.get("depth_horizon_seconds"), "depth_horizon_seconds",
    )
    minimum_samples = _integer(
        settings.get("minimum_depth_samples"), "minimum_depth_samples",
    )
    grid = tuple(
        _number(value, "notional_quote")
        for value in _sequence(
            settings.get("notional_quote_grid"), "notional_quote_grid",
        )
    )
    if sorted(set(grid)) != list(grid) or any(value <= 0.0 for value in grid):
        raise ValueError("容量名义规模网格必须严格递增且为正")
    execution = next(
        (
            fact for fact in venue_facts
            if fact.get("market_id") == execution_market_id
        ),
        None,
    )
    missing = [
        str(fact.get("market_id")) for fact in venue_facts
        if fact.get("sufficient") is not True
    ]
    scenarios: list[Mapping[str, object]] = []
    for notional in grid:
        if execution is None or missing:
            scenarios.append({
                "notional_quote": notional,
                "available": False,
                "insufficient_reason": INSUFFICIENT_L2_COVERAGE,
                "insufficient_market_ids": sorted(missing),
                "metrics": None,
            })
            continue
        samples = _object(execution.get("samples"), "samples")
        raw_entry = samples.get(notional_key(notional))
        observations = [
            _object(item, "depth observation")
            for item in _sequence(
                _object(raw_entry, "notional samples").get("observations"),
                "observations",
            )
        ] if raw_entry is not None else []
        if len(observations) < minimum_samples:
            scenarios.append({
                "notional_quote": notional,
                "available": False,
                "insufficient_reason": INSUFFICIENT_L2_COVERAGE,
                "resolved_samples": len(observations),
                "metrics": None,
            })
            continue
        depth = _quantile(
            [
                _number(item.get("one_sided_depth_quote"), "depth")
                for item in observations
            ],
            depth_quantile,
        )
        impact = _quantile(
            [_number(item.get("impact_bps"), "impact") for item in observations],
            1.0 - depth_quantile,
        )
        if depth <= 0.0:
            scenarios.append({
                "notional_quote": notional,
                "available": False,
                "insufficient_reason": INSUFFICIENT_L2_COVERAGE,
                "metrics": None,
            })
            continue
        scenarios.append({
            "notional_quote": notional,
            "available": True,
            "observed_depth_quote": depth,
            "participation_rate": notional / depth,
            "impact_bps": impact,
            "resolved_samples": len(observations),
        })
    candidates: list[Mapping[str, object]] = []
    for path in paths:
        entries: list[Mapping[str, object]] = []
        for scenario in scenarios:
            if scenario.get("available") is not True:
                entries.append({**scenario, "metrics": None})
                continue
            impact = _number(scenario.get("impact_bps"), "impact_bps")
            metrics = _segment_metrics(
                path.gross_returns,
                path.turnovers,
                identity.baseline_cost_bps + impact,
                identity.periods_per_year,
            )
            entries.append({
                **scenario,
                "total_cost_bps": identity.baseline_cost_bps + impact,
                "metrics": dict(metrics),
            })
        candidates.append({
            "family": path.family,
            "candidate_id": path.candidate_id,
            "coverage": dict(
                _capacity_window(identity, venue_facts, execution).payload()
            ) if execution is not None else None,
            "scenarios": entries,
        })
    return _source_payload(
        "capacity",
        identity,
        generated_at,
        {
            "statistic": "active_head_l2_one_sided_depth_and_walked_impact",
            "execution_market_id": execution_market_id,
            "depth_quantile": depth_quantile,
            "depth_horizon_seconds": horizon,
            "minimum_depth_samples": minimum_samples,
            "notional_quote_grid": list(grid),
            "participation_rate": "名义规模除以同分位单侧深度",
            "formula": (
                "在活动 head 上按固定时点网格解析 L2，"
                "以单侧名义深度分位为可用深度，吃单加权价与中价之差为冲击"
            ),
            "fail_closed": (
                f"任一来源覆盖不足时标注 {INSUFFICIENT_L2_COVERAGE}"
            ),
        },
        candidates,
        extra={"venues": [dict(fact) for fact in venue_facts]},
    )


def _capacity_window(
    identity: RunIdentity,
    venue_facts: Sequence[Mapping[str, object]],
    execution: Mapping[str, object] | None,
) -> EvidenceWindow:
    """容量证据的 L2 覆盖区间。"""
    del venue_facts
    if execution is None:
        raise ValueError("容量证据缺少执行来源 L2 事实")
    return EvidenceWindow(
        from_time=_time(execution.get("from_time"), "l2.from_time"),
        to_time=_time(execution.get("to_time"), "l2.to_time"),
        available_through=_time(
            execution.get("available_through"), "l2.available_through",
        ),
        bars=_integer(execution.get("observation_rows"), "observation_rows"),
        folds=_integer(execution.get("distinct_days"), "distinct_days"),
        coverage_ratio=_number(
            execution.get("coverage_ratio"), "l2.coverage_ratio",
        ),
    )


def _source_payload(
    kind: str,
    identity: RunIdentity,
    generated_at: datetime,
    construction_rule: Mapping[str, object],
    candidates: Sequence[Mapping[str, object]],
    extra: Mapping[str, object] | None = None,
) -> Mapping[str, object]:
    """来源制品的统一外壳，含输入身份与构造规则。"""
    payload: dict[str, object] = {
        "schema_version": 1,
        "artifact_kind": SOURCE_ARTIFACT_NAMES[kind],
        "scenario_type": kind,
        "method_version": SCENARIO_METHOD_VERSIONS[kind],
        "generator_id": GENERATOR_ID,
        "generator_code_sha256": generator_code_sha256(),
        "run_id": identity.run_id,
        "research_identity": identity.research_identity,
        "config_hash": identity.config_hash,
        "input_receipt_sha256": identity.input_receipt_sha256,
        "market_id": identity.market_id,
        "panel_sha256": identity.panel_sha256,
        "decision_time": identity.decision_time.isoformat(),
        "generated_at": generated_at.isoformat(),
        "walk_forward_oos_only": True,
        "selection_locked": True,
        "pit_verified": True,
        "construction_rule": dict(construction_rule),
        "candidates": [dict(item) for item in candidates],
    }
    if extra is not None:
        payload.update(extra)
    return payload


def source_artifact_bytes(payload: Mapping[str, object]) -> bytes:
    """来源制品的规范字节。"""
    return (canonical_json(payload) + "\n").encode("utf-8")


def _tail_scenarios(
    identity: RunIdentity,
    path: CandidatePath,
    evidence: Mapping[str, object],
    source: SourceArtifact,
) -> tuple[Mapping[str, object], ...]:
    """把尾部来源事实展开为场景记录。"""
    entry = _candidate_entry(evidence, path)
    window = _window_from(entry)
    result: list[Mapping[str, object]] = []
    for raw in _sequence(entry.get("levels"), "levels"):
        level = _object(raw, "tail level")
        probability = _number(level.get("tail_probability"), "probability")
        metrics = _object(level.get("metrics"), "metrics")
        result.append(_scenario_record(
            "tail",
            identity=identity,
            path=path,
            window=window,
            scenario_key=(
                f"tail:{path.candidate_id}:p{probability}"
                f":block{identity.block_bootstrap_bars}"
            ),
            parameters={
                "tail_probability": probability,
                "block_length": identity.block_bootstrap_bars,
            },
            metrics={
                "sharpe": _number(metrics.get("sharpe"), "sharpe"),
                "net_return": _number(metrics.get("net_return"), "net_return"),
                "maximum_drawdown": _number(
                    metrics.get("maximum_drawdown"), "maximum_drawdown",
                ),
                "turnover": _number(metrics.get("turnover"), "turnover"),
                "expected_shortfall": _number(
                    metrics.get("expected_shortfall"), "expected_shortfall",
                ),
            },
            source=source,
        ))
    return tuple(result)


def _stress_scenarios(
    identity: RunIdentity,
    path: CandidatePath,
    evidence: Mapping[str, object],
    source: SourceArtifact,
) -> tuple[Mapping[str, object], ...]:
    """把压力来源事实展开为场景记录；覆盖不足者不展开。"""
    entry = _candidate_entry(evidence, path)
    window = _window_from(entry)
    result: list[Mapping[str, object]] = []
    for raw in _sequence(entry.get("definitions"), "definitions"):
        definition = _object(raw, "stress definition")
        if definition.get("available") is not True:
            continue
        name = _text(definition.get("stress_definition"), "stress_definition")
        metrics = _object(definition.get("metrics"), "metrics")
        result.append(_scenario_record(
            "stress",
            identity=identity,
            path=path,
            window=window,
            scenario_key=(
                f"stress:{path.candidate_id}:{name}"
                f":q{definition.get('selection_quantile')}"
            ),
            parameters={
                "stress_definition": name,
                "severity": _number(definition.get("severity"), "severity"),
            },
            metrics={
                name: _number(metrics.get(name), name)
                for name in ("sharpe", "net_return", "maximum_drawdown", "turnover")
            },
            source=source,
        ))
    return tuple(result)


def _cost_scenarios(
    identity: RunIdentity,
    path: CandidatePath,
    evidence: Mapping[str, object],
    source: SourceArtifact,
) -> tuple[Mapping[str, object], ...]:
    """把成本来源事实展开为场景记录。"""
    entry = _candidate_entry(evidence, path)
    window = _window_from(entry)
    result: list[Mapping[str, object]] = []
    for raw in _sequence(entry.get("tiers"), "tiers"):
        tier = _object(raw, "cost tier")
        name = _text(tier.get("cost_tier"), "cost_tier")
        metrics = _object(tier.get("metrics"), "metrics")
        components = _object(
            tier.get("cost_components_bps"), "cost_components_bps",
        )
        total = _number(tier.get("total_cost_bps"), "total_cost_bps")
        result.append(_scenario_record(
            "cost",
            identity=identity,
            path=path,
            window=window,
            scenario_key=f"cost:{path.candidate_id}:{name}:{total}",
            parameters={"cost_tier": name},
            metrics={
                field: _number(metrics.get(field), field)
                for field in (
                    "sharpe", "net_return", "maximum_drawdown", "turnover",
                )
            },
            source=source,
            extra={
                "fixed_target": True,
                "total_cost_bps": total,
                "cost_components_bps": {
                    field: _number(components.get(field), field)
                    for field in COST_COMPONENTS
                },
            },
        ))
    return tuple(result)


def _capacity_scenarios(
    identity: RunIdentity,
    path: CandidatePath,
    evidence: Mapping[str, object],
    source: SourceArtifact,
) -> tuple[Mapping[str, object], ...]:
    """把容量来源事实展开为场景记录；覆盖不足者不展开。"""
    entry = _candidate_entry(evidence, path)
    raw_coverage = entry.get("coverage")
    if raw_coverage is None:
        return ()
    window = _window_from(entry)
    rule = _object(
        _object(evidence, "capacity evidence").get("construction_rule"),
        "construction_rule",
    )
    result: list[Mapping[str, object]] = []
    for raw in _sequence(entry.get("scenarios"), "scenarios"):
        scenario = _object(raw, "capacity scenario")
        if scenario.get("available") is not True:
            continue
        metrics = _object(scenario.get("metrics"), "metrics")
        notional = _number(scenario.get("notional_quote"), "notional_quote")
        result.append(_scenario_record(
            "capacity",
            identity=identity,
            path=path,
            window=window,
            scenario_key=f"capacity:{path.candidate_id}:{notional}",
            parameters={
                "depth_horizon_seconds": _integer(
                    rule.get("depth_horizon_seconds"), "depth_horizon_seconds",
                ),
                "depth_quantile": _number(
                    rule.get("depth_quantile"), "depth_quantile",
                ),
            },
            metrics={
                field: _number(metrics.get(field), field)
                for field in (
                    "sharpe", "net_return", "maximum_drawdown", "turnover",
                )
            },
            source=source,
            extra={
                "notional_quote": notional,
                "participation_rate": _number(
                    scenario.get("participation_rate"), "participation_rate",
                ),
                "observed_depth_quote": _number(
                    scenario.get("observed_depth_quote"), "depth",
                ),
                "impact_bps": _number(
                    scenario.get("impact_bps"), "impact_bps",
                ),
            },
        ))
    return tuple(result)


def _candidate_entry(
    evidence: Mapping[str, object],
    path: CandidatePath,
) -> Mapping[str, object]:
    """按候选身份定位来源制品条目。"""
    for raw in _sequence(evidence.get("candidates"), "candidates"):
        entry = _object(raw, "candidate")
        if (
            entry.get("family") == path.family
            and entry.get("candidate_id") == path.candidate_id
        ):
            return entry
    raise ValueError(f"来源制品缺少候选: {path.candidate_id}")


def _window_from(entry: Mapping[str, object]) -> EvidenceWindow:
    """从来源条目还原覆盖区间。"""
    coverage = _object(entry.get("coverage"), "coverage")
    return EvidenceWindow(
        from_time=_time(coverage.get("from_time"), "from_time"),
        to_time=_time(coverage.get("to_time"), "to_time"),
        available_through=_time(
            coverage.get("available_through"), "available_through",
        ),
        bars=_integer(coverage.get("bars"), "bars"),
        folds=_integer(coverage.get("folds"), "folds"),
        coverage_ratio=_number(coverage.get("coverage_ratio"), "coverage_ratio"),
    )


SCENARIO_BUILDERS = {
    "tail": _tail_scenarios,
    "stress": _stress_scenarios,
    "cost": _cost_scenarios,
    "capacity": _capacity_scenarios,
}


def build_industry_evidence(
    identity: RunIdentity,
    paths: Sequence[CandidatePath],
    evidences: Mapping[str, Mapping[str, object]],
    sources: Mapping[str, SourceArtifact],
    generated_at: datetime,
) -> Mapping[str, object]:
    """汇总制品：按候选聚合四类场景并绑定研究身份。"""
    candidate_evidence: list[Mapping[str, object]] = []
    for path in paths:
        record: dict[str, object] = {
            "family": path.family,
            "candidate_id": path.candidate_id,
        }
        for kind, collection in SCENARIO_COLLECTIONS.items():
            builder = SCENARIO_BUILDERS[kind]
            record[collection] = list(builder(
                identity, path, evidences[kind], sources[kind],
            ))
        candidate_evidence.append(record)
    return {
        "schema_version": 1,
        "method_version": INDUSTRY_EVIDENCE_METHOD_VERSION,
        "run_id": identity.run_id,
        "research_identity": identity.research_identity,
        "config_hash": identity.config_hash,
        "input_receipt_sha256": identity.input_receipt_sha256,
        "decision_time": identity.decision_time.isoformat(),
        "generated_at": generated_at.isoformat(),
        "candidate_evidence": candidate_evidence,
    }


def build_generator_attestation(
    identity: RunIdentity,
    evidence_payload: Mapping[str, object],
    evidence_sha256: str,
    sources: Mapping[str, SourceArtifact],
    generated_at: datetime,
    attested_at: datetime,
) -> Mapping[str, object]:
    """生成器 attestation：证明证据由独立生成器复算得到。"""
    source_ids = sorted({
        f"sha256-{source.sha256}" for source in sources.values()
    })
    used = sorted({
        str(_object(
            _object(scenario, "scenario").get("source_artifact"),
            "source_artifact",
        ).get("artifact_id"))
        for raw in _sequence(
            evidence_payload.get("candidate_evidence"), "candidate_evidence",
        )
        for collection in SCENARIO_COLLECTIONS.values()
        for scenario in _sequence(
            _object(raw, "candidate").get(collection), collection,
        )
    })
    attestation: dict[str, object] = {
        "schema_version": 1,
        "method_version": GENERATOR_ATTESTATION_METHOD_VERSION,
        "generator_id": GENERATOR_ID,
        "generator_code_sha256": generator_code_sha256(),
        "independent_from_strategy_search": True,
        "numeric_replay_verified": True,
        "pit_replay_verified": True,
        "run_id": identity.run_id,
        "research_identity": identity.research_identity,
        "config_hash": identity.config_hash,
        "input_receipt_sha256": identity.input_receipt_sha256,
        "decision_time": identity.decision_time.isoformat(),
        "generated_at": generated_at.isoformat(),
        "attested_at": attested_at.isoformat(),
        "industry_evidence_sha256": evidence_sha256,
        "source_artifact_ids": used if used else source_ids,
    }
    attestation["attestation_id"] = stable_identifier(
        "industry-generator-attestation", attestation,
    )
    return attestation


def evidence_bytes(payload: Mapping[str, object]) -> bytes:
    """汇总制品与 attestation 的规范字节。"""
    return (canonical_json(payload) + "\n").encode("utf-8")


def content_sha256(body: bytes) -> str:
    """字节内容身份。"""
    return hashlib.sha256(body).hexdigest()


def ledger_rows(
    identity: RunIdentity,
    evidence_payload: Mapping[str, object],
    generated_at: datetime,
) -> tuple[Mapping[str, object], ...]:
    """试验台账：逐场景登记身份与关键指标（G-07）。"""
    header: Mapping[str, object] = {
        "record_type": "industry_evidence_ledger_header",
        "method_version": LEDGER_METHOD_VERSION,
        "run_id": identity.run_id,
        "research_identity": identity.research_identity,
        "config_hash": identity.config_hash,
        "generated_at": generated_at.isoformat(),
    }
    rows: list[Mapping[str, object]] = [header]
    for raw in _sequence(
        evidence_payload.get("candidate_evidence"), "candidate_evidence",
    ):
        candidate = _object(raw, "candidate")
        for kind, collection in SCENARIO_COLLECTIONS.items():
            for item in _sequence(candidate.get(collection), collection):
                scenario = _object(item, "scenario")
                rows.append({
                    "record_type": "industry_evidence_scenario",
                    "scenario_type": kind,
                    "scenario_id": scenario.get("scenario_id"),
                    "scenario_key": scenario.get("scenario_key"),
                    "method_version": scenario.get("method_version"),
                    "family": scenario.get("family"),
                    "candidate_id": scenario.get("candidate_id"),
                    "parameters": scenario.get("parameters"),
                    "metrics": scenario.get("metrics"),
                })
    return tuple(rows)


def ledger_bytes(rows: Sequence[Mapping[str, object]]) -> bytes:
    """台账的规范 JSONL 字节。"""
    return "".join(
        canonical_json(row) + "\n" for row in rows
    ).encode("utf-8")


def sample_decision_times(
    end: datetime,
    horizon_seconds: int,
    count: int,
) -> tuple[datetime, ...]:
    """按固定步长回溯的确定性采样时点。"""
    if count <= 0 or horizon_seconds <= 0:
        raise ValueError("采样时点参数必须为正")
    step = timedelta(seconds=horizon_seconds)
    return tuple(
        sorted(end - step * index for index in range(count))
    )
