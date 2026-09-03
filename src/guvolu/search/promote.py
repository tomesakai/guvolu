"""把搜索循环提案应用为新的研究配置文件版本，不自动运行研究。

新配置是一条新的谱系根：去掉 `evolution_parent`，以 `search_loop_source`
登记来源提案与父配置散列；候选预算由 `build_family_batches` 复核。

提升同时把搜索阶段的家族试验证据写入 `search_loop_source.family_trials`
（评估数、粗筛通过数、折级得分相关性有效试验数、年化 Sharpe 离散度与
台账散列），供研究验证把搜索试验计入 DSR；并在 `validation` 声明部署
候选取折冠军众数与固定多头基准超额门（准入扩展）。
"""
from __future__ import annotations

import hashlib
import json
import math
import statistics
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from guvolu.data.durable_io import atomic_write_text
from guvolu.research.config_lineage import (
    load_governed_strategy_config,
    load_verified_config_lineage,
)
from guvolu.search.proposal import PROPOSAL_METHOD_VERSION, STATUS_PROPOSED
from guvolu.strategy.generation import build_family_batches

PROMOTE_METHOD_VERSION = "search-loop-promote-v2"
# 提升配置缺省的准入扩展
PROMOTED_DEPLOYMENT_RULE = "most_selected_fold_champion"
PROMOTED_BENCHMARK_SHARPE_EXCESS = 0.05
CANDIDATE_CONFIG_PREFIX = "strategy_research_candidate_"


def _object(value: object, name: str) -> Mapping[str, object]:
    """验证对象。"""
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} 必须为对象")
    return {str(key): item for key, item in value.items()}


def _text(value: object, name: str) -> str:
    """验证非空文本。"""
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} 必须为非空文本")
    return value


@dataclass(frozen=True)
class PromotionResult:
    """提案应用结果。"""

    config: Mapping[str, object]
    applied_families: tuple[str, ...]
    skipped_families: Mapping[str, str]
    parent_config_path: str
    parent_config_sha256: str
    proposal_sha256: str


def effective_trial_count_from_fold_scores(
    vectors: Sequence[Sequence[float]],
) -> float:
    """折级得分相关矩阵参与率的有效试验数（Gram 等价算法）。

    与 `research.validation._effective_trial_count` 同义：N 平方除以
    相关系数平方和。逐对计算为 N 平方乘折数，改用标准化矩阵的
    折数阶 Gram 矩阵，其 Frobenius 范数平方即相关系数平方和；常量
    向量之间按完全相同记相关一，与其它向量记零。
    """
    count = len(vectors)
    if count <= 1:
        return float(count)
    lengths = {len(vector) for vector in vectors}
    if len(lengths) != 1 or next(iter(lengths)) < 2:
        raise ValueError("有效试验数需要同长的至少两个折级得分")
    length = next(iter(lengths))
    gram = [[0.0] * length for _ in range(length)]
    constant_groups: dict[tuple[float, ...], int] = {}
    for vector in vectors:
        mean = sum(vector) / length
        centered = [value - mean for value in vector]
        norm = math.sqrt(sum(value * value for value in centered))
        if norm <= 0.0:
            key = tuple(float(value) for value in vector)
            constant_groups[key] = constant_groups.get(key, 0) + 1
            continue
        unit = [value / norm for value in centered]
        for row_index, row_value in enumerate(unit):
            if row_value == 0.0:
                continue
            row = gram[row_index]
            for column_index, column_value in enumerate(unit):
                row[column_index] += row_value * column_value
    squared_sum = sum(value * value for row in gram for value in row)
    squared_sum += sum(size * size for size in constant_groups.values())
    return min(max(count * count / squared_sum, 1.0), float(count))


def family_trial_evidence(
    root: Path,
    proposal_path: Path,
    proposal: Mapping[str, object],
    families: Sequence[str],
) -> dict[str, dict[str, object]]:
    """从搜索结果台账汇总各流派的试验证据。"""
    result_id = _text(proposal.get("search_result_id"), "search_result_id")
    ledgers = sorted((proposal_path.parent / result_id).glob("trial-ledger-*.jsonl"))
    if len(ledgers) != 1:
        raise ValueError("搜索结果台账必须恰有一份")
    ledger = ledgers[0]
    raw = ledger.read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    evaluated: dict[str, int] = {family: 0 for family in families}
    passed: dict[str, int] = {family: 0 for family in families}
    vectors: dict[str, list[list[float]]] = {family: [] for family in families}
    sharpes: dict[str, list[float]] = {family: [] for family in families}
    for line in raw.decode("utf-8").splitlines():
        if not line.strip():
            continue
        row = _object(json.loads(line), "trial")
        if row.get("record_type") != "search_trial":
            continue
        # 结构 challenger 计入父流派
        family = str(row.get("family")).split("~", 1)[0]
        if family not in evaluated:
            continue
        evaluated[family] += 1
        passed[family] += 1 if row.get("screen_passed") else 0
        metrics = row.get("metrics")
        if isinstance(metrics, Mapping) and isinstance(
            metrics.get("sharpe"), (int, float),
        ):
            sharpes[family].append(float(metrics["sharpe"]))
        resample = row.get("resample")
        if isinstance(resample, Mapping):
            folds = resample.get("fold_test_sharpe")
            if isinstance(folds, list) and len(folds) >= 2:
                vectors[family].append([float(value) for value in folds])
    proposal_families = proposal.get("families")
    evidence: dict[str, dict[str, object]] = {}
    for family in families:
        if evaluated[family] <= 0:
            raise ValueError(f"搜索台账没有流派试验: {family}")
        deviation = (
            statistics.pstdev(sharpes[family])
            if len(sharpes[family]) > 1 else 0.0
        )
        effective = (
            effective_trial_count_from_fold_scores(vectors[family])
            if vectors[family] else 1.0
        )
        method = "fold-score-correlation-participation-v1"
        # 搜索循环给出收益相关性有效数时优先采用
        family_item = (
            proposal_families.get(family)
            if isinstance(proposal_families, Mapping) else None
        )
        summary = (
            family_item.get("summary") if isinstance(family_item, Mapping)
            else None
        )
        returns_evidence = (
            summary.get("trial_evidence") if isinstance(summary, Mapping)
            else None
        )
        if isinstance(returns_evidence, Mapping):
            candidate = returns_evidence.get("effective_trial_count")
            if isinstance(candidate, (int, float)) and 1.0 <= float(candidate):
                effective = min(float(candidate), float(evaluated[family]))
                method = str(returns_evidence.get("method_version"))
            oos_std = returns_evidence.get("oos_sharpe_std")
            if isinstance(oos_std, (int, float)) and float(oos_std) >= 0.0:
                # 样本外离散度才是被去膨胀的统计量
                deviation = float(oos_std)
        evidence[family] = {
            "evaluated": evaluated[family],
            "screen_passed": passed[family],
            "effective_trial_count": effective,
            "effective_trial_method_version": method,
            "annual_sharpe_std": deviation,
            "ledger_sha256": digest,
            "ledger_path": _relative(ledger, root),
        }
    return evidence


def load_proposal(path: Path) -> tuple[Mapping[str, object], str]:
    """读取提案并返回其内容散列。"""
    raw = path.read_bytes()
    proposal = _object(json.loads(raw.decode("utf-8")), "proposal")
    if proposal.get("proposal_method_version") != PROPOSAL_METHOD_VERSION:
        raise ValueError("提案方法版本不受支持")
    return proposal, hashlib.sha256(raw).hexdigest()


def promoted_config(
    root: Path,
    proposal_path: Path,
    families: Sequence[str] | None = None,
) -> PromotionResult:
    """按提案生成新研究配置；只采纳状态为 proposed 的流派。"""
    root = root.resolve()
    proposal, proposal_sha256 = load_proposal(proposal_path)
    parent = _object(proposal.get("parent_research_config"), "parent_research_config")
    parent_relative = _text(parent.get("path"), "parent_research_config.path")
    parent_path = (root / parent_relative).resolve()
    try:
        parent_path.relative_to(root)
    except ValueError as error:
        raise ValueError("父配置路径越出项目目录") from error
    config, config_hash, _root_hash, _depth = load_verified_config_lineage(
        root, parent_path,
    )
    if config_hash != parent.get("sha256"):
        raise ValueError("父配置散列与提案登记不一致，提案已过期")
    proposals = _object(proposal.get("families"), "families")
    requested = None if families is None else set(families)
    applied: list[str] = []
    skipped: dict[str, str] = {}
    new_config = json.loads(json.dumps(config))
    strategies = _object(new_config.get("strategies"), "strategies")
    for family, raw_item in sorted(proposals.items()):
        item = _object(raw_item, f"families.{family}")
        if requested is not None and family not in requested:
            skipped[family] = "not_requested"
            continue
        if item.get("status") != STATUS_PROPOSED:
            skipped[family] = str(item.get("status"))
            continue
        proposed = _object(item.get("proposed_strategy"), f"families.{family}.proposed_strategy")
        if family not in strategies:
            raise ValueError(f"父配置缺少流派: {family}")
        new_config["strategies"][family] = dict(proposed)
        applied.append(family)
    if requested is not None:
        unknown = sorted(requested - set(proposals))
        if unknown:
            raise ValueError("提案不含流派: " + ",".join(unknown))
    if not applied:
        raise ValueError("提案没有可采纳的流派")
    features = _object(new_config.get("features"), "features")
    lookbacks: set[int] = set()
    raw_lookbacks = features.get("lookbacks")
    if isinstance(raw_lookbacks, list):
        lookbacks.update(int(value) for value in raw_lookbacks)
    for family_strategy in _object(new_config.get("strategies"), "strategies").values():
        strategy_lookbacks = _object(family_strategy, "strategy").get("lookbacks")
        if isinstance(strategy_lookbacks, list):
            lookbacks.update(int(value) for value in strategy_lookbacks)
    new_config["features"]["lookbacks"] = sorted(lookbacks)
    new_config.pop("evolution_parent", None)
    try:
        proposal_path.resolve().relative_to(root)
    except ValueError as error:
        raise ValueError("提案路径越出项目目录，来源不可复核") from error
    validation = dict(_object(new_config.get("validation"), "validation"))
    validation.setdefault("deployment_candidate_rule", PROMOTED_DEPLOYMENT_RULE)
    validation.setdefault(
        "minimum_benchmark_sharpe_excess", PROMOTED_BENCHMARK_SHARPE_EXCESS,
    )
    new_config["validation"] = validation
    new_config["search_loop_source"] = {
        "promote_method_version": PROMOTE_METHOD_VERSION,
        "proposal_path": _relative(proposal_path, root),
        "family_trials": family_trial_evidence(
            root, proposal_path.resolve(), proposal, applied,
        ),
        "proposal_sha256": proposal_sha256,
        "search_run_id": proposal.get("search_run_id"),
        "bundle_id": proposal.get("bundle_id"),
        "search_result_id": proposal.get("search_result_id"),
        "parent_config_path": parent_relative,
        "parent_config_sha256": config_hash,
        "applied_families": list(applied),
        "holdout_consumed": False,
    }
    build_family_batches(new_config)
    return PromotionResult(
        config=new_config,
        applied_families=tuple(applied),
        skipped_families=skipped,
        parent_config_path=parent_relative,
        parent_config_sha256=config_hash,
        proposal_sha256=proposal_sha256,
    )


def write_promoted_config(
    root: Path,
    result: PromotionResult,
    output_directory: Path | None = None,
) -> Path:
    """以内容散列短名写出新配置，并复核其谱系可加载。"""
    root = root.resolve()
    directory = (output_directory or root / "config").resolve()
    try:
        directory.relative_to(root)
    except ValueError as error:
        raise ValueError("配置输出目录越出项目目录") from error
    directory.mkdir(parents=True, exist_ok=True)
    content = json.dumps(result.config, ensure_ascii=False, indent=2) + "\n"
    digest = hashlib.sha256(content.encode("utf-8")).hexdigest()
    path = directory / f"{CANDIDATE_CONFIG_PREFIX}{digest[:12]}.json"
    if path.exists() and path.read_text(encoding="utf-8") != content:
        raise FileExistsError(f"候选配置已存在且内容不同: {path}")
    atomic_write_text(path, content)
    load_governed_strategy_config(root, path)
    return path


def research_command(root: Path, config_path: Path) -> str:
    """打印可直接运行的研究命令。"""
    return (
        "python scripts/run_strategy_research.py --config "
        + _relative(config_path, root)
    )


def _relative(path: Path, root: Path) -> str:
    """尽量以项目相对路径表示。"""
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return path.as_posix()
