"""把搜索循环提案应用为新的研究配置文件版本，不自动运行研究。

新配置是一条新的谱系根：去掉 `evolution_parent`，以 `search_loop_source`
登记来源提案与父配置散列；候选预算由 `build_family_batches` 复核。
"""
from __future__ import annotations

import hashlib
import json
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

PROMOTE_METHOD_VERSION = "search-loop-promote-v1"
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
    new_config["search_loop_source"] = {
        "promote_method_version": PROMOTE_METHOD_VERSION,
        "proposal_path": _relative(proposal_path, root),
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
