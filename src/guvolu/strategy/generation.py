"""可独立运行的策略家族候选生成合同。"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from guvolu.strategy.baselines import (
    STRATEGY_METHOD_VERSION,
    SUPPORTED_FAMILIES,
    build_candidates,
)
from guvolu.strategy.contracts import CandidateSpec

GENERATOR_METHOD_VERSION = "scripted-family-grid-v2"


@dataclass(frozen=True)
class FamilyCandidateBatch:
    """一个流派独立生成的一批候选。"""

    family: str
    mode: str
    generator_method_version: str
    candidates: tuple[CandidateSpec, ...]


def build_family_batches(
    config: Mapping[str, object],
    family_scope: Sequence[str] | None = None,
) -> tuple[FamilyCandidateBatch, ...]:
    """生成指定流派的确定性候选批次。"""
    requested = (
        SUPPORTED_FAMILIES
        if family_scope is None
        else tuple(sorted(set(family_scope)))
    )
    candidates = build_candidates(config, requested)
    raw_features = config.get("features")
    if not isinstance(raw_features, Mapping):
        raise ValueError("features 必须为对象")
    raw_lookbacks = raw_features.get("lookbacks")
    if not isinstance(raw_lookbacks, list):
        raise ValueError("features.lookbacks 必须为数组")
    feature_lookbacks = {int(value) for value in raw_lookbacks}
    missing_lookbacks = sorted({
        int(candidate.parameters["lookback"])
        for candidate in candidates
        if "lookback" in candidate.parameters
        and int(candidate.parameters["lookback"]) not in feature_lookbacks
    })
    if missing_lookbacks:
        raise ValueError(
            "策略回看窗缺少共享特征: "
            + ",".join(str(value) for value in missing_lookbacks)
        )
    batches: list[FamilyCandidateBatch] = []
    for family in requested:
        family_candidates = tuple(
            candidate for candidate in candidates if candidate.family == family
        )
        modes = {candidate.mode for candidate in family_candidates}
        if len(modes) != 1:
            raise ValueError(f"策略家族模式不唯一: {family}")
        batches.append(FamilyCandidateBatch(
            family=family,
            mode=next(iter(modes)),
            generator_method_version=GENERATOR_METHOD_VERSION,
            candidates=family_candidates,
        ))
    return tuple(batches)


def candidate_registry_payload(
    batches: Sequence[FamilyCandidateBatch],
    config_hash: str,
) -> Mapping[str, object]:
    """生成可持久化候选注册表。"""
    return {
        "schema_version": 1,
        "generator_method_version": GENERATOR_METHOD_VERSION,
        "strategy_method_version": STRATEGY_METHOD_VERSION,
        "config_hash": config_hash,
        "family_scope": [batch.family for batch in batches],
        "candidate_count": sum(len(batch.candidates) for batch in batches),
        "families": [{
            "family": batch.family,
            "mode": batch.mode,
            "candidate_count": len(batch.candidates),
            "candidate_ids": [
                candidate.candidate_id for candidate in batch.candidates
            ],
        } for batch in batches],
        "candidates": [{
            "candidate_id": candidate.candidate_id,
            "family": candidate.family,
            "mode": candidate.mode,
            "parameters": dict(candidate.parameters),
            "complexity": candidate.complexity,
        } for batch in batches for candidate in batch.candidates],
    }
