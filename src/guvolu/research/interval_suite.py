"""多决策节拍研究的预登记试验域。"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from pathlib import Path

from guvolu.research.config_lineage import (
    load_governed_strategy_config_with_paths,
)
from guvolu.research.provenance import canonical_json, stable_identifier
from guvolu.strategy.generation import (
    build_family_batches,
    candidate_registry_payload,
)

INTERVAL_SUITE_METHOD_VERSION = "pre-registered-multi-interval-suite-v2"
_INTERVAL_SECONDS = {
    "5min": 300,
    "15min": 900,
    "1hour": 3_600,
    "4hour": 14_400,
}
LoadedIntervalConfig = tuple[
    Mapping[str, object], str, str, int, tuple[Path, ...],
]


def _mapping(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} 必须为对象")
    return {str(key): item for key, item in value.items()}


def _text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} 必须为非空文本")
    return value.strip()


def _positive_integer(value: object, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{name} 必须为正整数")
    return value


def _duration_contract(
    config: Mapping[str, object],
    interval_seconds: int,
) -> Mapping[str, object]:
    """把按柱配置转换为可跨节拍比较的墙钟时长。"""
    features = _mapping(config.get("features"), "features")
    raw_lookbacks = features.get("lookbacks")
    if not isinstance(raw_lookbacks, list) or not raw_lookbacks:
        raise ValueError("features.lookbacks 必须为非空数组")
    lookbacks = sorted({
        _positive_integer(value, "features.lookbacks") * interval_seconds
        for value in raw_lookbacks
    })
    state_lookback = _positive_integer(
        features.get("state_lookback"), "features.state_lookback",
    )
    volume_lookback = _positive_integer(
        features.get("volume_lookback"), "features.volume_lookback",
    )
    maximum_gap = _positive_integer(
        features.get("maximum_structural_gap_bars_assumption"),
        "features.maximum_structural_gap_bars_assumption",
    )
    walk_forward = _mapping(config.get("walk_forward"), "walk_forward")
    validation = dict(_mapping(config.get("validation"), "validation"))
    validation["minimum_oos_seconds"] = _positive_integer(
        validation.pop("minimum_oos_bars"), "validation.minimum_oos_bars",
    ) * interval_seconds
    validation["block_bootstrap_seconds"] = _positive_integer(
        validation.pop("block_bootstrap_bars"),
        "validation.block_bootstrap_bars",
    ) * interval_seconds
    governance = _mapping(config.get("data_governance"), "data_governance")
    holdout = dict(_mapping(
        governance.get("holdout_policy"), "data_governance.holdout_policy",
    ))
    holdout["minimum_seconds"] = _positive_integer(
        holdout.pop("minimum_bars"), "holdout_policy.minimum_bars",
    ) * interval_seconds
    strategies: dict[str, object] = {}
    for family, raw in sorted(
        _mapping(config.get("strategies"), "strategies").items(),
    ):
        strategy = dict(_mapping(raw, f"strategies.{family}"))
        raw_strategy_lookbacks = strategy.pop("lookbacks", None)
        if not isinstance(raw_strategy_lookbacks, list) or not raw_strategy_lookbacks:
            raise ValueError(f"strategies.{family}.lookbacks 必须为非空数组")
        strategy["lookback_seconds"] = sorted({
            _positive_integer(
                value, f"strategies.{family}.lookbacks",
            ) * interval_seconds
            for value in raw_strategy_lookbacks
        })
        strategies[family] = strategy
    return {
        "feature_lookback_seconds": lookbacks,
        "state_lookback_seconds": state_lookback * interval_seconds,
        "volume_lookback_seconds": volume_lookback * interval_seconds,
        "maximum_structural_gap_seconds": maximum_gap * interval_seconds,
        "minimum_train_seconds": _positive_integer(
            walk_forward.get("minimum_train_bars"), "minimum_train_bars",
        ) * interval_seconds,
        "test_seconds": _positive_integer(
            walk_forward.get("test_bars"), "test_bars",
        ) * interval_seconds,
        "step_seconds": _positive_integer(
            walk_forward.get("step_bars"), "step_bars",
        ) * interval_seconds,
        "embargo_seconds": _positive_integer(
            walk_forward.get("embargo_bars"), "embargo_bars",
        ) * interval_seconds,
        "validation": validation,
        "holdout_policy": holdout,
        "strategies": strategies,
    }


def build_interval_suite_plan(
    repository_root: Path,
    config_paths: Sequence[Path],
    *,
    loaded_configs: Mapping[Path, LoadedIntervalConfig] | None = None,
) -> Mapping[str, object]:
    """生成跨节拍统一身份和全局多重检验域，不执行回测。"""
    root = repository_root.resolve()
    if len(config_paths) < 2:
        raise ValueError("多节拍套件至少需要两个配置")
    members: list[Mapping[str, object]] = []
    trial_domain: list[Mapping[str, object]] = []
    intervals: set[str] = set()
    market_id: str | None = None
    from_time: str | None = None
    data_scope: str | None = None
    duration_contract: Mapping[str, object] | None = None
    allocation_contract: Mapping[str, object] | None = None
    for raw_path in config_paths:
        path = (
            raw_path.resolve()
            if raw_path.is_absolute()
            else (root / raw_path).resolve()
        )
        if loaded_configs is None:
            loaded = load_governed_strategy_config_with_paths(root, path)
        else:
            preloaded = loaded_configs.get(path)
            if preloaded is None:
                raise ValueError(f"预加载套件配置未覆盖路径: {path}")
            loaded = preloaded
        config, config_hash, root_hash, depth, source_paths = loaded
        interval = _text(config.get("bar_interval"), "bar_interval")
        seconds = _INTERVAL_SECONDS.get(interval)
        if seconds is None:
            raise ValueError(f"多节拍套件不支持 bar_interval: {interval}")
        if interval in intervals:
            raise ValueError(f"多节拍套件包含重复节拍: {interval}")
        intervals.add(interval)
        current_market = _text(config.get("market_id"), "market_id")
        if market_id is None:
            market_id = current_market
        elif current_market != market_id:
            raise ValueError("多节拍套件只能比较同一 market_id")
        current_from_time = _text(config.get("from_time"), "from_time")
        if from_time is None:
            from_time = current_from_time
        elif current_from_time != from_time:
            raise ValueError("多节拍套件必须共享同一 from_time")
        governance = _mapping(config.get("data_governance"), "data_governance")
        current_scope = _text(governance.get("scope"), "data_governance.scope")
        if data_scope is None:
            data_scope = current_scope
        elif current_scope != data_scope:
            raise ValueError("多节拍套件必须共享同一数据治理 scope")
        current_duration = _duration_contract(config, seconds)
        if duration_contract is None:
            duration_contract = current_duration
        elif current_duration != duration_contract:
            raise ValueError("多节拍配置的墙钟回看或 walk-forward 合同不一致")
        current_allocation = _mapping(config.get("allocation"), "allocation")
        if allocation_contract is None:
            allocation_contract = current_allocation
        elif current_allocation != allocation_contract:
            raise ValueError("多节拍配置必须共享同一 allocation 合同")
        batches = build_family_batches(config)
        registry = candidate_registry_payload(batches, config_hash)
        search_plan = _mapping(registry.get("search_plan"), "search_plan")
        member_id = stable_identifier("interval-member", {
            "market_id": current_market,
            "bar_interval": interval,
            "config_hash": config_hash,
            "search_plan_id": search_plan.get("search_plan_id"),
        })
        family_trials: list[Mapping[str, object]] = []
        for batch in batches:
            candidate_ids = sorted(
                candidate.candidate_id for candidate in batch.candidates
            )
            family_trial_id = stable_identifier("interval-family-trial", {
                "member_id": member_id,
                "family": batch.family,
                "candidate_ids": candidate_ids,
            })
            family_trials.append({
                "trial_id": family_trial_id,
                "family": batch.family,
                "candidate_ids": candidate_ids,
            })
            trial_domain.append({
                "trial_id": family_trial_id,
                "member_id": member_id,
                "bar_interval": interval,
                "family": batch.family,
                "role": "walk_forward_family_path",
            })
            for candidate_id in candidate_ids:
                trial_domain.append({
                    "trial_id": stable_identifier("interval-candidate-trial", {
                        "member_id": member_id,
                        "candidate_id": candidate_id,
                    }),
                    "member_id": member_id,
                    "bar_interval": interval,
                    "family": batch.family,
                    "candidate_id": candidate_id,
                    "role": "candidate_oos_path",
                })
        members.append({
            "member_id": member_id,
            "bar_interval": interval,
            "interval_seconds": seconds,
            "config_path": path.relative_to(root).as_posix(),
            "config_hash": config_hash,
            "config_lineage_root_hash": root_hash,
            "config_lineage_depth": depth,
            "config_source_paths": [
                item.relative_to(root).as_posix() for item in source_paths
            ],
            "search_plan_id": search_plan.get("search_plan_id"),
            "candidate_registry_sha256": hashlib.sha256(
                (canonical_json(registry) + "\n").encode("utf-8"),
            ).hexdigest(),
            "candidate_count": registry.get("candidate_count"),
            "family_trials": family_trials,
        })
    ordered_members = sorted(
        members,
        key=lambda item: _positive_integer(
            item.get("interval_seconds"), "interval_seconds",
        ),
    )
    ordered_trials = sorted(trial_domain, key=lambda item: str(item["trial_id"]))
    if len({str(item["trial_id"]) for item in ordered_trials}) != len(
        ordered_trials
    ):
        raise ValueError("多节拍全局试验域存在身份冲突")
    body: dict[str, object] = {
        "schema_version": 1,
        "method_version": INTERVAL_SUITE_METHOD_VERSION,
        "market_id": market_id,
        "from_time": from_time,
        "data_scope": data_scope,
        "duration_contract": duration_contract,
        "allocation_contract": allocation_contract,
        "members": ordered_members,
        "global_multiple_testing_domain": ordered_trials,
    }
    return {
        **body,
        "suite_plan_id": stable_identifier("interval-suite-plan", body),
    }


def interval_suite_plan_text(plan: Mapping[str, object]) -> str:
    """返回可直接持久化的规范 JSON。"""
    return json.dumps(
        plan,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ) + "\n"
