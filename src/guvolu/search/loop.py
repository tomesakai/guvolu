"""策略生成迭代循环 v1：候选生成、GPU 宽筛、CPU 复算、台账与受约束提案。

循环只产出提案与制品，不改写 config/strategy_research.json，不写 SQLite 与
生产数据目录；研究准入（walk-forward、FDR、PBO、DSR、bootstrap、邻域）仍由
`run_strategy_research.py` 在 CPU 完成。进程不 import api 与 ops（G-01）。
"""
from __future__ import annotations

import json
import math
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from guvolu.data.durable_io import atomic_write_text
from guvolu.research.config_lineage import load_governed_strategy_config
from guvolu.research.provenance import code_identity
from guvolu.research.validation import SECONDS_PER_YEAR
from guvolu.search.bundle import SearchBundle, build_search_bundle, write_search_bundle
from guvolu.search.identity import canonical_json, sha256_text
from guvolu.search.ledger import read_ledger
from guvolu.search.panel_io import panel_payload, write_panel_payload
from guvolu.search.panel_source import ResearchPanel, load_research_panel, panel_from_bars
from guvolu.search.parity import ParityTolerance, tolerance_from_config
from guvolu.search.proposal import ARRAY_AXES, ProposalThresholds, build_proposal
from guvolu.search.resample import (
    ResampleScreen,
    ResampleSpec,
    ResampleTolerance,
    resample_screen_from_config,
    resample_spec_from_config,
    resample_tolerance_from_config,
)
from guvolu.search.runner import (
    EvaluationOptions,
    ScreenConfig,
    evaluate_bundle,
    load_search_result,
    run_parity,
    screen_from_config,
)
from guvolu.search.synthetic import SYNTHETIC_METHOD_VERSION, synthetic_panel
from guvolu.search.tensorize import tensorize_panel
from guvolu.search.torch_runtime import resolve_device, runtime_identity
from guvolu.strategy.baselines import SUPPORTED_FAMILIES
from guvolu.strategy.contracts import CandidateSpec
from guvolu.strategy.expression import (
    StrategyExpression,
    Unit,
    candidate_identity,
    expression_id,
    strategy_expression,
    strategy_expression_payload,
)
from guvolu.strategy.generation import (
    GENERATOR_METHOD_VERSION,
    FamilyCandidateBatch,
    SearchPlanEntry,
    build_family_batches,
    candidate_search_plan_payload,
    search_plan_payload,
)
from guvolu.strategy.mutation import (
    MUTATION_OPERATORS,
    StructuralChallenger,
    bounded_typed_crossovers,
    bounded_typed_mutations,
    structural_challenger_registry_payload,
)

LOOP_METHOD_VERSION = "search-loop-v1"
LOOP_SCHEMA_VERSION = 1
SOURCE_REGISTERED_GRID = "registered_grid"
SOURCE_NEIGHBORHOOD = "neighborhood_grid"
SOURCE_STRUCTURAL = "structural_challenger"
CHALLENGER_LABEL_SEPARATOR = "~"
_INTERVAL_SECONDS = {
    "5min": 300.0,
    "15min": 900.0,
    "1hour": 3600.0,
    "4hour": 14_400.0,
}


def _object(value: object, name: str) -> Mapping[str, object]:
    """验证配置对象。"""
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} 必须为对象")
    return {str(key): item for key, item in value.items()}


def _integer(value: object, name: str) -> int:
    """验证正整数。"""
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{name} 必须为正整数")
    return value


def _number(value: object, name: str) -> float:
    """验证有限数值。"""
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(float(value))
    ):
        raise ValueError(f"{name} 必须为有限数值")
    return float(value)


def _text(value: object, name: str) -> str:
    """验证非空文本。"""
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} 必须为非空文本")
    return value


def axis_values(spec: object, name: str, integral: bool) -> tuple[int | float, ...]:
    """把邻域轴配置（数组或 minimum/maximum/step）展开为有序取值。"""
    if isinstance(spec, list):
        values = [_number(item, name) for item in spec]
    elif isinstance(spec, Mapping):
        minimum = _number(spec.get("minimum"), f"{name}.minimum")
        maximum = _number(spec.get("maximum"), f"{name}.maximum")
        step = _number(spec.get("step"), f"{name}.step")
        if step <= 0 or maximum < minimum:
            raise ValueError(f"{name} 的 step 必须为正且 maximum 不小于 minimum")
        count = int(math.floor((maximum - minimum) / step + 1e-9)) + 1
        values = [minimum + step * index for index in range(count)]
    else:
        raise ValueError(f"{name} 必须为数组或区间对象")
    if not values:
        raise ValueError(f"{name} 不得为空")
    if integral:
        rounded = sorted({int(round(value)) for value in values})
        if any(value <= 0 for value in rounded):
            raise ValueError(f"{name} 必须为正整数")
        return tuple(rounded)
    return tuple(sorted({float(value) for value in values}))


@dataclass(frozen=True)
class LoopConfig:
    """循环配置（G-06：预算、网格、阈值与容差全部为配置）。"""

    path: Path
    raw: Mapping[str, object]
    research_config_path: Path
    output_root: Path
    panel_to_time: str
    family_scope: tuple[str, ...]
    candidate_budget_per_family: int
    structural_budget_per_family: int
    structural_limit_per_family: int
    structural_operators: tuple[str, ...]
    structural_donors: Mapping[str, tuple[str, ...]]
    neighborhood: Mapping[str, Mapping[str, object]]
    device: str
    candidate_chunk: int
    scan_method: str
    seed: int
    screen: ScreenConfig
    resample_screen: ResampleScreen
    parity_tolerance: ParityTolerance
    resample_tolerance: ResampleTolerance
    parity_candidate_limit: int | None
    proposal: ProposalThresholds
    write_reference_panel: bool
    synthetic: Mapping[str, object]

    def config_sha256(self) -> str:
        """循环配置文件散列。"""
        return sha256_text(self.path.read_text(encoding="utf-8"))


def load_loop_config(root: Path, path: Path) -> LoopConfig:
    """读取并验证循环配置。"""
    raw = _object(json.loads(path.read_text(encoding="utf-8")), "search_loop")
    if raw.get("schema_version") != LOOP_SCHEMA_VERSION:
        raise ValueError("search_loop.schema_version 不受支持")
    research_path = Path(_text(raw.get("research_config"), "research_config"))
    if not research_path.is_absolute():
        research_path = root / research_path
    output_root = Path(_text(raw.get("output_root"), "output_root"))
    if not output_root.is_absolute():
        output_root = root / output_root
    families = raw.get("family_scope")
    if not isinstance(families, list) or not families:
        raise ValueError("family_scope 必须为非空数组")
    family_scope = tuple(sorted({_text(item, "family_scope") for item in families}))
    unknown = sorted(set(family_scope) - set(SUPPORTED_FAMILIES))
    if unknown:
        raise ValueError("family_scope 含未知流派: " + ",".join(unknown))
    structural = _object(raw.get("structural_challengers", {}), "structural_challengers")
    operators_raw = structural.get("operators", list(MUTATION_OPERATORS))
    if not isinstance(operators_raw, list):
        raise ValueError("structural_challengers.operators 必须为数组")
    operators = tuple(sorted({_text(item, "operator") for item in operators_raw}))
    unknown_operators = sorted(set(operators) - set(MUTATION_OPERATORS))
    if unknown_operators:
        raise ValueError("未知结构变异算子: " + ",".join(unknown_operators))
    donors_raw = _object(structural.get("donors", {}), "structural_challengers.donors")
    donors = {
        family: tuple(sorted({_text(item, "donor") for item in _list(value, family)}))
        for family, value in donors_raw.items()
    }
    gpu = _object(raw.get("gpu", {}), "gpu")
    proposal = _object(raw.get("proposal", {}), "proposal")
    parity_limit = raw.get("parity_candidate_limit")
    if parity_limit is not None:
        parity_limit = _integer(parity_limit, "parity_candidate_limit")
    return LoopConfig(
        path=path,
        raw=raw,
        research_config_path=research_path,
        output_root=output_root,
        panel_to_time=_text(raw.get("panel_to_time"), "panel_to_time"),
        family_scope=family_scope,
        candidate_budget_per_family=_integer(
            raw.get("candidate_budget_per_family"), "candidate_budget_per_family",
        ),
        structural_budget_per_family=_integer(
            structural.get("projected_budget_per_family", 1),
            "structural_challengers.projected_budget_per_family",
        ),
        structural_limit_per_family=int(_number(
            structural.get("limit_per_family", 0),
            "structural_challengers.limit_per_family",
        )),
        structural_operators=operators,
        structural_donors=donors,
        neighborhood={
            family: _object(value, f"neighborhood.{family}")
            for family, value in _object(raw.get("neighborhood", {}), "neighborhood").items()
        },
        device=str(gpu.get("device", "auto")),
        candidate_chunk=_integer(gpu.get("candidate_chunk", 512), "gpu.candidate_chunk"),
        scan_method=str(gpu.get("scan_method", "parallel")),
        seed=int(_number(raw.get("seed", 0), "seed")),
        screen=screen_from_config(_optional_object(raw.get("screen"), "screen")),
        resample_screen=resample_screen_from_config(
            _optional_object(raw.get("resample_screen"), "resample_screen"),
        ),
        parity_tolerance=tolerance_from_config(
            _optional_object(raw.get("parity_tolerance"), "parity_tolerance"),
        ),
        resample_tolerance=resample_tolerance_from_config(
            _optional_object(raw.get("resample_tolerance"), "resample_tolerance"),
        ),
        parity_candidate_limit=parity_limit,
        proposal=ProposalThresholds.from_config(proposal),
        write_reference_panel=bool(raw.get("write_reference_panel", False)),
        synthetic=_object(raw.get("synthetic", {}), "synthetic"),
    )


def _list(value: object, name: str) -> list[object]:
    """验证数组。"""
    if not isinstance(value, list):
        raise ValueError(f"{name} 必须为数组")
    return list(value)


def _optional_object(value: object, name: str) -> Mapping[str, object] | None:
    """读取可选对象。"""
    return None if value is None else _object(value, name)


@dataclass(frozen=True)
class LoopCandidates:
    """循环候选集合：注册网格、邻域网格、结构 challenger 与搜索计划。"""

    batches: tuple[FamilyCandidateBatch, ...]
    candidates: Mapping[str, CandidateSpec]
    sources: Mapping[str, str]
    labels: Mapping[str, str]
    templates: Mapping[str, StrategyExpression]
    challengers: Mapping[str, tuple[StructuralChallenger, ...]]
    challenger_registries: Mapping[str, Mapping[str, object]]
    plan: Mapping[str, object]
    lookbacks: tuple[int, ...]
    budgets: Mapping[str, Mapping[str, int]]

    def registry_payload(self) -> Mapping[str, object]:
        """导出候选登记（含来源与预算）。"""
        return {
            "schema_version": 1,
            "loop_method_version": LOOP_METHOD_VERSION,
            "generator_method_version": GENERATOR_METHOD_VERSION,
            "search_plan_id": self.plan["search_plan_id"],
            "candidate_count": len(self.candidates),
            "budgets": {family: dict(item) for family, item in self.budgets.items()},
            "labels": {
                label: {
                    "family": template.family,
                    "expression_id": expression_id(template),
                    "structural": CHALLENGER_LABEL_SEPARATOR in label,
                }
                for label, template in sorted(self.templates.items())
            },
            "candidates": [
                {
                    "candidate_id": candidate_id,
                    "label": self.labels[candidate_id],
                    "family": candidate.family,
                    "mode": candidate.mode,
                    "parameters": dict(candidate.parameters),
                    "expression_id": candidate.expression_id,
                    "source": self.sources[candidate_id],
                }
                for candidate_id, candidate in sorted(self.candidates.items())
            ],
        }


def _candidate(
    template: StrategyExpression,
    parameters: Mapping[str, int | float],
) -> CandidateSpec:
    """按模板构造候选身份。"""
    return CandidateSpec(
        candidate_id=candidate_identity(template, parameters),
        family=template.family,
        mode=template.mode,
        parameters=dict(parameters),
        complexity=len(parameters),
        expression_id=expression_id(template),
    )


def neighborhood_candidates(
    family: str,
    research_config: Mapping[str, object],
    neighborhood: Mapping[str, object],
    base: Sequence[CandidateSpec],
) -> tuple[CandidateSpec, ...]:
    """按邻域轴配置做笛卡尔积；未配置的轴取注册网格中的已有取值。"""
    template = strategy_expression(family)
    strategies = _object(research_config.get("strategies"), "strategies")
    strategy = _object(strategies.get(family), f"strategies.{family}")
    axes: dict[str, tuple[int | float, ...]] = {}
    for name, expected in sorted(template.parameter_types.items()):
        integral = expected.unit is Unit.WINDOW
        if name in neighborhood:
            axes[name] = axis_values(neighborhood[name], f"neighborhood.{family}.{name}", integral)
            continue
        config_key = ARRAY_AXES.get(name, name)
        raw = strategy.get(config_key)
        if raw is None:
            values = sorted({float(item.parameters[name]) for item in base})
            axes[name] = tuple(int(value) for value in values) if integral else tuple(values)
        else:
            axes[name] = axis_values(
                raw if isinstance(raw, list) else [raw], f"strategies.{family}.{config_key}",
                integral,
            )
    names = list(axes)
    result: list[CandidateSpec] = []

    def expand(index: int, current: dict[str, int | float]) -> None:
        if index == len(names):
            result.append(_candidate(template, dict(current)))
            return
        name = names[index]
        for value in axes[name]:
            current[name] = value
            expand(index + 1, current)
        current.pop(name, None)

    expand(0, {})
    return tuple(result)


def challenger_label(family: str, challenger: StructuralChallenger) -> str:
    """结构 challenger 在搜索计划中的流派标签。"""
    return f"{family}{CHALLENGER_LABEL_SEPARATOR}{challenger.expression_id[-16:]}"


def generate_loop_candidates(
    research_config: Mapping[str, object],
    config_hash: str,
    loop: LoopConfig,
) -> LoopCandidates:
    """生成注册网格、邻域网格与结构 challenger，并编译为一个搜索计划。"""
    batches = build_family_batches(research_config, loop.family_scope)
    candidates: dict[str, CandidateSpec] = {}
    sources: dict[str, str] = {}
    labels: dict[str, str] = {}
    templates: dict[str, StrategyExpression] = {}
    challengers: dict[str, tuple[StructuralChallenger, ...]] = {}
    registries: dict[str, Mapping[str, object]] = {}
    entries: list[SearchPlanEntry] = []
    budgets: dict[str, Mapping[str, int]] = {}
    lookbacks: set[int] = set()
    features = _object(research_config.get("features"), "features")
    for value in _list(features.get("lookbacks"), "features.lookbacks"):
        lookbacks.add(_integer(value, "features.lookbacks"))
    for batch in batches:
        family = batch.family
        template = strategy_expression(family)
        templates[family] = template
        family_candidates: dict[str, CandidateSpec] = {}
        for candidate in batch.candidates:
            family_candidates[candidate.candidate_id] = candidate
            sources[candidate.candidate_id] = SOURCE_REGISTERED_GRID
        for candidate in neighborhood_candidates(
            family, research_config, loop.neighborhood.get(family, {}), batch.candidates,
        ):
            if candidate.candidate_id not in family_candidates:
                family_candidates[candidate.candidate_id] = candidate
                sources[candidate.candidate_id] = SOURCE_NEIGHBORHOOD
        if len(family_candidates) > loop.candidate_budget_per_family:
            raise ValueError(
                f"流派候选超过循环预算: {family}:"
                f"{len(family_candidates)}>{loop.candidate_budget_per_family}"
            )
        for candidate in family_candidates.values():
            candidates[candidate.candidate_id] = candidate
            labels[candidate.candidate_id] = family
            lookbacks.add(int(candidate.parameters["lookback"]))
        entries.append(SearchPlanEntry(
            label=family,
            mode=batch.mode,
            template=template,
            candidate_budget=loop.candidate_budget_per_family,
            candidates=tuple(sorted(
                family_candidates.values(), key=lambda item: item.candidate_id,
            )),
        ))
        family_challengers = _structural_challengers(family, template, loop)
        challengers[family] = family_challengers
        structural_count = 0
        if family_challengers:
            registries[family] = structural_challenger_registry_payload(
                family,
                config_hash,
                [candidate.candidate_id for candidate in batch.candidates],
                str(candidate_search_plan_payload((batch,))["search_plan_id"]),
                GENERATOR_METHOD_VERSION,
                loop.structural_budget_per_family,
                family_challengers,
            )
        for challenger in family_challengers:
            label = challenger_label(family, challenger)
            templates[label] = challenger.expression
            rows = tuple(
                _candidate(challenger.expression, dict(candidate.parameters))
                for candidate in batch.candidates
            )
            for candidate in rows:
                candidates[candidate.candidate_id] = candidate
                sources[candidate.candidate_id] = SOURCE_STRUCTURAL
                labels[candidate.candidate_id] = label
                structural_count += 1
            entries.append(SearchPlanEntry(
                label=label,
                mode=challenger.expression.mode,
                template=challenger.expression,
                candidate_budget=loop.structural_budget_per_family,
                candidates=rows,
                extra={
                    "parent_family": family,
                    "structural": {
                        "operator": challenger.operator,
                        "source_path": challenger.source_path,
                        "parent_expression_id": challenger.parent_expression_id,
                        "donor_family": challenger.donor_family,
                        "donor_expression_id": challenger.donor_expression_id,
                        "donor_path": challenger.donor_path,
                    },
                    "expression": strategy_expression_payload(challenger.expression),
                },
            ))
        budgets[family] = {
            "registered_grid": len(batch.candidates),
            "neighborhood_grid": len(family_candidates) - len(batch.candidates),
            "candidate_budget": loop.candidate_budget_per_family,
            "structural_challengers": len(family_challengers),
            "structural_candidates": structural_count,
            "structural_budget": loop.structural_budget_per_family,
        }
    plan = search_plan_payload(entries)
    return LoopCandidates(
        batches=batches,
        candidates=candidates,
        sources=sources,
        labels=labels,
        templates=templates,
        challengers=challengers,
        challenger_registries=registries,
        plan=plan,
        lookbacks=tuple(sorted(lookbacks)),
        budgets=budgets,
    )


def _structural_challengers(
    family: str,
    template: StrategyExpression,
    loop: LoopConfig,
) -> tuple[StructuralChallenger, ...]:
    """按上限生成有界 typed 变异与交叉 challenger。"""
    limit = loop.structural_limit_per_family
    if limit <= 0:
        return ()
    result = list(bounded_typed_mutations(template, loop.structural_operators, limit))
    remaining = limit - len(result)
    for donor_family in loop.structural_donors.get(family, ()):
        if remaining <= 0:
            break
        donor = strategy_expression(donor_family)
        known = {item.expression_id for item in result}
        result.extend(
            item for item in bounded_typed_crossovers(template, donor, remaining)
            if item.expression_id not in known
        )
        remaining = limit - len(result)
    return tuple(result[:limit])


def loop_cost_model(research_config: Mapping[str, object]) -> Mapping[str, object]:
    """由研究配置派生 GPU 成本模型，与 validation 同口径。"""
    cost = _object(research_config.get("cost_model"), "cost_model")
    bps = sum(_number(cost.get(name), name) for name in (
        "fee_bps_assumption",
        "half_spread_bps_assumption",
        "slippage_bps_assumption",
        "impact_bps_assumption",
    ))
    interval = research_config.get("bar_interval")
    if not isinstance(interval, str) or interval not in _INTERVAL_SECONDS:
        raise ValueError("bar_interval 不支持成本回放")
    features = _object(research_config.get("features"), "features")
    gap_bars = _integer(
        features.get("maximum_structural_gap_bars_assumption"),
        "maximum_structural_gap_bars_assumption",
    )
    seconds = _INTERVAL_SECONDS[interval]
    return {
        "one_way_cost_rate": bps / 10_000.0,
        "maximum_gap_seconds": seconds * gap_bars,
        "periods_per_year": SECONDS_PER_YEAR / seconds,
    }


@dataclass(frozen=True)
class LoopRunResult:
    """一次循环运行的制品位置与摘要。"""

    search_run_id: str
    run_directory: Path
    manifest_path: Path
    proposal_path: Path
    bundle_directory: Path
    result_directory: Path
    parity_directory: Path
    summary: Mapping[str, object] = field(default_factory=dict)


def _synthetic_panel(
    loop: LoopConfig,
    research_config: Mapping[str, object],
    lookbacks: Sequence[int],
) -> ResearchPanel:
    """按循环配置的 synthetic 段生成合成面板。"""
    options = loop.synthetic
    bars = _integer(options.get("bars", 2048), "synthetic.bars")
    seed = int(_number(options.get("seed", 20260823), "synthetic.seed"))
    features = _object(research_config.get("features"), "features")
    bar_rows, _feature_rows = synthetic_panel(bars, lookbacks, seed)
    market_id = str(research_config.get("market_id") or "synthetic")
    return panel_from_bars(
        market_id,
        bar_rows,
        lookbacks,
        _integer(features.get("volume_lookback"), "volume_lookback"),
        _integer(
            features.get("maximum_structural_gap_bars_assumption"),
            "maximum_structural_gap_bars_assumption",
        ),
        bar_rows[0].open_time,
        bar_rows[-1].decision_time,
        sha256_text(canonical_json({
            "method": SYNTHETIC_METHOD_VERSION, "bars": bars, "seed": seed,
            "lookbacks": list(lookbacks),
        })),
        None,
        {"synthetic": True, "method_version": SYNTHETIC_METHOD_VERSION, "seed": seed},
    )


def run_search_loop(
    root: Path,
    loop_config_path: Path,
    *,
    data_root: Path | None = None,
    synthetic: bool = False,
    device: str | None = None,
) -> LoopRunResult:
    """执行一次完整循环：候选、搜索束、GPU 评估、复算、台账与提案。"""
    started = time.perf_counter()
    root = root.resolve()
    loop = load_loop_config(root, loop_config_path.resolve())
    research_config, config_hash, lineage_root_hash, lineage_depth = (
        load_governed_strategy_config(root, loop.research_config_path)
    )
    candidates = generate_loop_candidates(research_config, config_hash, loop)
    run_root = loop.output_root / f"search-run.partial-{int(time.time())}"
    run_root.mkdir(parents=True, exist_ok=False)
    if synthetic:
        panel = _synthetic_panel(loop, research_config, candidates.lookbacks)
    else:
        panel = load_research_panel(
            (data_root or root / "data").resolve(),
            research_config,
            loop.raw,
            candidates.lookbacks,
            run_root / "panel",
        )
    panel_elapsed = time.perf_counter() - started
    spec: ResampleSpec = resample_spec_from_config(research_config)
    cost_model = loop_cost_model(research_config)
    identity = code_identity(root, (loop.research_config_path, loop.path))
    tensor = tensorize_panel(panel.bars, panel.features, candidates.lookbacks)
    bundle: SearchBundle = build_search_bundle(
        tensor,
        candidates.plan,
        cost_model,
        spec.fold_spec_payload(),
        spec.bootstrap_payload(),
        feature_method_version=panel.feature_method_version,
        code_tree_digest=identity.tree_digest,
    )
    bundle_directory = write_search_bundle(bundle, run_root)
    if loop.write_reference_panel:
        write_panel_payload(
            run_root / f"reference-panel-{bundle.identity.panel_sha256}.json",
            panel_payload(panel.bars, panel.features, panel.feature_method_version),
        )
    resolved_device = resolve_device(device or loop.device)
    options = EvaluationOptions(
        device=resolved_device,
        candidate_chunk=loop.candidate_chunk,
        scan_method=loop.scan_method,
        seed=loop.seed,
        screen=loop.screen,
        resample=spec,
        resample_screen=loop.resample_screen,
    )
    evaluate_started = time.perf_counter()
    result_directory = evaluate_bundle(bundle, options, run_root, root)
    evaluate_elapsed = time.perf_counter() - evaluate_started
    parity_started = time.perf_counter()
    parity_directory = run_parity(
        result_directory,
        bundle,
        panel.bars,
        panel.features,
        loop.parity_tolerance,
        only_screen_passed=True,
        templates=candidates.templates,
        candidate_limit=loop.parity_candidate_limit,
    )
    parity_elapsed = time.perf_counter() - parity_started
    manifest = load_search_result(result_directory)
    ledger_record = _object(manifest.get("trial_ledger"), "trial_ledger")
    _header, trial_rows = read_ledger(result_directory / str(ledger_record.get("path")))
    parity_summary = _object(json.loads(
        (parity_directory / "parity-summary.json").read_text(encoding="utf-8"),
    ), "parity_summary")
    parity_ledger = _object(parity_summary.get("parity_ledger"), "parity_ledger")
    _parity_header, parity_rows = read_ledger(
        parity_directory / str(parity_ledger.get("path")),
    )
    runtime = runtime_identity(resolved_device)
    for family, registry in candidates.challenger_registries.items():
        atomic_write_text(
            run_root / f"structural-challengers-{family}.json",
            canonical_json(registry) + "\n",
        )
    atomic_write_text(
        run_root / "candidate-registry.json",
        canonical_json(candidates.registry_payload()) + "\n",
    )
    stage_counts = _stage_counts(trial_rows, parity_rows)
    body: dict[str, object] = {
        "schema_version": LOOP_SCHEMA_VERSION,
        "loop_method_version": LOOP_METHOD_VERSION,
        "loop_config": {
            "path": _relative(loop.path, root),
            "sha256": loop.config_sha256(),
        },
        "research_config": {
            "path": _relative(loop.research_config_path, root),
            "sha256": config_hash,
            "lineage_root_sha256": lineage_root_hash,
            "lineage_depth": lineage_depth,
        },
        "code_identity": {
            "git_hash": identity.git_hash,
            "tree_digest": identity.tree_digest,
            "dirty": identity.dirty,
        },
        "panel": panel.payload(),
        "synthetic": synthetic,
        "bundle_id": bundle.bundle_id,
        "search_bundle_identity": bundle.identity.payload(),
        "search_plan_id": candidates.plan["search_plan_id"],
        "search_result_id": manifest.get("search_result_id"),
        "cost_model": dict(cost_model),
        "resample": spec.payload(),
        "options": options.payload(),
        "runtime": runtime,
        "budgets": {family: dict(item) for family, item in candidates.budgets.items()},
        "family_scope": list(loop.family_scope),
        "stage_counts": stage_counts,
        "parity_summary": parity_summary,
        "thresholds": {
            "screen": loop.screen.payload(),
            "resample_screen": loop.resample_screen.payload(),
            "parity_tolerance": loop.parity_tolerance.payload(),
            "resample_tolerance": loop.resample_tolerance.payload(),
            "proposal": loop.proposal.payload(),
        },
        "artifacts": {
            "bundle": bundle_directory.name,
            "search_result": result_directory.name,
            "trial_ledger": str(ledger_record.get("path")),
            "parity_ledger": str(parity_ledger.get("path")),
            "candidate_registry": "candidate-registry.json",
            "structural_challengers": [
                f"structural-challengers-{family}.json"
                for family in sorted(candidates.challenger_registries)
            ],
            "proposal": "proposal.json",
        },
    }
    search_run_id = "search-run-" + sha256_text(canonical_json(body))
    proposal = build_proposal(
        search_run_id,
        body,
        research_config,
        candidates,
        trial_rows,
        parity_rows,
        loop.proposal,
    )
    atomic_write_text(run_root / "proposal.json", canonical_json(proposal) + "\n")
    timings = {
        "panel_seconds": panel_elapsed,
        "evaluate_seconds": evaluate_elapsed,
        "parity_seconds": parity_elapsed,
        "total_seconds": time.perf_counter() - started,
        "finished_at": datetime.now(UTC).isoformat(),
    }
    manifest_body = {
        **body,
        "search_run_id": search_run_id,
        "proposal_sha256": sha256_text(canonical_json(proposal) + "\n"),
        "timings": timings,
    }
    atomic_write_text(run_root / "manifest.json", canonical_json(manifest_body) + "\n")
    final_directory = loop.output_root / search_run_id
    if final_directory.exists():
        raise FileExistsError(f"搜索运行已存在: {final_directory}")
    run_root.replace(final_directory)
    return LoopRunResult(
        search_run_id=search_run_id,
        run_directory=final_directory,
        manifest_path=final_directory / "manifest.json",
        proposal_path=final_directory / "proposal.json",
        bundle_directory=final_directory / bundle_directory.name,
        result_directory=final_directory / result_directory.name,
        parity_directory=final_directory / result_directory.name / "parity",
        summary={
            "search_run_id": search_run_id,
            "stage_counts": stage_counts,
            "proposal_status": {
                family: _object(item, family).get("status")
                for family, item in _object(proposal.get("families"), "families").items()
            },
            "timings": timings,
            "runtime": runtime,
        },
    )


def _stage_counts(
    trial_rows: Sequence[Mapping[str, object]],
    parity_rows: Sequence[Mapping[str, object]],
) -> Mapping[str, int]:
    """统计各阶段行数（G-07：含被拒者）。"""
    counts = {
        "total": len(trial_rows),
        "F0_rejected": 0,
        "F1_screened": 0,
        "F1_passed": 0,
        "F3_checked": len(parity_rows),
        "F3_exact": 0,
    }
    for row in trial_rows:
        stage = str(row.get("stage"))
        if stage == "F0_rejected":
            counts["F0_rejected"] += 1
        elif stage == "F1_screened":
            counts["F1_screened"] += 1
            counts["F1_passed"] += int(bool(row.get("screen_passed")))
    for row in parity_rows:
        counts["F3_exact"] += int(bool(row.get("promotable")))
    return counts


def _relative(path: Path, root: Path) -> str:
    """尽量以项目相对路径登记。"""
    try:
        return path.resolve().relative_to(root).as_posix()
    except ValueError:
        return path.name
