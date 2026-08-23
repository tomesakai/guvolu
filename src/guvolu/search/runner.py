"""SearchFast 运行编排：导出搜索束、GPU 评估、CPU 数值对照与制品写入。

worker 只依赖 strategy 与 search 的纯函数合同，永不 import api 与 ops。
"""
from __future__ import annotations

import json
import math
import subprocess
import time
from array import array
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from guvolu.data.durable_io import atomic_write_bytes, atomic_write_text
from guvolu.search.bundle import (
    SearchBundle,
    evaluation_identifier,
)
from guvolu.search.identity import canonical_json, sha256_bytes, sha256_text
from guvolu.search.kernels import (
    DEFAULT_CANDIDATE_CHUNK,
    FamilyPlan,
    KernelSession,
    candidate_chunks,
    parse_family_plans,
)
from guvolu.search.ledger import (
    STAGE_F0_REJECTED,
    STAGE_F1_SCREENED,
    STAGE_F3_EXACT,
    LedgerRow,
    TrialLedgerWriter,
    ledger_header,
    read_ledger,
)
from guvolu.search.metrics import (
    METRICS_METHOD_VERSION,
    chunk_metrics,
    strategy_returns_tensor,
)
from guvolu.search.parity import (
    ParityTolerance,
    compare_parity,
    exact_reference,
)
from guvolu.search.resample import (
    ResampleMetrics,
    ResampleScreen,
    ResampleSpec,
    fold_payload,
    resample_chunk,
)
from guvolu.search.scan import SCAN_METHOD_VERSION, scan_targets
from guvolu.search.tensorize import array_bytes, array_from_bytes
from guvolu.search.torch_runtime import runtime_identity, torch_module
from guvolu.strategy.contracts import CandidateSpec, FeatureRow, ResearchBar
from guvolu.strategy.expression import StrategyExpression, Unit, strategy_expression

RUNNER_METHOD_VERSION = "searchfast-runner-v1"
SEARCH_RESULT_SCHEMA_VERSION = 1
SEARCH_RESULT_METHOD_VERSION = "searchfast-search-result-v1"
PRECISION = "f32"
REASON_LOOKBACK = "lookback_not_in_panel"
REASON_NEGATIVE_SIZING = "negative_sizing_parameter"


@dataclass(frozen=True)
class ScreenConfig:
    """F1 粗筛阈值，全部来自配置（G-06）。"""

    minimum_sharpe: float = 0.0
    maximum_drawdown: float = 1.0
    minimum_turnover: float = 0.0

    def payload(self) -> Mapping[str, object]:
        """导出配置。"""
        return {
            "minimum_sharpe": self.minimum_sharpe,
            "maximum_drawdown": self.maximum_drawdown,
            "minimum_turnover": self.minimum_turnover,
        }

    def passes(self, metrics: Mapping[str, float | int]) -> bool:
        """判断粗筛是否通过。"""
        sharpe = float(metrics["sharpe"])
        drawdown = float(metrics["maximum_drawdown"])
        turnover = float(metrics["turnover"])
        return (
            math.isfinite(sharpe)
            and sharpe >= self.minimum_sharpe
            and drawdown <= self.maximum_drawdown
            and turnover >= self.minimum_turnover
        )


def screen_from_config(config: Mapping[str, object] | None) -> ScreenConfig:
    """由配置读取粗筛阈值，缺省使用初值。"""
    if config is None:
        return ScreenConfig()
    values: dict[str, float] = {}
    default = ScreenConfig()
    for name in ("minimum_sharpe", "maximum_drawdown", "minimum_turnover"):
        raw = config.get(name, getattr(default, name))
        if (
            not isinstance(raw, (int, float))
            or isinstance(raw, bool)
            or not math.isfinite(float(raw))
        ):
            raise ValueError(f"粗筛阈值必须为有限数值: {name}")
        values[name] = float(raw)
    return ScreenConfig(**values)


@dataclass(frozen=True)
class EvaluationOptions:
    """一次 GPU 评估的运行参数。"""

    device: str
    candidate_chunk: int = DEFAULT_CANDIDATE_CHUNK
    scan_method: str = "parallel"
    seed: int = 0
    screen: ScreenConfig = ScreenConfig()
    resample: ResampleSpec | None = None
    resample_screen: ResampleScreen = ResampleScreen()

    def payload(self) -> Mapping[str, object]:
        """导出运行参数。"""
        return {
            "device": self.device,
            "candidate_chunk": self.candidate_chunk,
            "scan_method": self.scan_method,
            "scan_method_version": SCAN_METHOD_VERSION,
            "metrics_method_version": METRICS_METHOD_VERSION,
            "seed": self.seed,
            "precision": PRECISION,
            "screen": self.screen.payload(),
            "resample": (
                None if self.resample is None else self.resample.payload()
            ),
            "resample_screen": (
                None if self.resample is None else self.resample_screen.payload()
            ),
        }


def static_gate(
    family: FamilyPlan,
    row: Sequence[float],
    panel_lookbacks: Sequence[int],
) -> str | None:
    """F0 静态闸门：返回拒绝原因，通过返回空。"""
    names = family.parameter_names
    if "lookback" in names:
        lookback = row[names.index("lookback")]
        if int(lookback) not in set(panel_lookbacks):
            return REASON_LOOKBACK
    if family.sizing == "volatility_target":
        for name in ("annual_volatility_target", "maximum_target"):
            if name in names and row[names.index(name)] < 0:
                return REASON_NEGATIVE_SIZING
    return None


def worker_code_identity(root: Path) -> Mapping[str, object]:
    """记录 worker 检出的提交与脏标记，不读取密钥。"""

    def git(arguments: Sequence[str]) -> str | None:
        try:
            completed = subprocess.run(
                ["git", *arguments],
                cwd=root,
                check=True,
                capture_output=True,
                text=True,
                encoding="utf-8",
            )
        except (OSError, subprocess.CalledProcessError):
            return None
        return completed.stdout.strip()

    status = git(("status", "--porcelain=v1", "--untracked-files=all"))
    return {
        "git_hash": git(("rev-parse", "HEAD")),
        "dirty": None if status is None else bool(status),
    }


def candidate_from_plan(
    family: FamilyPlan,
    row_index: int,
    expression_id: str | None = None,
    template: StrategyExpression | None = None,
) -> CandidateSpec:
    """由 SearchPlan 参数行重建候选规格，供精确复算。"""
    if template is None:
        template = strategy_expression(family.family)
    parameters: dict[str, int | float] = {}
    for name, value in zip(family.parameter_names, family.parameter_rows[row_index], strict=True):
        expected = template.parameter_types.get(name)
        if expected is not None and expected.unit is Unit.WINDOW:
            parameters[name] = int(value)
        else:
            parameters[name] = float(value)
    return CandidateSpec(
        candidate_id=family.candidate_ids[row_index],
        family=template.family,
        mode=family.mode,
        parameters=parameters,
        complexity=len(parameters),
        expression_id=expression_id,
    )


def _family_expression_ids(plan: Mapping[str, object]) -> Mapping[str, str]:
    """读取各流派表达式身份。"""
    result: dict[str, str] = {}
    families = plan.get("families")
    if isinstance(families, list):
        for item in families:
            if isinstance(item, Mapping):
                result[str(item.get("family"))] = str(item.get("expression_id"))
    return result


def evaluate_bundle(
    bundle: SearchBundle,
    options: EvaluationOptions,
    output_root: Path,
    worker_root: Path,
) -> Path:
    """GPU/CPU Torch 评估整个搜索束，写 SearchResult 与台账。"""
    torch = torch_module()
    torch.manual_seed(options.seed)
    started = time.perf_counter()
    session = KernelSession(bundle.search_plan, bundle.panel, options.device)
    runtime = runtime_identity(options.device)
    work_directory = output_root / f"search-result.partial-{bundle.bundle_id[-16:]}"
    if work_directory.exists():
        raise FileExistsError(f"未完成的评估目录已存在: {work_directory}")
    work_directory.mkdir(parents=True)
    ledger = TrialLedgerWriter(work_directory)
    ledger.append_header(ledger_header(
        bundle.bundle_id,
        bundle.identity.payload(),
        runtime,
        {"options": options.payload(), "runner_method_version": RUNNER_METHOD_VERSION},
    ))
    families_payload: list[Mapping[str, object]] = []
    evaluations: list[Mapping[str, object]] = []
    total_candidates = 0
    rejected = 0
    screened = 0
    bar_count = bundle.panel.bar_count
    folds_record: list[Mapping[str, object]] | None = None
    for family in session.families:
        family_started = time.perf_counter()
        accepted_rows: list[int] = []
        rows: list[LedgerRow] = []
        for row_index, row in enumerate(family.parameter_rows):
            reason = static_gate(family, row, bundle.panel.lookbacks)
            candidate_id = family.candidate_ids[row_index]
            if reason is None:
                accepted_rows.append(row_index)
                continue
            rejected += 1
            rows.append(LedgerRow(
                evaluation_id=evaluation_identifier(candidate_id, bundle.identity),
                candidate_id=candidate_id,
                family=family.family,
                bundle_id=bundle.bundle_id,
                stage=STAGE_F0_REJECTED,
                device=options.device,
                precision=PRECISION,
                metrics=None,
                parity=None,
                screen_passed=None,
                promotable=False,
                reason=reason,
            ))
        ledger.append_rows(rows)
        targets_store: array[float] = array("f")
        accepted_ids: list[str] = []
        for start, stop in candidate_chunks(len(accepted_rows), options.candidate_chunk):
            subset = accepted_rows[start:stop]
            chunk_family = _subset_family(family, subset)
            signals = session.evaluate_chunk(chunk_family, 0, len(subset))
            targets = scan_targets(
                session, signals, _periods_per_year(bundle), options.scan_method,
            )
            metrics = chunk_metrics(session, targets, bundle.cost_model)
            metric_rows = metrics.rows()
            resample_rows: tuple[Mapping[str, object], ...] | None = None
            if options.resample is not None:
                returns, _turnover, _held = strategy_returns_tensor(
                    session, targets, bundle.cost_model,
                )
                resampled: ResampleMetrics = resample_chunk(
                    session,
                    family.family,
                    returns,
                    options.resample,
                    _periods_per_year(bundle),
                )
                resample_rows = resampled.rows()
                if folds_record is None:
                    folds_record = fold_payload(resampled.folds)
            targets_store.frombytes(_tensor_f32_bytes(targets))
            chunk_rows: list[LedgerRow] = []
            for offset, row_index in enumerate(subset):
                candidate_id = family.candidate_ids[row_index]
                row_metrics = metric_rows[offset]
                passed = options.screen.passes(row_metrics)
                row_resample: Mapping[str, object] | None = None
                if resample_rows is not None:
                    row_resample = resample_rows[offset]
                    passed = passed and options.resample_screen.passes(row_resample)
                screened += 1
                accepted_ids.append(candidate_id)
                evaluation_id = evaluation_identifier(candidate_id, bundle.identity)
                chunk_rows.append(LedgerRow(
                    evaluation_id=evaluation_id,
                    candidate_id=candidate_id,
                    family=family.family,
                    bundle_id=bundle.bundle_id,
                    stage=STAGE_F1_SCREENED,
                    device=options.device,
                    precision=PRECISION,
                    metrics=row_metrics,
                    parity=None,
                    screen_passed=passed,
                    promotable=False,
                    resample=row_resample,
                ))
                evaluation: dict[str, object] = {
                    "evaluation_id": evaluation_id,
                    "candidate_id": candidate_id,
                    "family": family.family,
                    "screen_passed": passed,
                    "metrics": dict(row_metrics),
                }
                if row_resample is not None:
                    evaluation["resample"] = dict(row_resample)
                evaluations.append(evaluation)
            ledger.append_rows(chunk_rows)
        if options.device.startswith("cuda"):
            torch.cuda.synchronize()
        targets_record = _write_targets(work_directory, family.family, targets_store)
        elapsed = time.perf_counter() - family_started
        total_candidates += len(family.parameter_rows)
        families_payload.append({
            "family": family.family,
            "sizing": family.sizing,
            "candidate_count": len(family.parameter_rows),
            "rejected_count": len(family.parameter_rows) - len(accepted_rows),
            "evaluated_candidate_ids": accepted_ids,
            "targets": targets_record,
            "bar_count": bar_count,
            "elapsed_seconds": elapsed,
        })
    ledger_path, ledger_sha256 = ledger.finalize()
    total_elapsed = time.perf_counter() - started
    body: dict[str, object] = {
        "schema_version": SEARCH_RESULT_SCHEMA_VERSION,
        "search_result_method_version": SEARCH_RESULT_METHOD_VERSION,
        "runner_method_version": RUNNER_METHOD_VERSION,
        "bundle_id": bundle.bundle_id,
        "search_bundle_identity": bundle.identity.payload(),
        "search_plan_id": bundle.identity.search_plan_id,
        "cost_model": dict(bundle.cost_model),
        "options": options.payload(),
        "runtime": runtime,
        "worker_code_identity": worker_code_identity(worker_root),
        "candidate_count": total_candidates,
        "rejected_count": rejected,
        "screened_count": screened,
        "bar_count": bar_count,
        "folds": folds_record,
        "families": families_payload,
        "evaluations": evaluations,
        "trial_ledger": {
            "path": ledger_path.name,
            "sha256": ledger_sha256,
            "rows": ledger.rows_written,
        },
        "timings": {
            "total_seconds": total_elapsed,
            "candidates_per_second": (
                screened / total_elapsed if total_elapsed > 0 else None
            ),
            "candidate_bars_per_second": (
                screened * bar_count / total_elapsed if total_elapsed > 0 else None
            ),
        },
    }
    result_id = search_result_identifier(body)
    manifest = {**body, "search_result_id": result_id}
    atomic_write_text(
        work_directory / "manifest.json", canonical_json(manifest) + "\n",
    )
    final_directory = output_root / result_id
    if final_directory.exists():
        raise FileExistsError(f"SearchResult 已存在: {final_directory}")
    work_directory.replace(final_directory)
    return final_directory


def _tensor_f32_bytes(tensor: object) -> bytes:
    """把 f32 张量搬回主机并导出小端字节。"""
    host = getattr(tensor, "cpu")().contiguous().reshape(-1)
    try:
        body = bytes(host.numpy().astype("<f4", copy=False).tobytes())
    except (RuntimeError, ModuleNotFoundError):
        body = array_bytes(array("f", host.tolist()))
    return body


def _periods_per_year(bundle: SearchBundle) -> float:
    """读取成本模型中的年化周期。"""
    value = bundle.cost_model.get("periods_per_year")
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValueError("成本模型缺少 periods_per_year")
    return float(value)


def _subset_family(family: FamilyPlan, rows: Sequence[int]) -> FamilyPlan:
    """按通过静态闸门的行子集构造分块流派登记。"""
    return FamilyPlan(
        index=family.index,
        family=family.family,
        mode=family.mode,
        sizing=family.sizing,
        parameter_names=family.parameter_names,
        candidate_ids=tuple(family.candidate_ids[index] for index in rows),
        parameter_rows=tuple(family.parameter_rows[index] for index in rows),
        required=family.required,
        entry=family.entry,
        exit=family.exit,
        target=family.target,
        node_order=family.node_order,
    )


def _write_targets(
    directory: Path,
    family: str,
    values: array[float],
) -> Mapping[str, object]:
    """以内容散列命名写入一个流派的目标矩阵。"""
    body = array_bytes(values)
    digest = sha256_bytes(body)
    name = f"targets-{family}-{digest}.bin"
    atomic_write_bytes(directory / name, body)
    return {"file": name, "sha256": digest, "count": len(values), "typecode": "f"}


def _row_sharpe(row: Mapping[str, object]) -> float:
    """读取台账行的粗筛 Sharpe，缺失记为负无穷。"""
    metrics = row.get("metrics")
    if not isinstance(metrics, Mapping):
        return -math.inf
    value = metrics.get("sharpe")
    if not isinstance(value, (int, float)) or not math.isfinite(float(value)):
        return -math.inf
    return float(value)


def search_result_identifier(body: Mapping[str, object]) -> str:
    """SearchResult 身份绑定评估内容与运行时，不含墙钟计时。"""
    identity_body = {
        key: value for key, value in body.items()
        if key not in ("search_result_id", "timings")
    }
    return "search-result-" + sha256_text(canonical_json(identity_body))


def load_search_result(directory: Path) -> Mapping[str, object]:
    """读取并校验 SearchResult manifest 身份。"""
    manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    if not isinstance(manifest, Mapping):
        raise ValueError("SearchResult manifest 必须为对象")
    expected = search_result_identifier(manifest)
    if manifest.get("search_result_id") != expected or directory.name != expected:
        raise ValueError("SearchResult 身份与内容不一致")
    return {str(key): value for key, value in manifest.items()}


def _family_targets(
    directory: Path,
    record: Mapping[str, object],
    bar_count: int,
) -> Mapping[str, tuple[float, ...]]:
    """读取一个流派的目标矩阵并按候选拆行。"""
    targets = record.get("targets")
    if not isinstance(targets, Mapping):
        raise ValueError("SearchResult 流派缺少 targets")
    path = directory / str(targets.get("file"))
    body = path.read_bytes()
    if sha256_bytes(body) != targets.get("sha256"):
        raise ValueError(f"目标矩阵散列不匹配: {path.name}")
    values = array_from_bytes("f", body)
    ids = record.get("evaluated_candidate_ids")
    if not isinstance(ids, list):
        raise ValueError("SearchResult 流派缺少候选列表")
    if len(values) != len(ids) * bar_count:
        raise ValueError("目标矩阵长度与候选数不一致")
    result: dict[str, tuple[float, ...]] = {}
    for index, candidate_id in enumerate(ids):
        start = index * bar_count
        result[str(candidate_id)] = tuple(
            float(item) for item in values[start:start + bar_count]
        )
    return result


def run_parity(
    result_directory: Path,
    bundle: SearchBundle,
    bars: Sequence[ResearchBar],
    features: Sequence[FeatureRow],
    tolerance: ParityTolerance,
    only_screen_passed: bool = True,
    templates: Mapping[str, StrategyExpression] | None = None,
    candidate_limit: int | None = None,
) -> Path:
    """对晋级候选做 CPU 精确复算并登记数值对照。

    `templates` 按计划流派标签给出未注册 challenger 的表达式；
    `candidate_limit` 为复算上限，按粗筛 Sharpe 降序截取。
    """
    manifest = load_search_result(result_directory)
    if manifest.get("bundle_id") != bundle.bundle_id:
        raise ValueError("SearchResult 与搜索束身份不一致")
    if len(bars) != bundle.panel.bar_count:
        raise ValueError("参考面板柱数与搜索束不一致")
    ledger_record = manifest.get("trial_ledger")
    if not isinstance(ledger_record, Mapping):
        raise ValueError("SearchResult 缺少台账记录")
    _header, rows = read_ledger(result_directory / str(ledger_record.get("path")))
    expression_ids = _family_expression_ids(bundle.search_plan)
    families = {item.family: item for item in parse_family_plans(bundle.search_plan)}
    family_records = manifest.get("families")
    if not isinstance(family_records, list):
        raise ValueError("SearchResult 缺少 families")
    targets_by_candidate: dict[str, tuple[float, ...]] = {}
    for record in family_records:
        if isinstance(record, Mapping):
            targets_by_candidate.update(
                _family_targets(result_directory, record, bundle.panel.bar_count),
            )
    parity_directory = result_directory / "parity"
    parity_directory.mkdir(exist_ok=True)
    writer = TrialLedgerWriter(parity_directory)
    writer.append_header(ledger_header(
        bundle.bundle_id,
        bundle.identity.payload(),
        {"device": "cpu", "precision": "f64", "accumulation": "ordered"},
        {
            "search_result_id": manifest.get("search_result_id"),
            "tolerance": tolerance.payload(),
            "only_screen_passed": only_screen_passed,
        },
    ))
    checked = 0
    passed_count = 0
    worst = {"target": 0.0, "sharpe": 0.0, "turnover": 0.0}
    output_rows: list[LedgerRow] = []
    selected_rows = [
        row for row in rows
        if row.get("stage") == STAGE_F1_SCREENED
        and (not only_screen_passed or bool(row.get("screen_passed")))
    ]
    if candidate_limit is not None:
        selected_rows.sort(key=lambda row: (
            -_row_sharpe(row), str(row.get("candidate_id")),
        ))
        selected_rows = selected_rows[:max(candidate_limit, 0)]
    for row in selected_rows:
        candidate_id = str(row.get("candidate_id"))
        family = families[str(row.get("family"))]
        row_index = family.candidate_ids.index(candidate_id)
        template = None if templates is None else templates.get(family.family)
        candidate = candidate_from_plan(
            family, row_index, expression_ids.get(family.family), template,
        )
        reference_targets, reference = exact_reference(
            candidate, bars, features, bundle.cost_model, template,
        )
        metrics = row.get("metrics")
        if not isinstance(metrics, Mapping):
            raise ValueError("台账行缺少指标")
        fast_metrics = {str(key): float(value) for key, value in metrics.items()}
        result = compare_parity(
            targets_by_candidate[candidate_id],
            fast_metrics,
            reference_targets,
            reference,
            tolerance,
        )
        checked += 1
        passed_count += int(result.passed)
        worst["target"] = max(worst["target"], result.target_max_abs_diff)
        worst["sharpe"] = max(worst["sharpe"], result.sharpe_abs_diff)
        worst["turnover"] = max(worst["turnover"], result.turnover_abs_diff)
        output_rows.append(LedgerRow(
            evaluation_id=str(row.get("evaluation_id")),
            candidate_id=candidate_id,
            family=family.family,
            bundle_id=bundle.bundle_id,
            stage=STAGE_F3_EXACT if result.passed else STAGE_F1_SCREENED,
            device="cpu",
            precision="f64",
            metrics=reference.payload(),
            parity=result.payload(),
            screen_passed=bool(row.get("screen_passed")),
            promotable=result.passed,
            reason=None if result.passed else "parity_out_of_tolerance",
        ))
    writer.append_rows(output_rows)
    ledger_path, ledger_sha256 = writer.finalize()
    summary = {
        "search_result_id": manifest.get("search_result_id"),
        "bundle_id": bundle.bundle_id,
        "tolerance": tolerance.payload(),
        "candidate_limit": candidate_limit,
        "checked": checked,
        "passed": passed_count,
        "failed": checked - passed_count,
        "max_abs_diff": worst,
        "parity_ledger": {"path": ledger_path.name, "sha256": ledger_sha256},
    }
    atomic_write_text(
        parity_directory / "parity-summary.json", canonical_json(summary) + "\n",
    )
    return parity_directory
