"""GPU SearchFast 入口：导出搜索束、Torch 评估、CPU 数值对照与自检。

子命令：export 在 CPU 研究进程导出只读搜索束与参考面板；evaluate 为 worker，
只读搜索束并写 SearchResult 与台账，永不 import api 与 ops（G-01）；
parity 在 CPU 以 f64 精确复算晋级候选；selfcheck 以合成面板串联三步。
"""
from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path

from guvolu.search.bundle import (
    SearchBundle,
    build_search_bundle,
    load_search_bundle,
    write_search_bundle,
)
from guvolu.search.panel_io import (
    load_panel_payload,
    panel_payload,
    write_panel_payload,
)
from guvolu.search.parity import tolerance_from_config
from guvolu.search.runner import (
    EvaluationOptions,
    evaluate_bundle,
    run_parity,
    screen_from_config,
)
from guvolu.search.synthetic import (
    SYNTHETIC_METHOD_VERSION,
    synthetic_panel,
    synthetic_strategy_config,
)
from guvolu.search.tensorize import tensorize_panel
from guvolu.search.torch_runtime import resolve_device
from guvolu.strategy.contracts import FeatureRow, ResearchBar
from guvolu.strategy.generation import (
    build_family_batches,
    candidate_search_plan_payload,
)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LOOKBACKS = (24, 72, 168)


def _lookbacks(text: str) -> tuple[int, ...]:
    """解析逗号分隔的回看窗。"""
    values = tuple(sorted({int(item) for item in text.split(",") if item.strip()}))
    if not values:
        raise argparse.ArgumentTypeError("回看窗不得为空")
    return values


def _json_file(path: Path | None) -> Mapping[str, object] | None:
    """读取可选 JSON 配置。"""
    if path is None:
        return None
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, Mapping):
        raise ValueError(f"配置必须为对象: {path}")
    return {str(key): value for key, value in raw.items()}


def _code_tree_digest(config_paths: Sequence[Path]) -> str:
    """由研究侧代码身份读取代码树摘要，worker 不调用。"""
    from guvolu.research.provenance import code_identity

    return code_identity(ROOT, tuple(config_paths)).tree_digest


def _panel_inputs(
    arguments: argparse.Namespace,
) -> tuple[tuple[ResearchBar, ...], tuple[FeatureRow, ...], str, tuple[int, ...]]:
    """由合成参数或参考面板 JSON 得到行情柱与特征。"""
    if arguments.panel_json is not None:
        bars, features, method = load_panel_payload(arguments.panel_json)
        lookbacks = tuple(sorted(features[0].trend_scores)) if features else ()
        return bars, features, method, lookbacks
    lookbacks = tuple(arguments.lookbacks)
    bars, features = synthetic_panel(
        arguments.synthetic_bars, lookbacks, arguments.synthetic_seed,
    )
    return bars, features, SYNTHETIC_METHOD_VERSION, lookbacks


def _strategy_config(
    arguments: argparse.Namespace,
    lookbacks: Sequence[int],
) -> Mapping[str, object]:
    """读取研究配置的策略段，缺省为合成自检配置。"""
    config = _json_file(arguments.strategy_config)
    if config is None:
        return synthetic_strategy_config(lookbacks)
    return config


def export_bundle(arguments: argparse.Namespace) -> tuple[Path, Path, SearchBundle]:
    """导出搜索束与参考面板，返回束目录、面板路径与束。"""
    bars, features, feature_method_version, lookbacks = _panel_inputs(arguments)
    config = _strategy_config(arguments, lookbacks)
    strategies = config.get("strategies")
    if not isinstance(strategies, Mapping):
        raise ValueError("策略配置缺少 strategies")
    family_scope = tuple(sorted(str(name) for name in strategies))
    plan = candidate_search_plan_payload(build_family_batches(config, family_scope))
    panel = tensorize_panel(bars, features, lookbacks)
    cost_model = {
        "one_way_cost_rate": arguments.cost_rate,
        "maximum_gap_seconds": arguments.maximum_gap_seconds,
        "periods_per_year": arguments.periods_per_year,
    }
    fold_spec = {"method": "full_sample", "bars": len(bars)}
    bootstrap = {"seed": arguments.seed, "block": arguments.bootstrap_block, "paths": 0}
    config_paths = [] if arguments.strategy_config is None else [arguments.strategy_config]
    bundle = build_search_bundle(
        panel,
        plan,
        cost_model,
        fold_spec,
        bootstrap,
        feature_method_version=feature_method_version,
        code_tree_digest=_code_tree_digest(config_paths),
    )
    output = Path(arguments.output)
    directory = write_search_bundle(bundle, output)
    payload = panel_payload(bars, features, feature_method_version)
    panel_path = output / f"reference-panel-{bundle.identity.panel_sha256}.json"
    write_panel_payload(panel_path, payload)
    return directory, panel_path, bundle


def evaluate(arguments: argparse.Namespace, bundle: SearchBundle | None = None) -> Path:
    """worker 评估：只读搜索束，写 SearchResult 与台账。"""
    loaded = load_search_bundle(Path(arguments.bundle)) if bundle is None else bundle
    options = EvaluationOptions(
        device=resolve_device(arguments.device),
        candidate_chunk=arguments.candidate_chunk,
        scan_method=arguments.scan_method,
        seed=arguments.seed,
        screen=screen_from_config(_json_file(arguments.screen_config)),
    )
    return evaluate_bundle(loaded, options, Path(arguments.output), ROOT)


def parity(
    arguments: argparse.Namespace,
    bundle: SearchBundle | None = None,
    panel_path: Path | None = None,
    result_directory: Path | None = None,
) -> Path:
    """CPU 精确复算晋级候选并登记数值对照。"""
    loaded = load_search_bundle(Path(arguments.bundle)) if bundle is None else bundle
    source = Path(arguments.panel_json) if panel_path is None else panel_path
    bars, features, _method = load_panel_payload(source)
    tolerance = tolerance_from_config(_json_file(arguments.tolerance_config))
    directory = Path(arguments.result) if result_directory is None else result_directory
    return run_parity(
        directory, loaded, bars, features, tolerance,
        only_screen_passed=not arguments.parity_all,
    )


def _add_panel_arguments(parser: argparse.ArgumentParser) -> None:
    """登记面板来源参数。"""
    parser.add_argument("--panel-json", type=Path, default=None)
    parser.add_argument("--strategy-config", type=Path, default=None)
    parser.add_argument("--synthetic-bars", type=int, default=2048)
    parser.add_argument("--synthetic-seed", type=int, default=20260823)
    parser.add_argument("--lookbacks", type=_lookbacks, default=DEFAULT_LOOKBACKS)
    parser.add_argument("--cost-rate", type=float, default=0.001)
    parser.add_argument("--maximum-gap-seconds", type=float, default=None)
    parser.add_argument("--periods-per-year", type=float, default=8760.0)
    parser.add_argument("--bootstrap-block", type=int, default=24)


def _add_evaluate_arguments(parser: argparse.ArgumentParser) -> None:
    """登记评估参数。"""
    parser.add_argument("--device", default="auto")
    parser.add_argument("--candidate-chunk", type=int, default=1024)
    parser.add_argument("--scan-method", default="parallel")
    parser.add_argument("--screen-config", type=Path, default=None)


def _add_parity_arguments(parser: argparse.ArgumentParser) -> None:
    """登记数值对照参数。"""
    parser.add_argument("--tolerance-config", type=Path, default=None)
    parser.add_argument("--parity-all", action="store_true")


def build_parser() -> argparse.ArgumentParser:
    """构造命令行解析器。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=0)
    subparsers = parser.add_subparsers(dest="command", required=True)
    export_parser = subparsers.add_parser("export")
    export_parser.add_argument("--output", type=Path, required=True)
    _add_panel_arguments(export_parser)
    evaluate_parser = subparsers.add_parser("evaluate")
    evaluate_parser.add_argument("--bundle", type=Path, required=True)
    evaluate_parser.add_argument("--output", type=Path, required=True)
    _add_evaluate_arguments(evaluate_parser)
    parity_parser = subparsers.add_parser("parity")
    parity_parser.add_argument("--bundle", type=Path, required=True)
    parity_parser.add_argument("--result", type=Path, required=True)
    parity_parser.add_argument("--panel-json", type=Path, required=True)
    _add_parity_arguments(parity_parser)
    selfcheck_parser = subparsers.add_parser("selfcheck")
    selfcheck_parser.add_argument("--output", type=Path, required=True)
    _add_panel_arguments(selfcheck_parser)
    _add_evaluate_arguments(selfcheck_parser)
    _add_parity_arguments(selfcheck_parser)
    return parser


def main(argv: Sequence[str] | None = None) -> Mapping[str, object]:
    """执行子命令并返回制品路径摘要。"""
    arguments = build_parser().parse_args(argv)
    if arguments.command == "export":
        directory, panel_path, bundle = export_bundle(arguments)
        summary: dict[str, object] = {
            "command": "export",
            "bundle_id": bundle.bundle_id,
            "bundle_directory": str(directory),
            "reference_panel": str(panel_path),
        }
    elif arguments.command == "evaluate":
        result = evaluate(arguments)
        summary = {"command": "evaluate", "search_result_directory": str(result)}
    elif arguments.command == "parity":
        parity_directory = parity(arguments)
        summary = {"command": "parity", "parity_directory": str(parity_directory)}
    else:
        directory, panel_path, bundle = export_bundle(arguments)
        arguments.bundle = directory
        result = evaluate(arguments, bundle)
        parity_directory = parity(arguments, bundle, panel_path, result)
        summary = {
            "command": "selfcheck",
            "bundle_id": bundle.bundle_id,
            "bundle_directory": str(directory),
            "reference_panel": str(panel_path),
            "search_result_directory": str(result),
            "parity_directory": str(parity_directory),
        }
    text = json.dumps(summary, ensure_ascii=False, sort_keys=True, indent=2)
    sys.stdout.write(text + "\n")
    return summary


if __name__ == "__main__":
    main()
