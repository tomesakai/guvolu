"""策略生成到验证的一轮闭环：搜索、提升、决策级研究、裁决。

一轮做四件事：在只读数据快照上跑 GPU 搜索循环得到提案；把状态为
proposed 的流派提升为新研究配置（写入搜索试验证据与准入扩展）；对每份
新配置在同一快照、同一面板截止上运行完整研究；把家族准入结论汇总为
一份周期报告。提案是否进入冻结计划仍由维护者决定（A-05），本脚本不
改写基准配置，不触碰任何交易端点。

面板截止必须早于当前 holdout vintage 起点，否则研究会消费样本外数据。
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

from guvolu.data.durable_io import atomic_write_text
from guvolu.research.panel import parse_time
from guvolu.research.pipeline import run_research
from guvolu.search.promote import (
    load_proposal,
    promoted_config,
    write_promoted_config,
)

ROOT = Path(__file__).resolve().parents[1]
CYCLE_METHOD_VERSION = "strategy-evolution-cycle-v1"


def _run_search(
    root: Path, gpu_python: Path, search_config: Path, data_root: Path,
    device: str,
) -> Path:
    """以 GPU 解释器运行搜索循环，返回提案路径。"""
    command = [
        str(gpu_python), str(root / "scripts" / "run_search_loop.py"),
        "--config", str(search_config), "--data-root", str(data_root),
        "--device", device,
    ]
    environment = {"PYTHONPATH": str(root / "src"), "PYTHONIOENCODING": "utf-8"}
    completed = subprocess.run(
        command, cwd=root, capture_output=True, text=True, encoding="utf-8",
        env={**_inherited_environment(), **environment}, check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"搜索循环失败({completed.returncode}): {completed.stderr[-2000:]}"
        )
    summary = json.loads(completed.stdout)
    return Path(str(summary["proposal"]))


def _inherited_environment() -> dict[str, str]:
    import os

    return dict(os.environ)


def _family_verdicts(summary_path: Path) -> list[dict[str, object]]:
    """从研究摘要提取家族准入结论。"""
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    verdicts: list[dict[str, object]] = []
    for item in summary.get("family_evaluations", []):
        if not isinstance(item, dict):
            continue
        extension = item.get("admission_extensions") or {}
        verdicts.append({
            "family": item.get("family"),
            "eligible": item.get("eligible"),
            "rejection_reasons": item.get("rejection_reasons"),
            "deployment_parameters": item.get("deployment_parameters"),
            "stitched_sharpe": (item.get("validation_metrics") or {}).get("sharpe"),
            "deployment_sharpe": (
                item.get("deployment_oos_metrics") or {}
            ).get("sharpe"),
            "fdr_q": item.get("fdr_q"),
            "probability_backtest_overfitting": (
                item.get("probability_backtest_overfitting")
            ),
            "deflated_sharpe_probability_effective": (
                item.get("deflated_sharpe_probability_effective")
            ),
            "raw_trial_count": item.get("raw_trial_count"),
            "effective_trial_count": item.get("effective_trial_count"),
            "benchmark_sharpe": extension.get("benchmark_sharpe"),
            "benchmark_sharpe_excess": extension.get("benchmark_sharpe_excess"),
            "search_trial_count": extension.get("search_trial_count"),
        })
    return verdicts


def run_cycle(
    root: Path,
    *,
    data_root: Path,
    panel_to_time: datetime,
    search_config: Path,
    gpu_python: Path,
    device: str,
    proposal: Path | None,
    families: Sequence[str] | None,
    output_root: Path,
) -> dict[str, object]:
    """执行一轮闭环并写出周期报告。"""
    started = datetime.now(UTC)
    proposal_path = (
        proposal.resolve() if proposal is not None
        else _run_search(root, gpu_python, search_config, data_root, device)
    )
    proposal_body, proposal_sha = load_proposal(proposal_path)
    statuses = {
        family: (item.get("status") if isinstance(item, dict) else None)
        for family, item in dict(proposal_body.get("families") or {}).items()
    }
    proposed = sorted(
        family for family, status in statuses.items()
        if status == "proposed" and (families is None or family in families)
    )
    runs: list[dict[str, object]] = []
    for family in proposed:
        promotion = promoted_config(root, proposal_path, [family])
        config_path = write_promoted_config(root, promotion)
        output = output_root / f"cycle-{started:%Y%m%dT%H%M%SZ}" / family
        result = run_research(
            root, config_path, output, [family],
            data_root=data_root, panel_to_time=panel_to_time,
        )
        runs.append({
            "family": family,
            "config_path": str(config_path.relative_to(root)),
            "run_id": result.run_id,
            "manifest_sha256": result.manifest_sha256,
            "decision_grade": result.decision_grade,
            "paper_eligible_families": list(result.paper_eligible_families),
            "verdicts": _family_verdicts(Path(result.summary_path)),
        })
    report = {
        "schema_version": 1,
        "method_version": CYCLE_METHOD_VERSION,
        "started_at": started.isoformat(),
        "finished_at": datetime.now(UTC).isoformat(),
        "data_root": str(data_root),
        "panel_to_time": panel_to_time.isoformat(),
        "proposal_path": str(proposal_path),
        "proposal_sha256": proposal_sha,
        "proposal_statuses": statuses,
        "runs": runs,
        "next_step": (
            "eligible 家族可登记为后继冻结计划候选（A-05 人工决定）"
            if any(run["paper_eligible_families"] for run in runs)
            else "无合格候选；调整搜索邻域或等待新数据"
        ),
    }
    output_root.mkdir(parents=True, exist_ok=True)
    report_path = output_root / f"cycle-{started:%Y%m%dT%H%M%SZ}.json"
    atomic_write_text(
        report_path,
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
    )
    report["report_path"] = str(report_path)
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument(
        "--data-root", type=Path, required=True,
        help="只读静态数据快照根；不得指向生产数据根",
    )
    parser.add_argument(
        "--to-time", required=True,
        help="面板截止（ISO8601 UTC），须早于 holdout vintage 起点",
    )
    parser.add_argument(
        "--search-config", type=Path, default=Path("config/search_loop.json"),
    )
    parser.add_argument(
        "--gpu-python", type=Path,
        default=Path(".venv-gpu/Scripts/python.exe"),
    )
    parser.add_argument("--device", default="cuda")
    parser.add_argument(
        "--proposal", type=Path, default=None,
        help="复用已有提案，跳过搜索循环",
    )
    parser.add_argument("--family", action="append", dest="families")
    parser.add_argument(
        "--output-root", type=Path, default=Path("reports/strategy-evolution"),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    root = arguments.root.resolve()

    def absolute(path: Path) -> Path:
        return path if path.is_absolute() else root / path

    report = run_cycle(
        root,
        data_root=absolute(arguments.data_root).resolve(),
        panel_to_time=parse_time(arguments.to_time, "--to-time"),
        search_config=absolute(arguments.search_config),
        gpu_python=absolute(arguments.gpu_python),
        device=str(arguments.device),
        proposal=(
            None if arguments.proposal is None else absolute(arguments.proposal)
        ),
        families=arguments.families,
        output_root=absolute(arguments.output_root),
    )
    sys.stdout.write(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
