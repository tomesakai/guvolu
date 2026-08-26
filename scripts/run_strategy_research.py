"""运行完整策略研究管线。"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from guvolu.research.panel import parse_time
from guvolu.research.pipeline import run_research


def main(argv: Sequence[str] | None = None) -> int:
    """执行 CPU 研究并打印制品位置。"""
    parser = argparse.ArgumentParser(description="运行策略研究管线")
    parser.add_argument(
        "--to-time",
        dest="to_time",
        help=(
            "面板截止上限（ISO8601 UTC）；覆盖配置 "
            "data_governance.panel_to_time 且只能更早"
        ),
    )
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/strategy_research.json"),
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--data-root",
        type=Path,
        help="只读权威市场数据根；研究治理与制品仍写入项目目录",
    )
    parser.add_argument(
        "--family",
        action="append",
        dest="families",
        help="仅运行指定策略家族；可重复传入",
    )
    parser.add_argument(
        "--no-industry-evidence",
        dest="industry_evidence",
        action="store_false",
        help="跳过行业稳健性证据生成；仅供快速迭代，产出不可用于准入",
    )
    arguments = parser.parse_args(argv)
    root = arguments.root.resolve()
    config = arguments.config
    if not config.is_absolute():
        config = root / config
    output = arguments.output
    if output is not None and not output.is_absolute():
        output = root / output
    data_root = arguments.data_root
    if data_root is not None and not data_root.is_absolute():
        data_root = root / data_root
    to_time = (
        None if arguments.to_time is None
        else parse_time(arguments.to_time, "--to-time")
    )
    result = run_research(
        root,
        config,
        output,
        arguments.families,
        data_root=data_root,
        panel_to_time=to_time,
        generate_industry_evidence=arguments.industry_evidence,
    )
    print(json.dumps({
        "run_id": result.run_id,
        "manifest": str(result.manifest_path),
        "manifest_sha256": result.manifest_sha256,
        "summary": str(result.summary_path),
        "trial_ledger": str(result.trial_ledger_path),
        "target_position": str(result.target_position_path),
        "decision_grade": result.decision_grade,
        "paper_eligible_families": result.paper_eligible_families,
        "operational_nonzero_families": result.operational_nonzero_families,
        "family_scope": result.family_scope,
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
