"""独立运行一个策略流派的完整研究闭环。"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from guvolu.research.pipeline import run_research


def main(argv: Sequence[str] | None = None) -> int:
    """运行单流派并发布到独立目录。"""
    parser = argparse.ArgumentParser(description="独立运行一个策略流派")
    parser.add_argument("family")
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
    arguments = parser.parse_args(argv)
    root = arguments.root.resolve()
    config = arguments.config
    if not config.is_absolute():
        config = root / config
    output = arguments.output
    if output is None:
        output = root / "reports" / "strategy-research" / "families" / arguments.family
    elif not output.is_absolute():
        output = root / output
    data_root = arguments.data_root
    if data_root is not None and not data_root.is_absolute():
        data_root = root / data_root
    result = run_research(
        root, config, output, (arguments.family,), data_root=data_root,
    )
    print(json.dumps({
        "run_id": result.run_id,
        "family_scope": result.family_scope,
        "manifest": str(result.manifest_path),
        "summary": str(result.summary_path),
        "paper_eligible_families": result.paper_eligible_families,
        "operational_nonzero_families": result.operational_nonzero_families,
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
