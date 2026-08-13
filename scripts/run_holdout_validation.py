"""在一次性封存段上评估冻结部署候选。"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from guvolu.research.holdout import run_holdout_validation


def main(argv: Sequence[str] | None = None) -> int:
    """消费 vintage 并发布不可重跑的 holdout 结果。"""
    parser = argparse.ArgumentParser(
        description="原子消费 G-08 vintage 并评估冻结组合候选",
    )
    parser.add_argument("vintage_id")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/strategy_research.json"),
    )
    parser.add_argument("--source-summary", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    arguments = parser.parse_args(argv)
    root = arguments.root.resolve()
    config = arguments.config if arguments.config.is_absolute() else root / arguments.config
    summary = (
        arguments.source_summary
        if arguments.source_summary.is_absolute()
        else root / arguments.source_summary
    )
    output = arguments.output
    if output is not None and not output.is_absolute():
        output = root / output
    result = run_holdout_validation(
        root,
        config,
        summary,
        arguments.vintage_id,
        output,
    )
    print(json.dumps({
        "evaluation_id": result.evaluation_id,
        "verdict": result.verdict,
        "manifest": result.manifest_path.as_posix(),
        "manifest_sha256": result.manifest_sha256,
        "result": result.result_path.as_posix(),
    }, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
