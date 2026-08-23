"""把搜索循环提案应用为新的研究配置文件版本，并打印研究运行命令。

不自动运行研究，不改写 config/strategy_research.json；新配置为谱系根并以
`search_loop_source` 登记来源，研究准入仍由 run_strategy_research.py 完成。
"""
from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

from guvolu.search.promote import (
    promoted_config,
    research_command,
    write_promoted_config,
)

ROOT = Path(__file__).resolve().parents[1]


def build_parser() -> argparse.ArgumentParser:
    """构造命令行解析器。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--proposal", type=Path, required=True)
    parser.add_argument(
        "--family", action="append", dest="families",
        help="只采纳指定流派；可重复传入，缺省采纳全部 proposed 流派",
    )
    parser.add_argument(
        "--output-dir", type=Path, default=None,
        help="新配置目录，缺省为项目 config 目录",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """应用提案并打印新配置与研究命令。"""
    arguments = build_parser().parse_args(argv)
    root = arguments.root.resolve()
    proposal = arguments.proposal
    if not proposal.is_absolute():
        proposal = root / proposal
    output = arguments.output_dir
    if output is not None and not output.is_absolute():
        output = root / output
    result = promoted_config(root, proposal, arguments.families)
    path = write_promoted_config(root, result, output)
    command = research_command(root, path)
    sys.stdout.write(json.dumps({
        "config": str(path),
        "applied_families": list(result.applied_families),
        "skipped_families": dict(result.skipped_families),
        "parent_config_path": result.parent_config_path,
        "parent_config_sha256": result.parent_config_sha256,
        "proposal_sha256": result.proposal_sha256,
        "run_command": command,
    }, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
    sys.stdout.write(command + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
