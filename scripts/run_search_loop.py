"""运行策略生成迭代循环 v1：GPU 宽筛、受约束配置提案、CPU 复算与台账。

循环只读活动 head 与研究配置，输出搜索束、SearchResult、试验台账、
数值对照与 proposal.json；不改写 config/strategy_research.json，不写
SQLite 与生产数据目录，不 import api 与 ops（G-01）。
"""
from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

from guvolu.search.loop import run_search_loop

ROOT = Path(__file__).resolve().parents[1]


def build_parser() -> argparse.ArgumentParser:
    """构造命令行解析器。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument(
        "--config", type=Path, default=Path("config/search_loop.json"),
    )
    parser.add_argument(
        "--data-root", type=Path, default=None,
        help="只读权威市场数据根；缺省为项目目录下 data",
    )
    parser.add_argument(
        "--synthetic", action="store_true",
        help="以配置 synthetic 段生成合成面板，不读取任何真实数据",
    )
    parser.add_argument("--device", default=None, help="auto、cpu 或 cuda")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """执行循环并打印制品摘要。"""
    arguments = build_parser().parse_args(argv)
    root = arguments.root.resolve()
    config = arguments.config
    if not config.is_absolute():
        config = root / config
    data_root = arguments.data_root
    if data_root is not None and not data_root.is_absolute():
        data_root = root / data_root
    result = run_search_loop(
        root,
        config,
        data_root=data_root,
        synthetic=bool(arguments.synthetic),
        device=arguments.device,
    )
    summary = {
        "search_run_id": result.search_run_id,
        "run_directory": str(result.run_directory),
        "manifest": str(result.manifest_path),
        "proposal": str(result.proposal_path),
        "bundle": str(result.bundle_directory),
        "search_result": str(result.result_directory),
        "parity": str(result.parity_directory),
        **dict(result.summary),
    }
    sys.stdout.write(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
