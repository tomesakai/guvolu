"""为多节拍研究冻结一个可复用的最小数据根。"""
from __future__ import annotations

import argparse
from pathlib import Path

from guvolu.research.suite_data_snapshot import create_suite_data_snapshot


def main() -> None:
    parser = argparse.ArgumentParser(description="冻结多节拍研究输入数据根")
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--market", required=True)
    parser.add_argument(
        "--shadow-market", action="append", default=[],
        help="完整管线需要冻结的 L2/cross-venue 市场；可重复传入",
    )
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    snapshot = create_suite_data_snapshot(
        arguments.data_root,
        arguments.market,
        arguments.output,
        tuple(arguments.shadow_market),
    )
    print(snapshot)


if __name__ == "__main__":
    main()
