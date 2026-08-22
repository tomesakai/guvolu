"""生成不执行回测的多节拍策略研究套件计划。"""
from __future__ import annotations

import argparse
from pathlib import Path

from guvolu.data.durable_io import atomic_write_text
from guvolu.research.interval_suite import (
    build_interval_suite_plan,
    interval_suite_plan_text,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="生成多节拍统一试验域")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument(
        "--config", type=Path, action="append", required=True,
        help="重复传入至少两个已登记配置",
    )
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    root = arguments.root.resolve()
    paths = [
        path if path.is_absolute() else root / path
        for path in arguments.config
    ]
    plan = build_interval_suite_plan(root, paths)
    output = arguments.output
    if not output.is_absolute():
        output = root / output
    atomic_write_text(output, interval_suite_plan_text(plan))
    print(output)


if __name__ == "__main__":
    main()
