"""复核多节拍研究成员并生成全局 FDR 与相关性证据。"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from guvolu.data.durable_io import atomic_write_text
from guvolu.research.interval_suite import build_interval_suite_plan
from guvolu.research.interval_suite_evidence import evaluate_interval_suite


def main() -> None:
    parser = argparse.ArgumentParser(description="评价多节拍策略研究套件")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--config", type=Path, action="append", required=True)
    parser.add_argument("--manifest", type=Path, action="append", required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    root = arguments.root.resolve()

    def resolve(path: Path) -> Path:
        return path if path.is_absolute() else root / path

    plan = build_interval_suite_plan(
        root, tuple(resolve(path) for path in arguments.config),
    )
    evidence = evaluate_interval_suite(
        root, plan, tuple(resolve(path) for path in arguments.manifest),
    )
    output = resolve(arguments.output)
    atomic_write_text(output, json.dumps(
        evidence,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ) + "\n")
    print(output)


if __name__ == "__main__":
    main()
