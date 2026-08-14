"""创建跨节拍冻结前向计划。"""
from __future__ import annotations

import argparse
from dataclasses import asdict
from pathlib import Path

from guvolu.research.interval_suite_forward import (
    freeze_interval_suite_forward_plan,
)
from guvolu.research.provenance import canonical_json


def main() -> None:
    parser = argparse.ArgumentParser(description="管理多节拍冻结前向计划")
    parser.add_argument("vintage_id")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--config", type=Path, action="append", required=True)
    parser.add_argument("--manifest", type=Path, action="append", required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--registry", type=Path)
    arguments = parser.parse_args()
    root = arguments.root.resolve()

    def resolve(path: Path) -> Path:
        return path if path.is_absolute() else root / path

    result = freeze_interval_suite_forward_plan(
        root,
        tuple(resolve(path) for path in arguments.config),
        tuple(resolve(path) for path in arguments.manifest),
        resolve(arguments.evidence),
        arguments.vintage_id,
        None if arguments.registry is None else resolve(arguments.registry),
    )
    payload = asdict(result)
    payload["plan_path"] = result.plan_path.as_posix()
    print(canonical_json(payload))


if __name__ == "__main__":
    main()
