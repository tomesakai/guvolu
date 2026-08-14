"""只读检查多节拍研究套件的 operational 与 promotion 就绪状态。"""
from __future__ import annotations

import argparse
from pathlib import Path

from guvolu.research.interval_suite_readiness import (
    interval_suite_readiness,
    persist_interval_suite_readiness,
)
from guvolu.research.provenance import canonical_json


def main() -> None:
    parser = argparse.ArgumentParser(description="检查多节拍策略套件就绪状态")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--config", type=Path, action="append", required=True)
    parser.add_argument("--manifest", type=Path, action="append", required=True)
    parser.add_argument("--output-directory", type=Path)
    parser.add_argument("--suite-registry", type=Path)
    arguments = parser.parse_args()
    root = arguments.root.resolve()

    def resolve(path: Path) -> Path:
        return path if path.is_absolute() else root / path

    result = interval_suite_readiness(
        root,
        tuple(resolve(path) for path in arguments.config),
        tuple(resolve(path) for path in arguments.manifest),
        suite_registry_path=(
            None if arguments.suite_registry is None
            else resolve(arguments.suite_registry)
        ),
    )
    output = persist_interval_suite_readiness(
        root, result, arguments.output_directory,
    )
    print(canonical_json(result))
    print(output)


if __name__ == "__main__":
    main()
