"""只读检查 operational 与一次性 holdout 的就绪状态。"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from guvolu.research.provenance import canonical_json
from guvolu.research.readiness import strategy_readiness


def main(argv: Sequence[str] | None = None) -> int:
    """输出机器可读的 readiness 报告，不创建或消费 vintage。"""
    parser = argparse.ArgumentParser(description="检查策略研究外部数据就绪状态")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/strategy_research.json"),
    )
    parser.add_argument("--manifest", type=Path)
    arguments = parser.parse_args(argv)
    root = arguments.root.resolve()
    config = arguments.config
    if not config.is_absolute():
        config = root / config
    manifest = arguments.manifest
    if manifest is not None and not manifest.is_absolute():
        manifest = root / manifest
    print(canonical_json(strategy_readiness(root, config, manifest)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
