"""运行或复核独立 L2 被动网格 shadow。"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from guvolu.research.passive_grid_shadow import (
    run_passive_grid_shadow,
    verify_passive_grid_shadow,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", type=Path, default=Path.cwd())
    parser.add_argument("--data-root", type=Path)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--verify", metavar="RUN_ID")
    parser.add_argument(
        "--full",
        action="store_true",
        help="输出完整输入身份和候选制品摘要",
    )
    args = parser.parse_args()
    result = (
        verify_passive_grid_shadow(
            args.repository,
            args.verify,
            data_root=args.data_root,
        )
        if args.verify
        else run_passive_grid_shadow(
            args.repository,
            data_root=args.data_root,
            config_path=args.config,
        )
    )
    if args.verify or args.full:
        output = result
    else:
        output = {
            "run_id": result["run_id"],
            "quality": result["quality"],
            "evolution_monitor": result["evolution_monitor"],
            "capital_weight": result["capital_weight"],
            "promotion_eligible": result["promotion_eligible"],
            "promotion_blockers": result["promotion_blockers"],
            "candidates": result["candidates"],
        }
    print(json.dumps(output, ensure_ascii=False, sort_keys=True, indent=2))


if __name__ == "__main__":
    main()
