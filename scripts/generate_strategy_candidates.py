"""为一个或多个策略流派生成内容寻址候选注册表。"""
from __future__ import annotations

import argparse
from collections.abc import Mapping
from pathlib import Path
from typing import Sequence

from guvolu.data.durable_io import atomic_write_text
from guvolu.research.config_lineage import load_governed_strategy_config
from guvolu.research.provenance import canonical_json, sha256_text
from guvolu.strategy.generation import build_family_batches, candidate_registry_payload


def main(argv: Sequence[str] | None = None) -> int:
    """执行独立候选生成。"""
    parser = argparse.ArgumentParser(description="生成策略候选注册表")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/strategy_research.json"),
    )
    parser.add_argument("--family", action="append", dest="families")
    parser.add_argument("--output", type=Path)
    arguments = parser.parse_args(argv)
    root = arguments.root.resolve()
    config_path = arguments.config
    if not config_path.is_absolute():
        config_path = root / config_path
    config, config_hash, _lineage_root_hash, _lineage_depth = (
        load_governed_strategy_config(root, config_path)
    )
    batches = build_family_batches(config, arguments.families)
    payload = candidate_registry_payload(batches, config_hash)
    search_plan = payload.get("search_plan")
    if not isinstance(search_plan, Mapping):
        raise ValueError("候选注册表缺少 search_plan")
    content = canonical_json(payload) + "\n"
    digest = sha256_text(content)
    output = arguments.output
    if output is None:
        scope = "all" if arguments.families is None else "-".join(
            sorted(set(arguments.families))
        )
        output = root / "reports" / "strategy-research" / "candidates" / scope
    elif not output.is_absolute():
        output = root / output
    path = output / f"candidate-registry-sha256-{digest}.json"
    atomic_write_text(path, content)
    print(canonical_json({
        "path": path.as_posix(),
        "sha256": digest,
        "family_scope": payload["family_scope"],
        "candidate_count": payload["candidate_count"],
        "search_plan_id": search_plan["search_plan_id"],
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
