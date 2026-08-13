"""为一个或多个策略流派生成内容寻址候选注册表。"""
from __future__ import annotations

import argparse
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Sequence

from guvolu.data.durable_io import atomic_write_text
from guvolu.research.provenance import canonical_json, sha256_file, sha256_text
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
    raw_config = json.loads(config_path.read_text(encoding="utf-8"))
    if not isinstance(raw_config, Mapping):
        raise ValueError("策略研究配置必须为对象")
    config = {str(key): value for key, value in raw_config.items()}
    batches = build_family_batches(config, arguments.families)
    payload = candidate_registry_payload(batches, sha256_file(config_path))
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
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
