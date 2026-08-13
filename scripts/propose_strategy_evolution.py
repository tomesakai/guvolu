"""为一个策略流派生成下一代预登记配置提案。"""
from __future__ import annotations

import argparse
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Sequence

from guvolu.data.durable_io import atomic_write_text
from guvolu.research.provenance import canonical_json, sha256_file, sha256_text
from guvolu.research.tuning import propose_family_evolution


def _load_object(path: Path) -> Mapping[str, object]:
    """读取 JSON 对象。"""
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, Mapping):
        raise ValueError(f"JSON 必须为对象: {path}")
    return {str(key): item for key, item in value.items()}


def main(argv: Sequence[str] | None = None) -> int:
    """生成提案和可选派生配置。"""
    parser = argparse.ArgumentParser(description="生成策略流派进化提案")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--monitor", type=Path, required=True)
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/strategy_research.json"),
    )
    parser.add_argument("--output", type=Path)
    arguments = parser.parse_args(argv)
    root = arguments.root.resolve()
    config_path = arguments.config if arguments.config.is_absolute() else root / arguments.config
    monitor_path = arguments.monitor if arguments.monitor.is_absolute() else root / arguments.monitor
    config = _load_object(config_path)
    monitor = _load_object(monitor_path)
    parent_hash = sha256_file(config_path)
    proposal, proposed_config = propose_family_evolution(
        config,
        monitor,
        parent_hash,
    )
    output = arguments.output
    if output is None:
        output = (
            root / "reports" / "strategy-research" / "evolution-proposals"
            / str(proposal["family"])
        )
    elif not output.is_absolute():
        output = root / output
    proposal_content = canonical_json(proposal) + "\n"
    proposal_hash = sha256_text(proposal_content)
    proposal_path = output / f"proposal-sha256-{proposal_hash}.json"
    atomic_write_text(proposal_path, proposal_content)
    config_path_output: Path | None = None
    if proposed_config is not None:
        config_content = canonical_json(proposed_config) + "\n"
        config_hash = sha256_text(config_content)
        config_path_output = output / f"strategy-research-sha256-{config_hash}.json"
        atomic_write_text(config_path_output, config_content)
    print(canonical_json({
        "proposal": proposal_path.as_posix(),
        "status": proposal["status"],
        "derived_config": (
            config_path_output.as_posix() if config_path_output is not None else None
        ),
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
