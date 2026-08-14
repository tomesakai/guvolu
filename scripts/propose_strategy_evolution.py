"""为一个策略流派生成下一代预登记配置提案。"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from guvolu.data.durable_io import atomic_write_text
from guvolu.research.provenance import canonical_json, sha256_text
from guvolu.research.tuning import propose_family_evolution


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
    parser.add_argument(
        "--output",
        type=Path,
        metavar="DIRECTORY",
        help="内容寻址提案与派生配置的输出目录（不是 JSON 文件路径）",
    )
    arguments = parser.parse_args(argv)
    root = arguments.root.resolve()
    config_path = arguments.config if arguments.config.is_absolute() else root / arguments.config
    monitor_path = arguments.monitor if arguments.monitor.is_absolute() else root / arguments.monitor
    proposal, proposed_config = propose_family_evolution(
        root,
        config_path,
        monitor_path,
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
    atomic_write_text(output / "latest.json", canonical_json({
        "schema_version": 1,
        "family": proposal["family"],
        "proposal_method_version": proposal["proposal_method_version"],
        "proposal": proposal_path.name,
        "proposal_sha256": proposal_hash,
        "status": proposal["status"],
        "source_monitor_sha256": proposal["source_monitor_sha256"],
        "derived_config": (
            config_path_output.name if config_path_output is not None else None
        ),
        "derived_config_sha256": (
            config_hash if config_path_output is not None else None
        ),
    }) + "\n")
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
