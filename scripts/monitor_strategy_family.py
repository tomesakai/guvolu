"""生成策略流派的参数方向与跨运行监视制品。"""
from __future__ import annotations

import argparse
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Sequence

from guvolu.data.durable_io import atomic_write_text
from guvolu.research.evolution import monitor_family_run
from guvolu.research.provenance import canonical_json, sha256_file, sha256_text


def main(argv: Sequence[str] | None = None) -> int:
    """读取研究运行并发布监视结果。"""
    parser = argparse.ArgumentParser(description="监视一个策略流派")
    parser.add_argument("family")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--summary", type=Path)
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/strategy_research.json"),
    )
    parser.add_argument("--prior-summary", action="append", type=Path, default=[])
    parser.add_argument(
        "--output",
        type=Path,
        metavar="DIRECTORY",
        help="内容寻址监视制品的输出目录（不是 JSON 文件路径）",
    )
    arguments = parser.parse_args(argv)
    root = arguments.root.resolve()
    config_path = arguments.config
    if not config_path.is_absolute():
        config_path = root / config_path
    raw_config = json.loads(config_path.read_text(encoding="utf-8"))
    if not isinstance(raw_config, Mapping):
        raise ValueError("策略研究配置必须为对象")
    config = {str(key): value for key, value in raw_config.items()}
    summary_path = arguments.summary
    if summary_path is None:
        latest_path = (
            root / "reports" / "strategy-research" / "families"
            / arguments.family / "latest.json"
        )
        latest = json.loads(latest_path.read_text(encoding="utf-8"))
        summary_path = root / str(latest["summary"])
    elif not summary_path.is_absolute():
        summary_path = root / summary_path
    prior_paths = tuple(
        path if path.is_absolute() else root / path
        for path in arguments.prior_summary
    )
    payload = monitor_family_run(
        root,
        summary_path,
        arguments.family,
        config,
        sha256_file(config_path),
        prior_paths,
    )
    content = canonical_json(payload) + "\n"
    digest = sha256_text(content)
    output = arguments.output
    if output is None:
        output = (
            root / "reports" / "strategy-research" / "monitors"
            / arguments.family
        )
    elif not output.is_absolute():
        output = root / output
    path = output / f"family-monitor-sha256-{digest}.json"
    atomic_write_text(path, content)
    print(canonical_json({
        "path": path.as_posix(),
        "sha256": digest,
        "family": arguments.family,
        "cross_run_direction": payload["cross_run_direction"],
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
