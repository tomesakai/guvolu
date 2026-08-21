"""发布不重选候选的策略成本敏感性制品。"""
from __future__ import annotations

import argparse
import hashlib
from pathlib import Path
from typing import Sequence

from guvolu.data.durable_io import atomic_write_bytes
from guvolu.research.cost_sensitivity import (
    build_fixed_target_cost_sensitivity,
    cost_sensitivity_bytes,
)
from guvolu.research.provenance import canonical_json


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="扫描已固定部署/逐折目标路径的成本余量",
    )
    parser.add_argument("family")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument(
        "--cost-bps",
        type=float,
        nargs="+",
        default=(0.0, 5.0, 10.0, 20.0, 30.0, 40.0, 50.0, 60.0),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("reports/strategy-research/cost-sensitivity"),
    )
    arguments = parser.parse_args(argv)
    root = arguments.root.resolve()
    manifest = arguments.manifest
    if not manifest.is_absolute():
        manifest = root / manifest
    output = arguments.output
    if not output.is_absolute():
        output = root / output
    payload = build_fixed_target_cost_sensitivity(
        root, manifest, arguments.family, arguments.cost_bps,
    )
    body = cost_sensitivity_bytes(payload)
    digest = hashlib.sha256(body).hexdigest()
    output.mkdir(parents=True, exist_ok=True)
    path = output / f"fixed-cost-sensitivity-sha256-{digest}.json"
    atomic_write_bytes(path, body)
    print(canonical_json({
        "family": arguments.family,
        "path": path.as_posix(),
        "sha256": digest,
        "results": payload["results"],
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
