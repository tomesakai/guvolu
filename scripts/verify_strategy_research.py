"""复核最近一次或指定策略研究运行。"""
from __future__ import annotations

import argparse
from pathlib import Path

from guvolu.research.provenance import canonical_json
from guvolu.research.verification import verify_research_run


def main() -> None:
    """执行只读制品复核。"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--manifest", type=Path)
    arguments = parser.parse_args()
    result = verify_research_run(arguments.root, arguments.manifest)
    print(canonical_json({
        "run_id": result.run_id,
        "manifest_path": result.manifest_path.as_posix(),
        "manifest_sha256": result.manifest_sha256,
        "checked_artifacts": list(result.checked_artifacts),
    }))


if __name__ == "__main__":
    main()
