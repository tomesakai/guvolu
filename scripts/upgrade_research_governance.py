"""显式备份并升级研究治理库的 schema 写入上限。"""
from __future__ import annotations

import argparse
from pathlib import Path

from guvolu.research.governance import upgrade_governance_write_ceiling
from guvolu.research.provenance import canonical_json, sha256_file


def main() -> None:
    """仅在调用方给出旧版本、旧上限和新备份路径时执行迁移。"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--backup", type=Path, required=True)
    parser.add_argument("--expected-version", type=int, required=True)
    parser.add_argument("--expected-write-ceiling", type=int, required=True)
    arguments = parser.parse_args()
    backup = upgrade_governance_write_ceiling(
        arguments.registry,
        arguments.backup,
        expected_version=arguments.expected_version,
        expected_write_ceiling=arguments.expected_write_ceiling,
    )
    print(canonical_json({
        "registry": arguments.registry.resolve().as_posix(),
        "backup": backup.as_posix(),
        "backup_sha256": sha256_file(backup),
    }))


if __name__ == "__main__":
    main()
