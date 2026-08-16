"""dry-run 执行器命令行封装，逻辑见 execution.dry_run_executor。"""
from __future__ import annotations

from guvolu.execution.dry_run_executor import main

if __name__ == "__main__":
    raise SystemExit(main())
