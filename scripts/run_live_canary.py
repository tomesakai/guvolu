"""最小实盘 canary 命令行封装，逻辑见 execution.live_canary。"""
from __future__ import annotations

from guvolu.execution.live_canary import main

if __name__ == "__main__":
    raise SystemExit(main())
