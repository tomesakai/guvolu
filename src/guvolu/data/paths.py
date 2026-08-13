"""数据根目录的唯一解析入口。"""
from __future__ import annotations

import os
from pathlib import Path


def data_root() -> Path:
    """读取可选的 ``GUVOLU_DATA_ROOT``，缺省保持仓库内 ``data/``。"""
    configured = os.environ.get("GUVOLU_DATA_ROOT", "").strip()
    if not configured:
        env_file = Path(".env")
        if env_file.exists():
            for line in env_file.read_text(encoding="utf-8").splitlines():
                key, separator, value = line.partition("=")
                if separator and key.strip() == "GUVOLU_DATA_ROOT":
                    configured = value.strip().strip('"')
                    break
    return Path(configured) if configured else Path("data")
