"""搜索束与评估的规范 JSON 散列工具，不依赖 research 包。"""
from __future__ import annotations

import hashlib
import json


def canonical_json(value: object) -> str:
    """生成确定性 JSON 文本。"""
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def sha256_text(value: str) -> str:
    """计算 UTF-8 文本散列。"""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def sha256_bytes(value: bytes) -> str:
    """计算字节散列。"""
    return hashlib.sha256(value).hexdigest()


def content_identifier(prefix: str, value: object) -> str:
    """生成带前缀的内容寻址标识。"""
    return f"{prefix}-{sha256_text(canonical_json(value))}"
