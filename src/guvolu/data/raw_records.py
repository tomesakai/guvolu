"""raw 记录兼容读取：同时支持旧 parsed payload 与新 wire payload。"""
from __future__ import annotations

import json
from collections.abc import Mapping


def ws_payload(record: Mapping[str, object]) -> Mapping[str, object] | None:
    """返回 WS 业务包络；新格式从落盘后的原始文本解析。

    `payload_raw` 保留交易所发来的完整文本并先于任何业务解析落盘；
    `payload` 是历史格式。读取侧兼容两者，迁移无需重写旧 raw。
    """
    payload = record.get("payload")
    if isinstance(payload, Mapping):
        return payload
    raw = record.get("payload_raw")
    if not isinstance(raw, str):
        return None
    try:
        loaded = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return loaded if isinstance(loaded, Mapping) else None


def ws_channel(
    record: Mapping[str, object], payload: Mapping[str, object]
) -> str:
    """返回兼容新旧 raw 的频道标识。"""
    direct = record.get("channel")
    return str(direct if direct is not None else payload.get("channel", ""))
