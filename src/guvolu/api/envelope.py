"""响应包络处理：列表与单对象载荷的统一解包（W-07 消除重复）。"""
from __future__ import annotations

from collections.abc import Mapping

from guvolu.domain.errors import ApiSchemaError
from guvolu.domain.models import Raw


def rows(data: object) -> tuple[Raw, ...]:
    """取出列表包络。data 为列表、含 list 键对象、空对象或缺省。"""
    if isinstance(data, list):
        return tuple(item for item in data if isinstance(item, Mapping))
    if isinstance(data, Mapping):
        inner = data.get("list")
        if isinstance(inner, list):
            return tuple(item for item in inner if isinstance(item, Mapping))
    return ()


def one(data: object, path: str) -> Raw:
    """校验单对象载荷，结构不符即为缺陷。"""
    if not isinstance(data, Mapping):
        raise ApiSchemaError(f"响应结构非预期 {path}")
    return data
