"""品种类型：现物与杠杆形态由类型区分（U-02、T-09）。"""
import re

from guvolu.domain.errors import SymbolError

# 现物形态，无下划线
_SPOT_RE = re.compile(r"[A-Z0-9]{2,10}")
# 杠杆形态，_JPY 结尾
_LEVERAGE_RE = re.compile(r"[A-Z0-9]{2,10}_JPY")


class SpotSymbol(str):
    """现物品种，形如 BTC。"""

    __slots__ = ()

    def __new__(cls, value: str) -> "SpotSymbol":
        if not _SPOT_RE.fullmatch(value):
            raise SymbolError(f"非法现物品种: {value!r}")
        return super().__new__(cls, value)


class LeverageSymbol(str):
    """杠杆品种，形如 BTC_JPY。当前执行路径禁用（T-09）。"""

    __slots__ = ()

    def __new__(cls, value: str) -> "LeverageSymbol":
        if not _LEVERAGE_RE.fullmatch(value):
            raise SymbolError(f"非法杠杆品种: {value!r}")
        return super().__new__(cls, value)


Symbol = SpotSymbol | LeverageSymbol


def parse_symbol(value: str) -> Symbol:
    """按形态解析品种字符串。"""
    if value.endswith("_JPY"):
        return LeverageSymbol(value)
    return SpotSymbol(value)
