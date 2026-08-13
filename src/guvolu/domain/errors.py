"""异常层级。按具体异常处理，不统一吞没（C-03）。"""
import re

# 错误码形态见处置册
_ERROR_CODE_RE = re.compile(r"ERR-\d+")


def extract_error_code(text: str) -> str | None:
    """从错误文本提取错误码，无则返回 None。"""
    match = _ERROR_CODE_RE.search(text)
    return match.group(0) if match else None


class GuvoluError(Exception):
    """项目基础异常。"""


class ConfigError(GuvoluError):
    """配置缺失或非法。"""


class SymbolError(GuvoluError):
    """品种形态非法或不在白名单（U-02、T-09）。"""


class DryRunBlocked(GuvoluError):
    """模拟运行模式下拒绝实盘写请求（T-04）。"""


class ClockDriftError(GuvoluError):
    """本机时钟偏移超限，拒绝启动（R-05）。"""


class ApiNetworkError(GuvoluError):
    """网络层失败。写请求收到本异常后必须先查询再决策（T-06）。"""

    def __init__(self, path: str, detail: str) -> None:
        super().__init__(f"网络错误 {path}: {detail}")
        self.path = path
        self.detail = detail


class ApiTimeout(ApiNetworkError):
    """请求超时。写请求超时的处置同 T-06。"""


class ApiSchemaError(GuvoluError):
    """响应结构与官方文档核实的形态不符。"""


class ApiHttpError(GuvoluError):
    """HTTP 层非预期状态码。"""

    def __init__(self, http_status: int, path: str, detail: str = "") -> None:
        super().__init__(f"HTTP {http_status} {path} {detail}".rstrip())
        self.http_status = http_status
        self.path = path


class GmoApiError(GuvoluError):
    """业务层错误：HTTP 200 但 status 非 0（T-10）。

    错误码处置策略见 docs/error-catalog.md。
    """

    def __init__(
        self,
        codes: tuple[str, ...],
        messages: tuple[str, ...],
        path: str,
        http_status: int,
    ) -> None:
        super().__init__(f"{','.join(codes)} {path}: {'; '.join(messages)}")
        self.codes = codes
        self.messages = messages
        self.path = path
        self.http_status = http_status


class WsError(GuvoluError):
    """WebSocket 层错误，含订阅被拒的错误帧（C-09）。

    code 为错误帧提取的错误码，处置策略见 docs/error-catalog.md。
    """

    def __init__(self, message: str, code: str | None = None) -> None:
        super().__init__(message)
        self.code = code
