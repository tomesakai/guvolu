"""标识生成。体系定义见采纳评估第 3 节与 D-05。"""
import hashlib
import secrets
import time
import uuid

_BASE36 = "0123456789abcdefghijklmnopqrstuvwxyz"


def _base36(value: int) -> str:
    """非负整数转 36 进制。"""
    if value == 0:
        return "0"
    digits: list[str] = []
    while value > 0:
        value, rem = divmod(value, 36)
        digits.append(_BASE36[rem])
    return "".join(reversed(digits))


def new_intent_id() -> str:
    """生成下单意图标识（T-05）。

    GMO 无客户端自定义委托号（2026-08-05 官方文档核实），
    本标识仅本地留存，发送后与交易所 orderId 建立映射。
    """
    return f"it{_base36(time.time_ns() // 1_000_000)}{secrets.token_hex(3)}"


def new_correlation_id() -> str:
    """生成因果链标识，贯穿研究、执行、账本、日志。"""
    return f"co{uuid.uuid4().hex[:16]}"


def new_run_id() -> str:
    """生成进程会话标识。"""
    return f"run{_base36(time.time_ns() // 1_000_000)}{secrets.token_hex(2)}"


def sha256_hex(data: bytes) -> str:
    """内容散列，用于制品与确定性产物身份。"""
    return hashlib.sha256(data).hexdigest()
