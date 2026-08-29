"""服务状态门禁（R-03）。

非 OPEN 拒绝生成新意图。撤单不经本门禁，与紧急停止开关
口径一致（T-07 紧急路径必须随时可达）；维护期交易所拒绝
撤单时由错误处置承担。
"""
from __future__ import annotations

from guvolu.domain.enums import ServiceStatus
from guvolu.risk.errors import ServiceNotOpen


def allows_new_intent(status: ServiceStatus) -> bool:
    """仅 OPEN 允许生成新意图（R-03）。"""
    return status is ServiceStatus.OPEN


def allows_cancel(status: ServiceStatus) -> bool:
    """撤单只减少风险，任何服务状态均允许（T-07）。"""
    return True


def ensure_open_for_new_intent(status: ServiceStatus) -> None:
    """非 OPEN 即抛错拒绝新意图（R-03）。"""
    if not allows_new_intent(status):
        raise ServiceNotOpen(f"服务状态 {status.value} 拒绝新意图")
