"""服务状态门禁单测（R-03、C-15）。全部离线（C-13）。"""
from __future__ import annotations

import pytest

from guvolu.domain.enums import ServiceStatus
from guvolu.risk.errors import ServiceNotOpen
from guvolu.risk.service_gate import (
    allows_cancel,
    allows_new_intent,
    ensure_open_for_new_intent,
)


def test_open_allows_new_intent() -> None:
    """OPEN 允许生成新意图。"""
    assert allows_new_intent(ServiceStatus.OPEN)
    ensure_open_for_new_intent(ServiceStatus.OPEN)


@pytest.mark.parametrize(
    "status", [ServiceStatus.MAINTENANCE, ServiceStatus.PREOPEN]
)
def test_non_open_rejects_new_intent(status: ServiceStatus) -> None:
    """非 OPEN 拒绝新意图（R-03）。"""
    assert not allows_new_intent(status)
    with pytest.raises(ServiceNotOpen, match=status.value):
        ensure_open_for_new_intent(status)


@pytest.mark.parametrize(
    "status",
    [ServiceStatus.OPEN, ServiceStatus.MAINTENANCE, ServiceStatus.PREOPEN],
)
def test_cancel_allowed_in_any_status(status: ServiceStatus) -> None:
    """撤单不受门禁限制，紧急路径可达（T-07）。"""
    assert allows_cancel(status)
