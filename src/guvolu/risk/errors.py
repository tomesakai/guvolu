"""风控域异常。按具体异常处理，不统一吞没（C-03）。"""
from guvolu.domain.errors import GuvoluError


class RiskError(GuvoluError):
    """风控域基础异常。"""


class LimitExceeded(RiskError):
    """限额超限。调用方必须按 T-11 触发熔断处置。"""


class LimitAdjustmentRejected(RiskError):
    """限额调整被拒：只可调低，不可调高（T-11、X-05）。"""


class ServiceNotOpen(RiskError):
    """服务状态非 OPEN，拒绝生成新意图（R-03）。"""


class CircuitTripped(RiskError):
    """熔断已触发，拒绝生成新意图（R-02）。"""
