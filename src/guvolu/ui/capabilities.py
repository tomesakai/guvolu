"""现有能力范围清单：查看版控制面的展示数据源。

与 docs/architecture.md 第 6 节阶段表保持一致，
阶段推进时同步更新本清单。
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Capability:
    """已具备能力条目。"""

    name: str
    detail: str
    phase: str


@dataclass(frozen=True, slots=True)
class PendingItem:
    """未具备能力条目与阻塞项。"""

    name: str
    phase: str
    blocker: str


IMPLEMENTED: tuple[Capability, ...] = (
    Capability(
        name="公开行情读取",
        detail="サービス稼働状態、最新レート、板情報、逐笔成交、KLine、取引ルール",
        phase="1",
    ),
    Capability(
        name="账户只读",
        detail="資産残高、余力情報、取引高、四类入出金履历、委托、挂单、成交、建玉",
        phase="1",
    ),
    Capability(
        name="实时推送客户端",
        detail="公开三频道与私有四频道的 WS 客户端，含重连与共享限速，常驻接入属阶段 4",
        phase="1",
    ),
    Capability(
        name="交易客户端",
        detail="下单、改单、三类撤单。模拟运行缺省拦截建仓类写请求，实盘未启用",
        phase="1",
    ),
    Capability(
        name="紧急停止开关",
        detail="命令行全品种撤单，独立于策略进程，任何模式可用",
        phase="2",
    ),
    Capability(
        name="安全与治理",
        detail="签名含 ws-auth 例外、限速自律、写留痕与令牌脱敏、时钟偏移校验",
        phase="1",
    ),
    Capability(
        name="查询服务与查看版控制面",
        detail="只读监控：模式、服务状态、资产、挂单、成交、K 线。零密钥、仅本机",
        phase="6-pre",
    ),
    Capability(
        name="足迹图",
        detail="现物全品种，2018-09-05 起官方逐笔归档；当期 bar 由录制流增量聚合",
        phase="6-pre",
    ),
    Capability(
        name="订单流",
        detail="盘口热力瓦片金字塔（挂量末态与净增挂、净撤减、成交消耗分解）、"
        "底部指标带、档带追踪、区域判读、成交刻线。判读是判定不是事实",
        phase="6-pre",
    ),
    Capability(
        name="报警",
        detail="规则实例匹配区域判读事件产生报警，列表查看与确认回填。"
        "无声无弹窗，确认仅改呈现状态，无任何交易语义",
        phase="6-pre",
    ),
)

PENDING: tuple[PendingItem, ...] = (
    PendingItem(
        name="行情采集与 raw 层落盘",
        phase="3",
        blocker="存储选型 · 落盘格式 · 采集粒度",
    ),
    PendingItem(
        name="对账状态机与状态持久化",
        phase="4",
        blocker="对账周期 · 恢复流程",
    ),
    PendingItem(
        name="风控闸门与模拟运行执行器", phase="5", blocker="熔断阈值数值"
    ),
    PendingItem(
        name="操作面与运维进程（紧急停止按钮化）", phase="6b", blocker="阶段 6a"
    ),
    PendingItem(name="回测框架", phase="7", blocker="回测引擎形态"),
    PendingItem(name="首个策略（网格）", phase="8", blocker="阶段 5、7"),
    PendingItem(
        name="CUDA 因子研究管线", phase="9", blocker="GPU 栈与硬件，交易先行"
    ),
)
