# 契约版本、幂等、错误与批量数据传输

版本：1.1-draft  
日期：2026-08-05  
文档性质：规范性  
主责：平台架构负责人  
前置文档：common.proto、research.proto、execution.proto  
主要消费者：所有进程  

外部依据编号见 `16_references.md`。原始 v1.0 文档为本次重构的需求基线。


## 证据与决策状态

本文档集不把尚未实现或尚未实测的内容伪装成事实。关键结论使用以下状态：

| 标记 | 含义 | 可否直接作为实现事实 |
|---|---|---|
| `VERIFIED-SOURCE` | 已由官方文档、标准或原始论文核验 | 可以，但仍需固定引用版本 |
| `VERIFIED-CALC` | 可由确定性算术、组合数学或数据规模直接推导 | 可以 |
| `DECISION` | 本项目已经选择的架构语义 | 可以，变更须走 ADR |
| `POLICY` | 项目阈值或治理规则，不是普适真理 | 可以配置，不得宣称外部有效性 |
| `TO-BENCHMARK` | 只能在目标 RTX 5070、驱动、CUDA 与操作系统组合上测得 | 不可以预填性能结论 |
| `TO-RESEARCH` | 证据不足或依赖尚未选定的数据源、市场、券商或交易所 | 不可以进入生产默认值 |

出现冲突时，优先级为：正式契约与 ADR > 本文档正文 > 示例代码。外部资料只证明其明确支持的事实，不自动证明本项目的设计阈值。


## 1. 兼容性

- additive optional field：minor 兼容；
- 改变字段语义/单位：breaking，新增 message/package major；
- 字段编号删除后 `reserved`，永不复用；
- consumer 遇到未知 enum 必须保留原始数字并降级，不得崩溃；
- Artifact schema 与控制 message 分别版本化。

## 2. 幂等

写命令包含 `idempotency_key`。服务端保存 key→result hash；同 key 同 payload 返回原结果，不同 payload 返回 `IDEMPOTENCY_CONFLICT`。

OrderEvent/FillEvent 使用 event_id、venue sequence 和 client/venue IDs 去重；乱序事件进入缓冲或 reconciliation，不覆盖较新事实。

## 3. 大制品

Protobuf 只传 `ArtifactRef`。推荐 media types：

```text
application/vnd.apache.arrow.file
application/vnd.apache.arrow.stream
application/x-parquet
application/octet-stream; schema=gpu-stage-v1
```

读取步骤：resolve locator → 校验 size/hash → 校验 schema → 建立 mmap/stream → 使用。locator 可变化，sha256 是身份。

## 4. 错误分类

```text
DATA_*
CONTRACT_*
EXPRESSION_*
COMPILE_*
GPU_*
EVALUATION_*
STATISTICS_*
RISK_*
ORDER_*
VENUE_*
ACCOUNTING_*
REGISTRY_*
```

错误声明 retryable；调用方不得按 message 文本判断重试。

## 5. 传输

- researchd↔gpu-worker：本地 RPC + ArtifactRef；transport `TO-RESEARCH`；
- api-server↔browser：HTTP/Arrow + WebSocket event stream；
- executiond 事件：append-only log；是否采用 JetStream `TO-RESEARCH`；
- live adapter：按 venue 协议，但内部统一 execution.proto。

## 6. 安全

- control message 不含 API secret；
- live 命令需要 factor version、portfolio policy、operator arm token；
- read API 与 trade API 使用不同身份与网络权限；
- correlation_id 贯穿研究、执行、账本和 telemetry。

## 7. 长任务状态机

GPU 评估和执行回放不得依赖长时间同步 RPC。服务先返回 `OperationRef`，状态只能按下列路径迁移：

```text
QUEUED -> RUNNING -> SUCCEEDED
QUEUED -> CANCEL_REQUESTED -> CANCELLED
RUNNING -> CANCEL_REQUESTED -> CANCELLED
QUEUED/RUNNING -> FAILED
```

取消是请求而不是事实；只有 worker 确认后才进入 `CANCELLED`。成功结果只通过带 hash 的 `ArtifactRef` 或结构化小消息返回。

## 8. 进程级服务边界

| 服务 | 写权限 | 不允许依赖 | 失败边界 |
|---|---|---|---|
| `GpuWorkerService` | 仅本地作业/缓存 | Registry DB、统计政策、交易凭证 | CUDA 失败不得改研究事实 |
| `ExecutionService` | 订单、成交、会计事件 | Search、自由 AST、holdout | LIVE 凭证仅在 executiond |
| `RegistryService` | Trial/Result/Factor 状态 | GPU 内部、venue API | 只写已验证契约 |
| Browser Query API | 无事实写权限 | Artifact 内部路径、交易凭证 | 只读 read model |

研究模块与执行模块通过 `SignalFrame`、`TargetPortfolio`、`ExecutionEnvelope` 和 `ArtifactRef` 连接；不得共享内存中的可变对象。

## 9. 命令与事件

- 命令表示意图，可被拒绝；事件表示已经发生的事实。
- `SubmitOrder` 返回接受命令的 `CommandAck`，不代表交易所已确认；真实状态只能来自 `OrderEvent`。
- `OrderEvent.resulting_status` 与 `event_type` 分离；状态由事件流 fold。
- `stream_sequence` 只保证本系统运行内的重放顺序，venue sequence 仍需单独保存。
- 断线重连使用 `after_sequence`；缺口必须触发 reconciliation，不得猜测。

## 10. 浏览器查询契约

REST/Arrow 查询保留在 `api-server`，不把 gRPC 服务直接暴露给浏览器。每个数值响应必须带：definition version、unit、timezone、gross/net、validity 和 downsample method。对 timeline/L2 使用 viewport + resolution/tile 请求；UI 不自行重算正式指标。
