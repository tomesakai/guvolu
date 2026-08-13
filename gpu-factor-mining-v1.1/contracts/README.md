# 契约目录

版本：1.1-draft

本目录定义进程边界、持久化边界和跨团队交付边界。Rust 内部 trait 可以更具体，但不得改变这里的时间、单位、身份、事件和错误语义。

## 文件边界

| 文件 | 唯一职责 |
|---|---|
| `common.proto` | 元数据、哈希、定点数、制品、异步操作、命令确认和错误 |
| `domain.proto` | 标的、产品、市场规则、动态宇宙、决策时钟、指标定义 |
| `research.proto` | 面板、表达式、编译计划、评估、资源报告、试验记录 |
| `execution.proto` | 信号、目标仓位、风险、订单、成交、会计、持仓、对账、TCA |
| `registry.proto` | 统计裁定、因子版本、状态迁移、SelectionView、vintage 消费 |
| `services.proto` | `gpu-worker`、`executiond`、`registry` 的进程级 RPC |
| `api_contracts.md` | 兼容性、幂等、长任务、错误、制品和浏览器 API 规则 |

## 原则

1. 控制消息使用 Protobuf；大数组使用 `ArtifactRef` 指向 Arrow/Parquet/压缩二进制。
2. 时间统一 UTC epoch nanoseconds；业务时间字段必须标明 event/available/decision/execution 语义。
3. 订单价格、数量、现金使用 `FixedDecimal`；研究数组可使用浮点。
4. 所有事实事件有 `event_id`、`correlation_id`、`causation_id`、schema version 和 payload hash。
5. 写操作必须支持 idempotency key；消费者必须处理重复和乱序。
6. breaking change 增加 package major 或新 message；字段编号删除后 `reserved`，永不复用。
7. enum 消费者必须容忍未知值并进入 `UNSPECIFIED/UNKNOWN` 路径。
8. Artifact 必须先校验 hash、size、media type 和 schema 再使用。
9. 事实事件 append-only；状态由确定性 fold 或 read model 生成。
10. 错误包含稳定 code、retryable、correlation_id 和结构化 details。
11. 长任务返回 `OperationRef`；调用者轮询或订阅，不以超长同步 RPC 占住连接。
12. 真实交易命令与研究评估命令使用不同服务身份和网络权限。

详见 `api_contracts.md`。
