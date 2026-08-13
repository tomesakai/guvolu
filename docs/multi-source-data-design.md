# 多源数据设计兼容索引

> **状态：Legacy / Superseded。** 本文件只保留旧链接兼容，不再承载现行设计。
> 文档类别：长期维护（兼容索引），登记于
> [docs/00-rules-registry.md](00-rules-registry.md)。

现行内容已迁移到以下唯一权威入口：

| 主题 | 现行文档 |
|---|---|
| `market_id + artifact_id + normalization_version`、PIT 与活动 head | [物化设计](materialization-design.md) |
| 来源能力、主力、同所回填、服务 fallback 与跨所聚合口径 | [来源能力对照册](venue-capability-matrix.md) |
| 来源事实隔离、订单流选择、质量与 REST anchor | [订单流数据契约](order-flow-data-contract.md) |
| 适配器、游标、限速、数组次序与来源描述符 | [来源接口设计](venue-api-reference.md) |
| 执行域不多源与只读适配器边界 | [架构台账](architecture.md) |

跨所顶档已有读取期 PIT/quorum 聚合，但尚无活动聚合制品；旧提案不得被解释为
已实现成交量、VWAP、FX 或回测级持久化聚合。
