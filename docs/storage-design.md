# 本地存储设计兼容索引

> **状态：Legacy / Superseded。** 本文件只保留旧链接兼容，不再承载现行设计。
> 文档类别：长期维护（兼容索引），登记于
> [docs/00-rules-registry.md](00-rules-registry.md)。

现行内容已迁移到以下唯一权威入口：

| 主题 | 现行文档 |
|---|---|
| raw 不可变、SQLite schema v20、Parquet 与 DuckDB 边界 | [物化设计](materialization-design.md) |
| 断点、恢复、单写者、空间门禁与冷盘迁移 | [运行时设计](runtime-ops.md) |
| L2、逐笔、OFL、质量、状态与 REST anchor | [订单流数据契约](order-flow-data-contract.md) |
| 执行状态、intent 与 READ_ONLY 真相边界 | [架构台账](architecture.md) |

旧版提案中的目录、SQLite 大事实表、日增估算和任务数量均不得作为当前实现依据；
现场数值只见最新日期快照。
