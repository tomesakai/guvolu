# 可视化与可观测性兼容索引

> **状态：Legacy / Superseded。** 本文件只保留旧链接兼容，不再承载现行设计。
> 文档类别：长期维护（兼容索引），登记于
> [docs/00-rules-registry.md](00-rules-registry.md)。

现行内容已迁移到以下唯一权威入口：

| 主题 | 现行文档 |
|---|---|
| 页面、图表技术栈、交互、陈旧与数据健康反馈 | [UI 设计](ui-design.md) |
| MON 盘口形态、指标、动画与质量输入 | [MON 盘口设计](mon-orderbook-design.md) |
| OFL、stacked area、缺口、缓存与前端成品契约 | [订单流数据契约](order-flow-data-contract.md) |
| 任务、低基数健康摘要、日志和恢复 | [运行时设计](runtime-ops.md) |
| 颜色、密度、布局与通用视觉规则 | [设计语言](design-language.md) |

高基数 artifact、attempt、connection、frame 和 payload SHA 只用于按需下钻，
不得作为常驻指标标签。
