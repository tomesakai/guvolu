# 0 号文档：编号与文档登记册

> 全仓库编号域与文档清单的唯一登记处（W-01、W-02）。
> 一致性由 [tests/test_doc_compliance.py](../tests/test_doc_compliance.py) 自动校验（W-03）。
> 本册只登记，不复述规则内容（W-07）。

## 1. 编号域登记

| 前缀 | 域 | 所属文档 | 现行最大编号 | 已废弃编号 |
|---|---|---|---|---|
| T | 铁律 | SKILLS.md | 13 | 无 |
| R | 风控 | SKILLS.md | 8 | 无 |
| C | 代码 | SKILLS.md | 15 | 无 |
| D | 数据架构 | SKILLS.md | 10 | 无 |
| X | 控制面 | SKILLS.md | 9 | 无 |
| G | 研究管线 | SKILLS.md | 8 | 无 |
| U | 用语 | SKILLS.md | 7 | 无 |
| A | 代理协作 | SKILLS.md | 6 | 无 |
| W | 文书 | SKILLS.md | 8 | 无 |
| TBD | 未决项 | docs/architecture.md | 39 | 无 |

## 2. 文档清单

| 路径 | 类别 | 说明 |
|---|---|---|
| SKILLS.md | 长期维护 | 最高约束，规则正文唯一所在 |
| CLAUDE.md | 长期维护 | 入口索引 |
| docs/00-rules-registry.md | 长期维护 | 本册 |
| docs/architecture.md | 长期维护 | 架构锁定项与 TBD 台账 |
| docs/error-catalog.md | 长期维护 | 错误码对照与处置册 |
| docs/2026-08-05-gmo-api-capability-report.md | 时效快照 | GMO API 能力调查 |
| docs/2026-08-05-gpu-factor-mining-adoption.md | 时效快照 | 因子挖掘资料采纳评估 |
| docs/2026-08-05-gmo-order-id-erratum.md | 时效快照 | 委托标识勘误与参数补充核实 |
| docs/2026-08-06-gmo-data-scope-survey.md | 时效快照 | GMO 数据范围与量级实测 |
| docs/2026-08-07-multi-venue-api-survey.md | 时效快照 | 多交易所 API 能力调查 |
| docs/2026-08-07-bitflyer-api-verification.md | 时效快照 | bitFlyer API 能力实测 |
| docs/2026-08-07-gmo-trade-print-semantics.md | 时效快照 | GMO 逐笔打印口径与官方归档实测 |
| docs/2026-08-09-bitflyer-lightning-ui-study.md | 时效快照 | bitFlyer Lightning 交易界面考察 |
| docs/2026-08-08-bitbank-api-verification.md | 时效快照 | bitbank API 能力实测 |
| docs/2026-08-09-btc-alert-verification.md | 时效快照 | BTC 报警数据准确性复核、遗漏扫描与视觉核验 |
| docs/2026-08-10-multi-source-api-validation.md | 时效快照 | 多来源 API、历史盘口、归一化与信号台账完整验证 |
| docs/2026-08-11-multi-source-closure-validation.md | 时效快照 | 三家闭环、Binance 校验归档与日元旁路实测 |
| docs/2026-08-11-all-venue-data-status.md | 时效快照 | 全来源数据类型、物化验收、主力/fallback 与有限度总表 |
| docs/2026-08-11-live-l2-kline-jpy-rollout.md | 时效快照 | 三所实时 L2、GMO K 线、日元市场扩展进度与来源职责 |
| docs/2026-08-11-okx-l2-sample-validation.md | 时效快照 | OKX 历史 L2 日档、重放语义、持久化与物化闭环验证 |
| docs/2026-08-12-l2-l3-quality-report.md | 时效快照 | L2 质量审计、L3 契约、聚合边界与迁移计划 |
| docs/2026-08-13-l2-quality-and-l3-readiness.md | 时效快照 | DB20、L2 v5、quality/status/anchor、OKX live、跨所顶档、OFL v8 与 L3 合同验收 |
| docs/2026-08-13-data-strategy-execution-roadmap.md | 时效快照 | 多场所数据、L2/L3、研究策略与执行路线只读审计 |
| docs/2026-08-13-data-platform-strategy-audit-review.md | 时效快照 | 多场所数据平台、运行缺陷、L3、策略优先级与 soft-gating 复核审计 |
| docs/2026-08-13-production-rollout.md | 时效快照 | 市场数据生产运行与验证记录 |
| docs/2026-08-15-framework-review.md | 时效快照 | 全框架评审、总图落位、代码文档偏差与运行现场偏差 |
| docs/2026-08-17-execution-link-verification.md | 时效快照 | 执行链路验证性实盘的授权、端点与结论记录 |
| docs/2026-08-22-decision-io-contract-v2.md | 时效快照 | 决策生成 I/O 契约 v2 提案（TBD-39 提案载体） |
| docs/2026-08-22-gpu-searchfast-architecture.md | 时效快照 | GPU SearchFast 架构，TBD-18/19/20 实施案 |
| docs/2026-08-22-theory-system-and-fastest-live-path.md | 时效快照 | v7 栈切换后现场、分层理论体系与最快实盘路径 |
| docs/2026-08-11-account-assets-capability.md | 时效快照 | GMO Coin 与 bitFlyer 私有资产接口复核 |
| docs/2026-08-10-persistence-audit.md | 时效快照 | 持久化完整性、恢复探针与历史损坏边界审计 |
| docs/ui-design.md | 长期维护 | 控制面与可视化设计（TBD-12 至 17 提案载体） |
| docs/storage-design.md | 长期维护 | Legacy 存储设计兼容索引；现行内容已迁移至物化、运行与订单流文档 |
| docs/materialization-design.md | 长期维护 | P2 分析物化、键链、Parquet 与 DuckDB 边界 |
| docs/multi-source-data-design.md | 长期维护 | Legacy 多源设计兼容索引；现行内容已迁移至能力、物化、订单流与接口文档 |
| docs/venue-capability-matrix.md | 长期维护 | 来源能力对照册，唯一现行对照 |
| docs/venue-api-reference.md | 长期维护 | 来源接口细节与公开行情采集设计 |
| docs/multi-venue-adapters.md | 长期维护 | Legacy 多所适配器兼容索引；现行内容已迁移至来源接口与能力文档 |
| docs/design-language.md | 长期维护 | 控制面设计语言（外部风格规范的消化改制） |
| docs/visualization-observability.md | 长期维护 | Legacy 可视化/可观测性兼容索引；现行内容已迁移至 UI、MON、订单流与运行文档 |
| docs/mon-orderbook-design.md | 长期维护 | MON 盘口结构、指标与动画改造设计 |
| docs/footprint-design.md | 长期维护 | 足迹图与盘口判读设计（TBD-28 至 30 提案载体） |
| docs/runtime-ops.md | 长期维护 | 运行时保活与进程操作设计（TBD-31 提案载体） |
| docs/order-flow-data-contract.md | 长期维护 | 订单流事实、回补边界、长期任务与前端反馈契约 |
| docs/strategy-research.md | 长期维护 | PIT 策略研究、样本外验证、组合分配与 paper 目标位置契约 |
| docs/llm-pipeline-design.md | 长期维护 | LLM 决策管线接入设计（TBD-34 提案载体） |
| .claude/skills/guvolu-rules/SKILL.md | 长期维护 | 自动加载的规则摘要 |

## 3. 外部引入资料

| 目录 | 说明 | 完整性 |
|---|---|---|
| gpu-factor-mining-v1.1/ | GPU 因子挖掘系统设计资料，外部引入，不按本仓库文书规范撰写 | 目录内 MANIFEST.json 记录逐文件 SHA-256 |
| outsides/ | 金融终端 UI 风格规范（fintech-terminal-style），外部引入 | 原样保全，消化产物为 docs/design-language.md |
| docs/evidence/crypto_api_l3_registry_2026-08-12.json | L3 来源工作簿的文件身份、工作表与端点登记 manifest；工作簿不入仓 | SHA-256、字节数、工作表数、端点数与重复/公式错误检查齐全 |

外部资料保持原样，不套用 W 章样式规则，两套合规脚本均将其排除；其采纳结论以时效快照形式登记于第 2 节清单。

## 4. 变更流程

新增规则：在所属文档追加下一编号，更新第 1 节最大编号，运行合规校验。
废弃规则：原文标注「已废弃」并保留正文，编号登记入第 1 节废弃列，编号不复用。
新增文档：登记入第 2 节。时效快照命名 `YYYY-MM-DD-主题.md`，内容冻结，修订以新快照发布并更新各处引用。
