# GPU 加速因子挖掘系统 v1.1 文档集

本目录是对 v1.0 单体设计文档的工程化重构。目标不是增加文档数量，而是将容易相互污染的领域拆开，使数据、编译器、GPU、统计、执行、搜索、注册表和前端团队能够依靠稳定契约并行开发。

版本：1.1-draft  
日期：2026-08-05


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


## 最重要的结构调整

1. 采用**模块化单体优先**，而不是把每个模块做成微服务。只有 GPU 资源隔离、真实交易安全隔离和前端查询边界形成独立进程。
2. 将原 M5 拆成表达式编译器、GPU 运行时和评估流水线三个域；“一个万能融合 kernel”不再是系统前提。
3. 将原 M8 扩展成独立的组合与执行域，覆盖目标仓位、风险、订单、成交、持仓、会计、TCA、纸面交易和真实适配器。
4. 将原 M10 拆成试验账本/因子注册表与可视化/可观测性两个域。
5. 所有大数组通过内容寻址的 `ArtifactRef` 传递；控制面使用版本化 Protobuf，不在 RPC 中传输整块因子矩阵。
6. 所有不能从官方资料或确定性算术得出的性能与阈值，明确标记为 `TO-BENCHMARK`、`POLICY` 或 `TO-RESEARCH`。

## 阅读顺序

| 顺序 | 文档 | 用途 |
|---:|---|---|
| 0 | `00_master_spec_v1.1.md` | 合并后的完整新文档 |
| 1 | `01_system_boundaries.md` | 系统边界、模块大小、依赖规则、进程拓扑 |
| 2 | `02_domain_semantics.md` | 时间、标签、宇宙、市场与单位的唯一语义 |
| 3 | `03_data_platform.md` | 原始数据、PIT 数据、面板、数据质量与存储 |
| 4 | `04_expression_compiler.md` | AST、算子、身份、字节码、执行计划与 CPU 参考 |
| 5 | `05_gpu_runtime.md` | E0–E3 后端、显存、数值、资源闸门与目标机基准 |
| 6 | `06_evaluation_pipeline.md` | F0–F3、IC、信号、组合输入与指标输出 |
| 7 | `07_statistics_governance.md` | CPCV、DSR、PBO、敏感度、null replay、封存 vintage |
| 8 | `08_portfolio_execution.md` | 组合构建、订单状态、模拟、真实执行、安全与 TCA |
| 9 | `09_search_evolution.md` | NSGA-II、MAP-Elites、繁殖与 LLM 边界 |
| 10 | `10_registry_lifecycle.md` | 试验账本、因子注册表、血统和生命周期 |
| 11 | `11_visualization_observability.md` | 研究、交易、回放、GPU 与运维可视化 |
| 12 | `12_testing_benchmark.md` | 正确性、统计、执行、GPU、UI 与发布门槛 |
| 13 | `13_engineering_work_packages.md` | 工程阶段、分工、并行关系与交付契约 |
| 14 | `14_research_backlog.md` | 尚不能百分百明确的研究项与决策门 |
| 15 | `15_change_log.md` | v1.0 到 v1.1 的逐项变动 |
| 16 | `16_references.md` | 官方文档、标准和原始论文 |

## 契约目录

- `contracts/common.proto`：通用元数据、定点十进制、制品引用、异步操作和错误。
- `contracts/domain.proto`：标的、市场、动态宇宙、决策时钟和指标定义。
- `contracts/research.proto`：面板、表达式、执行计划、评估结果与试验账本。
- `contracts/execution.proto`：信号、目标仓位、风险、订单、成交、持仓、会计、对账和 TCA。
- `contracts/registry.proto`：统计裁定、FactorVersion、生命周期、SelectionView 与 vintage 消费。
- `contracts/services.proto`：跨进程命令、查询、事件流和长任务 RPC。
- `contracts/api_contracts.md`：兼容性、幂等、异步操作、错误和批量数据传输规范。

## ADR

ADR 固化会影响多个域的决定。正文不能绕过 ADR 改变这些结论：

- `ADR-0001-modular-monolith.md`
- `ADR-0002-non-overlapping-daily-decisions.md`
- `ADR-0003-dual-gpu-backends.md`
- `ADR-0004-execution-safety-boundary.md`
- `ADR-0005-visualization-stack.md`

## 规范性边界

`00_master_spec_v1.1.md` 是便于通读的合并视图。若合并视图与拆分文档不同，以拆分文档和契约文件为准。所有实现 PR 必须指出所满足的文档章节、契约版本和测试向量。
