# 15 v1.0 到 v1.1 变更说明

版本：1.1-draft  
日期：2026-08-05  
文档性质：说明性  
主责：架构负责人  
前置文档：原 v1.0 文档  
主要消费者：评审者、迁移负责人  

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


## 1. 总体变化

v1.0 的目标与术语被保留，但“完整实现文档”改为证据分级的模块化规格。原文的全局一致性声明、7.1GB 固定显存、统一融合 kernel、预设吞吐和“封存+DSR 等于真正样本外”等表述被撤销。

## 2. 模块映射

| v1.0 | v1.1 |
|---|---|
| M1 数据 | Domain Semantics + Data Platform |
| M2 合成行情 | Statistics Governance 的路径/null/场景三类模块 |
| M3 表达式 | Expression Compiler + CPU Reference |
| M4 进化 | Search & Evolution |
| M5 GPU 求值 | Expression Planner + GPU Runtime + Evaluation Pipeline |
| M6 参数敏感度 | Statistics Governance/Sensitivity |
| M7 统计验证 | Statistics Governance + Trial Ledger |
| M8 成本模拟 | Portfolio & Execution + TCA |
| M9 LLM | Search 的可选 proposal/review 子模块 |
| M10 存储展示 | Ledger & Registry + Visualization & Observability |

## 3. 关键替换

- 双布局常驻 → 时间主序 SoA 默认，资产主序按基准派生；
- label rank 常驻 → label order/tie + common-valid 子集重排；
- 8 字节 bytecode → 16 字节 typed instruction + parameter table；
- 单一 512 线程融合核 → E0/E1/E2 时序后端 + E3 截面后端；
- 寄存器环形缓冲 → 显式 global scratch；
- 同 kernel CSE 写读 → 独立 materialization stage；
- 每候选 43,800 行“逐日”缓冲 → F1 aggregate、F2 D 行、F3 微批；
- F3 候选试验计数 → 全选择过程 hypothesis ledger；
- overlap 滚动封存 → 不重叠 vintage 一次消费；
- 合成行情统一 gate → path bootstrap、null search、scenario stress 分开；
- 成本公式 → 完整 target/order/fill/accounting/TCA；
- 通用监控页 → Research/Execution/Ops 三产品面。

## 4. 保留项

- 类型化 AST；
- CPU 双精度参考思想；
- 计数器型随机数和可重放；
- NSGA-II/MAP-Elites 作为候选搜索选项；
- CPCV 用于内部时段稳定性；
- DSR 作为多重试验校正的一部分；
- 因子血统与生命周期；
- 单卡首版和 L2 不进入进化内环。

## 5. 兼容性

v1.0 的 `factor_id=u64 shash`、`PanelPair`、`Instr{u8...}`、`DailyStats` 和 M8 cost output 不向前兼容。迁移工具应把旧记录导入为 `legacy`，保存原始表达式和结果，不伪造新的 evaluation_id。

## 契约补强

- 新增 `domain.proto`，避免各模块重复定义 instrument、universe、clock 和 metric。
- 新增 `registry.proto`，把统计裁定、发布和状态迁移从数据库表结构中解耦。
- 新增 `services.proto`，明确 GPU、Execution、Registry 的异步 RPC；长任务统一使用 `OperationRef`。
- 执行契约补齐 AccountingEvent、Cash、PortfolioSnapshot、Reconciliation 和 TCA 分解。
- MetricValue 增加明确状态，禁止以 0 代替未计算或无效值。
