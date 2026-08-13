# 14 待研究事项、证据缺口与决策门

版本：1.1-draft  
日期：2026-08-05  
文档性质：治理性  
主责：架构委员会  
前置文档：全部文档  
主要消费者：研究和工程负责人  

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


## 1. 使用方式

本文件收集不能百分百明确的事项。每项必须有 owner、需要的证据、阻塞的 gate 和保守默认。完成研究后写 ADR，并从本表移除或标记 closed。

## 2. 高优先级阻断项

| ID | 问题 | 需要证据 | 阻塞 | 保守默认 |
|---|---|---|---|---|
| R-GPU-01 | RTX 5070 各 E0–E3 实际吞吐/occupancy/spill | 目标机 Nsight/ptxas | Gate C | 无吞吐承诺 |
| R-TIME-01 | 加密货币决策日界和执行窗口 | 多交易所流动性/数据可用性 | Gate D/E | UTC 固定日界、非重叠 |
| R-ETF-01 | ETF 执行基准与数据许可 | 交易所/供应商/券商 | ETF scope | ETF 功能禁用 |
| R-L2-01 | 是否具备完整增量簿和 sequence | 数据样本/许可/恢复测试 | B3 | 只提供 B0/B1/B2 |
| R-COST-01 | impact/participation 参数 | 真实/纸面成交标定 | F3 release | 保守固定成本，标记 POLICY |
| R-STAT-01 | effective trial count | 模拟与敏感度 | Gate F | 同时报 raw/effective，不放宽 raw |
| R-NULL-01 | null surrogate 多变量方法 | 风格化事实/交叉结构测试 | Gate F | stationary bootstrap + 简单 null |
| R-UI-01 | L2 renderer 技术和性能 | WebGL2/PixiJS spike | Execution Console | 无 L2 页面或低分辨率 |

## 3. 中优先级研究项

- 参数域、Morris 轨迹数、Sobol N 和二阶项；
- F1 proxy 对 F3 排名保真；
- score 压缩与相关性误差；
- MAP-Elites 描述符和 reachable cells；
- LLM proposal 的消融与审查可靠性；
- PostgreSQL/SQLite 本地双实现；
- NATS JetStream 是否需要；
- FactorVersion 签名/RBAC；
- 多因子净额容量与风险模型；
- ETF 借券、税费、结算和公司行动；
- 真实 venue 的 dead-man switch、限速和 reconciliation。

## 4. 决策模板

```text
Research ID:
Question:
Current conservative behavior:
Data/source versions:
Experiment design:
Acceptance criteria:
Results and uncertainty:
Decision:
Affected contracts:
Migration plan:
ADR:
```

## 5. 不允许的处理

- 用“行业惯例”替代数据或官方规则；
- 把单次 benchmark 当作所有表达式吞吐；
- 把项目阈值写成统计定理；
- 因为前端需要而反向修改指标定义；
- 因数据不足仍将 snapshot walk-the-book 称为 L2 replay；
- 反复使用已消费 vintage 并继续称样本外。
