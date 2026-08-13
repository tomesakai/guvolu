# 09 进化搜索、MAP-Elites 与 LLM 边界

版本：1.1-draft  
日期：2026-08-05  
文档性质：规范性  
主责：搜索算法负责人  
前置文档：04_expression_compiler.md、06_evaluation_pipeline.md、10_registry_lifecycle.md  
主要消费者：Research Coordinator  

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


## 1. 边界

Search 只生成、繁殖和选择候选。它不定义指标、不直接访问 holdout、不计算成本、不写因子最终状态。

## 2. 候选生成

来源：

- 类型安全随机生成；
- 节点/参数/子树变异；
- 类型安全子树交叉；
- 子树提升与简化；
- 已冻结 LLM proposal artifact。

所有产物先经 Compiler canonicalization；相同 candidate_id 复用历史 evaluation，不产生新 hypothesis。

## 3. 多目标

建议目标：

```text
maximize adaptive_robust_score
minimize measured_complexity
minimize library_redundancy
```

`measured_complexity` 结合 AST、state/materialized bytes、barriers 和实测 latency。相关性只使用 Evaluation 明确定义的 signal_corr 或 residual score。

目标集合和权重是 `POLICY`，版本化进入 SearchPolicy。

## 4. MAP-Elites

描述符必须在其真实生产阶段之后才可用：

- turnover：F3；
- signal persistence：F3 score matrix；
- drawdown：F3 net returns。

不可在 F1 伪造这些描述符。格子边界通过历史分布校准；报告 reachable cells 和 occupied/reachable coverage，不以固定 80% 作为正确性门槛。

## 5. 繁殖配比

v1.0 的 60/30/10 改为 `SearchPolicy` 默认实验值。需要与纯随机、新颖度优先和无 LLM 版本做消融。

## 6. LLM

LLM 是 proposal generator，不是事实裁判。保存：

```text
provider/model identifier
prompt template hash
full prompt
raw response
parsed AST
sampling parameters
proposal artifact hash
```

重放不重新调用模型。经济逻辑审查输出报告和证据缺口；自动硬否决需冻结规则或人工审批，不能依赖会漂移的远程回答。

## 7. 选择反馈

Search 读取 Registry 发布的不可变 `SelectionView`：

```text
candidate_id
policy_version
metric_summary
stage_reached
selection_rank
novelty_cell
```

Search 不通过数据库联表自行重算。

## 8. 停止条件

停止条件是 `POLICY`：

- evaluation budget；
- null-calibrated improvement；
- 多样性收敛；
- GPU/数据预算；
- 人工 research campaign 边界。

不能以“一夜 200–500 代”作为架构事实。

## 9. 待研究

- `TO-RESEARCH SE-01`：NSGA-II、lexicase、novelty search 或混合选择的效果；
- `TO-RESEARCH SE-02`：LLM 注入真实增量，需固定预算消融；
- `TO-RESEARCH SE-03`：MAP-Elites 描述符与格子边界；
- `TO-RESEARCH SE-04`：候选族定义用于 frozen gate 与试验计数；
- `TO-RESEARCH SE-05`：表达式简化和语义等价类的搜索偏差。
