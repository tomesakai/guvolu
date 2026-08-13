# 07 统计验证、试验计数与数据治理

版本：1.1-draft  
日期：2026-08-05  
文档性质：规范性  
主责：统计研究负责人  
前置文档：06_evaluation_pipeline.md、10_registry_lifecycle.md  
主要消费者：Search、Registry、Release Review  

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


## 1. 数据使用等级

| Scope | 可反复使用 | 可进入适应度 | 失败后可继续针对性修改 |
|---|---:|---:|---:|
| `DEV_ADAPTIVE` | 是 | 是 | 是 |
| `INTERNAL_CV` | 是 | 是 | 是 |
| `FROZEN_GATE` | 候选族冻结后一次 | 否 | 只能生成新版本并等待新 gate |
| `HOLDOUT_VINTAGE` | 永久只消费一次 | 否 | 否 |
| `LIVE_FORWARD` | 前瞻产生 | 否 | 版本化处理 |

“只否决不奖励”不能消除自适应；只要 pass/fail 决定繁殖，数据就属于 adaptive。

## 2. 试验账本

区分：

- `HypothesisTrial`：独特 AST+参数参与选择；
- `EvaluationReplicate`：CPCV、bootstrap、噪声等重复评估；
- `Replay`：同 evaluation_id 的确定性重放；
- `CacheReuse`：结果复用；
- `InvalidatedRun`：代码/数据错误后作废但保留。

所有 LLM、随机注入、参数变体和未到 F3 的候选都计入完整选择过程。DSR 原论文要求考虑未选择试验、独立试验数、Sharpe 方差、样本长度、偏度与峰度。[R-DSR]

## 3. CPCV

`DECISION`：当前 12 块、留 2 块的 66 组合称为“组合净化时段稳定性评估”，不称为 PBO/CSCV。

每个样本显式拥有：

```text
feature_interval
label_interval
information_interval = union
```

训练样本的信息区间与测试区间重叠即 purge。Embargo 比例是 `POLICY`，需保存敏感度结果。

所有方向、标准化和参数选择只在训练部分拟合。

## 4. PBO/CSCV

若启用原始 CSCV/PBO：

- 输入为同步 `T×N` 候选收益矩阵；
- 时间切成偶数 S 段；
- 每次选 S/2 训练、互补 S/2 测试；
- S=12 时组合数 `C(12,6)=924`。

它与 66 个留二组合是独立统计模块。[R-PBO]

## 5. DSR

报告：

```text
raw_hypothesis_count
estimated_effective_count
DSR(raw_count)
DSR(effective_count)
effective-count sensitivity
```

相关性聚类只能估计有效试验数，不能删除原始计数。只有 F3 候选参与聚类会低估选择规模，因此禁止。

## 6. 参数敏感度

参数域按类型定义：Window、Decay、Threshold、Constant、Categorical。统一 ±30% 被删除。

流程：

1. Morris 用于排序与病态筛查；
2. Sobol 使用当前 SALib API；不含二阶时样本数 `N(D+2)`，含二阶时 `N(2D+2)`。[R-SALIB-SOBOL]
3. 高原检验按合法域和离散邻域；
4. 参数无效优先触发表达式简化，不自动拒绝因子；
5. 极小变化引起方向翻转、孤峰或边界异常才是拒绝证据。

敏感度若反复反馈搜索，必须标为 adaptive；若作为 frozen gate，则候选冻结后只测试一次。

## 7. 合成与 null 检验

### 路径重采样

Stationary bootstrap 使用随机几何块长，适用于依赖时间序列的重采样推断。[R-STATIONARY-BOOTSTRAP]

所有标的和字段同步使用同一时间块索引，保留同期截面结构。块长由依赖结构或自动选择方法决定，不能固定声称 20–60 最优。

### Null surrogate

逐标的独立 IAAFT 不足以保持多变量交叉结构。null 的推荐用途是运行完整搜索器，得到“纯 null 条件下最佳候选”的分布，而不是要求真实因子在被破坏机制的 surrogate 上继续盈利。

### 端到端 null replay

```text
for each null panel:
  使用同样种群、代数、F0–F3、参数选择和 LLM 配额
  记录搜索器最佳得分
empirical p = (1 + count(null_best >= real_best)) / (K + 1)
```

K 决定 p 值分辨率，是算术，不是性能保证。

## 8. 封存 vintage

封存按不重叠新数据段消费：

```text
vintage_id
start/end
sealed_at
consumed_at
candidate_version
verdict
```

已经解封的数据永久标记 consumed。若失败后修改因子，新版本不得继续使用同一 vintage 声称样本外。

## 9. 决策输出

`StatDecision` 包含：

- 输入 evaluation IDs；
- data scopes；
- CPCV 分布；
- DSR inputs/outputs；
- PBO（如启用）；
- sensitivity；
- null replay；
- holdout verdict；
- policy version；
- reviewer/automation identity。

## 10. 待研究

- `TO-RESEARCH ST-01`：有效试验数估计方法和相关性矩阵稳定性；需模拟校准。
- `POLICY ST-02`：DSR、PBO、coverage、holdout 的门槛。
- `TO-RESEARCH ST-03`：多变量 surrogate 算法与风格化事实验收。
- `TO-RESEARCH ST-04`：不同市场制度下 stationary bootstrap 分段方式。
- `TO-RESEARCH ST-05`：六个月还是多个季度 vintage；取决于非重叠观测数与交易频率。
