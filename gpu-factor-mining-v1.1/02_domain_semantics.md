# 02 领域语义、时间、标签与市场契约

版本：1.1-draft  
日期：2026-08-05  
文档性质：规范性  
主责：量化研究负责人 + 数据负责人  
前置文档：01_system_boundaries.md  
主要消费者：Data、Compiler、Evaluation、Execution、Statistics  

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


## 1. 唯一领域词汇

系统统一使用以下实体：

- `Instrument`：不可变内部标的；ticker、交易对和交易所代码只是带有效期的别名；
- `MarketProfile`：日历、执行时段、估值、费用、做空、资金费率和公司行动规则；
- `Observation`：具有事件时间和可用时间的数据；
- `DecisionPoint`：系统允许形成信号的离散时点；
- `SignalFrame`：某一决策点、共同有效集合上的分数；
- `TargetPortfolio`：约束后目标仓位；
- `OrderIntent`：从当前仓位到目标仓位所需的交易意图；
- `Evaluation`：固定候选、数据、语义、成本和代码版本的一次评估；
- `HypothesisTrial`：参与选择的独特候选假设；
- `Vintage`：只允许作为样本外门槛消费一次的新到数据段。

## 2. 时间四元组

`DECISION`：所有数据记录至少包含：

```text
event_time      经济事件实际发生时刻
available_time  研究或交易系统最早可合法得知该值的时刻
ingest_time     系统接收该值的时刻
revision_id     同一事件的修订版本
```

合法读取条件是：

```text
available_time <= decision_time
```

数组索引 `t` 不再承担前视防护。所有时间为 UTC epoch nanoseconds；UI 可按用户时区显示，但不可改变计算时区。

## 3. 基准决策语义

`DECISION`：首版采用**非重叠日频决策**。

```text
DecisionPoint[d]
  -> 使用截至 decision_time[d] 已可用的数据
  -> 生成 SignalFrame[d]
  -> 在 execution_window[d] 中成交
  -> 持有/盯市至下一 DecisionPoint
```

加密货币基准可定义每日固定 UTC 时点；ETF 必须使用交易所日历与明确的开盘/收盘/竞价窗口，禁止用 `t % 24` 推断交易日。

原文的“每小时产生 h=24 标签”与“日频调仓”被分离。若未来支持每小时形成 24 小时重叠持仓，必须新增 cohort 持仓模型，不能复用日频收益定义。

## 4. 标签定义

标签仅用于预测质量，不等于可交易组合收益。

```text
label_return[d, i]
= reference_exit_price[d, i]
/ reference_entry_price[d, i] - 1
```

`reference_entry_price`、`reference_exit_price` 和价格调整方式必须由 `LabelSpec` 明确。任何使用真实执行价的组合收益由 Execution 域计算。

标签方向、标准化、winsorization 等可拟合变换必须仅在训练段拟合，并随评估记录保存。

## 5. Spearman 共同有效集合

`DECISION`：每个候选、每个决策点重新构造：

```text
common_valid
= factor_valid
∩ label_valid
∩ universe_eligible
∩ evaluation_mask
```

标签不能只在全宇宙预计算固定 rank 后直接删掉无效标的。数据层预计算 `label_order` 和 tie group；评估层在 `common_valid` 子集内重分配 midrank。

排序语义：

- 无效值排除；
- 相同值使用 midrank；
- Top-K 平分时按 `instrument_id` 升序稳定决胜；
- 有效标的不足配置的 `min_cs_count` 时指标无效；
- 有效标的不足 K 时 Top-K 指标无效，不静默缩小 K。

## 6. Point-in-time 宇宙

```text
universe[d]
= listed_at(d)
∩ sufficient_history(d)
∩ liquidity_rule_using_data_available_before(d)
∩ tradable_at(d)
∩ product_scope
```

必须保留后来退市、清算、归零、改名或迁移的标的。宇宙规则输出版本化的 `UniverseSnapshot`，而不是动态 SQL 的隐含结果。

## 7. 单位与货币

每个字段声明：

```text
shape          Scalar / TimeSeries / CrossSection / EventSeries
unit           Price(CCY) / Return / Quantity / Notional(CCY) / Rate / Dimensionless
frequency      Hourly / DecisionOnly / EventDriven
availability   CloseAvailable / Delayed / PublishedEvent
missing_policy StrictInvalid / MinPeriods(n) / AsOfKnownValue / ExplicitFill
```

编译器静态拒绝不同单位的非法加减、未转换货币的金额比较和事件字段的无定义连续广播。

订单、成交、现金和数量使用定点十进制，不使用 f32/f64 作为事实存储。研究面板和 GPU 中间量可使用浮点。

## 8. 市场配置

### 8.1 加密货币

配置至少包含：

- venue、spot/linear perpetual/inverse perpetual；
- tick、step、min quantity、min notional；
- mark/index/last 的估值用途；
- funding event 的实际时刻、interval、cap/floor；
- 上线、迁移、下架与结算；
- 交易所维护和数据中断。

资金费率不能硬编码八小时；交易所可提供可变 `fundingIntervalHours`。[R-BINANCE-FUNDING]

### 8.2 ETF

配置至少包含：

- 交易所日历与竞价时段；
- raw/adjusted price 与公司行动；
- NAV、iNAV、PCF 的发布时间；
- 分红、拆分、合并、清盘；
- 做空可得性、借券费、交易单位和价格档位；
- 底层市场与 ETF 交易时段错位。

TSE 当前现货竞价为 09:00–11:30、12:30–15:30；该事实应来自版本化交易日历而不是代码常量。[R-JPX-HOURS]

## 9. 年化时钟

`AnnualizationClock` 由 MarketProfile 提供：

- 加密货币日频可按实际观测日数；
- ETF 按交易日数；
- 小时或事件级收益不可直接套用 252 或 365；
- 所有 Sharpe 输出必须同时保存未年化均值、标准差、观测数和年化因子。

## 10. 待研究

- `TO-RESEARCH DS-01`：加密货币基准决策时点采用 UTC 00:00、流动性更高时段还是多时点 ensemble；需避免数据供应商日界偏差。
- `TO-RESEARCH DS-02`：ETF 基准执行选择开盘竞价、开盘后窗口或收盘竞价；取决于数据许可与真实券商能力。
- `TO-RESEARCH DS-03`：复权价格与现金分红会计的唯一事实源；在数据供应商确定后冻结。
- `TO-RESEARCH DS-04`：跨币种组合的 FX 可用时间和估值源。
