# ETH 跨品种预注册复现与搜索循环裁决快照（2026-09-03）

本文冻结 2026-09-03 在 GMO ETH 面板上完成的两组研究结论：以 BTC 基准网格
预注册复现冻结计划家族，以及搜索循环提案在准入扩展下的裁决。运行身份以
内容寻址 run 目录为准，管线版本 `strategy-research-pipeline-v14`，准入扩展
定义见[策略研究契约](strategy-research.md)第 4.1 节，静态快照做法见
[runtime-ops.md](runtime-ops.md)第 8.2 节。前序跨所与 XRP 复现见
[2026-09-01 快照](2026-09-01-cross-venue-replication-snapshot.md)。

## 1. 数据与运行清单

数据为 `D:\dev\guvolu-research-snapshot-eth\data` 静态快照（由
`scripts/build_research_snapshot.py` 自生产数据根构建，GMO ETH 历史成交
92 个月分区加 13 个按日合并的实时头，面板起点 2019-02-01，截止
2026-08-23T09:00Z，与 BTC holdout vintage 起点之前对齐）。

| run（前缀） | 配置 | 候选数 | 试验计数 | 代码身份 |
|---|---|---|---|---|
| `research-run-6215f76b` | `strategy_research_eth_replication.json`（BTC 基准网格换市场，准入扩展） | trend 6、price_breakout 3 | 无搜索试验 | 决策级 |
| `research-run-6aecaf31` | `strategy_research_candidate_db98e4059291.json`（搜索循环提升） | trend 9 | 搜索 2,160，Galwey 有效 68.7 | 非决策级（提升配置写入后树未提交） |

搜索循环运行为 `search-run-1559ca6e`（8,462 候选，仅 trend 有提案）。

## 2. 预注册复现结果

BTC 基准网格在 ETH 上不搜参直接验证，两家族均通过全部门禁（含基准超额门
与折冠军众数部署规则）：

| 指标 | price_breakout | trend |
|---|---|---|
| 拼接 OOS Sharpe | 0.965 | 0.943 |
| 部署候选 OOS Sharpe | 1.088 | 1.145 |
| 最大回撤 | 0.312 | 0.332 |
| FDR q | 0.026 | 0.026 |
| PBO | 0.039 | 0.172 |
| bootstrap p | 0.010 | 0.013 |
| DSR（有效计数） | 0.988 | 0.975 |
| DSR（原始计数） | 0.957 | 0.865 |
| 固定多头基准 Sharpe | 0.513 | 0.513 |
| 基准超额 | 0.452 | 0.431 |
| 折冠军众数 | lookback 168、vol 0.4 | lookback 168、entry 0.5、exit 0、vol 0.4 |
| 众数选中率 | 0.84 | 0.88 |

ETH 各折训练冠军的众数恰为 BTC 冻结计划登记的两个候选，说明冻结参数在
独立品种上复现，且不是 ETH 上重新选参的结果。BTC 同网格的对应数字见
2026-09-01 快照第 2 节（price_breakout 0.964 与 1.179，trend 0.780 与 0.918）。

## 3. 搜索循环提案裁决

| 面板 | 提案 | 拼接 Sharpe | Galwey 有效试验数 | DSR | 裁决 |
|---|---|---|---|---|---|
| GMO BTC（`research-run-01d89582`） | trend 312、entry 1.5、vol 0.2 | 1.274 | 70.2 | 0.941 | 不合格 |
| GMO ETH（`research-run-6aecaf31`） | trend 144、entry 1.5、exit 0.25、vol 0.2 | 1.219 | 68.7 | 0.939 | 不合格 |

两个面板上的提案都在计入搜索试验后差 0.01 落在 DSR 门之外，且提案参数
互不相同；预注册网格反而在两个品种上同参数通过。结论：继续在单一面板上
搜参没有信息增量，跨品种预注册复现是当前最强的证据形态。

## 4. 后续

后继冻结计划候选应以预注册网格的跨品种一致性为准入维度；下一步在 XRP
与 SOL（GMO 历史成交自 2019 与 2023 年起）重复第 2 节流程，四个品种同参数
的联合准入由多品种 GPU 评估承担。
