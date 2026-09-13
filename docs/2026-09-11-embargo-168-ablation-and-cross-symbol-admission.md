# embargo 168 消融与多品种联合准入（2026-09-11）

> 文档类别：时效快照，登记于 [docs/00-rules-registry.md](00-rules-registry.md)。
> 范围：把走前验证的 embargo 由 24 柱提到 168 柱（不低于最长特征回看窗）后，在 GMO BTC、ETH、XRP、SOL 四个静态快照上重跑研究，与 9 月初基线并排；同时交付多品种联合准入汇总脚本与每周实盘成本汇总任务。不改冻结计划，不消费封存段（G-08），不触碰交易端点（G-01）。阈值全部为配置（G-06），本文只记录当日事实。

## 1. 做法

| 项 | 取值 |
|---|---|
| 派生配置 | `config/strategy_research_embargo168_<btc,eth,xrp,sol>.json`，各自复制基线只改 `walk_forward.embargo_bars`，不带 `evolution_parent`（该块保留给监视驱动的演进配置，`tuning.verify_evolution_config` 要求 `source_monitor_path`） |
| 基线配置 | BTC `strategy_research.json`（散列 66590f5b，即冻结计划来源运行的配置）；ETH、XRP、SOL 为各自 `*_replication.json` |
| 数据 | `D:\dev\guvolu-research-snapshot{,-eth,-xrp,-sol}\data` 静态快照，面板截止 `2026-08-23T09:00Z`，早于封存段起点 |
| 代码身份 | 主仓 `882edd0`，工作树干净，八个运行全部决策级 |
| 两组范围 | 两家族（`--family price_breakout --family trend`，与基线同口径，9 候选）；全流派（六家族，37 候选） |
| 用时 | BTC 全流派 14 分钟、两家族 8 分钟；ETH 10 与 4 分钟；XRP 约 8 与 3 分钟；SOL 3 与 1 分钟；均以低优先级运行 |

运行清单：

| 品种 | 基线（embargo 24，两家族） | embargo 168 两家族 | embargo 168 全流派 |
|---|---|---|---|
| BTC | `research-run-14c57fe7` | `research-run-4f74a106` | `research-run-ee9d380a` |
| ETH | `research-run-6215f76b` | `research-run-0b0c3ede` | `research-run-885f1e1c` |
| XRP | `research-run-7b3387f6` | `research-run-51ebf57b` | `research-run-2cf1172d` |
| SOL | `research-run-542c2b27` | `research-run-b6687692` | `research-run-ce1c0c18` |

## 2. 两家族范围：embargo 24 对 168

| 家族 | 品种 | Sharpe 24 | Sharpe 168 | 回撤 | PBO 24 | PBO 168 | DSR 168 | 基准超额 168 | 联合 24 | 联合 168 |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|---|
| price_breakout | BTC | 1.102 | 1.022 | 0.268 | 0.006 | 0.008 | 0.995 | 0.413 | 可 | 可 |
| price_breakout | ETH | 0.965 | 0.978 | 0.287 | 0.039 | 0.023 | 0.991 | 0.433 | 可 | 可 |
| price_breakout | XRP | 0.733 | 0.713 | 0.553 | 0.377 | 0.357 | 0.951 | 0.462 | 不可 | 不可 |
| price_breakout | SOL | 0.259 | 0.241 | 0.265 | 0.000 | 0.000 | 0.624 | 0.646 | 不可 | 不可 |
| trend | BTC | 0.772 | 0.735 | 0.432 | 0.320 | 0.346 | 0.956 | 0.126 | 可 | 可 |
| trend | ETH | 0.943 | 0.968 | 0.313 | 0.172 | 0.137 | 0.981 | 0.424 | 可 | 可 |
| trend | XRP | 1.043 | 1.005 | 0.452 | 0.314 | 0.287 | 0.987 | 0.754 | 不可 | 不可 |
| trend | SOL | 0.704 | 0.724 | 0.211 | 0.000 | 0.100 | 0.829 | 1.129 | 不可 | 不可 |

联合判定用统一阈值：回撤上限 0.45、基准超额下限 0.05、PBO 上限 0.4、FDR q 上限 0.2、DSR 下限 0.95、块自助 p 上限 0.05，与 `strategy_research.json` 的 `validation` 一致。XRP 的两家族失败原因不变：price_breakout 回撤 0.553 与 PBO，trend 只差回撤 0.452 对 0.45。SOL 样本三年、7 折，FDR q 接近 1。

## 3. 全流派范围下的结果

| 家族 | 品种 | Sharpe | 回撤 | PBO | DSR | 基准超额 | 联合 |
|---|---|---:|---:|---:|---:|---:|---|
| breakout | BTC | 1.078 | 0.276 | 0.061 | 0.996 | 0.469 | 可 |
| breakout | ETH | 0.998 | 0.267 | 0.090 | 0.991 | 0.454 | 可 |
| breakout | XRP | 0.778 | 0.533 | 0.436 | 0.964 | 0.527 | 不可 |
| breakout | SOL | 0.165 | 0.287 | 0.000 | 0.586 | 0.570 | 不可 |
| flow_trend | BTC | 0.652 | 0.353 | 0.369 | 0.950 | 0.043 | 不可 |
| flow_trend | ETH | 0.819 | 0.287 | 0.516 | 0.974 | 0.275 | 不可 |
| flow_trend | XRP | 1.024 | 0.463 | 0.850 | 0.995 | 0.773 | 不可 |
| flow_trend | SOL | 0.623 | 0.226 | 0.000 | 0.793 | 1.028 | 不可 |

`breakout`（量能确认突破）的部署参数在 BTC 与 ETH 上相同：回看 168、`flow_confirmation` 负 0.1、波动目标 0.4。`mean_reversion` 与 `grid_shadow` 在四个品种上样本外 Sharpe 均为负，未列。全流派范围的 FDR q 因试验数增加（1,924 至 1,998 对 468 至 486）而略高于两家族范围，但没有改变任何家族的准入结论。

## 4. 判断

1. embargo 从 24 提到 168 的影响很小：BTC 两家族 Sharpe 下降 0.04 至 0.08，ETH 反而上升 0.01 至 0.03，XRP 与 SOL 变动在 0.04 以内；没有任何准入结论翻转。此前担心的样本外泄漏在本管线的折结构下并不显著。
2. `breakout` 是本轮新出现的候选：在两个长样本品种上以同一参数通过全部门禁，与 `price_breakout` 的跨品种形态一致（BTC 与 ETH 通过，XRP 因回撤与 PBO 失败，SOL 样本不足）。它未进入任何冻结计划；进入后继计划前还需要成本校准后的重评与多品种联合准入。
3. XRP `trend` 在统一口径下仍差回撤 0.002。回撤上限是配置阈值（G-06），是否调整属提案，本文不改。
4. 冻结计划与封存段不动（G-08）。当前实盘继续用 08-23 冻结的 trend 与 price_breakout。

## 5. 交付的工具与任务

| 项 | 内容 |
|---|---|
| `scripts/summarize_cross_symbol_admission.py` | 读若干研究运行目录的 `summary.json`，按统一阈值输出 Markdown 表与 JSON；阈值由命令行给出；只读 |
| `scripts/run_live_cost_summary.ps1`、`scripts/register_live_cost_summary_task.ps1` | 每周日 07:00 JST 以执行仓 READ_ONLY 密钥汇总 live 成交成本到 `guvolu-exec\data\execution\live\cost-summary\`；任务 `guvolu-live-cost-summary` 已登记，首次试跑成功 |
| 成本现状 | 实盘成交 2 笔、名义 504 円，合计成本 43 bp，其中 1 円取整占主要部分；累计 20 笔后再把 `cost_model` 四项假设改成实测值 |

## 6. 教训

- 研究运行结束时复核代码身份，运行期间仓库有任何文件变动即作废（本日 BTC 首轮因此作废）；先提交再运行。
- 研究负载会拉长每小时实盘链：14:12 JST 一轮从平时 16 分钟拖到 34 分钟（任务上限 55 分钟）。研究进程须以低优先级运行并避免与实盘轮次重叠。
