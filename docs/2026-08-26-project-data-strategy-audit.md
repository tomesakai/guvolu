# Git、数据、策略管线与实盘准入审计（2026-08-26）

## 结论

截至 2026-08-26 01:45 JST，项目结论为 `NOT_READY`，不得实盘。
已有研究结果可以支持继续做受控前向与 paper 验证，但还不能支持资金风险：
主远端没有任何可取回引用，OKX 历史 L2 的 58 个活动 Parquet 指向离线 E 盘，
多条衍生数据链陈旧，当前封存前向尚未终态，paper 浸泡与执行安全证明也未形成。

本审计把“量化业界标准可用”解释为一套可复核的项目准入政策，而不是声称存在
单一的行业通用阈值。任何技术门禁全过后，系统最多输出
`READY_FOR_EXTERNAL_LIVE_APPROVAL`；人工实盘批准始终外置，检查器自身固定
`live_authorized=false`。

审计期间没有推送远端、没有执行 Git 垃圾回收、没有删除本地数据、没有读取密钥值、
没有执行 OKX 真实恢复，也没有启用 live。运行态变更限于修复受版本控制的冻结
shadow/paper 与 holdout 预检任务入口；前者只允许 dry-run/paper，后者严格只读。

## 快照边界

- Git 与工作树：2026-08-26 00:00 至 01:45 JST；
- SQLite、数据规模与时效：2026-08-26 00:27 至 00:35 JST；
- 磁盘空间复核：2026-08-26 01:13 JST；
- 调度首个成功周期：2026-08-26 00:39:35 至 01:07:21 JST；
- 行情采集仍在运行，所以文件数、行数、时效和剩余空间是有界快照，不是常量。

## Git 现状

| 项目 | 证据 | 判断 |
|---|---:|---|
| 审计起点 | `main@bd3d7fee0e08b5b06622486b1f345cdb41365397` | 起点主工作树干净 |
| 上游 | `origin/main [gone]`；`git ls-remote origin` 成功但返回 0 个 ref | 当前没有可验证远端备份 |
| worktree | 17 个，其中 E 盘冻结运行根为 `prunable` | 不能批量清理或假定都可达 |
| 研究 WIP | `codex/research-next@b6e392a`，4 个 tracked 修改、2 个 untracked | 约 1,030+/16-，且落后 main 76 个提交，必须独立保全和语义迁移 |
| 执行分支 | `codex/execution-chain@c04f8b0`、`codex/paper-executor@c04f8b0` | 相对 main 为 76 个 main-only、32 个 branch-only 提交 |
| 对象库 | 3,122 个 loose object、153.59 MiB；2 个临时垃圾对象、26.59 MiB | 连通性通过，但禁止在无远端备份时 prune/gc |

主仓当前为有意的审计实现工作树，包含经济代理、准入检查器、调度注册器和 OKX
恢复代码，尚未提交。执行链仍只存在于分离工作树；main 没有真实下单权限，不能把
执行分支的存在误报为已部署。

最高优先级的 Git 风险不是代码冲突，而是恢复面：远端空、存在未提交的大型 WIP、
还有悬空对象。任何 merge、rebase、prune 或工作树删除之前，必须先建立可验证的
异机/远端备份，并为 WIP 生成独立 bundle 或提交。

## 本地数据现状

主数据根快照为 267,525 个文件、49,427,147,311 B（约 46.03 GiB）。其中 raw
约 32.75 GB、materialized 约 11.28 GB、archive 约 2.67 GB；SQLite 主库约
2.358 GB，另有约 99.8 MB WAL。D 盘实时 L2 原件约 26,243 个文件、23.04 GB，
采集仍在增长。

SQLite schema 为 20，35 个业务表；`quick_check=ok`，外键错误为 0。关键行数：

| 表 | 行数 |
|---|---:|
| `market_kline` | 3,781,423 |
| `trade_tick` | 1,353,575 |
| `book_top` | 190,977 |
| `artifact` | 201,292 |
| `artifact_location` | 201,772 |
| `partition_attempt` | 57,271 |
| `materialization_partition_head` | 19,690，审计时仍在增长 |

### 路由与可用性

`data/storage-roots.json` 把
`materialized/book_l2/schema_version=2/normalization_version=book-l2-normalization-v2`
路由到缺失的 `E:\guvolu-cold\v1`，但状态仍为 active。结果是 29 天、每一天
frame/level 成对的 58 个 OKX BTC-USDT 活动 L2 Parquet 全部不可用，登记总字节
15,438,105,544 B；相应 29 个 manifest 也不可达。另有 7 个 trade Parquet 缺失，
但它们不是活动 head，风险级别较低。

C 盘仍保存 29 个完整 OKX raw `tar.gz` 和 manifest，raw 登记总字节
3,622,395,110 B，因此可以离线、逐日、按原 attempt 重算。新增恢复路径只做计划内
缺失文件的重放和精确 SHA/字节比较，不下载、不登记新 attempt/head、不改路由；
同日 frame/level 使用稳定路径锁，第二个文件落盘失败会尽力补偿回移第一个文件，并
留下精确阶段/失败计数；掉电时仍不声称具有跨文件原子性。真实 58 项恢复尚未执行。

D 盘在 01:13 JST 的剩余空间为 181,844,303,872 B，约 18.18%，低于项目 20%
门禁；恢复 backlog 前不能切换 Full。C 盘约 24.26% 空闲。实际恢复前还必须重新
计算持久输出和单日临时峰值后的 C/D 双盘安全余量。

### 时效与质量

| 数据链 | 审计时陈旧度/状态 | 影响 |
|---|---:|---|
| REST L2 anchor | 约 8 分钟 | 新鲜，但不能代替连续 L2 |
| trade realtime 总体 | 约 10 分钟 | 总体可用 |
| bitbank BTC/JPY trade | 约 68 小时 | 单所输入陈旧 |
| BTC L2 v5 | 约 83 小时 | 原始 L2 虽增长，规范化未追上 |
| book_state / OFL v8 | 约 83 小时 | 策略微观结构特征不可用 |
| market_status | 约 117 小时 | 运行状态特征陈旧 |
| OKX L2 | 约 376 小时且文件不可达 | 跨所历史证据不可用 |
| klines | 约 17 至 25 天 | 不能用于当前决策输入 |

K 线主键、OHLC 关系与 UTC 基本合同通过，但有 158 行未收束多小时/日线出现
`ingest_time < available_time`。所有消费者必须继续执行
`available_time <= decision_time`，不能用摄取时刻替代 PIT。

`trade_tick` 有 377 行 `size<=0`，均为 GMO BTC synthetic，进入研究前必须显式
排除或解释。`book_top` 没有 crossed/非法报价，但 28,609 行主要为 GMO 的
`ingest_time < available_time`，最大约 0.779 秒，符合来源时钟偏移而不是简单的
数据倒置；时间字段必须分开保留。

## 策略怎样生成

当前策略不是由 LLM 自由生成。生成器从六个手工、版本化、类型化模板出发：
`trend`、`flow_trend`、`breakout`、`price_breakout`、`mean_reversion` 和仅用于
shadow 的 `grid_shadow`。模板经 bounded grid、邻域展开、类型约束 mutation/crossover
变成 typed DAG / SearchPlan；Candidate Registry 在试验前以规范内容散列登记，
避免先看结果再决定是否计入多重检验。

```text
ResearchProposal
  -> 模板白名单与参数边界
  -> typed AST / Candidate Registry 预登记
  -> GPU SearchFast 宽筛
  -> CPU f64 exact 一致性与全试验台账
  -> expanding walk-forward + 24h embargo
  -> FDR / DSR / CSCV-PBO / circular block bootstrap
  -> 参数邻域、成本与容量门禁
  -> proposal-only candidate config
  -> sealed forward holdout
  -> paper soak 与对账
  -> 外部人工 live approval
```

搜索循环累计评估 8,462 个候选，1,028 个通过 F1，256 个通过 F3 GPU/CPU parity，
最终只提出一个 trend 邻域配置
`config/strategy_research_candidate_417aaf574008.json`。该文件只是 proposal；上次
CPU 尝试只留下配置/血缘，没有完整 summary、manifest 和 trial ledger，因此未获
资格，也未自动改写正式配置。

## 当前最强研究证据

最近一个完整、clean、decision-grade 的 CPU 运行是
`research-run-14c57fe...`，代码身份 `356b45e`，单一 GMO BTC/JPY、1 小时柱、
66,314 根、9 个候选、486 个 trial、26 个 expanding folds。

| family | validation OOS Sharpe | deployment OOS Sharpe | 最大回撤 | PBO | bootstrap p | DSR 概率 | FDR q |
|---|---:|---:|---:|---:|---:|---:|---:|
| `price_breakout` | 1.102 | 1.260 | 0.268 | 0.0059 | 0.0049 | 0.9973 | 0.0147 |
| `trend` | 0.772 | 0.970 | 0.432 | 0.3203 | 0.0380 | 0.9677 | 0.0352 |

研究权重为 price_breakout 0.5188、trend 0.0812、reserve 0.4；运行权重仍为 0，
原因码是 `feature_snapshot_stale` 与 `strategy_data_stale`。price_breakout 的统计证据
明显较完整；trend 的 PBO 0.3203 超过当前项目 0.2 门禁，不能因为组合均值看起来
可接受而忽略。

该研究已经具备 PIT、内容寻址收据、embargo walk-forward、FDR、PBO、bootstrap、
DSR、参数邻域和成本回放等正确主线；仍缺少跨市场/跨 venue 重复、alpha/beta、
CVaR/尾部、回撤持续时间、容量曲线、流动性/延迟/部分成交/排队/逆向选择压力，且
当前约 10 bps 的固定成本与静态容量假设不足以支撑实盘。

## 冻结前向与 paper

原计划任务通过未受版本控制的 `D:\dev\guvolu-ops-alt\run_frozen_forward_c698.ps1`
间接启动。该 UTF-8 无 BOM 文件在 Windows PowerShell 5.1 下被错误解码，首行中文
注释末字节吞掉下一行 `$Wrapper` 赋值，最后把 `-PlanId` 当作 `-File` 路径，形成
`0xFFFD0000` 且没有有效运行日志。

新增注册器直接绑定仓库内 `scripts/run_frozen_shadow_task.ps1`。第一次受控运行在
00:39:35 至 01:07:21 JST 完成：数据快照 3,543 个输入、1,646,695,338 B，
`quick_check=ok`、外键错误 0；生成 00:00 JST 决策预测
`frozen-forward-prediction-6471e9...`，aggregate target 0.587847，dry-run 为
`DRY_RUN_BLOCKED`，`write_touched=[]`，调度退出码 0。该周期明确带 `-NoPaper`。

验证零写后，任务已移除 `-NoPaper`。01:10 JST 周期复用了同一 00:00 预测，paper
于 01:24:53 正确拒绝已经在 01:00 到期的目标，没有产生 paper 账或成交。根因不是
执行有效期少算一柱：面板 `decision_time` 是闭合柱末端；01:20 的冻结输入事件上界仍为
00:48:49，无法形成 01:00 完整柱，而旧运行根又在 existing 复用路径之前漏做 3,900 秒
时效检查。把有效期右移会跨缺失整柱沿用旧信号，破坏回测、holdout 与执行同构。

主仓已把时效检查移到复用之前；该核心修复只能进入新的 clean frozen runtime/plan，
不能原地改写当前冻结运行根。当前编排先以 45 分钟年龄硬限保护旧运行根，并让 paper
非零、`needs_reconciliation` 新报告或复用报告都保持非零失败。任务按每小时第 25 分钟
运行，设置为 IgnoreNew、StartWhenAvailable、允许电池、不中途因电池停止、WakeToRun、
45 分钟上限、失败 3 次且间隔 5 分钟；下个周期为 02:25 JST。

当前 vintage 使用 `zero_exposure` 缺失策略：缺失预测会以零暴露计分而不是事后补写。
因此历史缺口使结果 degraded，但不会被静默回填，也不能简单说 vintage 自动烧毁。
01:36--01:40 JST 的只读预检通过 frozen-forward 完整性校验且无 `would_burn` blocker，
但 38 个应有决策仅登记 1 个、缺口率 100%，状态为 `degraded`。这些前向缺口不可回填；
项目的更严格准入门禁要求新的 clean plan/vintage 和完整高质量前向证据。每日预检已
绑定该 vintage，于 09:35 JST 单次运行；`degraded` 保留非零结果，不做无意义自动重试。

## 经济研究代理

新增 research-only 经济研究代理核心，负责：追加式 hash-chain 经济观测、
`event/available/ingest/decision` 四时点分离、修订链、按 `available_time` 的 as-of
重放、growth/inflation/rates/liquidity/fx/risk 六维 regime、missing/stale/partial
质量状态、proposal 配额与模板/参数/holdout 门禁，以及 model/prompt/input/output
身份和所有接受/拒绝尝试的内容寻址回执。

它默认不调用 LLM、不联网、不读取密钥，没有 TRADE、配置修改、注册、晋级或下单
权限；输出只能是供 SearchPlan 显式审查的 proposal。实现现已在路径锁内绑定观测台账
`sequence/head_sha256` 前缀并逐字节重建 context，接受时刻只取内部可信壁钟，公共读均
加锁；PIT 只以 `available_time` 判定，`ingest_time` 不承担防未来职责。任何配置了
holdout 边界的政策都会以 `holdout_governance_unbound` 失败关闭，直到未来直接验证
governance SQLite 的 sealed vintage。它仍是 `proposal_only`，不是策略生成许可。

## 项目准入检查器

新增只读、fail-closed 的 `industry-strategy-readiness-v1`。它检查：

1. clean/decision-grade CPU manifest 和必需制品；
2. 候选完整指标；
3. 全局 trial ledger、FDR、DSR、PBO、bootstrap 和邻域稳定；
4. 尾部、压力、容量、成本与基准；
5. 封存前向终态、制品散列、决策网格和覆盖；
6. paper 时长、决策数、差异账、对账和零真实写；
7. 执行限额、熔断、超时/双通道对账、独立 kill switch 和权限隔离；
8. 永远外置的人工实盘批准。

01:43 JST 的实际结果是 `NOT_READY`、`live_authorized=false`。研究 manifest 与候选基础
指标通过；统计门禁因 trend PBO 0.3203 和 price_breakout 仅一个参数邻居失败；成本、
压力与尾部场景数均为 0；当前 sealed vintage 覆盖率仅 0.0004167 且无 consumed 终态；
paper 为 0 决策、0 账本、0 对账；execution safety attestation 缺失，六项控制与三项权限
隔离均未证明。检查器没有写文件、联网或做自动 promotion。

## 从现在到 live 的顺序

| 阶段 | 必须完成的可验证出口 | 当前状态 |
|---|---|---|
| G0 恢复面 | 非空远端/异机备份；WIP bundle；禁止依赖 dangling object | 未完成 |
| G1 数据面 | OKX 58 项离线精确恢复；路由复核；D 盘 >=20%；L2/OFL/状态追平 | 未完成 |
| G2 研究面 | proposal 完整 CPU exact；尾部/容量/成本/延迟压力；跨 venue 重复 | 未完成 |
| G3 前向 | sealed vintage 终态、制品全验、项目政策要求的预测覆盖 | 旧 vintage 已降级；须新建 |
| G4 paper | 至少 720 小时、500 个决策/账本/对账、错误率 <=1%、零真实写 | 尚未形成首条有效账 |
| G5 执行安全 | 独立权限、硬限额、熔断、kill、超时与双通道对账 attestation | 未完成 |
| G6 live | 外部人工批准后才可做最小 canary；可撤回、可降级、持续监控 | 未授权 |

禁止跳级：研究 Sharpe 不能替代前向覆盖，前向通过不能替代 paper，对账通过也不能
替代外部 live 授权。

## 外部方法与监管锚点

- 模型风险治理按美联储 2026 年修订指导的概念健全性、独立验证、持续监控、有效挑战
  和治理职责映射：[SR 26-2](https://www.federalreserve.gov/supervisionreg/srletters/SR2602.htm)、
  [完整指导](https://www.federalreserve.gov/frrs/guidance/supervisory-guidance-on-model-risk-management.htm)。
- 算法交易控制按 MiFID II Article 17 的容量、阈值、错误订单阻断、连续性、充分测试
  和监控映射：[ESMA Article 17](https://www.esma.europa.eu/publications-and-data/interactive-single-rulebook/mifid-ii/article-17-algorithmic-trading)。
- 实时监控、kill 和两道防线参考 2026 ESMA supervisory briefing：
  [官方 PDF](https://www.esma.europa.eu/sites/default/files/2026-02/ESMA74-1505669079-10311_Supervisory_Briefing_on_Algorithmic_Trading_in_the_EU.pdf)。
- 自动化预交易控制参考 SEC Rule 15c3-5 FAQ：
  [SEC Market Access Rule FAQ](https://www.sec.gov/rules-regulations/staff-guidance/trading-markets-frequently-asked-questions/divisionsmarketregfaq-0)。
- 日本加密资产系统与风险监督参考金融厅监督指针：
  [金融商品取引業者等向け監督指針](https://www.fsa.go.jp/common/law/guide/kinyushohin/03.html)。
- 回测过拟合与多重尝试证据分别以
  [CSCV/PBO 原论文](https://escholarship.org/uc/item/4hn4t174) 和
  [Deflated Sharpe Ratio](https://doi.org/10.2139/ssrn.2460551) 为方法锚点。

这些来源提供治理和验证原则，不替本项目背书，也不证明任何候选未来盈利。
