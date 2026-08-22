# 2026-08-22 理论体系整理与最快实盘路径

> 时效快照（W-02）：内容冻结于 2026-08-22，修订以新快照发布。
> 本文整理当日 main 切换到 v7 栈后的运行现场、按层归纳理论体系，并给出到
> 最小实盘的最短路径。全部事实以 2026-08-22 的代码树、治理注册库与现场观测为据；
> 路径与时间估计是建议，不构成锁定。
> 本文局部用语：彩排 vintage 指覆盖已破、期末必被烧毁、只用于演练运行链路而
> 不产生裁决的封存段；一致切换指在同一观察窗内按既定先后次序重启全部守护进程，
> 使采集器与物化器加载同一代码版本；链路级 canary 指只验证执行链路正确性、不
> 主张策略资格的最小实盘；策略资格级实盘指以 holdout 裁决为前提的实盘。
> 本次同步登记：本文入 [00-rules-registry.md](00-rules-registry.md) 文档清单；
> [architecture.md](architecture.md) TBD-37 条目追加本文为事故依据，状态标签不变。

## 1. 现状

### 1.1 main 切换到 v7 栈

main 于 2026-08-22 由 `ad59389` 前进到 `adf0055`，共八个提交：

- `e659b4a`：TBD-35 热冷存储根与冻结 shadow 运行时；
- `5701a0e`：holdout 消费前只读预检工具 [preflight_holdout.py](../scripts/preflight_holdout.py)；
- `2f1192c`：预检计划任务包装 [run_holdout_preflight_task.ps1](../scripts/run_holdout_preflight_task.ps1)，计划任务尚未注册；
- `79d6c4f`：两份提案文档（TBD-36 至 TBD-39）与术语登记；
- `002097e`：冷迁移评审 A1 与 A2 加固，路由切换先验证后落盘、跨进程锁、独占创建；
- `bfac94f` 与 `88d378a`：合并 `codex/gmo-flow-integrity`，107 个文件，治理 schema 由 v2 升至 v7，GMO 逐笔改为 TAKER_ONLY 订阅（端点契约 EP-0007 r1），`trade-realtime-normalization-v4`，4hour 区间套件，v14 纯价格 cohort，治理升级脚本；
- `adf0055`：冻结 shadow 包装脚本在解析运行根前保证留痕（运行根不可达记退出码 3），修正第 1.3 节暴露的日志缺陷。

合并后全量测试 713 项通过，`mypy --strict` 无错误，Markdown 样式校验通过。

### 1.2 现行 vintage 覆盖已破

现行封存段与冻结计划的身份登记在 [strategy-research.md](strategy-research.md) 第 6 节。
截至 2026-08-22T05:25Z，30 个决策窗只登记 22 条预测：缺 08-21 18:00Z 至 21:00Z 四条、
08-22 00:00Z 至 02:00Z 三条、08-22 04:00Z 一条，根因为设备关机与物化延迟；08-22 10:00Z
起又因 E 盘事故中断。现行 [holdout.py](../src/guvolu/research/holdout.py) 先原子消费
vintage，再逐柱校验覆盖，任一缺柱即抛错而 vintage 已不可重跑，因此该 vintage 期末必被
烧毁，降级为彩排 vintage：继续运行只为演练链路，不产生裁决。

### 1.3 E 盘事故

E 盘为 WD_BLACK SN850X 2 TB USB 外置 SSD，承载冷层制品（OKX 历史 L2 v2 14.38 GiB、
`trade-normalization-v1` 7.40 GiB，二者热副本已释放）与温层冻结运行根
`frozen-forward-runtime-6d70`，后者内含权威预测注册库（TBD-37 提案所述现状）。
2026-08-22 约 19:10 JST 起，E 盘在操作系统层无响应：卷查询与目录列举超时，该时刻正在
运行的 shadow 实例成为不可终止的内核 IO 进程；22:10 JST 计划任务退出码 1 且无调度日志，
原因是包装脚本在 `try` 之前对运行根执行 `Resolve-Path`。影响：冻结前向预测与研究面板停止
（历史 `trade_observation` 仅存 E 盘）；采集与实时物化位于 C、D 盘，不受影响。恢复需
人工复位 USB。

由此得出两条结论。其一，TBD-37 由提案升为必须尽快执行：运行根与权威注册库落内置盘，
E 盘只作不可变冷层。其二，研究关键 Parquet 须保留热副本；现行回退流程要求热副本存在
（[materialization-design.md](materialization-design.md) 第 8.2 节），因此需新增由冷层恢复
热副本的 restore 命令，列入第 4 节待办。

### 1.4 守护进程版本与部分重启危害

采集器与 trade-realtime 物化器均为 2026-08-22 15:13 JST 重启的切换前代码。D 盘守护脚本
`D:\dev\guvolu-ops-frozen`（提交 `e871841`）以 C 盘 `.venv` 拉起，因此任何一次重启都会加载
v7：GMO 采集器变为 TAKER_ONLY 与 EP-0007 r1；物化器以 normalization v4 作为完成态复用键，
会把全部 `trade_realtime` 分区重物化为 v4（Parquet 列 schema 不变，面板 `union_by_name`
读取兼容）。部分重启的危害：若只重启 GMO 采集器，r1 原件会被旧物化器以「endpoint_revision
与端点契约不一致」拒绝，面板随即停止。建议在稳定窗起点做一次受观察的一致切换，次序为
先物化器、后 GMO 采集器。

### 1.5 存储、执行仓与研究制品

存储实测、守护版本与执行仓状态汇总如下；v14 研究制品位于 flow 工作树，未在 main。
holdout 要求评估树与来源运行同树，因此新 vintage 之前须在 `88d378a` 树重跑 v14 研究。

| 序 | 事实 | 数值或位置 |
|---|---|---|
| 1 | C 盘 NVMe | 465 GB，空闲 28.9% |
| 2 | D 盘 SATA SSD | 931 GB，空闲 19.0%；guvolu 实占约 16 GB |
| 3 | E 盘 USB SSD | 2 TB，空闲 98.2%；2026-08-22 晚起无响应 |
| 4 | raw L2 未压缩日增 | 1.46 GB（bitFlyer 0.70、GMO 0.48、bitbank 0.28） |
| 5 | L2 v5 Parquet 日增 | 约 81 MB |
| 6 | 现行 vintage 权重 | flow_trend 0.180456、trend 0.219544、reserve 0.6 |
| 7 | 预测覆盖 | 截至 08-22T05:25Z 为 22 条对 30 窗 |
| 8 | 执行仓 `codex/execution-chain` | 提交 `1d5b0be`：冻结目标适配器与 dry-run 执行器 |
| 9 | P2 工作树 | `C:\Users\wu_zh\dev\guvolu-paper`，分支 `codex/paper-executor` 已建 |
| 10 | v14 研究制品 | flow 工作树 `reports/strategy-research/v14-baselines/price-combination/research-run-82672ddf…` |

## 2. 理论体系整理

各层按数据单向血缘（D-01）排列。状态取「已锁定」「已实施」「提案」三值：已锁定指
[architecture.md](architecture.md) 标注【已锁定】的条目；已实施指有生产代码与验收记录但
未上升为锁定项；提案指 TBD 台账中的提案。

| 层 | 职责 | 不变量 | 现状与状态 | 文档 |
|---|---|---|---|---|
| 1 不可变事实 | 原件落盘、控制面、分析面 | raw 只追加永不改写（D-02）；制品内容寻址；冷盘身份不符即读取失败 | raw v3、Parquet、SQLite v20 已锁定；热冷根 TBD-35 首批已实施 | [materialization-design.md](materialization-design.md) 第 8.1 节、第 8.2 节 |
| 2 精确重放与状态 | 由原件重建盘口与质量事实 | 重放确定性；质量与状态旗标随事实登记 | bitbank 重放、book-state v3、quality v1、REST anchor v2 已在产 | [order-flow-data-contract.md](order-flow-data-contract.md)、[2026-08-13 快照](2026-08-13-l2-quality-and-l3-readiness.md) |
| 3 PIT 面板与特征 | 固定品种、字段、期间的研究矩阵 | 时间四元组分开（D-03）；`available_time <= decision_time`（D-04） | `trade-bars-pit-v1`、`research-features-v1` 已实施 | [strategy-research.md](strategy-research.md) 第 2 节 |
| 4 候选与搜索 | 生成并枚举候选 | typed DSL；`expression_id` 与 `candidate_id` 内容寻址；SearchPlan 为 DAG；GPU 永不持密钥（T-13） | CPU 路径已实施；GPU SearchFast 为 TBD-18、TBD-19、TBD-20 实施案 | [2026-08-22 GPU SearchFast](2026-08-22-gpu-searchfast-architecture.md) |
| 5 验证 | 样本外准入 | 双边 10bp 成本模型、embargo walk-forward、FDR q 不超过 0.2、PBO、DSR 不低于 0.95、bootstrap、邻域保留；台账全量计数（G-07） | ValidationExact 已实施 | [strategy-research.md](strategy-research.md) 第 4 节 |
| 6 治理 | 封存段与冻结计划 | 封存段一次消费（G-08）；冻结计划；小时预测不可改写；3,900 秒登记时效；`missing_policy` 预登记为提案 | governance schema v7 已实施；预登记项随 TBD-39 提案 | [strategy-research.md](strategy-research.md) 第 6 节 |
| 7 决策生成 I/O | 决策输入、决策记录、执行目标 | 目标域 `[0, 1]` 纯多头；有效期显式；`correlation_id` 贯穿 | TBD-39 提案 | [2026-08-22 决策 I/O 契约 v2](2026-08-22-decision-io-contract-v2.md) |
| 8 执行域 | 意图、委托、对账 | 双密钥正交（T-02）；`intent_id` 先落盘（T-05）；T-11 限额；dry-run 缺省（T-04）；dry-run、paper、canary、live 阶梯不跳级（T-12） | 链路验证性实盘已完成；冻结目标适配器与 dry-run 执行器在执行仓 | [2026-08-17 链路验证](2026-08-17-execution-link-verification.md) |
| 9 运维 | 守护、分层、容量、预检、日志 | 单写者；冷热分层；容量阶梯；日志不变量（失败必有调度日志） | 守护与日程已实施；容量双判据 TBD-38 与运行根落位 TBD-37 为提案 | [runtime-ops.md](runtime-ops.md) 第 8 节 |

## 3. 最快实盘路径

| 阶段 | 动作 | 退出判据 | 依赖 |
|---|---|---|---|
| S0 | 人工复位 E 盘 USB；执行 TBD-37：运行根与权威注册库落 D 盘，E 盘只作不可变冷层；新增 restore 命令并为研究关键 Parquet 恢复热副本 | E 盘可读；D 盘运行根与注册库就位；热副本散列核对通过 | 无 |
| S1 | 一致切换 v7 运行时：先物化器、后 GMO 采集器 | 观察两个物化周期与一个预测周期无 reject、无缺柱 | S0 |
| S2 | 在 `88d378a` 树重跑 v14 研究（cpu-v9，price_breakout 与 trend 纯价格），冻结新计划（1hour）；封存前预登记 `missing_policy`（无预测则零暴露）、`valid_until`、overlay 函数与阈值、降级码 | 新计划登记完成；候选权重以重跑结果为准（参考 price_breakout 0.5188、trend 0.0812、reserve 0.4） | S1 |
| S3 | 稳定窗观察 | 连续不少于 7 天零缺口且预检通过，随后封存新 vintage 起跑 | S2 |
| S4 | 并行 P2：paper executor 与 L2 覆盖层门控先只记录两周 | 差异账完整：prediction、target、intent、模拟成交、成本 bp 逐项可追溯 | S1 |
| S5 | shadow 到 canary 五门禁（连续 24 小时起，推荐 72 小时）满足后，按 A-01 人工批准链路级 canary | GMO BTC 单笔限价约 500 JPY 以内、最小 0.00001 BTC，完成即停机复核 | S3 或 S4 任一达标 |
| S6 | 策略资格级实盘 | 新 vintage 结束后 holdout 裁决通过 | S3 |

S4 的成交模型与门控细则：paper executor 的 B2 成交模型使用自家三所 L2 顶档与深度，
taker 费在运行时读取 symbols；L2 覆盖层门控变量为质量窗 `eligible`、锚龄、熔断、spread、
前五档深度，乘子以 imbalance 为基础、幅度限 ±0.3，两周内只记录不施加。五门禁与 canary
约束的正文见 [strategy-research.md](strategy-research.md) 第 6 节，本文不复述。

链路级 canary 与策略资格级实盘必须区分：前者只证明执行链路在真实写请求下正确，不主张
任何策略资格，可在 S5 门禁满足后申请；后者以 holdout 裁决为前提，只能在新 vintage 结束
后发生。

时间估计：E 盘复位后 S0 与 S1 合计一至两日；S2 半日；S3 开始计时后最早 2026 年 9 月上旬
封存新 vintage，100 日区间至 12 月上旬裁决；canary 在 S5 满足后即可申请。

## 4. 风险与待办

| 序 | 事项 | 处置 |
|---|---|---|
| 1 | 评审待办 A3：`partition_guid` 需实做系统查询 | 冷热根身份识别补齐 |
| 2 | 评审待办 A3b：cold 与 hot_bulk 强制 `volume_guid` | 同上 |
| 3 | 评审待办 A4：`_resolve_recorded_path` 绝对路径兼容 | 路径解析补齐 |
| 4 | 评审待办 B5：latest-run-only 按 `(venue, symbol)` 判定 | 物化选择修正 |
| 5 | 评审待办 B6：删除 `_latest_run_inputs` | 清理 |
| 6 | 评审待办 B7：`market_status` 被 latest-run-only 搁置需补偿 | 物化补偿 |
| 7 | 评审待办 B8：冻结隔离显式断言 `guvolu.__file__` | 冻结运行根自检 |
| 8 | 评审待办 B9：`run_shadow` 顶层锁 | 并发保护 |
| 9 | 评审待办 B10：写路径 fail-closed 文档收窄 | 文档修订 |
| 10 | 调度包装脚本在 `try` 之前执行 `Resolve-Path`，失败无调度日志 | 已于 `adf0055` 修正，待下一周期验证 |
| 11 | 预检计划任务未注册 | 待 v7 运行根建好后注册，保持 v7 对 v7 |
| 12 | raw 压缩封口段 | TBD-36 |
| 13 | D 盘守护脚本与 main 分叉 | 待收敛 |
| 14 | 冷层到热副本的 restore 命令缺失 | 新增命令，见第 1.3 节 |

## 5. 结论

系统当前处于「v7 栈已合并、运行时尚未一致切换、E 盘事故待复位、现行 vintage 降级为彩排」
的过渡状态。最短路径的关键在 S0 至 S3：运行根落内置盘、一致切换、在 main 树重跑研究并
封存带预登记处置的新计划，随后以稳定窗开始计时。链路级 canary 不依赖新 vintage 裁决，可在
五门禁满足后单独申请。
