# 2026-08-15 全框架评审快照

> 时效快照（W-02）：内容冻结于 2026-08-15，修订以新快照发布。
> 评审方法：五路并行只读调查（执行与基础层、数据平台层、研究策略层、
> 文档与前端、运行时状态），全部结论以代码位置或实测时间戳为据。
> 本次同步维护：[architecture.md](architecture.md) 第 1 节新增全框架 mermaid 总图。

## 1. 架构总图落位

全框架总图落位于 [architecture.md](architecture.md) 第 1 节，覆盖运维保活、
外部来源、采集、数据平台、查询与控制面、研究、执行边界七个平面；
虚线标注四条已定义未接线路径（intent 账本、目标位置执行器、GPU worker、
LLM 管线）。数据平台细图与研究细图维持原位，不重复表达。

## 2. 代码与文档偏差（以 main 为基准）

| 序 | 偏差 | 位置 | 处置建议 |
|---|---|---|---|
| 1 | intent 账本未接线：`new_intent_id` 在 src 内零调用，执行链当前止于 TradeClient 返回委托号 | `src/guvolu/domain/ids.py` | 属执行层未实施，总图已以虚线表达 |
| 2 | 目标位置合同无消费者：`target-position` 制品仅有生产者与复核者 | `src/guvolu/research/pipeline.py` | 与 TBD-08 执行层归属未决一致，待执行器立项 |
| 3 | TBD-13 锁定「HTTP JSON + WebSocket 推送」，前端实际为分档轮询（5 秒、15 秒、60 秒），无 WebSocket 构造 | `web/src/api.ts` | 推送侧未实施，建议在 ui-design 标注现状或立项补齐 |
| 4 | [strategy-research.md](strategy-research.md) 第 4 节准入列表为 8 条，代码实际 11 条（另有 DSR 与三项参数邻域）；第 6 节制品表缺 cross_venue_shadow 行 | `src/guvolu/research/validation.py` | 该文档在 codex/research-next 分支大幅演进，待合并时统一复核，不在 main 单改 |
| 5 | `run_research` 编排函数（约 478 行）无任何回归测试；holdout 成功路径、shadow 模块亦未覆盖 | `src/guvolu/research/pipeline.py` | 建议列入下一步工作 |
| 6 | `flat_allocation` 为死 import | `src/guvolu/research/pipeline.py` | 待研究分支合并后清理，避免制造冲突 |
| 7 | venues 与 data 存在全仓唯一层间循环依赖（registry 写库、l2_capture 引流适配器） | `src/guvolu/venues/registry.py` | 现状可控，分层图按「采集与存储平面」合并表达 |
| 8 | architecture.md 第 7 节 TBD 汇总表无状态列，锁定状态须回详情表逐条读 | [architecture.md](architecture.md) | 与 W-02 单处表达存在张力，是否增列待议 |

## 3. 运行现场与文档偏差（时效事实，2026-08-15 23:30 JST 观测）

| 序 | 现场事实 | 文档现状 |
|---|---|---|
| 1 | 仓库存在三个工作树：main（数据根）、codex/research-next（领先 86 提交的研究树）、D 盘冻结运维树（detached） | main 文档未记载 |
| 2 | 两条 marketdata 计划任务的动作指向 D 盘冻结树脚本，携带 main 版脚本不存在的 Profile 与 Repository 参数；按 main 版重跑注册会把 ForwardMinimal 降级回 Full | main 的 register_marketdata_tasks.ps1 与实际注册不一致 |
| 3 | ForwardMinimal 配置生效中：六条 raw 采集与逐笔物化在跑；L2、book-state、OFL、market_status 物化自 2026-08-14 11:00 至 11:23 起停止，sealed L2 段持续积压 | ForwardMinimal 与恢复口径仅存在于 codex/research-next 的 runtime-ops.md，未合入 main |
| 4 | book_l2 实时 raw 目录已于 2026-08-14 11:09 以 NTFS junction 迁至 D 盘 | 迁移规程同上，未合入 main |
| 5 | 磁盘余量 C 盘 12.4%、D 盘 14.3%，均低于 runtime-ops 第 8 节 20% 门槛；data/backups 约 7.2 GB 为主要压力源 | 与「暂停派生重建」区间一致，现场处置合规 |
| 6 | 研究产物卫生：5 个无 manifest 空壳 run 目录、3 个 0 字节孤儿临时 parquet、monitors 双布局并存、一处误传目录型输出路径 | 待清理 |

## 4. 研究治理链现状与风险

治理注册表现状（data/research/governance.sqlite3，schema v2）：holdout vintage
一条，区间 2026-08-21T00:00Z 至 2026-11-29T00:00Z，已封存未消费；冻结前向计划
一条；前向预测零条；计划任务已注册（Ready），首次运行 2026-08-21 09:10 JST。

| 序 | 风险 | 依据 |
|---|---|---|
| 1 | 特征成熟临界：最长特征需 169 根连续小时柱，最新断点后尚未凑足，最早成熟约 2026-08-20，仅早于 vintage 起点约一日；期间任何再断流都会把成熟推迟、令前向预测在成熟前合法输出零仓 | readiness 实测与 `_maturity_gate` 行为 |
| 2 | 计划任务以交互登录型注册，注销或关机超过约两小时即产生预测缺口；预测登记有 3900 秒时效窗，缺口不可事后补算，只能烧毁 vintage | register_frozen_forward_task.ps1 与 governance 时效校验 |
| 3 | 工作树整洁度即决策级身份：任何未提交改动使 decision_grade 失效；代码树摘要仅覆盖 src、tests 与配置，纯文档提交不影响既有冻结计划绑定 | `src/guvolu/research/provenance.py` |
| 4 | 三个通过准入流派的有效试验数仅约 1.2 至 1.8，同流派候选高度相关，多重检验保护实际力度有限 | validation 有效试验数披露 |

## 5. 结论

数据平台与研究管线的既有闭环均有产物与复核佐证；当前系统处于「冻结前向
等待期 + 磁盘受限最小运行面」的过渡状态。下一步优先级建议：磁盘减压与
派生物化恢复、运维现场口径合入 main、`run_research` 回归测试补齐、
冻结前向断电风险处置。以上不构成锁定，供讨论。
