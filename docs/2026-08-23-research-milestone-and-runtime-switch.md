# 2026-08-23 研究里程碑与冻结运行根切换

> 时效快照（W-02）：内容冻结于 2026-08-23，修订以新快照发布。
> 本文记录 [2026-08-22 快照](2026-08-22-theory-system-and-fastest-live-path.md) 第 3 节
> 路径 S0 至 S2 的落地结果：数据前提恢复、旧封存段废弃、在 main 干净树重跑研究、新 vintage
> 与冻结计划登记、冻结运行根与权威注册库落 D 盘，以及同日并行落地项与残余风险。
> 全部事实以 2026-08-23 的代码树、治理注册库、运行 manifest 与现场观测为据；结论措辞限于
> 开发门禁层面，不主张策略资格。本文沿用该快照头部定义的「一致切换」与「策略资格级实盘」。
> 本文局部用语：显式废弃路径指 [strategy-research.md](strategy-research.md) 第 6.2 节所述
> sealed vintage 的 `abandoned` 终态及其入口。散列只列给定前缀，完整值见治理注册库、
> 运行目录 `manifest.json` 与 `plan.json`。
> 本次同步登记：本文入 [00-rules-registry.md](00-rules-registry.md) 文档清单；
> [architecture.md](architecture.md) TBD-37 状态改为【已实施首批 2026-08-23】并引本文，
> TBD-39 条目补记 `missing_policy` 已合入 main。

## 1. 数据前提恢复

E 盘（USB 外置 SSD）在 2026-08-22 事故复位后仍持续复位：系统日志中事件 129（存储控制器
复位）约每 18 秒一次、disk 事件 153 反复重试，随后该盘从系统中消失。冷层到热层的
`restore-hot` 依赖冷副本可读，仅恢复 57/1,803 个 Parquet。改用 `restore-hot --from-raw`
（[materialization-design.md](materialization-design.md) 第 8.2 节：由 C 盘 raw 归档重算，
以登记 SHA-256 与字节数相等为门禁）并按 8 分片并行，结果如下。

| 项 | 数量 | 说明 |
|---|---|---|
| restored | 1,731 | 重算散列与登记相等，已写回热层 |
| present | 65 | 热副本已存在，跳过 |
| mismatched | 7 | 被取代的非 head 旧输出，重算不等，不写入 |
| failed | 0 | 无 |
| 合计可用 | 1,796/1,803 | 活动 head 全部在 C 盘 |

随后以 `rollback --allow-missing-superseded` 把 `trade-normalization-v1` 的逻辑前缀路由回滚到
热层，被取代的 7 项缺失不阻断回滚。相关提交均在 main：`5a15506`（restore-hot）、`f971fdc`
（分片并行）、`3e9aca2`（回滚容忍被取代项缺失）、`e7d86e1`（合并 `data/restore-from-raw`）。

## 2. 治理：旧封存段废弃

研究运行首先被治理库拦下：开发研究区间与未消费封存段重叠。旧 vintage
`holdout-vintage-b1ed13e18f28…`（2026-08-21T00:00Z 至 2026-11-29T00:00Z，身份见
[strategy-research.md](strategy-research.md) 第 6 节）仍为 `sealed`，但其运行根所在 E 盘已失效、
预测自 2026-08-22T05:00Z 起中断、从未开始评估。处置如下。

| 步骤 | 事实 |
|---|---|
| 新增显式废弃路径 | 治理 schema v9，`abandon_holdout_vintage`；封存新段时忽略 `abandoned` 段但 `start_time` 不得早于其 `abandoned_at`；SKILLS.md G-08 补句；规则正文在 [strategy-research.md](strategy-research.md) 第 6.2 节；main `356b45e` |
| 旧 vintage 废弃 | 2026-08-23T10:07:04Z，理由写入 `abandon_reason`，账本不删行 |
| 开发治理库升级 | 显式升级 v2 至 v7、v8、v9，每步均有备份 |

## 3. 研究结果

研究运行 `research-run-14c57fe7…` 在 main `356b45e` 干净树执行（`code_tree_digest`
`a89b1b78…`），耗时 16 分钟，`family_scope` 为 `price_breakout` 与 `trend`，`config_hash`
`66590f5b…` 与 v14 一致，研究暴露区间 2019-01-01 至 2026-08-23T09:58Z。验证指标口径见
[strategy-research.md](strategy-research.md) 第 4 节，本文只列本次数值。

| 指标 | price_breakout | trend |
|---|---|---|
| stitched OOS Sharpe | 1.102 | 0.772 |
| 部署候选 Sharpe | 1.260 | 0.970 |
| FDR q | 0.0147 | 0.0352 |
| block bootstrap p | 0.0049 | 0.0380 |
| DSR（有效） | 0.997 | 0.968 |
| 参数邻域 Sharpe 保留 | 0.70 | 0.96 |
| development paper eligible | 是 | 是 |

组合分配（research）：price_breakout 0.5188、trend 0.0812、reserve 0.4，aggregate 0.588。
operational 分配归零，原因码 `feature_snapshot_stale` 与 `strategy_data_stale`，由运行耗时
16 分钟导致快照超出时效，不反映候选质量。verifier 复核 12 类制品通过。

结论措辞：两个流派为开发门禁层面的可用策略候选；策略资格级结论待第 4 节 vintage 的
holdout 裁决。

## 4. 新 vintage 与冻结计划

| 项 | 值 |
|---|---|
| vintage | `holdout-vintage-690a9c9b…`，市场 `mkt__gmo__btc__r0`，2026-08-24T00:00Z 至 2026-12-02T00:00Z，封存于 2026-08-23T10:25:07Z |
| 冻结计划 | `frozen-forward-plan-c6981780…8826b4`，`missing_policy=zero_exposure`（[strategy-research.md](strategy-research.md) 第 6.1 节），冻结于 2026-08-23T10:48:44Z |
| 同树约束 | 计划 `code_tree_digest` `a89b1b78…` 与第 3 节来源运行同树，在运行根内冻结 |
| 制品位置 | 运行根 `reports/strategy-research/frozen-forward/<vintage_id>/<plan_id>/plan.json` |

新段起点 2026-08-24T00:00Z 晚于旧段废弃时刻，且与第 3 节研究暴露区间零重叠，符合 G-08。

## 5. 冻结运行根切换（TBD-37 落地）

运行根与权威注册库自 E 盘温层迁至内置 D 盘，E 盘不再在任何决策链上。

| 项 | 值 |
|---|---|
| 运行根 | `D:\dev\guvolu-frozen-runtime-356b45e`，main `356b45e` 的 detached 克隆，干净树 |
| 权威治理注册库 | 运行根内，复制自开发治理库，含废弃旧段与新 sealed 段 |
| 来源运行制品 | research-run 目录、`research-artifacts/<research_identity>`、`data/research/input-receipts` 收据、`data/research/physical` 面板 |
| 数据快照 | `refresh_frozen_runtime` 产出 2,952 个输入、1.64 GB，GMO BTC 市场活动成交 head |
| 计划任务 | `guvolu-frozen-forward-f8981e8826b4`，包装 `run_frozen_shadow_task.ps1`，2026-08-24 09:10 JST 起每小时至 2026-12-02，`RuntimeRoot` 指向 D 盘运行根，执行仓 `guvolu-exec` |
| 旧计划任务 | `guvolu-frozen-forward-61c0c4cab6c5` 已停用 |

## 6. 并行落地

| 项 | 事实 |
|---|---|
| P2 paper executor 阶段一 | 执行仓 `codex/execution-chain` 提交 `c04f8b0` |
| P3-1 GPU SearchFast | main `c04efe3`；RTX 5070 实测约 2,250 候选/秒；parity 目标序列与 Sharpe 全部通过，换手容差 1e-6 偏严，建议 1e-5；主 `.venv` 无 torch。细节见 [GPU SearchFast P3-1 快照](2026-08-23-gpu-searchfast-p1.md) |
| 守护进程一致切换 | 2026-08-23 01:27 JST 重启完成：GMO 采集器 TAKER_ONLY（EP-0007 r1），`trade_realtime` 全部分区为 normalization v4，10,502 个 head |

## 7. 残余风险与待办

| 序 | 事项 | 处置 |
|---|---|---|
| 1 | E 盘硬件持续复位 | 建议把 SN850X 改装内置 M.2，或更换外置盒与线缆 |
| 2 | OKX 历史 L2 冷副本仅存 E 盘 | 待 E 盘硬件处置后恢复可读 |
| 3 | paper executor 未接入小时调度 | 待接入 |
| 4 | preflight 计划任务未注册 | 运行根与开发库均为 schema v9，可直接注册 |
| 5 | 评审待办 A3、A3b、A4、B5 至 B10 | 清单见 [2026-08-22 快照](2026-08-22-theory-system-and-fastest-live-path.md) 第 4 节 |
| 6 | GPU SearchFast P3-2 | 见 [GPU SearchFast P3-1 快照](2026-08-23-gpu-searchfast-p1.md) 第 6 节 |

## 8. 结论

S0 至 S2 已完成：活动 head 全部回到 C 盘热层，旧封存段经显式废弃路径留痕退役，main 干净树
上的研究产出两个开发门禁层面的可用候选，新 vintage 与 `zero_exposure` 计划已封存冻结，
运行根与权威注册库落 D 盘并由新计划任务自 2026-08-24 起每小时驱动。后续以稳定窗观察（S3）
计时，策略资格级结论待 2026-12-02 后 holdout 裁决。
