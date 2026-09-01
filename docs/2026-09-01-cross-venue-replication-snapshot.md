# 跨所与跨品种复现证据快照（2026-09-01）

本文冻结 2026-08-27 至 2026-08-29 三组复现研究的结论。此前这些结论只存在
于 `reports/strategy-research/research-run-*/summary.json` 与提交说明中，
未入文档清单；本文按 W-02 收敛为唯一表达处。运行身份以内容寻址
run 目录为准，管线版本 `strategy-research-pipeline-v14`，样本自
2019-01-01 起的一小时柱，walk-forward 与验证门禁定义见
[策略研究契约](strategy-research.md) 第 4 节。

## 1. 运行清单

| run（前缀） | 日期 | 配置 | 市场 | 候选数 |
|---|---|---|---|---|
| `research-run-14c57fe7` | 2026-08-23 | 基线 | GMO BTC | 9 |
| `research-run-585baa38` | 2026-08-27 | price_breakout 七点加密网格 | GMO BTC | 13 |
| `research-run-f6779caf` | 2026-08-27 | `strategy_research_bitbank_replication.json`，费率 12 bps | bitbank BTC | 13 |
| `research-run-5ab15d3f` | 2026-08-29 | `strategy_research_xrp_replication.json` | bitbank XRP | 41 |
| `research-run-2824cebb` | 2026-08-29 | `strategy_research_dense_pb_median_rank.json` | GMO BTC | 41 |

## 2. 同品种跨所复现（GMO BTC 对 bitbank BTC）

| 指标 | GMO price_breakout | bitbank price_breakout | GMO trend | bitbank trend |
|---|---|---|---|---|
| 拼接 OOS Sharpe | 0.964 | 0.913 | 0.780 | 0.578 |
| 部署候选 OOS Sharpe | 1.179 | 1.218 | 0.918 | 0.897 |
| 最大回撤 | 0.268 | 0.266 | 0.432 | — |
| FDR q | 0.0245 | 0.0254 | 0.0486 | 0.0985 |
| PBO | 0.402 | 0.428 | 0.330 | 0.481 |
| 阻断项 | 仅 PBO | 仅 PBO | 无 | 四项 |

price_breakout 在两所的量级、回撤与「唯一被 PBO 阻断」的失败形态一致，
判为同构复现；trend 在 bitbank 上正折比、PBO、bootstrap 与 DSR 四项
阻断，不复现，支持其 GMO 表现属场所噪声的判读。

## 3. 跨品种检验（bitbank XRP）

| 家族 | Sharpe | 净收益 | 最大回撤 | PBO | 结论 |
|---|---|---|---|---|---|
| trend | 1.114 | 2.555 | 0.440 | 0.143 | 通过 |
| flow_trend | 1.082 | 2.416 | 0.409 | 0.580 | PBO 阻断 |
| price_breakout | 0.899 | 2.176 | 0.520 | 0.570 | 回撤与 PBO 阻断 |
| breakout | 0.801 | 1.838 | 0.612 | 0.811 | 回撤与 PBO 阻断 |

跨品种维度结论反转：trend 通过而 price_breakout 被拒。因此
「price_breakout 有真实结构、trend 是场所噪声」当前仅由同品种跨所
证据支持，不得外推到跨品种。

## 4. 网格加密与 PBO 的结构性互斥

为满足参数邻域数下限而把 price_breakout 网格从三点加密到七点后，
其 PBO 由 0.0059 升至 0.402：高度相关的参数变体族使 CSCV 折外秩
趋近随机。处置为 `selection_stability_gate_mode` 配置键（缺省
`pbo_hard` 逐字节兼容；`median_rank` 以折外平均秩中位数准入，PBO
仍完整计算并写入摘要作披露）。`median_rank` 模式下的对照
（`research-run-2824cebb`）：

| 家族 | Sharpe | FDR q | PBO | CSCV 秩中位数 | 准入 |
|---|---|---|---|---|---|
| breakout | 1.030 | 0.0321 | 0.117 | 0.917 | 是 |
| price_breakout | 0.964 | 0.0321 | 0.402 | 0.643 | 是（median_rank） |
| trend | 0.780 | 0.0486 | 0.330 | 0.583 | 是 |
| flow_trend | 0.761 | 0.0511 | 0.357 | 0.542 | 否（FDR） |

## 5. 边界与后续义务

1. `median_rank` 只调整研究管线内部准入；项目准入政策
   `guvolu-industry-strategy-admission-v4` 的 `maximum_pbo: 0.2`
   未变，price_breakout 仍不满足 live 准入政策。
2. breakout 是当前唯一 PBO 达标家族，但不在活动冻结计划
   （2026-08-23 签发）的候选集内；纳入须等下一个 clean plan
   与 vintage，不改写活动冻结运行根。
3. 容量证据仍完全空缺（`venue_l2_coverage` 三所 resolved 为零），
   压力场景缺一类；这些缺口不受本文结论影响，仍按行业证据
   生成器路线补齐。
