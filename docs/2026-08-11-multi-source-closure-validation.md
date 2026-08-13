# 多来源闭环与 P1 验证快照

采集与验证时刻：2026-08-11。本文冻结本地数据层的实际写入结果；接口字段依据使用交易所官方文档，运行结论以本地 raw、归档散列、SQLite 分区台账和离线测试为准。

## 1. 已完成范围

P0 完成 GMO、bitbank、bitFlyer 三家日元现货的归档逐笔投影。每个归档分区以「来源、来源品种、日期、文件路径、压缩原件 SHA-256」登记 `normalized_partition`；同散列完整分区重跑跳过，散列变化才重新投影。`backfill_run` 记录每个来源品种的运行终态。

P1 完成三项：

| 项目 | 实现与验证 | 可信边界 |
|---|---|---|
| Binance `BTCUSDT` aggTrades | 下载 ZIP 与同目录 `.CHECKSUM` 比对原字节散列；2025-01-02 成功入库 | 聚合成交，不得与逐笔成交直接相加；2025 起时间戳按微秒解析 |
| Coincheck `btc_jpy` | 原始 WebSocket 逐频道订阅，实测 42 帧；首笔逐笔已归一；断线在采集窗口内自动重连 | 盘口是无序号差分，不重放、不生造顶档 |
| bitbank `btc_jpy` | Engine.IO/Socket.IO 4.x wire 帧原样入 raw，实测 46 包；断线在采集窗口内自动重连 | `depth_diff` 序号只验证不回退，不能以跳号断言丢帧；顶档仅取 `depth_whole` 快照 |

## 2. 统一事实表结果

| 来源 | 规范化品种 | `trade_tick` | `book_top` | `stream_health` 结论 |
|---|---|---:|---:|---|
| GMO | BTC/JPY、ETH/JPY、XRP/JPY | 17,705 | 190,830 | 6 个 `snapshot` 窗口 |
| bitbank | BTC/JPY、ETH/JPY、XRP/JPY | 25,428 | 3 | REST 快照 1；实录 book 流 44 帧、无回退 |
| bitFlyer | BTC/JPY | 11,276 | 144 | 19 个完整快照、125 个 ticker 顶档；均为 `snapshot` |
| Binance | BTC/USDT | 1,299,165 | 0 | 归档数据，不适用实时流健康 |
| Coincheck | BTC/JPY | 1 | 0 | orderbook 41 帧、trades 1 帧，均为 `none` |

SQLite 共有 `trade_tick` 1,353,575 行、`book_top` 190,977 行、`stream_health` 12 行、`normalized_partition` 8 行和 `backfill_run` 25 行。P0 三家与 P1 Binance 的分区验证均通过：金额列为文本、`available_time >= event_time`、血缘位置非空、无失败分区。

## 3. 口径与富化规则

| 来源 | 成交标识与侧别 | 时间与数量 | 使用限制 |
|---|---|---|---|
| GMO | 无原生成交号，按原始位置合成稳定标识；侧别为吃单侧 | UTC 归档时刻；零量打印保留价格血缘 | 零量不进入成交量聚合 |
| bitbank | `transaction_id`；侧别为吃单侧 | `executed_at` 毫秒 | 日度逐笔可深回补 |
| bitFlyer | `id`；侧别为吃单侧 | `exec_date` UTC | 历史窗口约 31 日；增量板必须以快照重建 |
| Binance | `a`；由 `m` 反推吃单侧；保留 `f/l` | 2025 起微秒；数量为 base asset | `aggregate` 口径，USDT 与 JPY 不折算混同 |
| Coincheck | 成交数组的 id 与订单侧 | 秒级时刻 | itayose 或字段不全记录 raw，不猜测方向 |

所有金额在 normalized 层写入十进制文本；时间用整数 epoch 转 UTC ISO，拒绝超出微秒表达能力的时间戳，避免通过 `float` 失真。

## 4. 已隔离的数据缺陷

| 来源与分区 | 现象 | 处理 | 对可用性的影响 |
|---|---|---|---|
| GMO BTC 2026-08-07 | 377 条 `size=0.0000` 打印 | 正常入事实，保留 OHLC 价格路径 | 不计入量与成交额 |
| bitFlyer BTC_JPY 2026-08-07 | 5 条成交 `side` 为空 | raw 保留；分区标为 `complete_with_rejections`，不写方向性事实 | 当日方向性逐笔缺 5 条，约 0.04% |
| bitFlyer `lightning_board_*` | 为增量而非完整板 | 不直接取顶档；仅接受 `board_snapshot` 与 ticker | 需本地簿重建后才可做盘口研究 |
| Coincheck orderbook | 无序号差分 | 只登记健康窗口 `none` | 不可用于队列级或连续性证明 |
| raw 2026-08-08 `ws_public.jsonl` | 既有 1 条畸形 JSON | 原文件不修改，审计继续报告 | 不影响本次已验证分区 |

2026-08-07 另外发现 25 个未映射归档分区：GMO 的非共同日元品种 24 个及 bitFlyer `FX_BTC_JPY` 1 个。它们未被误映射到现货共同品种，也未进入本次统一事实表。

## 5. 数据规模与全量回补边界

三家归档覆盖合计约 260,498,477 行，其中共同映射日元现货的 bitbank 三品种约 193,541,152 行、GMO BTC/ETH/XRP 约 45,625,553 行、bitFlyer BTC_JPY 327,676 行。当前实现可按分区散列续跑，但尚未把这约 2.39 亿行全部物化进 SQLite；全量宽事实表及双索引需要先完成磁盘容量和运行窗口评估。

实际全量执行入口：

```text
python -m guvolu.venues.collect project-trades --venue gmo,bitbank,bitflyer --full
```

首次应先以单日和有限分区验证，再逐来源品种扩展。任何 `failed` 分区均阻断研究使用；`complete_with_rejections` 必须在查询和回测中显示隔离计数。

## 6. 验证命令

```text
python -m pytest tests/ -q
node --test tests/md_style.test.mjs
python -m mypy
python -m guvolu.data.persistence_audit --data-root data --mode full
```

## 7. 全量持久化审计

全量审计已输出为 [persistence-audit-full-2026-08-11.json](../data/export/persistence-audit-full-2026-08-11.json)：逐项检查 53,725 个文件、5,876,206,996 字节、52,842 个归档文件与 52,845 条覆盖登记。新建 P0/P1 分区的内容散列、行数、金额文本、时间顺序和血缘均通过；但整个历史数据集的 `loss_detected=true`、`fully_proven=false`，因此不得把本次分区验证扩大表述为“历史库零缺陷”。

审计的确定性错误是既有 `data/raw/2026-08-08/ws_public.jsonl` 第 42,288 行畸形 JSON。另有 18 个 manifest 后追加记录警告、4 个未完成运行、215,459 行和 16 个 manifest 缺少 durable-ack 版本证据，以及归档覆盖表与 heatmap 元数据缺少逐文件/逐源散列的可证明性盲区。原始介质保持不改写；这些问题应以隔离清单、补充 manifest 与后续内容散列台账修复。

接口事实依据：[Binance Public Data](https://github.com/binance/binance-public-data)、[Coincheck Exchange API](https://coincheck.com/documents/exchange/api)、[bitbank Public Stream](https://github.com/bitbankinc/bitbank-api-docs/blob/master/public-stream.md)。
