# 多来源 API、回补与归一化完整验证

> 文档类别：时效快照，调查时点 2026-08-10，登记于
> [docs/00-rules-registry.md](00-rules-registry.md)。
> 本快照纠正 2026-08-07 调查中“盘口历史全部不可回补”的错误结论；旧快照冻结，现行结论以
> [venue-capability-matrix.md](venue-capability-matrix.md) 为准。

## 1. 结论

多来源能力并非“十所都已接入”。截至本次验证，GMO、bitbank、bitFlyer 已有采集或归档实现；全球六所只有设计和官方文档核证。必须把三件事分开陈述：来源是否公开提供、仓库是否已实现、现有本地数据是否已覆盖。任何一项不得替代另外两项。

现有“盘口历史全部不可回补”结论错误。官方可得形态如下：

| 来源 | 官方历史盘口 | 可重放性 | 可补用途 | 结论 |
|---|---|---|---|---|
| OKX | 2023-03 起高分辨率 L2 下载 | 取决于下载产品字段，导入前逐文件验 schema | L2 研究与部分缺口 | 可回补，尚未接入 |
| Bybit | 官方历史数据目录列明 orderbook | 取决于产品、日期与快照频率 | 快照研究与部分缺口 | 可回补，尚未接入 |
| Binance 现货 | 官方公开归档仅列 K 线、逐笔与聚合逐笔 | 无 | 无 | 仍须自录或商业源 |
| Binance 衍生品 | 部分市场有每日 `bookDepth` | 仅百分比带宽聚合，不是逐档 L2 | 深度带因子 | 可补聚合深度，不可补逐档簿 |
| 其余已核来源 | 未发现官方历史 L2 产品 | 无 | 无 | 保持自录或商业源 |

因此，`backfillable` 单一布尔值不足以表达能力，必须拆为 `backfill_mode` 与 `replay_fidelity`。能下载聚合深度不等于能恢复订单簿，能恢复快照也不等于能重放增量队列。

## 2. 官方证据与纠错

| 项 | 旧结论 | 核证结果 | 纠正动作 |
|---|---|---|---|
| bitbank 成交粒度 | 每撮合成对买卖两行 | 官方与本地样本均为一个 `transaction_id` 对应一行和一个 side | 禁止双写，单行归一 |
| Coinbase `side` | 直接当吃单方向 | 官方 `GET product trades` 定义为 maker side | 入库前反向为 taker side，并保存源侧依据 |
| Binance aggTrades | 用 `a-1` 构造 sequence | 官方给 `a/f/l/m`，没有 `a-1` 字段 | `a` 作聚合成交 id，另存 `f/l`，禁止造序号 |
| Bybit K 线周期 | 12 档 | 官方列出 `1,3,5,15,30,60,120,240,360,720,D,W,M` 共 13 档，数组倒序 | 对照册改 13，解析先声明顺序 |
| OKX 逐笔盘口等级 | 50 档 VIP4、400 档 VIP5 | 2026 变更记录显示两者均为 VIP4；checksum 已弃用并固定 0 | 以 `seqId/prevSeqId` 校验，不再算 checksum |
| 盘口历史 | 十所全部不可回补 | OKX 与 Bybit 有官方历史订单簿；Binance 衍生品有聚合 `bookDepth` | 按保真度重新登记 |

官方证据入口：

- [bitbank Public API](https://github.com/bitbankinc/bitbank-api-docs/blob/master/public-api.md)
- [Binance Public Data](https://github.com/binance/binance-public-data/blob/master/README.md)
- [Binance Spot Market Data](https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints)
- [Coinbase Product Trades](https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/get-product-trades)
- [Bybit Kline](https://bybit-exchange.github.io/docs/v5/market/kline)
- [Bybit Historical Market Data](https://www.bybit.com/future-activity/developer)
- [OKX Historical Market Data](https://www.okx.com/historical-data)
- [OKX API Change Log](https://my.okx.com/docs-v5/log_en/)
- [Kraken Downloadable OHLCVT](https://support.kraken.com/articles/360047124832-downloadable-historical-ohlcvt-open-high-low-close-volume-trades-data)

## 3. 归一化合同

归一化输出必须同时回答“值是什么”和“值为何能这样解释”。成交统一为 taker 方向，但随行保存 `source_side_basis`；聚合成交保存首尾原生成交 id；任何来源、端点与频道的时间单位保存在能力版本，不再用来源级单值概括。

| 字段组 | 必填内容 | 拒绝条件 |
|---|---|---|
| 血缘 | `raw_source`、`raw_item_index`、`normalization_version`、`schema_version` | 找不到原始行或数组位次 |
| 时间 | `event_time`、`available_time`、`ingest_time`、`time_origin` | 单位未核、时区不明、时刻倒置 |
| 标识 | `venue_trade_id`、`id_origin`、可选 `first_trade_id/last_trade_id` | 人造来源 id 或无位次的合成 id |
| 侧别 | `side=taker`、`source_side_basis` | maker/taker 语义未核 |
| 数值 | `price/size` 十进制字符串 | 浮点输入、非正值、科学计数法未规范化 |
| 粒度 | `match_granularity` | 聚合与逐笔未区分 |

Hyperliquid 等未完成侧别语义核证的映射应 fail closed，不得为了“覆盖率”猜测。能力审计将“公开但未实现”“实现但证据过期”“字段未核”分别列为不同缺口。

## 4. 富化落库与自我纠错

数据库版本新增五类追加式事实：端点能力版本、规范化成交、顶档帧、回补任务、分析台账。旧事实不覆写，修订用 `revision_id` 或新能力版本追加。

自我纠错结构由四个闭环组成：

1. 能力版本带 `evidence_uri/surveyed_at/valid_until`，过期自动成为审计缺口。
2. 归一化版本随事实落库，同一 raw 可重建新 revision，旧 revision 保留供差分。
3. 回补任务记录计划日、成功、缺失、空文件、行数、校验失败与失败原因，不以“进程成功退出”代替数据完整。
4. 信号分析台账保存四判定全量结果、逐条件分数、基线散列、源码版本与配置散列；`book_feature` 只保存成立事件并引用台账。

## 5. 最小稳健上线门槛

| 等级 | 必须满足 | 当前判断 |
|---|---|---|
| P0 数据安全 | raw 先落盘、金额文本、时间三元、schema 版本、来源与品种稳定键 | 既有主链具备，新增来源须继承 |
| P0 能力诚实 | 能力证据不过期；未实现与未提供分开；未核映射拒绝运行 | 本次实现审计器 |
| P0 回补可验 | 每任务有覆盖统计、checksum 或文件散列、缺日与空日分开 | 台账结构与写入器已补齐；现有回补命令尚未接线 |
| P0 实时盘口 | 自录进程常开、断连重建、流健康窗口、空窗明确显示 | 日元域仍必须常开；流健康事实尚未接线 |
| P0 信号可复读 | 全判定台账、基线散列、版本双钥、样本不足拒判 | 本次实现 |
| P1 多源研究 | 至少两种独立市场结构来源完成同口径逐日对账 | 尚未满足 |
| P1 交易决策 | PIT 特征、交易成本、风险与执行状态接入统一 run | 尚未满足 |

最少上线不是把所有来源都长期开着。查询 API 可按需运行；K 线、逐笔归档和官方历史下载可以定期获取；GMO、bitFlyer、bitbank 的不可回补实时盘口，以及任何要用于实时信号的来源，采集器必须持续运行。OKX 与 Bybit 历史产品只能降低冷启动成本，不能保证补回下载尚未发布的最近窗口。

## 6. 现有本地回补统计

本次读取现有 `archive_coverage`：bitbank `btc_jpy` 已登记 3,456 个成功日、2 个缺失日、5 个空日、72,192,223 行；`eth_jpy` 2,267 个成功日、28,323,973 行；`xrp_jpy` 3,361 个成功日、1 个缺失日、93,024,956 行。bitFlyer `BTC_JPY` 与 `FX_BTC_JPY` 各 32 日，分别 327,676 与 934,313 行。

这些数字证明“任务跑过”，不单独证明归一化正确。上线前仍需每来源完成：首尾时刻、单调性、重复 id、非正价格量、侧别域、逐日行数异常、与 K 线或归档恒等式的交叉验证。

## 7. 信号检测独立结论

现实现的四类公式算术可复算，但不具备生产标定：吸收滑窗成立率过高；成立事件的 `confidence` 因阈值封顶而恒为 1；仅保存成立结果造成幸存者偏差；基线与源码身份未入请求散列；延载列会污染样本；规则覆盖项除 `min_confidence` 外被静默忽略。

本次实现将 `confidence` 明确定义为 `rule-strength-min-v2`，它是最弱条件的有界强度，不是概率。阈值处为 0.50，越过阈值后随裕量增加，未达阈值低于 0.50；每个条件保存 observed、comparator、threshold、met 与 score。基线不足 30 个有效非延载样本时四类均不得成立。真正概率置信度仍需带标签数据做校准，不能由规则裕量冒充。

## 8. 仍未解决的遗漏

- `trade_tick`、`book_top`、`backfill_run` 与 `stream_health` 的 schema、幂等写入器和映射测试已完成，但现有采集命令仍以 raw 与覆盖登记为主；本次验收时四表均无生产事实。接线及逐分区投影完成前，不得宣称“新增来源已经富化落库”。
- OKX 与 Bybit 历史订单簿尚未逐文件下载，确切字段、频率、品种与许可范围仍须在接入任务中探测。
- Binance 衍生品 `bookDepth` 存在官方仓库问题报告中的异常值，使用前必须与标记价交叉检查。
- 多来源参考价需要独立 JPY/USD 腿；缺失时不得用 BTC 跨所差价反推汇率。
- 自动全窗信号扫描器与有标签标定仍未实现；本次只修正区域分析的可审计性。
- 商业历史盘口只有在目标策略需要订单队列级回测、且官方历史产品保真度不足时才有必要引入。
