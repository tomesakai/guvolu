# 来源接口与公开行情采集设计

> 文档类别：长期维护，登记于 [docs/00-rules-registry.md](00-rules-registry.md)。
> 能力对照与证据等级见 [docs/venue-capability-matrix.md](venue-capability-matrix.md)，本文不重复对照，只承载**逐来源接口细节**与**公开行情采集器设计**。
> GMO 端点细节的唯一所在是 [能力调查报告](2026-08-05-gmo-api-capability-report.md)（W-07），本文 GMO 节只含采集设计参数。
> 标注「未核」的条目实施前须按 C-14 探测法补证。

## 1. 采集器公共约定

全部来源的公开行情适配器遵守同一只读协议；协议只统一请求窗口、原始包络与
错误形态，不把来源语义抹平：

1. **落盘先于解析**：收到即写不可变 raw，解析失败不影响原文落盘。
2. **订阅即快照**：凡差分型盘口流，连接建立后先取快照（REST 或带内频道），再应用缓冲差分；重连一律重走此流程，不假设增量连续（C-10 的推广）。
3. **校验按完整性等级分派**（等级定义见对照册第 5 节）：`checksum` 逐帧验、`sequence` 查缺口、`snapshot` 按来源状态机重锚、`none` 只作旁路诊断。结果进入 `l2-quality-v1` 或来源专用状态事实，不中断采集。
4. **限速器按模型分派**：固定速率、权重记账、计数器衰减三种实现，参数在各来源节声明。
5. **时间语义显式**：帧无事件时刻的来源，落库 `time_origin = local`；有则 `venue`。
6. **适配器只做协议差异**：地址、认证、分页、限速退避、字段提取与时间单位在来源层；PIT、方向、撮合粒度、snapshot/delta 重放与 fallback 在版本化 normalizer/物化层。
7. **来源描述符显式**：解析身份至少是 `venue_id + endpoint_id + endpoint_revision + payload_schema_version`；数组原生次序先声明再归一，Bybit 倒序、Binance 升序等差异不得靠调用者猜测。
8. **游标可恢复**：分页来源返回记录与下一游标；Kraken `since`、Coinbase `CB-AFTER`、Binance `fromId` 只在原件封口并通过校验后推进，重启续扫。
9. **归档优先但不越源**：同所官方归档优先于 REST 长回扫，REST 只补同所尾差；任何畸形 L2 update 使整个分区失败，不跳行后继续污染状态。

REST 盘口锚点不复用 WS L2 frame。bitFlyer `EP-0001@r0`、bitbank
`EP-0003@r0`、GMO `EP-0006@r0` 的请求与响应各自内容寻址，后台有界队列在
connection-open、reconnect 或 periodic 触发。锚点超时、限频或不可用只产生旁路
状态，不能阻塞订阅、补写断流或修正 WS 活动 head。

## 2. GMO Coin（角色：执行加行情，实测）

端点与频道细节见能力调查报告。采集设计参数：

| 项 | 设定 | 依据 |
|---|---|---|
| 公开 REST 限速 | 官方未公布明确上限；项目固定 3 次每秒总预算，ERR-5003 时 GET 限定退避 | 量级实测第 6 节；不得套用私有 GET 20 次每秒 |
| 私有 REST 限速 | 10 次每秒（R-04 保守值） | 实测零限速 |
| WS 订阅节奏 | 1.1 秒每命令 | 官方 1 次每秒限制 |
| 盘口采集 | WS 30 档快照流直落 raw；500 档走 REST 定时 | 层 1 与层 2 语义 |
| MON 盘口刷新 | 前端 500ms（2Hz）；查询服务按品种 500ms 单飞缓存 | 当前 3 req/s 总预算下的最高可持续档；实测 WS 约 2.12 帧每秒 |
| 校验 | `snapshot` 型：帧自足，帧间隔异常写 L2 quality；REST 深簿只作 approximate anchor | 帧即快照；无同序身份 |
| K 线回补 | 短周期按日、长周期按年，先探测年份再拉取 | ERR-5207 边界 |

## 3. bitFlyer（角色：日元行情加衍生品信号，实测）

REST 基址 `https://api.bitflyer.com`；实测细节与权限清单见 [实测快照](2026-08-07-bitflyer-api-verification.md)。

公开 REST：

| 端点 | 参数要点 | 上限与边界 |
|---|---|---|
| `GET /v1/markets` | 别名 `getmarkets` | 9 品种（实测） |
| `GET /v1/board` | `product_code` | 全簿无上限（实测 1,243 档） |
| `GET /v1/ticker` | `product_code` | 含 `tick_id`、`timestamp` |
| `GET /v1/executions` | `count`、`before`、`after`（id 域分页） | 单次 500，超量静默截断；**31 天**外报 -156 |
| `GET /v1/getboardstate` | `product_code` | 板状态与健康度分级 |
| `GET /v1/gethealth` | `product_code` | 同上 |
| `GET /v1/getfundingrate` | `product_code=FX_BTC_JPY` | 实测可用 |
| `GET /v1/getfundingratehistory` | 未核 | 权限清单在列 |

私有能力：13 个读取 REST 端点（签名 `HMAC-SHA256(timestamp + method + path含查询串 + body)`，头 `ACCESS-KEY/TIMESTAMP/SIGN`）加 4 个私有 WS 频道；另有 5 个写 REST 端点。权限清单见实测快照第 1 节；全撤为 `POST /v1/me/cancelallchildorders`。

公开 WS（JSON-RPC 2.0，`wss://ws.lightstream.bitflyer.com/json-rpc`）：

| 频道 | 形态 | 实测节奏 |
|---|---|---|
| `lightning_board_snapshot_{p}` | 全簿节流快照 | 每 5.0 秒一帧，约 21 KB |
| `lightning_board_{p}` | 差分，键仅 `asks/bids/mid_price` | BTC_JPY 4.4 帧每秒 |
| `lightning_ticker_{p}` | 快照 | 约 1.5 帧每秒 |
| `lightning_executions_{p}` | 逐笔数组 | 现货约 0.08 帧每秒 |

订阅 `{"jsonrpc":"2.0","method":"subscribe","params":{"channel":名},"id":n}`；私有频道先 `auth`（`HMAC-SHA256(timestamp + nonce)`，实测通过）。

采集设计：

| 项 | 设定 |
|---|---|
| REST 限速 | 固定 1.5 次每秒（上限 500 次每 5 分钟的九成） |
| 盘口维护 | WS snapshot 频道到帧即整簿重置；board 差分逐帧应用；带内 snapshot 是同一来源状态机的重锚。独立 REST board 只作 approximate/unknown 诊断，不回写 WS |
| 时间语义 | 盘口两频道无事件时刻，`time_origin = local`；ticker 与 executions 取帧内时刻 |
| 逐笔回补 | 以 `before` 游标回扫至 31 天边界；常态每小时前向增量；**采集空洞不得超 31 天** |
| 断连 | 不补发；重连即重订阅，缺口段标记不完整 |

## 4. bitbank（角色：日元行情，文档）

公开 REST 基址 `https://public.bitbank.cc`：

| 端点 | 要点 |
|---|---|
| `GET /{pair}/ticker`、`/tickers`、`/tickers_jpy` | 全对或日元对 |
| `GET /{pair}/depth` | 200 档，熔断期合计 400 |
| `GET /{pair}/transactions/{YYYYMMDD}` | **按日全量逐笔**；省略日期返回最新 60 |
| `GET /{pair}/candlestick/{type}/{YYYY 或 YYYYMMDD}` | 短周期按日、长周期按年（与 GMO 同型） |
| `GET /{pair}/circuit_break_info` | 熔断状态 |

私有 REST 基址 `https://api.bitbank.cc/v1`（头 `ACCESS-KEY/NONCE 或 REQUEST-TIME/SIGNATURE`；GET 签名为 `nonce + 全路径含查询串`，POST 为 `nonce + body`；时间窗默认 5000 毫秒上限 60000）。查询 10 次每秒、更新 6 次每秒，超限 429。

公开流：socket.io 4.x，`wss://stream.bitbank.cc`。频道 `ticker_{pair}`、`transactions_{pair}`、`depth_whole_{pair}`（200 档快照，含 `sequenceId`）、`depth_diff_{pair}`（含 `s`）、`circuit_break_info_{pair}`。私有流经 `GET /user/subscribe` 取 PubNub 令牌（未核细节）。

采集设计：

| 项 | 设定 |
|---|---|
| REST 限速 | 固定 5 次每秒以内 |
| 盘口维护 | `depth_whole` 重置本地簿，缓冲 `depth_diff` 按序号应用；两个 room 各自严格递增但数值不要求连续；同 room 回退计 regression，跨 room 同序合法 |
| 逐笔回补 | 按日逐对回扫（唯一可深回补的日元逐笔源），冷启动优先执行 |
| 依赖注意 | socket.io 协议需专用客户端库，引入前按 C-05 评估；不可用裸 websockets |

## 5. Coincheck（角色：日元行情备用，文档）

公开 REST 基址 `https://coincheck.com`：`/api/ticker`、`/api/trades`、`/api/order_books`、`/api/rate/{pair}`、`/api/exchange_status`。无 K 线端点。私有：`ACCESS-KEY/NONCE/SIGNATURE`（`HMAC-SHA256(nonce + url + body)`）；下单 4 次每秒、委托详情 1 次每秒。

公开 WS `wss://ws-api.coincheck.com`：订阅 `{"type":"subscribe","channel":"btc_jpy-trades"}` 等；`{pair}-orderbook` 为差分、无序号、带 `last_update_at`；私有 `wss://stream.coincheck.com/private`，明示消息不重发。

采集设计：完整性 `none`——本地簿只能配合定时 REST `order_books` 旁路对照；差分档数上限未核。作备用源，仅在 bitbank 失效时临时启用 trades 频道，不承担盘口研究。

## 6. Binance（角色：全球参考加长历史，文档）

REST 基址 `https://api.binance.com`（纯行情可用 `https://data-api.binance.vision`）。权重制：`klines` 2、`aggTrades` 4、`trades/historicalTrades` 25、`depth` 按档 5/25/50/250，每 IP 6,000 权重每分钟，超限 429 且续犯封 IP。

| 端点 | 要点 |
|---|---|
| `GET /api/v3/klines` | 16 周期（1s 至 1M），单次 1,000 |
| `GET /api/v3/depth` | 至 5,000 档 |
| `GET /api/v3/trades` 与 `historicalTrades` | 单次 1,000；历史按 `fromId` 全回溯 |
| `GET /api/v3/aggTrades` | 聚合口径（`trade_tick.match_granularity = aggregate`，保存 `a/f/l`） |
| `GET /api/v3/exchangeInfo` | 品种规则与限速对象 |

WS `wss://stream.binance.com:9443`：`{sym}@depth@100ms` 差分（`U/u/lastUpdateId` 规则）、`@trade`、`@kline_{i}`、`@bookTicker`；单连接 1,024 流、入站 5 条每秒、单 IP 300 连接每 5 分钟、24 小时强制断连。

归档站 `https://data.binance.vision/data/spot/{daily,monthly}/{klines,trades,aggTrades}/...`，zip 加 `.CHECKSUM`。

采集设计：限速器为**权重记账**（每分钟额度按 `exchangeInfo` 声明取半自用）；盘口维护按官方序号流程（REST 快照引导、`U/u` 连续性断裂即重建）；历史回补优先走归档站而非 REST（校验文件核对后入 raw）；24 小时断连按计划重连处理，不计异常。

## 7. Kraken（角色：全球参考加逐笔全历史，文档）

REST：`GET /0/public/OHLC`（9 周期，仅近 720 根）、`GET /0/public/Trades`（`since` 纳秒游标，1,000 笔每页，**自首笔全历史**）、`GET /0/public/Depth`。私有计数器：上限 15/20/20、衰减 0.33/0.5/1 每秒按验证等级；历史类调用计 2。

WS v2 `wss://ws.kraken.com/v2`：`book`（深度 10/25/100/500/1000，快照加增量，**前 10 档 CRC32**）、`ticker`、`trade`、`ohlc`；私有 `wss://ws-auth.kraken.com/v2`，令牌经 REST `GetWebSocketsToken`（未核细节）。

认证 WS 另有 [`level3`](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/level3)
订单级频道：订阅深度只允许 10、100 或 1000 个**价位**；snapshot 和后续
`add/modify/delete` 均带公开 `order_id`、价格、剩余数量与订单时刻，前十价位提供
CRC32。官方状态机明确“不需要 sequence”；消费者须在每次更新后自行裁剪至订阅
深度，价位跌出范围时也不会收到 delete。它因此是可重放但深度受限的 L3/MBO，
不是普通 `book` L2 的别名。

采集设计：限速器为**计数器衰减**模型；普通 L2 与 L3 分开建端点修订、sequence
domain 和 checkpoint，均逐帧算 CRC32，失败即重订阅并计 `checksum_failures`。
L3 是 Coinbase 之后的第二接入候选，先验证令牌、深度裁剪与断连重锚；当前仓库
没有 Kraken L3 connector、封口 raw、物化事实或活动 head。逐笔全历史回补以
`since=0` 起纳秒游标推进，进度持久化于游标文件，可断点续扫；K 线不采（720 根
无回补价值），由逐笔自聚合（`origin = derived`）。

## 8. OKX（角色：全球参考加历史 L2，文档加实测）

REST 基址 `https://www.okx.com`：`/api/v5/market/candles`（300 根，近 1,440 根）、`history-candles`（100 根每页）、`books`（400 档）、`trades`（500）、`history-trades`（100 每页）。多数行情端点 20 次每 2 秒每 IP，历史端点 10 次每 2 秒。

WS `wss://ws.okx.com:8443/ws/v5/public`：`books`（公开 400 档增量，100 毫秒）、`books5`、`bbo-tbt`；2026-04 起 `books50-l2-tbt` 与 400 档逐笔 `books-l2-tbt` 均需 VIP4。2026-06 起 checksum 已弃用并固定为 0，完整性改由 `seqId/prevSeqId` 判断。连接 3 次每秒每 IP；单连接订阅类操作 480 次每小时；30 秒无推送自动断。

官方历史市场数据另提供 2023-03 起高分辨率 L2 下载。2026-08-11 已实测 `BTC-USDT` 400 档日档：`.tar.gz` 内单个 JSONL 成员，`snapshot` 双侧 400 档，后续 `update` 是绝对数量 set/delete，每约十五分钟重锚；档位为 `[price,size,order_count]`。文件没有 `seqId/prevSeqId/checksum`，只可按文件 SHA、严格递增 `ts` 与周期快照验证。下载使用页面计划接口、目标级互斥、Range/If-Range checkpoint 与原子 manifest，物化只接受 sealed 文件。

实时 `books` 已有 raw v3、`seqId/prevSeqId` 状态机和 L2 normalization v5 的有界
实现并已通过真实有界隔离小样本，但仍不是连续或生产能力。live 与历史文件不共用
完整性声明；同市场成品由 OFL v8 在 live 实际覆盖内选 live，覆盖外才 fallback archive，
而不是逐帧混池。实测历史细节见
[OKX 小样本验证](2026-08-11-okx-l2-sample-validation.md)，当前验收状态见
[2026-08-13 L2 质量与 L3 就绪度快照](2026-08-13-l2-quality-and-l3-readiness.md)。

## 9. Bybit（角色：全球参考备用，文档）

REST 基址 `https://api.bybit.com`：`/v5/market/kline`（13 周期，单次 1,000，返回倒序 list[0] 为最新）、`orderbook`、`tickers`、`recent-trade`、`funding/history`。2026-07 官方变更另增 Spot full depth，单侧最多 10,000 档，本地尚未实测。公开侧 600 次每 5 秒每 IP，超限 403 锁 10 分钟。WS `wss://stream.bybit.com/v5/public/{category}`：`orderbook.{depth}.{symbol}` 快照加增量带序号、`publicTrade`、`tickers`。单 IP 500 连接每 5 分钟。

官方文档明确历史成交与 K 线等下载，但 2026-08-11 对当前公开目录的实探未取得 orderbook 根目录，既有候选路径返回不可用。采集设计：实时备用地位，不常开；接入时按 `u/seq` 维护 snapshot 加 delta。历史 L2 保持 `unverified/blocked`，只有取得真实文件 URL、许可和样本后才能重新进入回补计划。

## 10. Coinbase Exchange（角色：全球参考备用，文档）

REST：`/products`、[`/products/{id}/book?level=1|2|3`](https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/get-product-book)、`/products/{id}/candles`（周期 60/300/900/3600/21600/86400 秒，单次 300 根）、`/products/{id}/trades`（CB-BEFORE/CB-AFTER 游标分页）。公开 10 次每秒峰值 15（令牌桶）。Level 1/2 聚合，Level 3 返回完整非聚合订单簿。

[`full` 频道](https://docs.cdp.coinbase.com/exchange/websocket-feed/channels#full-channel)
提供订单生命周期与成交更新。官方 L3 引导流程固定为：先订阅并缓冲 WS 消息，
再取 REST Level 3 snapshot，丢弃 `sequence <= snapshot.sequence` 的缓冲消息，按序
重放剩余消息，最后转入实时应用。`open` 与 `match` 必然改变订单簿，`done` 与
`change` 只有在订单已经 resting 时才改变订单簿；`match` 同时给出
`maker_order_id`、`taker_order_id`、`trade_id`、价格和数量，`side` 表示 maker 侧。

采集设计：Coinbase Full 加 REST Level 3 是首个 L3 接入候选；必须实现上述
buffer/snapshot/discard/replay 状态机、序列缺口重锚和订单生命周期校验，再由 L3
确定性派生 L2。它仍可作为美元/欧元参考票，但不再被错误收窄为“仅 ticker”。
当前仓库没有 Coinbase L3 connector、封口 raw、物化事实或活动 head；候选排序
不代表已经接入。

### 10.1 其他 L3/MBO 候选（文档；本地未接入）

- Bitfinex 公共 WS [`Raw Books`](https://docs.bitfinex.com/reference/ws-public-raw-books)
  使用 `prec=R0`，条目为 `order_id/price/amount`，`price=0` 表示删除；可选
  checksum。但 `len` 只允许 1、25、100、250，而且表示**订单数而非价位数**，
  所以最多 250 单是截断 MBO（B 级），不能声明为完整 L3。先做 250 单小样本，
  验证越界、重连和 checksum 后再决定是否接入。
- Bitstamp 在外部 L3 登记工作簿中以 `group=2` 加 live-orders 流列为 B 级候选；
  [官方 API 页面](https://www.bitstamp.net/api/) 与官方 live-orders 示例尚不足以
  在本地证明稳定 order ID、snapshot/update 边界、顺序和缺口恢复。它只进入
  小样本验证队列，在验证前不得登记为可重放 L3，也没有本地 connector 或事实。

## 11. Hyperliquid（角色：链上永续补充，未核为主）

`POST /info` 公开查询（K 线、盘口、资金费率），`candleSnapshot` 字段 `t/o/c/h/l/v/i/s/n`，官方文档载明仅最近 5,000 根；`l2Book` 最多返回单侧 20 档，WS 也提供 `l2Book` 订阅。权重 1,200 每分钟每 IP；WS 订阅数与连接数上限数值未核。写操作为链上签名，与本项目无关。仅当链上永续资金费率进入因子研究（阶段 9 后）时评估，当前不实施。

## 12. 与既有台账的关系

- 本文第 2 至 11 节的采集参数是 TBD-23（接入范围）各序的实施细则；接入某来源时按对应节实施并在 [architecture.md](architecture.md) 台账标注。
- 校验、质量、市场状态与 REST anchor 的落库形态见
  [订单流数据契约](order-flow-data-contract.md) 第 3.5 节和
  [物化设计](materialization-design.md) 第 12.6 节。
- 「未核」项集中于 bitbank 私有流、Kraken L3 令牌实测、OKX 衍生品端点与
  5000 档历史产品、Bybit 历史深度、Coinbase L3 订阅小样本、Bitstamp
  `group=2`/live-orders 重放语义、Hyperliquid 成交映射与 WS 配额——均不阻塞
  现行阶段。
