# 2026-08-24 日本交易所 API 全集与 L3 候选核对

> 时效快照（W-02）：内容冻结于 2026-08-24，修订以新快照发布。
> 本文核对金融庁 2026-08-21 版暗号資産交換業者登録一覧全部 27 家的公开 API 存在性
> 与能力、日本境内订单级数据的非加密路径，以及全球 L3 候选自
> [来源能力对照册](venue-capability-matrix.md) 第 5.2 节登记以来的官方变化。方法为逐家
> 阅读官方 API 文档、费率页与规约原文，本文不含本仓库探测法实测；证据等级沿用对照册
> 口径（实测、文档、未核）。GMO、bitFlyer、bitbank 的实测结论以既有快照为准，本文只补
> 充文档层差异，不复述实测数值。
> 本文局部用语：逐单事件流（L3，MBO）指按单个委托的新增、修改、撤销与成交事件发布
> 并带委托标识的盘口数据；按价聚合盘口（L2，MBP）指按价位聚合数量的盘口数据；重放锚点
> 指能把增量流与某一全量快照对齐的序号或校验字段；成交级归属指成交记录带有买卖双方或
> 受理侧委托标识，但不含委托生命周期事件；再配信指向第三方转发或再分发所取得的 API
> 数据；法人路径指只对法人签约开放、无公开自助接入的数据产品。
> 本次同步登记：本文入 [00-rules-registry.md](00-rules-registry.md) 文档清单；对照册
> 第 1 节补行与补注、第 5.2 节 L3 候选表与第 7 节自定义委托号按本文修订，均标
> 「2026-08-24 文档核实」。

## 1. 范围与方法

| 核对项 | 口径 |
|---|---|
| 名单 | 金融庁暗号資産交換業者登録一覧 2026-08-21 版，27 家；BITPOINT 已于 2026-04-01 并入 SBI VCトレード，不单列；已退出业者另列第 2.3 节 |
| API 存在性 | 是否对个人或法人账户公开提供可自助接入的交易 API 文档；販売所、OTC、ATM、托管与未开业者计为无 |
| 能力项 | 逐笔、盘口、K 线、官方归档、重放锚点、成交归属、BTC/JPY 最小数量与 tick、费率、自定义委托号、权限分离、限速、规约对再配信的限制 |
| 证据等级 | 「实测」引用既有快照；「文档」指在官方页逐字核实；「未核」指本次未在官方页取得原文，不得据以实施（A-04） |
| URL | 逐条官方地址统一列于第 9 节；正文不重复 |

## 2. 金融庁登録 27 家的 API 存在性

### 2.1 有公开交易 API（10 家）

| 业者 | 覆盖 | 接口形态 | 证据 |
|---|---|---|---|
| GMOコイン | 现物加杠杆 | REST 加 WS，官方逐笔 csv.gz 归档 | 实测（既有快照）加文档 |
| bitFlyer | 现物 Lightning 加 FX | REST 加 JSON-RPC WS | 实测（既有快照） |
| bitbank | 现物 | REST 加 WS，私有流为 PubNub | 实测（既有快照） |
| Coincheck | 取引所现物 | REST 加 WS | 文档 |
| Zaif | 现物 | REST 加 WS | 文档 |
| BTCBOX | 现物 | REST v1，无 WS | 文档 |
| OKJ（旧 OKCoin Japan） | 现物 | REST v3 加 WS v3 | 文档 |
| BitTrade（旧 Huobi Japan） | 现物 | REST 加 WS | 文档 |
| Binance Japan | 现物，BTCJPY 自 2024-03-12 | 共用 `api.binance.com` 接口族 | 文档 |
| 楽天ウォレット | 仅証拠金取引所（杠杆） | REST 加 WS；现物无 API | 文档 |

### 2.2 无公开交易 API（17 家）

| 业者 | 业态 | 说明 | 证据 |
|---|---|---|---|
| SBI VCトレード（含 2026-04-01 吸收合并的 BITPOINT） | 取引所加販売所 | 官方称「API 公開予定」；取引所 Maker -0.01%、Taker 0.05%，最小 0.00000001 BTC | 文档 |
| Mercoin | 販売所 | 规约禁止第三方程序接入 | 文档，见本快照调研记录 |
| Coin Estate | 販売所 | 无 API | 文档，见本快照调研记录 |
| FINX JCrypto | 販売所 | 无 API | 文档，见本快照调研记录 |
| S.BLOX | 販売所 | 无 API | 文档，见本快照调研记录 |
| OSL Japan（旧 CoinBest） | 取引所加販売所 | 无公开 API 文档 | 文档，见本快照调研记录 |
| CoinTrade | 販売所 | 无 API | 文档，见本快照调研记录 |
| BACKSEAT | 販売所 | 无 API | 文档，见本快照调研记录 |
| マネーパートナーズ | 仅 CFD | 无加密 API | 文档，见本快照调研记录 |
| Crypto Garage | OTC | 无公开 API | 文档，见本快照调研记录 |
| HashKey Japan（旧東京ハッシュ） | 電話 OTC | 无公开 API | 文档，见本快照调研记录 |
| COINHUB | ATM | 不适用 | 文档，见本快照调研记录 |
| ガイア | BTM | 不适用 | 文档，见本快照调研记录 |
| Custodiem（旧 FTX Japan、Liquid） | 托管 | 无交易 API | 文档，见本快照调研记录 |
| Laser Digital Japan | 机构业务，2026-08-21 新登録 | 无公开 API | 文档，见本快照调研记录 |
| Gate Japan | 未开业 | 不适用 | 文档，见本快照调研记录 |
| デジタルアセットマーケッツ | 法人 | FIX 直连，无公开文档 | 文档，见本快照调研记录 |

### 2.3 已退出业者（不在 27 家内）

| 业者 | 状态 |
|---|---|
| LINE BITMAX | 2026-06-01 终止服务 |
| DMM Bitcoin | 2025-03-08 廃業，口座移管至 SBI VCトレード |
| Kraken Japan | 2023-01-31 退出 |
| Coinbase Japan | 2023-02 退出 |

## 3. 有 API 十家的能力对照

### 3.1 公开行情、历史与 K 线

| 来源 | 逐笔 | 官方逐笔归档 | 盘口 | K 线 | 证据 |
|---|---|---|---|---|---|
| GMOコイン | REST `trades` 分页，无成交 id | 有，csv.gz 自 2018 | WS `orderbooks` 全量快照 | 12 周期 | 实测（既有快照）加文档 |
| bitFlyer | `executions` 仅 31 天，含 `child_order_acceptance_id` | 无 | `board` 差分，断连不补 | 无端点 | 实测（既有快照） |
| bitbank | `transactions` 按日全量 | 无 | `depth` 200 档；WS `depth_whole` 加 `depth_diff` | 11 周期 | 实测（既有快照） |
| Coincheck | REST `trades` 分页；WS `trades` | 无 | WS `orderbook` 差分，无快照说明 | 无端点 | 文档 |
| Zaif | `trades` 最近 150 | 无 | `depth` 150 档；WS 合并快照 | 未核 | 文档 |
| BTCBOX | `orders` 最近 100 | 无 | REST `depth`，无 WS | 无端点 | 文档 |
| OKJ | `trades` 游标，单次 100 | 无 | WS `depth400`：首推 `partial`，后续 `update` | 12 周期 | 文档 |
| BitTrade | WS `trade.detail` | 无 | `depth` step0 至 step5，带 `version` | 9 周期 | 文档 |
| Binance Japan | Binance 通用 `trades` 与 `aggTrades` | 有，`data.binance.vision` 日度 | `depth` 差分加 REST 快照引导 | Binance 通用 | 文档 |
| 楽天ウォレット | `trades` 60 条 | 无 | `orderbook` 全量快照 | 10 周期 | 文档 |

### 3.2 盘口重放锚点与成交归属

| 来源 | 盘口流模型 | 重放锚点 | 成交归属 | 完整性等级（本文判定） |
|---|---|---|---|---|
| GMOコイン | 全量快照 | 无 | 无 | `snapshot`（帧即快照） |
| bitFlyer | 差分加节流快照 | 无 | `child_order_acceptance_id`（成交级归属） | `snapshot` |
| bitbank | 快照加差分 | `sequenceId` 单调不连续 | 无 | `monotonic` |
| Coincheck | 差分 | 无 | taker 与 maker order id（成交级归属） | `none` |
| Zaif | 合并快照 | 无 | 无 | `snapshot`（帧即快照） |
| BTCBOX | 仅 REST 轮询 | 不适用 | 无 | 不适用 |
| OKJ | `partial` 加 `update` | CRC32 checksum，仅覆盖前 25 档；无 `seqId` | 无 | `checksum`（有界于前 25 档） |
| BitTrade | 全量推送 | `version` 字段，非连续序号 | 无 | `snapshot`（帧即快照） |
| Binance Japan | 差分加 REST 快照引导 | `U/u` 连续性规则 | 无，Binance `trade` 流 2024-06-18 起不含买卖方订单 id | `sequence` |
| 楽天ウォレット | 全量快照 | 无 | 无 | `snapshot`（帧即快照） |

完整性等级定义沿用对照册第 5 节，`monotonic` 的含义见对照册 bitbank 行。L2 完整性
证据由强到弱为 Binance Japan、OKJ、bitbank，其余均无序号。

### 3.3 BTC/JPY 交易参数与费率

| 来源 | 最小数量 | tick | Maker / Taker | 备注 | 证据 |
|---|---|---|---|---|---|
| GMOコイン | 0.00001 BTC | 1 日元 | -0.01% / 0.05% | 取引所现物 | 文档 |
| bitFlyer | 0.001 BTC | 未载 | 0.01% 至 0.15% 按月交易额分档，不区分 Maker 与 Taker | Lightning 现物 | 文档 |
| bitbank | 0.0001 BTC | 1 日元 | 0.00% / 0.10%（2026-02 起） | 取引所 | 文档 |
| Coincheck | 0.005 BTC 且不低于 500 日元 | 未载 | 主要对 0 / 0 | 取引所；杠杆 2020-03 终止 | 文档 |
| Zaif | 0.001 BTC，步进 0.0001 | 5 日元 | 0 / 0.1% | 信用取引 2026-08-05 废止 | 文档 |
| BTCBOX | 0.00001 BTC | 未载 | 0.05% / 0.05% | 取引所 | 文档 |
| OKJ | 0.00005 BTC | 1 日元 | Lv1 0.07% / 0.14% | 取引所 | 文档 |
| BitTrade | 0.00001 BTC | 1 日元 | 0 / 0.10%（2025-12-22 起） | 取引所 | 文档 |
| Binance Japan | minQty 0.000001 BTC，minNotional 100 日元 | 1 日元 | 0.10% / 0.10%，JPY 对不享 BNB 与 VIP 折扣 | 现物 | 文档 |
| 楽天ウォレット | 0.01 BTC | 未载 | -0.01% / 0% | 証拠金取引所 | 文档 |

### 3.4 私有 API、权限、限速与规约

| 来源 | 自定义委托号 | 权限分离 | 限速 | 规约对再配信的限制 | 证据 |
|---|---|---|---|---|---|
| GMOコイン | 无 | READ_ONLY 与 TRADE 双密钥，交易所侧固定 | 私有 GET 与 POST 各 20 次每秒（tier1） | 《取引所サービス約款》第 26 条禁止再配信与商用 | 实测（既有快照）加文档 |
| bitFlyer | 无 | 权限逐项勾选 | 500 次每 5 分钟每 IP | 未核 | 文档；权限分离为实测 |
| bitbank | 无 | 权限逐项勾选 | 私有取得 10 次每秒，更新 6 次每秒 | 未核 | 文档 |
| Coincheck | 无 | 未核 | 下单 4 次每秒 | 未核 | 文档 |
| Zaif | 无，仅 `comment` 字段 | rights 按 info、trade、withdraw 可分 | 未载 | 未核 | 文档 |
| BTCBOX | 未载 | 数据取得与全权限两档 | 未载 | 未核 | 文档 |
| OKJ | 有，`client_oid` | 未核 | 6 次每秒；下单 100 次每 2 秒 | 规约第 19 条禁止 API 数据再配信与商用 | 文档 |
| BitTrade | 有，`client-order-id` 不超过 64 字符 | 读取、出金、交易三权限可分 | 公共 10 次每秒每 IP | 未核 | 文档 |
| Binance Japan | 有，`newClientOrderId` | 权限逐项勾选（Binance 通用） | 权重 6000 每分钟 | 未核 | 文档 |
| 楽天ウォレット | 无 | 单一 API-KEY | 请求间隔不少于 200 ms | API 規約禁止再配信与商用 | 文档 |

## 4. L3 可重建性判定

日本全部加密交易所的公开流均不发布逐单新增、修改、撤销事件，也没有可把增量流对齐到
全量快照的重放锚点，因此全部不可做 L3 重建。最接近的只有成交级归属：Coincheck 成交含
maker 与 taker order id，bitFlyer 成交含 `child_order_acceptance_id`，两者都只能在成交
发生后把成交关联到委托，不能恢复委托队列。L2 层面的完整性证据按第 3.2 节排序，日元
域仍没有可证明逐增量无缺口的公开盘口流，对照册第 5 节结论不变。

## 5. 日本境内非加密订单级路径

日本境内带委托标识的订单级数据只存在于证券与衍生品市场，且全为法人路径。

| 路径 | 粒度与标识 | 时效 | 取得方式 | 费用 | 证据 |
|---|---|---|---|---|---|
| 東証 FLEX MBO 与 MBO BC | 逐单，Order ID 明示 | 实时 | IPLA 资格加专线，或经 vendor | Full Order Information 许可 1,530,000 日元每月，外配 1,450,000 日元每月；经 vendor 内部用约 150,000 至 200,000 日元每月 | 文档 |
| FLEX MBO Historical | pcap，含 Order ID | 历史，自 2011-01-11 | JPX総研，互联网 API 或 S3 | 通常 165,000 日元每月；全期间 495,000 日元；学术 82,500 日元 | 文档 |
| J-GATE 注文・約定データ | 含 AskOrderID 与 BidOrderID | T+1 | JPX 付费数据 | 450,000 日元每月或スポット 20,000 日元每日；ITCH 二进制 80,000 日元每月 | 文档 |
| Japannext ITCH 与 GLIMPSE | 逐单 | 实时 | 仅特定投資家 | 未核 | 文档 |
| Cboe Japan PITCH | 逐单 | 实时 | 参与者 cross-connect | 未核 | 文档，见本快照调研记录 |

零售券商与 FX 的 API（kabuステーション、楽天 RSS、SBI、OANDA、GMO FX）全为按价聚合或
更粗粒度，不在订单级路径之内。费用以官方页标价为准，税前税込口径未逐一核对。

## 6. 全球 L3 候选的官方变化

对照册第 5.2 节登记时的来源能力与本次核实的差异如下；次序影响已回写对照册。

| 来源 | 第 5.2 节原登记 | 2026-08-24 核实变化 | 对次序的影响 | 证据 |
|---|---|---|---|---|
| Coinbase Exchange `full` | 完整序列引导，首接候选 | `full` 频道现需认证；每 product 每 channel 订阅上限 10；WS 8 次每秒每 IP；REST level3 全簿带 `sequence`；Market Data Terms 2026-08-07 版限 personal 与 research 用途，禁止组织外再分发与任何 AI 或 ML 训练用途 | 仍为首接，前置条件增加：账户、API key、条款合规与订阅配额 | 文档 |
| Kraken `level3` | 认证，10/100/1000 价位，无序号，前十价位 CRC32 | 新增 REST `/0/private/Level3`（depth 0、10、25、100、250、1000，含 order_id 与纳秒 timestamp）；WS 端点 `wss://ws-l3.kraken.com/v2`；`level3` 消息增 `timestamp`；仍无序号，CRC32 仍只覆盖前 10 档 | 仍为第二 | 文档 |
| Bitstamp | 工作簿登记，`unverified`，仅小样本队列 | `live_orders` 含 `id` 与 `event_id`（MarketEventID）；新增 `POST /api/v2/order_data/` 按 `since_id` 与 `until_id` 回放公共订单事件 | 升为 `documented`，进入小样本队列 | 文档 |
| Bitfinex R0 | 截断 MBO，可选 checksum | `SEQ_ALL` 提供序号；R0 checksum 基于 order ID 与数量 | 保持第三，序号能力补注 | 文档 |
| Binance | 不在 L3 候选 | `trade` 流 2024-06-18 起移除买卖方订单 id | 成交级归属能力取消 | 文档，见本快照调研记录 |
| OKX | 不在 L3 候选 | `books` checksum 2026-06-23 起弃用 | 对照册第 5 节已登记「已弃用且固定 0」，补日期 | 文档，见本快照调研记录 |

## 7. 对 guvolu 的结论

- GMO 仍是唯一执行所，不因本次核对改变。
- 第二执行所候选为 BitTrade（费率 0 与 0.10%、有自定义委托号、读取出金交易三权限分离）或 OKJ（CRC32 盘口、有自定义委托号，费率较高）；两者都尚未探测法实测，接入前按 C-14 补证。
- bitbank 作为日元行情主轴的地位不变。
- 日本境内 L3 只有证券与衍生品的法人路径，加密域不存在。
- 全球 L3 首接仍为 Coinbase `full`，前置为账户、API key、条款合规与订阅配额；Kraken 第二；Bitstamp 进入小样本队列。

## 8. 与既有文档的差异及拿不准处

| 项 | 说明 |
|---|---|
| bitFlyer 自定义委托号 | [2026-08-07 多所调查](2026-08-07-multi-venue-api-survey.md) 第 8 节登记为「有」，本次核实官方下单端点无自定义委托号，仅返回交易所受理号；对照册第 7 节按本文改为「无」，接入前仍须探测法核实 |
| Zaif K 线端点 | 本次未核实是否存在 |
| BTCBOX 自定义委托号 | 官方页未载 |
| Binance trade 流与 OKX checksum 变更 | 变更日期取自官方更新记录，本文未保留记录页 URL |
| 第 2.2 节无 API 业者 | 多数以官方站点与规约确认，逐家 URL 未保留 |
| 法人路径费用 | 取官方标价，税前税込与合同期限未逐一核对；Japannext 与 Cboe Japan 未取得费用 |
| 金融庁名单 | 2026-08-21 版名单来自金融庁公开 PDF，本文未保留 URL |

## 9. 来源

| 来源 | 官方地址 |
|---|---|
| GMOコイン | `https://api.coin.z.com/docs/`、`https://coin.z.com/jp/corp/guide/fees/`、`https://api.coin.z.com/data/trades/` |
| bitFlyer | `https://lightning.bitflyer.com/docs?lang=ja`、`https://bf-lightning-api.readme.io/docs`、`https://bitflyer.com/ja-jp/s/commission` |
| bitbank | `https://github.com/bitbankinc/bitbank-api-docs`、`https://bitbank.cc/docs/fees/`、`https://bitbank.cc/docs/pairs/` |
| Coincheck | `https://coincheck.com/ja/documents/exchange/api`、`https://coincheck.com/ja/exchange/fee`、`https://coincheck.com/ja/article/83` |
| Zaif | `https://zaif-api-document.readthedocs.io/ja/latest/`、`https://zaif.jp/fee` |
| BTCBOX | `https://blog.btcbox.jp/archives/8759` |
| OKJ | `https://dev.okj.com/en/`、`https://www.okj.com/pages/products/fees.html` |
| BitTrade | `https://api-doc.bittrade.co.jp/` |
| Binance Japan | `https://developers.binance.com/docs/binance-spot-api-docs/`、`https://www.binance.com/ja/fee/trading`、`https://data.binance.vision/` |
| 楽天ウォレット | `https://www.rakuten-wallet.co.jp/service/api-leverage-exchange/` |
| SBI VCトレード | `https://www.sbivc.co.jp/faqs/content/5c0nv5540jm3` |
| 東証 FLEX 实时 | `https://www.jpx.co.jp/markets/paid-info-equities/realtime/index.html` |
| FLEX MBO Historical | `https://www.jpx.co.jp/english/markets/paid-info-equities/historical/01.html` |
| J-GATE | `https://www.jpx.co.jp/markets/paid-info-alternative/j-gate/index.html` |
| Japannext | `https://www.japannext.co.jp/en/pts` |
| Coinbase Exchange | `https://docs.cdp.coinbase.com/exchange/websocket-feed/channels`、`https://www.coinbase.com/legal/market_data` |
| Kraken | `https://docs.kraken.com/api/docs/websocket-v2/level3/`、`https://docs.kraken.com/api/docs/rest-api/get-level-3-order-book/` |
| Bitstamp | `https://www.bitstamp.net/api/` |
| Bitfinex | `https://docs.bitfinex.com/reference/ws-public-raw-books` |
