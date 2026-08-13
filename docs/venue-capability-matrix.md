# 来源能力对照册

> 文档类别：长期维护，登记于 [docs/00-rules-registry.md](00-rules-registry.md)。
> 本册是全部数据来源能力的**唯一现行对照**；证据为各时效快照，快照冻结，本册随新证据更新（W-02、W-07）。
> 具体端点、频道与采集设计见 [来源接口设计](venue-api-reference.md)；存储与路由
> 结论见 [物化设计](materialization-design.md) 和
> [订单流数据契约](order-flow-data-contract.md)。
> 证据等级：「实测」指本仓库探测法核实（A-04），「文档」指官方文档载明未实测，「未核」指两者皆无——未核项不得据以实施。
> `implementation_status=implemented` 只表示存在代码入口，不表示已有长期连续数据或完成物化闭环；逐来源实际完成度见 [物化设计第 13 节](materialization-design.md#13-当前来源能力并未全部充分发挥)。

## 1. 来源登记与证据

| 来源 | 角色 | 证据文档 | 证据等级 |
|---|---|---|---|
| GMO Coin | 执行加行情 | [能力报告](2026-08-05-gmo-api-capability-report.md)、[勘误](2026-08-05-gmo-order-id-erratum.md)、[量级实测](2026-08-06-gmo-data-scope-survey.md)、[打印口径实测](2026-08-07-gmo-trade-print-semantics.md) | 实测 |
| bitFlyer | 日元行情加衍生品信号 | [实测快照](2026-08-07-bitflyer-api-verification.md) | 实测 |
| bitbank | 日元行情 | [多所调查](2026-08-07-multi-venue-api-survey.md)、[实测快照](2026-08-08-bitbank-api-verification.md)、[闭环验证](2026-08-11-multi-source-closure-validation.md) | 实测 |
| Coincheck | 日元行情备用 | [闭环验证](2026-08-11-multi-source-closure-validation.md) | 文档加实录 |
| Binance | 全球参考加长历史 | [闭环验证](2026-08-11-multi-source-closure-validation.md) | 文档加实测 |
| Kraken | 全球参考加逐笔全历史 | 同上 | 文档 |
| OKX | 全球参考加历史 L2 | [单日闭环验证](2026-08-11-okx-l2-sample-validation.md)、[最新质量验收](2026-08-13-l2-quality-and-l3-readiness.md) | 文档加实测 |
| Bybit | 全球参考备用 | [OKX 验证中的目录复核](2026-08-11-okx-l2-sample-validation.md) | 文档；历史 L2 未核 |
| Coinbase Exchange | 全球参考备用 | 同上 | 文档 |
| Bitfinex | 截断 MBO 候选 | [官方 Raw Books](https://docs.bitfinex.com/reference/ws-public-raw-books)、[L3 工作簿证据](evidence/crypto_api_l3_registry_2026-08-12.json) | 文档；本地未实测 |
| Bitstamp | L3/MBO 待核候选 | [官方 API](https://www.bitstamp.net/api/)、[L3 工作簿证据](evidence/crypto_api_l3_registry_2026-08-12.json) | 工作簿登记；待小样本验证 |
| Hyperliquid | 链上永续补充 | 同上 | 未核为主 |

## 2. 准入与密钥模型

| 来源 | 公开行情门槛 | 密钥形态 | 密钥正交性 | 等级门槛 |
|---|---|---|---|---|
| GMO Coin | 无 | 双密钥，权限交易所侧固定 | **完全正交**（实测） | 限速按前周取引高分两档 |
| bitFlyer | 无 | 权限逐项勾选 | **不正交**：TRADE 密钥兼具全部读取，现含出金权限（实测，待人工收缩） | 无 |
| bitbank | 无 | 权限逐项勾选 | 未核 | 无 |
| Coincheck | 无 | 权限逐项勾选 | 未核 | 无 |
| Binance | 无 | 权限逐项勾选 | 可配置只读 | 行情无门槛 |
| Kraken | 无 | 权限逐项勾选 | 可配置只读 | 私有限速按验证等级 |
| OKX | 无 | 读写分权 | 可配置只读 | **50/400 档逐笔盘口均须 VIP4** |
| Bybit | 无 | 权限逐项勾选 | 可配置只读 | 交易限速按 VIP |
| Coinbase Exchange | 无 | 权限逐项勾选 | 可配置只读 | 无 |
| Bitfinex | 公开 raw book 无密钥 | 未核 | 未核 | 无公开行情门槛 |
| Bitstamp | 公开行情无密钥 | 未核 | 未核 | 无公开行情门槛 |
| Hyperliquid | 无 | 链上地址签名 | 不适用 | 无 |

密钥正交性只有 GMO 是交易所侧保证；其余一律依赖本仓库 client 类型层实施 T-02 式隔离。

## 3. 公开行情 REST

| 来源 | 盘口档数（单侧） | 逐笔单次 | K 线周期数 | K 线单次上限 | 证据 |
|---|---|---|---|---|---|
| GMO Coin | 500 | 100 | 12 | 按日或按年整段 | 实测 |
| bitFlyer | **无上限全簿**（实测 1,243） | 500（超量静默截断） | 无 K 线端点 | 不适用 | 实测 |
| bitbank | 200（熔断期合计 400，常态实测 200） | 60（无日期参数时，实测） | 11 | 按日或按年整段（1min 日 1,440 根、1day 年整段实测） | 实测 |
| Coincheck | 未载 | 未载 | 无 K 线端点 | 不适用 | 文档 |
| Binance | 5,000 | 1,000 | 16 | 1,000 | 文档 |
| Kraken | 未载 | 1,000 | 9 | 720 根且更早不可取 | 文档 |
| OKX | 400 | 500 | 15 | 300（历史端点 100） | 文档加实测 |
| Bybit | 1,000（常规）；Spot full depth 新端点单侧至 10,000，未实测 | 未载 | 13 | 1,000 | 文档 |
| Coinbase Exchange | L1/L2 聚合；L3 完整非聚合订单簿 | 分页 | 6 | 300 | 文档 |

## 4. 历史深度与回补

| 来源 | K 线历史 | 逐笔回补 | 盘口历史 | 批量归档 | 证据 |
|---|---|---|---|---|---|
| GMO Coin | 日线自上市，分钟线自 2021-04-15 | REST 约 1 万笔（约 21 小时）；**官方归档全历史** | 不可 | **有：逐笔归档自 2018-09-05，27 品种含下架，按交易日一文件，无校验文件** | 实测 |
| bitFlyer | 无 K 线端点 | **31 天**（错误码 -156 实测） | 不可 | 无 | 实测 |
| bitbank | 按年，1day 自 2016 有文件（无成交出零量根） | **按日全量回补**（btc_jpy 自 2017-02-13，UTC 日切割） | 不可 | 无 | 实测 |
| Coincheck | 无 K 线端点 | 未载 | 不可 | 无 | 文档 |
| Binance | 自上市 | 全历史 | **现货不可；部分衍生品有百分比聚合 `bookDepth`，不可逐档重放** | **公开归档站，带校验文件** | 文档 |
| Kraken | 仅 720 根 | **自首笔全历史** | 不可 | 未核 | 文档 |
| OKX | 近端加历史端点 | 历史端点分页 | **官方高分辨率 L2 自 2023-03 起；400 档 BTC-USDT 已通过有界历史重放** | **有：逐笔、K 线、资金费率、L2** | 文档加实测 |
| Bybit | 未载 | 官方成交归档，覆盖待核 | **当前公开目录未取得 orderbook 文件，阻断** | 成交、K 线等；历史盘口未证实 | 文档加目录实探 |
| Coinbase Exchange | 分页回溯 | 分页回溯 | 不可 | 无 | 文档 |

盘口历史必须按保真度判断。OKX 400 档日档已证实是周期 snapshot 加绝对数量 update，但不含逐帧 sequence/checksum；Bybit 尚无可用文件证据；Binance 部分衍生品只有百分比带宽聚合深度。它们都不能补齐日元三所或最近尚未发布窗口。日元三所仍须常开。只有订单队列级回测要求超过 OKX 保真度且自录样本不足时，才触发商业供应商评估（TBD-26）。实测见 [OKX 小样本验证](2026-08-11-okx-l2-sample-validation.md)。

## 5. 实时流与完整性等级

完整性等级定义（本册与 `venue_capability.integrity` 列共用）：

| 等级 | 含义 | 丢帧后果 | 校验逻辑 |
|---|---|---|---|
| `checksum` | 帧携带有效校验和 | 立即检出 | 逐帧校验，失败即重建 |
| `sequence` | 帧携带单调序号 | 缺口可检出 | 序号缺口检测，缺口即重建 |
| `snapshot` | 无序号但有带内定期快照 | 静默错误，**有界**（至下一快照） | 快照对照重置，不一致窗口标不可信 |
| `none` | 无任何可校验字段且无带内快照 | 静默错误，无界 | 仅能定期 REST 快照旁路对照 |

| 来源 | 盘口流模型 | 完整性等级 | 断连补发 | 证据 |
|---|---|---|---|---|
| GMO Coin | 30 档全量快照流 | `snapshot`（帧即快照，丢帧仅丢时点） | 不补发 | 实测 |
| bitFlyer | 差分加 **5 秒节流快照**（实测） | `snapshot`（错误有界 5 秒） | 明示不补发 | 实测 |
| bitbank | 快照加差分 | `monotonic`（序号只证明回退，不能证明中间无缺口） | 回退时重建；缺帧不能由序号证明 | 文档 |
| Coincheck | 差分 | `none` | 明示不重发 | 文档 |
| Binance | 差分加 REST 快照引导 | `sequence`（U/u 连续性规则） | 靠序号自查 | 文档 |
| Kraken | 快照加差分 | `checksum`（前 10 档 CRC32） | 靠校验和自查 | 文档 |
| OKX | 分级频道 | `sequence`（`seqId/prevSeqId`；checksum 已弃用且固定 0） | 靠序号自查 | 文档 |
| Bybit | 快照加差分 | `sequence` | 靠序号自查 | 文档 |
| Coinbase Exchange | `level2` 快照加更新；`full` L3 另表 | `sequence` | 靠序号自查 | 文档 |

GMO 与 bitFlyer 同判 `snapshot` 但失效形态相反：GMO 帧自足、丢帧丢时点；bitFlyer 差分丢帧丢正确性、由 5 秒快照定界自愈。bitbank 序号只保证单调，不能把数值跳跃解释为缺帧，因此不再误标为 `sequence`。现有日元域没有可证明逐增量无缺口的公开盘口流；做市研究必须结合快照旁路、健康窗口和保守降级。

三所 BTC/JPY 实时成交现分别使用 GMO `trades`、bitbank
`transactions_{pair}` 与 bitFlyer `lightning_executions_{product}`，均已进入
run-scoped 五分钟封口和 schema v2 逐笔物化。它们只证明已实现并正在运行；
连续性仍按 24 小时和 7 天门槛另行验收。逐笔与 L2 的事实、回补和守护边界
见 [订单流数据事实契约](order-flow-data-contract.md)。

### 5.1 REST 锚点与可比范围

三所公开 REST 盘口端点已按精确端点修订登记：bitFlyer `EP-0001@r0`、bitbank
`EP-0003@r0`、GMO `EP-0006@r0`。锚点保存独立 raw artifact 和
`book_l2_anchor_observation`，只做 WS 状态旁路验证。它不是 WS frame、不能补写
断流，也不能修正活动 L2 head。

bitbank 只有在 REST 与 WS 原生序列相等且比较深度一致时才可裁决 full-book
`match/mismatch`。GMO 与 bitFlyer 没有同序身份，时间近邻只能标
`approximate/unknown`；不同深度只能输出共同范围的 best/depth/hash 诊断。任何
超时、限频或队列满标 `unavailable`，bitFlyer 断连仍不可回补。

### 5.2 L3/MBO 候选保真度与接入次序

以下只登记来源能力和验证顺序；四个来源在本仓库都没有 L3 connector、封口 raw、
活动 Parquet head 或生产任务。`A/B` 是来源可达到的保真度，不是本地完成度。

| 次序 | 来源与官方能力 | 保真度 | 重放与有限度 | 本地状态 |
|---|---|---|---|---|
| 1 | Coinbase [`full` WS](https://docs.cdp.coinbase.com/exchange/websocket-feed/channels#full-channel) 加 [REST Level 3](https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/get-product-book) | A：完整非聚合订单簿与订单生命周期 | 先缓冲 WS，再取 snapshot，丢弃小于等于 snapshot sequence 的消息后重放；`match` 带 maker/taker order ID，`side` 是 maker 侧 | `documented`；首接候选，未实现 |
| 2 | Kraken 认证 [`level3`](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/level3) | A：深度受限 L3/MBO | 10/100/1000 价位，order ID 加 `add/modify/delete`；官方称无需 sequence，以前十价位 CRC32 校验；出界价位无 delete | `documented`；第二候选，令牌与裁剪待小样本 |
| 3 | Bitfinex [`prec=R0` Raw Books](https://docs.bitfinex.com/reference/ws-public-raw-books) | B：截断 MBO | `order_id/price/amount`，`price=0` 删除，可选 checksum；`len` 最大 250 且是订单数而非价位数 | `documented`；未实现，先验 250 单边界 |
| 4 | Bitstamp 工作簿登记的 `group=2` 加 live-orders | B 候选；尚未核实 | [官方 API](https://www.bitstamp.net/api/) 与公开示例不足以证明 order ID 稳定性、snapshot/update 顺序和缺口恢复 | `unverified`；仅进入小样本队列 |

Coinbase 的完整序列引导使它优先于深度受限且认证的 Kraken；Bitfinex 只能研究
可见的截断队列；Bitstamp 在真实响应、重连和缺口样本通过前不得升格。任何候选
接入后仍须按 [L3 四表合同](order-flow-data-contract.md#11-l3-升级兼容与派生边界)
分别保存逻辑事件、原件证据、可证明撮合关联与状态 checkpoint，再派生 L2；不能
直接写入现有原生 L2 事实。

## 6. 限速模型

| 来源 | 模型 | 公开侧要点 | 私有侧要点 | 证据 |
|---|---|---|---|---|
| GMO Coin | 固定速率 | 无明文，实测 5 次每秒触发 ERR-5003 | 20 或 30 次每秒按档 | 实测 |
| bitFlyer | 固定速率 | 500 次每 5 分钟每 IP | 500 次每 5 分钟；下单 300 次每 5 分钟；小额委托另限 100 次每分钟 | 文档 |
| bitbank | 固定速率 | 未载总限 | 查询 10 次每秒，更新 6 次每秒 | 文档 |
| Coincheck | 固定速率 | 未载总限 | 下单 4 次每秒，委托详情 1 次每秒 | 文档 |
| Binance | **权重制** | 权重每分钟每 IP，深盘口权重 250 | 权重加委托计数 | 文档 |
| Kraken | **计数器衰减** | 未载 | 上限 15/20/20，衰减 0.33/0.5/1 每秒按验证等级 | 文档 |
| OKX | 按端点窗口 | 多为 20 次每 2 秒 | 按 User ID 按端点 | 文档 |
| Bybit | 固定速率 | 600 次每 5 秒每 IP，超限锁 10 分钟 | 按 UID 按 VIP | 文档 |
| Coinbase Exchange | 令牌桶 | 10 次每秒峰值 15 | 15 次每秒峰值 30 | 文档 |
| Hyperliquid | 权重制 | 1,200 权重每分钟每 IP | 同池 | 未核 |

跨源限速互不干扰（各按各家 IP 或账户计量）；本机共享资源是带宽、磁盘与进程数。

## 7. 私有能力与对账形态

| 来源 | 自定义委托号 | 全撤端点 | 私有推送 | 对账含义 | 证据 |
|---|---|---|---|---|---|
| GMO Coin | **无** | `cancelBulkOrder` | 私有 WS 四频道 | intent 与 orderId 映射（T-05），事后关联 | 实测 |
| bitFlyer | 有 | `cancelallchildorders` | `child_order_events` 等两频道（认证实测通过） | 自定义号可直接承载 intent_id | 实测 |
| bitbank | 未载 | 批量撤单端点 | PubNub 私有流 | 待接入时核实 | 文档 |
| Coincheck | 未载 | 无批量端点载明 | order-events 等，明示不重发 | 待接入时核实 | 文档 |
| Binance | 有 | 有 | 用户数据流 | 自定义号承载 | 文档 |
| Kraken | 有 | 有 | WS v2 私有频道 | 自定义号承载 | 文档 |
| OKX | 有 | 有 | 私有频道 | 自定义号承载 | 文档 |
| Bybit | 有 | 有 | 私有频道 | 自定义号承载 | 文档 |
| Coinbase Exchange | 有 | 有 | 私有频道 | 自定义号承载 | 文档 |

GMO 是唯一无自定义委托号的来源；T-05 的映射法是 GMO 特例，不得推广为公共设计（multi-source 设计第 2 节不变量）。

## 8. 更新规则

1. 任何单元格从「文档」升「实测」时，以新时效快照登记证据并更新本册引用。
2. 新来源接入时先补第 1、2 节两行，能力各节允许暂标「未核」。
3. 「未核」单元格不得成为实现依据；实现前按 C-14 探测法补证。

## 9. 能力成熟度与 fallback 语义

`implementation_status` 只描述代码入口，不足以决定数据能否用于研究。运行决策使用以下递进等级；后一级必须满足前一级：

| 等级 | 判据 | 可用于什么 |
|---|---|---|
| `documented` | 官方文档已核，尚无本地响应 | 设计与探测计划 |
| `adapter` | 只读客户端或录制器可运行 | 受控小样本 |
| `raw_sample` | 原文已封口并可散列 | schema 与语义验证 |
| `continuous` | 轮转、重连、健康窗口与断点连续运行 | 实时监控 |
| `materialized` | 原件进入带市场、制品和版本键的活动事实 | 可重复分析 |
| `closed` | 覆盖、散列、PIT、唯一性、拒绝和重放均通过 | 决策级研究 |

fallback 分三类，语义不可混用：

1. **同所回填 fallback**：同一来源优先官方归档，其次同一来源 REST 日期或游标，最后从当前时点继续实时采集。只有这一类可以补齐该来源自己的事实；若官方不存在历史，缺口必须保留。
2. **研究服务 fallback**：主力来源不可用时，查询服务可临时返回另一来源的同类指标，但必须同时返回 `source_set`、市场、报价币、覆盖率和降级状态。它替代的是服务，不是原始事实。
3. **派生聚合**：多个来源的事实经过时间窗和口径对齐后生成新制品，例如跨所 VWAP、中间价中位数或成交量份额。派生制品使用自己的 artifact 与版本，绝不反写或覆盖任何来源事实。

第三类已有读取期最小实现：`GET /api/v2/aggregates/book/top` 对同 base/quote/
instrument/market kind 的活动顶档应用 PIT、质量、新鲜度与 quorum，返回 contributors、
synthetic best、mid median 和 `crossed`，固定 `no-store`。它没有活动聚合 Parquet
head，不做隐式 FX，也不等于回测级持久化聚合制品。

禁止使用前值、零值或另一交易所数据填补来源逐笔与盘口空洞。允许的前值仅限查询呈现，并必须带 `stale=true`、原始时刻和最大允许陈旧时间。

## 10. 数据域的主力、回填与聚合路由

| 数据域 | 主力输入 | 正确回填或 fallback | 聚合逻辑 | 明确禁止 |
|---|---|---|---|---|
| 市场与精度元数据 | 各所 `symbols/pairs/markets/instruments` 原文 | 在证据有效期内使用最后封口修订；过期则停止新市场接入 | 仅映射到共同 `instrument_id`，保留 `market_id` | 猜测已下架市场历史精度 |
| 历史成交 | 同所官方归档；无归档时用日期或稳定游标 REST | 归档校验失败可重下；REST 只补同所；两者皆无则登记 gap | 来源内保持 `match/aggregate`；跨所只做时间窗指标 | 用别所成交补该所缺口；混淆 aggregate 与 match |
| 实时成交 | 同所 WS 原文 | 断连后用同所稳定 ID 的 REST 回扫；无稳定回扫能力则缺口标记 | 窗口成交额、VWAP、taker Delta，保留来源维度 | 按价格时间相近跨所去重 |
| K 线 | 同所原生完结 K 线与由逐笔生成的 derived K 线并列 | 无原生端点时从已闭环逐笔聚合；近期未完结根重算 | `market_id + interval + open_time + origin + value_revision + normalization_version` | 原生根与派生根静默互相覆盖 |
| 实时 L2 | 同所 WS 快照或快照加差分 | REST/带内快照只用于重新锚定当前簿；断连窗口保持不可信 | 帧与价位分表，按完整性等级重建 | 以快照插值历史盘口；只存顶档后声称完整 L2 |
| 历史 L2 | 仅来源官方历史产品或自建封口实时段 | 当前 OKX 产品只补 OKX 市场；Bybit 保持阻断；日元三所无官方回补 | 保留 snapshot/delta、深度、频率、可得时刻与 sequence/checksum 缺失事实 | 把百分比聚合深度当逐档订单簿 |
| Ticker 与参考价 | 同所 ticker、顶档与最新成交 | ticker 缺失时由同所 book/trade 派生并标 `origin=derived` | 同报价币可做稳健中位数；异报价币先绑定 FX 制品 | 将 BTC/JPY 与 BTC/USDT 直接平均 |
| 市场状态与熔断 | 同所 status/health/circuit-break 原文 | 无端点时只可由无帧、价差异常推断，标 `inferred` | 按有效区间关联事实 | 把推断状态写成交易所声明 |
| 资金费率、OI、标记价 | 对应衍生合约原生端点 | 无同合约历史就留空；不得从现货价格伪造 | 先对齐合约种类、结算周期与计价币 | 现货与 leverage/perpetual 共用市场键 |
| 私有账户、订单、成交回报 | 账户私有 REST/WS | REST 对账补私有 WS 缺口；密钥权限隔离 | 绑定 account、intent、order 与 execution | 混入公开 `market_*` 事实或使用公共 fallback |

### 10.1 重合能力的确定性主力选择

“主力”只决定查询服务和回补队列的优先顺序，不改变来源事实的所有权。相同 `instrument_id` 的多所行仍按各自 `market_id` 保存；服务降级必须返回实际 `source_set`，不能沿用主力来源标签。

| 服务场景 | 主力 | 第二输入 | 正确的选择或聚合 |
|---|---|---|---|
| 单所执行价与订单执行研究 | 对应 `market_id` 的原生交易所 | 其他所只作 validator | 先保留本所执行主场语义；跨所只计算共同窗口价差、VWAP 和成交量份额 |
| 日元现货长历史成交 | bitbank BTC/ETH/XRP；GMO 负责自身 27 币 | 另一所同品种历史 | 每所独立覆盖；研究层按日比较，不把另一所数据填入缺失日 |
| 日元现货当前稳健参考价 | 三所健康输入的中位数 | 单所健康输入 | 先对齐 `SPOT:* / JPY` 与 `available_time`；少于两所时返回 degraded，不伪造中位数 |
| 日元实时盘口完整性研究 | bitbank whole+单调 sequence diff | GMO 30 档无序号 snapshot；bitFlyer 无序号 snapshot+diff | bitbank 适合可重建主轴；另两所是独立市场事实/验证票，不能拼成虚构订单簿 |
| 日元 leverage 信号 | bitFlyer `FX_BTC_JPY` | GMO `_JPY` leverage 市场 | 合约类型、杠杆规则与资金费率分别绑定；不得退化到现货市场键 |
| 全球现货参考与长历史 | Binance 普通 trades/klines（接入后） | Kraken、Coinbase | 同报价币直接做稳健统计；异报价币必须经带时间键的 FX 制品转换 |
| 全球可校验实时 L2 | Kraken checksum；Binance/OKX/Bybit/Coinbase sequence | Hyperliquid snapshot | 优先完整性可证且健康的来源；聚合只产生 NBBO/深度指标，不合并价位动作 |
| 历史 L2 重放 | OKX 已核 400 档官方产品 | 自建实时封口段；Bybit 待重新核证 | 保留深度、频率、snapshot/delta 与完整性差异；无 sequence 时不声称逐增量无缺口 |

确定性路由顺序为：先锁定 `market_id + domain`，再用同所官方归档，随后用同所稳定游标或日期 REST，最后从实时当前点续采；仍有缺口就登记 gap。只有在查询服务层才允许切换另一所或计算聚合结果，且必须产生新的分析 artifact、版本和覆盖说明。

聚合执行以下固定口径：先在每个 `market_id` 内按 `available_time <= decision_time` 生成窗口指标，再跨所合成；稳健参考价取健康来源的“逐所中间价或逐所 VWAP”的中位数，不把所有逐笔先混池，否则大流量交易所会隐式取得不透明权重。成交量份额才使用逐所 quote volume 除以健康来源合计，并返回每所覆盖率。派生 K 线始终逐市场生成；跨所合成蜡烛若确有研究需求，使用独立 dataset 和 method version，不能写入 `market_kline` 的原生来源行。

现行 `GET /api/v2/aggregates/book/top` 只实现顶档读取聚合：显式市场集合、PIT、
最大陈旧度、质量过滤、quorum、contributors、synthetic best bid/ask、mid median 与
`crossed`。crossed 是市场间可观察事实，不能静默夹平为零 spread。无显式 FX
制品时只接受相同 base/quote/instrument/market kind，响应 `no-store`；成交量份额、
跨所 VWAP 和持久化聚合 artifact 仍待后续实现。

## 11. 来源角色与有限度

| 来源 | 主力场景 | 合法 fallback 角色 | 不可突破的有限度 |
|---|---|---|---|
| GMO | 日元现货官方历史广度、原生 K 线、30 档实时快照 | bitbank/bitFlyer 只可作为跨所指标参考 | 无官方历史盘口；快照无 sequence/checksum；历史已下架币精度可能未知 |
| bitbank | 日元长历史逐笔与可重建实时 L2 主轴 | GMO/bitFlyer 作为研究服务降级 | 个别官方日期 404；sequence 单调但不要求连续；动态市场覆盖见日期快照 |
| bitFlyer | 自有 JPY spot 近端成交、全簿与 leverage/CFD 信号 | bitbank/GMO 作为现货参考 | executions 官方边界约 31 日；无 K 线；L2 无序号且断连窗不可重放 |
| Coincheck | 日元实时备用、市场广度旁路 | 只在主力实时源故障时返回降级指标 | 无 K 线、无历史闭环、盘口差分无序号且不补发 |
| Binance | 全球现货长历史、校验归档、USDT 参考 | Kraken/OKX/Coinbase 可组成全球参考 quorum | `aggTrades` 不是逐撮合；报价币不能直接对齐 JPY |
| Kraken | BTC/USD 等全球逐笔全历史候选、CRC32 L2；深度受限 L3 第二候选 | Binance/Coinbase 全球参考降级 | 原生 OHLC 仅近 720 根；L3 认证、无 sequence 且出界无 delete；本地尚无闭环原件 |
| OKX | 全球现货与衍生品、官方历史 L2 主候选 | Binance/Coinbase 参考降级 | 400 档历史已有有界闭环；live books 隔离小样本已验，生产持续运行、其他品种和 5000 档未核 |
| Bybit | 全球衍生品实时备用 | OKX/Binance 参考降级 | 当前公开目录无历史 L2 文件证据；不得列为盘口冷启动来源 |
| Coinbase | BTC/USD 第三参考票；Full 加 REST L3 首接候选 | Kraken/Binance 参考降级 | 必须完成 buffer/snapshot/discard/replay；`match.side` 是 maker 侧；本地尚未接入 |
| Bitfinex | 最多 250 单的截断 MBO 研究候选 | 不作完整 L3 fallback | `len` 是订单数，不是价位数；看不到完整队列；本地尚未接入 |
| Bitstamp | `group=2`/live-orders 小样本候选 | 不作任何 L3 fallback | 稳定订单身份、顺序与缺口恢复均待实测；本地尚未接入 |
| Hyperliquid | 链上永续 L2、资金费率候选 | 不作为日元现货 fallback | K 线仅近 5,000 根、L2 快照单侧 20 档；成交映射未核 |

本册不固化本地行数、活动市场数、热层天数或审计时刻；这些动态值只见
[2026-08-13 L2 质量与 L3 就绪度快照](2026-08-13-l2-quality-and-l3-readiness.md)。
能力表中的“文档支持”不能外推为本地已完成；OKX live books 已达到
`raw_sample` 并完成隔离物化，但尚未达到 `continuous` 或生产 `closed`。L3 仍只有
合同。
