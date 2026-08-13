# GMO Coin API 能力调查报告（2026-08-05）

- **文档类别**：时效快照（内容冻结，修订以新快照发布）
- **调查日期**：2026-08-05
- **文档**：<https://api.coin.z.com/docs/>（v1）
- **方法**：用两把真实密钥实测全部端点。GET 直接调用；POST 一律用**故意非法的参数**（`symbol=ZZZ_INVALID`、`size=0`、`orderId=1`、`amount=0`）探测——凡返回参数错误或业务错误即证明权限通过，凡返回 `ERR-5012` 即证明权限被拒。**全程未产生任何真实委托、成交或资金划转。**
- **账户快照**：JPY 3,009 / tierLevel 1（20 req/s）/ 无持仓无挂单 / 服务状态 `OPEN`

## 1. 结论速览

| 项 | READ_ONLY | TRADE |
|---|---|---|
| REST 读取（13 个） | 全部可用 | **全部被拒** |
| Private WS 频道（4 个） | 全部可订阅 | **全部被拒** |
| 下单/改单/撤单（5 个） | 全部被拒 | 全部可用 |
| 未启用（4 个） | 不可 | 不可 |
| `ws-auth` 令牌签发 | 可 | 可（但订阅一律被拒） |

**结论**：两把密钥权限**完全正交**——READ_ONLY 仅可读取，TRADE 仅可下单撤单，且**无任何读取能力**。

## 2. READ_ONLY：声明能力 vs 实测

| # | 声明能力 | 方法 | 路径 | 实测 | 备注 |
|---|---|---|---|---|---|
| 1 | 資産残高を取得 | GET | `/v1/account/assets` | 可 | 返回各币种 amount 与 available |
| 2 | 余力情報を取得 | GET | `/v1/account/margin` | 可 | |
| 3 | 取引高情報を取得 | GET | `/v1/account/tradingVolume` | 可 | 含 tierLevel 与每日限额 |
| 4 | 日本円の入金履歴 | GET | `/v1/account/fiatDeposit/history` | 可 | `fromTimestamp` 必填 |
| 5 | 日本円の出金履歴 | GET | `/v1/account/fiatWithdrawal/history` | 可 | 同上 |
| 6 | 暗号資産の預入履歴 | GET | `/v1/account/deposit/history` | 可 | 同上 |
| 7 | 暗号資産の送付履歴 | GET | `/v1/account/withdrawal/history` | 可 | 同上 |
| 8 | 注文情報取得 | GET | `/v1/orders` | 可 | `orderId` 或 `clientOrderId` |
| 9 | 有効注文一覧 | GET | `/v1/activeOrders` | 可 | |
| 10 | 約定情報取得 | GET | `/v1/executions` | 可 | `orderId` 或 `executionId` |
| 11 | 最新の約定一覧 | GET | `/v1/latestExecutions` | 可 | 直近 1 日 |
| 12 | 建玉一覧を取得 | GET | `/v1/openPositions` | 可 | 杠杆专用 |
| 13 | 建玉サマリーを取得 | GET | `/v1/positionSummary` | 可 | 杠杆专用 |
| 14 | 約定情報通知 (WS) | WS | `executionEvents` | 可 | |
| 15 | 注文情報通知 (WS) | WS | `orderEvents` | 可 | |
| 16 | ポジション情報通知 (WS) | WS | `positionEvents` | 可 | |
| 17 | ポジションサマリー情報通知 (WS) | WS | `positionSummaryEvents` | 可 | |

> 声明的 17 项能力**全部核实通过**，无缺失、无越权。4 个历史查询端点的路径形态为 `.../xxx/history`（不是 `xxxHistory`），缺 `fromTimestamp` 时返回 `ERR-5106`。

## 3. TRADE：声明能力 vs 实测

| # | 声明能力 | 方法 | 路径 | 实测 | 探测返回 |
|---|---|---|---|---|---|
| 1 | 注文 | POST | `/v1/order` | 可 | `ERR-5106 symbol size`（参数错误即有权限） |
| 2 | 注文変更 | POST | `/v1/changeOrder` | 可 | `ERR-5123 The orderID not exist.` |
| 3 | 注文キャンセル | POST | `/v1/cancelOrder` | 可 | `ERR-151`（委托不存在） |
| 4 | 注文の複数キャンセル | POST | `/v1/cancelOrders` | 可 | `status=0`，逐单 failed |
| 5 | 注文の一括キャンセル | POST | `/v1/cancelBulkOrder` | 可 | `ERR-5106 symbols` |

> 声明的 5 项**全部核实通过**。此外实测发现：**TRADE 对全部 13 个 REST 读取端点和全部 4 个 WS 频道均返回 `ERR-5012`**——这一点未在能力清单中体现，但对架构设计影响最大。

## 4. 未使用的 4 项

| 声明能力 | 方法 | 路径 | READ_ONLY | TRADE |
|---|---|---|---|---|
| 決済注文 | POST | `/v1/closeOrder` | 不可（ERR-5012） | 不可（ERR-5012） |
| 一括決済注文 | POST | `/v1/closeBulkOrder` | 不可（ERR-5012） | 不可（ERR-5012） |
| ロスカットレート変更 | POST | `/v1/changeLosscutPrice` | 不可（ERR-5012） | 不可（ERR-5012） |
| 口座振替 | POST | `/v1/account/transfer` | 不可（ERR-5012） | 不可（ERR-5012） |

确认四项在两把密钥上均未开通。

## 5. Public API（无需认证，不消耗私有额度）

| 能力 | 路径 | 实测 |
|---|---|---|
| サービス稼働状態 | `GET /v1/status` | 可，返回 OPEN |
| 最新レート | `GET /v1/ticker` | 可 |
| 板情報 | `GET /v1/orderbooks` | 可 |
| 取引履歴 | `GET /v1/trades` | 可 |
| KLine情報 | `GET /v1/klines` | 可 |
| 取引ルール | `GET /v1/symbols` | 可，含 minOrderSize、sizeStep、tickSize |

## 6. 关键发现

**F-1 · TRADE 无任何读取能力（最重要）**
TRADE 无法读取 `activeOrders`，不能用同一把密钥完成「下单后确认」。任何写后校验必须经由 READ_ONLY，**两把密钥的状态以 `clientOrderId` 关联**。此为架构约束，非可选项。

**F-2 · 开仓与平仓权限不对称（风险最高）**
杠杆交易中 `/v1/order` 建仓、`/v1/closeOrder` 平仓是两个端点。现状为**开仓已开通、平仓未开通**——一旦在杠杆盘建仓，程序无法通过 API 平仓，只能人工登录处置。撤单仅对未成交挂单有效，对已成交持仓无效。
处置：要么开通平仓权限，要么**硬性只做现物**（现物卖出经由 `/v1/order` `side=SELL` 完成，不受影响）。

**F-3 · 成交回报只在 READ_ONLY 侧**
TRADE 无法订阅 `executionEvents`。委托生命周期只能由 READ_ONLY 的 WS 连接监听，交易执行器必须与行情、回报进程共享状态。

**F-4 · `ws-auth` 的 PUT/DELETE 签名不含 body**
普通端点签名串 = `timestamp + method + path + body`；但 `PUT/DELETE /v1/ws-auth` 的签名串 = `timestamp + method + path`（body 照发但**不参与签名**）。按通用逻辑实现必得 `ERR-5010`。已实测确认。

**F-5 · 令牌可签发不等于频道可订阅**
两把密钥都能取得 WS 令牌并建立连接，权限校验发生在 `subscribe` 之后，以 JSON 消息 `{"error":"ERR-5012 ..."}` 返回，**不断开连接**。必须显式解析订阅后的错误帧，否则无法察觉订阅失败，持续收不到数据。

**F-6 · 资金规模约束**
余额 3,009 JPY，BTC 现物 `minOrderSize=0.00001`（约 101 JPY/笔）。约可下 30 笔最小委托，足以验证链路与小额功能，不足以支撑统计意义上的策略验证。

## 7. 权限分配是否合理

**合理的部分：**

1. **读写分离彻底**，符合最小权限原则。READ_ONLY 泄露仅损失信息；TRADE 泄露时攻击者无法读取余额与持仓，难以侦察，损害范围显著缩小。优于「单一全权限密钥」的常见做法。
2. **撤单归在 TRADE**：紧急撤单必须与下单同权限，否则紧急停止开关会因权限缺失而失效。此项分配正确。
3. **划转与强制平仓线变更未开通**：两项不产生超额收益，仅增加风险面，默认关闭正确。

**不合理或需处理的部分：**

1. **F-2 的开平不对称是设计缺陷**，不是权限收紧。「仅可开仓」较「开平皆可」更危险——将可控风险转为不可控风险。
2. **注文変更（changeOrder）价值存疑**：撤单重下（cancel + replace）语义更清晰、状态机更简单，且改单在部分成交时行为易错。可保留，但建议策略层**约定不使用**。
3. TRADE 无读取能力带来的**对账复杂度**由应用层承担。此代价可接受，但必须在代码中显式建模（见 SKILLS.md 铁律 T-06）。

## 8. 未使用的 4 项是否需要添加

| 能力 | 建议 | 理由 |
|---|---|---|
| **一括決済注文** `closeBulkOrder` | 做杠杆则必须开通；只做现物则不开 | 杠杆持仓的紧急全量平仓手段，缺失即无紧急出口 |
| **決済注文** `closeOrder` | 做杠杆则必须开通；只做现物则不开 | 精细平仓与部分止盈的唯一手段，与上一条配套 |
| **ロスカットレート変更** `changeLosscutPrice` | 暂不开通 | GMO 有自动强制平仓机制；策略层自有止损委托更可控 |
| **口座振替** `transfer` | 不开通 | 转移实际资金，不产生收益，仅增加风险面，以手工操作代替 |

**决策路径（二者择一）：**

- **路线 A（推荐先行）｜只做现物**：不动权限，现状即完备。`/v1/order` 的 BUY/SELL 足以完成建仓平仓，F-2 不成立。为当前资金规模下的合理选择。
- **路线 B｜要做杠杆**：**先开通 `closeOrder` 与 `closeBulkOrder`，再实现任何下单代码**。权限补齐前，代码必须硬性禁止杠杆品种。

## 9. 实现注意事项

| # | 现象 | 正确做法 |
|---|---|---|
| 1 | 历史端点缺 `fromTimestamp` 时 `ERR-5106` | 4 个 history 端点必传 ISO8601（如 `2018-01-01T00:00:00.000Z`） |
| 2 | 路径写成 `depositHistory` 时 404 `ERR-5204` | 正确为 `/v1/account/deposit/history` |
| 3 | WS 地址漏 `/v1` 时 HTTP 404 | `wss://api.coin.z.com/ws/private/v1/{token}` |
| 4 | 频道名用小写无效 | 正确为 `executionEvents` / `orderEvents` / `positionEvents` / `positionSummaryEvents` |
| 5 | `ws-auth` PUT/DELETE 签名带 body 时 `ERR-5010` | 该两个方法签名串**不含 body** |
| 6 | 签名路径含 `/private` 无效 | 签名只用 `/v1/...` |
| 7 | GET 签名带 query string 无效 | GET 的 body 部分为**空串**，query 不参与签名 |
| 8 | 用 float 解析价格损失精度 | API 全部返回**字符串**，一律 `Decimal` |
| 9 | HTTP 200 不代表成功 | 必须判 `status == 0`，失败也返回 200 |
| 10 | WS 订阅后不读错误帧则收不到数据 | 权限错误以消息形式返回且不断连 |
| 11 | 忽略 60s 心跳导致断连 | 服务端每 60s ping，连续 3 次无 pong 断开 |

## 10. 错误码对照（本次实测遇到）

长期维护版本见 error-catalog（错误码对照与处置册）。以下为调查时点记录：

| 码 | 含义 | 触发场景 |
|---|---|---|
| `ERR-5012` | 密钥/IP/权限不匹配 | 越权调用——**权限探测的判定依据** |
| `ERR-5010` | 签名无效 | 签名串构造错误 |
| `ERR-5106` | 请求参数无效 | 缺 `fromTimestamp`、非法 symbol 或 size |
| `ERR-5123` | 委托不存在 | changeOrder 传入不存在的 orderId |
| `ERR-5204` | Forbidden（HTTP 404） | 路径不存在 |
| `ERR-151` | 服务暂不可用（字面义） | cancelOrder 传入不存在的 orderId 时返回 |

## 11. 由本次调查推导的架构基线

```text
                Public API / Public WS   （行情，无需密钥）
                          │
        ┌─────────────────┴─────────────────┐
        │                                   │
  READ_ONLY 密钥                        TRADE 密钥
  ├ REST × 13（对账/冷启动快照）          └ order / changeOrder
  └ Private WS × 4（实时委托·成交回报）      cancelOrder / cancelOrders
        │                                   cancelBulkOrder
        └──────── clientOrderId ────────────┘
              （唯一关联键，必须自生成、必须幂等）
```

- **唯一真相源**：READ_ONLY。TRADE 的响应只能证明「请求被受理」，不能证明「状态已改变」。
- **状态机**：下单前先落盘本地 intent（含 `clientOrderId`），发送 TRADE 请求，由 READ_ONLY WS 确认落地；超时未确认则用 READ_ONLY REST 查询实际状态，**绝不盲目重发**。
- **紧急停止开关（kill-switch）**：`cancelBulkOrder` 必须可脱离主策略进程独立触发。

*所有探测脚本位于会话临时目录，未纳入版本库。报告中的判定均来自实际 HTTP 响应，无推测。*
