# GMO 委托标识勘误（2026-08-05）

- **文档类别**：时效快照（内容冻结，修订以新快照发布）
- **性质**：对同日能力调查报告的事实勘误
- **核实方式**：官方文档全文检索（本地留存的完整 HTML），`clientOrderId` 出现次数为零

## 勘误内容

| 原表述位置 | 原表述 | 核实结果 |
|---|---|---|
| 能力调查报告第 2 节第 8 行 | 注文情報取取参数「`orderId` 或 `clientOrderId`」 | 实际仅 `orderId`，逗号连接最多 10 个 |
| 能力调查报告第 11 节 | 「clientOrderId 为两把密钥间唯一关联键，必须自生成」 | **GMO API 不存在客户端自定义委托号**。`POST /v1/order` 参数仅 symbol、side、executionType、timeInForce、price、losscutPrice、size、cancelBefore，响应 data 即交易所分配的 orderId |

原表述来源为一次不可靠的文档摘要抓取，成为 A-04（不得凭记忆或摘要推测）的又一实证。

## 修正后的关联模型

- 本地生成 `intent_id`（下单前落盘，承担 T-05 的幂等与追溯职责）。
- `POST /v1/order` 成功响应同步返回交易所 `orderId`，立即持久化 `intent_id` 与 `orderId` 的映射。
- 两把密钥之间的实际关联键为**交易所 orderId**。
- 写请求超时未获 orderId 时（T-06）：以 READ_ONLY 查询 `activeOrders` 与 `latestExecutions`，按品种、方向、数量、价格与提交时间窗匹配。
- 配套纪律（建议随 T-05 修订一并确认）：**同一品种同一时刻至多一笔在途写请求**，保证超时匹配唯一。

## 附带核实的参数事实

| 端点 | 新核实事实 |
|---|---|
| 4 个履历端点 | `fromTimestamp` 与 `toTimestamp` 间隔最长 30 分钟；仅给 from 时 to 自动取 from 加 30 分钟 |
| `POST /v1/order` | `cancelBefore` 参数存在，仅限现物 MARKET 卖出场景 |
| `POST /v1/cancelBulkOrder` | 参数为 `symbols`（数组）、可选 `side`、`settleType`、`desc` |
| `GET /v1/activeOrders` | `symbol` 为必填参数 |
| WS 令牌 | 有效期 60 分钟，PUT 延长、DELETE 撤销 |
