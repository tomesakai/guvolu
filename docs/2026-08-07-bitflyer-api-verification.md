# bitFlyer API 能力实测（2026-08-07）

- **文档类别**：时效快照（内容冻结，修订以新快照发布），登记于 [docs/00-rules-registry.md](00-rules-registry.md)
- **调查时刻**：2026-08-07 12:18 至 12:20 UTC
- **方法**：[scripts/probe_bitflyer.py](../scripts/probe_bitflyer.py) 只读探测（C-14），仅公开端点与私有读取端点，未触碰任何写端点；两把密钥各以 `getpermissions` 枚举能力（A-04 探测法）；响应原样落盘 `data/raw/2026-08-07/bitflyer/`，行格式沿用 storage-design 第 4 节
- **运行标识**：`runbfe8e4420c3f35`，REST 31 请求，2 个业务错误均为有意探测（31 天边界、权限边界）；公开 WS 采样 90 秒共 1,355 数据帧；私有 WS 认证与订阅核实通过
- **对照基准**：GMO 侧事实引自 [能力调查报告](2026-08-05-gmo-api-capability-report.md) 与 [数据范围实测](2026-08-06-gmo-data-scope-survey.md)，本文不复述（W-07）

## 1. 密钥能力实测

`.env` 中两把 bitFlyer 密钥（命名与 GMO 对称）实测权限：

| 密钥 | 公开端点 | 私有读取 | 写端点 | 出金端点 |
|---|---|---|---|---|
| `BITFLYER_READ_ONLY` | 全部 | 17 个 `me/` 读取端点 | 无 | 无 |
| `BITFLYER_TRADE` | 全部 | 与读取密钥几乎相同的全部读取 | 5 个下单撤单端点 | **含 `me/withdraw`** |

与 GMO 的根本差异：**bitFlyer 双密钥不正交**。TRADE 密钥同时持有完整读取能力（getbalance、getchildorders、getexecutions 等全部通过权限枚举确认），GMO 式「TRADE 无任何读取」的前提在 bitFlyer 不成立。T-02 的密钥隔离在 bitFlyer 侧只能由本仓库 client 类型层自行实施，交易所侧无此保证。

两项需处置的发现：

1. **TRADE 密钥权限含 `/v1/me/withdraw`（日元出金）与 `/v1/me/getbankaccounts`。** 本项目任何进程都不需要出金能力，该权限纯属风险面。密钥权限配置变更属人工确认事项（A-01），建议在 bitFlyer 管理界面为该密钥移除出金权限后再投入常态使用。
2. READ_ONLY 密钥缺 `/v1/me/getaddresses` 权限（实测 401 `Permission denied`，status `-500`），对本项目无影响，记录以核对权限清单的完备性。

写端点权限清单（仅枚举核实，未调用）：`sendchildorder`、`sendparentorder`、`cancelchildorder`、`cancelparentorder`、`cancelallchildorders`。其中 `cancelallchildorders` 可承担 kill-switch 全撤职责（对应 GMO 的 `cancelBulkOrder`）。

## 2. 公开 REST 实测

| 项 | 实测值 |
|---|---|
| 可交易品种 | 9 个：BTC_JPY、XRP_JPY、ETH_JPY、XLM_JPY、MONA_JPY、ELF_JPY、ETH_BTC、BCH_BTC、FX_BTC_JPY |
| `board` 盘口档数 | **全量返回，无档数上限**：BTC_JPY 实测 bids 1,043 档、asks 1,243 档；FX_BTC_JPY 641 与 642 档 |
| `executions` 单次上限 | 500（请求 `count=1000` 实返 500，静默截断而非报错） |
| `executions` 历史边界 | `before=1000` 返回 HTTP 400，`status -156`，`Execution history is limited to the most recent 31 days.` |
| 资金费率 | `getfundingrate?product_code=FX_BTC_JPY` 返回 200 |
| 板状态与健康度 | `getboardstate`、`gethealth` 两品种均正常 |
| 端点别名 | `/v1/markets` 与 `/v1/getmarkets` 等价，二者均实测可用 |

与 GMO 对照：GMO REST 盘口每侧至多 500 档，bitFlyer 无上限直接给全簿——REST 快照维度 bitFlyer 更深；但 GMO 逐笔约 1 万笔（约 21 小时）对 bitFlyer 31 天，历史回补维度 bitFlyer 更深。二者的逐笔均远浅于 bitbank 的按日全量。

## 3. 公开 WS 实测（90 秒采样）

四频道两品种（BTC_JPY、FX_BTC_JPY）：

| 频道 | 帧数 | 折算速率 | 帧均字节 |
|---|---|---|---|
| `lightning_board_FX_BTC_JPY` | 623 | 6.9 帧每秒 | 266 B |
| `lightning_board_BTC_JPY` | 396 | 4.4 帧每秒 | 218 B |
| `lightning_board_snapshot_BTC_JPY` | 19 | 每 5 秒一帧 | 21.0 KB |
| `lightning_board_snapshot_FX_BTC_JPY` | 19 | 每 5 秒一帧 | 20.5 KB |
| `lightning_ticker_FX_BTC_JPY` | 151 | 1.7 帧每秒 | 535 B |
| `lightning_ticker_BTC_JPY` | 125 | 1.4 帧每秒 | 538 B |
| `lightning_executions_FX_BTC_JPY` | 15 | 0.17 帧每秒 | 490 B |
| `lightning_executions_BTC_JPY` | 7 | 0.08 帧每秒 | 405 B |

盘口三频道形态（用户提示项，实测确认）：

1. **`board_snapshot`（节流快照）**：全簿快照，实测推送间隔稳定在 5.0 秒（36 个间隔样本中 33 个落在 4.9 至 5.1 秒）。
2. **`board`（增量差分）**：帧内仅 `asks`、`bids`、`mid_price` 三个键。数量为 0 表示档位删除。
3. REST `board`：随时可取的全簿快照，与 snapshot 频道内容同构。

**完整性实测结论**：差分帧与快照帧均**无 sequence、无 checksum、无 event_time**（键集合实测仅 `asks`、`bids`、`mid_price`）。ticker 帧有 `tick_id` 与 `timestamp`，executions 帧有逐笔 `id`，但盘口两频道没有任何可校验字段。叠加官方文档明示「单连接内消息保序」「断线期间数据不补发」，得到：

- 本地按差分维护的订单簿，**协议层无法证明未丢帧**；
- 唯一可用的校验方式是**以每 5 秒的节流快照对照重置本地簿**——两快照之间的差分应用结果与下一快照不一致时，该 5 秒窗口标记为不可信；
- 盘口帧无 event_time，事件时刻只能以本地 `ingest_time` 近似，时间语义必须显式标记（多源设计的 `time_origin` 列即为此）。

与 GMO 的失效形态对照（对校验设计是关键差异）：

| 来源 | 盘口流模型 | 丢帧后果 | 可检测性 |
|---|---|---|---|
| GMO | 30 档全量快照，约 2.1 帧每秒 | 缺一个采样时点，簿本身仍正确 | 不可检测，但损害有界 |
| bitFlyer | 差分加 5 秒节流快照 | **本地簿静默错误**，至多持续 5 秒 | 快照对照可事后检出并定界 |

即：GMO 丢帧丢「时点」，bitFlyer 丢帧丢「正确性」但有 5 秒自愈边界。两者都达不到 bitbank（序号）与 Kraken（校验和）的可证明等级。

## 4. 私有 WS 实测

READ_ONLY 密钥 JSON-RPC `auth`（HMAC-SHA256，`timestamp + nonce`）返回 `true`；`child_order_events` 与 `parent_order_events` 订阅均返回 `true`。账户无委托，无消息帧属预期。私有推送能力可用性已核实，消息形态待有委托时（bitFlyer 侧模拟运行阶段）再实测。

## 5. 量级推算

按本次采样折算（深夜时段 JST 21 时，日中另有倍数）：

| 项 | 日量（未压缩） |
|---|---|
| 两品种 `board_snapshot` | 约 717 MB（21 KB 乘 17,280 帧每品种） |
| 两品种 `board` 差分 | 约 30 MB |
| 两品种 ticker 加 executions | 约 13 MB |

体积主导项是节流快照而非差分，与 GMO（快照流主导）同型。若只为「差分校验」保留快照，可落盘时对快照做顶部 N 档裁剪或仅存散列，完整快照按需保留——该取舍在采集分层（storage-design 第 6 节的层语义）内决定，raw 不可变原则不受影响：落盘什么由采集层决定，落了就不改。

## 6. 对多源设计的输入

1. bitFlyer 完整性等级为「无」，但具备**带内快照对照**条件（5 秒节流快照），校验逻辑设计见 [multi-source-data-design.md](multi-source-data-design.md) 校验节。
2. 盘口帧无 event_time，`book_top.time_origin` 列必须落 `local`。
3. 双密钥不正交且 TRADE 密钥现含出金权限——venue 登记表的 `write_allowed` 语义在 bitFlyer 侧必须由本仓库类型层保证，并待人工完成权限收缩。
4. `executions` 的 `id` 为逐笔原生标识，`trade_tick.venue_trade_id` 直接可用，`id_origin` 为 `venue`（GMO 逐笔无原生 id，须合成标识，两者在同一张表内共存的依据即在此）。
5. 31 天逐笔回补窗口意味着 bitFlyer 侧冷启动必须在接入后 31 天内完成首轮回扫，且此后不允许出现超过 31 天的采集空洞。

## 7. 未核实项

| 项 | 状态 |
|---|---|
| `getfundingratehistory` | 权限清单确认存在，未调用 |
| Socket.IO 端点形态 | 未探测，本项目取 JSON-RPC 单形态即可 |
| 私有频道消息形态 | 需有委托时实测 |
| `board` 差分在断线重连后的首帧语义 | 需断线注入实验 |
| chats 端点 | 与量化无关，不探测 |
