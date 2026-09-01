# 执行链设计

> 文档类别：长期维护，登记于 [docs/00-rules-registry.md](00-rules-registry.md)。
> 本文是执行链的分阶段实施契约，也是 TBD-07、TBD-10 与 TBD-11 的提案载体
> （A-05，三项均为提案，待人工确认）。下单状态机的锁定推导见
> [架构文档](architecture.md) 第 3 节，上游研究契约见
> [策略研究管线](strategy-research.md)。

## 1. 执行链全貌

```text
paper 目标位置（研究域产物，见策略研究管线）
      |
      v
float 到 Decimal 转换闸门（G-05，唯一转换点）
      |
      v
意图生成（intent_id 先落盘入意图账本，T-05）
      |
      v
风控闸门（T-09 白名单 / R-02 熔断 / R-03 服务状态 / T-05 在途 / T-11 限额）
      |
      v
发送边界（OrderSender 抽象，TradeClient 适配，T-02；模拟运行在此拦截，T-04）
      |
      v
对账状态机（明确成功 / 明确失败 / 超时查询，T-03、T-06；R-08 双通道在阶段三闭合）
```

各环节的实现位置：

| 环节 | 模块 | 状态 |
|---|---|---|
| 意图模型与状态机 | `src/guvolu/domain/intent.py` | 阶段一实现，阶段二增补模拟拦截终态 |
| 意图账本 | `src/guvolu/data/intent_ledger.py` | 阶段一实现 |
| 三限额闸门 | `src/guvolu/risk/limits.py` | 阶段一实现 |
| 服务状态门禁 | `src/guvolu/risk/service_gate.py` | 阶段一实现 |
| 熔断骨架 | `src/guvolu/risk/circuit_breaker.py` | 阶段一实现（计数与触发） |
| 发送编排 | `src/guvolu/execution/dispatch.py` | 阶段一实现，阶段二增补拦截分类 |
| G-05 转换闸门 | `src/guvolu/execution/conversion.py` | 阶段二实现，阶段三增补差分入口 |
| TradeClient 发送适配 | `src/guvolu/execution/trade_sender.py` | 阶段二实现 |
| 超时对账查询 | `src/guvolu/execution/reconcile.py` | 阶段二实现，阶段三纳入自动调度 |
| dry-run 执行器 | `src/guvolu/execution/dry_run_executor.py` 与 `scripts/run_dry_run_executor.py` | 阶段二实现（一次性折算彩排入口） |
| 对账域委托状态 | `src/guvolu/execution/order_state.py` | 阶段三实现 |
| 双通道对账 | `src/guvolu/execution/dual_reconcile.py` | 阶段三实现 |
| 超时查询调度 | `src/guvolu/execution/timeout_scheduler.py` | 阶段三实现 |
| 熔断全撤接线 | `src/guvolu/execution/emergency_stop.py` | 阶段三实现 |
| 对账会话 | `src/guvolu/execution/reconcile_session.py` 与 `scripts/run_reconcile_session.py` | 阶段三实现 |
| 常驻浸泡进程 | `src/guvolu/execution/soak_runner.py` 与 `scripts/run_execution_soak.ps1` | 阶段四实现 |
| 跨进程在途锁 | `src/guvolu/execution/inflight_lock.py` | 阶段四增补 |
| 限额用量重放 | `src/guvolu/execution/limit_replay.py` | 阶段四增补 |

G-05 转换点：研究域目标位置的数值以 float 承载，进入执行域时必须经
`execution.conversion` 完成 float 到 Decimal 的转换，并按品种 `tickSize`
与 `sizeStep` 取整，超界拒绝。模块提供两个公开入口：一次性折算
`convert_target_to_order` 与差分折算 `convert_target_to_delta_order`
（阶段三），两者共用模块内唯一的转换语句 `_target_to_decimal`，该处是
全项目唯一允许两域相接的位置，配套单测覆盖取整与拒绝边界。除此之外
执行域一切入口只接受 Decimal（T-08）。目标取值域为 [-1, 1]，正买负卖；
数量向下取整到 `sizeStep` 绝不超过目标名义，限价按买向下、卖向上取整到
`tickSize`，不劣于参考价；折算数量低于最小委托量时不生成委托。

## 2. 阶段划分

| 阶段 | 范围 | 依赖 |
|---|---|---|
| 一（已完成） | 意图状态机、意图账本、三限额闸门、服务状态门禁、熔断计数骨架；发送为注入抽象 | 无 |
| 二（已完成） | 目标位置消费与意图生成（G-05 转换点）、TradeClient 适配 OrderSender、dry-run 执行器（T-04）、超时意图的人工触发对账查询（T-06 后半） | 阶段一 |
| 三（已完成） | READ_ONLY 对账消费：WS 实时回报与定时 REST 快照双通道（R-08、C-10、TBD-07）、超时意图的自动查询决策（T-06）、熔断触发动作接入紧急停止开关（T-07）、目标跟踪差分与对账会话命令行 | 阶段二 |
| 四（浸泡进程本次交付） | 常驻对账浸泡进程与通道时延竞态观测（第 12 节）；其后回测、模拟运行纸上交易、最小手数实盘逐级验证（T-12）；切换实盘需人工确认（T-04、A-01） | 阶段三 |

## 3. 意图账本

- 位置：数据根下 `execution/intent_ledger.jsonl`，由 `data.paths.data_root` 于运行时解析，代码不硬编码绝对路径（C-04）。
- 形态：追加式 JSONL，每行一条事件，写入即 fsync（复用 `data/durable_io` 原语）；意图创建行先于任何发送落盘（T-05），每次状态迁移各成一行（R-07），永不覆写既有行。
- 映射：受理时登记交易所 `orderId` 与 `intent_id` 的映射，`orderId` 不得重复映射（T-05、D-05）。
- 在途约束：同品种同一时刻至多一笔在途写请求；`SENDING` 与 `SEND_TIMEOUT` 均占用在途额度，超时意图未经查询定论前不得对同品种再发（T-05、T-06）。多进程各持独立账本时，消耗写预算的发送期间另持品种级独占文件锁（数据根 `execution/.inflight/<symbol>.lock`，`execution.inflight_lock`，锁随进程句柄释放）：`begin_send` 前非阻塞取得，终态落账后释放，取不到即按闸门拒绝；零写发送路径不取锁。
- 恢复：装载时重放全量事件重建状态并复验每次迁移合法性；进程中断留下的尾部不完整行移入同目录 `.partial-` 旁证文件后截断主文件，不静默丢弃字节；处于 `SENDING` 的意图恢复后经显式标记转入 `SEND_TIMEOUT`，等待查询决策（T-06）。
- 事件行字段：

| 字段 | intent 行 | transition 行 |
|---|---|---|
| `schema_version` | 3 | 3 |
| `record` | `intent` | `transition` |
| `at` | 落盘时刻（UTC） | 迁移时刻（UTC） |
| 标识 | `intent_id`、`correlation_id` | `intent_id` |
| 委托字段 | `symbol`、`side`、`execution_type`、`size`、`price`、`time_in_force`、`created_at` | 无 |
| 迁移字段 | 无 | `source`、`target`、`order_id`、`reason`、`evidence`、`write_budget` |

金额与数量以字符串落盘（D-07）。`evidence` 为查询证据键值表，离开
`SEND_TIMEOUT` 的迁移必须携带（T-06）。第 2 版增血缘字段、第 3 版增
`write_budget` 标记，均只增字段（D-06），旧版行兼容读取。`write_budget`
只在进入 `SENDING` 的迁移行有值，取 `consumed` 或 `exempt`，记录该笔
是否消耗写预算（T-11，口径见第 5 节）；无标记的旧版行按消耗计。

## 4. 意图状态机

| 状态 | 含义 | 类别 |
|---|---|---|
| `RECORDED` | 已落盘，未过闸门 | 起点 |
| `GATE_REJECTED` | 风控闸门拒绝 | 终态 |
| `SENDING` | 已过闸门，写请求在途 | 在途 |
| `ACCEPTED` | 交易所受理，`orderId` 已映射 | 终态（账本视角） |
| `REJECTED` | 交易所明确拒绝 | 终态 |
| `SEND_TIMEOUT` | 发送超时或网络错，结果未知 | 在途 |
| `FAILED` | 经 READ_ONLY 查询确认未受理 | 终态 |
| `DRY_RUN_BLOCKED` | 模拟运行守卫在发送边界拦截，未触达写端点 | 终态 |
| `PAPER_FILLED` | paper 成交模型在发送边界结算，未触达写端点 | 终态 |
| `PAPER_REJECTED` | paper 成交模型在发送边界拒绝结算，未触达写端点 | 终态 |

| 迁移 | 守卫 |
|---|---|
| `RECORDED -> GATE_REJECTED` | 记录拒绝理由 |
| `RECORDED -> SENDING` | 同品种无在途意图（T-05） |
| `SENDING -> ACCEPTED` | 必须携带 `orderId` |
| `SENDING -> REJECTED` | 记录错误码 |
| `SENDING -> SEND_TIMEOUT` | 记录超时事由 |
| `SENDING -> DRY_RUN_BLOCKED` | 记录拦截理由（T-04） |
| `SENDING -> PAPER_FILLED` | 必须携带成交模型证据（T-04） |
| `SENDING -> PAPER_REJECTED` | 记录拒绝理由（T-04） |
| `SEND_TIMEOUT -> ACCEPTED` | 必须携带 READ_ONLY 查询证据与 `orderId`（T-06） |
| `SEND_TIMEOUT -> FAILED` | 必须携带 READ_ONLY 查询证据（T-06） |

上表之外的一切迁移非法，账本直接拒绝且不落盘。`ACCEPTED` 是账本视角的
终态：其后的委托生命周期（成交、撤销、失效）属对账域，由阶段三经 READ_ONLY
消费（T-03、U-01），不回写意图账本。`DRY_RUN_BLOCKED` 是本地终点：拦截发生
在任何网络调用之前，与交易所明确拒绝的 `REJECTED` 严格区分（T-03），不计入
熔断的写路径异常计数，是模拟运行彩排的预期终点（T-04）；三限额校验在
彩排与实盘同口径执行，用量累计仅真实写请求记入（第 5 节）。

## 5. 风控闸门

发送编排按固定次序过闸，任一环节拒绝即把意图记为 `GATE_REJECTED`：

| 次序 | 闸门 | 规则 | 拒绝动作 |
|---|---|---|---|
| 1 | 品种白名单 | T-09 | 拒绝意图 |
| 2 | 熔断状态 | R-02 | 拒绝意图 |
| 3 | 服务状态 | R-03 | 拒绝意图 |
| 4 | 在途约束 | T-05 | 拒绝意图 |
| 5 | 三限额 | T-11 | 拒绝意图并触发熔断 |

- 三限额（T-11）：单笔金额、单日累计金额、单日笔数，取值来自 `domain.config.Limits`（装载时已按绝对硬顶截取）。名义金额为 `size` 乘限价，市价意图乘调用方给出的参考价；单日归属按 JST 06:00 交易日边界（C-11、D-08）。三项校验对全部发送模式照常执行；用量累计只在发送边界消耗写预算（`OrderSender.consumes_write_budget` 为真，即真实写路径）时记入——T-11 的语义边界是真实写请求，零写终态（模拟拦截、paper 结算与拒绝）不占单日预算；记入后即使发送超时或被拒也不回退，保守计数。账本的 `SENDING` 迁移行落 `write_budget` 标记（第 3 节），dry-run、paper 与浸泡入口启动时经 `execution.limit_replay` 按该标记重放当日用量。运行时限额只可调低，调高请求直接拒绝（X-05）。超限不是普通拒绝，而是按 T-11 触发熔断。
- 服务状态门禁（R-03）：仅 `OPEN` 允许生成新意图。撤单不经本门禁，与紧急停止开关口径一致（T-07 紧急路径必须随时可达）；维护期交易所拒绝撤单时由错误处置承担。
- 熔断器（R-02）：状态机仅 `NORMAL` 与 `TRIPPED` 两态。计数域为写路径连续异常（明确失败、超时、网络错各计一次，成功清零）；双通道对账不一致计入同一计数（R-08）；行情断流秒数与资产异动达到阈值直接触发。触发动作为拒绝新意图，并自阶段三起执行登记的紧急停止全撤（T-07，接线见第 9 节）；进程退出动作未接入。复位仅经显式运维调用，不自动恢复。阈值从版本化配置 `config/circuit_breaker.json` 读取（G-06）。

## 6. TBD-10 熔断阈值提案

以下数值为提案，待人工确认后在 [架构文档](architecture.md) 台账回填；全部为
版本化配置（G-06），登记于 `config/circuit_breaker.json`（schema_version 1）。

| 阈值 | 提案值 | 理由 |
|---|---|---|
| 连续异常次数 | 3 次 | 写请求永不自动重试（C-08、T-06），连续三笔独立意图异常已超出网络抖动的合理解释，指示系统性故障；当前验证资金规模下宁可停摆 |
| 行情断流秒数 | 90 秒 | GMO WS 心跳周期 60 秒（C-10），取一个心跳周期加二分之一余量：单次调度延迟不误触，两个心跳周期内必触发 |
| 资产异动比例 | 1%，绝对下限 30 JPY | 口径为对账时点无法由账本内委托、成交与手续费解释的資産残高 `amount` 合计差额（U-03 语义限定，即 R-02 所称余额异动）。理论应为零，任何未解释差额都可疑；按当前约 3,009 JPY 规模，1% 约 30 JPY，低于最小委托额（约 101 JPY），可在单笔异常成交前触发；绝对下限防止小规模资产下尘埃级差额反复误触 |

## 7. TBD-11 落地口径：统一 cancel + replace

执行链不使用 `changeOrder`，改价改量一律先撤后下
（[GMO API 能力报告](2026-08-05-gmo-api-capability-report.md) 第 7 节倾向）：

- 理由一：`changeOrder` 引入改单中间态与 `ERR-5123` 分支，改单请求超时后新旧两个价格版本无法唯一对账；先撤后下的每一步结果二元，均可独立经 READ_ONLY 判定（T-06 的可判定性优先于往返次数）。
- 理由二：与同品种单在途约束（T-05）吻合，任一时刻至多一笔在途写请求，无需为改单增设并发例外。
- 理由三：代价为两次写请求与排队位置损失，在 R-04 限速余量与当前验证资金规模下可接受。

口径如下：

- 撤单意图与替换意图各持独立 `intent_id`，以同一 `correlation_id` 关联血缘（D-05、X-08）。
- 撤单发出后必须经 READ_ONLY 确认旧委托终态方可生成替换意图；确认前该品种保持在途占用。
- 确认结果为已全部成交时放弃替换（U-01 委托与成交不混同）；部分成交时替换数量按 READ_ONLY 报告的剩余量重算，不复用原数量（T-03）。
- 撤单请求不经服务状态门禁与模拟运行守卫（T-07 口径）；替换下单按新意图走第 5 节全量闸门。
- `changeOrder` 端点保留在 `TradeClient` 供人工处置，不进入执行链自动路径。

## 8. 发送适配、执行器与超时对账（阶段二）

发送适配 `execution.trade_sender.TradeClientSender` 实现 `OrderSender`，内部
只持 `api.trade_client.TradeClient`（T-02）。适配器把发送结果收敛为编排可
判定的三分类（T-06）：受理返回 `orderId`；业务错误原样上抛记为 `REJECTED`；
超时与网络错、HTTP 层异常、响应形态异常与委托号不可解析一律按结果未知折算
为网络错记为 `SEND_TIMEOUT`，绝不重试（C-08）。模拟运行守卫保持在
`TradeClient` 内（T-04），撤单透传不受其限制（T-07）。

dry-run 执行器 `execution.dry_run_executor`（命令行封装
`scripts/run_dry_run_executor.py`）消费 target-position 制品的
`operational_target_contract.aggregate_target`，经 G-05 转换闸门折算为单笔
限价意图，过第 5 节全量闸门后进入发送适配；模拟运行模式下被拦截记为
`DRY_RUN_BLOCKED` 即预期终点，账本留痕全程（T-05、R-07）。报告列明品种、
方向、数量、金额与触碰端点：实盘将触碰的写端点仅 `POST /v1/order`，缺省
拉取取引ルール、参考价与服务状态时另触碰三个公开只读端点（A-03）。本
执行器保留一次性从零折算，作为无对账依赖的链路彩排入口；目标跟踪差分
自阶段三起由对账会话承载（第 9 节）。

超时对账 `execution.reconcile.resolve_send_timeout` 经注入的只读抽象查询
挂单一览与最新成交一览（`ReadClient` 为生产实现，T-02），以同品种单在途
约束保证唯一匹配（T-05）：恰一笔未映射候选即 `ACCEPTED` 并登记映射，零笔
即 `FAILED`，多笔为歧义，拒绝自动判定并保持超时态，留待人工处置。查询
证据随迁移写入账本（T-06）。人工触发入口 `--resolve-timeouts` 保留，
自阶段三起同一逻辑由超时查询调度自动执行（第 9 节）。

## 9. 双通道对账、自动超时与差分（阶段三）

对账域：`execution.order_state` 承载 `ACCEPTED` 之后的委托生命周期与
成交事实（T-03，委托与成交分体，U-01），意图账本不回写。事实来源仅限
READ_ONLY 的两条通道，成交按 `execution_id` 去重（D-05），WS 应用带
乱序守卫（已成量不回退、终态不复活）；持仓类事件属杠杆域，现物执行链
忽略（T-09）。

- WS 通道：`execution.dual_reconcile.apply_private_event` 消费
  `api.ws_private` 解析出的 `orderEvents` 与 `executionEvents` 事实，
  累积对账域状态。未映射委托的事件与恰一笔同品种超时意图相符（品种、
  方向、类型、数量、价格与时窗；同品种单在途保证唯一，T-05）时，事件
  本身即 READ_ONLY 证据，受理并登记映射（T-06）。
- REST 通道：按 TBD-07 周期拉取全量快照（挂单一览、最新成交一览、
  資産残高），与 WS 累积状态比对；不一致以 REST 为准（T-03），稳态轮
  每处不一致记为异常事件计入熔断计数（R-08）。WS 在场而 REST 未列的
  委托按 `GET /v1/orders` 查询终态覆写，查询无果按失效登记。
- 快照三模式：基线（首轮，无比对起点）与重连补齐（C-10）只对齐不
  计数——断线期间的增量缺口是重连快照的预期修复对象，不构成双通道
  同时在线的相互矛盾；稳态周期快照计数。重连后的强制全量快照由
  `ReconcileSession.on_ws_reconnect` 执行，接 `PrivateWsClient.run` 的
  重连回调。
- 资产核对：以会话首轮快照建立資産残高 `amount` 基线（U-03），此后仅
  账本内委托的成交与手续费推进期望值；未解释差额按快照汇率折 JPY 交
  熔断器判定（口径见第 6 节 TBD-10）。
- 自动超时查询：`execution.timeout_scheduler` 把阶段二的人工对账纳入
  对账循环：意图进入 `SEND_TIMEOUT` 即到期，首查在当轮执行；歧义与
  查询自身失败按指数退避重查（倍率 2，参数见第 10 节），直到意图离开
  超时态，含 WS 通道抢先受理后的自动出队。查询是只读 GET，自动重试
  合规（C-08）；写请求仍绝不重发（T-06）。
- 熔断动作升级：熔断器支持登记触发动作，`execution.emergency_stop` 把
  `ops.kill_switch.cancel_all` 接为动作（T-07），首次触发即全量撤单并
  留痕（R-07）；动作失败不回滚熔断状态，留待人工处置。`kill_switch`
  自身保持零依赖不变，执行侧只调用其函数。
- 差分决策：`conversion.convert_target_to_delta_order` 把目标折算为
  目标持仓（目标乘预算除参考价，带符号），差分为目标持仓减账本推算
  持仓；持仓只来自 READ_ONLY 成交事实（两通道并集，限账本映射委托），
  受理回执不计（T-03）。不交易带与研究配置 `no_trade_band` 同口径
  （目标权重空间比例），在执行侧独立配置，避免执行域反向依赖研究
  配置文件；差分名义低于不交易带乘预算时不生成委托，恰等边界时生成。
- 对账会话命令行：`scripts/run_reconcile_session.py` 单命令跑一轮
  「WS 注入事实、快照对账、超时处理、差分决策」的 dry-run 会话
  （T-04），差分意图过第 5 节全量闸门进入发送适配。报告列明触碰端点
  （A-03）：只读为 `GET /v1/activeOrders`、`GET /v1/latestExecutions`、
  `GET /v1/account/assets`，歧义裁决另加 `GET /v1/orders`，基线回填另加
  `GET /v1/executions`，缺省拉取市场输入时另加公开端点；写计划仅
  `POST /v1/order`；熔断触发时全撤真实触碰 `GET /v1/symbols` 与
  `POST /v1/cancelBulkOrder`（T-07 不受模拟运行守卫限制）。退出码非零
  表示存在歧义悬置、意图非预期终态或熔断已触发。

## 10. TBD-07 对账周期提案

以下数值为提案，待人工确认后在 [架构文档](architecture.md) 台账回填；
全部为版本化配置（G-06），登记于 `config/reconcile_session.json`
（schema_version 1）。

| 参数 | 提案值 | 理由 |
|---|---|---|
| REST 快照周期 | 30 秒 | 单轮快照触碰三个只读端点，单品种平均 0.1 req/s，占 R-04 自我约束 10 req/s 的百分之一，留足余量；30 秒为 WS 心跳周期（60 秒，C-10）之半、行情断流阈值（TBD-10 提案 90 秒）的三分之一，断流触发熔断前必有一轮快照佐证 |
| 超时查询初始退避 | 5 秒 | 首查在进入超时态当轮即时执行（T-06）；5 秒起步覆盖交易所受理落账延迟，避免连续空查 |
| 超时查询退避上限 | 300 秒 | 指数退避封顶五分钟：歧义悬置期间保持在途占用（T-05），既不放弃查询也不挤占限速余量（R-04） |
| 不交易带 | 0.01 | 复用研究配置 `no_trade_band` 同口径数值（目标权重空间比例，见 [策略研究管线](strategy-research.md)）；按当前预算 500 JPY 折 5 JPY，低于最小委托额约 101 JPY，滤掉尘埃级往返 |

## 11. 阶段三边界与未决

- 对账会话保留单轮命令行；长期驻留进程已随阶段四交付（第 12 节），意图自动生成仍以目标制品差分为唯一入口。
- 稳态快照的不一致计数存在通道时延竞态：成交先入快照而 WS 帧后到会计入一次不一致。保守取向符合 R-02；观测项已在浸泡进程落地（第 12 节），仅记录分类计数与时间差分布，不改变计数逻辑，浸泡观测误计率后再议。
- 资产核对基线为会话内存态，进程重启即重建；基线持久化与状态恢复联动（TBD-09）未决。
- 真实发送仍缺省不可达（T-04），切换实盘需人工确认（A-01）；本阶段全部测试注入替身，无任何触达交易所端点的调用（C-14）。熔断触发的全量撤单在实际运行中真实可达是 T-07 的设计要求，测试中同样以替身断言。
- 未决关联项：TBD-07 待人工确认；多策略委托归属（TBD-08）；执行进程状态持久化（TBD-09）。

## 12. 常驻浸泡进程（阶段四）

`execution.soak_runner`（命令行封装 `scripts/run_execution_soak.py`，
PowerShell 包装 `scripts/run_execution_soak.ps1`）把第 9 节的单轮逻辑
编排为长期驻留循环，复用 `ReconcileSession`，不复制。启动即断言运行
模式为模拟运行，live 配置直接拒绝启动并返回退出码 2（T-04）；本阶段
不提供 live 入口，切换实盘另行人工确认（A-01）。

- WS 常连：经 `api.ws_private` 令牌生命周期（签发、周期延长、停机
  撤销）常连消费 orderEvents 与 executionEvents；断线由客户端按退避
  重连，重连回调把下一轮标记为强制全量快照并立即唤醒（C-10）。
- 周期循环：按 `config/reconcile_session.json` 的快照周期执行「快照
  对账、超时处理、差分决策」（TBD-07、G-06），命令行可临时覆写
  周期；轮内错误留痕后按周期重试，连续三次即停止。只读对账收到
  ERR-5201 或服务状态非 OPEN 时记「维护窗暂停」事件、跳过本轮且
  不计轮错误，直至恢复（R-03，处置见错误码册）；连续维护窗超过
  具名常量 `MAINTENANCE_PAUSE_LIMIT_SECONDS`（4 小时）仍停机防呆。
- 目标来源：`--target` 制品路径每轮重读，制品更新即生效；制品损坏
  当轮跳过差分并记录错误；缺省目标为零，零目标落在不交易带内，不
  生成任何委托，即纯只读浸泡。
- 停止双通道：控制台中断（SIGINT）与停止标记文件；停止请求不打断
  执行中的轮，完成当轮后写终态 checkpoint 再退出。
- 落盘：每轮报告追加 JSONL 并累计触碰端点（A-03）；checkpoint 与
  心跳文件原子覆写，心跳按节流间隔刷新供外部判活。缺省位置在数据
  根 `execution/` 下（C-04），意图账本沿用第 3 节契约。
- 端点口径（A-03）：只读为第 9 节会话集合加每轮刷新的公开端点；
  令牌生命周期为 `POST`、`PUT`、`DELETE /v1/ws-auth`（READ_ONLY
  密钥）；写计划仅差分生成时的 `POST /v1/order`，模拟运行不触碰；
  熔断触发的全撤沿第 9 节口径真实触碰（T-07）。
- 退出码：0 正常停止；1 停止时熔断已触发；2 live 配置拒绝启动。

通道时延竞态观测（第 11 节登记项）：稳态快照轮在裁决前留存 WS
累积视图与 REST 快照行，分类计数「WS 已见但 REST 未含」的场内
委托、「REST 已含但 WS 未见」的场内委托与「REST 先见」的成交，
时间差取观测时点与事实时戳之差，按轮与累计汇总计数、最小、均值、
最大，随每轮报告落盘。观测不改变裁决与熔断计数逻辑（R-08 口径
不变）。

## 13. 最小实盘 canary 入口（阶段五）

入口为 [scripts/run_live_canary.py](../scripts/run_live_canary.py)，逻辑在
`execution.live_canary`。它是 T-12 第三级「最小手数实盘」的唯一入口，按
[策略研究管线](strategy-research.md) 第 6 节 canary 合同实现，不服务任何
自动化调度。

- 前置条件：`GUVOLU_MODE=live` 由人工显式设置（T-04、A-01），入口不代为
  切换；进程必须挂交互式终端，启动横幅醒目标示 live 模式、委托计划、
  当前限额与将触碰的全部端点，随后要求两重键入确认——原样口令与名义
  金额复述（X-02）。任一确认失败即退出，不发送任何写请求。
- 计划约束：固定现物 BTC 单笔限价买入；限价缺省取最优买价并向下取整到
  tick，不得越过最优买价；名义不超过 canary 合同 500 JPY 与当前 T-11
  单笔限额的较小者；数量须落在取引ルール步长与上下限内。
- 入场前先定退出（R-01）：等待窗口届满即经 `POST /v1/cancelOrder` 撤单
  并轮询确认终态；撤单后仍未确认终态时明示改用 kill-switch（T-07）。
- 过程约束：发送经第 3 至 5 节统一编排（意图先落盘、五道闸门、消耗写
  预算、跨进程在途锁）；服务状态非 OPEN 不发写请求（R-03）；发送前核对
  可用 JPY 覆盖名义加手续费缓冲（R-06）；委托状态一律以 READ_ONLY 轮询
  为准（T-03）；发送超时经第 8 节超时对账查询后决策（T-06）。
- 落盘：意图账本沿第 3 节契约；终局报告内容寻址落在数据根
  `execution/canary/`，含计划、终态、撤单标记、前后资产快照（amount 与
  available 分列，U-03）与触碰端点清单（A-03、R-07）。
- 端点口径（A-03）：读取为 `GET /v1/status`、`GET /v1/symbols`、
  `GET /v1/orderbooks`、`GET /v1/account/assets`、`GET /v1/orders`、
  `GET /v1/activeOrders`、`GET /v1/latestExecutions`；写入为
  `POST /v1/order` 与 `POST /v1/cancelOrder`。
- 完成后按合同立即停机复核，不自动扩到其他品种、来源或资金规模。

## 14. 授权信封（阶段六）

授权信封是 live 自动交易的边界合同：维护者填值签发的内容寻址 JSON
文件。live 执行路径启动时校验信封，任一条件不满足即拒绝进入 live；
用量随意图账本持久追踪，重启不重置；触界即按登记动作熔断停机。
签发信封与运行上膛命令构成两重人工确认（X-02 语义由「每委托键入」
改为「每信封一次」，2026-09-01 经维护者确认）。T-12 三级中的最小
手数实盘由首封实现：`envelope_jpy_total` 等于单笔上限即只容一笔，
耗尽停机复核后方可签发常规信封。

| 字段 | 语义 | 越界处置 |
|---|---|---|
| `valid_from` / `valid_until` | 有效期，UTC | 期外拒绝进入 live |
| `order_jpy_max` | 单笔名义上限，不得超过 T-11 硬顶 | 拒单 |
| `day_jpy_max` / `day_count_max` | 当日累计额与笔数，不得超过 T-11 硬顶 | 熔断（T-11） |
| `envelope_jpy_total` | 信封生命周期累计下单总额 | 耗尽即停机 |
| `max_position_jpy` | 多头持仓名义上限 | 拒绝加仓 |
| `max_cumulative_loss_jpy` | 信封内已实现加浮动亏损熔断线 | 熔断并执行 `on_trip` |
| `breaker` | 连续写失败、断流秒数、资产异动比例与下限 | 熔断（R-02） |
| `on_trip` | `cancel_only` 或 `cancel_and_flatten` | 熔断动作 |
| `day_loss_jpy_max` | 当日已实现加浮动亏损熔断线 | 当日停机 |
| `canary_first_order_jpy_max` | 信封首单名义上限（T-12 最小手数级），首单终态且双通道对账通过后解除 | 首单拒超 |
| `max_prediction_age_minutes` | 陈旧目标不执行 | 跳过该轮 |
| `market_risk.price_move_pause` | 参考价短窗急变超阈即暂停下单一段时间，仅允许撤单 | 暂停下单 |
| `market_risk.spread_skip_bp` | 盘口价差超阈跳过该轮 | 跳过该轮 |
| `market_risk.min_book_depth_ratio` | 对手侧盘口深度不足委托名义倍数跳过 | 跳过该轮 |
| `market_risk.stream_gap_seconds` | 行情断流超秒数熔断（R-02） | 熔断 |
| `ops_breaker` | 连续写失败、资产异动比例与下限 | 熔断（R-02） |
| `symbols` | 现物品种白名单（T-09 子集） | 拒单 |

信封文件位于 `config/authorization_envelope.json`，SHA-256 进入执行
报告与意图账本行；具体取值由维护者签发时决定（G-06），不在本文固化。
上膛协议：维护者签发信封、设 `GUVOLU_MODE=live` 并亲自启动或注册
live 执行任务；代理可编写与测试全部代码，不代行上膛与首次启动。
