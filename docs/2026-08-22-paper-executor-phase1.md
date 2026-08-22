# 2026-08-22 paper 执行器阶段一落地快照

> 时效快照（W-02）：内容冻结于 2026-08-22，修订以新快照发布。
> 本文记录决策生成 I/O 契约 v2 提案（主仓 `docs/2026-08-22-decision-io-contract-v2.md`，
> 主仓 TBD 台账 2026-08-22 登记项）中执行目标与执行器义务在本执行仓的落地点、
> paper 成交模型、差异账与 L2 覆盖层门控记录，以及阶段二待办。
> 阶段一全程零资金、零写请求（T-04、T-13）：paper 路径不构造任何私有客户端，
> 发送边界由成交模型结算或拒绝，报告中 `write_touched` 恒为空。
> 本文局部用语：paper 持仓账指只由模型成交累积的本地库存账；差异账指每个决策
> 一行、记录目标、差分意图、模型成交、成本分解与门控判定的追加式账目。

## 1. 范围与结论

| 项 | 结论 |
|---|---|
| 执行目标适配器 | 升为第 2 版，只增字段（D-06）；拒绝负值与越界目标；派生有效期与因果链标识并标注来源 |
| paper 执行器 | 新增 `execution.paper_executor` 与 `scripts/run_paper_executor.py`；差分转换、paper 库存、闸门链、paper 发送边界、差异账 |
| 成交模型 | `execution.paper_fill_model` 按盘口深度逐档估算 taker 成交与成本分解；阶段一盘口来源为公开端点快照 |
| 意图状态机 | 新增 `PAPER_FILLED`、`PAPER_REJECTED` 两个本地终态，只增不改既有语义（D-06） |
| 意图账本 | 升为第 2 版，意图行增 `prediction_id`、`decision_time`；`correlation_id` 自执行目标继承；第 1 版行兼容读取 |
| 配置 | `config/paper_executor.json` 承载名义预算、不交易带、费率降级值与覆盖层阈值（G-06）；预算装载时校验不超过 T-11 硬顶 |

## 2. 契约落地点

| 契约项 | 落地位置 | 说明 |
|---|---|---|
| 执行目标 schema 第 2 版 | `src/guvolu/execution/frozen_target_adapter.py` `build_operational_target` | 第 1 版字段全部保留，新增见第 3 节 |
| 目标域唯一 | 同上 `_validate_target_value`、`_validate_semantics`、`TARGET_SEMANTICS` | 负值与大于 1 在适配器拒绝；第 2 版预测自带 `target_semantics` 时必须一致 |
| 有效期显式 | 同上 `_resolve_validity`、`paper_config.bar_interval_duration` | 预测自带 `valid_until` 优先并标注 `prediction`；否则按决策柱间隔派生并标注 `derived` |
| 因果链标识 | 同上 `derive_correlation_id` | 预测自带则继承并标注 `prediction`；否则由 `prediction_id` 确定性派生并标注 `adapter`，同一预测重跑得到同一标识，目标快照保持内容寻址 |
| 预算来自配置 | `src/guvolu/execution/paper_config.py` `load_paper_config`、`ensure_budget_within_ceiling` | 命令行无缺省预算；显式参数可覆盖配置但同样受硬顶约束 |
| 消费前校验 | `src/guvolu/execution/paper_executor.py` `load_execution_target`、`validate_execution_target` | 版本、目标域、`exposure_target` 区间、`valid_from` 与 `valid_until`、`market_id` 与 `symbol` 一致、`mode` 为 `paper`、预算不超过配置 |
| 差分转换 | 同上 `run_paper_decision` 调用 `conversion.convert_target_to_delta_order` | float 到 Decimal 仍只经 G-05 唯一闸门；库存来自 paper 持仓账 |
| 永不从零开卖单 | 同上 | 目标域非负且库存非负，差分卖出数量不超过库存；超过时记为 `sell_exceeds_position` 不生成意图 |
| 意图血缘 | `src/guvolu/domain/intent.py` `OrderIntent.prediction_id`、`decision_time`；`src/guvolu/data/intent_ledger.py` `SCHEMA_VERSION = 2` | 意图行同时携带继承的 `correlation_id`（R-07、X-08） |
| 发送边界 | `paper_executor.PaperFillSender`、`dispatch.dispatch_order_intent` | 闸门次序不变：账本落盘、白名单、熔断、服务状态、单在途、T-11 限额，随后由成交模型抛出 `PaperSettled` 或 `PaperRejected` |
| 差异账 | `paper_executor.DifferenceLedger`，缺省 `data/execution/paper/difference_ledger.jsonl` | 每决策一行，同一 `prediction_id` 重跑记为 `duplicate_prediction` 且不再追加意图 |
| 按日汇总 | `src/guvolu/execution/paper_ledger_summary.py`、`scripts/summarize_paper_ledger.py` | 按 JST 06:00 交易日边界聚合（D-08） |

## 3. 与执行目标契约的逐项对应

| 契约字段 | 第 2 版快照字段 | 来源 |
|---|---|---|
| `correlation_id` | `correlation_id`、`correlation_id_source` | 预测继承或适配器派生 |
| `prediction_id` | `run_id`、`lineage.prediction_id` | 预测 |
| `decision_time` | `decision_time`、`valid_from` | 预测 |
| `valid_until` | `valid_until`、`valid_until_source`、`bar_interval` | 预测或派生 |
| `market_id`、`symbol` | `market_id`、`symbol` | 配置或命令行 |
| `exposure_target` | `exposure_target`、`target_semantics` | 预测 `aggregate_target` 夹到 `[0, 1]`，负值已拒绝 |
| `risk_budget_jpy` | `risk_budget_jpy` | 配置，字符串承载 Decimal（D-07） |
| `mode` | `mode` | 命令行 `--mode`，缺省 `dry-run`（T-04） |
| `lineage` | `lineage{plan_id, input_head_generation, source_prediction_path, source_prediction_sha256, decision_input_sha256?}` | 预测；`decision_input_sha256` 仅第 2 版预测携带 |

执行器义务逐项：`valid_until` 未过且 `market_id`、`symbol` 一致由 `validate_execution_target`
承担；差分转换由 `convert_target_to_delta_order` 承担；库存来源在 paper 模式固定为 paper
持仓账，不读 READ_ONLY 持仓，二者不得混用（T-03）；永不从零开卖单由目标域与库存
双重非负保证并在执行器显式复核；意图账本行携带三项血缘字段。

## 4. 状态机与账本

| 变更 | 内容 |
|---|---|
| 新增状态 | `PAPER_FILLED`、`PAPER_REJECTED`，均为终态且属本地终态集合 `LOCAL_TERMINAL_STATES` |
| 新增迁移 | `SENDING -> PAPER_FILLED`（携带成交模型证据）、`SENDING -> PAPER_REJECTED`（记录拒绝理由） |
| 账本方法 | `IntentLedger.paper_fill`、`IntentLedger.paper_reject` |
| 报告口径 | dry-run 执行器、对账会话、浸泡运行的 `write_touched` 判定统一改用 `LOCAL_TERMINAL_STATES` |
| 账目位置 | paper 意图账本、持仓账、差异账与费率缓存同置于 `data/execution/paper/`，与 dry-run 及 shadow 账本分离 |

## 5. 成交模型与差异账

成交模型（B2）输入为发送时刻盘口快照、意图限价（作参考价）与 taker 费率；买入自最优卖价向上、卖出自最优买价向下逐档吃单，深度不足时拒绝而非部分成交。成本分解均为基点、不利方向为正：

| 字段 | 定义 |
|---|---|
| `fee_bps` | taker 费率；来源为 `GET /v1/symbols` 的 `takerFee`（`public_symbols_taker_fee`）、本地缓存（`public_symbols_taker_fee_cached`）或配置降级值（`config_fallback`，附降级原因） |
| `half_spread_bps` | 最优买卖价差的一半相对中间价 |
| `impact_bps` | 成交均价相对触价的不利偏移 |
| `slippage_vs_reference_bps` | 成交均价相对参考价的不利偏移 |
| `total_cost_bps` | `fee_bps + slippage_vs_reference_bps` |

阶段一盘口来源为公开端点 `GET /v1/orderbooks` 快照，产物标注 `fill_basis: public_orderbook_snapshot`；同一进程内决策参考价（中间价）与发送时刻盘口共用一次快照。差异账每行包含：`prediction_id`、`decision_time`、`valid_until`、`correlation_id`、`exposure_target`、`risk_budget_jpy`、`target_notional_jpy`、`reference_price`、`position_before`、`position_after`、`status`、`delta`（目标数量、库存、差分、委托参数与跳过理由）、`intent`（意图号、方向、数量、价格、终态、理由）、`fill`、`cost`、`fee`、`overlay`、`service_status`、`endpoints`。

## 6. 覆盖层门控记录

阶段一只记录不改目标：`overlay{applied: false, would_apply, complete, value, multiplier, top_imbalance, limit, gates[]}`。门控列表固定为质量窗 `eligible`、服务状态、REST 锚点年龄、最优价差基点、前五档挂量；乘子候选为顶档不平衡乘以 `limit`（缺省 0.3），夹在正负 `limit` 之内。任一门控输入不可得时该门控标注 `unavailable`、`passed` 为空，`complete` 与 `would_apply` 均不成立。REST 锚点年龄在阶段一只接受命令行显式给定，缺省标注不可得。

## 7. 运行方式

```text
python scripts/adapt_frozen_target.py --prediction <预测> --output-directory <目录> --config config/paper_executor.json --mode paper
python scripts/run_paper_executor.py --target <目标快照> --config config/paper_executor.json [--rules <取引ルール快照>] [--book <盘口快照>] [--service-status OPEN] [--anchor-age-seconds N] [--ledger-root <账目根>] [--now <ISO 时刻>]
python scripts/summarize_paper_ledger.py --config config/paper_executor.json [--ledger-root <账目根>]
```

缺省经公开只读端点拉取取引ルール、盘口与服务状态，报告列明 `read_touched`（A-03）；测试一律以文件夹具离线运行。进程返回零当且仅当无意图、同预测去重或意图终态为 `PAPER_FILLED`、`PAPER_REJECTED`；`write_touched` 非空返回 2。

## 8. 验证

| 项 | 覆盖 |
|---|---|
| `tests/test_frozen_target_adapter.py` | 第 2 版字段、内容寻址幂等、有效期派生与继承、负值与越界拒绝、质量失败、非法有效期、预算硬顶、模式、目标域不一致、命令行预算来自配置 |
| `tests/test_paper_fill_model.py` | 逐档吃单均价与成本分解、深度不足拒绝、盘口交叉与乱序拒绝、夹具装载、费率拉取、缓存、过期与降级标注 |
| `tests/test_paper_executor.py` | 从零买入并记录三账、降目标卖出不低于库存、零目标零库存不开卖单、同预测去重、越期与未生效拒绝、市场、品种、模式与预算不符拒绝、目标装载拒绝越界与第 1 版、费率降级标注、覆盖层门控与不可得标注、维护状态闸门拒绝、持仓账不连续拒绝、命令行离线端到端与按日汇总、`write_touched` 为空 |
| 类型 | 新增与修改模块通过 `mypy --strict` |

## 9. 已知限制与阶段二待办

| 序 | 项 | 说明 |
|---|---|---|
| 1 | L2 活动 head 直读 | 阶段一盘口来自公开端点快照；阶段二改为按主仓控制面活动 `book_l2` head 读取 Parquet（只读、不枚举目录），产物标注新的 `fill_basis` |
| 2 | REST 锚点年龄 | 阶段一只接受显式参数；阶段二自锚点观测物化读取 |
| 3 | 限价排队 | 阶段一按 taker 口径估算，不模拟限价排队与部分成交 |
| 4 | 覆盖层生效 | 阶段一只记录判定；是否施加乘子、阈值取值与原因码表由契约提案的未决项裁定 |
| 5 | 调度接线 | paper 执行器尚未接入每小时 shadow 任务；接入前须确认 paper 账目目录与 shadow 账目分离 |
| 6 | 费率缓存 | 缓存文件按品种保存并带时效；缓存读取失败静默退回拉取或降级，仅在差异账标注来源 |
