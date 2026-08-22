# 2026-08-22 决策生成 I/O 契约 v2 提案

> 时效快照（W-02）：内容冻结于 2026-08-22，修订以新快照发布。
> 本文是 TBD-39 的提案载体（A-05）：把冻结前向预测的输入、输出与执行目标拆为
> 决策输入、决策记录、执行目标三份显式契约。提案确认前，现行 `frozen-forward-v1`
> 预测与执行仓适配器契约继续有效；现行 vintage 与冻结计划不受本文溯及。
> 本文局部用语：目标域指目标值的取值区间、参照基准与方向语义；缺预测处置指
> 预测窗口内无预测时该决策柱暴露的预登记处置规则。
> 本次同步登记：TBD-39 入 [architecture.md](architecture.md) 第 2 节与第 7 节；
> 术语决策输入、决策记录、执行目标入 [SKILLS.md](../SKILLS.md) 第 7 章。

## 1. 现状事实

以下事实以 main 分支与执行仓（`codex/execution-chain` 工作树）代码为据，路径已核实：

| 序 | 事实 | 位置 |
|---|---|---|
| 1 | 预测由 `run_frozen_forward_prediction` 生成，`method_version` 为 `frozen-forward-v1`；字段为 `schema_version`、`method_version`、`scope`、`prediction_id`、`plan_id`、`vintage_id`、`decision_time`、`input_head_generation`、`panel_sha256`、`config_hash`、`code_identity{git_hash, tree_digest, decision_grade}`、`quality{integrity, freshness, clock, coverage, pit, lineage, eligible, reasons}`、`families[]{family, candidate_id, family_target, frozen_allocation_weight, portfolio_target_contribution}`、`reserve`、`aggregate_target`、`unit = risk_weighted_directional_target` | [frozen_forward.py](../src/guvolu/research/frozen_forward.py) |
| 2 | `prediction_id` 是 `(governance_method_version, plan_id, decision_time)` 的身份散列；内容散列是预测文件的 SHA-256，登记于治理注册表 | 同上 |
| 3 | 3,900 秒登记时效来自 `strategy_decision_max_age_seconds`；预测本身没有有效期字段 | [strategy_research.json](../config/strategy_research.json) |
| 4 | 研究侧目标值域为 `[0, maximum_target]` 纯多头：状态机 sizing 由波动率缩放并以 `maximum_target` 为上限；表达式 sizing 以零为下限、`maximum_target` 为上限 | [baselines.py](../src/guvolu/strategy/baselines.py)、[expression.py](../src/guvolu/strategy/expression.py) |
| 5 | 执行仓目标适配器接受 `abs(aggregate_target) <= 1`，不校验符号 | 执行仓 `src/guvolu/execution/frozen_target_adapter.py` |
| 6 | 执行仓转换闸门 `convert_target_to_order` 的目标域为 `[-1, 1]`，负目标映射为 `SELL` 并从零开仓 | 执行仓 `src/guvolu/execution/conversion.py` |
| 7 | dry-run 执行器只消费 `operational_target_contract.aggregate_target`，使用一次性从零折算；差分入口 `convert_target_to_delta_order` 已存在且仅由对账会话使用，冻结前向 shadow 路径未使用 | 执行仓 `src/guvolu/execution/dry_run_executor.py`、`reconcile_session.py`；[run_frozen_shadow.py](../scripts/run_frozen_shadow.py) |
| 8 | 意图账本行只有 `intent_id` 与 `correlation_id`，没有 `prediction_id` 与 `decision_time` 回链；`correlation_id` 在每个意图生成时随机产生 | 执行仓 `src/guvolu/data/intent_ledger.py`、`src/guvolu/domain/ids.py` |
| 9 | 预测不含 L2 overlay、市场状态与分配诊断；分配器结果只有权重、储备、目标函数值、regime 与迭代数，没有 `reasons` | [allocator.py](../src/guvolu/research/allocator.py) |
| 10 | holdout 先原子消费 vintage，再逐柱校验前向预测覆盖；任一候选任一柱缺预测即抛错，vintage 已消费而无法重跑 | [holdout.py](../src/guvolu/research/holdout.py) |
| 11 | 名义预算来自 dry-run 执行器命令行参数 `--budget-jpy`，缺省 500 JPY；shadow 调度脚本沿用同一缺省 | 执行仓 `dry_run_executor.py`；[run_frozen_shadow.py](../scripts/run_frozen_shadow.py) |

由此形成四处缺口：研究侧 `[0, maximum_target]` 与执行侧 `[-1, 1]` 两个目标域并存；
有效期只以登记时效隐含，不随预测制品传递；血缘在意图账本处断链；预算不来自
版本化配置。

## 2. 三份契约

### 2.1 决策输入（DecisionInput）

每根决策柱冻结一份，内容寻址身份为 `decision_input_sha256`（规范 JSON 的 SHA-256）；
候选按引用携带（`plan_id` 与 `candidate_set_hash`），不内嵌候选全文。

| 分组 | 字段 | 说明 |
|---|---|---|
| 身份 | `plan_id`、`vintage_id`、`market_id`、`bar_interval`、`decision_time`、`correlation_id` | `correlation_id` 在此生成，贯穿决策记录、执行目标与意图账本（D-05） |
| 输入收据 | `input_receipt{head_generation, attempt_ids, artifact_ids, normalization_versions, files[]{path, sha256}}` | 活动 head 冻结后的输入集合，与 `FrozenPanelInputs` 同源 |
| 面板 | `panel{sha256, last_open_time, latest_available_time}` | `latest_available_time <= decision_time` 即 PIT 证据（D-04） |
| 特征 | `features{method_version, snapshot_sha256}` | 特征快照身份 |
| 质量 | `quality{integrity, freshness, clock, coverage, pit, lineage, eligible, reasons[]}` | 质量向量六维旗标与派生 `eligible` 共七个布尔位；`reasons` 为编码原因 |
| 成熟度 | `maturity{contiguous_bars, required, mature}` | 现行 `_maturity_gate` 判定的显式化 |
| 时钟 | `clock{now, max_age, lag}` | `lag = now - decision_time`；`max_age` 取自配置 |
| 调节输入 | `l2_overlay{eligible, basis, value, source_heads, age}`、`market_state{regime, uncertainty}`、`venue_status` | 现行 `l2_overlay_from_shadow` 证据的扩展；市场状态取自 `MarketState` |
| 身份证据 | `code_identity{git_hash, tree_digest, dirty_digest, decision_grade}`、`config_hash` | 与 `CodeIdentity` 同源 |

### 2.2 决策记录（DecisionRecord）

决策记录是预测 v2：保留第 1 节序 1 的全部字段与语义，只增字段、不改语义
（D-06），`schema_version` 升为 2。

| 新增字段 | 取值 | 说明 |
|---|---|---|
| `valid_from` | `decision_time` | 有效期起点 |
| `valid_until` | `decision_time + bar_interval` | 有效期终点；执行器不得消费越期记录 |
| `decision_input_sha256` | 决策输入内容散列 | 输入到输出的唯一回链 |
| `correlation_id` | 继承自决策输入 | 贯穿全链路 |
| `target_semantics` | `{domain: long_only_spot, range: [0, 1], reference: fraction_of_risk_budget, short_allowed: false}` | 目标域声明，消费方必须校验 |
| `exposure_target` | `aggregate_target` 截取到 `[0, 1]` | 执行侧唯一消费的目标值 |
| `raw_targets[]` | 门禁前逐候选目标 | 诊断 |
| `effective_targets[]` | 门禁后逐候选目标 | 与 `families[].family_target` 一致 |
| `overlay` | `{applied, value, multiplier, limit}` | L2 overlay 是否施加及其幅度 |
| `gates[]` | `{name, passed, value, threshold, reason_code}` | 逐门禁结果，阈值来自配置（G-06） |
| `degradation` | `{level, reasons[]}` | `level` 取 `none`、`reduced`、`flat` 之一；`reasons` 为编码原因 |

计划级预登记项 `missing_policy`：预测窗口内无预测时，该柱 `exposure_target` 记为 0，
holdout 覆盖校验据此视为完整。该项在 vintage 开始前随冻结计划登记，不得事后增补。

### 2.3 执行目标（ExecutionTarget）

适配器由决策记录生成，只供执行域消费：

| 字段 | 说明 |
|---|---|
| `correlation_id`、`prediction_id`、`decision_time`、`valid_until` | 自决策记录继承 |
| `market_id`、`symbol` | 执行器必须校验二者与自身配置一致 |
| `exposure_target` | 取值 `[0, 1]`；负值与越界在适配器处拒绝 |
| `risk_budget_jpy` | 来自版本化配置，不来自命令行缺省 |
| `mode` | `dry-run`、`paper` 或 `live` |
| `lineage` | 决策记录路径与 SHA-256、`decision_input_sha256` |

执行器义务：消费前校验 `valid_until` 未过且 `market_id`、`symbol` 一致；以差分转换
生成意图（`convert_target_to_delta_order`），库存来自 READ_ONLY 持仓事实或由其
成交事实推算的持仓账（T-03）；永不从零开出卖单；意图账本行携带 `correlation_id`、`prediction_id` 与
`decision_time`（R-07、X-08）。

## 3. 不变量

1. PIT：决策输入只含 `available_time <= decision_time` 的事实（D-04）。
2. 身份散列与内容散列分离：`prediction_id` 由 `(governance_method_version, plan_id, decision_time)` 决定；内容散列为文件 SHA-256；决策输入另有 `decision_input_sha256`。
3. 目标域单一：全链路只有 `long_only_spot`、`[0, 1]`、`fraction_of_risk_budget` 一个目标域；负值在适配器处拒绝，不在下游解释。
4. 有效期显式：`valid_from`、`valid_until` 随记录传递，执行器不得消费越期记录。
5. 原因码有编码与分级：`quality.reasons`、`gates[].reason_code`、`degradation.reasons` 均为编码值；与 [错误码对照与处置册](error-catalog.md) 的对齐方式列为待办（第 5 节）。
6. 缺省预测为零暴露：缺失、越期、质量失败或代码身份失败均折算为 `exposure_target = 0`。
7. 三域互不回写：研究与 GPU 产候选与证据，冻结计划产预测，执行域产意图（D-01、G-02、T-13）。

## 4. 与现行契约的差异与迁移

| 项 | 现行（v1 与执行仓） | v2 提案 | 迁移动作 |
|---|---|---|---|
| 目标域 | 研究侧 `[0, maximum_target]`；适配器 `abs <= 1`；转换闸门 `[-1, 1]`，负值为 `SELL` | `target_semantics` 与 `exposure_target` 统一为 `[0, 1]` 纯多头 | 适配器增加目标域校验并拒绝负值；转换闸门不变，冻结前向路径只传非负值 |
| 有效期 | 登记时效 3,900 秒，预测无字段 | `valid_from`、`valid_until` | 只增字段；执行器增加越期校验 |
| 血缘 | 意图行无预测回链；`correlation_id` 每意图随机 | `correlation_id` 自决策输入生成并贯穿；意图行携带 `prediction_id`、`decision_time` | 意图账本 schema 升版（D-06） |
| 折算 | shadow 路径一次性从零折算 | 差分折算，库存来自 READ_ONLY | 执行器改用差分入口 |
| 预算 | 命令行缺省 500 JPY | `risk_budget_jpy` 来自配置 | 配置文件登记并在启动时校验不超过 T-11 硬顶 |
| 诊断 | 无 overlay、市场状态、门禁与降级字段；分配器无 `reasons` | `overlay`、`gates[]`、`degradation`、`raw_targets[]`、`effective_targets[]` | 只增字段 |
| 缺预测 | 覆盖缺口令已消费 vintage 永久失效 | 计划级 `missing_policy` 折算零暴露 | 新 vintage 计划封存前预登记；现行 vintage 不溯及 |

迁移顺序：新字段只增不改语义（D-06）；新 vintage 的冻结计划封存前预登记
`missing_policy`、`valid_until` 规则、overlay 函数与阈值、降级码表；适配器拒绝负值
与越期记录；执行器改差分转换并固定库存来源；意图账本升版携带回链。

## 5. 未决项

以下各项需人工裁定，本文不预设结论：

| 序 | 未决 | 说明 |
|---|---|---|
| 1 | `risk_budget_jpy` 的来源与调整权限 | 配置值受 T-11 硬顶约束，运行时只可调低（X-05）；由谁、经何处调整待定 |
| 2 | overlay 首批门槛变量 | 候选变量：质量窗 `eligible`、REST 锚点年龄、熔断状态、最优买卖价差（bp）、前五档挂量；乘子以盘口不平衡为基础，幅度限 ±0.3（现行 `l2_overlay_limit`）；变量集与阈值按 G-06 登记为配置 |
| 3 | 原因码表归属 | 并入 [错误码对照与处置册](error-catalog.md)，或另设研究与执行共用的原因码表 |
