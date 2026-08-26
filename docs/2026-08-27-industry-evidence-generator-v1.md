# 行业稳健性证据生成器 v1（2026-08-27）

## 结论

本版交付一个只读上游、内容寻址、失败关闭的行业稳健性证据生成器。它把一次 decision-grade 研究运行的受完整性保护制品与活动 head L2 事实，转换为项目准入政策所要求但此前标注为 `not_implemented` 的四类证据制品，并按候选聚合为 `industry-evidence-v2` 汇总制品与独立生成器 attestation。

它只产证据。它不重选候选、不改研究配置、不写治理库、不做晋级、不解除任何准入 blocking，也不联网、不读取密钥、不导入交易执行包。生成结果中阈值未达的部分按事实登记，不得为通过而调参（G-06）。

## 输入与身份

生成器的唯一输入是一次研究运行的 `manifest.json` 及其登记的制品。入口先以 `verify_research_run` 完整语义复验运行，再按 manifest 记录的散列逐份读取字节快照；任一字节与散列不一致即中止。参与生成的上游制品为：

| 制品 | 用途 |
|---|---|
| `summary_json` | 合格部署候选、面板区间、基准消融 |
| `config` | 成本分量假设、折块长度与随机种子、bar 间隔 |
| `label_cost_replay` | 样本外逐柱目标、换手、行情对数收益与 fold 归属 |
| `features` | 决策时刻可见的成交量分位特征 |

每份生成的制品都记录 `run_id`、`research_identity`、`config_hash`、`input_receipt_sha256`、`panel_sha256`、`decision_time`、生成器标识与生成器源文件散列（D-09）。汇总制品的 `candidate_evidence` 逐场景引用来源制品的 `name / kind / path / sha256 / artifact_id / bytes`，attestation 记录汇总制品散列与被引用来源制品身份集合。

## 时点与封存边界

面板截止上限由 `data_governance.panel_to_time` 与 `--to-time` 解析，命令行只能更早（[panel_limit](../src/guvolu/research/panel_limit.py)）。打开样本外区段前执行封存段只读预检，区间与任何未消费封存段重叠即中止（G-08）。读取回放时，任何 `label_available_time` 晚于生效上限的样本外行都会使生成失败。

场景的 `registered_at` 取研究运行的 `decision_time`；`coverage.available_through` 取面板的 `latest_available_time` 或 L2 活动 head 的最大事件时点。检查器要求的时序为：

```text
coverage.from_time < coverage.to_time <= available_through <= registered_at <= decision_time <= execution_evaluated_at
```

汇总制品的 `generated_at` 必须落在 `[decision_time, execution_evaluated_at]` 内。这意味着证据必须在研究运行自身的执行窗口内产生；对已经封版的历史运行事后补生成，检查器会给出 `INDUSTRY_EVIDENCE_GENERATION_CUTOFF_INVALID`。生成报告以 `generation_within_registration_window` 字段如实记录该判定，不做时间戳改写。

## 四类证据的构造规则

### 成本情景（`fixed-target-cost-sensitivity-v1`）

对固定部署候选的样本外目标路径按成本档重放，不重选候选。档位由 `config/industry_evidence.json` 的 `cost.tier_multipliers` 对研究配置的基准分量整体缩放得到：

```text
total_cost_bps(tier) = multiplier(tier) * (fee + half_spread + slippage + impact)
```

基准档倍数为 1.0，因而 `policy_baseline` 恒等于研究运行自身的成本假设合计。生成器在写出前强制档位严格递增且相邻差不小于 `cost.minimum_step_bps`，否则直接抛错。每档给出 Sharpe、净对数收益、最大回撤、换手与四项成本分量，分量之和等于该档总成本。

### 尾部情景（`walk-forward-tail-v1`）

以基准成本下的样本外逐柱净收益为总体，按研究配置的 `block_bootstrap_bars` 做循环折块重采样，样本数与随机种子来自版本化配置与研究配置，种子固定因而逐位可复现（G-03）。对每个政策要求的概率 p：

- Sharpe 与净收益取重采样分布的 p 分位；
- 最大回撤与换手取 1−p 分位；
- 期望短缺取净收益最差 `ceil(p × 样本数)` 条重采样路径的均值。

期望短缺在写入场景时按检查器值域裁剪到 `[-1, 0]`，来源制品同时保留未裁剪的 `expected_shortfall_raw`。

### 压力情景（`walk-forward-stress-v1`）

三类定义各自给出一条按分位阈值选出的确定性子区间规则，并在子区间上以基准成本重算指标。`severity` 定义为全样本外柱数除以子区间柱数。

| 定义 | 统计量 | 选择 |
|---|---|---|
| `volatility_spike` | 决策时刻可见的滞后已实现波动 | 统计量不小于配置分位 |
| `liquidity_gap` | 受保护特征面板的 PIT 成交量分位 | 统计量不大于配置分位 |
| `cross_venue_dislocation` | 决策对齐的跨所中价价差 | 统计量不小于配置分位 |

统计量全部只使用决策时刻已可见的数据（D-04）。子区间柱数低于 `stress.minimum_subinterval_bars` 时标注 `insufficient_subinterval_bars` 且不给出指标。当前研究运行的输入身份中不含决策对齐的跨所价格序列，`cross_venue_dislocation` 因此标注 `insufficient_cross_venue_coverage` 并不展开为场景。

### 容量情景（`l2-depth-capacity-v1`）

在自家三所的 L2 活动 head 上按固定步长回溯采样解析盘口状态，只读。对每个名义规模：

- 可用深度取各采样点买卖两侧名义深度较小者的配置分位；
- 参与率为名义规模除以可用深度；
- 冲击 bps 由按名义规模逐档吃单的加权成交价与中价之差复算。

场景指标为在基准成本上叠加该规模冲击后重算的净成本指标。三所中任一来源缺少活动 head、解析成功的采样点少于 `capacity.minimum_depth_samples`、或盘口深度不足以成交该名义规模时，一律标注 `insufficient_l2_coverage` 并不给出指标，绝不外推。执行边界为 GMO，参与率与冲击只用 GMO 的深度事实，其余两所的事实随制品留存作为对照。

## 失败关闭边界

以下情形一律不产出可用场景，而不是给出近似值：

- 成本档位不严格递增或相邻差低于配置步长；
- 尾部概率网格重复或非递增；
- 压力子区间柱数不足，或跨所序列缺失；
- L2 活动 head 缺失、采样不足或盘口深度不足；
- 样本外区段越过面板截止上限，或面板区间触及未消费封存段；
- 上游制品字节与 manifest 散列不一致。

## 台账

每次生成写出一份 `industry-evidence-ledger-v1` JSONL 台账：一行表头记录运行身份与生成时刻，其后逐场景登记 `scenario_id`、`scenario_key`、方法版本、候选身份、参数与指标（G-07）。台账与制品同为内容寻址文件名，不覆盖既有内容。

## 政策键 `industry_evidence_generator_status`

准入检查器 v4 的政策合同只接受该键取值 `not_implemented`，并同时要求 `accepted_generator_attestation_method_versions` 与 `allowed_industry_evidence_generators` 为空；任何其他取值会使政策数值域校验失败，产生 `ADMISSION_POLICY_DOMAIN_INVALID` 与 `ADMISSION_POLICY_NOT_APPROVED`，从而使全部下游门禁不再评估。检查器另以文件散列与规范内容散列绑定唯一获批政策文件。

因此本版**不修改** `config/industry_strategy_readiness.json`。把该键改为 `implemented` 属于准入政策版本升级（v4 至 v5）与获批散列重签，需要同时登记生成器三元组与 attestation 方法版本，并放宽一条失败关闭门禁；这超出「只产证据」的范围，须单独评审。生成器实现状态在本快照与生成报告中记录，不写入受绑定的政策文件。

## 本次生成结果

对 `research-run-14c57fe70bc97725b32155f8a78c0b4e85902382c1bddcf22d417b5d28d2ad4c`（代码身份 356b45e，`config_hash` 66590f5b，family_scope `price_breakout` 与 `trend`）生成。样本外区段 56160 柱、26 折，覆盖 2020-01-06 至 2026-06-22；基准消融 `fixed_long` 的 Sharpe 为 0.6519。

成本情景，档位 10 / 15 / 20 bps：

| 候选 | 档位 | Sharpe | 净收益 | 最大回撤 | 换手 |
|---|---|---|---|---|---|
| price_breakout | policy_baseline | 1.2601 | 1.9213 | 0.2679 | 238.20 |
| price_breakout | adverse | 1.1816 | 1.8022 | 0.2782 | 238.20 |
| price_breakout | severe | 1.1031 | 1.6831 | 0.2884 | 238.20 |
| trend | policy_baseline | 0.9698 | 1.6453 | 0.4315 | 335.91 |
| trend | adverse | 0.8705 | 1.4774 | 0.4509 | 335.91 |
| trend | severe | 0.7712 | 1.3094 | 0.4697 | 335.91 |

基准档逐位复现研究 summary 的 `deployment_oos_metrics`。`price_breakout` 三档全部达阈；`trend` 三档换手 335.91 超过政策上限 250.0，`adverse` 与 `severe` 的回撤亦超过 0.45。

尾部情景，折块长度 168、重采样 512 条、种子 20260814：

| 候选 | p | Sharpe | 净收益 | 最大回撤 | 换手 | 期望短缺 |
|---|---|---|---|---|---|---|
| price_breakout | 0.01 | 0.2519 | 0.3718 | 0.4851 | 269.66 | 0（未裁剪 +0.2387） |
| price_breakout | 0.025 | 0.4536 | 0.6420 | 0.4614 | 265.00 | 0（未裁剪 +0.3656） |
| price_breakout | 0.05 | 0.5217 | 0.7720 | 0.4231 | 260.12 | 0（未裁剪 +0.5350） |
| trend | 0.01 | -0.0149 | -0.0250 | 0.6139 | 379.55 | -0.2205 |
| trend | 0.025 | 0.1528 | 0.2465 | 0.5513 | 374.36 | -0.0131 |
| trend | 0.05 | 0.2900 | 0.4718 | 0.4956 | 369.16 | +0.1686 |

两个候选的尾部换手均超过政策上限 250.0，回撤在低概率端超过 0.45；`trend` 在 p=0.01 的期望短缺 -0.2205 低于政策下限 -0.2。六条尾部场景全部未达阈。

压力情景，波动分位 0.9、成交量分位 0.1、子区间下限 240 柱：

| 候选 | 定义 | 子区间柱数 | severity | Sharpe | 净收益 | 最大回撤 | 换手 |
|---|---|---|---|---|---|---|---|
| price_breakout | liquidity_gap | 5583 | 10.06 | 3.8477 | 0.3652 | 0.0679 | 0.00 |
| price_breakout | volatility_spike | 5614 | 10.00 | -0.0616 | -0.0114 | 0.1662 | 15.60 |
| trend | liquidity_gap | 5583 | 10.06 | 4.5364 | 0.4750 | 0.0356 | 21.36 |
| trend | volatility_spike | 5614 | 10.00 | -0.2901 | -0.0589 | 0.1633 | 14.98 |

`cross_venue_dislocation` 标注 `insufficient_cross_venue_coverage`。`liquidity_gap` 两个候选均达阈，`volatility_spike` 两个候选的 Sharpe 与净收益均为负，未达阈。

容量情景，GMO 活动 head `sha256-ebcb939d`、23,699,964 条 L2 观测、24 个小时级采样点全部解析成功：

| 名义规模 JPY | 可用深度 JPY | 参与率 | 冲击 bps |
|---|---|---|---|
| 100,000 | 54,646,406 | 0.00183 | 0.000494 |
| 200,000 | 54,646,406 | 0.00366 | 0.000494 |
| 400,000 | 54,646,406 | 0.00732 | 0.000494 |

单侧深度在采样点上的最小、中位与最大为 11.26 / 54.65 / 107.35 百万 JPY，冲击的最小、中位与最大为 0.000494 / 0.000494 / 3.3699 bps。三所活动 head 覆盖率均为 1.0。六条容量场景全部达阈。

原因码对照，仅列稳健性门禁：

| 原因码 | 生成前 | 生成后 |
|---|---|---|
| INDUSTRY_EVIDENCE_ARTIFACT_MISSING | 报出 | 消除 |
| INDUSTRY_EVIDENCE_CANDIDATE_COVERAGE_INCOMPLETE | 报出 | 消除 |
| CAPACITY_SCENARIO_EVIDENCE_INCOMPLETE | 报出 | 消除 |
| CAPACITY_SCENARIO_NOTIONAL_GRID_INSUFFICIENT | 报出 | 消除 |
| COST_SCENARIO_COST_GRID_INSUFFICIENT | 报出 | 仍报出（trend 未达阈） |
| COST_SCENARIO_COST_GRID_NOT_STRICTLY_INCREASING | 报出 | 仍报出（trend 未达阈） |
| COST_SCENARIO_REQUIRED_TIERS_INCOMPLETE | 报出 | 仍报出（trend 未达阈） |
| COST_SCENARIO_POLICY_BASELINE_MISSING | 报出 | 仍报出（trend 未达阈） |
| COST_SCENARIO_EVIDENCE_INCOMPLETE | 报出 | 仍报出（trend 未达阈） |
| TAIL_RISK_SCENARIO_PROBABILITY_GRID_INCOMPLETE | 报出 | 仍报出（尾部未达阈） |
| TAIL_RISK_SCENARIO_EVIDENCE_INCOMPLETE | 报出 | 仍报出（尾部未达阈） |
| STRESS_SCENARIO_DEFINITION_SET_INCOMPLETE | 报出 | 仍报出（跨所序列缺失） |
| STRESS_SCENARIO_EVIDENCE_INCOMPLETE | 报出 | 仍报出（跨所序列缺失） |
| COST_SCENARIO_OUTCOME_BELOW_POLICY | 未报出 | 新增报出 |
| TAIL_RISK_SCENARIO_OUTCOME_BELOW_POLICY | 未报出 | 新增报出 |
| TAIL_RISK_SCENARIO_EXPECTED_SHORTFALL_BELOW_POLICY | 未报出 | 新增报出 |
| STRESS_SCENARIO_OUTCOME_BELOW_POLICY | 未报出 | 新增报出 |
| INDUSTRY_EVIDENCE_GENERATION_CUTOFF_INVALID | 未报出 | 新增报出（事后补生成） |
| INDUSTRY_EVIDENCE_GENERATOR_CUTOFF_INVALID | 未报出 | 新增报出（事后补生成） |

「新增报出」不是回退：生成前这些原因码之所以不出现，只是因为没有任何场景可供评估。准入结论在生成前后同为 `NOT_READY`。

## 残余限制

- 跨所错位压力缺少决策对齐的多所价格序列。bitbank 现物成交覆盖 2017 年至今，但未物化为研究面板并登记进运行输入身份；由证据生成器临时构建会产生不可追溯输入，因此本版失败关闭。补齐需在研究管线侧登记跨所面板。
- 事后补生成必然违反 `[decision_time, execution_evaluated_at]` 生成窗口。要使证据可被接受，生成器必须由研究管线在同一次运行内调用。
- 容量冲击取采样点中位数，且以中价为基准，与基准成本中 2 bps 的半价差假设存在轻微重复计入；本次实测冲击中位 0.000494 bps，数值影响可忽略，但保守口径应取上尾分位（本次最大 3.3699 bps）。
- L2 活动 head 仅覆盖 2026-08-11 至 2026-08-22，容量证据只反映该窗口的盘口状态，不代表样本外全区间的可执行容量。
- 检查器不复算场景数值，只校验结构、身份与阈值；`scenario_source_replay_status` 仍为 `not_implemented`，独立数值重放仍是显式信任边界。

## 运行命令

```bash
python scripts/generate_industry_evidence.py \
  --source-summary reports/strategy-research/<run-id>/summary.json \
  --config config/industry_evidence.json \
  --data-root data \
  --to-time 2026-08-23T09:00:00+00:00 \
  --output-dir reports/strategy-research/industry-evidence/<run-id>
```

`--root` 指定只读上游根，`--evidence-root` 指定证据写入根，两者可分离以便在独立工作树内生成。`--to-time` 缺省取研究面板的 `to_time`，且不得晚于配置上限或触及封存段。
