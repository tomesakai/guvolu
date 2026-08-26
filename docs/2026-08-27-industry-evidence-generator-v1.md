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
