# 经济研究代理 v1（2026-08-26）

## 结论

本版交付一个最小但完整、可重放、可审计的 research-only 经济研究代理。它只能把已落盘的经济观测转换为时点正确的经济语境，再将有证据、受配额约束的 `ResearchProposal` 转换为供 `SearchPlan` 人工审查的 proposal-only 制品。

它不联网、不读取密钥、不导入交易执行包，不修改配置、候选注册表、研究真实源或促销状态。它不能产生订单，也不构成实盘资格结论。

## 权限边界

每个语境、提案和运行回执制品都显式声明以下权限为 `false`：

- network；
- secrets；
- trade / execution；
- config mutation；
- registry mutation；
- promotion。

提案制品中的 `search_plan_interface.contract` 固定为 `proposal_only`，并强制 `requires_explicit_review=true` 与 `requires_candidate_registration=true`。后续接入 typed AST / Candidate Registry / SearchPlan 时，消费方必须单独实现人工审查和预登记，不得将提案制品视为已注册候选。

## 数据合同

观测以 canonical JSONL 追加。每行包含连续 `sequence`、`previous_record_sha256` 和 `record_sha256`；读取时必须全链校验。业务字段至少为：

- `observation_id`：规范观测全内容的稳定散列标识；
- `series_id` / `value` / `unit`；
- `event_time`：经济事实所属时间；
- `available_time`：数据在源端可知时间；
- `ingest_time`：本系统持久化时间；
- `revision_id` / `supersedes_revision_id`；
- `source_receipt.source_id` / `source_receipt.receipt_sha256`，以及可选的非密钥 `locator`。

来源事实时钟必须满足：

```text
event_time <= available_time
```

PIT 回放严格只用 `available_time <= decision_time` 判定当时可合法使用的事实。`ingest_time` 仅记录本地下载/持久化时刻；历史回补时它可以晚于 `decision_time`，embargo 预载时也可以早于 `available_time`。它不得替代 `available_time` 承担防未来职责（D-03/D-04）。同一 `(series_id, event_time)` 的修订必须精确指向前一 `revision_id`，只要求 `available_time` 严格递增；修订排序不使用 `ingest_time`。重复 ID、修订分叉、非有限数值、无时区时间或来源时钟倒置会使整个输入批次在落盘前失败。

`source_receipt` 只允许上述三个字段，用于避免将 token、cookie 或原始响应意外写入研究台账。代理只校验回执散列的形式；原始回执文件由上游内容寻址存储和数据质量流程负责。

## PIT 语境

策略文件登记每个序列的：

- 所属维度：`growth` / `inflation` / `rates` / `liquidity` / `fx` / `risk`；
- 单位；
- 中性值、尺度、方向与权重；
- `max_age_seconds` 新鲜度上限。

回放在 `decision_time` 之前选择每个统计期的最新可知修订，再为每个序列选择最新统计期。过期序列不进入维度得分。每个维度都输出：

- `data_status`: `fresh` / `partial` / `stale` / `missing`；
- 截断到 `[-3, 3]` 后的确定性加权得分；
- 维度特定 regime，无新鲜数据时固定为 `unknown`；
- 选中观测、修订、时钟、年龄和标准化得分。

`EconomicContextArtifact` 绑定政策散列、`decision_time`、所有 as-of 合格观测 ID、输入散列，以及生成时观测台账的 `sequence/head_sha256`。`verify_economic_context` 会在共享路径锁内取出已绑定台账前缀，重建完整 context 并逐字节比对。后续追加不会使已有 context 失效；但从更新后的完整台账生成新 context 时，新的链头会有新制品 ID。

## ResearchProposal 门禁

输入提案必须精确包含：

```json
{
  "hypothesis": "可证伪的经济—策略假设",
  "evidence_ids": ["economic-observation-..."],
  "family": "trend",
  "template": "macro_regime_filter",
  "parameter_bounds": {
    "lookback": {"minimum": 24, "maximum": 72, "step": 24}
  },
  "regimes": ["growth:strong"],
  "horizon": {"unit": "hours", "minimum": 24, "maximum": 168},
  "falsification": "walk-forward OOS 成本后结果不显著则证伪",
  "trial_budget": 8
}
```

门禁会检查：

- evidence 必须是当前语境内的新鲜观测；
- 提案 regime 必须与当前语境一致；
- family/template 和参数名必须在政策白名单；
- 参数数量、预测期、单提案 trial budget、批次提案数与总 trial budget 均不得超额；
- 与历史已接受提案或当前批次提案重复时拒绝；
- 已到 `holdout_start_time`、语境跨过 holdout 边界，或证据在边界后才可知时拒绝；
- 只要配置了 holdout 边界，当前版本就以 `holdout_governance_unbound` 拒绝全部提案，不允许本地文件或提案输入自行证明已绑定治理证据。

配额选择不依赖输入顺序：通过合同校验的提案按内容寻址 `proposal_id` 排序后再分配配额。所有被拒提案仍进入运行台账。

提案接受时间由模块内部 `research.clock.utc_now()` 生成，CLI 不接受可回填时间。无 holdout 的开发研究可以通过其余门禁，但全部制品仍显式记录 `holdout_governance_bound=false`。

治理库绑定是阻断型遗留接口。未来版本必须直接查询 governance SQLite 中已封存的 holdout vintage，并将不可变记录身份绑定到运行输入；在该查询、封存状态校验和可信时钟校验完成前，本模块不会产生 `holdout_governance_bound=true`，也没有 receipt 参数可以绕过这一限制。

## 身份与台账

代理默认不使用 LLM，并记录：

- `provider=none`；
- `model_id=deterministic-rules-v1`；
- 空 prompt / model input / model output 的 SHA-256。

如果上游在隔离流程中使用 LLM 生成结构化提案，可通过 `--inference-identity` 提供 `provider`、`model_id`、模型参数散列、prompt 模板与散列、model input/output 散列。本模块只记录并绑定这些身份，仍不会自行联网或调用模型。

每次运行产生内容寻址 run receipt，并在哈希链 JSONL 追加一条记录。记录绑定 model / prompt / input / output 身份、全部提案尝试、接受制品 ID、拒绝原因与 trial budget。

## CLI

入口为 `scripts/run_economic_research_agent.py`。以下命令都是显式的 research-only 本地文件操作：

```powershell
.\.venv\Scripts\python.exe scripts\run_economic_research_agent.py ingest `
  --input data\research\economic\incoming.jsonl

.\.venv\Scripts\python.exe scripts\run_economic_research_agent.py context `
  --policy config\economic-agent-policy.json `
  --decision-time 2026-05-03T00:00:00Z

.\.venv\Scripts\python.exe scripts\run_economic_research_agent.py propose `
  --context reports\economic-research\contexts\economic-context-....json `
  --observation-ledger data\research\economic\observations.jsonl `
  --proposals data\research\economic\proposals.json `
  --policy config\economic-agent-policy.json

.\.venv\Scripts\python.exe scripts\run_economic_research_agent.py verify
```

`ingest` 输入可为 JSON 数组、单个 JSON 对象或 JSONL。`context` 只读观测台账并写内容寻址语境。`propose` 必须重新读取观测台账、验证 context 前缀与语义，再写 proposal-only 制品、run receipt 和追加式审计台账。CLI 没有接受时间或治理回执覆盖参数。`verify` 会全链校验两个台账。

## 与策略管线的遗留接口

本版到 `SearchPlan` 的边界刻意保持单向且未自动接通。后续消费器应完成：

1. 验证 proposal artifact 散列、源 context 散列和当前代理政策散列；
2. 把已审批的 `family/template/parameter_bounds` 编译为现有 typed AST；
3. 在 Candidate Registry 中预登记并生成新的 SearchPlan；
4. 把 `proposal_id` / `economic-context` / observation evidence IDs 写入 SearchPlan 与全试验台账的上游 lineage；
5. 依次经过 SearchFast、CPU f64 exact、embargo walk-forward、多重检验、邻域稳定、未来 holdout 和 paper 门禁。

在这些接口和治理证据完成前，经济代理的任何提案都只是待检验假设，不是策略，更不是上线许可。
