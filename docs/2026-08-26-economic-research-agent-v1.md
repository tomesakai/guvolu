# 经济研究代理 v1（2026-08-26）

## 结论

本版交付一个可重放、可审计且失败关闭的 research-only 经济研究代理。它把已落盘的经济观测转换为时点正确的经济语境，并审计结构化 `ResearchProposal`。由于 market 与治理库中的 sealed holdout vintage 尚未直接绑定，v1 会拒绝全部提案，只产生已提交的拒绝回执；它不会产生可供 `SearchPlan` 消费的 accepted proposal。

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

观测以 canonical JSONL 逻辑追加。每行包含连续 `sequence`、`previous_record_sha256`、`record_sha256` 和登记的规范绝对台账路径；读取时必须按精确 schema 全链校验。批次固定并排他打开 canonical ledger inode，记录原字节长度后原位追加，以完整写循环、长度检查和文件 `fsync` 提交。运行时短写或任何提交前检查失败都会在仍持有同一 inode 句柄时恢复旧字节、截回原长度并再次 `fsync`；首次建账失败则删除 canonical 空文件。

进程或断电发生在最终 `fsync` 前时可能留下不以换行结尾的尾部，loader 会将其视为未完成行并整本失败关闭，而不会接受半行 commitment。崩溃也可能发生在完整行已写入并完成文件 `fsync`、但尚未越过最终路径/目录提交检查的窗口；重启后的 loader 可能接受该完整 commitment。观测重试依靠确定性 ID 保持幂等，但 agent run 的内部壁钟可能在重启后生成另一条全拒绝回执，所以运维必须先验证台账和最后一条 commitment，不能把“调用方未收到成功”直接解释为“没有提交”。

首次建立台账父目录时只逐层 `mkdir`，每层都拒绝 reparse、同步父目录项并重验从卷根到直接父目录的 resolved path / file ID / reparse token；不会把祖先建立委托给锁原语的递归 `mkdir`。相对路径、`.` / `..`、symlink / junction、硬链接以及复制到另一登记路径的台账都会失败关闭。业务字段至少为：

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

`EconomicContextArtifact` 绑定政策散列、`decision_time`、所有 as-of 合格观测 ID、输入散列，以及生成时观测台账的 `sequence/head_sha256`。`verify_economic_context` 会在规范路径锁内取出已绑定台账前缀，重建历史 context 并逐字节比对。历史 context 可继续回放，但这不等于它仍是当前完整输入：`propose` 会在持有观测台账锁时拒绝前缀之后任何 `available_time <= decision_time` 的记录，并保持观测锁直至运行台账 commitment 完成。run receipt 另行绑定该次提交时已重验的完整观测 `sequence/head_sha256`，因此后续追加不会倒灌改变既有运行的提交时事实。

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
- 参数数量、预测期与单提案 trial budget 不得超额；
- 已到 `holdout_start_time`、语境跨过 holdout 边界，或证据在边界后才可知时增加对应拒绝原因；
- 无论本地政策中的 `holdout_start_time` 是时间还是 `null`，当前版本都会无条件加入 `holdout_governance_unbound` 并拒绝全部提案；`null` 不是开发绕过入口。

批次会先执行 `max_proposals_per_run` 数量上限，再完整验证 `ResearchProposal` 合同；任一输入非法或超额时整批在生成回执前失败，且不会把未知字段或原始对象封存到台账。CLI 对 ingest 批次设置固定 records / bytes 上限；proposal 的 records 上限直接取受信政策，bytes 上限按配额计算并另有 8 MiB 硬顶。读取采用 `max_bytes + 1` 的有界二进制读取，JSON / JSONL 都不会先把无界整文件载入内存。只有合同合法且在配额内的输入才会按 `input_index` 保存规范 proposal，供 verifier 完整重建。总 trial budget 分配和 accepted duplicate 门禁在 v1 全拒绝期间不启用，未来治理绑定版本必须以新的 method version 恢复并重新验证。

提案评估时间由模块内部 `research.clock.utc_now()` 生成，CLI 不接受可回填时间。该壁钟尚不是治理可信时钟，因此它只进入全拒绝回执；在可信时钟和治理绑定完成前不能产生 accepted proposal。

治理库绑定是阻断型遗留接口。未来版本必须显式绑定 market，直接查询 governance SQLite 中已封存的 holdout vintage，并将不可变记录身份绑定到运行输入；在该查询、封存状态校验和可信时钟校验完成前，本模块不会产生 `holdout_governance_bound=true`，也没有本地政策、receipt 或参数可以绕过这一限制。

## 身份与台账

代理默认不使用 LLM，并记录：

- `provider=none`；
- `model_id=deterministic-rules-v1`；
- 空 prompt / model input / model output 的 SHA-256。

v1 不接受调用方自行声明的外部 inference hash，因为单独的 64 位散列不能证明模型、prompt 或输入输出制品存在。若未来接入隔离 LLM 流程，必须先把这些制品封存、内容寻址并登记到可复核的治理身份，再恢复相应入口。本模块仍不会自行联网或调用模型。

每次运行产生内容寻址 run receipt，并在哈希链 JSONL 逻辑追加一条 commitment。ledger schema 为 `2`，method 为 `economic-research-agent-v1-embedded-ledger`。receipt 内嵌受信政策、完整 context、规范 valid proposal、输入散列、全部拒绝原因、固定 authority 和提交时观测链头。台账行固定 `receipt_storage=embedded_in_ledger` 且 `artifact_commitments=[]`。verifier 会从这些输入重建 receipt 与台账行并 canonical 全等比较，而不是只检查自洽 SHA-256。run loader 按 `run_id` 从已完整验证的台账行返回内嵌 receipt；proposal loader 在 v1 固定失败关闭。

这些是不带密钥的本地散列链：它能拒绝散列重算但语义非法的伪造，却不能把一套完整、语义仍合法的两本台账重写与原历史区分。后续若需要独立审计真实性，必须增加仓库外签名或不可变 checkpoint。v1 不存在 accepted proposal，因此该剩余风险不能伪造可消费提案，但仍限制拒绝历史的外部可证明性。

v1 的接受数永远为零，因此持久化面收缩为观测台账和运行台账两本文件。正式 `propose` 不建立 `.staging`、`runs` 或 `proposals`，也不执行 hard-link landing；通用内容制品 writer 也明确拒绝 proposal/run kind。这些可被双换位攻击的 standalone 通路已从实现中删除。未来如果允许 accepted proposal，必须升级 method/schema，并另行引入经端到端证明的安全 CAS，不得恢复旧 staging/landing 实现。

剩余写路径在操作前固定直接父目录。POSIX 使用 `O_DIRECTORY|O_NOFOLLOW` 的目录 fd，锁文件与首次 ledger inode 都相对 fd 建立，既有 ledger 也相对 fd 打开，因此 lexical 父目录即使被换走又恢复，追加与回滚仍只作用于原 inode。Windows 首先以 `Global\guvolu-economic-ledger-<path-hash>` 命名 mutex 串行同一规范台账，不为锁创建任何路径文件；再以原生 `CreateFileW` 创建随机、delete-on-close 的子锚点。锚点句柄的 final path 必须精确属于已验证父目录；如果打开边界被 junction 换位，锚点关闭时由内核从外部目录删除并失败关闭。锚点验证正确后，其不共享 `DELETE` 的打开句柄在整个 lock/read/append 事务内阻止父树改名。Windows 的台账读句柄以 no-reparse/no-delete 原生打开；更新句柄不共享任何访问。两者都核对普通文件、inode 身份与单链接；POSIX 锁则相对固定 fd 打开并使用 `flock`。

台账不再建立 `.append-*` 临时 inode，也不使用 replace。这样攻击者无法先对完整新临时文件建立外部 hardlink，再让该 inode 成为 canonical ledger。若攻击者在追加期间对 canonical inode 建立 hardlink，锁退出前的句柄级 link-count 检查会失败；回滚直接在仍固定的 inode 上恢复旧内容并截回原长度，因此不可删除的外链也只能看到旧 commitment，首次建账的外链最多保留空文件。

文件与目录同步完成后的最后一次句柄身份、canonical 路径身份及单链接检查是明确 commit point；此前任何错误都保持可回滚。`propose` 在 run ledger 越过该点前，还会在同一观测锁内重读并精确比对先前已验证的 observation ledger 前缀；依赖台账在此时改变会使 run append 回滚。只有 run commitment 已持久化后才共享 operation-level committed 状态，此后包围的只读 observation lock 不再用事后路径检查反转公共 API 的成功结果。

实现会在 commit point 先脱离 pending transaction，再关闭数据 fd；`close` 已生效后报告的清理错误不会改写为普通 API 失败或诱导调用方重试。回滚时的 data-fd `close` 后效错误同样不遮蔽触发回滚的主异常，且仍继续删除首次建账文件并同步目录；真实的内容恢复、unlink 或目录同步失败才升级为回滚失败。同一 commit 状态继续传到 mutex/flock 解锁、锁 fd、父目录 fd 与 anchor 的外层清理：提交后的不确定清理错误不再反转 API 结果，body 已失败时也不允许次生 cleanup 错误遮蔽原异常。Windows 父链清理会收集首个应传播的错误、继续关闭其余全部祖先句柄，然后再返回首错。

之后由其他主体新建的 hardlink 属于已提交台账的事后篡改，后续 loader 仍会失败关闭。回归覆盖 observation ledger、run ledger、Windows mutex/anchor、POSIX `<ledger>.lock`、“检查后换位—写入—恢复”、两个公共入口的 hardlink 竞态，以及数据 fd 和三层外部资源的 close/unlock-after-effect 边界。内容寻址 context 仍在固定父目录下通过临时文件和 no-clobber link 安装。目录 `fsync` 在 POSIX 上强制；Windows 仍依赖文件 `fsync` 和存储栈，因此断电耐久性需在目标 NTFS/控制器上做独立故障注入认证。

## CLI

入口为 `scripts/run_economic_research_agent.py`。以下命令都是显式的 research-only 本地文件操作：

```powershell
.\.venv\Scripts\python.exe scripts\run_economic_research_agent.py ingest `
  --input data\research\economic\incoming.jsonl

.\.venv\Scripts\python.exe scripts\run_economic_research_agent.py context `
  --policy data\research\economic\policy.json `
  --decision-time 2026-05-03T00:00:00Z

.\.venv\Scripts\python.exe scripts\run_economic_research_agent.py propose `
  --context reports\economic-research\contexts\economic-context-....json `
  --observation-ledger data\research\economic\observations.jsonl `
  --proposals data\research\economic\proposals.json `
  --policy data\research\economic\policy.json

.\.venv\Scripts\python.exe scripts\run_economic_research_agent.py verify `
  --policy data\research\economic\policy.json
```

示例中的 `policy.json` 是调用方必须在 `--root` 内提供的受信政策，不是仓库内置文件。`ingest` 输入可为 JSON 数组、单个 JSON 对象或 JSONL，并受 10,000 records / 16 MiB 的整批上限。proposal 输入 records 上限为政策中的 `max_proposals_per_run`，bytes 上限为 `min(8 MiB, 4096 + 配额 × 64 KiB)`。所有输入必须位于 `--root` 内；观测/运行台账只可写入 `data/research/economic`，只有 context 内容制品写入 `reports/economic-research`。CLI 在 resolve 前拒绝所有写路径及允许写入根的 lexical alias、symlink 与 junction；核心落盘使用固定父目录操作。`context` 只读观测台账并写内容寻址语境。`propose` 必须重新读取观测台账、验证 context 的前缀语义与当前 as-of 完整性，再仅写入内嵌全拒绝 receipt 的运行台账。CLI 返回 `run_id`、`ledger` 与 `receipt_storage=embedded_in_ledger`，不再返回伪 standalone receipt 路径。CLI 没有接受时间、外部 inference identity 或治理回执覆盖参数。`verify` 需要受信 policy，并语义重建两个台账及全部内嵌 receipt commitment；正向 CLI `propose` / `verify` 已有端到端回归。

## 与策略管线的遗留接口

本版没有可消费的 accepted proposal。未来显式绑定治理、升级 method version 并恢复接受逻辑后，`SearchPlan` 消费器还必须完成：

1. 通过专用 loader 验证 proposal 语义、源 context、受信政策和运行台账 commitment；
2. 把已审批的 `family/template/parameter_bounds` 编译为现有 typed AST；
3. 在 Candidate Registry 中预登记并生成新的 SearchPlan；
4. 把 `proposal_id` / `economic-context` / observation evidence IDs 写入 SearchPlan 与全试验台账的上游 lineage；
5. 依次经过 SearchFast、CPU f64 exact、embargo walk-forward、多重检验、邻域稳定、未来 holdout 和 paper 门禁。

在这些接口和治理证据完成前，经济代理的任何提案都只是待检验假设，不是策略，更不是上线许可。
