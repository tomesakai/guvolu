# 行业稳健性证据接入研究管线（2026-08-27）

## 结论

行业稳健性证据不再是研究运行之外的补算步骤。研究管线在同一次运行内、执行截止时刻确定之前完成四类证据生成、汇总、attestation 与台账写出，并把制品身份登记进本次运行的 `manifest.json` 与 `summary.json`。检查器由此可以直接从运行制品定位证据，不需要外部拼装，也不再报 `INDUSTRY_EVIDENCE_GENERATION_CUTOFF_INVALID` 与 `INDUSTRY_EVIDENCE_GENERATOR_CUTOFF_INVALID`。

[docs/2026-08-27-industry-evidence-generator-v1.md](2026-08-27-industry-evidence-generator-v1.md) 描述的四类构造规则、阈值来源与数值口径不变。本快照只记录接入点、窗口约束、开关、失败关闭语义与独立审计入口的定位。

## 检查器的窗口判定

两个截止原因码的判定条件（[industry_readiness](../src/guvolu/research/industry_readiness.py) 的 `_industry_evidence_bindings`）：

| 原因码 | 判定为不合格的条件 |
|---|---|
| `INDUSTRY_EVIDENCE_GENERATION_CUTOFF_INVALID` | 汇总制品的 `generated_at` 缺失或不可解析；manifest 的 `execution_evaluated_at` 或 `decision_time` 缺失或不可解析；汇总制品的 `decision_time` 与 manifest 的 `decision_time` 不等；或 `decision_time <= generated_at <= execution_evaluated_at` 不成立 |
| `INDUSTRY_EVIDENCE_GENERATOR_CUTOFF_INVALID` | attestation 的 `decision_time` 与 manifest 的 `decision_time` 不等；attestation 的 `generated_at` 与汇总制品的 `generated_at` 不等；`attested_at` 缺失或不可解析；或 `generated_at <= attested_at <= execution_evaluated_at` 不成立 |

两端边界都是闭区间。因此一次运行内的合法时序为：

```text
decision_time <= generated_at <= attested_at <= execution_evaluated_at
```

## 接入点

接入点在 [pipeline](../src/guvolu/research/pipeline.py) 的 `run_research` 内，位于 walk-forward 验证与研究质量门禁之后、`execution_evaluated_at` 打点之前：

1. 写出本次运行的 `features`、`trial_ledger`、`label_cost_replay` 与 `candidate_registry` 制品；
2. 以同一次运行已经冻结的面板、特征、`label_cost_replay`、输入收据与 `config_hash` 生成四类证据；
3. 打点 `execution_evaluated_at`，随后照旧计算运行时质量门禁、分配与目标位置；
4. 把证据制品登记进 `artifacts`，并在 `summary.json` 与 `manifest.json` 新增 `industry_evidence` 记录。

生成使用与独立审计入口相同的装配代码 [industry_evidence_run](../src/guvolu/research/industry_evidence_run.py)，输入身份由同一个 `read_run_identity` 读取，因而两条路径不会产生数值或身份漂移。生成过程不重新冻结输入、不另建面板、不重选候选，也不写治理库。

### 运行标识的派生变更

汇总制品必须携带本次运行的 `run_id`，而 `run_id` 原本由 `execution_evaluated_at` 派生，证据将无法在打点之前构造。现在 `run_id` 改由运行起始时点派生：

```text
run_id = stable_identifier("research-run", {research_identity, run_started_at})
```

`run_started_at` 是 manifest 与 summary 的新增字段（D-06 只增）。[verification](../src/guvolu/research/verification.py) 在 manifest 带 `run_started_at` 时按新口径重算，否则按 `execution_evaluated_at` 的旧口径重算，历史运行的复核结论不变。

## 新增字段

`manifest.json` 与 `summary.json` 的 `artifacts` 新增七份内容寻址制品：`tail_risk_evidence`、`stress_scenario_evidence`、`fixed_target_cost_replay`、`l2_depth_capacity_evidence`、`industry_evidence`、`industry_evidence_generator_attestation`、`industry_evidence_ledger`。每份记录 `kind`、仓库相对 `path`、`sha256` 与 `bytes`。

两份文件同时新增顶层 `industry_evidence` 记录，字段为 `status`、`method_version`、阈值配置位置与其散列、`generated_at`、`attested_at`、`industry_evidence_sha256`、逐制品身份、`venue_l2_coverage` 与逐候选 `scenario_counts`。

## 开关

| 键 | 位置 | 缺省 | 语义 |
|---|---|---|---|
| `research.generate_industry_evidence` | 研究配置 | `true` | 关闭后本次运行不生成证据 |
| `research.industry_evidence_config` | 研究配置 | `config/industry_evidence.json` | 版本化阈值与网格位置，必须位于项目目录内 |
| `--no-industry-evidence` | `scripts/run_strategy_research.py` | 未传即开启 | 只能关闭配置已开启的生成，供快速迭代 |

命令行开关只做单向收紧，不能开启配置已关闭的生成。关闭生成的运行产出不可用于准入。

## 失败关闭边界

生成失败与证据缺失是两件事，分别处理：

| 情形 | 结果 | `status` |
|---|---|---|
| 开关关闭 | 不生成，如实标注 | `disabled` |
| 本次运行没有 paper 可用部署候选 | 不生成，如实标注 | `absent` |
| L2 活动 head 覆盖不足 | 照常生成，容量场景标注 `insufficient_l2_coverage` 且不展开为场景，绝不外推 | `generated` |
| 阈值配置不可读、上游制品不一致、样本外区段越过面板截止上限、生成器内部抛错 | 整次研究运行失败并给中文错误 | 无 |

覆盖不足是有效结果：容量证据不达标会让检查器继续报容量类原因码，但不会让研究运行失败，也不会产出缺证据却看似完整的 summary。除上述两类显式标注外，生成路径上的任何异常都会被包成 `行业稳健性证据生成失败` 抛出，整次运行失败关闭。

面板截止上限与封存段预检仍由运行自身在冻结阶段完成，证据生成复用同一个生效上限读取样本外区段，不另行放宽。

## 独立审计入口的定位

[scripts/generate_industry_evidence.py](../scripts/generate_industry_evidence.py) 保留为独立审计入口，用于对历史运行重算并与已登记证据比对。它在研究运行封版之后执行，产出证据的 `generated_at` 必然晚于该运行的 `execution_evaluated_at`，检查器会报出上述两个截止原因码而不接受。该定位写在入口的模块文档与 `--help` 尾注中，生成报告以 `entry_point` 与 `generation_within_registration_window` 两个字段如实记录，不做时间戳改写。

## 仍未满足的准入原因码

接入本身不解除任何准入 blocking。政策 [config/industry_strategy_readiness.json](../config/industry_strategy_readiness.json) 仍把生成器与来源重放标注为 `not_implemented` 且批准生成器清单为空，因此稳健性门禁继续报出 `INDUSTRY_EVIDENCE_GENERATOR_NOT_IMPLEMENTED` 与 `INDUSTRY_EVIDENCE_SOURCE_REPLAY_NOT_IMPLEMENTED`。政策文件散列受 `industry_readiness` 固定绑定，任何放宽都需要单独评审。
