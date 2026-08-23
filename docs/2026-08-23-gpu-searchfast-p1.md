# 2026-08-23 GPU SearchFast P3-1 实现快照：阈值网格、状态机扫描与数值对照

> 时效快照（W-02）：内容冻结于 2026-08-23，修订以新快照发布。
> 本文记录 [GPU SearchFast 架构](2026-08-22-gpu-searchfast-architecture.md) 第 5 节
> P3-1 阶段的实现落点、等价性测试、目标机实测与残余限制；不变更架构快照与任何
> 【已锁定】条目。阈值、容差与预算均为配置初值（G-06），实测数字只描述本次运行。

## 1. 范围与退出判据

| 项 | 架构要求 | 本次状态 |
|---|---|---|
| 扫描与 `generate_targets` 数值对照 | 逐柱等价 | 六流派、含门禁分支的合成面板逐柱等价测试通过；目标机 CUDA 实测见第 4 节 |
| DAG 求值与 `search_plan.py` 逐节点对照 | 逐节点等价 | 逐节点对照与三态真值表测试通过 |
| 无 CUDA 自动跳过 GPU 用例，CPU 对照用例仍运行 | 必须 | Torch 缺失时 Torch 用例跳过，纯 CPU 用例（身份、参考、台账）始终运行 |
| 试验台账全量登记 | G-07 | 每候选一行追加式 JSONL，含 F0 被拒者 |

## 2. 实现落点

代码位于 `src/guvolu/search/`，只依赖 `strategy/` 与 `data/durable_io.py` 的纯函数合同；
worker 路径不 import `research/`、`api/` 与 `ops/`。

| 模块 | 关键函数 | 职责 | 等价性测试 |
|---|---|---|---|
| `tensorize.py` | `tensorize_panel`、`panel_sha256`、`round_to_f32` | 面板到固定列名 f32 矩阵与 int8 有效性掩码；`None` 只转 NaN 加掩码 | `tests/test_search_bundle.py` |
| `bundle.py` | `SearchBundleIdentity`、`bundle_identifier`、`evaluation_identifier`、`write_search_bundle`、`load_search_bundle` | 十一字段内容寻址身份；manifest 加内容散列命名数组文件；加载逐项校验 | 身份随任一字段变化；篡改数组拒绝 |
| `kernels.py` | `KernelSession.evaluate_chunk`、`evaluate_nodes`、`candidate_chunks` | 按 DAG 拓扑序求值，参数无关节点会话内缓存；分块 1 至 1,024 | `tests/test_search_kernels.py` 逐节点对照 `search_plan._node_value`、三态真值表、分块不变 |
| `scan.py` | `scan_targets`、`scaled_targets` | 状态机 sizing 扫描；`parallel` 为结合律前缀扫描，`sequential` 为逐柱顺序 | `tests/test_search_scan.py` 对照 `generate_targets`，两种方法逐格相同 |
| `metrics.py` | `strategy_returns_tensor`、`chunk_metrics` | 成本后收益、Sharpe、换手、回撤；逐柱 f32、归约 f64 | `tests/test_search_metrics.py` 对照 f64 参考 |
| `parity.py` | `reference_returns`、`reference_metrics`、`exact_reference`、`compare_parity`、`ParityTolerance` | CPU f64 有序精确复算与容差比较 | `tests/test_search_parity.py` 对照 `validation.strategy_returns` 与 `evaluate_targets` |
| `ledger.py` | `TrialLedgerWriter`、`read_ledger` | 追加式 JSONL，完成后按内容散列命名 | `tests/test_search_ledger.py` |
| `runner.py` | `evaluate_bundle`、`run_parity`、`static_gate`、`ScreenConfig` | 评估编排、F0 静态闸门、F1 粗筛、SearchResult 制品、F3 精确登记 | `tests/test_search_runner.py` |
| `synthetic.py`、`panel_io.py`、`torch_runtime.py`、`identity.py` | `synthetic_panel`、`panel_payload`、`runtime_identity` | 合成自检面板、参考面板 JSON、Torch 与 CUDA 运行时登记、规范散列 | 同上 |
| `scripts/run_search_fast.py` | `export`、`evaluate`、`parity`、`selfcheck` | 入口；`evaluate` 为只读 worker | `tests/test_search_runner.py` |

`strategy/search_plan.py` 的 CPU 解释器同步修正为只求值所选流派根可达的节点；此前多流派
计划会因其他流派的 `parameter` 节点误报缺少参数。

## 3. 合同落实

| 合同 | 落实 |
|---|---|
| 搜索束身份 | `panel_sha256`、`feature_method_version`、`columns`、`dtype=f32`、`mask_semantics`、`search_plan_id`、`cost_model_hash`、`fold_spec`、`bootstrap{seed,block,paths}`、`kernel_method_version=searchfast-torch-dag-scan-v1`、`code_tree_digest`；缺一即拒绝 |
| 评估身份 | `evaluation-` 加 `sha256(canonical{candidate_id, search_bundle_identity})` |
| 列清单 | `close`、`log_return`、`gap_seconds`、`gate_open`、`flow_imbalance`、`volume_score`、`jump_score`，以及每个回看窗的 `trend_score@L`、`price_score@L`、`prior_high@L`、`volatility@L` |
| 三态编码 | int8 `1`、`0`、`-1` 为真、假、未知；`and` 任一假为假、全真为真、否则未知；`div_strict` 除零未知；`missing_or_lt` 左缺失为真、右缺失未知 |
| 状态机 | `gate_open` 为 `as_of <= decision_time` 且 `contiguous`；必要字段任一无效为零；`volatility_target` 进出场与 `baselines.generate_targets` 同序；`expression_target` 取目标表达式值，无效为零 |
| F0 静态闸门 | 候选回看窗不在面板中；`annual_volatility_target` 或 `maximum_target` 为负 |
| 容差初值 | 目标序列逐柱最大绝对差 1e-5、Sharpe 1e-3、换手 1e-6，`searchfast-parity-tolerance-v1` |
| 阶段 | `F0_rejected`、`F1_screened`（含 `screen_passed`）、`F3_exact`（parity 通过才 `promotable`）；parity 不通过者留在 F1 并记 `parity_out_of_tolerance` |
| 制品 | 搜索束目录 `search-bundle-<sha>/manifest.json` 加 `arrays/<sha>.bin`；`search-result-<sha>/manifest.json`、`targets-<family>-<sha>.bin`、`trial-ledger-<sha>.jsonl`、`parity/trial-ledger-<sha>.jsonl`、`parity/parity-summary.json`；不写 SQLite 与 Parquet |

## 4. 目标机实测

环境：Windows、RTX 5070 12 GB、compute capability 12.0、驱动 580.88、PyTorch 2.11.0+cu128、
CUDA 12.8、cuDNN 91900。主研究 venv（Python 3.12）未安装 Torch；GPU 评估在独立的
Python 3.11 venv 中以同一工作树 `src` 运行，与架构第 2 节「独立 venv、独立进程」一致。
输入为合成面板 `searchfast-synthetic-panel-v1`（种子 20260823，8,192 根小时柱，回看窗
24/72/168），策略配置 `config/search_fast_selfcheck.json`（六流派 2,019 个候选），成本率
0.001，年化周期 8,760。

每次评估为独立进程，计时自 Torch 载入后开始，含 CUDA 上下文初始化、面板上载、
DAG 求值、扫描、指标与制品写入；候选分块 1,024，扫描方法 `parallel`。

| 项 | 实测 |
|---|---|
| CUDA 评估总耗时 | 两次独立运行 0.89 秒与 0.91 秒 |
| CUDA 吞吐 | 约 2,230 至 2,260 候选/秒，约 1.8e7 候选柱/秒 |
| CUDA 分流派耗时 | breakout 96 候选 0.22 秒（含首个流派的上下文初始化）；flow_trend 1,536 候选 0.31 至 0.33 秒；trend 192 候选 0.04 秒；mean_reversion、grid_shadow 各 96 候选 0.03 秒；price_breakout 3 候选 0.02 秒 |
| CPU Torch 评估总耗时（同一实现，device=cpu） | 2.69 秒，约 750 候选/秒 |
| CPU 纯 Python f64 精确复算 | 每候选约 0.07 至 0.10 秒（8,192 柱），仅用于晋级候选 |
| 台账 | 2,019 行，F0 拒绝 0，F1 粗筛通过 653（阈值初值：Sharpe 不低于 0、回撤不高于 1、换手不低于 0） |

### 4.1 数值对照

对 653 个粗筛通过候选做 CPU f64 有序精确复算，比较 CUDA f32 目标序列与指标：

| 项 | 容差初值 | 实测最大绝对差 | 结论 |
|---|---|---|---|
| 目标序列逐柱 | 1e-5 | 1.10e-7 | 653 个候选全部在容差内 |
| Sharpe | 1e-3 | 2.31e-7 | 653 个候选全部在容差内 |
| 换手 | 1e-6 | 3.26e-6 | 261 个在容差内，392 个（flow_trend 303、trend 89）超出 |

按初值容差，261 个候选登记为 `F3_exact` 且 `promotable`，392 个留在 `F1_screened`
并记 `parity_out_of_tolerance`。换手差来自 f32 目标逐柱取整误差沿 8,191 根柱累加：
本次换手量级为 4 至 455，最大绝对差折合相对差约 1e-8。以 `--tolerance-config`
把换手容差设为 1e-5 的敏感性复算使 653 个候选全部通过，其余两项不变。该调整只作为
P3-2 的配置修订提案（随柱数缩放或改为相对容差），本次未改变缺省初值。

该结果只覆盖信号判断、状态机扫描与全样本粗筛指标，不含 walk-forward、bootstrap、
多重检验或制品登记，不能外推为全管线加速比。CPU 侧对照为纯 Python f64 有序实现，
未与 GPU 做同口径计时比较。

## 5. 运行方式

```text
python scripts/run_search_fast.py export   --output OUT [--panel-json P | --synthetic-bars N --synthetic-seed S --lookbacks 24,72,168] [--strategy-config C]
python scripts/run_search_fast.py evaluate --bundle OUT/search-bundle-<sha> --output RES --device auto|cpu|cuda --candidate-chunk 1024 --scan-method parallel
python scripts/run_search_fast.py parity   --bundle OUT/search-bundle-<sha> --result RES/search-result-<sha> --panel-json OUT/reference-panel-<sha>.json
python scripts/run_search_fast.py selfcheck --output OUT
```

`export` 与 `parity` 在 CPU 研究进程运行（读取研究侧 `CodeIdentity.tree_digest`）；
`evaluate` 为 worker，只读搜索束，不读取密钥、不接触网络与 SQLite。

## 6. 残余限制与 P3-2 待办

| 项 | 说明 |
|---|---|
| 真实面板接入 | 入口只接受合成面板或 `reference-panel` JSON；从活动 Parquet 经 DuckDB 列裁剪导出面板的 CPU 侧适配未做 |
| 折与 bootstrap | `fold_spec` 与 `bootstrap` 仅进入身份；折内指标、块 bootstrap 与 CSCV 为 P3-2 范围 |
| 粗筛阈值 | `ScreenConfig` 初值只含 Sharpe、回撤、换手下限，未接现行 walk-forward 门禁 |
| 扫描 | 结合律前缀扫描以 `torch.cat` 逐步拼接，未做原地更新与 CUDA Graph；单候选目标负值（负 `maximum_target`）由 F0 静态闸门排除，不在扫描内支持 |
| 精度 | 指标归约在 f64 完成，f32 仅用于逐柱量；目标序列 f32 与 CPU f64 的差来自输入取整与除法，受容差约束 |
| promotion | `promote_search_results.py` 未实现；F3 行只在台账标记 `promotable`，不写 Candidate Registry |
| 结构搜索 | `mutation.py` challenger 批量 fitness 为 P3-3 范围 |
| 环境 | 主研究 venv 未装 Torch，GPU 用例在该 venv 下全部跳过；目标机实测依赖独立 Torch venv |
