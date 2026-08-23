# 2026-08-24 策略生成迭代循环 v1：GPU 宽筛、重采样粗筛、受约束提案与 CPU 研究运行

> 时效快照（W-02）：内容冻结于 2026-08-24，修订以新快照发布。
> 本文记录 [GPU SearchFast 架构](2026-08-22-gpu-searchfast-architecture.md) P3-2 阶段与
> 「策略生成迭代循环」第一版的实现落点、循环边界、运行方式、实测与残余；不变更架构快照
> 与任何【已锁定】条目。阈值、预算、容差全部为配置初值（G-06），实测数字只描述本次运行。
> 本文局部用语：循环（loop）指 `run_search_loop.py` 一次完整执行；提案（proposal）指
> 循环产出的 `proposal.json`，即研究配置候选网格的下一版建议；邻域平坦指候选在
> 一轴最近邻上的粗筛 OOS Sharpe 保留率与正向比例达到配置阈值。

## 1. 循环边界

| 边界 | 落实 |
|---|---|
| GPU 只产提案 | 循环写搜索束、SearchResult、试验台账、数值对照与 `proposal.json`；不改写 `config/strategy_research.json`，不写 Candidate Registry、SQLite 与 Parquet 真相 |
| 研究准入不变 | 提案经 `promote_search_results.py` 落为新研究配置文件，再由 `run_strategy_research.py` 走完整 walk-forward、FDR、PBO、DSR、bootstrap、邻域与成本后 OOS 门禁；循环中的 GPU 指标不构成准入 |
| 封存段不可触碰 | 面板上限为显式配置 `search_loop.panel_to_time`；晚于上限的柱与可得时间一律拒绝；面板区间与未消费封存段重叠即拒绝（G-08）；本次 `2026-08-23T09:00:00Z`，早于 vintage `holdout-vintage-690a9c9b…` 起点 `2026-08-24T00:00Z`，且在已登记研究暴露范围内 |
| 进程 | 循环在独立 `.venv-gpu` 进程运行，只读活动 head 与配置，不 import `api` 与 `ops`，不读取密钥（G-01） |
| 台账全量 | F0 静态闸门被拒、F1 粗筛（含重采样）与 F3 精确复算逐候选追加式登记，结构 challenger 同入台账（G-07） |
| 可复现 | 固定种子；折、bootstrap 子集与 CSCV 分割由 CPU 同种子随机源生成；manifest 登记 Torch、CUDA、驱动、cuDNN、设备与代码树摘要（G-03） |

## 2. 数据流

```text
config/strategy_research.json（谱系校验）+ config/search_loop.json
  -> (a) 候选：v4 注册网格 + 邻域网格（连续化轴）+ 有界 typed 结构 challenger
         -> 候选登记 candidate-registry.json，预算逐流派登记
         -> SearchPlan（注册流派与 challenger 共用 DAG 编译）
  -> (b) 面板：freeze_trade_inputs + build_panel_snapshot(to_time=panel_to_time)
         + compute_features -> tensorize -> 搜索束（身份含 panel_sha256、fold_spec、bootstrap）
         -> GPU F1（信号、扫描、成本后指标）+ P3-2（折内 OOS Sharpe、bootstrap、CSCV）
  -> (c) F3：对粗筛通过候选（按 Sharpe 降序、上限可配）做 CPU f64 精确复算 parity
  -> (d) 试验台账（F0/F1/F3 全量）+ SearchResult + 运行 manifest
  -> (e) proposal.json：每流派锚点（F3 通过且邻域平坦、得分最高）一轴切片上的正向取值，
         受 evolution.constraints 与研究预算约束；challenger 只登记证据
  -> scripts/promote_search_results.py -> config/strategy_research_candidate_<sha12>.json
  -> scripts/run_strategy_research.py（研究准入不变）
```

## 3. 实现落点

| 模块 | 关键函数 | 职责 | 测试 |
|---|---|---|---|
| `search/panel_source.py` | `load_research_panel`、`enforce_to_time`、`sealed_vintages_overlapping`、`panel_from_bars` | 冻结活动 head，按显式 `to_time` 构建面板并计算特征；只读查询封存段 | `tests/test_search_panel_source.py` |
| `search/resample.py` | `resample_spec_from_config`、`resample_chunk`、`cscv_subsets`、`bootstrap_block_starts`、`ResampleScreen` | embargo walk-forward 折（复用 `validation.make_folds`）、studentized 循环块 bootstrap、CSCV；GPU 归约、CPU 同种子抽样 | `tests/test_search_resample.py` 对照 `research.validation` |
| `search/runner.py` | `EvaluationOptions.resample`、`run_parity(templates, candidate_limit)` | F1 评估接入重采样并写台账；F3 支持 challenger 表达式与复算上限 | 既有 `tests/test_search_runner.py` |
| `search/loop.py` | `load_loop_config`、`generate_loop_candidates`、`neighborhood_candidates`、`run_search_loop` | 循环编排、预算登记、manifest | `tests/test_search_loop.py` |
| `search/proposal.py` | `build_proposal`、`flatness`、`one_axis_neighbors`、`ProposalThresholds` | 受约束提案 | `tests/test_search_proposal.py` |
| `search/promote.py` | `promoted_config`、`write_promoted_config`、`research_command` | 提案落为新研究配置文件 | 同上 |
| `strategy/expression.py`、`baselines.py`、`generation.py` | `strategy_expression_from_payload`、`generate_expression_targets`、`search_plan_payload` | challenger 表达式载荷重建、显式模板复算、通用搜索计划编译 | 既有策略测试 |
| `scripts/run_search_loop.py`、`scripts/promote_search_results.py` | `--config`、`--data-root`、`--synthetic`、`--device`；`--proposal`、`--family`、`--output-dir` | 入口 | 同上 |
| `config/search_loop.json` | 面板上限、流派范围、预算、邻域网格、challenger 上限、设备分块、阈值、容差、提案阈值 | 配置初值 | 合成自检对 `config/search_loop_selfcheck.json` 与 `config/strategy_research_selfcheck.json` |

## 4. 合同落实

| 合同 | 落实 |
|---|---|
| 搜索束身份 | `fold_spec` 为 `embargo-walk-forward` 加 `walk_forward` 四参数；`bootstrap` 为 `seed`、`block`、`paths`、`one_sided_alpha` 与方法版本，均取自研究配置，不再为占位 |
| 折 | 与 `validation.make_folds` 同参数同边界；每候选登记折内训练与测试 Sharpe、测试净收益、正向折比例 |
| bootstrap | `studentized-circular-block-sharpe-v2` 口径；流派种子派生与 CPU 相同；每候选在自身 OOS 拼接序列上给出下界与单侧 p |
| CSCV | `cscv-recent-even-window-tie-average-v4` 口径；子集由 CPU 同种子枚举或抽样；每候选登记样本外秩中位数，流派级 PBO 与秩中位数随行登记 |
| F1 粗筛 | `screen`（Sharpe、回撤、换手）与 `resample_screen`（OOS Sharpe、正向折比例、bootstrap p、PBO）同时通过 |
| F3 | `parity_tolerance` 目标 1e-5、Sharpe 1e-3、换手 1e-5（P3-1 建议值采纳为循环配置初值）；`parity_candidate_limit` 按粗筛 Sharpe 降序截取 |
| 提案 | 锚点为 F3 通过且邻域平坦（`minimum_neighbor_count`、`minimum_positive_neighbor_ratio`、`minimum_neighbor_sharpe_retention`）中得分最高者；数组轴取锚点一轴切片上 OOS Sharpe 与净收益为正的取值，围绕锚点至多 `maximum_axis_values` 个，越出 `evolution.constraints` 的取值记入 `rejected_by_constraint`；网格乘积缩减到 `evolution.maximum_candidates_per_family` 内；标量轴取锚点值 |
| promote | 新配置为谱系根（无 `evolution_parent`），`search_loop_source` 登记提案散列、搜索运行与父配置散列；父配置散列不符即拒绝；`build_family_batches` 复核预算；`features.lookbacks` 取并集 |
| 结构 challenger | `mutation.py` 有界变异与交叉，候选取注册网格参数，计划标签 `family~<expression_id 尾 16 位>`；GPU 评估与 CPU 复算同入台账；提案只登记证据，激活仍需源码登记、clean commit 与完整 ValidationExact |

## 5. 运行方式

```text
set PYTHONPATH=src
.venv-gpu\Scripts\python.exe scripts\run_search_loop.py --config config\search_loop.json --data-root <只读数据根> --device cuda
.venv-gpu\Scripts\python.exe scripts\run_search_loop.py --config config\search_loop_selfcheck.json --synthetic --device cpu
python scripts\promote_search_results.py --proposal reports\strategy-search\<search_run_id>\proposal.json [--family trend]
python scripts\run_strategy_research.py --config config\strategy_research_candidate_<sha12>.json
```

制品位于 `reports/strategy-search/<search_run_id>/`：`manifest.json`、`candidate-registry.json`、
`structural-challengers-<family>.json`、`search-bundle-<sha>/`、`search-result-<sha>/`（含
`trial-ledger-<sha>.jsonl`、`targets-*.bin`、`parity/`）、`panel/research-panel-sha256-<sha>.parquet`
与 `proposal.json`。`PYTHONPATH=src` 保证工作树 `src` 优先于 `.venv-gpu` 的可编辑安装。

## 6. P3-2 对照

合成面板（`searchfast-synthetic-panel-v1`，1,200 根柱，趋势流派）上 GPU 重采样与
`research.validation` 的对照（`tests/test_search_resample.py`，CPU 与 CUDA 两设备）：

| 项 | 对照对象 | 容差初值 | 结果 |
|---|---|---|---|
| 折内测试 Sharpe | `walk_forward_validate` 的 `testing` 试验行与 `evaluate_targets` | 1e-3 | 逐候选逐折通过 |
| bootstrap 下界与 p | `_studentized_circular_block_bootstrap_sharpe`（同序列、同种子） | 1e-3、2e-3 | 逐候选通过 |
| PBO 与秩中位数 | `_probability_backtest_overfitting`（同折得分、同种子） | 1e-6 | 流派级通过，分割数相同 |

## 7. 目标机实测

REAL_RUN_SECTION

## 8. 残余限制

| 项 | 说明 |
|---|---|
| 进程隔离 | 循环 v1 在 `.venv-gpu` 单进程内完成 CPU 面板导出、GPU 评估与 CPU 复算；GPU worker 独立进程的拆分未做 |
| 提案范围 | 只对数组轴（lookback、entry_score、flow_confirmation）与标量轴给出网格建议；不提案新增流派、不改成本模型与门禁阈值 |
| 重采样 | 每候选 bootstrap 与 CSCV 为粗筛近似：CPU 研究以流派冠军拼接序列计算，循环以候选自身 OOS 序列计算；DSR 与 FDR 不在循环内计算 |
| 研究运行 | 循环不自动运行 `run_strategy_research.py`；promote 只生成配置文件与命令 |
| 结构 challenger | 只做 GPU 评估与 CPU 复算并登记证据；激活路径不变 |
| 参考面板 | 真实面板缺省不写 `reference-panel` JSON（`write_reference_panel=false`），独立进程复跑 parity 需打开该项 |
| 折数与 CSCV | 折数由研究配置决定；CSCV 在折数不足四时跳过并记分割数零 |
