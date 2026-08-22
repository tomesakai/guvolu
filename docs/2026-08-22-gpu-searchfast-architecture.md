# 2026-08-22 GPU SearchFast 架构：TBD-18/19/20 实施案

> 时效快照（W-02）：内容冻结于 2026-08-22，修订以新快照发布。
> 本文是 TBD-18、TBD-19、TBD-20 的实施案（A-05 提案），在
> [architecture.md](architecture.md) 第 5 节锁定项与
> [策略研究管线](strategy-research.md) 第 9 节 GPU 方式之内展开，不变更任何
> 【已锁定】条目。
> 本文局部用语：`SearchPlan` 指类型化公共子表达式 DAG 的内容寻址搜索计划制品；
> `SearchResult` 指 GPU worker 对一个搜索束的全部候选评估输出；数值对照（parity）
> 指 GPU 结果与 CPU 参考在登记容差内的逐项比较；P3 为本文使用的实施阶段标号。
> 本次同步登记：TBD-18、TBD-19、TBD-20 台账条目引用本文；术语搜索束、三态逻辑
> 入 [SKILLS.md](../SKILLS.md) 第 7 章。

## 1. 依据

| 序 | 依据 | 位置 |
|---|---|---|
| 1 | 研究与 GPU 管线五条锁定项；GPU 核输入边界固定在活动 Parquet 事实经内存 DuckDB 列裁剪、CPU 完成 schema、PIT、散列验证与数值域转换之后 | [architecture.md](architecture.md) 第 5 节 |
| 2 | TBD-18：RTX 5070 目标机 `typed-searchfast-threshold-grid-v1` 微基准，CUDA f32 对 CPU f64 加速 956 倍，最大绝对差低于预登记容差；首阶段隔离 PyTorch worker | [architecture.md](architecture.md) TBD-18、[策略研究管线](strategy-research.md) 第 9 节 |
| 3 | TBD-19：typed CPU reference 已实现；公共子表达式 DAG、typed mutation/crossover 与长期 promotion registry 未决 | [architecture.md](architecture.md) TBD-19 |
| 4 | TBD-20：向量化 walk-forward 基线已实现；做市、queue、部分成交与撤单失败仍需事件驱动模拟器，未做 | [architecture.md](architecture.md) TBD-20 |
| 5 | 身份链 `expression_id`、`candidate_id`、`evaluation_id`；试验台账全量登记；F0 至 F3 分级评估 | [GPU 因子挖掘资料采纳评估](2026-08-05-gpu-factor-mining-adoption.md) 第 2、3 节 |
| 6 | 基准脚本：f32 输入、候选分块 512、容差 `max(1e-6, 2e-5 * max(abs(reference)))`，CPU 侧有序 `fsum` f64 归约 | [benchmark_strategy_search.py](../scripts/benchmark_strategy_search.py) |
| 7 | 执行仓 `codex/execution-chain` 工作树已有 generation v4 的 `SearchPlan`（`typed-common-subexpression-dag-v1`）、`search_plan.py` CPU 解释器与 `mutation.py` 有界结构搜索（`bounded-typed-structure-search-v1`），将随分支合并进 main | 执行仓 `src/guvolu/strategy/generation.py`、`search_plan.py`、`mutation.py` |

## 2. 数据流

```text
PIT 面板 + 特征（CPU 导出 f32 数组与有效性掩码；锁定输入边界）
  -> SearchPlan DAG（CPU 解释器即规范）
  -> GPU worker（独立 venv .venv-gpu、独立进程、只读搜索束；
     永不 import api 与 ops，不持密钥）
  -> SearchResult + 试验台账（G-07，含 F0 静态闸门拒绝）
  -> CPU ValidationExact（f64 精确复算晋级候选，登记数值对照）
  -> Candidate Registry promotion
  -> 既有 walk-forward、FDR、DSR、PBO
  -> paper 与 shadow
```

各环节所有权：面板与特征导出、`SearchPlan` 生成、ValidationExact、walk-forward
与统计门禁全部在 CPU 研究进程；GPU worker 只承担上列第三步，输入是只读搜索束，输出
是 `SearchResult` 与台账追加行。`SearchResult` 不构成候选资格：任何候选必须经
ValidationExact 在登记容差内复算通过后才进入 Candidate Registry，其后路径与现行
CPU 网格完全相同（[策略研究管线](strategy-research.md) 第 10 节）。

## 3. 部件与职责

建议代码落点为 `src/guvolu/search/`，只依赖 `strategy/` 与 `research/` 的纯函数合同：

| 部件 | 建议落点 | 职责 | 等价性义务 |
|---|---|---|---|
| 输入束 | `src/guvolu/search/bundle.py` | 构造内容寻址搜索束与身份（第 4.1 节） | 身份字段缺一即拒绝 |
| 张量化 | `src/guvolu/search/tensorize.py` | 面板到张量；`None` 转 NaN 加有效性掩码；布尔三态以 int8 的 `1、0、-1` 表示真、假、未知 | `and`、`div_strict`、`missing_or_lt` 逐条与 CPU `evaluate_expression` 等价 |
| 核 | `src/guvolu/search/kernels.py` | 按 DAG 拓扑序求值；参数无关节点共享缓存；节点张量形状为候选分块乘柱数的 f32 | 与 `search_plan.py` 解释器逐节点对照 |
| 扫描 | `src/guvolu/search/scan.py` | 状态机 sizing 逐柱推进、候选维度并行成批；先以 PyTorch 实现，profile 后再决定是否写融合核 | 与 `baselines.generate_targets` 逐柱等价 |
| 重采样 | `src/guvolu/search/bootstrap.py` | 折与块 bootstrap、CSCV，固定种子（G-03） | 与 `research/validation.py` 同种子同分割 |
| 数值对照 | `src/guvolu/search/parity.py` 与 `research/validation.py` | 精确复算与容差登记 | 容差为配置（第 4.2 节） |
| 入口 | `scripts/run_search_fast.py`、`scripts/promote_search_results.py` | 运行搜索；把通过 ValidationExact 的候选登记进 Candidate Registry | 只读输入，只写制品与台账 |

## 4. 合同

### 4.1 搜索束身份

搜索束身份由以下字段的规范 JSON 散列构成，任一字段变化即为新的搜索束：

| 字段 | 说明 |
|---|---|
| `panel_sha256` | 冻结面板身份 |
| `feature_method_version` | 特征方法版本 |
| `columns` | 导出列名与顺序 |
| `dtype` | 固定 `f32` |
| `mask_semantics` | 掩码与三态编码约定 |
| `search_plan_id` | `SearchPlan` 制品身份 |
| `cost_model_hash` | 成本模型配置散列 |
| `fold_spec` | walk-forward 折与 embargo 定义 |
| `bootstrap{seed, block, paths}` | 重采样种子、块长与路径数 |
| `kernel_method_version` | GPU 核方法版本 |
| `code_tree_digest` | 代码树摘要，与 `CodeIdentity.tree_digest` 同源 |

### 4.2 评估身份与容差

`evaluation_id = sha256(candidate_id + 搜索束身份)`，与采纳评估第 3 节定义一致。
每项指标的容差为版本化配置（G-06），以下为配置初值提案：

| 指标 | 容差初值 | 比较对象 |
|---|---|---|
| 目标序列 | 逐柱最大绝对差不超过 1e-5 | GPU f32 扫描对 CPU `generate_targets` |
| Sharpe | 绝对差不超过 1e-3 | GPU 归约对 CPU f64 |
| 换手 | 绝对差不超过 1e-6 | 同上 |

### 4.3 预算与 TDR

预算由三项构成：每流派候选预算（现行 `evolution.maximum_candidates_per_family`）、
参数邻域半径、结构 challenger 上限（`mutation.py` 的 `candidate_budget` 投影校验）。DSR
与 FDR 的试验数从试验台账全量取得，不以 GPU 实际评估数代替（G-07）；GPU 小时只是
资源约束，不进入统计口径。

TDR 约束（[architecture.md](architecture.md) TBD-22）：候选分块 512 至 1,024，柱按
窗分段，单核执行时间保持在 1 秒以内；8,192 柱乘 1,024 候选的 f32 节点张量约
32 MiB，显存峰值按 DAG 节点数与分块数估算，在 12 GB 内留余量。

## 5. 阶段与测试

| 阶段 | 范围 | 退出判据 |
|---|---|---|
| P3-1 | 阈值与参数网格、状态机扫描 | 扫描与 `generate_targets` 数值对照通过；DAG 求值与 `search_plan.py` 逐节点对照通过 |
| P3-2 | 折与块 bootstrap、CSCV | 同种子同分割下与 `research/validation.py` 结果在容差内 |
| P3-3 | 结构搜索 fitness 批处理 | `mutation.py` 生成的 challenger 经 GPU 批量 fitness 后全部进入台账，晋级只经 ValidationExact |

测试义务：三态逻辑真值表（`and`、`div_strict`、`missing_or_lt`）；`scan.py` 与
`baselines.generate_targets` 等价；`kernels.py` 与 `search_plan.py` 对照；无 CUDA
环境自动跳过 GPU 用例，CPU 对照用例仍必须运行。

## 6. 禁区的落实点

GPU 职责边界的正文见 [策略研究管线](strategy-research.md) 第 9 节，本文只登记各
禁区的落实部件：

| 禁区 | 落实部件 | 机制 |
|---|---|---|
| 不解析 raw JSON | `bundle.py` | 只接受 CPU 导出的 f32 数组与掩码 |
| 不补数据缺口 | `tensorize.py` | `None` 只转 NaN 与掩码，不插值、不前向填充 |
| 不决定统计阈值 | `parity.py` 与配置 | 阈值与容差全部来自版本化配置（G-06） |
| 不写 SQLite 与 Parquet 真相 | GPU worker 进程 | 只写 `SearchResult` 制品与台账追加行；登记由 CPU 侧 `promote_search_results.py` 完成 |
| 不持密钥、不下单 | GPU worker 进程 | 独立 venv 与独立进程，永不 import `api` 与 `ops`（G-01、T-13） |
