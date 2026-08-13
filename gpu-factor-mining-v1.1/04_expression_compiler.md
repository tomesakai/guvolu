# 04 表达式编译器、算子语义与 CPU 参考实现

版本：1.1-draft  
日期：2026-08-05  
文档性质：规范性  
主责：编译器负责人  
前置文档：02_domain_semantics.md、03_data_platform.md  
主要消费者：GPU Runtime、Evaluation、Search  

外部依据编号见 `16_references.md`。原始 v1.0 文档为本次重构的需求基线。


## 证据与决策状态

本文档集不把尚未实现或尚未实测的内容伪装成事实。关键结论使用以下状态：

| 标记 | 含义 | 可否直接作为实现事实 |
|---|---|---|
| `VERIFIED-SOURCE` | 已由官方文档、标准或原始论文核验 | 可以，但仍需固定引用版本 |
| `VERIFIED-CALC` | 可由确定性算术、组合数学或数据规模直接推导 | 可以 |
| `DECISION` | 本项目已经选择的架构语义 | 可以，变更须走 ADR |
| `POLICY` | 项目阈值或治理规则，不是普适真理 | 可以配置，不得宣称外部有效性 |
| `TO-BENCHMARK` | 只能在目标 RTX 5070、驱动、CUDA 与操作系统组合上测得 | 不可以预填性能结论 |
| `TO-RESEARCH` | 证据不足或依赖尚未选定的数据源、市场、券商或交易所 | 不可以进入生产默认值 |

出现冲突时，优先级为：正式契约与 ADR > 本文档正文 > 示例代码。外部资料只证明其明确支持的事实，不自动证明本项目的设计阈值。


## 1. 模块边界

编译器拥有 AST、类型检查、规范化、候选身份、静态成本、公共子表达式 DAG、字节码和阶段计划。它不拥有 GPU 分配、统计阈值、组合构建或候选选择。

CPU Reference 是独立 crate，但共享同一算子规范；它是数学基准，不复用 GPU 实现细节。

## 2. 类型系统

每个节点类型包含：

```text
ValueShape
Unit
Frequency
Availability
MissingPolicy
NumericDomain
```

示例：

```text
close: TimeSeries × Price(USDT) × Hourly × CloseAvailable × StrictInvalid
funding_rate: EventSeries × Rate × EventDriven × PublishedEvent × StrictInvalid
cs_rank(x): CrossSection × Dimensionless × x.frequency
```

条件节点的两个分支必须可统一类型；窗口参数为离散 `Window`，不是裸 f32。

## 3. 算子语义

严格与受保护算子必须是不同 opcode：

```text
DIV_STRICT(a,b): b==0 或任一输入非有限 -> invalid
DIV_PROTECTED(a,b,epsilon): 显式 epsilon 规则
LOG_STRICT(x): x<=0 -> invalid
LOG1P_STRICT(x): x<=-1 -> invalid
SQRT_STRICT(x): x<0 -> invalid
SIGNED_SQRT(x): sign(x)*sqrt(abs(x))
```

不能由编译器静默改变表达式数学含义。

滚动节点必须保存：

```text
window, min_periods, ddof, missing_policy, anchoring_policy
```

截面精确秩和近似秩是不同算子：

```text
CS_RANK_EXACT
CS_RANK_HISTOGRAM(bucket_count)
```

## 4. 候选身份

```text
expression_id = SHA-256(canonical AST bytes)
candidate_id  = SHA-256(expression_id + resolved parameter bytes)
evaluation_id = SHA-256(candidate_id + panel + universe + label + cost
                         + perturbation + numeric mode + binary hash)
factor_version_id = SHA-256(evaluation_id + admission batch + review version)
```

规范化只做语义保持变换，例如交换律节点的稳定排序、常数编码规范化。浮点代数重排默认禁止，因为可能改变有效性和数值结果。

哈希命中后仍比较 canonical bytes。

## 5. 字节码与参数表

原 8 字节指令的 `u8` 槽位和裸 `f32 imm` 被替换为：

```rust
#[repr(C, align(16))]
pub struct Instr {
    pub op: u16,
    pub dst: u16,
    pub a: u16,
    pub b: u16,
    pub imm_index: u32,
    pub flags: u32,
}
```

参数表为有类型值：

```rust
pub enum ParamValue {
    F32(f32),
    F64(f64),
    U32(u32),
    Window(u32),
    DurationNs(i64),
}
```

## 6. 阶段计划

编译结果不是单一程序，而是 DAG stages：

```text
E0 Elementwise/Lag
E1 O(1) Recursive State
E2 Finite Window Statistics
E3 Cross-sectional / Order Statistics
M  Metric / Reduction
```

```rust
pub struct CompiledPlan {
    pub plan_id: Hash256,
    pub stages: Vec<StagePlan>,
    pub state_layout: StateLayout,
    pub materializations: Vec<MaterializedNode>,
    pub estimated_resources: ResourceEstimate,
}
```

截面节点输出若继续进入时序节点，必须显式物化边界。编译器不得假设所有节点在一个 kernel 中完成。

## 7. 静态成本模型

输出：

```text
ast_nodes
instruction_count
state_bytes_per_asset
ring_bytes_per_asset
materialized_bytes
estimated_barriers
estimated_sort_stages
estimated_fp64_ops
candidate_complexity_score
```

复杂度目标不能只使用节点数。静态估计只用于早期拒绝和微批规划；实际资源报告由 GPU 编译后覆盖。

## 8. 公共子表达式

全批次构建公共 DAG，只有独立 materialization kernel 完成后，候选 stage 才能读取缓存。普通 CUDA 同一网格的不同 block 不得通过“首个候选写、后续候选读”建立依赖。

缓存收益估计：

```text
fanout × saved_compute
- write_cost
- read_cost
- memory_occupation_cost
```

缓存键使用完整候选子树语义、参数、面板、有效性、数值模式和编译版本。

## 9. CPU 参考实现

CPU Reference 使用 f64 和固定遍历顺序，输出：

- 值与有效位图；
- tie group 与选中 instrument IDs；
- 每个滚动节点的边界状态；
- 组合和成本前的标准化 score；
- 可序列化 golden fixture。

“与 NumPy 一致”不是规范；NumPy 仅可作为第三方交叉检查。规范来自本文档的算子定义。

## 10. 编译接口

```text
ParseExpression(text) -> ExpressionSpec
ValidateExpression(spec, market_schema) -> ValidationReport
Canonicalize(spec) -> canonical bytes + expression_id
Compile(spec, target_profile) -> CompiledPlan
EvaluateReference(plan, panel_slice) -> ReferenceArtifact
```

错误必须包含稳定 code、AST path、预期类型和实际类型。

## 11. 待研究

- `TO-RESEARCH EC-01`：rolling rank/min/max 的专用算法和最大允许窗口，须结合 GPU 基准。
- `TO-RESEARCH EC-02`：是否允许编译器进行严格受控的数值等价重写；默认禁用。
- `TO-RESEARCH EC-03`：NVRTC 精英通道的代码生成粒度、CUBIN 缓存淘汰与安全沙箱。
- `TO-RESEARCH EC-04`：表达式总槽位上限和 AST 深度；由目标机资源和搜索质量共同校准。
