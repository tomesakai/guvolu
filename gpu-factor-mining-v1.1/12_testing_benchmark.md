# 12 正确性、统计、执行、GPU 与 UI 验证

版本：1.1-draft  
日期：2026-08-05  
文档性质：规范性  
主责：质量负责人  
前置文档：全部规范文档  
主要消费者：CI/CD、Release Review  

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


## 1. 测试层级

```text
Contract tests
Unit/property tests
Golden differential tests
Component integration
End-to-end research replay
Paper execution replay
Target-machine performance
Release candidate soak
```

任何性能优化必须先保持 contract/golden tests 通过。

## 2. 契约测试

- Protobuf schema 兼容；
- 必填字段和版本；
- FixedDecimal canonical form；
- Artifact hash/size；
- unknown enum 处理；
- 幂等和重复事件；
- 不同模块对同一 fixture 解释一致。

## 3. 数据测试

- PIT 可见性；
- universe survivorship fixture；
- corporate action/下架/迁移；
- available_time 晚于 event_time；
- label execution window；
- invalid/tie/common-valid；
- Parquet→Arrow→Panel round trip。

## 4. 编译器与 CPU

- 随机 AST property-based type safety；
- canonicalization 幂等；
- hash collision secondary compare；
- protected/strict op 边界；
- rolling min periods/ddof/missing；
- expression→bytecode→reference 一致；
- exact rank tie fixtures。

## 5. GPU

- CPU f64 differential；
- chunk boundary；
- random AST fuzz；
- constant、极小方差、NaN/Inf、全 ties；
- deterministic replay；
- Compute Sanitizer 四工具；
- forced OOM 与回退；
- CUDA failure 后作业恢复；
- compiled resource report assertions。

## 6. 统计

- CPCV 组合数与路径；
- information interval purge；
- CSCV `C(S,S/2)`；
- DSR 与论文公式/独立实现；
- trial count 包含失败候选；
- SALib 样本数；
- null data 假阳性率模拟；
- planted signal recovery；
- vintage 不重复消费。

## 7. 执行

- 订单状态 fold；
- duplicate/out-of-order events；
- partial fills/cancel/replace；
- tick/step/min notional；
- funding/fee/dividend/split；
- cash/NAV/position invariant；
- bar 数据不得使用未来整根流动性；
- reconciliation；
- kill switch 和 disconnect；
- TCA 分解加总到 implementation shortfall。

## 8. 可视化

- replay cursor 跨图同步；
- downsampling 保存高低和关键事件；
- invalid 不显示为 0；
- visual regression；
- 订单/成交与账本逐项一致；
- L2 tile 边界无断层；
- attribution/NOTICE；
- 基本键盘导航和可访问标签。

## 9. 目标机性能基准

所有性能门槛先由批准的基准构建产生：

```text
hardware UUID
OS/WDDM/TCC
GPU driver
CUDA runtime/toolkit
compiler flags
CUBIN hash
panel hash
thermal/power state
```

发布回归可以定义为“不低于批准基准的某比例”，比例是 `POLICY`。禁止跨驱动或不同表达式混合比较。

## 10. 发布门槛

必须通过：

- 所有规范性 contract/golden；
- 无未解释 GPU sanitizer 错误；
- 无账本缺口；
- holdout 数据不可见性测试；
- executiond 权限隔离；
- 数据和制品 hash 可重放；
- UI 关键数值与 API 一致；
- 所有 `TO-RESEARCH` 项若影响当前功能，已有明确禁用或保守默认。

## 11. 不再使用的原验收项

删除预设的：1000/300/30 候选每秒、一代 15–60 秒、一夜 200–500 代、固定 3 blocks/SM、统一 `<1e-5`、Warp 发散 `<5%`、网格非空率 `>80%`。它们只有目标机实测后才可成为某一发布基线。
