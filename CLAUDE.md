# guvolu

以 GMO Coin 执行为安全边界、并接入多来源公开市场数据的量化交易与研究体系。

## 必读文档

1. [SKILLS.md](SKILLS.md) — 最高约束，编号规则 T/R/C/D/X/G/U/A/W。冲突时以它为准。
2. [docs/00-rules-registry.md](docs/00-rules-registry.md) — 0 号文档，编号域与文档清单登记册。
3. [docs/architecture.md](docs/architecture.md) — 架构锁定项与 TBD 台账。TBD 可提案实施并在台账标注；【已锁定】条目不得单方变更。
4. [docs/materialization-design.md](docs/materialization-design.md) — SQLite schema v20、raw v3、Parquet、版本与活动 head 的长期权威契约。
5. [docs/order-flow-data-contract.md](docs/order-flow-data-contract.md) — L2、质量、状态、REST anchor、OFL 与 L3 合同边界。
6. [docs/venue-capability-matrix.md](docs/venue-capability-matrix.md) — 各来源能力、主力、fallback 与有限度的唯一现行对照。
7. [docs/runtime-ops.md](docs/runtime-ops.md) — 长期采集、物化、恢复和单写运行边界。
8. [docs/2026-08-13-l2-quality-and-l3-readiness.md](docs/2026-08-13-l2-quality-and-l3-readiness.md) — 当前活动头、质量和 L3 就绪度的冻结验收快照。
9. [docs/evidence/crypto_api_l3_registry_2026-08-12.json](docs/evidence/crypto_api_l3_registry_2026-08-12.json) — L3 来源工作簿的内容身份与端点登记清单；不是接入证明。
10. [docs/error-catalog.md](docs/error-catalog.md) — 错误码对照与处置册。
11. [docs/strategy-research.md](docs/strategy-research.md) — PIT 策略研究、验证与 paper 目标位置契约。

## 架构前提

两把密钥完全正交：READ_ONLY 仅读（13 REST + 4 WS 频道），TRADE 仅写（5 个下单撤单端点）且无任何读取能力。
唯一真相源是 READ_ONLY；本地 intent_id 先落盘，两侧以交易所 orderId 关联（GMO 无客户端自定义委托号）。
公共市场数据以 `market_id + artifact_id + normalization_version` 保持来源隔离；
跨所只产生显式派生，不补写来源事实。L3 当前只有合同，没有生产 connector。

## 环境

- Python 3.12+ / Windows
- 密钥在 .env（已 gitignore，永不提交），模板见 .env.example
- 缺省运行模式为模拟运行（dry-run）
- 切换实盘需人工确认（A-01）

## 校验命令

内容合规（编号、链接、清单、符号、注释）：

```bash
python -m pytest tests/ -q
```

Markdown 样式（标题、表格、围栏、空白）：

```bash
node --test tests/md_style.test.mjs
```
