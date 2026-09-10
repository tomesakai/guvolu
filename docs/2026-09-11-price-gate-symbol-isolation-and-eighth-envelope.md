# 急变门按品种隔离与第八封信封（2026-09-11）

> 文档类别：时效快照，登记于 [docs/00-rules-registry.md](00-rules-registry.md)。
> 范围：授权信封急变门的一处缺陷、修复与回归测试；第八封信封取值；T-11 单日硬顶与执行预算的调整依据。合同正文仍在[执行链设计第 14 节](execution-chain-design.md)，本文只记录当日事实与决定。

## 1. 缺陷事实（实测）

信封状态文件按信封身份落盘，BTC 与 ETH 两条每小时任务共用同一份 `price_history`。观测行只有时刻与价格，没有品种。ETH 轮次把 ETH 参考价（约 380,000 JPY）与 BTC 上一轮参考价（约 12,000,000 JPY）放进同一窗口计算涨跌幅，得到约 307,000 bp，超过 500 bp 阈值即写入一小时暂停；紧接着的 BTC 轮次被 `price_move_pause` 拦截。

| 日期 | 触发轮次 | 连带暂停轮次 |
|---|---|---|
| 2026-09-08 | 3 | 3 |
| 2026-09-09 | 1 | 1 |
| 2026-09-10 | 2 | 3 |

证据为执行仓 `data/execution/live/live-report-sha256-*.json` 的 `gate_reason` 字段。该缺陷自 09-06 上膛 ETH 第二市场起存在，此前单市场运行不会触发。

## 2. 修复

- `PriceObservation` 新增 `symbol` 字段；`observe_price` 记录本轮品种；`price_move_bp` 与 `apply_price_move_gate` 只在同品种观测间计算涨跌幅；`GateInputs` 新增 `symbol`，live 执行器传入 `rule.symbol`。
- 状态文件的 `price_history` 行新增 `symbol` 键；旧行缺该键时装载为空品种，不参与任何品种的比较。
- 暂停截止时刻 `paused_until` 仍跨市场共享：一个品种真实急变时两个市场一起暂停，这是原设计意图，不改。
- 回归测试 `test_price_move_gate_compares_same_symbol_only` 覆盖跨品种不触发、同品种仍触发、状态往返与旧格式装载。

## 3. 第八封信封与硬顶

维护者 2026-09-11 确认（A-01）：入金后账户 JPY 约 30,000；单日硬顶由 10,000 上调至 30,000；单笔硬顶与单日笔数硬顶不变。

| 字段 | 第七封草案 | 第八封 | 依据 |
|---|---|---|---|
| order_jpy_max | 20,000 | 10,000 | 单笔硬顶不变；预算 10,000 乘最大暴露 0.60 为 6,000 |
| day_jpy_max | 100,000 | 30,000 | 两市场同日建仓合计约 11,800，需超过 10,000 |
| day_count_max | 40 | 20 | 两阶段执行每笔至多两个意图 |
| envelope_jpy_total | 1,000,000 | 300,000 | 两周有效期内足够 |
| max_position_jpy | 30,000 | 20,000 | 留约 10,000 现金给吃单阶段与手续费 |
| max_cumulative_loss_jpy | 5,000 | 3,000 | 账户资金约 10% |
| day_loss_jpy_max | 2,500 | 1,500 | 账户资金约 5% |
| 有效期 | 09-07 至 09-21 | 09-11 至 09-26 | 换封即新身份，首单 canary 压额重新生效 |

第七封草案保留在 `config/authorization_envelope.draft-7.json` 作为放量阶段的参考，不签发。

## 4. 执行预算

`config/paper_executor.json` 与 `config/paper_executor_eth.json` 的 `risk_budget_jpy` 由 5,000 改为 10,000，`no_trade_band` 由 0.01 改为 0.05；`scripts/run_frozen_live.py` 与 `scripts/run_frozen_shadow.py` 的 `--budget-jpy` 缺省同步为 10,000。预算上限受 `frozen_target_adapter` 的约束不得超过单笔硬顶 10,000，因此本次不取 15,000。两市场最大合计暴露约 11,800 JPY，其余为现金缓冲。

## 5. 触碰端点（A-03）

本次改动不新增任何写端点。live 路径触碰的端点与执行链设计第 13 节一致。

## 6. 签发步骤（维护者）

1. 主仓提交本次改动，确认 `pytest tests/ -q`、`node --test tests/md_style.test.mjs` 与 `mypy src` 通过。
2. 执行仓 `.env` 设 `GUVOLU_DAY_JPY_MAX=30000`（本次已写入），白名单为 `BTC,ETH`。
3. 运行 `scripts/issue_envelope.ps1 -Draft config\authorization_envelope.draft-8.json`：校验草案、提交主仓、执行仓快进、以新身份复核未熔断、重启观察进程。
4. 两条 -live 每小时任务已注册，无需重注册；下一轮起以新代码与新信封运行，任务日志 `logs/research/frozen-forward/live-scheduler.jsonl` 的 `envelope_sha256` 应变为新身份。
