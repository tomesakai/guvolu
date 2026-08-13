# 2026-08-11 全来源数据状态与物化验收

> 本快照回答“本地到底有什么、能否重放、哪些能物化、各来源能做到什么以及还缺什么”。能力定义的现行版本仍以 [来源能力对照册](venue-capability-matrix.md) 为准；本文件冻结一次本地数据与实现验收结果。

## 1. 技术摘要

- 仓库根目录为 `C:\Users\wu_zh\dev\guvolu`，数据根目录为其下的 `data\`；SQLite 控制面、raw、archive 与 Parquet 均不跨盘隐式存放。
- 大事实采用 Parquet，SQLite schema v15 只保存维度、能力证据、覆盖、制品、尝试、拒绝/忽略和活动头；DuckDB 负责单写构建及只读分析。这一边界避免把数亿行重复写入 SQLite，也不引入跨进程 DuckDB 写竞争。
- `trade_observation` 的事实链为 `market_id + source_artifact_id + source_row_index + normalization_version`，批级再外键绑定能力修订；新尝试记为 `recorded`，迁移前历史记为 `migration-inferred`，两者不得混报。
- 物化只接受已封口、可散列、映射明确且可逐行重放的原件。新版 GMO K 线与三所 L2 已进入 P2；旧 SQLite `kline/book_top` 仍只是兼容层，不参与新版完成声明。
- 来源缺口只允许同所官方归档或稳定 REST 回扫；跨所数据只可作为带 `source_set` 的服务降级，或生成新的聚合 artifact，绝不填进原来源事实。

## 2. 范围、粒度与完成度

“全部来源”指当前 `venue` 维表登记的 10 家：GMO、bitbank、bitFlyer、Coincheck、Binance、Kraken、OKX、Bybit、Coinbase Exchange 与 Hyperliquid。验收单位不是“交易所有代码”，而是“来源市场乘数据域乘分区”。状态定义如下：

| 状态 | 必须满足 | 可用范围 |
|---|---|---|
| `documented` | 官方文档已核，尚无本地原文 | 接入设计 |
| `raw_sample` | 至少一个封口原件可散列 | schema 与语义验证 |
| `materialized` | 原件进入带市场、制品与版本键的活动事实 | 可重复分析 |
| `closed_partition` | 覆盖、SHA、PIT、唯一性、行数、拒绝与重放全部通过 | 该分区决策级研究 |
| `continuous` | 实时轮转、重连、健康窗口和长时运行已证明 | 实时监控 |

`closed_partition` 不等于“交易所所有 API 闭环”。一个来源的成交闭环不能外推为 K 线、盘口、状态、资金费率或私有账户闭环。

## 3. 本地数据类型与结果

| 数据集或存储 | 自然粒度 | 本地结果 | 是否进入新版 P2 | 有限度 |
|---|---|---:|---|---|
| `trade_observation` Parquet | 一条来源撮合或来源聚合成交 | 258,488,895 条活动事实，1,796 个活动月分区、43 个 market | 是 | Binance 是 `aggregate`，其余已接入归档为 `match`；不能混作逐撮合分布 |
| `trade_tick` SQLite | 旧规范化成交 | 1,353,575 行 | 否，兼容层 | 多数已由 P2 取代；Coincheck 只有 1 条短样本 |
| `kline` SQLite | 市场周期根 | 3,781,423 行 | 暂否 | 表内缺 `venue_id/market_id/artifact_id`；158 条仍为未完结或可得时刻早于本机获取时刻的临时根 |
| `market_kline` Parquet | 市场周期根的一种 OHLCV 状态 | 29 个 GMO market、3,781,784 状态 | 是 | 361 个真实修订冲突与 460 个 provisional 状态均保留，不默认当完成根 |
| `fact_source_evidence` Parquet | 一个 JSONL 请求行内的一个 `data[]` 项 | 3,941,741 行 | 是 | 重复观察不膨胀事实，但不能从证据层删除 |
| `book_top` SQLite | 一帧顶档 | 190,977 行 | 暂否 | 不是完整 L2；GMO 190,830、bitFlyer 144、bitbank 3，不能用于声称盘口历史完整 |
| `book_l2_frame/book_l2_level` Parquet | 一个来源帧 / 帧内一个价格级动作 | 21 个活动 segment、10,396 帧、352,423 档位动作 | 是 | 三所完整性模式不同；当前长期 run 尚未达到 24 小时验收门槛 |
| `stream_health` SQLite | 一个短运行或频道统计 | 12 行 | 控制证据 | 旧投影的 reconnect 为固定 0，不能证明长期重连能力 |
| raw / archive | 原始响应或官方文件 | `artifact` 登记 56,715 个内容身份、56,849 个物理位置 | 是，作为血缘输入 | open 文件不能散列成完成制品；日元三所盘口无官方历史回填 |
| `archive_coverage` | 来源市场、域、来源日 | 53,164 行 | 是，控制面 | `missing` 是缺口证据，不是零成交；`empty/0` 才是确认空日 |
| `materialization_rejection/materialization_ignore` | 尝试、原件、来源行/帧 | 成交 reject 367；L2 控制帧 ignore 55 | 是 | reject 是数据不满足事实契约；协议握手/控制帧不是坏数据 |
| `analysis_run/book_feature/alert_event` | 一次分析、一个特征窗口、一个规则事件 | 16 / 29 / 24 行 | 派生层 | 只读基础事实并保留请求/配置/来源散列；不能反向证明市场数据完整 |
| `backfill_run/data_correction` | 一次回补台账、一次显式修正 | 25 / 2 行 | 审计层 | 旧列序与 book PIT 已有修正证据；修正不覆盖原始文件 |

### 3.1 全部活动成交市场

| 来源 | 市场 | instrument | 类型/粒度 | 分区 | 来源行 | 事实行 | 拒绝 | UTC 范围 |
|---|---|---|---|---:|---:|---:|---:|---|
| Binance | BTCUSDT | `SPOT:BTC/USDT` | spot/aggregate | 1 | 1,299,165 | 1,299,165 | 0 | 2025-01-02 至 2025-01-02 |
| bitbank | btc_jpy | `SPOT:BTC/JPY` | spot/match | 113 | 70,353,938 | 70,353,937 | 1 | 2017-02-14 至 2026-08-07 |
| bitbank | eth_jpy | `SPOT:ETH/JPY` | spot/match | 76 | 28,323,973 | 28,323,973 | 0 | 2020-05-24 至 2026-08-07 |
| bitbank | xrp_jpy | `SPOT:XRP/JPY` | spot/match | 111 | 91,148,017 | 91,148,016 | 1 | 2017-05-25 至 2026-08-07 |
| bitbank | doge_jpy | `SPOT:DOGE/JPY` | spot/match | 2 | 78,522 | 78,522 | 0 | 2026-07-12 至 2026-08-10 |
| bitbank | dot_jpy | `SPOT:DOT/JPY` | spot/match | 2 | 5,080 | 5,080 | 0 | 2026-07-12 至 2026-08-10 |
| bitbank | ltc_jpy | `SPOT:LTC/JPY` | spot/match | 2 | 28,885 | 28,885 | 0 | 2026-07-12 至 2026-08-10 |
| bitbank | sol_jpy | `SPOT:SOL/JPY` | spot/match | 2 | 75,178 | 75,178 | 0 | 2026-07-12 至 2026-08-10 |
| bitbank | xlm_jpy | `SPOT:XLM/JPY` | spot/match | 2 | 46,752 | 46,752 | 0 | 2026-07-12 至 2026-08-10 |
| bitFlyer | BTC_JPY | `SPOT:BTC/JPY` | spot/match | 2 | 327,676 | 327,615 | 61 | 2026-07-08 至 2026-08-08 |
| bitFlyer | ELF_JPY | `SPOT:ELF/JPY` | spot/match | 2 | 2,370 | 2,370 | 0 | 2026-07-11 至 2026-08-11 |
| bitFlyer | ETH_JPY | `SPOT:ETH/JPY` | spot/match | 2 | 108,244 | 108,229 | 15 | 2026-07-11 至 2026-08-11 |
| bitFlyer | FX_BTC_JPY | `LEVERAGE:BTC/JPY` | leverage/match | 2 | 934,313 | 934,040 | 273 | 2026-07-08 至 2026-08-08 |
| bitFlyer | MONA_JPY | `SPOT:MONA/JPY` | spot/match | 3 | 500 | 500 | 0 | 2026-06-28 至 2026-08-11 |
| bitFlyer | XLM_JPY | `SPOT:XLM/JPY` | spot/match | 2 | 10,874 | 10,866 | 8 | 2026-07-11 至 2026-08-11 |
| bitFlyer | XRP_JPY | `SPOT:XRP/JPY` | spot/match | 2 | 50,439 | 50,431 | 8 | 2026-07-11 至 2026-08-11 |
| GMO | ADA | `SPOT:ADA/JPY` | spot/match | 50 | 1,187,348 | 1,187,348 | 0 | 2022-07-13 至 2026-08-07 |
| GMO | ASTR | `SPOT:ASTR/JPY` | spot/match | 42 | 1,129,525 | 1,129,525 | 0 | 2023-03-22 至 2026-08-07 |
| GMO | ATOM | `SPOT:ATOM/JPY` | spot/match | 51 | 598,433 | 598,433 | 0 | 2022-06-08 至 2026-08-07 |
| GMO | BAT | `SPOT:BAT/JPY` | spot/match | 54 | 322,403 | 322,403 | 0 | 2022-03-30 至 2025-06-27 |
| GMO | BCH | `SPOT:BCH/JPY` | spot/match | 92 | 1,714,044 | 1,714,044 | 0 | 2019-01-30 至 2026-08-07 |
| GMO | BTC | `SPOT:BTC/JPY` | spot/match | 96 | 19,414,663 | 19,414,663 | 0 | 2018-09-05 至 2026-08-07 |
| GMO | DAI | `SPOT:DAI/JPY` | spot/match | 50 | 75,538 | 75,538 | 0 | 2022-07-13 至 2026-06-05 |
| GMO | DOGE | `SPOT:DOGE/JPY` | spot/match | 37 | 2,223,204 | 2,223,204 | 0 | 2023-08-05 至 2026-08-07 |
| GMO | DOT | `SPOT:DOT/JPY` | spot/match | 51 | 819,739 | 819,739 | 0 | 2022-06-08 至 2026-08-07 |
| GMO | ENJ | `SPOT:ENJ/JPY` | spot/match | 51 | 417,942 | 417,942 | 0 | 2022-06-08 至 2025-06-27 |
| GMO | ETH | `SPOT:ETH/JPY` | spot/match | 92 | 10,374,190 | 10,374,190 | 0 | 2019-01-30 至 2026-08-07 |
| GMO | FCR | `SPOT:FCR/JPY` | spot/match | 52 | 455,259 | 455,259 | 0 | 2022-05-18 至 2026-08-07 |
| GMO | LINK | `SPOT:LINK/JPY` | spot/match | 50 | 523,303 | 523,303 | 0 | 2022-07-13 至 2026-08-07 |
| GMO | LTC | `SPOT:LTC/JPY` | spot/match | 92 | 1,406,844 | 1,406,844 | 0 | 2019-01-30 至 2026-08-07 |
| GMO | MKR | `SPOT:MKR/JPY` | spot/match | 50 | 276,443 | 276,443 | 0 | 2022-07-13 至 2025-10-04 |
| GMO | MONA | `SPOT:MONA/JPY` | spot/match | 57 | 184,676 | 184,676 | 0 | 2021-12-01 至 2025-06-27 |
| GMO | NAC | `SPOT:NAC/JPY` | spot/match | 21 | 74,656 | 74,656 | 0 | 2024-12-13 至 2026-08-07 |
| GMO | OMG | `SPOT:OMG/JPY` | spot/match | 51 | 113,527 | 113,527 | 0 | 2022-06-08 至 2023-07-28 |
| GMO | QTUM | `SPOT:QTUM/JPY` | spot/match | 54 | 404,686 | 404,686 | 0 | 2022-03-30 至 2025-06-27 |
| GMO | SOL | `SPOT:SOL/JPY` | spot/match | 37 | 3,859,188 | 3,859,188 | 0 | 2023-08-05 至 2026-08-07 |
| GMO | SUI | `SPOT:SUI/JPY` | spot/match | 8 | 156,951 | 156,951 | 0 | 2026-01-17 至 2026-08-07 |
| GMO | WILD | `SPOT:WILD/JPY` | spot/match | 6 | 63,463 | 63,463 | 0 | 2026-03-23 至 2026-08-07 |
| GMO | XEM | `SPOT:XEM/JPY` | spot/match | 63 | 911,952 | 911,952 | 0 | 2021-06-23 至 2025-06-27 |
| GMO | XLM | `SPOT:XLM/JPY` | spot/match | 61 | 1,147,925 | 1,147,925 | 0 | 2021-08-18 至 2026-08-07 |
| GMO | XRP | `SPOT:XRP/JPY` | spot/match | 92 | 15,836,700 | 15,836,700 | 0 | 2019-01-30 至 2026-08-07 |
| GMO | XTZ | `SPOT:XTZ/JPY` | spot/match | 51 | 337,253 | 337,253 | 0 | 2022-06-08 至 2026-07-17 |
| GMO | XYM | `SPOT:XYM/JPY` | spot/match | 59 | 1,665,481 | 1,665,481 | 0 | 2021-10-20 至 2025-06-28 |

K 线新版已经实现为 `market_kline(market_id, interval, open_time, origin, value_revision, normalization_version, ...)`，逐项原件身份在 `fact_source_evidence`。盘口已经拆成 `book_l2_frame` 与 `book_l2_level`，保存 snapshot/delta、sequence/checksum、深度和原始段身份。二者复用市场、制品、尝试和能力绑定模块，但不复用成交自然主键。

## 4. 全部来源的有限度与当前结果

| 来源 | 本地最高完成度 | 已有结果 | 适合担任主力 | 正确 fallback 或聚合 | 尚未完成及硬限制 |
|---|---|---|---|---|---|
| GMO | `materialized`；L2 为 `running_validation` | 27 个现货币官方逐日成交；29 个市场 K 线；BTC 30 档快照 L2 正在分段 | 日元历史成交与原生 K 线主轴；GMO 自身 snapshot L2 | 同所归档优先；查询层才与 bitbank/bitFlyer 比 VWAP、价差和量份额 | 无官方历史盘口；10 个历史已下架币精度未知并正确留空；L2 无序号/checksum |
| bitbank | `materialized`；L2 为 `running_validation` | BTC/ETH/XRP 长历史；SOL/DOT/DOGE/LTC/XLM 近 30 日；whole+diff L2 | 三对长历史与有 sequence 的实时 L2 主轴 | 同所日文件回补；REST 只重锚当前簿；跨所只做派生中位数 | 3 个官方日持续 404；47 个 JPY 对中仍有 39 个未回补；sequence 单调但不保证连续 |
| bitFlyer | `materialized`；L2 为 `running_validation` | 6 个 JPY spot 加 FX_BTC_JPY 近端成交；snapshot+diff L2 | 自有现货/CFD primary 与跨所独立验证票 | `/v1/executions` 同所游标回扫；现货与 leverage 只在分析层关联 | 无 K 线端点；execution 官方边界约 31 日；L2 无 sequence、断连窗不可重放 |
| Binance | `materialized` 小样本 | BTCUSDT 单日 `aggTrades` 加 CHECKSUM | 全球参考接入的校验样本 | 后续同所普通 `trades` 与 K 线；跨报价币须先绑定 FX 制品 | 只有一天且为 aggregate；逐笔、K 线、序号 L2 尚未形成完整本地闭环 |
| Coincheck | `raw_sample` | 42 个短 WS 帧、1 条成交旧事实 | 日元旁路可用性观察 | 服务降级必须标来源与无序号完整性 | 无历史闭环；orderbook 无序号且不补发；不能物化成完整历史事实 |
| Kraken | `documented` | 无本地事实 | 未来全球逐笔游标与 CRC32 L2 主力候选 | 与 Binance/Coinbase 做共同报价币稳健指标 | 采集器、映射、封口原件与小分区验证均未完成；OHLC 只有近 720 根 |
| OKX | `documented` | 无本地事实 | 全球历史 L2 与衍生品候选 | 仅补本所历史产品；跨所只生成派生指标 | 历史文件频率、snapshot/delta 与保真度尚未逐文件实测；实时深簿权限有限制 |
| Bybit | `documented` | 无本地事实 | 多产品行情与历史数据候选 | 与 OKX 分来源保存后再做共同窗口统计 | 历史目录的实际覆盖、深度和完整性尚未本地验证 |
| Coinbase Exchange | `documented` | 无本地事实 | 全球现货 L2/L3 与分页成交候选 | 与 Binance/Kraken 做同报价币参考 | 未接入；成交 `side` 是 maker 侧，规范化为主动方时必须显式反转 |
| Hyperliquid | `documented`，成交被阻断 | 无本地事实 | 链上永续 L2、资金费率候选 | 不作为日元现货 fallback | candle 只有近 5,000 根、L2 单侧 20 档；成交映射未核；永续与现货不得共用 instrument |

## 5. 重合能力与非重合能力如何绑定

重合能力不去重来源事实。三所都出现 BTC/JPY 时，共同的 `instrument_id=SPOT:BTC/JPY` 只用于对齐经济品种；每个来源仍有独立 `market_id`、来源成交 ID、能力修订和 artifact。非重合能力通过同一市场维度体系扩展：例如 `FX_BTC_JPY` 绑定 `LEVERAGE:BTC/JPY`，不能绑到现货 BTC/JPY；资金费率再以衍生市场与结算区间为事实键。

正确的聚合输出至少保存：

```text
analysis_artifact_id
+ method_version
+ source_market_ids[]
+ source_partition_attempt_ids[]
+ window_start / window_end / available_time
+ quote_conversion_artifact_id（异报价币时必需）
+ coverage_by_source / degraded_reason
```

日元稳健参考价只有在至少两所健康输入、相同现货品种、共同可得窗口内才取中位数；否则返回单所值并标 `degraded=true`。NBBO、跨所 VWAP、收益率中位数和成交量份额都是新分析制品，不覆盖基础事实。盘口价位绝不跨所拼接成虚构订单簿。

## 6. 验证方法与证据

最终验收按下列顺序运行，避免一边增长一边散列：

1. 确认只有一个物化写者，等待 `archive-backfill` 自然结束。
2. 再次执行 `init-dims`，登记所有已证明现货和 leverage 映射。
3. `archive-plan` 必须达到 `pending=0`；已知 404 月保持 blocked，不能强行完成。
4. `audit` 逐文件重算 SHA-256，并检查 Parquet 行数、PIT、唯一性、必填字段、市场和版本绑定。
5. SQLite 执行 `integrity_check` 与 `foreign_key_check`，再核能力绑定 basis、临时文件残留和活动头总量。
6. 只运行覆盖主键、绑定、拒绝、重放与文档契约的精准测试。

验收结果如下：

- `archive-plan`：1,799 个任务中 1,796 完成、`pending=0`、3 个 bitbank 已知 404 月保持 blocked；活动输入 258,489,262 行等于 258,488,895 条事实加 367 条拒绝。
- 成交物化全审计：56,813 个登记位置、1,804 个完成输出、259,842,469 行（含非活动历史）逐项核对，0 warning、0 error；其后新增 L2 用独立域审计验收。
- 全活动成交集：43 个市场、4 个已物化来源，PIT 违规 0、必填空值 0、跨分区重复 `observation_id` 0。
- GMO K 线：29 个 market、3,781,784 个事实状态、3,941,741 条逐项 evidence、361 个冲突修订、460 个 provisional，独立审计 0 error。
- 三所 L2：17:49 JST 冻结点为 21 个活动 segment、10,396 帧、352,423 档位动作、55 个协议控制帧 ignored、0 reject，独立审计 0 error；三条采集和一个 300 秒追赶物化窗口持续运行。
- SQLite：`integrity_check=ok`、FK 违规 0、canonical location 违规 0、未绑定尝试 0；873 个迁移前尝试为 `migration-inferred`，1,219 个新尝试为 `recorded`。
- 134 个全空月以零行 Parquet 正确提交；未登记终态文件和 `.stage/.tmp` 均为 0。14 个修复前失败清单移入 `data/quarantine/materialization-orphans/2026-08-11`，可恢复且不参与查询。
- 18 个静默 raw run 已逐有效行重算并追加 `recovered_incomplete` 清单，明确 `completion_claim=false`；open/unfinished 警告清零。旧 `ws_public.jsonl:42288` 的 65-byte 尾片段仍不可恢复，原文件不改写，其文件、目标行和相邻行散列登记在 `data/quarantine/raw-corruption/2026-08-08-ws-public-line-42288.json`。
- full 持久化审计因此仍正确报告一项旧 raw 损坏和 legacy durable-ack/归档 SHA/heatmap 源 SHA 盲区；这些限制不影响已由独立 artifact、输入行和 Parquet 全审计证明的 P2 成交事实，但禁止宣称全项目历史零丢失。
- 本次更新后 `data/` 为 13.70 GiB，其中 `materialized/` 为 8.04 GiB，C 盘剩余 124.71 GiB。按三所当前 BTC L2 样本，raw 约 1.3 至 1.5 GiB/日；连同 Parquet 与余量按 2 至 3 GiB/日做容量预算。

## 7. 可重复运行手册

```powershell
Set-Location C:\Users\wu_zh\dev\guvolu
uv run python -m guvolu.venues.collect init-dims
uv run python -m guvolu.data.materialize --data-root data recover-stale --older-minutes 60
uv run python -m guvolu.data.collect reconcile-raw --older-minutes 60
uv run python -m guvolu.data.materialize --data-root data archive-plan
uv run python -m guvolu.data.materialize --data-root data archive-backfill
uv run python -m guvolu.data.materialize --data-root data audit
uv run python -m guvolu.data.kline_materialize audit
uv run python -m guvolu.data.l2_materialize audit
PowerShell -ExecutionPolicy Bypass -File scripts/start_l2_collectors.ps1
uv run python -m guvolu.data.persistence_audit --mode quick --output logs/persistence-audit.json
```

重启时不使用另存的手工进度；SQLite 活动头、输入集合散列和覆盖台账共同构成断点。完成分区被复用，未提交分区重做，blocked 月保持隔离。查询只从 `materialization_partition_head` 读取活动 Parquet，禁止目录 glob 混入旧版本。

## 8. 后续顺序

1. 把实时正文改为按 `market_id/domain/run/segment` 封口，并实现 `collection_segment` 能力外键；先保障日元三所不可回补盘口。
2. 从四份已封口 GMO K 线原件逐数组项重放，补 `source_item_index`、完结状态和 origin 后再物化 `market_kline`；不直接迁移语义不足的旧表。
3. 建立 `book_l2_frame/book_l2_level`，先做每所一个短封口段的快照或差分重建验证，再长时采集。
4. 全球来源按 Binance 普通 trades、Kraken BTC/USD、Coinbase BTC-USD、OKX/Bybit 单一现货小分区的顺序接入；每个来源先闭环一个市场，再扩币种。
5. 最后接入资金费率、OI、标记价与私有账户域；它们复用维度和制品模块，但使用独立事实契约与权限边界。

这些步骤按“不可回补实时流优先、已在本地且可重放的数据其次、可随时重新下载的数据最后”排序。
