# 运行时保活与进程操作设计（TBD-31 提案载体）

> 默认公共行情白名单为三所 BTC L2 加三所
> BTC/JPY 实时逐笔分段采集。完整事实与回补边界见
> [订单流数据事实契约](order-flow-data-contract.md)。
> 控制面使用 SQLite schema v20；新采集段使用 raw v3，三所 L2 与实时逐笔
> 分别物化为 L2 物理 schema v3 / `book-l2-normalization-v5` 与 trade schema v3 /
> `trade-realtime-normalization-v3`；OKX 历史 L2 仍为 schema v2 /
> `book-l2-normalization-v2`；book-state 为 schema v1 /
> `book-state-checkpoint-v3`，OFL 为 schema v2 /
> `orderflow-tile-sparse-v8`。
> 当前项目根为 `C:\Users\wu_zh\dev\guvolu`，数据根为
> `C:\Users\wu_zh\dev\guvolu\data`；脚本工作目录和 `--data-root` 均应解析到
> 这两个位置，不能沿用旧的 D 盘假设。
> L2 每条任务 5 分钟或 128 MiB、逐笔每条任务 5 分钟或 32 MiB 封一段，
> 运行 checkpoint 与完成 manifest
> 分离。需要在桌面查看逐段 rows/bytes/SHA-256 进度时运行
> `powershell -File scripts/start_marketdata_pipeline.ps1`；脚本按完整命令行
> 幂等，已有同任务不会重复启动，并额外拉起 L2、实时逐笔、book-state 与
> OFL tile 四个域内单写 materializer。前两者追赶 sealed segment；book-state
> 维护当前状态；OFL watcher 只处理最近两小时仍活动的 L2 市场，物化上一完整
> 小时及当前已到达桶。各任务每 300 秒按输入散列复用完成分区；
> 向同一 SQLite 控制面提交时受跨进程写锁串行化；OFL watcher 在最外层取锁
> 超时或遇到可恢复 IO/SQL/DuckDB 错误时记录 `orderflow_tile_cycle_error`，等待
> 下一轮继续，不能因一次长迁移占锁而退出。
> 每轮守护还会隔离片段与 checkpoint 都已静默一小时的崩溃尾段；
> `recovered_incomplete` 不进入事实活动头，鲜活稀疏流不会被误封。

> 文档类别：长期维护，登记于 [docs/00-rules-registry.md](00-rules-registry.md)。
> 范围：查询服务、采集进程、回补任务的保活链、互监与前端拉起；**不含交易进程**——实盘切换与交易进程管理始终是人工确认事项（A-01），不入本设计。
> 平台前提：Windows，Python 绝对路径调用（PATH 漂移教训），任务计划程序为系统级守护位。

## 1. 进程清单与保活责任链

| 进程 | 职责 | 保活方 | 判活依据 |
|---|---|---|---|
| 查询服务（api，8721） | UI 数据供给、进程操作端点 | 发布期服务宿主；当前不由市场数据任务代管 | `/api/health` |
| 录制守护（record，逐来源逐域） | 三所盘口与逐笔实时落盘 | 任务计划程序加查询服务进程管理器 | 60 秒 checkpoint；高频 L2 有业务静默看门狗，稀疏逐笔依赖 WebSocket keepalive，并分别记录 wire/data 新鲜度 |
| sealed 物化器 | 日元三所 L2 schema 3 / normalization v5 与实时逐笔 schema/normalization v3 Parquet 追赶；OKX 历史 L2 schema/normalization v2 独立回补 | 任务计划程序幂等拉起 | 输入散列、活动 head、schema/normalization version 与物化日志 |
| 派生物化器 | book-state checkpoint v3、L2 quality 与小时 OFL tile v8 | 任务计划程序幂等拉起 | 上游 attempt 外键、活动 head、逐轮 audit/日志 |
| 市场状态观察 | L2 watcher 周期内非阻塞刷新 bitbank status/circuit-break 独立事实 | 不另设常驻进程 | `market_status_input_scan`、事实 head 与错误日志 |
| REST anchor worker | 连接打开、重连或每 300 秒触发的三所 REST 旁路锚定 | 嵌入每条 L2 采集进程，不另设常驻进程 | 有界队列、超时/限频、raw/事实散列与 `l2_anchor_status` |
| 回补任务（backfill 与 archive，批处理） | 历史求缺 | 不保活——幂等可重跑，断了由求缺续 | 覆盖表 `archive_coverage` |
| web（5173 开发期） | 查看器 | 开发期人工；发布期为静态文件由 api 托管，无独立进程 | 不适用 |

**互监形态**：任务计划程序直接保证公共市场数据写者，查询服务可收编同命令
实例并显示状态；web 不保任何进程。查询服务与市场数据互不作为对方的唯一
保活条件，避免 api 故障造成不可回补实时流断档。

### 1.1 实时身份与健康口径

raw v3 每帧必须保存 `endpoint_id + endpoint_revision + connection_id + channel_id`、
UTC/单调接收时钟及 payload SHA-256；segment manifest 再保存原件 artifact
SHA-256。采集进程的 endpoint revision 在启动时固定，一个 run 中不得静默切换。
物化成功事务把合格数据帧观察登记到 schema v20 的 `collection_connection` 与
`collection_channel`，并把频道绑定到 market 与能力修订。

`connection_ordinal` 是一个 run 内成功打开连接的从一开始顺序号，首个连接也是
1，不能显示或解释为 reconnect 次数。reconnect 数只来自独立运行统计。当前
`opened_at/subscribed_at` 的 basis 为
`first_successfully_materialized_raw_v3_frame`：它是首个成功物化数据帧的观察时刻，
不是 socket 建连或订阅 ACK 时刻；未记录独立关闭/退订事件时终止列保持 NULL。
旧 raw v1 可继续重投影，但端点修订、连接/频道和单调时钟列必须为 NULL，并在
`data_quality` 明示降级；守护和 UI 不得把这类行报成 raw v3 完整身份。

GMO 实时逐笔即使在无限期运行模式也使用九十秒数据静默看门狗。收到带
`error`/`errors` 的帧时先按 raw v3 原样持久化并计为控制帧，再让当前连接失败、
退避并重新订阅；九十秒没有任何 wire 帧也走同一重连路径。只有成功持久化正常
`trades` 数据帧才清零连续失败计数。这样 `ERR-5003` 等服务端错误不会被误当成
健康控制帧而使采集器永久静默。

L2 watcher 每轮在 L2 主事实提交之外刷新 `l2-quality-v1` 和 bitbank 市场状态；
任一旁路失败只记录错误并等待下一轮，不能回滚或阻塞 L2。REST anchor 的
connection-open/reconnect/periodic 触发必须投递后台有界队列，网络请求、Decimal
解析、散列和 reconciliation 均不得占用 WS 接收循环。队列满、限频、超时或来源
不可用写 `unavailable/unknown`，不重试成 WS 补帧。该 worker 已嵌入三条 L2
采集命令，随连接打开、重连及每 300 秒定期触发；它不增加独立进程，也不改变
L2 活动 head。
OKX live books 已完成有界真实隔离小样本，但尚未证明重连和长期连续性；受控生产
验收前不得加入长期白名单。

## 2. api 侧进程管理器

- 登记表驱动：每采集进程一条登记（名称、命令行、工作目录、自动重启策略、退避序列 2/4/8/16 秒封顶、最大连续失败数）。
- 判活双通道：子进程存活状态加心跳清单时戳；进程活而心跳超时视为僵死，杀后重启并记事件。
- 全部事件（启动、退出码、重启、僵死判定）落 JSONL 运维日志（logs/ 运维视角，与 raw 分轨）。
- 崩溃循环保护：连续失败达上限即停止自动重启，状态转「须人工」，UI 显示 danger 徽章。
- 外启实例收编（2026-08-09 实施）：判活与拉起前按命令行匹配扫描本机同命令进程，命中即收编为运行态并记 `adopt` 事件——拉起因此幂等、不重复采集（写者唯一性），判活、心跳僵死判定与停止对收编实例同样生效；收编实例消失记 `external_exit` 并按重启策略处置；扫描失败按无外启继续（可用性优先）。

## 3. 操作端点（查询服务内，与读取同源）

| 端点 | 语义 |
|---|---|
| `GET /api/ops/processes` | 登记表全量：名称、状态（运行 / 停止 / 退避中 / 须人工）、心跳时刻、重启计数 |
| `POST /api/ops/processes/{name}/start` | 拉起（幂等：已运行返回现状） |
| `POST /api/ops/processes/{name}/stop` | 停止（仅采集进程；无任何交易语义） |

安全边界：仅绑 127.0.0.1 加本地令牌（沿用既有守卫）；登记表白名单之外的命令不可执行（端点不接受任意命令参数）；采集进程的启停属查看版可自治范围，与实盘切换（A-01）无关且物理隔离——本端点永不管理任何持有 TRADE 密钥的进程。

## 4. 前端拉起形制

1. **就地拉起**：数据面板的空态与陈旧态即拉起入口——热力图空态（录制未运行）显示拉起按钮替代命令文本；按钮按语义色纪律描边形制，点击后按钮转「退避中」态直至状态端点确认。控制出现在需要它的地方，不设独立进程页。
2. **能力页汇总**：CAP 页增采集总览区——逐进程状态徽章行（数据源自 `/api/ops/processes` 与 `archive_coverage`），含拉起与停止按钮。
3. 状态条连接态徽章沿用，不重复陈列进程细节。

## 5. 系统级守护脚本

- 任务计划程序两条：`guvolu-marketdata-logon` 在登录时启动，
  `guvolu-marketdata-guard` 每五分钟调用同一幂等入口。
- 两条任务只调用 `scripts/start_marketdata_pipeline.ps1 -WindowStyle Hidden`；
  重复实例策略为 IgnoreNew，不以 TCP 端口替代采集 checkpoint。
- `scripts/register_marketdata_tasks.ps1` 负责登记、查询和精确清理旧
  `guvolu-api-*` 任务；不含任意命令执行能力。
- NSSM 服务化保留为后续选项（宿主常态 24 小时运行且需要免登录守护时启用）。

任务定义与任务当前是否启用是两件事。版本切换期间可临时禁用 guard；是否已经
恢复必须以任务计划程序现场状态和当次质量快照为准，长期文档不把“已登记”写成
“当前已启用”。

## 6. 补节（2026-08-10）：API 停机与重启后的期间重建

### 6.1 停机与重启覆盖盘点

覆盖缺口逐项核对现有实现，结论是**常规重建路径齐全，仍有两个自动化缺口**：

| 数据面 | 停机期间 | 重启后重建 | 覆盖保证 |
|---|---|---|---|
| kline（SQLite） | 无新数据 | `upsert_klines` 幂等，求缺补拉 | `fetched_periods` 按可得时刻（D-04） |
| raw 公开流 | 断流即空档 | 不补发，空档如实保留 | raw 不可变（D-02） |
| 当日瓦片 | 停在最后视界 | `refresh_all` 自当日起点增量重建 | 幂等 |
| 跨多日未完结瓦片 | 不复建 | 需 CLI 全量重建（`--all`） | 手工或计划触发 |
| GMO 官方归档 | 不影响 | 覆盖表求缺续传 | 下载校验后写 `archive_coverage`；历史未登记文件可扫描补登 |
| 报警与判读 | 无新事件 | `book_feature` 追加式 | schema v4 幂等键复用既有行 |

### 6.2 缺陷与建议

1. **瓦片游标只写不读**：当日增量刷新推进游标但重启后不回读游标续建，而是从当日 UTC 起点全量重建；正确性无碍（幂等），成本在当日数据量大时偏高。
2. **跨多日停机未完结瓦片不自动重建**：api 重启后 `refresh_all` 只处理当日，前一未完结日停留在旧视界；需 CLI `--all` 全量重建，建议接入任务计划程序每日一次兜底。

旧整日瓦片的查询性能迁移不需要重读 raw。按桶档运行：

```powershell
$env:PYTHONPATH='src'
python -m guvolu.data.heatmap_tiles index --symbol BTC --venue gmo --bucket 1s --all
```

索引一次扫描整日 gzip，每 512 列写独立物理块，meta 原子切换到新代次；重复执行幂等。查询只读命中块，避免拖动时为每个窗口重复解压整日文件。
3. **raw 与 coverage 已闭环**：公开 WS 原始文本现先 `fsync` 后解析；GMO archive 下载校验后登记 coverage，旧文件通过 `scan-gmo-archive` 幂等补登。
4. **保活链单向无环**：任务计划程序保 api、api 进程管理器以进程状态加 420 秒 checkpoint 宽限保采集，采集断线重连重订阅并把缺口段标不完整。链条不依赖环形心跳；api 自身死亡期间 raw 空档如实保留。

### 6.3 适配器侧重建约定

后续来源适配器遵守 [来源接口与公开行情采集设计](venue-api-reference.md) 的游标
契约：Kraken `since`、Coinbase `CB-AFTER`、Binance `fromId` 持久化后续扫；
未完结日归档不登记覆盖，重跑幂等。

## 7. 持久化审计与恢复探针

上线前、回补后及每日低峰运行只读 quick 审计；发布验收再运行 full。恢复探针只在系统临时目录构造提交、回滚、幂等重放、撕裂 gzip 与原子替换故障，不修改生产数据：

```powershell
$env:PYTHONPATH='src'
python -m guvolu.data.persistence_audit --mode quick --probe --probe-records 500 --output logs/persistence-audit.json
```

退出码 `0` 表示现有证据完整证明，`1` 表示未发现确定丢失但仍有证据盲区，`2`
表示发现损坏或跨层不一致。不得把 `1` 报成“已证明零丢失”；报告中的 `unproven`
应按新旧数据分别治理。审计覆盖 raw/manifest、archive/coverage、SQLite
integrity/foreign key/跨表终态、raw v3 端点/连接/频道/接收时钟/payload 散列、
Parquet schema 与归一版本、L2 quality、market status、REST anchor 的 endpoint/PIT/
散列/可比范围、heatmap generation/meta/index，并统计空表以暴露尚未接线的落库
链路。raw v1 的预期 NULL 加质量旗标不算错误，倒填身份才算错误。

## 8. 长期采集、回填与物化日程

长期任务按数据可恢复性分优先级。不可回补的实时盘口优先于可回补成交，成交优先于可由成交派生的 K 线；派生任务永不反向阻塞采集。

| 周期 | 任务 | 断点与判据 | 资源策略 |
|---|---|---|---|
| 常驻 | 三所 BTC L2 WS 加内嵌 REST anchor worker、三所 BTC/JPY 实时逐笔 | 每 60 秒 open checkpoint；5 分钟或容量封口；anchor 按连接与 300 秒周期触发 | L2 最高优先；每市场每域单写者；anchor 失败不阻塞 WS |
| 每 300 秒 | sealed L2、实时逐笔、book-state、L2 quality、bitbank status 与 OFL tile 追赶 | 输入散列与活动 head 断点；旁路观察失败不回滚 L2；open 段跳过；当前小时不生成未来 gap；OFL 外层锁失败只终止本轮 | 每数据集单写；SQLite 提交写锁串行；不阻塞 raw |
| 每小时 | bitFlyer executions 增量回扫 | `before` 游标；最老未核时点不得逼近 31 日边界 | 限速 1.5 次每秒；失败不影响 WS |
| 每日 00:15 UTC 后 | bitbank 前一 UTC 日逐笔 | 日期覆盖 `ok/empty/missing` | 5 次每秒以内；公开 K 线尚未接入主干，不虚报已采集 |
| 每日 06:30 JST 后 | GMO 新官方逐笔归档与 K 线 | JST 06:00 发布后散列、行数和覆盖登记 | 与实时采集分限速池；失败次日重试 |
| Binance D+1 发布后 | ZIP 与 `.CHECKSUM` 成对下载 | SHA-256 匹配才登记 artifact | 归档优先于 REST；变更 CHECKSUM 产生新制品 |
| 每日低峰 | `archive-plan` 后 `archive-backfill` | SQLite 活动头是断点；跳过 blocked 月 | 全机仅一个 DuckDB/Parquet 写任务 |
| 每日低峰 | OKX L2 `plan` 后有界 `run` | 日级 sealed manifest、活动头与 `backfill_run` | 与其他 Parquet 写任务串行；保留 20 GiB 加单日工作空间 |
| 每日 | quick 持久化审计、陈旧尝试恢复 | `recover-stale` 只清专属临时文件 | 不散列全部大制品 |
| 每周或发布前 | full 制品、PIT、绑定与覆盖审计 | warning/error 均进入报告 | 允许长时间顺序读盘，不与全量回补并发 |

标准恢复顺序固定为：

```text
停止重复写者检查
-> 初始化/迁移 SQLite schema v20 与端点修订
-> recover-stale
-> reconcile-raw（仅静默旧 run）
-> 检查并幂等恢复孤立 REST anchor raw
-> repair-control-ledger（先规划，确认后 --apply）
-> 各来源 coverage 求缺或游标续扫
-> archive-plan
-> archive-backfill
-> audit
-> 查询服务读取新的 partition_head
```

磁盘余量高于 20% 正常运行；低于 20% 暂停历史扩量与派生重建；低于 10% 只保留不可回补实时流并报警；任何写入出现 `ENOSPC` 或 fsync 失败时立即停止该写者，不能继续推进 checkpoint、manifest 或 coverage。

当前可重复命令为：

```powershell
uv run python -m guvolu.venues.collect init-dims
uv run python -m guvolu.data.materialize --data-root data recover-stale --older-minutes 60
uv run python -m guvolu.data.collect reconcile-raw --older-minutes 60
uv run python -m guvolu.data.book_l2_anchor recover-raw --data-root data --raw-path <path> --check-only
uv run python -m guvolu.data.book_l2_anchor recover-raw --data-root data --raw-path <path>
uv run python -m guvolu.data.materialize --data-root data repair-control-ledger
uv run python -m guvolu.data.materialize --data-root data repair-control-ledger --apply
uv run python -m guvolu.data.materialize --data-root data archive-plan
uv run python -m guvolu.data.materialize --data-root data archive-backfill
uv run python -m guvolu.data.materialize --data-root data audit
uv run python -m guvolu.data.okx_l2_backfill --data-root data plan --symbol BTC-USDT --from-day 2026-08-01 --to-day 2026-08-10
uv run python -m guvolu.data.okx_l2_backfill --data-root data status --symbol BTC-USDT --from-day 2026-08-01 --to-day 2026-08-10
uv run python -m guvolu.data.okx_l2_backfill --data-root data run --symbol BTC-USDT --from-day 2026-08-01 --to-day 2026-08-10 --max-days 1
uv run python -m guvolu.data.okx_l2_materialize --data-root data audit --from-day 2026-08-01 --to-day 2026-08-10
uv run python -m guvolu.data.persistence_audit --mode quick --output logs/persistence-audit.json
```

长批次另开一个只读进度终端；它从 sealed manifest、活动 head 和工作进程现场
重算状态，不维护第二份进度：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/watch_okx_l2_backfill.ps1 `
  -Symbol BTC-USDT -FromDay 2026-07-12 -ToDay 2026-08-10 -IntervalSeconds 15
```

终端分别显示 `download=active+sealed` 与 `material=active`。`sealed` 只表示原始
压缩包已通过长度、SHA-256 和 manifest 封口，不能报成已物化；只有审计通过并
推进 `materialization_partition_head` 后才计入 `active`。监视器不写 SQLite、
不启动回补 worker，可以安全关闭并重开。

`archive-plan` 从覆盖表、实际文件和活动输入绑定现场推导，不使用另存进度文件。进程中断后重跑，完成月复用、未提交月重做、blocked 月继续隔离，因此是主恢复入口。

OKX L2 使用相同原则，但断点粒度为 UTC 日。`--max-days 1` 适合调度器逐日调用；
有界的 30 日热批次可以省略该参数，因为实现仍然逐日下载、物化、审计和提交，
不会并行展开多个日档。当前项目根目录的热层目标为最近 30 个完整 UTC 日：
具体已占用、历史日均、实时日增、暂存峰值与可用空间见日期质量快照；调度器按
当前实测重新计算所需永久空间，并在单日暂存之外保留磁盘安全余量。
5000 档与无映射品种保持阻断，不以命令参数绕过能力证据。附属冷盘接入后只迁移
封口原件与非活动旧 Parquet；复制、散列复核和 location 更新完成前不得删除热副本。

`reconcile-raw` 只处理最后一条有效 raw 已静默超过阈值、且仍无终态清单的 run。它逐有效 JSON 行重算计数并追加 `status=recovered_incomplete`、`completion_claim=false` 的恢复清单；已有 open checkpoint 保持原样，原 raw 不改写。它只能证明“现场现存多少有效行”，不能把异常退出改写成正常完成，也不能恢复缺失字节。

REST anchor 的 `recover-raw` 只处理内容完整、但未完成控制面提交的
单文件原件。它在任何写入前验证文件名/内容 SHA-256、分区、端点、
请求、响应与时间字段，重放时不改写原件，不使用事后 WS checkpoint，
且保留较新活动 head。`repair-control-ledger` 只增登记已通过结构、
attempt、字节数和散列验证的终态 manifest，以及有唯一主位置的旧
输入位置绑定。数据库中已失败的 attempt 对应清单登记为
`failed_materialization_manifest`，不改成完成；全过程不删文件。缺省命令只读
规划，`--apply` 幂等执行，之后必须运行 full `audit`。book-state v3
与 OFL v8 的新终态清单已在同一最终事务中正常登记，修复命令只
用于历史缺口。

版本滚动发布必须先让现有采集器优雅封口，再迁移/验证 schema v20 和端点注册，
随后启动 raw v3 采集、L2 normalization v5、实时逐笔 normalization v3、
book-state checkpoint v3 与 OFL v8 物化器。
旧 `.open` 段不得通过改写 schema 或补字段升级；
只有原采集器正常封口的段可进入旧版兼容重投影，崩溃尾段继续按
`recovered_incomplete` 隔离。守护任务在受控切换完成并确认单写者后再恢复启用。

## 9. 未决项登记

| 编号 | 问题 | 本文提案 |
|---|---|---|
| TBD-31 | 保活链与进程操作端点 | 第 1 至 5 节；实施随热力图交互批次 |
