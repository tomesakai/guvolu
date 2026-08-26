# Git、数据、策略管线与实盘准入审计（2026-08-26）

## 结论

截至 2026-08-26 10:45 JST，项目结论为 `NOT_READY`，不得实盘。
已有研究结果可以支持继续做受控前向与 paper 验证，但还不能支持资金风险：
主远端没有任何可取回引用，OKX 历史 L2 的 58 个活动 Parquet 指向离线 E 盘，
多条衍生数据链陈旧，当前封存前向缺口率 97.56%，paper 有 1 个历史模拟决策但有效准入计数为 0，
执行安全、场景重放与长期浸泡证明均未形成。

本审计把“量化业界标准可用”解释为一套可复核的项目准入政策，而不是声称存在
单一的行业通用阈值。任何技术门禁全过后，系统最多输出
`READY_FOR_EXTERNAL_LIVE_APPROVAL`；人工实盘批准始终外置，检查器自身固定
`live_authorized=false`。

审计期间没有推送远端、没有执行 Git 垃圾回收、没有删除本地数据、没有读取密钥值、
没有执行 OKX 真实恢复，也没有启用 live。05:07 JST 发现调度仓的 dry-run 入口仍存在
环境模式绕过真实 sender 的 P0 后，冻结 shadow/paper 任务已立即禁用。09:35 的遗留 holdout
预检自然触发在参数绑定阶段失败且权威文件逐字节不变，随后也已可逆禁用。进一步审计已撤销
“旧 holdout 预检严格只读”的结论：旧治理 reader 会创建目录、以读写模式打开 SQLite 并进入
WAL/DDL/升级路径。当前 WIP 已改成 `mode=ro`、不建库、不迁移并完成独立复核，但辅助文件与
路径身份仍须由外层 wrapper 的三文件 guard、一致快照和 A-B-A 关闭；遗留任务最近结果为 1
且已 Disabled，不能把失败记录误报为可靠预检证据。

## 快照边界

- Git、工作树与任务状态：2026-08-26 00:00 至 10:28 JST；
- SQLite 完整性基线：2026-08-26 00:27 至 00:35 JST；增量行数与主要时效复核至 07:46 JST，bitbank trade checkpoint 复核至 10:45 JST，权威治理文件指纹观测至 09:35 JST；
- 磁盘空间复核：2026-08-26 10:19 JST；
- 最新成功 dry-run/paper 周期：2026-08-26 04:14:02 至 04:38:06 JST；
- 行情采集仍在运行，所以文件数、行数、时效和剩余空间是有界快照，不是常量。

## Git 现状

| 项目 | 证据 | 判断 |
|---|---:|---|
| 当前主线 | `main@eba92de` | 在行业准入 v4、L2 进程所有权上新增经独立复核的全拒绝经济研究代理 |
| 上游 | `origin/main [gone]`；`git ls-remote origin` 成功但返回 0 个 ref | 当前没有可验证远端备份 |
| worktree | 18 个；新增 `D:\dev\guvolu-ops-frozen-10dab`，E 盘冻结运行根仍为 `prunable` | 不能批量清理或假定都可达 |
| 研究 WIP | `codex/research-next@b6e392a`，4 个 tracked 修改、2 个 untracked | 约 1,030+/16-，且落后 main 82 个提交，必须独立保全和语义迁移 |
| 执行分支 | `codex/execution-chain@02f1e56`、`codex/paper-executor@6edce4e` | 同一执行补丁已在停止态独立提交并部署；仍未恢复调度 |
| 对象库 | 3,122 个 loose object、153.59 MiB；2 个临时垃圾对象、26.59 MiB | 连通性通过，但禁止在无远端备份时 prune/gc |

主仓当前仍有有意的未提交审计实现：冻结 shadow/preflight 编排与注册绑定、bitbank trade
静默重连和本审计文档。经济代理已在
独立最终复核确认无 P0--P3 后以 `eba92de` 单独提交；它仍固定全拒绝且没有任何执行权限。
L2 启动所有权修复已通过独立对抗复核、79 项定向测试、全仓测试、严格类型检查与
PowerShell AST 检查，并以 `10dab56` 提交。06:16 JST 从该提交建立干净 detached 运维树
`D:\dev\guvolu-ops-frozen-10dab`，两项市场数据守护在短暂禁用、脚本 AST 与部署树定向测试
通过后切换到该树并恢复；首个五分钟守护周期结果为 0。
调度仓已在任务停止态从 `c04f8b0` 更新为 `guvolu-exec@02f1e56`；同补丁的审查分支为
`guvolu-paper@6edce4e`。模式隔离、有效期、市场绑定、来源/目标内容寻址与血缘修复在部署
目录复跑 106 项执行测试和严格类型检查通过。冻结任务仍为 `Disabled`；main 没有真实
下单权限，也不能把代码部署误报为已形成新的 paper 准入证据。

最高优先级的 Git 风险不是代码冲突，而是恢复面：远端空、存在未提交的大型 WIP、
还有悬空对象。任何 merge、rebase、prune 或工作树删除之前，必须先建立可验证的
异机/远端备份，并为 WIP 生成独立 bundle 或提交。

对 `codex/research-next@b6e392a` 的增量只读审计确认，它含 4 个 tracked 修改（约
1,030+/16-）和两个未跟踪的 `interval_suite_holdout*.py`，治理实现仍以 schema v8 为基线，
而 main 已是 v9 且加入物理只读入口，因此不能整块 merge 覆盖。其单个治理测试文件在
`-B -X dev -W error` 下 30/30 通过，但收集到的 interval-suite 用例只有 5 项：覆盖 plan、
prediction、原子开始和两项 plan 校验，并没有调用两个未跟踪模块中的完整 holdout run 或终局
attestation。后续只能先保护这份无远端 WIP，再把套件合同逐项迁移到当前 v9+ 只读/写入分层，
补齐失败恢复、终局重建和独立复核；当前不得用它消费任何 sealed vintage。

10:28 JST 逐一读取 18 个 worktree 状态：除正在加固的 main 与上述 `research-next` 外，其余
15 个实际可达 worktree 都是 tracked-clean；E 盘冷存储 worktree 仍为路径缺失/prunable，不能
因此删除其 Git 元数据。main 当时有 16 个 tracked 修改、0 个 untracked，其中新增的
`segmented_raw.py` 是正在进行的统一采集耐久性修复，不代表已通过复核。

## 本地数据现状

主数据根完整快照为 267,525 个文件、49,427,147,311 B（约 46.03 GiB）。其中 raw
约 32.75 GB、materialized 约 11.28 GB、archive 约 2.67 GB；07:46 增量复核时 SQLite
主库为 2,364,542,976 B，另有 99,807,032 B WAL。D 盘实时 L2 原件已增至 27,038 个文件、
23,559,799,580 B，07:50 仍在增长。

07:49 JST 的只读进程盘点仍显示 3 个逻辑 L2 collector（GMO BTC、bitbank btc_jpy、
bitFlyer BTC_JPY）、7 个逻辑 trade capture 和 1 个 trade materializer 正在运行，
但没有 L2 materializer。Windows 的 venv launcher 与基础解释器父子对按一个逻辑 worker
计数，不能误报成两套采集器。06:16 新守护脚本首轮运行后再次盘点，逻辑 worker 的创建
时间与 PID 均未变化，没有重复启动；仍为 3/7/1/0。D 盘低于 20% 门禁期间只允许保持
已有采集，不启动 Full 或补开 L2 materializer。

07:55 对三条 L2 collector 各自最新 sealed segment 做独立只读核验：GMO `sequence=940`
（580 行）、bitbank `sequence=940`（873 行）、bitFlyer `sequence=939`（1,172 行）的文件
字节数、逐行计数和 SHA-256 均与 schema v3 manifest 精确一致。由此只能认定 raw capture
持续且最新封口完整；不能把它外推为规范化 L2、book state 或 OFL 已追平。

SQLite schema 为 20，35 个业务表；`quick_check=ok`，外键错误为 0。关键行数：

| 表 | 行数 |
|---|---:|
| `market_kline` | 3,781,423 |
| `trade_tick` | 1,353,575 |
| `book_top` | 190,977 |
| `artifact` | 203,827 |
| `artifact_location` | 204,311 |
| `partition_attempt` | 58,217 |
| `materialization_partition_head` | 20,082，审计时仍在增长 |

### 路由与可用性

`data/storage-roots.json` 把
`materialized/book_l2/schema_version=2/normalization_version=book-l2-normalization-v2`
路由到缺失的 `E:\guvolu-cold\v1`，但状态仍为 active。结果是 29 天、每一天
frame/level 成对的 58 个 OKX BTC-USDT 活动 L2 Parquet 全部不可用，登记总字节
15,438,105,544 B；相应 29 个 manifest 也不可达。另有 7 个 trade Parquet 缺失，
但它们不是活动 head，风险级别较低。

C 盘仍保存 29 个完整 OKX raw `tar.gz` 和 manifest，raw 登记总字节
3,622,395,110 B，因此可以离线、逐日、按原 attempt 重算。新增恢复路径只做计划内
缺失文件的重放和精确 SHA/字节比较，不下载、不登记新 attempt/head、不改路由；
同日 frame/level 使用稳定路径锁，第二个文件落盘失败会尽力补偿回移第一个文件，并
留下精确阶段/失败计数；掉电时仍不声称具有跨文件原子性。真实 58 项恢复尚未执行。

D 盘在 08:24 JST 的精确只读容量快照为总计 931.496090 GiB、空闲
171.346573 GiB（18.394771%），低于项目 20% 门禁；按当前已用空间至少
还需实际释放 14.952645 GiB，实操目标应不低于 16 GiB 以容纳持续写入波动。
恢复 backlog 前不能切换 Full。C 盘约空闲 110.61 GiB（23.79%）；清理 C
不能解决 D 盘门禁。实际恢复前还必须重新计算持久输出和单日临时
峰值后的 C/D 双盘安全余量。

09:57 JST 增量复核时，D 盘空闲 183,937,388,544 B（171.305042 GiB、18.390313%），
瞬时 20% 缺口已随持续采集增至 16,099,873,588 B（14.994176 GiB）；C 盘空闲
118,696,570,880 B（110.544796 GiB、23.778323%）。这不改变至少 16 GiB 只是瞬时下限、
长期 soak 仍需更大容量的结论。

10:19 JST 的再次只读快照为：D 盘空闲 183,928,291,328 B（18.389403%），距离 20% 仍差
16,108,970,803.2 B；C 盘空闲 118,644,453,376 B（23.767883%）。三项冻结/预检任务仍全部
`Disabled`，启用项仅为现有行情守护与登录采集；这次复核没有注册、启用或重启任何任务。

`14.94 GiB` 只是“现在这一刻”回到 20%，不是 paper 周期容量计划。08:01 对最近完整
24 小时的 864 个 sealed L2 segment 逐文件复核：数据、manifest 和锁文件逻辑量合计
1,808,686,199 B，但 Windows `GetCompressedFileSizeW` 显示实际分配 643,965,557 B，NTFS
压缩比为 35.60%。若仅按这一实际分配速率线性外推，30 天 raw L2 约需 17.99 GiB，100 天
约 59.97 GiB，尚未计物化输出、临时峰值和安全余量。若全程仍要求 D 盘不低于 20%，从
当前状态起理论最低释放/新增容量分别约 32.94 GiB（720 小时）和 74.93 GiB（100 天）。
因此 iCloud 的 18.01 GiB 选项只能解决瞬时门禁，不能单独支撑最短 paper soak；长期窗口
必须恢复可用冷存储、扩容或取得更大的可恢复空间，并建立不删除权威数据的归档容量方案。

本次容量核算使用 allocated bytes，并校正了 NTFS 压缩、云端占位符和
硬链接重复承诺。`D:\dev\guvolu-runtime` 原始 L2 逻辑量 21.85 GiB，但实际
分配只有 7.69 GiB；它与主数据 5.62 GiB、cold backup 7.01 GiB、frozen runtime
3.89 GiB 均是权威数据或当前恢复链，不得为跨线而删除，且单项也不足以安全越过
门禁。项目路径全量 LinkType 检查和代表文件 `fsutil` 检查未见硬链接名。

可恢复的非项目选项均需用户另行授权，审计期间未执行：首选是在 iCloud
客户端确认无待上传/同步错误后用“释放空间/移除下载”回收已同步的
18.01 GiB 本地副本，回滚为重新下载；或通过官方启动器卸载可重装的
Split Fiction（88.05 GiB）/ Epic Games 内容（108.87 GiB）。`D:\AI` 模型 31.91 GiB
只能在完成版本、下载地址与 SHA-256 清单后处置；`D:\dev\myfans-downloader`
29.70 GiB 属个人下载，需外部备份和逐文件散列核对。`D:\OneDriveTemp`
39.82 GiB 是高风险缓存/VHDX，仅可在确认同步健康后走 OneDrive 官方重置/清理，
禁止直接删除。

### 时效与质量

| 数据链 | 审计时陈旧度/状态 | 影响 |
|---|---:|---|
| REST L2 anchor | 约 4 分钟 | 新鲜，但不能代替连续 L2 |
| trade realtime 总体 | 约 7 分钟 | 总体可用 |
| bitbank BTC/JPY trade | 10:45 JST 约 78.5 小时 | 单所输入陈旧 |
| BTC L2 v5 | 约 90 小时 | 原始 L2 虽增长，规范化未追上 |
| book_state / OFL v8 | 约 90 小时 | 策略微观结构特征不可用 |
| market_status | 约 124 小时 | 运行状态特征陈旧 |
| OKX L2 | 约 383 小时且文件不可达 | 跨所历史证据不可用 |
| klines | 约 17 至 25 天 | 不能用于当前决策输入 |

bitbank trade 的进程存活和 `checkpoint_at` 每分钟更新掩盖了死连接：07:55 的 checkpoint
仍为 `sessions=1/reconnects=0/disconnects=0`，但 `last_wire_time` 停在 08 月 23 日 04:21 JST，
`last_data_time` 更早。源码对该 `ping_interval=None` 的 Socket.IO 路径在无限运行模式直接
`await recv()`，没有 L2 同类已有的 90 秒静默界限。第一版修复 WIP 虽通过 21 项相关测试和
strict mypy，但独立对抗复核已否决部署：join-room 与 pong 的 `send()` 仍可无限阻塞；任意
`42` 包中的非空 `room_name` 都可能伪造 data freshness；`writer.write_frame()` 的存储
`OSError` 会被网络重连分支吞掉。这三项均为 P1，另有握手静默预算、有限运行 connect/backoff
边界和协议包解析等 P2/P3。后续修复已按真实 Engine.IO open、Socket.IO connect 和
`42["message",{"room_name":"transactions_<symbol>","message":{"data":{"transactions":[...]}}}]`
合同重写：price/amount 只接受有界正定点十进制字符串，交易时刻只允许接收前 5 分钟至未来
30 秒，且至少一个正整数 transaction ID 必须推进跨重连的进程内游标；旧包、重复包、错误
room/event/envelope 仍先落原始层，但不能刷新业务 watchdog。connect/recv/send/pong/backoff
同时受 wire 和 run deadline 约束，TimeoutError 来源分离，持久化 OSError 越过网络 retry
原样失败关闭。cleanup 保持 body primary 优先，以 websockets 16 的公开
`transport.abort -> connection_lost -> close` 合同有界终止；若该不变量不成立则停止重连，
单次调用最多留下一个受观察尾任务。Python 无法安全强杀任意恶意 coroutine，这是明确残留边界。

冻结 WIP 的 `trade_capture.py` SHA-256 为
`ef335b8a33cfa728595024addb640cd6d8a467845f16dd0378053739666a7cb8`，测试文件为
`a89a113265a7e692af4ffa595ff4c81a6ebd24cf6618f264f8ea85507020c197`。目标测试连续五轮各
86/86，通过 15/15 async-debug/warnings-as-errors cleanup 对抗、92/92 相邻回归和 strict
mypy；真实库对象配合无网络内存 transport 证明 abort 路径，但按禁令没有连接 live bitbank
端点。修复仍须按固定哈希独立复核、单独提交和受控部署；现有 worker 尚未重启，所以本表继续
按陈旧输入处理，不能用 checkpoint 文件 mtime 充当 wire freshness。

10:45 JST 的同一运行 `runmt4lgdvz761b` 仍只有 537 records、1 session、0 reconnect；checkpoint
刚更新到 10:45，但 `last_wire_time` 与 `last_data_time` 分别已陈旧 78.395421 与 78.516254 小时。
这证明未部署修复前旧 worker 没有自愈，且本轮审计没有通过重启来掩盖缺口。

同一时点的其余 6 条 trade worker 中，GMO BTC/ETH/SOL/XRP 与 bitFlyer BTC 的 data age 为
0.2--1.9 分钟；GMO DOGE 则暴露另一类运行质量问题：当前 run 只有 6,178 帧，却已累计
2,033 sessions、2,037 reconnects、2,033 disconnects，最近数据约 28.5 分钟且连续失败 13 次。
原因是应用层 90 秒 `recv` 静默会重连，即使低成交品种的 WebSocket transport 仍可能通过
ping/pong 健康。后续需把 transport pong 与业务 data freshness 分开计量，健康 pong 不能伪造
data frame，但也不应每 90 秒制造握手风暴与限流风险。

10:48 JST 对全部 trade raw run 做不改文件的命名/终态盘点：14,370 个 final `.jsonl` 与
14,370 个 segment manifest 一一对应，未见 final orphan 或无数据 manifest；7 个 `.open` 正好
对应 7 个当前 worker。更高一层的 run 终态却不完整：49 个 run 目录中只有 10 个含
`run.manifest.json`；其余 39 个 checkpoint 均仍自报 `status=open`，扣除当前 7 个后，还有
32 个历史中断 run 没有 `.open`、也没有 run manifest，合计 8,511 个 sealed segment、509,934
checkpoint records、439,507,187 B。抽查的恢复尾段正确标为 `recovered_incomplete` 且
`completion_claim=false`，但整个 run 永久没有终局。这批数据只能逐 segment 按真实 manifest
消费；未来 reconciliation 必须登记 interrupted/recovered 事实，禁止事后伪造 `complete`。

D 盘 L2 同构盘点得到 53 个 run、24 个 run manifest；29 个无终态 run 中 3 个是当前 worker，
另 26 个为历史中断，涉及 5,005 个 sealed segment、4,554,301 checkpoint records、
9,279,842,919 B。当前 12,188 个 final L2 segment 与 12,188 个 manifest 也逐一对应，3 个
`.open` 正好属于当前 worker。由此可区分两层事实：已发布 segment 当前没有命名孤儿，但大量
run 级生命周期没有闭合；共同 reconciliation 必须覆盖 trade 与 L2，不能用 segment 数倒推
run completion。

逐份 manifest 搜索又得到 trade 32 个、L2 26 个 `completion_claim=false`，恰好与两类历史中断
run 数一一对应；即每个中断 run 当前各有一个 `recovered_incomplete` 尾段。这个对应关系是未来
reconciliation 的可复核输入，不是把 run 升格为成功的理由。

固定哈希终审随后又否决了这版部署。单调 `max(transaction_id)` 没有上界、epoch 或近期集合语义，
一个新鲜但异常偏大的 ID 会跨重连永久抬高 cursor，使后续真实流都不能续 data watchdog。扩大到
本地全部可读样本后，76,461 个唯一 ID 位于 1,234,811,233--1,235,502,315；当前样本内虽严格
递增，历史核验已见从 1 重新起算，官方字段又只承诺整数，故不能把本地最大值或观测 gap 硬编码
成协议上界。非法 UTF-8 binary 在 `write_frame` 前由 `to_text` 抛出，既没有保留原始 bytes，也不
进入网络重连集合。真实 websockets 16 的 abort 收尾允许十个事件循环轮次，生产路径却只让出一轮
就判为永久 cleanup invariant failure；全局尾任务集合也未按 recorder/event-loop/symbol 隔离。
指数退避在约第 1,024 次连续失败时还会先算 `2**attempt` 再截断，约 17 小时持续故障即可溢出并
永久退出。更外层的常驻 checkpoint task 若写盘失败不会主动打断 recorder，结束时又可能覆盖主
异常；cleanup 的 SystemExit/BaseException 还可能以 `status=complete` 封口。普通目标 86/86、
相邻回归 92/92 和 strict mypy 虽通过，完整目标在 `PYTHONASYNCIODEBUG=1 -X dev -W error` 下仍有
3 个 ResourceWarning 失败，均指向四项测试留下的未关闭 `.jsonl.open` writer。

继续下钻还发现共享 `SegmentedRawWriter` 在 durable append 前递增 record sequence、没有检查
short write；partial append 失败后外层 `finish()` 可能把含半行 JSON 的 segment 标记为
`completion_claim=true`。`os.replace` 成功后若散列或 manifest 发布失败，也可能留下未登记的
final segment。故下一版不能只把 cursor 换成近期去重集合；必须同步修复 trade/L2/OKX 的
checkpoint/终态仲裁和共同分段写入耐久性，再做独立复核。

K 线主键、OHLC 关系与 UTC 基本合同通过，但有 158 行未收束多小时/日线出现
`ingest_time < available_time`。所有消费者必须继续执行
`available_time <= decision_time`，不能用摄取时刻替代 PIT。

`trade_tick` 有 377 行 `size<=0`，均为 GMO BTC synthetic，进入研究前必须显式
排除或解释。`book_top` 没有 crossed/非法报价，但 28,609 行主要为 GMO 的
`ingest_time < available_time`，最大约 0.779 秒，符合来源时钟偏移而不是简单的
数据倒置；时间字段必须分开保留。

## 策略怎样生成

当前策略不是由 LLM 自由生成。生成器从六个手工、版本化、类型化模板出发：
`trend`、`flow_trend`、`breakout`、`price_breakout`、`mean_reversion` 和仅用于
shadow 的 `grid_shadow`。模板经 bounded grid、邻域展开、类型约束 mutation/crossover
变成 typed DAG / SearchPlan；Candidate Registry 在试验前以规范内容散列登记，
避免先看结果再决定是否计入多重检验。

```text
ResearchProposal
  -> 模板白名单与参数边界
  -> typed AST / Candidate Registry 预登记
  -> GPU SearchFast 宽筛
  -> CPU f64 exact 一致性与全试验台账
  -> expanding walk-forward + 24h embargo
  -> FDR / DSR / CSCV-PBO / circular block bootstrap
  -> 参数邻域、成本与容量门禁
  -> proposal-only candidate config
  -> sealed forward holdout
  -> paper soak 与对账
  -> 外部人工 live approval
```

搜索循环累计评估 8,462 个候选，1,028 个通过 F1，256 个通过 F3 GPU/CPU parity，
最终只提出一个 trend 邻域配置
`config/strategy_research_candidate_417aaf574008.json`。该文件只是 proposal；上次
CPU 尝试只留下配置/血缘，没有完整 summary、manifest 和 trial ledger，因此未获
资格，也未自动改写正式配置。

## 当前最强研究证据

最近一个完整、clean、decision-grade 的 CPU 运行是
`research-run-14c57fe...`，代码身份 `356b45e`，单一 GMO BTC/JPY、1 小时柱、
66,314 根、9 个候选、486 个 trial、26 个 expanding folds。

| family | validation OOS Sharpe | deployment OOS Sharpe | 最大回撤 | PBO | bootstrap p | DSR 概率 | FDR q |
|---|---:|---:|---:|---:|---:|---:|---:|
| `price_breakout` | 1.102 | 1.260 | 0.268 | 0.0059 | 0.0049 | 0.9973 | 0.0147 |
| `trend` | 0.772 | 0.970 | 0.432 | 0.3203 | 0.0380 | 0.9677 | 0.0352 |

研究权重为 price_breakout 0.5188、trend 0.0812、reserve 0.4；运行权重仍为 0，
原因码是 `feature_snapshot_stale` 与 `strategy_data_stale`。price_breakout 的统计证据
明显较完整；trend 的 PBO 0.3203 超过当前项目 0.2 门禁，不能因为组合均值看起来
可接受而忽略。

该研究已经具备 PIT、内容寻址收据、embargo walk-forward、FDR、PBO、bootstrap、
DSR、参数邻域和成本回放等正确主线；仍缺少跨市场/跨 venue 重复、alpha/beta、
CVaR/尾部、回撤持续时间、容量曲线、流动性/延迟/部分成交/排队/逆向选择压力，且
当前约 10 bps 的固定成本与静态容量假设不足以支撑实盘。

## 冻结前向与 paper

原计划任务通过未受版本控制的 `D:\dev\guvolu-ops-alt\run_frozen_forward_c698.ps1`
间接启动。该 UTF-8 无 BOM 文件在 Windows PowerShell 5.1 下被错误解码，首行中文
注释末字节吞掉下一行 `$Wrapper` 赋值，最后把 `-PlanId` 当作 `-File` 路径，形成
`0xFFFD0000` 且没有有效运行日志。

新增注册器直接绑定仓库内 `scripts/run_frozen_shadow_task.ps1`。00:39:35 至 01:07:21
JST 的首个受控周期仅做 dry-run；01:10 周期的 paper 正确拒绝已过期目标。02:25 与
03:10 周期又因输入没有及时形成新完整柱而分别以预测年龄约 56 分钟和 83 分钟失败。
这些失败没有通过延长目标有效期来掩盖：面板 `decision_time` 仍是闭合柱末端，跨缺失
整柱沿用旧信号会破坏回测、holdout 与执行同构。

04:14:02 至 04:38:06 JST 的新周期首次完整通过。GMO BTC 原始 segment 816 含
04:04:05 JST 事件并在 04:06:16 封口，04:12:44 完成 materialize，随后冻结快照校验
`quick_check=ok`、外键错误 0，包含 3,582 个输入、1,647,210,724 B。输入收据为
`trade-input-receipt-sha256-aa551c...`，源成交 19,702,297 行、经济合格成交
19,462,492 行；`volume_qualified=false` 被保留为质量事实，不能伪报为完整成交量覆盖。

本轮登记 04:00 JST 决策预测 `frozen-forward-prediction-950cd0...`，aggregate target
0.587847，生成时年龄约 38 分 03 秒，低于当前 45 分钟编排硬限。dry-run 终态为
`DRY_RUN_BLOCKED`，只读触碰 symbols/ticker/status，`write_touched=[]`。paper 使用独立
内容寻址目标 `target-4f5df4...`，经公开 orderbook/symbols 建模为一笔 BUY 0.00002 BTC
的模拟成交：模型价 12,564,721 JPY、名义 251.29442 JPY、费用 0.125647210 JPY、
总成本 5.000795879 bps，模拟持仓从 0 变为 0.00002 BTC。它没有私有请求、真实资金或
交易所委托，`write_planned=[]`、`write_touched=[]`。

对该 paper 样本使用正式 loader 重放认领账、意图账、持仓账和差异账：1 次认领、
1 个意图、`RECORDED -> SENDING -> PAPER_FILLED`、1 条差异行和 1 条持仓行完全一致；
报告、预测和两个目标的 SHA-256、费用/名义/成本算术均通过，核验前后文件散列与 mtime
不变。覆盖层仍是 `complete=false`：REST anchor age 不可得，top-5 bid depth 低于阈值，
因此该样本只证明纸面账链能闭环，不证明微观结构覆盖或成交模型已充分。

后续 runner 对抗复核还确认，这份报告只自报 half-spread、impact 与 slippage 数值，没有绑定
bid/ask、touch 或 order-book snapshot 的内容身份，因而无法独立重放成交成本。该样本必须从
G4 有效计数中剔除；在执行生产者升级为可重放的规范成本证据并通过独立核验前，有效 paper
累计仍为 0 小时、0 个决策。

独立审计同时发现两类执行缺口：dry-run 意图账没有继承预测 ID、决策时刻与目标
correlation ID；更严重的是调度仓入口只校验目标 `mode=dry-run`，却可在进程环境为 live 时
构造真实交易 sender。后者已由隔离替身实证为可调用一次 `.order()` 的 P0。尽管 04:14 样本
实际为 dry-run/paper 且 `write_touched=[]`，不能以这次没有触发来替代代码级保证。

因此任务于 05:07 JST 禁用；06:49 复核现用 `f8981e8826b4` 与更早的 `61c0c4cab6c5`
两项冻结任务均为 `Disabled/Enabled=False`。任务信息仍显示 07:14/07:10 的后续时点，
但禁用状态不会执行这些时点。隔离的 `guvolu-paper` 候选补丁已移除真实
sender 可达路径，把 dry-run/paper 的目标来源重建统一到 adapter 的单一字节合同，并补齐
模式、有效期、版本化市场/品种/预算、内容寻址和 correlation 血缘的公共入口失败关闭测试；
reconcile 拒绝任何动态目标，soak 只允许固定零目标基础设施路径，避免形成旁路消费者。
该补丁经独立复核未留 P0--P3，并已在停止态合入 `guvolu-exec@02f1e56`。

随后对 shadow 编排层的独立复核又发现两组 P1：预测器已经返回权威
`prediction_sha256`，编排却忽略它并重新“册封”路径现状；以及只凭报告文件存在就复用，
会把散列后换位或“已写报告但子进程非零退出”的失败，在下次运行误报为成功。目标 adapter
返回的 SHA 也未被消费。当前冻结 WIP 已把 source/target/report 改成内容寻址 commitment，
把旧回执在任何业务 child 前按 schema v5 拒绝，公开/私有/CLI 的 paper 入口全部固定失败关闭，
并绑定空 child 环境、执行 venv 清单、Git/Python 身份、Windows Job/有界管道与超时清理。定向
runner 119/119、注册/包装 28/28、strict mypy、编译与 PowerShell AST 在实现方均通过，但还不能
提交或部署。

固定哈希独立终审发现该实现仍有确定性 P1。`_ISOLATED_RUNPY` 把可写 `src` 与 venv
`site-packages` 放在标准库之前，而前后 Git/venv 清单只检测窗口端点，新增后删除的模块可在窗口
内被导入后消失，形成 import-membership A-B-A。更直接的是仓库检查主动把 ignored `.pyc/.pyo`
视为安全；`-B` 只禁止写字节码、不禁止读取 sourceless legacy bytecode，因此在 clean HEAD 下
预置 `src/argparse.pyc` 即可能遮蔽 predictor、adapter 与 dry-run 都会导入的标准库模块。修复必须
取消字节码豁免，并让子进程只从启动前内容寻址、受固定句柄保护且默认拒绝新增成员的 import
closure 取代码；单纯增加前后清单或目录句柄不充分。终审尚在核对 v4 时序、timeout 与注册回滚，
故即使执行仓 P0 已部署，也不允许恢复调度。完成后仍须以新的 clean plan/vintage 生成受控样本，
不得复用旧报告充当准入证据。

任务原配置为每小时第 14 分、IgnoreNew、StartWhenAvailable、允许电池、不中途因电池停止、
WakeToRun、45 分钟上限和失败重试，但 Principal 为 `InteractiveToken`：用户登出后不具备
100 天连续无人值守能力。日历跨度不能替代实际成功决策、账本、对账与错误率证据；在没有
另行批准并安全配置专用运行身份前，注册器必须明确标记
`unattended_coverage_capable=false`。权威前向治理库按既定 TBD-37 隔离设计位于
`D:\dev\guvolu-frozen-runtime-356b45e\data\research\governance.sqlite3`；主仓开发治理库不登记
该 frozen plan，不能误在主仓库上续写或创建 replacement vintage。权威库当前 plan 有
3 条冻结预测，批量制品验证通过；另有 18 条属于已留痕废弃的前代 vintage，不能与当前
覆盖率合并。`holdout_evaluation_attempt` 仍为空，当前 sealed vintage 尚未开始正式评估。
07:52 的查询快照显示库文件 147,456 B、SHA-256
`aaf8bde528cf4a853f677580d37e193944519a6f17523e2a79940da174870cd3`，schema/write ceiling
均为 9、`quick_check=ok`、外键错误 0；2 个 vintage、2 个 plan、21 条总预测和 0 个评估
attempt 均未变化。
04:47--04:51 JST 的深度预检以 2 小时尾部宽限计算
41 个应有时点，只覆盖 1 个，缺 40 个、缺口率 97.56%，状态仍为 `degraded`。另外两条较新
预测尚未进入该评分网格；即使随后计入，也无法填补历史空洞。`zero_exposure` 只使缺口按零
暴露计分而不是烧毁 vintage，不授权事后补写；更严格准入仍要求新的 clean plan/vintage 和
完整高质量前向证据。07:50 复核时旧 `guvolu-holdout-preflight` 仍为
`Enabled=True/Ready`、计划 09:35 触发，但最近结果为 3，action 仍绑定正在加固的主工作树；
对应 08 月 25 日 09:35 的 scheduler 记录是运行根不可达并失败关闭；D 根恢复可达后的受控
查询曾生成 `degraded` 报告，但后来确认其底层治理连接并非物理只读。因此它不能作为可靠
预检证据。替代 wrapper 与注册器
正在按 code/data 根隔离、clean detached
HEAD 和原生 Disabled 回读合同修复，完成独立复核前不得重新注册或据此开启 shadow/paper。

09:25--09:35 JST 又对该遗留任务完成一次不干预的自然触发观测。触发前权威 DB 为
147,456 B、SHA-256
`aaf8bde528cf4a853f677580d37e193944519a6f17523e2a79940da174870cd3`；空 WAL 的 SHA-256 为
`e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`，32,768 B SHM 的
SHA-256 为 `fd4c9fda9cd3f9ae7c962b0ddf37232294d55580e1aa165aa06129b8549389eb`，rollback
journal 不存在，scheduler JSONL 为 3,952 B、SHA-256
`e378a62b036edd72fcc950f348afc430a7b33b2645cec5d08536cdd24888c85d`。任务在墙钟
09:35:00 进入 Running，约 2.4 秒后回到 Ready、结果为 1；Task Scheduler 回读的
`LastRunTime` 为 09:35:35，故不能用该字段推断实际开始秒。旧 action 没有提供当前 wrapper
新增的冻结 Python/Git/code/runtime 身份必填参数，观测到的快速失败与参数绑定阶段失败一致；
最重要的是上述五项文件身份在运行前、运行中、运行后逐字节不变，未产生业务日志。主机的
Task Scheduler Operational 日志原本未启用，本次没有为了取证修改系统日志配置。确认任务
已退出后，于 09:35:24 JST 可逆禁用；回读为 `State=Disabled`、`Enabled=False`、结果 1，
且禁用后的五项文件身份仍完全一致。没有注册、启用或手动运行任何替代任务。

预检还存在两项必须同时关闭的 P1：旧 wrapper 把可变 C 盘仓库同时当成业务 `--root`，且未
显式绑定 D 盘权威 registry；而 `governance._connect(write=False)` 在当前 schema 等于 ceiling
时仍走普通读写连接、`PRAGMA journal_mode=WAL`、DDL 与升级检查。wrapper 的运行前后散列
只能发现写入，不能阻止写入。故旧任务与替代任务都必须保持 Disabled，直到业务根明确为
D 盘 frozen runtime、权威 registry 以绝对路径绑定，并以 SQLite `mode=ro`/`query_only` 或
经 online backup 取得的一致性隔离快照证明 DB、WAL、SHM 的身份在预检前后不变。
SQLite 官方合同明确区分 URI `mode=ro` 与 `immutable=1`；后者假定文件绝不会并发变化，
不适合直接套在活动 WAL 库上。WAL 只读还要求既有 `-wal/-shm` 可读、允许创建它们或使用
immutable。因此调度层除 `mode=ro` 外还须持有 Windows no-write-share 文件/目录 guards，
不能只做事后散列比对：[SQLite URI filenames](https://www.sqlite.org/uri.html)、
[SQLite WAL read-only databases](https://www.sqlite.org/wal.html#read_only_databases)。

真实 Windows 对抗试验进一步缩小了可接受合同：目录 `R|H` oplock 能在目录自身 rename/delete
前 break 并阻塞换位，但不能阻止在目录内瞬时创建新子项；子项创建可以先成功，随后才由 oplock
报告 break。因此 sidecarless authority 不能进入 business 窗口，DB、WAL、SHM 必须三者预先
存在、均为非 reparse 普通文件、身份稳定并各自成功持有 no-write-share 读句柄；缺全部或只缺
一个 sidecar 都必须在 `business_invocation_attempted=false` 时失败关闭，rollback journal 始终
拒绝。`CancelIoEx` 后也必须用 `GetOverlappedResult(wait=true)` 等待异步 oplock I/O 真正完成，
再释放 `OVERLAPPED` 和 native buffer，不能取消后立即 free：
[FSCTL_REQUEST_OPLOCK](https://learn.microsoft.com/en-us/windows/win32/api/winioctl/ni-winioctl-fsctl_request_oplock)、
[CancelIoEx](https://learn.microsoft.com/en-us/windows/win32/api/ioapiset/nf-ioapiset-cancelioex)。

SQLite online backup 还会继承 source 的持久 WAL journal mode；若直接把该文件当 disposable
snapshot，治理 reader 会在 snapshot 旁创建自己的 WAL/SHM，而只绑定主 snapshot 的 guard
无法覆盖它们。故 destination 在 backup 后、进入 `query_only` 前必须强制并验证
`journal_mode=DELETE`，关闭后和业务前后都要断言 snapshot 的 WAL/SHM/journal 不存在；主
snapshot 自身还须完成 exclusive/non-reparse 创建、SHA-256/文件身份 A-B-A 和 no-share 读绑定。
SQLite 官方文件格式把 header offset 18/19 定义为 read/write version，`1` 为 rollback、`2` 为
WAL：[SQLite database file format](https://sqlite.org/fileformat.html)。这些是当前替代 wrapper
正在关闭的 P2 组合边界，修复与独立复核完成前仍不可注册。

治理 reader 四文件已完成独立只读终审且起止指纹未漂移：`governance.py` SHA-256 为
`66c37bee898072e2bed5e2a6761546b0f3e449e7f2dba1fe73fa0a58fd74c6fd`，58 项定向测试与
source strict mypy 通过，主 DB 只读合同内无 P0/P1。终审同时复现了 resolve/is-file 到 connect
之间的路径换位，以及只读连接创建/修改 WAL/SHM；故这不是单文件“authority-wide 物理只读”
证明，必须与上述外层合同组合后再做一次整体终审。三个测试文件另有 9 个既存 strict-mypy
错误，集中在未修改的 fixture 类型，不影响运行结果，但应作为后续测试基线债务单独清理。

治理迁移尚未执行，预先验证的顺序是：先提交并在 clean detached code root 复跑 runner；确认
preflight 与两条旧 shadow 均 Disabled 且没有残留进程；随后用 SQLite online backup 配合
`BEGIN IMMEDIATE` 保护分别生成 PRE/POST 备份，核对 SHA-256、逻辑 dump、schema、
`quick_check` 和外键，禁止对活动 WAL 库直接 `Copy-Item`。当前 sealed vintage 没有 evaluation
attempt，故只能以“3 条 runner 缺陷样本保留作审计、不得计分”的明确理由留痕 abandon，再从
D 盘 frozen runtime 的既有 research summary/config 建立新窗口
`[2026-08-27T00:00Z, 2026-12-05T00:00Z)`。权威 CLI 预计算身份为：

- vintage：`holdout-vintage-0f0a9a54459b1095f3ded040e0bf7a6f885705ac574891c9df36f28471961d73`；
- plan：`frozen-forward-plan-e3fa0c858896666f7a6b755df642b5aed9036381ee31dc17450b7c26f28d7f00`；
- task：`guvolu-frozen-forward-7c26f28d7f00`。

注册后仍必须是原生 Disabled，先做受控手工样本，再按实际有效账本累计；上述确定性身份不是
启用许可，也不能从修复后的 main 重新生成研究结果来改变已封存来源。

## 经济研究代理

新增 research-only 经济研究代理核心，负责：追加式 hash-chain 经济观测、
`event/available/ingest/decision` 四时点分离、修订链、按 `available_time` 的 as-of
重放、growth/inflation/rates/liquidity/fx/risk 六维 regime、missing/stale/partial
质量状态、模板/参数/holdout 门禁，以及 model/prompt/input/output 身份和已提交拒绝
尝试的内容寻址回执。v1 全拒绝期间不启用 proposal 接受配额与 trial budget 分配；
合同非法的批次会在生成回执前整体失败，也不会封存未知原始字段。

它默认不调用 LLM、不联网、不读取密钥，没有 TRADE、配置修改、注册、晋级或下单
权限。当前 v1 无条件加入 `holdout_governance_unbound` 并拒绝全部合同合法提案，只输出
有运行台账 commitment 的拒绝回执，不产生可供 SearchPlan 消费的 accepted proposal。
实现现已在路径锁内绑定观测台账 `sequence/head_sha256` 前缀并逐字节重建 context；
提案时会拒绝前缀之后在 decision 时已可知的尾记录，并持有观测锁直到 commitment。
评估时刻来自内部可注入壁钟，但该壁钟不具治理可信性，只能进入全拒绝回执；PIT 只以
`available_time` 判定，`ingest_time` 不承担防未来职责。无论本地 holdout 边界是时间还是
`null` 都会失败关闭，直到未来直接绑定 market 与 governance SQLite 的 sealed vintage。
因此它不是策略生成许可。

经济代理的两本 JSONL 台账以固定 canonical inode 原位追加，并在最终提交前保持可回滚；
观测锁覆盖到运行台账提交点，提交前重验所依赖的观测前缀。只读入口不会为缺失的 POSIX
锁文件制造状态，首次建账、短写、路径换位、hardlink、junction、数据 fd 与外层
mutex/flock/父目录清理异常均失败关闭；已越过持久提交点的 cleanup 后效不会诱导调用方
重试一个其实已经成功的写事务。Windows 原生 `CloseHandle` 与 `ReleaseMutex` 的零返回也
会显式转为错误，同时已有主异常不会被次生清理异常遮蔽。最终定向测试共 62 项，61 项在
当前 Windows 环境通过，1 项 POSIX 专用分支静态复核并按平台跳过；strict mypy、diff check
通过，独立复核结论为无 P0--P3。该结论只允许提交 research-only 实现，不授权新样本、
策略晋级、任务启用或 live。

## 项目准入检查器

只读、fail-closed 检查器现已加固为 `industry-strategy-readiness-v4`。它检查：

1. clean/decision-grade CPU manifest 和必需制品；
2. 候选完整指标；
3. 全局 trial ledger、FDR、DSR、PBO、bootstrap 和邻域稳定；
4. 尾部、压力、容量、成本与基准；
5. 封存前向终态、制品散列、决策网格和覆盖；
6. paper 时长、决策数、差异账、对账和零真实写；
7. 执行限额、熔断、超时/双通道对账、独立 kill switch 和权限隔离；
8. 永远外置的人工实盘批准。

v4 不再读取 summary 内可随手填写的场景数组。tail、stress、cost 和 capacity 必须来自
research manifest 中名为 `industry_evidence` 的内容寻址制品；路径、SHA-256 和字节数先由
既有完整性验证器复核，基础研究还必须通过 `verify_research_run` 全量语义重建。检查器随后从单次
字节快照重新核对 manifest、summary、config、candidate registry、trial ledger、行业证据和其
来源散列，避免完整性检查后路径被替换。制品精确绑定格式合法且非空的 run、research identity、
config、input receipt、decision time 和每个 paper-eligible family/candidate。

每条场景的顶层、`parameters`、`metrics`、`coverage`、`source_artifact` 都是 exact schema，未知或
缺失字段一律拒绝；同时必须具有可复算内容身份、方法白名单、锁定选择、仅 walk-forward OOS，并
满足 `from < to <= available_through <= registered_at <= decision_time`。语义去重只使用代码允许
的经济维度：tail 是概率与 block length，stress 是政策登记的冲击定义与 severity，cost 是成本
档位和四项分解，capacity 是名义金额、参与率、观测深度与 impact；改 nonce、展示名、
覆盖率或结果数字不能伪造多样性。普通 panel 不能作为 L2 capacity 来源。

尾部、压力、成本的 Sharpe、净收益、回撤和周转率均有项目阈值；expected shortfall 明确定义为
收益单位，必须位于 `[-1, 0]` 且不低于 `-0.2`。成本的 fee、half-spread、slippage、impact 和
total 必须有限、非负且合计一致，总成本必须为正；网格必须包含 10 bps 政策基线，并按
`policy_baseline/adverse/severe` 至少 5 bps 严格递增，成本上升时固定 target 的净收益不得改善。
FDR、PBO、bootstrap p-value、DSR probability 与邻域比率全部严格检查为有限的 `[0, 1]` 数值。

检查器当前不能从四类来源重放场景数字：tail/stress 没有代码级结果重建器，cost 虽有基础成本
回放模块但尚未生成并复核本合同的固定档位场景，capacity 也没有从 L2 depth 逐项重建 notional、
participation 与 impact 的独立实现。仅验证这些来源的 path/kind/bytes/SHA-256 只能证明文件身份，
不能证明数字语义，因此不作为充分准入证据。

v4 政策把 `industry_evidence_generator_status` 和 `scenario_source_replay_status` 都固定为
`not_implemented`，允许的 generator 与 attestation method 清单为空。即使有人自报预期
generator/method/code magic、伪造 `numeric_replay_verified=true` 并补齐所有 JSON，仍会得到
`INDUSTRY_EVIDENCE_GENERATOR_NOT_IMPLEMENTED` 和
`INDUSTRY_EVIDENCE_SOURCE_REPLAY_NOT_IMPLEMENTED`，不能合成 READY。未来只有在仓库实现独立
generator、把实际 code/bundle 作为 manifest 内内容寻址制品绑定，并为四类来源完成可复算语义
验证后，才能以新政策版本登记；不能只解除状态字段。正式入口仍只接受代码登记的 policy ID 与
文件散列。旧 `capacity_score` 仅保留为诊断，因此真实管线继续 `NOT_READY` 是预期行为。

01:43 JST 的正式 v4 结果是 `NOT_READY`、`live_authorized=false`。研究 manifest 与候选基础
指标通过；统计门禁因 trend PBO 0.3203 和 price_breakout 仅一个参数邻居失败；成本、
压力与尾部场景数均为 0；当前 sealed vintage 覆盖率仅 0.0004167 且无 consumed 终态；
paper 为 0 决策、0 账本、0 对账；execution safety attestation 缺失，六项控制与三项权限
隔离均未证明。04:14 后虽新增 1 个经正式 loader 重放一致的历史 paper 样本，但其成本未绑定
book/touch 内容身份，必须从有效累计中剔除；也尚未重新生成一份完整 v4 结果，不能手工改写
旧报告。检查器没有写文件、联网或做自动 promotion。

## 从现在到 live 的顺序

| 阶段 | 必须完成的可验证出口 | 当前状态 |
|---|---|---|
| G0 恢复面 | 非空远端/异机备份；WIP bundle；禁止依赖 dangling object | 未完成 |
| G1 数据面 | OKX 58 项离线精确恢复；路由复核；D 盘 >=20% 且覆盖 soak 容量；bitbank trade 静默重连；L2/OFL/状态追平 | 未完成 |
| G2 研究面 | proposal 完整 CPU exact；尾部/容量/成本/延迟压力；跨 venue 重复 | 未完成 |
| G3 前向 | sealed vintage 终态、制品全验、项目政策要求的预测覆盖 | 旧 vintage 已降级；须新建 |
| G4 paper | 至少 720 小时、500 个决策/账本/对账、错误率 <=1%、零真实写 | 0/500 个有效决策、0 小时；历史 1 个模拟样本的成本缺少可重放 book/touch 身份，只能作账链诊断；任务停用等待 runner 与执行生产者合同修复；`InteractiveToken` 也不能证明无人值守覆盖 |
| G5 执行安全 | 独立权限、硬限额、熔断、kill、超时与双通道对账 attestation | 未完成 |
| G6 live | 外部人工批准后才可做最小 canary；可撤回、可降级、持续监控 | 未授权 |

禁止跳级：研究 Sharpe 不能替代前向覆盖，前向通过不能替代 paper，对账通过也不能
替代外部 live 授权。

## 外部方法与监管锚点

- 模型风险治理按美联储 2026 年修订指导的概念健全性、独立验证、持续监控、有效挑战
  和治理职责映射：[SR 26-2](https://www.federalreserve.gov/supervisionreg/srletters/SR2602.htm)、
  [完整指导](https://www.federalreserve.gov/frrs/guidance/supervisory-guidance-on-model-risk-management.htm)。
- 算法交易控制按 MiFID II Article 17 的容量、阈值、错误订单阻断、连续性、充分测试
  和监控映射：[ESMA Article 17](https://www.esma.europa.eu/publications-and-data/interactive-single-rulebook/mifid-ii/article-17-algorithmic-trading)。
- 实时监控、kill 和两道防线参考 2026 ESMA supervisory briefing：
  [官方 PDF](https://www.esma.europa.eu/sites/default/files/2026-02/ESMA74-1505669079-10311_Supervisory_Briefing_on_Algorithmic_Trading_in_the_EU.pdf)。
- 自动化预交易控制参考 SEC Rule 15c3-5 FAQ：
  [SEC Market Access Rule FAQ](https://www.sec.gov/rules-regulations/staff-guidance/trading-markets-frequently-asked-questions/divisionsmarketregfaq-0)。
- 日本加密资产系统与风险监督参考金融厅监督指针：
  [金融商品取引業者等向け監督指針](https://www.fsa.go.jp/common/law/guide/kinyushohin/03.html)。
- 回测过拟合与多重尝试证据分别以
  [CSCV/PBO 原论文](https://escholarship.org/uc/item/4hn4t174) 和
  [Deflated Sharpe Ratio](https://doi.org/10.2139/ssrn.2460551) 为方法锚点。

这些来源提供治理和验证原则，不替本项目背书，也不证明任何候选未来盈利。
