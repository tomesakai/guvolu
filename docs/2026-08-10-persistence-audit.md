# 2026-08-10 持久化完整性审计

> 文档类别：时效快照。本文区分“未发现损坏”“已发现损坏”与“缺少证据”，不得将文件存在等同于端到端零丢失。

## 1. 结论

当前生产数据**不能声明历史零丢失**。全读 raw 定位到一条旧并发 writer 遗留的 ticker 尾片段；其缺失字段无法精确恢复。该行不被 OFL 的 trades/orderbooks 派生链消费，附近重复 recorder 也保留了完整市场状态，所以不影响现有 OFL 重建，但原始 ticker 帧已经不完整。

新写入链已经改为先持久化再解析：原始 WS 文本逐帧跨进程串行追加，`fsync` 成功后才计数；读取侧兼容新 `payload_raw` 与旧 `payload`。manifest、归档游标、瓦片指针和 SQLite 也补齐了对应崩溃边界。

## 2. 当前证据

| 数据面 | 已验证 | 不能据此证明 |
|---|---|---|
| raw | 全读 215,459 条有效行；1 条损坏行；14 组实际行数多于旧 manifest，无 manifest shortfall | 16 个旧 manifest 没有 durable 版本；损坏 ticker 无法还原 |
| archive | 52,841 个 gzip、2,639,753,155 bytes 全解压；CRC、行数、首尾时刻和状态对账错误均为 0；27 个旧 GMO 未登记文件已补登 | coverage 没有采集时 SHA-256，现有内容正确不能反证下载后从未被替换 |
| SQLite | `quick_check=ok`、外键错误 0；恢复探针提交、回滚与幂等均通过 | `trade_tick`、`book_top`、`stream_health`、`backfill_run` 仍为空，生产接线未被证明 |
| heatmap | 全读 289,172 列、572 chunks，daily/meta/current generation 一致 | 9 个旧 pointer 无原子版本；meta 无源 raw 内容散列 |

确定损坏位置：`data/raw/2026-08-08/ws_public.jsonl:42288`。内容只有 65-byte ticker 尾片段；缺少头部、run、source、source timestamp、ask、bid 与 last，任何补写都会伪造原文，因此保留现场并由审计报告错误，不修改 raw。

## 3. 恢复与压力结果

隔离探针写入 1,000 条，结果为：已提交行全部持久化、未提交事务全部回滚、重复重放不增行、原子替换保留旧完整值、撕裂 gzip 可检测且只恢复完整 member。quick 审计共索引 53,518 个文件、2,397,932,598 bytes，退出码为 `1`：未新增确定损坏，但仍存在证据盲区。

归档全读另建立当前压缩文件集合的 SHA-256 根：`66dc9abd4ee626bdaa891345df437dc7b3b6975a34b15185500a5093cdcfb75e`。它可用于检测今后集合是否发生变化，但不是交易所发布时的官方校验值。

运行命令：

```powershell
$env:PYTHONPATH='src'
python -m guvolu.data.persistence_audit --data-root data --mode quick --probe --probe-records 1000 --output data/export/persistence-audit-2026-08-10.json
```

退出码含义与日常运行方式见 [runtime-ops.md](runtime-ops.md) 第 7 节。

## 4. 最小稳健上线门槛

1. 接通 `trade_tick`、`book_top`、`stream_health` 与 `backfill_run` 的生产写入并跑一次跨层对账；空表不能视为“零异常”。
2. 为新 archive coverage 保存来源校验值或逐文件本地 SHA-256；旧归档已有集合 hash 基线，但仍需把逐文件散列写入可定位的台账。
3. 完结日 heatmap meta 保存源 raw 的大小与散列；重建后再核对 current generation。
4. 对旧无 durable 版本的 manifest 与 pointer 继续标 `legacy/unproven`，不通过改写历史来伪造保证。
5. 发布门禁要求 quick 审计无 error、恢复探针通过；首次生产接线与介质迁移后再跑 full。
