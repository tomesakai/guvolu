---
name: guvolu-rules
description: guvolu 量化交易项目的铁律与规范。在本仓库中编写、修改、审查任何代码或文档前必须加载——尤其涉及 GMO Coin API、密钥使用、下单/撤单/持仓、签名、风控限额、术语用词、文档与注释规范时。触发词：guvolu、GMO Coin、API 密钥、下单、委托、成交、持仓、挂单、撤单、平仓、余力、签名、量化、自动交易、回测、dry-run、kill-switch、文档、注释、术语、合规。
---

# guvolu 项目规则

**工作前先读 [SKILLS.md](../../../SKILLS.md)（仓库根目录）——本项目最高约束。**
编号域与文档清单登记于 0 号文档 [docs/00-rules-registry.md](../../../docs/00-rules-registry.md)（W-01）。
API 行为的事实依据在 [docs/2026-08-05-gmo-api-capability-report.md](../../../docs/2026-08-05-gmo-api-capability-report.md)，结论均来自实测。
架构锁定项与 TBD 台账在 [docs/architecture.md](../../../docs/architecture.md)——TBD 项可提出并实施提案，在台账标注即可；【已锁定】条目不得单方变更（A-05）。
文档或规则变更后须通过两项校验：[tests/test_doc_compliance.py](../../../tests/test_doc_compliance.py) 与 [tests/md_style.test.mjs](../../../tests/md_style.test.mjs)（W-03）。

## 必须记住的架构前提

两把密钥能力**完全正交**，这是实测确认的硬约束：

- `READ_ONLY`：13 个 REST 读取 + 4 个 Private WS 频道。**不能写。**
- `TRADE`：`order` / `changeOrder` / `cancelOrder` / `cancelOrders` / `cancelBulkOrder`。**无任何读取能力**（含自身挂单）。
- 未开通：`closeOrder` / `closeBulkOrder` / `changeLosscutPrice` / `account/transfer`。

GMO **无客户端自定义委托号**（2026-08-05 勘误核实）。本地 `intent_id` 先落盘，发送成功后持久化交易所 `orderId` 映射，两把密钥之间**关联键为 `orderId`**；同品种同时至多一笔在途写请求。

## 最高频引用的铁律

| 编号 | 内容 |
|---|---|
| T-02 | 两把密钥由不同 client 类持有，类型层面不可互换 |
| T-03 | 唯一真相源是 READ_ONLY；TRADE 的响应只证明「被受理」 |
| T-04 | 缺省模拟运行（dry-run），实盘必须显式开启 |
| T-05 | 下单前先落盘 `intent_id`，成功后立即映射交易所 `orderId` |
| T-06 | 写请求超时先查询再决策，**绝不盲目重发** |
| T-08 | 金额一律 `Decimal`，禁止 `float`（研究域例外见 G-05） |
| T-09 | 平仓权限开通前，代码级硬性禁止杠杆品种（`*_JPY`） |
| T-10 | HTTP 200 不等于成功，必须判 `status == 0` |

## API 实现要点

1. 签名串 = `timestamp + method + path + body`，`path` 以 `/v1` 开头（**不含** `/private`）。
2. **例外**：`PUT`/`DELETE /v1/ws-auth` 签名串**不含 body**（body 照发）。按通用逻辑实现必得 `ERR-5010`。
3. Private WS 地址是 `wss://api.coin.z.com/ws/private/v1/{token}`，漏 `/v1` 得 404。
4. WS 频道名：`executionEvents` / `orderEvents` / `positionEvents` / `positionSummaryEvents`。
5. 订阅后权限错误以 `{"error":"ERR-5012 ..."}` **消息帧**返回且不断连——必须解析，否则持续收不到数据。
6. 4 个历史端点路径为 `/v1/account/{deposit,withdrawal,fiatDeposit,fiatWithdrawal}/history`，且 `fromTimestamp` 必填。
7. 现物 symbol 是 `BTC`，杠杆 symbol 是 `BTC_JPY`——形态不同，不得用裸字符串混传。

错误码处置见 [docs/error-catalog.md](../../../docs/error-catalog.md)。

## 文书规范（W 章摘要）

- 代码注释一律中文正规书写，单处不超过二十字（W-04）。
- 禁用行业隐语、自造词、口语、比喻；新名词先入 SKILLS.md 术语表（W-05）。
- 禁用表情符号、装饰符号、重复标点、情感化表达（W-06）。
- 同一信息只在一处表达；时效快照文档命名带日期前缀（W-02、W-07）。
- 表格已足以承载对照与枚举，正文不加水平分割线；围栏须带语言标注，纯文本图示用 `text`（W-08）。

## 用语

严格遵循 SKILLS.md 第 7 章术语表。要点：**委托（order）不等于成交（execution）**；`amount`（总额）与 `available`（可用）语义不同，禁止笼统说「余额」。

## 自主执行的边界（A 章摘要）

代理可自主完成读取、研究、回测、模拟运行、对账，以及代码与文档修改。仅三类需人工确认：

- 切换实盘（T-04）
- 调整 T-11 硬顶
- 变更密钥权限配置

其余为记录义务而非审批环节：产生真实写请求的改动，在变更说明中列明所触碰端点（A-03）；API 行为结论注明实测或官方文档依据，存疑时以探测法验证而非依赖记忆（A-04）；密钥内容不得写入任何文件、输出、日志或提交（A-06）。
