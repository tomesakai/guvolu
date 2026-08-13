# GMO Coin 与 bitFlyer 账户资产能力复核（2026-08-11）

- **文档类别**：时效快照（内容冻结）；后续变化以新快照修订
- **范围**：只读私有资产接口与控制面展示边界；未调用交易、撤单、出金或划转端点
- **方法**：使用两所的 READ_ONLY 凭据各执行一条资产读取请求；不记录凭据与金额值

## 1. 结果

| 来源 | 接口 | 结果 | 原始字段 | 控制面采用 |
|---|---|---|---|---|
| GMO Coin | `GET /v1/account/assets` | 可用 | `symbol`、`amount`、`available` | `symbol → amount / available` |
| bitFlyer | `GET /v1/me/getbalance` | 可用，返回 41 个币种 | `currency_code`、`amount`、`available` | `currency_code → amount / available` |

bitFlyer 的已配置读取密钥可访问 `getbalance`；既有 [bitFlyer API 能力实测](2026-08-07-bitflyer-api-verification.md) 的权限结论继续有效。当前查询服务此前只接入 GMO Coin 的私有资产 client；bitFlyer 已有公开行情、逐笔和盘口采集，不等于账户资产已接入控制面。

## 2. 展示边界

1. 总览仅按相同币种合计，并保留 GMO Coin 与 bitFlyer 的 amount / available 分列；不计算跨币种净值。
2. 交易所未返回某币种、私有端点未配置或读取失败时显示「—」并保留来源状态，绝不填零。
3. `getcollateral`、持仓、未实现盈亏及可取现等字段与资产表不同义，不混入 amount / available；需要时另设账户风险面板。
4. 读取密钥只用于读取路径。bitFlyer 的 TRADE 密钥现有出金权限，不可作为账户聚合的替代凭据。
