# ADR-0004：真实执行为独立安全边界

状态：Accepted  
日期：2026-08-05

## 决定

只有 executiond 持有交易凭证并发送订单。researchd 只能提交签名 FactorVersion、PortfolioPolicy 和 SignalFrame。

## 原因

研究代码、GPU 崩溃、LLM 和 UI 不应获得直接下单能力。

## 后果

LIVE 需要 arm/disarm、kill switch、reconciliation、审计和独立权限；UI 只发送受控操作命令。
