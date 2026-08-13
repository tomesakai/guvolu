# ADR-0001：模块化单体优先

状态：Accepted  
日期：2026-08-05

## 决定

使用 Rust workspace 的模块化单体，独立进程仅为 gpu-worker、executiond、api-server 和可选 ingestd。

## 原因

单卡、本地大数组和高频内部调用不适合细粒度微服务；模块化边界、IDL 和制品引用已足够支持团队并行。真实交易和 GPU 崩溃需要进程隔离。

## 后果

不得以“低耦合”为由把 AST 节点、统计函数或 kernel stage 变成网络服务。未来水平扩展时保持契约再迁移。
