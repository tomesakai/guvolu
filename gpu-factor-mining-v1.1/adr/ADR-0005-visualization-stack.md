# ADR-0005：分层可视化技术栈

状态：Accepted with benchmarks  
日期：2026-08-05

## 决定

React/TypeScript；Lightweight Charts 用于价格主图，ECharts 用于统计图，Perspective 用于大表，PixiJS/WebGL2 用于 L2 heatmap，Grafana+OpenTelemetry 用于运维。

## 原因

单一图库无法同时覆盖金融时间轴、复杂统计图、流式表格、高密度深度纹理和运维告警。

## 待验证

L2 renderer 的 tile、帧率和浏览器兼容需要 UI-01 benchmark；WebGPU 不作为首版硬依赖。
