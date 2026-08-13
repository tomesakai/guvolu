# 16 官方资料、标准与原始论文

版本：1.1-draft  
日期：2026-08-05  
文档性质：参考性  
主责：架构负责人  
前置文档：无  
主要消费者：所有文档  

外部依据编号见 `16_references.md`。原始 v1.0 文档为本次重构的需求基线。


## 证据与决策状态

本文档集不把尚未实现或尚未实测的内容伪装成事实。关键结论使用以下状态：

| 标记 | 含义 | 可否直接作为实现事实 |
|---|---|---|
| `VERIFIED-SOURCE` | 已由官方文档、标准或原始论文核验 | 可以，但仍需固定引用版本 |
| `VERIFIED-CALC` | 可由确定性算术、组合数学或数据规模直接推导 | 可以 |
| `DECISION` | 本项目已经选择的架构语义 | 可以，变更须走 ADR |
| `POLICY` | 项目阈值或治理规则，不是普适真理 | 可以配置，不得宣称外部有效性 |
| `TO-BENCHMARK` | 只能在目标 RTX 5070、驱动、CUDA 与操作系统组合上测得 | 不可以预填性能结论 |
| `TO-RESEARCH` | 证据不足或依赖尚未选定的数据源、市场、券商或交易所 | 不可以进入生产默认值 |

出现冲突时，优先级为：正式契约与 ADR > 本文档正文 > 示例代码。外部资料只证明其明确支持的事实，不自动证明本项目的设计阈值。


## GPU 与运行时

- `[R-NVIDIA-RTX]` NVIDIA, *NVIDIA RTX Blackwell GPU Architecture*, RTX 5070 规格表。https://images.nvidia.com/aem-dam/Solutions/geforce/blackwell/nvidia-rtx-blackwell-gpu-architecture.pdf
- `[R-NVIDIA-CC12]` NVIDIA CUDA Programming Guide, *Compute Capabilities*, CC 12.x 的 resident threads、registers、shared memory、FP32:FP64。https://docs.nvidia.com/cuda/cuda-programming-guide/05-appendices/compute-capabilities.html
- `[R-NVIDIA-OCCUPANCY]` NVIDIA CUDA Runtime API, occupancy APIs。https://docs.nvidia.com/cuda/cuda-runtime-api/
- `[R-NVIDIA-NVRTC]` NVIDIA NVRTC Documentation。https://docs.nvidia.com/cuda/nvrtc/index.html
- `[R-NVIDIA-SANITIZER]` NVIDIA Compute Sanitizer。https://docs.nvidia.com/compute-sanitizer/ComputeSanitizer/index.html
- `[R-MS-TDR]` Microsoft, WDDM Timeout Detection and Recovery；默认 2 秒，registry keys 面向测试/调试。https://learn.microsoft.com/en-us/windows-hardware/drivers/display/timeout-detection-and-recovery 及 https://learn.microsoft.com/en-us/windows-hardware/drivers/display/tdr-registry-keys

## 数据与列式交换

- `[R-ARROW-CDI]` Apache Arrow C Data Interface。https://arrow.apache.org/docs/format/CDataInterface.html
- `[R-ARROW-IPC]` Apache Arrow IPC。https://arrow.apache.org/docs/python/ipc.html

## 统计

- `[R-SALIB-SOBOL]` SALib Sobol sampler，`N(D+2)` / `N(2D+2)`。https://salib.readthedocs.io/en/latest/_modules/SALib/sample/sobol.html
- `[R-DSR]` Bailey & López de Prado, *The Deflated Sharpe Ratio*. https://www.davidhbailey.com/dhbpapers/deflated-sharpe.pdf
- `[R-PBO]` Bailey et al., *The Probability of Backtest Overfitting*. https://www.davidhbailey.com/dhbpapers/backtest-prob.pdf
- `[R-STATIONARY-BOOTSTRAP]` Politis & Romano, *The Stationary Bootstrap*. https://www.ssc.wisc.edu/~bhansen/718/Politis%20Romano.pdf

## 交易与市场规则

- `[R-FIX-ORDER-STATE]` FIX Latest fields；ExecType 表示事件，OrdStatus 表示当前状态。https://fiximate.fixtrading.org/en/FIX.Latest/fields_sorted_by_tagnum.html
- `[R-FIX-TCA]` FIX Trading Community, *TCA Best Practices for Equities*. https://fixtrading.org/packages/tca-best-practices-for-equities/
- `[R-BINANCE-FUNDING]` Binance USDⓈ-M Futures funding info，含 `fundingIntervalHours`。https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data
- `[R-BINANCE-FILTERS]` Binance Futures symbol filters。https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/common-definition
- `[R-JPX-HOURS]` JPX Domestic Stock Trading Hours。https://www.jpx.co.jp/english/equities/trading/domestic/01.html
- `[R-JPX-INAV]` JPX ETF NAV/iNAV/PCF。https://www.jpx.co.jp/english/equities/products/etfs/inav/index.html

## 可视化与可观测性

- `[R-LWC]` TradingView Lightweight Charts 5.2，panes/custom series/plugins。https://tradingview.github.io/lightweight-charts/docs/
- `[R-LWC-LICENSE]` Lightweight Charts license/attribution notice，同上 Getting Started。
- `[R-ECHARTS]` Apache ECharts，Canvas/SVG、progressive rendering、stream loading。https://echarts.apache.org/
- `[R-PERSPECTIVE]` FINOS Perspective，大型/流式数据、WASM、Arrow、virtualized view。https://perspective.finos.org/
- `[R-PIXI]` PixiJS v8，WebGL/WebGPU 2D renderer。https://pixijs.com/8.x/guides/getting-started/intro
- `[R-WEBGL2]` MDN WebGL2RenderingContext。https://developer.mozilla.org/en-US/docs/Web/API/WebGL2RenderingContext
- `[R-GRAFANA]` Grafana fundamentals，metrics/logs/traces visualization and alerting。https://grafana.com/docs/grafana/latest/fundamentals/
- `[R-GRAFANA-CARDINALITY]` Grafana high-cardinality alerts。https://grafana.com/docs/grafana/latest/alerting/examples/high-cardinality-alerts/
- `[R-OTEL]` OpenTelemetry signals。https://opentelemetry.io/docs/concepts/signals/
- `[R-DASH]` Plotly Dash Graph / AG Grid。https://dash.plotly.com/dash-core-components/graph 及 https://dash.plotly.com/dash-ag-grid
- `[R-BOOKMAP]` Bookmap heatmap and volume dots documentation。https://bookmap.com/knowledgebase/docs/KB-SettingUpAndOperating-HeatmapMainChart 及 https://bookmap.com/knowledgebase/docs/KB-SettingUpAndOperating-HeatmapTradedVolumeVisualization
