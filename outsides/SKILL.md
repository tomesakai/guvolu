---
name: fintech-terminal-style
description: 深色高密度金融终端 UI 风格规范 — 配色、字体、间距、label 化语法、组件视觉语法、全局效果与禁止元素的统一实现描述。构建任何"专业终端感"界面(仪表盘、数据工作台、监控台)时使用,与业务逻辑无关,只约束外观。
---

# Fintech Terminal Style — 视觉风格统一规范

一套"密集金融终端"视觉语言。目标观感:像 Bloomberg 一类专业工作站,而不是消费级 SaaS。整体气质由八个词定义:

```text
compact          紧凑
monospace-heavy  等宽字体承载数据
panel-based      面板拼装
low ornament     近零装饰
semantic color   颜色只表语义
clear borders    1px 实线分区
high contrast    高对比
fast scanning    为扫读优化
```

核心心法:**颜色是数据,不是装饰;边框是结构,不是美化;动画是反馈,不是氛围。** 界面的"美"来自密度、对齐和克制,而不是来自任何视觉效果。

本文档是**规范态**描述:凡与既有实现冲突之处,以本文档为准统一。历史实现中常见的漂移(散落的裸 px 字号、组件各自硬编码控件高度、引用了从未定义的变量、微标签样式不一)在下文均给出唯一裁定。

---

## 0. Token 纪律(最高优先级规则)

1. **单一事实源**:所有颜色、字号、间距、控件高度、圆角、动效时长、z-index、遮罩,全部定义在一个 `tokens.css` 的 `:root` 里。组件样式**只准引用变量,禁止出现任何裸值**(裸 hex、裸 px 字号、裸 z-index、裸 rgba)。
2. **先定义后引用**:`var(--x)` 引用一个未定义的变量不会报错,会静默回退并可能整块打碎布局(grid 模板中回退为 `none` → 轨道全变 auto)。因此:新增变量必须先落 tokens 文件;评审时对每个 `var()` 核对定义存在。
3. **一个概念一个 token**:同一语义(如"控件高度""微标签字号")只允许一个变量。发现两个组件对同一概念用了不同值,视为缺陷,向 token 收敛,而不是再加一个变量。
4. **一套间距刻度**:全局只有一套 `--sp-*`,禁止并行的第二套(`--space-1..4` 之类一律并入)。
5. 语义列宽/控件宽用 `ch` 单位定义在 token 中(内容推导、以等宽字体度量),让宽度自动跟随基准字号,避免双重缩放。

---

## 1. 配色

近黑蓝灰底 + 灰阶文字 + 六个语义色。

```css
:root {
  /* 背景四层:整体底 → 面板 → 抬升面(表头/悬浮/弹层) → 输入框 */
  --background-base:     #050607;
  --background-panel:    #0b0d10;
  --background-elevated: #11151a;
  --background-input:    #141a20;

  /* 边框两级:默认分区线 / 焦点与激活强调 */
  --border-default: #26313a;
  --border-focus:   #64748b;

  /* 文字四级 */
  --text-primary:   #e5e7eb;
  --text-secondary: #9ca3af;
  --text-muted:     #6b7280;
  --text-inverse:   #050607;

  /* 语义色 — 每个色只允许携带下述含义 */
  --state-positive: #16a34a;  /* 上涨 / 通过 / 新鲜 */
  --state-negative: #dc2626;  /* 下跌 */
  --state-warning:  #d97706;  /* 警告 / 数据过期 */
  --state-danger:   #ef4444;  /* 高危动作 / 错误 */
  --state-info:     #2563eb;  /* 信息 / 主操作按钮描边 */
  --state-disabled: #374151;  /* 禁用 */
  --state-editable: #d97706;  /* 可编辑字段标识(与 warning 同色,专用于输入) */

  /* 遮罩两档(基底色 + alpha;禁止另造第三档) */
  --scrim-heavy: rgba(5, 6, 7, 0.75);  /* 阻断式模态 */
  --scrim-light: rgba(5, 6, 7, 0.55);  /* 命令面板 / 快捷键卡等轻弹层 */
}
```

要点:

- **背景是分层的,不是分色的。** 四层背景全部是同一色相的近黑蓝灰,层级差极小。区分区域靠 1px 边框,不靠背景跳变。
- **没有品牌色 / accent 色。** 唯一的"强调"是 `--border-focus`,用于焦点、激活 tab、选中面板。任何"随手加个好看的颜色"都违反本风格。
- 颜色的合法用途仅限:涨跌方向、风险/订单/连接状态、数据新鲜度、可编辑字段标识。
- 半透明数据色一律 `color-mix(in srgb, var(--state-x) 14%, transparent)` 派生(深度条、行内数据条),禁止渐变。
- 深浅只有这一套主题,不做亮色模式的敷衍版——如需亮色主题,整套 token 另行成对定义。

---

## 2. 字体

双主字体:UI 用现代几何无衬线,数据用等宽。等宽的覆盖面远大于常规应用——**所有数据即等宽**。

```css
:root {
  --font-ui:   "Geist Sans", system-ui, sans-serif;
  --font-mono: "Geist Mono", "JetBrains Mono", "SF Mono", Menlo, Consolas, monospace;
  /* 可选点缀:像素字体只准用于产品 wordmark 和一个模式徽章,严禁进入数据/正文 */
  --font-pixel: "Silkscreen", monospace;
}
body {
  font-family: var(--font-ui);
  font-size: var(--fs-base);
  line-height: 1.35;
  -webkit-font-smoothing: antialiased;
  font-variant-numeric: tabular-nums; /* 全局:所有数字表格对齐,含 SVG */
}
```

**等宽字体的适用域**(出现以下内容必须切 mono):标的符号、价格、订单、日志、命令、ID、时间戳、公式、状态码、面板模块代码、页脚元数据。

字号阶梯 —— **完整八档,全部具名;禁止在组件里写裸 px 字号**(历史上 10px/9px 曾散落各处,统一收编为 badge/formula 两档):

```css
:root {
  --fs-title:   18px;  /* 屏幕标题 / 模态标题 */
  --fs-command: 15px;  /* 命令输入、大号指标值 */
  --fs-header:  15px;  /* 面板头 */
  --fs-base:    14px;  /* 正文 */
  --fs-table:   13px;  /* 表格、表单、按钮 —— 实际主力字号 */
  --fs-micro:   12px;  /* 页脚说明、细横幅、注释 */
  --fs-nano:    11px;  /* 公式 ID、面板行内小标签、快捷键提示 */
  --fs-badge:   10px;  /* 徽章文字、uppercase 微标签、字段提示 */
  --fs-formula:  9px;  /* 指标格公式角标、报价单元格标签(最小档,慎用) */
}
```

字重克制:正文 400,表头 500,强调 600,不用 700+。

CJK 处理(如需中日文):拉丁字体永远在字体栈首位并独占全部拉丁字形和数字(保住 tabular-nums 对齐);CJK 伴随字体通过 `unicode-range` 只认领 CJK 码位,并用 `size-adjust` / `ascent-override` / `descent-override` 把行高钉到拉丁等宽字体的度量,保证中西混排行高一致、表格不涨行。字体本地打包,禁止运行时 CDN 加载。

---

## 3. 间距、尺寸、边框、层级

```css
:root {
  /* 间距 — 唯一一套 */
  --sp-micro:  2px;
  --sp-tight:  5px;
  --sp-normal: 10px;
  --sp-panel:  14px;
  --sp-modal:  20px;

  /* 控件高度 — 唯一两档;按钮/输入/表格行/chip 全部引用,禁止各写各的 */
  --h-control:    26px;  /* 标准:按钮、输入框、下拉 */
  --h-control-sm: 23px;  /* 紧凑:头部内控件、chip、时间档切换 */
  --h-row:        22px;  /* 密集表格行(虚拟滚动的定高依据) */

  /* 结构高度 */
  --h-command-bar:  47px;
  --h-status-bar:   33px;
  --h-panel-header: 33px;
  --h-panel-footer: 26px;
  --w-module-rail:  60px;
  --panel-gap:       5px;
  --panel-padding:  10px;

  /* 边框与圆角 */
  --radius:       2px;   /* 默认 —— 几乎是直角 */
  --radius-modal: 4px;

  /* 动效 */
  --motion-fast: 120ms;  /* 面板 / 弹层过渡 */
  --motion-pop:   80ms;  /* toast / 悬浮卡 */

  /* z-index 刻度 — 全应用只有这一列,禁止裸写 z-index */
  --z-sticky:   10;   /* 表头、面板内 sticky 行 */
  --z-rail:     40;
  --z-dropdown: 60;   /* 命令建议、下拉 */
  --z-modal:   100;
  --z-palette: 120;
  --z-toast:   130;
}
```

- 24px 以上的间距需要明确理由,默认不存在"呼吸感留白"。
- 圆角只有 2px / 4px 两档,禁止胶囊形装饰组件(pill)。
- 边框统一 `1px solid var(--border-default)`,焦点/激活换 `--border-focus`。
- 阴影:**没有**。层级用背景层 + 边框表达,不用 box-shadow。
- 滚动条自绘:8px 宽,轨道 = panel 背景,滑块 = 边框色,2px 圆角。

### 2px 左边线语法(全局统一的语义记号)

「元素左缘 2px 竖线」是本风格的通用语义标记,含义由颜色唯一决定,全应用一致:

```text
--state-editable(橙)  可编辑字段(输入框/下拉/命令输入)
--state-positive(绿)  成功反馈(toast)
--state-error(红)     失败反馈(toast)/ 阻断原因块
--border-focus(蓝灰)  激活项(rail 按钮)/ 建议与后续动作块
--state-disabled(灰)  禁用输入
```

嵌套结构(条件树、引用块)用 **1px** `--border-default` 左边线表达层级——1px 是结构,2px 是语义,两者不得混用。

---

## 4. 布局骨架

整个应用是一个满屏 grid 壳,`overflow: hidden`,内部各面板各自滚动:

```text
┌──────────────────────────────────────────────┐
│ command bar(顶部命令条, --h-command-bar)     │
├────┬─────────────────────────────────────────┤
│rail│  workspace(面板网格, gap --panel-gap)   │
│60px│  ┌─────────────┐ ┌─────────────┐        │
│    │  │ panel       │ │ panel       │        │
│    │  └─────────────┘ └─────────────┘        │
├────┴─────────────────────────────────────────┤
│ status bar(底部状态条, --h-status-bar)       │
└──────────────────────────────────────────────┘
```

- **command bar**:panel 背景 + 下边框;内含等宽字体命令输入框(带可编辑橙色左边线)。
- **module rail**:左侧 60px 窄竖条,mono 短代码按钮(非图标网格)。按钮无边框,激活态 = 左缘 2px `--border-focus` + elevated 背景;危险项文字用 danger 色;工具类按钮沉底(`margin-top: auto`)并用上边框分隔。
- **status bar**:table 字号,secondary 文字,单行不换行。
- **workspace**:CSS grid 拼面板,gap 用 `--panel-gap`;每列/行设最小宽高下限,低于下限时整屏转为滚动,而不是把面板压扁。

面板(panel)解剖 —— 本风格的原子容器,**所有内容都装在它里面,不存在游离于面板外的内容块**:

```css
.panel {
  display: flex; flex-direction: column;
  background: var(--background-panel);
  border: 1px solid var(--border-default);
  border-radius: var(--radius);
  overflow: hidden; min-height: 0; min-width: 0;
}
.panel.active { border-color: var(--border-focus); }   /* 焦点面板整框提亮 */
.panel-header {
  height: var(--h-panel-header); padding: 0 var(--panel-padding);
  display: flex; align-items: center; gap: var(--sp-normal);
  background: var(--background-elevated);
  border-bottom: 1px solid var(--border-default);
  font-size: var(--fs-header); white-space: nowrap;
}
.panel-header .module-code { font-family: var(--font-mono); font-weight: 600; }
.panel-body   { flex: 1; min-height: 0; overflow: auto; padding: var(--panel-padding); }
.panel-footer {
  height: var(--h-panel-footer); padding: 0 var(--panel-padding);
  border-top: 1px solid var(--border-default);
  font-family: var(--font-mono); font-size: var(--fs-badge); color: var(--text-muted);
}
```

面板头以等宽粗体短代码开头(如 `MKT`、`EXE`),后跟 secondary 色的上下文标签/面包屑(分隔符 `>`)。页脚放元数据(刷新时间、行数)。全空表格等 full-bleed 内容用 no-pad body(padding 0 + flex 列),让内部滚动区自己贴边。

---

## 5. Label 化语法(全局统一)

本风格里**没有无名数字、无名区块、无名状态**。所有标注收敛为以下五种形制,禁止自造第六种:

### 5.1 微标签(micro-label)—— 唯一的小标题样式

区块标题、指标标签、表单节名、报价单元格标签,统一用同一个配方:

```css
.micro-label {
  font-size: var(--fs-badge);          /* 区块级可升到 --fs-table */
  color: var(--text-muted);
  text-transform: uppercase;
  letter-spacing: 0.04em;              /* 统一带字距;历史上时有时无,一律有 */
}
```

规则:标签永远比值弱(muted vs primary)、比值小、在值的上方或左侧。禁止加粗标签、彩色标签、带背景的标签。

### 5.2 值的标注

- **每个派生数字必须带标签**;指标格三行制:uppercase 微标签 / mono 大值 / 可选公式角标(`--fs-formula`, mono, muted)。有公式的值,公式 ID 必须可见。
- **单位**:跟在值后,`--fs-micro`、muted 色、`margin-left: var(--sp-micro)`,不进入值本体的 mono 大字。
- **空值统一 `—`**(em dash),muted 色。禁止空白、`N/A`、`null`、`-` 混用。
- 时间戳、ID 等标识值:mono,过长中段省略(mid-ellipsis),列宽用 `ch` token 保底,永不被挤压换行。

### 5.3 键值对(kv)

```css
.kv { display: grid; grid-template-columns: minmax(130px, auto) 1fr;
      gap: var(--sp-micro) var(--sp-normal); font-size: var(--fs-table); }
.kv dt { color: var(--text-muted); }
.kv dd { font-family: var(--font-mono); text-align: right;
         overflow: hidden; text-overflow: ellipsis; }
.kv dd.text { text-align: left; }   /* 句子型值左对齐;数值型值一律右对齐 */
```

label 列宽全应用统一为一个 token(如 `--w-kv-label: 130px`),不同屏不得各定各的。

### 5.4 表单字段

- 字段**必须**有标签(`--fs-table`,secondary 色),布局为固定 label 列宽的两列 grid。
- 字段**必须**有校验态:提示行 `--fs-badge` muted;错误时提示与输入框边框同转 error 色。
- 可编辑记号(橙色 2px 左边线)只出现在真正可编辑的控件上;只读展示禁止使用。

### 5.5 状态标注

- 状态徽章:mono `--fs-badge` + 6px **方点**(radius 1px)+ 描边,点与文字同色(见 §6)。
- 检查行(check-row):`PASS / FAIL / —` 旗标列固定宽、mono;通过绿、失败红、劝告性警示只准用 warning 橙,**不得借用阻断性的红**——颜色档位必须与后果档位对齐。
- 图标独立出现必须带 `aria-label`;紧邻文字标签时 `aria-hidden`。

---

## 6. 核心组件视觉语法

### 密集表格(风格的主角)

```css
table.dense {
  width: max-content; min-width: 100%;
  border-collapse: collapse; font-size: var(--fs-table);
}
table.dense th {
  position: sticky; top: 0; z-index: var(--z-sticky);
  background: var(--background-elevated);
  color: var(--text-secondary);
  font-weight: 500; text-align: left;
  padding: 3px 6px; white-space: nowrap;
  border-bottom: 1px solid var(--border-default);
}
table.dense td {
  padding: 2px 6px;                      /* 行高 = --h-row */
  border-bottom: 1px solid var(--border-default);
  white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
}
td.num, th.num { text-align: right; font-family: var(--font-mono); }
tr:hover    td { background: var(--background-elevated); }
tr.selected td { background: var(--background-input); }
```

规则:数值列一律右对齐 + mono;ID/时间戳列用 `ch` token 保底最小宽,表格宽出容器时**容器横滚**,永不挤压标识列;涨跌 `.pos/.neg` 只给文字上色,不给单元格底色;行号列 mono muted 右对齐。

### 按钮 —— 「框携带语义,文字保持中性」

```css
.btn {
  height: var(--h-control); padding: 0 var(--sp-normal);
  display: inline-flex; align-items: center; gap: var(--sp-tight);
  background: var(--background-elevated);
  border: 1px solid var(--border-default);
  border-radius: var(--radius);
  color: var(--text-primary); font-size: var(--fs-table);
}
.btn:hover:not(:disabled) { border-color: var(--border-focus); }
.btn.primary  { border-color: var(--state-info); }       /* 文字仍是中性色 */
.btn.positive { border-color: var(--state-positive); }
.btn.danger   { border-color: var(--state-danger); color: var(--state-danger); }
.btn:disabled { color: var(--text-muted); border-color: var(--state-disabled); }
```

**只有高危破坏性按钮(danger)同时给文字上色**;其余语义只落在描边上。没有实心填充按钮,没有大按钮。所有按钮高度 = `--h-control`(头部内紧凑控件用 `--h-control-sm`),禁止第三种高度。

### 徽章

```css
.badge {
  display: inline-flex; align-items: center; gap: var(--sp-tight);
  padding: 1px var(--sp-tight);
  border: 1px solid var(--border-default); border-radius: var(--radius);
  font-family: var(--font-mono); font-size: var(--fs-badge); line-height: 14px;
  color: var(--text-secondary); white-space: nowrap;
}
.badge .dot { width: 6px; height: 6px; border-radius: 1px; } /* 方点,非圆点 */
```

状态色同时落在方点和文字(fresh=绿 / stale=橙 / error=红 / info=蓝 / muted=灰)。徽章永远是描边形,不是实心色块。

### 表单输入(签名细节:橙色左边线)

```css
.input {
  height: var(--h-control); padding: 0 var(--sp-tight);
  background: var(--background-input);
  border: 1px solid var(--border-default);
  border-left: 2px solid var(--state-editable);
  border-radius: var(--radius);
  color: var(--text-primary);
  font-family: var(--font-mono); font-size: var(--fs-table); outline: none;
}
.input:focus    { border-color: var(--border-focus); border-left-color: var(--state-editable); }
.input.invalid  { border-color: var(--state-error); border-left-color: var(--state-error); }
.input:disabled { border-left-color: var(--state-disabled); color: var(--text-muted); }
```

### Tab / 激活态语法(全应用统一)

激活指示 = **一条 2px `--border-focus` 边线 + elevated 背景**,方向随组件:水平 tab 用下边线,竖 rail 用左边线。非激活文字 muted、背景透明;hover 提亮为 primary 文字 + elevated 背景。禁止实底色 tab、圆角胶囊 tab、第二种激活样式。

### 指标格

```css
.metric-cell {
  border: 1px solid var(--border-default); border-radius: var(--radius);
  padding: var(--sp-tight) var(--sp-normal);
  background: var(--background-elevated);
}
.metric-cell .label   { /* micro-label 配方, --fs-badge */ }
.metric-cell .value   { font-family: var(--font-mono); font-size: var(--fs-command); }
.metric-cell .formula { font-family: var(--font-mono); font-size: var(--fs-formula); color: var(--text-muted); }
```

指标网格 `repeat(auto-fill, minmax(118px, 1fr))`。悬浮出详情卡(elevated 背景 + focus 描边 + mono micro 字)属于合规的渐进披露。

### 模态 / 命令面板 / Toast

- 模态:elevated 背景 + `--border-focus` 描边(危险模态换 danger 描边并给标题上 danger 色)+ `--radius-modal` + `--sp-modal` 内边距;遮罩 `--scrim-heavy`;标题 mono `--fs-title`。
- 命令面板:居中偏上(`padding-top: 12vh`),约 560px 宽,行高 30px,行结构 = 「uppercase 微标签域名 | mono 代码列 | secondary 标签 | nano 元数据」;匹配高亮**只用 `text-primary` + 下划线,不准用颜色**;底部一条 nano mono 快捷键提示行;遮罩 `--scrim-light`。
- Toast:右下角贴状态条上方堆叠,elevated 背景 + 默认描边,语义只落在 2px 左边线(§3 语法)。

### 行内数据条(深度条/占比条)

数据编码允许、装饰禁止:横条用语义色 14% 透明度(`color-mix`),绝对定位于文字之下、贴单元格边缘,无渐变无圆角。宽度必须由数据驱动(占比/累计量),不存在"装饰性进度条"。

### 空态 / 加载 / 错误占位

一律是 mono `--fs-table` 文字块:标题 secondary(错误转 error、过期转 warning),细节 muted。没有插画、大图标、骨架屏。后台刷新失败时在面板内容顶部加一条细横幅(`--fs-micro` warning 文字 + elevated 背景 + 下边框),旧数据保持可见——**错误不清空数据**。

---

## 7. 图标与符号

- 图标只准功能性存在,**单一定义点**(一个 Icon 组件 + 注册表),禁止外部图标库/图标字体;未注册的图标名不得渲染。
- 形制:inline SVG、`stroke: currentColor`、1.5px 线宽、12/16px 两档、无填充无渐变。永远单色,颜色继承所在文字的语义 token。
- 一个面板里图标只出现在三类位置:状态列、动作按钮、展开指示。一图标一义(check=通过、cross=失败、warn=警告、clock=等待、run=执行、sync=同步、open=跳转、expand=展开、back=返回…),不得一形多义。
- 符号契约 —— 用户可见文本中每个字形只有一个注册含义:

```text
—    空值 / 不适用(仅此一义;不作破折号连接句子)
·    元数据分隔(meta 行 / 页脚 / 徽章序列;永不出现在正文句子内部)
▸ ▾  行展开指示(可交互)
▲ ▼  表头排序方向(可交互)
→    流程链示意(如 signal → intent → confirm);永不作句内连接词
```

禁止带圈数字(①❶⑴)等装饰性 Unicode;伪勾选(`✓ ○`)一律换真勾选图标;按钮内 `›` 后缀一律换 open 图标。

---

## 8. 全局效果统一

交互反馈全应用只有一套语法,任何组件不得自创:

- **hover** = 背景提一层(transparent→elevated,或 elevated→input)+ 文字提一级(muted→primary),或按钮描边转 `--border-focus`。链接式按钮的 hover 是虚线下划线变实线。**hover 永不引入新颜色。**
- **激活/选中** = 2px `--border-focus` 边线 + elevated 背景(tab/rail),或整框描边转 focus(面板/chip),或 input 层背景(表格选中行)。
- **焦点** = `:focus-visible { outline: 1px solid var(--border-focus); outline-offset: 1px; }`,细环,不用粗蓝光晕。
- **动效**只有两个时长(`--motion-fast` 120ms / `--motion-pop` 80ms)+ 一种数据反馈闪(状态变化触发,700ms ease-out 播一次即停)。**没有数据驱动的循环动画一律禁止**(呼吸、shimmer、脉冲、装饰进度环)。
- **加载** = 10px 纯 CSS 描边旋转圈(border-top 透明,0.7s linear),内联在禁用按钮里;不做整屏 loading。
- **reduced motion**:全局 `@media (prefers-reduced-motion: reduce)` 一刀关停所有 transition/animation。
- **z-index** 只准取 §3 的刻度值。
- **溢出**:面板内允许滚动(横竖皆可),页面级永不滚动;文本溢出统一省略号,不换行不折叠。

---

## 9. 禁止清单(负空间即风格)

以下出现任何一个都会立刻破坏这个风格:

```text
渐变背景 · glassmorphism · 毛玻璃 · 阴影卡片
hero 区 / 大标题动画 · 吉祥物 · 非功能性插画
glow / 霓虹光效 · 大而空的装饰卡片 · confetti
装饰性进度环 · 骨架屏闪烁 · 胶囊按钮
实心大色块按钮 · 彩色 tab 底色 · 无刻度的彩虹热力图
随机 accent 色 · 圆头像 · social feed 式布局
无标签的派生数字 · 无公式的评分 · 同屏重复指标
营销化文案(next-generation / powerful / smart / AI-powered)
```

判断法:如果一个视觉元素删掉后信息量不变,它就不该存在。

---

## 10. 落地顺序与一致性自检

落地顺序:

1. 建 `tokens.css`(§1–§3 全部变量)+ 全局 reset(`box-sizing: border-box`,`html/body/#root { height: 100%; overflow: hidden }`,body 挂字体/行高/tabular-nums)。
2. 搭 §4 四区壳 → 实现 panel 原子 → 按 §6 依次做表格、按钮/徽章、表单、kv/指标格、模态/面板/toast。
3. 对照 §9 做一次减法审查。

一致性自检(历史漂移最高发的六项,每次评审过一遍):

- [ ] grep 不到裸 hex / 裸 px 字号 / 裸 z-index / 裸 rgba(token 定义处除外)
- [ ] 每个 `var()` 都能在 tokens 里找到定义(未定义引用会静默碎版)
- [ ] 所有按钮/输入高度 = `--h-control` 或 `--h-control-sm`,没有第三种
- [ ] 所有小标题/标签走 §5.1 微标签配方(uppercase + 0.04em 字距 + muted),无变体
- [ ] 2px 左边线的颜色语义符合 §3 表;1px 左边线只用于结构嵌套
- [ ] 空值全部渲染为 `—`,劝告性警示用 warning 而非 error 红
