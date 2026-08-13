import { useState } from "react";
import type { ReactElement } from "react";
import {
  ALERTS_POLL_INTERVAL_MS,
  FEATURES_POLL_INTERVAL_MS,
  HEATMAP_POLL_INTERVAL_MS,
  KLINE_POLL_INTERVAL_MS,
  OPS_POLL_INTERVAL_MS,
  POLL_INTERVAL_MS,
} from "./api";
import {
  BASIS_NOTIONAL,
  BASIS_SIZE,
  DEFAULT_FORMAT_SWITCHES,
} from "./format";
import type { FormatSwitches } from "./format";
import {
  DEFAULT_CHART_KIND,
  PRICE_CHART_KIND_GROUPS,
} from "./chart";
import { Tag } from "./panels";

// 设置只影响呈现，键名带版本前缀
export const SETTINGS_STORAGE_KEY = "guvolu.v1.display";

export interface DisplaySettings {
  readonly display: FormatSwitches;
  readonly chartKind: string;
}

const MS_PER_SECOND = 1000;

/** 毫秒周期转秒文本。 */
function seconds(ms: number): string {
  return `${String(ms / MS_PER_SECOND)}s`;
}

/** 读取本地存储的显示设置，缺失或损坏回缺省。 */
export function loadDisplaySettings(): DisplaySettings {
  let raw: string | null = null;
  try {
    raw = window.localStorage.getItem(SETTINGS_STORAGE_KEY);
  } catch {
    // 本地存储不可用时回缺省
    return { display: DEFAULT_FORMAT_SWITCHES, chartKind: DEFAULT_CHART_KIND };
  }
  if (raw === null) {
    return { display: DEFAULT_FORMAT_SWITCHES, chartKind: DEFAULT_CHART_KIND };
  }
  let parsed: unknown = null;
  try {
    parsed = JSON.parse(raw);
  } catch {
    // 存值损坏时回缺省
    return { display: DEFAULT_FORMAT_SWITCHES, chartKind: DEFAULT_CHART_KIND };
  }
  const shaped = parsed as {
    groupDigits?: unknown;
    abbrevTotals?: unknown;
    valueBasis?: unknown;
    chartKind?: unknown;
  };
  const chartKind =
    typeof shaped.chartKind === "string" &&
    PRICE_CHART_KIND_GROUPS.flatMap((group) => group.options).some(
      (option) => option.key === shaped.chartKind,
    )
    ? shaped.chartKind
    : DEFAULT_CHART_KIND;
  return {
    display: {
      groupDigits:
        typeof shaped.groupDigits === "boolean"
          ? shaped.groupDigits
          : DEFAULT_FORMAT_SWITCHES.groupDigits,
      abbrevTotals:
        typeof shaped.abbrevTotals === "boolean"
          ? shaped.abbrevTotals
          : DEFAULT_FORMAT_SWITCHES.abbrevTotals,
      valueBasis:
        shaped.valueBasis === BASIS_NOTIONAL ? BASIS_NOTIONAL : BASIS_SIZE,
    },
    chartKind,
  };
}

/** 写入本地存储，失败不阻断显示。 */
export function storeDisplaySettings(next: DisplaySettings): void {
  try {
    window.localStorage.setItem(
      SETTINGS_STORAGE_KEY,
      JSON.stringify({ ...next.display, chartKind: next.chartKind }),
    );
  } catch {
    // 存储失败时仅保留内存态
  }
}

interface ChoiceOption {
  readonly key: string;
  readonly label: string;
}

/** 二选一 chip 行：kv 行左标签右控件。 */
function ChoiceRow({
  label,
  options,
  value,
  onPick,
}: {
  label: string;
  options: readonly ChoiceOption[];
  value: string;
  onPick: (key: string) => void;
}): ReactElement {
  return (
    <span className="kv-row">
      <span className="micro-label">{label}</span>
      <span className="chip-group">
        {options.map((option) => (
          <button
            key={option.key}
            type="button"
            className={option.key === value ? "chip is-active" : "chip"}
            aria-pressed={option.key === value}
            onClick={() => {
              onPick(option.key);
            }}
          >
            {option.label}
          </button>
        ))}
      </span>
    </span>
  );
}

/** 固定项：值弱化并挂固定徽章。 */
function FixedRow({
  label,
  value,
}: {
  label: string;
  value: string;
}): ReactElement {
  return (
    <span className="kv-row">
      <span className="micro-label">{label}</span>
      <span className="set-pair">
        <span className="value value--muted">{value}</span>
        <Tag text="固定" hint="不可配置" />
      </span>
    </span>
  );
}

const ABBREV_OPTIONS: readonly ChoiceOption[] = [
  { key: "abbrev", label: "缩写 k/M" },
  { key: "full", label: "完整" },
];

const GROUP_OPTIONS: readonly ChoiceOption[] = [
  { key: "on", label: "开" },
  { key: "off", label: "关" },
];

const BASIS_OPTIONS: readonly ChoiceOption[] = [
  { key: BASIS_SIZE, label: "数量" },
  { key: BASIS_NOTIONAL, label: "金额 JPY" },
];

const TAB_DISPLAY = "display";
const TAB_DATA = "data";

const TABS: readonly ChoiceOption[] = [
  { key: TAB_DISPLAY, label: "显示" },
  { key: TAB_DATA, label: "数据" },
];

/** 显示 tab：两项可配置加固定项陈列。 */
function DisplayTab({
  display,
  chartKind,
  onChange,
  onChartKindChange,
}: {
  display: FormatSwitches;
  chartKind: string;
  onChange: (next: FormatSwitches) => void;
  onChartKindChange: (next: string) => void;
}): ReactElement {
  return (
    <div className="stack">
      <ChoiceRow
        label="量与合计"
        options={ABBREV_OPTIONS}
        value={display.abbrevTotals ? "abbrev" : "full"}
        onPick={(key) => {
          onChange({ ...display, abbrevTotals: key === "abbrev" });
        }}
      />
      <ChoiceRow
        label="千分位分组 · 价格与金额"
        options={GROUP_OPTIONS}
        value={display.groupDigits ? "on" : "off"}
        onPick={(key) => {
          onChange({ ...display, groupDigits: key === "on" });
        }}
      />
      <ChoiceRow
        label="数值基准"
        options={BASIS_OPTIONS}
        value={display.valueBasis}
        onPick={(key) => {
          onChange({ ...display, valueBasis: key });
        }}
      />
      <span className="kv-row">
        <span className="micro-label">价格图型</span>
        <span className="select-wrap">
          <select
            className="select"
            aria-label="价格图型"
            value={chartKind}
            onChange={(event) => {
              onChartKindChange(event.target.value);
            }}
          >
            {PRICE_CHART_KIND_GROUPS.map((group) => (
              <optgroup key={group.label} label={group.label}>
                {group.options.map((option) => (
                  <option key={option.key} value={option.key}>
                    {option.label}
                  </option>
                ))}
              </optgroup>
            ))}
          </select>
        </span>
      </span>
      <FixedRow label="提示框" value="原文" />
      <FixedRow label="时区" value="JST" />
      <FixedRow label="主题" value="深色" />
    </div>
  );
}

/** 数据 tab：只读陈述轮询周期与数据源标注。 */
function DataTab(): ReactElement {
  return (
    <div className="stack">
      <div>
        <div className="micro-label">轮询周期</div>
        <dl className="kv">
          <dt>行情 · 资产 · 盘口 · 挂单 · 成交</dt>
          <dd>{seconds(POLL_INTERVAL_MS)}</dd>
          <dt>报警 · 进程</dt>
          <dd>{seconds(Math.min(ALERTS_POLL_INTERVAL_MS, OPS_POLL_INTERVAL_MS))}</dd>
          <dt>K 线</dt>
          <dd>{seconds(KLINE_POLL_INTERVAL_MS)}</dd>
          <dt>热力</dt>
          <dd>{seconds(HEATMAP_POLL_INTERVAL_MS)}</dd>
          <dt>足迹 · 判读事件 · 成交刻线</dt>
          <dd>{seconds(FEATURES_POLL_INTERVAL_MS)}</dd>
        </dl>
      </div>
      <div>
        <div className="micro-label">数据源标注</div>
        <dl className="kv">
          <dt>live</dt>
          <dd>上游实时</dd>
          <dt>store</dt>
          <dd>本地库存</dd>
          <dt>store+live</dt>
          <dd>库存加当期上游</dd>
        </dl>
      </div>
    </div>
  );
}

/** 设置页：水平 tab 分区，显示层偏好即时生效。 */
export function SettingsPage({
  display,
  chartKind,
  onChange,
  onChartKindChange,
}: {
  display: FormatSwitches;
  chartKind: string;
  onChange: (next: FormatSwitches) => void;
  onChartKindChange: (next: string) => void;
}): ReactElement {
  const [tab, setTab] = useState<string>(TAB_DISPLAY);
  return (
    <div className="stack">
      <div className="tab-bar" role="tablist">
        {TABS.map((item) => (
          <button
            key={item.key}
            type="button"
            role="tab"
            aria-selected={item.key === tab}
            className={item.key === tab ? "tab is-active" : "tab"}
            onClick={() => {
              setTab(item.key);
            }}
          >
            {item.label}
          </button>
        ))}
      </div>
      {tab === TAB_DISPLAY ? (
        <DisplayTab
          display={display}
          chartKind={chartKind}
          onChange={onChange}
          onChartKindChange={onChartKindChange}
        />
      ) : (
        <DataTab />
      )}
    </div>
  );
}
