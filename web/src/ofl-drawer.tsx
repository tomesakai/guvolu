import { memo, useEffect, useRef } from "react";
import type { ReactElement } from "react";
import type {
  AlertItem,
  BookFeatureItem,
  LevelTrackResponse,
  RegionResponse,
  TrackHistoryPoint,
} from "./api";
import {
  EMPTY_TEXT,
  jstClock,
  jstStamp,
  plotNumber,
  priceText,
  rawText,
  sizeText,
} from "./format";
import { channelFill, chartChannels } from "./lwc";
import { Badge, EmptyBlock } from "./panels";

export const DRAWER_TRACK = "track";
export const DRAWER_REGION = "region";
export const DRAWER_EVENTS = "events";
export const DRAWER_ALERTS = "alerts";

// 页签四项：追踪、区域分析、事件、报警
const DRAWER_TABS: readonly { key: string; label: string }[] = [
  { key: DRAWER_TRACK, label: "追踪" },
  { key: DRAWER_REGION, label: "区域分析" },
  { key: DRAWER_EVENTS, label: "事件" },
  { key: DRAWER_ALERTS, label: "报警" },
];

const SPARK_HEIGHT = 28;
const HALF = 2;

export interface FetchState<T> {
  readonly pending: boolean;
  readonly error: string | null;
  readonly data: T | null;
}

/** 挂量史 sparkline：空档断开不插值。 */
function Sparkline({
  points,
}: {
  points: readonly TrackHistoryPoint[];
}): ReactElement {
  const canvas = useRef<HTMLCanvasElement | null>(null);

  useEffect(() => {
    const target = canvas.current;
    if (target === null) {
      return;
    }
    const paint = (): void => {
      const width = target.clientWidth;
      const scale = window.devicePixelRatio;
      target.width = Math.max(1, Math.round(width * scale));
      target.height = Math.round(SPARK_HEIGHT * scale);
      const context = target.getContext("2d");
      if (context === null || points.length === 0) {
        return;
      }
      let peak = 0;
      let first = Number.POSITIVE_INFINITY;
      let last = Number.NEGATIVE_INFINITY;
      for (const point of points) {
        peak = Math.max(peak, plotNumber(point.qty) ?? 0);
        const epoch = Date.parse(point.t);
        if (Number.isFinite(epoch)) {
          first = Math.min(first, epoch);
          last = Math.max(last, epoch);
        }
      }
      if (peak <= 0) {
        return;
      }
      const hasTime = Number.isFinite(first) && last > first;
      context.strokeStyle = channelFill(chartChannels("--border-focus"));
      context.lineWidth = scale;
      context.beginPath();
      let open = false;
      points.forEach((point, at) => {
        const value = plotNumber(point.qty);
        if (value === null) {
          // 空档中断线段
          open = false;
          return;
        }
        const epoch = Date.parse(point.t);
        const ratio =
          hasTime && Number.isFinite(epoch)
            ? (epoch - first) / (last - first)
            : at / Math.max(1, points.length - 1);
        const x = ratio * target.width;
        const y =
          target.height -
          (value / peak) * (target.height - scale) -
          scale / HALF;
        if (open) {
          context.lineTo(x, y);
        } else {
          context.moveTo(x, y);
        }
        open = true;
      });
      context.stroke();
    };
    paint();
    const observer = new ResizeObserver(paint);
    observer.observe(target);
    return () => {
      observer.disconnect();
    };
  }, [points]);

  return <canvas className="drawer-spark" ref={canvas} />;
}

function KvRow({ label, value }: { label: string; value: string }): ReactElement {
  return (
    <span className="kv-row">
      <span className="micro-label">{label}</span>
      <span className="value">{value}</span>
    </span>
  );
}

/** 追踪页签：点击档带的流动性追踪。 */
function TrackTab({
  track,
  tickSize,
  sizeStep,
}: {
  track: FetchState<LevelTrackResponse>;
  tickSize: string;
  sizeStep: string;
}): ReactElement {
  if (track.error !== null) {
    return <EmptyBlock text={track.error} />;
  }
  if (track.pending) {
    return <EmptyBlock text="读取中" />;
  }
  const data = track.data;
  if (data === null) {
    return <EmptyBlock text="点击档带唤出追踪" />;
  }
  const found = data.track;
  const replenish = found.replenishment;
  const reaction = found.price_reaction;
  return (
    <div className="stack">
      <KvRow label="档价" value={priceText(found.price_bin, tickSize)} />
      <div>
        <div className="micro-label">挂量史</div>
        <Sparkline points={found.history} />
      </div>
      <div>
        <div className="micro-label">存续期</div>
        {found.segments.length === 0 ? (
          <EmptyBlock text={EMPTY_TEXT} />
        ) : (
          <ul className="drawer-list">
            {found.segments.map((segment, at) => (
              <li className="drawer-list__row" key={at}>
                <span className="value">
                  {`${jstClock(segment.first_seen)}–${jstClock(segment.last_seen)}`}
                </span>
                <span className="micro-label">
                  {`${String(segment.buckets)} 桶`}
                </span>
                <span className="value">
                  {segment.vanished_at === null
                    ? EMPTY_TEXT
                    : jstClock(segment.vanished_at)}
                </span>
              </li>
            ))}
          </ul>
        )}
      </div>
      <KvRow label="净增挂" value={sizeText(found.net_add_total, sizeStep)} />
      <KvRow label="净撤减" value={sizeText(found.net_cancel_total, sizeStep)} />
      <KvRow label="成交消耗" value={sizeText(found.executed_total, sizeStep)} />
      <KvRow label="撤单率" value={rawText(found.cancel_ratio)} />
      <KvRow
        label="补单"
        value={`${String(replenish.count)} 次 · ${sizeText(replenish.size, sizeStep)}`}
      />
      {reaction === null ? (
        <KvRow label="价格反应" value={EMPTY_TEXT} />
      ) : (
        <>
          <KvRow label="价格反应" value={`${reaction.change_bp}bp`} />
          <KvRow
            label="反应窗"
            value={`${jstClock(reaction.event_t)} · ${String(reaction.buckets)} 桶`}
          />
        </>
      )}
    </div>
  );
}

/** 指标值转显示文本，布尔中性化。 */
function metricText(value: unknown): string {
  if (typeof value === "boolean") {
    return value ? "是" : "否";
  }
  if (typeof value === "string") {
    return rawText(value);
  }
  if (typeof value === "number") {
    return String(value);
  }
  return EMPTY_TEXT;
}

/** 区域分析页签：四判定并列展示依据。 */
function RegionTab({
  region,
}: {
  region: FetchState<RegionResponse>;
}): ReactElement {
  if (region.error !== null) {
    return <EmptyBlock text={region.error} />;
  }
  if (region.pending) {
    return <EmptyBlock text="分析中" />;
  }
  const data = region.data;
  if (data === null) {
    return <EmptyBlock text="框选区域唤出分析" />;
  }
  return (
    <div className="stack">
      {data.meta.coverage_clipped ? (
        <Badge
          tone="warning"
          text="截断"
          hint={`请求窗超出瓦片视界，实际覆盖 ${jstStamp(data.meta.coverage_from)}–${jstStamp(data.meta.coverage_to)}`}
        />
      ) : null}
      <KvRow label="有效样本" value={String(data.meta.context_samples)} />
      <KvRow label="分析版本" value={data.meta.code_version} />
      {data.judgments.map((judgment) => (
        <div
          className={judgment.met ? "judgment judgment--met" : "judgment"}
          key={judgment.kind}
        >
          <div className="judgment__head">
            <span className="judgment__kind">{judgment.label}</span>
            <span className="set-pair">
              <span
                className="micro-label"
                title="规则条件的最弱项强度，不是概率"
              >
                规则强度
              </span>
              <span
                className={
                  judgment.met ? "value judgment__conf--met" : "value value--muted"
                }
              >
                {judgment.confidence}
              </span>
            </span>
          </div>
          <div className="judgment__metrics">
            {Object.entries(judgment.metrics).map(([key, value]) => (
              <span className="kv-row" key={key}>
                <span className="micro-label">
                  {judgment.metric_labels[key] ?? key}
                </span>
                <span className="value">{metricText(value)}</span>
              </span>
            ))}
          </div>
          <div className="judgment__metrics">
            {judgment.criteria.map((criterion) => (
              <span className="kv-row" key={criterion.name}>
                <span className="micro-label">{criterion.name}</span>
                <span className={criterion.met ? "value" : "value value--muted"}>
                  {`${criterion.met ? "满足" : "未满足"} · ${metricText(criterion.observed)} ${criterion.comparator} ${metricText(criterion.threshold)} · 强度 ${criterion.score}`}
                </span>
              </span>
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}

/** 事件页签：判读事件时序列表，点击跳转。 */
function EventsTab({
  features,
  selected,
  tickSize,
  onJump,
}: {
  features: readonly BookFeatureItem[];
  selected: BookFeatureItem | null;
  tickSize: string;
  onJump: (feature: BookFeatureItem) => void;
}): ReactElement {
  if (features.length === 0) {
    return <EmptyBlock text="无事件" />;
  }
  return (
    <ul className="drawer-list">
      {features.map((feature) => (
        <li key={feature.feature_id}>
          <button
            type="button"
            className={
              selected !== null && selected.feature_id === feature.feature_id
                ? "event-row is-active"
                : "event-row"
            }
            onClick={() => {
              onJump(feature);
            }}
          >
            <span className="event-row__kind">{feature.label}</span>
            <span className="value">
              {`${priceText(feature.price_low, tickSize)}–${priceText(feature.price_high, tickSize)}`}
            </span>
            <span className="micro-label">
              {`${jstClock(feature.from_ts)}–${jstClock(feature.to_ts)}`}
            </span>
          </button>
        </li>
      ))}
    </ul>
  );
}

/** 报警页签：列表加确认，唯一交互写动作。 */
function AlertsTab({
  alerts,
  unackedIds,
  tickSize,
  onAck,
  onJump,
  features,
}: {
  alerts: readonly AlertItem[];
  unackedIds: ReadonlySet<number>;
  tickSize: string;
  onAck: (alertId: number) => void;
  onJump: (feature: BookFeatureItem) => void;
  features: readonly BookFeatureItem[];
}): ReactElement {
  if (alerts.length === 0) {
    return <EmptyBlock text="无报警" />;
  }
  return (
    <ul className="drawer-list">
      {alerts.map((alert) => {
        const unacked = unackedIds.has(alert.alert_id);
        const feature =
          features.find((item) => item.feature_id === alert.feature_id) ?? null;
        return (
          <li
            className={unacked ? "alert-row" : "alert-row alert-row--acked"}
            key={alert.alert_id}
          >
            <button
              type="button"
              className="alert-row__body"
              onClick={() => {
                if (feature !== null) {
                  onJump(feature);
                }
              }}
            >
              <span className="alert-row__head">
                <span
                  className={
                    unacked ? "alert-row__kind warn" : "alert-row__kind"
                  }
                >
                  {alert.label}
                </span>
                <span
                  className="micro-label micro-label--literal"
                  title={alert.rule_id}
                >
                  {alert.rule_id}
                </span>
              </span>
              <span className="value">
                {`${priceText(alert.price_low, tickSize)}–${priceText(alert.price_high, tickSize)}`}
              </span>
              <span className="micro-label">{jstStamp(alert.triggered_at)}</span>
            </button>
            {unacked ? (
              <button
                type="button"
                className="btn"
                onClick={() => {
                  onAck(alert.alert_id);
                }}
              >
                确认
              </button>
            ) : (
              <Badge tone="muted" text={jstClock(alert.acked_at)} hint="确认时刻" />
            )}
          </li>
        );
      })}
    </ul>
  );
}

export interface OflDrawerProps {
  tab: string;
  onTab: (tab: string) => void;
  onClose: () => void;
  track: FetchState<LevelTrackResponse>;
  region: FetchState<RegionResponse>;
  features: readonly BookFeatureItem[];
  alerts: readonly AlertItem[];
  unackedIds: ReadonlySet<number>;
  selected: BookFeatureItem | null;
  tickSize: string;
  sizeStep: string;
  onAck: (alertId: number) => void;
  onJump: (feature: BookFeatureItem) => void;
}

/** 右侧恒宽抽屉：追踪、区域分析、事件、报警四页签。 */
function OflDrawerImpl({
  tab,
  onTab,
  onClose,
  track,
  region,
  features,
  alerts,
  unackedIds,
  selected,
  tickSize,
  sizeStep,
  onAck,
  onJump,
}: OflDrawerProps): ReactElement {
  return (
    <aside className="ofl-drawer">
      <div className="tab-bar">
        {DRAWER_TABS.map((item) => (
          <button
            key={item.key}
            type="button"
            role="tab"
            aria-selected={item.key === tab}
            className={item.key === tab ? "tab is-active" : "tab"}
            onClick={() => {
              onTab(item.key);
            }}
          >
            {item.label}
          </button>
        ))}
        <button
          type="button"
          className="chip chip--nano ofl-drawer__close"
          onClick={onClose}
        >
          关闭
        </button>
      </div>
      <div className="ofl-drawer__body">
        {tab === DRAWER_TRACK ? (
          <TrackTab track={track} tickSize={tickSize} sizeStep={sizeStep} />
        ) : tab === DRAWER_REGION ? (
          <RegionTab region={region} />
        ) : tab === DRAWER_EVENTS ? (
          <EventsTab
            features={features}
            selected={selected}
            tickSize={tickSize}
            onJump={onJump}
          />
        ) : (
          <AlertsTab
            alerts={alerts}
            unackedIds={unackedIds}
            tickSize={tickSize}
            onAck={onAck}
            onJump={onJump}
            features={features}
          />
        )}
      </div>
    </aside>
  );
}

/** memo 边界：入参实变才重渲。 */
export const OflDrawer = memo(OflDrawerImpl);
