import { Fragment, useEffect, useState } from "react";
import type { ReactElement, ReactNode, Ref } from "react";
import { apiPost, opsActionPath } from "./api";
import type {
  ActiveOrdersResponse,
  AssetSource,
  AssetsResponse,
  CapabilitiesResponse,
  ExecutionsResponse,
  OpsActionResponse,
  OpsProcessesResponse,
  PollMeta,
} from "./api";
import {
  BASIS_NOTIONAL,
  EMPTY_TEXT,
  META_SEPARATOR,
  amountText,
  idText,
  isZeroDecimal,
  jstClock,
  jstStamp,
  notionalText,
  orderTypeLabel,
  priceText,
  rawText,
  relativeAge,
  sizeText,
  tallyText,
} from "./format";

interface BadgeProps {
  tone: string;
  text: string;
  hint: string | null;
}

/** 状态徽章：描边形制，语义只由文字色承担。 */
export function Badge({ tone, text, hint }: BadgeProps): ReactElement {
  return (
    <span
      className={`badge badge--${tone}`}
      title={hint === null ? undefined : hint}
    >
      {text}
    </span>
  );
}

/** 标签徽章：同一描边配方，不带语义色。 */
export function Tag({ text, hint }: { text: string; hint: string }): ReactElement {
  return (
    <span className="badge" title={hint}>
      {text}
    </span>
  );
}

export function FreshnessBadge({ meta }: { meta: PollMeta }): ReactElement {
  if (meta.stale && meta.pending) {
    return (
      <Badge tone="danger" text={meta.errorCode ?? EMPTY_TEXT} hint="读取失败" />
    );
  }
  if (meta.stale) {
    return (
      <Badge
        tone="warning"
        text={`陈旧 ${relativeAge(meta.lastUpdatedAt)}`}
        hint="旧数据保留，等待恢复"
      />
    );
  }
  if (meta.lastUpdatedAt === null) {
    return <Badge tone="muted" text="读取中" hint={null} />;
  }
  return (
    <Badge
      tone="positive"
      text={jstClock(meta.lastUpdatedAt)}
      hint="最后更新时刻"
    />
  );
}

/** 标题元数据组：空组不出分隔符。 */
function MetaGroup({
  items,
  side,
}: {
  items: ReactNode[];
  side: string;
}): ReactElement {
  return (
    <span className={`meta-group meta-group--${side}`}>
      {items.map((item, index) => (
        <Fragment key={index}>
          {index > 0 ? <span className="sep">{META_SEPARATOR}</span> : null}
          {item}
        </Fragment>
      ))}
    </span>
  );
}

interface PanelProps {
  title: string;
  area: string;
  meta: PollMeta | null;
  facts: ReactNode[];
  source: string | null;
  toolbar: ReactNode;
  flush: boolean;
  danger: boolean;
  /** 摘要块可省去整个标题行，避免无信息的结构空白。 */
  header?: boolean;
  // 体滚动容器引用，供盘口回中
  bodyRef?: Ref<HTMLDivElement>;
  children: ReactNode;
}

/**
 * 面板原子：需要时显示标题含状态，体自滚动。
 * 标题有控件时并入工具行左端；空标题不占左侧槽。
 */
export function Panel({
  title,
  area,
  meta,
  facts,
  source,
  toolbar,
  flush,
  danger,
  header = true,
  bodyRef,
  children,
}: PanelProps): ReactElement {
  const classes = ["panel", `area-${area}`];
  if (danger) {
    classes.push("panel--danger");
  }
  const banner = meta !== null && meta.stale && meta.error !== null;
  const label = title === "" ? null : <span className="micro-label">{title}</span>;
  // 右槽组次序：数据源标注在前，新鲜度收尾
  const right: ReactNode[] = [];
  if (source !== null) {
    right.push(
      <span key="source" title="数据来源">
        {source}
      </span>,
    );
  }
  if (meta !== null) {
    right.push(
      <span className="badge-slot" key="freshness">
        <FreshnessBadge meta={meta} />
      </span>,
    );
  }
  const headerMeta = (
    <span className="panel-header__meta" aria-label={title === "" ? "面板元数据" : `${title}元数据`}>
      <MetaGroup items={facts} side="left" />
      <MetaGroup items={right} side="right" />
    </span>
  );
  return (
    <section className={classes.join(" ")}>
      {!header ? null : toolbar === null ? (
        <div className="panel-title-row">
          {label}
          {headerMeta}
        </div>
      ) : (
        <div className="panel-toolbar">
          {label}
          <span className="panel-toolbar__controls">{toolbar}</span>
          {headerMeta}
        </div>
      )}
      {banner && meta !== null ? (
        <p className={meta.pending ? "banner banner--error" : "banner"}>
          {meta.error}
        </p>
      ) : null}
      <div
        ref={bodyRef}
        className={flush ? "panel-body panel-body--flush" : "panel-body"}
      >
        {children}
      </div>
    </section>
  );
}

/** 空态与无效态文字块。 */
export function EmptyBlock({ text }: { text: string }): ReactElement {
  return <p className="empty">{text}</p>;
}

/** 指标格：微标签加等宽大值。 */
export function MetricCell({
  label,
  value,
}: {
  label: string;
  value: string;
}): ReactElement {
  return (
    <div className="metric-cell">
      <span className="micro-label">{label}</span>
      <span className="value">{value}</span>
    </div>
  );
}

function sideClass(side: string): string {
  if (side === "BUY") {
    return "pos";
  }
  if (side === "SELL") {
    return "neg";
  }
  return "";
}

function assetValues(
  item: AssetsResponse["items"][number],
  source: string,
): { amount: string; available: string } | null {
  if (source === "all") {
    return { amount: item.amount, available: item.available };
  }
  if (item.venues === undefined) {
    return source === "gmo"
      ? { amount: item.amount, available: item.available }
      : null;
  }
  return item.venues[source] ?? null;
}

const LEGACY_GMO_SOURCE: AssetSource = {
  id: "gmo",
  label: "GMO Coin",
  status: "ok",
};

function zeroAsset(
  item: AssetsResponse["items"][number],
  source: string,
): boolean {
  const values = assetValues(item, source);
  return (
    values !== null &&
    isZeroDecimal(values.amount) &&
    isZeroDecimal(values.available)
  );
}

/** 当前来源中同时为零的资产行数，供标题工具行显示折叠入口。 */
export function assetZeroCount(
  data: AssetsResponse | null,
  source: string,
): number {
  if (data === null) {
    return 0;
  }
  return data.items.filter(
    (item) => assetValues(item, source) !== null && zeroAsset(item, source),
  ).length;
}

/** 资产表：同币种分来源与合计；零 amount/available 默认折叠。 */
export function AssetsTable({
  data,
  source,
  showZero,
  header,
}: {
  data: AssetsResponse | null;
  source: string;
  showZero: boolean;
  header: ReactNode;
}): ReactElement {
  // 兼容单来源旧契约
  const sources = data?.sources ?? (data === null ? [] : [LEGACY_GMO_SOURCE]);
  const activeSource: AssetSource | undefined =
    source === "all" ? undefined : sources.find((item) => item.id === source);
  const sourceMessage =
    activeSource !== undefined && activeSource.status !== "ok"
      ? `${activeSource.label} ${activeSource.status === "unconfigured" ? "未配置" : "读取失败"}`
      : null;
  const matching = (data?.items ?? []).filter(
    (item) => assetValues(item, source) !== null,
  );
  const visibleItems = showZero
    ? matching
    : matching.filter((item) => !zeroAsset(item, source));
  const visibleSources =
    source === "all"
      ? sources
      : sources.filter((item) => item.id === source);
  const columnCount =
    1 + visibleSources.length * 2 + (source === "all" ? 2 : 0);
  const emptyText =
    data === null
      ? EMPTY_TEXT
      : sourceMessage ?? (matching.length === 0 ? "无资产" : null);
  return (
    <div className="asset-table">
      <table className="dense">
        <thead>
          <tr>
            <th className="asset-table__lead" aria-label="资产品种">
              {header}
            </th>
            {visibleSources.map((venue) => (
              <Fragment key={venue.id}>
                <th className="num">{`${venue.label} 总额`}</th>
                <th className="num">{`${venue.label} 可用`}</th>
              </Fragment>
            ))}
            {source === "all" ? (
              <>
                <th className="num">合计总额</th>
                <th className="num">合计可用</th>
              </>
            ) : null}
          </tr>
        </thead>
        <tbody>
          {emptyText === null ? null : (
            <tr>
              <td className="asset-table__empty" colSpan={columnCount}>
                {emptyText}
              </td>
            </tr>
          )}
          {visibleItems.map((item) => {
            const total = assetValues(item, "all");
            return (
              <tr
                key={item.symbol}
                className={zeroAsset(item, source) ? "row--muted" : undefined}
              >
                <td className="mono asset-table__symbol">
                  {rawText(item.symbol)}
                </td>
                {visibleSources.flatMap((venue) => {
                  const values = assetValues(item, venue.id);
                  return [
                    <td className="num" key={`${venue.id}-amount`}>
                      {values === null ? EMPTY_TEXT : amountText(values.amount)}
                    </td>,
                    <td className="num" key={`${venue.id}-available`}>
                      {values === null ? EMPTY_TEXT : amountText(values.available)}
                    </td>,
                  ];
                })}
                {source === "all" && total !== null ? (
                  <>
                    <td className="num">{amountText(total.amount)}</td>
                    <td className="num">{amountText(total.available)}</td>
                  </>
                ) : null}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

/** 挂单表：委托号至时刻共八列，价量精度由品种规则推导。 */
export function ActiveOrdersTable({
  data,
  tickSize,
  sizeStep,
}: {
  data: ActiveOrdersResponse | null;
  tickSize: string;
  sizeStep: string;
}): ReactElement {
  if (data === null) {
    return <EmptyBlock text={EMPTY_TEXT} />;
  }
  if (data.items.length === 0) {
    return <EmptyBlock text="无挂单" />;
  }
  return (
    <table className="dense">
      <thead>
        <tr>
          <th className="col-id">委托号</th>
          <th>方向</th>
          <th>类型</th>
          <th className="num">价格</th>
          <th className="num">数量</th>
          <th className="num">已成交数量</th>
          <th>状态</th>
          <th className="col-time">时刻</th>
        </tr>
      </thead>
      <tbody>
        {data.items.map((item) => (
          <tr key={item.order_id}>
            <td className="mono col-id">{idText(item.order_id)}</td>
            <td className={sideClass(item.side)}>{rawText(item.side)}</td>
            <td>{orderTypeLabel(item.execution_type)}</td>
            <td className="num">{priceText(item.price, tickSize)}</td>
            <td className="num">{sizeText(item.size, sizeStep)}</td>
            <td className="num">{sizeText(item.executed_size, sizeStep)}</td>
            <td>{rawText(item.status)}</td>
            <td className="mono col-time">{jstStamp(item.timestamp)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

/** 成交表：成交号与委托号并列，两者不可混称。 */
export function ExecutionsTable({
  data,
  tickSize,
  sizeStep,
  basis,
}: {
  data: ExecutionsResponse | null;
  tickSize: string;
  sizeStep: string;
  basis: string;
}): ReactElement {
  if (data === null) {
    return <EmptyBlock text={EMPTY_TEXT} />;
  }
  if (data.items.length === 0) {
    return <EmptyBlock text="无成交" />;
  }
  return (
    <table className="dense">
      <thead>
        <tr>
          <th className="col-id">成交号</th>
          <th className="col-id">委托号</th>
          <th>方向</th>
          <th className="num">价格</th>
          <th className="num">数量</th>
          <th className="num">手续费</th>
          <th className="col-time">时刻</th>
        </tr>
      </thead>
      <tbody>
        {data.items.map((item) => (
          <tr key={item.execution_id}>
            <td className="mono col-id">{idText(item.execution_id)}</td>
            <td className="mono col-id">{idText(item.order_id)}</td>
            <td className={sideClass(item.side)}>{rawText(item.side)}</td>
            <td className="num">{priceText(item.price, tickSize)}</td>
            <td className="num">
              {basis === BASIS_NOTIONAL
                ? notionalText(item.price, item.size, tickSize, sizeStep)
                : sizeText(item.size, sizeStep)}
            </td>
            <td className="num">{amountText(item.fee)}</td>
            <td className="mono col-time">{jstStamp(item.timestamp)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

/** 进程状态转语义色。 */
function processTone(status: string): string {
  if (status === "运行") {
    return "positive";
  }
  if (status === "退避中") {
    return "warning";
  }
  if (status === "须人工") {
    return "danger";
  }
  return "muted";
}

/**
 * 拉起按钮：描边形制，点击 POST 拉起。
 * 发出后转退避态，直至状态端点确认运行。
 */
export function ProcessStartButton({
  name,
  processes,
}: {
  name: string;
  processes: OpsProcessesResponse | null;
}): ReactElement {
  const [sent, setSent] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const row = processes?.items.find((item) => item.name === name) ?? null;
  const status = row?.status ?? null;
  const running = status === "运行";

  useEffect(() => {
    if (running) {
      setSent(false);
    }
  }, [running]);

  if (error !== null) {
    return <Badge tone="danger" text={error} hint="拉起失败" />;
  }
  if (row === null) {
    return <Badge tone="muted" text={`${name} 未登记`} hint="白名单外" />;
  }
  if (running) {
    return <Badge tone="positive" text={`${name} 运行`} hint="采集进程" />;
  }
  const waiting = sent || status === "退避中";
  return (
    <button
      type="button"
      className="btn"
      disabled={waiting}
      onClick={() => {
        setSent(true);
        apiPost<OpsActionResponse>(opsActionPath(name, "start")).catch(
          (cause: unknown) => {
            setSent(false);
            setError(cause instanceof Error ? cause.message : "拉起失败");
          },
        );
      }}
    >
      {waiting ? "退避中" : `拉起 ${name}`}
    </button>
  );
}

/** 停止按钮：仅采集进程，无交易语义。 */
function ProcessStopButton({ name }: { name: string }): ReactElement {
  const [sent, setSent] = useState<boolean>(false);
  return (
    <button
      type="button"
      className="btn"
      disabled={sent}
      onClick={() => {
        setSent(true);
        apiPost<OpsActionResponse>(opsActionPath(name, "stop")).finally(() => {
          setSent(false);
        });
      }}
    >
      停止
    </button>
  );
}

/** 采集总览：逐进程状态徽章行，含拉起停止。 */
export function OpsProcessesSection({
  data,
}: {
  data: OpsProcessesResponse | null;
}): ReactElement {
  return (
    <div className="cap-section">
      <div className="micro-label">采集 · 状态 · 心跳 · 重启</div>
      {data === null || data.items.length === 0 ? (
        <EmptyBlock text={EMPTY_TEXT} />
      ) : (
        <ul className="cap-list">
          {data.items.map((item) => (
            <li className="ops-row" key={item.name}>
              <span className="ops-row__name">{item.name}</span>
              <Badge
                tone={processTone(item.status)}
                text={item.status}
                hint="进程状态"
              />
              <span className="ops-row__meta">
                <span className="micro-label">心跳</span>
                <span className="value">
                  {item.heartbeat_at === null
                    ? EMPTY_TEXT
                    : jstClock(item.heartbeat_at)}
                </span>
              </span>
              <span className="ops-row__meta">
                <span className="micro-label">重启</span>
                <span className="value">{tallyText(item.restart_count)}</span>
              </span>
              <span className="ops-row__actions">
                {item.status === "运行" ? (
                  <ProcessStopButton name={item.name} />
                ) : (
                  <ProcessStartButton name={item.name} processes={data} />
                )}
              </span>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

/** 能力范围：阶段与阻塞项徽章化。 */
export function CapabilitiesPanel({
  data,
}: {
  data: CapabilitiesResponse | null;
}): ReactElement {
  if (data === null) {
    return <EmptyBlock text={EMPTY_TEXT} />;
  }
  return (
    <div className="two-col">
      <div className="cap-section">
        <div className="micro-label">已具备 · 阶段</div>
        {data.implemented.length === 0 ? (
          <EmptyBlock text={EMPTY_TEXT} />
        ) : (
          <ul className="cap-list">
            {data.implemented.map((item) => (
              <li className="cap-item" key={item.name}>
                <div className="cap-head">
                  <span>{rawText(item.name)}</span>
                  <Tag text={rawText(item.phase)} hint="阶段" />
                </div>
                <p className="cap-detail">{rawText(item.detail)}</p>
              </li>
            ))}
          </ul>
        )}
      </div>
      <div className="cap-section">
        <div className="micro-label">未具备 · 阶段 · 阻塞项</div>
        {data.pending.length === 0 ? (
          <EmptyBlock text={EMPTY_TEXT} />
        ) : (
          <ul className="cap-list">
            {data.pending.map((item) => (
              <li className="cap-item" key={item.name}>
                <div className="cap-head">
                  <span>{rawText(item.name)}</span>
                  <Tag text={rawText(item.phase)} hint="阶段" />
                  <Tag text={rawText(item.blocker)} hint="阻塞项" />
                </div>
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}
