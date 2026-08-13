import { Fragment, memo, useEffect, useLayoutEffect, useRef } from "react";
import type { CSSProperties, ReactElement } from "react";
import type { OrderbookLevel, OrderbookResponse } from "./api";
import {
  BASIS_NOTIONAL,
  EMPTY_TEXT,
  bpText,
  plotNumber,
  priceText,
  sizeFixedText,
  totalText,
  unitCountText,
} from "./format";
import { EmptyBlock } from "./panels";

const FULL_PERCENT = 100;
const PERCENT_DIGITS = 2;
const BP_FACTOR = 10000;
const ORDERBOOK_MOVE_MS = 140;
const TRAILING_ZERO_RUN = /(0{2,})$/;
const GAP_ROWS_PER_BAND = 10;
const GAP_LABEL_MIN_UNITS = 1;

/** MON 可选的带内宽度；指标与结构均按此带裁切。 */
export const OB_BAND_WIDTHS: readonly string[] = ["5", "10", "25"];

export function obBandLabel(bandBp: string): string {
  return `${bandBp}bp`;
}

interface ProfileLevel extends OrderbookLevel {
  readonly cumulative: number;
  readonly magnitude: number;
}

/** 从最优档向外累加，两个方向共用像素刻度。 */
function profileLevels(
  levels: readonly OrderbookLevel[],
  notional: boolean,
): ProfileLevel[] {
  let cumulative = 0;
  return levels.map((level) => {
    const value = plotNumber(notional ? level.notional : level.size) ?? 0;
    const magnitude = Math.max(0, value);
    cumulative += magnitude;
    return { ...level, cumulative, magnitude };
  });
}

function widthText(value: number, peak: number): string {
  if (value <= 0 || peak <= 0) {
    return "0%";
  }
  return `${Math.min(FULL_PERCENT, (value / peak) * FULL_PERCENT).toFixed(
    PERCENT_DIGITS,
  )}%`;
}

function withinBand(
  level: OrderbookLevel,
  mid: string,
  bandBp: string,
  side: "ask" | "bid",
): boolean {
  const price = plotNumber(level.price);
  const middle = plotNumber(mid);
  const band = plotNumber(bandBp);
  if (price === null || middle === null || band === null || middle <= 0) {
    return false;
  }
  const distance =
    side === "ask" ? price - middle : middle - price;
  return distance <= (middle * band) / BP_FACTOR;
}

function percentText(value: string): string {
  const parsed = plotNumber(value);
  if (parsed === null) {
    return EMPTY_TEXT;
  }
  const percent = parsed * FULL_PERCENT;
  return `${percent > 0 ? "+" : ""}${percent.toFixed(0)}%`;
}

interface GapMeasure {
  readonly bp: number;
  readonly units: number;
}

/** 等档视图中，只把相邻报价之间确实缺失的 bp 留作空白。 */
function gapMeasure(
  first: string,
  second: string,
  mid: string,
  bandBp: string,
  tickSize: string,
): GapMeasure {
  const [left, right, middle, band, tick] = [
    plotNumber(first),
    plotNumber(second),
    plotNumber(mid),
    plotNumber(bandBp),
    plotNumber(tickSize),
  ];
  if (
    left === null ||
    right === null ||
    middle === null ||
    band === null ||
    tick === null ||
    middle <= 0 ||
    band <= 0
  ) {
    return { bp: 0, units: 0 };
  }
  const blankBp = Math.max(0, ((Math.abs(left - right) - tick) / middle) * BP_FACTOR);
  return {
    bp: blankBp,
    units: Math.min(GAP_ROWS_PER_BAND, (blankBp / band) * GAP_ROWS_PER_BAND),
  };
}

function microTickText(microprice: string, mid: string, tickSize: string): string {
  const [micro, middle, tick] = [
    plotNumber(microprice),
    plotNumber(mid),
    plotNumber(tickSize),
  ];
  if (micro === null || middle === null || tick === null || tick <= 0) {
    return "μ —";
  }
  const delta = (micro - middle) / tick;
  const digits = Math.abs(delta) < 10 ? 1 : 0;
  return `μ${delta > 0 ? "+" : ""}${delta.toFixed(digits)}t`;
}

/** 保留定列精度，只降低连续尾零的视觉权重。 */
function MutedTrailingZeros({ text }: { text: string }): ReactElement {
  const found = TRAILING_ZERO_RUN.exec(text);
  if (found === null || found.index === 0) {
    return <>{text}</>;
  }
  return (
    <>
      {text.slice(0, found.index)}
      0
      <span className="numeric-tail-zero">{found[1]?.slice(1)}</span>
    </>
  );
}

/** 数字保持原文；横向不足时只压缩字面宽度，并以 title 暴露完整值。 */
function FittedNumber({
  text,
  align = "center",
}: {
  text: string;
  align?: "left" | "center" | "right";
}): ReactElement {
  const value = useRef<HTMLSpanElement | null>(null);
  useLayoutEffect(() => {
    const node = value.current;
    if (node === null) {
      return;
    }
    const parent = node.parentElement;
    if (parent === null) {
      return;
    }
    const fit = (): void => {
      const style = window.getComputedStyle(parent);
      const padding =
        Number.parseFloat(style.paddingLeft) +
        Number.parseFloat(style.paddingRight);
      const available = Math.max(0, parent.clientWidth - padding);
      const natural = node.scrollWidth;
      const scale = natural > 0 ? Math.min(1, available / natural) : 1;
      node.style.setProperty("--numeric-scale", scale.toFixed(3));
      node.toggleAttribute("data-compressed", scale < 0.999);
    };
    fit();
    const observer = new ResizeObserver(fit);
    observer.observe(parent);
    return () => {
      observer.disconnect();
    };
  }, [text]);
  return (
    <span
      ref={value}
      className={`numeric-fit numeric-fit--${align}`}
      title={text}
      aria-label={text}
    >
      <MutedTrailingZeros text={text} />
    </span>
  );
}

function ProfileRow({
  level,
  side,
  peak,
  levelPeak,
  tickSize,
  sizeStep,
  notional,
  changed,
}: {
  level: ProfileLevel;
  side: "ask" | "bid";
  peak: number;
  levelPeak: number;
  tickSize: string;
  sizeStep: string;
  notional: boolean;
  changed: boolean;
}): ReactElement {
  const row = useRef<HTMLDivElement | null>(null);
  const previousTop = useRef<number | null>(null);
  useLayoutEffect(() => {
    const node = row.current;
    if (node === null) {
      return;
    }
    const nextTop = node.offsetTop;
    const before = previousTop.current;
    previousTop.current = nextTop;
    if (
      before === null ||
      before === nextTop ||
      window.matchMedia("(prefers-reduced-motion: reduce)").matches
    ) {
      return;
    }
    for (const active of node.getAnimations()) {
      if (active.id === "ob-shift") {
        active.cancel();
      }
    }
    const movement = node.animate(
      [
        { transform: `translateY(${String(before - nextTop)}px)` },
        { transform: "translateY(0)" },
      ],
      {
        duration: ORDERBOOK_MOVE_MS,
        easing: "cubic-bezier(0.2, 0, 0, 1)",
      },
    );
    movement.id = "ob-shift";
  });
  const value = notional
    ? totalText(level.notional, "1")
    : sizeFixedText(level.size, sizeStep);
  return (
    <div className="ob-row" ref={row}>
      {changed ? (
        <i
          key={notional ? level.notional : level.size}
          className={
            side === "ask"
              ? "ob-row__pulse ob-row__pulse--ask"
              : "ob-row__pulse ob-row__pulse--bid"
          }
          aria-hidden="true"
        />
      ) : null}
      {side === "ask" ? (
        <span className="ob-depth ob-depth--ask">
          <i
            className="ob-depth__bar ob-depth__bar--ask"
            style={{ width: widthText(level.cumulative, peak) }}
            aria-hidden="true"
          />
          <i
            className="ob-depth__level ob-depth__level--ask"
            style={{ width: widthText(level.magnitude, levelPeak) }}
            aria-hidden="true"
          />
          <b className="ob-depth__value">
            <FittedNumber text={value} align="right" />
          </b>
        </span>
      ) : (
        <span className="ob-depth" aria-hidden="true" />
      )}
      <span className={side === "ask" ? "ob-price neg" : "ob-price pos"}>
        <FittedNumber text={priceText(level.price, tickSize)} />
      </span>
      {side === "bid" ? (
        <span className="ob-depth ob-depth--bid">
          <i
            className="ob-depth__bar ob-depth__bar--bid"
            style={{ width: widthText(level.cumulative, peak) }}
            aria-hidden="true"
          />
          <i
            className="ob-depth__level ob-depth__level--bid"
            style={{ width: widthText(level.magnitude, levelPeak) }}
            aria-hidden="true"
          />
          <b className="ob-depth__value">
            <FittedNumber text={value} align="left" />
          </b>
        </span>
      ) : (
        <span className="ob-depth" aria-hidden="true" />
      )}
    </div>
  );
}

function ProfileRows({
  levels,
  side,
  peak,
  levelPeak,
  tickSize,
  sizeStep,
  notional,
  changed,
  gapScaled,
  mid,
  bandBp,
}: {
  levels: readonly ProfileLevel[];
  side: "ask" | "bid";
  peak: number;
  levelPeak: number;
  tickSize: string;
  sizeStep: string;
  notional: boolean;
  changed: (level: OrderbookLevel) => boolean;
  gapScaled: boolean;
  mid: string;
  bandBp: string;
}): ReactElement {
  return (
    <>
      {levels.map((level, index) => {
        const next = levels[index + 1];
        const gap =
          next === undefined
            ? { bp: 0, units: 0 }
            : gapMeasure(level.price, next.price, mid, bandBp, tickSize);
        return (
          <Fragment key={level.price}>
            <ProfileRow
              level={level}
              side={side}
              peak={peak}
              levelPeak={levelPeak}
              tickSize={tickSize}
              sizeStep={sizeStep}
              notional={notional}
              changed={changed(level)}
            />
            {gapScaled && gap.units > 0 ? (
              <span
                className="ob-price-gap"
                style={{ "--ob-gap-units": gap.units.toFixed(PERCENT_DIGITS) } as CSSProperties}
                aria-label={`空档 ${gap.bp.toFixed(PERCENT_DIGITS)}bp`}
              >
                {gap.units >= GAP_LABEL_MIN_UNITS ? (
                  <small>{`${gap.bp.toFixed(PERCENT_DIGITS)}bp`}</small>
                ) : null}
              </span>
            ) : null}
          </Fragment>
        );
      })}
    </>
  );
}

/**
 * MON 盘口剖面：卖方深度、价格轨、买方深度共用一个刻度。
 */
function OrderbookLadderImpl({
  data,
  tickSize,
  sizeStep,
  bandBp,
  basis,
  gapScaled,
}: {
  data: OrderbookResponse | null;
  tickSize: string;
  sizeStep: string;
  bandBp: string;
  basis: string;
  gapScaled: boolean;
}): ReactElement {
  const previousLevels = useRef<{
    basis: string;
    values: Map<string, string>;
  } | null>(null);
  useEffect(() => {
    if (data === null) {
      previousLevels.current = null;
      return;
    }
    const values = new Map<string, string>();
    for (const level of [...data.asks, ...data.bids]) {
      values.set(
        level.price,
        basis === BASIS_NOTIONAL ? level.notional : level.size,
      );
    }
    previousLevels.current = { basis, values };
  }, [basis, data]);

  if (data === null) {
    return <EmptyBlock text={EMPTY_TEXT} />;
  }
  const notional = basis === BASIS_NOTIONAL;
  const previous = previousLevels.current;
  const changedLevel = (level: OrderbookLevel): boolean => {
    if (previous === null || previous.basis !== basis) {
      return false;
    }
    const value = notional ? level.notional : level.size;
    return previous.values.get(level.price) !== value;
  };
  const asks = data.asks.filter((level) =>
    withinBand(level, data.mid, bandBp, "ask"),
  );
  const bids = data.bids.filter((level) =>
    withinBand(level, data.mid, bandBp, "bid"),
  );
  if (asks.length === 0 || bids.length === 0) {
    return <EmptyBlock text="带内无完整盘口" />;
  }
  const askProfile = profileLevels(asks, notional);
  const bidProfile = profileLevels(bids, notional);
  const askRows = [...askProfile].reverse();
  const peak = Math.max(
    askProfile.at(-1)?.cumulative ?? 0,
    bidProfile.at(-1)?.cumulative ?? 0,
  );
  const levelPeak = Math.max(
    ...askProfile.map((level) => level.magnitude),
    ...bidProfile.map((level) => level.magnitude),
  );
  const selectedBand =
    (data.bands ?? []).find((item) => item.band_bp === bandBp) ?? null;
  const imbalanceValue =
    selectedBand === null
      ? null
      : notional
        ? selectedBand.imbalance_notional
        : selectedBand.imbalance_size;
  const imbalance =
    selectedBand === null || !selectedBand.complete || imbalanceValue === null
      ? EMPTY_TEXT
      : percentText(imbalanceValue);
  const askDepth =
    selectedBand === null || !selectedBand.ask_complete
      ? null
      : plotNumber(notional ? selectedBand.ask_notional : selectedBand.ask_size);
  const bidDepth =
    selectedBand === null || !selectedBand.bid_complete
      ? null
      : plotNumber(notional ? selectedBand.bid_notional : selectedBand.bid_size);
  const bandDepth = Math.max(0, askDepth ?? 0) + Math.max(0, bidDepth ?? 0);
  const shareText = (value: number | null): string =>
    value === null || bandDepth <= 0
      ? EMPTY_TEXT
      : `${((Math.max(0, value) / bandDepth) * FULL_PERCENT).toFixed(0)}%`;
  const askShare = shareText(askDepth);
  const bidShare = shareText(bidDepth);
  const shareHeight = (value: number | null): string =>
    value === null || bandDepth <= 0
      ? "0%"
      : `${((Math.max(0, value) / bandDepth) * FULL_PERCENT).toFixed(PERCENT_DIGITS)}%`;
  const spreadTicks = unitCountText(data.spread, tickSize);
  const spreadBp = bpText(data.spread, data.mid);
  const spreadDetail = `${spreadTicks} tick · ¥${priceText(data.spread, tickSize)} · ${spreadBp}bp`;
  const askDepthText =
    selectedBand === null || !selectedBand.ask_complete
      ? EMPTY_TEXT
      : notional
        ? totalText(selectedBand.ask_notional, "1")
        : sizeFixedText(selectedBand.ask_size, sizeStep);
  const bidDepthText =
    selectedBand === null || !selectedBand.bid_complete
      ? EMPTY_TEXT
      : notional
        ? totalText(selectedBand.bid_notional, "1")
        : sizeFixedText(selectedBand.bid_size, sizeStep);
  const microSummary = microTickText(data.microprice, data.mid, tickSize);
  const bandCoverage = selectedBand?.complete === false ? " · 带宽不足" : "";
  const coverageSummary =
    `显示 ask ${data.coverage.ask_bp}bp · bid ${data.coverage.bid_bp}bp` +
    ` · 来源 ask ${data.source_coverage.ask_bp}bp` +
    ` · bid ${data.source_coverage.bid_bp}bp${bandCoverage}`;
  return (
    <div className={gapScaled ? "ob-shell ob-shell--gap-scaled" : "ob-shell"}>
      <div className="ob-scroll">
        <div className={notional ? "ob ob--notional" : "ob"}>
          <div className="ob-side">
            <ProfileRows
              levels={askRows}
              side="ask"
              peak={peak}
              levelPeak={levelPeak}
              tickSize={tickSize}
              sizeStep={sizeStep}
              notional={notional}
              changed={changedLevel}
              gapScaled={gapScaled}
              mid={data.mid}
              bandBp={bandBp}
            />
          </div>
          <div className="ob-seam">
            <span
              className="ob-seam__band ob-seam__band--ask"
              title={`${obBandLabel(bandBp)} 卖方深度 ${askDepthText}`}
            >
              <b>ask</b>
              <FittedNumber text={askDepthText} align="right" />
            </span>
            <span
              className="ob-seam__mid"
              title={`${microSummary} · ${spreadDetail} · ${coverageSummary}`}
            >
              <span className="ob-seam__mid-price">
                <FittedNumber text={priceText(data.mid, tickSize)} />
              </span>
            </span>
            <span
              className="ob-seam__band ob-seam__band--bid"
              title={`${obBandLabel(bandBp)} 买方深度 ${bidDepthText}`}
            >
              <FittedNumber text={bidDepthText} align="left" />
              <b>bid</b>
            </span>
          </div>
          <div className="ob-side">
            <ProfileRows
              levels={bidProfile}
              side="bid"
              peak={peak}
              levelPeak={levelPeak}
              tickSize={tickSize}
              sizeStep={sizeStep}
              notional={notional}
              changed={changedLevel}
              gapScaled={gapScaled}
              mid={data.mid}
              bandBp={bandBp}
            />
          </div>
        </div>
      </div>
      <aside
        className="ob-imbalance-rail"
        title={`${obBandLabel(bandBp)} 盘口不平衡 ${imbalance} · ${coverageSummary}`}
        aria-label={`${obBandLabel(bandBp)} 盘口不平衡，卖方 ${askShare}，买方 ${bidShare}`}
      >
        <span className="ob-imbalance-rail__side ob-imbalance-rail__side--ask">
          <i
            className="ob-imbalance-rail__fill ob-imbalance-rail__fill--ask"
            style={{ height: shareHeight(askDepth) }}
            aria-hidden="true"
          />
          <b>{askShare}</b>
        </span>
        <span className="ob-imbalance-rail__side ob-imbalance-rail__side--bid">
          <i
            className="ob-imbalance-rail__fill ob-imbalance-rail__fill--bid"
            style={{ height: shareHeight(bidDepth) }}
            aria-hidden="true"
          />
          <b>{bidShare}</b>
        </span>
      </aside>
    </div>
  );
}

export const OrderbookLadder = memo(OrderbookLadderImpl);
