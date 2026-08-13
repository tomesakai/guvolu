// 展示层统一转东京时区
const JST_ZONE = "Asia/Tokyo";
const ZONE_SUFFIX_RE = /(?:Z|[+-]\d{2}:?\d{2})$/i;

export const JST_LABEL = "JST";
// 空值统一破折号，禁止零与空白
export const EMPTY_TEXT = "—";
// 元数据分隔符
export const META_SEPARATOR = " · ";
// 时钟偏移容许上限
export const CLOCK_DRIFT_LIMIT_SECONDS = 5;
// 偏移显示小数位
const DRIFT_DIGITS = 1;

const SECOND_MS = 1000;
const MINUTE_MS = 60000;
const HOUR_MS = 3600000;

const JST_FORMAT = new Intl.DateTimeFormat("en-US", {
  timeZone: JST_ZONE,
  hourCycle: "h23",
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
});

interface JstParts {
  readonly year: string;
  readonly month: string;
  readonly day: string;
  readonly hour: string;
  readonly minute: string;
  readonly second: string;
}

/** 解析后端的 UTC 时刻字符串，无效时返回空值。缺时区标识时按 UTC 处理。 */
export function parseUtcIso(iso: string | null | undefined): Date | null {
  if (typeof iso !== "string") {
    return null;
  }
  const text = iso.trim();
  if (text === "") {
    return null;
  }
  const normalized = ZONE_SUFFIX_RE.test(text) ? text : `${text}Z`;
  const date = new Date(normalized);
  return Number.isNaN(date.getTime()) ? null : date;
}

function jstParts(iso: string | null | undefined): JstParts | null {
  const date = parseUtcIso(iso);
  if (date === null) {
    return null;
  }
  const found = new Map<string, string>();
  for (const part of JST_FORMAT.formatToParts(date)) {
    found.set(part.type, part.value);
  }
  const pick = (key: string): string => found.get(key) ?? "";
  return {
    year: pick("year"),
    month: pick("month"),
    day: pick("day"),
    hour: pick("hour"),
    minute: pick("minute"),
    second: pick("second"),
  };
}

/** 时刻转时分秒，时区由状态条统一声明。 */
export function jstClock(iso: string | null | undefined): string {
  const parts = jstParts(iso);
  return parts === null
    ? EMPTY_TEXT
    : `${parts.hour}:${parts.minute}:${parts.second}`;
}

/** 时刻转月日与时分秒。 */
export function jstStamp(iso: string | null | undefined): string {
  const parts = jstParts(iso);
  return parts === null
    ? EMPTY_TEXT
    : `${parts.month}-${parts.day} ${parts.hour}:${parts.minute}:${parts.second}`;
}

/** 时刻转时分，供 K 线横轴使用。 */
export function jstHourMinute(iso: string | null | undefined): string {
  const parts = jstParts(iso);
  return parts === null ? EMPTY_TEXT : `${parts.hour}:${parts.minute}`;
}

/** 时刻转年份，供图表刻度使用。 */
export function jstYear(iso: string | null | undefined): string {
  const parts = jstParts(iso);
  return parts === null ? EMPTY_TEXT : parts.year;
}

/** 时刻转年月，供图表刻度使用。 */
export function jstYearMonth(iso: string | null | undefined): string {
  const parts = jstParts(iso);
  return parts === null ? EMPTY_TEXT : `${parts.year}-${parts.month}`;
}

/** 时刻转月日，供图表刻度使用。 */
export function jstMonthDay(iso: string | null | undefined): string {
  const parts = jstParts(iso);
  return parts === null ? EMPTY_TEXT : `${parts.month}-${parts.day}`;
}

/** 秒级时戳转 ISO，图表时间为 UTC 秒。 */
export function epochToIso(seconds: number): string {
  return new Date(seconds * SECOND_MS).toISOString();
}

/** 距今时长，供陈旧徽章显示。 */
export function relativeAge(iso: string | null | undefined): string {
  const date = parseUtcIso(iso);
  if (date === null) {
    return EMPTY_TEXT;
  }
  const delta = Math.max(0, Date.now() - date.getTime());
  if (delta < MINUTE_MS) {
    return `${String(Math.floor(delta / SECOND_MS))}s`;
  }
  if (delta < HOUR_MS) {
    return `${String(Math.floor(delta / MINUTE_MS))}m`;
  }
  return `${String(Math.floor(delta / HOUR_MS))}h`;
}

const DAY_RE = /^\d{8}$/;

function parseDay(day: string): Date | null {
  if (!DAY_RE.test(day)) {
    return null;
  }
  const value = new Date(
    Date.UTC(
      Number(day.slice(0, 4)),
      Number(day.slice(4, 6)) - 1,
      Number(day.slice(6, 8)),
    ),
  );
  return Number.isNaN(value.getTime()) ? null : value;
}

function formatDay(value: Date): string {
  const year = String(value.getUTCFullYear()).padStart(4, "0");
  const month = String(value.getUTCMonth() + 1).padStart(2, "0");
  const date = String(value.getUTCDate()).padStart(2, "0");
  return `${year}${month}${date}`;
}

/** 交易日回推自然日，格式 YYYYMMDD，非法输入原样返回。 */
export function shiftDays(day: string, back: number): string {
  const value = parseDay(day);
  if (value === null) {
    return day;
  }
  value.setUTCDate(value.getUTCDate() - back);
  return formatDay(value);
}

/** 交易日回推整年，格式 YYYYMMDD，非法输入原样返回。 */
export function shiftYears(day: string, back: number): string {
  const value = parseDay(day);
  if (value === null) {
    return day;
  }
  value.setUTCFullYear(value.getUTCFullYear() - back);
  return formatDay(value);
}

/** 金额与数量原样显示，缺失时显示破折号。 */
export function decimalText(value: string | null | undefined): string {
  if (typeof value !== "string" || value.trim() === "") {
    return EMPTY_TEXT;
  }
  return value;
}

/** 整数标识原样显示，缺失时显示破折号。 */
export function idText(value: number | null | undefined): string {
  return typeof value === "number" && Number.isFinite(value)
    ? String(value)
    : EMPTY_TEXT;
}

/** 官方枚举原样显示，缺失时显示破折号。 */
export function rawText(value: string | null | undefined): string {
  if (typeof value !== "string" || value.trim() === "") {
    return EMPTY_TEXT;
  }
  return value;
}

/** 十进制字符串转数值，仅用于绘图与比例，非法返回空。 */
export function plotNumber(text: string | null | undefined): number | null {
  if (typeof text !== "string" || text.trim() === "") {
    return null;
  }
  const parsed = Number(text);
  return Number.isFinite(parsed) ? parsed : null;
}

/** 十进制字符串的小数位数，按字面统计不经浮点。 */
export function decimalPlaces(text: string): number {
  const trimmed = text.trim();
  const dot = trimmed.indexOf(".");
  return dot < 0 ? 0 : trimmed.length - dot - 1;
}

// 数值基准两态：数量为缺省
export const BASIS_SIZE = "size";
export const BASIS_NOTIONAL = "notional";
// 金额基准显示步长（JPY 整数）
export const NOTIONAL_STEP = "1";

/** 显示开关：设置页写入，只改呈现不改来源。 */
export interface FormatSwitches {
  readonly groupDigits: boolean;
  readonly abbrevTotals: boolean;
  readonly valueBasis: string;
}

// 缺省值即设计语言裁定形态
export const DEFAULT_FORMAT_SWITCHES: FormatSwitches = {
  groupDigits: true,
  abbrevTotals: true,
  valueBasis: BASIS_SIZE,
};

// 当前生效的显示开关
let activeSwitches: FormatSwitches = DEFAULT_FORMAT_SWITCHES;

/** 写入显示开关，其后的变换即时生效。 */
export function applyFormatSwitches(next: FormatSwitches): void {
  activeSwitches = next;
}

// 千分位分隔符与分组位数
const GROUP_MARK = ",";
const GROUP_SIZE = 3;
// 缩写档：先百万后千
const ABBREV_TIERS: readonly { mark: string; digits: number }[] = [
  { mark: "M", digits: 6 },
  { mark: "k", digits: 3 },
];
const ABBREV_PLACES = 1;
const ZERO_CODE = 48;
const ROUND_HALF = 5;
const TEN = 10;
const DECIMAL_RE = /^([+-]?)(\d*)(?:\.(\d*))?$/;
const TRAIL_ZERO_RE = /0+$/;
const LEAD_ZERO_RE = /^0+/;

interface DecimalParts {
  readonly sign: string;
  readonly int: string;
  readonly frac: string;
}

/** 拆十进制字符串为符号、整数、小数三段，全程不经浮点。 */
function splitDecimal(value: string | null | undefined): DecimalParts | null {
  if (typeof value !== "string") {
    return null;
  }
  const found = DECIMAL_RE.exec(value.trim());
  if (found === null) {
    return null;
  }
  const int = found[2] ?? "";
  const frac = found[3] ?? "";
  if (int === "" && frac === "") {
    return null;
  }
  return {
    sign: found[1] === "-" ? "-" : "",
    int: int === "" ? "0" : int,
    frac,
  };
}

/** 取舍到指定小数位，逐字符进位不经浮点。 */
function roundTo(parts: DecimalParts, places: number): DecimalParts {
  if (parts.frac.length <= places) {
    return { ...parts, frac: parts.frac.padEnd(places, "0") };
  }
  const keep = parts.frac.slice(0, places);
  if (parts.frac.charCodeAt(places) - ZERO_CODE < ROUND_HALF) {
    return { ...parts, frac: keep };
  }
  const digits = `${parts.int}${keep}`.split("");
  let at = digits.length - 1;
  while (at >= 0) {
    const raised = (digits[at] ?? "0").charCodeAt(0) - ZERO_CODE + 1;
    if (raised < TEN) {
      digits[at] = String(raised);
      break;
    }
    digits[at] = "0";
    at -= 1;
  }
  if (at < 0) {
    digits.unshift("1");
  }
  const joined = digits.join("");
  const cut = joined.length - places;
  return {
    sign: parts.sign,
    int: joined.slice(0, cut),
    frac: joined.slice(cut),
  };
}

/** 整数段插入千分位，仅为显示分组。 */
function groupInt(int: string): string {
  let out = "";
  let at = int.length;
  while (at > GROUP_SIZE) {
    out = `${GROUP_MARK}${int.slice(at - GROUP_SIZE, at)}${out}`;
    at -= GROUP_SIZE;
  }
  return `${int.slice(0, at)}${out}`;
}

/** 三段合回文本，可选截尾零与整数分组。 */
function joinParts(parts: DecimalParts, trim: boolean, group: boolean): string {
  const frac = trim ? parts.frac.replace(TRAIL_ZERO_RE, "") : parts.frac;
  const int = group ? groupInt(parts.int) : parts.int;
  return `${parts.sign}${frac === "" ? int : `${int}.${frac}`}`;
}

/** 价格：按 tickSize 精度补位，分组受显示开关控制。 */
export function priceText(
  value: string | null | undefined,
  tickSize: string | null | undefined,
): string {
  const parts = splitDecimal(value);
  if (parts === null) {
    return EMPTY_TEXT;
  }
  return joinParts(
    roundTo(parts, decimalPlaces(tickSize ?? "")),
    false,
    activeSwitches.groupDigits,
  );
}

/** 轴价格：数值按 tickSize 精度转文本，分组受显示开关控制。 */
export function axisPriceText(
  value: number,
  tickSize: string | null | undefined,
): string {
  if (!Number.isFinite(value)) {
    return EMPTY_TEXT;
  }
  const places = decimalPlaces(tickSize ?? "");
  return priceText(value.toFixed(places), tickSize);
}

/** 数量：按 sizeStep 精度取舍并截去尾零。 */
export function sizeText(
  value: string | null | undefined,
  sizeStep: string | null | undefined,
): string {
  const parts = splitDecimal(value);
  if (parts === null) {
    return EMPTY_TEXT;
  }
  return joinParts(roundTo(parts, decimalPlaces(sizeStep ?? "")), true, true);
}

/** 金额：小数取原文截尾零，分组受显示开关控制。 */
export function amountText(value: string | null | undefined): string {
  const parts = splitDecimal(value);
  if (parts === null) {
    return EMPTY_TEXT;
  }
  return joinParts(parts, true, activeSwitches.groupDigits);
}

/** 计数：千分位整数。 */
export function tallyText(value: number | null | undefined): string {
  return typeof value === "number" && Number.isFinite(value)
    ? groupInt(String(Math.trunc(Math.abs(value))))
    : EMPTY_TEXT;
}

/** 合计与量：缩写开关开时达千位转 k 或 M，否则完整形。 */
export function totalText(
  value: string | null | undefined,
  step: string | null | undefined,
): string {
  const parts = splitDecimal(value);
  if (parts === null) {
    return EMPTY_TEXT;
  }
  if (activeSwitches.abbrevTotals) {
    const plain = parts.int.replace(LEAD_ZERO_RE, "");
    for (const tier of ABBREV_TIERS) {
      if (plain.length > tier.digits) {
        const cut = plain.length - tier.digits;
        const shifted: DecimalParts = {
          sign: parts.sign,
          int: plain.slice(0, cut),
          frac: `${plain.slice(cut)}${parts.frac}`,
        };
        const head = joinParts(roundTo(shifted, ABBREV_PLACES), false, true);
        return `${head}${tier.mark}`;
      }
    }
  }
  return joinParts(roundTo(parts, decimalPlaces(step ?? "")), true, true);
}

/** 轴合计：数值按 sizeStep 精度取舍后走量与合计变换，供量窗格刻度。 */
export function axisTotalText(
  value: number,
  sizeStep: string | null | undefined,
): string {
  if (!Number.isFinite(value)) {
    return EMPTY_TEXT;
  }
  const places = decimalPlaces(sizeStep ?? "");
  return totalText(value.toFixed(places), sizeStep);
}

const TEN_BIG = 10n;
const ZERO_BIG = 0n;
// bp 换算幂：万分乘两位小数
const BP_SHIFT = 6;
const BP_PLACES = 2;

/** 十进制字符串转定点整数，超出位数截尾。 */
function toFixedInt(
  value: string | null | undefined,
  places: number,
): bigint | null {
  const parts = splitDecimal(value);
  if (parts === null) {
    return null;
  }
  const frac = parts.frac.slice(0, places).padEnd(places, "0");
  const magnitude = BigInt(`${parts.int}${frac}`);
  return parts.sign === "-" ? -magnitude : magnitude;
}

/** 定点整数回三段形态。 */
function fromFixedInt(value: bigint, places: number): DecimalParts {
  const negative = value < ZERO_BIG;
  const digits = (negative ? -value : value)
    .toString()
    .padStart(places + 1, "0");
  const cut = digits.length - places;
  return {
    sign: negative ? "-" : "",
    int: digits.slice(0, cut),
    frac: digits.slice(cut),
  };
}

/** 数值按最小单位换算为精确计数，无法整除则显示空值。 */
export function unitCountText(
  value: string | null | undefined,
  unit: string | null | undefined,
): string {
  const places = Math.max(decimalPlaces(value ?? ""), decimalPlaces(unit ?? ""));
  const valueInt = toFixedInt(value, places);
  const unitInt = toFixedInt(unit, places);
  if (valueInt === null || unitInt === null || unitInt <= ZERO_BIG) {
    return EMPTY_TEXT;
  }
  if (valueInt % unitInt !== ZERO_BIG) {
    return EMPTY_TEXT;
  }
  return joinParts(fromFixedInt(valueInt / unitInt, 0), false, false);
}

/** 金额基准：价乘量定点整数乘法，截尾到整数位。 */
export function notionalText(
  price: string | null | undefined,
  size: string | null | undefined,
  tickSize: string | null | undefined,
  sizeStep: string | null | undefined,
): string {
  const pricePlaces = decimalPlaces(tickSize ?? "");
  const sizePlaces = decimalPlaces(sizeStep ?? "");
  const priceInt = toFixedInt(price, pricePlaces);
  const sizeInt = toFixedInt(size, sizePlaces);
  if (priceInt === null || sizeInt === null) {
    return EMPTY_TEXT;
  }
  const product = fromFixedInt(priceInt * sizeInt, pricePlaces + sizePlaces);
  return joinParts(
    { ...product, frac: "" },
    false,
    activeSwitches.groupDigits,
  );
}

/** 价差定点相减，按 tickSize 精度，零浮点。 */
export function priceDiffText(
  left: string | null | undefined,
  right: string | null | undefined,
  tickSize: string | null | undefined,
): string {
  const places = decimalPlaces(tickSize ?? "");
  const leftInt = toFixedInt(left, places);
  const rightInt = toFixedInt(right, places);
  if (leftInt === null || rightInt === null) {
    return EMPTY_TEXT;
  }
  return joinParts(
    fromFixedInt(leftInt - rightInt, places),
    false,
    activeSwitches.groupDigits,
  );
}

/** 比值转基点：定点整除截尾两位小数。 */
export function bpText(
  numerator: string | null | undefined,
  denominator: string | null | undefined,
): string {
  const numParts = splitDecimal(numerator);
  const denParts = splitDecimal(denominator);
  if (numParts === null || denParts === null) {
    return EMPTY_TEXT;
  }
  const numPlaces = numParts.frac.length;
  const denPlaces = denParts.frac.length;
  const numInt = toFixedInt(numerator, numPlaces);
  const denInt = toFixedInt(denominator, denPlaces);
  if (numInt === null || denInt === null || denInt === ZERO_BIG) {
    return EMPTY_TEXT;
  }
  const shift = denPlaces + BP_SHIFT - numPlaces;
  const scaledNum =
    shift >= 0 ? numInt * TEN_BIG ** BigInt(shift) : numInt;
  const scaledDen =
    shift >= 0 ? denInt : denInt * TEN_BIG ** BigInt(-shift);
  return joinParts(fromFixedInt(scaledNum / scaledDen, BP_PLACES), false, false);
}

/** 两值之差对基值的基点，定点截尾两位。 */
export function bpDiffText(
  value: string | null | undefined,
  base: string | null | undefined,
): string {
  const valueParts = splitDecimal(value);
  const baseParts = splitDecimal(base);
  if (valueParts === null || baseParts === null) {
    return EMPTY_TEXT;
  }
  const places = Math.max(valueParts.frac.length, baseParts.frac.length);
  const valueInt = toFixedInt(value, places);
  const baseInt = toFixedInt(base, places);
  if (valueInt === null || baseInt === null || baseInt === ZERO_BIG) {
    return EMPTY_TEXT;
  }
  const scaled = (valueInt - baseInt) * TEN_BIG ** BigInt(BP_SHIFT);
  return joinParts(fromFixedInt(scaled / baseInt, BP_PLACES), false, false);
}

/** 数量定列：小数位按 sizeStep 恒定，超出截尾。 */
export function sizeFixedText(
  value: string | null | undefined,
  sizeStep: string | null | undefined,
): string {
  const parts = splitDecimal(value);
  if (parts === null) {
    return EMPTY_TEXT;
  }
  const places = decimalPlaces(sizeStep ?? "");
  const frac = parts.frac.slice(0, places).padEnd(places, "0");
  return joinParts({ ...parts, frac }, false, true);
}

/** 同精度数量求和，定点整数加法零浮点。 */
export function sumSizeTexts(
  values: readonly string[],
  sizeStep: string | null | undefined,
): string {
  const places = decimalPlaces(sizeStep ?? "");
  let total = ZERO_BIG;
  for (const value of values) {
    const fixed = toFixedInt(value, places);
    if (fixed !== null) {
      total += fixed;
    }
  }
  return joinParts(fromFixedInt(total, places), true, false);
}

/** 金额合计：逐档价乘量定点累计，截尾到整数。 */
export function sumNotionalTexts(
  levels: readonly { price: string; size: string }[],
  tickSize: string | null | undefined,
  sizeStep: string | null | undefined,
): string {
  const pricePlaces = decimalPlaces(tickSize ?? "");
  const sizePlaces = decimalPlaces(sizeStep ?? "");
  let total = ZERO_BIG;
  for (const level of levels) {
    const priceInt = toFixedInt(level.price, pricePlaces);
    const sizeInt = toFixedInt(level.size, sizePlaces);
    if (priceInt !== null && sizeInt !== null) {
      total += priceInt * sizeInt;
    }
  }
  const product = fromFixedInt(total, pricePlaces + sizePlaces);
  return joinParts({ ...product, frac: "" }, false, false);
}

/** 价格档：按档宽向下对齐，返回档价文本。 */
export function priceBinText(
  price: string | null | undefined,
  bin: string | null | undefined,
): string | null {
  const priceParts = splitDecimal(price);
  const binParts = splitDecimal(bin);
  if (priceParts === null || binParts === null) {
    return null;
  }
  const places = Math.max(priceParts.frac.length, binParts.frac.length);
  const priceInt = toFixedInt(price, places);
  const binInt = toFixedInt(bin, places);
  if (priceInt === null || binInt === null || binInt <= ZERO_BIG) {
    return null;
  }
  const aligned = (priceInt / binInt) * binInt;
  return joinParts(fromFixedInt(aligned, places), true, false);
}

/** 档宽乘整数档位，返回档宽文本。 */
export function binTimesText(
  tickSize: string | null | undefined,
  factor: number,
): string | null {
  const places = decimalPlaces(tickSize ?? "");
  const tickInt = toFixedInt(tickSize, places);
  if (tickInt === null || tickInt <= ZERO_BIG) {
    return null;
  }
  return joinParts(
    fromFixedInt(tickInt * BigInt(factor), places),
    true,
    false,
  );
}

const ZERO_RE = /^[+-]?0*(?:\.0*)?$/;

/** 判定十进制字符串是否为零，仅用于降低行的视觉权重。 */
export function isZeroDecimal(value: string | null | undefined): boolean {
  return (
    typeof value === "string" &&
    value.trim() !== "" &&
    ZERO_RE.test(value.trim())
  );
}

/** 时钟偏移紧凑形，带符号一位小数，如 +0.5s。 */
export function driftText(seconds: number | null | undefined): string {
  if (typeof seconds !== "number" || !Number.isFinite(seconds)) {
    return EMPTY_TEXT;
  }
  const sign = seconds < 0 ? "-" : "+";
  return `${sign}${Math.abs(seconds).toFixed(DRIFT_DIGITS)}s`;
}

/** 判定时钟偏移是否越过上限。 */
export function driftExceeded(seconds: number | null | undefined): boolean {
  return (
    typeof seconds === "number" &&
    Number.isFinite(seconds) &&
    Math.abs(seconds) > CLOCK_DRIFT_LIMIT_SECONDS
  );
}

const ORDER_TYPE_LABELS = new Map<string, string>([
  ["LIMIT", "限价"],
  ["MARKET", "市价"],
  ["STOP", "止损"],
]);

/** 委托类型转中文，未登记的类型原样显示。 */
export function orderTypeLabel(value: string | null | undefined): string {
  if (typeof value !== "string" || value.trim() === "") {
    return EMPTY_TEXT;
  }
  return ORDER_TYPE_LABELS.get(value) ?? value;
}
