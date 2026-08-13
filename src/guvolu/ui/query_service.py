"""查看版查询服务：只读代理 READ_ONLY 与公开端点（TBD-13 已锁定形态）。

进程定位（X-01）：独立于执行进程，UI 经本服务取数，零密钥进浏览器。
鉴权（TBD-14 已锁定）：仅绑定 127.0.0.1，请求须带本地令牌头。
本阶段无状态存储，数据为实时转发；执行进程出现后改读状态存储。
操作端点仅覆盖白名单采集进程（TBD-31），不接受任何自由命令参数；
本服务启动时不自动拉起任何进程，拉起一律经端点显式触发。
"""
from __future__ import annotations

import json
import logging
import secrets
import threading
import time
from dataclasses import asdict, is_dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse

from guvolu.api.public_client import PublicClient
from guvolu.api.read_client import ReadClient
from guvolu.api.bitflyer_read_client import BitflyerReadClient
from guvolu.domain.config import Config, load_config
from guvolu.domain.enums import KlineInterval
from guvolu.domain.errors import ApiHttpError, ApiNetworkError, ApiSchemaError, GmoApiError
from guvolu.data.book_features import (
    BAND_SOURCE_REQUEST,
    BAND_SOURCE_RULE_BP,
    CONFIDENCE_VERSION,
    JUDGMENT_LABELS,
    METRIC_LABELS,
    SIGNAL_CODE_VERSION,
    AlertRule,
    BandSample,
    analyze_region,
    band_sample,
    load_alert_rules,
    match_rules,
    region_config_hash,
    rule_band,
)
from guvolu.data.footprint import (
    BIN_TIERS,
    INTERVAL_SECONDS as FOOTPRINT_INTERVALS,
    build_footprint,
    dedupe_ws_rows,
    infer_sides,
    load_recent_trade_rows,
)
from guvolu.data.heatmap_tiles import (
    SIDE_BASIS,
    IncrementalTileRegistry,
    MAX_TRACK_COLUMNS,
    TILE_BUCKETS,
    VENUE_GMO,
    iter_tile_columns,
    level_track,
    load_print_ticks,
    load_tile_meta,
    slice_columns,
    window_dates,
)
from guvolu.data.kline_plan import YEARLY_INTERVALS
from guvolu.data.store import (
    ack_alert_event,
    connect,
    connect_readonly,
    insert_analysis_run,
    insert_alert_event,
    insert_book_feature,
    list_alert_events,
    list_analysis_runs,
    query_klines,
)
from guvolu.domain.models import Kline, Orderbook
from guvolu.domain.ids import new_run_id, sha256_hex
from guvolu.ops.process_manager import DEFAULT_REGISTRY, ProcessManager
from guvolu.ui.book_heatmap import build_heatmap, load_recent_book_frames
from guvolu.ui.book_metrics import snapshot_metrics
from guvolu.ui.capabilities import IMPLEMENTED, PENDING
from guvolu.ui.cross_venue_query import (
    CrossVenueCompatibilityError,
    CrossVenueQuery,
    CrossVenueQueryError,
)
from guvolu.ui.materialized_query import (
    FOOTPRINT_INTERVALS as V2_FOOTPRINT_INTERVALS,
    MAX_KLINES as V2_MAX_KLINES,
    MAX_TILE_COLUMNS as V2_MAX_TILE_COLUMNS,
    MAX_TRADES as V2_MAX_TRADES,
    MaterializedQuery,
    MaterializedQueryError,
)
from guvolu.ui.query_catalog import CATALOG_SCHEMA_VERSION, QueryCatalog

BIND_HOST = "127.0.0.1"
PORT = 8721
# 令牌文件名（TBD-14）
TOKEN_FILE_NAME = "ui-token.txt"
TOKEN_HEADER = "X-Guvolu-Token"
# 交易日界见 D-08
TRADING_DAY_OFFSET = timedelta(hours=9 - 6)
# 时钟偏移仅展示，不在查询服务拒绝启动
CLOCK_DISPLAY_LIMIT_SECONDS = 3600.0
# K 线单次请求的上游调用上限（R-04）
MAX_KLINE_REQUESTS = 24
# 缺数据错误码，聚合跳过
KLINE_NOT_FOUND = "ERR-5207"
# 区域分析窗口日数上限
REGION_MAX_WINDOW_DAYS = 3
# 区域分析数值基准（现仅数量基准）
REGION_BASIS = "quantity"
# 台账清单单次返回上限
ANALYSIS_RUN_LIST_LIMIT = 200
# 报警清单单次返回上限
ALERT_LIST_LIMIT = 200
# 判读事件清单单次返回上限
FEATURE_LIST_LIMIT = 200
# 报警规则配置目录缺省
CONFIG_DIR_DEFAULT = Path("config")
# 近窗逐笔缺省秒窗
RECENT_TRADES_DEFAULT_SECONDS = 60
# 完结日瓦片不可变缓存头
IMMUTABLE_CACHE = "public, max-age=31536000, immutable"
NO_STORE_CACHE = "no-store"
V2_REVALIDATE_CACHE = "private, no-cache"
# MON 盘口缓存秒数
ORDERBOOK_CACHE_SECONDS = 0.5

_ALLOWED_HOSTS = frozenset({"127.0.0.1", "localhost"})


def ensure_token(log_dir: Path) -> str:
    """读取或生成本地令牌文件（TBD-14）。"""
    log_dir.mkdir(parents=True, exist_ok=True)
    token_path = log_dir / TOKEN_FILE_NAME
    if token_path.exists():
        existing = token_path.read_text(encoding="utf-8").strip()
        if existing:
            return existing
    token = secrets.token_urlsafe(24)
    token_path.write_text(token + "\n", encoding="utf-8")
    return token


def jsonable(value: object) -> object:
    """领域对象转 JSON 形态。金额保持字符串（D-07）。"""
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value) and not isinstance(value, type):
        return {key: jsonable(item) for key, item in asdict(value).items()}
    if isinstance(value, (list, tuple)):
        return [jsonable(item) for item in value]
    if isinstance(value, dict):
        return {str(key): jsonable(item) for key, item in value.items()}
    return value


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def jst_kline_date(now: datetime) -> str:
    """按 JST 06:00 交易日界计算 K 线日期（D-08、C-11）。"""
    return (now.astimezone(UTC) + TRADING_DAY_OFFSET).strftime("%Y%m%d")


def kline_dates(
    interval: KlineInterval, from_day: str, to_day: str
) -> tuple[list[str], bool]:
    """列举上游拉取日期或年份，超上限截断并保留最新。

    分钟与小时周期按交易日逐日；其余周期按年（官方参数约定）。
    """
    if interval in YEARLY_INTERVALS:
        dates = [
            str(year) for year in range(int(from_day[:4]), int(to_day[:4]) + 1)
        ]
    else:
        start = datetime.strptime(from_day, "%Y%m%d")
        end = datetime.strptime(to_day, "%Y%m%d")
        if end < start:
            raise ValueError("起始日不得晚于结束日")
        dates = []
        cursor = start
        while cursor <= end:
            dates.append(cursor.strftime("%Y%m%d"))
            cursor += timedelta(days=1)
    truncated = len(dates) > MAX_KLINE_REQUESTS
    return dates[-MAX_KLINE_REQUESTS:], truncated


def window_epochs(from_ts: str, to_ts: str) -> tuple[int, int]:
    """解析窗口起止参数为 UTC 秒时戳。"""
    try:
        start = datetime.fromisoformat(from_ts)
        end = datetime.fromisoformat(to_ts)
    except ValueError:
        raise HTTPException(status_code=400, detail="时刻非法") from None
    if start.tzinfo is None or end.tzinfo is None:
        raise HTTPException(status_code=400, detail="时刻须带时区")
    if start >= end:
        raise HTTPException(status_code=400, detail="窗口起止倒置")
    return int(start.timestamp()), int(end.timestamp())


def window_coverage(
    columns: list[dict[str, object]],
    from_s: int,
    to_s: int,
    bucket_seconds: int | None,
) -> dict[str, object]:
    """请求窗对瓦片视界的实际覆盖与截断旗标。

    空档列是显式录制事实，计入覆盖；覆盖起讫
    只由在场列决定，超出视界即标截断（缺陷 6）。
    """
    epochs = [
        epoch
        for column in columns
        if isinstance(epoch := column.get("e"), int)
    ]
    if not epochs or bucket_seconds is None:
        return {
            "coverage_clipped": True,
            "coverage_from": None,
            "coverage_to": None,
        }
    first = min(epochs)
    last = max(epochs) + bucket_seconds
    return {
        "coverage_clipped": first > from_s or last < to_s,
        "coverage_from": datetime.fromtimestamp(first, UTC).isoformat(),
        "coverage_to": datetime.fromtimestamp(last, UTC).isoformat(),
    }


def window_mid_median(columns: list[dict[str, object]]) -> Decimal | None:
    """窗内非空非延载列中间价中位数，偶数取下中位。"""
    mids = sorted(
        Decimal(str(column["mid"]))
        for column in columns
        if not column.get("gap")
        and not column.get("carried")
        and isinstance(column.get("mid"), str)
    )
    if not mids:
        return None
    return mids[(len(mids) - 1) // 2]


def create_app(
    config: Config,
    public: PublicClient,
    read: ReadClient,
    token: str,
    bitflyer_read: BitflyerReadClient | None = None,
    data_root: Path | None = None,
    process_manager: ProcessManager | None = None,
    config_dir: Path | None = None,
) -> FastAPI:
    """组装查询服务。客户端由外部注入，测试可替换（C-13）。

    data_root 给定时 K 线优先读库存，仅当期走上游刷新。
    进程管理器缺省按白名单登记表构造，且不自动拉起任何进程。
    config_dir 为报警规则实例配置目录（6.8 节四元组）。
    """
    app = FastAPI(title="guvolu 查询服务（查看版）", docs_url=None, redoc_url=None)
    app.state.data_root = data_root
    run_id = new_run_id()
    orderbook_cache: dict[str, tuple[float, Orderbook, str]] = {}
    orderbook_cache_lock = threading.Lock()
    rules_dir = config_dir if config_dir is not None else CONFIG_DIR_DEFAULT
    materialized_query = (
        MaterializedQuery(data_root) if data_root is not None else None
    )
    cross_venue_query = (
        CrossVenueQuery(materialized_query)
        if materialized_query is not None else None
    )
    manager = (
        process_manager
        if process_manager is not None
        else ProcessManager(
            DEFAULT_REGISTRY,
            data_root=data_root if data_root is not None else Path("data"),
            log_dir=config.log_dir,
        )
    )

    @app.middleware("http")
    async def guard(request: Request, call_next: Any) -> Response:
        """本机来源与令牌校验（TBD-14）。"""
        host = request.headers.get("host", "").split(":")[0]
        if host not in _ALLOWED_HOSTS:
            return JSONResponse(status_code=403, content={"detail": "非本机来源"})
        if request.url.path.startswith("/api"):
            if request.headers.get(TOKEN_HEADER, "") != token:
                return JSONResponse(status_code=401, content={"detail": "令牌无效"})
        result: Response = await call_next(request)
        return result

    @app.exception_handler(GmoApiError)
    async def gmo_error(_: Request, exc: GmoApiError) -> JSONResponse:
        """交易所业务错误透传错误码，处置见错误处置册。"""
        code = exc.codes[0] if exc.codes else ""
        return JSONResponse(
            status_code=502,
            content={"detail": {"code": code, "message": str(exc)}},
        )

    @app.exception_handler(ApiNetworkError)
    async def network_error(_: Request, exc: ApiNetworkError) -> JSONResponse:
        """网络失败以 504 表达，前端按陈旧处理（X-06）。"""
        return JSONResponse(
            status_code=504,
            content={"detail": {"code": "NETWORK", "message": str(exc)}},
        )

    @app.get("/api/health")
    def health() -> dict[str, object]:
        """服务自身状态与运行模式（X-02 数据源）。"""
        return {
            "mode": config.mode.value,
            "frontend_mode": "view",
            "server_time": _now_iso(),
            "run_id": run_id,
        }

    @app.get("/api/capabilities")
    def capabilities() -> dict[str, object]:
        """现有能力范围清单。"""
        return {
            "implemented": jsonable(IMPLEMENTED),
            "pending": jsonable(PENDING),
            "generated_at": _now_iso(),
        }

    @app.get("/api/v2/markets")
    def markets_v2() -> dict[str, object]:
        """市场身份、活动数据覆盖与最新来源能力的只读目录。"""
        if data_root is None:
            raise HTTPException(status_code=404, detail="无本地数据目录")
        snapshot = QueryCatalog(data_root).snapshot()
        return {
            "schema_version": CATALOG_SCHEMA_VERSION,
            "generated_at": _now_iso(),
            **snapshot,
        }

    def v2_window(
        from_ts: str | None, to_ts: str | None, default_seconds: int,
    ) -> tuple[datetime, datetime]:
        """解析 v2 半开 UTC 窗口；缺省以当前时刻向前取。"""
        now = datetime.now(UTC)

        def parsed(value: str | None, fallback: datetime) -> datetime:
            if value is None:
                return fallback
            try:
                result = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                raise HTTPException(status_code=400, detail="时间格式非法") from None
            if result.tzinfo is None:
                raise HTTPException(status_code=400, detail="时间必须带时区")
            return result.astimezone(UTC)

        end = parsed(to_ts, now)
        start = parsed(from_ts, end - timedelta(seconds=default_seconds))
        if start >= end:
            raise HTTPException(status_code=400, detail="from_ts 必须早于 to_ts")
        return start, end

    def v2_result(
        request: Request, payload: dict[str, object], etag: str,
    ) -> Response:
        """活动 head 响应统一使用 ETag 重验证，不承诺路径永久不可变。"""
        meta = payload.get("meta")
        generation = str(
            meta.get("head_generation", "") if isinstance(meta, dict) else ""
        )
        headers = {
            "ETag": etag,
            "Cache-Control": V2_REVALIDATE_CACHE,
            "X-Guvolu-Head-Generation": generation,
        }
        if request.headers.get("if-none-match") == etag:
            return Response(status_code=304, headers=headers)
        return JSONResponse(content=jsonable(payload), headers=headers)

    def v2_engine() -> MaterializedQuery:
        if materialized_query is None:
            raise HTTPException(status_code=404, detail="无本地数据目录")
        return materialized_query

    @app.get("/api/v2/markets/{market_id}/klines")
    def market_klines_v2(
        market_id: str,
        request: Request,
        interval: str = "1hour",
        from_ts: str | None = Query(default=None, alias="from"),
        to_ts: str | None = Query(default=None, alias="to"),
        limit: int = Query(default=5000, ge=1, le=V2_MAX_KLINES),
    ) -> Response:
        """只读活动 ``market_kline``，按市场身份返回来源原生 K线。"""
        start, end = v2_window(from_ts, to_ts, 30 * 86_400)
        try:
            payload, etag = v2_engine().klines(
                market_id, interval, start, end, limit,
            )
        except LookupError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from None
        except (FileNotFoundError, MaterializedQueryError) as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from None
        return v2_result(request, payload, etag)

    @app.get("/api/v2/markets/{market_id}/trades")
    def market_trades_v2(
        market_id: str,
        request: Request,
        from_ts: str | None = Query(default=None, alias="from"),
        to_ts: str | None = Query(default=None, alias="to"),
        limit: int = Query(default=5000, ge=1, le=V2_MAX_TRADES),
    ) -> Response:
        """只读活动历史/实时成交事实，按 observation_id 去重。"""
        start, end = v2_window(from_ts, to_ts, 3600)
        try:
            payload, etag = v2_engine().trades(market_id, start, end, limit)
        except LookupError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from None
        except (FileNotFoundError, MaterializedQueryError) as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from None
        return v2_result(request, payload, etag)

    @app.get("/api/v2/markets/{market_id}/footprint")
    def market_footprint_v2(
        market_id: str,
        request: Request,
        interval: str = "15min",
        price_bin: str | None = None,
        from_ts: str | None = Query(default=None, alias="from"),
        to_ts: str | None = Query(default=None, alias="to"),
    ) -> Response:
        """从活动逐笔事实确定性派生 Footprint，不读取 raw。"""
        if interval not in V2_FOOTPRINT_INTERVALS:
            raise HTTPException(status_code=400, detail="Footprint 周期不支持")
        start, end = v2_window(from_ts, to_ts, 86_400)
        try:
            payload, etag = v2_engine().footprint(
                market_id, interval, price_bin, start, end,
            )
        except LookupError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from None
        except (FileNotFoundError, MaterializedQueryError) as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from None
        return v2_result(request, payload, etag)

    @app.get("/api/v2/markets/{market_id}/book/l2/latest")
    def market_l2_latest_v2(
        market_id: str,
        request: Request,
        depth: int = Query(default=30, ge=1, le=400),
    ) -> Response:
        """从最近完整快照和后续 delta 重放出最新已封口 L2 状态。"""
        try:
            payload, etag = v2_engine().latest_l2(market_id, depth)
        except LookupError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from None
        except (FileNotFoundError, MaterializedQueryError) as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from None
        return v2_result(request, payload, etag)

    @app.get("/api/v2/markets/{market_id}/book/l2/quality")
    def market_l2_quality_v2(market_id: str) -> Response:
        """读取最新质量窗；新鲜度只代表物化层。"""
        payload = v2_engine().latest_l2_quality(market_id)
        return JSONResponse(
            content=jsonable(payload),
            headers={"Cache-Control": NO_STORE_CACHE},
        )

    @app.get("/api/v2/aggregates/book/top")
    def aggregate_book_top_v2(
        market_id: list[str] = Query(),
        min_quorum: int = Query(default=2, ge=1, le=8),
        max_age_seconds: int = Query(default=720, ge=1, le=3600),
    ) -> Response:
        """同 quote、同现货身份的 PIT synthetic consolidated top。"""
        if cross_venue_query is None:
            raise HTTPException(status_code=404, detail="无本地数据目录")
        try:
            payload = cross_venue_query.latest_top(
                market_id,
                min_quorum=min_quorum,
                max_age_seconds=max_age_seconds,
            )
        except CrossVenueCompatibilityError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from None
        except LookupError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from None
        except (FileNotFoundError, CrossVenueQueryError) as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from None
        return JSONResponse(
            content=jsonable(payload),
            headers={"Cache-Control": NO_STORE_CACHE},
        )

    @app.get("/api/v2/markets/{market_id}/orderflow/tiles")
    def market_orderflow_tiles_v2(
        market_id: str,
        request: Request,
        bucket: str = "5s",
        from_ts: str | None = Query(default=None, alias="from"),
        to_ts: str | None = Query(default=None, alias="to"),
        limit: int = Query(default=720, ge=1, le=V2_MAX_TILE_COLUMNS),
    ) -> Response:
        """读取稀疏 OFL tile；L2 减量与逐笔成交不做伪归因。"""
        if bucket not in {"1s", "5s", "1min"}:
            raise HTTPException(status_code=400, detail="OFL tile 周期不支持")
        start, end = v2_window(from_ts, to_ts, 3600)
        try:
            payload, etag = v2_engine().orderflow_tiles(
                market_id, bucket, start, end, limit,
            )
        except LookupError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from None
        except (FileNotFoundError, MaterializedQueryError) as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from None
        return v2_result(request, payload, etag)

    @app.get("/api/service-status")
    def service_status() -> dict[str, object]:
        """GMO 服务状态与时钟偏移（R-03、R-05 展示）。"""
        status = public.status()
        drift = public.check_clock(max_drift_seconds=CLOCK_DISPLAY_LIMIT_SECONDS)
        return {"status": status.value, "clock_drift_seconds": round(drift, 3)}

    @app.get("/api/assets")
    def assets() -> dict[str, object]:
        """多来源资产：同币种合计，来源字段分列，缺失绝不伪装为零。"""
        gmo_assets = {
            item.symbol: {"amount": item.amount, "available": item.available}
            for item in read.assets()
        }
        by_venue: dict[str, dict[str, dict[str, Decimal]]] = {"gmo": gmo_assets}
        sources: list[dict[str, object]] = [
            {"id": "gmo", "label": "GMO Coin", "status": "ok"}
        ]
        if bitflyer_read is None:
            sources.append(
                {"id": "bitflyer", "label": "bitFlyer", "status": "unconfigured"}
            )
        else:
            try:
                bitflyer_assets = {
                    item.symbol: {"amount": item.amount, "available": item.available}
                    for item in bitflyer_read.assets()
                }
            except (ApiHttpError, ApiNetworkError, ApiSchemaError) as exc:
                sources.append(
                    {
                        "id": "bitflyer",
                        "label": "bitFlyer",
                        "status": "error",
                        "error": str(exc),
                    }
                )
            else:
                by_venue["bitflyer"] = bitflyer_assets
                sources.append(
                    {"id": "bitflyer", "label": "bitFlyer", "status": "ok"}
                )
        symbols = sorted({symbol for rows in by_venue.values() for symbol in rows})
        items: list[dict[str, object]] = []
        for symbol in symbols:
            venue_values: dict[str, dict[str, str]] = {}
            amount = Decimal("0")
            available = Decimal("0")
            for venue, rows in by_venue.items():
                value = rows.get(symbol)
                if value is None:
                    continue
                amount += value["amount"]
                available += value["available"]
                venue_values[venue] = {
                    "amount": format(value["amount"], "f"),
                    "available": format(value["available"], "f"),
                }
            items.append(
                {
                    "symbol": symbol,
                    "amount": format(amount, "f"),
                    "available": format(available, "f"),
                    "venues": venue_values,
                }
            )
        return {"items": items, "sources": sources, "as_of": _now_iso()}

    @app.get("/api/symbols")
    def symbols() -> dict[str, object]:
        """全部现物品种的取引ルール，供品种选择器。

        查看面只读行情，品种范围不受交易白名单约束；
        白名单单列返回，仅表达可交易性（T-09 仍由执行侧守护）。
        """
        whitelist = sorted(str(symbol) for symbol in config.spot_whitelist)
        rules = sorted(
            (
                {
                    "symbol": rule.symbol,
                    "min_order_size": format(rule.min_order_size, "f"),
                    "size_step": format(rule.size_step, "f"),
                    "tick_size": format(rule.tick_size, "f"),
                }
                for rule in public.symbols()
                if not rule.symbol.endswith("_JPY")
            ),
            key=lambda rule: rule["symbol"],
        )
        return {"whitelist": whitelist, "rules": rules}

    @app.get("/api/ticker")
    def ticker(symbol: str = Query(min_length=2)) -> dict[str, object]:
        """最新レート单品种。"""
        rows = public.ticker(symbol)
        if not rows:
            raise HTTPException(status_code=404, detail="品种无行情")
        row = rows[0]
        return {
            "symbol": row.symbol,
            "last": format(row.last, "f"),
            "ask": format(row.ask, "f"),
            "bid": format(row.bid, "f"),
            "high": format(row.high, "f"),
            "low": format(row.low, "f"),
            "volume": format(row.volume, "f"),
            "timestamp": row.timestamp.isoformat(),
        }

    @app.get("/api/klines")
    def klines(
        symbol: str = Query(min_length=2),
        interval: str = "1hour",
        from_day: str | None = Query(default=None, alias="from"),
        to_day: str | None = Query(default=None, alias="to"),
    ) -> dict[str, object]:
        """区间 K 线，多次上游拉取聚合（R-04 限次）。

        from 与 to 为 JST 交易日 YYYYMMDD；缺省为当日。
        缺数据的日期或年份（KLINE_NOT_FOUND）静默跳过。
        """
        today = jst_kline_date(datetime.now(UTC))
        parsed = KlineInterval(interval)
        end = to_day or today
        start = from_day or end
        for value in (start, end):
            datetime.strptime(value, "%Y%m%d")

        def fetch(date: str) -> list[Kline]:
            try:
                return list(public.klines(symbol, parsed, date))
            except GmoApiError as exc:
                if KLINE_NOT_FOUND in exc.codes:
                    return []
                raise

        merged: dict[str, dict[str, str]] = {}
        source = "live"
        requests_made = 0
        truncated = False
        store_conn = (
            connect_readonly(data_root) if data_root is not None else None
        )
        if store_conn is not None:
            db_rows = query_klines(store_conn, symbol, parsed.value, start, end)
            store_conn.close()
            if db_rows:
                source = "store"
                for open_time, o, h, low, c, v in db_rows:
                    merged[open_time] = {
                        "open_time": open_time,
                        "open": o,
                        "high": h,
                        "low": low,
                        "close": c,
                        "volume": v,
                    }
        if source == "store":
            # 当期尾部由上游刷新一刀
            current = today[:4] if parsed in YEARLY_INTERVALS else today
            if end >= current[: len(end)]:
                for row in fetch(current):
                    merged[row.open_time.isoformat()] = {
                        "open_time": row.open_time.isoformat(),
                        "open": format(row.open, "f"),
                        "high": format(row.high, "f"),
                        "low": format(row.low, "f"),
                        "close": format(row.close, "f"),
                        "volume": format(row.volume, "f"),
                    }
                requests_made = 1
                source = "store+live"
        else:
            dates, truncated = kline_dates(parsed, start, end)
            for date in dates:
                for row in fetch(date):
                    merged[row.open_time.isoformat()] = {
                        "open_time": row.open_time.isoformat(),
                        "open": format(row.open, "f"),
                        "high": format(row.high, "f"),
                        "low": format(row.low, "f"),
                        "close": format(row.close, "f"),
                        "volume": format(row.volume, "f"),
                    }
            requests_made = len(dates)
        items = [merged[key] for key in sorted(merged)]
        return {
            "items": items,
            "meta": {
                "interval": parsed.value,
                "from": start,
                "to": end,
                "today": today,
                "requests": requests_made,
                "truncated": truncated,
                "source": source,
            },
        }

    @app.get("/api/orderbooks")
    def orderbooks(
        symbol: str = Query(min_length=2), depth: int = Query(default=15, le=50)
    ) -> dict[str, object]:
        """板情報梯形视图：截取档深并给出价差与双侧合计。"""
        now = time.monotonic()
        with orderbook_cache_lock:
            cached = orderbook_cache.get(symbol)
            if cached is not None and now - cached[0] < ORDERBOOK_CACHE_SECONDS:
                _, book, observed_at = cached
            else:
                book = public.orderbooks(symbol)
                observed_at = _now_iso()
                orderbook_cache[symbol] = (time.monotonic(), book, observed_at)
        asks = tuple(sorted(book.asks, key=lambda level: level.price))[:depth]
        bids = tuple(
            sorted(book.bids, key=lambda level: level.price, reverse=True)
        )[:depth]
        if not asks or not bids:
            raise HTTPException(status_code=404, detail="盘口为空")
        metrics = snapshot_metrics(asks, bids)
        ask_total = sum(level.size for level in asks)
        bid_total = sum(level.size for level in bids)
        return {
            "symbol": book.symbol,
            "source": "REST",
            "asks": [
                {
                    "price": format(l.price, "f"),
                    "size": format(l.size, "f"),
                    "notional": format(l.price * l.size, "f"),
                }
                for l in asks
            ],
            "bids": [
                {
                    "price": format(l.price, "f"),
                    "size": format(l.size, "f"),
                    "notional": format(l.price * l.size, "f"),
                }
                for l in bids
            ],
            "best_ask": format(metrics.best_ask, "f"),
            "best_bid": format(metrics.best_bid, "f"),
            "spread": format(metrics.spread, "f"),
            "mid": format(metrics.mid, "f"),
            "microprice": format(metrics.microprice, "f"),
            "coverage": {
                "ask_bp": format(metrics.ask_coverage_bp, "f"),
                "bid_bp": format(metrics.bid_coverage_bp, "f"),
            },
            "bands": [
                {
                    "band_bp": format(band.band_bp, "f"),
                    "ask_size": format(band.ask_size, "f"),
                    "bid_size": format(band.bid_size, "f"),
                    "ask_notional": format(band.ask_notional, "f"),
                    "bid_notional": format(band.bid_notional, "f"),
                    "imbalance_size": format(band.imbalance_size, "f"),
                    "imbalance_notional": format(band.imbalance_notional, "f"),
                }
                for band in metrics.bands
            ],
            "ask_total": format(ask_total, "f"),
            "bid_total": format(bid_total, "f"),
            "as_of": observed_at,
        }

    @app.get("/api/book-heatmap")
    def book_heatmap(
        symbol: str = Query(min_length=2),
        minutes: float = Query(default=15.0, gt=0, le=180),
    ) -> dict[str, object]:
        """快照序列热力矩阵，空档列显式标记（不插值）。"""
        if data_root is None:
            return {"rows": 0, "cols": [], "ask": [], "bid": [], "mid_row": []}
        now = datetime.now(UTC)
        frames = load_recent_book_frames(data_root, symbol, minutes, now)
        return build_heatmap(frames, minutes, now)

    @app.get("/api/footprint")
    def footprint(
        symbol: str = Query(min_length=2, max_length=16),
        interval: str = "15min",
        from_day: str | None = Query(default=None, alias="from"),
        to_day: str | None = Query(default=None, alias="to"),
        bin_arg: str = Query(default="auto", alias="bin"),
        tick: str = Query(default="1"),
    ) -> dict[str, object]:
        """足迹聚合：逐 bar 档阵列与汇总，数值全为字符串。

        纯本地读取归档与录制流，零上游调用；
        当期 bar 标 live，昨日及以前标 archive；
        tick 为品种规则 tickSize 原文，由前端转传。
        """
        if not symbol.isalnum():
            raise HTTPException(status_code=400, detail="品种非法")
        if interval not in FOOTPRINT_INTERVALS:
            raise HTTPException(status_code=400, detail="周期不支持")
        if bin_arg != "auto" and bin_arg not in {
            str(tier) for tier in BIN_TIERS
        }:
            raise HTTPException(status_code=400, detail="档位非法")
        try:
            tick_value = Decimal(tick)
        except InvalidOperation:
            raise HTTPException(status_code=400, detail="tick 非法") from None
        if tick_value <= 0:
            raise HTTPException(status_code=400, detail="tick 非法")
        today = jst_kline_date(datetime.now(UTC))
        end = to_day or today
        start = from_day or end
        for value in (start, end):
            try:
                datetime.strptime(value, "%Y%m%d")
            except ValueError:
                raise HTTPException(
                    status_code=400, detail="日期非法"
                ) from None
        if data_root is None:
            return {
                "bars": [],
                "meta": {
                    "symbol": symbol,
                    "interval": interval,
                    "from": start,
                    "to": end,
                    "today": today,
                    "bin": None,
                    "tier": None,
                    "auto": bin_arg == "auto",
                    "truncated": False,
                    "coverage_clipped": False,
                    "side_basis": (
                        "by_bar_source:archive_taker|live_tick_rule_inference"
                    ),
                    "unknown_side_count": 0,
                    "coverage_from": None,
                    "coverage_to": None,
                },
            }
        built = build_footprint(
            data_root, symbol, interval, start, end, bin_arg, tick, today
        )
        return {"bars": jsonable(built["bars"]), "meta": built["meta"]}

    def _tiles_window(
        symbol: str, bucket: str, from_ts: str, to_ts: str
    ) -> tuple[Path, int, int]:
        """瓦片端点公共校验，返回数据根与窗口秒。"""
        if not symbol.isalnum():
            raise HTTPException(status_code=400, detail="品种非法")
        if bucket not in TILE_BUCKETS:
            raise HTTPException(status_code=400, detail="桶档不支持")
        if data_root is None:
            raise HTTPException(status_code=404, detail="无本地数据目录")
        from_s, to_s = window_epochs(from_ts, to_ts)
        return data_root, from_s, to_s

    @app.get("/api/heatmap-tiles")
    def heatmap_tiles(
        response: Response,
        symbol: str = Query(min_length=2, max_length=16),
        bucket: str = "1s",
        from_ts: str = Query(),
        to_ts: str = Query(),
    ) -> dict[str, object]:
        """瓦片窗口切片：列区间裁剪，数值全为字符串。

        纯本地读预聚合网格文件，零上游调用；
        空档列与延载列如实标记（6.5 节）；
        窗口全落完结日时响应带不可变缓存头。
        """
        root, from_s, to_s = _tiles_window(symbol, bucket, from_ts, to_ts)
        sliced = slice_columns(root, VENUE_GMO, symbol, bucket, from_s, to_s)
        today_start = int(
            datetime.now(UTC)
            .replace(hour=0, minute=0, second=0, microsecond=0)
            .timestamp()
        )
        frozen = to_s <= today_start
        if frozen:
            for date_text in window_dates(from_s, to_s):
                meta = load_tile_meta(
                    root, VENUE_GMO, symbol, bucket, date_text
                )
                if meta is None or meta.get("complete") is not True:
                    frozen = False
                    break
        response.headers["Cache-Control"] = (
            IMMUTABLE_CACHE if frozen else NO_STORE_CACHE
        )
        return sliced

    @app.get("/api/level-track")
    def level_track_endpoint(
        symbol: str = Query(min_length=2, max_length=16),
        price_bin: str = Query(min_length=1),
        from_ts: str = Query(),
        to_ts: str = Query(),
        bucket: str = "1s",
    ) -> dict[str, object]:
        """档带流动性追踪（6.4 节点击追踪的数据面）。"""
        root, from_s, to_s = _tiles_window(symbol, bucket, from_ts, to_ts)
        try:
            Decimal(price_bin)
        except InvalidOperation:
            raise HTTPException(status_code=400, detail="价格档非法") from None
        sliced = slice_columns(
            root, VENUE_GMO, symbol, bucket, from_s, to_s,
            max_columns=MAX_TRACK_COLUMNS,
        )
        columns = sliced["columns"]
        assert isinstance(columns, list)
        tracked = level_track(columns, price_bin)
        meta = sliced["meta"]
        assert isinstance(meta, dict)
        meta.update(
            window_coverage(columns, from_s, to_s, TILE_BUCKETS.get(bucket))
        )
        return {"track": tracked, "meta": meta}

    @app.post("/api/region-analysis")
    def region_analysis(
        symbol: str = Query(min_length=2, max_length=16),
        price_lo: str = Query(min_length=1),
        price_hi: str = Query(min_length=1),
        from_ts: str = Query(),
        to_ts: str = Query(),
        bucket: str = "1s",
    ) -> dict[str, object]:
        """框选区域四判定并列输出（6.4 节），判定非事实。

        判定成立者追加 book_feature 派生事件表并做报警
        规则流上匹配（6.8 节）；阈值具名配置，散列随行落库。
        本端点落库，故用 POST 表达；同请求同配置重放幂等，
        复用既有 feature 行与报警行，不重复落库（schema v4）。
        """
        root, from_s, to_s = _tiles_window(symbol, bucket, from_ts, to_ts)
        try:
            low = Decimal(price_lo)
            high = Decimal(price_hi)
        except InvalidOperation:
            raise HTTPException(status_code=400, detail="价格带非法") from None
        if low > high:
            raise HTTPException(status_code=400, detail="价格带倒置")
        dates = window_dates(from_s, to_s)
        if len(dates) > REGION_MAX_WINDOW_DAYS:
            raise HTTPException(status_code=400, detail="窗口过长")
        sliced = slice_columns(
            root, VENUE_GMO, symbol, bucket, from_s, to_s,
            max_columns=MAX_TRACK_COLUMNS,
        )
        meta = sliced["meta"]
        assert isinstance(meta, dict)
        if meta.get("truncated") is True:
            raise HTTPException(
                status_code=400,
                detail="区域窗口超过分析列上限，请缩短时段或改用粗桶",
            )
        row_bin = meta.get("row_bin")
        if not isinstance(row_bin, str):
            raise HTTPException(status_code=404, detail="窗口无瓦片数据")
        columns = sliced["columns"]
        assert isinstance(columns, list)
        coverage = window_coverage(
            columns, from_s, to_s, TILE_BUCKETS.get(bucket)
        )
        rules = load_alert_rules(rules_dir)
        mid_median = window_mid_median(columns)
        request_key = (format(low, "f"), format(high, "f"))
        # 规则带几何评估计划：缺省沿用请求带并记录
        plans: list[tuple[AlertRule, tuple[str, str] | None, str]] = []
        for rule in rules:
            if not rule.enabled or rule.symbol != symbol:
                continue
            geometry = rule_band(rule, low, high, mid_median)
            if geometry is None:
                plans.append((rule, None, BAND_SOURCE_RULE_BP))
                continue
            band_low, band_high, band_source = geometry
            plans.append(
                (
                    rule,
                    (format(band_low, "f"), format(band_high, "f")),
                    band_source,
                )
            )
        bands: dict[tuple[str, str], tuple[Decimal, Decimal]] = {
            request_key: (low, high)
        }
        for _plan_rule, pair, _plan_source in plans:
            if pair is not None:
                bands.setdefault(pair, (Decimal(pair[0]), Decimal(pair[1])))
        # 全部评估带同趟扫描基线，逐带入样
        contexts: dict[tuple[str, str], list[BandSample]] = {
            key: [] for key in bands
        }
        for date_text in dates:
            for column in iter_tile_columns(
                root, VENUE_GMO, symbol, bucket, date_text
            ):
                for key, (band_low, band_high) in bands.items():
                    sample = band_sample(column, band_low, band_high)
                    if sample is not None:
                        contexts[key].append(sample)
        judgments_by_band = {
            key: analyze_region(
                columns, contexts[key], band_low, band_high, Decimal(row_bin)
            )
            for key, (band_low, band_high) in bands.items()
        }
        judgments = judgments_by_band[request_key]
        context = contexts[request_key]
        config_hash = region_config_hash()
        created_at = _now_iso()
        from_iso = datetime.fromtimestamp(from_s, UTC).isoformat()
        to_iso = datetime.fromtimestamp(to_s, UTC).isoformat()
        baseline_rows = [
            [
                format(sample.executed, "f"),
                format(sample.net_cancel, "f"),
                format(sample.depth, "f"),
            ]
            for sample in context
        ]
        baseline = {
            "dates": dates,
            "samples": len(context),
            "price_low": format(low, "f"),
            "price_high": format(high, "f"),
            "excludes": ["gap", "carried"],
            "basis": REGION_BASIS,
            "window_columns": len(columns),
            **coverage,
        }
        baseline_hash = sha256_hex(
            json.dumps(
                baseline_rows, separators=(",", ":"), ensure_ascii=False
            ).encode("utf-8")
        )
        tile_revisions: list[dict[str, object]] = []
        for date_text in dates:
            tile_meta = load_tile_meta(
                root, VENUE_GMO, symbol, bucket, date_text
            )
            if tile_meta is None:
                tile_revisions.append({"date": date_text, "missing": True})
                continue
            tile_revisions.append(
                {
                    "date": date_text,
                    "built_at": tile_meta.get("built_at"),
                    "config_hash": tile_meta.get("config_hash"),
                    "source": tile_meta.get("source"),
                    "complete": tile_meta.get("complete"),
                    "columns": tile_meta.get("columns"),
                }
            )
        source_hash = sha256_hex(
            json.dumps(
                tile_revisions,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            ).encode("utf-8")
        )
        analysis_identity = {
            "venue": VENUE_GMO,
            "symbol": symbol,
            "price_low": format(low, "f"),
            "price_high": format(high, "f"),
            "from_ts": from_iso,
            "to_ts": to_iso,
            "bucket": bucket,
            "config_hash": config_hash,
            "baseline_hash": baseline_hash,
            "source_hash": source_hash,
            "code_version": SIGNAL_CODE_VERSION,
            "confidence_version": CONFIDENCE_VERSION,
        }
        analysis_request_hash = sha256_hex(
            json.dumps(
                analysis_identity, sort_keys=True, ensure_ascii=False
            ).encode("utf-8")
        )
        conn = connect(root)
        try:
            run_id = insert_analysis_run(
                conn,
                (
                    new_run_id(),
                    analysis_request_hash,
                    VENUE_GMO,
                    symbol,
                    format(low, "f"),
                    format(high, "f"),
                    from_iso,
                    to_iso,
                    bucket,
                    json.dumps(judgments, ensure_ascii=False),
                    json.dumps(baseline, ensure_ascii=False),
                    baseline_hash,
                    source_hash,
                    config_hash,
                    SIGNAL_CODE_VERSION,
                    CONFIDENCE_VERSION,
                    created_at,
                    "complete" if len(context) >= 30 else "insufficient_baseline",
                    None,
                    REGION_BASIS,
                    len(columns),
                ),
            )
            for judgment in judgments:
                if judgment.get("met") is not True:
                    continue
                kind = str(judgment["kind"])
                metrics_obj = judgment["metrics"]
                metrics = (
                    dict(metrics_obj) if isinstance(metrics_obj, dict) else {}
                )
                metrics["confidence"] = judgment["confidence"]
                metrics["criteria"] = judgment.get("criteria", [])
                metrics["confidence_version"] = CONFIDENCE_VERSION
                metrics["band_source"] = BAND_SOURCE_REQUEST
                request_hash = sha256_hex(
                    json.dumps(
                        {
                            "kind": kind,
                            "analysis_request_hash": analysis_request_hash,
                        },
                        sort_keys=True,
                        ensure_ascii=False,
                    ).encode("utf-8")
                )
                feature_id = insert_book_feature(
                    conn,
                    (
                        kind, VENUE_GMO, symbol, format(low, "f"),
                        format(high, "f"), from_iso, to_iso,
                        json.dumps(metrics, ensure_ascii=False),
                        config_hash, created_at, request_hash,
                    ),
                    run_id=run_id,
                    code_version=SIGNAL_CODE_VERSION,
                    confidence_version=CONFIDENCE_VERSION,
                )
                judgment["feature_id"] = feature_id
            # 匹配器按规则带几何评估
            rule_evaluations: list[dict[str, object]] = []
            for rule, pair, band_source in plans:
                evaluation: dict[str, object] = {
                    "rule_id": rule.rule_id,
                    "kind": rule.kind,
                    "band_source": band_source,
                }
                if rule.band_bp is not None:
                    evaluation["band_bp"] = rule.band_bp
                if pair is None:
                    # 标准带无窗内中价，不可评估
                    evaluation["evaluable"] = False
                    rule_evaluations.append(evaluation)
                    continue
                evaluation["evaluable"] = True
                evaluation["price_low"] = pair[0]
                evaluation["price_high"] = pair[1]
                judged = next(
                    row
                    for row in judgments_by_band[pair]
                    if row["kind"] == rule.kind
                )
                met = judged.get("met") is True
                confidence = Decimal(str(judged["confidence"]))
                evaluation["met"] = met
                evaluation["confidence"] = str(judged["confidence"])
                matched = met and bool(
                    match_rules([rule], rule.kind, symbol, confidence)
                )
                evaluation["matched"] = matched
                if not matched:
                    rule_evaluations.append(evaluation)
                    continue
                if band_source == BAND_SOURCE_REQUEST:
                    held_id = judged.get("feature_id")
                    assert isinstance(held_id, int)
                    feature_id = held_id
                else:
                    # 规则带成立事件独立落事件流
                    metrics_obj = judged["metrics"]
                    metrics = (
                        dict(metrics_obj)
                        if isinstance(metrics_obj, dict)
                        else {}
                    )
                    metrics["confidence"] = judged["confidence"]
                    metrics["criteria"] = judged.get("criteria", [])
                    metrics["confidence_version"] = CONFIDENCE_VERSION
                    metrics["band_source"] = band_source
                    if rule.band_bp is not None:
                        metrics["band_bp"] = rule.band_bp
                    request_hash = sha256_hex(
                        json.dumps(
                            {
                                "kind": rule.kind,
                                "analysis_request_hash": analysis_request_hash,
                                "price_low": pair[0],
                                "price_high": pair[1],
                            },
                            sort_keys=True,
                            ensure_ascii=False,
                        ).encode("utf-8")
                    )
                    feature_id = insert_book_feature(
                        conn,
                        (
                            rule.kind, VENUE_GMO, symbol, pair[0], pair[1],
                            from_iso, to_iso,
                            json.dumps(metrics, ensure_ascii=False),
                            config_hash, created_at, request_hash,
                        ),
                        run_id=run_id,
                        code_version=SIGNAL_CODE_VERSION,
                        confidence_version=CONFIDENCE_VERSION,
                    )
                    evaluation["feature_id"] = feature_id
                alert_id = insert_alert_event(
                    conn, feature_id, rule.rule_id, created_at
                )
                evaluation["alert_id"] = alert_id
                if band_source == BAND_SOURCE_REQUEST:
                    judged["alert_id"] = alert_id
                rule_evaluations.append(evaluation)
        finally:
            conn.close()
        for judgment in judgments:
            metrics_obj = judgment.get("metrics")
            if isinstance(metrics_obj, dict):
                judgment["metric_labels"] = {
                    key: METRIC_LABELS.get(key, key) for key in metrics_obj
                }
        return {
            "judgments": judgments,
            "meta": {
                "symbol": symbol,
                "bucket": bucket,
                "price_lo": format(low, "f"),
                "price_hi": format(high, "f"),
                "from_ts": from_iso,
                "to_ts": to_iso,
                "config_hash": config_hash,
                "run_id": run_id,
                "baseline_hash": baseline_hash,
                "source_hash": source_hash,
                "code_version": SIGNAL_CODE_VERSION,
                "confidence_version": CONFIDENCE_VERSION,
                "basis": REGION_BASIS,
                "context_samples": len(context),
                "columns": len(columns),
                "rule_evaluations": rule_evaluations,
                **coverage,
            },
        }

    @app.get("/api/analysis-runs")
    def analysis_runs(
        symbol: str | None = Query(default=None, max_length=16),
        limit: int = Query(
            default=ANALYSIS_RUN_LIST_LIMIT,
            ge=1,
            le=ANALYSIS_RUN_LIST_LIMIT,
        ),
    ) -> dict[str, object]:
        """分析台账时序清单（6.4 节全量登记的检索面）。

        只读 analysis_run 表；台账答「分析过什么、
        判了什么」，成立事件流另见 book_feature。
        """
        if symbol is not None and not symbol.isalnum():
            raise HTTPException(status_code=400, detail="品种非法")
        if data_root is None:
            return {"items": [], "as_of": _now_iso()}
        conn = connect(data_root)
        try:
            rows = list_analysis_runs(conn, symbol, limit)
        finally:
            conn.close()
        items = []
        for row in rows:
            judgments_loaded = json.loads(row[9])
            met_kinds = [
                str(item.get("kind"))
                for item in judgments_loaded
                if isinstance(item, dict) and item.get("met") is True
            ]
            items.append(
                {
                    "run_id": row[0],
                    "symbol": row[1],
                    "price_low": row[2],
                    "price_high": row[3],
                    "from_ts": row[4],
                    "to_ts": row[5],
                    "bucket": row[6],
                    "basis": row[7],
                    "window_columns": row[8],
                    "judgments": judgments_loaded,
                    "met_kinds": met_kinds,
                    "baseline_hash": row[10],
                    "config_hash": row[11],
                    "code_version": row[12],
                    "confidence_version": row[13],
                    "created_at": row[14],
                    "status": row[15],
                }
            )
        return {"items": items, "as_of": _now_iso()}

    @app.get("/api/recent-trades")
    def recent_trades(
        symbol: str = Query(min_length=2, max_length=16),
        seconds: int = Query(default=RECENT_TRADES_DEFAULT_SECONDS, ge=1),
    ) -> dict[str, object]:
        """市场撮合近窗逐笔：双侧去重合一加侧别推断。

        读当日 raw 尾部（口径快照第 2 节），秒窗上限
        入配置；侧别为 tick 规则推断，供成交强度计。
        """
        if not symbol.isalnum():
            raise HTTPException(status_code=400, detail="品种非法")
        window = min(seconds, config.recent_trades_max_seconds)
        now = datetime.now(UTC)
        if data_root is None:
            return {
                "items": [],
                "meta": {
                    "symbol": symbol,
                    "seconds": window,
                    "side_basis": SIDE_BASIS,
                    "as_of": now.isoformat(),
                },
            }
        rows, seed = load_recent_trade_rows(data_root, symbol, window, now)
        prints = infer_sides(dedupe_ws_rows(rows), seed)
        items = [
            {
                "e": epoch_ms,
                "t": datetime.fromtimestamp(
                    epoch_ms / 1000, UTC
                ).isoformat(),
                "price": format(price, "f"),
                "size": format(size, "f"),
                "side": side,
            }
            for epoch_ms, price, size, side in prints
        ]
        return {
            "items": items,
            "meta": {
                "symbol": symbol,
                "seconds": window,
                "side_basis": SIDE_BASIS,
                "as_of": now.isoformat(),
            },
        }

    @app.get("/api/print-ticks")
    def print_ticks(
        symbol: str = Query(min_length=2, max_length=16),
        from_ts: str = Query(),
        to_ts: str = Query(),
    ) -> dict[str, object]:
        """成交刻线清单：量达分位阈的大额成交（6.7 节）。"""
        if not symbol.isalnum():
            raise HTTPException(status_code=400, detail="品种非法")
        if data_root is None:
            raise HTTPException(status_code=404, detail="无本地数据目录")
        from_s, to_s = window_epochs(from_ts, to_ts)
        return load_print_ticks(data_root, VENUE_GMO, symbol, from_s, to_s)

    @app.get("/api/book-features")
    def book_features(
        symbol: str = Query(min_length=2, max_length=16),
        limit: int = Query(default=FEATURE_LIST_LIMIT, ge=1, le=FEATURE_LIST_LIMIT),
    ) -> dict[str, object]:
        """判读事件时序清单（6.6 节事件列表数据面）。

        只读 book_feature 派生表，按事件起始时刻倒序。
        """
        if not symbol.isalnum():
            raise HTTPException(status_code=400, detail="品种非法")
        if data_root is None:
            return {"items": [], "as_of": _now_iso()}
        conn = connect(data_root)
        try:
            rows = conn.execute(
                "SELECT feature_id, kind, symbol, price_low, price_high, "
                "from_ts, to_ts, metrics, created_at FROM book_feature "
                "WHERE symbol=? ORDER BY from_ts DESC, feature_id DESC "
                "LIMIT ?",
                (symbol, limit),
            ).fetchall()
        finally:
            conn.close()
        items = []
        for row in rows:
            metrics = json.loads(row[7])
            items.append(
                {
                    "feature_id": row[0],
                    "kind": row[1],
                    "label": JUDGMENT_LABELS.get(row[1], row[1]),
                    "symbol": row[2],
                    "price_low": row[3],
                    "price_high": row[4],
                    "from_ts": row[5],
                    "to_ts": row[6],
                    "metrics": metrics,
                    "created_at": row[8],
                    "metric_labels": {
                        key: METRIC_LABELS.get(key, key) for key in metrics
                    },
                }
            )
        return {"items": items, "as_of": _now_iso()}

    @app.get("/api/alerts")
    def alerts() -> dict[str, object]:
        """报警清单：未确认优先（6.8 节呈现数据面）。"""
        if data_root is None:
            return {"items": [], "unacked": 0, "as_of": _now_iso()}
        conn = connect(data_root)
        try:
            rows = list_alert_events(conn, ALERT_LIST_LIMIT)
        finally:
            conn.close()
        items = []
        for row in rows:
            metrics = json.loads(row[11])
            items.append(
                {
                    "alert_id": row[0],
                    "feature_id": row[1],
                    "rule_id": row[2],
                    "triggered_at": row[3],
                    "acked_at": row[4],
                    "kind": row[5],
                    "label": JUDGMENT_LABELS.get(row[5], row[5]),
                    "symbol": row[6],
                    "price_low": row[7],
                    "price_high": row[8],
                    "from_ts": row[9],
                    "to_ts": row[10],
                    "metrics": metrics,
                    "metric_labels": {
                        key: METRIC_LABELS.get(key, key) for key in metrics
                    },
                }
            )
        unacked = sum(1 for item in items if item["acked_at"] is None)
        return {"items": items, "unacked": unacked, "as_of": _now_iso()}

    @app.post("/api/alerts/{alert_id}/ack")
    def alert_ack(alert_id: int) -> dict[str, object]:
        """确认报警：唯一写动作，仅回填确认时刻。

        无任何交易语义，只改呈现状态（6.8 节状态机）。
        """
        if data_root is None:
            raise HTTPException(status_code=404, detail="无本地数据目录")
        conn = connect(data_root)
        try:
            row = ack_alert_event(conn, alert_id, _now_iso())
        finally:
            conn.close()
        if row is None:
            raise HTTPException(status_code=404, detail="报警不存在")
        return {
            "item": {
                "alert_id": row[0],
                "feature_id": row[1],
                "rule_id": row[2],
                "triggered_at": row[3],
                "acked_at": row[4],
            },
            "as_of": _now_iso(),
        }

    @app.get("/api/active-orders")
    def active_orders(symbol: str = Query(min_length=2)) -> dict[str, object]:
        """挂单一览（U-01 委托）。"""
        items = [
            {
                "order_id": order.order_id,
                "side": order.side.value,
                "execution_type": order.execution_type.value,
                "price": None if order.price is None else format(order.price, "f"),
                "size": format(order.size, "f"),
                "executed_size": format(order.executed_size, "f"),
                "status": order.status.value,
                "time_in_force": order.time_in_force.value,
                "timestamp": order.timestamp.isoformat(),
            }
            for order in read.active_orders(symbol)
        ]
        return {"items": items, "as_of": _now_iso()}

    @app.get("/api/latest-executions")
    def latest_executions(symbol: str = Query(min_length=2)) -> dict[str, object]:
        """最新成交一览（U-01 成交）。"""
        items = [
            {
                "execution_id": item.execution_id,
                "order_id": item.order_id,
                "side": item.side.value,
                "price": format(item.price, "f"),
                "size": format(item.size, "f"),
                "fee": format(item.fee, "f"),
                "timestamp": item.timestamp.isoformat(),
            }
            for item in read.latest_executions(symbol)
        ]
        return {"items": items, "as_of": _now_iso()}

    @app.get("/api/ops/processes")
    def ops_processes() -> dict[str, object]:
        """采集进程登记表全量状态（TBD-31）。"""
        return {"items": manager.snapshot(), "as_of": _now_iso()}

    @app.post("/api/ops/processes/{name}/start")
    def ops_process_start(name: str) -> dict[str, object]:
        """拉起白名单采集进程，已运行返回现状（幂等）。"""
        if not manager.has(name):
            raise HTTPException(status_code=404, detail="进程未登记")
        return {"item": manager.start(name), "as_of": _now_iso()}

    @app.post("/api/ops/processes/{name}/stop")
    def ops_process_stop(name: str) -> dict[str, object]:
        """停止白名单采集进程，无任何交易语义。"""
        if not manager.has(name):
            raise HTTPException(status_code=404, detail="进程未登记")
        return {"item": manager.stop(name), "as_of": _now_iso()}

    return app


def start_tile_refresh_thread(
    config: Config, data_root: Path
) -> threading.Thread:
    """当日瓦片增量刷新线程，间隔入配置。

    单一 api 进程即当日唯一写者；IO 与解析类失败
    记录后继续，未知异常任线程终止以暴露缺陷。
    """
    registry = IncrementalTileRegistry(
        data_root, sorted(str(symbol) for symbol in config.spot_whitelist)
    )

    def loop() -> None:
        while True:
            try:
                registry.refresh_all(datetime.now(UTC))
            except (OSError, ValueError, json.JSONDecodeError) as exc:
                logging.getLogger(__name__).warning(
                    "瓦片增量刷新失败: %s", exc
                )
            time.sleep(config.tile_refresh_seconds)

    thread = threading.Thread(target=loop, name="tile-refresh", daemon=True)
    thread.start()
    return thread


def main() -> None:
    """装载配置并启动查询服务，仅绑定本机（TBD-14）。

    进程管理器随服务启动，仅开判活线程，不拉起任何进程；
    当日瓦片增量刷新线程随服务常驻。
    """
    config = load_config()
    token = ensure_token(config.log_dir)
    manager = ProcessManager(
        DEFAULT_REGISTRY, data_root=Path("data"), log_dir=config.log_dir
    )
    manager.start_poll_thread()
    start_tile_refresh_thread(config, Path("data"))
    app = create_app(
        config,
        PublicClient.from_config(config),
        ReadClient.from_config(config),
        token,
        bitflyer_read=BitflyerReadClient.from_config(config),
        data_root=Path("data"),
        process_manager=manager,
    )
    uvicorn.run(app, host=BIND_HOST, port=PORT, log_level="warning")


if __name__ == "__main__":
    main()
