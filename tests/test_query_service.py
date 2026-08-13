"""查询服务单测：鉴权、序列化、能力清单、操作端点。全程离线（C-13）。"""
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from guvolu.api.public_client import PublicClient
from guvolu.api.bitflyer_read_client import BitflyerAsset
from guvolu.api.read_client import ReadClient
from guvolu.api.transport import (
    HttpMethod,
    Params,
    PrivateTransport,
    PublicTransport,
    RateLimiter,
)
from guvolu.domain.config import load_config
from guvolu.domain.enums import RunMode
from guvolu.domain.enums import KlineInterval
from guvolu.data.store import connect, upsert_klines
from guvolu.data.materialize import ensure_markets
from guvolu.ops.process_manager import ProcessManager, ProcessSpec
from guvolu.ui.query_service import (
    create_app,
    ensure_token,
    jst_kline_date,
    kline_dates,
)
from guvolu.venues import registry

_TOKEN = "test-token"

_ASSETS = [
    {"amount": "3009", "available": "3009", "conversionRate": "1", "symbol": "JPY"}
]
_TICKER = [
    {
        "ask": "10140050",
        "bid": "10138320",
        "high": "10174419",
        "last": "10141182",
        "low": "10068720",
        "symbol": "BTC",
        "timestamp": "2026-08-05T12:24:46.560Z",
        "volume": "352.72",
    }
]
_SYMBOLS = [
    {
        "symbol": "XRP",
        "minOrderSize": "1",
        "maxOrderSize": "100000",
        "sizeStep": "1",
        "tickSize": "0.001",
        "takerFee": "0.0005",
        "makerFee": "-0.0001",
    },
    {
        "symbol": "BTC",
        "minOrderSize": "0.00001",
        "maxOrderSize": "5",
        "sizeStep": "0.00001",
        "tickSize": "1",
        "takerFee": "0.0005",
        "makerFee": "-0.0001",
    },
    {
        "symbol": "BTC_JPY",
        "minOrderSize": "0.01",
        "maxOrderSize": "5",
        "sizeStep": "0.01",
        "tickSize": "1",
        "takerFee": "0",
        "makerFee": "0",
    },
]


class _FakePublicTransport(PublicTransport):
    """公开传输替身，按路径返回预置数据。"""

    def __init__(self) -> None:
        super().__init__(RateLimiter(1000.0))
        self.orderbook_calls = 0

    def get_payload(
        self, path: str, params: Params | None = None
    ) -> Mapping[str, object]:
        del params
        if path == "/v1/status":
            return {
                "status": 0,
                "data": {"status": "OPEN"},
                "responsetime": "2100-01-01T00:00:00.000Z",
            }
        raise AssertionError(f"未预置载荷 {path}")

    def get(self, path: str, params: Params | None = None) -> object:
        del params
        if path == "/v1/status":
            return {"status": "OPEN"}
        if path == "/v1/ticker":
            return _TICKER
        if path == "/v1/symbols":
            return _SYMBOLS
        if path == "/v1/klines":
            return []
        if path == "/v1/orderbooks":
            self.orderbook_calls += 1
            return {
                "symbol": "BTC",
                "asks": [{"price": "101", "size": "0.2"}, {"price": "102", "size": "0.3"}],
                "bids": [{"price": "99", "size": "0.1"}, {"price": "98", "size": "0.4"}],
            }
        raise AssertionError(f"未预置数据 {path}")


class _FakePrivateTransport(PrivateTransport):
    """私有传输替身，只读端点返回预置数据。"""

    def __init__(self, tmp_path: Path) -> None:
        super().__init__("k", "s", RateLimiter(1000.0), tmp_path)

    def request(
        self,
        method: HttpMethod,
        path: str,
        *,
        params: Params | None = None,
        body: Mapping[str, object] | None = None,
    ) -> object:
        del method, params, body
        if path == "/v1/account/assets":
            return _ASSETS
        if path == "/v1/activeOrders":
            return {}
        if path == "/v1/latestExecutions":
            return {"list": []}
        raise AssertionError(f"未预置数据 {path}")


@pytest.fixture()
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """构造注入替身的测试客户端。"""
    for name in ("GUVOLU_MODE", "GUVOLU_SPOT_WHITELIST"):
        monkeypatch.delenv(name, raising=False)
    config = load_config(env_file=tmp_path / "absent.env")
    app = create_app(
        config,
        PublicClient(_FakePublicTransport()),
        ReadClient(_FakePrivateTransport(tmp_path)),
        _TOKEN,
    )
    return TestClient(
        app, base_url="http://127.0.0.1", headers={"X-Guvolu-Token": _TOKEN}
    )


def test_rejects_missing_token(client: TestClient) -> None:
    """无令牌一律 401（TBD-14）。"""
    bare = TestClient(client.app, base_url="http://127.0.0.1")
    assert bare.get("/api/health").status_code == 401


def test_health_reports_dry_run(client: TestClient) -> None:
    """健康端点报告缺省模拟运行（T-04）。"""
    payload: dict[str, Any] = client.get("/api/health").json()
    assert payload["mode"] == RunMode.DRY_RUN.value
    assert payload["frontend_mode"] == "view"


def test_capabilities_shape(client: TestClient) -> None:
    """能力清单含已具备与未具备两栏。"""
    payload = client.get("/api/capabilities").json()
    assert payload["implemented"] and payload["pending"]
    assert all("blocker" in item for item in payload["pending"])


def test_markets_v2_uses_market_identity(tmp_path: Path) -> None:
    """v2 市场目录以 market_id 返回身份，未覆盖市场保留空 domains。"""
    conn = connect(tmp_path)
    try:
        registry.register_all(conn)
        ensure_markets(conn)
        conn.commit()
    finally:
        conn.close()
    config = load_config(env_file=tmp_path / "absent.env")
    app = create_app(
        config,
        PublicClient(_FakePublicTransport()),
        ReadClient(_FakePrivateTransport(tmp_path)),
        _TOKEN,
        data_root=tmp_path,
    )
    catalog_client = TestClient(
        app, base_url="http://127.0.0.1",
        headers={"X-Guvolu-Token": _TOKEN},
    )
    response = catalog_client.get("/api/v2/markets")
    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == 1
    assert payload["venues"]
    gmo = next(
        item for item in payload["markets"]
        if item["market_id"] == "mkt__gmo__btc__r0"
    )
    assert gmo["instrument_id"] == "SPOT:BTC/JPY"
    assert gmo["venue_symbol"] == "BTC"
    assert gmo["base_currency"] == "BTC"
    assert gmo["quote_currency"] == "JPY"
    assert gmo["domains"] == {}


def test_assets_decimal_strings(client: TestClient) -> None:
    """金额以字符串返回（D-07）。"""
    payload = client.get("/api/assets").json()
    assert payload["items"][0]["amount"] == "3009"
    assert isinstance(payload["items"][0]["available"], str)
    assert payload["sources"][1]["status"] == "unconfigured"


class _FakeBitflyerRead:
    """bitFlyer 只读账户替身。"""

    def assets(self) -> tuple[BitflyerAsset, ...]:
        from decimal import Decimal

        return (
            BitflyerAsset("JPY", Decimal("1"), Decimal("1")),
            BitflyerAsset("BTC", Decimal("0.1"), Decimal("0.1")),
        )


def test_assets_group_venues_by_currency(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """多来源资产只在相同币种内合计，并保留来源分列。"""
    config = load_config(env_file=tmp_path / "absent.env")
    app = create_app(
        config,
        PublicClient(_FakePublicTransport()),
        ReadClient(_FakePrivateTransport(tmp_path)),
        _TOKEN,
        bitflyer_read=_FakeBitflyerRead(),  # type: ignore[arg-type]
    )
    client = TestClient(
        app, base_url="http://127.0.0.1", headers={"X-Guvolu-Token": _TOKEN}
    )
    payload = client.get("/api/assets").json()
    jpy = next(item for item in payload["items"] if item["symbol"] == "JPY")
    assert jpy["amount"] == "3010"
    assert jpy["venues"]["gmo"]["available"] == "3009"
    assert jpy["venues"]["bitflyer"]["available"] == "1"
    assert payload["sources"][1]["status"] == "ok"


def test_symbols_spot_rules_full(client: TestClient) -> None:
    """规则覆盖全部现物品种并排除杠杆形态，白名单单列。"""
    payload = client.get("/api/symbols").json()
    assert payload["whitelist"] == ["BTC"]
    assert [rule["symbol"] for rule in payload["rules"]] == ["BTC", "XRP"]


def test_ticker_serialization(client: TestClient) -> None:
    """行情序列化保持字符串。"""
    payload = client.get("/api/ticker", params={"symbol": "BTC"}).json()
    assert payload["last"] == "10141182"


def test_empty_tables(client: TestClient) -> None:
    """空挂单与空成交返回空列表而非缺字段。"""
    orders = client.get("/api/active-orders", params={"symbol": "BTC"}).json()
    fills = client.get("/api/latest-executions", params={"symbol": "BTC"}).json()
    assert orders["items"] == [] and fills["items"] == []


def test_ensure_token_stable(tmp_path: Path) -> None:
    """令牌文件生成后保持稳定。"""
    first = ensure_token(tmp_path)
    second = ensure_token(tmp_path)
    assert first == second and len(first) > 20


def test_jst_kline_date_before_boundary() -> None:
    """JST 06:00 前仍属前一交易日（D-08）。"""
    from datetime import UTC, datetime

    late_utc = datetime(2026, 8, 5, 16, 0, tzinfo=UTC)
    assert jst_kline_date(late_utc) == "20260805"


def test_jst_kline_date_after_boundary() -> None:
    """JST 06:00 后进入新交易日（D-08）。"""
    from datetime import UTC, datetime

    boundary_utc = datetime(2026, 8, 5, 21, 0, tzinfo=UTC)
    assert jst_kline_date(boundary_utc) == "20260806"

def test_kline_dates_daily_enumeration() -> None:
    """分钟周期逐交易日列举。"""
    dates, truncated = kline_dates(KlineInterval.HOUR_1, "20260801", "20260803")
    assert dates == ["20260801", "20260802", "20260803"]
    assert truncated is False


def test_kline_dates_truncation_keeps_latest() -> None:
    """超上限截断且保留最新（R-04）。"""
    dates, truncated = kline_dates(KlineInterval.MIN_1, "20260101", "20260430")
    assert truncated is True
    assert len(dates) == 24
    assert dates[-1] == "20260430"


def test_kline_dates_yearly() -> None:
    """日线以上按年列举。"""
    dates, truncated = kline_dates(KlineInterval.DAY_1, "20180905", "20260806")
    assert dates == [str(y) for y in range(2018, 2027)]
    assert truncated is False


def test_klines_range_aggregation(client: TestClient) -> None:
    """区间聚合并带元信息。"""
    payload = client.get(
        "/api/klines",
        params={"symbol": "BTC", "interval": "1day", "from": "20240101"},
    ).json()
    assert payload["meta"]["requests"] >= 2
    assert payload["meta"]["truncated"] is False
    assert isinstance(payload["items"], list)

def test_klines_store_hybrid(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """库存优先，当期上游刷新一刀（source 标注）。"""
    for name in ("GUVOLU_MODE", "GUVOLU_SPOT_WHITELIST"):
        monkeypatch.delenv(name, raising=False)
    conn = connect(tmp_path)
    row = (
        "BTC", "1day", "2024-06-01T00:00:00+00:00",
        "2024-06-02T00:00:00+00:00", "2024-06-02T01:00:00+00:00", "20240601",
        "100", "110", "90", "105", "5", 0, "d/klines.jsonl:1",
    )
    upsert_klines(conn, [row])
    conn.close()
    config = load_config(env_file=tmp_path / "absent.env")
    app = create_app(
        config,
        PublicClient(_FakePublicTransport()),
        ReadClient(_FakePrivateTransport(tmp_path)),
        _TOKEN,
        data_root=tmp_path,
    )
    client = TestClient(
        app, base_url="http://127.0.0.1", headers={"X-Guvolu-Token": _TOKEN}
    )
    payload = client.get(
        "/api/klines",
        params={"symbol": "BTC", "interval": "1day", "from": "20240101"},
    ).json()
    assert payload["meta"]["source"] == "store+live"
    assert payload["meta"]["requests"] == 1
    assert payload["items"][0]["open"] == "100"

def test_orderbooks_ladder(client: TestClient) -> None:
    """盘口快照含最优报价、覆盖和带内指标。"""
    payload = client.get("/api/orderbooks", params={"symbol": "BTC"}).json()
    assert payload["spread"] == "2"
    assert payload["mid"] == "100"
    assert payload["best_ask"] == "101"
    assert payload["best_bid"] == "99"
    assert payload["source"] == "REST"
    assert payload["microprice"] == "99.66666666666666666666666667"
    assert payload["coverage"]["ask_bp"] == "200"
    assert payload["bands"][0]["band_bp"] == "5"
    assert payload["ask_total"] == "0.5"
    assert payload["bids"][0]["price"] == "99"


def test_orderbooks_reuses_half_second_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """同一品种的并近请求复用取样时刻与上游快照。"""
    for name in ("GUVOLU_MODE", "GUVOLU_SPOT_WHITELIST"):
        monkeypatch.delenv(name, raising=False)
    config = load_config(env_file=tmp_path / "absent.env")
    transport = _FakePublicTransport()
    app = create_app(
        config,
        PublicClient(transport),
        ReadClient(_FakePrivateTransport(tmp_path)),
        _TOKEN,
    )
    cache_client = TestClient(
        app, base_url="http://127.0.0.1", headers={"X-Guvolu-Token": _TOKEN}
    )
    first = cache_client.get("/api/orderbooks", params={"symbol": "BTC"}).json()
    second = cache_client.get("/api/orderbooks", params={"symbol": "BTC"}).json()
    assert transport.orderbook_calls == 1
    assert second["as_of"] == first["as_of"]


@pytest.fixture()
def ops_client(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> TestClient:
    """构造带进程管理器的测试客户端（TBD-31）。"""
    for name in ("GUVOLU_MODE", "GUVOLU_SPOT_WHITELIST"):
        monkeypatch.delenv(name, raising=False)
    config = load_config(env_file=tmp_path / "absent.env")
    spec = ProcessSpec(
        name="record-fake",
        argv=(sys.executable, "-c", "import time; time.sleep(60)"),
        cwd=tmp_path,
        auto_restart=True,
    )
    manager = ProcessManager(
        (spec,),
        data_root=tmp_path / "data",
        log_dir=tmp_path / "logs",
        # 扫描与终止注入替身，离线（C-13）
        external_scan=lambda argv: [],
        external_terminate=lambda pid: True,
    )

    def _cleanup() -> None:
        for name in manager.names():
            manager.stop(name)

    request.addfinalizer(_cleanup)
    app = create_app(
        config,
        PublicClient(_FakePublicTransport()),
        ReadClient(_FakePrivateTransport(tmp_path)),
        _TOKEN,
        process_manager=manager,
    )
    return TestClient(
        app, base_url="http://127.0.0.1", headers={"X-Guvolu-Token": _TOKEN}
    )


def test_ops_processes_inventory(ops_client: TestClient) -> None:
    """登记表全量返回名称、状态、心跳与计数（TBD-31）。"""
    payload = ops_client.get("/api/ops/processes").json()
    row = payload["items"][0]
    assert row["name"] == "record-fake"
    assert row["status"] == "停止"
    assert row["heartbeat_at"] is None
    assert row["restart_count"] == 0
    assert "last_event_at" in row


def test_ops_start_idempotent_then_stop(ops_client: TestClient) -> None:
    """拉起幂等返回现状，停止转停止态。"""
    first = ops_client.post("/api/ops/processes/record-fake/start").json()
    assert first["item"]["status"] == "运行"
    pid = first["item"]["pid"]
    again = ops_client.post("/api/ops/processes/record-fake/start").json()
    assert again["item"]["pid"] == pid
    stopped = ops_client.post("/api/ops/processes/record-fake/stop").json()
    assert stopped["item"]["status"] == "停止"
    assert stopped["item"]["pid"] is None


def test_ops_unknown_process_404(ops_client: TestClient) -> None:
    """白名单之外一律 404，端点不接受自由命令。"""
    assert ops_client.post("/api/ops/processes/evil/start").status_code == 404
    assert ops_client.post("/api/ops/processes/evil/stop").status_code == 404


def test_ops_requires_token(ops_client: TestClient) -> None:
    """操作端点同受令牌守卫（TBD-14）。"""
    bare = TestClient(ops_client.app, base_url="http://127.0.0.1")
    assert bare.get("/api/ops/processes").status_code == 401


def test_book_heatmap_gap_marking(tmp_path: Path) -> None:
    """热力矩阵空档列显式标记且不插值。"""
    from datetime import UTC, datetime, timedelta

    from guvolu.ui.book_heatmap import build_heatmap

    now = datetime(2026, 8, 7, 12, 0, tzinfo=UTC)
    frame = {
        "time": now - timedelta(seconds=55),
        "payload": {
            "symbol": "BTC",
            "asks": [{"price": "101", "size": "1"}],
            "bids": [{"price": "99", "size": "2"}],
        },
    }
    matrix = build_heatmap([frame], 1.0, now)
    gaps = [col["gap"] for col in matrix["cols"]]
    assert gaps.count(False) == 1 and gaps.count(True) == len(gaps) - 1
    assert matrix["mid_row"][gaps.index(False)] is not None
    assert all(m is None for i, m in enumerate(matrix["mid_row"]) if gaps[i])


_DAY2 = "2026-01-03"


def _day2_epoch() -> int:
    from datetime import UTC, datetime

    return int(datetime(2026, 1, 3, tzinfo=UTC).timestamp())


def _write_vacuum_tile(data_root: Path) -> None:
    """手工瓦片日：末五列带内挂量清零，供判定夹具。"""
    import gzip
    import json as jsonlib
    from datetime import UTC, datetime

    from guvolu.data.heatmap_tiles import tile_paths

    base = _day2_epoch()
    gz_path, meta_path = tile_paths(data_root, "gmo", "BTC", "1s", _DAY2)
    gz_path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(gz_path, "wt", encoding="utf-8") as fh:
        for at in range(100):
            vacuum = at >= 95
            cells = (
                [["100", "bid", "0", "0", "5", "0"]]
                if vacuum
                else [["100", "bid", "5", "0", "0", "0"]]
            )
            column = {
                "t": datetime.fromtimestamp(base + at, UTC).isoformat(),
                "e": base + at,
                "gap": False,
                "carried": False,
                "reset": at == 0,
                "frames": 1,
                "mid": "102",
                "cells": cells,
                "bands": None,
            }
            fh.write(jsonlib.dumps(column) + "\n")
    meta_path.write_text(
        jsonlib.dumps(
            {
                "schema_version": 1,
                "venue": "gmo",
                "symbol": "BTC",
                "bucket": "1s",
                "bucket_seconds": 1,
                "date": _DAY2,
                "tick_size": "1",
                "row_tier": 1,
                "row_bin": "1",
                "columns": 100,
                "complete": True,
            }
        ),
        encoding="utf-8",
    )


@pytest.fixture()
def tiles_client(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> TestClient:
    """构造带瓦片数据与规则配置的测试客户端。"""
    from test_heatmap_tiles import build_synthetic_day

    for name in ("GUVOLU_MODE", "GUVOLU_SPOT_WHITELIST"):
        monkeypatch.delenv(name, raising=False)
    data_root = tmp_path / "data"
    data_root.mkdir()
    build_synthetic_day(data_root)
    _write_vacuum_tile(data_root)
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "alert_rules.json").write_text(
        """
        {"schema_version": 1, "rules": [
          {"rule_id": "vacuum-btc", "kind": "liquidity_vacuum",
           "symbol": "BTC", "overrides": {"min_confidence": "0.7"},
           "enabled": true}
        ]}
        """,
        encoding="utf-8",
    )
    config = load_config(env_file=tmp_path / "absent.env")
    app = create_app(
        config,
        PublicClient(_FakePublicTransport()),
        ReadClient(_FakePrivateTransport(tmp_path)),
        _TOKEN,
        data_root=data_root,
        config_dir=config_dir,
    )
    return TestClient(
        app, base_url="http://127.0.0.1", headers={"X-Guvolu-Token": _TOKEN}
    )


def test_heatmap_tiles_slice(tiles_client: TestClient) -> None:
    """瓦片端点：窗口切片、字符串数值、空档保留。"""
    payload = tiles_client.get(
        "/api/heatmap-tiles",
        params={
            "symbol": "BTC",
            "bucket": "1s",
            "from_ts": "2026-01-02T00:00:10Z",
            "to_ts": "2026-01-02T00:00:13Z",
        },
    ).json()
    assert payload["meta"]["row_bin"] == "1"
    columns = payload["columns"]
    assert len(columns) == 3
    cell = next(c for c in columns[1]["cells"] if c[0] == "101")
    assert cell[2] == "2" and cell[5] == "3"
    assert all(isinstance(v, str) for v in cell[2:])
    gap_payload = tiles_client.get(
        "/api/heatmap-tiles",
        params={
            "symbol": "BTC",
            "bucket": "1s",
            "from_ts": "2026-01-02T00:01:00Z",
            "to_ts": "2026-01-02T00:01:05Z",
        },
    ).json()
    assert all(col["gap"] for col in gap_payload["columns"])


def test_heatmap_tiles_validation(tiles_client: TestClient) -> None:
    """桶档白名单与窗口校验。"""
    bad_bucket = tiles_client.get(
        "/api/heatmap-tiles",
        params={
            "symbol": "BTC",
            "bucket": "100ms",
            "from_ts": "2026-01-02T00:00:00Z",
            "to_ts": "2026-01-02T00:01:00Z",
        },
    )
    assert bad_bucket.status_code == 400
    reversed_window = tiles_client.get(
        "/api/heatmap-tiles",
        params={
            "symbol": "BTC",
            "bucket": "1s",
            "from_ts": "2026-01-02T00:01:00Z",
            "to_ts": "2026-01-02T00:00:00Z",
        },
    )
    assert reversed_window.status_code == 400


def test_level_track_endpoint(tiles_client: TestClient) -> None:
    """档带追踪端点：挂量史与三值合计。"""
    payload = tiles_client.get(
        "/api/level-track",
        params={
            "symbol": "BTC",
            "price_bin": "101",
            "from_ts": "2026-01-02T00:00:10Z",
            "to_ts": "2026-01-02T00:00:13Z",
        },
    ).json()
    track = payload["track"]
    assert track["executed_total"] == "3"
    assert [row["qty"] for row in track["history"]] == ["5", "2", "2"]
    assert track["segments"][0]["vanished_at"] is None


def test_region_analysis_writes_feature_and_alert(
    tiles_client: TestClient,
) -> None:
    """区域分析：四判定并列、判定落库、报警匹配。"""
    payload = tiles_client.post(
        "/api/region-analysis",
        params={
            "symbol": "BTC",
            "price_lo": "100",
            "price_hi": "100",
            "from_ts": "2026-01-03T00:01:35Z",
            "to_ts": "2026-01-03T00:01:40Z",
        },
    ).json()
    kinds = [row["kind"] for row in payload["judgments"]]
    assert kinds == ["absorption", "pull", "sweep", "liquidity_vacuum"]
    vacuum = payload["judgments"][3]
    assert vacuum["met"] is True
    assert vacuum["feature_id"] >= 1
    assert vacuum["alert_id"] >= 1
    assert vacuum["metric_labels"]["min_depth"] == "最低挂量"
    assert vacuum["metric_labels"]["depth_percentile"] == "挂量分位"
    assert payload["meta"]["config_hash"]
    assert payload["meta"]["run_id"].startswith("run")
    assert payload["meta"]["confidence_version"] == "rule-strength-min-v2"
    alerts = tiles_client.get("/api/alerts").json()
    assert alerts["unacked"] == 1
    top = alerts["items"][0]
    assert top["rule_id"] == "vacuum-btc"
    assert top["kind"] == "liquidity_vacuum"
    assert top["label"] == "流动性真空"
    assert top["acked_at"] is None
    assert top["metrics"]["confidence"] == "0.75"
    assert top["metric_labels"]["confidence"] == "规则强度"


def test_region_analysis_replay_idempotent(tiles_client: TestClient) -> None:
    """区域分析重放：同请求同配置复用既有行与报警。"""
    params = {
        "symbol": "BTC",
        "price_lo": "100",
        "price_hi": "100",
        "from_ts": "2026-01-03T00:01:35Z",
        "to_ts": "2026-01-03T00:01:40Z",
    }
    first = tiles_client.post("/api/region-analysis", params=params).json()
    again = tiles_client.post("/api/region-analysis", params=params).json()
    f1 = first["judgments"][3]
    f2 = again["judgments"][3]
    assert f1["met"] is True and f2["met"] is True
    assert f1["feature_id"] == f2["feature_id"]
    assert f1["alert_id"] == f2["alert_id"]
    features = tiles_client.get(
        "/api/book-features", params={"symbol": "BTC"}
    ).json()
    assert len(features["items"]) == 1
    alerts = tiles_client.get("/api/alerts").json()
    assert len(alerts["items"]) == 1
    assert alerts["unacked"] == 1
    from guvolu.data.store import connect_readonly
    import json

    conn = connect_readonly(Path(tiles_client.app.state.data_root))
    assert conn is not None
    run = conn.execute(
        "SELECT judgments, confidence_version FROM analysis_run"
    ).fetchone()
    assert run is not None
    assert len(json.loads(run[0])) == 4
    assert run[1] == "rule-strength-min-v2"
    conn.close()


def test_book_features_listing(tiles_client: TestClient) -> None:
    """判读事件清单：落库事件按时序倒序返回。"""
    tiles_client.post(
        "/api/region-analysis",
        params={
            "symbol": "BTC",
            "price_lo": "100",
            "price_hi": "100",
            "from_ts": "2026-01-03T00:01:35Z",
            "to_ts": "2026-01-03T00:01:40Z",
        },
    )
    payload = tiles_client.get(
        "/api/book-features", params={"symbol": "BTC"}
    ).json()
    assert len(payload["items"]) >= 1
    row = payload["items"][0]
    assert row["kind"] == "liquidity_vacuum"
    assert row["label"] == "流动性真空"
    assert row["price_low"] == "100"
    assert isinstance(row["metrics"], dict)
    assert row["metric_labels"]["confidence"] == "规则强度"
    empty = tiles_client.get(
        "/api/book-features", params={"symbol": "XRP"}
    ).json()
    assert empty["items"] == []


def test_alert_ack_backfills_once(tiles_client: TestClient) -> None:
    """确认仅回填确认时刻，重复确认保持原值。"""
    tiles_client.post(
        "/api/region-analysis",
        params={
            "symbol": "BTC",
            "price_lo": "100",
            "price_hi": "100",
            "from_ts": "2026-01-03T00:01:35Z",
            "to_ts": "2026-01-03T00:01:40Z",
        },
    )
    alerts = tiles_client.get("/api/alerts").json()
    alert_id = alerts["items"][0]["alert_id"]
    first = tiles_client.post(f"/api/alerts/{alert_id}/ack").json()
    stamp = first["item"]["acked_at"]
    assert stamp is not None
    again = tiles_client.post(f"/api/alerts/{alert_id}/ack").json()
    assert again["item"]["acked_at"] == stamp
    assert tiles_client.post("/api/alerts/99999/ack").status_code == 404
    after = tiles_client.get("/api/alerts").json()
    assert after["unacked"] == 0


def test_level_track_coverage_flag(tiles_client: TestClient) -> None:
    """请求窗超出瓦片视界即标截断并给实际覆盖。"""
    clipped = tiles_client.get(
        "/api/level-track",
        params={
            "symbol": "BTC",
            "price_bin": "100",
            "from_ts": "2026-01-03T00:01:30Z",
            "to_ts": "2026-01-03T00:03:00Z",
        },
    ).json()
    meta = clipped["meta"]
    assert meta["coverage_clipped"] is True
    assert meta["coverage_from"] == "2026-01-03T00:01:30+00:00"
    assert meta["coverage_to"] == "2026-01-03T00:01:40+00:00"
    covered = tiles_client.get(
        "/api/level-track",
        params={
            "symbol": "BTC",
            "price_bin": "100",
            "from_ts": "2026-01-03T00:01:30Z",
            "to_ts": "2026-01-03T00:01:40Z",
        },
    ).json()
    assert covered["meta"]["coverage_clipped"] is False


def test_region_analysis_coverage_and_run_registry(
    tiles_client: TestClient,
) -> None:
    """区域分析：截断旗标、台账区域参数、检索端点。"""
    payload = tiles_client.post(
        "/api/region-analysis",
        params={
            "symbol": "BTC",
            "price_lo": "100",
            "price_hi": "100",
            "from_ts": "2026-01-03T00:01:35Z",
            "to_ts": "2026-01-03T00:02:00Z",
        },
    ).json()
    meta = payload["meta"]
    assert meta["coverage_clipped"] is True
    assert meta["coverage_to"] == "2026-01-03T00:01:40+00:00"
    assert meta["basis"] == "quantity"
    assert meta["columns"] == 5
    runs = tiles_client.get(
        "/api/analysis-runs", params={"symbol": "BTC"}
    ).json()
    assert len(runs["items"]) == 1
    run = runs["items"][0]
    assert run["run_id"] == meta["run_id"]
    assert run["basis"] == "quantity"
    assert run["window_columns"] == 5
    assert run["bucket"] == "1s"
    assert len(run["judgments"]) == 4
    assert "liquidity_vacuum" in run["met_kinds"]
    assert run["confidence_version"] == "rule-strength-min-v2"
    empty = tiles_client.get(
        "/api/analysis-runs", params={"symbol": "XRP"}
    ).json()
    assert empty["items"] == []


def test_region_analysis_rule_band_evaluation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """匹配器按规则带几何评估，成立事件落规则带。"""
    from test_heatmap_tiles import build_synthetic_day

    for name in ("GUVOLU_MODE", "GUVOLU_SPOT_WHITELIST"):
        monkeypatch.delenv(name, raising=False)
    data_root = tmp_path / "data"
    data_root.mkdir()
    build_synthetic_day(data_root)
    _write_vacuum_tile(data_root)
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "alert_rules.json").write_text(
        """
        {"schema_version": 2, "rules": [
          {"rule_id": "vacuum-band", "kind": "liquidity_vacuum",
           "symbol": "BTC", "overrides": {}, "band_bp": "200",
           "enabled": true}
        ]}
        """,
        encoding="utf-8",
    )
    config = load_config(env_file=tmp_path / "absent.env")
    app = create_app(
        config,
        PublicClient(_FakePublicTransport()),
        ReadClient(_FakePrivateTransport(tmp_path)),
        _TOKEN,
        data_root=data_root,
        config_dir=config_dir,
    )
    client = TestClient(
        app, base_url="http://127.0.0.1", headers={"X-Guvolu-Token": _TOKEN}
    )
    payload = client.post(
        "/api/region-analysis",
        params={
            "symbol": "BTC",
            "price_lo": "100",
            "price_hi": "100",
            "from_ts": "2026-01-03T00:01:35Z",
            "to_ts": "2026-01-03T00:01:40Z",
        },
    ).json()
    evaluations = payload["meta"]["rule_evaluations"]
    assert len(evaluations) == 1
    evaluation = evaluations[0]
    assert evaluation["band_source"] == "band_bp"
    assert evaluation["band_bp"] == "200"
    # 规则带自窗内中位中间价上下各 200bp
    assert evaluation["price_low"] == "99.96"
    assert evaluation["price_high"] == "104.04"
    assert evaluation["met"] is True
    assert evaluation["matched"] is True
    assert evaluation["alert_id"] >= 1
    # 报警引用规则带事件，请求带事件并存
    alerts = client.get("/api/alerts").json()
    assert len(alerts["items"]) == 1
    assert alerts["items"][0]["price_low"] == "99.96"
    assert alerts["items"][0]["rule_id"] == "vacuum-band"
    assert alerts["items"][0]["metrics"]["band_source"] == "band_bp"
    features = client.get(
        "/api/book-features", params={"symbol": "BTC"}
    ).json()
    bands = sorted(
        (row["price_low"], row["price_high"])
        for row in features["items"]
        if row["kind"] == "liquidity_vacuum"
    )
    assert bands == [("100", "100"), ("99.96", "104.04")]
    # 重放幂等：规则带事件与报警不重复
    client.post(
        "/api/region-analysis",
        params={
            "symbol": "BTC",
            "price_lo": "100",
            "price_hi": "100",
            "from_ts": "2026-01-03T00:01:35Z",
            "to_ts": "2026-01-03T00:01:40Z",
        },
    )
    again = client.get("/api/alerts").json()
    assert len(again["items"]) == 1


def test_region_analysis_window_cap(tiles_client: TestClient) -> None:
    """区域分析窗口日数超限即拒绝。"""
    response = tiles_client.post(
        "/api/region-analysis",
        params={
            "symbol": "BTC",
            "price_lo": "100",
            "price_hi": "101",
            "from_ts": "2026-01-01T00:00:00Z",
            "to_ts": "2026-01-05T00:00:00Z",
        },
    )
    assert response.status_code == 400


def test_print_ticks_endpoint(tiles_client: TestClient) -> None:
    """成交刻线端点：窗口内大额成交清单。"""
    payload = tiles_client.get(
        "/api/print-ticks",
        params={
            "symbol": "BTC",
            "from_ts": "2026-01-02T00:00:00Z",
            "to_ts": "2026-01-02T00:05:00Z",
        },
    ).json()
    assert len(payload["items"]) == 1
    item = payload["items"][0]
    assert item["price"] == "101"
    assert item["side"] == "BUY"
    assert payload["meta"]["quantile"] == "0.95"
    assert payload["meta"]["side_basis"] == "tick_rule_inference"



def test_heatmap_tiles_cache_headers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """完结日窗口带不可变缓存头，未完结禁缓存。"""
    from datetime import UTC, datetime
    from decimal import Decimal

    from test_heatmap_tiles import DAY, write_synthetic_raw

    from guvolu.data.heatmap_tiles import build_day_tiles

    for name in ("GUVOLU_MODE", "GUVOLU_SPOT_WHITELIST"):
        monkeypatch.delenv(name, raising=False)
    data_root = tmp_path / "data"
    data_root.mkdir()
    write_synthetic_raw(data_root)
    build_day_tiles(
        data_root, "BTC", DAY, Decimal("1"),
        now=datetime(2026, 1, 2, 0, 3, tzinfo=UTC),
    )
    config = load_config(env_file=tmp_path / "absent.env")
    app = create_app(
        config,
        PublicClient(_FakePublicTransport()),
        ReadClient(_FakePrivateTransport(tmp_path)),
        _TOKEN,
        data_root=data_root,
    )
    client = TestClient(
        app, base_url="http://127.0.0.1", headers={"X-Guvolu-Token": _TOKEN}
    )
    params = {
        "symbol": "BTC",
        "bucket": "1s",
        "from_ts": f"{DAY}T00:00:00+00:00",
        "to_ts": f"{DAY}T00:03:00+00:00",
    }
    pending = client.get("/api/heatmap-tiles", params=params)
    assert pending.headers["cache-control"] == "no-store"
    # 次日重建即完结，同窗转不可变
    build_day_tiles(
        data_root, "BTC", DAY, Decimal("1"),
        now=datetime(2026, 1, 3, 0, 0, tzinfo=UTC),
    )
    frozen = client.get("/api/heatmap-tiles", params=params)
    assert "immutable" in frozen.headers["cache-control"]
    today = datetime.now(UTC).strftime("%Y-%m-%d")
    live = client.get(
        "/api/heatmap-tiles",
        params={
            "symbol": "BTC",
            "bucket": "1s",
            "from_ts": f"{today}T00:00:00+00:00",
            "to_ts": f"{today}T00:01:00+00:00",
        },
    )
    assert live.headers["cache-control"] == "no-store"
