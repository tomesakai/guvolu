"""瓦片金字塔单测：帧去重、空档、三值恒等式、五带、刻线。全程离线（C-13）。"""
import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from guvolu.data.heatmap_tiles import (
    PRINT_TICK_QUANTILE,
    TILE_BUCKETS,
    VENUE_GMO,
    build_day_tiles,
    build_print_ticks,
    iter_tile_columns,
    level_track,
    load_print_ticks,
    load_tile_meta,
    index_day_chunks,
    pending_dates,
    slice_columns,
    tile_paths,
)

DAY = "2026-01-02"
DAY_EPOCH = int(
    datetime(2026, 1, 2, tzinfo=UTC).timestamp()
)


def _ws_line(channel: str, payload: dict) -> str:
    return json.dumps(
        {
            "schema_version": 1,
            "run_id": "runtest",
            "source": "ws_public",
            "channel": channel,
            "symbol": payload.get("symbol"),
            "payload": {"channel": channel, **payload},
            "ingest_time": f"{DAY}T00:10:00+00:00",
        }
    )


def _book(stamp: str, asks: list, bids: list) -> str:
    return _ws_line(
        "orderbooks",
        {
            "symbol": "BTC",
            "asks": [{"price": p, "size": s} for p, s in asks],
            "bids": [{"price": p, "size": s} for p, s in bids],
            "timestamp": stamp,
        },
    )


def _trade(stamp: str, price: str, size: str, side: str) -> str:
    return _ws_line(
        "trades",
        {
            "symbol": "BTC",
            "price": price,
            "size": size,
            "side": side,
            "timestamp": stamp,
        },
    )


def write_synthetic_raw(data_root: Path) -> None:
    """合成单日 raw：含双写者重复帧、成交与录制空窗。"""
    lines = [
        # 首帧与其双写者重复帧
        _book(
            f"{DAY}T00:00:10.000Z",
            [("101", "5"), ("102", "3")],
            [("99", "4"), ("98", "2")],
        ),
        _book(
            f"{DAY}T00:00:10.000Z",
            [("101", "5"), ("102", "3")],
            [("99", "4"), ("98", "2")],
        ),
        # 双侧成对打印，去重合一为一笔
        _trade(f"{DAY}T00:00:11.000Z", "101", "3", "BUY"),
        _trade(f"{DAY}T00:00:11.000Z", "101", "3", "SELL"),
        _book(
            f"{DAY}T00:00:11.200Z",
            [("101", "2"), ("102", "3")],
            [("99", "4"), ("98", "2")],
        ),
        # 长静默后恢复帧（超延载上限）
        _book(
            f"{DAY}T00:02:00.000Z",
            [("103", "1")],
            [("97", "6")],
        ),
    ]
    day_dir = data_root / "raw" / DAY
    day_dir.mkdir(parents=True, exist_ok=True)
    (day_dir / "ws_public.jsonl").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )


def build_synthetic_day(data_root: Path) -> dict:
    """构建合成日瓦片，视界为当日 00:03。"""
    write_synthetic_raw(data_root)
    return build_day_tiles(
        data_root,
        "BTC",
        DAY,
        Decimal("1"),
        now=datetime(2026, 1, 2, 0, 3, tzinfo=UTC),
    )


def _columns(data_root: Path, bucket: str = "1s") -> list[dict]:
    return list(iter_tile_columns(data_root, VENUE_GMO, "BTC", bucket, DAY))


def _cell(column: dict, price_bin: str) -> list | None:
    for cell in column["cells"]:
        if cell[0] == price_bin:
            return cell
    return None


def test_build_dedupes_frames_and_marks_gaps(tmp_path: Path) -> None:
    """帧按 timestamp 去重，空档列显式保留。"""
    report = build_synthetic_day(tmp_path)
    assert report["frames"] == 3
    columns = _columns(tmp_path)
    assert len(columns) == 180
    # 首帧前全为空档列
    assert all(col["gap"] for col in columns[:10])
    assert columns[10]["gap"] is False
    assert columns[10]["frames"] == 1
    assert columns[10]["reset"] is True
    # 静默段先延载后空档
    assert columns[12]["carried"] is True
    assert columns[40]["carried"] is True
    assert columns[41]["gap"] is True
    assert columns[119]["gap"] is True
    # 恢复帧基线重置
    assert columns[120]["gap"] is False
    assert columns[120]["reset"] is True
    # 末帧后先延载至上限再空档
    assert columns[149]["carried"] is True
    assert columns[150]["gap"] is True
    meta = load_tile_meta(tmp_path, VENUE_GMO, "BTC", "1s", DAY)
    assert meta is not None
    assert meta["complete"] is False
    assert meta["columns"] == 180
    assert meta["gap_columns"] == 10 + (120 - 41) + 30


def test_three_way_identity(tmp_path: Path) -> None:
    """恒等式：挂量差 = 净增挂 − 净撤减 − 成交消耗。"""
    build_synthetic_day(tmp_path)
    columns = _columns(tmp_path)
    previous: dict[str, Decimal] = {}
    for column in columns:
        if column["gap"]:
            previous = {}
            continue
        if column["reset"]:
            # 基线重置列不与前列比较
            previous = {
                cell[0]: Decimal(cell[2]) for cell in column["cells"]
            }
            continue
        keys = {cell[0] for cell in column["cells"]} | set(previous)
        for key in keys:
            cell = _cell(column, key)
            qty = Decimal(cell[2]) if cell else Decimal(0)
            add = Decimal(cell[3]) if cell else Decimal(0)
            cancel = Decimal(cell[4]) if cell else Decimal(0)
            eaten = Decimal(cell[5]) if cell else Decimal(0)
            before = previous.get(key, Decimal(0))
            assert qty - before == add - cancel - eaten, (column["t"], key)
        previous = {
            cell[0]: Decimal(cell[2]) for cell in column["cells"]
        }


def test_executed_depth_from_prints(tmp_path: Path) -> None:
    """成交消耗自逐笔精确扣除，净值为残差。"""
    build_synthetic_day(tmp_path)
    columns = _columns(tmp_path)
    cell = _cell(columns[11], "101")
    assert cell is not None
    # 挂量 5 变 2，成交 3，残差为零
    assert cell[2] == "2"
    assert cell[3] == "0"
    assert cell[4] == "0"
    assert cell[5] == "3"


def test_bands_series(tmp_path: Path) -> None:
    """五带随列产出：价差、OFI、不平衡、Delta、深度。"""
    build_synthetic_day(tmp_path)
    columns = _columns(tmp_path)
    first = columns[10]["bands"]
    # 中间价 100，价差 200bp
    assert first["spread_bp"] == "200.00"
    assert columns[10]["mid"] == "100"
    # 帧内买深 6 卖深 8，不平衡 -1/7
    assert first["imbalance"] == "-0.1429"
    second = columns[11]["bands"]
    # 卖侧最优 5 减至 2，OFI 为正 3
    assert second["ofi"] == "3"
    assert second["trade_delta"] == "3"
    depth = {row[0]: (row[1], row[2]) for row in second["depth"]}
    assert set(depth) == {"5", "10", "25"}
    # 空档列带值置空但 Delta 仍在
    gap_bands = columns[50]["bands"]
    assert gap_bands["spread_bp"] is None
    assert gap_bands["trade_delta"] == "0"
    # 延载列沿用末帧带值
    carried = columns[12]["bands"]
    assert carried["spread_bp"] == "200.00"
    assert columns[12]["cells"] and columns[12]["cells"][0][3] == "0"


def test_print_ticks_quantile() -> None:
    """成交刻线分位：阈取最近秩，阈下不入清单。"""
    prints = [
        (1000 + at, Decimal("100"), Decimal(at + 1), "BUY")
        for at in range(100)
    ]
    built = build_print_ticks(prints, PRINT_TICK_QUANTILE)
    assert built["threshold"] == "95"
    sizes = [item["size"] for item in built["items"]]
    assert sizes == ["95", "96", "97", "98", "99", "100"]
    assert built["items"][0]["size_quantile"] == "0.9500"
    assert built["prints_total"] == 100


def test_print_ticks_file_and_window(tmp_path: Path) -> None:
    """刻线文件随构建产出并按窗口切片。"""
    build_synthetic_day(tmp_path)
    day = load_print_ticks(
        tmp_path, VENUE_GMO, "BTC", DAY_EPOCH, DAY_EPOCH + 86400
    )
    assert len(day["items"]) == 1
    item = day["items"][0]
    assert item["price"] == "101"
    assert item["size"] == "3"
    assert item["side"] == "BUY"
    empty = load_print_ticks(
        tmp_path, VENUE_GMO, "BTC", DAY_EPOCH + 60, DAY_EPOCH + 120
    )
    assert empty["items"] == []
    assert empty["meta"]["quantile"] == "0.95"


def test_slice_columns_window_and_cap(tmp_path: Path) -> None:
    """窗口切片按列区间裁剪并可截断。"""
    build_synthetic_day(tmp_path)
    sliced = slice_columns(
        tmp_path, VENUE_GMO, "BTC", "1s", DAY_EPOCH + 10, DAY_EPOCH + 13
    )
    assert [col["e"] - DAY_EPOCH for col in sliced["columns"]] == [10, 11, 12]
    assert sliced["meta"]["row_bin"] == "1"
    assert sliced["meta"]["truncated"] is False
    meta = load_tile_meta(tmp_path, VENUE_GMO, "BTC", "1s", DAY)
    assert meta is not None
    assert meta["chunk_columns"] == 512
    gz_path, _ = tile_paths(tmp_path, VENUE_GMO, "BTC", "1s", DAY)
    gz_path.unlink()
    from_chunks = slice_columns(
        tmp_path, VENUE_GMO, "BTC", "1s", DAY_EPOCH + 10, DAY_EPOCH + 13
    )
    assert [col["e"] - DAY_EPOCH for col in from_chunks["columns"]] == [
        10,
        11,
        12,
    ]
    capped = slice_columns(
        tmp_path, VENUE_GMO, "BTC", "1s",
        DAY_EPOCH, DAY_EPOCH + 180, max_columns=7,
    )
    assert len(capped["columns"]) == 7
    assert capped["meta"]["truncated"] is True
    missing = slice_columns(
        tmp_path, VENUE_GMO, "BTC", "1s",
        DAY_EPOCH + 86400, DAY_EPOCH + 86460,
    )
    assert missing["columns"] == []
    assert missing["meta"]["missing_dates"] == ["2026-01-03"]


def test_legacy_tile_chunk_index(tmp_path: Path) -> None:
    """旧整日制品可一次扫描补块并幂等。"""
    build_synthetic_day(tmp_path)
    _, meta_path = tile_paths(tmp_path, VENUE_GMO, "BTC", "1s", DAY)
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    meta.pop("chunk_generation")
    meta.pop("chunk_columns")
    meta_path.write_text(json.dumps(meta), encoding="utf-8")
    first = index_day_chunks(tmp_path, VENUE_GMO, "BTC", "1s", DAY)
    again = index_day_chunks(tmp_path, VENUE_GMO, "BTC", "1s", DAY)
    assert first == {"date": DAY, "indexed": True, "chunks": 1}
    assert again == {"date": DAY, "indexed": False, "chunks": 0}


def test_multi_bucket_pyramid(tmp_path: Path) -> None:
    """三桶档同趟构建，粗桶列数按秒宽折算。"""
    build_synthetic_day(tmp_path)
    for bucket, seconds in TILE_BUCKETS.items():
        meta = load_tile_meta(tmp_path, VENUE_GMO, "BTC", bucket, DAY)
        assert meta is not None
        assert meta["columns"] == 180 // seconds
        gz_path, _ = tile_paths(tmp_path, VENUE_GMO, "BTC", bucket, DAY)
        assert gz_path.exists()
    minute = _columns(tmp_path, "1min")
    assert len(minute) == 3
    # 首分钟含全部成交消耗
    assert minute[0]["gap"] is False
    cell = _cell(minute[0], "100")
    assert cell is not None and cell[5] == "3"


def test_rebuild_idempotent(tmp_path: Path) -> None:
    """幂等重建：重跑产物一致（时刻字段除外）。"""
    build_synthetic_day(tmp_path)
    first = _columns(tmp_path)
    build_synthetic_day(tmp_path)
    assert _columns(tmp_path) == first


def test_pending_dates_seeks_incomplete(tmp_path: Path) -> None:
    """求缺：未完结日始终在求缺清单内。"""
    write_synthetic_raw(tmp_path)
    assert pending_dates(tmp_path, VENUE_GMO, "BTC") == [DAY]
    build_synthetic_day(tmp_path)
    # 视界早于日终，标未完结仍求缺
    assert pending_dates(tmp_path, VENUE_GMO, "BTC") == [DAY]


def test_level_track_lifetime_and_reaction() -> None:
    """档带追踪：存续期、撤单率、补单与价格反应。"""
    def column(at: int, qty: str, add: str, cancel: str, eaten: str, mid: str) -> dict:
        cells = []
        if any(Decimal(v) != 0 for v in (qty, add, cancel, eaten)):
            cells = [["100", "bid", qty, add, cancel, eaten]]
        return {
            "t": f"2026-01-02T00:00:{at:02d}+00:00",
            "e": DAY_EPOCH + at,
            "gap": False,
            "carried": False,
            "reset": at == 0,
            "frames": 1,
            "mid": mid,
            "cells": cells,
            "bands": None,
        }

    columns = [
        column(0, "5", "5", "0", "0", "101"),
        column(1, "3", "0", "0", "2", "101"),
        column(2, "4", "1", "0", "0", "101"),
        column(3, "0", "0", "3", "1", "101"),
        column(4, "0", "0", "0", "0", "99"),
        column(5, "0", "0", "0", "0", "99"),
    ]
    tracked = level_track(columns, "100", reaction_buckets=1)
    assert tracked["segments"] == [
        {
            "first_seen": "2026-01-02T00:00:00+00:00",
            "last_seen": "2026-01-02T00:00:02+00:00",
            "vanished_at": "2026-01-02T00:00:03+00:00",
            "buckets": 3,
        }
    ]
    assert tracked["net_add_total"] == "6"
    assert tracked["net_cancel_total"] == "3"
    assert tracked["executed_total"] == "3"
    assert tracked["cancel_ratio"] == "0.5000"
    # 首次消耗有回补，末次消耗后无回补
    assert tracked["replenishment"]["count"] == 1
    assert tracked["replenishment"]["size"] == "1"
    reaction = tracked["price_reaction"]
    assert reaction is not None
    assert reaction["before_mid"] == "101"
    assert reaction["after_mid"] == "99"
    assert reaction["change_bp"] == "-198.02"
    history = [row["qty"] for row in tracked["history"]]
    assert history == ["5", "3", "4", "0", "0", "0"]


def write_carried_trades_raw(data_root: Path) -> None:
    """合成日：七笔合计 0.01473 落入延载列，一笔落空档列。

    对照验证专项缺陷 1 的实测量化案例形态。
    """
    sizes = ["0.002", "0.002", "0.002", "0.002", "0.002", "0.002", "0.00273"]
    lines = [
        _book(f"{DAY}T00:00:10.000Z", [("101", "0.5")], [("99", "0.4")]),
    ]
    for at, size in enumerate(sizes):
        stamp = f"{DAY}T00:00:{12 + at}.000Z"
        lines.append(_trade(stamp, "101", size, "BUY"))
        lines.append(_trade(stamp, "101", size, "SELL"))
    lines.append(
        _book(f"{DAY}T00:00:25.000Z", [("101", "0.48527")], [("99", "0.4")])
    )
    # 末帧延载三十秒后转空档，空档内一笔
    lines.append(_trade(f"{DAY}T00:01:10.000Z", "100", "0.003", "BUY"))
    lines.append(_trade(f"{DAY}T00:01:10.000Z", "100", "0.003", "SELL"))
    day_dir = data_root / "raw" / DAY
    day_dir.mkdir(parents=True, exist_ok=True)
    (day_dir / "ws_public.jsonl").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )


def test_carried_prints_attributed_to_natural_columns(tmp_path: Path) -> None:
    """延载列逐笔记入其自然列，不再错记后续闭合列。"""
    write_carried_trades_raw(tmp_path)
    build_day_tiles(
        tmp_path,
        "BTC",
        DAY,
        Decimal("1"),
        now=datetime(2026, 1, 2, 0, 2, tzinfo=UTC),
    )
    columns = _columns(tmp_path)
    # 七笔各入其延载列，量与案例合计一致
    eaten_total = Decimal("0")
    hit_columns = 0
    for at in range(12, 19):
        column = columns[at]
        assert column["carried"] is True
        cell = _cell(column, "101")
        assert cell is not None
        assert cell[3] == "0" and cell[4] == "0"
        eaten_total += Decimal(cell[5])
        hit_columns += 1
    assert hit_columns == 7
    assert eaten_total == Decimal("0.01473")
    # 延载末态逐列扣减传递
    first = _cell(columns[12], "101")
    last = _cell(columns[18], "101")
    assert first is not None and first[2] == "0.498"
    assert last is not None and last[2] == "0.48527"
    # 后续闭合列不再出现假净撤减
    closed = columns[25]
    assert closed["carried"] is False and closed["gap"] is False
    cell = _cell(closed, "101")
    assert cell is not None
    assert cell[2] == "0.48527"
    assert cell[4] == "0"
    assert cell[5] == "0"


def test_gap_prints_recorded_as_facts(tmp_path: Path) -> None:
    """空档列只记成交消耗事实，净值不可知记零。"""
    write_carried_trades_raw(tmp_path)
    build_day_tiles(
        tmp_path,
        "BTC",
        DAY,
        Decimal("1"),
        now=datetime(2026, 1, 2, 0, 2, tzinfo=UTC),
    )
    columns = _columns(tmp_path)
    gap_column = columns[70]
    assert gap_column["gap"] is True
    cell = _cell(gap_column, "100")
    assert cell is not None
    assert cell[1] == "void"
    assert cell[2] == "0" and cell[3] == "0" and cell[4] == "0"
    assert cell[5] == "0.003"
    bands = gap_column["bands"]
    # 价乘量，方向按 tick 规则
    assert bands["trade_delta"] == "-0.003"
    assert bands["trade_delta_notional"] == "-0.300"


def test_carried_identity_holds_with_consumption(tmp_path: Path) -> None:
    """延载消耗后三值恒等式在列链上保持成立。"""
    write_carried_trades_raw(tmp_path)
    build_day_tiles(
        tmp_path,
        "BTC",
        DAY,
        Decimal("1"),
        now=datetime(2026, 1, 2, 0, 2, tzinfo=UTC),
    )
    columns = _columns(tmp_path)
    previous: dict[str, Decimal] = {}
    for column in columns[:60]:
        if column["gap"]:
            previous = {}
            continue
        if column["reset"]:
            previous = {
                cell[0]: Decimal(cell[2]) for cell in column["cells"]
            }
            continue
        keys = {cell[0] for cell in column["cells"]} | set(previous)
        for key in keys:
            cell = _cell(column, key)
            qty = Decimal(cell[2]) if cell else Decimal(0)
            add = Decimal(cell[3]) if cell else Decimal(0)
            cancel = Decimal(cell[4]) if cell else Decimal(0)
            eaten = Decimal(cell[5]) if cell else Decimal(0)
            before = previous.get(key, Decimal(0))
            assert qty - before == add - cancel - eaten, (column["t"], key)
        previous = {
            cell[0]: Decimal(cell[2]) for cell in column["cells"]
        }


def test_bands_dual_basis_notional(tmp_path: Path) -> None:
    """量类两带附金额基准：逐笔与帧内价乘量精确累计。"""
    lines = [
        _book(
            f"{DAY}T00:00:10.000Z", [("10001", "2")], [("9999", "4")]
        ),
        _trade(f"{DAY}T00:00:11.000Z", "10001", "3", "BUY"),
        _trade(f"{DAY}T00:00:11.000Z", "10001", "3", "SELL"),
        _book(
            f"{DAY}T00:00:11.200Z", [("10001", "2")], [("9999", "4")]
        ),
    ]
    day_dir = tmp_path / "raw" / DAY
    day_dir.mkdir(parents=True, exist_ok=True)
    (day_dir / "ws_public.jsonl").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )
    build_day_tiles(
        tmp_path,
        "BTC",
        DAY,
        Decimal("1"),
        now=datetime(2026, 1, 2, 0, 1, tzinfo=UTC),
    )
    columns = _columns(tmp_path)
    closed = columns[11]
    bands = closed["bands"]
    assert bands["trade_delta"] == "3"
    assert bands["trade_delta_notional"] == "30003"
    depth = {row[0]: row for row in bands["depth"]}
    # 5bp 带含双侧两档，金额为价乘量合计
    assert depth["5"][1] == "6"
    assert depth["5"][3] == "59998"
    assert len(depth["5"]) == 4
    # 延载列带值沿用末帧，含金额基准
    carried = columns[12]
    assert carried["carried"] is True
    carried_depth = {row[0]: row for row in carried["bands"]["depth"]}
    assert carried_depth["5"][3] == "59998"
    assert carried["bands"]["trade_delta_notional"] == "0"


def test_incremental_builder_appends_only_new(tmp_path: Path) -> None:
    """增量构建：游标推进、只追加新列、前缀与全量一致。"""
    from decimal import Decimal

    from guvolu.data.heatmap_tiles import (
        IncrementalTileBuilder,
        cursor_path,
    )

    write_synthetic_raw(tmp_path)
    raw = tmp_path / "raw" / DAY / "ws_public.jsonl"
    whole = raw.read_bytes()
    lines = whole.splitlines(keepends=True)
    # 参照：全量构建于另一根目录
    reference = tmp_path / "ref"
    (reference / "raw" / DAY).mkdir(parents=True)
    (reference / "raw" / DAY / "ws_public.jsonl").write_bytes(whole)
    build_day_tiles(
        reference, "BTC", DAY, Decimal("1"),
        now=datetime(2026, 1, 2, 0, 3, tzinfo=UTC),
    )
    full_cols = {
        bucket: list(
            iter_tile_columns(reference, VENUE_GMO, "BTC", bucket, DAY)
        )
        for bucket in TILE_BUCKETS
    }
    # 第一批：前四行
    raw.write_bytes(b"".join(lines[:4]))
    builder = IncrementalTileBuilder(tmp_path, "BTC", DAY, Decimal("1"))
    first = builder.refresh()
    offset_first = builder.offset
    assert offset_first == len(b"".join(lines[:4]))
    flushed_first = dict(builder.flushed)
    # 第二批：追加剩余行
    raw.write_bytes(whole)
    second = builder.refresh()
    assert builder.offset == len(whole)
    assert builder.offset > offset_first
    appended = second["appended"]
    assert isinstance(appended, dict)
    # 追加列数为增量而非全量
    for bucket in TILE_BUCKETS:
        grown = builder.flushed[bucket] - flushed_first[bucket]
        assert appended[bucket] == grown
    # 无新字节的刷新不追加
    third = builder.refresh()
    assert third["offset"] == builder.offset
    assert all(count == 0 for count in third["appended"].values())
    # 已闭桶前缀与全量结果逐列一致
    for bucket in TILE_BUCKETS:
        got = list(iter_tile_columns(tmp_path, VENUE_GMO, "BTC", bucket, DAY))
        assert len(got) == builder.flushed[bucket]
        assert got == full_cols[bucket][: len(got)]
        meta = load_tile_meta(tmp_path, VENUE_GMO, "BTC", bucket, DAY)
        assert meta is not None
        assert meta["incremental"] is True and meta["complete"] is False
    cursor = json.loads(
        cursor_path(tmp_path, VENUE_GMO, "BTC", DAY).read_text(
            encoding="utf-8"
        )
    )
    assert cursor["offset"] == len(whole)
    assert cursor["finalized"] is False
    assert first["date"] == DAY


def test_incremental_finalize_completes_day(tmp_path: Path) -> None:
    """收尾补齐至日末并标完结，成交刻线随之落盘。"""
    from decimal import Decimal

    from guvolu.data.heatmap_tiles import (
        IncrementalTileBuilder,
        cursor_path,
        print_ticks_path,
    )

    write_synthetic_raw(tmp_path)
    builder = IncrementalTileBuilder(tmp_path, "BTC", DAY, Decimal("1"))
    builder.refresh()
    builder.finalize()
    for bucket, seconds in TILE_BUCKETS.items():
        meta = load_tile_meta(tmp_path, VENUE_GMO, "BTC", bucket, DAY)
        assert meta is not None
        assert meta["complete"] is True
        assert meta["columns"] == 86400 // seconds
    cursor = json.loads(
        cursor_path(tmp_path, VENUE_GMO, "BTC", DAY).read_text(
            encoding="utf-8"
        )
    )
    assert cursor["finalized"] is True
    ticks = json.loads(
        print_ticks_path(tmp_path, VENUE_GMO, "BTC", DAY).read_text(
            encoding="utf-8"
        )
    )
    assert ticks["prints_total"] == 1
