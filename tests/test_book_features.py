"""区域判读单测：四判定合成夹具与规则匹配。全程离线（C-13）。"""
from decimal import Decimal
from pathlib import Path

from guvolu.data.book_features import (
    BAND_SOURCE_EXPLICIT,
    BAND_SOURCE_REQUEST,
    BAND_SOURCE_RULE_BP,
    AlertRule,
    BandSample,
    analyze_region,
    band_sample,
    load_alert_rules,
    match_rules,
    percentile_rank,
    region_config,
    region_config_hash,
    rule_band,
)

_ZERO = Decimal("0")


def _column(
    at: int,
    cells: list[list[str]],
    mid: str,
    gap: bool = False,
) -> dict:
    return {
        "t": f"2026-01-02T00:00:{at:02d}+00:00",
        "e": 1000 + at,
        "gap": gap,
        "carried": False,
        "reset": False,
        "frames": 0 if gap else 1,
        "mid": None if gap else mid,
        "cells": [] if gap else cells,
        "bands": None,
    }


def _quiet_context(count: int, depth: str = "5") -> list[BandSample]:
    return [
        BandSample(
            executed=_ZERO, net_cancel=_ZERO, depth=Decimal(depth)
        )
        for _ in range(count)
    ]


def _by_kind(judgments: list[dict]) -> dict[str, dict]:
    return {str(row["kind"]): row for row in judgments}


def test_judgments_always_listed_in_parallel() -> None:
    """四判定并列输出，永不合成单一结论。"""
    judgments = analyze_region(
        [], [], Decimal("100"), Decimal("102"), Decimal("1")
    )
    kinds = [row["kind"] for row in judgments]
    assert kinds == ["absorption", "pull", "sweep", "liquidity_vacuum"]
    assert all(row["met"] is False for row in judgments)
    labels = [row["label"] for row in judgments]
    assert labels == ["吸收", "抽离", "扫单", "流动性真空"]


def test_absorption_fixture_met() -> None:
    """吸收：高消耗、回补达阈比、价格无进展。"""
    columns = [
        _column(0, [["100", "bid", "6", "0", "0", "4"]], "100.5"),
        _column(1, [["100", "bid", "7", "5", "0", "4"]], "100.5"),
    ]
    context = _quiet_context(40)
    judgments = _by_kind(
        analyze_region(
            columns, context, Decimal("100"), Decimal("100"), Decimal("1")
        )
    )
    absorption = judgments["absorption"]
    assert absorption["met"] is True
    assert absorption["confidence"] == "0.62"
    assert absorption["confidence_version"] == "rule-strength-min-v2"
    assert len(absorption["criteria"]) == 4
    assert absorption["metrics"]["executed_total"] == "8"
    assert absorption["metrics"]["net_add_total"] == "5"
    assert absorption["metrics"]["mid_drift_bp"] == "0.00"
    # 同夹具下扫单不成立（无连档击穿）
    assert judgments["sweep"]["met"] is False


def test_absorption_rejected_when_price_moves() -> None:
    """价格位移超阈即吸收不成立（判定非事实）。"""
    columns = [
        _column(0, [["100", "bid", "6", "0", "0", "4"]], "100"),
        _column(1, [["100", "bid", "7", "5", "0", "4"]], "101"),
    ]
    judgments = _by_kind(
        analyze_region(
            columns, _quiet_context(40),
            Decimal("100"), Decimal("100"), Decimal("1"),
        )
    )
    assert judgments["absorption"]["met"] is False


def test_pull_fixture_met_with_spoofing_flag() -> None:
    """抽离：逼近中高撤减低消耗，含嫌疑强形态。"""
    columns = [
        _column(0, [["100", "ask", "9", "0", "0", "0"]], "97"),
        _column(1, [["100", "ask", "0", "0", "9", "0"]], "99"),
    ]
    context = _quiet_context(60)
    judgments = _by_kind(
        analyze_region(
            columns, context, Decimal("100"), Decimal("100"), Decimal("1")
        )
    )
    pull = judgments["pull"]
    assert pull["met"] is True
    assert pull["metrics"]["approaching"] is True
    assert pull["metrics"]["net_cancel_total"] == "9"
    # 大额且短存续，嫌疑旗标为真
    assert pull["metrics"]["spoofing_suspicion"] is True


def test_pull_rejected_when_receding() -> None:
    """价格远离时抽离不成立。"""
    columns = [
        _column(0, [["100", "ask", "9", "0", "0", "0"]], "99"),
        _column(1, [["100", "ask", "0", "0", "9", "0"]], "97"),
    ]
    judgments = _by_kind(
        analyze_region(
            columns, _quiet_context(60),
            Decimal("100"), Decimal("100"), Decimal("1"),
        )
    )
    assert judgments["pull"]["met"] is False


def test_sweep_fixture_met() -> None:
    """扫单：单侧连续三档在桶跨度内被击穿。"""
    columns = [
        _column(0, [["100", "ask", "0", "0", "0", "2"]], "100"),
        _column(1, [["101", "ask", "0", "0", "0", "3"]], "101"),
        _column(2, [["102", "ask", "0", "0", "0", "1"]], "102"),
    ]
    judgments = _by_kind(
        analyze_region(
            columns, _quiet_context(30),
            Decimal("100"), Decimal("102"), Decimal("1"),
        )
    )
    sweep = judgments["sweep"]
    assert sweep["met"] is True
    assert sweep["metrics"]["levels_broken"] == "3"
    assert sweep["metrics"]["side"] == "ask"
    assert sweep["metrics"]["bucket_span"] == "3"


def test_sweep_rejected_when_span_too_wide() -> None:
    """击穿跨度超上限即扫单不成立。"""
    columns = []
    hits = {0: "100", 4: "101", 8: "102"}
    for at in range(9):
        cells = (
            [[hits[at], "ask", "0", "0", "0", "2"]] if at in hits else []
        )
        columns.append(_column(at, cells, "100"))
    judgments = _by_kind(
        analyze_region(
            columns, _quiet_context(30),
            Decimal("100"), Decimal("102"), Decimal("1"),
        )
    )
    assert judgments["sweep"]["met"] is False


def test_vacuum_fixture_met() -> None:
    """真空：带内挂量跌破低分位。"""
    columns = [
        _column(0, [["100", "bid", "0.1", "0", "0", "0"]], "100"),
    ]
    context = _quiet_context(99, depth="5") + [
        BandSample(executed=_ZERO, net_cancel=_ZERO, depth=Decimal("0.1"))
    ]
    judgments = _by_kind(
        analyze_region(
            columns, context, Decimal("100"), Decimal("100"), Decimal("1")
        )
    )
    vacuum = judgments["liquidity_vacuum"]
    assert vacuum["met"] is True
    assert vacuum["metrics"]["min_depth"] == "0.1"
    assert vacuum["metrics"]["depth_percentile"] == "0.01"


def test_band_sample_skips_gap_and_filters_band() -> None:
    """带内样本：空档列不计，带外格不计。"""
    column = _column(
        0,
        [
            ["100", "bid", "2", "1", "0", "3"],
            ["105", "ask", "9", "0", "0", "0"],
        ],
        "101",
    )
    sample = band_sample(column, Decimal("99"), Decimal("101"))
    assert sample is not None
    assert sample.executed == Decimal("3")
    assert sample.depth == Decimal("2")
    carried = dict(column)
    carried["carried"] = True
    assert band_sample(carried, Decimal("99"), Decimal("101")) is None
    gap = _column(1, [], "101", gap=True)
    assert band_sample(gap, Decimal("99"), Decimal("101")) is None


def test_percentile_rank_bounds() -> None:
    """分位序：空样本为零，全体不大于为一。"""
    assert percentile_rank([], Decimal("1")) == Decimal("0")
    population = [Decimal(v) for v in ("1", "2", "3", "4")]
    assert percentile_rank(population, Decimal("4")) == Decimal("1.00")
    assert percentile_rank(population, Decimal("1")) == Decimal("0.25")


def test_config_hash_stable_and_named() -> None:
    """阈值全部具名入配置，散列稳定（G-06、D-09）。"""
    config = region_config()
    assert "absorption_executed_percentile" in config
    assert "vacuum_depth_percentile" in config
    assert region_config_hash() == region_config_hash()
    assert len(region_config_hash()) == 64


def test_alert_rules_loading_and_matching(tmp_path: Path) -> None:
    """规则实例四元组装载与流上匹配。"""
    assert load_alert_rules(tmp_path) == ()
    (tmp_path / "alert_rules.json").write_text(
        """
        {"schema_version": 1, "rules": [
          {"rule_id": "a", "kind": "absorption", "symbol": "BTC",
           "overrides": {"min_confidence": "0.9"}, "enabled": true},
          {"rule_id": "b", "kind": "absorption", "symbol": "BTC",
           "overrides": {}, "enabled": false},
          {"rule_id": "c", "kind": "sweep", "symbol": "ETH",
           "overrides": {}, "enabled": true}
        ]}
        """,
        encoding="utf-8",
    )
    rules = load_alert_rules(tmp_path)
    assert len(rules) == 3
    # 置信度低于覆盖阈不匹配
    assert match_rules(rules, "absorption", "BTC", Decimal("0.8")) == []
    matched = match_rules(rules, "absorption", "BTC", Decimal("0.95"))
    assert [rule.rule_id for rule in matched] == ["a"]
    # 品种不合不匹配，停用不匹配
    assert match_rules(rules, "sweep", "BTC", Decimal("1")) == []


def _mixed_context(
    zeros: int, mids: int, highs: int, mid_value: str, high_value: str
) -> list[BandSample]:
    """分档消耗样本：分位刻度可控的基线总体。"""
    out = [
        BandSample(executed=_ZERO, net_cancel=_ZERO, depth=Decimal("5"))
        for _ in range(zeros)
    ]
    out += [
        BandSample(
            executed=Decimal(mid_value), net_cancel=_ZERO, depth=Decimal("5")
        )
        for _ in range(mids)
    ]
    out += [
        BandSample(
            executed=Decimal(high_value), net_cancel=_ZERO, depth=Decimal("5")
        )
        for _ in range(highs)
    ]
    return out


def test_confidence_low_when_barely_over_threshold() -> None:
    """正反例其一：刚过阈的成立事件置信度接近半。"""
    # 三判据全部恰在阈值处
    columns = [
        _column(0, [["100", "bid", "6", "0", "0", "5"]], "100"),
        _column(1, [["100", "bid", "6", "5", "0", "5"]], "100.05"),
    ]
    context = _mixed_context(85, 5, 10, "5", "10")
    judgments = _by_kind(
        analyze_region(
            columns, context, Decimal("100"), Decimal("100"), Decimal("1")
        )
    )
    absorption = judgments["absorption"]
    assert absorption["metrics"]["executed_percentile"] == "0.90"
    assert absorption["met"] is True
    assert absorption["confidence"] == "0.50"
    # 最低置信度门槛恢复效力
    rule = AlertRule(
        rule_id="a", kind="absorption", symbol="BTC",
        overrides={"min_confidence": "0.8"}, enabled=True,
    )
    assert match_rules([rule], "absorption", "BTC", Decimal("0.50")) == []


def test_confidence_high_but_below_one_when_far_over() -> None:
    """正反例其二：远超阈置信度高而不封顶为一。"""
    columns = [
        _column(0, [["100", "bid", "60", "0", "0", "50"]], "100"),
        _column(1, [["100", "bid", "60", "100", "0", "50"]], "100"),
    ]
    context = _mixed_context(96, 3, 1, "10", "200")
    judgments = _by_kind(
        analyze_region(
            columns, context, Decimal("100"), Decimal("100"), Decimal("1")
        )
    )
    absorption = judgments["absorption"]
    assert absorption["met"] is True
    assert absorption["metrics"]["executed_percentile"] == "0.99"
    confidence = Decimal(str(absorption["confidence"]))
    assert Decimal("0.8") <= confidence < Decimal("1")
    rule = AlertRule(
        rule_id="a", kind="absorption", symbol="BTC",
        overrides={"min_confidence": "0.8"}, enabled=True,
    )
    assert match_rules([rule], "absorption", "BTC", confidence)


def test_rule_band_geometry_resolution() -> None:
    """规则带几何三态：标准带、显式价带、缺省请求带。"""
    bp_rule = AlertRule(
        rule_id="a", kind="pull", symbol="BTC",
        overrides={}, enabled=True, band_bp="25",
    )
    resolved = rule_band(
        bp_rule, Decimal("1"), Decimal("2"), Decimal("10000")
    )
    assert resolved == (
        Decimal("9975"), Decimal("10025"), BAND_SOURCE_RULE_BP
    )
    # 标准带无窗内中价即不可评估
    assert rule_band(bp_rule, Decimal("1"), Decimal("2"), None) is None
    explicit = AlertRule(
        rule_id="b", kind="pull", symbol="BTC",
        overrides={}, enabled=True, band_low="99", band_high="101",
    )
    assert rule_band(
        explicit, Decimal("1"), Decimal("2"), Decimal("10000")
    ) == (Decimal("99"), Decimal("101"), BAND_SOURCE_EXPLICIT)
    plain = AlertRule(
        rule_id="c", kind="pull", symbol="BTC", overrides={}, enabled=True
    )
    assert rule_band(
        plain, Decimal("1"), Decimal("2"), None
    ) == (Decimal("1"), Decimal("2"), BAND_SOURCE_REQUEST)


def test_rule_band_loading_and_validation(tmp_path: Path) -> None:
    """带几何装载：两模式互斥，非法值拒绝。"""
    (tmp_path / "alert_rules.json").write_text(
        """
        {"schema_version": 2, "rules": [
          {"rule_id": "a", "kind": "absorption", "symbol": "BTC",
           "overrides": {}, "band_bp": "25", "enabled": true},
          {"rule_id": "b", "kind": "pull", "symbol": "BTC",
           "overrides": {}, "band_low": "99", "band_high": "101",
           "enabled": true}
        ]}
        """,
        encoding="utf-8",
    )
    rules = load_alert_rules(tmp_path)
    assert rules[0].band_bp == "25"
    assert rules[1].band_low == "99" and rules[1].band_high == "101"
    (tmp_path / "alert_rules.json").write_text(
        """
        {"schema_version": 2, "rules": [
          {"rule_id": "a", "kind": "absorption", "symbol": "BTC",
           "overrides": {}, "band_bp": "25", "band_low": "1",
           "band_high": "2", "enabled": true}
        ]}
        """,
        encoding="utf-8",
    )
    import pytest

    with pytest.raises(ValueError):
        load_alert_rules(tmp_path)
    (tmp_path / "alert_rules.json").write_text(
        """
        {"schema_version": 2, "rules": [
          {"rule_id": "a", "kind": "absorption", "symbol": "BTC",
           "overrides": {}, "band_low": "5", "enabled": true}
        ]}
        """,
        encoding="utf-8",
    )
    with pytest.raises(ValueError):
        load_alert_rules(tmp_path)


def test_repo_default_rules_valid() -> None:
    """仓库缺省规则文件可装载且全带带几何。"""
    rules = load_alert_rules(Path("config"))
    assert rules
    kinds = {rule.kind for rule in rules}
    assert kinds <= {"absorption", "pull", "sweep", "liquidity_vacuum"}
    for rule in rules:
        assert isinstance(rule, AlertRule)
        assert rule.rule_id and rule.symbol
        assert rule.band_bp is not None
