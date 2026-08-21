"""经济成交资格与流量 fail-close 回归。"""
from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pytest

from guvolu.data.trade_economics import (
    economic_trade_qualification_sql,
    economic_trade_qualified,
)
from guvolu.research.features import compute_features
from guvolu.research.contracts import (
    FrozenPanelInputs,
    FrozenPanelPartition,
    PanelSnapshot,
)
from guvolu.research.panel import (
    _trade_qualification_summaries,
    compact_trade_panel,
    load_panel_bars,
)
from guvolu.research.verification import _attest_panel_volume_qualification
from guvolu.strategy.baselines import generate_targets
from guvolu.strategy.contracts import ResearchBar
from guvolu.strategy.expression import (
    candidate_identity,
    expression_id,
    strategy_expression,
)
from guvolu.strategy.contracts import CandidateSpec


def _bar(index: int, *, qualified: bool) -> ResearchBar:
    start = datetime(2026, 1, 1, tzinfo=UTC) + timedelta(hours=index)
    return ResearchBar(
        open_time=start,
        decision_time=start + timedelta(hours=1),
        latest_available_time=start + timedelta(minutes=59),
        open=100.0 + index,
        high=101.0 + index,
        low=99.0 + index,
        close=100.5 + index,
        base_volume=1.0 if qualified else 0.0,
        quote_volume=100.0 if qualified else 0.0,
        signed_base_volume=0.5 if qualified else 0.0,
        trade_count=1 if qualified else 0,
        source_trade_count=1,
        unqualified_trade_count=0 if qualified else 1,
        volume_qualified=qualified,
    )


def test_gmo_realtime_requires_physical_r1_v4_lineage() -> None:
    base: dict[str, object] = {
        "venue_id": "gmo",
        "normalization_version": "trade-realtime-normalization-v4",
        "raw_schema_version": 3,
        "endpoint_id": "EP-0007",
        "endpoint_revision": 1,
        "source_side_basis": "taker",
    }
    assert economic_trade_qualified(base)
    for key, value in (
        ("normalization_version", "trade-realtime-normalization-v3"),
        ("raw_schema_version", 2),
        ("endpoint_revision", 0),
        ("source_side_basis", "participant_side_unfiltered"),
    ):
        changed = dict(base)
        changed[key] = value
        assert not economic_trade_qualified(changed)


def test_gmo_verified_archive_is_a_separate_allowed_contract() -> None:
    assert economic_trade_qualified({
        "venue_id": "gmo",
        "normalization_version": "trade-normalization-v1",
        "source_side_basis": "taker",
    })


def test_gmo_realtime_control_fails_closed_when_lineage_columns_are_missing(
) -> None:
    assert economic_trade_qualification_sql(
        "gmo",
        {"normalization_version", "source_side_basis"},
        ("trade_realtime", "trade-realtime-normalization-v4"),
    ) == "FALSE"


def test_coinbase_verified_maker_label_remains_economic_trade() -> None:
    assert economic_trade_qualified({
        "venue_id": "coinbase",
        "source_side_basis": "maker",
    })


def test_unqualified_volume_invalidates_flow_and_rolling_volume() -> None:
    bars = tuple(_bar(index, qualified=index != 3) for index in range(8))
    features = compute_features(bars, (2,), volume_lookback=3)
    assert features[3].flow_imbalance is None
    assert features[3].volume_score is None
    assert features[4].volume_score is None
    assert features[5].volume_score is None
    assert features[6].volume_score is not None
    assert features[3].volume_qualified is False


def test_research_and_latest_volume_qualification_are_distinct_and_attested(
    tmp_path: Path,
) -> None:
    bars = tuple(_bar(index, qualified=index != 0) for index in range(5))
    features = compute_features(bars, (2,), volume_lookback=2)
    panel = PanelSnapshot(
        market={"market_id": "gmo-market"}, bars=bars,
        head_generation="head", attempt_ids=("attempt",),
        artifact_ids=("artifact",), normalization_versions=("v4",),
        panel_path=tmp_path / "panel.parquet", panel_sha256="panel",
        decision_time=bars[-1].decision_time,
        latest_available_time=bars[-1].latest_available_time,
    )
    correct = {"panel": {
        "research_economic_volume_qualified": False,
        "latest_economic_volume_qualified": True,
    }}
    _attest_panel_volume_qualification(panel, features, 4, correct)
    tampered = {"panel": {
        "research_economic_volume_qualified": True,
        "latest_economic_volume_qualified": True,
    }}
    with pytest.raises(ValueError, match="经济成交资格摘要"):
        _attest_panel_volume_qualification(panel, features, 4, tampered)


def test_gmo_mixed_r0_r1_panel_counts_only_verified_economic_trade(
    tmp_path: Path,
) -> None:
    source = tmp_path / "gmo-mixed.parquet"
    db = duckdb.connect()
    try:
        db.execute("""
            CREATE TABLE source(
              observation_id VARCHAR,event_time TIMESTAMPTZ,
              available_time TIMESTAMPTZ,ingest_time TIMESTAMPTZ,
              side VARCHAR,source_side_basis VARCHAR,price VARCHAR,size VARCHAR,
              source_artifact_id VARCHAR,source_row_index BIGINT,
              market_id VARCHAR,venue_id VARCHAR,normalization_version VARCHAR,
              raw_schema_version INTEGER,endpoint_id VARCHAR,
              endpoint_revision INTEGER
            )
        """)
        start = datetime(2026, 1, 1, tzinfo=UTC)
        common = (start, start, start, "100", "gmo-market", "gmo")
        db.executemany(
            "INSERT INTO source VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                (
                    "r0", *common[:3], "buy", "participant_side_unfiltered",
                    common[3], "2", "raw", 0, common[4], common[5],
                    "trade-realtime-normalization-v4", 3, "EP-0007", 0,
                ),
                (
                    "r1", *common[:3], "sell", "taker", common[3], "1",
                    "raw", 1, common[4], common[5],
                    "trade-realtime-normalization-v4", 3, "EP-0007", 1,
                ),
            ),
        )
        db.execute("COPY source TO ? (FORMAT PARQUET)", (str(source),))
    finally:
        db.close()
    inputs = FrozenPanelInputs(
        market={
            "market_id": "gmo-market", "venue_id": "gmo",
            "mapping_revision": 0, "tick_size": "1", "size_step": "0.1",
        },
        paths=(source,), head_generation="head", attempt_ids=("attempt",),
        artifact_ids=("artifact",),
        normalization_versions=("trade-realtime-normalization-v4",),
        maximum_event_time=start + timedelta(hours=1),
        partitions=(FrozenPanelPartition(
            source, 2, start, start,
            "trade_realtime", "trade-realtime-normalization-v4",
        ),),
    )
    panel_path, _digest = compact_trade_panel(
        inputs, tmp_path / "panel", "1hour", start,
        start + timedelta(hours=1), 100_000_000,
    )
    bar = load_panel_bars(panel_path)[0]
    assert bar.source_trade_count == 2
    assert bar.trade_count == 1
    assert bar.unqualified_trade_count == 1
    assert bar.base_volume == 1.0
    assert bar.quote_volume == 100.0
    assert bar.signed_base_volume == -1.0
    assert bar.volume_qualified is False
    with pytest.raises(ValueError, match="缺少逐文件控制合同"):
        compact_trade_panel(
            replace(inputs, partitions=()), tmp_path / "panel-missing-contract",
            "1hour", start, start + timedelta(hours=1), 100_000_000,
        )


def test_gmo_physical_lineage_cannot_cross_control_contracts(
    tmp_path: Path,
) -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    paths = (tmp_path / "archive-as-realtime.parquet",
             tmp_path / "realtime-as-archive.parquet")
    db = duckdb.connect()
    try:
        db.execute("""
            CREATE TABLE source(
              observation_id VARCHAR,event_time TIMESTAMPTZ,
              available_time TIMESTAMPTZ,ingest_time TIMESTAMPTZ,
              side VARCHAR,source_side_basis VARCHAR,price VARCHAR,size VARCHAR,
              source_artifact_id VARCHAR,source_row_index BIGINT,
              market_id VARCHAR,venue_id VARCHAR,normalization_version VARCHAR,
              raw_schema_version INTEGER,endpoint_id VARCHAR,
              endpoint_revision INTEGER
            )
        """)
        rows = (
            (
                "archive", start, start, start, "buy", "taker", "100", "1",
                "raw", 0, "gmo-market", "gmo", "trade-normalization-v1",
                None, None, None,
            ),
            (
                "realtime", start, start, start, "sell", "taker", "100", "1",
                "raw", 1, "gmo-market", "gmo",
                "trade-realtime-normalization-v4", 3, "EP-0007", 1,
            ),
        )
        for path, row in zip(paths, rows, strict=True):
            db.execute("DELETE FROM source")
            db.execute(
                "INSERT INTO source VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                row,
            )
            db.execute("COPY source TO ? (FORMAT PARQUET)", (str(path),))
    finally:
        db.close()
    inputs = FrozenPanelInputs(
        market={
            "market_id": "gmo-market", "venue_id": "gmo",
            "mapping_revision": 0, "tick_size": "1", "size_step": "0.1",
        },
        paths=paths, head_generation="head", attempt_ids=("a", "b"),
        artifact_ids=("x", "y"),
        normalization_versions=(
            "trade-normalization-v1", "trade-realtime-normalization-v4",
        ),
        maximum_event_time=start + timedelta(hours=1),
        partitions=(
            FrozenPanelPartition(
                paths[0], 1, start, start,
                "trade_realtime", "trade-realtime-normalization-v4",
            ),
            FrozenPanelPartition(
                paths[1], 1, start, start,
                "trade", "trade-normalization-v1",
            ),
        ),
    )
    panel_path, _digest = compact_trade_panel(
        inputs, tmp_path / "panel-cross", "1hour", start,
        start + timedelta(hours=1), 100_000_000,
    )
    bar = load_panel_bars(panel_path)[0]
    assert bar.source_trade_count == 2
    assert bar.trade_count == 0
    assert bar.unqualified_trade_count == 2
    assert bar.base_volume == 0.0
    assert bar.volume_qualified is False


def test_receipt_qualification_binds_physical_market_and_venue(
    tmp_path: Path,
) -> None:
    source = tmp_path / "identity-mix.parquet"
    db = duckdb.connect()
    try:
        db.execute("""
            CREATE TABLE source(
              market_id VARCHAR,venue_id VARCHAR,
              normalization_version VARCHAR,source_side_basis VARCHAR,
              raw_schema_version INTEGER,endpoint_id VARCHAR,
              endpoint_revision INTEGER
            )
        """)
        db.executemany(
            "INSERT INTO source VALUES (?,?,?,?,?,?,?)",
            (
                ("gmo-market", "gmo", "trade-realtime-normalization-v4",
                 "taker", 3, "EP-0007", 1),
                ("other-market", "gmo", "trade-realtime-normalization-v4",
                 "taker", 3, "EP-0007", 1),
                ("gmo-market", "other", "trade-realtime-normalization-v4",
                 "taker", 3, "EP-0007", 1),
            ),
        )
        db.execute("COPY source TO ? (FORMAT PARQUET)", (str(source),))
    finally:
        db.close()
    summary = _trade_qualification_summaries(
        (source,), "gmo-market", "gmo",
        {source.resolve(): (
            "trade_realtime", "trade-realtime-normalization-v4",
        )},
    )[source.resolve()]
    assert summary == (3, 1, 2, False)


def test_flow_candidate_flattens_when_economic_volume_is_unqualified() -> None:
    bars = tuple(_bar(index, qualified=index < 3) for index in range(4))
    features = compute_features(bars, (2,), volume_lookback=2)
    template = strategy_expression("flow_trend")
    parameters: dict[str, int | float] = {
        "lookback": 2,
        "entry_score": -10.0,
        "flow_confirmation": -1.0,
        "minimum_volume_score": -10.0,
        "exit_score": -20.0,
        "annual_volatility_target": 0.4,
        "maximum_target": 1.0,
    }
    candidate = CandidateSpec(
        candidate_id=candidate_identity(template, parameters),
        family="flow_trend", mode="paper", parameters=parameters,
        complexity=len(parameters), expression_id=expression_id(template),
    )
    targets = generate_targets(candidate, bars, features, periods_per_year=8760)
    assert targets[-1] == 0.0
