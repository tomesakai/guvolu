"""策略研究管线的 PIT、成本与门禁测试。"""
from __future__ import annotations

import hashlib
import json
import math
import subprocess
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pytest

import guvolu.research.allocator as allocator_module
import guvolu.research.tuning as tuning_module
from guvolu.data import store
from guvolu.data.materialize import ensure_markets
from guvolu.research.allocator import _covariance, allocate
from guvolu.research.artifact_contracts import (
    cost_replay_body,
    family_payload,
    market_state_payload,
    position_contract_payload,
    trial_ledger_body,
)
from guvolu.research import provenance
from guvolu.research.contracts import (
    AllocationResult,
    CodeIdentity,
    FamilyEvaluation,
    FrozenPanelInputs,
    FrozenPanelPartition,
    PanelSnapshot,
    PerformanceMetrics,
    QualityVector,
    RegimeAttribution,
    TrialRecord,
)
from guvolu.research.data_location import (
    data_root_locator,
    resolve_data_root_locator,
)
from guvolu.research.config_lineage import (
    attest_config_lineage_snapshot,
    snapshot_verified_config_lineage,
    verify_config_lineage,
)
from guvolu.research.features import MarketState, classify_market_state, compute_features
from guvolu.research.governance import (
    get_active_head_receipt,
    register_active_head_receipt,
)
from guvolu.research.evolution import monitor_family_run
from guvolu.research.panel import (
    _panel_path_groups,
    attest_trade_input_receipt,
    capture_trade_input_receipt,
    compact_trade_panel,
    load_panel_bars,
    registered_trade_inputs,
)
from guvolu.research.pipeline import (
    _attest_stable_code_identity,
    _research_output_paths,
)
from guvolu.research.provenance import canonical_json, stable_identifier
from guvolu.research.quality import (
    gate_economic_trade_volume,
    gate_feature_snapshot,
    panel_quality,
)
from guvolu.research.validation import (
    BLOCK_BOOTSTRAP_METHOD_VERSION,
    LEGACY_BLOCK_BOOTSTRAP_METHOD_VERSION,
    REGIME_ATTRIBUTION_METHOD_VERSION,
    ValidationResult,
    WalkForwardFold,
    _circular_block_bootstrap_sharpe,
    _deflated_sharpe_probability,
    _effective_trial_count,
    make_folds,
    _parameter_neighbors,
    _probabilistic_sharpe_p_value,
    _probability_backtest_overfitting,
    _studentized_circular_block_bootstrap_sharpe,
    _stitched_oos_regime_attribution,
    strategy_returns,
)
from guvolu.research.verification import (
    _gate_operational_quality_for_pipeline,
    _verify_operational_gate,
    _verify_run_identity,
    verify_research_run,
)
from guvolu.research.tuning import (
    propose_family_evolution,
    verify_evolution_config,
)
from guvolu.strategy.baselines import build_candidates, generate_targets
from guvolu.strategy.contracts import CandidateSpec, FeatureRow, ResearchBar
from guvolu.strategy.generation import build_family_batches
from guvolu.venues import registry


def _time(hour: int, minute: int = 0) -> datetime:
    """生成固定 UTC 测试时间。"""
    return datetime(2026, 1, 1, hour, minute, tzinfo=UTC)


def _bar(hour: int, close: float) -> ResearchBar:
    """生成一根小时测试柱。"""
    return ResearchBar(
        open_time=_time(hour),
        decision_time=_time(hour + 1),
        latest_available_time=_time(hour, 59),
        open=close,
        high=close,
        low=close,
        close=close,
        base_volume=1.0,
        quote_volume=close,
        signed_base_volume=1.0,
        trade_count=1,
    )


def _metrics() -> PerformanceMetrics:
    """生成分配器使用的正收益指标。"""
    return PerformanceMetrics(
        bars=10_000,
        net_return=0.2,
        annual_return=0.1,
        annual_volatility=0.2,
        sharpe=0.5,
        maximum_drawdown=0.1,
        turnover=2.0,
        annual_turnover=1.0,
        hit_rate=0.51,
        exposure=0.2,
        cost=0.01,
        p_value=0.02,
        capacity_score=1.0,
    )


def _regime_feature(
    hour: int,
    trend: float | None,
    *,
    jump: float | None = 0.0,
    contiguous: bool = True,
) -> FeatureRow:
    """生成只含预决策状态所需字段的测试特征。"""
    return FeatureRow(
        decision_time=_time(hour),
        as_of=_time(hour),
        return_one=0.0,
        trend_scores={3: trend},
        volatility={3: 0.01 if trend is not None else None},
        price_scores={3: 0.0},
        prior_highs={3: 1.0},
        prior_lows={3: 1.0},
        flow_imbalance=0.0,
        volume_score=0.0,
        jump_score=jump,
        contiguous=contiguous,
    )


@pytest.mark.parametrize("step_bars", [5, 15])
def test_stitched_walk_forward_requires_contiguous_test_windows(
    step_bars: int,
) -> None:
    """共享拼接目标不得静默覆盖重叠窗或遗漏间隔窗。"""
    with pytest.raises(ValueError, match="step_bars 与 test_bars 相等"):
        make_folds(100, {
            "minimum_train_bars": 20,
            "test_bars": 10,
            "step_bars": step_bars,
            "embargo_bars": 2,
        })


def test_compact_panel_enforces_pit_and_integer_projection(tmp_path: Path) -> None:
    """迟到成交不得进入当期柱，整数投影须与 Decimal 一致。"""
    source = tmp_path / "source.parquet"
    db = duckdb.connect()
    try:
        db.execute("""
            CREATE TABLE source(
              observation_id VARCHAR,event_time TIMESTAMPTZ,
              available_time TIMESTAMPTZ,ingest_time TIMESTAMPTZ,
              side VARCHAR,source_side_basis VARCHAR,price VARCHAR,size VARCHAR,
              source_artifact_id VARCHAR,source_row_index BIGINT,
              market_id VARCHAR,venue_id VARCHAR
            )
        """)
        rows = [
            ("a", _time(0, 10), _time(0, 10), _time(0, 11), "buy", "taker", "100", "1", "x", 0, "m", "bitbank"),
            ("a", _time(0, 10), _time(0, 10), _time(0, 12), "buy", "taker", "100", "1", "y", 1, "m", "bitbank"),
            ("b", _time(0, 20), _time(1, 5), _time(1, 6), "buy", "taker", "120", "1", "x", 2, "m", "bitbank"),
            ("c", _time(1, 10), _time(1, 10), _time(1, 11), "sell", "taker", "110", "2", "x", 3, "m", "bitbank"),
            ("d", _time(1, 20), _time(1, 20), _time(1, 21), "buy", "participant_side_unfiltered", "110", "3", "x", 4, "m", "bitbank"),
        ]
        db.executemany("INSERT INTO source VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", rows)
        escaped = str(source.resolve()).replace("'", "''")
        db.execute(f"COPY source TO '{escaped}' (FORMAT PARQUET)")
    finally:
        db.close()
    inputs = FrozenPanelInputs(
        market={
                "market_id": "m",
                "venue_id": "bitbank",
            "mapping_revision": 0,
            "tick_size": "1",
            "size_step": "0.1",
        },
        paths=(source,),
        head_generation="sha256-" + "1" * 64,
        attempt_ids=("attempt",),
        artifact_ids=("artifact",),
        normalization_versions=("v1",),
        maximum_event_time=_time(2),
    )
    panel, _digest = compact_trade_panel(
        inputs,
        tmp_path / "output",
        "1hour",
        _time(0),
        _time(2),
        100_000_000,
    )
    bars = load_panel_bars(panel)
    assert len(bars) == 2
    assert bars[0].close == 100.0
    assert bars[0].trade_count == 1
    assert bars[1].base_volume == 2.0
    assert bars[1].signed_base_volume == -2.0
    assert bars[1].trade_count == 1
    assert bars[1].source_trade_count == 2
    assert bars[1].unqualified_trade_count == 1
    assert bars[1].volume_qualified is False
    check = duckdb.connect()
    try:
        row = check.execute(
            "SELECT close_ticks,base_volume_lots,notional_atoms "
            "FROM read_parquet(?) ORDER BY open_time LIMIT 1",
            (str(panel),),
        ).fetchone()
    finally:
        check.close()
    assert row == (100, 10, 10_000_000_000)


def test_compact_panel_deduplicates_only_overlapping_partition_groups(
    tmp_path: Path,
) -> None:
    """不相交分区独立聚合，同小时片段合并且重叠组仍全局去重。"""
    columns = (
        "observation_id VARCHAR,event_time TIMESTAMPTZ,"
        "available_time TIMESTAMPTZ,ingest_time TIMESTAMPTZ,"
        "side VARCHAR,source_side_basis VARCHAR,price VARCHAR,size VARCHAR,"
        "source_artifact_id VARCHAR,source_row_index BIGINT,market_id VARCHAR,"
        "venue_id VARCHAR"
    )

    def write_source(name: str, rows: list[tuple[object, ...]]) -> Path:
        path = tmp_path / f"{name}.parquet"
        database = duckdb.connect()
        try:
            database.execute(f"CREATE TABLE source({columns})")
            database.executemany(
                "INSERT INTO source VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", rows,
            )
            escaped = str(path.resolve()).replace("'", "''")
            database.execute(f"COPY source TO '{escaped}' (FORMAT PARQUET)")
        finally:
            database.close()
        return path

    first = write_source("first", [
        ("a", _time(0, 10), _time(0, 10), _time(0, 11),
         "buy", "taker", "100", "1", "x", 0, "m", "bitbank"),
    ])
    second = write_source("second", [
        ("b", _time(0, 20), _time(0, 20), _time(0, 21),
         "buy", "taker", "110", "1", "y", 0, "m", "bitbank"),
    ])
    overlap = write_source("overlap", [
        ("b", _time(0, 20), _time(0, 20), _time(0, 22),
         "buy", "taker", "110", "1", "z", 0, "m", "bitbank"),
        ("c", _time(0, 30), _time(0, 30), _time(0, 31),
         "sell", "taker", "120", "2", "z", 1, "m", "bitbank"),
    ])
    inputs = FrozenPanelInputs(
        market={
                "market_id": "m", "venue_id": "bitbank", "mapping_revision": 0,
            "tick_size": "1", "size_step": "0.1",
        },
        paths=(first, second, overlap),
        head_generation="sha256-" + "2" * 64,
        attempt_ids=("a", "b", "c"),
        artifact_ids=("a", "b", "c"),
        normalization_versions=("v1",),
        maximum_event_time=_time(1),
        partitions=(
            FrozenPanelPartition(first, 1, _time(0, 10), _time(0, 10)),
            FrozenPanelPartition(second, 1, _time(0, 20), _time(0, 20)),
            FrozenPanelPartition(overlap, 2, _time(0, 20), _time(0, 30)),
        ),
    )
    assert _panel_path_groups(inputs, _time(0), _time(1)) == (
        (first.resolve(),), (second.resolve(), overlap.resolve()),
    )
    panel, _digest = compact_trade_panel(
        inputs, tmp_path / "output-grouped", "1hour",
        _time(0), _time(1), 100_000_000,
    )
    bars = load_panel_bars(panel)
    assert len(bars) == 1
    assert bars[0].open == 100.0
    assert bars[0].close == 120.0
    assert bars[0].base_volume == 4.0
    assert bars[0].trade_count == 3
    check = duckdb.connect()
    try:
        schema = {
            str(row[0]): str(row[1])
            for row in check.execute(
                "DESCRIBE SELECT * FROM read_parquet(?)", (str(panel),),
            ).fetchall()
        }
    finally:
        check.close()
    assert schema["quote_volume"] == "DECIMAL(38,24)"
    assert schema["trade_count"] == "BIGINT"


def test_registered_trade_inputs_rebuilds_control_plane_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """历史 panel 输入必须由控制面和物理文件共同证明。"""
    data_root = tmp_path / "data"
    source = data_root / "materialized" / "trade.parquet"
    source.parent.mkdir(parents=True)
    database = duckdb.connect()
    try:
        database.execute("""
            CREATE TABLE source(
              observation_id VARCHAR,event_time TIMESTAMPTZ,
              available_time TIMESTAMPTZ,ingest_time TIMESTAMPTZ,
              side VARCHAR,source_side_basis VARCHAR,price VARCHAR,size VARCHAR,
              source_artifact_id VARCHAR,source_row_index BIGINT,
              market_id VARCHAR
            )
        """)
        database.execute(
            "INSERT INTO source VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                "observation-one", _time(0, 10), _time(0, 10), _time(0, 11),
                "buy", "taker", "100", "1", "source-one", 0,
                "mkt__gmo__btc__r0",
            ),
        )
        escaped = str(source.resolve()).replace("'", "''")
        database.execute(f"COPY source TO '{escaped}' (FORMAT PARQUET)")
    finally:
        database.close()
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    artifact_id = f"sha256-{digest}"
    attempt_id = "attempt-research-input-one"
    low = "2026-01-01T00:00:00+00:00"
    high = "2026-01-01T01:00:00+00:00"
    connection = store.connect(data_root)
    try:
        registry.register_all(connection)
        ensure_markets(connection)
        connection.execute(
            "INSERT INTO partition_attempt "
            "(attempt_id,market_id,domain,partition_key,normalization_version,"
            "input_set_hash,status,source_rows,normalized_rows,ignored_rows,"
            "rejected_rows,started_at,finished_at,code_version,config_hash) "
            "VALUES (?,?,?,?,?,?,'complete',1,1,0,0,?,?,?,?)",
            (
                attempt_id, "mkt__gmo__btc__r0", "trade", "2026-01-01",
                "trade-test-v1", "input-one", low, high, "test", "config",
            ),
        )
        relative = source.relative_to(data_root).as_posix()
        connection.execute(
            "INSERT INTO artifact VALUES "
            "(?,'materialized_parquet',?,?,?,?,?,'sha256-file-v1',1)",
            (
                artifact_id, relative, digest, source.stat().st_size,
                high, high,
            ),
        )
        connection.execute(
            "INSERT INTO artifact_location VALUES (?,?,?,1)",
            (artifact_id, relative, high),
        )
        connection.execute(
            "INSERT INTO materialization_output VALUES (?,?,?,?,?,?,?)",
            (
                attempt_id, artifact_id, "trade_observation", 1,
                low, high, high,
            ),
        )
        connection.execute(
            "INSERT INTO materialization_partition_head VALUES (?,?,?,?,?,?)",
            (
                "mkt__gmo__btc__r0", "trade", "2026-01-01",
                "trade-test-v1", attempt_id, high,
            ),
        )
        connection.commit()
    finally:
        connection.close()
    rebuilt = registered_trade_inputs(
        data_root, "mkt__gmo__btc__r0", (artifact_id,), (attempt_id,),
    )
    assert rebuilt.paths == (source.resolve(),)
    assert rebuilt.artifact_ids == (artifact_id,)
    assert rebuilt.attempt_ids == (attempt_id,)
    assert rebuilt.normalization_versions == ("trade-test-v1",)
    assert rebuilt.maximum_event_time == _time(1)
    assert len(rebuilt.partitions) == 1
    assert rebuilt.partitions[0] == FrozenPanelPartition(
        source.resolve(), 1, _time(0), _time(1),
        "trade", "trade-test-v1",
    )
    assert rebuilt.head_generation.startswith("sha256-")

    captured = capture_trade_input_receipt(
        data_root,
        "mkt__gmo__btc__r0",
        tmp_path / "receipts",
    )
    assert captured.receipt_path is not None
    assert captured.receipt_sha256 is not None
    attested = attest_trade_input_receipt(
        data_root, captured.receipt_path, require_current_head=True,
    )
    assert attested.head_generation == captured.head_generation
    assert attested.partitions == rebuilt.partitions
    receipt_payload = json.loads(
        captured.receipt_path.read_text(encoding="utf-8")
    )
    legacy_payload = json.loads(json.dumps(receipt_payload))
    legacy_payload["method_version"] = "active-trade-head-receipt-v1"
    for field in (
        "trade_flow_input_method_version", "source_trade_rows",
        "economic_trade_rows", "unqualified_trade_rows", "volume_qualified",
    ):
        legacy_payload.pop(field, None)
        for entry in legacy_payload["entries"]:
            entry.pop(field, None)
    legacy_receipt = tmp_path / "legacy-trade-input-receipt.json"
    legacy_receipt.write_text(
        canonical_json(legacy_payload) + "\n", encoding="utf-8",
    )
    legacy_attested = attest_trade_input_receipt(
        data_root, legacy_receipt, require_current_head=False,
    )
    assert legacy_attested.paths == captured.paths
    assert legacy_attested.trade_flow_input_method_version is None
    assert legacy_attested.volume_qualified is False
    with pytest.raises(ValueError, match="不能证明当前 head"):
        attest_trade_input_receipt(
            data_root, legacy_receipt, require_current_head=True,
        )
    receipt_payload["entries"][0]["domain"] = "trade_realtime"
    receipt_payload["entries"][0]["partition_key"] = "forged-partition"
    receipt_payload["entries"][0]["normalization_version"] = "forged-v1"
    receipt_payload["normalization_versions"] = ["forged-v1"]
    forged_receipt = tmp_path / "forged-trade-input-receipt.json"
    forged_receipt.write_text(
        canonical_json(receipt_payload) + "\n", encoding="utf-8",
    )
    with pytest.raises(ValueError, match="控制面字段不匹配"):
        attest_trade_input_receipt(
            data_root, forged_receipt, require_current_head=False,
        )
    monkeypatch.setattr(
        "guvolu.research.governance.clock.utc_now", lambda: _time(1),
    )
    registration = register_active_head_receipt(
        tmp_path / "governance.sqlite3",
        "research",
        "research-identity-one",
        "mkt__gmo__btc__r0",
        captured.head_generation,
        captured.receipt_path.relative_to(tmp_path).as_posix(),
        captured.receipt_sha256,
        repository_root=tmp_path,
        data_root=data_root,
    )
    assert get_active_head_receipt(
        tmp_path / "governance.sqlite3",
        "research",
        "research-identity-one",
    ) == registration
    monkeypatch.setattr(
        "guvolu.research.governance.clock.utc_now", lambda: _time(2),
    )
    assert register_active_head_receipt(
        tmp_path / "governance.sqlite3",
        "research",
        "research-identity-one",
        "mkt__gmo__btc__r0",
        captured.head_generation,
        captured.receipt_path.relative_to(tmp_path).as_posix(),
        captured.receipt_sha256,
        repository_root=tmp_path,
        data_root=data_root,
    ) == registration
    connection = store.connect(data_root)
    try:
        connection.execute(
            "UPDATE materialization_partition_head "
            "SET normalization_version='trade-test-v2' "
            "WHERE market_id='mkt__gmo__btc__r0' AND domain='trade'",
        )
        connection.commit()
    finally:
        connection.close()
    historical = attest_trade_input_receipt(
        data_root, captured.receipt_path, require_current_head=False,
    )
    assert historical.receipt_sha256 == captured.receipt_sha256
    with pytest.raises(ValueError, match="不是登记时的完整当前 head"):
        attest_trade_input_receipt(
            data_root, captured.receipt_path, require_current_head=True,
        )

    source.write_bytes(source.read_bytes() + b"tampered")
    with pytest.raises(ValueError, match="字节数与控制面不一致"):
        registered_trade_inputs(
            data_root, "mkt__gmo__btc__r0", (artifact_id,), (attempt_id,),
        )


def test_feature_windows_reject_large_gap_and_allow_structural_gap() -> None:
    """结构性空窗上限须显式控制特征有效性。"""
    bars = [_bar(0, 100), _bar(1, 101), _bar(3, 102)]
    strict = compute_features(bars, (2,), 2, 1)
    bounded = compute_features(bars, (2,), 2, 2)
    assert strict[-1].contiguous is False
    assert bounded[-1].contiguous is True
    assert all(row.as_of <= row.decision_time for row in bounded)


def test_market_state_annualization_uses_configured_bar_periods() -> None:
    """市场状态的同一每柱波动率必须按实际节拍年化。"""
    _bar_value, feature = _bar(0, 100), FeatureRow(
        decision_time=_time(1),
        as_of=_time(1),
        return_one=0.0,
        trend_scores={2: 0.0},
        volatility={2: 0.01},
        price_scores={2: 0.0},
        prior_highs={2: 100.0},
        prior_lows={2: 100.0},
        flow_imbalance=0.0,
        volume_score=0.0,
        jump_score=0.0,
        contiguous=True,
    )
    hourly = classify_market_state(feature, 2, 0.0, 365.0 * 24.0)
    four_hour = classify_market_state(feature, 2, 0.0, 365.0 * 6.0)
    assert hourly.volatility == pytest.approx(four_hour.volatility * 2.0)


def test_strategy_return_uses_prior_decision_and_cost() -> None:
    """下一期收益只能使用前一决策目标并扣换手成本。"""
    bars = (_bar(0, 100), _bar(1, 110))
    features = tuple(FeatureRow(
        decision_time=bar.decision_time,
        as_of=bar.latest_available_time,
        return_one=0.0,
        trend_scores={2: 2.0},
        volatility={2: 0.01},
        price_scores={2: 1.0},
        prior_highs={2: 99.0},
        prior_lows={2: 90.0},
        flow_imbalance=1.0,
        volume_score=0.0,
        jump_score=0.0,
        contiguous=True,
    ) for bar in bars)
    candidate = CandidateSpec(
        candidate_id="candidate",
        family="trend",
        mode="paper",
        parameters={
            "lookback": 2,
            "entry_score": 1.0,
            "exit_score": 0.0,
            "annual_volatility_target": 0.4,
            "maximum_target": 1.0,
        },
        complexity=5,
    )
    targets = generate_targets(
        candidate, bars, features, periods_per_year=365.0 * 24.0,
    )
    returns = strategy_returns(bars, targets, 0.001)
    assert targets[0] > 0
    assert returns[1] == pytest.approx(
        targets[0] * math.log(1.1) - targets[0] * 0.001,
    )


def test_covariance_rejects_misaligned_oos_series() -> None:
    """组合器不得把未对齐收益静默当成零相关。"""
    with pytest.raises(ValueError, match="长度不一致"):
        _covariance((0.1, 0.2), (0.1,))


def test_nonnormal_sharpe_and_pbo_diagnostics_are_deterministic() -> None:
    """非正态 Sharpe 概率与折块 PBO 必须可复现。"""
    assert _probabilistic_sharpe_p_value((0.01, 0.01, 0.01)) == 0.0
    assert _probabilistic_sharpe_p_value((-0.01, -0.01, -0.01)) == 1.0
    scores = {
        "a": (1.0, 1.0, -1.0, -1.0),
        "b": (-1.0, -1.0, 1.0, 1.0),
    }
    first = _probability_backtest_overfitting(scores, 10, 7)
    second = _probability_backtest_overfitting(scores, 10, 7)
    assert first == second
    assert first == (1.0 / 3.0, 0.5, 3)
    odd = _probability_backtest_overfitting(
        {"a": (9.0, 1.0, 1.0, -1.0, -1.0),
         "b": (9.0, -1.0, -1.0, 1.0, 1.0)},
        10,
        7,
    )
    recent_even = _probability_backtest_overfitting(scores, 10, 7)
    assert odd == recent_even
    tied = _probability_backtest_overfitting(
        {"a": (1.0,) * 4, "b": (1.0,) * 4}, 10, 7
    )
    assert tied == (0.0, 0.5, 3)
    identity_first = _probability_backtest_overfitting({
        "a": (-1.0, -1.0, -1.0, -1.0),
        "b": (-1.0, -1.0, -1.0, 0.0),
    }, 10, 7)
    identity_renamed = _probability_backtest_overfitting({
        "z": (-1.0, -1.0, -1.0, -1.0),
        "b": (-1.0, -1.0, -1.0, 0.0),
    }, 10, 7)
    assert identity_first == identity_renamed


def test_circular_block_bootstrap_sharpe_is_deterministic() -> None:
    """循环折块 Sharpe 下界必须保留依赖结构且可复现。"""
    values = tuple((0.002 if index % 7 else -0.003) for index in range(140))
    first = _circular_block_bootstrap_sharpe(values, 14, 128, 0.05, 19)
    second = _circular_block_bootstrap_sharpe(values, 14, 128, 0.05, 19)
    assert first == second
    assert first[0] > 0.0
    assert 0.0 < first[1] <= 1.0
    assert first[2] == 128
    with pytest.raises(ValueError, match="折块长度"):
        _circular_block_bootstrap_sharpe(values, 141, 10, 0.05, 19)


def test_studentized_block_bootstrap_is_deterministic_and_one_sided() -> None:
    """bootstrap-t 必须逐样本重估误差并保守处理非正收益。"""
    positive = tuple(
        0.001 + (index % 7 - 3) * 0.0001 for index in range(280)
    )
    first = _studentized_circular_block_bootstrap_sharpe(
        positive, 14, 128, 0.05, 19,
    )
    second = _studentized_circular_block_bootstrap_sharpe(
        positive, 14, 128, 0.05, 19,
    )
    assert first == second
    assert first[0] > 0.0
    assert first[1] == pytest.approx(1.0 / 129.0)
    negative = tuple(-value for value in positive)
    lower, p_value, count = _studentized_circular_block_bootstrap_sharpe(
        negative, 14, 128, 0.05, 19,
    )
    assert lower < 0.0
    assert p_value == 1.0
    assert count == 128
    assert _studentized_circular_block_bootstrap_sharpe(
        (0.0,) * 280, 14, 128, 0.05, 19,
    ) == (0.0, 1.0, 128)
    with pytest.raises(ValueError, match="折块长度"):
        _studentized_circular_block_bootstrap_sharpe(
            positive, 141, 10, 0.05, 19,
        )


def test_regime_attribution_uses_predecision_state_and_partitions_oos() -> None:
    """状态桶须使用区间前特征，且贡献严格加总回 stitched OOS。"""
    features = (
        _regime_feature(0, 1.0),
        _regime_feature(1, 0.0),
        _regime_feature(2, -1.0),
        _regime_feature(3, 1.0, jump=5.0),
        _regime_feature(4, 0.6),
        _regime_feature(5, 1.0, contiguous=False),
        _regime_feature(6, -1.0),
    )
    targets = (1.0, 0.5, -0.5, 0.25, -0.25, 0.75, 0.0)
    returns = (0.0, 0.01, -0.02, 0.03, -0.04, 0.05, -0.06)
    attribution = _stitched_oos_regime_attribution(
        features,
        targets,
        returns,
        (False, True, True, True, True, True, True),
        3,
        365.0 * 24.0,
    )
    assert [item.regime for item in attribution] == [
        "jump_risk",
        "positive_trend",
        "negative_trend",
        "range",
        "mixed",
        "unavailable",
    ]
    assert [item.bars for item in attribution] == [1, 1, 1, 1, 1, 1]
    by_regime = {item.regime: item for item in attribution}
    assert by_regime["positive_trend"].net_log_return == pytest.approx(0.01)
    assert by_regime["positive_trend"].mean_absolute_target == 1.0
    assert by_regime["unavailable"].net_log_return == pytest.approx(-0.06)
    assert sum(item.net_log_return for item in attribution) == pytest.approx(
        sum(returns[1:])
    )
    assert sum(item.bar_share for item in attribution) == pytest.approx(1.0)


def test_trial_ledger_binds_actual_bootstrap_method_version() -> None:
    """历史 verifier 重建 v1 时不得被当前 v2 常量改写 header。"""
    legacy = ValidationResult(
        families=(),
        trials=(),
        candidate_targets={},
        folds=(),
        block_bootstrap_method_version=LEGACY_BLOCK_BOOTSTRAP_METHOD_VERSION,
    )
    current = replace(
        legacy,
        block_bootstrap_method_version=BLOCK_BOOTSTRAP_METHOD_VERSION,
        regime_attribution_method_version=REGIME_ATTRIBUTION_METHOD_VERSION,
    )
    legacy_header = json.loads(trial_ledger_body(legacy, "research").splitlines()[0])
    current_header = json.loads(trial_ledger_body(current, "research").splitlines()[0])
    assert legacy_header["block_bootstrap_method_version"] == (
        LEGACY_BLOCK_BOOTSTRAP_METHOD_VERSION
    )
    assert current_header["block_bootstrap_method_version"] == (
        BLOCK_BOOTSTRAP_METHOD_VERSION
    )
    assert "regime_attribution_method_version" not in legacy_header
    assert current_header["regime_attribution_method_version"] == (
        REGIME_ATTRIBUTION_METHOD_VERSION
    )


def test_family_payload_omits_regime_diagnostic_for_legacy_manifest() -> None:
    """旧 manifest 重建不得凭当前代码新增状态归因字段。"""
    candidate = CandidateSpec("candidate", "trend", "paper", {}, 1)
    attribution = RegimeAttribution(
        regime="positive_trend",
        bars=10,
        bar_share=1.0,
        net_log_return=0.1,
        mean_return=0.01,
        period_volatility=0.02,
        annualized_sharpe=1.0,
        hit_rate=0.6,
        active_target_share=0.8,
        mean_absolute_target=0.5,
    )
    family = FamilyEvaluation(
        family="trend",
        mode="paper",
        deployment_candidate=candidate,
        latest_target=0.5,
        deployment_oos_metrics=_metrics(),
        deployment_oos_returns=(0.01,),
        metrics=_metrics(),
        adjusted_sharpe=0.5,
        fdr_q=0.1,
        eligible=True,
        rejection_reasons=(),
        oos_returns=(0.01,),
        regime_attribution=(attribution,),
    )
    legacy = ValidationResult((family,), (), {}, ())
    current = replace(
        legacy,
        regime_attribution_method_version=REGIME_ATTRIBUTION_METHOD_VERSION,
    )
    assert "regime_attribution" not in family_payload(legacy)[0]
    assert family_payload(current)[0]["regime_attribution"] == [{
        "regime": "positive_trend",
        "bars": 10,
        "bar_share": 1.0,
        "net_log_return": 0.1,
        "mean_return": 0.01,
        "period_volatility": 0.02,
        "annualized_sharpe": 1.0,
        "hit_rate": 0.6,
        "active_target_share": 0.8,
        "mean_absolute_target": 0.5,
    }]


def test_deflated_sharpe_penalizes_more_trials_and_reports_effective_count() -> None:
    """DSR 须随试验空间扩大而收紧，并显式报告相关性折算。"""
    values = tuple(0.00101 if index % 2 else -0.00099 for index in range(500))
    trial_sharpes = (-0.01, 0.0, 0.01)
    small_probability, small_benchmark = _deflated_sharpe_probability(
        values,
        trial_sharpes,
        3.0,
    )
    large_probability, large_benchmark = _deflated_sharpe_probability(
        values,
        trial_sharpes,
        100.0,
    )
    assert large_benchmark > small_benchmark > 0.0
    assert large_probability < small_probability
    _fractional_probability, fractional_benchmark = _deflated_sharpe_probability(
        values,
        trial_sharpes,
        1.25,
    )
    assert fractional_benchmark >= 0.0
    assert _effective_trial_count({
        "a": (1.0, -1.0, 1.0, -1.0),
        "b": (1.0, 1.0, -1.0, -1.0),
    }) == pytest.approx(2.0)
    assert _effective_trial_count({
        "a": (1.0, -1.0, 1.0, -1.0),
        "b": (1.0, -1.0, 1.0, -1.0),
    }) == pytest.approx(1.0)
    negative_degenerate, _benchmark = _deflated_sharpe_probability(
        (-2.0, -1.0, -1.0),
        (0.0,),
        1.0,
    )
    positive_degenerate, _benchmark = _deflated_sharpe_probability(
        (2.0, 1.0, 1.0),
        (0.0,),
        1.0,
    )
    assert negative_degenerate == 0.0
    assert positive_degenerate == 1.0


def test_parameter_neighbors_select_only_nearest_one_axis_changes() -> None:
    """参数稳定性只比较其他轴固定时最近的上下候选。"""
    selected = CandidateSpec("selected", "trend", "paper", {"x": 2, "y": 10}, 2)
    candidates = (
        selected,
        CandidateSpec("x-1", "trend", "paper", {"x": 1, "y": 10}, 2),
        CandidateSpec("x-3", "trend", "paper", {"x": 3, "y": 10}, 2),
        CandidateSpec("x-4", "trend", "paper", {"x": 4, "y": 10}, 2),
        CandidateSpec("y-20", "trend", "paper", {"x": 2, "y": 20}, 2),
        CandidateSpec("diagonal", "trend", "paper", {"x": 3, "y": 20}, 2),
    )
    assert {
        candidate.candidate_id for candidate in _parameter_neighbors(
            selected,
            candidates,
        )
    } == {"x-1", "x-3", "y-20"}


def test_large_gap_flattens_without_collecting_unobserved_return() -> None:
    """超限断流不得把跨缺口涨跌计入策略收益。"""
    first = _bar(0, 100)
    second = ResearchBar(
        open_time=_time(8),
        decision_time=_time(9),
        latest_available_time=_time(8, 59),
        open=200,
        high=200,
        low=200,
        close=200,
        base_volume=1,
        quote_volume=200,
        signed_base_volume=1,
        trade_count=1,
    )
    returns = strategy_returns(
        (first, second),
        (0.5, 0.0),
        0.001,
        maximum_gap_seconds=4 * 3600,
    )
    assert returns[1] == pytest.approx(-0.001)


def test_quality_failure_forces_zero_allocation(tmp_path: Path) -> None:
    """硬门禁失败后软分配器不得恢复仓位。"""
    bars = tuple(_bar(index, 100 + index) for index in range(3))
    panel_path = tmp_path / "panel.parquet"
    panel_path.write_bytes(b"panel")
    panel = PanelSnapshot(
        market={"market_id": "m"},
        bars=bars,
        head_generation="sha256-" + "1" * 64,
        attempt_ids=("attempt",),
        artifact_ids=("artifact",),
        normalization_versions=("v1",),
        panel_path=panel_path,
        panel_sha256="2" * 64,
        decision_time=bars[-1].decision_time,
        latest_available_time=bars[-1].latest_available_time,
    )
    quality = panel_quality(panel, bars[-1].decision_time, 3600, 3)
    stale = gate_feature_snapshot(
        quality,
        bars[0].decision_time,
        bars[-1].decision_time + timedelta(hours=10),
        3600,
    )
    candidate = CandidateSpec("candidate", "mean_reversion", "paper", {}, 1)
    family = FamilyEvaluation(
        family="mean_reversion",
        mode="paper",
        deployment_candidate=candidate,
        latest_target=0.5,
        deployment_oos_metrics=_metrics(),
        deployment_oos_returns=(0.01, 0.02),
        metrics=_metrics(),
        adjusted_sharpe=0.5,
        fdr_q=0.1,
        eligible=True,
        rejection_reasons=(),
        oos_returns=(0.01, 0.02),
    )
    state = MarketState(0.0, 0.2, 0.0, 0.0, None, None, None, 0.0, "range", 0.0)
    config = json.loads(Path("config/strategy_research.json").read_text())["allocation"]
    result = allocate((family,), state, stale, config)
    assert result.weights["mean_reversion"] == 0
    assert result.reserve == 1


def test_candidate_registry_and_identifiers_are_deterministic() -> None:
    """同配置必须展开同候选身份和确定性 JSON。"""
    config = json.loads(Path("config/strategy_research.json").read_text())
    first = build_candidates(config)
    second = build_candidates(config)
    assert first == second
    assert len(first) == 34
    value = {"b": 2, "a": 1}
    assert canonical_json(value) == '{"a":1,"b":2}'
    assert stable_identifier("x", value) == stable_identifier("x", value)


def test_execution_snapshots_share_research_artifact_directory(
    tmp_path: Path,
) -> None:
    """同一研究身份的墙钟快照不得复制大体积研究制品。"""
    first = _research_output_paths(tmp_path, "research-one", _time(1))
    second = _research_output_paths(tmp_path, "research-one", _time(2))
    assert first[0] != second[0]
    assert first[1] != second[1]
    assert first[2] == second[2]
    assert first[2] == tmp_path / "research-artifacts" / "research-one"


def test_trial_roles_and_stitched_replay_are_explicitly_bounded(
    tmp_path: Path,
) -> None:
    """台账须区分冠军角色，stitched 回放只覆盖声明的 OOS 折。"""
    bars = tuple(_bar(index, 100.0 + index) for index in range(6))
    candidate = CandidateSpec("candidate", "trend", "paper", {}, 1)
    family = FamilyEvaluation(
        family="trend",
        mode="paper",
        deployment_candidate=candidate,
        latest_target=0.5,
        deployment_oos_metrics=_metrics(),
        deployment_oos_returns=(0.01, 0.02),
        metrics=_metrics(),
        adjusted_sharpe=0.5,
        fdr_q=0.1,
        eligible=True,
        rejection_reasons=(),
        oos_returns=(0.01, 0.02),
        fold_selected_candidate_ids=(candidate.candidate_id,),
    )
    fold = WalkForwardFold("fold-001", 0, 2, 2, 4)
    trials = (
        TrialRecord(
            "fold-evaluation", candidate, fold.fold_id, "testing",
            bars[2].decision_time, bars[3].decision_time, True, _metrics(),
        ),
        TrialRecord(
            "full-evaluation", candidate, "full", "training",
            bars[1].decision_time, bars[-1].decision_time, True, _metrics(),
        ),
    )
    deployment_targets = (0.0, 0.5, 0.5, 0.5, 0.5, 0.5)
    stitched_targets = (0.0, 0.5, 0.5, 0.5, 0.0, 0.0)
    validation = ValidationResult(
        families=(family,),
        trials=trials,
        candidate_targets={candidate.candidate_id: deployment_targets},
        folds=(fold,),
        family_validation_targets={"trend": stitched_targets},
    )
    ledger_rows = [
        json.loads(line)
        for line in trial_ledger_body(validation, "research-one").splitlines()
    ]
    roles = {
        row["evaluation_id"]: row["selection_role"]
        for row in ledger_rows if row["record_type"] == "trial"
    }
    assert roles == {
        "fold-evaluation": "fold_training_champion",
        "full-evaluation": "deployment_champion",
    }

    panel_path = tmp_path / "panel.parquet"
    panel_path.write_bytes(b"panel")
    panel = PanelSnapshot(
        market={"market_id": "m"},
        bars=bars,
        head_generation="sha256-" + "1" * 64,
        attempt_ids=("attempt",),
        artifact_ids=("artifact",),
        normalization_versions=("v1",),
        panel_path=panel_path,
        panel_sha256="2" * 64,
        decision_time=bars[-1].decision_time,
        latest_available_time=bars[-1].latest_available_time,
    )
    config = json.loads(Path("config/strategy_research.json").read_text())
    replay_rows = [
        json.loads(line)
        for line in cost_replay_body(
            panel, validation, config, "research-one",
        ).splitlines()
        if json.loads(line)["record_type"] == "label_cost"
    ]
    assert [row["in_walk_forward_oos"] for row in replay_rows] == [
        False, True, True, False, False,
    ]
    assert [row["walk_forward_fold_id"] for row in replay_rows] == [
        None, "fold-001", "fold-001", None, None,
    ]
    assert all(
        (row["replays"]["walk_forward_stitched"] is not None)
        == row["in_walk_forward_oos"]
        for row in replay_rows
    )


def test_family_generators_are_independently_scoped() -> None:
    """单流派生成不得携带其他流派候选。"""
    config = json.loads(Path("config/strategy_research.json").read_text())
    batches = build_family_batches(config, ("trend",))
    assert len(batches) == 1
    assert batches[0].family == "trend"
    assert len(batches[0].candidates) == 6
    assert all(item.family == "trend" for item in batches[0].candidates)
    with pytest.raises(ValueError, match="未知策略家族"):
        build_family_batches(config, ("unknown",))


def test_true_quality_respects_family_caps() -> None:
    """状态分配不得突破均值回归和总风险上限。"""
    quality = QualityVector(True, True, True, True, True, True, ())
    candidate = CandidateSpec("candidate", "mean_reversion", "paper", {}, 1)
    family = FamilyEvaluation(
        family="mean_reversion",
        mode="paper",
        deployment_candidate=candidate,
        latest_target=0.5,
        deployment_oos_metrics=_metrics(),
        deployment_oos_returns=(0.01, 0.02, 0.01),
        metrics=_metrics(),
        adjusted_sharpe=0.5,
        fdr_q=0.1,
        eligible=True,
        rejection_reasons=(),
        oos_returns=(0.01, 0.02, 0.01),
    )
    state = MarketState(0.0, 0.2, 0.0, 0.0, None, None, None, 0.0, "range", 0.0)
    config = json.loads(Path("config/strategy_research.json").read_text())["allocation"]
    result = allocate((family,), state, quality, config)
    assert 0 <= result.weights["mean_reversion"] <= 0.25
    assert result.reserve >= 0.15


def test_allocator_uses_stitched_validation_evidence() -> None:
    """组合器收益证据必须复现逐折选择过程，不能事后挑固定冠军。"""
    quality = QualityVector(True, True, True, True, True, True, ())
    candidate = CandidateSpec("candidate", "trend", "paper", {}, 1)
    family = FamilyEvaluation(
        family="trend",
        mode="paper",
        deployment_candidate=candidate,
        latest_target=1.0,
        deployment_oos_metrics=replace(
            _metrics(),
            annual_return=-0.1,
            annual_volatility=0.0,
        ),
        deployment_oos_returns=(0.0, 0.0, 0.0),
        metrics=replace(_metrics(), annual_return=1.0),
        adjusted_sharpe=0.5,
        fdr_q=0.1,
        eligible=True,
        rejection_reasons=(),
        oos_returns=(0.1, 0.1, 0.1),
    )
    state = MarketState(
        1.0, 0.2, 0.0, 0.0, None, None, None, 0.0, "positive_trend", 0.0,
    )
    config = json.loads(Path("config/strategy_research.json").read_text())["allocation"]
    result = allocate((family,), state, quality, config)
    assert result.weights["trend"] > 0.0


def test_allocator_weight_is_independent_of_current_family_signal() -> None:
    """当前信号为零时仍估计长期资本权重，最终贡献保持为零。"""
    quality = QualityVector(True, True, True, True, True, True, ())
    candidate = CandidateSpec("candidate", "trend", "paper", {}, 1)
    family = FamilyEvaluation(
        family="trend",
        mode="paper",
        deployment_candidate=candidate,
        latest_target=0.0,
        deployment_oos_metrics=_metrics(),
        deployment_oos_returns=(0.01, 0.02, 0.01),
        metrics=replace(_metrics(), annual_return=1.0),
        adjusted_sharpe=0.5,
        fdr_q=0.1,
        eligible=True,
        rejection_reasons=(),
        oos_returns=(0.01, 0.02, 0.01),
    )
    state = MarketState(
        1.0, 0.2, 0.0, 0.0, None, None, None, 0.0, "positive_trend", 0.0,
    )
    config = json.loads(Path("config/strategy_research.json").read_text())["allocation"]
    result = allocate((family,), state, quality, config)
    contract = position_contract_payload(
        ValidationResult((family,), (), {}, ()), result,
    )
    assert result.weights["trend"] > 0.0
    assert contract["families"][0]["portfolio_target_contribution"] == 0.0
    assert contract["aggregate_target"] == 0.0


def test_flow_operational_quality_requires_current_economic_volume() -> None:
    """flow 家族不得依靠零目标掩盖当前经济成交窗口不可用。"""
    quality = QualityVector(True, True, True, True, True, True, ())
    flow = FamilyEvaluation(
        family="flow_trend",
        mode="paper",
        deployment_candidate=CandidateSpec(
            "candidate-flow", "flow_trend", "paper", {}, 1,
        ),
        latest_target=0.0,
        deployment_oos_metrics=_metrics(),
        deployment_oos_returns=(0.01, 0.02),
        metrics=_metrics(),
        adjusted_sharpe=0.5,
        fdr_q=0.1,
        eligible=True,
        rejection_reasons=(),
        oos_returns=(0.01, 0.02),
    )
    feature = replace(
        _regime_feature(1, 1.0),
        flow_imbalance=None,
        volume_score=None,
        volume_qualified=False,
    )

    gated = gate_economic_trade_volume(
        quality,
        (flow,),
        feature,
    )

    assert not gated.eligible
    assert not gated.coverage
    assert "latest_economic_trade_volume_unqualified" in gated.reasons

    # v13 保留旧质量语义。
    assert _gate_operational_quality_for_pipeline(
        "strategy-research-pipeline-v13", quality, (flow,), feature,
    ) == quality
    assert _gate_operational_quality_for_pipeline(
        "strategy-research-pipeline-v14", quality, (flow,), feature,
    ) == gated


def test_price_only_operational_quality_ignores_unqualified_volume() -> None:
    """纯价格突破不得被未使用的经济成交量误伤。"""
    quality = QualityVector(True, True, True, True, True, True, ())
    trend = FamilyEvaluation(
        family="price_breakout",
        mode="paper",
        deployment_candidate=CandidateSpec(
            "candidate-price-breakout", "price_breakout", "paper", {}, 1,
        ),
        latest_target=1.0,
        deployment_oos_metrics=_metrics(),
        deployment_oos_returns=(0.01, 0.02),
        metrics=_metrics(),
        adjusted_sharpe=0.5,
        fdr_q=0.1,
        eligible=True,
        rejection_reasons=(),
        oos_returns=(0.01, 0.02),
    )
    feature = replace(
        _regime_feature(1, 1.0),
        flow_imbalance=None,
        volume_score=None,
        volume_qualified=False,
    )

    assert gate_economic_trade_volume(
        quality,
        (trend,),
        feature,
    ) == quality


def test_research_publish_rechecks_code_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """长运行期间代码树变化必须阻止最终发布。"""
    initial = CodeIdentity(
        git_hash="a" * 40,
        tree_digest="b" * 64,
        dirty_digest="c" * 64,
        dirty=False,
        decision_grade=True,
        reason=None,
    )
    changed = replace(
        initial,
        tree_digest="d" * 64,
        dirty=True,
        decision_grade=False,
        reason="repository_dirty",
    )
    monkeypatch.setattr(
        "guvolu.research.pipeline.code_identity",
        lambda *_args: changed,
    )

    with pytest.raises(ValueError, match="代码身份发生变化"):
        _attest_stable_code_identity(tmp_path, (), initial)


def test_allocator_covariance_uses_stitched_oos_returns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """跨家族协方差必须与逐折选择后的收益证据对齐。"""
    quality = QualityVector(True, True, True, True, True, True, ())
    deployment_returns = {
        "trend": (0.01, -0.02, 0.03),
        "mean_reversion": (-0.04, 0.05, -0.06),
    }
    stitched_returns = {
        "trend": (0.7, 0.8, 0.9),
        "mean_reversion": (-0.7, -0.8, -0.9),
    }
    evaluations = tuple(FamilyEvaluation(
        family=family,
        mode="paper",
        deployment_candidate=CandidateSpec(
            "candidate-" + family, family, "paper", {}, 1,
        ),
        latest_target=1.0,
        deployment_oos_metrics=_metrics(),
        deployment_oos_returns=deployment_returns[family],
        metrics=_metrics(),
        adjusted_sharpe=0.5,
        fdr_q=0.1,
        eligible=True,
        rejection_reasons=(),
        oos_returns=stitched_returns[family],
    ) for family in deployment_returns)
    observed: list[tuple[tuple[float, ...], tuple[float, ...]]] = []

    def record_covariance(
        left: tuple[float, ...],
        right: tuple[float, ...],
    ) -> float:
        observed.append((left, right))
        return 0.0

    monkeypatch.setattr(allocator_module, "_covariance", record_covariance)
    state = MarketState(
        1.0, 0.2, 0.0, 0.0, None, None, None, 0.0, "mixed", 0.0,
    )
    config = json.loads(Path("config/strategy_research.json").read_text())["allocation"]
    allocate(evaluations, state, quality, config)

    expected = {
        (left, right)
        for left in stitched_returns.values()
        for right in stitched_returns.values()
    }
    assert len(observed) == 4
    assert set(observed) == expected
    assert not any(
        left in deployment_returns.values() or right in deployment_returns.values()
        for left, right in observed
    )


def test_directional_families_share_one_cap() -> None:
    """量价趋势不得绕过趋势与突破共享的方向风险上限。"""
    quality = QualityVector(True, True, True, True, True, True, ())
    evaluations = tuple(FamilyEvaluation(
        family=family,
        mode="paper",
        deployment_candidate=CandidateSpec("candidate-" + family, family, "paper", {}, 1),
        latest_target=1.0,
        deployment_oos_metrics=_metrics(),
        deployment_oos_returns=(0.01, 0.02, 0.01),
        metrics=_metrics(),
        adjusted_sharpe=0.5,
        fdr_q=0.1,
        eligible=True,
        rejection_reasons=(),
        oos_returns=(0.01, 0.02, 0.01),
    ) for family in ("trend", "flow_trend", "breakout"))
    state = MarketState(1.0, 0.2, 0.0, 0.5, None, None, None, 0.0, "positive_trend", 0.0)
    config = json.loads(Path("config/strategy_research.json").read_text())["allocation"]
    result = allocate(evaluations, state, quality, config)
    assert sum(result.weights.values()) <= 0.6 + 1e-12


def test_dirty_git_identity_is_not_decision_grade(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """有 HEAD 的脏工作树也不得获得决策级身份。"""
    monkeypatch.setattr(provenance, "hash_paths", lambda *_args: "tree")

    def fake_git(_root: Path, arguments: tuple[str, ...]) -> str | None:
        return " M src/example.py" if arguments[0] == "status" else "abc123"

    monkeypatch.setattr(provenance, "_git_output", fake_git)
    identity = provenance.code_identity(tmp_path, ())
    assert identity.git_hash == "abc123"
    assert identity.dirty
    assert not identity.decision_grade
    assert identity.reason == "repository_dirty"


def test_data_root_locator_separates_external_market_data_from_state(
    tmp_path: Path,
) -> None:
    """外部只读数据根可定位，研究状态仍保留在仓库内。"""
    repository = tmp_path / "repository"
    repository.mkdir()
    internal = repository / "data"
    external = tmp_path / "market-data"
    internal.mkdir()
    external.mkdir()

    internal_record = data_root_locator(repository, internal)
    external_record = data_root_locator(repository, external)
    assert internal_record == {
        "schema_version": 1,
        "kind": "repository_relative",
        "path": "data",
    }
    assert external_record["kind"] == "absolute"
    assert resolve_data_root_locator(repository, internal_record) == internal
    assert resolve_data_root_locator(repository, external_record) == external
    assert resolve_data_root_locator(repository, None) == internal
    with pytest.raises(ValueError, match="越出项目目录"):
        resolve_data_root_locator(repository, {
            "schema_version": 1,
            "kind": "repository_relative",
            "path": "../market-data",
        })


def test_code_identity_includes_wrappers_native_sources_and_build_contracts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """冻结身份必须覆盖 PowerShell、未来 GPU/native 源码与构建合同。"""
    included = (
        tmp_path / "src" / "guvolu" / "research.py",
        tmp_path / "src" / "gpu" / "kernel.cu",
        tmp_path / "src" / "native" / "worker.rs",
        tmp_path / "scripts" / "run_frozen_forward_task.ps1",
        tmp_path / "tests" / "test_research.py",
        tmp_path / "pyproject.toml",
        tmp_path / "config.json",
    )
    for path in included:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(path.name, encoding="utf-8")
    ignored = tmp_path / "scripts" / "notes.txt"
    ignored.write_text("not executable", encoding="utf-8")
    captured: tuple[Path, ...] = ()

    def capture_paths(_root: Path, paths: tuple[Path, ...]) -> str:
        nonlocal captured
        captured = paths
        return "tree"

    monkeypatch.setattr(provenance, "hash_paths", capture_paths)
    monkeypatch.setattr(
        provenance,
        "_git_output",
        lambda _root, arguments: (
            "a" * 40 if arguments[0] == "rev-parse" else " M src/guvolu/research.py"
        ),
    )
    identity = provenance.code_identity(tmp_path, (included[-1],))
    assert not identity.decision_grade
    assert set(included).issubset(set(captured))
    assert ignored not in captured


def test_clean_code_identity_uses_commit_bytes_across_crlf_checkout(
    tmp_path: Path,
) -> None:
    """clean/smudge 后的 CRLF 工作区仍必须重建出相同的决策级身份。"""
    source = tmp_path / "src" / "example.py"
    config = tmp_path / "config" / "strategy.json"
    source.parent.mkdir(parents=True)
    config.parent.mkdir(parents=True)
    (tmp_path / ".gitattributes").write_text(
        "*.py text eol=crlf\n*.json text eol=crlf\n",
        encoding="utf-8",
    )
    source.write_bytes(b"first = 1\nsecond = 2\n")
    config.write_bytes(b'{"market":"BTC"}\n')
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    subprocess.run(["git", "add", "."], cwd=tmp_path, check=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=Research Test",
            "-c",
            "user.email=research@example.invalid",
            "commit",
            "-q",
            "-m",
            "fixture",
        ],
        cwd=tmp_path,
        check=True,
    )
    source.unlink()
    config.unlink()
    subprocess.run(["git", "checkout-index", "-a"], cwd=tmp_path, check=True)
    assert b"\r\n" in source.read_bytes()
    assert b"\r\n" in config.read_bytes()
    subprocess.run(["git", "add", "src/example.py", "config/strategy.json"], cwd=tmp_path, check=True)
    assert subprocess.run(
        ["git", "status", "--porcelain=v1"],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
    ).stdout == ""

    identity = provenance.code_identity(tmp_path, (config,))
    assert identity.decision_grade
    assert identity.git_hash is not None
    assert identity.tree_digest == provenance.code_tree_digest_at_commit(
        tmp_path, identity.git_hash, (config,),
    )


def test_position_contract_combines_family_direction_and_risk_weight() -> None:
    """发布目标必须是分配权重与家族方向目标的乘积。"""
    family = FamilyEvaluation(
        family="trend",
        mode="paper",
        deployment_candidate=CandidateSpec("candidate", "trend", "paper", {}, 1),
        latest_target=-0.5,
        deployment_oos_metrics=_metrics(),
        deployment_oos_returns=(0.01,),
        metrics=_metrics(),
        adjusted_sharpe=0.5,
        fdr_q=0.1,
        eligible=True,
        rejection_reasons=(),
        oos_returns=(0.01,),
    )
    validation = ValidationResult((family,), (), {}, ())
    allocation = AllocationResult({"trend": 0.2}, 0.8, 0.1, "mixed", 1)
    payload = position_contract_payload(validation, allocation)
    assert payload == {
        "method_version": "risk-weighted-family-target-v1",
        "unit": "risk_weighted_directional_target",
        "aggregate_target": pytest.approx(-0.1),
        "families": [{
            "family": "trend",
            "deployment_candidate_id": "candidate",
            "eligible": True,
            "family_target": -0.5,
            "allocation_weight": 0.2,
            "portfolio_target_contribution": pytest.approx(-0.1),
        }],
    }


def test_market_state_contract_matches_hand_authored_fixture() -> None:
    """共享合同必须保持与 producer/verifier 无关的字段语义。"""
    state = MarketState(
        trend=0.5,
        volatility=0.25,
        liquidity=-0.1,
        flow=0.2,
        carry=None,
        cross_venue=None,
        relative=None,
        jump=1.5,
        regime="mixed",
        uncertainty=0.3,
    )
    assert market_state_payload(state) == {
        "trend": 0.5,
        "volatility": 0.25,
        "liquidity": -0.1,
        "flow": 0.2,
        "carry": None,
        "cross_venue": None,
        "relative": None,
        "jump": 1.5,
        "regime": "mixed",
        "uncertainty": 0.3,
    }


def test_operational_gate_requires_flat_position_for_non_decision_grade() -> None:
    """代码身份不可信时必须零权重、全余量且聚合目标为零。"""
    payload: dict[str, object] = {
        "decision_grade": False,
        "operational_quality": {"eligible": True},
        "operational_position": {
            "weights": {"trend": 0.1},
            "reserve": 0.9,
        },
        "operational_target_contract": {
            "aggregate_target": 0.1,
            "families": [{
                "family": "trend",
                "family_target": 1.0,
                "allocation_weight": 0.1,
                "portfolio_target_contribution": 0.1,
            }],
        },
    }
    with pytest.raises(ValueError, match="非决策级但存在非零仓位"):
        _verify_operational_gate(payload)
    payload["operational_position"] = {
        "weights": {"trend": 0.0},
        "reserve": 1.0,
    }
    payload["operational_target_contract"] = {
        "aggregate_target": 0.0,
        "families": [{
            "family": "trend",
            "family_target": 1.0,
            "allocation_weight": 0.0,
            "portfolio_target_contribution": 0.0,
        }],
    }
    _verify_operational_gate(payload)


def test_verifier_rejects_unsafe_legacy_v13_flow_allocation() -> None:
    """旧 v13 不得在最新经济成交量不合格时保留 flow 权重。"""
    payload: dict[str, object] = {
        "pipeline_method_version": "strategy-research-pipeline-v13",
        "decision_grade": True,
        "panel": {"latest_economic_volume_qualified": False},
        "family_evaluations": [{
            "family": "flow_trend",
            "mode": "paper",
            "eligible": True,
        }],
        "operational_quality": {"eligible": True},
        "operational_position": {
            "weights": {"flow_trend": 0.2},
            "reserve": 0.8,
        },
        "operational_target_contract": {
            "aggregate_target": 0.2,
            "families": [{
                "family": "flow_trend",
                "family_target": 1.0,
                "allocation_weight": 0.2,
                "portfolio_target_contribution": 0.2,
            }],
        },
    }
    with pytest.raises(ValueError, match="v13 flow 运行门禁语义过期"):
        _verify_operational_gate(payload)


def test_manifest_verifier_checks_hashes_and_flat_gate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """复核器须按 manifest 对象形状检查散列与质量清仓不变量。"""
    monkeypatch.setattr(
        "guvolu.research.verification._verify_data_governance",
        lambda _root, _summary: None,
    )
    report = tmp_path / "reports" / "strategy-research" / "run"
    report.mkdir(parents=True)
    candidate_registry = report / "candidate-registry.json"
    candidate_registry.write_text(json.dumps({
        "candidates": [{"candidate_id": "candidate-one"}],
    }), encoding="utf-8")
    code_identity = {
        "git_hash": "abc123",
        "tree_digest": "t" * 64,
        "dirty_digest": "d" * 64,
        "dirty": False,
        "decision_grade": True,
        "reason": None,
    }
    identity_fields: dict[str, object] = {
        "schema_version": 11,
        "pipeline_method_version": "strategy-research-pipeline-v11",
        "p_value_method_version": "p-value-test",
        "pbo_method_version": "pbo-test",
        "block_bootstrap_method_version": "bootstrap-test",
        "deflated_sharpe_method_version": "dsr-test",
        "effective_trial_method_version": "trial-test",
        "parameter_stability_method_version": "stability-test",
        "position_contract_method_version": "position-test",
        "governance_method_version": "governance-test",
        "generator_method_version": "generator-test",
        "family_scope": ["trend"],
        "decision_time": "2026-01-01T01:00:00+00:00",
        "execution_evaluated_at": "2026-01-01T01:01:00+00:00",
        "code_identity": code_identity,
        "config_hash": "c" * 64,
        "config_lineage_root_hash": "c" * 64,
        "config_lineage_depth": 0,
    }
    research_identity = stable_identifier("research-identity", {
        "pipeline_method_version": identity_fields["pipeline_method_version"],
        "p_value_method_version": identity_fields["p_value_method_version"],
        "pbo_method_version": identity_fields["pbo_method_version"],
        "block_bootstrap_method_version": identity_fields[
            "block_bootstrap_method_version"
        ],
        "deflated_sharpe_method_version": identity_fields[
            "deflated_sharpe_method_version"
        ],
        "effective_trial_method_version": identity_fields[
            "effective_trial_method_version"
        ],
        "parameter_stability_method_version": identity_fields[
            "parameter_stability_method_version"
        ],
        "position_contract_method_version": identity_fields[
            "position_contract_method_version"
        ],
        "config_hash": identity_fields["config_hash"],
        "config_lineage_root_hash": identity_fields["config_lineage_root_hash"],
        "config_lineage_depth": identity_fields["config_lineage_depth"],
        "head_generation": "head-one",
        "attempt_ids": [],
        "artifact_ids": [],
        "code_tree_digest": code_identity["tree_digest"],
        "dirty_digest": code_identity["dirty_digest"],
        "generator_method_version": identity_fields["generator_method_version"],
        "family_scope": identity_fields["family_scope"],
        "candidate_ids": ("candidate-one",),
        "governance_method_version": identity_fields["governance_method_version"],
        "data_scope": "DEV_ADAPTIVE",
    })
    run_id = stable_identifier("research-run", {
        "research_identity": research_identity,
        "execution_evaluated_at": identity_fields["execution_evaluated_at"],
    })
    identity_fields.update({
        "run_id": run_id,
        "research_identity": research_identity,
    })
    summary = report / "summary.json"
    summary_payload = {
        **identity_fields,
        "decision_grade": True,
        "operational_quality": {"eligible": False},
        "operational_position": {
            "weights": {"trend": 0.0},
            "reserve": 1.0,
        },
        "operational_target_contract": {
            "aggregate_target": 0.0,
            "families": [{
                "family": "trend",
                "family_target": 1.0,
                "allocation_weight": 0.0,
                "portfolio_target_contribution": 0.0,
            }],
        },
    }
    summary.write_text(json.dumps(summary_payload), encoding="utf-8")
    summary_bytes = summary.read_bytes()
    manifest = report / "manifest.json"
    manifest_payload = {
        **identity_fields,
        "input_head_generation": "head-one",
        "input_attempt_ids": [],
        "input_artifact_ids": [],
        "data_scope": "DEV_ADAPTIVE",
        "artifacts": {
            "summary_json": {
                "path": summary.relative_to(tmp_path).as_posix(),
                "sha256": hashlib.sha256(summary_bytes).hexdigest(),
                "bytes": len(summary_bytes),
            },
            "candidate_registry": {
                "path": candidate_registry.relative_to(tmp_path).as_posix(),
                "sha256": hashlib.sha256(
                    candidate_registry.read_bytes()
                ).hexdigest(),
                "bytes": candidate_registry.stat().st_size,
            },
        },
    }
    manifest.write_text(json.dumps(manifest_payload), encoding="utf-8")
    result = verify_research_run(tmp_path, manifest)
    assert result.run_id == run_id
    summary_payload["research_identity"] = "research-identity-forged"
    summary.write_text(json.dumps(summary_payload), encoding="utf-8")
    manifest_payload["artifacts"]["summary_json"] = {  # type: ignore[index]
        "path": summary.relative_to(tmp_path).as_posix(),
        "sha256": hashlib.sha256(summary.read_bytes()).hexdigest(),
        "bytes": summary.stat().st_size,
    }
    manifest.write_text(json.dumps(manifest_payload), encoding="utf-8")
    with pytest.raises(ValueError, match="research_identity"):
        verify_research_run(tmp_path, manifest)
    summary.write_text("{}", encoding="utf-8")
    with pytest.raises(ValueError, match="散列不匹配"):
        verify_research_run(tmp_path, manifest)


def test_legacy_v12_full_verification_and_current_head_use_fail_closed(
    tmp_path: Path,
) -> None:
    """旧制品可做字节读取，但不得冒充当前语义的完整验证或 head。"""
    identity = {
        "git_hash": "legacy-commit",
        "tree_digest": "t" * 64,
        "dirty_digest": hashlib.sha256(b"").hexdigest(),
        "dirty": False,
        "decision_grade": True,
        "reason": None,
    }
    manifest = {
        "pipeline_method_version": "strategy-research-pipeline-v12",
        "code_identity": identity,
        "decision_grade": True,
    }
    with pytest.raises(ValueError, match="仅允许制品完整性"):
        _verify_run_identity(
            tmp_path,
            manifest,
            dict(manifest),
            {"candidates": []},
            {},
            tmp_path,
        )
    receipt = tmp_path / "legacy-receipt.json"
    receipt.write_text(json.dumps({
        "schema_version": 1,
        "method_version": "active-trade-head-receipt-v1",
    }), encoding="utf-8")
    with pytest.raises(ValueError, match="不能证明当前 head"):
        attest_trade_input_receipt(
            tmp_path, receipt, require_current_head=True,
        )


def test_family_monitor_reports_parameter_search_direction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """监视器须从全候选事实给出独立参数轴方向。"""
    monkeypatch.setattr(
        "guvolu.research.evolution._verified_summary_source",
        lambda _root, path: (
            json.loads(path.read_text(encoding="utf-8")), "m" * 64,
        ),
    )
    ledger = tmp_path / "ledger.jsonl"
    records = []
    for lookback, sharpe in ((24, 0.1), (72, 0.6)):
        records.append(json.dumps({
            "record_type": "trial",
            "family": "trend",
            "fold_id": "walk-forward",
            "segment": "testing_aggregate",
            "parameters": {"lookback": lookback, "entry_score": 0.5},
            "metrics": {"sharpe": sharpe},
        }))
    ledger.write_text("\n".join(records) + "\n", encoding="utf-8")
    summary = tmp_path / "summary.json"
    summary.write_text(json.dumps({
        "run_id": "run",
        "research_identity": "research-one",
        "decision_time": "2026-01-01T00:00:00+00:00",
        "market_id": "market-one",
        "config_hash": "c" * 64,
        "panel": {"sha256": "p" * 64},
        "code_identity": {"tree_digest": "t" * 64},
        "family_evaluations": [{
            "family": "trend",
            "eligible": True,
            "rejection_reasons": [],
            "adjusted_sharpe": 0.5,
            "fdr_q": 0.1,
            "latest_unallocated_target": 1.0,
        }],
        "artifacts": {
            "trial_ledger": {
                "path": ledger.relative_to(tmp_path).as_posix(),
                "sha256": hashlib.sha256(ledger.read_bytes()).hexdigest(),
            },
        },
    }), encoding="utf-8")
    config = json.loads(Path("config/strategy_research.json").read_text())
    payload = monitor_family_run(tmp_path, summary, "trend", config, "c" * 64)
    assert payload["monitor_method_version"] == "family-direction-monitor-v8"
    assert payload["failure_attribution"] == {
        "category": "eligible_performance",
    }
    directions = {
        item["parameter"]: item["direction"]
        for item in payload["parameter_directions"]
    }
    assert directions["lookback"] == "explore_higher_after_preregistration"
    assert directions["entry_score"] == "fixed"
    prior_payload = json.loads(summary.read_text(encoding="utf-8"))
    prior_payload["run_id"] = "prior-instance-one"
    prior_payload["research_identity"] = "same-prior-research"
    prior_payload["decision_time"] = "2025-09-01T00:00:00+00:00"
    first_prior = tmp_path / "prior-one.json"
    first_prior.write_text(json.dumps(prior_payload), encoding="utf-8")
    prior_payload["run_id"] = "prior-instance-two"
    second_prior = tmp_path / "prior-two.json"
    second_prior.write_text(json.dumps(prior_payload), encoding="utf-8")
    deduplicated = monitor_family_run(
        tmp_path,
        summary,
        "trend",
        config,
        "c" * 64,
        (first_prior, second_prior),
    )
    deduplicated_reversed = monitor_family_run(
        tmp_path,
        summary,
        "trend",
        config,
        "c" * 64,
        (second_prior, first_prior),
    )
    assert deduplicated == deduplicated_reversed
    assert deduplicated["monitor_method_version"] == "family-direction-monitor-v8"
    assert len(deduplicated["source"]["history_summaries"]) == 2
    assert len(deduplicated["history"]) == 0
    assert {
        item["reason"] for item in deduplicated["excluded_history"]
    } == {"duplicate_data_vintage", "duplicate_research_identity"}
    assert deduplicated["cross_run_direction"] == "insufficient_history"

    cost_dominated = json.loads(summary.read_text(encoding="utf-8"))
    cost_dominated["run_id"] = "cost-dominated"
    cost_dominated["research_identity"] = "cost-dominated-research"
    cost_dominated["family_evaluations"][0].update({
        "eligible": False,
        "rejection_reasons": ["non_positive_oos_net_return"],
        "validation_metrics": {"net_return": -0.1, "cost": 0.3},
    })
    cost_path = tmp_path / "cost-dominated.json"
    cost_path.write_text(json.dumps(cost_dominated), encoding="utf-8")
    cost_monitor = monitor_family_run(
        tmp_path, cost_path, "trend", config, "c" * 64,
    )
    assert cost_monitor["failure_attribution"] == {
        "category": "execution_cost_dominated",
        "net_return": -0.1,
        "estimated_gross_return_before_cost": pytest.approx(0.2),
        "cost": 0.3,
    }
    assert cost_monitor["evolution_action"] == (
        "reduce_turnover_or_improve_execution_before_parameter_evolution"
    )

    signal_negative = json.loads(json.dumps(cost_dominated))
    signal_negative["run_id"] = "signal-negative"
    signal_negative["research_identity"] = "signal-negative-research"
    signal_negative["family_evaluations"][0]["validation_metrics"] = {
        "net_return": -0.4,
        "cost": 0.1,
    }
    signal_path = tmp_path / "signal-negative.json"
    signal_path.write_text(json.dumps(signal_negative), encoding="utf-8")
    signal_monitor = monitor_family_run(
        tmp_path, signal_path, "trend", config, "c" * 64,
    )
    assert signal_monitor["failure_attribution"]["category"] == (
        "signal_edge_non_positive"
    )
    assert signal_monitor["evolution_action"] == (
        "revise_hypothesis_before_parameter_evolution"
    )

    recent_prior = json.loads(summary.read_text(encoding="utf-8"))
    recent_prior["run_id"] = "time-separated-one"
    recent_prior["research_identity"] = "time-separated-research-one"
    recent_prior["decision_time"] = "2025-09-01T00:00:00+00:00"
    recent_prior["panel"]["sha256"] = "q" * 64
    recent_prior["family_evaluations"][0]["adjusted_sharpe"] = 0.1
    recent_prior["family_evaluations"][0]["fdr_q"] = 0.2
    recent_path = tmp_path / "time-separated-one.json"
    recent_path.write_text(json.dumps(recent_prior), encoding="utf-8")
    recent_duplicate = json.loads(json.dumps(recent_prior))
    recent_duplicate["run_id"] = "time-separated-duplicate"
    recent_duplicate["research_identity"] = "time-separated-duplicate-research"
    recent_duplicate_path = tmp_path / "time-separated-duplicate.json"
    recent_duplicate_path.write_text(
        json.dumps(recent_duplicate), encoding="utf-8",
    )
    vintage_forward = monitor_family_run(
        tmp_path,
        summary,
        "trend",
        config,
        "c" * 64,
        (recent_path, recent_duplicate_path),
    )
    vintage_reversed = monitor_family_run(
        tmp_path,
        summary,
        "trend",
        config,
        "c" * 64,
        (recent_duplicate_path, recent_path),
    )
    assert vintage_forward == vintage_reversed
    assert len(vintage_forward["history"]) == 1
    assert vintage_forward["excluded_history"][0]["reason"] == (
        "duplicate_data_vintage"
    )
    older_prior = json.loads(json.dumps(recent_prior))
    older_prior["run_id"] = "time-separated-two"
    older_prior["research_identity"] = "time-separated-research-two"
    older_prior["decision_time"] = "2025-05-01T00:00:00+00:00"
    older_prior["panel"]["sha256"] = "r" * 64
    older_path = tmp_path / "time-separated-two.json"
    older_path.write_text(json.dumps(older_prior), encoding="utf-8")
    time_separated = monitor_family_run(
        tmp_path,
        summary,
        "trend",
        config,
        "c" * 64,
        (recent_path, older_path),
    )
    assert len(time_separated["history"]) == 2
    assert time_separated["excluded_history"] == []
    assert time_separated["cross_run_direction"] == "improving"

    other_market = json.loads(json.dumps(older_prior))
    other_market["run_id"] = "other-market"
    other_market["research_identity"] = "other-market-research"
    other_market["market_id"] = "market-two"
    other_market["panel"]["sha256"] = "s" * 64
    other_market_path = tmp_path / "other-market.json"
    other_market_path.write_text(json.dumps(other_market), encoding="utf-8")
    cohort_filtered = monitor_family_run(
        tmp_path,
        summary,
        "trend",
        config,
        "c" * 64,
        (other_market_path,),
    )
    assert cohort_filtered["history"] == []
    assert cohort_filtered["excluded_history"][0]["reason"] == "incomparable_cohort"
    assert cohort_filtered["excluded_history"][0]["source_summary_sha256"]
    assert cohort_filtered["excluded_history"][0]["source_manifest_sha256"] == "m" * 64

    canonical_directory = (
        tmp_path / "reports" / "strategy-research" / "families" / "trend"
        / "research-run-canonical"
    )
    canonical_directory.mkdir(parents=True)
    canonical_summary = canonical_directory / "summary.json"
    canonical_summary.write_text(json.dumps(recent_prior), encoding="utf-8")
    combined_directory = (
        tmp_path / "reports" / "strategy-research" / "research-run-combined"
    )
    combined_directory.mkdir(parents=True)
    combined_summary = combined_directory / "summary.json"
    combined_summary.write_text(json.dumps(older_prior), encoding="utf-8")
    canonical = monitor_family_run(
        tmp_path, summary, "trend", config, "c" * 64,
    )
    assert len(canonical["history"]) == 1
    assert canonical["history"][0]["source_summary_path"] == (
        canonical_summary.relative_to(tmp_path).as_posix()
    )
    assert canonical["source"]["history_summaries"] == [{
        "summary_path": canonical_summary.relative_to(tmp_path).as_posix(),
        "summary_sha256": hashlib.sha256(canonical_summary.read_bytes()).hexdigest(),
        "manifest_sha256": "m" * 64,
    }]

    ledger.write_text(ledger.read_text(encoding="utf-8") + "{}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="trial ledger 散列"):
        monitor_family_run(tmp_path, summary, "trend", config, "c" * 64)


def test_evolution_proposal_updates_strategy_and_feature_dependencies(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """扩展回看轴时必须同步共享特征并遵守候选预算。"""
    monkeypatch.setattr(
        "guvolu.research.tuning.verify_monitor_sources",
        lambda *_args: None,
    )
    config = json.loads(Path("config/strategy_research.json").read_text())
    config_content = canonical_json(config) + "\n"
    config_path = tmp_path / "strategy_research.json"
    config_path.write_bytes(config_content.encode("utf-8"))
    parent_hash = hashlib.sha256(config_path.read_bytes()).hexdigest()
    monitor = {
        "family": "trend",
        "run_id": "run",
        "monitor_method_version": "family-direction-monitor-v6",
        "source": {
            "summary_sha256": "a" * 64,
            "trial_ledger_sha256": "b" * 64,
            "config_hash": parent_hash,
            "panel_sha256": "p" * 64,
            "manifest_sha256": "c" * 64,
            "code_identity": {"tree_digest": "d" * 64},
        },
        "evolution_action": "eligible_axis_refinement",
        "cross_run_direction": "insufficient_history",
        "history_policy": {"comparison_cohort_id": "cohort"},
        "parameter_directions": [{
            "parameter": "lookback",
            "direction": "explore_higher_after_preregistration",
            "association": 0.8,
        }],
    }
    monitor_content = canonical_json(monitor) + "\n"
    monitor_hash = hashlib.sha256(monitor_content.encode("utf-8")).hexdigest()
    monitor_path = tmp_path / f"family-monitor-sha256-{monitor_hash}.json"
    monitor_path.write_bytes(monitor_content.encode("utf-8"))
    proposal, proposed = propose_family_evolution(
        tmp_path, config_path, monitor_path, (),
    )
    assert proposal["status"] == "proposed"
    assert proposal["proposal_method_version"] == "family-evolution-proposal-v3"
    assert proposed is not None
    assert 264 in proposed["strategies"]["trend"]["lookbacks"]
    assert 264 in proposed["features"]["lookbacks"]
    assert len(build_family_batches(proposed, ("trend",))[0].candidates) == 8
    assert proposal["source_monitor_sha256"] == hashlib.sha256(
        monitor_path.read_bytes(),
    ).hexdigest()
    assert proposal["source_monitor_path"] == monitor_path.name
    assert proposed["evolution_parent"]["source_monitor_sha256"] == monitor_hash
    assert proposed["evolution_parent"]["parent_config_path"] == config_path.name
    assert proposed["evolution_parent"]["lineage_depth"] == 1
    assert proposed["evolution_parent"]["lineage_root_config_hash"] == parent_hash
    prior_content = canonical_json(proposal) + "\n"
    prior_hash = hashlib.sha256(prior_content.encode("utf-8")).hexdigest()
    prior_path = tmp_path / f"proposal-sha256-{prior_hash}.json"
    prior_path.write_bytes(prior_content.encode("utf-8"))
    unsupported_prior = {**proposal}
    unsupported_prior.pop("proposal_method_version")
    unsupported_content = canonical_json(unsupported_prior) + "\n"
    unsupported_hash = hashlib.sha256(
        unsupported_content.encode("utf-8"),
    ).hexdigest()
    unsupported_path = tmp_path / (
        f"proposal-sha256-{unsupported_hash}.json"
    )
    unsupported_path.write_bytes(unsupported_content.encode("utf-8"))
    forged_variants = (
        ("direction", "explore_lower_after_preregistration"),
        ("schema_version", True),
        ("candidate_count", float(proposal["candidate_count"])),
        ("candidate_budget", float(proposal["candidate_budget"])),
        ("proposed_value", float(proposal["proposed_value"])),
        ("holdout_consumed", 0),
    )
    forged_paths: list[Path] = []
    for field, value in forged_variants:
        forged_content = canonical_json({**proposal, field: value}) + "\n"
        forged_hash = hashlib.sha256(
            forged_content.encode("utf-8"),
        ).hexdigest()
        forged_path = tmp_path / f"proposal-sha256-{forged_hash}.json"
        forged_path.write_bytes(forged_content.encode("utf-8"))
        forged_paths.append(forged_path)
    duplicate, duplicate_config = propose_family_evolution(
        tmp_path,
        config_path,
        monitor_path,
        (unsupported_path, *forged_paths, prior_path),
    )
    assert duplicate_config is None
    assert duplicate["status"] == "no_parameter_proposal"
    assert duplicate["reason"] == "duplicate_axis_value_proposal"
    assert duplicate["duplicate_basis"] == "same_data_vintage"
    assert duplicate["prior_proposal_sha256"] == prior_hash
    exclusions = duplicate["excluded_proposal_history"]
    assert any(
        item["path"] == unsupported_path.name
        and item["detail"] == "历史提案方法版本不受支持"
        for item in exclusions
    )
    excluded_paths = {
        item["path"] for item in exclusions
        if "不能由父配置与 monitor 重建" in item.get("detail", "")
    }
    assert excluded_paths.issuperset(path.name for path in forged_paths)
    rejection_content = canonical_json(duplicate) + "\n"
    rejection_hash = hashlib.sha256(
        rejection_content.encode("utf-8"),
    ).hexdigest()
    rejection_path = tmp_path / (
        f"proposal-sha256-{rejection_hash}.json"
    )
    rejection_path.write_bytes(rejection_content.encode("utf-8"))
    repeated, repeated_config = propose_family_evolution(
        tmp_path,
        config_path,
        monitor_path,
        (unsupported_path, *forged_paths, prior_path, rejection_path),
    )
    assert repeated_config is None
    assert repeated == duplicate

    canonical_history = (
        tmp_path / "reports" / "strategy-research" / "evolution-proposals"
        / "trend"
    )
    canonical_history.mkdir(parents=True)
    canonical_prior = canonical_history / prior_path.name
    canonical_prior.write_bytes(prior_content.encode("utf-8"))
    corrupt_content = "{"
    corrupt_hash = hashlib.sha256(corrupt_content.encode("utf-8")).hexdigest()
    corrupt_path = canonical_history / f"proposal-sha256-{corrupt_hash}.json"
    corrupt_path.write_text(corrupt_content, encoding="utf-8")
    nested_content = "[" * 2000 + "0" + "]" * 2000
    nested_hash = hashlib.sha256(nested_content.encode("utf-8")).hexdigest()
    nested_path = canonical_history / f"proposal-sha256-{nested_hash}.json"
    nested_path.write_text(nested_content, encoding="utf-8")
    canonical_duplicate, canonical_config = propose_family_evolution(
        tmp_path, config_path, monitor_path, (),
    )
    assert canonical_config is None
    assert canonical_duplicate["reason"] == "duplicate_axis_value_proposal"
    assert canonical_duplicate["prior_proposal_path"] == (
        canonical_prior.relative_to(tmp_path).as_posix()
    )
    assert any(
        item["path"] == corrupt_path.relative_to(tmp_path).as_posix()
        and item["reason"] == "unreadable_or_invalid_proposal_artifact"
        for item in canonical_duplicate["excluded_proposal_history"]
    )
    assert any(
        item["path"] == nested_path.relative_to(tmp_path).as_posix()
        and item["reason"] == "unreadable_or_invalid_proposal_artifact"
        for item in canonical_duplicate["excluded_proposal_history"]
    )

    future_monitor = json.loads(json.dumps(monitor))
    future_monitor["cross_run_direction"] = "stable"
    future_monitor["source"]["panel_sha256"] = "q" * 64
    future_content = canonical_json(future_monitor) + "\n"
    future_hash = hashlib.sha256(future_content.encode("utf-8")).hexdigest()
    future_path = tmp_path / f"family-monitor-sha256-{future_hash}.json"
    future_path.write_bytes(future_content.encode("utf-8"))
    future_proposal, future_config = propose_family_evolution(
        tmp_path, config_path, future_path, (prior_path,),
    )
    assert future_proposal["status"] == "proposed"
    assert future_config is not None

    unseparated_monitor = json.loads(json.dumps(future_monitor))
    unseparated_monitor["cross_run_direction"] = "insufficient_history"
    unseparated_content = canonical_json(unseparated_monitor) + "\n"
    unseparated_hash = hashlib.sha256(
        unseparated_content.encode("utf-8"),
    ).hexdigest()
    unseparated_path = tmp_path / (
        f"family-monitor-sha256-{unseparated_hash}.json"
    )
    unseparated_path.write_bytes(unseparated_content.encode("utf-8"))
    unseparated, unseparated_config = propose_family_evolution(
        tmp_path, config_path, unseparated_path, (prior_path,),
    )
    assert unseparated_config is None
    assert unseparated["duplicate_basis"] == "insufficient_new_history"

    invalid_prior_path = tmp_path / "proposal-invalid-name.json"
    invalid_prior_path.write_bytes(prior_content.encode("utf-8"))
    invalid_history, invalid_config = propose_family_evolution(
        tmp_path, config_path, monitor_path, (invalid_prior_path,),
    )
    assert invalid_config is None
    assert any(
        item["path"] == invalid_prior_path.name
        and item["reason"] == "unreadable_or_invalid_proposal_artifact"
        for item in invalid_history["excluded_proposal_history"]
    )
    derived_path = tmp_path / "derived.json"
    derived_path.write_bytes((canonical_json(proposed) + "\n").encode("utf-8"))
    assert verify_config_lineage(tmp_path, derived_path) == (parent_hash, 1)
    verify_evolution_config(tmp_path, derived_path, proposed)
    arbitrary_child = json.loads(derived_path.read_text(encoding="utf-8"))
    arbitrary_child["strategies"]["trend"]["maximum_target"] = 0.25
    arbitrary_path = tmp_path / "arbitrary-child.json"
    arbitrary_path.write_bytes(
        (canonical_json(arbitrary_child) + "\n").encode("utf-8"),
    )
    assert verify_config_lineage(tmp_path, arbitrary_path) == (parent_hash, 1)
    with pytest.raises(ValueError, match="允许的单轴变换"):
        verify_evolution_config(tmp_path, arbitrary_path, arbitrary_child)
    type_changed = json.loads(derived_path.read_text(encoding="utf-8"))
    type_changed["strategies"]["trend"]["lookbacks"][0] = 24.0
    type_changed_path = tmp_path / "type-changed-child.json"
    type_changed_path.write_bytes(
        (canonical_json(type_changed) + "\n").encode("utf-8"),
    )
    assert verify_config_lineage(tmp_path, type_changed_path) == (parent_hash, 1)
    with pytest.raises(ValueError, match="允许的单轴变换"):
        verify_evolution_config(tmp_path, type_changed_path, type_changed)
    boolean_changed = json.loads(derived_path.read_text(encoding="utf-8"))
    boolean_changed["schema_version"] = True
    boolean_path = tmp_path / "boolean-child.json"
    boolean_path.write_bytes(
        (canonical_json(boolean_changed) + "\n").encode("utf-8"),
    )
    with pytest.raises(ValueError, match="允许的单轴变换"):
        verify_evolution_config(tmp_path, boolean_path, boolean_changed)
    non_finite_path = tmp_path / "non-finite.json"
    non_finite_path.write_text('{"maximum_target":NaN}', encoding="utf-8")
    with pytest.raises(ValueError, match="非有限数值"):
        verify_config_lineage(tmp_path, non_finite_path)
    forged_lineage = json.loads(derived_path.read_text(encoding="utf-8"))
    forged_lineage["evolution_parent"]["lineage_root_config_hash"] = "f" * 64
    derived_path.write_bytes(
        (canonical_json(forged_lineage) + "\n").encode("utf-8"),
    )
    with pytest.raises(ValueError, match="谱系根散列"):
        verify_config_lineage(tmp_path, derived_path)

    changed_config = json.loads(config_content)
    changed_config["strategies"]["trend"]["lookbacks"] = [24, 72]
    config_path.write_bytes((canonical_json(changed_config) + "\n").encode("utf-8"))
    with pytest.raises(ValueError, match="来源配置"):
        propose_family_evolution(tmp_path, config_path, monitor_path, ())
    config_path.write_bytes(config_content.encode("utf-8"))

    rejected_monitor = json.loads(monitor_content)
    rejected_monitor["evolution_action"] = "revise_hypothesis_or_cost_model"
    rejected_content = canonical_json(rejected_monitor) + "\n"
    rejected_hash = hashlib.sha256(rejected_content.encode("utf-8")).hexdigest()
    rejected_path = tmp_path / f"family-monitor-sha256-{rejected_hash}.json"
    rejected_path.write_bytes(rejected_content.encode("utf-8"))
    rejected, rejected_config = propose_family_evolution(
        tmp_path, config_path, rejected_path, (),
    )
    assert rejected_config is None
    assert rejected["status"] == "no_parameter_proposal"
    assert rejected["reason"] == "revise_hypothesis_or_cost_model"
    assert rejected["source_monitor_sha256"] == rejected_hash
    assert rejected["source_summary_sha256"] == "a" * 64
    assert rejected["source_trial_ledger_sha256"] == "b" * 64

    tampered = json.loads(monitor_path.read_text(encoding="utf-8"))
    tampered["parameter_directions"][0]["direction"] = (
        "explore_lower_after_preregistration"
    )
    monitor_path.write_bytes((canonical_json(tampered) + "\n").encode("utf-8"))
    with pytest.raises(ValueError, match="文件名与实际制品散列"):
        propose_family_evolution(tmp_path, config_path, monitor_path, ())


def test_monitor_source_verification_recomputes_consumed_direction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """自洽命名的伪造方向也必须被来源事实重算拒绝。"""
    summary = tmp_path / "summary.json"
    ledger = tmp_path / "ledger.jsonl"
    summary.write_text("{}", encoding="utf-8")
    ledger.write_text("{}\n", encoding="utf-8")
    monitor = {
        "monitor_method_version": "family-direction-monitor-v7",
        "family": "trend",
        "run_id": "run",
        "research_identity": "research",
        "source": {
            "summary_path": "summary.json",
            "summary_sha256": hashlib.sha256(summary.read_bytes()).hexdigest(),
            "trial_ledger_path": "ledger.jsonl",
            "trial_ledger_sha256": hashlib.sha256(ledger.read_bytes()).hexdigest(),
            "config_hash": "c" * 64,
            "history_summaries": [],
        },
        "cross_run_direction": "insufficient_history",
        "history": [],
        "excluded_history": [],
        "history_policy": {"method": "test"},
    }
    protected_summary = {
        "run_id": "run",
        "research_identity": "research",
        "config_hash": "c" * 64,
        "artifacts": {
            "trial_ledger": {
                "path": "ledger.jsonl",
                "sha256": monitor["source"]["trial_ledger_sha256"],
            },
        },
    }
    monkeypatch.setattr(
        "guvolu.research.tuning.verify_research_run", lambda *_args: None,
    )
    monkeypatch.setattr(
        "guvolu.research.tuning.json.loads", lambda *_args, **_kwargs: protected_summary,
    )
    recomputed = {
        **monitor,
        "evolution_action": "eligible_axis_refinement",
        "parameter_directions": [{
            "parameter": "lookback",
            "direction": "explore_higher_after_preregistration",
        }],
    }
    monkeypatch.setattr(
        "guvolu.research.tuning.monitor_family_run",
        lambda *_args, **_kwargs: recomputed,
    )
    forged = {
        **monitor,
        "evolution_action": "eligible_axis_refinement",
        "parameter_directions": [{
            "parameter": "lookback",
            "direction": "explore_lower_after_preregistration",
        }],
    }
    from guvolu.research.tuning import verify_monitor_sources

    with pytest.raises(ValueError, match="parameter_directions"):
        verify_monitor_sources(tmp_path, {}, forged, "c" * 64)
    forged_direction = {
        **recomputed,
        "cross_run_direction": "stable",
    }
    with pytest.raises(ValueError, match="cross_run_direction"):
        verify_monitor_sources(tmp_path, {}, forged_direction, "c" * 64)


def test_legacy_monitor_replay_does_not_discover_later_history(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """v5/v6 必须按原登记历史重放，不能吸收事后新增 canonical 运行。"""
    summary = tmp_path / "summary.json"
    ledger = tmp_path / "ledger.jsonl"
    summary.write_text("{}", encoding="utf-8")
    ledger.write_text("{}\n", encoding="utf-8")
    ledger_hash = hashlib.sha256(ledger.read_bytes()).hexdigest()
    monitor = {
        "monitor_method_version": "family-direction-monitor-v5",
        "family": "trend",
        "run_id": "run",
        "research_identity": "research",
        "source": {
            "summary_path": "summary.json",
            "summary_sha256": hashlib.sha256(summary.read_bytes()).hexdigest(),
            "trial_ledger_path": "ledger.jsonl",
            "trial_ledger_sha256": ledger_hash,
            "config_hash": "c" * 64,
        },
    }
    protected_summary = {
        "run_id": "run",
        "research_identity": "research",
        "config_hash": "c" * 64,
        "artifacts": {
            "trial_ledger": {"path": "ledger.jsonl", "sha256": ledger_hash},
        },
    }
    monkeypatch.setattr(tuning_module, "verify_research_run", lambda *_args: None)
    monkeypatch.setattr(
        tuning_module.json,
        "loads",
        lambda *_args, **_kwargs: protected_summary,
    )
    observed: dict[str, object] = {}

    def replay(*args: object, **kwargs: object) -> dict[str, object]:
        observed["prior_paths"] = args[5]
        observed["method"] = kwargs["monitor_method_version"]
        return {"monitor_method_version": kwargs["monitor_method_version"]}

    monkeypatch.setattr(tuning_module, "_monitor_family_run", replay)
    monkeypatch.setattr(
        tuning_module,
        "monitor_family_run",
        lambda *_args, **_kwargs: pytest.fail(
            "legacy monitor 不得自动发现后来新增的 canonical 历史"
        ),
    )
    rebuilt = tuning_module._recompute_monitor_sources(
        tmp_path, {}, monitor, "c" * 64, (),
    )
    assert rebuilt["monitor_method_version"] == "family-direction-monitor-v5"
    assert observed == {
        "prior_paths": (),
        "method": "family-direction-monitor-v5",
    }


def test_content_addressed_config_lineage_survives_source_changes_and_binds_git(
    tmp_path: Path,
) -> None:
    """历史运行使用完整快照，决策级配置逐字节绑定记录 commit。"""
    subprocess.run(["git", "init"], cwd=tmp_path, check=True, capture_output=True)
    subprocess.run(
        ["git", "config", "core.autocrlf", "false"],
        cwd=tmp_path,
        check=True,
    )
    subprocess.run(
        ["git", "config", "user.email", "research@example.invalid"],
        cwd=tmp_path,
        check=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Research Test"],
        cwd=tmp_path,
        check=True,
    )
    config_directory = tmp_path / "config"
    config_directory.mkdir()
    root_config = config_directory / "root.json"
    root_raw = b'{"market_id":"market-one","value":1}\n'
    root_config.write_bytes(root_raw)
    root_hash = hashlib.sha256(root_raw).hexdigest()
    leaf_config = config_directory / "leaf.json"
    leaf_payload = {
        "market_id": "market-one",
        "value": 2,
        "evolution_parent": {
            "parent_config_path": "config/root.json",
            "parent_config_hash": root_hash,
            "lineage_root_config_hash": root_hash,
            "lineage_depth": 1,
        },
    }
    leaf_config.write_bytes(
        (json.dumps(leaf_payload, sort_keys=True, separators=(",", ":")) + "\n").encode()
    )
    subprocess.run(
        ["git", "add", "config/root.json", "config/leaf.json"],
        cwd=tmp_path,
        check=True,
    )
    subprocess.run(
        ["git", "commit", "-m", "freeze config"],
        cwd=tmp_path,
        check=True,
        capture_output=True,
    )
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    snapshot = snapshot_verified_config_lineage(
        tmp_path, leaf_config, tmp_path / "reports" / "artifacts-one",
    )
    config, config_hash, lineage_root, depth, sources, artifacts = (
        attest_config_lineage_snapshot(
            tmp_path, snapshot.bundle_path, snapshot.leaf_config_path,
        )
    )
    assert config["value"] == 2
    assert config_hash == snapshot.leaf_config_sha256
    assert lineage_root == root_hash
    assert depth == 1
    provenance.verify_artifacts_match_commit(
        tmp_path, commit, tuple(zip(sources, artifacts, strict=True)),
    )

    leaf_payload["value"] = 3
    leaf_config.write_bytes(
        (json.dumps(leaf_payload, sort_keys=True, separators=(",", ":")) + "\n").encode()
    )
    historical = attest_config_lineage_snapshot(
        tmp_path, snapshot.bundle_path, snapshot.leaf_config_path,
    )
    assert historical[0]["value"] == 2
    changed_snapshot = snapshot_verified_config_lineage(
        tmp_path, leaf_config, tmp_path / "reports" / "artifacts-two",
    )
    changed = attest_config_lineage_snapshot(
        tmp_path, changed_snapshot.bundle_path, changed_snapshot.leaf_config_path,
    )
    with pytest.raises(ValueError, match="Git blob 不一致"):
        provenance.verify_artifacts_match_commit(
            tmp_path,
            commit,
            tuple(zip(changed[4], changed[5], strict=True)),
        )


def test_config_lineage_rejects_invalid_and_excessive_chains(
    tmp_path: Path,
) -> None:
    """配置父链须验证路径、SHA、深度、多级递归和最大长度。"""
    base = tmp_path / "base.json"
    base.write_bytes(b'{"schema_version":1}\n')
    root_hash = hashlib.sha256(base.read_bytes()).hexdigest()

    def write_child(
        path: Path,
        parent: Path,
        parent_hash: str,
        depth: int,
    ) -> None:
        path.write_bytes((canonical_json({
            "schema_version": 1,
            "evolution_parent": {
                "parent_config_path": parent.relative_to(tmp_path).as_posix(),
                "parent_config_hash": parent_hash,
                "lineage_root_config_hash": root_hash,
                "lineage_depth": depth,
            },
        }) + "\n").encode("utf-8"))

    first = tmp_path / "first.json"
    write_child(first, base, root_hash, 1)
    first_hash = hashlib.sha256(first.read_bytes()).hexdigest()
    second = tmp_path / "second.json"
    write_child(second, first, first_hash, 2)
    assert verify_config_lineage(tmp_path, second) == (root_hash, 2)

    bad_hash = tmp_path / "bad-hash.json"
    write_child(bad_hash, base, "0" * 64, 1)
    with pytest.raises(ValueError, match="父配置实际散列"):
        verify_config_lineage(tmp_path, bad_hash)
    bad_depth = tmp_path / "bad-depth.json"
    write_child(bad_depth, base, root_hash, 2)
    with pytest.raises(ValueError, match="谱系深度"):
        verify_config_lineage(tmp_path, bad_depth)
    outside = tmp_path.parent / "outside-lineage.json"
    outside.write_bytes(b'{"schema_version":1}\n')
    outside_hash = hashlib.sha256(outside.read_bytes()).hexdigest()
    outside_child = tmp_path / "outside-child.json"
    outside_child.write_bytes((canonical_json({
        "evolution_parent": {
            "parent_config_path": "../outside-lineage.json",
            "parent_config_hash": outside_hash,
            "lineage_root_config_hash": outside_hash,
            "lineage_depth": 1,
        },
    }) + "\n").encode("utf-8"))
    with pytest.raises(ValueError, match="父配置路径越出"):
        verify_config_lineage(tmp_path, outside_child)

    parent = base
    parent_hash = root_hash
    for depth in range(1, 33):
        child = tmp_path / f"deep-{depth:02d}.json"
        write_child(child, parent, parent_hash, depth)
        parent = child
        parent_hash = hashlib.sha256(child.read_bytes()).hexdigest()
    with pytest.raises(ValueError, match="最大深度"):
        verify_config_lineage(tmp_path, parent)
