"""Query Catalog 只暴露活动 head，不扫描或混入未激活 attempt。"""
from __future__ import annotations

from pathlib import Path

from guvolu.data import store
from guvolu.data.materialize import ensure_markets
from guvolu.ui.query_catalog import CATALOG_SCHEMA_VERSION, QueryCatalog
from guvolu.venues import registry


def test_market_catalog_uses_only_active_head(tmp_path: Path) -> None:
    conn = store.connect(tmp_path)
    try:
        registry.register_all(conn)
        ensure_markets(conn)
        market_id = "mkt__gmo__btc__r0"
        conn.execute(
            "INSERT INTO partition_attempt "
            "(attempt_id,market_id,domain,partition_key,normalization_version,"
            "input_set_hash,status,source_rows,normalized_rows,rejected_rows,"
            "started_at,finished_at,code_version,config_hash,ignored_rows) "
            "VALUES ('active',?,'trade','2026-08','trade-v1','set-a','complete',"
            "10,9,1,'2026-08-11T00:00:00+00:00',"
            "'2026-08-11T00:00:01+00:00','test','cfg',0)",
            (market_id,),
        )
        conn.execute(
            "INSERT INTO partition_attempt "
            "(attempt_id,market_id,domain,partition_key,normalization_version,"
            "input_set_hash,status,source_rows,normalized_rows,rejected_rows,"
            "started_at,finished_at,code_version,config_hash,ignored_rows) "
            "VALUES ('failed',?,'trade','2026-09','trade-v2','set-b','failed',"
            "99,0,0,'2026-08-11T00:00:02+00:00',"
            "'2026-08-11T00:00:03+00:00','test','cfg',0)",
            (market_id,),
        )
        conn.execute(
            "INSERT INTO artifact VALUES "
            "(?,'materialized_parquet','materialized/out.parquet',"
            "?,1,'2026-08-11T00:00:01+00:00',"
            "'2026-08-11T00:00:01+00:00','sha256-file-v1',2)",
            ("sha256-" + "a" * 64, "a" * 64),
        )
        conn.execute(
            "INSERT INTO materialization_output VALUES "
            "('active',?,'trade_observation',9,"
            "'2026-08-01T00:00:00+00:00','2026-08-01T00:00:09+00:00',"
            "'2026-08-11T00:00:01+00:00')",
            ("sha256-" + "a" * 64,),
        )
        conn.execute(
            "INSERT INTO materialization_partition_head VALUES "
            "(?,'trade','2026-08','trade-v1','active',"
            "'2026-08-11T00:00:01+00:00')",
            (market_id,),
        )
        conn.commit()
    finally:
        conn.close()

    markets = QueryCatalog(tmp_path).list_markets()
    gmo = next(item for item in markets if item["market_id"] == market_id)
    assert CATALOG_SCHEMA_VERSION == 1
    assert gmo["instrument_id"] == "SPOT:BTC/JPY"
    assert gmo["base_currency"] == "BTC"
    assert gmo["quote_currency"] == "JPY"
    trade = gmo["domains"]["trade"]
    assert trade["partition_count"] == 1
    assert trade["normalization_versions"] == ["trade-v1"]
    assert trade["datasets"]["trade_observation"]["rows"] == 9
    assert trade["coverage_state"] == "available"
    assert trade["head_generation"].startswith("sha256-")
    assert "trade-v2" not in trade["normalization_versions"]
