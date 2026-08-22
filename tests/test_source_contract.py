"""端点身份控制面的确定性与非破坏迁移测试。"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from guvolu.data.source_contract import (
    EndpointNaturalIdentity,
    EndpointRevisionRow,
    live_jpy_l2_endpoint_revisions,
    live_jpy_realtime_endpoint_revisions,
    live_jpy_rest_l2_endpoint_revisions,
    validate_endpoint_id,
    validate_endpoint_natural_key,
)
from guvolu.data.store import (
    DB_SCHEMA_VERSION,
    connect,
    register_endpoint_revisions,
)


def _insert_live_venues(conn: sqlite3.Connection) -> None:
    conn.executemany(
        "INSERT INTO venue VALUES (?,?,?,?,?,?)",
        [
            ("gmo", "exchange", "market", "milliseconds", "UTC00", 0),
            ("bitbank", "exchange", "market", "milliseconds", "UTC00", 0),
            ("bitflyer", "exchange", "market", "milliseconds", "UTC00", 0),
        ],
    )


def test_endpoint_natural_key_is_exactly_twelve_dimensions() -> None:
    original = live_jpy_l2_endpoint_revisions()[2]
    equivalent_identity = EndpointNaturalIdentity(
        legal_entity="  GMO   Coin, Inc. ",
        venue_brand="GMO Coin",
        product="Spot/Leverage",
        environment="prod",
        region="Japan",
        transport="WSS",
        protocol="public",
        auth_mode="P0",
        host="API.COIN.Z.COM.",
        port=443,
        base_path_or_channel="/ws/public",
        data_level="L2/trades",
    )
    assert equivalent_identity.natural_key() == original.identity.natural_key()
    assert set(original.identity.canonical_components()) == {
        "legal_entity", "venue_brand", "product", "environment", "region",
        "transport", "protocol", "auth_mode", "host", "port",
        "base_path_or_channel", "data_level",
    }
    changed_revision_attributes = EndpointRevisionRow(
        endpoint_id=original.endpoint_id,
        revision_id=1,
        venue_id=original.venue_id,
        identity=original.identity,
        scope="different-scope-does-not-change-natural-key",
        source_schema_revision="different-schema-revision",
        documentation_uri=original.documentation_uri,
        documentation_sha256=original.documentation_sha256,
        effective_from=original.effective_from,
        valid_until=original.valid_until,
        registered_at=original.registered_at,
    )
    assert (
        changed_revision_attributes.identity.natural_key_sha256()
        == original.identity.natural_key_sha256()
    )
    validate_endpoint_natural_key(
        original.identity,
        original.identity.natural_key(),
        original.identity.natural_key_sha256(),
    )
    with pytest.raises(ValueError, match="does not match"):
        validate_endpoint_natural_key(
            original.identity, original.identity.natural_key(), "0" * 64
        )
    assert validate_endpoint_id("ep-0007") == "EP-0007"
    with pytest.raises(ValueError, match="EP-"):
        validate_endpoint_id("gmo-ws")


def test_rest_anchor_endpoint_ids_match_workbook_twelve_dimensions() -> None:
    rows = live_jpy_rest_l2_endpoint_revisions()
    assert [(row.endpoint_id, row.revision_id, row.venue_id) for row in rows] == [
        ("EP-0001", 0, "bitflyer"),
        ("EP-0003", 0, "bitbank"),
        ("EP-0006", 0, "gmo"),
    ]
    assert [row.identity.canonical_components() for row in rows] == [
        {
            "legal_entity": "bitFlyer, Inc.", "venue_brand": "bitFlyer",
            "product": "Spot/CFD", "environment": "prod",
            "region": "Japan", "transport": "HTTPS", "protocol": "v1",
            "auth_mode": "P0/P2", "host": "api.bitflyer.com", "port": 443,
            "base_path_or_channel": "/v1/",
            "data_level": "L2/trades/private",
        },
        {
            "legal_entity": "bitbank, inc.", "venue_brand": "bitbank",
            "product": "Spot", "environment": "prod", "region": "Japan",
            "transport": "HTTPS", "protocol": "public",
            "auth_mode": "P0", "host": "public.bitbank.cc", "port": 443,
            "base_path_or_channel": "/", "data_level": "L2/trades",
        },
        {
            "legal_entity": "GMO Coin, Inc.", "venue_brand": "GMO Coin",
            "product": "Spot/Leverage", "environment": "prod",
            "region": "Japan", "transport": "HTTPS",
            "protocol": "public", "auth_mode": "P0",
            "host": "api.coin.z.com", "port": 443,
            "base_path_or_channel": "/public", "data_level": "L2/trades",
        },
    ]
    assert all(len(row.identity.canonical_components()) == 12 for row in rows)


def test_registers_live_endpoint_revisions_idempotently_and_rejects_duplicate(
    tmp_path: Path,
) -> None:
    conn = connect(tmp_path)
    _insert_live_venues(conn)
    rows = live_jpy_realtime_endpoint_revisions()
    assert register_endpoint_revisions(conn, rows) == 6
    assert register_endpoint_revisions(conn, rows) == 0
    stored = conn.execute(
        "SELECT endpoint_id, revision_id, venue_brand, base_path_or_channel, data_level, "
        "scope, source_schema_revision FROM endpoint_revision "
        "ORDER BY endpoint_id,revision_id"
    ).fetchall()
    assert stored == [
        (
            "EP-0002", 0, "bitFlyer", "/json-rpc", "L2/trades/private",
            "realtime", "unversioned-realtime-schema@2026-08-12",
        ),
        (
            "EP-0005", 0, "bitbank",
            "/socket.io/?EIO=4&transport=websocket", "L2",
            "depth_whole/depth_diff",
            "socket.io-eio4-depth-schema@2026-08-12",
        ),
        (
            "EP-0005", 1, "bitbank",
            "/socket.io/?EIO=4&transport=websocket", "L2",
            "depth_whole/depth_diff/circuit_break_info",
            "local_registry_extension:circuit_break_info@2026-08-12",
        ),
        (
            "EP-0007", 0, "GMO Coin", "/ws/public", "L2/trades", "public",
            "public-websocket-schema@2026-08-12",
        ),
        (
            "EP-0007", 1, "GMO Coin", "/ws/public", "L2/trades",
            "public/trades:TAKER_ONLY",
            "local_registry_extension:trades-TAKER_ONLY@2026-08-14",
        ),
        (
            "EP-0075", 0, "bitbank",
            "/socket.io/?EIO=4&transport=websocket", "trades", "transactions",
            "local_registry_extension:transactions@2026-08-12",
        ),
    ]
    hashes = conn.execute(
        "SELECT natural_key_sha256 FROM endpoint_revision"
    ).fetchall()
    assert len(hashes) == 6
    assert len(set(hashes)) == 4

    first = rows[0]
    duplicate = EndpointRevisionRow(
        endpoint_id="EP-9999",
        revision_id=0,
        venue_id=first.venue_id,
        identity=first.identity,
        scope=first.scope,
        source_schema_revision=first.source_schema_revision,
        documentation_uri=first.documentation_uri,
        documentation_sha256=first.documentation_sha256,
        effective_from=first.effective_from,
        valid_until=first.valid_until,
        registered_at=first.registered_at,
    )
    with pytest.raises(ValueError, match="conflicts"):
        register_endpoint_revisions(conn, (duplicate,))
    assert conn.execute("SELECT COUNT(*) FROM endpoint_revision").fetchone()[0] == 6
    conn.close()


def test_endpoint_connection_and_channel_foreign_keys(tmp_path: Path) -> None:
    conn = connect(tmp_path)
    _insert_live_venues(conn)
    register_endpoint_revisions(conn, live_jpy_realtime_endpoint_revisions())
    conn.execute(
        "INSERT INTO collection_connection "
        "(connection_id,endpoint_id,endpoint_revision,collection_run_id,"
        "connection_ordinal,opened_at,opened_at_basis,closed_at,close_reason) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        (
            "run-1-c000001", "EP-0007", 0, "run-1", 1,
            "2026-08-12T00:00:00+00:00",
            "first_successfully_materialized_raw_v3_frame", None, None,
        ),
    )
    conn.execute(
        "INSERT INTO collection_channel "
        "(connection_id, channel_id, native_channel, market_id, "
        "subscription_key, subscription_sha256, subscribed_at, "
        "subscribed_at_basis) VALUES (?,?,?,?,?,?,?,?)",
        (
            "run-1-c000001", "orderbooks", "orderbooks", None,
            '{"channel":"orderbooks","symbol":"BTC"}',
            "c" * 64,
            "2026-08-12T00:00:01+00:00",
            "first_successfully_materialized_raw_v3_frame",
        ),
    )
    conn.commit()
    assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO collection_connection "
            "(connection_id,endpoint_id,endpoint_revision,collection_run_id,"
            "connection_ordinal,opened_at,opened_at_basis,closed_at,"
            "close_reason) VALUES (?,?,?,?,?,?,?,?,?)",
            (
                "missing-c1", "EP-9999", 0, "missing", 1,
                "2026-08-12T00:00:00+00:00",
                "first_successfully_materialized_raw_v3_frame", None, None,
            ),
        )
    conn.close()


def test_endpoint_revision_rejects_orphan_venue(tmp_path: Path) -> None:
    conn = connect(tmp_path)
    with pytest.raises(ValueError, match="registry"):
        register_endpoint_revisions(
            conn, (live_jpy_l2_endpoint_revisions()[0],)
        )
    assert conn.execute("SELECT COUNT(*) FROM endpoint_revision").fetchone()[0] == 0
    assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    conn.close()


def _downgrade_to_v17(
    conn: sqlite3.Connection,
    *,
    with_endpoint: bool,
    with_connection: bool,
) -> None:
    """把测试库的空 v18 控制面精确替换为旧 v17 DDL。"""
    conn.execute("DROP TABLE collection_channel")
    conn.execute("DROP TABLE collection_connection")
    conn.execute("DROP TABLE endpoint_revision")
    conn.executescript(
        """
        CREATE TABLE endpoint_revision (
          endpoint_id TEXT NOT NULL, revision_id INTEGER NOT NULL,
          natural_key TEXT NOT NULL, natural_key_sha256 TEXT NOT NULL,
          legal_entity TEXT NOT NULL,
          venue_id TEXT NOT NULL REFERENCES venue(venue_id),
          product TEXT NOT NULL, environment TEXT NOT NULL, region TEXT NOT NULL,
          transport TEXT NOT NULL, protocol TEXT NOT NULL, auth_mode TEXT NOT NULL,
          host TEXT NOT NULL, port INTEGER, path TEXT NOT NULL, channel TEXT NOT NULL,
          source_schema_revision TEXT NOT NULL, documentation_uri TEXT NOT NULL,
          documentation_sha256 TEXT, effective_from TEXT NOT NULL,
          valid_until TEXT NOT NULL, registered_at TEXT NOT NULL,
          PRIMARY KEY (endpoint_id, revision_id)
        );
        CREATE TABLE collection_connection (
          connection_id TEXT PRIMARY KEY, endpoint_id TEXT NOT NULL,
          endpoint_revision INTEGER NOT NULL, collection_run_id TEXT NOT NULL,
          reconnect_ordinal INTEGER NOT NULL, opened_at TEXT NOT NULL,
          closed_at TEXT, close_reason TEXT,
          FOREIGN KEY (endpoint_id, endpoint_revision)
            REFERENCES endpoint_revision(endpoint_id, revision_id)
        );
        CREATE TABLE collection_channel (
          connection_id TEXT NOT NULL REFERENCES collection_connection(connection_id),
          channel_id TEXT NOT NULL, native_channel TEXT NOT NULL,
          market_id TEXT REFERENCES market(market_id), subscription_key TEXT NOT NULL,
          subscription_sha256 TEXT NOT NULL, subscribed_at TEXT NOT NULL,
          unsubscribed_at TEXT, capability_venue_id TEXT, capability_domain TEXT,
          capability_endpoint TEXT, capability_revision INTEGER,
          PRIMARY KEY (connection_id, channel_id)
        );
        """
    )
    if with_endpoint:
        conn.execute(
            "INSERT INTO endpoint_revision VALUES (" + ",".join("?" for _ in range(22)) + ")",
            (
                "EP-0007", 0, '{"wrong":"v17"}', "a" * 64,
                "GMO Coin, Inc.", "gmo", "spot", "production", "jp",
                "websocket", "json", "public", "api.coin.z.com", 443,
                "/ws/public/v1", "orderbooks", "public-ws-v1",
                "https://api.coin.z.com/docs/#ws-public-api", None,
                "2026-08-12T00:00:00+00:00",
                "9999-12-31T23:59:59+00:00",
                "2026-08-12T00:00:00+00:00",
            ),
        )
    if with_connection:
        conn.execute(
            "INSERT INTO collection_connection "
            "(connection_id,endpoint_id,endpoint_revision,collection_run_id,"
            "reconnect_ordinal,opened_at,closed_at,close_reason) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (
                "legacy-c1", "EP-0007", 0, "legacy-run", 0,
                "2026-08-12T01:00:00+00:00", None, None,
            ),
        )
        conn.execute(
            "INSERT INTO collection_channel "
            "(connection_id,channel_id,native_channel,market_id,"
            "subscription_key,subscription_sha256,subscribed_at,"
            "unsubscribed_at,capability_venue_id,capability_domain,"
            "capability_endpoint,capability_revision) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "legacy-c1", "orderbooks", "orderbooks", None, "{}", "b" * 64,
                "2026-08-12T01:00:01+00:00", None, None, None, None, None,
            ),
        )
    conn.execute("PRAGMA user_version=17")
    conn.commit()


def test_v17_unreferenced_wrong_rows_are_archived_then_correct_r0_registered(
    tmp_path: Path,
) -> None:
    conn = connect(tmp_path)
    _insert_live_venues(conn)
    _downgrade_to_v17(conn, with_endpoint=True, with_connection=False)
    conn.close()

    migrated = connect(tmp_path)
    assert migrated.execute("PRAGMA user_version").fetchone()[0] == DB_SCHEMA_VERSION
    assert migrated.execute(
        "SELECT endpoint_id, revision_id FROM endpoint_revision"
    ).fetchall() == []
    assert migrated.execute(
        "SELECT endpoint_id, revision_id, path, channel "
        "FROM endpoint_revision_v17_archive"
    ).fetchall() == [("EP-0007", 0, "/ws/public/v1", "orderbooks")]
    assert register_endpoint_revisions(
        migrated, (live_jpy_l2_endpoint_revisions()[2],)
    ) == 1
    assert migrated.execute(
        "SELECT endpoint_id, revision_id, base_path_or_channel, data_level "
        "FROM endpoint_revision"
    ).fetchall() == [("EP-0007", 0, "/ws/public", "L2/trades")]
    migrated.close()


def test_v17_referenced_rows_are_preserved_and_connections_rebound(
    tmp_path: Path,
) -> None:
    conn = connect(tmp_path)
    _insert_live_venues(conn)
    _downgrade_to_v17(conn, with_endpoint=True, with_connection=True)
    conn.close()

    migrated = connect(tmp_path)
    legacy_revision = 1_000_000_000
    assert migrated.execute(
        "SELECT endpoint_id, revision_id, data_level, scope "
        "FROM endpoint_revision"
    ).fetchall() == [
        (
            "EP-0007", legacy_revision, "legacy-v17-unclassified",
            "legacy-v17 channel=orderbooks",
        )
    ]
    assert migrated.execute(
        "SELECT endpoint_id,endpoint_revision,connection_ordinal,"
        "opened_at_basis FROM collection_connection"
    ).fetchall() == [(
        "EP-0007", legacy_revision, 0,
        "legacy_unqualified_recorded_time",
    )]
    assert migrated.execute(
        "SELECT connection_id,channel_id,subscribed_at_basis "
        "FROM collection_channel"
    ).fetchall() == [(
        "legacy-c1", "orderbooks", "legacy_unqualified_recorded_time",
    )]
    assert migrated.execute("PRAGMA foreign_key_check").fetchall() == []
    migrated.close()


def test_v16_without_endpoint_rows_migrates_without_inventing_bindings(
    tmp_path: Path,
) -> None:
    conn = connect(tmp_path)
    conn.execute("PRAGMA user_version=16")
    conn.commit()
    conn.close()
    migrated = connect(tmp_path)
    assert migrated.execute("PRAGMA user_version").fetchone()[0] == DB_SCHEMA_VERSION
    assert migrated.execute("SELECT COUNT(*) FROM endpoint_revision").fetchone()[0] == 0
    assert migrated.execute("PRAGMA foreign_key_check").fetchall() == []
    migrated.close()


def test_production_registry_registers_workbook_rows_plus_local_extension(
    tmp_path: Path,
) -> None:
    from guvolu.venues.registry import register_all

    conn = connect(tmp_path)
    assert register_all(conn) > 0
    assert register_all(conn) == 0
    assert conn.execute(
        "SELECT endpoint_id, documentation_sha256 FROM endpoint_revision "
        "ORDER BY endpoint_id"
    ).fetchall() == [
        ("EP-0001", None),
        ("EP-0002", None),
        ("EP-0003", None),
        ("EP-0005", None),
        ("EP-0005", None),
            ("EP-0006", None),
            ("EP-0007", None),
            ("EP-0007", None),
            ("EP-0032", None),
        ("EP-0075", None),
    ]
    conn.close()
