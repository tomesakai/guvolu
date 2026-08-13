from __future__ import annotations

from pathlib import Path

import pytest

from guvolu.data import store
from guvolu.data.materialize import ensure_markets
from guvolu.data.realtime_control import (
    OBSERVATION_BASIS,
    RealtimeChannelObservation,
    register_materialized_raw_v3_observations,
)
from guvolu.venues.registry import register_all


def _register(
    conn: object,
    observations: tuple[RealtimeChannelObservation, ...],
    *,
    endpoint_revision: int = 0,
) -> None:
    register_materialized_raw_v3_observations(
        conn,  # type: ignore[arg-type]
        endpoint_id="EP-0007",
        endpoint_revision=endpoint_revision,
        run_id="run-control",
        market_id="mkt__gmo__btc__r0",
        capability_venue_id="gmo",
        capability_domain="book_realtime",
        capability_endpoint="orderbooks/ws",
        capability_revision=0,
        observations=observations,
    )


def test_control_observation_is_cross_segment_idempotent(tmp_path: Path) -> None:
    conn = store.connect(tmp_path)
    try:
        register_all(conn)
        ensure_markets(conn)
        later = RealtimeChannelObservation(
            "run-control-c000001", "orderbooks",
            "2026-08-12T00:00:02+00:00",
        )
        earlier = RealtimeChannelObservation(
            "run-control-c000001", "orderbooks",
            "2026-08-12T00:00:01+00:00",
        )
        _register(conn, (later,))
        conn.commit()
        _register(conn, (later, earlier))
        conn.commit()
        connection = conn.execute(
            "SELECT connection_ordinal,opened_at,opened_at_basis "
            "FROM collection_connection"
        ).fetchone()
        channel = conn.execute(
            "SELECT subscribed_at,subscribed_at_basis,"
            "length(subscription_sha256) FROM collection_channel"
        ).fetchone()
    finally:
        conn.close()
    assert connection == (1, "2026-08-12T00:00:01+00:00", OBSERVATION_BASIS)
    assert channel == ("2026-08-12T00:00:01+00:00", OBSERVATION_BASIS, 64)


def test_control_observation_requires_registered_endpoint_revision(
    tmp_path: Path,
) -> None:
    conn = store.connect(tmp_path)
    try:
        register_all(conn)
        ensure_markets(conn)
        observation = RealtimeChannelObservation(
            "run-control-c000001", "orderbooks",
            "2026-08-12T00:00:01+00:00",
        )
        with pytest.raises(ValueError, match="endpoint revision"):
            _register(conn, (observation,), endpoint_revision=99)
        assert conn.execute(
            "SELECT COUNT(*) FROM collection_connection"
        ).fetchone() == (0,)
    finally:
        conn.close()
