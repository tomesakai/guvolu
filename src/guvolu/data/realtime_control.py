"""成功物化的 raw v3 连接与频道观察登记。"""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from dataclasses import dataclass


OBSERVATION_BASIS = "first_successfully_materialized_raw_v3_frame"
_CONNECTION = re.compile(r"^(?P<run>.+)-c(?P<ordinal>[0-9]{6})$")


@dataclass(frozen=True)
class RealtimeChannelObservation:
    """一个已通过 raw、payload 与事实校验的数据频道观察。"""

    connection_id: str
    channel_id: str
    received_at: str


def _connection_parts(connection_id: str, run_id: str) -> tuple[str, int]:
    match = _CONNECTION.fullmatch(connection_id)
    if match is None or match.group("run") != run_id:
        raise ValueError("connection_id 与 collection_run_id 不一致")
    ordinal = int(match.group("ordinal"))
    if ordinal <= 0:
        raise ValueError("connection_ordinal 必须为正数")
    return run_id, ordinal


def register_materialized_raw_v3_observations(
    conn: sqlite3.Connection,
    *,
    endpoint_id: str,
    endpoint_revision: int,
    run_id: str,
    market_id: str,
    capability_venue_id: str,
    capability_domain: str,
    capability_endpoint: str,
    capability_revision: int,
    observations: tuple[RealtimeChannelObservation, ...],
) -> None:
    """在事实完成事务内幂等登记首个成功物化的数据帧。"""
    endpoint = conn.execute(
        "SELECT 1 FROM endpoint_revision WHERE endpoint_id=? AND revision_id=?",
        (endpoint_id, endpoint_revision),
    ).fetchone()
    if endpoint is None:
        raise ValueError(
            f"raw v3 endpoint revision 未登记: {endpoint_id} r{endpoint_revision}"
        )
    earliest: dict[tuple[str, str], str] = {}
    for observation in observations:
        _connection_parts(observation.connection_id, run_id)
        key = (observation.connection_id, observation.channel_id)
        earliest[key] = min(earliest.get(key, observation.received_at), observation.received_at)
    for (connection_id, channel_id), received_at in sorted(earliest.items()):
        _, ordinal = _connection_parts(connection_id, run_id)
        existing_connection = conn.execute(
            "SELECT endpoint_id,endpoint_revision,collection_run_id,"
            "connection_ordinal,opened_at_basis FROM collection_connection "
            "WHERE connection_id=?",
            (connection_id,),
        ).fetchone()
        expected_connection = (
            endpoint_id, endpoint_revision, run_id, ordinal, OBSERVATION_BASIS,
        )
        if (
            existing_connection is not None
            and tuple(existing_connection) != expected_connection
        ):
            raise ValueError(f"连接控制面身份冲突: {connection_id}")
        conn.execute(
            "INSERT INTO collection_connection "
            "(connection_id,endpoint_id,endpoint_revision,collection_run_id,"
            "connection_ordinal,opened_at,opened_at_basis,closed_at,close_reason) "
            "VALUES (?,?,?,?,?,?,?,NULL,NULL) "
            "ON CONFLICT(connection_id) DO UPDATE SET opened_at="
            "MIN(collection_connection.opened_at,excluded.opened_at) "
            "WHERE collection_connection.endpoint_id=excluded.endpoint_id "
            "AND collection_connection.endpoint_revision=excluded.endpoint_revision "
            "AND collection_connection.collection_run_id=excluded.collection_run_id "
            "AND collection_connection.connection_ordinal="
            "excluded.connection_ordinal "
            "AND collection_connection.opened_at_basis=excluded.opened_at_basis",
            (
                connection_id, endpoint_id, endpoint_revision, run_id,
                ordinal, received_at, OBSERVATION_BASIS,
            ),
        )
        subscription = json.dumps(
            {
                "channel_id": channel_id,
                "endpoint_id": endpoint_id,
                "endpoint_revision": endpoint_revision,
                "market_id": market_id,
            },
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        subscription_sha = hashlib.sha256(subscription.encode("utf-8")).hexdigest()
        existing_channel = conn.execute(
            "SELECT native_channel,market_id,subscription_key,"
            "subscription_sha256,subscribed_at_basis,capability_venue_id,"
            "capability_domain,capability_endpoint,capability_revision "
            "FROM collection_channel WHERE connection_id=? AND channel_id=?",
            (connection_id, channel_id),
        ).fetchone()
        expected_channel = (
            channel_id, market_id, subscription, subscription_sha,
            OBSERVATION_BASIS, capability_venue_id, capability_domain,
            capability_endpoint, capability_revision,
        )
        if existing_channel is not None and tuple(existing_channel) != expected_channel:
            raise ValueError(f"频道控制面身份冲突: {connection_id}/{channel_id}")
        conn.execute(
            "INSERT INTO collection_channel "
            "(connection_id,channel_id,native_channel,market_id,"
            "subscription_key,subscription_sha256,subscribed_at,"
            "subscribed_at_basis,unsubscribed_at,capability_venue_id,"
            "capability_domain,capability_endpoint,capability_revision) "
            "VALUES (?,?,?,?,?,?,?, ?,NULL,?,?,?,?) "
            "ON CONFLICT(connection_id,channel_id) DO UPDATE SET subscribed_at="
            "MIN(collection_channel.subscribed_at,excluded.subscribed_at) "
            "WHERE collection_channel.native_channel=excluded.native_channel "
            "AND collection_channel.market_id=excluded.market_id "
            "AND collection_channel.subscription_key=excluded.subscription_key "
            "AND collection_channel.subscription_sha256=excluded.subscription_sha256 "
            "AND collection_channel.subscribed_at_basis=excluded.subscribed_at_basis "
            "AND collection_channel.capability_venue_id="
            "excluded.capability_venue_id "
            "AND collection_channel.capability_domain=excluded.capability_domain "
            "AND collection_channel.capability_endpoint="
            "excluded.capability_endpoint "
            "AND collection_channel.capability_revision="
            "excluded.capability_revision",
            (
                connection_id, channel_id, channel_id, market_id,
                subscription, subscription_sha, received_at,
                OBSERVATION_BASIS, capability_venue_id, capability_domain,
                capability_endpoint, capability_revision,
            ),
        )
