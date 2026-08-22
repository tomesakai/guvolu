"""采集来源端点的稳定身份与修订契约。

``endpoint_id`` 是人工登记、跨修订稳定的工作簿式 ID（例如
``EP-0007``），不是自然键散列。自然键只由工作簿规定的十二个端点维度
组成；``scope`` 与 ``source_schema_revision`` 是可变修订属性，不得借它们
制造新的端点身份。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
import re

_ENDPOINT_ID_PATTERN = re.compile(r"EP-[0-9]{4,}")
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")


def _text(value: str, field: str, *, lower: bool = False) -> str:
    normalized = " ".join(value.strip().split())
    if not normalized:
        raise ValueError(f"{field} must not be empty")
    return normalized.lower() if lower else normalized


def _aware_timestamp(value: str, field: str) -> datetime:
    text = _text(value, field)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field} must include a UTC offset")
    return parsed


def validate_endpoint_id(endpoint_id: str) -> str:
    """验证并返回工作簿式稳定端点 ID。"""
    normalized = endpoint_id.strip().upper()
    if _ENDPOINT_ID_PATTERN.fullmatch(normalized) is None:
        raise ValueError("endpoint_id must match EP- followed by at least four digits")
    return normalized


@dataclass(frozen=True, slots=True)
class EndpointNaturalIdentity:
    """工作簿规定的十二维端点自然身份。

    ``venue_brand`` 是来源文档里的品牌名；内部 ``venue_id`` 是数据库外键，
    属于 :class:`EndpointRevisionRow`，不进入自然键。``base_path_or_channel``
    可以是 URL 路径、频道说明或 FIX/组播通道，因而只要求非空而不强制 ``/``。
    """

    legal_entity: str
    venue_brand: str
    product: str
    environment: str
    region: str
    transport: str
    protocol: str
    auth_mode: str
    host: str
    port: int | None
    base_path_or_channel: str
    data_level: str

    def canonical_components(self) -> dict[str, str | int | None]:
        """返回可跨进程、跨平台复算的十二个规范分量。"""
        if isinstance(self.port, bool) or (
            self.port is not None and not 1 <= self.port <= 65535
        ):
            raise ValueError("port must be between 1 and 65535")
        return {
            "auth_mode": _text(self.auth_mode, "auth_mode"),
            "base_path_or_channel": _text(
                self.base_path_or_channel, "base_path_or_channel"
            ),
            "data_level": _text(self.data_level, "data_level"),
            "environment": _text(self.environment, "environment"),
            "host": _text(self.host.strip().rstrip("."), "host", lower=True),
            "legal_entity": _text(self.legal_entity, "legal_entity"),
            "port": self.port,
            "product": _text(self.product, "product"),
            "protocol": _text(self.protocol, "protocol"),
            "region": _text(self.region, "region"),
            "transport": _text(self.transport, "transport"),
            "venue_brand": _text(self.venue_brand, "venue_brand"),
        }

    def natural_key(self) -> str:
        """返回 UTF-8 JSON 规范自然键；不可作为稳定 ``endpoint_id``。"""
        return json.dumps(
            self.canonical_components(),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )

    def natural_key_sha256(self) -> str:
        """返回规范自然键的裸十六进制 SHA-256。"""
        return hashlib.sha256(self.natural_key().encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class EndpointRevisionRow:
    """可追加登记的一版端点证据。"""

    endpoint_id: str
    revision_id: int
    venue_id: str
    identity: EndpointNaturalIdentity
    scope: str
    source_schema_revision: str
    documentation_uri: str
    documentation_sha256: str | None
    effective_from: str
    valid_until: str
    registered_at: str

    def validate(self) -> None:
        validate_endpoint_id(self.endpoint_id)
        if self.revision_id < 0:
            raise ValueError("revision_id must be non-negative")
        _text(self.venue_id, "venue_id", lower=True)
        self.identity.canonical_components()
        _text(self.scope, "scope")
        _text(self.source_schema_revision, "source_schema_revision")
        _text(self.documentation_uri, "documentation_uri")
        if (
            self.documentation_sha256 is not None
            and _SHA256_PATTERN.fullmatch(self.documentation_sha256) is None
        ):
            raise ValueError("documentation_sha256 must be lowercase SHA-256")
        effective = _aware_timestamp(self.effective_from, "effective_from")
        valid = _aware_timestamp(self.valid_until, "valid_until")
        if valid <= effective:
            raise ValueError("valid_until must be after effective_from")
        _aware_timestamp(self.registered_at, "registered_at")

    def as_db_row(self) -> tuple[object, ...]:
        """按 v18 ``endpoint_revision`` 表声明顺序返回不可变行。"""
        self.validate()
        components = self.identity.canonical_components()
        return (
            validate_endpoint_id(self.endpoint_id),
            self.revision_id,
            self.identity.natural_key(),
            self.identity.natural_key_sha256(),
            components["legal_entity"],
            components["venue_brand"],
            _text(self.venue_id, "venue_id", lower=True),
            components["product"],
            components["environment"],
            components["region"],
            components["transport"],
            components["protocol"],
            components["auth_mode"],
            components["host"],
            components["port"],
            components["base_path_or_channel"],
            components["data_level"],
            _text(self.scope, "scope"),
            _text(self.source_schema_revision, "source_schema_revision"),
            self.documentation_uri.strip(),
            self.documentation_sha256,
            self.effective_from.strip(),
            self.valid_until.strip(),
            self.registered_at.strip(),
        )


def validate_endpoint_natural_key(
    identity: EndpointNaturalIdentity,
    natural_key: str,
    natural_key_sha256: str,
) -> None:
    """核验持久化自然键及散列可由十二个分量精确复算。"""
    if natural_key != identity.natural_key():
        raise ValueError("endpoint natural_key does not match its components")
    if natural_key_sha256 != identity.natural_key_sha256():
        raise ValueError("endpoint natural_key_sha256 does not match natural_key")


def live_jpy_realtime_endpoint_revisions() -> tuple[EndpointRevisionRow, ...]:
    """返回现行三所实时公共数据采集使用的端点修订。

    EP-0002、EP-0005、EP-0007 r0 逐字段来自 2026-08-12 工作簿。工作簿把
    bitbank 的同一 Socket.IO 地址只登记为 L2，因此本地以 EP-0075 追加
    ``transactions`` 的 trades 身份；另以 EP-0005 r1 表达同一稳定 WSS
    身份新增 ``circuit_break_info`` 频道。两者的 ``source_schema_revision``
    明示本地扩展，不伪装成工作簿原行。EP-0007 r1 仅增加成交频道的
    ``TAKER_ONLY`` 订阅约束。EP-0005 与 EP-0007 的 r0 保持不可变；修订
    有效期有意重叠，因为只能由 raw 行显式绑定，绝不按时间猜测。
    """
    observed = "2026-08-12T00:00:00+00:00"
    valid_until = "9999-12-31T23:59:59+00:00"
    return (
        EndpointRevisionRow(
            endpoint_id="EP-0002",
            revision_id=0,
            venue_id="bitflyer",
            identity=EndpointNaturalIdentity(
                legal_entity="bitFlyer, Inc.",
                venue_brand="bitFlyer",
                product="Spot/CFD",
                environment="prod",
                region="Japan",
                transport="WSS",
                protocol="JSON-RPC",
                auth_mode="P0/P2",
                host="ws.lightstream.bitflyer.com",
                port=443,
                base_path_or_channel="/json-rpc",
                data_level="L2/trades/private",
            ),
            scope="realtime",
            source_schema_revision="unversioned-realtime-schema@2026-08-12",
            documentation_uri="https://lightning.bitflyer.com/docs",
            documentation_sha256=None,
            effective_from=observed,
            valid_until=valid_until,
            registered_at=observed,
        ),
        EndpointRevisionRow(
            endpoint_id="EP-0005",
            revision_id=0,
            venue_id="bitbank",
            identity=EndpointNaturalIdentity(
                legal_entity="bitbank, inc.",
                venue_brand="bitbank",
                product="Spot",
                environment="prod",
                region="Japan",
                transport="WSS",
                protocol="Socket.IO EIO=4",
                auth_mode="P0",
                host="stream.bitbank.cc",
                port=443,
                base_path_or_channel=(
                    "/socket.io/?EIO=4&transport=websocket"
                ),
                data_level="L2",
            ),
            scope="depth_whole/depth_diff",
            source_schema_revision="socket.io-eio4-depth-schema@2026-08-12",
            documentation_uri="https://github.com/bitbankinc/bitbank-api-docs",
            documentation_sha256=None,
            effective_from=observed,
            valid_until=valid_until,
            registered_at=observed,
        ),
        EndpointRevisionRow(
            endpoint_id="EP-0005",
            revision_id=1,
            venue_id="bitbank",
            identity=EndpointNaturalIdentity(
                legal_entity="bitbank, inc.",
                venue_brand="bitbank",
                product="Spot",
                environment="prod",
                region="Japan",
                transport="WSS",
                protocol="Socket.IO EIO=4",
                auth_mode="P0",
                host="stream.bitbank.cc",
                port=443,
                base_path_or_channel=(
                    "/socket.io/?EIO=4&transport=websocket"
                ),
                data_level="L2",
            ),
            scope="depth_whole/depth_diff/circuit_break_info",
            source_schema_revision=(
                "local_registry_extension:circuit_break_info@2026-08-12"
            ),
            documentation_uri=(
                "https://github.com/bitbankinc/bitbank-api-docs/"
                "blob/master/public-stream.md"
            ),
            documentation_sha256=None,
            effective_from="2026-08-12T14:49:04+00:00",
            valid_until=valid_until,
            registered_at="2026-08-12T14:49:04+00:00",
        ),
        EndpointRevisionRow(
            endpoint_id="EP-0007",
            revision_id=0,
            venue_id="gmo",
            identity=EndpointNaturalIdentity(
                legal_entity="GMO Coin, Inc.",
                venue_brand="GMO Coin",
                product="Spot/Leverage",
                environment="prod",
                region="Japan",
                transport="WSS",
                protocol="public",
                auth_mode="P0",
                host="api.coin.z.com",
                port=443,
                base_path_or_channel="/ws/public",
                data_level="L2/trades",
            ),
            scope="public",
            source_schema_revision="public-websocket-schema@2026-08-12",
            documentation_uri="https://api.coin.z.com/docs/",
            documentation_sha256=None,
            effective_from=observed,
            valid_until=valid_until,
            registered_at=observed,
        ),
        EndpointRevisionRow(
            endpoint_id="EP-0007",
            revision_id=1,
            venue_id="gmo",
            identity=EndpointNaturalIdentity(
                legal_entity="GMO Coin, Inc.",
                venue_brand="GMO Coin",
                product="Spot/Leverage",
                environment="prod",
                region="Japan",
                transport="WSS",
                protocol="public",
                auth_mode="P0",
                host="api.coin.z.com",
                port=443,
                base_path_or_channel="/ws/public",
                data_level="L2/trades",
            ),
            scope="public/trades:TAKER_ONLY",
            source_schema_revision=(
                "local_registry_extension:trades-TAKER_ONLY@2026-08-14"
            ),
            documentation_uri="https://api.coin.z.com/docs/",
            documentation_sha256=None,
            effective_from="2026-08-14T00:00:00+00:00",
            valid_until=valid_until,
            registered_at="2026-08-14T00:00:00+00:00",
        ),
        EndpointRevisionRow(
            endpoint_id="EP-0075",
            revision_id=0,
            venue_id="bitbank",
            identity=EndpointNaturalIdentity(
                legal_entity="bitbank, inc.",
                venue_brand="bitbank",
                product="Spot",
                environment="prod",
                region="Japan",
                transport="WSS",
                protocol="Socket.IO EIO=4",
                auth_mode="P0",
                host="stream.bitbank.cc",
                port=443,
                base_path_or_channel=(
                    "/socket.io/?EIO=4&transport=websocket"
                ),
                data_level="trades",
            ),
            scope="transactions",
            source_schema_revision=(
                "local_registry_extension:transactions@2026-08-12"
            ),
            documentation_uri="https://github.com/bitbankinc/bitbank-api-docs",
            documentation_sha256=None,
            effective_from=observed,
            valid_until=valid_until,
            registered_at=observed,
        ),
    )


def live_jpy_rest_l2_endpoint_revisions() -> tuple[EndpointRevisionRow, ...]:
    """返回工作簿登记的三所公开 HTTPS 端点原始修订。"""
    observed = "2026-08-12T00:00:00+00:00"
    valid_until = "9999-12-31T23:59:59+00:00"
    return (
        EndpointRevisionRow(
            endpoint_id="EP-0001", revision_id=0, venue_id="bitflyer",
            identity=EndpointNaturalIdentity(
                legal_entity="bitFlyer, Inc.", venue_brand="bitFlyer",
                product="Spot/CFD", environment="prod", region="Japan",
                transport="HTTPS", protocol="v1", auth_mode="P0/P2",
                host="api.bitflyer.com", port=443,
                base_path_or_channel="/v1/",
                data_level="L2/trades/private",
            ),
            scope="public+private",
            source_schema_revision="unversioned-v1-schema@2026-08-12",
            documentation_uri="https://lightning.bitflyer.com/docs",
            documentation_sha256=None, effective_from=observed,
            valid_until=valid_until, registered_at=observed,
        ),
        EndpointRevisionRow(
            endpoint_id="EP-0003", revision_id=0, venue_id="bitbank",
            identity=EndpointNaturalIdentity(
                legal_entity="bitbank, inc.", venue_brand="bitbank",
                product="Spot", environment="prod", region="Japan",
                transport="HTTPS", protocol="public", auth_mode="P0",
                host="public.bitbank.cc", port=443,
                base_path_or_channel="/", data_level="L2/trades",
            ),
            scope="public",
            source_schema_revision="unversioned-public-schema@2026-08-12",
            documentation_uri=(
                "https://github.com/bitbankinc/bitbank-api-docs"
            ),
            documentation_sha256=None, effective_from=observed,
            valid_until=valid_until, registered_at=observed,
        ),
        EndpointRevisionRow(
            endpoint_id="EP-0006", revision_id=0, venue_id="gmo",
            identity=EndpointNaturalIdentity(
                legal_entity="GMO Coin, Inc.", venue_brand="GMO Coin",
                product="Spot/Leverage", environment="prod", region="Japan",
                transport="HTTPS", protocol="public", auth_mode="P0",
                host="api.coin.z.com", port=443,
                base_path_or_channel="/public", data_level="L2/trades",
            ),
            scope="public",
            source_schema_revision="public-rest-schema@2026-08-12",
            documentation_uri="https://api.coin.z.com/docs/",
            documentation_sha256=None, effective_from=observed,
            valid_until=valid_until, registered_at=observed,
        ),
    )


def live_jpy_l2_endpoint_revisions() -> tuple[EndpointRevisionRow, ...]:
    """兼容入口：只返回三所工作簿原始 L2 修订，不含本地扩展。"""
    return tuple(
        row
        for row in live_jpy_realtime_endpoint_revisions()
        if row.endpoint_id != "EP-0075"
        and not (row.endpoint_id == "EP-0005" and row.revision_id == 1)
    )


def okx_live_endpoint_revisions() -> tuple[EndpointRevisionRow, ...]:
    """Return the workbook-defined OKX public WSS endpoint identity."""
    observed = "2026-08-12T00:00:00+00:00"
    return (
        EndpointRevisionRow(
            endpoint_id="EP-0032",
            revision_id=0,
            venue_id="okx",
            identity=EndpointNaturalIdentity(
                legal_entity="OKX",
                venue_brand="OKX",
                product="All",
                environment="prod",
                region="global",
                transport="WSS",
                protocol="v5 public",
                auth_mode="P0/P3",
                host="ws.okx.com",
                port=8443,
                base_path_or_channel="/ws/v5/public",
                data_level="L2",
            ),
            scope="market data",
            source_schema_revision="okx-v5-public-books-schema@2026-08-13",
            documentation_uri="https://www.okx.com/docs-v5/en/",
            documentation_sha256=None,
            effective_from=observed,
            valid_until="9999-12-31T23:59:59+00:00",
            registered_at="2026-08-13T00:00:00+00:00",
        ),
    )


def registered_realtime_endpoint_revisions() -> tuple[EndpointRevisionRow, ...]:
    """返回当前采集与 REST 锚点实际使用的端点修订。"""
    return (
        *live_jpy_realtime_endpoint_revisions(),
        *live_jpy_rest_l2_endpoint_revisions(),
        *okx_live_endpoint_revisions(),
    )
