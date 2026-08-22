"""决策级经济成交资格合同。

方向标签本身不是成交语义证明。特别是 GMO 的旧逐笔端点会同时发送
成交双方；这些观察可以保留价格和时钟信息，但不能被当作经济成交量。
"""
from __future__ import annotations

from collections.abc import Collection, Mapping


TRADE_FLOW_INPUT_METHOD_VERSION = "economic-trade-basis-v1"
GMO_ARCHIVE_NORMALIZATION_VERSION = "trade-normalization-v1"
GMO_REALTIME_NORMALIZATION_VERSION = "trade-realtime-normalization-v4"
GMO_TAKER_ENDPOINT_ID = "EP-0007"
GMO_TAKER_ENDPOINT_REVISION = 1
GMO_TAKER_RAW_SCHEMA_VERSION = 3


def economic_trade_qualified(row: Mapping[str, object]) -> bool:
    """按物理行血缘判断该观察是否代表一笔经济成交。"""
    basis = str(row.get("source_side_basis") or "")
    venue = str(row.get("venue_id") or "")
    if venue != "gmo":
        return basis.startswith("taker") or (
            venue == "coinbase" and basis == "maker"
        )
    normalization = str(row.get("normalization_version") or "")
    if normalization == GMO_ARCHIVE_NORMALIZATION_VERSION:
        return basis.startswith("taker")
    return (
        normalization == GMO_REALTIME_NORMALIZATION_VERSION
        and row.get("raw_schema_version") == GMO_TAKER_RAW_SCHEMA_VERSION
        and row.get("endpoint_id") == GMO_TAKER_ENDPOINT_ID
        and row.get("endpoint_revision") == GMO_TAKER_ENDPOINT_REVISION
        and basis == "taker"
    )


def economic_trade_qualification_sql(
    venue_id: str,
    columns: Collection[str],
    control_contract: tuple[str, str] | None = None,
) -> str:
    """返回与 :func:`economic_trade_qualified` 等价的 DuckDB 谓词。"""
    available = set(columns)
    if "source_side_basis" not in available or "venue_id" not in available:
        return "FALSE"
    venue_literal = venue_id.replace("'", "''")

    def bound(predicate: str) -> str:
        return f"(venue_id='{venue_literal}' AND ({predicate}))"

    if venue_id != "gmo":
        maker = " OR source_side_basis='maker'" if venue_id == "coinbase" else ""
        return bound(f"source_side_basis LIKE 'taker%'{maker}")
    if "normalization_version" not in available:
        return "FALSE"
    archive = (
        "(normalization_version='trade-normalization-v1' "
        "AND source_side_basis LIKE 'taker%')"
    )
    if control_contract == ("trade", GMO_ARCHIVE_NORMALIZATION_VERSION):
        return bound(archive)
    if control_contract is not None and control_contract != (
        "trade_realtime", GMO_REALTIME_NORMALIZATION_VERSION,
    ):
        return "FALSE"
    realtime_fields = {
        "raw_schema_version", "endpoint_id", "endpoint_revision",
    }
    if not realtime_fields.issubset(available):
        if control_contract == (
            "trade_realtime", GMO_REALTIME_NORMALIZATION_VERSION,
        ):
            return "FALSE"
        return bound(archive)
    realtime = (
        "(normalization_version='trade-realtime-normalization-v4' "
        "AND raw_schema_version=3 AND endpoint_id='EP-0007' "
        "AND endpoint_revision=1 AND source_side_basis='taker')"
    )
    if control_contract == ("trade_realtime", GMO_REALTIME_NORMALIZATION_VERSION):
        return bound(realtime)
    return bound(f"({archive} OR {realtime})")
