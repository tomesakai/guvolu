"""同报价币种、同现货品种的只读跨交易所盘口顶层聚合。"""
from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

from guvolu.ui.materialized_query import MaterializedQuery, MaterializedQueryError
from guvolu.ui.query_catalog import ActiveOutputSnapshot

CROSS_VENUE_SCHEMA_VERSION = 1
CROSS_VENUE_METHOD_VERSION = "consolidated-top-v1"
DEFAULT_MAX_AGE_SECONDS = 12 * 60

_HARD_QUALITY_REASONS = frozenset({
    "sequence_duplicate_same_connection_channel",
    "sequence_regression_same_connection_channel",
    "checksum_failed",
    "delta_before_connection_snapshot",
    "snapshot_anchor_identity_unknown",
    "source_data_quality_untrusted",
    "connection_or_channel_identity_unknown",
})


class CrossVenueQueryError(ValueError):
    """跨所输入或来源事实不能安全解释。"""


class CrossVenueCompatibilityError(CrossVenueQueryError):
    """请求的市场集合不满足无换汇聚合边界。"""


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise CrossVenueQueryError("decision_time 必须带时区")
    return value.astimezone(UTC)


def _time(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _decimal(value: object) -> Decimal | None:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return result if result.is_finite() else None


def _median(values: Iterable[Decimal]) -> Decimal:
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2


def _identifier(value: object) -> str:
    body = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return "sha256-" + hashlib.sha256(body.encode("utf-8")).hexdigest()


def _split_snapshot(
    snapshot: ActiveOutputSnapshot,
    domain: str,
) -> ActiveOutputSnapshot:
    return ActiveOutputSnapshot(
        market=snapshot.market,
        outputs=tuple(row for row in snapshot.outputs if row.domain == domain),
        head_generation=snapshot.head_generation,
    )


def _active_l2_identity(snapshot: ActiveOutputSnapshot) -> dict[str, list[str]]:
    l2_outputs = [row for row in snapshot.outputs if row.domain == "book_l2"]
    return {
        "attempt_ids": sorted({
            row.attempt_id for row in l2_outputs
            if row.dataset == "book_l2_frame"
        }),
        "artifact_ids": sorted({row.artifact_id for row in l2_outputs}),
        "normalization_versions": sorted({
            row.normalization_version for row in l2_outputs
        }),
    }


def _quality_rejections(
    quality: dict[str, Any], active_attempt_ids: list[str],
) -> tuple[list[str], list[str]]:
    hard: list[str] = []
    source_attempts = sorted(str(item) for item in quality.get(
        "source_attempt_ids", [],
    ))
    if source_attempts != active_attempt_ids:
        hard.append("quality_source_attempts_do_not_match_active_heads")
    if int(quality.get("source_attempt_count") or 0) != len(source_attempts):
        hard.append("quality_source_attempt_count_invalid")
    status = str(quality.get("status", "unknown"))
    if status == "unknown":
        hard.append("quality_window_unavailable")
    elif status == "failed":
        hard.append("quality_status_failed")
    if str(quality.get("materialized_freshness_status")) == "not_applicable":
        hard.append("historical_freshness_not_applicable_to_latest_aggregate")
    reasons = [str(item) for item in quality.get("reasons", [])]
    hard.extend(reason for reason in reasons if reason in _HARD_QUALITY_REASONS)
    numeric_hard = {
        "sequence_duplicates": "sequence_duplicates_observed",
        "sequence_regressions": "sequence_regressions_observed",
        "checksum_failures": "checksum_failures_observed",
        "unanchored_before_snapshot_frames": "unanchored_frames_observed",
        "anchor_unknown_frames": "anchor_unknown_frames_observed",
    }
    for field, reason in numeric_hard.items():
        value = quality.get(field)
        if isinstance(value, (int, float)) and value > 0:
            hard.append(reason)
    soft = [
        reason for reason in reasons
        if reason not in _HARD_QUALITY_REASONS
    ]
    return sorted(set(hard)), sorted(set(soft))


class CrossVenueQuery:
    """基于同一控制面快照构造 synthetic consolidated top。"""

    def __init__(self, materialized: MaterializedQuery) -> None:
        self.materialized = materialized
        self.catalog = materialized.catalog

    def latest_top(
        self,
        market_ids: Iterable[str],
        *,
        decision_time: datetime | None = None,
        min_quorum: int = 2,
        max_age_seconds: int = DEFAULT_MAX_AGE_SECONDS,
    ) -> dict[str, Any]:
        requested = tuple(sorted(str(item) for item in market_ids))
        if len(requested) < 2:
            raise CrossVenueCompatibilityError("跨所聚合至少需要两个 market_id")
        if len(set(requested)) != len(requested):
            raise CrossVenueCompatibilityError("market_id 不得重复")
        if min_quorum < 1 or min_quorum > len(requested):
            raise CrossVenueCompatibilityError("min_quorum 超出市场集合")
        if max_age_seconds <= 0:
            raise CrossVenueCompatibilityError("max_age_seconds 必须为正数")
        decided = _utc(decision_time or datetime.now(UTC))
        frozen = self.catalog.active_outputs_many(
            requested,
            domains=("book_l2", "book_state"),
            datasets=(
                "book_l2_frame", "book_l2_level", "book_state_checkpoint",
            ),
            decision_time=decided,
        )
        markets = [snapshot.market for snapshot in frozen.markets]
        first = markets[0]
        identity = (
            first["instrument_id"], first["base_currency"], first["quote_currency"],
            first["instrument_kind"], first["market_kind"],
        )
        if any((
            market["instrument_id"], market["base_currency"],
            market["quote_currency"],
            market["instrument_kind"], market["market_kind"],
        ) != identity for market in markets[1:]):
            raise CrossVenueCompatibilityError(
                "无 FX 转换时只能聚合同 base/quote/instrument/market kind；"
                "JPY 与 USDT 不可混合"
            )
        venues = [str(market["venue_id"]) for market in markets]
        if len(set(venues)) != len(venues):
            raise CrossVenueCompatibilityError("跨所集合不得重复 venue_id")

        contributors: list[dict[str, Any]] = []
        excluded: list[dict[str, Any]] = []
        for snapshot in frozen.markets:
            market_id = str(snapshot.market["market_id"])
            identity_lineage = _active_l2_identity(snapshot)
            quality = frozen.quality_for(market_id)
            quality_attempts = sorted(
                str(item) for item in quality.get("source_attempt_ids", [])
            )
            hard, soft = _quality_rejections(
                quality, identity_lineage["attempt_ids"],
            )
            common = {
                "market_id": market_id,
                "venue_id": str(snapshot.market["venue_id"]),
                "venue_symbol": str(snapshot.market["venue_symbol"]),
                "head_generation": snapshot.head_generation,
                "source_attempt_count": len(identity_lineage["attempt_ids"]),
                "source_attempt_set_hash": _identifier(
                    identity_lineage["attempt_ids"]
                ),
                "source_artifact_count": len(identity_lineage["artifact_ids"]),
                "source_artifact_set_hash": _identifier(
                    identity_lineage["artifact_ids"]
                ),
                "normalization_versions": identity_lineage[
                    "normalization_versions"
                ],
                "quality_version": quality.get("quality_version"),
                "quality_window_start": quality.get("window_start"),
                "quality_computed_at": quality.get("computed_at"),
                "quality_source_attempt_count": len(quality_attempts),
                "quality_source_attempt_set_hash": _identifier(quality_attempts),
            }
            if hard:
                excluded.append({**common, "reasons": hard})
                continue
            l2_snapshot = _split_snapshot(snapshot, "book_l2")
            checkpoint_snapshot = _split_snapshot(snapshot, "book_state")
            try:
                book, _ = self.materialized.latest_l2_from_snapshot(
                    l2_snapshot,
                    1,
                    decision_time=decided,
                    checkpoint_snapshot=checkpoint_snapshot,
                    use_cache=False,
                )
            except (MaterializedQueryError, FileNotFoundError) as exc:
                excluded.append({
                    **common,
                    "reasons": ["l2_state_unavailable"],
                    "detail": str(exc),
                })
                continue
            bid = _decimal(book.get("best_bid"))
            ask = _decimal(book.get("best_ask"))
            bids = book.get("bids")
            asks = book.get("asks")
            bid_size = _decimal(
                bids[0].get("size")
                if isinstance(bids, list) and bids and isinstance(bids[0], dict)
                else None
            )
            ask_size = _decimal(
                asks[0].get("size")
                if isinstance(asks, list) and asks and isinstance(asks[0], dict)
                else None
            )
            meta = book.get("meta")
            if not isinstance(meta, dict):
                meta = {}
            available = _time(meta.get("as_of_available_time"))
            event = _time(meta.get("as_of_event_time"))
            state_reasons: list[str] = []
            if (
                bid is None or ask is None or bid_size is None or ask_size is None
                or bid <= 0 or ask <= 0 or bid_size <= 0 or ask_size <= 0
            ):
                state_reasons.append("invalid_or_empty_top_of_book")
            age_seconds: float | None = None
            if available is None:
                state_reasons.append("as_of_available_time_invalid")
            else:
                age_seconds = (decided - available).total_seconds()
                if age_seconds < 0:
                    state_reasons.append("available_time_after_decision_time")
                elif age_seconds > max_age_seconds:
                    state_reasons.append("materialized_state_stale_at_decision")
            if "untrusted" in str(meta.get("integrity_mode", "")).casefold():
                state_reasons.append("l2_state_integrity_untrusted")
            if state_reasons:
                excluded.append({
                    **common,
                    "reasons": sorted(set(state_reasons)),
                    "as_of_available_time": (
                        available.isoformat() if available is not None else None
                    ),
                    "age_seconds": age_seconds,
                })
                continue
            assert bid is not None and ask is not None
            assert bid_size is not None and ask_size is not None
            assert available is not None and age_seconds is not None
            mid = (bid + ask) / 2
            contributor_quality = "degraded" if soft else "ok"
            contributors.append({
                **common,
                "accepted": True,
                "quality_state": contributor_quality,
                "quality_reasons": soft,
                "bid": format(bid, "f"),
                "bid_size": format(bid_size, "f"),
                "ask": format(ask, "f"),
                "ask_size": format(ask_size, "f"),
                "mid": format(mid, "f"),
                "as_of_event_time": event.isoformat() if event else None,
                "as_of_available_time": available.isoformat(),
                "age_seconds": age_seconds,
                "state_source": meta.get("state_source"),
                "as_of_frame_id": meta.get("as_of_frame_id"),
                "snapshot_frame_id": meta.get("snapshot_frame_id"),
                "as_of_source_attempt_id": meta.get("source_attempt_id"),
                "as_of_source_artifact_id": meta.get("source_artifact_id"),
                "state_attempt_id": meta.get("state_attempt_id"),
                "state_artifact_id": meta.get("state_artifact_id"),
                "integrity_mode": meta.get("integrity_mode"),
            })

        contributor_count = len(contributors)
        quorum_met = contributor_count >= min_quorum
        bbo: dict[str, Any] | None = None
        reference: dict[str, Any] | None = None
        max_age: float | None = None
        max_skew: float | None = None
        if contributors:
            best_bid = max(Decimal(str(item["bid"])) for item in contributors)
            best_ask = min(Decimal(str(item["ask"])) for item in contributors)
            bid_contributors = sorted(
                str(item["market_id"]) for item in contributors
                if Decimal(str(item["bid"])) == best_bid
            )
            ask_contributors = sorted(
                str(item["market_id"]) for item in contributors
                if Decimal(str(item["ask"])) == best_ask
            )
            spread = best_ask - best_bid
            bbo = {
                "bid": format(best_bid, "f"),
                "ask": format(best_ask, "f"),
                "spread": format(spread, "f"),
                "crossed": spread < 0,
                "bid_contributors": bid_contributors,
                "ask_contributors": ask_contributors,
            }
            mids = [Decimal(str(item["mid"])) for item in contributors]
            median_mid = _median(mids)
            mad = _median(abs(value - median_mid) for value in mids)
            dispersion = max(mids) - min(mids)
            robustness = (
                "strong" if len(mids) >= 3
                else "weak" if len(mids) == 2 else "single_fallback"
            )
            reference = {
                "price": format(median_mid, "f"),
                "estimator": (
                    "equal_venue_median_mid"
                    if len(mids) >= 2 else "single_source_mid"
                ),
                "source_count": len(mids),
                "robustness": robustness,
                "mad_bp": format(
                    Decimal(0) if median_mid == 0
                    else mad / median_mid * Decimal(10_000),
                    "f",
                ),
                "dispersion_bp": format(
                    Decimal(0) if median_mid == 0
                    else dispersion / median_mid * Decimal(10_000),
                    "f",
                ),
            }
            ages = [float(item["age_seconds"]) for item in contributors]
            max_age = max(ages)
            available_times = [
                _time(item["as_of_available_time"]) for item in contributors
            ]
            concrete_times = [item for item in available_times if item is not None]
            max_skew = (
                (max(concrete_times) - min(concrete_times)).total_seconds()
                if concrete_times else None
            )

        degraded = (
            contributor_count < len(requested)
            or contributor_count < 3
            or any(item["quality_state"] != "ok" for item in contributors)
        )
        quality_state = (
            "unavailable" if not quorum_met else "degraded" if degraded else "ok"
        )
        source_set_version = _identifier(sorted(requested))
        result_id = _identifier({
            "method_version": CROSS_VENUE_METHOD_VERSION,
            "decision_time": decided.isoformat(),
            "source_head_generation": frozen.head_generation,
            "source_set_version": source_set_version,
            "min_quorum": min_quorum,
            "max_age_seconds": max_age_seconds,
        })
        return {
            "schema_version": CROSS_VENUE_SCHEMA_VERSION,
            "method_version": CROSS_VENUE_METHOD_VERSION,
            "result_id": result_id,
            "source": "materialized_active_heads",
            "decision_time": decided.isoformat(),
            "source_head_generation": frozen.head_generation,
            "source_set_version": source_set_version,
            "subject": {
                "instrument_id": str(first["instrument_id"]),
                "base_currency": str(first["base_currency"]),
                "quote_currency": str(first["quote_currency"]),
                "instrument_kind": str(first["instrument_kind"]),
                "market_kind": str(first["market_kind"]),
            },
            "expected_market_ids": list(requested),
            "contributors": contributors,
            "excluded": excluded,
            "quorum": {
                "required": min_quorum,
                "eligible": contributor_count,
                "contributing": contributor_count,
                "met": quorum_met,
            },
            "consolidated_bbo": bbo,
            "robust_mid_reference": reference,
            "freshness": {
                "basis": "as_of_available_time_at_decision",
                "threshold_seconds": max_age_seconds,
                "max_age_seconds": max_age,
                "max_as_of_skew_seconds": max_skew,
                "scope": "materialized_only",
            },
            "quality_state": quality_state,
            "conversion_path": [],
        }
