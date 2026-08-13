from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path

from guvolu.data.okx_l2_archive import (
    _day_end_millis,
    _recover_unmanifested_archive,
    _reuse_result,
    parse_download_plan,
)


def _plan_body() -> bytes:
    return json.dumps({
        "code": "0",
        "data": {
            "details": [{
                "instId": "BTC-USDT",
                "groupDetails": [{
                    "filename": "BTC-USDT-L2orderbook-400lv-2026-08-07.tar.gz",
                    "sizeMB": "105.92",
                    "url": (
                        "https://static.okx.com/cdn/okx/match/orderbook/pro/"
                        "L2/400lv/daily/20260807/"
                        "BTC-USDT-L2orderbook-400lv-2026-08-07.tar.gz"
                    ),
                }],
            }],
        },
        "msg": "",
    }, separators=(",", ":")).encode()


def test_parse_download_plan_preserves_response_identity() -> None:
    body = _plan_body()
    plan = parse_download_plan(
        body,
        venue_symbol="BTC-USDT",
        day=date(2026, 8, 7),
        depth_levels=400,
    )

    assert plan.advertised_size_mb == "105.92"
    assert plan.response_body == body
    assert plan.response_artifact_id == (
        "sha256-" + hashlib.sha256(body).hexdigest()
    )


def test_download_day_uses_tokyo_day_end() -> None:
    assert _day_end_millis(date(2026, 8, 7)) == "1786114799999"


def test_sealed_archive_reuse_rechecks_sha(tmp_path: Path) -> None:
    archive = tmp_path / "sample.tar.gz"
    archive.write_bytes(b"immutable-okx-sample")
    sha = hashlib.sha256(archive.read_bytes()).hexdigest()
    manifest = tmp_path / "sample.tar.gz.manifest.json"
    manifest.write_text(json.dumps({
        "status": "sealed",
        "completion_claim": True,
        "sha256": sha,
        "byte_count": archive.stat().st_size,
    }), encoding="utf-8")

    result = _reuse_result(tmp_path, archive, manifest)

    assert result.reused is True
    assert result.artifact_id == "sha256-" + sha


def test_completed_archive_can_recover_manifest_from_checkpoint(
    tmp_path: Path,
) -> None:
    plan = parse_download_plan(
        _plan_body(),
        venue_symbol="BTC-USDT",
        day=date(2026, 8, 7),
        depth_levels=400,
    )
    archive = tmp_path / plan.filename
    archive.write_bytes(b"complete-before-manifest")
    checkpoint = tmp_path / (plan.filename + ".checkpoint.json")
    checkpoint.write_text(json.dumps({
        "status": "open",
        "venue_id": "okx",
        "venue_symbol": plan.venue_symbol,
        "domain": "book_l2",
        "endpoint": "historical-data/order-book",
        "day": plan.day,
        "depth_levels": plan.depth_levels,
        "source_url": plan.url,
        "downloaded_bytes": archive.stat().st_size,
        "expected_bytes": archive.stat().st_size,
        "response_etag": "sample-etag",
        "response_last_modified": "Sat, 08 Aug 2026 00:00:47 GMT",
    }), encoding="utf-8")
    plan_evidence = tmp_path / "plan.json"
    instrument_evidence = tmp_path / "instrument.json"
    plan_evidence.write_bytes(plan.response_body)
    instrument_evidence.write_bytes(b"{}")
    manifest = tmp_path / (plan.filename + ".manifest.json")

    result = _recover_unmanifested_archive(
        tmp_path,
        archive,
        tmp_path / (plan.filename + ".part"),
        checkpoint,
        manifest,
        plan,
        plan_identity=plan.response_artifact_id,
        plan_evidence=plan_evidence.as_posix(),
        instrument_identity="sha256-instrument",
        instrument_evidence=instrument_evidence.as_posix(),
    )

    assert result.reused is True
    assert manifest.is_file()
    assert not checkpoint.exists()
